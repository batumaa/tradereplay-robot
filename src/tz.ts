// TradeZero REST adapter: the only file that knows TradeZero's URLs and field names. Endpoints and rate limits are from
// developer.tradezero.com (read 2026-10-19). Account, position and order FIELD names below are NOT verified on a real account.
export class TzError extends Error {
  status: number;
  code: string | null;
  constructor(status: number, code: string | null, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}
export class TzTimeout extends Error {}
export class TzBusy extends Error {}
export class TzAborted extends Error {}

export interface Deps {
  fetch: (url: string, init: RequestInit) => Promise<Response>;
  now: () => number;
  sleep: (ms: number, signal?: AbortSignal) => Promise<void>;
  random?: () => number;
}

export const REAL_HOST = "webapi.tradezero.com";
export const REAL_BASE = `https://${REAL_HOST}/v1/api`;
const ATTEMPT_MS = 5000;
export const CALL_LIMIT_MS = 15000;

// Calls per second, per key, per endpoint. Held as a sliding window, so no second ever goes over.
const RATES: Record<string, number> = {
  place: 10, cancel: 15, cancelAll: 1, getOrder: 2, orders: 2, positions: 3, routes: 1, borrow: 2, accounts: 2, account: 2, pnl: 2,
};

export class Limiter {
  private hits = new Map<string, number[]>();
  /** How long to wait before this call may go. */
  wait(name: string, now: number): number {
    const rate = RATES[name] ?? 1;
    const list = (this.hits.get(name) ?? []).filter((t) => now - t < 1000);
    this.hits.set(name, list);
    if (list.length < rate) {
      list.push(now);
      return 0;
    }
    return list[0] + 1000 - now;
  }
}

export interface Position { symbol: string; qty: number; price: number | null }
export interface BrokerOrder { coid: string; status: string; filled: number; qty: number; priceAvg: number | null; text: string | null }
export interface RouteRow { name: string; securityTypes: string[]; orderTypes: string[]; timesInForce: string[] }
export const ORDER_TYPES: Record<string, string> = { market: "Market", limit: "Limit", stop: "Stop", stop_limit: "StopLimit" };
export const TIMES_IN_FORCE: Record<string, string> = { day: "Day", day_plus: "Day_Plus", gtc: "GoodTillCancel", gtc_plus: "GTC_Plus", ioc: "ImmediateOrCancel" };
/** The first route that takes this stock order. None means no order, never a made-up name. */
export function pickRoute(rows: RouteRow[], type: string, tif: string): string | null {
  const row = rows.find((r) => r.securityTypes.includes("Stock") && r.orderTypes.includes(ORDER_TYPES[type]) && r.timesInForce.includes(TIMES_IN_FORCE[tif]));
  return row ? row.name : null;
}
const badShape = (name: string) => new TzError(200, "bad_shape", `${name}: answer isn't the expected shape`);
const texts = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

const OPEN_STATUS = new Set(["PendingNew", "Accepted", "New", "PartiallyFilled", "PendingCancel"]);
export const isOpenStatus = (s: string) => OPEN_STATUS.has(s);

export class TradeZero {
  private base: string;
  private keyId: string;
  private secret: string;
  private deps: Deps;
  private limiter: Limiter;
  signal?: AbortSignal;
  /** Clock difference against the last Date header, with the round trip allowed for. */
  lastSkewMs = 0;

  constructor(base: string, keyId: string, secret: string, deps: Deps, limiter: Limiter) {
    this.base = base.replace(/\/$/, "");
    this.keyId = keyId;
    this.secret = secret;
    this.deps = deps;
    this.limiter = limiter;
  }

  private async attempt(method: string, path: string, body?: unknown, form?: boolean): Promise<Response> {
    const timeout = AbortSignal.timeout(ATTEMPT_MS);
    const signal = this.signal ? AbortSignal.any([this.signal, timeout]) : timeout;
    const headers: Record<string, string> = { "TZ-API-KEY-ID": this.keyId, "TZ-API-SECRET-KEY": this.secret };
    let payload: string | undefined;
    if (body !== undefined) {
      headers["content-type"] = form ? "application/x-www-form-urlencoded" : "application/json";
      payload = form ? new URLSearchParams(body as Record<string, string>).toString() : JSON.stringify(body);
    }
    return this.deps.fetch(this.base + path, { method, headers, body: payload, signal });
  }

  /** One logical call: rate limit, 429 back-off and timeouts, all under one total limit. */
  private async call(name: string, method: string, path: string, body?: unknown, opts: { form?: boolean; retryTimeout?: boolean } = {}): Promise<any> {
    const start = this.deps.now();
    const over = () => this.deps.now() - start >= CALL_LIMIT_MS;
    let backoff = 2000;
    for (;;) {
      if (this.signal?.aborted) throw new TzAborted("aborted");
      const wait = this.limiter.wait(name, this.deps.now());
      if (wait > 0) {
        if (this.deps.now() - start + wait >= CALL_LIMIT_MS) throw new TzBusy(`${name}: rate limit wait over the time limit`);
        await this.pause(wait);
        continue;
      }
      const t0 = this.deps.now();
      let res: Response;
      try {
        res = await this.attempt(method, path, body, opts.form);
      } catch (err) {
        if (this.signal?.aborted) throw new TzAborted("aborted");
        if (opts.retryTimeout === false) throw new TzTimeout(`${name}: timed out`);
        if (over()) throw new TzBusy(`${name}: timed out until the time limit`);
        continue;
      }
      const t1 = this.deps.now();
      const date = res.headers.get("date");
      if (date) {
        const d = Date.parse(date);
        if (!Number.isNaN(d)) this.lastSkewMs = Math.max(0, Math.abs(d - (t0 + t1) / 2) - (t1 - t0) / 2);
      }
      if (res.status === 429) {
        const jitter = (this.deps.random?.() ?? Math.random()) * 250;
        if (this.deps.now() - start + backoff + jitter >= CALL_LIMIT_MS) throw new TzBusy(`${name}: rate limited until the time limit`);
        await this.pause(backoff + jitter);
        backoff = Math.min(backoff * 2, 60000);
        continue;
      }
      const text = await res.text();
      let json: any = null;
      try { json = text ? JSON.parse(text) : null; } catch { /* not JSON */ }
      if (!res.ok) throw new TzError(res.status, json?.code ?? null, json?.message ?? (text.slice(0, 200) || `${name}: HTTP ${res.status}`));
      return json;
    }
  }

  private async pause(ms: number) {
    await this.deps.sleep(ms, this.signal);
    if (this.signal?.aborted) throw new TzAborted("aborted");
  }

  async accounts(): Promise<string[]> {
    const j = await this.call("accounts", "GET", "/accounts");
    if (!Array.isArray(j?.accounts)) throw badShape("accounts");
    return j.accounts.map((a: any) => a?.account ?? a?.accountId).filter((x: unknown) => typeof x === "string" && x).map(String);
  }

  /** `paper` only when the reply says so. Anything else, a missing field included, is not paper. */
  async account(id: string): Promise<{ paper: boolean; type: string | null; buyingPower: number | null }> {
    const j = await this.call("account", "GET", `/account/${encodeURIComponent(id)}`);
    const type = typeof j?.accountType === "string" ? j.accountType : null;
    const bp = typeof j?.bp === "number" ? j.bp : typeof j?.buyingPower === "number" ? j.buyingPower : null;
    return { paper: type?.toLowerCase() === "paper", type, buyingPower: bp };
  }

  async routes(id: string): Promise<RouteRow[]> {
    const j = await this.call("routes", "GET", `/accounts/${encodeURIComponent(id)}/routes`);
    if (!Array.isArray(j?.routes)) throw badShape("routes");
    return j.routes.filter((r: any) => typeof r?.routeName === "string").map((r: any) => ({
      name: r.routeName, securityTypes: texts(r.securityTypes), orderTypes: texts(r.orderTypes), timesInForce: texts(r.timesInForce),
    }));
  }

  /** One POST, never retried here: a timeout may mean TradeZero has it. */
  async place(id: string, o: { coid: string; side: "Buy" | "Sell"; openClose: "Open" | "Close"; symbol: string; qty: number; type: string; limit?: number | null; stop?: number | null; tif: string; route: string }): Promise<{ status: string | null; text: string | null }> {
    const j = await this.call("place", "POST", `/accounts/${encodeURIComponent(id)}/order`, {
      clientOrderId: o.coid, securityType: "Stock", side: o.side, openClose: o.openClose, symbol: o.symbol.toUpperCase(),
      orderQuantity: o.qty, orderType: ORDER_TYPES[o.type], limitPrice: o.limit ?? undefined, stopPrice: o.stop ?? undefined, timeInForce: TIMES_IN_FORCE[o.tif], route: o.route,
    }, { retryTimeout: false });
    // A 200 says only that the body was valid: the outcome is in orderStatus.
    return { status: typeof j?.orderStatus === "string" ? j.orderStatus : null, text: typeof j?.text === "string" ? j.text : null };
  }

  async getOrder(id: string, coid: string): Promise<BrokerOrder | null> {
    try {
      return normOrder(await this.call("getOrder", "GET", `/accounts/${encodeURIComponent(id)}/order/${encodeURIComponent(coid)}`));
    } catch (err) {
      if (err instanceof TzError && err.status === 404) return null;
      throw err;
    }
  }

  async orders(id: string): Promise<BrokerOrder[]> {
    const j = await this.call("orders", "GET", `/accounts/${encodeURIComponent(id)}/orders`);
    if (!Array.isArray(j?.orders)) throw badShape("orders");
    return j.orders.map(normOrder);
  }

  async cancel(id: string, coid: string): Promise<void> {
    await this.call("cancel", "DELETE", `/accounts/${encodeURIComponent(id)}/orders/${encodeURIComponent(coid)}`);
  }

  async cancelAll(id: string): Promise<void> {
    await this.call("cancelAll", "DELETE", "/accounts/orders", { account: id }, { form: true });
  }

  async positions(id: string): Promise<Position[]> {
    const { checked } = await this.positionsRead(id);
    if (!checked) throw badShape("positions"); // never act on rows that failed the strict read
    return checked;
  }

  /** `checked` is null unless every part of the answer is exactly what we expect: accepting the broker's numbers on a wrong
   *  shape would read as flat and wipe the ledger. Field names checked on a real paper account, 2026-11-06. */
  async positionsRead(id: string): Promise<{ positions: Position[]; checked: Position[] | null; other: number }> {
    const j = await this.call("positions", "GET", `/accounts/${encodeURIComponent(id)}/positions`);
    if (!Array.isArray(j?.positions)) throw badShape("positions");
    const rows: any[] = j.positions;
    const wellFormed = (p: any) => !!p && typeof p.symbol === "string" && !!p.symbol && typeof p.securityType === "string"
      && typeof p.shares === "number" && Number.isFinite(p.shares) && (p.side === "Long" || p.side === "Short");
    // Stock only, one line per symbol: an account can hold several lots of a symbol, and an option row's `shares` counts contracts.
    const bySymbol = new Map<string, Position>();
    for (const p of rows) {
      if (!p || (typeof p.securityType === "string" && p.securityType !== "Stock")) continue;
      const symbol = String(p.symbol);
      const qty = Math.abs(Number(p.shares)) * (p.side === "Short" ? -1 : 1);
      const seen = bySymbol.get(symbol);
      const price = typeof p.lastPrice === "number" ? p.lastPrice : null;
      bySymbol.set(symbol, seen ? { symbol, qty: seen.qty + qty, price: seen.price ?? price } : { symbol, qty, price });
    }
    const positions = [...bySymbol.values()];
    const other = rows.filter((p) => typeof p?.securityType === "string" && p.securityType !== "Stock").length;
    return { positions, checked: rows.every(wellFormed) ? positions : null, other };
  }

  /** Today's P&L, or null when the reply doesn't carry it. */
  async pnl(id: string): Promise<number | null> {
    const j = await this.call("pnl", "GET", `/accounts/${encodeURIComponent(id)}/pnl`);
    return typeof j?.dayPnl === "number" ? j.dayPnl : null;
  }

  async easyToBorrow(id: string, sym: string): Promise<boolean> {
    const j = await this.call("borrow", "GET", `/accounts/${encodeURIComponent(id)}/is-easy-to-borrow/symbol/${encodeURIComponent(sym)}`);
    return j?.isEasyToBorrow === true;
  }
}

function normOrder(o: any): BrokerOrder {
  const priceAvg = typeof o.priceAvg === "number" && o.priceAvg > 0 ? o.priceAvg : null;
  return { coid: String(o.clientOrderId), status: String(o.orderStatus), filled: Number(o.executed ?? 0), qty: Number(o.orderQuantity ?? 0), priceAvg, text: typeof o.text === "string" ? o.text : null };
}
