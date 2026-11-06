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
export interface BrokerOrder { coid: string; status: string; filled: number; qty: number; priceAvg: number | null }

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
      if (!res.ok) throw new TzError(res.status, json?.code ?? null, json?.message ?? `${name}: HTTP ${res.status}`);
      return json;
    }
  }

  private async pause(ms: number) {
    await this.deps.sleep(ms, this.signal);
    if (this.signal?.aborted) throw new TzAborted("aborted");
  }

  async accounts(): Promise<string[]> {
    const j = await this.call("accounts", "GET", "/accounts");
    const list = Array.isArray(j) ? j : j?.accounts ?? [];
    return list.map((a: any) => String(a.accountId ?? a.id));
  }

  /** `paper` only when the reply says so. Anything else, a missing field included, is not paper. */
  async account(id: string): Promise<{ paper: boolean; type: string | null; buyingPower: number | null }> {
    const j = await this.call("account", "GET", `/account/${encodeURIComponent(id)}`);
    const type = typeof j?.accountType === "string" ? j.accountType : null;
    const bp = typeof j?.buyingPower === "number" ? j.buyingPower : null;
    return { paper: type?.toLowerCase() === "paper", type, buyingPower: bp };
  }

  async routes(id: string): Promise<string[]> {
    const j = await this.call("routes", "GET", `/accounts/${encodeURIComponent(id)}/routes`);
    return Array.isArray(j) ? j.map((r: any) => String(r.route ?? r)) : [];
  }

  /** One POST, never retried here: a timeout may mean TradeZero has it. */
  async place(id: string, o: { coid: string; side: "Buy" | "Sell"; openClose: "Open" | "Close"; symbol: string; qty: number; type: string; limit?: number | null; stop?: number | null; tif: string; route: string }): Promise<void> {
    const TYPES: Record<string, string> = { market: "Market", limit: "Limit", stop: "Stop", stop_limit: "StopLimit" };
    const TIFS: Record<string, string> = { day: "Day", day_plus: "DayPlus", gtc: "GTC", gtc_plus: "GTCPlus", ioc: "IOC" };
    await this.call("place", "POST", `/accounts/${encodeURIComponent(id)}/order`, {
      clientOrderId: o.coid, securityType: "Stock", side: o.side, openClose: o.openClose, symbol: o.symbol.toUpperCase(),
      orderQuantity: o.qty, orderType: TYPES[o.type], limitPrice: o.limit ?? undefined, stopPrice: o.stop ?? undefined, timeInForce: TIFS[o.tif], route: o.route,
    }, { retryTimeout: false });
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
    return (Array.isArray(j) ? j : []).map(normOrder);
  }

  async cancel(id: string, coid: string): Promise<void> {
    await this.call("cancel", "DELETE", `/accounts/${encodeURIComponent(id)}/orders/${encodeURIComponent(coid)}`);
  }

  async cancelAll(id: string): Promise<void> {
    await this.call("cancelAll", "DELETE", "/accounts/orders", { accountId: id }, { form: true });
  }

  async positions(id: string): Promise<Position[]> {
    const j = await this.call("positions", "GET", `/accounts/${encodeURIComponent(id)}/positions`);
    return (Array.isArray(j) ? j : []).map((p: any) => ({
      symbol: String(p.symbol), qty: Number(p.shares) * (p.side === "Short" ? -1 : 1),
      price: typeof p.lastPrice === "number" ? p.lastPrice : null,
    }));
  }

  /** Today's P&L, or null when the reply doesn't carry it. */
  async pnl(id: string): Promise<number | null> {
    const j = await this.call("pnl", "GET", `/accounts/${encodeURIComponent(id)}/pnl`);
    return typeof j?.dayTotal === "number" ? j.dayTotal : null;
  }

  async easyToBorrow(id: string, sym: string): Promise<boolean> {
    const j = await this.call("borrow", "GET", `/accounts/${encodeURIComponent(id)}/is-easy-to-borrow/symbol/${encodeURIComponent(sym)}`);
    return j?.isEasyToBorrow === true;
  }
}

function normOrder(o: any): BrokerOrder {
  const priceAvg = typeof o.priceAvg === "number" && o.priceAvg > 0 ? o.priceAvg : null;
  return { coid: String(o.clientOrderId), status: String(o.orderStatus), filled: Number(o.executed ?? 0), qty: Number(o.orderQuantity ?? 0), priceAvg };
}
