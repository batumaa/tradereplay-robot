// The robot: checks each signal, sizes it from the strategy's balance, places it once, tracks fills, reconciles, flattens.
// Everything runs one at a time through one queue, because a Durable Object's requests interleave during a fetch.
import { isClose, isEntry, type Signal } from "./signal.ts";
import { inHours, isRegular, ny, sessionRule } from "./session.ts";
import { sizeShares } from "./size.ts";
import { Limiter, REAL_HOST, TradeZero, TzAborted, TzBusy, TzError, TzTimeout, isOpenStatus, type BrokerOrder, type Deps } from "./tz.ts";

export interface Store {
  get(key: string): Promise<any>;
  put(key: string, value: any): Promise<void>;
}

export interface Limits { maxPerTrade: number; maxPositions: number; maxDailyLoss: number; hoursFrom: number; hoursTo: number }
export const DEFAULT_LIMITS: Limits = { maxPerTrade: 500, maxPositions: 3, maxDailyLoss: 200, hoursFrom: 4 * 60, hoursTo: 16 * 60 };

const UNKNOWN_WATCH_MS = 5 * 60 * 1000;
const STATE = "state";
// Pairing lives under its own keys: these change outside the queue, so they must never be saved with State.
const PAIR_TOKEN = "pair_token";
const PAIR_SESSION = "pair_session";
const PAIR_REPORTED = "pair_reported";
const SESSION_MS = 30 * 60 * 1000;
const ENGINE_MS = 15000; // the engine can cold start

interface Order {
  coid: string; strategy: string; symbol: string; action: string; qty: number; filled: number;
  status: string; cancelAt: number | null; unknownSince?: number; afterKill?: boolean;
  // Filled shares per "strategy|SYMBOL", what's been booked for them, and keys no longer waiting for a price.
  alloc?: Record<string, number>; booked?: Record<string, number>; avg?: number | null; waived?: Record<string, true>;
}
interface State {
  keys: { id: string; secret: string; hash: string } | null;
  verdict: { paper: boolean; keyHash: string; reason: string } | null;
  accountId: string | null;
  route: string | null;
  skewBad: boolean;
  limits: Limits;
  strategies: Record<string, { capital: number; realized?: number }>;
  holdings: Record<string, number>; // "strategy|SYMBOL" → signed shares
  cash: Record<string, number>; // "strategy|SYMBOL" → money in and out since it was last flat
  cashBad: Record<string, true>; // keys whose cash is missing a fill: it counts as 0 when they go flat
  orders: Record<string, Order>;
  answers: Record<string, Ack>; // signal id → the answer it got, newest 1,000
  paused: { reason: string; date: string } | null;
  drift: unknown;
  closeByHand: string[];
  lastReconcile: number;
}

const fresh = (): State => ({
  keys: null, verdict: null, accountId: null, route: null, skewBad: false, limits: { ...DEFAULT_LIMITS }, strategies: {}, holdings: {}, cash: {}, cashBad: {},
  orders: {}, answers: {}, paused: null, drift: null, closeByHand: [], lastReconcile: 0,
});

export interface Ack { status: "placed" | "rejected" | "unknown" | "done" | "gone"; reason?: string; coid?: string; qty?: number; closeByHand?: string[]; repeat?: boolean }

// Worth trying again with the same id, so never stored as that id's answer.
const RETRYABLE = new Set(["expired", "tz_busy", "outside_hours", "clock_skew", "session_rule", "balance_unknown"]);

export type PairAnswer = { ok: true; session?: string } | { ok: false; status: number; error: string };

const randomToken = () => [...crypto.getRandomValues(new Uint8Array(32))].map((b) => b.toString(16).padStart(2, "0")).join("");

export async function sha256(text: string): Promise<string> {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

const hk = (strategy: string, symbol: string) => `${strategy}|${symbol.toUpperCase()}`;
const sideOf = (a: string) => (a === "buy" || a === "cover" ? 1 : -1);
const awaitsPrice = (o: Order, k: string) => (o.alloc?.[k] ?? 0) > 0 && o.avg == null && !o.waived?.[k];
const lacksPrice = (o: Order) => Object.keys(o.alloc ?? {}).some((k) => awaitsPrice(o, k));
const balanceOf = (st: { capital: number; realized?: number }) => st.capital + (st.realized ?? 0);

export class Robot {
  private store: Store;
  private base: string;
  private deps: Deps;
  private limiter = new Limiter();
  private queue: Array<() => void> = [];
  private busy = false;
  private current: AbortController | null = null;
  private engineUrl: string | null;

  constructor(store: Store, base: string, deps: Deps, engineUrl: string | null = null) {
    this.store = store;
    this.base = base;
    this.deps = deps;
    this.engineUrl = engineUrl ? engineUrl.replace(/\/$/, "") : null;
  }

  // ── pairing with TradeReplay ──────────────────────────────────────────────
  private async callEngine(path: string, body: unknown, token?: string): Promise<{ status: number; json: any }> {
    if (!this.engineUrl) throw new Error("no engine address");
    const res = await this.deps.fetch(`${this.engineUrl}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}) },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(ENGINE_MS),
    });
    let json: any = null;
    try { json = await res.json(); } catch { /* not JSON */ }
    return { status: res.status, json };
  }

  pairedToken(): Promise<string | null> {
    return this.store.get(PAIR_TOKEN).then((t) => t ?? null);
  }

  /** D3-D5 of docs/SITE_ROBOT_PAIRING_PLAN.md. A paired robot is never handed to another account. */
  async pair(origin: string, code: string): Promise<PairAnswer> {
    const unreachable: PairAnswer = { ok: false, status: 502, error: "Could not reach TradeReplay. Try again." };
    const held = await this.pairedToken();
    if (held) {
      try {
        const r = await this.callEngine("/v1/robots/check-code", { code }, held);
        if (r.status >= 500) return unreachable;
        return r.status === 200 ? { ok: true } : { ok: false, status: 409, error: "This robot is already paired with an account." };
      } catch {
        return unreachable;
      }
    }
    return this.run(async () => {
      // Two calls at once: the queue lets only the first make a token.
      if (await this.pairedToken()) return { ok: false, status: 409, error: "This robot is already paired with an account." };
      const token = randomToken();
      try {
        const r = await this.callEngine("/v1/robots", { code, url: origin, token });
        if (r.status >= 500) return unreachable;
        if (r.status !== 200) return { ok: false, status: r.status, error: r.json?.detail?.message || "Could not pair this robot." };
      } catch {
        return unreachable;
      }
      await this.store.put(PAIR_TOKEN, token);
      return { ok: true };
    });
  }

  /** A code from the signed-in owner buys a short session on this robot, so a mistyped key can be tried again. */
  async openSession(code: string): Promise<PairAnswer> {
    const token = await this.pairedToken();
    if (!token) return { ok: false, status: 409, error: "This robot isn't paired yet." };
    try {
      const r = await this.callEngine("/v1/robots/check-code", { code }, token);
      if (r.status >= 500) return { ok: false, status: 502, error: "Could not reach TradeReplay. Try again." };
      if (r.status !== 200) return { ok: false, status: 403, error: "Open this page again from TradeReplay." };
    } catch {
      return { ok: false, status: 502, error: "Could not reach TradeReplay. Try again." };
    }
    const session = randomToken();
    await this.store.put(PAIR_SESSION, { hash: await sha256(session), expires: this.deps.now() + SESSION_MS });
    return { ok: true, session };
  }

  async sessionOk(session: string): Promise<boolean> {
    const held = await this.store.get(PAIR_SESSION);
    return !!held && held.expires > this.deps.now() && held.hash === (await sha256(session));
  }

  /** Tells the engine the key's state, only when it differs from the last one that got through. */
  async reportBroker(): Promise<void> {
    const token = await this.pairedToken();
    if (!token || !this.engineUrl) return;
    const s = await this.load();
    if (!s.keys) return;
    const broker = this.blocked(s) ? "not_paper" : "connected";
    if ((await this.store.get(PAIR_REPORTED)) === broker) return;
    try {
      const r = await this.callEngine("/v1/robots/me/status", { broker }, token);
      if (r.status === 200) await this.store.put(PAIR_REPORTED, broker);
    } catch { /* the next tick tries again */ }
  }

  private run<T>(fn: (ctrl: AbortController) => Promise<T>, front = false): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const job = async () => {
        const ctrl = new AbortController();
        this.current = ctrl;
        try { resolve(await fn(ctrl)); } catch (err) { reject(err); }
        this.current = null;
        this.busy = false;
        this.pump();
      };
      if (front) this.queue.unshift(job); else this.queue.push(job);
      this.pump();
    });
  }

  private pump() {
    if (this.busy) return;
    const next = this.queue.shift();
    if (!next) return;
    this.busy = true;
    next();
  }

  private async load(): Promise<State> {
    return { ...fresh(), ...((await this.store.get(STATE)) ?? {}) };
  }

  private async save(s: State) {
    // Durable Object values are capped at 128 KiB: keep recent ids and only the orders that matter.
    const ids = Object.keys(s.answers);
    for (const id of ids.slice(0, Math.max(0, ids.length - 1000))) delete s.answers[id];
    const done = Object.values(s.orders).filter((o) => !isOpenStatus(o.status) && o.status !== "Unknown" && !lacksPrice(o));
    for (const o of done.slice(0, Math.max(0, done.length - 200))) delete s.orders[o.coid];
    await this.store.put(STATE, s);
  }

  private tz(s: State, ctrl?: AbortController): TradeZero {
    if (!s.keys) throw new Error("no key");
    const t = new TradeZero(this.base, s.keys.id, s.keys.secret, this.deps, this.limiter);
    t.signal = ctrl?.signal;
    return t;
  }

  // ── broker key ────────────────────────────────────────────────────────────
  /** Checks a key before keeping it. A refused key isn't saved; a live key is saved with a not-paper verdict. */
  saveBroker(id: string, secret: string): Promise<{ ok: boolean; reason?: string; paper?: boolean }> {
    return this.run(async (ctrl) => {
      const s = await this.load();
      const t = new TradeZero(this.base, id, secret, this.deps, this.limiter);
      t.signal = ctrl.signal;
      let accountId: string;
      try {
        const list = await t.accounts();
        if (!list.length) return { ok: false, reason: "That key has no account." };
        accountId = list[0];
      } catch (err) {
        if (err instanceof TzError && (err.status === 401 || err.status === 403)) return { ok: false, reason: "TradeZero refused that key." };
        return { ok: false, reason: "Could not reach TradeZero." };
      }
      const hash = await sha256(`${id}:${secret}`);
      s.keys = { id, secret, hash };
      s.accountId = accountId;
      s.route = null;
      s.verdict = { paper: false, keyHash: hash, reason: "not checked" };
      try {
        await this.checkAccount(s, t);
      } catch {
        s.verdict = { paper: false, keyHash: hash, reason: "Could not read the account type." };
      }
      await this.save(s);
      return { ok: true, paper: s.verdict.paper };
    });
  }

  /** D1 and skew check. Throws when the call itself fails, so a blip keeps the last verdict. */
  private async checkAccount(s: State, t: TradeZero) {
    const acct = await t.account(s.accountId as string);
    const hash = (s.keys as { hash: string }).hash;
    s.verdict = acct.paper
      ? { paper: true, keyHash: hash, reason: "" }
      : { paper: false, keyHash: hash, reason: acct.type ? `Account type is ${acct.type}: paper only.` : "Account type is missing: paper only." };
    s.skewBad = t.lastSkewMs > 2000;
  }

  private blocked(s: State): string | null {
    if (!s.keys || !s.accountId) return "no_broker";
    if (!s.verdict || !s.verdict.paper || s.verdict.keyHash !== s.keys.hash) return "not_paper";
    return null;
  }

  // ── settings ──────────────────────────────────────────────────────────────
  setLimits(patch: Partial<Limits>): Promise<Limits> {
    return this.run(async () => {
      const s = await this.load();
      for (const k of Object.keys(DEFAULT_LIMITS) as Array<keyof Limits>) {
        const v = patch[k];
        if (typeof v === "number" && Number.isFinite(v) && v >= 0) s.limits[k] = v;
      }
      await this.save(s);
      return s.limits;
    });
  }

  setStrategy(id: string, capital: number): Promise<void> {
    return this.run(async () => {
      const s = await this.load();
      const old = s.strategies[id];
      // The hub resends Capital on a new robot address, and that must not wipe P&L.
      if (old?.capital === capital) return;
      s.strategies[id] = { capital, realized: 0 };
      for (const o of Object.values(s.orders)) {
        for (const k of Object.keys(o.alloc ?? {})) {
          if (!k.startsWith(`${id}|`) || !awaitsPrice(o, k)) continue;
          o.waived = { ...o.waived, [k]: true };
          s.cashBad[k] = true;
        }
      }
      this.settle(s);
      await this.save(s);
    });
  }

  resume(): Promise<void> {
    return this.run(async () => {
      const s = await this.load();
      s.paused = null;
      s.drift = null;
      await this.save(s);
    });
  }

  status(): Promise<any> {
    return this.run(async () => {
      const s = await this.load();
      const strategies = Object.fromEntries(Object.entries(s.strategies).map(([id, st]) => [id, { capital: st.capital, balance: balanceOf(st), balanceUnknown: this.balanceUnknown(s, id) }]));
      return {
        broker: s.keys ? "connected" : null, paper: s.verdict?.paper ?? false, why: this.blocked(s) ? s.verdict?.reason || this.blocked(s) : null,
        clockSkew: s.skewBad, paused: s.paused, drift: s.drift, limits: s.limits, strategies, holdings: s.holdings,
        orders: Object.values(s.orders), closeByHand: s.closeByHand,
      };
    });
  }

  connected(): Promise<boolean> {
    return this.run(async () => !!(await this.load()).keys);
  }

  // ── signals ───────────────────────────────────────────────────────────────
  onSignal(sig: Signal): Promise<Ack> {
    return this.run(async (ctrl) => {
      const s = await this.load();
      const known = s.answers[sig.id];
      if (known) {
        const settled = this.settled(s, known);
        s.answers[sig.id] = settled;
        await this.save(s);
        return { ...settled, repeat: true };
      }
      const ack = await this.safely(() => this.handleSignal(s, sig, ctrl));
      if (!(ack.status === "rejected" && RETRYABLE.has(ack.reason ?? ""))) s.answers[sig.id] = ack;
      await this.save(s);
      return ack;
    });
  }

  /** A TradeZero failure on any check answers the signal; it doesn't crash the route. */
  private async safely(fn: () => Promise<Ack>): Promise<Ack> {
    try {
      return await fn();
    } catch (err) {
      if (err instanceof TzBusy || err instanceof TzTimeout || err instanceof TzAborted) return { status: "rejected", reason: "tz_busy" };
      if (err instanceof TzError) return { status: "rejected", reason: `tz_rejected: ${err.code ?? err.status}` };
      throw err;
    }
  }

  /** A stored `unknown` follows its order as polling settles it. */
  private settled(s: State, ack: Ack): Ack {
    if (ack.status !== "unknown" || !ack.coid) return ack;
    const order = s.orders[ack.coid];
    if (order?.status === "Unknown") return ack;
    if (order?.status === "Gone") return { status: "gone", coid: ack.coid };
    return { status: "placed", coid: ack.coid, qty: ack.qty };
  }

  private clearDailyPause(s: State, now: number) {
    if (s.paused?.reason === "daily_loss" && s.paused.date !== ny(now).date) s.paused = null;
  }

  private async handleSignal(s: State, sig: Signal, ctrl: AbortController): Promise<Ack> {
    const now = this.deps.now();
    const reject = (reason: string): Ack => ({ status: "rejected", reason });
    const stop = this.blocked(s);
    if (stop) return reject(stop);
    if (now > Date.parse(sig.ts) + sig.valid_ms) return reject("expired");
    const t = this.tz(s, ctrl);
    const acct = s.accountId as string;

    if (sig.action === "cancel") {
      await t.cancel(acct, sig.ref as string);
      return { status: "done" };
    }
    if (sig.action === "flatten") return this.flattenStrategy(s, sig.strategy, t);

    const symbol = (sig.symbol as string).toUpperCase();
    const type = sig.type!;
    const tif = sig.tif!;
    const rule = sessionRule(type, tif, now);
    if (rule) return reject(rule);

    if (isClose(sig.action)) {
      const held = s.holdings[hk(sig.strategy, symbol)] ?? 0;
      const qty = sig.action === "sell" ? Math.max(0, held) : Math.max(0, -held);
      if (!qty) return reject("nothing_to_close");
      return this.placeOrder(s, sig, t, symbol, qty, ctrl);
    }

    // Entries only from here.
    this.clearDailyPause(s, now);
    if (s.paused) return reject("paused");
    if (s.skewBad) return reject("clock_skew");
    if (!inHours(now, s.limits.hoursFrom, s.limits.hoursTo)) return reject("outside_hours");
    const strat = s.strategies[sig.strategy];
    if (!strat) return reject("no_capital");
    if (this.balanceUnknown(s, sig.strategy)) return reject("balance_unknown");
    const price = sig.limit ?? (sig.price as number);
    const qty = sizeShares(sig.pct as number, balanceOf(strat), price);
    if (qty < 1) return reject("too_small");
    if (qty * price > s.limits.maxPerTrade) return reject("max_per_trade");
    if (this.openCount(s) >= s.limits.maxPositions) return reject("max_positions");
    const pnl = await t.pnl(acct);
    if (pnl === null) return reject("pnl_unknown");
    if (pnl <= -s.limits.maxDailyLoss) {
      s.paused = { reason: "daily_loss", date: ny(now).date };
      return reject("daily_loss");
    }
    if (sig.action === "short" && !(await t.easyToBorrow(acct, symbol))) return reject("no_borrow");
    const info = await t.account(acct);
    if (info.buyingPower === null) return reject("buying_power_unknown");
    if (qty * price > info.buyingPower) return reject("buying_power");
    return this.placeOrder(s, sig, t, symbol, qty, ctrl);
  }

  /** Symbols held plus entries still working, across every strategy. */
  private openCount(s: State): number {
    const syms = new Set<string>();
    for (const [k, q] of Object.entries(s.holdings)) if (q !== 0) syms.add(k.split("|")[1]);
    for (const o of Object.values(s.orders)) {
      if ((isOpenStatus(o.status) || o.status === "Unknown") && isEntry(o.action as any)) syms.add(o.symbol);
    }
    return syms.size;
  }

  private async route(s: State, t: TradeZero): Promise<string> {
    if (!s.route) s.route = (await t.routes(s.accountId as string))[0] ?? "SMART";
    return s.route;
  }

  private async placeOrder(s: State, sig: Signal, t: TradeZero, symbol: string, qty: number, ctrl: AbortController): Promise<Ack> {
    const order: Order = {
      coid: sig.id, strategy: sig.strategy, symbol, action: sig.action, qty, filled: 0, status: "New",
      cancelAt: sig.cancel_after_ms ? this.deps.now() + sig.cancel_after_ms : null,
    };
    const ack = await this.sendOnce(s, t, order, {
      side: sig.action === "buy" || sig.action === "cover" ? "Buy" : "Sell",
      openClose: isEntry(sig.action) ? "Open" : "Close",
      type: sig.type as string, limit: sig.limit, stop: sig.stop, tif: sig.tif as string,
    }, ctrl);
    return ack;
  }

  /** Never sends twice: after a timeout, look first; R114 means TradeZero already has it. */
  private async sendOnce(s: State, t: TradeZero, order: Order, f: { side: "Buy" | "Sell"; openClose: "Open" | "Close"; type: string; limit?: number | null; stop?: number | null; tif: string }, ctrl: AbortController): Promise<Ack> {
    const acct = s.accountId as string;
    const body = { coid: order.coid, symbol: order.symbol, qty: order.qty, ...f, route: "" };
    const placed = (): Ack => {
      s.orders[order.coid] = order;
      return { status: "placed", coid: order.coid, qty: order.qty };
    };
    const unknown = (): Ack => {
      order.status = "Unknown";
      order.unknownSince = this.deps.now();
      s.orders[order.coid] = order;
      return { status: "unknown", reason: "tz_unknown", coid: order.coid, qty: order.qty };
    };
    try {
      body.route = await this.route(s, t);
      await t.place(acct, body);
      return placed();
    } catch (err) {
      if (err instanceof TzBusy) return { status: "rejected", reason: "tz_busy" };
      if (err instanceof TzAborted) return unknown();
      if (err instanceof TzError) {
        if (err.code === "R114") return placed();
        return { status: "rejected", reason: `tz_rejected: ${err.code ?? err.status}` };
      }
      if (!(err instanceof TzTimeout)) throw err;
    }
    // Timed out: TradeZero may hold it.
    try {
      if (await t.getOrder(acct, order.coid)) return placed();
    } catch {
      return unknown();
    }
    try {
      await t.place(acct, body);
      return placed();
    } catch (err) {
      if (err instanceof TzError && err.code === "R114") return placed();
      if (err instanceof TzBusy) return { status: "rejected", reason: "tz_busy" };
      if (err instanceof TzError) return { status: "rejected", reason: `tz_rejected: ${err.code ?? err.status}` };
      return unknown();
    }
  }

  private async flattenStrategy(s: State, strategy: string, t: TradeZero): Promise<Ack> {
    for (const o of Object.values(s.orders)) {
      if (o.strategy === strategy && isOpenStatus(o.status)) {
        await t.cancel(s.accountId as string, o.coid);
        o.status = "Canceled";
      }
    }
    const positions = await t.positions(s.accountId as string);
    const byHand: string[] = [];
    for (const [k, q] of Object.entries(s.holdings)) {
      const [st, symbol] = k.split("|");
      if (st !== strategy || !q) continue;
      await this.closeQty(s, t, st, symbol, q, positions.find((p) => p.symbol === symbol)?.price ?? null);
      if (s.closeByHand.includes(symbol)) byHand.push(symbol);
    }
    return byHand.length ? { status: "done", closeByHand: byHand } : { status: "done" };
  }

  /** Market in regular hours; a limit at the position's price ± 2% outside them, since market orders are refused there. */
  private async closeQty(s: State, t: TradeZero, strategy: string, symbol: string, signed: number, price: number | null) {
    const now = this.deps.now();
    const regular = isRegular(now);
    if (!regular && price === null) {
      if (!s.closeByHand.includes(symbol)) s.closeByHand.push(symbol);
      return;
    }
    const coid = `x-${symbol}-${now}-${Object.keys(s.orders).length}`.slice(0, 36);
    const action = signed > 0 ? "sell" : "cover";
    const limit = regular ? null : Math.round((signed > 0 ? (price as number) * 0.98 : (price as number) * 1.02) * 100) / 100;
    const order: Order = { coid, strategy, symbol, action, qty: Math.abs(signed), filled: 0, status: "New", cancelAt: null };
    await this.sendOnce(s, t, order, { side: signed > 0 ? "Sell" : "Buy", openClose: "Close", type: regular ? "market" : "limit", limit, tif: "day" }, this.current ?? new AbortController());
  }

  // ── orders ────────────────────────────────────────────────────────────────
  pollOrders(): Promise<void> {
    return this.run(async (ctrl) => {
      const s = await this.load();
      try { await this.poll(s, this.tz(s, ctrl)); } finally { await this.save(s); }
    });
  }

  private balanceUnknown(s: State, id: string): boolean {
    return Object.values(s.orders).some((o) => Object.keys(o.alloc ?? {}).some((k) => k.startsWith(`${id}|`) && awaitsPrice(o, k)));
  }

  /** Takes an order's latest numbers from TradeZero: its fills, its status, then the money they moved. */
  private track(s: State, o: Order, b: BrokerOrder) {
    this.applyFill(s, o, b.filled - o.filled);
    o.filled = b.filled;
    o.status = b.status;
    o.avg = b.priceAvg;
    if (o.avg == null) return;
    o.booked ??= {};
    for (const [k, shares] of Object.entries(o.alloc ?? {})) {
      if (o.waived?.[k]) continue;
      const total = shares * o.avg; // priceAvg covers all the order's fills, so recount rather than add
      s.cash[k] = (s.cash[k] ?? 0) - sideOf(o.action) * (total - (o.booked[k] ?? 0));
      o.booked[k] = total;
    }
  }

  /** A strategy's position counts toward its balance once flat, with every fill's price known. */
  private settle(s: State) {
    const orders = Object.values(s.orders);
    for (const k of new Set([...Object.keys(s.cash), ...Object.keys(s.cashBad)])) {
      if (s.holdings[k] || orders.some((o) => awaitsPrice(o, k))) continue;
      const st = s.strategies[k.split("|")[0]];
      if (st && !s.cashBad[k]) st.realized = Math.round(((st.realized ?? 0) + (s.cash[k] ?? 0)) * 100) / 100;
      delete s.cash[k];
      delete s.cashBad[k];
    }
  }

  private applyFill(s: State, o: Order, delta: number) {
    if (delta <= 0) return;
    o.alloc ??= {};
    if (o.strategy !== "*") {
      const k = hk(o.strategy, o.symbol);
      s.holdings[k] = (s.holdings[k] ?? 0) + sideOf(o.action) * delta;
      o.alloc[k] = (o.alloc[k] ?? 0) + delta;
      return;
    }
    // A kill's close: take it from whichever strategies hold the symbol.
    let left = delta;
    for (const k of Object.keys(s.holdings)) {
      if (k.split("|")[1] !== o.symbol || !left) continue;
      const q = s.holdings[k];
      const take = Math.min(Math.abs(q), left);
      if (!take || Math.sign(q) === sideOf(o.action)) continue;
      s.holdings[k] = q + sideOf(o.action) * take;
      o.alloc[k] = (o.alloc[k] ?? 0) + take;
      left -= take;
    }
  }

  private async poll(s: State, t: TradeZero) {
    if (!s.accountId) return;
    const acct = s.accountId;
    const now = this.deps.now();
    const open = Object.values(s.orders).filter((o) => isOpenStatus(o.status));
    if (open.length) {
      const list = await t.orders(acct);
      for (const o of open) {
        const b = list.find((x) => x.coid === o.coid);
        if (!b) continue;
        this.track(s, o, b);
        if (o.cancelAt && now >= o.cancelAt && isOpenStatus(o.status)) {
          await t.cancel(acct, o.coid);
          o.status = "Canceled";
        }
      }
    }
    for (const o of Object.values(s.orders)) {
      if (o.status !== "Unknown") continue;
      const b = await t.getOrder(acct, o.coid);
      if (!b) {
        if (now - (o.unknownSince ?? now) > UNKNOWN_WATCH_MS) o.status = "Gone";
        continue;
      }
      this.track(s, o, b);
      if (!o.afterKill) continue;
      // It reached TradeZero after a kill: it must not stay on a killed robot.
      if (isOpenStatus(b.status)) {
        await t.cancel(acct, o.coid);
        o.status = "Canceled";
      }
      if (o.filled > 0) {
        const pos = (await t.positions(acct)).find((p) => p.symbol === o.symbol);
        if (pos && pos.qty !== 0) await this.closeQty(s, t, "*", o.symbol, pos.qty, pos.price);
      }
    }
    // Finished orders whose price hasn't come yet.
    for (const o of Object.values(s.orders)) {
      if (isOpenStatus(o.status) || o.status === "Unknown" || !lacksPrice(o)) continue;
      const b = await t.getOrder(acct, o.coid);
      if (b) this.track(s, o, b);
    }
    this.settle(s);
  }

  // ── reconcile ─────────────────────────────────────────────────────────────
  reconcile(): Promise<void> {
    return this.run(async (ctrl) => {
      const s = await this.load();
      try { await this.reconcileNow(s, ctrl); } finally { await this.save(s); }
    });
  }

  private async reconcileNow(s: State, ctrl: AbortController) {
    if (!s.keys || !s.accountId) return;
    const t = this.tz(s, ctrl);
    s.lastReconcile = this.deps.now();
    this.clearDailyPause(s, s.lastReconcile);
    try {
      await this.checkAccount(s, t);
    } catch {
      /* a failed re-check keeps the last verdict */
    }
    await this.poll(s, t);
    if (Object.values(s.orders).some((o) => o.status === "Unknown")) return;
    const positions = await t.positions(s.accountId);
    const ours: Record<string, number> = {};
    for (const [k, q] of Object.entries(s.holdings)) {
      const sym = k.split("|")[1];
      ours[sym] = (ours[sym] ?? 0) + q;
    }
    const theirs: Record<string, number> = {};
    for (const p of positions) theirs[p.symbol] = p.qty;
    const diff = [...new Set([...Object.keys(ours), ...Object.keys(theirs)])]
      .filter((sym) => (ours[sym] ?? 0) !== (theirs[sym] ?? 0))
      .map((sym) => ({ symbol: sym, ledger: ours[sym] ?? 0, broker: theirs[sym] ?? 0 }));
    if (diff.length) {
      s.drift = diff;
      if (!s.paused || s.paused.reason === "daily_loss") s.paused = { reason: "drift", date: ny(s.lastReconcile).date };
    }
  }

  // ── kill ──────────────────────────────────────────────────────────────────
  /** Doesn't wait in line: stops the running call and runs next. */
  kill(flatten: boolean): Promise<{ closeByHand: string[] }> {
    this.current?.abort();
    return this.run(async (ctrl) => {
      const s = await this.load();
      s.paused = { reason: "kill", date: ny(this.deps.now()).date };
      for (const o of Object.values(s.orders)) if (o.status === "Unknown") o.afterKill = true;
      if (!s.keys || !s.accountId) {
        await this.save(s);
        return { closeByHand: [] };
      }
      const t = this.tz(s, ctrl);
      try {
        await t.cancelAll(s.accountId);
        for (const o of Object.values(s.orders)) if (isOpenStatus(o.status)) o.status = "Canceled";
        if (flatten && !this.blocked(s)) {
          for (const p of await t.positions(s.accountId)) {
            if (p.qty !== 0) await this.closeQty(s, t, "*", p.symbol, p.qty, p.price);
          }
        }
      } finally {
        await this.save(s);
      }
      return { closeByHand: s.closeByHand };
    }, true);
  }

  // ── alarm ─────────────────────────────────────────────────────────────────
  /** One timer tick. Returns ms until the next one: 2 s while orders are open, else 60 s. */
  async tick(): Promise<number> {
    const next = await this.tickNow();
    await this.reportBroker();
    return next;
  }

  private tickNow(): Promise<number> {
    return this.run(async (ctrl) => {
      const s = await this.load();
      if (!s.keys) return 60000;
      try {
        if (this.deps.now() - s.lastReconcile >= 60000) await this.reconcileNow(s, ctrl);
        else await this.poll(s, this.tz(s, ctrl));
      } catch {
        /* the next tick tries again */
      } finally {
        await this.save(s);
      }
      return Object.values(s.orders).some((o) => isOpenStatus(o.status) || o.status === "Unknown") ? 2000 : 60000;
    });
  }
}

/** The test clock is honoured only away from the real TradeZero host. */
export function clockFor(env: { TEST_NOW?: string; TEST_EPOCH?: string; TZ_BASE_URL?: string }): () => number {
  const real = () => Date.now();
  if (!env.TEST_NOW) return real;
  let host = REAL_HOST;
  try { host = new URL(env.TZ_BASE_URL || `https://${REAL_HOST}`).hostname; } catch { /* treat as real */ }
  if (host === REAL_HOST) return real;
  const shift = Date.parse(env.TEST_NOW) - Number(env.TEST_EPOCH || Date.now());
  return () => Date.now() + shift;
}
