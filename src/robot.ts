// The robot: checks each signal, sizes it from the strategy's balance, places it once, tracks fills, reconciles, flattens.
// Everything runs one at a time through one queue, because a Durable Object's requests interleave during a fetch.
import { isClose, isEntry, type Signal } from "./signal.ts";
import { inHours, isRegular, ny, sessionRule } from "./session.ts";
import { sizeShares } from "./size.ts";
import { Limiter, REAL_HOST, TradeZero, TzAborted, TzBusy, TzError, TzTimeout, isOpenStatus, pickRoute, type BrokerOrder, type Deps, type RouteRow } from "./tz.ts";

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
const REPORT_MS = 5000;  // reports are running totals: a missed one is simply sent again
const REPORT_BACKOFF_MS = 60000;
const BEAT_MS = 55000;
const CANCEL_LOOKS = 5;
const CANCEL_LOOK_MS = 500;
const FILLS_PER_REPORT = 100;
const KEEP_ORDERS = 200;

interface Order {
  coid: string; strategy: string; symbol: string; action: string; qty: number; filled: number;
  status: string; cancelAt: number | null; unknownSince?: number; afterKill?: boolean;
  // Filled shares per "strategy|SYMBOL", what's been booked for them, and keys no longer waiting for a price.
  alloc?: Record<string, number>; booked?: Record<string, number>; avg?: number | null; waived?: Record<string, true>;
  at?: number; filledAt?: number; sent?: string; byHand?: boolean; // first fill, latest fill, what the engine last took
  goneSince?: number; // a cancelled order missing from the broker's list since then
  closeAfter?: boolean; // cancelled by a flatten: when it ends, whatever is still open gets closed
  cancelAsked?: boolean; // the robot asked for this cancel, so ending short isn't a failure
  mustClose?: boolean; // a flatten's own close
  closeChecked?: boolean; // its ending has been looked at
}
interface State {
  keys: { id: string; secret: string; hash: string } | null;
  verdict: { paper: boolean; keyHash: string; reason: string } | null;
  accountId: string | null;
  routes: RouteRow[] | null;
  readBad: boolean; // the broker's positions answer can't be trusted: entries stop, and the owner is told
  otherPositions: number;
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
  outside: Record<string, number>; // symbol → shares at the broker no strategy owns, as the owner accepted at Resume
  unreportedDropped: number;
}

const fresh = (): State => ({
  keys: null, verdict: null, accountId: null, routes: null, readBad: false, otherPositions: 0, skewBad: false, limits: { ...DEFAULT_LIMITS }, strategies: {}, holdings: {}, cash: {}, cashBad: {},
  orders: {}, answers: {}, paused: null, drift: null, closeByHand: [], lastReconcile: 0, outside: {}, unreportedDropped: 0,
});

export interface Ack { status: "placed" | "rejected" | "unknown" | "done" | "gone"; reason?: string; coid?: string; qty?: number; closeByHand?: string[]; repeat?: boolean }

// Worth trying again with the same id, so never stored as that id's answer.
const RETRYABLE = new Set(["expired", "tz_busy", "outside_hours", "clock_skew", "session_rule", "balance_unknown", "broker_unreadable"]);

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
const orderSig = (o: Order) => `${o.filled}|${o.avg ?? ""}`;
const unreported = (o: Order) => Object.keys(o.alloc ?? {}).length > 0 && o.sent !== orderSig(o);
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
  private reporting: Promise<void> | null = null;
  private reportAfter = 0;

  constructor(store: Store, base: string, deps: Deps, engineUrl: string | null = null) {
    this.store = store;
    this.base = base;
    this.deps = deps;
    this.engineUrl = engineUrl ? engineUrl.replace(/\/$/, "") : null;
  }

  // ── pairing with TradeReplay ──────────────────────────────────────────────
  private async callEngine(path: string, body: unknown, token?: string, limit = ENGINE_MS): Promise<{ status: number; json: any }> {
    if (!this.engineUrl) throw new Error("no engine address");
    const res = await this.deps.fetch(`${this.engineUrl}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}) },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(limit),
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

  /** The heartbeat: about once a minute, and at once when something in it changed. Throws when the engine can't take it. */
  private async beat(token: string, state: { broker: string | null; paused: string | null; closeByHand: string[] }, force = false): Promise<void> {
    const sig = JSON.stringify(state);
    const last = await this.store.get(PAIR_REPORTED);
    if (!force && last?.sig === sig && this.deps.now() - last.at < BEAT_MS) return;
    const body = { paused: state.paused, closeByHand: state.closeByHand, ...(state.broker ? { broker: state.broker } : {}) };
    const r = await this.callEngine("/v1/robots/me/status", body, token, REPORT_MS);
    if (r.status !== 200) throw new Error(`status ${r.status}`);
    await this.store.put(PAIR_REPORTED, { sig, at: this.deps.now() });
  }

  private healthOf(s: State) {
    return { broker: !s.keys ? null : this.blocked(s) ? "not_paper" : "connected", paused: s.paused?.reason ?? (s.readBad ? "broker_unreadable" : null), closeByHand: s.closeByHand };
  }

  /** Right after a key is saved, so the page can move on. A failure is left to the next tick. */
  async reportBroker(): Promise<void> {
    const token = await this.pairedToken();
    if (!token || !this.engineUrl) return;
    try {
      await this.beat(token, this.healthOf(await this.load()), true);
    } catch { /* the next tick tries again */ }
  }

  /** At most one at a time, and never waited for by the order loop. Only the queue ever saves State: rows are read in it,
   *  sent outside it, and marked in a new job, so a slow engine can't delay a poll or wipe an order placed meanwhile. */
  startReports(): Promise<void> {
    if (this.reporting) return this.reporting;
    if (!this.engineUrl || this.deps.now() < this.reportAfter) return Promise.resolve();
    this.reporting = this.sendReports()
      .catch(() => { this.reportAfter = this.deps.now() + REPORT_BACKOFF_MS; })
      .finally(() => { this.reporting = null; });
    return this.reporting;
  }

  private async sendReports(): Promise<void> {
    const token = await this.pairedToken();
    if (!token) return;
    const { rows, sigs, health } = await this.run(async () => {
      const s = await this.load();
      const rows: any[] = [];
      const sigs: Record<string, string> = {};
      for (const o of Object.values(s.orders)) {
        if (!unreported(o) || rows.length >= FILLS_PER_REPORT) continue;
        const keys = Object.entries(o.alloc ?? {});
        if (rows.length + keys.length > FILLS_PER_REPORT) continue;
        sigs[o.coid] = orderSig(o);
        for (const [k, shares] of keys) {
          const [strategy, symbol] = k.split("|");
          rows.push({
            coid: o.coid, strategy, symbol, action: o.action, shares, avg: o.avg ?? null,
            at: (o.at ?? this.deps.now()) / 1000, filled_at: (o.filledAt ?? o.at ?? this.deps.now()) / 1000, ...(o.byHand ? { by_hand: true } : {}),
          });
        }
      }
      return { rows, sigs, health: this.healthOf(s) };
    });
    await this.beat(token, health);
    if (!rows.length) return;
    const r = await this.callEngine("/v1/robots/me/fills", { fills: rows }, token, REPORT_MS);
    if (r.status !== 200) throw new Error(`fills ${r.status}`);
    await this.run(async () => {
      const s = await this.load();
      for (const [coid, sig] of Object.entries(sigs)) if (s.orders[coid]) s.orders[coid].sent = sig;
      await this.save(s);
    });
  }

  reportsDone(): Promise<void> {
    return this.reporting ?? Promise.resolve();
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
    let excess = done.length - KEEP_ORDERS;
    // Orders the engine already has go first. Past that the oldest report is lost, and counted.
    for (const o of [...done.filter((x) => !unreported(x)), ...done.filter(unreported)]) {
      if (excess <= 0) break;
      if (unreported(o)) s.unreportedDropped++;
      delete s.orders[o.coid];
      excess--;
    }
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
      s.routes = null;
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

  /** After a drift or kill, the owner's Resume accepts what the broker holds (docs/LIVE_RESULTS_PLAN.md D18). */
  resume(): Promise<{ ok: boolean; error?: string; waiting?: string[] }> {
    return this.run(async (ctrl) => {
      const s = await this.load();
      const waiting: string[] = [];
      if (s.paused && s.paused.reason !== "daily_loss" && s.keys && s.accountId) {
        const t = this.tz(s, ctrl);
        let checked: Array<{ symbol: string; qty: number }> | null = null;
        try {
          await this.poll(s, t);
          checked = (await t.positionsRead(s.accountId)).checked;
        } catch { /* a failed read, below */ }
        if (!checked) {
          await this.save(s);
          return { ok: false, error: "Could not read your positions at TradeZero. Still paused." };
        }
        const broker: Record<string, number> = {};
        for (const p of checked) broker[p.symbol] = (broker[p.symbol] ?? 0) + p.qty;
        const ledger = Object.keys(s.holdings).map((k) => k.split("|")[1]);
        for (const symbol of new Set([...ledger, ...Object.keys(s.outside), ...Object.keys(broker)])) {
          if (this.ordersOn(s, symbol)) waiting.push(symbol);
          else this.acceptBroker(s, symbol, broker[symbol] ?? 0);
        }
        this.settle(s);
      }
      s.paused = null;
      s.drift = null;
      await this.save(s);
      return waiting.length ? { ok: true, waiting } : { ok: true };
    });
  }

  private ordersOn(s: State, symbol: string): boolean {
    return Object.values(s.orders).some((o) => o.symbol === symbol && (isOpenStatus(o.status) || o.status === "Unknown"));
  }

  /** The ledger shrinks towards the broker, so a later close can't open the other way; anything else is `outside`. */
  private acceptBroker(s: State, symbol: string, target: number) {
    const keys = Object.keys(s.holdings).filter((k) => k.split("|")[1] === symbol && s.holdings[k]);
    const sum = keys.reduce((n, k) => n + s.holdings[k], 0);
    const keep = sum > 0 ? Math.min(Math.max(target, 0), sum) : Math.max(Math.min(target, 0), sum);
    const shrink = Math.abs(sum - keep);
    if (shrink > 0) {
      const sign = Math.sign(sum);
      const same = keys.filter((k) => Math.sign(s.holdings[k]) === sign);
      const total = same.reduce((n, k) => n + Math.abs(s.holdings[k]), 0);
      // Whole shares, proportional, largest remainder first.
      const parts = same.map((k) => ({ k, exact: (Math.abs(s.holdings[k]) * shrink) / total }));
      const takes = new Map(parts.map((p) => [p.k, Math.floor(p.exact)]));
      let left = shrink - [...takes.values()].reduce((a, b) => a + b, 0);
      for (const p of [...parts].sort((a, b) => (b.exact % 1) - (a.exact % 1))) {
        if (left <= 0) break;
        takes.set(p.k, (takes.get(p.k) as number) + 1);
        left--;
      }
      const now = this.deps.now();
      for (const [k, take] of takes) {
        if (!take) continue;
        s.holdings[k] -= sign * take;
        s.cashBad[k] = true;
        const coid = `hand-${symbol}-${now}-${Object.keys(s.orders).length}`.slice(0, 36);
        s.orders[coid] = {
          coid, strategy: k.split("|")[0], symbol, action: sign > 0 ? "sell" : "cover", qty: take, filled: take, status: "ByHand", cancelAt: null,
          alloc: { [k]: take }, avg: null, waived: { [k]: true }, at: now, filledAt: now, byHand: true,
        };
      }
    }
    const rest = target - keep;
    if (rest) s.outside[symbol] = rest; else delete s.outside[symbol];
  }

  status(): Promise<any> {
    return this.run(async () => {
      const s = await this.load();
      const strategies = Object.fromEntries(Object.entries(s.strategies).map(([id, st]) => [id, { capital: st.capital, balance: balanceOf(st), balanceUnknown: this.balanceUnknown(s, id) }]));
      return {
        broker: s.keys ? "connected" : null, paper: s.verdict?.paper ?? false, why: this.blocked(s) ? s.verdict?.reason || this.blocked(s) : null,
        clockSkew: s.skewBad, paused: s.paused, drift: s.drift, limits: s.limits, strategies, holdings: s.holdings,
        orders: Object.values(s.orders), closeByHand: s.closeByHand, outside: s.outside, unreportedDropped: s.unreportedDropped, otherPositions: s.otherPositions, brokerUnreadable: s.readBad,
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
      const target = s.orders[sig.ref as string];
      if (target) await this.askCancel(t, acct, target);
      else await t.cancel(acct, sig.ref as string);
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
    if (s.readBad) return reject("broker_unreadable");
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

  /** The route taking this order, or null: no order then. Routes are read once per key. */
  private async route(s: State, t: TradeZero, type: string, tif: string): Promise<string | null> {
    if (!s.routes) s.routes = await t.routes(s.accountId as string);
    return pickRoute(s.routes, type, tif);
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
    const outcome = (r: { status: string | null; text: string | null }): Ack => {
      if (r.status !== "Rejected" || r.text?.includes("R114")) return placed();
      return { status: "rejected", reason: `tz_rejected: ${r.text ?? "no reason given"}` };
    };
    try {
      const route = await this.route(s, t, f.type, f.tif);
      if (!route) return { status: "rejected", reason: "no_route" };
      body.route = route;
      return outcome(await t.place(acct, body));
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
      const found = await t.getOrder(acct, order.coid);
      if (found) return outcome({ status: found.status, text: found.text });
    } catch {
      return unknown();
    }
    try {
      return outcome(await t.place(acct, body));
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
        await this.askCancel(t, s.accountId as string, o);
        o.closeAfter = true;
      }
    }
    await this.awaitCancels(s, t);
    const byHand: string[] = [];
    for (const [k, q] of Object.entries(s.holdings)) {
      const [st, symbol] = k.split("|");
      if (st !== strategy || !q) continue;
      await this.closeQty(s, t, st, symbol, q, await this.priceOf(t, s.accountId as string, symbol));
      if (s.closeByHand.includes(symbol)) byHand.push(symbol);
    }
    return byHand.length ? { status: "done", closeByHand: byHand } : { status: "done" };
  }

  /** Market in regular hours; a limit at the position's price ± 2% outside them, since market orders are refused there. */
  private async closeQty(s: State, t: TradeZero, strategy: string, symbol: string, wanted: number, price: number | null) {
    // Never on a key that isn't paper, whichever path asked.
    if (this.blocked(s)) {
      if (!s.closeByHand.includes(symbol)) s.closeByHand.push(symbol);
      return;
    }
    // Shares a close already working will take aren't closed twice: two closes would flip the position.
    const side = wanted > 0 ? "sell" : "cover";
    const working = Object.values(s.orders)
      .filter((o) => o.symbol === symbol && o.action === side && (isOpenStatus(o.status) || o.status === "Unknown") && (strategy === "*" || o.strategy === strategy || o.strategy === "*"))
      .reduce((n, o) => n + Math.max(0, o.qty - o.filled), 0);
    const left = Math.abs(wanted) - working;
    if (left <= 0) return;
    const signed = Math.sign(wanted) * left;
    const now = this.deps.now();
    const regular = isRegular(now);
    if (!regular && price === null) {
      if (!s.closeByHand.includes(symbol)) s.closeByHand.push(symbol);
      return;
    }
    const coid = `x-${symbol}-${now}-${Object.keys(s.orders).length}`.slice(0, 36);
    const action = signed > 0 ? "sell" : "cover";
    const limit = regular ? null : Math.round((signed > 0 ? (price as number) * 0.98 : (price as number) * 1.02) * 100) / 100;
    const order: Order = { coid, strategy, symbol, action, qty: Math.abs(signed), filled: 0, status: "New", cancelAt: null, mustClose: true };
    const ack = await this.sendOnce(s, t, order, { side: signed > 0 ? "Sell" : "Buy", openClose: "Close", type: regular ? "market" : "limit", limit, tif: "day" }, this.current ?? new AbortController());
    // A close that didn't get placed must not read as done: the position is still open.
    if (ack.status !== "placed" && ack.status !== "unknown" && !s.closeByHand.includes(symbol)) s.closeByHand.push(symbol);
  }

  // ── orders ────────────────────────────────────────────────────────────────
  /** A cancel's 200 only says the request arrived, so the order stays watched until the broker says it ended. */
  private async askCancel(t: TradeZero, acct: string, o: Order) {
    if (o.status === "Rejected") return; // cancelling one overwrites its rejection reason
    o.cancelAt = null;
    o.cancelAsked = true;
    try {
      await t.cancel(acct, o.coid);
    } catch (err) {
      if (!(err instanceof TzError && err.status === 404)) throw err;
    }
    o.status = "PendingCancel";
  }

  /** A flatten's cancel lost the race to a fill. A flatten means flat, so that close goes out too. */
  private async closeRest(s: State, t: TradeZero, o: Order) {
    const acct = s.accountId as string;
    if (o.strategy === "*" || s.paused?.reason === "kill") {
      const pos = (await t.positions(acct)).find((p) => p.symbol === o.symbol);
      if (pos && pos.qty !== 0) await this.closeQty(s, t, "*", o.symbol, pos.qty, pos.price);
      return;
    }
    const held = s.holdings[hk(o.strategy, o.symbol)] ?? 0;
    if (held) await this.closeQty(s, t, o.strategy, o.symbol, held, await this.priceOf(t, acct, o.symbol));
  }

  /** Only a limit outside regular hours needs it. A failed read means no price, never a failed close. */
  private async priceOf(t: TradeZero, acct: string, symbol: string): Promise<number | null> {
    try {
      return (await t.positions(acct)).find((p) => p.symbol === symbol)?.price ?? null;
    } catch {
      return null;
    }
  }

  /** A few short looks, so a close sent next uses shares filled just before the cancel. A later fill reads as drift, for Resume. */
  private async awaitCancels(s: State, t: TradeZero) {
    for (let i = 0; i < CANCEL_LOOKS; i++) {
      if (!Object.values(s.orders).some((o) => o.status === "PendingCancel")) return;
      await this.deps.sleep(CANCEL_LOOK_MS, t.signal);
      await this.poll(s, t);
    }
  }

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
    const at = this.deps.now();
    o.at ??= at;
    o.filledAt = at;
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
        let b = list.find((x) => x.coid === o.coid) ?? null;
        if (!b && o.status === "PendingCancel") b = await t.getOrder(acct, o.coid);
        if (!b) {
          // A 404 can also mean not registered yet, so it only counts as ended once it has held.
          if (o.status === "PendingCancel") {
            o.goneSince ??= now;
            if (now - o.goneSince > UNKNOWN_WATCH_MS) {
              o.status = "Canceled";
              await this.finishClose(s, t, o);
            }
          }
          continue;
        }
        delete o.goneSince;
        const wasCancelling = o.status === "PendingCancel";
        this.track(s, o, b);
        if (wasCancelling && isOpenStatus(o.status)) o.status = "PendingCancel";
        await this.finishClose(s, t, o);
        if (o.cancelAt && now >= o.cancelAt && isOpenStatus(o.status)) await this.askCancel(t, acct, o);
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
      o.closeAfter = true;
      if (isOpenStatus(b.status)) await this.askCancel(t, acct, o);
      await this.finishClose(s, t, o);
    }
    // Finished orders whose price hasn't come yet.
    for (const o of Object.values(s.orders)) {
      if (isOpenStatus(o.status) || o.status === "Unknown" || !lacksPrice(o)) continue;
      const b = await t.getOrder(acct, o.coid);
      if (b) this.track(s, o, b);
    }
    this.noteFailedCloses(s);
    this.settle(s);
  }

  /** The broker has ended an order a flatten cancelled: close what it filled, or what it was itself going to close. */
  private async finishClose(s: State, t: TradeZero, o: Order) {
    if (!o.closeAfter || isOpenStatus(o.status) || o.status === "Unknown") return;
    o.closeAfter = false;
    if (o.filled <= 0 && !isClose(o.action as any)) return;
    try {
      await this.closeRest(s, t, o);
    } catch {
      if (!s.closeByHand.includes(o.symbol)) s.closeByHand.push(o.symbol); // never lost: the owner sees it
    }
  }

  /** A close accepted, then ended short with no cancel from us, leaves shares open while everything else agrees. Say so.
   *  A limit exit a strategy sent can simply expire at the day's end: only a flat refusal or loss counts there. */
  private noteFailedCloses(s: State) {
    for (const o of Object.values(s.orders)) {
      if (o.closeChecked || o.byHand || !isClose(o.action as any)) continue;
      if (isOpenStatus(o.status) || o.status === "Unknown") continue;
      o.closeChecked = true;
      if (o.filled >= o.qty || o.cancelAsked) continue;
      if (o.mustClose || o.status === "Rejected" || o.status === "Gone") {
        if (!s.closeByHand.includes(o.symbol)) s.closeByHand.push(o.symbol);
      }
    }
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
    let read;
    try {
      read = await t.positionsRead(s.accountId);
    } catch (err) {
      if (err instanceof TzError && err.code === "bad_shape") s.readBad = true;
      throw err;
    }
    const { checked, other } = read;
    // An answer we can't read says nothing about the ledger: no drift from it, and entries stop until it reads again.
    s.readBad = !checked;
    if (!checked) return;
    s.otherPositions = other;
    // The robot asked for these hand-closes, so they aren't a mismatch.
    for (const symbol of [...s.closeByHand]) {
      if (checked.some((p) => p.symbol === symbol && p.qty !== 0) || this.ordersOn(s, symbol)) continue;
      this.acceptBroker(s, symbol, 0);
      s.closeByHand = s.closeByHand.filter((x) => x !== symbol);
    }
    this.settle(s);
    const ours: Record<string, number> = { ...s.outside };
    for (const [k, q] of Object.entries(s.holdings)) {
      const sym = k.split("|")[1];
      ours[sym] = (ours[sym] ?? 0) + q;
    }
    const theirs: Record<string, number> = {};
    for (const p of checked) theirs[p.symbol] = p.qty;
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
        for (const o of Object.values(s.orders)) if (isOpenStatus(o.status)) { o.status = "PendingCancel"; o.cancelAt = null; o.cancelAsked = true; o.closeAfter = flatten; }
        await this.awaitCancels(s, t);
        if (flatten && !this.blocked(s)) {
          for (const p of await t.positions(s.accountId)) {
            if (p.qty !== 0) await this.closeQty(s, t, "*", p.symbol, p.qty, p.price);
          }
        }
      } catch (err) {
        // A failed read can't say what's open. The robot stays paused, and the owner sees it.
        if (!(err instanceof TzError || err instanceof TzBusy || err instanceof TzTimeout)) throw err;
        for (const symbol of new Set(Object.keys(s.holdings).filter((k) => s.holdings[k]).map((k) => k.split("|")[1]))) {
          if (flatten && !s.closeByHand.includes(symbol)) s.closeByHand.push(symbol);
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
    void this.startReports();
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
