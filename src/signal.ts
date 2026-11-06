// Signal format: what the robot accepts. Holds % of capital, never shares, and no field names where prices come from.
export type Action = "buy" | "sell" | "short" | "cover" | "cancel" | "flatten";
export type OrderType = "market" | "limit" | "stop" | "stop_limit";
export type Tif = "day" | "day_plus" | "gtc" | "gtc_plus" | "ioc";

export interface Signal {
  id: string;
  seq: number;
  strategy: string;
  ts: string;
  valid_ms: number;
  action: Action;
  symbol?: string;
  pct?: number;
  price?: number;
  type?: OrderType;
  limit?: number | null;
  stop?: number | null;
  tif?: Tif;
  cancel_after_ms?: number | null;
  ref?: string;
}

const ACTIONS = ["buy", "sell", "short", "cover", "cancel", "flatten"];
const TYPES = ["market", "limit", "stop", "stop_limit"];
const TIFS = ["day", "day_plus", "gtc", "gtc_plus", "ioc"];
const pos = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v) && v > 0;

export const isEntry = (a: Action) => a === "buy" || a === "short";
export const isClose = (a: Action) => a === "sell" || a === "cover";

export function validateSignal(x: unknown): { ok: true; signal: Signal } | { ok: false; reason: string } {
  const bad = (why: string) => ({ ok: false as const, reason: `bad_signal: ${why}` });
  if (!x || typeof x !== "object") return bad("not an object");
  const s = x as Signal;
  if (typeof s.id !== "string" || !s.id || s.id.length > 36) return bad("id must be 1 to 36 characters");
  if (typeof s.strategy !== "string" || !s.strategy) return bad("strategy");
  if (typeof s.ts !== "string" || Number.isNaN(Date.parse(s.ts))) return bad("ts");
  if (!pos(s.valid_ms)) return bad("valid_ms");
  if (!ACTIONS.includes(s.action)) return bad("unknown action");
  if (s.action === "cancel") return typeof s.ref === "string" && s.ref ? { ok: true, signal: s } : bad("cancel needs ref");
  if (s.action === "flatten") return { ok: true, signal: s };
  if (typeof s.symbol !== "string" || !/^[A-Za-z.]{1,10}$/.test(s.symbol)) return bad("symbol");
  if (!TYPES.includes(s.type as string)) return bad("type");
  if (!TIFS.includes(s.tif as string)) return bad("tif");
  if ((s.type === "limit" || s.type === "stop_limit") && !pos(s.limit)) return bad("limit");
  if ((s.type === "stop" || s.type === "stop_limit") && !pos(s.stop)) return bad("stop");
  if (isEntry(s.action)) {
    if (!pos(s.pct)) return bad("pct");
    if (!pos(s.price)) return bad("price");
  }
  return { ok: true, signal: s };
}
