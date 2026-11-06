// TradeZero's session rules, by New York time (DST handled by Intl).
import type { OrderType, Tif } from "./signal.ts";

const FMT = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
});

export function ny(ms: number): { date: string; minutes: number } {
  const p = Object.fromEntries(FMT.formatToParts(new Date(ms)).map((x) => [x.type, x.value]));
  return { date: `${p.year}-${p.month}-${p.day}`, minutes: Number(p.hour) * 60 + Number(p.minute) };
}

const OPEN = 9 * 60 + 30;
const CLOSE = 16 * 60;
const PRE = 4 * 60;

export const isRegular = (ms: number) => {
  const m = ny(ms).minutes;
  return m >= OPEN && m < CLOSE;
};

/** Reason this order can't be sent now, or null. Paper account rules: stop_limit runs in regular hours only. */
export function sessionRule(type: OrderType, tif: Tif, ms: number): string | null {
  const m = ny(ms).minutes;
  const regular = m >= OPEN && m < CLOSE;
  if ((tif === "day_plus" || tif === "gtc_plus") && type !== "limit" && type !== "stop_limit") return "session_rule";
  if ((type === "market" || type === "stop" || type === "stop_limit") && !regular) return "session_rule";
  if (type === "limit" && m < PRE) return "session_rule";
  return null;
}

export function inHours(ms: number, from: number, to: number): boolean {
  const m = ny(ms).minutes;
  return m >= from && m < to;
}
