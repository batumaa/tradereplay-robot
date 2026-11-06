/** Whole shares for pct % of this strategy's Capital, rounded down. 0 means too small to trade. */
export function sizeShares(pct: number, capital: number, price: number): number {
  return Math.floor(((pct / 100) * capital) / price + 1e-9);
}
