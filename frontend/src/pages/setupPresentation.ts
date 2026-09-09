/** Shared presentation for published trade-setup state.
 *
 *  The setup payload is immutable published analysis, so wording is corrected here at
 *  render time rather than in the classifier. Every page that shows these fields must
 *  use these helpers, otherwise the same ticker reads differently per page.
 */

/** Wilder's no-trend line. Below it the EMA ordering carries no directional meaning. */
export const NO_TREND_ADX = 20

/**
 * `ema_alignment.primary` reports EMA ordering only, with no strength term, so a stack
 * is shown as a direction only when ADX confirms a trend exists.
 */
export function trendLabel(
  primary: string | null | undefined,
  adx: number | null | undefined,
): string | null {
  if (!primary) return null
  if (adx != null && adx < NO_TREND_ADX) return 'Neutral'
  switch (primary) {
    case 'Bullish Stack': return 'Bullish'
    case 'Bearish Stack': return 'Bearish'
    case 'Short-term Bullish': return 'Mixed ↑'
    case 'Short-term Bearish': return 'Mixed ↓'
    default: return primary
  }
}

export function trendTitle(
  primary: string | null | undefined,
  detail: string | null | undefined,
  adx: number | null | undefined,
): string | undefined {
  if (!primary) return undefined
  if (adx != null && adx < NO_TREND_ADX) {
    return `ADX ${adx.toFixed(1)} is below ${NO_TREND_ADX}, so there is no measurable trend strength. EMA structure: ${detail ?? primary}`
  }
  return detail ?? primary
}

/** `momentum.state` is the price/50MA/200MA quadrant, so say which MAs it is measuring. */
export function momentumTitle(state: string | null | undefined, detail: string | null | undefined): string | undefined {
  if (!state) return undefined
  const position = state.includes('Strong')
    ? `${state.includes('Up') ? 'Price above' : 'Price below'} both the 50 and 200 MA`
    : state === 'Uptrend' ? 'Price above the 50 MA but not the 200 MA'
    : state === 'Downtrend' ? 'Price below the 50 MA but not the 200 MA'
    : null
  const base = position ? `${position}. This is moving-average position, not move strength — see ADX for that.` : null
  return [base, detail].filter(Boolean).join(' ') || undefined
}
