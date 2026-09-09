export type ScanInterval = '5m' | '15m' | '30m' | '1h' | '1d'

export const SCAN_INTERVALS: ScanInterval[] = ['5m', '15m', '30m', '1h', '1d']

export interface IntervalSemantics {
  /** Human label for the candle size. */
  candle: string
  /** Higher-timeframe confirmation the backend actually evaluates. */
  higherTfLabel: string | null
  /** Long-baseline SMA the backend actually evaluates, or null when it is skipped. */
  baselineLabel: string | null
  /** The trend gate that actually decides admission at this interval. */
  trendRule: string
  /** Whether results come from the published daily snapshot or a live scan. */
  source: 'published' | 'live'
  /** Short caveat surfaced next to the interval picker. */
  caveat: string | null
}

/**
 * Mirrors the interval branching in `screeners.py`. The scanners change which
 * trend tests run per interval, so the page must not keep describing the daily
 * rules when a different set was applied.
 */
export function intervalSemantics(interval: ScanInterval): IntervalSemantics {
  if (interval === '1d') {
    return {
      candle: 'Daily candles',
      higherTfLabel: 'Weekly EMA stack',
      baselineLabel: '200-day SMA',
      trendRule: '≥2 stacked EMA pairs plus weekly stack or the 200-day SMA',
      source: 'published',
      caveat: null,
    }
  }
  if (interval === '1h') {
    return {
      candle: 'Hourly candles',
      higherTfLabel: 'Daily EMA stack',
      baselineLabel: '200-hour SMA',
      trendRule: '≥2 stacked EMA pairs plus the daily stack or the 200-hour SMA',
      source: 'live',
      caveat:
        'Higher-timeframe confirmation resamples hourly bars to daily, not weekly, and the ' +
        'baseline is a 200-hour SMA (roughly 30 sessions) rather than the 200-day SMA. ' +
        'Tickers with under 200 hourly bars record the baseline as not met.',
    }
  }
  return {
    candle: `${interval} candles (intraday)`,
    higherTfLabel: null,
    baselineLabel: null,
    trendRule: '≥3 of 4 stacked EMA pairs, evaluated on the intraday series alone',
    source: 'live',
    caveat:
      'No higher-timeframe or 200-period baseline test runs at this interval. The trend gate ' +
      'is tightened to ≥3 stacked EMA pairs instead. Those two columns read “not evaluated”.',
  }
}

/**
 * The scan endpoints only serve the materialized snapshot for an unfiltered 1d
 * request; every other combination triggers a live full-universe scan.
 */
export function isPublishedSnapshot(interval: ScanInterval, scanDate: string): boolean {
  return interval === '1d' && !scanDate
}
