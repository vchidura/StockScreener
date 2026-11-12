/**
 * Nov 12 (user): pages read when new data lands instead of on a plain timer.
 *
 * Measured on the Nov 12 session:
 * - a 1-minute stock bar is stored about 2 s after its minute ends (p90 2.4 s, max 2.5 s);
 * - a 5-minute Equity window (ticker workspaces, charts) publishes 41-71 s after its 5-minute mark (the day's first
 *   window about 200 s);
 * - the Options chain capture lands 47-130 s after the mark and the Options window completes 60-121 s after it.
 *
 * So the developing candle is read a few seconds after each minute, and a 5-minute page is first checked shortly
 * before its window usually lands, then every few seconds until it does, then not again until the next mark.
 */
export const MINUTE_MS = 60_000
export const WINDOW_MS = 5 * MINUTE_MS
/** 1-minute bars are stored ~2 s (max 2.5 s) after the minute; read 5 s after it. */
export const MINUTE_READ_DELAY_MS = 5_000
/** Equity windows publish 41-71 s after the mark. */
export const EQUITY_FIRST_CHECK_MS = 40_000
/** Options captures land 47-130 s and windows complete 60-121 s after the mark. */
export const OPTIONS_FIRST_CHECK_MS = 50_000
/** While the expected window is being built. */
export const CATCH_UP_POLL_MS = 10_000
/** A window still missing this long after its mark is late: check less often. */
export const LATE_AFTER_MS = 4 * MINUTE_MS
export const LATE_POLL_MS = 30_000
/** A window normally lands within this time after its mark; later than this the page says the data is not current
 * (before it, the page shows no message). */
export const NORMAL_BUILD_MS = 150_000

const floorTo = (ms: number, step: number) => Math.floor(ms / step) * step

/** Delay until the next developing-candle read: 5 s after the next minute boundary. */
export function msUntilMinuteRead(nowMs: number, delayMs = MINUTE_READ_DELAY_MS): number {
  const next = floorTo(nowMs - delayMs, MINUTE_MS) + MINUTE_MS + delayMs
  return Math.max(1_000, next - nowMs)
}

/**
 * Delay until the next read of a page built every 5 minutes, given the time of the newest window it shows
 * (`dataMs`, the window's cutoff). `idleMs` applies when no live window is expected: no data yet, or data more than
 * two windows old (before the open, after the close, a past session, or a stalled pipeline).
 */
export function msUntilWindowRead(nowMs: number, dataMs: number | null, firstCheckMs: number, idleMs: number): number {
  if (dataMs === null || !Number.isFinite(dataMs)) return idleMs
  const mark = floorTo(nowMs, WINDOW_MS)
  if (dataMs >= mark) return Math.max(1_000, mark + WINDOW_MS + firstCheckMs - nowMs)
  if (mark - dataMs > 2 * WINDOW_MS) return idleMs
  const due = mark + firstCheckMs
  if (nowMs < due) return Math.max(1_000, due - nowMs)
  return nowMs - mark >= LATE_AFTER_MS ? LATE_POLL_MS : CATCH_UP_POLL_MS
}

/**
 * While the page shows the previous window and the current one is within its normal build time, the time it is
 * being updated to (the current mark); otherwise null (the data is current, or the window is late).
 */
export function windowBuildingTo(nowMs: number, dataMs: number | null): number | null {
  if (dataMs === null || !Number.isFinite(dataMs)) return null
  const mark = floorTo(nowMs, WINDOW_MS)
  return dataMs === mark - WINDOW_MS && nowMs - mark < NORMAL_BUILD_MS ? mark : null
}

/** True while a page shows the previous window and the next one is within its normal build time (no message then). */
export function marketWindowBuilding(nowMs: number, marketTime?: string | null): boolean {
  return windowBuildingTo(nowMs, windowTimeMs(marketTime)) !== null
}

/** The window a page shows, from its newest timestamps: the earliest of them, each floored to its 5-minute mark. */
export function windowTimeMs(...stamps: (string | null | undefined)[]): number | null {
  const times = stamps
    .filter((stamp): stamp is string => typeof stamp === 'string' && stamp.length > 0)
    .map(stamp => Date.parse(stamp))
    .filter(Number.isFinite)
  return times.length ? floorTo(Math.min(...times), WINDOW_MS) : null
}
