/**
 * G106 OV1 (Nov 16): display logic for the Options Overview page (dealer exposure per strike and expiry). Pure
 * functions only, so the node tests can load this file without React. The numbers come from
 * `/api/options/exposure`; nothing here changes them, it only selects, scales and labels.
 */

export type ExposureUnit = 'usd' | 'shares'
/** Net: one bar per strike. Calls vs puts: stacked call and put parts. Overlay (Nov 18): the calls vs puts bars with
 * the net per strike drawn as a line on top, so the gross walls and the net read together. */
export type ExposureView = 'net' | 'split' | 'overlay'
export type HeatmapMode = 'net' | 'call' | 'put'

const ETF_UNDERLYERS = new Set(['SPY', 'QQQ', 'IWM', 'DIA'])

/** Default strike window around spot: index ETFs list strikes every $1, single stocks are wider apart. */
export const defaultStrikeBand = (underlyer: string) => ETF_UNDERLYERS.has(underlyer.toUpperCase()) ? 0.05 : 0.1

/** Exposure in dollars, or in shares (dollars / spot), as the page's unit toggle chooses. */
export const inUnit = (value: number, unit: ExposureUnit, spot: number) => unit === 'shares' && spot > 0 ? value / spot : value

const compact = (value: number) => {
  const magnitude = Math.abs(value)
  const steps: Array<[number, string]> = [[1e12, 'T'], [1e9, 'B'], [1e6, 'M'], [1e3, 'K']]
  for (const [size, suffix] of steps) {
    if (magnitude >= size) {
      const scaled = magnitude / size
      return `${scaled >= 100 ? scaled.toFixed(0) : scaled >= 10 ? scaled.toFixed(1) : scaled.toFixed(2)}${suffix}`
    }
  }
  return magnitude >= 100 ? magnitude.toFixed(0) : magnitude.toFixed(magnitude >= 10 ? 1 : 2)
}

/** A value already in the display unit as compact text: $1.2B, −$840M, 6.2M sh (axis ticks; zero is plain 0). */
export function formatScaled(scaled: number, unit: ExposureUnit, signed = false): string {
  if (scaled === 0) return '0'
  const sign = scaled < 0 ? '−' : signed ? '+' : ''
  return unit === 'shares' ? `${sign}${compact(scaled)} sh` : `${sign}$${compact(scaled)}`
}

/** Signed compact text: +$1.83B, −$840M, +6.22M sh (`signed` false drops the plus sign). Zero is plain 0. */
export function formatExposure(value: number, unit: ExposureUnit, spot: number, signed = true): string {
  return formatScaled(inUnit(value, unit, spot), unit, signed)
}

/** Short form for heatmap cells: +1.8B, −840M (the unit is in the legend). */
export function formatCell(value: number, unit: ExposureUnit, spot: number): string {
  const scaled = inUnit(value, unit, spot)
  if (scaled === 0) return '0'
  return `${scaled > 0 ? '+' : '−'}${compact(scaled)}`
}

export const formatPercent = (fraction: number | null | undefined, digits = 1) =>
  fraction == null ? '—' : `${(fraction * 100).toFixed(digits)}%`

export const formatSignedPercent = (fraction: number | null | undefined, digits = 1) =>
  fraction == null ? '—' : `${fraction > 0 ? '+' : fraction < 0 ? '−' : ''}${Math.abs(fraction * 100).toFixed(digits)}%`

/** Strikes within `band` (a fraction of spot) on either side; `null` keeps every captured strike. */
export function strikesInBand<T extends { strike: string }>(rows: T[], spot: number, band: number | null): T[] {
  if (band == null || !(spot > 0)) return rows
  const low = spot * (1 - band), high = spot * (1 + band)
  return rows.filter(row => Number(row.strike) >= low && Number(row.strike) <= high)
}

/** Which expiries a bar part holds when the 0 DTE split is on: today's (0 DTE) or every later one. */
export type ExpiryPart = 'today' | 'later'
export type BarSegment = { kind: 'call' | 'put' | 'net'; from: number; to: number; value: number; part?: ExpiryPart }

/**
 * Bar segments for one strike. Net: one bar from zero. Calls vs puts (and Overlay, whose bars are the same): positive
 * parts stack up from zero and negative parts stack down, so gamma (calls up, puts down under the convention) and
 * delta (both up) both read correctly.
 */
export function strikeSegments(call: number, put: number, view: ExposureView): BarSegment[] {
  if (view === 'net') {
    const net = call + put
    return net === 0 ? [] : [{ kind: 'net', from: 0, to: net, value: net }]
  }
  let up = 0, down = 0
  const segments: BarSegment[] = []
  for (const [kind, value] of [['call', call], ['put', put]] as const) {
    if (value === 0) continue
    if (value > 0) { segments.push({ kind, from: up, to: up + value, value }); up += value }
    else { segments.push({ kind, from: down, to: down + value, value }); down += value }
  }
  return segments
}

/** The value range a set of strikes needs (always including zero). */
export function valueExtent(rows: Array<{ call: number; put: number }>, view: ExposureView): [number, number] {
  let low = 0, high = 0
  for (const row of rows) {
    for (const segment of strikeSegments(row.call, row.put, view)) {
      low = Math.min(low, segment.from, segment.to)
      high = Math.max(high, segment.from, segment.to)
    }
  }
  return [low, high]
}

/** A strike's call and put exposure split into today's expiry (0 DTE) and every later expiry. */
export type ZeroDteParts = { todayCall: number; todayPut: number; laterCall: number; laterPut: number }

/**
 * Whether strike bars split into today's and later expiries (Nov 18): only when the scope reaches past today (≤ 7 days,
 * All) and the capture holds both today's expiry (calendar DTE 0) and a later one. `no-today`: the scope reaches past
 * today but nothing in it expires today (a name without a 0 DTE expiry, or after 4:00 PM ET). `off`: 0 DTE scope, or
 * every expiry in range is today's, so there is nothing to split.
 */
export function zeroDteSplitState(scope: string, expiries: Array<{ calendar_dte: number }>): 'split' | 'no-today' | 'off' {
  if (scope === '0dte') return 'off'
  const today = expiries.some(row => row.calendar_dte === 0)
  if (!today) return 'no-today'
  return expiries.some(row => row.calendar_dte > 0) ? 'split' : 'off'
}

/**
 * Today's and later call and put exposure per strike index, summed from the read's sparse strike x expiry cells
 * [strike index, expiry index, call, put] with each expiry's calendar DTE (0 = expires today). Both parts come from
 * the cells, so a part never takes a sign its contracts do not have; their sum equals the strike's total up to the
 * read's whole-dollar rounding per cell.
 */
export function zeroDteByStrike(
  grid: Array<[number, number, number, number]>, expiries: Array<{ calendar_dte: number }>, strikeCount: number,
): ZeroDteParts[] {
  const parts = Array.from({ length: strikeCount }, () => ({ todayCall: 0, todayPut: 0, laterCall: 0, laterPut: 0 }))
  for (const [strikeIndex, expiryIndex, call, put] of grid) {
    const row = parts[strikeIndex]
    const dte = expiries[expiryIndex]?.calendar_dte
    if (!row || dte == null) continue
    if (dte === 0) { row.todayCall += call; row.todayPut += put } else { row.laterCall += call; row.laterPut += put }
  }
  return parts
}

/** The parts in the display unit (dollars, or shares = dollars / spot). */
export const partsInUnit = (parts: ZeroDteParts, unit: ExposureUnit, spot: number): ZeroDteParts => ({
  todayCall: inUnit(parts.todayCall, unit, spot), todayPut: inUnit(parts.todayPut, unit, spot),
  laterCall: inUnit(parts.laterCall, unit, spot), laterPut: inUnit(parts.laterPut, unit, spot),
})

/**
 * Bar segments for one strike with the 0 DTE split. Each kind (net, or call then put) puts its later-expiry part on
 * the zero line and today's part beyond it, so the solid base is what is still there after today's 4:00 PM ET expiry
 * and the outer part is what expires. Positive parts stack up and negative parts stack down, as in Calls vs puts, so in
 * Net a strike whose today and later nets differ in sign shows both (its net is marked separately).
 */
export function partSegments(parts: ZeroDteParts, view: ExposureView): BarSegment[] {
  const pieces: Array<[BarSegment['kind'], ExpiryPart, number]> = view === 'net'
    ? [['net', 'later', parts.laterCall + parts.laterPut], ['net', 'today', parts.todayCall + parts.todayPut]]
    : [['call', 'later', parts.laterCall], ['call', 'today', parts.todayCall], ['put', 'later', parts.laterPut], ['put', 'today', parts.todayPut]]
  let up = 0, down = 0
  const segments: BarSegment[] = []
  for (const [kind, part, value] of pieces) {
    if (value === 0) continue
    if (value > 0) { segments.push({ kind, part, from: up, to: up + value, value }); up += value }
    else { segments.push({ kind, part, from: down, to: down + value, value }); down += value }
  }
  return segments
}

export type StrikeBar = { segments: BarSegment[]; net: number; netMarker: boolean }

/**
 * Everything drawn for one strike: its segments (split by expiry when `parts` is given), its net, and whether the net
 * gets its own marker: always in Overlay (the net line's points), and in Net only where the split parts point opposite
 * ways (the bar ends no longer show the net there).
 */
export function strikeBar(call: number, put: number, view: ExposureView, parts?: ZeroDteParts | null): StrikeBar {
  if (!parts) return { segments: strikeSegments(call, put, view), net: call + put, netMarker: view === 'overlay' }
  const segments = partSegments(parts, view)
  const net = parts.todayCall + parts.todayPut + parts.laterCall + parts.laterPut
  const opposite = segments.some(segment => segment.value > 0) && segments.some(segment => segment.value < 0)
  return { segments, net, netMarker: view === 'overlay' || (view === 'net' && opposite) }
}

/** The smaller of a bar's upward and downward stacks (0 when every part points one way). In Net, the page marks the
 * net only where this is at least a couple of pixels, so near-zero strikes far from spot do not fill with dots. */
export function opposingSize(bar: StrikeBar): number {
  let up = 0, down = 0
  for (const segment of bar.segments) { if (segment.value > 0) up += segment.value; else down -= segment.value }
  return Math.min(up, down)
}

/** The value range a set of bars needs (always including zero and every marked net). */
export function barsExtent(bars: StrikeBar[]): [number, number] {
  let low = 0, high = 0
  for (const bar of bars) {
    for (const segment of bar.segments) {
      low = Math.min(low, segment.from, segment.to)
      high = Math.max(high, segment.from, segment.to)
    }
    if (bar.netMarker) { low = Math.min(low, bar.net); high = Math.max(high, bar.net) }
  }
  return [low, high]
}

/** SVG path through the Overlay's net points, left to right. */
export function netLinePath(points: Array<[number, number]>): string {
  return points.map(([x, y], index) => `${index ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`).join(' ')
}

/** About `count` round axis ticks inside [low, high]: the step (1, 2, 2.5 or 5 x a power of ten) nearest the range
 * divided by `count`. */
export function niceTicks(low: number, high: number, count = 5): number[] {
  if (!(high > low)) return [low]
  const raw = (high - low) / Math.max(1, count)
  const power = 10 ** Math.floor(Math.log10(raw))
  const step = [1, 2, 2.5, 5, 10].map(item => item * power)
    .reduce((best, item) => Math.abs(Math.log(item / raw)) < Math.abs(Math.log(best / raw)) ? item : best)
  const ticks: number[] = []
  for (let value = Math.ceil(low / step) * step; value <= high + step * 1e-9; value += step) {
    ticks.push(Math.abs(value) < step * 1e-9 ? 0 : value)
  }
  return ticks
}

/** Every k-th category label so labels of `labelWidth` px do not collide on `width` px; always keeps the ends. */
export function labelStride(count: number, width: number, labelWidth = 44): number {
  if (count <= 1 || width <= 0) return 1
  return Math.max(1, Math.ceil(count / Math.max(1, Math.floor(width / labelWidth))))
}

/** Rows for marker labels so labels closer than `minGap` px stack instead of overlapping (greedy by x). */
export function markerRows(xs: number[], minGap: number): number[] {
  const order = xs.map((x, index) => ({ x, index })).sort((a, b) => a.x - b.x)
  const lastX: number[] = []
  const rows = new Array<number>(xs.length).fill(0)
  for (const { x, index } of order) {
    let row = lastX.findIndex(previous => x - previous >= minGap)
    if (row < 0) { row = lastX.length; lastX.push(x) } else { lastX[row] = x }
    rows[index] = row
  }
  return rows
}

/** x of a price on a strike axis whose categories sit at `positions` (interpolated between neighbours). */
export function priceToX(price: number, strikes: number[], positions: number[]): number | null {
  const last = strikes.length - 1
  if (last < 0 || !Number.isFinite(price) || price < strikes[0] || price > strikes[last]) return null
  for (let index = 1; index <= last; index++) {
    if (price <= strikes[index]) {
      const span = strikes[index] - strikes[index - 1]
      return positions[index - 1] + (positions[index] - positions[index - 1]) * (span ? (price - strikes[index - 1]) / span : 0)
    }
  }
  return positions[0]
}

export type HeatmapGrid = {
  strikes: string[]
  expiries: string[]
  /** values[strikeIndex][expiryIndex], null where the capture has no contract with weight and greeks. */
  values: Array<Array<number | null>>
}

/** The strike x expiry matrix for one mode, from the API's sparse cells [strike, expiry, call, put]. */
export function heatmapGrid(
  grid: Array<[number, number, number, number]>, strikes: string[], expiries: string[], mode: HeatmapMode,
): HeatmapGrid {
  const values: Array<Array<number | null>> = strikes.map(() => expiries.map(() => null))
  for (const [strikeIndex, expiryIndex, call, put] of grid) {
    values[strikeIndex][expiryIndex] = mode === 'call' ? call : mode === 'put' ? put : call + put
  }
  return { strikes, expiries, values }
}

/** The colour scale's end: the 98th percentile of |value| among shown cells, so one cell does not wash out the rest. */
export function robustMaximum(values: Array<number | null>, quantile = 0.98): number {
  const sizes = values.filter((value): value is number => value != null && value !== 0).map(Math.abs).sort((a, b) => a - b)
  if (!sizes.length) return 0
  return sizes[Math.min(sizes.length - 1, Math.floor(quantile * (sizes.length - 1)))]
}

/** Colour strength 0..1 for a cell (clipped at the scale end). */
export const cellStrength = (value: number | null, maximum: number) =>
  value == null || maximum <= 0 ? 0 : Math.min(1, Math.abs(value) / maximum)

/** Index of the strike nearest spot in a list of strike texts. */
export function nearestStrikeIndex(strikes: string[], spot: number): number {
  let best = -1, distance = Infinity
  strikes.forEach((strike, index) => {
    const gap = Math.abs(Number(strike) - spot)
    if (gap < distance) { best = index; distance = gap }
  })
  return best
}

const GREEK_NAMES: Record<string, string> = { gamma: 'Gamma', delta: 'Delta', charm: 'Charm', vanna: 'Vanna' }
export const greekName = (greek: string) => GREEK_NAMES[greek] ?? greek

export const SCOPE_LABELS: Record<string, string> = { '0dte': '0 DTE', '7d': '≤ 7 days', all: 'All captured' }
export const WEIGHT_LABELS: Record<string, string> = { oi: 'Open interest', oi_volume: 'OI + today’s volume (est.)' }

/** What an unavailable read means for the reader, with the scope to try instead when there is one. */
export function unavailableText(reason: string | null | undefined, underlyer: string, scope: string): { title: string; detail: string; tryScope?: string } {
  switch (reason) {
    case 'NO_CHAIN_CAPTURE':
      return { title: 'No chain capture yet', detail: `No stored option chain for ${underlyer} in this session yet. Chains are captured every 5 minutes during the session.` }
    case 'SPOT_UNAVAILABLE':
      return { title: 'Spot price unavailable', detail: `The latest ${underlyer} chain capture has no underlying price, so exposures cannot be scaled.` }
    case 'NO_CONTRACTS_IN_SCOPE':
      return scope === '0dte'
        ? { title: 'Nothing expires today', detail: `No ${underlyer} expiry in the captured chain ends today.`, tryScope: '7d' }
        : { title: 'No contracts in this range', detail: `The captured ${underlyer} chain has no expiry in ${SCOPE_LABELS[scope] ?? scope}.`, tryScope: 'all' }
    case 'SCOPE_EXPIRED':
      return { title: 'Today’s expiries have closed', detail: 'Contracts expiring today stopped trading at 4:00 PM ET; this capture has no live 0 DTE contract.', tryScope: 'all' }
    case 'NO_EXPOSURE_IN_SCOPE':
      return { title: 'No exposure in this range', detail: 'No contract in this range has both a weight and a Greek.', tryScope: scope === 'all' ? undefined : 'all' }
    default:
      return { title: 'Exposure unavailable', detail: reason || 'The exposure read did not return data.' }
  }
}

export type QualityNote = { tone: 'warning' | 'note'; text: string }

/** Plain-language notes for the read's quality reasons; unknown reasons are shown as they are. */
export function qualityNotes(reasons: string[], context: { underlyer: string; providerIvContracts?: number;
  weightCoverage?: number | null; crossCheck?: number | null; crossCheckNet?: number | null;
  spotGap?: number | null; quotePrice?: string | null; spot?: string | null }): QualityNote[] {
  const notes: QualityNote[] = []
  for (const reason of reasons) {
    switch (reason) {
      case 'LATEST_CAPTURE_NOT_COMPLETE':
        notes.push({ tone: 'warning', text: 'The newest chain capture did not complete; the page shows the last complete one (time above).' }); break
      case 'WEIGHT_WITHOUT_GREEKS':
        notes.push({ tone: 'warning', text: `Only ${formatPercent(context.weightCoverage)} of the weight in range has a Greek; contracts without an IV are left out.` }); break
      case 'PROVIDER_CROSS_CHECK_DIVERGES': {
        // The read flags either gap over 10%; quote the one that is larger, so the number explains the warning.
        const gross = context.crossCheck ?? null
        const net = context.crossCheckNet ?? null
        const netLeads = net != null && (gross == null || Math.abs(net) > Math.abs(gross))
        notes.push({ tone: 'warning', text: netLeads
          ? `Our net exposure differs from the provider’s Greeks by ${formatSignedPercent(net)} of gross on the same contracts (gross ${formatSignedPercent(gross)}): calls and puts are split differently.`
          : `Our exposure differs from the provider’s Greeks by ${formatSignedPercent(gross)} on the same contracts.` })
        break
      }
      case 'SPOT_QUOTE_GAP':
        notes.push({ tone: 'warning', text: `The capture’s spot (${context.spot ?? '—'}, last equity bar) is ${formatSignedPercent(context.spotGap, 2)} against the underlying price on the option quotes (${context.quotePrice ?? '—'}). Our IVs and Greeks use that spot, so the shortest expiries are the least reliable in this capture.` })
        break
      case 'INSUFFICIENT_CONTRACTS':
      case 'INSUFFICIENT_GAMMA_CONTRACTS':
        if (!notes.some(note => note.text.startsWith('Few contracts'))) notes.push({ tone: 'warning', text: 'Few contracts in this range: levels are fragile.' })
        break
      case 'PROVIDER_IV_FALLBACK':
        notes.push({ tone: 'note', text: `${context.providerIvContracts ?? 'Some'} contracts have no IV of ours (no usable quote) and use the provider’s IV in the same model.` }); break
      case 'VOLUME_WEIGHT_ESTIMATE':
        notes.push({ tone: 'note', text: 'Open interest + today’s volume assumes every contract traded today opened a position on the conventional dealer side: an estimate, not a count.' }); break
      case 'NO_FLIP_IN_SEARCH_RANGE':
        notes.push({ tone: 'note', text: 'Net gamma keeps one sign from 20% below to 20% above spot: no flip level in range.' }); break
      case 'MULTIPLE_FLIP_CANDIDATES':
        notes.push({ tone: 'note', text: 'Net gamma changes sign more than once in range; the flip shown is the crossing nearest spot.' }); break
      case 'NO_ELIGIBLE_GAMMA_CONTRACT':
      case 'NO_GAMMA_CONTRACTS':
        break
      default:
        notes.push({ tone: 'note', text: reason })
    }
  }
  return notes
}

export const regimeText = (regime: string | null | undefined) =>
  regime === 'POSITIVE_GAMMA' ? 'Positive gamma' : regime === 'NEGATIVE_GAMMA' ? 'Negative gamma' : 'Undetermined'

// Nov 18 (user's short list): level tooltips, dealer-sign legend labels and the factual summary line.

export type LevelKind = 'call_wall' | 'put_wall' | 'flip'

/** What each gamma level is, for its tooltip wherever it appears. The weight is switchable on this page, so the text
 * says "with the selected weight" rather than naming one. */
export const LEVEL_DEFINITIONS: Record<LevelKind, string> = {
  call_wall: 'The strike with the largest call gamma exposure in the selected expiries (dollar gamma per 1% move, with '
    + 'the selected weight, calls only). Dealers are assumed long these calls, so their hedging sells into rallies '
    + 'toward it — often read as resistance. The net bar at this strike can still be negative when puts there are larger.',
  put_wall: 'The strike with the largest put gamma exposure in the selected expiries (same units: dollar gamma per 1% '
    + 'move, with the selected weight, puts only). Often read as support; under the convention used here dealers are '
    + 'short these puts, so their hedging adds to selling if price breaks below it.',
  flip: 'The price where total modeled dealer gamma changes sign. We recompute every contract’s gamma at 81 prices from '
    + '20% below to 20% above spot and take the crossing nearest spot. Above it, hedging leans against moves; below it, '
    + 'hedging adds to moves. “No flip” = the sign never changes within ±20%.',
}

export const LEVEL_NAMES: Record<LevelKind, string> = { call_wall: 'Call wall', put_wall: 'Put wall', flip: 'Flip' }

/** Legend labels for the sign of exposure. Gamma says what the sign means for hedging; other Greeks keep the plain
 * dealer side. */
export function signLabels(greek: string): { positive: string; negative: string } {
  return greek === 'gamma'
    ? { positive: 'Dealers modeled long gamma — hedging dampens moves', negative: 'Dealers modeled short gamma — hedging amplifies moves' }
    : { positive: 'Positive (dealers long)', negative: 'Negative (dealers short)' }
}

export const CONVENTION_NOTE = 'Signs assume dealers are long calls and short puts; open interest does not say who holds which side.'

/** A capture older than this during market hours is marked stale (captures land every 5 minutes). */
export const STALE_CAPTURE_MS = 10 * 60_000

export type SummaryToken = { text: string; term?: LevelKind; stale?: boolean }
export type ExposureSummary = { tokens: SummaryToken[]; text: string; stale: boolean; ageMinutes: number | null }
type SummaryLevel = { strike: string; distance_to_spot_fraction: number } | null | undefined
export type SummaryRead = {
  available: boolean; reason?: string | null; underlyer: string; scope: string; greek: string
  spot?: string | null; observed_at?: string | null; expiring_today_share?: number | null; quality_reasons?: string[]
  levels?: { flip_spot: string | null; flip_distance_to_spot_fraction: number | null; regime_at_spot: string
    call_wall: SummaryLevel; put_wall: SummaryLevel } | null
}

const priceText = (value: string | number) => Number(value).toLocaleString('en-US', { maximumFractionDigits: 2 })

/** Capture time in Eastern time, e.g. "Nov 12, 2:02 PM ET". */
export const formatEasternTime = (value: string) =>
  `${new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }).format(new Date(value))} ET`

/** "1.13% above spot" / "at spot" for a level's signed distance (level − spot) / spot. */
function sideOfSpot(fraction: number): string {
  if (Math.abs(fraction) < 0.00005) return 'at spot'
  return `${formatPercent(Math.abs(fraction), 2)} ${fraction > 0 ? 'above' : 'below'} spot`
}

/**
 * The page's one-line factual summary (Nov 18): where spot sits against the flip and the gamma regime at spot, the call
 * and put walls' distance from spot, the share of the captured exposure expiring today at 4:00 PM ET, and the capture
 * time in ET, marked stale when older than 10 minutes while the market is open. No advice; it always ends with
 * "Context, not a trade signal." Levels are gamma levels in every Greek view; the expiring share is the shown Greek's.
 * `marketOpen` is whether the read's session is today's and the clock is in regular hours (the caller decides).
 */
export function exposureSummary(read: SummaryRead, context: { nowMs: number; marketOpen: boolean }): ExposureSummary {
  const tokens: SummaryToken[] = []
  const say = (text: string, extra: Omit<SummaryToken, 'text'> = {}) => { tokens.push({ text, ...extra }) }
  const underlyer = read.underlyer
  const observed = read.observed_at ? Date.parse(read.observed_at) : NaN
  const ageMinutes = Number.isFinite(observed) ? Math.max(0, Math.floor((context.nowMs - observed) / 60_000)) : null
  const stale = context.marketOpen && Number.isFinite(observed) && context.nowMs - observed > STALE_CAPTURE_MS

  if (!read.available) {
    const title = unavailableText(read.reason, underlyer, read.scope).title
    say(`No levels to summarize for ${underlyer} · ${SCOPE_LABELS[read.scope] ?? read.scope}: ${title.charAt(0).toLowerCase()}${title.slice(1)}. `)
  } else {
    const levels = read.levels
    const spot = read.spot ? priceText(read.spot) : '—'
    const regime = levels?.regime_at_spot
    const sign = regime === 'POSITIVE_GAMMA' ? 'positive' : regime === 'NEGATIVE_GAMMA' ? 'negative' : null
    if (levels?.flip_spot != null && levels.flip_distance_to_spot_fraction != null) {
      // The read gives the flip's distance as (flip − spot) / spot: spot is above the flip when that is negative.
      const distance = levels.flip_distance_to_spot_fraction
      const where = Math.abs(distance) < 0.00005 ? 'is at the ' : `is ${formatPercent(Math.abs(distance), 2)} ${distance < 0 ? 'above' : 'below'} the `
      say(`${underlyer} ${spot} ${where}`); say('flip', { term: 'flip' })
      say(` (${priceText(levels.flip_spot)}); ${sign ? `${sign} gamma at spot` : 'gamma regime at spot undetermined'}. `)
    } else if (sign) {
      // No crossing in the searched range: the modeled curve keeps the sign it has at spot from −20% to +20%.
      say('No '); say('flip', { term: 'flip' }); say(` within ±20% of ${underlyer} ${spot}: ${sign} gamma at spot and across that range. `)
    } else {
      const few = (read.quality_reasons ?? []).some(reason =>
        ['INSUFFICIENT_GAMMA_CONTRACTS', 'NO_GAMMA_CONTRACTS', 'NO_ELIGIBLE_GAMMA_CONTRACT'].includes(reason))
      say('No '); say('flip', { term: 'flip' })
      say(` for ${underlyer} ${spot}: gamma regime at spot undetermined${few ? ' (too few gamma contracts in range)' : ''}. `)
    }

    const call = levels?.call_wall, put = levels?.put_wall
    if (call && put) {
      say('Call wall', { term: 'call_wall' }); say(` ${priceText(call.strike)} is ${sideOfSpot(call.distance_to_spot_fraction)}; `)
      say('put wall', { term: 'put_wall' }); say(` ${priceText(put.strike)} is ${sideOfSpot(put.distance_to_spot_fraction)}. `)
    } else if (call) {
      say('Call wall', { term: 'call_wall' }); say(` ${priceText(call.strike)} is ${sideOfSpot(call.distance_to_spot_fraction)}; no `)
      say('put wall', { term: 'put_wall' }); say(' (no put gamma in range). ')
    } else if (put) {
      say('No '); say('call wall', { term: 'call_wall' }); say(' (no call gamma in range); ')
      say('put wall', { term: 'put_wall' }); say(` ${priceText(put.strike)} is ${sideOfSpot(put.distance_to_spot_fraction)}. `)
    } else {
      say('No '); say('call wall', { term: 'call_wall' }); say(' or '); say('put wall', { term: 'put_wall' }); say(' in range. ')
    }

    const greek = greekName(read.greek).toLowerCase()
    const share = read.expiring_today_share
    say(share != null && share > 0
      ? `${formatPercent(share)} of captured ${greek} exposure (gross) expires today at 4:00 PM ET. `
      : `No captured ${greek} exposure expires today. `)
  }

  if (read.observed_at && Number.isFinite(observed)) {
    say(`Chain captured ${formatEasternTime(read.observed_at)}`)
    if (stale) say(` — stale, ${ageMinutes} min old`, { stale: true })
    say('. ')
  }
  say('Context, not a trade signal.')
  return { tokens, text: tokens.map(token => token.text).join(''), stale, ageMinutes }
}
