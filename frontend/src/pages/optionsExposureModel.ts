/**
 * G106 OV1 (Nov 16): display logic for the Options Overview page (dealer exposure per strike and expiry). Pure
 * functions only, so the node tests can load this file without React. The numbers come from
 * `/api/options/exposure`; nothing here changes them, it only selects, scales and labels.
 */

export type ExposureUnit = 'usd' | 'shares'
export type ExposureView = 'net' | 'split'
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

export type BarSegment = { kind: 'call' | 'put' | 'net'; from: number; to: number; value: number }

/**
 * Bar segments for one strike. Net: one bar from zero. Calls vs puts: positive parts stack up from zero and negative
 * parts stack down, so gamma (calls up, puts down under the convention) and delta (both up) both read correctly.
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
