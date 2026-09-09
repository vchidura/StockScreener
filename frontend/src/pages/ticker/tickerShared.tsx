import { CSSProperties } from 'react'
import {
  ChartDataPoint,
  CrossFramePatternSummary,
  LatestQuote,
  PriceChannel,
  ScannerInterval,
  TradeSetup,
} from '../../services/api'
import {
  LineData,
  Time,
  TickMarkType,
  WhitespaceData,
} from 'lightweight-charts'

export const MARKET_TIME_ZONE = 'America/New_York'
export const SESSION_BARS: Record<string, number> = {
  '1m': 390,
  '5m': 78,
  '15m': 26,
  '30m': 13,
  '1h': 7,
}

export function chartTimeToDate(time: Time): Date {
  if (typeof time === 'number') return new Date(time * 1000)
  if (typeof time === 'string') return new Date(`${time}T00:00:00Z`)
  return new Date(Date.UTC(time.year, time.month - 1, time.day))
}

export function isIntradayInterval(interval: string): boolean {
  return interval !== '1d' && interval !== '1wk'
}

export function formatChartTime(time: Time, interval: string): string {
  const intraday = isIntradayInterval(interval)
  return new Intl.DateTimeFormat(undefined, {
    timeZone: intraday ? MARKET_TIME_ZONE : 'UTC',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    ...(intraday ? { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' } as const : {}),
  }).format(chartTimeToDate(time))
}

export function formatQuoteTime(quote: LatestQuote): string {
  if (quote.source === 'daily') {
    return new Intl.DateTimeFormat(undefined, {
      timeZone: 'UTC', year: 'numeric', month: 'short', day: 'numeric',
    }).format(new Date(`${quote.trade_date}T00:00:00Z`))
  }
  return new Intl.DateTimeFormat(undefined, {
    timeZone: MARKET_TIME_ZONE,
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'short',
  }).format(new Date(quote.as_of))
}

export function formatChartTick(time: Time, type: TickMarkType, interval: string, period: string): string {
  const date = chartTimeToDate(time)
  const timeZone = isIntradayInterval(interval) ? MARKET_TIME_ZONE : 'UTC'

  if (interval in SESSION_BARS && period === '1d') {
    return new Intl.DateTimeFormat(undefined, {
      timeZone,
      hour: 'numeric',
      minute: '2-digit',
    }).format(date)
  }

  if (type === TickMarkType.Year) {
    return new Intl.DateTimeFormat(undefined, { timeZone, year: 'numeric' }).format(date)
  }
  if (type === TickMarkType.Month) {
    return new Intl.DateTimeFormat(undefined, { timeZone, month: 'short' }).format(date)
  }
  if (type === TickMarkType.DayOfMonth || !isIntradayInterval(interval)) {
    return new Intl.DateTimeFormat(undefined, { timeZone, month: 'short', day: 'numeric' }).format(date)
  }
  if (interval === '1h' && period !== '1d' && period !== '5d') {
    return new Intl.DateTimeFormat(undefined, { timeZone, month: 'short', day: 'numeric' }).format(date)
  }
  return new Intl.DateTimeFormat(undefined, {
    timeZone,
    hour: 'numeric',
    minute: '2-digit',
  }).format(date)
}

export function getVisibleChartData(data: ChartDataPoint[], period: string, interval: string): ChartDataPoint[] {
  if (data.length === 0) return data

  if (interval in SESSION_BARS && period === '1d') {
    const latestSession = new Intl.DateTimeFormat('en-CA', {
      timeZone: MARKET_TIME_ZONE,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(chartTimeToDate(data[data.length - 1].time as Time))
    return data.filter(point => new Intl.DateTimeFormat('en-CA', {
      timeZone: MARKET_TIME_ZONE,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(chartTimeToDate(point.time as Time)) === latestSession)
  }

  const tradingSessionBars: Record<string, number> = interval === '1wk'
    ? { '1d': 1, '5d': 1 }
    : interval in SESSION_BARS
      ? { '5d': SESSION_BARS[interval] * 5 }
      : {}
  if (period in tradingSessionBars) return data.slice(-tradingSessionBars[period])

  const calendarDays: Record<string, number> = {
    '1mo': 30,
    '3mo': 90,
    '6mo': 180,
    '1y': 365,
    '2y': 730,
  }
  const days = calendarDays[period]
  if (!days) return data

  const cutoff = data[data.length - 1].time - days * 24 * 60 * 60
  return data.filter(point => point.time >= cutoff)
}

export function buildSmaSeries(
  data: ChartDataPoint[],
  period: number
): Array<LineData<Time> | WhitespaceData<Time>> {
  return data.map((point, index) => {
    if (index + 1 < period) {
      return { time: point.time as Time }
    }

    const window = data.slice(index + 1 - period, index + 1)
    const average = window.reduce((sum, item) => sum + item.close, 0) / period

    return {
      time: point.time as Time,
      value: Number(average.toFixed(4)),
    }
  })
}

export function buildEmaSeries(
  data: ChartDataPoint[],
  period: number
): Array<LineData<Time> | WhitespaceData<Time>> {
  const alpha = 2 / (period + 1)
  let prev: number | null = null
  return data.map((point, index) => {
    prev = prev === null ? point.close : alpha * point.close + (1 - alpha) * prev
    // Seed bars are unstable, so hide them rather than draw a misleading tail.
    if (index + 1 < period) return { time: point.time as Time }
    return { time: point.time as Time, value: Number(prev.toFixed(4)) }
  })
}

export function buildRsiSeries(
  data: ChartDataPoint[], period = 14,
): Array<LineData<Time> | WhitespaceData<Time>> {
  return data.map((point, index) => {
    if (index < period) return { time: point.time as Time }
    let gains = 0
    let losses = 0
    for (let cursor = index - period + 1; cursor <= index; cursor += 1) {
      const change = data[cursor].close - data[cursor - 1].close
      if (change >= 0) gains += change
      else losses -= change
    }
    const averageGain = gains / period
    const averageLoss = losses / period
    const value = averageLoss === 0 ? 100 : 100 - 100 / (1 + averageGain / averageLoss)
    return { time: point.time as Time, value: Number(value.toFixed(2)) }
  })
}

export function buildBollingerBands(
  data: ChartDataPoint[],
  period = 20,
  stdDevMultiplier = 2
): {
  middle: Array<LineData<Time> | WhitespaceData<Time>>
  upper: Array<LineData<Time> | WhitespaceData<Time>>
  lower: Array<LineData<Time> | WhitespaceData<Time>>
} {
  const middle: Array<LineData<Time> | WhitespaceData<Time>> = []
  const upper: Array<LineData<Time> | WhitespaceData<Time>> = []
  const lower: Array<LineData<Time> | WhitespaceData<Time>> = []

  data.forEach((point, index) => {
    if (index + 1 < period) {
      const whitespacePoint = { time: point.time as Time }
      middle.push(whitespacePoint)
      upper.push(whitespacePoint)
      lower.push(whitespacePoint)
      return
    }

    const window = data.slice(index + 1 - period, index + 1)
    const mean = window.reduce((sum, item) => sum + item.close, 0) / period
    const variance = window.reduce((sum, item) => sum + (item.close - mean) ** 2, 0) / period
    const stdDev = Math.sqrt(variance)

    middle.push({ time: point.time as Time, value: Number(mean.toFixed(4)) })
    upper.push({
      time: point.time as Time,
      value: Number((mean + stdDevMultiplier * stdDev).toFixed(4)),
    })
    lower.push({
      time: point.time as Time,
      value: Number((mean - stdDevMultiplier * stdDev).toFixed(4)),
    })
  })

  return { middle, upper, lower }
}

// One palette for the whole page: saturated tone is reserved for meaning, never for chrome.
// These resolve per theme, so the same markup reads correctly in dark and light.
export const POS = 'var(--tm-pos)'
export const NEG = 'var(--tm-neg)'
export const WARN = 'var(--tm-warn)'
export const INFO = 'var(--tm-accent)'
export const MUTED = 'var(--tm-muted)'
export const INK = 'var(--tm-ink)'
export const LINE = 'var(--tm-line)'
export const SURFACE = 'var(--tm-surface-sunken)'
export const POS_SOFT = 'var(--tm-pos-soft)'
export const NEG_SOFT = 'var(--tm-neg-soft)'
export const INFO_SOFT = 'var(--tm-accent-soft)'
export const WARN_SOFT = 'var(--tm-warn-soft)'
export const ON_TONE = 'var(--tm-on-accent)'

export const money = (n: number | null | undefined, dp = 2) =>
  n === null || n === undefined || !Number.isFinite(n)
    ? '—'
    : `$${n.toLocaleString(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp })}`

export const signedPct = (n: number | null | undefined, dp = 1) =>
  n === null || n === undefined || !Number.isFinite(n) ? '—' : `${n > 0 ? '+' : ''}${n.toFixed(dp)}%`

export const plainPct = (n: number | null | undefined, dp = 1) =>
  n === null || n === undefined || !Number.isFinite(n) ? '—' : `${n.toFixed(dp)}%`

export const ordinal = (n: number) => {
  const value = Math.round(n)
  const remainder100 = value % 100
  const suffix = remainder100 >= 11 && remainder100 <= 13
    ? 'th'
    : value % 10 === 1 ? 'st' : value % 10 === 2 ? 'nd' : value % 10 === 3 ? 'rd' : 'th'
  return `${value}${suffix}`
}

export function formatScannerEventTime(value: string, interval: ScannerInterval): string {
  return new Intl.DateTimeFormat(undefined, {
    timeZone: interval === '1h' ? MARKET_TIME_ZONE : 'UTC',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    ...(interval === '1h' ? { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' } as const : {}),
  }).format(new Date(value))
}

export function relativeAge(iso: string | null | undefined): string | null {
  if (!iso) return null
  const ms = Date.now() - Date.parse(iso)
  if (!Number.isFinite(ms) || ms < 0) return null
  const minutes = Math.floor(ms / 60_000)
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  return hours < 24 ? `${hours}h ago` : `${Math.floor(hours / 24)}d ago`
}

export function sideOfBias(bias: string): 'LONG' | 'SHORT' | null {
  if (bias === 'Bullish') return 'LONG'
  if (bias === 'Bearish') return 'SHORT'
  return null
}

export function gradeTone(grade: string | null | undefined): string {
  if (!grade) return MUTED
  if (grade.startsWith('A')) return POS
  if (grade.startsWith('B')) return INFO
  return MUTED
}

export const SETUP_INTERVALS = ['1mo', '1wk', '1d', '1h'] as const
export type SetupTimeframe = '30m' | '1h' | '1d' | '1wk' | '1mo'
export const CHART_INTERVALS = ['1m', '5m', '15m', '30m', '1h', '1d', '1wk'] as const
export const INTERVAL_NOUN: Record<string, string> = { '1mo': 'monthly', '1wk': 'weekly', '1d': 'daily', '1h': 'hourly' }
export const PATTERN_INTERVAL_LABEL: Record<string, string> = {
  '1m': '1 minute', '5m': '5 minute', '15m': '15 minute',
  '30m': '30 minute', '1h': 'Hourly', '1d': 'Daily', '1wk': 'Weekly',
}
export const PATTERN_INTERVAL_ORDER = ['1wk', '1d', '1h', '30m', '15m', '5m'] as const

export const patternChoiceValue = (interval: string, type: string) => `frame|${interval}|${type}`

export const parsePatternChoice = (value: string) => {
  const [prefix, interval, type] = value.split('|')
  return prefix === 'frame' && interval && type ? { interval, type } : null
}

export function crossFrameReading(summary: CrossFramePatternSummary | null | undefined) {
  if (!summary) return null
  if (summary.state === 'ALIGNED_BULLISH' || summary.state === 'ALIGNED_BEARISH') {
    const bullish = summary.state === 'ALIGNED_BULLISH'
    return {
      label: bullish ? 'Aligned bullish' : 'Aligned bearish',
      detail: `${summary.directional_frames} timeframes agree; highest active frame leads`,
      tone: bullish ? POS : NEG,
    }
  }
  if (summary.state === 'COUNTERTREND') {
    const highest = summary.frames.find(frame => frame.bias === summary.dominant_bias)
    const bias = summary.dominant_bias === 'BULLISH' ? 'bullish' : 'bearish'
    return {
      label: 'Countertrend',
      detail: `${highest ? PATTERN_INTERVAL_LABEL[highest.interval] : 'Higher frame'} is ${bias}; lower frames oppose`,
      tone: WARN,
    }
  }
  if (summary.state === 'MIXED') {
    return { label: 'Mixed', detail: 'Opposing frame biases; do not count as confirmation', tone: WARN }
  }
  if (summary.state === 'SINGLE_FRAME') {
    return { label: 'Single frame', detail: 'No cross-frame confirmation yet', tone: INFO }
  }
  return { label: 'Neutral', detail: 'No directional agreement across frames', tone: MUTED }
}

export function priceChannelReading(channel: PriceChannel) {
  if (channel.position === 'NEAR_SUPPORT') {
    return {
      position: `Near support $${channel.support_price.toFixed(2)}`,
      distance: channel.support_distance_pct,
      watch: `Watch for support to hold; a completed close below $${channel.support_price.toFixed(2)} breaks the channel`,
    }
  }
  if (channel.position === 'NEAR_RESISTANCE') {
    return {
      position: `Near resistance $${channel.resistance_price.toFixed(2)}`,
      distance: channel.resistance_distance_pct,
      watch: `Watch for rejection or a completed close above $${channel.resistance_price.toFixed(2)} to break the channel`,
    }
  }
  return {
    position: `Mid-channel · support $${channel.support_price.toFixed(2)} · resistance $${channel.resistance_price.toFixed(2)}`,
    distance: null,
    watch: 'No boundary decision yet; monitor the next approach to support or resistance',
  }
}

export const defaultPeriodForInterval = (interval: string) => {
  if (interval === '5m') return '5d'
  if (interval === '15m' || interval === '30m') return '1mo'
  if (interval === '1h') return '3mo'
  if (interval === '1m') return '1d'
  if (interval === '1wk') return '2y'
  return '1y'
}

const INTERVAL_RANK: Record<string, number> = {
  '1m': 1, '5m': 2, '15m': 3, '30m': 4, '1h': 5, '1d': 6, '1wk': 7, '1mo': 8,
}

/**
 * Analysis is published only on 30m and coarser, so finer chart intervals resolve to the nearest
 * published frame at or above them rather than claiming an analysis that does not exist.
 */
export function resolveAnalysisInterval(
  chartInterval: string,
  available: readonly string[],
): { interval: string; snapped: boolean } {
  const ordered = [...available].sort((left, right) => INTERVAL_RANK[left] - INTERVAL_RANK[right])
  if (ordered.length === 0) return { interval: chartInterval, snapped: false }
  if (ordered.includes(chartInterval)) return { interval: chartInterval, snapped: false }
  const chartRank = INTERVAL_RANK[chartInterval] ?? 0
  const coarser = ordered.find(value => INTERVAL_RANK[value] >= chartRank)
  return { interval: coarser ?? ordered[ordered.length - 1], snapped: true }
}

/** Reward:risk below this is not worth taking, but the plan is still shown with a warning. */
export const MIN_EXECUTABLE_RR = 2
/** A stop closer than this many ATR sits inside normal noise and will be hit at random. */
export const MIN_STOP_ATR = 1

/** Beyond this the stored daily state is not describing today's market any more. */
export const MAX_DISCOVERY_AGE_DAYS = 5

export const TILE: CSSProperties = {
  padding: '9px 11px',
  background: 'var(--tm-surface)',
  border: 0,
  borderRadius: 0,
}
export const LABEL: CSSProperties = {
  fontSize: '8.5px',
  textTransform: 'uppercase',
  letterSpacing: '0.1em',
  color: 'var(--tm-faint)',
  fontWeight: 800,
}
export const PANEL: CSSProperties = {
  padding: '0.85rem',
  background: 'var(--tm-surface)',
  border: '1px solid var(--tm-line)',
  borderRadius: '0.5rem',
}

export function Pill({ text, tone, solid = false, title }: {
  text: string
  tone: string
  solid?: boolean
  title?: string
}) {
  return (
    <span
      title={title}
      style={{
        padding: '0.12rem 0.5rem',
        borderRadius: '9999px',
        fontSize: '0.68rem',
        fontWeight: 700,
        whiteSpace: 'nowrap',
        border: `1px solid ${tone}`,
        color: solid ? ON_TONE : tone,
        background: solid ? tone : 'transparent',
      }}
    >{text}</span>
  )
}

export function Tile({ label, value, tone = INK, sub, note, accent, noteTone }: {
  label: string
  value: string
  tone?: string
  sub?: string
  note?: string
  accent?: string
  noteTone?: string
}) {
  return (
    <div style={{ ...TILE, ...(accent ? { borderLeft: `3px solid ${accent}` } : {}) }}>
      <div style={LABEL}>{label}</div>
      <div style={{ fontSize: '15px', fontWeight: 700, color: tone, lineHeight: 1.25, marginTop: '2px' }}>{value}</div>
      {sub && <div style={{ fontSize: '10.5px', color: MUTED, marginTop: '2px' }}>{sub}</div>}
      {note && <div style={{ fontSize: '10px', color: noteTone ?? MUTED, marginTop: '4px', fontWeight: noteTone ? 600 : 400, lineHeight: 1.4 }}>{note}</div>}
    </div>
  )
}

export function VolumeSparkline({ values, state, slopeState }: {
  values: number[]
  state: TradeSetup['technicals']['volume_trend_state']
  slopeState: TradeSetup['technicals']['volume_slope_state']
}) {
  const width = 88
  const height = 28
  if (values.length < 2) {
    return <div style={{ width, height, color: MUTED, fontSize: '0.66rem' }}>No trend</div>
  }
  const minimum = Math.min(1, ...values)
  const maximum = Math.max(1, ...values)
  const span = maximum - minimum || 1
  const yFor = (value: number) => 3 + (maximum - value) / span * (height - 6)
  const points = values.map((value, index) => (
    `${(index / (values.length - 1) * (width - 4) + 2).toFixed(1)},${yFor(value).toFixed(1)}`
  )).join(' ')
  const tone = state === 'EXPANDING' ? INFO : state === 'CONTRACTING' ? WARN : MUTED
  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={`Volume ${state.toLowerCase()}, recent slope ${slopeState.toLowerCase()}`}
      style={{ display: 'block', flexShrink: 0 }}
    >
      <line x1="2" x2={width - 2} y1={yFor(1)} y2={yFor(1)} stroke={LINE} strokeDasharray="3 2" />
      <polyline points={points} fill="none" stroke={tone} strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={width - 2} cy={yFor(values[values.length - 1])} r="2.3" fill={tone} />
    </svg>
  )
}

export function DirectionStrengthTrack({ adx, plusDi, minusDi }: {
  adx: number | null
  plusDi: number | null
  minusDi: number | null
}) {
  if (adx === null || plusDi === null || minusDi === null) {
    return <span style={{ color: MUTED }}>Unavailable</span>
  }
  const total = plusDi + minusDi
  const plusShare = total > 0 ? plusDi / total * 100 : 50
  const difference = plusDi - minusDi
  const direction = Math.abs(difference) < 1 ? 'Balanced'
    : difference > 0 ? `+DI ${adx >= 20 ? 'control' : 'edge'}`
    : `−DI ${adx >= 20 ? 'control' : 'edge'}`
  const tone = Math.abs(difference) < 1 || adx < 20 ? MUTED : difference > 0 ? POS : NEG
  const strength = adx >= 40 ? 'very strong' : adx >= 25 ? 'strong' : adx >= 20 ? 'developing' : 'weak'
  return (
    <div style={{ minWidth: 132 }} title="ADX measures trend strength; +DI and −DI identify directional control. Completed bars only.">
      <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.35rem', marginBottom: '0.25rem' }}>
        <strong style={{ color: tone }}>{direction}</strong>
        <span style={{ color: MUTED, fontSize: '0.68rem' }}>ADX {adx.toFixed(1)} · {strength}</span>
      </div>
      <div style={{ display: 'flex', width: 128, height: 6, overflow: 'hidden', borderRadius: 4, background: LINE, marginBottom: '0.25rem' }}>
        <div style={{ width: `${plusShare}%`, background: POS }} />
        <div style={{ width: `${100 - plusShare}%`, background: NEG }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', width: 128, fontSize: '0.68rem' }}>
        <span style={{ color: POS }}>+DI {plusDi.toFixed(1)}</span>
        <span style={{ color: NEG }}>−DI {minusDi.toFixed(1)}</span>
      </div>
    </div>
  )
}

export function VolatilityTrack({ value, percentile, state, atrPct }: {
  value: number | null
  percentile: number | null
  state: TradeSetup['technicals']['historical_volatility_state']
  atrPct: number
}) {
  if (value === null || percentile === null) {
    return <span style={{ color: MUTED }}>Unavailable</span>
  }
  const boundedPercentile = Math.max(0, Math.min(100, percentile))
  const tone = state === 'ELEVATED' ? WARN : state === 'QUIET' ? INFO : MUTED
  const stateLabel = state === 'ELEVATED' ? 'high rank' : state === 'QUIET' ? 'low rank' : 'mid range'
  return (
    <div style={{ minWidth: 120 }} title="HV20 is annualized 20-bar realized volatility; percentile ranks it against the last 252 rolling windows.">
      <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.35rem', marginBottom: '0.25rem' }}>
        <strong>HV20 {plainPct(value, 1)}</strong>
        <span style={{ color: tone, fontSize: '0.68rem', fontWeight: 700 }}>{stateLabel}</span>
      </div>
      <div style={{ width: 112, height: 6, overflow: 'hidden', borderRadius: 4, background: LINE, marginBottom: '0.25rem' }}>
        <div style={{ width: `${boundedPercentile}%`, height: '100%', background: tone }} />
      </div>
      <div style={{ color: MUTED, fontSize: '0.68rem' }}>
        {ordinal(percentile)} percentile · ATR {plainPct(atrPct, 1)}
      </div>
    </div>
  )
}

export function StructureRead({ setup, currentPrice }: {
  setup: TradeSetup
  currentPrice: number | null
}) {
  const pattern = setup.structural_patterns[0] ?? null
  const volumePivots = setup.zones.filter(zone => zone.source === 'Volume Pivot')
  const fibonacciPivots = volumePivots.filter(zone => (zone.fibonacci_levels?.length ?? 0) > 0)
  const referencePrice = currentPrice ?? setup.last_close
  const necklineHolds = pattern
    ? pattern.direction === 'BULLISH'
      ? referencePrice >= pattern.neckline
      : referencePrice <= pattern.neckline
    : null
  const tone = pattern?.direction === 'BULLISH' ? POS : pattern?.direction === 'BEARISH' ? NEG : MUTED
  const title = pattern
    ? `${pattern.name}: neckline ${money(pattern.neckline)}, target ${money(pattern.target)}, invalidation ${money(pattern.invalidation)}. ${pattern.bars_ago} bars since confirmation.`
    : 'No active confirmed double-top, double-bottom, or head-and-shoulders pattern.'

  return (
    <div style={{ minWidth: 155 }} title={title}>
      {pattern ? (
        <>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.35rem' }}>
            <strong style={{ color: tone }}>{pattern.name}</strong>
            {setup.structural_patterns.length > 1 && (
              <span style={{ color: MUTED, fontSize: '0.66rem' }}>+{setup.structural_patterns.length - 1}</span>
            )}
          </div>
          <div style={{ color: necklineHolds ? tone : WARN, fontSize: '0.68rem', fontWeight: 600 }}>
            Neckline {money(pattern.neckline)} · {necklineHolds ? 'holds' : 'weakened'}
          </div>
          <div style={{ color: MUTED, fontSize: '0.66rem' }}>Target {money(pattern.target)}</div>
        </>
      ) : (
        <div style={{ color: MUTED, fontSize: '0.7rem' }}>No active pattern</div>
      )}
      <div style={{ color: fibonacciPivots.length > 0 ? INFO : MUTED, fontSize: '0.66rem', marginTop: '0.2rem' }}>
        {volumePivots.length} volume pivot{volumePivots.length === 1 ? '' : 's'}
        {fibonacciPivots.length > 0 ? ` · ${fibonacciPivots.length} near Fib` : ''}
      </div>
    </div>
  )
}

export function trendTone(state: string | null | undefined): string {
  if (!state) return MUTED
  // Neutral and Mixed are explicitly non-directional, so they must not read as a call.
  if (/^(neutral|mixed)/i.test(state)) return MUTED
  if (/bullish|uptrend|recovery/i.test(state)) return POS
  if (/bearish|downtrend/i.test(state)) return NEG
  return MUTED
}
