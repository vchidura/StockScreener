import { useState, useEffect, useRef, useCallback, CSSProperties, Fragment } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  createChart,
  IChartApi,
  ISeriesApi,
  CandlestickData,
  HistogramData,
  LineData,
  WhitespaceData,
  Time,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  TickMarkType,
} from 'lightweight-charts'
import { 
  getChartData, 
  getLatestQuote, 
  getMultiTradeSetup,
  ChartDataPoint, 
  LatestQuote, 
  TradeSetup, 
  MultiTradeSetupResponse,
  getTickerDiscoveryState,
  TickerDiscoveryResponse,
  TickerDiscoveryState,
  getTickerScannerEvents,
  ScannerEventRow,
  ScannerInterval,
} from '../services/api'

const MARKET_TIME_ZONE = 'America/New_York'
const SESSION_BARS: Record<string, number> = {
  '1m': 390,
  '5m': 78,
  '15m': 26,
  '30m': 13,
  '1h': 7,
}

function chartTimeToDate(time: Time): Date {
  if (typeof time === 'number') return new Date(time * 1000)
  if (typeof time === 'string') return new Date(`${time}T00:00:00Z`)
  return new Date(Date.UTC(time.year, time.month - 1, time.day))
}

function isIntradayInterval(interval: string): boolean {
  return interval !== '1d' && interval !== '1wk'
}

function formatChartTime(time: Time, interval: string): string {
  const intraday = isIntradayInterval(interval)
  return new Intl.DateTimeFormat(undefined, {
    timeZone: intraday ? MARKET_TIME_ZONE : 'UTC',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    ...(intraday ? { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' } as const : {}),
  }).format(chartTimeToDate(time))
}

function formatQuoteTime(quote: LatestQuote): string {
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

function formatChartTick(time: Time, type: TickMarkType, interval: string, period: string): string {
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

function getVisibleChartData(data: ChartDataPoint[], period: string, interval: string): ChartDataPoint[] {
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

function buildSmaSeries(
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

function buildEmaSeries(
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

function buildRsiSeries(
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

function buildBollingerBands(
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
const POS = '#15803d'
const NEG = '#b91c1c'
const WARN = '#b45309'
const INFO = '#1d4ed8'
const MUTED = '#64748b'
const INK = '#0f172a'
const LINE = '#e2e8f0'
const SURFACE = '#f8fafc'
const POS_SOFT = '#dcfce7'
const NEG_SOFT = '#fee2e2'
const INFO_SOFT = '#eff6ff'
const WARN_SOFT = '#fffbeb'

// Overlay hues are mid-tone and equal weight so no single average dominates the candles.
const MA_TONE: Record<string, string> = {
  'MA 50': '#475569',
  'MA 100': '#7c3aed',
  'MA 200': '#0f766e',
  'EMA 8': '#ea580c',
  'EMA 21': '#2563eb',
  'EMA 50': '#be185d',
}
const BB_TONE = '#cbd5e1'

const money = (n: number | null | undefined, dp = 2) =>
  n === null || n === undefined || !Number.isFinite(n)
    ? '—'
    : `$${n.toLocaleString(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp })}`

const signedPct = (n: number | null | undefined, dp = 1) =>
  n === null || n === undefined || !Number.isFinite(n) ? '—' : `${n > 0 ? '+' : ''}${n.toFixed(dp)}%`

const plainPct = (n: number | null | undefined, dp = 1) =>
  n === null || n === undefined || !Number.isFinite(n) ? '—' : `${n.toFixed(dp)}%`

function formatScannerEventTime(value: string, interval: ScannerInterval): string {
  return new Intl.DateTimeFormat(undefined, {
    timeZone: interval === '1h' ? MARKET_TIME_ZONE : 'UTC',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    ...(interval === '1h' ? { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' } as const : {}),
  }).format(new Date(value))
}

function relativeAge(iso: string | null | undefined): string | null {
  if (!iso) return null
  const ms = Date.now() - Date.parse(iso)
  if (!Number.isFinite(ms) || ms < 0) return null
  const minutes = Math.floor(ms / 60_000)
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  return hours < 24 ? `${hours}h ago` : `${Math.floor(hours / 24)}d ago`
}

function sideOfBias(bias: string): 'LONG' | 'SHORT' | null {  if (bias === 'Bullish') return 'LONG'
  if (bias === 'Bearish') return 'SHORT'
  return null
}

function gradeTone(grade: string | null | undefined): string {
  if (!grade) return MUTED
  if (grade.startsWith('A')) return POS
  if (grade.startsWith('B')) return INFO
  return MUTED
}

const SETUP_INTERVALS = ['1wk', '1d', '1h'] as const
const INTERVAL_NOUN: Record<string, string> = { '1wk': 'weekly', '1d': 'daily', '1h': 'hourly' }

/** Reward:risk below this is not worth taking, but the plan is still shown with a warning. */
const MIN_EXECUTABLE_RR = 2
/** A stop closer than this many ATR sits inside normal noise and will be hit at random. */
const MIN_STOP_ATR = 1

/** Beyond this the stored daily state is not describing today's market any more. */
const MAX_DISCOVERY_AGE_DAYS = 5

const TILE: CSSProperties = {
  padding: '0.7rem 0.85rem',
  background: '#fff',
  border: '1px solid #e2e8f0',
  borderRadius: '0.5rem',
}
const LABEL: CSSProperties = {
  fontSize: '0.66rem',
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  color: MUTED,
  fontWeight: 700,
}
const PANEL: CSSProperties = {
  padding: '0.85rem',
  background: '#fff',
  border: '1px solid #e2e8f0',
  borderRadius: '0.5rem',
}

function Pill({ text, tone, solid = false, title }: {
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
        color: solid ? '#fff' : tone,
        background: solid ? tone : 'transparent',
      }}
    >{text}</span>
  )
}

function Tile({ label, value, tone = INK, sub, note, accent, noteTone }: {
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
      <div style={{ fontSize: '1.05rem', fontWeight: 700, color: tone, lineHeight: 1.3, marginTop: '0.1rem' }}>{value}</div>
      {sub && <div style={{ fontSize: '0.72rem', color: MUTED, marginTop: '0.15rem' }}>{sub}</div>}
      {note && <div style={{ fontSize: '0.7rem', color: noteTone ?? MUTED, marginTop: '0.25rem', fontWeight: noteTone ? 600 : 400 }}>{note}</div>}
    </div>
  )
}

function VolumeSparkline({ values, state, slopeState }: {
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

function DirectionStrengthTrack({ adx, plusDi, minusDi }: {
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
        <div style={{ width: `${plusShare}%`, background: '#78c493' }} />
        <div style={{ width: `${100 - plusShare}%`, background: '#e99999' }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', width: 128, fontSize: '0.68rem' }}>
        <span style={{ color: POS }}>+DI {plusDi.toFixed(1)}</span>
        <span style={{ color: NEG }}>−DI {minusDi.toFixed(1)}</span>
      </div>
    </div>
  )
}

function VolatilityTrack({ value, percentile, state, atrPct }: {
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
        {percentile.toFixed(0)}th pct · ATR {plainPct(atrPct, 1)}
      </div>
    </div>
  )
}

function trendTone(state: string | null | undefined): string {
  if (!state) return MUTED
  if (/bullish|uptrend|recovery/i.test(state)) return POS
  if (/bearish|downtrend/i.test(state)) return NEG
  return MUTED
}

export interface TradePlan {
  side: 'LONG' | 'SHORT'
  entry: number
  stop: number
  target: number
  stopLabel: string
  targetLabel: string
  risk: number
  reward: number
  riskPct: number
  rewardPct: number
  rr: number
  stopAtr: number | null
}

/** Nearest technical stop and first target around the last close, on the selected interval. */
function buildTradePlan(setup: TradeSetup): TradePlan | null {
  const side = sideOfBias(setup.direction.bias)
  if (!side) return null

  const entry = setup.last_close
  if (!Number.isFinite(entry) || entry <= 0) return null

  const below = [...(setup.stops ?? [])].filter(s => s.price < entry).sort((a, b) => b.price - a.price)
  const above = [...(setup.targets ?? [])].filter(t => t.price > entry).sort((a, b) => a.price - b.price)
  const atr = setup.technicals.atr

  const shortStops = above.filter(level => level.source !== 'ATR')
  const shortTargets = below.filter(level => level.source !== 'ATR')
  if (Number.isFinite(atr) && atr > 0) {
    shortStops.push({ level: 'ATR Stop (1R)', price: entry + atr, source: 'ATR' })
    if (entry - 2 * atr > 0) {
      shortTargets.push({ level: 'ATR Target (2R)', price: entry - 2 * atr, source: 'ATR' })
    }
  }
  shortStops.sort((a, b) => a.price - b.price)
  shortTargets.sort((a, b) => b.price - a.price)

  const stopPick = side === 'LONG' ? below[0] : shortStops[0]
  const targetPick = side === 'LONG' ? above[0] : shortTargets[0]
  if (!stopPick || !targetPick) return null

  const risk = Math.abs(entry - stopPick.price)
  const reward = Math.abs(targetPick.price - entry)
  if (!Number.isFinite(risk) || risk <= 0) return null

  return {
    side,
    entry,
    stop: stopPick.price,
    target: targetPick.price,
    stopLabel: `${stopPick.level} (${stopPick.source})`,
    targetLabel: `${targetPick.level} (${targetPick.source})`,
    risk,
    reward,
    riskPct: (risk / entry) * 100,
    rewardPct: (reward / entry) * 100,
    rr: reward / risk,
    stopAtr: Number.isFinite(atr) && atr > 0 ? risk / atr : null,
  }
}

interface PlanCheck {
  label: string
  detail: string
  /** Sentence fragment used when this check fails; `detail` describes the state either way. */
  reason: string
  /** null means not applicable, so it is excluded from the score rather than counted as a failure. */
  pass: boolean | null
  blocking?: boolean
}

interface PlanCaution {
  detail: string
  source: string
}

interface PlanVerdict {
  label: string
  tone: string
  summary: string
  tooltip: string
  cautions: PlanCaution[]
}

/**
 * Confidence is scored only from computed geometry, volatility and timeframe agreement.
 * Unvalidated signals raise cautions instead, so a shadow heuristic can never hide a plan.
 */
function evaluatePlan(
  plan: TradePlan,
  setup: TradeSetup,
  livePrice: number | null,
  discovery: TickerDiscoveryState | null,
  discoveryStale: boolean,
): PlanVerdict {
  const drift = livePrice === null
    ? null
    : plan.side === 'LONG' ? livePrice - plan.entry : plan.entry - livePrice
  const confirmTf = setup.ema_alignment.confirm_interval

  const checks: PlanCheck[] = [
    {
      label: 'Reward covers risk',
      pass: plan.rr >= 1,
      blocking: true,
      detail: `${plan.rr.toFixed(2)}R to the first target`,
      reason: `reward is only ${plan.rr.toFixed(2)}R, closer than the stop`,
    },
    {
      label: 'Stop clears noise',
      pass: plan.stopAtr === null ? null : plan.stopAtr >= MIN_STOP_ATR,
      blocking: true,
      detail: plan.stopAtr === null ? 'ATR unavailable' : `${plan.stopAtr.toFixed(2)}× ATR from entry`,
      reason: `the stop sits ${plan.stopAtr?.toFixed(2)}× ATR from entry, inside normal bar range`,
    },
    {
      label: `Meets ${MIN_EXECUTABLE_RR}R floor`,
      pass: plan.rr >= MIN_EXECUTABLE_RR,
      detail: `${plan.rr.toFixed(2)}R`,
      reason: `${plan.rr.toFixed(2)}R is below the ${MIN_EXECUTABLE_RR}R floor`,
    },
    {
      label: 'Higher timeframe agrees',
      pass: setup.ema_alignment.multi_tf_agree,
      detail: setup.ema_alignment.confirm === null
        ? `No ${confirmTf} data to confirm`
        : `${confirmTf} EMA 8/21 ${setup.ema_alignment.confirm.toLowerCase()}`,
      reason: `the ${confirmTf} EMA stack diverges`,
    },
    {
      label: 'Signal confluence',
      pass: /^[AB]/.test(setup.confluence.grade),
      detail: `Grade ${setup.confluence.grade} · ${setup.confluence.count} signals`,
      reason: `confluence is only grade ${setup.confluence.grade} on ${setup.confluence.count} signals`,
    },
    {
      label: 'Entry still valid',
      pass: drift === null ? null : drift <= plan.risk * 0.5,
      detail: drift === null ? 'No live quote' : 'Price is still near the entry',
      reason: `price has run ${money(drift ?? 0)} past the entry`,
    },
  ]

  const cautions: PlanCaution[] = []
  if (discoveryStale) {
    cautions.push({
      detail: 'Daily market state is out of date, so it was left out of this read.',
      source: 'discovery overlay',
    })
  } else if (discovery) {
    const source = `daily discovery overlay · ${discovery.validation_status === 'CANDIDATE_ALPHA' ? 'candidate alpha' : 'unvalidated'}`
    const reversalAgainst = plan.side === 'LONG'
      ? discovery.reversal_trigger?.startsWith('BEARISH')
      : discovery.reversal_trigger?.startsWith('BULLISH')
    const withTrend = (plan.side === 'LONG' && discovery.trend_state === 'UPTREND')
      || (plan.side === 'SHORT' && discovery.trend_state === 'DOWNTREND')
    const againstTrend = (plan.side === 'LONG' && discovery.trend_state === 'DOWNTREND')
      || (plan.side === 'SHORT' && discovery.trend_state === 'UPTREND')
    const extended = !!discovery.extension_risk && discovery.extension_risk !== 'NORMAL'

    if (reversalAgainst) {
      cautions.push({
        detail: discovery.position_guidance
          ?? `Daily reversal trigger ${(discovery.reversal_trigger ?? '').replace(/_/g, ' ').toLowerCase()} runs against this ${plan.side}.`,
        source,
      })
    } else if (withTrend && extended) {
      cautions.push({
        detail: discovery.position_guidance
          ?? `Daily trend is ${discovery.extension_risk?.replace(/_/g, ' ').toLowerCase()} — entering here is chasing.`,
        source,
      })
    }
    // A bias that opposes the daily trend is the falling-knife case, previously unflagged.
    if (againstTrend) {
      cautions.push({
        detail: `Daily trend is ${(discovery.trend_state ?? '').toLowerCase()}, so this ${plan.side} is counter-trend.`,
        source,
      })
    }
  }

  const scored = checks.filter(check => check.pass !== null)
  const failures = scored.filter(check => check.pass === false)
  const blocked = failures.some(check => check.blocking)

  const label = blocked ? 'Not tradeable'
    : failures.length === 0 && cautions.length === 0 ? 'Take'
    : failures.length + cautions.length <= 1 ? 'Take with care'
    : 'Wait'
  const tone = blocked ? NEG
    : label === 'Take' ? POS
    : label === 'Take with care' ? WARN
    : MUTED

  const listed = (blocked ? failures.filter(c => c.blocking) : failures).map(c => c.reason)
  const sentence = (parts: string[]) => parts.length <= 1
    ? parts[0]
    : `${parts.slice(0, -1).join(', ')} and ${parts[parts.length - 1]}`
  const summary = listed.length > 0
    ? `${sentence(listed).replace(/^./, c => c.toUpperCase())}.`
    : `${plan.rr.toFixed(2)}R with the stop ${plan.stopAtr !== null ? `${plan.stopAtr.toFixed(1)}× ATR` : 'clear'} from entry; every check passed.`

  const tooltip = checks
    .map(c => `${c.pass === null ? '–' : c.pass ? '✓' : '✗'} ${c.label} — ${c.detail}`)
    .join('\n')

  return { label, tone, summary, tooltip, cautions }
}


function TickerDetail() {
  const { symbol } = useParams<{ symbol: string }>()
  const navigate = useNavigate()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null)
  const ma50SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const ma100SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const ma200SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const ema8SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const ema21SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const ema50SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const bbMiddleSeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const bbUpperSeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const bbLowerSeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const rsiSeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const rsiPaneIndexRef = useRef<number | null>(null)
  const legendRef = useRef<HTMLDivElement>(null)

  const queryClient = useQueryClient()
  const [period, setPeriod] = useState('1y')
  const [interval, setInterval] = useState('1d')
  const [setupInterval, setSetupInterval] = useState('1d')
  const [chartHeight, setChartHeight] = useState(450)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [showRsi, setShowRsi] = useState(false)
  const [tab, setTab] = useState<'timeframes' | 'levels' | 'fibonacci' | 'scanner'>('timeframes')
  const [expandedEvents, setExpandedEvents] = useState<Set<number>>(new Set())

  // Track previous interval to avoid showing stale data across interval switches
  const prevIntervalRef = useRef(interval)
  const intervalRef = useRef(interval)
  const periodRef = useRef(period)
  const chartRequestPeriod = interval === '1h' ? '2y' : interval === '1wk' ? '5y' : period

  const { data: chartData = [], isFetching: loading } = useQuery<ChartDataPoint[]>({
    queryKey: ['chart', symbol, chartRequestPeriod, interval],
    queryFn: () => getChartData(symbol!, chartRequestPeriod, interval),
    enabled: !!symbol,
    placeholderData: (prev) => prevIntervalRef.current === interval ? prev : undefined,
  })
  const visibleChartData = getVisibleChartData(chartData, period, interval)

  const { data: latestQuote = null } = useQuery({
    queryKey: ['latest-quote', symbol],
    queryFn: () => getLatestQuote(symbol!),
    enabled: !!symbol,
    refetchInterval: 60_000,
  })

  const { data: multiSetup = null, isFetching: setupLoading } = useQuery<MultiTradeSetupResponse | null>({
    queryKey: ['trade-setup-multi', symbol],
    queryFn: () => getMultiTradeSetup(symbol!),
    enabled: !!symbol,
    staleTime: 0,
    refetchInterval: 120_000,
  })
  const tradeSetup = multiSetup?.setups[setupInterval as '1h' | '1d' | '1wk'] ?? null
  const setups = multiSetup?.setups ?? {}
  const confluenceZones = multiSetup?.confluence_zones ?? []

  const { data: discoveryResp = null } = useQuery<TickerDiscoveryResponse | null>({
    queryKey: ['market-discovery', symbol],
    queryFn: () => getTickerDiscoveryState(symbol!),
    enabled: !!symbol,
  })
  const discoveryState = discoveryResp?.state ?? null
  const { data: tickerScannerEvents = {
    ticker: symbol ?? '', daily_sessions: 21, hourly_sessions: 5, events: [],
  } } = useQuery({
    queryKey: ['scanner-events', symbol, 21, 5],
    queryFn: () => getTickerScannerEvents(symbol!, 100, 21, 5),
    enabled: !!symbol,
  })

  const techSide = tradeSetup ? sideOfBias(tradeSetup.direction.bias) : null
  const discoveryAgeDays = discoveryState?.trade_date
    ? Math.floor((Date.now() - Date.parse(`${discoveryState.trade_date}T00:00:00Z`)) / 86_400_000)
    : null
  const discoveryStale = discoveryAgeDays !== null && discoveryAgeDays > MAX_DISCOVERY_AGE_DAYS
  const plan = tradeSetup ? buildTradePlan(tradeSetup) : null

  useEffect(() => {
    prevIntervalRef.current = interval
    intervalRef.current = interval
    periodRef.current = period
    chartRef.current?.applyOptions({
      localization: {
        timeFormatter: (time: Time) => formatChartTime(time, interval),
      },
      timeScale: {
        tickMarkFormatter: (time: Time, type: TickMarkType) => formatChartTick(time, type, interval, period),
      },
    })
  }, [interval, period])
  useEffect(() => {
    setExpandedEvents(new Set())
  }, [setupInterval])

  const fitSelectedPeriod = useCallback(() => {
    if (!chartRef.current) return
    const timeScale = chartRef.current.timeScale()
    const visibleData = getVisibleChartData(chartData, period, interval)
    if ((interval in SESSION_BARS || interval === '1wk') && visibleData.length > 0) {
      const lastIndex = chartData.length - 1
      timeScale.setVisibleLogicalRange({
        from: chartData.length - visibleData.length - 0.5,
        to: lastIndex + 0.5,
      })
    } else {
      timeScale.fitContent()
    }
  }, [chartData, period, interval])

  const handleRefresh = useCallback(async () => {
    const chartKey = ['chart', symbol, chartRequestPeriod, interval]
    const setupKey = ['trade-setup-multi', symbol]
    const quoteKey = ['latest-quote', symbol]
    queryClient.setQueryData(chartKey, undefined)
    queryClient.setQueryData(setupKey, undefined)
    queryClient.setQueryData(quoteKey, undefined)
    await Promise.all([
      queryClient.fetchQuery({ queryKey: chartKey, queryFn: () => getChartData(symbol!, chartRequestPeriod, interval, true) }),
      queryClient.fetchQuery({ queryKey: setupKey, queryFn: () => getMultiTradeSetup(symbol!, true) }),
      queryClient.fetchQuery({ queryKey: quoteKey, queryFn: () => getLatestQuote(symbol!, true) }),
    ])
  }, [symbol, chartRequestPeriod, interval, queryClient])

  const handleChartIntervalChange = useCallback((nextInterval: string) => {
    setInterval(nextInterval)
    if ((SETUP_INTERVALS as readonly string[]).includes(nextInterval)) {
      setSetupInterval(nextInterval)
    }
    if (nextInterval in SESSION_BARS) setPeriod('1d')
    else if (nextInterval === '1d') setPeriod('1y')
    else if (nextInterval === '1wk') setPeriod('2y')
  }, [])

  // Effect 1: Create chart once on mount
  useEffect(() => {
    if (!chartContainerRef.current) return

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { color: '#ffffff' },
        textColor: '#333',
      },
      localization: {
        timeFormatter: (time: Time) => formatChartTime(time, intervalRef.current),
      },
      grid: {
        vertLines: { color: '#f0f0f0' },
        horzLines: { color: '#f0f0f0' },
      },
      crosshair: {
        mode: 1,
      },
      rightPriceScale: {
        borderColor: '#e0e0e0',
      },
      timeScale: {
        borderColor: '#e0e0e0',
        timeVisible: true,
        secondsVisible: false,
        tickMarkFormatter: (time: Time, type: TickMarkType) => formatChartTick(time, type, intervalRef.current, periodRef.current),
      },
      width: chartContainerRef.current.clientWidth,
      height: 450,
    })

    chartRef.current = chart

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: POS,
      downColor: NEG,
      borderDownColor: NEG,
      borderUpColor: POS,
      wickDownColor: NEG,
      wickUpColor: POS,
    })
    candleSeriesRef.current = candleSeries

    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: '#cbd5e1',
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    })
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    })
    volumeSeriesRef.current = volumeSeries

    ma50SeriesRef.current = chart.addSeries(LineSeries, {
      color: MA_TONE['MA 50'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    ma100SeriesRef.current = chart.addSeries(LineSeries, {
      color: MA_TONE['MA 100'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    ma200SeriesRef.current = chart.addSeries(LineSeries, {
      color: MA_TONE['MA 200'], lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
    })
    ema8SeriesRef.current = chart.addSeries(LineSeries, {
      color: MA_TONE['EMA 8'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    ema21SeriesRef.current = chart.addSeries(LineSeries, {
      color: MA_TONE['EMA 21'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    ema50SeriesRef.current = chart.addSeries(LineSeries, {
      color: MA_TONE['EMA 50'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    bbMiddleSeriesRef.current = chart.addSeries(LineSeries, {
      color: BB_TONE, lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false,
    })
    bbUpperSeriesRef.current = chart.addSeries(LineSeries, {
      color: BB_TONE, lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false,
    })
    bbLowerSeriesRef.current = chart.addSeries(LineSeries, {
      color: BB_TONE, lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false,
    })

    // Crosshair legend — write directly to DOM to avoid React re-renders
    chart.subscribeCrosshairMove((param) => {
      const el = legendRef.current
      if (!el) return
      if (!param.time || param.seriesData.size === 0) {
        el.style.opacity = '0.6'
        return
      }
      el.style.opacity = '1'
      const seriesMap = [
        { series: candleSeriesRef.current!, label: 'OHLC', color: INK, isCandle: true },
        { series: ma50SeriesRef.current!, label: 'MA 50', color: MA_TONE['MA 50'], isCandle: false },
        { series: ma100SeriesRef.current!, label: 'MA 100', color: MA_TONE['MA 100'], isCandle: false },
        { series: ma200SeriesRef.current!, label: 'MA 200', color: MA_TONE['MA 200'], isCandle: false },
        { series: ema8SeriesRef.current!, label: 'EMA 8', color: MA_TONE['EMA 8'], isCandle: false },
        { series: ema21SeriesRef.current!, label: 'EMA 21', color: MA_TONE['EMA 21'], isCandle: false },
        { series: ema50SeriesRef.current!, label: 'EMA 50', color: MA_TONE['EMA 50'], isCandle: false },
        { series: bbMiddleSeriesRef.current!, label: 'BB Mid', color: BB_TONE, isCandle: false },
        { series: bbUpperSeriesRef.current!, label: 'BB Up', color: BB_TONE, isCandle: false },
        { series: bbLowerSeriesRef.current!, label: 'BB Lo', color: BB_TONE, isCandle: false },
      ]
      let line1 = ''
      let line2 = ''
      for (const item of seriesMap) {
        const d = param.seriesData.get(item.series) as any
        if (!d) continue
        if (item.isCandle) {
          line1 += `<span style="color:${item.color}"><b>O</b> ${d.open?.toFixed(2)}  <b>H</b> ${d.high?.toFixed(2)}  <b>L</b> ${d.low?.toFixed(2)}  <b>C</b> ${d.close?.toFixed(2)}</span> `
        } else if (d.value != null) {
          line2 += `<span style="color:${item.color}">● ${item.label}: ${d.value.toFixed(2)}</span>  `
        }
      }
      const vol = param.seriesData.get(volumeSeriesRef.current!) as any
      if (vol?.value != null) {
        line1 += `<span style="color:${MUTED}">Vol: ${(vol.value / 1e6).toFixed(2)}M</span>`
      }
      el.innerHTML = line1 + '<br/>' + line2
    })

    // Handle resize
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: chartContainerRef.current.clientWidth })
      }
    }
    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Effect: sync chart height when chartHeight or fullscreen changes
  useEffect(() => {
    if (chartRef.current && chartContainerRef.current) {
      const h = isFullscreen ? chartContainerRef.current.clientHeight : chartHeight
      chartRef.current.applyOptions({ height: h, width: chartContainerRef.current.clientWidth })
      fitSelectedPeriod()
    }
  }, [chartHeight, isFullscreen, fitSelectedPeriod])

  const handleZoomIn = useCallback(() => {
    if (chartRef.current) {
      const timeScale = chartRef.current.timeScale()
      const range = timeScale.getVisibleLogicalRange()
      if (range) {
        const mid = (range.from + range.to) / 2
        const span = (range.to - range.from) * 0.35
        timeScale.setVisibleLogicalRange({ from: mid - span, to: mid + span })
      }
    }
  }, [])

  const handleZoomOut = useCallback(() => {
    if (chartRef.current) {
      const timeScale = chartRef.current.timeScale()
      const range = timeScale.getVisibleLogicalRange()
      if (range) {
        const span = (range.to - range.from) * 1.5
        timeScale.setVisibleLogicalRange({ from: Math.max(-0.5, range.to - span), to: range.to })
      }
    }
  }, [])

  const handleResetZoom = useCallback(() => {
    fitSelectedPeriod()
  }, [fitSelectedPeriod])

  const handleResizeChart = useCallback((delta: number) => {
    setChartHeight(h => Math.max(250, Math.min(900, h + delta)))
  }, [])

  const toggleFullscreen = useCallback(() => {
    setIsFullscreen(f => !f)
  }, [])

  // Effect 2: Update chart data in-place (no destroy/recreate)
  useEffect(() => {
    if (chartData.length === 0 || !candleSeriesRef.current) return

    const candleData: CandlestickData<Time>[] = chartData.map(d => ({
      time: d.time as Time, open: d.open, high: d.high, low: d.low, close: d.close,
    }))
    const volumeData: HistogramData<Time>[] = chartData.map(d => ({
      time: d.time as Time,
      value: d.volume,
      color: d.close >= d.open ? 'rgba(21, 128, 61, 0.28)' : 'rgba(185, 28, 28, 0.28)',
    }))

    candleSeriesRef.current.setData(candleData)
    volumeSeriesRef.current?.setData(volumeData)
    ma50SeriesRef.current?.setData(buildSmaSeries(chartData, 50))
    ma100SeriesRef.current?.setData(buildSmaSeries(chartData, 100))
    ma200SeriesRef.current?.setData(buildSmaSeries(chartData, 200))
    ema8SeriesRef.current?.setData(buildEmaSeries(chartData, 8))
    ema21SeriesRef.current?.setData(buildEmaSeries(chartData, 21))
    ema50SeriesRef.current?.setData(buildEmaSeries(chartData, 50))

    const bollinger = buildBollingerBands(chartData, 20, 2)
    bbMiddleSeriesRef.current?.setData(bollinger.middle)
    bbUpperSeriesRef.current?.setData(bollinger.upper)
    bbLowerSeriesRef.current?.setData(bollinger.lower)

    fitSelectedPeriod()
  }, [chartData, fitSelectedPeriod])

  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return
    if (!showRsi) {
      if (rsiSeriesRef.current) {
        chart.removeSeries(rsiSeriesRef.current)
        rsiSeriesRef.current = null
      }
      if (rsiPaneIndexRef.current !== null && chart.panes()[rsiPaneIndexRef.current]) {
        chart.removePane(rsiPaneIndexRef.current)
      }
      rsiPaneIndexRef.current = null
      return
    }
    if (!rsiSeriesRef.current) {
      const pane = chart.addPane()
      pane.setHeight(110)
      rsiPaneIndexRef.current = pane.paneIndex()
      const series = chart.addSeries(LineSeries, {
        color: INFO,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: true,
        title: `RSI 14 · ${interval}`,
      }, pane.paneIndex())
      series.createPriceLine({ price: 70, color: NEG, lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '70' })
      series.createPriceLine({ price: 30, color: POS, lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '30' })
      rsiSeriesRef.current = series
    }
    rsiSeriesRef.current.setData(buildRsiSeries(chartData))
    rsiSeriesRef.current.applyOptions({ title: `RSI 14 · ${interval}` })
  }, [showRsi, chartData, interval])

  const lastPrice = chartData.length > 0 ? chartData[chartData.length - 1] : null
  const prevPrice = chartData.length > 1 ? chartData[chartData.length - 2] : null
  const displayPrice = latestQuote?.price ?? lastPrice?.close ?? null
  const chartPriceChange = lastPrice && prevPrice ? lastPrice.close - prevPrice.close : 0
  const chartPriceChangePercent = prevPrice ? (chartPriceChange / prevPrice.close) * 100 : 0
  const priceChange = latestQuote?.change ?? chartPriceChange
  const priceChangePercent = latestQuote?.change_percent ?? chartPriceChangePercent

  const verdict = plan && tradeSetup
    ? evaluatePlan(plan, tradeSetup, displayPrice, discoveryState, discoveryStale)
    : null
  const scannerEvents = tickerScannerEvents.events
  const bestStrategyGrade = (() => {
    const pullback = tradeSetup?.strategy_results.momentum_pullback
    const bounce = tradeSetup?.strategy_results.bearish_bounce
    if (pullback && bounce) {
      return pullback.score >= bounce.score
        ? `Pullback ${pullback.grade} (${pullback.score}/100)`
        : `Bearish bounce ${bounce.grade} (${bounce.score}/100)`
    }
    if (pullback) return `Pullback ${pullback.grade} (${pullback.score}/100)`
    if (bounce) return `Bearish bounce ${bounce.grade} (${bounce.score}/100)`
    return null
  })()
  const visibleConfluenceZones = (() => {
    const selected = new Map<string, (typeof confluenceZones)[number]>()
    const add = (zone: (typeof confluenceZones)[number] | undefined) => {
      if (zone) selected.set(`${zone.low}-${zone.high}`, zone)
    }
    const supports = confluenceZones.filter(zone => zone.role === 'SUPPORT')
      .sort((a, b) => Math.abs(a.distance_pct) - Math.abs(b.distance_pct))
    const resistances = confluenceZones.filter(zone => zone.role === 'RESISTANCE')
      .sort((a, b) => Math.abs(a.distance_pct) - Math.abs(b.distance_pct))
    const activeZones = confluenceZones.filter(zone => zone.role === 'ACTIVE')
      .sort((a, b) => Math.abs(a.distance_pct) - Math.abs(b.distance_pct))
    add(activeZones[0])
    add(supports[0])
    add(resistances[0])

    const preferredRole = techSide === 'LONG' ? 'RESISTANCE'
      : techSide === 'SHORT' ? 'SUPPORT' : null
    confluenceZones
      .filter(zone => !selected.has(`${zone.low}-${zone.high}`))
      .sort((a, b) => {
        const score = (zone: (typeof confluenceZones)[number]) => {
          const trendWeight = zone.role === preferredRole ? 0.75 : 1
          const strengthWeight = zone.strength === 'STRONG_CONFLUENCE' ? 0.85 : 1
          return Math.abs(zone.distance_pct) * trendWeight * strengthWeight
        }
        return score(a) - score(b)
      })
      .forEach(zone => {
        if (selected.size < 7) add(zone)
      })
    return [...selected.values()].sort((a, b) => b.midpoint - a.midpoint)
  })()

  return (
    <div>
      <div className="card-header" style={{ border: 'none', padding: 0, marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
            <button className="btn btn-secondary btn-sm" onClick={() => navigate(-1)}>
              ← Back
            </button>
            <div>
              <h1 style={{ fontSize: '1.75rem', fontWeight: 700 }}>{symbol}</h1>
              {displayPrice !== null && (
                <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.75rem', marginTop: '0.25rem' }}>
                  <span style={{ fontSize: '1.5rem', fontWeight: 600 }}>
                    ${displayPrice.toFixed(2)}
                  </span>
                  <span style={{ 
                    color: priceChange >= 0 ? POS : NEG,
                    fontWeight: 500
                  }}>
                    {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)} ({priceChangePercent >= 0 ? '+' : ''}{priceChangePercent.toFixed(2)}%)
                  </span>
                </div>
              )}
              {(latestQuote || lastPrice) && (
                <div style={{ fontSize: '0.75rem', color: MUTED, marginTop: '0.15rem' }}>
                  As of {latestQuote
                    ? formatQuoteTime(latestQuote)
                    : formatChartTime(lastPrice!.time as Time, interval)}
                </div>
              )}
            </div>
          </div>
          {visibleChartData.length > 0 && (
            <div style={{ display: 'flex', gap: '2.5rem', alignItems: 'center', marginLeft: 'auto' }}>
              {[
                { label: 'Period High', value: `$${Math.max(...visibleChartData.map(d => d.high)).toFixed(2)}`, color: POS },
                { label: 'Period Low', value: `$${Math.min(...visibleChartData.map(d => d.low)).toFixed(2)}`, color: NEG },
                { label: 'Avg Vol', value: `${(visibleChartData.reduce((s, d) => s + d.volume, 0) / visibleChartData.length / 1000000).toFixed(2)}M` },
                { label: 'Points', value: `${visibleChartData.length}` },
              ].map(stat => (
                <div key={stat.label} style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: '1.3rem', fontWeight: 600, color: stat.color || INK }}>{stat.value}</div>
                  <div style={{ fontSize: '0.78rem', color: MUTED, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{stat.label}</div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Chart Controls */}
      <div className="card" style={{ marginBottom: 0, borderBottomLeftRadius: 0, borderBottomRightRadius: 0, paddingBottom: '0.75rem' }}>
        <div className="filter-bar" style={{ marginBottom: 0 }}>
          <div className="tabs" style={{ borderBottom: 'none', marginBottom: 0 }}>
            {['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y'].map(p => (
              <button
                key={p}
                className={`tab ${period === p ? 'active' : ''}`}
                onClick={() => setPeriod(p)}
              >
                {p.toUpperCase()}
              </button>
            ))}
          </div>
          <select value={interval} onChange={(e) => {
            handleChartIntervalChange(e.target.value)
          }}>
            <option value="1m">1 Minute</option>
            <option value="5m">5 Minutes</option>
            <option value="15m">15 Minutes</option>
            <option value="30m">30 Minutes</option>
            <option value="1h">1 Hour</option>
            <option value="1d">Daily</option>
            <option value="1wk">Weekly</option>
          </select>
          <button className="btn btn-primary btn-sm" onClick={handleRefresh} disabled={loading || setupLoading} style={{ marginLeft: '0.5rem' }}>
            {loading ? 'Loading...' : '🔄 Refresh'}
          </button>
        </div>
      </div>

      {/* Chart */}
      <div
        className={isFullscreen ? undefined : 'chart-container'}
        style={{
          position: isFullscreen ? 'fixed' : 'relative',
          ...(isFullscreen
            ? { inset: 0, zIndex: 1000, background: '#fff', display: 'flex', flexDirection: 'column' }
            : { borderTopLeftRadius: 0, borderTopRightRadius: 0, borderTop: 'none', paddingTop: 0, height: 'auto', overflow: 'visible' }),
        }}
      >
        {/* Fullscreen header bar */}
        {isFullscreen && (
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.75rem',
            padding: '6px 12px',
            borderBottom: '1px solid #e2e8f0',
            background: '#f8fafc',
            flexShrink: 0,
          }}>
            <span style={{ fontWeight: 700, fontSize: '1.1rem' }}>{symbol}</span>
            <div style={{ display: 'flex', gap: '2px', background: '#e2e8f0', borderRadius: '4px', padding: '2px' }}>
              {['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y'].map(p => (
                <button
                  key={p}
                  onClick={() => setPeriod(p)}
                  style={{
                    padding: '2px 8px',
                    fontSize: '0.75rem',
                    fontWeight: period === p ? 700 : 400,
                    border: 'none',
                    borderRadius: '3px',
                    background: period === p ? '#fff' : 'transparent',
                    color: period === p ? INK : MUTED,
                    cursor: 'pointer',
                    boxShadow: period === p ? '0 1px 2px rgba(0,0,0,0.1)' : 'none',
                  }}
                >
                  {p.toUpperCase()}
                </button>
              ))}
            </div>
            <select
              value={interval}
              onChange={(e) => handleChartIntervalChange(e.target.value)}
              style={{ fontSize: '0.8rem', padding: '3px 6px', borderRadius: '4px', border: '1px solid #cbd5e1' }}
            >
              <option value="1m">1m</option>
              <option value="5m">5m</option>
              <option value="15m">15m</option>
              <option value="30m">30m</option>
              <option value="1h">1h</option>
              <option value="1d">1d</option>
              <option value="1wk">1wk</option>
            </select>
            {displayPrice !== null && (
              <span style={{ fontSize: '0.9rem', fontWeight: 600 }}>
                ${displayPrice.toFixed(2)}
                <span style={{ color: priceChange >= 0 ? POS : NEG, marginLeft: '6px', fontSize: '0.8rem' }}>
                  {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)} ({priceChangePercent >= 0 ? '+' : ''}{priceChangePercent.toFixed(2)}%)
                </span>
              </span>
            )}
          </div>
        )}
        {/* Zoom & resize toolbar */}
        <div style={{
          position: 'absolute',
          top: isFullscreen ? 8 : 8,
          right: 12,
          zIndex: 20,
          display: 'flex',
          gap: '4px',
          background: 'rgba(255,255,255,0.92)',
          borderRadius: '6px',
          padding: '3px',
          boxShadow: '0 1px 4px rgba(0,0,0,0.12)',
          border: '1px solid #e2e8f0',
        }}>
          <button
            onClick={() => setShowRsi(value => !value)}
            title="Toggle RSI (14) pane"
            aria-pressed={showRsi}
            style={{
              height: 28,
              minWidth: 34,
              border: 'none',
              borderRadius: '4px',
              background: showRsi ? INFO_SOFT : 'transparent',
              cursor: 'pointer',
              fontSize: '0.68rem',
              fontWeight: 700,
              color: showRsi ? INFO : MUTED,
            }}
          >RSI</button>
          {[
            { label: '+', title: 'Zoom In (time axis)', onClick: handleZoomIn },
            { label: '−', title: 'Zoom Out (time axis)', onClick: handleZoomOut },
            { label: '⟲', title: 'Reset Zoom', onClick: handleResetZoom },
            { label: '↕+', title: 'Increase chart height', onClick: () => handleResizeChart(100) },
            { label: '↕−', title: 'Decrease chart height', onClick: () => handleResizeChart(-100) },
            { label: isFullscreen ? '✕' : '⛶', title: isFullscreen ? 'Exit Fullscreen' : 'Fullscreen', onClick: toggleFullscreen },
          ].map((btn) => (
            <button
              key={btn.title}
              onClick={btn.onClick}
              title={btn.title}
              style={{
                width: 30,
                height: 28,
                border: 'none',
                borderRadius: '4px',
                background: 'transparent',
                cursor: 'pointer',
                fontSize: '1rem',
                fontWeight: 600,
                color: '#475569',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                lineHeight: 1,
              }}
              onMouseEnter={e => { (e.target as HTMLElement).style.background = '#e2e8f0' }}
              onMouseLeave={e => { (e.target as HTMLElement).style.background = 'transparent' }}
            >
              {btn.label}
            </button>
          ))}
        </div>
        {loading && (
          <div className="loading" style={{ position: 'absolute', inset: 0, zIndex: 10, display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'rgba(255,255,255,0.7)' }}>
            <div className="spinner"></div>
            <span>Loading chart data...</span>
          </div>
        )}
        <div style={{ position: 'relative', flex: isFullscreen ? 1 : undefined }}>
          <div 
            ref={chartContainerRef} 
            style={{ 
              width: '100%', 
              height: isFullscreen ? '100%' : `${chartHeight}px`,
            }} 
          />
          {/* OHLCV + MA legend overlay — TradingView style */}
          <div
            ref={legendRef}
            style={{
              position: 'absolute',
              top: 8,
              left: 10,
              zIndex: 15,
              fontSize: '0.78rem',
              lineHeight: '1.7',
              color: INK,
              pointerEvents: 'none',
              opacity: 0.6,
              maxWidth: '60%',
            }}
          />
        </div>
      </div>

      {/* Trade Setup Analysis */}
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <div className="card-header" style={{ flexWrap: 'wrap', gap: '0.75rem', alignItems: 'flex-start' }}>
          <div>
            <h2 className="card-title" style={{ marginBottom: '0.1rem' }}>Trade Setup</h2>
            {tradeSetup && (
              <div style={{ fontSize: '0.72rem', color: MUTED }}>
                {tradeSetup.date} · close {money(tradeSetup.last_close)} · read on {INTERVAL_NOUN[setupInterval] ?? setupInterval} bars
                {tradeSetup.ema_alignment.confirm_interval
                  ? `, confirmed against ${INTERVAL_NOUN[tradeSetup.ema_alignment.confirm_interval] ?? tradeSetup.ema_alignment.confirm_interval}`
                  : ''}
                {relativeAge(tradeSetup.computed_at) ? ` · computed ${relativeAge(tradeSetup.computed_at)}` : ''}
              </div>
            )}
          </div>
          <Pill
            text={interval === setupInterval ? `${setupInterval} chart + setup` : `${interval} chart · ${setupInterval} setup`}
            tone={interval === setupInterval ? INFO : MUTED}
            title={interval === setupInterval
              ? 'Chart and trade setup use the same bars.'
              : `${interval} changes the chart only; the trade plan remains on ${setupInterval}.`}
          />
        </div>

        {setupLoading && !tradeSetup && (
          <div className="loading" style={{ padding: '2rem' }}>
            <div className="spinner"></div>
            <span>Analyzing strategies...</span>
          </div>
        )}

        {setupLoading && tradeSetup && (
          <div style={{ fontSize: '0.68rem', color: MUTED, textAlign: 'right', marginBottom: '0.35rem' }}>
            Refreshing synchronized timeframes…
          </div>
        )}

        {!setupLoading && !tradeSetup && (
          <p style={{ color: 'var(--text-secondary)', padding: '1rem' }}>
            No trade setup data available.
          </p>
        )}

        {tradeSetup && (
          <>
            {/* Decision bar — every tile recomputes on the selected interval */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(145px, 1fr))', gap: '0.6rem', marginBottom: '0.85rem' }}>
              <Tile
                label="Bias"
                value={techSide ?? 'NEUTRAL'}
                tone={techSide === 'LONG' ? POS : techSide === 'SHORT' ? NEG : MUTED}
                accent={techSide === 'LONG' ? POS : techSide === 'SHORT' ? NEG : MUTED}
                sub={`${tradeSetup.direction.conviction} · ${tradeSetup.direction.bull_signals}↑ / ${tradeSetup.direction.bear_signals}↓`}
                note={tradeSetup.ema_alignment.multi_tf_agree === null
                  ? `No ${tradeSetup.ema_alignment.confirm_interval} data to confirm`
                  : tradeSetup.ema_alignment.multi_tf_agree
                    ? `✓ ${tradeSetup.ema_alignment.confirm_interval} agrees`
                    : `⚠ ${tradeSetup.ema_alignment.confirm_interval} diverges`}
                noteTone={tradeSetup.ema_alignment.multi_tf_agree === null ? MUTED
                  : tradeSetup.ema_alignment.multi_tf_agree ? POS : WARN}
              />

              <Tile
                label="Setup quality"
                value={tradeSetup.confluence.grade}
                tone={gradeTone(tradeSetup.confluence.grade)}
                sub={`${tradeSetup.confluence.count} directional inputs`}
                note={bestStrategyGrade ?? undefined}
              />

              <Tile
                label="Trend"
                value={tradeSetup.ema_alignment.primary}
                tone={trendTone(tradeSetup.ema_alignment.primary)}
                accent={trendTone(tradeSetup.ema_alignment.primary)}
                sub={tradeSetup.golden_cross
                  ? tradeSetup.golden_cross.type === 'Golden Cross' || tradeSetup.golden_cross.type === 'Death Cross'
                    ? `${tradeSetup.golden_cross.type}${tradeSetup.golden_cross.bars_ago !== null ? ` · ${tradeSetup.golden_cross.bars_ago} bars ago` : ''}`
                    : tradeSetup.golden_cross.type.includes('Bullish') ? '50 SMA above 200 SMA' : '50 SMA below 200 SMA'
                  : '50/200 SMA needs more history'}
                note={`Directional consistency ${plainPct(tradeSetup.technicals.trend_consistency, 0)}`}
              />

              <Tile
                label="Momentum"
                value={tradeSetup.momentum.state}
                tone={trendTone(tradeSetup.momentum.state)}
                accent={trendTone(tradeSetup.momentum.state)}
                sub={`RSI ${tradeSetup.technicals.rsi.toFixed(1)} · ${tradeSetup.technicals.rsi_state}`}
                note={`MACD ${tradeSetup.technicals.macd_state.replace(/_/g, ' ').toLowerCase()}`}
                noteTone={tradeSetup.technicals.rsi > 70 || tradeSetup.technicals.rsi < 30 ? WARN : undefined}
              />

              <Tile
                label="Timing"
                value={tradeSetup.timing.urgency}
                tone={INK}
                accent={tradeSetup.timing.urgency === 'Immediate' ? POS : tradeSetup.timing.urgency === 'Watchlist' ? MUTED : WARN}
                sub={tradeSetup.duration.estimate}
                note={tradeSetup.timing.detail.length > 58 ? `${tradeSetup.timing.detail.slice(0, 58)}…` : tradeSetup.timing.detail}
              />
            </div>

            {/* Executable plan — levels, risk math and sizing */}
            {plan && verdict ? (() => {
              const isLong = plan.side === 'LONG'
              // Lay the ladder out on a real price axis: low price left, high price right.
              const lo = isLong ? plan.stop : plan.target
              const hi = isLong ? plan.target : plan.stop
              const span = hi - lo
              const pctOf = (p: number) => Math.max(0, Math.min(100, ((p - lo) / span) * 100))
              const entryPct = pctOf(plan.entry)
              const showNow = displayPrice !== null && Math.abs(displayPrice - plan.entry) > 0.005
              const nowPct = showNow ? pctOf(displayPrice!) : null
              const nowBeyond = showNow && (displayPrice! < lo || displayPrice! > hi)

              const endCap = (
                p: number, role: string, tone: string, source: string, align: 'left' | 'right',
              ) => (
                <div style={{ textAlign: align, minWidth: 0 }}>
                  <div style={{ ...LABEL, color: tone }}>{role}</div>
                  <div style={{ fontSize: '1.25rem', fontWeight: 700, color: tone, lineHeight: 1.2 }}>{money(p)}</div>
                  <div style={{ fontSize: '0.7rem', color: MUTED }}>
                    {signedPct(((p - plan.entry) / plan.entry) * 100)} · {source}
                  </div>
                </div>
              )

              return (
                <div style={{ border: '1px solid #e2e8f0', borderRadius: '0.5rem', marginBottom: '1rem', overflow: 'hidden' }}>
                  <div style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    gap: '0.75rem',
                    flexWrap: 'wrap',
                    padding: '0.5rem 0.85rem',
                    background: isLong ? POS_SOFT : NEG_SOFT,
                    borderBottom: '1px solid #e2e8f0',
                  }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                      <strong style={{ fontSize: '0.85rem', color: isLong ? POS : NEG }}>{plan.side} plan</strong>
                      <Pill text={verdict.label} tone={verdict.tone} solid />
                    </div>
                    <span style={{ fontSize: '0.72rem', color: MUTED }}>
                      Nearest technical stop and first target on {INTERVAL_NOUN[setupInterval] ?? setupInterval} bars
                    </span>
                  </div>

                  <div style={{ padding: '0.9rem 0.85rem 0.5rem' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.5rem' }}>
                      {isLong
                        ? endCap(plan.stop, 'Stop', NEG, plan.stopLabel, 'left')
                        : endCap(plan.target, 'Target', POS, plan.targetLabel, 'left')}
                      {isLong
                        ? endCap(plan.target, 'Target', POS, plan.targetLabel, 'right')
                        : endCap(plan.stop, 'Stop', NEG, plan.stopLabel, 'right')}
                    </div>

                    {/* Segment widths are the reward:risk ratio, drawn to scale. */}
                    <div style={{ position: 'relative' }}>
                      <div style={{ display: 'flex', height: 10, borderRadius: 5, overflow: 'hidden', background: LINE }}>
                        <div style={{ flexGrow: isLong ? plan.risk : plan.reward, background: isLong ? '#fbcfcf' : '#bfe5cd' }} />
                        <div style={{ flexGrow: isLong ? plan.reward : plan.risk, background: isLong ? '#bfe5cd' : '#fbcfcf' }} />
                      </div>
                      <div style={{
                        position: 'absolute', left: `${entryPct}%`, top: -5, bottom: -5,
                        width: 2, background: INK, transform: 'translateX(-1px)',
                      }} />
                      {nowPct !== null && (
                        <div style={{
                          position: 'absolute', left: `${nowPct}%`, top: -9, bottom: -9,
                          width: 2, background: INFO, transform: 'translateX(-1px)',
                        }} />
                      )}
                    </div>

                    <div style={{ position: 'relative', height: nowPct !== null ? 62 : 42, marginTop: '0.4rem' }}>
                      <div style={{
                        position: 'absolute',
                        top: 0,
                        left: `${Math.max(10, Math.min(90, entryPct))}%`,
                        transform: 'translateX(-50%)',
                        textAlign: 'center',
                        whiteSpace: 'nowrap',
                      }}>
                        <div style={LABEL}>Entry · last close</div>
                        <div style={{ fontSize: '1rem', fontWeight: 700, color: INK }}>{money(plan.entry)}</div>
                      </div>
                      {nowPct !== null && (
                        <div style={{
                          position: 'absolute',
                          left: `${Math.max(10, Math.min(90, nowPct))}%`,
                          top: 42,
                          transform: 'translateX(-50%)',
                          whiteSpace: 'nowrap',
                          fontSize: '0.7rem',
                          fontWeight: 600,
                          color: INFO,
                        }}>
                          ▲ now {money(displayPrice)}{nowBeyond ? ' (outside plan)' : ''}
                        </div>
                      )}
                    </div>

                    <div style={{
                      display: 'flex', gap: '1.25rem', flexWrap: 'wrap',
                      fontSize: '0.72rem', color: MUTED,
                      borderTop: '1px solid #f1f5f9', paddingTop: '0.45rem',
                    }}>
                      <span>Risk <strong style={{ color: NEG }}>{money(plan.risk)}</strong> ({plainPct(plan.riskPct)})</span>
                      <span>Reward <strong style={{ color: POS }}>{money(plan.reward)}</strong> ({plainPct(plan.rewardPct)})</span>
                      <span>Ratio <strong style={{ color: INK }}>{plan.rr.toFixed(2)}R</strong></span>
                    </div>
                  </div>

                  <div
                    title={verdict.tooltip}
                    style={{
                      padding: '0.5rem 0.85rem',
                      borderTop: '1px solid #f1f5f9',
                      background: SURFACE,
                      fontSize: '0.74rem',
                      color: verdict.tone === MUTED ? INK : verdict.tone,
                      cursor: 'help',
                    }}
                  >
                    {verdict.summary}
                  </div>

                  {verdict.cautions.length > 0 && (
                    <div style={{ borderTop: '1px solid #f1f5f9', background: WARN_SOFT }}>
                      {verdict.cautions.map((caution, i) => (
                        <div key={i} style={{ padding: '0.45rem 0.85rem', fontSize: '0.72rem', color: INK }}>
                          <span style={{ color: WARN, fontWeight: 700 }}>⚠ </span>
                          {caution.detail}
                          <span style={{ color: MUTED }}> — {caution.source}, not scored above</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )
            })() : (
              <div style={{ padding: '0.85rem', marginBottom: '1rem', border: '1px dashed #cbd5e1', borderRadius: '0.5rem', fontSize: '0.82rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                  <Pill text="No plan" tone={MUTED} solid />
                  <strong style={{ fontSize: '0.85rem', color: INK }}>No executable plan on this timeframe</strong>
                </div>
                <div style={{ color: MUTED }}>
                  {techSide
                    ? `A ${techSide} needs a stop and a first target bracketing ${money(tradeSetup.last_close)} — one of them is missing on ${INTERVAL_NOUN[setupInterval] ?? setupInterval} bars.`
                    : `Technicals are neutral on ${INTERVAL_NOUN[setupInterval] ?? setupInterval} bars, so there is no side to plan.`}
                </div>
              </div>
            )}

            {/* Evidence tabs */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.25rem', borderBottom: '1px solid #e2e8f0', marginBottom: '0.85rem' }}>
              {([
                ['timeframes', 'Timeframes', Object.keys(setups).length],
                ['levels', 'Levels', visibleConfluenceZones.length],
                ['fibonacci', 'Fibonacci', null],
                ['scanner', 'Scanner history', scannerEvents.length],
              ] as const).map(([key, label, count]) => (
                <button
                  key={key}
                  onClick={() => setTab(key)}
                  style={{
                    padding: '0.4rem 0.8rem',
                    border: 'none',
                    background: 'transparent',
                    cursor: 'pointer',
                    fontSize: '0.8rem',
                    fontWeight: tab === key ? 700 : 500,
                    color: tab === key ? INK : MUTED,
                    borderBottom: `2px solid ${tab === key ? INFO : 'transparent'}`,
                  }}
                >
                  {label}{count ? ` (${count})` : ''}
                </button>
              ))}
            </div>

            {tab === 'timeframes' && (
              <div style={{ border: `1px solid ${LINE}`, borderRadius: '0.5rem', maxWidth: '100%', overflowX: 'auto', marginBottom: '0.85rem' }}>
                <table style={{ width: '100%', minWidth: 900, borderCollapse: 'collapse', fontSize: '0.76rem' }}>
                  <thead>
                    <tr style={{ background: SURFACE, borderBottom: `1px solid ${LINE}` }}>
                      {[
                        ['TF', 'Candle interval'],
                        ['Bias', 'Directional technical vote'],
                        ['Trend', 'EMA stack and 14-bar directional consistency'],
                        ['Direction', 'Completed-bar ADX(14) trend strength with +DI and −DI directional control'],
                        ['Momentum', 'RSI and MACD state'],
                        ['Volume flow', '5-vs-20 completed-bar volume, completed-bar RVOL, 8-bar slope and CMF'],
                        ['Volatility', 'Annualized 20-bar historical volatility, trailing percentile and ATR percentage'],
                      ].map(([label, description]) => (
                        <th key={label} title={description} style={{ padding: '7px 9px', textAlign: 'left', color: MUTED }}>{label}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {(['1mo', '1wk', '1d', '1h'] as const).map(timeframe => {
                      const setup = setups[timeframe]
                      if (!setup) return (
                        <tr key={timeframe}><td style={{ padding: '9px' }}>{timeframe}</td><td colSpan={6} style={{ color: MUTED }}>Unavailable</td></tr>
                      )
                      const side = sideOfBias(setup.direction.bias)
                      const macdLabel = setup.technicals.macd_state.replace(/_/g, ' ').toLowerCase()
                      return (
                        <tr key={timeframe} style={{ borderBottom: '1px solid #f1f5f9', background: timeframe === setupInterval ? INFO_SOFT : '#fff' }}>
                          <td style={{ padding: '9px', fontWeight: 700 }}>
                            {timeframe}{timeframe === setupInterval ? ' · primary' : timeframe === '1mo' ? ' · context' : ''}
                          </td>
                          <td style={{ padding: '9px', color: side === 'LONG' ? POS : side === 'SHORT' ? NEG : MUTED, fontWeight: 700 }}>{side ?? 'NEUTRAL'}</td>
                          <td style={{ padding: '9px' }}>
                            <strong style={{ color: trendTone(setup.ema_alignment.primary) }}>{setup.ema_alignment.primary}</strong>
                            <div style={{ color: MUTED, fontSize: '0.68rem' }}>{plainPct(setup.technicals.trend_consistency, 0)} consistency</div>
                          </td>
                          <td style={{ padding: '9px' }}>
                            <DirectionStrengthTrack
                              adx={setup.technicals.adx}
                              plusDi={setup.technicals.plus_di}
                              minusDi={setup.technicals.minus_di}
                            />
                          </td>
                          <td style={{ padding: '9px' }}>
                            <strong>RSI {setup.technicals.rsi.toFixed(1)}</strong>
                            <div style={{ color: MUTED, fontSize: '0.68rem' }}>{macdLabel}</div>
                          </td>
                          <td style={{ padding: '9px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.45rem', minWidth: 190 }}>
                              <VolumeSparkline
                                values={setup.technicals.volume_sparkline}
                                state={setup.technicals.volume_trend_state}
                                slopeState={setup.technicals.volume_slope_state}
                              />
                              <div>
                                <strong style={{ color: setup.technicals.volume_trend_state === 'EXPANDING' ? INFO : setup.technicals.volume_trend_state === 'CONTRACTING' ? WARN : INK }}>
                                  {setup.technicals.volume_trend_state.toLowerCase()}
                                  {setup.technicals.volume_trend_pct !== null ? ` ${signedPct(setup.technicals.volume_trend_pct, 0)}` : ''}
                                </strong>
                                <div style={{ color: MUTED, fontSize: '0.68rem' }}>
                                  {setup.technicals.relative_volume === null ? '—' : `${setup.technicals.relative_volume.toFixed(2)}× last completed`}
                                </div>
                                <div style={{ color: MUTED, fontSize: '0.68rem' }}>
                                  {setup.technicals.volume_slope_state.toLowerCase()} slope · {setup.technicals.volume_pressure.toLowerCase()}
                                </div>
                              </div>
                            </div>
                          </td>
                          <td style={{ padding: '9px' }}>
                            <VolatilityTrack
                              value={setup.technicals.historical_volatility_pct}
                              percentile={setup.technicals.historical_volatility_percentile}
                              state={setup.technicals.historical_volatility_state}
                              atrPct={setup.technicals.atr_pct}
                            />
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}

            {tab === 'levels' && (() => {
              const price = tradeSetup.last_close
              const rows = visibleConfluenceZones
              const firstBelow = rows.findIndex(zone => zone.midpoint < price)

              const priceRow = (
                <tr style={{ background: INFO_SOFT }}>
                  <td colSpan={6} style={{ padding: '5px 8px', fontSize: '0.74rem', fontWeight: 700, color: INFO }}>
                    ── Last {INTERVAL_NOUN[setupInterval] ?? setupInterval} close {money(price)}
                    {displayPrice !== null && Math.abs(displayPrice - price) > 0.005
                      ? ` · live ${money(displayPrice)}` : ''} ──
                  </td>
                </tr>
              )

              return (
                <>
                  {rows.length === 0 ? (
                    <p style={{ fontSize: '0.8rem', color: MUTED }}>No cross-timeframe zones resolved.</p>
                  ) : (
                    <div style={{ border: '1px solid #e2e8f0', borderRadius: '0.5rem', maxWidth: '100%', overflowX: 'auto', marginBottom: '1rem' }}>
                      <div style={{ padding: '0.5rem 0.75rem', background: SURFACE, borderBottom: `1px solid ${LINE}`, fontSize: '0.72rem', color: MUTED }}>
                        Showing {rows.length} nearest zones of {confluenceZones.length} · all contributing levels remain summarized in each zone
                      </div>
                      <table style={{ width: '100%', minWidth: 900, borderCollapse: 'collapse', fontSize: '0.78rem' }}>
                        <thead>
                          <tr style={{ background: SURFACE, borderBottom: '1px solid #e2e8f0' }}>
                            {['Zone', 'Distance', 'Role', 'Confluence', 'Evidence', 'Confirmation'].map((label, i) => (
                              <th key={label} style={{ padding: '6px 8px', textAlign: i <= 1 ? 'right' : 'left', color: MUTED, fontWeight: 700 }}>{label}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {rows.map((zone, i) => {
                            const strengthTone = zone.strength === 'STRONG_CONFLUENCE' ? POS
                              : zone.strength === 'CONFLUENCE' ? INFO : MUTED
                            const roleTone = zone.role === 'SUPPORT' ? POS : zone.role === 'RESISTANCE' ? NEG : WARN
                            const evidence = zone.references.slice(0, 4)
                              .map(reference => `${reference.interval} ${reference.label}`)
                            return (
                            <Fragment key={`${zone.low}-${zone.high}`}>
                              {i === firstBelow && priceRow}
                              <tr style={{ borderBottom: '1px solid #f1f5f9', background: zone.role === 'ACTIVE' ? WARN_SOFT : undefined }}>
                                <td style={{ padding: '6px 8px', textAlign: 'right', fontWeight: 700, whiteSpace: 'nowrap' }}>
                                  {Math.abs(zone.high - zone.low) > 0.005 ? `${money(zone.low)}–${money(zone.high)}` : money(zone.midpoint)}
                                </td>
                                <td style={{ padding: '6px 8px', textAlign: 'right', color: zone.distance_pct >= 0 ? POS : NEG }}>
                                  {signedPct(zone.distance_pct)}
                                </td>
                                <td style={{ padding: '6px 8px' }}><Pill text={zone.role.replace(/_/g, ' ')} tone={roleTone} /></td>
                                <td style={{ padding: '6px 8px' }}>
                                  <Pill text={zone.strength.replace(/_/g, ' ')} tone={strengthTone} />
                                  <div style={{ color: MUTED, fontSize: '0.68rem', marginTop: '0.15rem' }}>{zone.intervals.join(' · ')} · {zone.families.length} families</div>
                                </td>
                                <td style={{ padding: '6px 8px' }}>
                                  <div>{evidence.join(' · ')}</div>
                                  {zone.references.length > evidence.length && <div style={{ color: MUTED, fontSize: '0.68rem' }}>+{zone.references.length - evidence.length} more references</div>}
                                </td>
                                <td style={{ padding: '6px 8px' }}>
                                  {zone.confirmations.length > 0
                                    ? zone.confirmations.map((pattern, index) => (
                                      <div key={`${pattern.interval}-${pattern.bar_time}-${index}`} style={{ color: pattern.direction === 'BULLISH' ? POS : pattern.direction === 'BEARISH' ? NEG : MUTED }}>
                                        {pattern.interval} · {pattern.name}
                                      </div>
                                    ))
                                    : <span style={{ color: MUTED }}>No completed-candle confirmation</span>}
                                </td>
                              </tr>
                            </Fragment>
                          )})}
                          {firstBelow === -1 && priceRow}
                        </tbody>
                      </table>
                    </div>
                  )}
                </>
              )
            })()}

            {tab === 'fibonacci' && !tradeSetup.strategy_results.fibonacci && (
              <p style={{ fontSize: '0.8rem', color: MUTED }}>
                No confirmed Fibonacci swing on {INTERVAL_NOUN[setupInterval] ?? setupInterval} bars — the zigzag needs a completed
                leg larger than the detection threshold before levels can be drawn.
              </p>
            )}

            {tab === 'fibonacci' && tradeSetup.strategy_results.fibonacci && (() => {
              const fib = tradeSetup.strategy_results.fibonacci
              const active = fib.active_leg
              const price = tradeSetup.last_close
              const confirmedMove = fib.trend_direction === 'uptrend_retracement'
                ? `Low ${money(fib.swing_low)} (${fib.swing_low_date}) → High ${money(fib.swing_high)} (${fib.swing_high_date})`
                : `High ${money(fib.swing_high)} (${fib.swing_high_date}) → Low ${money(fib.swing_low)} (${fib.swing_low_date})`
              const activeProgress = active?.retracement_pct ?? 0

              interface FibMapRow {
                price: number
                label: string
                source: string
                meaning: string
                tone: string
              }
              const confirmedContinuationRows: FibMapRow[] = active ? [
                ...fib.retracement_levels
                  .filter(level => level.kind !== 'full_retracement')
                  .filter(level => active.end.type === 'high'
                    ? level.price > active.end.price
                    : level.price < active.end.price)
                  .map(level => ({
                    price: level.price,
                    label: `Confirmed ${level.name}`,
                    source: 'Confirmed-basis retracement',
                    meaning: active.end.type === 'high'
                      ? 'Structural recovery target beyond the provisional high'
                      : 'Structural decline target beyond the provisional low',
                    tone: INFO,
                  })),
                ...fib.extension_levels
                  .filter(level => active.end.type === 'high'
                    ? level.price > active.end.price
                    : level.price < active.end.price)
                  .map(level => ({
                    price: level.price,
                    label: `Extension ${level.name}`,
                    source: 'Confirmed-basis extension',
                    meaning: active.end.type === 'high'
                      ? 'Extended upside target after full recovery'
                      : 'Extended downside target after full retracement',
                    tone: WARN,
                  })),
              ] : []
              const rawFibMapRows: FibMapRow[] = active ? [
                {
                  price: active.start.type === 'low' ? fib.swing_high : fib.swing_low,
                  label: `Confirmed ${active.start.type === 'low' ? 'high' : 'low'}`,
                  source: 'Structural basis',
                  meaning: '100% recovery of the confirmed leg',
                  tone: INFO,
                },
                {
                  price: active.end.price,
                  label: `Provisional ${active.end.type}`,
                  source: 'Developing extreme',
                  meaning: active.end.type === 'high' ? 'Recovery high to exceed' : 'Decline low to break',
                  tone: WARN,
                },
                ...confirmedContinuationRows,
                ...active.levels.map(level => ({
                  price: level.price,
                  label: level.name,
                  source: 'Provisional retracement',
                  meaning: level.name === active.nearest_level
                    ? `Nearest level · ${level.price <= price ? 'support' : 'resistance'}`
                    : level.name === '50.0%' || level.name === '61.8%'
                      ? `Golden zone · ${level.price <= price ? 'support' : 'resistance'}`
                      : level.price <= price ? 'Potential support' : 'Potential resistance',
                  tone: level.name === active.nearest_level ? INFO : MUTED,
                })),
                {
                  price: active.start.price,
                  label: `Provisional origin · confirmed ${active.start.type}`,
                  source: 'Invalidation boundary',
                  meaning: active.end.type === 'high' ? 'Recovery fully retraced below here' : 'Decline fully retraced above here',
                  tone: NEG,
                },
              ] : []
              const fibRowsByPrice = new Map<string, FibMapRow>()
              rawFibMapRows.forEach(row => {
                const key = row.price.toFixed(2)
                if (!fibRowsByPrice.has(key)) fibRowsByPrice.set(key, row)
              })
              const allFibMapRows = [...fibRowsByPrice.values()]
              const aboveRows = allFibMapRows
                .filter(row => row.price >= price)
                .sort((a, b) => a.price - b.price)
              const belowRows = allFibMapRows
                .filter(row => row.price < price)
                .sort((a, b) => b.price - a.price)
              const trendRows = active?.end.type === 'high' ? aboveRows : belowRows
              const selectedFibRows = new Map<string, FibMapRow>()
              const selectFibRow = (row: FibMapRow | null | undefined) => {
                if (row) selectedFibRows.set(row.price.toFixed(2), row)
              }
              selectFibRow(aboveRows[0])
              selectFibRow(belowRows[0])
              selectFibRow(allFibMapRows.find(row => row.source === 'Developing extreme'))
              selectFibRow(trendRows.find(row => row.source.startsWith('Confirmed-basis')))

              allFibMapRows
                .filter(row => !selectedFibRows.has(row.price.toFixed(2)))
                .sort((a, b) => {
                  const aTrend = trendRows.includes(a)
                  const bTrend = trendRows.includes(b)
                  const aScore = Math.abs(a.price - price) / price * (aTrend ? 0.8 : 1)
                  const bScore = Math.abs(b.price - price) / price * (bTrend ? 0.8 : 1)
                  return aScore - bScore
                })
                .forEach(row => {
                  if (selectedFibRows.size < 7) selectFibRow(row)
                })

              const fibMapRows = [...selectedFibRows.values()].sort((a, b) => b.price - a.price)
              const currentRowIndex = fibMapRows.findIndex(row => row.price < price)
              const fibMatchTolerance = Math.max(price * 0.003, tradeSetup.technicals.atr * 0.25)
              const matchingFibTimeframes = (levelPrice: number) =>
                (['1wk', '1d', '1h'] as const).filter(timeframe => {
                  if (timeframe === setupInterval) return false
                  const otherFib = setups[timeframe]?.strategy_results.fibonacci
                  if (!otherFib) return false
                  const levels = [
                    ...(otherFib.active_leg?.levels ?? []),
                    ...otherFib.target_levels,
                  ]
                  return levels.some(level => Math.abs(level.price - levelPrice) <= fibMatchTolerance)
                })

              return (
                <>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.85rem', marginBottom: '0.85rem' }}>
                    <div style={PANEL}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                        <span style={LABEL}>Confirmed structural leg</span>
                        <Pill text="Fibonacci basis" tone={INFO} />
                      </div>
                      <div style={{ fontSize: '0.82rem', fontWeight: 600, color: INK }}>{confirmedMove}</div>
                      <div style={{ fontSize: '0.74rem', color: MUTED, marginTop: '0.25rem' }}>
                        Swing {plainPct(fib.swing_size_pct, 2)} · {fib.scope_bars} bars searched · {plainPct(fib.swing_detection_pct, 2)} pivot threshold
                      </div>
                    </div>

                    <div style={PANEL}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                        <span style={LABEL}>Provisional move</span>
                        <Pill text="In progress" tone={WARN} />
                      </div>
                      {active ? (
                        <>
                        <div style={{ fontSize: '0.82rem', fontWeight: 600, color: INK }}>
                          {active.start.type === 'high' ? 'High' : 'Low'} {money(active.start.price)} ({active.start.date})
                          {' → '}{active.end.type === 'high' ? 'High' : 'Low'} {money(active.end.price)} ({active.end.date})
                        </div>
                        <div style={{ display: 'flex', gap: '1.25rem', flexWrap: 'wrap', marginTop: '0.45rem', fontSize: '0.76rem' }}>
                          <span><span style={{ color: MUTED }}>Reached </span><strong>{plainPct(fib.progress_reached_pct, 2)}</strong></span>
                          <span><span style={{ color: MUTED }}>Current </span><strong>{plainPct(fib.progress_current_pct, 2)}</strong></span>
                        </div>
                        </>
                      ) : <span style={{ fontSize: '0.76rem', color: MUTED }}>No developing move.</span>}
                    </div>
                  </div>

                  {fibMapRows.length > 0 && (
                    <div style={{ border: `1px solid ${LINE}`, borderRadius: '0.5rem', maxWidth: '100%', overflowX: 'auto' }}>
                      <div style={{ padding: '0.55rem 0.75rem', background: SURFACE, borderBottom: `1px solid ${LINE}` }}>
                        <strong style={{ fontSize: '0.8rem', color: INK }}>Fibonacci price map</strong>
                        <span style={{ marginLeft: '0.5rem', fontSize: '0.7rem', color: MUTED }}>
                          Up to seven nearby levels, weighted toward the provisional trend
                        </span>
                      </div>
                      <table style={{ width: '100%', minWidth: 760, borderCollapse: 'collapse', fontSize: '0.76rem' }}>
                        <thead>
                          <tr style={{ background: '#fff', borderBottom: `1px solid ${LINE}` }}>
                            {['Price', 'Distance', 'Reference', 'Source', 'Meaning'].map((label, index) => (
                              <th key={label} style={{ padding: '6px 8px', textAlign: index <= 1 ? 'right' : 'left', color: MUTED, fontWeight: 700 }}>{label}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {fibMapRows.map((row, index) => (
                            <Fragment key={`${row.label}-${row.price}`}>
                              {index === currentRowIndex && (
                                <tr style={{ background: INFO_SOFT }}>
                                  <td colSpan={5} style={{ padding: '5px 8px', textAlign: 'center', fontWeight: 700, color: INFO }}>
                                    ── Current {money(price)} · {plainPct(activeProgress, 2)} provisional retracement ──
                                  </td>
                                </tr>
                              )}
                              <tr style={{ borderBottom: '1px solid #f1f5f9' }}>
                                <td style={{ padding: '6px 8px', textAlign: 'right', fontWeight: 700 }}>{money(row.price)}</td>
                                <td style={{ padding: '6px 8px', textAlign: 'right', color: row.price >= price ? POS : NEG }}>
                                  {signedPct(((row.price - price) / price) * 100, 2)}
                                </td>
                                <td style={{ padding: '6px 8px', fontWeight: 600, color: row.tone }}>
                                  {row.label}
                                  {matchingFibTimeframes(row.price).map(timeframe => (
                                    <span key={timeframe} style={{ marginLeft: '0.3rem', color: INFO, fontSize: '0.66rem' }}>{timeframe}</span>
                                  ))}
                                </td>
                                <td style={{ padding: '6px 8px', color: MUTED }}>{row.source}</td>
                                <td style={{ padding: '6px 8px' }}>{row.meaning}</td>
                              </tr>
                            </Fragment>
                          ))}
                          {currentRowIndex === -1 && (
                            <tr style={{ background: INFO_SOFT }}>
                              <td colSpan={5} style={{ padding: '5px 8px', textAlign: 'center', fontWeight: 700, color: INFO }}>
                                ── Current {money(price)} · {plainPct(activeProgress, 2)} provisional retracement ──
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  )}
                </>
              )
            })()}

            {tab === 'scanner' && (() => {
              const resolved = scannerEvents.filter(e => e.outcomes.length > 0)
              const outcomeOf = (event: ScannerEventRow) => event.outcomes[event.outcomes.length - 1] ?? null
              const hits = resolved.filter(e => outcomeOf(e)?.first_hit === 'TARGET').length
              const alphas = resolved
                .map(e => outcomeOf(e)?.net_alpha_return)
                .filter((a): a is number => a !== null && a !== undefined)
              const avgAlpha = alphas.length > 0 ? alphas.reduce((s, a) => s + a, 0) / alphas.length : null

              const statusOf = (event: ScannerEventRow): { label: string; tone: string } => {
                const outcome = outcomeOf(event)
                if (!outcome) {
                  return event.outcomes.some(o => o.entry_price != null)
                    ? { label: 'Open', tone: INFO }
                    : { label: 'Pending entry', tone: MUTED }
                }
                switch (outcome.first_hit) {
                  case 'TARGET': return { label: 'Target hit', tone: POS }
                  case 'STOP': return { label: 'Stopped', tone: NEG }
                  case 'SAME_BAR': return { label: 'Both in one bar', tone: WARN }
                  default: return { label: 'Expired flat', tone: MUTED }
                }
              }

              return (
                <>
                  <div style={{
                    display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap',
                    padding: '0.6rem 0.85rem', marginBottom: '0.75rem',
                    background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '0.5rem',
                  }}>
                    <div style={{ display: 'flex', gap: '1.5rem', flexWrap: 'wrap' }}>
                      <span style={{ fontSize: '0.78rem' }}>
                        <strong>{scannerEvents.length}</strong>{' '}
                        <span style={{ color: MUTED }}>
                          setups · daily/weekly last {tickerScannerEvents.daily_sessions} sessions · hourly last {tickerScannerEvents.hourly_sessions} sessions
                        </span>
                      </span>
                      <span style={{ fontSize: '0.78rem' }}>
                        <strong style={{ color: resolved.length === 0 ? MUTED : hits / resolved.length >= 0.5 ? POS : NEG }}>
                          {resolved.length === 0 ? '—' : plainPct((hits / resolved.length) * 100, 0)}
                        </strong>{' '}
                        <span style={{ color: MUTED }}>reached target ({hits}/{resolved.length} resolved)</span>
                      </span>
                      <span style={{ fontSize: '0.78rem' }}>
                        <strong style={{ color: avgAlpha === null ? MUTED : avgAlpha >= 0 ? POS : NEG }}>
                          {avgAlpha === null ? '—' : signedPct(avgAlpha * 100, 2)}
                        </strong>{' '}
                        <span style={{ color: MUTED }}>average alpha vs benchmark</span>
                      </span>
                    </div>
                    <span style={{ fontSize: '0.7rem', color: WARN }}>Research signals · not recommendations</span>
                  </div>

                  {scannerEvents.length === 0 ? (
                    <p style={{ fontSize: '0.8rem', color: MUTED }}>
                      No scanner setups recorded for {symbol} in the last {tickerScannerEvents.daily_sessions} daily/weekly sessions or {tickerScannerEvents.hourly_sessions} hourly sessions.
                    </p>
                  ) : (
                    <div style={{ border: '1px solid #e2e8f0', borderRadius: '0.5rem', maxWidth: '100%', overflowX: 'auto' }}>
                      <table style={{ width: '100%', minWidth: 920, borderCollapse: 'collapse', fontSize: '0.76rem' }}>
                        <thead>
                          <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                            {['TF', 'Setup', 'Signal time', 'Entry', 'Stop / target', 'Actual entry', 'Return / alpha', 'Status'].map((label, i) => (
                              <th key={label} style={{ padding: '6px 8px', textAlign: i === 0 || i === 6 ? 'left' : 'right', color: MUTED, fontWeight: 700, whiteSpace: 'nowrap' }}>{label}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {scannerEvents.map(event => {
                            const nextOpen = event.outcomes.find(o => o.entry_price != null)
                            const outcome = outcomeOf(event)
                            const status = statusOf(event)
                            const open = expandedEvents.has(event.event_id)
                            return (
                              <Fragment key={event.event_id}>
                                <tr
                                  onClick={() => setExpandedEvents(prev => {
                                    const next = new Set(prev)
                                    if (next.has(event.event_id)) next.delete(event.event_id)
                                    else next.add(event.event_id)
                                    return next
                                  })}
                                  style={{ borderBottom: '1px solid #f1f5f9', cursor: 'pointer', background: event.interval === setupInterval ? INFO_SOFT : undefined }}
                                >
                                  <td style={{ padding: '7px 8px', fontWeight: 700, color: event.interval === setupInterval ? INFO : MUTED }}>{event.interval}</td>
                                  <td style={{ padding: '7px 8px' }}>
                                    <div style={{ fontWeight: 600, color: event.direction === 1 ? POS : NEG }}>
                                      {open ? '▾' : '▸'} {event.direction === 1 ? 'Long' : 'Short'} · {event.trigger_type.replace(/_/g, ' ')}
                                    </div>
                                    <div style={{ color: MUTED, fontSize: '0.68rem' }}>
                                      {event.scanner_name.replace(/_/g, ' ').replace(/^sma200/i, 'SMA200')}
                                    </div>
                                  </td>
                                  <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                                    {formatScannerEventTime(event.signal_time, event.interval)}
                                  </td>
                                  <td style={{ padding: '7px 8px', textAlign: 'right' }}><strong>{money(event.entry_price)}</strong></td>
                                  <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                                    <span style={{ color: NEG }}>{money(event.stop_price)}</span>
                                    <span style={{ color: MUTED }}> / </span>
                                    <span style={{ color: POS, fontWeight: 600 }}>{money(event.target_price)}</span>
                                  </td>
                                  <td style={{ padding: '7px 8px', textAlign: 'right' }}>
                                    {nextOpen?.entry_price != null ? money(nextOpen.entry_price) : <span style={{ color: MUTED }}>Pending</span>}
                                  </td>
                                  <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                                    {!outcome ? <span style={{ color: MUTED }}>—</span> : (
                                      <>
                                        <span style={{ color: (outcome.net_signed_return ?? 0) >= 0 ? POS : NEG, fontWeight: 600 }}>
                                          {outcome.net_signed_return != null ? signedPct(outcome.net_signed_return * 100) : '—'}
                                        </span>
                                        {outcome.net_alpha_return != null && (
                                          <span style={{ color: MUTED }}> · α {signedPct(outcome.net_alpha_return * 100)}</span>
                                        )}
                                      </>
                                    )}
                                  </td>
                                  <td style={{ padding: '7px 8px' }}><Pill text={status.label} tone={status.tone} /></td>
                                </tr>
                                {open && (
                                  <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                                    <td colSpan={8} style={{ padding: '8px 12px' }}>
                                      <div style={{ display: 'flex', gap: '1.5rem', flexWrap: 'wrap', fontSize: '0.72rem', color: MUTED, marginBottom: '0.4rem' }}>
                                        <span>Signal bar open {money(event.signal_open_price)}</span>
                                        <span>Last seen {formatScannerEventTime(event.last_seen_at, event.interval)}</span>
                                        <span>{event.occurrence_count} observation{event.occurrence_count === 1 ? '' : 's'}</span>
                                        {nextOpen?.entry_time && <span>Evaluation entry {formatScannerEventTime(nextOpen.entry_time, event.interval)}</span>}
                                      </div>
                                      {event.outcomes.length === 0 ? (
                                        <div style={{ fontSize: '0.72rem', color: MUTED }}>Waiting for future bars to resolve this setup.</div>
                                      ) : (
                                        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.72rem' }}>
                                          <thead>
                                            <tr style={{ color: MUTED }}>
                                              {['Horizon', 'Exit', 'Return', 'Alpha', 'MAE / MFE', 'First hit'].map(h => (
                                                <th key={h} style={{ padding: '3px 6px', textAlign: 'left', fontWeight: 600 }}>{h}</th>
                                              ))}
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {event.outcomes.map(o => (
                                              <tr key={o.horizon_bars}>
                                                <td style={{ padding: '3px 6px' }}>{o.horizon_bars} {event.interval === '1wk' ? 'sessions' : 'bars'}</td>
                                                <td style={{ padding: '3px 6px' }} title={formatScannerEventTime(o.exit_time, event.interval)}>{money(o.exit_price)}</td>
                                                <td style={{ padding: '3px 6px', color: (o.net_signed_return ?? 0) >= 0 ? POS : NEG }}>
                                                  {o.net_signed_return != null ? signedPct(o.net_signed_return * 100) : '—'}
                                                </td>
                                                <td style={{ padding: '3px 6px' }}>{o.net_alpha_return != null ? signedPct(o.net_alpha_return * 100) : '—'}</td>
                                                <td style={{ padding: '3px 6px' }}>
                                                  {o.mae_pct != null ? plainPct(o.mae_pct * 100) : '—'} / {o.mfe_pct != null ? plainPct(o.mfe_pct * 100) : '—'}
                                                </td>
                                                <td style={{ padding: '3px 6px' }}>{o.first_hit.replace('_', ' ').toLowerCase()}</td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      )}
                                    </td>
                                  </tr>
                                )}
                              </Fragment>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </>
              )
            })()}
          </>
        )}
      </div>
    </div>
  )
}

export default TickerDetail
