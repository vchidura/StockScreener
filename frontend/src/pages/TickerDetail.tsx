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
  getTradeSetup, 
  ChartDataPoint, 
  LatestQuote, 
  TradeSetup, 
  LevelRetest,
  getTickerDiscoveryState,
  TickerDiscoveryResponse,
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

function sideOfBias(bias: string): 'LONG' | 'SHORT' | null {
  if (bias === 'Bullish') return 'LONG'
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

/** Horizontal 0–100 meter with optional zone bands, for RSI/Stochastic/consistency. */
function Meter({ value, tone, bands }: { value: number; tone: string; bands?: [number, number] }) {
  return (
    <div style={{ position: 'relative', height: 6, background: '#e5e7eb', borderRadius: 9999, overflow: 'hidden' }}>
      {bands && (
        <>
          <div style={{ position: 'absolute', left: 0, width: `${bands[0]}%`, top: 0, bottom: 0, background: POS_SOFT }} />
          <div style={{ position: 'absolute', left: `${bands[1]}%`, right: 0, top: 0, bottom: 0, background: NEG_SOFT }} />
        </>
      )}
      <div style={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: `${Math.max(0, Math.min(100, value))}%`, background: tone, opacity: 0.85 }} />
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

  // Backend emits stops below price and targets above, so a short inverts the two lists.
  const below = [...(setup.stops ?? [])].filter(s => s.price < entry).sort((a, b) => b.price - a.price)
  const above = [...(setup.targets ?? [])].filter(t => t.price > entry).sort((a, b) => a.price - b.price)

  const stopPick = side === 'LONG' ? below[0] : above[0]
  const targetPick = side === 'LONG' ? above[0] : below[0]
  if (!stopPick || !targetPick) return null

  const risk = Math.abs(entry - stopPick.price)
  const reward = Math.abs(targetPick.price - entry)
  if (!Number.isFinite(risk) || risk <= 0) return null

  const atr = setup.technicals.atr

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

interface PlanStatus {
  label: string
  tone: string
  note: string
}

function planStatusOf(plan: TradePlan, livePrice: number | null): PlanStatus {
  if (plan.rr < 1) {
    return { label: 'Skip', tone: NEG, note: `Reward is only ${plan.rr.toFixed(2)}R — the first target sits closer than the stop.` }
  }
  if (plan.stopAtr !== null && plan.stopAtr < MIN_STOP_ATR) {
    return { label: 'Stop too tight', tone: NEG, note: `Stop is ${plan.stopAtr.toFixed(2)}× ATR from entry — inside normal bar noise, expect a random stop-out.` }
  }
  // A plan whose entry has already run away is a chase, not a setup.
  if (livePrice !== null) {
    const drift = plan.side === 'LONG' ? livePrice - plan.entry : plan.entry - livePrice
    if (drift > plan.risk * 0.5) {
      return { label: 'Price past entry', tone: WARN, note: `Price has already moved ${money(Math.abs(drift))} beyond the entry — remaining reward:risk is worse than shown.` }
    }
  }
  if (plan.rr < MIN_EXECUTABLE_RR) {
    return { label: 'Thin', tone: WARN, note: `${plan.rr.toFixed(2)}R is below the ${MIN_EXECUTABLE_RR}R floor — needs a tighter stop or a further target.` }
  }
  return { label: 'Actionable', tone: POS, note: `${plan.rr.toFixed(2)}R with the stop ${plan.stopAtr !== null ? `${plan.stopAtr.toFixed(1)}× ATR` : 'clear'} from entry.` }
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
  const legendRef = useRef<HTMLDivElement>(null)

  const queryClient = useQueryClient()
  const [period, setPeriod] = useState('1y')
  const [interval, setInterval] = useState('1d')
  const [setupInterval, setSetupInterval] = useState('1d')
  const [chartHeight, setChartHeight] = useState(450)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [tab, setTab] = useState<'setup' | 'levels' | 'fibonacci' | 'scanner'>('setup')
  const [expandedEvents, setExpandedEvents] = useState<Set<number>>(new Set())

  // Track previous interval to avoid showing stale data across interval switches
  const prevIntervalRef = useRef(interval)
  const prevSetupIntervalRef = useRef(setupInterval)
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

  const { data: tradeSetup = null, isFetching: setupLoading } = useQuery<TradeSetup | null>({
    queryKey: ['trade-setup', symbol, setupInterval],
    queryFn: () => getTradeSetup(symbol!, setupInterval),
    enabled: !!symbol,
    placeholderData: (prev) => prevSetupIntervalRef.current === setupInterval ? prev : undefined,
  })

  // All 5 calibration layers arrive in a single request.
  const { data: discoveryResp = null } = useQuery<TickerDiscoveryResponse | null>({
    queryKey: ['market-discovery', symbol],
    queryFn: () => getTickerDiscoveryState(symbol!),
    enabled: !!symbol,
  })
  const discoveryState = discoveryResp?.state ?? null
  const { data: tickerScannerEvents = { ticker: symbol ?? '', events: [] } } = useQuery<{ ticker: string; events: ScannerEventRow[] }>({
    queryKey: ['scanner-events', symbol],
    queryFn: () => getTickerScannerEvents(symbol!, 120),
    enabled: !!symbol,
  })

  const techSide = tradeSetup ? sideOfBias(tradeSetup.direction.bias) : null
  const reversalTrigger = discoveryState?.reversal_trigger ?? 'NONE'
  const extensionRisk = discoveryState?.extension_risk ?? 'NORMAL'
  const positionBlocksPlan = techSide === 'LONG'
    ? reversalTrigger.startsWith('BEARISH')
      || (discoveryState?.trend_state === 'UPTREND' && extensionRisk !== 'NORMAL')
    : techSide === 'SHORT'
      ? reversalTrigger.startsWith('BULLISH')
        || (discoveryState?.trend_state === 'DOWNTREND' && extensionRisk !== 'NORMAL')
      : false
  const plan = tradeSetup && !positionBlocksPlan ? buildTradePlan(tradeSetup) : null

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
    prevSetupIntervalRef.current = setupInterval
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
    const setupKey = ['trade-setup', symbol, setupInterval]
    const quoteKey = ['latest-quote', symbol]
    queryClient.setQueryData(chartKey, undefined)
    queryClient.setQueryData(setupKey, undefined)
    queryClient.setQueryData(quoteKey, undefined)
    await Promise.all([
      queryClient.fetchQuery({ queryKey: chartKey, queryFn: () => getChartData(symbol!, chartRequestPeriod, interval, true) }),
      queryClient.fetchQuery({ queryKey: setupKey, queryFn: () => getTradeSetup(symbol!, setupInterval, true) }),
      queryClient.fetchQuery({ queryKey: quoteKey, queryFn: () => getLatestQuote(symbol!, true) }),
    ])
  }, [symbol, chartRequestPeriod, interval, setupInterval, queryClient])

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

  const lastPrice = chartData.length > 0 ? chartData[chartData.length - 1] : null
  const prevPrice = chartData.length > 1 ? chartData[chartData.length - 2] : null
  const displayPrice = latestQuote?.price ?? lastPrice?.close ?? null
  const chartPriceChange = lastPrice && prevPrice ? lastPrice.close - prevPrice.close : 0
  const chartPriceChangePercent = prevPrice ? (chartPriceChange / prevPrice.close) * 100 : 0
  const priceChange = latestQuote?.change ?? chartPriceChange
  const priceChangePercent = latestQuote?.change_percent ?? chartPriceChangePercent

  const planStatus = plan ? planStatusOf(plan, displayPrice) : null
  const scannerEvents = tickerScannerEvents.events.filter(e => e.interval === setupInterval)
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
            const iv = e.target.value
            setInterval(iv)
            if (iv in SESSION_BARS) setPeriod('1d')
            else if (iv === '1d') setPeriod('1y')
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
              onChange={(e) => {
                const iv = e.target.value
                setInterval(iv)
                if (iv in SESSION_BARS) setPeriod('1d')
                else if (iv === '1d') setPeriod('1y')
              }}
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
              </div>
            )}
          </div>
          <div style={{ display: 'flex', gap: '0.25rem', background: '#f1f5f9', borderRadius: '0.375rem', padding: '0.15rem' }}>
            {SETUP_INTERVALS.map(iv => (
              <button
                key={iv}
                onClick={() => setSetupInterval(iv)}
                style={{
                  padding: '0.2rem 0.6rem',
                  fontSize: '0.75rem',
                  fontWeight: setupInterval === iv ? 700 : 400,
                  border: 'none',
                  borderRadius: '0.25rem',
                  background: setupInterval === iv ? INFO : 'transparent',
                  color: setupInterval === iv ? '#fff' : MUTED,
                  cursor: 'pointer',
                }}
              >
                {iv}
              </button>
            ))}
          </div>
        </div>

        {setupLoading && (
          <div className="loading" style={{ padding: '2rem' }}>
            <div className="spinner"></div>
            <span>Analyzing strategies...</span>
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
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '0.6rem', marginBottom: '0.85rem' }}>
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
                sub={`${tradeSetup.confluence.count} confluence signals`}
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
                note={`Stoch %K ${tradeSetup.technicals.stoch_k.toFixed(0)} · ATR ${plainPct(tradeSetup.technicals.atr_pct)}`}
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
            {plan && planStatus ? (() => {
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
                      <Pill text={planStatus.label} tone={planStatus.tone} solid />
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
                  </div>

                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', borderTop: '1px solid #f1f5f9' }}>
                    {[
                      {
                        k: 'Reward : Risk',
                        v: `${plan.rr.toFixed(2)}R`,
                        tone: plan.rr >= MIN_EXECUTABLE_RR ? POS : plan.rr >= 1 ? WARN : NEG,
                        sub: plan.rr >= MIN_EXECUTABLE_RR ? `Meets the ${MIN_EXECUTABLE_RR}R floor` : `Below the ${MIN_EXECUTABLE_RR}R floor`,
                      },
                      {
                        k: 'Stop vs noise',
                        v: plan.stopAtr === null ? 'n/a' : `${plan.stopAtr.toFixed(2)}× ATR`,
                        tone: plan.stopAtr === null ? MUTED : plan.stopAtr >= MIN_STOP_ATR ? POS : NEG,
                        sub: plan.stopAtr === null ? 'ATR unavailable'
                          : plan.stopAtr >= MIN_STOP_ATR ? 'Clear of normal bar range' : 'Inside normal bar range',
                      },
                      {
                        k: 'Risk / share',
                        v: money(plan.risk),
                        tone: NEG,
                        sub: `${plainPct(plan.riskPct)} of entry`,
                      },
                      {
                        k: 'Reward / share',
                        v: money(plan.reward),
                        tone: POS,
                        sub: `${plainPct(plan.rewardPct)} of entry`,
                      },
                    ].map(x => (
                      <div key={x.k} style={{ padding: '0.45rem 0.85rem', borderRight: '1px solid #f1f5f9' }}>
                        <div style={{ ...LABEL, fontSize: '0.6rem' }}>{x.k}</div>
                        <div style={{ fontSize: '0.85rem', fontWeight: 700, color: x.tone }}>{x.v}</div>
                        <div style={{ fontSize: '0.65rem', color: MUTED }}>{x.sub}</div>
                      </div>
                    ))}
                  </div>

                  <div style={{ padding: '0.4rem 0.85rem', fontSize: '0.68rem', color: planStatus.tone, borderTop: '1px solid #f1f5f9', background: SURFACE }}>
                    {planStatus.note}
                  </div>
                </div>
              )
            })() : (
              <div style={{ padding: '0.85rem', marginBottom: '1rem', border: '1px dashed #cbd5e1', borderRadius: '0.5rem', fontSize: '0.82rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                  <Pill text={positionBlocksPlan ? 'Blocked' : 'No plan'} tone={positionBlocksPlan ? NEG : MUTED} solid />
                  <strong style={{ fontSize: '0.85rem', color: INK }}>
                    {positionBlocksPlan ? `${techSide} setup is blocked by the daily market state` : 'No executable plan on this timeframe'}
                  </strong>
                </div>
                <div style={{ color: MUTED }}>
                  {positionBlocksPlan && discoveryState?.position_guidance
                    ? discoveryState.position_guidance
                    : positionBlocksPlan
                      ? `Discovery state ${(discoveryState?.state ?? 'unknown').replace(/_/g, ' ')} contradicts the ${techSide} bias.`
                      : `Needs a directional bias plus a stop below and a first target above ${money(tradeSetup.last_close)}.`}
                </div>
                {positionBlocksPlan && (
                  <div style={{ fontSize: '0.7rem', color: MUTED, marginTop: '0.35rem' }}>
                    Discovery state is computed daily and applies to every timeframe, not just {INTERVAL_NOUN[setupInterval] ?? setupInterval} bars.
                  </div>
                )}
              </div>
            )}

            {/* Evidence tabs */}
            <div style={{ display: 'flex', gap: '0.25rem', borderBottom: '1px solid #e2e8f0', marginBottom: '0.85rem' }}>
              {([
                ['setup', 'Setup', null],
                ['levels', 'Levels', new Set([
                  ...tradeSetup.targets.map(t => t.price.toFixed(2)),
                  ...tradeSetup.stops.map(s => s.price.toFixed(2)),
                  ...tradeSetup.entries.filter(e => e.zone_low !== null).map(e => e.zone_low!.toFixed(2)),
                  ...tradeSetup.zones.map(z => z.low.toFixed(2)),
                ]).size],
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

            {tab === 'setup' && (() => {
              const t = tradeSetup.technicals
              const emaChips = [
                { label: '8 EMA', value: t.ema8 },
                { label: '21 EMA', value: t.ema21 },
                { label: '50 EMA', value: t.ema50 },
              ]

              return (
                <>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.85rem', marginBottom: '0.85rem' }}>
                    <div style={PANEL}>
                      <div style={LABEL}>Trend</div>
                      <div style={{ fontSize: '1.05rem', fontWeight: 700, color: trendTone(tradeSetup.ema_alignment.primary), margin: '0.15rem 0 0.2rem' }}>
                        {tradeSetup.ema_alignment.primary}
                      </div>
                      <div style={{ fontSize: '0.75rem', color: MUTED, marginBottom: '0.6rem' }}>
                        {tradeSetup.ema_alignment.primary_detail}
                      </div>

                      <div style={{ display: 'flex', gap: '0.35rem', flexWrap: 'wrap', marginBottom: '0.6rem' }}>
                        {emaChips.map(chip => {
                          const above = chip.value !== null && tradeSetup.last_close >= chip.value
                          return (
                            <Pill
                              key={chip.label}
                              text={`${chip.label} ${above ? '▲ price above' : '▼ price below'}`}
                              tone={above ? POS : NEG}
                              title={`${chip.label} at ${money(chip.value)}`}
                            />
                          )
                        })}
                      </div>

                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.76rem', padding: '0.3rem 0', borderTop: '1px solid #f1f5f9' }}>
                        <span style={{ color: MUTED }}>50 / 200 SMA</span>
                        <span style={{ fontWeight: 600, color: tradeSetup.golden_cross ? trendTone(tradeSetup.golden_cross.type) : MUTED }}
                          title={tradeSetup.golden_cross?.detail}>
                          {tradeSetup.golden_cross
                            ? `${tradeSetup.golden_cross.type}${tradeSetup.golden_cross.bars_ago !== null ? ` · ${tradeSetup.golden_cross.bars_ago} bars ago` : ''}`
                            : 'Not enough history'}
                        </span>
                      </div>

                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.76rem', padding: '0.3rem 0', borderTop: '1px solid #f1f5f9' }}>
                        <span style={{ color: MUTED }}>
                          {tradeSetup.ema_alignment.confirm_interval} confirmation
                        </span>
                        <span style={{ fontWeight: 600, color: tradeSetup.ema_alignment.multi_tf_agree === null ? MUTED : tradeSetup.ema_alignment.multi_tf_agree ? POS : WARN }}>
                          {tradeSetup.ema_alignment.confirm === null
                            ? 'No data'
                            : `${tradeSetup.ema_alignment.confirm} · ${tradeSetup.ema_alignment.multi_tf_agree ? 'agrees' : 'diverges'}`}
                        </span>
                      </div>

                      <div style={{ paddingTop: '0.5rem', borderTop: '1px solid #f1f5f9', marginTop: '0.3rem' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.76rem', marginBottom: '0.25rem' }}>
                          <span style={{ color: MUTED }}>Directional consistency</span>
                          <span style={{ fontWeight: 600 }}>{plainPct(t.trend_consistency, 0)} of last 14 bars</span>
                        </div>
                        <Meter value={t.trend_consistency} tone={trendTone(tradeSetup.ema_alignment.primary)} />
                      </div>
                    </div>

                    <div style={PANEL}>
                      <div style={LABEL}>Momentum</div>
                      <div style={{ fontSize: '1.05rem', fontWeight: 700, color: trendTone(tradeSetup.momentum.state), margin: '0.15rem 0 0.2rem' }}>
                        {tradeSetup.momentum.state}
                      </div>
                      <div style={{ fontSize: '0.75rem', color: MUTED, marginBottom: '0.6rem' }}>
                        {tradeSetup.momentum.detail}
                      </div>

                      <div style={{ marginBottom: '0.55rem' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.76rem', marginBottom: '0.25rem' }}>
                          <span style={{ color: MUTED }}>RSI (14)</span>
                          <span style={{ fontWeight: 600, color: t.rsi > 70 ? NEG : t.rsi < 30 ? POS : INK }}>
                            {t.rsi.toFixed(1)} · {t.rsi_state}
                          </span>
                        </div>
                        <Meter value={t.rsi} tone={t.rsi > 70 ? NEG : t.rsi < 30 ? POS : INFO} bands={[30, 70]} />
                      </div>

                      <div style={{ marginBottom: '0.55rem' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.76rem', marginBottom: '0.25rem' }}>
                          <span style={{ color: MUTED }}>Stochastic %K</span>
                          <span style={{ fontWeight: 600 }}>{t.stoch_k.toFixed(1)}</span>
                        </div>
                        <Meter value={t.stoch_k} tone={t.stoch_k > 80 ? NEG : t.stoch_k < 20 ? POS : INFO} bands={[20, 80]} />
                      </div>

                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.76rem', padding: '0.3rem 0', borderTop: '1px solid #f1f5f9' }}>
                        <span style={{ color: MUTED }}>Extension from 8 / 21 EMA</span>
                        <span style={{ fontWeight: 600, color: Math.abs(t.dist_to_8ema) > 5 ? WARN : INK }}>
                          {signedPct(t.dist_to_8ema)} / {signedPct(t.dist_to_21ema)}
                        </span>
                      </div>

                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.76rem', padding: '0.3rem 0', borderTop: '1px solid #f1f5f9' }}>
                        <span style={{ color: MUTED }}>VWAP (20 bars)</span>
                        <span style={{ fontWeight: 600, color: t.price_vs_vwap === 'Above' ? POS : NEG }}>
                          {money(t.vwap)} · price {t.price_vs_vwap.toLowerCase()}
                        </span>
                      </div>

                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.76rem', padding: '0.3rem 0', borderTop: '1px solid #f1f5f9' }}>
                        <span style={{ color: MUTED }}>Average true range</span>
                        <span style={{ fontWeight: 600 }}>{money(t.atr)} · {plainPct(t.atr_pct)} of price</span>
                      </div>
                    </div>
                  </div>
                </>
              )
            })()}

            {tab === 'levels' && (() => {
              const atr = tradeSetup.technicals.atr
              const price = tradeSetup.last_close
              const retests: Array<LevelRetest & { tf: string }> = [
                ...tradeSetup.level_retests.primary.map(r => ({ ...r, tf: setupInterval })),
                ...tradeSetup.level_retests.confirm.map(r => ({ ...r, tf: tradeSetup.level_retests.confirm_interval })),
              ]

              interface Row {
                price: number
                zoneHigh: number | null
                name: string
                source: string
                role: 'Target' | 'Stop' | 'Entry' | 'Zone'
                status: string
                tone: string
                detail: string
                inPlan: boolean
              }

              const statusOfLevel = (levelPrice: number, matchName: string) => {
                const rt = retests.find(r => r.level_name === matchName || Math.abs(r.level_price - levelPrice) < 0.01)
                const distAtr = atr > 0 ? Math.abs(levelPrice - price) / atr : null
                if (rt) {
                  return {
                    status: rt.held ? 'Held' : 'Broken',
                    tone: rt.held ? POS : NEG,
                    detail: `${rt.touch_type} ${rt.bars_ago} bars ago · ${signedPct(rt.bounce_pct)} · ${rt.tf}`,
                  }
                }
                const detail = distAtr !== null ? `${distAtr.toFixed(1)}× ATR away` : ''
                if (distAtr !== null && distAtr <= 0.25) return { status: 'Active', tone: WARN, detail: 'Price is sitting on this level' }
                if (distAtr !== null && distAtr <= 1) return { status: 'Approaching', tone: INFO, detail }
                if (distAtr !== null && distAtr <= 2) return { status: 'Near', tone: MUTED, detail }
                return { status: 'Far', tone: MUTED, detail }
              }

              const inPlanAt = (p: number) =>
                plan !== null && (Math.abs(plan.stop - p) < 0.01 || Math.abs(plan.target - p) < 0.01)

              const seen = new Set<string>()
              const rows: Row[] = []
              const collect = (levels: typeof tradeSetup.targets, role: 'Target' | 'Stop') => {
                levels.forEach(lvl => {
                  const key = lvl.price.toFixed(2)
                  if (seen.has(key)) return
                  seen.add(key)
                  rows.push({
                    price: lvl.price,
                    zoneHigh: null,
                    name: lvl.level,
                    source: lvl.source,
                    role,
                    ...statusOfLevel(lvl.price, lvl.level),
                    inPlan: inPlanAt(lvl.price),
                  })
                })
              }
              collect(tradeSetup.targets, 'Target')
              collect(tradeSetup.stops, 'Stop')

              // Entry triggers carry zones (FVG, gaps) and EMAs that the target/stop lists omit.
              tradeSetup.entries.forEach(e => {
                if (e.zone_low === null) return
                const key = e.zone_low.toFixed(2)
                if (seen.has(key)) return
                seen.add(key)
                const zoneHigh = e.zone_high !== null && Math.abs(e.zone_high - e.zone_low) > 0.005 ? e.zone_high : null
                const base = statusOfLevel(e.zone_low, e.strategy)
                rows.push({
                  price: e.zone_low,
                  zoneHigh,
                  name: e.strategy,
                  source: 'Entry trigger',
                  role: 'Entry',
                  ...base,
                  detail: `${e.strength} · ${e.condition}`,
                  inPlan: inPlanAt(e.zone_low),
                })
              })

              // Remaining unmitigated gap/FVG zones, so the table never degrades to a bare count.
              tradeSetup.zones.forEach(z => {
                const key = z.low.toFixed(2)
                if (seen.has(key)) return
                seen.add(key)
                const inside = price >= z.low && price <= z.high
                const nearestEdge = price < z.low ? z.low : z.high
                const base = inside
                  ? { status: 'Price inside', tone: WARN, detail: 'Price is trading inside this zone' }
                  : statusOfLevel(nearestEdge, z.name)
                rows.push({
                  price: z.low,
                  zoneHigh: z.high,
                  name: z.name,
                  source: z.source,
                  role: 'Zone',
                  ...base,
                  detail: z.qualifier ? `${z.qualifier} · ${base.detail}` : base.detail,
                  inPlan: inPlanAt(z.low),
                })
              })

              rows.sort((a, b) => b.price - a.price)
              const firstBelow = rows.findIndex(r => r.price < price)

              const priceRow = (
                <tr style={{ background: INFO_SOFT }}>
                  <td colSpan={6} style={{ padding: '5px 8px', fontSize: '0.74rem', fontWeight: 700, color: INFO }}>
                    ── Current price {money(price)} ──
                  </td>
                </tr>
              )

              return (
                <>
                  {rows.length === 0 ? (
                    <p style={{ fontSize: '0.8rem', color: MUTED }}>No levels resolved on this timeframe.</p>
                  ) : (
                    <div style={{ border: '1px solid #e2e8f0', borderRadius: '0.5rem', overflow: 'hidden', marginBottom: '1rem' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.78rem' }}>
                        <thead>
                          <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                            {['Price', 'Distance', 'Level', 'Source', 'Role', 'Status'].map((label, i) => (
                              <th key={label} style={{ padding: '6px 8px', textAlign: i <= 1 ? 'right' : 'left', color: MUTED, fontWeight: 700 }}>{label}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {rows.map((r, i) => (
                            <Fragment key={`${r.name}-${r.price}`}>
                              {i === firstBelow && priceRow}
                              <tr style={{ borderBottom: '1px solid #f1f5f9', background: r.inPlan ? WARN_SOFT : undefined }}>
                                <td style={{ padding: '6px 8px', textAlign: 'right', fontWeight: 700, whiteSpace: 'nowrap' }}>
                                  {r.zoneHigh !== null ? `${money(r.price)} – ${money(r.zoneHigh)}` : money(r.price)}
                                </td>
                                <td style={{ padding: '6px 8px', textAlign: 'right', color: r.price >= price ? POS : NEG }}>
                                  {signedPct(((r.price - price) / price) * 100)}
                                </td>
                                <td style={{ padding: '6px 8px' }}>
                                  {r.name}
                                  {r.inPlan && <span style={{ marginLeft: '0.35rem', fontSize: '0.68rem', color: WARN, fontWeight: 700 }}>in plan</span>}
                                </td>
                                <td style={{ padding: '6px 8px', color: MUTED }}>{r.source}</td>
                                <td style={{ padding: '6px 8px', color: r.role === 'Target' ? POS : r.role === 'Stop' ? NEG : r.role === 'Entry' ? INFO : MUTED, fontWeight: 600 }}>{r.role}</td>
                                <td style={{ padding: '6px 8px' }}>
                                  <Pill text={r.status} tone={r.tone} />
                                  {r.detail && <div style={{ fontSize: '0.68rem', color: MUTED, marginTop: '0.15rem' }}>{r.detail}</div>}
                                </td>
                              </tr>
                            </Fragment>
                          ))}
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

              const levelGrid = (levels: typeof fib.levels, nearestName: string) => (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.35rem', marginTop: '0.5rem' }}>
                  {levels.map(level => {
                    const isNearest = level.name === nearestName
                    const above = level.price >= price
                    return (
                      <div key={level.name} style={{
                        padding: '0.35rem 0.5rem',
                        border: `1px solid ${isNearest ? INFO : LINE}`,
                        background: isNearest ? INFO_SOFT : '#fff',
                        borderRadius: '0.35rem',
                        fontSize: '0.74rem',
                      }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.4rem' }}>
                          <span style={{ color: MUTED }}>{level.name}</span>
                          <strong>{money(level.price)}</strong>
                        </div>
                        <div style={{ color: above ? POS : NEG, fontSize: '0.68rem' }}>
                          {signedPct(((level.price - price) / price) * 100, 2)} · {above ? 'above' : 'below'} price
                        </div>
                      </div>
                    )
                  })}
                </div>
              )

              return (
                <>
                  <div style={{ display: 'grid', gridTemplateColumns: active ? '1fr 1fr' : '1fr', gap: '0.85rem', marginBottom: '0.85rem' }}>
                    <div style={PANEL}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                        <span style={LABEL}>Confirmed leg</span>
                        <Pill text="Valid basis" tone={POS} />
                      </div>
                      <div style={{ fontSize: '0.82rem', fontWeight: 600, color: INK }}>{confirmedMove}</div>
                      <div style={{ fontSize: '0.74rem', color: MUTED, marginTop: '0.25rem' }}>
                        {fib.signal} · swing {plainPct(fib.swing_size_pct, 2)} · retraced {plainPct(fib.retracement_pct, 2)} · detection {plainPct(fib.swing_detection_pct, 2)} dynamic
                      </div>
                      <div style={{ fontSize: '0.76rem', marginTop: '0.4rem', paddingTop: '0.4rem', borderTop: '1px solid #f1f5f9' }}>
                        <span style={{ color: MUTED }}>Nearest reference: </span>
                        <strong>{fib.nearest_level} · {money(fib.nearest_level_price)}</strong>
                        <span style={{ color: fib.distance_pct >= 0 ? POS : NEG }}> · {signedPct(fib.distance_pct, 2)}</span>
                      </div>
                      {levelGrid(fib.levels, fib.nearest_level)}
                    </div>

                    {active && (
                      <div style={PANEL}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                          <span style={LABEL}>Active provisional leg</span>
                          <Pill text="Unconfirmed" tone={WARN} />
                        </div>
                        <div style={{ fontSize: '0.82rem', fontWeight: 600, color: INK }}>
                          {active.start.type === 'high' ? 'High' : 'Low'} {money(active.start.price)} ({active.start.date})
                          {' → '}{active.end.type === 'high' ? 'High' : 'Low'} {money(active.end.price)} ({active.end.date})
                        </div>
                        <div style={{ fontSize: '0.74rem', color: MUTED, marginTop: '0.25rem' }}>
                          Acts as {active.level_role === 'provisional_support' ? 'support' : 'resistance'} · retraced {plainPct(active.retracement_pct, 2)}
                        </div>
                        <div style={{ fontSize: '0.76rem', marginTop: '0.4rem', paddingTop: '0.4rem', borderTop: '1px solid #f1f5f9' }}>
                          <span style={{ color: MUTED }}>Confirms when price moves </span>
                          <strong>
                            {active.confirmation.condition === 'at_or_below' ? 'to or below' : 'to or above'} {money(active.confirmation.price)}
                          </strong>
                        </div>
                        {levelGrid(active.levels, active.nearest_level)}
                      </div>
                    )}
                  </div>

                  {!active && (
                    <div style={{ ...PANEL, marginBottom: '0.85rem' }}>
                      <div style={{ ...LABEL, marginBottom: '0.3rem' }}>Developing pivot</div>
                      <div style={{ fontSize: '0.82rem' }}>
                        {fib.developing_pivot.type === 'high' ? 'High' : 'Low'} {money(fib.developing_pivot.price)} ({fib.developing_pivot.date})
                        {' · '}{plainPct(fib.developing_pivot.move_pct_from_confirmed, 2)} from the confirmed pivot
                      </div>
                    </div>
                  )}
            {tradeSetup.strategy_results.fibonacci?.active_leg?.scenarios && (
              <section className="ticker-fibonacci-scenarios">
                <div className="ticker-fibonacci-scenarios__header">
                  <div>
                    <strong>Fibonacci conditional paths</strong>
                    <div>Structural alternatives from the active provisional leg, not forecasts.</div>
                  </div>
                  <span>Current: unconfirmed range</span>
                </div>
                <div>
                  {tradeSetup.strategy_results.fibonacci.active_leg.scenarios.map((scenario) => {
                    const condition = scenario.condition === 'between'
                      ? `${money(scenario.lower_price ?? 0)} to ${money(scenario.upper_price ?? 0)}`
                      : scenario.condition === 'after_confirmation'
                        ? 'After pivot confirmation'
                        : `${scenario.condition === 'above' ? 'Above' : scenario.condition === 'below' ? 'Below' : scenario.condition === 'at_or_below' ? 'At/below' : 'At/above'} ${money(scenario.trigger_price ?? 0)}`
                    return (
                      <div className="ticker-fibonacci-scenario" key={scenario.id}>
                        <div>
                          <strong>{scenario.title}</strong>
                          {scenario.id === tradeSetup.strategy_results.fibonacci?.active_leg?.current_state.id && <span>Current state</span>}
                        </div>
                        <div>{condition}</div>
                        <div>{scenario.detail}</div>
                        <div>
                          {scenario.levels.length > 0
                            ? scenario.levels.map((level) => `${level.name} ${money(level.price)}`).join(' · ')
                            : 'Levels recalculate with the developing pivot'}
                        </div>
                      </div>
                    )
                  })}
                </div>
              </section>
            )}
            {!!tradeSetup.strategy_results.fibonacci?.confirmed_legs?.length && (
              <details>
                <summary style={{ cursor: 'pointer', fontSize: '0.78rem', fontWeight: 700, color: MUTED, padding: '0.4rem 0' }}>
                  Confirmed leg history ({tradeSetup.strategy_results.fibonacci.confirmed_legs.length})
                </summary>
                <section className="ticker-fibonacci-history">
                  <div className="ticker-fibonacci-history__header">
                    <strong>Confirmed leg history</strong>
                    <span>Newest six completed legs · invalidated levels are historical only</span>
                  </div>
                  <div className="ticker-fibonacci-history__columns">
                    <span>Status</span><span>Completed move</span><span>Role</span>
                    <span>Swing</span><span>Nearest level</span><span>Invalidation</span>
                  </div>
                  {tradeSetup.strategy_results.fibonacci?.confirmed_legs?.map((leg) => (
                    <div className={`ticker-fibonacci-history__row ticker-fibonacci-history__row--${leg.status}`} key={`${leg.start.date}-${leg.end.date}`}>
                      <div>
                        <span className="ticker-fibonacci-history__status">
                          {leg.is_primary ? 'Primary' : leg.status === 'valid' ? 'Valid' : 'Invalidated'}
                        </span>
                      </div>
                      <div>
                        {leg.start.type === 'high' ? 'High' : 'Low'} {money(leg.start.price)} · {leg.start.date}
                        {' '}→ {leg.end.type === 'high' ? 'High' : 'Low'} {money(leg.end.price)} · {leg.end.date}
                      </div>
                      <div>{leg.level_role === 'confirmed_support' ? 'Support' : 'Resistance'}</div>
                      <div>{leg.swing_size_pct.toFixed(2)}%</div>
                      <div>{leg.nearest_level} · {money(leg.nearest_level_price)} · {leg.distance_pct > 0 ? '+' : ''}{leg.distance_pct.toFixed(2)}%</div>
                      <div>
                        {leg.status === 'invalidated'
                          ? `${leg.invalidation.date} · moved ${leg.invalidation.condition} ${money(leg.invalidation.price)}`
                          : `Valid until ${leg.invalidation.condition} ${money(leg.invalidation.price)}`}
                      </div>
                    </div>
                  ))}
                </section>
              </details>
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

              const barNoun = setupInterval === '1wk' ? 'sessions' : 'bars'

              return (
                <>
                  <div style={{
                    display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap',
                    padding: '0.6rem 0.85rem', marginBottom: '0.75rem',
                    background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '0.5rem',
                  }}>
                    <div style={{ display: 'flex', gap: '1.5rem', flexWrap: 'wrap' }}>
                      <span style={{ fontSize: '0.78rem' }}>
                        <strong>{scannerEvents.length}</strong> <span style={{ color: MUTED }}>setups on {INTERVAL_NOUN[setupInterval] ?? setupInterval} bars</span>
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
                      No scanner setups recorded for {symbol} on {INTERVAL_NOUN[setupInterval] ?? setupInterval} bars.
                    </p>
                  ) : (
                    <div style={{ border: '1px solid #e2e8f0', borderRadius: '0.5rem', overflow: 'hidden' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.76rem' }}>
                        <thead>
                          <tr style={{ background: '#f8fafc', borderBottom: '1px solid #e2e8f0' }}>
                            {['Setup', 'Signal time', 'Entry', 'Stop / target', 'Actual entry', 'Return / alpha', 'Status'].map((label, i) => (
                              <th key={label} style={{ padding: '6px 8px', textAlign: i === 0 || i === 6 ? 'left' : 'right', color: MUTED, fontWeight: 700, whiteSpace: 'nowrap' }}>{label}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {scannerEvents.slice(0, 12).map(event => {
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
                                  style={{ borderBottom: '1px solid #f1f5f9', cursor: 'pointer' }}
                                >
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
                                    <td colSpan={7} style={{ padding: '8px 12px' }}>
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
                                                <td style={{ padding: '3px 6px' }}>{o.horizon_bars} {barNoun}</td>
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
