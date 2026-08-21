import { useState, useEffect, useRef, useCallback, CSSProperties } from 'react'
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
  getTickerCalibration,
  TickerCalibration,
  RecommendationLayer,
  getTickerSignal,
  TickerSignalResponse,
  getTickerDiscoveryState,
  TickerDiscoveryResponse,
  getTickerScannerEvents,
  ScannerEventRow,
  ScannerInterval,
  PatternKey
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

const POS = '#16a34a'
const NEG = '#dc2626'
const WARN = '#d97706'
const MUTED = '#64748b'
const INK = '#0f172a'

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
  if (grade.startsWith('B')) return '#2563eb'
  return MUTED
}

/** Below this many resolved calls a per-ticker win rate is indistinguishable from chance. */
const MIN_RELIABLE_SAMPLE = 20
const MIN_EXECUTABLE_RR = 2

function wilson95(k: number, n: number): [number, number] | null {
  if (n <= 0) return null
  const z = 1.96
  const p = k / n
  const d = 1 + (z * z) / n
  const centre = (p + (z * z) / (2 * n)) / d
  const margin = (z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n))) / d
  return [Math.max(0, (centre - margin) * 100), Math.min(100, (centre + margin) * 100)]
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
  fromModel: boolean
  winRate: number | null
  evPerShare: number | null
}

/** Derive an executable plan: model levels win, otherwise nearest technical levels. */
function buildTradePlan(
  setup: TradeSetup,
  rec: RecommendationLayer | null,
  winRate: number | null,
): TradePlan | null {
  const side = rec?.direction === 'BULL' ? 'LONG'
    : rec?.direction === 'BEAR' ? 'SHORT'
    : sideOfBias(setup.direction.bias)
  if (!side) return null

  const entry = rec?.levels.entry ?? setup.last_close
  if (!Number.isFinite(entry) || entry <= 0) return null

  // Backend emits stops below price and targets above, so a short inverts the two lists.
  const below = [...(setup.stops ?? [])].filter(s => s.price < entry).sort((a, b) => b.price - a.price)
  const above = [...(setup.targets ?? [])].filter(t => t.price > entry).sort((a, b) => a.price - b.price)

  const wellFormed = (s: number, t: number) =>
    side === 'LONG' ? s < entry && t > entry : s > entry && t < entry

  const modelStop = rec?.levels.stop ?? null
  const modelTarget = rec?.levels.target_1 ?? null
  // Only trust model levels when they bracket entry on the correct side.
  const fromModel = modelStop !== null && modelTarget !== null && wellFormed(modelStop, modelTarget)

  let stop: number
  let target: number
  let stopLabel: string
  let targetLabel: string

  if (fromModel) {
    stop = modelStop as number
    target = modelTarget as number
    stopLabel = 'Model stop'
    targetLabel = 'Model target'
  } else {
    const stopPick = side === 'LONG' ? below[0] : above[0]
    const targetPick = side === 'LONG' ? above[0] : below[0]
    if (!stopPick || !targetPick) return null
    stop = stopPick.price
    target = targetPick.price
    stopLabel = `${stopPick.level} (${stopPick.source})`
    targetLabel = `${targetPick.level} (${targetPick.source})`
  }

  const risk = Math.abs(entry - stop)
  const reward = Math.abs(target - entry)
  const rr = reward / risk
  if (!Number.isFinite(risk) || risk <= 0 || !Number.isFinite(rr) || rr < MIN_EXECUTABLE_RR) return null

  const wr = winRate !== null && winRate > 0 ? winRate / 100 : null
  const evPerShare = wr === null ? null : wr * reward - (1 - wr) * risk

  return {
    side,
    entry,
    stop,
    target,
    stopLabel,
    targetLabel,
    risk,
    reward,
    riskPct: (risk / entry) * 100,
    rewardPct: (reward / entry) * 100,
    rr,
    fromModel,
    winRate,
    evPerShare,
  }
}

function TickerDetail() {
  const { symbol } = useParams<{ symbol: string }>()
  const navigate = useNavigate()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null)
  const ma5SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const ma50SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const ma100SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const ma200SeriesRef = useRef<ISeriesApi<'Line'> | null>(null)
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
  const [evidenceTab, setEvidenceTab] = useState<'technicals' | 'levels' | 'signals' | 'model'>('technicals')

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
  const { data: calibration = null, isFetching: calibrationLoading } = useQuery<TickerCalibration | null>({
    queryKey: ['calibration', symbol],
    queryFn: () => getTickerCalibration(symbol!),
    enabled: !!symbol,
  })

  const patternScores = calibration?.pattern_scores ?? null
  const analogMatches = calibration?.analogs ?? null
  const patternPriors = calibration?.priors ?? {}
  const tickerRec = calibration?.recommendation ?? null
  const tickerPerf = calibration?.performance ?? null
  const baseline = calibration?.baseline ?? null

  // The only component with an out-of-sample validated edge.
  const { data: signalResp = null } = useQuery<TickerSignalResponse | null>({
    queryKey: ['xs-signal', symbol],
    queryFn: () => getTickerSignal(symbol!),
    enabled: !!symbol,
  })
  const xsSignal = signalResp?.signal ?? null
  const { data: discoveryResp = null } = useQuery<TickerDiscoveryResponse | null>({
    queryKey: ['market-discovery', symbol],
    queryFn: () => getTickerDiscoveryState(symbol!),
    enabled: !!symbol,
  })
  const discoveryState = discoveryResp?.state ?? null
  const { data: tickerScannerEvents = { ticker: symbol ?? '', events: [] } } = useQuery<{ ticker: string; events: ScannerEventRow[] }>({
    queryKey: ['scanner-events', symbol],
    queryFn: () => getTickerScannerEvents(symbol!, 20),
    enabled: !!symbol,
  })

  const techSide = tradeSetup ? sideOfBias(tradeSetup.direction.bias) : null
  const dailyModelSide = tickerRec?.direction === 'BULL' ? 'LONG'
    : tickerRec?.direction === 'BEAR' ? 'SHORT'
    : null
  // Calibration layers are daily-only, so they must not steer an intraday plan.
  const isDailySetup = setupInterval === '1d'
  const modelSide = isDailySetup ? dailyModelSide : null
  const planSide = modelSide ?? techSide
  // Prefer this ticker's own record, but only at a sample size that means anything.
  const sideStats = planSide === 'LONG' ? tickerPerf?.bull_stats
    : planSide === 'SHORT' ? tickerPerf?.bear_stats
    : null
  const winRateBasis: 'ticker' | 'system' | null = !isDailySetup ? null
    : sideStats && sideStats.count >= MIN_RELIABLE_SAMPLE ? 'ticker'
    : baseline && baseline.total_recs >= MIN_RELIABLE_SAMPLE ? 'system'
    : null
  const sideWinRate = winRateBasis === 'ticker' ? sideStats!.win_rate
    : winRateBasis === 'system' ? baseline!.win_rate
    : null
  const reversalTrigger = discoveryState?.reversal_trigger ?? 'NONE'
  const extensionRisk = discoveryState?.extension_risk ?? 'NORMAL'
  const positionBlocksPlan = planSide === 'LONG'
    ? reversalTrigger.startsWith('BEARISH')
      || (discoveryState?.trend_state === 'UPTREND' && extensionRisk !== 'NORMAL')
    : planSide === 'SHORT'
      ? reversalTrigger.startsWith('BULLISH')
        || (discoveryState?.trend_state === 'DOWNTREND' && extensionRisk !== 'NORMAL')
      : false
  const plan = tradeSetup && !positionBlocksPlan
    ? buildTradePlan(tradeSetup, isDailySetup ? tickerRec : null, sideWinRate)
    : null
  const agreement: 'aligned' | 'conflict' | 'model-only' | 'technical-only' | 'none' =
    techSide && modelSide ? (techSide === modelSide ? 'aligned' : 'conflict')
    : modelSide ? 'model-only'
    : techSide ? 'technical-only'
    : 'none'

  const tileStyle: CSSProperties = {
    padding: '0.7rem 0.85rem',
    background: '#fff',
    border: '1px solid #e2e8f0',
    borderRadius: '0.5rem',
  }
  const labelStyle: CSSProperties = {
    fontSize: '0.66rem',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    color: MUTED,
    fontWeight: 700,
  }
  const panelStyle: CSSProperties = {
    marginBottom: '0.85rem',
    padding: '0.85rem',
    background: '#fff',
    border: '1px solid #e2e8f0',
    borderRadius: '0.5rem',
  }
  const panelHeadStyle: CSSProperties = {
    fontSize: '0.68rem',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    color: MUTED,
    fontWeight: 700,
    marginBottom: '0.6rem',
  }

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
  useEffect(() => { prevSetupIntervalRef.current = setupInterval }, [setupInterval])

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
    const calibrationKey = ['calibration', symbol]
    queryClient.setQueryData(chartKey, undefined)
    queryClient.setQueryData(setupKey, undefined)
    queryClient.setQueryData(quoteKey, undefined)
    queryClient.setQueryData(calibrationKey, undefined)
    await Promise.all([
      queryClient.fetchQuery({ queryKey: chartKey, queryFn: () => getChartData(symbol!, chartRequestPeriod, interval, true) }),
      queryClient.fetchQuery({ queryKey: setupKey, queryFn: () => getTradeSetup(symbol!, setupInterval, true) }),
      queryClient.fetchQuery({ queryKey: quoteKey, queryFn: () => getLatestQuote(symbol!, true) }),
      queryClient.fetchQuery({ queryKey: calibrationKey, queryFn: () => getTickerCalibration(symbol!, { refresh: true }) }),
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
      upColor: '#16a34a',
      downColor: '#dc2626',
      borderDownColor: '#dc2626',
      borderUpColor: '#16a34a',
      wickDownColor: '#dc2626',
      wickUpColor: '#16a34a',
    })
    candleSeriesRef.current = candleSeries

    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: '#26a69a',
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    })
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    })
    volumeSeriesRef.current = volumeSeries

    ma5SeriesRef.current = chart.addSeries(LineSeries, {
      color: '#000000', lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
    })
    ma50SeriesRef.current = chart.addSeries(LineSeries, {
      color: '#008000', lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
    })
    ma100SeriesRef.current = chart.addSeries(LineSeries, {
      color: '#800000', lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
    })
    ma200SeriesRef.current = chart.addSeries(LineSeries, {
      color: '#ff0000', lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
    })
    bbMiddleSeriesRef.current = chart.addSeries(LineSeries, {
      color: '#6b7280', lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false,
    })
    bbUpperSeriesRef.current = chart.addSeries(LineSeries, {
      color: '#2563eb', lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    bbLowerSeriesRef.current = chart.addSeries(LineSeries, {
      color: '#2563eb', lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
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
        { series: candleSeriesRef.current!, label: 'OHLC', color: '#333', isCandle: true },
        { series: ma5SeriesRef.current!, label: 'MA 5', color: '#000000', isCandle: false },
        { series: ma50SeriesRef.current!, label: 'MA 50', color: '#008000', isCandle: false },
        { series: ma100SeriesRef.current!, label: 'MA 100', color: '#800000', isCandle: false },
        { series: ma200SeriesRef.current!, label: 'MA 200', color: '#ff0000', isCandle: false },
        { series: bbMiddleSeriesRef.current!, label: 'BB Mid', color: '#6b7280', isCandle: false },
        { series: bbUpperSeriesRef.current!, label: 'BB Up', color: '#2563eb', isCandle: false },
        { series: bbLowerSeriesRef.current!, label: 'BB Lo', color: '#2563eb', isCandle: false },
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
        line1 += `<span style="color:#6b7280">Vol: ${(vol.value / 1e6).toFixed(2)}M</span>`
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
      color: d.close >= d.open ? 'rgba(22, 163, 74, 0.5)' : 'rgba(220, 38, 38, 0.5)',
    }))

    candleSeriesRef.current.setData(candleData)
    volumeSeriesRef.current?.setData(volumeData)
    ma5SeriesRef.current?.setData(buildSmaSeries(chartData, 5))
    ma50SeriesRef.current?.setData(buildSmaSeries(chartData, 50))
    ma100SeriesRef.current?.setData(buildSmaSeries(chartData, 100))
    ma200SeriesRef.current?.setData(buildSmaSeries(chartData, 200))

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
                    color: priceChange >= 0 ? '#16a34a' : '#dc2626',
                    fontWeight: 500
                  }}>
                    {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)} ({priceChangePercent >= 0 ? '+' : ''}{priceChangePercent.toFixed(2)}%)
                  </span>
                </div>
              )}
              {(latestQuote || lastPrice) && (
                <div style={{ fontSize: '0.75rem', color: '#94a3b8', marginTop: '0.15rem' }}>
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
                { label: 'Period High', value: `$${Math.max(...visibleChartData.map(d => d.high)).toFixed(2)}`, color: '#16a34a' },
                { label: 'Period Low', value: `$${Math.min(...visibleChartData.map(d => d.low)).toFixed(2)}`, color: '#dc2626' },
                { label: 'Avg Vol', value: `${(visibleChartData.reduce((s, d) => s + d.volume, 0) / visibleChartData.length / 1000000).toFixed(2)}M` },
                { label: 'Points', value: `${visibleChartData.length}` },
              ].map(stat => (
                <div key={stat.label} style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: '1.3rem', fontWeight: 600, color: stat.color || '#1e293b' }}>{stat.value}</div>
                  <div style={{ fontSize: '0.78rem', color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.04em' }}>{stat.label}</div>
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
                    color: period === p ? '#1e293b' : '#64748b',
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
                <span style={{ color: priceChange >= 0 ? '#16a34a' : '#dc2626', marginLeft: '6px', fontSize: '0.8rem' }}>
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
              color: '#1e293b',
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
                {tradeSetup.date} · close {money(tradeSetup.last_close)} · {setupInterval} · {tradeSetup.momentum.state}
              </div>
            )}
          </div>
          <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', flexWrap: 'wrap' }}>
            {tradeSetup?.golden_cross && (
              <span
                title={tradeSetup.golden_cross.detail}
                style={{
                  padding: '0.2rem 0.6rem',
                  borderRadius: '0.35rem',
                  fontWeight: 600,
                  fontSize: '0.72rem',
                  border: `1px solid ${tradeSetup.golden_cross.type.includes('Golden') || tradeSetup.golden_cross.type.includes('Bullish') ? POS : NEG}`,
                  color: tradeSetup.golden_cross.type.includes('Golden') || tradeSetup.golden_cross.type.includes('Bullish') ? POS : NEG,
                }}
              >
                {tradeSetup.golden_cross.type === 'Golden Cross' ? `Golden Cross${tradeSetup.golden_cross.bars_ago !== null ? ` · ${tradeSetup.golden_cross.bars_ago}b` : ''}`
                  : tradeSetup.golden_cross.type === 'Death Cross' ? `Death Cross${tradeSetup.golden_cross.bars_ago !== null ? ` · ${tradeSetup.golden_cross.bars_ago}b` : ''}`
                  : tradeSetup.golden_cross.type.includes('Bullish') ? '50 > 200 SMA'
                  : '50 < 200 SMA'}
              </span>
            )}
            <div style={{ display: 'flex', gap: '0.25rem', background: '#f1f5f9', borderRadius: '0.375rem', padding: '0.15rem' }}>
              {['1d', '1h', '30m', '15m', '5m'].map(iv => (
                <button
                  key={iv}
                  onClick={() => setSetupInterval(iv)}
                  style={{
                    padding: '0.2rem 0.5rem',
                    fontSize: '0.75rem',
                    fontWeight: setupInterval === iv ? 700 : 400,
                    border: 'none',
                    borderRadius: '0.25rem',
                    background: setupInterval === iv ? '#2563eb' : 'transparent',
                    color: setupInterval === iv ? '#fff' : MUTED,
                    cursor: 'pointer',
                  }}
                >
                  {iv}
                </button>
              ))}
            </div>
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
            {/* Verdict strip — the four questions to answer before sizing a trade */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '0.6rem', marginBottom: '0.85rem' }}>
              <div style={{ ...tileStyle, borderLeft: `3px solid ${techSide === 'LONG' ? POS : techSide === 'SHORT' ? NEG : MUTED}` }}>
                <div style={labelStyle}>Bias</div>
                <div style={{ fontSize: '1.15rem', fontWeight: 700, color: techSide === 'LONG' ? POS : techSide === 'SHORT' ? NEG : MUTED }}>
                  {techSide ?? 'NEUTRAL'}
                </div>
                <div style={{ fontSize: '0.72rem', color: MUTED }}>
                  {tradeSetup.direction.conviction} · {tradeSetup.direction.bull_signals}↑ / {tradeSetup.direction.bear_signals}↓
                </div>
                <div style={{
                  fontSize: '0.7rem',
                  fontWeight: 600,
                  marginTop: '0.3rem',
                  color: !isDailySetup ? MUTED : agreement === 'aligned' ? POS : agreement === 'conflict' ? NEG : MUTED,
                }}>
                  {!isDailySetup
                    ? (dailyModelSide ? `Daily model: ${dailyModelSide} · not this timeframe` : 'No daily model call')
                    : agreement === 'aligned' ? `✓ Model agrees (${modelSide})`
                    : agreement === 'conflict' ? `⚠ Model says ${modelSide}`
                    : agreement === 'model-only' ? `Model: ${modelSide}, technicals flat`
                    : agreement === 'technical-only' ? 'No model call today'
                    : 'No directional call'}
                </div>
              </div>

              <div style={tileStyle}>
                <div style={labelStyle}>Confidence</div>
                <div style={{ fontSize: '1.15rem', fontWeight: 700, color: isDailySetup && tickerRec ? INK : gradeTone(tradeSetup.confluence.grade) }}>
                  {isDailySetup && tickerRec ? `${tickerRec.predicted_confidence}%` : tradeSetup.confluence.grade}
                </div>
                <div style={{ fontSize: '0.72rem', color: MUTED }}>
                  {isDailySetup && tickerRec
                    ? `Model grade ${tickerRec.signal_grade ?? '—'}`
                    : `Confluence · ${tradeSetup.confluence.count} signals`}
                </div>
                <div style={{ fontSize: '0.7rem', fontWeight: 600, marginTop: '0.3rem', color: gradeTone(tradeSetup.confluence.grade) }}>
                  {!isDailySetup
                    ? (tickerRec ? `Daily model: ${tickerRec.predicted_confidence}% ${tickerRec.direction}` : 'No daily model call')
                    : tickerRec
                      ? `Confluence ${tradeSetup.confluence.grade} · ${tradeSetup.confluence.count} signals`
                      : 'No daily model call'}
                </div>
              </div>

              {(() => {
                const tickerN = tickerPerf?.total_recs ?? 0
                const useTicker = tickerN >= MIN_RELIABLE_SAMPLE
                const k = useTicker ? tickerPerf!.correct_recs : baseline?.correct_recs ?? 0
                const n = useTicker ? tickerN : baseline?.total_recs ?? 0
                const rate = useTicker ? tickerPerf!.win_rate : baseline?.win_rate ?? 0
                const ci = wilson95(k, n)
                // Colour on statistical significance, not the point estimate.
                const tone = !ci ? MUTED : ci[0] > 50 ? POS : ci[1] < 50 ? NEG : MUTED
                return (
                  <div style={tileStyle}>
                    <div style={labelStyle}>Model reliability</div>
                    <div style={{ fontSize: '1.15rem', fontWeight: 700, color: tone }}>
                      {n > 0 ? plainPct(rate) : 'No data'}
                    </div>
                    <div style={{ fontSize: '0.72rem', color: MUTED }}>
                      {ci ? `95% CI ${ci[0].toFixed(0)}–${ci[1].toFixed(0)}% · n=${n}` : 'No resolved calls yet'}
                    </div>
                    <div style={{ fontSize: '0.7rem', marginTop: '0.3rem', color: MUTED }}>
                      {n === 0 ? 'Backtest has not run'
                        : useTicker ? `${symbol} only · ${tickerPerf!.period_days}d`
                        : `System-wide · ${baseline?.ticker_count ?? 0} tickers · ${symbol} sample too small (${tickerN})`}
                    </div>
                    {ci && ci[0] <= 50 && ci[1] >= 50 && (
                      <div style={{ fontSize: '0.68rem', marginTop: '0.2rem', color: WARN }}>
                        Not distinguishable from chance
                      </div>
                    )}
                  </div>
                )
              })()}

              {(() => {
                const tone = xsSignal?.side === 'LONG' ? POS
                  : xsSignal?.side === 'SHORT' ? NEG
                  : MUTED
                const rank = xsSignal && xsSignal.percentile != null
                  ? Math.round((1 - xsSignal.percentile) * (xsSignal.universe_size - 1)) + 1
                  : null
                return (
                  <div style={{ ...tileStyle, borderLeft: `3px solid ${tone}` }}>
                    <div style={labelStyle}>Universe rank</div>
                    <div style={{ fontSize: '1.15rem', fontWeight: 700, color: tone }}>
                      {xsSignal ? (xsSignal.side === 'FLAT' ? `D${xsSignal.decile}` : xsSignal.side) : 'No signal'}
                    </div>
                    <div style={{ fontSize: '0.72rem', color: MUTED }}>
                      {xsSignal && rank != null
                        ? `Rank ${rank} of ${xsSignal.universe_size} · decile ${xsSignal.decile}`
                        : 'Not scored today'}
                    </div>
                    <div style={{ fontSize: '0.7rem', marginTop: '0.3rem', color: MUTED }}>
                      {xsSignal
                        ? `${xsSignal.model_version} · ${xsSignal.horizon_days}d hold · validated`
                        : 'Runs after each close'}
                    </div>
                  </div>
                )
              })()}

              {(() => {
                const state = discoveryState?.state ?? null
                const extension = discoveryState?.extension_risk ?? null
                const tone = extension === 'EXHAUSTION_WATCH' ? NEG
                  : extension === 'EXTENDED' ? WARN
                  : discoveryState?.trend_state === 'UPTREND' ? POS
                  : discoveryState?.trend_state === 'DOWNTREND' ? NEG
                  : MUTED
                const label = state ? state.replace(/_/g, ' ') : 'No state'
                return (
                  <div style={{ ...tileStyle, borderLeft: `3px solid ${tone}` }}>
                    <div style={labelStyle}>Current position</div>
                    <div style={{ fontSize: '1rem', fontWeight: 700, color: tone }}>
                      {discoveryState?.trend_state?.replace(/_/g, ' ') ?? label}
                      {extension && ` · ${extension.replace(/_/g, ' ')}`}
                    </div>
                    <div style={{ fontSize: '0.72rem', color: MUTED }}>
                      Discovery: {label}
                      {discoveryState?.reversal_trigger && discoveryState.reversal_trigger !== 'NONE'
                        ? ` · ${discoveryState.reversal_trigger.replace(/_/g, ' ')}` : ''}
                    </div>
                    <div style={{ fontSize: '0.7rem', marginTop: '0.3rem', color: tone }} title={discoveryState?.position_guidance ?? undefined}>
                      {discoveryState?.position_guidance ?? 'Runs after a complete close'}
                    </div>
                  </div>
                )
              })()}

              <div style={{ ...tileStyle, borderLeft: `3px solid ${tradeSetup.timing.urgency === 'Immediate' ? POS : tradeSetup.timing.urgency === 'Watchlist' ? MUTED : WARN}` }}>
                <div style={labelStyle}>Timing</div>
                <div style={{ fontSize: '1.15rem', fontWeight: 700, color: INK }}>{tradeSetup.timing.urgency}</div>
                <div style={{ fontSize: '0.72rem', color: MUTED }} title={tradeSetup.timing.detail}>
                  {tradeSetup.duration.estimate}
                </div>
                <div style={{ fontSize: '0.7rem', marginTop: '0.3rem', color: MUTED }} title={tradeSetup.timing.detail}>
                  {tradeSetup.timing.detail.length > 58 ? `${tradeSetup.timing.detail.slice(0, 58)}…` : tradeSetup.timing.detail}
                </div>
              </div>
            </div>

            {tickerScannerEvents.events.length > 0 && (
              <div style={{ border: '1px solid #e2e8f0', borderRadius: '0.5rem', marginBottom: '1rem', overflow: 'hidden' }}>
                <div style={{ padding: '0.55rem 0.85rem', background: '#f8fafc', borderBottom: '1px solid #e2e8f0', display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
                  <div>
                    <strong style={{ fontSize: '0.84rem', color: INK }}>Scanner setup history</strong>
                    <div style={{ fontSize: '0.7rem', color: MUTED, marginTop: '0.15rem' }}>
                      Signal open/close are the matching bar (the full session for Daily, the trigger hour for Hourly). Planned stop-loss and 2R target are measured from its close; next-bar open is the no-look-ahead evaluation entry.
                    </div>
                  </div>
                  <span style={{ fontSize: '0.7rem', color: WARN }}>Research signals · not recommendations</span>
                </div>
                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem' }}>
                    <thead><tr style={{ borderBottom: '1px solid #e2e8f0' }}>
                      {['Interval', 'Scanner setup', 'Signal time', 'Last seen', 'Signal open', 'Signal close', 'Planned stop-loss', 'Planned target (2R)', 'Next-bar open', 'Exit close by horizon', 'Return / alpha', 'MAE / MFE', 'First hit'].map(label => (
                        <th key={label} style={{ padding: '6px 8px', textAlign: label === 'Scanner setup' || label === 'First hit' ? 'left' : 'right', color: MUTED, whiteSpace: 'nowrap' }}>{label}</th>
                      ))}
                    </tr></thead>
                    <tbody>{tickerScannerEvents.events.slice(0, 8).map(event => {
                      const nextOpen = event.outcomes.find(outcome => outcome.entry_price != null)
                      return (
                        <tr key={event.event_id} style={{ borderBottom: '1px solid #f1f5f9' }}>
                        <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap', fontWeight: 600 }}>
                          {event.interval === '1d' ? 'Daily' : event.interval === '1wk' ? 'Weekly' : 'Hourly'}
                        </td>
                        <td style={{ padding: '7px 8px' }}>
                          <div style={{ fontWeight: 600, color: event.direction === 1 ? POS : NEG }}>
                            {event.direction === 1 ? 'Long' : 'Short'} · {event.trigger_type.replace(/_/g, ' ')}
                          </div>
                          <div style={{ color: MUTED, fontSize: '0.68rem' }}>
                            {event.scanner_name.replace(/_/g, ' ').replace(/^sma200/i, 'SMA200')}
                          </div>
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                          {formatScannerEventTime(event.signal_time, event.interval)}
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                          {formatScannerEventTime(event.last_seen_at, event.interval)}
                          <div style={{ color: MUTED, fontSize: '0.68rem' }}>{event.occurrence_count} observation{event.occurrence_count === 1 ? '' : 's'}</div>
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right' }}>
                          {money(event.signal_open_price)}
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right' }}>
                          <strong>{money(event.entry_price)}</strong>
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap', color: NEG }}>
                          {money(event.stop_price)}
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap', color: POS, fontWeight: 600 }}>
                          {money(event.target_price)}
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right' }}>
                          {nextOpen?.entry_price != null ? <strong>{money(nextOpen.entry_price)}</strong> : <span style={{ color: MUTED }}>Pending</span>}
                          {nextOpen?.entry_time && <div style={{ color: MUTED, fontSize: '0.68rem', whiteSpace: 'nowrap' }}>{formatScannerEventTime(nextOpen.entry_time, event.interval)}</div>}
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                          {event.outcomes.length === 0 ? <span style={{ color: MUTED }}>Waiting for future bars</span> : event.outcomes.map(outcome => (
                            <div key={outcome.horizon_bars} title={formatScannerEventTime(outcome.exit_time, event.interval)}>
                              {outcome.horizon_bars} {event.interval === '1wk' ? 'sessions' : 'bars'} · {money(outcome.exit_price)}
                            </div>
                          ))}
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                          {event.outcomes.length === 0 ? '—' : event.outcomes.map(outcome => (
                            <div key={outcome.horizon_bars}>
                              <span style={{ color: (outcome.net_signed_return ?? 0) >= 0 ? POS : NEG }}>
                                {outcome.net_signed_return != null ? `${(outcome.net_signed_return * 100).toFixed(1)}%` : '—'}
                              </span>
                              {outcome.net_alpha_return != null ? ` · α ${(outcome.net_alpha_return * 100).toFixed(1)}%` : ''}
                            </div>
                          ))}
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                          {event.outcomes.length === 0 ? '—' : event.outcomes.map(outcome => (
                            <div key={outcome.horizon_bars}>
                              {outcome.mae_pct != null ? `${(outcome.mae_pct * 100).toFixed(1)}%` : '—'} / {outcome.mfe_pct != null ? `${(outcome.mfe_pct * 100).toFixed(1)}%` : '—'}
                            </div>
                          ))}
                        </td>
                        <td style={{ padding: '7px 8px', textAlign: 'left', whiteSpace: 'nowrap' }}>
                          {event.outcomes.length === 0 ? 'Pending' : event.outcomes.map(outcome => (
                            <div key={outcome.horizon_bars}>{outcome.first_hit.replace('_', ' ')}</div>
                          ))}
                        </td>
                      </tr>
                      )
                    })}</tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Executable plan — levels, risk math and sizing */}
            {plan ? (
              <div style={{ border: '1px solid #e2e8f0', borderRadius: '0.5rem', marginBottom: '1rem', overflow: 'hidden' }}>
                <div style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '0.45rem 0.85rem',
                  background: plan.side === 'LONG' ? '#f0fdf4' : '#fef2f2',
                  borderBottom: '1px solid #e2e8f0',
                }}>
                  <strong style={{ fontSize: '0.85rem', color: plan.side === 'LONG' ? POS : NEG }}>
                    {plan.side} plan
                  </strong>
                  <span style={{ fontSize: '0.7rem', color: MUTED }}>
                    {plan.fromModel ? 'Model levels' : 'Derived from nearest technical levels'}
                  </span>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', borderBottom: '1px solid #f1f5f9' }}>
                  {[
                    { k: 'Entry', v: plan.entry, sub: plan.fromModel ? 'Model entry' : 'Last close', tone: INK },
                    { k: 'Stop', v: plan.stop, sub: plan.stopLabel, tone: NEG },
                    { k: 'Target', v: plan.target, sub: plan.targetLabel, tone: POS },
                  ].map(x => (
                    <div key={x.k} style={{ padding: '0.7rem 0.85rem', borderRight: '1px solid #f1f5f9' }}>
                      <div style={labelStyle}>{x.k}</div>
                      <div style={{ fontSize: '1.25rem', fontWeight: 700, color: x.tone }}>{money(x.v)}</div>
                      <div style={{ fontSize: '0.7rem', color: MUTED }}>
                        {x.k === 'Entry' ? x.sub : `${signedPct(((x.v - plan.entry) / plan.entry) * 100)} · ${x.sub}`}
                      </div>
                    </div>
                  ))}
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', borderBottom: '1px solid #f1f5f9' }}>
                  <div style={{ padding: '0.7rem 0.85rem', borderRight: '1px solid #f1f5f9' }}>
                    <div style={labelStyle}>Risk / share</div>
                    <div style={{ fontSize: '1rem', fontWeight: 700, color: NEG }}>{money(plan.risk)}</div>
                    <div style={{ fontSize: '0.7rem', color: MUTED }}>{plainPct(plan.riskPct)} of entry</div>
                  </div>
                  <div style={{ padding: '0.7rem 0.85rem', borderRight: '1px solid #f1f5f9' }}>
                    <div style={labelStyle}>Reward / share</div>
                    <div style={{ fontSize: '1rem', fontWeight: 700, color: POS }}>{money(plan.reward)}</div>
                    <div style={{ fontSize: '0.7rem', color: MUTED }}>{plainPct(plan.rewardPct)} of entry</div>
                  </div>
                  <div style={{ padding: '0.7rem 0.85rem', borderRight: '1px solid #f1f5f9' }}>
                    <div style={labelStyle}>Reward : Risk</div>
                    <div style={{ fontSize: '1rem', fontWeight: 700, color: plan.rr >= 2 ? POS : plan.rr >= 1 ? WARN : NEG }}>
                      {plan.rr.toFixed(2)}R
                    </div>
                    <div style={{ fontSize: '0.7rem', color: MUTED }}>
                      {plan.rr >= 2 ? 'Meets 2R floor' : plan.rr >= 1 ? 'Thin — tighten stop' : 'Below 1R — skip'}
                    </div>
                  </div>
                  <div style={{ padding: '0.7rem 0.85rem' }}>
                    <div style={labelStyle}>Expected value</div>
                    <div style={{
                      fontSize: '1rem',
                      fontWeight: 700,
                      color: plan.evPerShare === null ? MUTED : plan.evPerShare > 0 ? POS : NEG,
                    }}>
                      {plan.evPerShare === null ? 'n/a' : `${plan.evPerShare > 0 ? '+' : ''}${money(plan.evPerShare)}/sh`}
                    </div>
                    <div style={{ fontSize: '0.7rem', color: MUTED }}>
                      {plan.winRate === null
                        ? (isDailySetup ? 'Sample too small to estimate' : 'Daily win rate does not apply intraday')
                        : winRateBasis === 'ticker'
                          ? `at ${plainPct(plan.winRate)} ${symbol} ${plan.side} accuracy`
                          : `at ${plainPct(plan.winRate)} system-wide accuracy`}
                    </div>
                  </div>
                </div>

                {plan.evPerShare !== null && (
                  <div style={{ padding: '0.4rem 0.85rem', fontSize: '0.66rem', color: MUTED, borderTop: '1px solid #f1f5f9' }}>
                    EV assumes the historical directional win rate holds and the trade resolves at stop or target; it is not a target-hit probability.
                  </div>
                )}
              </div>
            ) : (
              <div style={{ padding: '0.85rem', marginBottom: '1rem', border: '1px dashed #cbd5e1', borderRadius: '0.5rem', fontSize: '0.82rem', color: MUTED }}>
                {positionBlocksPlan && planSide && discoveryState?.position_guidance
                  ? `No executable ${planSide} plan — ${discoveryState.position_guidance}`
                  : `No executable plan — needs a directional bias plus valid stop and first-target levels that provide at least ${MIN_EXECUTABLE_RR}R around ${money(tradeSetup.last_close)}.`}
              </div>
            )}

            {tradeSetup.strategy_results.fibonacci && (() => {
              const fibonacci = tradeSetup.strategy_results.fibonacci
              const confirmedMove = fibonacci.trend_direction === 'uptrend_retracement'
                ? `Low ${money(fibonacci.swing_low)} (${fibonacci.swing_low_date}) → High ${money(fibonacci.swing_high)} (${fibonacci.swing_high_date})`
                : `High ${money(fibonacci.swing_high)} (${fibonacci.swing_high_date}) → Low ${money(fibonacci.swing_low)} (${fibonacci.swing_low_date})`
              const active = fibonacci.active_leg
              const activeMove = active
                ? `${active.start.type === 'high' ? 'High' : 'Low'} ${money(active.start.price)} (${active.start.date}) → ${active.end.type === 'high' ? 'High' : 'Low'} ${money(active.end.price)} (${active.end.date})`
                : null
              return (
                <div className="ticker-fibonacci-basis">
                  <div>
                    <div style={labelStyle}>Confirmed basis</div>
                    <div className="ticker-fibonacci-basis__move">{confirmedMove}</div>
                  </div>
                  {active && activeMove && (
                    <div>
                      <div style={labelStyle}>Active provisional</div>
                      <div className="ticker-fibonacci-basis__move">{activeMove}</div>
                    </div>
                  )}
                  <div>
                    <div style={labelStyle}>Detection</div>
                    <div className="ticker-fibonacci-basis__value">{fibonacci.swing_detection_pct.toFixed(2)}% dynamic</div>
                  </div>
                  <div>
                    <div style={labelStyle}>Confirmed reference</div>
                    <div className="ticker-fibonacci-basis__value">
                      {fibonacci.nearest_level} · {money(fibonacci.nearest_level_price)} · {fibonacci.distance_pct > 0 ? '+' : ''}{fibonacci.distance_pct.toFixed(2)}%
                    </div>
                  </div>
                  <div>
                    <div style={labelStyle}>{active?.level_role === 'provisional_support' ? 'Provisional support' : active ? 'Provisional resistance' : 'Developing pivot'}</div>
                    <div className="ticker-fibonacci-basis__value">
                      {active
                        ? <>{active.nearest_level} · {money(active.nearest_level_price)} · {active.distance_pct > 0 ? '+' : ''}{active.distance_pct.toFixed(2)}%</>
                        : <>{fibonacci.developing_pivot.type === 'high' ? 'High' : 'Low'} · {money(fibonacci.developing_pivot.price)} · {fibonacci.developing_pivot.date}</>}
                    </div>
                  </div>
                </div>
              )
            })()}

            {/* Evidence tabs */}
            <div style={{ display: 'flex', gap: '0.25rem', borderBottom: '1px solid #e2e8f0', marginBottom: '0.85rem' }}>
              {([
                ['technicals', 'Technicals', null],
                ['levels', 'Levels', (tradeSetup.level_retests?.daily?.length ?? 0) + (tradeSetup.level_retests?.hourly?.length ?? 0)],
                ['signals', 'Signals', tradeSetup.signals?.length ?? 0],
                ['model', 'Model', patternScores?.fired_count ?? null],
              ] as const).map(([key, label, count]) => (
                <button
                  key={key}
                  onClick={() => setEvidenceTab(key)}
                  style={{
                    padding: '0.4rem 0.8rem',
                    border: 'none',
                    background: 'transparent',
                    cursor: 'pointer',
                    fontSize: '0.8rem',
                    fontWeight: evidenceTab === key ? 700 : 500,
                    color: evidenceTab === key ? INK : MUTED,
                    borderBottom: `2px solid ${evidenceTab === key ? '#2563eb' : 'transparent'}`,
                  }}
                >
                  {label}{count ? ` (${count})` : ''}
                </button>
              ))}
            </div>

            {evidenceTab === 'technicals' && (
              <>
            {/* EMA / SMA Alignment + VWAP row */}
            <div className="dashboard-grid" style={{ gridTemplateColumns: 'repeat(3, 1fr)', marginBottom: '1rem' }}>
              <div className="stat-card" style={{ textAlign: 'center', padding: '0.75rem', borderLeft: `4px solid ${tradeSetup.ema_alignment.daily.includes('Bullish') ? '#16a34a' : tradeSetup.ema_alignment.daily.includes('Bearish') ? '#dc2626' : '#9ca3af'}` }}>
                <div className="stat-value" style={{ fontSize: '0.95rem' }}>{tradeSetup.ema_alignment.daily}</div>
                <div className="stat-label">Daily EMA / SMA Stack</div>
                <table style={{ width: '100%', fontSize: '0.7rem', marginTop: '0.35rem', borderCollapse: 'collapse', color: 'var(--text-secondary)' }}>
                  <tbody>
                    {[
                      { label: '8 EMA', val: tradeSetup.technicals.ema8, sma: 'MA 10', smaVal: tradeSetup.technicals.ma10 },
                      { label: '21 EMA', val: tradeSetup.technicals.ema21, sma: 'MA 20', smaVal: tradeSetup.technicals.ma20 },
                      { label: '50 EMA', val: tradeSetup.technicals.ema50, sma: 'MA 50', smaVal: tradeSetup.technicals.ma50 },
                    ].map(r => (
                      <tr key={r.label}>
                        <td style={{ textAlign: 'left', padding: '1px 2px', fontWeight: 500 }}>{r.label}</td>
                        <td style={{ textAlign: 'right', padding: '1px 2px', color: tradeSetup.last_close >= (r.val ?? 0) ? '#16a34a' : '#dc2626' }}>${r.val?.toFixed(2) ?? 'N/A'}</td>
                        <td style={{ textAlign: 'left', padding: '1px 6px', fontWeight: 500 }}>{r.sma}</td>
                        <td style={{ textAlign: 'right', padding: '1px 2px', color: tradeSetup.last_close >= (r.smaVal ?? 0) ? '#16a34a' : '#dc2626' }}>${r.smaVal?.toFixed(2) ?? 'N/A'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="stat-card" style={{ textAlign: 'center', padding: '0.75rem', borderLeft: `4px solid ${tradeSetup.ema_alignment.hourly === 'Bullish' ? '#16a34a' : tradeSetup.ema_alignment.hourly === 'Bearish' ? '#dc2626' : '#9ca3af'}` }}>
                <div className="stat-value" style={{ fontSize: '0.95rem' }}>{tradeSetup.ema_alignment.hourly ?? 'N/A'}</div>
                <div className="stat-label">Hourly EMA (8/21)</div>
                {tradeSetup.ema_alignment.hourly_ema8 && tradeSetup.ema_alignment.hourly_ema21 && (
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
                    8: ${tradeSetup.ema_alignment.hourly_ema8} | 21: ${tradeSetup.ema_alignment.hourly_ema21}
                  </div>
                )}
                {tradeSetup.ema_alignment.multi_tf_agree !== null && (
                  <div style={{ fontSize: '0.7rem', fontWeight: 600, marginTop: '0.15rem', color: tradeSetup.ema_alignment.multi_tf_agree ? '#16a34a' : '#dc2626' }}>
                    {tradeSetup.ema_alignment.multi_tf_agree ? '✓ Multi-TF Aligned' : '✗ TF Divergence'}
                  </div>
                )}
              </div>
              <div className="stat-card" style={{ textAlign: 'center', padding: '0.75rem', borderLeft: `4px solid ${tradeSetup.technicals.price_vs_vwap === 'Above' ? '#16a34a' : '#dc2626'}` }}>
                <div className="stat-value" style={{ fontSize: '0.95rem' }}>${tradeSetup.technicals.vwap}</div>
                <div className="stat-label">VWAP(20) — Price {tradeSetup.technicals.price_vs_vwap}</div>
                <table style={{ width: '100%', fontSize: '0.7rem', marginTop: '0.35rem', borderCollapse: 'collapse', color: 'var(--text-secondary)' }}>
                  <tbody>
                    {[
                      { label: 'MA 100', val: tradeSetup.technicals.ma100 },
                      { label: 'MA 200', val: tradeSetup.technicals.ma200 },
                    ].map(r => {
                      const d = r.val ? ((tradeSetup.last_close - r.val) / r.val * 100) : null
                      return (
                        <tr key={r.label}>
                          <td style={{ textAlign: 'left', padding: '1px 2px', fontWeight: 500 }}>{r.label}</td>
                          <td style={{ textAlign: 'right', padding: '1px 2px', color: d !== null ? (d >= 0 ? '#16a34a' : '#dc2626') : 'inherit' }}>
                            ${r.val?.toFixed(2) ?? 'N/A'}
                          </td>
                          <td style={{ textAlign: 'right', padding: '1px 6px', color: d !== null ? (d >= 0 ? '#16a34a' : '#dc2626') : 'inherit', fontWeight: 500 }}>
                            {d !== null ? `${d >= 0 ? '+' : ''}${d.toFixed(1)}%` : '—'}
                          </td>
                        </tr>
                      )
                    })}
                    <tr>
                      <td style={{ textAlign: 'left', padding: '1px 2px', fontWeight: 500 }}>Dist 8E</td>
                      <td colSpan={2} style={{ textAlign: 'right', padding: '1px 2px', color: tradeSetup.technicals.dist_to_8ema >= 0 ? '#16a34a' : '#dc2626', fontWeight: 500 }}>
                        {tradeSetup.technicals.dist_to_8ema > 0 ? '+' : ''}{tradeSetup.technicals.dist_to_8ema}% | 21E: {tradeSetup.technicals.dist_to_21ema > 0 ? '+' : ''}{tradeSetup.technicals.dist_to_21ema}%
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Technicals row */}
            <div className="dashboard-grid" style={{ gridTemplateColumns: 'repeat(4, 1fr)', marginBottom: '1rem' }}>
              <div className="stat-card" style={{ textAlign: 'center', padding: '0.75rem' }}>
                <div className="stat-value" style={{ fontSize: '1rem', color: tradeSetup.technicals.rsi > 70 ? '#dc2626' : tradeSetup.technicals.rsi < 30 ? '#16a34a' : 'inherit' }}>
                  {tradeSetup.technicals.rsi.toFixed(1)}
                </div>
                <div className="stat-label">RSI ({tradeSetup.technicals.rsi_state})</div>
              </div>
              <div className="stat-card" style={{ textAlign: 'center', padding: '0.75rem' }}>
                <div className="stat-value" style={{ fontSize: '1rem' }}>{tradeSetup.technicals.stoch_k.toFixed(1)}</div>
                <div className="stat-label">Stoch %K</div>
              </div>
              <div className="stat-card" style={{ textAlign: 'center', padding: '0.75rem' }}>
                <div className="stat-value" style={{ fontSize: '1rem' }}>${tradeSetup.technicals.atr.toFixed(2)}</div>
                <div className="stat-label">ATR ({tradeSetup.technicals.atr_pct.toFixed(1)}%)</div>
              </div>
              <div className="stat-card" style={{ textAlign: 'center', padding: '0.75rem' }}>
                <div className="stat-value" style={{ fontSize: '1rem' }}>{tradeSetup.technicals.trend_consistency.toFixed(0)}%</div>
                <div className="stat-label">Trend Consistency</div>
              </div>
            </div>

            <div style={{ fontSize: '0.75rem', color: MUTED, marginBottom: '1rem' }}>
              {tradeSetup.momentum.state} — {tradeSetup.momentum.detail}
            </div>
              </>
            )}

            {evidenceTab === 'levels' && (
              <>
            {/* Level Retests */}
            {(tradeSetup.level_retests.daily.length > 0 || tradeSetup.level_retests.hourly.length > 0) && (
              <div style={{ marginBottom: '1rem' }}>
                <h3 style={{ fontSize: '0.95rem', fontWeight: 600, marginBottom: '0.5rem' }}>Level Retests</h3>
                <div style={{ display: 'grid', gridTemplateColumns: tradeSetup.level_retests.hourly.length > 0 ? '1fr 1fr' : '1fr', gap: '1rem' }}>
                  {tradeSetup.level_retests.daily.length > 0 && (
                    <div>
                      <div style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: '0.25rem' }}>Daily Timeframe</div>
                      <table className="data-table">
                        <thead>
                          <tr><th>Level</th><th>Price</th><th>Candle H/L</th><th>Touch</th><th>Held</th><th>Bounce</th></tr>
                        </thead>
                        <tbody>
                          {tradeSetup.level_retests.daily.map((rt: LevelRetest, i: number) => (
                            <tr key={i}>
                              <td style={{ fontWeight: 500, fontSize: '0.8rem' }}>{rt.level_name}</td>
                              <td>${rt.level_price.toFixed(2)}</td>
                              <td style={{ fontSize: '0.75rem' }}>H:${rt.candle_high.toFixed(2)} L:${rt.candle_low.toFixed(2)}</td>
                              <td style={{ fontSize: '0.75rem' }}>{rt.touch_type}</td>
                              <td>
                                <span style={{ color: rt.held ? '#16a34a' : '#dc2626', fontWeight: 600 }}>
                                  {rt.held ? '✓' : '✗'}
                                </span>
                              </td>
                              <td style={{ color: rt.bounce_pct >= 0 ? '#16a34a' : '#dc2626', fontWeight: 500, fontSize: '0.8rem' }}>
                                {rt.bounce_pct >= 0 ? '+' : ''}{rt.bounce_pct}%
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                  {tradeSetup.level_retests.hourly.length > 0 && (
                    <div>
                      <div style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: '0.25rem' }}>Hourly Timeframe</div>
                      <table className="data-table">
                        <thead>
                          <tr><th>Level</th><th>Price</th><th>Candle H/L</th><th>Touch</th><th>Held</th><th>Bounce</th></tr>
                        </thead>
                        <tbody>
                          {tradeSetup.level_retests.hourly.map((rt: LevelRetest, i: number) => (
                            <tr key={i}>
                              <td style={{ fontWeight: 500, fontSize: '0.8rem' }}>{rt.level_name}</td>
                              <td>${rt.level_price.toFixed(2)}</td>
                              <td style={{ fontSize: '0.75rem' }}>H:${rt.candle_high.toFixed(2)} L:${rt.candle_low.toFixed(2)}</td>
                              <td style={{ fontSize: '0.75rem' }}>{rt.touch_type}</td>
                              <td>
                                <span style={{ color: rt.held ? '#16a34a' : '#dc2626', fontWeight: 600 }}>
                                  {rt.held ? '✓' : '✗'}
                                </span>
                              </td>
                              <td style={{ color: rt.bounce_pct >= 0 ? '#16a34a' : '#dc2626', fontWeight: 500, fontSize: '0.8rem' }}>
                                {rt.bounce_pct >= 0 ? '+' : ''}{rt.bounce_pct}%
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Entry Criteria Table */}
            {tradeSetup.entries.length > 0 && (
              <div style={{ marginBottom: '1rem' }}>
                <h3 style={{ fontSize: '0.95rem', fontWeight: 600, marginBottom: '0.5rem' }}>Entry Criteria</h3>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Strategy</th>
                      <th>Condition</th>
                      <th>Price Zone</th>
                      <th>Strength</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tradeSetup.entries.map((e, i) => (
                      <tr key={i}>
                        <td style={{ fontWeight: 500 }}>{e.strategy}</td>
                        <td>{e.condition}</td>
                        <td>{e.price_zone}</td>
                        <td>
                          <span style={{
                            padding: '0.15rem 0.5rem',
                            borderRadius: '9999px',
                            fontSize: '0.75rem',
                            fontWeight: 600,
                            background: e.strength === 'Strong' ? '#dcfce7' : e.strength === 'Moderate' ? '#fef9c3' : '#f3f4f6',
                            color: e.strength === 'Strong' ? '#166534' : e.strength === 'Moderate' ? '#854d0e' : '#374151',
                          }}>{e.strength}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Targets & Stops */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
              {tradeSetup.targets.length > 0 && (
                <div>
                  <h3 style={{ fontSize: '0.95rem', fontWeight: 600, marginBottom: '0.5rem', color: '#16a34a' }}>Targets</h3>
                  <table className="data-table">
                    <thead>
                      <tr><th>Level</th><th>Price</th><th>Source</th></tr>
                    </thead>
                    <tbody>
                      {tradeSetup.targets.map((t, i) => (
                        <tr key={i}>
                          <td>{t.level}</td>
                          <td style={{ fontWeight: 500, color: '#16a34a' }}>${t.price.toFixed(2)}</td>
                          <td>{t.source}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {tradeSetup.stops.length > 0 && (
                <div>
                  <h3 style={{ fontSize: '0.95rem', fontWeight: 600, marginBottom: '0.5rem', color: '#dc2626' }}>Stop Levels</h3>
                  <table className="data-table">
                    <thead>
                      <tr><th>Level</th><th>Price</th><th>Source</th></tr>
                    </thead>
                    <tbody>
                      {tradeSetup.stops.map((s, i) => (
                        <tr key={i}>
                          <td>{s.level}</td>
                          <td style={{ fontWeight: 500, color: '#dc2626' }}>${s.price.toFixed(2)}</td>
                          <td>{s.source}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
              </>
            )}

            {evidenceTab === 'signals' && (
              <>
            {tradeSetup.signals.length > 0 && (
              <div style={{ marginBottom: '1rem' }}>
                <h3 style={{ fontSize: '0.95rem', fontWeight: 600, marginBottom: '0.5rem' }}>
                  Signal Reasons ({tradeSetup.signals.length})
                </h3>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem' }}>
                  {tradeSetup.signals.map((s, i) => {
                    const bullish = /bullish|golden|above|support|oversold|held|pullback|\+/i.test(s)
                    const bearish = /bearish|death|below|resistance|overbought|rejected/i.test(s)
                    const tone = bullish && !bearish ? POS : bearish && !bullish ? NEG : MUTED
                    return (
                      <span key={i} style={{
                        padding: '0.2rem 0.6rem',
                        background: '#fff',
                        border: `1px solid ${tone}`,
                        color: tone,
                        borderRadius: '0.3rem',
                        fontSize: '0.74rem',
                        fontWeight: 500,
                      }}>{s}</span>
                    )
                  })}
                </div>
              </div>
            )}

            {/* Strategy Results Summary */}
            <div style={{ padding: '0.75rem 1rem', background: '#f8fafc', borderRadius: '0.5rem', border: '1px solid #e2e8f0' }}>
              <strong style={{ fontSize: '0.875rem', color: 'var(--text-secondary)', display: 'block', marginBottom: '0.5rem' }}>Strategy Results</strong>
              <div className="ticker-strategy-results" style={{ fontSize: '0.8rem' }}>
                {tradeSetup.strategy_results.ma_crossover && (
                  <div style={{ padding: '0.5rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0' }}>
                    <div style={{ fontWeight: 600, marginBottom: '0.25rem' }}>MA Crossover</div>
                    <div>Signal: <strong>{tradeSetup.strategy_results.ma_crossover.signal}</strong></div>
                    {tradeSetup.strategy_results.ma_crossover.spread_pct !== null && (
                      <div>Spread: {tradeSetup.strategy_results.ma_crossover.spread_pct.toFixed(1)}%</div>
                    )}
                    {tradeSetup.strategy_results.ma_crossover.weekly_signal && (
                      <div>Weekly: {tradeSetup.strategy_results.ma_crossover.weekly_signal}</div>
                    )}
                  </div>
                )}
                {tradeSetup.strategy_results.momentum_pullback && (
                  <div style={{ padding: '0.5rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0' }}>
                    <div style={{ fontWeight: 600, marginBottom: '0.25rem' }}>Momentum Pullback</div>
                    <div>Grade: <strong>{tradeSetup.strategy_results.momentum_pullback.grade}</strong></div>
                    <div>Score: {tradeSetup.strategy_results.momentum_pullback.score}/100</div>
                  </div>
                )}
                {tradeSetup.strategy_results.bearish_bounce && (
                  <div style={{ padding: '0.5rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0' }}>
                    <div style={{ fontWeight: 600, marginBottom: '0.25rem' }}>Bearish Bounce</div>
                    <div>Grade: <strong>{tradeSetup.strategy_results.bearish_bounce.grade}</strong></div>
                    <div>Score: {tradeSetup.strategy_results.bearish_bounce.score}/100</div>
                  </div>
                )}
                {tradeSetup.strategy_results.gaps && (
                  <div style={{ padding: '0.5rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0' }}>
                    <div style={{ fontWeight: 600, marginBottom: '0.25rem' }}>Gap Levels</div>
                    <div>Support: {tradeSetup.strategy_results.gaps.support_count}</div>
                    <div>Resistance: {tradeSetup.strategy_results.gaps.resistance_count}</div>
                  </div>
                )}
                {tradeSetup.strategy_results.fvg && (
                  <div style={{ padding: '0.5rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0' }}>
                    <div style={{ fontWeight: 600, marginBottom: '0.25rem' }}>Fair Value Gaps</div>
                    <div>Bull (unmit.): {tradeSetup.strategy_results.fvg.bull_unmitigated}</div>
                    <div>Bear (unmit.): {tradeSetup.strategy_results.fvg.bear_unmitigated}</div>
                  </div>
                )}
                {tradeSetup.strategy_results.fibonacci && (
                  <div style={{ padding: '0.5rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.5rem', marginBottom: '0.25rem' }}>
                      <span style={{ fontWeight: 600 }}>Fibonacci</span>
                      <span style={{ color: 'var(--text-secondary)', fontSize: '0.65rem' }}>Context only · not scored</span>
                    </div>
                    <div>Signal: <strong>{tradeSetup.strategy_results.fibonacci.signal}</strong></div>
                    <div>
                      Detection: <strong>{tradeSetup.strategy_results.fibonacci.swing_detection_pct.toFixed(2)}% dynamic</strong>
                    </div>
                    <div>
                      Confirmed move: {tradeSetup.strategy_results.fibonacci.trend_direction === 'uptrend_retracement' ? (
                        <>Low ${tradeSetup.strategy_results.fibonacci.swing_low.toFixed(2)} ({tradeSetup.strategy_results.fibonacci.swing_low_date}) → High ${tradeSetup.strategy_results.fibonacci.swing_high.toFixed(2)} ({tradeSetup.strategy_results.fibonacci.swing_high_date})</>
                      ) : (
                        <>High ${tradeSetup.strategy_results.fibonacci.swing_high.toFixed(2)} ({tradeSetup.strategy_results.fibonacci.swing_high_date}) → Low ${tradeSetup.strategy_results.fibonacci.swing_low.toFixed(2)} ({tradeSetup.strategy_results.fibonacci.swing_low_date})</>
                      )}
                    </div>
                    <div>
                      Developing: {tradeSetup.strategy_results.fibonacci.developing_pivot.type === 'high' ? 'High' : 'Low'}
                      {' '}${tradeSetup.strategy_results.fibonacci.developing_pivot.price.toFixed(2)} ({tradeSetup.strategy_results.fibonacci.developing_pivot.date})
                      {' '}· {tradeSetup.strategy_results.fibonacci.developing_pivot.move_pct_from_confirmed.toFixed(2)}% from confirmed pivot
                    </div>
                    <div>
                      Size: {tradeSetup.strategy_results.fibonacci.swing_size_pct.toFixed(2)}%
                      {' '}· Retraced: {tradeSetup.strategy_results.fibonacci.retracement_pct.toFixed(2)}%
                    </div>
                    <div>
                      Nearest: {tradeSetup.strategy_results.fibonacci.nearest_level} at ${tradeSetup.strategy_results.fibonacci.nearest_level_price.toFixed(2)}
                      {' '}({tradeSetup.strategy_results.fibonacci.distance_pct > 0 ? '+' : ''}{tradeSetup.strategy_results.fibonacci.distance_pct.toFixed(2)}%)
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '0.1rem 0.5rem', marginTop: '0.3rem', color: 'var(--text-secondary)' }}>
                      {tradeSetup.strategy_results.fibonacci.levels.map((level) => (
                        <span key={level.name}>{level.name}: ${level.price.toFixed(2)}</span>
                      ))}
                    </div>
                    {tradeSetup.strategy_results.fibonacci.active_leg && (
                      <>
                        <div style={{ fontWeight: 600, marginTop: '0.45rem' }}>
                          Active provisional {tradeSetup.strategy_results.fibonacci.active_leg.level_role === 'provisional_support' ? 'supports' : 'resistances'}
                        </div>
                        <div>
                          {tradeSetup.strategy_results.fibonacci.active_leg.start.type === 'high' ? 'High' : 'Low'} ${tradeSetup.strategy_results.fibonacci.active_leg.start.price.toFixed(2)} ({tradeSetup.strategy_results.fibonacci.active_leg.start.date})
                          {' '}→ {tradeSetup.strategy_results.fibonacci.active_leg.end.type === 'high' ? 'High' : 'Low'} ${tradeSetup.strategy_results.fibonacci.active_leg.end.price.toFixed(2)} ({tradeSetup.strategy_results.fibonacci.active_leg.end.date})
                          {' '}· retraced {tradeSetup.strategy_results.fibonacci.active_leg.retracement_pct.toFixed(2)}%
                        </div>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '0.1rem 0.5rem', marginTop: '0.3rem', color: 'var(--text-secondary)' }}>
                          {tradeSetup.strategy_results.fibonacci.active_leg.levels.map((level) => (
                            <span key={level.name}>{level.name}: ${level.price.toFixed(2)}</span>
                          ))}
                        </div>
                        <div style={{ color: 'var(--text-secondary)', marginTop: '0.3rem' }}>
                          Confirms when price moves {tradeSetup.strategy_results.fibonacci.active_leg.confirmation.condition === 'at_or_below' ? 'to or below' : 'to or above'} ${tradeSetup.strategy_results.fibonacci.active_leg.confirmation.price.toFixed(2)}.
                        </div>
                      </>
                    )}
                  </div>
                )}
              </div>
            </div>
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
            )}
              </>
            )}

            {evidenceTab === 'model' && (
              <>
            <div>

              {/* Layer 1: Pattern Scores */}
              {patternScores && !calibrationLoading && (
                <div style={panelStyle}>
                  <h3 style={panelHeadStyle}>
                    Layer 1 · Opening pattern scores · 9:25 AM
                  </h3>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: '0.5rem' }}>
                    {([
                      { key: 'breakout' as PatternKey, name: 'Breakout' },
                      { key: 'vwap' as PatternKey, name: 'VWAP' },
                      { key: 'volatility' as PatternKey, name: 'Volatility' },
                      { key: 'trend' as PatternKey, name: 'Trend' },
                      { key: 'rs' as PatternKey, name: 'RS' },
                      { key: 'calendar' as PatternKey, name: 'Calendar' },
                    ]).map(({ key, name }) => {
                      const val = patternScores.patterns[key] ?? 0
                      const fired = patternScores.fired[key]
                      const tone = val >= 60 ? POS : val >= 40 ? WARN : NEG
                      return (
                        <div key={key} style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: `1px solid ${fired ? POS : '#e2e8f0'}`, textAlign: 'center' }}>
                          <div style={{ fontSize: '1.25rem', fontWeight: 700, color: tone }}>{val}</div>
                          <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
                            {name}{fired ? ' \u25cf' : ''}
                          </div>
                          <div style={{ marginTop: '0.35rem', height: '3px', background: '#e5e7eb', borderRadius: '9999px', overflow: 'hidden' }}>
                            <div style={{ height: '100%', background: tone, width: `${Math.min(val, 100)}%` }} />
                          </div>
                        </div>
                      )
                    })}
                  </div>
                  <div style={{ marginTop: '0.6rem', display: 'flex', flexWrap: 'wrap', gap: '0.75rem', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                    <span>{patternScores.trade_date}</span>
                    <span>{patternScores.fired_count}/6 fired (score &gt; 60)</span>
                    {patternScores.primary_regime && <span>Market: {patternScores.primary_regime}</span>}
                    {patternScores.sector_regime && <span>Sector: {patternScores.sector_regime}</span>}
                    {patternScores.market_breadth_score !== null && <span>Breadth: {patternScores.market_breadth_score}</span>}
                  </div>
                </div>
              )}

              {/* Layer 3: Analog Matches */}
              {analogMatches && !calibrationLoading && (
                <div style={panelStyle}>
                  <h3 style={panelHeadStyle}>
                    Layer 3 · Analog pattern matching · 9:30 AM
                  </h3>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem', marginBottom: '0.75rem' }}>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', textAlign: 'center' }}>
                      <div style={{ fontSize: '1.25rem', fontWeight: 700, color: analogMatches.analog_accuracy >= 55 ? POS : analogMatches.analog_accuracy >= 45 ? WARN : NEG }}>{analogMatches.analog_accuracy.toFixed(1)}%</div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>Analog Accuracy</div>
                    </div>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', textAlign: 'center' }}>
                      <div style={{ fontSize: '1.25rem', fontWeight: 700, color: analogMatches.confidence_boost >= 0 ? POS : NEG }}>
                        {analogMatches.confidence_boost > 0 ? '+' : ''}{analogMatches.confidence_boost}
                      </div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>Confidence Boost</div>
                    </div>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', textAlign: 'center' }}>
                      <div style={{ fontSize: '1.25rem', fontWeight: 700, color: INK }}>{analogMatches.analog_count}</div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
                        Similar Days{analogMatches.sector_regime ? ` \u00b7 ${analogMatches.sector_regime}` : ''}
                      </div>
                    </div>
                  </div>
                  {analogMatches.similar_days.length > 0 && (
                    <table className="data-table" style={{ fontSize: '0.8rem' }}>
                      <thead>
                        <tr><th>Analog Date</th><th>Distance</th><th>Return</th><th>Hit</th></tr>
                      </thead>
                      <tbody>
                        {analogMatches.similar_days.map((d, i) => (
                          <tr key={`${d.date}-${i}`}>
                            <td>{d.date}</td>
                            <td style={{ textAlign: 'center' }}>{d.distance}</td>
                            <td style={{ textAlign: 'center', color: d.actual_return >= 0 ? '#16a34a' : '#dc2626' }}>
                              {d.actual_return > 0 ? '+' : ''}{d.actual_return}%
                            </td>
                            <td style={{ textAlign: 'center', color: d.hit ? '#16a34a' : '#dc2626', fontWeight: 600 }}>
                              {d.hit ? '\u2713' : '\u2717'}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              )}

              {/* Layer 2: Pattern Priors */}
              {Object.keys(patternPriors).length > 0 && (
                <div style={panelStyle}>
                  <h3 style={panelHeadStyle}>
                    Layer 2 · Historical win-rate priors · Friday EOD
                  </h3>
                  <table className="data-table" style={{ fontSize: '0.85rem' }}>
                    <thead>
                      <tr>
                        <th>Pattern</th>
                        <th>Win Rate</th>
                        <th>Sample Size</th>
                        <th>Confidence Multiplier</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(patternPriors).map(([name, data]: [string, any]) => (
                        <tr key={name}>
                          <td style={{ fontWeight: 500 }}>{name}</td>
                          <td style={{ textAlign: 'center', color: data.win_rate >= 55 ? '#059669' : data.win_rate >= 45 ? '#d97706' : '#dc2626' }}>
                            {data.win_rate.toFixed(1)}%
                          </td>
                          <td style={{ textAlign: 'center' }}>{data.sample_size}</td>                          <td style={{ textAlign: 'center', fontWeight: 600, color: data.confidence_multiplier > 1 ? '#059669' : data.confidence_multiplier < 1 ? '#dc2626' : 'inherit' }}>
                            {data.confidence_multiplier.toFixed(2)}x
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Layer 4: Current Recommendation */}
              {tickerRec && !calibrationLoading && (
                <div style={panelStyle}>
                  <h3 style={panelHeadStyle}>
                    Layer 4 · Daily recommendation · {tickerRec.trade_date}
                  </h3>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '0.5rem', marginBottom: '0.75rem' }}>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', textAlign: 'center' }}>
                      <div style={{ fontSize: '1.1rem', fontWeight: 700, color: tickerRec.direction === 'BULL' ? POS : NEG }}>
                        {tickerRec.direction}
                      </div>
                      <div style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>Direction</div>
                    </div>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', textAlign: 'center' }}>
                      <div style={{ fontSize: '1.1rem', fontWeight: 700, color: INK }}>{tickerRec.predicted_confidence}%</div>
                      <div style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>Confidence</div>
                    </div>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', textAlign: 'center' }}>
                      <div style={{ fontSize: '1.1rem', fontWeight: 700, color: gradeTone(tickerRec.signal_grade) }}>{tickerRec.signal_grade || '—'}</div>
                      <div style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>Signal Grade</div>
                    </div>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', textAlign: 'center' }}>
                      <div style={{ fontSize: '1.1rem', fontWeight: 700, color: INK }}>{tickerRec.rank === null ? '—' : `#${tickerRec.rank}`}</div>
                      <div style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>Rank in Category</div>
                    </div>
                  </div>
                  <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', marginBottom: '0.75rem' }}>
                    <div style={{ ...labelStyle, marginBottom: '0.4rem' }}>Pattern scores at signal time</div>
                    {Object.values(tickerRec.pattern_scores).some(v => v > 0) ? (
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: '0.35rem' }}>
                        {([
                          { name: 'Breakout', key: 'breakout' as PatternKey },
                          { name: 'VWAP', key: 'vwap' as PatternKey },
                          { name: 'Volatility', key: 'volatility' as PatternKey },
                          { name: 'Trend', key: 'trend' as PatternKey },
                          { name: 'RS', key: 'rs' as PatternKey },
                          { name: 'Calendar', key: 'calendar' as PatternKey },
                        ]).map(s => {
                          const val = tickerRec.pattern_scores[s.key] ?? 0
                          return (
                            <div key={s.key} style={{ textAlign: 'center', fontSize: '0.72rem' }}>
                              <div style={{ fontWeight: 700, color: val >= 60 ? POS : val >= 40 ? WARN : NEG }}>{val}</div>
                              <div style={{ color: 'var(--text-secondary)' }}>{s.name}</div>
                            </div>
                          )
                        })}
                      </div>
                    ) : (
                      <div style={{ fontSize: '0.75rem', color: MUTED }}>
                        Not recorded — this signal was generated without opening pattern scoring.
                      </div>
                    )}
                  </div>
                  <div style={{ padding: '0.6rem', background: '#f8fafc', borderRadius: '0.375rem', border: '1px solid #e2e8f0', fontSize: '0.8rem' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                      <span style={{ color: MUTED }}>Calibration:</span>
                      <strong>{tickerRec.calibration.confidence_before ?? '—'}</strong>
                      <span style={{ color: MUTED }}>→</span>
                      <strong style={{
                        color: (tickerRec.calibration.confidence_after ?? 0) >= (tickerRec.calibration.confidence_before ?? 0) ? POS : NEG,
                      }}>
                        {tickerRec.calibration.confidence_after ?? '—'}
                      </strong>
                      {tickerRec.calibration.confidence_before !== null && tickerRec.calibration.confidence_after !== null && (
                        <span style={{ color: MUTED, fontSize: '0.75rem' }}>
                          ({tickerRec.calibration.confidence_after - tickerRec.calibration.confidence_before > 0 ? '+' : ''}
                          {tickerRec.calibration.confidence_after - tickerRec.calibration.confidence_before})
                        </span>
                      )}
                    </div>
                    <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                      Applied: {tickerRec.calibration.pattern_priors_applied ? '✓ Priors' : '— Priors'}
                      {' · '}{tickerRec.calibration.analog_matching_applied ? '✓ Analogs' : '— Analogs'}
                      {tickerRec.calibration.sources ? ` · ${tickerRec.calibration.sources}` : ''}
                    </div>
                  </div>
                </div>
              )}

              {/* Layer 5: Historical Performance */}
              {tickerPerf && !calibrationLoading && (
                <div style={panelStyle}>
                  <h3 style={panelHeadStyle}>
                    Layer 5 · Realised performance · last {tickerPerf.period_days} days
                  </h3>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '0.5rem', marginBottom: '0.75rem' }}>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0', textAlign: 'center' }}>
                      <div style={{ fontSize: '1.25rem', fontWeight: 700, color: tickerPerf.total_recs === 0 ? MUTED : tickerPerf.win_rate >= 55 ? POS : tickerPerf.win_rate >= 45 ? WARN : NEG }}>
                        {tickerPerf.win_rate.toFixed(1)}%
                      </div>
                      <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
                        Overall Win Rate ({tickerPerf.correct_recs}/{tickerPerf.total_recs})
                      </div>
                    </div>
                    <div style={{ padding: '0.6rem', background: '#fff', borderRadius: '0.375rem', border: '1px solid #e2e8f0' }}>
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem', textAlign: 'center' }}>
                        <div>
                          <div style={{ fontSize: '1rem', fontWeight: 700, color: POS }}>{tickerPerf.bull_stats.win_rate.toFixed(1)}%</div>
                          <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>BULL ({tickerPerf.bull_stats.count})</div>
                        </div>
                        <div>
                          <div style={{ fontSize: '1rem', fontWeight: 700, color: '#dc2626' }}>{tickerPerf.bear_stats.win_rate.toFixed(1)}%</div>
                          <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>BEAR ({tickerPerf.bear_stats.count})</div>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div style={{ padding: '0.75rem', background: '#fafafa', borderRadius: '0.375rem', border: '1px solid #e5e7eb', fontSize: '0.85rem' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.25rem' }}>
                      <span>Avg return in call direction:</span>
                      <strong style={{ color: (tickerPerf.returns.avg_return_directional ?? 0) >= 0 ? '#059669' : '#dc2626' }}>
                        {tickerPerf.returns.avg_return_directional === null ? '—'
                          : `${tickerPerf.returns.avg_return_directional > 0 ? '+' : ''}${tickerPerf.returns.avg_return_directional.toFixed(2)}%`}
                      </strong>
                    </div>
                    {baseline && (
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                        <span>System-wide ({baseline.total_recs} calls, {baseline.ticker_count} tickers):</span>
                        <strong style={{ color: (baseline.avg_return_directional ?? 0) >= 0 ? '#059669' : '#dc2626' }}>
                          {plainPct(baseline.win_rate)} win rate
                          {baseline.avg_return_directional !== null
                            ? ` · ${baseline.avg_return_directional > 0 ? '+' : ''}${baseline.avg_return_directional.toFixed(2)}%`
                            : ''}
                        </strong>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {!calibrationLoading && (() => {
                const missing = [
                  !patternScores && 'opening pattern scores',
                  Object.keys(patternPriors).length === 0 && 'win-rate priors',
                  !analogMatches && 'analog matches',
                  !tickerRec && 'daily recommendation',
                ].filter(Boolean) as string[]
                if (missing.length === 0) return null
                return (
                  <p style={{ color: 'var(--text-secondary)', fontSize: '0.78rem', margin: 0 }}>
                    Not recorded for {symbol} on this date: {missing.join(', ')}.
                    {tickerRec ? ' Confidence above is uncalibrated.' : ' Layers populate at 9:25, 9:30 and 9:35 AM ET.'}
                  </p>
                )
              })()}
            </div>
              </>
            )}
          </>
        )}
      </div>
    </div>
  )
}

export default TickerDetail
