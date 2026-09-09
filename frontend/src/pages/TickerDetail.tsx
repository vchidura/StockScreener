import { useState, useEffect, useRef, useCallback, useMemo, Fragment } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Eraser, ScanLine, TrendingUp, X } from 'lucide-react'
import {
  createChart,
  IChartApi,
  ISeriesApi,
  CandlestickData,
  HistogramData,
  Time,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  LineType,
  TickMarkType,
} from 'lightweight-charts'
import { 
  getChartData,
  getChartPatterns,
  getPriceChannel,
  scanTickerChartPatterns,
  getLatestQuote, 
  getMultiTradeSetup,
  ChartDataPoint,
  FormingChartPattern,
  MultiTradeSetupResponse,
  getTickerDiscoveryState,
  TickerDiscoveryResponse,
  getTickerScannerEvents,
  ScannerEventRow,
} from '../services/api'
import { formingPatternRead } from '../utils/formingPatterns'

import {
  CHART_INTERVALS,
  INFO,
  INFO_SOFT,
  INK,
  INTERVAL_NOUN,
  LABEL,
  LINE,
  MAX_DISCOVERY_AGE_DAYS,
  MARKET_TIME_ZONE,
  MUTED,
  NEG,
  NEG_SOFT,
  PANEL,
  PATTERN_INTERVAL_LABEL,
  PATTERN_INTERVAL_ORDER,
  POS,
  POS_SOFT,
  Pill,
  SESSION_BARS,
  SETUP_INTERVALS,
  SURFACE,
  type SetupTimeframe,
  StructureRead,
  Tile,
  VolatilityTrack,
  VolumeSparkline,
  WARN,
  WARN_SOFT,
  DirectionStrengthTrack,
  buildBollingerBands,
  buildEmaSeries,
  buildRsiSeries,
  buildSmaSeries,
  crossFrameReading,
  defaultPeriodForInterval,
  formatChartTick,
  formatChartTime,
  formatScannerEventTime,
  getVisibleChartData,
  gradeTone,
  money,
  parsePatternChoice,
  patternChoiceValue,
  plainPct,
  priceChannelReading,
  relativeAge,
  resolveAnalysisInterval,
  sideOfBias,
  signedPct,
  trendTone,
} from './ticker/tickerShared'
import { NO_TREND_ADX, trendLabel } from './setupPresentation'
import { buildTradePlan, evaluatePlan } from './ticker/tickerPlan'
import PriceLadder, { type LadderBadge, type LadderRow } from './ticker/PriceLadder'
import { resolveChartTheme, useChartTheme } from './ticker/chartTheme'

export type TickerView = 'overview' | 'timeframes' | 'levels' | 'fibonacci' | 'scanner' | 'financials'

const DEVELOPING_CANDLE_INTERVALS = new Set(['15m', '30m', '1h', '1d'])

function formatDevelopingThrough(unixSeconds: number): string {
  return new Intl.DateTimeFormat(undefined, {
    timeZone: MARKET_TIME_ZONE,
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'short',
  }).format(new Date(unixSeconds * 1000))
}

function TickerDetail({ view }: { view: TickerView }) {
  const { symbol } = useParams<{ symbol: string }>()
  const [searchParams] = useSearchParams()
  const requestedInterval = searchParams.get('interval')
  const initialInterval = requestedInterval && (CHART_INTERVALS as readonly string[]).includes(requestedInterval)
    ? requestedInterval : '1d'
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
  const patternSeriesRef = useRef<ISeriesApi<'Line'>[]>([])
  const channelSeriesRef = useRef<ISeriesApi<'Line'>[]>([])
  const rsiPaneIndexRef = useRef<number | null>(null)
  const legendRef = useRef<HTMLDivElement>(null)
  const chartTheme = useChartTheme()
  const chartThemeRef = useRef(chartTheme)

  const queryClient = useQueryClient()
  const [period, setPeriod] = useState(() => defaultPeriodForInterval(initialInterval))
  const [interval, setInterval] = useState(initialInterval)
  // null means the analysis follows the chart interval.
  const [analysisPin, setAnalysisPin] = useState<string | null>(null)
  const [chartHeight, setChartHeight] = useState(450)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [showRsi, setShowRsi] = useState(false)
  const [showAutoPatterns, setShowAutoPatterns] = useState(() => searchParams.get('patterns') === 'on')
  const [showPriceChannel, setShowPriceChannel] = useState(() => searchParams.get('channel') === 'on' && initialInterval !== '1m')
  const [patternSelection, setPatternSelection] = useState(() => {
    const requestedPattern = searchParams.get('pattern')
    return requestedPattern ? patternChoiceValue(initialInterval, requestedPattern) : 'best'
  })
  const [expandedEvents, setExpandedEvents] = useState<Set<number>>(new Set())
  const [chartOverrides, setChartOverrides] = useState<Partial<Record<TickerView, boolean>>>({})
  const [selectedLevel, setSelectedLevel] = useState<{ key: string; price: number; label: string } | null>(null)

  const chartLayout = view === 'overview'
    ? 'full'
    : view === 'levels' || view === 'fibonacci' ? 'split' : 'collapsed'
  const chartCollapsed = chartOverrides[view] ?? (chartLayout === 'collapsed')
  const effectiveChartHeight = chartLayout === 'split' ? Math.min(chartHeight, 380) : chartHeight

  const handleLevelSelect = (row: LadderRow) => {
    setSelectedLevel(current => current?.key === row.key
      ? null
      : { key: row.key, price: row.price, label: row.title })
    if (chartCollapsed) setChartOverrides(current => ({ ...current, [view]: false }))
  }

  // Track previous interval to avoid showing stale data across interval switches
  const prevIntervalRef = useRef(interval)
  const intervalRef = useRef(interval)
  const periodRef = useRef(period)
  const patternScopeRef = useRef(`${symbol}-${interval}`)
  const pendingPatternSelectionRef = useRef<{ scope: string; selection: string } | null>(null)
  const chartRequestPeriod = interval === '1h' ? '2y' : interval === '1wk' ? '5y' : period

  const {
    data: chartData = [],
    isFetching: loading,
    isError: chartError,
  } = useQuery<ChartDataPoint[]>({
    queryKey: ['chart', symbol, chartRequestPeriod, interval],
    queryFn: () => getChartData(symbol!, chartRequestPeriod, interval),
    enabled: !!symbol,
    placeholderData: (prev) => prevIntervalRef.current === interval ? prev : undefined,
    refetchInterval: DEVELOPING_CANDLE_INTERVALS.has(interval) ? 60_000 : false,
  })
  const visibleChartData = getVisibleChartData(chartData, period, interval)
  const finalizedChartData = useMemo(
    () => chartData.filter(point => !point.provisional),
    [chartData],
  )

  const { data: chartPatterns = null, isFetching: patternLoading } = useQuery({
    queryKey: ['chart-patterns', symbol, interval],
    queryFn: () => getChartPatterns(symbol!, interval),
    enabled: !!symbol && showAutoPatterns,
    staleTime: interval in SESSION_BARS ? 60_000 : 300_000,
  })
  const { data: crossFramePatterns = null, isFetching: crossFrameLoading } = useQuery({
    queryKey: ['chart-patterns', symbol, 'all'],
    queryFn: () => scanTickerChartPatterns(symbol!),
    enabled: !!symbol && showAutoPatterns,
    staleTime: 60_000,
    refetchInterval: 60_000,
  })
  const { data: priceChannelResponse = null, isFetching: channelLoading } = useQuery({
    queryKey: ['price-channel', symbol, interval],
    queryFn: () => getPriceChannel(symbol!, interval),
    enabled: !!symbol && showPriceChannel && interval !== '1m',
    staleTime: interval in SESSION_BARS ? 60_000 : 300_000,
  })
  const priceChannel = priceChannelResponse?.channel ?? null
  const availablePatterns = chartPatterns?.patterns ?? []
  const displayedPatterns = useMemo(() => {
    if (patternSelection === 'all') return availablePatterns
    if (patternSelection === 'best') return availablePatterns.slice(0, 1)
    const choice = parsePatternChoice(patternSelection)
    const selected = choice?.interval === interval
      ? availablePatterns.find(pattern => pattern.type === choice.type)
      : undefined
    return selected
      ? [selected]
      : availablePatterns.slice(0, 1)
  }, [availablePatterns, interval, patternSelection])
  const patternGroups = useMemo(() => {
    const grouped = new Map<string, FormingChartPattern[]>()
    for (const row of crossFramePatterns?.results ?? []) {
      if (row.interval !== interval) {
        grouped.set(row.interval, [...(grouped.get(row.interval) ?? []), row.pattern])
      }
    }
    if (availablePatterns.length > 0) grouped.set(interval, availablePatterns)
    const orderedIntervals = [interval, ...PATTERN_INTERVAL_ORDER.filter(value => value !== interval)]
    return orderedIntervals
      .filter(value => grouped.has(value))
      .map(value => {
        const patterns = grouped.get(value) ?? []
        const frame = crossFramePatterns?.cross_frame?.frames.find(item => item.interval === value)
        return {
          interval: value,
          patterns,
          bias: frame?.bias,
          primaryType: frame?.primary_pattern_type ?? patterns[0]?.type,
        }
      })
  }, [availablePatterns, crossFramePatterns?.cross_frame?.frames, crossFramePatterns?.results, interval])
  const crossFrameView = crossFrameReading(crossFramePatterns?.cross_frame)

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
  const setups = multiSetup?.setups ?? {}
  const selectableSetupIntervals: readonly string[] = setups['30m']
    ? [...SETUP_INTERVALS, '30m']
    : SETUP_INTERVALS
  // Resolved before the setup lookup so the pin and the chart cannot disagree about which frame is read.
  const analysis = analysisPin
    ? { interval: analysisPin, snapped: false }
    : resolveAnalysisInterval(interval, selectableSetupIntervals)
  const setupInterval = analysis.interval
  const tradeSetup = multiSetup?.setups[setupInterval as SetupTimeframe] ?? null
  const tradeSetupError = multiSetup?.errors[setupInterval as SetupTimeframe] ?? null
  const setupTimeframes: SetupTimeframe[] = setups['30m']
    ? ['1mo', '1wk', '1d', '1h', '30m']
    : ['1mo', '1wk', '1d', '1h']
  const fibonacciTimeframes = setupTimeframes.filter(timeframe => timeframe !== '1mo')
  const confluenceZones = multiSetup?.confluence_zones ?? []
  const analysisLabel = INTERVAL_NOUN[setupInterval] ?? PATTERN_INTERVAL_LABEL[setupInterval] ?? setupInterval

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
  useEffect(() => {
    if (interval === '1m') setShowPriceChannel(false)
  }, [interval])
  useEffect(() => {
    const scope = `${symbol}-${interval}`
    if (scope !== patternScopeRef.current) {
      patternScopeRef.current = scope
      const pending = pendingPatternSelectionRef.current
      setPatternSelection(pending?.scope === scope ? pending.selection : 'best')
      pendingPatternSelectionRef.current = null
    }
  }, [symbol, interval])

  const clearPatternSeries = useCallback(() => {
    const chart = chartRef.current
    if (chart) {
      for (const series of patternSeriesRef.current) {
        try {
          chart.removeSeries(series)
        } catch {
          // The chart may already be disposed during route teardown.
        }
      }
    }
    patternSeriesRef.current = []
  }, [])

  const clearChannelSeries = useCallback(() => {
    const chart = chartRef.current
    if (chart) {
      for (const series of channelSeriesRef.current) {
        try {
          chart.removeSeries(series)
        } catch {
          // The chart may already be disposed during route teardown.
        }
      }
    }
    channelSeriesRef.current = []
  }, [])

  const clearResearchOverlays = useCallback(() => {
    clearPatternSeries()
    clearChannelSeries()
    setShowAutoPatterns(false)
    setShowPriceChannel(false)
    setPatternSelection('best')
    queryClient.removeQueries({ queryKey: ['chart-patterns', symbol] })
    queryClient.removeQueries({ queryKey: ['price-channel', symbol] })
  }, [clearChannelSeries, clearPatternSeries, queryClient, symbol])

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
    const patternKey = ['chart-patterns', symbol, interval]
    const crossPatternKey = ['chart-patterns', symbol, 'all']
    const channelKey = ['price-channel', symbol, interval]
    queryClient.setQueryData(chartKey, undefined)
    queryClient.setQueryData(setupKey, undefined)
    queryClient.setQueryData(quoteKey, undefined)
    queryClient.removeQueries({ queryKey: patternKey, exact: true })
    queryClient.removeQueries({ queryKey: crossPatternKey, exact: true })
    queryClient.removeQueries({ queryKey: channelKey, exact: true })
    const requests: Promise<unknown>[] = [
      queryClient.fetchQuery({ queryKey: chartKey, queryFn: () => getChartData(symbol!, chartRequestPeriod, interval, true) }),
      queryClient.fetchQuery({ queryKey: setupKey, queryFn: () => getMultiTradeSetup(symbol!, true) }),
      queryClient.fetchQuery({ queryKey: quoteKey, queryFn: () => getLatestQuote(symbol!, true) }),
    ]
    await Promise.all(requests)
    if (showAutoPatterns) {
      await queryClient.fetchQuery({
        queryKey: crossPatternKey,
        queryFn: () => scanTickerChartPatterns(symbol!, true),
      })
      await queryClient.fetchQuery({
        queryKey: patternKey,
        queryFn: () => getChartPatterns(symbol!, interval),
      })
    }
    if (showPriceChannel && interval !== '1m') {
      await queryClient.fetchQuery({
        queryKey: channelKey,
        queryFn: () => getPriceChannel(symbol!, interval, true),
      })
    }
  }, [symbol, chartRequestPeriod, interval, queryClient, showAutoPatterns, showPriceChannel])

  const handleChartIntervalChange = useCallback((nextInterval: string, nextPattern?: string) => {
    if (nextPattern) {
      pendingPatternSelectionRef.current = {
        scope: `${symbol}-${nextInterval}`,
        selection: nextPattern,
      }
      setPatternSelection(nextPattern)
    } else {
      pendingPatternSelectionRef.current = null
    }
    setInterval(nextInterval)
    setPeriod(defaultPeriodForInterval(nextInterval))
  }, [symbol])

  const handlePatternSelectionChange = useCallback((selection: string) => {
    const choice = parsePatternChoice(selection)
    if (choice && choice.interval !== interval) {
      handleChartIntervalChange(choice.interval, selection)
      return
    }
    setPatternSelection(selection)
  }, [handleChartIntervalChange, interval])

  // Effect 1: Create chart once on mount
  useEffect(() => {
    if (!chartContainerRef.current) return
    const theme = resolveChartTheme()

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { color: theme.background },
        textColor: theme.text,
      },
      localization: {
        timeFormatter: (time: Time) => formatChartTime(time, intervalRef.current),
      },
      grid: {
        vertLines: { color: theme.grid },
        horzLines: { color: theme.grid },
      },
      crosshair: {
        mode: 1,
      },
      rightPriceScale: {
        borderColor: theme.border,
      },
      timeScale: {
        borderColor: theme.border,
        timeVisible: true,
        secondsVisible: false,
        tickMarkFormatter: (time: Time, type: TickMarkType) => formatChartTick(time, type, intervalRef.current, periodRef.current),
      },
      width: chartContainerRef.current.clientWidth,
      height: 450,
    })

    chartRef.current = chart

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: theme.up,
      downColor: theme.down,
      borderDownColor: theme.down,
      borderUpColor: theme.up,
      wickDownColor: theme.down,
      wickUpColor: theme.up,
    })
    candleSeriesRef.current = candleSeries

    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: theme.bb,
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    })
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    })
    volumeSeriesRef.current = volumeSeries

    ma50SeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.ma['MA 50'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    ma100SeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.ma['MA 100'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    ma200SeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.ma['MA 200'], lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
    })
    ema8SeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.ma['EMA 8'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    ema21SeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.ma['EMA 21'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    ema50SeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.ma['EMA 50'], lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
    })
    bbMiddleSeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.bb, lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false,
    })
    bbUpperSeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.bb, lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false,
    })
    bbLowerSeriesRef.current = chart.addSeries(LineSeries, {
      color: theme.bb, lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false,
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
      const active = chartThemeRef.current
      const seriesMap = [
        { series: candleSeriesRef.current!, label: 'OHLC', color: active.ink, isCandle: true },
        { series: ma50SeriesRef.current!, label: 'MA 50', color: active.ma['MA 50'], isCandle: false },
        { series: ma100SeriesRef.current!, label: 'MA 100', color: active.ma['MA 100'], isCandle: false },
        { series: ma200SeriesRef.current!, label: 'MA 200', color: active.ma['MA 200'], isCandle: false },
        { series: ema8SeriesRef.current!, label: 'EMA 8', color: active.ma['EMA 8'], isCandle: false },
        { series: ema21SeriesRef.current!, label: 'EMA 21', color: active.ma['EMA 21'], isCandle: false },
        { series: ema50SeriesRef.current!, label: 'EMA 50', color: active.ma['EMA 50'], isCandle: false },
        { series: bbMiddleSeriesRef.current!, label: 'BB Mid', color: active.bb, isCandle: false },
        { series: bbUpperSeriesRef.current!, label: 'BB Up', color: active.bb, isCandle: false },
        { series: bbLowerSeriesRef.current!, label: 'BB Lo', color: active.bb, isCandle: false },
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
        line1 += `<span style="color:${active.muted}">Vol: ${(vol.value / 1e6).toFixed(2)}M</span>`
      }
      el.innerHTML = line1 + '<br/>' + line2
    })

    // Collapsing the rail or switching to the split view resizes the container without any
    // window resize, and applyOptions({ width }) does not repaint, so resize() is used instead.
    const resizeObserver = new ResizeObserver(() => {
      const element = chartContainerRef.current
      if (!chartRef.current || !element) return
      const { width, height } = element.getBoundingClientRect()
      if (width > 0 && height > 0) chartRef.current.resize(width, height)
    })
    resizeObserver.observe(chartContainerRef.current)

    return () => {
      resizeObserver.disconnect()
      patternSeriesRef.current = []
      channelSeriesRef.current = []
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Chart colors cannot reference CSS variables, so re-apply them whenever the theme changes.
  useEffect(() => {
    chartThemeRef.current = chartTheme
    const chart = chartRef.current
    if (!chart) return
    chart.applyOptions({
      layout: { background: { color: chartTheme.background }, textColor: chartTheme.text },
      grid: { vertLines: { color: chartTheme.grid }, horzLines: { color: chartTheme.grid } },
      rightPriceScale: { borderColor: chartTheme.border },
      timeScale: { borderColor: chartTheme.border },
    })
    candleSeriesRef.current?.applyOptions({
      upColor: chartTheme.up,
      downColor: chartTheme.down,
      borderUpColor: chartTheme.up,
      borderDownColor: chartTheme.down,
      wickUpColor: chartTheme.up,
      wickDownColor: chartTheme.down,
    })
    volumeSeriesRef.current?.applyOptions({ color: chartTheme.bb })
    ma50SeriesRef.current?.applyOptions({ color: chartTheme.ma['MA 50'] })
    ma100SeriesRef.current?.applyOptions({ color: chartTheme.ma['MA 100'] })
    ma200SeriesRef.current?.applyOptions({ color: chartTheme.ma['MA 200'] })
    ema8SeriesRef.current?.applyOptions({ color: chartTheme.ma['EMA 8'] })
    ema21SeriesRef.current?.applyOptions({ color: chartTheme.ma['EMA 21'] })
    ema50SeriesRef.current?.applyOptions({ color: chartTheme.ma['EMA 50'] })
    bbMiddleSeriesRef.current?.applyOptions({ color: chartTheme.bb })
    bbUpperSeriesRef.current?.applyOptions({ color: chartTheme.bb })
    bbLowerSeriesRef.current?.applyOptions({ color: chartTheme.bb })
  }, [chartTheme])

  // Effect: refit the visible range when the chart box changes
  useEffect(() => {
    if (chartRef.current) fitSelectedPeriod()
  }, [effectiveChartHeight, isFullscreen, fitSelectedPeriod])

  useEffect(() => setSelectedLevel(null), [symbol, view, setupInterval])

  // Levels are prices, so the chart draws them natively rather than as an overlay series.
  useEffect(() => {
    const series = candleSeriesRef.current
    if (!series || !selectedLevel) return
    const priceLine = series.createPriceLine({
      price: selectedLevel.price,
      color: chartTheme.accent,
      lineWidth: 1,
      lineStyle: 2,
      axisLabelVisible: true,
      title: selectedLevel.label,
    })
    return () => {
      try {
        series.removePriceLine(priceLine)
      } catch {
        // The chart may already be disposed during route teardown.
      }
    }
  }, [selectedLevel, chartTheme])

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
      time: d.time as Time,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
      ...(d.provisional ? {
        color: WARN,
        borderColor: WARN,
        wickColor: WARN,
      } : {}),
    }))
    const volumeData: HistogramData<Time>[] = chartData.map(d => ({
      time: d.time as Time,
      value: d.volume,
      color: d.provisional
        ? WARN
        : d.close >= d.open ? chartTheme.upFill : chartTheme.downFill,
    }))

    candleSeriesRef.current.setData(candleData)
    volumeSeriesRef.current?.setData(volumeData)
    ma50SeriesRef.current?.setData(buildSmaSeries(finalizedChartData, 50))
    ma100SeriesRef.current?.setData(buildSmaSeries(finalizedChartData, 100))
    ma200SeriesRef.current?.setData(buildSmaSeries(finalizedChartData, 200))
    ema8SeriesRef.current?.setData(buildEmaSeries(finalizedChartData, 8))
    ema21SeriesRef.current?.setData(buildEmaSeries(finalizedChartData, 21))
    ema50SeriesRef.current?.setData(buildEmaSeries(finalizedChartData, 50))

    const bollinger = buildBollingerBands(finalizedChartData, 20, 2)
    bbMiddleSeriesRef.current?.setData(bollinger.middle)
    bbUpperSeriesRef.current?.setData(bollinger.upper)
    bbLowerSeriesRef.current?.setData(bollinger.lower)

    fitSelectedPeriod()
  }, [chartData, chartTheme, finalizedChartData, fitSelectedPeriod])

  useEffect(() => {
    clearPatternSeries()
    const chart = chartRef.current
    if (!chart || !showAutoPatterns) return

    const roleTone: Record<string, string> = {
      resistance: NEG,
      support: POS,
      neckline: WARN,
      structure: '#7c3aed',
      rim: WARN,
      cup: INFO,
      handle: '#0f766e',
      flagpole: MUTED,
    }
    displayedPatterns.forEach((pattern, patternIndex) => {
      pattern.lines.forEach(line => {
        const series = chart.addSeries(LineSeries, {
          color: roleTone[line.role] ?? INFO,
          lineWidth: patternIndex === 0 ? 2 : 1,
          lineStyle: 2,
          lineType: line.role === 'cup' ? LineType.Curved : LineType.Simple,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
          title: '',
        })
        series.setData(line.points.map(point => ({
          time: point.time as Time,
          value: point.price,
        })))
        patternSeriesRef.current.push(series)
      })
    })

    return clearPatternSeries
  }, [clearPatternSeries, displayedPatterns, showAutoPatterns])

  useEffect(() => {
    clearChannelSeries()
    const chart = chartRef.current
    if (!chart || !showPriceChannel || !priceChannel) return

    priceChannel.lines.forEach(line => {
      const series = chart.addSeries(LineSeries, {
        color: line.role === 'support' ? POS : NEG,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
        title: '',
      })
      series.setData(line.points.map(point => ({
        time: point.time as Time,
        value: point.price,
      })))
      channelSeriesRef.current.push(series)
    })

    return clearChannelSeries
  }, [clearChannelSeries, priceChannel, showPriceChannel])

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
    rsiSeriesRef.current.setData(buildRsiSeries(finalizedChartData))
    rsiSeriesRef.current.applyOptions({ title: `RSI 14 · ${interval}` })
  }, [showRsi, finalizedChartData, interval])

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
  const selectedStructuralPatterns = (tradeSetup?.structural_patterns ?? []).slice(0, 2)
    .map(pattern => ({ ...pattern, timeframe: setupInterval, selected: true }))
  const contextualStructuralPatterns = setupTimeframes
    .filter(timeframe => timeframe !== setupInterval)
    .flatMap(timeframe => (setups[timeframe]?.structural_patterns ?? []).slice(0, 1)
      .map(pattern => ({ ...pattern, timeframe, selected: false })))
  const visibleStructuralPatterns = [...selectedStructuralPatterns, ...contextualStructuralPatterns]
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
    confluenceZones
      .filter(zone => zone.families.includes('volume_pivot') && zone.families.includes('fibonacci'))
      .sort((a, b) => {
        const aSelected = a.references.some(reference => reference.interval === setupInterval && reference.family === 'volume_pivot')
        const bSelected = b.references.some(reference => reference.interval === setupInterval && reference.family === 'volume_pivot')
        if (aSelected !== bSelected) return aSelected ? -1 : 1
        return Math.abs(a.distance_pct) - Math.abs(b.distance_pct)
      })
      .slice(0, 2)
      .forEach(add)

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
    <div className={chartLayout === 'split' && !chartCollapsed ? 'tk-views tk-views--split' : 'tk-views'}>
      <section className="tk-panel tk-panel--chart">
        <div className="tk-panel__bar">
          <div className="tk-segment" role="group" aria-label="Chart period">
            {['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y'].map(p => (
              <button
                key={p}
                type="button"
                className={period === p ? 'is-active' : undefined}
                onClick={() => setPeriod(p)}
              >
                {p.toUpperCase()}
              </button>
            ))}
          </div>
          <select
            className="tk-select"
            aria-label="Chart interval"
            value={interval}
            onChange={(e) => handleChartIntervalChange(e.target.value)}
          >
            <option value="1m">1 Minute</option>
            <option value="5m">5 Minutes</option>
            <option value="15m">15 Minutes</option>
            <option value="30m">30 Minutes</option>
            <option value="1h">1 Hour</option>
            <option value="1d">Daily</option>
            <option value="1wk">Weekly</option>
          </select>
          <button type="button" className="tk-action" onClick={handleRefresh} disabled={loading || setupLoading}>
            {loading ? 'Loading…' : 'Refresh'}
          </button>
          <button
            type="button"
            className="tk-toggle"
            aria-expanded={!chartCollapsed}
            onClick={() => setChartOverrides(current => ({ ...current, [view]: !chartCollapsed }))}
          >
            {chartCollapsed ? 'Show chart' : 'Hide chart'}
          </button>
          {chartData[chartData.length - 1]?.provisional && (
            <Pill
              text="Developing candle"
              tone={WARN}
              title={`Built from finalized 5m bars through ${formatDevelopingThrough(
                (chartData[chartData.length - 1].available_through
                  ?? chartData[chartData.length - 1].time),
              )}`}
            />
          )}
          {visibleChartData.length > 0 && (
            <div className="tk-stats">
              {[
                { label: 'Period high', value: `$${Math.max(...visibleChartData.map(d => d.high)).toFixed(2)}`, color: POS },
                { label: 'Period low', value: `$${Math.min(...visibleChartData.map(d => d.low)).toFixed(2)}`, color: NEG },
                { label: 'Avg vol', value: `${(visibleChartData.reduce((s, d) => s + d.volume, 0) / visibleChartData.length / 1000000).toFixed(2)}M`, color: undefined },
                { label: 'Bars', value: `${visibleChartData.length}`, color: undefined },
              ].map(stat => (
                <div key={stat.label}>
                  <span>{stat.label}</span>
                  <strong style={stat.color ? { color: stat.color } : undefined}>{stat.value}</strong>
                </div>
              ))}
            </div>
          )}
        </div>

      {/* Chart */}
      <div className={chartCollapsed && !isFullscreen ? 'tk-chart-shell is-collapsed' : 'tk-chart-shell'}>
      <div
        className={isFullscreen ? undefined : 'tk-chart'}
        style={{
          position: isFullscreen ? 'fixed' : 'relative',
          ...(isFullscreen
            ? { inset: 0, zIndex: 1000, background: 'var(--tm-surface)', display: 'flex', flexDirection: 'column' }
            : {}),
        }}
      >
        {/* Fullscreen header bar */}
        {isFullscreen && (
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.75rem',
            padding: '6px 12px',
            borderBottom: '1px solid var(--tm-line)',
            background: 'var(--tm-surface-sunken)',
            flexShrink: 0,
          }}>
            <span style={{ fontWeight: 700, fontSize: '1.1rem' }}>{symbol}</span>
            <div style={{ display: 'flex', gap: '2px', background: 'var(--tm-line)', borderRadius: '4px', padding: '2px' }}>
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
                    background: period === p ? 'var(--tm-surface)' : 'transparent',
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
              style={{ fontSize: '0.8rem', padding: '3px 6px', borderRadius: '4px', border: '1px solid var(--tm-line-strong)', background: 'var(--tm-surface)', color: 'var(--tm-ink)' }}
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
          background: 'var(--tm-float)',
          borderRadius: '6px',
          padding: '3px',
          boxShadow: '0 1px 4px rgba(0,0,0,0.12)',
          border: '1px solid var(--tm-line)',
        }}>
          <button
            onClick={() => setShowAutoPatterns(value => !value)}
            title="Toggle automatic forming-pattern trendlines"
            aria-label="Toggle automatic forming-pattern trendlines"
            aria-pressed={showAutoPatterns}
            style={{
              width: 30,
              height: 28,
              border: 'none',
              borderRadius: '4px',
              background: showAutoPatterns ? INFO_SOFT : 'transparent',
              color: showAutoPatterns ? INFO : MUTED,
              cursor: 'pointer',
              display: 'grid',
              placeItems: 'center',
            }}
          >
            <ScanLine size={17} strokeWidth={2} aria-hidden="true" />
          </button>
          <button
            onClick={() => setShowPriceChannel(value => !value)}
            disabled={interval === '1m'}
            title={interval === '1m' ? 'Price channels are available from 5 minutes through weekly' : 'Toggle selected-interval price channel'}
            aria-label="Toggle selected-interval price channel"
            aria-pressed={showPriceChannel}
            style={{
              width: 30,
              height: 28,
              border: 'none',
              borderRadius: '4px',
              background: showPriceChannel ? INFO_SOFT : 'transparent',
              color: showPriceChannel ? INFO : MUTED,
              cursor: interval === '1m' ? 'default' : 'pointer',
              opacity: interval === '1m' ? 0.35 : 1,
              display: 'grid',
              placeItems: 'center',
            }}
          >
            <TrendingUp size={17} strokeWidth={2} aria-hidden="true" />
          </button>
          <button
            onClick={clearResearchOverlays}
            disabled={!showAutoPatterns && !showPriceChannel}
            title="Erase pattern and channel overlays"
            aria-label="Erase pattern and channel overlays"
            style={{
              width: 30,
              height: 28,
              border: 'none',
              borderRadius: '4px',
              background: 'transparent',
              color: MUTED,
              cursor: showAutoPatterns || showPriceChannel ? 'pointer' : 'default',
              opacity: showAutoPatterns || showPriceChannel ? 1 : 0.35,
              display: 'grid',
              placeItems: 'center',
            }}
          >
            <Eraser size={16} strokeWidth={2} aria-hidden="true" />
          </button>
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
                color: 'var(--tm-muted)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                lineHeight: 1,
              }}
              onMouseEnter={e => { (e.target as HTMLElement).style.background = 'var(--tm-line)' }}
              onMouseLeave={e => { (e.target as HTMLElement).style.background = 'transparent' }}
            >
              {btn.label}
            </button>
          ))}
        </div>
        {showAutoPatterns && (
          <div style={{
            position: 'absolute',
            top: 44,
            right: 12,
            zIndex: 21,
            display: 'grid',
            gap: '0.4rem',
            padding: '0.3rem 0.4rem',
            border: `1px solid ${LINE}`,
            borderRadius: '5px',
            background: 'var(--tm-float)',
            boxShadow: '0 1px 4px rgba(0,0,0,0.1)',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
              <span style={{ color: MUTED, fontSize: '0.68rem', whiteSpace: 'nowrap' }}>
                {patternLoading
                  ? 'Measuring current frame…'
                  : availablePatterns.length === 0
                    ? `No pattern · ${interval}`
                    : `${availablePatterns.length} possible · ${interval}`}
              </span>
              {patternGroups.length > 0 && (
              <select
                aria-label="Possible patterns across intervals"
                title="Possible patterns across intervals; the current interval's primary pattern is shown by default"
                value={patternSelection}
                onChange={event => handlePatternSelectionChange(event.target.value)}
                style={{ border: `1px solid ${LINE}`, borderRadius: 4, padding: '2px 4px', fontSize: '0.68rem', background: 'var(--tm-surface)', color: 'var(--tm-ink)', maxWidth: 250 }}
              >
                <option value="best">Possible patterns</option>
                {availablePatterns.length > 1 && <option value="all">Show all on {PATTERN_INTERVAL_LABEL[interval] ?? interval}</option>}
                {patternGroups.map(group => (
                  <optgroup
                    key={group.interval}
                    label={`${PATTERN_INTERVAL_LABEL[group.interval] ?? group.interval}${group.bias ? ` · ${group.bias === 'MIXED' ? 'Mixed' : group.bias.charAt(0) + group.bias.slice(1).toLowerCase()}` : ''}`}
                  >
                    {group.patterns.map(pattern => (
                      <option
                        key={`${group.interval}-${pattern.type}-${pattern.start_time}`}
                        value={patternChoiceValue(group.interval, pattern.type)}
                      >
                        {pattern.type === group.primaryType ? 'Primary' : 'Alternative'} · {pattern.name} · {pattern.bias === 'BULLISH' ? 'Bullish' : pattern.bias === 'BEARISH' ? 'Bearish' : 'Neutral'} · {pattern.readiness === 'AT_EDGE' ? 'At edge' : pattern.readiness === 'NEAR_EDGE' ? 'Near edge' : 'Forming'}
                      </option>
                    ))}
                  </optgroup>
                ))}
              </select>
              )}
            </div>
            <div style={{ color: crossFrameView?.tone ?? MUTED, fontSize: '0.66rem', fontWeight: 700, whiteSpace: 'nowrap' }}>
              {crossFrameLoading
                ? 'Cross-frame · measuring…'
                : crossFrameView
                  ? `Cross-frame · ${crossFrameView.label} · ${crossFrameView.detail}`
                  : 'Cross-frame · no supported-frame patterns'}
            </div>
          </div>
        )}
        {loading && (
          <div className="loading" style={{ position: 'absolute', inset: 0, zIndex: 10, display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'var(--tm-scrim)' }}>
            <div className="spinner"></div>
            <span>Loading chart data...</span>
          </div>
        )}
        {chartError && !loading && chartData.length === 0 && (
          <div style={{ position: 'absolute', inset: 0, zIndex: 10, display: 'grid', placeItems: 'center', background: 'var(--tm-scrim)', textAlign: 'center', padding: 24 }}>
            <div>
              <strong style={{ color: INK }}>Chart data is not current yet.</strong>
              <div style={{ color: MUTED, fontSize: '0.78rem', marginTop: 5 }}>The latest completed market interval has not been published.</div>
              <button type="button" onClick={() => void handleRefresh()} style={{ marginTop: 12, border: 0, borderRadius: 5, padding: '7px 12px', background: 'var(--tm-accent)', color: 'var(--tm-on-accent)', fontWeight: 700, cursor: 'pointer' }}>Retry</button>
            </div>
          </div>
        )}
        <div style={{ position: 'relative', flex: isFullscreen ? 1 : undefined }}>
          <div 
            ref={chartContainerRef} 
            style={{ 
              width: '100%', 
              height: isFullscreen ? '100%' : `${effectiveChartHeight}px`,
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
          {(showPriceChannel || (showAutoPatterns && displayedPatterns.length > 0)) && (
            <div style={{
              position: 'absolute',
              left: 10,
              top: 84,
              zIndex: 15,
              maxWidth: 'min(430px, 72%)',
              pointerEvents: 'none',
              display: 'grid',
              gap: '0.25rem',
            }}>
              {showPriceChannel && (
                <div style={{
                  padding: '0.3rem 0.45rem',
                  borderLeft: `3px solid ${priceChannel?.bias === 'BULLISH' ? POS : priceChannel?.bias === 'BEARISH' ? NEG : MUTED}`,
                  background: 'var(--tm-float)',
                  fontSize: '0.68rem',
                }}>
                  {channelLoading ? (
                    <strong>Measuring directional channel…</strong>
                  ) : priceChannel ? (() => {
                    const read = priceChannelReading(priceChannel)
                    return (
                      <>
                        <div>
                          <strong>{priceChannel.name}</strong>
                          <span style={{ color: priceChannel.bias === 'BULLISH' ? POS : NEG, fontWeight: 700 }}>
                            {` · ${priceChannel.bias === 'BULLISH' ? 'Bullish' : 'Bearish'} structure`}
                          </span>
                        </div>
                        <div style={{ color: MUTED, marginTop: '0.12rem' }}>
                          <strong style={{ color: INK }}>{read.position}</strong>
                          {read.distance !== null ? ` · ${read.distance.toFixed(2)}% away` : ''}
                        </div>
                        <div style={{ color: MUTED, marginTop: '0.12rem' }}>{read.watch}</div>
                      </>
                    )
                  })() : (
                    <span style={{ color: MUTED }}>No reliable directional channel on {PATTERN_INTERVAL_LABEL[interval] ?? interval}</span>
                  )}
                </div>
              )}
              {displayedPatterns.map((pattern: FormingChartPattern) => {
                const read = formingPatternRead(pattern)
                const readiness = pattern.readiness === 'AT_EDGE'
                  ? 'At edge' : pattern.readiness === 'NEAR_EDGE' ? 'Near edge' : 'Forming'
                return (
                  <div
                    key={`${pattern.type}-${pattern.start_time}`}
                    style={{
                      padding: '0.3rem 0.45rem',
                      borderLeft: `3px solid ${pattern.bias === 'BULLISH' ? POS : pattern.bias === 'BEARISH' ? NEG : INFO}`,
                      background: 'var(--tm-float)',
                      fontSize: '0.68rem',
                    }}
                  >
                    <div>
                      <strong>{pattern.name}</strong>
                      <span style={{ color: pattern.bias === 'BULLISH' ? POS : pattern.bias === 'BEARISH' ? NEG : INFO, fontWeight: 700 }}>
                        {` · Bias: ${pattern.bias === 'BULLISH' ? 'Bullish' : pattern.bias === 'BEARISH' ? 'Bearish' : 'Neutral'}`}
                      </span>
                    </div>
                    <div style={{ color: MUTED, marginTop: '0.12rem' }}>
                      <strong style={{ color: INK }}>{`${readiness} of ${pattern.boundary_role} $${pattern.boundary_price.toFixed(2)}`}</strong>
                      {pattern.edge_distance_pct !== null ? ` · ${pattern.edge_distance_pct.toFixed(2)}% away` : ''}
                    </div>
                    <div style={{ color: MUTED, marginTop: '0.12rem' }}>
                      <strong style={{ color: INK }}>Break watch:</strong> {read.watch}
                    </div>
                    <div style={{ color: MUTED, marginTop: '0.12rem' }}>{read.outcome}</div>
                    <div style={{ color: MUTED, marginTop: '0.12rem' }}>{read.invalidation}</div>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      </div>
      </div>
      </section>

      {/* Trade Setup Analysis */}
      {view !== 'financials' && (
      <section className="tk-panel tk-panel--analysis">
        <div className="tk-analysis-bar">
          <label className="tk-field">
            <span>Analysis timeframe</span>
            <select
              className="tk-select"
              value={analysisPin ?? ''}
              onChange={event => setAnalysisPin(event.target.value || null)}
            >
              <option value="">Follow chart</option>
              {selectableSetupIntervals.map(value => (
                <option key={value} value={value}>
                  {INTERVAL_NOUN[value] ?? PATTERN_INTERVAL_LABEL[value] ?? value}
                </option>
              ))}
            </select>
          </label>
          <Pill
            text={analysisPin
              ? `Pinned to ${analysisLabel}`
              : analysis.snapped
                ? `${analysisLabel} · nearest published to ${interval}`
                : `Following chart · ${analysisLabel}`}
            tone={analysisPin ? INFO : analysis.snapped ? WARN : MUTED}
            title={analysisPin
              ? `Analysis stays on ${setupInterval} regardless of the ${interval} chart.`
              : analysis.snapped
                ? `No analysis is published on ${interval}; ${setupInterval} is the nearest published frame.`
                : 'Analysis follows the chart interval.'}
          />
          {analysisPin && (
            <button type="button" className="tk-toggle" onClick={() => setAnalysisPin(null)}>
              Follow chart
            </button>
          )}
          {tradeSetup && (
            <div className="tk-analysis-meta">
              <span>{tradeSetup.date} · close {money(tradeSetup.last_close)}</span>
              <span>
                {tradeSetup.ema_alignment.confirm_interval
                  ? `Confirmed against ${INTERVAL_NOUN[tradeSetup.ema_alignment.confirm_interval] ?? tradeSetup.ema_alignment.confirm_interval}`
                  : 'No higher-timeframe confirmation'}
                {relativeAge(tradeSetup.computed_at) ? ` · computed ${relativeAge(tradeSetup.computed_at)}` : ''}
              </span>
            </div>
          )}
        </div>

        <div className="tk-panel__body">
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
            <strong style={{ color: INK }}>Setup unavailable.</strong>{' '}
            {tradeSetupError ?? `No ${INTERVAL_NOUN[setupInterval] ?? setupInterval} setup data is available.`}
          </p>
        )}

        {tradeSetup && (
          <>
            {view === 'overview' && (<>
            {/* Decision bar — every tile recomputes on the selected interval */}
            <div className="tk-metrics">
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
                value={trendLabel(tradeSetup.ema_alignment.primary, tradeSetup.technicals.adx) ?? '—'}
                tone={trendTone(trendLabel(tradeSetup.ema_alignment.primary, tradeSetup.technicals.adx))}
                accent={trendTone(trendLabel(tradeSetup.ema_alignment.primary, tradeSetup.technicals.adx))}
                sub={tradeSetup.golden_cross
                  ? tradeSetup.golden_cross.type === 'Golden Cross' || tradeSetup.golden_cross.type === 'Death Cross'
                    ? `${tradeSetup.golden_cross.type}${tradeSetup.golden_cross.bars_ago !== null ? ` · ${tradeSetup.golden_cross.bars_ago} bars ago` : ''}`
                    : tradeSetup.golden_cross.type.includes('Bullish') ? '50 SMA above 200 SMA' : '50 SMA below 200 SMA'
                  : '50/200 SMA needs more history'}
                note={tradeSetup.technicals.adx != null && tradeSetup.technicals.adx < NO_TREND_ADX
                  ? `ADX ${tradeSetup.technicals.adx.toFixed(1)} — below ${NO_TREND_ADX}, no measurable trend`
                  : `Directional consistency ${plainPct(tradeSetup.technicals.trend_consistency, 0)}`}
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
                <div style={{ border: '1px solid var(--tm-line)', borderRadius: '0.5rem', marginBottom: '1rem', overflow: 'hidden' }}>
                  <div style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    gap: '0.75rem',
                    flexWrap: 'wrap',
                    padding: '0.5rem 0.85rem',
                    background: isLong ? POS_SOFT : NEG_SOFT,
                    borderBottom: '1px solid var(--tm-line)',
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
                        <div style={{ flexGrow: isLong ? plan.risk : plan.reward, background: isLong ? NEG : POS }} />
                        <div style={{ flexGrow: isLong ? plan.reward : plan.risk, background: isLong ? POS : NEG }} />
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
                      borderTop: '1px solid var(--tm-line)', paddingTop: '0.45rem',
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
                      borderTop: '1px solid var(--tm-line)',
                      background: SURFACE,
                      fontSize: '0.74rem',
                      color: verdict.tone === MUTED ? INK : verdict.tone,
                      cursor: 'help',
                    }}
                  >
                    {verdict.summary}
                  </div>

                  {verdict.cautions.length > 0 && (
                    <div style={{ borderTop: '1px solid var(--tm-line)', background: WARN_SOFT }}>
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
              <div style={{ padding: '0.85rem', marginBottom: '1rem', border: '1px dashed var(--tm-line-strong)', borderRadius: '0.5rem', fontSize: '0.82rem' }}>
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

            </>)}

            {view === 'timeframes' && (() => {
              const votes = setupTimeframes.map(timeframe => ({
                timeframe,
                side: setups[timeframe] ? sideOfBias(setups[timeframe]!.direction.bias) : undefined,
              }))
              const available = votes.filter(vote => vote.side !== undefined)
              const longs = available.filter(vote => vote.side === 'LONG').length
              const shorts = available.filter(vote => vote.side === 'SHORT').length
              const neutrals = available.length - longs - shorts
              const share = (count: number) => available.length > 0 ? (count / available.length) * 100 : 0
              const headline = available.length === 0 ? 'No timeframe reads available'
                : longs === available.length ? 'Every timeframe reads long'
                  : shorts === available.length ? 'Every timeframe reads short'
                    : longs > 0 && shorts > 0 ? 'Timeframes disagree'
                      : longs > 0 ? `${longs} of ${available.length} timeframes read long`
                        : `${shorts} of ${available.length} timeframes read short`

              return (
                <>
                  <div className="tk-agreement">
                    <div className="tk-agreement__bar" aria-hidden="true">
                      <span style={{ width: `${share(longs)}%`, background: POS }} />
                      <span style={{ width: `${share(shorts)}%`, background: NEG }} />
                      <span style={{ width: `${share(neutrals)}%`, background: MUTED }} />
                    </div>
                    <strong>{headline}</strong>
                    <small>
                      {longs} long · {shorts} short · {neutrals} neutral · counted from the directional
                      technical vote on each interval, not a combined signal
                    </small>
                  </div>

                  <div className="tk-matrix-wrap">
                    <table className="tk-matrix">
                      <thead>
                        <tr>
                          {[
                            ['TF', 'Candle interval'],
                            ['Bias', 'Directional technical vote'],
                            ['Trend', 'EMA stack and 14-bar directional consistency'],
                            ['Structure', 'Newest active confirmed chart pattern plus elevated-volume pivot count and Fibonacci overlap'],
                            ['Direction', 'Completed-bar ADX(14) trend strength with +DI and −DI directional control'],
                            ['Momentum', 'RSI and MACD state'],
                            ['Volume flow', '5-vs-20 completed-bar volume, completed-bar RVOL, 8-bar slope and CMF'],
                            ['Volatility', 'Annualized 20-bar historical volatility, trailing percentile and ATR percentage'],
                          ].map(([label, description], index) => (
                            <th
                              key={label}
                              title={description}
                              className={index === 0 ? 'tk-matrix__tf' : undefined}
                            >
                              {label}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {setupTimeframes.map(timeframe => {
                          const setup = setups[timeframe]
                          if (!setup) return (
                            <tr key={timeframe}>
                              <td className="tk-matrix__tf">{timeframe}</td>
                              <td colSpan={7} style={{ color: MUTED }}>Unavailable</td>
                            </tr>
                          )
                          const side = sideOfBias(setup.direction.bias)
                          const macdLabel = setup.technicals.macd_state.replace(/_/g, ' ').toLowerCase()
                          return (
                            <tr key={timeframe} className={timeframe === setupInterval ? 'is-primary' : undefined}>
                              <td className="tk-matrix__tf">
                                {timeframe}
                                <span>{timeframe === setupInterval ? 'primary' : timeframe === '1mo' ? 'context' : ''}</span>
                              </td>
                              <td style={{ color: side === 'LONG' ? POS : side === 'SHORT' ? NEG : MUTED, fontWeight: 700 }}>{side ?? 'NEUTRAL'}</td>
                              <td>
                                <strong style={{ color: trendTone(trendLabel(setup.ema_alignment.primary, setup.technicals.adx)) }}>
                                  {trendLabel(setup.ema_alignment.primary, setup.technicals.adx) ?? '—'}
                                </strong>
                                <div style={{ color: MUTED, fontSize: '10px' }}>{plainPct(setup.technicals.trend_consistency, 0)} consistency</div>
                              </td>
                              <td>
                                <StructureRead setup={setup} currentPrice={displayPrice} />
                              </td>
                              <td>
                                <DirectionStrengthTrack
                                  adx={setup.technicals.adx}
                                  plusDi={setup.technicals.plus_di}
                                  minusDi={setup.technicals.minus_di}
                                />
                              </td>
                              <td>
                                <strong>RSI {setup.technicals.rsi.toFixed(1)}</strong>
                                <div style={{ color: MUTED, fontSize: '10px' }}>{macdLabel}</div>
                              </td>
                              <td>
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
                                    <div style={{ color: MUTED, fontSize: '10px' }}>
                                      {setup.technicals.relative_volume === null ? '—' : `${setup.technicals.relative_volume.toFixed(2)}× last completed`}
                                    </div>
                                    <div style={{ color: MUTED, fontSize: '10px' }}>
                                      {setup.technicals.volume_slope_state.toLowerCase()} slope · {setup.technicals.volume_pressure.toLowerCase()}
                                    </div>
                                  </div>
                                </div>
                              </td>
                              <td>
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
                </>
              )
            })()}

            {view === 'levels' && (() => {
              const price = tradeSetup.last_close
              const rows = visibleConfluenceZones

              return (
                <>
                  <div className="tk-subpanel">
                    <div className="tk-subpanel__head">
                      <strong>Confirmed price structures</strong>
                      <span>{setupInterval} selected · other timeframes are context</span>
                    </div>
                    {selectedStructuralPatterns.length === 0 && (
                      <div style={{ padding: '0.55rem 0.7rem', color: MUTED, fontSize: '0.72rem', borderBottom: visibleStructuralPatterns.length > 0 ? `1px solid ${LINE}` : undefined }}>
                        No active confirmed structure on {INTERVAL_NOUN[setupInterval] ?? setupInterval} bars.
                      </div>
                    )}
                    {visibleStructuralPatterns.length === 0 ? (
                      <div style={{ padding: '0.65rem 0.7rem', color: MUTED, fontSize: '0.74rem' }}>
                        No active head-and-shoulders, double-top, or double-bottom context on other timeframes.
                      </div>
                    ) : (
                      <div className="tk-structure-list">
                        {visibleStructuralPatterns.map(pattern => {
                          const tone = pattern.direction === 'BULLISH' ? POS : NEG
                          const breakDirection = pattern.direction === 'BULLISH' ? 'above' : 'below'
                          const failureDirection = pattern.direction === 'BULLISH' ? 'below' : 'above'
                          const targetDistance = displayPrice && displayPrice > 0
                            ? (pattern.target - displayPrice) / displayPrice * 100 : null
                          const targetLocation = targetDistance === null ? null
                            : `${plainPct(Math.abs(targetDistance))} ${targetDistance >= 0 ? 'above' : 'below'} live price`
                          const necklineHolds = displayPrice === null
                            ? null
                            : pattern.direction === 'BULLISH'
                              ? displayPrice >= pattern.neckline
                              : displayPrice <= pattern.neckline
                          const necklineStatus = necklineHolds === null ? null
                            : necklineHolds ? 'NECKLINE HOLDS'
                              : pattern.direction === 'BULLISH' ? 'BACK BELOW NECKLINE' : 'BACK ABOVE NECKLINE'
                          const pivots = pattern.pivots
                            .map(pivot => `${pivot.type} ${money(pivot.price)}`)
                            .join(' · ')
                          return (
                            <div
                              className="tk-structure-card"
                              key={`${pattern.timeframe}-${pattern.type}-${pattern.confirmation_time}`}
                              title={pivots}
                              style={{
                                borderTop: `${pattern.selected ? 3 : 1}px solid ${tone}`,
                                background: pattern.selected ? INFO_SOFT : 'var(--tm-surface)',
                              }}
                            >
                              <div className="tk-structure-card__head">
                                <Pill text={`${pattern.timeframe}${pattern.selected ? ' · SELECTED' : ' · CONTEXT'}`} tone={pattern.selected ? INFO : MUTED} />
                                <strong style={{ color: tone }}>{pattern.name}</strong>
                                {necklineStatus && (
                                  <Pill
                                    text={necklineStatus}
                                    tone={necklineHolds ? tone : WARN}
                                    title={necklineHolds
                                      ? 'Live price remains on the confirmed side of the neckline.'
                                      : 'Live price crossed back through the neckline; the pattern is weakened but has not reached its invalidation level.'}
                                  />
                                )}
                              </div>
                              <div style={{ fontSize: '0.7rem', color: INK }}>
                                Confirmed: close {breakDirection} {money(pattern.neckline)}
                              </div>
                              <div style={{ fontSize: '0.68rem', color: MUTED, marginTop: '0.15rem' }}>
                                Measured target {money(pattern.target)}{targetLocation ? ` · ${targetLocation}` : ''}
                              </div>
                              <div style={{ fontSize: '0.68rem', color: MUTED, marginTop: '0.15rem' }}>
                                Pattern fails on close {failureDirection} {money(pattern.invalidation)}
                              </div>
                              <div style={{ fontSize: '0.66rem', color: MUTED, marginTop: '0.15rem' }}>
                                Break occurred {pattern.bars_ago} {INTERVAL_NOUN[pattern.timeframe] ?? pattern.timeframe} bars ago
                              </div>
                            </div>
                          )
                        })}
                      </div>
                    )}
                  </div>
                  <div className="tk-subpanel">
                    <div className="tk-subpanel__head">
                      <div>
                        <strong>Cross-timeframe zones</strong>
                        <span>
                          {rows.length} nearest of {confluenceZones.length} · {setupInterval} evidence first · volume pivots are liquidity proxies
                        </span>
                      </div>
                      {selectedLevel && (
                        <div className="tk-level-selection" role="status">
                          <span>
                            On chart: {selectedLevel.label} {money(selectedLevel.price)}
                          </span>
                          <button
                            type="button"
                            onClick={() => setSelectedLevel(null)}
                            aria-label="Remove selected level from chart"
                            title="Remove selected level from chart"
                          >
                            <X size={14} strokeWidth={2} aria-hidden="true" />
                          </button>
                        </div>
                      )}
                    </div>
                    <PriceLadder
                      price={price}
                      priceCaption={`Last ${INTERVAL_NOUN[setupInterval] ?? setupInterval} close ${money(price)}${displayPrice !== null && Math.abs(displayPrice - price) > 0.005 ? ` · live ${money(displayPrice)}` : ''}`}
                      emptyMessage="No cross-timeframe zones resolved."
                      selectedKey={selectedLevel?.key ?? null}
                      onSelect={handleLevelSelect}
                      rows={rows.map(zone => {
                        const strengthTone = zone.strength === 'STRONG_CONFLUENCE' ? POS
                          : zone.strength === 'CONFLUENCE' ? INFO : MUTED
                        const roleTone = zone.role === 'SUPPORT' ? POS : zone.role === 'RESISTANCE' ? NEG : WARN
                        const orderedReferences = [...zone.references].sort((a, b) => {
                          const aSelected = a.interval === setupInterval
                          const bSelected = b.interval === setupInterval
                          return aSelected === bSelected ? 0 : aSelected ? -1 : 1
                        })
                        const evidence = orderedReferences.slice(0, 4)
                          .map(reference => `${reference.interval} ${reference.label}`)
                        const volumeReference = orderedReferences.find(reference => reference.family === 'volume_pivot')
                        const badges: LadderBadge[] = [{
                          text: zone.strength.replace(/_/g, ' '),
                          tone: strengthTone,
                          title: zone.strength === 'STRONG_CONFLUENCE'
                            ? 'At least two timeframes and two independent evidence families overlap here.'
                            : zone.strength === 'CONFLUENCE'
                              ? 'Multiple timeframes or independent evidence families overlap here.'
                              : 'One evidence source defines this level.',
                        }]
                        if (zone.families.includes('volume_pivot') && zone.families.includes('fibonacci')) {
                          badges.push({
                            text: 'Fib + volume pivot',
                            tone: INFO,
                            title: 'An elevated-volume swing pivot overlaps a Fibonacci retracement in this price zone.',
                          })
                        }
                        zone.confirmations.forEach(pattern => badges.push({
                          text: `${pattern.interval} ${pattern.name}`,
                          tone: pattern.direction === 'BULLISH' ? POS : pattern.direction === 'BEARISH' ? NEG : MUTED,
                          title: 'Completed-candle confirmation at this zone.',
                        }))
                        return {
                          key: `${zone.low}-${zone.high}`,
                          price: zone.midpoint,
                          priceLabel: Math.abs(zone.high - zone.low) > 0.005
                            ? `${money(zone.low)}–${money(zone.high)}`
                            : money(zone.midpoint),
                          title: zone.role.replace(/_/g, ' '),
                          titleTone: roleTone,
                          detail: evidence.join(' · '),
                          emphasis: zone.role === 'ACTIVE',
                          badges,
                          note: `${zone.intervals.length} timeframe${zone.intervals.length === 1 ? '' : 's'} · ${zone.families.length} evidence type${zone.families.length === 1 ? '' : 's'}`
                            + (volumeReference?.qualifier ? ` · ${volumeReference.qualifier}` : '')
                            + (zone.references.length > evidence.length ? ` · +${zone.references.length - evidence.length} more references` : '')
                            + (zone.confirmations.length === 0 ? ' · no completed-candle confirmation' : ''),
                        }
                      })}
                    />
                  </div>
                </>
              )
            })()}

            {view === 'fibonacci' && !tradeSetup.strategy_results.fibonacci && (
              <p style={{ fontSize: '0.8rem', color: MUTED }}>
                No confirmed Fibonacci swing on {INTERVAL_NOUN[setupInterval] ?? setupInterval} bars — the zigzag needs a completed
                leg larger than the detection threshold before levels can be drawn.
              </p>
            )}

            {view === 'fibonacci' && tradeSetup.strategy_results.fibonacci && (() => {
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
              const fibMatchTolerance = Math.max(price * 0.003, tradeSetup.technicals.atr * 0.25)
              const matchingFibTimeframes = (levelPrice: number) =>
                fibonacciTimeframes.filter(timeframe => {
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
                    <div className="tk-subpanel">
                      <div className="tk-subpanel__head">
                        <strong>Fibonacci price map</strong>
                        <span>Up to seven nearby levels, weighted toward the provisional trend</span>
                      </div>
                      <PriceLadder
                        price={price}
                        priceCaption={`Current ${money(price)} · ${plainPct(activeProgress, 2)} provisional retracement`}
                        emptyMessage="No Fibonacci levels resolved."
                        selectedKey={selectedLevel?.key ?? null}
                        onSelect={handleLevelSelect}
                        rows={fibMapRows.map(row => ({
                          key: `${row.label}-${row.price}`,
                          price: row.price,
                          title: row.label,
                          titleTone: row.tone,
                          detail: row.meaning,
                          note: row.source,
                          badges: matchingFibTimeframes(row.price).map(timeframe => ({
                            text: timeframe,
                            tone: INFO,
                            title: 'A Fibonacci level within tolerance also exists on this timeframe.',
                          })),
                        }))}
                      />
                    </div>
                  )}
                </>
              )
            })()}

            {view === 'scanner' && (() => {
              const outcomeOf = (event: ScannerEventRow) => event.outcomes[event.outcomes.length - 1] ?? null

              const statusOf = (event: ScannerEventRow): { key: string; label: string; tone: string } => {
                const outcome = outcomeOf(event)
                if (!outcome) {
                  return event.outcomes.some(o => o.entry_price != null)
                    ? { key: 'OPEN', label: 'Open', tone: INFO }
                    : { key: 'PENDING', label: 'Pending entry', tone: MUTED }
                }
                switch (outcome.first_hit) {
                  case 'TARGET': return { key: 'TARGET', label: 'Target hit', tone: POS }
                  case 'STOP': return { key: 'STOP', label: 'Stopped', tone: NEG }
                  case 'SAME_BAR': return { key: 'SAME_BAR', label: 'Both in one bar', tone: WARN }
                  default: return { key: 'EXPIRED', label: 'Expired flat', tone: MUTED }
                }
              }

              const counts = scannerEvents.reduce<Record<string, number>>((totals, event) => {
                const { key } = statusOf(event)
                totals[key] = (totals[key] ?? 0) + 1
                return totals
              }, {})
              const countTiles = [
                { key: 'TARGET', label: 'Target hit', tone: POS },
                { key: 'STOP', label: 'Stopped', tone: NEG },
                { key: 'SAME_BAR', label: 'Both in one bar', tone: WARN },
                { key: 'EXPIRED', label: 'Expired flat', tone: MUTED },
                { key: 'OPEN', label: 'Open', tone: INFO },
                { key: 'PENDING', label: 'Pending entry', tone: MUTED },
              ].filter(tile => (counts[tile.key] ?? 0) > 0)

              return (
                <>
                  <div className="tk-ledger-summary">
                    <div className="tk-ledger-summary__counts">
                      <div>
                        <span>Setups</span>
                        <strong>{scannerEvents.length}</strong>
                      </div>
                      {countTiles.map(tile => (
                        <div key={tile.key}>
                          <span>{tile.label}</span>
                          <strong style={{ color: tile.tone }}>{counts[tile.key]}</strong>
                        </div>
                      ))}
                    </div>
                    <p>
                      Daily and weekly cover the last {tickerScannerEvents.daily_sessions} sessions, hourly the last{' '}
                      {tickerScannerEvents.hourly_sessions}. These are recorded outcomes for {symbol} alone — a history,
                      not evidence of an edge. Sample sizes this small cannot support a hit rate or an average return,
                      so qualification is published separately in <Link to="/stock-research/research">Stock Research</Link>.
                    </p>
                  </div>

                  {scannerEvents.length === 0 ? (
                    <p className="tk-ladder__empty">
                      No scanner setups recorded for {symbol} in the last {tickerScannerEvents.daily_sessions} daily/weekly sessions or {tickerScannerEvents.hourly_sessions} hourly sessions.
                    </p>
                  ) : (
                    <div className="tk-matrix-wrap">
                      <table className="tk-matrix tk-matrix--ledger">
                        <thead>
                          <tr>
                            {['TF', 'Setup', 'Signal time', 'Entry', 'Stop / target', 'Actual entry', 'Return / alpha', 'Status'].map((label, i) => (
                              <th key={label} className={i >= 2 && i <= 6 ? 'is-num' : undefined}>{label}</th>
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
                                  className={`is-clickable${event.interval === setupInterval ? ' is-primary' : ''}`}
                                  onClick={() => setExpandedEvents(prev => {
                                    const next = new Set(prev)
                                    if (next.has(event.event_id)) next.delete(event.event_id)
                                    else next.add(event.event_id)
                                    return next
                                  })}
                                >
                                  <td style={{ fontWeight: 700, color: event.interval === setupInterval ? INFO : MUTED }}>{event.interval}</td>
                                  <td>
                                    <div style={{ fontWeight: 600, color: event.direction === 1 ? POS : NEG }}>
                                      {open ? '▾' : '▸'} {event.direction === 1 ? 'Long' : 'Short'} · {event.trigger_type.replace(/_/g, ' ')}
                                    </div>
                                    <div style={{ color: MUTED, fontSize: '10px' }}>
                                      {event.scanner_name.replace(/_/g, ' ').replace(/^sma200/i, 'SMA200')}
                                    </div>
                                  </td>
                                  <td className="is-num">{formatScannerEventTime(event.signal_time, event.interval)}</td>
                                  <td className="is-num"><strong>{money(event.entry_price)}</strong></td>
                                  <td className="is-num">
                                    <span style={{ color: NEG }}>{money(event.stop_price)}</span>
                                    <span style={{ color: MUTED }}> / </span>
                                    <span style={{ color: POS, fontWeight: 600 }}>{money(event.target_price)}</span>
                                  </td>
                                  <td className="is-num">
                                    {nextOpen?.entry_price != null ? money(nextOpen.entry_price) : <span style={{ color: MUTED }}>Pending</span>}
                                  </td>
                                  <td className="is-num">
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
                                  <td><Pill text={status.label} tone={status.tone} /></td>
                                </tr>
                                {open && (
                                  <tr className="is-detail">
                                    <td colSpan={8}>
                                      <div className="tk-detail-meta">
                                        <span>Signal bar open {money(event.signal_open_price)}</span>
                                        <span>Last seen {formatScannerEventTime(event.last_seen_at, event.interval)}</span>
                                        <span>{event.occurrence_count} observation{event.occurrence_count === 1 ? '' : 's'}</span>
                                        {nextOpen?.entry_time && <span>Evaluation entry {formatScannerEventTime(nextOpen.entry_time, event.interval)}</span>}
                                      </div>
                                      {event.outcomes.length === 0 ? (
                                        <div className="tk-detail-meta">Waiting for future bars to resolve this setup.</div>
                                      ) : (
                                        <table className="tk-detail-table">
                                          <thead>
                                            <tr>
                                              {['Horizon', 'Exit', 'Return', 'Alpha', 'MAE / MFE', 'First hit'].map(h => (
                                                <th key={h}>{h}</th>
                                              ))}
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {event.outcomes.map(o => (
                                              <tr key={o.horizon_bars}>
                                                <td>{o.horizon_bars} {event.interval === '1wk' ? 'sessions' : 'bars'}</td>
                                                <td title={formatScannerEventTime(o.exit_time, event.interval)}>{money(o.exit_price)}</td>
                                                <td style={{ color: (o.net_signed_return ?? 0) >= 0 ? POS : NEG }}>
                                                  {o.net_signed_return != null ? signedPct(o.net_signed_return * 100) : '—'}
                                                </td>
                                                <td>{o.net_alpha_return != null ? signedPct(o.net_alpha_return * 100) : '—'}</td>
                                                <td>
                                                  {o.mae_pct != null ? plainPct(o.mae_pct * 100) : '—'} / {o.mfe_pct != null ? plainPct(o.mfe_pct * 100) : '—'}
                                                </td>
                                                <td>{o.first_hit.replace('_', ' ').toLowerCase()}</td>
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
      </section>
      )}
    </div>
  )
}

export default TickerDetail
