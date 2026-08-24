import axios from 'axios'

// Development: uses Vite proxy (/api → localhost:8001)
// Production: set VITE_API_BASE_URL to your backend URL
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api'

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 120000,
})

export interface GapResult {
  ticker: string
  gap_type: string
  gap_low: number
  gap_high: number
  last_close: number
  current_open: number
  current_high: number
  current_low: number
  trend: string
  gap_diff: number
  gap_pct: number
  gap_atr_ratio: number
  gap_date: string
  entry_direction: string | null
}

export interface FVGResult {
  ticker: string
  fvg_type: string
  status: string
  fvg_low: number
  fvg_high: number
  fvg_size: number
  fvg_pct: number
  atr_ratio: number
  last_close: number
  current_open: number
  current_high: number
  current_low: number
  proximity: string
  trend: string
  trend_aligned: boolean
  gap_date: string
  streak_count: number
  streak_direction: string | null
  bull_unmitigated: number
  bear_unmitigated: number
  total_fvgs: number
}

export interface FVGScanResponse {
  scan_datetime: string
  interval: string
  lookback: number
  total_scanned: number
  total_signals: number
  results_by_type: { [key: string]: FVGResult[] }
  results: FVGResult[]
}

export interface MAResult {
  ticker: string
  signal: string
  short_ma: number
  long_ma: number
  last_close: number
  ma_spread_pct: number
  price_vs_short_pct: number
  price_vs_long_pct: number
  days_since_cross: number | null
  crossover_date: string | null
  price_at_cross: number | null
  price_change_since_cross_pct: number | null
  weekly_short_ma: number | null
  weekly_long_ma: number | null
  weekly_spread_pct: number | null
  weekly_signal: string | null
  markers: string[]
  date: string
}

export interface MomentumPullbackResult {
  ticker: string
  last_close: number
  grade: string
  score: number
  daily_stack: boolean
  stack_count: number
  weekly_stack: boolean
  above_sma200: boolean
  sma200: number
  stoch_k: number
  stoch_d: number
  adx: number
  atr: number
  ema21: number
  dist_to_ema21_pct: number
  rubber_band: boolean
  rel_volume: number
  volume: number
  avg_volume: number
  rsi: number
  date: string
}

// Streak Analysis
export interface StreakResult {
  ticker: string
  days_matched: number
  total_days: number
  consistency: number
  dates_matched: string[]
  gap_analysis?: GapStreakAnalysis
  ma_analysis?: MaStreakAnalysis
  fib_analysis?: FibStreakAnalysis
}

export interface GapDailyDetail {
  gap_types: string[]
  gap_count: number
  freshest_gap_age: number | null
  nearest_distance_pct: number | null
  new_gaps: number
  volume_ratio: number | null
  last_close: number
}

export interface GapStreakAnalysis {
  freshest_gap_age: number | null
  freshness: string
  fill_progress: string
  fill_distances: number[]
  new_gaps_in_window: number
  type_sequence: string[]
  transition_summary: string
  avg_volume_ratio: number | null
  daily_details: Record<string, GapDailyDetail>
}

export interface MaDailyDetail {
  signal: string
  ma_spread_pct: number
  price_vs_short_pct: number
  price_vs_long_pct: number
  days_since_cross: number | null
  price_change_since_cross_pct: number | null
  volume_ratio: number | null
  markers: string[]
  last_close: number
  weekly_signal: string | null
  weekly_spread_pct: number | null
}

export interface MaStreakAnalysis {
  direction: string
  spread_trend: string
  spreads: number[]
  price_momentum: string
  price_changes: number[]
  signal_sequence: string[]
  signal_flow: string
  avg_volume_ratio: number | null
  days_since_cross: number | null
  markers: string[]
  weekly_alignment: string
  weekly_signal: string | null
  weekly_spread_pct: number | null
  weekly_spreads: number[]
  weekly_spread_trend: string
  daily_details: Record<string, MaDailyDetail>
}

export interface FibDailyDetail {
  signal: string
  nearest_level: string
  distance_pct: number
  retracement_pct: number
  zone: string
  trend_direction: string
  swing_high: number
  swing_low: number
  swing_size_pct: number
  last_close: number
  volume_ratio: number | null
}

export interface FibStreakAnalysis {
  dominant_level: string
  level_stability: string
  level_sequence: string[]
  proximity_trend: string
  distances: number[]
  depth_trend: string
  retrace_pcts: number[]
  trend_consistency: string
  signal_flow: string
  signal_sequence: string[]
  avg_volume_ratio: number | null
  pivot_stable: boolean
  daily_details: Record<string, FibDailyDetail>
}

export interface StreakResponse {
  strategy: string
  streak_days: number
  scan_dates: string[]
  total_scanned: number
  total_with_signals: number
  results: StreakResult[]
}

export interface GapScanResponse {
  scan_datetime: string
  total_scanned: number
  total_signals: number
  results_by_type: { [key: string]: GapResult[] }
  results: GapResult[]
}

export interface MAScanResponse {
  scan_datetime: string
  total_scanned: number
  total_signals: number
  short_period: number
  long_period: number
  results_by_signal: { [key: string]: MAResult[] }
  results: MAResult[]
}

export interface MomentumPullbackScanResponse {
  scan_datetime: string
  total_scanned: number
  total_signals: number
  results: MomentumPullbackResult[]
}

export interface BearishBounceResult {
  ticker: string
  last_close: number
  grade: string
  score: number
  daily_stack: boolean
  stack_count: number
  weekly_stack: boolean
  below_sma200: boolean
  sma200: number
  stoch_k: number
  stoch_d: number
  adx: number
  atr: number
  ema21: number
  dist_to_ema21_pct: number
  rubber_band: boolean
  rel_volume: number
  volume: number
  avg_volume: number
  rsi: number
  date: string
}

export interface BearishBounceScanResponse {
  scan_datetime: string
  total_scanned: number
  total_signals: number
  results: BearishBounceResult[]
}

export interface FibTarget {
  level: string
  price: number
  pct?: number
}

export interface FibExtension {
  level: string
  price: number
}

export interface FibLevel {
  name: string
  price: number
}

export interface FibNearestLevel {
  name: string
  price: number
  distance_pct: number
}

export interface FibonacciResult {
  ticker: string
  signal: string
  trend_direction: string
  last_close: number
  swing_high: number
  swing_low: number
  swing_high_date: string
  swing_low_date: string
  swing_size_pct: number
  retracement_pct: number
  fib_236: number
  fib_382: number
  fib_500: number
  fib_618: number
  fib_786: number
  nearest_level: string
  distance_pct: number
  zone: string
  support_fibs: FibLevel[]
  resistance_fibs: FibLevel[]
  nearest_support: FibNearestLevel
  nearest_resistance: FibNearestLevel
  support_targets: FibTarget[]
  resistance_targets: FibTarget[]
  upside_extensions: FibExtension[]
  downside_extensions: FibExtension[]
  date: string
}

export interface FibonacciScanResponse {
  scan_datetime: string
  total_scanned: number
  total_signals: number
  min_swing_pct: number
  results: FibonacciResult[]
}

// Market Regime Detection
export interface IndexTechnicals {
  ticker: string
  price: number
  sma_20: number
  sma_50: number
  sma_200: number
  ema_9: number
  ema_21: number
  ema_bullish: boolean
  wsma_50: number | null
  wsma_200: number | null
  macd: number
  macd_signal: number
  macd_histogram: number
  macd_hist_trend: string
  rsi: number
  dist_from_20: number
  dist_from_50: number
  dist_from_200: number
  ma_spread_50_200: number
  golden_cross: boolean
  drawdown_from_52w_high: number
  chg_20d: number
}

export interface MarketRegime {
  regime: string
  description: string
  caution_buy: boolean
  caution_sell: boolean
  divergence: string | null
  spy: IndexTechnicals
  qqq: IndexTechnicals | null
}

// Combined scan response (all 5 strategies in one request)
export interface CombinedScanResponse {
  scan_datetime: string
  total_scanned: number
  market_regime?: MarketRegime
  gaps: { total_signals: number; results: GapResult[] }
  ma_crossover: { total_signals: number; results: MAResult[] }
  momentum_pullback: { total_signals: number; results: MomentumPullbackResult[] }
  bearish_bounce: { total_signals: number; results: BearishBounceResult[] }
  fibonacci: { total_signals: number; results: FibonacciResult[]; min_swing_pct: number }
}

// RSI Screener Types
export interface RSIResult {
  ticker: string
  signal: string
  rsi: number
  last_close: number
  date: string
}

export interface RSIScanResponse {
  scan_datetime: string
  total_scanned: number
  total_signals: number
  period: number
  oversold: number
  overbought: number
  results_by_signal: { [key: string]: RSIResult[] }
  results: RSIResult[]
}

// Volume Screener Types
export interface VolumeResult {
  ticker: string
  signal: string
  volume: number
  avg_volume: number
  volume_ratio: number
  last_close: number
  price_change_pct: number
  date: string
}

export interface VolumeScanResponse {
  scan_datetime: string
  total_scanned: number
  total_signals: number
  volume_multiplier: number
  results: VolumeResult[]
}

export interface ChartDataPoint {
  time: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export interface LatestQuote {
  ticker: string
  price: number
  previous_close: number | null
  change: number | null
  change_percent: number | null
  as_of: string
  trade_date: string
  source: '5m' | '1h' | 'daily'
}

export interface TickerOverviewRow {
  ticker: string
  date: string | null
  open: number | null
  high: number | null
  low: number | null
  close: number | null
  volume: number
  chg_pct: number | null
  rel_vol: number | null
  high_52w: number | null
  pct_from_high: number | null
  low_52w: number | null
  pct_from_low: number | null
  sma_20: number | null
  sma_50: number | null
  sma_200: number | null
  dist_200: number | null
  wsma_50: number | null
  wsma_200: number | null
  dist_200w: number | null
}

// Gap Strategies
export const scanGaps = async (
  tickers?: string,
  scanDate?: string,
  interval = '1d',
  refresh = false,
): Promise<GapScanResponse> => {
  const params: Record<string, string | boolean> = { interval }
  if (tickers) params.tickers = tickers
  if (scanDate) params.scan_date = scanDate
  if (refresh) params.refresh = true
  const response = await api.get('/scan/gaps', { params })
  return response.data
}

// Fair Value Gaps
export const scanFVG = async (
  tickers?: string,
  scanDate?: string,
  interval = '1d',
  lookback = 50,
  refresh = false,
): Promise<FVGScanResponse> => {
  const params: Record<string, string | number | boolean> = { interval, lookback }
  if (tickers) params.tickers = tickers
  if (scanDate) params.scan_date = scanDate
  if (refresh) params.refresh = true
  const response = await api.get('/scan/fvg', { params })
  return response.data
}

// MA Crossover
export const scanMACrossover = async (
  tickers?: string,
  shortPeriod = 9,
  longPeriod = 21,
  scanDate?: string,
  interval = '1d',
  refresh = false,
): Promise<MAScanResponse> => {
  const params: Record<string, string | number | boolean> = { short_period: shortPeriod, long_period: longPeriod, interval }
  if (tickers) params.tickers = tickers
  if (scanDate) params.scan_date = scanDate
  if (refresh) params.refresh = true
  const response = await api.get('/scan/ma-crossover', { params })
  return response.data
}

// Momentum Pullback
export const scanMomentumPullback = async (
  tickers?: string,
  scanDate?: string,
  interval = '1d',
  refresh = false,
): Promise<MomentumPullbackScanResponse> => {
  const params: Record<string, string | boolean> = { interval }
  if (tickers) params.tickers = tickers
  if (scanDate) params.scan_date = scanDate
  if (refresh) params.refresh = true
  const response = await api.get('/scan/momentum-pullback', { params })
  return response.data
}

// Bearish Bounce
export const scanBearishBounce = async (
  tickers?: string,
  scanDate?: string,
  interval = '1d',
  refresh = false,
): Promise<BearishBounceScanResponse> => {
  const params: Record<string, string | boolean> = { interval }
  if (tickers) params.tickers = tickers
  if (scanDate) params.scan_date = scanDate
  if (refresh) params.refresh = true
  const response = await api.get('/scan/bearish-bounce', { params })
  return response.data
}

// Fibonacci Retracement
export const scanFibonacci = async (
  tickers?: string,
  scanDate?: string,
  minSwingPct = 5.0,
  interval = '1d',
  refresh = false,
): Promise<FibonacciScanResponse> => {
  const params: Record<string, string | number | boolean> = { min_swing_pct: minSwingPct, interval }
  if (tickers) params.tickers = tickers
  if (scanDate) params.scan_date = scanDate
  if (refresh) params.refresh = true
  const response = await api.get('/scan/fibonacci', { params })
  return response.data
}

export const scanAll = async (
  scanDate?: string,
  minSwingPct = 5.0,
  refresh = false,
): Promise<CombinedScanResponse> => {
  const params: Record<string, string | number | boolean> = { min_swing_pct: minSwingPct }
  if (scanDate) params.scan_date = scanDate
  if (refresh) params.refresh = true
  const response = await api.get('/scan/all', { params })
  return response.data
}

export const getMarketRegime = async (refresh = false): Promise<MarketRegime> => {
  const params: Record<string, boolean> = {}
  if (refresh) params.refresh = true
  const response = await api.get('/market-regime', { params })
  return response.data
}

// RSI Screener (TODO: Backend endpoint not yet implemented)
export const scanRSI = async (
  tickers?: string,
  period = 14,
  oversold = 30,
  overbought = 70,
): Promise<RSIScanResponse> => {
  const params: Record<string, string | number> = { period, oversold, overbought }
  if (tickers) params.tickers = tickers
  const response = await api.get('/scan/rsi', { params })
  return response.data
}

// Volume Breakout Screener (TODO: Backend endpoint not yet implemented)
export const scanVolumeBreakout = async (
  tickers?: string,
  volumeMultiplier = 2.0,
): Promise<VolumeScanResponse> => {
  const params: Record<string, string | number> = { volume_multiplier: volumeMultiplier }
  if (tickers) params.tickers = tickers
  const response = await api.get('/scan/volume-breakout', { params })
  return response.data
}

// Streak Analysis
export const scanStreak = async (
  strategy: string,
  days: number,
  shortPeriod?: number,
  longPeriod?: number,
): Promise<StreakResponse> => {
  const params: Record<string, string | number> = { strategy, days }
  if (shortPeriod != null) params.short_period = shortPeriod
  if (longPeriod != null) params.long_period = longPeriod
  const response = await api.get('/scan/streak', { params })
  return response.data
}

export interface StreakSummary {
  days: number
  scan_dates: string[]
  total_tickers: number
  tickers_with_signals: number
  summary: Record<string, Record<string, number>>
}

export const getStreakSummary = async (days: number = 3, fibSwingPct: number = 5, refresh = false): Promise<StreakSummary> => {
  const params: Record<string, number | boolean> = { days, fib_swing_pct: fibSwingPct }
  if (refresh) params.refresh = true
  const response = await api.get('/scan/streak-summary', { params })
  return response.data
}

// Get Tickers
export const getTickers = async (refresh = false): Promise<string[]> => {
  const params: Record<string, boolean> = {}
  if (refresh) params.refresh = true
  const response = await api.get('/tickers', { params })
  return response.data
}

// Get Latest Price Date
export const getLatestPriceDate = async (refresh = false): Promise<string> => {
  const params: Record<string, boolean> = {}
  if (refresh) params.refresh = true
  const response = await api.get('/latest-price-date', { params })
  return response.data.latest_date
}

// Get Chart Data
export const getChartData = async (
  ticker: string,
  period = '1y',
  interval = '1d',
  refresh = false,
): Promise<ChartDataPoint[]> => {
  const params: Record<string, string | boolean> = { period, interval }
  if (refresh) params.refresh = true
  const response = await api.get(`/stock/${ticker}/chart`, { params })
  return response.data
}

export const getLatestQuote = async (ticker: string, refresh = false): Promise<LatestQuote> => {
  const params: Record<string, boolean> = {}
  if (refresh) params.refresh = true
  const response = await api.get(`/stock/${ticker}/quote`, { params })
  return response.data
}

// Get Stock Prices from DB
export const getStockPrices = async (ticker: string, days = 365, refresh = false) => {
  const params: Record<string, number | boolean> = { days }
  if (refresh) params.refresh = true
  const response = await api.get(`/stock/${ticker}/prices`, { params })
  return response.data
}

// Trade Setup Analysis
export interface TradeSetupEntry {
  strategy: string
  condition: string
  price_zone: string
  strength: string
}

export interface TradeSetupLevel {
  level: string
  price: number
  source: string
}

export interface LevelRetest {
  level_name: string
  level_price: number
  source: string
  candle_high: number
  candle_low: number
  candle_close: number
  touch_type: string
  held: boolean
  bounce_pct: number
  bars_ago: number
}

export interface TradeSetup {
  ticker: string
  last_close: number
  date: string
  technicals: {
    rsi: number
    rsi_state: string
    stoch_k: number
    atr: number
    atr_pct: number
    ma10: number | null
    ma20: number | null
    ma50: number | null
    ma100: number | null
    ma200: number | null
    ema8: number
    ema21: number
    ema50: number
    vwap: number
    price_vs_vwap: string
    dist_to_8ema: number
    dist_to_21ema: number
    trend_consistency: number
  }
  ema_alignment: {
    daily: string
    daily_detail: string
    hourly: string | null
    hourly_ema8: number | null
    hourly_ema21: number | null
    multi_tf_agree: boolean | null
  }
  level_retests: {
    daily: LevelRetest[]
    hourly: LevelRetest[]
  }
  momentum: { state: string; detail: string }
  direction: { bias: string; conviction: string; bull_signals: number; bear_signals: number }
  golden_cross: { type: string; bars_ago: number | null; detail: string } | null
  interval: string
  signals: string[]
  entries: TradeSetupEntry[]
  targets: TradeSetupLevel[]
  stops: TradeSetupLevel[]
  timing: { urgency: string; detail: string }
  duration: { estimate: string; detail: string }
  confluence: { grade: string; count: number }
  strategy_results: {
    ma_crossover: { signal: string; spread_pct: number | null; days_since_cross: number | null; weekly_signal: string | null; markers: string[] } | null
    momentum_pullback: { grade: string; score: number } | null
    bearish_bounce: { grade: string; score: number } | null
    gaps: { support_count: number; resistance_count: number } | null
    fvg: { bull_unmitigated: number; bear_unmitigated: number; total: number } | null
    fibonacci: {
      scoring_role: 'structural_context_only'
      signal: string
      trend_direction: 'uptrend_retracement' | 'downtrend_retracement'
      swing_basis: 'latest_valid_confirmed_leg'
      swing_detection_pct: number
      swing_high: number
      swing_low: number
      swing_high_date: string
      swing_low_date: string
      swing_size_pct: number
      developing_pivot: {
        type: 'high' | 'low'
        price: number
        date: string
        move_pct_from_confirmed: number
      }
      active_leg?: {
        status: 'provisional'
        level_role: 'provisional_support' | 'provisional_resistance'
        trend_direction: 'uptrend_retracement' | 'downtrend_retracement'
        start: { type: 'high' | 'low'; price: number; date: string }
        end: { type: 'high' | 'low'; price: number; date: string }
        swing_range: number
        swing_size_pct: number
        retracement_pct: number
        levels: Array<{ name: string; price: number }>
        nearest_level: string
        nearest_level_price: number
        distance_pct: number
        confirmation: {
          condition: 'at_or_below' | 'at_or_above'
          price: number
          reversal_pct: number
        }
        current_state: {
          id: 'unconfirmed_range'
          detail: string
        }
        scenarios: Array<{
          id: 'continuation' | 'confirmation' | 'unconfirmed_range' | 'support_hold' | 'resistance_hold' | 'failure'
          title: string
          condition: 'above' | 'below' | 'at_or_below' | 'at_or_above' | 'between' | 'after_confirmation'
          trigger_price?: number
          lower_price?: number
          upper_price?: number
          detail: string
          levels: Array<{ name: string; price: number }>
        }>
      }
      confirmed_legs: Array<{
        status: 'valid' | 'invalidated'
        is_primary: boolean
        level_role: 'confirmed_support' | 'confirmed_resistance'
        trend_direction: 'uptrend_retracement' | 'downtrend_retracement'
        start: { type: 'high' | 'low'; price: number; date: string }
        end: { type: 'high' | 'low'; price: number; date: string }
        swing_range: number
        swing_size_pct: number
        levels: Array<{ name: string; price: number }>
        nearest_level: string
        nearest_level_price: number
        distance_pct: number
        invalidation: {
          condition: 'below' | 'above'
          price: number
          date: string | null
        }
      }>
      nearest_level: string
      nearest_level_price: number
      distance_pct: number
      retracement_pct: number
      levels: Array<{ name: string; price: number }>
    } | null
  }
}

export interface DailyRecommendation {
  rec_id: number
  ticker: string
  direction: 'BULL' | 'BEAR'
  confidence: number
  rank: number
  calibration_sources: string
}

export interface DailyRecommendationsResponse {
  trade_date: string
  bull_recommendations: DailyRecommendation[]
  bear_recommendations: DailyRecommendation[]
  total_bull: number
  total_bear: number
}

export const getTradeSetup = async (ticker: string, interval: string = '1d', refresh = false): Promise<TradeSetup> => {
  const params: Record<string, string | boolean> = { interval }
  if (refresh) params.refresh = true
  const response = await api.get(`/stock/${ticker}/trade-setup`, { params })
  return response.data
}

export const getDailyRecommendations = async (tradeDate?: string, refresh = false): Promise<DailyRecommendationsResponse> => {
  const params: Record<string, string> = {}
  if (tradeDate) params.trade_date = tradeDate
  if (refresh) {
    // Force refresh by bypassing cache
  }
  
  const { data } = await api.get<DailyRecommendationsResponse>(
    '/daily-recommendations',
    { params }
  )
  return data
}

export const getDailyRecommendationsWithFallback = async (refresh = false): Promise<DailyRecommendationsResponse & { used_date: string }> => {
  // Get today's date
  const today = new Date()
  
  // Try current day first
  let data = await getDailyRecommendations(today.toISOString().split('T')[0], refresh)
  if (data.bull_recommendations.length > 0 || data.bear_recommendations.length > 0) {
    return { ...data, used_date: data.trade_date }
  }
  
  // If no data today, try previous trading days (skip weekends)
  for (let i = 1; i <= 5; i++) {
    const prevDate = new Date(today)
    prevDate.setDate(prevDate.getDate() - i)
    
    // Skip weekends (0 = Sunday, 6 = Saturday)
    if (prevDate.getDay() === 0 || prevDate.getDay() === 6) {
      continue
    }
    
    const dateStr = prevDate.toISOString().split('T')[0]
    data = await getDailyRecommendations(dateStr, refresh)
    if (data.bull_recommendations.length > 0 || data.bear_recommendations.length > 0) {
      return { ...data, used_date: dateStr }
    }
  }
  
  // Return empty data with today's date if nothing found
  return {
    ...data,
    used_date: today.toISOString().split('T')[0]
  }
}

// Get All Strategies Info
export const getStrategies = async () => {
  const response = await api.get('/strategies')
  return response.data
}

// Get Tickers Overview (OHLC + daily MAs + weekly MAs)
export const getTickersOverview = async (scanDate?: string, refresh = false): Promise<TickerOverviewRow[]> => {
  const params: Record<string, string | boolean> = {}
  if (scanDate) params.scan_date = scanDate
  if (refresh) params.refresh = true
  const response = await api.get('/tickers/overview', { params })
  return response.data
}

// ============================================================================
// 5-LAYER CALIBRATION DATA
// ============================================================================

export type PatternKey = 'breakout' | 'vwap' | 'volatility' | 'trend' | 'rs' | 'calendar'

export type PatternMap = Record<PatternKey, number>

export interface PatternScoresLayer {
  trade_date: string
  sector: string | null
  patterns: PatternMap
  fired: Record<PatternKey, boolean>
  fired_count: number
  primary_regime: string | null
  sector_regime: string | null
  market_breadth_score: number | null
  analog_match_count: number
  analog_win_rate: number | null
}

export interface PatternPrior {
  win_rate: number
  sample_size: number
  confidence_multiplier: number
  lookback_days: number
  effective_date: string
}

export interface AnalogDay {
  date: string
  distance: number
  actual_return: number
  hit: boolean
}

export interface AnalogsLayer {
  trade_date: string
  analog_count: number
  analog_accuracy: number
  confidence_boost: number
  sector_regime: string | null
  similar_days: AnalogDay[]
}

export interface RecommendationLayer {
  trade_date: string
  direction: 'BULL' | 'BEAR'
  predicted_confidence: number
  predicted_return_pct: number | null
  rank: number | null
  signal_grade: string | null
  pattern_scores: PatternMap
  levels: {
    entry: number | null
    stop: number | null
    target_1: number | null
    risk_reward: number | null
  }
  calibration: {
    pattern_priors_applied: boolean
    analog_matching_applied: boolean
    confidence_before: number | null
    confidence_after: number | null
    sources: string
  }
}

export interface PerformanceLayer {
  period_days: number
  total_recs: number
  correct_recs: number
  win_rate: number
  bull_stats: { count: number; win_rate: number }
  bear_stats: { count: number; win_rate: number }
  returns: {
    avg_return_directional: number | null
  }
}

export interface BaselineLayer {
  total_recs: number
  correct_recs: number
  win_rate: number
  ticker_count: number
  avg_return_directional: number | null
}

export interface TickerCalibration {
  ticker: string
  requested_date: string
  pattern_scores: PatternScoresLayer | null
  priors: Record<string, PatternPrior>
  analogs: AnalogsLayer | null
  recommendation: RecommendationLayer | null
  performance: PerformanceLayer | null
  baseline: BaselineLayer | null
}

export const getTickerCalibration = async (
  ticker: string,
  opts: { tradeDate?: string; perfDays?: number; refresh?: boolean } = {},
): Promise<TickerCalibration> => {
  const params: Record<string, string | number | boolean> = {}
  if (opts.tradeDate) params.trade_date = opts.tradeDate
  if (opts.perfDays) params.perf_days = opts.perfDays
  if (opts.refresh) params.refresh = true
  const response = await api.get(`/stock/${ticker}/calibration`, { params })
  return response.data
}

// ============================================================================
// CROSS-SECTIONAL SIGNAL (validated momentum model)
// ============================================================================

export interface CrossSectionalSignal {
  trade_date: string
  model_version: string
  horizon_days: number
  raw_score: number | null
  neutral_score: number | null
  percentile: number | null
  decile: number | null
  side: 'LONG' | 'SHORT' | 'FLAT'
  universe_size: number
}

export interface TickerSignalResponse {
  ticker: string
  signal: CrossSectionalSignal | null
  history?: { trade_date: string; decile: number; percentile: number | null; side: string }[]
}

export interface CrossSectionalRow {
  ticker: string
  model_version: string
  horizon_days: number
  neutral_score: number | null
  percentile: number | null
  decile: number | null
  side: 'LONG' | 'SHORT' | 'FLAT'
  universe_size: number
  sector: string | null
  sector_rank: number | null
  sector_size: number | null
}

export interface CrossSectionalListResponse {
  trade_date: string | null
  count: number
  scope?: 'universe' | 'sector'
  sector?: string | null
  results: CrossSectionalRow[]
  detail?: string
}

export interface SectorCoverage {
  sector: string
  tickers: number
}

export const getSignalSectors = async (): Promise<{ sectors: SectorCoverage[] }> => {
  const response = await api.get('/signals/sectors')
  return response.data
}

export const getCrossSectionalSignals = async (
  side?: 'LONG' | 'SHORT',
  limit = 10,
  sector?: string,
): Promise<CrossSectionalListResponse> => {
  const params: Record<string, string | number> = { limit }
  if (side) params.side = side
  if (sector) params.sector = sector
  const response = await api.get('/signals/cross-sectional', { params })
  return response.data
}

export const getTickerSignal = async (ticker: string): Promise<TickerSignalResponse> => {
  const response = await api.get(`/stock/${ticker}/cross-sectional-signal`)
  return response.data
}

export type DiscoveryState =
  | 'CONTINUATION'
  | 'REVERSAL_WATCH'
  | 'EMERGING_REVERSAL'
  | 'REVERSAL_CONFIRMED'
  | 'CONFLICT'
  | 'LAGGARD'
  | 'NEUTRAL'

export type TrendState = 'UPTREND' | 'DOWNTREND' | 'NEUTRAL'
export type ExtensionRisk = 'NORMAL' | 'EXTENDED' | 'EXHAUSTION_WATCH'
export type ReversalTrigger =
  | 'NONE'
  | 'BEARISH_EARLY'
  | 'BULLISH_EARLY'
  | 'BEARISH_CONFIRMED'
  | 'BULLISH_CONFIRMED'

export interface DiscoveryRow {
  ticker: string
  model_version: string
  state: DiscoveryState
  validation_status: 'CANDIDATE_ALPHA' | 'DISCOVERY_ONLY'
  activity_percentile: number | null
  echo_percentile: number | null
  older_momentum_percentile: number | null
  long_momentum_percentile: number | null
  recent_21d_percentile: number | null
  recent_21d_return: number | null
  recent_5d_return: number | null
  close_price: number | null
  sma_20: number | null
  sma_50: number | null
  higher_swing_high: boolean | null
  higher_swing_low: boolean | null
  trend_state: TrendState | null
  extension_risk: ExtensionRisk | null
  reversal_trigger: ReversalTrigger | null
  position_guidance: string | null
  sector: string | null
  evidence: Record<string, unknown>
}

export interface DiscoveryResponse {
  trade_date: string | null
  count: number
  summary: Partial<Record<DiscoveryState, number>>
  results: DiscoveryRow[]
}

export interface TickerDiscoveryState {
  trade_date: string
  state: DiscoveryState
  validation_status: 'CANDIDATE_ALPHA' | 'DISCOVERY_ONLY'
  activity_percentile: number | null
  echo_percentile: number | null
  recent_21d_percentile: number | null
  recent_21d_return: number | null
  recent_5d_return: number | null
  trend_state: TrendState | null
  extension_risk: ExtensionRisk | null
  reversal_trigger: ReversalTrigger | null
  position_guidance: string | null
  evidence: Record<string, unknown>
}

export interface TickerDiscoveryResponse {
  ticker: string
  state: TickerDiscoveryState | null
  history: TickerDiscoveryState[]
}

export const getDiscoveryStates = async (
  state?: DiscoveryState,
  limit = 100,
  sector?: string,
): Promise<DiscoveryResponse> => {
  const params: Record<string, string | number> = { limit }
  if (state) params.state = state
  if (sector) params.sector = sector
  const response = await api.get('/discovery/states', { params })
  return response.data
}

export const getTickerDiscoveryState = async (ticker: string): Promise<TickerDiscoveryResponse> => {
  const response = await api.get(`/stock/${ticker}/discovery-state`)
  return response.data
}

export type ScannerPromotionStatus =
  | 'COLLECTING'
  | 'INSUFFICIENT_SAMPLE'
  | 'FAILED'
  | 'PROMISING'
  | 'VALIDATED'

export interface ScannerEventOutcome {
  horizon_bars: number
  entry_time?: string
  entry_price?: number
  entry_model?: string
  exit_time: string
  exit_price?: number
  net_signed_return: number | null
  net_alpha_return: number | null
  mae_pct: number | null
  mfe_pct: number | null
  mae_r: number | null
  mfe_r: number | null
  first_hit: 'STOP' | 'TARGET' | 'SAME_BAR' | 'NONE'
}

export type ScannerInterval = '1d' | '1wk' | '1h'

export interface ScannerEventRow {
  event_id: number
  scanner_name: string
  scanner_version: string
  interval: ScannerInterval
  ticker?: string
  signal_time: string
  last_seen_at: string
  occurrence_count: number
  direction: 1 | -1
  trigger_type: string
  discovery_state: DiscoveryState | null
  validation_status: 'UNVALIDATED_TIMING'
  signal_open_price: number | null
  entry_price: number
  atr_at_signal: number | null
  reference_level: number | null
  stop_price: number | null
  target_price: number | null
  risk_per_share: number | null
  metadata: Record<string, unknown>
  outcomes: ScannerEventOutcome[]
}

export interface LatestScannerSignalRow {
  event_id: number
  scanner_name: string
  scanner_version: string
  interval: ScannerInterval
  ticker: string
  signal_time: string
  direction: 1 | -1
  trigger_type: string
  sector: string | null
  discovery_state: DiscoveryState | null
  current_discovery_state: DiscoveryState | null
  trend_state: TrendState | null
  extension_risk: ExtensionRisk | null
  reversal_trigger: ReversalTrigger | null
  position_guidance: string | null
  validation_status: 'UNVALIDATED_TIMING'
  signal_open_price: number | null
  signal_close_price: number
  stop_price: number | null
  target_price: number | null
  next_open_price: number | null
  next_open_time: string | null
  review_priority_tier: 'HIGHER' | 'STANDARD' | 'LOWER' | 'UNRANKED'
  review_priority_reasons: string[]
}

export interface SectorPerformanceRow {
  sector: string
  trade_date: string
  tickers: number
  average_return: number
  median_return: number
  positive_tickers: number
  negative_tickers: number
  positive_breadth: number
  best_ticker: string
  best_return: number
  worst_ticker: string
  worst_return: number
}

export type SectorPerformanceSessions = 1 | 5 | 10 | 21

export interface ScannerEventSummaryRow {
  scanner_name: string
  scanner_version: string
  interval: ScannerInterval
  discovery_state: DiscoveryState | null
  direction: 1 | -1
  horizon_bars: number
  independent_periods: number
  events: number
  mean_net_return: number | null
  mean_net_alpha: number | null
  alpha_t_stat: number | null
  hit_rate: number | null
  mean_mae_pct: number | null
  mean_mfe_pct: number | null
  mean_mae_r: number | null
  mean_mfe_r: number | null
  stop_first_rate: number | null
  target_first_rate: number | null
  promotion_status: ScannerPromotionStatus
}

export interface ScannerBacklogRow {
  interval: ScannerInterval
  horizon_bars: number
  pending: number
  evaluated: number
}

export interface ScannerQualificationRow {
  scanner_name: string
  scanner_version: string
  interval: ScannerInterval
  direction: 1 | -1
  horizon_bars: number
  events: number
  independent_periods: number
  mean_net_return: number | null
  mean_net_alpha: number | null
  alpha_t_stat: number | null
  alpha_p_value: number | null
  alpha_fdr_q: number | null
  alpha_ci_low: number | null
  alpha_ci_high: number | null
  early_alpha: number | null
  late_alpha: number | null
  hit_rate: number | null
  hit_rate_ci_low: number | null
  hit_rate_ci_high: number | null
  mean_mae_pct: number | null
  mean_mfe_pct: number | null
  stop_first_rate: number | null
  target_first_rate: number | null
  mean_sector_alpha: number | null
  sector_alpha_t_stat: number | null
  distinct_tickers: number | null
  top5_concentration: number | null
  regime_alpha: Record<'BULL' | 'BEAR' | 'CHOPPY', { mean_alpha: number | null; periods: number }>
  qualification_status: 'PRIMARY_PASS' | 'NOT_QUALIFIED'
  evidence_status: 'ROBUST_PASS' | 'MONITOR_ONLY' | 'UNRANKED'
  calibration_status: 'RESEARCH_CALIBRATED' | 'FAILED_DIAGNOSTICS' | 'NOT_ELIGIBLE'
  calibration_oos_periods: number
  calibrated_win_probability: number | null
  calibrated_win_probability_ci_low: number | null
  calibrated_win_probability_ci_high: number | null
  brier_score: number | null
  brier_skill_score_vs_50: number | null
  expected_calibration_error: number | null
  live_expected_alpha: number | null
  live_expected_alpha_ci_low: number | null
  live_expected_alpha_ci_high: number | null
  calibration_curve: Array<{
    count: number
    mean_predicted: number
    observed_frequency: number
    minimum_prediction: number
    maximum_prediction: number
  }>
}

export interface ScannerQualificationResponse {
  entry_model: string
  gates: {
    minimum_events: number
    minimum_independent_periods: number
    minimum_alpha_t_stat: number
    requires_positive_early_late_alpha: boolean
    maximum_false_discovery_rate: number
    minimum_calibration_oos_periods: number
    maximum_brier_score: number
    maximum_expected_calibration_error: number
  }
  results: ScannerQualificationRow[]
}

export const getScannerEventSummary = async (
  interval?: ScannerInterval, minPeriods = 1,
): Promise<{ results: ScannerEventSummaryRow[] }> => {
  const params: Record<string, string | number> = { min_periods: minPeriods }
  if (interval) params.interval = interval
  const response = await api.get('/scanner-events/summary', { params })
  return response.data
}

export const getScannerEventBacklog = async (): Promise<{ results: ScannerBacklogRow[] }> => {
  const response = await api.get('/scanner-events/backlog')
  return response.data
}

export const getScannerQualification = async (
  interval?: ScannerInterval,
): Promise<ScannerQualificationResponse> => {
  const response = await api.get('/scanner-events/qualification', {
    params: interval ? { interval } : undefined,
  })
  return response.data
}

export const getScannerEvents = async (
  interval?: ScannerInterval, limit = 100,
): Promise<{ results: ScannerEventRow[] }> => {
  const params: Record<string, string | number> = { limit }
  if (interval) params.interval = interval
  const response = await api.get('/scanner-events', { params })
  return response.data
}

export const getLatestScannerSignals = async (
  interval?: ScannerInterval, limit = 500, sessions = 10, hourlySessions = 2,
): Promise<{ results: LatestScannerSignalRow[] }> => {
  const params: Record<string, string | number> = { limit, sessions, hourly_sessions: hourlySessions }
  if (interval) params.interval = interval
  const response = await api.get('/scanner-events/latest-by-ticker', { params })
  return response.data
}

export const getScannerSectorPerformance = async (
  sessions: SectorPerformanceSessions = 1,
): Promise<{ sessions: SectorPerformanceSessions; results: SectorPerformanceRow[] }> => {
  const response = await api.get('/scanner-events/sector-performance', { params: { sessions } })
  return response.data
}

export interface SectorRotationWindow {
  average_return: number
  positive_breadth: number
  tickers: number
  rank: number
}

export interface SectorCrossSectionalSkew {
  long_skew: number
  short_skew: number
  average_percentile: number | null
  covered: number
  net_tilt: number | null
  long_names: string[]
  short_names: string[]
}

export interface SectorLeaderRow {
  ticker: string
  return_pct: number
}

export interface SectorIntelligenceRow {
  sector: string
  rotation: Record<string, SectorRotationWindow | null>
  rotation_delta: number | null
  discovery_mix: Partial<Record<DiscoveryState, number>>
  discovery_universe: number
  cross_sectional_skew: SectorCrossSectionalSkew | null
  leaders: Record<string, SectorLeaderRow[]>
  laggards: Record<string, SectorLeaderRow[]>
}

export interface SectorIntelligenceResponse {
  trade_date: string | null
  discovery_trade_date: string | null
  cross_sectional_trade_date: string | null
  sessions: number[]
  leader_sessions: number[]
  results: SectorIntelligenceRow[]
}

export const getSectorIntelligence = async (leaderLimit = 5): Promise<SectorIntelligenceResponse> => {
  const response = await api.get('/sector-intelligence', { params: { leader_limit: leaderLimit } })
  return response.data
}

export const getTickerScannerEvents = async (
  ticker: string, limit = 50,
): Promise<{ ticker: string; events: ScannerEventRow[] }> => {
  const response = await api.get(`/stock/${ticker}/scanner-events`, { params: { limit } })
  return response.data
}

export default api
