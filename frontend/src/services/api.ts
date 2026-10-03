import axios from 'axios'

// Development: uses Vite proxy (/api → localhost:8001)
// Production: set VITE_API_BASE_URL to your backend URL
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api'

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 120000,
})

export interface RotationFact<Value> {
  status: string
  value: Value | null
  reason_codes: string[]
  available_at?: string | null
  market_time?: string | null
  session?: string
  timely_observations?: number
  expected_observations?: number
  last_attempt_at?: string | null
  last_fetch_error?: string
  units?: string
  boundary?: string
  weight?: number
  method?: string
  input_statuses?: string[]
  input_sessions?: Array<string | null>
  input_market_times?: Array<string | null>
  action_review?: Array<{ type: string; effective_date: string; split_from?: number | null; split_to?: number | null; source?: string | null; revision_id: string }>
  split_adjustments?: Array<{ action_revision_id: string; effective_date: string; price_factor: number; reviewed_at: string; review_sha256: string; evidence_url: string }>
  coverage?: Array<{ available: number | null; expected: number | null }>
  source_url?: string
  lineage_ref: string
}

export interface RotationPrices {
  close: number
  direction: string
  ema50: number
  ema50_change10: number
  return1: number
  return5: number
  return20: number
  volume: number
  range252_low: number
  range252_high: number
}

export interface RotationValues {
  state: string
  relative1: number
  relative5: number
  relative20: number
  relative_change5: number
  benchmark_return5?: number | null
  absolute_return5: number
  absolute_return20: number
  emerging_leadership: boolean
}

export interface RotationObservation extends RotationFact<RotationValues> {
  persistence?: { status: string; observations: number; confirmed_state: string | null }
}

export interface SectorRotationRow {
  ticker: string
  sector: string
  context: RotationFact<RotationPrices>
  rotation: RotationObservation
  history: RotationObservation[]
}

export interface StockDivergenceRow {
  security_id: string
  security_type: string | null
  ticker: string
  sector: string | null
  proxy: string | null
  context: RotationFact<RotationPrices>
  relative: RotationObservation
  divergence: RotationFact<{ labels: string[]; stock_return: number; sector_return: number }>
}

export interface BondComparisonStatistics {
  expected_pairs: number
  matched_pairs: number
  coverage_fraction: number | null
  pearson: number | null
  spearman: number | null
  directional_agreement: number | null
  nonzero_pairs: number
  tied_or_zero_pairs: number
  correlation_status: string
}

export interface BondComparisonView {
  status: string
  reason_codes?: string[]
  period_start?: string
  period_end?: string
  cutoff?: string
  report_sha256?: string
  input_sha256?: string
  reference?: { status: string; series_id: string; received_at: string | null; last_observation?: string; response_sha256: string | null }
  summary?: Record<string, Record<'rolling' | 'nonoverlapping' | 'first_half' | 'second_half', BondComparisonStatistics>>
  current_proxy?: RotationFact<Record<string, number | string | null>>
  limitations?: string[]
  largest_direction_disagreements?: Array<{ session: string; start_session: string; horizon: number; hyg_return: number; lqd_return: number; proxy_pp: number; oas_change_bps: number; known_distribution_dates: string[] }>
}

export interface MarketConditionsResponse {
  status: string
  price_basis?: string
  split_review_sha256?: string | null
  session?: string
  as_of?: string
  capture_age_seconds?: number
  stale?: boolean
  expected_session?: string
  market?: RotationFact<{ direction: string }>
  benchmarks: Record<string, RotationFact<RotationPrices>>
  sectors: SectorRotationRow[]
  stocks: StockDivergenceRow[]
  coverage?: { expected_sectors: number; ready_sectors: number; expected_stocks: number; stock_states: Record<string, number> }
  warnings?: string[]
  snapshot_sha256?: string
  refresh_mode?: string
  additional_context?: Record<string, RotationFact<Record<string, number | string | null>>>
  intraday_volumes?: Record<string, RotationFact<{ cumulative_volume: number; average_volume20: number; relative_volume: number }>>
  bond_comparison?: BondComparisonView
  score_components?: Record<string, RotationFact<{ score: number; weight: number; contribution: number }>>
  sector_option_activity?: Record<string, RotationFact<{ call_volume: number; put_volume: number; call_premium: number; put_premium: number; put_call_volume_ratio: number | null }>>
  etf_creations?: Record<string, RotationFact<{ net_creations_usd: number; share_change: number; nav: number; shares_outstanding: number; previous_session: string }>>
}

export const getMarketConditions = async (): Promise<MarketConditionsResponse> => {
  const response = await api.get('/stocks/market-conditions')
  return response.data
}

export interface MaterializationStatus {
  status: 'READY' | 'STALE' | 'EXPIRED' | 'UNAVAILABLE'
  checked_at: string
  max_serveable_stale_sessions: number
  staleness_sessions: number
  last_market_time: string | null
  oldest_stale_market_time: string | null
  stale_intervals: string[]
  pipeline_active?: boolean
  active_intervals?: string[]
}

export const fetchMaterializationStatus = async (): Promise<MaterializationStatus> => {
  const response = await api.get('/equity/materialization-status')
  const status = response.data as MaterializationStatus
  if (status.pipeline_active !== undefined) return status
  const health = await api.get('/equity/health')
  const activeIntervals = Array.from(new Set<string>(
    (health.data.recent_runs || [])
      .filter((run: { status?: string }) => run.status === 'PENDING' || run.status === 'RUNNING')
      .map((run: { interval: string }) => run.interval),
  )).sort()
  return { ...status, pipeline_active: activeIntervals.length > 0, active_intervals: activeIntervals }
}

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
  gap_direction?: 'UP' | 'DOWN'
  gap_lifecycle?: 'OPEN' | 'PARTIALLY_FILLED' | 'FILLED' | 'SAME_SESSION_FADE' | 'FAILED'
  gap_classification?: 'BREAKAWAY' | 'CONTINUATION' | 'EXHAUSTION_WATCH' | 'COMMON' | 'UNCLASSIFIED'
  classification_confidence?: 'MODERATE' | 'LIMITED'
  classification_reason_codes?: string[]
  formation_trend?: 'BULLISH' | 'BEARISH' | 'SIDEWAYS' | 'UNKNOWN'
  formation_relative_volume?: number | null
  opening_gap_pct?: number
  full_gap_pct?: number
  fill_pct?: number
  fill_target?: number
  first_fill_date?: string | null
  gap_age_bars?: number
  gap_age_sessions?: number
  range_gap_survived?: boolean
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

export interface ChartDataPoint {
  time: number
  open: number
  high: number
  low: number
  close: number
  volume: number
  derived?: boolean
  provisional?: boolean
  source_interval?: '5m'
  available_through?: number
}

export interface FormingPatternLine {
  role: 'resistance' | 'support' | 'neckline' | 'structure' | 'rim' | 'cup' | 'handle' | 'flagpole'
  points: Array<{ time: number; price: number }>
}

export interface FormingChartPattern {
  type: 'ASCENDING_TRIANGLE' | 'DESCENDING_TRIANGLE' | 'SYMMETRICAL_TRIANGLE'
    | 'RISING_WEDGE' | 'FALLING_WEDGE' | 'BULL_PENNANT' | 'BEAR_PENNANT'
    | 'BULL_FLAG' | 'BEAR_FLAG' | 'CUP_AND_HANDLE'
    | 'HEAD_AND_SHOULDERS' | 'INVERSE_HEAD_AND_SHOULDERS' | 'TRIPLE_TOP' | 'TRIPLE_BOTTOM'
  name: string
  status: 'FORMING'
  bias: 'BULLISH' | 'BEARISH' | 'NEUTRAL'
  grade: 'STRONG_GEOMETRY' | 'VALID_GEOMETRY'
  start_time: number
  end_time: number
  formation_bars: number
  upper_touches: number
  lower_touches: number
  contraction_pct: number | null
  apex_bars_ahead: number | null
  fit_error_atr: number | null
  flagpole_atr: number | null
  invalidation_price: number | null
  readiness: 'AT_EDGE' | 'NEAR_EDGE' | 'FORMING'
  boundary_role: 'resistance' | 'support'
  boundary_price: number
  edge_distance_atr: number
  edge_distance_pct: number | null
  lines: FormingPatternLine[]
}

export interface ChartPatternsResponse {
  ticker: string
  interval: string
  status: 'FORMING_RESEARCH'
  last_close: number | null
  patterns: FormingChartPattern[]
  computed_at: string
}

export interface PriceChannel {
  type: 'RISING_CHANNEL' | 'FALLING_CHANNEL'
  name: string
  bias: 'BULLISH' | 'BEARISH'
  grade: 'STRONG_GEOMETRY' | 'VALID_GEOMETRY'
  position: 'NEAR_SUPPORT' | 'NEAR_RESISTANCE' | 'MID_CHANNEL'
  start_time: number
  end_time: number
  formation_bars: number
  upper_touches: number
  lower_touches: number
  support_price: number
  resistance_price: number
  support_distance_atr: number
  resistance_distance_atr: number
  support_distance_pct: number | null
  resistance_distance_pct: number | null
  width_atr: number
  slope_atr_per_bar: number
  fit_error_atr: number
  lines: Array<{
    role: 'resistance' | 'support'
    points: Array<{ time: number; price: number }>
  }>
}

export interface PriceChannelResponse {
  ticker: string
  interval: string
  status: 'CHANNEL_RESEARCH'
  last_close: number | null
  channel: PriceChannel | null
  computed_at: string
}

export interface PatternWatchRow {
  ticker: string
  sector: string | null
  interval: string
  last_close: number | null
  pattern: FormingChartPattern
  channel: PriceChannel | null
}

export interface CrossFramePatternGroup {
  interval: '5m' | '15m' | '30m' | '1h' | '1d' | '1wk'
  tier: 'CONTEXT' | 'SETUP' | 'TRIGGER'
  bias: 'BULLISH' | 'BEARISH' | 'NEUTRAL' | 'MIXED'
  primary_pattern_type: FormingChartPattern['type']
  primary_pattern_name: string
  readiness: FormingChartPattern['readiness']
  pattern_types: FormingChartPattern['type'][]
  pattern_count: number
}

export interface CrossFramePatternSummary {
  state: 'ALIGNED_BULLISH' | 'ALIGNED_BEARISH' | 'COUNTERTREND' | 'MIXED' | 'NEUTRAL' | 'SINGLE_FRAME'
  dominant_bias: 'BULLISH' | 'BEARISH' | null
  directional_frames: number
  frames: CrossFramePatternGroup[]
}

export interface PatternWatchResponse {
  interval: string
  scanned: number
  matched_tickers: number
  results: PatternWatchRow[]
  cross_frame: CrossFramePatternSummary | null
  computed_at: string
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

export interface FundamentalReport {
  timeframe: string | null
  fiscal_year: number | null
  fiscal_quarter: number | null
  period_end: string | null
  filing_date: string | null
  revenue: number | string | null
  gross_profit: number | string | null
  operating_income: number | string | null
  ebitda: number | string | null
  net_income: number | string | null
  diluted_eps: number | null
  total_assets: number | string | null
  total_liabilities: number | string | null
  total_equity: number | string | null
  operating_cash_flow: number | string | null
  free_cash_flow: number | string | null
  source: string | null
  quality_codes: string[] | null
}

export interface EquitySecurityProfileResponse {
  ticker: string
  security: {
    company_name: string | null
    security_type: string | null
    primary_exchange: string | null
    sector: string | null
    industry: string | null
    sic_description: string | null
    list_date: string | null
    market_cap: number | string | null
    free_float: number | string | null
    free_float_percent: number | null
    weighted_shares: number | string | null
    cik: string | null
    composite_figi: string | null
    share_class_figi: string | null
  } | null
  fundamental_reports: FundamentalReport[]
}

export interface TickerOverviewRow {
  ticker: string
  sector: string | null
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

export type SetupSummaryInterval = '30m' | '1h' | '1d' | '1wk' | '1mo'

/** Published per-ticker setup state; descriptive only, never a ranking. */
export interface SetupSummaryRow {
  ticker: string
  trend: string | null
  trend_detail: string | null
  confirm_trend: string | null
  confirm_interval: string | null
  multi_tf_agree: boolean | null
  momentum: string | null
  momentum_detail: string | null
  bias: string | null
  conviction: string | null
  bull_signals: number | null
  bear_signals: number | null
  rsi: number | null
  rsi_state: string | null
  stoch_k: number | null
  adx: number | null
  macd_state: string | null
  macd_histogram: number | null
  trend_consistency: number | null
  atr_pct: number | null
  relative_volume: number | null
  volume_trend_state: string | null
  volume_pressure: string | null
  historical_volatility_pct: number | null
  historical_volatility_state: string | null
  range_position_pct: number | null
  price_vs_vwap: string | null
  golden_cross: string | null
  signals: string[]
}

export interface SetupSummaryResponse {
  interval: SetupSummaryInterval
  source: string
  analysis_run_id: string | null
  computed_at: string | null
  is_fresh: boolean
  staleness_sessions: number
  market_time: string | null
  count: number
  results: SetupSummaryRow[]
}

export const getSetupSummary = async (interval: SetupSummaryInterval = '1d'): Promise<SetupSummaryResponse> => {
  const response = await api.get('/tickers/setup-summary', { params: { interval } })
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

// Selectable scan sessions, oldest first (weekends, holidays and un-ingested days absent)
export interface TradingSessionsResponse {
  interval: string
  sessions: string[]
  data_as_of: string | null
  data_interval: string | null
}

export const getTradingSessions = async (interval = '1d', limit = 260): Promise<TradingSessionsResponse> => {
  const response = await api.get('/trading-sessions', { params: { interval, limit } })
  return response.data
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

export const getChartPatterns = async (
  ticker: string,
  interval = '1d',
  refresh = false,
): Promise<ChartPatternsResponse> => {
  const params: Record<string, string | boolean> = { interval }
  if (refresh) params.refresh = true
  const response = await api.get(`/stock/${ticker}/chart-patterns`, { params })
  return response.data
}

export const getPriceChannel = async (
  ticker: string,
  interval = '1d',
  refresh = false,
): Promise<PriceChannelResponse> => {
  const params: Record<string, string | boolean> = { interval }
  if (refresh) params.refresh = true
  const response = await api.get(`/stock/${ticker}/price-channel`, { params })
  return response.data
}

export const scanChartPatterns = async (
  interval = '1d',
  refresh = false,
): Promise<PatternWatchResponse> => {
  const params: Record<string, string | boolean> = { interval }
  if (refresh) params.refresh = true
  const response = await api.get('/chart-patterns/scan', { params })
  return response.data
}

export const scanTickerChartPatterns = async (
  ticker: string,
  refresh = false,
): Promise<PatternWatchResponse> => {
  const response = await api.get(`/chart-patterns/ticker/${ticker}`, {
    params: refresh ? { refresh: true } : undefined,
  })
  return response.data
}

export const getLatestQuote = async (ticker: string, refresh = false): Promise<LatestQuote> => {
  const params: Record<string, boolean> = {}
  if (refresh) params.refresh = true
  const response = await api.get(`/stock/${ticker}/quote`, { params })
  return response.data
}

export const getEquitySecurityProfile = async (
  ticker: string,
): Promise<EquitySecurityProfileResponse> => {
  const response = await api.get(`/equity/security/${ticker}`, {
    params: { report_limit: 12 },
  })
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
  zone_low: number | null
  zone_high: number | null
  strength: string
}

export interface TradeSetupLevel {
  level: string
  price: number
  source: string
}

export interface TradeSetupZone {
  name: string
  low: number
  high: number
  source: string
  qualifier: string
  pivot_time?: string
  pivot_type?: 'high' | 'low'
  volume_ratio?: number
  bars_ago?: number
  fibonacci_levels?: Array<{ name: string; price: number }>
}

export interface StructuralPattern {
  type: 'HEAD_AND_SHOULDERS' | 'DOUBLE_TOP' | 'DOUBLE_BOTTOM'
  name: string
  direction: 'BULLISH' | 'BEARISH'
  status: 'CONFIRMED'
  confirmation_time: string
  bars_ago: number
  neckline: number
  target: number
  invalidation: number
  pivots: Array<{
    type: 'high' | 'low'
    price: number
    time: string
  }>
}

export interface CandlestickConfirmation {
  name: string
  direction: 'BULLISH' | 'BEARISH' | 'NEUTRAL'
  bar_time: string
  open: number
  high: number
  low: number
  close: number
  interval?: '1h' | '1d' | '1wk'
}

export interface ConfluenceReference {
  interval: '1h' | '1d' | '1wk'
  label: string
  low: number
  high: number
  price: number
  source: string
  family: string
  qualifier: string | null
}

export interface ConfluenceZone {
  low: number
  high: number
  midpoint: number
  distance_pct: number
  role: 'ACTIVE' | 'SUPPORT' | 'RESISTANCE'
  strength: 'STRONG_CONFLUENCE' | 'CONFLUENCE' | 'SINGLE_REFERENCE'
  intervals: Array<'1h' | '1d' | '1wk'>
  families: string[]
  references: ConfluenceReference[]
  confirmations: CandlestickConfirmation[]
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
  computed_at?: string
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
    macd: number
    macd_signal: number
    macd_histogram: number
    macd_histogram_previous: number
    macd_state: 'BULLISH_RISING' | 'BULLISH_FADING' | 'BEARISH_FALLING' | 'BEARISH_IMPROVING'
    adx: number | null
    plus_di: number | null
    minus_di: number | null
    historical_volatility_pct: number | null
    historical_volatility_percentile: number | null
    historical_volatility_state: 'ELEVATED' | 'NORMAL' | 'QUIET' | 'UNAVAILABLE'
    relative_volume: number | null
    volume_trend_ratio: number | null
    volume_trend_pct: number | null
    volume_trend_state: 'EXPANDING' | 'STABLE' | 'CONTRACTING' | 'UNAVAILABLE'
    volume_slope: number | null
    volume_slope_state: 'RISING' | 'FALLING' | 'FLAT' | 'UNAVAILABLE'
    volume_sparkline: number[]
    cmf_20: number | null
    volume_pressure: 'ACCUMULATION' | 'BALANCED' | 'DISTRIBUTION' | 'UNAVAILABLE'
    range_low: number
    range_high: number
    range_position_pct: number
  }
  ema_alignment: {
    primary: string
    primary_detail: string
    confirm_interval: string
    confirm: string | null
    confirm_ema8: number | null
    confirm_ema21: number | null
    multi_tf_agree: boolean | null
  }
  level_retests: {
    primary: LevelRetest[]
    confirm: LevelRetest[]
    confirm_interval: string
  }
  momentum: { state: string; detail: string }
  direction: { bias: string; conviction: string; bull_signals: number; bear_signals: number }
  golden_cross: { type: string; bars_ago: number | null; detail: string } | null
  interval: string
  signals: string[]
  candlestick_patterns: CandlestickConfirmation[]
  structural_patterns: StructuralPattern[]
  entries: TradeSetupEntry[]
  zones: TradeSetupZone[]
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
      swing_basis: 'structural_confirmed_leg'
      swing_detection_pct: number
      scope_bars: number
      swing_high: number
      swing_low: number
      swing_high_date: string
      swing_low_date: string
      swing_size_pct: number
      progress_reached_pct: number
      progress_current_pct: number
      target_kind: 'retracement' | 'extension'
      retracement_levels: Array<{ name: string; price: number; kind: 'retracement' | 'full_retracement' }>
      extension_levels: Array<{ name: string; price: number; kind: 'extension' }>
      target_levels: Array<{ name: string; price: number; kind: 'retracement' | 'full_retracement' | 'extension' }>
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
        progress_reached_pct: number
        progress_current_pct: number
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

export interface MultiTradeSetupResponse {
  ticker: string
  setups: Partial<Record<'30m' | '1h' | '1d' | '1wk' | '1mo', TradeSetup>>
  errors: Partial<Record<'30m' | '1h' | '1d' | '1wk' | '1mo', string>>
  setup_sources: Partial<Record<
    '30m' | '1h' | '1d' | '1wk' | '1mo',
    'MATERIALIZED_CURRENT_PROJECTION' | 'LEGACY_REQUEST_TIME'
  >>
  setup_read_metrics?: Partial<Record<'30m' | '1h' | '1d' | '1wk' | '1mo', {
    status: 'READY' | 'MISSING' | 'STALE' | 'EXPIRED'
    read_mode: 'MATERIALIZED'
    source: 'MATERIALIZED_CURRENT_PROJECTION'
    process_started_at: string
    analysis_run_id?: string
    evidence_id?: string
    expected_market_time?: string
    market_time?: string
    observed_at?: string
    published_at?: string
    projection_read_latency_ms?: number
    staleness_seconds?: number
    is_serveable?: boolean
    staleness_sessions?: number
    max_serveable_stale_sessions?: number
  }>>
  confluence_zones: ConfluenceZone[]
  as_of: string
}

export const getMultiTradeSetup = async (ticker: string, refresh = false): Promise<MultiTradeSetupResponse> => {
  const params = refresh ? { refresh: true } : undefined
  const response = await api.get(`/stock/${ticker}/trade-setup/multi`, { params })
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
// CROSS-SECTIONAL SIGNAL (validated momentum model)
// ============================================================================

export type DiscoveryState =
  | 'CONTINUATION'
  | 'REVERSAL_WATCH'
  | 'EMERGING_REVERSAL'
  | 'REVERSAL_CONFIRMED'
  | 'CONFLICT'
  | 'LAGGARD'
  | 'NEUTRAL'

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

export interface OptionsEnvelope<T> {
  available: boolean
  reason: string | null
  data_tier: string
  generated_at: string
  as_of: string | null
  observed_at: string | null
  policy_sha256: string | null
  model_version: string | null
  data: T
}

export type OptionEventWindowState = 'BLOCKED' | 'CLEAR' | 'UNAVAILABLE' | 'NOT_APPLICABLE'

export interface OptionCalendarEvent {
  market_event_id: string
  event_type: 'EARNINGS' | 'FED_RATE_DECISION'
  affected_underlying: string | null
  scheduled_time: string
  source: string
  source_key: string
  announcement_time: string | null
  source_observed_at: string
  first_observed_at: string
  confidence: string | number | null
  status: string
  payload_sha256: string
}

export interface OptionCalendarCoverage {
  coverage_id: string
  event_type: 'EARNINGS' | 'FED_RATE_DECISION'
  affected_underlying: string | null
  window_start: string
  window_end: string
  source: string
  source_key: string
  source_observed_at: string
  first_observed_at: string
  payload_sha256: string
}

export interface OptionTickerCalendarData {
  underlyer: string
  asset_type: 'STOCK' | 'ETF'
  configured_source: string | null
  decision_window_start: string
  decision_window_end: string
  earnings_state: OptionEventWindowState
  fed_state: OptionEventWindowState
  events: OptionCalendarEvent[]
  coverage: OptionCalendarCoverage[]
}

export interface OptionFlowOpenInterestChange {
  settlement_session: string
  prior_settlement_session: string
  matched_contract_count: number
  current_contract_count: number
  matched_coverage_fraction: number
  call_open_interest_change: number
  put_open_interest_change: number
  total_open_interest_change: number
}

export interface OptionFlowSummary {
  underlying: string
  asset_type: 'STOCK' | 'ETF'
  batch_id: string
  matrix_id: string
  scheduled_cycle: string
  market_data_time: string
  first_observed_at: string
  received_row_count: number
  retained_row_count: number
  retention_fraction: number
  model_version: string
  spot: string
  contract_count: number
  expiration_count: number
  strike_count: number
  call_contract_count: number
  put_contract_count: number
  call_volume: number
  put_volume: number
  total_volume: number
  call_open_interest: number
  put_open_interest: number
  total_open_interest: number
  call_premium_activity: string
  put_premium_activity: string
  total_premium_activity: string
  premium_activity_contract_count: number
  put_call_volume_ratio: number | null
  put_call_open_interest_ratio: number | null
  open_interest_change: OptionFlowOpenInterestChange | null
}

export interface OptionFlowExpiration {
  expiration_date: string
  calendar_dte: number
  contract_count: number
  call_volume: number
  put_volume: number
  call_open_interest: number
  put_open_interest: number
  call_premium_activity: string
  put_premium_activity: string
}

export interface OptionFlowStrike {
  expiration_date: string
  strike: string
  contract_count: number
  call_volume: number
  put_volume: number
  call_open_interest: number
  put_open_interest: number
}

export interface OptionFlowContract {
  contract_id: number
  contract_ticker: string
  contract_type: 'CALL' | 'PUT'
  expiration_date: string
  calendar_dte: number
  strike: string
  spot: string
  day_volume: number
  open_interest: number | null
  model_mark: string | null
  display_mark: string | null
  local_iv: number | null
  local_delta: number | null
  volume_open_interest_ratio: number | null
  premium_activity: string | null
  moneyness_fraction: string
}

export interface OptionFlowData {
  serving_mode: 'CURRENT_POLICY' | 'HISTORICAL_PREVIOUS_POLICY'
  active_policy_sha256: string
  active_configuration_sha256: string
  underlyers: OptionFlowSummary[]
  selected: string
  session_date: string | null
  selected_summary: OptionFlowSummary | null
  expirations: OptionFlowExpiration[]
  strikes: OptionFlowStrike[]
  top_contracts: OptionFlowContract[]
  directional_flow_available: false
  quote_liquidity: 'NOT_AVAILABLE'
  trade_tape_scope: 'EXCLUDED_FROM_TOTALS_WATCHLIST_BIASED'
  definitions: Record<string, string>
}

export type OptionCandidateStatus = 'SELECTED' | 'SUPPRESSED' | 'REJECTED'
export type OptionCandidatePersona = 'INCOME' | 'DEFINED_RISK_INCOME' | 'MOMENTUM' | 'NEUTRAL_VOL'

export interface OptionCandidateLeg {
  snapshot_id?: string
  batch_id?: string | null
  leg_index: number
  contract_id: number
  contract_ticker: string
  side: 'BUY' | 'SELL'
  ratio: number
  multiplier: number
  expiration_date: string
  strike: string
  contract_type: 'CALL' | 'PUT'
  model_mark: string | null
  local_iv: number | null
  local_delta: number | null
  local_gamma: number | null
  // Available when the reader projects original leg market evidence.
  local_theta_per_day?: number | null
  local_vega_per_vol_point?: number | null
  local_rho_per_rate_point?: number | null
  spot?: string | null
  day_volume?: number | null
  open_interest?: number | null
  source_market_time: string
  mark_source: string
  time_to_expiration_years?: number | null
  risk_free_rate?: number | null
  dividend_yield?: number | null
  model_version?: string | null
  valuation_policy_version?: string | null
  valuation_policy_sha256?: string | null
  normalized_payload_sha256?: string | null
  quality_flags: string[]
  quote_bid: string | null
  quote_ask: string | null
  quote_midpoint: string | null
  quote_spread_midpoint: number | null
}

export interface OptionCandidateRow {
  calendar_dte?: number
  candidate_id: string
  matrix_id: string
  strategy_name: string
  strategy_version: string
  display_name: string
  underlying: string
  candidate_kind: 'RESEARCH_ONLY' | 'SINGLE_CONTRACT' | 'MULTI_LEG'
  strategy_archetype: string
  persona_tags: OptionCandidatePersona[]
  structure_type: string
  structure_risk_class: string
  expiration_date: string | null
  candidate_rank: number
  status: OptionCandidateStatus
  primary_metric_name: string | null
  primary_metric_value: number | null
  rank_components: Record<string, unknown>
  primary_evidence: Record<string, unknown>
  net_premium: string | null
  collateral_required: string | null
  capital_at_risk: string | null
  maximum_profit: string | null
  maximum_loss: string | null
  return_on_collateral: number | null
  return_on_risk: number | null
  breakevens: string[]
  execution_eligibility: 'PAPER_PROXY' | 'LIVE_CANDIDATE' | null
  reason_codes: string[]
  management_policy_version: string | null
  management_policy: Record<string, unknown>
  policy_sha256: string
  model_version: string
  market_data_time: string
  observed_time: string
  valid_until: string | null
  presentation_metadata: Record<string, unknown>
  source_contract_id: number | null
  source_contract_ticker: string | null
  legs: OptionCandidateLeg[]
}

export interface OptionCandidatesData {
  serving_mode: 'CURRENT_POLICY' | 'HISTORICAL_PREVIOUS_POLICY'
  active_policy_sha256: string
  title: 'Weekly Research Candidates'
  rows: OptionCandidateRow[]
  total: number
  limit: number
  offset: number
  status_counts: {
    selected: number
    suppressed: number
    rejected: number
  }
  quote_liquidity: 'NOT_AVAILABLE'
  execution_mode: 'READ_ONLY_RESEARCH'
}

export type OptionDetectorSort = 'triggered_at' | 'run' | 'underlyer' | 'detector' | 'category' | 'strategy' | 'rank' | 'entry_limit'

export interface OptionDetectorDatasets {
  storage_ready: boolean
  datasets: string[]
  dataset_sessions: Record<string, string[]>
  sessions: string[]
  session_datasets: Record<string, string>
  current_session_date: string | null
  current_dataset_id: string | null
  default_dataset_id: string | null
  configured_dataset_id: string | null
  configured_effective_from: string | null
}

export const getOptionDetectorDatasets = async (): Promise<OptionsEnvelope<OptionDetectorDatasets>> => (await api.get('/options/alerts/datasets')).data

export interface OptionAlertSchedule {
  dataset_id: string
  as_of: string
  scheduled_cycle: string
  window_start: string
  window_end: string
  status: 'UPCOMING' | 'DUE'
  warning: boolean
  unpublished_windows: number
  last_unpublished_cycle: string | null
  evaluation_attempts?: {
    available: boolean
    attempts: Array<{ scheduled_cycle: string; started_at: string; finished_at: string | null;
      status: 'RUNNING' | 'FAILED' | 'INCOMPLETE' | 'UNVERIFIED'; reason: string | null }>
  }
  dataset_timing: 'EXPECTED_CHECK_WINDOW_NOT_COMPLETION_DEADLINE'
}

export const getOptionAlertSchedule = async (): Promise<OptionsEnvelope<OptionAlertSchedule>> => (await api.get('/options/alerts/schedule')).data

export interface OptionAlertOriginalPackage {
  status: 'AVAILABLE' | 'UNAVAILABLE'
  basis?: 'ORIGINAL_CANDIDATE_SNAPSHOTS'
  structure_type?: string
  expiration_date?: string | null
  calendar_dte?: number | null
  net_premium?: string | null
  capital_at_risk?: string | null
  maximum_loss?: string | null
  maximum_profit?: string | null
  breakevens?: string[]
  market_data_time?: string
  observed_time?: string
  legs: OptionCandidateLeg[]
}

export interface OptionDetectorRunSummary {
  run_id: string
  scheduled_cycle: string
  selected_at: string
  published_at: string
  market_time: string
  observed_time: string
  expected_underlyings: number
  covered_underlyings: number
  coverage_status?: 'COMPLETE' | 'PARTIAL'
  unavailable_underlyers?: Record<string, string[]>
  selection_counts: Record<string, number>
  rejections: Record<string, number>
}

export interface OptionLocalSurfaceObservation {
  schema_version?: 'option_local_surface_decision_v1'
  contract_type: 'CALL' | 'PUT'
  expiration_date: string
  market_cutoff: string
  decision_at: string
  valid_until: string
  input_count: number
  policy_sha256: string
  source_sha256: string
  finding_disposition: string
  stock_context_status: string
  reasons: string[]
  findings: Array<{ contract_id: number; snapshot_id: string; strike: string; local_iv: number;
    fitted_iv: number; residual: number; robust_z: number }>
}

export interface OptionO1IndicatorObservation {
  schema_version: 'option_o1_indicator_observation_v1' | 'option_o1_indicator_observation_v2' | 'option_o1_indicator_observation_v3'
  baseline_disposition: string
  baseline_reasons: string[]
  volume_oi_ratio: number
  contract_id: number
  snapshot_id: string
  decision_at: string
  valid_until: string
  outcome_status: 'NOT_YET_MEASURED'
  package_status: 'NOT_ASSESSED'
  measurements: Array<{ component_key: string; metric_id: string; unit: string;
    status: 'READY' | 'UNAVAILABLE'; value: number | null; reason_codes: string[] }>
  challengers: Array<{ challenger_id: string; verdict: 'PASS' | 'FAIL' | 'UNAVAILABLE';
    metric_ids: string[]; reason_codes: string[] }>
}

export interface OptionStockSetupIndicatorObservation {
  schema_version: 'option_stock_setup_indicator_observation_v1'
  detector_id: 'S1' | 'S2'
  baseline_disposition: string
  baseline_reasons: string[]
  decision_at: string
  valid_until: string
  setup_source: {
    source_status: 'ACTIVE_AT_SOURCE_READ' | 'EXPIRED_BEFORE_SOURCE_READ'
    publication_market_time: string
    published_at: string
    candidate_payload_sha256: string
  }
  outcome_status: 'NOT_YET_MEASURED'
  package_status: 'NOT_ASSESSED'
  measurements: Array<{ metric_id: string; unit: string;
    status: 'READY' | 'UNAVAILABLE'; value: number | null; reason_codes: string[] }>
  challengers: Array<{ challenger_id: string; verdict: 'PASS' | 'FAIL' | 'UNAVAILABLE';
    metric_ids: string[]; reason_codes: string[] }>
}

export interface OptionO3CreditObservation {
  schema_version: 'option_o3_credit_observation_v1'
  detector_id: 'O3'
  underlyer: string
  direction: -1 | 1
  candidate_rank: number
  structure_type: 'PUT_CREDIT_VERTICAL' | 'CALL_CREDIT_VERTICAL'
  decision_at: string
  valid_until: string
  disposition: 'QUALIFIED_INDICATIVE'
  net_credit: string
  maximum_profit: string
  maximum_loss: string
  breakeven: string
  return_on_risk: string
  structural_invalidation: string
  pricing_basis: 'ORIGINAL_COHERENT_MODEL_MARKS'
  legs: Array<{ leg_index: number; snapshot_id: string; contract_id: number; contract_ticker: string;
    side: 'BUY' | 'SELL'; ratio: number; multiplier: 100; expiration_date: string; strike: string;
    spot: string; model_mark: string; local_iv: number; local_delta: number; local_gamma: number;
    local_theta_per_day: number; local_vega_per_vol_point: number; local_rho_per_rate_point: number;
    day_volume: number | null; open_interest: number | null; bid: string | null; ask: string | null;
    source_market_time: string; mark_source: string; valuation_policy_version: string;
    valuation_policy_sha256: string }>
}

export type OptionDetectorId = 'O1' | 'O2' | 'O3' | 'S1' | 'S2'

export interface OptionDetectorEvaluationReview {
  storage_ready: boolean
  dataset_id: string | null
  datasets: string[]
  sessions: string[]
  session_date: string | null
  as_of: string
  maximum_new_alerts: number
  completed_runs: OptionDetectorRunSummary[]
  outcome_status: string
  total: number
  models: Array<{
    detector_id: OptionDetectorId; evaluated: number; detected: number; selected: number;
    not_selected: number; repeats: number; observations: number;
    measured: number | null; positive_rate: number | null; mean_net_return: number | null; outcome_status: string;
  }>
  rows: Array<{
    evaluation_id: string; candidate_id: string | null; run_id: string; scheduled_cycle: string;
    detector_id: OptionDetectorId; origin: 'OPTIONS_FIRST' | 'STOCK_FIRST'; underlyer: string;
    direction: -1 | 1 | null; category: string; strategy_name: string | null; candidate_rank: number | null;
    selection_status: 'SELECTED' | 'NOT_SELECTED' | 'REPEAT' | 'OBSERVATION'; selection_reason: string;
    plan_sha256: string | null; selected_at: string; entry_deadline: string | null; exit_deadline: string | null;
    entry_limit: string | null; outcome_status: string; net_return: number | null;
    event_horizon_status: string | null; shared_model_exposure: boolean;
    observation?: OptionLocalSurfaceObservation | OptionO1IndicatorObservation | OptionO3CreditObservation | OptionStockSetupIndicatorObservation;
  }>
  cells: Array<{
    detector_id: OptionDetectorId; selection_status: string; selection_reason: string;
    occurrences: number; distinct_packages: number; repeated_package_occurrences: number;
    measured: number | null; positive_rate: number | null; mean_net_return: number | null; outcome_status: string;
  }>
}

export const getOptionDetectorEvaluations = async (params: {
  session_date?: string; dataset_id?: string; detector?: OptionDetectorId;
  underlyer?: string; sort_by?: OptionDetectorSort; sort_order?: 'asc' | 'desc';
  selection_status?: 'SELECTED' | 'NOT_SELECTED' | 'REPEAT' | 'OBSERVATION'; limit?: number; offset?: number;
}): Promise<OptionsEnvelope<OptionDetectorEvaluationReview>> => (await api.get('/options/alerts/evaluations', { params })).data

export interface OptionCurrentMark {
  status: 'FRESH' | 'STALE' | 'UNAVAILABLE'
  reason: string | null
  market_time: string | null
  observed_time: string | null
  oldest_leg_market_time?: string
  age_seconds: number | null
  maximum_age_seconds?: number
  signed_package_mark: string | null
  package_price: string | null
  gross_pnl: string | null
  price_return: string | null
  estimated_cost: string | null
  net_pnl: string | null
  net_return: string | null
  basis: string
  price_return_basis: string
  net_return_basis: string
  valuation_policy_sha256?: string
  source_snapshot_ids?: string[]
  after_exit_deadline?: boolean | null
  slippage: 'UNAVAILABLE'
  execution_permission: false
}

export interface OptionDetectorAlertReview {
  version: 'option_detector_alert_review_v2'
  dataset_id: string
  scope: 'LATEST' | 'HISTORY'
  status: 'COMPLETE' | 'PARTIAL' | 'NO_COMPLETE_RUN' | 'RUNNING' | 'FAILED' | 'INCOMPLETE' | 'UNVERIFIED'
  latest_attempt?: { scheduled_cycle: string; started_at: string; finished_at: string | null;
    status: 'RUNNING' | 'FAILED' | 'INCOMPLETE' | 'UNVERIFIED'; reason: string | null } | null
  as_of: string
  latest_run_id: string | null
  run: OptionDetectorRunSummary | null
  latest_run: OptionDetectorRunSummary | null
  runs: OptionDetectorRunSummary[]
  session_date: string | null
  sessions: string[]
  total: number
  new_alerts: number
  detected_observations?: number
  repeat_hits: number
  maximum_new_alerts: number
  outcome_status: 'NOT_BOUND_TO_PROSPECTIVE_OUTCOMES'
  execution_permission: false
  rows: Array<{
    evaluation_id: string; candidate_id: string; matrix_id: string; run_id: string; scheduled_cycle: string;
    triggered_at: string | null;
    detector_id: OptionDetectorId; origin: 'OPTIONS_FIRST' | 'STOCK_FIRST'; underlyer: string;
    direction: -1 | 1; category: string; strategy_name: string | null; candidate_rank: number | null; first_selected_at: string;
    hit_count: number; repeat_count: number; confirmation_timeframes?: string[]; last_seen_at: string; plan_sha256: string | null;
    management_status: 'MONITORING' | 'TARGET_MET' | 'STOP_LOSS_HIT' | 'EXPIRED' | 'NOT_APPLICABLE' | 'UNAVAILABLE';
    entry_limit: string | null; entry_deadline: string | null; exit_deadline: string | null;
    management_policy: Record<string, unknown>; event_horizon_status: string | null; original_package: OptionAlertOriginalPackage;
    current_mark: OptionCurrentMark | null;
    observation?: OptionLocalSurfaceObservation | OptionO3CreditObservation;
    outcome_status: 'NOT_BOUND_TO_PROSPECTIVE_OUTCOMES'; net_return: null; fill: null;
  }>
}

export const getOptionDetectorAlerts = async (params: {
  dataset_id: string; scope?: 'LATEST' | 'HISTORY'; session_date?: string;
  session_rollup?: boolean;
  underlyer?: string; sort_by?: OptionDetectorSort; sort_order?: 'asc' | 'desc';
  detector?: OptionDetectorId; limit?: number; offset?: number;
}): Promise<OptionsEnvelope<OptionDetectorAlertReview>> => (await api.get('/options/alerts/detector-runs', { params })).data

export interface OptionDiscoveryCatalog {
  version: string
  contract_filters?: Record<string, { label: string; unit: string; minimum?: number; maximum?: number; integer?: boolean }>
  categories: { id: OptionCandidatePersona; label: string }[]
  models: {
    id: string
    label: string
    output_kind: 'STRUCTURE' | 'OBSERVATION'
    structures: { id: string; category_ids: OptionCandidatePersona[] }[]
  }[]
}

export interface EligibleChainRow {
  snapshot_id: string
  contract_id: number
  contract_ticker: string
  underlying: string
  contract_type: 'CALL' | 'PUT'
  expiration_date: string
  strike: string
  calendar_dte: number
  spot: string
  model_mark: string
  absolute_delta: number
  local_delta: number
  local_iv: number
  local_gamma: number
  local_theta_per_day: number
  local_vega_per_vol_point: number
  local_rho_per_rate_point?: number | null
  bid?: string | null
  ask?: string | null
  mark_market_data_time?: string | null
  day_volume: number | null
  open_interest: number | null
  volume_open_interest_ratio: number | null
  otm_fraction: string
  market_data_time: string
  first_observed_at: string
  mark_source: string
  quality_flags: string[]
  asset_type?: string | null
  provider?: string | null
  batch_id?: string | null
  matrix_id?: string | null
  expiration_cutoff?: string | null
  time_to_expiration_years?: number | null
  shares_per_contract?: number | null
  exercise_style?: string | null
  spot_market_data_time?: string | null
  display_mark?: string | null
  midpoint?: string | null
  intrinsic_value?: string | null
  extrinsic_value?: string | null
  single_contract_breakeven?: string | null
  provider_iv?: number | null
  provider_gamma?: number | null
  risk_free_rate?: number | null
  dividend_yield?: number | null
  iv_converged?: boolean | null
  iv_solver?: string | null
  iv_iteration_count?: number | null
  iv_price_error?: number | null
  iv_failure_reason?: string | null
  model_version?: string | null
  revised_observed_at?: string | null
  data_delay_seconds?: number | null
  revision?: number | null
  policy_version?: string | null
  policy_sha256?: string | null
  valuation_policy_version?: string | null
  valuation_policy_sha256?: string | null
  raw_payload_sha256?: string | null
  normalized_payload_sha256?: string | null
  created_at?: string | null
  updated_at?: string | null
}

export interface EligibleChainRequest {
  session_date?: string
  underlyer?: string
  contract_type?: 'CALL' | 'PUT'
  filters: { field: string; minimum?: number; maximum?: number }[]
  sort: string
  descending: boolean
  limit: number
  offset: number
}

export interface EligibleChainData {
  rows: EligibleChainRow[]
  total: number
  coverage: { underlying: string; batch_id: string; matrix_id: string; market_time: string; observed_time: string; received: number; retained: number; eligible: number }[]
  missing_underlyers?: string[]
  requested_underlyers?: string[]
  coverage_status?: 'MISSING' | 'PARTIAL' | 'COVERED'
  selection_basis?: 'LATEST_COMPLETE_MATRIX_PER_UNDERLYING'
  serving_mode?: 'CURRENT_POLICY'
}

export interface OptionScenarioResult {
  scenario_result_id: string
  candidate_id: string
  scenario_key: string
  spot_shock_fraction: number
  iv_shock_fraction: number
  time_fraction_remaining: number
  repriced_value: string | null
  profit_loss: string | null
  delta: number | null
  gamma: number | null
  theta_per_day: number | null
  vega_per_vol_point: number | null
  terminal: boolean
  assumptions: Record<string, unknown>
  quality_flags: string[]
}

export interface OptionExecutionGate {
  ledger_version: string
  gate_name: string
  verdict: 'PASS' | 'FAIL' | 'UNAVAILABLE'
  blocking: boolean
  reason_codes: string[]
  evidence: Record<string, unknown>
  evaluated_at: string
}

export interface OptionStockBehaviorGateEvidence {
  gate_id: string
  requirement: 'REQUIRED' | 'ADVISORY'
  verdict: 'PASS' | 'FAIL' | 'UNAVAILABLE' | 'NOT_APPLICABLE'
  factor: string | null
  component_key: string | null
  metric_id: string | null
  formula_id: string | null
  comparator: string
  actual_float: number | null
  actual_text: string | null
  threshold_float: number | null
  source_evidence_ids: string[]
  source_payload_sha256s: string[]
  source_market_times: string[]
  reason_codes: string[]
}

export interface OptionStockBehaviorAssessmentEvidence {
  schema_version: 'option_stock_behavior_assessment_v1'
  detector_policy_version?: string
  detector_policy_sha256?: string
  stock_profile?: string | null
  launch_id?: string | null
  launch_manifest_sha256?: string | null
  disposition: 'ELIGIBLE_RESEARCH' | 'BLOCKED' | 'UNAVAILABLE' | 'NOT_APPLICABLE'
  directional_thesis: string | null
  stock_market_cutoff: string | null
  decision_at: string
  target_horizon: string
  behavior_data_status: string | null
  behavior_alignment_state: string | null
  gates: OptionStockBehaviorGateEvidence[]
  assessment_only: true
  execution_permission: false
}

export interface OptionPackageTermsEvidence {
  schema_version: 'option_package_terms_v1'
  structure: string
  entry_basis: 'CANDIDATE_MODEL_MARKS'
  market_time: string
  observed_at: string
  valid_until: string | null
  net_premium: string
  collateral_required: string | null
  capital_at_risk: string | null
  maximum_loss_status: 'BOUNDED' | 'UNBOUNDED' | 'UNAVAILABLE'
  maximum_loss: string | null
  maximum_profit_status: 'BOUNDED' | 'UNBOUNDED' | 'UNAVAILABLE'
  maximum_profit: string | null
  breakevens: string[]
  research_only: true
  execution_permission: false
}

export interface OptionPackageAssessmentEvidence {
  schema_version: 'option_package_assessment_v1'
  assessment_policy_version: string
  assessment_policy_sha256: string
  valuation_policy_sha256: string
  assessed_at: string
  status: 'READY' | 'UNAVAILABLE'
  package: OptionPackageTermsEvidence | null
  package_terms_sha256: string | null
  reason_codes: string[]
  research_only: true
  execution_permission: false
}

export interface OptionCandidateResearchEvidence {
  version: 'option_candidate_research_evidence_v1'
  detector: {
    id: string
    version: string
    display_name: string
    output_kind: 'OBSERVATION' | 'STRUCTURED_PACKAGE'
  }
  category_ids: OptionCandidatePersona[]
  structure: string
  stock_behavior: {
    availability: 'AVAILABLE' | 'UNAVAILABLE'
    reason: string | null
    payload_sha256?: string
    recorded_at?: string
    assessment: OptionStockBehaviorAssessmentEvidence | null
  }
  package_assessment: {
    availability: 'AVAILABLE' | 'UNAVAILABLE'
    reason: string | null
    payload_sha256?: string
    recorded_at?: string
    assessment: OptionPackageAssessmentEvidence | null
  }
  probability: {
    status: 'UNAVAILABLE'
    value: null
    reason: string
    target: null
    outcome_basis: null
    report_sha256: null
  }
  source_basis: 'PERSISTED_EVIDENCE_ONLY'
  calculation_performed: false
  publication_permission: false
  execution_permission: false
}

export interface OptionCandidateDetailData {
  candidate: Omit<OptionCandidateRow, 'legs'> & {
    context_status: string | null
    trend_state: string | null
    earnings_blackout_state: string | null
    fed_blackout_state: string | null
    quote_spread_state: string | null
    context_reason_codes: string[] | null
    normalized_legs: Array<Record<string, unknown>>
    decision_context: Record<string, unknown>
    evidence_rank_components: Record<string, unknown>
    trigger_values: Record<string, unknown>
    evidence_quality_flags: string[]
    signal_id: string | null
    signal_status: string | null
    signal_blocked_reasons: string[] | null
  }
  legs: OptionCandidateLeg[]
  scenarios: OptionScenarioResult[]
  execution_gates: OptionExecutionGate[]
  market_event_evidence: Array<{
    market_event_id: string
    event_type: 'EARNINGS' | 'FED_RATE_DECISION'
    affected_underlying: string | null
    scheduled_time: string
    source: string
    source_key: string
    announcement_time: string | null
    source_observed_at: string | null
    first_observed_at: string
    confidence: 'CONFIRMED' | 'ESTIMATED' | 'UNKNOWN'
    status: 'SCHEDULED' | 'COMPLETED' | 'CANCELED' | 'REVISED'
    payload_sha256: string
  }>
  event_coverage_evidence: Array<{
    coverage_id: string
    event_type: 'EARNINGS' | 'FED_RATE_DECISION'
    affected_underlying: string | null
    window_start: string
    window_end: string
    source: string
    source_key: string
    source_observed_at: string | null
    first_observed_at: string
    payload_sha256: string
  }>
  research_evidence?: OptionCandidateResearchEvidence
  quote_liquidity: 'NOT_AVAILABLE'
  execution_mode: 'READ_ONLY_RESEARCH'
}

export interface OptionCurrentAdvancedQuote {
  contract_id: number
  contract_ticker: string
  quote_time: string
  received_at: string
  bid: string
  ask: string
  bid_size: number | null
  ask_size: number | null
  updated_at: string
}

export interface OptionCurrentAdvancedQuotesData {
  quotes: OptionCurrentAdvancedQuote[]
  requested_contract_ids: number[]
  missing_contract_ids: number[]
  source: 'PERSISTED_ADVANCED_QUOTE_CURRENT'
  provider_fetch_performed: false
  execution_permission: false
}

export const getOptionEventCalendar = async (
  underlyer: string,
  daysForward = 30,
): Promise<OptionsEnvelope<OptionTickerCalendarData>> => {
  const response = await api.get(`/options/calendar/${underlyer}`, {
    params: { days_forward: daysForward },
  })
  return response.data
}

export const getOptionFlow = async (
  underlyer?: string,
  sessionDate?: string,
): Promise<OptionsEnvelope<OptionFlowData>> => {
  const response = await api.get('/options/flow', { params: { underlyer, session_date: sessionDate } })
  return response.data
}

export const getOptionCandidates = async (params?: {
  category?: OptionCandidatePersona
  session_date?: string
  structured_only?: boolean
  minimum_dte?: number
  maximum_dte?: number
  maximum_capital?: number
  sort?: 'DEFAULT' | 'CAPITAL_ASC' | 'DTE_ASC'
  underlyer?: string
  persona?: OptionCandidatePersona
  status?: OptionCandidateStatus
  strategy?: string
  risk_class?: string
  expiration?: string
  limit?: number
  offset?: number
}): Promise<OptionsEnvelope<OptionCandidatesData>> => {
  const response = await api.get('/options/candidates', { params })
  return response.data
}

export const getOptionDiscoveryCatalog = async (): Promise<OptionDiscoveryCatalog> => {
  const response = await api.get('/options/discovery-catalog')
  return response.data
}

export const queryEligibleChain = async (request: EligibleChainRequest): Promise<OptionsEnvelope<EligibleChainData>> => {
  const response = await api.post('/options/eligible-chain/query', request)
  return response.data
}

export const getOptionCandidate = async (
  candidateId: string,
): Promise<OptionsEnvelope<OptionCandidateDetailData>> => {
  const response = await api.get(`/options/candidates/${candidateId}`)
  return response.data
}

export const getOptionCurrentAdvancedQuotes = async (
  contractIds: number[],
): Promise<OptionsEnvelope<OptionCurrentAdvancedQuotesData>> => {
  const response = await api.get('/options/quotes/current', { params: { contract_id: contractIds } })
  return response.data
}

export interface OptionAlertAssessment {
  capability: 'OBSERVATION' | 'INDICATIVE' | 'QUOTE_PAPER' | 'EXECUTION'
  status: 'SATISFIED' | 'BLOCKED' | 'UNAVAILABLE' | 'NOT_APPLICABLE'
  blocking_inputs: string[]
}

export interface OptionAlertManagement {
  policy_version: string
  strategy_name: 'DIRECTIONAL_LONG_PREMIUM' | 'DIRECTIONAL_DEBIT_SPREAD'
  stop_loss_fraction: string
  take_profit_fraction: string
  maximum_hold_seconds: number
  minimum_exit_dte: number
}

export interface OptionAlertPreviewRequest {
  entry_deadline?: string
  exit_deadline?: string
  entry_limit?: string
  management_policy?: OptionAlertManagement
}

export interface OptionAlertPreviewData {
  version: string
  status: 'NOT_APPLICABLE' | 'BLOCKED' | 'INDICATIVE_PLAN_VALID'
  candidate_id: string
  assessed_at: string
  original_valid_until: string | null
  source_market_time: string
  source_observed_at: string
  original_package: { legs: OptionCandidateLeg[]; economics: Record<string, unknown> }
  original_management_policy: Record<string, unknown> | null
  original_management_policy_version: string | null
  proposed_management_policy: Record<string, unknown> | null
  qualification: { assessments: OptionAlertAssessment[]; execution_permission: false }
  blockers: Array<{ code: string; field?: string; inputs?: string[]; message?: string }>
  plan_preview: { plan_id: string; plan_sha256: string; source_evidence_omitted: true; plan: Record<string, unknown> } | null
  persisted: false
  publication_permission: false
  execution_permission: false
  paper_position_created: false
}

export const previewOptionAlert = async (candidateId: string, request: OptionAlertPreviewRequest = {}): Promise<OptionsEnvelope<OptionAlertPreviewData>> => {
  const response = await api.post(`/options/alerts/preview/${candidateId}`, request)
  return response.data
}


export interface AccountEnvelope {
  available: boolean
  status: 'DISABLED'
  reason: string
  generated_at: string
  planned_capabilities: string[]
  user: null
}

// The credential routes answer 501 by design, so the typed envelope arrives on the error path.
const postAccountRequest = async (path: string, payload: unknown): Promise<AccountEnvelope> => {
  try {
    const response = await api.post<AccountEnvelope>(path, payload)
    return response.data
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 501 && error.response.data) {
      return error.response.data as AccountEnvelope
    }
    throw error
  }
}

export const requestSignIn = (payload: { email: string; password: string }) =>
  postAccountRequest('/auth/signin', payload)

export const requestSignUp = (payload: { email: string; display_name: string; password: string }) =>
  postAccountRequest('/auth/signup', payload)

export const getAccountSession = async (): Promise<AccountEnvelope> => {
  const response = await api.get<AccountEnvelope>('/auth/session')
  return response.data
}


export default api
