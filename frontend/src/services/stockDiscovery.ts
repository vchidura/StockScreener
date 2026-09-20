import api from './api'

export type AlertSource = 'SHADOW' | 'REPLAY' | 'LEGACY'

export interface AlertPublication {
  run_id: string
  strategy_instance_id?: string
  strategy_label?: string
  session: string
  trigger_at: string
  published_at: string
  status: string
  expected: number
  missing: number | null
  selected: number
  conflicts: number | null
}

export interface AlertContextFactor {
  status: string
  reason_codes: string[]
  market_time?: string | null
  observed_at?: string | null
  created_at?: string | null
  available_at?: string | null
  source_revision_ids: string[]
  source_snapshot_sha256?: string
  source_snapshot_session?: string
  source_lineage_ref?: string
  persistence?: { status: string; observations: number }
  expected_observations?: number
  timely_observations?: number
  value: {
    direction?: string
    return1?: number
    return5?: number
    return20?: number
    state?: string
    relative5?: number
    relative20?: number
    relative_change5?: number
    labels?: string[]
    advancing?: number
    declining?: number
    above_sma50_fraction?: number
    above_sma200_fraction?: number
    annualized_volatility20?: number
    level?: number
    percentile?: number
    score?: number
    relative_volume?: number
    disposition?: string
    live_gate_enabled?: boolean
    blocking_factors?: string[]
    unknown_factors?: string[]
    stock_minus_sector20?: number
    sector_minus_spy20?: number
    risk?: string
    coverage_complete?: boolean
    timing_uncertain?: boolean
    events?: { type: string; scheduled_time: string; confidence: string; relative_timing?: string }[]
    reports?: { report_id: string; timeframe: string; period_end: string; filing_date: string; age_days: number;
      stale: boolean; source: string; accession_number: string | null; quality_codes: string[]; units: string;
      metrics: Record<string, number | null> }[]
  } | null
}

export interface AlertPublicationContext {
  status: 'AVAILABLE' | 'UNAVAILABLE'
  reason: string | null
  input_cutoff?: string
  publication_at?: string
  assembled_at?: string
  capture_mode?: string
  bundle_sha256?: string
  source_publication_sha256?: string
  factors: Record<string, AlertContextFactor>
}

export interface AlertPlanRow {
  alert_id: string
  strategy_instance_id?: string
  strategy_label?: string
  original_alert_id?: string
  original_source_id?: string
  original_run_id?: string
  opposing_exposure?: boolean
  run_id: string
  security_id: string
  ticker: string
  company_name: string | null
  direction: number
  model: string
  interval: string
  lane: 'TRADE' | 'WATCH'
  triggered_at: string
  published_at: string
  trigger_price: number | null
  entry_price: number | null
  entry_at: string | null
  stop: number | null
  target: number | null
  risk_pct: number | null
  reward_risk: number | null
  entry_risk: { risk_pct: number | null; reward_risk: number | null } | null
  hold: string
  trade_style?: 'SWING' | 'INTRADAY'
  setup_interval?: string
  confirmation_interval?: string
  daily_setup_at?: string
  daily_setup_id?: string
  holding_sessions?: number
  holding_count?: string
  exit_due_at: string | null
  status: string
  reason: string | null
  exit_at: string | null
  exit_price: number | null
  paper_return: number | null
  price_return: number | null
  price_return_status: string
  price_return_basis: string
  latest_price: number | null
  latest_price_at: string | null
  latest_price_interval?: string | null
  latest_price_revision_id?: string | null
  latest_price_observed_at?: string | null
  latest_price_created_at?: string | null
  latest_price_checked_at?: string | null
  latest_price_source?: string | null
  mark_price: number | null
  mark_at: string | null
  hits: number | null
  first_seen: string | null
  last_seen: string | null
  hit_models: string[]
  hit_intervals: string[]
  plan_count?: number
  display_models?: string[]
  display_intervals?: string[]
  plan_variants?: Array<{
    alert_id: string
    model: string
    interval: string
    triggered_at: string
    trigger_price: number | null
    stop: number | null
    target: number | null
    status: string
    entry_price: number | null
    exit_price: number | null
    paper_return: number | null
    reason: string | null
  }>
  indicators: Record<string, number | string | null>
  indicator_at: string
  indicator_interval: string
  daily_context_at: string | null
  indicator_status: string
  warnings: string[]
  policy_version: string
  context?: AlertPublicationContext
}

export interface AlertViewResponse {
  schema: string
  source: AlertSource
  source_id: string | null
  source_label: string
  as_of: string | null
  price_as_of?: string | null
  next_publication_at?: string | null
  publication_mode?: string | null
  publication_window_start?: string | null
  publication_deadline?: string | null
  status: string
  sessions: string[]
  session: string | null
  view: 'latest' | 'history' | 'open'
  combined?: boolean
  latest_runs?: AlertPublication[]
  strategy_streams?: Array<{ stream: string; label: string; instance_id?: string; source_id?: string; status: string; error?: string | null;
    as_of?: string; checked_at?: string; imported_at?: string; source_age_seconds?: number; projector_stale?: boolean;
    publication_window_start?: string; publication_deadline?: string }>
  run: AlertPublication | null
  runs: AlertPublication[]
  withheld_run?: AlertPublication | null
  rows: AlertPlanRow[]
  total: number
  offset: number
  limit: number
  counts: Record<string, number>
  hit_coverage: string | null
  warnings: string[]
  outcome_policy: string | null
  indicators_available: string[]
}

export const getAlertView = async (params: Record<string, string | number | boolean | undefined>) =>
  (await api.get<AlertViewResponse>('/stocks/alert-view', { params })).data

export type StockEodSelection = 'SELECTED' | 'NOT_SELECTED' | 'REPEAT'

export interface StockEodReviewRow {
  candidate_id: string
  episode_id: string
  ticker: string
  security_id: string
  direction: -1 | 1
  trade_type: 'INTRADAY' | 'SWING'
  strategy_label: string
  model: 'resumption' | 'acceptance' | 'failure'
  interval: '30m' | '1h' | '1d'
  selection_status: StockEodSelection
  selection_reason: string
  reason_counts: Record<string, number>
  occurrences: number
  repeat_occurrences: number
  first_seen: string
  last_seen: string
  model_rank: number | null
  pool_size: number | null
  priority_rank: number | null
  priority_pool_size: number | null
  ranking_basis: string
  trigger_at: string
  trigger_price: number
  stop: number
  target: number
  risk_pct: number | null
  reward_risk: number | null
  rs_percentile: number
  extension_atr: number
  momentum: number
  liquidity: number
  policy_version: string
  outcome_status: string | null
  paper_return: number | null
}

export interface StockEodReview {
  schema: 'stock_alert_eod_review_v1'
  storage_ready: boolean
  as_of: string
  session_date: string | null
  sessions: string[]
  completed_runs: number
  missed_runs: number
  detected_occurrences: number
  unique_candidates: number
  outcome_status: 'DESCRIPTIVE_CURRENT_PAPER_OUTCOMES_NOT_CALIBRATED'
  execution_permission: false
  review_signals: Array<
    | { code: 'COVERAGE_LIMITED'; severity: 'CAUTION'; completed: number; missed: number; total_runs: number; completion_rate: number }
    | { code: 'ALLOCATION_PRESSURE'; severity: 'CAUTION'; candidates: number; top_three_priority: number }
    | { code: 'RANK_ORDER_INVERSION'; severity: 'REVIEW'; model: 'resumption' | 'acceptance' | 'failure'; rank1_measured: number; rank1_mean_return: number; rank2_3_measured: number; rank2_3_mean_return: number }
    | { code: 'LOW_FILL_CONVERSION'; severity: 'REVIEW'; model: 'resumption' | 'acceptance' | 'failure'; fill_rate: number; entered: number; no_fill: number; resolved: number }
    | { code: 'ALLOCATION_CONCENTRATION'; severity: 'REVIEW'; dimension: 'direction' | 'interval'; value: string | number; share: number; selected: number }
  >
  diagnostics: {
    coverage: { completed: number; missed: number; completion_rate: number | null }
    conversion: StockEodOutcomeSummary
    rank_buckets: Array<StockEodOutcomeSummary & { model: 'resumption' | 'acceptance' | 'failure'; bucket: 'RANK_1' | 'RANK_2_3' | 'RANK_4_PLUS' }>
    bottlenecks: Array<{ reason: string; category: 'ALLOCATION' | 'PLAN_GEOMETRY' | 'TIMING_OR_DATA'; candidates: number; ranked: number; top_three_priority: number; median_priority: number | null }>
    direction_mix: Array<StockEodOutcomeSummary & { value: -1 | 1; share: number | null }>
    interval_mix: Array<StockEodOutcomeSummary & { value: '30m' | '1h' | '1d'; share: number | null }>
    model_mix: Array<StockEodOutcomeSummary & { value: 'resumption' | 'acceptance' | 'failure'; share: number | null }>
    limitations: string[]
  }
  models: Array<{
    model: 'resumption' | 'acceptance' | 'failure'
    detected_occurrences: number
    unique_candidates: number
    selected: number
    not_selected: number
    repeats: number
    entered: number
    closed: number
    open: number
    no_fill: number
    pending: number
    unavailable: number
    fill_rate: number | null
    measured: number
    positive_rate: number | null
    mean_return: number | null
    median_return: number | null
    top_reasons: Array<{ reason: string; count: number }>
    ranking_basis: string
  }>
  total: number
  offset: number
  limit: number
  rows: StockEodReviewRow[]
}

export interface StockEodOutcomeSummary {
  selected: number
  entered: number
  closed: number
  open: number
  no_fill: number
  pending: number
  unavailable: number
  fill_rate: number | null
  measured: number
  positive_rate: number | null
  mean_return: number | null
  median_return: number | null
}

export const getStockEodReview = async (params: Record<string, string | number | undefined>) =>
  (await api.get<StockEodReview>('/stocks/alert-eod-review', { params })).data