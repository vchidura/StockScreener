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