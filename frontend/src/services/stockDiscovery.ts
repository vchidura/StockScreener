import api from './api'

export type AlertSource = 'SHADOW' | 'REPLAY' | 'LEGACY'

export interface AlertPublication {
  run_id: string
  session: string
  trigger_at: string
  published_at: string
  status: string
  expected: number
  missing: number | null
  selected: number
  conflicts: number | null
}

export interface AlertPlanRow {
  alert_id: string
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
  view: 'latest' | 'history'
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