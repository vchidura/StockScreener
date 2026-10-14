import api from './api'

export type RealtimeCanaryStatus = {
  status: 'READY' | 'BLOCKED'
  checked_at: string
  database_target: string
  universe_version: string
  universe_sha256: string
  expected_tickers: string[]
  ready_tickers: string[]
  missing_tickers: string[]
  screening_tickers: string[]
  stock_alerts_available: boolean
  option_alerts_available: boolean
  option_alerts_status?: string
  provider_fetch_performed: boolean
  canonical_fallback: boolean
  reason_codes: string[]
}

export type RealtimeStoredValue = Record<string, unknown>

export const REALTIME_INTERVALS = ['30m', '1h', '1d'] as const
export type RealtimeInterval = typeof REALTIME_INTERVALS[number]
export type RealtimeTechnicalValues = Record<string, number | string | null>
export type RealtimeDetectorState = { status: string; reason_codes: string[] }
export type RealtimeTickerPayload = {
  ticker: string
  technicals: RealtimeTechnicalValues
  technical_intervals: Record<RealtimeInterval, RealtimeTechnicalValues>
  detectors: Record<string, RealtimeDetectorState>
  validation_scope: string
  split_safe: boolean
  adjusted_discontinuities: Array<Record<string, unknown>>
  provider_fetch_performed: boolean
}
export type RealtimeTickerPage = {
  snapshot_id: string
  payload: RealtimeTickerPayload
  generated_at: string
  source_generation: number
  source_manifest_sha256: string
  provider_tier: string
  is_fresh: boolean
  is_serveable: boolean
  provider_fetch_performed: boolean
}
export type RealtimeScreeningRow = RealtimeTickerPayload & {
  security_id: string
  eligible: boolean
}
export type RealtimeScreening = {
  available: boolean
  reason: string | null
  rows: RealtimeScreeningRow[]
  manifest?: Record<string, unknown>
  generation_id?: string
  session_date?: string
  generated_at?: string
  provider_tier?: string
  provider_fetch_performed?: boolean
}
export type RealtimeStockObservation = {
  ticker: string
  detector_id: string
  selected: boolean
  payload: RealtimeDetectorState
}
export type RealtimeStockAlerts = {
  available: boolean
  reason: string | null
  alerts: Array<Record<string, unknown>>
  observations: RealtimeStockObservation[]
  schedule_streams: Array<Record<string, unknown>>
  sessions: string[]
  source: string
  provider_tier: string
  provider_fetch_performed: boolean
}
export type RealtimeOptionAlerts = {
  available: boolean
  reason: string | null
  data_tier: string
  generated_at: string
  data: { evaluations?: Array<Record<string, unknown>> } | null
}
export type RealtimeHealth = {
  status: 'READY'
  database_target: string
  canonical_fallback: boolean
  provider_fetch_performed: boolean
}

export const REALTIME_COHORT_ID = import.meta.env.VITE_REALTIME_COHORT_ID?.trim() ?? ''

const cohortParams = (cohortId?: string) => cohortId ? { cohort_id: cohortId } : undefined

export async function getRealtimeHealth(signal?: AbortSignal): Promise<RealtimeHealth> {
  return (await api.get('/realtime/health', { signal })).data
}

export async function getRealtimeCanaryStatus(cohortId?: string, signal?: AbortSignal): Promise<RealtimeCanaryStatus> {
  return (await api.get('/realtime/canary', {
    signal,
    params: cohortParams(cohortId),
  })).data
}

export async function getRealtimeTickerPage(ticker: string, cohortId?: string, signal?: AbortSignal): Promise<RealtimeTickerPage> {
  return (await api.get(`/realtime/tickers/${ticker}`, { signal, params: cohortParams(cohortId) })).data
}

export async function getRealtimeScreening(cohortId?: string, signal?: AbortSignal): Promise<RealtimeScreening> {
  return (await api.get('/realtime/screening', { signal, params: cohortParams(cohortId) })).data
}

export async function getRealtimeStockAlerts(cohortId?: string, signal?: AbortSignal): Promise<RealtimeStockAlerts> {
  return (await api.get('/realtime/stock-alerts', { signal, params: cohortParams(cohortId) })).data
}

export async function getRealtimeOptionAlerts(cohortId?: string, signal?: AbortSignal): Promise<RealtimeOptionAlerts> {
  return (await api.get('/realtime/option-alerts', { signal, params: cohortParams(cohortId) })).data
}

export async function getRealtimeEquityValidationStatus(cohortId?: string, signal?: AbortSignal): Promise<RealtimeCanaryStatus> {
  return (await api.get('/realtime/equity-validation', {
    signal,
    params: cohortParams(cohortId),
  })).data
}