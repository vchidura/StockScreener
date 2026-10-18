import api from './api'

export type RealtimeValidationStatus = {
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

export async function getRealtimeOptionAlerts(cohortId?: string, signal?: AbortSignal): Promise<RealtimeOptionAlerts> {
  return (await api.get('/realtime/option-alerts', { signal, params: cohortParams(cohortId) })).data
}

export async function getRealtimeEquityValidationStatus(cohortId?: string, signal?: AbortSignal): Promise<RealtimeValidationStatus> {
  return (await api.get('/realtime/equity-validation', {
    signal,
    params: cohortParams(cohortId),
  })).data
}