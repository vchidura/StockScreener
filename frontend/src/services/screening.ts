import api from './api'
import type { GapDetails, GapPredicate, HourlyDetails, ScreeningCatalog, ScreeningRequest, ScreeningResult } from '../pages/screeningModel'

const screeningParams = () => {
  if (import.meta.env.VITE_REALTIME_ONLY !== 'true') return undefined
  const cohort_id = import.meta.env.VITE_REALTIME_COHORT_ID?.trim()
  if (!cohort_id) throw new Error('Realtime screening requires VITE_REALTIME_COHORT_ID')
  return { cohort_id }
}

export const getScreeningCatalog = async (signal?: AbortSignal): Promise<ScreeningCatalog> => (await api.get('/stocks/screening/catalog', { signal, params: screeningParams() })).data
export const queryScreening = async (request: ScreeningRequest, signal?: AbortSignal): Promise<ScreeningResult> => (await api.post('/stocks/screening/query', request, { signal, params: screeningParams() })).data
export const getGapDetails = async (generation: string, security_id: string, gap: GapPredicate | null, signal?: AbortSignal): Promise<GapDetails> => (await api.post('/stocks/screening/gap-details', { generation, security_id, gap }, { signal, params: screeningParams() })).data
export const getHourlyDetails = async (generation: string, security_id: string, signal?: AbortSignal): Promise<HourlyDetails> => (await api.post('/stocks/screening/hourly-details', { generation, security_id }, { signal, params: screeningParams() })).data