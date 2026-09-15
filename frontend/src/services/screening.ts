import api from './api'
import type { GapDetails, GapPredicate, HourlyDetails, ScreeningCatalog, ScreeningRequest, ScreeningResult } from '../pages/screeningModel'

export const getScreeningCatalog = async (signal?: AbortSignal): Promise<ScreeningCatalog> => (await api.get('/stocks/screening/catalog', { signal })).data
export const queryScreening = async (request: ScreeningRequest, signal?: AbortSignal): Promise<ScreeningResult> => (await api.post('/stocks/screening/query', request, { signal })).data
export const getGapDetails = async (generation: string, security_id: string, gap: GapPredicate | null, signal?: AbortSignal): Promise<GapDetails> => (await api.post('/stocks/screening/gap-details', { generation, security_id, gap }, { signal })).data
export const getHourlyDetails = async (generation: string, security_id: string, signal?: AbortSignal): Promise<HourlyDetails> => (await api.post('/stocks/screening/hourly-details', { generation, security_id }, { signal })).data