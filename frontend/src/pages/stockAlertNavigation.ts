import type { AlertSource } from '../services/stockDiscovery'

export type AlertView = 'latest' | 'history' | 'open'

export function alertSort(params: URLSearchParams, view: AlertView) {
  return {
    sort: params.get('sort') || (view === 'history' ? 'triggered_at' : 'published_at'),
    descending: params.get('ascending') !== '1',
  }
}

export const tradeAlertModels = ['resumption', 'acceptance', 'failure']
export const tradeAlertStatuses = ['PENDING', 'OPEN', 'CLOSED', 'NO_FILL', 'UNRESOLVED']
export const legacyAlertStatuses = ['OPEN_PAPER', 'CLOSED_PAPER', 'WAITING_FOR_ENTRY', 'WAITING_FOR_EVALUATION',
  'ENTRY_UNAVAILABLE', 'EXIT_UNAVAILABLE', 'PATH_UNAVAILABLE', 'NON_TRADING_PATH',
  'CORPORATE_ACTION_UNRESOLVED', 'IDENTITY_UNAVAILABLE']

export function alertTradeFilters(params: URLSearchParams, source: AlertSource) {
  const direction = params.get('direction')
  const model = params.get('model')
  const status = params.get('status')
  const models = source === 'LEGACY' ? ['legacy_daily'] : tradeAlertModels
  const statuses = source === 'LEGACY' ? [...tradeAlertStatuses, ...legacyAlertStatuses] : tradeAlertStatuses
  return {
    lane: 'TRADE' as const,
    direction: direction === '1' || direction === '-1' ? direction : undefined,
    model: model && models.includes(model) ? model : undefined,
    status: status && statuses.includes(status) ? status : undefined,
  }
}

export function resolveAlertRoute(params: URLSearchParams): { source: AlertSource; view: AlertView } {
  const view = params.get('view') === 'history' ? 'history' : params.get('view') === 'open' ? 'open' : 'latest'
  const requested = params.get('source')
  const source = requested === 'REPLAY' || requested === 'LEGACY' || requested === 'SHADOW'
    ? requested : view === 'history' ? 'REPLAY' : 'SHADOW'
  return { source: view === 'open' ? 'SHADOW' : source, view }
}

export function alertSourceParams(source: AlertSource, view: AlertView): URLSearchParams {
  const next = new URLSearchParams({ source })
  if (view !== 'latest') next.set('view', view)
  return next
}

export function alertTabParams(params: URLSearchParams, view: AlertView, forwardEnrolled = false): URLSearchParams {
  const next = new URLSearchParams(params)
  if (forwardEnrolled && !params.has('source')) next.set('source', 'SHADOW')
  if (view !== 'latest') next.set('view', view)
  else next.delete('view')
  if (view === 'open') {
    next.set('source', 'SHADOW')
    next.delete('session_date')
    next.delete('status')
  }
  next.delete('run')
  next.delete('offset')
  if (resolveAlertRoute(params).view !== view) {
    next.delete('sort')
    next.delete('ascending')
  }
  if (resolveAlertRoute(params).source !== resolveAlertRoute(next).source) {
    next.delete('session_date')
  }
  return next
}