import { useEffect, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { DashboardPublication } from '../services/api'

const isTickerPath = (path: string) => /^\/ticker\/[^/]+(?:\/|$)/.test(path)
export const isTickerWorkspaceQuery = (name: string) => name.startsWith('ticker-workspace:')

export function publicationPath(path: string, snapshotId?: string, cohortId?: string, interval?: '1d' | '1h') {
  if (import.meta.env.VITE_REALTIME_ONLY !== 'true' || (snapshotId === undefined && cohortId === undefined)) return path
  const cohort = cohortId ?? import.meta.env.VITE_REALTIME_COHORT_ID?.trim()
  if (!cohort) throw new Error('Realtime publication requires VITE_REALTIME_COHORT_ID')
  const params = new URLSearchParams({ cohort_id: cohort })
  // The ticker page always shows the current view, so a release pin never travels to it.
  if (snapshotId !== undefined && !isTickerPath(path)) params.set('snapshot_id', snapshotId)
  if (interval === '1h') params.set('interval', interval)
  return `${path}?${params}`
}

export function isDailyStrategyPath(path: string) {
  return /^\/(gaps|ma-crossover|momentum-pullback|bearish-bounce|fibonacci)(?:\/|$)/.test(path)
    || path.startsWith('/strategy-research/') || path === '/stocks/persistence' || path === '/strategies/overview'
}

export function navigationPublicationPath(path: string, sourcePath: string, snapshotId?: string, cohortId?: string, interval?: '1d' | '1h') {
  const sourceTicker = /^\/ticker\/([^/]+)(?:\/|$)/.exec(sourcePath)?.[1]?.toUpperCase()
  const targetTicker = /^\/ticker\/([^/]+)(?:\/|$)/.exec(path)?.[1]?.toUpperCase()
  const changesTicker = (sourceTicker !== undefined || targetTicker !== undefined) && sourceTicker !== targetTicker
  const changesSummary = (sourcePath === '/overview' || path === '/overview') && sourcePath !== path
  const changesStrategyRelease = isDailyStrategyPath(sourcePath) !== isDailyStrategyPath(path)
  return publicationPath(path, changesTicker || changesSummary || changesStrategyRelease ? undefined : snapshotId, cohortId,
    isDailyStrategyPath(path) && isDailyStrategyPath(sourcePath) ? interval : undefined)
}

export function useStrategyInterval(searchState?: { current: { pending: URLSearchParams } }): { interval: '1d' | '1h'; setInterval: (selected: '1d' | '1h') => void } {
  const [params, setParams] = useSearchParams()
  const interval = params.get('interval') === '1h' ? '1h' : '1d'
  const setInterval = (selected: '1d' | '1h') => setParams(previous => {
    const next = new URLSearchParams(searchState?.current.pending ?? previous)
    if (selected === (next.get('interval') ?? '1d')) return next
    if (selected === '1h') next.set('interval', selected)
    else next.delete('interval')
    next.delete('snapshot_id')
    next.delete('evaluation_session')
    const filter = next.get('strategy_filter')
    if (filter) next.set('strategy_filter', selected === '1h'
      ? filter.replace('_1D', '_1H').replace('FIBONACCI_1H_5', 'FIBONACCI_1H_ATR')
      : filter.replace('_1H', '_1D').replace('FIBONACCI_1D_ATR', 'FIBONACCI_1D_5'))
    if (searchState) searchState.current.pending = next
    return next
  }, { replace: true })
  return { interval, setInterval }
}

export function usePublicationQuery<T extends { publication: Pick<DashboardPublication, 'cohort_id' | 'snapshot_id'> }>(
  name: string, load: (snapshotId?: string) => Promise<T>,
  requireIdentity = false,
) {
  const realtimeOnly = import.meta.env.VITE_REALTIME_ONLY === 'true'
  const cohort = import.meta.env.VITE_REALTIME_COHORT_ID?.trim()
  const [params, setParams] = useSearchParams()
  // A stale ticker link may still carry a pin; the ticker page ignores it and loads the current view.
  const pin = isTickerWorkspaceQuery(name) ? undefined : params.get('snapshot_id') ?? undefined
  const internalSelection = !requireIdentity && pin === undefined
    && (isTickerWorkspaceQuery(name) || name.startsWith('daily-'))
  const interval = name.startsWith('daily-') ? params.get('interval') ?? '1d' : '1d'
  const inheritedCohort = params.get('cohort_id')
  const scopeError = name.startsWith('daily-') && !['1d', '1h'].includes(interval)
    ? 'Unsupported strategy interval; no substitution was made.' : inheritedCohort && inheritedCohort !== cohort
    ? 'Requested cohort does not match this realtime deployment; no substitution was made.'
    : requireIdentity && (!inheritedCohort || !pin)
      ? 'Daily strategy research requires explicit cohort and snapshot identities.' : null
  const queryClient = useQueryClient()
  const key = [name, cohort, pin, ...(interval === '1h' ? ['1h'] : [])]
  const [refreshError, setRefreshError] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const generation = useRef(0)
  const refreshInFlight = useRef(false)
  useEffect(() => {
    setRefreshError(null)
    setRefreshing(false)
    refreshInFlight.current = false
    return () => { generation.current += 1; refreshInFlight.current = false }
  }, [name, cohort, pin, interval])
  const query = useQuery({
    queryKey: key,
    queryFn: () => {
      if (scopeError) throw new Error(scopeError)
      if (!isTickerWorkspaceQuery(name) && params.has('snapshot_id') && !pin) throw new Error('Requested snapshot identity is empty')
      return load(pin)
    },
    enabled: realtimeOnly,
    retry: false,
    staleTime: Infinity,
    refetchOnMount: internalSelection ? 'always' : undefined,
    refetchOnWindowFocus: false,
  })
  const commit = (result: T) => {
    const snapshot = result.publication.snapshot_id
    queryClient.setQueryData([name, cohort, snapshot, ...(interval === '1h' ? ['1h'] : [])], result)
    if (internalSelection) {
      queryClient.setQueryData(key, result)
      return
    }
    setParams(previous => {
      const next = new URLSearchParams(previous)
      next.set('cohort_id', cohort ?? '')
      next.set('snapshot_id', snapshot)
      return next
    }, { replace: true })
  }
  useEffect(() => {
    if (realtimeOnly && query.data && !pin && !scopeError && !internalSelection) commit(query.data)
  }, [realtimeOnly, query.data, pin, scopeError, internalSelection])
  const refresh = async () => {
    if (refreshInFlight.current) return
    refreshInFlight.current = true
    const requestedGeneration = generation.current
    setRefreshError(null)
    setRefreshing(true)
    try {
      if (scopeError) throw new Error(scopeError)
      const complete = await load()
      if (generation.current === requestedGeneration) commit(complete)
    } catch (error) {
      if (generation.current === requestedGeneration) {
        setRefreshError(error instanceof Error ? error.message : 'Stored publication refresh failed')
      }
    } finally {
      if (generation.current === requestedGeneration) {
        refreshInFlight.current = false
        setRefreshing(false)
      }
    }
  }
  return {
    data: scopeError ? undefined : query.data,
    error: scopeError ?? refreshError ?? query.error?.message,
    loading: query.isFetching || refreshing,
    refresh,
    retry: () => {
      setRefreshError(null)
      return query.refetch()
    },
  }
}
