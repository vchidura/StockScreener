import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useLocation, useSearchParams } from 'react-router-dom'
import { getAllTickersClosingSessions, getPatternClosingSessions, getStrategyTradingSessions, getTickerClosingSessions, getTradingSessions } from '../services/api'
import { isDailyStrategyPath } from '../pages/publicationNavigation'
import { activeTickerSymbol } from './navigation'

type SessionDateStore = {
  /** Empty while the newest session is selected, so callers keep published-snapshot semantics. */
  pinned: string
  /** Resolved session actually being shown. */
  date: string
  sessions: string[]
  isLatest: boolean
  canStepBack: boolean
  canStepForward: boolean
  step: (delta: number) => void
  reset: () => void
  choose: (session: string) => void
  isStrategy: boolean
  isTicker: boolean
  isPattern: boolean
  isAllTickers: boolean
  error: string | null
  loading: boolean
  /** Interval whose sessions are listed; today only appears once that interval has bars. */
  interval: string
  requestInterval: (interval: string) => void
  /** Newest stored bar across all intervals, ISO. */
  dataAsOf: string | null
  dataInterval: string | null
  /** Today while its session runs, on pages that read live windows. Null otherwise. */
  runningSession: string | null
}

/**
 * Closing pages list closed sessions, so during a session they open on the last close.
 * The ticker page and All Tickers show today's prices, so they also list the running session, as the newest.
 */
export function listedSessions(
  data: { sessions?: string[]; current_session?: string | null; current_is_session_close?: boolean } | undefined,
  listsRunningSession: boolean,
): string[] {
  const listed = data?.sessions ?? []
  if (data?.current_is_session_close !== false || !data.current_session) return listed
  const closed = listed.filter(session => session !== data.current_session)
  return listsRunningSession ? [...closed, data.current_session] : closed
}

const SessionDateContext = createContext<SessionDateStore | null>(null)

export function SessionDateProvider({ children, enabled = true }: { children: ReactNode; enabled?: boolean }) {
  const [localPinned, setPinned] = useState('')
  const [interval, setInterval] = useState('1d')
  const location = useLocation()
  const [params, setParams] = useSearchParams()
  const isStrategy = import.meta.env.VITE_REALTIME_ONLY === 'true' && isDailyStrategyPath(location.pathname)
  const ticker = import.meta.env.VITE_REALTIME_ONLY === 'true' ? activeTickerSymbol(location.pathname) : null
  const isTicker = ticker !== null
  const isPattern = import.meta.env.VITE_REALTIME_ONLY === 'true' && location.pathname === '/pattern-watch'
  const isAllTickers = import.meta.env.VITE_REALTIME_ONLY === 'true' && location.pathname === '/overview'
  const persistedSelection = isStrategy || isTicker || isPattern || isAllTickers
  const research = location.pathname.startsWith('/strategy-research/')
  const sessionKey = research ? 'evaluation_session' : 'session_date'
  const pinned = persistedSelection ? params.get(sessionKey) ?? '' : localPinned
  const selectedInterval = isTicker || isPattern || isAllTickers ? '1d' : isStrategy ? params.get('interval') ?? '1d' : interval
  const researchPin = isStrategy && research ? params.get('snapshot_id') ?? undefined : undefined

  const { data, error, isFetching } = useQuery({
    queryKey: ['market', isTicker ? 'ticker-closing-sessions' : isPattern ? 'pattern-closing-sessions'
      : isAllTickers ? 'all-tickers-closing-sessions' : isStrategy ? 'strategy-sessions' : 'trading-sessions',
      selectedInterval, ...(ticker ? [ticker] : []), ...(researchPin ? [researchPin] : [])],
    queryFn: () => ticker ? getTickerClosingSessions(ticker) : isPattern ? getPatternClosingSessions()
      : isAllTickers ? getAllTickersClosingSessions()
      : isStrategy ? getStrategyTradingSessions(selectedInterval, researchPin) : getTradingSessions(selectedInterval),
    staleTime: 60 * 1000,
    placeholderData: previous => previous,
    enabled,
  })

  const followsRunningSession = isTicker || isAllTickers
  const sessions = useMemo(() => listedSessions(data, followsRunningSession), [data, followsRunningSession])
  const runningSession = followsRunningSession && data?.current_is_session_close === false ? data.current_session ?? null : null

  // Switching interval can strand the pinned session: today exists for 5m but not yet for 1d.
  useEffect(() => {
    if (persistedSelection || !pinned || !sessions.length) return
    const newest = sessions[sessions.length - 1]
    // At or past the newest available session there is nothing historical to pin.
    if (pinned >= newest) { setPinned(''); return }
    if (sessions.includes(pinned)) return
    const fallback = [...sessions].reverse().find(s => s < pinned)
    setPinned(fallback ?? '')
  }, [pinned, sessions, persistedSelection])

  const latest = sessions.length ? sessions[sessions.length - 1] : ''
  const date = pinned || latest

  const choose = useCallback((session: string) => {
    const historical = session === latest ? '' : session
    if (!persistedSelection) { setPinned(historical); return }
    setParams(previous => {
      const next = new URLSearchParams(previous)
      if (historical) next.set(sessionKey, historical)
      else next.delete(sessionKey)
      if (!research) {
        next.delete('snapshot_id')
        next.delete('evaluation_session')
      }
      next.delete('page')
      return next
    })
  }, [persistedSelection, latest, research, sessionKey, setParams])

  const step = useCallback((delta: number) => {
    const from = pinned ? sessions.indexOf(pinned) : sessions.length - 1
    if (from < 0) return
    const next = Math.min(Math.max(from + delta, 0), sessions.length - 1)
    choose(sessions[next])
  }, [pinned, sessions, choose])
  const reset = useCallback(() => choose(latest), [choose, latest])
  const requestInterval = useCallback((next: string) => setInterval(next), [])

  const index = date ? sessions.indexOf(date) : -1
  const value = useMemo<SessionDateStore>(() => ({
    pinned,
    date,
    sessions,
    isLatest: !pinned,
    canStepBack: index > 0,
    canStepForward: index >= 0 && index < sessions.length - 1,
    step,
    reset,
    choose,
    isStrategy,
    isTicker,
    isPattern,
    isAllTickers,
    error: persistedSelection && !sessions.length ? error?.message ?? null : null,
    loading: !!isFetching && !sessions.length,
    interval: selectedInterval,
    requestInterval,
    dataAsOf: data?.data_as_of ?? null,
    dataInterval: data?.data_interval ?? null,
    runningSession,
  }), [pinned, date, sessions, index, step, reset, choose, isStrategy, isTicker, isPattern, isAllTickers, persistedSelection, error, isFetching, selectedInterval, requestInterval, data, runningSession])

  return <SessionDateContext.Provider value={value}>{children}</SessionDateContext.Provider>
}

/** The session store when one is mounted, for shared hooks that also run without it. */
export function useOptionalSessionDate(): SessionDateStore | null {
  return useContext(SessionDateContext)
}

export function useSessionDate(): SessionDateStore {
  const store = useContext(SessionDateContext)
  if (!store) throw new Error('useSessionDate must be used inside SessionDateProvider')
  return store
}

/** Weekday-and-month label for the stepper, e.g. "Fri, Sep 4". */
export function sessionLabel(date: string): string {
  if (!date) return '—'
  const [year, month, day] = date.split('-').map(Number)
  if (!year || !month || !day) return date
  return new Date(year, month - 1, day).toLocaleDateString(undefined, {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
  })
}

/**
 * Nov 12 (user): whether the market session is running now, for the header's Mode. True only when the current
 * publication belongs to today's Eastern date and the clock is in regular hours (9:30 AM to 4:00 PM ET); a weekend,
 * holiday or the time before the open keeps the previous session's publication and reads false. (An early close at
 * 1:00 PM still reads true until 4:00 PM; three days a year.)
 */
export function marketSessionOpen(publication: { session_date?: string | null } | null | undefined, now = new Date()): boolean {
  if (!publication?.session_date) return false
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(now).map(part => [part.type, part.value]))
  const today = `${parts.year}-${parts.month}-${parts.day}`
  const minutes = Number(parts.hour) * 60 + Number(parts.minute)
  return publication.session_date === today && minutes >= 9 * 60 + 30 && minutes < 16 * 60
}

/**
 * The header's Market data stamp, always one format: "Nov 11, 2026, 4:00 PM EST" (Eastern time). A date-only value
 * (a session date, no time) reads as that session's 4:00 PM ET close instead of switching to a weekday label.
 */
export function formatMarketStamp(value: string): string {
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    const [year, month, day] = value.split('-').map(Number)
    // 21:00 UTC is 4:00 PM EST and 5:00 PM EDT, so build the close from the zone's own offset at noon that day.
    const noonUtc = Date.UTC(year, month - 1, day, 12)
    const offsetHours = Number(new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hour: 'numeric', hourCycle: 'h23' })
      .format(new Date(noonUtc))) - 12
    return formatMarketTime(new Date(Date.UTC(year, month - 1, day, 16 - offsetHours)).toISOString())
  }
  return formatMarketTime(value)
}

/** Exchange-local stamp, e.g. "Sep 9, 2026, 11:45 AM EDT". */
export function formatMarketTime(iso: string): string {
  // A session date has no time; read as one, it shows as 7 PM the evening before.
  if (/^\d{4}-\d{2}-\d{2}$/.test(iso)) return sessionLabel(iso)
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'short',
  }).format(new Date(iso))
}
