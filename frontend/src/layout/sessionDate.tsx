import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getTradingSessions } from '../services/api'

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
  /** Interval whose sessions are listed; today only appears once that interval has bars. */
  interval: string
  requestInterval: (interval: string) => void
  /** Newest stored bar across all intervals, ISO. */
  dataAsOf: string | null
  dataInterval: string | null
}

const SessionDateContext = createContext<SessionDateStore | null>(null)

export function SessionDateProvider({ children }: { children: ReactNode }) {
  const [pinned, setPinned] = useState('')
  const [interval, setInterval] = useState('1d')

  const { data } = useQuery({
    queryKey: ['market', 'trading-sessions', interval],
    queryFn: () => getTradingSessions(interval),
    staleTime: 60 * 1000,
  })

  const sessions = useMemo(() => data?.sessions ?? [], [data])

  // Switching interval can strand the pinned session: today exists for 5m but not yet for 1d.
  useEffect(() => {
    if (!pinned || !sessions.length) return
    const newest = sessions[sessions.length - 1]
    // At or past the newest available session there is nothing historical to pin.
    if (pinned >= newest) { setPinned(''); return }
    if (sessions.includes(pinned)) return
    const fallback = [...sessions].reverse().find(s => s < pinned)
    setPinned(fallback ?? '')
  }, [pinned, sessions])

  const latest = sessions.length ? sessions[sessions.length - 1] : ''
  const date = pinned || latest

  const step = useCallback((delta: number) => {
    setPinned(current => {
      if (!sessions.length) return current
      const from = current ? sessions.indexOf(current) : sessions.length - 1
      if (from < 0) return current
      const next = Math.min(Math.max(from + delta, 0), sessions.length - 1)
      // Landing on the newest session unpins, restoring the live snapshot view.
      return next === sessions.length - 1 ? '' : sessions[next]
    })
  }, [sessions])

  const reset = useCallback(() => setPinned(''), [])
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
    interval,
    requestInterval,
    dataAsOf: data?.data_as_of ?? null,
    dataInterval: data?.data_interval ?? null,
  }), [pinned, date, sessions, index, step, reset, interval, requestInterval, data])

  return <SessionDateContext.Provider value={value}>{children}</SessionDateContext.Provider>
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

/** Exchange-local stamp, e.g. "Sep 9, 2026, 11:45 AM EDT". */
export function formatMarketTime(iso: string): string {
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
