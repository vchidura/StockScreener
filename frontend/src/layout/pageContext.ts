import { createContext, useContext, useEffect } from 'react'

export type PageContextStatus = {
  label: string
  value: string
  note?: string
  title?: string
  tone?: 'positive' | 'negative'
}

export type PageContextValue = {
  eyebrow?: string
  title: string
  detail?: string
  /** No longer shown (Nov 12, user): the header's right side is the same Market data / Mode / session / refresh panel
   * on every page. Kept so existing pages compile; page facts belong in the page body. */
  status?: PageContextStatus[]
  /** The header's Mode line when it is not "Real-Time" (default) or "Session close" (a past session is selected),
   * e.g. "Replay" for simulated Stock Alerts. */
  mode?: string
  modeTitle?: string
  /** Interval to step sessions by; the date itself lives in `useSessionDate`. */
  session?: string
  alertSessions?: { dates: string[]; selected: string; source: string; emptyLabel?: string; resetKeys?: string[];
    datasetByDate?: Record<string, string>; sessionRollup?: boolean; historicalView?: string; historicalSource?: string }
}

export const PageContextSetter = createContext<(value: PageContextValue | null) => void>(() => {})

/** Publishes the page's identity and freshness into the shell context bar. */
export function usePublishPageContext(value: PageContextValue | null) {
  const setPageContext = useContext(PageContextSetter)
  const serialized = value ? JSON.stringify(value) : null

  useEffect(() => {
    setPageContext(serialized ? (JSON.parse(serialized) as PageContextValue) : null)
    return () => setPageContext(null)
  }, [serialized, setPageContext])
}
