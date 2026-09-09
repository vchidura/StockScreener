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
  status?: PageContextStatus[]
  /** Interval to step sessions by; the date itself lives in `useSessionDate`. */
  session?: string
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
