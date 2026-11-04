import { useEffect, useRef, useState, type ReactNode } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { refreshPortalIdentity } from '../services/portalIdentity'

const REFRESH_MILLISECONDS = 60_000

/** Resolves the served publication before the first read and follows session rollover. */
export function PortalIdentityGate({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient()
  const [settled, setSettled] = useState(false)
  const refreshFailed = useRef(false)
  useEffect(() => {
    const controller = new AbortController()
    const refresh = async () => {
      try {
        const changed = await refreshPortalIdentity(controller.signal)
        const recovered = refreshFailed.current
        refreshFailed.current = false
        if (changed || recovered) await queryClient.invalidateQueries({ type: 'active' })
        else await queryClient.invalidateQueries({
          type: 'active',
          predicate: query => query.state.status === 'error',
        })
      } catch {
        refreshFailed.current = true
      } finally {
        if (!controller.signal.aborted) setSettled(true)
      }
    }
    void refresh()
    const timer = window.setInterval(() => void refresh(), REFRESH_MILLISECONDS)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [queryClient])
  if (!settled) return <div className="loading" role="status" aria-busy="true"><div className="spinner" aria-hidden="true" />Loading workspace…</div>
  return <>{children}</>
}
