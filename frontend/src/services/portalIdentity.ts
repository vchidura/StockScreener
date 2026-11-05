import { useSyncExternalStore } from 'react'
import axios from 'axios'

// Each session has its own cohort, so the portal discovers the current one at
// runtime. Requests still name it, which keeps one view on one stored identity.
export interface PortalPublicationIdentity {
  cohort_id: string
  session_date: string
  status: 'CURRENT' | 'PRIOR_SESSION' | 'AWAITING_FIRST_RELEASE'
  latest_session_date: string
  release_id: string | null
  cutoff: string | null
}

export interface PortalIdentity {
  equity: PortalPublicationIdentity | null
  options: PortalPublicationIdentity | null
  options_reads_enabled: boolean
  session_cohorts: Record<string, string>
  options_session_cohorts: Record<string, string>
}

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api'

let identity: PortalIdentity | null = null
const listeners = new Set<() => void>()

const subscribe = (listener: () => void) => {
  listeners.add(listener)
  return () => { listeners.delete(listener) }
}

const snapshot = () => identity

export const realtimeCohortId = () => identity?.equity?.cohort_id
export const realtimeOptionsCohortId = (sessionDate?: string) =>
  sessionDate
    ? identity?.options_session_cohorts[sessionDate]
    : identity?.options?.cohort_id
export const realtimeOptionsReadsEnabled = () => identity?.options_reads_enabled === true
export const usePortalIdentity = () => useSyncExternalStore(subscribe, snapshot)

/** Returns true when a served cohort changed, so cached reads must be refetched. */
export async function refreshPortalIdentity(signal?: AbortSignal): Promise<boolean> {
  const next = (await axios.get<PortalIdentity>(`${API_BASE_URL}/realtime/portal/current`, { signal, timeout: 15000 })).data
  const changed = identity !== null && (
    identity.equity?.cohort_id !== next.equity?.cohort_id
    || identity.options?.cohort_id !== next.options?.cohort_id
  )
  identity = next
  listeners.forEach(listener => listener())
  return changed
}
