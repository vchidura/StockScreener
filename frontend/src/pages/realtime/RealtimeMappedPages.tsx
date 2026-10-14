import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, Check, CircleX, Database, RefreshCw, ShieldCheck } from 'lucide-react'
import { usePublishPageContext } from '../../layout/pageContext'
import {
  REALTIME_COHORT_ID,
  getRealtimeEquityValidationStatus,
  getRealtimeHealth,
  getRealtimeOptionAlerts,
  type RealtimeCanaryStatus,
  type RealtimeHealth,
} from '../../services/realtimeCanary'
import '../RealtimeCanaryPage.css'
import '../StockDiscoveryPage.css'
import '../StockAlertsPage.css'
import '../StockScreeningPage.css'
import '../OptionsAlertsPage.css'
import '../ticker/ticker.css'
import './RealtimeMappedPages.css'

const label = (value: unknown) => String(value || 'UNAVAILABLE').toLowerCase().replace(/_/g, ' ').replace(/^./, first => first.toUpperCase())
const stamp = (value?: string) => value ? new Date(value).toLocaleString('en-US', {
  timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit',
}) + ' ET' : 'Unavailable'

function useRealtimeBoundary() {
  const health = useQuery({
    queryKey: ['realtime-health'],
    queryFn: ({ signal }) => getRealtimeHealth(signal),
    retry: false,
    staleTime: 15_000,
  })
  const status = useQuery({
    queryKey: ['realtime-equity-status', REALTIME_COHORT_ID],
    queryFn: ({ signal }) => getRealtimeEquityValidationStatus(REALTIME_COHORT_ID, signal),
    retry: false,
    staleTime: 15_000,
  })
  return { health, status }
}

function State({ ready, children }: { ready: boolean; children: string }) {
  return <span className={`rc-state ${ready ? 'is-ready' : 'is-blocked'}`}>
    {ready ? <Check size={14} aria-hidden="true" /> : <CircleX size={14} aria-hidden="true" />}
    {children}
  </span>
}

function Isolation({ health, status }: { health?: RealtimeHealth; status?: RealtimeCanaryStatus }) {
  return <section className="rc-isolation" aria-label="Database isolation">
    <Database size={17} aria-hidden="true" />
    <div><span>Database</span><strong>{health?.database_target ?? 'Unavailable'}</strong></div>
    <State ready={health?.canonical_fallback === false}>{health?.canonical_fallback ? 'Canonical fallback detected' : 'No canonical fallback'}</State>
    <State ready={health?.provider_fetch_performed === false && status?.provider_fetch_performed === false}>
      {health?.provider_fetch_performed || status?.provider_fetch_performed ? 'Provider fetch detected' : 'Stored projections only'}
    </State>
  </section>
}

function Loading() {
  return <div className="rt-empty" role="status">Loading stored realtime projections...</div>
}

function Failure({ retry }: { retry: () => void }) {
  return <div className="rt-empty is-error" role="alert"><AlertTriangle size={18} />Realtime projection unavailable.<button type="button" onClick={retry}><RefreshCw size={14} />Retry</button></div>
}

function Boundary({ boundary, children }: {
  boundary: ReturnType<typeof useRealtimeBoundary>
  children: (health: RealtimeHealth, status: RealtimeCanaryStatus) => React.ReactNode
}) {
  if (boundary.health.isPending || boundary.status.isPending) return <Loading />
  if (!boundary.health.data || !boundary.status.data || boundary.health.isError || boundary.status.isError) {
    return <Failure retry={() => { void boundary.health.refetch(); void boundary.status.refetch() }} />
  }
  return <>{children(boundary.health.data, boundary.status.data)}</>
}

export function RealtimeOptionsAlertsPage() {
  const boundary = useRealtimeBoundary()
  const query = useQuery({
    queryKey: ['realtime-option-alerts', REALTIME_COHORT_ID],
    queryFn: ({ signal }) => getRealtimeOptionAlerts(REALTIME_COHORT_ID, signal),
    retry: false,
  })
  usePublishPageContext({
    eyebrow: 'Realtime equity',
    title: 'Options Alerts',
    status: [
      { label: 'Capability', value: 'Not applicable' },
      { label: 'Data tier', value: query.data?.data_tier ?? 'UNAVAILABLE' },
      { label: 'Evaluations', value: String(query.data?.data?.evaluations?.length ?? 0) },
    ],
  })

  return <div className="stock-discovery rt-page">
    <Boundary boundary={boundary}>{(health, status) => <>
      <Isolation health={health} status={status} />
      <section className="rt-band" aria-label="Options alert availability"><header><div><span>Equity validation scope</span><h2>Options are not applicable</h2></div><State ready={status.option_alerts_status === 'NOT_APPLICABLE_EQUITY_VALIDATION'}>Not applicable</State></header>
        <div className="rt-unavailable"><ShieldCheck size={28} aria-hidden="true" /><strong>{label(status.option_alerts_status)}</strong><p>{label(query.data?.reason || 'EQUITY_VALIDATION_NO_OPTIONS')}</p><button type="button" onClick={() => void query.refetch()} disabled={query.isFetching}><RefreshCw size={14} />Refresh stored evaluations</button></div>
      </section>
      <details className="rt-provenance"><summary>Endpoint evidence</summary><dl><div><dt>Available</dt><dd>{query.data?.available ? 'Yes' : 'No'}</dd></div><div><dt>Data tier</dt><dd>{query.data?.data_tier ?? 'Unavailable'}</dd></div><div><dt>Generated</dt><dd>{stamp(query.data?.generated_at)}</dd></div><div><dt>Evaluations</dt><dd>{query.data?.data?.evaluations?.length ?? 0}</dd></div></dl></details>
    </>}</Boundary>
  </div>
}
