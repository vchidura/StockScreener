import { useQueries, useQuery } from '@tanstack/react-query'
import { AlertTriangle, Check, CircleX, Database, RefreshCw, ShieldCheck } from 'lucide-react'
import { useSearchParams } from 'react-router-dom'
import { usePublishPageContext } from '../layout/pageContext'
import {
  getRealtimeCanaryStatus,
  getRealtimeEquityValidationStatus,
  getRealtimeOptionAlerts,
  getRealtimeScreening,
  getRealtimeStockAlerts,
  getRealtimeTickerPage,
} from '../services/realtimeCanary'
import './RealtimeCanaryPage.css'

const label = (value: string) => value.toLowerCase().replace(/_/g, ' ').replace(/^./, (first: string) => first.toUpperCase())
const stamp = (value?: string) => value
  ? new Date(value).toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit' }) + ' ET'
  : 'Unavailable'
const payload = (value: unknown) => JSON.stringify(value, null, 2)

function Availability({ ready, children }: { ready: boolean; children: string }) {
  return <span className={`rc-state ${ready ? 'is-ready' : 'is-blocked'}`}>
    {ready ? <Check size={14} aria-hidden="true" /> : <CircleX size={14} aria-hidden="true" />}
    {children}
  </span>
}

export default function RealtimeCanaryPage() {
  const [searchParams] = useSearchParams()
  const cohortId = searchParams.get('cohort_id') ?? undefined
  const equityValidation = searchParams.get('scope') === 'equity'
  const query = useQuery({
    queryKey: ['realtime-canary-status', equityValidation ? 'equity' : 'options', cohortId],
    queryFn: ({ signal }) => equityValidation
      ? getRealtimeEquityValidationStatus(cohortId, signal)
      : getRealtimeCanaryStatus(cohortId, signal),
    retry: false,
    refetchInterval: 15_000,
    refetchOnWindowFocus: true,
  })
  const data = query.data
  const ready = data?.status === 'READY'
  const tickerQueries = useQueries({
    queries: (data?.expected_tickers ?? []).map(ticker => ({
      queryKey: ['realtime-ticker-page', equityValidation ? 'equity' : 'options', cohortId, ticker],
      queryFn: ({ signal }: { signal: AbortSignal }) => getRealtimeTickerPage(ticker, cohortId, signal),
      enabled: Boolean(data),
      retry: false,
    })),
  })
  const screeningQuery = useQuery({
    queryKey: ['realtime-screening', equityValidation ? 'equity' : 'options', cohortId],
    queryFn: ({ signal }) => getRealtimeScreening(cohortId, signal),
    enabled: Boolean(data),
    retry: false,
  })
  const stockAlertsQuery = useQuery({
    queryKey: ['realtime-stock-alerts', equityValidation ? 'equity' : 'options', cohortId],
    queryFn: ({ signal }) => getRealtimeStockAlerts(cohortId, signal),
    enabled: Boolean(data),
    retry: false,
  })
  const optionAlertsQuery = useQuery({
    queryKey: ['realtime-option-alerts', 'options', cohortId],
    queryFn: ({ signal }) => getRealtimeOptionAlerts(cohortId, signal),
    enabled: Boolean(data) && !equityValidation,
    retry: false,
  })

  usePublishPageContext({
    eyebrow: equityValidation ? 'Equity validation cohort' : 'Developer cohort',
    title: equityValidation ? 'Realtime Equity Validation' : 'Realtime Canary',
    status: [
      { label: 'Canary', value: data?.status ?? 'Unavailable', tone: ready ? 'positive' : 'negative' },
      { label: 'Coverage', value: data ? `${data.ready_tickers.length} / ${data.expected_tickers.length}` : 'Unavailable' },
      { label: 'Checked', value: stamp(data?.checked_at) },
    ],
  })

  if (query.isLoading) return <div className="loading" role="status">Loading realtime canary status...</div>
  if (query.isError || !data) return <div className="rc-empty" role="alert">
    <AlertTriangle size={18} aria-hidden="true" />
    <span>Dedicated realtime status is unavailable.</span>
    <button type="button" title="Retry realtime status" onClick={() => void query.refetch()}><RefreshCw size={15} /> Retry</button>
  </div>

  const readyTickers = new Set(data.ready_tickers)
  const screeningTickers = new Set(data.screening_tickers)
  return <div className="realtime-canary">
    <section className={`rc-summary ${ready ? 'is-ready' : 'is-blocked'}`} aria-label="Canary readiness">
      <div className="rc-summary__state">
        {ready ? <ShieldCheck size={24} aria-hidden="true" /> : <AlertTriangle size={24} aria-hidden="true" />}
        <div><span>Canary status</span><strong>{data.status}</strong></div>
      </div>
      <dl>
        <div><dt>Tickers ready</dt><dd>{data.ready_tickers.length} / {data.expected_tickers.length}</dd></div>
        <div><dt>Stock alerts</dt><dd>{data.stock_alerts_available ? 'Available' : 'Unavailable'}</dd></div>
        <div><dt>Option alerts</dt><dd>{equityValidation ? 'Not applicable' : data.option_alerts_available ? 'Available' : 'Unavailable'}</dd></div>
        <div><dt>Checked</dt><dd>{stamp(data.checked_at)}</dd></div>
      </dl>
      <button className="rc-refresh" type="button" title="Refresh realtime status" aria-label="Refresh realtime status" disabled={query.isFetching} onClick={() => void query.refetch()}>
        <RefreshCw size={16} aria-hidden="true" />
      </button>
    </section>

    <section className="rc-isolation" aria-label="Database isolation">
      <Database size={17} aria-hidden="true" />
      <div><span>Database</span><strong>{data.database_target}</strong></div>
      <Availability ready={!data.canonical_fallback}>{data.canonical_fallback ? 'Canonical fallback detected' : 'No canonical fallback'}</Availability>
      <Availability ready={!data.provider_fetch_performed}>{data.provider_fetch_performed ? 'Request-time provider fetch detected' : 'Stored projections only'}</Availability>
    </section>

    <section className="rc-coverage" aria-labelledby="rc-coverage-title">
      <header><div><span>Reviewed universe</span><h2 id="rc-coverage-title">Ticker coverage</h2></div><code title={data.universe_sha256}>{data.universe_version}</code></header>
      <div className="rc-table-wrap">
        <table>
          <thead><tr><th>Ticker</th><th>Ticker page</th><th>Screening</th></tr></thead>
          <tbody>{data.expected_tickers.map(ticker => <tr key={ticker}>
            <th scope="row">{ticker}</th>
            <td><Availability ready={readyTickers.has(ticker)}>{readyTickers.has(ticker) ? 'Ready' : 'Missing'}</Availability></td>
            <td><Availability ready={screeningTickers.has(ticker)}>{screeningTickers.has(ticker) ? 'Ready' : 'Missing'}</Availability></td>
          </tr>)}</tbody>
        </table>
      </div>
    </section>

    <section className="rc-values" aria-labelledby="rc-values-title">
      <header><div><span>Stored cohort values</span><h2 id="rc-values-title">Inspection</h2></div></header>
      <div className="rc-values__grid">
        <article>
          <h3>Ticker technicals</h3>
          <div className="rc-ticker-values">{data.expected_tickers.map((ticker, index) => <details key={ticker}>
            <summary>{ticker}</summary>
            <pre>{tickerQueries[index]?.data ? payload(tickerQueries[index].data) : 'Unavailable'}</pre>
          </details>)}</div>
        </article>
        <article><h3>Screening</h3><pre>{screeningQuery.data ? payload(screeningQuery.data) : 'Unavailable'}</pre></article>
        <article><h3>Stock alerts</h3><pre>{stockAlertsQuery.data ? payload(stockAlertsQuery.data) : 'Unavailable'}</pre></article>
        <article><h3>Option evaluations</h3><pre>{equityValidation ? 'Not applicable for equity validation' : optionAlertsQuery.data ? payload(optionAlertsQuery.data) : 'Unavailable'}</pre></article>
      </div>
    </section>

    {!!data.reason_codes.length && <section className="rc-blockers" aria-labelledby="rc-blockers-title">
      <header><AlertTriangle size={17} aria-hidden="true" /><h2 id="rc-blockers-title">Blocking conditions</h2></header>
      <ul>{data.reason_codes.map(reason => <li key={reason}>{label(reason)}</li>)}</ul>
    </section>}
  </div>
}