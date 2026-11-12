import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Database,
  Layers3,
  Radio,
} from 'lucide-react'
import { usePublishPageContext } from '../layout/pageContext'
import { useSessionDate } from '../layout/sessionDate'
import {
  getOptionFlow,
  optionsDataTierLabel,
  type OptionFlowContract,
  type OptionFlowExpiration,
  type OptionFlowStrike,
  type OptionFlowSummary,
} from '../services/api'
import { msUntilWindowRead, OPTIONS_FIRST_CHECK_MS, windowTimeMs } from '../utils/refreshTiming'
import './OptionsFlowPage.css'

/** The window the Flow read shows: its release cutoff, or the chain capture's time when that is older. */
export const flowWindowMs = (envelope?: { release_cutoff?: string; as_of?: string | null;
  data?: { coverage_source?: string; breadth?: { breadth_observed_at?: string } | null } } | null) =>
  envelope ? windowTimeMs(envelope.release_cutoff ?? envelope.as_of,
    envelope.data?.coverage_source === 'BREADTH' ? envelope.data.breadth?.breadth_observed_at : undefined) : null

type ProfileMetric = 'volume' | 'open_interest'

const integer = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })
const compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })
const currency = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  notation: 'compact',
  maximumFractionDigits: 1,
})

const formatInteger = (value: number | null | undefined) => integer.format(value || 0)
const formatCompact = (value: number | string | null | undefined) => compact.format(Number(value || 0))
const formatCurrency = (value: number | string | null | undefined) => currency.format(Number(value || 0))
const formatRatio = (value: number | null | undefined) => value == null ? 'N/A' : value.toFixed(2)
const formatPercent = (value: number | null | undefined) => `${((value || 0) * 100).toFixed(0)}%`
const formatDate = (value: string | null | undefined) => value
  ? new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(new Date(`${value}T12:00:00Z`))
  : 'N/A'
const formatDateTime = (value: string | null | undefined) => value
  ? new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    }).format(new Date(value))
  : 'Unavailable'

export function optionFlowUnavailableReason(reason: string | null | undefined, error?: unknown) {
  if (error instanceof Error && error.message.startsWith('No Options publication')) return error.message
  if (reason === 'EQUITY_ONLY_COHORT') return 'The selected realtime cohort contains no option underlyings.'
  if (reason === 'NO_COMPLETE_MATRIX') return 'No complete analyzed option matrices were retained for this cohort.'
  if (reason === 'ACTIVE_COHORT_UNAVAILABLE') return 'The selected realtime cohort is unavailable.'
  return reason || 'The options service could not be read.'
}

function metricValues(row: OptionFlowExpiration | OptionFlowStrike, metric: ProfileMetric) {
  return metric === 'volume'
    ? [row.call_volume, row.put_volume]
    : [row.call_open_interest, row.put_open_interest]
}

function ActivitySplit({ row }: { row: OptionFlowSummary }) {
  const callShare = row.total_volume > 0 ? row.call_volume / row.total_volume : 0.5
  return (
    <div className="flow-split" title={`Calls ${formatPercent(callShare)} · Puts ${formatPercent(1 - callShare)}`}>
      <span className="flow-split__call" style={{ width: `${callShare * 100}%` }} />
      <span className="flow-split__put" style={{ width: `${(1 - callShare) * 100}%` }} />
    </div>
  )
}

function ProfileToggle({ value, onChange }: { value: ProfileMetric; onChange: (value: ProfileMetric) => void }) {
  return (
    <div className="flow-segmented" role="group" aria-label="Profile metric">
      <button type="button" className={value === 'volume' ? 'active' : ''} onClick={() => onChange('volume')}>Volume</button>
      <button type="button" className={value === 'open_interest' ? 'active' : ''} onClick={() => onChange('open_interest')}>Open interest</button>
    </div>
  )
}

export function TickerBoard({ rows, selected, onSelect }: {
  rows: OptionFlowSummary[]
  selected: string
  onSelect: (ticker: string) => void
}) {
  return (
    <section className="flow-section flow-board">
      <header className="flow-section__header">
        <div><Radio size={17} /><div><h2>Tracked universe</h2><p>{rows.some(row => row.coverage_source === 'BREADTH') ? 'Full listed chain for each configured underlying · provider day volume and open interest' : 'Latest complete matrix for each configured underlying'}</p></div></div>
        <span>{rows.length} underlyings</span>
      </header>
      <div className="flow-table-wrap">
        <table className="flow-table flow-table--board">
          <thead><tr><th>Ticker</th><th>Call / put activity</th><th>Call vol</th><th>Put vol</th><th>P/C vol</th><th>Est. premium activity</th><th>Open interest</th><th>Source</th></tr></thead>
          <tbody>
            {rows.map(row => (
              <tr key={row.underlying} className={row.underlying === selected ? 'is-selected' : ''}>
                <td><button type="button" className="flow-ticker" onClick={() => onSelect(row.underlying)}>{row.underlying}<small>{row.asset_type}</small></button></td>
                <td><ActivitySplit row={row} /><small className="flow-split-label"><span>{formatPercent(row.total_volume ? row.call_volume / row.total_volume : 0)}</span><span>{formatPercent(row.total_volume ? row.put_volume / row.total_volume : 0)}</span></small></td>
                <td className="flow-call">{formatInteger(row.call_volume)}</td>
                <td className="flow-put">{formatInteger(row.put_volume)}</td>
                <td>{formatRatio(row.put_call_volume_ratio)}</td>
                <td>{formatCurrency(row.total_premium_activity)}<small>{formatPercent(row.premium_activity_contract_count / Math.max(row.contract_count, 1))} marked</small></td>
                <td>{formatInteger(row.total_open_interest)}<small>P/C OI {formatRatio(row.put_call_open_interest_ratio)}</small></td>
                <td>{formatDateTime(row.market_data_time)}<small>{formatPercent(row.retention_fraction)} retained</small></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

/** Nov 12 (user): the expiry chart can leave out today's expiring contracts (0 DTE dominates SPY's volume) and label the
 * call and put bars with their share of the expiry; the total column then also shows the expiry's share of the shown
 * total. Both are display choices only. */
export function expiryProfileRows(rows: OptionFlowExpiration[], includeZeroDte: boolean): OptionFlowExpiration[] {
  return includeZeroDte ? rows : rows.filter(row => row.calendar_dte > 0)
}

function ExpiryProfile({ rows, metric, showPercent }: { rows: OptionFlowExpiration[]; metric: ProfileMetric; showPercent: boolean }) {
  const maximum = Math.max(1, ...rows.map(row => metricValues(row, metric).reduce((total, value) => total + value, 0)))
  const shown = rows.reduce((sum, row) => sum + metricValues(row, metric).reduce((total, value) => total + value, 0), 0)
  if (!rows.length) return <div className="flow-expiry-profile"><p className="flow-expiry-empty">No expiries to show.</p></div>
  return (
    <div className="flow-expiry-profile">
      <div className="flow-expiry-legend"><span className="flow-call">Call</span><span className="flow-put">Put</span><span>{showPercent ? 'Total · share · P/C' : 'Total · P/C'}</span></div>
      {rows.map(row => {
        const [calls, puts] = metricValues(row, metric)
        const total = calls + puts
        const callShare = total > 0 ? calls / total : 0.5
        return (
          <div className="flow-expiry-row" key={row.expiration_date}>
            <div><strong>{formatDate(row.expiration_date)}</strong><small>{row.calendar_dte} DTE</small></div>
            <div className="flow-expiry-track">
              <div className="flow-expiry-stack" style={{ width: `${total / maximum * 100}%` }}>
                <span className="flow-expiry-stack__call" style={{ width: `${callShare * 100}%` }}>{showPercent && total > 0 && <em>{formatPercent(callShare)}</em>}</span>
                <span className="flow-expiry-stack__put" style={{ width: `${(1 - callShare) * 100}%` }}>{showPercent && total > 0 && <em>{formatPercent(1 - callShare)}</em>}</span>
              </div>
            </div>
            <div className="flow-expiry-total"><strong>{formatCompact(total)}</strong><small>{showPercent ? `${formatPercent(shown > 0 ? total / shown : 0)} · ` : ''}P/C {formatRatio(calls > 0 ? puts / calls : null)}</small></div>
          </div>
        )
      })}
    </div>
  )
}

type ContractSortKey = 'contract' | 'expiry' | 'volume' | 'open_interest' | 'ratio' | 'mark' | 'premium' | 'delta' | 'distance'

const CONTRACT_COLUMNS: { key: ContractSortKey; label: string }[] = [
  { key: 'contract', label: 'Contract' }, { key: 'expiry', label: 'Expiry' }, { key: 'volume', label: 'Volume' },
  { key: 'open_interest', label: 'Open interest' }, { key: 'ratio', label: 'Vol / OI' }, { key: 'mark', label: 'Aligned mark' },
  { key: 'premium', label: 'Est. premium activity' }, { key: 'delta', label: 'Delta' }, { key: 'distance', label: 'Distance' },
]

function contractSortValue(contract: OptionFlowContract, key: ContractSortKey): number | string | null {
  const number = (value: string | number | null | undefined) => value == null || value === '' ? null : Number(value)
  switch (key) {
    case 'contract': return `${contract.contract_type}:${String(Number(contract.strike)).padStart(12, '0')}`
    case 'expiry': return contract.expiration_date
    case 'volume': return number(contract.day_volume)
    case 'open_interest': return number(contract.open_interest)
    case 'ratio': return number(contract.volume_open_interest_ratio)
    case 'mark': return number(contract.model_mark)
    case 'premium': return number(contract.premium_activity)
    case 'delta': return number(contract.local_delta)
    case 'distance': return number(contract.moneyness_fraction)
  }
}

/** Nov 12 (user): click a column header to sort the high-activity contracts; no sort keeps the server's ranking.
 * Missing values (N/A) always go last. */
export function sortContracts(rows: OptionFlowContract[], key: ContractSortKey | null, descending: boolean): OptionFlowContract[] {
  if (!key) return rows
  return [...rows].sort((left, right) => {
    const a = contractSortValue(left, key), b = contractSortValue(right, key)
    if (a == null || b == null) return a == null && b == null ? 0 : a == null ? 1 : -1
    const order = typeof a === 'number' && typeof b === 'number' ? a - b : String(a).localeCompare(String(b))
    return descending ? -order : order
  })
}

function aggregateStrikes(rows: OptionFlowStrike[]): OptionFlowStrike[] {
  const grouped = new Map<string, OptionFlowStrike>()
  rows.forEach(row => {
    const current = grouped.get(row.strike)
    if (current) {
      current.contract_count += row.contract_count
      current.call_volume += row.call_volume
      current.put_volume += row.put_volume
      current.call_open_interest += row.call_open_interest
      current.put_open_interest += row.put_open_interest
    } else {
      grouped.set(row.strike, { ...row, expiration_date: 'ALL' })
    }
  })
  return [...grouped.values()]
}

function StrikeProfile({ rows, metric }: { rows: OptionFlowStrike[]; metric: ProfileMetric }) {
  const ranked = [...rows]
    .sort((left, right) => metricValues(right, metric).reduce((a, b) => a + b, 0) - metricValues(left, metric).reduce((a, b) => a + b, 0))
    .slice(0, 32)
    .sort((left, right) => Number(left.strike) - Number(right.strike))
  const maximum = Math.max(1, ...ranked.flatMap(row => metricValues(row, metric)))
  return (
    <div className="flow-strike-profile">
      <div className="flow-strike-legend"><span className="flow-put">Put</span><span>Strike</span><span className="flow-call">Call</span></div>
      {ranked.map(row => {
        const [calls, puts] = metricValues(row, metric)
        return (
          <div className="flow-strike-row" key={row.strike}>
            <div className="flow-strike-half flow-strike-half--put"><b>{formatCompact(puts)}</b><span style={{ width: `${puts / maximum * 100}%` }} /></div>
            <strong>{Number(row.strike).toLocaleString('en-US', { maximumFractionDigits: 2 })}</strong>
            <div className="flow-strike-half flow-strike-half--call"><span style={{ width: `${calls / maximum * 100}%` }} /><b>{formatCompact(calls)}</b></div>
          </div>
        )
      })}
    </div>
  )
}

const VOLUME_INCOMPLETE_NOTE = 'Volume may be slightly off: a trade correction could not be matched to its original trade.'

/** Known holes in a session's Options data, so a short total says so. */
const OPTION_SESSION_NOTICES: Record<string, string> = {
  '2026-11-05': 'Options trades were not captured from 10:18:12 to 10:33:53 AM ET, or for under a minute each at about 10:42, 10:48 and 10:53 AM ET, so estimated premium activity and flow events are short by those periods. No Options updates were published for 9:55 to 10:45 AM ET.',
}

export function optionSessionNotice(session: string | null | undefined): string | null {
  return session ? OPTION_SESSION_NOTICES[session] ?? null : null
}

export function OptionSessionNotice({ session }: { session: string | null | undefined }) {
  const notice = optionSessionNotice(session)
  if (!notice) return null
  return <div className="flow-state flow-state--warning" role="status"><AlertTriangle size={20} /><div><strong>Incomplete session</strong><span>{notice}</span></div></div>
}

/** R48 (Nov 11): say when the full chain is older than the latest capture, or not shown at all. */
export function breadthCoverageWarning(data: {
  selected?: string; coverage_source?: string
  breadth?: { breadth_state?: string; breadth_observed_at?: string; breadth_reason?: string | null
    latest_capture_state?: string; latest_capture_at?: string; latest_capture_reason?: string | null }
  breadth_fallback?: { selected: boolean; streamed_only: string[] } | null
  selected_summary?: { contract_count?: number } | null
}, format: (value: string | undefined) => string): { title: string; detail: string } | null {
  const breadth = data.breadth
  const others = (data.breadth_fallback?.streamed_only || []).filter(ticker => ticker !== data.selected)
  const othersNote = others.length ? ` Streamed contracts only for ${others.join(', ')}.` : ''
  if (data.breadth_fallback?.selected) {
    const state = breadth?.breadth_state && breadth.breadth_state !== 'NOT_CAPTURED'
      ? `latest chain capture ${breadth.breadth_state.toLowerCase()}${breadth.breadth_observed_at ? ` at ${format(breadth.breadth_observed_at)}` : ''}`
      : 'no chain capture yet this session'
    return { title: 'Full chain unavailable',
      detail: `${data.selected}: ${state}${breadth?.breadth_reason ? ` (${breadth.breadth_reason})` : ''}. Showing the ${data.selected_summary?.contract_count ?? ''} streamed contracts until the next capture.${othersNote}` }
  }
  if (data.coverage_source === 'BREADTH' && breadth?.latest_capture_state) {
    return { title: 'Latest chain capture failed',
      detail: `${data.selected}: capture at ${format(breadth.latest_capture_at)} ${breadth.latest_capture_state.toLowerCase()}${breadth.latest_capture_reason ? ` (${breadth.latest_capture_reason})` : ''}. Showing the chain captured at ${format(breadth.breadth_observed_at)}.${othersNote}` }
  }
  if (others.length) return { title: 'Partial chain coverage', detail: othersNote.trim() }
  return null
}

export default function OptionsFlowPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const ticker = (searchParams.get('underlyer') || 'SPY').toUpperCase()
  const { pinned: sessionDate } = useSessionDate()
  const [profileMetric, setProfileMetric] = useState<ProfileMetric>('volume')
  const [strikeExpiration, setStrikeExpiration] = useState('ALL')
  const [includeZeroDte, setIncludeZeroDte] = useState(true)
  const [showExpiryPercent, setShowExpiryPercent] = useState(false)
  const [contractSort, setContractSort] = useState<{ key: ContractSortKey | null; descending: boolean }>({ key: null, descending: true })
  // Nov 12 (user): read when the next window's data lands (its release and, for breadth, its chain capture: first check
  // 50 s after each 5-minute mark, then every 10 s until both are in); 60 s when no live window is expected.
  const flow = useQuery({
    queryKey: ['options', 'flow', ticker, sessionDate],
    queryFn: () => getOptionFlow(ticker, sessionDate || undefined),
    refetchInterval: current => msUntilWindowRead(Date.now(), flowWindowMs(current.state.data), OPTIONS_FIRST_CHECK_MS,
      60_000),
  })
  const data = flow.data?.data
  const selected = data?.selected_summary

  useEffect(() => setStrikeExpiration('ALL'), [data?.selected, data?.session_date])

  usePublishPageContext({
    eyebrow: `${optionsDataTierLabel(flow.data?.data_tier)} options activity`,
    title: 'Options Flow',
    detail: 'Call and put activity, positioning change, and contract concentration for one selected underlying.',
    status: [{ label: 'Data', value: optionsDataTierLabel(flow.data?.data_tier) }],
    session: '5m',
  })

  if (flow.isLoading) return <div className="flow-state"><Database size={20} /><div><strong>Loading options activity</strong><span>Reading the latest complete option matrices.</span></div></div>
  if (flow.isError || !data || flow.data?.available === false) return <div className="flow-state flow-state--warning"><AlertTriangle size={20} /><div><strong>Options activity unavailable</strong><span>{optionFlowUnavailableReason(flow.data?.reason, flow.error)}</span></div></div>

  const strikeRows = strikeExpiration === 'ALL'
    ? aggregateStrikes(data.strikes)
    : data.strikes.filter(row => row.expiration_date === strikeExpiration)
  const selectedExpiration = data.expirations.find(row => row.expiration_date === strikeExpiration)
  return (
    <div className="options-flow-page">
      <OptionSessionNotice session={data.session_date} />
      <section className="flow-command" aria-label="Options flow filters">
        <label>Underlying<select value={data.selected} onChange={event => setSearchParams(current => {
          const next = new URLSearchParams(current)
          next.set('underlyer', event.target.value)
          return next
        })}>{data.underlyers.map(row => <option key={row.underlying} value={row.underlying}>{row.underlying} · {row.asset_type}</option>)}</select></label>
        <div className="flow-command__measure"><span>Chart measure</span><ProfileToggle value={profileMetric} onChange={setProfileMetric} /></div>
        <div className="flow-command__context"><Radio size={15} /><span><strong>{data.session_date ? formatDate(data.session_date) : 'Latest session'}</strong>{data.coverage_source === 'BREADTH' ? `Full listed chain (0-60 DTE, strikes within 20% of spot) · provider day volume and open interest · captured ${formatDateTime(data.breadth?.breadth_observed_at)}` : 'Selected ticker · latest complete matrix in this exchange session'}</span></div>
      </section>
      {(() => {
        const warning = breadthCoverageWarning(data, value => formatDateTime(value))
        return warning && <div className="flow-state flow-state--warning" role="status"><AlertTriangle size={20} /><div><strong>{warning.title}</strong><span>{warning.detail}</span></div></div>
      })()}
      {data.coverage_source === 'BREADTH' && <p className="flow-coverage-note" role="note">Volume and open interest cover every listed contract in range. Estimated premium activity uses the current quote midpoint. Flow events and alerts come from the {selected?.streamed_contract_count ?? 'streamed'} streamed contracts per underlying.</p>}

      {selected && <>
        <section className="flow-summary" aria-label={`${selected.underlying} activity summary`}>
          <div><span>Total volume</span><strong>{formatInteger(selected.total_volume)}</strong><small><i className="flow-call">{formatCompact(selected.call_volume)} calls</i><i className="flow-put">{formatCompact(selected.put_volume)} puts</i></small></div>
          <div><span>Total open interest</span><strong>{formatInteger(selected.total_open_interest)}</strong><small><i className="flow-call">{formatCompact(selected.call_open_interest)} calls</i><i className="flow-put">{formatCompact(selected.put_open_interest)} puts</i></small></div>
          <div><span>Put / call volume</span><strong>{formatRatio(selected.put_call_volume_ratio)}</strong><small>P/C open interest {formatRatio(selected.put_call_open_interest_ratio)}</small></div>
          <div><span>Est. premium activity</span><strong>{formatCurrency(selected.total_premium_activity)}</strong><small>{formatPercent(selected.premium_activity_contract_count / Math.max(selected.contract_count, 1))} mark coverage</small></div>
          <div><span>{selected.coverage_source === 'BREADTH' ? 'Chain coverage' : 'Matrix coverage'}</span><strong>{selected.contract_count}</strong><small>{selected.expiration_count} expiries · {selected.strike_count} strikes · {formatPercent(selected.retention_fraction)} retained</small></div>
        </section>

        <div className="flow-profile-grid">
          <section className="flow-section">
            <header className="flow-section__header flow-section__header--expiry"><div><Layers3 size={17} /><div><h2>{selected.underlying} expiry concentration</h2><p>Selected ticker only · total {profileMetric === 'volume' ? 'day volume' : 'open interest'} and call/put composition</p></div></div>
              <div className="flow-expiry-options">
                <label className="flow-check"><input type="checkbox" checked={includeZeroDte} onChange={event => setIncludeZeroDte(event.target.checked)} />Include 0 DTE</label>
                <label className="flow-check"><input type="checkbox" checked={showExpiryPercent} onChange={event => setShowExpiryPercent(event.target.checked)} />Show %</label>
                <span className="flow-expiry-options__count">{expiryProfileRows(data.expirations, includeZeroDte).length} expiries</span>
              </div>
            </header>
            <ExpiryProfile rows={expiryProfileRows(data.expirations, includeZeroDte)} metric={profileMetric} showPercent={showExpiryPercent} />
          </section>
          <section className="flow-section">
            <header className="flow-section__header flow-section__header--strike"><div><BarChart3 size={17} /><div><h2>{selected.underlying} strike concentration</h2><p>{strikeExpiration === 'ALL' ? `Selected ticker only · ${data.expirations.length} expiries aggregated` : `Selected ticker only · ${formatDate(strikeExpiration)} · ${selectedExpiration?.calendar_dte ?? '—'} DTE`} · showing {Math.min(32, strikeRows.length)} of {strikeRows.length} strikes</p></div></div><label className="flow-scope-select">Expiry<select value={strikeExpiration} onChange={event => setStrikeExpiration(event.target.value)}><option value="ALL">All retained expiries</option>{data.expirations.map(row => <option key={row.expiration_date} value={row.expiration_date}>{formatDate(row.expiration_date)} · {row.calendar_dte} DTE</option>)}</select></label></header>
            <StrikeProfile rows={strikeRows} metric={profileMetric} />
          </section>
        </div>

        <section className="flow-section flow-contracts">
          <header className="flow-section__header"><div><Activity size={17} /><div><h2>{selected.underlying} high-activity contracts</h2><p>Selected ticker only · ranked by estimated premium activity in the retained matrix</p></div></div><span>{data.top_contracts.length} contracts</span></header>
          <div className="flow-table-wrap">
            <table className="flow-table">
              <thead><tr>{CONTRACT_COLUMNS.map(column => {
                const active = contractSort.key === column.key
                return <th key={column.key} aria-sort={active ? (contractSort.descending ? 'descending' : 'ascending') : 'none'}>
                  <button type="button" className={`flow-sort${active ? ' is-active' : ''}`} title="Sort by this column; click again to reverse, a third time for the default ranking"
                    onClick={() => setContractSort(current => current.key !== column.key
                      ? { key: column.key, descending: true }
                      : current.descending ? { key: column.key, descending: false } : { key: null, descending: true })}>
                    {column.label}{active ? (contractSort.descending ? ' ▼' : ' ▲') : ''}
                  </button></th>
              })}</tr></thead>
              {/* Nov 12: keyed by contract ticker; breadth rows (not streamed) all carry contract_id 0, and the shared key
                  made React reuse rows on re-render and sort, which showed as duplicate records. */}
              <tbody>{sortContracts(data.top_contracts, contractSort.key, contractSort.descending).map(contract => <tr key={contract.contract_ticker}>
                <td><strong className={contract.contract_type === 'CALL' ? 'flow-call' : 'flow-put'}>{Number(contract.strike).toLocaleString('en-US', { maximumFractionDigits: 2 })} {contract.contract_type}</strong><small title={contract.contract_ticker}>{contract.contract_ticker}</small></td>
                <td>{formatDate(contract.expiration_date)}<small>{contract.calendar_dte} DTE</small></td>
                <td title={contract.volume_baseline === 'INCOMPLETE' ? VOLUME_INCOMPLETE_NOTE : undefined}>{formatInteger(contract.day_volume)}{contract.volume_baseline === 'INCOMPLETE' ? ' *' : ''}</td>
                <td>{contract.open_interest == null ? 'N/A' : formatInteger(contract.open_interest)}</td>
                <td>{formatRatio(contract.volume_open_interest_ratio)}</td>
                <td>{contract.model_mark ? `$${Number(contract.model_mark).toFixed(2)}` : 'N/A'}</td>
                <td>{contract.premium_activity == null ? 'N/A' : formatCurrency(contract.premium_activity)}</td>
                <td>{contract.local_delta == null ? 'N/A' : contract.local_delta.toFixed(3)}</td>
                <td>{(Number(contract.moneyness_fraction) * 100).toFixed(2)}%</td>
              </tr>)}</tbody>
            </table>
          </div>
          {data.top_contracts.some(contract => contract.volume_baseline === 'INCOMPLETE') && <small role="note">* {VOLUME_INCOMPLETE_NOTE}</small>}
        </section>
      </>}
    </div>
  )
}