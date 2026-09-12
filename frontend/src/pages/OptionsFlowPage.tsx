import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Clock3,
  Database,
  Info,
  Layers3,
  Radio,
} from 'lucide-react'
import { usePublishPageContext } from '../layout/pageContext'
import { useSessionDate } from '../layout/sessionDate'
import {
  getOptionFlow,
  type OptionFlowExpiration,
  type OptionFlowStrike,
  type OptionFlowSummary,
} from '../services/api'
import './OptionsFlowPage.css'

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
const formatSigned = (value: number | null | undefined) => `${Number(value || 0) > 0 ? '+' : ''}${formatCompact(value)}`
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
        <div><Radio size={17} /><div><h2>Tracked universe</h2><p>Latest complete matrix for each configured underlying</p></div></div>
        <span>{rows.length} underlyings</span>
      </header>
      <div className="flow-table-wrap">
        <table className="flow-table flow-table--board">
          <thead><tr><th>Ticker</th><th>Call / put activity</th><th>Call vol</th><th>Put vol</th><th>P/C vol</th><th>Est. premium activity</th><th>Matched OI change</th><th>Source</th></tr></thead>
          <tbody>
            {rows.map(row => (
              <tr key={row.underlying} className={row.underlying === selected ? 'is-selected' : ''}>
                <td><button type="button" className="flow-ticker" onClick={() => onSelect(row.underlying)}>{row.underlying}<small>{row.asset_type}</small></button></td>
                <td><ActivitySplit row={row} /><small className="flow-split-label"><span>{formatPercent(row.total_volume ? row.call_volume / row.total_volume : 0)}</span><span>{formatPercent(row.total_volume ? row.put_volume / row.total_volume : 0)}</span></small></td>
                <td className="flow-call">{formatInteger(row.call_volume)}</td>
                <td className="flow-put">{formatInteger(row.put_volume)}</td>
                <td>{formatRatio(row.put_call_volume_ratio)}</td>
                <td>{formatCurrency(row.total_premium_activity)}<small>{formatPercent(row.premium_activity_contract_count / Math.max(row.contract_count, 1))} marked</small></td>
                <td>{row.open_interest_change ? formatSigned(row.open_interest_change.total_open_interest_change) : 'Pending'}<small>{row.open_interest_change ? `${formatDate(row.open_interest_change.prior_settlement_session)} → ${formatDate(row.open_interest_change.settlement_session)}` : 'Needs two captures'}</small></td>
                <td>{formatDateTime(row.market_data_time)}<small>{formatPercent(row.retention_fraction)} retained</small></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

function ExpiryProfile({ rows, metric }: { rows: OptionFlowExpiration[]; metric: ProfileMetric }) {
  const maximum = Math.max(1, ...rows.map(row => metricValues(row, metric).reduce((total, value) => total + value, 0)))
  return (
    <div className="flow-expiry-profile">
      <div className="flow-expiry-legend"><span className="flow-call">Call</span><span className="flow-put">Put</span><span>Total · P/C</span></div>
      {rows.map(row => {
        const [calls, puts] = metricValues(row, metric)
        const total = calls + puts
        const callShare = total > 0 ? calls / total : 0.5
        return (
          <div className="flow-expiry-row" key={row.expiration_date}>
            <div><strong>{formatDate(row.expiration_date)}</strong><small>{row.calendar_dte} DTE</small></div>
            <div className="flow-expiry-track">
              <div className="flow-expiry-stack" style={{ width: `${total / maximum * 100}%` }}>
                <span className="flow-expiry-stack__call" style={{ width: `${callShare * 100}%` }} />
                <span className="flow-expiry-stack__put" style={{ width: `${(1 - callShare) * 100}%` }} />
              </div>
            </div>
            <div className="flow-expiry-total"><strong>{formatCompact(total)}</strong><small>P/C {formatRatio(calls > 0 ? puts / calls : null)}</small></div>
          </div>
        )
      })}
    </div>
  )
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

export default function OptionsFlowPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const ticker = (searchParams.get('underlyer') || 'SPY').toUpperCase()
  const { pinned: sessionDate } = useSessionDate()
  const [profileMetric, setProfileMetric] = useState<ProfileMetric>('volume')
  const [strikeExpiration, setStrikeExpiration] = useState('ALL')
  const flow = useQuery({
    queryKey: ['options', 'flow', ticker, sessionDate],
    queryFn: () => getOptionFlow(ticker, sessionDate || undefined),
    refetchInterval: 60_000,
  })
  const data = flow.data?.data
  const selected = data?.selected_summary

  useEffect(() => setStrikeExpiration('ALL'), [data?.selected, data?.session_date])

  usePublishPageContext({
    eyebrow: 'Delayed options activity',
    title: 'Options Flow',
    detail: 'Call and put activity, positioning change, and contract concentration for one selected underlying.',
    status: [
      { label: 'Underlying', value: data?.selected || ticker },
      { label: 'Source', value: formatDateTime(flow.data?.as_of), note: flow.data?.data_tier || 'Loading' },
    ],
    session: '1d',
  })

  if (flow.isLoading) return <div className="flow-state"><Database size={20} /><div><strong>Loading options activity</strong><span>Reading the latest complete option matrices.</span></div></div>
  if (flow.isError || !data || flow.data?.available === false) return <div className="flow-state flow-state--warning"><AlertTriangle size={20} /><div><strong>Options activity unavailable</strong><span>{flow.data?.reason || 'The options service could not be read.'}</span></div></div>

  const oiChange = selected?.open_interest_change
  const strikeRows = strikeExpiration === 'ALL'
    ? aggregateStrikes(data.strikes)
    : data.strikes.filter(row => row.expiration_date === strikeExpiration)
  const selectedExpiration = data.expirations.find(row => row.expiration_date === strikeExpiration)
  return (
    <div className="options-flow-page">
      <section className="flow-capability">
        <Info size={16} />
        <div><strong>Activity, not buyer/seller premium flow</strong><span>Developer data has no NBBO aggressor side. Put bars are plotted opposite calls for comparison only; estimated premium activity uses the latest aligned mark.</span></div>
        <span className="flow-delay"><Clock3 size={13} />15-minute delayed</span>
      </section>
      {data.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && <section className="flow-state flow-state--warning"><AlertTriangle size={20} /><div><strong>Historical policy matrix</strong><span>The active valuation policy has not completed this underlying yet. Activity below uses the latest prior-policy matrix.</span></div></section>}

      <section className="flow-command" aria-label="Options flow filters">
        <label>Underlying<select value={data.selected} onChange={event => setSearchParams({ underlyer: event.target.value })}>{data.underlyers.map(row => <option key={row.underlying} value={row.underlying}>{row.underlying} · {row.asset_type}</option>)}</select></label>
        <div className="flow-command__measure"><span>Chart measure</span><ProfileToggle value={profileMetric} onChange={setProfileMetric} /></div>
        <div className="flow-command__context"><Radio size={15} /><span><strong>{data.session_date ? formatDate(data.session_date) : 'Latest session'}</strong>Selected ticker · latest complete matrix in this exchange session</span></div>
      </section>

      {selected && <>
        <section className="flow-summary" aria-label={`${selected.underlying} activity summary`}>
          <div><span>Total volume</span><strong>{formatInteger(selected.total_volume)}</strong><small><i className="flow-call">{formatCompact(selected.call_volume)} calls</i><i className="flow-put">{formatCompact(selected.put_volume)} puts</i></small></div>
          <div><span>Put / call volume</span><strong>{formatRatio(selected.put_call_volume_ratio)}</strong><small>P/C open interest {formatRatio(selected.put_call_open_interest_ratio)}</small></div>
          <div><span>Est. premium activity</span><strong>{formatCurrency(selected.total_premium_activity)}</strong><small>{formatPercent(selected.premium_activity_contract_count / Math.max(selected.contract_count, 1))} mark coverage</small></div>
          <div><span>Matched OI change</span><strong>{oiChange ? formatSigned(oiChange.total_open_interest_change) : 'Pending'}</strong><small>{oiChange ? `${formatPercent(oiChange.matched_coverage_fraction)} matched · ${formatDate(oiChange.settlement_session)}` : 'Two settlements required'}</small></div>
          <div><span>Matrix coverage</span><strong>{selected.contract_count}</strong><small>{selected.expiration_count} expiries · {selected.strike_count} strikes · {formatPercent(selected.retention_fraction)} retained</small></div>
        </section>

        <div className="flow-profile-grid">
          <section className="flow-section">
            <header className="flow-section__header"><div><Layers3 size={17} /><div><h2>{selected.underlying} expiry concentration</h2><p>Selected ticker only · total {profileMetric === 'volume' ? 'day volume' : 'open interest'} and call/put composition</p></div></div><span>{data.expirations.length} expiries</span></header>
            <ExpiryProfile rows={data.expirations} metric={profileMetric} />
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
              <thead><tr><th>Contract</th><th>Expiry</th><th>Volume</th><th>Open interest</th><th>Vol / OI</th><th>Aligned mark</th><th>Est. premium activity</th><th>Delta</th><th>Distance</th></tr></thead>
              <tbody>{data.top_contracts.map(contract => <tr key={contract.contract_id}>
                <td><strong className={contract.contract_type === 'CALL' ? 'flow-call' : 'flow-put'}>{Number(contract.strike).toLocaleString('en-US', { maximumFractionDigits: 2 })} {contract.contract_type}</strong><small title={contract.contract_ticker}>{contract.contract_ticker}</small></td>
                <td>{formatDate(contract.expiration_date)}<small>{contract.calendar_dte} DTE</small></td>
                <td>{formatInteger(contract.day_volume)}</td>
                <td>{contract.open_interest == null ? 'N/A' : formatInteger(contract.open_interest)}</td>
                <td>{formatRatio(contract.volume_open_interest_ratio)}</td>
                <td>{contract.model_mark ? `$${Number(contract.model_mark).toFixed(2)}` : 'N/A'}</td>
                <td>{formatCurrency(contract.premium_activity)}</td>
                <td>{contract.local_delta == null ? 'N/A' : contract.local_delta.toFixed(3)}</td>
                <td>{formatPercent(Number(contract.moneyness_fraction))}</td>
              </tr>)}</tbody>
            </table>
          </div>
        </section>
      </>}
    </div>
  )
}