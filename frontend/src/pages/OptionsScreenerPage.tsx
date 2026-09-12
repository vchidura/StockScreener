import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { useSearchParams } from 'react-router-dom'
import { AlertTriangle, Database, Filter, Info, RotateCcw, ScanSearch, SlidersHorizontal } from 'lucide-react'
import { usePublishPageContext } from '../layout/pageContext'
import { useSessionDate } from '../layout/sessionDate'
import {
  getOptionScreener,
  type OptionScreenerScope,
  type OptionScreenerSort,
} from '../services/api'
import './OptionsScreenerPage.css'

const PRESETS = [
  { key: 'all', label: 'All detected', scope: 'ALL' as const },
  { key: 'unusual-calls', label: 'Unusual Calls', scope: 'RESEARCH' as const, strategy: 'VOLUME_OI_ANOMALY', type: 'CALL' as const },
  { key: 'unusual-puts', label: 'Unusual Puts', scope: 'RESEARCH' as const, strategy: 'VOLUME_OI_ANOMALY', type: 'PUT' as const },
  { key: 'structured', label: 'Structured legs', scope: 'STRUCTURED' as const },
  { key: 'sweep', label: 'Sweep-like', scope: 'RESEARCH' as const, strategy: 'SWEEP_LIKE_CLUSTER' },
  { key: 'smile', label: 'Smile distortion', scope: 'RESEARCH' as const, strategy: 'VOLATILITY_SMILE_DISTORTION' },
  { key: 'long-calls', label: 'Long-Dated Calls', scope: 'ALL' as const, type: 'CALL' as const, minimumDte: 21 },
  { key: 'long-puts', label: 'Long-Dated Puts', scope: 'ALL' as const, type: 'PUT' as const, minimumDte: 21 },
  { key: 'board', label: 'Published Board', scope: 'BOARD' as const },
]

const compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })
const integer = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })
const money = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', notation: 'compact', maximumFractionDigits: 1 })
const formatCompact = (value: number | string | null | undefined) => compact.format(Number(value || 0))
const formatInteger = (value: number | null | undefined) => value == null ? 'N/A' : integer.format(value)
const formatMoney = (value: number | string | null | undefined) => value == null ? 'N/A' : money.format(Number(value))
const percent = (value: number | string | null | undefined, digits = 1) => value == null ? 'N/A' : `${(Number(value) * 100).toFixed(digits)}%`
const signed = (value: number | null | undefined) => value == null ? 'N/A' : `${value > 0 ? '+' : ''}${formatCompact(value)}`
const date = (value: string | null | undefined) => value ? new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(new Date(`${value}T12:00:00Z`)) : 'N/A'
const time = (value: string | null | undefined) => value ? new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit' }).format(new Date(value)) : 'N/A'
const readable = (value: string) => value.replace(/_/g, ' ')
const numberParam = (params: URLSearchParams, name: string, fallback: number) => {
  const raw = params.get(name)
  if (raw == null || raw.trim() === '') return fallback
  const value = Number(raw)
  return Number.isFinite(value) && value >= 0 ? value : fallback
}

export default function OptionsScreenerPage() {
  const [params, setParams] = useSearchParams()
  const [filtersOpen, setFiltersOpen] = useState(() => (
    ['underlyer', 'min_volume', 'min_oi', 'min_ratio', 'sort'].some(name => params.has(name))
    || (!params.has('preset') && ['type', 'min_dte', 'max_dte'].some(name => params.has(name)))
  ))
  const { pinned: sessionDate } = useSessionDate()
  const preset = PRESETS.find(item => item.key === params.get('preset')) || PRESETS[0]
  const underlyer = params.get('underlyer') || 'ALL'
  const contractType = params.get('type') || 'ALL'
  const minimumDte = numberParam(params, 'min_dte', 0)
  const maximumDte = numberParam(params, 'max_dte', 60)
  const minimumVolume = numberParam(params, 'min_volume', 0)
  const minimumOi = numberParam(params, 'min_oi', 0)
  const minimumRatio = numberParam(params, 'min_ratio', 0)
  const sort = (params.get('sort') || 'PREMIUM_ACTIVITY') as OptionScreenerSort
  const offset = numberParam(params, 'offset', 0)
  const query = useQuery({
    queryKey: ['options', 'screener', sessionDate, preset.key, underlyer, contractType, minimumDte, maximumDte, minimumVolume, minimumOi, minimumRatio, sort, offset],
    queryFn: () => getOptionScreener({
      session_date: sessionDate || undefined,
      scope: preset.scope as OptionScreenerScope,
      strategy: preset.strategy,
      underlyer: underlyer === 'ALL' ? undefined : underlyer,
      contract_type: contractType === 'ALL' ? undefined : contractType as 'CALL' | 'PUT',
      minimum_dte: minimumDte,
      maximum_dte: maximumDte,
      minimum_volume: minimumVolume,
      minimum_open_interest: minimumOi,
      minimum_volume_oi_ratio: minimumRatio,
      sort,
      limit: 100,
      offset,
    }),
    refetchInterval: 60_000,
  })
  const data = query.data?.data
  const rows = data?.rows || []
  const calls = rows.filter(row => row.contract_type === 'CALL').length
  const puts = rows.length - calls
  const update = (values: Record<string, string | null>) => {
    const next = new URLSearchParams(params)
    Object.entries(values).forEach(([key, value]) => value ? next.set(key, value) : next.delete(key))
    if (!('offset' in values)) next.delete('offset')
    setParams(next)
  }
  const applyPreset = (item: typeof PRESETS[number]) => {
    const next = new URLSearchParams()
    if (item.key !== 'all') next.set('preset', item.key)
    if (item.type) next.set('type', item.type)
    if (item.minimumDte) next.set('min_dte', String(item.minimumDte))
    setParams(next)
  }

  usePublishPageContext({
    eyebrow: 'Contract discovery',
    title: 'Options Screener',
    detail: 'Filter contracts referenced by persisted strategy detections without inferring executable or buyer/seller flow.',
    status: [
      { label: 'Matches', value: data ? integer.format(data.total) : 'Loading' },
      { label: 'Scope', value: preset.label },
    ],
    session: '1d',
  })

  return <div className="option-screener">
    <section className="screener-terminal-bar">
      <nav className="screener-presets" aria-label="Options screener presets">
        {PRESETS.map(item => <button type="button" key={item.key} className={preset.key === item.key ? 'active' : ''} onClick={() => applyPreset(item)}>{item.label}</button>)}
      </nav>
      <div className="screener-actions">
        <span title="Delayed research marks; no quote aggressor side"><Info size={13} />Delayed · no NBBO direction</span>
        <button type="button" className={filtersOpen ? 'active' : ''} aria-expanded={filtersOpen} onClick={() => setFiltersOpen(value => !value)}><SlidersHorizontal size={14} />Filters</button>
        <button type="button" title="Reset scanner" aria-label="Reset scanner" onClick={() => { setParams(new URLSearchParams()); setFiltersOpen(false) }}><RotateCcw size={14} /></button>
      </div>
    </section>
    {filtersOpen && <section className="screener-filters" aria-label="Contract filters">
      <div className="screener-filter-heading"><Filter size={15} /><strong>Advanced filters</strong></div>
      <label>Underlying<select value={underlyer} onChange={event => update({ underlyer: event.target.value === 'ALL' ? null : event.target.value })}><option value="ALL">All tracked</option>{(data?.underlyers || []).map(value => <option key={value}>{value}</option>)}</select></label>
      <label>Type<select value={contractType} onChange={event => update({ type: event.target.value === 'ALL' ? null : event.target.value, preset: null })}><option value="ALL">Calls + puts</option><option value="CALL">Calls</option><option value="PUT">Puts</option></select></label>
      <label>DTE range<span className="screener-range"><input aria-label="Minimum DTE" type="number" min="0" value={minimumDte} onChange={event => update({ min_dte: event.target.value === '0' ? null : event.target.value, preset: null })} /><i>to</i><input aria-label="Maximum DTE" type="number" min="0" value={maximumDte} onChange={event => update({ max_dte: event.target.value === '60' ? null : event.target.value, preset: null })} /></span></label>
      <label>Min volume<input type="number" min="0" value={minimumVolume} onChange={event => update({ min_volume: event.target.value === '0' ? null : event.target.value })} /></label>
      <label>Min OI<input type="number" min="0" value={minimumOi} onChange={event => update({ min_oi: event.target.value === '0' ? null : event.target.value })} /></label>
      <label>Min Vol / OI<input type="number" min="0" step="0.5" value={minimumRatio} onChange={event => update({ min_ratio: event.target.value === '0' ? null : event.target.value })} /></label>
      <label>Sort<select value={sort} onChange={event => update({ sort: event.target.value === 'PREMIUM_ACTIVITY' ? null : event.target.value })}><option value="PREMIUM_ACTIVITY">Premium activity</option><option value="VOLUME">Volume</option><option value="OPEN_INTEREST">Open interest</option><option value="VOLUME_OI">Volume / OI</option><option value="OI_CHANGE">Absolute OI change</option><option value="IV">Implied volatility</option></select></label>
    </section>}
    {query.isLoading && <div className="screener-state"><Database size={18} /><span>Loading detected contracts…</span></div>}
    {query.isError && <div className="screener-state is-warning"><AlertTriangle size={18} /><span>Options Screener data is unavailable.</span></div>}
    {!query.isLoading && !query.isError && data?.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && <div className="screener-state is-warning"><AlertTriangle size={18} /><span>Showing the latest prior-policy detections while the active valuation policy builds its first complete cohort.</span></div>}
    {!query.isLoading && !query.isError && <>
      <section className="screener-summary"><span><strong>{data?.total || 0}</strong> matched</span><span className="is-call"><strong>{calls}</strong> calls shown</span><span className="is-put"><strong>{puts}</strong> puts shown</span><span><strong>{rows.filter(row => row.board_position != null).length}</strong> published Board rows shown</span></section>
      <section className="screener-table-panel">
        <header><div><ScanSearch size={17} /><div><h2>{preset.label}</h2><p>{preset.key === 'board' ? 'Exact contracts referenced by the latest immutable Board publication.' : 'Latest detected contract per underlying and source matrix in the selected session.'}</p></div></div><span>{rows.length} shown</span></header>
        <div className="screener-table-wrap"><table className="screener-table"><thead><tr><th>Ticker</th><th>Contract</th><th>DTE</th><th>Stock</th><th>OTM</th><th title="Latest aligned delayed mark">Last</th><th title="Change from the same contract's prior complete matrix">Chg %</th><th>Vol</th><th>OI</th><th>OI +/−</th><th>OI +/− %</th><th>Vol / OI</th><th title="Estimated premium activity, not transacted premium">Prem</th><th>IV</th><th title="Local IV change from the prior complete matrix">IV Chg</th><th>Delta</th><th>Gamma</th><th>Detected by</th><th>Board</th><th>Source</th></tr></thead>
        <tbody>{rows.length ? rows.map(row => <tr key={row.snapshot_id}><td className="screener-symbol"><Link to={`/options/flow?underlyer=${row.underlying}`}>{row.underlying}</Link></td><td><Link className={row.contract_type === 'CALL' ? 'is-call' : 'is-put'} to={`/options/explorer/${row.underlying}?expiration=${row.expiration_date}`}><strong>{Number(row.strike).toLocaleString('en-US', { maximumFractionDigits: 2 })} {row.contract_type}</strong><small>{date(row.expiration_date)} · {row.contract_ticker}</small></Link></td><td>{row.calendar_dte}</td><td>${Number(row.spot).toFixed(2)}</td><td className={Number(row.otm_fraction) >= 0 ? 'is-positive' : 'is-negative'}>{percent(row.otm_fraction)}</td><td>{row.mark == null ? 'N/A' : `$${Number(row.mark).toFixed(2)}`}</td><td className={Number(row.mark_change_fraction) >= 0 ? 'is-positive' : 'is-negative'}>{percent(row.mark_change_fraction)}</td><td>{formatInteger(row.day_volume)}</td><td>{formatInteger(row.open_interest)}</td><td className={Number(row.open_interest_change) >= 0 ? 'is-positive' : 'is-negative'}>{signed(row.open_interest_change)}</td><td>{percent(row.open_interest_change_fraction)}</td><td>{row.volume_open_interest_ratio == null ? 'N/A' : row.volume_open_interest_ratio.toFixed(2)}</td><td>{formatMoney(row.premium_activity)}</td><td>{percent(row.local_iv)}</td><td className={Number(row.iv_change) >= 0 ? 'is-positive' : 'is-negative'}>{percent(row.iv_change)}</td><td>{row.local_delta == null ? 'N/A' : row.local_delta.toFixed(3)}</td><td>{row.local_gamma == null ? 'N/A' : row.local_gamma.toFixed(4)}</td><td className="screener-detectors">{row.strategy_names.map(name => <span key={name}>{readable(name)}</span>)}</td><td>{row.board_position == null ? '—' : `#${row.board_position}`}<small>{row.best_candidate_rank ? `raw #${row.best_candidate_rank}` : ''}</small></td><td>{time(row.market_data_time)}<small>{row.oi_settlement_session ? `OI ${date(row.oi_settlement_session)}` : 'OI unavailable'}</small></td></tr>) : <tr><td colSpan={20} className="screener-empty">{preset.key === 'board' ? 'No immutable Board publication exists yet. The next complete option cycle will start prospective membership.' : 'No detected contracts match these filters.'}</td></tr>}</tbody></table></div>
        {data && <footer><span>{data.total ? `Showing ${data.offset + 1}-${data.offset + rows.length} of ${data.total}` : '0 results'}</span><div><button type="button" disabled={data.offset === 0} onClick={() => update({ offset: String(Math.max(0, data.offset - data.limit)) })}>Previous</button><button type="button" disabled={data.offset + rows.length >= data.total} onClick={() => update({ offset: String(data.offset + data.limit) })}>Next</button></div></footer>}
      </section>
    </>}
  </div>
}