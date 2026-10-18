import { Fragment, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { ChevronDown, ChevronLeft, ChevronRight, ChevronUp, RefreshCw } from 'lucide-react'
import { CommandGroup } from '../../layout/PageChrome'
import { getStrategyPersistence, type PersistenceSession, type PersistenceStrategyRow, type StrategyPersistencePublication } from '../../services/api'
import { publicationPath, usePublicationQuery, useStrategyInterval } from '../publicationNavigation'
import { StrategyPublicationError } from '../StrategyPublication'
import { persistenceStrategies } from './RealtimePersistenceView'
import { persistenceAxes, type PersistenceAxis, type PersistenceAxisKey } from './persistencePresentation'
import './persistence.css'

const label = (text: string) => text.toLowerCase().replace(/_/g, ' ')
const PAGE_SIZE = 100

export function legacyPersistenceRows(data: StrategyPersistencePublication): PersistenceStrategyRow[] {
  return data.members.flatMap(ticker => persistenceStrategies(data.publication.interval ?? '1d').map(({ contract }) => {
    const member = data.analysis[contract].members.find(row => row.ticker === ticker)!
    const native = data.scanners[contract].results.filter(row => row.ticker === ticker)
    const matches = native.filter(row => contract.startsWith('SCAN_MA_') ? 'signal' in row && /Crossover|Recent/.test(row.signal)
      : contract.startsWith('SCAN_FIBONACCI_') ? 'signal' in row && row.signal !== 'Between Levels' : true)
    const known = ['COMPLETE', 'COMPLETE_EMPTY'].includes(member.status)
    const sessions: PersistenceSession[] = data.evaluation_sessions.map(session => {
      const current = session === data.publication.session
      return { session, state: !current ? 'UNAVAILABLE' : known ? matches.length ? 'MATCH' : 'NO_MATCH' : member.status as PersistenceSession['state'],
        reason: current ? member.reason : 'EVALUATION_NOT_RETAINED', transition: 'UNKNOWN', consecutive_run: current && matches.length ? 1 : 0,
        event_ids: [], direction: [], direction_changed: false, native_rows: current ? native : [],
        evidence_ref: current ? data.evidence_refs[ticker]?.[contract] : null, input_sha256: null,
        market_time: current ? data.publication.market_time : null, observed_at: current ? data.publication.generated_at : null,
        available_bars: current ? member.available_bars : 0, required_bars: current ? member.required_bars : null,
        distance: null, magnitude: null, maturity: null, volume: null }
    })
    return { ticker, contract, sessions, matched: known && matches.length ? 1 : 0, evaluated: known ? 1 : 0,
      coverage: `${known ? 1 : 0}/5`, consecutive_run: sessions[4].consecutive_run, transition: 'UNKNOWN' }
  }))
}

function Spark({ values, metric }: { values: Array<number | null>; metric: string }) {
  const available = values.filter((value): value is number => value != null)
  if (available.length < 2) return null
  const minimum = Math.min(...available), span = Math.max(...available) - minimum || 1
  const points = values.map((value, index) => value == null ? null : `${index * 13 + 1},${13 - (value - minimum) / span * 11}`)
  const segments: string[] = []
  let segment: string[] = []
  for (const point of points) {
    if (point) segment.push(point)
    else { if (segment.length) segments.push(segment.join(' ')); segment = [] }
  }
  if (segment.length) segments.push(segment.join(' '))
  return <svg width="54" height="14" role="img" aria-label={`${metric} trajectory`}>
      {segments.map((path, index) => <polyline key={index} points={path} fill="none" stroke="var(--tm-accent)" strokeWidth="1.5" />)}
    </svg>
}

function AxisCell({ axis, metric }: { axis: PersistenceAxis; metric: string }) {
  return <div className="pv-axis"><strong>{axis.label}</strong>
    {axis.series && <Spark values={axis.series} metric={metric} />}
    {axis.note && <small>{axis.note}</small>}</div>
}

export default function RealtimeSessionPersistenceView() {
  const [params, setParams] = useSearchParams()
  const pending = useRef({ rendered: params.toString(), pending: params })
  if (pending.current.rendered !== params.toString()) pending.current = { rendered: params.toString(), pending: params }
  const { interval, setInterval } = useStrategyInterval(pending)
  const strategies = persistenceStrategies(interval).map(item => item.contract.startsWith('SCAN_MA_') ? { ...item, label: 'MA crossover / recent' } : item)
  const query = usePublicationQuery('daily-session-persistence', (pin, session) => getStrategyPersistence(pin, interval, session))
  const update = (key: string, value: string) => {
    const next = new URLSearchParams(pending.current.pending)
    if (value) next.set(key, value)
    else next.delete(key)
    if (key !== 'page') next.delete('page')
    pending.current.pending = next
    setParams(next, { replace: true })
  }
  const strategy = params.get('strategy_filter') ?? 'all', term = (params.get('ticker_filter') ?? '').toUpperCase()
  const group = params.get('group_by') ?? 'none', sort = params.get('sort_by') ?? 'depth'
  const [expanded, setExpanded] = useState<string | null>(null)
  const data = query.data
  const allRows = data ? data.persistence?.rows ?? legacyPersistenceRows(data) : []
  const axes = new Map(allRows.map(row => [`${row.ticker}:${row.contract}`, persistenceAxes(row)]))
  const strategyLabel = (row: PersistenceStrategyRow) => strategies.find(item => item.contract === row.contract)?.label ?? row.contract
  const groupLabel = (row: PersistenceStrategyRow) => group === 'strategy' ? strategyLabel(row) : group === 'none' ? ''
    : axes.get(`${row.ticker}:${row.contract}`)?.[group as PersistenceAxisKey]?.label ?? 'Unavailable'
  const counts = new Map<string, number>()
  allRows.filter(row => row.matched > 0).forEach(row => counts.set(row.ticker, (counts.get(row.ticker) ?? 0) + 1))
  const minimum = Math.max(1, Math.min(6, Number(params.get('min_strategies')) || 1))
  const includeAll = params.get('include_evaluations') === '1'
  const ordered = allRows.filter(row => (strategy === 'all' || row.contract === strategy) && row.ticker.includes(term)
    && (includeAll || row.matched > 0) && (includeAll && minimum === 1 || (counts.get(row.ticker) ?? 0) >= minimum)
    && (params.get('every_session') !== '1' || row.matched === 5 && row.evaluated === 5))
    .sort((left, right) => groupLabel(left).localeCompare(groupLabel(right))
      || (sort === 'ticker' ? left.ticker.localeCompare(right.ticker) : sort === 'strategy' ? strategyLabel(left).localeCompare(strategyLabel(right))
        : (right.evaluated ? right.matched / right.evaluated : -1) - (left.evaluated ? left.matched / left.evaluated : -1))
      || left.ticker.localeCompare(right.ticker) || left.contract.localeCompare(right.contract))
  const pages = Math.max(1, Math.ceil(ordered.length / PAGE_SIZE))
  const page = Math.max(0, Math.min(pages - 1, Number(params.get('page')) || 0))
  const visible = ordered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
  const overlap = Array.from({ length: strategies.length + 1 }, () => 0)
  let unknownMembers = 0
  data?.members.forEach(ticker => {
    if (allRows.filter(row => row.ticker === ticker).some(row => row.evaluated < 5)) unknownMembers++
    else overlap[counts.get(ticker) ?? 0]++
  })
  const rolling = data?.interpretation === 'STORED_SESSION_OUTCOMES_WITH_EXPLICIT_PROVENANCE'
  const sourceLabel = rolling ? Object.values(data?.session_provenance ?? {}).some(value => value.startsWith('RECONSTRUCTED'))
    ? 'Stored outcomes + reconstructed research' : 'Stored session outcomes' : data?.persistence ? 'Reconstructed research' : 'Retained latest evaluation only'

  return <div className="pv pv-realtime pv-original">
    <div className="pv-status">
      <span className="pv-note">Recurrence only. Not a trading qualification.</span>
      <div className="pv-status-actions"><CommandGroup label="Interval">{(['1d', '1h'] as const).map(value => <button key={value} type="button" className={interval === value ? 'is-active' : undefined} onClick={() => setInterval(value)}>{value}</button>)}</CommandGroup>
      <button type="button" className="pv-refresh" aria-label="Refresh stored Persistence" title="Refresh stored Persistence" disabled={query.loading} onClick={query.refresh}><RefreshCw size={15} /></button>
      </div>
    </div>
    <StrategyPublicationError error={query.error} publication={data?.publication} />
    {!data && <p className="pv-empty" role="status">{query.loading ? 'Reading stored Persistence...' : 'Persistence release unavailable.'}</p>}
    {data && <>
      <section className="pv-section" aria-label="Persistence coverage">
        <div className="pv-panel__head"><div><h2>Coverage this window</h2><p>{[...data.evaluation_sessions].reverse().join(' / ')}</p></div>
          <span className="pv-note" role="status">{data.observed_sessions.length}/5 stored evaluation sessions | {sourceLabel}</span></div>
        <div className="pv-coverage"><div className="pv-rates">
          {strategies.map(item => { const rows = allRows.filter(row => row.contract === item.contract)
            const matched = rows.filter(row => row.matched > 0).length, evaluated = rows.filter(row => row.evaluated > 0).length
            const share = evaluated ? matched / evaluated * 100 : null
            return <div className="pv-rate" key={item.contract}><span className="pv-rate__label">{item.label}</span>
              <strong>{share == null ? 'Unavailable' : `${share.toFixed(0)}%`}</strong><small>{matched} of {evaluated} assessed | {item.contract.startsWith('SCAN_MOMENTUM_') ? 'Long' : item.contract.startsWith('SCAN_BEARISH_') ? 'Short' : 'Both'}</small>
              <small>{rows.reduce((sum, row) => sum + row.evaluated, 0)}/{data.members.length * 5} evaluated cells</small>
              <div className="pv-rate__bar"><i style={{ width: `${share ?? 0}%` }} /></div></div>
          })}</div>
          <div className="pv-table-wrap"><table className="pv-mini" aria-label="Observed strategy overlap"><thead><tr><th>Strategies matched</th>{overlap.map((_, index) => <th key={index}>{index}</th>)}<th>Incomplete coverage</th></tr></thead>
            <tbody><tr><td>Observed tickers</td>{overlap.map((count, index) => <td key={index}>{count}</td>)}<td>{unknownMembers}</td></tr></tbody></table></div>
          <p className="pv-note">Strategy overlap is not independent confirmation. Missing evaluations are not misses.</p>
        </div>
      </section>
      <section className="pv-section" aria-label="Persistence detail"><div className="pv-panel__head"><h2>Persistence detail</h2></div>
        <div className="pv-controls">
          <label className="pv-field"><span>Strategy</span><select aria-label="Persistence strategy" value={strategy} onChange={event => update('strategy_filter', event.target.value)}><option value="all">All strategies</option>{strategies.map(item => <option key={item.contract} value={item.contract}>{item.label}</option>)}</select></label>
          <label className="pv-field"><span>Sort by</span><select aria-label="Persistence sort" value={sort} onChange={event => update('sort_by', event.target.value)}><option value="depth">Recurrence depth</option><option value="ticker">Ticker</option><option value="strategy">Strategy</option></select></label>
          <label className="pv-field"><span>Group by</span><select aria-label="Persistence group" value={group} onChange={event => update('group_by', event.target.value)}>{['none', 'strategy', 'stability', 'distance', 'magnitude', 'maturity', 'volume'].map(value => <option key={value} value={value}>{value === 'none' ? 'No grouping' : label(value)}</option>)}</select></label>
          <label className="pv-field"><span>In at least</span><select aria-label="Minimum strategies" value={minimum} onChange={event => update('min_strategies', event.target.value)}>{[1, 2, 3, 4, 5, 6].map(value => <option key={value} value={value}>{value} {value === 1 ? 'strategy' : 'strategies'}</option>)}</select></label>
          <label className="pv-field"><span>Ticker</span><input aria-label="Persistence ticker" value={params.get('ticker_filter') ?? ''} placeholder="Filter" onChange={event => update('ticker_filter', event.target.value)} /></label>
          <label className="pv-check"><input type="checkbox" checked={params.get('every_session') === '1'} onChange={event => update('every_session', event.target.checked ? '1' : '')} />Every session only</label>
          <label className="pv-check"><input type="checkbox" checked={includeAll} onChange={event => update('include_evaluations', event.target.checked ? '1' : '')} />Include all evaluations</label>
          <span className="pv-count">{ordered.length} rows | {new Set(ordered.map(row => row.ticker)).size} tickers</span>
        </div>
        <div className="pv-table-wrap"><table className="pv-table"><thead><tr><th>Ticker</th><th>Strategy</th><th>Recurrence<small className="pv-reason">Matched / evaluated</small></th><th>Coverage</th><th>Stability</th><th>Distance trend</th><th>Magnitude trend</th><th>Maturity</th><th>Volume</th><th>Context</th><th>Transition / run</th></tr></thead>
          <tbody>{visible.map((row, index) => { const key = `${row.ticker}:${row.contract}`, open = expanded === key, heading = groupLabel(row), nativeAxes = axes.get(key)!
            return <Fragment key={key}>
              {heading && (index === 0 || heading !== groupLabel(visible[index - 1])) && <tr><td className="pv-group" colSpan={11}>{heading}</td></tr>}
              <tr><td><button type="button" className="pv-expand" aria-label={`Session details for ${row.ticker} ${strategyLabel(row)}`} aria-expanded={open} onClick={() => setExpanded(open ? null : key)}>{open ? <ChevronUp size={14} /> : <ChevronDown size={14} />}</button>
                <Link className="pv-ticker" to={publicationPath(`/strategy-research/${row.ticker}`, data.publication.snapshot_id, data.publication.cohort_id, interval, data.publication.evaluation_session)}>{row.ticker}</Link></td>
                <td><div className="pv-axis"><strong>{strategyLabel(row)}</strong><small>{nativeAxes.direction}</small></div></td><td><strong className="pv-matched">{row.matched}/{row.evaluated}</strong><span className="pv-dots" aria-label="Session evaluation states">
                  {row.sessions.map(cell => <i key={cell.session} className={`pv-cell-${cell.state.toLowerCase()}`} title={`${cell.session}: ${label(cell.state)}${cell.reason ? ` | ${cell.reason}` : ''}`} />)}</span></td>
                <td>{row.coverage}</td>
                {(['stability', 'distance', 'magnitude', 'maturity', 'volume'] as const).map(metric => <td key={metric}><AxisCell axis={nativeAxes[metric]} metric={metric} /></td>)}
                <td>{nativeAxes.context}</td>
                <td>{label(row.transition)}<small className="pv-reason">{row.consecutive_run} consecutive observed matches</small></td></tr>
              {open && <tr className="pv-detail"><td colSpan={11}><table className="pv-detail-table"><thead><tr><th>Session</th><th>Evaluation</th><th>Transition</th><th>Native details</th><th>Evidence / input</th><th>Provenance</th></tr></thead>
                <tbody>{row.sessions.map(cell => <tr key={cell.session}><td>{data.persistence && cell.evidence_ref
                  ? <Link to={`${publicationPath(`/strategy-research/${row.ticker}`, data.publication.snapshot_id, data.publication.cohort_id, interval)}&evaluation_session=${cell.session}`}>{cell.session}</Link> : cell.session}<small className="pv-reason">{cell.market_time ?? 'Cutoff unavailable'}</small></td>
                  <td>{label(cell.state)}<small className="pv-reason">{cell.reason ?? `${cell.available_bars}/${cell.required_bars} input bars`}</small></td><td>{label(cell.transition)}{cell.direction_changed ? ' | Direction changed' : ''}</td>
                  <td>{cell.native_rows.length ? <details><summary>{cell.native_rows.length} retained row(s)</summary><pre>{JSON.stringify(cell.native_rows, null, 2)}</pre></details> : 'No retained rows'}</td>
                  <td>{cell.evidence_ref ? <code title={`${cell.evidence_ref.evidence_id} | SHA-256 ${cell.evidence_ref.payload_sha256} | Input ${cell.input_sha256 ?? 'Legacy window'} | Observed ${cell.observed_at}`}>{cell.evidence_ref.evidence_id.slice(0, 8)}</code> : 'Not evaluated'}</td>
                  <td>{cell.observed_at ? <><span>{data.session_provenance?.[cell.session]?.startsWith('RECONSTRUCTED') || !rolling && data.persistence ? 'Reconstructed research' : 'Stored evaluation'}</span><small className="pv-reason">Observed {cell.observed_at}</small></> : 'Unavailable'}</td></tr>)}</tbody></table></td></tr>}
            </Fragment>
          })}</tbody></table></div>
        {!ordered.length && <p className="pv-empty">No rows match these filters.</p>}
        {pages > 1 && <div className="pv-pagination"><button type="button" className="pv-refresh" title="Previous page" aria-label="Previous page" disabled={page === 0} onClick={() => update('page', String(page - 1))}><ChevronLeft size={15} /></button>
          <span className="pv-note">Page {page + 1} of {pages}</span><button type="button" className="pv-refresh" title="Next page" aria-label="Next page" disabled={page === pages - 1} onClick={() => update('page', String(page + 1))}><ChevronRight size={15} /></button></div>}
      </section>
    </>}
  </div>
}