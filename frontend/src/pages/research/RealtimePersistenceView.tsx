import { Fragment, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { ChevronDown, ChevronUp, RefreshCw } from 'lucide-react'
import { CommandBar, CommandField, CommandGroup, CommandSpacer } from '../../layout/PageChrome'
import { getDailyStrategyEvaluations, HOURLY_STRATEGY_CONTRACTS, type DailyScannerContract, type DailyStrategyEvaluations } from '../../services/api'
import { publicationPath, usePublicationQuery, useStrategyInterval } from '../publicationNavigation'
import { StrategyPublicationError } from '../StrategyPublication'
import './persistence.css'

export const PERSISTENCE_STRATEGIES: Array<{ contract: DailyScannerContract; label: string; short: string }> = [
  { contract: 'SCAN_GAPS_1D', label: 'Opening gaps', short: 'Gaps' },
  { contract: 'SCAN_FVG_1D_50', label: 'Fair value gaps', short: 'FVG' },
  { contract: 'SCAN_MA_1D_9_21', label: 'MA 9/21 state', short: 'MA state' },
  { contract: 'SCAN_MOMENTUM_1D', label: 'Momentum Pullback', short: 'Momentum' },
  { contract: 'SCAN_BEARISH_1D', label: 'Bearish Bounce', short: 'Bearish' },
  { contract: 'SCAN_FIBONACCI_1D_5', label: 'Fibonacci 5%', short: 'Fibonacci' },
]

export function persistenceStrategies(interval: '1d' | '1h') {
  return interval === '1d' ? PERSISTENCE_STRATEGIES : PERSISTENCE_STRATEGIES.map(item => ({ ...item,
    contract: HOURLY_STRATEGY_CONTRACTS[item.contract], label: item.contract === 'SCAN_FIBONACCI_1D_5' ? 'Fibonacci ATR' : item.label }))
}

export function evaluationCell(data: DailyStrategyEvaluations, contract: DailyScannerContract, ticker: string) {
  const member = data.analysis[contract].members.find(row => row.ticker === ticker)!
  const rows = data.scanners[contract].results.filter(row => row.ticker === ticker)
  const first = rows[0]
  let summary = 'No match'
  if (member.reason === 'CORPORATE_ACTION_REVIEW') summary = 'Action review'
  else if (member.status === 'UNAVAILABLE') summary = 'Unavailable'
  else if (member.status === 'INSUFFICIENT_HISTORY') summary = 'Short history'
  else if (member.status === 'FAILED') summary = 'Failed'
  else if (first && contract.startsWith('SCAN_GAPS_')) summary = `${rows.length} gap${rows.length === 1 ? '' : 's'}`
  else if (first && contract.startsWith('SCAN_FVG_')) summary = `${rows.length} zone${rows.length === 1 ? '' : 's'}`
  else if (first && 'grade' in first) summary = `${first.grade} (score ${first.score})`
  else if (first && 'signal' in first) summary = first.signal
  return { member, rows, summary }
}

function detailMetric(data: DailyStrategyEvaluations, contract: DailyScannerContract, ticker: string) {
  const { rows } = evaluationCell(data, contract, ticker)
  const row = rows[0]
  if (!row) return 'Unavailable'
  if ('ma_spread_pct' in row) return `Spread ${row.ma_spread_pct}%`
  if ('dist_to_ema21_pct' in row) return `EMA 21 distance ${row.dist_to_ema21_pct}% | RSI ${row.rsi} | ADX ${row.adx}`
  if ('distance_pct' in row) return `${row.nearest_level} | Distance ${row.distance_pct}%`
  if ('fvg_type' in row) return `${row.fvg_type} | ${row.status} | ${row.proximity}`
  if ('gap_type' in row) return `${row.gap_type} | ${row.gap_lifecycle ?? 'Lifecycle unavailable'}`
  return 'Unavailable'
}

export default function RealtimeStrategyOverview() {
  const [params, setParams] = useSearchParams()
  const searchState = useRef({ rendered: params.toString(), pending: params })
  if (searchState.current.rendered !== params.toString()) {
    searchState.current = { rendered: params.toString(), pending: params }
  }
  const { interval, setInterval } = useStrategyInterval(searchState)
  const strategies = persistenceStrategies(interval)
  const query = usePublicationQuery('daily-persistence', (pin, session) => getDailyStrategyEvaluations(pin, interval, session))
  const strategy = strategies.find(item => item.contract === params.get('strategy_filter'))?.contract ?? 'all'
  const search = params.get('ticker_filter') ?? ''
  const rowsOnly = params.get('rows_only') === '1'
  const [expanded, setExpanded] = useState<string | null>(null)
  const updateFilter = (name: string, value: string) => {
    const next = new URLSearchParams(searchState.current.pending)
    if (value) next.set(name, value)
    else next.delete(name)
    searchState.current.pending = next
    setParams(next, { replace: true })
  }
  const data = query.data
  const columns = strategies.filter(item => strategy === 'all' || item.contract === strategy)
  const members = (data?.members ?? []).filter(ticker => ticker.includes(search.trim().toUpperCase())
    && (!rowsOnly || columns.some(item => data!.scanners[item.contract].results.some(row => row.ticker === ticker))))

  return <div className="pv pv-realtime">
    <CommandBar>
      <CommandField label="Strategy"><select aria-label="Persistence strategy" name="strategy_filter" value={strategy}
        onChange={event => updateFilter('strategy_filter', event.target.value === 'all' ? '' : event.target.value)}>
        <option value="all">All strategies</option>
        {strategies.map(item => <option key={item.contract} value={item.contract}>{item.label}</option>)}
      </select></CommandField>
      <CommandField label="Ticker"><input aria-label="Persistence ticker" name="ticker_filter" autoComplete="off" spellCheck={false} value={search}
        onChange={event => updateFilter('ticker_filter', event.target.value)} placeholder="Ticker" /></CommandField>
      <label className="pv-check"><input type="checkbox" name="rows_only" checked={rowsOnly}
        onChange={event => updateFilter('rows_only', event.target.checked ? '1' : '')} />With rows only</label>
      <CommandSpacer />
      <CommandGroup label="Interval">{(['1d', '1h'] as const).map(value => <button key={value} type="button" className={interval === value ? 'is-active' : undefined} onClick={() => setInterval(value)}>{value}</button>)}</CommandGroup>
      <button type="button" className="pv-refresh" title="Refresh stored evaluations" aria-label="Refresh stored evaluations"
        disabled={query.loading} onClick={query.refresh}><RefreshCw size={15} aria-hidden="true" /></button>
    </CommandBar>
    <StrategyPublicationError error={query.error} publication={data?.publication} />
    {query.loading && !data && <p className="pv-empty" role="status">Reading stored evaluations...</p>}
    {!query.loading && !data && <p className="pv-empty" role="status">Evaluation release unavailable.</p>}
    {data && <>
      <div className="pv-status" role="status" aria-live="polite"><span className="pv-note">Session {data.publication.session} | {data.observed_sessions.length} stored session | Recurrence unavailable</span>
        <span className="pv-note">{data.publication.status}{query.loading ? ' | Refreshing' : ''} | Published {data.publication.generated_at}</span></div>
      <section className="pv-section" aria-label="Evaluation coverage">
        <h2>Evaluation coverage</h2>
        <div className="pv-table-wrap"><table className="pv-table">
          <thead><tr><th>Strategy</th><th>Evaluated</th><th>Members with rows</th><th>No match</th><th>Not evaluated</th></tr></thead>
          <tbody>{strategies.map(item => {
            const analysis = data.analysis[item.contract]
            const count = new Set(data.scanners[item.contract].results.map(row => row.ticker)).size
            return <tr key={item.contract}><td>{item.label}</td><td>{analysis.total_evaluated}/{data.members.length}</td>
              <td>{count}</td><td>{analysis.total_evaluated - count}</td><td>{analysis.total_unavailable}</td></tr>
          })}</tbody>
        </table></div>
      </section>
      <section className="pv-section" aria-label="Ticker evaluations">
        <h2>Ticker evaluations <small>({members.length})</small></h2>
        <div className="pv-table-wrap"><table className="pv-table">
          <thead><tr><th>Ticker</th>{columns.map(item => <th key={item.contract}>{item.short}</th>)}</tr></thead>
          <tbody>{members.map(ticker => <Fragment key={ticker}>
            <tr><td className="pv-symbol"><button type="button" className="pv-expand" aria-label={`Evaluation details for ${ticker}`}
              aria-expanded={expanded === ticker} onClick={() => setExpanded(expanded === ticker ? null : ticker)}>
              {expanded === ticker ? <ChevronUp size={14} aria-hidden="true" /> : <ChevronDown size={14} aria-hidden="true" />}</button>
              <Link className="pv-ticker" title={`Open exact release research for ${ticker}`}
                to={publicationPath(`/strategy-research/${ticker}`, data.publication.snapshot_id, data.publication.cohort_id, interval, data.publication.evaluation_session)}>{ticker}</Link></td>
              {columns.map(item => { const cell = evaluationCell(data, item.contract, ticker)
                return <td key={item.contract} title={cell.member.reason ?? cell.member.status}>{cell.summary}</td> })}</tr>
            {expanded === ticker && <tr className="pv-detail"><td colSpan={columns.length + 1}>
              <table className="pv-detail-table"><thead><tr><th>Strategy</th><th>Evaluation</th><th>Native metric</th><th>Evidence</th></tr></thead>
                <tbody>{columns.map(item => { const cell = evaluationCell(data, item.contract, ticker)
                  const ref = data.evidence_refs[ticker]?.[item.contract]
                  return <tr key={item.contract}><td>{item.label}</td><td>{cell.member.status}
                    {cell.member.reason && <small className="pv-reason">{cell.member.reason}</small>}</td>
                    <td>{detailMetric(data, item.contract, ticker)}</td><td>{ref
                      ? <code title={`${ref.evidence_id} | SHA-256 ${ref.payload_sha256}`}>{ref.evidence_id.slice(0, 8)}</code> : 'Not evaluated'}</td></tr>
                })}</tbody></table>
            </td></tr>}
          </Fragment>)}</tbody>
        </table></div>
        {members.length === 0 && <p className="pv-empty">No members match these filters.</p>}
      </section>
    </>}
  </div>
}