import { Fragment, useDeferredValue, useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { AlertTriangle, ArrowDown, ArrowLeft, ArrowRight, ArrowUp, Bell, ChevronDown, ChevronRight, Clock, Download, History, RefreshCw, ShieldAlert } from 'lucide-react'
import { ColumnPicker, useColumnPreferences, type ColumnSpec } from '../layout/PageChrome'
import { usePublishPageContext } from '../layout/pageContext'
import { getAlertView, type AlertContextFactor, type AlertPlanRow } from '../services/stockDiscovery'
import { alertSort, alertSourceParams, alertTabParams, alertTradeFilters, legacyAlertStatuses, resolveAlertRoute, tradeAlertModels, tradeAlertStatuses, type AlertView } from './stockAlertNavigation'
import { alertColumnLayout, alertPlanTiming, alertRiskAssessment, unavailableAlertProbability } from './stockAlertPresentation'
import './StockDiscoveryPage.css'
import './StockAlertsPage.css'

const number = (value: unknown, digits = 2) => typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits }) : 'N/A'
const percent = (value: unknown) => typeof value === 'number' && Number.isFinite(value) ? `${value > 0 ? '+' : ''}${number(value * 100)}%` : 'N/A'
const price = (value: unknown) => typeof value === 'number' && Number.isFinite(value) ? `$${number(value)}` : 'N/A'
const time = (value: string | null | undefined) => value ? new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }).format(new Date(value)) : 'N/A'
const readable = (value: string) => value.toLowerCase().replace(/_/g, ' ')
const tone = (value: unknown) => typeof value !== 'number' || value === 0 ? '' : value > 0 ? 'sd-positive' : 'sd-negative'
const models: Record<string, string> = { resumption: 'Trend resumption', acceptance: 'Breakout acceptance', failure: 'Extension reversal', discovery: 'Momentum watch', legacy_daily: 'Legacy daily' }
type AlertColumn = ColumnSpec & { history?: boolean; tip?: string; indicator?: boolean }
const columns: AlertColumn[] = [
  { key: 'ticker', label: 'Stock', group: 'Alert details', locked: true }, { key: 'direction', label: 'Direction', group: 'Alert details', locked: true },
  { key: 'trade_style', label: 'Trade type', group: 'Alert details', locked: true, tip: 'Independent original plan: intraday minute cap or swing trading-session horizon. Shared display does not merge positions, quotas or performance.' },
  { key: 'model', label: 'Model / interval', group: 'Alert details', tip: 'Swing: the daily setup owns the bracket and session horizon; intraday confirmation only times entry. Intraday: a separate short-duration plan.' }, { key: 'published_at', label: 'Published (ET)', group: 'Alert details' },
  { key: 'triggered_at', label: 'Triggered (ET)', group: 'Alert details', history: true, tip: 'Original trigger-bar timestamp, separate from when the alert was published' },
  { key: 'trigger_price', label: 'Trigger price', group: 'Trade plan', tip: 'Completed trigger-bar price, not an executable fill' },
  { key: 'latest_price', label: 'Current price', group: 'Price comparison', history: true, tip: 'Latest eligible stored completed-bar price; delayed, not a real-time quote. Replay prices stay frozen at cutoff.' },
  { key: 'price_return', label: 'Price P/L %', group: 'Price comparison', history: true, tip: 'Direction-adjusted move from the original trigger price to the displayed price, before costs. Not an executed return; ignores paper entry and stop/target exits.' },
  { key: 'entry_price', label: 'Paper entry', group: 'Paper outcomes', history: true }, { key: 'stop', label: 'Original stop', group: 'Trade plan' }, { key: 'target', label: 'Target price', group: 'Trade plan', tip: 'Original target frozen with the alert plan, not a price forecast' },
  { key: 'risk_pct', label: 'Risk to stop', group: 'Trade plan', tip: 'Distance from trigger price to stop, not a maximum-loss guarantee' },
  { key: 'reward_risk', label: 'Reward/risk', group: 'Trade plan', tip: 'Frozen trigger-price target room divided by stop risk' },
  { key: 'success_probability', label: 'Success probability', group: 'Trade plan', hiddenByDefault: true, tip: unavailableAlertProbability.reason },
  { key: 'risk_assessment', label: 'Risk assessment', group: 'Trade plan', hiddenByDefault: true, tip: 'Original stop distance and retained cautions; not a calibrated risk score or maximum-loss guarantee' },
  { key: 'hold', label: 'Maximum hold', group: 'Trade plan', tip: 'Swing limits count trading sessions, not calendar days; the entry session counts as session one. Intraday limits run from entry until the minute cap or session close. Stop or target exits can occur earlier; gaps can exceed stop risk.' }, { key: 'hits', label: 'Hits', group: 'Alert details', tip: 'Distinct valid stock/direction publication windows over 21 sessions; not confidence' },
  { key: 'paper_return', label: 'Paper P/L %', group: 'Paper outcomes', history: true },
  { key: 'status', label: 'Status', group: 'Alert details' }, { key: 'exit_price', label: 'Fixed exit', group: 'Paper outcomes', history: true, hiddenByDefault: true },
  { key: 'rsi', label: 'RSI14 (trigger)', group: 'Momentum', hiddenByDefault: true, indicator: true, tip: '14-bar simple rolling RSI, frozen at trigger; unavailable if the denominator is zero' },
  { key: 'ema20_distance', label: 'vs EMA20 (trigger)', group: 'Trend', hiddenByDefault: true, indicator: true },
  { key: 'ema50_distance', label: 'vs EMA50 (trigger)', group: 'Trend', hiddenByDefault: true, indicator: true },
  { key: 'ema50_slope', label: 'EMA50 slope (trigger)', group: 'Trend', hiddenByDefault: true, indicator: true, tip: 'EMA50 change over 10 completed trigger bars' },
  { key: 'rs_percentile', label: 'RS63 (daily)', group: 'Momentum', hiddenByDefault: true, indicator: true, tip: 'Daily full dated-universe percentile; not probability' },
  { key: 'momentum', label: '12-1 momentum (daily)', group: 'Momentum', hiddenByDefault: true, indicator: true },
  { key: 'relative_volume', label: 'Rel volume (trigger)', group: 'Liquidity & activity', hiddenByDefault: true, indicator: true, tip: 'Trigger volume divided by prior 20-bar average; not time-of-day adjusted' },
  { key: 'liquidity', label: 'Median $ vol (daily)', group: 'Liquidity & activity', hiddenByDefault: true, indicator: true },
  { key: 'atr_pct', label: 'Activation ATR %', group: 'Volatility & extension', hiddenByDefault: true, indicator: true },
  { key: 'volatility', label: '21-bar volatility', group: 'Volatility & extension', hiddenByDefault: true, indicator: true, tip: 'Standard deviation of trigger-bar returns, not annualized' },
  { key: 'extension_atr', label: 'Extension / ATR', group: 'Volatility & extension', hiddenByDefault: true, indicator: true },
  { key: 'sector', label: 'Sector', group: 'Alert details', hiddenByDefault: true, indicator: true },
]
const presets: Record<string, string[]> = {
  plan: columns.filter(column => !column.hiddenByDefault).map(column => column.key),
  trend: ['ticker', 'direction', 'model', 'trigger_price', 'risk_pct', 'reward_risk', 'hits', 'ema20_distance', 'ema50_distance', 'ema50_slope', 'rs_percentile', 'status'],
  momentum: ['ticker', 'direction', 'model', 'trigger_price', 'risk_pct', 'hits', 'rsi', 'momentum', 'rs_percentile', 'extension_atr', 'status'],
  liquidity: ['ticker', 'direction', 'model', 'trigger_price', 'risk_pct', 'hits', 'relative_volume', 'liquidity', 'atr_pct', 'volatility', 'status'],
}

function exportAlerts(rows: AlertPlanRow[], visible: AlertColumn[], value: (row: AlertPlanRow, key: string) => unknown) {
  const escape = (item: unknown) => `"${String(item ?? '').replace(/"/g, '""')}"`
  const exportValue = (row: AlertPlanRow, key: string) => {
    const item = value(row, key)
    return key === 'price_return' && typeof item === 'number' ? item * 100 : item
  }
  const content = [visible.map(column => escape(column.label)).join(','), ...rows.map(row => visible.map(column => escape(exportValue(row, column.key))).join(','))].join('\r\n')
  const url = URL.createObjectURL(new Blob([content], { type: 'text/csv;charset=utf-8' }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = 'stock-alerts.csv'
  anchor.click()
  URL.revokeObjectURL(url)
}

function contextValue(key: string, factor: AlertContextFactor): ReactNode {
  const value = factor.value
  if (!value) return 'Unavailable'
  if (key === 'market') return value.direction ? readable(value.direction) : 'Unavailable'
  if (key === 'spy' || key === 'qqq') return <>{value.direction ? readable(value.direction) : 'Unavailable'} / 1 session {percent(value.return1)} / 5 sessions {percent(value.return5)} / 20 sessions {percent(value.return20)}</>
  if (key === 'sector_rotation' || key === 'stock_relative_rotation') return <>
    {value.state ? readable(value.state) : 'Unavailable'} / 5-session relative {number((value.relative5 ?? NaN) * 100)} pp
    <p>20-session relative {number((value.relative20 ?? NaN) * 100)} pp / relative change {number((value.relative_change5 ?? NaN) * 100)} pp</p>
    {factor.persistence && <p>{readable(factor.persistence.status)} ({factor.persistence.observations}) / reconstructed trail</p>}
  </>
  if (key === 'stock_divergence') return value.labels?.map(readable).join('; ') || 'Unavailable'
  if (key === 'tracked_breadth') return <>{value.advancing} advancing / {value.declining} declining
    <p>Above SMA50 {number((value.above_sma50_fraction ?? NaN) * 100)}% / SMA200 {number((value.above_sma200_fraction ?? NaN) * 100)}%</p>
    <p>Coverage {factor.timely_observations ?? 'Unknown'} / {factor.expected_observations ?? 'Unknown'}</p></>
  if (key === 'spy_realized_volatility') return <>{number((value.annualized_volatility20 ?? NaN) * 100)}% annualized / 20 daily returns</>
  if (key === 'vix' || key === 'credit') return <>{number(value.level)} {key === 'credit' ? 'bps' : 'points'} / percentile {number((value.percentile ?? NaN) * 100)}%</>
  if (key === 'conditions_score') return <>{number(value.score, 1)} / 100 / development only</>
  if (key === 'market_volume' || key === 'sector_volume') return <>{number(value.relative_volume)}x / same elapsed session time</>
  if (key === 'event_shadow') return <>{value.disposition ? readable(value.disposition) : 'Unknown'} / live gate disabled
    {!!value.blocking_factors?.length && <p>Holding-window events: {value.blocking_factors.join(', ')}</p>}
    {!!value.unknown_factors?.length && <p>Uncertain: {value.unknown_factors.join(', ')}</p>}</>
  if (key === 'stock_daily') return <>{value.direction ? readable(value.direction) : 'Unavailable'} / 20-session return {percent(value.return20)}</>
  if (key === 'sector') return <>{value.direction ? readable(value.direction) : 'Unavailable'} / stock vs sector {number((value.stock_minus_sector20 ?? NaN) * 100)} pp / sector vs SPY {number((value.sector_minus_spy20 ?? NaN) * 100)} pp (20 sessions)</>
  if (key === 'financials') return value.reports?.map(report => <div className="sa-financial-report" key={report.report_id}>
    <strong>{readable(report.timeframe)} / {report.period_end}</strong>
    <p>Filed {report.filing_date} / {report.age_days} days since period end{report.stale ? ' / Stale' : ''}</p>
    <p>{readable(report.units)}</p>
    <p>Cash {number(report.metrics.cash_and_equivalents)} / current debt {number(report.metrics.current_debt)} / long-term debt {number(report.metrics.long_term_debt)}</p>
    <p>Operating cash flow {number(report.metrics.operating_cash_flow)} / capex {number(report.metrics.capital_expenditures)} / reported free cash flow {number(report.metrics.free_cash_flow)}</p>
    <p>{report.source}{report.accession_number ? ` / ${report.accession_number}` : ''}</p>
  </div>) || 'Unavailable'
  if (value.events?.length) return <>{value.events.map((event, index) => <p key={`${event.type}-${event.scheduled_time}-${index}`}>
    {readable(event.type)} / {time(event.scheduled_time)} ET / {readable(event.confidence)}
    {event.relative_timing && <small>{readable(event.relative_timing)}</small>}
  </p>)}{value.timing_uncertain && <span>Event timing uncertain</span>}</>
  return value.coverage_complete ? 'No known event in the holding horizon' : 'Event coverage unknown'
}

function SavedAlertContext({ row }: { row: AlertPlanRow }) {
  const context = row.context
  const labels: [string, string][] = [['market', 'Market'], ['stock_daily', 'Ticker daily'], ['sector', 'Sector'],
    ['earnings', 'Earnings'], ['fomc', 'FOMC'], ['financials', 'Financials / debt / cash flow'],
    ['sector_rotation', 'Sector rotation vs SPY'], ['stock_relative_rotation', 'Ticker rotation vs sector'],
    ['stock_divergence', 'Five-session divergence'], ['tracked_breadth', 'Tracked-stock breadth'],
    ['spy', 'SPY'], ['qqq', 'QQQ'], ['spy_realized_volatility', 'SPY realized volatility'],
    ['vix', 'VIX'], ['credit', 'High-yield OAS'], ['conditions_score', 'Development conditions score'],
    ['market_volume', 'SPY comparable volume'], ['sector_volume', 'Sector comparable volume'], ['event_shadow', 'Event shadow study']]
  return <section className="sa-context" aria-label={`Saved context for ${row.ticker}`}>
    <h3>Publication context</h3>
    {context?.status !== 'AVAILABLE' ? <p className="sa-context-meta">{context?.reason ? readable(context.reason) : 'No saved publication context'}</p> : <>
      <p className="sa-context-meta">Retained as-of evidence / cutoff {time(context.input_cutoff)} ET / assembled {time(context.assembled_at)} ET</p>
      <dl className="sa-context-grid">{labels.filter(([key], index) => index < 6 || key in context.factors).map(([key, label]) => {
        const factor = context.factors[key]
        return <div key={key}><dt>{label}</dt><dd>
          <span className="sa-context-status">{factor ? readable(factor.status) : 'Not covered'}</span>
          {factor && <div>{contextValue(key, factor)}</div>}
          {factor && <details className="sa-context-evidence"><summary>Source evidence ({factor.source_revision_ids.length})</summary>
            <p>Market/event time {time(factor.market_time)} ET</p>
            <p>Observed {time(factor.observed_at)} ET / available {time(factor.available_at)} ET</p>
            {factor.source_snapshot_sha256 && <><p>Archived Market Conditions / session {factor.source_snapshot_session}</p><code>{factor.source_snapshot_sha256}</code><p>Source lineage</p><code>{factor.source_lineage_ref}</code></>}
            {factor.reason_codes.length > 0 && <p>{factor.reason_codes.map(readable).join('; ')}</p>}
            <code>{factor.source_revision_ids.join(', ') || 'No eligible source revisions'}</code>
          </details>}
        </dd></div>
      })}</dl>
      <details className="sa-context-evidence"><summary>Publication provenance</summary>
        <p>{context.capture_mode ? readable(context.capture_mode) : 'Unavailable'}</p>
        <p>Published {time(context.publication_at)} ET</p>
        <p>Context hash <code>{context.bundle_sha256}</code></p>
        <p>Original publication hash <code>{context.source_publication_sha256}</code></p>
      </details>
    </>}
  </section>
}

export default function StockAlertsPage() {
  const [params, setParams] = useSearchParams()
  const [expanded, setExpanded] = useState<string | null>(null)
  const search = useDeferredValue(params.get('search') || '')
  const { source: requestedSource, view } = resolveAlertRoute(params)
  const offset = Math.max(0, Number(params.get('offset')) || 0)
  const { sort, descending } = alertSort(params, view)
  const preferences = useColumnPreferences('stock-alert-view-v1', columns)
  const { columns: effectiveColumns, selectable, hidden, visible } = alertColumnLayout(columns, preferences.hidden, view)
  const selectedPreset = Object.entries(presets).find(([, keys]) =>
    effectiveColumns.every(column => !hidden.has(column.key) === (keys.includes(column.key) || !!column.locked)))?.[0] || ''
  const filters = alertTradeFilters(params, requestedSource)
  const tradeType = params.get('trade_type') === 'SWING' ? 'SWING' : params.get('trade_type') === 'INTRADAY' ? 'INTRADAY' : undefined
  const args = { source: requestedSource, view, session_date: view === 'open' ? undefined : params.get('session_date') || undefined,
    trade_type: tradeType, combined: params.has('combined') ? params.get('combined') === 'true' : undefined,
    run: view === 'latest' ? params.get('run') || undefined : undefined, search,
    interval: params.get('interval') || undefined, ...filters, sort, descending, offset, limit: 100 }
  const query = useQuery({ queryKey: ['stock-alert-view', args], queryFn: () => getAlertView(args), refetchInterval: requestedSource === 'REPLAY' ? false : 30_000 })
  const data = query.data
  const source = data?.source || requestedSource
  const sourceLabel = source === 'REPLAY' ? 'Backtested history' : source === 'LEGACY' ? 'Legacy daily history' : data?.combined ? 'Intraday + swing shadow' : data?.source_id === 'stock_ideas_forward_swing_v1' ? 'Swing shadow' : 'Forward shadow'
  const rows = data?.rows || []
  const displayedRuns = view === 'history' ? data?.runs || [] : view === 'open' ? [] : data?.latest_runs || (data?.run ? [data.run] : [])
  const incompleteCoverage = displayedRuns.some(run => (run.missing ?? 0) > 0 || run.status === 'MISSED_PUBLICATION')
  const dataWarnings = data?.warnings.filter(warning => /stale|missing|unavailable/i.test(warning)) || []
  const update = (values: Record<string, string | null>) => {
    const next = new URLSearchParams(params)
    Object.entries(values).forEach(([key, value]) => value ? next.set(key, value) : next.delete(key))
    if (!('offset' in values)) next.delete('offset')
    setParams(next)
    setExpanded(null)
  }
  const changeView = (next: AlertView) => {
    setParams(alertTabParams(params, next, data?.source === 'SHADOW' && !!data.sessions.length))
    setExpanded(null)
  }
  usePublishPageContext({ eyebrow: 'Stocks', title: 'Stock Alerts',
    status: [{ label: 'Data', value: sourceLabel, title: source === 'REPLAY' ? `Simulated alerts, not delivered live. Prices and outcomes frozen at ${time(data?.as_of)} ET.` : data?.source_label }],
    alertSessions: { dates: data?.sessions || [], selected: data?.session || '', source: requestedSource } })
  const choosePreset = (preset: string) => {
    const wanted = new Set(presets[preset])
    for (const column of effectiveColumns) {
      if (!column.locked && hidden.has(column.key) === wanted.has(column.key)) preferences.toggle(column.key)
    }
  }
  const value = (row: AlertPlanRow, key: string): unknown => {
    if (key === 'trade_style') return alertPlanTiming(row).style
    if (key === 'hold') return alertPlanTiming(row).hold
    if (key === 'success_probability') return unavailableAlertProbability.label
    if (key === 'risk_assessment') {
      const assessment = alertRiskAssessment(row)
      return [assessment.label, assessment.stopDistance === null ? 'Stop distance unavailable' : `${number(assessment.stopDistance * 100)}% to stop`, assessment.reason, ...assessment.cautions].join('; ')
    }
    return key in row ? row[key as keyof AlertPlanRow] : row.indicators[key]
  }
  const sortColumn = (key: string) => {
    if (['trade_style', 'hold', 'status', 'exit_price', 'sector', 'success_probability', 'risk_assessment'].includes(key)) return
    update({ sort: key, ascending: sort === key && descending ? '1' : null })
  }
  const cell = (row: AlertPlanRow, column: AlertColumn): ReactNode => {
    const item = value(row, column.key)
    if (column.key === 'ticker') return <><Link to={`/ticker/${encodeURIComponent(row.ticker)}`}>{row.ticker || 'N/A'}</Link><small>{row.lane === 'WATCH' ? 'Watch only' : row.indicators.sector || row.company_name}</small></>
    if (column.key === 'direction') return <span className={tone(row.direction)}>{row.direction === 1 ? <ArrowUp size={13} /> : row.direction === -1 ? <ArrowDown size={13} /> : null}{row.direction === 1 ? 'Long' : row.direction === -1 ? 'Short' : 'Context'}</span>
    if (column.key === 'model') return <>{models[row.model] || readable(row.model)}<small>{alertPlanTiming(row).interval}</small></>
    if (column.key === 'trade_style') return <>{alertPlanTiming(row).style}<small>{row.strategy_label}{row.opposing_exposure ? ' / Opposing exposure' : ''}</small></>
    if (column.key === 'published_at') return <span title={`Trigger ${time(row.triggered_at)} ET`}>{time(row.published_at)}</span>
    if (column.key === 'triggered_at') return <span title={row.triggered_at}>{time(row.triggered_at)}</span>
    if (column.key === 'hits') return <span title={`${data?.hit_coverage || 'No retained recurrence data'}. Models: ${row.hit_models.map(model => models[model] || model).join(', ')}`}>{number(row.hits, 0)}</span>
    if (column.key === 'latest_price') return <span title={row.latest_price_checked_at ? `Stored ${row.latest_price_interval || ''} bar; price read ${time(row.latest_price_checked_at)} ET` : column.tip}>{price(row.latest_price)}<small>{time(row.latest_price_at)} ET{row.latest_price_interval ? ` / ${row.latest_price_interval}` : ''}</small></span>
    if (column.key === 'price_return') return <span className={tone(row.price_return)} title={`${column.tip} ${row.price_return_status ? readable(row.price_return_status) : ''}`}>
      {percent(row.price_return)}<small>{row.price_return == null ? 'Comparison unavailable' : 'From trigger, gross'}</small>
    </span>
    if (column.key === 'entry_price') return <>{price(row.entry_price)}<small>{time(row.entry_at)}</small></>
    if (column.key === 'exit_price') return <>{price(row.exit_price)}<small>{time(row.exit_at)}</small></>
    if (column.key === 'paper_return') return <span className={tone(row.paper_return)}>{percent(row.paper_return)}<small>{row.status.includes('CLOSED') ? 'Fixed exit' : row.status.includes('OPEN') ? 'Unrealized' : row.status === 'NO_FILL' ? 'No exposure' : row.status === 'PENDING' ? 'Awaiting entry data' : ''}</small></span>
    if (column.key === 'status') return <span title={row.reason ? readable(row.reason) : undefined}>{readable(row.status)}{row.warnings.length > 0 && <ShieldAlert size={13} className="sa-risk-icon" aria-label="Risk details available" />}</span>
    if (column.key === 'success_probability') return <span title={unavailableAlertProbability.reason}>{unavailableAlertProbability.label}</span>
    if (column.key === 'risk_assessment') {
      const assessment = alertRiskAssessment(row)
      return <span className="sa-risk-assessment" title={[assessment.reason, ...assessment.cautions].join('\n')}>
        <span>{assessment.stopDistance === null ? 'Stop distance unavailable' : `${number(assessment.stopDistance * 100)}% to stop`}</span>
        <small>{assessment.label}{assessment.cautions.length ? ` / ${assessment.cautions.length} cautions` : ''}</small>
      </span>
    }
    if (['trigger_price', 'stop', 'target'].includes(column.key)) return price(item)
    if (['risk_pct', 'atr_pct', 'momentum', 'ema20_distance', 'ema50_distance', 'ema50_slope', 'volatility'].includes(column.key)) return percent(item)
    if (column.key === 'liquidity') return typeof item === 'number' ? `$${new Intl.NumberFormat('en-US', { notation: 'compact' }).format(item)}` : 'N/A'
    if (column.key === 'rs_percentile') return typeof item === 'number' ? `${number(item * 100, 1)}%` : 'N/A'
    if (['reward_risk', 'relative_volume', 'extension_atr'].includes(column.key)) return typeof item === 'number' ? `${number(item)}x` : 'N/A'
    if (column.key === 'rsi') return number(item, 1)
    return typeof item === 'string' ? item : 'N/A'
  }
  return <div className="stock-discovery stock-alerts">
    <section className="sd-toolbar sa-toolbar">
      <div className="sd-presets" role="tablist" aria-label="Alert views" onKeyDown={event => {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return
        event.preventDefault()
        const views: AlertView[] = source === 'SHADOW' ? ['latest', 'history', 'open'] : ['latest', 'history']
        const next = views[event.key === 'Home' ? 0 : event.key === 'End' ? views.length - 1 : (views.indexOf(view) + (event.key === 'ArrowRight' ? 1 : views.length - 1)) % views.length]
        changeView(next)
        event.currentTarget.querySelector<HTMLButtonElement>(`#sa-${next}`)?.focus()
      }}>
        <button role="tab" id="sa-latest" tabIndex={view === 'latest' ? 0 : -1} aria-controls="sa-results" aria-selected={view === 'latest'} className={view === 'latest' ? 'active' : ''} onClick={() => changeView('latest')}>Latest Run</button>
        <button role="tab" id="sa-history" tabIndex={view === 'history' ? 0 : -1} aria-controls="sa-results" aria-selected={view === 'history'} className={view === 'history' ? 'active' : ''} onClick={() => changeView('history')}>Day History</button>
        {source === 'SHADOW' && <button role="tab" id="sa-open" tabIndex={view === 'open' ? 0 : -1} aria-controls="sa-results" aria-selected={view === 'open'} className={view === 'open' ? 'active' : ''} onClick={() => changeView('open')}>Open Positions</button>}
      </div>
      <div className="sd-tools">
        <label className="sa-preset"><span className="sa-sr-only">Column preset</span><select aria-label="Column preset" value={selectedPreset} onChange={event => choosePreset(event.target.value)}><option value="" disabled>Custom columns</option><option value="plan">Trade plan</option><option value="trend">Trend</option><option value="momentum">Momentum</option><option value="liquidity">Liquidity</option></select></label>
        <ColumnPicker columns={selectable} hidden={hidden} onToggle={preferences.toggle} onShowAll={preferences.showAll} onReset={preferences.reset} />
        <button type="button" title="Export displayed alerts" aria-label="Export displayed alerts" disabled={!rows.length} onClick={() => exportAlerts(rows, visible, value)}><Download size={16} /></button>
        <button type="button" title="Refresh retained alerts" aria-label="Refresh retained alerts" disabled={query.isFetching} onClick={() => void query.refetch()}><RefreshCw size={16} /></button>
      </div>
    </section>
    {data?.combined && <section className="sa-streams" aria-label="Strategy publication status">{data.strategy_streams?.map(stream => {
      const latest = data.latest_runs?.find(run => run.strategy_instance_id === stream.instance_id)
      return <div key={stream.stream}><strong>{stream.label}</strong><span className={stream.error || stream.projector_stale ? 'sd-negative' : ''}>{readable(stream.status)}{stream.error ? ` / ${readable(stream.error)}` : ''}</span>
        <span>Source {time(stream.as_of)} ET / Imported {time(stream.imported_at)} ET</span>
        <span>{latest ? `${readable(latest.status)} / ${latest.selected} selected / ${time(latest.published_at)} ET` : 'No publication for selected session'}</span>
        {stream.publication_deadline && <span>Next window {time(stream.publication_window_start)} to {time(stream.publication_deadline)} ET</span>}
      </div>
    })}</section>}
    <div className="sa-trade-types" role="group" aria-label="Trade type">
      {([['', 'All'], ['INTRADAY', 'Intraday'], ['SWING', 'Swing']] as const).map(([key, label]) =>
        <button key={label} type="button" aria-pressed={(tradeType || '') === key} onClick={() => update({ trade_type: key || null, run: null })}>{label}</button>)}
      {view === 'open' && <span>All retained open, pending and unresolved positions</span>}
    </div>
    <section className="sd-filters sa-filters" aria-label="Alert filters">
      <label>Stock<input value={params.get('search') || ''} placeholder="Ticker or company" onChange={event => update({ search: event.target.value })} /></label>
      <label>Direction<select value={filters.direction || ''} onChange={event => update({ direction: event.target.value })}><option value="">All</option><option value="1">Long</option><option value="-1">Short</option></select></label>
      <label>Model<select value={filters.model || ''} onChange={event => update({ model: event.target.value })}><option value="">All models</option>{(source === 'LEGACY' ? ['legacy_daily'] : tradeAlertModels).map(key => <option key={key} value={key}>{models[key]}</option>)}</select></label>
      <label>Trigger interval<select value={params.get('interval') || ''} onChange={event => update({ interval: event.target.value })}><option value="">All</option>{['30m', '1h', '1d'].map(interval => <option key={interval}>{interval}</option>)}</select></label>
      <label>Status<select value={filters.status || ''} onChange={event => update({ status: event.target.value })}><option value="">All</option>{[...tradeAlertStatuses, ...(source === 'LEGACY' ? legacyAlertStatuses : [])].map(state => <option key={state} value={state}>{readable(state)}</option>)}</select></label>
    </section>
    <div className="sa-results-heading">
      <span role="status" aria-label="Matching alerts"><strong>{query.isLoading ? 'Loading' : data?.total ?? 'N/A'}</strong> {data?.total === 1 ? 'alert' : 'alerts'}</span>
      <details key={`${source}:${view}:${data?.session || ''}`} className="sa-run-details">
        <summary>Run details</summary>
        <div className="sa-run-content">
          <dl>
            <div><dt>Record source</dt><dd>{data?.source_label || sourceLabel}</dd></div>
            <div><dt>{source === 'REPLAY' ? 'Frozen cutoff (ET)' : 'Read as of (ET)'}</dt><dd>{time(data?.as_of)}</dd></div>
            {view === 'history' && data?.price_as_of && <div><dt>Current prices read (ET)</dt><dd>{time(data.price_as_of)}</dd></div>}
            <div><dt>Publications in session</dt><dd>{data?.runs.length ?? 'N/A'}</dd></div>
            {view === 'history' && data?.withheld_run && <div><dt>Latest run excluded (ET)</dt><dd>{time(data.withheld_run.published_at)}</dd></div>}
            {data?.publication_mode === 'SOURCE_READINESS' && <div><dt>Next source-ready window (ET)</dt><dd>{time(data.publication_window_start)} to {time(data.publication_deadline)}</dd></div>}
            <div><dt>Matched outcomes</dt><dd>{(data?.counts.CLOSED || 0) + (data?.counts.CLOSED_PAPER || 0)} closed / {data?.counts.NO_FILL || 0} no fill</dd></div>
            {view === 'latest' && data?.run && <>
              <div><dt>Trigger / published (ET)</dt><dd>{time(data.run.trigger_at)} / {time(data.run.published_at)}</dd></div>
              <div><dt>Publication status</dt><dd>{readable(data.run.status)}</dd></div>
              <div><dt>Input coverage</dt><dd>{data.run.missing == null ? 'Unavailable' : `${data.run.expected - data.run.missing}/${data.run.expected}`}</dd></div>
            </>}
            <div><dt>Paper policy</dt><dd>{data?.outcome_policy || 'Unavailable'}</dd></div>
            <div><dt>Study / policy</dt><dd>{data?.source_id || 'Unavailable'}</dd></div>
          </dl>
          {view === 'latest' && !!data?.runs.length && <label className="sa-run-selector">Publication (ET)<select aria-label="Publication run" value={params.get('run') || ''} onChange={event => update({ run: event.target.value })}><option value="">{data.combined ? 'Latest per strategy' : 'Latest publication'}</option>{[...data.runs].reverse().map(run => <option key={run.run_id} value={run.run_id}>{run.strategy_label ? `${run.strategy_label} / ` : ''}{time(run.published_at)} / {run.selected} selected</option>)}</select></label>}
          {!!data?.warnings.length && <ul className="sa-run-notes">{data.warnings.map(warning => <li key={warning}>{warning}</li>)}</ul>}
          <nav className="sa-record-links" aria-label="Historical records">
            {source !== 'LEGACY' && <Link to={`?${alertSourceParams('LEGACY', 'history')}`}>Legacy daily history</Link>}
            {requestedSource !== 'REPLAY' && <Link to={`?${alertSourceParams('REPLAY', 'history')}`}>Backtested history</Link>}
          </nav>
        </div>
      </details>
    </div>
    {(incompleteCoverage || dataWarnings.length > 0) && <div className="sa-data-warning" role="note"><AlertTriangle size={14} /><span>{incompleteCoverage ? 'Some alert inputs were unavailable at publication time. ' : ''}{dataWarnings.join(' / ')}</span></div>}
    {query.isError && <div className="sd-notice" role="alert"><AlertTriangle size={16} />Alert view unavailable<button onClick={() => void query.refetch()}>Retry</button>{params.get('session_date') && <button onClick={() => update({ session_date: null, run: null })}>Latest session</button>}</div>}
    <section id="sa-results" role="tabpanel" aria-labelledby={`sa-${view}`} className="sd-table-panel">
      {data?.publication_mode === 'SOURCE_READINESS' && <div className="sa-data-warning" role="status"><Clock size={14} /><span>Next publication after inputs complete: {time(data.publication_window_start)} to {time(data.publication_deadline)} ET</span></div>}
      {query.isLoading ? <div className="sd-empty" role="status">Loading retained alerts...</div> : query.isError ? null : !rows.length ? <div className="sd-empty"><Bell size={24} /><strong>{data?.status === 'WAITING_FOR_PUBLICATION' ? 'Waiting for the next forward publication' : data?.status === 'AWAITING_PUBLICATION' ? source === 'SHADOW' ? 'No shadow publications yet' : 'Awaiting retained publications' : !data?.runs.length ? view === 'history' && data?.withheld_run ? 'No earlier runs for this session' : 'No publication recorded for this session' : view === 'latest' && data?.run?.selected === 0 ? 'No new alerts in this publication' : view === 'history' && data?.runs.every(run => run.selected === 0) ? data?.withheld_run ? 'No alerts in earlier runs' : 'No alerts published for this session' : 'No matching alerts'}</strong>{data?.next_publication_at && <span>Next publication {time(data.next_publication_at)} ET</span>}{source === 'SHADOW' && data?.status === 'AWAITING_PUBLICATION' && <button type="button" className="sa-history-action" onClick={() => setParams(alertSourceParams('REPLAY', 'history'))}><History size={16} />View backtested history</button>}</div> :
        <div className="sd-table-scroll sa-scroll"><table><thead><tr><th aria-label="Plan details" />{visible.map(column => <th key={column.key} aria-sort={sort === column.key ? descending ? 'descending' : 'ascending' : undefined}><button type="button" title={column.tip || column.label} onClick={() => sortColumn(column.key)}>{column.key === 'latest_price' && source === 'REPLAY' ? 'Price at cutoff' : column.label}{sort === column.key && (descending ? <ArrowDown size={11} /> : <ArrowUp size={11} />)}</button></th>)}</tr></thead><tbody>
          {rows.map(row => <Fragment key={row.alert_id}><tr><td><button className="sd-expand" aria-label={`Plan details for ${row.ticker} ${row.interval}`} aria-expanded={expanded === row.alert_id} onClick={() => setExpanded(expanded === row.alert_id ? null : row.alert_id)}>{expanded === row.alert_id ? <ChevronDown size={15} /> : <ChevronRight size={15} />}</button></td>
            {visible.map(column => <td key={column.key} className={column.key === 'ticker' ? 'sd-symbol' : ''} title={column.indicator ? `${column.tip || column.label}. ${column.key === 'momentum' || column.key === 'liquidity' || column.key === 'rs_percentile' ? time(row.daily_context_at) : `${row.indicator_interval} ${time(row.indicator_at)}`} ET; ${readable(row.indicator_status)}` : column.tip}>{cell(row, column)}</td>)}
          </tr>{expanded === row.alert_id && <tr className="sd-detail"><td colSpan={visible.length + 1}><dl>
            <div><dt>Trigger / published (ET)</dt><dd>{time(row.triggered_at)} / {time(row.published_at)}</dd></div>
            {row.trade_style === 'SWING' && <><div><dt>Daily setup (ET)</dt><dd>{time(row.daily_setup_at)}</dd></div>
              <div><dt>Entry confirmation</dt><dd>{row.confirmation_interval || row.interval} / {time(row.triggered_at)} ET</dd></div>
              <div><dt>Holding policy</dt><dd>{alertPlanTiming(row).hold}; entry session counts as one</dd></div>
              <div><dt>Daily setup ID</dt><dd>{row.daily_setup_id || 'Unavailable'}</dd></div></>}
            <div><dt>Original bracket</dt><dd>{price(row.stop)} stop / {price(row.target)} target</dd></div>
            {row.strategy_instance_id && <><div><dt>Strategy instance</dt><dd>{row.strategy_instance_id}</dd></div>
              <div><dt>Original alert / run</dt><dd>{row.original_alert_id} / {row.original_run_id}</dd></div>
              <div><dt>Exposure</dt><dd>{row.opposing_exposure ? 'Opposite-direction open or pending plan also retained; positions not netted' : 'Independent strategy plan'}</dd></div></>}
            <div><dt>Entry risk / reward-risk</dt><dd>{percent(row.entry_risk?.risk_pct)} / {number(row.entry_risk?.reward_risk)}x</dd></div>
            <div><dt>Latest eligible exit (ET)</dt><dd>{time(row.exit_due_at)}</dd></div>
            <div><dt>First / last seen (ET)</dt><dd>{time(row.first_seen)} / {time(row.last_seen)}</dd></div>
            <div><dt>Hit attribution</dt><dd>{row.hit_models.map(model => models[model] || model).join(', ') || 'Unavailable'} / {row.hit_intervals.join(', ')}</dd></div>
            <div><dt>Risk / data warnings</dt><dd>{row.warnings.join('; ')}{row.reason ? `; ${readable(row.reason)}` : ''}</dd></div>
            <div><dt>Indicator snapshot</dt><dd>{row.indicator_interval} / {time(row.indicator_at)} ET / {readable(row.indicator_status)}</dd></div>
            {view === 'history' && <><div><dt>Paper mark (ET)</dt><dd>{price(row.mark_price)} / {time(row.mark_at)}</dd></div><div><dt>Fixed exit (ET)</dt><dd>{price(row.exit_price)} / {time(row.exit_at)}</dd></div></>}
            <div><dt>Policy</dt><dd>{row.policy_version}</dd></div><div><dt>Recurrence coverage</dt><dd>{data?.hit_coverage || 'Unavailable in this source'}</dd></div>
          </dl><SavedAlertContext row={row} /></td></tr>}</Fragment>)}
        </tbody></table></div>}
      <footer className="sd-pager"><span>{data?.total ? `${offset + 1}-${offset + rows.length} of ${data.total}` : '0 results'}</span><div><button title="Previous page" aria-label="Previous page" disabled={!offset} onClick={() => update({ offset: String(Math.max(0, offset - 100)) })}><ArrowLeft size={16} /></button><button title="Next page" aria-label="Next page" disabled={offset + rows.length >= (data?.total || 0)} onClick={() => update({ offset: String(offset + 100) })}><ArrowRight size={16} /></button></div></footer>
    </section>
  </div>
}