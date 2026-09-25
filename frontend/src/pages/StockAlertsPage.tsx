import { useDeferredValue, useEffect, useEffectEvent, useRef, useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { AlertTriangle, ArrowDown, ArrowLeft, ArrowRight, ArrowUp, Bell, Clock, Download, Eye, History, RefreshCw, ShieldAlert, X } from 'lucide-react'
import { ColumnPicker, useColumnPreferences, type ColumnSpec } from '../layout/PageChrome'
import { usePublishPageContext } from '../layout/pageContext'
import { getAlertView, type AlertPlanRow } from '../services/stockDiscovery'
import { alertSort, alertSourceParams, alertTabParams, alertTradeFilters, legacyAlertStatuses, resolveAlertRoute, tradeAlertModels, tradeAlertStatuses, type AlertView } from './stockAlertNavigation'
import { alertColumnLayout, alertPlanTiming, alertPublicationFacts, alertRiskAssessment, earliestAlertWindow, unavailableAlertProbability } from './stockAlertPresentation'
import StockEodReviewPanel from './StockEodReviewPanel'
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
  { key: 'triggered_at', label: 'Triggered (ET)', group: 'Alert details', tip: 'Original trigger-bar timestamp, separate from when the alert was published' },
  { key: 'ticker', label: 'Stock', group: 'Alert details', locked: true }, { key: 'direction', label: 'Direction', group: 'Alert details', locked: true },
  { key: 'trade_style', label: 'Trade type', group: 'Alert details', locked: true, tip: 'Independent original plan: intraday minute cap or swing trading-session horizon. Shared display does not merge positions, quotas or performance.' },
  { key: 'published_at', label: 'Published (ET)', group: 'Alert details' },
  { key: 'trigger_price', label: 'Trigger price', group: 'Trade plan', tip: 'Completed trigger-bar price, not an executable fill' },
  { key: 'latest_price', label: 'Current price', group: 'Price comparison', history: true, tip: 'Latest eligible stored completed-bar price; delayed, not a real-time quote. Replay prices stay frozen at cutoff.' },
  { key: 'price_return', label: 'Price P/L %', group: 'Price comparison', history: true, tip: 'Direction-adjusted move from the original trigger price to the displayed price, before costs. Not an executed return; ignores paper entry and stop/target exits.' },
  { key: 'entry_price', label: 'Paper entry', group: 'Paper outcomes', history: true }, { key: 'stop', label: 'Original stop', group: 'Trade plan' }, { key: 'target', label: 'Target price', group: 'Trade plan', tip: 'Original target frozen with the alert plan, not a price forecast' },
  { key: 'risk_pct', label: 'Risk to stop', group: 'Trade plan', tip: 'Distance from trigger price to stop, not a maximum-loss guarantee' },
  { key: 'reward_risk', label: 'Reward/risk', group: 'Trade plan', tip: 'Frozen trigger-price target room divided by stop risk' },
  { key: 'hits', label: 'Hits', group: 'Alert details', tip: 'Distinct valid stock/direction publication windows over 21 sessions; not confidence' },
  { key: 'success_probability', label: 'Success probability', group: 'Trade plan', hiddenByDefault: true, tip: unavailableAlertProbability.reason },
  { key: 'risk_assessment', label: 'Risk assessment', group: 'Trade plan', hiddenByDefault: true, tip: 'Original stop distance and retained cautions; not a calibrated risk score or maximum-loss guarantee' },
  { key: 'model', label: 'Model / interval', group: 'Alert details', tip: 'Swing: the daily setup owns the bracket and session horizon; intraday confirmation only times entry. Intraday: a separate short-duration plan.' },
  { key: 'hold', label: 'Maximum hold', group: 'Trade plan', tip: 'Swing limits count trading sessions, not calendar days; the entry session counts as session one. Intraday limits run from entry until the minute cap or session close. Stop or target exits can occur earlier; gaps can exceed stop risk.' },
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

function AlertDetailField({ label, value }: { label: string; value: ReactNode }) {
  return <div className="sa-detail-field"><span>{label}</span><strong>{value}</strong></div>
}

function AlertDetailDrawer({ row, returnFocus, onClose }: {
  row: AlertPlanRow
  returnFocus: HTMLButtonElement | null
  onClose: () => void
}) {
  const assessment = alertRiskAssessment(row)
  const timing = alertPlanTiming(row)
  const direction = row.direction === 1 ? 'Long' : row.direction === -1 ? 'Short' : 'Context'
  const displayedModels = (row.display_models || [row.model]).map(model => models[model] || readable(model))
  const displayedIntervals = row.display_intervals || [row.interval]
  const publicationFacts = alertPublicationFacts(row)
  const closeOnEscape = useEffectEvent(onClose)
  useEffect(() => {
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') closeOnEscape()
    }
    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.body.style.overflow = previousOverflow
      returnFocus?.focus()
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [returnFocus])
  return <div className="sa-drawer-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
    <aside className="sa-alert-drawer" role="dialog" aria-modal="true" aria-labelledby="sa-alert-drawer-title">
      <header><div><span>{timing.style} alert review</span><h2 id="sa-alert-drawer-title"><Link to={`/ticker/${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link> <small className={tone(row.direction)}>{direction}</small></h2><p>{displayedModels.join(' + ')} / {displayedIntervals.join(' + ')} / {readable(row.status)}</p></div>
        <button type="button" className="sa-drawer-close" title="Close alert details" aria-label="Close alert details" autoFocus onClick={onClose}><X size={18} /></button></header>
      <div className="sa-alert-drawer__body">
        <section><h3>Trade plan</h3><div className="sa-detail-grid">
          <AlertDetailField label="Trigger" value={price(row.trigger_price)} />
          <AlertDetailField label="Original stop" value={price(row.stop)} />
          <AlertDetailField label="Target" value={price(row.target)} />
          <AlertDetailField label="Risk to stop" value={percent(row.risk_pct)} />
          <AlertDetailField label="Reward / risk" value={`${number(row.reward_risk)}x`} />
          <AlertDetailField label="Maximum hold" value={timing.hold} />
        </div>{(row.plan_count || 1) > 1 && <p className="sa-detail-empty">Primary plan shown above; {row.plan_count} same-side plans are consolidated in this row.</p>}</section>
        {(row.plan_variants?.length || 0) > 1 && <section><h3>Plan variants</h3><div className="sa-variant-list">{row.plan_variants!.map(variant => <div key={variant.alert_id}>
          <span><strong>{models[variant.model] || readable(variant.model)} / {variant.interval}</strong><small>{readable(variant.status)}</small></span>
          <span>{price(variant.trigger_price)} trigger / {price(variant.stop)} stop / {price(variant.target)} target</span>
          <span>{variant.status === 'NO_FILL' ? 'No paper entry' : variant.paper_return == null ? 'Outcome pending' : `${percent(variant.paper_return)} paper P/L`}</span>
        </div>)}</div></section>}
        <section><h3>Trigger evidence</h3><div className="sa-detail-grid sa-detail-grid--metrics">
          <AlertDetailField label="Price vs EMA50" value={percent(row.indicators.ema50_distance)} />
          <AlertDetailField label="EMA50 slope / 10 bars" value={percent(row.indicators.ema50_slope)} />
          <AlertDetailField label="RSI14" value={number(row.indicators.rsi, 1)} />
          <AlertDetailField label="12-1 momentum" value={percent(row.indicators.momentum)} />
          <AlertDetailField label="RS63 percentile" value={typeof row.indicators.rs_percentile === 'number' ? `${number(row.indicators.rs_percentile * 100, 1)}%` : 'N/A'} />
          <AlertDetailField label={`${row.indicator_interval || row.interval} relative volume`} value={typeof row.indicators.relative_volume === 'number' ? `${number(row.indicators.relative_volume)}x` : 'N/A'} />
          <AlertDetailField label="Daily relative volume (20-session)" value={<>{typeof row.indicators.daily_rvol20 === 'number' ? `${number(row.indicators.daily_rvol20)}x` : 'N/A'}<small>{row.daily_context_at ? `${time(row.daily_context_at)} ET completed bar` : 'Completed daily context unavailable'}</small></>} />
        </div></section>
        <section className={assessment.cautions.length ? 'sa-detail-risk is-caution' : 'sa-detail-risk'}><h3>Risk and data</h3><p>{assessment.reason}</p>{assessment.cautions.length ? <ul>{assessment.cautions.map(caution => <li key={caution}>{caution}</li>)}</ul> : <p>No evidence-specific warning at the retained trigger snapshot.</p>}</section>
        <section><h3>Ticker and alert context</h3>{publicationFacts.length ? <><div className="sa-detail-grid sa-detail-grid--context">{publicationFacts.map(fact => <AlertDetailField key={fact.label} label={fact.label} value={fact.value} />)}</div>{row.context?.status !== 'AVAILABLE' && <p className="sa-detail-empty">Publication factors were not retained; showing available alert history.</p>}</> : <p className="sa-detail-empty">Ticker and alert context were not retained for this publication.</p>}</section>
        <section><h3>Paper status</h3><div className="sa-detail-grid">
          <AlertDetailField label="Status" value={readable(row.status)} />
          <AlertDetailField label="Paper entry" value={<>{price(row.entry_price)}<small>{time(row.entry_at)} ET</small></>} />
          <AlertDetailField label="Latest stored price" value={<>{price(row.latest_price)}<small>{time(row.latest_price_at)} ET</small></>} />
          <AlertDetailField label="Paper P/L" value={percent(row.paper_return)} />
          <AlertDetailField label="Latest eligible exit" value={`${time(row.exit_due_at)} ET`} />
          {row.exit_price != null && <AlertDetailField label="Fixed exit" value={<>{price(row.exit_price)}<small>{time(row.exit_at)} ET</small></>} />}
        </div></section>
      </div>
    </aside>
  </div>
}

export default function StockAlertsPage() {
  const [params, setParams] = useSearchParams()
  const [selectedAlertId, setSelectedAlertId] = useState<string | null>(null)
  const [eodSessions, setEodSessions] = useState({ dates: [] as string[], selected: '' })
  const detailTrigger = useRef<HTMLButtonElement | null>(null)
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
  const query = useQuery({ queryKey: ['stock-alert-view', args], queryFn: () => getAlertView(args), enabled: view !== 'eod', refetchInterval: requestedSource === 'REPLAY' ? false : 30_000 })
  const data = view === 'eod' ? undefined : query.data
  const source = data?.source || requestedSource
  const sourceLabel = source === 'REPLAY' ? 'Backtested history' : source === 'LEGACY' ? 'Legacy daily history' : data?.combined ? 'Intraday + swing shadow' : data?.source_id === 'stock_ideas_forward_swing_v1' ? 'Swing shadow' : 'Forward shadow'
  const rows = data?.rows || []
  const selectedAlert = rows.find(row => row.alert_id === selectedAlertId) || null
  const displayedRuns = view === 'history' ? data?.runs || [] : view === 'open' ? [] : data?.latest_runs || (data?.run ? [data.run] : [])
  const incompleteCoverage = displayedRuns.some(run => (run.missing ?? 0) > 0 || run.status === 'MISSED_PUBLICATION')
  const dataWarnings = data?.warnings.filter(warning => /stale|missing|unavailable/i.test(warning)) || []
  const currentSchedules = data?.schedule_streams?.filter(stream => !tradeType || stream.stream === (tradeType === 'SWING' ? 'swing' : 'intraday'))
  const nextWindow = earliestAlertWindow(currentSchedules ?? (data?.combined ? data.strategy_streams?.filter(stream => !tradeType
    || stream.stream === (tradeType === 'SWING' ? 'swing' : 'intraday')) : data?.publication_window_start && data.publication_deadline
    ? [{ publication_window_start: data.publication_window_start, publication_deadline: data.publication_deadline }] : []))
  const windowOverdue = !!nextWindow && Date.parse(nextWindow.publication_deadline!) < Date.now()
  const skippedWindow = currentSchedules ? currentSchedules.some(stream => (stream.skipped_windows || 0) > 0)
    : displayedRuns.some(run => run.status === 'MISSED_PUBLICATION')
  const scheduleWarnings = currentSchedules?.filter(stream => stream.status !== 'RETAINED') || []
  const latestWorkerPublication = currentSchedules?.filter(stream => stream.latest_run?.status === 'PUBLISHED')
    .sort((left, right) => Date.parse(right.latest_run!.published_at) - Date.parse(left.latest_run!.published_at))[0]
  const retainedDataStale = data?.strategy_streams?.some(stream => stream.projector_stale || stream.status === 'STALE' || !!stream.error)
  const update = (values: Record<string, string | null>) => {
    const next = new URLSearchParams(params)
    Object.entries(values).forEach(([key, value]) => value ? next.set(key, value) : next.delete(key))
    if (!('offset' in values)) next.delete('offset')
    setParams(next)
    setSelectedAlertId(null)
  }
  const changeView = (next: AlertView) => {
    setParams(alertTabParams(params, next, data?.source === 'SHADOW' && !!data.sessions.length))
    setSelectedAlertId(null)
  }
  usePublishPageContext({ eyebrow: 'Stocks', title: 'Stock Alerts',
    status: [{ label: 'Data', value: view === 'eod' ? 'Retained selection evidence' : sourceLabel, title: source === 'REPLAY' ? `Simulated alerts, not delivered live. Prices and outcomes frozen at ${time(data?.as_of)} ET.` : data?.source_label }],
    alertSessions: view === 'eod'
      ? { dates: eodSessions.dates, selected: eodSessions.selected, source: 'STOCK_EOD_REVIEW', emptyLabel: 'No evaluated sessions', resetKeys: ['offset'] }
      : { dates: data?.sessions || [], selected: data?.session || '', source: requestedSource } })
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
    if (column.key === 'model') {
      const displayedModels = (row.display_models || [row.model]).map(model => models[model] || readable(model))
      const displayedIntervals = row.display_intervals || [row.interval]
      return <>{displayedModels.join(' + ')}<small>{(row.plan_count || 1) > 1 ? `${displayedIntervals.join(' + ')} / ${row.plan_count} plans` : alertPlanTiming(row).interval}</small></>
    }
    if (column.key === 'trade_style') return <>{alertPlanTiming(row).style}<small>{row.strategy_label}{row.opposing_exposure ? ' / Opposing exposure' : ''}</small></>
    if (column.key === 'published_at') return <span title={`Trigger ${time(row.triggered_at)} ET`}>{time(row.published_at)}</span>
    if (column.key === 'triggered_at') return <span title={row.triggered_at}>{time(row.triggered_at)}</span>
    if (column.key === 'hits') return <span title={`${data?.hit_coverage || 'No retained recurrence data'}. Models: ${row.hit_models.map(model => models[model] || model).join(', ')}`}>{number(row.hits, 0)}</span>
    if (column.key === 'latest_price') return <span className={view === 'history' ? tone(row.price_return) : ''} title={row.latest_price_checked_at ? `Stored ${row.latest_price_interval || ''} bar; price read ${time(row.latest_price_checked_at)} ET` : column.tip}>{price(row.latest_price)}<small>{time(row.latest_price_at)} ET{row.latest_price_interval ? ` / ${row.latest_price_interval}` : ''}</small></span>
    if (column.key === 'price_return') return <span className={tone(row.price_return)} title={`${column.tip} ${row.price_return_status ? readable(row.price_return_status) : ''}`}>
      {percent(row.price_return)}<small>{row.price_return == null ? 'Comparison unavailable' : 'From trigger, gross'}</small>
    </span>
    if (column.key === 'entry_price') return <>{price(row.entry_price)}<small>{time(row.entry_at)}</small></>
    if (column.key === 'exit_price') return <span className={view === 'history' ? tone(row.paper_return) : ''}>{price(row.exit_price)}<small>{time(row.exit_at)}</small></span>
    if (column.key === 'paper_return') return <span className={tone(row.paper_return)}>{percent(row.paper_return)}<small>{row.status.includes('CLOSED') ? 'Fixed exit' : row.status.includes('OPEN') ? 'Unrealized' : row.status === 'NO_FILL' ? 'No exposure' : row.status === 'PENDING' ? 'Awaiting entry data' : ''}</small></span>
    if (column.key === 'status') {
      const cautions = alertRiskAssessment(row).cautions
      return <span title={row.reason ? readable(row.reason) : undefined}>{readable(row.status)}{cautions.length > 0 && <ShieldAlert size={13} className="sa-risk-icon" aria-label="Risk details available" />}</span>
    }
    if (column.key === 'success_probability') return <span title={unavailableAlertProbability.reason}>{unavailableAlertProbability.label}</span>
    if (column.key === 'risk_assessment') {
      const assessment = alertRiskAssessment(row)
      return <span className="sa-risk-assessment" title={[assessment.reason, ...assessment.cautions].join('\n')}>
        <span>{assessment.stopDistance === null ? 'Stop distance unavailable' : `${number(assessment.stopDistance * 100)}% to stop`}</span>
        <small>{assessment.label}{assessment.cautions.length ? ` / ${assessment.cautions.length} cautions` : ''}</small>
      </span>
    }
    if (column.key === 'trigger_price') return <span className={view === 'history' ? 'sa-trigger-price' : undefined}>{price(item)}</span>
    if (column.key === 'stop') return <span className={view === 'history' ? 'sd-negative' : undefined}>{price(item)}</span>
    if (column.key === 'target') return <span className={view === 'history' ? 'sd-positive' : undefined}>{price(item)}</span>
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
        const views: AlertView[] = source === 'SHADOW' ? ['latest', 'history', 'eod', 'open'] : ['latest', 'history']
        const next = views[event.key === 'Home' ? 0 : event.key === 'End' ? views.length - 1 : (views.indexOf(view) + (event.key === 'ArrowRight' ? 1 : views.length - 1)) % views.length]
        changeView(next)
        event.currentTarget.querySelector<HTMLButtonElement>(`#sa-${next}`)?.focus()
      }}>
        <button role="tab" id="sa-latest" tabIndex={view === 'latest' ? 0 : -1} aria-controls="sa-results" aria-selected={view === 'latest'} className={view === 'latest' ? 'active' : ''} onClick={() => changeView('latest')}>Latest Run</button>
        <button role="tab" id="sa-history" tabIndex={view === 'history' ? 0 : -1} aria-controls="sa-results" aria-selected={view === 'history'} className={view === 'history' ? 'active' : ''} onClick={() => changeView('history')}>Day History</button>
        {source === 'SHADOW' && <button role="tab" id="sa-eod" tabIndex={view === 'eod' ? 0 : -1} aria-controls="sa-results" aria-selected={view === 'eod'} className={view === 'eod' ? 'active' : ''} onClick={() => changeView('eod')}>EOD Review</button>}
        {source === 'SHADOW' && <button role="tab" id="sa-open" tabIndex={view === 'open' ? 0 : -1} aria-controls="sa-results" aria-selected={view === 'open'} className={view === 'open' ? 'active' : ''} onClick={() => changeView('open')}>Open Positions</button>}
      </div>
      {view !== 'eod' && <div className="sd-tools">
        <label className="sa-preset"><span className="sa-sr-only">Column preset</span><select aria-label="Column preset" value={selectedPreset} onChange={event => choosePreset(event.target.value)}><option value="" disabled>Custom columns</option><option value="plan">Trade plan</option><option value="trend">Trend</option><option value="momentum">Momentum</option><option value="liquidity">Liquidity</option></select></label>
        <ColumnPicker columns={selectable} hidden={hidden} onToggle={preferences.toggle} onShowAll={preferences.showAll} onReset={preferences.reset} />
        <button type="button" title="Export displayed alerts" aria-label="Export displayed alerts" disabled={!rows.length} onClick={() => exportAlerts(rows, visible, value)}><Download size={16} /></button>
        <button type="button" title="Refresh retained alerts" aria-label="Refresh retained alerts" disabled={query.isFetching} onClick={() => void query.refetch()}><RefreshCw size={16} /></button>
      </div>}
    </section>
    {view === 'eod' ? <StockEodReviewPanel params={params} update={update} offset={offset} onSessionContext={setEodSessions} /> : <>
    <div className="sa-trade-types" role="group" aria-label="Trade type">
      {([['', 'All'], ['INTRADAY', 'Intraday'], ['SWING', 'Swing']] as const).map(([key, label]) =>
        <button key={label} type="button" aria-pressed={(tradeType || '') === key} onClick={() => update({ trade_type: key || null, run: null })}>{label}</button>)}
      {view === 'open' && <span>All retained open, pending and unresolved positions</span>}
      {source === 'SHADOW' && (retainedDataStale || nextWindow || skippedWindow || scheduleWarnings.length > 0) && <span className="sa-schedule" role="status">
        {retainedDataStale && <span className="sa-schedule__stale"><AlertTriangle size={13} />Retained data stale</span>}
        {nextWindow && <strong className={windowOverdue || skippedWindow ? 'sa-schedule__warning' : undefined}><Clock size={13} />{windowOverdue ? 'Alert window overdue' : 'Next alert window'} {time(nextWindow.publication_window_start)}-{time(nextWindow.publication_deadline)} ET</strong>}
        {skippedWindow && <strong className="sa-schedule__warning">Recorded skipped publication</strong>}
        {latestWorkerPublication?.latest_run && <span title="Current worker snapshot, separate from the retained results projection">Latest {latestWorkerPublication.label.toLowerCase()} run {time(latestWorkerPublication.latest_run.trigger_at)} / published {time(latestWorkerPublication.latest_run.published_at)} ET</span>}
        {windowOverdue && !skippedWindow && <strong className="sa-schedule__warning">Completion unverified</strong>}
        {scheduleWarnings.map(stream => <strong key={stream.stream} className="sa-schedule__warning">{stream.label}: {stream.status === 'OVERDUE' ? 'window overdue' : 'window unavailable'}</strong>)}
      </span>}
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
      {query.isLoading ? <div className="sd-empty" role="status">Loading retained alerts...</div> : query.isError ? null : !rows.length ? <div className="sd-empty"><Bell size={24} /><strong>{data?.status === 'WAITING_FOR_PUBLICATION' ? 'Waiting for the next forward publication' : data?.status === 'AWAITING_PUBLICATION' ? source === 'SHADOW' ? 'No shadow publications yet' : 'Awaiting retained publications' : !data?.runs.length ? view === 'latest' ? 'No current-session alerts' : view === 'history' && data?.withheld_run ? 'No earlier runs for this session' : 'No publication recorded for this session' : view === 'latest' && data?.run?.selected === 0 ? 'No new alerts in this publication' : view === 'history' && data?.runs.every(run => run.selected === 0) ? data?.withheld_run ? 'No alerts in earlier runs' : 'No alerts published for this session' : view === 'latest' && tradeType ? `No ${tradeType.toLowerCase()} alerts in this run` : 'No matching alerts'}</strong>{view === 'latest' && !data?.runs.length && <span>No publication recorded for this session.</span>}{data?.next_publication_at && <span>Next publication {time(data.next_publication_at)} ET</span>}{source === 'SHADOW' && data?.status === 'AWAITING_PUBLICATION' && <button type="button" className="sa-history-action" onClick={() => setParams(alertSourceParams('REPLAY', 'history'))}><History size={16} />View backtested history</button>}</div> :
        <div className="sd-table-scroll sa-scroll"><table><thead><tr><th className="sa-plan-details" scope="col">View</th>{visible.map(column => <th key={column.key} aria-sort={sort === column.key ? descending ? 'descending' : 'ascending' : undefined}><button type="button" title={column.tip || column.label} onClick={() => sortColumn(column.key)}>{column.key === 'latest_price' && source === 'REPLAY' ? 'Price at cutoff' : column.label}{sort === column.key && (descending ? <ArrowDown size={11} /> : <ArrowUp size={11} />)}</button></th>)}</tr></thead><tbody>
          {rows.map(row => <tr key={row.alert_id}>
            <td className="sa-plan-details"><button className="sd-expand sa-detail-launch" title="View alert details" aria-label={`View alert details for ${row.ticker}`} aria-haspopup="dialog" onClick={event => { detailTrigger.current = event.currentTarget; setSelectedAlertId(row.alert_id) }}><Eye size={15} /></button></td>
            {visible.map(column => <td key={column.key} className={column.key === 'ticker' ? 'sd-symbol' : ''} title={column.indicator ? `${column.tip || column.label}. ${column.key === 'momentum' || column.key === 'liquidity' || column.key === 'rs_percentile' ? time(row.daily_context_at) : `${row.indicator_interval} ${time(row.indicator_at)}`} ET; ${readable(row.indicator_status)}` : column.tip}>{cell(row, column)}</td>)}
          </tr>)}
        </tbody></table></div>}
      <footer className="sd-pager"><span>{data?.total ? `${offset + 1}-${offset + rows.length} of ${data.total}` : '0 results'}</span><div><button title="Previous page" aria-label="Previous page" disabled={!offset} onClick={() => update({ offset: String(Math.max(0, offset - 100)) })}><ArrowLeft size={16} /></button><button title="Next page" aria-label="Next page" disabled={offset + rows.length >= (data?.total || 0)} onClick={() => update({ offset: String(offset + 100) })}><ArrowRight size={16} /></button></div></footer>
    </section>
    {selectedAlert && <AlertDetailDrawer row={selectedAlert} returnFocus={detailTrigger.current} onClose={() => setSelectedAlertId(null)} />}
    </>}
  </div>
}