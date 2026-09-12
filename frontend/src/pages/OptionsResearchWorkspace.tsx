import { useEffect, useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import {
  Activity,
  AlertTriangle,
  ArrowRight,
  BarChart3,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  CircleHelp,
  Database,
  Eye,
  Gauge,
  History,
  LayoutList,
  LockKeyhole,
  PanelRightClose,
  Radar,
  Search,
  Settings,
  ShieldCheck,
  TrendingUp,
} from 'lucide-react'
import {
  getOptionAnalysis,
  getOptionCandidate,
  getOptionCandidates,
  getOptionChain,
  getOptionDataQuality,
  getOptionGamma,
  getOptionHealth,
  getOptionOpportunities,
  getOptionPerformance,
  getOptionSignals,
  getOptionUniverse,
  OptionAnalysisData,
  OptionCandidateDetailData,
  OptionCandidatePersona,
  OptionCandidateRow,
  OptionCandidateStatus,
  OptionCandidatesData,
  OptionChainData,
  OptionChainRow,
  OptionDataQualityData,
  OptionGammaData,
  OptionGammaStrike,
  OptionExclusionReason,
  OptionHealthData,
  OptionOpportunitiesData,
  OptionOpportunityRow,
  OptionPerformanceData,
  OptionPerformanceMeasurement,
  OptionPerformanceRow,
  OptionSignalsData,
  OptionSignalStatus,
  OptionsEnvelope,
  OptionUniverseRow,
} from '../services/api'
import { usePublishPageContext } from '../layout/pageContext'
import './OptionsResearchWorkspace.css'

type WorkspaceView = 'opportunities' | 'research' | 'candidates' | 'recommendations' | 'performance' | 'explorer' | 'operations'
type ResearchLens = 'income' | 'directional' | 'volatility' | 'activity' | 'gamma'
type OperationsSection = 'health' | 'universe' | 'quality'
type ContractTypeFilter = 'ALL' | 'CALL' | 'PUT'

const delayedLabel = '15-MINUTE DELAYED RESEARCH DATA'
const lenses: Array<{ value: ResearchLens; label: string; icon: typeof Activity }> = [
  { value: 'income', label: 'Income Evidence', icon: Gauge },
  { value: 'directional', label: 'Directional Context', icon: TrendingUp },
  { value: 'volatility', label: 'Volatility & Range', icon: BarChart3 },
  { value: 'activity', label: 'OI & Activity', icon: Activity },
  { value: 'gamma', label: 'Gamma Exposure', icon: Radar },
]

const candidateSuites: Array<{ value: OptionCandidatePersona; label: string; detail: string }> = [
  { value: 'INCOME', label: 'Income Generation', detail: 'Cash-secured put research with collateral and modeled return context.' },
  { value: 'DEFINED_RISK_INCOME', label: 'Defined-Risk Income', detail: 'Credit verticals and condors with complete listed legs and bounded loss.' },
  { value: 'MOMENTUM', label: 'Momentum / Activity', detail: '0-DTE directional structures plus research-only volume/OI and sweep-like activity findings. Activity alone does not imply direction.' },
  { value: 'NEUTRAL_VOL', label: 'Neutral / Vol', detail: 'Bounded range structures and volatility or activity research.' },
]

const lensCopy: Record<ResearchLens, { title: string; detail: string; unavailable: string }> = {
  income: {
    title: 'Listed put evidence',
    detail: 'Inspect delayed put observations by expiration and strike without selecting a Delta target or ranking an income strategy.',
    unavailable: 'Collateral, return on collateral, strategy rank, and management policy require persisted Phase 2 strategy evidence.',
  },
  directional: {
    title: 'Directional contract context',
    detail: 'Compare call and put observations without inferring a bullish or bearish thesis from contract activity.',
    unavailable: 'Finalized trend, event blackout, trigger direction, and structure selection require migration 016 context.',
  },
  volatility: {
    title: 'Volatility and range evidence',
    detail: 'Review persisted local-volatility fields and expiration surfaces only where the model-quality policy permits them.',
    unavailable: 'Historical IV rank, regime labels, scenario grids, and range strategies require versioned Phase 2 evidence.',
  },
  activity: {
    title: 'Open-interest and activity evidence',
    detail: 'Review volume and open interest in deterministic chain order. Activity is not trade direction or institutional ownership.',
    unavailable: 'Aggressor side, sweep classification, OI-based strategy claims, and quote marketability are not available.',
  },
  gamma: {
    title: 'Chain gamma exposure',
    detail: 'Per-strike call and put gamma are measured unsigned; the net value applies a versioned dealer assumption that open interest cannot confirm.',
    unavailable: 'Dealer positioning is modeled, not observed. Regime and flip levels are research context, not a directional recommendation.',
  },
}

const money = (value: string | number | null | undefined) => value == null
  ? 'Unavailable'
  : new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(Number(value))
const number = (value: number | null | undefined) => value == null
  ? 'Unavailable'
  : new Intl.NumberFormat('en-US', { maximumFractionDigits: 3 }).format(value)
const integer = (value: number | null | undefined) => value == null
  ? 'Unavailable'
  : new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(value)
const pct = (value: number | null | undefined) => value == null ? 'Unavailable' : `${(value * 100).toFixed(1)}%`
const signedMoney = (value: string | number | null | undefined) => {
  if (value == null) return 'Unavailable'
  const numeric = Number(value)
  return `${numeric > 0 ? '+' : ''}${money(numeric)}`
}
const dateTime = (value: string | null | undefined) => value
  ? new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }).format(new Date(value))
  : 'Unavailable'
const readable = (value: string) => value.replace(/_/g, ' ')
const age = (value: string | null | undefined) => {
  if (!value) return 'Unavailable'
  const minutes = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 60_000))
  if (minutes < 60) return `${minutes}m old`
  const hours = Math.floor(minutes / 60)
  return hours < 24 ? `${hours}h ${minutes % 60}m old` : `${Math.floor(hours / 24)}d old`
}
const candidateReasons = (row: OptionOpportunityRow) => row.signal_blocked_reasons?.length
  ? row.signal_blocked_reasons
  : row.reason_codes

function routeState(pathname: string, searchParams: URLSearchParams) {
  const parts = pathname.split('/').filter(Boolean)
  const route = parts[1]
  const requestedLens = searchParams.get('lens') as ResearchLens | null
  const requestedSection = searchParams.get('section') as OperationsSection | null
  let view: WorkspaceView = 'opportunities'
  let lens: ResearchLens = lenses.some(item => item.value === requestedLens) ? requestedLens! : 'income'
  let section: OperationsSection = ['health', 'universe', 'quality'].includes(requestedSection || '') ? requestedSection! : 'health'

  if (route === 'opportunities') view = 'opportunities'
  if (route === 'research') view = 'research'
  if (route === 'explorer' || route === 'chain') view = 'explorer'
  if (route === 'candidates') view = 'candidates'
  if (route === 'recommendations' || route === 'signals') view = 'recommendations'
  if (route === 'performance' || route === 'outcomes') view = 'performance'
  if (route === 'operations' || route === 'universe' || route === 'data-quality') view = 'operations'
  if (route === 'analysis') { view = 'research'; lens = 'volatility' }
  if (route === 'universe') section = 'universe'
  if (route === 'data-quality') section = 'quality'
  const requestedUnderlyer = parts[2] || searchParams.get('underlyer')

  return {
    view,
    lens,
    section,
    underlyer: (
      requestedUnderlyer || (view === 'opportunities' || view === 'candidates' || view === 'recommendations' || view === 'performance' ? 'ALL' : 'SPY')
    ).toUpperCase(),
  }
}

function Status({ value }: { value: string }) {
  const normalized = value.toLowerCase()
  const tone = normalized.includes('complete') || normalized.includes('ready') || normalized.includes('valid')
    ? 'good'
    : normalized.includes('failed') || normalized.includes('error') || normalized.includes('blocked')
      ? 'bad'
      : 'warn'
  return <span className={`options-status options-status--${tone}`}>{readable(value)}</span>
}

function StatePanel({ title, detail, warning = false }: { title: string; detail: string; warning?: boolean }) {
  return <section className={`options-state ${warning ? 'options-state--warning' : ''}`}>
    {warning ? <AlertTriangle size={19} /> : <Database size={19} />}
    <div><strong>{title}</strong><span>{detail}</span></div>
  </section>
}

function Metric({ label, value, detail }: { label: string; value: string; detail: string }) {
  return <div className="options-metric"><span>{label}</span><strong>{value}</strong><small>{detail}</small></div>
}

type TableColumn<TRow> = { key: string; label: string; fixed?: boolean; className?: string; render: (row: TRow) => ReactNode }

function ColumnSettings({ columns, hidden, onToggle }: { columns: { key: string; label: string; fixed?: boolean }[]; hidden: Set<string>; onToggle: (key: string) => void }) {
  const [open, setOpen] = useState(false)
  const selectable = columns.filter(column => !column.fixed && column.label)
  const hiddenCount = selectable.filter(column => hidden.has(column.key)).length
  const title = `Choose columns${hiddenCount ? ` (${hiddenCount} hidden)` : ''}`
  return <div className="options-column-settings">
    <button type="button" className={`options-icon-button ${open ? 'is-open' : ''}`} title={title} aria-label={title} aria-expanded={open} onClick={() => setOpen(!open)}><Settings size={15} /></button>
    {open && <div className="options-column-menu">{selectable.map(column => <label key={column.key}><input type="checkbox" checked={!hidden.has(column.key)} onChange={() => onToggle(column.key)} />{column.label}</label>)}</div>}
  </div>
}

function Field({ label, value, title }: { label: string; value: string; title?: string }) {
  return <div className="options-field"><span>{label}</span><strong title={title}>{value}</strong></div>
}

const workspaceHeaderCopy: Record<WorkspaceView, { eyebrow: string; title: string; detail: string }> = {
  opportunities: {
    eyebrow: 'Latest all-universe slate',
    title: 'Strategy-ranked option structures',
    detail: 'Leading packages from each backend strategy and underlyer, kept separate from anomaly and volatility detectors.',
  },
  research: {
    eyebrow: 'Causal market evidence',
    title: 'Option chain research',
    detail: 'Inspect retained contracts, model marks, volatility structure, and activity without inferring execution quality.',
  },
  explorer: {
    eyebrow: 'Contract-level evidence',
    title: 'Option chain explorer',
    detail: 'Compare retained contracts across expiration, strike, type, model value, and source quality.',
  },
  candidates: {
    eyebrow: 'Persisted decision audit',
    title: 'Strategy candidates',
    detail: 'Review selected, suppressed, and rejected structures with their original evidence and policy gates.',
  },
  recommendations: {
    eyebrow: 'Durable signal ledger',
    title: 'Published option signals',
    detail: 'Inspect immutable structured signal events, management levels, validity windows, and blocked reasons.',
  },
  performance: {
    eyebrow: 'Delayed proxy outcomes',
    title: 'Signal performance',
    detail: 'Review worker-materialized 15-minute, 30-minute, 60-minute, close, and next-open checkpoints.',
  },
  operations: {
    eyebrow: 'Pipeline control plane',
    title: 'Option operations',
    detail: 'Monitor universe coverage, ingestion quality, partitions, safety gates, and runtime readiness.',
  },
}

function WorkspaceNavigation({ view, underlyer, navigate }: { view: WorkspaceView; underlyer: string; navigate: ReturnType<typeof useNavigate> }) {
  const evidenceUnderlyer = underlyer === 'ALL' ? 'SPY' : underlyer
  const items: Array<{ value: string; activeViews: WorkspaceView[]; label: string; icon: typeof Activity; path: string }> = [
    { value: 'opportunities', activeViews: ['opportunities'], label: 'Opportunity Board', icon: Radar, path: '/options' },
    { value: 'research', activeViews: ['research', 'explorer'], label: 'Research', icon: BarChart3, path: `/options/research?underlyer=${evidenceUnderlyer}` },
    { value: 'decisions', activeViews: ['candidates', 'recommendations', 'performance'], label: 'Decisions', icon: LayoutList, path: underlyer === 'ALL' ? '/options/candidates' : `/options/candidates?underlyer=${underlyer}` },
    { value: 'operations', activeViews: ['operations'], label: 'Operations', icon: ShieldCheck, path: '/options/operations' },
  ]
  return <nav className="options-tabs" aria-label="Options workspace views">
    {items.map(item => { const Icon = item.icon; return <button key={item.value} type="button" className={item.activeViews.includes(view) ? 'active' : ''} onClick={() => navigate(item.path)}><Icon size={15} /><span>{item.label}</span></button> })}
  </nav>
}

function DecisionNavigation({ view, underlyer, navigate }: { view: 'candidates' | 'recommendations' | 'performance'; underlyer: string; navigate: ReturnType<typeof useNavigate> }) {
  const focus = underlyer === 'ALL' ? '' : `?underlyer=${underlyer}`
  return <section className="options-subview-bar">
    <div><LayoutList size={17} /><div><h2>Decisions</h2><p>Review the complete candidate audit trail and its published signal subset.</p></div></div>
    <div className="options-segmented" aria-label="Decision views"><button type="button" className={view === 'candidates' ? 'active' : ''} onClick={() => navigate(`/options/candidates${focus}`)}><LayoutList size={14} />Candidate audit</button><button type="button" className={view === 'recommendations' ? 'active' : ''} onClick={() => navigate(`/options/recommendations${focus}`)}><CheckCircle2 size={14} />Signal ledger</button><button type="button" className={view === 'performance' ? 'active' : ''} onClick={() => navigate(`/options/performance${focus}`)}><History size={14} />Performance</button></div>
  </section>
}

function OpportunityCommandBar({ strategies, strategy, dataTier, onStrategy }: {
  strategies: Array<{ value: string; label: string }>
  strategy: string
  dataTier: string
  onStrategy: (value: string) => void
}) {
  return <section className="options-opportunity-command" aria-label="Opportunity board filters">
    <label>Structured strategy<select value={strategy} onChange={event => onStrategy(event.target.value)}><option value="ALL">All structured strategies</option>{strategies.map(row => <option key={row.value} value={row.value}>{row.label}</option>)}</select></label>
    <div className="options-selection-basis"><Radar size={15} /><div><strong>Immutable Board publication</strong><span>Complete-universe revision; survivors reranked within strategy and underlyer</span></div><span className="options-inline-delay"><Activity size={12} />{dataTier}</span></div>
  </section>
}

function LatestOpportunityTable({ rows, hidden, onSelect, onTicker }: { rows: OptionOpportunityRow[]; hidden: Set<string>; onSelect: (candidateId: string) => void; onTicker: (ticker: string) => void }) {
  const columns = opportunityColumns(onSelect, onTicker)
  const visible = columns.filter(column => !hidden.has(column.key))
  return <div className="options-table-wrap"><table className="options-table options-table--opportunities"><thead><tr>{visible.map(column => <th key={column.key} aria-label={column.label ? undefined : 'Details'}>{column.label}</th>)}</tr></thead><tbody>{rows.map(row => <tr key={row.candidate_id}>{visible.map(column => <td key={column.key} className={column.className}>{column.render(row)}</td>)}</tr>)}</tbody></table></div>
}

function opportunityColumns(onSelect: (candidateId: string) => void, onTicker: (ticker: string) => void): TableColumn<OptionOpportunityRow>[] {
  return [
    { key: 'source', label: 'Source', render: row => <>{dateTime(row.market_data_time)}<small className="options-cell-subtitle">{age(row.market_data_time)}</small></> },
    { key: 'underlying', label: 'Underlying', render: row => <><button type="button" className="options-ticker-link" onClick={() => onTicker(row.underlying)}>{row.underlying}</button><small className="options-cell-subtitle">Latest matrix</small></> },
    { key: 'strategy', label: 'Strategy', render: row => <>{row.display_name}<small className="options-cell-subtitle">Rank #{row.candidate_rank}</small></> },
    { key: 'structure', label: 'Structure', render: row => <>{readable(row.structure_type)}<small className="options-cell-subtitle">{readable(row.structure_risk_class)}</small></> },
    { key: 'legs', label: 'Ordered legs', className: 'options-leg-cell', render: row => <div className="options-leg-lines">{row.legs.map(leg => <span key={leg.leg_index} title={leg.contract_ticker}>{leg.side === 'SELL' ? 'Sell' : 'Buy'} {leg.ratio} {money(leg.strike)} {leg.contract_type === 'CALL' ? 'Call' : 'Put'}</span>)}</div> },
    { key: 'expiration', label: 'Expiration', render: row => row.expiration_date || 'Unavailable' },
    { key: 'net_premium', label: 'Net premium', render: row => <div className="options-performance-result"><strong>{money(row.net_premium)}</strong><div className="options-leg-detail">{row.legs.map(leg => <span key={leg.leg_index}>{money(leg.model_mark)}</span>)}</div></div> },
    { key: 'maximum_loss', label: 'Maximum loss', className: 'options-risk-cell', render: row => money(row.maximum_loss) },
    { key: 'modeled_return', label: 'Modeled return', render: row => { const metric = row.return_on_risk ?? row.return_on_collateral; return <>{metric == null ? 'Unavailable' : pct(metric)}<small className="options-cell-subtitle">{row.return_on_collateral != null ? 'On collateral' : 'On risk'}</small></> } },
    { key: 'stock_price', label: 'Stock price', render: row => <LegStockPrice legs={row.legs} /> },
    { key: 'day_volume', label: 'Volume', render: row => <LegLiquidity legs={row.legs} field="day_volume" /> },
    { key: 'open_interest', label: 'Open interest', render: row => <LegLiquidity legs={row.legs} field="open_interest" /> },
    ...greekColumns.map(column => ({ key: column.key, label: column.label, render: (row: OptionOpportunityRow) => <LegGreek legs={row.legs} column={column} /> })),
    { key: 'window', label: 'Window', render: row => <Status value={`WINDOW ${row.window_state}`} /> },
    { key: 'signal', label: 'Signal', render: row => <Status value={row.signal_status || 'RESEARCH ONLY'} /> },
    { key: 'gates', label: 'Research gates', render: row => { const reasons = candidateReasons(row); return <>{reasons.length ? readable(reasons[0]) : 'None'}{reasons.length > 1 && <small className="options-cell-subtitle">+{reasons.length - 1} more</small>}</> } },
    { key: 'details', label: '', fixed: true, render: row => <button type="button" className="options-icon-button" title="Review opportunity evidence" aria-label={`Review opportunity evidence ${row.candidate_id}`} onClick={() => onSelect(row.candidate_id)}><Eye size={15} /></button> },
  ]
}

function PriorBoardSignals({ history, loading, error, onOpenHistory, onTicker }: {
  history?: OptionsEnvelope<OptionPerformanceData>
  loading: boolean
  error: boolean
  onOpenHistory: () => void
  onTicker: (ticker: string) => void
}) {
  const data = history?.data
  const previous = data?.rows || []
  const previousTotal = data?.total || 0
  const latestRead = (row: OptionPerformanceRow) => {
    const measured = [...row.checkpoints].reverse().find(item => item.outcome)
    if (measured?.outcome) return `${checkpointLabels[measured.measurement_type]} ${signedMoney(measured.outcome.net_pnl)} / ${pct(Number(measured.outcome.net_return))}`
    const pending = row.checkpoints.find(item => item.status === 'PENDING')
    if (pending) return `${checkpointLabels[pending.measurement_type]} pending`
    const next = row.checkpoints.find(item => item.status === 'NOT_DUE')
    return next ? `${checkpointLabels[next.measurement_type]} not due` : 'No checkpoint'
  }
  return <section className="options-panel options-prior-board"><div className="options-panel__header"><div><h3>Recent raw rank-leading signals</h3><p>Lowest raw candidate rank within each matrix and strategy. This is not reconstructed Opportunity Board history.</p></div><div className="options-panel-tools"><span>{previousTotal} signals</span><button type="button" onClick={onOpenHistory}><History size={14} />Open performance</button></div></div>{loading ? <div className="options-empty-block">Loading rank-leading signals...</div> : error ? <div className="options-empty-block options-history-error">Rank-leading signal history is temporarily unavailable. Current structures remain unaffected.</div> : previous.length ? <div className="options-table-wrap"><table className="options-table"><thead><tr><th>Source time</th><th>Underlying</th><th>Strategy</th><th>Structure</th><th>Status</th><th>Premium stop / target</th><th>Latest checkpoint</th><th>Reason</th></tr></thead><tbody>{previous.slice(0, 12).map(row => <tr key={row.event_id}><td>{dateTime(row.market_data_time)}<small className="options-cell-subtitle">{age(row.market_data_time)}</small></td><td><button type="button" className="options-ticker-link" onClick={() => onTicker(row.underlying)}>{row.underlying}</button></td><td>{readable(row.strategy_name)}</td><td>{readable(row.structure_type)}</td><td><Status value={row.status} /></td><td>{row.stop_loss == null || row.take_profit == null ? 'Not defined' : `${money(row.stop_loss)} / ${money(row.take_profit)}`}</td><td>{latestRead(row)}</td><td>{row.blocked_reasons.length ? readable(row.blocked_reasons[0]) : 'None'}</td></tr>)}</tbody></table></div> : <div className="options-empty-block">No raw rank-leading signals are in this window yet.</div>}</section>
}

function OpportunityBoard({ envelope, history, historyLoading, historyError, strategy, onSelect, onOpenHistory, onTicker }: {
  envelope?: OptionsEnvelope<OptionOpportunitiesData>
  history?: OptionsEnvelope<OptionPerformanceData>
  historyLoading: boolean
  historyError: boolean
  strategy: string
  onSelect: (candidateId: string) => void
  onOpenHistory: () => void
  onTicker: (ticker: string) => void
}) {
  if (!envelope?.available) return <StatePanel title={envelope?.reason === 'NO_COMPLETE_BOARD_PUBLICATION' ? 'Awaiting first immutable Board publication' : 'No current opportunity slate'} detail={envelope?.reason === 'NO_COMPLETE_BOARD_PUBLICATION' ? 'The Board now requires one complete 13-underlying publication. Restart the option worker onto the new publisher; broad detections remain available in Options Screener.' : 'The latest matrices have not produced persisted strategy decisions yet. Review Operations for ingestion or quality gates.'} warning />
  const data = envelope.data
  const latestStructured = data.structured.filter(row => strategy === 'ALL' || row.strategy_name === strategy).sort((left, right) => Number(right.window_state === 'ACTIVE') - Number(left.window_state === 'ACTIVE') || left.underlying.localeCompare(right.underlying) || left.strategy_name.localeCompare(right.strategy_name))
  const activeWindows = latestStructured.filter(row => row.window_state === 'ACTIVE').length
  const blocked = latestStructured.filter(row => row.signal_status === 'BLOCKED').length
  const coverageExceptions = Math.max(0, data.configured_underlyer_count - data.covered_underlyer_count)
  const priorExcluded = data.selection_evidence?.prior_contract_excluded_candidates || 0
  const [hiddenBoardColumns, setHiddenBoardColumns] = useState<Set<string>>(new Set([...greekColumns.map(column => column.key), 'stock_price', 'day_volume', 'open_interest']))
  const toggleBoardColumn = (key: string) => setHiddenBoardColumns(current => { const next = new Set(current); if (next.has(key)) next.delete(key); else next.add(key); return next })
  return <>
    {data.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && <StatePanel title="Historical policy publication" detail="The current valuation policy has not completed a full universe cycle yet. This immutable Board is the latest prior-policy publication and remains research-only." warning />}
    {coverageExceptions > 0 && <StatePanel title="Universe coverage is incomplete" detail={`${coverageExceptions} configured underlyer${coverageExceptions === 1 ? '' : 's'} lack a current matrix. Inspect Operations before relying on the slate.`} warning />}
    <section className="options-panel options-opportunity-section"><div className="options-panel__header"><div><h3>Latest published structures</h3><p>Atomic complete-universe revision from {dateTime(data.published_at)}. Positions are reranked after prior-contract exclusion; ranks are not comparable across strategies.</p></div><div className="options-board-summary"><span><strong>{latestStructured.length}</strong> surfaced</span><span><strong>{activeWindows}</strong> active</span><span><strong>{blocked}</strong> blocked</span><span><strong>{priorExcluded}</strong> prior-contract exclusions</span><span className={coverageExceptions ? 'has-exception' : ''}><strong>{coverageExceptions}</strong> coverage gaps</span><ColumnSettings columns={opportunityColumns(onSelect, onTicker)} hidden={hiddenBoardColumns} onToggle={toggleBoardColumn} /></div></div>{latestStructured.length ? <LatestOpportunityTable rows={latestStructured} hidden={hiddenBoardColumns} onSelect={onSelect} onTicker={onTicker} /> : <div className="options-empty-block">This publication contains no structured survivors. Detector findings and the selector funnel remain auditable.</div>}</section>
    <PriorBoardSignals history={history} loading={historyLoading} error={historyError} onOpenHistory={onOpenHistory} onTicker={onTicker} />
  </>
}

function CandidateCommandBar({ members, underlyer, persona, status, onUnderlyer, onPersona, onStatus }: {
  members: OptionUniverseRow[]
  underlyer: string
  persona: OptionCandidatePersona
  status: OptionCandidateStatus | 'ALL'
  onUnderlyer: (value: string) => void
  onPersona: (value: OptionCandidatePersona) => void
  onStatus: (value: OptionCandidateStatus | 'ALL') => void
}) {
  return <section className="options-candidate-command" aria-label="Strategy candidate filters">
    <label>Underlying<select value={underlyer} onChange={event => onUnderlyer(event.target.value)}><option value="ALL">All underlyings</option>{members.map(row => <option key={row.ticker} value={row.ticker}>{row.ticker}</option>)}</select></label>
    <div className="options-personas"><span>Opportunity suite</span><div>{candidateSuites.map(item => <button type="button" key={item.value} className={persona === item.value ? 'active' : ''} onClick={() => onPersona(item.value)}>{item.label}</button>)}</div></div>
    <label>Candidate status<select value={status} onChange={event => onStatus(event.target.value as OptionCandidateStatus | 'ALL')}><option value="ALL">All decisions</option><option value="SELECTED">Selected</option><option value="SUPPRESSED">Suppressed</option><option value="REJECTED">Rejected</option></select></label>
    <div className="options-capability"><LockKeyhole size={14} /><span>Delayed research only / no order entry</span></div>
  </section>
}

function CandidateWorkbench({ envelope, persona, status, onOffsetChange, onSelect }: { envelope?: OptionsEnvelope<OptionCandidatesData>; persona: OptionCandidatePersona; status: OptionCandidateStatus | 'ALL'; onOffsetChange: (offset: number) => void; onSelect: (candidateId: string) => void }) {
  const suite = candidateSuites.find(item => item.value === persona)!
  const rows = envelope?.data?.rows || []
  const counts = envelope?.data?.status_counts || { selected: 0, suppressed: 0, rejected: 0 }
  const firstRow = rows.length && envelope?.data ? envelope.data.offset + 1 : 0
  const lastRow = envelope?.data ? envelope.data.offset + rows.length : 0
  return <>
    {envelope?.data?.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && <StatePanel title="Historical policy candidates" detail="The active valuation policy has not produced a complete candidate cohort yet. These decisions are the latest prior-policy research results." warning />}
    <section className="options-candidate-intro"><div><span>Phase 2 strategy evidence</span><h2>{suite.label}</h2><p>{suite.detail}</p></div><div><strong>Weekly Research Candidates</strong><span>Future expiration cohort, not a next-week forecast</span></div></section>
    <section className="options-metrics-grid"><Metric label="Matching decisions" value={integer(envelope?.data?.total || 0)} detail="Current status page" /><Metric label="Selected research" value={integer(counts.selected)} detail="All matching underlyings" /><Metric label="Suppressed" value={integer(counts.suppressed)} detail="Inspectable failed gates" /><Metric label="Quote liquidity" value="Unavailable" detail="Developer entitlement" /></section>
    {!envelope?.available && status === 'SELECTED' && <StatePanel title="No selected recommendations" detail={`${counts.suppressed} matching decisions were suppressed by backend quality or context gates. Choose Suppressed or All decisions to inspect them.`} warning />}
    {!envelope?.available && status !== 'SELECTED' && <StatePanel title="No matching strategy decisions" detail="No persisted backend decisions match the current suite and filters." warning />}
    <section className="options-panel"><div className="options-panel__header"><div><h3>{suite.label} decisions</h3><p>Stable backend rank and explanations; the browser does not infer candidates.</p></div><span>{rows.length} shown</span></div><div className="options-table-wrap"><table className="options-table options-table--candidates"><thead><tr><th>Status</th><th>Underlying</th><th>Strategy</th><th>Decision type</th><th>Contract / Structure</th><th>Expiration</th><th>Ordered legs</th><th>Net premium</th><th>Maximum loss</th><th>Return on risk</th><th>Source time</th><th>Primary evidence</th><th>Reason / boundary</th><th aria-label="Details" /></tr></thead><tbody>{rows.length ? rows.map(row => <CandidateRow key={row.candidate_id} row={row} onSelect={onSelect} />) : <tr><td colSpan={14} className="options-empty-cell">No persisted decisions match this suite and filter.</td></tr>}</tbody></table></div>{envelope?.data && <div className="options-pagination"><span>{envelope.data.total ? `Showing ${firstRow}-${lastRow} of ${envelope.data.total}` : '0 results'}</span><div><button type="button" aria-label="Previous candidate page" title="Previous page" disabled={envelope.data.offset === 0} onClick={() => onOffsetChange(Math.max(0, envelope.data.offset - envelope.data.limit))}><ChevronLeft size={16} /></button><button type="button" aria-label="Next candidate page" title="Next page" disabled={lastRow >= envelope.data.total} onClick={() => onOffsetChange(envelope.data.offset + envelope.data.limit)}><ChevronRight size={16} /></button></div></div>}</section>
  </>
}

function RecommendationCommandBar({ members, underlyer, status, onUnderlyer, onStatus }: {
  members: OptionUniverseRow[]
  underlyer: string
  status: OptionSignalStatus | 'ALL'
  onUnderlyer: (value: string) => void
  onStatus: (value: OptionSignalStatus | 'ALL') => void
}) {
  return <section className="options-recommendation-command" aria-label="Recommendation filters">
    <label>Underlying<select value={underlyer} onChange={event => onUnderlyer(event.target.value)}><option value="ALL">All underlyings</option>{members.map(row => <option key={row.ticker} value={row.ticker}>{row.ticker}</option>)}</select></label>
    <label>Recommendation status<select value={status} onChange={event => onStatus(event.target.value as OptionSignalStatus | 'ALL')}><option value="ALL">All statuses</option><option value="PENDING">Pending</option><option value="READY">Ready</option><option value="BLOCKED">Blocked</option><option value="EXPIRED">Expired</option></select></label>
    <div className="options-capability"><LockKeyhole size={14} /><span>Backend-published only / no order entry</span></div>
  </section>
}

function RecommendationWorkbench({ envelope, onOffsetChange }: {
  envelope?: OptionsEnvelope<OptionSignalsData>
  onOffsetChange: (offset: number) => void
}) {
  const rows = envelope?.data?.rows || []
  const counts = envelope?.data?.status_counts || { pending: 0, ready: 0, blocked: 0, expired: 0 }
  const firstRow = rows.length && envelope?.data ? envelope.data.offset + 1 : 0
  const lastRow = envelope?.data ? envelope.data.offset + rows.length : 0
  return <>
    <section className="options-candidate-intro"><div><span>Durable recommendation ledger</span><h2>Published option recommendations</h2><p>Backend-selected structures with immutable legs, payoff context, validity, and gate status.</p></div><div><strong>No browser inference</strong><span>Rows originate only from option_signal_events</span></div></section>
    <section className="options-metrics-grid"><Metric label="Matching recommendations" value={integer(envelope?.data?.total || 0)} detail="Current filter" /><Metric label="Ready" value={integer(counts.ready)} detail="Requires non-null eligibility" /><Metric label="Blocked" value={integer(counts.blocked)} detail="Inspectable backend gates" /><Metric label="Expired" value={integer(counts.expired)} detail="Retained audit records" /></section>
    {!envelope?.available && <StatePanel title="No published option recommendations" detail="No structured candidate has passed market-data, pricing, context, and strategy gates. Inspect suppressed decisions in Strategy Workbench." warning />}
    <section className="options-panel"><div className="options-panel__header"><div><h3>Recommendation events</h3><p>Read-only standardized signals persisted atomically with their candidate legs.</p></div><span>{rows.length} shown</span></div><div className="options-table-wrap"><table className="options-table options-table--recommendations"><thead><tr><th>Status</th><th>Underlying</th><th>Strategy</th><th>Action</th><th>Structure</th><th>Ordered legs</th><th>Net premium</th><th>Stop</th><th>Take profit</th><th>Maximum loss</th><th>Return on risk</th><th>Valid through</th><th>Data quality</th><th>Eligibility</th><th>Blocked reasons</th></tr></thead><tbody>{rows.length ? rows.map(row => <tr key={row.event_id}><td><Status value={row.status} /></td><td><strong>{row.underlying}</strong></td><td>{readable(row.strategy_name)}<small className="options-cell-subtitle">{readable(row.strategy_version)}</small></td><td>{row.action}</td><td>{readable(row.structure_type)}</td><td className="options-leg-cell">{row.legs.map(leg => `${leg.action} ${leg.ratio} ${leg.contract_ticker}`).join(' / ')}</td><td>{money(row.net_premium)}</td><td>{money(row.stop_loss)}</td><td>{money(row.take_profit)}</td><td className="options-risk-cell">{money(row.maximum_loss)}</td><td>{row.return_on_risk == null ? 'Unavailable' : pct(row.return_on_risk)}</td><td>{dateTime(row.valid_until)}<small className="options-cell-subtitle"><Status value={new Date(row.valid_until).getTime() > Date.now() ? 'WINDOW ACTIVE' : 'WINDOW ELAPSED'} /></small></td><td>{readable(row.data_quality)}</td><td>{row.execution_eligibility ? readable(row.execution_eligibility) : 'Not eligible'}</td><td>{row.blocked_reasons.length ? <>{readable(row.blocked_reasons[0])}{row.blocked_reasons.length > 1 && <small className="options-cell-subtitle">+{row.blocked_reasons.length - 1} more gates</small>}</> : 'None'}</td></tr>) : <tr><td colSpan={15} className="options-empty-cell">No recommendation events match this filter.</td></tr>}</tbody></table></div>{envelope?.data && <div className="options-pagination"><span>{envelope.data.total ? `Showing ${firstRow}-${lastRow} of ${envelope.data.total}` : '0 results'}</span><div><button type="button" aria-label="Previous recommendation page" title="Previous page" disabled={envelope.data.offset === 0} onClick={() => onOffsetChange(Math.max(0, envelope.data.offset - envelope.data.limit))}><ChevronLeft size={16} /></button><button type="button" aria-label="Next recommendation page" title="Next page" disabled={lastRow >= envelope.data.total} onClick={() => onOffsetChange(envelope.data.offset + envelope.data.limit)}><ChevronRight size={16} /></button></div></div>}</section>
  </>
}

function PerformanceCommandBar({ members, strategies, underlyer, strategy, expiration, onUnderlyer, onStrategy, onExpiration }: {
  members: OptionUniverseRow[]
  strategies: Array<{ value: string; label: string }>
  underlyer: string
  strategy: string
  expiration: string
  onUnderlyer: (value: string) => void
  onStrategy: (value: string) => void
  onExpiration: (value: string) => void
}) {
  return <section className="options-recommendation-command options-performance-command" aria-label="Signal performance filters">
    <label>Underlying<select value={underlyer} onChange={event => onUnderlyer(event.target.value)}><option value="ALL">All underlyings</option>{members.map(row => <option key={row.ticker} value={row.ticker}>{row.ticker}</option>)}</select></label>
    <label>Strategy<select value={strategy} onChange={event => onStrategy(event.target.value)}><option value="ALL">All strategies</option>{strategies.map(row => <option key={row.value} value={row.value}>{row.label}</option>)}</select></label>
    <label>Expiration<input type="date" value={expiration} onChange={event => onExpiration(event.target.value)} /></label>
    <div className="options-capability"><History size={14} /><span>Delayed proxy marks / no fill claim</span></div>
  </section>
}

const checkpointLabels: Record<OptionPerformanceMeasurement, string> = {
  '15MIN': '15m', '30MIN': '30m', '60MIN': '60m', 'CLOSE': 'Close', 'NEXT_OPEN': 'Next open',
}

function PerformanceCurrentMark({ mark }: { mark: OptionPerformanceRow['current_mark'] }) {
  if (!mark) return <span className="options-performance-pending">Pending</span>
  const grossPnl = Number(mark.gross_pnl)
  return <div className={`options-performance-result ${grossPnl >= 0 ? 'options-performance-result--positive' : 'options-performance-result--negative'}`}><strong>{signedMoney(mark.gross_pnl)}</strong><small>Net {signedMoney(mark.net_pnl)} after {money(mark.estimated_cost)} costs</small><small>{dateTime(mark.market_time)}</small><div className="options-leg-detail">{mark.legs.map(leg => <span key={leg.contract_id}>{leg.side === 'SELL' ? 'Sell' : 'Buy'} {leg.ratio} · {money(leg.entry_mark)} → {money(leg.current_mark)} · {signedMoney(leg.gross_pnl)}</span>)}</div></div>
}

// Package premium is positive for a net credit and negative for a net debit.
function PerformancePackagePremium({ mark, phase }: { mark: OptionPerformanceRow['current_mark']; phase: 'entry' | 'current' }) {
  if (!mark) return <span className="options-performance-pending">Pending</span>
  const premium = Number(phase === 'entry' ? mark.entry_net_premium : mark.exit_net_premium)
  return <div className="options-performance-result"><strong>{premium >= 0 ? `Credit ${money(Math.abs(premium))}` : `Debit ${money(Math.abs(premium))}`}</strong><div className="options-leg-detail">{mark.legs.map(leg => <span key={leg.contract_id}>{money(phase === 'entry' ? leg.entry_mark : leg.current_mark)}</span>)}</div></div>
}

type GreekKey = 'local_iv' | 'local_delta' | 'local_gamma' | 'local_theta_per_day' | 'local_vega_per_vol_point' | 'local_rho_per_rate_point'
type LegLike = { leg_index: number; side: 'BUY' | 'SELL'; ratio: number; spot?: string | null; day_volume?: number | null } & { [K in GreekKey]?: number | null } & { open_interest?: number | null }

// IV is ratio-weighted; the rest are signed position sums so short legs offset long legs.
const greekColumns: { key: GreekKey; label: string; weighted?: boolean }[] = [
  { key: 'local_iv', label: 'IV', weighted: true },
  { key: 'local_delta', label: 'Delta' },
  { key: 'local_gamma', label: 'Gamma' },
  { key: 'local_theta_per_day', label: 'Theta / day' },
  { key: 'local_vega_per_vol_point', label: 'Vega' },
  { key: 'local_rho_per_rate_point', label: 'Rho' },
]

function packageGreek(legs: LegLike[], key: GreekKey, weighted?: boolean) {
  const usable = legs.filter(leg => leg[key] != null)
  if (!usable.length) return null
  if (weighted) {
    const weight = usable.reduce((total, leg) => total + leg.ratio, 0)
    return weight ? usable.reduce((total, leg) => total + Number(leg[key]) * leg.ratio, 0) / weight : null
  }
  return usable.reduce((total, leg) => total + (leg.side === 'BUY' ? 1 : -1) * leg.ratio * Number(leg[key]), 0)
}

function LegGreek({ legs, column }: { legs: LegLike[]; column: typeof greekColumns[number] }) {
  const value = packageGreek(legs, column.key, column.weighted)
  if (value == null) return <span className="options-performance-pending">Unavailable</span>
  return <div className="options-performance-result"><strong>{column.weighted ? pct(value) : value.toFixed(4)}</strong><div className="options-leg-detail">{legs.map(leg => <span key={leg.leg_index}>{leg[column.key] == null ? '—' : column.weighted ? pct(Number(leg[column.key])) : Number(leg[column.key]).toFixed(4)}</span>)}</div></div>
}

// All legs share one underlying, so the first leg carries the package spot.
function LegStockPrice({ legs, currentLegs }: { legs: LegLike[]; currentLegs?: { spot?: string | null }[] }) {
  const detection = legs.find(leg => leg.spot != null)?.spot
  const current = currentLegs?.find(leg => leg.spot != null)?.spot
  if (detection == null) return <span className="options-performance-pending">Unavailable</span>
  return <div className="options-performance-result"><strong>{money(current ?? detection)}</strong><small>{current == null ? 'At detection' : `Detected ${money(detection)}`}</small></div>
}

// One value per leg, ordered to match the legs column.
function LegLiquidity({ legs, field }: { legs: LegLike[]; field: 'day_volume' | 'open_interest' }) {
  if (!legs.length) return <span className="options-performance-pending">Unavailable</span>
  return <div className="options-leg-lines">{legs.map(leg => <span key={leg.leg_index}>{leg[field] == null ? 'Unavailable' : integer(leg[field]!)}</span>)}</div>
}

function PerformanceLegs({ row }: { row: OptionPerformanceRow }) {  if (!row.legs.length) return <span className="options-performance-pending">No legs</span>
  return <div className="options-leg-lines">{row.legs.map(leg => <span key={leg.leg_index} title={leg.contract_ticker}>{leg.side === 'SELL' ? 'Sell' : 'Buy'} {leg.ratio} {money(leg.strike)} {leg.contract_type === 'CALL' ? 'Call' : 'Put'}</span>)}</div>
}

function performanceColumns(): TableColumn<OptionPerformanceRow>[] {
  return [
    { key: 'signal_time', label: 'Signal time', render: row => <>{dateTime(row.market_data_time)}<small className="options-cell-subtitle">Valid to {dateTime(row.valid_until)}</small></> },
    { key: 'underlying', label: 'Underlying', className: 'options-symbol', render: row => row.underlying },
    { key: 'strategy', label: 'Strategy', render: row => <>{readable(row.strategy_name)}<small className="options-cell-subtitle">{readable(row.strategy_version)}</small></> },
    { key: 'status', label: 'Status', render: row => <Status value={row.status} /> },
    { key: 'structure', label: 'Structure', render: row => readable(row.structure_type) },
    { key: 'legs', label: 'Ordered legs', render: row => <PerformanceLegs row={row} /> },
    { key: 'expiration', label: 'Expiration', render: row => row.expiration_date || 'No expiration' },
    { key: 'stop_target', label: 'Premium stop / target', render: row => <>{row.stop_loss == null || row.take_profit == null ? 'Not defined' : `${money(row.stop_loss)} / ${money(row.take_profit)}`}<small className="options-cell-subtitle">Option premium levels</small></> },
    { key: 'detection_price', label: 'Detection price', render: row => <PerformancePackagePremium mark={row.current_mark} phase="entry" /> },
    { key: 'current_price', label: 'Current price', render: row => <PerformancePackagePremium mark={row.current_mark} phase="current" /> },
    { key: 'current_pnl', label: 'Current P&L', render: row => <PerformanceCurrentMark mark={row.current_mark} /> },
    { key: 'stock_price', label: 'Stock price', render: row => <LegStockPrice legs={row.legs} currentLegs={row.current_mark?.legs} /> },
    { key: 'day_volume', label: 'Volume', render: row => <LegLiquidity legs={row.legs} field="day_volume" /> },
    { key: 'open_interest', label: 'Open interest', render: row => <LegLiquidity legs={row.legs} field="open_interest" /> },
    ...greekColumns.map(column => ({ key: column.key, label: column.label, render: (row: OptionPerformanceRow) => <LegGreek legs={row.legs} column={column} /> })),
    { key: 'blocked', label: 'Blocked reason', render: row => <>{row.blocked_reasons.length ? readable(row.blocked_reasons[0]) : 'None'}{row.blocked_reasons.length > 1 && <small className="options-cell-subtitle">+{row.blocked_reasons.length - 1} more</small>}</> },
  ]
}

function PerformanceWorkbench({ envelope, onOffsetChange }: {
  envelope?: OptionsEnvelope<OptionPerformanceData>
  onOffsetChange: (offset: number) => void
}) {
  const data = envelope?.data
  const rows = data?.rows || []
  const firstRow = rows.length && data ? data.offset + 1 : 0
  const lastRow = data ? data.offset + rows.length : 0
  const columns = performanceColumns()
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set([...greekColumns.map(column => column.key), 'stock_price', 'day_volume', 'open_interest']))
  const toggleColumn = (key: string) => setHiddenColumns(current => { const next = new Set(current); if (next.has(key)) next.delete(key); else next.add(key); return next })
  const visible = columns.filter(column => !hiddenColumns.has(column.key))
  return <>
    <section className="options-candidate-intro"><div><span>Delayed proxy tracking</span><h2>Signal performance</h2><p>Fixed checkpoints are immutable; current simulated-close P&amp;L is refreshed by the worker from a coherent delayed package. Opening this view only reads persisted values.</p></div><div><strong>{data?.cohort === 'BOARD_PUBLICATIONS' ? 'Published Opportunity Board' : data?.cohort === 'ALL_SIGNALS' ? 'All structured signals' : 'Raw rank-leading signals'}</strong><span>{data?.days || 14}-day history · {data?.board_membership_exact ? 'exact prospective membership' : 'not exact Board membership'}</span></div></section>
    {!envelope?.available && <StatePanel title={data?.cohort === 'BOARD_PUBLICATIONS' ? 'No published Board performance yet' : 'No signal history in this window'} detail={data?.cohort === 'BOARD_PUBLICATIONS' ? 'Exact membership begins prospectively with the first immutable Board publication; earlier raw rank leaders are not relabeled.' : 'Continuous option collection must observe selected structures before checkpoint tracking can begin.'} warning />}
    {envelope?.available && data?.measurement_count === 0 && <StatePanel title="Checkpoint outcomes are pending" detail="Signals are retained, but later coherent observations for their exact legs have not matured yet." />}
    <section className="options-panel"><div className="options-panel__header"><div><h3>Signal ledger</h3><p>Detection and current package prices come from worker-materialized delayed marks.</p></div><div className="options-panel-tools"><span>{rows.length} shown</span><ColumnSettings columns={columns} hidden={hiddenColumns} onToggle={toggleColumn} /></div></div><div className="options-table-wrap"><table className="options-table options-table--performance"><thead><tr>{visible.map(column => <th key={column.key}>{column.label}</th>)}</tr></thead><tbody>{rows.length ? rows.map(row => <tr key={row.event_id}>{visible.map(column => <td key={column.key} className={column.className}>{column.render(row)}</td>)}</tr>) : <tr><td colSpan={visible.length} className="options-empty-cell">No retained structured signals match this history window.</td></tr>}</tbody></table></div>{data && <div className="options-pagination"><span>{data.total ? `Showing ${firstRow}-${lastRow} of ${data.total}` : '0 results'}</span><div><button type="button" aria-label="Previous performance page" title="Previous page" disabled={data.offset === 0} onClick={() => onOffsetChange(Math.max(0, data.offset - data.limit))}><ChevronLeft size={16} /></button><button type="button" aria-label="Next performance page" title="Next page" disabled={lastRow >= data.total} onClick={() => onOffsetChange(data.offset + data.limit)}><ChevronRight size={16} /></button></div></div>}</section>
  </>
}

function CandidateRow({ row, onSelect }: { row: OptionCandidateRow; onSelect: (candidateId: string) => void }) {
  const researchOnly = row.candidate_kind === 'RESEARCH_ONLY'
  const decisionType = researchOnly ? (row.status === 'SELECTED' ? 'Research finding' : 'Suppression record') : row.candidate_kind === 'MULTI_LEG' ? 'Multi-leg package' : 'Single-contract package'
  const contractOrStructure = researchOnly && row.status === 'SELECTED'
    ? row.source_contract_ticker || 'Source contract unavailable'
    : readable(row.structure_type)
  const legs = row.legs.length ? row.legs.map(leg => `${leg.side} ${leg.ratio} ${leg.contract_ticker}`).join(' / ') : researchOnly && row.status === 'SELECTED' ? 'No legs by design' : 'No package; gates not passed'
  const evidence = row.primary_metric_name && row.primary_metric_value != null ? `${readable(row.primary_metric_name)} ${number(row.primary_metric_value)}` : 'Unavailable'
  return <tr><td><Status value={row.status} /></td><td className="options-symbol">{row.underlying}</td><td><strong>{row.display_name}</strong><small className="options-cell-subtitle">{readable(row.strategy_version)}</small></td><td><span className={`options-kind-label ${researchOnly ? 'options-kind-label--research' : ''}`}>{decisionType}</span></td><td className={researchOnly && row.status === 'SELECTED' ? 'options-contract-id' : ''}>{contractOrStructure}</td><td>{row.expiration_date || 'Unavailable'}</td><td className="options-leg-cell">{legs}</td><td>{money(row.net_premium)}</td><td className="options-risk-cell">{money(row.maximum_loss)}</td><td>{row.return_on_risk == null ? 'Unavailable' : pct(row.return_on_risk)}</td><td>{dateTime(row.market_data_time)}</td><td>{evidence}</td><td>{row.reason_codes.length ? <>{readable(row.reason_codes[0])}{row.reason_codes.length > 1 && <small className="options-cell-subtitle">+{row.reason_codes.length - 1} more</small>}</> : 'None'}</td><td><button type="button" className="options-icon-button" title="Open candidate evidence" aria-label={`Open candidate ${row.candidate_id}`} onClick={() => onSelect(row.candidate_id)}><Eye size={15} /></button></td></tr>
}

function CandidateDrawer({ envelope, loading, error, onClose }: { envelope?: OptionsEnvelope<OptionCandidateDetailData>; loading: boolean; error: boolean; onClose: () => void }) {
  const data = envelope?.data
  const candidate = data?.candidate
  const researchOnly = candidate?.candidate_kind === 'RESEARCH_ONLY'
  const gates = data?.execution_gates || []
  const quoteGate = gates.find(gate => gate.gate_name === 'QUOTE_LIQUIDITY')
  return <div className="options-drawer-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}><aside className="options-drawer options-drawer--candidate" role="dialog" aria-modal="true" aria-labelledby="candidate-drawer-title"><header><div><span>{researchOnly ? 'Research-only detector finding' : 'Structured research candidate'}</span><h2 id="candidate-drawer-title">{loading ? 'Loading evidence' : candidate?.display_name || 'Candidate unavailable'}</h2><p>{researchOnly ? 'Selected evidence record; no directional package or execution claim.' : 'Delayed research record. Candidate state is not broker authorization.'}</p></div><button type="button" className="options-icon-button" title="Close candidate" aria-label="Close candidate drawer" autoFocus onClick={onClose} onKeyDown={event => { if (event.key === 'Escape') onClose() }}><PanelRightClose size={18} /></button></header>{loading && <StatePanel title="Loading candidate evidence" detail="Reading immutable decision and scenario records." />}{error && <StatePanel title="Candidate evidence unavailable" detail="The detail API could not load this persisted decision." warning />}{!loading && candidate && data && <div className="options-drawer__body">
    <section><h3>{researchOnly ? 'Finding' : 'Structure'}</h3><div className="options-field-grid"><Field label="Status" value={readable(candidate.status)} /><Field label="Decision type" value={researchOnly ? (candidate.status === 'SELECTED' ? 'Research finding' : 'Suppression record') : readable(candidate.candidate_kind)} />{!researchOnly && <Field label="Structure" value={readable(candidate.structure_type)} />}<Field label="Risk class" value={readable(candidate.structure_risk_class)} /><Field label="Source contract" value={candidate.source_contract_ticker || 'Not contract-specific'} /><Field label="Source contract ID" value={candidate.source_contract_id == null ? 'Unavailable' : String(candidate.source_contract_id)} /><Field label="Expiration" value={candidate.expiration_date || 'Unavailable'} /><Field label="Rank" value={String(candidate.candidate_rank)} /><Field label="Primary evidence" value={candidate.primary_metric_name && candidate.primary_metric_value != null ? `${readable(candidate.primary_metric_name)} ${number(candidate.primary_metric_value)}` : 'Unavailable'} /><Field label="Eligibility" value={candidate.execution_eligibility || 'Not eligible'} /></div>{data.legs.length ? <div className="options-leg-stack">{data.legs.map(leg => <div key={leg.leg_index}><span>{leg.side} {leg.ratio}</span><strong>{leg.contract_ticker}</strong><small>{leg.contract_type} {money(leg.strike)} / model {money(leg.model_mark)}</small></div>)}</div> : <p className="options-section-note">{researchOnly && candidate.status === 'SELECTED' ? 'This row is one contract-level detector result. The same detector selected different source contracts; they are not duplicate strategy executions. No option legs, direction, or recommendation event are constructed by design.' : 'No option package was created because the required quality or strategy gates did not pass.'}</p>}</section>
    <section><h3>Risk and payoff</h3><div className="options-field-grid"><Field label="Net premium" value={money(candidate.net_premium)} /><Field label="Maximum loss" value={money(candidate.maximum_loss)} /><Field label="Maximum profit" value={money(candidate.maximum_profit)} /><Field label="Capital at risk" value={money(candidate.capital_at_risk)} /><Field label="Return on risk" value={candidate.return_on_risk == null ? 'Unavailable' : pct(candidate.return_on_risk)} /><Field label="Breakevens" value={candidate.breakevens.length ? candidate.breakevens.map(value => money(value)).join(', ') : 'Unavailable'} /></div>{data.scenarios.length ? <div className="options-scenario-wrap"><table className="options-scenario-table"><thead><tr><th>Spot shock</th><th>IV shock</th><th>Time left</th><th>Repriced</th><th>P&amp;L</th></tr></thead><tbody>{data.scenarios.map(row => <tr key={row.scenario_result_id}><td>{pct(row.spot_shock_fraction)}</td><td>{pct(row.iv_shock_fraction)}</td><td>{pct(row.time_fraction_remaining)}</td><td>{money(row.repriced_value)}</td><td>{money(row.profit_loss)}</td></tr>)}</tbody></table></div> : <p className="options-section-note">{researchOnly ? 'Research-only findings do not define premium, payoff, breakevens, or scenario grids because no option structure is constructed.' : 'Scenario results are unavailable because no complete strategy package passed selection.'}</p>}</section>
    <section><h3>Trend and context</h3><div className="options-field-grid"><Field label="Context status" value={candidate.context_status ? readable(candidate.context_status) : 'Unavailable'} /><Field label="Trend state" value={candidate.trend_state || 'Unavailable'} /><Field label="Earnings blackout" value={candidate.earnings_blackout_state || 'Unavailable'} /><Field label="Fed blackout" value={candidate.fed_blackout_state || 'Unavailable'} /></div></section>
    <section><h3>Event calendar evidence</h3>{data.market_event_evidence.length || data.event_coverage_evidence.length ? <><div className="options-field-grid">{data.market_event_evidence.map(event => <Field key={event.market_event_id} label={`${readable(event.event_type)} event`} value={`${readable(event.status)} · ${dateTime(event.scheduled_time)}`} title={`${event.source} / ${event.source_key} · source observed ${dateTime(event.source_observed_at)} · received ${dateTime(event.first_observed_at)}`} />)}{data.event_coverage_evidence.map(coverage => <Field key={coverage.coverage_id} label={`${readable(coverage.event_type)} coverage`} value={`${dateTime(coverage.window_start)} to ${dateTime(coverage.window_end)}`} title={`${coverage.source} / ${coverage.source_key} · source observed ${dateTime(coverage.source_observed_at)} · received ${dateTime(coverage.first_observed_at)}`} />)}</div><p className="options-section-note">Event rows explain active or canceled calendar revisions. Coverage rows are the evidence that absence could safely mean clear.</p></> : <p className="options-section-note">Direct event-calendar lineage was not recorded for this earlier context. Its persisted blackout state remains unchanged.</p>}</section>
    <section><h3>Execution gates</h3>{gates.length ? <div className="options-field-grid">{gates.map(gate => <Field key={`${gate.ledger_version}:${gate.gate_name}`} label={readable(gate.gate_name)} value={`${readable(gate.verdict)}${gate.reason_codes.length ? ` · ${gate.reason_codes.map(readable).join(', ')}` : ''}`} title={`${gate.ledger_version} · evaluated ${dateTime(gate.evaluated_at)}`} />)}</div> : <p className="options-section-note">Gate verdicts were not recorded for this earlier candidate.</p>}<div className="options-field-grid"><Field label="Quote liquidity" value={quoteGate ? readable(quoteGate.verdict) : 'Not recorded'} /><Field label="Signal state" value={candidate.signal_status ? readable(candidate.signal_status) : 'No signal'} /><Field label="Execution mode" value={readable(data.execution_mode)} /></div></section>
    <section><h3>Management policy</h3>{Object.keys(candidate.management_policy).length ? <div className="options-field-grid">{Object.entries(candidate.management_policy).map(([key, value]) => <Field key={key} label={readable(key)} value={String(value)} />)}</div> : <p className="options-section-note">No management policy is attached to this suppressed or research-only record.</p>}</section>
    <section><h3>Decision evidence</h3><div className="options-field-grid"><Field label="Candidate ID" value={candidate.candidate_id} /><Field label="Matrix ID" value={candidate.matrix_id} /><Field label="Strategy version" value={candidate.strategy_version} /><Field label="Model version" value={candidate.model_version} /><Field label="Source market time" value={dateTime(candidate.market_data_time)} title={candidate.market_data_time} /><Field label="First observed" value={dateTime(candidate.observed_time)} title={candidate.observed_time} /></div>{candidate.reason_codes.length > 0 && <div className="options-reason-list">{candidate.reason_codes.map(reason => <span key={reason}>{readable(reason)}</span>)}</div>}</section>
  </div>}</aside></div>
}

function EvidenceBar({
  view,
  lens,
  members,
  underlyer,
  expiration,
  contractType,
  onUnderlyer,
  onLens,
  onExpiration,
  onContractType,
}: {
  view: 'research' | 'explorer'
  lens: ResearchLens
  members: OptionUniverseRow[]
  underlyer: string
  expiration: string
  contractType: ContractTypeFilter
  onUnderlyer: (value: string) => void
  onLens: (value: ResearchLens) => void
  onExpiration: (value: string) => void
  onContractType: (value: ContractTypeFilter) => void
}) {
  return <section className="options-command-bar" aria-label="Evidence filters">
    <label>Underlying<select value={underlyer} onChange={event => onUnderlyer(event.target.value)}>{members.map(row => <option key={row.ticker} value={row.ticker}>{row.ticker}</option>)}</select></label>
    {view === 'research' && <div className="options-lens-control"><span>Research lens</span><div>{lenses.map(item => { const Icon = item.icon; return <button type="button" key={item.value} className={lens === item.value ? 'active' : ''} onClick={() => onLens(item.value)}><Icon size={14} /><span>{item.label}</span></button> })}</div></div>}
    <label>Expiration<input type="date" value={expiration} onChange={event => onExpiration(event.target.value)} /></label>
    <label>Contract type<select value={contractType} onChange={event => onContractType(event.target.value as ContractTypeFilter)}><option value="ALL">{view === 'research' && lens === 'income' ? 'Puts (lens default)' : 'All listed'}</option><option value="CALL">Calls</option><option value="PUT">Puts</option></select></label>
    <div className="options-capability"><LockKeyhole size={14} /><span>Quotes unavailable; raw rows are not candidates</span></div>
  </section>
}

function LensHeader({ lens }: { lens: ResearchLens }) {
  const copy = lensCopy[lens]
  return <section className="options-lens-summary">
    <div><span>Capability-neutral lens</span><h2>{copy.title}</h2><p>{copy.detail}</p></div>
    <div className="options-boundary"><LockKeyhole size={16} /><span>{copy.unavailable}</span></div>
  </section>
}

function EvidenceStatus({ row }: { row: OptionChainRow }) {
  if (row.model_mark != null && row.iv_converged) return <Status value="MODEL VALID" />
  return <Status value="SOURCE OBSERVATION" />
}

function EvidenceTable({ lens, rows, onSelect }: { lens: ResearchLens; rows: OptionChainRow[]; onSelect: (row: OptionChainRow) => void }) {
  const empty = <tr><td colSpan={10} className="options-empty-cell">No retained observations match these evidence filters.</td></tr>
  if (lens === 'income') return <div className="options-table-wrap"><table className="options-table"><thead><tr><th>Contract</th><th>Expiry</th><th>DTE</th><th>Strike</th><th>Display mark</th><th>Local Delta</th><th>Volume</th><th>Open interest</th><th>Evidence state</th><th aria-label="Details" /></tr></thead><tbody>{rows.length ? rows.map(row => <EvidenceRow key={row.snapshot_id} row={row} cells={[row.contract_ticker, row.expiration_date, String(row.calendar_dte), money(row.strike), money(row.display_mark), number(row.local_delta), integer(row.day_volume), integer(row.open_interest)]} onSelect={onSelect} />) : empty}</tbody></table></div>
  if (lens === 'directional') return <div className="options-table-wrap"><table className="options-table"><thead><tr><th>Contract</th><th>Type</th><th>Expiry</th><th>Strike</th><th>Display mark</th><th>Model mark</th><th>Local Delta</th><th>Local Gamma</th><th>Evidence state</th><th aria-label="Details" /></tr></thead><tbody>{rows.length ? rows.map(row => <EvidenceRow key={row.snapshot_id} row={row} cells={[row.contract_ticker, row.contract_type, row.expiration_date, money(row.strike), money(row.display_mark), money(row.model_mark), number(row.local_delta), number(row.local_gamma)]} onSelect={onSelect} />) : empty}</tbody></table></div>
  if (lens === 'volatility') return <div className="options-table-wrap"><table className="options-table"><thead><tr><th>Contract</th><th>Expiry</th><th>Strike</th><th>Local IV</th><th>Local Vega</th><th>Theta / day</th><th>Model mark</th><th>IV solver</th><th>Evidence state</th><th aria-label="Details" /></tr></thead><tbody>{rows.length ? rows.map(row => <EvidenceRow key={row.snapshot_id} row={row} cells={[row.contract_ticker, row.expiration_date, money(row.strike), pct(row.local_iv), number(row.local_vega_per_vol_point), number(row.local_theta_per_day), money(row.model_mark), row.iv_solver || 'Unavailable']} onSelect={onSelect} />) : empty}</tbody></table></div>
  return <div className="options-table-wrap"><table className="options-table"><thead><tr><th>Contract</th><th>Expiry</th><th>Type</th><th>Strike</th><th>Volume</th><th>Open interest</th><th>Display mark</th><th>Mark source</th><th>Evidence state</th><th aria-label="Details" /></tr></thead><tbody>{rows.length ? rows.map(row => <EvidenceRow key={row.snapshot_id} row={row} cells={[row.contract_ticker, row.expiration_date, row.contract_type, money(row.strike), integer(row.day_volume), integer(row.open_interest), money(row.display_mark), readable(row.mark_source)]} onSelect={onSelect} />) : empty}</tbody></table></div>
}

function EvidenceRow({ row, cells, onSelect }: { row: OptionChainRow; cells: string[]; onSelect: (row: OptionChainRow) => void }) {
  return <tr className="options-evidence-row">{cells.map((cell, index) => <td key={index} className={index === 0 ? 'options-contract-id' : ''}>{cell}</td>)}<td><EvidenceStatus row={row} /></td><td><button type="button" className="options-icon-button" title="Open evidence" aria-label={`Open evidence for ${row.contract_ticker}`} onClick={() => onSelect(row)}><Eye size={15} /></button></td></tr>
}

function ExpirationEvidence({ envelope }: { envelope?: OptionsEnvelope<OptionAnalysisData> }) {
  if (!envelope?.available) return <StatePanel title="Expiration analysis unavailable" detail="No completed persisted analysis is available for this underlying." warning />
  const analysis = envelope.data.analysis
  const reasons = Array.isArray(analysis.quality_reasons) ? analysis.quality_reasons.map(String) : []
  return <>
    {String(analysis.status) !== 'COMPLETE' && <StatePanel title="Expiration surfaces blocked by model quality" detail={reasons.length ? reasons.map(readable).join(' · ') : 'The persisted analysis did not pass its quality policy.'} warning />}
    <section className="options-panel">
      <div className="options-panel__header"><div><h3>Expiration structure</h3><p>ATM IV, skew, activity, and OI concentration context only.</p></div><Status value={String(analysis.status || 'UNKNOWN')} /></div>
      <div className="options-table-wrap"><table className="options-table"><thead><tr><th>Expiry</th><th>ATM IV</th><th>25 Delta Call</th><th>25 Delta Put</th><th>Risk reversal</th><th>Call volume</th><th>Put volume</th><th>Call OI</th><th>Put OI</th><th>Breadth</th></tr></thead><tbody>{envelope.data.expirations.length ? envelope.data.expirations.map(row => <tr key={row.expiration_date}><td>{row.expiration_date}</td><td>{pct(row.atm_iv)}</td><td>{pct(row.call_25_delta_iv)}</td><td>{pct(row.put_25_delta_iv)}</td><td>{pct(row.risk_reversal_25_delta)}</td><td>{integer(row.call_volume)}</td><td>{integer(row.put_volume)}</td><td>{integer(row.call_open_interest)}</td><td>{integer(row.put_open_interest)}</td><td>{number(row.breadth)}</td></tr>) : <tr><td colSpan={10} className="options-empty-cell">No expiration surface passed the current model-quality policy.</td></tr>}</tbody></table></div>
    </section>
  </>
}

function GammaRegimeBadge({ regime }: { regime: string }) {
  const tone = regime === 'NEGATIVE_GAMMA' ? 'options-gamma-badge--negative' : regime === 'POSITIVE_GAMMA' ? 'options-gamma-badge--positive' : 'options-gamma-badge--unknown'
  const label = regime === 'NEGATIVE_GAMMA' ? 'Negative gamma' : regime === 'POSITIVE_GAMMA' ? 'Positive gamma' : 'Undetermined'
  return <span className={`options-gamma-badge ${tone}`}>{label}</span>
}

function GammaCurve({ rows, spot, flip }: { rows: OptionGammaStrike[]; spot: number; flip: number | null }) {
  const peak = Math.max(...rows.map(row => Math.max(row.call_gamma_notional_per_percent, row.put_gamma_notional_per_percent)), 1)
  return <div className="options-gamma-curve" role="img" aria-label="Per-strike call and put gamma exposure">
    {rows.map(row => {
      const strike = Number(row.strike)
      const call = (row.call_gamma_notional_per_percent / peak) * 100
      const put = (row.put_gamma_notional_per_percent / peak) * 100
      const isSpot = Math.abs(strike - spot) < 1e-9
      const isFlip = flip != null && Math.abs(strike - flip) < 1e-9
      return <div key={row.strike} className={`options-gamma-bar${isSpot ? ' is-spot' : ''}${isFlip ? ' is-flip' : ''}`} title={`Strike ${row.strike} · calls ${compactNotional(row.call_gamma_notional_per_percent)} · puts ${compactNotional(row.put_gamma_notional_per_percent)}`}>
        <span className="options-gamma-bar__call" style={{ height: `${call}%` }} />
        <span className="options-gamma-bar__put" style={{ height: `${put}%` }} />
      </div>
    })}
  </div>
}

function compactNotional(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return 'Unavailable'
  const abs = Math.abs(value)
  if (abs >= 1e9) return `${(value / 1e9).toFixed(2)}B`
  if (abs >= 1e6) return `${(value / 1e6).toFixed(2)}M`
  if (abs >= 1e3) return `${(value / 1e3).toFixed(2)}K`
  return value.toFixed(2)
}

function GammaEvidence({ envelope, underlyer }: { envelope?: OptionsEnvelope<OptionGammaData>; underlyer: string }) {
  if (!envelope?.available) return <StatePanel title="No gamma profile persisted" detail={envelope?.reason === 'NO_GAMMA_PROFILE_FOR_POLICY' ? 'The active gamma policy has not produced a profile for this underlying yet. Profiles are written by the option worker per slot.' : 'The gamma profile table is not available in this database.'} warning />
  const data = envelope.data
  const profile = data.profiles.find(item => item.underlying === underlyer) || data.profiles[0]
  if (!profile) return <StatePanel title="No gamma profile persisted" detail="No profile matched the active gamma policy." warning />
  const spot = Number(profile.spot)
  const flip = profile.flip_spot != null ? Number(profile.flip_spot) : null
  return <>
    <section className="options-metrics-grid">
      <Metric label="Regime at spot" value={profile.regime_at_spot === 'NEGATIVE_GAMMA' ? 'Negative' : profile.regime_at_spot === 'POSITIVE_GAMMA' ? 'Positive' : 'Undetermined'} detail="Modeled dealer positioning" />
      <Metric label="Gamma flip" value={flip != null ? money(profile.flip_spot) : 'None in range'} detail={`Spot ${money(profile.spot)}`} />
      <Metric label="Net gamma / 1% move" value={compactNotional(profile.net_gamma_notional_per_percent)} detail="Signed by dealer assumption" />
      <Metric label="Chain coverage" value={pct(profile.coverage_fraction)} detail={`${integer(profile.contributing_contract_count)} of ${integer(profile.eligible_contract_count)} contracts`} />
    </section>
    <section className="options-panel">
      <div className="options-panel__header">
        <div><h3>Gamma exposure by strike <GammaRegimeBadge regime={profile.regime_at_spot} /></h3><p>{data.dealer_convention_note}</p></div>
        <span>{readable(profile.scope)} · {readable(profile.dealer_convention)}</span>
      </div>
      {profile.strike_profile?.length ? <GammaCurve rows={profile.strike_profile} spot={spot} flip={flip} /> : <div className="options-empty-block">Curve not requested for this view.</div>}
      <div className="options-gamma-legend"><span><i className="options-gamma-swatch options-gamma-swatch--call" />Call gamma</span><span><i className="options-gamma-swatch options-gamma-swatch--put" />Put gamma</span><span><i className="options-gamma-swatch options-gamma-swatch--spot" />Spot</span><span><i className="options-gamma-swatch options-gamma-swatch--flip" />Flip level</span></div>
      <div className="options-field-grid">
        <Field label="Peak gamma strike" value={money(profile.peak_gamma_strike)} />
        <Field label="Call gamma / 1%" value={compactNotional(profile.call_gamma_notional_per_percent)} />
        <Field label="Put gamma / 1%" value={compactNotional(profile.put_gamma_notional_per_percent)} />
        <Field label="Unsigned total / 1%" value={compactNotional(profile.absolute_gamma_notional_per_percent)} />
        <Field label="Strikes" value={integer(profile.strike_count)} />
        <Field label="Sign changes searched" value={integer(profile.sign_change_count)} />
        <Field label="Volatility assumption" value={readable(profile.volatility_assumption)} />
        <Field label="Wall gates" value={data.wall_gates_enabled ? 'Enabled' : 'Disabled'} />
        <Field label="Gamma policy" value={data.gamma_policy_version} title={data.gamma_policy_sha256} />
        <Field label="Source market time" value={dateTime(profile.market_data_time)} title={profile.market_data_time} />
        <Field label="Quality reasons" value={profile.quality_reasons.length ? profile.quality_reasons.map(readable).join(', ') : 'None'} />
      </div>
      <p className="options-section-note">Open interest states how many contracts exist, never who holds them. Signed values apply a versioned dealer assumption and are modeled research context, not observed positioning or a trade recommendation. Open interest is prior-session settled, so intraday values are stale.</p>
    </section>
  </>
}

function ResearchWorkbench({ lens, chain, analysis, gamma, underlyer, onSelect, onExplore }: { lens: ResearchLens; chain?: OptionsEnvelope<OptionChainData>; analysis?: OptionsEnvelope<OptionAnalysisData>; gamma?: OptionsEnvelope<OptionGammaData>; underlyer: string; onSelect: (row: OptionChainRow) => void; onExplore: () => void }) {
  if (!chain?.available) return <StatePanel title="No complete evidence matrix" detail="A complete delayed ingestion is required before retained observations can be investigated." warning />
  const analysisStatus = chain.data.analysis?.status || 'UNANALYZED'
  const reasons = chain.data.analysis?.quality_reasons || []
  const modelValidShown = chain.data.rows.filter(row => row.model_mark != null && row.iv_converged).length
  if (lens === 'gamma') return <>
    <LensHeader lens={lens} />
    {chain.data.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && <StatePanel title="Historical policy evidence" detail="The active valuation policy has not completed this underlying yet. Research evidence below comes from the latest prior-policy matrix." warning />}
    <GammaEvidence envelope={gamma} underlyer={underlyer} />
  </>
  return <>
    <LensHeader lens={lens} />
    {chain.data.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && <StatePanel title="Historical policy evidence" detail="The active valuation policy has not completed this underlying yet. Research evidence below comes from the latest prior-policy matrix." warning />}
    <section className="options-metrics-grid">
      <Metric label="Retained observations" value={integer(chain.data.total)} detail="Current complete matrix" />
      <Metric label="Analysis state" value={readable(analysisStatus)} detail="Persisted model-quality result" />
      <Metric label="Model-valid shown" value={`${modelValidShown} / ${chain.data.rows.length}`} detail="Current evidence page only" />
      <Metric label="Quote liquidity" value="Unavailable" detail="Developer entitlement" />
    </section>
    {analysisStatus !== 'COMPLETE' && <StatePanel title="Strategy interpretation is blocked" detail={reasons.length ? reasons.map(readable).join(' · ') : 'No passing persisted analysis is available.'} warning />}
    {lens === 'volatility' && <ExpirationEvidence envelope={analysis} />}
    <section className="options-panel">
      <div className="options-panel__header"><div><h3>Contract evidence</h3><p>Deterministic expiration and strike order; no opportunity score or strategy rank.</p></div><div className="options-panel-tools"><span>{chain.data.rows.length} shown</span><button type="button" onClick={onExplore}><Search size={14} />Full matrix</button></div></div>
      <EvidenceTable lens={lens} rows={chain.data.rows} onSelect={onSelect} />
    </section>
  </>
}

function ChainExplorer({ envelope, onOffsetChange, onSelect, onResearch }: { envelope?: OptionsEnvelope<OptionChainData>; onOffsetChange: (offset: number) => void; onSelect: (row: OptionChainRow) => void; onResearch: () => void }) {
  if (!envelope?.available) return <StatePanel title="No complete chain matrix" detail="Incomplete and failed page chains are diagnostic records and never appear as the current retained matrix." warning />
  const data = envelope.data
  const firstRow = data.rows.length ? data.offset + 1 : 0
  const lastRow = data.offset + data.rows.length
  return <>{data.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && <StatePanel title="Historical policy matrix" detail="The active valuation policy has not completed this underlying yet. This retained matrix is the latest prior-policy evidence." warning />}<section className="options-panel">
    <div className="options-panel__header"><div><h2>{data.underlyer} retained matrix</h2><p>Full paginated evidence matrix for detailed contract inspection.</p></div><div className="options-panel-tools"><Status value={data.analysis?.status || 'UNANALYZED'} /><button type="button" onClick={onResearch}><BarChart3 size={14} />Guided research</button></div></div>
    <div className="options-table-wrap"><table className="options-table options-table--chain"><thead><tr><th>Contract</th><th>Expiry</th><th>Type</th><th>Strike</th><th>DTE</th><th>Spot</th><th>Display</th><th>Model</th><th>Volume</th><th>OI</th><th>Local IV</th><th>Delta</th><th>Gamma</th><th>Quality</th><th aria-label="Details" /></tr></thead><tbody>{data.rows.map(row => <tr key={row.snapshot_id}><td className="options-contract-id">{row.contract_ticker}</td><td>{row.expiration_date}</td><td>{row.contract_type}</td><td>{money(row.strike)}</td><td>{row.calendar_dte}</td><td>{money(row.spot)}</td><td>{money(row.display_mark)}</td><td>{money(row.model_mark)}</td><td>{integer(row.day_volume)}</td><td>{integer(row.open_interest)}</td><td>{pct(row.local_iv)}</td><td>{number(row.local_delta)}</td><td>{number(row.local_gamma)}</td><td>{row.quality_flags.length ? row.quality_flags.map(readable).join(', ') : 'Clean'}</td><td><button type="button" className="options-icon-button" title="Open evidence" aria-label={`Open evidence for ${row.contract_ticker}`} onClick={() => onSelect(row)}><Eye size={15} /></button></td></tr>)}</tbody></table></div>
    <div className="options-pagination"><span>Showing {firstRow}-{lastRow} of {data.total}</span><div><button type="button" aria-label="Previous chain page" title="Previous page" disabled={data.offset === 0} onClick={() => onOffsetChange(Math.max(0, data.offset - data.limit))}><ChevronLeft size={16} /></button><button type="button" aria-label="Next chain page" title="Next page" disabled={lastRow >= data.total} onClick={() => onOffsetChange(data.offset + data.limit)}><ChevronRight size={16} /></button></div></div>
  </section></>
}

function OperationsHealth({ envelope }: { envelope?: OptionsEnvelope<OptionHealthData> }) {
  const health = envelope?.data
  if (!health) return <StatePanel title="Health unavailable" detail="The options health API could not be loaded." warning />
  const complete = health.underlyings.filter(row => row.status === 'COMPLETE').length
  const retained = health.underlyings.reduce((sum, row) => sum + Number(row.retained_row_count || 0), 0)
  return <>
    <section className="options-metrics-grid"><Metric label="Complete cycles" value={`${complete} / ${health.underlyings.length || 13}`} detail="Configured universe" /><Metric label="Retained contracts" value={integer(retained)} detail="Latest complete cycles" /><Metric label="Pending work" value={String(health.work.pending ?? 0)} detail={health.work.oldest_pending_seconds == null ? 'No pending backlog' : `${Math.round(health.work.oldest_pending_seconds)}s oldest`} /><Metric label="Scheduler" value={health.leader?.status || 'No leader'} detail={health.leader ? `Heartbeat ${age(health.leader.last_heartbeat_at)}` : 'Resident worker not active'} /></section>
    <section className="options-panel"><div className="options-panel__header"><div><h3>Underlying health</h3><p>Latest durable ingestion state by configured symbol.</p></div></div><div className="options-table-wrap"><table className="options-table"><thead><tr><th>Underlying</th><th>Status</th><th>Source time</th><th>Observed</th><th>Received</th><th>Retained</th><th>Unknown refs</th><th>Failure</th></tr></thead><tbody>{health.underlyings.map(row => <tr key={row.underlying}><td className="options-symbol">{row.underlying}</td><td><Status value={row.status} /></td><td>{dateTime(row.market_data_time)}</td><td>{dateTime(row.first_observed_at)}</td><td>{integer(row.received_row_count)}</td><td>{integer(row.retained_row_count)}</td><td>{integer(row.unknown_reference_count)}</td><td>{row.failure_reason || 'None'}</td></tr>)}</tbody></table></div></section>
  </>
}

function OperationsUniverse({ envelope }: { envelope?: OptionsEnvelope<OptionUniverseRow[]> }) {
  const rows = envelope?.data || []
  return <section className="options-panel"><div className="options-panel__header"><div><h3>Configured universe</h3><p>Read-only fixed stock and ETF cohorts. Portal actions cannot promote membership.</p></div><span>{rows.length} symbols</span></div><div className="options-table-wrap"><table className="options-table"><thead><tr><th>Rank</th><th>Symbol</th><th>Cohort</th><th>State</th><th>Effective</th><th>Completeness</th></tr></thead><tbody>{rows.map((row, index) => <tr key={row.ticker}><td>{row.member_rank ?? index + 1}</td><td className="options-symbol">{row.ticker}</td><td>{row.asset_type}</td><td><Status value={row.run_status || row.state || 'PENDING'} /></td><td>{row.effective_from || 'After first run'}</td><td>{pct(row.completeness_fraction)}</td></tr>)}</tbody></table></div></section>
}

function ExclusionBreakdown({ reasons }: { reasons: OptionExclusionReason[] }) {
  if (!reasons.length) return <span className="options-clean-value">None</span>
  return <div className="options-exclusion-breakdown">{reasons.map(reason => <span key={reason.code} title={readable(reason.code)}><strong>{integer(reason.count)}</strong>{reason.label}</span>)}</div>
}

function OperationsQuality({ envelope }: { envelope?: OptionsEnvelope<OptionDataQualityData> }) {
  if (!envelope?.available) return <StatePanel title="No ingestion diagnostics" detail="Durable batch and work state will appear after the first controlled refresh." warning />
  const data = envelope.data
  return <>
    <section className="options-quality-guide"><div className="options-panel__header"><div><h3>Ingestion decision funnel</h3><p>Provider rows remain auditable even when they cannot enter normalized or model-quality research.</p></div><span>Policy-derived thresholds</span></div><div className="options-quality-flow"><div><span>1</span><strong>Received</strong><small>{data.definitions.received}</small></div><ArrowRight size={17} /><div><span>2</span><strong>Catalog matched</strong><small>{data.definitions.catalog}</small></div><ArrowRight size={17} /><div><span>3</span><strong>Retained diagnostic</strong><small>{data.definitions.retained}</small></div><ArrowRight size={17} /><div><span>4</span><strong>Model eligible</strong><small>Fresh aligned marks and converged local IV feed strategy analysis.</small></div></div><div className="options-quality-policy-grid"><div><h4>Retention criteria</h4>{data.retention_criteria.map(item => <div key={item.label}><CheckCircle2 size={13} /><p><strong>{item.label}</strong><span>{item.detail}</span></p></div>)}</div><div><h4>Additional model criteria</h4>{data.model_eligibility_criteria.map(item => <div key={item.label}><Gauge size={13} /><p><strong>{item.label}</strong><span>{item.detail}</span></p></div>)}</div><div><h4>Unknown references</h4><div><CircleHelp size={14} /><p><strong>Catalog integrity check</strong><span>{data.definitions.unknown_references}</span></p></div><p className="options-policy-note">Alert gate: above the lower of {data.unknown_reference_gate.maximum_count} contracts or {pct(data.unknown_reference_gate.maximum_fraction)} of received rows.</p></div></div></section>
    <section className="options-panel"><div className="options-panel__header"><div><h3>Recent ingestion runs</h3><p>Distinct excluded rows are the received minus retained count; reason occurrences can overlap.</p></div></div><div className="options-table-wrap"><table className="options-table options-table--quality"><thead><tr><th>Underlying</th><th>Status</th><th>Cycle</th><th>Pages</th><th>Received</th><th>Catalog matched</th><th>Retained</th><th>Excluded</th><th>Why excluded</th><th title={data.definitions.unknown_references}>Unknown refs</th><th>Failure</th></tr></thead><tbody>{data.runs.map(row => <tr key={row.batch_id}><td className="options-symbol">{row.underlying}</td><td><Status value={row.status} /></td><td>{dateTime(row.scheduled_cycle)}</td><td>{integer(row.page_count)}</td><td>{integer(row.received_row_count)}</td><td>{integer(row.catalog_row_count)}<small className="options-cell-subtitle">{pct(row.catalog_coverage_fraction)} coverage</small></td><td>{integer(row.retained_row_count)}<small className="options-cell-subtitle">{pct(row.retention_fraction)} retained</small></td><td><strong>{integer(row.excluded_row_count)}</strong><small className="options-cell-subtitle">{pct(1 - row.retention_fraction)} excluded</small></td><td><ExclusionBreakdown reasons={row.exclusion_breakdown} /></td><td className={row.unknown_reference_count === 0 ? 'options-catalog-clean' : 'options-catalog-drift'}><strong>{integer(row.unknown_reference_count)}</strong><small>{row.unknown_reference_count === 0 ? 'All cataloged' : 'Review drift'}</small></td><td>{row.failure_reason || 'None'}</td></tr>)}</tbody></table></div></section>
    <section className="options-ops-grid"><div className="options-panel"><div className="options-panel__header"><div><h3>Durable work</h3><p>Stage and lease state.</p></div></div>{data.work.length ? data.work.map((row, index) => <div className="options-key-row" key={index}><span>{String(row.stage)} / {String(row.status)}</span><strong>{String(row.count)}</strong></div>) : <div className="options-empty-block">No work rows.</div>}</div><div className="options-panel"><div className="options-panel__header"><div><h3>Reference and backfill</h3><p>New-series quarantine and trade recovery.</p></div></div>{[...data.new_series, ...data.trade_backfills].length ? [...data.new_series, ...data.trade_backfills].map((row, index) => <div className="options-key-row" key={index}><span>{String(row.state || row.backfill_status)}</span><strong>{String(row.count)}</strong></div>) : <div className="options-empty-block">No pending reference or backfill work.</div>}</div></section>
  </>
}

function OperationsWorkspace({ section, health, universe, quality, onSection }: { section: OperationsSection; health?: OptionsEnvelope<OptionHealthData>; universe?: OptionsEnvelope<OptionUniverseRow[]>; quality?: OptionsEnvelope<OptionDataQualityData>; onSection: (value: OperationsSection) => void }) {
  return <>
    <section className="options-section-bar"><div><ShieldCheck size={18} /><div><h2>Operations</h2><p>Pipeline evidence stays available without occupying the default research view.</p></div></div><div className="options-segmented">{(['health', 'universe', 'quality'] as OperationsSection[]).map(value => <button type="button" key={value} className={section === value ? 'active' : ''} onClick={() => onSection(value)}>{value === 'health' ? 'Health' : value === 'universe' ? 'Universe' : 'Data Quality'}</button>)}</div></section>
    {section === 'health' && <OperationsHealth envelope={health} />}
    {section === 'universe' && <OperationsUniverse envelope={universe} />}
    {section === 'quality' && <OperationsQuality envelope={quality} />}
  </>
}

function EvidenceDrawer({ row, envelope, onClose, onExplore }: { row: OptionChainRow; envelope?: OptionsEnvelope<OptionChainData>; onClose: () => void; onExplore: () => void }) {
  const modelReason = row.iv_failure_reason || (row.model_mark == null ? 'No aligned model mark passed the current quality policy.' : 'Available')
  return <div className="options-drawer-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
    <aside className="options-drawer" role="dialog" aria-modal="true" aria-labelledby="evidence-drawer-title">
      <header><div><span>Retained contract evidence</span><h2 id="evidence-drawer-title">{row.contract_ticker}</h2><p>No strategy, rank, or execution claim is attached to this observation.</p></div><button type="button" className="options-icon-button" title="Close evidence" aria-label="Close evidence drawer" autoFocus onClick={onClose}><PanelRightClose size={18} /></button></header>
      <div className="options-drawer__body">
        <section><h3>Contract observation</h3><div className="options-field-grid"><Field label="Type" value={row.contract_type} /><Field label="Expiration" value={row.expiration_date} /><Field label="Strike" value={money(row.strike)} /><Field label="Calendar DTE" value={String(row.calendar_dte)} /><Field label="Underlying spot" value={money(row.spot)} /><Field label="Evidence state" value={row.model_mark != null && row.iv_converged ? 'Model valid' : 'Source observation'} /></div></section>
        <section><h3>Economics and marks</h3><div className="options-field-grid"><Field label="Display mark" value={money(row.display_mark)} /><Field label="Display source" value={readable(row.mark_source)} /><Field label="Model mark" value={money(row.model_mark)} title={modelReason} /><Field label="Intrinsic value" value={money(row.intrinsic_value)} /><Field label="Extrinsic value" value={money(row.extrinsic_value)} /><Field label="Single-leg breakeven" value={money(row.single_contract_breakeven)} /></div><p className="options-section-note">Display marks are delayed observations. They are not bids, asks, midpoint, NBBO, or executable prices.</p></section>
        <section><h3>Local risk measures</h3><div className="options-field-grid"><Field label="Local IV" value={pct(row.local_iv)} title={modelReason} /><Field label="Delta" value={number(row.local_delta)} title={modelReason} /><Field label="Gamma" value={number(row.local_gamma)} title={modelReason} /><Field label="Theta / day" value={number(row.local_theta_per_day)} title={modelReason} /><Field label="Vega / vol point" value={number(row.local_vega_per_vol_point)} title={modelReason} /><Field label="Rho / rate point" value={number(row.local_rho_per_rate_point)} title={modelReason} /></div></section>
        <section><h3>Activity and marketability</h3><div className="options-field-grid"><Field label="Day volume" value={integer(row.day_volume)} /><Field label="Open interest" value={integer(row.open_interest)} /><Field label="Quote liquidity" value="Not available" /><Field label="Bid / ask spread" value="Not available" /></div><p className="options-section-note">Volume and open interest do not establish aggressor side, institutional ownership, support, resistance, or expected pinning.</p></section>
        <section className="options-drawer-blocked"><LockKeyhole size={18} /><div><h3>No strategy package attached to this row</h3><p>This is raw retained evidence. Persisted selections and suppressions are available under Decisions.</p><button type="button" className="options-inline-action" onClick={onExplore}>View expiration in full matrix<ArrowRight size={14} /></button></div></section>
        <section><h3>Evidence provenance</h3><div className="options-field-grid"><Field label="Source market time" value={dateTime(row.market_data_time)} title={row.market_data_time} /><Field label="First observed" value={dateTime(row.first_observed_at)} title={row.first_observed_at} /><Field label="Policy hash" value={envelope?.policy_sha256?.slice(0, 12) || 'Unavailable'} title={envelope?.policy_sha256 || undefined} /><Field label="Model version" value={row.model_version || envelope?.model_version || 'Unavailable'} /><Field label="IV solver" value={row.iv_solver || 'Unavailable'} /><Field label="Quality reasons" value={row.quality_flags.length ? row.quality_flags.map(readable).join(', ') : 'None'} /></div></section>
      </div>
    </aside>
  </div>
}

export default function OptionsResearchWorkspace() {
  const location = useLocation()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const { view, lens, section, underlyer } = routeState(location.pathname, searchParams)
  const expiration = searchParams.get('expiration') || ''
  const requestedType = searchParams.get('type') as ContractTypeFilter | null
  const contractType: ContractTypeFilter = ['CALL', 'PUT'].includes(requestedType || '') ? requestedType! : 'ALL'
  const requestedOffset = Number(searchParams.get('offset') || 0)
  const offset = view === 'explorer' && Number.isFinite(requestedOffset) && requestedOffset > 0 ? Math.floor(requestedOffset) : 0
  const candidateOffset = view === 'candidates' && Number.isFinite(requestedOffset) && requestedOffset > 0 ? Math.floor(requestedOffset) : 0
  const recommendationOffset = view === 'recommendations' && Number.isFinite(requestedOffset) && requestedOffset > 0 ? Math.floor(requestedOffset) : 0
  const performanceOffset = view === 'performance' && Number.isFinite(requestedOffset) && requestedOffset > 0 ? Math.floor(requestedOffset) : 0
  const opportunityStrategy = (searchParams.get('strategy') || 'ALL').toUpperCase()
  const effectiveType = contractType === 'ALL' ? (view === 'research' && lens === 'income' ? 'PUT' : undefined) : contractType
  const [selectedRow, setSelectedRow] = useState<OptionChainRow | null>(null)
  const selectedCandidateId = searchParams.get('candidate')
  const requestedPersona = searchParams.get('persona') as OptionCandidatePersona | null
  const candidatePersona: OptionCandidatePersona = candidateSuites.some(item => item.value === requestedPersona) ? requestedPersona! : 'INCOME'
  const requestedCandidateStatus = searchParams.get('status') as OptionCandidateStatus | 'ALL' | null
  const candidateStatus: OptionCandidateStatus | 'ALL' = ['ALL', 'SELECTED', 'SUPPRESSED', 'REJECTED'].includes(requestedCandidateStatus || '') ? requestedCandidateStatus! : 'SELECTED'
  const requestedSignalStatus = searchParams.get('signal_status') as OptionSignalStatus | 'ALL' | null
  const signalStatus: OptionSignalStatus | 'ALL' = ['ALL', 'PENDING', 'READY', 'BLOCKED', 'EXPIRED'].includes(requestedSignalStatus || '') ? requestedSignalStatus! : 'ALL'

  const health = useQuery({ queryKey: ['options', 'health'], queryFn: getOptionHealth, refetchInterval: 60_000 })
  const universe = useQuery({ queryKey: ['options', 'universe'], queryFn: getOptionUniverse })
  const opportunities = useQuery({ queryKey: ['options', 'opportunities'], queryFn: () => getOptionOpportunities({ per_strategy: 1 }), enabled: view === 'opportunities' || view === 'performance' })
  const opportunityHistory = useQuery({ queryKey: ['options', 'rank-leader-history', opportunityStrategy], queryFn: () => getOptionPerformance({ strategy: opportunityStrategy === 'ALL' ? undefined : opportunityStrategy, cohort: 'RANK_LEADERS', days: 14, limit: 200, offset: 0 }), enabled: view === 'opportunities', refetchInterval: 60_000 })
  const chain = useQuery({ queryKey: ['options', 'chain', underlyer, expiration, effectiveType, offset], queryFn: () => getOptionChain(underlyer, { expiration: expiration || undefined, contract_type: effectiveType, limit: 100, offset }), enabled: view === 'research' || view === 'explorer' })
  const analysis = useQuery({ queryKey: ['options', 'analysis', underlyer], queryFn: () => getOptionAnalysis(underlyer), enabled: view === 'research' })
  const quality = useQuery({ queryKey: ['options', 'data-quality'], queryFn: getOptionDataQuality, enabled: view === 'operations' && section === 'quality' })
  const gamma = useQuery({ queryKey: ['options', 'gamma', underlyer], queryFn: () => getOptionGamma({ underlyer, scope: 'TOTAL', include_curve: true }), enabled: view === 'research' && lens === 'gamma' })
  const candidates = useQuery({ queryKey: ['options', 'candidates', underlyer, candidatePersona, candidateStatus, candidateOffset], queryFn: () => getOptionCandidates({ underlyer: underlyer === 'ALL' ? undefined : underlyer, persona: candidatePersona, status: candidateStatus === 'ALL' ? undefined : candidateStatus, limit: 100, offset: candidateOffset }), enabled: view === 'candidates' })
  const candidateDetail = useQuery({ queryKey: ['options', 'candidate', selectedCandidateId], queryFn: () => getOptionCandidate(selectedCandidateId!), enabled: (view === 'candidates' || view === 'opportunities') && selectedCandidateId != null })
  const recommendations = useQuery({ queryKey: ['options', 'recommendations', underlyer, signalStatus, recommendationOffset], queryFn: () => getOptionSignals({ underlyer: underlyer === 'ALL' ? undefined : underlyer, status: signalStatus === 'ALL' ? undefined : signalStatus, limit: 100, offset: recommendationOffset }), enabled: view === 'recommendations' })
  const performance = useQuery({ queryKey: ['options', 'performance', underlyer, opportunityStrategy, expiration, performanceOffset], queryFn: () => getOptionPerformance({ underlyer: underlyer === 'ALL' ? undefined : underlyer, strategy: opportunityStrategy === 'ALL' ? undefined : opportunityStrategy, expiration: expiration || undefined, cohort: 'BOARD_PUBLICATIONS', days: 60, limit: 20, offset: performanceOffset }), enabled: view === 'performance', refetchInterval: 60_000 })
  const members = universe.data?.data || [{ ticker: 'SPY', asset_type: 'ETF' as const }]
  const opportunityStrategies = Array.from(new Map((opportunities.data?.data.structured || []).map(row => [row.strategy_name, row.display_name])).entries()).map(([value, label]) => ({ value, label })).sort((left, right) => left.label.localeCompare(right.label))
  const loading = health.isLoading || universe.isLoading || (view === 'opportunities' && opportunities.isLoading) || ((view === 'research' || view === 'explorer') && chain.isLoading) || (view === 'research' && analysis.isLoading) || (view === 'candidates' && candidates.isLoading) || (view === 'recommendations' && recommendations.isLoading) || (view === 'performance' && performance.isLoading) || (view === 'operations' && section === 'quality' && quality.isLoading)
  const errored = health.isError || universe.isError || (view === 'opportunities' && opportunities.isError) || ((view === 'research' || view === 'explorer') && chain.isError) || (view === 'research' && analysis.isError) || (view === 'candidates' && candidates.isError) || (view === 'recommendations' && recommendations.isError) || (view === 'performance' && performance.isError) || (view === 'operations' && section === 'quality' && quality.isError)

  useEffect(() => setSelectedRow(null), [view, underlyer, lens, expiration, contractType])
  useEffect(() => {
    if (!selectedRow) return
    const closeOnEscape = (event: KeyboardEvent) => { if (event.key === 'Escape') setSelectedRow(null) }
    document.addEventListener('keydown', closeOnEscape)
    return () => document.removeEventListener('keydown', closeOnEscape)
  }, [selectedRow])

  const updateSearch = (changes: Record<string, string | null>) => {
    const next = new URLSearchParams(searchParams)
    Object.entries(changes).forEach(([key, value]) => value ? next.set(key, value) : next.delete(key))
    setSearchParams(next)
  }
  const changeUnderlyer = (value: string) => {
    if (view === 'explorer') navigate(`/options/explorer/${value}?${new URLSearchParams({ ...(expiration ? { expiration } : {}), ...(contractType !== 'ALL' ? { type: contractType } : {}) })}`)
    else updateSearch({ underlyer: value, offset: null })
  }
  const changeLens = (value: ResearchLens) => updateSearch({ lens: value, type: null, offset: null })
  const activeEnvelope = view === 'opportunities' ? opportunities.data
    : view === 'research' ? analysis.data
      : view === 'explorer' ? chain.data
        : view === 'candidates' ? candidates.data
          : view === 'recommendations' ? recommendations.data
            : view === 'performance' ? performance.data
              : quality.data || health.data
  const sourceTime = activeEnvelope?.as_of || health.data?.as_of
  usePublishPageContext({
    ...workspaceHeaderCopy[view],
    status: [
      { label: 'Source', value: dateTime(sourceTime), note: age(sourceTime), title: sourceTime || undefined },
    ],
  })

  return <div className="options-workspace">
    <WorkspaceNavigation view={view} underlyer={underlyer} navigate={navigate} />
    {(view === 'candidates' || view === 'recommendations' || view === 'performance') && <DecisionNavigation view={view} underlyer={underlyer} navigate={navigate} />}
    {view === 'opportunities' && <OpportunityCommandBar strategies={opportunityStrategies} strategy={opportunityStrategy} dataTier={opportunities.data?.data_tier || health.data?.data_tier || delayedLabel} onStrategy={value => updateSearch({ strategy: value === 'ALL' ? null : value, underlyer: null, candidate: null })} />}
    {(view === 'research' || view === 'explorer') && <EvidenceBar view={view} lens={lens} members={members} underlyer={underlyer} expiration={expiration} contractType={contractType} onUnderlyer={changeUnderlyer} onLens={changeLens} onExpiration={value => updateSearch({ expiration: value, offset: null })} onContractType={value => updateSearch({ type: value === 'ALL' ? null : value, offset: null })} />}
    {view === 'candidates' && <CandidateCommandBar members={members} underlyer={underlyer} persona={candidatePersona} status={candidateStatus} onUnderlyer={value => updateSearch({ underlyer: value === 'ALL' ? null : value, candidate: null, offset: null })} onPersona={value => updateSearch({ persona: value, candidate: null, offset: null })} onStatus={value => updateSearch({ status: value, candidate: null, offset: null })} />}
    {view === 'recommendations' && <RecommendationCommandBar members={members} underlyer={underlyer} status={signalStatus} onUnderlyer={value => updateSearch({ underlyer: value === 'ALL' ? null : value, offset: null })} onStatus={value => updateSearch({ signal_status: value === 'ALL' ? null : value, offset: null })} />}
    {view === 'performance' && <PerformanceCommandBar members={members} strategies={opportunityStrategies} underlyer={underlyer} strategy={opportunityStrategy} expiration={expiration} onUnderlyer={value => updateSearch({ underlyer: value === 'ALL' ? null : value, offset: null })} onStrategy={value => updateSearch({ strategy: value === 'ALL' ? null : value, offset: null })} onExpiration={value => updateSearch({ expiration: value || null, offset: null })} />}
    {loading && <StatePanel title="Loading durable option evidence" detail="Reading the latest causally visible PostgreSQL records." />}
    {errored && <StatePanel title="Options API unavailable" detail="The portal could not read the options service. Existing equity pages remain available." warning />}
    {!loading && !errored && view === 'opportunities' && <OpportunityBoard envelope={opportunities.data} history={opportunityHistory.data} historyLoading={opportunityHistory.isLoading} historyError={opportunityHistory.isError} strategy={opportunityStrategy} onSelect={candidateId => updateSearch({ candidate: candidateId })} onOpenHistory={() => navigate('/options/performance')} onTicker={ticker => navigate(`/ticker/${ticker}`)} />}
    {!loading && !errored && view === 'research' && <ResearchWorkbench lens={lens} chain={chain.data} analysis={analysis.data} gamma={gamma.data} underlyer={underlyer} onSelect={setSelectedRow} onExplore={() => navigate(`/options/explorer/${underlyer}${expiration ? `?expiration=${expiration}` : ''}`)} />}
    {!loading && !errored && view === 'candidates' && <CandidateWorkbench envelope={candidates.data} persona={candidatePersona} status={candidateStatus} onOffsetChange={value => updateSearch({ offset: value > 0 ? String(value) : null, candidate: null })} onSelect={candidateId => updateSearch({ candidate: candidateId })} />}
    {!loading && !errored && view === 'recommendations' && <RecommendationWorkbench envelope={recommendations.data} onOffsetChange={value => updateSearch({ offset: value > 0 ? String(value) : null })} />}
    {!loading && !errored && view === 'performance' && <PerformanceWorkbench envelope={performance.data} onOffsetChange={value => updateSearch({ offset: value > 0 ? String(value) : null })} />}
    {!loading && !errored && view === 'explorer' && <ChainExplorer envelope={chain.data} onOffsetChange={value => updateSearch({ offset: value > 0 ? String(value) : null })} onSelect={setSelectedRow} onResearch={() => navigate(`/options/research?underlyer=${underlyer}`)} />}
    {!loading && !errored && view === 'operations' && <OperationsWorkspace section={section} health={health.data} universe={universe.data} quality={quality.data} onSection={value => updateSearch({ section: value })} />}
    <footer className="options-footer"><CheckCircle2 size={14} />Read-only workspace. No quote-backed pricing, paper orders, or broker routing.</footer>
    {selectedRow && <EvidenceDrawer row={selectedRow} envelope={chain.data} onClose={() => setSelectedRow(null)} onExplore={() => navigate(`/options/explorer/${underlyer}?expiration=${selectedRow.expiration_date}`)} />}
    {(view === 'candidates' || view === 'opportunities') && selectedCandidateId && <CandidateDrawer envelope={candidateDetail.data} loading={candidateDetail.isLoading} error={candidateDetail.isError} onClose={() => updateSearch({ candidate: null })} />}
  </div>
}
