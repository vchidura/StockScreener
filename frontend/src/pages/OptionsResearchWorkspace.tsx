import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import {
  Activity,
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Database,
  Eye,
  Gauge,
  LayoutList,
  LockKeyhole,
  PanelRightClose,
  Search,
  ShieldCheck,
  TrendingUp,
} from 'lucide-react'
import {
  getOptionAnalysis,
  getOptionCandidate,
  getOptionCandidates,
  getOptionChain,
  getOptionDataQuality,
  getOptionHealth,
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
  OptionHealthData,
  OptionsEnvelope,
  OptionUniverseRow,
} from '../services/api'
import './OptionsResearchWorkspace.css'

type WorkspaceView = 'research' | 'candidates' | 'explorer' | 'operations'
type ResearchLens = 'income' | 'directional' | 'volatility' | 'activity'
type OperationsSection = 'health' | 'universe' | 'quality'
type ContractTypeFilter = 'ALL' | 'CALL' | 'PUT'

const delayedLabel = '15-MINUTE DELAYED RESEARCH DATA'
const lenses: Array<{ value: ResearchLens; label: string; icon: typeof Activity }> = [
  { value: 'income', label: 'Income Evidence', icon: Gauge },
  { value: 'directional', label: 'Directional Context', icon: TrendingUp },
  { value: 'volatility', label: 'Volatility & Range', icon: BarChart3 },
  { value: 'activity', label: 'OI & Activity', icon: Activity },
]

const candidateSuites: Array<{ value: OptionCandidatePersona; label: string; detail: string }> = [
  { value: 'INCOME', label: 'Income Generation', detail: 'Cash-secured put research with collateral and modeled return context.' },
  { value: 'DEFINED_RISK_INCOME', label: 'Defined-Risk Income', detail: 'Credit verticals and condors with complete listed legs and bounded loss.' },
  { value: 'MOMENTUM', label: 'Momentum', detail: 'Directional structures backed by persisted triggers and context.' },
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
const dateTime = (value: string | null | undefined) => value
  ? new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }).format(new Date(value))
  : 'Unavailable'
const readable = (value: string) => value.replace(/_/g, ' ')

function routeState(pathname: string, searchParams: URLSearchParams) {
  const parts = pathname.split('/').filter(Boolean)
  const route = parts[1]
  const requestedLens = searchParams.get('lens') as ResearchLens | null
  const requestedSection = searchParams.get('section') as OperationsSection | null
  let view: WorkspaceView = 'research'
  let lens: ResearchLens = lenses.some(item => item.value === requestedLens) ? requestedLens! : 'income'
  let section: OperationsSection = ['health', 'universe', 'quality'].includes(requestedSection || '') ? requestedSection! : 'health'

  if (route === 'explorer' || route === 'chain') view = 'explorer'
  if (route === 'candidates') view = 'candidates'
  if (route === 'operations' || route === 'universe' || route === 'data-quality') view = 'operations'
  if (route === 'analysis') lens = 'volatility'
  if (route === 'universe') section = 'universe'
  if (route === 'data-quality') section = 'quality'
  const requestedUnderlyer = parts[2] || searchParams.get('underlyer')

  return {
    view,
    lens,
    section,
    underlyer: (
      requestedUnderlyer || (view === 'candidates' ? 'ALL' : 'SPY')
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

function Field({ label, value, title }: { label: string; value: string; title?: string }) {
  return <div className="options-field"><span>{label}</span><strong title={title}>{value}</strong></div>
}

function WorkspaceHeader({ health }: { health?: OptionsEnvelope<OptionHealthData> }) {
  return <header className="options-header">
    <div>
      <div className="options-eyebrow">Options research workspace</div>
      <h1>Delayed market structure</h1>
      <p>Investigate durable contract evidence now; strategy conclusions remain evidence-gated.</p>
    </div>
    <div className="options-header__status">
      <span className="options-delay"><Activity size={14} />{health?.data_tier || delayedLabel}</span>
      <span title={health?.as_of || undefined}>Source {dateTime(health?.as_of)}</span>
      <span title={health?.observed_at || undefined}>Observed {dateTime(health?.observed_at)}</span>
    </div>
  </header>
}

function WorkspaceNavigation({ view, underlyer, navigate }: { view: WorkspaceView; underlyer: string; navigate: ReturnType<typeof useNavigate> }) {
  const evidenceUnderlyer = underlyer === 'ALL' ? 'SPY' : underlyer
  const items: Array<{ value: WorkspaceView; label: string; icon: typeof Activity; path: string }> = [
    { value: 'research', label: 'Research', icon: BarChart3, path: `/options?underlyer=${evidenceUnderlyer}` },
    { value: 'candidates', label: 'Strategy Workbench', icon: LayoutList, path: underlyer === 'ALL' ? '/options/candidates' : `/options/candidates?underlyer=${underlyer}` },
    { value: 'explorer', label: 'Explorer', icon: Search, path: `/options/explorer/${evidenceUnderlyer}` },
    { value: 'operations', label: 'Operations', icon: ShieldCheck, path: '/options/operations' },
  ]
  return <nav className="options-tabs" aria-label="Options workspace views">
    {items.map(item => { const Icon = item.icon; return <button key={item.value} type="button" className={view === item.value ? 'active' : ''} onClick={() => navigate(item.path)}><Icon size={15} /><span>{item.label}</span></button> })}
  </nav>
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
    <section className="options-candidate-intro"><div><span>Phase 2 strategy evidence</span><h2>{suite.label}</h2><p>{suite.detail}</p></div><div><strong>Weekly Research Candidates</strong><span>Future expiration cohort, not a next-week forecast</span></div></section>
    <section className="options-metrics-grid"><Metric label="Matching decisions" value={integer(envelope?.data?.total || 0)} detail="Current status page" /><Metric label="Selected research" value={integer(counts.selected)} detail="All matching underlyings" /><Metric label="Suppressed" value={integer(counts.suppressed)} detail="Inspectable failed gates" /><Metric label="Quote liquidity" value="Unavailable" detail="Developer entitlement" /></section>
    {!envelope?.available && status === 'SELECTED' && <StatePanel title="No selected recommendations" detail={`${counts.suppressed} matching decisions were suppressed by backend quality or context gates. Choose Suppressed or All decisions to inspect them.`} warning />}
    {!envelope?.available && status !== 'SELECTED' && <StatePanel title="No matching strategy decisions" detail="No persisted backend decisions match the current suite and filters." warning />}
    <section className="options-panel"><div className="options-panel__header"><div><h3>{suite.label} decisions</h3><p>Stable backend rank and explanations; the browser does not infer candidates.</p></div><span>{rows.length} shown</span></div><div className="options-table-wrap"><table className="options-table options-table--candidates"><thead><tr><th>Status</th><th>Strategy</th><th>Structure</th><th>Expiration</th><th>Ordered legs</th><th>Net premium</th><th>Maximum loss</th><th>Return on risk</th><th>Source time</th><th>Primary evidence</th><th>Blocked reason</th><th aria-label="Details" /></tr></thead><tbody>{rows.length ? rows.map(row => <CandidateRow key={row.candidate_id} row={row} onSelect={onSelect} />) : <tr><td colSpan={12} className="options-empty-cell">No persisted decisions match this suite and filter.</td></tr>}</tbody></table></div>{envelope?.data && <div className="options-pagination"><span>{envelope.data.total ? `Showing ${firstRow}-${lastRow} of ${envelope.data.total}` : '0 results'}</span><div><button type="button" aria-label="Previous candidate page" title="Previous page" disabled={envelope.data.offset === 0} onClick={() => onOffsetChange(Math.max(0, envelope.data.offset - envelope.data.limit))}><ChevronLeft size={16} /></button><button type="button" aria-label="Next candidate page" title="Next page" disabled={lastRow >= envelope.data.total} onClick={() => onOffsetChange(envelope.data.offset + envelope.data.limit)}><ChevronRight size={16} /></button></div></div>}</section>
  </>
}

function CandidateRow({ row, onSelect }: { row: OptionCandidateRow; onSelect: (candidateId: string) => void }) {
  const legs = row.legs.length ? row.legs.map(leg => `${leg.side} ${leg.ratio} ${leg.contract_ticker}`).join(' / ') : 'No tradable package'
  const evidence = row.primary_metric_name && row.primary_metric_value != null ? `${readable(row.primary_metric_name)} ${number(row.primary_metric_value)}` : 'Unavailable'
  return <tr><td><Status value={row.status} /></td><td><strong>{row.display_name}</strong><small className="options-cell-subtitle">{readable(row.strategy_version)}</small></td><td>{readable(row.structure_type)}</td><td>{row.expiration_date || 'Unavailable'}</td><td className="options-leg-cell">{legs}</td><td>{money(row.net_premium)}</td><td className="options-risk-cell">{money(row.maximum_loss)}</td><td>{row.return_on_risk == null ? 'Unavailable' : pct(row.return_on_risk)}</td><td>{dateTime(row.market_data_time)}</td><td>{evidence}</td><td>{row.reason_codes.length ? readable(row.reason_codes[0]) : 'None'}</td><td><button type="button" className="options-icon-button" title="Open candidate evidence" aria-label={`Open candidate ${row.candidate_id}`} onClick={() => onSelect(row.candidate_id)}><Eye size={15} /></button></td></tr>
}

function CandidateDrawer({ envelope, loading, error, onClose }: { envelope?: OptionsEnvelope<OptionCandidateDetailData>; loading: boolean; error: boolean; onClose: () => void }) {
  const data = envelope?.data
  const candidate = data?.candidate
  return <div className="options-drawer-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}><aside className="options-drawer options-drawer--candidate" role="dialog" aria-modal="true" aria-labelledby="candidate-drawer-title"><header><div><span>Weekly research candidate</span><h2 id="candidate-drawer-title">{loading ? 'Loading evidence' : candidate?.display_name || 'Candidate unavailable'}</h2><p>Delayed research record. Candidate state is not broker authorization.</p></div><button type="button" className="options-icon-button" title="Close candidate" aria-label="Close candidate drawer" autoFocus onClick={onClose} onKeyDown={event => { if (event.key === 'Escape') onClose() }}><PanelRightClose size={18} /></button></header>{loading && <StatePanel title="Loading candidate evidence" detail="Reading immutable decision and scenario records." />}{error && <StatePanel title="Candidate evidence unavailable" detail="The detail API could not load this persisted decision." warning />}{!loading && candidate && data && <div className="options-drawer__body">
    <section><h3>Structure</h3><div className="options-field-grid"><Field label="Status" value={readable(candidate.status)} /><Field label="Structure" value={readable(candidate.structure_type)} /><Field label="Risk class" value={readable(candidate.structure_risk_class)} /><Field label="Expiration" value={candidate.expiration_date || 'Unavailable'} /><Field label="Rank" value={String(candidate.candidate_rank)} /><Field label="Eligibility" value={candidate.execution_eligibility || 'Not eligible'} /></div>{data.legs.length ? <div className="options-leg-stack">{data.legs.map(leg => <div key={leg.leg_index}><span>{leg.side} {leg.ratio}</span><strong>{leg.contract_ticker}</strong><small>{leg.contract_type} {money(leg.strike)} / model {money(leg.model_mark)}</small></div>)}</div> : <p className="options-section-note">No tradable package was created for this suppressed or research-only record.</p>}</section>
    <section><h3>Risk and payoff</h3><div className="options-field-grid"><Field label="Net premium" value={money(candidate.net_premium)} /><Field label="Maximum loss" value={money(candidate.maximum_loss)} /><Field label="Maximum profit" value={money(candidate.maximum_profit)} /><Field label="Capital at risk" value={money(candidate.capital_at_risk)} /><Field label="Return on risk" value={candidate.return_on_risk == null ? 'Unavailable' : pct(candidate.return_on_risk)} /><Field label="Breakevens" value={candidate.breakevens.length ? candidate.breakevens.map(value => money(value)).join(', ') : 'Unavailable'} /></div>{data.scenarios.length ? <div className="options-scenario-wrap"><table className="options-scenario-table"><thead><tr><th>Spot shock</th><th>IV shock</th><th>Time left</th><th>Repriced</th><th>P&amp;L</th></tr></thead><tbody>{data.scenarios.map(row => <tr key={row.scenario_result_id}><td>{pct(row.spot_shock_fraction)}</td><td>{pct(row.iv_shock_fraction)}</td><td>{pct(row.time_fraction_remaining)}</td><td>{money(row.repriced_value)}</td><td>{money(row.profit_loss)}</td></tr>)}</tbody></table></div> : <p className="options-section-note">Scenario results are unavailable because no complete strategy package passed selection.</p>}</section>
    <section><h3>Trend and context</h3><div className="options-field-grid"><Field label="Context status" value={candidate.context_status ? readable(candidate.context_status) : 'Unavailable'} /><Field label="Trend state" value={candidate.trend_state || 'Unavailable'} /><Field label="Earnings blackout" value={candidate.earnings_blackout_state || 'Unavailable'} /><Field label="Fed blackout" value={candidate.fed_blackout_state || 'Unavailable'} /></div></section>
    <section><h3>Liquidity and marketability</h3><div className="options-field-grid"><Field label="Quote liquidity" value="Not available" /><Field label="Quote spread" value="Not available" /><Field label="Signal state" value={candidate.signal_status ? readable(candidate.signal_status) : 'No signal'} /><Field label="Execution mode" value={readable(data.execution_mode)} /></div></section>
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

function ResearchWorkbench({ lens, chain, analysis, onSelect }: { lens: ResearchLens; chain?: OptionsEnvelope<OptionChainData>; analysis?: OptionsEnvelope<OptionAnalysisData>; onSelect: (row: OptionChainRow) => void }) {
  if (!chain?.available) return <StatePanel title="No complete evidence matrix" detail="A complete delayed ingestion is required before retained observations can be investigated." warning />
  const analysisStatus = chain.data.analysis?.status || 'UNANALYZED'
  const reasons = chain.data.analysis?.quality_reasons || []
  const modelValidShown = chain.data.rows.filter(row => row.model_mark != null && row.iv_converged).length
  return <>
    <LensHeader lens={lens} />
    <section className="options-metrics-grid">
      <Metric label="Retained observations" value={integer(chain.data.total)} detail="Current complete matrix" />
      <Metric label="Analysis state" value={readable(analysisStatus)} detail="Persisted model-quality result" />
      <Metric label="Model-valid shown" value={`${modelValidShown} / ${chain.data.rows.length}`} detail="Current evidence page only" />
      <Metric label="Quote liquidity" value="Unavailable" detail="Developer entitlement" />
    </section>
    {analysisStatus !== 'COMPLETE' && <StatePanel title="Strategy interpretation is blocked" detail={reasons.length ? reasons.map(readable).join(' · ') : 'No passing persisted analysis is available.'} warning />}
    {lens === 'volatility' && <ExpirationEvidence envelope={analysis} />}
    <section className="options-panel">
      <div className="options-panel__header"><div><h3>Contract evidence</h3><p>Deterministic expiration and strike order; no opportunity score or strategy rank.</p></div><span>{chain.data.rows.length} shown</span></div>
      <EvidenceTable lens={lens} rows={chain.data.rows} onSelect={onSelect} />
    </section>
  </>
}

function ChainExplorer({ envelope, onOffsetChange, onSelect }: { envelope?: OptionsEnvelope<OptionChainData>; onOffsetChange: (offset: number) => void; onSelect: (row: OptionChainRow) => void }) {
  if (!envelope?.available) return <StatePanel title="No complete chain matrix" detail="Incomplete and failed page chains are diagnostic records and never appear as the current retained matrix." warning />
  const data = envelope.data
  const firstRow = data.rows.length ? data.offset + 1 : 0
  const lastRow = data.offset + data.rows.length
  return <section className="options-panel">
    <div className="options-panel__header"><div><h2>{data.underlyer} retained matrix</h2><p>Raw evidence remains available here without dominating the research workflow.</p></div><Status value={data.analysis?.status || 'UNANALYZED'} /></div>
    <div className="options-table-wrap"><table className="options-table options-table--chain"><thead><tr><th>Contract</th><th>Expiry</th><th>Type</th><th>Strike</th><th>DTE</th><th>Spot</th><th>Display</th><th>Model</th><th>Volume</th><th>OI</th><th>Local IV</th><th>Delta</th><th>Gamma</th><th>Quality</th><th aria-label="Details" /></tr></thead><tbody>{data.rows.map(row => <tr key={row.snapshot_id}><td className="options-contract-id">{row.contract_ticker}</td><td>{row.expiration_date}</td><td>{row.contract_type}</td><td>{money(row.strike)}</td><td>{row.calendar_dte}</td><td>{money(row.spot)}</td><td>{money(row.display_mark)}</td><td>{money(row.model_mark)}</td><td>{integer(row.day_volume)}</td><td>{integer(row.open_interest)}</td><td>{pct(row.local_iv)}</td><td>{number(row.local_delta)}</td><td>{number(row.local_gamma)}</td><td>{row.quality_flags.length ? row.quality_flags.map(readable).join(', ') : 'Clean'}</td><td><button type="button" className="options-icon-button" title="Open evidence" aria-label={`Open evidence for ${row.contract_ticker}`} onClick={() => onSelect(row)}><Eye size={15} /></button></td></tr>)}</tbody></table></div>
    <div className="options-pagination"><span>Showing {firstRow}-{lastRow} of {data.total}</span><div><button type="button" aria-label="Previous chain page" title="Previous page" disabled={data.offset === 0} onClick={() => onOffsetChange(Math.max(0, data.offset - data.limit))}><ChevronLeft size={16} /></button><button type="button" aria-label="Next chain page" title="Next page" disabled={lastRow >= data.total} onClick={() => onOffsetChange(data.offset + data.limit)}><ChevronRight size={16} /></button></div></div>
  </section>
}

function OperationsHealth({ envelope }: { envelope?: OptionsEnvelope<OptionHealthData> }) {
  const health = envelope?.data
  if (!health) return <StatePanel title="Health unavailable" detail="The options health API could not be loaded." warning />
  const complete = health.underlyings.filter(row => row.status === 'COMPLETE').length
  const retained = health.underlyings.reduce((sum, row) => sum + Number(row.retained_row_count || 0), 0)
  return <>
    <section className="options-metrics-grid"><Metric label="Complete cycles" value={`${complete} / ${health.underlyings.length || 13}`} detail="Configured universe" /><Metric label="Retained contracts" value={integer(retained)} detail="Latest complete cycles" /><Metric label="Pending work" value={String(health.work.pending ?? 0)} detail={health.work.oldest_pending_seconds == null ? 'No pending backlog' : `${Math.round(health.work.oldest_pending_seconds)}s oldest`} /><Metric label="Partitions" value={health.partitions_ready ? 'Ready' : 'Not ready'} detail="Current monthly partitions" /></section>
    <section className="options-panel"><div className="options-panel__header"><div><h3>Underlying health</h3><p>Latest durable ingestion state by configured symbol.</p></div></div><div className="options-table-wrap"><table className="options-table"><thead><tr><th>Underlying</th><th>Status</th><th>Source time</th><th>Observed</th><th>Received</th><th>Retained</th><th>Unknown refs</th><th>Failure</th></tr></thead><tbody>{health.underlyings.map(row => <tr key={row.underlying}><td className="options-symbol">{row.underlying}</td><td><Status value={row.status} /></td><td>{dateTime(row.market_data_time)}</td><td>{dateTime(row.first_observed_at)}</td><td>{integer(row.received_row_count)}</td><td>{integer(row.retained_row_count)}</td><td>{integer(row.unknown_reference_count)}</td><td>{row.failure_reason || 'None'}</td></tr>)}</tbody></table></div></section>
  </>
}

function OperationsUniverse({ envelope }: { envelope?: OptionsEnvelope<OptionUniverseRow[]> }) {
  const rows = envelope?.data || []
  return <section className="options-panel"><div className="options-panel__header"><div><h3>Configured universe</h3><p>Read-only fixed stock and ETF cohorts. Portal actions cannot promote membership.</p></div><span>{rows.length} symbols</span></div><div className="options-table-wrap"><table className="options-table"><thead><tr><th>Rank</th><th>Symbol</th><th>Cohort</th><th>State</th><th>Effective</th><th>Completeness</th></tr></thead><tbody>{rows.map((row, index) => <tr key={row.ticker}><td>{row.member_rank ?? index + 1}</td><td className="options-symbol">{row.ticker}</td><td>{row.asset_type}</td><td><Status value={row.run_status || row.state || 'PENDING'} /></td><td>{row.effective_from || 'After first run'}</td><td>{pct(row.completeness_fraction)}</td></tr>)}</tbody></table></div></section>
}

function OperationsQuality({ envelope }: { envelope?: OptionsEnvelope<OptionDataQualityData> }) {
  if (!envelope?.available) return <StatePanel title="No ingestion diagnostics" detail="Durable batch and work state will appear after the first controlled refresh." warning />
  const data = envelope.data
  return <>
    <section className="options-panel"><div className="options-panel__header"><div><h3>Recent ingestion runs</h3><p>Provider pagination, catalog coverage, retained rows, and exact failure state.</p></div></div><div className="options-table-wrap"><table className="options-table"><thead><tr><th>Underlying</th><th>Status</th><th>Cycle</th><th>Pages</th><th>Received</th><th>Catalog</th><th>Retained</th><th>Unknown</th><th>Failure</th></tr></thead><tbody>{data.runs.map((row, index) => <tr key={String(row.batch_id || index)}><td className="options-symbol">{String(row.underlying)}</td><td><Status value={String(row.status)} /></td><td>{dateTime(row.scheduled_cycle as string)}</td><td>{String(row.page_count)}</td><td>{String(row.received_row_count)}</td><td>{String(row.catalog_row_count)}</td><td>{String(row.retained_row_count)}</td><td>{String(row.unknown_reference_count)}</td><td>{String(row.failure_reason || 'None')}</td></tr>)}</tbody></table></div></section>
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

function EvidenceDrawer({ row, envelope, onClose }: { row: OptionChainRow; envelope?: OptionsEnvelope<OptionChainData>; onClose: () => void }) {
  const modelReason = row.iv_failure_reason || (row.model_mark == null ? 'No aligned model mark passed the current quality policy.' : 'Available')
  return <div className="options-drawer-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
    <aside className="options-drawer" role="dialog" aria-modal="true" aria-labelledby="evidence-drawer-title">
      <header><div><span>Retained contract evidence</span><h2 id="evidence-drawer-title">{row.contract_ticker}</h2><p>No strategy, rank, or execution claim is attached to this observation.</p></div><button type="button" className="options-icon-button" title="Close evidence" aria-label="Close evidence drawer" autoFocus onClick={onClose}><PanelRightClose size={18} /></button></header>
      <div className="options-drawer__body">
        <section><h3>Contract observation</h3><div className="options-field-grid"><Field label="Type" value={row.contract_type} /><Field label="Expiration" value={row.expiration_date} /><Field label="Strike" value={money(row.strike)} /><Field label="Calendar DTE" value={String(row.calendar_dte)} /><Field label="Underlying spot" value={money(row.spot)} /><Field label="Evidence state" value={row.model_mark != null && row.iv_converged ? 'Model valid' : 'Source observation'} /></div></section>
        <section><h3>Economics and marks</h3><div className="options-field-grid"><Field label="Display mark" value={money(row.display_mark)} /><Field label="Display source" value={readable(row.mark_source)} /><Field label="Model mark" value={money(row.model_mark)} title={modelReason} /><Field label="Intrinsic value" value={money(row.intrinsic_value)} /><Field label="Extrinsic value" value={money(row.extrinsic_value)} /><Field label="Single-leg breakeven" value={money(row.single_contract_breakeven)} /></div><p className="options-section-note">Display marks are delayed observations. They are not bids, asks, midpoint, NBBO, or executable prices.</p></section>
        <section><h3>Local risk measures</h3><div className="options-field-grid"><Field label="Local IV" value={pct(row.local_iv)} title={modelReason} /><Field label="Delta" value={number(row.local_delta)} title={modelReason} /><Field label="Gamma" value={number(row.local_gamma)} title={modelReason} /><Field label="Theta / day" value={number(row.local_theta_per_day)} title={modelReason} /><Field label="Vega / vol point" value={number(row.local_vega_per_vol_point)} title={modelReason} /><Field label="Rho / rate point" value={number(row.local_rho_per_rate_point)} title={modelReason} /></div></section>
        <section><h3>Activity and marketability</h3><div className="options-field-grid"><Field label="Day volume" value={integer(row.day_volume)} /><Field label="Open interest" value={integer(row.open_interest)} /><Field label="Quote liquidity" value="Not available" /><Field label="Bid / ask spread" value="Not available" /></div><p className="options-section-note">Volume and open interest do not establish aggressor side, institutional ownership, support, resistance, or expected pinning.</p></section>
        <section className="options-drawer-blocked"><LockKeyhole size={18} /><div><h3>No strategy package attached to this row</h3><p>This is raw retained evidence. Persisted recommendations and suppressions are available separately in the Strategy Workbench.</p></div></section>
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
  const effectiveType = contractType === 'ALL' ? (view === 'research' && lens === 'income' ? 'PUT' : undefined) : contractType
  const [selectedRow, setSelectedRow] = useState<OptionChainRow | null>(null)
  const selectedCandidateId = searchParams.get('candidate')
  const requestedPersona = searchParams.get('persona') as OptionCandidatePersona | null
  const candidatePersona: OptionCandidatePersona = candidateSuites.some(item => item.value === requestedPersona) ? requestedPersona! : 'INCOME'
  const requestedCandidateStatus = searchParams.get('status') as OptionCandidateStatus | 'ALL' | null
  const candidateStatus: OptionCandidateStatus | 'ALL' = ['ALL', 'SELECTED', 'SUPPRESSED', 'REJECTED'].includes(requestedCandidateStatus || '') ? requestedCandidateStatus! : 'SELECTED'

  const health = useQuery({ queryKey: ['options', 'health'], queryFn: getOptionHealth, refetchInterval: 60_000 })
  const universe = useQuery({ queryKey: ['options', 'universe'], queryFn: getOptionUniverse })
  const chain = useQuery({ queryKey: ['options', 'chain', underlyer, expiration, effectiveType, offset], queryFn: () => getOptionChain(underlyer, { expiration: expiration || undefined, contract_type: effectiveType, limit: 100, offset }), enabled: view === 'research' || view === 'explorer' })
  const analysis = useQuery({ queryKey: ['options', 'analysis', underlyer], queryFn: () => getOptionAnalysis(underlyer), enabled: view === 'research' })
  const quality = useQuery({ queryKey: ['options', 'data-quality'], queryFn: getOptionDataQuality, enabled: view === 'operations' && section === 'quality' })
  const candidates = useQuery({ queryKey: ['options', 'candidates', underlyer, candidatePersona, candidateStatus, candidateOffset], queryFn: () => getOptionCandidates({ underlyer: underlyer === 'ALL' ? undefined : underlyer, persona: candidatePersona, status: candidateStatus === 'ALL' ? undefined : candidateStatus, limit: 100, offset: candidateOffset }), enabled: view === 'candidates' })
  const candidateDetail = useQuery({ queryKey: ['options', 'candidate', selectedCandidateId], queryFn: () => getOptionCandidate(selectedCandidateId!), enabled: view === 'candidates' && selectedCandidateId != null })
  const members = universe.data?.data || [{ ticker: 'SPY', asset_type: 'ETF' as const }]
  const loading = health.isLoading || universe.isLoading || ((view === 'research' || view === 'explorer') && chain.isLoading) || (view === 'research' && analysis.isLoading) || (view === 'candidates' && candidates.isLoading) || (view === 'operations' && section === 'quality' && quality.isLoading)
  const errored = health.isError || universe.isError || ((view === 'research' || view === 'explorer') && chain.isError) || (view === 'research' && analysis.isError) || (view === 'candidates' && candidates.isError) || (view === 'operations' && section === 'quality' && quality.isError)

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

  return <div className="options-workspace">
    <WorkspaceHeader health={health.data} />
    <WorkspaceNavigation view={view} underlyer={underlyer} navigate={navigate} />
    {(view === 'research' || view === 'explorer') && <EvidenceBar view={view} lens={lens} members={members} underlyer={underlyer} expiration={expiration} contractType={contractType} onUnderlyer={changeUnderlyer} onLens={changeLens} onExpiration={value => updateSearch({ expiration: value, offset: null })} onContractType={value => updateSearch({ type: value === 'ALL' ? null : value, offset: null })} />}
    {view === 'candidates' && <CandidateCommandBar members={members} underlyer={underlyer} persona={candidatePersona} status={candidateStatus} onUnderlyer={value => updateSearch({ underlyer: value === 'ALL' ? null : value, candidate: null, offset: null })} onPersona={value => updateSearch({ persona: value, candidate: null, offset: null })} onStatus={value => updateSearch({ status: value, candidate: null, offset: null })} />}
    {loading && <StatePanel title="Loading durable option evidence" detail="Reading the latest causally visible PostgreSQL records." />}
    {errored && <StatePanel title="Options API unavailable" detail="The portal could not read the delayed options service. Existing equity pages remain available." warning />}
    {!loading && !errored && view === 'research' && <ResearchWorkbench lens={lens} chain={chain.data} analysis={analysis.data} onSelect={setSelectedRow} />}
    {!loading && !errored && view === 'candidates' && <CandidateWorkbench envelope={candidates.data} persona={candidatePersona} status={candidateStatus} onOffsetChange={value => updateSearch({ offset: value > 0 ? String(value) : null, candidate: null })} onSelect={candidateId => updateSearch({ candidate: candidateId })} />}
    {!loading && !errored && view === 'explorer' && <ChainExplorer envelope={chain.data} onOffsetChange={value => updateSearch({ offset: value > 0 ? String(value) : null })} onSelect={setSelectedRow} />}
    {!loading && !errored && view === 'operations' && <OperationsWorkspace section={section} health={health.data} universe={universe.data} quality={quality.data} onSection={value => updateSearch({ section: value })} />}
    <footer className="options-footer"><CheckCircle2 size={14} />Read-only delayed strategy research. No quote-backed pricing, paper orders, or broker routing.</footer>
    {selectedRow && <EvidenceDrawer row={selectedRow} envelope={chain.data} onClose={() => setSelectedRow(null)} />}
    {view === 'candidates' && selectedCandidateId && <CandidateDrawer envelope={candidateDetail.data} loading={candidateDetail.isLoading} error={candidateDetail.isError} onClose={() => updateSearch({ candidate: null })} />}
  </div>
}