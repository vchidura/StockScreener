import { useDeferredValue, useEffect, useState, type FormEvent, type KeyboardEvent, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { AlertTriangle, ArrowDown, ArrowLeft, ArrowRight, ArrowUp, ArrowUpDown, BarChart3, Bell, CheckCircle2, Clock, Eye, FlaskConical, History, ListFilter, RefreshCw, RotateCcw, Save, ShieldAlert, Trash2 } from 'lucide-react'
import { usePublishPageContext, type PageContextValue } from '../layout/pageContext'
import { ColumnPicker, useColumnPreferences, type ColumnSpec } from '../layout/PageChrome'
import {
  getOptionAlertHistory, getOptionBehaviorReview, getOptionCandidate, getOptionCandidates, getOptionDiscoveryCatalog, getOptionDetectorEvaluations, getOptionDetectorAlerts, getOptionDetectorDatasets, previewOptionAlert,
  type OptionAlertHistoryRow, type OptionAlertPreviewRequest, type OptionCandidateDetailData, type OptionCandidateLeg,
  type OptionCandidateResearchEvidence, type OptionStockBehaviorGateEvidence,
  type OptionDetectorEvaluationReview, type OptionDetectorAlertReview, type OptionDetectorSort, type OptionDetectorDatasets,
  type OptionCandidatePersona, type OptionCandidateStatus,
} from '../services/api'
import { Modal } from './ScreenLibrary'
import { optionDetectorSort, optionDetectorSortChange, optionPackageStrategyName, detectorAlertColumns, detectorColumnPresets, detectorViewPreset, readDetectorPresets, writeDetectorPresets, type DetectorViewPreset } from './optionsAlertPresentation'
import { emptyOptionAlertForm, optionAlertColumns, optionAlertTabParams, optionReviewScope, optionReviewSelection, optionAlertVisibleColumns, optionBehaviorMetrics, optionCandidateBoardFacts, optionLegMarketValues, optionMarketColumns, optionPackagePremium, optionPackagePrice, optionPlanTerms, optionRemainingHold, optionAlertLabel as label, optionAlertMoney as money, optionAlertPercent, optionAlertPreviewRequest, optionAlertTime as time, optionEntryWindow, optionGateTimeframe, optionGateValue, type OptionAlertColumn, type OptionAlertForm, type OptionAlertView } from './optionsAlertPresentation'
import './StockDiscoveryPage.css'
import './StockScreeningPage.css'
import './OptionsAlertsPage.css'

const pageSize = 50
const capabilityLabels: Record<string, string> = { OBSERVATION: 'Observation evidence', INDICATIVE: 'Indicative package', QUOTE_PAPER: 'Quote-qualified paper', EXECUTION: 'Execution' }
const candidateStates: OptionCandidateStatus[] = ['SELECTED', 'SUPPRESSED', 'REJECTED']
const categories: OptionCandidatePersona[] = ['INCOME', 'DEFINED_RISK_INCOME', 'MOMENTUM', 'NEUTRAL_VOL']

function ColumnHeaders({ columns }: { columns: OptionAlertColumn[] }) {
  return <>{columns.map(column => <th key={column.key} scope="col" title={column.tip}>{column.label}</th>)}</>
}

function ColumnCells({ columns, values }: { columns: ColumnSpec[]; values: Record<string, ReactNode> }) {
  return <>{columns.map(column => <td key={column.key}>{values[column.key] ?? 'Unavailable'}</td>)}</>
}

function marketCells(legs: Array<Pick<OptionCandidateLeg, 'leg_index' | 'contract_ticker' | 'side' | 'ratio'> & Partial<OptionCandidateLeg>>): Record<string, ReactNode> {
  const entries = legs.map(leg => ({ leg, values: optionLegMarketValues(leg) }))
  return Object.fromEntries(optionMarketColumns.map(column => [column.key, entries.length ? entries.map(({ leg, values }) =>
    <div className="oa-market-leg" key={leg.leg_index} title={leg.contract_ticker}><small>{leg.side} {leg.ratio}</small><span>{values[column.key === 'leg_prices' ? 'option_price' : column.key]}</span></div>
  ) : 'Unavailable']))
}

function PackagePrice({ netPremium, legs }: { netPremium: unknown; legs: Parameters<typeof optionPackagePrice>[1] }) {
  const price = optionPackagePrice(netPremium, legs)
  return <span className="oa-package-price" title="Original retained package price at the recorded leg ratios; not a current quote or fill">
    {price.price == null ? 'Unavailable' : optionPackagePremium(price.price)}
    {price.amount != null && <small>{optionPackagePremium(price.amount)} / package</small>}
    {price.reason && <small>{label(price.reason)}</small>}
  </span>
}

function planCells(netPremium: unknown, legs: Parameters<typeof optionPackagePrice>[1], management: Record<string, unknown> | undefined,
  options: Parameters<typeof optionPlanTerms>[3], maximumLoss: unknown, cautions: string[] = []): Record<string, ReactNode> {
  const plan = optionPlanTerms(netPremium, legs, management, options)
  const threshold = (price: number | null, amount: number | null) => <span title={`${label(plan.basis)} / ${label(plan.closeKind)} / Not a guaranteed fill`}>
    {price == null ? 'Unavailable' : money(price)}<small>{amount == null ? 'No recorded threshold' : `${money(amount)} / package`}</small>
  </span>
  const reasons = [...new Set([...plan.reasons, ...cautions])]
  return {
    plan_stop: threshold(plan.stopPrice, plan.stop), plan_target: threshold(plan.targetPrice, plan.target),
    risk_to_stop: <span title={label(plan.basis)}>{money(plan.risk)}<small>{optionAlertPercent(plan.riskFraction)} of entry basis</small></span>,
    reward_risk: plan.rewardRisk == null ? 'Unavailable' : `${plan.rewardRisk.toFixed(2)}x`, maximum_hold: plan.hold,
    risk_assessment: <span className="oa-plan-cautions" title={reasons.map(label).join(' / ')}>
      {plan.reasons.length ? 'Incomplete plan' : 'Indicative plan'}<small>Max loss {money(maximumLoss)}</small><small>Stop not guaranteed</small>
      {reasons.slice(0, 2).map(reason => <small key={reason}>{label(reason)}</small>)}{reasons.length > 2 && <small>+{reasons.length - 2} cautions</small>}
    </span>,
  }
}

function currentMarkCells(row: Pick<OptionAlertHistoryRow, 'current_mark' | 'original_economics' | 'hit_count'> & { exit_deadline?: string }): Record<string, ReactNode> {
  const mark = row.current_mark
  const valid = mark && mark.status !== 'UNAVAILABLE'
  const packageValue = valid && mark.signed_package_mark != null ? -Number(mark.signed_package_mark) : null
  const shareValue = valid && mark.package_price != null ? -Number(mark.package_price) : null
  const short = Number(row.original_economics.net_premium) > 0
  const amount = (value: number | null) => value == null || !Number.isFinite(value) ? 'Unavailable' : money(short ? -value : value)
  return {
    current_price: <span>{amount(shareValue)}<small>{amount(packageValue)} / package</small><small>{short ? 'Buy-to-close cost' : 'Sell-to-close value'}</small></span>,
    price_pnl: <span title="Gross marked P/L / absolute original net premium; no fills or stop/target exit simulation">{optionAlertPercent(valid ? mark.price_return : null)}</span>,
    net_return: <span title="Net marked P/L / original capital at risk; commission included, slippage unavailable">{optionAlertPercent(valid ? mark.net_return : null)}</span>,
    gross_pnl: money(valid ? mark.gross_pnl : null), net_pnl: money(valid ? mark.net_pnl : null), estimated_cost: money(valid ? mark.estimated_cost : null),
    mark_time: <>{time(mark?.market_time)}<small>{mark?.age_seconds == null ? 'Age unavailable' : `${(mark.age_seconds / 3600).toFixed(1)} hours old`}</small></>,
    mark_status: <><Status value={mark?.status || 'UNAVAILABLE'} />{mark?.reason && <small>{label(mark.reason)}</small>}{mark?.after_exit_deadline && <small>After planned exit deadline</small>}</>,
    hit_count: row.hit_count == null ? 'Unavailable' : <span title="Distinct recorded source windows for this exact frozen plan; not independent samples">{row.hit_count}</span>,
    remaining_hold: optionRemainingHold(row.exit_deadline),
  }
}

function Status({ value }: { value: string }) {
  const good = ['SATISFIED', 'OPEN', 'PASS', 'READY', 'AVAILABLE', 'ELIGIBLE_RESEARCH', 'TIMELY'].includes(value)
  const inactive = ['ELAPSED', 'EXPIRED', 'NOT_APPLICABLE', 'SUPPRESSED'].includes(value)
  const Icon = good ? CheckCircle2 : inactive ? Clock : ShieldAlert
  return <span className={`oa-status ${good ? 'oa-status--good' : inactive ? 'oa-status--muted' : 'oa-status--warn'}`}><Icon size={12} aria-hidden="true" />{label(value)}</span>
}

function Notice({ children, retry }: { children: ReactNode; retry?: () => void }) {
  return <div className="sd-notice" role="alert"><AlertTriangle size={16} aria-hidden="true" /><span>{children}</span>{retry && <button type="button" onClick={retry}>Retry</button>}</div>
}

function Rules({ values }: { values: Record<string, unknown> | null | undefined }) {
  if (!values || !Object.keys(values).length) return <p className="oa-muted">No retained management rules</p>
  return <dl className="oa-facts">{Object.entries(values).map(([key, value]) => <div key={key}><dt>{label(key)}</dt><dd>{value == null ? 'Unavailable' : typeof value === 'object' ? JSON.stringify(value) : String(value)}</dd></div>)}</dl>
}

function Economics({ values }: { values: Record<string, unknown> }) {
  return <dl className="oa-facts oa-economics">{[
    ['net_premium', 'Net premium / package'], ['capital_at_risk', 'Model capital at risk'],
    ['maximum_loss', 'Model maximum loss'], ['maximum_profit', 'Model maximum profit'],
    ['collateral_required', 'Collateral required'],
  ].map(([key, title]) => <div key={key}><dt>{title}</dt><dd>{money(values[key])}</dd></div>)}
    <div><dt>Model breakevens</dt><dd>{Array.isArray(values.breakevens) && values.breakevens.length ? values.breakevens.map(value => money(value)).join(', ') : 'Unavailable'}</dd></div>
    <div><dt>Model return on risk</dt><dd>{optionAlertPercent(values.return_on_risk)}</dd></div>
  </dl>
}

function Legs({ rows }: { rows: Pick<OptionCandidateLeg, 'leg_index' | 'contract_ticker' | 'side' | 'ratio' | 'multiplier' | 'expiration_date' | 'strike' | 'contract_type' | 'model_mark' | 'source_market_time'>[] }) {
  if (!rows.length) return <p className="oa-muted">No package legs</p>
  return <div className="oa-leg-scroll" tabIndex={0} role="region" aria-label="Exact option package legs"><table className="oa-legs"><thead><tr><th>Contract</th><th>Side / ratio</th><th>Expiry</th><th>Strike</th><th>Multiplier</th><th>Model mark / share</th><th>Source (ET)</th></tr></thead><tbody>{rows.map(leg => <tr key={leg.leg_index}>
    <td><strong>{leg.contract_type}</strong><small>{leg.contract_ticker}</small></td><td>{leg.side} / {leg.ratio}</td><td>{leg.expiration_date}</td><td>{money(leg.strike)}</td><td>{leg.multiplier}</td><td>{money(leg.model_mark)}</td><td>{time(leg.source_market_time)}</td>
  </tr>)}</tbody></table></div>
}

function GateEvidence({ gate }: { gate: OptionStockBehaviorGateEvidence }) {
  return <tr><td><strong>{optionGateTimeframe(gate.component_key)}</strong><small>{gate.component_key || 'Assessment scope'}</small></td><td>{label(gate.factor || gate.gate_id)}<small>{label(gate.metric_id || gate.gate_id)}</small></td><td><Status value={gate.verdict} /><small>{label(gate.requirement)}</small></td><td>{optionGateValue(gate)}<small>{gate.threshold_float == null ? label(gate.comparator) : `${label(gate.comparator)} ${gate.threshold_float}`}</small></td><td>{gate.source_market_times.length ? gate.source_market_times.map(value => time(value)).join(', ') : 'Unavailable'}<small>{gate.reason_codes.length ? gate.reason_codes.map(label).join(' / ') : 'No gate reason codes'}</small></td></tr>
}

function StockBehaviorEvidence({ evidence }: { evidence: OptionCandidateResearchEvidence | undefined }) {
  const source = evidence?.stock_behavior
  const assessment = source?.assessment
  return <section aria-label="Persisted stock behavior evidence"><div className="oa-section-heading"><h3>Stock behavior evidence</h3><Status value={assessment?.disposition || source?.availability || 'UNAVAILABLE'} /></div>
    {!source || source.availability === 'UNAVAILABLE' || !assessment ? <Notice>{label(source?.reason || 'RESEARCH_EVIDENCE_VERSION_UNAVAILABLE')}</Notice> : <>
      <dl className="oa-facts"><div><dt>Target horizon</dt><dd>{label(assessment.target_horizon)}</dd></div><div><dt>Stock source cutoff (ET)</dt><dd>{time(assessment.stock_market_cutoff)}</dd></div><div><dt>Assessment decision (ET)</dt><dd>{time(assessment.decision_at)}</dd></div><div><dt>Alignment</dt><dd>{label(assessment.behavior_alignment_state || 'UNAVAILABLE')}</dd></div></dl>
      <div className="oa-leg-scroll" tabIndex={0} role="region" aria-label="Daily hourly and 30 minute stock behavior gates"><table className="oa-legs oa-gates"><thead><tr><th>Timeframe</th><th>Factor / metric</th><th>Gate</th><th>Observed value</th><th>Source / reasons</th></tr></thead><tbody>{assessment.gates.map(gate => <GateEvidence key={gate.gate_id} gate={gate} />)}</tbody></table></div>
      <details className="oa-provenance"><summary>Stock evidence provenance</summary><dl className="oa-facts"><div><dt>Detector policy</dt><dd>{assessment.detector_policy_version || 'Unavailable'}<code>{assessment.detector_policy_sha256}</code></dd></div><div><dt>Stock profile</dt><dd>{assessment.stock_profile || 'Unavailable'}</dd></div><div><dt>Collection launch</dt><dd>{assessment.launch_id || 'Unavailable'}<code>{assessment.launch_manifest_sha256}</code></dd></div><div><dt>Payload SHA256</dt><dd><code>{source.payload_sha256}</code></dd></div><div><dt>Recorded (ET)</dt><dd>{time(source.recorded_at)}</dd></div><div><dt>Assessment only</dt><dd>{assessment.assessment_only ? 'Yes' : 'No'}</dd></div><div><dt>Execution permission</dt><dd>{assessment.execution_permission ? 'Yes' : 'No'}</dd></div></dl></details>
    </>}
  </section>
}

function PackageEvidence({ evidence }: { evidence: OptionCandidateResearchEvidence | undefined }) {
  const source = evidence?.package_assessment
  const assessment = source?.assessment
  const pack = assessment?.package
  return <section aria-label="Persisted package reward and risk evidence"><div className="oa-section-heading"><h3>Persisted package reward / risk</h3><Status value={assessment?.status || source?.availability || 'UNAVAILABLE'} /></div>
    {!source || source.availability === 'UNAVAILABLE' || !assessment ? <Notice>{label(source?.reason || 'RESEARCH_EVIDENCE_VERSION_UNAVAILABLE')}</Notice> : assessment.status !== 'READY' || !pack ? <Notice>{assessment.reason_codes.map(label).join(' / ') || 'Package assessment unavailable'}</Notice> : <>
      <dl className="oa-facts oa-economics"><div><dt>Net premium / package</dt><dd>{money(pack.net_premium)}</dd></div><div><dt>Capital at risk / package</dt><dd>{money(pack.capital_at_risk)}</dd></div><div><dt>Maximum loss / package</dt><dd>{pack.maximum_loss_status === 'BOUNDED' ? money(pack.maximum_loss) : label(pack.maximum_loss_status)}</dd></div><div><dt>Maximum profit / package</dt><dd>{pack.maximum_profit_status === 'BOUNDED' ? money(pack.maximum_profit) : label(pack.maximum_profit_status)}</dd></div></dl>
      <dl className="oa-facts"><div><dt>Entry basis</dt><dd>{label(pack.entry_basis)}</dd></div><div><dt>Valuation time (ET)</dt><dd>{time(pack.market_time)}</dd></div><div><dt>Assessment time (ET)</dt><dd>{time(assessment.assessed_at)}</dd></div><div><dt>Breakevens</dt><dd>{pack.breakevens.length ? pack.breakevens.map(money).join(', ') : 'Unavailable'}</dd></div></dl>
      <details className="oa-provenance"><summary>Package evidence provenance</summary><dl className="oa-facts"><div><dt>Package terms SHA256</dt><dd><code>{assessment.package_terms_sha256}</code></dd></div><div><dt>Payload SHA256</dt><dd><code>{source.payload_sha256}</code></dd></div><div><dt>Assessment policy</dt><dd>{assessment.assessment_policy_version}</dd></div><div><dt>Valuation policy SHA256</dt><dd><code>{assessment.valuation_policy_sha256}</code></dd></div></dl></details>
    </>}
  </section>
}

function ScenarioEvidence({ rows }: { rows: OptionCandidateDetailData['scenarios'] }) {
  const percent = (value: number, digits = 1) => Number(value).toLocaleString('en-US', { style: 'percent', maximumFractionDigits: digits })
  return <section aria-label="Retained option package scenarios"><h3>Package scenarios and assumptions</h3>{rows.length ? <details className="oa-scenario-set"><summary>Inspect {rows.length} retained scenarios</summary><div className="oa-scenarios">{rows.map(row => <details key={row.scenario_result_id}><summary><span>Spot {percent(row.spot_shock_fraction)} / IV {percent(row.iv_shock_fraction)} / time {percent(row.time_fraction_remaining, 0)}</span><strong>{row.profit_loss == null ? 'P/L unavailable' : `${money(row.profit_loss)} P/L / package`}</strong></summary><dl className="oa-facts"><div><dt>Scenario key</dt><dd><code>{row.scenario_key}</code></dd></div><div><dt>Repriced value / package</dt><dd>{money(row.repriced_value)}</dd></div><div><dt>Terminal valuation</dt><dd>{row.terminal ? 'Yes' : 'No'}</dd></div><div><dt>Quality flags</dt><dd>{row.quality_flags.length ? row.quality_flags.map(label).join(' / ') : 'None'}</dd></div></dl><Rules values={row.assumptions} /></details>)}</div></details> : <p className="oa-muted">No retained scenario evidence</p>}</section>
}

function RecordedDecisionEvidence({ data }: { data: OptionCandidateDetailData }) {
  const facts = optionCandidateBoardFacts(data)
  const gates = data.execution_gates || []
  const events = data.market_event_evidence || []
  const coverage = data.event_coverage_evidence || []
  return <>
    <section aria-label="Original selection evidence"><h3>Original selection evidence</h3><Rules values={facts.selection} /></section>
    <section aria-label="Original trend and context"><h3>Trend and context</h3><Rules values={facts.context} /></section>
    <section aria-label="Recorded event calendar evidence"><h3>Event calendar evidence</h3>
      {events.length ? <div className="oa-leg-scroll" tabIndex={0} role="region" aria-label="Retained calendar events"><table className="oa-legs"><thead><tr>
        <th>Event / underlying</th><th>Status / confidence</th><th>Scheduled (ET)</th><th>Source / source observed (ET)</th><th>Received (ET)</th>
      </tr></thead><tbody>{events.map(event => <tr key={event.market_event_id}>
        <td>{label(event.event_type)}<small>{event.affected_underlying || 'Market-wide'}</small></td>
        <td>{label(event.status)}<small>{label(event.confidence)}</small></td><td>{time(event.scheduled_time)}</td>
        <td>{event.source}<small>{event.source_key}</small><small>{time(event.source_observed_at)}</small></td><td>{time(event.first_observed_at)}</td>
      </tr>)}</tbody></table></div> : <p className="oa-muted">No retained event rows.</p>}
      {coverage.length ? <div className="oa-leg-scroll" tabIndex={0} role="region" aria-label="Retained event coverage windows"><table className="oa-legs"><thead><tr>
        <th>Coverage / underlying</th><th>Window start (ET)</th><th>Window end (ET)</th><th>Source / source observed (ET)</th><th>Received (ET)</th>
      </tr></thead><tbody>{coverage.map(item => <tr key={item.coverage_id}>
        <td>{label(item.event_type)}<small>{item.affected_underlying || 'Market-wide'}</small></td>
        <td>{time(item.window_start)}</td><td>{time(item.window_end)}</td><td>{item.source}<small>{item.source_key}</small><small>{time(item.source_observed_at)}</small></td><td>{time(item.first_observed_at)}</td>
      </tr>)}</tbody></table></div> : <p className="oa-muted">Calendar coverage unavailable. No event rows does not establish a clear calendar.</p>}
    </section>
    <section aria-label="Original execution gate evidence"><h3>Original execution gates</h3>
      {gates.length ? <div className="oa-leg-scroll" tabIndex={0} role="region" aria-label="Retained execution gate verdicts"><table className="oa-legs"><thead><tr>
        <th>Gate</th><th>Verdict</th><th>Reasons</th><th>Ledger version</th><th>Evaluated (ET)</th>
      </tr></thead><tbody>{gates.map(gate => <tr key={`${gate.ledger_version}:${gate.gate_name}`}>
        <td>{label(gate.gate_name)}</td><td><Status value={gate.verdict} /></td><td>{gate.reason_codes.length ? gate.reason_codes.map(label).join(' / ') : 'None recorded'}</td>
        <td>{gate.ledger_version}</td><td>{time(gate.evaluated_at)}</td>
      </tr>)}</tbody></table></div> : <p className="oa-muted">Original gate verdicts were not recorded for this candidate.</p>}
      <Rules values={facts.execution} />
      <p className="oa-permission"><ShieldAlert size={14} />Original recorded decision / Not current qualification / No execution permission</p>
    </section>
  </>
}

function CandidateDetail({ candidateId, close }: { candidateId: string; close: () => void }) {
  const detail = useQuery({ queryKey: ['option-alert-candidate', candidateId], queryFn: () => getOptionCandidate(candidateId), staleTime: 0, refetchOnMount: 'always', retry: false, refetchOnWindowFocus: false })
  const [form, setForm] = useState<OptionAlertForm>({ ...emptyOptionAlertForm })
  const [formError, setFormError] = useState('')
  const [submission, setSubmission] = useState<{ request: OptionAlertPreviewRequest; form: string; revision: number }>({ request: {}, form: JSON.stringify(emptyOptionAlertForm), revision: 0 })
  const preview = useQuery({ queryKey: ['option-alert-preview', candidateId, submission.request, submission.revision], queryFn: () => previewOptionAlert(candidateId, submission.request), staleTime: 0, refetchOnMount: 'always', retry: false, refetchOnWindowFocus: false })
  const data = detail.data?.available ? detail.data.data : undefined
  const candidate = data?.candidate
  const assessment = preview.data?.available ? preview.data.data : undefined
  const dirty = JSON.stringify(form) !== submission.form
  const edit = (field: keyof OptionAlertForm, value: string) => { setForm(previous => ({ ...previous, [field]: value })); setFormError('') }
  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (!candidate || preview.isFetching) return
    try {
      const request = optionAlertPreviewRequest(form, candidate.strategy_name)
      setSubmission(previous => ({ request, form: JSON.stringify(form), revision: previous.revision + 1 }))
      setFormError('')
    } catch (error) { setFormError(error instanceof Error ? error.message : 'Invalid preview terms') }
  }
  const canManage = candidate?.strategy_name === 'DIRECTIONAL_LONG_PREMIUM' || candidate?.strategy_name === 'DIRECTIONAL_DEBIT_SPREAD'
  return <Modal title="Option candidate details" close={close} wide><div className="oa-detail">
    {detail.isPending && <div className="oa-loading" role="status">Loading candidate...</div>}
    {detail.isError && <Notice retry={() => { void detail.refetch() }}>Candidate request failed.</Notice>}
    {detail.data && !detail.data.available && <Notice>{label(detail.data.reason || 'CANDIDATE_UNAVAILABLE')}</Notice>}
    {candidate && <>
      <div className="oa-detail-heading"><div><h3>{candidate.underlying} <span>{label(candidate.structure_type)}</span></h3><p>{candidate.display_name || label(candidate.strategy_name)} <span className="oa-muted">/ {candidate.strategy_version}</span></p></div><Status value={candidate.status} /></div>
      <dl className="oa-facts"><div><dt>Source market time (ET)</dt><dd>{time(candidate.market_data_time)}</dd></div><div><dt>Originally observed (ET)</dt><dd>{time(candidate.observed_time)}</dd></div><div><dt>Original entry deadline (ET)</dt><dd>{time(candidate.valid_until)}</dd></div><div><dt>Entry window</dt><dd><Status value={optionEntryWindow(candidate.valid_until)} /></dd></div></dl>
      <section aria-label="Detector category and structure"><h3>Discovery identity</h3><dl className="oa-facts"><div><dt>Detector</dt><dd>{data?.research_evidence?.detector.display_name || candidate.display_name}<small>{data?.research_evidence?.detector.id || candidate.strategy_name}</small></dd></div><div><dt>Category</dt><dd>{(data?.research_evidence?.category_ids || candidate.persona_tags).map(label).join(', ') || 'Unavailable'}</dd></div><div><dt>Structure</dt><dd>{label(data?.research_evidence?.structure || candidate.structure_type)}</dd></div><div><dt>Output</dt><dd>{label(data?.research_evidence?.detector.output_kind || (candidate.candidate_kind === 'RESEARCH_ONLY' ? 'OBSERVATION' : 'STRUCTURED_PACKAGE'))}</dd></div></dl></section>
      {candidate.candidate_kind === 'RESEARCH_ONLY' ? <section aria-label="Observation evidence"><h3>Observation output</h3><p className="oa-muted">{candidate.source_contract_ticker || 'Source contract unavailable'} / No structured package</p><Rules values={candidate.primary_evidence} /></section> : <section aria-label="Package economics"><h3>Indicative structured package</h3><dl className="oa-facts"><div><dt>Original package price / share</dt><dd><PackagePrice netPremium={candidate.net_premium} legs={data?.legs || []} /></dd></div></dl><Legs rows={data?.legs || []} /><Economics values={candidate as unknown as Record<string, unknown>} /></section>}
      <StockBehaviorEvidence evidence={data?.research_evidence} />
      {candidate.candidate_kind !== 'RESEARCH_ONLY' && <><PackageEvidence evidence={data?.research_evidence} /><ScenarioEvidence rows={data?.scenarios || []} /></>}
      {data && <RecordedDecisionEvidence data={data} />}
      <section aria-label="Calibrated probability"><div className="oa-section-heading"><h3>Probability</h3><Status value={data?.research_evidence?.probability.status || 'UNAVAILABLE'} /></div><Notice>{label(data?.research_evidence?.probability.reason || 'NO_QUALIFIED_CALIBRATION_REPORT')}</Notice><p className="oa-permission"><ShieldAlert size={14} aria-hidden="true" />Probability unavailable / No calibration claim / No execution permission</p></section>
      <section aria-label="Candidate qualification"><div className="oa-section-heading"><h3>Qualification</h3><span className="oa-muted">{assessment ? `${time(assessment.assessed_at)} ET` : ''}</span></div>
        {preview.isFetching && <p className="oa-muted" role="status">Assessing retained evidence...</p>}
        {preview.isError && <Notice retry={() => { void preview.refetch() }}>Qualification request failed. No preview result is available.</Notice>}
        {preview.data && !preview.data.available && <Notice>{label(preview.data.reason || 'ASSESSMENT_UNAVAILABLE')}</Notice>}
        {assessment && <><div className="oa-assessments">{assessment.qualification.assessments.map(item => <div key={item.capability}><strong>{capabilityLabels[item.capability]}</strong><Status value={item.status} /><p>{item.blocking_inputs.length ? item.blocking_inputs.map(label).join(' / ') : 'No blocking inputs for this capability'}</p></div>)}</div>
          <div className="oa-section-heading"><h3>Plan blockers</h3><Status value={preview.isFetching ? 'ASSESSING' : dirty ? 'UNASSESSED_CHANGES' : assessment.status} /></div>
          {assessment.blockers.length ? <ul className="oa-blockers">{assessment.blockers.map((item, index) => <li key={`${item.code}-${index}`}><AlertTriangle size={13} aria-hidden="true" /><span>{label(item.code)}{item.field ? `: ${label(item.field)}` : ''}{item.inputs?.length ? `: ${item.inputs.map(label).join(', ')}` : ''}{item.message ? `: ${item.message}` : ''}</span></li>)}</ul> : <p className="oa-muted">No planning blockers at assessment time</p>}
          <p className="oa-permission"><ShieldAlert size={14} aria-hidden="true" />Not published / No paper position / No execution permission</p>
        </>}
      </section>
      <section><h3>Original management</h3><span className="oa-muted">{candidate.management_policy_version || 'No policy version'}</span><Rules values={candidate.management_policy} /></section>
      {candidate.candidate_kind !== 'RESEARCH_ONLY' && <section><h3>Proposed plan terms</h3><form onSubmit={submit}>
        <div className="oa-form-grid">
          <label>Entry deadline (UTC)<input type="datetime-local" value={form.entryDeadline} onChange={event => edit('entryDeadline', event.target.value)} /></label>
          <label>Exit deadline (UTC)<input type="datetime-local" value={form.exitDeadline} onChange={event => edit('exitDeadline', event.target.value)} /></label>
          <label>Entry limit (USD / package)<input type="number" min="0.01" step="0.01" value={form.entryLimit} onChange={event => edit('entryLimit', event.target.value)} /></label>
          <label>Management source<select aria-label="Management source" value={form.management} onChange={event => edit('management', event.target.value)}><option value="ORIGINAL">Original candidate rules</option>{canManage && <option value="EXPLICIT">Explicit development policy</option>}</select></label>
        </div>
        {form.management === 'EXPLICIT' && <fieldset className="oa-management"><legend>Development policy / Not calibrated</legend><div className="oa-form-grid">
          <label>Policy version<input value={form.policyVersion} maxLength={80} required onChange={event => edit('policyVersion', event.target.value)} /></label>
          <label>Stop loss (% of debit)<input type="number" min="0.01" max="99.99" step="0.01" required value={form.stopPercent} onChange={event => edit('stopPercent', event.target.value)} /></label>
          <label>Take profit (% of debit)<input type="number" min="0.01" step="0.01" required value={form.targetPercent} onChange={event => edit('targetPercent', event.target.value)} /></label>
          <label>Maximum hold (elapsed hours)<input type="number" min="0.01" max="8784" step="0.01" required value={form.holdHours} onChange={event => edit('holdHours', event.target.value)} /></label>
          <label>Minimum exit DTE<input type="number" min="1" step="1" required value={form.exitDte} onChange={event => edit('exitDte', event.target.value)} /></label>
        </div></fieldset>}
        {formError && <Notice>{formError}</Notice>}
        <div className="oa-form-actions"><button type="submit" className="oa-command" aria-disabled={preview.isFetching}><Eye size={15} aria-hidden="true" />Preview plan</button><button type="button" className="oa-icon" title="Clear proposed terms" aria-label="Clear proposed terms" onClick={() => { setForm({ ...emptyOptionAlertForm }); setFormError('') }}><RotateCcw size={15} /></button>{dirty && <span className="oa-muted">Unassessed changes</span>}</div>
      </form></section>}
      {assessment?.plan_preview && !dirty && !preview.isFetching && <section><h3>Indicative plan preview / Unpublished</h3><dl className="oa-facts"><div><dt>Plan ID</dt><dd><code>{assessment.plan_preview.plan_id}</code></dd></div><div><dt>Plan SHA256</dt><dd><code>{assessment.plan_preview.plan_sha256}</code></dd></div></dl>{assessment.proposed_management_policy && <Rules values={assessment.proposed_management_policy} />}</section>}
      <details className="oa-provenance"><summary>Original evidence and provenance</summary><dl className="oa-facts"><div><dt>Candidate ID</dt><dd><code>{candidateId}</code></dd></div><div><dt>Matrix ID</dt><dd><code>{candidate.matrix_id}</code></dd></div><div><dt>Strategy policy SHA256</dt><dd><code>{candidate.policy_sha256}</code></dd></div><div><dt>Model</dt><dd>{candidate.model_version}</dd></div></dl><Rules values={candidate.primary_evidence} />{candidate.reason_codes.length > 0 && <ul>{candidate.reason_codes.map(reason => <li key={reason}>{label(reason)}</li>)}</ul>}</details>
    </>}
  </div></Modal>
}

function PublicationDetail({ row, close, openCandidate }: { row: OptionAlertHistoryRow; close: () => void; openCandidate: () => void }) {
  const marks = currentMarkCells(row)
  const terms = planCells(row.original_economics.net_premium, row.legs, row.management_policy,
    { version: row.management_policy_version, source: row.management_source, entryLimit: row.entry_limit, entryKind: row.entry_limit_kind, exitDeadline: row.exit_deadline }, row.original_economics.maximum_loss)
  return <Modal title="Published option event" close={close} wide><div className="oa-detail">
    <div className="oa-detail-heading"><div><h3>{row.underlying} <span>{label(row.structure)}</span></h3><p>{label(row.strategy)}</p></div><Status value={row.event_type} /></div>
    <dl className="oa-facts"><div><dt>Recorded (ET)</dt><dd>{time(row.recorded_at)}</dd></div><div><dt>Source market time (ET)</dt><dd>{time(row.source_market_time)}</dd></div><div><dt>Entry deadline (ET)</dt><dd>{time(row.entry_deadline)}</dd></div><div><dt>Exit deadline (ET)</dt><dd>{time(row.exit_deadline)}</dd></div><div><dt>{label(row.entry_limit_kind)} / package</dt><dd>{money(row.entry_limit)}</dd></div><div><dt>Entry window</dt><dd><Status value={optionEntryWindow(row.entry_deadline)} /></dd></div></dl>
    <h3>Frozen package</h3><dl className="oa-facts"><div><dt>Original package price / share</dt><dd><PackagePrice netPremium={row.original_economics.net_premium} legs={row.legs} /></dd></div></dl><Legs rows={row.legs.map(leg => ({ ...leg, leg_index: leg.index, model_mark: leg.entry_model_mark }))} /><Economics values={row.original_economics} />
    <h3>Frozen management</h3><Rules values={row.management_policy} />
    <section aria-label="Frozen trade plan"><h3>Frozen trade plan</h3><dl className="oa-facts">{['plan_stop', 'plan_target', 'risk_to_stop', 'reward_risk', 'maximum_hold', 'risk_assessment'].map(key => <div key={key}><dt>{label(key)}</dt><dd>{terms[key]}</dd></div>)}</dl></section>
    <section aria-label="Retained marked performance"><h3>Retained marked performance</h3><dl className="oa-facts">{['current_price', 'price_pnl', 'gross_pnl', 'net_return', 'net_pnl', 'estimated_cost', 'mark_time', 'mark_status', 'remaining_hold', 'hit_count'].map(key => <div key={key}><dt>{label(key)}</dt><dd>{marks[key]}</dd></div>)}</dl>
      <p className="oa-permission">Original candidate-mark basis / Commission-only net return / Slippage unavailable / Ignores fills and stop/target exits</p>
      <details className="oa-provenance"><summary>Mark provenance</summary><Rules values={{ valuation_policy: row.current_mark?.valuation_policy_sha256, snapshot_ids: row.current_mark?.source_snapshot_ids, received: row.current_mark?.observed_time, freshness_limit_seconds: row.current_mark?.maximum_age_seconds }} /></details>
    </section>
    {row.request.reason && <Notice>{row.request.reason}</Notice>}
    <p className="oa-permission"><ShieldAlert size={14} />Indicative publication / Not a fill / No execution permission</p>
    <details className="oa-provenance"><summary>Publication evidence</summary><dl className="oa-facts"><div><dt>Plan ID</dt><dd><code>{row.plan_id}</code></dd></div><div><dt>Plan SHA256</dt><dd><code>{row.plan_sha256}</code></dd></div><div><dt>Event ID / sequence</dt><dd><code>{row.event_id}</code> / {row.sequence}</dd></div><div><dt>Source IDs</dt><dd><code>{row.request.source_ids.join(', ') || 'No additional event sources'}</code></dd></div></dl></details>
    <button type="button" className="oa-command" onClick={openCandidate}><Eye size={15} />Original candidate</button>
  </div></Modal>
}

const detectorNames = { O1: 'High Volume/OI Participation', O2: 'Local IV Surface Distortion',
  S1: 'Range Breakout Acceptance', S2: 'Relative Trend Resumption' }
const selectionNames: Record<string, string> = { SELECTED: 'Selected', NOT_SELECTED: 'Not selected', REPEAT: 'Repeat', OBSERVATION: 'Observation' }

function DetectorSortHeader({ title, field, params, update }: {
  title: string; field: OptionDetectorSort; params: URLSearchParams;
  update: (values: Record<string, string | null>) => void;
}) {
  const sort = optionDetectorSort(params)
  const selected = sort.sort_by === field
  const Icon = !selected ? ArrowUpDown : sort.sort_order === 'asc' ? ArrowUp : ArrowDown
  return <th scope="col" aria-sort={!selected ? 'none' : sort.sort_order === 'asc' ? 'ascending' : 'descending'}>
    <button type="button" className="oa-sort" onClick={() => update(optionDetectorSortChange(params, field))}
      title={`Sort by ${title.toLowerCase()}`} aria-label={`Sort by ${title.toLowerCase()}`}>{title}<Icon size={13} aria-hidden="true" /></button>
  </th>
}

function DetectorResultVersion({ data, params, update }: {
  data?: OptionDetectorDatasets; params: URLSearchParams; update: (values: Record<string, string | null>) => void;
}) {
  const explicit = params.get('evaluation_dataset')
  const current = !explicit || explicit === data?.current_dataset_id
  return <details className="oa-result-version"><summary>{current ? 'Current models' : 'Archived results'} / Results version</summary>
    <label>Results version<select aria-label="Results version" value={explicit || ''} onChange={event => update({ evaluation_dataset: event.target.value, session_date: null, offset: null })}>
      <option value="">Current four-model results</option>
      {data?.datasets.map(value => <option key={value} value={value}>{value === data.current_dataset_id ? `Current: ${value}` : value}</option>)}
    </select></label><code>{explicit || data?.default_dataset_id || 'No configured results version'}</code>
  </details>
}

function DetectorAlertsPanel({ view, params, update, offset, onSessionContext }: {
  view: 'behavior' | 'day_history'; params: URLSearchParams;
  update: (values: Record<string, string | null>) => void; offset: number;
  onSessionContext: (value: NonNullable<PageContextValue['alertSessions']>) => void;
}) {
  const preferences = useColumnPreferences('option-detector-alerts-v4', detectorAlertColumns)
  const columns = detectorAlertColumns.filter(column => column.locked || !preferences.hidden.has(column.key))
  const [planDetail, setPlanDetail] = useState<OptionDetectorAlertReview['rows'][number] | null>(null)
  const [presets, setPresets] = useState<DetectorViewPreset[]>([])
  const [presetError, setPresetError] = useState('')
  const [presetWritable, setPresetWritable] = useState(false)
  const [savingPreset, setSavingPreset] = useState(false)
  const [presetName, setPresetName] = useState('')
  const [selectedPreset, setSelectedPreset] = useState('')
  useEffect(() => {
    try { setPresets(readDetectorPresets(localStorage)); setPresetWritable(true) }
    catch (error) { setPresetError(error instanceof Error ? error.message : 'Saved presets unavailable') }
  }, [])
  const inventory = useQuery({ queryKey: ['option-detector-datasets'], queryFn: getOptionDetectorDatasets,
    retry: false, refetchInterval: 30_000 })
  const index = inventory.data?.available ? inventory.data.data : undefined
  const dataset = params.get('evaluation_dataset') || index?.default_dataset_id || ''
  const rawDetector = params.get('evaluation_detector')
  const detector = rawDetector === 'O1' || rawDetector === 'S1' || rawDetector === 'S2' ? rawDetector : undefined
  const underlyer = useDeferredValue((params.get('underlyer') || '').trim().toUpperCase())
  const request = { dataset_id: dataset, scope: view === 'behavior' ? 'LATEST' : 'HISTORY',
    session_date: params.get('session_date') || undefined, detector, underlyer: underlyer || undefined,
    ...optionDetectorSort(params), limit: pageSize, offset } as const
  const review = useQuery({ queryKey: ['option-detector-alerts', request], queryFn: () => getOptionDetectorAlerts(request),
    enabled: Boolean(dataset), retry: false, refetchInterval: 30_000 })
  const data = review.data?.available ? review.data.data : undefined
  const failed = inventory.isError || inventory.data && !inventory.data.available || index?.storage_ready === false
    || review.isError || review.data && !review.data.available
  useEffect(() => { setPlanDetail(null) }, [params])
  useEffect(() => {
    onSessionContext({ dates: data?.sessions || [], selected: data?.session_date || '', source: 'OPTIONS_DETECTOR_ALERTS',
      emptyLabel: 'No completed detector runs', resetKeys: ['candidate', 'event'] })
  }, [data, onSessionContext])
  const refresh = () => { void inventory.refetch(); if (dataset) void review.refetch() }
  const applyColumns = (keys: string[]) => {
    detectorAlertColumns.filter(column => !column.locked).forEach(column => {
      if (preferences.hidden.has(column.key) === keys.includes(column.key)) preferences.toggle(column.key)
    })
  }
  const columnPreset = Object.entries(detectorColumnPresets).find(([, keys]) => detectorAlertColumns.every(column =>
    Boolean(column.locked) || keys.includes(column.key) === !preferences.hidden.has(column.key)))?.[0] || ''
  const applyPreset = (value: string) => {
    setSelectedPreset(value)
    const preset = presets.find(item => `saved:${item.name}` === value)
    if (!preset) return
    applyColumns(preset.columns)
    update({ underlyer: null, evaluation_detector: null, detector_sort: null, detector_order: null, ...preset.query, offset: null })
  }
  const savePreset = (event: FormEvent) => {
    event.preventDefault()
    try {
      const preset = detectorViewPreset(presetName, params, columns.map(column => column.key))
      const next = [...readDetectorPresets(localStorage).filter(item => item.name !== preset.name), preset]
      writeDetectorPresets(localStorage, next); setPresets(next); setSelectedPreset(`saved:${preset.name}`); setSavingPreset(false); setPresetError('')
    } catch (error) { setPresetError(error instanceof Error ? error.message : 'Preset was not saved') }
  }
  const deletePreset = () => {
    try {
      const next = readDetectorPresets(localStorage).filter(item => `saved:${item.name}` !== selectedPreset)
      writeDetectorPresets(localStorage, next); setPresets(next); setSelectedPreset(''); setPresetError('')
    } catch (error) { setPresetError(error instanceof Error ? error.message : 'Preset was not deleted') }
  }
  const sortable = new Set(['underlyer', 'detector', 'category', 'strategy', 'run', 'entry_limit'])
  return <section className="oa-evaluation" aria-label={view === 'behavior' ? 'Latest detector alerts' : 'Detector alert day history'}>
    <div className="sd-filters oa-filters oa-detector-filters">
      {presets.length > 0 && <label>Saved views<select aria-label="Saved alert view" value={selectedPreset} onChange={event => applyPreset(event.target.value)}>
        <option value="">Select saved view</option>
        {presets.map(preset => <option key={preset.name} value={`saved:${preset.name}`}>{preset.name}</option>)}
      </select></label>}
      <label>Underlying<input aria-label="Detector alert underlying" type="search" placeholder="All underlyings" maxLength={12}
        value={params.get('underlyer') || ''} onChange={event => { setSelectedPreset(''); update({ underlyer: event.target.value.toUpperCase() }) }} /></label>
      <label>Model<select aria-label="Detector alert model" value={detector || ''} onChange={event => { setSelectedPreset(''); update({ evaluation_detector: event.target.value }) }}>
        <option value="">All models</option>{(['O1', 'S1', 'S2'] as const).map(id => <option key={id} value={id}>{detectorNames[id]}</option>)}
      </select></label>
      <label>Column preset<select aria-label="Alert column preset" value={columnPreset} onChange={event => { setSelectedPreset(''); const keys = detectorColumnPresets[event.target.value]; if (keys) applyColumns(keys) }}>
        <option value="">Custom columns</option>{Object.keys(detectorColumnPresets).map(name => <option key={name} value={name}>{name}</option>)}
      </select></label>
      <div className="oa-detector-tools"><ColumnPicker columns={detectorAlertColumns} hidden={preferences.hidden}
        onToggle={key => { setSelectedPreset(''); preferences.toggle(key) }} onShowAll={() => { setSelectedPreset(''); preferences.showAll() }} onReset={() => { setSelectedPreset(''); preferences.reset() }} />
        <span className="oa-tool-label">Columns</span>
        <button type="button" className="oa-icon" title="Save filter and column preset" aria-label="Save alert preset" disabled={!presetWritable}
          onClick={() => { setPresetName(selectedPreset.startsWith('saved:') ? selectedPreset.slice(6) : ''); setSavingPreset(true) }}><Save size={15} /></button>
        <button type="button" className="oa-icon" title="Delete selected preset" aria-label="Delete alert preset" disabled={!presetWritable || !selectedPreset.startsWith('saved:')} onClick={deletePreset}><Trash2 size={15} /></button>
        <button type="button" className="oa-icon" title="Refresh detector alerts" aria-label="Refresh detector alerts"
          disabled={inventory.isFetching || review.isFetching} onClick={refresh}><RefreshCw size={15} /></button>
      </div>
    </div>
    <DetectorResultVersion data={index} params={params} update={update} />
    {presetError && <Notice>{presetError}</Notice>}
    {failed && <Notice retry={refresh}>Detector alerts are unavailable.</Notice>}
    {inventory.isPending || dataset && review.isPending ? <div className="sd-empty" role="status">Loading detector alerts...</div> : !failed && <>
      <div className="sd-metrics"><span>{data?.session_date || 'No completed detector run'}</span>
        <span><strong>{data?.new_alerts ?? 0}</strong> new alerts</span><span><strong>{data?.repeat_hits ?? 0}</strong> repeat hits</span>
        <span>Maximum 20 new alerts / run</span></div>
      <div className="sd-table-panel"><div className="sd-table-scroll" tabIndex={0} role="region" aria-label="Detector alerts table">
        <table><thead><tr>{columns.map(column => sortable.has(column.key)
          ? <DetectorSortHeader key={column.key} title={column.label} field={column.key as OptionDetectorSort} params={params} update={update} />
          : <th key={column.key} scope="col">{column.label}</th>)}</tr></thead>
          <tbody>{data?.rows.map(row => {
            const technical = row.management_policy.technical_exit as Record<string, unknown> | undefined
            const original = row.original_package
            const legs = original?.status === 'AVAILABLE' ? original.legs : []
            return <tr key={row.evaluation_id}><ColumnCells columns={columns} values={{
              underlyer: <><button type="button" className="oa-row-link" onClick={() => setPlanDetail(row)}>{row.underlyer}</button><small>{row.direction === 1 ? 'Bullish' : 'Bearish'}</small></>,
              detector: <>{detectorNames[row.detector_id]}<small>{label(row.origin)}</small></>,
              category: label(row.category), strategy: row.strategy_name ? optionPackageStrategyName(row.strategy_name) : 'Unavailable',
              contracts: legs.length ? <>{label(original?.structure_type || '')}{legs.map(leg => <small key={leg.leg_index}>{leg.side} {leg.ratio} {leg.contract_ticker}</small>)}</> : 'Unavailable',
              expiry: original?.expiration_date ? <>{original.expiration_date}<small>{original.calendar_dte ?? 'Unavailable'} DTE at source</small></> : 'Unavailable',
              ...marketCells(legs), option_price: <PackagePrice netPremium={original?.net_premium} legs={legs} />,
              ...planCells(original?.net_premium, legs, row.management_policy, { version: String(row.management_policy.policy_version || ''),
                source: 'EXPLICIT_ALERT_POLICY', entryLimit: row.entry_limit, entryKind: 'MAXIMUM_DEBIT', exitDeadline: row.exit_deadline }, original?.maximum_loss),
              capital: money(original?.capital_at_risk), maximum_loss: money(original?.maximum_loss), maximum_profit: money(original?.maximum_profit),
              breakevens: original?.breakevens?.length ? original.breakevens.map(money).join(', ') : 'Unavailable',
              outcome_status: 'Not connected',
              run: time(row.scheduled_cycle), entry_limit: <>{money(row.entry_limit)}<small>Debit / Not a fill</small></>,
              hit_count: <span title={`Last observed ${time(row.last_seen_at)} ET`}>{row.hit_count}<small>{row.repeat_count} repeats</small></span>,
              technical_stop: money(technical?.underlying_stop), technical_target: money(technical?.underlying_target),
              entry_window: <><Status value={optionEntryWindow(row.entry_deadline)} /><small>{time(row.entry_deadline)} ET</small></>,
              exit_due: time(row.exit_deadline), net_return: optionAlertPercent(row.net_return),
              details: <button type="button" className="oa-icon" title="View frozen alert plan" aria-label={`View ${row.underlyer} ${row.detector_id} alert plan`} onClick={() => setPlanDetail(row)}><Eye size={15} /></button>,
            }} /></tr>
          })}</tbody>
        </table>{!data?.rows.length && <div className="sd-empty">{!dataset ? index?.datasets.length ? 'Select a detector dataset.' : 'No detector runs have been recorded.'
          : data?.status === 'NO_COMPLETE_RUN' ? 'No completed detector run for this dataset and session.'
          : view === 'behavior' ? 'No new alerts in this completed run and filter scope.' : 'No earlier admitted alerts for this session and filter scope.'}</div>}
      </div><div className="sd-pager"><span>{data?.total ?? 0} admitted alerts</span><div>
        <button aria-label="Previous detector alerts page" disabled={!offset || review.isFetching} onClick={() => update({ offset: String(Math.max(0, offset - pageSize)) })}><ArrowLeft size={15} /></button>
        <button aria-label="Next detector alerts page" disabled={offset + pageSize >= (data?.total ?? 0) || review.isFetching} onClick={() => update({ offset: String(offset + pageSize) })}><ArrowRight size={15} /></button>
      </div></div></div>
    </>}
    {savingPreset && <Modal title="Save alert preset" close={() => setSavingPreset(false)}><form className="oa-detail" onSubmit={savePreset}>
      <label>Preset name<input aria-label="Alert preset name" value={presetName} maxLength={60} onChange={event => setPresetName(event.target.value)} required /></label>
      {presetError && <Notice>{presetError}</Notice>}<div className="oa-form-actions"><button className="oa-command" type="submit"><Save size={15} />Save preset</button></div>
    </form></Modal>}
    {planDetail && <Modal title={`${planDetail.underlyer} ${planDetail.detector_id} frozen alert`} close={() => setPlanDetail(null)}><div className="oa-detail">
      <Rules values={{ category: label(planDetail.category), package_strategy: optionPackageStrategyName(planDetail.strategy_name),
        first_selected: time(planDetail.first_selected_at), entry_limit: money(planDetail.entry_limit), entry_deadline: time(planDetail.entry_deadline),
        exit_due: time(planDetail.exit_deadline), hits: planDetail.hit_count, plan_sha256: planDetail.plan_sha256 }} />
      <h3>Original management</h3><Rules values={planDetail.management_policy} />
      <p className="oa-permission">Indicative plan / Outcomes unavailable / No fill or execution permission</p>
      <button type="button" className="oa-command" onClick={() => { update({ candidate: planDetail.candidate_id, offset: String(offset) }); setPlanDetail(null) }}><Eye size={15} />Original candidate evidence</button>
    </div></Modal>}
  </section>
}

function DetectorEvaluationPanel({ params, update, offset, onSessionContext }: {
  params: URLSearchParams; update: (values: Record<string, string | null>) => void; offset: number;
  onSessionContext: (value: NonNullable<PageContextValue['alertSessions']>) => void;
}) {
  const detector = params.get('evaluation_detector')
  const status = params.get('evaluation_status')
  const underlyer = useDeferredValue((params.get('underlyer') || '').trim().toUpperCase())
  const inventory = useQuery({ queryKey: ['option-detector-datasets'], queryFn: getOptionDetectorDatasets, retry: false, refetchInterval: 30_000 })
  const index = inventory.data?.available ? inventory.data.data : undefined
  const [observationDetail, setObservationDetail] = useState<OptionDetectorEvaluationReview['rows'][number] | null>(null)
  const request = { session_date: params.get('session_date') || undefined, dataset_id: params.get('evaluation_dataset') || index?.default_dataset_id || undefined,
    detector: detector === 'O1' || detector === 'O2' || detector === 'S1' || detector === 'S2' ? detector : undefined,
    selection_status: status === 'SELECTED' || status === 'NOT_SELECTED' || status === 'REPEAT' || status === 'OBSERVATION' ? status : undefined,
    underlyer: underlyer || undefined, ...optionDetectorSort(params), limit: pageSize, offset } as const
  const review = useQuery({ queryKey: ['option-detector-evaluations', request], queryFn: () => getOptionDetectorEvaluations(request), retry: false,
    enabled: Boolean(index), refetchInterval: 30_000 })
  const data = review.data?.available ? review.data.data : undefined
  useEffect(() => { setObservationDetail(null) }, [params])
  useEffect(() => {
    if (data) onSessionContext({ dates: data.sessions, selected: data.session_date || '', source: 'OPTIONS_DETECTOR_EVALUATIONS',
      emptyLabel: 'No evaluated sessions', resetKeys: ['candidate', 'event'] })
  }, [data, onSessionContext])
  return <section className="oa-evaluation" aria-label="Selected and non-selected detector evaluation">
    <div className="oa-section-heading"><h3>Daily model results</h3><DetectorResultVersion data={index} params={params} update={update} /></div>
    <div className="sd-filters oa-filters">
      <label>Underlying<input aria-label="Evaluation underlying" type="search" placeholder="All underlyings" maxLength={12}
        value={params.get('underlyer') || ''} onChange={event => update({ underlyer: event.target.value.toUpperCase() })} /></label>
      <label>Model<select aria-label="Evaluation model" value={detector || ''} onChange={event => update({ evaluation_detector: event.target.value, offset: null })}>
        <option value="">All four models</option>{Object.entries(detectorNames).map(([id, name]) => <option key={id} value={id}>{name}</option>)}
      </select></label>
      <label>Selection<select aria-label="Evaluation selection" value={status || ''} onChange={event => update({ evaluation_status: event.target.value, offset: null })}>
        <option value="">All retained results</option><option value="SELECTED">Selected</option><option value="NOT_SELECTED">Not selected</option><option value="REPEAT">Repeats</option><option value="OBSERVATION">Observations</option>
      </select></label>
      <button type="button" className="oa-icon" title="Refresh detector evaluation" aria-label="Refresh detector evaluation" disabled={review.isFetching} onClick={() => { void review.refetch() }}><RefreshCw size={15} /></button>
    </div>
    {(inventory.isError || inventory.data && !inventory.data.available) && <Notice>Current result version is unavailable.</Notice>}
    {(inventory.isPending || index && review.isPending) && <div className="sd-empty" role="status">Loading detector evaluation...</div>}
    {(review.isError || review.data && !review.data.available) && <Notice retry={() => { void review.refetch() }}>Detector evaluation is unavailable.</Notice>}
    {data && !data.storage_ready && <Notice>Detector evaluation storage is not deployed. No selected or overflow results have been recorded.</Notice>}
    {data?.storage_ready && <>
      <div className="sd-metrics"><span>{data.session_date || 'No evaluated session'}</span><span><strong>{data.total}</strong> evaluation records in scope</span><span>Alert limit: {data.maximum_new_alerts} new / run</span></div>
      <p className="oa-permission">Prospective outcomes are not connected. Success rates are unavailable.</p>
      <div className="sd-table-panel"><div className="sd-table-scroll" tabIndex={0} role="region" aria-label="Selection comparison summary"><table>
        <thead><tr><th>Model</th><th title="Qualified package occurrences, including repeats; O2 counts detected surface groups only.">Detected occurrences</th><th>Selected</th><th>Not selected</th><th>Repeats</th><th>Evaluations</th><th>Measured returns</th><th>Positive rate</th><th>Mean net return</th></tr></thead>
        <tbody>{(data.models || []).map(cell => <tr key={cell.detector_id}>
          <td>{detectorNames[cell.detector_id]}</td><td>{cell.detected}</td><td>{cell.detector_id === 'O2' ? 'Not applicable' : cell.selected}</td>
          <td>{cell.detector_id === 'O2' ? 'Not applicable' : cell.not_selected}</td><td>{cell.detector_id === 'O2' ? 'Not applicable' : cell.repeats}</td><td>{cell.evaluated}</td>
          <td>{cell.detector_id === 'O2' ? 'Not applicable' : cell.measured ?? 'Unavailable'}</td><td>{cell.detector_id === 'O2' ? 'Not applicable' : optionAlertPercent(cell.positive_rate)}</td><td>{cell.detector_id === 'O2' ? 'Not applicable' : optionAlertPercent(cell.mean_net_return)}</td>
        </tr>)}</tbody>
      </table></div></div>
      <div className="sd-table-panel"><div className="sd-table-scroll" tabIndex={0} role="region" aria-label="All qualified detector packages"><table>
        <thead><tr><DetectorSortHeader title="Underlying" field="underlyer" params={params} update={update} />
          <DetectorSortHeader title="Run (ET)" field="run" params={params} update={update} /><th>Selection / reason</th>
          <DetectorSortHeader title="Original rank" field="rank" params={params} update={update} />
          <DetectorSortHeader title="Entry limit / package" field="entry_limit" params={params} update={update} /><th>Net marked return</th>
          <DetectorSortHeader title="Model" field="detector" params={params} update={update} />
          <DetectorSortHeader title="Category" field="category" params={params} update={update} />
          <DetectorSortHeader title="Package strategy" field="strategy" params={params} update={update} /><th>Details</th></tr></thead>
        <tbody>{data.rows.map(row => <tr key={row.evaluation_id}>
          <td>{row.underlyer}<small>{row.direction === 1 ? 'Bullish' : row.direction === -1 ? 'Bearish' : 'Nondirectional'}</small></td>
          <td>{time(row.scheduled_cycle)}</td><td>{selectionNames[row.selection_status]}<small>{label(row.selection_reason)}</small>{row.observation && <><small>{label(row.observation.finding_disposition)}</small><small>{row.observation.findings.length} residual findings</small></>}</td>
          <td>{row.candidate_rank ?? 'Not applicable'}</td><td>{row.observation ? 'Not applicable' : <>{money(row.entry_limit)}<small>Not a fill</small></>}</td><td>{row.observation ? 'Not applicable' : optionAlertPercent(row.net_return)}</td>
          <td>{detectorNames[row.detector_id]}<small>{label(row.origin)}</small>{row.shared_model_exposure && <small>Shared exposure</small>}</td>
          <td>{label(row.category)}</td><td>{row.observation ? 'Not applicable' : row.strategy_name ? optionPackageStrategyName(row.strategy_name) : 'Unavailable'}</td>
          <td><button className="oa-icon" aria-label={`View ${row.underlyer} ${row.detector_id} evaluation package`} title={row.observation ? 'View surface observation evidence' : 'View original candidate evidence'}
            onClick={() => row.observation ? setObservationDetail(row) : update({ candidate: row.candidate_id, offset: String(offset) })}><Eye size={15} /></button></td>
        </tr>)}</tbody>
      </table>{!data.rows.length && <div className="sd-empty">No detector evaluation records for this dataset and session.</div>}</div>
        <div className="sd-pager"><span>{data.total} evaluation records</span><div>
          <button aria-label="Previous evaluation page" disabled={offset === 0 || review.isFetching} onClick={() => update({ offset: String(Math.max(0, offset - pageSize)) })}><ArrowLeft size={15} /></button>
          <button aria-label="Next evaluation page" disabled={offset + pageSize >= data.total || review.isFetching} onClick={() => update({ offset: String(offset + pageSize) })}><ArrowRight size={15} /></button>
        </div></div>
      </div>
    </>}
    <details className="oa-provenance"><summary>Legacy diagnostics</summary><button type="button" className="oa-command" onClick={() => update({ eod: 'legacy', offset: null })}>Legacy stock gates</button></details>
    {observationDetail?.observation && <Modal title={`${observationDetail.underlyer} surface observation`} close={() => setObservationDetail(null)}>
      <div className="oa-detail"><dl className="oa-facts">
        <div><dt>Finding</dt><dd>{label(observationDetail.observation.finding_disposition)}</dd></div>
        <div><dt>Stock context</dt><dd>{label(observationDetail.observation.stock_context_status)}</dd></div>
        <div><dt>Event context</dt><dd>{label(observationDetail.event_horizon_status)}</dd></div>
        <div><dt>Output</dt><dd>Observation only</dd></div>
      </dl><ul>{observationDetail.observation.reasons.map(reason => <li key={reason}>{label(reason)}</li>)}</ul>
        <div className="oa-leg-scroll" tabIndex={0} role="region" aria-label="Surface residual evidence"><table className="oa-legs">
          <thead><tr><th>Contract ID</th><th>Strike</th><th>Observed IV</th><th>Fitted IV</th><th>Residual</th><th>Robust z</th></tr></thead>
          <tbody>{observationDetail.observation.findings.map(finding => <tr key={finding.snapshot_id}><td>{finding.contract_id}</td><td>{money(finding.strike)}</td>
            <td>{optionAlertPercent(finding.local_iv)}</td><td>{optionAlertPercent(finding.fitted_iv)}</td><td>{optionAlertPercent(finding.residual)}</td><td>{finding.robust_z.toFixed(3)}</td></tr>)}</tbody>
        </table></div>
      </div>
    </Modal>}
  </section>
}

function BehaviorReviewPanel({ view, params, update, offset, columns, onSessionContext }: {
  view: 'behavior' | 'day_history' | 'daily'; params: URLSearchParams; update: (values: Record<string, string | null>) => void; offset: number; columns: ColumnSpec[];
  onSessionContext: (value: NonNullable<PageContextValue['alertSessions']>) => void;
}) {
  const daily = view === 'daily'
  const dayHistory = view === 'day_history'
  const current = view === 'behavior'
  const behaviorView = daily ? 'DAILY' : optionReviewSelection(params)
  const underlyer = useDeferredValue((params.get('underlyer') || '').trim().toUpperCase())
  const request = { behavior_view: behaviorView, review_scope: optionReviewScope(view), session_date: params.get('session_date') || undefined,
    underlyer: underlyer || undefined, strategy: params.get('strategy') || undefined, limit: pageSize, offset } as const
  const review = useQuery({ queryKey: ['option-behavior-review', request], queryFn: () => getOptionBehaviorReview(request),
    refetchInterval: daily ? false : 30_000, staleTime: 0, retry: false, enabled: !daily || params.get('eod') === 'legacy' })
  const data = review.data?.available ? review.data.data : undefined
  const baseline = data?.history_basis === 'PERSISTED_BASELINE_ALERT_MEMBERS'
  const modelNames = data?.models || {
    DIRECTIONAL_LONG_PREMIUM: 'Directional long premium', DIRECTIONAL_DEBIT_SPREAD: 'Directional debit spread',
    INCOME_WHEEL: 'Income / Wheel', SPREAD_RANGE_LOCATOR: 'Spread / Range', ZERO_DTE_GAMMA_SQUEEZE: '0DTE gamma',
  }
  useEffect(() => {
    if (data?.sessions && (!daily || params.get('eod') === 'legacy')) onSessionContext({ dates: data.sessions, selected: data.session_date,
      source: 'OPTIONS_DETECTIONS', emptyLabel: 'No completed runs', resetKeys: ['candidate', 'event'] })
  }, [data?.sessions, data?.session_date, onSessionContext, daily, params])
  const percent = (value: number | null | undefined) => value == null ? 'Unavailable' : `${(100 * value).toFixed(2)}%`
  const arms: Record<string, string> = { PERSISTED_BASELINE_ALERTS: 'First baseline alerts', ASSESSMENT_COVERED_CANDIDATE_BASELINE: 'Covered baseline', BEHAVIOR_V1_PASSED: 'Behavior passed', BEHAVIOR_V1_TIMELY: 'Timely behavior' }
  if (daily && params.get('eod') !== 'legacy') return <DetectorEvaluationPanel params={params} update={update} offset={offset} onSessionContext={onSessionContext} />
  return <>
    <div className="sd-filters oa-filters">
      {daily && <label>Review<select aria-label="EOD review scope" value="legacy" onChange={event => update({ eod: event.target.value, offset: null, candidate: null })}>
        <option value="detectors">Detector selection</option><option value="legacy">Legacy stock gates</option>
      </select></label>}
      <label>Underlying<input aria-label="Review underlying" type="search" placeholder="All underlyings" maxLength={12} value={params.get('underlyer') || ''} onChange={event => update({ underlyer: event.target.value.toUpperCase() })} /></label>
      <label>Model<select aria-label="Review model" value={params.get('strategy') || ''} onChange={event => update({ strategy: event.target.value })}>
        <option value="">All models</option>{Object.entries(modelNames).map(([id, name]) => <option key={id} value={id}>{name}</option>)}
      </select></label>
      {!daily && <label>Selection<select aria-label="Behavior result" value={behaviorView} onChange={event => update({ behavior: event.target.value })}>
        <option value="ALL">{baseline ? 'All recorded alerts' : 'Model-selected complete packages'}</option><option value="ELIGIBLE">Directional stock gates passed</option><option value="SHORTLIST">Timely directional shortlist</option>
      </select></label>}
      <button type="button" className="oa-icon" aria-label="Refresh behavior review" title="Refresh behavior review" disabled={review.isFetching} onClick={() => { void review.refetch() }}><RefreshCw size={15} /></button>
    </div>
    {review.isError && <Notice retry={() => { void review.refetch() }}>Behavior review request failed.</Notice>}
    {review.data && !review.data.available && <Notice>{label(review.data.reason || 'BEHAVIOR_REVIEW_UNAVAILABLE')}</Notice>}
    {review.isPending && <div className="sd-empty" role="status">Loading retained behavior evidence...</div>}
    {data && <>
      <div className="sd-metrics oa-funnel" aria-label="Behavior selection funnel">
        <span>{data.session_date} / {label(data.session_state)}</span>
        <span><strong>{data.funnel.candidates.toLocaleString()}</strong> {baseline ? 'alerts in filter scope' : 'selected detections'}</span>
        {baseline ? <>
          <span><strong>{data.run_funnel?.new_alerts ?? 0}</strong> new alerts</span>
          <span><strong>{data.run_funnel?.repeat_hits ?? 0}</strong> repeat hits</span>
          <span>Maximum 50 new alerts / run</span>
        </> : <>
        <span><strong>{data.funnel.applicable.toLocaleString()}</strong> profile applicable</span>
        <span><strong>{data.funnel.eligible.toLocaleString()}</strong> stock gates passed</span>
        <span><strong>{data.funnel.timely.toLocaleString()}</strong> recorded before deadline</span>
        <span><strong>{data.funnel.shortlist.toLocaleString()}</strong> timely representatives</span>
        <span><strong>{data.funnel.entry_open_now}</strong> entry windows open now</span>
        </>}
      </div>
      <p className="oa-permission"><ShieldAlert size={14} />{baseline ? 'Worker-recorded baseline alerts / Repeat hits excluded from outcome cohorts' : dayHistory ? 'Retained detections, not publication events / Repeated contract occurrences are not independent samples' : 'Retained research evidence'} / Indicative marks / No execution permission</p>
      <details className="oa-provenance"><summary>Run details</summary>
        <Rules values={{ selected_session: data.session_date, cycle: data.run ? `${time(data.run.scheduled_cycle)} ET` : 'No completed run',
          completed: data.run ? `${time(data.run.completed_at)} ET` : null,
          universe: data.run ? `${data.run.covered_underlyings} / ${data.run.expected_underlyings}` : null,
          included_runs: data.runs?.length ?? 0,
          latest_run_excluded: dayHistory && data.withheld_run ? `${time(data.withheld_run.scheduled_cycle)} ET` : 'None',
          selection_basis: data.selection_basis, selector: data.selector_version, selector_sha256: data.selector_sha256 }} />
        {baseline && data.runs?.map(run => <div key={run.run_id}><h4>{time(run.scheduled_cycle)} ET</h4><Rules values={run.rejections || {}} /></div>)}
      </details>
      {current && data.newer_partial_run && data.run?.run_id === data.active_run?.run_id && <Notice>Newer cycle incomplete: {data.newer_partial_run.covered_underlyings}/{data.newer_partial_run.expected_underlyings} underlyings completed at {time(data.newer_partial_run.scheduled_cycle)} ET. {data.run ? 'Last completed run retained.' : 'No complete run available.'}</Notice>}
      {dayHistory && <p className="oa-permission">Price P/L compares original and latest retained marks / Commission-only net return / Slippage unavailable / No fill or stop/target exit simulation</p>}
      {current && data.funnel.candidates === 0 && <div className="oa-section-heading"><p className="oa-muted">{data.run ? 'No selected detections match this completed run and filter scope.' : 'No completed detector run for the selected session.'}</p><button className="oa-command" type="button" onClick={() => update({ view: 'day_history' })}><History size={14} />Open Day History</button></div>}
      {data.funnel.eligible > 0 && data.funnel.timely === 0 && <Notice>No stock-gate pass has a verified pre-deadline receipt. No timely shortlist is available for this source.</Notice>}
      {data.schema.stock_ready === false && <Notice>Persisted stock behavior assessments are unavailable.</Notice>}
      {daily ? <>
        <div className="oa-section-heading"><h3>{data.session_state === 'IN_PROGRESS' ? 'Partial-session assessment' : 'End-of-day assessment'}</h3><span className="oa-muted">As of {time(data.as_of)} ET</span></div>
        {data.schema.outcomes_ready === false && <Notice>Retained outcome storage is unavailable.</Notice>}
        <div className="sd-table-panel"><div className="sd-table-scroll" tabIndex={0} role="region" aria-label="Daily behavior outcome scorecard"><table><thead><tr>
          <ColumnHeaders columns={columns} />
        </tr></thead><tbody>{data.cells?.map(cell => <tr key={`${cell.arm}-${cell.strategy}-${cell.structure}-${cell.horizon}`}>
          <ColumnCells columns={columns} values={{
            comparison: <>{arms[cell.arm] || label(cell.arm)}<small>{modelNames[cell.strategy] || label(cell.strategy)}</small></>,
            structure: <>{label(cell.structure)}<small>{label(cell.horizon)}</small></>,
            cohorts: cell.cohorts, coverage: <>{cell.measured}<small>{percent(cell.outcome_coverage)}</small></>,
            mean: percent(cell.mean_net_return), positive: percent(cell.positive_mark_fraction),
            minimum: percent(cell.minimum_net_return), maximum: percent(cell.maximum_net_return),
            states: <>{Object.entries(cell.states).map(([state, count]) => <small key={state}>{label(state)}: {count}</small>)}<small>{label(cell.verdict)}</small></>,
          }} />
        </tr>)}</tbody></table>{!data.cells?.length && <div className="sd-empty">No assessment-covered cohorts for this session.</div>}</div></div>
        <p className="oa-permission">{baseline ? 'First recorded alerts only / Repeat hits are not new samples' : 'Same covered candidate pool / First assessed model-ranked cohort'} / Commission included, slippage unavailable / Descriptive, not calibrated</p>
        <details className="oa-provenance"><summary>Exact assessment cohorts ({data.cohort_count || 0})</summary>
          <div className="oa-leg-scroll" tabIndex={0} role="region" aria-label="Exact daily assessment cohorts"><table className="oa-legs"><thead><tr>
            <th>Underlying / model</th><th>Structure / rank</th><th>Source (ET)</th><th>Stock behavior</th><th>60 min</th><th>Close</th><th>Next open</th><th>Details</th>
          </tr></thead><tbody>{data.cohort_rows?.map(row => <tr key={row.candidate_id}>
            <td>{row.underlying}<small>{modelNames[row.strategy_name] || label(row.strategy_name)}</small></td><td>{label(row.structure_type)}<small>Model rank {row.candidate_rank}</small></td>
            <td>{time(row.market_data_time)}</td><td><Status value={row.behavior.disposition} /><small>{row.behavior.timely_at_recording ? 'Timely receipt' : 'No timely receipt'}</small></td>
            {['60MIN', 'CLOSE', 'NEXT_OPEN'].map(horizon => <td key={horizon}>{percent(row.outcomes[horizon]?.net_return)}<small>{label(row.outcomes[horizon]?.state || 'UNAVAILABLE')}</small></td>)}
            <td><button className="oa-icon" title="View exact cohort package" aria-label={`View ${row.underlying} ${label(row.structure_type)} cohort`} onClick={() => update({ candidate: row.candidate_id, offset: String(offset) })}><Eye size={15} /></button></td>
          </tr>)}</tbody></table></div>
        </details>
        <details className="oa-provenance"><summary>Quality factors and exclusions</summary>
          <p className="oa-muted">Candidate occurrences / Correlated observations, not independent samples / No threshold changes</p>
          <div className="oa-leg-scroll" tabIndex={0} role="region" aria-label="Recorded quality factor distribution"><table className="oa-legs"><thead><tr><th>Factor</th><th>Occurrences</th><th>Minimum</th><th>Mean</th><th>Maximum</th></tr></thead><tbody>
            {Object.entries(data.factors || {}).map(([factor, values]) => <tr key={factor}><td>{label(factor)}</td><td>{values.count}</td><td>{values.minimum.toPrecision(5)}</td><td>{values.mean.toPrecision(5)}</td><td>{values.maximum.toPrecision(5)}</td></tr>)}
          </tbody></table></div><Rules values={data.funnel.reasons} />
        </details>
      </> : <section id="oa-results" className="sd-table-panel" aria-label={dayHistory ? 'Option detection day history' : 'Behavior-filtered candidates'} aria-busy={review.isFetching}>
        <div className="sd-table-scroll" tabIndex={0} role="region" aria-label={dayHistory ? 'Option day history table' : 'Detected option packages with behavior'}><table><thead><tr>
          <ColumnHeaders columns={columns} />
        </tr></thead><tbody>{data.rows?.map(row => <tr key={row.candidate_id}>
          <ColumnCells columns={columns} values={{
            underlying: <><button className="oa-row-link" onClick={() => update({ candidate: row.candidate_id, offset: String(offset) })}>{row.underlying}</button><small>{row.display_name}</small><small>Model rank {row.candidate_rank}</small><small>Source {time(row.market_data_time)} ET</small></>,
            contracts: <>{label(row.structure_type)}{row.legs.map(leg => <small key={leg.leg_index}>{leg.side} {leg.ratio} {leg.contract_ticker}</small>)}</>,
            expiry: <>{row.expiration_date}<small>{row.calendar_dte} DTE at source</small></>,
            ...marketCells(row.legs),
            ...(dayHistory ? currentMarkCells({ current_mark: row.current_mark, original_economics: { net_premium: row.net_premium } }) : {}),
            category: row.category_ids?.map(category => <small key={category}>{label(category)}</small>),
            hit_count: row.hit_count == null ? 'Unavailable' : <span title={`Distinct qualifying worker runs; last seen ${time(row.alert?.last_seen)} ET`}>{row.hit_count}</span>,
            source: <>{time(row.market_data_time)}<small>Run {time(row.scheduled_cycle)} ET</small></>,
            option_price: <PackagePrice netPremium={row.net_premium} legs={row.legs} />,
            ...planCells(row.net_premium, row.legs, row.management_policy, { version: row.management_policy_version }, row.maximum_loss, [...row.reason_codes, ...row.behavior.reasons]),
            premium: <>{optionPackagePremium(row.net_premium)}<small>Risk {money(row.capital_at_risk)}</small></>,
            behavior: <><Status value={row.behavior.disposition} />{row.behavior.reasons.map(reason => <small key={reason}>{label(reason)}</small>)}</>,
            receipt: <><Status value={row.behavior.recorded_at ? row.behavior.timely_at_recording ? 'TIMELY' : 'LATE' : 'UNAVAILABLE'} /><small>{time(row.behavior.recorded_at)} ET</small></>,
            entry: <><Status value={!row.valid_until ? 'UNAVAILABLE' : row.behavior.entry_open_now ? 'OPEN' : 'ELAPSED'} /><small>{time(row.valid_until)} ET</small></>,
            details: <button className="oa-icon" title="View behavior and exact package details" aria-label={`View ${row.underlying} behavior candidate ${row.candidate_rank}`} onClick={() => update({ candidate: row.candidate_id, offset: String(offset) })}><Eye size={15} /></button>,
            decision: time(row.behavior.decision_at), selection: label(row.behavior.selection_reason || 'NOT_SELECTED'),
            ...Object.fromEntries(optionBehaviorMetrics.map(metric => [metric.key, optionGateValue({ metric_id: metric.metric, actual_float: row.behavior.metrics[metric.key] ?? null, actual_text: null })])),
          }} />
        </tr>)}</tbody></table>{!data.rows?.length && <div className="sd-empty">{dayHistory ? data.withheld_run && !data.runs?.length ? 'No earlier completed runs for this session. The active latest run is excluded.' : 'No retained detections match this day and filter scope.' : 'No candidates match this latest run and filter scope.'}</div>}</div>
        <div className="sd-pager"><span>{data.total || 0} retained detections</span><div>
          <button aria-label="Previous behavior page" disabled={offset === 0 || review.isFetching} onClick={() => update({ offset: String(Math.max(0, offset - pageSize)) })}><ArrowLeft size={15} /></button>
          <button aria-label="Next behavior page" disabled={offset + pageSize >= (data.total || 0) || review.isFetching} onClick={() => update({ offset: String(offset + pageSize) })}><ArrowRight size={15} /></button>
        </div></div>
      </section>}
      <details className="oa-provenance"><summary>Review policy and coverage</summary><p>{data.policy_version}</p><code>{data.policy_sha256}</code><Rules values={{ session: data.session_date, scope: data.selection_basis, checked_at: data.as_of, query_seconds: data.elapsed_seconds, ...data.funnel.dispositions }} /></details>
    </>}
  </>
}

export default function OptionsAlertsPage() {
  const [params, setParams] = useSearchParams()
  const [sessionContext, setSessionContext] = useState<NonNullable<PageContextValue['alertSessions']>>({ dates: [], selected: '', source: 'OPTIONS_DETECTIONS', emptyLabel: 'No completed runs' })
  const activeView: OptionAlertView = ['history', 'daily', 'day_history'].includes(params.get('view') || '') ? params.get('view') as OptionAlertView : 'behavior'
  const preferencesByView = {
    behavior: useColumnPreferences('option-alerts-behavior-v2', optionAlertColumns.behavior),
    candidates: useColumnPreferences('option-alerts-candidates-v2', optionAlertColumns.candidates),
    daily: useColumnPreferences('option-alerts-daily-v1', optionAlertColumns.daily),
    history: useColumnPreferences('option-alerts-history-v2', optionAlertColumns.history),
    day_history: useColumnPreferences('option-alerts-day-history-v1', optionAlertColumns.day_history),
  }
  const preferences = preferencesByView[activeView]
  const columns = optionAlertVisibleColumns(activeView, preferences.hidden)
  const hidden = new Set(optionAlertColumns[activeView].filter(column => !column.locked && preferences.hidden.has(column.key)).map(column => column.key))
  const historyView = params.get('view') === 'history'
  const reviewView = activeView === 'behavior' || activeView === 'daily' || activeView === 'day_history'
  const offsetValue = Number(params.get('offset') || 0)
  const offset = Number.isSafeInteger(offsetValue) && offsetValue >= 0 ? offsetValue : 0
  const underlyer = useDeferredValue((params.get('underlyer') || '').trim().toUpperCase())
  const category = categories.find(value => value === params.get('category'))
  const status = candidateStates.find(value => value === (params.get('status') || 'SELECTED'))
  const packagesOnly = params.get('output') !== 'ALL'
  const minimum = params.get('min_dte') || ''
  const maximum = params.get('max_dte') || ''
  const validDte = [minimum, maximum].every(value => !value || Number.isInteger(Number(value)) && Number(value) >= 0 && Number(value) <= 365) && (!minimum || !maximum || Number(minimum) <= Number(maximum))
  const catalog = useQuery({ queryKey: ['option-discovery-catalog'], queryFn: getOptionDiscoveryCatalog, staleTime: 300_000 })
  const args = { underlyer: underlyer || undefined, category, status, structured_only: packagesOnly, strategy: params.get('strategy') || undefined,
    session_date: params.get('session_date') || undefined, minimum_dte: minimum ? Number(minimum) : undefined,
    maximum_dte: maximum ? Number(maximum) : undefined, sort: (params.get('sort') === 'CAPITAL_ASC' || params.get('sort') === 'DTE_ASC' ? params.get('sort') : 'DEFAULT') as 'DEFAULT' | 'CAPITAL_ASC' | 'DTE_ASC', limit: pageSize, offset }
  const candidates = useQuery({ queryKey: ['option-alert-candidates', args], queryFn: () => getOptionCandidates(args), enabled: !historyView && !reviewView && validDte, refetchInterval: 60_000 })
  const history = useQuery({ queryKey: ['option-alert-history', offset], queryFn: () => getOptionAlertHistory({ limit: pageSize, offset }), enabled: historyView, refetchInterval: 60_000 })
  const candidateData = candidates.data?.available && validDte ? candidates.data.data : undefined
  const candidateRows = candidateData?.rows || []
  const historyRows = history.data?.available ? history.data.data.rows : []
  const query = historyView ? history : candidates
  const visibleCount = historyView ? historyRows.length : candidateRows.length
  const total = historyView ? undefined : candidateData?.total
  const selectedId = params.get('candidate')
  const selectedEvent = historyRows.find(row => row.event_id === params.get('event'))
  const update = (values: Record<string, string | null>) => {
    const next = new URLSearchParams(window.location.search)
    Object.entries(values).forEach(([key, value]) => value ? next.set(key, value) : next.delete(key))
    if (!('offset' in values)) next.delete('offset')
    if (!('candidate' in values)) next.delete('candidate')
    if (!('event' in values)) next.delete('event')
    setParams(next)
  }
  const closeDetail = () => { const next = new URLSearchParams(window.location.search); next.delete('candidate'); next.delete('event'); setParams(next) }
  const switchTab = (view: OptionAlertView) => setParams(optionAlertTabParams(new URLSearchParams(window.location.search), view))
  const tabKey = (event: KeyboardEvent<HTMLButtonElement>) => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return
    event.preventDefault()
    const views: OptionAlertView[] = ['behavior', 'day_history', 'daily', 'history']
    const nextIndex = event.key === 'Home' ? 0 : event.key === 'End' ? views.length - 1 : (views.indexOf(activeView) + (event.key === 'ArrowRight' ? 1 : views.length - 1)) % views.length
    const buttons = event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>('[role="tab"]')
    switchTab(views[nextIndex])
    buttons?.[nextIndex]?.focus()
  }
  usePublishPageContext({ eyebrow: 'Options', title: 'Options Alerts', status: [{ label: 'Mode', value: 'Read-only / Indicative' }, { label: 'Data', value: '15-minute delayed' }],
    alertSessions: reviewView ? { ...sessionContext, selected: params.get('session_date') || sessionContext.dates[sessionContext.dates.length - 1] || '' } : undefined })
  return <div className="stock-discovery option-alerts">
    <div className="sd-toolbar"><div className="sd-presets" role="tablist" aria-label="Options alert views">
      <button type="button" role="tab" aria-selected={activeView === 'behavior'} tabIndex={activeView === 'behavior' ? 0 : -1} onKeyDown={tabKey} className={activeView === 'behavior' ? 'active' : ''} onClick={() => switchTab('behavior')}><ListFilter size={14} />Latest Run</button>
      <button type="button" role="tab" aria-selected={activeView === 'day_history'} tabIndex={activeView === 'day_history' ? 0 : -1} onKeyDown={tabKey} className={activeView === 'day_history' ? 'active' : ''} onClick={() => switchTab('day_history')}><History size={14} />Day History</button>
      <button type="button" role="tab" aria-selected={activeView === 'daily'} tabIndex={activeView === 'daily' ? 0 : -1} onKeyDown={tabKey} className={activeView === 'daily' ? 'active' : ''} onClick={() => switchTab('daily')}><BarChart3 size={14} />EOD Review</button>
      <button type="button" role="tab" aria-selected={historyView} tabIndex={historyView ? 0 : -1} onKeyDown={tabKey} aria-controls="oa-results" className={historyView ? 'active' : ''} onClick={() => switchTab('history')}><History size={14} aria-hidden="true" />Legacy Publication Audit</button>
    </div><div className="sd-tools">{(historyView || activeView === 'daily' && params.get('eod') === 'legacy') && <ColumnPicker key={activeView} columns={optionAlertColumns[activeView]} hidden={hidden} onToggle={preferences.toggle} onShowAll={preferences.showAll} onReset={preferences.reset} />}<Link to="/options" className="oa-research"><FlaskConical size={14} />Research</Link>{!reviewView && <button type="button" title="Refresh options alerts" aria-label="Refresh options alerts" disabled={query.isFetching} onClick={() => { void query.refetch() }}><RefreshCw size={15} /></button>}</div></div>
    {reviewView ? activeView === 'daily' ? <BehaviorReviewPanel key={activeView} view="daily" params={params} update={update} offset={offset} columns={columns} onSessionContext={setSessionContext} />
      : <DetectorAlertsPanel key={`detector-v4-${activeView}`} view={activeView as 'behavior' | 'day_history'} params={params} update={update} offset={offset} onSessionContext={setSessionContext} /> : <>
    {!historyView && <div className="sd-filters oa-filters">
      <label>Underlying<input type="search" placeholder="All underlyings" maxLength={12} value={params.get('underlyer') || ''} onChange={event => update({ underlyer: event.target.value.toUpperCase() })} /></label>
      <label>Category<select aria-label="Category" value={category || ''} onChange={event => update({ category: event.target.value, strategy: null })}><option value="">All categories</option>{catalog.data?.categories.map(item => <option value={item.id} key={item.id}>{item.label}</option>)}</select></label>
      <label>Model<select aria-label="Model" value={params.get('strategy') || ''} onChange={event => update({ strategy: event.target.value })}><option value="">All models</option>{catalog.data?.models.filter(item => (!packagesOnly || item.output_kind === 'STRUCTURE') && (!category || item.structures.some(structure => structure.category_ids.includes(category)))).map(item => <option value={item.id} key={item.id}>{item.label}</option>)}</select></label>
      <label>Candidate status<select aria-label="Candidate status" value={params.get('status') || 'SELECTED'} onChange={event => update({ status: event.target.value, output: event.target.value === 'SELECTED' ? null : 'ALL' })}><option value="ALL">All statuses</option>{candidateStates.map(value => <option value={value} key={value}>{label(value)}</option>)}</select></label>
      <label>Output<select aria-label="Output" value={packagesOnly ? 'PACKAGES' : 'ALL'} onChange={event => update({ output: event.target.value === 'ALL' ? 'ALL' : null, strategy: null })}><option value="PACKAGES">Complete packages</option><option value="ALL">Packages + observations</option></select></label>
      <label>Minimum DTE<input type="number" min="0" max="365" value={minimum} onChange={event => update({ min_dte: event.target.value })} /></label>
      <label>Maximum DTE<input type="number" min="0" max="365" value={maximum} onChange={event => update({ max_dte: event.target.value })} /></label>
      <label>Source session<input type="date" value={params.get('session_date') || ''} onChange={event => update({ session_date: event.target.value })} /></label>
      <label>Sort<select aria-label="Sort" value={args.sort} onChange={event => update({ sort: event.target.value })}><option value="DEFAULT">Model rank</option><option value="CAPITAL_ASC">Capital: low to high</option><option value="DTE_ASC">DTE: low to high</option></select></label>
      <button type="button" className="oa-icon" title="Reset candidate filters" aria-label="Reset candidate filters" onClick={() => setParams(new URLSearchParams({ view: 'candidates' }))}><RotateCcw size={15} /></button>
    </div>}
    {catalog.isError && !historyView && <Notice retry={() => { void catalog.refetch() }}>Model catalog unavailable.</Notice>}
    {!validDte && !historyView && <Notice>DTE bounds must be whole numbers from 0 to 365, with minimum no greater than maximum.</Notice>}
    <div className="sd-metrics"><span><strong>{historyView ? visibleCount : total?.toLocaleString() ?? '...'}</strong> {historyView ? 'events on page' : 'stored candidates'}</span>{!historyView && <><span><strong>{candidateData?.status_counts.selected.toLocaleString() ?? '...'}</strong> selected by model</span><span>{params.get('session_date') || 'Latest stored per underlying'}</span></>}<span className="sd-timestamp">{historyView ? 'Recorded' : 'Source'}: {time(query.data?.as_of)} ET</span></div>
    {candidateData?.serving_mode === 'HISTORICAL_PREVIOUS_POLICY' && !historyView && <Notice>Previous-policy evidence / Not current-policy qualification</Notice>}
    <section id="oa-results" role="tabpanel" aria-label={historyView ? 'Published option history' : 'Option candidates'} className="sd-table-panel" aria-busy={query.isFetching}>
      {query.isError ? <Notice retry={() => { void query.refetch() }}>Options data request failed.</Notice> : query.data && !query.data.available ? <Notice>{label(query.data.reason || 'SOURCE_UNAVAILABLE')}</Notice> : query.isPending && (historyView || validDte) ? <div className="sd-empty" role="status">Loading options {historyView ? 'history' : 'candidates'}...</div> : visibleCount === 0 ? <div className="sd-empty"><Bell size={28} aria-hidden="true" /><strong>{historyView ? 'No published option events' : 'No matching candidates'}</strong><span>{historyView ? 'Forward indicative ledger / 0 events on this page' : 'No rows in the selected source and filter scope'}</span></div> : <div className="sd-table-scroll" tabIndex={0} role="region" aria-label={historyView ? 'Published option events table' : 'Option candidates table'}>
        {historyView ? <table><thead><tr><ColumnHeaders columns={columns} /></tr></thead><tbody>{historyRows.map(row => <tr key={row.event_id}><ColumnCells columns={columns} values={{
          event: <button type="button" className="oa-row-link" aria-label={`Open ${row.underlying} publication ${row.sequence}`} onClick={() => update({ event: row.event_id, offset: String(offset) })}><Status value={row.event_type} /></button>,
          underlying: <><strong>{row.underlying}</strong><small>{label(row.strategy)}</small></>,
          structure: <>{label(row.structure)}<small>{row.legs.length} legs</small></>,
          recorded: time(row.recorded_at), entry_limit: <>{money(row.entry_limit)}<small>{label(row.entry_limit_kind)}</small></>,
          entry_deadline: time(row.entry_deadline), exit_deadline: time(row.exit_deadline), source: time(row.source_market_time),
          contracts: row.legs.length ? row.legs.map(leg => <small key={leg.index}>{leg.side} {leg.ratio} {leg.contract_ticker}</small>) : 'Unavailable',
          entry_marks: <PackagePrice netPremium={row.original_economics.net_premium} legs={row.legs} />,
          entry_leg_marks: row.legs.length ? row.legs.map(leg => <div className="oa-market-leg" key={leg.index} title={leg.contract_ticker}><small>{leg.side} {leg.ratio}</small><span>{money(leg.entry_model_mark)}</span></div>) : 'Unavailable',
          ...currentMarkCells(row),
          ...planCells(row.original_economics.net_premium, row.legs, row.management_policy, { version: row.management_policy_version,
            source: row.management_source, entryLimit: row.entry_limit, entryKind: row.entry_limit_kind, exitDeadline: row.exit_deadline }, row.original_economics.maximum_loss),
          capital: money(row.original_economics.capital_at_risk), maximum_loss: money(row.original_economics.maximum_loss), maximum_profit: money(row.original_economics.maximum_profit),
          plan: <code className="oa-table-hash">{row.plan_sha256}</code>,
          details: <button type="button" className="oa-icon" title="View publication event" aria-label={`View ${row.underlying} publication ${row.sequence}`} onClick={() => update({ event: row.event_id, offset: String(offset) })}><Eye size={15} /></button>,
        }} /></tr>)}</tbody></table>
          : <table><thead><tr><ColumnHeaders columns={columns} /></tr></thead><tbody>{candidateRows.map(row => <tr key={row.candidate_id}><ColumnCells columns={columns} values={{
            underlying: <><button type="button" className="oa-row-link" aria-label={`Open ${row.underlying} ${label(row.structure_type)} candidate ${row.candidate_rank}`} onClick={() => update({ candidate: row.candidate_id, offset: String(offset) })}>{row.underlying}</button><small title={row.display_name}>{row.display_name}</small></>,
            structure: <>{label(row.structure_type)}<small>{row.candidate_kind === 'RESEARCH_ONLY' ? 'Observation only' : `${row.legs.length} listed ${row.legs.length === 1 ? 'leg' : 'legs'}`}</small></>,
            expiry: <>{row.expiration_date || 'Not applicable'}<small>{row.calendar_dte == null ? '' : `${row.calendar_dte} DTE at source`}</small></>,
            ...marketCells(row.legs),
            option_price: <PackagePrice netPremium={row.net_premium} legs={row.legs} />,
            ...planCells(row.net_premium, row.legs, row.management_policy, { version: row.management_policy_version }, row.maximum_loss, row.reason_codes),
            premium: optionPackagePremium(row.net_premium), capital: money(row.capital_at_risk), source: time(row.market_data_time), observed: time(row.observed_time),
            entry: <Status value={optionEntryWindow(row.valid_until)} />, status: <>{label(row.status)}<small>Model rank {row.candidate_rank}</small></>,
            details: <button type="button" className="oa-icon" title="View candidate details" aria-label={`View ${row.underlying} ${label(row.structure_type)} candidate ${row.candidate_rank}`} onClick={() => update({ candidate: row.candidate_id, offset: String(offset) })}><Eye size={15} /></button>,
            contracts: <>{label(row.structure_type)}{row.legs.length ? row.legs.map(leg => <small key={leg.leg_index}>{leg.side} {leg.ratio} {leg.contract_ticker}</small>) : <small>{row.source_contract_ticker || 'Unavailable'}</small>}</>,
            maximum_loss: money(row.maximum_loss), maximum_profit: money(row.maximum_profit), collateral: money(row.collateral_required),
            breakevens: row.breakevens.length ? row.breakevens.map(value => money(value)).join(', ') : 'Unavailable',
            reasons: row.reason_codes.length ? row.reason_codes.map(reason => <small key={reason}>{label(reason)}</small>) : 'None recorded',
            policy: <code className="oa-table-hash">{row.policy_sha256}</code>,
          }} /></tr>)}</tbody></table>}
      </div>}
      <div className="sd-pager"><span>{visibleCount ? `${offset + 1}-${offset + visibleCount}${total == null ? '' : ` of ${total.toLocaleString()}`}` : '0 rows'}</span><div><button type="button" aria-label="Previous options page" title="Previous page" disabled={offset === 0 || query.isFetching} onClick={() => update({ offset: String(Math.max(0, offset - pageSize)) })}><ArrowLeft size={15} /></button><button type="button" aria-label="Next options page" title="Next page" disabled={query.isFetching || !visibleCount || (total == null ? visibleCount < pageSize : offset + visibleCount >= total)} onClick={() => update({ offset: String(offset + pageSize) })}><ArrowRight size={15} /></button></div></div>
    </section>
    <p className="oa-permission"><ShieldAlert size={14} aria-hidden="true" />{historyView ? 'Event history: repeated plan events are not independent samples / Marked P/L ignores fills and stop/target exits / Commission-only net return, slippage unavailable' : 'Model-selected candidates / Not published alerts'} / No execution permission</p>
    </>}
    {selectedId && <CandidateDetail key={selectedId} candidateId={selectedId} close={closeDetail} />}
    {!selectedId && selectedEvent && <PublicationDetail row={selectedEvent} close={closeDetail} openCandidate={() => update({ candidate: selectedEvent.candidate_id, event: null, offset: String(offset) })} />}
  </div>
}