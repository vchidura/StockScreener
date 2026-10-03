import type { OptionAlertPreviewRequest, OptionCandidateDetailData, OptionCandidateLeg, OptionStockBehaviorGateEvidence, OptionDetectorAlertReview } from '../services/api'
import type { ColumnSpec } from '../layout/PageChrome'

export type OptionAlertView = 'behavior' | 'day_history' | 'daily'
export type OptionAlertColumn = ColumnSpec & { tip?: string }

const detectorObservationColumns: OptionAlertColumn[] = [
  { key: 'fitted_iv', label: 'Fitted IV', group: 'Observation evidence', hiddenByDefault: true },
  { key: 'residual', label: 'IV gap (points)', group: 'Observation evidence', hiddenByDefault: true },
  { key: 'robust_z', label: 'Robust z-score', group: 'Observation evidence', hiddenByDefault: true },
  { key: 'source_cutoff', label: 'Source cutoff (ET)', group: 'Observation evidence', hiddenByDefault: true, tip: 'Retained surface evaluation cutoff, not a stock trigger or an individual option quote timestamp.' },
  { key: 'analyzed_at', label: 'Analyzed (ET)', group: 'Observation evidence', hiddenByDefault: true },
]

export function detectorObservationFieldStates(row: OptionDetectorAlertReview['rows'][number]): Record<string, string> {
  if (!row.observation) return {}
  return {
    ...Object.fromEntries(optionMarketColumns.map(column => [column.key, 'Not retained'])),
    ...Object.fromEntries(['triggered_at', 'hit_count', 'technical_stop', 'technical_target',
      'current_price', 'price_pnl', 'current_mark_time', 'current_mark_status', 'gross_pnl',
      'net_pnl', 'estimated_cost', 'capital', 'maximum_loss', 'maximum_profit', 'breakevens',
      'net_return'].map(key => [key, 'Not applicable'])),
    strategy: 'Local IV surface observation', outcome_status: 'Observation only',
    option_price: 'Not applicable', leg_prices: 'Not applicable',
  }
}

export const detectorSortKeys = ['triggered_at', 'run', 'underlyer', 'detector', 'category', 'strategy', 'rank', 'entry_limit'] as const

export function optionDetectorSort(params: URLSearchParams) {
  const raw = params.get('detector_sort')
  const sort_by = detectorSortKeys.find(key => key === raw) || 'run'
  const sort_order: 'asc' | 'desc' = params.get('detector_order') === 'asc' ? 'asc' : 'desc'
  return { sort_by, sort_order }
}

export function optionDetectorSortChange(params: URLSearchParams, key: typeof detectorSortKeys[number]) {
  const current = optionDetectorSort(params)
  return { detector_sort: key, detector_order: current.sort_by === key && current.sort_order === 'asc' ? 'desc' : 'asc', offset: null }
}

export function optionPackageStrategyName(strategy: string | null | undefined): string {
  if (!strategy) return 'Not applicable'
  if (strategy === 'INCOME_WHEEL') return 'Income / Wheel'
  return optionAlertLabel(strategy)
}

export function optionAlertTabParams(params: URLSearchParams, view: OptionAlertView): URLSearchParams {
  const next = new URLSearchParams(params)
  next.set('view', view)
  for (const key of ['offset', 'candidate', 'event']) next.delete(key)
  return next
}

export const optionPlanColumns: OptionAlertColumn[] = [
  { key: 'plan_stop', label: 'Original stop', group: 'Trade plan', tip: 'Recorded policy close-value threshold, not a guaranteed fill or maximum loss.' },
  { key: 'plan_target', label: 'Original target', group: 'Trade plan', tip: 'Recorded policy close-value target, not a forecast.' },
  { key: 'risk_to_stop', label: 'Risk to stop', group: 'Trade plan', tip: 'Planned loss per package and percent of the stated entry basis, before costs.' },
  { key: 'reward_risk', label: 'Reward / Risk', group: 'Trade plan', tip: 'Planned target profit divided by stop risk, not expiration maximum profit/loss.' },
  { key: 'maximum_hold', label: 'Maximum hold', group: 'Trade plan', tip: 'Recorded elapsed-hour cap, exit-DTE rule or fixed deadline. No horizon is invented from DTE lanes.' },
  { key: 'risk_assessment', label: 'Risk assessment', group: 'Trade plan', tip: 'Recorded limitations and plan availability; not a calibrated risk score.' },
]

export const optionMarketColumns: OptionAlertColumn[] = [
  { key: 'stock', label: 'Stock at source', group: 'Original market data', tip: 'Underlying spot retained with each original candidate leg; not a live price.' },
  { key: 'otm', label: '% OTM', group: 'Original market data', tip: 'Signed distance from original spot to strike; negative means in the money. Side does not change moneyness.' },
  { key: 'option_price', label: 'Option price', group: 'Original market data', tip: 'Net retained sell-minus-buy premium, including leg ratios, divided by the common contract multiplier. Debit is paid; credit is received. Not a current quote or fill.' },
  { key: 'leg_prices', label: 'Leg prices / share', group: 'Original market data', hiddenByDefault: true, tip: 'Original model marks for each leg in listed order, before netting the package.' },
  { key: 'volume', label: 'Volume', group: 'Original market data', tip: 'Contract day volume in the original retained snapshot; not summed across package legs.' },
  { key: 'open_interest', label: 'OI', group: 'Original market data', tip: 'Contract open interest in the original retained snapshot; not intraday position changes.' },
  { key: 'iv', label: 'IV', group: 'Original market data', tip: 'Original local model implied volatility, displayed as a percentage.' },
  { key: 'delta', label: 'Delta', group: 'Original market data', hiddenByDefault: true },
  { key: 'gamma', label: 'Gamma', group: 'Original market data', hiddenByDefault: true },
  { key: 'theta', label: 'Theta / day', group: 'Original market data', hiddenByDefault: true },
  { key: 'vega', label: 'Vega / vol point', group: 'Original market data', hiddenByDefault: true },
  { key: 'rho', label: 'Rho / rate point', group: 'Original market data', hiddenByDefault: true },
  { key: 'bid_ask', label: 'Bid / ask', group: 'Original market data', hiddenByDefault: true, tip: 'Original retained quotes only; never inferred from volume or model marks.' },
  { key: 'volume_oi', label: 'Volume / OI', group: 'Original market data', hiddenByDefault: true, tip: 'Unavailable when original open interest is zero or missing.' },
  { key: 'mark_time', label: 'Option price time (ET)', group: 'Original market data', hiddenByDefault: true },
  { key: 'mark_source', label: 'Option price basis', group: 'Original market data', hiddenByDefault: true },
]

const detectorHiddenMarketColumns = new Set(['theta', 'vega', 'rho', 'bid_ask', 'mark_time', 'mark_source'])

const detectorMarketColumns: OptionAlertColumn[] = optionMarketColumns.flatMap(column => {
  if (column.key === 'option_price') return []
  const source = { ...column, hiddenByDefault: detectorHiddenMarketColumns.has(column.key),
    label: column.label, tip: column.tip }
  return column.key === 'leg_prices' ? [source,
    { key: 'option_price', label: 'Trigger price', group: 'Original market data', tip: 'Original net package price per share from the trigger snapshots. Not a fill or current mark.' },
    { key: 'current_price', label: 'Current price', group: 'Marked performance', tip: 'Latest retained coherent package mark per share. Delayed and not an executable quote or fill.' },
    { key: 'price_pnl', label: 'Price P/L %', group: 'Marked performance', tip: 'Gross package P/L divided by absolute original net premium. Credit gains reflect lower close cost. Ignores fills and stop/target exits.' },
    { key: 'technical_stop', label: 'Stop price', group: 'Risk / target', tip: 'Frozen option package close threshold per share, normalized by the common contract multiplier. Not total contract cost or a guaranteed fill.' },
    { key: 'technical_target', label: 'Target price', group: 'Risk / target', tip: 'Frozen option package target threshold per share, normalized by the common contract multiplier. Not total contract proceeds or a guaranteed fill.' },
  ] : [source]
})

export const detectorAlertColumns: OptionAlertColumn[] = [
  { key: 'details', label: 'View', group: 'Alert', locked: true },
  { key: 'triggered_at', label: 'Triggered (ET)', group: 'Alert', locked: true, tip: 'Retained option source time for O1 or original stock trigger-bar time for S1/S2, not selection or run time.' },
  { key: 'underlyer', label: 'Underlying', group: 'Alert', locked: true },
  { key: 'contract_summary', label: 'Contract summary', group: 'Package' },
  { key: 'contracts', label: 'Contracts / structure', group: 'Package', hiddenByDefault: true },
  { key: 'expiry', label: 'Expiry / DTE at source', group: 'Package' },
  ...detectorMarketColumns,
  { key: 'run', label: 'Run (ET)', group: 'Alert', hiddenByDefault: true },
  { key: 'hit_count', label: 'Hits', group: 'Alert' },
  { key: 'management_status', label: 'Management status', group: 'Management', tip: 'First fresh retained mark crossing of the frozen target/stop rule. Not a fill or realized outcome.' },
  { key: 'capital', label: 'Original capital at risk', group: 'Economics', hiddenByDefault: true },
  { key: 'maximum_loss', label: 'Maximum expiration loss', group: 'Economics', hiddenByDefault: true },
  { key: 'maximum_profit', label: 'Maximum expiration profit', group: 'Economics', hiddenByDefault: true },
  { key: 'breakevens', label: 'Expiration breakevens', group: 'Economics', hiddenByDefault: true },
  { key: 'net_return', label: 'Net marked return', group: 'Performance', hiddenByDefault: true },
  { key: 'gross_pnl', label: 'Gross package P/L', group: 'Marked performance', hiddenByDefault: true },
  { key: 'net_pnl', label: 'Net package P/L', group: 'Marked performance', hiddenByDefault: true },
  { key: 'estimated_cost', label: 'Commission estimate', group: 'Marked performance', hiddenByDefault: true },
  { key: 'current_mark_time', label: 'Current price time (ET)', group: 'Marked performance', hiddenByDefault: true },
  { key: 'current_mark_status', label: 'Current mark status', group: 'Marked performance', hiddenByDefault: true },
  { key: 'outcome_status', label: 'Outcome status', group: 'Performance', hiddenByDefault: true },
  ...detectorObservationColumns,
  { key: 'detector', label: 'Model', group: 'Classification', hiddenByDefault: true },
  { key: 'category', label: 'Category', group: 'Classification', hiddenByDefault: true },
  { key: 'strategy', label: 'Package strategy', group: 'Classification', hiddenByDefault: true },
]

export function detectorAlertColumnsForView(view: 'behavior' | 'day_history'): OptionAlertColumn[] {
  return detectorAlertColumns.map(column =>
    column.key === 'current_price' || column.key === 'price_pnl'
      ? { ...column, hiddenByDefault: view === 'behavior' }
      : column)
}

const detectorIdentity = ['details', 'triggered_at', 'underlyer', 'contract_summary', 'detector']
export const detectorColumnPresets: Record<string, string[]> = {
  'Package / market data': detectorAlertColumns.filter(column => !column.hiddenByDefault).map(column => column.key),
  'Greeks / volatility': ['triggered_at', 'underlyer', 'contract_summary', 'contracts', 'expiry', 'stock', 'option_price', 'iv', 'delta', 'gamma', 'theta', 'vega', 'rho'],
  'Price / activity': [...detectorIdentity, 'category', 'strategy', 'contracts', 'expiry', 'stock', 'option_price', 'current_price', 'price_pnl', 'technical_stop', 'technical_target', 'volume', 'open_interest', 'volume_oi', 'iv'],
  Performance: [...detectorIdentity, 'category', 'strategy', 'run', 'hit_count', 'management_status', 'option_price', 'current_price', 'price_pnl', 'gross_pnl', 'net_pnl', 'net_return', 'current_mark_time', 'current_mark_status', 'outcome_status'],
  'IV observation evidence': [...detectorIdentity, 'contracts', 'expiry', 'iv', ...detectorObservationColumns.map(column => column.key)],
  All: detectorAlertColumns.map(column => column.key),
}

export const DETECTOR_PRESETS_KEY = 'options-detector-view-presets-v1'
export type DetectorViewPreset = { name: string; query: Record<string, string>; columns: string[] }

export function detectorViewPreset(name: string, params: URLSearchParams, columns: string[]): DetectorViewPreset {
  const trimmed = name.trim()
  if (!trimmed || trimmed.length > 60) throw new Error('Preset name must contain 1-60 characters')
  const known = new Set(detectorAlertColumns.map(column => column.key))
  if (columns.some(key => !known.has(key)) || new Set(columns).size !== columns.length) throw new Error('Invalid preset columns')
  const query: Record<string, string> = {}
  for (const key of ['underlyer', 'evaluation_detector', 'detector_sort', 'detector_order']) {
    const value = params.get(key)
    if (value) query[key] = value
  }
  if (query.underlyer && !/^[A-Z][A-Z0-9.]{0,14}$/.test(query.underlyer)) throw new Error('Invalid underlying')
  if (query.evaluation_detector && !['O1', 'O2', 'O3', 'S1', 'S2'].includes(query.evaluation_detector)) throw new Error('Invalid detector model')
  if (query.detector_sort && !detectorSortKeys.some(key => key === query.detector_sort)) throw new Error('Invalid sort')
  if (query.detector_order && !['asc', 'desc'].includes(query.detector_order)) throw new Error('Invalid sort direction')
  return { name: trimmed, query, columns: [...new Set([...detectorAlertColumns.filter(column => column.locked).map(column => column.key), ...columns])] }
}

export function readDetectorPresets(storage: Pick<Storage, 'getItem'>): DetectorViewPreset[] {
  const raw = storage.getItem(DETECTOR_PRESETS_KEY)
  if (!raw) return []
  if (raw.length > 100_000) throw new Error('Saved alert presets exceed size limit')
  const payload = JSON.parse(raw)
  if (payload.version !== 1 || !Array.isArray(payload.presets) || payload.presets.length > 50) throw new Error('Unsupported alert preset library')
  const names = new Set<string>()
  return payload.presets.map((value: DetectorViewPreset) => {
    if (!value || typeof value.name !== 'string' || !Array.isArray(value.columns) || !value.query || typeof value.query !== 'object'
      || Object.entries(value.query).some(([key, item]) => !['underlyer', 'evaluation_detector', 'detector_sort', 'detector_order'].includes(key) || typeof item !== 'string')) throw new Error('Invalid saved alert preset')
    const preset = detectorViewPreset(value.name, new URLSearchParams(value.query), value.columns)
    if (names.has(preset.name)) throw new Error('Duplicate alert preset name')
    names.add(preset.name)
    return preset
  })
}

export function writeDetectorPresets(storage: Pick<Storage, 'setItem'>, presets: DetectorViewPreset[]) {
  const raw = JSON.stringify({ version: 1, presets })
  readDetectorPresets({ getItem: () => raw })
  storage.setItem(DETECTOR_PRESETS_KEY, raw)
}

export const optionAlertLabel = (value: string) => value.toLowerCase().replace(/_/g, ' ').replace(/^./, letter => letter.toUpperCase())

export function optionAlertMoney(value: unknown): string {
  if (value == null || typeof value === 'string' && !value.trim() || (typeof value !== 'number' && typeof value !== 'string')) return 'Unavailable'
  const numeric = Number(value)
  return Number.isFinite(numeric) ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(numeric) : 'Unavailable'
}

export function optionAlertPercent(value: unknown): string {
  if (value == null || typeof value === 'string' && !value.trim() || (typeof value !== 'number' && typeof value !== 'string')) return 'Unavailable'
  const numeric = Number(value)
  return Number.isFinite(numeric) ? new Intl.NumberFormat('en-US', { style: 'percent', maximumFractionDigits: 2 }).format(numeric) : 'Unavailable'
}

export function optionCandidateBoardFacts(data: OptionCandidateDetailData) {
  const candidate = data.candidate
  const quoteGate = data.execution_gates?.find(gate => gate.gate_name === 'QUOTE_LIQUIDITY')
  const reasons = (values: string[] | null | undefined) => values == null ? null : values.length ? values.map(optionAlertLabel).join(' / ') : 'None recorded'
  return {
    selection: {
      decision_type: optionAlertLabel(candidate.candidate_kind),
      risk_class: optionAlertLabel(candidate.structure_risk_class),
      source_contract: candidate.source_contract_ticker || 'Not contract-specific',
      source_contract_id: candidate.source_contract_id,
      expiration: candidate.expiration_date,
      model_rank: candidate.candidate_rank,
      primary_metric: candidate.primary_metric_name ? optionAlertLabel(candidate.primary_metric_name) : null,
      primary_metric_value: Number.isFinite(candidate.primary_metric_value) ? candidate.primary_metric_value : null,
      original_execution_eligibility: candidate.execution_eligibility || 'Not eligible',
    },
    context: {
      context_status: candidate.context_status ? optionAlertLabel(candidate.context_status) : null,
      trend_state: candidate.trend_state,
      earnings_blackout: candidate.earnings_blackout_state,
      fed_blackout: candidate.fed_blackout_state,
      context_reasons: reasons(candidate.context_reason_codes),
    },
    execution: {
      quote_liquidity_gate: quoteGate ? optionAlertLabel(quoteGate.verdict) : 'Not recorded',
      signal_state: candidate.signal_status ? optionAlertLabel(candidate.signal_status) : 'No signal',
      signal_id: candidate.signal_id,
      signal_blocked_reasons: reasons(candidate.signal_blocked_reasons),
      execution_mode: optionAlertLabel(data.execution_mode),
    },
  }
}

export function optionAlertTime(value: string | null | undefined): string {
  if (!value || !Number.isFinite(Date.parse(value))) return 'Unavailable'
  return new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit' }).format(new Date(value))
}

export function optionEntryWindow(validUntil: string | null | undefined, now = Date.now()): 'OPEN' | 'ELAPSED' | 'UNAVAILABLE' {
  const deadline = validUntil ? Date.parse(validUntil) : NaN
  return !Number.isFinite(deadline) ? 'UNAVAILABLE' : deadline > now ? 'OPEN' : 'ELAPSED'
}

export interface OptionAlertForm {
  entryDeadline: string; exitDeadline: string; entryLimit: string; management: 'ORIGINAL' | 'EXPLICIT'
  policyVersion: string; stopPercent: string; targetPercent: string; holdHours: string; exitDte: string
}

export const emptyOptionAlertForm: OptionAlertForm = {
  entryDeadline: '', exitDeadline: '', entryLimit: '', management: 'ORIGINAL',
  policyVersion: '', stopPercent: '', targetPercent: '', holdHours: '', exitDte: '',
}

export function optionAlertPreviewRequest(form: OptionAlertForm, strategy: string): OptionAlertPreviewRequest {
  const result: OptionAlertPreviewRequest = {}
  const utcTime = (value: string) => {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(value)) throw new Error('Enter a complete UTC date and time.')
    const parsed = new Date(`${value}:00Z`)
    if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 16) !== value) throw new Error('Enter a valid UTC date and time.')
    return parsed.toISOString()
  }
  const positive = (value: string, label: string) => {
    if (!value.trim() || !Number.isFinite(Number(value)) || Number(value) <= 0) throw new Error(`${label} must be positive and finite.`)
    return Number(value)
  }
  if (form.entryDeadline) result.entry_deadline = utcTime(form.entryDeadline)
  if (form.exitDeadline) result.exit_deadline = utcTime(form.exitDeadline)
  if (form.entryLimit.trim()) {
    positive(form.entryLimit, 'Entry limit')
    result.entry_limit = form.entryLimit.trim()
  }
  if (form.management === 'EXPLICIT') {
    if (strategy !== 'DIRECTIONAL_LONG_PREMIUM' && strategy !== 'DIRECTIONAL_DEBIT_SPREAD') throw new Error('Explicit management is only supported for directional debit strategies.')
    if (!form.policyVersion.trim()) throw new Error('A management policy version is required.')
    const stop = positive(form.stopPercent, 'Stop loss')
    if (stop >= 100) throw new Error('Stop loss must be below 100% of the entry debit.')
    const target = positive(form.targetPercent, 'Take profit')
    const seconds = positive(form.holdHours, 'Maximum hold') * 3600
    const exitDte = positive(form.exitDte, 'Minimum exit DTE')
    if (!Number.isInteger(seconds) || seconds > 366 * 86400) throw new Error('Maximum hold must be whole elapsed seconds within 366 days.')
    if (!Number.isInteger(exitDte)) throw new Error('Minimum exit DTE must be a whole number.')
    result.management_policy = { policy_version: form.policyVersion.trim(), strategy_name: strategy,
      stop_loss_fraction: String(stop / 100), take_profit_fraction: String(target / 100), maximum_hold_seconds: seconds, minimum_exit_dte: exitDte }
  }
  return result
}

export function optionGateTimeframe(componentKey: string | null): 'Daily' | 'Hourly' | '30 minute' | 'Assessment' {
  if (componentKey?.endsWith('.1d')) return 'Daily'
  if (componentKey?.endsWith('.1h')) return 'Hourly'
  if (componentKey?.endsWith('.30m')) return '30 minute'
  return 'Assessment'
}

export function optionGateValue(gate: Pick<OptionStockBehaviorGateEvidence, 'metric_id' | 'actual_float' | 'actual_text'>): string {
  if (gate.actual_float == null) return gate.actual_text ? optionAlertLabel(gate.actual_text) : 'Unavailable'
  const value = Number(gate.actual_float)
  if (!Number.isFinite(value)) return 'Unavailable'
  if (gate.metric_id === 'median_dollar_volume20') return `${optionAlertMoney(value)} / session`
  if (gate.metric_id === 'excess_return20') return new Intl.NumberFormat('en-US', { style: 'percent', maximumFractionDigits: 2 }).format(value)
  if (gate.metric_id === 'daily_rvol20') return `${value.toFixed(2)}x`
  if (gate.metric_id === 'adx14') return `${value.toFixed(1)} / 100`
  if (gate.metric_id === 'ema50_slope10_atr') return `${value.toFixed(3)} ATR / bar`
  if (gate.metric_id === 'extension_ema21_atr') return `${value.toFixed(2)} ATR`
  return String(value)
}

function marketNumber(value: unknown): number | null {
  if (value == null || typeof value === 'string' && !value.trim() || !['number', 'string'].includes(typeof value)) return null
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : null
}

export function optionPackagePremium(value: unknown): string {
  const numeric = marketNumber(value)
  return numeric == null ? 'Unavailable' : `${optionAlertMoney(Math.abs(numeric))} ${numeric < 0 ? 'debit' : numeric > 0 ? 'credit' : 'net'}`
}

export function optionContractSummary(underlyer: string, legs: readonly (Pick<OptionCandidateLeg, 'strike' | 'contract_type'> & Partial<Pick<OptionCandidateLeg, 'side' | 'ratio'>> )[]): string {
  if (!legs.length) return 'Unavailable'
  const strike = (value: unknown) => marketNumber(value)?.toLocaleString('en-US', { maximumFractionDigits: 3 }) ?? 'Unavailable'
  return legs.length === 1
    ? `${underlyer} ${strike(legs[0].strike)} ${legs[0].contract_type}`
    : `${underlyer} / ${legs.map(leg => `${leg.side ? `${leg.side} ` : ''}${leg.ratio ? `${leg.ratio}x ` : ''}${strike(leg.strike)} ${leg.contract_type}`).join(' / ')}`
}

type PackagePriceLeg = {
  side: string; ratio: unknown; multiplier: unknown; model_mark?: unknown; entry_model_mark?: unknown
}

export function optionPackagePrice(netPremium: unknown, legs: readonly PackagePriceLeg[]) {
  const amount = marketNumber(netPremium)
  const unavailable = (reason: string) => ({ amount: null, price: null, multiplier: null, reason })
  if (amount == null) return unavailable('PACKAGE_PREMIUM_UNAVAILABLE')
  if (!legs.length) return unavailable('PACKAGE_LEGS_UNAVAILABLE')
  let computed = 0
  let magnitude = 0
  const multipliers = new Set<number>()
  for (const leg of legs) {
    const mark = marketNumber('model_mark' in leg ? leg.model_mark : leg.entry_model_mark)
    const ratio = marketNumber(leg.ratio)
    const multiplier = marketNumber(leg.multiplier)
    if (!['BUY', 'SELL'].includes(leg.side) || mark == null || mark <= 0
      || ratio == null || !Number.isSafeInteger(ratio) || ratio <= 0
      || multiplier == null || !Number.isSafeInteger(multiplier) || multiplier <= 0) {
      return unavailable('PACKAGE_LEG_TERMS_UNAVAILABLE')
    }
    const cash = mark * ratio * multiplier
    computed += leg.side === 'SELL' ? cash : -cash
    magnitude += cash
    multipliers.add(multiplier)
  }
  if (!Number.isFinite(computed) || !Number.isFinite(magnitude)
    || Math.abs(computed - amount) > 0.000001 + Number.EPSILON * magnitude * legs.length) {
    return unavailable('PACKAGE_PREMIUM_LEG_MISMATCH')
  }
  if (multipliers.size !== 1) return { amount, price: null, multiplier: null, reason: 'MIXED_LEG_MULTIPLIERS' }
  const multiplier = Number(legs[0].multiplier)
  return { amount, price: amount / multiplier, multiplier, reason: null }
}

export function optionPlanTerms(netPremium: unknown, legs: readonly PackagePriceLeg[], management: Record<string, unknown> | null | undefined,
  options: { version?: string | null; source?: string; entryLimit?: unknown; entryKind?: string; exitDeadline?: string | null } = {}) {
  const price = optionPackagePrice(netPremium, legs)
  const policy = management || {}
  const explicit = options.source === 'EXPLICIT_ALERT_POLICY'
  const entry = explicit ? marketNumber(options.entryLimit) : price.amount == null ? null : Math.abs(price.amount)
  const credit = price.amount != null && price.amount > 0
  const fraction = marketNumber(policy.stop_loss_fraction)
  const multiple = marketNumber(policy.stop_loss_multiple)
  const takeProfit = marketNumber(policy.take_profit_fraction)
  const reasons: string[] = []
  let stop: number | null = null
  let target: number | null = null
  const basisValid = price.amount != null && entry != null && entry > 0 && !!options.version && (!explicit || !credit && options.entryKind === 'MAXIMUM_DEBIT')
  if (basisValid) {
    if (credit && multiple != null && multiple > 1) stop = entry * multiple
    if (!credit && fraction != null && fraction > 0 && fraction < 1) stop = entry * (1 - fraction)
    if (takeProfit != null && takeProfit > 0 && (!credit || takeProfit <= 1)) target = entry * (credit ? 1 - takeProfit : 1 + takeProfit)
  }
  if (!options.version) reasons.push('MANAGEMENT_POLICY_NOT_RECORDED')
  if (price.reason) reasons.push(price.reason)
  if (stop == null) reasons.push('STOP_UNAVAILABLE')
  if (target == null) reasons.push('TARGET_UNAVAILABLE')
  const risk = stop != null && entry != null ? credit ? stop - entry : entry - stop : null
  const reward = target != null && entry != null ? credit ? entry - target : target - entry : null
  const seconds = marketNumber(policy.maximum_hold_seconds)
  const exitDte = marketNumber(policy.exit_dte)
  const hold: string[] = []
  if (seconds != null && seconds > 0 && Number.isSafeInteger(seconds)) hold.push(`${(seconds / 3600).toLocaleString('en-US', { maximumFractionDigits: 2 })} elapsed hours`)
  if (exitDte != null && exitDte >= 0 && Number.isSafeInteger(exitDte)) hold.push(`Exit at ${exitDte} DTE`)
  if (options.exitDeadline && Number.isFinite(Date.parse(options.exitDeadline))) hold.push(`By ${optionAlertTime(options.exitDeadline)} ET`)
  if (!hold.length) reasons.push('MAXIMUM_HOLD_UNAVAILABLE')
  return { stop, target, stopPrice: stop != null && price.multiplier ? stop / price.multiplier : null,
    targetPrice: target != null && price.multiplier ? target / price.multiplier : null,
    risk, riskFraction: risk != null && entry ? risk / entry : null,
    rewardRisk: risk != null && risk > 0 && reward != null ? reward / risk : null,
    hold: hold.length ? hold.join(' / ') : 'Unavailable', reasons,
    basis: explicit ? 'PLANNED_ENTRY_LIMIT_NOT_FILL' : 'ORIGINAL_CANDIDATE_MARKS',
    closeKind: credit ? 'BUY_TO_CLOSE_COST' : 'SELL_TO_CLOSE_VALUE' }
}

export function optionRemainingHold(deadline: string | null | undefined, now = Date.now()): string {
  const end = deadline ? Date.parse(deadline) : NaN
  if (!Number.isFinite(end)) return 'Unavailable'
  if (end <= now) return 'Deadline elapsed'
  return `${((end - now) / 3600000).toLocaleString('en-US', { maximumFractionDigits: 1 })} hours`
}

export function optionLegMarketValues(leg: Partial<OptionCandidateLeg>): Record<string, string> {
  const spot = marketNumber(leg.spot)
  const strike = marketNumber(leg.strike)
  const volume = marketNumber(leg.day_volume)
  const interest = marketNumber(leg.open_interest)
  const numeric = (value: unknown, digits = 4) => {
    const parsed = marketNumber(value)
    return parsed == null ? 'Unavailable' : parsed.toLocaleString('en-US', { maximumFractionDigits: digits })
  }
  const count = (value: number | null) => value != null && value >= 0 && Number.isInteger(value) ? numeric(value, 0) : 'Unavailable'
  const otm = spot != null && spot > 0 && strike != null && strike > 0 && ['CALL', 'PUT'].includes(leg.contract_type || '')
    ? (leg.contract_type === 'CALL' ? strike - spot : spot - strike) / spot : null
  return {
    stock: spot != null && spot > 0 ? optionAlertMoney(spot) : 'Unavailable',
    otm: optionAlertPercent(otm), option_price: optionAlertMoney(leg.model_mark),
    volume: count(volume), open_interest: count(interest), iv: optionAlertPercent(leg.local_iv),
    delta: numeric(leg.local_delta), gamma: numeric(leg.local_gamma), theta: numeric(leg.local_theta_per_day),
    vega: numeric(leg.local_vega_per_vol_point), rho: numeric(leg.local_rho_per_rate_point),
    bid_ask: leg.quote_bid == null || leg.quote_ask == null ? 'Unavailable' : `${optionAlertMoney(leg.quote_bid)} / ${optionAlertMoney(leg.quote_ask)}`,
    volume_oi: volume != null && volume >= 0 && interest != null && interest > 0 ? `${numeric(volume / interest, 2)}x` : 'Unavailable',
    mark_time: optionAlertTime(leg.source_market_time), mark_source: leg.mark_source ? optionAlertLabel(leg.mark_source) : 'Unavailable',
  }
}

export function optionContractExpired(expirationDate: string | null | undefined, asOf: string | null | undefined): boolean {
  if (!expirationDate || !asOf || !/^\d{4}-\d{2}-\d{2}$/.test(expirationDate)) return false
  const currentSession = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(new Date(asOf))
  return expirationDate < currentSession
}