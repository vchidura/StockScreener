import type { EligibleChainRow, OptionDiscoveryCatalog } from '../services/api'

export const OPTIONS_LIBRARY_KEY = 'options-screen-library-v1'
export type OptionsView = 'contracts' | 'packages' | 'chain'
export type OptionsColumn = { key: string; label: string; locked?: boolean; group?: string; hiddenByDefault?: boolean; tip?: string }
export const contractColumns: OptionsColumn[] = [
  { key: 'underlying', label: 'Ticker', locked: true }, { key: 'contract', label: 'Contract', locked: true },
  { key: 'calendar_dte', label: 'DTE' }, { key: 'spot', label: 'Stock' }, { key: 'otm_fraction', label: 'OTM %' },
  { key: 'mark', label: 'Mark' }, { key: 'mark_change_fraction', label: 'Change %' },
  { key: 'day_volume', label: 'Volume' }, { key: 'open_interest', label: 'OI' },
  { key: 'open_interest_change', label: 'OI change' }, { key: 'open_interest_change_fraction', label: 'OI change %' }, { key: 'volume_open_interest_ratio', label: 'Vol / OI' },
  { key: 'premium_activity', label: 'Premium activity' }, { key: 'local_iv', label: 'IV' },
  { key: 'iv_change', label: 'IV change' }, { key: 'local_delta', label: 'Delta' }, { key: 'local_gamma', label: 'Gamma' },
  { key: 'strategy_names', label: 'Detected by' },
  { key: 'board_position', label: 'Board' }, { key: 'market_data_time', label: 'Source (ET)' },
]
export const packageColumns: OptionsColumn[] = [
  { key: 'underlying', label: 'Ticker', locked: true }, { key: 'structure_type', label: 'Structure', locked: true },
  { key: 'legs', label: 'Ordered legs', locked: true }, { key: 'calendar_dte', label: 'DTE' },
  { key: 'net_premium', label: 'Credit / debit' }, { key: 'capital_at_risk', label: 'Capital at risk' },
  { key: 'collateral_required', label: 'Collateral' }, { key: 'maximum_profit', label: 'Max expiration profit' },
  { key: 'maximum_loss', label: 'Max expiration loss' }, { key: 'breakevens', label: 'Breakevens' },
  { key: 'structure_risk_class', label: 'Risk class' }, { key: 'display_name', label: 'Model' },
  { key: 'market_data_time', label: 'Source (ET)' }, { key: 'eligibility', label: 'Eligibility' },
]
export const chainColumns: OptionsColumn[] = [
  { key: 'market_data_time', label: 'Source (ET)', group: 'Evidence' },
  { key: 'underlying', label: 'Ticker', group: 'Contract', locked: true },
  { key: 'contract', label: 'Contract', group: 'Contract', locked: true },
  { key: 'calendar_dte', label: 'DTE', group: 'Contract', tip: 'Calendar days to expiry at the retained source time.' },
  { key: 'spot', label: 'Stock at source', group: 'Price & activity', tip: 'Underlying spot in the retained snapshot, not a live price.' },
  { key: 'otm_fraction', label: '% OTM', group: 'Price & activity', tip: 'Signed distance from spot to strike; negative means in the money.' },
  { key: 'model_mark', label: 'Option price / share', group: 'Price & activity', tip: 'Retained model mark in USD per share, not a current quote, last trade or fill.' },
  { key: 'day_volume', label: 'Volume', group: 'Price & activity', tip: 'Contract day volume in the retained snapshot.' },
  { key: 'open_interest', label: 'OI', group: 'Price & activity', tip: 'Snapshot open interest, not intraday position changes.' },
  { key: 'local_iv', label: 'IV', group: 'Greeks & volatility', tip: 'Retained local model implied volatility, displayed as a percentage.' },
  { key: 'local_delta', label: 'Delta', group: 'Greeks & volatility', hiddenByDefault: true, tip: 'Signed local model delta, not a calibrated probability.' },
  { key: 'absolute_delta', label: 'Absolute delta', group: 'Greeks & volatility', hiddenByDefault: true },
  { key: 'local_gamma', label: 'Gamma', group: 'Greeks & volatility', hiddenByDefault: true },
  { key: 'local_theta_per_day', label: 'Theta / day', group: 'Greeks & volatility', hiddenByDefault: true, tip: 'USD per share per day.' },
  { key: 'local_vega_per_vol_point', label: 'Vega / vol point', group: 'Greeks & volatility', hiddenByDefault: true, tip: 'USD per share per volatility percentage point.' },
  { key: 'local_rho_per_rate_point', label: 'Rho / rate point', group: 'Greeks & volatility', hiddenByDefault: true, tip: 'USD per share per interest-rate percentage point.' },
  { key: 'volume_open_interest_ratio', label: 'Volume / OI', group: 'Price & activity', hiddenByDefault: true, tip: 'Unavailable when snapshot open interest is zero or missing.' },
  { key: 'bid_ask', label: 'Bid / ask', group: 'Price & activity', hiddenByDefault: true, tip: 'Retained quotes only; unavailable quotes are not inferred from model marks.' },
  { key: 'mark_market_data_time', label: 'Option price time (ET)', group: 'Evidence', hiddenByDefault: true },
  { key: 'mark_source', label: 'Option price basis', group: 'Evidence', hiddenByDefault: true },
  { key: 'first_observed_at', label: 'Observed (ET)', group: 'Evidence', hiddenByDefault: true },
  { key: 'quality_flags', label: 'Quality flags', group: 'Evidence', hiddenByDefault: true },
]
export const columnsForView = (view: OptionsView) => view === 'packages' ? packageColumns : view === 'chain' ? chainColumns : contractColumns

export function visibleOptionsColumns(view: OptionsView, selected: string | null = null): OptionsColumn[] {
  const keys = selected?.split(',')
  return columnsForView(view).filter(column => column.locked || (keys ? keys.includes(column.key) : !column.hiddenByDefault))
}

type ContractDetailFormat = 'text' | 'money' | 'percent' | 'number' | 'time' | 'boolean' | 'flags'
const contractDetailGroups: Array<{ title: string; fields: Array<[keyof EligibleChainRow, string, ContractDetailFormat?]> }> = [
  { title: 'Contract terms', fields: [
    ['underlying', 'Underlying'], ['contract_ticker', 'Contract'], ['contract_id', 'Contract ID'],
    ['contract_type', 'Type'], ['asset_type', 'Underlying asset type'], ['expiration_date', 'Expiration'],
    ['expiration_cutoff', 'Expiration cutoff (ET)', 'time'], ['calendar_dte', 'DTE at source', 'number'],
    ['strike', 'Strike / share', 'money'], ['shares_per_contract', 'Contract multiplier', 'number'],
    ['exercise_style', 'Exercise style'], ['otm_fraction', 'Signed OTM distance', 'percent'],
  ] },
  { title: 'Prices and economics', fields: [
    ['spot', 'Underlying spot', 'money'], ['model_mark', 'Model mark / share', 'money'],
    ['display_mark', 'Display mark / share', 'money'], ['mark_source', 'Mark basis'],
    ['intrinsic_value', 'Intrinsic value / share', 'money'], ['extrinsic_value', 'Extrinsic value / share', 'money'],
    ['single_contract_breakeven', 'Long-option expiry breakeven', 'money'],
    ['bid', 'Retained bid / share', 'money'], ['ask', 'Retained ask / share', 'money'], ['midpoint', 'Retained midpoint / share', 'money'],
  ] },
  { title: 'Greeks and volatility', fields: [
    ['local_iv', 'Local IV', 'percent'], ['local_delta', 'Delta', 'number'], ['absolute_delta', 'Absolute delta', 'number'],
    ['local_gamma', 'Gamma', 'number'], ['local_theta_per_day', 'Theta / share / day', 'number'],
    ['local_vega_per_vol_point', 'Vega / share / vol point', 'number'], ['local_rho_per_rate_point', 'Rho / share / rate point', 'number'],
    ['provider_iv', 'Provider IV', 'percent'], ['provider_gamma', 'Provider gamma', 'number'],
  ] },
  { title: 'Activity', fields: [
    ['day_volume', 'Day volume (contracts)', 'number'], ['open_interest', 'Snapshot open interest', 'number'],
    ['volume_open_interest_ratio', 'Volume / OI ratio', 'number'],
  ] },
  { title: 'Model quality and assumptions', fields: [
    ['model_version', 'Model version'], ['iv_converged', 'IV converged', 'boolean'], ['iv_solver', 'IV solver'],
    ['iv_iteration_count', 'Solver iterations', 'number'], ['iv_price_error', 'IV price error / share', 'number'],
    ['iv_failure_reason', 'IV failure reason'], ['quality_flags', 'Quality flags', 'flags'],
    ['risk_free_rate', 'Risk-free rate', 'percent'], ['dividend_yield', 'Dividend yield', 'percent'],
    ['time_to_expiration_years', 'Time to expiration (years)', 'number'],
  ] },
  { title: 'Source times', fields: [
    ['market_data_time', 'Snapshot market time (ET)', 'time'], ['spot_market_data_time', 'Stock price time (ET)', 'time'],
    ['mark_market_data_time', 'Option price time (ET)', 'time'], ['first_observed_at', 'First observed (ET)', 'time'],
    ['revised_observed_at', 'Revised observation (ET)', 'time'], ['data_delay_seconds', 'Recorded data delay (seconds)', 'number'],
    ['created_at', 'Stored (ET)', 'time'], ['updated_at', 'Last stored update (ET)', 'time'],
  ] },
  { title: 'Provenance', fields: [
    ['provider', 'Provider'], ['snapshot_id', 'Snapshot ID'], ['batch_id', 'Batch ID'], ['matrix_id', 'Matrix ID'],
    ['revision', 'Snapshot revision', 'number'], ['policy_version', 'Market policy version'], ['policy_sha256', 'Market policy SHA256'],
    ['valuation_policy_version', 'Valuation policy version'], ['valuation_policy_sha256', 'Valuation policy SHA256'],
    ['raw_payload_sha256', 'Raw payload SHA256'], ['normalized_payload_sha256', 'Normalized payload SHA256'],
  ] },
]

export function optionContractDetailSections(row: EligibleChainRow) {
  return contractDetailGroups.map(group => ({ title: group.title,
    fields: group.fields.map(([key, label, format = 'text']) => ({ key, label, format, value: row[key] ?? null })),
  }))
}

export function optionColumnPresets(view: OptionsView): Record<string, string[]> {
  const all = columnsForView(view).map(column => column.key)
  if (view === 'packages') return { All: all, Economics: ['underlying', 'structure_type', 'legs', 'net_premium', 'capital_at_risk', 'maximum_profit', 'maximum_loss', 'eligibility'] }
  const shared = ['underlying', 'contract', 'calendar_dte', 'market_data_time']
  return {
    ...(view === 'chain' ? { 'Price / activity': visibleOptionsColumns(view).map(column => column.key) } : {}),
    All: all,
    'Greeks / volatility': [...shared, view === 'chain' ? 'model_mark' : 'mark', 'local_iv', ...(view === 'chain' ? ['local_delta', 'absolute_delta'] : ['local_delta']), 'local_gamma', ...(view === 'chain' ? ['local_theta_per_day', 'local_vega_per_vol_point', 'local_rho_per_rate_point'] : [])],
    Activity: [...shared, 'day_volume', 'open_interest', 'volume_open_interest_ratio'],
  }
}
export type OptionsScreen = { id: string; name: string; query: Record<string, string>; created_at: string; updated_at: string }
export type OptionsLibrary = { schema_version: 1; screens: OptionsScreen[]; tabs?: string[] }
const allowedKeys = new Set(['view', 'category', 'model', 'scope', 'underlyer', 'type', 'min_dte', 'max_dte', 'min_volume', 'min_oi', 'min_ratio', 'sort', 'max_capital', 'columns', 'preset'])
const presetIds = ['all', 'unusual-calls', 'unusual-puts', 'structured', 'sweep', 'smile', 'long-calls', 'long-puts', 'board']
const contractSorts = ['PREMIUM_ACTIVITY', 'VOLUME', 'OPEN_INTEREST', 'VOLUME_OI', 'OI_CHANGE', 'IV']
const packageSorts = ['DEFAULT', 'CAPITAL_ASC', 'DTE_ASC']

export function validateScreenQuery(input: Record<string, string>, catalog: OptionDiscoveryCatalog): Record<string, string> {
  const query: Record<string, string> = {}
  for (const key of Object.keys(input).sort()) {
    const rangeKey = input.view === 'chain' && Object.keys(catalog.contract_filters || {}).some(field => key === `range_${field}_min` || key === `range_${field}_max`)
    if (!(allowedKeys.has(key) || rangeKey || input.view === 'chain' && key === 'descending') || typeof input[key] !== 'string' || input[key].length > 2000) throw new Error(`Unsupported screen field: ${key}`)
    if (input[key] !== '') query[key] = input[key]
  }
  const view = query.view || 'contracts'
  if (!['contracts', 'packages', 'chain'].includes(view)) throw new Error('Unsupported screen view')
  if (query.category && !catalog.categories.some(item => item.id === query.category)) throw new Error('Unknown category')
  const model = catalog.models.find(item => item.id === query.model)
  if (query.model && !model) throw new Error('Unknown model')
  if (query.category && model && !model.structures.some(item => item.category_ids.some(id => id === query.category))) throw new Error('Model does not match category')
  if (query.scope && !['ALL', 'STRUCTURED', 'RESEARCH', 'BOARD'].includes(query.scope)) throw new Error('Unknown evidence scope')
  if (query.type && !['ALL', 'CALL', 'PUT'].includes(query.type)) throw new Error('Unknown contract type')
  if (query.underlyer && !/^[A-Z][A-Z0-9.]{0,14}$/.test(query.underlyer)) throw new Error('Invalid underlying')
  if (query.preset && !presetIds.includes(query.preset)) throw new Error('Unknown preset')
  for (const key of ['min_dte', 'max_dte', 'min_volume', 'min_oi', 'min_ratio', 'max_capital']) {
    if (query[key] === undefined) continue
    const value = Number(query[key])
    if (!query[key].trim() || !Number.isFinite(value) || value < 0 || value > 1e12) throw new Error(`Invalid ${key}`)
    if (['min_dte', 'max_dte', 'min_volume', 'min_oi'].includes(key) && !Number.isInteger(value)) throw new Error(`Whole number required for ${key}`)
    if (key.endsWith('dte') && value > 365) throw new Error('DTE cannot exceed 365')
    query[key] = String(value)
  }
  if (Number(query.min_dte || 0) > Number(query.max_dte || 60)) throw new Error('Minimum DTE exceeds maximum DTE')
  if (query.sort && !(view === 'packages' ? packageSorts : view === 'chain' ? Object.keys(catalog.contract_filters || {}) : contractSorts).includes(query.sort)) throw new Error('Sort does not match view')
  if (view === 'chain') {
    if (!catalog.contract_filters) throw new Error('Eligible-chain filter catalog unavailable')
    if (['category', 'model', 'scope', 'preset', 'min_dte', 'max_dte', 'min_volume', 'min_oi', 'min_ratio', 'max_capital'].some(key => key in query)) throw new Error('Detection or package filters cannot be applied to the eligible chain')
    if (query.descending && !['0', '1'].includes(query.descending)) throw new Error('Invalid sort direction')
    chainFilterRanges(query, catalog)
  }
  if (view === 'packages') {
    if (model?.output_kind === 'OBSERVATION') throw new Error('Observation models do not produce packages')
    if (['preset', 'type', 'min_volume', 'min_oi', 'min_ratio'].some(key => key in query) || query.scope && query.scope !== 'STRUCTURED') throw new Error('Contract filters cannot be applied to packages')
  } else if ('max_capital' in query) throw new Error('Capital limit requires package view')
  if (query.columns !== undefined) {
    const columns = columnsForView(view as OptionsView)
    const selected = query.columns.split(',')
    if (new Set(selected).size !== selected.length || selected.some(key => !columns.some(column => column.key === key))) throw new Error('Invalid screen columns')
    if (columns.some(column => column.locked && !selected.includes(column.key))) throw new Error('Required screen columns cannot be hidden')
  }
  return query
}

export function chainFilterRanges(query: Record<string, string>, catalog: OptionDiscoveryCatalog) {
  const filters: { field: string; minimum?: number; maximum?: number }[] = []
  for (const [field, specification] of Object.entries(catalog.contract_filters || {})) {
    const bounds: { field: string; minimum?: number; maximum?: number } = { field }
    for (const [key, suffix] of [['minimum', 'min'], ['maximum', 'max']] as const) {
      const raw = query[`range_${field}_${suffix}`]
      if (raw == null || raw === '') continue
      const value = Number(raw) / (specification.unit === 'fraction' ? 100 : 1)
      if (!raw.trim() || !Number.isFinite(value) || Math.abs(value) > 1e12 || value < (specification.minimum ?? -Infinity) || value > (specification.maximum ?? Infinity) || specification.integer && !Number.isInteger(value)) throw new Error(`Invalid ${specification.label} ${key}`)
      bounds[key] = value
    }
    if (bounds.minimum != null && bounds.maximum != null && bounds.minimum > bounds.maximum) throw new Error(`${specification.label}: minimum exceeds maximum`)
    if (bounds.minimum != null || bounds.maximum != null) filters.push(bounds)
  }
  return filters
}

export function switchOptionsView(params: URLSearchParams, view: OptionsView) {
  const next = new URLSearchParams({ view })
  for (const key of ['underlyer', 'session_date']) if (params.has(key)) next.set(key, params.get(key)!)
  if (params.get('view') !== 'chain' && view !== 'chain') {
    for (const key of ['category', 'min_dte', 'max_dte']) if (params.has(key)) next.set(key, params.get(key)!)
  }
  return next
}

export function screenQuery(params: URLSearchParams, catalog: OptionDiscoveryCatalog) {
  return validateScreenQuery(Object.fromEntries([...params].filter(([key]) => !['screen', 'builtin', 'offset', 'session_date'].includes(key))), catalog)
}

export function readOptionsLibrary(storage: Pick<Storage, 'getItem'>, catalog: OptionDiscoveryCatalog): OptionsLibrary {
  const raw = storage.getItem(OPTIONS_LIBRARY_KEY)
  if (raw === null) return { schema_version: 1, screens: [] }
  if (raw.length > 250_000) throw new Error('Options library exceeds size limit')
  const data = JSON.parse(raw)
  if (data?.schema_version !== 1 || !Array.isArray(data.screens) || data.screens.length > 100) throw new Error('Unsupported options library')
  const ids = new Set<string>()
  const screens = data.screens.map((screen: OptionsScreen) => {
    if (!screen || typeof screen.id !== 'string' || !/^[a-zA-Z0-9-]{1,80}$/.test(screen.id) || ids.has(screen.id)) throw new Error('Invalid or duplicate screen ID')
    ids.add(screen.id)
    if (typeof screen.name !== 'string' || !screen.name.trim() || screen.name.length > 80) throw new Error('Screen name must contain 1-80 characters')
    if (![screen.created_at, screen.updated_at].every(value => typeof value === 'string' && Number.isFinite(Date.parse(value)))) throw new Error('Invalid screen timestamp')
    if (!screen.query || typeof screen.query !== 'object' || Array.isArray(screen.query)) throw new Error('Invalid saved filters')
    return { ...screen, query: validateScreenQuery(screen.query, catalog) }
  })
  if (data.tabs !== undefined && (!Array.isArray(data.tabs) || data.tabs.length > 100 || data.tabs.some((key: unknown) => typeof key !== 'string' || !/^(builtin|saved):[a-zA-Z0-9-]{1,80}$/.test(key)))) throw new Error('Invalid open screen tabs')
  return { schema_version: 1, screens, ...(data.tabs === undefined ? {} : { tabs: [...new Set(data.tabs)] as string[] }) }
}

export function writeOptionsLibrary(storage: Pick<Storage, 'setItem'>, library: OptionsLibrary, catalog: OptionDiscoveryCatalog) {
  const serialized = JSON.stringify(library)
  readOptionsLibrary({ getItem: () => serialized }, catalog)
  storage.setItem(OPTIONS_LIBRARY_KEY, serialized)
}

export function makeOptionsScreen(name: string, query: Record<string, string>, catalog: OptionDiscoveryCatalog, prior?: OptionsScreen): OptionsScreen {
  if (!name.trim() || name.trim().length > 80) throw new Error('Screen name must contain 1-80 characters')
  const now = new Date().toISOString()
  return { id: prior?.id || crypto.randomUUID(), name: name.trim(), query: validateScreenQuery(query, catalog), created_at: prior?.created_at || now, updated_at: now }
}

export type OptionTemplate = { id: string; name: string; query: Record<string, string>; summary: string; unavailable?: string }

export function optionTemplates(catalog?: OptionDiscoveryCatalog): OptionTemplate[] {
  const missing = catalog?.contract_filters ? undefined : 'Contract screening fields are unavailable.'
  return [
    { id: 'all', name: 'All options', query: { view: 'chain' }, summary: 'All eligible retained contracts, independent of alert detections.', unavailable: missing },
    { id: 'unusual-calls', name: 'Unusual Calls', query: { view: 'chain', type: 'CALL', range_volume_open_interest_ratio_min: '3' }, summary: 'Calls with volume at least 3 times snapshot open interest. Activity does not imply buying or opening flow.', unavailable: missing },
    { id: 'unusual-puts', name: 'Unusual Puts', query: { view: 'chain', type: 'PUT', range_volume_open_interest_ratio_min: '3' }, summary: 'Puts with volume at least 3 times snapshot open interest. Activity does not imply selling or opening flow.', unavailable: missing },
    { id: 'long-calls', name: 'Long-Dated Calls', query: { view: 'chain', type: 'CALL', range_calendar_dte_min: '21' }, summary: 'Listed calls with at least 21 calendar days to expiration, within retained coverage.', unavailable: missing },
    { id: 'long-puts', name: 'Long-Dated Puts', query: { view: 'chain', type: 'PUT', range_calendar_dte_min: '21' }, summary: 'Listed puts with at least 21 calendar days to expiration, within retained coverage.', unavailable: missing },
    { id: 'chain-delta', name: '20-40 delta / 21-45 DTE', query: { view: 'chain', range_absolute_delta_min: '0.2', range_absolute_delta_max: '0.4', range_calendar_dte_min: '21', range_calendar_dte_max: '45' }, summary: 'Contracts filtered by absolute delta and expiration, not a strategy recommendation.', unavailable: missing },
    { id: 'structured', name: 'Structured legs', query: { view: 'chain' }, summary: 'Former model-selected structure tab.', unavailable: 'Requires selected strategy packages. Alert-model outputs are not contract screening rules.' },
    { id: 'sweep', name: 'Sweep-like', query: { view: 'chain' }, summary: 'Former trade-cluster detector tab.', unavailable: 'No independent trade-cluster screening field is published. The alert detector is not run by the screener.' },
    { id: 'smile', name: 'Smile distortion', query: { view: 'chain' }, summary: 'Former volatility-detector tab.', unavailable: 'No independent smile-residual screening field is published. The alert detector is not run by the screener.' },
    { id: 'board', name: 'Published Board', query: { view: 'chain' }, summary: 'Former selected-model Board tab.', unavailable: 'Board membership is an alert-model selection, not an independent screener.' },
  ]
}

export const optionFilterGroups: Record<string, string[]> = {
  'Expiration & price': ['calendar_dte', 'model_mark', 'spot', 'otm_fraction'],
  'Greeks & volatility': ['absolute_delta', 'local_iv', 'local_gamma', 'local_theta_per_day', 'local_vega_per_vol_point'],
  'Volume & open interest': ['day_volume', 'open_interest', 'volume_open_interest_ratio'],
}

export function independentScreenQuery(params: URLSearchParams, catalog: OptionDiscoveryCatalog) {
  const raw = Object.fromEntries([...params].filter(([key]) => !['screen', 'builtin', 'offset', 'session_date'].includes(key)))
  if (raw.view && raw.view !== 'chain' || ['model', 'category', 'scope', 'preset'].some(key => key in raw)) throw new Error('This legacy screen uses alert-model output. Its saved definition is preserved; choose a contract screen from the library.')
  return validateScreenQuery({ ...raw, view: 'chain' }, catalog)
}

export function optionRuleLabels(query: Record<string, string>, catalog: OptionDiscoveryCatalog): string[] {
  const labels: string[] = []
  if (query.underlyer) labels.push(`Underlying: ${query.underlyer}`)
  if (query.type && query.type !== 'ALL') labels.push(query.type === 'CALL' ? 'Calls' : 'Puts')
  for (const [field, spec] of Object.entries(catalog.contract_filters || {})) {
    const lower = query[`range_${field}_min`]
    const upper = query[`range_${field}_max`]
    if (lower == null && upper == null) continue
    labels.push(`${spec.label}: ${lower != null ? `>= ${lower}` : ''}${lower != null && upper != null ? ', ' : ''}${upper != null ? `<= ${upper}` : ''}${spec.unit === 'fraction' ? '%' : ` ${spec.unit}`}`)
  }
  return labels.length ? labels : ['All eligible retained contracts']
}