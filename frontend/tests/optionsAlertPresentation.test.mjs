import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const source = readFileSync(new URL('../src/pages/optionsAlertPresentation.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const model = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)

test('surface observations distinguish unretained market fields from inapplicable package terms', () => {
  const observation = { evaluation_id: 'surface', detector_id: 'O2', triggered_at: null,
    observation: { decision_at: '2026-09-22T14:09:53Z', market_cutoff: '2026-09-22T13:54:16Z', findings: [] } }
  const packageRow = { evaluation_id: 'package', detector_id: 'O1', triggered_at: '2026-09-22T14:00:00Z' }
  const original = structuredClone(observation)
  const result = model.detectorObservationFieldStates(observation)
  for (const key of ['stock', 'otm', 'delta', 'volume', 'open_interest', 'bid_ask', 'mark_time']) assert.equal(result[key], 'Not retained')
  for (const key of ['triggered_at', 'option_price', 'leg_prices', 'current_price', 'price_pnl', 'technical_stop', 'technical_target', 'net_return', 'capital', 'maximum_profit']) assert.equal(result[key], 'Not applicable')
  assert.equal(result.outcome_status, 'Observation only')
  assert.deepEqual(model.detectorObservationFieldStates(packageRow), {})
  assert.deepEqual(observation, original)
})

test('O2 alerts render observation evidence without package actions', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /rawDetector === 'O2'/)
  assert.doesNotMatch(page, /row\.management_policy\?\.technical_exit/)
  assert.match(page, /credit \? <CreditObservationDetails observation=\{credit\} \/> : surface \? <>/)
  assert.match(page, /data\?\.rows\.map/)
  assert.match(page, /aria-label="Detector alerts table"/)
  assert.doesNotMatch(page, /showPackageTools|IV surface observations table|<h3>IV surface observations/)
  assert.match(page, /<label>Column preset<select aria-label="Alert column preset"/)
  assert.match(page, /<div className="oa-detector-tools"><ColumnPicker columns=\{detectorAlertColumns\}/)
  assert.match(page, /source_cutoff: time\(surface\.market_cutoff\), analyzed_at: time\(surface\.decision_at\)/)
  assert.doesNotMatch(page, /option_price: 'Not a package'/)
  assert.match(page, /title="Observation evidence"/)
  assert.match(page, /'Retained alert evidence \/ Delayed marks'/)
  assert.match(page, /Not a fill or execution permission/)
})

test('detector sorting uses explicit server keys and resets pagination', () => {
  const params = new URLSearchParams('evaluation_dataset=frozen-v1&detector_sort=strategy&detector_order=asc&offset=50')
  assert.deepEqual(model.optionDetectorSort(params), { sort_by: 'strategy', sort_order: 'asc' })
  assert.deepEqual(model.optionDetectorSortChange(params, 'strategy'), { detector_sort: 'strategy', detector_order: 'desc', offset: null })
  assert.deepEqual(model.optionDetectorSortChange(params, 'category'), { detector_sort: 'category', detector_order: 'asc', offset: null })
  assert.equal(params.get('evaluation_dataset'), 'frozen-v1')
  assert.deepEqual(model.optionDetectorSort(new URLSearchParams('detector_sort=invalid')), { sort_by: 'run', sort_order: 'desc' })
  assert.deepEqual(model.optionDetectorSort(new URLSearchParams('detector_sort=triggered_at&detector_order=asc')), { sort_by: 'triggered_at', sort_order: 'asc' })
  assert.equal(model.optionPackageStrategyName('INCOME_WHEEL'), 'Income / Wheel')
  assert.equal(model.optionPackageStrategyName(null), 'Not applicable')
})

test('detector columns restore shared screener fields and presets exclude transient scope', () => {
  const keys = model.detectorAlertColumns.map(column => column.key)
  assert.equal(new Set(keys).size, keys.length)
  for (const key of ['contracts', 'expiry', 'stock', 'option_price', 'leg_prices', 'current_price', 'price_pnl', 'technical_stop', 'technical_target', 'volume', 'open_interest', 'iv', 'delta', 'gamma', 'theta', 'vega', 'rho', 'volume_oi', 'bid_ask', 'maximum_profit']) assert.ok(keys.includes(key))
  for (const key of ['entry_limit', 'plan_stop', 'plan_target', 'risk_to_stop', 'reward_risk', 'maximum_hold', 'risk_assessment', 'entry_window', 'exit_due']) assert.ok(!keys.includes(key))
  assert.ok(!('Trade plan' in model.detectorColumnPresets))
  for (const columns of Object.values(model.detectorColumnPresets)) for (const key of columns) assert.ok(keys.includes(key))
  const defaults = model.detectorAlertColumns.filter(column => !column.hiddenByDefault).map(column => column.key)
  const market = model.optionMarketColumns.flatMap(column => column.key === 'option_price' || ['theta', 'vega', 'rho', 'bid_ask', 'mark_time', 'mark_source'].includes(column.key) ? [] : column.key === 'leg_prices'
    ? ['leg_prices', 'option_price', 'current_price', 'price_pnl', 'technical_stop', 'technical_target'] : [column.key])
  assert.deepEqual(defaults, ['details', 'triggered_at', 'underlyer', 'contracts', 'expiry', ...market])
  assert.deepEqual(model.detectorColumnPresets['Package / market data'], defaults)
  assert.equal(model.detectorAlertColumns.find(column => column.key === 'option_price').label, 'Trigger price')
  assert.deepEqual(defaults.slice(defaults.indexOf('leg_prices'), defaults.indexOf('volume')), ['leg_prices', 'option_price', 'current_price', 'price_pnl', 'technical_stop', 'technical_target'])
  assert.deepEqual(model.detectorColumnPresets['Greeks / volatility'], ['triggered_at', 'underlyer', 'contracts', 'expiry', 'stock', 'option_price', 'iv', 'delta', 'gamma', 'theta', 'vega', 'rho'])
  assert.deepEqual(model.detectorAlertColumns.slice(0, 3).map(column => [column.key, column.locked]),
    [['details', true], ['triggered_at', true], ['underlyer', true]])
  for (const columns of Object.values(model.detectorColumnPresets)) assert.ok(columns.includes('triggered_at'))
  assert.deepEqual(keys.slice(-3), ['detector', 'category', 'strategy'])
  assert.ok(model.detectorColumnPresets['Price / activity'].includes('volume'))
  for (const key of ['iv', 'fitted_iv', 'residual', 'robust_z', 'source_cutoff', 'analyzed_at']) {
    assert.ok(keys.includes(key))
    assert.ok(model.detectorColumnPresets['IV observation evidence'].includes(key))
  }
  for (const key of ['theta', 'vega', 'rho', 'bid_ask', 'mark_time', 'mark_source', 'fitted_iv', 'residual', 'robust_z', 'source_cutoff', 'analyzed_at']) {
    assert.equal(model.detectorAlertColumns.find(column => column.key === key).hiddenByDefault, true)
  }
  const preset = model.detectorViewPreset('My activity', new URLSearchParams('underlyer=AAPL&evaluation_detector=O1&session_date=2026-09-18&evaluation_dataset=old&offset=50&detector_sort=run'), ['iv', 'volume'])
  assert.deepEqual(preset.query, { underlyer: 'AAPL', evaluation_detector: 'O1', detector_sort: 'run' })
  assert.deepEqual(preset.columns, ['details', 'triggered_at', 'underlyer', 'iv', 'volume'])
  assert.equal(model.detectorViewPreset('Surface', new URLSearchParams('evaluation_detector=O2'), ['iv']).query.evaluation_detector, 'O2')
  let saved
  model.writeDetectorPresets({ setItem: (_, value) => { saved = value } }, [preset])
  assert.deepEqual(model.readDetectorPresets({ getItem: () => saved }), [preset])
  assert.throws(() => model.readDetectorPresets({ getItem: () => '{invalid' }))
  assert.throws(() => model.writeDetectorPresets({ setItem: () => {} }, [preset, preset]))
  assert.throws(() => model.writeDetectorPresets({ setItem: () => { throw Error('storage blocked') } }, [preset]), /storage blocked/)
})

test('entry timing never asserts qualification and closes at the exact boundary', () => {
  const now = Date.parse('2026-09-17T20:00:00Z')
  assert.equal(model.optionEntryWindow('2026-09-17T20:00:01Z', now), 'OPEN')
  assert.equal(model.optionEntryWindow('2026-09-17T20:00:00Z', now), 'ELAPSED')
  assert.equal(model.optionEntryWindow(null, now), 'UNAVAILABLE')
  assert.equal(model.optionEntryWindow('invalid', now), 'UNAVAILABLE')
})

test('missing monetary data is never shown as zero or unlimited profit', () => {
  for (const value of [null, undefined, '', '   ', 'NaN', Infinity, {}]) assert.equal(model.optionAlertMoney(value), 'Unavailable')
  assert.equal(model.optionAlertMoney('0'), '$0.00')
  assert.equal(model.optionAlertMoney('-200.25'), '-$200.25')
})

test('stock behavior gates retain timeframe and metric units', () => {
  assert.equal(model.optionGateTimeframe('TREND.1d'), 'Daily')
  assert.equal(model.optionGateTimeframe('TREND.1h'), 'Hourly')
  assert.equal(model.optionGateTimeframe('TREND.30m'), '30 minute')
  assert.equal(model.optionGateTimeframe(null), 'Assessment')
  assert.equal(model.optionGateValue({ metric_id: 'ema50_slope10_atr', actual_float: 0.125, actual_text: null }), '0.125 ATR / bar')
  assert.equal(model.optionGateValue({ metric_id: 'adx14', actual_float: 21.25, actual_text: null }), '21.3 / 100')
  assert.equal(model.optionGateValue({ metric_id: 'extension_ema21_atr', actual_float: -1.234, actual_text: null }), '-1.23 ATR')
  assert.equal(model.optionGateValue({ metric_id: 'daily_rvol20', actual_float: 1.2, actual_text: null }), '1.20x')
  assert.equal(model.optionGateValue({ metric_id: 'excess_return20', actual_float: 0.025, actual_text: null }), '2.5%')
  assert.equal(model.optionGateValue({ metric_id: 'median_dollar_volume20', actual_float: 1250000, actual_text: null }), '$1,250,000.00 / session')
  assert.equal(model.optionGateValue({ metric_id: null, actual_float: null, actual_text: 'READY' }), 'Ready')
})

test('empty preview leaves plan terms missing rather than inventing defaults', () => {
  assert.deepEqual(model.optionAlertPreviewRequest(model.emptyOptionAlertForm, 'INCOME_WHEEL'), {})
})

test('preview deadlines are explicit UTC and never include client assessment time', () => {
  const result = model.optionAlertPreviewRequest({ ...model.emptyOptionAlertForm, entryDeadline: '2026-09-18T14:05', exitDeadline: '2026-09-21T19:00', entryLimit: '200.25' }, 'INCOME_WHEEL')
  assert.deepEqual(result, { entry_deadline: '2026-09-18T14:05:00.000Z', exit_deadline: '2026-09-21T19:00:00.000Z', entry_limit: '200.25' })
  assert.equal(result.decision_at, undefined)
  for (const value of ['2026-02-30T10:00', 'tomorrow']) assert.throws(() => model.optionAlertPreviewRequest({ ...model.emptyOptionAlertForm, entryDeadline: value }, 'INCOME_WHEEL'))
})

test('explicit management uses entered percentages and elapsed hours once', () => {
  const form = { ...model.emptyOptionAlertForm, management: 'EXPLICIT', policyVersion: 'review_v1', stopPercent: '35', targetPercent: '50', holdHours: '48', exitDte: '1' }
  assert.deepEqual(model.optionAlertPreviewRequest(form, 'DIRECTIONAL_LONG_PREMIUM').management_policy, {
    policy_version: 'review_v1', strategy_name: 'DIRECTIONAL_LONG_PREMIUM', stop_loss_fraction: '0.35', take_profit_fraction: '0.5', maximum_hold_seconds: 172800, minimum_exit_dte: 1,
  })
  assert.throws(() => model.optionAlertPreviewRequest(form, 'INCOME_WHEEL'))
  for (const changes of [{ stopPercent: '100' }, { holdHours: 'Infinity' }, { exitDte: '0' }, { policyVersion: '' }]) assert.throws(() => model.optionAlertPreviewRequest({ ...form, ...changes }, 'DIRECTIONAL_LONG_PREMIUM'))
})

test('Alerts route is explicit and Research stays available with no publisher UI', () => {
  const app = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8')
  const navigation = readFileSync(new URL('../src/layout/navigation.ts', import.meta.url), 'utf8')
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(app, /path="\/options\/alerts" element=\{<OptionsAlertsPage/)
  assert.match(app, /path="\/options\/\*" element=\{<OptionsResearchWorkspace/)
  assert.match(navigation, /to: '\/options\/alerts', label: 'Options Alerts'/)
  assert.match(page, /structured_only: packagesOnly/)
  assert.match(page, /getOptionAlertHistory/)
  assert.match(page, /previewOptionAlert/)
  assert.doesNotMatch(page, /publishOption|createPosition|executeTrade|activatePublisher/)
})

test('preview refreshes cached assessments and retains focused submit during requests', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /staleTime: 0, refetchOnMount: 'always'/)
  assert.match(page, /if \(!candidate \|\| preview.isFetching\) return/)
  assert.match(page, /type="submit" className="oa-command" aria-disabled=\{preview.isFetching\}/)
  assert.match(page, /assessment\?\.plan_preview && !dirty && !preview.isFetching/)
})

test('candidate detail presents versioned evidence without adding action controls', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /Stock behavior evidence/)
  assert.match(page, /assessment\.detector_policy_version/)
  assert.match(page, /assessment\.detector_policy_sha256/)
  assert.match(page, /assessment\.launch_manifest_sha256/)
  assert.match(page, /Persisted package reward \/ risk/)
  assert.match(page, /Package scenarios and assumptions/)
  assert.match(page, /NO_QUALIFIED_CALIBRATION_REPORT/)
  assert.match(page, /Daily hourly and 30 minute stock behavior gates/)
  assert.match(page, /candidate\.candidate_kind === 'RESEARCH_ONLY'/)
  assert.match(page, /refetchOnWindowFocus: false/)
  assert.match(page, /<RecordedDecisionEvidence data=\{data\}/)
  assert.match(page, /Original selection evidence/)
  assert.match(page, /Trend and context/)
  assert.match(page, /Event calendar evidence/)
  assert.match(page, /data.market_event_evidence/)
  assert.match(page, /data.event_coverage_evidence/)
  assert.match(page, /Original execution gates/)
  assert.match(page, /data.execution_gates/)
  assert.match(page, /Model return on risk/)
  assert.match(page, /No event rows does not establish a clear calendar/)
  assert.match(page, /Original recorded decision \/ Not current qualification/)
  assert.doesNotMatch(page, /import .*OptionsResearchWorkspace|iframe/)
  assert.doesNotMatch(page, /publishOption|createPosition|executeTrade|activatePublisher/)
})

test('global staleness banner distinguishes an active close pipeline', () => {
  const banner = readFileSync(new URL('../src/layout/StalenessBanner.tsx', import.meta.url), 'utf8')
  const api = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8')
  assert.match(banner, /status\.status === 'EXPIRED' && status\.pipeline_active/)
  assert.match(banner, /Updating data/)
  assert.match(banner, /worker is active; no restart is needed/)
  assert.match(banner, /will remain blocked until the close pipeline catches up/)
  assert.match(api, /if \(status\.pipeline_active !== undefined\) return status/)
  assert.match(api, /run\.status === 'PENDING' \|\| run\.status === 'RUNNING'/)
})

test('behavior and EOD views reuse persisted readers without hiding the candidate inventory', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /getOptionBehaviorReview/)
  assert.match(page, /Directional stock gates passed/)
  assert.match(page, /Timely directional shortlist/)
  assert.match(page, /Model-selected complete packages/)
  assert.match(page, /EOD Review/)
  assert.match(source, /Outcome states/)
  assert.match(page, /!historyView && !reviewView && validDte/)
  assert.match(page, /No timely shortlist is available/)
  assert.match(page, /Partial-session assessment/)
  assert.match(page, /Exact assessment cohorts/)
  assert.match(page, /Correlated observations, not independent samples/)
  assert.match(page, /URLSearchParams\(\{ view: 'candidates' \}\)/)
})

test('column layouts preserve defaults, expose retained fields and protect identity', () => {
  const expected = { behavior: 22, day_history: 21, candidates: 21, history: 22, daily: 7 }
  for (const [view, count] of Object.entries(expected)) {
    const columns = model.optionAlertColumns[view]
    assert.equal(new Set(columns.map(column => column.key)).size, columns.length)
    const defaults = new Set(columns.filter(column => column.hiddenByDefault).map(column => column.key))
    assert.equal(model.optionAlertVisibleColumns(view, defaults).length, count)
    assert.equal(model.optionAlertVisibleColumns(view, new Set()).length, columns.length)
    assert.deepEqual(model.optionAlertVisibleColumns(view, new Set(columns.map(column => column.key))), columns.filter(column => column.locked))
  }
  assert.equal(model.optionBehaviorMetrics.length, 12)
  assert.ok(model.optionAlertColumns.candidates.some(column => column.key === 'maximum_loss' && column.hiddenByDefault))
  assert.ok(model.optionAlertColumns.history.some(column => column.key === 'exit_deadline' && column.hiddenByDefault))
  assert.ok(model.optionAlertColumns.daily.some(column => column.key === 'minimum' && column.hiddenByDefault))
})

test('options tables reuse the shared picker with isolated saved preferences', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  for (const view of ['behavior', 'candidates', 'daily', 'history']) {
    const version = view === 'daily' ? 1 : 2
    assert.ok(page.includes(`useColumnPreferences('option-alerts-${view}-v${version}', optionAlertColumns.${view})`))
  }
  assert.match(page, /<ColumnPicker key=\{activeView\}/)
  assert.match(page, /onShowAll=\{preferences.showAll\} onReset=\{preferences.reset\}/)
  assert.equal((page.match(/<ColumnHeaders columns=\{columns\}/g) || []).length, 4)
  assert.equal((page.match(/<ColumnCells columns=\{columns\}/g) || []).length, 5)
  assert.doesNotMatch(page, /<ColumnCells columns=\{detectorObservationColumns\}/)
  assert.match(page, /optionAlertVisibleColumns\(activeView, preferences.hidden\)/)
  assert.equal((page.match(/\.\.\.marketCells\(row.legs\)/g) || []).length, 2)
  assert.match(page, /title=\{column.tip\}/)
  assert.match(page, /money\(leg.entry_model_mark\)/)
})

test('board detail facts preserve original decisions and distinguish absent evidence', () => {
  const data = { candidate: { candidate_kind: 'SINGLE_CONTRACT', structure_risk_class: 'PREMIUM_AT_RISK_DEBIT',
    source_contract_ticker: null, source_contract_id: null, expiration_date: '2026-10-16', candidate_rank: 1,
    primary_metric_name: 'EDGE', primary_metric_value: 0, execution_eligibility: null,
    context_status: 'DEGRADED', trend_state: 'UP', earnings_blackout_state: 'UNKNOWN', fed_blackout_state: 'CLEAR',
    context_reason_codes: ['EARNINGS_COVERAGE_MISSING'], signal_status: 'BLOCKED', signal_id: 'original-signal',
    signal_blocked_reasons: ['QUOTE_LIQUIDITY_UNAVAILABLE'] }, execution_mode: 'READ_ONLY_RESEARCH',
    execution_gates: [{ gate_name: 'QUOTE_LIQUIDITY', verdict: 'UNAVAILABLE' }] }
  const original = structuredClone(data)
  const result = model.optionCandidateBoardFacts(data)
  assert.equal(result.selection.primary_metric_value, 0)
  assert.equal(result.selection.model_rank, 1)
  assert.equal(result.selection.original_execution_eligibility, 'Not eligible')
  assert.equal(result.context.earnings_blackout, 'UNKNOWN')
  assert.equal(result.execution.signal_state, 'Blocked')
  assert.equal(result.execution.quote_liquidity_gate, 'Unavailable')
  assert.deepEqual(data, original)
  data.execution_gates = []
  Object.assign(data.candidate, { primary_metric_value: NaN, context_status: null, trend_state: null,
    earnings_blackout_state: null, fed_blackout_state: null, context_reason_codes: null, signal_status: null,
    signal_blocked_reasons: [] })
  const missing = model.optionCandidateBoardFacts(data)
  assert.equal(missing.selection.primary_metric_value, null)
  assert.equal(missing.context.earnings_blackout, null)
  assert.equal(missing.context.context_reasons, null)
  assert.equal(missing.execution.signal_blocked_reasons, 'None recorded')
  assert.equal(missing.execution.quote_liquidity_gate, 'Not recorded')
})

test('return on risk formatting preserves zero and never fabricates missing ratios', () => {
  for (const value of [null, undefined, '', ' ', NaN, Infinity, {}, false]) assert.equal(model.optionAlertPercent(value), 'Unavailable')
  assert.equal(model.optionAlertPercent(0), '0%')
  assert.equal(model.optionAlertPercent('0.235'), '23.5%')
  assert.equal(model.optionAlertPercent(-0.15), '-15%')
})

test('original contract prices retain units, moneyness and missing market data', () => {
  const leg = { spot: '100', strike: '110', contract_type: 'CALL', model_mark: '2.35', day_volume: 0,
    open_interest: 1000, local_iv: .35, local_delta: .4, quote_bid: null, quote_ask: null }
  const result = model.optionLegMarketValues(leg)
  assert.equal(result.stock, '$100.00')
  assert.equal(result.option_price, '$2.35')
  assert.equal(result.otm, '10%')
  assert.equal(result.iv, '35%')
  assert.equal(result.volume, '0')
  assert.equal(result.open_interest, '1,000')
  assert.equal(result.volume_oi, '0x')
  assert.equal(result.bid_ask, 'Unavailable')
  assert.equal(model.optionLegMarketValues({ ...leg, contract_type: 'PUT' }).otm, '-10%')
  assert.equal(model.optionLegMarketValues({ ...leg, spot: 0 }).otm, 'Unavailable')
  assert.equal(model.optionLegMarketValues({ ...leg, open_interest: 0 }).volume_oi, 'Unavailable')
  assert.ok(Object.values(model.optionLegMarketValues({})).every(value => value === 'Unavailable'))
  assert.equal(model.optionPackagePremium('-235'), '$235.00 debit')
  assert.equal(model.optionPackagePremium('150'), '$150.00 credit')
  assert.equal(model.optionPackagePremium(null), 'Unavailable')
  for (const view of ['behavior', 'candidates']) {
    const defaults = model.optionAlertColumns[view].filter(column => !column.hiddenByDefault).map(column => column.key)
    for (const key of ['contracts', 'stock', 'option_price', 'otm', 'volume', 'open_interest', 'iv']) assert.ok(defaults.includes(key))
  }
})

test('package price uses signed ratio-weighted leg premiums and the actual multiplier', () => {
  const leg = (side, mark, ratio = 1, multiplier = 100) => ({ side, model_mark: mark, ratio, multiplier })
  const debit = model.optionPackagePrice('-473', [leg('BUY', '7'), leg('SELL', '2.27')])
  assert.deepEqual(debit, { amount: -473, price: -4.73, multiplier: 100, reason: null })
  assert.equal(model.optionPackagePremium(debit.price), '$4.73 debit')
  const credit = model.optionPackagePrice('24', [leg('SELL', '.90'), leg('BUY', '.66')])
  assert.equal(credit.price, .24)
  assert.equal(model.optionPackagePremium(credit.price), '$0.24 credit')
  const condor = model.optionPackagePrice(110, [leg('SELL', 1.5), leg('BUY', .8), leg('SELL', 1.2), leg('BUY', .8)])
  assert.equal(condor.price, 1.1)
  assert.equal(model.optionPackagePrice(-400, [leg('BUY', 3, 2), leg('SELL', 2)]).price, -4)
  assert.equal(model.optionPackagePrice(-100, [leg('BUY', 3, 1, 50), leg('SELL', 1, 1, 50)]).price, -2)
  assert.equal(model.optionPackagePrice(0, [leg('BUY', 1), leg('SELL', 1)]).price, 0)
  assert.equal(model.optionPackagePremium(0), '$0.00 net')
  assert.equal(model.optionPackagePrice(-50, [leg('BUY', 2, 1, 50), leg('SELL', .5)]).reason, 'MIXED_LEG_MULTIPLIERS')
})

test('package prices fail closed on missing marks, invalid terms and mismatched retained economics', () => {
  const legs = [{ side: 'BUY', model_mark: 7, ratio: 1, multiplier: 100 },
    { side: 'SELL', model_mark: 2.27, ratio: 1, multiplier: 100 }]
  for (const changes of [{ model_mark: null }, { model_mark: NaN }, { model_mark: Infinity }, { model_mark: 0 },
    { side: 'OTHER' }, { ratio: 0 }, { ratio: 1.5 }, { multiplier: null }, { multiplier: 0 }]) {
    assert.equal(model.optionPackagePrice(-473, [{ ...legs[0], ...changes }, legs[1]]).price, null)
  }
  assert.equal(model.optionPackagePrice(null, legs).reason, 'PACKAGE_PREMIUM_UNAVAILABLE')
  assert.equal(model.optionPackagePrice(-473, []).reason, 'PACKAGE_LEGS_UNAVAILABLE')
  assert.equal(model.optionPackagePrice(473, legs).reason, 'PACKAGE_PREMIUM_LEG_MISMATCH')
  assert.equal(model.optionPackagePrice(-470, legs).price, null)
  assert.equal(model.optionPackagePrice(-473, [legs[0]]).price, null)
})

test('published history uses frozen entry leg prices with the same package basis', () => {
  const legs = [{ side: 'BUY', entry_model_mark: '7', ratio: 1, multiplier: 100 },
    { side: 'SELL', entry_model_mark: '2.27', ratio: 1, multiplier: 100 }]
  const original = structuredClone(legs)
  assert.equal(model.optionPackagePrice('-473', legs).price, -4.73)
  assert.equal(model.optionPackagePrice('-450', legs).reason, 'PACKAGE_PREMIUM_LEG_MISMATCH')
  assert.equal(model.optionPackagePrice('-473', [{ ...legs[0], entry_model_mark: null }, legs[1]]).price, null)
  assert.deepEqual(legs, original)
})

test('Alerts and Published History share package-price display without repricing history', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.equal((page.match(/option_price: <PackagePrice netPremium=\{row.net_premium\} legs=\{row.legs\}/g) || []).length, 2)
  assert.match(page, /entry_marks: <PackagePrice netPremium=\{row.original_economics.net_premium\} legs=\{row.legs\}/)
  assert.match(page, /entry_leg_marks: row.legs.length/)
  assert.doesNotMatch(page, /<PackagePrice netPremium=\{row.entry_limit\}/)
  assert.match(page, /column.key === 'leg_prices' \? 'option_price'/)
  assert.equal(model.optionAlertColumns.history.find(column => column.key === 'entry_marks').label, 'Original package price / share')
  assert.ok(model.optionAlertColumns.history.find(column => column.key === 'entry_leg_marks').hiddenByDefault)
  for (const view of ['behavior', 'candidates']) {
    assert.equal(model.optionAlertColumns[view].find(column => column.key === 'option_price').label, 'Option price')
    assert.ok(model.optionAlertColumns[view].find(column => column.key === 'leg_prices').hiddenByDefault)
  }
})

test('plan columns distinguish debit stops from credit close costs and never invent management', () => {
  const legs = [{ side: 'BUY', model_mark: 4, ratio: 1, multiplier: 100 }, { side: 'SELL', model_mark: 2, ratio: 1, multiplier: 100 }]
  const debit = model.optionPlanTerms(-200, legs, { stop_loss_fraction: .35, take_profit_fraction: .5, maximum_hold_seconds: 172800 }, { version: 'test_v1' })
  assert.equal(debit.stopPrice, 1.3)
  assert.equal(debit.targetPrice, 3)
  assert.equal(debit.risk, 70)
  assert.equal(debit.riskFraction, .35)
  assert.equal(debit.rewardRisk, 100 / 70)
  assert.equal(debit.hold, '48 elapsed hours')
  const creditLegs = legs.map(leg => ({ ...leg, side: leg.side === 'BUY' ? 'SELL' : 'BUY' }))
  const credit = model.optionPlanTerms(200, creditLegs, { stop_loss_multiple: 2, take_profit_fraction: .5, exit_dte: 21 }, { version: 'v1' })
  assert.equal(credit.stopPrice, 4)
  assert.equal(credit.targetPrice, 1)
  assert.equal(credit.risk, 200)
  assert.equal(credit.rewardRisk, .5)
  assert.equal(credit.closeKind, 'BUY_TO_CLOSE_COST')
  assert.equal(credit.hold, 'Exit at 21 DTE')
  const missing = model.optionPlanTerms(-200, legs, { dte_lane: 'NEAR' }, { version: 'v1' })
  assert.equal(missing.stop, null)
  assert.equal(missing.target, null)
  assert.equal(missing.rewardRisk, null)
  assert.equal(missing.hold, 'Unavailable')
  assert.equal(model.optionPlanTerms(-200, legs, { stop_loss_fraction: .35 }).stop, null)
})

test('explicit plan limits retain their entry basis and missing or invalid thresholds stay unavailable', () => {
  const legs = [{ side: 'BUY', entry_model_mark: 2, ratio: 1, multiplier: 100 }]
  const policy = { stop_loss_fraction: .5, take_profit_fraction: .5 }
  const explicit = model.optionPlanTerms(-200, legs, policy, { version: 'v1', source: 'EXPLICIT_ALERT_POLICY', entryLimit: '180', entryKind: 'MAXIMUM_DEBIT' })
  assert.equal(explicit.stopPrice, .9)
  assert.equal(explicit.targetPrice, 2.7)
  assert.equal(explicit.basis, 'PLANNED_ENTRY_LIMIT_NOT_FILL')
  for (const invalid of [0, -1, 1, Infinity, NaN, null]) assert.equal(model.optionPlanTerms(-200, legs, { ...policy, stop_loss_fraction: invalid }, { version: 'v1' }).stop, null)
  assert.equal(model.optionPlanTerms(-200, [], policy, { version: 'v1' }).stop, null)
  const clock = Date.parse('2026-09-18T20:00:00Z')
  assert.equal(model.optionRemainingHold('2026-09-18T21:00:00Z', clock), '1 hours')
  assert.equal(model.optionRemainingHold('2026-09-18T20:00:00Z', clock), 'Deadline elapsed')
  assert.equal(model.optionRemainingHold(null, clock), 'Unavailable')
})

test('history presents marked return bases and recorded plans without publishing or fabricating hits', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /\.\.\.currentMarkCells\(row\)/)
  assert.equal((page.match(/\.\.\.planCells\(/g) || []).length, 3)
  assert.ok((page.match(/detectorThresholdCells\(/g) || []).length >= 3)
  assert.match(page, /mark.status !== 'UNAVAILABLE'/)
  assert.match(page, /row.hit_count == null \? 'Unavailable'/)
  assert.match(page, /Original candidate-mark basis/)
  assert.match(page, /After planned exit deadline/)
  assert.match(page, /repeated plan events are not independent samples/)
  assert.match(page, /Retained marked performance/)
  assert.match(page, /row.original_economics.net_premium/)
  assert.doesNotMatch(page, /publishOption|createPosition|executeTrade|activatePublisher/)
})

test('Latest Run and Day History share selection and retain the chosen session', () => {
  const original = new URLSearchParams('view=day_history&session_date=2026-09-18&underlyer=AAPL&behavior=ALL&offset=50&candidate=one&event=two')
  const current = model.optionAlertTabParams(original, 'behavior')
  assert.equal(current.get('session_date'), '2026-09-18')
  assert.equal(current.get('underlyer'), 'AAPL')
  assert.equal(current.get('behavior'), 'ALL')
  for (const key of ['offset', 'candidate', 'event']) assert.equal(current.get(key), null)
  assert.equal(model.optionAlertTabParams(original, 'day_history').get('session_date'), '2026-09-18')
  assert.equal(model.optionReviewScope('behavior'), 'CURRENT')
  assert.equal(model.optionReviewScope('day_history'), 'HISTORY')
  assert.equal(model.optionReviewScope('daily'), 'HISTORY')
  assert.equal(model.optionReviewScope('history'), 'LATEST')
  assert.equal(original.get('offset'), '50')
  assert.ok(model.optionAlertColumns.day_history.some(column => column.key === 'price_pnl'))
  assert.ok(!model.optionAlertColumns.day_history.some(column => column.key === 'event'))
  assert.ok(model.optionAlertColumns.day_history.some(column => column.key === 'hit_count'))
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /review_scope: optionReviewScope\(view\)/)
  assert.match(page, /session_date: params.get\('session_date'\) \|\| undefined/)
  assert.match(page, /Open Day History/)
  assert.match(page, /Legacy Publication Audit/)
  assert.match(page, /Retained detections, not publication events/)
  assert.match(page, /option-alerts-day-history-v1/)
  assert.match(page, /refetchInterval: daily \? false : 30_000/)
  assert.match(page, /currentMarkCells\(\{ current_mark: row.current_mark/)
  for (const view of ['behavior', 'day_history']) {
    assert.equal(model.optionReviewSelection(model.optionAlertTabParams(new URLSearchParams(), view)), 'ALL')
    assert.equal(model.optionReviewSelection(model.optionAlertTabParams(new URLSearchParams('behavior=ELIGIBLE'), view)), 'ELIGIBLE')
    assert.equal(model.optionReviewSelection(model.optionAlertTabParams(new URLSearchParams('behavior=SHORTLIST'), view)), 'SHORTLIST')
  }
  assert.match(page, /optionReviewSelection\(params\)/)
  assert.match(page, /alertSessions: reviewView \? \{ \.\.\.sessionContext, selected: params.get\('session_date'\)/)
  assert.match(page, /Last completed run retained/)
  assert.match(page, /latest_run_excluded/)
  const shell = readFileSync(new URL('../src/layout/AppShell.tsx', import.meta.url), 'utf8')
  assert.match(shell, /emptyLabel = 'No publications', resetKeys = \[\]/)
  assert.match(shell, /for \(const key of resetKeys\) next.delete\(key\)/)
})

test('Alerts no longer exposes the broad candidate inventory as a navigable tab', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /\['history', 'daily', 'day_history'\]\.includes\(params.get\('view'\)/)
  assert.match(page, /const views: OptionAlertView\[\] = \['behavior', 'day_history', 'daily', 'history'\]/)
  assert.doesNotMatch(page, />Candidates<\/button>/)
})

test('Latest and History use dataset membership with sortable category and strategy columns', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  const detectorPanel = page.slice(page.indexOf('function DetectorAlertsPanel'), page.indexOf('function DetectorEvaluationPanel'))
  assert.match(detectorPanel, /getOptionDetectorAlerts\(request\)/)
  assert.match(detectorPanel, /queryKey: \['option-detector-alerts', request\]/)
  assert.match(detectorPanel, /scope: view === 'behavior' \? 'LATEST' : 'HISTORY'/)
  assert.match(detectorPanel, /session_rollup: view === 'day_history' && !explicitDataset/)
  assert.match(detectorPanel, /enabled: Boolean\(dataset\)/)
  assert.match(detectorPanel, /index\?\.storage_ready === false/)
  assert.match(detectorPanel, /optionDetectorSort\(params\)/)
  assert.match(detectorPanel, /optionPackageStrategyName\(row.strategy_name\)/)
  assert.doesNotMatch(detectorPanel, /getOptionCandidates|optionReviewSelection|Directional stock gates passed|Timely directional shortlist/)
  assert.match(page, /<DetectorAlertsPanel key=\{`detector-v5-\$\{activeView\}`\}/)
  assert.match(page, /aria-sort=/)
  assert.match(page, /title="Category" field="category"/)
  assert.match(page, /title="Package strategy" field="strategy"/)
  assert.match(page, /aria-label="Evaluation underlying"/)
})

test('detector workspace restores source columns and uses current models without mandatory dataset choice', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  const shell = readFileSync(new URL('../src/layout/AppShell.tsx', import.meta.url), 'utf8')
  const panel = page.slice(page.indexOf('function DetectorAlertsPanel'), page.indexOf('function DetectorEvaluationPanel'))
  const daily = page.slice(page.indexOf('function DetectorEvaluationPanel'), page.indexOf('function BehaviorReviewPanel'))
  assert.match(panel, /queryFn: getOptionDetectorDatasets/)
  assert.match(panel, /index\?\.session_datasets\[requestedSession\]/)
  assert.match(panel, /datasetByDate: index\?\.session_datasets/)
  assert.match(page, /historicalView: 'day_history'/)
  assert.match(shell, /next\.set\('evaluation_dataset', dataset\)/)
  assert.match(shell, /next\.delete\('evaluation_dataset'\)/)
  assert.match(panel, /index\?\.default_dataset_id/)
  assert.doesNotMatch(panel, /aria-label="Alert dataset"/)
  assert.match(panel, /aria-label="Alert column preset"/)
  assert.doesNotMatch(panel, /Alert filter preset|value="model:|startsWith\('model:'/)
  assert.match(panel, /presets.length > 0 && <label>Saved views/)
  assert.match(panel, /aria-label="Saved alert view"/)
  assert.match(panel, /useColumnPreferences\('option-detector-alerts-v8', detectorAlertColumns\)/)
  assert.match(panel, /option_price: <span className="oa-trigger-price">/)
  assert.match(panel, /const thresholds = detectorThresholdCells/)
  assert.doesNotMatch(panel, /technical\?\.underlying_(stop|target)/)
  assert.match(panel, /<DetectorAlertSidecar row=\{planDetail\}/)
  assert.match(page, /className="oa-detail oa-alert-sidecar"/)
  for (const heading of ['Alert selection', 'Package prices and performance', 'Contract terms', 'Prices and economics', 'Greeks and volatility', 'Activity', 'Model quality and assumptions', 'Source times']) assert.ok(page.includes(`title="${heading}"`))
  assert.match(page, /Alert and snapshot provenance/)
  assert.match(page, /Option stop \/ share/)
  assert.match(page, /Option target \/ share/)
  assert.match(page, /<small>Option package \/ share<\/small>/)
  assert.match(page, /filter\(\(\[key\]\) => key !== 'technical_exit'\)/)
  assert.doesNotMatch(page.slice(page.indexOf('aria-label="Original alert management"'), page.indexOf('Alert and snapshot provenance')), /Rules values=\{row\.management_policy\}/)
  assert.doesNotMatch(panel, /option-detector-alerts-\$\{view\}/)
  assert.match(panel, /aria-label="Save alert preset"/)
  assert.match(panel, /readDetectorPresets\(localStorage\)/)
  assert.match(panel, /\.\.\.marketCells\(legs\)/)
  assert.match(panel, /source: credit \? 'O3_MODEL_MARK_CREDIT' : 'EXPLICIT_ALERT_POLICY'/)
  assert.match(daily, /Daily model results/)
  assert.match(daily, /All five models/)
  assert.match(page, /Current five-model results/)
  assert.match(page, /O3: 'Defined-Risk Credit'/)
  assert.match(daily, /O3 credit result/)
  assert.match(page, /Selected indicative research alert \/ Original retained model marks/)
  assert.match(daily, /All retained results/)
  assert.doesNotMatch(daily, /aria-label="EOD review scope"/)
  assert.match(daily, /Legacy diagnostics/)
  assert.match(daily, /Prospective outcomes are not connected/)
})

test('options alert window uses a separate read-only schedule with explicit completion warnings', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /queryFn: getOptionAlertSchedule/)
  assert.match(page, /aria-label="Options alert window"/)
  assert.match(page, /elapsed .*without recorded completion/)
  assert.match(page, /schedule\.dataset_id !== dataset/)
  assert.match(page, /<strong title="Expected delayed worker check window/)
  assert.match(page, /queryClient.invalidateQueries\(\{ queryKey: \['option-alert-schedule'\] \}\)/)
  assert.match(page, /aria-label="Detector evaluation failure"/)
  assert.match(page, /latestFailure.reason === 'DETECTOR_SOURCE_TIMEOUT'/)
  assert.match(page, /No completed alert result for this run/)
  assert.match(page, /Evaluation diagnostics unavailable/)
  assert.match(page, /latestAttempt.status === 'RUNNING' \? 'Run in progress'/)
})

test('zero-alert runs show publication clocks and retained exclusions independently of rows', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /aria-label="Latest detector publication"/)
  assert.match(page, /Published \{time\(data.run.published_at\)\}/)
  assert.match(page, /Scheduled run \{time\(data.run.scheduled_cycle\)\}/)
  assert.match(page, /Read as of \{time\(data.as_of\)\}/)
  assert.match(page, /aria-label="Detector run publication history"/)
  assert.match(page, /Object.entries\(run.rejections\)/)
  assert.match(page, /<DetectorRunDetails runs=\{data.completed_runs/)
  assert.match(page, /No qualified new alerts in this completed run/)
})

test('baseline alert views separate models, categories and recorded repeat hits', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  for (const view of ['behavior', 'day_history']) {
    assert.ok(model.optionAlertColumns[view].some(column => column.key === 'hit_count'))
    assert.ok(model.optionAlertColumns[view].some(column => column.key === 'category'))
  }
  assert.match(page, /data\?\.history_basis === 'PERSISTED_BASELINE_ALERT_MEMBERS'/)
  assert.match(page, /Object.entries\(modelNames\)/)
  assert.match(page, /row.hit_count == null \? 'Unavailable'/)
  assert.match(page, /First recorded alerts only \/ Repeat hits are not new samples/)
  assert.doesNotMatch(page, /\.slice\(0, 50\)|setHitCount|publishBaseline/)
})

test('EOD compares all qualified selection records without inventing outcomes', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  const api = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8')
  assert.match(page, /getOptionDetectorEvaluations\(request\)/)
  assert.match(page, /All qualified detector packages/)
  assert.match(page, /NOT_SELECTED">Not selected/)
  assert.match(page, /Detector evaluation storage is not deployed/)
  assert.match(page, /cell.measured \?\? 'Unavailable'/)
  assert.match(page, /queryKey: \['option-detector-evaluations', request\]/)
  assert.match(page, /Legacy stock gates/)
  assert.match(api, /api.get\('\/options\/alerts\/evaluations', \{ params \}\)/)
  assert.doesNotMatch(page, /evaluateOnGet|recordOverflow|createEvaluation/)
})

test('all five detector names are separate from observation-only return fields', () => {
  const page = readFileSync(new URL('../src/pages/OptionsAlertsPage.tsx', import.meta.url), 'utf8')
  assert.match(page, /O2: 'Local IV Surface Distortion'/)
  assert.match(page, /S2: 'Relative Trend Resumption'/)
  assert.match(page, /value="OBSERVATION">Observations/)
  assert.match(page, /cell.detector_id === 'O2' \? 'Not applicable'/)
  assert.match(page, /Surface residual evidence/)
  assert.match(page, /'Nondirectional'/)
})

test('contract expiration is conservative and uses the New York session date', () => {
  assert.equal(model.optionContractExpired('2026-09-24', '2026-09-25T14:00:00Z'), true)
  assert.equal(model.optionContractExpired('2026-09-25', '2026-09-25T21:00:00Z'), false)
  assert.equal(model.optionContractExpired('2026-09-26', '2026-09-25T21:00:00Z'), false)
  assert.equal(model.optionContractExpired(null, '2026-09-25T21:00:00Z'), false)
})