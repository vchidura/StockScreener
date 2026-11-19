// G106 D5 (Nov 18): F0DTE, the 0DTE intraday lane, on the Options Alerts page. An F0DTE alert row (as the window's
// projection writes it to rt.alert_current) shows its model name, the 0DTE intraday category, a long call / long put
// package, its stop / target from its management policy and its exit deadline. f42 (with API r138): the Model selects
// offer F0DTE and send ?detector=F0DTE (the alert list filters on the row's detector id; the evaluation list offers it
// beside the O1 lane its evaluations ride).
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8')
const compiled = ts.transpileModule(read('../src/pages/optionsAlertPresentation.ts'),
  { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const model = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
const page = read('../src/pages/OptionsAlertsPage.tsx')
const api = read('../src/services/api.ts')

// The row the window writes for an F0DTE alert (advanced_options_projection._alert_run_row): SPY 681 call bought at
// the 1.04 ask at the 16:00 UTC window, exit deadline 16:45 UTC.
const managementPolicy = {
  policy_version: 'f0dte_intraday_management_v1', strategy_name: 'ZERO_DTE_INTRADAY', take_profit_fraction: '0.5',
  stop_loss_fraction: '0.35', maximum_hold_seconds: 2700, exit_before_close_minutes: 15, entry_basis: 'ASK_AT_DETECTION',
  mark_basis: 'NBBO_MIDPOINT',
}
const f0dteRow = (type, ask) => ({
  detector_id: 'F0DTE', origin: 'OPTIONS_FIRST', underlyer: 'SPY', direction: type === 'CALL' ? 1 : -1,
  category: 'ZERO_DTE_INTRADAY', strategy_name: type === 'CALL' ? 'LONG_CALL' : 'LONG_PUT',
  triggered_at: '2026-11-18T16:00:00+00:00', exit_deadline: '2026-11-18T16:45:00+00:00',
  entry_deadline: '2026-11-18T16:45:00+00:00', entry_limit: (Number(ask) * 100).toFixed(2),
  management_policy: managementPolicy,
  original_package: {
    status: 'AVAILABLE', structure_type: type === 'CALL' ? 'LONG_CALL' : 'LONG_PUT', calendar_dte: 0,
    expiration_date: '2026-11-18', net_premium: (-Number(ask) * 100).toFixed(2), capital_at_risk: (Number(ask) * 100).toFixed(2),
    legs: [{ leg_index: 0, side: 'BUY', ratio: 1, multiplier: 100, contract_type: type, model_mark: ask,
      contract_ticker: `O:SPY261118${type[0]}00681000`, strike: '681' }],
  },
})
const plan = row => model.optionPlanTerms(row.original_package.net_premium, row.original_package.legs, row.management_policy, {
  version: String(row.management_policy.policy_version), source: 'EXPLICIT_ALERT_POLICY', entryLimit: row.entry_limit,
  entryKind: 'MAXIMUM_DEBIT', exitDeadline: row.exit_deadline,
})

test('an F0DTE row has its own model name, the 0DTE intraday category and a long call / long put package', () => {
  assert.match(page, /F0DTE: '0DTE Intraday Flow'/)
  assert.match(api, /export type OptionDetectorId = 'O1' \| 'O2' \| 'O3' \| 'OS1' \| 'OS2' \| 'F0DTE'/)
  assert.equal(model.optionAlertCategoryName('ZERO_DTE_INTRADAY'), '0DTE intraday')
  assert.equal(model.optionAlertCategoryName('O1'), 'O1')
  assert.equal(model.optionAlertCategoryName(null), 'Unavailable')
  assert.equal(model.optionPackageStrategyName(f0dteRow('CALL', '1.04').strategy_name), 'Long call')
  assert.equal(model.optionPackageStrategyName(f0dteRow('PUT', '0.94').strategy_name), 'Long put')
  assert.equal(model.optionAlertLabel(f0dteRow('PUT', '0.94').original_package.structure_type), 'Long put')
  // The table, the alert sidecar and the evaluation table show the category through the same name.
  assert.equal((page.match(/optionAlertCategoryName\(row\.category\)/g) || []).length, 3)
})

test('an F0DTE row stop / target cells come from its management policy at the ask it was alerted at', () => {
  const call = plan(f0dteRow('CALL', '1.04'))
  assert.ok(Math.abs(call.stopPrice - 0.676) < 1e-9 && Math.abs(call.targetPrice - 1.56) < 1e-9)
  assert.equal(call.basis, 'PLANNED_ENTRY_LIMIT_NOT_FILL')
  assert.equal(call.closeKind, 'SELL_TO_CLOSE_VALUE')
  assert.ok(Math.abs(call.riskFraction - 0.35) < 1e-9 && Math.abs(call.rewardRisk - 0.5 / 0.35) < 1e-9)
  assert.deepEqual(call.reasons, [])
  const put = plan(f0dteRow('PUT', '0.94'))
  assert.ok(Math.abs(put.stopPrice - 0.611) < 1e-9 && Math.abs(put.targetPrice - 1.41) < 1e-9)
})

test('an F0DTE row hold names its 45 minutes and its exit deadline', () => {
  const call = plan(f0dteRow('CALL', '1.04'))
  assert.match(call.hold, /^0\.75 elapsed hours \/ By .+ ET$/)
  assert.ok(call.hold.includes(model.optionAlertTime('2026-11-18T16:45:00+00:00')))
  assert.equal(model.optionAlertTime('2026-11-18T16:45:00+00:00'), model.optionAlertTime(f0dteRow('CALL', '1.04').exit_deadline))
})

test('the triggered-at tooltip says an F0DTE trigger is the end of its print window', () => {
  assert.match(model.optionAlertTriggerBasis('F0DTE'), /^End of the 5-minute print window/)
  assert.equal(model.optionAlertTriggerBasis('O1'), 'Original option source time')
  assert.equal(model.optionAlertTriggerBasis('O3'), 'Original option source time')
  assert.equal(model.optionAlertTriggerBasis('OS1'), 'Original stock trigger-bar time')
  assert.match(page, /title=\{optionAlertTriggerBasis\(row\.detector_id\)\}/)
})

test('f42: the Model selects offer F0DTE and send it to the API (accepted from API r138)', () => {
  assert.match(page, /const apiDetectorFilters = \['O1', 'O2', 'O3', 'OS1', 'OS2', 'F0DTE'\] as const/)
  assert.match(page, /apiDetectorFilters\.map\(id => <option key=\{id\} value=\{id\}>\{detectorNames\[id\]\}/)
  // Both panels parse the URL's model through the one list: the alert list and the evaluation list send F0DTE.
  assert.match(page, /const detector = apiDetectorFilter\(rawDetector\)/)
  assert.match(page, /detector: apiDetectorFilter\(detector\),/)
  assert.doesNotMatch(page, /rawDetector === 'O1'|detector === 'O1' \|\|/)
  // The evaluation list offers F0DTE beside the O1 lane its evaluations ride, and the full list without lanes.
  assert.match(page, /data\.models\.some\(model => model\.detector_id === 'O1'\) \? \['F0DTE' as const\] : \[\]/)
  assert.match(page, /: apiDetectorFilters\r?\n/)
  assert.doesNotMatch(page, /Object\.keys\(detectorNames\)/)
  // A saved view with the F0DTE model is valid; an unknown model is still refused.
  const params = new URLSearchParams({ evaluation_detector: 'F0DTE' })
  assert.deepEqual(model.detectorViewPreset('0DTE', params, []).query, { evaluation_detector: 'F0DTE' })
  assert.throws(() => model.detectorViewPreset('bad', new URLSearchParams({ evaluation_detector: 'F0' }), []), /Invalid detector model/)
})

test('f42: the parse helper keeps only the models the API accepts', () => {
  const source = page.match(/const apiDetectorFilters = (\[[^\]]+\]) as const/)[1]
  const filters = JSON.parse(source.replaceAll("'", '"'))
  const parse = value => filters.find(id => id === value)
  assert.deepEqual(filters, ['O1', 'O2', 'O3', 'OS1', 'OS2', 'F0DTE'])
  assert.equal(parse('F0DTE'), 'F0DTE')
  assert.equal(parse('OS2'), 'OS2')
  assert.equal(parse('f0dte'), undefined)
  assert.equal(parse(null), undefined)
  assert.equal(parse('S1'), undefined)
})
