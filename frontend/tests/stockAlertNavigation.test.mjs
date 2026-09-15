import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const source = readFileSync(new URL('../src/pages/stockAlertNavigation.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const { resolveAlertRoute, alertSourceParams, alertTabParams, alertTradeFilters, alertSort } = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
const presentationSource = readFileSync(new URL('../src/pages/stockAlertPresentation.ts', import.meta.url), 'utf8')
const presentation = ts.transpileModule(presentationSource, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const { alertColumnLayout, alertRiskAssessment, unavailableAlertProbability } = await import(`data:text/javascript;base64,${Buffer.from(presentation.outputText).toString('base64')}`)

test('sidebar Latest Run stays shadow while Day History defaults to backtested data', () => {
  const latest = new URLSearchParams()
  assert.deepEqual(resolveAlertRoute(latest), { source: 'SHADOW', view: 'latest' })
  const history = alertTabParams(latest, 'history')
  assert.deepEqual(resolveAlertRoute(history), { source: 'REPLAY', view: 'history' })
  assert.deepEqual(resolveAlertRoute(new URLSearchParams('view=history')), { source: 'REPLAY', view: 'history' })
})

for (const source of ['SHADOW', 'REPLAY', 'LEGACY']) {
  test(`explicit ${source} selection is never silently replaced`, () => {
    const params = new URLSearchParams({ source, view: 'history', session_date: '2026-08-18' })
    assert.equal(resolveAlertRoute(params).source, source)
    const latest = alertTabParams(params, 'latest')
    assert.equal(resolveAlertRoute(latest).source, source)
    assert.equal(latest.get('session_date'), '2026-08-18')
  })
}

test('changing sources preserves Day History without carrying an incompatible date or filters', () => {
  const params = alertSourceParams('REPLAY', 'history')
  assert.equal(params.toString(), 'source=REPLAY&view=history')
  assert.equal(alertSourceParams('LEGACY', 'latest').toString(), 'source=LEGACY')
})

test('switching a default-source tab clears source-specific dates and run pagination', () => {
  const history = new URLSearchParams('view=history&session_date=2026-08-18&run=old&offset=100')
  const latest = alertTabParams(history, 'latest')
  assert.deepEqual(resolveAlertRoute(latest), { source: 'SHADOW', view: 'latest' })
  assert.equal(latest.has('session_date'), false)
  assert.equal(latest.has('run'), false)
  assert.equal(latest.has('offset'), false)
  assert.equal(history.get('session_date'), '2026-08-18')
})

test('old watch links cannot silently hide trade alerts after lane control is removed', () => {
  const params = new URLSearchParams('lane=WATCH&model=discovery&status=WATCH&direction=0')
  assert.deepEqual(alertTradeFilters(params, 'REPLAY'), {
    lane: 'TRADE', direction: undefined, model: undefined, status: undefined,
  })
  assert.equal(params.get('lane'), 'WATCH')
})

test('visible trade filters are preserved without mixing legacy or watch models', () => {
  assert.deepEqual(alertTradeFilters(new URLSearchParams('model=failure&status=CLOSED&direction=-1'), 'REPLAY'), {
    lane: 'TRADE', direction: '-1', model: 'failure', status: 'CLOSED',
  })
  assert.deepEqual(alertTradeFilters(new URLSearchParams('model=legacy_daily&status=OPEN_PAPER&direction=1'), 'LEGACY'), {
    lane: 'TRADE', direction: '1', model: 'legacy_daily', status: 'OPEN_PAPER',
  })
  assert.equal(alertTradeFilters(new URLSearchParams('model=legacy_daily&status=OPEN_PAPER'), 'REPLAY').model, undefined)
  assert.equal(alertTradeFilters(new URLSearchParams('status=OPEN_PAPER'), 'REPLAY').status, undefined)
})

test('an enrolled forward view keeps Day History in the genuine shadow source', () => {
  const params = new URLSearchParams('session_date=2026-09-14')
  const history = alertTabParams(params, 'history', true)
  assert.deepEqual(resolveAlertRoute(history), { source: 'SHADOW', view: 'history' })
  assert.equal(history.get('session_date'), '2026-09-14')
  const replay = alertTabParams(new URLSearchParams('source=REPLAY&session_date=2026-08-18'), 'history', true)
  assert.equal(resolveAlertRoute(replay).source, 'REPLAY')
  assert.equal(replay.get('session_date'), '2026-08-18')
})

test('Day History defaults to newest trigger and tab switches clear incompatible sorting', () => {
  const params = new URLSearchParams('source=SHADOW&sort=ticker&ascending=1&search=ABC')
  assert.deepEqual(alertSort(new URLSearchParams(), 'history'), { sort: 'triggered_at', descending: true })
  assert.deepEqual(alertSort(new URLSearchParams(), 'latest'), { sort: 'published_at', descending: true })
  assert.deepEqual(alertSort(params, 'history'), { sort: 'ticker', descending: false })
  const history = alertTabParams(params, 'history')
  assert.equal(history.get('search'), 'ABC')
  assert.deepEqual(alertSort(history, 'history'), { sort: 'triggered_at', descending: true })
  history.set('sort', 'price_return')
  assert.equal(alertTabParams(history, 'history').get('sort'), 'price_return')
  assert.deepEqual(alertSort(alertTabParams(history, 'latest'), 'latest'), { sort: 'published_at', descending: true })
})

test('Latest Run required plan columns survive old hidden preferences and every preset shape', () => {
  const required = ['stop', 'target', 'reward_risk', 'success_probability', 'risk_assessment']
  const columns = [{ key: 'ticker', locked: true }, ...required.map(key => ({ key })), { key: 'rsi' }, { key: 'entry_price', history: true }]
  for (const hidden of [new Set(columns.map(column => column.key)), new Set(required), new Set(['rsi']), new Set()]) {
    const before = [...hidden]
    const result = alertColumnLayout(columns, hidden, 'latest')
    assert.ok(required.every(key => result.visible.some(column => column.key === key && column.locked)))
    assert.ok(!result.visible.some(column => column.key === 'entry_price'))
    assert.deepEqual([...hidden], before)
  }
  assert.ok(columns.filter(column => required.includes(column.key)).every(column => !column.locked))
})

test('Day History keeps customizable plan columns instead of inheriting Latest Run locks', () => {
  const columns = [{ key: 'ticker', locked: true }, { key: 'stop' }, { key: 'success_probability' }, { key: 'entry_price', history: true }]
  const history = alertColumnLayout(columns, new Set(['ticker', 'stop', 'success_probability']), 'history')
  assert.deepEqual(history.visible.map(column => column.key), ['ticker', 'entry_price'])
  assert.equal(history.columns.find(column => column.key === 'stop').locked, false)
})


test('Day History always pairs the current price and price return without exposing them in Latest Run', () => {
  const columns = [{ key: 'ticker', locked: true }, { key: 'triggered_at', history: true }, { key: 'latest_price', history: true },
    { key: 'price_return', history: true }, { key: 'paper_return', history: true }]
  const hidden = new Set(['triggered_at', 'latest_price', 'price_return', 'paper_return'])
  const history = alertColumnLayout(columns, hidden, 'history')
  assert.deepEqual(history.visible.map(column => column.key), ['ticker', 'triggered_at', 'latest_price', 'price_return'])
  assert.ok(history.visible.every(column => column.locked))
  assert.deepEqual(alertColumnLayout(columns, new Set(), 'latest').visible.map(column => column.key), ['ticker'])
  assert.equal(hidden.size, 4)
})

test('risk assessment quantifies both directions without inventing low risk or a probability', () => {
  const row = { direction: 1, trigger_price: 100, stop: 98, target: 104, status: 'PENDING', reason: null, warnings: ['Forward paper only', 'Stop gaps can exceed planned risk'] }
  const result = alertRiskAssessment(row)
  assert.equal(result.stopDistance, .02)
  assert.equal(result.label, 'Unrated')
  assert.deepEqual(result.cautions, ['Stop gaps can exceed planned risk'])
  assert.equal(alertRiskAssessment({ ...row, direction: -1, stop: 102, target: 96 }).stopDistance, .02)
  assert.equal(alertRiskAssessment({ ...row, status: 'UNRESOLVED', reason: 'IDENTITY_UNAVAILABLE' }).label, 'Data issue')
  assert.equal(alertRiskAssessment({ ...row, target: 1000 }).label, 'Unrated')
  assert.equal(unavailableAlertProbability.label, 'Unavailable')
  assert.match(unavailableAlertProbability.reason, /not probabilities/)
  for (const change of [{ stop: null }, { stop: 102 }, { target: 99 }, { trigger_price: 0 }, { target: Infinity }, { direction: 0 }]) {
    const invalid = alertRiskAssessment({ ...row, ...change })
    assert.equal(invalid.label, 'Unavailable')
    assert.equal(invalid.stopDistance, null)
  }
})