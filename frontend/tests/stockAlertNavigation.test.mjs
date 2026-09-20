import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const source = readFileSync(new URL('../src/pages/stockAlertNavigation.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const { resolveAlertRoute, alertSourceParams, alertTabParams, alertTradeFilters, alertSort } = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
const presentationSource = readFileSync(new URL('../src/pages/stockAlertPresentation.ts', import.meta.url), 'utf8')
const presentation = ts.transpileModule(presentationSource, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const { alertColumnLayout, alertPlanTiming, alertPublicationFacts, alertRiskAssessment, earliestAlertWindow, unavailableAlertProbability } = await import(`data:text/javascript;base64,${Buffer.from(presentation.outputText).toString('base64')}`)

test('combined alert status keeps only the earliest actionable publication window', () => {
  const streams = [
    { publication_window_start: '2026-09-21T15:15:00Z', publication_deadline: '2026-09-21T15:29:00Z' },
    { publication_window_start: '2026-09-21T14:15:00Z', publication_deadline: '2026-09-21T14:29:00Z' },
    { publication_window_start: '2026-09-21T13:15:00Z' },
  ]
  assert.deepEqual(earliestAlertWindow(streams), streams[1])
  assert.equal(earliestAlertWindow(undefined), null)
})

test('alert publication context is limited to six ticker and alert facts without provenance', () => {
  const factor = value => ({ status: 'READY', reason_codes: [], source_revision_ids: ['hidden'], value })
  const row = { hits: 2, first_seen: '2026-09-17T15:00:00Z', last_seen: '2026-09-18T15:00:00Z',
    hit_models: ['failure'], hit_intervals: ['1d'], context: { status: 'AVAILABLE', reason: null, factors: {
    stock_daily: factor({ direction: 'UP', return20: .1234 }),
    stock_relative_rotation: factor({ state: 'LEADING', relative5: .012, relative20: .034, relative_change5: .006 }),
    sector: factor({ direction: 'DOWN', stock_minus_sector20: .0456, sector_minus_spy20: -.021 }),
    stock_divergence: factor({ labels: ['STOCK_SPECIFIC_STRENGTH'] }),
    earnings: factor({ events: [{ type: 'EARNINGS', scheduled_time: '2026-09-22T20:00:00Z', confidence: 'HIGH', relative_timing: 'DURING_HOLD' }] }),
    fomc: factor({ events: [{ type: 'FOMC', scheduled_time: '2026-09-23T18:00:00Z', confidence: 'HIGH' }] }),
  } } }
  const facts = alertPublicationFacts(row)
  assert.deepEqual(facts.map(item => item.label), ['Ticker trend', 'Relative rotation', 'Sector influence', 'Divergence', 'Holding-window event', 'Alert persistence'])
  assert.equal(facts[0].value, 'up / 20-session return +12.34%')
  assert.match(facts[1].value, /^leading .* 5-session \+1.2 pp .* change \+0.6 pp$/)
  assert.match(facts[2].value, /stock vs sector \+4.6 pp \/ sector vs SPY -2.1 pp/)
  assert.equal(facts[3].value, 'stock specific strength')
  assert.match(facts[4].value, /^earnings .* during hold$/)
  assert.match(facts[5].value, /^2 retained windows \/ failure, 1d/)
  assert.equal(JSON.stringify(facts).includes('hidden'), false)
  assert.deepEqual(alertPublicationFacts({ hits: null, first_seen: null, last_seen: null, hit_models: [], hit_intervals: [], context: { status: 'UNAVAILABLE', reason: 'MISSING', factors: {} } }), [])
  row.context.factors.earnings = factor({ coverage_complete: true, events: [] })
  row.context.factors.fomc = factor({ coverage_complete: true, events: [] })
  assert.equal(alertPublicationFacts(row).find(item => item.label === 'Holding-window event').value, 'No known earnings or FOMC event during hold')
})

test('swing entry timeframe cannot be mistaken for an intraday holding cap', () => {
  const row = { interval: '30m', confirmation_interval: '30m', lane: 'TRADE', trade_style: 'SWING', holding_sessions: 21, hold: '21 sessions' }
  assert.deepEqual(alertPlanTiming(row), { style: 'Swing', interval: 'Swing / 1d setup / 30m confirmation', hold: '21 trading sessions' })
  for (const holding_sessions of [5, 10]) assert.equal(alertPlanTiming({ ...row, holding_sessions }).hold, `${holding_sessions} trading sessions`)
  assert.equal(alertPlanTiming({ ...row, holding_sessions: undefined, hold: '' }).hold, 'Unavailable')
})

test('existing intraday daily and watch holding labels are preserved', () => {
  const intraday = { interval: '1h', lane: 'TRADE', hold: '120 min / close' }
  assert.deepEqual(alertPlanTiming(intraday), { style: 'Intraday', interval: 'Intraday / 1h', hold: '120 min / close' })
  assert.deepEqual(alertPlanTiming({ ...intraday, interval: '1d', hold: '10 sessions' }), { style: 'Swing', interval: 'Swing / 1d', hold: '10 sessions' })
  assert.equal(alertPlanTiming({ ...intraday, lane: 'WATCH' }).hold, 'Watch only')
})

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
  assert.deepEqual(result.cautions, [])
  assert.equal(alertRiskAssessment({ ...row, direction: -1, stop: 102, target: 96 }).stopDistance, .02)
  assert.equal(alertRiskAssessment({ ...row, status: 'UNRESOLVED', reason: 'IDENTITY_UNAVAILABLE' }).label, 'Data issue')
  const context = 'Directional context: countertrend SHORT against bullish trigger evidence (12-minus-1 momentum +235.00%; RS63 percentile 94.0); trigger metrics unavailable: EMA50 slope'
  const consolidated = alertRiskAssessment({ ...row, warnings: ['Stop gaps can exceed planned risk', 'Action coverage not certified',
    'Latest stored price may be stale; inspect its timestamp', 'Short borrow unverified', context] })
  assert.equal(consolidated.label, 'Countertrend')
  assert.deepEqual(consolidated.cautions, [context])
  assert.equal(consolidated.reason, 'Original bracket: 2.00% to stop and 4.00% to target from trigger; not a maximum-loss estimate.')
  assert.deepEqual(alertRiskAssessment({ ...row, warnings: ['Current price stale: latest completed 5m bar ended 2026-09-18T20:00:00Z'] }).cautions,
    ['Current price stale: latest completed 5m bar ended 2026-09-18T20:00:00Z'])
  assert.equal(alertRiskAssessment({ ...row, warnings: ['Directional context: trigger metrics unavailable: EMA50 slope'] }).label, 'Data limited')
  assert.equal(alertRiskAssessment({ ...row, status: 'UNRESOLVED', warnings: [context] }).label, 'Data issue')
  assert.equal(alertRiskAssessment({ ...row, target: 1000 }).label, 'Unrated')
  assert.equal(unavailableAlertProbability.label, 'Unavailable')
  assert.match(unavailableAlertProbability.reason, /not probabilities/)
  for (const change of [{ stop: null }, { stop: 102 }, { target: 99 }, { trigger_price: 0 }, { target: Infinity }, { direction: 0 }]) {
    const invalid = alertRiskAssessment({ ...row, ...change })
    assert.equal(invalid.label, 'Unavailable')
    assert.equal(invalid.stopDistance, null)
  }
})

test('open positions use forward results without a historical date or stale run filter', () => {
  const current = new URLSearchParams('source=REPLAY&view=history&session_date=2026-08-18&run=old&status=CLOSED&trade_type=SWING&offset=100')
  const open = alertTabParams(current, 'open')
  assert.deepEqual(resolveAlertRoute(open), { source: 'SHADOW', view: 'open' })
  assert.equal(open.get('trade_type'), 'SWING')
  for (const key of ['session_date', 'run', 'offset', 'status']) assert.equal(open.has(key), false)
  assert.deepEqual(resolveAlertRoute(new URLSearchParams('source=REPLAY&view=open')), { source: 'SHADOW', view: 'open' })
  assert.equal(alertTabParams(open, 'latest', true).get('source'), 'SHADOW')
})

test('EOD review is always retained shadow evidence and keeps its selected session', () => {
  const current = new URLSearchParams('source=REPLAY&view=history&session_date=2026-09-18&status=CLOSED&review_selection=NOT_SELECTED')
  const review = alertTabParams(current, 'eod')
  assert.deepEqual(resolveAlertRoute(review), { source: 'SHADOW', view: 'eod' })
  assert.equal(review.get('session_date'), null)
  assert.equal(review.has('status'), false)
  review.set('session_date', '2026-09-18')
  assert.equal(alertTabParams(review, 'eod').get('session_date'), '2026-09-18')
  assert.equal(alertTabParams(review, 'latest', true).has('review_selection'), false)
})