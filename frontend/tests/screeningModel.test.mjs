import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const source = readFileSync(new URL('../src/pages/screeningModel.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const model = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
const catalog = { fields: { price: { type: 'number', unit: 'USD', label: 'Close' }, change: { type: 'number', unit: 'fraction', label: 'Change' }, instrument_type: { type: 'category', options: ['CS', 'ETF', 'ETV'] } }, patterns: { doji: { direction: 0 } } }

test('draft zero, empty, percent units and inclusive ranges remain distinct', () => {
  const draft = model.emptyDraft()
  draft.fields.change = { min: '0', max: '5', values: [] }
  const result = model.compileDraft(draft, catalog)
  assert.deepEqual(result.errors, {})
  assert.deepEqual(result.predicate.filters, [{ field: 'change', min: 0, max: .05 }])
  assert.deepEqual(model.toDraft(result.predicate, catalog), draft)
  draft.fields.change.min = ''
  assert.equal(model.compileDraft(draft, catalog).predicate.filters[0].min, undefined)
})

test('invalid ranges and nonfinite numbers fail locally without mutating applied rules', () => {
  const applied = model.emptyPredicate()
  for (const [min, max] of [['10', '5'], ['NaN', ''], ['Infinity', '']]) {
    const draft = model.emptyDraft()
    draft.fields.price = { min, max, values: [] }
    assert.ok(model.compileDraft(draft, catalog).errors.price)
    assert.deepEqual(applied, model.emptyPredicate())
  }
})

test('pattern direction and interval come from catalog, not a bullish default', () => {
  const draft = { ...model.emptyDraft(), patterns: ['doji'], pattern_mode: 'NONE' }
  assert.deepEqual(model.compileDraft(draft, catalog).predicate.patterns, [{ id: 'doji', direction: 0, interval: '1d', state: 'OCCURRENCE' }])
})

test('rule identity ignores field ordering and absent optional serialization', () => {
  const first = { ...model.emptyPredicate(), filters: [{ field: 'price', min: 0 }, { field: 'change', max: .1 }] }
  const second = { ...first, filters: [...first.filters].reverse().map(filter => ({ min: null, max: null, values: [], ...filter })) }
  assert.equal(model.ruleIdentity(first), model.ruleIdentity(second))
  assert.equal(model.ruleIdentity(first), model.ruleIdentity(Object.fromEntries(Object.entries(first).reverse())))
})

test('removing an applied chip preserves unrelated unsaved field edits', () => {
  const draft = { ...model.emptyDraft(), fields: { price: { min: '20', max: '', values: [] }, change: { min: '5', max: '', values: [] } } }
  const removed = model.removeDraftFilter(draft, 'price')
  assert.deepEqual(removed.fields, { change: draft.fields.change })
  assert.equal(draft.fields.price.min, '20')
})

test('filter selection visibility recognizes categories and zero bounds without marking empty fields', () => {
  for (const input of [undefined, { min: '', max: '', values: [] }, { min: ' ', max: ' ', values: [] }]) {
    assert.equal(model.hasDraftFilter(input), false)
  }
  for (const input of [{ min: '0', max: '', values: [] }, { min: '', max: '0', values: [] },
    { min: '', max: '', values: ['PULLBACK'] }, { min: 'invalid draft', max: '', values: [] }]) {
    assert.equal(model.hasDraftFilter(input), true)
  }
})

test('field explanations distinguish momentum returns, universe ranks and moving-average distances', () => {
  const momentum = model.screeningFieldHelp('momentum_12_1')
  assert.match(momentum.formula, /21 trading sessions ago.*252 trading sessions ago/)
  assert.match(momentum.meaning, /most recent month is skipped/)
  assert.match(momentum.note, /253 consecutive/)
  const rank = model.screeningFieldHelp('momentum_percentile')
  assert.match(rank.meaning, /before your filters or pagination/)
  assert.match(rank.example, /not a 90% price gain or chance of success/)
  assert.match(rank.note, /stable security ID/)
  for (const [field, average] of [['vs_ema20', 'EMA20'], ['vs_ema50', 'EMA50'], ['vs_sma200', 'SMA200']]) {
    const help = model.screeningFieldHelp(field)
    assert.match(help.formula, new RegExp(`Latest daily close / ${average} - 1`))
    assert.match(help.note, /does not change/)
    assert.match(help.example, /\+10%.*-5%/)
  }
  assert.match(model.screeningFieldHelp('vs_ema20', 'hourly').note, /not 20 days/)
  assert.match(model.screeningFieldHelp('ema20_change_3', 'hourly').note, /23-bar/)
  assert.match(model.screeningFieldHelp('state', 'gap').note, /reversible/)
  assert.equal(model.screeningFieldHelp('unknown'), undefined)
})

test('every current field and context has nonempty help and unknown patterns fail closed', () => {
  const scopes = {
    daily: ['ticker', 'instrument_type', 'price', 'volume', 'change', 'dollar_volume_20', 'relative_volume_20', 'vs_ema20', 'vs_ema50', 'vs_sma200', 'momentum_12_1', 'momentum_6_1', 'momentum_3_1', 'momentum_percentile', 'realized_volatility_21', 'vs_prior_high20', 'discovery_state', 'discovery_trend', 'patterns', 'gaps', 'hourly'],
    hourly: ['close', 'change', 'vs_ema20', 'ema20_change_3'],
    gap: ['direction', 'state', 'formation_age', 'fill_fraction', 'price_location', 'distance_fraction'],
    pattern: ['bullish_engulfing', 'bearish_engulfing', 'shooting_star', 'hammer', 'doji'],
  }
  for (const [scope, fields] of Object.entries(scopes)) for (const field of fields) {
    const help = model.screeningFieldHelp(field, scope)
    assert.ok(help.title && help.purpose && help.meaning && help.note, `${scope}.${field}`)
    assert.ok(help.purpose.length < 200, `${scope}.${field} purpose should stay concise`)
  }
  assert.equal(model.screeningFieldHelp('invented', 'pattern'), undefined)
})

test('field help explains distinct tracking value rather than only defining a calculation', () => {
  assert.match(model.screeningFieldHelp('momentum_12_1').purpose, /medium-term.*not entry timing/)
  assert.match(model.screeningFieldHelp('momentum_percentile').purpose, /leaders or laggards/)
  assert.match(model.screeningFieldHelp('vs_ema20').purpose, /short-term pullbacks/)
  assert.match(model.screeningFieldHelp('vs_ema50').purpose, /medium-term baseline/)
  assert.match(model.screeningFieldHelp('vs_sma200').purpose, /longer-term price baseline/)
  assert.match(model.screeningFieldHelp('relative_volume_20').purpose, /unusually active or quiet/)
  assert.match(model.screeningFieldHelp('realized_volatility_21').purpose, /not automatically better/)
  assert.match(model.screeningFieldHelp('ema20_change_3', 'hourly').purpose, /average itself has risen or fallen/)
})

test('short momentum help states tracking value, exact horizons and skipped month', () => {
  for (const [field, lookback] of [['momentum_6_1', 126], ['momentum_3_1', 63]]) {
    const help = model.screeningFieldHelp(field)
    assert.match(help.formula, new RegExp(`21 trading sessions ago.*${lookback} trading sessions ago`))
    assert.match(help.note, new RegExp(`${lookback + 1} consecutive valid daily closes`))
    assert.match(help.meaning, /excluding the latest 21 trading sessions/)
    assert.equal(model.screeningValueClass(field, .1), 'sw-value-positive')
    assert.equal(model.screeningValueClass(field, -.1), 'sw-value-negative')
    assert.equal(model.screeningValueClass(field, null), '')
    assert.equal(model.screeningValueClass(field, 0), '')
  }
  assert.match(model.screeningFieldHelp('momentum_6_1').purpose, /half-year window/)
  assert.match(model.screeningFieldHelp('momentum_3_1').purpose, /temporary price moves/)
})

test('hourly timing distinguishes refreshed context from frozen historical snapshots', () => {
  const daily = '2026-09-11T20:00:00+00:00'
  const source = { market_time: '2026-09-14T14:30:00+00:00', slot_minutes: 60, capture: 'LATEST_COMPLETED_HOURLY_WITH_FROZEN_DAILY' }
  const label = model.hourlyContextTiming(daily, source)
  assert.match(label, /Daily: Sep 11, 4:00 PM EDT/)
  assert.match(label, /1h: Sep 14, 10:30 AM EDT/)
  assert.match(label, /60-minute slot \/ Refreshed completed hour/)
  assert.match(model.hourlyContextTiming(daily, { ...source, capture: undefined }), /Frozen at daily cutoff/)
  const csv = model.resultCsv({ generation: { generation: 'pair', daily_generation: 'anchor', market_time: daily, source_cutoff: 'daily-cutoff', hourly_source: source }, rows: [{ security_id: 'a', ticker: 'A', session: '2026-09-11', values: {} }] }, ['ticker'])
  assert.ok(csv.includes('"daily_generation","daily_market_time","daily_source_cutoff","hourly_capture"'))
  assert.ok(csv.includes('"anchor","2026-09-11T20:00:00+00:00","daily-cutoff","LATEST_COMPLETED_HOURLY_WITH_FROZEN_DAILY"'))
})

test('momentum summaries explain minimum percent thresholds without needing calculation details', () => {
  for (const [field, horizon] of [['momentum_12_1', '12'], ['momentum_6_1', 'six'], ['momentum_3_1', 'three']]) {
    const summary = model.screeningFieldHelp(field).example
    assert.match(summary, new RegExp(`From about ${horizon} months ago to one month ago`))
    assert.match(summary, /\$100 to \$105 is \+5%/)
    assert.match(summary, /The latest month is excluded/)
    assert.match(summary, /Minimum 5 means at least 5%\./)
    assert.doesNotMatch(summary, /minimum 0\.05/i)
    assert.ok(summary.length < 250)
    const percentageCatalog = { ...catalog, fields: { [field]: { type: 'number', unit: 'fraction', options: [] } } }
    for (const [input, fraction] of [['5', .05], ['0.05', .0005]]) {
      const draft = { ...model.emptyDraft(), fields: { [field]: { min: input, max: '', values: [] } } }
      const result = model.compileDraft(draft, percentageCatalog)
      assert.deepEqual(result.errors, {})
      assert.equal(result.predicate.filters[0].min, fraction)
      assert.equal(model.formatValue(fraction, { unit: 'fraction' }), `${input}%`)
    }
  }
})

test('optional momentum horizons round-trip percent bounds, saved definitions, sort and columns', async () => {
  const fields = Object.fromEntries(['momentum_6_1', 'momentum_3_1'].map(field => [field, { type: 'number', unit: 'fraction', options: [], label: field }]))
  const extended = { ...catalog, fields: { ...catalog.fields, volume: { type: 'number', unit: 'shares', options: [] }, ...fields } }
  const draft = { ...model.emptyDraft(), fields: { momentum_6_1: { min: '0', max: '25', values: [] }, momentum_3_1: { min: '-5', max: '', values: [] } } }
  const compiled = model.compileDraft(draft, extended)
  assert.deepEqual(compiled.errors, {})
  assert.deepEqual(compiled.predicate.filters, [{ field: 'momentum_6_1', min: 0, max: .25 }, { field: 'momentum_3_1', min: -.05, max: undefined }])
  assert.deepEqual(model.toDraft(compiled.predicate, extended), draft)
  const saved = await model.makeScreen('Momentum horizons', compiled.predicate, [...model.DEFAULT_COLUMNS, ...Object.keys(fields)], { field: 'momentum_6_1', descending: true })
  assert.deepEqual(await model.importDefinitions(model.exportDefinitions([saved]), extended), JSON.parse(JSON.stringify([saved])))
  await assert.rejects(model.importDefinitions(model.exportDefinitions([saved]), catalog), /Unsupported/)
  assert.ok(model.compileDraft(draft, catalog).errors.momentum_6_1)
  assert.equal(model.compileDraft(model.emptyDraft(), extended).predicate.filters.length, 0)
  assert.deepEqual(model.removeDraftFilter(draft, 'momentum_6_1').fields, { momentum_3_1: draft.fields.momentum_3_1 })
  const csv = model.resultCsv({ generation: { generation: 'fixed' }, rows: [{ security_id: 'security', ticker: 'TEST', session: '2026-09-11', values: { momentum_6_1: .25, momentum_3_1: -.05 }, patterns: {} }] }, Object.keys(fields))
  assert.ok(csv.includes('"momentum_6_1","momentum_3_1"'))
  assert.ok(csv.includes('"0.25","\'-0.05"'))
})

test('adding momentum choices leaves every built-in and default column preset unchanged', () => {
  const extended = { ...builtInCatalog, fields: { ...builtInCatalog.fields, momentum_6_1: builtInCatalog.fields.momentum_12_1, momentum_3_1: builtInCatalog.fields.momentum_12_1 } }
  assert.deepEqual(model.getBuiltInScreens(extended), model.getBuiltInScreens(builtInCatalog))
  assert.deepEqual(model.screeningColumnPresets(extended), model.screeningColumnPresets(builtInCatalog))
  assert.deepEqual(model.emptyDraft().fields, {})
})

test('CSV exports carry source generation and escape formula-like text', () => {
  const csv = model.resultCsv({ generation: { generation: 'fixed' }, rows: [{ security_id: 'security', ticker: '=SUM(1,2)', session: '2026-09-11', values: { price: 10 }, patterns: {} }] }, ['ticker', 'price'])
  assert.ok(csv.includes('"fixed","2026-09-11","security"'))
  assert.ok(csv.includes('"\'=SUM(1,2)"'))
})

test('New only is a transient request choice and resets pagination without changing rules', async () => {
  const predicate = { ...model.emptyPredicate(), filters: [{ field: 'price', min: 10 }] }
  const request = { predicate, generation: null, view: 'CURRENT', sort: 'price', descending: true, offset: 200, limit: 100 }
  const enabled = model.newOnlyRequest(request, true)
  assert.equal(enabled.offset, 0)
  assert.equal(enabled.new_only, true)
  assert.equal(enabled.predicate, predicate)
  assert.equal(request.offset, 200)
  assert.equal(model.newOnlyRequest(enabled, false).new_only, false)
  const saved = await model.makeScreen('New review', enabled.predicate, model.DEFAULT_COLUMNS, { field: enabled.sort, descending: enabled.descending })
  assert.equal('new_only' in saved, false)
  assert.equal(model.comparisonDate('2026-09-10'), 'Sep 10')
  assert.match(model.newComparisonReason('SOURCE_HISTORY_CHANGED'), /Source history/)
  assert.match(model.newComparisonReason('unknown'), /unavailable/)
})

test('New result CSV carries the filter and exact prior comparison provenance', () => {
  const result = { generation: { generation: 'current' }, new_only: true,
    comparison: { status: 'READY', previous_session: '2026-09-10', previous_generation: 'prior' },
    rows: [{ security_id: 'security', ticker: 'TEST', session: '2026-09-11', new_status: 'NEW', new_reason: 'PRIOR_NONMATCH', values: { price: 10 }, patterns: {} }] }
  const csv = model.resultCsv(result, ['ticker', 'price'])
  assert.ok(csv.includes('"new_status","new_reason","new_only","comparison_status","previous_session","previous_generation"'))
  assert.ok(csv.includes('"NEW","PRIOR_NONMATCH","true","READY","2026-09-10","prior"'))
})

const gapCatalog = { ...catalog, fields: { ...catalog.fields, volume: { type: 'number', unit: 'shares', options: [] } }, gaps: { version: model.GAP_VERSION, interval: '1d', max_formation_age: 20, fields: {
  direction: { type: 'category', unit: 'category', options: ['UP', 'DOWN'] },
  formation_age: { type: 'number', unit: 'sessions', options: [] },
  fill_fraction: { type: 'number', unit: 'fraction', options: [] },
  distance_fraction: { type: 'number', unit: 'fraction', options: [] },
} } }

test('gap group roundtrips independently and preserves percent units and old rule identity', async () => {
  const oldRule = model.emptyPredicate()
  assert.equal(model.ruleIdentity(oldRule), model.ruleIdentity({ ...oldRule, gap: null }))
  const draft = { ...model.emptyDraft(), gap: { enabled: true, fields: {
    direction: { min: '', max: '', values: ['UP'] }, fill_fraction: { min: '10', max: '50', values: [] },
    formation_age: { min: '0', max: '5', values: [] },
  } } }
  const compiled = model.compileDraft(draft, gapCatalog)
  assert.deepEqual(compiled.errors, {})
  assert.equal(compiled.predicate.filters.length, 0)
  assert.equal(compiled.predicate.gap.filters.find(field => field.field === 'fill_fraction').max, .5)
  assert.deepEqual(model.toDraft(compiled.predicate, gapCatalog), draft)
  const saved = await model.makeScreen('Gap screen', compiled.predicate, [...model.DEFAULT_COLUMNS, 'gaps'], { field: 'ticker', descending: false })
  assert.deepEqual(await model.importDefinitions(model.exportDefinitions([saved]), gapCatalog), [saved])
  assert.equal(model.removeDraftFilter(draft, 'gap').gap, undefined)
  assert.ok(model.compileDraft(draft, catalog).errors.gap)
  assert.equal(model.compileDraft({ ...draft, gap: { ...draft.gap, enabled: false } }, gapCatalog).predicate.gap, undefined)
})

test('gap age fill and draft limits fail closed without allowing cross-field injection', () => {
  for (const filter of [{ field: 'formation_age', max: 21 }, { field: 'formation_age', min: .5 }, { field: 'fill_fraction', max: 1.01 }, { field: 'distance_fraction', min: -1 }, { field: 'price', min: 10 }]) {
    assert.throws(() => model.validateGapPredicate({ version: model.GAP_VERSION, interval: '1d', filters: [filter] }, gapCatalog))
  }
  assert.throws(() => model.validateGapPredicate({ version: model.GAP_VERSION, interval: '1h', filters: [] }, gapCatalog))
  assert.throws(() => model.validateGapDraft({ enabled: true, fields: { injected: { min: '', max: '', values: [] } } }, gapCatalog))
})

const hourlyCatalog = { ...gapCatalog, hourly: { version: model.HOURLY_VERSION, interval: '1h', fields: {
  close: { type: 'number', unit: 'USD', options: [] }, change: { type: 'number', unit: 'fraction', options: [] },
  vs_ema20: { type: 'number', unit: 'fraction', options: [] }, ema20_change_3: { type: 'number', unit: 'fraction', options: [] },
} } }

test('Bearish risk hourly draft requires an explicit bound and preserves bearish daily rules', () => {
  const complete = { ...builtInCatalog, hourly: { ...hourlyCatalog.hourly, fields: Object.fromEntries(
    Object.entries(hourlyCatalog.hourly.fields).map(([field, spec]) => [field, { ...spec, interval: '1h', versions: [model.SCREENING_VERSION] }])) } }
  const builtin = model.getBuiltInScreens(complete).find(screen => screen.id === 'laggards')
  const daily = structuredClone(builtin.predicate)
  const base = model.toDraft(daily, complete)
  for (const fields of [{}, { change: { min: ' ', max: '', values: [] } }]) {
    const compiled = model.compileDraft({ ...base, hourly: { enabled: true, fields } }, complete)
    assert.deepEqual(compiled.errors, { hourly: 'Enter a Min or Max for at least one 1h field, or turn off Filter hourly context.' })
    assert.equal(compiled.predicate.hourly.filters.length, 0)
    assert.throws(() => model.validateHourlyPredicate(compiled.predicate.hourly, complete))
  }
  for (const field of ['close', 'change', 'vs_ema20', 'ema20_change_3']) for (const bound of ['min', 'max']) {
    const draft = { ...base, hourly: { enabled: true, fields: { [field]: { min: '', max: '', values: [], [bound]: '0' } } } }
    const compiled = model.compileDraft(draft, complete)
    assert.deepEqual(compiled.errors, {})
    assert.equal(compiled.predicate.hourly.filters[0][bound], 0)
    assert.equal(model.ruleIdentity({ ...compiled.predicate, hourly: undefined }), model.ruleIdentity(daily))
    model.validatePredicate(compiled.predicate, complete)
  }
  const maxOnly = { ...base, hourly: { enabled: true, fields: { change: { min: '', max: '-1', values: [] } } } }
  assert.equal(model.compileDraft(maxOnly, complete).predicate.hourly.filters[0].max, -.01)
  const disabled = model.compileDraft({ ...base, hourly: { enabled: false, fields: {} } }, complete)
  assert.deepEqual(disabled.errors, {})
  assert.equal(disabled.predicate.hourly, undefined)
  assert.deepEqual(builtin.predicate, daily)
})

test('hourly numeric errors remain field-specific without a misleading condition-count error', () => {
  for (const [min, max, message] of [['NaN', '', 'Enter finite numbers'], ['', 'Infinity', 'Enter finite numbers'], ['1', '-1', 'Minimum must not exceed maximum']]) {
    const compiled = model.compileDraft({ ...model.emptyDraft(), hourly: { enabled: true, fields: { change: { min, max, values: [] } } } }, hourlyCatalog)
    assert.deepEqual(compiled.errors, { 'hourly.change': message })
  }
})

test('hourly rules roundtrip without reinterpreting daily fields or old hashes', async () => {
  const base = model.emptyPredicate()
  assert.equal(model.ruleIdentity(base), model.ruleIdentity({ ...base, hourly: null }))
  const draft = { ...model.emptyDraft(), fields: { price: { min: '5', max: '', values: [] } },
    hourly: { enabled: true, fields: { change: { min: '1', max: '', values: [] }, vs_ema20: { min: '0', max: '2', values: [] } } } }
  const compiled = model.compileDraft(draft, hourlyCatalog)
  assert.deepEqual(compiled.errors, {})
  assert.equal(compiled.predicate.interval, '1d')
  assert.equal(compiled.predicate.hourly.interval, '1h')
  assert.equal(compiled.predicate.hourly.filters[0].min, .01)
  assert.deepEqual(model.toDraft(compiled.predicate, hourlyCatalog), draft)
  const saved = await model.makeScreen('Hourly check', compiled.predicate, [...model.DEFAULT_COLUMNS, 'hourly'], { field: 'ticker', descending: false })
  assert.deepEqual(await model.importDefinitions(model.exportDefinitions([saved]), hourlyCatalog), JSON.parse(JSON.stringify([saved])))
  assert.equal(model.removeDraftFilter(draft, 'hourly').hourly, undefined)
  assert.ok(model.compileDraft(draft, catalog).errors.hourly)
  assert.match(model.newComparisonReason('HOURLY_COMPARISON_NOT_ENABLED'), /hourly-filtered/)
  assert.match(model.hourlyTimestamp('2026-09-11T20:00:00+00:00'), /4:00 PM EDT/)
})

test('hourly-filtered displays add context without rewriting stored layouts or daily-only columns', () => {
  const columns = [...model.DEFAULT_COLUMNS, 'vs_ema20']
  const original = [...columns]
  const predicate = { ...model.emptyPredicate(), hourly: { version: model.HOURLY_VERSION, interval: '1h', filters: [{ field: 'change', min: 0 }] } }
  assert.deepEqual(model.screeningDisplayColumns(columns, predicate), [...columns, 'hourly', 'hourly_behavior'])
  assert.deepEqual(model.screeningDisplayColumns([...columns, 'hourly', 'momentum_6_1'], predicate), [...columns, 'hourly', 'hourly_behavior', 'momentum_6_1'])
  assert.deepEqual(model.screeningDisplayColumns(columns, model.emptyPredicate()), columns)
  assert.deepEqual(columns, original)
  for (const [field, label, explicit] of [['price', 'Close', 'Daily close'], ['change', 'Session change', 'Daily change'], ['volume', 'Volume', 'Daily volume'], ['vs_ema20', 'Close / EMA20 - 1', 'Daily Close / EMA20 - 1']]) {
    assert.equal(model.screeningTimeframeLabel(field, label, true), explicit)
    assert.equal(model.screeningTimeframeLabel(field, label, false), label)
  }
})

test('hourly context follows selected conditions and inclusive bounds, not a fixed indicator recipe', () => {
  const row = { values: { discovery_state: 'PULLBACK' }, hourly: { values: { close: 101, change: .008, vs_ema20: -.02, ema20_change_3: .001 }, missing: {} } }
  const original = structuredClone(row)
  const priceChange = { ...model.emptyPredicate(), filters: [{ field: 'discovery_state', values: ['PULLBACK'] }], hourly: { version: model.HOURLY_VERSION, interval: '1h', filters: [{ field: 'change', min: 0 }] } }
  assert.equal(model.hourlyObservedBehavior(row, priceChange), 'Daily pullback. 1h close rose 0.8% (min 0%).')
  const alignment = { ...model.emptyPredicate(), hourly: { ...priceChange.hourly, filters: [{ field: 'vs_ema20', min: -.03, max: 0 }, { field: 'ema20_change_3', min: 0 }] } }
  assert.equal(model.hourlyObservedBehavior(row, alignment), 'Price 2% below 1h EMA20 (min -3%, max 0%). 1h EMA20 rose 0.1% over 3 bars (min 0%).')
  assert.doesNotMatch(model.hourlyObservedBehavior(row, alignment), /Daily pullback|close rose/)
  const priceOnly = { ...model.emptyPredicate(), hourly: { ...priceChange.hourly, filters: [{ field: 'close', max: 150 }] } }
  assert.equal(model.hourlyObservedBehavior(row, priceOnly), '1h close $101 (max $150).')
  assert.equal(model.hourlyObservedBehavior(row, model.emptyPredicate()), '')
  assert.deepEqual(row, original)
  for (const value of [null, undefined, NaN, Infinity]) {
    const text = model.hourlyObservedBehavior({ values: {}, hourly: { values: { close: 100, change: value } } }, priceChange)
    assert.match(text, /1h last-bar change unavailable \(min 0%\)/)
    assert.doesNotMatch(text, /unchanged|flat|rose|fell/)
  }
  assert.equal(model.hourlyObservedBehavior({ values: {}, hourly: { values: { change: 0 } } }, priceChange), '1h close unchanged (min 0%).')
  assert.equal(model.hourlyObservedBehavior({ values: {}, hourly: { values: { vs_ema20: 0, ema20_change_3: 0 } } }, alignment), 'Price at 1h EMA20 (min -3%, max 0%). 1h EMA20 flat over 3 bars (min 0%).')
  const decline = { ...priceChange, filters: [{ field: 'discovery_trend', values: ['DOWN'] }], hourly: { ...priceChange.hourly, filters: [{ field: 'change', max: 0 }] } }
  assert.match(model.hourlyObservedBehavior({ values: { discovery_trend: 'DOWN' }, hourly: { values: { change: -.000001 } } }, decline), /Daily trend down\. 1h close fell <0.01% \(max 0%\)/)
  assert.match(model.hourlyObservedBehavior(original, priceChange, true), /^Stale hourly context\. Daily pullback\./)
  const smallBound = { ...priceChange, hourly: { ...priceChange.hourly, filters: [{ field: 'change', min: .000001 }] } }
  assert.match(model.hourlyObservedBehavior(original, smallBound), /min 0.0001%/)
  assert.ok(model.hourlyObservedBehavior(original, alignment).length < 200)
  assert.match(model.screeningFieldHelp('hourly_behavior').note, /not a prediction.*crossover/)
  const csv = model.resultCsv({ generation: { generation: 'pair' }, hourly_stale: true, rows: [{ ...original, ticker: 'TEST', security_id: 'test', session: '2026-09-11' }] }, ['hourly_behavior'], priceChange)
  assert.ok(csv.includes('"hourly_behavior"'))
  assert.ok(csv.includes(model.hourlyObservedBehavior(original, priceChange, true)))
})

test('hourly schema rejects wrong intervals, empty conditions and injected categories', () => {
  for (const hourly of [{ version: model.HOURLY_VERSION, interval: '1h', filters: [] },
    { version: model.HOURLY_VERSION, interval: '1d', filters: [{ field: 'close', min: 1 }] },
    { version: model.HOURLY_VERSION, interval: '1h', filters: [{ field: 'close', min: 1, values: [] }] }]) {
    assert.throws(() => model.validateHourlyPredicate(hourly, hourlyCatalog))
  }
  assert.throws(() => model.validateHourlyDraft({ enabled: true, fields: { invalid: { min: '', max: '', values: [] } } }, hourlyCatalog))
  const csv = model.resultCsv({ generation: { generation: 'fixed', hourly_source: { market_time: 'close-time', source_cutoff: 'observed-time', source_publication_id: 'hourly-source', slot_minutes: 30 } }, rows: [{ security_id: 'a', ticker: 'A', session: '2026-09-11', values: {}, hourly: { values: { change: .01 }, missing: {} } }] }, ['ticker', 'hourly'])
  assert.match(csv, /hourly_market_time/)
  assert.match(csv, /"close-time","observed-time","hourly-source","30"/)
})

test('saved definition roundtrip preserves rules and ordered presentation without result rows', async () => {
  const screen = await model.makeScreen('Price screen', { ...model.emptyPredicate(), filters: [{ field: 'price', min: 10 }] }, ['price', 'ticker'], { field: 'price', descending: true })
  const restored = await model.importDefinitions(model.exportDefinitions([screen]), catalog)
  assert.deepEqual(restored, [screen])
  assert.equal('rows' in restored[0], false)
  assert.equal('generation' in restored[0], false)
  assert.equal(restored[0].time_mode, 'LATEST_COMPLETE')
})

test('name and layout changes preserve revision while changed predicates advance it', async () => {
  const first = await model.makeScreen('First', model.emptyPredicate(), ['ticker'], { field: 'ticker', descending: false })
  const renamed = await model.makeScreen('Renamed', first.predicate, ['ticker', 'price'], { field: 'price', descending: true }, first)
  assert.equal(renamed.id, first.id)
  assert.equal(renamed.predicate_revision, 1)
  assert.equal(renamed.predicate_hash, first.predicate_hash)
  const revised = await model.makeScreen('Renamed', { ...first.predicate, filters: [{ field: 'price', min: 0 }] }, renamed.columns, renamed.sort, renamed)
  assert.equal(revised.predicate_revision, 2)
  assert.notEqual(revised.predicate_hash, first.predicate_hash)
  assert.deepEqual(first.predicate, model.emptyPredicate())
})

test('import fails closed for unknown keys, bad hashes, duplicate IDs and oversized files', async () => {
  const screen = await model.makeScreen('Valid', model.emptyPredicate(), ['ticker'], { field: 'ticker', descending: false })
  for (const invalid of [{ ...screen, html: '<script/>' }, { ...screen, predicate_hash: '0'.repeat(64) }, { ...screen, columns: ['made_up'] }, { ...screen, predicate: { ...screen.predicate, interval: '1h' } }]) {
    await assert.rejects(model.importDefinitions(model.exportDefinitions([invalid]), catalog))
  }
  await assert.rejects(model.importDefinitions(model.exportDefinitions([screen, screen]), catalog), /Duplicate/)
  await assert.rejects(model.importDefinitions(' '.repeat(model.MAX_IMPORT_BYTES + 1), catalog), /256/)
})

test('import conflicts are explicit copy or replace and never replace the whole library', async () => {
  const screen = await model.makeScreen('Original', model.emptyPredicate(), ['ticker'], { field: 'ticker', descending: false })
  const workspace = { ...model.emptyWorkspace(), screens: [screen], pins: [screen.id], active_id: screen.id }
  const copy = model.mergeDefinitions(workspace, [{ ...screen, name: 'Imported' }], 'COPY')
  assert.equal(copy.screens.length, 2)
  assert.equal(copy.screens[0].name, 'Original')
  assert.notEqual(copy.screens[1].id, screen.id)
  assert.deepEqual(copy.pins, workspace.pins)
  const replaced = model.mergeDefinitions(workspace, [{ ...screen, name: 'Imported' }], 'REPLACE')
  assert.equal(replaced.screens.length, 1)
  assert.equal(replaced.screens[0].name, 'Imported')
})

test('local storage failures are surfaced and saved rules remain separate from draft storage', async () => {
  const screen = await model.makeScreen('Retained', model.emptyPredicate(), ['ticker'], { field: 'ticker', descending: false })
  const workspace = { ...model.emptyWorkspace(), screens: [screen] }
  const storage = new Map()
  model.writeWorkspace({ setItem: (key, value) => storage.set(key, value) }, workspace)
  storage.set(model.DRAFT_KEY, JSON.stringify({ dirty: true, fields: { price: { min: '999' } } }))
  assert.deepEqual(await model.readWorkspace({ getItem: key => storage.get(key) }, catalog), workspace)
  assert.throws(() => model.writeWorkspace({ setItem: () => { throw new Error('Quota exceeded') } }, workspace), /Quota/)
  assert.deepEqual(workspace.screens[0].predicate, model.emptyPredicate())
})

test('fixed daily columns lead every layout exactly once while candles and instrument type are optional', () => {
  const fixed = ['ticker', 'price', 'change', 'volume']
  assert.deepEqual(model.DEFAULT_COLUMNS, fixed)
  assert.deepEqual(model.screeningColumns([]), fixed)
  const legacy = ['price', 'instrument_type', 'ticker', 'relative_volume_20', 'patterns', 'price', 'instrument_type']
  const original = [...legacy]
  assert.deepEqual(model.screeningColumns(legacy), [...fixed, 'instrument_type', 'relative_volume_20', 'patterns'])
  assert.deepEqual(legacy, original)
  assert.deepEqual(model.screeningColumns(['discovery_state', 'volume', 'change']), [...fixed, 'discovery_state'])
  assert.deepEqual(model.screeningColumns(model.screeningColumns(legacy)), model.screeningColumns(legacy))
  assert.deepEqual(model.screeningColumns(['patterns']), [...fixed, 'patterns'])
  assert.deepEqual(model.screeningColumns(model.screeningColumns(['patterns']).filter(column => column !== 'patterns')), fixed)
})

const builtInCatalog = {
  version: model.SCREENING_VERSION, patterns: {},
  fields: Object.fromEntries(['instrument_type', 'price', 'change', 'volume', 'dollar_volume_20', 'relative_volume_20', 'momentum_12_1', 'momentum_percentile', 'vs_ema20', 'vs_prior_high20', 'discovery_state', 'discovery_trend'].map(field => [field, {
    type: ['instrument_type', 'discovery_state', 'discovery_trend'].includes(field) ? 'category' : 'number', options: field === 'instrument_type' ? ['CS', 'ETF', 'ETV'] : field === 'discovery_state' ? ['PULLBACK', 'BOUNCE', 'RESUMING_UP', 'RESUMING_DOWN', 'TRENDING', 'MIXED'] : field === 'discovery_trend' ? ['UP', 'DOWN', 'MIXED'] : [],
    unit: ['momentum_12_1', 'momentum_percentile', 'vs_ema20', 'vs_prior_high20', 'change'].includes(field) ? 'fraction' : 'USD', versions: [model.SCREENING_VERSION], label: field,
  }])),
}

test('all seven earlier presets use supported state facts and older catalogs stay gated', () => {
  const screens = model.getBuiltInScreens(builtInCatalog)
  assert.deepEqual(screens.filter(screen => screen.group === 'DEFAULT' && !screen.predicate.hourly).map(screen => screen.name), ['All equities', 'Long interest', 'Bearish risk', 'Pullbacks', 'Bounces', 'Resuming up', 'Resuming down'])
  for (const screen of screens.filter(screen => ['pullbacks', 'bounces', 'resuming-up', 'resuming-down'].includes(screen.id))) {
    assert.equal(screen.predicate.filters[0].field, 'discovery_state')
    assert.equal(screen.unavailable, null)
  }
  assert.equal(screens.filter(screen => !screen.unavailable).length, 9)
  const oldCatalog = { ...builtInCatalog, fields: { ...builtInCatalog.fields } }
  delete oldCatalog.fields.discovery_state
  assert.ok(model.getBuiltInScreens(oldCatalog).find(screen => screen.id === 'pullbacks').unavailable)
})

test('available built-ins use valid daily filters, correct direction and matching layouts', async () => {
  for (const screen of model.getBuiltInScreens(builtInCatalog).filter(screen => !screen.unavailable)) {
    model.validatePredicate(screen.predicate, builtInCatalog)
    const saved = await model.makeScreen(screen.name, screen.predicate, screen.columns, screen.sort)
    model.validateScreen(saved, builtInCatalog)
    assert.equal(model.ruleIdentity(model.compileDraft(model.toDraft(screen.predicate, builtInCatalog), builtInCatalog).predicate), model.ruleIdentity(screen.predicate))
  }
  const leaders = model.getBuiltInScreens(builtInCatalog).find(screen => screen.id === 'leaders')
  const laggards = model.getBuiltInScreens(builtInCatalog).find(screen => screen.id === 'laggards')
  assert.equal(leaders.predicate.filters.find(filter => filter.field === 'momentum_12_1').min, .0001)
  assert.equal(laggards.predicate.filters.find(filter => filter.field === 'momentum_12_1').max, -.0001)
  assert.equal(leaders.sort.descending, true)
  assert.equal(laggards.sort.descending, false)
  assert.match(leaders.qualification, /not the former discovery cohort/)
})

test('hourly built-ins populate every selected daily and hourly filter with zero bounds intact', async () => {
  const complete = { ...builtInCatalog, hourly: { ...hourlyCatalog.hourly, fields: Object.fromEntries(Object.entries(hourlyCatalog.hourly.fields).map(([field, spec]) => [field, { ...spec, interval: '1h', versions: [model.SCREENING_VERSION] }])) } }
  const screens = model.getBuiltInScreens(complete).filter(screen => screen.predicate.hourly)
  assert.deepEqual(screens.map(screen => screen.id), ['pullbacks-hourly', 'uptrend-hourly'])
  assert.equal(model.getBuiltInScreens(complete).filter(screen => !screen.unavailable).length, 11)
  for (const screen of screens) {
    assert.equal(screen.unavailable, null)
    assert.deepEqual(screen.columns.slice(0, 4), model.DEFAULT_COLUMNS)
    assert.ok(screen.columns.includes('hourly'))
    assert.ok(!screen.columns.includes(screen.predicate.filters[0].field))
    const draft = model.toDraft(screen.predicate, complete)
    assert.deepEqual(draft.fields[screen.predicate.filters[0].field].values, screen.predicate.filters[0].values)
    assert.equal(draft.hourly.enabled, true)
    for (const condition of screen.predicate.hourly.filters) assert.deepEqual(draft.hourly.fields[condition.field], { min: '0', max: '', values: [] })
    const roundtrip = model.compileDraft(draft, complete)
    assert.deepEqual(roundtrip.errors, {})
    assert.equal(model.ruleIdentity(roundtrip.predicate), model.ruleIdentity(screen.predicate))
    const presets = model.screeningColumnPresets(complete, screen.columns, screen.predicate)
    assert.deepEqual(presets.screen.columns, screen.columns)
    assert.equal(model.selectedScreeningColumnPreset(screen.columns, presets), 'screen')
    const saved = await model.makeScreen(screen.name, screen.predicate, screen.columns, screen.sort)
    assert.deepEqual(await model.importDefinitions(model.exportDefinitions([saved]), complete), [saved])
    const workspace = model.openScreenTab(model.emptyWorkspace(), `builtin:${screen.id}`)
    assert.deepEqual(await model.readWorkspace({ getItem: () => JSON.stringify(workspace) }, complete), workspace)
  }
  for (const catalogWithoutHourly of [builtInCatalog, { ...complete, hourly: { ...complete.hourly, version: 'other' } },
    { ...complete, hourly: { ...complete.hourly, interval: '1d' } }]) {
    assert.ok(model.getBuiltInScreens(catalogWithoutHourly).filter(screen => screen.predicate.hourly).every(screen => screen.unavailable))
  }
  const missingField = structuredClone(complete)
  delete missingField.hourly.fields.ema20_change_3
  assert.ok(model.getBuiltInScreens(missingField).find(screen => screen.id === 'uptrend-hourly').unavailable)
  assert.equal(model.getBuiltInScreens(missingField).find(screen => screen.id === 'pullbacks-hourly').unavailable, null)
  screens[0].predicate.hourly.filters[0].min = 99
  assert.equal(model.getBuiltInScreens(complete).find(screen => screen.id === 'pullbacks-hourly').predicate.hourly.filters[0].min, 0)
})

test('built-ins are searchable, capability gated and isolated from edited copies', () => {
  assert.deepEqual(model.getBuiltInScreens(builtInCatalog, '  RESUMING ').map(screen => screen.id), ['resuming-up', 'resuming-down'])
  assert.equal(model.getBuiltInScreens(builtInCatalog, 'no-such-screen').length, 0)
  assert.ok(model.getBuiltInScreens(undefined).every(screen => screen.unavailable))
  const missing = { ...builtInCatalog, fields: { ...builtInCatalog.fields } }
  delete missing.fields.momentum_percentile
  assert.ok(model.getBuiltInScreens(missing).find(screen => screen.id === 'leaders').unavailable)
  const edited = model.getBuiltInScreens(builtInCatalog).find(screen => screen.id === 'leaders')
  edited.predicate.filters[0].min = 900
  edited.columns.reverse()
  const original = model.getBuiltInScreens(builtInCatalog).find(screen => screen.id === 'leaders')
  assert.equal(original.predicate.filters[0].min, 5)
  assert.equal(original.columns[0], 'ticker')
})

test('open tabs deduplicate saved and built-in selections; close never deletes a definition', async () => {
  const saved = await model.makeScreen('Saved', model.emptyPredicate(), ['ticker'], { field: 'ticker', descending: false })
  const initial = { ...model.emptyWorkspace(), screens: [saved] }
  let workspace = model.openScreenTab(initial, `saved:${saved.id}`)
  workspace = model.openScreenTab(workspace, 'builtin:pullbacks')
  workspace = model.openScreenTab(workspace, 'builtin:pullbacks')
  assert.deepEqual(workspace.tabs, [`saved:${saved.id}`, 'builtin:pullbacks'])
  assert.equal(workspace.active_id, null)
  workspace = model.closeScreenTab(workspace, 'builtin:pullbacks')
  assert.equal(workspace.active_id, saved.id)
  workspace = model.closeScreenTab(workspace, `saved:${saved.id}`)
  assert.equal(workspace.screens.length, 1)
  assert.equal(workspace.active_tab, null)
  assert.deepEqual(initial.tabs, [])
  assert.throws(() => model.openScreenTab(workspace, 'builtin:fake'), /Unknown/)
})

test('workspace reload migrates old pins to tabs and validates new tab identities', async () => {
  const saved = await model.makeScreen('Saved', model.emptyPredicate(), ['ticker'], { field: 'ticker', descending: false })
  const legacy = { schema_version: 1, screens: [saved], pins: [saved.id], active_id: saved.id }
  const restored = await model.readWorkspace({ getItem: () => JSON.stringify(legacy) }, builtInCatalog)
  assert.deepEqual(restored.tabs, [`saved:${saved.id}`])
  const opened = model.openScreenTab(restored, 'builtin:resuming-up')
  assert.deepEqual(await model.readWorkspace({ getItem: () => JSON.stringify(opened) }, builtInCatalog), opened)
  await assert.rejects(model.readWorkspace({ getItem: () => JSON.stringify({ ...opened, tabs: ['builtin:fake'] }) }, builtInCatalog), /Invalid/)
})

test('All eligible is an alias of the single All equities view across opening and reload', async () => {
  const saved = await model.makeScreen('All eligible custom', model.emptyPredicate(), ['ticker'], { field: 'ticker', descending: false })
  const old = { ...model.emptyWorkspace(), screens: [saved], tabs: ['builtin:pullbacks', 'builtin:all'], active_tab: 'builtin:all' }
  const restored = await model.readWorkspace({ getItem: () => JSON.stringify(old) }, builtInCatalog)
  assert.equal(restored.active_tab, null)
  assert.deepEqual(restored.tabs, ['builtin:pullbacks'])
  assert.deepEqual(restored.screens, [saved])
  const reopened = model.openScreenTab(restored, 'builtin:all')
  assert.deepEqual(reopened, restored)
  assert.equal(model.canonicalScreenTab('builtin:all'), model.canonicalScreenTab(null))
  assert.equal(model.openScreenTab(restored, `saved:${saved.id}`).active_id, saved.id)
})

test('applying filters adds their columns without losing layout order or changing rules', () => {
  const columns = [...model.DEFAULT_COLUMNS, 'vs_ema20', 'momentum_6_1']
  const predicate = { ...model.emptyPredicate(), filters: [
    { field: 'price', min: 5 }, { field: 'momentum_6_1', min: 0 },
    { field: 'momentum_3_1', max: 0 }, { field: 'dollar_volume_20', min: 20_000_000 },
    { field: 'relative_volume_20', min: 1 }, { field: 'discovery_state', values: ['PULLBACK'] },
  ] }
  const original = structuredClone({ columns, predicate })
  const applied = model.appliedScreeningColumns(columns, predicate)
  assert.deepEqual(applied, [...columns, 'momentum_3_1', 'dollar_volume_20', 'relative_volume_20', 'discovery_state'])
  assert.deepEqual(model.appliedScreeningColumns(applied, predicate), applied)
  assert.deepEqual(model.appliedScreeningColumns(applied, model.emptyPredicate()), applied)
  assert.deepEqual({ columns, predicate }, original)
  assert.equal(model.screeningColumns(applied.filter(field => field !== 'momentum_6_1')).includes('momentum_6_1'), false)
})

test('applied candle gap and hourly filters enable their grouped summary columns only when selected', async () => {
  const predicate = { ...model.emptyPredicate(),
    patterns: [{ id: 'doji', direction: 0, interval: '1d', state: 'OCCURRENCE' }], pattern_mode: 'NONE',
    gap: { version: model.GAP_VERSION, interval: '1d', filters: [] },
    hourly: { version: model.HOURLY_VERSION, interval: '1h', filters: [{ field: 'change', min: 0 }] },
  }
  const columns = model.appliedScreeningColumns(model.DEFAULT_COLUMNS, predicate)
  assert.deepEqual(columns, [...model.DEFAULT_COLUMNS, 'patterns', 'gaps', 'hourly'])
  assert.deepEqual(model.appliedScreeningColumns(model.DEFAULT_COLUMNS, model.emptyPredicate()), model.DEFAULT_COLUMNS)
  const saved = await model.makeScreen('Applied context columns', predicate, columns, { field: 'ticker', descending: false })
  assert.deepEqual((await model.importDefinitions(model.exportDefinitions([saved]), hourlyCatalog))[0].columns, columns)
})

test('column picker groups separate momentum liquidity trend and context without changing field identity', () => {
  const expected = {
    'Stock & price': ['ticker', 'price', 'change', 'instrument_type'],
    'Liquidity & activity': ['volume', 'dollar_volume_20', 'relative_volume_20'],
    Momentum: ['momentum_12_1', 'momentum_6_1', 'momentum_3_1', 'momentum_percentile'],
    Trend: ['vs_ema20', 'vs_ema50', 'vs_sma200', 'discovery_trend'],
    'Setups & context': ['discovery_state', 'patterns', 'gaps', 'hourly'],
  }
  for (const [group, fields] of Object.entries(expected)) {
    for (const field of fields) assert.equal(model.screeningColumnGroup(field, { group: 'Catalog group' }), group)
  }
  assert.equal(model.screeningColumnGroup('realized_volatility_21', { group: 'Volatility / structure' }), 'Volatility / structure')
  assert.equal(model.screeningColumnGroup('future_field'), 'Other')
})

test('column presets expose only available facts and never mutate a screen rule or layout', () => {
  const predicate = { ...model.emptyPredicate(), filters: [{ field: 'price', min: 10 }] }
  const original = structuredClone(predicate)
  const screenColumns = ['ticker', 'price', 'discovery_state']
  const presets = model.screeningColumnPresets(builtInCatalog, screenColumns)
  assert.deepEqual(Object.values(presets).map(preset => preset.label), ['Screen default', 'Overview', 'Trend', 'Momentum', 'Liquidity'])
  assert.ok(Object.values(presets).every(preset => preset.columns.includes('ticker')))
  assert.equal(presets.trend.columns.includes('vs_sma200'), false)
  assert.equal(model.selectedScreeningColumnPreset([...presets.momentum.columns], presets), 'momentum')
  assert.equal(model.selectedScreeningColumnPreset(['ticker', 'price', 'instrument_type'], presets), '')
  presets.screen.columns.push('change')
  assert.deepEqual(screenColumns, ['ticker', 'price', 'discovery_state'])
  assert.deepEqual(predicate, original)
})

test('all presets and built-ins preserve the fixed prefix without enabling optional candles or instrument type', () => {
  const presets = model.screeningColumnPresets(builtInCatalog)
  const layouts = [...Object.values(presets), ...model.getBuiltInScreens(builtInCatalog)]
  for (const layout of layouts) {
    assert.deepEqual(layout.columns.slice(0, model.DEFAULT_COLUMNS.length), model.DEFAULT_COLUMNS)
    assert.equal(new Set(layout.columns).size, layout.columns.length)
    assert.equal(layout.columns.includes('instrument_type'), false)
    assert.equal(layout.columns.includes('patterns'), false)
  }
  assert.equal(model.selectedScreeningColumnPreset(model.DEFAULT_COLUMNS, presets), 'screen')
  assert.ok(presets.momentum.columns.indexOf('momentum_12_1') >= model.DEFAULT_COLUMNS.length)
  assert.deepEqual(presets.overview.columns, model.DEFAULT_COLUMNS)
})

test('custom screen layouts can retain optional candle occurrences', () => {
  const presets = model.screeningColumnPresets(builtInCatalog, ['ticker', 'patterns', 'momentum_12_1'])
  assert.deepEqual(presets.screen.columns, [...model.DEFAULT_COLUMNS, 'patterns', 'momentum_12_1'])
  assert.equal(model.selectedScreeningColumnPreset(presets.screen.columns, presets), 'screen')
})

test('built-in column preferences roundtrip separately without changing definitions or rule hashes', async () => {
  const saved = await model.makeScreen('Unchanged', model.emptyPredicate(), ['ticker', 'price'], { field: 'ticker', descending: false })
  const workspace = { ...model.emptyWorkspace(), screens: [saved], builtin_columns_version: 2, builtin_columns: { pullbacks: model.screeningColumnPresets(builtInCatalog).momentum.columns, all: [...model.DEFAULT_COLUMNS, 'patterns'] } }
  const restored = await model.readWorkspace({ getItem: () => JSON.stringify(workspace) }, builtInCatalog)
  assert.deepEqual(restored, workspace)
  assert.deepEqual(restored.screens, [saved])
  assert.deepEqual(model.openScreenTab(restored, 'builtin:bounces').builtin_columns, workspace.builtin_columns)
  assert.equal(model.exportDefinitions(restored.screens).includes('builtin_columns'), false)
  assert.equal(model.getBuiltInScreens(builtInCatalog).find(screen => screen.id === 'pullbacks').columns.includes('patterns'), false)
})

test('invalid built-in column preferences fail closed', async () => {
  for (const builtin_columns of [{ fake: ['ticker'] }, { pullbacks: ['ticker', 'unknown'] }, { all: ['ticker', 'ticker'] }, { all: ['price'] }]) {
    await assert.rejects(model.readWorkspace({ getItem: () => JSON.stringify({ ...model.emptyWorkspace(), builtin_columns }) }, builtInCatalog))
  }
})

test('four state-built-ins omit their fixed state and implied trend from defaults and presets', () => {
  for (const screen of model.getBuiltInScreens(builtInCatalog).filter(screen => ['pullbacks', 'bounces', 'resuming-up', 'resuming-down'].includes(screen.id))) {
    const originalRule = structuredClone(screen.predicate)
    assert.deepEqual(model.constantScreeningColumns(screen.predicate), ['discovery_state', 'discovery_trend'])
    assert.deepEqual(screen.columns, [...model.DEFAULT_COLUMNS, 'momentum_12_1', 'relative_volume_20'])
    const presets = model.screeningColumnPresets(builtInCatalog, screen.columns, screen.predicate)
    for (const preset of Object.values(presets)) {
      assert.equal(preset.columns.includes('discovery_state'), false)
      assert.equal(preset.columns.includes('discovery_trend'), false)
    }
    assert.ok(presets.trend.columns.includes('vs_ema20'))
    assert.deepEqual(screen.predicate, originalRule)
    assert.ok(model.screeningColumns([...screen.columns, 'discovery_state']).includes('discovery_state'))
  }
})

test('only rule-implied constants are suppressed, never same-page values or numeric ranges', () => {
  for (const screen of model.getBuiltInScreens(builtInCatalog).filter(screen => !['pullbacks', 'bounces', 'resuming-up', 'resuming-down', 'pullbacks-hourly', 'uptrend-hourly'].includes(screen.id))) {
    assert.deepEqual(model.constantScreeningColumns(screen.predicate), [])
  }
  assert.deepEqual(model.constantScreeningColumns(model.getBuiltInScreens(builtInCatalog).find(screen => screen.id === 'pullbacks-hourly').predicate), ['discovery_state', 'discovery_trend'])
  assert.deepEqual(model.constantScreeningColumns(model.getBuiltInScreens(builtInCatalog).find(screen => screen.id === 'uptrend-hourly').predicate), ['discovery_trend'])
  assert.deepEqual(model.constantScreeningColumns({ ...model.emptyPredicate(), filters: [{ field: 'discovery_state', values: ['PULLBACK', 'RESUMING_UP'] }] }), ['discovery_trend'])
  assert.deepEqual(model.constantScreeningColumns({ ...model.emptyPredicate(), filters: [{ field: 'discovery_state', values: ['PULLBACK', 'BOUNCE'] }] }), [])
  assert.deepEqual(model.constantScreeningColumns({ ...model.emptyPredicate(), filters: [{ field: 'discovery_state', values: ['TRENDING'] }] }), ['discovery_state'])
  assert.deepEqual(model.constantScreeningColumns({ ...model.emptyPredicate(), filters: [{ field: 'relative_volume_20', min: 0, max: 0 }, { field: 'price', min: 5, max: 5 }] }), ['relative_volume_20'])
  const unrestricted = model.screeningColumnPresets(builtInCatalog)
  assert.ok(unrestricted.trend.columns.includes('discovery_state'))
  assert.ok(unrestricted.trend.columns.includes('discovery_trend'))
})

test('old built-in standard layouts upgrade once without rewriting saved screens or custom layouts', async () => {
  const oldDefault = model.screeningColumns(['ticker', 'price', 'discovery_state', 'discovery_trend', 'momentum_12_1', 'relative_volume_20'])
  const oldTrend = model.screeningColumnPresets(builtInCatalog).trend.columns
  const custom = [...oldDefault, 'instrument_type']
  const saved = await model.makeScreen('Keep my screen', model.emptyPredicate(), oldDefault, { field: 'ticker', descending: false })
  const original = { ...model.emptyWorkspace(), screens: [saved], builtin_columns: { pullbacks: oldTrend, bounces: oldDefault, 'resuming-up': custom } }
  const restored = await model.readWorkspace({ getItem: () => JSON.stringify(original) }, builtInCatalog)
  assert.equal(restored.builtin_columns_version, 2)
  assert.deepEqual(restored.builtin_columns.pullbacks, [...model.DEFAULT_COLUMNS, 'vs_ema20'])
  assert.deepEqual(restored.builtin_columns.bounces, [...model.DEFAULT_COLUMNS, 'momentum_12_1', 'relative_volume_20'])
  assert.deepEqual(restored.builtin_columns['resuming-up'], custom)
  assert.deepEqual(restored.screens, [saved])
  assert.deepEqual(original.builtin_columns.pullbacks, oldTrend)
  assert.deepEqual(await model.readWorkspace({ getItem: () => JSON.stringify(restored) }, builtInCatalog), restored)
})

test('manual re-addition of redundant fields remains available and persists after upgrade', async () => {
  const columns = model.screeningColumnPresets(builtInCatalog).trend.columns
  const current = { ...model.emptyWorkspace(), builtin_columns_version: 2, builtin_columns: { pullbacks: columns } }
  const restored = await model.readWorkspace({ getItem: () => JSON.stringify(current) }, builtInCatalog)
  assert.deepEqual(restored.builtin_columns.pullbacks, columns)
  assert.ok(restored.builtin_columns.pullbacks.includes('discovery_state'))
  await assert.rejects(model.readWorkspace({ getItem: () => JSON.stringify({ ...current, builtin_columns_version: 99 }) }, builtInCatalog), /Unsupported/)
})

test('signed screening measures use positive and negative font classes without altering values', () => {
  for (const field of ['change', 'momentum_12_1', 'vs_ema20', 'vs_ema50', 'vs_sma200', 'vs_prior_high20']) {
    assert.equal(model.screeningValueClass(field, .12), 'sw-value-positive')
    assert.equal(model.screeningValueClass(field, -.12), 'sw-value-negative')
    for (const value of [0, -0, null, undefined, NaN, Infinity, -Infinity, '0.12', 'UNAVAILABLE']) assert.equal(model.screeningValueClass(field, value), '')
  }
  assert.equal(model.formatValue(-.12, { unit: 'fraction' }), '-12%')
})

test('prices volumes ranks volatility and categorical observations remain neutral', () => {
  for (const field of ['price', 'volume', 'relative_volume_20', 'dollar_volume_20', 'momentum_percentile', 'realized_volatility_21', 'discovery_state', 'discovery_trend', 'patterns', 'unknown']) {
    for (const value of [.12, -.12, 0, 'UP', 'DOWN', 'RESUMING_UP', 'RESUMING_DOWN']) assert.equal(model.screeningValueClass(field, value), '')
  }
})