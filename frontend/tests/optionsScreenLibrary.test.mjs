import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const source = readFileSync(new URL('../src/pages/optionsScreenLibrary.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const model = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
const catalog = { categories: ['INCOME', 'DEFINED_RISK_INCOME', 'MOMENTUM', 'NEUTRAL_VOL'].map(id => ({ id, label: id })), models: [
  { id: 'WHEEL', output_kind: 'STRUCTURE', structures: [{ category_ids: ['INCOME'] }] },
  { id: 'ACTIVITY', output_kind: 'OBSERVATION', structures: [{ category_ids: ['MOMENTUM'] }] },
] , contract_filters: {
  calendar_dte: { label: 'DTE', unit: 'days', minimum: 0, maximum: 365, integer: true },
  absolute_delta: { label: 'Absolute delta', unit: 'ratio', minimum: 0, maximum: 1 },
  local_iv: { label: 'IV', unit: 'fraction', minimum: 0 },
  local_theta_per_day: { label: 'Theta', unit: 'USD/share/day' },
  day_volume: { label: 'Volume', unit: 'contracts', minimum: 0, integer: true },
  volume_open_interest_ratio: { label: 'Volume / OI', unit: 'ratio', minimum: 0 },
} }
const screen = { id: 'test-one', name: 'My options', query: { view: 'packages', category: 'INCOME', min_dte: '21', max_dte: '45', max_capital: '5000', sort: 'CAPITAL_ASC', columns: 'underlying,structure_type,legs,capital_at_risk' }, created_at: '2026-09-17T12:00:00Z', updated_at: '2026-09-17T12:00:00Z' }

test('saved package filters, columns and sort round trip in separate options storage', () => {
  const entries = new Map([['stock-screen-library-v1', 'unchanged']])
  const storage = { getItem: key => entries.get(key) ?? null, setItem: (key, value) => entries.set(key, value) }
  model.writeOptionsLibrary(storage, { schema_version: 1, screens: [screen] }, catalog)
  assert.deepEqual(model.readOptionsLibrary(storage, catalog), { schema_version: 1, screens: [screen] })
  assert.equal(entries.get('stock-screen-library-v1'), 'unchanged')
})

test('saving excludes transient session and paging state', () => {
  assert.deepEqual(model.screenQuery(new URLSearchParams('view=packages&min_dte=0&offset=100&session_date=2026-09-16&screen=test-one'), catalog), { min_dte: '0', view: 'packages' })
})

test('built-ins validate through the same contract as personal definitions', () => {
  for (const template of model.optionTemplates(catalog)) assert.ok(model.validateScreenQuery(template.query, catalog))
})

test('old tab recipes become independent built-ins, never alert-model selections', () => {
  const templates = model.optionTemplates(catalog)
  for (const id of ['all', 'unusual-calls', 'unusual-puts', 'long-calls', 'long-puts', 'structured', 'sweep', 'smile', 'board']) assert.ok(templates.some(item => item.id === id))
  for (const template of templates.filter(item => !item.unavailable)) {
    assert.equal(template.query.view, 'chain')
    for (const field of ['model', 'category', 'scope', 'preset']) assert.equal(template.query[field], undefined)
    assert.ok(model.independentScreenQuery(new URLSearchParams(template.query), catalog))
  }
  assert.equal(templates.find(item => item.id === 'unusual-calls').query.range_volume_open_interest_ratio_min, '3')
  for (const id of ['structured', 'sweep', 'smile', 'board']) assert.ok(templates.find(item => item.id === id).unavailable)
})

test('model-backed saved definitions are retained but blocked from independent screening', () => {
  const original = JSON.stringify({ schema_version: 1, screens: [screen] })
  assert.equal(model.readOptionsLibrary({ getItem: () => original }, catalog).screens[0].query.view, 'packages')
  assert.throws(() => model.independentScreenQuery(new URLSearchParams(screen.query), catalog), /alert-model/)
  assert.deepEqual(model.independentScreenQuery(new URLSearchParams(), catalog), { view: 'chain' })
})

test('open tab identities persist separately from filter definitions', () => {
  const library = { schema_version: 1, screens: [], tabs: ['builtin:unusual-calls', 'builtin:long-puts'] }
  assert.deepEqual(model.readOptionsLibrary({ getItem: () => JSON.stringify(library) }, catalog), library)
  assert.throws(() => model.readOptionsLibrary({ getItem: () => JSON.stringify({ ...library, tabs: ['bad'] }) }, catalog))
})

test('eligible-chain filters retain percentage inputs and convert exactly once', () => {
  const query = model.validateScreenQuery({ view: 'chain', range_local_iv_min: '25', range_local_iv_max: '65', range_local_theta_per_day_min: '-0.5', range_day_volume_min: '0' }, catalog)
  assert.equal(query.range_local_iv_min, '25')
  assert.deepEqual(model.chainFilterRanges(query, catalog), [
    { field: 'local_iv', minimum: .25, maximum: .65 },
    { field: 'local_theta_per_day', minimum: -.5 }, { field: 'day_volume', minimum: 0 },
  ])
})

test('eligible-chain filters reject unsupported, reversed or fractional count bounds', () => {
  for (const input of [{ category: 'INCOME' }, { scope: 'ALL' }, { range_unknown_min: '1' },
    { range_absolute_delta_min: '2' }, { range_calendar_dte_min: '1.5' },
    { range_local_iv_min: '60', range_local_iv_max: '20' }, { range_local_iv_min: ' ' },
    { range_local_iv_min: 'Infinity' }, { descending: 'false' }]) assert.throws(() => model.validateScreenQuery({ view: 'chain', ...input }, catalog))
})

test('chain definitions coexist with old package definitions without migration', () => {
  const chain = { ...screen, id: 'chain', query: { view: 'chain', range_local_iv_min: '30', columns: 'underlying,contract,local_iv', sort: 'local_iv', descending: '0' } }
  const library = { schema_version: 1, screens: [screen, chain] }
  assert.deepEqual(model.readOptionsLibrary({ getItem: () => JSON.stringify(library) }, catalog), library)
})

test('view switching does not leak category, model or typed ranges into another reader', () => {
  const chain = model.switchOptionsView(new URLSearchParams('view=packages&category=INCOME&min_dte=21&underlyer=SPY&screen=one'), 'chain')
  assert.equal(chain.toString(), 'view=chain&underlyer=SPY')
  assert.equal(model.switchOptionsView(new URLSearchParams('view=chain&range_local_iv_min=40'), 'packages').toString(), 'view=packages')
  assert.equal(model.switchOptionsView(new URLSearchParams('view=chain&session_date=2026-09-16'), 'packages').get('session_date'), '2026-09-16')
})

test('column presets always retain view identity columns and supported fields', () => {
  for (const view of ['contracts', 'packages', 'chain']) {
    const available = model.columnsForView(view)
    for (const columns of Object.values(model.optionColumnPresets(view))) {
      assert.ok(available.filter(item => item.locked).every(item => columns.includes(item.key)))
      assert.ok(columns.every(key => available.some(item => item.key === key)))
    }
  }
})

test('cross-view filters and unknown fields cannot silently disappear', () => {
  for (const query of [{ view: 'packages', min_oi: '100' }, { view: 'contracts', max_capital: '100' },
    { view: 'packages', model: 'ACTIVITY' }, { view: 'packages', sort: 'VOLUME' },
    { view: 'packages', scope: 'BOARD' }, { category: 'OTHER' }, { model: 'OTHER' }, { confidence: '99' }]) {
    assert.throws(() => model.validateScreenQuery(query, catalog))
  }
})

test('invalid ranges and incompatible models fail before querying or saving', () => {
  for (const query of [{ min_dte: '-1' }, { min_dte: '46', max_dte: '45' }, { min_dte: '1.5' },
    { max_dte: '366' }, { min_oi: 'NaN' }, { min_volume: ' ' }, { min_ratio: 'Infinity' },
    { category: 'INCOME', model: 'ACTIVITY' }, { type: 'BAD' }]) assert.throws(() => model.validateScreenQuery(query, catalog))
  assert.equal(model.validateScreenQuery({ min_oi: '0' }, catalog).min_oi, '0')
})

test('required identity columns remain visible and view-specific', () => {
  for (const columns of ['underlying', 'underlying,contract,legs', 'underlying,contract,contract']) assert.throws(() => model.validateScreenQuery({ columns }, catalog))
  assert.equal(model.validateScreenQuery({ columns: 'underlying,contract' }, catalog).columns, 'underlying,contract')
})

test('malformed, duplicate, oversized or future-version libraries are preserved', () => {
  for (const raw of ['bad json', 'x'.repeat(250001), JSON.stringify({ schema_version: 2, screens: [] }),
    JSON.stringify({ schema_version: 1, screens: [screen, screen] }),
    JSON.stringify({ schema_version: 1, screens: [{ ...screen, query: { unsupported: '1' } }] })]) {
    assert.throws(() => model.readOptionsLibrary({ getItem: () => raw }, catalog))
  }
  assert.deepEqual(model.readOptionsLibrary({ getItem: () => null }, catalog), { schema_version: 1, screens: [] })
})

test('storage failures are surfaced without success or overwriting another key', () => {
  assert.throws(() => model.writeOptionsLibrary({ setItem: () => { throw new Error('quota') } }, { schema_version: 1, screens: [screen] }, catalog), /quota/)
})

test('editing keeps identity and creation time, copying receives a new identity', () => {
  const updated = model.makeOptionsScreen('Updated', screen.query, catalog, screen)
  const copied = model.makeOptionsScreen('Copy', screen.query, catalog)
  assert.equal(updated.id, screen.id)
  assert.equal(updated.created_at, screen.created_at)
  assert.notEqual(copied.id, screen.id)
  assert.throws(() => model.makeOptionsScreen(' ', {}, catalog))
})

test('routed options screener uses stock layout and only the independent chain results', () => {
  const app = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8')
  const workspace = readFileSync(new URL('../src/pages/OptionsScreeningWorkspace.tsx', import.meta.url), 'utf8')
  const library = readFileSync(new URL('../src/pages/OptionsScreenLibrary.tsx', import.meta.url), 'utf8')
  assert.match(app, /import\('\.\/pages\/OptionsScreeningWorkspace'\)/)
  for (const name of ['sw-layout', 'sw-with-rail', 'sw-rail', 'sw-drawer', 'sw-filter-body', 'sw-filter-actions']) assert.ok(workspace.includes(name))
  assert.ok(workspace.includes('draftQuery={draftQuery}'))
  assert.ok(workspace.includes('OptionsChainResults'))
  for (const name of ['getOptionScreener', 'getOptionCandidates', 'OptionsPackageResults', 'getOptionSignals', 'getOptionOpportunities']) assert.equal(workspace.includes(name), false)
  for (const name of ['sw-library-browser', 'sw-choice-list', 'sw-choice-detail', 'sw-library-modes', 'sw-library-footer', 'sw-desktop-pins', 'sw-apply-options']) assert.ok(library.includes(name))
})

test('screener filters start closed and open only through explicit controls', () => {
  const workspace = readFileSync(new URL('../src/pages/OptionsScreeningWorkspace.tsx', import.meta.url), 'utf8')
  assert.match(workspace, /\[filtersOpen, setFiltersOpen\] = useState\(false\)/)
  assert.match(workspace, /aria-label="Toggle filters" aria-expanded=\{filtersOpen\} onClick=\{\(\) => setFiltersOpen\(value => !value\)\}/)
  assert.match(workspace, /if \(filtersOpen && !drawer.current.open\) drawer.current.showModal\(\)/)
  assert.match(workspace, /aria-label="Applied filters"/)
})

test('chain defaults match alert prices without changing explicit saved columns', () => {
  const defaults = model.visibleOptionsColumns('chain').map(column => column.key)
  assert.deepEqual(defaults, ['market_data_time', 'underlying', 'contract', 'calendar_dte', 'spot', 'otm_fraction', 'model_mark', 'day_volume', 'open_interest', 'local_iv'])
  assert.deepEqual(model.optionColumnPresets('chain')['Price / activity'], defaults)
  const selected = 'underlying,contract,absolute_delta,local_gamma'
  assert.deepEqual(model.visibleOptionsColumns('chain', selected).map(column => column.key), selected.split(','))
  assert.deepEqual(model.visibleOptionsColumns('chain', '').map(column => column.key), ['underlying', 'contract'])
  assert.equal(model.visibleOptionsColumns('chain', model.optionColumnPresets('chain').All.join(',')).length, model.chainColumns.length)
  assert.ok(model.chainColumns.every(column => column.group))
  assert.match(model.chainColumns.find(column => column.key === 'model_mark').tip, /not a current quote/)
  for (const view of ['contracts', 'packages']) assert.equal(model.visibleOptionsColumns(view).length, model.columnsForView(view).length)
  const saved = { ...screen, query: { view: 'chain', columns: selected } }
  assert.deepEqual(model.readOptionsLibrary({ getItem: () => JSON.stringify({ schema_version: 1, screens: [saved] }) }, catalog).screens[0], saved)
  for (const key of ['local_delta', 'local_rho_per_rate_point', 'bid_ask', 'mark_market_data_time', 'mark_source', 'first_observed_at', 'quality_flags']) {
    assert.ok(model.chainColumns.find(column => column.key === key).hiddenByDefault)
    assert.equal(model.validateScreenQuery({ view: 'chain', columns: `underlying,contract,${key}` }, catalog).columns, `underlying,contract,${key}`)
  }
})

test('chain table and picker share visibility, reset defaults and display-only state', () => {
  const page = readFileSync(new URL('../src/pages/OptionsScreenLibrary.tsx', import.meta.url), 'utf8')
  assert.match(page, /visibleOptionsColumns\(view, params.get\('columns'\)\)/)
  assert.match(page, /visibleOptionsColumns\('chain', params.get\('columns'\)\)/)
  assert.match(page, /<ColumnPicker columns=\{columns\}/)
  assert.match(page, /onShowAll=\{.*next.set\('columns', columns.map/)
  assert.match(page, /onReset=\{.*next.delete\('columns'\)/)
  assert.match(page, /title=\{column.tip\}/)
  assert.match(page, /optionAlertMoney\(value\)/)
  assert.match(page, /optionAlertPercent\(value\)/)
  const request = page.slice(page.indexOf('const request: EligibleChainRequest'), page.indexOf('const query = useQuery', page.indexOf('const request: EligibleChainRequest')))
  assert.doesNotMatch(request, /columns/)
  assert.doesNotMatch(page, /useColumnPreferences|option-alerts-behavior/)
})

test('contract detail preserves exact retained fields, zeros and missingness', () => {
  const row = { snapshot_id: 'original-snapshot', contract_id: 123, contract_ticker: 'O:AAPL261016C00340000',
    model_mark: '7.00', local_iv: .2246, day_volume: 0, open_interest: null, bid: null, ask: null,
    iv_converged: false, quality_flags: [], market_data_time: '2026-09-18T19:45:00Z',
    normalized_payload_sha256: 'a'.repeat(64), exercise_style: 'AMERICAN' }
  const original = structuredClone(row)
  const sections = model.optionContractDetailSections(row)
  assert.deepEqual(sections.map(section => section.title), ['Contract terms', 'Prices and economics', 'Greeks and volatility',
    'Activity', 'Model quality and assumptions', 'Source times', 'Provenance'])
  const fields = Object.fromEntries(sections.flatMap(section => section.fields.map(field => [field.key, field])))
  assert.equal(fields.snapshot_id.value, row.snapshot_id)
  assert.equal(fields.normalized_payload_sha256.value, row.normalized_payload_sha256)
  assert.equal(fields.model_mark.value, '7.00')
  assert.equal(fields.model_mark.format, 'money')
  assert.equal(fields.local_iv.format, 'percent')
  assert.equal(fields.day_volume.value, 0)
  assert.equal(fields.iv_converged.value, false)
  assert.equal(fields.open_interest.value, null)
  assert.equal(fields.shares_per_contract.value, null)
  assert.equal(fields.bid.value, null)
  assert.deepEqual(fields.quality_flags.value, [])
  assert.deepEqual(row, original)
  assert.equal(new Set(sections.flatMap(section => section.fields.map(field => field.key))).size, Object.keys(fields).length)
  assert.ok(Object.values(fields).every(field => field.label && field.format))
  for (const forbidden of ['candidate_id', 'stop_loss', 'take_profit', 'confidence', 'execution_permission']) assert.equal(fields[forbidden], undefined)
})

test('screener View opens a fixed snapshot without new queries or saved screen fields', () => {
  const page = readFileSync(new URL('../src/pages/OptionsScreenLibrary.tsx', import.meta.url), 'utf8')
  const chain = page.slice(page.indexOf('export function OptionsChainResults'), page.indexOf('export function OptionsPackageResults'))
  const detail = page.slice(page.indexOf('function OptionContractDetail'), page.indexOf('export function OptionsChainResults'))
  assert.match(chain, /setSelectedContract\(structuredClone\(row\)\)/)
  assert.match(chain, /contractTrigger.current = event.currentTarget/)
  assert.match(chain, /if \(!selectedContract\) contractTrigger.current\?\.focus\(\{ preventScroll: true \}\)/)
  assert.match(chain, /className="os-contract-view" scope="col">View/)
  assert.match(chain, /aria-label=\{`View \$\{row.contract_ticker\} details`\}/)
  assert.match(chain, /colSpan=\{columns.length \+ 1\}/)
  assert.match(chain, /<OptionContractDetail row=\{selectedContract\} close=\{\(\) => setSelectedContract\(null\)\}/)
  assert.equal((chain.match(/useQuery\(/g) || []).length, 1)
  assert.doesNotMatch(detail, /useQuery|fetch\(|getOptionCandidate/)
  assert.match(detail, /<Modal title="Option contract details"/)
  assert.match(detail, /optionContractDetailSections\(row\)/)
  assert.match(detail, /className=\{detailTone\(field.key, field.value\)\}/)
  assert.match(detail, /key === 'contract_type'/)
  assert.match(detail, /\['model_mark', 'display_mark'\]/)
  assert.match(detail, /'local_delta', 'local_theta_per_day', 'local_vega_per_vol_point', 'local_rho_per_rate_point'/)
  assert.match(detail, /JSON.stringify\(row, null, 2\)/)
  assert.match(detail, /Not a fill or an alert package/)
  const style = readFileSync(new URL('../src/pages/OptionsScreenerPage.css', import.meta.url), 'utf8')
  assert.match(style, /\.sw-modal:has\(\.os-contract-detail\).*inset: 0 0 0 auto/)
  assert.match(style, /width: min\(680px, 100%\); max-width: 100%/)
  assert.match(style, /\.os-contract-facts dd.*overflow-wrap: anywhere/)
})

test('screener ticker links and table colors match the shared table conventions', () => {
  const page = readFileSync(new URL('../src/pages/OptionsScreenLibrary.tsx', import.meta.url), 'utf8')
  const chain = page.slice(page.indexOf('export function OptionsChainResults'), page.indexOf('export function OptionsPackageResults'))
  assert.ok(chain.includes('<Link to={`/ticker/${encodeURIComponent(row.underlying)}`}'))
  assert.match(chain, /title=\{`Open \$\{row.underlying\} ticker page`\}/)
  assert.match(chain, /aria-label=\{`View \$\{row.contract_ticker\} details`\}/)
  const style = readFileSync(new URL('../src/pages/OptionsScreenerPage.css', import.meta.url), 'utf8')
  assert.match(style, /\.options-screening-workspace \.os-package-table th \{ color: var\(--tm-muted\); \}/)
  assert.match(style, /\.options-screening-workspace \.os-package-table td \{ color: var\(--tm-ink\); \}/)
  assert.match(style, /\.options-screening-workspace \.os-package-table a \{ color: var\(--tm-accent\); font-weight: 700;/)
  assert.match(style, /\.os-package-table td small.*color: var\(--tm-muted\)/)
})