import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import Module from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import ts from 'typescript'

const require = createRequire(import.meta.url)
function load(file, dependencies = {}) {
  const filename = fileURLToPath(new URL(file, import.meta.url))
  const module = new Module(filename)
  module.filename = filename
  module.paths = Module._nodeModulePaths(path.dirname(filename))
  module.require = specifier => {
    if (Object.hasOwn(dependencies, specifier)) return dependencies[specifier]
    if (specifier === './tickerShared') return load('../src/pages/ticker/tickerShared.tsx')
    if (specifier === 'lightweight-charts') return { TickMarkType: {} }
    return require(specifier)
  }
  module._compile(ts.transpileModule(readFileSync(filename, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
  }).outputText, filename)
  return module.exports
}

const { CHART_PERIODS, readChartPreferences, writeChartPreferences, selectChartPatterns } = load('../src/pages/ticker/chartPreferences.ts')

test('missing monthly history is date-specific rather than described as live data staleness', () => {
  const { formatTickerReadError } = load('../src/pages/ticker/tickerShared.tsx')
  const message = formatTickerReadError(new Error('NATIVE_ADJUSTED_MONTHLY_HISTORY_INCOMPLETE'), '2026-10-15')
  assert.match(message, /Monthly closing history for 2026-10-15 is incomplete/)
  assert.doesNotMatch(message, /not current|latest completed market/)
  assert.equal(formatTickerReadError(new Error('Ticker stored publication identity/checksum drift'), '2026-10-15'),
    'Ticker stored publication identity/checksum drift')
  assert.equal(formatTickerReadError(null, '2026-10-15'), null)
})

test('legacy ticker links keep their interval defaults and linked pattern', () => {
  assert.equal(readChartPreferences(new URLSearchParams()).period, '2y')
  for (const [interval, period] of [['1m', '1d'], ['5m', '5d'], ['15m', '1mo'], ['30m', '1mo'],
    ['1h', '3mo'], ['1d', '2y'], ['1wk', '2y'], ['1mo', '5y']]) {
    const state = readChartPreferences(new URLSearchParams({ interval, patterns: 'on', channel: 'on', pattern: 'TRIPLE_TOP' }))
    assert.equal(state.interval, interval)
    assert.equal(state.period, period)
    assert.equal(state.showAutoPatterns, true)
    assert.equal(state.showPriceChannel, interval !== '1m')
    assert.equal(state.patternSelection, ['30m', '1h', '1d', '1wk', '1mo'].includes(interval)
      ? `frame|${interval}|TRIPLE_TOP` : 'best')
  }
})

test('all chart preferences round-trip without changing publication or date identity', () => {
  const initial = new URLSearchParams('session_date=2026-10-15&cohort_id=cohort&snapshot_id=saved&filter=keep')
  const changes = { interval: '1h', period: '6mo', analysisPin: '30m', showRsi: true,
    showAutoPatterns: true, showPriceChannel: true, patternSelection: 'frame|1h|RISING_WEDGE' }
  const saved = writeChartPreferences(initial, changes)
  assert.deepEqual(readChartPreferences(saved), changes)
  for (const key of ['session_date', 'cohort_id', 'snapshot_id', 'filter']) assert.equal(saved.get(key), initial.get(key))
  assert.equal(initial.has('period'), false)
  for (const date of ['2026-10-16', '2026-10-15']) {
    saved.set('session_date', date)
    assert.deepEqual(readChartPreferences(new URLSearchParams(saved.toString())), changes)
  }
  saved.delete('session_date')
  assert.deepEqual(readChartPreferences(saved), changes)
})

test('rapid control updates merge pending URL selections rather than overwrite each other', () => {
  let params = new URLSearchParams('cohort_id=cohort&session_date=2026-10-15&interval=1h')
  let ref
  const writes = []
  const { useChartPreferences } = load('../src/pages/ticker/chartPreferences.ts', {
    react: { useRef: initial => ref ??= { current: initial }, useCallback: callback => callback },
    'react-router-dom': { useSearchParams: () => {
      const renderedParams = params
      return [params, (update, options) => {
        assert.equal(options.replace, true)
        writes.push(update(renderedParams))
      }]
    } },
  })
  const original = useChartPreferences()
  original.updateChartPreferences({ period: '6mo' })
  original.updateChartPreferences({ showRsi: true })
  original.updateChartPreferences({ analysisPin: '1wk' })
  params = writes[0]
  useChartPreferences().updateChartPreferences({ showPriceChannel: true })
  params = writes.at(-1)
  const restored = useChartPreferences()
  assert.equal(restored.period, '6mo')
  assert.equal(restored.showRsi, true)
  assert.equal(restored.analysisPin, '1wk')
  assert.equal(restored.showPriceChannel, true)
  assert.equal(params.get('session_date'), '2026-10-15')
  params = new URLSearchParams('cohort_id=cohort&session_date=2026-10-16&interval=1d&period=1y')
  const navigated = useChartPreferences()
  navigated.updateChartPreferences({ showPriceChannel: true })
  assert.equal(writes.at(-1).get('session_date'), '2026-10-16')
  assert.equal(readChartPreferences(writes.at(-1)).showRsi, false)
  assert.equal(readChartPreferences(writes.at(-1)).period, '1y')
})

test('lookback periods, disabled toggles, follow-chart analysis and show-all survive a reload', () => {
  assert.deepEqual(CHART_PERIODS, ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y'])
  for (const period of CHART_PERIODS) {
    const saved = writeChartPreferences(new URLSearchParams('patterns=on&channel=on&rsi=on&analysis_interval=1wk'), {
      period, showAutoPatterns: false, showPriceChannel: false, showRsi: false, analysisPin: null, patternSelection: 'all',
    })
    const restored = readChartPreferences(new URLSearchParams(saved.toString()))
    assert.equal(restored.period, period)
    assert.equal(restored.analysisPin, null)
    assert.equal(restored.patternSelection, 'all')
    for (const key of ['showAutoPatterns', 'showPriceChannel', 'showRsi']) assert.equal(restored[key], false)
  }
})

test('a missing selected pattern never falls back to another pattern or interval', () => {
  const top = { type: 'TRIPLE_TOP' }, wedge = { type: 'RISING_WEDGE' }
  const selection = 'frame|1d|TRIPLE_TOP'
  assert.deepEqual(selectChartPatterns([wedge, top], '1d', selection), [top])
  assert.deepEqual(selectChartPatterns([wedge], '1d', selection), [])
  assert.deepEqual(selectChartPatterns([], '1d', selection), [])
  assert.deepEqual(selectChartPatterns([top], '1h', selection), [])
  assert.deepEqual(selectChartPatterns([wedge, top], '1d', 'best'), [wedge])
  assert.deepEqual(selectChartPatterns([wedge, top], '1d', 'all'), [wedge, top])
})

test('ticker query navigation retains the mounted view while other pages keep their existing reset scope', () => {
  let location = { pathname: '/ticker/TSLA', search: '?session_date=2026-10-15' }
  const { PortalViewBoundary } = load('../src/layout/PortalViewBoundary.tsx', {
    'react-router-dom': { useLocation: () => location },
  })
  const first = PortalViewBoundary({ children: 'chart' })
  location = { ...location, search: '?session_date=2026-10-16&period=6mo' }
  const second = PortalViewBoundary({ children: 'chart' })
  assert.equal(first.key, second.key)
  assert.notEqual(first.props.resetKey, second.props.resetKey)
  location = { pathname: '/ticker/SPY', search: location.search }
  assert.notEqual(PortalViewBoundary({ children: 'chart' }).key, second.key)
  location = { pathname: '/overview', search: '?session_date=2026-10-15' }
  const overview = PortalViewBoundary({ children: 'table' })
  location = { ...location, search: '?session_date=2026-10-16' }
  assert.notEqual(overview.key, PortalViewBoundary({ children: 'table' }).key)

  const boundary = new second.type(second.props)
  boundary.setState = next => { boundary.state = { ...boundary.state, ...next } }
  boundary.state = { failed: true }
  boundary.componentDidUpdate(first.props, { failed: true })
  assert.equal(boundary.state.failed, false)
  assert.equal(boundary.render(), 'chart')
  boundary.state = { failed: true }
  boundary.componentDidUpdate(second.props, { failed: true })
  assert.equal(boundary.state.failed, true)
  boundary.componentDidUpdate(first.props, { failed: false })
  assert.equal(boundary.state.failed, true)
})
