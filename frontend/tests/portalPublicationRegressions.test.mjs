import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import Module from 'node:module'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import test from 'node:test'
import ts from 'typescript'
import React from 'react'
import { renderToStaticMarkup } from 'react-dom/server'

const require = createRequire(import.meta.url)
const root = fileURLToPath(new URL('../src/', import.meta.url))
const read = file => readFileSync(path.join(root, file), 'utf8')

function compile(file, overrides = {}) {
  const filename = path.join(root, file)
  const module = new Module(filename)
  module.filename = filename
  module.paths = Module._nodeModulePaths(path.dirname(filename))
  module.require = specifier => Object.hasOwn(overrides, specifier) ? overrides[specifier] : require(specifier)
  module._compile(ts.transpileModule(read(file).replaceAll('import.meta.env', 'globalThis.__regressionEnvironment'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
  }).outputText, filename)
  return module.exports
}

test('market-hours banner skips freshness validation on weekends, holidays and outside session hours', async context => {
  let now = Date.parse('2026-10-09T15:00:00Z')
  context.mock.method(Date, 'now', () => now)
  const originalWindow = globalThis.window
  const originalEnvironment = globalThis.__regressionEnvironment
  globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: 'true' }
  try {
    for (const scenario of [
      { date: '2026-10-11', checked: '15:00:00', trading: false, enabled: false },
      { date: '2026-11-26', checked: '15:00:00', trading: false, enabled: false },
      { date: '2026-10-09', checked: '13:29:00', trading: true, enabled: false },
      { date: '2026-10-09', checked: '20:00:00', trading: true, enabled: false },
      { date: '2026-11-27', checked: '18:00:00', trading: true, enabled: false, close: '18:00:00' },
      { date: '2026-10-09', checked: '15:00:00', trading: true, enabled: true },
    ]) {
      const market = { calendar: 'XNYS', session: scenario.date,
        checked_at: `${scenario.date}T${scenario.checked}Z`, is_trading_session: scenario.trading,
        notifications_enabled: scenario.enabled,
        session_open: scenario.trading ? `${scenario.date}T13:30:00Z` : null,
        session_close: scenario.trading ? `${scenario.date}T${scenario.close ?? '20:00:00'}Z` : null }
      now = Date.parse(market.checked_at)
      let state, effect, freshnessReads = 0, poll
      const boundaries = new Map()
      globalThis.window = {
        setInterval: callback => { poll = callback; return 1 }, clearInterval: () => {},
        setTimeout: (callback, delay) => { boundaries.set(2, { callback, delay }); return 2 },
        clearTimeout: id => boundaries.delete(id),
      }
      const { StalenessBanner } = compile('layout/StalenessBanner.tsx', {
        react: { ...React, useState: () => [state ?? null, value => { state = value }],
          useEffect: callback => { effect ??= callback } },
        'react-router-dom': { useLocation: () => ({ pathname: '/ticker/MSFT' }),
          useSearchParams: () => [new URLSearchParams('snapshot_id=original-release')] },
        './navigation': { activeTickerSymbol: () => 'MSFT' },
        '../pages/publicationNavigation': { isDailyStrategyPath: () => false },
        '../services/api': {
          fetchMarketNotificationWindow: async () => market,
          fetchMaterializationStatus: async (pin, ticker, strategies) => {
            freshnessReads++
            assert.deepEqual([pin, ticker, strategies], ['original-release', 'MSFT', false])
            return { status: 'STALE', last_market_time: '2026-10-08T20:00:00Z',
              stale_intervals: ['1d', '30m'], staleness_sessions: 1 }
          },
        },
      })
      assert.equal(StalenessBanner(), null)
      const cleanup = effect()
      await new Promise(setImmediate)
      assert.equal(freshnessReads, scenario.enabled ? 1 : 0)
      const element = StalenessBanner()
      if (scenario.enabled) {
        assert.match(renderToStaticMarkup(element), /Delayed market data/)
        const boundary = boundaries.get(2)
        assert.equal(boundary.delay, Date.parse(market.session_close) - now)
        now = Date.parse(market.session_close)
        assert.equal(StalenessBanner(), null, 'Hide at close even before a delayed timer runs')
        market.notifications_enabled = false
        market.checked_at = market.session_close
        boundary.callback()
        await new Promise(setImmediate)
        assert.equal(freshnessReads, 1, 'Do not validate again after close')
        assert.equal(StalenessBanner(), null)
      } else {
        assert.equal(element, null)
        await poll()
        assert.equal(freshnessReads, 0)
      }
      cleanup()
      assert.equal(boundaries.size, 0)
    }
  } finally {
    globalThis.window = originalWindow
    globalThis.__regressionEnvironment = originalEnvironment
  }
})

test('market-hours banner suppresses unknown calendars and late responses, but reports failed validation while open', async () => {
  const originalWindow = globalThis.window
  const originalEnvironment = globalThis.__regressionEnvironment
  globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: 'true' }
  globalThis.window = { setInterval: () => 1, clearInterval: () => {}, setTimeout: () => 2, clearTimeout: () => {} }
  try {
    for (const calendarFails of [true, false]) {
      let state, effect, freshnessReads = 0
      const market = { calendar: 'XNYS', is_trading_session: true, notifications_enabled: true,
        checked_at: '2026-10-09T15:00:00Z', session_open: '2026-10-09T13:30:00Z', session_close: '2026-10-09T20:00:00Z' }
      const { StalenessBanner, marketNotificationsEnabled } = compile('layout/StalenessBanner.tsx', {
        react: { ...React, useState: () => [state ?? null, value => { state = value }], useEffect: callback => { effect ??= callback } },
        'react-router-dom': { useLocation: () => ({ pathname: '/' }), useSearchParams: () => [new URLSearchParams()] },
        './navigation': { activeTickerSymbol: () => undefined },
        '../pages/publicationNavigation': { isDailyStrategyPath: () => false },
        '../services/api': {
          fetchMarketNotificationWindow: async () => { if (calendarFails) throw new Error('Calendar unavailable'); return market },
          fetchMaterializationStatus: async () => { freshnessReads++; throw new Error('Publication unavailable') },
        },
      })
      StalenessBanner()
      const cleanup = effect()
      await new Promise(setImmediate)
      assert.equal(freshnessReads, calendarFails ? 0 : 1)
      if (calendarFails) assert.equal(StalenessBanner(), null)
      else assert.match(renderToStaticMarkup(StalenessBanner()), /Market data status unavailable/)
      assert.equal(marketNotificationsEnabled(market, 5 * 60 * 60 * 1000), false)
      cleanup()
    }
  } finally {
    globalThis.window = originalWindow
    globalThis.__regressionEnvironment = originalEnvironment
  }
})

test('column picker clamps its measured menu before paint and repositions on resize/scroll', () => {
  const original = { document: globalThis.document, window: globalThis.window }
  let viewportWidth = 1440, anchorRight = 1395, menuWidth = 200, refIndex = 0
  const layoutEffects = [], effects = [], listeners = new Map()
  const styles = new Map()
  const menu = {
    style: { setProperty: (key, value) => styles.set(key, value) },
    getBoundingClientRect: () => ({ width: Math.min(menuWidth, parseFloat(styles.get('--tm-column-menu-max-width') ?? 'Infinity')) }),
  }
  const container = { getBoundingClientRect: () => ({ right: anchorRight }), contains: () => true }
  const refs = [container, menu]
  const eventTarget = prefix => ({
    addEventListener: (event, callback, capture = false) => listeners.set(`${prefix}:${event}:${capture}`, callback),
    removeEventListener: (event, callback, capture = false) => {
      assert.equal(listeners.get(`${prefix}:${event}:${capture}`), callback)
      listeners.delete(`${prefix}:${event}:${capture}`)
    },
  })
  globalThis.document = { documentElement: { get clientWidth() { return viewportWidth } }, ...eventTarget('document') }
  globalThis.window = eventTarget('window')
  try {
    const { ColumnPicker } = compile('layout/PageChrome.tsx', {
      react: {
        ...React, useState: () => [true, () => {}], useRef: () => ({ current: refs[refIndex++] }),
        useCallback: callback => callback, useLayoutEffect: callback => layoutEffects.push(callback),
        useEffect: callback => effects.push(callback),
      },
      './pageChrome.css': {},
    })
    ColumnPicker({ columns: [{ key: 'ticker', label: 'Ticker' }], hidden: new Set(),
      onToggle: () => {}, onShowAll: () => {}, onReset: () => {} })
    assert.equal(layoutEffects.length, 1, 'Position the open menu before paint')
    layoutEffects[0]()
    assert.equal(menu.style.right, '0px', 'Preserve desktop right alignment when it fits')
    const cleanup = effects[0]()
    const left = () => anchorRight - menu.getBoundingClientRect().width - parseFloat(menu.style.right)
    viewportWidth = 375
    anchorRight = 106.5
    listeners.get('window:resize:false')()
    assert.equal(left(), 8, 'The formerly off-screen mobile menu starts inside the viewport')
    anchorRight = 365
    listeners.get('window:scroll:true')()
    assert.equal(left(), 165)
    anchorRight = 410
    listeners.get('window:resize:false')()
    assert.equal(left() + menu.getBoundingClientRect().width, 367, 'Clamp the right edge too')
    viewportWidth = 320
    menuWidth = 330
    listeners.get('window:resize:false')()
    assert.equal(menu.getBoundingClientRect().width, 304, 'Constrain wider grouped menus')
    assert.equal(left(), 8)
    cleanup()
    assert.equal(listeners.size, 0)
    assert.match(read('layout/pageChrome.css'), /min-width: min\(200px, var\(--tm-column-menu-max-width/)
  } finally {
    for (const [key, value] of Object.entries(original)) {
      if (value === undefined) delete globalThis[key]
      else globalThis[key] = value
    }
  }
})

test('brand navigation preserves explicit realtime identities and canonical home remains unchanged', () => {
  const source = read('layout/AppShell.tsx')
  const ast = ts.createSourceFile('AppShell.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX)
  let destination
  const visit = node => {
    if (ts.isJsxOpeningElement(node)) {
      const attributes = node.attributes.properties
      if (attributes.some(attribute => ts.isJsxAttribute(attribute) && attribute.name.getText(ast) === 'className'
        && attribute.initializer?.text === 'tm-brand')) {
        destination = attributes.find(attribute => ts.isJsxAttribute(attribute) && attribute.name.getText(ast) === 'to').initializer.expression.getText(ast)
      }
    }
    ts.forEachChild(node, visit)
  }
  visit(ast)
  assert.ok(destination)
  const home = new Function('realtimeOnly', 'realtimeHome', 'pinnedPath', `return ${destination}`)
  globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  try {
    const { publicationPath } = compile('pages/publicationNavigation.ts')
    assert.equal(home(true, '/', route => publicationPath(route, 'old', 'exact-cohort')),
      '/?cohort_id=exact-cohort&snapshot_id=old')
    assert.equal(home(true, '/', route => publicationPath(route, 'old', 'wrong-cohort')),
      '/?cohort_id=wrong-cohort&snapshot_id=old')
    assert.equal(home(false, '/', () => { throw new Error('Canonical home must not request pins') }), '/')
    globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: 'false' }
    assert.equal(home(true, '/realtime-canary?scope=equity', route => publicationPath(route, 'old')),
      '/realtime-canary?scope=equity')
  } finally {
    delete globalThis.__regressionEnvironment
  }
})

test('navigation clears incompatible ticker and summary pins without dropping the cohort', () => {
  globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  try {
    const { navigationPublicationPath } = compile('pages/publicationNavigation.ts')
    const path = (target, source, pin = 'selected-release', cohort = 'exact-cohort') =>
      navigationPublicationPath(target, source, pin, cohort)
    assert.equal(path('/overview', '/ticker/AAPL'), '/overview?cohort_id=exact-cohort')
    assert.equal(path('/ticker/AAPL', '/overview'), '/ticker/AAPL?cohort_id=exact-cohort')
    assert.equal(path('/ticker/MSFT', '/ticker/AAPL'), '/ticker/MSFT?cohort_id=exact-cohort')
    // The ticker page always shows the current view: no release pin travels to it, even within one ticker.
    assert.equal(path('/ticker/AAPL/levels', '/ticker/AAPL'), '/ticker/AAPL/levels?cohort_id=exact-cohort')
    assert.equal(path('/ticker/AAPL', '/ticker/AAPL/fibonacci'), '/ticker/AAPL?cohort_id=exact-cohort')
    assert.equal(path('/ticker/AAPL', '/gaps'), '/ticker/AAPL?cohort_id=exact-cohort')
    assert.equal(path('/overview', '/overview'), '/overview?cohort_id=exact-cohort&snapshot_id=selected-release')
    assert.equal(path('/dashboard', '/overview'), '/dashboard?cohort_id=exact-cohort')
    assert.equal(path('/overview', '/dashboard'), '/overview?cohort_id=exact-cohort')
    assert.equal(path('/gaps', '/dashboard'), '/gaps?cohort_id=exact-cohort')
    assert.equal(path('/dashboard', '/gaps'), '/dashboard?cohort_id=exact-cohort')
    assert.equal(path('/fibonacci', '/gaps'), '/fibonacci?cohort_id=exact-cohort&snapshot_id=selected-release')
    assert.equal(path('/overview', '/ticker/AAPL', 'selected-release', 'wrong-cohort'),
      '/overview?cohort_id=wrong-cohort')
    assert.match(read('layout/AppShell.tsx'),
      /navigationPublicationPath\(path, location.pathname, publicationPin, publicationCohort, publicationInterval\)/)
    globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: 'false' }
    assert.equal(path('/overview', '/ticker/AAPL'), '/overview')
  } finally {
    delete globalThis.__regressionEnvironment
  }
})

test('ticker page ignores a stale release pin and loads the current view', () => {
  const source = read('pages/publicationNavigation.ts')
  assert.match(source, /const pin = isTickerWorkspaceQuery\(name\) \? undefined : params\.get\('snapshot_id'\) \?\? undefined/)
  assert.match(source, /!isTickerWorkspaceQuery\(name\) && params\.has\('snapshot_id'\) && !pin/)
  assert.match(source, /!isTickerPath\(path\)\) params\.set\('snapshot_id', snapshotId\)/)
})

const publication = {
  snapshot_id: 'old', session: '2026-10-02', status: 'READY',
  market_time: '2026-10-02T20:00:00Z', generated_at: '2026-10-04T20:00:00Z',
  read_at: '2026-10-04T20:05:00Z', scope_note: 'Selected realtime cohort: 2 synthetic tickers.',
  missing_producers: {},
}
const overview = [
  { ticker: 'UP', close: 100, chg_pct: 2, volume: 3_000_000, rel_vol: 3 },
  { ticker: 'DOWN', close: 90, chg_pct: -2, volume: 1_000_000, rel_vol: 1 },
]

function renderDashboard({ realtime = true, retained = true, error = 'Stored scanner unavailable' } = {}) {
  globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: String(realtime) }
  const Dashboard = compile('pages/Dashboard.tsx', {
    '@tanstack/react-query': {
      useQueryClient: () => ({}),
      useQuery: ({ queryKey }) => ({
        data: queryKey[0] === 'tickers' ? overview : queryKey[0] === 'latest-price-date' ? { latest_date: publication.session } : undefined,
        error: queryKey[0] === 'tickers' && error ? new Error(error) : null, isFetching: false,
      }),
    },
    'react-router-dom': { useNavigate: () => () => {} },
    '../services/api': {},
    './EquityContextPanels': { MomentumRanks: () => null },
    '../layout/pageContext': { usePublishPageContext: () => {} },
    '../layout/PageChrome': {
      CommandBar: ({ children }) => React.createElement('div', null, children),
      CommandSpacer: () => React.createElement('span'),
    },
    './publicationNavigation': {
      publicationPath: route => route,
      usePublicationQuery: () => ({
        data: retained ? { publication, overview, scan: {
          tickerStrategies: {}, fibSentiment: {}, fibWatchlist: [], marketRegime: null,
          scanTime: publication.generated_at, publication,
        } } : undefined,
        error, loading: false,
      }),
    },
  }).default
  try {
    return renderToStaticMarkup(React.createElement(Dashboard))
  } finally {
    delete globalThis.__regressionEnvironment
  }
}

test('Dashboard retains rendered mover/volume rows and original clocks alongside failed realtime refresh', () => {
  const html = renderDashboard()
  assert.match(html, /role="alert"/)
  assert.match(html, /Stored scanner unavailable/)
  assert.match(html, /Published 2026-10-04T20:00:00Z/)
  assert.match(html, /Read at 2026-10-04T20:05:00Z/)
  assert.equal((html.match(/<tbody>/g) ?? []).length, 3)
  assert.equal((html.match(/>UP<\/td>/g) ?? []).length, 2)
  assert.equal((html.match(/>DOWN<\/td>/g) ?? []).length, 1)
  assert.doesNotMatch(html, /Movers publication unavailable|Volume publication unavailable/)
})

test('Dashboard still fails closed without retained publication and preserves canonical error rendering', () => {
  for (const options of [{ retained: false }, { realtime: false }]) {
    const html = renderDashboard(options)
    assert.equal((html.match(/Movers publication unavailable/g) ?? []).length, 2)
    assert.match(html, /Volume publication unavailable/)
    assert.match(html, /role="alert"/)
    assert.doesNotMatch(html, />UP<\/td>|>DOWN<\/td>/)
  }
})

test('retrying a ticker publication ignores a stale pin and reloads the current view', async () => {
  globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const calls = []
  try {
    const { usePublicationQuery } = compile('pages/publicationNavigation.ts', {
      react: { ...React, useState: initial => [initial, () => {}], useRef: initial => ({ current: initial }), useEffect: () => {} },
      'react-router-dom': { useSearchParams: () => [new URLSearchParams('cohort_id=exact-cohort&snapshot_id=pinned'), () => {}] },
      '@tanstack/react-query': { useQueryClient: () => ({}), useQuery: options => ({
        refetch: () => options.queryFn(), isFetching: false, error: new Error('Unavailable'),
      }) },
    })
    const release = usePublicationQuery('ticker-workspace:AAPL', async pin => { calls.push(pin); return {} })
    await release.retry()
    assert.deepEqual(calls, [undefined])
  } finally {
    delete globalThis.__regressionEnvironment
  }
})

test('ordinary ticker, strategy and Persistence browsing keeps snapshot identities internal', async () => {
  globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  try {
    const scenarios = [['ticker-workspace:NVDA', '1d'], ['daily-gaps', '1d'], ['daily-persistence', '1d'],
      ['daily-gaps', '1h'], ['daily-persistence', '1h']]
      .flatMap(([name, interval]) => [false, true].map(explicitPin => ({ name, interval, explicitPin })))
    for (const { name, interval, explicitPin: urlHasPin } of scenarios) {
      // The ticker page never honours a pin in the URL.
      const explicitPin = urlHasPin && !name.startsWith('ticker-workspace:')
      const params = new URLSearchParams(urlHasPin ? 'cohort_id=exact-cohort&snapshot_id=selected' : '')
      if (interval === '1h') params.set('interval', interval)
      const originalSearch = params.toString()
      const original = { publication: { cohort_id: 'exact-cohort', snapshot_id: 'selected' } }
      const newer = { publication: { cohort_id: 'exact-cohort', snapshot_id: 'newer' } }
      const effects = [], updates = [], writes = [], calls = []
      let options, failed = false
      const { usePublicationQuery } = compile('pages/publicationNavigation.ts', {
        react: { ...React, useState: initial => [initial, () => {}], useRef: initial => ({ current: initial }),
          useEffect: callback => effects.push(callback) },
        'react-router-dom': { useSearchParams: () => [params, update => updates.push(update(params))] },
        '@tanstack/react-query': {
          useQueryClient: () => ({ setQueryData: (key, data) => writes.push({ key, data }) }),
          useQuery: value => {
            options = value
            return { data: original, refetch: () => value.queryFn(), isFetching: false, error: null }
          },
        },
      })
      const view = usePublicationQuery(name, async pin => {
        calls.push(pin)
        if (failed) throw new Error('Publication refresh unavailable')
        return newer
      })
      effects.forEach(callback => callback())
      assert.equal(updates.length, 0)
      assert.equal(options.refetchOnMount, explicitPin ? undefined : 'always')
      await view.retry()
      assert.deepEqual(calls, [explicitPin ? 'selected' : undefined])
      await view.refresh()
      assert.deepEqual(calls, [explicitPin ? 'selected' : undefined, undefined])
      if (explicitPin) {
        assert.equal(updates[0].get('snapshot_id'), 'newer')
      } else {
        assert.equal(updates.length, 0)
        assert.ok(writes.some(({ key, data }) => key[2] === undefined && data === newer))
        assert.equal(params.toString(), originalSearch)
        assert.ok(writes.every(({ key }) => interval !== '1h' || key[3] === '1h'))
      }
      const writeCount = writes.length, updateCount = updates.length
      failed = true
      await view.refresh()
      assert.equal(writes.length, writeCount)
      assert.equal(updates.length, updateCount)
      assert.equal(view.data, original)
    }
  } finally {
    delete globalThis.__regressionEnvironment
  }
})

test('ticker publication failures stay within sections without blocking workspace navigation', () => {
  for (const state of ['loading', 'failed', 'ready', 'refreshing', 'retained', 'retained-with-warnings', 'canonical']) {
    const retained = state === 'ready' || state === 'refreshing' || state.startsWith('retained')
    const failure = 'Ticker reference identity changed; republish required'
    const referenceNotice = 'Security reference is not current; displaying the retained publication.'
    globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: state === 'canonical' ? 'false' : 'true' }
    try {
      const Page = compile('pages/ticker/TickerWorkspace.tsx', {
        react: { ...React, useState: initial => [initial, () => {}] },
        '@tanstack/react-query': { useQuery: () => ({ data: null, isFetching: false }) },
        'react-router-dom': { useParams: () => ({ symbol: 'AAPL' }), useLocation: () => ({ search: '?snapshot_id=pinned' }),
          Link: ({ children, to }) => React.createElement('a', { href: to }, children) },
        '../TickerDetail': { default: ({ readsEnabled, publicationLoading, availabilityError }) => React.createElement('div', { 'data-reads-enabled': readsEnabled },
          'Chart workspace', publicationLoading && React.createElement('div', { role: 'status', 'aria-busy': true },
            React.createElement('div', { className: 'spinner' }), 'Loading chart data...'),
          availabilityError && React.createElement('p', { role: 'alert' }, availabilityError)) },
        './EntityRail': { default: ({ referenceWarning, calendarAvailabilityError }) => React.createElement('aside', null,
          'Reference panel', referenceWarning && React.createElement('p', { role: 'alert' }, referenceWarning),
          'Event calendar', calendarAvailabilityError && React.createElement('p', { role: 'alert' }, calendarAvailabilityError)) },
        './FinancialsView': { default: () => null },
        '../../layout/pageContext': { usePublishPageContext: () => {} },
        '../../services/api': {},
        '../publicationNavigation': { usePublicationQuery: () => ({
          data: retained ? { publication: { snapshot_id: 'pinned', cohort_id: 'cohort',
            section_warnings: state.startsWith('retained') ? { reference: referenceNotice,
              ...(state === 'retained-with-warnings' ? { actions: 'Corporate-action evidence changed.', market: 'Market data is not current.' } : {}) } : {} } } : undefined,
          loading: state === 'loading' || state === 'refreshing',
          error: state === 'failed' || state === 'refreshing' ? failure : null,
          retry: () => {}, refresh: () => {},
        }) },
        './tickerShared': { formatQuoteTime: () => '' }, './ticker.css': {},
      }).default
      const html = renderToStaticMarkup(React.createElement(Page))
      if (state === 'loading') {
        assert.match(html, /Loading chart data/)
        assert.equal((html.match(/class="spinner"/g) ?? []).length, 1)
        assert.match(html, /aria-busy="true"/)
        assert.match(html, /data-reads-enabled="false"/)
      } else if (state === 'failed') {
        assert.match(html, /role="alert"/)
        assert.doesNotMatch(html, /AAPL publication unavailable/)
        assert.match(html, /Ticker reference identity changed/)
        assert.match(html, /data-reads-enabled="false"/)
        assert.match(html, /Chart workspace/)
        assert.match(html, /Reference panel/)
      }
      assert.equal((html.match(/<a href="\/ticker\/AAPL/g) ?? []).length, 5)
      assert.match(html, /Chart workspace/)
      assert.match(html, /Reference panel/)
      assert.doesNotMatch(html, /Loading AAPL workspace|Loading ticker publication|ticker-workspace__state/)
      if (state === 'refreshing') assert.match(html, /Ticker reference identity changed/)
      if (state.startsWith('retained')) {
        assert.equal((html.match(/Security reference is not current/g) ?? []).length, 1)
        assert.match(html, /data-reads-enabled="true"/)
        if (state === 'retained-with-warnings') {
          assert.match(html, /Corporate-action evidence changed/)
          assert.match(html, /Market data is not current/)
        }
      }
    } finally {
      delete globalThis.__regressionEnvironment
    }
  }
})

test('ticker route and publication loading share the chart spinner without an analysis spinner', () => {
  const app = read('App.tsx')
  assert.match(app, /const tickerRoute = useLocation\(\)\.pathname\.startsWith\('\/ticker\/'\)/)
  assert.match(app, /tickerRoute \? 'Loading chart data\.\.\.'/)
  const detail = read('pages/TickerDetail.tsx')
  assert.match(detail, /const loading = publicationLoading \|\| chartLoading/)
  assert.equal((detail.match(/className="spinner"/g) ?? []).length, 1)
  assert.match(detail, /loading && chartData\.length === 0 && \([\s\S]*?role="status"[\s\S]*?Loading chart data/)
  assert.match(detail, /!publicationLoading && !setupLoading && !tradeSetup/)
})

test('ticker reference, calendar and financial failures are labeled within their sections', () => {
  const failure = 'Ticker reference identity changed; republish required'
  const rail = compile('pages/ticker/EntityRail.tsx', {
    './tickerShared': { INTERVAL_NOUN: {}, MARKET_TIME_ZONE: 'America/New_York', money: () => '', relativeAge: () => '' },
  })
  const Financials = compile('pages/ticker/FinancialsView.tsx', { './EntityRail': { compactMoney: rail.compactMoney } }).default
  for (const availabilityError of [failure, null]) {
    const referenceHtml = renderToStaticMarkup(React.createElement(rail.default, {
      profile: null, loading: false, calendar: null, calendarLoading: false, calendarError: false,
      referenceWarning: availabilityError, calendarAvailabilityError: availabilityError,
    }))
    const financialHtml = renderToStaticMarkup(React.createElement(Financials, { profile: null, loading: false, availabilityError }))
    assert.match(referenceHtml, /Reference data/)
    assert.match(referenceHtml, /Event calendar/)
    assert.match(financialHtml, /Fundamental reports/)
    if (availabilityError) {
      assert.equal((referenceHtml.match(/role="alert"/g) ?? []).length, 2)
      assert.equal((referenceHtml.match(/Ticker reference identity changed/g) ?? []).length, 2)
      assert.match(financialHtml, /role="alert"/)
      assert.match(financialHtml, /Ticker reference identity changed/)
      assert.doesNotMatch(referenceHtml, /No published security reference record/)
      assert.doesNotMatch(financialHtml, /No fundamental reports are stored/)
    } else {
      assert.match(referenceHtml, /No published security reference record/)
      assert.match(financialHtml, /No fundamental reports are stored/)
      assert.doesNotMatch(referenceHtml + financialHtml, /role="alert"/)
    }
  }
})

test('All Tickers renders weekly-200 columns for both missing and available publications', () => {
  for (const realtime of [false, true]) {
    for (const available of [false, true]) {
      const rows = [{ ticker: 'AAPL', sector: null, date: '2026-10-09', open: 109, high: 111,
        low: 108, close: 110, volume: 1_000_000, chg_pct: 1, rel_vol: null,
        high_52w: null, low_52w: null, pct_from_high: null, pct_from_low: null,
        sma_20: null, sma_50: null, sma_200: null, dist_200: null, wsma_50: 95,
        wsma_200: available ? 100 : null, dist_200w: available ? 10 : null }]
      const setup = { results: [], count: 0, is_fresh: true }
      globalThis.__regressionEnvironment = { VITE_REALTIME_ONLY: String(realtime) }
      try {
        const Page = compile('pages/TickersOverview.tsx', {
          react: { ...React, useState: initial => [typeof initial === 'function' ? initial() : initial, () => {}],
            useMemo: compute => compute(), useCallback: callback => callback },
          'react-router-dom': { Link: ({ to, children, ...props }) => React.createElement('a', { href: to, ...props }, children) },
          '@tanstack/react-query': { useQueryClient: () => ({}),
            useQuery: ({ queryKey }) => ({ data: queryKey[0] === 'tickers' ? rows : setup, isFetching: false }) },
          '../layout/pageContext': { usePublishPageContext: () => {} },
          '../layout/sessionDate': { useSessionDate: () => ({ pinned: null }) },
          '../layout/pageChrome.css': {},
          './publicationNavigation': { usePublicationQuery: () => ({ data: { overview: rows, setup, publication },
            loading: false, refresh: () => {}, error: null }) },
          './setupPresentation': { NO_TREND_ADX: 20, momentumTitle: () => '',
            trendLabel: value => value, trendTitle: () => '' },
          '../services/api': {},
        }).default
        const html = renderToStaticMarkup(React.createElement(Page))
        assert.ok(!html.includes(publication.scope_note))
        assert.doesNotMatch(html, /Split-adjusted daily history|Reading the selected realtime cohort publication/)
        assert.doesNotMatch(html, /Published 2026-10-04T20:00:00Z|Read 2026-10-04T20:05:00Z/)
        assert.match(html, />200W MA<\/th>/)
        assert.match(html, />% from 200W<\/th>/)
        const unavailableCells = html.match(/<span title="(?:200 complete weekly bars[^\"]*|No published distance[^\"]*)">N\/A<\/span>/g) ?? []
        assert.equal(unavailableCells.length, available ? 0 : 2)
        assert.equal((html.match(/<option[^>]* disabled=""[^>]*>(?:Below|Near) 200W MA<\/option>/g) ?? []).length,
          realtime && !available ? 2 : 0)
        if (available) {
          assert.match(html, />100\.00<\/td>/)
          assert.match(html, />\+10\.0%<\/td>/)
        }
      } finally {
        delete globalThis.__regressionEnvironment
      }
    }
  }
})
