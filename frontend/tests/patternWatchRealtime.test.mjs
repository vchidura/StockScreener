import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8')


test('original Pattern Watch route and page remain active in realtime', () => {
  const app = read('../src/App.tsx')
  const page = read('../src/pages/PatternWatch.tsx')
  assert.equal(app.match(/path="\/pattern-watch" element=\{<PatternWatch \/>\}/g)?.length, 2)
  assert.match(page, /queryFn: \(\) => getTickers\(\)/)
  assert.match(page, /scanChartPatterns\(interval, refresh, undefined, true, sessionDate\)/)
  assert.match(page, /const tickerOnlyIntervals = new Set\(\['all'\]\)/)
  assert.match(page, /scanTickerChartPatterns\(selectedTicker, refresh, publication.snapshot_id, true\)/)
  assert.match(page, /session: realtimeOnly \? '1d' : undefined/)
  assert.match(page, /query\.set\('session_date', sessionDate\)/)
  assert.match(page, /'1mo': 'Monthly'/)
  assert.match(page, /if \(!supportsPatternInterval\(row.interval\)\) return false/)
  assert.match(page, /5m and 15m pattern research is retired/)
  assert.doesNotMatch(page, /'15m': '15 minutes'|'5m': '5 minutes/)
  assert.match(page, /useSearchParams\(\)/)
  assert.match(page, /const \[search, setSearch\] = useState\(linkedTicker\)/)
  const link = read('../src/pages/PatternWatchLink.tsx')
  assert.match(link, /params\.set\('ticker', symbol\)/)
  assert.match(link, /params\.set\('interval', 'all'\)/)
  assert.match(link, /new URLSearchParams\(search\)/)
  assert.match(read('../src/pages/StockScreeningPage.tsx'), /<PatternWatchLink ticker=\{row\.ticker\}/)
  assert.match(read('../src/pages/StockAlertsPage.tsx'), /<PatternWatchLink ticker=\{row\.ticker\}/)
})


test('Pattern Watch ticker links open the selected pattern on its chart frame', async () => {
  const source = read('../src/pages/patternWatchNavigation.ts')
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  })
  const navigation = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
  const path = navigation.patternTickerPath({
    ticker: 'AAPL', interval: '1mo', patternType: 'RISING_WEDGE', hasChannel: true,
  }, '?cohort_id=exact-cohort&snapshot_id=frozen&ticker=MSFT')
  const [pathname, query] = path.split('?')
  const params = new URLSearchParams(query)

  assert.equal(pathname, '/ticker/AAPL')
  assert.equal(params.get('cohort_id'), 'exact-cohort')
  assert.equal(params.get('snapshot_id'), 'frozen')
  assert.equal(params.has('ticker'), false)
  assert.equal(params.get('interval'), '1mo')
  assert.equal(params.get('patterns'), 'on')
  assert.equal(params.get('pattern'), 'RISING_WEDGE')
  assert.equal(params.get('channel'), 'on')
  const publishedPath = navigation.patternTickerPath({
    ticker: 'AAPL', interval: '1d', patternType: 'ASCENDING_TRIANGLE', hasChannel: false,
    snapshotId: 'exact-member-workspace',
  }, '?cohort_id=exact-cohort&snapshot_id=global-pattern-release')
  assert.equal(new URLSearchParams(publishedPath.split('?')[1]).get('snapshot_id'), 'exact-member-workspace')
  assert.match(read('../src/pages/PatternWatch.tsx'), /publication\?\.workspaces.find\(item => item.ticker === ticker\)/)

  const shared = read('../src/pages/ticker/tickerShared.tsx')
  assert.match(shared, /CHART_INTERVALS = \[[^\]]*'1mo'/)
  assert.match(shared, /PATTERN_INTERVAL_ORDER = \['1mo'/)
  assert.match(shared, /if \(interval === '1mo'\) return '5y'/)
  const ticker = read('../src/pages/TickerDetail.tsx')
  assert.match(ticker, /<option value="1mo">Monthly<\/option>/)
  assert.equal(ticker.match(/CHART_PERIODS\.map/g)?.length, 2)
  assert.match(ticker, /\{loading && chartData\.length === 0 && \(/)
})


test('ticker catalog and universe scan add only explicit realtime cohort scope', async () => {
  const calls = []
  globalThis.__patternApi = {
    get: async (...args) => { calls.push(args); return { data: 'stored-payload' } },
  }
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__patternApi }')
    .replaceAll('import.meta.env', 'globalThis.__patternEnvironment')
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  })

  test('saved Pattern Watch requests retain exact dates and reject another date instead of substituting it', async () => {
    const calls = []
    let returnedSession = '2026-10-15'
    globalThis.__patternCloseEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
    globalThis.__patternCloseApi = { get: async (url, options) => {
      calls.push([url, options.params])
      return { data: url.endsWith('/sessions')
        ? { cohort_id: 'exact-cohort', interval: '1d', sessions: ['2026-10-15', '2026-10-16'],
          data_as_of: '2026-10-16T20:00:00Z', data_interval: '1d' }
        : { publication: { session: returnedSession, read_mode: 'SESSION_CLOSE' }, results: [] } }
    } }
    const source = read('../src/services/api.ts')
      .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__patternCloseApi }')
      .replaceAll('import.meta.env', 'globalThis.__patternCloseEnvironment')
    const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
    try {
      const services = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
      const dates = await services.getPatternClosingSessions()
      assert.equal(dates.data_as_of, '2026-10-16T20:00:00Z')
      const result = await services.scanChartPatterns('1d', false, undefined, true, '2026-10-15')
      assert.equal(result.publication.session, '2026-10-15')
      assert.deepEqual(calls, [
        ['/chart-patterns/sessions', { cohort_id: 'exact-cohort' }],
        ['/chart-patterns/scan', { interval: '1d', session_date: '2026-10-15', read_mode: 'session_close', cohort_id: 'exact-cohort' }],
      ])
      returnedSession = '2026-10-16'
      await assert.rejects(services.scanChartPatterns('1d', true, undefined, true, '2026-10-15'), /selected session close/)
    } finally {
      delete globalThis.__patternCloseApi
      delete globalThis.__patternCloseEnvironment
    }
  })
  try {
    globalThis.__patternEnvironment = {}
    const services = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
    for (const realtime of [false, true]) {
      calls.length = 0
      globalThis.__patternEnvironment = {
        VITE_REALTIME_ONLY: String(realtime),
        VITE_REALTIME_COHORT_ID: ' exact-cohort ',
      }
      assert.equal(await services.getTickers(), 'stored-payload')
      assert.equal(await services.scanChartPatterns('1d'), 'stored-payload')
      assert.deepEqual(calls.map(call => call[0]), ['/tickers', '/chart-patterns/scan'])
      assert.deepEqual(calls[0][1].params, realtime ? { cohort_id: 'exact-cohort' } : {})
      assert.deepEqual(calls[1][1].params, realtime
        ? { interval: '1d', cohort_id: 'exact-cohort', read_mode: 'retained' }
        : { interval: '1d' })
    }
    calls.length = 0
    globalThis.__patternEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
    await services.scanChartPatterns('1d', false, 'exact-pattern-release')
    assert.deepEqual(calls[0][1].params, { interval: '1d', cohort_id: 'exact-cohort', snapshot_id: 'exact-pattern-release', read_mode: 'retained' })
    calls.length = 0
    globalThis.__patternEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(services.getTickers(), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(services.scanChartPatterns(), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__patternApi
    delete globalThis.__patternEnvironment
  }
})
