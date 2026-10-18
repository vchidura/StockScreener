import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

test('chart, pattern and channel reads expose the stored unavailable reason rather than a generic HTTP error', async () => {
  const failure = Object.assign(new Error('Request failed with status code 503'), {
    isAxiosError: true, response: { data: { detail: 'NATIVE_ADJUSTED_MONTHLY_HISTORY_INCOMPLETE' } },
  })
  globalThis.__tickerReadApi = { get: async () => { throw failure } }
  globalThis.__tickerReadEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'cohort' }
  try {
    const source = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8')
      .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__tickerReadApi, isAxiosError: error => error?.isAxiosError === true }')
      .replaceAll('import.meta.env', 'globalThis.__tickerReadEnvironment')
    const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
    const services = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
    for (const read of [services.getChartData, services.getChartPatterns, services.getPriceChannel]) {
      await assert.rejects(() => read('NFLX'), { message: 'NATIVE_ADJUSTED_MONTHLY_HISTORY_INCOMPLETE' })
    }
  } finally {
    delete globalThis.__tickerReadApi
    delete globalThis.__tickerReadEnvironment
  }
})

test('both deployment modes route every ticker view to the original workspace', () => {
  const app = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8')
  assert.equal(app.match(/path="\/ticker\/:symbol" element=\{<TickerWorkspace \/>\}/g)?.length, 2)
  assert.equal(app.match(/path="\/ticker\/:symbol\/:view" element=\{<TickerWorkspace \/>\}/g)?.length, 2)
  assert.doesNotMatch(app, /RealtimeTickerWorkspace/)
  const mapped = readFileSync(new URL('../src/pages/realtime/RealtimeMappedPages.tsx', import.meta.url), 'utf8')
  assert.doesNotMatch(mapped, /RealtimeTickerWorkspace|getRealtimeTickerPage|TechnicalFacts/)
  const workspace = readFileSync(new URL('../src/pages/ticker/TickerWorkspace.tsx', import.meta.url), 'utf8')
  for (const view of ['overview', 'timeframes', 'levels', 'fibonacci', 'financials']) {
    assert.ok(workspace.includes(`value: '${view}'`))
  }
  assert.doesNotMatch(workspace, /availableViews/)
  assert.match(workspace, /usePublicationQuery\(`ticker-workspace:/)
  assert.match(workspace, /queryScope = realtimeOnly \? \[publication\?\.cohort_id, snapshotId, \.\.\.\(closingView \? \['session_close'\] : \[\]\)\] : \[\]/)
  assert.doesNotMatch(workspace, /view === 'financials'/)
  assert.match(workspace, /active === 'financials' && <FinancialsView/)
  assert.match(workspace, /<TickerDetail view=\{active\} publication=\{publication\} readsEnabled=\{readsEnabled\}/)
  assert.match(workspace, /<FinancialsView profile=\{profile\}/)
  assert.match(workspace, /<EntityRail/)
  const entityRail = readFileSync(new URL('../src/pages/ticker/EntityRail.tsx', import.meta.url), 'utf8')
  assert.match(entityRail, /UNAVAILABLE: 'N\/A'/)
  assert.match(entityRail, /calendarError \|\| !calendarAvailable\s*\? calendarAvailabilityError \?\? 'N\/A'/)
  assert.match(workspace, /section_warnings\?\.reference/)
  assert.match(workspace, /referenceWarning=\{referenceWarning\}/)
  assert.match(workspace, /calendarAvailabilityError=\{publicationError\}/)
  assert.match(workspace, /active === 'overview' && \(\s*<EntityRail[\s\S]*publication=\{publication\}[\s\S]*analysisInterval=\{analysisInterval\}/)
  assert.match(workspace, /onAnalysisIntervalChange=\{setAnalysisInterval\}/)
  assert.match(workspace, /onAnalysisSetupChange=\{onAnalysisSetupChange\}/)
  assert.match(workspace, /setup=\{analysisSetup\?\.snapshotId === snapshotId \? analysisSetup\?\.setup \?\? null : null\}/)
  const detail = readFileSync(new URL('../src/pages/TickerDetail.tsx', import.meta.url), 'utf8')
  assert.match(detail, /const fibonacciSetupInterval: SetupTimeframe = tradeSetup\?\.strategy_results\.fibonacci/)
  assert.match(detail, /showing the nearest published/)
  assert.match(detail, /fibonacciSetupInterval !== setupInterval/)
  assert.match(detail, /previousQuery\?\.queryKey\[3\] === interval && \(!realtimeOnly \|\| previousQuery\.queryKey\[5\] === snapshotId\)/)
  assert.match(detail, /queryKey: \['trade-setup-multi', symbol, \.\.\.queryScope\]/)
  assert.match(detail, /queryKey: \['chart', symbol, chartRequestPeriod, interval, \.\.\.queryScope\]/)
  assert.doesNotMatch(detail, /aria-label="Run details"|tk-analysis-provenance/)
  assert.doesNotMatch(detail, /tk-analysis-meta|Confirmed against|relativeAge\(tradeSetup.computed_at\)/)
  assert.match(detail, /onAnalysisIntervalChange\?\.\(setupInterval as SetupTimeframe\)/)
  assert.match(detail, /onAnalysisSetupChange\?\.\(tradeSetup\)/)
  assert.match(entityRail, /<details className="tk-analysis-provenance" aria-label="Run details">\s*<summary>Run details<\/summary>/)
  assert.doesNotMatch(entityRail, /<details className="tk-analysis-provenance"[^>]*\bopen\b/)
  assert.ok(entityRail.indexOf('Upcoming events') < entityRail.indexOf('aria-label="Run details"'))
  const runDetails = entityRail.match(/<details className="tk-analysis-provenance"[\s\S]*?<\/details>/)?.[0]
  assert.ok(runDetails)
  assert.match(runDetails, /\['Market', evidenceRef.market_time\]/)
  assert.match(runDetails, /\['Observed', evidenceRef.observed_at\]/)
  assert.match(runDetails, /\['Published', evidenceRef.published_at\]/)
  assert.match(runDetails, /<dt>Evidence<\/dt>/)
  assert.match(runDetails, /evidenceRef.payload_sha256/)
  assert.match(runDetails, /<dt>Date<\/dt>/)
  assert.match(runDetails, /<dt>Close<\/dt>/)
  assert.match(runDetails, /<dt>Confirmed against<\/dt>/)
  assert.match(runDetails, /setup.ema_alignment.confirm_interval/)
  assert.match(runDetails, /setup.computed_at/)
  assert.match(runDetails, /\{publication.snapshot_id\}/)
  assert.match(entityRail, /evidence_refs\[analysisInterval\]/)
  assert.doesNotMatch(detail, /prevIntervalRef/)
  assert.match(detail, /if \(chartData\.length === 0\) \{[\s\S]*candleSeriesRef\.current\.setData\(\[\]\)/)
  assert.match(detail, /clearPatternSeries\(\)[\s\S]*clearChannelSeries\(\)/)
  assert.match(detail, /const resizeObserver = new ResizeObserver\([\s\S]*?resize\(width, height, true\)/)
  assert.match(detail, /fitSelectedPeriod\(\)\s*const element = chartContainerRef.current[\s\S]*?resize\(width, height, true\)/)
  const shared = readFileSync(new URL('../src/pages/ticker/tickerShared.tsx', import.meta.url), 'utf8')
  assert.match(shared, /interval === '1d'\) return '2y'/)
})

test('original ticker service contracts preserve parameters and add only explicit realtime cohort scope', async () => {
  const calls = []
  globalThis.__tickerApi = {
    get: async (...args) => { calls.push(args); return { data: 'stored-payload' } },
  }
  const source = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__tickerApi }')
    .replaceAll('import.meta.env', 'globalThis.__tickerEnvironment')
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
  try {
    globalThis.__tickerEnvironment = {}
    const services = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
    const contextSource = readFileSync(new URL('../src/services/stockContext.ts', import.meta.url), 'utf8')
      .replace("import api, { tickerParams, retainedTickerParams } from './api'", 'const api = globalThis.__tickerApi; const tickerParams = globalThis.__tickerParams; const retainedTickerParams = globalThis.__retainedTickerParams')
    globalThis.__tickerParams = services.tickerParams
    globalThis.__retainedTickerParams = services.retainedTickerParams
    const contextCompiled = ts.transpileModule(contextSource, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
    const context = await import(`data:text/javascript;base64,${Buffer.from(contextCompiled.outputText).toString('base64')}`)
    for (const realtime of [false, true]) {
      for (const refresh of [false, true]) {
        globalThis.__tickerEnvironment = { VITE_REALTIME_ONLY: String(realtime), VITE_REALTIME_COHORT_ID: ' exact-cohort ' }
        calls.length = 0
        await services.getChartData('SPY', '2y', '1h', refresh)
        await services.getChartPatterns('SPY', '15m', refresh)
        await services.getPriceChannel('SPY', '1wk', refresh)
        await services.scanTickerChartPatterns('SPY', refresh)
        await services.getLatestQuote('SPY', refresh)
        await services.getMultiTradeSetup('SPY', refresh)
        await services.getEquitySecurityProfile('SPY')
        await services.getOptionEventCalendar('SPY', 30)
        await context.getStockContext('SPY')
        assert.deepEqual(calls.map(call => call[0]), [
          '/stock/SPY/chart', '/stock/SPY/chart-patterns', '/stock/SPY/price-channel',
          '/chart-patterns/ticker/SPY', '/stock/SPY/quote', '/stock/SPY/trade-setup/multi',
          '/equity/security/SPY', '/options/calendar/SPY', '/stocks/context',
        ])
        const refreshParams = refresh ? { refresh: true } : {}
        const expected = [
          { period: '2y', interval: '1h', ...refreshParams }, { interval: '15m', ...refreshParams },
          { interval: '1wk', ...refreshParams }, refresh ? refreshParams : undefined, refreshParams,
          refresh ? refreshParams : undefined, { report_limit: 12 }, { days_forward: 30 }, { ticker: 'SPY' },
        ]
        calls.forEach((call, index) => {
          assert.deepEqual(call[1].params, realtime ? { ...expected[index], cohort_id: 'exact-cohort', read_mode: 'retained' } : expected[index])
        })
      }
    }
    calls.length = 0
    globalThis.__tickerEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
    await services.getChartData('SPY', '2y', '1h', false, 'release-pin')
    await services.getChartPatterns('SPY', '15m', false, 'release-pin')
    await services.getPriceChannel('SPY', '1wk', false, 'release-pin')
    await services.scanTickerChartPatterns('SPY', false, 'release-pin')
    await services.getLatestQuote('SPY', false, 'release-pin')
    await services.getMultiTradeSetup('SPY', false, 'release-pin')
    await services.getEquitySecurityProfile('SPY', 'release-pin')
    await services.getOptionEventCalendar('SPY', 30, 'release-pin')
    await context.getStockContext('SPY', 'release-pin')
    assert.equal(calls.length, 9)
    assert.ok(calls.every(call => call[1].params.snapshot_id === 'release-pin'))
    calls.length = 0
    await services.getChartData('SPY', '2y', '1h', false, 'close-pin', true)
    await services.getChartPatterns('SPY', '15m', false, 'close-pin', true)
    await services.getPriceChannel('SPY', '1wk', false, 'close-pin', true)
    await services.scanTickerChartPatterns('SPY', false, 'close-pin', true)
    await services.getLatestQuote('SPY', false, 'close-pin', true)
    await services.getMultiTradeSetup('SPY', false, 'close-pin', true)
    await services.getEquitySecurityProfile('SPY', 'close-pin', true)
    await services.getOptionEventCalendar('SPY', 30, 'close-pin', true)
    await context.getStockContext('SPY', 'close-pin', true)
    assert.equal(calls.length, 9)
    assert.ok(calls.every(call => call[1].params.snapshot_id === 'close-pin' && call[1].params.read_mode === 'session_close'))
    calls.length = 0
    globalThis.__tickerEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(services.getLatestQuote('SPY'), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(context.getStockContext('SPY'), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__tickerApi
    delete globalThis.__tickerEnvironment
    delete globalThis.__tickerParams
    delete globalThis.__retainedTickerParams
  }
})

test('ticker publication resolver rejects wrong ticker, cohort and substituted snapshot', async () => {
  let publication = { ticker: 'AAPL', cohort_id: 'exact-cohort', snapshot_id: 'selected-release' }
  globalThis.__tickerPublicationApi = { get: async () => ({ data: publication }) }
  globalThis.__tickerPublicationEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const source = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__tickerPublicationApi }')
    .replaceAll('import.meta.env', 'globalThis.__tickerPublicationEnvironment')
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText
  try {
    const services = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
    assert.equal((await services.getTickerWorkspacePublication('AAPL', 'selected-release')).publication.snapshot_id, 'selected-release')
    assert.equal((await services.getTickerWorkspacePublication('aapl', 'selected-release')).publication.snapshot_id, 'selected-release')
    await assert.rejects(services.getTickerWorkspacePublication('AAPL', 'another-release'), /requested release/)
    await assert.rejects(services.getTickerWorkspacePublication('AAPL', undefined, '2026-10-15'), /requested release/)
    publication = { ...publication, session: '2026-10-15', read_mode: 'SESSION_CLOSE' }
    assert.equal((await services.getTickerWorkspacePublication('AAPL', undefined, undefined, true)).publication.read_mode, 'SESSION_CLOSE')
    assert.equal((await services.getTickerWorkspacePublication('AAPL', undefined, '2026-10-15')).publication.session, '2026-10-15')
    await assert.rejects(services.getTickerWorkspacePublication('AAPL', undefined, '2026-10-14'), /requested release/)
    publication = { ...publication, ticker: 'MSFT' }
    await assert.rejects(services.getTickerWorkspacePublication('AAPL'), /requested release/)
    publication = { ...publication, ticker: 'AAPL', cohort_id: 'another-cohort' }
    await assert.rejects(services.getTickerWorkspacePublication('AAPL'), /requested release/)
  } finally {
    delete globalThis.__tickerPublicationApi
    delete globalThis.__tickerPublicationEnvironment
  }
})

test('ticker freshness uses the pinned ticker publication and preserves backend stale states', async () => {
  const calls = []
  const publication = { ticker: 'AAPL', cohort_id: 'exact-cohort', snapshot_id: 'selected-release',
    generated_at: '2026-10-11T00:04:00Z', market_time: '2026-10-09T20:00:00Z', read_at: '2026-10-11T01:00:00Z' }
  let error = null
  let setup = { ticker: 'AAPL', as_of: publication.generated_at, setup_read_metrics: {
    '1d': { status: 'READY', market_time: publication.market_time, staleness_sessions: 0, max_serveable_stale_sessions: 3 },
    '1h': { status: 'STALE', market_time: '2026-10-09T19:30:00Z', staleness_sessions: 1, max_serveable_stale_sessions: 3 },
  } }
  globalThis.__tickerFreshnessApi = { get: async (url, options) => {
    calls.push([url, options.params])
    if (error) throw error
    if (url === '/stock/AAPL/workspace-publication') return { data: publication }
    if (url === '/stock/AAPL/trade-setup/multi') return { data: setup }
    throw new Error('Ticker status must not use Dashboard or canonical health')
  } }
  globalThis.__tickerFreshnessEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const source = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__tickerFreshnessApi, isAxiosError: error => !!error.isAxiosError }')
    .replaceAll('import.meta.env', 'globalThis.__tickerFreshnessEnvironment')
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText
  try {
    const api = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
    const result = await api.fetchMaterializationStatus('selected-release', 'AAPL')
    assert.equal(result.status, 'STALE')
    assert.equal(result.staleness_sessions, 1)
    assert.deepEqual(result.stale_intervals, ['1h'])
    assert.deepEqual(calls, [
      ['/stock/AAPL/workspace-publication', { cohort_id: 'exact-cohort', snapshot_id: 'selected-release', read_mode: 'retained' }],
      ['/stock/AAPL/trade-setup/multi', { cohort_id: 'exact-cohort', snapshot_id: 'selected-release', read_mode: 'retained' }],
    ])
    setup = { ...setup, errors: { '1wk': 'SETUP_HISTORY_SHORT' } }
    const missingInterval = await api.fetchMaterializationStatus('selected-release', 'AAPL')
    assert.equal(missingInterval.status, 'UNAVAILABLE')
    assert.deepEqual(missingInterval.stale_intervals, ['1h', '1wk'])
    setup = { ...setup, as_of: 'another-release' }
    await assert.rejects(api.fetchMaterializationStatus('selected-release', 'AAPL'), /selected publication/)
    error = { isAxiosError: true, message: 'Request failed with status code 503',
      response: { data: { detail: 'Ticker reference identity changed; republish required' } } }
    await assert.rejects(api.getTickerWorkspacePublication('AAPL', 'selected-release'),
      /Ticker reference identity changed; republish required/)
  } finally {
    delete globalThis.__tickerFreshnessApi
    delete globalThis.__tickerFreshnessEnvironment
  }
})
