import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8')
const dashboard = read('../src/pages/Dashboard.tsx')

test('both modes retain the original Dashboard, with explicit unavailable navigation and cohort scope', () => {
  const app = read('../src/App.tsx')
  assert.equal(app.match(/path="\/" element=\{<Dashboard \/>\}/g)?.length, 2)
  assert.match(read('../src/layout/navigation.ts'), /label: 'Dashboard'/)
  assert.match(read('../src/layout/AppShell.tsx'), /NAV_GROUPS\.map/)
  for (const section of ['Top Movers', 'Top Gainers', 'Top Losers', 'Unusual Volume', 'Scanner Agreement', 'Fib Sentiment', 'Near Key Levels']) {
    assert.ok(dashboard.includes(section), section)
  }
  assert.match(dashboard, /<MomentumRanks snapshotId=\{publication\?\.snapshot_id\} \/>/)
  assert.match(dashboard, /Selected Cohort Breadth/)
  assert.doesNotMatch(dashboard, /disabled=\{realtimeOnly && !publication\}/)
  assert.match(dashboard, /onClick=\{\(\) => navigate\(publicationPath\(s\.path, publication\?\.snapshot_id\)\)\}/)
  assert.match(dashboard, /publicationPath\('\/overview', publication\?\.snapshot_id\)/)
  assert.doesNotMatch(dashboard, /getMarketConditions|getSectorIntelligence|getRealtime/)
  assert.match(dashboard, /role="alert"/)
  assert.match(dashboard, /publication\.generated_at/)
  assert.match(dashboard, /publication\.read_at/)
  assert.match(dashboard, /scanTime: realtimeOnly \? combined.scan_datetime/)
  assert.match(dashboard, /minmax\(min\(100%, 300px\), 1fr\)/)
})

test('portal render failures keep the shell available and reset on navigation', () => {
  const app = read('../src/App.tsx')
  const boundary = read('../src/layout/PortalViewBoundary.tsx')
  assert.equal(app.match(/<PortalViewBoundary>/g)?.length, 2)
  assert.match(boundary, /key=\{`\$\{location\.pathname\}\$\{location\.search\}`\}/)
  assert.match(boundary, /This page could not be displayed/)
  assert.match(boundary, /The rest of the portal is still available/)
  assert.match(boundary, />\s*Retry\s*</)
})

test('all actual Dashboard requests preserve canonical contracts and pin realtime cohort/publication', async () => {
  const calls = []
  globalThis.__dashboardApi = {
    get: async (...args) => { calls.push(args); return { data: { latest_date: '2026-10-02' } } },
  }
  const compile = source => ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__dashboardApi }')
    .replaceAll('import.meta.env', 'globalThis.__dashboardEnvironment')
  try {
    globalThis.__dashboardEnvironment = {}
    const api = await import(`data:text/javascript;base64,${Buffer.from(compile(source)).toString('base64')}`)
    globalThis.__dashboardParams = api.tickerParams
    const context = await import(`data:text/javascript;base64,${Buffer.from(compile(read('../src/services/stockContext.ts')
      .replace("import api, { tickerParams, retainedTickerParams } from './api'", 'const api = globalThis.__dashboardApi; const tickerParams = globalThis.__dashboardParams; const retainedTickerParams = () => { throw new Error("Dashboard must not use ticker retained browsing") }'))).toString('base64')}`)
    for (const realtime of [false, true]) {
      for (const refresh of [false, true]) {
        calls.length = 0
        globalThis.__dashboardEnvironment = { VITE_REALTIME_ONLY: String(realtime), VITE_REALTIME_COHORT_ID: ' exact-cohort ' }
        const snapshot = realtime ? 'immutable-publication' : undefined
        assert.equal(await api.getLatestPriceDate(refresh), '2026-10-02')
        await api.getTickersOverview(undefined, refresh, snapshot)
        await api.scanAll(undefined, undefined, refresh, snapshot)
        await context.getStockContext(undefined, snapshot)
        assert.deepEqual(calls.map(call => call[0]), ['/latest-price-date', '/tickers/overview', '/scan/all', '/stocks/context'])
        const refreshed = refresh ? { refresh: true } : {}
        const pinned = realtime ? { snapshot_id: snapshot } : {}
        const expected = [refreshed, { ...refreshed, ...pinned }, { min_swing_pct: 5, ...refreshed, ...pinned }, realtime ? pinned : undefined]
        calls.forEach((call, index) => assert.deepEqual(call[1].params, realtime ? { ...expected[index], cohort_id: 'exact-cohort' } : expected[index]))
      }
    }
    calls.length = 0
    globalThis.__dashboardEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(api.getLatestPriceDate(), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(api.getTickersOverview(), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(api.scanAll(), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(context.getStockContext(), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__dashboardApi
    delete globalThis.__dashboardParams
    delete globalThis.__dashboardEnvironment
  }
})

test('Dashboard dependencies use one immutable publication and failures never become fabricated no-match results', () => {
  assert.match(dashboard, /usePublicationQuery\('dashboard-publication'/)
  assert.match(dashboard, /getTickersOverview\(undefined, false, publication\.snapshot_id\)/)
  assert.match(dashboard, /scanAll\(undefined, undefined, refresh, snapshotId\)/)
  assert.match(dashboard, /requireMatchingPublication\(scan\.publication, publication\)/)
  assert.match(dashboard, /setScanError\(err instanceof Error/)
  assert.match(dashboard, /retry: realtimeOnly \? false : undefined/)
  assert.match(dashboard, /Volume publication unavailable/)
  const context = read('../src/pages/EquityContextPanels.tsx')
  assert.match(context, /queryKey: snapshotId \? \['stock-context', snapshotId\]/)
  assert.match(context, /getStockContext\(undefined, snapshotId\)/)
  assert.match(context, /Sector references unavailable/)
  assert.match(context, /Cohort percentile/)
})

test('notification calendar uses a global calendar-only request in both deployment modes', async () => {
  const calls = []
  let result = { calendar: 'XNYS', checked_at: '2026-10-11T15:00:00Z', session: '2026-10-11',
    is_trading_session: false, notifications_enabled: false, session_open: null, session_close: null }
  globalThis.__calendarApi = { get: async (...args) => { calls.push(args); return { data: result } } }
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__calendarApi }')
    .replaceAll('import.meta.env', 'globalThis.__calendarEnvironment')
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  try {
    globalThis.__calendarEnvironment = {}
    const api = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
    for (const realtime of [false, true]) {
      globalThis.__calendarEnvironment = { VITE_REALTIME_ONLY: String(realtime) }
      assert.deepEqual(await api.fetchMarketNotificationWindow(), result)
    }
    assert.deepEqual(calls, [['/equity/notification-window'], ['/equity/notification-window']])
    for (const invalid of [
      { ...result, calendar: 'UNKNOWN' }, { ...result, checked_at: 'invalid' },
      { ...result, is_trading_session: true, notifications_enabled: true },
    ]) {
      result = invalid
      await assert.rejects(api.fetchMarketNotificationWindow(), /Exchange notification window is unavailable/)
    }
  } finally {
    delete globalThis.__calendarApi
    delete globalThis.__calendarEnvironment
  }
})

test('shell freshness status pins the realtime cohort and Dashboard publication', async () => {
  const calls = []
  globalThis.__dashboardStatusApi = {
    get: async (...args) => {
      calls.push(args)
      return { data: { status: 'READY', pipeline_active: false } }
    },
  }
  const compile = source => ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__dashboardStatusApi }')
    .replaceAll('import.meta.env', 'globalThis.__dashboardStatusEnvironment')
  try {
    globalThis.__dashboardStatusEnvironment = {
      VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: ' exact-cohort ',
    }
    const api = await import(`data:text/javascript;base64,${Buffer.from(compile(source)).toString('base64')}`)
    await api.fetchMaterializationStatus('immutable-publication')
    await api.getTradingSessions()
    assert.deepEqual(calls, [[
      '/equity/materialization-status',
      { params: { snapshot_id: 'immutable-publication', cohort_id: 'exact-cohort' } },
    ], [
      '/trading-sessions',
      { params: { interval: '1d', limit: 260, cohort_id: 'exact-cohort' } },
    ]])
    calls.length = 0
    globalThis.__dashboardStatusEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(api.fetchMaterializationStatus('immutable-publication'), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(api.getTradingSessions(), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
    const banner = read('../src/layout/StalenessBanner.tsx')
    assert.match(banner, /params\.get\('snapshot_id'\)/)
    assert.match(banner, /fetchMaterializationStatus\(snapshotId, ticker, dailyStrategies, interval\)/)
    assert.match(banner, /activeTickerSymbol\(location.pathname\)/)
    assert.match(banner, /\[realtimeOnly, snapshotId, ticker, dailyStrategies, scope\]/)
    assert.match(banner, /Delayed market data/)
    assert.match(banner, /Pages remain available with their latest retained content/)
    assert.doesNotMatch(banner, /remain blocked|no longer served/)
  } finally {
    delete globalThis.__dashboardStatusApi
    delete globalThis.__dashboardStatusEnvironment
  }
})
