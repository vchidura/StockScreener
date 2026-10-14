import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const app = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8')
const page = readFileSync(new URL('../src/pages/RealtimeCanaryPage.tsx', import.meta.url), 'utf8')
const service = readFileSync(new URL('../src/services/realtimeCanary.ts', import.meta.url), 'utf8')
const navigation = readFileSync(new URL('../src/layout/navigation.ts', import.meta.url), 'utf8')
const shell = readFileSync(new URL('../src/layout/AppShell.tsx', import.meta.url), 'utf8')
const globalSearch = readFileSync(new URL('../src/layout/GlobalSearch.tsx', import.meta.url), 'utf8')
const mapped = readFileSync(new URL('../src/pages/realtime/RealtimeMappedPages.tsx', import.meta.url), 'utf8')
const vite = readFileSync(new URL('../vite.config.ts', import.meta.url), 'utf8')
const tasks = JSON.parse(readFileSync(new URL('../../.vscode/tasks.json', import.meta.url), 'utf8'))

test('realtime deployment restores the full portal shell around the dedicated canary', () => {
  assert.match(app, /path="\/realtime-canary" element=\{<RealtimeCanaryPage \/>\}/)
  assert.match(service, /api\.get\('\/realtime\/canary'/)
  assert.match(service, /const cohortParams =/)
  assert.match(page, /searchParams\.get\('cohort_id'\)/)
  assert.match(page, /getRealtimeCanaryStatus\(cohortId, signal\)/)
  for (const value of ['getRealtimeTickerPage', 'getRealtimeScreening', 'getRealtimeStockAlerts', 'getRealtimeOptionAlerts']) {
    assert.ok(page.includes(value), `missing ${value}`)
  }
  assert.doesNotMatch(service, /api\.(post|put|patch|delete)/)
  assert.doesNotMatch(service, /\/stocks\//)
  assert.doesNotMatch(service, /\/options\//)
  assert.doesNotMatch(navigation, /realtime-canary/)
  assert.match(app, /VITE_REALTIME_ONLY === 'true'/)
  assert.match(app, /Realtime-only portal requires VITE_REALTIME_COHORT_ID/)
  assert.match(app, /<PortalRoutes realtimeOnly realtimeHome=\{realtimeHome\} \/>/)
  assert.equal(app.match(/<SessionDateProvider>/g)?.length, 2)
  assert.doesNotMatch(app, /SessionDateProvider enabled=/)
  for (const route of [
    '/', '/overview', '/market-conditions', '/sector-intelligence', '/pattern-watch',
    '/ticker/:symbol', '/stocks/screener', '/stocks/alerts', '/stocks/persistence',
    '/gaps', '/ma-crossover', '/momentum-pullback', '/bearish-bounce', '/fibonacci',
    '/options/activity', '/options/flow', '/options/screener', '/options/alerts',
  ]) {
    assert.ok(app.includes(`path="${route}"`), `missing restored route ${route}`)
  }
  for (const reader of ['getRealtimeHealth', 'getRealtimeOptionAlerts']) {
    assert.ok(mapped.includes(reader), `missing mapped reader ${reader}`)
  }
  assert.match(shell, /NAV_GROUPS\.map/)
  assert.match(shell, /PINNED_ITEMS\.map/)
  assert.match(shell, /Validation status/)
  assert.match(shell, /realtimeOnly \? pinnedPath\(baseDestination\) : baseDestination/)
  assert.match(shell, /publicationPin \|\| publicationCohort/)
  assert.match(shell, /pinnedPath\('\/realtime-canary'\)/)
  assert.match(shell, /<GlobalSearch \/>/)
  assert.match(shell, /<StalenessBanner \/>/)
  assert.doesNotMatch(shell, /!realtimeOnly && <GlobalSearch/)
  assert.doesNotMatch(shell, /!realtimeOnly && <StalenessBanner/)
  assert.match(globalSearch, /navigate\(publicationPath\(/)
  assert.match(globalSearch, /searchParams\.get\('snapshot_id'\)/)
  assert.match(globalSearch, /searchParams\.get\('cohort_id'\)/)
  assert.equal(app.match(/path="\/options\/alerts" element=\{<OptionsAlertsPage \/>\}/g)?.length, 2)
  assert.doesNotMatch(app, /RealtimeOptionsAlertsPage/)
})

test('realtime screener restores the original page and only adds explicit cohort scope', () => {
  const screening = readFileSync(new URL('../src/services/screening.ts', import.meta.url), 'utf8')
  assert.equal(app.match(/path="\/stocks\/screener" element=\{<StockScreenerPage \/>\}/g)?.length, 2)
  assert.doesNotMatch(app + mapped, /RealtimeStockScreeningPage/)
  assert.match(screening, /VITE_REALTIME_ONLY !== 'true'/)
  assert.match(screening, /Realtime screening requires VITE_REALTIME_COHORT_ID/)
  assert.equal(screening.match(/params: screeningParams\(\)/g)?.length, 4)
  for (const route of ['catalog', 'query', 'gap-details', 'hourly-details']) {
    assert.ok(screening.includes(`/stocks/screening/${route}`))
  }
})

test('all four original screening requests preserve bodies and abort signals in both deployment modes', async () => {
  const source = readFileSync(new URL('../src/services/screening.ts', import.meta.url), 'utf8')
    .replace("import api from './api'", 'const api = globalThis.__screeningApi')
    .replaceAll('import.meta.env', 'globalThis.__screeningEnvironment')
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
  const calls = []
  globalThis.__screeningApi = {
    get: async (...args) => { calls.push(['get', ...args]); return { data: 'payload' } },
    post: async (...args) => { calls.push(['post', ...args]); return { data: 'payload' } },
  }
  try {
    const module = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
    const signal = new AbortController().signal
    const request = { predicate: { filters: [] }, offset: 0 }
    for (const realtime of [false, true]) {
      globalThis.__screeningEnvironment = { VITE_REALTIME_ONLY: String(realtime), VITE_REALTIME_COHORT_ID: 'cohort' }
      calls.length = 0
      assert.equal(await module.getScreeningCatalog(signal), 'payload')
      await module.queryScreening(request, signal)
      await module.getGapDetails('generation', 'security', null, signal)
      await module.getHourlyDetails('generation', 'security', signal)
      assert.deepEqual(calls.map(call => call[1]), ['/stocks/screening/catalog', '/stocks/screening/query', '/stocks/screening/gap-details', '/stocks/screening/hourly-details'])
      assert.equal(calls[1][2], request)
      assert.deepEqual(calls[2][2], { generation: 'generation', security_id: 'security', gap: null })
      for (const call of calls) {
        const config = call.at(-1)
        assert.equal(config.signal, signal)
        assert.deepEqual(config.params, realtime ? { cohort_id: 'cohort' } : undefined)
      }
    }
    calls.length = 0
    globalThis.__screeningEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(module.getScreeningCatalog(), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__screeningApi
    delete globalThis.__screeningEnvironment
  }
})

test('canary page exposes exact coverage and isolation evidence', () => {
  for (const field of ['expected_tickers', 'ready_tickers', 'screening_tickers', 'stock_alerts_available',
    'option_alerts_available', 'database_target', 'provider_fetch_performed', 'canonical_fallback', 'reason_codes']) {
    assert.ok(page.includes(`data.${field}`), `missing ${field}`)
  }
  assert.match(page, /No canonical fallback/)
  assert.match(page, /Stored projections only/)
  assert.match(page, /refetchInterval: 15_000/)
  assert.match(page, /Stored cohort values/)
  assert.match(page, /Option evaluations/)
})

test('local portal has one realtime factory backend on port 8001', () => {
  const apiTasks = tasks.tasks.filter(task => task.label.includes('API reader'))

  assert.deepEqual(apiTasks.map(task => task.label), ['Start Realtime API reader'])
  assert.deepEqual(apiTasks[0].args, [
    '-B', '-m', 'uvicorn', 'scripts.run_realtime_portal_api:create_app', '--factory',
    '--app-dir', 'backend', '--env-file', 'backend/.env', '--reload',
    '--host', '127.0.0.1', '--port', '8001',
  ])
  assert.equal(apiTasks[0].options.cwd, '${workspaceFolder}')
  assert.match(vite, /VITE_API_PROXY_TARGET \|\| 'http:\/\/127\.0\.0\.1:8001'/)
})