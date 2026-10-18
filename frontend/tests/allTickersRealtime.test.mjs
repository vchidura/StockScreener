import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8')

test('original All Tickers is routed and navigable in both modes', () => {
  assert.equal(read('../src/App.tsx').match(/path="\/overview" element=\{<TickersOverview \/>\}/g)?.length, 2)
  assert.match(read('../src/layout/navigation.ts'), /to: '\/overview', label: 'All Tickers'/)
  assert.match(read('../src/layout/AppShell.tsx'), /realtimeOnly \? pinnedPath\(baseDestination\) : baseDestination/)
  const page = read('../src/pages/TickersOverview.tsx')
  assert.match(page, /usePublicationQuery\('all-tickers', \(pin, session\) => getAllTickersPublication\(pin, session, true\)\)/)
  assert.match(read('../src/pages/publicationNavigation.ts'), /const interval = name.startsWith\('daily-'\)/)
  assert.match(read('../src/pages/publicationNavigation.ts'), /const key = queryKey\(pin\)/)
  assert.match(read('../src/pages/publicationNavigation.ts'), /internalWorkspace = isTickerWorkspaceQuery\(name\) \|\| name === 'all-tickers'/)
  assert.match(read('../src/pages/publicationNavigation.ts'), /retry: false/)
  assert.match(page, /enabled: !realtimeOnly/)
  assert.match(page, /Retaining the previous publication/)
  assert.match(page, /data\.length > 0 && \(realtimeOnly \|\| \(!loading && !error\)\)/)
  assert.match(page, /disabled=\{realtimeOnly && sectors\.length === 0\}/)
  assert.match(page, /disabled=\{realtimeOnly && opt\.key === 'wsma_200' && !weekly200Available\}/)
  assert.match(page, /Market · selected realtime cohort/)
  assert.match(page, /className="ticker-publication-link"\s+to=\{realtimeOnly && publication/)
  assert.match(page, /publicationPath\(`\/ticker\/\$\{row\.ticker\}`, undefined, publication\.cohort_id, undefined, publication\.session\)/)
  assert.doesNotMatch(page, /workspaceMap/)
  assert.match(page, /aria-label=\{`Open \$\{row\.ticker\} ticker workspace`\}/)
  assert.doesNotMatch(page, /onClick=\{\(\) => navigate/)
  assert.doesNotMatch(page, /publication\?\.scope_note|Split-adjusted daily history; raw retained quotes|Observed <time/)
})

test('All Tickers closing reads use one exact saved bundle and never fall back to Dashboard', async () => {
  const calls = []
  let session = '2026-10-15', corruptRows = false, fail = false
  globalThis.__allClosingEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  globalThis.__allClosingApi = { get: async (url, options) => {
    calls.push([url, options.params])
    if (fail) throw new Error('Saved closing summary unavailable')
    if (url === '/tickers/sessions') return { data: {
      cohort_id: 'exact-cohort', interval: '1d', sessions: ['2026-10-15', '2026-10-16'],
      data_as_of: '2026-10-16T20:00:00Z', data_interval: '1d',
    } }
    assert.equal(url, '/tickers/workspace-publication')
    const publication = { cohort_id: 'exact-cohort', snapshot_id: 'closing-summary', session, read_mode: 'SESSION_CLOSE' }
    return { data: { publication, overview: [{ ticker: 'SPY', date: corruptRows ? '2026-10-16' : session, close: 771.77 }],
      setup: { publication, count: 1, results: [{ ticker: 'SPY', trend: 'stored' }] } } }
  } }
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__allClosingApi }')
    .replaceAll('import.meta.env', 'globalThis.__allClosingEnvironment')
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText
  try {
    const api = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
    const dates = await api.getAllTickersClosingSessions()
    assert.equal(dates.data_as_of, '2026-10-16T20:00:00Z')
    calls.length = 0
    const result = await api.getAllTickersPublication(undefined, '2026-10-15', true)
    assert.equal(result.overview[0].date, '2026-10-15')
    assert.deepEqual(calls, [['/tickers/workspace-publication',
      { cohort_id: 'exact-cohort', session_date: '2026-10-15', read_mode: 'session_close' }]])
    session = '2026-10-16'
    await assert.rejects(api.getAllTickersPublication(undefined, '2026-10-15', true), /selected session/)
    session = '2026-10-15'
    corruptRows = true
    await assert.rejects(api.getAllTickersPublication(undefined, '2026-10-15', true), /selected session/)
    corruptRows = false
    fail = true
    calls.length = 0
    await assert.rejects(api.getAllTickersPublication(undefined, '2026-10-15', true), /Saved closing summary unavailable/)
    assert.equal(calls.length, 1)
    assert.equal(calls[0][0], '/tickers/workspace-publication')
  } finally {
    delete globalThis.__allClosingApi
    delete globalThis.__allClosingEnvironment
  }
})

test('200-week columns stay visible and selectable even when published values are unavailable', () => {
  const page = read('../src/pages/TickersOverview.tsx')
  const hidden = page.match(/const DEFAULT_HIDDEN_COLUMNS = \[([\s\S]*?)\]/)?.[1]
  assert.ok(hidden)
  assert.doesNotMatch(hidden, /wsma_200|dist_200w/)
  assert.match(page, /new Set\(DEFAULT_HIDDEN_COLUMNS\)/)
  assert.doesNotMatch(page, /disabled=\{realtimeOnly && \(c\.key === 'wsma_200'/)
  assert.match(page, /weekly200Available = data\.some\(row => row\.wsma_200 != null\)/)
  assert.match(page, /200 complete weekly bars are required[^<]*">N\/A<\/span>/)
  assert.match(page, /No published distance from the 200-week average is available\.\">N\/A<\/span>/)
})

test('canonical summary intervals remain unchanged and realtime reads pin the cohort bundle atomically', async () => {
  const calls = []
  const publication = { snapshot_id: 'original-publication', session: '2026-10-02' }
  const workspacePublication = { snapshot_id: 'ticker-workspace-publication', session: '2026-10-02' }
  const workspaceBundle = {
    overview: [{ ticker: 'MSFT' }],
    setup: { results: [{ ticker: 'MSFT', trend: 'Bearish Stack' }], publication: workspacePublication },
    publication: workspacePublication,
  }
  let summaryPublication = publication
  let failure = false
  globalThis.__allTickersApi = {
    get: async (url, options) => {
      calls.push([url, options.params])
      if (url === '/latest-price-date') return { data: { latest_date: publication.session, publication } }
      if (url === '/tickers/overview') return { data: [{ ticker: 'AAPL' }] }
      if (url === '/tickers/workspace-publication') return { data: workspaceBundle }
      if (failure) throw new Error('Stored summary unavailable')
      return { data: { results: [{ ticker: 'AAPL', trend: 'Bullish Stack' }], publication: summaryPublication } }
    },
  }
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__allTickersApi }')
    .replaceAll('import.meta.env', 'globalThis.__allTickersEnvironment')
  const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText
  try {
    globalThis.__allTickersEnvironment = {}
    const api = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
    for (const realtime of [false, true]) {
      calls.length = 0
      globalThis.__allTickersEnvironment = { VITE_REALTIME_ONLY: String(realtime), VITE_REALTIME_COHORT_ID: ' exact-cohort ' }
      for (const interval of ['30m', '1h', '1d', '1wk', '1mo']) await api.getSetupSummary(interval, 'pinned')
      calls.forEach(([url, params], index) => {
        assert.equal(url, '/tickers/setup-summary')
        assert.deepEqual(params, {
          interval: ['30m', '1h', '1d', '1wk', '1mo'][index],
          ...(realtime ? { cohort_id: 'exact-cohort', snapshot_id: 'pinned' } : {}),
        })
      })
    }
    calls.length = 0
    const result = await api.getAllTickersPublication()
    assert.deepEqual(result.overview, [{ ticker: 'AAPL' }])
    assert.deepEqual(result.publication, publication)
    assert.deepEqual(calls, [
      ['/latest-price-date', { cohort_id: 'exact-cohort' }],
      ['/tickers/overview', { cohort_id: 'exact-cohort', snapshot_id: 'original-publication' }],
      ['/tickers/setup-summary', { cohort_id: 'exact-cohort', snapshot_id: 'original-publication', interval: '1d' }],
    ])
    summaryPublication = { snapshot_id: 'rotated-publication' }
    assert.deepEqual(await api.getAllTickersPublication(), workspaceBundle)
    summaryPublication = undefined
    assert.deepEqual(await api.getAllTickersPublication(), workspaceBundle)
    failure = true
    assert.deepEqual(await api.getAllTickersPublication(), workspaceBundle)
    calls.length = 0
    assert.deepEqual(await api.getAllTickersPublication('ticker-workspace-publication'), workspaceBundle)
    assert.ok(calls.some(([url, params]) => url === '/tickers/workspace-publication' && params.snapshot_id === 'ticker-workspace-publication'))
    await assert.rejects(api.getAllTickersPublication('another-summary'), /requested release/)
    assert.ok(calls.some(([url]) => url === '/tickers/workspace-publication'))
    calls.length = 0
    globalThis.__allTickersEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(api.getSetupSummary(), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(api.getAllTickersPublication(), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__allTickersApi
    delete globalThis.__allTickersEnvironment
  }
})
