import assert from 'node:assert/strict'
import { existsSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import Module from 'node:module'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import test from 'node:test'
import ts from 'typescript'
import React from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import * as ReactQuery from '@tanstack/react-query'
import * as Router from 'react-router-dom'

const read = file => readFileSync(new URL(file, import.meta.url), 'utf8')
const require = createRequire(import.meta.url)
globalThis.__dailyEnvironment = {}
const root = fileURLToPath(new URL('../src/', import.meta.url))
const basis = {
  price: 'PROVIDER_SPLIT_ADJUSTED_OHLC', volume: 'PROVIDER_ADJUSTED_SERIES_VOLUME',
  raw_volume_parity: 'UNVERIFIED', weekly: 'DAILY_RESAMPLED_W_FRI_INCLUDES_PARTIAL_CURRENT_WEEK',
  usage: 'RESEARCH_ONLY_HEURISTIC_GRADES_NOT_PROBABILITIES_OR_RECOMMENDATIONS',
}
const publication = id => ({
  cohort_id: 'exact-cohort', strategy_ticker_available: true,
  release_type: 'DAILY_STRATEGIES_V1', payload_sha256: `hash-${id}`,
  staleness_sessions: 0, max_serveable_stale_sessions: 5,
  snapshot_id: id, status: 'READY', session: '2026-10-02', market_time: '2026-10-02T20:00:00Z',
  generated_at: id === 'old' ? '2026-10-02T20:20:00Z' : '2026-10-02T20:21:00Z',
  read_at: '2026-10-02T20:25:00Z', expected_market_time: '2026-10-02T20:00:00Z',
  scope_note: 'Selected realtime cohort: 9 tickers; not full-market or sector coverage.',
  price_basis: 'PROVIDER_SPLIT_ADJUSTED_HISTORY_PINNED_CURRENT_RAW_QUOTE', missing_producers: {},
})
const envelope = pub => ({
  publication: pub, scanner_basis: basis, scan_datetime: pub.generated_at, interval: '1d',
  total_scanned: 9, total_signals: 0, results: [], results_by_type: {}, results_by_signal: {},
  short_period: 9, long_period: 21, lookback: 50, min_swing_pct: 5,
  analysis: { status: 'COMPLETE_EMPTY', total_evaluated: 9, total_unavailable: 0,
    members: ['AAPL', 'MSFT', 'AMZN', 'IWM', 'NFLX', 'NVDA', 'QQQ', 'SPY', 'TSLA'].map(ticker => ({
      ticker, status: 'COMPLETE_EMPTY', available_bars: 283, required_bars: 210, total_signals: 0, reason: null,
    })) },
})
const routes = ['gaps', 'fvg', 'ma-crossover', 'momentum-pullback', 'bearish-bounce', 'fibonacci']
const pages = ['GapScreener', 'MAScreener', 'MomentumPullback', 'BearishBounce', 'FibonacciScreener']
const kinds = ['gaps', 'ma-crossover', 'momentum-pullback', 'bearish-bounce', 'fibonacci']
const evaluationContracts = ['SCAN_GAPS_1D', 'SCAN_FVG_1D_50', 'SCAN_MA_1D_9_21', 'SCAN_MOMENTUM_1D', 'SCAN_BEARISH_1D', 'SCAN_FIBONACCI_1D_5']

function evaluationBundle(pub) {
  const members = envelope(pub).analysis.members.map(member => member.ticker)
  const required = { SCAN_GAPS_1D: 20, SCAN_FVG_1D_50: 50, SCAN_MA_1D_9_21: 26,
    SCAN_MOMENTUM_1D: 210, SCAN_BEARISH_1D: 210, SCAN_FIBONACCI_1D_5: 50 }
  return { publication: pub, scanner_basis: basis, members, observed_sessions: [pub.session], recurrence_available: false,
    scanners: Object.fromEntries(evaluationContracts.map(key => [key, envelope(pub)])),
    analysis: Object.fromEntries(evaluationContracts.map(key => [key, { ...envelope(pub).analysis,
      members: envelope(pub).analysis.members.map(member => ({ ...member, required_bars: required[key] })) }])),
    evidence_refs: Object.fromEntries(members.map(ticker => [ticker, Object.fromEntries(evaluationContracts.map(key => [key, {
      cohort_id: pub.cohort_id, evidence_id: `${ticker}:${key}`, evidence_type: 'SCANNER_RESULT', market_time: pub.market_time,
      payload_sha256: 'immutable-hash',
    }]))])) }
}

function compiler(source, filename, overrides = {}) {
  const compiled = ts.transpileModule(source.replaceAll('import.meta.env', 'globalThis.__dailyEnvironment'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
  }).outputText
  const module = new Module(filename)
  module.filename = filename
  module.paths = Module._nodeModulePaths(path.dirname(filename))
  module.require = specifier => Object.hasOwn(overrides, specifier) ? overrides[specifier] : require(specifier)
  module._compile(compiled, filename)
  return module.exports
}

test('Strategy Overview is discoverable and shares exact pins with strategies and research, not the ticker workspace', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const navigation = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'))
  const layout = compiler(read('../src/layout/navigation.ts'), path.join(root, 'layout', 'navigation.ts'), {
    '../auth/accountState': { ACCOUNT_UNAVAILABLE_REASON: 'Unavailable' },
  })
  const strategies = layout.NAV_GROUPS.find(group => group.id === 'strategies')
  assert.equal(strategies.items[0].to, '/strategies/overview')
  assert.deepEqual(layout.breadcrumbFor('/strategies/overview'), ['Strategies', 'Overview'])
  assert.deepEqual(layout.breadcrumbFor('/strategy-research/MSFT'), ['Strategies', 'Overview', 'MSFT'])
  assert.deepEqual(layout.breadcrumbFor('/ticker/MSFT'), ['Stocks', 'MSFT'])
  for (const target of ['/gaps', '/ma-crossover', '/momentum-pullback', '/bearish-bounce', '/fibonacci', '/strategy-research/MSFT', '/stocks/persistence']) {
    assert.equal(navigation.navigationPublicationPath(target, '/strategies/overview', 'old', 'exact-cohort'),
      `${target}?cohort_id=exact-cohort&snapshot_id=old`)
    assert.equal(navigation.navigationPublicationPath('/strategies/overview', target, 'old', 'exact-cohort'),
      '/strategies/overview?cohort_id=exact-cohort&snapshot_id=old')
  }
  for (const target of ['/ticker/MSFT', '/overview', '/stocks/screener']) {
    assert.equal(navigation.navigationPublicationPath(target, '/strategies/overview', 'old', 'exact-cohort'),
      `${target}?cohort_id=exact-cohort`)
  }
  assert.match(read('../src/App.tsx'), /path="\/strategies\/overview" element={<StrategyOverviewPage \/>}/)
  assert.match(read('../src/pages/DailyStrategyTicker.tsx'), /publicationPath\('\/strategies\/overview', pin, cohort, interval, evaluationSession\)/)
})

test('hourly navigation retains compatible pins and interval selection clears only incompatible identity', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  let params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=old&ticker_filter=MSFT&strategy_filter=SCAN_FIBONACCI_1D_5')
  const navigation = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'), {
    'react-router-dom': { useSearchParams: () => [params, update => { params = update(params) }] },
  })
  navigation.useStrategyInterval().setInterval('1h')
  assert.equal(params.get('snapshot_id'), null)
  assert.equal(params.get('cohort_id'), 'exact-cohort')
  assert.equal(params.get('ticker_filter'), 'MSFT')
  assert.equal(params.get('strategy_filter'), 'SCAN_FIBONACCI_1H_ATR')
  assert.equal(navigation.navigationPublicationPath('/gaps', '/strategies/overview', 'hour-old', 'exact-cohort', '1h'),
    '/gaps?cohort_id=exact-cohort&snapshot_id=hour-old&interval=1h')
  assert.equal(navigation.navigationPublicationPath('/ticker/MSFT', '/strategies/overview', 'hour-old', 'exact-cohort', '1h'),
    '/ticker/MSFT?cohort_id=exact-cohort')
  navigation.useStrategyInterval().setInterval('1d')
  assert.equal(params.get('interval'), null)
  assert.equal(params.get('strategy_filter'), 'SCAN_FIBONACCI_1D_5')
})

function mockApi() {
  const calls = []
  let current = 'old', failRoute, mismatch, absent, missingBasis, research, legacy, actionReview, evaluationFault
  const api = compiler(read('../src/services/api.ts'), path.join(root, 'services', 'api.ts'), {
    axios: { default: { create: () => ({
      get: async (url, { params } = {}) => {
        calls.push([url, params])
        if (url === failRoute) throw new Error('Stored scanner unavailable')
        const id = params?.snapshot_id ?? current
        if (!['old', 'new'].includes(id)) throw new Error('Explicit pin unavailable')
        const pub = { ...publication(id), strategy_ticker_available: !legacy }
        if (url === '/strategies/daily') {
          if (params.view === 'evaluations') {
            const result = evaluationBundle(pub)
            if (mismatch) result.publication = publication('new')
            if (absent) result.publication = undefined
            if (missingBasis) result.scanner_basis = undefined
            evaluationFault?.(result)
            return { data: result }
          }

          if (!params.strategy && !params.ticker) return { data: { publication: pub } }
          const selectedRoutes = params.ticker ? routes : params.strategy === 'gaps' ? ['gaps', 'fvg'] : [params.strategy]
          if (selectedRoutes.some(route => `/scan/${route}` === failRoute)
            || params.ticker && ['/tickers/overview', '/tickers/setup-summary'].includes(failRoute)) throw new Error('Stored scanner unavailable')
          const actual = absent ? undefined : mismatch ? publication('new') : pub
          const scanner = () => ({ ...envelope(pub), publication: actual, scanner_basis: missingBasis ? undefined : basis })
          if (params.ticker) {
            if (!['AAPL', 'MSFT'].includes(params.ticker)) throw new Error('Daily research membership does not match')
            const names = ['gaps', 'fvg', 'ma', 'momentum', 'bearish', 'fibonacci']
            const result = { publication: actual, ticker: params.ticker, cohort_count: 2,
              scanner_basis: missingBasis ? undefined : basis, overview: { ticker: params.ticker },
              setup: { ticker: params.ticker, signals: [] }, evidence_refs: Object.fromEntries([
                'SCAN_GAPS_1D', 'SCAN_FVG_1D_50', 'SCAN_MA_1D_9_21', 'SCAN_MOMENTUM_1D', 'SCAN_BEARISH_1D', 'SCAN_FIBONACCI_1D_5',
              ].map(contract => [contract, { cohort_id: pub.cohort_id, evidence_id: contract, evidence_type: 'SCANNER_RESULT',
                market_time: pub.market_time, payload_sha256: 'immutable-hash' }])),
              analysis: Object.fromEntries(names.map(name => [name, { ...envelope(pub).analysis.members[0], ticker: params.ticker,
                status: 'COMPLETE', total_signals: 1 }])),
              scans: Object.fromEntries(names.map(name => [name, [{ ticker: params.ticker }]])) }
            if (actionReview) {
              result.evidence_refs = {}
              result.overview = null
              result.setup = null
              result.analysis = Object.fromEntries(names.map(name => [name, { ticker: params.ticker, status: 'UNAVAILABLE',
                available_bars: 283, required_bars: 210, total_signals: 0, reason: 'CORPORATE_ACTION_REVIEW' }]))
              result.scans = Object.fromEntries(names.map(name => [name, []]))
            }
            return { data: result }
          }
          return { data: { publication: actual, data: scanner(), ...(params.strategy === 'gaps' ? { fvgData: scanner() } : {}) } }
        }
        if (url === '/latest-price-date') return { data: { latest_date: pub.session, publication: pub } }
        if (research && url === '/tickers/overview') return { data: ['AAPL', 'MSFT'].map(ticker => ({ ticker, close: 100, volume: 1000 })) }
        const base = { ...envelope(pub), ...(research ? { total_scanned: 2, total_signals: 2,
          results: [{ ticker: 'AAPL' }, { ticker: 'MSFT' }] } : {}) }
        if (research && url === '/tickers/setup-summary') Object.assign(base, {
          count: 2, results: ['AAPL', 'MSFT'].map(ticker => ({ ticker, signals: [] })),
          source: 'MATERIALIZED_CURRENT_PROJECTION', computed_at: pub.generated_at, market_time: pub.market_time,
        })
        return { data: { ...base, publication: absent ? undefined : mismatch ? publication('new') : pub,
          scanner_basis: missingBasis ? undefined : basis } }
      },
    }) } },
  })
  return { api, calls, rotate: () => { current = 'new' },
    fail: route => { failRoute = route }, mismatch: value => { mismatch = value },
    absent: value => { absent = value }, missingBasis: value => { missingBasis = value },
    research: () => { research = true }, legacy: () => { legacy = true }, actionReview: () => { actionReview = true },
    evaluationFault: fault => { evaluationFault = fault } }
}

test('historical navigation carries dates only within compatible strategy or ticker views', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const navigation = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'))
  for (const [target, source, expected] of [
    ['/fibonacci', '/gaps', '/fibonacci?cohort_id=exact-cohort&snapshot_id=archive&interval=1h&session_date=2026-10-12'],
    ['/ticker/SPY', '/gaps', '/ticker/SPY?cohort_id=exact-cohort'],
    ['/ticker/SPY/levels', '/ticker/SPY', '/ticker/SPY/levels?cohort_id=exact-cohort&session_date=2026-10-12'],
    ['/ticker/MSFT', '/ticker/SPY', '/ticker/MSFT?cohort_id=exact-cohort&session_date=2026-10-12'],
    ['/ticker/SPY', '/overview', '/ticker/SPY?cohort_id=exact-cohort&session_date=2026-10-12'],
    ['/overview', '/ticker/SPY', '/overview?cohort_id=exact-cohort&session_date=2026-10-12'],
    ['/gaps', '/ticker/SPY', '/gaps?cohort_id=exact-cohort'],
  ]) assert.equal(navigation.navigationPublicationPath(target, source, 'archive', 'exact-cohort', '1h', '2026-10-12'), expected)
  assert.equal(navigation.publicationPath('/strategy-research/SPY', 'archive', 'exact-cohort', '1h', '2026-10-12'),
    '/strategy-research/SPY?cohort_id=exact-cohort&snapshot_id=archive&interval=1h&evaluation_session=2026-10-12')
})

test('strategy, ticker and Pattern Watch selectors use closing dates and a static market timestamp', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  let params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=archive&interval=1h&ticker_filter=SPY')
  let pathname = '/gaps'
  const sessions = ['2026-10-12', '2026-10-13', '2026-10-14', '2026-10-15', '2026-10-16']
  const navigation = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'))
  const { SessionDateProvider } = compiler(read('../src/layout/sessionDate.tsx'), path.join(root, 'layout', 'sessionDate.tsx'), {
    react: { ...React, useState: value => [value, () => {}], useMemo: callback => callback(),
      useCallback: callback => callback, useEffect: () => {} },
    'react-router-dom': { useLocation: () => ({ pathname }), useSearchParams: () => [params, update => { params = update(params) }] },
    '@tanstack/react-query': { useQuery: () => ({ data: { sessions, data_as_of: '2026-10-16T20:00:00Z', data_interval: '1d' }, error: null }) },
    '../services/api': {},
    '../pages/publicationNavigation': navigation,
    './navigation': { activeTickerSymbol: path => /^\/ticker\/([^/]+)/.exec(path)?.[1] ?? null },
  })
  const store = () => SessionDateProvider({ children: null }).props.value
  store().step(-1)
  assert.equal(params.get('session_date'), '2026-10-15')
  assert.equal(params.has('snapshot_id'), false)
  store().choose('2026-10-12')
  params.set('interval', '1d')
  assert.equal(store().date, '2026-10-12')
  assert.equal(store().canStepBack, false)
  store().reset()
  assert.equal(store().isLatest, true)
  pathname = '/ticker/SPY'
  assert.equal(store().date, '2026-10-16')
  store().step(-1)
  assert.equal(params.get('session_date'), '2026-10-15')
  assert.equal(store().isLatest, false)
  assert.equal(store().dataAsOf, '2026-10-16T20:00:00Z')
  store().step(1)
  assert.equal(params.has('session_date'), false)
  assert.equal(store().canStepForward, false)
  pathname = '/pattern-watch'
  assert.equal(store().isPattern, true)
  assert.equal(store().date, '2026-10-16')
  store().choose('2026-10-12')
  assert.equal(store().date, '2026-10-12')
  assert.equal(store().dataAsOf, '2026-10-16T20:00:00Z')
  store().reset()
  assert.equal(params.has('session_date'), false)
  pathname = '/overview'
  assert.equal(store().isAllTickers, true)
  assert.equal(store().date, '2026-10-16')
  store().step(-1)
  assert.equal(params.get('session_date'), '2026-10-15')
  assert.equal(store().dataAsOf, '2026-10-16T20:00:00Z')
  store().reset()
  pathname = '/strategy-research/SPY'
  params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=archive&evaluation_session=2026-10-14')
  store().step(-1)
  assert.equal(params.get('snapshot_id'), 'archive')
  assert.equal(params.get('evaluation_session'), '2026-10-13')
})

test('selected ticker dates isolate publication cache and refresh without an inherited foreign pin', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const client = new QueryClient()
  let params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=foreign&session_date=2026-10-15'), options
  const calls = []
  const hooks = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'), {
    react: { useState: value => [value, () => {}], useRef: value => ({ current: value }), useEffect: () => {} },
    'react-router-dom': { useSearchParams: () => [params, update => { params = update(params) }] },
    '@tanstack/react-query': { useQueryClient: () => client, useQuery: spec => {
      options = spec
      return { data: client.getQueryData(spec.queryKey), isFetching: false, error: null }
    } },
  })
  const selected = hooks.usePublicationQuery('ticker-workspace:SPY', async (pin, session) => {
    calls.push([pin, session])
    return { publication: { ...publication('archive'), session } }
  })
  assert.deepEqual(options.queryKey, ['ticker-workspace:SPY', 'exact-cohort', undefined, '2026-10-15'])
  await options.queryFn()
  await selected.refresh()
  assert.deepEqual(calls, [[undefined, '2026-10-15'], [undefined, '2026-10-15']])
  assert.equal(params.get('session_date'), '2026-10-15')
  params.set('session_date', '2026-10-14')
  hooks.usePublicationQuery('ticker-workspace:SPY', async () => ({ publication: publication('latest') }))
  await assert.rejects(options.queryFn(), /selected trading session; no substitution/)
  client.clear()
})

test('Persistence loads one exact daily evaluation bundle without legacy streak or unrelated overview requests', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const mock = mockApi()
  mock.rotate()
  const result = await mock.api.getDailyStrategyEvaluations('old')
  assert.equal(result.publication.snapshot_id, 'old')
  assert.deepEqual(result.observed_sessions, ['2026-10-02'])
  assert.equal(result.recurrence_available, false)
  assert.equal(result.members.length, 9)
  assert.deepEqual(mock.calls, [
    ['/strategies/daily', { cohort_id: 'exact-cohort', snapshot_id: 'old', view: 'evaluations', read_mode: 'retained' }],
  ])
  for (const fault of [
    result => { result.scanners.SCAN_FVG_1D_50.total_signals++ },
    result => { result.analysis.SCAN_MOMENTUM_1D.total_evaluated = 0 },
    result => { result.members.push('AAPL') },
    result => { result.recurrence_available = true },
    result => { result.evidence_refs.AAPL.SCAN_GAPS_1D.cohort_id = 'wrong' },
    result => { result.scanners.SCAN_FVG_1D_50.lookback = 20 },
    result => { result.analysis.SCAN_GAPS_1D.members[0].status = 'COMPLETE'; delete result.evidence_refs.AAPL.SCAN_GAPS_1D },
    result => { result.analysis.SCAN_GAPS_1D.members[0].status = 'invented'; result.analysis.SCAN_GAPS_1D.total_evaluated--; result.analysis.SCAN_GAPS_1D.total_unavailable++ },
    result => { result.analysis.SCAN_FVG_1D_50.members[0].available_bars = 20 },
  ]) {
    mock.evaluationFault(fault)
    await assert.rejects(mock.api.getDailyStrategyEvaluations('old'), /Stored/)
  }
  mock.evaluationFault(undefined)
  mock.fail('/strategies/daily')
  await assert.rejects(mock.api.getDailyStrategyEvaluations('old'), /Stored scanner unavailable/)
})

test('six service contracts preserve canonical arguments and add realtime cohort/snapshot only', async () => {
  const { api, calls } = mockApi()
  const functions = [
    () => api.scanGaps('AAPL', '2026-09-01', '1h', true, 'old'),
    () => api.scanFVG('AAPL', '2026-09-01', '1h', 20, true, 'old'),
    () => api.scanMACrossover('AAPL', 5, 50, '2026-09-01', '1h', true, 'old'),
    () => api.scanMomentumPullback('AAPL', '2026-09-01', '1h', true, 'old'),
    () => api.scanBearishBounce('AAPL', '2026-09-01', '1h', true, 'old'),
    () => api.scanFibonacci('AAPL', '2026-09-01', 8, '1h', true, 'old'),
  ]
  const defaults = [{}, { lookback: 20 }, { short_period: 5, long_period: 50 }, {}, {}, { min_swing_pct: 8 }]
  for (const realtime of [false, true]) {
    globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: String(realtime), VITE_REALTIME_COHORT_ID: ' exact-cohort ' }
    calls.length = 0
    for (const fn of functions) await fn()
    assert.deepEqual(calls.map(call => call[0]), routes.map(route => `/scan/${route}`))
    calls.forEach(([, params], index) => assert.deepEqual(params, {
      interval: '1h', tickers: 'AAPL', scan_date: '2026-09-01', refresh: true, ...defaults[index],
      ...(realtime ? { cohort_id: 'exact-cohort', snapshot_id: 'old' } : {}),
    }))
  }
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true' }
  calls.length = 0
  for (const fn of functions) await assert.rejects(fn(), /requires VITE_REALTIME_COHORT_ID/)
  assert.equal(calls.length, 0)
})

test('atomic default page reads pin metadata and every scanner, including FVG, across rotation', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const mock = mockApi()
  for (const kind of kinds) {
    mock.calls.length = 0
    const result = await mock.api.getDailyScannerPublication(kind, 'old')
    assert.deepEqual(result.publication, publication('old'))
    assert.ok(result.data)
    if (kind === 'gaps') assert.ok(result.fvgData)
    assert.equal(mock.calls.length, 1)
    for (const [, params] of mock.calls) {
      assert.equal(params.cohort_id, 'exact-cohort')
      assert.equal(params.snapshot_id, 'old')
      assert.equal(params.strategy, kind)
      assert.equal(params.read_mode, 'retained')
      assert.equal(params.scan_date, undefined)
      assert.equal(params.tickers, undefined)
    }
  }
  mock.rotate()
  assert.equal((await mock.api.getDailyScannerPublication('gaps', 'old')).publication.snapshot_id, 'old')
  assert.equal((await mock.api.getDailyScannerPublication('gaps')).publication.snapshot_id, 'new')
  mock.calls.length = 0
  await assert.rejects(mock.api.getDailyScannerPublication('gaps', 'missing'), /Explicit pin unavailable/)
  assert.equal(mock.calls.length, 1)
  mock.fail('/scan/fvg')
  await assert.rejects(mock.api.getDailyScannerPublication('gaps', 'old'), /Stored scanner unavailable/)
  mock.fail(undefined)
  mock.mismatch(true)
  await assert.rejects(mock.api.getDailyScannerPublication('gaps', 'old'), /does not match/)
  mock.mismatch(false)
  mock.absent(true)
  await assert.rejects(mock.api.getDailyScannerPublication('gaps', 'old'), /does not match/)
  mock.absent(false)
  mock.missingBasis(true)
  await assert.rejects(mock.api.getDailyScannerPublication('gaps', 'old'), /research basis/)
})

test('shared query executes atomic commit only on success and retains old data/clocks on refresh failure', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const mock = mockApi()
  const client = new QueryClient()
  const original = await mock.api.getDailyScannerPublication('gaps', 'old')
  client.setQueryData(['daily-gaps', 'exact-cohort', 'old'], original)
  let params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=old'), cursor = 0, options
  const states = []
  const hooks = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'), {
    react: {
      useEffect: () => {},
      useRef: value => {
        const index = cursor++
        states[index] ??= { current: value }
        return states[index]
      },
      useState: initial => {
        const index = cursor++
        if (!(index in states)) states[index] = initial
        return [states[index], value => { states[index] = value }]
      },
    },
    'react-router-dom': { useSearchParams: () => [params, update => { params = update(params) }] },
    '@tanstack/react-query': {
      useQueryClient: () => client,
      useQuery: spec => {
        options = spec
        return { data: client.getQueryData(spec.queryKey), error: null, isFetching: false }
      },
    },
  })
  const render = () => {
    cursor = 0
    return hooks.usePublicationQuery('daily-gaps', pin => mock.api.getDailyScannerPublication('gaps', pin))
  }
  let view = render()
  assert.deepEqual(options.queryKey, ['daily-gaps', 'exact-cohort', 'old'])
  assert.equal(options.retry, false)
  assert.equal(options.refetchOnWindowFocus, false)
  assert.equal(view.data, original)
  mock.rotate()
  mock.fail('/scan/fvg')
  await view.refresh()
  view = render()
  assert.match(view.error, /Stored scanner unavailable/)
  assert.equal(view.data, original)
  assert.equal(view.data.publication.generated_at, publication('old').generated_at)
  assert.equal(view.data.publication.read_at, publication('old').read_at)
  assert.equal(params.get('snapshot_id'), 'old')
  assert.equal(client.getQueryData(['daily-gaps', 'exact-cohort', 'new']), undefined)
  mock.fail(undefined)
  await view.refresh()
  view = render()
  assert.equal(view.error, undefined)
  assert.equal(view.data.publication.snapshot_id, 'new')
  assert.equal(view.data.fvgData.publication.snapshot_id, 'new')
  assert.equal(params.get('snapshot_id'), 'new')
  mock.calls.length = 0
  params = new URLSearchParams('cohort_id=wrong&snapshot_id=old')
  view = render()
  assert.match(view.error, /no substitution/)
  assert.throws(() => options.queryFn(), /no substitution/)
  await view.refresh()
  assert.equal(mock.calls.length, 0)
  params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=')
  render()
  assert.throws(() => options.queryFn(), /identity is empty/)
  assert.equal(hooks.publicationPath('/gaps', 'old'), '/gaps?cohort_id=exact-cohort&snapshot_id=old')
  assert.equal(hooks.publicationPath('/gaps', 'old', 'wrong'), '/gaps?cohort_id=wrong&snapshot_id=old')
  assert.equal(hooks.publicationPath('/gaps', undefined, 'wrong'), '/gaps?cohort_id=wrong')
  client.clear()
})

function createPageRenderer() {
  const cache = new Map()
  const api = mockApi().api
  function load(file) {
    if (cache.has(file)) return cache.get(file)
    const source = readFileSync(file, 'utf8')
    const mod = new Module(file)
    mod.filename = file
    mod.paths = Module._nodeModulePaths(path.dirname(file))
    mod.require = specifier => {
      if (specifier === '@tanstack/react-query') return ReactQuery
      if (specifier === 'react-router-dom') return Router
      if (specifier.endsWith('.css')) return {}
      if (specifier.endsWith('/pageContext')) return { usePublishPageContext: () => {} }
      if (specifier.endsWith('/sessionDate')) return { useSessionDate: () => ({ pinned: '' }) }
      if (specifier.endsWith('/api')) return api
      if (specifier.startsWith('.')) {
        const stem = path.resolve(path.dirname(file), specifier)
        for (const ext of ['.ts', '.tsx']) {
          try { readFileSync(stem + ext); return load(stem + ext) } catch (error) {
            if (error.code !== 'ENOENT') throw error
          }
        }
        throw new Error(`Missing module ${specifier}`)
      }
      return require(specifier)
    }
    mod._compile(ts.transpileModule(source.replaceAll('import.meta.env', 'globalThis.__dailyEnvironment'), {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
    }).outputText, file)
    cache.set(file, mod.exports)
    return mod.exports
  }
  return (name, result, cacheRealtimePublication = true, search = '') => {
    const Page = load(path.join(root, 'pages', `${name}.tsx`)).default
    const client = new QueryClient()
    const research = name === 'DailyStrategyTicker'
    const persistence = name === 'research/RealtimePersistenceView' || name === 'StrategyOverviewPage'
    const sessionPersistence = name === 'research/RealtimeSessionPersistenceView'
    const kind = persistence ? 'persistence' : sessionPersistence ? 'session-persistence'
      : research ? `strategy-ticker:${result.ticker}${result.evaluation_session ? `:${result.evaluation_session}` : ''}` : kinds[pages.indexOf(name)]
    const cohort = globalThis.__dailyEnvironment.VITE_REALTIME_COHORT_ID
    const pin = result.publication.snapshot_id
    const interval = result.publication.interval ?? '1d'
    if (cacheRealtimePublication) client.setQueryData([`daily-${kind}`, cohort, pin, ...(interval === '1h' ? ['1h'] : [])], result)
    const canonicalKeys = {
      gaps: ['scan', 'gaps', '1d', ''],
      'ma-crossover': ['scan', 'ma-crossover', '1d', '', 9, 21],
      'momentum-pullback': ['scan', 'momentum-pullback', '1d', ''],
      'bearish-bounce': ['scan', 'bearish-bounce', '1d', ''],
      fibonacci: ['scan', 'fibonacci', '1d', '', 5],
    }
    if (!research && !persistence && !sessionPersistence) client.setQueryData(canonicalKeys[kind], result.data)
    if (result.fvgData) client.setQueryData(['scan', 'fvg', '1d', ''], result.fvgData)
    const html = renderToStaticMarkup(React.createElement(QueryClientProvider, { client },
      React.createElement(MemoryRouter, { initialEntries: [`/${persistence ? 'strategies/overview' : sessionPersistence ? 'stocks/persistence' : research ? `strategy-research/${result.ticker}` : kind}?cohort_id=${cohort}&snapshot_id=${pin}${interval === '1h' ? '&interval=1h' : ''}${result.evaluation_session ? `&evaluation_session=${result.evaluation_session}` : ''}${search ? `&${search}` : ''}`] },
        research ? React.createElement(Router.Routes, null, React.createElement(Router.Route, {
          path: '/strategy-research/:symbol', element: React.createElement(Page),
        })) : React.createElement(Page))))
    client.clear()
    return html
  }
}

const offlineFixturePath = new URL('../../backend/.realtime-canary/strategy-offline-fixture.json', import.meta.url)

test('serialized offline hourly releases satisfy all five services, Overview, Persistence and exact-session research', {
  skip: !existsSync(offlineFixturePath),
}, async () => {
  const { releases } = JSON.parse(readFileSync(offlineFixturePath, 'utf8'))
  const calls = []
  const cohort = releases.daily.latest.publication.cohort_id
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: cohort }
  const api = compiler(read('../src/services/api.ts'), path.join(root, 'services', 'api.ts'), {
    axios: { default: { create: () => ({ get: async (url, { params }) => {
      calls.push([url, params])
      assert.equal(url, '/strategies/daily')
      const release = Object.values(releases).find(item => item.latest.publication.snapshot_id === params.snapshot_id)
      if (!release || (release.latest.publication.interval ?? '1d') !== (params.interval ?? '1d')) throw new Error('Exact interval/pin unavailable')
      if (params.view) return { data: release[params.view] }
      if (params.strategy) return { data: release.strategies[params.strategy] }
      if (params.ticker) return { data: params.evaluation_session ? release.research_session : release.research }
      return { data: release.latest }
    } }) } },
  })
  const pin = releases.hourly_history.latest.publication.snapshot_id
  for (const kind of kinds) assert.equal((await api.getDailyScannerPublication(kind, pin, '1h')).data.interval, '1h')
  const overview = await api.getDailyStrategyEvaluations(pin, '1h')
  assert.equal(overview.analysis.SCAN_MOMENTUM_1H.members[0].required_bars, 200)
  const persistence = await api.getStrategyPersistence(pin, '1h')
  assert.equal(persistence.persistence.sessions.length, 5)
  assert.equal(persistence.persistence.window_unit, 'XNYS_TRADING_SESSIONS')
  const session = persistence.evaluation_sessions[0]
  const research = await api.getDailyStrategyTickerPublication('AAPL', pin, '1h', session)
  assert.equal(research.evaluation_session, session)
  assert.ok(research.evaluation_market_time.startsWith(session))
  await assert.rejects(api.getDailyStrategyEvaluations(releases.daily.latest.publication.snapshot_id, '1h'), /interval\/pin/)
  assert.ok(calls.every(([url]) => url === '/strategies/daily'))
})

test('real serialized hourly views retain research pins, native metrics, five-session cells and trajectories', {
  skip: !existsSync(offlineFixturePath),
}, () => {
  const { releases } = JSON.parse(readFileSync(offlineFixturePath, 'utf8'))
  const fixture = releases.hourly_history
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: fixture.latest.publication.cohort_id }
  const render = createPageRenderer()
  for (const [index, name] of pages.entries()) {
    const html = render(name, fixture.strategies[kinds[index]])
    assert.match(html, /class="is-active"[^>]*>1h<\/button>/)
    if (name === 'FibonacciScreener') assert.match(html, /placeholder="ATR"/)
  }
  const overview = render('StrategyOverviewPage', fixture.evaluations)
  assert.match(overview, /Strategy Overview|Evaluation coverage/)
  assert.match(overview, /snapshot_id=[^" ]+&amp;interval=1h/)
  const persistence = render('research/RealtimeSessionPersistenceView', fixture.persistence, true, 'include_evaluations=1')
  assert.match(persistence, /5\/5 stored evaluation sessions/)
  assert.match(persistence, /Reconstructed research/)
  assert.match(persistence, /Matched \/ evaluated/)
  assert.match(persistence, /Distance trend|Magnitude trend/)
  assert.equal((persistence.match(/class="pv-cell-/g) ?? []).length, 30)
  assert.match(persistence, /aria-label="volume trajectory"/)
  const research = render('DailyStrategyTicker', fixture.research_session)
  assert.match(research, /Reconstructed research/)
  assert.match(research, /Full ticker workspace/)
  assert.match(research, /interval-aware ATR swing/)
})

test('serialized rolling releases keep the original Persistence layout and expose stored session provenance', {
  skip: !existsSync(offlineFixturePath),
}, async () => {
  const { releases } = JSON.parse(readFileSync(offlineFixturePath, 'utf8'))
  assert.ok(releases.rolling && releases.hourly_rolling, 'Regenerate the guarded offline rolling fixtures')
  const cohort = releases.rolling.latest.publication.cohort_id
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: cohort }
  const api = compiler(read('../src/services/api.ts'), path.join(root, 'services', 'api.ts'), {
    axios: { default: { create: () => ({ get: async (url, { params }) => {
      assert.equal(url, '/strategies/daily')
      const fixture = Object.values(releases).find(item => item.latest.publication.snapshot_id === params.snapshot_id)
      assert.ok(fixture)
      return { data: params.view ? fixture[params.view] : params.ticker ? fixture.research_session : fixture.latest }
    } }) } },
  })
  const render = createPageRenderer()
  for (const [name, interval] of [['rolling', '1d'], ['hourly_rolling', '1h']]) {
    const fixture = releases[name], pin = fixture.latest.publication.snapshot_id
    const data = await api.getStrategyPersistence(pin, interval)
    assert.equal(data.interpretation, 'STORED_SESSION_OUTCOMES_WITH_EXPLICIT_PROVENANCE')
    assert.equal(data.session_provenance[data.evaluation_sessions[4]], 'STORED_EVALUATION')
    const research = await api.getDailyStrategyTickerPublication('AAPL', pin, interval, data.evaluation_sessions[0])
    assert.equal(research.interpretation, 'RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS')
    const html = render('research/RealtimeSessionPersistenceView', data, true, 'include_evaluations=1')
    assert.match(html, /Coverage this window/)
    assert.equal((html.match(/class="pv-rate"/g) ?? []).length, 6)
    assert.match(html, /Observed tickers/)
    assert.match(html, /Incomplete coverage/)
    assert.match(html, /aria-label="Minimum strategies"/)
    assert.match(html, />Context<\/th>/)
    assert.match(html, /Stored outcomes \+ reconstructed research/)
    assert.equal((html.match(/class="pv-cell-/g) ?? []).length, 30)
    assert.doesNotMatch(html, /Independence reference/)
    const filtered = render('research/RealtimeSessionPersistenceView', data, true, 'min_strategies=6')
    assert.match(filtered, /No rows match these filters/)
  }
})

test('realtime Persistence renders all six stored evaluations without false recurrence or independence forecasts', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const data = evaluationBundle(publication('old'))
  const html = createPageRenderer()('research/RealtimePersistenceView', data)
  assert.match(html, /1 stored session \| Recurrence unavailable/)
  assert.match(html, /Evaluation coverage/)
  assert.match(html, /Ticker evaluations/)
  assert.match(html, /MA 9\/21 state/)
  assert.match(html, /9\/9/)
  assert.match(html, /No match/)
  assert.match(html, /aria-label="Persistence strategy"/)
  assert.match(html, /aria-label="Persistence ticker"/)
  assert.match(html, /href="\/strategy-research\/AAPL\?cohort_id=exact-cohort&amp;snapshot_id=old"/)
  assert.match(html, /aria-live="polite"/)
  assert.match(html, /name="ticker_filter"/)
  assert.doesNotMatch(html, /Independence reference|5\/5|Five-session source window|Loading coverage/)
  const source = read('../src/pages/research/PersistenceView.tsx')
  assert.match(source, /VITE_REALTIME_ONLY === 'true' \? <RealtimeSessionPersistenceView \/> : <CanonicalPersistenceView \/>/)
  assert.match(read('../src/pages/research/RealtimePersistenceView.tsx'), /usePublicationQuery\('daily-persistence', \(pin, session\) => getDailyStrategyEvaluations\(pin, interval, session\)\)/)
  assert.match(read('../src/pages/research/RealtimePersistenceView.tsx'), /new URLSearchParams\(searchState.current.pending\)/)
})

test('rapid Persistence filter changes merge pending URL state without losing the ticker or release pin', () => {
  const params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=old')
  const updates = []
  const component = compiler(read('../src/pages/research/RealtimePersistenceView.tsx'), path.join(root, 'pages', 'research', 'RealtimePersistenceView.tsx'), {
    react: { ...React, useRef: value => ({ current: value }), useState: value => [value, () => {}] },
    'react-router-dom': { ...Router, useSearchParams: () => [params, next => updates.push(next)] },
    '../../layout/PageChrome': { CommandBar: 'div', CommandField: 'div', CommandSpacer: 'span' },
    '../../services/api': {}, '../publicationNavigation': { usePublicationQuery: () => ({ data: undefined, loading: false }), useStrategyInterval: () => ({ interval: '1d', setInterval: () => {} }) },
    '../StrategyPublication': { StrategyPublicationError: 'div' }, './persistence.css': {},
  })
  const tree = component.default()
  const controls = tree.props.children[0].props.children
  controls[1].props.children.props.onChange({ target: { value: 'MSFT' } })
  controls[0].props.children.props.onChange({ target: { value: 'SCAN_MOMENTUM_1D' } })
  const selected = updates.at(-1)
  assert.equal(selected.get('ticker_filter'), 'MSFT')
  assert.equal(selected.get('strategy_filter'), 'SCAN_MOMENTUM_1D')
  assert.equal(selected.get('cohort_id'), 'exact-cohort')
  assert.equal(selected.get('snapshot_id'), 'old')
})

test('restored Persistence keeps four unknown sessions distinct from the single retained MA state', () => {
  const component = compiler(read('../src/pages/research/RealtimeSessionPersistenceView.tsx'), path.join(root, 'pages', 'research', 'RealtimeSessionPersistenceView.tsx'), {
    '../../layout/PageChrome': {}, '../../services/api': {}, '../publicationNavigation': {}, '../StrategyPublication': {},
    './persistencePresentation': {},
    './RealtimePersistenceView': { persistenceStrategies: () => [{ contract: 'SCAN_MA_1D_9_21' }] }, './persistence.css': {},
  })
  const data = { ...evaluationBundle(publication('old')), evaluation_sessions: ['2026-09-28', '2026-09-29', '2026-09-30', '2026-10-01', '2026-10-02'] }
  data.scanners.SCAN_MA_1D_9_21.results = [{ ticker: 'AAPL', signal: 'Above' }]
  data.analysis.SCAN_MA_1D_9_21.members[0].status = 'COMPLETE'
  const row = component.legacyPersistenceRows(data)[0]
  assert.deepEqual(row.sessions.map(cell => cell.state), ['UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', 'UNAVAILABLE', 'NO_MATCH'])
  assert.equal(row.evaluated, 1)
  assert.equal(row.matched, 0)
  assert.equal(row.coverage, '1/5')
  assert.equal(row.transition, 'UNKNOWN')
  assert.equal(row.sessions[4].native_rows[0].signal, 'Above')
})

test('Persistence presentation retains original Fibonacci and gap labels without smoothing unknown sessions', () => {
  const { persistenceAxes } = compiler(read('../src/pages/research/persistencePresentation.ts'), path.join(root, 'pages', 'research', 'persistencePresentation.ts'))
  const sessions = Array.from({ length: 5 }, (_, index) => ({ session: `2026-10-0${index + 5}`, state: 'MATCH',
    distance: index + 1, volume: 1.23, maturity: 63, event_ids: ['same-pivots'], magnitude: 10,
    native_rows: [{ nearest_level: index < 3 ? '138.2%' : '161.8%', retracement_pct: 10 + index * 3 }] }))
  const axes = persistenceAxes({ contract: 'SCAN_FIBONACCI_1D_5', evaluated: 5, matched: 5, sessions })
  assert.equal(axes.stability.label, 'Sticky')
  assert.equal(axes.distance.label, 'Diverging')
  assert.equal(axes.magnitude.label, 'Deepening')
  assert.equal(axes.maturity.label, 'Pivots stable')
  assert.equal(axes.volume.label, 'Normal')
  assert.equal(axes.volume.note, '1.23x')
  const gapSessions = sessions.map((cell, index) => ({ ...cell, native_rows: [{ gap_date: '2026-01-01', gap_age_sessions: 63,
    gap_type: index === 0 || index === 4 ? 'At Support (Unfilled Gap Up)' : 'Possible Downside (In Gap Up)' }] }))
  const gap = persistenceAxes({ contract: 'SCAN_GAPS_1D', evaluated: 5, matched: 5, sessions: gapSessions })
  assert.equal(gap.magnitude.label, '0 new')
  assert.equal(gap.maturity.label, 'Stale')
  assert.equal(gap.context, 'At Edge')
  assert.match(gap.stability.label, /At Edge -> In Gap/)
  sessions[2] = { ...sessions[2], state: 'UNAVAILABLE', native_rows: [], distance: null, volume: null }
  const partial = persistenceAxes({ contract: 'SCAN_FIBONACCI_1D_5', evaluated: 4, matched: 4, sessions })
  assert.equal(partial.distance.series[2], null)
  assert.match(partial.stability.note, /Unknown.*Partial coverage/)
})

test('rolling Persistence service preserves per-session observation clocks and explicit provenance', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const pub = { ...publication('old'), release_type: 'DAILY_STRATEGY_ROLLING_V1' }
  const sessions = ['2026-09-28', '2026-09-29', '2026-09-30', '2026-10-01', '2026-10-02']
  const data = { ...evaluationBundle(pub), members: ['AAPL'], evaluation_sessions: sessions, observed_sessions: sessions,
    interpretation: 'STORED_SESSION_OUTCOMES_WITH_EXPLICIT_PROVENANCE', reconstruction_at: null,
    session_provenance: Object.fromEntries(sessions.map(session => [session, 'STORED_EVALUATION'])),
    persistence: { sessions, window_unit: 'XNYS_TRADING_SESSIONS', rows: evaluationContracts.map(contract => ({
      ticker: 'AAPL', contract, matched: 0, evaluated: 5, coverage: '5/5', consecutive_run: 0, transition: 'UNCHANGED',
      sessions: sessions.map(session => ({ session, state: 'NO_MATCH', native_rows: [], observed_at: `${session}T20:20:00Z`,
        market_time: `${session}T20:00:00Z`, input_sha256: 'immutable-input', evidence_ref: { cohort_id: 'exact-cohort',
          evidence_id: `${contract}:${session}`, market_time: `${session}T20:00:00Z` } })),
    })) } }
  const api = compiler(read('../src/services/api.ts'), path.join(root, 'services', 'api.ts'), {
    axios: { default: { create: () => ({ get: async (url, { params }) => ({ data: params.view ? data : { publication: pub } }) }) } },
  })
  const result = await api.getStrategyPersistence('old')
  assert.equal(result.persistence.rows[0].sessions[0].observed_at, '2026-09-28T20:20:00Z')
  data.persistence.rows[0].sessions[0].observed_at = '2026-10-03T20:20:00Z'
  await assert.rejects(api.getStrategyPersistence('old'), /sessions or evidence/)
})

test('rapid ticker plus interval changes retain pending filters and clear only the incompatible pin', () => {
  const params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=old')
  const updates = []
  const router = { ...Router, useSearchParams: () => [params, next => updates.push(typeof next === 'function' ? next(params) : next)] }
  const navigation = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'), {
    'react-router-dom': router,
  })
  const component = compiler(read('../src/pages/research/RealtimePersistenceView.tsx'), path.join(root, 'pages', 'research', 'RealtimePersistenceView.tsx'), {
    react: { ...React, useRef: value => ({ current: value }), useState: value => [value, () => {}] },
    'react-router-dom': router, '../../layout/PageChrome': { CommandBar: 'div', CommandField: 'div', CommandGroup: 'div', CommandSpacer: 'span' },
    '../../services/api': {}, '../publicationNavigation': { ...navigation, usePublicationQuery: () => ({ data: undefined, loading: false }) },
    '../StrategyPublication': { StrategyPublicationError: 'div' }, './persistence.css': {},
  })
  const controls = component.default().props.children[0].props.children
  controls[1].props.children.props.onChange({ target: { value: 'MSFT' } })
  controls[4].props.children[1].props.onClick()
  const selected = updates.at(-1)
  assert.equal(selected.get('ticker_filter'), 'MSFT')
  assert.equal(selected.get('cohort_id'), 'exact-cohort')
  assert.equal(selected.get('interval'), '1h')
  assert.equal(selected.get('snapshot_id'), null)
})

test('Persistence keeps action exclusions distinct from eligible no-match and preserves native metrics', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const data = evaluationBundle(publication('old'))
  for (const key of evaluationContracts) {
    data.analysis[key].members[0] = { ...data.analysis[key].members[0], status: 'UNAVAILABLE', reason: 'CORPORATE_ACTION_REVIEW' }
    data.analysis[key].total_evaluated = 8
    data.analysis[key].total_unavailable = 1
    delete data.evidence_refs.AAPL[key]
  }
  const html = createPageRenderer()('research/RealtimePersistenceView', data)
  assert.match(html, /Action review/)
  assert.match(html, /8\/9/)
  const component = compiler(read('../src/pages/research/RealtimePersistenceView.tsx'), path.join(root, 'pages', 'research', 'RealtimePersistenceView.tsx'), {
    '../../layout/PageChrome': {}, '../../services/api': {}, '../publicationNavigation': {}, '../StrategyPublication': {}, './persistence.css': {},
  })
  data.scanners.SCAN_MOMENTUM_1D.results = [{ ticker: 'MSFT', grade: 'B', score: 68 }]
  data.analysis.SCAN_MOMENTUM_1D.members.find(member => member.ticker === 'MSFT').status = 'COMPLETE'
  assert.equal(component.evaluationCell(data, 'SCAN_MOMENTUM_1D', 'MSFT').summary, 'B (score 68)')
  assert.equal(component.evaluationCell(data, 'SCAN_GAPS_1D', 'AAPL').summary, 'Action review')
  assert.match(read('../src/pages/research/RealtimePersistenceView.tsx'), /publicationPath\(`\/strategy-research\/\$\{ticker\}`, data.publication.snapshot_id, data.publication.cohort_id, interval, data.publication.evaluation_session\)/)
})

test('all strategy pages begin with controls without publication prose and retain daily gates and sorting', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const renderPage = createPageRenderer()
  for (const [index, name] of pages.entries()) {
    const pub = publication('old')
    const result = { data: envelope(pub), publication: pub, ...(index === 0 ? { fvgData: envelope(pub) } : {}) }
    const html = renderPage(name, result)
    assert.doesNotMatch(html, /strategy-publication|Stored daily research publication|Market:|Published:|Read:|Snapshot:|Daily strategy analysis:/)
    assert.match(html, /^<div[^>]*>\s*<div class="tm-commandbar"/)
    for (const iv of ['5m', '15m', '30m']) assert.doesNotMatch(html, new RegExp(`<button[^>]*>${iv}</button>`))
    assert.match(html, /<button[^>]*>1h<\/button>/)
    assert.match(html, /<button[^>]*>1d<\/button>/)
    assert.match(html, /<th tabindex="0" aria-sort=/)
    assert.doesNotMatch(html, /<(input|select)(?![^>]*aria-label=)[^>]*>/)
    if (name === 'MAScreener') assert.match(html, /<input[^>]*disabled=""[^>]*value="9"/)
    if (name === 'FibonacciScreener') assert.match(html, /<input[^>]*disabled=""[^>]*value="5"/)
  }
})

test('strategy pages share portal typography and compact category tabs without legacy inline fonts', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const render = createPageRenderer()
  for (const [index, page] of pages.entries()) {
    const result = { data: envelope(publication('old')), publication: publication('old'), ...(index === 0 ? { fvgData: envelope(publication('old')) } : {}) }
    assert.match(render(page, result), /^<div class="strategy-workspace">/)
    assert.match(read(`../src/pages/${page}.tsx`), /import '\.\/strategyPages.css'/)
  }
  for (const page of ['GapScreener', 'MAScreener']) {
    const source = read(`../src/pages/${page}.tsx`)
    assert.match(source, /className="strategy-tabs"/)
    assert.match(source, /className="strategy-tab-count"/)
    assert.match(source, /aria-pressed={isActive}/)
    assert.doesNotMatch(source, /fontSize: (?:isCrossTab[^\n]*|)'0\.95rem'/)
  }
  const styles = read('../src/pages/strategyPages.css')
  assert.match(styles, /font-family: inherit/)
  assert.match(styles, /font-size: 12\.5px/)
  assert.match(styles, /text-transform: none/)
  assert.match(styles, /\.strategy-workspace \.pv-table th\s*{[^}]*font-size: 11\.5px/)
  assert.match(render('StrategyOverviewPage', evaluationBundle(publication('old'))), /^<div class="strategy-workspace">/)
  assert.match(read('../src/pages/DailyStrategyTicker.tsx'), /className="daily-research strategy-workspace"/)
  assert.match(styles, /border-bottom-color: var\(--tm-accent\)/)
})

test('coverage-aware empty states distinguish missing, short, partial, failed and completed analyses on every page', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const renderPage = createPageRenderer()
  for (const status of ['UNAVAILABLE', 'INSUFFICIENT_HISTORY', 'PARTIAL', 'FAILED', 'COMPLETE_EMPTY']) {
    for (const [index, name] of pages.entries()) {
      const pub = publication('old')
      const data = envelope(pub)
      data.analysis = { ...data.analysis, status, total_evaluated: status === 'PARTIAL' ? 1 : status === 'COMPLETE_EMPTY' ? 9 : 0,
        total_unavailable: status === 'PARTIAL' ? 8 : status === 'COMPLETE_EMPTY' ? 0 : 9,
        members: data.analysis.members.map(member => ({ ...member, status, available_bars: 19 })) }
      const html = renderPage(name, { data, publication: pub, ...(index === 0 ? { fvgData: data } : {}) })
      assert.doesNotMatch(html, /strategy-publication/)
      if (status === 'INSUFFICIENT_HISTORY') assert.match(html, /Insufficient input history; analysis was not completed/)
      if (status === 'UNAVAILABLE') assert.match(html, /Inputs unavailable; analysis was not performed/)
      if (status === 'FAILED') assert.match(html, /Strategy analysis failed/)
      if (status === 'PARTIAL') assert.match(html, /1 members evaluated; 8 unavailable/)
    }
  }
})

test('independent strategy pins stay within strategy pages and exact member research', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const { navigationPublicationPath: nav } = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'))
  assert.equal(nav('/fibonacci', '/gaps', 'old', 'exact-cohort'), '/fibonacci?cohort_id=exact-cohort&snapshot_id=old')
  assert.equal(nav('/gaps', '/strategy-research/MSFT', 'old', 'exact-cohort'), '/gaps?cohort_id=exact-cohort&snapshot_id=old')
  assert.equal(nav('/', '/gaps', 'old', 'exact-cohort'), '/?cohort_id=exact-cohort')
  assert.equal(nav('/gaps', '/', 'dashboard', 'exact-cohort'), '/gaps?cohort_id=exact-cohort')
  assert.equal(nav('/stocks/persistence', '/gaps', 'old', 'exact-cohort'), '/stocks/persistence?cohort_id=exact-cohort&snapshot_id=old')
  assert.equal(nav('/stocks/persistence', '/ticker/AAPL', 'ticker-pin', 'exact-cohort'), '/stocks/persistence?cohort_id=exact-cohort')
})

test('strategy freshness uses its independent exact pin and never reads Dashboard or ticker status', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const mock = mockApi()
  const status = await mock.api.fetchMaterializationStatus('old', undefined, true)
  assert.equal(status.source, 'DAILY_STRATEGIES_V1')
  assert.equal(status.status, 'READY')
  assert.equal(status.last_market_time, publication('old').market_time)
  assert.deepEqual(mock.calls, [['/strategies/daily', { cohort_id: 'exact-cohort', snapshot_id: 'old', read_mode: 'retained' }]])
  assert.match(read('../src/layout/StalenessBanner.tsx'), /fetchMaterializationStatus\(snapshotId, ticker, dailyStrategies, interval\)/)
})

test('exact member research renders missing and insufficient analysis without fabricated no-match or metrics', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const renderPage = createPageRenderer()
  for (const status of ['UNAVAILABLE', 'INSUFFICIENT_HISTORY', 'COMPLETE_EMPTY']) {
    const pub = publication('old')
    const names = ['gaps', 'fvg', 'ma', 'momentum', 'bearish', 'fibonacci']
    const html = renderPage('DailyStrategyTicker', { publication: pub, scanner_basis: basis, ticker: 'AAPL', cohort_count: 9,
      overview: null, setup: null, evidence_refs: {},
      analysis: Object.fromEntries(names.map(name => [name, { ticker: 'AAPL', status, available_bars: 19, required_bars: 210 }])),
      scans: Object.fromEntries(names.map(name => [name, []])) })
    assert.match(html, /Retained quote unavailable/)
    assert.match(html, /Daily setup analysis unavailable/)
    if (status !== 'COMPLETE_EMPTY') assert.doesNotMatch(html, /No matches for this ticker in this complete retained publication/)
    else assert.match(html, /No matches for this ticker in this complete retained publication/)
  }
})

test('corporate-action partial coverage keeps healthy page rows and exact unavailable ticker research honest', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const renderPage = createPageRenderer()
  const action = { action_id: 'action-id', action_type: 'MERGER', effective_date: '2026-09-01',
    payload_sha256: 'action-hash', cash_amount: null, currency: null, distribution_type: null, handling: 'CORPORATE_ACTION_REVIEW' }
  for (const [index, name] of pages.entries()) {
    const pub = publication('old'), data = envelope(pub)
    data.analysis = { ...data.analysis, status: 'PARTIAL', total_evaluated: 8, total_unavailable: 1,
      members: data.analysis.members.map(member => member.ticker === 'AAPL'
        ? { ...member, status: 'UNAVAILABLE', reason: 'CORPORATE_ACTION_REVIEW', corporate_actions: [action] } : member) }
    const html = renderPage(name, { data, publication: pub, ...(index === 0 ? { fvgData: data } : {}) })
    assert.match(html, /8 members evaluated; 1 unavailable/)
    assert.doesNotMatch(html, /strategy-publication|MERGER 2026-09-01/)
  }
  const mock = mockApi()
  mock.actionReview()
  const research = await mock.api.getDailyStrategyTickerPublication('AAPL', 'old')
  assert.equal(research.publication.snapshot_id, 'old')
  assert.deepEqual(research.evidence_refs, {})
  assert.ok(Object.values(research.analysis).every(member => member.reason === 'CORPORATE_ACTION_REVIEW'))
  const html = renderPage('DailyStrategyTicker', research)
  assert.match(html, /Corporate action review required; this ticker was not evaluated/)
  assert.doesNotMatch(html, /No matches for this ticker in this complete retained publication/)
})

test('recurring dividend disclosure preserves price-series basis and never implies total-return adjustment', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const pub = publication('old'), data = envelope(pub)
  data.analysis.members[0].corporate_actions = [{ action_id: 'dividend-id', action_type: 'DIVIDEND', effective_date: '2026-09-01',
    cash_amount: '0.26', currency: 'USD', distribution_type: 'recurring', handling: 'RECURRING_CASH_DIVIDEND_PRICE_SERIES_UNCHANGED' }]
  const component = compiler(read('../src/pages/StrategyPublication.tsx'), path.join(root, 'pages', 'StrategyPublication.tsx'), {
    './StrategyPublication.css': {},
  })
  const html = renderToStaticMarkup(React.createElement(component.StrategyActionContext, { member: data.analysis.members[0] }))
  assert.match(html, /0.26 USD/)
  assert.match(html, /split-adjusted prices unchanged/)
  assert.match(html, /Gap signals may include the ex-dividend effect/)
  assert.equal(component.StrategyPublicationError({ error: null, publication: pub }), null)
  const error = renderToStaticMarkup(React.createElement(component.StrategyPublicationError, {
    error: 'Stored publication unavailable', publication: pub,
  }))
  assert.match(error, /role="alert"/)
  assert.match(error, /Retaining the previous complete publication and its original clocks/)
})

test('cold realtime pages report stored reads without text above the controls', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const renderPage = createPageRenderer()
  for (const [index, name] of pages.entries()) {
    const pub = publication('old')
    const result = { data: envelope(pub), publication: pub, ...(index === 0 ? { fvgData: envelope(pub) } : {}) }
    const html = renderPage(name, result, false)
    assert.match(html, /Reading stored 1d/)
    assert.doesNotMatch(html, /Scanning/)
    assert.doesNotMatch(html, /strategy-publication|ticker links open|ticker links are disabled/)
    assert.match(html, /^<div[^>]*>\s*<div class="tm-commandbar"/)
  }
})

test('guarded exported real-producer rows enable only bounded publication research in all original pages',
  { skip: !process.env.DAILY_SCANNERS_OFFLINE_FIXTURE }, () => {
    const fixture = JSON.parse(readFileSync(process.env.DAILY_SCANNERS_OFFLINE_FIXTURE, 'utf8'))
    const pub = fixture['/api/latest-price-date'].publication
    const cohort = new URLSearchParams(process.env.DAILY_SCANNERS_OFFLINE_COHORT ?? '').get('cohort_id')
    assert.ok(cohort, 'Supply the synthetic exporter cohort, never a runtime source')
    globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: cohort }
    const renderPage = createPageRenderer()
    for (const [index, name] of pages.entries()) {
      const result = {
        data: fixture[`/api/scan/${kinds[index]}`], publication: pub,
        ...(index === 0 ? { fvgData: fixture['/api/scan/fvg'] } : {}),
      }
      assert.ok(result.data.total_signals > 0)
      const html = renderPage(name, result)
      assert.doesNotMatch(html, /strategy-publication|Snapshot:/)
      assert.match(html, /aria-sort=/)
      assert.doesNotMatch(html, /href="\/ticker/)
      for (const button of html.match(/<button\b[^>]*class="ticker"[^>]*>/g) ?? []) {
        assert.doesNotMatch(button, /disabled=""/)
        assert.match(button, /Open retained daily research/)
      }
      if (name === 'MomentumPullback') assert.match(html, /MSFT/)
      if (name === 'BearishBounce') assert.match(html, /AMZN/)
    }
  })

test('canonical original pages retain interval and custom-parameter controls without realtime disclosure', () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'false' }
  const renderPage = createPageRenderer()
  for (const [index, name] of pages.entries()) {
    const pub = publication('old')
    const result = { data: envelope(pub), publication: pub, ...(index === 0 ? { fvgData: envelope(pub) } : {}) }
    const html = renderPage(name, result)
    assert.doesNotMatch(html, /Stored daily research publication/)
    for (const iv of ['5m', '15m', '30m', '1h']) assert.match(html, new RegExp(`<button(?![^>]*disabled=)[^>]*>${iv}</button>`))
    if (name === 'MAScreener') assert.match(html, /<input(?![^>]*disabled=)[^>]*value="9"/)
    if (name === 'FibonacciScreener') assert.match(html, /<input(?![^>]*disabled=)[^>]*value="5"/)
  }
})

test('sort activation uses Enter/Space, defaults remain unchanged and ticker gates cover every original page', () => {
  const shared = compiler(read('../src/pages/strategyShared.ts'), path.join(root, 'pages', 'strategyShared.ts'))
  let sorts = 0, prevented = 0
  const props = shared.sortHeaderProps(() => sorts++, true, 'desc')
  assert.equal(props['aria-sort'], 'descending')
  for (const key of ['Enter', ' ', 'Escape']) props.onKeyDown({ key, preventDefault: () => prevented++ })
  props.onClick()
  assert.equal(sorts, 3)
  assert.equal(prevented, 2)
  for (const page of pages) {
    const source = read(`../src/pages/${page}.tsx`)
    assert.match(source, /enabled: !realtimeOnly/)
    assert.match(source, /\(realtimeOnly \|\| !loading\) && data/)
    assert.match(source, /<StrategyTickerButton ticker=/)
    assert.doesNotMatch(source, /<span className="ticker" onClick=/)
  }
  for (const [route, page] of kinds.map((kind, index) => [kind, pages[index]])) {
    const routePath = route === 'gaps' ? 'gaps' : route
    assert.equal(read('../src/App.tsx').split(`path="/${routePath}" element={<${page} />}`).length - 1, 2)
  }
  const momentum = read('../src/pages/MomentumPullback.tsx')
  assert.match(momentum, /ADX 15-55.*2 ATR.*RSI 30-60/)
  assert.doesNotMatch(momentum, /ADX 20-60|Within 1 ATR|High-Probability/)
  const gap = read('../src/pages/GapScreener.tsx')
  assert.equal((gap.match(/aria-expanded=\{expandedTickers\.has\(g\.ticker\)\}/g) ?? []).length, 2)
  assert.match(read('../src/pages/FibonacciScreener.tsx'), /aria-expanded=\{expanded\}/)
})

test('bounded daily research reads all contracts with one explicit publication and selects only the requested member', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const mock = mockApi()
  mock.research()
  mock.rotate()
  const result = await mock.api.getDailyStrategyTickerPublication('aapl', 'old')
  assert.equal(result.ticker, 'AAPL')
  assert.equal(result.cohort_count, 2)
  assert.equal(result.publication.snapshot_id, 'old')
  assert.deepEqual(Object.keys(result.scans), ['gaps', 'fvg', 'ma', 'momentum', 'bearish', 'fibonacci'])
  for (const rows of Object.values(result.scans)) assert.deepEqual(rows, [{ ticker: 'AAPL' }])
  assert.equal(mock.calls.length, 2)
  for (const [, params] of mock.calls) {
    assert.equal(params.snapshot_id, 'old')
    assert.equal(params.cohort_id, 'exact-cohort')
    assert.equal(params.tickers, undefined)
    assert.equal(params.scan_date, undefined)
    assert.equal(params.refresh, undefined)
  }
  assert.ok(mock.calls.every(([url]) => !url.startsWith('/stock/') && !url.startsWith('/options/') && !url.startsWith('/equity/')))
  mock.calls.length = 0
  await assert.rejects(mock.api.getDailyStrategyTickerPublication('bad/ticker', 'old'), /Invalid daily research ticker/)
  assert.equal(mock.calls.length, 0)
  await assert.rejects(mock.api.getDailyStrategyTickerPublication('OUTSIDE', 'old'), /membership does not match/)
  assert.equal((await mock.api.getDailyStrategyTickerPublication('MSFT')).publication.snapshot_id, 'new')
})

test('bounded daily research rejects incomplete, mismatched and partially failed publications without fallback', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  for (const route of ['/tickers/overview', '/tickers/setup-summary', ...routes.map(route => `/scan/${route}`)]) {
    const mock = mockApi()
    mock.research()
    mock.fail(route)
    await assert.rejects(mock.api.getDailyStrategyTickerPublication('AAPL', 'old'), /Stored scanner unavailable/)
    assert.ok(mock.calls.every(([, params]) => params.snapshot_id === 'old'))
  }
  for (const fault of ['mismatch', 'absent', 'missingBasis']) {
    const mock = mockApi()
    mock.research()
    mock[fault](true)
    await assert.rejects(mock.api.getDailyStrategyTickerPublication('AAPL', 'old'), /does not match|research basis/)
  }
  const old = mockApi()
  old.legacy()
  await assert.rejects(old.api.getDailyStrategyTickerPublication('AAPL', 'old'), /does not retain complete/)
  assert.equal(old.calls.length, 1)
})

test('strategy ticker buttons preserve navigation and pin bounded research when available', () => {
  let destination, stopped = false
  const component = compiler(read('../src/pages/StrategyTickerButton.tsx'), path.join(root, 'pages', 'StrategyTickerButton.tsx'), {
    'react-router-dom': { useNavigate: () => path => { destination = path } },
    './publicationNavigation': compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts')),
  })
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const enabled = component.StrategyTickerButton({ ticker: 'AAPL', publication: publication('old') })
  enabled.props.onClick({ stopPropagation: () => { stopped = true } })
  assert.equal(destination, '/strategy-research/AAPL?cohort_id=exact-cohort&snapshot_id=old')
  assert.equal(stopped, true)
  for (const pub of [undefined, { ...publication('old'), strategy_ticker_available: false }, { ...publication('old'), cohort_id: undefined }]) {
    destination = undefined
    const fallback = component.StrategyTickerButton({ ticker: 'AAPL', publication: pub })
    assert.equal(fallback.props.disabled, undefined)
    fallback.props.onClick({ stopPropagation: () => {} })
    assert.equal(destination, '/ticker/AAPL')
  }
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'false' }
  component.StrategyTickerButton({ ticker: 'AAPL' }).props.onClick({ stopPropagation: () => {} })
  assert.equal(destination, '/ticker/AAPL')
})

test('bounded research requires explicit URL identities even when an old publication is already cached', async () => {
  globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: 'exact-cohort' }
  const client = new QueryClient()
  const original = { publication: publication('old') }
  client.setQueryData(['daily-strategy-ticker:AAPL', 'exact-cohort', 'old'], original)
  let params, options, calls = 0
  const hooks = compiler(read('../src/pages/publicationNavigation.ts'), path.join(root, 'pages', 'publicationNavigation.ts'), {
    react: { useEffect: () => {}, useRef: value => ({ current: value }), useState: value => [value, () => {}] },
    'react-router-dom': { useSearchParams: () => [params, () => {}] },
    '@tanstack/react-query': {
      useQueryClient: () => client,
      useQuery: spec => { options = spec; return { data: client.getQueryData(spec.queryKey), isFetching: false } },
    },
  })
  for (const search of ['snapshot_id=old', 'cohort_id=exact-cohort', 'cohort_id=wrong&snapshot_id=old']) {
    params = new URLSearchParams(search)
    const view = hooks.usePublicationQuery('daily-strategy-ticker:AAPL', async () => { calls++; return original }, true)
    assert.equal(view.data, undefined)
    assert.match(view.error, /explicit cohort and snapshot|no substitution/)
    assert.throws(options.queryFn, /explicit cohort and snapshot|no substitution/)
    await view.refresh()
  }
  params = new URLSearchParams('cohort_id=exact-cohort&snapshot_id=old')
  hooks.usePublicationQuery('daily-strategy-ticker:MSFT', async () => original, true)
  assert.deepEqual(options.queryKey, ['daily-strategy-ticker:MSFT', 'exact-cohort', 'old'])
  assert.equal(calls, 0)
  assert.equal(client.getQueryData(['daily-strategy-ticker:AAPL', 'exact-cohort', 'old']), original)
  client.clear()
})

test('guarded complete daily research renders all six sections without independent ticker APIs',
  { skip: !process.env.DAILY_SCANNERS_OFFLINE_FIXTURE }, async () => {
    const fixture = JSON.parse(readFileSync(process.env.DAILY_SCANNERS_OFFLINE_FIXTURE, 'utf8'))
    const cohort = new URLSearchParams(process.env.DAILY_SCANNERS_OFFLINE_COHORT).get('cohort_id')
    globalThis.__dailyEnvironment = { VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: cohort }
    const api = compiler(read('../src/services/api.ts'), path.join(root, 'services', 'api.ts'), {
      axios: { default: { create: () => ({ get: async url => {
        assert.ok(Object.hasOwn(fixture, `/api${url}`), `No independent workspace call: ${url}`)
        return { data: fixture[`/api${url}`] }
      } }) } },
    })
    const result = await api.getDailyStrategyTickerPublication('AAPL', fixture['/api/latest-price-date'].publication.snapshot_id)
    assert.equal(result.scans.fvg.length, 3)
    assert.equal(result.scans.ma.length, 1)
    const html = createPageRenderer()('DailyStrategyTicker', result)
    for (const label of ['Opening gaps', 'Fair value gaps - lookback 50', 'SMA 9/21', 'Momentum Pullback', 'Bearish Bounce', 'Fibonacci - minimum swing 5%']) {
      assert.ok(html.includes(label), label)
    }
    assert.match(html, /All stored setup descriptors/)
    assert.match(html, /Overview raw liquidity is distinct/)
    assert.match(html, /No matches for this ticker in this complete retained publication/)
    assert.doesNotMatch(html, /href="\/ticker\/|financials|option-event-calendar/)
  })
