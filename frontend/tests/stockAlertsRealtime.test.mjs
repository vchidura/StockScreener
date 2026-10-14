import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

test('both deployment modes use the original Stock Alerts page and retire only its validation adapter', () => {
  const app = readFileSync(new URL('../src/App.tsx', import.meta.url), 'utf8')
  const mapped = readFileSync(new URL('../src/pages/realtime/RealtimeMappedPages.tsx', import.meta.url), 'utf8')
  const css = readFileSync(new URL('../src/pages/StockAlertsPage.css', import.meta.url), 'utf8')
  assert.equal(app.match(/path="\/stocks\/alerts" element=\{<StockAlertsPage \/>\}/g)?.length, 2)
  assert.doesNotMatch(app + mapped, /RealtimeStockAlertsPage/)
  assert.match(mapped, /export function RealtimeOptionsAlertsPage/)
  assert.match(css, /\.stock-alerts \{[^}]*grid-template-columns: minmax\(0,1fr\)/)
})

test('both original alert services preserve filters and pin the realtime cohort only in realtime mode', async () => {
  const apiSource = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__alertApi }')
    .replaceAll('import.meta.env', 'globalThis.__alertEnvironment')
  const compile = source => ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const apiUrl = `data:text/javascript;base64,${Buffer.from(compile(apiSource)).toString('base64')}`
  const service = readFileSync(new URL('../src/services/stockDiscovery.ts', import.meta.url), 'utf8')
    .replace("from './api'", `from '${apiUrl}'`)
  const calls = []
  globalThis.__alertEnvironment = { VITE_REALTIME_ONLY: 'false' }
  globalThis.__alertApi = { get: async (...args) => { calls.push(args); return { data: { retained: true } } } }
  try {
    const { getAlertView, getStockEodReview } = await import(`data:text/javascript;base64,${Buffer.from(compile(service)).toString('base64')}`)
    const params = { source: 'SHADOW', view: 'history', combined: true, search: 'AAPL',
      trade_type: 'SWING', interval: '30m', status: 'OPEN', direction: 1, model: 'failure',
      session_date: '2026-10-02', run: undefined, sort: 'ticker', descending: false, offset: 100, limit: 100 }
    for (const realtime of [false, true]) {
      calls.length = 0
      globalThis.__alertEnvironment = { VITE_REALTIME_ONLY: String(realtime), VITE_REALTIME_COHORT_ID: 'fixed-cohort' }
      await getAlertView(params)
      await getStockEodReview({ session_date: params.session_date, selection_status: 'SELECTED', offset: 100 })
      assert.deepEqual(calls.map(call => call[0]), ['/stocks/alert-view', '/stocks/alert-eod-review'])
      assert.deepEqual(calls[0][1].params, realtime ? { ...params, cohort_id: 'fixed-cohort' } : params)
      assert.equal(calls[1][1].params.offset, 100)
      assert.equal(calls[1][1].params.selection_status, 'SELECTED')
      assert.equal(calls[1][1].params.cohort_id, realtime ? 'fixed-cohort' : undefined)
    }
    calls.length = 0
    globalThis.__alertEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(getAlertView(params), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(getStockEodReview({}), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
    assert.equal(params.cohort_id, undefined)
  } finally {
    delete globalThis.__alertApi
    delete globalThis.__alertEnvironment
  }
})
