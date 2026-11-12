import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

// Nov 12 (user): the ticker chart shows the developing bar for 5m to 1mo. It comes from the API's forming read, is
// appended as the provisional last candle only when it starts after the last final bar, and stays out of indicators
// and patterns (they read the finalized points).
const api = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8').replace(/\r/g, '')
const start = api.indexOf('export const withFormingBar')
const body = api.slice(start, api.indexOf('\n}\n', start) + 2).replace('export const', 'const')
const js = ts.transpileModule(`${body}\nexport { withFormingBar }`, {
  compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 },
}).outputText
const { withFormingBar } = await import(`data:text/javascript,${encodeURIComponent(js)}`)

const final = [
  { time: 100, open: 1, high: 2, low: 1, close: 2, volume: 10 },
  { time: 200, open: 2, high: 3, low: 2, close: 3, volume: 20 },
]
const forming = (time, status = 'FORMING') => ({
  interval: '5m', status, reason: null,
  point: { time, open: 3, high: 4, low: 3, close: 3.5, volume: 5, forming: true, as_of: '2026-11-12T16:07:00+00:00', minute_bars: 2, daily_bars: 0 },
})

test('the developing bar is appended as the provisional last candle', () => {
  const points = withFormingBar(final, forming(300))
  assert.equal(points.length, 3)
  assert.deepEqual(points[2], { time: 300, open: 3, high: 4, low: 3, close: 3.5, volume: 5, provisional: true,
    source_interval: '1m', available_through: Date.parse('2026-11-12T16:07:00Z') / 1000 })
  assert.equal(final.length, 2) // the published points are not changed
})

test('no developing bar when there is none, it is not newer, or the read failed', () => {
  assert.equal(withFormingBar(final, forming(200)), final)
  assert.equal(withFormingBar(final, forming(150)), final)
  assert.equal(withFormingBar(final, { interval: '5m', status: 'NONE', reason: 'FINAL_BAR_PUBLISHED', point: null }), final)
  assert.equal(withFormingBar(final, null), final)
  assert.deepEqual(withFormingBar([], forming(300)).map(point => point.time), [300])
})

test('the chart reads the developing bar for every interval from 5m to 1mo, outside closing views', () => {
  const page = readFileSync(new URL('../src/pages/TickerDetail.tsx', import.meta.url), 'utf8').replace(/\r/g, '')
  assert.match(page, /const FORMING_BAR_INTERVALS = new Set\(\['5m', '15m', '30m', '1h', '1d', '1wk', '1mo'\]\)/)
  assert.match(page, /enabled: !!symbol && readsEnabled && realtimeOnly && !closingView && FORMING_BAR_INTERVALS\.has\(interval\)/)
  assert.match(page, /withFormingBar\(publishedChartData, formingBar\)/)
  assert.match(page, /chartData\.filter\(point => !point\.provisional\)/) // indicators and patterns stay on final bars
  assert.match(api, /`\/stock\/\$\{ticker\}\/chart\/forming`/)
})
