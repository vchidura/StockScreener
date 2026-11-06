import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { createRequire, Module } from 'node:module'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'

const require = createRequire(import.meta.url)
const root = fileURLToPath(new URL('../src/', import.meta.url))

function load() {
  const filename = path.join(root, 'services/livePrices.ts')
  const module = new Module(filename)
  module.filename = filename
  module.paths = Module._nodeModulePaths(path.dirname(filename))
  module.require = specifier => (specifier === './api' ? {} : specifier === '@tanstack/react-query' ? { useQuery: () => ({}) } : require(specifier))
  module._compile(ts.transpileModule(readFileSync(filename, 'utf8').replaceAll('import.meta.env', 'globalThis.__liveEnvironment'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText, filename)
  return module.exports
}

const { formatLiveTime, toLivePrice, withLiveAlert, withLiveFact, withLiveOverview, withLiveScreeningRow } = load()
const session = '2026-11-05'

test('an alert compares today\'s price with its trigger, in the alert\'s direction', () => {
  const stored = { direction: 1, trigger_price: 100, latest_price: null, latest_price_at: null, price_return: null, price_return_status: 'UNAVAILABLE' }
  const live = { price: 102, asOf: '2026-11-05T18:45:00+00:00' }

  const long = withLiveAlert(stored, live)
  assert.equal(long.latest_price, 102)
  assert.equal(long.latest_price_at, live.asOf)
  assert.ok(Math.abs(long.price_return - 0.02) < 1e-12)
  assert.ok(Math.abs(withLiveAlert({ ...stored, direction: -1 }, live).price_return + 0.02) < 1e-12, 'a short loses when the price rises')

  assert.equal(withLiveAlert(stored, undefined), stored, 'no price, no change')
  const alreadyPriced = { ...stored, latest_price: 99 }
  assert.equal(withLiveAlert(alreadyPriced, live), alreadyPriced, 'a stored price is never replaced')
  const noTrigger = { ...stored, trigger_price: null }
  assert.equal(withLiveAlert(noTrigger, live), noTrigger, 'nothing to compare with')
})
const at = text => Date.parse(text) / 1000
const quote = { ticker: 'AAPL', price: 352.01, previous_close: 349.56, change: 2.45, change_percent: 0.7, as_of: '2026-11-05T14:55:00+00:00', trade_date: session, source: '5m' }
const bars = [
  { time: at('2026-11-04T20:55:00Z'), open: 349, high: 999, low: 1, close: 349.5, volume: 7777 },
  { time: at('2026-11-05T14:30:00Z'), open: 350, high: 353, low: 349, close: 352, volume: 100 },
  { time: at('2026-11-05T14:50:00Z'), open: 352.9, high: 353.86, low: 351.9, close: 352.01, volume: 50 },
]

test('a live price uses only today\'s bars for the session range', () => {
  assert.deepEqual(toLivePrice(quote, bars, session), {
    ticker: 'AAPL', price: 352.01, changePercent: 0.7, asOf: quote.as_of, open: 350, high: 353.86, low: 349, volume: 150,
  })
})

test('a late evening bar counts toward its New York session, not the UTC date', () => {
  const evening = { time: at('2026-11-06T00:30:00Z'), open: 1, high: 2, low: 1, close: 2, volume: 9 }
  assert.equal(toLivePrice(quote, [evening], session).volume, 9)
})

test('a quote from an earlier session is not a live price', () => {
  assert.equal(toLivePrice({ ...quote, trade_date: '2026-11-04' }, bars, session), null)
})

test('bars not read yet still give the price, without inventing a range', () => {
  assert.deepEqual(toLivePrice(quote, undefined, session), {
    ticker: 'AAPL', price: 352.01, changePercent: 0.7, asOf: quote.as_of, open: null, high: null, low: null, volume: null,
  })
})

const row = { ticker: 'AAPL', date: '2026-11-04', open: 341, high: 354, low: 340, close: 349.51, volume: 88000000, chg_pct: 5.4, rel_vol: 1.8,
  high_52w: 400, pct_from_high: -12, low_52w: 200, pct_from_low: 74, sma_20: 342.91, sma_200: 297.85, dist_200: 17.3, wsma_200: 224.1, dist_200w: 56 }
const live = toLivePrice(quote, bars, session)

test('All Tickers takes today\'s prices and keeps its daily averages', () => {
  const result = withLiveOverview(row, live, session)
  assert.equal(result.close, 352.01)
  assert.equal(result.date, session)
  assert.equal(result.chg_pct, 0.7)
  assert.equal(result.volume, 150)
  assert.equal(result.open, 350)
  assert.equal(result.sma_20, 342.91)
  assert.equal(result.sma_200, 297.85)
  assert.equal(result.rel_vol, null, 'part-day volume has no fair comparison')
  assert.equal(result.pct_from_high.toFixed(2), '-12.00')
  assert.equal(result.dist_200.toFixed(1), '18.2')
})

test('a row with no live price stays exactly on the last close', () => {
  assert.equal(withLiveOverview(row, undefined, session), row)
  const screening = { ticker: 'AAPL', values: { price: 349.57 } }
  assert.equal(withLiveScreeningRow(screening, undefined), screening)
  const fact = { value: { close: 766.81, return1: -0.0157 } }
  assert.equal(withLiveFact(fact, undefined), fact)
})

test('bars not read yet leave the last close range in place rather than blanks', () => {
  const result = withLiveOverview(row, toLivePrice(quote, undefined, session), session)
  assert.equal(result.close, 352.01)
  assert.equal(result.open, 341)
  assert.equal(result.volume, 88000000)
})

test('the screener shows today\'s price and keeps every other value', () => {
  const screening = { ticker: 'AAPL', values: { price: 349.57, change: 0.0531, volume: 70494196.33, rsi: 55 } }
  const values = withLiveScreeningRow(screening, live).values
  assert.equal(values.price, 352.01)
  assert.equal(values.change.toFixed(4), '0.0070', 'a fraction, as the catalog declares it')
  assert.equal(values.volume, 150)
  assert.equal(values.rsi, 55)
})

test('Market Conditions pairs today\'s price with today\'s change', () => {
  const fact = { status: 'READY', value: { close: 691.95, return1: -0.0348, sma50: 723.56 } }
  assert.deepEqual(withLiveFact(fact, { ...live, price: 667.77, changePercent: -3.49 }), {
    status: 'READY', value: { close: 667.77, return1: -0.0349, sma50: 723.56 },
  })
  assert.deepEqual(withLiveFact({ status: 'UNAVAILABLE', value: null }, live), { status: 'UNAVAILABLE', value: null })
})

test('the label reads in market time', () => {
  assert.equal(formatLiveTime(quote.as_of), '9:55 AM ET')
})

test('the Options pages label the Nov 5 hole, and only that session', () => {
  const read = file => readFileSync(path.join(root, file), 'utf8')
  const flow = read('pages/OptionsFlowPage.tsx')
  assert.match(flow, /'2026-11-05': 'Options trades were not captured from 10:18:12 to 10:33:53 AM ET/)
  assert.equal(flow.match(/^  '\d{4}-\d{2}-\d{2}':/gm).length, 1)
  assert.match(flow, /<OptionSessionNotice session=\{data\.session_date\} \/>/)
  assert.match(read('pages/OptionsActivityPage.tsx'), /<OptionSessionNotice session=\{data\.session_date\} \/>/)
})

test('a row on today\'s All Tickers opens today\'s ticker page, and a past session stays pinned', () => {
  const page = readFileSync(path.join(root, 'pages/TickersOverview.tsx'), 'utf8')
  assert.match(page, /to=\{realtimeOnly && publication && !liveSession\s*\? publicationPath\(`\/ticker\/\$\{row\.ticker\}`, undefined, publication\.cohort_id, undefined, publication\.session\)\s*: `\/ticker\/\$\{row\.ticker\}`\}/)
})

test('the contract panel hides the id a browser cannot hold exactly, and Flow shows distance to two decimals', () => {
  const read = file => readFileSync(path.join(root, file), 'utf8')
  assert.doesNotMatch(read('pages/optionsScreenLibrary.ts'), /\['contract_id', 'Contract ID'\]/)
  assert.match(read('pages/OptionsFlowPage.tsx'), /\(Number\(contract\.moneyness_fraction\) \* 100\)\.toFixed\(2\)/)
})
