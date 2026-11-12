import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

// Nov 12 (user): Options Flow shows total open interest instead of the never-computed matched OI change, the expiry
// chart can leave out 0 DTE and label call/put shares, and the high-activity contracts table sorts by any column.
const source = readFileSync(new URL('../src/pages/OptionsFlowPage.tsx', import.meta.url), 'utf8').replace(/\r/g, '')
const take = (marker) => {
  const start = source.indexOf(marker)
  assert.ok(start >= 0, marker)
  return source.slice(start, source.indexOf('\n}\n', start) + 2).replace(/^export function/, 'function')
}
const js = ts.transpileModule(
  `${take('export function expiryProfileRows')}\n${take('function contractSortValue')}\n${take('export function sortContracts')}\n`
  + 'export { expiryProfileRows, sortContracts }',
  { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } },
).outputText
const { expiryProfileRows, sortContracts } = await import(`data:text/javascript,${encodeURIComponent(js)}`)

const expiries = [
  { expiration_date: '2026-11-12', calendar_dte: 0, call_volume: 900, put_volume: 1100 },
  { expiration_date: '2026-11-13', calendar_dte: 1, call_volume: 300, put_volume: 100 },
  { expiration_date: '2026-11-21', calendar_dte: 9, call_volume: 50, put_volume: 50 },
]

test('0 DTE is shown by default and can be left out', () => {
  assert.deepEqual(expiryProfileRows(expiries, true).map(row => row.calendar_dte), [0, 1, 9])
  assert.deepEqual(expiryProfileRows(expiries, false).map(row => row.calendar_dte), [1, 9])
})

const contract = (strike, type, values) => ({
  contract_id: Number(strike), contract_ticker: `O:SPY${type}${strike}`, contract_type: type, expiration_date: '2026-11-21',
  calendar_dte: 9, strike: String(strike), spot: '600', day_volume: 0, open_interest: null, model_mark: null,
  display_mark: null, local_iv: null, local_delta: null, volume_open_interest_ratio: null, premium_activity: null,
  moneyness_fraction: '0.01', ...values,
})
const rows = [
  contract(600, 'CALL', { day_volume: 500, open_interest: 1000, premium_activity: '250000', local_delta: 0.5 }),
  contract(590, 'PUT', { day_volume: 900, open_interest: null, premium_activity: '90000', local_delta: -0.3 }),
  contract(610, 'CALL', { day_volume: 100, open_interest: 4000, premium_activity: '400000', local_delta: 0.2 }),
]

test('no sort keeps the server ranking', () => {
  assert.equal(sortContracts(rows, null, true), rows)
})

test('numeric columns sort both ways and missing values go last', () => {
  assert.deepEqual(sortContracts(rows, 'volume', true).map(row => row.day_volume), [900, 500, 100])
  assert.deepEqual(sortContracts(rows, 'volume', false).map(row => row.day_volume), [100, 500, 900])
  assert.deepEqual(sortContracts(rows, 'open_interest', true).map(row => row.open_interest), [4000, 1000, null])
  assert.deepEqual(sortContracts(rows, 'open_interest', false).map(row => row.open_interest), [1000, 4000, null])
  assert.deepEqual(sortContracts(rows, 'premium', true).map(row => row.premium_activity), ['400000', '250000', '90000'])
  assert.deepEqual(sortContracts(rows, 'delta', false).map(row => row.local_delta), [-0.3, 0.2, 0.5])
})

test('the contract column sorts by type then numeric strike', () => {
  assert.deepEqual(sortContracts(rows, 'contract', false).map(row => `${row.strike} ${row.contract_type}`),
    ['600 CALL', '610 CALL', '590 PUT'])
})

test('total open interest sits next to total volume; the expiry count is styled; contract rows have unique keys', () => {
  const tiles = [...source.matchAll(/<div><span>([^<{]+)<\/span><strong>/g)].map(match => match[1])
  assert.deepEqual(tiles.slice(0, 4), ['Total volume', 'Total open interest', 'Put / call volume', 'Est. premium activity'])
  assert.match(source, /<span className="flow-expiry-options__count">/)
  const css = readFileSync(new URL('../src/pages/OptionsFlowPage.css', import.meta.url), 'utf8')
  assert.match(css, /\.flow-expiry-options__count \{[^}]*font-size: 9px/)
  // Breadth rows all carry contract_id 0, so the row key must be the contract ticker.
  assert.match(source, /\.map\(contract => <tr key=\{contract\.contract_ticker\}>/)
  assert.doesNotMatch(source, /<tr key=\{contract\.contract_id\}>/)
})

test('the page shows total open interest, the 0 DTE and % options and sortable headers', () => {
  assert.doesNotMatch(source, /Matched OI change/)
  assert.match(source, /<span>Total open interest<\/span>/)
  assert.match(source, /<th>Open interest<\/th><th>Source<\/th>/)
  assert.match(source, />Include 0 DTE<\/label>/)
  assert.match(source, />Show %<\/label>/)
  assert.match(source, /sortContracts\(data\.top_contracts, contractSort\.key, contractSort\.descending\)/)
  assert.match(source, /aria-sort=/)
})
