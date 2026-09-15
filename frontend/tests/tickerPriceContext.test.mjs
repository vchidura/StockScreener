import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const source = readFileSync(new URL('../src/pages/ticker/priceContext.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const { confluenceZonesAtPrice, fibonacciPriceContext } = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)

const zone = {
  low: 48, high: 50, midpoint: 49, role: 'RESISTANCE', distance_pct: 7.7,
  strength: 'CONFLUENCE', intervals: ['1d'], families: ['fibonacci'], references: [], confirmations: [],
}
const fibonacci = {
  swing_low: 80, swing_high: 120, trend_direction: 'downtrend_retracement', progress_current_pct: 50,
  active_leg: {
    start: { type: 'low', price: 80 }, end: { type: 'high', price: 110 },
    retracement_pct: 33.33, nearest_level: '50.0%',
    levels: [{ name: '23.6%', price: 102.92 }, { name: '50.0%', price: 95 }, { name: '78.6%', price: 86.42 }],
  },
}

test('reprices zone roles and distances without modifying published evidence', () => {
  const before = structuredClone(zone)
  const [result] = confluenceZonesAtPrice([zone], 51.28)
  assert.equal(result.role, 'SUPPORT')
  assert.equal(result.distance_pct, (49 - 51.28) / 51.28 * 100)
  assert.equal(result.references, zone.references)
  assert.equal(result.confirmations, zone.confirmations)
  assert.deepEqual(zone, before)
  assert.notEqual(result, zone)
})

for (const [price, role] of [[47, 'RESISTANCE'], [48, 'ACTIVE'], [49, 'ACTIVE'], [50, 'ACTIVE'], [51, 'SUPPORT']]) {
  test(`zone role at ${price} is ${role}`, () => {
    assert.equal(confluenceZonesAtPrice([zone], price)[0].role, role)
  })
}

test('current distances select different nearby zones than the old close', () => {
  const zones = [zone, { ...zone, low: 51.25, high: 51.25, midpoint: 51.25 }, { ...zone, low: 52.28, high: 52.7, midpoint: 52.49 }]
  const current = confluenceZonesAtPrice(zones, 51.28)
  const nearest = current.filter(item => item.role === 'SUPPORT').sort((left, right) => Math.abs(left.distance_pct) - Math.abs(right.distance_pct))[0]
  assert.equal(nearest.midpoint, 51.25)
  assert.equal(current[2].role, 'RESISTANCE')
})

for (const price of [null, 0, -1, NaN, Infinity]) {
  test(`invalid current price ${price} preserves zones and leaves Fibonacci comparisons unavailable`, () => {
    const zones = [zone]
    assert.equal(confluenceZonesAtPrice(zones, price), zones)
    assert.deepEqual(fibonacciPriceContext(fibonacci, price), {
      progressCurrentPct: null, activeRetracementPct: null, nearestLevel: null, activeState: 'UNAVAILABLE',
    })
  })
}

test('rising provisional leg updates current progress, retracement and nearest level', () => {
  const before = structuredClone(fibonacci)
  const result = fibonacciPriceContext(fibonacci, 103)
  assert.ok(Math.abs(result.progressCurrentPct - 57.5) < 1e-10)
  assert.equal(result.activeRetracementPct, 7 / 30 * 100)
  assert.equal(result.nearestLevel, '23.6%')
  assert.equal(result.activeState, 'WITHIN')
  assert.deepEqual(fibonacci, before)
})

test('falling provisional leg uses the opposite direction', () => {
  const falling = {
    ...fibonacci, trend_direction: 'uptrend_retracement',
    active_leg: { ...fibonacci.active_leg, start: { type: 'high', price: 120 }, end: { type: 'low', price: 90 } },
  }
  const result = fibonacciPriceContext(falling, 96)
  assert.equal(result.progressCurrentPct, 60)
  assert.equal(result.activeRetracementPct, 20)
  assert.equal(result.nearestLevel, '50.0%')
  assert.equal(result.activeState, 'WITHIN')
  assert.equal(fibonacciPriceContext(falling, 88).activeState, 'EXTENDED')
  assert.equal(fibonacciPriceContext(falling, 120).activeState, 'RETRACED')
  assert.equal(fibonacciPriceContext(falling, 125).activeState, 'RETRACED')
})

test('an exceeded high and a fully retraced origin are distinct from an ordinary pullback', () => {
  assert.equal(fibonacciPriceContext(fibonacci, 112).activeState, 'EXTENDED')
  assert.equal(fibonacciPriceContext(fibonacci, 110).activeRetracementPct, 0)
  assert.equal(fibonacciPriceContext(fibonacci, 80).activeState, 'RETRACED')
  assert.equal(fibonacciPriceContext(fibonacci, 78).activeState, 'RETRACED')
})

test('RBLX uses 51.28 instead of the 45.48 analysis close without inventing a new high', () => {
  const rblx = {
    ...fibonacci, swing_low: 33.88, swing_high: 71.53, progress_current_pct: 30.81,
    active_leg: {
      ...fibonacci.active_leg, start: { type: 'low', price: 33.88 }, end: { type: 'high', price: 46.5 },
      retracement_pct: 8.08, levels: [{ name: '23.6%', price: 43.52 }, { name: '38.2%', price: 41.68 }],
    },
  }
  const before = structuredClone(rblx)
  const result = fibonacciPriceContext(rblx, 51.28)
  assert.equal(result.progressCurrentPct.toFixed(2), '46.22')
  assert.equal(result.activeState, 'EXTENDED')
  assert.ok(result.activeRetracementPct < 0)
  assert.deepEqual(rblx, before)
})

test('missing or zero-range active legs do not produce a fabricated zero retracement', () => {
  assert.equal(fibonacciPriceContext({ ...fibonacci, active_leg: undefined }, 100).activeState, 'UNAVAILABLE')
  const result = fibonacciPriceContext({ ...fibonacci, swing_high: 80, active_leg: { ...fibonacci.active_leg, end: { type: 'high', price: 80 } } }, 100)
  assert.equal(result.progressCurrentPct, null)
  assert.equal(result.activeRetracementPct, null)
})

test('nearest level handles absent levels and keeps the published order on ties', () => {
  assert.equal(fibonacciPriceContext({ ...fibonacci, active_leg: { ...fibonacci.active_leg, levels: [] } }, 100).nearestLevel, null)
  const tied = { ...fibonacci, active_leg: { ...fibonacci.active_leg, levels: [{ name: 'first', price: 99 }, { name: 'second', price: 101 }] } }
  assert.equal(fibonacciPriceContext(tied, 100).nearestLevel, 'first')
})