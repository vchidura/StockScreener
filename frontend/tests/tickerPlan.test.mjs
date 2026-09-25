import assert from 'node:assert/strict'
import test from 'node:test'
import { build } from 'esbuild'
import { fileURLToPath } from 'node:url'
import { readFileSync } from 'node:fs'

const compiled = await build({ entryPoints: [fileURLToPath(new URL('../src/pages/ticker/tickerPlan.ts', import.meta.url))],
  bundle: true, write: false, platform: 'node', format: 'esm', logLevel: 'silent' })
const { buildTradePlan, evaluatePlan } = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputFiles[0].text).toString('base64')}`)
const setup = {
  direction: { bias: 'Bullish' }, last_close: 100, technicals: { atr: 2 },
  stops: [{ price: 96, level: 'Support', source: 'Structure' }],
  targets: [{ price: 112, level: 'Resistance', source: 'Structure' }],
  ema_alignment: { confirm_interval: '1wk', multi_tf_agree: true, confirm: 'Bullish' },
  confluence: { grade: 'A', count: 3 },
}
const context = { daily: { percentile: .99 }, frames: { '1d': { status: 'READY', trend: 'Bullish Stack' } } }

test('long plan geometry depends only on the original technical setup', () => {
  const plan = buildTradePlan(setup)
  assert.ok(plan)
  assert.deepEqual([plan.entry, plan.stop, plan.target, plan.risk, plan.reward, plan.rr], [100, 96, 112, 4, 12, 3])
  assert.equal(evaluatePlan(plan, setup, 100, context).label, 'Take')
  assert.equal(evaluatePlan(plan, setup, 100, { ...context, daily: { percentile: 0 } }).label, 'Take')
})

test('short plan geometry retains the original ATR fallback', () => {
  const short = { ...setup, direction: { bias: 'Bearish' } }
  const plan = buildTradePlan(short)
  assert.equal(plan.side, 'SHORT')
  assert.deepEqual([plan.entry, plan.stop, plan.target, plan.rr], [100, 102, 96, 2])
})

test('missing and stale context cannot improve a plan verdict', () => {
  const plan = buildTradePlan(setup)
  for (const value of [null, { frames: { '1d': { status: 'STALE', trend: 'Bullish Stack' } } }]) {
    assert.equal(evaluatePlan(plan, setup, 100, value).label, 'Context unavailable')
    assert.equal(plan.target, 112)
  }
})

test('opposing published trend is a caution, not changed plan geometry', () => {
  const plan = buildTradePlan(setup)
  const verdict = evaluatePlan(plan, setup, 100, { frames: { '1d': { status: 'READY', trend: 'Bearish Stack' } } })
  assert.equal(verdict.label, 'Take with care')
  assert.match(verdict.cautions[0].detail, /countertrend/)
  assert.equal(plan.stop, 96)
})

test('invalid or neutral setups still cannot create a plan', () => {
  assert.equal(buildTradePlan({ ...setup, direction: { bias: 'Neutral' } }), null)
  assert.equal(buildTradePlan({ ...setup, last_close: 0 }), null)
})

test('current stock pages no longer request legacy discovery or cross-sectional ranks', () => {
  for (const name of ['Dashboard.tsx', 'SectorIntelligence.tsx', 'TickerDetail.tsx']) {
    const source = readFileSync(new URL(`../src/pages/${name}`, import.meta.url), 'utf8')
    assert.doesNotMatch(source, /getDiscoveryStates|getTickerDiscoveryState|getCrossSectionalSignals|getTickerCrossSectionalSignal|xsmom-1\.0|actionable LONG|actionable SHORT/)
  }
  const dashboard = readFileSync(new URL('../src/pages/Dashboard.tsx', import.meta.url), 'utf8')
  assert.doesNotMatch(dashboard, /Market Discovery|Validated Signal/)
})