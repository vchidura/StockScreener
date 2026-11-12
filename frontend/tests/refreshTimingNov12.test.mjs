// Nov 12 (user): pages read when new data lands instead of on a plain timer. The developing candle is read 5 s after
// each minute (1m bars are stored ~2 s after the minute); the ticker publication is checked 40 s after each 5-minute
// mark and the Options Flow read 50 s after it, then every 10 s until the window lands; within the normal build time
// the ticker page shows no message (not "Market data is not current", and no "updating" text either).
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import Module from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { build } from 'esbuild'
import ts from 'typescript'

const read = file => readFileSync(new URL(`../src/${file}`, import.meta.url), 'utf8').replace(/\r/g, '')

async function load() {
  const result = await build({
    entryPoints: [fileURLToPath(new URL('../src/utils/refreshTiming.ts', import.meta.url))],
    bundle: true, write: false, platform: 'node', format: 'cjs', logLevel: 'silent',
  })
  const filename = fileURLToPath(new URL('./bundle-refreshTiming.cjs', import.meta.url))
  const module = new Module(filename)
  module.filename = filename
  module.paths = Module._nodeModulePaths(path.dirname(filename))
  module._compile(result.outputFiles[0].text, filename)
  return module.exports
}

const at = iso => Date.parse(iso)

test('the developing candle is read 5 s after each minute', async () => {
  const { msUntilMinuteRead } = await load()
  assert.equal(msUntilMinuteRead(at('2026-11-12T15:30:00.000Z')), 5_000)
  assert.equal(msUntilMinuteRead(at('2026-11-12T15:30:04.000Z')), 1_000)
  assert.equal(msUntilMinuteRead(at('2026-11-12T15:30:05.000Z')), 60_000)
  assert.equal(msUntilMinuteRead(at('2026-11-12T15:30:30.000Z')), 35_000)
  assert.equal(msUntilMinuteRead(at('2026-11-12T15:30:59.500Z')), 5_500)
})

test('a 5-minute page waits for its next window, then checks every 10 s until it lands', async () => {
  const { msUntilWindowRead, EQUITY_FIRST_CHECK_MS, OPTIONS_FIRST_CHECK_MS } = await load()
  const shown = at('2026-11-12T15:30:00Z')
  const equity = now => msUntilWindowRead(at(now), shown, EQUITY_FIRST_CHECK_MS, 30_000)
  assert.equal(equity('2026-11-12T15:31:00Z'), 280_000)   // current: next check 15:35:40
  assert.equal(equity('2026-11-12T15:35:10Z'), 30_000)    // the 15:35 window is due from 15:35:40
  assert.equal(equity('2026-11-12T15:35:45Z'), 10_000)    // being built: every 10 s
  assert.equal(equity('2026-11-12T15:39:30Z'), 30_000)    // late (4 min after the mark): every 30 s
  // More than two windows behind (before the open, after the close, a past session): the idle interval.
  assert.equal(msUntilWindowRead(at('2026-11-12T15:45:10Z'), shown, EQUITY_FIRST_CHECK_MS, 30_000), 30_000)
  assert.equal(msUntilWindowRead(at('2026-11-12T15:35:10Z'), null, EQUITY_FIRST_CHECK_MS, 30_000), 30_000)
  assert.equal(msUntilWindowRead(at('2026-11-12T15:35:10Z'), NaN, OPTIONS_FIRST_CHECK_MS, 60_000), 60_000)
  // Options: first check 50 s after the mark.
  assert.equal(msUntilWindowRead(at('2026-11-12T15:35:10Z'), shown, OPTIONS_FIRST_CHECK_MS, 60_000), 40_000)
  assert.equal(msUntilWindowRead(at('2026-11-12T15:36:00Z'), at('2026-11-12T15:35:00Z'), OPTIONS_FIRST_CHECK_MS, 60_000),
    290_000)
})

test('the ticker page shows no message while the next window is within its normal build time', async () => {
  const { marketWindowBuilding, windowBuildingTo } = await load()
  assert.equal(marketWindowBuilding(at('2026-11-12T15:35:20Z'), '2026-11-12T15:30:00+00:00'), true)
  assert.equal(windowBuildingTo(at('2026-11-12T15:37:29Z'), at('2026-11-12T15:30:00Z')), at('2026-11-12T15:35:00Z'))
  assert.equal(windowBuildingTo(at('2026-11-12T15:37:30Z'), at('2026-11-12T15:30:00Z')), null)  // late: the warning
  assert.equal(windowBuildingTo(at('2026-11-12T15:36:00Z'), at('2026-11-12T15:35:00Z')), null)  // current
  assert.equal(windowBuildingTo(at('2026-11-12T15:36:00Z'), at('2026-11-12T15:25:00Z')), null)  // two behind
  assert.equal(marketWindowBuilding(at('2026-11-12T15:37:30Z'), '2026-11-12T15:30:00+00:00'), false)
  assert.equal(marketWindowBuilding(at('2026-11-12T15:36:00Z'), undefined), false)
  // No "updating" text anywhere (user, Nov 12).
  assert.doesNotMatch(read('utils/refreshTiming.ts') + read('pages/ticker/TickerWorkspace.tsx'), /Updating to/)
})

test('a page shows the older of its timestamps, floored to the 5-minute mark', async () => {
  const { windowTimeMs } = await load()
  assert.equal(windowTimeMs('2026-11-12T15:35:00Z', '2026-11-12T15:36:28.4Z'), at('2026-11-12T15:35:00Z'))
  assert.equal(windowTimeMs('2026-11-12T15:35:00Z', '2026-11-12T15:32:05Z'), at('2026-11-12T15:30:00Z'))
  assert.equal(windowTimeMs('2026-11-12T15:35:00Z', undefined, null, ''), at('2026-11-12T15:35:00Z'))
  assert.equal(windowTimeMs(undefined, 'not a time'), null)
})

test('the Flow page reads by its release and, on breadth, its chain capture', async () => {
  const { windowTimeMs } = await load()
  const page = read('pages/OptionsFlowPage.tsx')
  const start = page.indexOf('export const flowWindowMs')
  const body = page.slice(start, page.indexOf(': null\n', start) + 7).replace('export const', 'const')
  const js = ts.transpileModule(`${body}\nexport { flowWindowMs }`, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 },
  }).outputText
  globalThis.windowTimeMs = windowTimeMs
  const { flowWindowMs } = await import(`data:text/javascript,${encodeURIComponent(`const windowTimeMs = globalThis.windowTimeMs;\n${js}`)}`)
  const envelope = (cutoff, observed, source = 'BREADTH') => ({
    release_cutoff: cutoff, as_of: cutoff, data: { coverage_source: source, breadth: { breadth_observed_at: observed } },
  })
  assert.equal(flowWindowMs(envelope('2026-11-12T15:35:00Z', '2026-11-12T15:36:28Z')), at('2026-11-12T15:35:00Z'))
  assert.equal(flowWindowMs(envelope('2026-11-12T15:35:00Z', '2026-11-12T15:32:05Z')), at('2026-11-12T15:30:00Z'))
  assert.equal(flowWindowMs(envelope('2026-11-12T15:35:00Z', '2026-11-12T15:32:05Z', 'STREAM')), at('2026-11-12T15:35:00Z'))
  assert.equal(flowWindowMs(undefined), null)
  assert.match(page, /refetchInterval: current => msUntilWindowRead\(Date\.now\(\), flowWindowMs\(current\.state\.data\), OPTIONS_FIRST_CHECK_MS,/)
  assert.doesNotMatch(page, /refetchInterval: 60_000/)
})

test('the ticker page wires the timing in', () => {
  const navigation = read('pages/publicationNavigation.ts')
  assert.match(navigation, /msUntilWindowRead\(Date\.now\(\), publicationWindowMs\(current\.state\.data\), EQUITY_FIRST_CHECK_MS/)
  const detail = read('pages/TickerDetail.tsx')
  assert.match(detail, /queryKey: \['chart-forming'[\s\S]{0,400}refetchInterval: \(\) => msUntilMinuteRead\(Date\.now\(\)\)/)
  const workspace = read('pages/ticker/TickerWorkspace.tsx')
  assert.match(workspace, /marketWarning && !marketWindowBuilding\(Date\.now\(\), publication\?\.market_time\) \? marketWarning : null/)
})
