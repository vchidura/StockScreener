import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

// G106 OV1 (Nov 16): Options Overview (dealer gamma/delta exposure by strike and expiry). The model is pure; the page,
// route, menu and API reader are checked from their sources.
const read = path => readFileSync(new URL(path, import.meta.url), 'utf8').replace(/\r/g, '')
const compiled = ts.transpileModule(read('../src/pages/optionsExposureModel.ts'),
  { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const model = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
const page = read('../src/pages/OptionsOverviewPage.tsx')
const api = read('../src/services/api.ts')
const app = read('../src/App.tsx')
const navigation = read('../src/layout/navigation.ts')

test('dollars and shares: shares are dollars divided by spot, signed compact text', () => {
  assert.equal(model.inUnit(1_000_000, 'shares', 500), 2000)
  assert.equal(model.inUnit(1_000_000, 'usd', 500), 1_000_000)
  assert.equal(model.formatExposure(1_828_164_816, 'usd', 758.59), '+$1.83B')
  assert.equal(model.formatExposure(-840_246_061, 'usd', 758.59), '−$840M')
  assert.equal(model.formatExposure(-4_717_253_452, 'shares', 758.59), '−6.22M sh')
  assert.equal(model.formatExposure(1_828_164_816, 'usd', 758.59, false), '$1.83B')
  assert.equal(model.formatExposure(0, 'usd', 100), '0')
  assert.equal(model.formatCell(-1_250_000, 'usd', 100), '−1.25M')
  assert.equal(model.formatScaled(-2_500_000_000, 'usd'), '−$2.50B')
  assert.equal(model.formatSignedPercent(0.0031, 2), '+0.31%')
  assert.equal(model.formatSignedPercent(-0.024, 1), '−2.4%')
})

test('strike window keeps strikes within the band around spot; ETFs default narrower than stocks', () => {
  const rows = [90, 95, 99, 100, 101, 105, 111].map(strike => ({ strike: String(strike) }))
  assert.deepEqual(model.strikesInBand(rows, 100, 0.05).map(row => row.strike), ['95', '99', '100', '101', '105'])
  assert.equal(model.strikesInBand(rows, 100, null).length, 7)
  assert.equal(model.defaultStrikeBand('spy'), 0.05)
  assert.equal(model.defaultStrikeBand('NVDA'), 0.1)
})

test('bars: net is one bar; calls vs puts stack positives up and negatives down', () => {
  assert.deepEqual(model.strikeSegments(5, -2, 'net'), [{ kind: 'net', from: 0, to: 3, value: 3 }])
  assert.deepEqual(model.strikeSegments(5, -2, 'split'), [
    { kind: 'call', from: 0, to: 5, value: 5 }, { kind: 'put', from: 0, to: -2, value: -2 }])
  // Delta under the convention: both sides long, so the put segment stacks on top of the call segment.
  assert.deepEqual(model.strikeSegments(5, 3, 'split'), [
    { kind: 'call', from: 0, to: 5, value: 5 }, { kind: 'put', from: 5, to: 8, value: 3 }])
  assert.deepEqual(model.strikeSegments(0, 0, 'net'), [])
  assert.deepEqual(model.valueExtent([{ call: 5, put: -2 }, { call: 1, put: 4 }], 'split'), [-2, 5])
  assert.deepEqual(model.valueExtent([{ call: 5, put: -2 }, { call: 1, put: 4 }], 'net'), [0, 5])
})

test('axis ticks are round and include zero; labels and markers do not collide', () => {
  assert.deepEqual(model.niceTicks(-2.3e9, 1.9e9, 4), [-2e9, -1e9, 0, 1e9])
  assert.ok(model.niceTicks(0, 7.3, 5).includes(0))
  assert.equal(model.labelStride(272, 900, 46), 15)
  assert.equal(model.labelStride(10, 900, 46), 1)
  // Spot 758.59, flip 760.96 and put wall 760 sit a few pixels apart: their labels take separate rows.
  assert.deepEqual(model.markerRows([400, 409, 407, 560], 104), [0, 2, 1, 0])
  assert.ok(Math.abs(model.priceToX(758.59, [758, 759], [100, 110]) - 105.9) < 1e-9)
  assert.equal(model.priceToX(700, [758, 759], [100, 110]), null)
})

test('heatmap: sparse cells become a strike x expiry matrix per mode, scaled at a robust maximum', () => {
  const grid = [[0, 0, 10, -4], [1, 1, 3, -9], [1, 0, 0, -1]]
  const net = model.heatmapGrid(grid, ['95', '100'], ['2026-11-12', '2026-11-20'], 'net')
  assert.deepEqual(net.values, [[6, null], [-1, -6]])
  assert.deepEqual(model.heatmapGrid(grid, ['95', '100'], ['a', 'b'], 'put').values, [[-4, null], [-1, -9]])
  assert.equal(model.robustMaximum([1, -2, null, 3, -100], 0.5), 2)
  assert.equal(model.cellStrength(-6, 3), 1)
  assert.equal(model.cellStrength(null, 3), 0)
  assert.equal(model.nearestStrikeIndex(['760', '759', '758'], 758.59), 1)
})

test('empty and partial states say what to do instead', () => {
  assert.equal(model.unavailableText('NO_CONTRACTS_IN_SCOPE', 'NVDA', '0dte').tryScope, '7d')
  assert.match(model.unavailableText('SCOPE_EXPIRED', 'SPY', '0dte').detail, /4:00 PM ET/)
  assert.match(model.unavailableText('NO_CHAIN_CAPTURE', 'SPY', 'all').detail, /every 5 minutes/)
  const notes = model.qualityNotes(['LATEST_CAPTURE_NOT_COMPLETE', 'PROVIDER_IV_FALLBACK', 'VOLUME_WEIGHT_ESTIMATE',
    'INSUFFICIENT_CONTRACTS', 'INSUFFICIENT_GAMMA_CONTRACTS', 'PROVIDER_CROSS_CHECK_DIVERGES'],
  { underlyer: 'SPY', providerIvContracts: 37, crossCheck: -0.152 })
  assert.deepEqual(notes.map(note => note.tone), ['warning', 'note', 'note', 'warning', 'warning'])
  assert.match(notes[1].text, /^37 contracts/)
  assert.match(notes[4].text, /−15\.2%/)
  // Nov 12 QQQ 0 DTE: gross agrees (−0.2%) but calls and puts split differently (net −15.6% of gross); the
  // warning quotes the gap that tripped it, and the stale spot behind it.
  const qqq = model.qualityNotes(['SPOT_QUOTE_GAP', 'PROVIDER_CROSS_CHECK_DIVERGES'],
    { underlyer: 'QQQ', crossCheck: -0.0018, crossCheckNet: -0.1558, spotGap: -0.00171, quotePrice: '689.18', spot: '688' })
  assert.deepEqual(qqq.map(note => note.tone), ['warning', 'warning'])
  assert.match(qqq[0].text, /\(688, last equity bar\) is −0\.17% against .*\(689\.18\)/)
  assert.match(qqq[1].text, /net exposure differs .* by −15\.6% of gross .*\(gross −0\.2%\)/)
  assert.equal(model.regimeText('NEGATIVE_GAMMA'), 'Negative gamma')
})

test('route, menu and reader: /options/overview under Options, realtime Options cohort params, its own read', () => {
  assert.equal(app.match(/path="\/options\/overview" element=\{<OptionsOverviewPage \/>\}/g)?.length, 2)
  assert.match(navigation, /\{ to: '\/options\/overview', label: 'Options Overview', icon: LayoutGrid \}/)
  const reader = api.slice(api.indexOf('export const getOptionExposure'), api.indexOf('export const getOptionCandidates'))
  assert.match(reader, /requireRealtimeOptionsReads\(\)/)
  assert.match(reader, /api\.get<OptionExposureRead>\('\/options\/exposure', \{ params: optionParams\(/)
  assert.match(reader, /session_date: params\.sessionDate/)
})

test('the page states its limits and never offers a trade', () => {
  assert.match(page, /Research observation, not a trade signal/)
  assert.match(page, /dealers long calls and short puts/)
  assert.match(page, /Open interest is the previous settlement/)
  assert.match(page, /is the ETF, not the/)
  assert.match(page, /Coverage is the captured band/)
  // Phase 2 Greeks stay disabled until the API lists them as enabled.
  assert.match(page, /disabled: !enabled\.has\(item\)/)
  assert.match(page, /Second-order Greeks \(phase 2\)/)
  // Identity is never colour alone: puts are hatched, and a table view exists.
  assert.match(page, /ov-bar--hatch/)
  assert.match(page, /Show as table/)
  assert.doesNotMatch(page, /\b(Buy|Sell|Order now|Trade this)\b/)
  // Reads follow the 5-minute chain capture like the other Options pages.
  assert.match(page, /msUntilWindowRead\(Date\.now\(\), windowTimeMs\(current\.state\.data\?\.observed_at\), OPTIONS_FIRST_CHECK_MS/)
})
