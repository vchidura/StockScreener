import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

// G106 OV1 short list (Nov 18, user): level tooltips, dealer-sign legend labels, Overlay view, 0 DTE split inside each
// strike bar, and the factual summary line. The model is pure; the page wiring is checked from its source.
const read = path => readFileSync(new URL(path, import.meta.url), 'utf8').replace(/\r/g, '')
const compiled = ts.transpileModule(read('../src/pages/optionsExposureModel.ts'),
  { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const model = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)
const page = read('../src/pages/OptionsOverviewPage.tsx')

test('0 DTE split: only when the scope reaches past today and the capture holds today and a later expiry', () => {
  const both = [{ calendar_dte: 0 }, { calendar_dte: 1 }, { calendar_dte: 8 }]
  assert.equal(model.zeroDteSplitState('0dte', [{ calendar_dte: 0 }]), 'off')
  assert.equal(model.zeroDteSplitState('7d', both), 'split')
  assert.equal(model.zeroDteSplitState('all', both), 'split')
  // NVDA on a day without its own expiry, or any name after today's 4:00 PM ET expiry: nothing to split.
  assert.equal(model.zeroDteSplitState('all', [{ calendar_dte: 2 }, { calendar_dte: 9 }]), 'no-today')
  assert.equal(model.zeroDteSplitState('7d', [{ calendar_dte: 0 }]), 'off')
})

test('0 DTE split: today and later parts per strike come from the grid cells and each expiry\'s calendar DTE', () => {
  const expiries = [{ calendar_dte: 0 }, { calendar_dte: 3 }, { calendar_dte: 10 }]
  const grid = [[0, 0, 10, -4], [0, 1, 5, -2], [0, 2, 1, -1], [1, 1, 3, -9], [1, 0, 0, -1], [2, 7, 99, 99]]
  const parts = model.zeroDteByStrike(grid, expiries, 3)
  assert.deepEqual(parts[0], { todayCall: 10, todayPut: -4, laterCall: 6, laterPut: -3 })
  assert.deepEqual(parts[1], { todayCall: 0, todayPut: -1, laterCall: 3, laterPut: -9 })
  // A cell pointing at an expiry the read does not list is left out rather than guessed.
  assert.deepEqual(parts[2], { todayCall: 0, todayPut: 0, laterCall: 0, laterPut: 0 })
  assert.deepEqual(model.partsInUnit(parts[0], 'shares', 2), { todayCall: 5, todayPut: -2, laterCall: 3, laterPut: -1.5 })
  assert.deepEqual(model.partsInUnit(parts[0], 'usd', 2), parts[0])
})

test('0 DTE split segments: later expiries sit on the zero line, today\'s part beyond it, in every view', () => {
  const parts = { todayCall: 10, todayPut: -4, laterCall: 5, laterPut: -2 }
  assert.deepEqual(model.partSegments(parts, 'net'), [
    { kind: 'net', part: 'later', from: 0, to: 3, value: 3 }, { kind: 'net', part: 'today', from: 3, to: 9, value: 6 }])
  const split = [
    { kind: 'call', part: 'later', from: 0, to: 5, value: 5 }, { kind: 'call', part: 'today', from: 5, to: 15, value: 10 },
    { kind: 'put', part: 'later', from: 0, to: -2, value: -2 }, { kind: 'put', part: 'today', from: -2, to: -6, value: -4 }]
  assert.deepEqual(model.partSegments(parts, 'split'), split)
  assert.deepEqual(model.partSegments(parts, 'overlay'), split)
  // Delta under the convention (both sides long): the put parts stack on top of the call parts.
  assert.deepEqual(model.partSegments({ todayCall: 2, todayPut: 1, laterCall: 3, laterPut: 4 }, 'split').map(s => [s.kind, s.part, s.from, s.to]),
    [['call', 'later', 0, 3], ['call', 'today', 3, 5], ['put', 'later', 5, 9], ['put', 'today', 9, 10]])
  // Today's net and the later net point opposite ways: both show from zero, and the net gets its own marker.
  const opposite = model.strikeBar(9, -11, 'net', { todayCall: 1, todayPut: -6, laterCall: 8, laterPut: -5 })
  assert.deepEqual(opposite.segments, [
    { kind: 'net', part: 'later', from: 0, to: 3, value: 3 }, { kind: 'net', part: 'today', from: 0, to: -5, value: -5 }])
  assert.equal(opposite.net, -2)
  assert.equal(opposite.netMarker, true)
  assert.equal(model.strikeBar(15, -6, 'net', parts).netMarker, false)
  assert.deepEqual(model.barsExtent([opposite]), [-5, 3])
  // How far the parts point opposite ways (the page dots the net only where this is at least 2 px).
  assert.equal(model.opposingSize(opposite), 3)
  assert.equal(model.opposingSize(model.strikeBar(15, -6, 'net', parts)), 0)
  assert.equal(model.opposingSize(model.strikeBar(5, -2, 'split')), 2)
})

test('Overlay: the Calls vs puts bars with the net per strike as a line; Net and Calls vs puts unchanged', () => {
  const overlay = model.strikeBar(5, -2, 'overlay')
  assert.deepEqual(overlay.segments, model.strikeSegments(5, -2, 'split'))
  assert.deepEqual(model.strikeSegments(5, -2, 'overlay'), model.strikeSegments(5, -2, 'split'))
  assert.equal(overlay.net, 3)
  assert.equal(overlay.netMarker, true)
  // The existing views without the split are exactly what they were.
  assert.deepEqual(model.strikeBar(5, -2, 'net'), { segments: [{ kind: 'net', from: 0, to: 3, value: 3 }], net: 3, netMarker: false })
  assert.deepEqual(model.strikeBar(5, -2, 'split').segments, [
    { kind: 'call', from: 0, to: 5, value: 5 }, { kind: 'put', from: 0, to: -2, value: -2 }])
  assert.equal(model.strikeBar(5, -2, 'split').netMarker, false)
  assert.deepEqual(model.barsExtent([overlay, model.strikeBar(1, 4, 'overlay')]), [-2, 5])
  assert.equal(model.netLinePath([[10, 20], [15.25, 30.04], [20, 5]]), 'M10.0 20.0 L15.3 30.0 L20.0 5.0')
  assert.equal(model.netLinePath([]), '')
})

test('level tooltips: call wall, put wall and flip say what they are, with the selected weight', () => {
  const { call_wall: call, put_wall: put, flip } = model.LEVEL_DEFINITIONS
  assert.match(call, /largest call gamma exposure in the selected expiries \(dollar gamma per 1% move, with the selected weight, calls only\)/)
  assert.match(call, /Dealers are assumed long these calls, so their hedging sells into rallies toward it .* often read as resistance/)
  assert.match(call, /net bar at this strike can still be negative when puts there are larger/)
  assert.match(put, /largest put gamma exposure .* puts only\)\. Often read as support; under the convention used here dealers are short these puts, so their hedging adds to selling if price breaks below it/)
  assert.match(flip, /price where total modeled dealer gamma changes sign/)
  assert.match(flip, /81 prices from 20% below to 20% above spot and take the crossing nearest spot/)
  assert.match(flip, /Above it, hedging leans against moves; below it, hedging adds to moves/)
  assert.match(flip, /No flip.* = the sign never changes within ±20%/)
  for (const text of [call, put]) assert.doesNotMatch(text, /open-interest weighted/)
})

test('dealer-sign legend labels and the convention note', () => {
  assert.deepEqual(model.signLabels('gamma'), {
    positive: 'Dealers modeled long gamma — hedging dampens moves', negative: 'Dealers modeled short gamma — hedging amplifies moves' })
  assert.deepEqual(model.signLabels('delta'), { positive: 'Positive (dealers long)', negative: 'Negative (dealers short)' })
  assert.match(model.CONVENTION_NOTE, /dealers are long calls and short puts; open interest does not say who holds which side/)
})

const observed = '2026-11-12T11:02:07.506590-08:00' // 2:02 PM ET
const at = minutes => Date.parse(observed) + minutes * 60_000
const spy = {
  available: true, underlyer: 'SPY', scope: 'all', greek: 'gamma', spot: '758.59', observed_at: observed,
  expiring_today_share: 0.1601, quality_reasons: [],
  levels: {
    flip_spot: '755', flip_distance_to_spot_fraction: -0.0047, regime_at_spot: 'POSITIVE_GAMMA',
    call_wall: { strike: '770', distance_to_spot_fraction: 0.015 }, put_wall: { strike: '750', distance_to_spot_fraction: -0.0113 },
  },
}
const open = { nowMs: at(3), marketOpen: true }

test('summary line: spot against the flip, regime, walls, today\'s share, capture time, and no advice', () => {
  const summary = model.exposureSummary(spy, open)
  assert.match(summary.text, /^SPY 758\.59 is 0\.47% above the flip \(755\); positive gamma at spot\. /)
  assert.match(summary.text, /Call wall 770 is 1\.50% above spot; put wall 750 is 1\.13% below spot\. /)
  assert.match(summary.text, /16\.0% of captured gamma exposure \(gross\) expires today at 4:00 PM ET\. /)
  assert.match(summary.text, /Chain captured Nov 12, 2:02\sPM ET\. Context, not a trade signal\.$/)
  assert.equal(summary.stale, false)
  assert.deepEqual(summary.tokens.filter(token => token.term).map(token => [token.term, token.text]),
    [['flip', 'flip'], ['call_wall', 'Call wall'], ['put_wall', 'put wall']])
  assert.doesNotMatch(summary.text, /\b(buy|sell|actionable|should|consider|target|signal to)\b/i)
  // Negative gamma with spot under the flip; the delta view keeps the gamma levels and states delta's share.
  const below = model.exposureSummary({ ...spy, greek: 'delta', levels: { ...spy.levels, flip_spot: '761', flip_distance_to_spot_fraction: 0.0032, regime_at_spot: 'NEGATIVE_GAMMA' } }, open)
  assert.match(below.text, /^SPY 758\.59 is 0\.32% below the flip \(761\); negative gamma at spot\. /)
  assert.match(below.text, /of captured delta exposure \(gross\) expires today/)
  const none = model.exposureSummary({ ...spy, expiring_today_share: 0 }, open)
  assert.match(none.text, /No captured gamma exposure expires today\. /)
})

test('summary line: missing flip, missing walls and an unavailable view read plainly', () => {
  const noFlip = model.exposureSummary({ ...spy, quality_reasons: ['NO_FLIP_IN_SEARCH_RANGE'],
    levels: { ...spy.levels, flip_spot: null, flip_distance_to_spot_fraction: null, regime_at_spot: 'NEGATIVE_GAMMA' } }, open)
  assert.match(noFlip.text, /^No flip within ±20% of SPY 758\.59: negative gamma at spot and across that range\. /)
  assert.equal(noFlip.tokens[1].term, 'flip')
  const undetermined = model.exposureSummary({ ...spy, underlyer: 'NVDA', spot: '190.1', quality_reasons: ['INSUFFICIENT_GAMMA_CONTRACTS'],
    levels: { flip_spot: null, flip_distance_to_spot_fraction: null, regime_at_spot: 'UNDETERMINED', call_wall: null, put_wall: null } }, open)
  assert.match(undetermined.text, /^No flip for NVDA 190\.1: gamma regime at spot undetermined \(too few gamma contracts in range\)\. No call wall or put wall in range\. /)
  const callOnly = model.exposureSummary({ ...spy, levels: { ...spy.levels, put_wall: null } }, open)
  assert.match(callOnly.text, /Call wall 770 is 1\.50% above spot; no put wall \(no put gamma in range\)\. /)
  const putOnly = model.exposureSummary({ ...spy, levels: { ...spy.levels, call_wall: null } }, open)
  assert.match(putOnly.text, /No call wall \(no call gamma in range\); put wall 750 is 1\.13% below spot\. /)
  const noLevels = model.exposureSummary({ ...spy, levels: null }, open)
  assert.match(noLevels.text, /^No flip for SPY 758\.59: gamma regime at spot undetermined\. No call wall or put wall in range\. /)
  const empty = model.exposureSummary({ available: false, reason: 'NO_CONTRACTS_IN_SCOPE', underlyer: 'NVDA', scope: '0dte', greek: 'gamma', observed_at: observed }, open)
  assert.equal(empty.text.replace(/ /g, ' '), 'No levels to summarize for NVDA · 0 DTE: nothing expires today. Chain captured Nov 12, 2:02 PM ET. Context, not a trade signal.')
  const nothing = model.exposureSummary({ available: false, reason: 'NO_CHAIN_CAPTURE', underlyer: 'QQQ', scope: 'all', greek: 'gamma' }, open)
  assert.equal(nothing.text, 'No levels to summarize for QQQ · All captured: no chain capture yet. Context, not a trade signal.')
  for (const summary of [noFlip, undetermined, callOnly, putOnly, noLevels, empty, nothing]) assert.match(summary.text, /Context, not a trade signal\.$/)
})

test('summary line: a capture older than 10 minutes is stale only while the market is open', () => {
  const stale = model.exposureSummary(spy, { nowMs: at(14.5), marketOpen: true })
  assert.equal(stale.stale, true)
  assert.equal(stale.ageMinutes, 14)
  assert.match(stale.text, /Chain captured Nov 12, 2:02\sPM ET — stale, 14 min old\. Context, not a trade signal\.$/)
  assert.deepEqual(stale.tokens.filter(token => token.stale).map(token => token.text), [' — stale, 14 min old'])
  assert.equal(model.exposureSummary(spy, { nowMs: at(9.9), marketOpen: true }).stale, false)
  // After the close, on a weekend or for a past session: an old capture is simply the latest one.
  assert.equal(model.exposureSummary(spy, { nowMs: at(600), marketOpen: false }).stale, false)
  assert.equal(model.STALE_CAPTURE_MS, 600_000)
})

test('page wiring: Overlay view, legend rows, 0 DTE parts, level definitions everywhere, summary first', () => {
  assert.match(page, /pick<ExposureView>\(searchParams\.get\('view'\), \['net', 'split', 'overlay'\], 'net'\)/)
  assert.match(page, /\{ value: 'overlay', label: 'Overlay'/)
  assert.match(page, /\{ value: 'split', label: 'Calls vs puts' \}/)
  // Summary line before the controls, on the data page and the unavailable state.
  assert.equal(page.match(/<SummaryLine data=\{data\} \/>\n\s+\{controls\}/g)?.length, 2)
  assert.match(page, /marketSessionOpen\(\{ session_date: data\.session_date \}, new Date\(now\)\)/)
  // Tooltips: cards, chart markers, Largest levels tags and the summary line's terms.
  assert.match(page, /<LevelTerm kind="call_wall">Call wall<\/LevelTerm>/)
  assert.match(page, /<LevelTerm kind="put_wall">Put wall<\/LevelTerm>/)
  assert.match(page, /<LevelTerm kind="flip">Flip<\/LevelTerm>/)
  assert.match(page, /No <LevelTerm kind="flip">flip<\/LevelTerm> within ±20% of spot/)
  assert.match(page, /'ov-marker--flip', 'flip'\)/)
  assert.match(page, /'ov-marker--wall', 'call_wall'\)/)
  assert.match(page, /'ov-marker--wall', 'put_wall'\)/)
  assert.match(page, /<LevelTerm kind=\{tag === 'CALL_WALL' \? 'call_wall' : 'put_wall'\}>/)
  assert.match(page, /token\.term\s+\? <LevelTerm key=\{index\} kind=\{token\.term\}>/)
  // Touch and keyboard: a tap toggles, keyboard focus opens, Escape closes, SVG markers take Enter/Space; the bubble
  // is portalled with viewport-clamped fixed coordinates (no page-level horizontal scroll on a phone).
  assert.match(page, /event\.pointerType === 'mouse'/)
  assert.match(page, /matches\(':focus-visible'\)/)
  assert.match(page, /event\.key === 'Escape'/)
  assert.match(page, /event\.key === 'Enter' \|\| event\.key === ' '/)
  assert.match(page, /tabIndex=\{0\} role="button"/)
  assert.match(page, /createPortal\(/)
  assert.match(page, /viewWidth - width - 8/)
  // Legend rows: the dealer-sign labels and the convention note; the 0 DTE parts and the net line.
  assert.match(page, /signLabels\(greek\)/)
  assert.match(page, /CONVENTION_NOTE/)
  assert.match(page, /Expires today \(0 DTE\)/)
  assert.match(page, /Later expiries/)
  assert.match(page, /className="ov-net-line"/)
  assert.match(page, /<em \/><em>0 DTE<\/em><em>Later<\/em><em>Total<\/em>/)
  assert.match(page, /zeroDteByStrike\(data\.grid \?\? \[\], data\.expiries \?\? \[\], data\.strikes\?\.length \?\? 0\)/)
  assert.doesNotMatch(page, /actionable/i)
})
