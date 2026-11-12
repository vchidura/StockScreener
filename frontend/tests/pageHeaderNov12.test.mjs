// Nov 12 (user): every page header's right side is Market data, Mode, the session picker (or a fixed date) and refresh;
// page-specific header items are gone; the ticker close moved to the chart's stats before Period high.
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import Module from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { build } from 'esbuild'

const read = file => readFileSync(new URL(`../src/${file}`, import.meta.url), 'utf8').replace(/\r/g, '')
const shell = read('layout/AppShell.tsx')

test('the Market data stamp has one format, whether the value is a session date or a time', async () => {
  const result = await build({
    entryPoints: [fileURLToPath(new URL('../src/layout/sessionDate.tsx', import.meta.url))],
    bundle: true, write: false, platform: 'node', format: 'cjs', logLevel: 'silent', jsx: 'automatic',
    define: { 'import.meta.env': '{"VITE_REALTIME_ONLY":"true"}' },
  })
  const filename = fileURLToPath(new URL('./bundle-sessionDate.cjs', import.meta.url))
  const module = new Module(filename)
  module.filename = filename
  module.paths = Module._nodeModulePaths(path.dirname(filename))
  module._compile(result.outputFiles[0].text, filename)
  const plain = text => text.replace(/ /g, ' ')
  const { formatMarketStamp, marketSessionOpen } = module.exports
  assert.equal(plain(formatMarketStamp('2026-11-11T21:00:00+00:00')), 'Nov 11, 2026, 4:00 PM EST')
  assert.equal(plain(formatMarketStamp('2026-11-11')), 'Nov 11, 2026, 4:00 PM EST')
  assert.equal(plain(formatMarketStamp('2026-07-01')), 'Jul 1, 2026, 4:00 PM EDT')
  assert.equal(plain(formatMarketStamp('2026-11-11T20:58:50.088546+00:00')), 'Nov 11, 2026, 3:58 PM EST')

  // Mode: Real-Time only while today's session runs (9:30 AM to 4:00 PM ET); Session close off hours.
  const today = { session_date: '2026-11-12' }
  assert.equal(marketSessionOpen(today, new Date('2026-11-12T15:00:00Z')), true)   // 10:00 AM EST
  assert.equal(marketSessionOpen(today, new Date('2026-11-12T14:29:00Z')), false)  // 9:29 AM EST
  assert.equal(marketSessionOpen(today, new Date('2026-11-12T14:30:00Z')), true)   // 9:30 AM EST
  assert.equal(marketSessionOpen(today, new Date('2026-11-12T21:00:00Z')), false)  // 4:00 PM EST
  // Before today's first publication (or on a holiday) the current publication is the previous session's.
  assert.equal(marketSessionOpen({ session_date: '2026-11-11' }, new Date('2026-11-12T15:00:00Z')), false)
  assert.equal(marketSessionOpen({ session_date: '2026-11-13' }, new Date('2026-11-14T15:00:00Z')), false)  // Saturday
  assert.equal(marketSessionOpen(null, new Date('2026-11-12T15:00:00Z')), false)
})

test('the header shows one market panel and no page-specific status items', () => {
  assert.doesNotMatch(shell, /pageContext\.status\?\.map/)
  assert.doesNotMatch(shell, /function MarketDataStamp/)
  assert.match(shell, /<HeaderMarketPanel context=\{pageContext\} realtimeOnly=\{realtimeOnly\} \/>/)
  const panel = shell.slice(shell.indexOf('function HeaderMarketPanel'), shell.indexOf('export default function AppShell'))
  assert.match(panel, /<dt>Market data<\/dt>/)
  // One source for the stamp: the newest published window's cutoff (Options pages read Options').
  assert.match(panel, /const publication = optionsPage \? identity\?\.options : identity\?\.equity/)
  assert.match(panel, /const stamp = realtimeOnly \? publication\?\.cutoff \?\? null : dataAsOf/)
  assert.match(panel, /\{stamp \? formatMarketStamp\(stamp\) : '—'\}/)
  assert.match(panel, /<dt>Mode<\/dt>/)
  assert.match(panel, /const open = realtimeOnly && marketSessionOpen\(publication\)/)
  assert.match(panel, /const closed = historical \|\| \(realtimeOnly && !open\)/)
  assert.match(panel, /context\.mode \?\? \(closed \? 'Session close' : realtimeOnly \? 'Real-Time' : 'Research'\)/)
  // A date in the address that is the newest session is not a past session.
  const historical = shell.slice(shell.indexOf('function useHistoricalSelection'), shell.indexOf('function HeaderMarketPanel'))
  assert.match(historical, /date !== sessions\[sessions\.length - 1\]/)
  assert.doesNotMatch(historical, /!isLatest/)
  // A picker where the page has sessions, a fixed date otherwise; refresh everywhere.
  assert.match(panel, /context\.alertSessions \? <AlertSessionStepper/)
  assert.match(panel, /context\.session \? <SessionStepper interval=\{context\.session\} \/>/)
  assert.match(panel, /<StaticSession \/><RefreshButton historical=\{false\} \/>/)
})

test('refresh returns a past session to the latest and otherwise reloads the page data in place', () => {
  const button = shell.slice(shell.indexOf('function RefreshButton'), shell.indexOf('function StaticSession'))
  assert.match(button, /if \(historical && toLatest\) \{\s*toLatest\(\)\s*return/)
  assert.match(button, /await refreshPortalIdentity\(\)/)
  assert.match(button, /await queryClient\.invalidateQueries\(\)/)
  assert.match(button, /'Return to the latest session' : 'Refresh this page'/)
  const stepper = shell.slice(shell.indexOf('function SessionStepper'), shell.indexOf('function storedCollapsed'))
  assert.match(stepper, /<RefreshButton historical=\{!!date && sessions\.length > 0 && date !== sessions\[sessions\.length - 1\]\} toLatest=\{reset\} \/>/)
  const alerts = shell.slice(shell.indexOf('function AlertSessionStepper'), shell.indexOf('function useHistoricalSelection'))
  assert.match(alerts, /<RefreshButton historical=\{index >= 0 && index < dates\.length - 1\} toLatest=\{\(\) => choose\(dates\[dates\.length - 1\]\)\} \/>/)
  assert.equal(shell.match(/<RotateCcw /g)?.length, 1)
})

test('a ticker on the Options Screener and Options Alerts opens its Options Flow', () => {
  const presentation = read('pages/optionsAlertPresentation.ts')
  assert.match(presentation, /export const optionFlowPath = \(underlyer: string\) => `\/options\/flow\?underlyer=\$\{encodeURIComponent\(underlyer\.trim\(\)\.toUpperCase\(\)\)\}`/)
  assert.match(read('pages/OptionsScreenLibrary.tsx'), /<Link to=\{optionFlowPath\(row\.underlying\)\}/)
  const alerts = read('pages/OptionsAlertsPage.tsx')
  assert.equal(alerts.match(/<Link className="oa-row-link" to=\{optionFlowPath\(row\.underlyer\)\}/g)?.length, 2)
  // The alert's details stay one click away in the always-shown View column.
  assert.match(alerts, /onClick=\{\(\) => setPlanDetail\(row\)\}><Eye size=\{15\} \/>/)
  assert.match(read('pages/optionsAlertPresentation.ts'), /\{ key: 'details', label: 'View', group: 'Alert', locked: true \}/)
  // Options Flow selects its ticker from `underlyer`.
  assert.match(read('pages/OptionsFlowPage.tsx'), /searchParams\.get\('underlyer'\)/)
})

test('pages can name their mode; simulated Stock Alerts read Replay, not Real-Time', () => {
  assert.match(read('layout/pageContext.ts'), /mode\?: string/)
  assert.match(read('pages/StockAlertsPage.tsx'), /source === 'REPLAY' \? \{ mode: 'Replay',/)
})

test('the ticker close sits on the chart before Period high, not in the header', () => {
  const workspace = read('pages/ticker/TickerWorkspace.tsx')
  assert.doesNotMatch(workspace, /status: \[/)
  assert.doesNotMatch(workspace, /getLatestQuote/)
  const detail = read('pages/TickerDetail.tsx')
  const stats = detail.slice(detail.indexOf('<div className="tk-stats">'), detail.indexOf("{ label: 'Period low'"))
  assert.ok(stats.indexOf("{realtimeOnly ? 'Close' : 'Last'}") < stats.indexOf("label: 'Period high'"))
  assert.match(stats, /\{money\(displayPrice\)\}/)
  assert.match(stats, /As of \$\{formatQuoteTime\(latestQuote\)\}/)
  // The price itself is green or red by the day's change, left-aligned under its label and set apart.
  assert.match(stats, /className="tk-stats__close"/)
  assert.match(stats, /<strong style=\{Number\.isFinite\(priceChange\) \? \{ color: priceChange >= 0 \? POS : NEG \} : undefined\}>/)
  const css = read('pages/ticker/ticker.css')
  assert.match(css, /\.tk-stats > \.tk-stats__close \{[^}]*text-align: left/)
  assert.match(css, /\.tk-stats > \.tk-stats__close \{[^}]*border-right: 1px solid var\(--tm-line\)/)
})
