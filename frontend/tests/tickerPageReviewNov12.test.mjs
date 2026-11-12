// Nov 12 ticker page review (G102_TICKER_PAGE_REVIEW_20261112.md): TP1, TP7, TP11, TP14, TP15. TP16 and the chart legend were reverted on the user's review.
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import Module from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { build } from 'esbuild'

// Bundles one page module with React into CommonJS (react-dom/server needs require) and renders it in node.
async function bundle(entry, exports) {
  const source = `export { ${exports.join(', ')} } from ${JSON.stringify(fileURLToPath(new URL(entry, import.meta.url)))}
export { createElement } from 'react'
export { renderToStaticMarkup } from 'react-dom/server'`
  const result = await build({
    stdin: { contents: source, resolveDir: fileURLToPath(new URL('..', import.meta.url)), loader: 'js' },
    bundle: true, write: false, platform: 'node', format: 'cjs', logLevel: 'silent',
    loader: { '.css': 'empty' }, jsx: 'automatic',
    define: { 'import.meta.env': '{"VITE_REALTIME_ONLY":"true"}' },
  })
  const filename = fileURLToPath(new URL(`./bundle-${path.basename(entry)}.cjs`, import.meta.url))
  const module = new Module(filename)
  module.filename = filename
  module.paths = Module._nodeModulePaths(path.dirname(filename))
  module._compile(result.outputFiles[0].text, filename)
  return module.exports
}

const report = (timeframe, year, quarter, periodEnd, extra = {}) => ({
  timeframe, fiscal_year: year, fiscal_quarter: quarter, period_end: periodEnd, filing_date: periodEnd,
  revenue: 100e9, gross_profit: 47e9, operating_income: 30e9, net_income: 25e9, diluted_eps: 1.6,
  total_assets: 350e9, total_liabilities: 280e9, total_equity: 70e9, current_assets: 150e9,
  current_liabilities: 140e9, operating_cash_flow: 30e9, free_cash_flow: 27e9, ...extra,
})

test('TP1: the analysis timeframe selector, pill and pin are gone; analysis follows the chart', () => {
  const page = readFileSync(new URL('../src/pages/TickerDetail.tsx', import.meta.url), 'utf8')
  assert.doesNotMatch(page, /Analysis timeframe|Following chart|Pinned to|analysisPin|tk-analysis-bar/)
  assert.match(page, /const analysis = resolveAnalysisInterval\(interval, selectableSetupIntervals\)/)
  assert.match(page, /nearest published to the \$\{interval\} chart/)
  const preferences = readFileSync(new URL('../src/pages/ticker/chartPreferences.ts', import.meta.url), 'utf8')
  assert.doesNotMatch(preferences, /analysisPin/)
})

test('TP14: Financials asks for every stored period (12 quarterly, 6 annual, 12 TTM)', () => {
  const api = readFileSync(new URL('../src/services/api.ts', import.meta.url), 'utf8')
  assert.match(api, /export const FINANCIAL_REPORT_LIMIT = 40/)
  assert.match(api, /retainedTickerParams\(\{ report_limit: FINANCIAL_REPORT_LIMIT \}/)
})

test('TP15: Financials is one table with shared columns, a ratio format and visible trimming', async () => {
  const { default: FinancialsView, createElement, renderToStaticMarkup } = await bundle('../src/pages/ticker/FinancialsView.tsx', ['default'])
  const reports = [
    report('quarterly', 2026, 4, '2026-09-26'), report('quarterly', 2026, 3, '2026-06-27'),
    report('quarterly', 2025, 4, '2025-09-27', { revenue: 90e9 }),
  ]
  const profile = {
    ticker: 'AAPL', security: { security_type: 'CS', market_cap: 3e12 }, fundamental_reports: reports,
    fundamental_reports_status: 'TRIMMED_TO_WORKSPACE_BOUND', fundamental_reports_available: 30,
  }
  const html = renderToStaticMarkup(createElement(FinancialsView, { profile, loading: false }))
  assert.equal(html.match(/<table/g).length, 1)
  assert.equal(html.match(/<tbody/g).length, 3)
  assert.equal(html.match(/<col[ />]/g).length, 4)
  for (const section of ['Income statement', 'Balance sheet', 'Cash flow']) assert.match(html, new RegExp(`colgroup[^>]*>${section}<`))
  assert.match(html, /Current ratio<\/th><td[^>]*>1\.07<\/td>/)
  assert.match(html, /Revenue growth \(YoY\)<\/th><td[^>]*>11\.1%<\/td>/)
  assert.match(html, /Showing the newest 3 of 30 stored reports/)

  const adr = renderToStaticMarkup(createElement(FinancialsView, {
    profile: { ...profile, security: { security_type: 'ADRC' }, fundamental_reports_status: undefined }, loading: false,
  }))
  assert.match(adr, /<table/)
  assert.doesNotMatch(adr, /Showing the newest/)
  const fund = renderToStaticMarkup(createElement(FinancialsView, { profile: { ...profile, security: { security_type: 'ETF' } }, loading: false }))
  assert.doesNotMatch(fund, /<table/)
  assert.match(fund, /do not apply to ETFs and other funds/)

  // The period buttons use the ticker page's own segmented control, not the Options Flow stylesheet's class.
  assert.match(html, /class="tk-segment ticker-panel__segment" role="group" aria-label="Statement period"/)
  assert.match(html, /class="is-active" aria-pressed="true">Quarterly</)
  assert.doesNotMatch(html, /flow-segmented/)
})

test('TP11: volume flow shows participation bars and a CMF pressure bar; intraday RVOL is footnoted', async () => {
  const { VolumeFlowRead, createElement, renderToStaticMarkup } = await bundle('../src/pages/ticker/VolumeFlow.tsx', ['VolumeFlowRead'])
  const technicals = {
    volume_sparkline: [0.8, 1.1, 0.9, 1.4, 0.7, 1.9, 1.0, 2.8], relative_volume: 2.8, volume_trend_pct: 18,
    volume_trend_state: 'EXPANDING', cmf_20: 0.14, volume_pressure: 'ACCUMULATION',
  }
  const daily = renderToStaticMarkup(createElement(VolumeFlowRead, { technicals, intraday: false }))
  assert.equal(daily.match(/<rect /g).length, 8 + 2)
  assert.match(daily, /2\.80×</)
  assert.match(daily, /\+18% 5 vs 20 bars · expanding/)
  assert.match(daily, /\+0\.14/)
  assert.match(daily, /accumulation/)
  assert.doesNotMatch(daily, /any time of day/)
  const intraday = renderToStaticMarkup(createElement(VolumeFlowRead, { technicals, intraday: true }))
  assert.match(intraday, /2\.80×\*/)
  assert.match(intraday, /any time of day/)
  const empty = renderToStaticMarkup(createElement(VolumeFlowRead, {
    technicals: { ...technicals, volume_sparkline: [], relative_volume: null, volume_trend_pct: null, cmf_20: null, volume_pressure: 'UNAVAILABLE' },
    intraday: false,
  }))
  assert.match(empty, /No history/)
  assert.match(empty, /trend unavailable/)
})

test('TP7: every Timeframes abbreviation has a definition; the chart keeps its original labels', async () => {
  const { GLOSSARY, TIMEFRAME_TERMS, AbbreviationLegend, createElement, renderToStaticMarkup } = await bundle(
    '../src/pages/ticker/tickerGlossary.tsx', ['GLOSSARY', 'TIMEFRAME_TERMS', 'AbbreviationLegend'])
  for (const term of TIMEFRAME_TERMS) assert.ok(GLOSSARY[term]?.meaning, term)
  const html = renderToStaticMarkup(createElement(AbbreviationLegend, { terms: TIMEFRAME_TERMS }))
  assert.match(html, /<details/)
  assert.equal(html.match(/<dt>/g).length, TIMEFRAME_TERMS.length)
  const page = readFileSync(new URL('../src/pages/TickerDetail.tsx', import.meta.url), 'utf8')
  assert.match(page, /label: 'MA 50'/)
  assert.doesNotMatch(page, /Chart abbreviations|CHART_TERMS/)
})

