import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

// R48 (Nov 11): the Flow page warns when the chain shown is older than the latest capture, or not shown at all.
const source = readFileSync(new URL('../src/pages/OptionsFlowPage.tsx', import.meta.url), 'utf8').replace(/\r/g, '')
const start = source.indexOf('export function breadthCoverageWarning')
const body = source.slice(start, source.indexOf('\n}\n', start) + 2).replace('export function', 'function')
const js = ts.transpileModule(`${body}\nexport { breadthCoverageWarning }`, {
  compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 },
}).outputText
const { breadthCoverageWarning } = await import(`data:text/javascript,${encodeURIComponent(js)}`)
const format = value => value ?? '?'

test('the page warns when the selected ticker fell back to its streamed contracts', () => {
  const warning = breadthCoverageWarning({
    selected: 'SPY', breadth: { breadth_state: 'FAILED', breadth_observed_at: '18:13', breadth_reason: 'ConnectTimeout' },
    breadth_fallback: { selected: true, streamed_only: ['SPY', 'TSLA'] }, selected_summary: { contract_count: 8 },
  }, format)
  assert.equal(warning.title, 'Full chain unavailable')
  assert.match(warning.detail, /SPY: latest chain capture failed at 18:13 \(ConnectTimeout\)\. Showing the 8 streamed contracts/)
  assert.match(warning.detail, /Streamed contracts only for TSLA\./)
})

test('the page warns when the chain shown is the last good capture', () => {
  const warning = breadthCoverageWarning({
    selected: 'SPY', coverage_source: 'BREADTH',
    breadth: { breadth_state: 'COMPLETE', breadth_observed_at: '18:08', latest_capture_state: 'FAILED', latest_capture_at: '18:13' },
  }, format)
  assert.equal(warning.title, 'Latest chain capture failed')
  assert.match(warning.detail, /capture at 18:13 failed\. Showing the chain captured at 18:08\./)
})

test('no warning with a complete, current chain', () => {
  assert.equal(breadthCoverageWarning({ selected: 'SPY', coverage_source: 'BREADTH', breadth: { breadth_state: 'COMPLETE' },
    breadth_fallback: null }, format), null)
  assert.match(source, /breadthCoverageWarning\(data, value => formatDateTime\(value\)\)/)
})
