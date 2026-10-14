import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8')

test('original Options Alerts page shows exact unavailable scope without validation-route substitution', () => {
  const page = read('../src/pages/OptionsAlertsPage.tsx')
  assert.match(page, /The selected realtime cohort contains no option underlyings or detector datasets/)
  assert.match(page, /optionAlertsUnavailableReason\(inventory\.data\?\.reason/)
  assert.match(page, /optionAlertsUnavailableReason\(review\.data\?\.reason\)/)
  assert.doesNotMatch(page, /realtime\/option-alerts/)
  for (const name of ['Latest Run', 'Day History', 'EOD Review']) assert.ok(page.includes(name))
})

test('all original Options Alerts requests preserve the realtime cohort', async () => {
  const calls = []
  globalThis.__optionsAlertsApi = {
    get: async (...args) => {
      calls.push(['get', ...args])
      return { data: { available: false, reason: 'EQUITY_ONLY_COHORT', data: {} } }
    },
    post: async (...args) => {
      calls.push(['post', ...args])
      return { data: { available: false, reason: 'EQUITY_ONLY_COHORT', data: {} } }
    },
  }
  const compile = source => ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__optionsAlertsApi }')
    .replaceAll('import.meta.env', 'globalThis.__optionsAlertsEnvironment')
  const candidate = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
  const cohort = { cohort_id: 'exact-cohort' }
  const evaluation = { dataset_id: 'frozen-v1', detector: 'O1' }
  const detector = { dataset_id: 'frozen-v1', scope: 'LATEST' }
  try {
    globalThis.__optionsAlertsEnvironment = {
      VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: ' exact-cohort ',
    }
    const api = await import(`data:text/javascript;base64,${Buffer.from(compile(source)).toString('base64')}`)
    await api.getOptionDetectorDatasets()
    await api.getOptionAlertSchedule()
    await api.getOptionDetectorEvaluations(evaluation)
    await api.getOptionDetectorAlerts(detector)
    await api.getOptionCandidate(candidate)
    await api.getOptionCurrentAdvancedQuotes([11, 12])
    await api.previewOptionAlert(candidate, {})
    assert.deepEqual(calls, [
      ['get', '/options/alerts/datasets', { params: cohort }],
      ['get', '/options/alerts/schedule', { params: cohort }],
      ['get', '/options/alerts/evaluations', { params: { ...evaluation, ...cohort } }],
      ['get', '/options/alerts/detector-runs', { params: { ...detector, ...cohort } }],
      ['get', `/options/candidates/${candidate}`, { params: cohort }],
      ['get', '/options/quotes/current', { params: { contract_id: [11, 12], ...cohort } }],
      ['post', `/options/alerts/preview/${candidate}`, {}, { params: cohort }],
    ])
    calls.length = 0
    globalThis.__optionsAlertsEnvironment = { VITE_REALTIME_ONLY: 'true' }
    for (const readRequest of [
      () => api.getOptionDetectorDatasets(),
      () => api.getOptionAlertSchedule(),
      () => api.getOptionDetectorEvaluations(evaluation),
      () => api.getOptionDetectorAlerts(detector),
      () => api.getOptionCandidate(candidate),
      () => api.getOptionCurrentAdvancedQuotes([11]),
      () => api.previewOptionAlert(candidate, {}),
    ]) await assert.rejects(readRequest(), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__optionsAlertsApi
    delete globalThis.__optionsAlertsEnvironment
  }
})