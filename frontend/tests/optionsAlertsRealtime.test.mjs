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

test('Advanced Options V2 diagnostics never replace the original release-backed Alerts design', () => {
  const page = read('../src/pages/OptionsAlertsPage.tsx')
  assert.doesNotMatch(page, /AdvancedOptionsV2AlertsPanel|advancedV2Requested|getAdvancedOptionsV2Projection/)
  assert.match(page, /<DetectorAlertsPanel key=\{`detector-v5-\$\{activeView\}`\}/)
  assert.match(page, /<DetectorEvaluationPanel params=\{params\}/)
  assert.match(page, /<CandidateDetail key=\{selectedId\}/)
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

test('Advanced Options V2 selects only the flagged cohort-local stored release seam', async () => {
  const calls = []
  globalThis.__optionsAlertsApi = {
    get: async (...args) => {
      calls.push(args)
      if (args[0] === '/realtime/options-v2/status') {
        return { data: { version: 'advanced-options-v2-read-seam-v1', enabled: true, reason: null, stage_catalog: 'stages_v4',
          snapshot_type: null, provider_fetch_performed: false, detector_work_performed: false, publication_performed: false, pointer_advanced: false } }
      }
      if (args[0] === '/options/advanced-v2/status') {
        return { data: { data: { version: 'advanced-options-v2-read-seam-v1', enabled: false, reason: 'ADVANCED_OPTIONS_V2_DISABLED',
          stage_catalog: 'stages_v4', snapshot_type: null, provider_fetch_performed: false, detector_work_performed: false,
          publication_performed: false, pointer_advanced: false } } }
      }
      return { data: { available: true, release_id: 'disposable-release' } }
    },
  }
  const compile = source => ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__optionsAlertsApi }')
    .replaceAll('import.meta.env', 'globalThis.__optionsAlertsEnvironment')
  try {
    globalThis.__optionsAlertsEnvironment = {
      VITE_REALTIME_ONLY: 'true',
      VITE_REALTIME_OPTIONS_V2: 'true',
      VITE_REALTIME_COHORT_ID: ' disposable-cohort ',
    }
    const api = await import(`data:text/javascript;base64,${Buffer.from(compile(source)).toString('base64')}#advanced-v2`)
    const status = await api.getAdvancedOptionsV2Status()
    const result = await api.getAdvancedOptionsV2Projection('OPTIONS_ALERTS')
    assert.equal(status.enabled, true)
    assert.equal(result.release_id, 'disposable-release')
    assert.deepEqual(calls, [
      ['/realtime/options-v2/status', { params: { cohort_id: 'disposable-cohort' } }],
      ['/realtime/options-v2/projections/OPTIONS_ALERTS', { params: { cohort_id: 'disposable-cohort' } }],
    ])
    calls.length = 0
    globalThis.__optionsAlertsEnvironment.VITE_REALTIME_OPTIONS_V2 = 'false'
    assert.equal((await api.getAdvancedOptionsV2Status()).enabled, false)
    await assert.rejects(
      api.getAdvancedOptionsV2Projection('OPTIONS_ALERTS'),
      /stored reads are disabled/,
    )
    assert.deepEqual(calls, [['/options/advanced-v2/status']])
  } finally {
    delete globalThis.__optionsAlertsApi
    delete globalThis.__optionsAlertsEnvironment
  }
})