import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8')

test('Options Screener keeps its original catalog and explicit unavailable chain presentation', () => {
  const workspace = read('../src/pages/OptionsScreeningWorkspace.tsx')
  const library = read('../src/pages/OptionsScreenLibrary.tsx')
  assert.match(workspace, /getOptionDiscoveryCatalog/)
  assert.match(workspace, /OptionsChainResults/)
  assert.match(library, /queryEligibleChain\(request\)/)
  assert.match(library, /The selected realtime cohort contains no option underlyings/)
  assert.match(library, /No complete active-policy option matrix was retained/)
  assert.match(library, /query\.data\?\.available === false \? 'Data unavailable'/)
  assert.match(library, /\['session_date', 'cohort_id', 'snapshot_id'\]/)
  assert.match(library, /'screen', 'builtin', 'session_date', 'cohort_id', 'snapshot_id'/)
  assert.doesNotMatch(workspace + library, /AdvancedOptionsV2|getAdvancedOptionsV2Projection/)
  assert.match(workspace, /<OptionsChainResults catalog=\{catalog\}/)
  // Nov 12 (user): a ticker opens its Options Flow.
  assert.match(library, /<Link to=\{optionFlowPath\(row\.underlying\)\}/)
  assert.match(library, /optionsDataTierLabel\(query\.data\?\.data_tier\)/)
  assert.doesNotMatch(library, /delayed model marks/)
})

test('Options Screener GET and POST requests preserve the realtime cohort', async () => {
  const calls = []
  globalThis.__optionScreenerApi = {
    get: async (...args) => {
      calls.push(['get', ...args])
      return { data: { version: 'option_discovery_v1', categories: [], models: [], contract_filters: {} } }
    },
    post: async (...args) => {
      calls.push(['post', ...args])
      return { data: { available: false, reason: 'EQUITY_ONLY_COHORT' } }
    },
  }
  const compile = source => ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__optionScreenerApi }')
    .replace(
      "import { realtimeCohortId, realtimeOptionsCohortId, realtimeOptionsReadsEnabled } from './portalIdentity'",
      `const realtimeCohortId = () => globalThis.__optionScreenerIdentity?.equity?.cohort_id
       const realtimeOptionsCohortId = sessionDate => sessionDate
         ? globalThis.__optionScreenerIdentity?.options_session_cohorts?.[sessionDate]
         : globalThis.__optionScreenerIdentity?.options?.cohort_id
       const realtimeOptionsReadsEnabled = () => globalThis.__optionScreenerIdentity?.options_reads_enabled === true`,
    )
    .replaceAll('import.meta.env', 'globalThis.__optionScreenerEnvironment')
  const request = { filters: [], sort: 'day_volume', descending: true, limit: 100, offset: 0 }
  try {
    globalThis.__optionScreenerEnvironment = { VITE_REALTIME_ONLY: 'true' }
    globalThis.__optionScreenerIdentity = {
      equity: { cohort_id: 'equity-cohort' },
      options: { cohort_id: 'exact-cohort' },
      options_reads_enabled: true,
      options_session_cohorts: {
        '2026-10-06': 'historical-options-cohort',
      },
    }
    const api = await import(`data:text/javascript;base64,${Buffer.from(compile(source)).toString('base64')}`)
    await api.getOptionDiscoveryCatalog()
    await api.queryEligibleChain(request)
    assert.deepEqual(calls, [
      ['get', '/options/discovery-catalog', { params: { cohort_id: 'exact-cohort' } }],
      ['post', '/options/eligible-chain/query', request, { params: { cohort_id: 'exact-cohort' } }],
    ])
    calls.length = 0
    const historical = { ...request, session_date: '2026-10-06' }
    await api.queryEligibleChain(historical)
    assert.deepEqual(calls, [[
      'post',
      '/options/eligible-chain/query',
      historical,
      { params: { session_date: '2026-10-06', cohort_id: 'historical-options-cohort' } },
    ]])
    calls.length = 0
    globalThis.__optionScreenerIdentity.options = null
    await assert.rejects(api.getOptionDiscoveryCatalog(), /No Options publication/)
    await assert.rejects(api.queryEligibleChain(request), /No Options publication/)
    assert.equal(calls.length, 0)
    globalThis.__optionScreenerIdentity.options_reads_enabled = false
    await assert.rejects(
      api.getOptionDiscoveryCatalog(),
      /stored reads are disabled/,
    )
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__optionScreenerApi
    delete globalThis.__optionScreenerEnvironment
    delete globalThis.__optionScreenerIdentity
  }
})