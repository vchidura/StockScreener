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
  assert.match(library, /publicationPath\(`\/ticker\//)
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
    .replaceAll('import.meta.env', 'globalThis.__optionScreenerEnvironment')
  const request = { filters: [], sort: 'day_volume', descending: true, limit: 100, offset: 0 }
  try {
    globalThis.__optionScreenerEnvironment = {
      VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: ' exact-cohort ',
    }
    const api = await import(`data:text/javascript;base64,${Buffer.from(compile(source)).toString('base64')}`)
    await api.getOptionDiscoveryCatalog()
    await api.queryEligibleChain(request)
    assert.deepEqual(calls, [
      ['get', '/options/discovery-catalog', { params: { cohort_id: 'exact-cohort' } }],
      ['post', '/options/eligible-chain/query', request, { params: { cohort_id: 'exact-cohort' } }],
    ])
    calls.length = 0
    globalThis.__optionScreenerEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(api.getOptionDiscoveryCatalog(), /requires VITE_REALTIME_COHORT_ID/)
    await assert.rejects(api.queryEligibleChain(request), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__optionScreenerApi
    delete globalThis.__optionScreenerEnvironment
  }
})