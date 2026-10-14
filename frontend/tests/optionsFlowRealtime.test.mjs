import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8')

test('Options Activity and Flow keep the original pages and explicit unavailable states', () => {
  const app = read('../src/App.tsx')
  assert.equal(app.match(/path="\/options\/activity" element=\{<OptionsActivityPage \/>\}/g)?.length, 2)
  assert.equal(app.match(/path="\/options\/flow" element=\{<OptionsFlowPage \/>\}/g)?.length, 2)
  const activity = read('../src/pages/OptionsActivityPage.tsx')
  const flow = read('../src/pages/OptionsFlowPage.tsx')
  assert.match(activity, /optionFlowUnavailableReason\(activity\.data\?\.reason\)/)
  assert.match(activity, /new URLSearchParams\(searchParams\)/)
  assert.match(flow, /EQUITY_ONLY_COHORT/)
  assert.match(flow, /No complete analyzed option matrices were retained/)
  assert.match(flow, /new URLSearchParams\(current\)/)
  assert.doesNotMatch(activity + flow, /realtime\/option-alerts/)
})

test('Options Flow requests preserve realtime cohort identity without canonical fallback', async () => {
  const calls = []
  globalThis.__optionFlowApi = {
    get: async (...args) => {
      calls.push(args)
      return { data: { available: false, reason: 'EQUITY_ONLY_COHORT' } }
    },
  }
  const compile = source => ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const source = read('../src/services/api.ts')
    .replace("import axios from 'axios'", 'const axios = { create: () => globalThis.__optionFlowApi }')
    .replaceAll('import.meta.env', 'globalThis.__optionFlowEnvironment')
  try {
    globalThis.__optionFlowEnvironment = {
      VITE_REALTIME_ONLY: 'true', VITE_REALTIME_COHORT_ID: ' exact-cohort ',
    }
    const api = await import(`data:text/javascript;base64,${Buffer.from(compile(source)).toString('base64')}`)
    await api.getOptionFlow('AAPL', '2026-10-06')
    assert.deepEqual(calls, [[
      '/options/flow',
      { params: { underlyer: 'AAPL', session_date: '2026-10-06', cohort_id: 'exact-cohort' } },
    ]])
    calls.length = 0
    globalThis.__optionFlowEnvironment = { VITE_REALTIME_ONLY: 'true' }
    await assert.rejects(api.getOptionFlow('AAPL'), /requires VITE_REALTIME_COHORT_ID/)
    assert.equal(calls.length, 0)
  } finally {
    delete globalThis.__optionFlowApi
    delete globalThis.__optionFlowEnvironment
  }
})