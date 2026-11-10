import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

const source = readFileSync(new URL('../src/pages/ticker/financialsModel.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } })
const { latestPerPeriod, statementSections, periodLabel, formatValue, priorYear } = await import(`data:text/javascript;base64,${Buffer.from(compiled.outputText).toString('base64')}`)

const report = (overrides) => ({
  timeframe: 'quarterly', fiscal_year: 2026, fiscal_quarter: 4, period_end: '2026-09-26', filing_date: '2026-11-03',
  revenue: 114.5e9, gross_profit: 54e9, operating_income: 36e9, net_income: 30e9, diluted_eps: 2.1,
  operating_cash_flow: 40e9, capital_expenditures: -3e9, free_cash_flow: 37e9,
  cash_and_equivalents: 30e9, short_term_investments: 6e9, current_debt: 10e9, long_term_debt: 80e9,
  total_assets: 400e9, total_liabilities: 290e9, total_equity: 110e9, current_assets: 150e9, current_liabilities: 160e9,
  ...overrides,
})

test('a later filing restating a period replaces the earlier one', () => {
  const original = report({ period_end: '2025-09-27', fiscal_year: 2025, filing_date: '2025-10-31', revenue: 100e9 })
  const restated = { ...original, filing_date: '2026-11-03', revenue: 101e9 }
  const rows = latestPerPeriod([original, restated, report({})], 'quarterly')
  assert.deepEqual(rows.map(row => row.period_end), ['2026-09-26', '2025-09-27'])
  assert.equal(rows[1].revenue, 101e9)
})

test('year over year compares the same fiscal quarter a year earlier', () => {
  const now = report({})
  const prior = report({ fiscal_year: 2025, period_end: '2025-09-27', revenue: 100e9, diluted_eps: 1.5 })
  const other = report({ fiscal_year: 2025, fiscal_quarter: 3, period_end: '2025-06-28', revenue: 90e9 })
  assert.equal(priorYear(now, [now, other, prior]), prior)
  const { sections } = statementSections([now, prior, other], 'quarterly')
  const income = sections.find(section => section.title === 'Income statement')
  const growth = income.rows.find(row => row.label === 'Revenue growth (YoY)')
  assert.ok(Math.abs(growth.values[0] - 0.145) < 1e-9)
})

test('TTM has no balance sheet; quarterly derives net cash and margins', () => {
  assert.deepEqual(statementSections([report({ timeframe: 'trailing_twelve_months' })], 'trailing_twelve_months')
    .sections.map(section => section.title), ['Income statement', 'Cash flow'])
  const { sections } = statementSections([report({})], 'quarterly')
  const balance = sections.find(section => section.title === 'Balance sheet')
  assert.equal(balance.rows.find(row => row.label === 'Net cash (debt)').values[0], 36e9 - 90e9)
  assert.equal(formatValue(-54e9, 'money'), '-$54.0B')
  assert.equal(formatValue(0.4716, 'percent'), '47.2%')
  assert.equal(formatValue(2.1, 'eps'), '$2.10')
  assert.equal(formatValue(null, 'money'), '—')
})

test('period labels', () => {
  assert.equal(periodLabel(report({}), 'quarterly'), 'Q4 2026')
  assert.equal(periodLabel(report({ timeframe: 'annual' }), 'annual'), 'FY2026')
  assert.equal(periodLabel(report({}), 'trailing_twelve_months'), 'TTM Q4 2026')
})
