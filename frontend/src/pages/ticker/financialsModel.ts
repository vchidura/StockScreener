// Financials tab model (G37, Nov 10): filed statements as published, newest filing per period, periods as columns.
// Pure functions so the page logic is tested without a browser (tests/financialsModel.test.mjs).

export type Timeframe = 'quarterly' | 'annual' | 'trailing_twelve_months'

export interface StatementReport {
  timeframe: string | null
  fiscal_year: number | null
  fiscal_quarter: number | null
  period_end: string | null
  filing_date: string | null
  availability_time?: string | null
  quality_codes?: string[] | null
  [metric: string]: unknown
}

export interface StatementRow {
  label: string
  values: (number | null)[]
  format: 'money' | 'eps' | 'percent'
  emphasis?: boolean
}

export const PERIOD_LIMIT: Record<Timeframe, number> = { quarterly: 8, annual: 5, trailing_twelve_months: 8 }

const numeric = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

/** One report per period: the newest filing (a later filing restates an earlier period's figures). */
export function latestPerPeriod(reports: StatementReport[], timeframe: Timeframe): StatementReport[] {
  const byPeriod = new Map<string, StatementReport>()
  for (const report of reports) {
    if (report.timeframe !== timeframe || !report.period_end) continue
    const current = byPeriod.get(report.period_end)
    const newer = (report.filing_date ?? '') > (current?.filing_date ?? '')
      || ((report.filing_date ?? '') === (current?.filing_date ?? '') && (report.availability_time ?? '') > (current?.availability_time ?? ''))
    if (!current || newer) byPeriod.set(report.period_end, report)
  }
  return [...byPeriod.values()].sort((left, right) => (right.period_end ?? '').localeCompare(left.period_end ?? ''))
}

export function periodLabel(report: StatementReport, timeframe: Timeframe): string {
  if (report.fiscal_year === null || report.fiscal_year === undefined) return report.period_end ?? '—'
  if (timeframe === 'annual') return `FY${report.fiscal_year}`
  const quarter = report.fiscal_quarter ? `Q${report.fiscal_quarter}` : ''
  return `${timeframe === 'trailing_twelve_months' ? 'TTM ' : ''}${quarter} ${report.fiscal_year}`.trim()
}

/** Year over year: the same fiscal quarter a year earlier (quarterly, TTM) or the prior fiscal year (annual). */
export function priorYear(report: StatementReport, all: StatementReport[]): StatementReport | undefined {
  if (report.fiscal_year === null || report.fiscal_year === undefined) return undefined
  return all.find(other => other.fiscal_year === report.fiscal_year! - 1
    && (report.timeframe === 'annual' || other.fiscal_quarter === report.fiscal_quarter))
}

const ratio = (top: unknown, bottom: unknown) => {
  const numerator = numeric(top)
  const denominator = numeric(bottom)
  return numerator === null || denominator === null || denominator === 0 ? null : numerator / denominator
}

const growth = (report: StatementReport, all: StatementReport[], metric: string) => {
  const prior = priorYear(report, all)
  const now = numeric(report[metric])
  const then = prior ? numeric(prior[metric]) : null
  return now === null || then === null || then === 0 ? null : (now - then) / Math.abs(then)
}

const sum = (...values: unknown[]) => {
  const present = values.map(numeric).filter((value): value is number => value !== null)
  return present.length ? present.reduce((total, value) => total + value, 0) : null
}

export interface StatementSection {
  title: string
  rows: StatementRow[]
}

export function statementSections(reports: StatementReport[], timeframe: Timeframe): { periods: StatementReport[]; sections: StatementSection[] } {
  const all = latestPerPeriod(reports, timeframe)
  const periods = all.slice(0, PERIOD_LIMIT[timeframe])
  const row = (label: string, format: StatementRow['format'], pick: (report: StatementReport) => number | null, emphasis = false): StatementRow =>
    ({ label, format, emphasis, values: periods.map(pick) })
  const metric = (name: string) => (report: StatementReport) => numeric(report[name])
  const sections: StatementSection[] = [
    {
      title: 'Income statement',
      rows: [
        row('Revenue', 'money', metric('revenue'), true),
        row('Revenue growth (YoY)', 'percent', report => growth(report, all, 'revenue')),
        row('Gross profit', 'money', metric('gross_profit')),
        row('Gross margin', 'percent', report => ratio(report.gross_profit, report.revenue)),
        row('Operating income', 'money', metric('operating_income')),
        row('Operating margin', 'percent', report => ratio(report.operating_income, report.revenue)),
        row('Net income', 'money', metric('net_income'), true),
        row('Net margin', 'percent', report => ratio(report.net_income, report.revenue)),
        row('Diluted EPS', 'eps', metric('diluted_eps'), true),
        row('EPS growth (YoY)', 'percent', report => growth(report, all, 'diluted_eps')),
        row('EBITDA', 'money', metric('ebitda')),
        row('R&D', 'money', metric('research_and_development')),
      ],
    },
    {
      title: 'Cash flow',
      rows: [
        row('Operating cash flow', 'money', metric('operating_cash_flow'), true),
        row('Capital expenditures', 'money', metric('capital_expenditures')),
        row('Free cash flow', 'money', metric('free_cash_flow'), true),
        row('FCF margin', 'percent', report => ratio(report.free_cash_flow, report.revenue)),
        row('Dividends paid', 'money', metric('dividends')),
      ],
    },
  ]
  if (timeframe !== 'trailing_twelve_months') {
    sections.splice(1, 0, {
      title: 'Balance sheet',
      rows: [
        row('Total assets', 'money', metric('total_assets')),
        row('Total liabilities', 'money', metric('total_liabilities')),
        row('Total equity', 'money', metric('total_equity'), true),
        row('Cash and short-term investments', 'money', report => sum(report.cash_and_equivalents, report.short_term_investments)),
        row('Total debt', 'money', report => sum(report.current_debt, report.long_term_debt)),
        row('Net cash (debt)', 'money', report => {
          const cash = sum(report.cash_and_equivalents, report.short_term_investments)
          const debt = sum(report.current_debt, report.long_term_debt)
          return cash === null || debt === null ? null : cash - debt
        }, true),
        row('Current ratio', 'percent', report => ratio(report.current_assets, report.current_liabilities)),
      ],
    })
  }
  return { periods, sections }
}

export function formatValue(value: number | null, format: StatementRow['format'], label = ''): string {
  if (value === null) return '—'
  if (format === 'eps') return `${value < 0 ? '-' : ''}$${Math.abs(value).toFixed(2)}`
  if (format === 'percent') return label === 'Current ratio' ? value.toFixed(2) : `${(value * 100).toFixed(1)}%`
  const magnitude = Math.abs(value)
  const [divisor, suffix] = magnitude >= 1e12 ? [1e12, 'T'] : magnitude >= 1e9 ? [1e9, 'B'] : magnitude >= 1e6 ? [1e6, 'M'] : [1e3, 'K']
  return `${value < 0 ? '-' : ''}$${(magnitude / divisor).toFixed(magnitude / divisor >= 100 ? 0 : 1)}${suffix}`
}
