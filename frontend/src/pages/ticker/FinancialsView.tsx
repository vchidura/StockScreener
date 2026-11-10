import { useState } from 'react'
import type { EquitySecurityProfileResponse } from '../../services/api'
import { compactMoney } from './EntityRail'
import { formatValue, periodLabel, statementSections, type StatementReport, type Timeframe } from './financialsModel'

const TIMEFRAMES: { key: Timeframe; label: string }[] = [
  { key: 'quarterly', label: 'Quarterly' },
  { key: 'annual', label: 'Annual' },
  { key: 'trailing_twelve_months', label: 'TTM' },
]

export default function FinancialsView({ profile, loading, availabilityError }: {
  profile: EquitySecurityProfileResponse | null
  loading: boolean
  availabilityError?: string | null
}) {
  const [timeframe, setTimeframe] = useState<Timeframe>('quarterly')
  const reports = (profile?.fundamental_reports ?? []) as unknown as StatementReport[]
  const isEtf = profile?.security?.security_type && profile.security.security_type !== 'CS'
  const { periods, sections } = statementSections(reports, timeframe)
  const latest = periods[0]
  const security = profile?.security

  return (
    <section className="ticker-panel">
      <header className="ticker-panel__head">
        <div>
          <h2>Financials</h2>
          <p>Filed statements as published (Massive), newest filing per period. Nothing here is estimated; a later filing that restates a period replaces it.</p>
        </div>
        {reports.length > 0 && <div className="flow-segmented" role="group" aria-label="Statement period">
          {TIMEFRAMES.map(option => (
            <button key={option.key} type="button" className={timeframe === option.key ? 'active' : ''}
              onClick={() => setTimeframe(option.key)}>{option.label}</button>
          ))}
        </div>}
      </header>

      {(security || latest) && <dl className="ticker-financials-facts">
        {security?.market_cap != null && <><dt>Market cap</dt><dd>{compactMoney(security.market_cap)}</dd></>}
        {security?.free_float_percent != null && <><dt>Free float</dt><dd>{security.free_float_percent.toFixed(1)}%</dd></>}
        {latest && <><dt>Latest period</dt><dd>{latest.period_end}</dd></>}
        {latest?.filing_date && <><dt>Filed</dt><dd>{latest.filing_date}</dd></>}
      </dl>}

      {availabilityError && reports.length > 0 && <p role="alert" className="ticker-panel__empty">{availabilityError}</p>}
      {loading && reports.length === 0 ? (
        <p className="ticker-panel__empty">Loading financial statements…</p>
      ) : isEtf ? (
        <p className="ticker-panel__empty">Financial statements do not apply to ETFs.</p>
      ) : periods.length === 0 ? (
        <p role={availabilityError ? 'alert' : undefined} className="ticker-panel__empty">
          {availabilityError ?? (reports.length ? `No ${timeframe === 'trailing_twelve_months' ? 'TTM' : timeframe} statements are stored for this ticker.` : 'Financial statements are not available yet for this ticker.')}
        </p>
      ) : sections.map(section => (
        <div className="ticker-table-wrap" key={section.title}>
          <table className="ticker-table ticker-table--statement">
            <caption>{section.title}</caption>
            <thead>
              <tr>
                <th scope="col">{timeframe === 'annual' ? 'Fiscal year' : 'Period'}</th>
                {periods.map(report => <th scope="col" key={report.period_end ?? ''} title={`Period end ${report.period_end}; filed ${report.filing_date ?? 'unknown'}`}>{periodLabel(report, timeframe)}</th>)}
              </tr>
            </thead>
            <tbody>
              {section.rows.map(row => (
                <tr key={row.label}>
                  <th scope="row" className={row.emphasis ? 'is-strong' : undefined}>{row.label}</th>
                  {row.values.map((value, index) => <td key={index} className={row.emphasis ? 'is-strong' : undefined}>{formatValue(value, row.format, row.label)}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
      {periods.length > 0 && <p className="ticker-table__note">Source: Massive financials (SEC filings). Each period shows its newest filing; figures are visible from the filing date.</p>}
    </section>
  )
}
