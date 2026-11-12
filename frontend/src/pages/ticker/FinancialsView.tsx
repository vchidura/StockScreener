import { useState } from 'react'
import type { EquitySecurityProfileResponse } from '../../services/api'
import { compactMoney } from './EntityRail'
import { formatValue, isFundType, periodLabel, statementSections, type StatementReport, type Timeframe } from './financialsModel'

const TIMEFRAMES: { key: Timeframe; label: string }[] = [
  { key: 'quarterly', label: 'Quarterly' },
  { key: 'annual', label: 'Annual' },
  { key: 'trailing_twelve_months', label: 'TTM' },
]

/** Label column width; period columns share the rest equally so every section lines up (TP15, Nov 12). */
const LABEL_WIDTH = 230
const PERIOD_MIN_WIDTH = 84

export default function FinancialsView({ profile, loading, availabilityError }: {
  profile: EquitySecurityProfileResponse | null
  loading: boolean
  availabilityError?: string | null
}) {
  const [timeframe, setTimeframe] = useState<Timeframe>('quarterly')
  const reports = (profile?.fundamental_reports ?? []) as unknown as StatementReport[]
  const isFund = isFundType(profile?.security?.security_type)
  const { periods, sections } = statementSections(reports, timeframe)
  const latest = periods[0]
  const security = profile?.security
  const trimmed = profile?.fundamental_reports_status === 'TRIMMED_TO_WORKSPACE_BOUND'

  return (
    <section className="ticker-panel">
      <header className="ticker-panel__head">
        <div>
          <h2>Financials</h2>
          <p>Filed statements as published (Massive), newest filing per period. Nothing here is estimated; a later filing that restates a period replaces it.</p>
        </div>
        {/* Nov 12: the ticker page's own segmented control; `flow-segmented` lives in the Options Flow stylesheet and
            was unstyled here unless that page had been opened first. */}
        {reports.length > 0 && <div className="tk-segment ticker-panel__segment" role="group" aria-label="Statement period">
          {TIMEFRAMES.map(option => (
            <button key={option.key} type="button" className={timeframe === option.key ? 'is-active' : undefined}
              aria-pressed={timeframe === option.key} onClick={() => setTimeframe(option.key)}>{option.label}</button>
          ))}
        </div>}
      </header>

      <div className="ticker-panel__body">
        {(security || latest) && <dl className="ticker-financials-facts">
          {security?.market_cap != null && <><dt>Market cap</dt><dd>{compactMoney(security.market_cap)}</dd></>}
          {security?.free_float_percent != null && <><dt>Free float</dt><dd>{security.free_float_percent.toFixed(1)}%</dd></>}
          {latest && <><dt>Latest period</dt><dd>{latest.period_end}</dd></>}
          {latest?.filing_date && <><dt>Filed</dt><dd>{latest.filing_date}</dd></>}
        </dl>}

        {availabilityError && reports.length > 0 && <p role="alert" className="ticker-panel__empty">{availabilityError}</p>}
        {trimmed && reports.length > 0 && (
          <p role="status" className="ticker-table__note ticker-table__note--inline">
            Showing the newest {reports.length} of {profile?.fundamental_reports_available ?? 'more'} stored reports; older periods did not fit this publication.
          </p>
        )}
        {loading && reports.length === 0 ? (
          <p className="ticker-panel__empty">Loading financial statements…</p>
        ) : isFund ? (
          <p className="ticker-panel__empty">Financial statements do not apply to ETFs and other funds.</p>
        ) : periods.length === 0 ? (
          <p role={availabilityError ? 'alert' : undefined} className="ticker-panel__empty">
            {availabilityError ?? (reports.length ? `No ${timeframe === 'trailing_twelve_months' ? 'TTM' : timeframe} statements are stored for this ticker.` : 'Financial statements are not available yet for this ticker.')}
          </p>
        ) : (
          <div className="ticker-table-wrap">
            {/* One table, one header: the three statements share column widths by construction. */}
            <table className="ticker-table ticker-table--statement" style={{ minWidth: LABEL_WIDTH + periods.length * PERIOD_MIN_WIDTH }}>
              <colgroup>
                <col style={{ width: LABEL_WIDTH }} />
                {periods.map(report => <col key={report.period_end ?? ''} />)}
              </colgroup>
              <thead>
                <tr>
                  <th scope="col">{timeframe === 'annual' ? 'Fiscal year' : 'Period'}</th>
                  {periods.map(report => <th scope="col" key={report.period_end ?? ''} title={`Period end ${report.period_end}; filed ${report.filing_date ?? 'unknown'}`}>{periodLabel(report, timeframe)}</th>)}
                </tr>
              </thead>
              {sections.map(section => (
                <tbody key={section.title}>
                  <tr className="ticker-statement__section">
                    <th scope="colgroup" colSpan={periods.length + 1}>{section.title}</th>
                  </tr>
                  {section.rows.map(row => (
                    <tr key={row.label}>
                      <th scope="row" className={row.emphasis ? 'is-strong' : undefined}>{row.label}</th>
                      {row.values.map((value, index) => <td key={index} className={row.emphasis ? 'is-strong' : undefined}>{formatValue(value, row.format)}</td>)}
                    </tr>
                  ))}
                </tbody>
              ))}
            </table>
          </div>
        )}
        {periods.length > 0 && !isFund && <p className="ticker-table__note">Source: Massive financials (SEC filings). Each period shows its newest filing; figures are visible from the filing date.</p>}
      </div>
    </section>
  )
}
