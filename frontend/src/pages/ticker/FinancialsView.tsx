import type { EquitySecurityProfileResponse, FundamentalReport } from '../../services/api'
import { compactMoney } from './EntityRail'

const periodLabel = (report: FundamentalReport) => {
  if (report.fiscal_year === null) return report.period_end ?? '—'
  const quarter = report.fiscal_quarter ? `Q${report.fiscal_quarter}` : 'FY'
  return `${report.fiscal_year} ${quarter}`
}

const eps = (value: number | null | undefined) =>
  value === null || value === undefined || !Number.isFinite(value) ? '—' : `$${value.toFixed(2)}`

const margin = (numerator: number | string | null, denominator: number | string | null) => {
  const top = Number(numerator)
  const bottom = Number(denominator)
  if (!Number.isFinite(top) || !Number.isFinite(bottom) || bottom === 0) return '—'
  return `${((top / bottom) * 100).toFixed(1)}%`
}

export default function FinancialsView({ profile, loading }: {
  profile: EquitySecurityProfileResponse | null
  loading: boolean
}) {
  const reports = profile?.fundamental_reports ?? []

  return (
    <section className="ticker-panel">
      <header className="ticker-panel__head">
        <div>
          <h2>Fundamental reports</h2>
          <p>Filed statements as published, newest first. Nothing here is estimated or restated.</p>
        </div>
        {reports.length > 0 && <span className="ticker-panel__count">{reports.length} periods</span>}
      </header>

      {loading && reports.length === 0 ? (
        <p className="ticker-panel__empty">Loading fundamental reports…</p>
      ) : reports.length === 0 ? (
        <p className="ticker-panel__empty">No fundamental reports are stored for this ticker.</p>
      ) : (
        <div className="ticker-table-wrap">
          <table className="ticker-table">
            <thead>
              <tr>
                <th>Period</th>
                <th>Period end</th>
                <th>Revenue</th>
                <th>Gross margin</th>
                <th>Operating income</th>
                <th>Net income</th>
                <th>Diluted EPS</th>
                <th>Free cash flow</th>
                <th>Total equity</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {reports.map((report, index) => (
                <tr key={`${report.period_end ?? index}-${report.timeframe ?? ''}`}>
                  <td className="is-strong">{periodLabel(report)}</td>
                  <td>{report.period_end ?? '—'}</td>
                  <td>{compactMoney(report.revenue)}</td>
                  <td>{margin(report.gross_profit, report.revenue)}</td>
                  <td>{compactMoney(report.operating_income)}</td>
                  <td>{compactMoney(report.net_income)}</td>
                  <td>{eps(report.diluted_eps)}</td>
                  <td>{compactMoney(report.free_cash_flow)}</td>
                  <td>{compactMoney(report.total_equity)}</td>
                  <td>
                    {report.source ?? '—'}
                    {report.quality_codes?.length ? (
                      <span className="ticker-table__note">{report.quality_codes.join(' · ').toLowerCase()}</span>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
