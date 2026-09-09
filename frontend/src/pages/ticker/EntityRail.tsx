import type { EquitySecurityProfileResponse } from '../../services/api'

export const compactMoney = (value: number | string | null | undefined) => {
  if (value === null || value === undefined) return '—'
  const amount = Number(value)
  if (!Number.isFinite(amount)) return '—'
  const magnitude = Math.abs(amount)
  const [divisor, suffix] = magnitude >= 1e12 ? [1e12, 'T']
    : magnitude >= 1e9 ? [1e9, 'B']
      : magnitude >= 1e6 ? [1e6, 'M']
        : magnitude >= 1e3 ? [1e3, 'K']
          : [1, '']
  const scaled = amount / divisor
  return `${amount < 0 ? '-' : ''}$${Math.abs(scaled).toFixed(suffix ? 2 : 0)}${suffix}`
}

const listDate = (value: string | null | undefined) => {
  if (!value) return '—'
  const parsed = new Date(`${value}T00:00:00Z`)
  return Number.isNaN(parsed.getTime())
    ? '—'
    : new Intl.DateTimeFormat(undefined, { timeZone: 'UTC', year: 'numeric', month: 'short', day: 'numeric' }).format(parsed)
}

export default function EntityRail({ profile, loading }: {
  profile: EquitySecurityProfileResponse | null
  loading: boolean
}) {
  const security = profile?.security ?? null

  const rows: Array<[string, string]> = [
    ['Issue type', security?.security_type?.replace(/_/g, ' ') ?? '—'],
    ['Exchange', security?.primary_exchange ?? '—'],
    ['Sector', security?.sector ?? '—'],
    ['Industry', security?.industry ?? security?.sic_description ?? '—'],
    ['Market cap', compactMoney(security?.market_cap)],
    ['Free float', security?.free_float_percent !== null && security?.free_float_percent !== undefined
      ? `${security.free_float_percent.toFixed(1)}%`
      : compactMoney(security?.free_float)],
    ['Listed', listDate(security?.list_date)],
  ]

  return (
    <aside className="ticker-entity">
      <div className="ticker-entity__head">
        <strong>{security?.company_name ?? profile?.ticker ?? 'Security'}</strong>
        <span>Reference data</span>
      </div>
      {loading && !security ? (
        <p className="ticker-entity__empty">Loading reference data…</p>
      ) : !security ? (
        <p className="ticker-entity__empty">No published security reference record for this ticker.</p>
      ) : (
        <dl className="ticker-entity__rows">
          {rows.map(([label, value]) => (
            <div key={label}>
              <dt>{label}</dt>
              <dd>{value}</dd>
            </div>
          ))}
        </dl>
      )}
      <div className="ticker-entity__note">
        News, insider transactions, earnings dates and institutional holdings are not integrated.
        This portal stores only price, reference and fundamental facts.
      </div>
    </aside>
  )
}
