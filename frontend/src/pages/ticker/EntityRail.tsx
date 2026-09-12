import type {
  EquitySecurityProfileResponse,
  OptionEventWindowState,
  OptionsEnvelope,
  OptionTickerCalendarData,
} from '../../services/api'

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

const eventDateTime = (event: OptionTickerCalendarData['events'][number]) => {
  const parsed = new Date(event.scheduled_time)
  return Number.isNaN(parsed.getTime())
    ? '—'
    : event.event_type === 'EARNINGS' && event.confidence === 'UNKNOWN'
      ? new Intl.DateTimeFormat(undefined, {
        timeZone: 'America/New_York',
        month: 'short',
        day: 'numeric',
      }).format(parsed)
    : new Intl.DateTimeFormat(undefined, {
      timeZone: 'America/New_York',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      timeZoneName: 'short',
    }).format(parsed)
}

const eventLabel = (eventType: string) => (
  eventType === 'FED_RATE_DECISION' ? 'Fed rate decision' : 'Earnings'
)

const stateLabel: Record<OptionEventWindowState, string> = {
  BLOCKED: 'Blocked',
  CLEAR: 'Clear now',
  UNAVAILABLE: 'Unavailable',
  NOT_APPLICABLE: 'Not applicable',
}

const nextEventLabel = (
  event: OptionTickerCalendarData['events'][number] | undefined,
  state: OptionEventWindowState,
) => {
  if (event) return eventDateTime(event)
  if (state === 'NOT_APPLICABLE') return 'Not applicable'
  if (state === 'UNAVAILABLE') return 'Unavailable'
  return 'None in 30d'
}

export default function EntityRail({
  profile,
  loading,
  calendar,
  calendarLoading,
  calendarError,
}: {
  profile: EquitySecurityProfileResponse | null
  loading: boolean
  calendar: OptionsEnvelope<OptionTickerCalendarData> | null
  calendarLoading: boolean
  calendarError: boolean
}) {
  const security = profile?.security ?? null
  const calendarData = calendar?.data ?? null
  const upcomingEvents = (calendarData?.events ?? []).filter(event => (
    event.status !== 'CANCELED'
    && new Date(event.scheduled_time).getTime() >= Date.now()
  ))
  const nextEarnings = upcomingEvents.find(event => event.event_type === 'EARNINGS')
  const nextFed = upcomingEvents.find(event => event.event_type === 'FED_RATE_DECISION')
  const calendarMessage = calendarLoading
    ? 'Loading calendar coverage…'
    : calendarError
      ? 'Calendar service could not be read.'
      : calendar?.reason === 'EVENT_CALENDAR_UNCONFIGURED'
        ? 'Calendar source is not configured.'
        : calendar?.reason === 'EVENT_CALENDAR_COVERAGE_INCOMPLETE'
          ? 'Coverage is incomplete; absence is not treated as clear.'
          : calendarData?.events.length === 0
            ? 'No dated events in the next 30 days.'
            : null

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
      <div className="ticker-entity__section-head">
        <strong>Event calendar</strong>
        <span>30-day horizon</span>
      </div>
      {calendarData && (
        <dl className="ticker-entity__rows ticker-entity__rows--calendar">
          <div>
            <dt title="Blackout checks the previous 24 hours and next 72 hours">Earnings blackout</dt>
            <dd
              className={`ticker-calendar__state is-${calendarData.earnings_state.toLowerCase()}`}
              title={`${calendarData.decision_window_start} to ${calendarData.decision_window_end}`}
            >
              {stateLabel[calendarData.earnings_state]}
            </dd>
          </div>
          <div>
            <dt title="Blackout checks the previous 24 hours and next 72 hours">Fed blackout</dt>
            <dd
              className={`ticker-calendar__state is-${calendarData.fed_state.toLowerCase()}`}
              title={`${calendarData.decision_window_start} to ${calendarData.decision_window_end}`}
            >
              {stateLabel[calendarData.fed_state]}
            </dd>
          </div>
          <div>
            <dt>Next earnings</dt>
            <dd>{nextEventLabel(nextEarnings, calendarData.earnings_state)}</dd>
          </div>
          <div>
            <dt>Next Fed decision</dt>
            <dd>{nextEventLabel(nextFed, calendarData.fed_state)}</dd>
          </div>
          <div>
            <dt>Source</dt>
            <dd>{calendarData.configured_source ?? 'Not configured'}</dd>
          </div>
          <div>
            <dt>Coverage</dt>
            <dd title={`${calendarData.coverage.length} active coverage record${calendarData.coverage.length === 1 ? '' : 's'}`}>
              {calendar?.available ? 'Complete' : 'Incomplete'}
            </dd>
          </div>
        </dl>
      )}
      {upcomingEvents.length ? (
        <div className="ticker-calendar__events">
          <span>Upcoming events</span>
          {upcomingEvents.slice(0, 4).map(event => (
            <div key={event.market_event_id} className="ticker-calendar__event">
              <strong>{eventLabel(event.event_type)}</strong>
              <time dateTime={event.scheduled_time}>{eventDateTime(event)}</time>
              {event.status !== 'SCHEDULED' && <small>{event.status.replace(/_/g, ' ')}</small>}
            </div>
          ))}
        </div>
      ) : null}
      {calendarMessage && <p className="ticker-calendar__message">{calendarMessage}</p>}
      <div className="ticker-entity__note">
        News, insider transactions and institutional holdings are not integrated.
      </div>
    </aside>
  )
}
