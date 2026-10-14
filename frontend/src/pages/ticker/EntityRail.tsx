import type {
  EquitySecurityProfileResponse,
  OptionEventWindowState,
  OptionsEnvelope,
  OptionTickerCalendarData,
  TickerWorkspacePublication,
  TradeSetup,
} from '../../services/api'
import { INTERVAL_NOUN, MARKET_TIME_ZONE, money, relativeAge, type SetupTimeframe } from './tickerShared'

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
  UNAVAILABLE: 'N/A',
  NOT_APPLICABLE: 'Not applicable',
}

const nextEventLabel = (
  event: OptionTickerCalendarData['events'][number] | undefined,
  state: OptionEventWindowState,
) => {
  if (event) return eventDateTime(event)
  if (state === 'NOT_APPLICABLE') return 'Not applicable'
  if (state === 'UNAVAILABLE') return 'N/A'
  return 'None in 30d'
}

export default function EntityRail({
  profile,
  loading,
  calendar,
  calendarLoading,
  calendarError,
  availabilityError,
  publication,
  analysisInterval = '1d',
  setup,
}: {
  profile: EquitySecurityProfileResponse | null
  loading: boolean
  calendar: OptionsEnvelope<OptionTickerCalendarData> | null
  calendarLoading: boolean
  calendarError: boolean
  availabilityError?: string | null
  publication?: TickerWorkspacePublication
  analysisInterval?: SetupTimeframe
  setup?: TradeSetup | null
}) {
  const security = profile?.security ?? null
  const evidenceRef = publication?.evidence_refs[analysisInterval]
  const calendarData = calendar?.data ?? null
  const calendarAvailable = calendar?.available === true && calendarData !== null
  const upcomingEvents = (calendarAvailable ? calendarData.events : []).filter(event => (
    event.status !== 'CANCELED'
    && new Date(event.scheduled_time).getTime() >= Date.now()
  ))
  const nextEarnings = upcomingEvents.find(event => event.event_type === 'EARNINGS')
  const nextFed = upcomingEvents.find(event => event.event_type === 'FED_RATE_DECISION')
  const calendarMessage = calendarLoading
    ? 'Loading calendar coverage…'
    : calendarError || !calendarAvailable
      ? availabilityError ?? 'N/A'
      : calendarData.events.length === 0
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
      {availabilityError && security && <p role="alert" className="ticker-entity__empty">{availabilityError}</p>}
      {loading && !security ? (
        <p className="ticker-entity__empty">Loading reference data…</p>
      ) : !security ? (
        <p role={availabilityError ? 'alert' : undefined} className="ticker-entity__empty">{availabilityError ?? 'No published security reference record for this ticker.'}</p>
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
      {calendarAvailable && (
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
            <dd>{calendarData.configured_source ?? 'N/A'}</dd>
          </div>
          <div>
            <dt>Coverage</dt>
            <dd title={`${calendarData.coverage.length} active coverage record${calendarData.coverage.length === 1 ? '' : 's'}`}>
              Complete
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
      {(availabilityError || calendarMessage) && <p role={availabilityError ? 'alert' : undefined} className="ticker-calendar__message">{availabilityError ?? calendarMessage}</p>}
      <div className="ticker-entity__note">
        News, insider transactions and institutional holdings are not integrated.
      </div>
      {(setup || evidenceRef) && <details className="tk-analysis-provenance" aria-label="Run details">
        <summary>Run details</summary>
        <dl>
          <div><dt>Timeframe</dt><dd>{INTERVAL_NOUN[analysisInterval] ?? analysisInterval}</dd></div>
          {setup && <>
            <div><dt>Date</dt><dd><time dateTime={setup.date}>{setup.date}</time></dd></div>
            <div><dt>Close</dt><dd>{money(setup.last_close)}</dd></div>
            <div><dt>Confirmed against</dt><dd>{setup.ema_alignment.confirm_interval
              ? INTERVAL_NOUN[setup.ema_alignment.confirm_interval] ?? setup.ema_alignment.confirm_interval
              : 'No higher-timeframe confirmation'}</dd></div>
            {setup.computed_at && <div><dt>Computed</dt><dd><time dateTime={setup.computed_at}>
              {relativeAge(setup.computed_at) || setup.computed_at}
            </time></dd></div>}
          </>}
          {evidenceRef && ([['Market', evidenceRef.market_time], ['Observed', evidenceRef.observed_at], ['Published', evidenceRef.published_at]] as const).map(([label, timestamp]) => (
            <div key={label}><dt>{label}</dt><dd><time dateTime={timestamp}>{new Intl.DateTimeFormat(undefined, {
              timeZone: MARKET_TIME_ZONE, month: 'short', day: 'numeric', year: 'numeric',
              hour: 'numeric', minute: '2-digit', timeZoneName: 'short',
            }).format(new Date(timestamp))}</time></dd></div>
          ))}
        </dl>
        {evidenceRef && <dl><div><dt>Evidence</dt><dd><code>{evidenceRef.evidence_id}</code></dd></div>
          <div><dt>SHA-256</dt><dd><code>{evidenceRef.payload_sha256}</code></dd></div>
          <div><dt>Release</dt><dd><code>{publication.snapshot_id}</code></dd></div></dl>}
      </details>}
    </aside>
  )
}
