import { useCallback, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, Navigate, useLocation, useParams } from 'react-router-dom'
import TickerDetail, { type TickerView } from '../TickerDetail'
import EntityRail from './EntityRail'
import FinancialsView from './FinancialsView'
import { usePublishPageContext } from '../../layout/pageContext'
import { getEquitySecurityProfile, getLatestQuote, getOptionEventCalendar, getTickerWorkspacePublication, type TradeSetup } from '../../services/api'
import { usePublicationQuery } from '../publicationNavigation'
import { formatQuoteTime, type SetupTimeframe } from './tickerShared'
import './ticker.css'

const VIEWS: Array<{ value: TickerView; label: string }> = [
  { value: 'overview', label: 'Overview' },
  { value: 'timeframes', label: 'Timeframes' },
  { value: 'levels', label: 'Levels' },
  { value: 'fibonacci', label: 'Fibonacci' },
  { value: 'financials', label: 'Financials' },
]

export default function TickerWorkspace() {
  const { symbol = '', view } = useParams<{ symbol: string; view?: string }>()
  const location = useLocation()
  const active = (VIEWS.find(item => item.value === view)?.value ?? 'overview') as TickerView
  const [analysisInterval, setAnalysisInterval] = useState<SetupTimeframe>('1d')
  const [analysisSetup, setAnalysisSetup] = useState<{ snapshotId?: string; setup: TradeSetup | null } | null>(null)
  const realtimeOnly = import.meta.env.VITE_REALTIME_ONLY === 'true'
  const release = usePublicationQuery(`ticker-workspace:${symbol}`, (pin, session) => getTickerWorkspacePublication(symbol, pin, session, true))
  const publication = release.data?.publication
  const snapshotId = publication?.snapshot_id
  const onAnalysisSetupChange = useCallback((setup: TradeSetup | null) => {
    setAnalysisSetup({ snapshotId, setup })
  }, [snapshotId])
  const closingView = publication?.read_mode === 'SESSION_CLOSE'
  const readsEnabled = !realtimeOnly || !!publication
  const publicationLoading = realtimeOnly && !publication && !release.error
  const queryScope = realtimeOnly ? [publication?.cohort_id, snapshotId, ...(closingView ? ['session_close'] : [])] : []
  const publicationError = realtimeOnly ? release.error : null
  const referenceWarning = publicationError ?? publication?.section_warnings?.reference ?? null
  const laterActionEvidence = closingView && publication?.action_status === 'CHANGED'
  const technicalWarning = release.error ?? ([
    laterActionEvidence ? null : publication?.section_warnings?.actions,
    closingView ? null : publication?.section_warnings?.market,
  ].filter(Boolean).join(' ') || null)
  const publicationNotice = laterActionEvidence
    ? `This saved view uses the ${publication.session} closing inputs. Newer corporate-action evidence is not included in these frozen inputs.`
    : null

  const { data: profile = null, isFetching: profileLoading } = useQuery({
    queryKey: ['equity-security-profile', symbol, ...queryScope],
    queryFn: () => getEquitySecurityProfile(symbol, snapshotId, closingView),
    enabled: !!symbol && readsEnabled,
    staleTime: 24 * 60 * 60 * 1000,
  })
  const { data: quote = null } = useQuery({
    queryKey: ['latest-quote', symbol, ...queryScope],
    queryFn: () => getLatestQuote(symbol, false, snapshotId, closingView),
    enabled: !!symbol && readsEnabled,
    refetchInterval: realtimeOnly ? false : 60_000,
  })
  const {
    data: calendar = null,
    isFetching: calendarLoading,
    isError: calendarError,
  } = useQuery({
    queryKey: ['option-event-calendar', symbol, 30, ...queryScope],
    queryFn: () => getOptionEventCalendar(symbol, 30, snapshotId, closingView),
    enabled: !!symbol && active === 'overview' && readsEnabled,
    staleTime: 15 * 60 * 1000,
  })

  const company = profile?.security?.company_name ?? null
  const change = quote?.change ?? null
  const changePercent = quote?.change_percent ?? null

  usePublishPageContext({
    eyebrow: profile?.security?.sector ?? 'Equity',
    title: company ? `${symbol.toUpperCase()} · ${company}` : symbol.toUpperCase(),
    detail: [profile?.security?.primary_exchange, profile?.security?.industry]
      .filter(Boolean)
      .join(' · ') || undefined,
    status: [
      {
        label: realtimeOnly ? 'Close' : 'Last',
        value: quote
          ? `$${quote.price.toFixed(2)}${changePercent === null ? '' : `  ${changePercent >= 0 ? '+' : ''}${changePercent.toFixed(2)}%`}`
          : '—',
        title: quote ? `As of ${formatQuoteTime(quote)}` : undefined,
        tone: change === null ? undefined : change >= 0 ? 'positive' : 'negative',
      },
    ],
    session: realtimeOnly ? '1d' : undefined,
  })

  if (view === 'scanner') {
    return <Navigate to={`/ticker/${symbol}${location.search}`} replace />
  }

  return (
    <div className="ticker-workspace">
      <nav className="ticker-tabs" aria-label="Ticker views">
        {VIEWS.map(item => (
          <Link
            key={item.value}
            to={`/ticker/${symbol}${item.value === 'overview' ? '' : `/${item.value}`}${location.search}`}
            className={item.value === active ? 'is-active' : undefined}
            aria-current={item.value === active ? 'page' : undefined}
          >
            {item.label}
          </Link>
        ))}
      </nav>

      <div className={active === 'overview' ? 'ticker-body ticker-body--with-rail' : 'ticker-body'}>
        <div className="ticker-main">
          <TickerDetail view={active} publication={publication} readsEnabled={readsEnabled}
            publicationLoading={publicationLoading}
            availabilityError={realtimeOnly ? technicalWarning : null}
            onRefreshPublication={realtimeOnly ? async () => {
              if (release.error) await release.retry()
              else await release.refresh()
            } : undefined}
            onAnalysisIntervalChange={setAnalysisInterval}
            onAnalysisSetupChange={onAnalysisSetupChange} />
          {active === 'financials' && <FinancialsView profile={profile} loading={publicationLoading || profileLoading}
            availabilityError={referenceWarning} />}
        </div>
        {active === 'overview' && (
          <EntityRail
            profile={profile}
            loading={publicationLoading || profileLoading}
            calendar={calendar}
            calendarLoading={publicationLoading || calendarLoading}
            calendarError={calendarError}
            referenceWarning={referenceWarning}
            calendarAvailabilityError={publicationError}
            publication={publication}
            publicationNotice={realtimeOnly ? publicationNotice : null}
            analysisInterval={analysisInterval}
            setup={analysisSetup?.snapshotId === snapshotId ? analysisSetup?.setup ?? null : null}
          />
        )}
      </div>
    </div>
  )
}
