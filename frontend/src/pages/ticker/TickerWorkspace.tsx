import { useQuery } from '@tanstack/react-query'
import { Link, useLocation, useParams } from 'react-router-dom'
import TickerDetail, { type TickerView } from '../TickerDetail'
import EntityRail from './EntityRail'
import FinancialsView from './FinancialsView'
import { usePublishPageContext } from '../../layout/pageContext'
import { getEquitySecurityProfile, getLatestQuote } from '../../services/api'
import { formatQuoteTime } from './tickerShared'
import './ticker.css'

const VIEWS: Array<{ value: TickerView; label: string }> = [
  { value: 'overview', label: 'Overview' },
  { value: 'timeframes', label: 'Timeframes' },
  { value: 'levels', label: 'Levels' },
  { value: 'fibonacci', label: 'Fibonacci' },
  { value: 'scanner', label: 'Scanner history' },
  { value: 'financials', label: 'Financials' },
]

export default function TickerWorkspace() {
  const { symbol = '', view } = useParams<{ symbol: string; view?: string }>()
  const location = useLocation()
  const active = (VIEWS.find(item => item.value === view)?.value ?? 'overview') as TickerView

  const { data: profile = null, isFetching: profileLoading } = useQuery({
    queryKey: ['equity-security-profile', symbol],
    queryFn: () => getEquitySecurityProfile(symbol),
    enabled: !!symbol,
    staleTime: 24 * 60 * 60 * 1000,
  })
  const { data: quote = null } = useQuery({
    queryKey: ['latest-quote', symbol],
    queryFn: () => getLatestQuote(symbol),
    enabled: !!symbol,
    refetchInterval: 60_000,
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
        label: 'Last',
        value: quote
          ? `$${quote.price.toFixed(2)}${changePercent === null ? '' : `  ${changePercent >= 0 ? '+' : ''}${changePercent.toFixed(2)}%`}`
          : '—',
        title: quote ? `As of ${formatQuoteTime(quote)}` : undefined,
        tone: change === null ? undefined : change >= 0 ? 'positive' : 'negative',
      },
    ],
  })

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
          <TickerDetail view={active} />
          {active === 'financials' && <FinancialsView profile={profile} loading={profileLoading} />}
        </div>
        {active === 'overview' && <EntityRail profile={profile} loading={profileLoading} />}
      </div>
    </div>
  )
}
