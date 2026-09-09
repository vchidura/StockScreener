import { useCallback, useEffect, useState, type ReactNode } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { Activity, ChevronLeft, ChevronRight, LogIn, Menu, Moon, PanelLeftClose, PanelLeftOpen, Sun } from 'lucide-react'
import GlobalSearch from './GlobalSearch'
import { StalenessBanner } from './StalenessBanner'
import { PageContextSetter, type PageContextValue } from './pageContext'
import { formatMarketTime, sessionLabel, useSessionDate } from './sessionDate'
import {
  BRAND_MARK,
  BRAND_NAME,
  BRAND_SUFFIX,
  BRAND_TAGLINE,
  NAV_GROUPS,
  PINNED_ITEMS,
  activeTickerSymbol,
  breadcrumbFor,
} from './navigation'
import { useTheme } from '../theme/useTheme'
import './AppShell.css'

const RAIL_STORAGE_KEY = 'alphascreener.rail.collapsed'

function SessionStepper({ interval }: { interval: string }) {
  const { date, isLatest, canStepBack, canStepForward, step, reset, requestInterval } = useSessionDate()

  useEffect(() => requestInterval(interval), [interval, requestInterval])

  return (
    <div className="tm-session" role="group" aria-label="Scan session">
      <button
        type="button"
        onClick={() => step(-1)}
        disabled={!canStepBack}
        aria-label="Previous trading session"
        title="Previous trading session"
      >
        <ChevronLeft size={15} aria-hidden="true" />
      </button>
      <button
        type="button"
        className="tm-session__value"
        onClick={reset}
        disabled={isLatest}
        title={isLatest ? `Latest ${interval} session (${date || 'loading'})` : `${date} — click to return to the latest session`}
      >
        {sessionLabel(date)}
        {!isLatest && <small>Historical</small>}
      </button>
      <button
        type="button"
        onClick={() => step(1)}
        disabled={!canStepForward}
        aria-label="Next trading session"
        title="Next trading session"
      >
        <ChevronRight size={15} aria-hidden="true" />
      </button>
    </div>
  )
}

function storedCollapsed(): boolean {
  try {
    return localStorage.getItem(RAIL_STORAGE_KEY) === 'true'
  } catch {
    return false
  }
}

function MarketDataStamp() {
  const { dataAsOf, dataInterval } = useSessionDate()
  if (!dataAsOf) return null
  return (
    <div className="tm-datastamp">
      <span>Market data</span>
      <strong title={`Newest stored bar across all intervals${dataInterval ? ` (${dataInterval})` : ''}`}>
        {formatMarketTime(dataAsOf)}
      </strong>
    </div>
  )
}

export default function AppShell({ children }: { children: ReactNode }) {
  const location = useLocation()
  const { theme, toggleTheme } = useTheme()
  const [collapsed, setCollapsed] = useState(storedCollapsed)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [pageContext, setPageContext] = useState<PageContextValue | null>(null)

  useEffect(() => setDrawerOpen(false), [location.pathname])

  useEffect(() => {
    try {
      localStorage.setItem(RAIL_STORAGE_KEY, String(collapsed))
    } catch {
      // Storage can be unavailable in private browsing; the rail state still holds for the session.
    }
  }, [collapsed])

  const publishPageContext = useCallback((value: PageContextValue | null) => setPageContext(value), [])

  const symbol = activeTickerSymbol(location.pathname)
  const breadcrumb = breadcrumbFor(location.pathname)

  return (
    <div className={`tm-shell${collapsed ? ' tm-shell--collapsed' : ''}`}>
      <aside className={`tm-rail${drawerOpen ? ' is-open' : ''}`} aria-label="Primary navigation">
        <div className="tm-rail__brand">
          <NavLink to="/" className="tm-brand" title={`${BRAND_NAME} ${BRAND_SUFFIX}`}>
            <span className="tm-brand__mark">{BRAND_MARK}</span>
            <span className="tm-brand__text">
              <strong>{BRAND_NAME}</strong>
              <small>{BRAND_SUFFIX} · {BRAND_TAGLINE}</small>
            </span>
          </NavLink>
          <button
            type="button"
            className="tm-rail__toggle"
            onClick={() => setCollapsed(value => !value)}
            aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'}
            title={collapsed ? 'Expand navigation' : 'Collapse navigation'}
          >
            {collapsed ? <PanelLeftOpen size={15} /> : <PanelLeftClose size={15} />}
          </button>
        </div>

        <nav className="tm-rail__nav">
          {NAV_GROUPS.map(group => (
            <div className="tm-rail__group" key={group.id}>
              <span className="tm-rail__group-label">{group.label}</span>
              {group.items.map(item => {
                const Icon = item.icon
                return (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.end}
                    title={item.label}
                    className={({ isActive }) => `tm-rail__item${isActive ? ' is-active' : ''}`}
                  >
                    <Icon size={15} aria-hidden="true" />
                    <span>{item.label}</span>
                  </NavLink>
                )
              })}
            </div>
          ))}

          {symbol && (
            <div className="tm-rail__group">
              <span className="tm-rail__group-label">Ticker</span>
              <NavLink
                to={`/ticker/${symbol}`}
                title={symbol}
                className={({ isActive }) => `tm-rail__item${isActive ? ' is-active' : ''}`}
              >
                <Activity size={15} aria-hidden="true" />
                <span>{symbol}</span>
              </NavLink>
            </div>
          )}
        </nav>

        <div className="tm-rail__pinned">
          {PINNED_ITEMS.map(item => {
            const Icon = item.icon
            return (
              <button key={item.label} type="button" className="tm-rail__item is-disabled" disabled title={item.reason}>
                <Icon size={15} aria-hidden="true" />
                <span>{item.label}</span>
                <small>Soon</small>
              </button>
            )
          })}
        </div>
      </aside>

      {drawerOpen && (
        <button type="button" className="tm-scrim" aria-label="Close navigation" onClick={() => setDrawerOpen(false)} />
      )}

      <div className="tm-main">
        <header className="tm-topbar">
          <button
            type="button"
            className="tm-topbar__menu"
            onClick={() => setDrawerOpen(true)}
            aria-label="Open navigation"
          >
            <Menu size={16} aria-hidden="true" />
          </button>
          <nav className="tm-crumbs" aria-label="Breadcrumb">
            {breadcrumb.map((crumb, index) => (
              <span className="tm-crumbs__item" key={crumb}>
                {index > 0 && <ChevronRight size={12} aria-hidden="true" />}
                {crumb}
              </span>
            ))}
          </nav>
          <GlobalSearch />
          <div className="tm-topbar__actions">
            <button
              type="button"
              className="tm-icon-button"
              onClick={toggleTheme}
              aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
              title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
            >
              {theme === 'dark' ? <Sun size={15} aria-hidden="true" /> : <Moon size={15} aria-hidden="true" />}
            </button>
            <NavLink to="/signin" className="tm-signin">
              <LogIn size={14} aria-hidden="true" />
              <span>Sign in</span>
            </NavLink>
          </div>
        </header>

        {pageContext && (
          <section className="tm-contextbar">
            <div className="tm-contextbar__identity">
              {pageContext.eyebrow && <span className="tm-contextbar__eyebrow">{pageContext.eyebrow}</span>}
              <h1>{pageContext.title}</h1>
              {pageContext.detail && <p>{pageContext.detail}</p>}
            </div>
            <div className="tm-contextbar__status">
              {pageContext.status?.map(entry => (
                <div key={entry.label}>
                  <span>{entry.label}</span>
                  <strong
                    title={entry.title}
                    style={entry.tone ? { color: entry.tone === 'positive' ? 'var(--tm-pos)' : 'var(--tm-neg)' } : undefined}
                  >
                    {entry.value}
                  </strong>
                  {entry.note && <small>{entry.note}</small>}
                </div>
              ))}
              <MarketDataStamp />
              {pageContext.session && <SessionStepper interval={pageContext.session} />}
            </div>
          </section>
        )}

        <main className="tm-content">
          <StalenessBanner />
          <PageContextSetter.Provider value={publishPageContext}>
            {children}
          </PageContextSetter.Provider>
        </main>
      </div>
    </div>
  )
}
