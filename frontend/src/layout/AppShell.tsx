import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react'
import { useIsFetching, useQueryClient } from '@tanstack/react-query'
import { NavLink, useLocation, useSearchParams } from 'react-router-dom'
import { refreshPortalIdentity, usePortalIdentity } from '../services/portalIdentity'
import { ChevronDown, ChevronLeft, ChevronRight, LogIn, Menu, Moon, PanelLeftClose, PanelLeftOpen, RotateCcw, Sun } from 'lucide-react'
import GlobalSearch from './GlobalSearch'
import { StalenessBanner } from './StalenessBanner'
import { PageContextSetter, type PageContextValue } from './pageContext'
import { formatMarketStamp, marketSessionOpen, sessionLabel, useSessionDate } from './sessionDate'
import {
  BRAND_MARK,
  BRAND_NAME,
  BRAND_SUFFIX,
  BRAND_TAGLINE,
  NAV_GROUPS,
  PINNED_ITEMS,
  activeTickerSymbol,
  breadcrumbFor,
  readLastStockSymbol,
  rememberStockSymbol,
  type NavItem,
} from './navigation'
import { useTheme } from '../theme/useTheme'
import './AppShell.css'
import { navigationPublicationPath } from '../pages/publicationNavigation'

const RAIL_STORAGE_KEY = 'alphascreener.rail.collapsed'
const GROUP_STORAGE_KEY = 'alphascreener.rail.expanded-groups.v1'

function itemMatchesPath(item: NavItem, pathname: string): boolean {
  if (item.activeFor === 'ticker') return activeTickerSymbol(pathname) !== null
  return item.end ? pathname === item.to : pathname.startsWith(item.to)
}

function groupForPath(pathname: string): string | null {
  return NAV_GROUPS.find(group => group.items.some(item => itemMatchesPath(item, pathname)))?.id ?? null
}

function storedExpandedGroups(pathname: string): Set<string> {
  const activeGroup = groupForPath(pathname)
  try {
    const parsed = JSON.parse(localStorage.getItem(GROUP_STORAGE_KEY) || 'null')
    if (Array.isArray(parsed)) {
      const known = new Set(NAV_GROUPS.filter(group => group.collapsible !== false).map(group => group.id))
      const restored = new Set(parsed.filter(value => typeof value === 'string' && known.has(value)))
      if (activeGroup && activeGroup !== 'console') restored.add(activeGroup)
      return restored
    }
  } catch {
    // Invalid or unavailable storage falls back to the active section only.
  }
  return new Set(activeGroup && activeGroup !== 'console' ? [activeGroup] : [])
}

/**
 * Nov 12 (user): one refresh control in every page header. On a past session it returns to the latest session (as
 * the All Tickers icon did); on the latest it reloads the page's data in place (portal identity, then every query).
 */
function RefreshButton({ historical, toLatest }: { historical: boolean; toLatest?: () => void }) {
  const queryClient = useQueryClient()
  const fetching = useIsFetching() > 0
  const realtimeOnly = import.meta.env.VITE_REALTIME_ONLY === 'true'
  const refresh = async () => {
    if (historical && toLatest) {
      toLatest()
      return
    }
    if (realtimeOnly) {
      try {
        await refreshPortalIdentity()
      } catch {
        // The identity poll retries on its own; the page's reads still refresh below.
      }
    }
    await queryClient.invalidateQueries()
  }
  const label = historical && toLatest ? 'Return to the latest session' : 'Refresh this page'
  return (
    <button type="button" className={`tm-refresh${fetching ? ' is-busy' : ''}`} onClick={() => void refresh()}
      aria-label={label} title={label}>
      <RotateCcw size={14} aria-hidden="true" />
    </button>
  )
}

/** A page without session selection shows the latest stored session as a fixed date. */
function StaticSession() {
  const { date } = useSessionDate()
  return <div className="tm-session tm-session--static" title="Latest stored trading session">
    <span className="tm-session__value">{sessionLabel(date)}</span>
  </div>
}

function SessionStepper({ interval }: { interval: string }) {
  const { date, sessions, isLatest, canStepBack, canStepForward, step, reset, requestInterval, choose, isStrategy, isTicker, isPattern, isAllTickers, error } = useSessionDate()

  useEffect(() => requestInterval(interval), [interval, requestInterval])

  return (
    <><div className="tm-session" role="group" aria-label="Scan session">
      <button
        type="button"
        onClick={() => step(-1)}
        disabled={!canStepBack}
        aria-label="Previous trading session"
        title="Previous trading session"
      >
        <ChevronLeft size={15} aria-hidden="true" />
      </button>
      {/* Nov 13 (user): every page picks its session from a dropdown of the stored sessions (as the ticker pages
          did), and a past session shows "Historical" under it (as the Options pages' button did). Choosing the
          newest session, or the refresh control, returns to the latest. */}
      <span className="tm-session__pick">
        <select className="tm-session__value"
          aria-label={isTicker ? 'Ticker session date' : isPattern ? 'Pattern Watch session date'
            : isAllTickers ? 'All Tickers session date' : isStrategy ? 'Strategy session date' : 'Session date'}
          title={isLatest ? `Latest ${interval} session (${date || 'loading'})` : `${date}: a past session`}
          value={date} disabled={!sessions.length} onChange={event => choose(event.target.value)}>
          {date && !sessions.includes(date) && <option value={date}>{sessionLabel(date)} (unavailable)</option>}
          {!date && <option value="">No stored sessions</option>}
          {[...sessions].reverse().map(session => <option key={session} value={session}>{sessionLabel(session)}</option>)}
        </select>
      </span>
      <button
        type="button"
        onClick={() => step(1)}
        disabled={!canStepForward}
        aria-label="Next trading session"
        title="Next trading session"
      >
        <ChevronRight size={15} aria-hidden="true" />
      </button>
      <RefreshButton historical={!!date && sessions.length > 0 && date !== sessions[sessions.length - 1]} toLatest={reset} />
    </div>{error && <span role="alert">{error}</span>}</>
  )
}

function storedCollapsed(): boolean {
  try {
    return localStorage.getItem(RAIL_STORAGE_KEY) === 'true'
  } catch {
    return false
  }
}

function AlertSessionStepper({ dates, selected, emptyLabel = 'No publications', resetKeys = [], datasetByDate = {}, sessionRollup = false, historicalView, historicalSource }: NonNullable<PageContextValue['alertSessions']>) {
  const [params, setParams] = useSearchParams()
  const index = dates.indexOf(selected)
  const choose = (session: string) => {
    const next = new URLSearchParams(params)
    const latest = session === dates[dates.length - 1]
    if (latest) next.delete('session_date')
    else next.set('session_date', session)
    if (historicalView) {
      if (latest) next.delete('view')
      else next.set('view', historicalView)
    }
    if (!latest && historicalSource) next.set('source', historicalSource)
    const dataset = datasetByDate[session]
    if (latest || !dataset || sessionRollup) next.delete('evaluation_dataset')
    else next.set('evaluation_dataset', dataset)
    next.delete('run')
    next.delete('offset')
    for (const key of resetKeys) next.delete(key)
    setParams(next)
  }
  return <div className="tm-session sa-session" role="group" aria-label="Alert trading session">
    <button type="button" title="Previous alert session" aria-label="Previous alert session" disabled={index <= 0} onClick={() => choose(dates[index - 1])}><ChevronLeft size={15} /></button>
    <span className="tm-session__pick">
      <select className="tm-session__value" aria-label="Alert session date" value={selected || ''} disabled={!dates.length} onChange={event => choose(event.target.value)}>
        {!dates.length && <option value="">{emptyLabel}</option>}
        {[...dates].reverse().map(date => <option key={date} value={date}>{sessionLabel(date)}</option>)}
      </select>
    </span>
    <button type="button" title="Next alert session" aria-label="Next alert session" disabled={index < 0 || index === dates.length - 1} onClick={() => choose(dates[index + 1])}><ChevronRight size={15} /></button>
    <RefreshButton historical={index >= 0 && index < dates.length - 1} toLatest={() => choose(dates[dates.length - 1])} />
  </div>
}

/** Whether the header is showing a past session (alert pages list their own sessions). A date in the address that is
 * the newest session (links between ticker pages and All Tickers carry one) is not a past session. */
function useHistoricalSelection(context: PageContextValue) {
  const { date, sessions } = useSessionDate()
  const alerts = context.alertSessions
  if (alerts) return !!alerts.selected && alerts.dates.length > 0 && alerts.selected !== alerts.dates[alerts.dates.length - 1]
  return !!context.session && !!date && sessions.length > 0 && date !== sessions[sessions.length - 1]
}

/**
 * Nov 12 (user): the same right side on every page header: Market data (newest stored bar, Eastern time), Mode under
 * it, then the session picker where the page supports one (a fixed date otherwise) and the refresh control.
 */
function HeaderMarketPanel({ context, realtimeOnly }: { context: PageContextValue; realtimeOnly: boolean }) {
  const { dataAsOf, dataInterval } = useSessionDate()
  const identity = usePortalIdentity()
  const { pathname } = useLocation()
  const historical = useHistoricalSelection(context)
  // Nov 12 (user): one stamp, one format, the latest data we have. Each page's session list used to supply it, and
  // they disagree (`/trading-sessions` gives a date only). The realtime portal shows the newest published window:
  // its cutoff, Equity's on stock pages and Options' on Options pages, updated as each window publishes.
  const optionsPage = pathname === '/options' || pathname.startsWith('/options/')
  const publication = optionsPage ? identity?.options : identity?.equity
  const stamp = realtimeOnly ? publication?.cutoff ?? null : dataAsOf
  const stampTitle = realtimeOnly
    ? `Latest published ${optionsPage ? 'Options' : 'Equity'} data window (cutoff, Eastern time)`
    : `Newest stored bar across all intervals${dataInterval ? ` (${dataInterval})` : ''}`
  // Nov 12 (user): Real-Time while the market session runs; Session close off hours (the last session's closing data
  // is what is shown) and whenever a past session is picked. The identity poll re-renders this every minute.
  const open = realtimeOnly && marketSessionOpen(publication)
  const closed = historical || (realtimeOnly && !open)
  const mode = context.mode ?? (closed ? 'Session close' : realtimeOnly ? 'Real-Time' : 'Research')
  const modeTitle = context.modeTitle ?? (historical
    ? 'A past session is selected: its closing data is shown.'
    : closed ? 'Market closed: the last session\'s closing data is shown. Real-Time from 9:30 AM to 4:00 PM ET on trading days.'
      : realtimeOnly ? 'Market open: the newest published data, updated as each window publishes.' : undefined)
  return (
    <div className="tm-market">
      <dl className="tm-market__meta">
        <div>
          <dt>Market data</dt>
          <dd title={stampTitle}>{stamp ? formatMarketStamp(stamp) : '—'}</dd>
        </div>
        <div>
          <dt>Mode</dt>
          <dd title={modeTitle} className={closed || context.mode ? 'is-alternate' : undefined}>{mode}</dd>
        </div>
      </dl>
      <div className="tm-market__controls">
        {context.alertSessions ? <AlertSessionStepper {...context.alertSessions} />
          : context.session ? <SessionStepper interval={context.session} />
            : <><StaticSession /><RefreshButton historical={false} /></>}
      </div>
    </div>
  )
}

export default function AppShell({ children, realtimeOnly = false }: { children: ReactNode; realtimeOnly?: boolean }) {
  const location = useLocation()
  const publicationPin = new URLSearchParams(location.search).get('snapshot_id') ?? undefined
  const publicationCohort = new URLSearchParams(location.search).get('cohort_id') ?? undefined
  const publicationInterval = new URLSearchParams(location.search).get('interval') === '1h' ? '1h' : '1d'
  const publicationSession = new URLSearchParams(location.search).get('session_date')
    ?? new URLSearchParams(location.search).get('evaluation_session') ?? undefined
  const pinnedPath = (path: string) => navigationPublicationPath(path, location.pathname, publicationPin, publicationCohort, publicationInterval, publicationSession)
  const { theme, toggleTheme } = useTheme()
  const [collapsed, setCollapsed] = useState(storedCollapsed)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [pageContext, setPageContext] = useState<PageContextValue | null>(null)
  const [lastStockSymbol, setLastStockSymbol] = useState(readLastStockSymbol)
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(
    () => storedExpandedGroups(location.pathname),
  )
  const [flyoutGroupId, setFlyoutGroupId] = useState<string | null>(null)
  const [flyoutTop, setFlyoutTop] = useState(0)
  const flyoutRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    setDrawerOpen(false)
    setFlyoutGroupId(null)
    const tickerSymbol = activeTickerSymbol(location.pathname)
    if (tickerSymbol) setLastStockSymbol(rememberStockSymbol(tickerSymbol))
    const activeGroup = groupForPath(location.pathname)
    if (!activeGroup || activeGroup === 'console') return
    setExpandedGroups(current => {
      if (current.has(activeGroup)) return current
      const next = new Set(current)
      next.add(activeGroup)
      return next
    })
  }, [location.pathname])

  useEffect(() => {
    try {
      localStorage.setItem(RAIL_STORAGE_KEY, String(collapsed))
    } catch {
      // Storage can be unavailable in private browsing; the rail state still holds for the session.
    }
  }, [collapsed])

  useEffect(() => {
    try {
      localStorage.setItem(GROUP_STORAGE_KEY, JSON.stringify([...expandedGroups]))
    } catch {
      // Storage can be unavailable; disclosure state still holds for the session.
    }
  }, [expandedGroups])

  useEffect(() => {
    if (!flyoutGroupId) return
    const closeOnOutsideClick = (event: MouseEvent) => {
      const target = event.target as Node
      if (flyoutRef.current?.contains(target)) return
      if ((target as Element).closest?.('[data-nav-flyout-trigger]')) return
      setFlyoutGroupId(null)
    }
    document.addEventListener('mousedown', closeOnOutsideClick)
    return () => document.removeEventListener('mousedown', closeOnOutsideClick)
  }, [flyoutGroupId])

  const publishPageContext = useCallback((value: PageContextValue | null) => setPageContext(value), [])

  const breadcrumb = breadcrumbFor(location.pathname)
  const activeGroupId = groupForPath(location.pathname)
  const flyoutGroup = NAV_GROUPS.find(group => group.id === flyoutGroupId) ?? null
  const compactRail = collapsed && !drawerOpen

  const toggleGroup = (groupId: string, trigger: HTMLButtonElement) => {
    if (collapsed && window.innerWidth > 900) {
      const group = NAV_GROUPS.find(candidate => candidate.id === groupId)
      const estimatedHeight = 45 + (group?.items.length ?? 0) * 34
      const top = Math.min(
        trigger.getBoundingClientRect().top,
        window.innerHeight - estimatedHeight - 8,
      )
      setFlyoutTop(Math.max(8, top))
      setFlyoutGroupId(current => current === groupId ? null : groupId)
      return
    }
    setExpandedGroups(current => {
      const next = new Set(current)
      if (next.has(groupId)) next.delete(groupId)
      else next.add(groupId)
      return next
    })
  }

  const renderItem = (item: NavItem, flyout = false) => {
    const Icon = item.icon
    const active = itemMatchesPath(item, location.pathname)
    const baseDestination = item.activeFor === 'ticker'
      ? `/ticker/${lastStockSymbol}`
      : item.to
    const destination = realtimeOnly ? pinnedPath(baseDestination) : baseDestination
    return (
      <NavLink
        key={item.to}
        to={destination}
        end={item.end}
        title={item.label}
        aria-current={active ? 'page' : undefined}
        role={flyout ? 'menuitem' : undefined}
        className={`tm-rail__item${flyout ? ' tm-rail__flyout-item' : ''}${active ? ' is-active' : ''}`}
      >
        <Icon size={15} aria-hidden="true" />
        <span>{item.label}</span>
      </NavLink>
    )
  }

  return (
    <div className={`tm-shell${collapsed ? ' tm-shell--collapsed' : ''}`}>
      <aside className={`tm-rail${drawerOpen ? ' is-open' : ''}`} aria-label="Primary navigation">
        <div className="tm-rail__brand">
          <NavLink to={realtimeOnly ? pinnedPath('/') : '/'} className="tm-brand" title={`${BRAND_NAME} ${BRAND_SUFFIX}`}>
            <span className="tm-brand__mark">{BRAND_MARK}</span>
            <span className="tm-brand__text">
              <strong>{BRAND_NAME}</strong>
              <small>{BRAND_SUFFIX} · {BRAND_TAGLINE}</small>
            </span>
          </NavLink>
          <button
            type="button"
            className="tm-rail__toggle"
            onClick={() => {
              setCollapsed(value => !value)
              setFlyoutGroupId(null)
            }}
            aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'}
            title={collapsed ? 'Expand navigation' : 'Collapse navigation'}
          >
            {collapsed ? <PanelLeftOpen size={15} /> : <PanelLeftClose size={15} />}
          </button>
        </div>

        <nav className="tm-rail__nav">
          {NAV_GROUPS.map(group => {
            const GroupIcon = group.icon
            if (group.collapsible === false) {
              return <div className="tm-rail__group tm-rail__group--static" key={group.id}>
                <span className="tm-rail__group-label">{group.label}</span>
                {group.items.map(item => renderItem(item))}
              </div>
            }
            const expanded = expandedGroups.has(group.id)
            const active = activeGroupId === group.id
            return <div className={`tm-rail__group${expanded ? ' is-expanded' : ''}${active ? ' is-active' : ''}`} key={group.id}>
              <button
                type="button"
                className="tm-rail__group-trigger"
                aria-label={compactRail ? `Open ${group.label} menu` : `${expanded ? 'Collapse' : 'Expand'} ${group.label}`}
                aria-expanded={compactRail ? flyoutGroupId === group.id : expanded}
                aria-controls={compactRail ? 'tm-nav-group-flyout' : `tm-nav-group-${group.id}`}
                title={group.label}
                data-nav-flyout-trigger={group.id}
                onClick={event => toggleGroup(group.id, event.currentTarget)}
              >
                <GroupIcon size={15} aria-hidden="true" />
                <span>{group.label}</span>
                <ChevronDown className="tm-rail__group-chevron" size={13} aria-hidden="true" />
              </button>
              <div className="tm-rail__group-items" id={`tm-nav-group-${group.id}`} hidden={!expanded}>
                {group.items.map(item => renderItem(item))}
              </div>
            </div>
          })}

        </nav>

        {collapsed && flyoutGroup && (
          <div
            ref={flyoutRef}
            id="tm-nav-group-flyout"
            className="tm-rail__flyout"
            style={{ top: flyoutTop }}
            role="menu"
            aria-label={flyoutGroup.label}
          >
            <strong>{flyoutGroup.label}</strong>
            {flyoutGroup.items.map(item => renderItem(item, true))}
          </div>
        )}

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
            {!realtimeOnly && <NavLink to="/signin" className="tm-signin">
              <LogIn size={14} aria-hidden="true" />
              <span>Sign in</span>
            </NavLink>}
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
              <HeaderMarketPanel context={pageContext} realtimeOnly={realtimeOnly} />
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
