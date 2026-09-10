import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { ChevronDown, ChevronLeft, ChevronRight, LogIn, Menu, Moon, PanelLeftClose, PanelLeftOpen, Sun } from 'lucide-react'
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
  readLastStockSymbol,
  rememberStockSymbol,
  type NavItem,
} from './navigation'
import { useTheme } from '../theme/useTheme'
import './AppShell.css'

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
    const destination = item.activeFor === 'ticker'
      ? `/ticker/${lastStockSymbol}`
      : item.to
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
