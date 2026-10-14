import { useEffect, useState } from 'react'
import { useLocation, useSearchParams } from 'react-router-dom'
import { fetchMarketNotificationWindow, fetchMaterializationStatus, type MaterializationStatus, type MarketNotificationWindow } from '../services/api'
import { activeTickerSymbol } from './navigation'
import { isDailyStrategyPath } from '../pages/publicationNavigation'

const POLL_INTERVAL_MS = 120000

export function marketNotificationsEnabled(market: MarketNotificationWindow, elapsedMs = 0): boolean {
  const now = Date.parse(market.checked_at) + Math.max(0, elapsedMs)
  return market.is_trading_session && market.notifications_enabled
    && !!market.session_open && !!market.session_close
    && Date.parse(market.session_open) <= now && now < Date.parse(market.session_close)
}

interface NotificationState {
  scope: string
  market: MarketNotificationWindow
  receivedAt: number
  status: MaterializationStatus | null
  unavailable: boolean
}

function formatMarketTime(value: string | null): string {
  if (!value) return 'unknown'
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return 'unknown'
  return parsed.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function describe(status: MaterializationStatus): string {
  const asOf = formatMarketTime(status.last_market_time)
  const intervals = status.stale_intervals.join(', ')
  const scope = intervals ? ` Affected timeframes: ${intervals}.` : ''
  return `Showing the latest retained values as of ${asOf}.${scope}`
}

export function StalenessBanner() {
  const realtimeOnly = import.meta.env.VITE_REALTIME_ONLY === 'true'
  const [notification, setNotification] = useState<NotificationState | null>(null)
  const [params] = useSearchParams()
  const location = useLocation()
  const ticker = activeTickerSymbol(location.pathname) ?? undefined
  const snapshotId = params.get('snapshot_id') ?? undefined
  const dailyStrategies = isDailyStrategyPath(location.pathname)
  const interval = params.get('interval') === '1h' ? '1h' : '1d'
  const scope = JSON.stringify([realtimeOnly, snapshotId, ticker, dailyStrategies, interval])

  useEffect(() => {
    let cancelled = false
    let inFlight = false
    let boundaryTimer: ReturnType<typeof window.setTimeout> | undefined
    const load = async () => {
      if (cancelled || inFlight) return
      inFlight = true
      let current: NotificationState | null = null
      try {
        const market = await fetchMarketNotificationWindow()
        if (cancelled) return
        current = { scope, market, receivedAt: Date.now(), status: null, unavailable: false }
        setNotification(current)
        if (boundaryTimer !== undefined) window.clearTimeout(boundaryTimer)
        const boundary = market.notifications_enabled ? market.session_close : market.session_open
        const remaining = boundary ? Date.parse(boundary) - Date.parse(market.checked_at) : 0
        if (remaining > 0) {
          boundaryTimer = window.setTimeout(() => {
            if (!cancelled) {
              setNotification(null)
              void load()
            }
          }, remaining)
        }
        if (!marketNotificationsEnabled(market)) return
        const next = await fetchMaterializationStatus(snapshotId, ticker, dailyStrategies, interval)
        if (!cancelled) {
          setNotification({ ...current, status: next })
        }
      } catch {
        if (!cancelled) {
          setNotification(current ? { ...current, unavailable: realtimeOnly } : null)
        }
      } finally {
        inFlight = false
      }
    }
    void load()
    const timer = window.setInterval(load, POLL_INTERVAL_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
      if (boundaryTimer !== undefined) window.clearTimeout(boundaryTimer)
    }
  }, [realtimeOnly, snapshotId, ticker, dailyStrategies, scope])

  if (!notification || notification.scope !== scope
    || !marketNotificationsEnabled(notification.market, Date.now() - notification.receivedAt)) return null
  const { status, unavailable } = notification

  if (!status) {
    if (!unavailable) return null
    return (
      <div className="tm-staleness" role="status" aria-live="polite">
        <span className="tm-staleness__badge">Market data status unavailable</span>
        <span className="tm-staleness__text">Pages remain available with their latest retained content.</span>
        <span className="tm-staleness__hint">Use the timestamps shown in each view.</span>
      </div>
    )
  }
  if (status.status === 'READY') return null

  const updating = status.status === 'EXPIRED' && status.pipeline_active
  const active = status.active_intervals?.join(', ') || 'equity'
  const severe = status.status === 'EXPIRED' || status.staleness_sessions >= 1
  return (
    <div
      className={`tm-staleness${severe ? ' tm-staleness--severe' : ''}`}
      role="status"
      aria-live="polite"
    >
      <span className="tm-staleness__badge">
        {updating ? 'Updating market data' : 'Delayed market data'}
      </span>
      <span className="tm-staleness__text">
        {updating
          ? `Equity ${active} analysis is processing. ${describe(status)}`
          : describe(status)}
      </span>
      {updating && (
        <span className="tm-staleness__hint">Pages remain available while a newer publication is prepared.</span>
      )}
    </div>
  )
}
