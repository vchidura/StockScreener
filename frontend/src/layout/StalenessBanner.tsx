import { useEffect, useState } from 'react'
import { fetchMaterializationStatus, type MaterializationStatus } from '../services/api'

const POLL_INTERVAL_MS = 120000

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
  const asOf = formatMarketTime(status.oldest_stale_market_time ?? status.last_market_time)
  const intervals = status.stale_intervals.join(', ')
  const scope = intervals ? ` Affected timeframes: ${intervals}.` : ''
  if (status.staleness_sessions >= 1) {
    const plural = status.staleness_sessions === 1 ? 'session' : 'sessions'
    return (
      `Analysis is ${status.staleness_sessions} trading ${plural} behind. ` +
      `Oldest published data is from ${asOf}.${scope}`
    )
  }
  return `Showing the last published data, as of ${asOf}.${scope}`
}

export function StalenessBanner() {
  const [status, setStatus] = useState<MaterializationStatus | null>(null)

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        const next = await fetchMaterializationStatus()
        if (!cancelled) setStatus(next)
      } catch {
        // A failed status read must never break the page it is warning about.
        if (!cancelled) setStatus(null)
      }
    }
    load()
    const timer = window.setInterval(load, POLL_INTERVAL_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [])

  if (!status || status.status === 'READY') return null

  const severe = status.status === 'EXPIRED' || status.staleness_sessions >= 1
  return (
    <div
      className={`tm-staleness${severe ? ' tm-staleness--severe' : ''}`}
      role="status"
      aria-live="polite"
    >
      <span className="tm-staleness__badge">
        {status.status === 'EXPIRED' ? 'Data unavailable' : 'Stale data'}
      </span>
      <span className="tm-staleness__text">
        {status.status === 'EXPIRED'
          ? `Published analysis is more than ${status.max_serveable_stale_sessions} trading sessions old and is no longer served. Start the equity worker to resume updates.`
          : describe(status)}
      </span>
      <span className="tm-staleness__hint">Trade setup levels stay hidden until data is current.</span>
    </div>
  )
}
