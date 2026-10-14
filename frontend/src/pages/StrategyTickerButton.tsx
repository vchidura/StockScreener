import { useNavigate } from 'react-router-dom'
import type { DashboardPublication } from '../services/api'
import { publicationPath } from './publicationNavigation'

export function StrategyTickerButton({ ticker, publication }: { ticker: string; publication?: DashboardPublication }) {
  const navigate = useNavigate()
  const realtimeOnly = import.meta.env.VITE_REALTIME_ONLY === 'true'
  let destination = `/ticker/${encodeURIComponent(ticker)}`
  const retainedResearchAvailable = realtimeOnly
    && publication?.strategy_ticker_available === true
    && !!publication.cohort_id
    && !!publication.snapshot_id
  if (retainedResearchAvailable) {
    destination = publicationPath(`/strategy-research/${encodeURIComponent(ticker)}`, publication.snapshot_id, publication.cohort_id, publication.interval)
  }
  return <button type="button" className="ticker" title={retainedResearchAvailable ? 'Open retained strategy research' : 'Open ticker workspace'}
    onClick={event => { event.stopPropagation(); navigate(destination) }}>{ticker}</button>
}
