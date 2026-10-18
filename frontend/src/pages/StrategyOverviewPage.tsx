import { usePublishPageContext } from '../layout/pageContext'
import RealtimeStrategyOverview from './research/RealtimePersistenceView'
import { useStrategyInterval } from './publicationNavigation'
import './strategyPages.css'

export default function StrategyOverviewPage() {
  const { interval } = useStrategyInterval()
  usePublishPageContext({
    eyebrow: 'Strategies',
    title: 'Strategy Overview',
    status: [{ label: 'Window', value: 'Latest evaluation' }],
    session: interval,
  })

  return <div className="strategy-workspace"><RealtimeStrategyOverview /></div>
}