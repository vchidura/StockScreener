import { usePublishPageContext } from '../layout/pageContext'
import RealtimeStrategyOverview from './research/RealtimePersistenceView'
import './strategyPages.css'

export default function StrategyOverviewPage() {
  usePublishPageContext({
    eyebrow: 'Strategies',
    title: 'Strategy Overview',
    status: [{ label: 'Window', value: 'Latest evaluation' }],
  })

  return <div className="strategy-workspace"><RealtimeStrategyOverview /></div>
}