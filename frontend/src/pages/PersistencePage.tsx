import { usePublishPageContext } from '../layout/pageContext'
import PersistenceView from './research/PersistenceView'
import { useStrategyInterval } from './publicationNavigation'

export default function PersistencePage() {
  const { interval } = useStrategyInterval()
  usePublishPageContext({
    eyebrow: 'Stocks',
    title: 'Persistence',
    status: [{ label: 'Window', value: '5 trading sessions' }],
    session: interval,
  })

  return <PersistenceView />
}