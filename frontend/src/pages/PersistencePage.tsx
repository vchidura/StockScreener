import { usePublishPageContext } from '../layout/pageContext'
import PersistenceView from './research/PersistenceView'

export default function PersistencePage() {
  usePublishPageContext({
    eyebrow: 'Stocks',
    title: 'Persistence',
    status: [{ label: 'Window', value: '5 sessions' }],
  })

  return <PersistenceView />
}