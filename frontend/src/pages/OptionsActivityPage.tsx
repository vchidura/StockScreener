import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { AlertTriangle, Database } from 'lucide-react'
import { usePublishPageContext } from '../layout/pageContext'
import { useSessionDate } from '../layout/sessionDate'
import { getOptionFlow } from '../services/api'
import { TickerBoard } from './OptionsFlowPage'
import './OptionsFlowPage.css'

export default function OptionsActivityPage() {
  const navigate = useNavigate()
  const { pinned: sessionDate } = useSessionDate()
  const activity = useQuery({
    queryKey: ['options', 'activity', sessionDate],
    queryFn: () => getOptionFlow(undefined, sessionDate || undefined),
    refetchInterval: 60_000,
  })
  const data = activity.data?.data

  usePublishPageContext({
    eyebrow: 'Cross-sectional options activity',
    title: 'Option Activity',
    detail: 'Compare call and put participation, estimated premium activity, and settled positioning change across the tracked universe.',
    session: '1d',
  })

  if (activity.isLoading) return <div className="flow-state"><Database size={20} /><div><strong>Loading option activity</strong><span>Reading the latest complete matrices in the selected exchange session.</span></div></div>
  if (activity.isError || !data || activity.data?.available === false) return <div className="flow-state flow-state--warning"><AlertTriangle size={20} /><div><strong>Option activity unavailable</strong><span>{activity.data?.reason === 'NO_COMPLETE_MATRIX' ? 'No complete option matrices were retained for this exchange session.' : activity.data?.reason || 'The options service could not be read.'}</span></div></div>

  return <div className="options-flow-page">
    <TickerBoard rows={data.underlyers} selected="" onSelect={underlyer => navigate(`/options/flow?underlyer=${underlyer}`)} />
  </div>
}