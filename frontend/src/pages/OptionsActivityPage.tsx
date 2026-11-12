import { useQuery } from '@tanstack/react-query'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { AlertTriangle, Database } from 'lucide-react'
import { usePublishPageContext } from '../layout/pageContext'
import { useSessionDate } from '../layout/sessionDate'
import { getOptionFlow, optionsDataTierLabel } from '../services/api'
import { flowWindowMs, OptionSessionNotice, optionFlowUnavailableReason, TickerBoard } from './OptionsFlowPage'
import { msUntilWindowRead, OPTIONS_FIRST_CHECK_MS } from '../utils/refreshTiming'
import './OptionsFlowPage.css'

export default function OptionsActivityPage() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const { pinned: sessionDate } = useSessionDate()
  const activity = useQuery({
    queryKey: ['options', 'activity', sessionDate],
    queryFn: () => getOptionFlow(undefined, sessionDate || undefined),
    // Nov 12 (user, RF1 for Activity): read when the next window's release and chain capture land, as Options Flow does.
    refetchInterval: current => msUntilWindowRead(Date.now(), flowWindowMs(current.state.data), OPTIONS_FIRST_CHECK_MS,
      60_000),
  })
  const data = activity.data?.data

  usePublishPageContext({
    eyebrow: 'Cross-sectional options activity',
    title: 'Option Activity',
    detail: 'Compare call and put participation, estimated premium activity, and settled positioning change across the tracked universe.',
    status: [{ label: 'Data', value: optionsDataTierLabel(activity.data?.data_tier) }],
    session: '5m',
  })

  if (activity.isLoading) return <div className="flow-state"><Database size={20} /><div><strong>Loading option activity</strong><span>Reading the latest complete matrices in the selected exchange session.</span></div></div>
  if (activity.isError || !data || activity.data?.available === false) return <div className="flow-state flow-state--warning"><AlertTriangle size={20} /><div><strong>Option activity unavailable</strong><span>{optionFlowUnavailableReason(activity.data?.reason, activity.error)}</span></div></div>

  return <div className="options-flow-page">
    <OptionSessionNotice session={data.session_date} />
    <TickerBoard rows={data.underlyers} selected="" onSelect={underlyer => {
      const next = new URLSearchParams(searchParams)
      next.set('underlyer', underlyer)
      navigate(`/options/flow?${next}`)
    }} />
  </div>
}