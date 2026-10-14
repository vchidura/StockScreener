import type { DashboardPublication, ScannerBasis, ScannerAnalysis, ScannerMemberAnalysis } from '../services/api'
import './StrategyPublication.css'

export function strategyEmptyMessage(analysis: ScannerAnalysis | ScannerMemberAnalysis | undefined, completed: string) {
  if (!analysis) return 'Analysis coverage unavailable; completion cannot be confirmed.'
  if ('reason' in analysis && analysis.reason === 'CORPORATE_ACTION_REVIEW') {
    return 'Corporate action review required; this ticker was not evaluated.'
  }
  if (analysis.status === 'INSUFFICIENT_HISTORY') return 'Insufficient input history; analysis was not completed.'
  if (analysis.status === 'UNAVAILABLE') return 'Inputs unavailable; analysis was not performed.'
  if (analysis.status === 'FAILED') return 'Strategy analysis failed; no completed result is available.'
  if (analysis.status === 'PARTIAL' && 'total_evaluated' in analysis) {
    return `${completed} ${analysis.total_evaluated} members evaluated; ${analysis.total_unavailable} unavailable.`
  }
  return completed
}

export function StrategyActionContext({ member }: { member: ScannerMemberAnalysis }) {
  return <>{member.corporate_actions?.map(action => <p key={action.action_id}>
    {member.ticker}: {action.action_type} {action.effective_date}
    {action.action_type === 'DIVIDEND' && <> | {action.distribution_type ?? 'Unclassified distribution'}
      {action.cash_amount && <> | {action.cash_amount} {action.currency ?? 'currency unverified'}</>}</>}
    {' | '}{action.handling === 'CORPORATE_ACTION_REVIEW'
      ? 'Corporate action review required; ticker excluded.'
      : 'Recurring cash dividend; split-adjusted prices unchanged. Gap signals may include the ex-dividend effect.'}
  </p>)}</>
}

function Coverage({ label, analysis }: { label: string; analysis?: ScannerAnalysis }) {
  if (!analysis) return <p>{label}: analysis coverage unavailable.</p>
  return <div role="status">
    <p>{label}: <strong>{analysis.status}</strong> | {analysis.total_evaluated} evaluated | {analysis.total_unavailable} unavailable</p>
    {analysis.members.filter(member => !['COMPLETE', 'COMPLETE_EMPTY'].includes(member.status)).map(member =>
      <p key={member.ticker}>{member.ticker}: {member.reason === 'CORPORATE_ACTION_REVIEW' ? 'Corporate action review required' : member.status === 'INSUFFICIENT_HISTORY' ? 'Insufficient history' : member.status === 'FAILED' ? 'Analysis failed' : 'Inputs unavailable'}
        {' '}({member.available_bars}/{member.required_bars} input bars)</p>)}
    {analysis.members.filter(member => member.corporate_actions?.length).map(member =>
      <StrategyActionContext key={member.ticker} member={member} />)}
  </div>
}

export function StrategyPublicationError({ error, publication }: {
  error?: string | null; publication?: DashboardPublication
}) {
  if (!error) return null
  return <div role="alert">{error}{publication && ' Retaining the previous complete publication and its original clocks.'}</div>
}

export function StrategyPublication({ publication, basis, analysis, fvgAnalysis, error, loading }: {
  publication?: DashboardPublication; basis?: ScannerBasis; analysis?: ScannerAnalysis; fvgAnalysis?: ScannerAnalysis
  error?: string | null; loading: boolean
}) {
  const hourly = publication?.interval === '1h'
  return <section className="strategy-publication" aria-label={`Stored ${hourly ? 'hourly' : 'daily'} research publication`}>
    <StrategyPublicationError error={error} publication={publication} />
    <p>Research only | {hourly ? 'Hourly' : 'Daily'} exact-cohort evaluation | Heuristic grades, not probabilities or win rates</p>
    {publication && <>
      <p>Status at retained read: <strong>{publication.status}</strong>{loading && ' - reading; displayed values have not advanced'} | {publication.scope_note}</p>
      <p>Market: {publication.market_time} | Published: {publication.generated_at} | Read: {publication.read_at}</p>
      <p>Session: {publication.session} | Snapshot: {publication.snapshot_id}</p>
      {analysis && <Coverage label={`${hourly ? 'Hourly' : 'Daily'} strategy analysis`} analysis={analysis} />}
      {fvgAnalysis && <Coverage label="Fair value gap analysis" analysis={fvgAnalysis} />}
    </>}
    {basis && <p>Scanner prices: {basis.price}. Scanner volume: {basis.volume}; raw-volume parity {basis.raw_volume_parity}.
      {hourly ? ' Daily scanner confirmation includes the forming day; stored setup confirmation uses completed raw daily bars. Finalized RTH slots include partial closing hours.'
        : ' Volume-sensitive grades/classifications preserve the adjusted series. Weekly context: daily resampling to W-FRI, including the partial current week; not completed weekly history.'}</p>}
  </section>
}
