import { useDeferredValue, useEffect, useEffectEvent, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ArrowLeft, ArrowRight, Eye, RefreshCw, X } from 'lucide-react'
import {
  getStockEodReview,
  type StockEodOutcomeSummary,
  type StockEodReview,
  type StockEodReviewRow,
  type StockEodSelection,
} from '../services/stockDiscovery'

const models = { resumption: 'Trend resumption', acceptance: 'Breakout acceptance', failure: 'Extension reversal' }
const bucketNames = { RANK_1: 'Rank 1', RANK_2_3: 'Ranks 2-3', RANK_4_PLUS: 'Rank 4+' }
const readable = (value: string) => value.toLowerCase().replace(/_/g, ' ')
const number = (value: number | null | undefined, digits = 2) => typeof value === 'number' && Number.isFinite(value)
  ? value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits }) : 'Unavailable'
const percent = (value: number | null | undefined) => typeof value === 'number' && Number.isFinite(value)
  ? `${value > 0 ? '+' : ''}${number(value * 100)}%` : 'Unavailable'
const money = (value: number | null | undefined) => typeof value === 'number' && Number.isFinite(value) ? `$${number(value)}` : 'Unavailable'
const time = (value: string | null | undefined) => value ? new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit',
}).format(new Date(value)) : 'Unavailable'

function ReviewField({ label, value }: { label: string; value: string }) {
  return <div className="sa-detail-field"><span>{label}</span><strong>{value}</strong></div>
}

function reviewSignal(signal: StockEodReview['review_signals'][number]) {
  if (signal.code === 'COVERAGE_LIMITED') return {
    title: 'Coverage limits comparison',
    detail: `${signal.completed} of ${signal.total_runs} runs completed (${number(signal.completion_rate * 100, 1)}%); ${signal.missed} missed runs are excluded.`,
  }
  if (signal.code === 'ALLOCATION_PRESSURE') return {
    title: 'Allocation pressure',
    detail: `${signal.candidates} candidates were blocked by quota/capacity; ${signal.top_three_priority} were top-three pre-gate priorities.`,
  }
  if (signal.code === 'RANK_ORDER_INVERSION') return {
    title: `${models[signal.model]} rank ordering`,
    detail: `Rank 1 mean ${percent(signal.rank1_mean_return)} (n=${signal.rank1_measured}) trails ranks 2-3 ${percent(signal.rank2_3_mean_return)} (n=${signal.rank2_3_measured}).`,
  }
  if (signal.code === 'LOW_FILL_CONVERSION') return {
    title: `${models[signal.model]} entry conversion`,
    detail: `${number(signal.fill_rate * 100, 1)}% of resolved selections entered: ${signal.entered} entered / ${signal.no_fill} no fill.`,
  }
  const value = signal.dimension === 'direction' ? Number(signal.value) === 1 ? 'long' : 'short' : signal.value
  return {
    title: `${readable(signal.dimension)} concentration`,
    detail: `${number(signal.share * 100, 1)}% of selections (${signal.selected}) were ${value}.`,
  }
}

function ReviewDrawer({ row, returnFocus, onClose }: {
  row: StockEodReviewRow
  returnFocus: HTMLButtonElement | null
  onClose: () => void
}) {
  const closeOnEscape = useEffectEvent(onClose)
  useEffect(() => {
    const overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    const keydown = (event: KeyboardEvent) => { if (event.key === 'Escape') closeOnEscape() }
    document.addEventListener('keydown', keydown)
    return () => {
      document.body.style.overflow = overflow
      returnFocus?.focus()
      document.removeEventListener('keydown', keydown)
    }
  }, [returnFocus])
  return <div className="sa-drawer-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
    <aside className="sa-alert-drawer" role="dialog" aria-modal="true" aria-labelledby="stock-review-title">
      <header>
        <div><span>Retained selection evidence</span><h2 id="stock-review-title">{row.ticker} <small>{row.direction === 1 ? 'Long' : 'Short'}</small></h2><p>{models[row.model]} / {row.interval} / {readable(row.selection_status)}</p></div>
        <button type="button" className="sa-drawer-close" aria-label="Close candidate review" title="Close candidate review" autoFocus onClick={onClose}><X size={18} /></button>
      </header>
      <div className="sa-alert-drawer__body">
        <section><h3>Selection</h3><div className="sa-detail-grid">
          <ReviewField label="Decision" value={readable(row.selection_status)} />
          <ReviewField label="Reason" value={readable(row.selection_reason)} />
          <ReviewField label="Occurrences" value={String(row.occurrences)} />
          <ReviewField label="Pre-gate priority" value={row.priority_rank == null ? 'Not ranked' : `${row.priority_rank} of ${row.priority_pool_size}`} />
          <ReviewField label="Eligible-pool rank" value={row.model_rank == null ? 'Not eligible' : `${row.model_rank} of ${row.pool_size}`} />
          <ReviewField label="First / last seen (ET)" value={`${time(row.first_seen)} / ${time(row.last_seen)}`} />
        </div><p className="sa-detail-empty">{row.ranking_basis}</p></section>
        <section><h3>Ranking inputs</h3><div className="sa-detail-grid">
          <ReviewField label="RS63 percentile" value={`${number(row.rs_percentile * 100, 1)}%`} />
          <ReviewField label="Extension / ATR" value={`${number(row.extension_atr)}x`} />
          <ReviewField label="Reward / risk" value={`${number(row.reward_risk)}x`} />
          <ReviewField label="12-1 momentum" value={percent(row.momentum)} />
          <ReviewField label="Median dollar volume" value={`$${new Intl.NumberFormat('en-US', { notation: 'compact' }).format(row.liquidity)}`} />
          <ReviewField label="Trade type" value={readable(row.trade_type)} />
        </div></section>
        <section><h3>Candidate plan</h3><div className="sa-detail-grid">
          <ReviewField label="Trigger" value={money(row.trigger_price)} />
          <ReviewField label="Stop" value={money(row.stop)} />
          <ReviewField label="Target" value={money(row.target)} />
          <ReviewField label="Risk to stop" value={percent(row.risk_pct)} />
          <ReviewField label="Detected (ET)" value={time(row.trigger_at)} />
          <ReviewField label="Policy" value={readable(row.policy_version)} />
        </div></section>
        <section><h3>Descriptive outcome</h3><div className="sa-detail-grid">
          <ReviewField label="Outcome status" value={row.outcome_status ? readable(row.outcome_status) : 'Not selected'} />
          <ReviewField label="Paper return" value={row.outcome_status === 'NO_FILL' ? 'No exposure' : percent(row.paper_return)} />
        </div><p className="sa-detail-empty">Observed paper outcomes are descriptive and are not a calibrated success estimate.</p></section>
      </div>
    </aside>
  </div>
}

function ModelSummary({ data }: { data: StockEodReview }) {
  return <div className="sd-table-panel"><div className="sd-table-scroll sa-eod-summary" tabIndex={0} role="region" aria-label="Stock model selection summary">
    <table><thead><tr><th>Model</th><th>Detections</th><th>Candidates</th><th>Selected</th><th>Not selected</th><th>Fill rate</th><th>Top exclusions</th><th>Measured</th><th>Positive rate</th><th>Mean / median</th></tr></thead>
      <tbody>{data.models.map(cell => <tr key={cell.model}>
        <td>{models[cell.model]}<small title={cell.ranking_basis}>Ranking policy</small></td>
        <td>{cell.detected_occurrences}</td><td>{cell.unique_candidates}</td>
        <td>{cell.selected}<small>{cell.pending} pending</small></td>
        <td>{cell.not_selected}<small>{cell.repeats} repeats</small></td>
        <td>{percent(cell.fill_rate)}<small>{cell.entered} entered / {cell.no_fill} no fill</small></td>
        <td>{cell.top_reasons.length ? cell.top_reasons.map(item => <small key={item.reason}>{readable(item.reason)}: {item.count}</small>) : 'None'}</td>
        <td>{cell.measured}</td><td>{percent(cell.positive_rate)}</td>
        <td>{percent(cell.mean_return)}<small>median {percent(cell.median_return)}</small></td>
      </tr>)}</tbody>
    </table>
  </div></div>
}

function RankDiagnostics({ data }: { data: StockEodReview }) {
  return <section><h3>Rank quality</h3><p>Selected candidates only; compare ordering within each model.</p>
    <div className="sd-table-scroll" tabIndex={0}><table><thead><tr><th>Model</th><th>Rank bucket</th><th>Selected</th><th>Fill rate</th><th>Measured</th><th>Positive</th><th>Mean / median</th></tr></thead>
      <tbody>{data.diagnostics.rank_buckets.map(row => <tr key={`${row.model}:${row.bucket}`}>
        <td>{models[row.model]}</td><td>{bucketNames[row.bucket]}</td><td>{row.selected}</td>
        <td>{percent(row.fill_rate)}</td><td>{row.measured}</td><td>{percent(row.positive_rate)}</td>
        <td>{percent(row.mean_return)}<small>median {percent(row.median_return)}</small></td>
      </tr>)}</tbody>
    </table></div>
  </section>
}

function AllocationBottlenecks({ data }: { data: StockEodReview }) {
  return <section><h3>Allocation bottlenecks</h3><p>Rejected candidates have no counterfactual outcomes.</p>
    <div className="sd-table-scroll" tabIndex={0}><table><thead><tr><th>Reason</th><th>Class</th><th>Candidates</th><th>Top-3 priority</th><th>Median priority</th></tr></thead>
      <tbody>{data.diagnostics.bottlenecks.map(row => <tr key={row.reason}>
        <td>{readable(row.reason)}</td><td>{readable(row.category)}</td><td>{row.candidates}</td>
        <td>{row.top_three_priority}</td><td>{number(row.median_priority, 1)}</td>
      </tr>)}</tbody>
    </table></div>
  </section>
}

function MixRow({ dimension, label, row }: { dimension: string; label: string; row: StockEodOutcomeSummary & { share: number | null } }) {
  return <tr><td>{dimension}</td><td>{label}</td><td>{row.selected}</td><td>{percent(row.share)}</td>
    <td>{percent(row.fill_rate)}</td><td>{row.measured}</td><td>{percent(row.positive_rate)}</td><td>{percent(row.mean_return)}</td></tr>
}

function AllocationMix({ data }: { data: StockEodReview }) {
  return <section className="sa-eod-mix"><h3>Allocation mix</h3><p>Concentration and outcome mix among selected candidates.</p>
    <div className="sd-table-scroll" tabIndex={0}><table><thead><tr><th>Dimension</th><th>Value</th><th>Selected</th><th>Share</th><th>Fill rate</th><th>Measured</th><th>Positive</th><th>Mean return</th></tr></thead><tbody>
      {data.diagnostics.direction_mix.map(row => <MixRow key={`direction:${row.value}`} dimension="Direction" label={row.value === 1 ? 'Long' : 'Short'} row={row} />)}
      {data.diagnostics.interval_mix.map(row => <MixRow key={`interval:${row.value}`} dimension="Interval" label={row.value} row={row} />)}
    </tbody></table></div>
  </section>
}

function CandidateTable({ data, offset, update, onSelect }: {
  data: StockEodReview
  offset: number
  update: (values: Record<string, string | null>) => void
  onSelect: (row: StockEodReviewRow, trigger: HTMLButtonElement) => void
}) {
  return <div className="sd-table-panel">
    <div className="sd-table-scroll sa-eod-candidates" tabIndex={0} role="region" aria-label="All retained stock candidates">
      <table><thead><tr><th>Stock</th><th>Trade type</th><th>Model / interval</th><th>Seen</th><th>Selection / reason</th><th>Priority / eligible</th><th>RS63</th><th>Extension</th><th>Reward/risk</th><th>Outcome</th><th>Details</th></tr></thead>
        <tbody>{data.rows.map(row => <tr key={row.candidate_id}>
          <td className="sd-symbol">{row.ticker}<small>{row.direction === 1 ? 'Long' : 'Short'}</small></td>
          <td>{readable(row.trade_type)}</td><td>{models[row.model]}<small>{row.interval}</small></td>
          <td>{row.occurrences}<small>{time(row.first_seen)}-{time(row.last_seen)} ET</small></td>
          <td>{readable(row.selection_status)}{row.selection_reason !== row.selection_status && <small>{readable(row.selection_reason)}</small>}</td>
          <td>{row.priority_rank == null ? 'N/A' : `${row.priority_rank} / ${row.priority_pool_size}`}<small>{row.model_rank == null ? 'Not eligible' : `eligible ${row.model_rank} / ${row.pool_size}`}</small></td>
          <td>{number(row.rs_percentile * 100, 1)}%</td><td>{number(row.extension_atr)}x</td><td>{number(row.reward_risk)}x</td>
          <td>{row.outcome_status ? readable(row.outcome_status) : 'Not selected'}<small>{row.outcome_status === 'NO_FILL' ? 'No exposure' : percent(row.paper_return)}</small></td>
          <td><button type="button" className="sd-expand sa-detail-launch" aria-label={`View ${row.ticker} evaluation`} title="View candidate evaluation" onClick={event => onSelect(row, event.currentTarget)}><Eye size={15} /></button></td>
        </tr>)}</tbody>
      </table>
      {!data.rows.length && <div className="sd-empty">No candidates match these filters.</div>}
    </div>
    <footer className="sd-pager"><span>{data.total ? `${offset + 1}-${offset + data.rows.length} of ${data.total}` : '0 results'}</span><div>
      <button aria-label="Previous EOD review page" disabled={!offset} onClick={() => update({ offset: String(Math.max(0, offset - 100)) })}><ArrowLeft size={15} /></button>
      <button aria-label="Next EOD review page" disabled={offset + data.rows.length >= data.total} onClick={() => update({ offset: String(offset + 100) })}><ArrowRight size={15} /></button>
    </div></footer>
  </div>
}

export default function StockEodReviewPanel({ params, update, offset, onSessionContext }: {
  params: URLSearchParams
  update: (values: Record<string, string | null>) => void
  offset: number
  onSessionContext: (value: { dates: string[]; selected: string }) => void
}) {
  const search = useDeferredValue(params.get('search') || '')
  const rawModel = params.get('model')
  const rawSelection = params.get('review_selection')
  const rawDirection = params.get('direction')
  const rawTradeType = params.get('trade_type')
  const request = {
    session_date: params.get('session_date') || undefined,
    search: search || undefined,
    model: rawModel === 'resumption' || rawModel === 'acceptance' || rawModel === 'failure' ? rawModel : undefined,
    selection_status: rawSelection === 'SELECTED' || rawSelection === 'NOT_SELECTED' || rawSelection === 'REPEAT' ? rawSelection as StockEodSelection : undefined,
    direction: rawDirection === '1' || rawDirection === '-1' ? Number(rawDirection) : undefined,
    trade_type: rawTradeType === 'INTRADAY' || rawTradeType === 'SWING' ? rawTradeType : undefined,
    offset,
    limit: 100,
  }
  const review = useQuery({
    queryKey: ['stock-alert-eod-review', request],
    queryFn: () => getStockEodReview(request),
    retry: false,
    refetchInterval: 30_000,
  })
  const data = review.data
  const [selected, setSelected] = useState<StockEodReviewRow | null>(null)
  const detailTrigger = useRef<HTMLButtonElement | null>(null)
  useEffect(() => { setSelected(null) }, [params])
  useEffect(() => { if (data) onSessionContext({ dates: data.sessions, selected: data.session_date || '' }) }, [data, onSessionContext])

  return <section id="sa-results" className="sa-eod-review" role="tabpanel" aria-labelledby="sa-eod">
    <div className="sd-filters sa-eod-filters" aria-label="Stock EOD review filters">
      <label>Stock<input type="search" placeholder="Ticker" value={params.get('search') || ''} onChange={event => update({ search: event.target.value })} /></label>
      <label>Model<select value={request.model || ''} onChange={event => update({ model: event.target.value || null })}><option value="">All models</option>{Object.entries(models).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select></label>
      <label>Selection<select value={request.selection_status || ''} onChange={event => update({ review_selection: event.target.value || null })}><option value="">All decisions</option><option value="SELECTED">Selected</option><option value="NOT_SELECTED">Not selected</option><option value="REPEAT">Repeat</option></select></label>
      <label>Direction<select value={rawDirection === '1' || rawDirection === '-1' ? rawDirection : ''} onChange={event => update({ direction: event.target.value || null })}><option value="">All</option><option value="1">Long</option><option value="-1">Short</option></select></label>
      <label>Trade type<select value={request.trade_type || ''} onChange={event => update({ trade_type: event.target.value || null })}><option value="">All</option><option value="INTRADAY">Intraday</option><option value="SWING">Swing</option></select></label>
      <button type="button" className="sa-eod-refresh" aria-label="Refresh stock EOD review" title="Refresh stock EOD review" disabled={review.isFetching} onClick={() => void review.refetch()}><RefreshCw size={15} /></button>
    </div>
    {review.isPending && <div className="sd-empty" role="status">Loading retained candidate evaluation...</div>}
    {review.isError && <div className="sd-notice" role="alert">Stock EOD review is unavailable.<button onClick={() => void review.refetch()}>Retry</button></div>}
    {data && !data.storage_ready && <div className="sd-notice">Stock evaluation storage is not deployed.</div>}
    {data?.storage_ready && <>
      <div className="sd-metrics sa-eod-metrics"><span>{data.session_date || 'No evaluated session'}</span><span><strong>{data.unique_candidates}</strong> candidates / {data.detected_occurrences} detections</span><span>{data.completed_runs} completed / {data.missed_runs} missed runs</span></div>
      <p className="sa-eod-note">Selection and rank use the original retained policy. Outcomes are current descriptive paper results, not calibrated success probabilities.</p>
      {!!data.review_signals?.length && <section className="sa-eod-signals" aria-label="Ranking and allocation review signals"><h3>Review signals</h3><div>{data.review_signals.map((signal, index) => {
        const rendered = reviewSignal(signal)
        return <article key={`${signal.code}-${index}`} className={signal.severity === 'CAUTION' ? 'is-caution' : ''}><strong>{rendered.title}</strong><p>{rendered.detail}</p></article>
      })}</div></section>}
      <ModelSummary data={data} />
      {data.diagnostics && <><div className="sa-eod-analysis-grid"><RankDiagnostics data={data} /><AllocationBottlenecks data={data} /></div><AllocationMix data={data} /></>}
      <CandidateTable data={data} offset={offset} update={update} onSelect={(row, trigger) => { detailTrigger.current = trigger; setSelected(row) }} />
    </>}
    {selected && <ReviewDrawer row={selected} returnFocus={detailTrigger.current} onClose={() => setSelected(null)} />}
  </section>
}
