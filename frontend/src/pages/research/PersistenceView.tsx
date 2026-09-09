import { Fragment, useMemo, useState } from 'react'
import { useQueries, useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { scanStreak, getTickersOverview, type StreakResult, type TickerOverviewRow } from '../../services/api'
import './persistence.css'

const STRATEGIES = [
  { value: 'gaps', label: 'Gap & Imbalance', direction: 'Both' },
  { value: 'ma-crossover', label: 'MA Crossover', direction: 'Both' },
  { value: 'momentum-pullback', label: 'Momentum Pullback', direction: 'Long' },
  { value: 'bearish-bounce', label: 'Bearish Bounce', direction: 'Short' },
  { value: 'fibonacci', label: 'Fibonacci', direction: 'Both' },
] as const

/** Default window served from published portal snapshots; anything else re-scans live. */
const PUBLISHED_DAYS = 5
const PAGE_SIZE = 100

type AxisKey = 'stability' | 'distance' | 'magnitude' | 'maturity' | 'volume'
type Axis = { label: string; note?: string; series?: number[] }
type Axes = Partial<Record<AxisKey, Axis>> & { context?: string }

const GROUPS: Array<{ value: 'none' | 'strategy' | AxisKey; label: string }> = [
  { value: 'none', label: 'No grouping' },
  { value: 'strategy', label: 'Strategy' },
  { value: 'stability', label: 'Stability' },
  { value: 'distance', label: 'Distance trend' },
  { value: 'magnitude', label: 'Magnitude trend' },
  { value: 'maturity', label: 'Maturity' },
  { value: 'volume', label: 'Volume' },
]

const volumeBand = (ratio: number | null | undefined) => {
  if (ratio === null || ratio === undefined) return { label: 'No data' }
  if (ratio >= 1.5) return { label: 'High', note: `${ratio.toFixed(2)}×` }
  if (ratio >= 1) return { label: 'Normal', note: `${ratio.toFixed(2)}×` }
  return { label: 'Low', note: `${ratio.toFixed(2)}×` }
}

/** Labels stay strategy-native: gap "Converging" and MA "Widening" do not mean the same thing. */
function axesFor(row: StreakResult): Axes {
  if (row.gap_analysis) {
    const a = row.gap_analysis
    return {
      stability: { label: a.transition_summary, note: a.type_sequence.join(' → ') },
      distance: { label: a.fill_progress, series: a.fill_distances, note: 'distance to gap' },
      magnitude: { label: `${a.new_gaps_in_window} new`, note: 'gaps formed in window' },
      maturity: { label: a.freshness, note: a.freshest_gap_age != null ? `${a.freshest_gap_age}d old` : undefined },
      volume: volumeBand(a.avg_volume_ratio),
      context: a.type_sequence[a.type_sequence.length - 1],
    }
  }
  if (row.ma_analysis) {
    const a = row.ma_analysis
    return {
      stability: { label: a.signal_flow, note: a.signal_sequence.join(' → ') },
      distance: { label: a.spread_trend, series: a.spreads.map(Math.abs), note: 'MA spread' },
      magnitude: { label: a.price_momentum, series: a.price_changes.map(Math.abs), note: 'move since cross' },
      maturity: { label: a.days_since_cross != null ? `${a.days_since_cross}d since cross` : 'Unknown' },
      volume: volumeBand(a.avg_volume_ratio),
      context: a.weekly_alignment,
    }
  }
  if (row.fib_analysis) {
    const a = row.fib_analysis
    return {
      stability: { label: a.level_stability, note: a.level_sequence.join(' → ') },
      distance: { label: a.proximity_trend, series: a.distances, note: 'distance to level' },
      magnitude: { label: a.depth_trend, series: a.retrace_pcts, note: 'retracement depth' },
      maturity: { label: a.pivot_stable ? 'Pivots stable' : 'Pivots moved' },
      volume: volumeBand(a.avg_volume_ratio),
      context: a.dominant_level,
    }
  }
  return {}
}

function dailyDetails(row: StreakResult): Record<string, Record<string, unknown>> | null {
  const analysis = row.gap_analysis ?? row.ma_analysis ?? row.fib_analysis
  return (analysis?.daily_details as Record<string, Record<string, unknown>> | undefined) ?? null
}

function Spark({ values }: { values: number[] }) {
  if (values.length < 2) return null
  const width = 54
  const height = 14
  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = max - min || 1
  const points = values
    .map((value, index) => `${(index / (values.length - 1) * (width - 2) + 1).toFixed(1)},${(height - 2 - ((value - min) / span) * (height - 4)).toFixed(1)}`)
    .join(' ')
  return (
    <svg width={width} height={height} aria-hidden="true" style={{ display: 'block' }}>
      <polyline points={points} fill="none" stroke="var(--tm-accent)" strokeWidth="1.5" strokeLinejoin="round" />
    </svg>
  )
}

function Dots({ dates, matched }: { dates: string[]; matched: string[] }) {
  const hit = new Set(matched)
  return (
    <span className="pv-dots" aria-hidden="true">
      {dates.map(date => <i key={date} className={hit.has(date) ? 'on' : undefined} />)}
    </span>
  )
}

function AxisCell({ axis }: { axis?: Axis }) {
  if (!axis) return <span style={{ color: 'var(--tm-faint)' }}>—</span>
  return (
    <div className="pv-axis">
      <strong>{axis.label}</strong>
      {axis.series && axis.series.length > 1 && <Spark values={axis.series} />}
      {axis.note && <small>{axis.note}</small>}
    </div>
  )
}

/** Distribution of "matched k strategies" if each strategy drew independently at its own base rate. */
function poissonBinomial(probabilities: number[]): number[] {
  let dist = [1]
  for (const p of probabilities) {
    const next = new Array(dist.length + 1).fill(0)
    for (let k = 0; k < dist.length; k++) {
      next[k] += dist[k] * (1 - p)
      next[k + 1] += dist[k] * p
    }
    dist = next
  }
  return dist
}

type Row = {
  key: string
  ticker: string
  strategy: string
  strategyLabel: string
  direction: string
  result: StreakResult
  scanDates: string[]
  depth: number
}

export default function PersistenceView() {
  const navigate = useNavigate()
  const [strategyFilter, setStrategyFilter] = useState<string>('all')
  const [groupBy, setGroupBy] = useState<'none' | 'strategy' | AxisKey>('none')
  const [search, setSearch] = useState('')
  const [minStrategies, setMinStrategies] = useState(1)
  const [perfectOnly, setPerfectOnly] = useState(false)
  const [sortBy, setSortBy] = useState<'depth' | 'ticker' | 'strategy'>('depth')
  const [expanded, setExpanded] = useState<string | null>(null)
  const [page, setPage] = useState(0)

  const queries = useQueries({
    queries: STRATEGIES.map(item => ({
      queryKey: ['streak', item.value, PUBLISHED_DAYS],
      queryFn: () => scanStreak(item.value, PUBLISHED_DAYS),
      staleTime: 5 * 60 * 1000,
    })),
  })

  // Shared query key with the other pages, so this is normally already cached.
  const { data: overview = [] } = useQuery<TickerOverviewRow[]>({
    queryKey: ['tickers', 'overview'],
    queryFn: () => getTickersOverview(),
    staleTime: 5 * 60 * 1000,
  })

  const loading = queries.some(query => query.isLoading)
  const failed = STRATEGIES.filter((_, index) => queries[index].isError).map(item => item.label)
  const dataStamp = queries.map(query => query.dataUpdatedAt).join(',')

  const allRows = useMemo<Row[]>(() => queries.flatMap((query, index) => {
    const meta = STRATEGIES[index]
    const dates = query.data?.scan_dates ?? []
    return (query.data?.results ?? []).map(result => ({
      key: `${result.ticker}:${meta.value}`,
      ticker: result.ticker,
      strategy: meta.value,
      strategyLabel: meta.label,
      direction: meta.direction,
      result,
      scanDates: dates,
      depth: result.total_days ? result.days_matched / result.total_days : 0,
    }))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [dataStamp])

  /** Strategy count per ticker drives the filter only — it is not a ranking. */
  const strategyCount = useMemo(() => {
    const counts = new Map<string, number>()
    allRows.forEach(row => counts.set(row.ticker, (counts.get(row.ticker) ?? 0) + 1))
    return counts
  }, [allRows])

  const coverage = useMemo(() => {
    const universe = overview.length || strategyCount.size
    if (!universe) return null
    const counts = STRATEGIES.map((_, index) => queries[index].data?.results.length ?? 0)
    const probabilities = counts.map(count => count / universe)
    const expected = poissonBinomial(probabilities).map(value => value * universe)
    const observed = new Array(STRATEGIES.length + 1).fill(0)
    strategyCount.forEach(count => { observed[count] += 1 })
    observed[0] = universe - strategyCount.size
    return { universe, counts, probabilities, expected, observed }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [overview.length, strategyCount, dataStamp])

  const term = search.trim().toUpperCase()
  const filtered = useMemo(() => allRows.filter(row =>
    (strategyFilter === 'all' || row.strategy === strategyFilter)
    && (!term || row.ticker.includes(term))
    && (!perfectOnly || row.result.days_matched === row.result.total_days)
    && (strategyCount.get(row.ticker) ?? 0) >= minStrategies,
  ), [allRows, strategyFilter, term, perfectOnly, minStrategies, strategyCount])

  const groupLabel = (row: Row): string | null => {
    if (groupBy === 'none') return null
    if (groupBy === 'strategy') return row.strategyLabel
    return axesFor(row.result)[groupBy]?.label ?? 'Unavailable'
  }

  const ordered = useMemo(() => [...filtered].sort((left, right) => {
    if (groupBy !== 'none') {
      const byGroup = (groupLabel(left) ?? '').localeCompare(groupLabel(right) ?? '')
      if (byGroup !== 0) return byGroup
    }
    if (sortBy === 'ticker') return left.ticker.localeCompare(right.ticker) || left.strategyLabel.localeCompare(right.strategyLabel)
    if (sortBy === 'strategy') return left.strategyLabel.localeCompare(right.strategyLabel) || right.depth - left.depth
    return right.depth - left.depth
      || right.result.days_matched - left.result.days_matched
      || left.ticker.localeCompare(right.ticker)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [filtered, sortBy, groupBy])

  const pageCount = Math.max(1, Math.ceil(ordered.length / PAGE_SIZE))
  const safePage = Math.min(page, pageCount - 1)
  const visible = ordered.slice(safePage * PAGE_SIZE, safePage * PAGE_SIZE + PAGE_SIZE)
  const onFilterChange = <T,>(setter: (value: T) => void) => (value: T) => { setter(value); setPage(0) }

  return (
    <div className="pv">
      <p className="pv-note">
        Persistence counts how often a ticker appeared in a strategy scan over the last {PUBLISHED_DAYS} sessions,
        and how that signal moved while it persisted. It is a description of recurrence, not evidence of an edge —
        statistical qualification lives in the Research view. A ticker missing from a strategy may simply lack the
        history that strategy requires, so an absent row is not a negative reading.
      </p>

      <section className="pv-panel">
        <div className="pv-panel__head">
          <div>
            <h2>Coverage this window</h2>
            <p>How selective each scan is, and whether appearing in several of them means anything.</p>
          </div>
          {failed.length > 0 && <span className="pv-unavailable">Unavailable: {failed.join(', ')}</span>}
        </div>
        {loading || !coverage ? (
          <p className="pv-empty">Loading coverage…</p>
        ) : (
          <div className="pv-coverage">
            <div className="pv-rates">
              {STRATEGIES.map((item, index) => {
                const share = coverage.probabilities[index]
                return (
                  <div key={item.value} className="pv-rate">
                    <span className="pv-rate__label">{item.label}</span>
                    <strong>{(share * 100).toFixed(0)}%</strong>
                    <small>{coverage.counts[index]} of {coverage.universe} · {item.direction}</small>
                    <div className="pv-rate__bar"><i style={{ width: `${Math.min(100, share * 100)}%` }} /></div>
                  </div>
                )
              })}
            </div>
            <div className="pv-independence">
              <table className="pv-mini">
                <thead>
                  <tr>
                    <th>Strategies matched</th>
                    {coverage.observed.map((_, k) => <th key={k}>{k}</th>)}
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>Observed tickers</td>
                    {coverage.observed.map((value, k) => <td key={k}>{value}</td>)}
                  </tr>
                  <tr>
                    <td>If independent</td>
                    {coverage.expected.map((value, k) => <td key={k}>{value.toFixed(0)}</td>)}
                  </tr>
                </tbody>
              </table>
              <p>
                These two rows track each other closely, so matching several scans is mostly explained by how often
                each scan fires on its own rather than by the strategies agreeing. Use the strategy-count filter to
                narrow the list, not to rank it. Long and short strategies never co-occur by construction, so a high
                count mixes directions.
              </p>
            </div>
          </div>
        )}
      </section>

      <section className="pv-panel">
        <div className="pv-panel__head">
          <div>
            <h2>Persistence detail</h2>
            <p>One row per ticker and strategy. Trajectory columns use each strategy&apos;s own vocabulary.</p>
          </div>
        </div>
        <div className="pv-controls">
          <label className="pv-field">
            <span>Strategy</span>
            <select value={strategyFilter} onChange={event => onFilterChange(setStrategyFilter)(event.target.value)}>
              <option value="all">All strategies</option>
              {STRATEGIES.map((item, index) => (
                <option key={item.value} value={item.value}>
                  {item.label}{coverage ? ` — ${(coverage.probabilities[index] * 100).toFixed(0)}% of universe` : ''}
                </option>
              ))}
            </select>
          </label>
          <label className="pv-field">
            <span>Sort by</span>
            <select value={sortBy} onChange={event => onFilterChange(setSortBy)(event.target.value as typeof sortBy)}>
              <option value="depth">Recurrence depth</option>
              <option value="ticker">Ticker</option>
              <option value="strategy">Strategy</option>
            </select>
          </label>
          <label className="pv-field">
            <span>Group by</span>
            <select value={groupBy} onChange={event => onFilterChange(setGroupBy)(event.target.value as typeof groupBy)}>
              {GROUPS.map(item => <option key={item.value} value={item.value}>{item.label}</option>)}
            </select>
          </label>
          <label className="pv-field">
            <span>In at least</span>
            <select value={minStrategies} onChange={event => onFilterChange(setMinStrategies)(Number(event.target.value))}>
              {[1, 2, 3, 4].map(value => (
                <option key={value} value={value}>{value} strateg{value === 1 ? 'y' : 'ies'}</option>
              ))}
            </select>
          </label>
          <label className="pv-field">
            <span>Ticker</span>
            <input value={search} onChange={event => onFilterChange(setSearch)(event.target.value)} placeholder="Filter" />
          </label>
          <label className="pv-check">
            <input type="checkbox" checked={perfectOnly} onChange={event => onFilterChange(setPerfectOnly)(event.target.checked)} />
            Every session only
          </label>
          <span className="pv-count">
            {ordered.length} rows · {new Set(ordered.map(row => row.ticker)).size} tickers
          </span>
        </div>

        {loading ? (
          <p className="pv-empty">Loading persistence across all strategies…</p>
        ) : ordered.length === 0 ? (
          <p className="pv-empty">No rows match these filters in the published window.</p>
        ) : (
          <>
            <div className="pv-table-wrap">
              <table className="pv-table">
                <thead>
                  <tr>
                    <th>Ticker</th>
                    <th>Strategy</th>
                    <th>Recurrence</th>
                    <th>Stability</th>
                    <th>Distance trend</th>
                    <th>Magnitude trend</th>
                    <th>Maturity</th>
                    <th>Volume</th>
                    <th>Context</th>
                  </tr>
                </thead>
                <tbody>
                  {visible.map((row, index) => {
                    const axes = axesFor(row.result)
                    const details = dailyDetails(row.result)
                    const open = expanded === row.key
                    const label = groupLabel(row)
                    const showGroup = label !== null && (index === 0 || label !== groupLabel(visible[index - 1]))
                    const hasTrajectory = Object.keys(axes).length > 0
                    return (
                      <Fragment key={row.key}>
                        {showGroup && <tr><td className="pv-group" colSpan={9}>{label}</td></tr>}
                        <tr
                          onClick={() => setExpanded(open ? null : row.key)}
                          style={{ cursor: details ? 'pointer' : 'default' }}
                        >
                          <td>
                            <button
                              type="button"
                              className="pv-ticker"
                              onClick={event => { event.stopPropagation(); navigate(`/ticker/${row.ticker}`) }}
                            >
                              {row.ticker}
                            </button>
                          </td>
                          <td>
                            <div className="pv-axis">
                              <strong>{row.strategyLabel}</strong>
                              <small>{row.direction}</small>
                            </div>
                          </td>
                          <td>
                            <span className="pv-matched">{row.result.days_matched}/{row.result.total_days}</span>
                            <Dots dates={row.scanDates} matched={row.result.dates_matched} />
                          </td>
                          {hasTrajectory ? (
                            <>
                              <td><AxisCell axis={axes.stability} /></td>
                              <td><AxisCell axis={axes.distance} /></td>
                              <td><AxisCell axis={axes.magnitude} /></td>
                              <td><AxisCell axis={axes.maturity} /></td>
                              <td><AxisCell axis={axes.volume} /></td>
                              <td style={{ color: 'var(--tm-muted)' }}>{axes.context ?? '—'}</td>
                            </>
                          ) : (
                            <td colSpan={6} style={{ color: 'var(--tm-faint)' }}>
                              Recurrence only — this strategy publishes no per-session trajectory.
                            </td>
                          )}
                        </tr>
                        {open && details && (
                          <tr className="pv-detail">
                            <td colSpan={9}>
                              <table className="pv-detail-table">
                                <tbody>
                                  {row.scanDates.filter(date => details[date]).map(date => (
                                    <tr key={date}>
                                      <td style={{ fontWeight: 700 }}>{date}</td>
                                      <td>
                                        {Object.entries(details[date])
                                          .filter(([, value]) => value !== null && typeof value !== 'object')
                                          .map(([key, value]) => `${key.replace(/_/g, ' ')} ${value}`)
                                          .join(' · ')}
                                      </td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    )
                  })}
                </tbody>
              </table>
            </div>
            {pageCount > 1 && (
              <div className="pv-pagination">
                <span>
                  {safePage * PAGE_SIZE + 1}–{Math.min(ordered.length, (safePage + 1) * PAGE_SIZE)} of {ordered.length}
                </span>
                <div>
                  <button type="button" onClick={() => setPage(safePage - 1)} disabled={safePage === 0}>Previous</button>
                  <button type="button" onClick={() => setPage(safePage + 1)} disabled={safePage >= pageCount - 1}>Next</button>
                </div>
              </div>
            )}
          </>
        )}
      </section>
    </div>
  )
}
