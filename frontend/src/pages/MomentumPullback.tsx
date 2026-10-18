import { sortHeaderProps } from './strategyShared'
import './strategyPages.css'
import { useState, useMemo, useCallback } from 'react'
import { getDailyScannerPublication } from '../services/api'
import { usePublicationQuery, useStrategyInterval } from './publicationNavigation'
import { StrategyPublicationError, strategyEmptyMessage } from './StrategyPublication'
import { StrategyTickerButton } from './StrategyTickerButton'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { scanMomentumPullback, MomentumPullbackScanResponse } from '../services/api'
import { ScanInterval, SCAN_INTERVALS, intervalSemantics, isPublishedSnapshot } from './strategyShared'
import { usePublishPageContext } from '../layout/pageContext'
import { useSessionDate } from '../layout/sessionDate'
import { CommandBar, CommandGroup, CommandSpacer } from '../layout/PageChrome'

const STRATEGY_TOOLTIP =
  'Describes pullbacks within an uptrend using research heuristics, not trading recommendations.\n\n' +
  '1. TREND ANCHOR: At least 2 daily stacked EMA pairs, plus weekly EMA stack or price above 200 SMA.\n' +
  '2. PULLBACK ZONE: Slow Stochastic %K < 40, ADX 15-55, price within 2 ATR of EMA 21, RSI 30-60.\n' +
  '3. HEURISTIC SCORE: Composite A+ to C grade based on stochastic depth, EMA 21 proximity, stack alignment, relative volume, and ADX strength.'

const PILLAR_TOOLTIPS = {
  dailyStack: 'Daily EMA order 8 > 21 > 34 > 55 > 89. At least 2 of the 4 consecutive pairs must be aligned, plus weekly stack OR the 200-day SMA gate.',
  weeklyStack: 'Weekly EMA 8 > 21 > 34 — confirms the uptrend on the higher timeframe. Resampled from daily data.',
  sma200: 'Price must be above the 200-day SMA, confirming a long-term uptrend. Institutional investors use this as a baseline health indicator.',
  stochastic: 'Slow Stochastic %K (14,3,3). Values below 40 indicate the stock has pulled back within an uptrend — an "oversold in context" reading, not a weakening trend.',
  adx: 'Average Directional Index (14). The detector admits values 15-55.',
  rubberBand: 'Price must be within 2 ATR (14) of EMA 21. This is a proximity rule, not a prediction.',
  relVolume: 'Current volume vs 20-day average. Higher relative volume at pullback support strengthens the entry signal.',
  grade: 'Entry Quality Score — A+ (≥90), A (≥80), B+ (≥70), B (≥60), C (<60). Weighted by: Stoch depth (30%), EMA21 proximity (25%), stack alignment (20%), rel. volume (15%), ADX (10%).',
}

const GRADE_COLORS: Record<string, { bg: string; text: string }> = {
  'A+': { bg: '#00c853', text: '#fff' },
  'A':  { bg: '#4caf50', text: '#fff' },
  'B+': { bg: '#ff9800', text: '#1a1200' },
  'B':  { bg: '#ffc107', text: '#000' },
  'C':  { bg: '#9e9e9e', text: '#14181c' },
}

const InfoIcon = ({ tooltip }: { tooltip: string }) => (
  <span
    title={tooltip}
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
      width: '18px',
      height: '18px',
      borderRadius: '50%',
      border: '1.5px solid var(--text-secondary)',
      color: 'var(--text-secondary)',
      fontSize: '12px',
      fontWeight: 700,
      cursor: 'help',
      marginLeft: '0.5rem',
      verticalAlign: 'middle',
      fontStyle: 'normal',
    }}
  >
    i
  </span>
)

type SortKey = 'ticker' | 'last_close' | 'score' | 'stoch_k' | 'adx' | 'dist_to_ema21_pct' | 'rel_volume' | 'sma200' | 'volume' | 'rsi'
type SortDir = 'asc' | 'desc'

function MomentumPullback() {
  const realtimeOnly = import.meta.env.VITE_REALTIME_ONLY === 'true'
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [gradeFilter, setGradeFilter] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('score')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const { pinned: scanDate } = useSessionDate()
  const [canonicalInterval, setCanonicalInterval] = useState<ScanInterval>('1d')
  const storedInterval = useStrategyInterval()
  const interval = realtimeOnly ? storedInterval.interval : canonicalInterval
  const setInterval = (value: ScanInterval) => realtimeOnly ? storedInterval.setInterval(value === '1h' ? '1h' : '1d') : setCanonicalInterval(value)
  const realtimeQuery = usePublicationQuery('daily-momentum-pullback', (pin, session) => getDailyScannerPublication('momentum-pullback', pin, interval === '1h' ? '1h' : '1d', session))
  const canonicalQuery = useQuery<MomentumPullbackScanResponse>({
    queryKey: ['scan', 'momentum-pullback', interval, scanDate],
    queryFn: () => scanMomentumPullback(undefined, scanDate || undefined, interval),
    placeholderData: keepPreviousData,
    enabled: !realtimeOnly,
  })
  const data = realtimeOnly ? realtimeQuery.data?.data : canonicalQuery.data
  const loading = realtimeOnly ? realtimeQuery.loading : canonicalQuery.isFetching
  const error = realtimeOnly ? realtimeQuery.error : canonicalQuery.error?.message

  const handleRefresh = useCallback(async () => {
    if (realtimeOnly) return realtimeQuery.refresh()
    const key = ['scan', 'momentum-pullback', interval, scanDate]
    queryClient.setQueryData(key, undefined)
    await queryClient.fetchQuery({ queryKey: key, queryFn: () => scanMomentumPullback(undefined, scanDate || undefined, interval, true) })
  }, [interval, scanDate, queryClient, realtimeOnly, realtimeQuery.refresh])

  const filtered = useMemo(() => {
    if (!data?.results) return []
    let items = data.results
    if (filter) items = items.filter(r => r.ticker.toLowerCase().includes(filter.toLowerCase()))
    if (gradeFilter) items = items.filter(r => r.grade === gradeFilter)
    // sort
    items = [...items].sort((a, b) => {
      const av = a[sortKey] ?? -Infinity
      const bv = b[sortKey] ?? -Infinity
      if (typeof av === 'string' && typeof bv === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number)
    })
    return items
  }, [data, filter, gradeFilter, sortKey, sortDir])

  const allGrades = useMemo(() => {
    if (!data?.results) return []
    return Array.from(new Set(data.results.map(r => r.grade))).sort()
  }, [data])

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir(key === 'ticker' ? 'asc' : 'desc') }
  }

  const sortArrow = (key: SortKey) => {
    if (sortKey !== key) return ' ↕'
    return sortDir === 'asc' ? ' ↑' : ' ↓'
  }

  const thStyle = (_key: SortKey, align: 'left' | 'right' | 'center' = 'left'): React.CSSProperties => ({
    textAlign: align, cursor: 'pointer', userSelect: 'none', whiteSpace: 'nowrap',
  })

  const checkMark = (ok: boolean) => (
    <span style={{ color: ok ? 'var(--success)' : 'var(--danger)', fontWeight: 700 }}>
      {ok ? '✓' : '✗'}
    </span>
  )

  const semantics = intervalSemantics(interval)
  const published = isPublishedSnapshot(interval, scanDate)

  usePublishPageContext({
    eyebrow: 'Trend strategy · long setups',
    title: 'Momentum Pullback',
    detail: STRATEGY_TOOLTIP.split('\n')[0],
    status: [
      { label: 'Interval', value: interval },
    ],
    session: interval,
  })

  /** Renders a pass/fail mark, or a neutral dash when the test never ran. */
  const gateMark = (ok: boolean, evaluated: boolean, why: string) =>
    evaluated ? checkMark(ok) : (
      <span style={{ color: 'var(--tm-faint)', fontWeight: 700 }} title={why}>–</span>
    )

  return (
    <div className="strategy-workspace">
      <CommandBar>
        <CommandGroup label="Interval">
          {SCAN_INTERVALS.filter(iv => !realtimeOnly || iv === '1d' || iv === '1h').map((iv) => (
            <button
              key={iv}
              type="button"
              className={interval === iv ? 'is-active' : undefined}
              onClick={() => setInterval(iv)}
              title={intervalSemantics(iv).candle}
            >
              {iv}
            </button>
          ))}
        </CommandGroup>
        <CommandSpacer />
        <span className="tm-commandbar__note">{realtimeOnly && !data ? 'Publication unavailable' : `${filtered.length} of ${data?.results.length ?? 0} matches`}</span>
        <button type="button" className="tm-commandbar__action" onClick={handleRefresh} disabled={loading}>
          {loading ? (realtimeOnly ? 'Reading…' : 'Scanning…') : 'Refresh'}
        </button>
      </CommandBar>
      <StrategyPublicationError error={error} publication={realtimeOnly ? realtimeQuery.data?.publication : undefined} />

      {/* Provenance + interval semantics */}
      <div
        style={{
          display: 'flex', flexWrap: 'wrap', alignItems: 'baseline', gap: '0.5rem',
          margin: '0 0 1rem', padding: '0.6rem 0.75rem', borderRadius: '6px',
          background: published ? 'var(--tm-accent-soft)' : 'var(--tm-warn-soft)',
          border: `1px solid ${published ? 'var(--tm-accent)' : 'var(--tm-warn)'}`,
          fontSize: '0.82rem',
        }}
      >
        <strong style={{ color: published ? 'var(--tm-accent)' : 'var(--tm-warn)' }}>
          {published ? 'Published daily snapshot' : 'Live scan'}
        </strong>
        <span style={{ color: 'var(--text-secondary)' }}>
          {realtimeOnly ? 'Stored defaults for the selected realtime cohort.' : published
            ? 'Precomputed nightly across the full universe on daily candles.'
            : `Computed on request over the full universe using ${semantics.candle.toLowerCase()}.`}
        </span>
        <span style={{ color: 'var(--text-secondary)' }}>
          Trend gate: {semantics.trendRule}.
        </span>
        {semantics.caveat && (
          <span style={{ width: '100%', color: 'var(--tm-warn)', marginTop: '0.15rem' }}>
            {semantics.caveat}
          </span>
        )}
      </div>

      {/* Methodology card */}
      <div className="card" style={{ marginBottom: '1.5rem', background: 'var(--bg-secondary)', border: '1px solid var(--border)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
          <span style={{ fontSize: '1.5rem' }}>📊</span>
          <div>
            <div style={{ fontWeight: 700, fontSize: '1.1rem' }}>Elite Momentum Pullback Methodology</div>
            <div style={{ fontSize: '0.8rem', color: 'var(--success)', fontWeight: 600, letterSpacing: '0.05em', textTransform: 'uppercase' }}>Research Heuristics - Not Probabilities</div>
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '1.25rem' }}>
          <div>
            <h4 style={{ color: 'var(--success)', marginBottom: '0.5rem' }}>1. The Trend Anchor</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>Daily Stack:</strong> At least 2 of 4 EMA pairs, 8 {'>'} 21 {'>'} 34 {'>'} 55 {'>'} 89
              <InfoIcon tooltip={PILLAR_TOOLTIPS.dailyStack} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0', opacity: semantics.higherTfLabel ? 1 : 0.55 }}>
              {gateMark(true, !!semantics.higherTfLabel, 'Not evaluated at this interval')}{' '}
              <strong>Higher timeframe (or baseline):</strong>{' '}
              {semantics.higherTfLabel ?? 'not evaluated at this interval'}
              <InfoIcon tooltip={PILLAR_TOOLTIPS.weeklyStack} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0', opacity: semantics.baselineLabel ? 1 : 0.55 }}>
              {gateMark(true, !!semantics.baselineLabel, 'Not evaluated at this interval')}{' '}
              <strong>Baseline:</strong>{' '}
              {semantics.baselineLabel ? `Price above the ${semantics.baselineLabel}` : 'not evaluated at this interval'}
              <InfoIcon tooltip={PILLAR_TOOLTIPS.sma200} />
            </p>
          </div>
          <div>
            <h4 style={{ color: 'var(--success)', marginBottom: '0.5rem' }}>2. The Pullback Zone</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>Stochastics:</strong> Slow %K {'<'} 40
              <InfoIcon tooltip={PILLAR_TOOLTIPS.stochastic} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>ADX Strength:</strong> 15 – 55
              <InfoIcon tooltip={PILLAR_TOOLTIPS.adx} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              <span style={{ color: 'var(--tm-warn)', fontWeight: 700 }}>!</span> <strong>Rubber Band:</strong> Within 2 ATR of EMA 21; RSI 30–60
              <InfoIcon tooltip={PILLAR_TOOLTIPS.rubberBand} />
            </p>
          </div>
          <div>
            <h4 style={{ color: 'var(--success)', marginBottom: '0.5rem' }}>3. Entry &amp; Score</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              Candidates ranked by <strong>Entry Quality Score</strong>.
              <InfoIcon tooltip={PILLAR_TOOLTIPS.grade} />
            </p>
          </div>
        </div>
      </div>

      {loading && !data && (
        <div className="loading">
          <div className="spinner"></div>
          <span>{realtimeOnly ? `Reading stored ${interval} momentum publication...` : 'Scanning momentum pullback setups across all tickers...'}</span>
        </div>
      )}

      {(realtimeOnly || !loading) && data && (
        <>
          {/* Filters */}
          <div className="filter-bar">
            <input aria-label="Search ticker..."
              type="text"
              placeholder="Search ticker..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{ minWidth: '160px' }}
            />
            <select aria-label="All Grades"
              value={gradeFilter}
              onChange={(e) => setGradeFilter(e.target.value)}
              style={{ minWidth: '120px' }}
            >
              <option value="">All Grades</option>
              {allGrades.map(g => <option key={g} value={g}>{g}</option>)}
            </select>
            <span>
              {data.total_scanned} {realtimeOnly ? 'cohort members' : 'scanned'} &middot; {filtered.length} qualifying setups
            </span>
          </div>

          {/* Results table */}
          <div className="card" style={{ padding: 0, overflowX: 'auto' }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th {...sortHeaderProps(() => handleSort('ticker'), sortKey === 'ticker', sortDir)} style={thStyle('ticker')}>
                    Ticker{sortArrow('ticker')}
                  </th>
                  <th {...sortHeaderProps(() => handleSort('score'), sortKey === 'score', sortDir)} style={thStyle('score', 'center')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Grade{sortArrow('score')}<InfoIcon tooltip={PILLAR_TOOLTIPS.grade} /></span>
                  </th>
                  <th {...sortHeaderProps(() => handleSort('last_close'), sortKey === 'last_close', sortDir)} style={thStyle('last_close', 'right')}>
                    Close{sortArrow('last_close')}
                  </th>
                  <th {...sortHeaderProps(() => handleSort('volume'), sortKey === 'volume', sortDir)} style={thStyle('volume', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Volume{sortArrow('volume')}<InfoIcon tooltip="Latest daily trading volume and relative volume (vs 20-day avg). Higher relative volume at pullback support strengthens the signal." /></span>
                  </th>
                  <th {...sortHeaderProps(() => handleSort('rsi'), sortKey === 'rsi', sortDir)} style={thStyle('rsi', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>RSI{sortArrow('rsi')}<InfoIcon tooltip="Relative Strength Index (14-period). Below 30 = oversold, above 70 = overbought. In a momentum pullback context, RSI 30-50 confirms the pullback without breaking the trend." /></span>
                  </th>
                  <th {...sortHeaderProps(() => handleSort('sma200'), sortKey === 'sma200', sortDir)} style={thStyle('sma200', 'center')}>
                    <span style={{ whiteSpace: 'nowrap', opacity: semantics.baselineLabel ? 1 : 0.55 }}>
                      {semantics.baselineLabel ? `>${semantics.baselineLabel}` : '>SMA n/a'}
                      {sortArrow('sma200')}
                      <InfoIcon tooltip={semantics.baselineLabel
                        ? PILLAR_TOOLTIPS.sma200
                        : 'Not evaluated at this interval — the intraday scan skips the 200-period baseline entirely and tightens the EMA stack requirement instead.'} />
                    </span>
                  </th>
                  <th {...sortHeaderProps(() => handleSort('stoch_k'), sortKey === 'stoch_k', sortDir)} style={thStyle('stoch_k', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Stoch %K{sortArrow('stoch_k')}<InfoIcon tooltip={PILLAR_TOOLTIPS.stochastic} /></span>
                  </th>
                  <th {...sortHeaderProps(() => handleSort('adx'), sortKey === 'adx', sortDir)} style={thStyle('adx', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>ADX{sortArrow('adx')}<InfoIcon tooltip={PILLAR_TOOLTIPS.adx} /></span>
                  </th>
                  <th {...sortHeaderProps(() => handleSort('dist_to_ema21_pct'), sortKey === 'dist_to_ema21_pct', sortDir)} style={thStyle('dist_to_ema21_pct', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Dist EMA21{sortArrow('dist_to_ema21_pct')}<InfoIcon tooltip={PILLAR_TOOLTIPS.rubberBand} /></span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={9} style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-secondary)' }}>
                      {realtimeOnly ? strategyEmptyMessage(data.analysis, 'No qualifying momentum pullback setups found') : 'No qualifying momentum pullback setups found'}
                    </td>
                  </tr>
                ) : (
                  filtered.map((r, idx) => {
                    const gc = GRADE_COLORS[r.grade] || GRADE_COLORS['C']
                    return (
                      <tr key={idx}>
                        <td>
                          <StrategyTickerButton ticker={r.ticker} publication={realtimeQuery.data?.publication} />
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          <span style={{
                            display: 'inline-block',
                            padding: '3px 10px',
                            borderRadius: '4px',
                            background: gc.bg,
                            color: gc.text,
                            fontWeight: 700,
                            fontSize: '0.85rem',
                            minWidth: '36px',
                          }}>
                            {r.grade}
                          </span>
                          <span style={{ display: 'block', fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: '2px' }}>
                            {r.score}
                          </span>
                        </td>
                        <td style={{ textAlign: 'right' }}>${r.last_close.toFixed(2)}</td>
                        <td style={{ textAlign: 'right' }}>
                          <span>{(r.volume / 1000).toFixed(0)}K</span>
                          <span style={{
                            display: 'block',
                            fontSize: '0.7rem',
                            color: r.rel_volume >= 1.5 ? 'var(--success)' : 'var(--text-secondary)',
                            fontWeight: r.rel_volume >= 1.5 ? 600 : 400,
                          }}>
                            {r.rel_volume.toFixed(2)}x avg
                          </span>
                        </td>
                        <td style={{
                          textAlign: 'right',
                          color: r.rsi <= 30 ? 'var(--success)' : r.rsi >= 70 ? 'var(--danger)' : 'inherit',
                          fontWeight: r.rsi <= 30 || r.rsi >= 70 ? 600 : 400,
                        }}>
                          {r.rsi.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          {gateMark(r.above_sma200, !!semantics.baselineLabel, 'Not evaluated at this interval')}
                          {r.sma200 != null && (
                            <span style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', marginLeft: '4px' }}>
                              ${r.sma200.toFixed(0)}
                            </span>
                          )}
                        </td>
                        <td style={{ textAlign: 'right', color: r.stoch_k < 20 ? 'var(--success)' : 'inherit', fontWeight: r.stoch_k < 20 ? 600 : 400 }}>
                          {r.stoch_k.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'right', color: r.adx >= 30 && r.adx <= 40 ? 'var(--success)' : 'inherit', fontWeight: r.adx >= 30 && r.adx <= 40 ? 600 : 400 }}>
                          {r.adx.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'right' }}>
                          {r.dist_to_ema21_pct.toFixed(2)}%
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

export default MomentumPullback
