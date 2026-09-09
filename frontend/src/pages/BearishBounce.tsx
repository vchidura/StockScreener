import { useState, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { scanBearishBounce, BearishBounceScanResponse } from '../services/api'
import { ScanInterval, SCAN_INTERVALS, intervalSemantics, isPublishedSnapshot } from './strategyShared'
import { usePublishPageContext } from '../layout/pageContext'
import { CommandBar, CommandGroup, CommandSpacer } from '../layout/PageChrome'
import { useSessionDate } from '../layout/sessionDate'

const STRATEGY_TOOLTIP =
  'Finds stocks in confirmed downtrends bouncing toward resistance. Trade WITH the bearish trend, enter shorts ON the bounce.\n\n' +
  '1. TREND ANCHOR: Inverted daily EMA stack 89>55>34>21>8, inverted weekly EMA 34>21>8, price below 200 SMA.\n' +
  '2. BOUNCE ZONE: Slow Stochastic %K > 60 (overbought in downtrend), ADX 15-55 (healthy trend), price within 2 ATR of EMA 21.\n' +
  '3. ENTRY SCORE: Composite A+ to C grade based on stochastic height, EMA 21 proximity, stack alignment, relative volume, and ADX strength.'

const PILLAR_TOOLTIPS = {
  dailyStack: 'Inverted daily EMA 89 > 55 > 34 > 21 > 8 — all five exponential moving averages aligned bearishly. Stack count shows how many consecutive pairs are inverted (max 4).',
  weeklyStack: 'Inverted weekly EMA 34 > 21 > 8 — confirms the downtrend on the higher timeframe. Resampled from daily data.',
  sma200: 'Price must be below the 200-day SMA, confirming a long-term downtrend.',
  stochastic: 'Slow Stochastic %K (14,3,3). Values above 60 indicate the stock has bounced within a downtrend — an "overbought in context" reading, signaling a potential short entry.',
  adx: 'Average Directional Index (14). Values 15-55 indicate a healthy trending market. Below 15 = no trend; above 55 = overextended.',
  rubberBand: 'Price must be within 2 ATR (14) of EMA 21. The bounce has brought price back near mean resistance — where shorts can be initiated.',
  relVolume: 'Current volume vs 20-day average. Higher relative volume at bounce resistance strengthens the short signal.',
  grade: 'Entry Quality Score — A+ (≥90), A (≥80), B+ (≥70), B (≥60), C (<60). Weighted by: Stoch height (30%), EMA21 proximity (25%), stack alignment (20%), rel. volume (15%), ADX (10%).',
}

const GRADE_COLORS: Record<string, { bg: string; text: string }> = {
  'A+': { bg: '#d32f2f', text: '#fff' },
  'A':  { bg: '#e53935', text: '#fff' },
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

function BearishBounce() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [gradeFilter, setGradeFilter] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('score')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const { pinned: scanDate } = useSessionDate()
  const [interval, setInterval] = useState<ScanInterval>('1d')

  const { data, isFetching: loading } = useQuery<BearishBounceScanResponse>({
    queryKey: ['scan', 'bearish-bounce', interval, scanDate],
    queryFn: () => scanBearishBounce(undefined, scanDate || undefined, interval),
    placeholderData: keepPreviousData,
  })

  const handleRefresh = useCallback(async () => {
    const key = ['scan', 'bearish-bounce', interval, scanDate]
    queryClient.setQueryData(key, undefined)
    await queryClient.fetchQuery({ queryKey: key, queryFn: () => scanBearishBounce(undefined, scanDate || undefined, interval, true) })
  }, [interval, scanDate, queryClient])


  const filtered = useMemo(() => {
    if (!data?.results) return []
    let items = data.results
    if (filter) items = items.filter(r => r.ticker.toLowerCase().includes(filter.toLowerCase()))
    if (gradeFilter) items = items.filter(r => r.grade === gradeFilter)
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
    <span style={{ color: ok ? 'var(--danger)' : 'var(--text-secondary)', fontWeight: 700 }}>
      {ok ? '✓' : '✗'}
    </span>
  )

  const semantics = intervalSemantics(interval)
  const published = isPublishedSnapshot(interval, scanDate)

  /** Renders a pass/fail mark, or a neutral dash when the test never ran. */
  const gateMark = (ok: boolean, evaluated: boolean, why: string) =>
    evaluated ? checkMark(ok) : (
      <span style={{ color: 'var(--tm-faint)', fontWeight: 700 }} title={why}>–</span>
    )

  usePublishPageContext({
    eyebrow: 'Trend strategy · short setups',
    title: 'Bearish Bounce',
    detail: STRATEGY_TOOLTIP.split('\n')[0],
    status: [
      { label: 'Interval', value: interval },
    ],
    session: interval,
  })

  return (
    <div>
      <CommandBar>
        <CommandGroup label="Interval">
          {SCAN_INTERVALS.map((iv) => (
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
        <span className="tm-commandbar__note">{filtered.length} of {data?.results.length ?? 0} matches</span>
        <button type="button" className="tm-commandbar__action" onClick={handleRefresh} disabled={loading}>
          {loading ? 'Scanning…' : 'Refresh'}
        </button>
      </CommandBar>

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
          {published
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
            <div style={{ fontWeight: 700, fontSize: '1.1rem' }}>Bearish Bounce Methodology</div>
            <div style={{ fontSize: '0.8rem', color: 'var(--danger)', fontWeight: 600, letterSpacing: '0.05em', textTransform: 'uppercase' }}>High-Probability Short Setups</div>
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '1.25rem' }}>
          <div>
            <h4 style={{ color: 'var(--danger)', marginBottom: '0.5rem' }}>1. The Trend Anchor</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>Daily Stack:</strong> EMA 89 {'>'} 55 {'>'} 34 {'>'} 21 {'>'} 8
              <InfoIcon tooltip={PILLAR_TOOLTIPS.dailyStack} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0', opacity: semantics.higherTfLabel ? 1 : 0.55 }}>
              {gateMark(true, !!semantics.higherTfLabel, 'Not evaluated at this interval')}{' '}
              <strong>Higher timeframe:</strong>{' '}
              {semantics.higherTfLabel ? `Inverted ${semantics.higherTfLabel.toLowerCase()}` : 'not evaluated at this interval'}
              <InfoIcon tooltip={PILLAR_TOOLTIPS.weeklyStack} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0', opacity: semantics.baselineLabel ? 1 : 0.55 }}>
              {gateMark(true, !!semantics.baselineLabel, 'Not evaluated at this interval')}{' '}
              <strong>Baseline:</strong>{' '}
              {semantics.baselineLabel ? `Price below the ${semantics.baselineLabel}` : 'not evaluated at this interval'}
              <InfoIcon tooltip={PILLAR_TOOLTIPS.sma200} />
            </p>
          </div>
          <div>
            <h4 style={{ color: 'var(--danger)', marginBottom: '0.5rem' }}>2. The Bounce Zone</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>Stochastics:</strong> Slow %K {'>'} 60
              <InfoIcon tooltip={PILLAR_TOOLTIPS.stochastic} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>ADX Strength:</strong> 15 – 55
              <InfoIcon tooltip={PILLAR_TOOLTIPS.adx} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              <span style={{ color: 'var(--tm-warn)', fontWeight: 700 }}>!</span> <strong>Rubber Band:</strong> Within 2 ATR of EMA 21
              <InfoIcon tooltip={PILLAR_TOOLTIPS.rubberBand} />
            </p>
          </div>
          <div>
            <h4 style={{ color: 'var(--danger)', marginBottom: '0.5rem' }}>3. Entry &amp; Score</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              Candidates ranked by <strong>Entry Quality Score</strong>.
              <InfoIcon tooltip={PILLAR_TOOLTIPS.grade} />
            </p>
          </div>
        </div>
      </div>

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <span>Scanning bearish bounce setups across all tickers...</span>
        </div>
      )}

      {!loading && data && (
        <>
          {/* Filters */}
          <div className="filter-bar">
            <input
              type="text"
              placeholder="Search ticker..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{ minWidth: '160px' }}
            />
            <select
              value={gradeFilter}
              onChange={(e) => setGradeFilter(e.target.value)}
              style={{ minWidth: '120px' }}
            >
              <option value="">All Grades</option>
              {allGrades.map(g => <option key={g} value={g}>{g}</option>)}
            </select>
            <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
              {data.total_scanned} scanned &middot; {filtered.length} qualifying setups
            </span>
          </div>

          {/* Results table */}
          <div className="card" style={{ padding: 0, overflowX: 'auto' }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th onClick={() => handleSort('ticker')} style={thStyle('ticker')}>
                    Ticker{sortArrow('ticker')}
                  </th>
                  <th onClick={() => handleSort('score')} style={thStyle('score', 'center')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Grade{sortArrow('score')}<InfoIcon tooltip={PILLAR_TOOLTIPS.grade} /></span>
                  </th>
                  <th onClick={() => handleSort('last_close')} style={thStyle('last_close', 'right')}>
                    Close{sortArrow('last_close')}
                  </th>
                  <th onClick={() => handleSort('volume')} style={thStyle('volume', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Volume{sortArrow('volume')}<InfoIcon tooltip="Latest daily trading volume and relative volume (vs 20-day avg). Higher relative volume at bounce resistance strengthens the short signal." /></span>
                  </th>
                  <th onClick={() => handleSort('rsi')} style={thStyle('rsi', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>RSI{sortArrow('rsi')}<InfoIcon tooltip="Relative Strength Index (14-period). In a bearish bounce context, RSI 50-70 confirms the bounce toward resistance without reversing the downtrend." /></span>
                  </th>
                  <th onClick={() => handleSort('sma200')} style={thStyle('sma200', 'center')}>
                    <span style={{ whiteSpace: 'nowrap', opacity: semantics.baselineLabel ? 1 : 0.55 }}>
                      {semantics.baselineLabel ? `<${semantics.baselineLabel}` : '<SMA n/a'}
                      {sortArrow('sma200')}
                      <InfoIcon tooltip={semantics.baselineLabel
                        ? PILLAR_TOOLTIPS.sma200
                        : 'Not evaluated at this interval — the intraday scan skips the 200-period baseline entirely and tightens the EMA stack requirement instead.'} />
                    </span>
                  </th>
                  <th onClick={() => handleSort('stoch_k')} style={thStyle('stoch_k', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Stoch %K{sortArrow('stoch_k')}<InfoIcon tooltip={PILLAR_TOOLTIPS.stochastic} /></span>
                  </th>
                  <th onClick={() => handleSort('adx')} style={thStyle('adx', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>ADX{sortArrow('adx')}<InfoIcon tooltip={PILLAR_TOOLTIPS.adx} /></span>
                  </th>
                  <th onClick={() => handleSort('dist_to_ema21_pct')} style={thStyle('dist_to_ema21_pct', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Dist EMA21{sortArrow('dist_to_ema21_pct')}<InfoIcon tooltip={PILLAR_TOOLTIPS.rubberBand} /></span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={9} style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-secondary)' }}>
                      No qualifying bearish bounce setups found
                    </td>
                  </tr>
                ) : (
                  filtered.map((r, idx) => {
                    const gc = GRADE_COLORS[r.grade] || GRADE_COLORS['C']
                    return (
                      <tr key={idx}>
                        <td>
                          <span className="ticker" onClick={() => navigate(`/ticker/${r.ticker}`)}>
                            {r.ticker}
                          </span>
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
                            color: r.rel_volume >= 1.5 ? 'var(--danger)' : 'var(--text-secondary)',
                            fontWeight: r.rel_volume >= 1.5 ? 600 : 400,
                          }}>
                            {r.rel_volume.toFixed(2)}x avg
                          </span>
                        </td>
                        <td style={{
                          textAlign: 'right',
                          color: r.rsi >= 70 ? 'var(--danger)' : r.rsi <= 30 ? 'var(--success)' : 'inherit',
                          fontWeight: r.rsi >= 70 || r.rsi <= 30 ? 600 : 400,
                        }}>
                          {r.rsi.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          {gateMark(r.below_sma200, !!semantics.baselineLabel, 'Not evaluated at this interval')}
                          {r.sma200 != null && (
                            <span style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', marginLeft: '4px' }}>
                              ${r.sma200.toFixed(0)}
                            </span>
                          )}
                        </td>
                        <td style={{ textAlign: 'right', color: r.stoch_k > 80 ? 'var(--danger)' : 'inherit', fontWeight: r.stoch_k > 80 ? 600 : 400 }}>
                          {r.stoch_k.toFixed(1)}
                        </td>
                        <td style={{ textAlign: 'right', color: r.adx >= 30 && r.adx <= 40 ? 'var(--danger)' : 'inherit', fontWeight: r.adx >= 30 && r.adx <= 40 ? 600 : 400 }}>
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

export default BearishBounce
