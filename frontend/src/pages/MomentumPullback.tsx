import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { scanMomentumPullback, MomentumPullbackScanResponse, getLatestPriceDate } from '../services/api'
import StreakPanel from '../components/StreakPanel'

const STRATEGY_TOOLTIP =
  'Finds stocks in strong uptrends with optimal pullback entries. Trade WITH the trend, enter ON the pullback.\n\n' +
  '1. TREND ANCHOR: Daily EMA 8>21>34>55>89 stack, weekly EMA 8>21>34, price above 200 SMA.\n' +
  '2. PULLBACK ZONE: Slow Stochastic %K < 40 (oversold in uptrend), ADX 20-60 (healthy trend), price within 1 ATR of EMA 21 (rubber band).\n' +
  '3. ENTRY SCORE: Composite A+ to C grade based on stochastic depth, EMA 21 proximity, stack alignment, relative volume, and ADX strength.'

const PILLAR_TOOLTIPS = {
  dailyStack: 'Daily EMA 8 > 21 > 34 > 55 > 89 — all five exponential moving averages aligned bullishly. Stack count shows how many consecutive pairs are in order (max 4).',
  weeklyStack: 'Weekly EMA 8 > 21 > 34 — confirms the uptrend on the higher timeframe. Resampled from daily data.',
  sma200: 'Price must be above the 200-day SMA, confirming a long-term uptrend. Institutional investors use this as a baseline health indicator.',
  stochastic: 'Slow Stochastic %K (14,3,3). Values below 40 indicate the stock has pulled back within an uptrend — an "oversold in context" reading, not a weakening trend.',
  adx: 'Average Directional Index (14). Values 20-60 indicate a healthy trending market. Below 20 = no trend; above 60 = overextended.',
  rubberBand: 'Price must be within 1 ATR (14) of EMA 21. Like a rubber band stretched from the mean — it snaps back. This ensures price is near support.',
  relVolume: 'Current volume vs 20-day average. Higher relative volume at pullback support strengthens the entry signal.',
  grade: 'Entry Quality Score — A+ (≥90), A (≥80), B+ (≥70), B (≥60), C (<60). Weighted by: Stoch depth (30%), EMA21 proximity (25%), stack alignment (20%), rel. volume (15%), ADX (10%).',
}

const GRADE_COLORS: Record<string, { bg: string; text: string }> = {
  'A+': { bg: '#00c853', text: '#fff' },
  'A':  { bg: '#4caf50', text: '#fff' },
  'B+': { bg: '#ff9800', text: '#fff' },
  'B':  { bg: '#ffc107', text: '#000' },
  'C':  { bg: '#9e9e9e', text: '#fff' },
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
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [gradeFilter, setGradeFilter] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('score')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [scanDate, setScanDate] = useState('')
  const [latestDate, setLatestDate] = useState('')
  const [interval, setInterval] = useState<'5m' | '15m' | '30m' | '1h' | '1d'>('1d')

  const { data, isFetching: loading } = useQuery<MomentumPullbackScanResponse>({
    queryKey: ['scan', 'momentum-pullback', interval, scanDate],
    queryFn: () => scanMomentumPullback(undefined, scanDate || undefined, interval),
    placeholderData: keepPreviousData,
  })

  const handleRefresh = useCallback(async () => {
    const key = ['scan', 'momentum-pullback', interval, scanDate]
    queryClient.setQueryData(key, undefined)
    await queryClient.fetchQuery({ queryKey: key, queryFn: () => scanMomentumPullback(undefined, scanDate || undefined, interval, true) })
  }, [interval, scanDate, queryClient])

  useEffect(() => { getLatestPriceDate().then(setLatestDate).catch(() => {}) }, [])

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

  return (
    <div>
      {/* Header */}
      <div className="card-header" style={{ border: 'none', padding: 0, marginBottom: '1.5rem' }}>
        <div>
          <h1 style={{ fontSize: '1.75rem', fontWeight: 700 }}>
            📈 Momentum Pullback
            <InfoIcon tooltip={STRATEGY_TOOLTIP} />
          </h1>
          <p style={{ margin: '0.25rem 0 0', color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
            Finds elite stocks in strong uptrends with optimal pullback entry opportunities.
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <label style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Scan Date:</label>
            <input
              type="date"
              value={scanDate || latestDate}
              onChange={(e) => { setScanDate(e.target.value); }}
              style={{ padding: '0.35rem 0.5rem', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.85rem' }}
            />
            {scanDate && (
              <button
                className="btn btn-secondary"
                onClick={() => setScanDate('')}
                style={{ padding: '0.25rem 0.5rem', fontSize: '0.8rem' }}
                title="Reset to latest"
              >
                ✕
              </button>
            )}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <label style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Interval:</label>
            {(['5m', '15m', '30m', '1h', '1d'] as const).map((iv) => (
              <button
                key={iv}
                onClick={() => { setInterval(iv); }}
                style={{
                  padding: '0.25rem 0.5rem',
                  fontSize: '0.78rem',
                  borderRadius: '4px',
                  border: interval === iv ? '2px solid var(--primary-color)' : '1px solid var(--border)',
                  background: interval === iv ? 'var(--primary-color)' : 'transparent',
                  color: interval === iv ? '#fff' : 'var(--text-secondary)',
                  cursor: 'pointer',
                  fontWeight: interval === iv ? 600 : 400,
                }}
                title={iv === '1d' ? 'Daily candles' : iv === '1h' ? 'Hourly candles' : `${iv} candles (intraday)`}
              >
                {iv}
              </button>
            ))}
          </div>
          <button className="btn btn-primary" onClick={handleRefresh} disabled={loading}>
            {loading ? 'Scanning...' : 'Refresh'}
          </button>
        </div>
      </div>

      <StreakPanel strategy="momentum-pullback" />

      {/* Methodology card */}
      <div className="card" style={{ marginBottom: '1.5rem', background: 'var(--bg-secondary)', border: '1px solid var(--border)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
          <span style={{ fontSize: '1.5rem' }}>📊</span>
          <div>
            <div style={{ fontWeight: 700, fontSize: '1.1rem' }}>Elite Momentum Pullback Methodology</div>
            <div style={{ fontSize: '0.8rem', color: 'var(--success)', fontWeight: 600, letterSpacing: '0.05em', textTransform: 'uppercase' }}>High-Probability Trend Trading</div>
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '1.25rem' }}>
          <div>
            <h4 style={{ color: 'var(--success)', marginBottom: '0.5rem' }}>1. The Trend Anchor</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>Daily Stack:</strong> EMA 8 {'>'} 21 {'>'} 34 {'>'} 55 {'>'} 89
              <InfoIcon tooltip={PILLAR_TOOLTIPS.dailyStack} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>Weekly Stack:</strong> EMA 8 {'>'} 21 {'>'} 34
              <InfoIcon tooltip={PILLAR_TOOLTIPS.weeklyStack} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              {checkMark(true)} <strong>SMA 200:</strong> Price above 200-day MA
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
              {checkMark(true)} <strong>ADX Strength:</strong> 20 – 60
              <InfoIcon tooltip={PILLAR_TOOLTIPS.adx} />
            </p>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              <span style={{ color: '#ff9800', fontWeight: 700 }}>!</span> <strong>Rubber Band:</strong> Within 1 ATR of EMA 21
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

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <span>Scanning momentum pullback setups across all tickers...</span>
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
                    <span style={{ whiteSpace: 'nowrap' }}>Volume{sortArrow('volume')}<InfoIcon tooltip="Latest daily trading volume and relative volume (vs 20-day avg). Higher relative volume at pullback support strengthens the signal." /></span>
                  </th>
                  <th onClick={() => handleSort('rsi')} style={thStyle('rsi', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>RSI{sortArrow('rsi')}<InfoIcon tooltip="Relative Strength Index (14-period). Below 30 = oversold, above 70 = overbought. In a momentum pullback context, RSI 30-50 confirms the pullback without breaking the trend." /></span>
                  </th>
                  <th onClick={() => handleSort('sma200')} style={thStyle('sma200', 'center')}>
                    <span style={{ whiteSpace: 'nowrap' }}>{'>'}200 SMA{sortArrow('sma200')}<InfoIcon tooltip={PILLAR_TOOLTIPS.sma200} /></span>
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
                      No qualifying momentum pullback setups found
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
                          {checkMark(r.above_sma200)}
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
