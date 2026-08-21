import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { scanFibonacci, FibonacciScanResponse, FibTarget, FibExtension, getLatestPriceDate } from '../services/api'
import StreakPanel from '../components/StreakPanel'

const STRATEGY_TOOLTIP =
  'Fibonacci Retracement uses the golden ratio (and related ratios) to identify likely support/resistance levels after a significant price swing.\n\n' +
  '1. SWING DETECTION: Zigzag pivot algorithm finds meaningful highs and lows (min 5% swing).\n' +
  '2. LEVEL COMPUTATION: 23.6%, 38.2%, 50%, 61.8%, 78.6% retracement levels between swing points.\n' +
  '3. SIGNAL: Price proximity to nearest Fibonacci level determines actionability.'

const LEVEL_TOOLTIPS: Record<string, string> = {
  '23.6%': 'Shallow retracement — strong trend barely pauses. Fast-moving momentum stocks retrace here.',
  '38.2%': 'Prime entry zone. Most institutional buying/selling happens at this level.',
  '50.0%': 'Half retracement. Not a true Fibonacci number but universally watched by traders.',
  '61.8%': 'The Golden Ratio — last strong support/resistance before the trend is questioned.',
  '78.6%': 'Deep retracement — if this level breaks, the original trend is likely over.',
}

const SWING_PRESETS = [
  { label: 'Short-term', value: 3, tip: '3% — small swings, day/swing trade levels (days–2 weeks)' },
  { label: 'Standard', value: 5, tip: '5% — medium institutional swings (1–4 weeks)' },
  { label: 'Major', value: 8, tip: '8% — major trend pivots, position trades (weeks–months)' },
  { label: 'Structural', value: 12, tip: '12% — massive multi-month swings, long-term levels' },
] as const

const SIGNAL_COLORS: Record<string, { bg: string; text: string }> = {
  'Near Fib 23.6%':  { bg: '#e8f5e9', text: '#2e7d32' },
  'Near Fib 38.2%':  { bg: '#c8e6c9', text: '#1b5e20' },
  'Near Fib 50.0%':  { bg: '#fff3e0', text: '#e65100' },
  'Near Fib 61.8%':  { bg: '#ffd700', text: '#000' },
  'Near Fib 78.6%':  { bg: '#ffebee', text: '#c62828' },
  'Between Levels':  { bg: '#f5f5f5', text: '#616161' },
  'Below All Levels': { bg: '#ffcdd2', text: '#b71c1c' },
  'Above All Levels': { bg: '#ffcdd2', text: '#b71c1c' },
}

const TREND_COLORS: Record<string, { bg: string; text: string }> = {
  'uptrend_retracement':   { bg: '#e3f2fd', text: '#1565c0' },
  'downtrend_retracement': { bg: '#fce4ec', text: '#880e4f' },
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

type SortKey = 'ticker' | 'last_close' | 'distance_pct' | 'retracement_pct' | 'swing_size_pct' | 'swing_high' | 'swing_low' | 'fib_236' | 'fib_382' | 'fib_500' | 'fib_618' | 'fib_786'
type SortDir = 'asc' | 'desc'

function FibonacciScreener() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [signalFilter, setSignalFilter] = useState('')
  const [trendFilter, setTrendFilter] = useState('')
  const [zoneFilter, setZoneFilter] = useState('')
  const [maxDistance, setMaxDistance] = useState('')
  const [minRetrace, setMinRetrace] = useState('')
  const [maxRetrace, setMaxRetrace] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('distance_pct')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [scanDate, setScanDate] = useState('')
  const [latestDate, setLatestDate] = useState('')
  const [minSwing, setMinSwing] = useState(5)
  const [interval, setInterval] = useState<'5m' | '15m' | '30m' | '1h' | '1d'>('1d')

  const { data, isFetching: loading } = useQuery<FibonacciScanResponse>({
    queryKey: ['scan', 'fibonacci', interval, scanDate, minSwing],
    queryFn: () => scanFibonacci(undefined, scanDate || undefined, minSwing, interval),
    placeholderData: keepPreviousData,
  })

  const handleRefresh = useCallback(async () => {
    const key = ['scan', 'fibonacci', interval, scanDate, minSwing]
    queryClient.setQueryData(key, undefined)
    await queryClient.fetchQuery({ queryKey: key, queryFn: () => scanFibonacci(undefined, scanDate || undefined, minSwing, interval, true) })
  }, [interval, scanDate, minSwing, queryClient])

  useEffect(() => { getLatestPriceDate().then(setLatestDate).catch(() => {}) }, [])

  const filtered = useMemo(() => {
    if (!data?.results) return []
    let items = data.results
    if (filter) items = items.filter(r => r.ticker.toLowerCase().includes(filter.toLowerCase()))
    if (signalFilter) items = items.filter(r => r.signal === signalFilter)
    if (trendFilter) items = items.filter(r => r.trend_direction === trendFilter)
    if (zoneFilter) items = items.filter(r => r.zone === zoneFilter)
    if (maxDistance) items = items.filter(r => Math.abs(r.distance_pct) <= Number(maxDistance))
    if (minRetrace) items = items.filter(r => r.retracement_pct >= Number(minRetrace))
    if (maxRetrace) items = items.filter(r => r.retracement_pct <= Number(maxRetrace))
    items = [...items].sort((a, b) => {
      const av = a[sortKey] ?? -Infinity
      const bv = b[sortKey] ?? -Infinity
      if (typeof av === 'string' && typeof bv === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? (Math.abs(av as number)) - (Math.abs(bv as number)) : (Math.abs(bv as number)) - (Math.abs(av as number))
    })
    return items
  }, [data, filter, signalFilter, trendFilter, zoneFilter, maxDistance, minRetrace, maxRetrace, sortKey, sortDir])

  const allSignals = useMemo(() => {
    if (!data?.results) return []
    return Array.from(new Set(data.results.map(r => r.signal))).sort()
  }, [data])

  const allZones = useMemo(() => {
    if (!data?.results) return []
    return Array.from(new Set(data.results.map(r => r.zone))).sort()
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

  const trendLabel = (td: string) => td === 'uptrend_retracement' ? '↑ Pullback' : '↓ Bounce'

  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set())
  const toggleExpand = (ticker: string) => {
    setExpandedRows(prev => {
      const next = new Set(prev)
      if (next.has(ticker)) next.delete(ticker)
      else next.add(ticker)
      return next
    })
  }

  const targetPill = (t: FibTarget, color: string) => (
    <span key={t.level} style={{
      display: 'inline-block', padding: '0.15rem 0.45rem', borderRadius: '10px',
      fontSize: '0.72rem', fontWeight: 600, background: color, marginRight: '0.3rem', marginBottom: '0.2rem',
      whiteSpace: 'nowrap',
    }}>
      {t.level} · ${t.price.toFixed(2)}{t.pct != null ? ` (${t.pct > 0 ? '+' : ''}${t.pct.toFixed(1)}%)` : ''}
    </span>
  )

  const extPill = (e: FibExtension) => (
    <span key={e.level} style={{
      display: 'inline-block', padding: '0.15rem 0.45rem', borderRadius: '10px',
      fontSize: '0.72rem', fontWeight: 600, background: '#e8eaf6', color: '#283593',
      marginRight: '0.3rem', marginBottom: '0.2rem', whiteSpace: 'nowrap',
    }}>
      {e.level} · ${e.price.toFixed(2)}
    </span>
  )

  return (
    <div>
      {/* Header */}
      <div className="card-header" style={{ border: 'none', padding: 0, marginBottom: '1.5rem' }}>
        <div>
          <h1 style={{ fontSize: '1.75rem', fontWeight: 700 }}>
            📐 Fibonacci Retracement
            <InfoIcon tooltip={STRATEGY_TOOLTIP} />
          </h1>
          <p style={{ margin: '0.25rem 0 0', color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
            Identifies key support &amp; resistance levels from zigzag price swings.
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem' }}>
            <label style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Swing:</label>
            {SWING_PRESETS.map(p => (
              <button
                key={p.value}
                title={p.tip}
                onClick={() => setMinSwing(p.value)}
                className={minSwing === p.value ? 'btn btn-primary' : 'btn btn-secondary'}
                style={{
                  padding: '0.25rem 0.55rem', fontSize: '0.78rem', fontWeight: 600,
                  borderRadius: '14px', whiteSpace: 'nowrap',
                  ...(minSwing === p.value ? {} : { opacity: 0.7 }),
                }}
              >
                {p.label}
              </button>
            ))}
            <input
              type="number"
              min={3}
              max={15}
              step={1}
              value={minSwing}
              onChange={(e) => setMinSwing(Number(e.target.value))}
              style={{ width: '45px', padding: '0.3rem 0.4rem', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.82rem', textAlign: 'center' }}
            />
            <span style={{ fontSize: '0.82rem', color: 'var(--text-secondary)' }}>%</span>
          </div>
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

      {/* Methodology card */}
      <div className="card" style={{ marginBottom: '1.5rem', background: 'var(--bg-secondary)', border: '1px solid var(--border)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
          <span style={{ fontSize: '1.5rem' }}>📊</span>
          <div>
            <div style={{ fontWeight: 700, fontSize: '1.1rem' }}>Zigzag Fibonacci Methodology</div>
            <div style={{ fontSize: '0.8rem', color: 'var(--primary)', fontWeight: 600, letterSpacing: '0.05em', textTransform: 'uppercase' }}>Pivot-Based Retracement Levels</div>
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1.25rem' }}>
          <div>
            <h4 style={{ color: 'var(--primary)', marginBottom: '0.5rem' }}>1. Swing Detection</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              Zigzag pivots with min <strong>{minSwing}%</strong> swing. Finds meaningful highs/lows, filtering noise.
            </p>
          </div>
          <div>
            <h4 style={{ color: 'var(--primary)', marginBottom: '0.5rem' }}>2. Fibonacci Levels</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              5 key ratios: <strong>23.6%</strong>, <strong>38.2%</strong>, <strong>50%</strong>, <strong>61.8%</strong>, <strong>78.6%</strong>
              <InfoIcon tooltip="Levels computed between the most recent swing high and swing low. In uptrend retracements, levels are support. In downtrend retracements, levels are resistance." />
            </p>
          </div>
          <div>
            <h4 style={{ color: 'var(--primary)', marginBottom: '0.5rem' }}>3. Proximity Signal</h4>
            <p style={{ fontSize: '0.85rem', margin: '0.25rem 0' }}>
              "Near Fib" when price is within <strong>1.5%</strong> of a level — the actionable zone.
            </p>
          </div>
          <div>
            <h4 style={{ color: 'var(--primary)', marginBottom: '0.5rem' }}>4. Swing Size Guide</h4>
            <p style={{ fontSize: '0.8rem', margin: '0.15rem 0', color: 'var(--text-secondary)' }}>
              <strong>3–5%</strong>: Short-term swings — day/swing trades
            </p>
            <p style={{ fontSize: '0.8rem', margin: '0.15rem 0', color: 'var(--text-secondary)' }}>
              <strong>5–8%</strong>: Institutional swings — standard entries
            </p>
            <p style={{ fontSize: '0.8rem', margin: '0.15rem 0', color: 'var(--text-secondary)' }}>
              <strong>8–12%</strong>: Major pivots — position trades
            </p>
            <p style={{ fontSize: '0.8rem', margin: '0.15rem 0', color: 'var(--text-secondary)' }}>
              <strong>12–15%</strong>: Structural levels — long-term
            </p>
          </div>
        </div>
      </div>

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <span>Scanning Fibonacci levels across all tickers...</span>
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
              value={signalFilter}
              onChange={(e) => setSignalFilter(e.target.value)}
              style={{ minWidth: '140px' }}
            >
              <option value="">All Signals</option>
              {allSignals.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <select
              value={trendFilter}
              onChange={(e) => setTrendFilter(e.target.value)}
              style={{ minWidth: '160px' }}
            >
              <option value="">All Trends</option>
              <option value="uptrend_retracement">↑ Uptrend Pullback</option>
              <option value="downtrend_retracement">↓ Downtrend Bounce</option>
            </select>
            <select
              value={zoneFilter}
              onChange={(e) => setZoneFilter(e.target.value)}
              style={{ minWidth: '140px' }}
            >
              <option value="">All Zones</option>
              {allZones.map(z => <option key={z} value={z}>{z}</option>)}
            </select>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
              <label style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Dist ≤</label>
              <input
                type="number"
                min={0}
                step={0.5}
                placeholder="%"
                value={maxDistance}
                onChange={(e) => setMaxDistance(e.target.value)}
                style={{ width: '52px', padding: '0.3rem 0.4rem', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.82rem', textAlign: 'center' }}
              />
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
              <label style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Retrace</label>
              <input
                type="number"
                min={0}
                max={100}
                step={5}
                placeholder="Min"
                value={minRetrace}
                onChange={(e) => setMinRetrace(e.target.value)}
                style={{ width: '48px', padding: '0.3rem 0.4rem', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.82rem', textAlign: 'center' }}
              />
              <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>–</span>
              <input
                type="number"
                min={0}
                max={100}
                step={5}
                placeholder="Max"
                value={maxRetrace}
                onChange={(e) => setMaxRetrace(e.target.value)}
                style={{ width: '48px', padding: '0.3rem 0.4rem', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.82rem', textAlign: 'center' }}
              />
              <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>%</span>
            </div>
            <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
              {data.total_scanned} scanned &middot; {filtered.length} results
            </span>
          </div>

          {/* Results table */}
          <div className="card" style={{ padding: 0, overflowX: 'auto' }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th style={{ width: '28px' }}></th>
                  <th onClick={() => handleSort('ticker')} style={thStyle('ticker')}>
                    Ticker{sortArrow('ticker')}
                  </th>
                  <th style={{ textAlign: 'center' }}>Signal</th>
                  <th style={{ textAlign: 'center' }}>Trend</th>
                  <th onClick={() => handleSort('last_close')} style={thStyle('last_close', 'right')}>
                    Close{sortArrow('last_close')}
                  </th>
                  <th style={{ textAlign: 'center' }}>
                    Zone
                    <InfoIcon tooltip="The Fibonacci zone the price currently sits in (between which two levels)." />
                  </th>
                  <th style={{ textAlign: 'center' }}>
                    Nearest Level
                    <InfoIcon tooltip="The closest Fibonacci level to the current price." />
                  </th>
                  <th onClick={() => handleSort('distance_pct')} style={thStyle('distance_pct', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Distance %{sortArrow('distance_pct')}<InfoIcon tooltip="How far the current price is from the nearest Fibonacci level. Closer to 0% = more actionable." /></span>
                  </th>
                  <th onClick={() => handleSort('retracement_pct')} style={thStyle('retracement_pct', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Retrace %{sortArrow('retracement_pct')}<InfoIcon tooltip="How much of the swing has been retraced. 0% = at the extreme, 100% = fully retraced." /></span>
                  </th>
                  <th onClick={() => handleSort('swing_size_pct')} style={thStyle('swing_size_pct', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Swing %{sortArrow('swing_size_pct')}<InfoIcon tooltip="Total swing size as a percentage. Larger swings produce more reliable Fibonacci levels." /></span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={10} style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-secondary)' }}>
                      No Fibonacci signals found
                    </td>
                  </tr>
                ) : (
                  filtered.map((r, idx) => {
                    const sc = SIGNAL_COLORS[r.signal] || { bg: '#f5f5f5', text: '#333' }
                    const tc = TREND_COLORS[r.trend_direction] || { bg: '#f5f5f5', text: '#333' }
                    const expanded = expandedRows.has(r.ticker)
                    return (
                      <>
                      <tr key={idx} onClick={() => toggleExpand(r.ticker)} style={{ cursor: 'pointer' }}>
                        <td style={{ textAlign: 'center', fontSize: '0.75rem', width: '28px', color: 'var(--text-secondary)' }}>
                          {expanded ? '▼' : '▶'}
                        </td>
                        <td>
                          <span className="ticker" onClick={(e) => { e.stopPropagation(); navigate(`/ticker/${r.ticker}`) }}>
                            {r.ticker}
                          </span>
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          <span style={{
                            display: 'inline-block', padding: '0.2rem 0.6rem', borderRadius: '12px',
                            fontSize: '0.78rem', fontWeight: 600, background: sc.bg, color: sc.text, whiteSpace: 'nowrap',
                          }}>
                            {r.signal}
                          </span>
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          <span style={{
                            display: 'inline-block', padding: '0.2rem 0.6rem', borderRadius: '12px',
                            fontSize: '0.78rem', fontWeight: 600, background: tc.bg, color: tc.text, whiteSpace: 'nowrap',
                          }}>
                            {trendLabel(r.trend_direction)}
                          </span>
                        </td>
                        <td style={{ textAlign: 'right', fontWeight: 600 }}>
                          ${r.last_close.toFixed(2)}
                        </td>
                        <td style={{ textAlign: 'center', fontSize: '0.8rem' }}>
                          {r.zone}
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          <span
                            title={LEVEL_TOOLTIPS[r.nearest_level] || ''}
                            style={{ cursor: 'help', fontWeight: 700, color: r.nearest_level === '61.8%' ? '#b8860b' : 'inherit' }}
                          >
                            {r.nearest_level}
                          </span>
                        </td>
                        <td style={{
                          textAlign: 'right', fontWeight: 600,
                          color: Math.abs(r.distance_pct) <= 1.5 ? 'var(--success)' : 'var(--text-secondary)',
                        }}>
                          {r.distance_pct > 0 ? '+' : ''}{r.distance_pct.toFixed(2)}%
                        </td>
                        <td style={{ textAlign: 'right' }}>
                          {r.retracement_pct.toFixed(1)}%
                        </td>
                        <td style={{
                          textAlign: 'right',
                          color: r.swing_size_pct >= 15 ? 'var(--success)' : r.swing_size_pct >= 10 ? 'var(--warning, #ff9800)' : 'var(--text-secondary)',
                          fontWeight: r.swing_size_pct >= 15 ? 700 : 400,
                        }}>
                          {r.swing_size_pct.toFixed(1)}%
                        </td>
                      </tr>
                      {expanded && (
                        <tr key={`${idx}-detail`} style={{ background: 'var(--bg-secondary)' }}>
                          <td colSpan={10} style={{ padding: '0.75rem 1rem' }}>
                            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '1rem' }}>

                              {/* Support Fibonacci Levels (from Swing High) */}
                              <div>
                                <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: '0.5rem', color: '#2e7d32' }}>
                                  🟢 Support Levels <span style={{ fontWeight: 400, fontSize: '0.75rem', color: 'var(--text-secondary)' }}>(from Swing High)</span>
                                </div>
                                <div style={{ fontSize: '0.8rem', lineHeight: '1.8' }}>
                                  <div style={{ display: 'flex', justifyContent: 'space-between', borderBottom: '1px solid var(--border)', paddingBottom: '0.15rem', marginBottom: '0.15rem' }}>
                                    <span style={{ color: 'var(--text-secondary)', fontWeight: 600 }}>Swing High</span>
                                    <span style={{ fontWeight: 600 }}>${r.swing_high.toFixed(2)} <span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{r.swing_high_date}</span></span>
                                  </div>
                                  {r.support_fibs.map(lv => {
                                    const isNearest = lv.name === r.nearest_support.name
                                    const isActive = lv.price < r.last_close
                                    const isGolden = lv.name === '61.8%'
                                    return (
                                      <div key={lv.name} style={{
                                        display: 'flex', justifyContent: 'space-between',
                                        background: isNearest ? 'rgba(76, 175, 80, 0.12)' : undefined,
                                        padding: '0.1rem 0.25rem',
                                        borderRadius: '4px',
                                        borderLeft: isNearest ? '3px solid #2e7d32' : isGolden ? '3px solid #b8860b' : '3px solid transparent',
                                        opacity: isActive ? 1 : 0.5,
                                      }}>
                                        <span style={{ color: isGolden ? '#b8860b' : '#2e7d32', fontWeight: isNearest || isGolden ? 700 : 400 }}>
                                          S {lv.name}{isNearest ? ' ◀' : ''}
                                        </span>
                                        <span style={{ fontWeight: isNearest ? 700 : 400 }}>${lv.price.toFixed(2)}</span>
                                      </div>
                                    )
                                  })}
                                  <div style={{ display: 'flex', justifyContent: 'space-between', borderTop: '1px solid var(--border)', paddingTop: '0.15rem', marginTop: '0.15rem' }}>
                                    <span style={{ color: 'var(--text-secondary)', fontWeight: 600 }}>Swing Low</span>
                                    <span style={{ fontWeight: 600 }}>${r.swing_low.toFixed(2)} <span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{r.swing_low_date}</span></span>
                                  </div>
                                  <div style={{ marginTop: '0.4rem', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                                    Nearest: <strong style={{ color: '#2e7d32' }}>S {r.nearest_support.name}</strong> ({r.nearest_support.distance_pct > 0 ? '+' : ''}{r.nearest_support.distance_pct.toFixed(1)}%)
                                  </div>
                                </div>
                              </div>

                              {/* Resistance Fibonacci Levels (from Swing Low) */}
                              <div>
                                <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: '0.5rem', color: '#c62828' }}>
                                  🔴 Resistance Levels <span style={{ fontWeight: 400, fontSize: '0.75rem', color: 'var(--text-secondary)' }}>(from Swing Low)</span>
                                </div>
                                <div style={{ fontSize: '0.8rem', lineHeight: '1.8' }}>
                                  <div style={{ display: 'flex', justifyContent: 'space-between', borderBottom: '1px solid var(--border)', paddingBottom: '0.15rem', marginBottom: '0.15rem' }}>
                                    <span style={{ color: 'var(--text-secondary)', fontWeight: 600 }}>Swing High</span>
                                    <span style={{ fontWeight: 600 }}>${r.swing_high.toFixed(2)}</span>
                                  </div>
                                  {[...r.resistance_fibs].reverse().map(lv => {
                                    const isNearest = lv.name === r.nearest_resistance.name
                                    const isActive = lv.price > r.last_close
                                    const isGolden = lv.name === '61.8%'
                                    return (
                                      <div key={lv.name} style={{
                                        display: 'flex', justifyContent: 'space-between',
                                        background: isNearest ? 'rgba(198, 40, 40, 0.1)' : undefined,
                                        padding: '0.1rem 0.25rem',
                                        borderRadius: '4px',
                                        borderLeft: isNearest ? '3px solid #c62828' : isGolden ? '3px solid #b8860b' : '3px solid transparent',
                                        opacity: isActive ? 1 : 0.5,
                                      }}>
                                        <span style={{ color: isGolden ? '#b8860b' : '#c62828', fontWeight: isNearest || isGolden ? 700 : 400 }}>
                                          R {lv.name}{isNearest ? ' ◀' : ''}
                                        </span>
                                        <span style={{ fontWeight: isNearest ? 700 : 400 }}>${lv.price.toFixed(2)}</span>
                                      </div>
                                    )
                                  })}
                                  <div style={{ display: 'flex', justifyContent: 'space-between', borderTop: '1px solid var(--border)', paddingTop: '0.15rem', marginTop: '0.15rem' }}>
                                    <span style={{ color: 'var(--text-secondary)', fontWeight: 600 }}>Swing Low</span>
                                    <span style={{ fontWeight: 600 }}>${r.swing_low.toFixed(2)}</span>
                                  </div>
                                  <div style={{ marginTop: '0.4rem', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                                    Nearest: <strong style={{ color: '#c62828' }}>R {r.nearest_resistance.name}</strong> ({r.nearest_resistance.distance_pct > 0 ? '+' : ''}{r.nearest_resistance.distance_pct.toFixed(1)}%)
                                  </div>
                                </div>
                              </div>

                              {/* Targets & Extensions */}
                              <div>
                                <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: '0.5rem' }}>
                                  🎯 Support Targets <span style={{ fontWeight: 400, fontSize: '0.75rem', color: 'var(--text-secondary)' }}>(if price declines)</span>
                                </div>
                                <div style={{ marginBottom: '0.5rem' }}>
                                  {r.support_targets.length > 0
                                    ? r.support_targets.map(t => targetPill(t, '#e8f5e9'))
                                    : <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>Price below all support levels</span>
                                  }
                                </div>

                                <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: '0.5rem', marginTop: '0.75rem' }}>
                                  🎯 Resistance Targets <span style={{ fontWeight: 400, fontSize: '0.75rem', color: 'var(--text-secondary)' }}>(if price rallies)</span>
                                </div>
                                <div style={{ marginBottom: '0.5rem' }}>
                                  {r.resistance_targets.length > 0
                                    ? r.resistance_targets.map(t => targetPill(t, '#ffebee'))
                                    : <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>Price above all resistance levels</span>
                                  }
                                </div>

                                <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: '0.5rem', marginTop: '0.75rem' }}>
                                  🚀 Extensions
                                  <InfoIcon tooltip="Extension levels project where price could go if it breaks beyond the swing extremes." />
                                </div>
                                <div style={{ marginBottom: '0.3rem' }}>
                                  <span style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', fontWeight: 600 }}>↑ Upside: </span>
                                  {r.upside_extensions.map(e => extPill(e))}
                                </div>
                                <div>
                                  <span style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', fontWeight: 600 }}>↓ Downside: </span>
                                  {r.downside_extensions.map(e => extPill(e))}
                                </div>
                              </div>

                            </div>
                          </td>
                        </tr>
                      )}
                      </>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
      <StreakPanel strategy="fibonacci" />
    </div>
  )
}

export default FibonacciScreener
