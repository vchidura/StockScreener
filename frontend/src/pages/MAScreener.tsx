import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { scanMACrossover, MAScanResponse, MAResult, getLatestPriceDate } from '../services/api'
import StreakPanel from '../components/StreakPanel'

type TabKey = 'bullish-cross' | 'bearish-cross' | 'bullish-trend' | 'bearish-trend'

const TABS: { key: TabKey; label: string; signals: string[]; color: string }[] = [
  { key: 'bullish-cross',  label: 'Bullish Crossover',  signals: ['Bullish Crossover', 'Recent Bullish'],   color: 'var(--success)' },
  { key: 'bearish-cross',  label: 'Bearish Crossover',  signals: ['Bearish Crossover', 'Recent Bearish'],   color: 'var(--danger)' },
  { key: 'bullish-trend',  label: 'Bullish Trend',      signals: ['Above MA'],                              color: '#2196f3' },
  { key: 'bearish-trend',  label: 'Bearish Trend',      signals: ['Below MA'],                              color: '#ff9800' },
]

const STRATEGY_TOOLTIP =
  'A Moving Average Crossover compares two Simple Moving Averages (SMA) of different lengths. ' +
  'The short-period SMA reacts faster to recent price changes while the long-period SMA smooths out noise and represents the broader trend.\n\n' +
  'Bullish Crossover: Short SMA crosses above the long SMA — potential shift to bullish momentum.\n' +
  'Bearish Crossover: Short SMA crosses below the long SMA — signals bearish momentum.\n' +
  'Recent Bullish/Bearish: Crossover occurred within the last 5 trading days.\n' +
  'Bullish/Bearish Trend: Short SMA has been above (or below) the long SMA for more than 5 days.'

const MARKER_COLORS: Record<string, { bg: string; text: string }> = {
  'Golden Cross':  { bg: '#ffd700', text: '#000' },
  'Death Cross':   { bg: '#1a1a2e', text: '#fff' },
  'Above 200 SMA': { bg: '#e8f5e9', text: '#2e7d32' },
  'Below 200 SMA': { bg: '#ffebee', text: '#c62828' },
  'Near 200 SMA':  { bg: '#fff3e0', text: '#e65100' },
  'Near 50 SMA':   { bg: '#e3f2fd', text: '#1565c0' },
  'Above 50W SMA': { bg: '#e0f2f1', text: '#00695c' },
  'Below 50W SMA': { bg: '#fce4ec', text: '#880e4f' },
  'Near 50W SMA':  { bg: '#f3e5f5', text: '#7b1fa2' },
  'Above 200W SMA':{ bg: '#e8f5e9', text: '#1b5e20' },
  'Below 200W SMA':{ bg: '#ffebee', text: '#b71c1c' },
  'Near 200W SMA': { bg: '#fff8e1', text: '#f57f17' },
}

const MARKER_DESCRIPTIONS: Record<string, string> = {
  'Golden Cross':  '50 SMA crossed above 200 SMA (last 10d) — strong bullish long-term signal.',
  'Death Cross':   '50 SMA crossed below 200 SMA (last 10d) — bearish long-term signal.',
  'Above 200 SMA': 'Price above 200 SMA — long-term uptrend.',
  'Below 200 SMA': 'Price below 200 SMA — long-term downtrend.',
  'Near 200 SMA':  'Price within 2% of 200 SMA — key support/resistance zone.',
  'Near 50 SMA':   'Price within 1% of 50 SMA — medium-term support/resistance.',
  'Above 50W SMA': 'Price above 50-week SMA — intermediate weekly uptrend.',
  'Below 50W SMA': 'Price below 50-week SMA — intermediate weekly downtrend.',
  'Near 50W SMA':  'Price within 2% of 50-week SMA — weekly support/resistance zone.',
  'Above 200W SMA':'Price above 200-week SMA — long-term secular uptrend (very strong).',
  'Below 200W SMA':'Price below 200-week SMA — long-term secular downtrend (very weak).',
  'Near 200W SMA': 'Price within 3% of 200-week SMA — generational support/resistance level.',
}

const MARKERS_LEGEND =
  'Markers use fixed 50/200 SMAs, independent of your short/long MA settings. A Bullish Crossover + Death Cross means short-term up, long-term down.\n\n' +
  Object.entries(MARKER_DESCRIPTIONS)
    .map(([name, desc]) => `${name}: ${desc}`)
    .join('\n')

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

type SortKey = 'ticker' | 'last_close' | 'short_ma' | 'long_ma' | 'ma_spread_pct' |
  'days_since_cross' | 'price_change_since_cross_pct' |
  'weekly_short_ma' | 'weekly_long_ma' | 'weekly_spread_pct'
type SortDir = 'asc' | 'desc'

function MAScreener() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [markerFilter, setMarkerFilter] = useState('')
  const [weeklyFilter, setWeeklyFilter] = useState('')
  const [presetFilter, setPresetFilter] = useState('')
  const [shortPeriod, setShortPeriod] = useState(9)
  const [longPeriod, setLongPeriod] = useState(21)
  const [activeTab, setActiveTab] = useState<TabKey>('bullish-cross')
  const [sortKey, setSortKey] = useState<SortKey | null>(null)
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [scanDate, setScanDate] = useState('')
  const [latestDate, setLatestDate] = useState('')
  const [interval, setInterval] = useState<'5m' | '15m' | '30m' | '1h' | '1d'>('1d')

  const { data, isFetching: loading } = useQuery<MAScanResponse>({
    queryKey: ['scan', 'ma-crossover', interval, scanDate, shortPeriod, longPeriod],
    queryFn: () => scanMACrossover(undefined, shortPeriod, longPeriod, scanDate || undefined, interval),
    placeholderData: keepPreviousData,
  })

  const handleRefresh = useCallback(async () => {
    const key = ['scan', 'ma-crossover', interval, scanDate, shortPeriod, longPeriod]
    queryClient.setQueryData(key, undefined)
    await queryClient.fetchQuery({ queryKey: key, queryFn: () => scanMACrossover(undefined, shortPeriod, longPeriod, scanDate || undefined, interval, true) })
  }, [interval, scanDate, shortPeriod, longPeriod, queryClient])

  useEffect(() => { getLatestPriceDate().then(setLatestDate).catch(() => {}) }, [])

  const getTabItems = (tab: typeof TABS[number]): MAResult[] => {
    if (!data?.results_by_signal) return []
    return tab.signals.flatMap(sig => data.results_by_signal[sig] ?? [])
  }

  // Collect all unique markers across all results for filter dropdown
  const allMarkers = useMemo(() => {
    if (!data?.results) return []
    const set = new Set<string>()
    data.results.forEach(r => r.markers?.forEach(m => set.add(m)))
    return Array.from(set).sort()
  }, [data])

  const activeTabDef = TABS.find(t => t.key === activeTab)!

  // Determine if we're in cross-tab mode (any filter besides ticker search is active)
  const isCrossTab = !!(markerFilter || weeklyFilter || presetFilter)

  // Apply filters to a list of items (shared logic)
  const applyFilters = (items: MAResult[]): MAResult[] => {
    if (filter) items = items.filter(r => r.ticker.toLowerCase().includes(filter.toLowerCase()))
    if (markerFilter) items = items.filter(r => r.markers?.includes(markerFilter))
    if (weeklyFilter) items = items.filter(r => r.weekly_signal === weeklyFilter)
    if (presetFilter === 'weekly_confirmed') {
      items = items.filter(r => {
        const sig = r.signal
        const ws = r.weekly_signal
        const bull = sig.includes('Bullish') && (ws === 'W-Above' || ws === 'W-Bullish Cross')
        const bear = sig.includes('Bearish') && (ws === 'W-Below' || ws === 'W-Bearish Cross')
        return bull || bear
      })
    } else if (presetFilter === 'counter_trend') {
      items = items.filter(r => {
        const sig = r.signal
        const ws = r.weekly_signal
        const bullDaily = sig.includes('Bullish')
        const bearDaily = sig.includes('Bearish')
        return (bullDaily && (ws === 'W-Below' || ws === 'W-Bearish Cross')) ||
               (bearDaily && (ws === 'W-Above' || ws === 'W-Bullish Cross'))
      })
    } else if (presetFilter === 'wide_spread') {
      items = items.filter(r => Math.abs(r.ma_spread_pct) >= 2)
    } else if (presetFilter === 'narrow_spread') {
      items = items.filter(r => Math.abs(r.ma_spread_pct) < 1)
    } else if (presetFilter === 'fresh_cross') {
      items = items.filter(r => r.days_since_cross != null && r.days_since_cross <= 3)
    } else if (presetFilter === 'weekly_cross') {
      items = items.filter(r => r.weekly_signal === 'W-Bullish Cross' || r.weekly_signal === 'W-Bearish Cross')
    }
    return items
  }

  // Sort items
  const applySorting = (items: MAResult[]): MAResult[] => {
    if (!sortKey) return items
    return [...items].sort((a, b) => {
      const av = a[sortKey] ?? -Infinity
      const bv = b[sortKey] ?? -Infinity
      if (typeof av === 'string' && typeof bv === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number)
    })
  }

  // Per-tab filtered counts (for tab badges when filters are active)
  const tabFilteredCounts = useMemo(() => {
    const counts: Record<TabKey, number> = { 'bullish-cross': 0, 'bearish-cross': 0, 'bullish-trend': 0, 'bearish-trend': 0 }
    for (const tab of TABS) {
      counts[tab.key] = applyFilters(getTabItems(tab)).length
    }
    return counts
  }, [data, filter, markerFilter, weeklyFilter, presetFilter])

  const tabItems = useMemo(() => {
    if (isCrossTab) {
      // Cross-tab mode: pull from ALL tabs
      const all = TABS.flatMap(tab => getTabItems(tab))
      return applySorting(applyFilters(all))
    }
    // Single-tab mode
    return applySorting(applyFilters(getTabItems(activeTabDef)))
  }, [data, activeTab, filter, markerFilter, weeklyFilter, presetFilter, sortKey, sortDir])

  const spreadColor = (pct: number) => (pct >= 0 ? 'var(--success)' : 'var(--danger)')

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const sortArrow = (key: SortKey) => {
    if (sortKey !== key) return ' ↕'
    return sortDir === 'asc' ? ' ↑' : ' ↓'
  }

  const thStyle = (_key: SortKey, align: 'left' | 'right' | 'center' = 'left'): React.CSSProperties => ({
    textAlign: align,
    cursor: 'pointer',
    userSelect: 'none',
    whiteSpace: 'nowrap',
  })

  // Cross-tab ticker search: auto-switch to tab containing searched ticker
  // Only active when NOT in cross-tab filter mode (filters already show all tabs)
  useEffect(() => {
    if (isCrossTab || !filter || !data?.results_by_signal) return
    const lc = filter.toLowerCase()
    const currentItems = getTabItems(activeTabDef)
    if (currentItems.some(r => r.ticker.toLowerCase().includes(lc))) return
    for (const tab of TABS) {
      if (tab.key === activeTab) continue
      const items = getTabItems(tab)
      if (items.some(r => r.ticker.toLowerCase().includes(lc))) {
        setActiveTab(tab.key)
        return
      }
    }
  }, [filter])

  return (
    <div>
      {/* Header */}
      <div className="card-header" style={{ border: 'none', padding: 0, marginBottom: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <h1 style={{ fontSize: '1.5rem', fontWeight: 700, margin: 0 }}>
            MA Crossover
            <InfoIcon tooltip={STRATEGY_TOOLTIP} />
          </h1>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <label style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Short:</label>
            <input
              type="number"
              value={shortPeriod}
              onChange={(e) => setShortPeriod(Number(e.target.value))}
              min={2}
              max={50}
              style={{ width: '60px', padding: '0.3rem 0.4rem', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.85rem' }}
              title={`Short MA (${shortPeriod}): Fast-moving average period in days.`}
            />
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <label style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Long:</label>
            <input
              type="number"
              value={longPeriod}
              onChange={(e) => setLongPeriod(Number(e.target.value))}
              min={5}
              max={200}
              style={{ width: '60px', padding: '0.3rem 0.4rem', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.85rem' }}
              title={`Long MA (${longPeriod}): Slow-moving average period in days.`}
            />
          </div>
          <button className="btn btn-secondary" onClick={() => handleRefresh()} style={{ padding: '0.3rem 0.7rem', fontSize: '0.82rem' }}>
            Apply
          </button>
          <div style={{ width: '1px', height: '20px', background: 'var(--border)' }} />
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <label style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Date:</label>
            <input
              type="date"
              value={scanDate || latestDate}
              onChange={(e) => { setScanDate(e.target.value); }}
              style={{ padding: '0.3rem 0.4rem', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.85rem' }}
            />
            {scanDate && (
              <button
                className="btn btn-secondary"
                onClick={() => setScanDate('')}
                style={{ padding: '0.2rem 0.4rem', fontSize: '0.78rem' }}
                title="Reset to latest"
              >
                ✕
              </button>
            )}
          </div>
          <div style={{ width: '1px', height: '20px', background: 'var(--border)' }} />
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
          <button className="btn btn-primary" onClick={handleRefresh} disabled={loading} style={{ padding: '0.3rem 0.7rem', fontSize: '0.82rem' }}>
            {loading ? 'Scanning...' : 'Refresh'}
          </button>
        </div>
      </div>

      <StreakPanel strategy="ma-crossover" shortPeriod={shortPeriod} longPeriod={longPeriod} />

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <span>Scanning {shortPeriod}/{longPeriod} MA crossovers ({interval}) across all tickers...</span>
        </div>
      )}

      {!loading && data && (
        <>
          {/* Tabs */}
          <div style={{
            display: 'flex',
            gap: '0',
            borderBottom: '2px solid var(--border)',
            marginBottom: '1rem',
          }}>
            {isCrossTab && (
              <button
                style={{
                  padding: '0.75rem 1.25rem',
                  border: 'none',
                  borderBottom: '3px solid var(--primary-color)',
                  background: 'none',
                  cursor: 'default',
                  fontWeight: 700,
                  color: 'var(--primary-color)',
                  fontSize: '0.95rem',
                  marginBottom: '-2px',
                }}
              >
                All Tabs
                <span style={{
                  marginLeft: '0.5rem',
                  background: 'var(--primary-color)',
                  color: '#fff',
                  borderRadius: '10px',
                  padding: '2px 8px',
                  fontSize: '0.8rem',
                  fontWeight: 600,
                }}>
                  {tabItems.length}
                </span>
              </button>
            )}
            {TABS.map(tab => {
              const rawCount = getTabItems(tab).length
              const filteredCount = isCrossTab ? tabFilteredCounts[tab.key] : rawCount
              const isActive = !isCrossTab && activeTab === tab.key
              return (
                <button
                  key={tab.key}
                  onClick={() => {
                    if (isCrossTab) {
                      // Clear filters and switch to this tab
                      setMarkerFilter(''); setWeeklyFilter(''); setPresetFilter('')
                    }
                    setActiveTab(tab.key)
                  }}
                  style={{
                    padding: '0.75rem 1.25rem',
                    border: 'none',
                    borderBottom: isActive ? `3px solid ${tab.color}` : '3px solid transparent',
                    background: 'none',
                    cursor: 'pointer',
                    fontWeight: isActive ? 700 : 400,
                    color: isActive ? tab.color : 'var(--text-secondary)',
                    fontSize: isCrossTab ? '0.85rem' : '0.95rem',
                    transition: 'all 0.2s',
                    marginBottom: '-2px',
                    opacity: isCrossTab && filteredCount === 0 ? 0.4 : 1,
                  }}
                  title={isCrossTab ? `${filteredCount} matching in ${tab.label} (click to view this tab only)` : undefined}
                >
                  {tab.label}
                  <span style={{
                    marginLeft: '0.5rem',
                    background: isActive ? tab.color : (isCrossTab && filteredCount > 0) ? tab.color : 'var(--border)',
                    color: isActive || (isCrossTab && filteredCount > 0) ? '#fff' : 'var(--text-secondary)',
                    borderRadius: '10px',
                    padding: '2px 8px',
                    fontSize: '0.8rem',
                    fontWeight: 600,
                  }}>
                    {filteredCount}
                  </span>
                </button>
              )
            })}
          </div>

          {/* Filters */}
          <div className="filter-bar" style={{ flexWrap: 'wrap' }}>
            <input
              type="text"
              placeholder="Search ticker..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{ minWidth: '140px' }}
            />
            <select
              value={markerFilter}
              onChange={(e) => setMarkerFilter(e.target.value)}
              style={{ minWidth: '130px' }}
            >
              <option value="">All Markers</option>
              {allMarkers.map(m => <option key={m} value={m}>{m}</option>)}
            </select>
            <select
              value={weeklyFilter}
              onChange={(e) => setWeeklyFilter(e.target.value)}
              style={{ minWidth: '130px' }}
            >
              <option value="">All Weekly</option>
              <option value="W-Above">W-Above (Weekly Uptrend)</option>
              <option value="W-Below">W-Below (Weekly Downtrend)</option>
              <option value="W-Bullish Cross">W-Bullish Cross</option>
              <option value="W-Bearish Cross">W-Bearish Cross</option>
            </select>
            <select
              value={presetFilter}
              onChange={(e) => setPresetFilter(e.target.value)}
              style={{ minWidth: '150px' }}
            >
              <option value="">All Presets</option>
              <option value="weekly_confirmed">✅ Weekly Confirmed</option>
              <option value="counter_trend">⚠️ Counter-trend</option>
              <option value="fresh_cross">⚡ Fresh Cross (≤3d)</option>
              <option value="weekly_cross">🗓 Weekly Crossover</option>
              <option value="wide_spread">📈 Wide Spread (≥2%)</option>
              <option value="narrow_spread">📉 Narrow Spread ({`<`}1%)</option>
            </select>
            <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
              {data.total_scanned} scanned &middot; {tabItems.length} shown{isCrossTab ? ' (all tabs)' : ''}
            </span>
            {isCrossTab && (
              <button
                className="btn btn-secondary"
                onClick={() => { setMarkerFilter(''); setWeeklyFilter(''); setPresetFilter('') }}
                style={{ padding: '0.25rem 0.6rem', fontSize: '0.78rem' }}
                title="Clear all filters and return to tab view"
              >
                ✕ Clear Filters
              </button>
            )}
          </div>

          {/* Active tab table */}
          <div className="card" style={{ padding: 0, overflowX: 'auto' }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th onClick={() => handleSort('ticker')} style={thStyle('ticker')}>
                    Ticker{sortArrow('ticker')}
                  </th>
                  <th>Signal</th>
                  <th style={{ whiteSpace: 'nowrap' }}>Markers<InfoIcon tooltip={MARKERS_LEGEND} /></th>
                  <th onClick={() => handleSort('last_close')} style={thStyle('last_close', 'right')}>
                    Last Close{sortArrow('last_close')}
                  </th>
                  <th onClick={() => handleSort('short_ma')} style={thStyle('short_ma', 'right')}>
                    Short MA ({shortPeriod}){sortArrow('short_ma')}
                  </th>
                  <th onClick={() => handleSort('long_ma')} style={thStyle('long_ma', 'right')}>
                    Long MA ({longPeriod}){sortArrow('long_ma')}
                  </th>
                  <th onClick={() => handleSort('ma_spread_pct')} style={thStyle('ma_spread_pct', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>Spread %{sortArrow('ma_spread_pct')}<InfoIcon tooltip="Percentage gap between the short and long MAs. Wider spread = stronger trend momentum. Narrowing spread warns the crossover may reverse." /></span>
                  </th>
                  <th onClick={() => handleSort('days_since_cross')} style={thStyle('days_since_cross', 'center')}>
                    Days{sortArrow('days_since_cross')}
                  </th>
                  <th>Cross Date</th>
                  <th onClick={() => handleSort('price_change_since_cross_pct')} style={thStyle('price_change_since_cross_pct', 'right')}>
                    Since Cross %{sortArrow('price_change_since_cross_pct')}
                  </th>
                  <th onClick={() => handleSort('weekly_short_ma')} style={{ ...thStyle('weekly_short_ma', 'right'), borderLeft: '2px solid var(--border)' }}>
                    W-Short ({shortPeriod}){sortArrow('weekly_short_ma')}
                  </th>
                  <th onClick={() => handleSort('weekly_long_ma')} style={thStyle('weekly_long_ma', 'right')}>
                    W-Long ({longPeriod}){sortArrow('weekly_long_ma')}
                  </th>
                  <th onClick={() => handleSort('weekly_spread_pct')} style={thStyle('weekly_spread_pct', 'right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>W-Spread %{sortArrow('weekly_spread_pct')}<InfoIcon tooltip="Weekly MA spread: percentage gap between the weekly short and long SMAs. Computed from true weekly closes (last trading day of each week). Confirms whether the daily signal aligns with the broader weekly trend." /></span>
                  </th>
                  <th style={{ whiteSpace: 'nowrap' }}>W-Signal<InfoIcon tooltip="Weekly crossover signal using true weekly closes.\nW-Bullish Cross: Weekly short SMA just crossed above weekly long SMA.\nW-Bearish Cross: Weekly short SMA just crossed below weekly long SMA.\nW-Above: Weekly short SMA is above weekly long SMA (weekly uptrend).\nW-Below: Weekly short SMA is below weekly long SMA (weekly downtrend)." /></th>
                </tr>
              </thead>
              <tbody>
                {tabItems.length === 0 ? (
                  <tr>
                    <td colSpan={14} style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-secondary)' }}>
                      {isCrossTab
                        ? 'No results match the current filters across any tab'
                        : `No ${activeTabDef.label.toLowerCase()} signals found`}
                    </td>
                  </tr>
                ) : (
                  tabItems.map((r, idx) => (
                    <tr key={idx}>
                      <td>
                        <span className="ticker" onClick={() => navigate(`/ticker/${r.ticker}`)}>
                          {r.ticker}
                        </span>
                      </td>
                      <td>
                        {(() => {
                          const isRecent = r.signal.startsWith('Recent')
                          const isBullish = r.signal.includes('Bullish')
                          const color = isBullish ? 'var(--success)' : 'var(--danger)'
                          return (
                            <span style={{
                              fontSize: '0.78rem',
                              fontWeight: 600,
                              padding: '3px 8px',
                              borderRadius: '4px',
                              background: isRecent ? 'transparent' : color,
                              color: isRecent ? color : '#fff',
                              border: isRecent ? `1.5px solid ${color}` : 'none',
                              whiteSpace: 'nowrap',
                            }}>
                              {r.signal}{isRecent && r.days_since_cross != null ? ` (${r.days_since_cross}d)` : ''}
                            </span>
                          )
                        })()}
                      </td>
                      <td>
                        <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
                          {r.markers?.map((m, i) => {
                            const mc = MARKER_COLORS[m] || { bg: '#eee', text: '#333' }
                            return (
                              <span key={i} title={MARKER_DESCRIPTIONS[m] || m} style={{
                                fontSize: '0.72rem',
                                fontWeight: 600,
                                padding: '2px 6px',
                                borderRadius: '3px',
                                background: mc.bg,
                                color: mc.text,
                                whiteSpace: 'nowrap',
                                cursor: 'help',
                              }}>
                                {m}
                              </span>
                            )
                          })}
                        </div>
                      </td>
                      <td style={{ textAlign: 'right' }}>${r.last_close.toFixed(2)}</td>
                      <td style={{ textAlign: 'right' }}>${r.short_ma.toFixed(2)}</td>
                      <td style={{ textAlign: 'right' }}>${r.long_ma.toFixed(2)}</td>
                      <td style={{ textAlign: 'right', color: spreadColor(r.ma_spread_pct), fontWeight: 600 }}>
                        {r.ma_spread_pct > 0 ? '+' : ''}{r.ma_spread_pct.toFixed(2)}%
                      </td>
                      <td style={{ textAlign: 'center' }}>
                        {r.days_since_cross != null ? r.days_since_cross : '—'}
                      </td>
                      <td>{r.crossover_date ?? '—'}</td>
                      <td style={{
                        textAlign: 'right',
                        color: r.price_change_since_cross_pct != null
                          ? spreadColor(r.price_change_since_cross_pct)
                          : 'inherit',
                        fontWeight: 600,
                      }}>
                        {r.price_change_since_cross_pct != null
                          ? `${r.price_change_since_cross_pct > 0 ? '+' : ''}${r.price_change_since_cross_pct.toFixed(2)}%`
                          : '—'}
                      </td>
                      <td style={{ textAlign: 'right', borderLeft: '2px solid var(--border)' }}>
                        {r.weekly_short_ma != null ? `$${r.weekly_short_ma.toFixed(2)}` : '—'}
                      </td>
                      <td style={{ textAlign: 'right' }}>
                        {r.weekly_long_ma != null ? `$${r.weekly_long_ma.toFixed(2)}` : '—'}
                      </td>
                      <td style={{
                        textAlign: 'right',
                        color: r.weekly_spread_pct != null ? spreadColor(r.weekly_spread_pct) : 'inherit',
                        fontWeight: 600,
                      }}>
                        {r.weekly_spread_pct != null
                          ? `${r.weekly_spread_pct > 0 ? '+' : ''}${r.weekly_spread_pct.toFixed(2)}%`
                          : '—'}
                      </td>
                      <td>
                        {r.weekly_signal ? (() => {
                          const isBull = r.weekly_signal.includes('Bullish') || r.weekly_signal === 'W-Above'
                          const isCross = r.weekly_signal.includes('Cross')
                          const color = isBull ? 'var(--success)' : 'var(--danger)'
                          return (
                            <span style={{
                              fontSize: '0.75rem',
                              fontWeight: 600,
                              padding: '2px 7px',
                              borderRadius: '4px',
                              background: isCross ? color : 'transparent',
                              color: isCross ? '#fff' : color,
                              border: isCross ? 'none' : `1.5px solid ${color}`,
                              whiteSpace: 'nowrap',
                            }}>
                              {r.weekly_signal}
                            </span>
                          )
                        })() : '—'}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

export default MAScreener
