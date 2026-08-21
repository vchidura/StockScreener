import { useState, useEffect, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { scanGaps, scanFVG, GapScanResponse, GapResult, FVGScanResponse, FVGResult, getLatestPriceDate } from '../services/api'
import StreakPanel from '../components/StreakPanel'

type TabKey = 'support' | 'resistance' | 'inside-gap' | 'fvg'

const TABS: { key: TabKey; label: string; types: string[]; color: string }[] = [
  {
    key: 'support',
    label: 'Support Zones',
    types: ['At Support (Unfilled Gap Up)', 'At Support (Filled Gap Up)'],
    color: 'var(--success)',
  },
  {
    key: 'resistance',
    label: 'Resistance Zones',
    types: ['At Resistance (Unfilled Gap Down)', 'At Resistance (Filled Gap Down)'],
    color: 'var(--danger)',
  },
  {
    key: 'inside-gap',
    label: 'Inside Gap',
    types: ['Possible Upside (In Gap Down)', 'Possible Downside (In Gap Up)'],
    color: '#2196f3',
  },
  {
    key: 'fvg',
    label: 'Fair Value Gaps',
    types: ['Bullish FVG', 'Bearish FVG'],
    color: '#9c27b0',
  },
]

const STRATEGY_TOOLTIP =
  'Gaps occur when a stock opens significantly above its previous high (gap up) or below its previous low (gap down). ' +
  'Unfilled gaps often act as strong support or resistance levels.\n\n' +
  'Support Zones: Price is near or at an unfilled/filled gap-up level — potential buying opportunity.\n' +
  'Resistance Zones: Price is near or at an unfilled/filled gap-down level — potential selling pressure.\n' +
  'Inside Gap: Price is currently sitting inside an unfilled gap zone — breakout or reversal likely.'

const TYPE_TOOLTIPS: Record<string, string> = {
  'At Support (Unfilled Gap Up)': 'Pristine gap-up zone — never breached since formation. Price is near this level, acting as strong support. Higher probability bounce.',
  'At Support (Filled Gap Up)': 'Gap-up zone that was previously breached but price has returned to retest it. Weaker support — watch for confirmation before entry.',
  'At Resistance (Unfilled Gap Down)': 'Pristine gap-down zone — never breached since formation. Price is near this ceiling, acting as strong resistance. High probability rejection.',
  'At Resistance (Filled Gap Down)': 'Gap-down zone that was previously breached but price has returned to retest it. Weaker resistance — watch for rejection confirmation.',
  'Possible Upside (In Gap Down)': 'Price is sitting inside an unfilled gap-down zone. If it fills upward through the gap, expect a breakout rally toward the upper gap edge.',
  'Possible Downside (In Gap Up)': 'Price is sitting inside an unfilled gap-up zone. If it breaks down through the gap, expect a drop toward the lower gap edge.',
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

interface TickerGroup {
  ticker: string
  best: GapResult
  all: GapResult[]
}

type SortKey = 'ticker' | 'gap_date' | 'gap_diff' | 'gap_pct' | 'last_close' | 'trend'
type FvgSortKey = 'ticker' | 'fvg_pct' | 'fvg_size' | 'last_close' | 'streak_count' | 'gap_date' | 'proximity' | 'trend'
type SortDir = 'asc' | 'desc'

function GapScreener() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [activeTab, setActiveTab] = useState<TabKey>('support')
  const [sortKey, setSortKey] = useState<SortKey>('gap_pct')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [expandedTickers, setExpandedTickers] = useState<Set<string>>(new Set())
  const [scanDate, setScanDate] = useState('')
  const [latestDate, setLatestDate] = useState('')
  const [interval, setInterval] = useState<'5m' | '15m' | '30m' | '1h' | '1d'>('1d')
  const [fvgFilter, setFvgFilter] = useState<'all' | 'bullish' | 'bearish'>('all')
  const [fvgStatusFilter, setFvgStatusFilter] = useState<'all' | 'unmitigated' | 'partial' | 'mitigated'>('all')
  const [fvgPreset, setFvgPreset] = useState<'none' | 'high-bull' | 'high-bear' | 'streak'>('none')
  const [fvgSortKey, setFvgSortKey] = useState<FvgSortKey>('streak_count')
  const [fvgSortDir, setFvgSortDir] = useState<SortDir>('desc')

  const { data, error: queryError, isFetching: loading } = useQuery<GapScanResponse>({
    queryKey: ['scan', 'gaps', interval, scanDate],
    queryFn: () => scanGaps(undefined, scanDate || undefined, interval),
    placeholderData: keepPreviousData,
  })
  const { data: fvgData } = useQuery<FVGScanResponse>({
    queryKey: ['scan', 'fvg', interval, scanDate],
    queryFn: () => scanFVG(undefined, scanDate || undefined, interval),
    placeholderData: keepPreviousData,
  })
  const error = queryError ? (queryError as any)?.response?.data?.detail || (queryError as Error).message : null

  const handleRefresh = useCallback(async () => {
    // Force refresh: bypass backend cache, then refetch
    queryClient.setQueryData(['scan', 'gaps', interval, scanDate], undefined)
    queryClient.setQueryData(['scan', 'fvg', interval, scanDate], undefined)
    await Promise.all([
      queryClient.fetchQuery({ queryKey: ['scan', 'gaps', interval, scanDate], queryFn: () => scanGaps(undefined, scanDate || undefined, interval, true) }),
      queryClient.fetchQuery({ queryKey: ['scan', 'fvg', interval, scanDate], queryFn: () => scanFVG(undefined, scanDate || undefined, interval, 50, true) }),
    ])
  }, [interval, scanDate, queryClient])

  useEffect(() => { getLatestPriceDate().then(setLatestDate).catch(() => {}) }, [])
  useEffect(() => { setExpandedTickers(new Set()) }, [activeTab])

  // Cross-tab ticker search: auto-switch to tab containing searched ticker
  useEffect(() => {
    if (!filter || !data?.results) return
    const lc = filter.toLowerCase()
    // Check if current tab has any matches
    const currentGroups = allTickerAssignments[activeTab] ?? []
    if (currentGroups.some(g => g.ticker.toLowerCase().includes(lc))) return
    // Search other tabs
    for (const tab of TABS) {
      if (tab.key === activeTab) continue
      const groups = allTickerAssignments[tab.key] ?? []
      if (groups.some(g => g.ticker.toLowerCase().includes(lc))) {
        setActiveTab(tab.key)
        return
      }
    }
  }, [filter])

  /** Determine which tab a gap_type belongs to */
  const tabForType = (gapType: string): TabKey => {
    for (const tab of TABS) {
      if (tab.types.includes(gapType)) return tab.key
    }
    return 'support'
  }

  /** Assign each ticker to exactly one tab based on its most recent gap,
      and gather ALL gaps for that ticker into the expandable list. */
  const allTickerAssignments = useMemo(() => {
    if (!data?.results) return { support: [] as TickerGroup[], resistance: [] as TickerGroup[], 'inside-gap': [] as TickerGroup[], fvg: [] as TickerGroup[] }

    // Gather all gaps per ticker
    const tickerGaps = new Map<string, GapResult[]>()
    for (const r of data.results) {
      if (!tickerGaps.has(r.ticker)) tickerGaps.set(r.ticker, [])
      tickerGaps.get(r.ticker)!.push(r)
    }

    const assigned: Record<TabKey, TickerGroup[]> = { support: [], resistance: [], 'inside-gap': [], fvg: [] }

    for (const [ticker, gaps] of tickerGaps) {
      // Most recent gap determines which tab the ticker belongs to
      const sorted = [...gaps].sort((a, b) => b.gap_date.localeCompare(a.gap_date))
      const mostRecent = sorted[0]
      const assignedTab = tabForType(mostRecent.gap_type)

      assigned[assignedTab].push({ ticker, best: mostRecent, all: sorted })
    }

    return assigned
  }, [data])

  const getTabTickerCount = (tab: typeof TABS[number]): number => {
    if (tab.key === 'fvg') return fvgTickerGroups.length
    return allTickerAssignments[tab.key].length
  }

  // FVG ticker grouping
  const fvgTickerGroups = useMemo((): { ticker: string; best: FVGResult; all: FVGResult[] }[] => {
    if (!fvgData?.results) return []
    let filtered = fvgData.results

    // Presets override manual filters
    if (fvgPreset === 'high-bull') {
      filtered = filtered.filter(r =>
        r.fvg_type === 'Bullish FVG' && r.status === 'Unmitigated' && r.trend_aligned && r.streak_count >= 2
      )
    } else if (fvgPreset === 'high-bear') {
      filtered = filtered.filter(r =>
        r.fvg_type === 'Bearish FVG' && r.status === 'Unmitigated' && r.trend_aligned && r.streak_count >= 2
      )
    } else if (fvgPreset === 'streak') {
      filtered = filtered.filter(r => r.streak_count >= 3)
    } else {
      // Manual filters
      if (fvgFilter !== 'all') {
        filtered = filtered.filter(r =>
          fvgFilter === 'bullish' ? r.fvg_type === 'Bullish FVG' : r.fvg_type === 'Bearish FVG'
        )
      }
      if (fvgStatusFilter !== 'all') {
        filtered = filtered.filter(r => {
          if (fvgStatusFilter === 'unmitigated') return r.status === 'Unmitigated'
          if (fvgStatusFilter === 'partial') return r.status === 'Partially Mitigated'
          return r.status === 'Mitigated'
        })
      }
    }

    const tickerMap = new Map<string, FVGResult[]>()
    for (const r of filtered) {
      if (!tickerMap.has(r.ticker)) tickerMap.set(r.ticker, [])
      tickerMap.get(r.ticker)!.push(r)
    }
    const groups: { ticker: string; best: FVGResult; all: FVGResult[] }[] = []
    for (const [ticker, fvgs] of tickerMap) {
      const sorted = [...fvgs].sort((a, b) => b.gap_date.localeCompare(a.gap_date))
      groups.push({ ticker, best: sorted[0], all: sorted })
    }
    // Apply sort
    groups.sort((a, b) => {
      const av = a.best[fvgSortKey] ?? ''
      const bv = b.best[fvgSortKey] ?? ''
      if (typeof av === 'string' && typeof bv === 'string') return fvgSortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return fvgSortDir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number)
    })
    if (filter) return groups.filter(g => g.ticker.toLowerCase().includes(filter.toLowerCase()))
    return groups
  }, [fvgData, fvgFilter, fvgStatusFilter, fvgPreset, fvgSortKey, fvgSortDir, filter])

  // Filter and sort the groups for the active tab
  const tickerGroups = useMemo((): TickerGroup[] => {
    let groups = allTickerAssignments[activeTab] ?? []
    if (filter) groups = groups.filter(g => g.ticker.toLowerCase().includes(filter.toLowerCase()))

    const sorted = [...groups]
    sorted.sort((a, b) => {
      const av = a.best[sortKey] ?? ''
      const bv = b.best[sortKey] ?? ''
      if (typeof av === 'string' && typeof bv === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number)
    })
    return sorted
  }, [allTickerAssignments, activeTab, filter, sortKey, sortDir])

  const toggleExpand = (ticker: string) => {
    setExpandedTickers(prev => {
      const next = new Set(prev)
      if (next.has(ticker)) next.delete(ticker)
      else next.add(ticker)
      return next
    })
  }

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir(key === 'ticker' ? 'asc' : 'desc') }
  }

  const sortArrow = (key: SortKey) => {
    if (sortKey !== key) return ' ↕'
    return sortDir === 'asc' ? ' ↑' : ' ↓'
  }

  const thStyle = (align: 'left' | 'right' | 'center' = 'left'): React.CSSProperties => ({
    textAlign: align, cursor: 'pointer', userSelect: 'none', whiteSpace: 'nowrap',
  })

  const handleFvgSort = (key: FvgSortKey) => {
    if (fvgSortKey === key) setFvgSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setFvgSortKey(key); setFvgSortDir(key === 'ticker' ? 'asc' : 'desc') }
  }

  const fvgSortArrow = (key: FvgSortKey) => {
    if (fvgSortKey !== key) return ' ↕'
    return fvgSortDir === 'asc' ? ' ↑' : ' ↓'
  }

  /** Show gap range directionally: gap up → low to high, gap down → high to low */
  const gapRange = (r: GapResult) => {
    const isDown = r.gap_type.includes('Gap Down')
    return isDown
      ? `$${r.gap_high.toFixed(2)} – $${r.gap_low.toFixed(2)}`
      : `$${r.gap_low.toFixed(2)} – $${r.gap_high.toFixed(2)}`
  }

  const getProximityLabel = (r: GapResult): { label: string; color: string } => {
    const isSupport = r.gap_type.includes('Support')
    const isResistance = r.gap_type.includes('Resistance')
    if (!isSupport && !isResistance) return { label: '', color: '' }
    const isFilled = r.gap_type.includes('Filled') && !r.gap_type.includes('Unfilled')
    // For support: how far above the gap top (gap_high) is the close?
    // For resistance: how far below the gap bottom (gap_low) is the close?
    const edge = isSupport ? r.gap_high : r.gap_low
    const dist = Math.abs(r.last_close - edge) / edge
    // Support broken: price fell below the gap zone floor
    if (isSupport && r.last_close < r.gap_low) return { label: 'Broken', color: '#c62828' }
    if (dist < 0.003) return { label: 'At Edge', color: '#d32f2f' }      // < 0.3%
    if (dist < 0.008) return { label: 'Testing', color: '#e65100' }       // < 0.8%
    if (isFilled && isResistance) return { label: 'Gap Filled', color: '#757575' }
    return { label: 'Approaching', color: '#1565c0' }                     // 0.8% - 2%
  }

  const getSubtypeBadge = (r: GapResult) => {
    const isFilled = r.gap_type.includes('Filled') && !r.gap_type.includes('Unfilled')
    const isInside = r.gap_type.includes('In Gap')
    const isSupport = r.gap_type.includes('Support')

    if (isInside) {
      const isRally = r.entry_direction === 'rally'
      // Compute how far through the gap zone the price is
      const gapSize = Math.abs(r.gap_high - r.gap_low)
      const progress = gapSize > 0 ? ((r.last_close - r.gap_low) / gapSize) * 100 : 50
      const posLabel = progress > 66 ? 'Near Top' : progress < 33 ? 'Near Bottom' : 'Mid-Zone'
      return (
        <span style={{ display: 'inline-flex', gap: '4px', alignItems: 'center' }}>
          <span style={{
            display: 'inline-block', padding: '2px 8px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 600,
            background: isRally ? '#e8f5e9' : '#fff3e0',
            color: isRally ? '#2e7d32' : '#e65100',
          }}>
            {isRally ? '↑ Entry Rally' : '↓ Entry Drop'}
          </span>
          <span style={{
            display: 'inline-block', padding: '1px 6px', borderRadius: '3px', fontSize: '0.7rem', fontWeight: 500,
            background: '#f5f5f5', color: '#616161',
          }}>
            {posLabel}
          </span>
        </span>
      )
    }

    // Strength badge — trend-aware: "Strong" only when trend confirms the zone
    const trendConfirmsSupport = r.trend === 'Bullish' || r.trend === 'Neutral-Bullish'
    const trendConfirmsResistance = r.trend === 'Bearish' || r.trend === 'Neutral-Bearish'
    const strengthLabel = isFilled
      ? 'Retest'
      : isSupport
        ? (trendConfirmsSupport ? 'Strong Support' : 'Support')
        : (trendConfirmsResistance ? 'Strong Resistance' : 'Resistance')
    const strengthBg = isFilled ? '#fff3e0' : (isSupport ? '#e8f5e9' : '#ffebee')
    const strengthColor = isFilled ? '#e65100' : (isSupport ? '#2e7d32' : '#c62828')

    // Proximity badge
    const prox = getProximityLabel(r)

    return (
      <span style={{ display: 'inline-flex', gap: '4px', alignItems: 'center' }}>
        <span style={{
          display: 'inline-block', padding: '2px 8px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 600,
          background: strengthBg, color: strengthColor,
        }}>
          {strengthLabel}
        </span>
        {prox.label && (
          <span style={{
            display: 'inline-block', padding: '1px 6px', borderRadius: '3px', fontSize: '0.7rem', fontWeight: 500,
            background: '#f5f5f5', color: prox.color,
          }}>
            {prox.label}
          </span>
        )}
      </span>
    )
  }

  return (
    <div>
      {/* Header */}
      <div className="card-header" style={{ border: 'none', padding: 0, marginBottom: '1.5rem' }}>
        <div>
          <h1 style={{ fontSize: '1.75rem', fontWeight: 700 }}>
            Gap Strategies
            <InfoIcon tooltip={STRATEGY_TOOLTIP} />
          </h1>
          <p style={{ color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
            Identify unfilled gaps that act as support and resistance levels
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

      <StreakPanel strategy="gaps" />

      {loading && (
        <div className="loading">
          <div className="spinner"></div>
          <span>Scanning for gap strategies...</span>
        </div>
      )}

      {error && (
        <div className="card" style={{ padding: '1rem', background: '#fff3f3', color: '#c00', border: '1px solid #fcc' }}>
          <strong>Error:</strong> {error}
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
            {TABS.map(tab => {
              const count = getTabTickerCount(tab)
              const isActive = activeTab === tab.key
              return (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  style={{
                    padding: '0.75rem 1.25rem',
                    border: 'none',
                    borderBottom: isActive ? `3px solid ${tab.color}` : '3px solid transparent',
                    background: 'none',
                    cursor: 'pointer',
                    fontWeight: isActive ? 700 : 400,
                    color: isActive ? tab.color : 'var(--text-secondary)',
                    fontSize: '0.95rem',
                    transition: 'all 0.2s',
                    marginBottom: '-2px',
                  }}
                >
                  {tab.label}
                  <span style={{
                    marginLeft: '0.5rem',
                    background: isActive ? tab.color : 'var(--border)',
                    color: isActive ? '#fff' : 'var(--text-secondary)',
                    borderRadius: '10px',
                    padding: '2px 8px',
                    fontSize: '0.8rem',
                    fontWeight: 600,
                  }}>
                    {count}
                  </span>
                </button>
              )
            })}
          </div>

          {/* Filter bar */}
          <div className="filter-bar">
            <input
              type="text"
              placeholder="Search ticker..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{ minWidth: '160px' }}
            />
            {activeTab === 'fvg' && (<>
              <select
                value={fvgFilter}
                onChange={(e) => { setFvgFilter(e.target.value as typeof fvgFilter); setFvgPreset('none') }}
                style={{ minWidth: '130px' }}
              >
                <option value="all">All Directions</option>
                <option value="bullish">▲ Bullish FVG</option>
                <option value="bearish">▼ Bearish FVG</option>
              </select>
              <select
                value={fvgStatusFilter}
                onChange={(e) => { setFvgStatusFilter(e.target.value as typeof fvgStatusFilter); setFvgPreset('none') }}
                style={{ minWidth: '130px' }}
              >
                <option value="all">All Status</option>
                <option value="unmitigated">Unmitigated</option>
                <option value="partial">Partially Mitigated</option>
                <option value="mitigated">Mitigated</option>
              </select>
              <select
                value={fvgPreset}
                onChange={(e) => {
                  const v = e.target.value as typeof fvgPreset
                  setFvgPreset(v)
                  if (v !== 'none') { setFvgFilter('all'); setFvgStatusFilter('all') }
                }}
                style={{ minWidth: '150px' }}
              >
                <option value="none">All Presets</option>
                <option value="high-bull">🟢 High-Prob Bullish</option>
                <option value="high-bear">🔴 High-Prob Bearish</option>
                <option value="streak">📊 Streak Signals (≥3)</option>
              </select>
            </>)}
            <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
              {activeTab === 'fvg'
                ? `${fvgData?.total_scanned ?? 0} scanned · ${fvgTickerGroups.length} tickers with FVGs`
                : `${data.total_scanned} scanned · ${tickerGroups.length} tickers with gaps`}
            </span>
          </div>

          {/* Results table — Traditional Gaps */}
          {activeTab !== 'fvg' && (
          <div className="card" style={{ padding: 0, overflowX: 'auto' }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th onClick={() => handleSort('ticker')} style={thStyle()}>
                    Ticker{sortArrow('ticker')}
                  </th>
                  <th style={{ whiteSpace: 'nowrap' }}>
                    <span style={{ whiteSpace: 'nowrap' }}>
                      Status
                      <InfoIcon tooltip={
                        activeTab === 'support'
                          ? 'Strong Support = pristine gap zone never breached (high confidence bounce). Retest = gap was filled before, price revisiting (weaker). Proximity: At Edge (<0.3%), Testing (<0.8%), Approaching (<2%).'
                          : activeTab === 'resistance'
                          ? 'Strong Resistance = pristine gap zone never breached (high confidence rejection). Retest = gap was filled before, price revisiting (weaker). Proximity: At Edge (<0.3%), Testing (<0.8%), Approaching (<2%).'
                          : 'Gap Fill Rally = price inside gap-down zone, may break upward. Gap Fill Drop = price inside gap-up zone, may break down. Position: Near Top / Mid-Zone / Near Bottom within the gap.'
                      } />
                    </span>
                  </th>
                  <th style={{ whiteSpace: 'nowrap', textAlign: 'center' }}>
                    <span style={{ whiteSpace: 'nowrap' }}>
                      Gaps
                      <InfoIcon tooltip="Number of gap zones near current price for this ticker. Click count to expand and see all gaps." />
                    </span>
                  </th>
                  <th onClick={() => handleSort('gap_date')} style={thStyle()}>
                    Gap Date{sortArrow('gap_date')}
                  </th>
                  <th style={thStyle('right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>
                      Gap Range
                      <InfoIcon tooltip="The price range of the gap zone shown directionally. Gap Up (support): low → high. Gap Down (resistance): high → low. The first price is the reference edge nearest to the current price." />
                    </span>
                  </th>
                  <th onClick={() => handleSort('last_close')} style={thStyle('right')}>
                    Close{sortArrow('last_close')}
                  </th>
                  <th style={thStyle('right')}>Open</th>
                  <th style={thStyle('right')}>High</th>
                  <th style={thStyle('right')}>Low</th>
                  <th onClick={() => handleSort('gap_diff')} style={thStyle('right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>
                      Gap Size{sortArrow('gap_diff')}
                      <InfoIcon tooltip="Absolute dollar size of the gap zone (High - Low)." />
                    </span>
                  </th>
                  <th onClick={() => handleSort('gap_pct')} style={thStyle('right')}>
                    <span style={{ whiteSpace: 'nowrap' }}>
                      Gap %{sortArrow('gap_pct')}
                      <InfoIcon tooltip="Gap size as a percentage of the previous day's price. Larger gaps are more significant." />
                    </span>
                  </th>
                  <th onClick={() => handleSort('trend')} style={thStyle('center')}>
                    <span style={{ whiteSpace: 'nowrap' }}>
                      Trend{sortArrow('trend')}
                      <InfoIcon tooltip="Bigger-picture trend based on price vs 50-day and 200-day moving averages.\nBullish: Price > 50MA > 200MA.\nBearish: Price < 50MA < 200MA.\nNeutral-Bullish: Price > 50MA but 50MA < 200MA.\nNeutral-Bearish: Price < 50MA but 50MA > 200MA." />
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {tickerGroups.length === 0 ? (
                  <tr>
                    <td colSpan={11} style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-secondary)' }}>
                      No gap signals found in this category
                    </td>
                  </tr>
                ) : (
                  tickerGroups.flatMap(g => {
                    const isExpanded = expandedTickers.has(g.ticker)
                    const hasMore = g.all.length > 1
                    const rows: React.ReactNode[] = []

                    // Primary row (nearest gap)
                    rows.push(
                      <tr key={g.ticker}>
                        <td>
                          <span className="ticker" onClick={() => navigate(`/ticker/${g.ticker}`)}>
                            {g.ticker}
                          </span>
                        </td>
                        <td>
                          <span title={TYPE_TOOLTIPS[g.best.gap_type] ?? g.best.gap_type}>
                            {getSubtypeBadge(g.best)}
                          </span>
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          {hasMore ? (
                            <span
                              onClick={() => toggleExpand(g.ticker)}
                              style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: '4px',
                                cursor: 'pointer',
                                padding: '2px 10px',
                                borderRadius: '12px',
                                background: isExpanded ? 'var(--primary-color)' : 'var(--border)',
                                color: isExpanded ? '#fff' : 'var(--text-primary)',
                                fontSize: '0.8rem',
                                fontWeight: 600,
                                transition: 'all 0.15s',
                              }}
                              title={`${g.all.length} gap zones near current price — click to ${isExpanded ? 'collapse' : 'expand'}`}
                            >
                              {g.all.length} {isExpanded ? '▾' : '▸'}
                            </span>
                          ) : (
                            <span style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>1</span>
                          )}
                        </td>
                        <td>{g.best.gap_date}</td>
                        <td style={{ textAlign: 'right' }}>{gapRange(g.best)}</td>
                        <td style={{ textAlign: 'right' }}>${g.best.last_close.toFixed(2)}</td>
                        <td style={{ textAlign: 'right' }}>${g.best.current_open?.toFixed(2) ?? '—'}</td>
                        <td style={{ textAlign: 'right' }}>${g.best.current_high?.toFixed(2) ?? '—'}</td>
                        <td style={{ textAlign: 'right' }}>${g.best.current_low?.toFixed(2) ?? '—'}</td>
                        <td style={{ textAlign: 'right' }}>${g.best.gap_diff.toFixed(2)}</td>
                        <td style={{
                          textAlign: 'right',
                          color: g.best.gap_pct >= 3 ? 'var(--danger)' : g.best.gap_pct >= 2 ? '#ff9800' : 'inherit',
                          fontWeight: g.best.gap_pct >= 2 ? 600 : 400,
                        }}>
                          {g.best.gap_pct.toFixed(2)}%
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          {g.best.trend && g.best.trend !== 'N/A' ? (
                            <span style={{
                              display: 'inline-block',
                              padding: '2px 8px',
                              borderRadius: '4px',
                              fontSize: '0.75rem',
                              fontWeight: 600,
                              background: g.best.trend === 'Bullish' ? '#e8f5e9'
                                : g.best.trend === 'Bearish' ? '#fce4ec'
                                : g.best.trend === 'Neutral-Bullish' ? '#e3f2fd'
                                : '#fff3e0',
                              color: g.best.trend === 'Bullish' ? '#2e7d32'
                                : g.best.trend === 'Bearish' ? '#c62828'
                                : g.best.trend === 'Neutral-Bullish' ? '#1565c0'
                                : '#e65100',
                            }}>
                              {g.best.trend === 'Bullish' ? '▲ Bullish'
                                : g.best.trend === 'Bearish' ? '▼ Bearish'
                                : g.best.trend === 'Neutral-Bullish' ? '↗ N-Bull'
                                : '↘ N-Bear'}
                            </span>
                          ) : (
                            <span style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>—</span>
                          )}
                        </td>
                      </tr>
                    )

                    // Expanded child rows (excluding the best)
                    if (isExpanded) {
                      const others = g.all.filter(r => r !== g.best)
                      others.forEach((r, idx) => {
                        rows.push(
                          <tr key={`${g.ticker}-exp-${idx}`} style={{ background: 'var(--bg-secondary)' }}>
                            <td style={{ paddingLeft: '2rem' }}>
                              <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>↳</span>
                            </td>
                            <td>
                              <span title={TYPE_TOOLTIPS[r.gap_type] ?? r.gap_type}>
                                {getSubtypeBadge(r)}
                              </span>
                            </td>
                            <td></td>
                            <td>{r.gap_date}</td>
                            <td style={{ textAlign: 'right' }}>{gapRange(r)}</td>
                            <td style={{ textAlign: 'right' }}>${r.last_close.toFixed(2)}</td>
                            <td style={{ textAlign: 'right', color: 'var(--text-secondary)' }}>—</td>
                            <td style={{ textAlign: 'right', color: 'var(--text-secondary)' }}>—</td>
                            <td style={{ textAlign: 'right', color: 'var(--text-secondary)' }}>—</td>
                            <td style={{ textAlign: 'right' }}>${r.gap_diff.toFixed(2)}</td>
                            <td style={{
                              textAlign: 'right',
                              color: r.gap_pct >= 3 ? 'var(--danger)' : r.gap_pct >= 2 ? '#ff9800' : 'inherit',
                              fontWeight: r.gap_pct >= 2 ? 600 : 400,
                            }}>
                              {r.gap_pct.toFixed(2)}%
                            </td>
                            <td></td>
                          </tr>
                        )
                      })
                    }
                    return rows
                  })
                )}
              </tbody>
            </table>
          </div>
          )}

          {/* Results table — Fair Value Gaps */}
          {activeTab === 'fvg' && (<>
          <div style={{
            display: 'flex', gap: '1rem', padding: '0.6rem 0.8rem', fontSize: '0.78rem',
            background: 'var(--bg-secondary)', borderRadius: '6px', marginBottom: '0.5rem',
            color: 'var(--text-secondary)', lineHeight: 1.5, flexWrap: 'wrap',
          }}>
            <span><strong>Pro tips:</strong></span>
            <span>🟢 <strong>Buy the dip</strong> — Bullish + Unmitigated + Trend-aligned + Streak ≥2. Enter at FVG zone with stop below.</span>
            <span>🔴 <strong>Short the rip</strong> — Bearish + Unmitigated + Trend-aligned + Streak ≥2. Enter at FVG zone with stop above.</span>
            <span>📊 <strong>Structure bias</strong> — Streak ≥3 = strong directional flow. Higher ATR ratio = more institutional significance.</span>
            <span>🎯 <strong>Best on intraday</strong> — FVGs work best on 5m/15m for precise entries within daily-timeframe context.</span>
            <span>🔎 <strong>Multi-timeframe</strong> — Find an unmitigated daily bullish FVG in an uptrend, then drop to 5m/15m to time your entry as price enters the zone.</span>
          </div>
          <div className="card" style={{ padding: 0, overflowX: 'auto' }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th onClick={() => handleFvgSort('ticker')} style={thStyle()}>Ticker{fvgSortArrow('ticker')}</th>
                  <th style={{ whiteSpace: 'nowrap' }}>
                    Type / Status
                    <InfoIcon tooltip="Bullish FVG = demand zone (price may retrace down to fill). Bearish FVG = supply zone (price may retrace up to fill). Status: Unmitigated (never revisited), Partially Mitigated (touched edge), Mitigated (filled through midpoint)." />
                  </th>
                  <th onClick={() => handleFvgSort('streak_count')} style={thStyle('center')}>
                    Streak{fvgSortArrow('streak_count')}
                    <InfoIcon tooltip="Consecutive same-direction FVGs from most recent. High streak = strong institutional flow. Shows direction and count." />
                  </th>
                  <th style={{ whiteSpace: 'nowrap', textAlign: 'center' }}>
                    FVGs
                    <InfoIcon tooltip="Number of FVGs for this ticker. Click to expand all." />
                  </th>
                  <th onClick={() => handleFvgSort('gap_date')} style={thStyle()}>Date{fvgSortArrow('gap_date')}</th>
                  <th style={thStyle('right')}>FVG Zone</th>
                  <th onClick={() => handleFvgSort('last_close')} style={thStyle('right')}>Close{fvgSortArrow('last_close')}</th>
                  <th onClick={() => handleFvgSort('fvg_size')} style={thStyle('right')}>Size ${fvgSortArrow('fvg_size')}</th>
                  <th onClick={() => handleFvgSort('fvg_pct')} style={thStyle('right')}>
                    Size %{fvgSortArrow('fvg_pct')}
                    <InfoIcon tooltip="FVG size as percentage of the zone's low price." />
                  </th>
                  <th onClick={() => handleFvgSort('proximity')} style={thStyle('center')}>
                    Proximity{fvgSortArrow('proximity')}
                    <InfoIcon tooltip="Inside = price is inside the FVG zone. Near = within 2% of zone edge. Away = more than 2% from zone." />
                  </th>
                  <th onClick={() => handleFvgSort('trend')} style={thStyle('center')}>Trend{fvgSortArrow('trend')}</th>
                </tr>
              </thead>
              <tbody>
                {fvgTickerGroups.length === 0 ? (
                  <tr>
                    <td colSpan={11} style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-secondary)' }}>
                      No Fair Value Gaps found
                    </td>
                  </tr>
                ) : (
                  fvgTickerGroups.flatMap(g => {
                    const isExpanded = expandedTickers.has(g.ticker)
                    const hasMore = g.all.length > 1
                    const rows: React.ReactNode[] = []

                    const renderFvgRow = (r: FVGResult, key: string, isChild = false) => {
                      const isBullish = r.fvg_type === 'Bullish FVG'
                      return (
                        <tr key={key} style={isChild ? { background: 'var(--bg-secondary)' } : undefined}>
                          <td style={isChild ? { paddingLeft: '2rem' } : undefined}>
                            {isChild ? (
                              <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>↳</span>
                            ) : (
                              <span className="ticker" onClick={() => navigate(`/ticker/${g.ticker}`)}>{r.ticker}</span>
                            )}
                          </td>
                          <td>
                            <span style={{ display: 'inline-flex', gap: '4px', alignItems: 'center' }}>
                              <span style={{
                                padding: '2px 8px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 600,
                                background: isBullish ? '#e8f5e9' : '#ffebee',
                                color: isBullish ? '#2e7d32' : '#c62828',
                              }}>
                                {isBullish ? '▲ Bullish' : '▼ Bearish'}
                              </span>
                              <span style={{
                                padding: '1px 6px', borderRadius: '3px', fontSize: '0.7rem', fontWeight: 500,
                                background: r.status === 'Unmitigated' ? '#e3f2fd' : r.status === 'Partially Mitigated' ? '#fff3e0' : '#f5f5f5',
                                color: r.status === 'Unmitigated' ? '#1565c0' : r.status === 'Partially Mitigated' ? '#e65100' : '#757575',
                              }}>
                                {r.status}
                              </span>
                              {r.trend_aligned && (
                                <span style={{ padding: '1px 5px', borderRadius: '3px', fontSize: '0.65rem', fontWeight: 600, background: '#f3e5f5', color: '#7b1fa2' }}>
                                  Aligned
                                </span>
                              )}
                            </span>
                          </td>
                          <td style={{ textAlign: 'center' }}>
                            {!isChild && r.streak_count >= 2 && (
                              <span style={{
                                padding: '2px 8px', borderRadius: '10px', fontSize: '0.75rem', fontWeight: 600,
                                background: r.streak_direction === 'Bullish' ? '#e8f5e9' : '#ffebee',
                                color: r.streak_direction === 'Bullish' ? '#2e7d32' : '#c62828',
                              }}>
                                {r.streak_direction === 'Bullish' ? '▲' : '▼'} {r.streak_count}x
                              </span>
                            )}
                          </td>
                          <td style={{ textAlign: 'center' }}>
                            {!isChild && hasMore ? (
                              <span
                                onClick={() => toggleExpand(g.ticker)}
                                style={{
                                  display: 'inline-flex', alignItems: 'center', gap: '4px', cursor: 'pointer',
                                  padding: '2px 10px', borderRadius: '12px',
                                  background: isExpanded ? '#9c27b0' : 'var(--border)',
                                  color: isExpanded ? '#fff' : 'var(--text-primary)',
                                  fontSize: '0.8rem', fontWeight: 600,
                                }}
                                title={`${g.all.length} FVGs — click to ${isExpanded ? 'collapse' : 'expand'}`}
                              >
                                {g.all.length} {isExpanded ? '▾' : '▸'}
                              </span>
                            ) : !isChild ? (
                              <span style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>1</span>
                            ) : null}
                          </td>
                          <td>{r.gap_date}</td>
                          <td style={{ textAlign: 'right' }}>${r.fvg_low.toFixed(2)} – ${r.fvg_high.toFixed(2)}</td>
                          <td style={{ textAlign: 'right' }}>${r.last_close.toFixed(2)}</td>
                          <td style={{ textAlign: 'right' }}>${r.fvg_size.toFixed(2)}</td>
                          <td style={{
                            textAlign: 'right',
                            color: r.fvg_pct >= 2 ? 'var(--danger)' : r.fvg_pct >= 1 ? '#ff9800' : 'inherit',
                            fontWeight: r.fvg_pct >= 1 ? 600 : 400,
                          }}>
                            {r.fvg_pct.toFixed(2)}%
                          </td>
                          <td style={{ textAlign: 'center' }}>
                            <span style={{
                              padding: '2px 8px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 600,
                              background: r.proximity === 'Inside' ? '#f3e5f5' : r.proximity === 'Near' ? '#fff3e0' : '#f5f5f5',
                              color: r.proximity === 'Inside' ? '#7b1fa2' : r.proximity === 'Near' ? '#e65100' : '#757575',
                            }}>
                              {r.proximity}
                            </span>
                          </td>
                          <td style={{ textAlign: 'center' }}>
                            {r.trend && r.trend !== 'N/A' ? (
                              <span style={{
                                display: 'inline-block', padding: '2px 8px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 600,
                                background: r.trend === 'Bullish' ? '#e8f5e9' : r.trend === 'Bearish' ? '#fce4ec' : r.trend === 'Neutral-Bullish' ? '#e3f2fd' : '#fff3e0',
                                color: r.trend === 'Bullish' ? '#2e7d32' : r.trend === 'Bearish' ? '#c62828' : r.trend === 'Neutral-Bullish' ? '#1565c0' : '#e65100',
                              }}>
                                {r.trend === 'Bullish' ? '▲ Bullish' : r.trend === 'Bearish' ? '▼ Bearish' : r.trend === 'Neutral-Bullish' ? '↗ N-Bull' : '↘ N-Bear'}
                              </span>
                            ) : (
                              <span style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>—</span>
                            )}
                          </td>
                        </tr>
                      )
                    }

                    rows.push(renderFvgRow(g.best, g.ticker))
                    if (isExpanded) {
                      g.all.filter(r => r !== g.best).forEach((r, idx) => {
                        rows.push(renderFvgRow(r, `${g.ticker}-fvg-${idx}`, true))
                      })
                    }
                    return rows
                  })
                )}
              </tbody>
            </table>
          </div>
          </>)}
        </>
      )}
    </div>
  )
}

export default GapScreener
