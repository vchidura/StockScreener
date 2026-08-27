import { useState, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Settings } from 'lucide-react'
import {
  getTickersOverview, TickerOverviewRow, getStreakSummary, getLatestPriceDate,
  scanAll, MarketRegime,
} from '../services/api'
import StreakPanel from '../components/StreakPanel'

type SortField = keyof TickerOverviewRow | 'streak_badges' | 'action'
type SortDir = 'asc' | 'desc'

function TickersOverview() {
  const queryClient = useQueryClient()
  const [sortField, setSortField] = useState<SortField>('ticker')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [filter, setFilter] = useState('')
  const [sectorFilter, setSectorFilter] = useState('')
  const [maFilterIdx, setMaFilterIdx] = useState(0)
  const [maThreshold, setMaThreshold] = useState(3)
  const [presetFilter, setPresetFilter] = useState('')
  const [presetPct, setPresetPct] = useState(5)
  const [scanDate, setScanDate] = useState('')
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set(['rel_vol', 'high_52w', 'low_52w']))
  const [showColPicker, setShowColPicker] = useState(false)
  const [streakDays, setStreakDays] = useState(3)
  const [fibSwingPct, setFibSwingPct] = useState(5)
  // Cross-strategy Buy/Sell/Hold action
  type ActionEntry = { action: 'STRONG_BUY' | 'BUY' | 'HOLD' | 'SELL' | 'STRONG_SELL'; strategies: string[]; buys: number; sells: number; buyStrats: string[]; sellStrats: string[]; holdStrats: string[] }
  const navigate = useNavigate()

  const { data = [], isFetching: loading, error: queryError } = useQuery<TickerOverviewRow[]>({
    queryKey: ['tickers', 'overview', scanDate],
    queryFn: () => getTickersOverview(scanDate || undefined),
  })
  const error = queryError ? (queryError as Error).message : null

  const { data: latestDate = '' } = useQuery({
    queryKey: ['latest-price-date'],
    queryFn: () => getLatestPriceDate(),
  })

  const handleRefresh = useCallback(async () => {
    const key = ['tickers', 'overview', scanDate]
    queryClient.setQueryData(key, undefined)
    // Also invalidate streak cache on full refresh
    queryClient.invalidateQueries({ queryKey: ['streak-action'] })
    await queryClient.fetchQuery({ queryKey: key, queryFn: () => getTickersOverview(scanDate || undefined, true) })
  }, [scanDate, queryClient])

  // Streak + Action data — cached by React Query, survives navigation
  type StreakActionData = {
    streakMap: Record<string, Record<string, number>>
    actionMap: Record<string, ActionEntry>
    marketRegime: MarketRegime | null
  }
  const streakQueryKey = useMemo(() => ['streak-action', streakDays, fibSwingPct], [streakDays, fibSwingPct])

  const fetchStreakAction = useCallback(async (refresh = false): Promise<StreakActionData> => {
    const [streakRes, combined] = await Promise.all([
      getStreakSummary(streakDays, fibSwingPct, refresh),
      scanAll(undefined, fibSwingPct, refresh),
    ])
    const gapData = combined.gaps
    const maData = combined.ma_crossover
    const momentumData = combined.momentum_pullback
    const bearishData = combined.bearish_bounce
    const fibData = combined.fibonacci

    const GRADE_WEIGHT: Record<string, number> = { 'A+': 1.5, 'A': 1.2, 'B+': 1.0, 'B': 0.8 }
    type DirEntry = { strategy: string; direction: 'buy' | 'sell' | 'hold'; weight: number }
    const map: Record<string, DirEntry[]> = {}
    const add = (ticker: string, strategy: string, direction: 'buy' | 'sell' | 'hold', weight = 1.0) => {
      if (!map[ticker]) map[ticker] = []
      if (!map[ticker].find(e => e.strategy === strategy)) map[ticker].push({ strategy, direction, weight })
    }
    gapData.results.forEach(r => {
      let dir: 'buy' | 'sell' | 'hold' = 'hold'
      const gt = r.gap_type?.toLowerCase() || ''
      if (gt.includes('support') || gt.includes('upside')) dir = 'buy'
      else if (gt.includes('resistance') || gt.includes('downside')) dir = 'sell'
      add(r.ticker, 'Gap', dir)
    })
    maData.results.forEach(r => {
      let dir: 'buy' | 'sell' | 'hold' = 'hold'
      const sig = r.signal?.toLowerCase() || ''
      if (sig.includes('bullish') || sig === 'above ma') dir = 'buy'
      else if (sig.includes('bearish') || sig === 'below ma') dir = 'sell'
      let w = sig.includes('crossover') ? 1.2 : 1.0
      const ws = r.weekly_signal?.toLowerCase() || ''
      const weeklyBull = ws.includes('bullish') || ws === 'w-above'
      const weeklyBear = ws.includes('bearish') || ws === 'w-below'
      if ((dir === 'buy' && weeklyBull) || (dir === 'sell' && weeklyBear)) w += 0.2
      add(r.ticker, 'MA', dir, w)
    })
    momentumData.results.forEach(r => {
      const dir: 'buy' | 'hold' = (r.grade === 'C') ? 'hold' : 'buy'
      add(r.ticker, 'Momentum', dir, GRADE_WEIGHT[r.grade] ?? 1.0)
    })
    bearishData.results.forEach(r => {
      const dir: 'sell' | 'hold' = (r.grade === 'C') ? 'hold' : 'sell'
      add(r.ticker, 'Bearish', dir, GRADE_WEIGHT[r.grade] ?? 1.0)
    })
    fibData.results.forEach(r => {
      let dir: 'buy' | 'sell' | 'hold' = 'hold'
      const td = r.trend_direction?.toLowerCase() || ''
      const sig = r.signal?.toLowerCase() || ''
      if (td.includes('up')) {
        dir = sig.includes('below all') ? 'sell' : 'buy'
      } else if (td.includes('down')) {
        dir = sig.includes('above all') ? 'buy' : 'sell'
      }
      const w = sig.includes('near') ? 1.2 : 1.0
      add(r.ticker, 'Fibonacci', dir, w)
    })

    const result: Record<string, ActionEntry> = {}
    for (const [ticker, entries] of Object.entries(map)) {
      const buyStrats = entries.filter(e => e.direction === 'buy').map(e => e.strategy)
      const sellStrats = entries.filter(e => e.direction === 'sell').map(e => e.strategy)
      const holdStrats = entries.filter(e => e.direction === 'hold').map(e => e.strategy)
      const buyWeight = entries.filter(e => e.direction === 'buy').reduce((s, e) => s + e.weight, 0)
      const sellWeight = entries.filter(e => e.direction === 'sell').reduce((s, e) => s + e.weight, 0)
      const action: 'STRONG_BUY' | 'BUY' | 'HOLD' | 'SELL' | 'STRONG_SELL' =
        buyStrats.length >= 2 && sellStrats.length === 0 ? 'STRONG_BUY'
        : sellStrats.length >= 2 && buyStrats.length === 0 ? 'STRONG_SELL'
        : buyWeight > sellWeight ? 'BUY'
        : sellWeight > buyWeight ? 'SELL'
        : 'HOLD'
      result[ticker] = { action, strategies: entries.map(e => e.strategy), buys: buyStrats.length, sells: sellStrats.length, buyStrats, sellStrats, holdStrats }
    }

    return {
      streakMap: streakRes.summary,
      actionMap: result,
      marketRegime: combined.market_regime ?? null,
    }
  }, [streakDays, fibSwingPct])

  const { data: streakActionData, isFetching: streakLoading } = useQuery<StreakActionData>({
    queryKey: streakQueryKey,
    queryFn: () => fetchStreakAction(),
    enabled: false,
    staleTime: 30 * 60 * 1000,
  })
  const streakMap = streakActionData?.streakMap ?? null
  const actionMap = streakActionData?.actionMap ?? null
  const marketRegime = streakActionData?.marketRegime ?? null

  const loadStreak = useCallback(async (forceRefresh = false) => {
    if (forceRefresh) {
      queryClient.setQueryData(streakQueryKey, undefined)
    }
    await queryClient.fetchQuery({
      queryKey: streakQueryKey,
      queryFn: () => fetchStreakAction(forceRefresh),
    })
  }, [queryClient, streakQueryKey, fetchStreakAction])

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDir(field === 'ticker' ? 'asc' : 'desc')
    }
  }

  const sortIndicator = (field: SortField) => {
    if (sortField !== field) return ''
    return sortDir === 'asc' ? ' ▲' : ' ▼'
  }

  const maFilterOptions: { key: SortField | ''; label: string; type: 'all' | 'proximity' | 'above' | 'below' }[] = [
    { key: '', label: 'All Tickers', type: 'all' },
    { key: 'sma_200', label: 'Below 200 SMA', type: 'below' },
    { key: 'wsma_200', label: 'Below 200W MA', type: 'below' },
    { key: 'sma_50', label: 'Near 50 SMA', type: 'proximity' },
    { key: 'sma_200', label: 'Near 200 SMA', type: 'proximity' },
    { key: 'wsma_50', label: 'Near 50W MA', type: 'proximity' },
    { key: 'wsma_200', label: 'Near 200W MA', type: 'proximity' },
  ]

  const presetFilters: { key: string; label: string; group: string }[] = [
    { key: '', label: 'No Preset', group: '' },
    // Momentum & Activity
    { key: 'most_active', label: 'Most Active (Rel Vol ≥ 2x)', group: 'Momentum' },
    { key: 'high_vol_gainers', label: 'High Vol Gainers (↑ + Vol ≥ 1.5x)', group: 'Momentum' },
    { key: 'high_vol_losers', label: 'High Vol Losers (↓ + Vol ≥ 1.5x)', group: 'Momentum' },
    { key: 'top_gainers', label: 'Top Gainers (Chg% > 2%)', group: 'Momentum' },
    { key: 'top_losers', label: 'Top Losers (Chg% < -2%)', group: 'Momentum' },
    // 52-Week
    { key: 'near_52w_high', label: 'Near 52W High (within 5%)', group: '52-Week' },
    { key: 'near_52w_low', label: 'Near 52W Low (within 10%)', group: '52-Week' },
    { key: 'new_52w_high', label: 'New 52W High', group: '52-Week' },
    { key: 'new_52w_low', label: 'New 52W Low', group: '52-Week' },
    // MA Position
    { key: 'above_all_ma', label: 'Above All MAs (20/50/200)', group: 'MA Position' },
    { key: 'below_all_ma', label: 'Below All MAs (20/50/200)', group: 'MA Position' },
    { key: 'golden_cross', label: 'Golden Setup (50 > 200 SMA)', group: 'MA Position' },
    { key: 'death_cross', label: 'Death Setup (50 < 200 SMA)', group: 'MA Position' },
    // Streak
    { key: 'streak_any', label: 'Any Streak Signal', group: 'Streak' },
    { key: 'streak_multi', label: 'Multi-Strategy (≥2)', group: 'Streak' },
    { key: 'streak_consensus', label: 'Full Consensus (≥3)', group: 'Streak' },
  ]

  // Presets that support adjustable % threshold
  const presetHasPct: Record<string, number> = {
    most_active: 2, high_vol_gainers: 1.5, high_vol_losers: 1.5,
    top_gainers: 2, top_losers: 2,
    near_52w_high: 5, near_52w_low: 10,
    new_52w_high: 0.5, new_52w_low: 0.5,
  }

  const sectors = useMemo(() => (
    [...new Set(data.map(row => row.sector).filter((sector): sector is string => !!sector))]
      .sort((a, b) => a.localeCompare(b))
  ), [data])

  const applyPresetFilter = (rows: TickerOverviewRow[]): TickerOverviewRow[] => {
    const t = presetPct
    switch (presetFilter) {
      case 'most_active':
        return rows.filter(r => r.rel_vol != null && r.rel_vol >= t)
      case 'high_vol_gainers':
        return rows.filter(r => r.chg_pct != null && r.chg_pct > 0 && r.rel_vol != null && r.rel_vol >= t)
      case 'high_vol_losers':
        return rows.filter(r => r.chg_pct != null && r.chg_pct < 0 && r.rel_vol != null && r.rel_vol >= t)
      case 'top_gainers':
        return rows.filter(r => r.chg_pct != null && r.chg_pct > t)
      case 'top_losers':
        return rows.filter(r => r.chg_pct != null && r.chg_pct < -t)
      case 'near_52w_high':
        return rows.filter(r => r.pct_from_high != null && r.pct_from_high >= -t)
      case 'near_52w_low':
        return rows.filter(r => r.pct_from_low != null && r.pct_from_low <= t)
      case 'new_52w_high':
        return rows.filter(r => r.pct_from_high != null && r.pct_from_high >= -t)
      case 'new_52w_low':
        return rows.filter(r => r.pct_from_low != null && r.pct_from_low <= t)
      case 'above_all_ma':
        return rows.filter(r => r.close != null && r.sma_20 != null && r.sma_50 != null && r.sma_200 != null
          && r.close >= r.sma_20 && r.close >= r.sma_50 && r.close >= r.sma_200)
      case 'below_all_ma':
        return rows.filter(r => r.close != null && r.sma_20 != null && r.sma_50 != null && r.sma_200 != null
          && r.close < r.sma_20 && r.close < r.sma_50 && r.close < r.sma_200)
      case 'golden_cross':
        return rows.filter(r => r.sma_50 != null && r.sma_200 != null && r.sma_50 > r.sma_200)
      case 'death_cross':
        return rows.filter(r => r.sma_50 != null && r.sma_200 != null && r.sma_50 < r.sma_200)
      case 'streak_any':
        return rows.filter(r => streakMap?.[r.ticker] && Object.values(streakMap[r.ticker]).some(v => (v as number) > 0))
      case 'streak_multi':
        return rows.filter(r => streakMap?.[r.ticker] && Object.values(streakMap[r.ticker]).filter(v => (v as number) > 0).length >= 2)
      case 'streak_consensus':
        return rows.filter(r => streakMap?.[r.ticker] && Object.values(streakMap[r.ticker]).filter(v => (v as number) > 0).length >= 3)
      default:
        return rows
    }
  }

  const sorted = useMemo(() => {
    let filtered = filter
      ? data.filter((r) => r.ticker.toLowerCase().includes(filter.toLowerCase()))
      : data

    if (sectorFilter) {
      filtered = filtered.filter(row => row.sector === sectorFilter)
    }

    // Apply preset filter
    filtered = applyPresetFilter(filtered)

    // Apply MA filter
    const activeFilter = maFilterOptions[maFilterIdx]
    if (activeFilter && activeFilter.type !== 'all') {
      filtered = filtered.filter((r) => {
        const close = r.close
        const ma = r[activeFilter.key as keyof TickerOverviewRow] as number | null
        if (close == null || ma == null || ma === 0) return false
        if (activeFilter.type === 'above') return close >= ma
        if (activeFilter.type === 'below') return close < ma
        return Math.abs(close - ma) / ma * 100 <= maThreshold
      })
    }

    return [...filtered].sort((a, b) => {
      if (sortField === 'streak_badges') {
        const ac = streakMap?.[a.ticker] ? Object.values(streakMap[a.ticker]).filter(v => typeof v === 'number' && v > 0).length : -1
        const bc = streakMap?.[b.ticker] ? Object.values(streakMap[b.ticker]).filter(v => typeof v === 'number' && v > 0).length : -1
        return sortDir === 'asc' ? ac - bc : bc - ac
      }
      if (sortField === 'action') {
        const order: Record<string, number> = { STRONG_BUY: 0, BUY: 1, HOLD: 2, SELL: 3, STRONG_SELL: 4 }
        const aEntry = actionMap?.[a.ticker]
        const bEntry = actionMap?.[b.ticker]
        const aVal = aEntry ? order[aEntry.action] ?? 1 : 3
        const bVal = bEntry ? order[bEntry.action] ?? 1 : 3
        if (aVal !== bVal) return sortDir === 'asc' ? aVal - bVal : bVal - aVal
        // Secondary: by strategy count desc
        return (bEntry?.strategies.length ?? 0) - (aEntry?.strategies.length ?? 0)
      }
      const aVal = a[sortField as keyof TickerOverviewRow]
      const bVal = b[sortField as keyof TickerOverviewRow]
      if (aVal == null && bVal == null) return 0
      if (aVal == null) return 1
      if (bVal == null) return -1
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      const diff = (aVal as number) - (bVal as number)
      return sortDir === 'asc' ? diff : -diff
    })
  }, [data, sortField, sortDir, filter, sectorFilter, maFilterIdx, maThreshold, presetFilter, presetPct, streakMap])

  const fmt = (val: number | null) => (val != null ? val.toFixed(2) : '—')
  const fmtVol = (val: number) => {
    if (val >= 1_000_000) return (val / 1_000_000).toFixed(1) + 'M'
    if (val >= 1_000) return (val / 1_000).toFixed(0) + 'K'
    return val.toString()
  }

  const maColor = (close: number | null, ma: number | null) => {
    if (close == null || ma == null) return undefined
    return close >= ma
      ? { color: '#16a34a' } // green — above MA
      : { color: '#dc2626' } // red — below MA
  }

  const pctColor = (val: number | null) => {
    if (val == null) return undefined
    return val >= 0 ? { color: '#16a34a' } : { color: '#dc2626' }
  }

  const relVolColor = (val: number | null) => {
    if (val == null) return undefined
    if (val >= 2) return { color: '#7c3aed', fontWeight: 'bold' as const }
    if (val >= 1.5) return { color: '#2563eb' }
    return undefined
  }

  const columns: { key: SortField; label: string; group: string }[] = [
    { key: 'ticker', label: 'Ticker', group: 'price' },
    { key: 'open', label: 'Open', group: 'price' },
    { key: 'high', label: 'High', group: 'price' },
    { key: 'low', label: 'Low', group: 'price' },
    { key: 'close', label: 'Close', group: 'price' },
    { key: 'chg_pct', label: 'Chg%', group: 'momentum' },
    { key: 'volume', label: 'Volume', group: 'momentum' },
    { key: 'rel_vol', label: 'Rel Vol', group: 'momentum' },
    { key: 'high_52w', label: '52W High', group: 'momentum' },
    { key: 'pct_from_high', label: '% from High', group: 'momentum' },
    { key: 'low_52w', label: '52W Low', group: 'momentum' },
    { key: 'pct_from_low', label: '% from Low', group: 'momentum' },
    { key: 'streak_badges', label: 'Signals', group: 'streak' },
    { key: 'action' as SortField, label: 'Action', group: 'streak' },
    { key: 'sma_20', label: 'MA 20', group: 'daily_ma' },
    { key: 'sma_50', label: 'MA 50', group: 'daily_ma' },
    { key: 'sma_200', label: 'MA 200', group: 'daily_ma' },
    { key: 'dist_200', label: '% from 200', group: 'daily_ma' },
    { key: 'wsma_50', label: '50W MA', group: 'weekly_ma' },
    { key: 'wsma_200', label: '200W MA', group: 'weekly_ma' },
    { key: 'dist_200w', label: '% from 200W', group: 'weekly_ma' },
  ]

  const columnGroups = [
    { key: 'price', label: 'Price', bg: '#1e293b' },
    { key: 'momentum', label: 'Momentum', bg: '#0f766e' },
    { key: 'streak', label: 'Streak', bg: '#ea580c' },
    { key: 'daily_ma', label: 'Daily MAs', bg: '#1e40af' },
    { key: 'weekly_ma', label: 'Weekly MAs', bg: '#7e22ce' },
  ]

  const visibleColumns = columns.filter(c => !hiddenCols.has(c.key))

  const getFibAction = (detail: { signal: string; trend: string; nearest_level: string; distance_pct: number; retracement_pct: number } | undefined) => {
    if (!detail) return null
    const { signal, trend, nearest_level } = detail
    const up = trend === 'uptrend_retracement'
    if (signal === 'Below All Levels') return { action: up ? 'Exit Longs' : 'Breakdown', color: '#dc2626', bg: '#fef2f2' }
    if (signal === 'Above All Levels') return { action: up ? 'Strong' : 'Cover Shorts', color: '#16a34a', bg: '#f0fdf4' }
    const levelNum = parseFloat(nearest_level)
    if (up) {
      if (levelNum <= 38.2) return { action: 'Buy', color: '#16a34a', bg: '#f0fdf4' }
      if (levelNum <= 50) return { action: 'Buy Cautious', color: '#65a30d', bg: '#f7fee7' }
      if (levelNum <= 61.8) return { action: 'Watch', color: '#ca8a04', bg: '#fefce8' }
      return { action: 'Risky', color: '#dc2626', bg: '#fef2f2' }
    } else {
      if (levelNum <= 38.2) return { action: 'Short', color: '#dc2626', bg: '#fef2f2' }
      if (levelNum <= 50) return { action: 'Short Cautious', color: '#ea580c', bg: '#fff7ed' }
      if (levelNum <= 61.8) return { action: 'Watch', color: '#ca8a04', bg: '#fefce8' }
      return { action: 'Avoid Short', color: '#16a34a', bg: '#f0fdf4' }
    }
  }

  const STRATEGY_BADGES = [
    { key: 'gaps', color: '#16a34a', letter: 'G', name: 'Gaps' },
    { key: 'ma-crossover', color: '#2563eb', letter: 'M', name: 'MA Crossover' },
    { key: 'momentum-pullback', color: '#ea580c', letter: 'P', name: 'Momentum' },
    { key: 'bearish-bounce', color: '#dc2626', letter: 'B', name: 'Bearish Bounce' },
    { key: 'fibonacci', color: '#7c3aed', letter: 'F', name: 'Fibonacci' },
  ]

  const renderStreakBadges = (ticker: string) => {
    if (!streakMap) return <span style={{ color: '#ccc', fontSize: '0.75rem' }}>—</span>
    const s = streakMap[ticker]
    return (
      <div style={{ display: 'flex', gap: '2px', justifyContent: 'center' }}>
        {STRATEGY_BADGES.map(st => {
          const pct = s?.[st.key]
          const active = pct != null && pct > 0
          const fibDetail = st.key === 'fibonacci' && active ? (s as any)?.fib_detail : null
          const fibAct = fibDetail ? getFibAction(fibDetail) : null
          const fibTip = fibDetail
            ? `${st.name}: ${pct}% | ${fibDetail.nearest_level} · ${fibAct?.action ?? ''} | ${fibDetail.signal} | ${fibDetail.trend === 'uptrend_retracement' ? '↑ Uptrend Pullback' : '↓ Downtrend Bounce'} | Retrace: ${fibDetail.retracement_pct}% | Dist: ${fibDetail.distance_pct}%`
            : `${st.name}: ${pct != null ? pct + '%' : 'N/A'}`
          return (
            <span
              key={st.key}
              title={st.key === 'fibonacci' ? fibTip : `${st.name}: ${pct != null ? pct + '%' : 'N/A'}`}
              style={{
                display: 'inline-block', width: 16, height: 16, borderRadius: '50%',
                background: active ? st.color : '#e2e8f0',
                color: active ? '#fff' : '#94a3b8',
                fontSize: '0.6rem', lineHeight: '16px', textAlign: 'center', fontWeight: 700,
                opacity: active ? (pct! >= 80 ? 1 : pct! >= 50 ? 0.7 : 0.5) : 0.3,
              }}
            >{st.letter}</span>
          )
        })}
      </div>
    )
  }

  const ACTION_STYLE: Record<string, { bg: string; color: string; label: string }> = {
    STRONG_BUY: { bg: '#d1fae5', color: '#065f46', label: 'Strong Buy' },
    BUY:        { bg: '#e6f4ea', color: '#1a7d3f', label: 'Buy' },
    HOLD:       { bg: '#fff8e6', color: '#b08a1a', label: 'Hold' },
    SELL:       { bg: '#fdecea', color: '#b8524e', label: 'Sell' },
    STRONG_SELL:{ bg: '#fecaca', color: '#7f1d1d', label: 'Strong Sell' },
  }

  const renderCell = (key: string, row: TickerOverviewRow): { content: React.ReactNode; style?: React.CSSProperties } => {
    switch (key) {
      case 'ticker': return { content: row.ticker, style: { fontWeight: 'bold', color: '#2563eb', textAlign: 'left' } }
      case 'open': return { content: fmt(row.open) }
      case 'high': return { content: fmt(row.high) }
      case 'low': return { content: fmt(row.low) }
      case 'close': return { content: fmt(row.close), style: { fontWeight: 'bold' } }
      case 'chg_pct': return {
        content: row.chg_pct != null ? (row.chg_pct > 0 ? '+' : '') + row.chg_pct.toFixed(2) + '%' : '—',
        style: { fontWeight: 600, ...pctColor(row.chg_pct) },
      }
      case 'volume': return { content: fmtVol(row.volume) }
      case 'rel_vol': return {
        content: row.rel_vol != null ? row.rel_vol.toFixed(1) + 'x' : '—',
        style: relVolColor(row.rel_vol) || {},
      }
      case 'high_52w': return { content: fmt(row.high_52w) }
      case 'pct_from_high': return {
        content: row.pct_from_high != null ? row.pct_from_high.toFixed(1) + '%' : '—',
        style: pctColor(row.pct_from_high) || {},
      }
      case 'low_52w': return { content: fmt(row.low_52w) }
      case 'pct_from_low': return {
        content: row.pct_from_low != null ? '+' + row.pct_from_low.toFixed(1) + '%' : '—',
        style: pctColor(row.pct_from_low) || {},
      }
      case 'streak_badges': return { content: renderStreakBadges(row.ticker), style: { textAlign: 'center' } }
      case 'action': {
        if (!actionMap) return { content: <span style={{ color: '#ccc', fontSize: '0.75rem' }}>—</span>, style: { textAlign: 'center' } }
        const entry = actionMap[row.ticker]
        if (!entry) return { content: <span style={{ color: '#94a3b8', fontSize: '0.72rem' }}>—</span>, style: { textAlign: 'center' } }
        const ast = ACTION_STYLE[entry.action]
        return {
          content: (
            <span
              title={[entry.buys > 0 && `Buy (${entry.buys}): ${entry.buyStrats.join(', ')}`, entry.sells > 0 && `Sell (${entry.sells}): ${entry.sellStrats.join(', ')}`, entry.holdStrats.length > 0 && `Hold (${entry.holdStrats.length}): ${entry.holdStrats.join(', ')}`].filter(Boolean).join(' | ')}
              style={{ cursor: 'help' }}
            >
              <span style={{
                padding: '0.15rem 0.45rem', borderRadius: '10px',
                fontSize: '0.72rem', fontWeight: 700, background: ast.bg, color: ast.color,
              }}>{ast.label}</span>
              {marketRegime && (
                (marketRegime.caution_buy && (entry.action === 'STRONG_BUY' || entry.action === 'BUY'))
                || (marketRegime.caution_sell && (entry.action === 'STRONG_SELL' || entry.action === 'SELL'))
              ) && (
                <span title={marketRegime.caution_buy ? 'Counter-trend: market bearish' : 'Counter-trend: market bullish'} style={{ marginLeft: '3px', cursor: 'help', fontSize: '0.68rem' }}>⚠️</span>
              )}
            </span>
          ),
          style: { textAlign: 'center' },
        }
      }
      case 'sma_20': return { content: fmt(row.sma_20), style: maColor(row.close, row.sma_20) || {} }
      case 'sma_50': return { content: fmt(row.sma_50), style: maColor(row.close, row.sma_50) || {} }
      case 'sma_200': return { content: fmt(row.sma_200), style: { fontWeight: 'bold', ...(maColor(row.close, row.sma_200) || {}) } }
      case 'dist_200': return {
        content: row.dist_200 != null ? (row.dist_200 > 0 ? '+' : '') + row.dist_200.toFixed(1) + '%' : '—',
        style: pctColor(row.dist_200) || {},
      }
      case 'wsma_50': return { content: fmt(row.wsma_50), style: maColor(row.close, row.wsma_50) || {} }
      case 'wsma_200': return { content: fmt(row.wsma_200), style: { fontWeight: 'bold', ...(maColor(row.close, row.wsma_200) || {}) } }
      case 'dist_200w': return {
        content: row.dist_200w != null ? (row.dist_200w > 0 ? '+' : '') + row.dist_200w.toFixed(1) + '%' : '—',
        style: pctColor(row.dist_200w) || {},
      }
      default: return { content: '—' }
    }
  }

  return (
    <>
    <div style={{ padding: '8px 4px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '16px' }}>
        <div>
          <h2 style={{ margin: 0 }}>All Tickers Overview</h2>
          <p style={{ color: '#666', margin: '4px 0 0' }}>
            {data.length} selected tickers — price, momentum, key MAs, and weekly MAs.
            {scanDate && <span style={{ marginLeft: 12, fontSize: 13, color: '#b45309', fontWeight: 600 }}>📅 Historical: {scanDate}</span>}
            <span style={{ marginLeft: 12, fontSize: 13 }}>
              <span style={{ color: '#16a34a' }}>■</span> Above MA&nbsp;
              <span style={{ color: '#dc2626' }}>■</span> Below MA&nbsp;
              <span style={{ color: '#7c3aed' }}>■</span> High Rel Vol (≥2x)
            </span>
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexShrink: 0 }}>
          <label style={{ fontSize: '0.85rem', color: '#666', whiteSpace: 'nowrap' }}>Scan Date:</label>
          <input
            type="date"
            value={scanDate || latestDate}
            onChange={(e) => { const d = e.target.value; setScanDate(d) }}
            style={{ padding: '6px 8px', borderRadius: '4px', border: '1px solid #ccc', fontSize: '14px' }}
          />
          {scanDate && (
            <button
              onClick={() => { setScanDate('') }}
              style={{ padding: '4px 8px', borderRadius: '4px', border: '1px solid #ccc', background: '#fff', cursor: 'pointer', fontSize: '12px' }}
              title="Reset to latest"
            >
              ✕
            </button>
          )}
          <button
            onClick={handleRefresh}
            disabled={loading}
            style={{ padding: '6px 14px', borderRadius: '4px', border: 'none', background: '#2563eb', color: '#fff', cursor: 'pointer', fontSize: '13px', fontWeight: 600, opacity: loading ? 0.6 : 1 }}
          >
            {loading ? 'Loading...' : 'Refresh'}
          </button>
        </div>
      </div>

      <div style={{ marginBottom: '16px', display: 'flex', gap: '12px', alignItems: 'center', flexWrap: 'wrap' }}>
        <input
          type="text"
          placeholder="Filter by ticker..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{
            padding: '8px 12px',
            borderRadius: '4px',
            border: '1px solid #ccc',
            width: '200px',
            fontSize: '14px',
          }}
        />
        <select
          value={sectorFilter}
          onChange={(event) => setSectorFilter(event.target.value)}
          aria-label="Filter by sector"
          style={{
            padding: '8px 12px',
            borderRadius: '4px',
            border: '1px solid #ccc',
            fontSize: '14px',
            background: sectorFilter ? '#eff6ff' : '#fff',
            fontWeight: sectorFilter ? 600 : 400,
            maxWidth: '220px',
          }}
        >
          <option value="">All Sectors</option>
          {sectors.map(sector => (
            <option key={sector} value={sector}>{sector}</option>
          ))}
        </select>
        <select
          value={presetFilter}
          onChange={(e) => {
            const key = e.target.value
            setPresetFilter(key)
            if (key) setMaFilterIdx(0) // reset MA filter when preset is chosen
            if (key in presetHasPct) setPresetPct(presetHasPct[key])
          }}
          style={{
            padding: '8px 12px',
            borderRadius: '4px',
            border: '1px solid #ccc',
            fontSize: '14px',
            background: presetFilter ? '#eff6ff' : '#fff',
            fontWeight: presetFilter ? 600 : 400,
          }}
        >
          {(() => {
            const groups = [...new Set(presetFilters.map(f => f.group).filter(Boolean))]
            return (
              <>
                <option value="">No Preset</option>
                {groups.map(g => (
                  <optgroup key={g} label={g}>
                    {presetFilters.filter(f => f.group === g).map(f => (
                      <option key={f.key} value={f.key}>{f.label}</option>
                    ))}
                  </optgroup>
                ))}
              </>
            )
          })()}
        </select>
        {presetFilter && presetFilter in presetHasPct && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            <label style={{ fontSize: '13px', color: '#555' }}>
              {['most_active', 'high_vol_gainers', 'high_vol_losers'].includes(presetFilter) ? '≥' : '±'}
            </label>
            <input
              type="number"
              min={0.1}
              max={50}
              step={0.5}
              value={presetPct}
              onChange={(e) => setPresetPct(Number(e.target.value))}
              style={{
                padding: '6px 8px',
                borderRadius: '4px',
                border: '1px solid #ccc',
                width: '60px',
                fontSize: '14px',
                textAlign: 'center',
              }}
            />
            <span style={{ fontSize: '13px', color: '#555' }}>
              {['most_active', 'high_vol_gainers', 'high_vol_losers'].includes(presetFilter) ? 'x' : '%'}
            </span>
          </div>
        )}
        <select
          value={maFilterIdx}
          onChange={(e) => {
            const idx = Number(e.target.value)
            setMaFilterIdx(idx)
            if (idx > 0) setPresetFilter('') // reset preset when MA filter is chosen
          }}
          style={{
            padding: '8px 12px',
            borderRadius: '4px',
            border: '1px solid #ccc',
            fontSize: '14px',
            background: maFilterIdx > 0 ? '#eff6ff' : '#fff',
            fontWeight: maFilterIdx > 0 ? 600 : 400,
          }}
        >
          {maFilterOptions.map((opt, idx) => (
            <option key={idx} value={idx}>{opt.label}</option>
          ))}
        </select>
        {maFilterOptions[maFilterIdx]?.type === 'proximity' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            <label style={{ fontSize: '13px', color: '#555' }}>within</label>
            <input
              type="number"
              min={0.5}
              max={20}
              step={0.5}
              value={maThreshold}
              onChange={(e) => setMaThreshold(Number(e.target.value))}
              style={{
                padding: '6px 8px',
                borderRadius: '4px',
                border: '1px solid #ccc',
                width: '60px',
                fontSize: '14px',
                textAlign: 'center',
              }}
            />
            <span style={{ fontSize: '13px', color: '#555' }}>%</span>
          </div>
        )}
        <span style={{ color: '#888', fontSize: '13px' }}>
          Showing {sorted.length} of {data.length}
        </span>

        <div style={{ width: '1px', height: '24px', background: '#ddd' }} />

        <label style={{ fontSize: '0.82rem', color: '#666', whiteSpace: 'nowrap' }}>Streak:</label>
        <input
          type="number" min={2} max={10} value={streakDays}
          onChange={(e) => setStreakDays(Number(e.target.value))}
          style={{ width: 42, padding: '5px 4px', borderRadius: 4, border: '1px solid #ccc', fontSize: '13px', textAlign: 'center' }}
        />
        <label style={{ fontSize: '0.82rem', color: '#7c3aed', whiteSpace: 'nowrap', fontWeight: 600 }}>Fib:</label>
        <select
          value={fibSwingPct}
          onChange={(e) => setFibSwingPct(Number(e.target.value))}
          style={{ padding: '5px 6px', borderRadius: 4, border: '1px solid #ccc', fontSize: '13px', background: '#faf5ff' }}
        >
          <option value={3}>3%</option>
          <option value={5}>5%</option>
          <option value={8}>8%</option>
          <option value={12}>12%</option>
        </select>
        <button
          onClick={() => loadStreak(!!streakMap)}
          disabled={streakLoading}
          style={{
            padding: '6px 14px', borderRadius: 4, border: 'none', background: '#ea580c',
            color: '#fff', cursor: 'pointer', fontSize: '13px', fontWeight: 600,
            opacity: streakLoading ? 0.6 : 1,
          }}
        >
          {streakLoading ? 'Loading...' : streakMap ? 'Refresh Streak' : 'Load Streak'}
        </button>
        {streakMap && (
          <span style={{ fontSize: '0.82rem', color: '#ea580c', fontWeight: 600 }}>
            {Object.keys(streakMap).length} signals
          </span>
        )}
        {actionMap && (
          <span style={{ fontSize: '0.82rem', color: '#1a7d3f', fontWeight: 600 }}>
            {Object.values(actionMap).filter(e => e.action === 'BUY').length}B /
            {Object.values(actionMap).filter(e => e.action === 'SELL').length}S /
            {Object.values(actionMap).filter(e => e.action === 'HOLD').length}H
          </span>
        )}

        <div style={{ position: 'relative', flexShrink: 0, marginLeft: 'auto' }}>
          <button
            onClick={() => setShowColPicker(!showColPicker)}
            aria-label={`Choose columns${hiddenCols.size > 0 ? ` (${hiddenCols.size} hidden)` : ''}`}
            title={`Choose columns${hiddenCols.size > 0 ? ` (${hiddenCols.size} hidden)` : ''}`}
            style={{
              width: '32px', height: '32px', padding: 0, borderRadius: '4px', border: '1px solid #ccc',
              background: showColPicker ? '#eff6ff' : '#fff', cursor: 'pointer', fontSize: '17px',
              display: 'grid', placeItems: 'center', lineHeight: 1,
            }}
          >
            <Settings size={18} strokeWidth={2} aria-hidden="true" />
          </button>
          {showColPicker && (
            <div style={{
              position: 'absolute', right: 0, top: '100%', zIndex: 50, marginTop: 4,
              background: '#fff', border: '1px solid #e2e8f0', borderRadius: '6px',
              padding: '12px', boxShadow: '0 4px 12px rgba(0,0,0,0.15)', minWidth: '240px',
            }}>
              {columnGroups.filter(g => g.key !== 'streak').map(g => (
                <div key={g.key} style={{ marginBottom: '8px' }}>
                  <div style={{ fontSize: '0.8rem', fontWeight: 700, color: g.bg, marginBottom: '2px', borderBottom: `2px solid ${g.bg}`, paddingBottom: '2px' }}>{g.label}</div>
                  {columns.filter(c => c.group === g.key && c.key !== 'ticker').map(c => (
                    <label key={c.key} style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '0.82rem', cursor: 'pointer', padding: '2px 0' }}>
                      <input
                        type="checkbox"
                        checked={!hiddenCols.has(c.key)}
                        onChange={() => {
                          const next = new Set(hiddenCols)
                          if (next.has(c.key)) next.delete(c.key); else next.add(c.key)
                          setHiddenCols(next)
                        }}
                      />
                      {c.label}
                    </label>
                  ))}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {loading && <p>Loading overview for all selected tickers...</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {!loading && !error && data.length > 0 && (
        <div>
          <table
            style={{
              width: '100%',
              borderCollapse: 'collapse',
              fontSize: '11.5px',
              whiteSpace: 'nowrap',
              tableLayout: 'auto',
            }}
          >
            <thead>
              {/* Dynamic group header row */}
              <tr>
                {columnGroups.map((g, gi) => {
                  const count = visibleColumns.filter(c => c.group === g.key).length
                  if (count === 0) return null
                  const isLast = gi === columnGroups.length - 1 ||
                    columnGroups.slice(gi + 1).every(ng => visibleColumns.filter(c => c.group === ng.key).length === 0)
                  return (
                    <th key={g.key} colSpan={count} style={{
                      background: g.bg, color: '#fff', padding: '6px 8px', textAlign: 'center',
                      borderRight: !isLast ? '2px solid #475569' : undefined,
                    }}>
                      {g.label}
                    </th>
                  )
                })}
              </tr>
              {/* Dynamic column header row */}
              <tr>
                {visibleColumns.map((col, ci) => {
                  const nextCol = visibleColumns[ci + 1]
                  const isGroupEnd = nextCol != null && nextCol.group !== col.group
                  return (
                    <th
                      key={col.key}
                      onClick={() => handleSort(col.key)}
                      style={{
                        padding: '6px 5px',
                        background: '#f1f5f9',
                        borderBottom: '2px solid #cbd5e1',
                        cursor: 'pointer',
                        textAlign: col.key === 'ticker' ? 'left' : col.key === 'streak_badges' ? 'center' : 'right',
                        userSelect: 'none',
                        borderRight: isGroupEnd ? '2px solid #cbd5e1' : undefined,
                      }}
                    >
                      {col.label}
                      {sortIndicator(col.key)}
                    </th>
                  )
                })}
              </tr>
            </thead>
            <tbody>
              {sorted.map((row, i) => (
                <tr
                  key={row.ticker}
                  style={{
                    background: i % 2 === 0 ? '#fff' : '#f8fafc',
                    cursor: 'pointer',
                  }}
                  onClick={() => navigate(`/ticker/${row.ticker}`)}
                  title={`Click to view ${row.ticker} chart`}
                >
                  {visibleColumns.map((col, ci) => {
                    const { content, style: cellStyle } = renderCell(col.key, row)
                    const nextCol = visibleColumns[ci + 1]
                    const isGroupEnd = nextCol != null && nextCol.group !== col.group
                    return (
                      <td key={col.key} style={{
                        padding: '4px 5px',
                        textAlign: col.key === 'ticker' ? 'left' : col.key === 'streak_badges' ? 'center' : 'right',
                        ...cellStyle,
                        ...(isGroupEnd ? { borderRight: '2px solid #e2e8f0' } : {}),
                      }}>
                        {content}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
    <StreakPanel />
    </>
  )
}

export default TickersOverview
