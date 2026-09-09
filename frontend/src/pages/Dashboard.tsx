import React, { useState, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  scanAll,
  getTickersOverview, getLatestPriceDate,
  getDailyRecommendationsWithFallback,
  getCrossSectionalSignals,
  getSignalSectors,
  getDiscoveryStates,
  getScannerEventSummary,
  getScannerEventBacklog,
  getScannerEvents,
  TickerOverviewRow,
  MarketRegime,
  DailyRecommendationsResponse,
  CrossSectionalListResponse,
  SectorCoverage,
  DiscoveryResponse,
  DiscoveryState,
  ScannerEventSummaryRow,
  ScannerBacklogRow,
  ScannerEventRow,
  ScannerInterval,
} from '../services/api'
import { usePublishPageContext } from '../layout/pageContext'
import { CommandBar, CommandSpacer } from '../layout/PageChrome'

// Muted strategy palette
const STRAT = [
  { key: 'Gap',       color: 'var(--tm-accent)', bg: 'var(--tm-accent-soft)', icon: '📊', path: '/gaps' },
  { key: 'MA',        color: 'var(--tm-pos)',    bg: 'var(--tm-pos-soft)',    icon: '📈', path: '/ma-crossover' },
  { key: 'Momentum',  color: 'var(--tm-warn)',   bg: 'var(--tm-warn-soft)',   icon: '🚀', path: '/momentum-pullback' },
  { key: 'Bearish',   color: 'var(--tm-neg)',    bg: 'var(--tm-neg-soft)',    icon: '📉', path: '/bearish-bounce' },
  { key: 'Fibonacci', color: 'var(--tm-alt)',    bg: 'var(--tm-alt-soft)',    icon: '🔢', path: '/fibonacci' },
] as const
const STRAT_COLOR: Record<string, string> = Object.fromEntries(STRAT.map(s => [s.key, s.color]))

const TOP_N_OPTIONS = [5, 10, 15, 20, 30]
const MIN_VOL_OPTIONS = [{ label: 'Any', value: 0 }, { label: '100K+', value: 100_000 }, { label: '500K+', value: 500_000 }, { label: '1M+', value: 1_000_000 }]
const MIN_CHG_OPTIONS = [{ label: 'Any', value: 0 }, { label: '1%+', value: 1 }, { label: '2%+', value: 2 }, { label: '5%+', value: 5 }]

const CARD: React.CSSProperties = {
  background: 'var(--tm-surface)', border: '1px solid var(--tm-line)', borderRadius: '10px', padding: '16px',
  boxShadow: '0 1px 3px rgba(0,0,0,0.04)', display: 'flex', flexDirection: 'column',
}
const SECTION_TITLE: React.CSSProperties = {
  margin: '0 0 12px', fontSize: '0.92rem', fontWeight: 700, color: 'var(--tm-ink)',
  display: 'flex', alignItems: 'center', gap: '6px',
}
const SUBTITLE: React.CSSProperties = { fontSize: '0.72rem', color: 'var(--tm-faint)', fontWeight: 400 }
const selectStyle: React.CSSProperties = {
  padding: '3px 6px', borderRadius: '5px', border: '1px solid var(--tm-line-strong)',
  fontSize: '0.78rem', background: 'var(--tm-surface-raised)', color: 'var(--tm-muted)', cursor: 'pointer',
}

function Dashboard() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [loading, setLoading] = useState(false)
  const { data: latestDate = '' } = useQuery({
    queryKey: ['latest-price-date'],
    queryFn: () => getLatestPriceDate(),
  })
  const { data: overview = [], isFetching: overviewLoading } = useQuery<TickerOverviewRow[]>({
    queryKey: ['tickers', 'overview'],
    queryFn: () => getTickersOverview(),
  })
  const { data: dailyRecs = { trade_date: '', bull_recommendations: [], bear_recommendations: [], total_bull: 0, total_bear: 0, used_date: '' }, isFetching: recsLoading } = useQuery<DailyRecommendationsResponse & { used_date: string }>({
    queryKey: ['daily-recommendations'],
    queryFn: () => getDailyRecommendationsWithFallback(),
  })
  const [signalSector, setSignalSector] = useState<string>('')
  const { data: sectorList = { sectors: [] } } = useQuery<{ sectors: SectorCoverage[] }>({
    queryKey: ['xs-signal', 'sectors'],
    queryFn: () => getSignalSectors(),
  })
  const { data: xsLong = null, isFetching: xsLongLoading } = useQuery<CrossSectionalListResponse>({
    queryKey: ['xs-signal', 'LONG', signalSector],
    queryFn: () => getCrossSectionalSignals('LONG', 10, signalSector || undefined),
  })
  const { data: xsShort = null } = useQuery<CrossSectionalListResponse>({
    queryKey: ['xs-signal', 'SHORT', signalSector],
    queryFn: () => getCrossSectionalSignals('SHORT', 10, signalSector || undefined),
  })
  const xsShortRows = xsShort?.results ?? []
  const [discoveryState, setDiscoveryState] = useState<DiscoveryState>('REVERSAL_CONFIRMED')
  const { data: discovery = null, isFetching: discoveryLoading } = useQuery<DiscoveryResponse>({
    queryKey: ['market-discovery', discoveryState, signalSector],
    queryFn: () => getDiscoveryStates(discoveryState, 100, signalSector || undefined),
  })
  const [scannerInterval, setScannerInterval] = useState<ScannerInterval>('1d')
  const { data: scannerSummary = { results: [] } } = useQuery<{ results: ScannerEventSummaryRow[] }>({
    queryKey: ['scanner-events', 'summary', scannerInterval],
    queryFn: () => getScannerEventSummary(scannerInterval, 1),
  })
  const { data: scannerBacklog = { results: [] } } = useQuery<{ results: ScannerBacklogRow[] }>({
    queryKey: ['scanner-events', 'backlog'],
    queryFn: () => getScannerEventBacklog(),
  })
  const { data: scannerEvents = { results: [] } } = useQuery<{ results: ScannerEventRow[] }>({
    queryKey: ['scanner-events', 'latest', scannerInterval],
    queryFn: () => getScannerEvents(scannerInterval, 20),
  })
  type StratEntry = { strategy: string; direction: 'buy' | 'sell' | 'hold'; weight: number }
  type FibWatchEntry = { ticker: string; signal: string; nearest_level: string; distance_pct: number; trend: string }
  type DashboardScanData = {
    tickerStrategies: Record<string, StratEntry[]>
    fibSentiment: Record<string, number>
    fibWatchlist: FibWatchEntry[]
    marketRegime: MarketRegime | null
    scanTime: string
  }

  const fetchDashboardScan = useCallback(async (refresh = false): Promise<DashboardScanData> => {
    if (refresh) {
      queryClient.invalidateQueries({ queryKey: ['latest-price-date'] })
      queryClient.invalidateQueries({ queryKey: ['tickers', 'overview'] })
    }
    const combined = await scanAll(undefined, undefined, refresh)
    const gapData = combined.gaps
    const maData = combined.ma_crossover
    const momentumData = combined.momentum_pullback
    const bearishData = combined.bearish_bounce
    const fibData = combined.fibonacci

    const GRADE_WEIGHT: Record<string, number> = { 'A+': 1.5, 'A': 1.2, 'B+': 1.0, 'B': 0.8 }
    const map: Record<string, StratEntry[]> = {}
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

    const sent: Record<string, number> = { Bullish: 0, Bearish: 0, Watch: 0, Neutral: 0 }
    fibData.results.forEach(r => {
      const t = r.trend_direction || ''
      const d = Math.abs(r.distance_pct ?? 100)
      if (d > 5) { sent.Neutral++; return }
      if (t.toLowerCase().includes('up')) {
        const lvl = r.nearest_level || ''
        if (lvl.includes('23.6') || lvl.includes('38.2') || lvl.includes('50.0')) sent.Bullish++
        else sent.Watch++
      } else if (t.toLowerCase().includes('down')) {
        const lvl = r.nearest_level || ''
        if (lvl.includes('23.6') || lvl.includes('38.2') || lvl.includes('50.0')) sent.Bearish++
        else sent.Watch++
      } else { sent.Neutral++ }
    })

    const watchlist: FibWatchEntry[] = fibData.results
      .filter(r => Math.abs(r.distance_pct ?? 100) <= 2)
      .sort((a, b) => Math.abs(a.distance_pct ?? 100) - Math.abs(b.distance_pct ?? 100))
      .slice(0, 10)
      .map(r => ({
        ticker: r.ticker,
        signal: r.signal || '',
        nearest_level: r.nearest_level || '',
        distance_pct: r.distance_pct ?? 0,
        trend: (r.trend_direction || '').replace('_retracement', ''),
      }))

    return {
      tickerStrategies: map,
      fibSentiment: sent,
      fibWatchlist: watchlist,
      marketRegime: combined.market_regime ?? null,
      scanTime: new Date().toLocaleTimeString(),
    }
  }, [queryClient])

  const scanQueryKey = ['dashboard-scan']
  const { data: scanData } = useQuery<DashboardScanData>({
    queryKey: scanQueryKey,
    queryFn: () => fetchDashboardScan(),
    enabled: false,
    staleTime: 30 * 60 * 1000,
  })
  const scanComplete = !!scanData
  const tickerStrategies = scanData?.tickerStrategies ?? {}
  const fibSentiment = scanData?.fibSentiment ?? {}
  const fibWatchlist = scanData?.fibWatchlist ?? []
  const marketRegime = scanData?.marketRegime ?? null

  const runScan = useCallback(async (refresh = false) => {
    setLoading(true)
    try {
      if (refresh) {
        queryClient.setQueryData(scanQueryKey, undefined)
      }
      await queryClient.fetchQuery({
        queryKey: scanQueryKey,
        queryFn: () => fetchDashboardScan(refresh),
      })
    } catch (err) {
      console.error('Dashboard scan failed:', err)
    }
    setLoading(false)
  }, [queryClient, fetchDashboardScan])

  // Market breadth — computed from overview data (% of tickers above 200 SMA)
  const breadth = useMemo(() => {
    const withSma = overview.filter(r => r.sma_200 != null && r.close != null)
    if (withSma.length === 0) return null
    const above200 = withSma.filter(r => (r.close ?? 0) > (r.sma_200 ?? 0)).length
    const above50 = withSma.filter(r => (r.close ?? 0) > (r.sma_50 ?? 0)).length
    const above20 = withSma.filter(r => (r.close ?? 0) > (r.sma_20 ?? 0)).length
    return {
      total: withSma.length,
      above_200: above200,
      pct_200: Math.round(above200 / withSma.length * 100),
      above_50: above50,
      pct_50: Math.round(above50 / withSma.length * 100),
      above_20: above20,
      pct_20: Math.round(above20 / withSma.length * 100),
    }
  }, [overview])
  // Movers filters
  const [topN, setTopN] = useState(10)
  const [minVol, setMinVol] = useState(0)
  const [minChg, setMinChg] = useState(0)

  // Sort state per table
  type SortDir = 'asc' | 'desc'
  type SortState<K extends string = string> = { col: K; dir: SortDir }
  const [moverSort, setMoverSort] = useState<SortState>({ col: 'chg_pct', dir: 'desc' })
  const [volSort, setVolSort] = useState<SortState>({ col: 'rel_vol', dir: 'desc' })
  const [confSort, setConfSort] = useState<SortState>({ col: 'count', dir: 'desc' })

  const toggleSort = <K extends string>(cur: SortState<K>, col: K, setter: (s: SortState<K>) => void) => {
    setter({ col, dir: cur.col === col && cur.dir === 'asc' ? 'desc' : cur.col === col ? 'asc' : 'desc' })
  }

  const sortArrow = (active: boolean, dir: SortDir) =>
    active ? (dir === 'asc' ? ' ▲' : ' ▼') : ''

  const thSort = (label: string, col: string, sort: SortState, setter: (s: SortState) => void, align: 'left' | 'center' | 'right' = 'left') => ({
    onClick: () => toggleSort(sort, col, setter),
    children: <>{label}{sortArrow(sort.col === col, sort.dir)}</>,
    style: { textAlign: align, padding: '4px 0', color: sort.col === col ? 'var(--tm-ink)' : 'var(--tm-faint)', fontSize: '0.72rem', fontWeight: 600, cursor: 'pointer', userSelect: 'none' } as React.CSSProperties,
  })

  const highVolMovers = useMemo(() =>
    [...overview].filter(r => (r.rel_vol ?? 0) >= 2 && r.chg_pct != null)
      .sort((a, b) => (b.rel_vol ?? 0) - (a.rel_vol ?? 0))
      .slice(0, 20),
    [overview]
  )

  const filteredOverview = useMemo(() =>
    overview.filter(r => {
      if (minVol > 0 && (r.volume ?? 0) < minVol) return false
      if (minChg > 0 && Math.abs(r.chg_pct ?? 0) < minChg) return false
      return true
    }),
    [overview, minVol, minChg]
  )

  const sortMovers = (rows: TickerOverviewRow[]) => {
    const m = moverSort.dir === 'asc' ? 1 : -1
    return [...rows].sort((a, b) => {
      switch (moverSort.col) {
        case 'ticker': return m * a.ticker.localeCompare(b.ticker)
        case 'close': return m * ((a.close ?? 0) - (b.close ?? 0))
        case 'chg_pct': return m * ((a.chg_pct ?? 0) - (b.chg_pct ?? 0))
        case 'volume': return m * ((a.volume ?? 0) - (b.volume ?? 0))
        case 'rel_vol': return m * ((a.rel_vol ?? 0) - (b.rel_vol ?? 0))
        default: return 0
      }
    })
  }

  const topGainers = useMemo(() =>
    sortMovers([...filteredOverview].filter(r => r.chg_pct != null && r.chg_pct > 0)
      .sort((a, b) => (b.chg_pct ?? 0) - (a.chg_pct ?? 0)).slice(0, topN)),
    [filteredOverview, topN, moverSort]
  )
  const topLosers = useMemo(() =>
    sortMovers([...filteredOverview].filter(r => r.chg_pct != null && r.chg_pct < 0)
      .sort((a, b) => (a.chg_pct ?? 0) - (b.chg_pct ?? 0)).slice(0, topN)),
    [filteredOverview, topN, moverSort]
  )

  const confluenceTickers = useMemo(() =>
    Object.entries(tickerStrategies)
      .filter(([, entries]) => entries.length >= 2)
      .sort((a, b) => b[1].length - a[1].length)
      .slice(0, 20)
      .map(([ticker, entries]) => {
        const strategies = entries.map(e => e.strategy)
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
        return { ticker, strategies, count: entries.length, action, buys: buyStrats.length, sells: sellStrats.length, buyStrats, sellStrats, holdStrats }
      }),
    [tickerStrategies]
  )

  const actionSummary = useMemo(() => {
    const c: Record<string, number> = { STRONG_BUY: 0, BUY: 0, HOLD: 0, SELL: 0, STRONG_SELL: 0 }
    confluenceTickers.forEach(t => c[t.action]++)
    return c
  }, [confluenceTickers])

  const ACTION_STYLE: Record<string, { bg: string; color: string; label: string }> = {
    STRONG_BUY: { bg: 'var(--tm-pos-soft)', color: 'var(--tm-pos)', label: 'Strong Buy' },
    BUY:        { bg: 'var(--tm-pos-soft)', color: 'var(--tm-pos)', label: 'Buy' },
    HOLD:       { bg: 'var(--tm-warn-soft)', color: 'var(--tm-warn)', label: 'Hold' },
    SELL:       { bg: 'var(--tm-neg-soft)', color: 'var(--tm-neg)', label: 'Sell' },
    STRONG_SELL:{ bg: 'var(--tm-neg-soft)', color: 'var(--tm-neg)', label: 'Strong Sell' },
  }

  const pctBadge = (pct: number | null) => {
    if (pct == null) return <span style={{ color: 'var(--tm-faint)' }}>—</span>
    const color = pct >= 0 ? 'var(--tm-pos)' : 'var(--tm-neg)'
    return <span style={{ color, fontWeight: 700 }}>{pct >= 0 ? '+' : ''}{pct.toFixed(2)}%</span>
  }

  const emptySlate = (msg: string) => (
    <div style={{ color: 'var(--tm-faint)', fontSize: '0.82rem', padding: '24px 0', textAlign: 'center', fontStyle: 'italic' }}>
      {msg}
    </div>
  )

  const MOVERS_HEIGHT = 340

  // Market regime style/color mapping
  const REGIME_STYLE: Record<string, { icon: string; color: string; bg: string }> = {
    'Strong Bull': { icon: '🟢', color: 'var(--tm-pos)', bg: 'var(--tm-pos-soft)' },
    'Bull':        { icon: '🟩', color: 'var(--tm-pos)', bg: 'var(--tm-pos-soft)' },
    'Caution':     { icon: '🟡', color: 'var(--tm-warn)', bg: 'var(--tm-warn-soft)' },
    'Bear Rally':  { icon: '🟠', color: 'var(--tm-warn)', bg: 'var(--tm-warn-soft)' },
    'Bear':        { icon: '🔴', color: 'var(--tm-neg)', bg: 'var(--tm-neg-soft)' },
    'Strong Bear': { icon: '⛔', color: 'var(--tm-neg)', bg: 'var(--tm-neg-soft)' },
    'Unknown':     { icon: '⚪', color: 'var(--tm-muted)', bg: 'var(--tm-surface-sunken)' },
  }

  // Should this action get a caution flag based on market regime?
  const getCaution = (action: string): string | null => {
    if (!marketRegime) return null
    const { caution_buy, caution_sell, regime } = marketRegime
    if (caution_buy && (action === 'STRONG_BUY' || action === 'BUY'))
      return regime === 'Bear Rally' ? 'Dead-cat bounce risk' : 'Counter-trend in bear market'
    if (caution_sell && (action === 'STRONG_SELL' || action === 'SELL'))
      return 'Counter-trend in bull market'
    return null
  }

  const MOVER_COLS: { label: string; col: string; align: 'left' | 'right' }[] = [
    { label: 'Ticker', col: 'ticker', align: 'left' },
    { label: 'Price', col: 'close', align: 'right' },
    { label: 'Chg%', col: 'chg_pct', align: 'right' },
    { label: 'Vol', col: 'volume', align: 'right' },
    { label: 'RelVol', col: 'rel_vol', align: 'right' },
  ]

  usePublishPageContext({
    eyebrow: 'Console · cross-strategy overview',
    title: 'Market Pulse',
    detail: 'Separates validated ranking from descriptive scanner context.',
    status: [
      { label: 'Session', value: latestDate || '—' },
    ],
  })

  const renderMoversTable = (rows: TickerOverviewRow[]) => (
    <div style={{ maxHeight: MOVERS_HEIGHT, overflowY: 'auto' }}>
      <table style={{ width: '100%', fontSize: '0.84rem', borderCollapse: 'collapse' }}>
        <thead style={{ position: 'sticky', top: 0, background: 'var(--tm-surface)', zIndex: 1 }}>
          <tr style={{ borderBottom: '1px solid var(--tm-line)' }}>
            {MOVER_COLS.map(c => {
              const p = thSort(c.label, c.col, moverSort, setMoverSort, c.align)
              return <th key={c.col} onClick={p.onClick} style={p.style}>{p.children}</th>
            })}
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.ticker} style={{ borderBottom: '1px solid var(--tm-line)' }}>
              <td style={{ padding: '5px 0', fontWeight: 700, cursor: 'pointer', color: 'var(--tm-accent)' }}
                onClick={() => navigate(`/ticker/${r.ticker}`)}>{r.ticker}</td>
              <td style={{ textAlign: 'right', color: 'var(--tm-muted)' }}>${r.close?.toFixed(2)}</td>
              <td style={{ textAlign: 'right' }}>{pctBadge(r.chg_pct)}</td>
              <td style={{ textAlign: 'right', color: 'var(--tm-faint)', fontSize: '0.78rem' }}>
                {r.volume ? r.volume >= 1e6 ? (r.volume / 1e6).toFixed(1) + 'M' : (r.volume / 1e3).toFixed(0) + 'K' : '—'}
              </td>
              <td style={{ textAlign: 'right', color: (r.rel_vol ?? 0) >= 2 ? 'var(--tm-warn)' : 'var(--tm-faint)', fontSize: '0.78rem', fontWeight: (r.rel_vol ?? 0) >= 2 ? 700 : 400 }}>
                {r.rel_vol != null ? r.rel_vol.toFixed(1) + 'x' : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )

  return (
    <div style={{ padding: '4px' }}>
      <CommandBar>
        <span className="tm-commandbar__note">
          {overview.length} tickers tracked{marketRegime && marketRegime.regime !== 'Unknown' ? ` · market ${marketRegime.regime}` : ''}
        </span>
        <CommandSpacer />
        <button type="button" className="tm-commandbar__action" onClick={() => runScan(true)} disabled={loading}>
          {loading ? 'Scanning…' : scanComplete ? 'Rescan all strategies' : 'Run full scan'}
        </button>
      </CommandBar>

      {/* ─── TOP MOVERS ─── */}
      <div style={{ marginBottom: '1.25rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '14px', marginBottom: '8px', flexWrap: 'wrap' }}>
          <span style={{ fontSize: '0.84rem', color: 'var(--tm-ink)', fontWeight: 700 }}>Top Movers</span>
          <label style={{ fontSize: '0.78rem', color: 'var(--tm-muted)', display: 'flex', alignItems: 'center', gap: '4px' }}>
            Show <select value={topN} onChange={e => setTopN(Number(e.target.value))} style={selectStyle}>
              {TOP_N_OPTIONS.map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </label>
          <label style={{ fontSize: '0.78rem', color: 'var(--tm-muted)', display: 'flex', alignItems: 'center', gap: '4px' }}>
            Min Vol <select value={minVol} onChange={e => setMinVol(Number(e.target.value))} style={selectStyle}>
              {MIN_VOL_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </label>
          <label style={{ fontSize: '0.78rem', color: 'var(--tm-muted)', display: 'flex', alignItems: 'center', gap: '4px' }}>
            Min Chg <select value={minChg} onChange={e => setMinChg(Number(e.target.value))} style={selectStyle}>
              {MIN_CHG_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </label>
          <span style={{ fontSize: '0.72rem', color: 'var(--tm-faint)' }}>
            {filteredOverview.length} of {overview.length} tickers
          </span>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '14px' }}>
          <div style={CARD}>
            <h3 style={{ margin: '0 0 8px', fontSize: '0.92rem', color: 'var(--tm-pos)', fontWeight: 700 }}>🟢 Top Gainers</h3>
            {overviewLoading ? <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>Loading...</div>
              : topGainers.length === 0 ? <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>No data</div>
              : renderMoversTable(topGainers)}
          </div>
          <div style={CARD}>
            <h3 style={{ margin: '0 0 8px', fontSize: '0.92rem', color: 'var(--tm-neg)', fontWeight: 700 }}>🔴 Top Losers</h3>
            {overviewLoading ? <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>Loading...</div>
              : topLosers.length === 0 ? <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>No data</div>
              : renderMoversTable(topLosers)}
          </div>
        </div>
      </div>

      {/* ─── LOADING BAR ─── */}
      {loading && (
        <div style={{ textAlign: 'center', padding: '1.5rem', background: 'var(--tm-surface-sunken)', borderRadius: '10px', border: '1px solid var(--tm-line)', marginBottom: '1.25rem' }}>
          <div className="spinner" style={{ margin: '0 auto 10px', width: '28px', height: '28px' }}></div>
          <div style={{ fontSize: '0.9rem', fontWeight: 600, color: 'var(--tm-ink)' }}>Scanning 400 tickers across 5 strategies...</div>
        </div>
      )}

      {/* ─── MARKET HEALTH BANNER ─── */}
      {marketRegime && marketRegime.regime !== 'Unknown' && (() => {
        const rs = REGIME_STYLE[marketRegime.regime] || REGIME_STYLE['Unknown']
        const indices = [marketRegime.spy, marketRegime.qqq].filter(Boolean) as NonNullable<typeof marketRegime.spy>[]
        const TH: React.CSSProperties = { fontSize: '0.62rem', color: 'var(--tm-muted)', fontWeight: 600, textTransform: 'uppercase', padding: '0 6px 3px', whiteSpace: 'nowrap', textAlign: 'center' }
        const TD: React.CSSProperties = { fontSize: '0.78rem', fontWeight: 700, padding: '2px 6px', whiteSpace: 'nowrap', textAlign: 'center' }

        const colColor = (val: number, invert = false) => {
          const positive = invert ? val <= 0 : val >= 0
          return positive ? 'var(--tm-pos)' : 'var(--tm-neg)'
        }

        return (
          <div style={{ ...CARD, marginBottom: '1.25rem', background: rs.bg, border: `1px solid ${rs.color}`, padding: '14px 18px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: '16px', flexWrap: 'wrap' }}>
              {/* Left: Regime badge + description */}
              <div style={{ flex: '0 1 auto', minWidth: '200px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px', flexWrap: 'wrap' }}>
                  <span style={{ fontSize: '1.3rem' }}>{rs.icon}</span>
                  <span style={{ fontSize: '1rem', fontWeight: 800, color: rs.color }}>Market: {marketRegime.regime}</span>
                </div>
                <div style={{ fontSize: '0.8rem', color: rs.color, fontWeight: 500, lineHeight: 1.4, marginBottom: '6px' }}>{marketRegime.description}</div>
                {(marketRegime.caution_buy || marketRegime.caution_sell) && (
                  <div style={{ fontSize: '0.72rem', background: 'var(--tm-warn-soft)', color: 'var(--tm-warn)', padding: '2px 8px', borderRadius: '4px', fontWeight: 700, display: 'inline-block', marginBottom: '4px' }}>
                    ⚠️ {marketRegime.caution_buy ? 'Buy signals may trap' : 'Sell signals may trap'}
                  </div>
                )}
                {marketRegime.divergence && (
                  <div style={{ fontSize: '0.72rem', background: 'var(--tm-alt-soft)', color: 'var(--tm-alt)', padding: '2px 8px', borderRadius: '4px', fontWeight: 700, display: 'inline-block' }}>
                    🔀 {marketRegime.divergence}
                  </div>
                )}
              </div>

              {/* Right: SPY + QQQ table with shared headers */}
              {indices.length > 0 && (
                <div style={{ flex: '1 1 auto', overflowX: 'auto' }}>
                  <table style={{ borderCollapse: 'collapse', width: '100%' }}>
                    <thead>
                      <tr>
                        <th style={{ ...TH, textAlign: 'left' }}>Index</th>
                        <th style={TH}>Price</th>
                        <th style={TH}>RSI</th>
                        <th style={TH}>EMA 9/21</th>
                        <th style={TH}>MACD</th>
                        <th style={TH}>Trend</th>
                        <th style={TH}>vs 200</th>
                        <th style={TH}>20d Chg</th>
                        <th style={TH}>DD</th>
                        <th style={TH}>50/200</th>
                        <th style={TH}>W50</th>
                        <th style={TH}>W200</th>
                      </tr>
                    </thead>
                    <tbody>
                      {indices.map(idx => (
                        <tr key={idx.ticker} style={{ borderTop: '1px solid var(--tm-line)' }}>
                          <td style={{ ...TD, textAlign: 'left', fontWeight: 800, color: rs.color }}>{idx.ticker}</td>
                          <td style={{ ...TD, color: rs.color }}>${idx.price}</td>
                          <td style={{ ...TD, color: idx.rsi < 30 ? 'var(--tm-neg)' : idx.rsi > 70 ? 'var(--tm-warn)' : 'var(--tm-muted)' }}>{idx.rsi}</td>
                          <td style={{ ...TD, color: idx.ema_bullish ? 'var(--tm-pos)' : 'var(--tm-neg)' }}>{idx.ema_bullish ? '▲ Bull' : '▼ Bear'}</td>
                          <td style={{ ...TD, color: colColor(idx.macd_histogram) }}>{idx.macd_histogram > 0 ? '+' : ''}{idx.macd_histogram}</td>
                          <td style={{ ...TD, color: idx.macd_hist_trend === 'Rising' ? 'var(--tm-pos)' : 'var(--tm-neg)' }}>{idx.macd_hist_trend === 'Rising' ? '▲' : '▼'}</td>
                          <td style={{ ...TD, color: colColor(idx.dist_from_200) }}>{idx.dist_from_200 > 0 ? '+' : ''}{idx.dist_from_200}%</td>
                          <td style={{ ...TD, color: colColor(idx.chg_20d) }}>{idx.chg_20d > 0 ? '+' : ''}{idx.chg_20d}%</td>
                          <td style={{ ...TD, color: idx.drawdown_from_52w_high < -10 ? 'var(--tm-neg)' : 'var(--tm-muted)' }}>{idx.drawdown_from_52w_high}%</td>
                          <td style={{ ...TD, color: idx.golden_cross ? 'var(--tm-pos)' : 'var(--tm-neg)' }}>{idx.golden_cross ? 'Golden' : 'Death'}</td>
                          <td style={{ ...TD, color: idx.wsma_50 != null ? (idx.price > idx.wsma_50 ? 'var(--tm-pos)' : 'var(--tm-neg)') : 'var(--tm-faint)' }}>{idx.wsma_50 != null ? `$${idx.wsma_50}` : '—'}</td>
                          <td style={{ ...TD, color: idx.wsma_200 != null ? (idx.price > idx.wsma_200 ? 'var(--tm-pos)' : 'var(--tm-neg)') : 'var(--tm-faint)' }}>{idx.wsma_200 != null ? `$${idx.wsma_200}` : '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            {/* Breadth bar */}
            {breadth && (
              <div style={{ marginTop: '8px', paddingTop: '8px', borderTop: '1px solid var(--tm-line)' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '14px', flexWrap: 'wrap' }}>
                  <span style={{ fontSize: '0.7rem', fontWeight: 700, color: 'var(--tm-ink)' }}>📊 Market Breadth</span>
                  {([
                    ['> 200 SMA', breadth.pct_200, breadth.above_200],
                    ['> 50 SMA', breadth.pct_50, breadth.above_50],
                    ['> 20 SMA', breadth.pct_20, breadth.above_20],
                  ] as [string, number, number][]).map(([label, pct, count]) => (
                    <div key={label} style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                      <span style={{ fontSize: '0.68rem', color: 'var(--tm-muted)', fontWeight: 600 }}>{label}</span>
                      <div style={{ width: '60px', height: '8px', background: 'var(--tm-line)', borderRadius: '4px', overflow: 'hidden' }}>
                        <div style={{ width: `${pct}%`, height: '100%', background: pct >= 60 ? 'var(--tm-pos)' : pct >= 40 ? 'var(--tm-warn)' : 'var(--tm-neg)', transition: 'width 0.5s' }} />
                      </div>
                      <span style={{ fontSize: '0.7rem', fontWeight: 700, color: pct >= 60 ? 'var(--tm-pos)' : pct >= 40 ? 'var(--tm-warn)' : 'var(--tm-neg)' }} title={`${count} of ${breadth.total}`}>{pct}%</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )
      })()}

      {/* ─── ROW 2: Unusual Volume + Scanner Context ─── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '14px', marginBottom: '1.25rem' }}>

        {/* Unusual Volume */}
        <div style={CARD}>
          <h3 style={SECTION_TITLE}>🔥 Unusual Volume <span style={SUBTITLE}>RelVol ≥ 2x</span></h3>
          {overviewLoading ? <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>Loading...</div>
            : highVolMovers.length === 0 ? <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem', padding: '20px 0', textAlign: 'center' }}>No unusual volume detected</div>
            : (
              <div style={{ maxHeight: 340, overflowY: 'auto', flex: 1 }}>
                <table style={{ width: '100%', fontSize: '0.82rem', borderCollapse: 'collapse' }}>
                  <thead style={{ position: 'sticky', top: 0, background: 'var(--tm-surface)' }}>
                    <tr style={{ borderBottom: '1px solid var(--tm-line)' }}>
                      {[{ l: 'Ticker', c: 'ticker', a: 'left' as const }, { l: 'Chg%', c: 'chg_pct', a: 'right' as const }, { l: 'RelVol', c: 'rel_vol', a: 'right' as const }, { l: 'Vol', c: 'volume', a: 'right' as const }].map(h => {
                        const p = thSort(h.l, h.c, volSort, setVolSort, h.a)
                        return <th key={h.c} onClick={p.onClick} style={p.style}>{p.children}</th>
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {[...highVolMovers].sort((a, b) => {
                      const m = volSort.dir === 'asc' ? 1 : -1
                      switch (volSort.col) {
                        case 'ticker': return m * a.ticker.localeCompare(b.ticker)
                        case 'chg_pct': return m * ((a.chg_pct ?? 0) - (b.chg_pct ?? 0))
                        case 'rel_vol': return m * ((a.rel_vol ?? 0) - (b.rel_vol ?? 0))
                        case 'volume': return m * ((a.volume ?? 0) - (b.volume ?? 0))
                        default: return 0
                      }
                    }).map(r => (
                      <tr key={r.ticker} onClick={() => navigate(`/ticker/${r.ticker}`)} style={{ borderBottom: '1px solid var(--tm-line)', cursor: 'pointer' }}>
                        <td style={{ padding: '5px 0', fontWeight: 700, color: 'var(--tm-accent)', minWidth: 50 }}>{r.ticker}</td>
                        <td style={{ textAlign: 'right' }}>{pctBadge(r.chg_pct)}</td>
                        <td style={{ textAlign: 'right', color: 'var(--tm-warn)', fontWeight: 700, fontSize: '0.78rem' }}>{r.rel_vol?.toFixed(1)}x</td>
                        <td style={{ textAlign: 'right', color: 'var(--tm-faint)', fontSize: '0.72rem' }}>
                          {r.volume ? r.volume >= 1e6 ? (r.volume / 1e6).toFixed(1) + 'M' : (r.volume / 1e3).toFixed(0) + 'K' : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
        </div>

        {/* Scanner context */}
        <div style={CARD}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
            <h3 style={{ ...SECTION_TITLE, margin: 0 }}>Scanner Evidence <span style={SUBTITLE}>Shadow outcomes · no recommendations</span></h3>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <button onClick={() => navigate('/stock-research/research')} style={{ ...selectStyle, cursor: 'pointer', color: 'var(--tm-accent)', fontWeight: 700 }}>
                Full evaluation
              </button>
              <select value={scannerInterval} onChange={event => setScannerInterval(event.target.value as ScannerInterval)} style={selectStyle}>
                <option value="1d">Daily</option>
                <option value="1wk">Weekly</option>
                <option value="1h">Hourly</option>
              </select>
            </div>
          </div>
          {scannerSummary.results.length === 0 && scannerEvents.results.length === 0
            ? emptySlate('Collecting shadow events — metrics appear as horizons mature')
            : (
              <>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '8px', marginBottom: '10px' }}>
                  {(() => {
                    const best = [...scannerSummary.results].sort((a, b) => (b.alpha_t_stat ?? -99) - (a.alpha_t_stat ?? -99))[0]
                    const backlog = scannerBacklog.results.filter(row => row.interval === scannerInterval)
                    const pending = backlog.reduce((sum, row) => sum + row.pending, 0)
                    const evaluated = backlog.reduce((sum, row) => sum + row.evaluated, 0)
                    const cards = [
                      ['Events', scannerEvents.results.length.toString()],
                      ['Evaluated', evaluated.toString()],
                      ['Pending', pending.toString()],
                      ['Status', best?.promotion_status ?? 'COLLECTING'],
                    ]
                    return cards.map(([label, value]) => (
                      <div key={label} style={{ background: 'var(--tm-surface-sunken)', padding: '7px', borderRadius: '6px' }}>
                        <div style={{ fontSize: '0.66rem', color: 'var(--tm-faint)' }}>{label}</div>
                        <div style={{ fontSize: '0.82rem', fontWeight: 700, color: 'var(--tm-ink)' }}>{value}</div>
                      </div>
                    ))
                  })()}
                </div>
                {scannerSummary.results.length > 0 && (
                  <table style={{ width: '100%', fontSize: '0.76rem', borderCollapse: 'collapse', marginBottom: '9px' }}>
                    <thead><tr style={{ borderBottom: '1px solid var(--tm-line)' }}>
                      {['Scanner', 'Side', 'Horizon', 'Periods', 'Alpha', 't', 'MAE/MFE'].map(label => (
                        <th key={label} style={{ textAlign: label === 'Scanner' ? 'left' : 'right', padding: '4px 2px', color: 'var(--tm-muted)' }}>{label}</th>
                      ))}
                    </tr></thead>
                    <tbody>{scannerSummary.results.slice(0, 6).map(row => (
                      <tr key={`${row.scanner_name}-${row.scanner_version}-${row.outcome_policy_key}-${row.direction}-${row.horizon_bars}`} style={{ borderBottom: '1px solid var(--tm-line)' }}>
                        <td style={{ padding: '4px 2px' }}>{row.scanner_name.replace(/_/g, ' ')}<div style={{ color: 'var(--tm-faint)', fontSize: '0.64rem' }}>{row.return_mode === 'RECOMMENDATION_PLAN' ? 'Plan' : 'Horizon'}</div></td>
                        <td style={{ textAlign: 'right' }}>{row.direction === 1 ? 'Bull' : 'Bear'}</td>
                        <td style={{ textAlign: 'right' }}>{row.horizon_bars}{row.interval === '1wk' ? ' sessions' : ' bars'}</td>
                        <td style={{ textAlign: 'right' }}>{row.independent_periods}</td>
                        <td style={{ textAlign: 'right' }}>{row.mean_net_alpha != null ? `${(row.mean_net_alpha * 100).toFixed(2)}%` : '—'}</td>
                        <td style={{ textAlign: 'right' }}>{row.alpha_t_stat?.toFixed(2) ?? '—'}</td>
                        <td style={{ textAlign: 'right' }}>{row.mean_mae_pct != null && row.mean_mfe_pct != null ? `${(row.mean_mae_pct * 100).toFixed(1)} / ${(row.mean_mfe_pct * 100).toFixed(1)}%` : '—'}</td>
                      </tr>
                    ))}</tbody>
                  </table>
                )}
                <div style={{ fontSize: '0.7rem', color: 'var(--tm-faint)' }}>
                  Horizon-spaced portfolio observations; promotion remains blocked until sample, alpha and stability gates pass.
                </div>
              </>
            )}
        </div>
      </div>

      {/* ─── VALIDATED CROSS-SECTIONAL SIGNAL ─── */}
      <div style={{ marginBottom: '1.25rem' }}>
        <h3 style={SECTION_TITLE}>
          ✅ Validated Signal
          <span style={SUBTITLE}>
            {xsLong?.results?.[0]
              ? `${xsLong.results[0].model_version} · ${xsLong.trade_date} · ${xsLong.results[0].horizon_days}-day hold · ${xsLong.results[0].universe_size} names`
              : 'Runs after each close'}
          </span>
        </h3>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', margin: '-4px 0 10px' }}>
          <label htmlFor="xs-sector" style={{ fontSize: '0.75rem', color: 'var(--tm-muted)', fontWeight: 600 }}>
            Rank within
          </label>
          <select
            id="xs-sector"
            value={signalSector}
            onChange={e => setSignalSector(e.target.value)}
            style={{
              fontSize: '0.78rem', padding: '4px 8px', borderRadius: '6px',
              border: '1px solid var(--tm-line-strong)', color: 'var(--tm-ink)', background: 'var(--tm-surface-raised)',
            }}
          >
            <option value="">Whole universe</option>
            {sectorList.sectors.map(s => (
              <option key={s.sector} value={s.sector}>{s.sector} ({s.tickers})</option>
            ))}
          </select>
          {signalSector && (
            <span style={{ fontSize: '0.72rem', color: 'var(--tm-warn)' }}>
              Sector ranking diversifies (max sector share 35% → 21%) but historically
              returned ~19% vs ~25% a year.
            </span>
          )}
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '14px' }}>
          {([
            { label: signalSector ? `Top of ${signalSector} — LONG` : 'Top decile — LONG', rows: xsLong?.results ?? [], color: 'var(--tm-pos)' },
            { label: signalSector ? `Bottom of ${signalSector}` : 'Bottom decile — SHORT', rows: xsShortRows, color: 'var(--tm-neg)' },
          ]).map(panel => (            <div key={panel.label} style={CARD}>
              <h4 style={{ margin: '0 0 10px', fontSize: '0.9rem', color: panel.color, fontWeight: 700 }}>
                {panel.label}
              </h4>
              {xsLongLoading ? (
                <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>Loading signal...</div>
              ) : panel.rows.length === 0 ? (
                <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>
                  No signal yet — generated post-close.
                </div>
              ) : (
                <table style={{ width: '100%', fontSize: '0.84rem', borderCollapse: 'collapse' }}>
                  <thead>
                    <tr style={{ borderBottom: `2px solid ${panel.color}` }}>
                      <th style={{ padding: '8px 6px', textAlign: 'left', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem' }}>Ticker</th>
                      <th style={{ padding: '8px 6px', textAlign: 'right', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem' }}>Rank</th>
                      <th style={{ padding: '8px 6px', textAlign: 'right', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem' }}>Score</th>
                    </tr>
                  </thead>
                  <tbody>
                    {panel.rows.map(r => {
                      const rank = signalSector
                        ? r.sector_rank
                        : r.percentile !== null
                          ? Math.round((1 - r.percentile) * (r.universe_size - 1)) + 1
                          : null
                      const total = signalSector ? r.sector_size : r.universe_size
                      return (
                        <tr key={r.ticker} style={{ borderBottom: '1px solid var(--tm-line)' }}>
                          <td
                            onClick={() => navigate(`/ticker/${r.ticker}`)}
                            style={{ padding: '8px 6px', fontWeight: 700, cursor: 'pointer', color: 'var(--tm-accent)' }}
                          >
                            {r.ticker}
                          </td>
                          <td style={{ padding: '8px 6px', textAlign: 'right', color: 'var(--tm-muted)' }}>
                            {rank !== null ? `${rank} / ${total}` : '—'}
                          </td>
                          <td style={{ padding: '8px 6px', textAlign: 'right', fontWeight: 600, color: panel.color }}>
                            {r.neutral_score !== null ? r.neutral_score.toFixed(2) : '—'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              )}
            </div>
          ))}
        </div>
        <div style={{ marginTop: '8px', fontSize: '0.72rem', color: 'var(--tm-muted)' }}>
          Cross-sectional momentum, neutralised against beta / size / volatility / sector.
          Out-of-sample net Sharpe 1.32 vs 1.02 for equal-weight long-only. Monitored, not
          settled — IC t-stat 1.70 over 26 independent periods. The long leg carries the edge
          (+2.00% per 21d, t=2.85); the short leg is not statistically distinguishable from the
          universe (t=−0.22), so treat it as a hedge rather than a conviction short. See{' '}
          <code>docs/SIGNAL_RESEARCH.md</code>.
        </div>
      </div>

      {/* ─── MARKET DISCOVERY STATES ─── */}
      <div style={{ marginBottom: '1.25rem' }}>
        <h3 style={SECTION_TITLE}>
          Market Discovery
          <span style={SUBTITLE}>
            {discovery?.trade_date
              ? `${discovery.trade_date} · ${discovery.results[0]?.model_version ?? 'shadow model'}`
              : 'Runs after each complete close'}
          </span>
        </h3>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center', margin: '-4px 0 10px', flexWrap: 'wrap' }}>
          <label htmlFor="discovery-state" style={{ fontSize: '0.75rem', color: 'var(--tm-muted)', fontWeight: 600 }}>
            Discovery lane
          </label>
          <select
            id="discovery-state"
            value={discoveryState}
            onChange={event => setDiscoveryState(event.target.value as DiscoveryState)}
            style={{
              fontSize: '0.78rem', padding: '4px 8px', borderRadius: '6px',
              border: '1px solid var(--tm-line-strong)', color: 'var(--tm-ink)', background: 'var(--tm-surface-raised)',
            }}
          >
            <option value="CONTINUATION">Continuation ({discovery?.summary.CONTINUATION ?? 0})</option>
            <option value="REVERSAL_CONFIRMED">Confirmed reversal ({discovery?.summary.REVERSAL_CONFIRMED ?? 0})</option>
            <option value="EMERGING_REVERSAL">Emerging reversal ({discovery?.summary.EMERGING_REVERSAL ?? 0})</option>
            <option value="REVERSAL_WATCH">Reversal watch ({discovery?.summary.REVERSAL_WATCH ?? 0})</option>
            <option value="CONFLICT">Conflict ({discovery?.summary.CONFLICT ?? 0})</option>
            <option value="LAGGARD">Laggard ({discovery?.summary.LAGGARD ?? 0})</option>
          </select>
          <span style={{ fontSize: '0.72rem', color: discoveryState === 'CONTINUATION' ? 'var(--tm-pos)' : 'var(--tm-warn)' }}>
            {discoveryState === 'CONTINUATION'
              ? '21-day continuation candidate; monitored alpha, not settled.'
              : 'Discovery only — track outcomes before treating as a recommendation.'}
          </span>
        </div>
        <div style={CARD}>
          {discoveryLoading ? (
            <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>Loading discovery states...</div>
          ) : !discovery?.results.length ? (
            <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>No names currently match this lane.</div>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', fontSize: '0.82rem', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid var(--tm-line-strong)' }}>
                    {['Ticker', 'Sector', '21d move', 'Recent rank', 'Activity', 'Echo rank', 'Current position', 'Status'].map((label, index) => (
                      <th key={label} style={{
                        padding: '8px 6px', textAlign: index < 2 ? 'left' : 'right',
                        color: 'var(--tm-muted)', fontSize: '0.73rem', fontWeight: 700,
                      }}>{label}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {discovery.results.map(row => (
                    <tr key={row.ticker} style={{ borderBottom: '1px solid var(--tm-line)' }}>
                      <td
                        onClick={() => navigate(`/ticker/${row.ticker}`)}
                        style={{ padding: '8px 6px', fontWeight: 700, cursor: 'pointer', color: 'var(--tm-accent)' }}
                      >{row.ticker}</td>
                      <td style={{ padding: '8px 6px', color: 'var(--tm-muted)' }}>{row.sector ?? '—'}</td>
                      <td style={{ padding: '8px 6px', textAlign: 'right', fontWeight: 600 }}>
                        {row.recent_21d_return !== null ? `${(row.recent_21d_return * 100).toFixed(1)}%` : '—'}
                      </td>
                      <td style={{ padding: '8px 6px', textAlign: 'right' }}>
                        {row.recent_21d_percentile !== null ? `${Math.round(row.recent_21d_percentile * 100)}%` : '—'}
                      </td>
                      <td style={{ padding: '8px 6px', textAlign: 'right' }}>
                        {row.activity_percentile !== null ? `${Math.round(row.activity_percentile * 100)}%` : '—'}
                      </td>
                      <td style={{ padding: '8px 6px', textAlign: 'right' }}>
                        {row.echo_percentile !== null ? `${Math.round(row.echo_percentile * 100)}%` : 'N/A'}
                      </td>
                      <td style={{ padding: '8px 6px', textAlign: 'right', minWidth: 150 }} title={row.position_guidance ?? undefined}>
                        <div style={{ fontWeight: 700 }}>{row.trend_state?.replace(/_/g, ' ') ?? '—'}</div>
                        <div style={{ color: row.extension_risk === 'EXHAUSTION_WATCH' ? 'var(--tm-neg)' : row.extension_risk === 'EXTENDED' ? 'var(--tm-warn)' : 'var(--tm-muted)', fontSize: '0.7rem' }}>
                          {row.extension_risk?.replace(/_/g, ' ') ?? 'No overlay'}
                        </div>
                      </td>
                      <td style={{ padding: '8px 6px', textAlign: 'right', color: row.validation_status === 'CANDIDATE_ALPHA' ? 'var(--tm-pos)' : 'var(--tm-warn)', fontWeight: 700 }}>
                        {row.validation_status === 'CANDIDATE_ALPHA' ? 'Candidate alpha' : 'Discovery only'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* ─── DAILY RECOMMENDATIONS ─── */}
      <div style={{ marginBottom: '1.25rem' }}>
        <h3 style={SECTION_TITLE}>
          💡 Daily Recommendations
          <span style={{ ...SUBTITLE, color: 'var(--tm-warn)', fontWeight: 600 }}>
            UNVALIDATED — legacy engine, 44.6% accuracy vs 55.0% for always-long
          </span>
        </h3>
        <div style={{ fontSize: '0.72rem', color: 'var(--tm-faint)', margin: '-6px 0 10px' }}>
          {dailyRecs.used_date ? `Data for ${dailyRecs.used_date}. ` : ''}
          Retained as the historical baseline; not a trading recommendation.
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '14px' }}>
          {/* Bull Recommendations Table */}
          <div style={CARD}>
            <h4 style={{ margin: '0 0 10px', fontSize: '0.9rem', color: 'var(--tm-pos)', fontWeight: 700, display: 'flex', alignItems: 'center', gap: '6px' }}>
              📈 Top Bull Signals ({dailyRecs.total_bull})
            </h4>
            {recsLoading ? (
              <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>Loading recommendations...</div>
            ) : dailyRecs.bull_recommendations.length === 0 ? (
              <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>No bull recommendations available</div>
            ) : (
              <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
                <table style={{ width: '100%', fontSize: '0.84rem', borderCollapse: 'collapse' }}>
                  <thead style={{ position: 'sticky', top: 0, background: 'var(--tm-surface)', zIndex: 1 }}>
                    <tr style={{ borderBottom: '1px solid var(--tm-line)' }}>
                      <th style={{ padding: '8px 6px', textAlign: 'left', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem', borderBottom: '2px solid var(--tm-pos)' }}>Ticker</th>
                      <th style={{ padding: '8px 6px', textAlign: 'right', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem', borderBottom: '2px solid var(--tm-pos)' }}>Rank</th>
                      <th style={{ padding: '8px 6px', textAlign: 'right', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem', borderBottom: '2px solid var(--tm-pos)' }}>Confidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dailyRecs.bull_recommendations.map((rec) => (
                      <tr key={rec.rec_id} style={{ borderBottom: '1px solid var(--tm-line)', backgroundColor: rec.confidence >= 70 ? 'var(--tm-pos-soft)' : 'transparent' }}>
                        <td style={{ padding: '8px 6px', fontWeight: 700, cursor: 'pointer', color: 'var(--tm-accent)' }}>
                          {rec.ticker}
                        </td>
                        <td style={{ padding: '8px 6px', textAlign: 'right', color: 'var(--tm-muted)', fontSize: '0.82rem' }}>
                          #{rec.rank}
                        </td>
                        <td style={{ padding: '8px 6px', textAlign: 'right' }}>
                          <span style={{
                            fontSize: '0.85rem',
                            fontWeight: 700,
                            padding: '3px 8px',
                            background: 'var(--tm-pos)',
                            color: 'var(--tm-on-accent)',
                            borderRadius: '4px'
                          }}>
                            {rec.confidence.toFixed(0)}%
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Bear Recommendations Table */}
          <div style={CARD}>
            <h4 style={{ margin: '0 0 10px', fontSize: '0.9rem', color: 'var(--tm-neg)', fontWeight: 700, display: 'flex', alignItems: 'center', gap: '6px' }}>
              📉 Top Bear Signals ({dailyRecs.total_bear})
            </h4>
            {recsLoading ? (
              <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>Loading recommendations...</div>
            ) : dailyRecs.bear_recommendations.length === 0 ? (
              <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem' }}>No bear recommendations available</div>
            ) : (
              <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
                <table style={{ width: '100%', fontSize: '0.84rem', borderCollapse: 'collapse' }}>
                  <thead style={{ position: 'sticky', top: 0, background: 'var(--tm-surface)', zIndex: 1 }}>
                    <tr style={{ borderBottom: '1px solid var(--tm-line)' }}>
                      <th style={{ padding: '8px 6px', textAlign: 'left', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem', borderBottom: '2px solid var(--tm-neg)' }}>Ticker</th>
                      <th style={{ padding: '8px 6px', textAlign: 'right', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem', borderBottom: '2px solid var(--tm-neg)' }}>Rank</th>
                      <th style={{ padding: '8px 6px', textAlign: 'right', fontWeight: 700, color: 'var(--tm-muted)', fontSize: '0.75rem', borderBottom: '2px solid var(--tm-neg)' }}>Confidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dailyRecs.bear_recommendations.map((rec) => (
                      <tr key={rec.rec_id} style={{ borderBottom: '1px solid var(--tm-line)', backgroundColor: rec.confidence >= 70 ? 'var(--tm-neg-soft)' : 'transparent' }}>
                        <td style={{ padding: '8px 6px', fontWeight: 700, cursor: 'pointer', color: 'var(--tm-accent)' }}>
                          {rec.ticker}
                        </td>
                        <td style={{ padding: '8px 6px', textAlign: 'right', color: 'var(--tm-muted)', fontSize: '0.82rem' }}>
                          #{rec.rank}
                        </td>
                        <td style={{ padding: '8px 6px', textAlign: 'right' }}>
                          <span style={{
                            fontSize: '0.85rem',
                            fontWeight: 700,
                            padding: '3px 8px',
                            background: 'var(--tm-neg)',
                            color: 'var(--tm-on-accent)',
                            borderRadius: '4px'
                          }}>
                            {rec.confidence.toFixed(0)}%
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ─── ROW 3: Scanner Agreement + Fib Sentiment ─── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '14px', marginBottom: '1.25rem' }}>

        {/* Scanner agreement */}
        <div style={CARD}>
          <h3 style={SECTION_TITLE}>Scanner Agreement <span style={SUBTITLE}>2+ descriptive scanners pointing in the same direction</span></h3>
          {!scanComplete ? emptySlate('Run Full Scan to detect confluence') : confluenceTickers.length === 0 ? (
            <div style={{ color: 'var(--tm-faint)', fontSize: '0.84rem', padding: '16px 0', textAlign: 'center' }}>No multi-strategy confluence detected</div>
          ) : (
            <>
              {/* Action Summary Bar */}
              <div style={{ display: 'flex', gap: '10px', marginBottom: '10px', alignItems: 'center', flexWrap: 'wrap' }}>
                {(['STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL'] as const).map(a => (
                  <span key={a} style={{
                    display: 'flex', alignItems: 'center', gap: '4px', fontSize: '0.75rem', fontWeight: 700,
                    color: ACTION_STYLE[a].color,
                  }}>
                    <span style={{
                      width: '8px', height: '8px', borderRadius: '50%', background: ACTION_STYLE[a].color,
                      display: 'inline-block',
                    }} />
                    {actionSummary[a]} {ACTION_STYLE[a].label}
                  </span>
                ))}
                {/* Mini stacked bar */}
                {(() => {
                  const total = confluenceTickers.length || 1
                  const bars: [string, string][] = [
                    ['STRONG_BUY', 'var(--tm-pos)'], ['BUY', 'var(--tm-pos-soft)'], ['HOLD', 'var(--tm-warn)'], ['SELL', 'var(--tm-neg-soft)'], ['STRONG_SELL', 'var(--tm-neg)'],
                  ]
                  return (
                    <div style={{ flex: 1, display: 'flex', height: '8px', borderRadius: '4px', overflow: 'hidden', marginLeft: '4px', minWidth: '80px' }}>
                      {bars.map(([key, bg]) => actionSummary[key] > 0 ? <div key={key} style={{ width: `${(actionSummary[key] / total) * 100}%`, background: bg }} /> : null)}
                    </div>
                  )
                })()}
              </div>
              <div style={{ maxHeight: 300, overflowY: 'auto', flex: 1 }}>
                <table style={{ width: '100%', fontSize: '0.82rem', borderCollapse: 'collapse' }}>
                  <thead style={{ position: 'sticky', top: 0, background: 'var(--tm-surface)' }}>
                    <tr style={{ borderBottom: '1px solid var(--tm-line)' }}>
                      {[{ l: 'Ticker', c: 'ticker', a: 'left' as const }, { l: 'Strategies', c: 'strategies', a: 'center' as const }, { l: 'Action', c: 'action', a: 'center' as const }, { l: '#', c: 'count', a: 'right' as const }].map(h => {
                        const p = thSort(h.l, h.c, confSort, setConfSort, h.a)
                        return <th key={h.c} onClick={p.onClick} style={p.style}>{p.children}</th>
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {[...confluenceTickers].sort((a, b) => {
                      const m = confSort.dir === 'asc' ? 1 : -1
                      switch (confSort.col) {
                        case 'ticker': return m * a.ticker.localeCompare(b.ticker)
                        case 'action': { const o: Record<string, number> = { STRONG_BUY: 0, BUY: 1, HOLD: 2, SELL: 3, STRONG_SELL: 4 }; return m * ((o[a.action] ?? 2) - (o[b.action] ?? 2)) }
                        case 'count': return m * (a.count - b.count)
                        case 'strategies': return m * (a.count - b.count)
                        default: return 0
                      }
                    }).map(t => {
                      const ast = ACTION_STYLE[t.action]
                      return (
                        <tr key={t.ticker} style={{ borderBottom: '1px solid var(--tm-line)', cursor: 'pointer' }} onClick={() => navigate(`/ticker/${t.ticker}`)}>
                          <td style={{ padding: '5px 0', fontWeight: 700, color: 'var(--tm-accent)' }}>{t.ticker}</td>
                          <td style={{ padding: '5px 0', textAlign: 'center' }}>
                            <div style={{ display: 'flex', gap: '3px', justifyContent: 'center', flexWrap: 'wrap' }}>
                              {t.strategies.map(s => (
                                <span key={s} style={{
                                  background: STRAT_COLOR[s] || 'var(--tm-faint)', color: 'var(--tm-on-accent)',
                                  padding: '1px 6px', borderRadius: '4px', fontSize: '0.65rem', fontWeight: 700,
                                }}>{s.length <= 3 ? s : s.charAt(0)}</span>
                              ))}
                            </div>
                          </td>
                          <td style={{ padding: '5px 0', textAlign: 'center' }}>
                            <span
                              title={[t.buys > 0 && `Buy (${t.buys}): ${t.buyStrats.join(', ')}`, t.sells > 0 && `Sell (${t.sells}): ${t.sellStrats.join(', ')}`, t.holdStrats.length > 0 && `Hold (${t.holdStrats.length}): ${t.holdStrats.join(', ')}`].filter(Boolean).join(' | ')}
                              style={{ cursor: 'help' }}
                            >
                              <span style={{
                                padding: '2px 8px', borderRadius: '4px', fontSize: '0.72rem', fontWeight: 700,
                                background: ast.bg, color: ast.color,
                              }}>{ast.label}</span>
                              {getCaution(t.action) && (
                                <span title={getCaution(t.action)!} style={{ marginLeft: '4px', cursor: 'help', fontSize: '0.72rem' }}>⚠️</span>
                              )}
                            </span>
                          </td>
                          <td style={{ padding: '5px 0', textAlign: 'right' }}>
                            <span style={{
                              background: t.count >= 4 ? 'var(--tm-pos)' : t.count >= 3 ? 'var(--tm-warn)' : 'var(--tm-faint)',
                              color: 'var(--tm-on-accent)', padding: '1px 7px', borderRadius: '10px', fontSize: '0.72rem', fontWeight: 700,
                            }}>{t.count}</span>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
              <div style={{ fontSize: '0.68rem', color: 'var(--tm-faint)', marginTop: '6px', borderTop: '1px solid var(--tm-line)', paddingTop: '4px' }}>
                Direction summarizes scanner agreement only; historical combination tests did not demonstrate predictive alpha.
              </div>
            </>
          )}
        </div>

        {/* Fib Sentiment */}
        <div style={CARD}>
          <h3 style={SECTION_TITLE}>🔢 Fib Sentiment</h3>
          {!scanComplete ? emptySlate('Run Full Scan to see sentiment') : (
            <>
              {/* Stacked bar */}
              {(() => {
                const total = Object.values(fibSentiment).reduce((s, v) => s + v, 0) || 1
                const sentColors: Record<string, string> = { Bullish: 'var(--tm-pos)', Bearish: 'var(--tm-neg)', Watch: 'var(--tm-warn)', Neutral: 'var(--tm-line-strong)' }
                return (
                  <div style={{ display: 'flex', height: '18px', borderRadius: '6px', overflow: 'hidden', marginBottom: '12px' }}>
                    {Object.entries(fibSentiment).filter(([,c]) => c > 0).map(([label, count]) => (
                      <div key={label} title={`${label}: ${count}`}
                        style={{ width: `${(count / total) * 100}%`, background: sentColors[label], transition: 'width 0.5s' }} />
                    ))}
                  </div>
                )
              })()}
              {Object.entries(fibSentiment).map(([label, count]) => {
                const colors: Record<string, string> = { Bullish: 'var(--tm-pos)', Bearish: 'var(--tm-neg)', Watch: 'var(--tm-warn)', Neutral: 'var(--tm-faint)' }
                const total = Object.values(fibSentiment).reduce((s, v) => s + v, 0) || 1
                const pct = ((count / total) * 100).toFixed(0)
                return (
                  <div key={label} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', padding: '3px 0' }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                      <span style={{ width: '8px', height: '8px', borderRadius: '2px', background: colors[label], display: 'inline-block' }} />
                      <span style={{ fontWeight: 600, color: colors[label] }}>{label}</span>
                    </span>
                    <span style={{ color: 'var(--tm-muted)', fontWeight: 500 }}>{count} <span style={{ color: 'var(--tm-faint)' }}>({pct}%)</span></span>
                  </div>
                )
              })}
              {/* Fib Watchlist — tickers within 2% of a level */}
              {fibWatchlist.length > 0 && (
                <div style={{ marginTop: '10px', borderTop: '1px solid var(--tm-line)', paddingTop: '8px' }}>
                  <div style={{ fontSize: '0.76rem', fontWeight: 700, color: 'var(--tm-ink)', marginBottom: '6px' }}>🎯 Near Key Levels <span style={{ fontWeight: 400, color: 'var(--tm-faint)' }}>≤ 2% away</span></div>
                  <div style={{ maxHeight: 150, overflowY: 'auto' }}>
                    <table style={{ width: '100%', fontSize: '0.78rem', borderCollapse: 'collapse' }}>
                      <thead>
                        <tr style={{ borderBottom: '1px solid var(--tm-line)' }}>
                          <th style={{ textAlign: 'left', padding: '3px 0', color: 'var(--tm-faint)', fontSize: '0.7rem', fontWeight: 600 }}>Ticker</th>
                          <th style={{ textAlign: 'center', padding: '3px 0', color: 'var(--tm-faint)', fontSize: '0.7rem', fontWeight: 600 }}>Level</th>
                          <th style={{ textAlign: 'right', padding: '3px 0', color: 'var(--tm-faint)', fontSize: '0.7rem', fontWeight: 600 }}>Dist</th>
                          <th style={{ textAlign: 'right', padding: '3px 0', color: 'var(--tm-faint)', fontSize: '0.7rem', fontWeight: 600 }}>Trend</th>
                        </tr>
                      </thead>
                      <tbody>
                        {fibWatchlist.map(f => (
                          <tr key={f.ticker} style={{ borderBottom: '1px solid var(--tm-line)', cursor: 'pointer' }} onClick={() => navigate(`/ticker/${f.ticker}`)}>
                            <td style={{ padding: '4px 0', fontWeight: 700, color: 'var(--tm-accent)' }}>{f.ticker}</td>
                            <td style={{ padding: '4px 0', textAlign: 'center' }}>
                              <span style={{ background: 'var(--tm-alt-soft)', color: 'var(--tm-alt)', padding: '1px 6px', borderRadius: '4px', fontSize: '0.68rem', fontWeight: 600 }}>{f.nearest_level}</span>
                            </td>
                            <td style={{ padding: '4px 0', textAlign: 'right', fontWeight: 600, color: Math.abs(f.distance_pct) <= 0.5 ? 'var(--tm-warn)' : 'var(--tm-muted)' }}>
                              {f.distance_pct > 0 ? '+' : ''}{f.distance_pct.toFixed(1)}%
                            </td>
                            <td style={{ padding: '4px 0', textAlign: 'right' }}>
                              <span style={{ fontSize: '0.68rem', fontWeight: 600, color: f.trend.includes('up') ? 'var(--tm-pos)' : 'var(--tm-neg)' }}>
                                {f.trend.includes('up') ? '▲ Up' : '▼ Down'}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
              <div style={{ fontSize: '0.7rem', color: 'var(--tm-faint)', marginTop: '8px', borderTop: '1px solid var(--tm-line)', paddingTop: '5px' }}>
                Fibonacci retracement proximity & trend direction
              </div>
            </>
          )}
        </div>
      </div>

      {/* ─── QUICK NAV ─── */}
      <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
        {STRAT.map(s => (
          <button key={s.key} onClick={() => navigate(s.path)}
            style={{
              display: 'flex', alignItems: 'center', gap: '6px', padding: '7px 14px',
              background: 'var(--tm-surface)', border: `1px solid ${s.color}`, borderRadius: '7px',
              cursor: 'pointer', fontSize: '0.82rem', fontWeight: 600, color: s.color, transition: 'background 0.15s',
            }}
            onMouseEnter={e => (e.currentTarget.style.background = s.bg)}
            onMouseLeave={e => (e.currentTarget.style.background = 'var(--tm-surface)')}>
            {s.icon} {s.key} →
          </button>
        ))}
        <button onClick={() => navigate('/overview')}
          style={{
            display: 'flex', alignItems: 'center', gap: '6px', padding: '7px 14px',
            background: 'var(--tm-surface)', border: '1px solid var(--tm-line)', borderRadius: '7px',
            cursor: 'pointer', fontSize: '0.82rem', fontWeight: 600, color: 'var(--tm-muted)', transition: 'background 0.15s',
          }}
          onMouseEnter={e => (e.currentTarget.style.background = 'var(--tm-surface-raised)')}
          onMouseLeave={e => (e.currentTarget.style.background = 'var(--tm-surface)')}>
          📋 All Tickers →
        </button>
      </div>
    </div>
  )
}

export default Dashboard
