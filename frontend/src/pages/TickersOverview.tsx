import { useState, useMemo, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Settings } from 'lucide-react'
import { usePublishPageContext } from '../layout/pageContext'
import { useSessionDate } from '../layout/sessionDate'
import { NO_TREND_ADX, momentumTitle, trendLabel, trendTitle } from './setupPresentation'
import {
  getTickersOverview, TickerOverviewRow,
  getSetupSummary, SetupSummaryResponse, SetupSummaryRow, SetupSummaryInterval,
} from '../services/api'

type SetupField =
  | 'trend' | 'momentum' | 'bias' | 'conviction' | 'rsi' | 'stoch_k' | 'adx'
  | 'macd_state' | 'trend_consistency' | 'atr_pct' | 'volume_pressure'
  | 'volume_trend_state' | 'historical_volatility_pct' | 'range_position_pct'
  | 'price_vs_vwap' | 'golden_cross' | 'multi_tf_agree'

type SortField = keyof TickerOverviewRow | SetupField
type SortDir = 'asc' | 'desc'

/** The overview is a daily view, so setups are read from the daily projection. Other
 *  frames stay available on the ticker page. */
const SETUP_INTERVAL: SetupSummaryInterval = '1d'

const SETUP_SORT_FIELDS = new Set<SetupField>([
  'trend', 'momentum', 'bias', 'conviction', 'rsi', 'stoch_k', 'adx', 'macd_state',
  'trend_consistency', 'atr_pct', 'volume_pressure', 'volume_trend_state',
  'historical_volatility_pct', 'range_position_pct', 'price_vs_vwap', 'golden_cross',
  'multi_tf_agree',
])

/** Only Trend and Momentum ship visible; the rest are opt-in so the table still fits. */
const DEFAULT_HIDDEN_COLUMNS = [
  'rel_vol', 'high_52w', 'low_52w',
  'bias', 'conviction', 'multi_tf_agree', 'rsi', 'stoch_k', 'adx', 'macd_state',
  'trend_consistency', 'atr_pct', 'historical_volatility_pct', 'volume_pressure',
  'volume_trend_state', 'range_position_pct', 'price_vs_vwap', 'golden_cross',
]

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
  const { pinned: scanDate } = useSessionDate()
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set(DEFAULT_HIDDEN_COLUMNS))
  const [showColPicker, setShowColPicker] = useState(false)
  const navigate = useNavigate()

  const { data = [], isFetching: loading, error: queryError } = useQuery<TickerOverviewRow[]>({
    queryKey: ['tickers', 'overview', scanDate],
    queryFn: () => getTickersOverview(scanDate || undefined),
  })
  const error = queryError ? (queryError as Error).message : null

  const handleRefresh = useCallback(async () => {
    const key = ['tickers', 'overview', scanDate]
    queryClient.setQueryData(key, undefined)
    queryClient.invalidateQueries({ queryKey: ['setup-summary'] })
    await queryClient.fetchQuery({ queryKey: key, queryFn: () => getTickersOverview(scanDate || undefined, true) })
  }, [scanDate, queryClient])

  // Published setup state for the whole universe; a read, not a scan.
  const { data: setupData, isFetching: setupLoading, isError: setupError } = useQuery<SetupSummaryResponse>({
    queryKey: ['setup-summary', SETUP_INTERVAL],
    queryFn: () => getSetupSummary(SETUP_INTERVAL),
    staleTime: 5 * 60 * 1000,
  })

  const setupMap = useMemo(() => {
    const map: Record<string, SetupSummaryRow> = {}
    setupData?.results.forEach(row => { map[row.ticker] = row })
    return map
  }, [setupData])

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
    // Setup state
    { key: 'setup_bullish_stack', label: 'Bullish EMA stack', group: 'Setup' },
    { key: 'setup_bearish_stack', label: 'Bearish EMA stack', group: 'Setup' },
    { key: 'setup_no_trend', label: `No trend (ADX < ${NO_TREND_ADX})`, group: 'Setup' },
    { key: 'setup_strong_momentum', label: 'Strong up or downtrend', group: 'Setup' },
    { key: 'setup_weakening', label: 'Momentum weakening', group: 'Setup' },
    { key: 'setup_multi_tf', label: 'Multi-timeframe agreement', group: 'Setup' },
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
      case 'setup_bullish_stack':
        return rows.filter(r => setupMap[r.ticker]?.trend?.includes('Bullish'))
      case 'setup_bearish_stack':
        return rows.filter(r => setupMap[r.ticker]?.trend?.includes('Bearish'))
      case 'setup_no_trend':
        return rows.filter(r => {
          const adx = setupMap[r.ticker]?.adx
          return adx != null && adx < NO_TREND_ADX
        })
      case 'setup_strong_momentum':
        return rows.filter(r => setupMap[r.ticker]?.momentum?.startsWith('Strong'))
      case 'setup_weakening':
        return rows.filter(r => setupMap[r.ticker]?.momentum === 'Weakening')
      case 'setup_multi_tf':
        return rows.filter(r => setupMap[r.ticker]?.multi_tf_agree === true)
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
      if (SETUP_SORT_FIELDS.has(sortField as SetupField)) {
        const key = sortField as SetupField
        // Trend sorts on what the cell shows, which folds in the ADX gate.
        const aRaw = key === 'trend' ? trendLabel(setupMap[a.ticker]?.trend, setupMap[a.ticker]?.adx) : setupMap[a.ticker]?.[key]
        const bRaw = key === 'trend' ? trendLabel(setupMap[b.ticker]?.trend, setupMap[b.ticker]?.adx) : setupMap[b.ticker]?.[key]
        if (aRaw == null && bRaw == null) return 0
        if (aRaw == null) return 1
        if (bRaw == null) return -1
        if (typeof aRaw === 'number' && typeof bRaw === 'number') {
          return sortDir === 'asc' ? aRaw - bRaw : bRaw - aRaw
        }
        const cmp = String(aRaw).localeCompare(String(bRaw))
        return sortDir === 'asc' ? cmp : -cmp
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
  }, [data, sortField, sortDir, filter, sectorFilter, maFilterIdx, maThreshold, presetFilter, presetPct, setupMap])

  const fmt = (val: number | null) => (val != null ? val.toFixed(2) : '—')
  const fmtVol = (val: number) => {
    if (val >= 1_000_000) return (val / 1_000_000).toFixed(1) + 'M'
    if (val >= 1_000) return (val / 1_000).toFixed(0) + 'K'
    return val.toString()
  }

  const maColor = (close: number | null, ma: number | null) => {
    if (close == null || ma == null) return undefined
    return close >= ma
      ? { color: 'var(--tm-pos)' } // above MA
      : { color: 'var(--tm-neg)' } // below MA
  }

  const pctColor = (val: number | null) => {
    if (val == null) return undefined
    return val >= 0 ? { color: 'var(--tm-pos)' } : { color: 'var(--tm-neg)' }
  }

  const relVolColor = (val: number | null) => {
    if (val == null) return undefined
    if (val >= 2) return { color: 'var(--tm-alt)', fontWeight: 'bold' as const }
    if (val >= 1.5) return { color: 'var(--tm-accent)' }
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
    { key: 'momentum', label: 'Momentum', group: 'momentum' },
    { key: 'rsi', label: 'RSI', group: 'momentum' },
    { key: 'stoch_k', label: 'Stoch %K', group: 'momentum' },
    { key: 'macd_state', label: 'MACD', group: 'momentum' },
    { key: 'volume_pressure', label: 'Volume pressure', group: 'momentum' },
    { key: 'volume_trend_state', label: 'Volume trend', group: 'momentum' },
    { key: 'trend', label: 'Trend', group: 'setup' },
    { key: 'bias', label: 'Direction', group: 'setup' },
    { key: 'conviction', label: 'Signal tally', group: 'setup' },
    { key: 'multi_tf_agree', label: 'Multi-TF', group: 'setup' },
    { key: 'adx', label: 'ADX', group: 'setup' },
    { key: 'trend_consistency', label: 'Trend consistency', group: 'setup' },
    { key: 'atr_pct', label: 'ATR%', group: 'setup' },
    { key: 'historical_volatility_pct', label: 'Hist. volatility', group: 'setup' },
    { key: 'range_position_pct', label: 'Range position', group: 'setup' },
    { key: 'price_vs_vwap', label: 'vs VWAP', group: 'setup' },
    { key: 'golden_cross', label: '50/200 state', group: 'setup' },
    { key: 'sma_20', label: 'MA 20', group: 'daily_ma' },
    { key: 'sma_50', label: 'MA 50', group: 'daily_ma' },
    { key: 'sma_200', label: 'MA 200', group: 'daily_ma' },
    { key: 'dist_200', label: '% from 200', group: 'daily_ma' },
    { key: 'wsma_50', label: '50W MA', group: 'weekly_ma' },
    { key: 'wsma_200', label: '200W MA', group: 'weekly_ma' },
    { key: 'dist_200w', label: '% from 200W', group: 'weekly_ma' },
  ]

  const columnGroups = [
    { key: 'price', label: 'Price', bg: 'var(--tm-surface-raised)', fg: 'var(--tm-ink)', tip: undefined as string | undefined },
    { key: 'momentum', label: 'Momentum', bg: 'var(--tm-info-soft)', fg: 'var(--tm-info)', tip: 'Rel Vol is highlighted at ≥1.5x and bold at ≥2x.' },
    { key: 'setup', label: 'Trade setup', bg: 'var(--tm-warn-soft)', fg: 'var(--tm-warn)', tip: undefined },
    { key: 'daily_ma', label: 'Daily MAs', bg: 'var(--tm-accent-soft)', fg: 'var(--tm-accent)', tip: 'Green when the close is above the average, red when below.' },
    { key: 'weekly_ma', label: 'Weekly MAs', bg: 'var(--tm-alt-soft)', fg: 'var(--tm-alt)', tip: 'Green when the close is above the average, red when below.' },
  ]

  const visibleColumns = columns.filter(c => !hiddenCols.has(c.key))

  usePublishPageContext({
    eyebrow: 'Market · full universe',
    title: 'All Tickers',
    detail: 'Price, momentum, key moving averages, and published trade-setup state for every tracked ticker.',
    session: SETUP_INTERVAL,
  })

  const toneFor = (value: string | null | undefined) => {
    if (!value) return undefined
    const text = value.toLowerCase()
    // "Short-term X" means the stack is not aligned, so it gets no directional colour.
    if (text.startsWith('short-term')) return undefined
    if (/bull|uptrend|recovery|accumulation|above|expanding|rising/.test(text)) return 'var(--tm-pos)'
    if (/bear|downtrend|weakening|distribution|below|contracting|falling/.test(text)) return 'var(--tm-neg)'
    return undefined
  }

  /** Descriptive setup state, rendered plainly so it does not read as a recommendation. */
  const setupText = (row: TickerOverviewRow, key: SetupField, title?: string | null) => {
    const value = setupMap[row.ticker]?.[key]
    if (value === null || value === undefined || value === '') {
      return { content: <span style={{ color: 'var(--tm-faint)' }}>—</span>, style: { textAlign: 'center' as const } }
    }
    const text = typeof value === 'boolean' ? (value ? 'Agree' : 'Diverge') : String(value)
    return {
      content: <span title={title ?? undefined} style={{ color: toneFor(text) }}>{text}</span>,
      style: { textAlign: 'left' as const, whiteSpace: 'nowrap' as const },
    }
  }

  const setupNumber = (row: TickerOverviewRow, key: SetupField, digits = 1, suffix = '') => {
    const value = setupMap[row.ticker]?.[key]
    if (typeof value !== 'number') {
      return { content: <span style={{ color: 'var(--tm-faint)' }}>—</span>, style: { textAlign: 'right' as const } }
    }
    return { content: `${value.toFixed(digits)}${suffix}`, style: { textAlign: 'right' as const } }
  }

  const renderCell = (key: string, row: TickerOverviewRow): { content: React.ReactNode; style?: React.CSSProperties } => {
    switch (key) {
      case 'ticker': return { content: row.ticker, style: { fontWeight: 'bold', color: 'var(--tm-accent)', textAlign: 'left' } }
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
      case 'trend': {
        const entry = setupMap[row.ticker]
        const label = trendLabel(entry?.trend, entry?.adx)
        if (!label) {
          return { content: <span style={{ color: 'var(--tm-faint)' }}>—</span>, style: { textAlign: 'center' } }
        }
        return {
          content: (
            <span title={trendTitle(entry?.trend, entry?.trend_detail, entry?.adx)} style={{ color: toneFor(label) }}>
              {label}
            </span>
          ),
          style: { textAlign: 'left', whiteSpace: 'nowrap' },
        }
      }
      case 'momentum': return setupText(
        row,
        'momentum',
        momentumTitle(setupMap[row.ticker]?.momentum, setupMap[row.ticker]?.momentum_detail),
      )
      case 'bias': return setupText(row, 'bias', 'Majority of the setup\u2019s weighted signals, not a trend measure or a recommendation.')
      case 'conviction': {
        const entry = setupMap[row.ticker]
        if (!entry || entry.bull_signals == null || entry.bear_signals == null) {
          return { content: <span style={{ color: 'var(--tm-faint)' }}>—</span>, style: { textAlign: 'right' } }
        }
        return {
          content: (
            <span title={`${entry.bull_signals} bullish vs ${entry.bear_signals} bearish signals (${entry.conviction ?? 'n/a'} agreement). A count, not a probability.`}>
              {entry.bull_signals}:{entry.bear_signals}
            </span>
          ),
          style: { textAlign: 'right', whiteSpace: 'nowrap' },
        }
      }
      case 'multi_tf_agree': return setupText(row, 'multi_tf_agree', `Confirmed on ${setupMap[row.ticker]?.confirm_interval ?? 'the confirm interval'}`)
      case 'rsi': return setupNumber(row, 'rsi')
      case 'stoch_k': return setupNumber(row, 'stoch_k')
      case 'adx': return setupNumber(row, 'adx')
      case 'macd_state': return setupText(row, 'macd_state')
      case 'trend_consistency': return setupNumber(row, 'trend_consistency', 0, '%')
      case 'atr_pct': return setupNumber(row, 'atr_pct', 2, '%')
      case 'historical_volatility_pct': return setupNumber(row, 'historical_volatility_pct', 1, '%')
      case 'volume_pressure': return setupText(row, 'volume_pressure')
      case 'volume_trend_state': return setupText(row, 'volume_trend_state')
      case 'range_position_pct': return setupNumber(row, 'range_position_pct', 0, '%')
      case 'price_vs_vwap': return setupText(row, 'price_vs_vwap')
      case 'golden_cross': return setupText(row, 'golden_cross')
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
      <div style={{ marginBottom: '16px', display: 'flex', gap: '12px', alignItems: 'center', flexWrap: 'wrap' }}>
        <input
          type="text"
          placeholder="Filter by ticker..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{
            padding: '8px 12px',
            borderRadius: '4px',
            border: '1px solid var(--tm-line-strong)',
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
            border: '1px solid var(--tm-line-strong)',
            fontSize: '14px',
            background: sectorFilter ? 'var(--tm-accent-soft)' : 'var(--tm-surface-raised)',
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
            border: '1px solid var(--tm-line-strong)',
            fontSize: '14px',
            background: presetFilter ? 'var(--tm-accent-soft)' : 'var(--tm-surface-raised)',
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
            <label style={{ fontSize: '13px', color: 'var(--tm-muted)' }}>
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
                border: '1px solid var(--tm-line-strong)',
                width: '60px',
                fontSize: '14px',
                textAlign: 'center',
              }}
            />
            <span style={{ fontSize: '13px', color: 'var(--tm-muted)' }}>
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
            border: '1px solid var(--tm-line-strong)',
            fontSize: '14px',
            background: maFilterIdx > 0 ? 'var(--tm-accent-soft)' : 'var(--tm-surface-raised)',
            fontWeight: maFilterIdx > 0 ? 600 : 400,
          }}
        >
          {maFilterOptions.map((opt, idx) => (
            <option key={idx} value={idx}>{opt.label}</option>
          ))}
        </select>
        {maFilterOptions[maFilterIdx]?.type === 'proximity' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            <label style={{ fontSize: '13px', color: 'var(--tm-muted)' }}>within</label>
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
                border: '1px solid var(--tm-line-strong)',
                width: '60px',
                fontSize: '14px',
                textAlign: 'center',
              }}
            />
            <span style={{ fontSize: '13px', color: 'var(--tm-muted)' }}>%</span>
          </div>
        )}
        <span style={{ color: 'var(--tm-faint)', fontSize: '13px', whiteSpace: 'nowrap' }}>
          {sorted.length} of {data.length}
          {' · '}
          <span style={{ color: setupError ? 'var(--tm-neg)' : undefined }}>
            {setupLoading
              ? 'loading setups…'
              : setupError
                ? 'setups unavailable'
                : setupData
                  ? `${setupData.count} setups${setupData.is_fresh ? '' : ' · stale'}`
                  : '—'}
          </span>
        </span>

        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexShrink: 0, marginLeft: 'auto' }}>
          <button type="button" className="tm-commandbar__action" onClick={handleRefresh} disabled={loading}>
            {loading ? 'Loading…' : 'Refresh'}
          </button>
          <div style={{ position: 'relative' }}>
          <button
            onClick={() => setShowColPicker(!showColPicker)}
            aria-label={`Choose columns${hiddenCols.size > 0 ? ` (${hiddenCols.size} hidden)` : ''}`}
            title={`Choose columns${hiddenCols.size > 0 ? ` (${hiddenCols.size} hidden)` : ''}`}
            style={{
              width: '32px', height: '32px', padding: 0, borderRadius: '4px', border: '1px solid var(--tm-line-strong)',
              background: showColPicker ? 'var(--tm-accent-soft)' : 'var(--tm-surface-raised)', cursor: 'pointer', fontSize: '17px',
              display: 'grid', placeItems: 'center', lineHeight: 1,
            }}
          >
            <Settings size={18} strokeWidth={2} aria-hidden="true" />
          </button>
          {showColPicker && (
            <div style={{
              position: 'absolute', right: 0, top: '100%', zIndex: 50, marginTop: 4,
              background: 'var(--tm-float)', border: '1px solid var(--tm-line)', borderRadius: '6px',
              padding: '12px', boxShadow: '0 4px 12px rgba(0,0,0,0.15)', minWidth: '240px',
            }}>
              {columnGroups.map(g => (
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
      </div>

      {loading && <p>Loading overview for all selected tickers...</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {!loading && !error && data.length > 0 && (
        <div className="tm-fit">
          <table
            style={{
              width: '100%',
              borderCollapse: 'collapse',
              fontSize: '11.5px',
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
                    <th key={g.key} colSpan={count} title={g.tip} style={{
                      background: g.bg, color: g.fg, padding: '6px 8px', textAlign: 'center',
                      borderRight: !isLast ? '2px solid var(--tm-line-strong)' : undefined,
                      cursor: g.tip ? 'help' : undefined,
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
                        background: 'var(--tm-surface-sunken)',
                        borderBottom: '2px solid var(--tm-line-strong)',
                        cursor: 'pointer',
                        textAlign: col.key === 'ticker' ? 'left' : col.group === 'setup' ? 'left' : 'right',
                        userSelect: 'none',
                        borderRight: isGroupEnd ? '2px solid var(--tm-line-strong)' : undefined,
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
                    background: i % 2 === 0 ? 'var(--tm-surface)' : 'var(--tm-surface-raised)',
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
                        textAlign: col.key === 'ticker' ? 'left' : col.group === 'setup' ? 'left' : 'right',
                        ...cellStyle,
                        ...(isGroupEnd ? { borderRight: '2px solid var(--tm-line)' } : {}),
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
    </>
  )
}

export default TickersOverview
