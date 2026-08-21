import { useState, useMemo, useCallback, ReactNode } from 'react'
import { useNavigate } from 'react-router-dom'
import { scanStreak, StreakResponse, StreakResult } from '../services/api'

interface StreakPanelProps {
  strategy?: string
  shortPeriod?: number
  longPeriod?: number
}

type GapTab = 'overview' | 'freshness' | 'fill' | 'newgaps' | 'transitions' | 'volume'
type MaTab = 'overview' | 'direction' | 'spread' | 'momentum' | 'signals' | 'volume'

const GAP_TABS: { key: GapTab; label: string; icon: string }[] = [
  { key: 'overview', label: 'Overview', icon: '📊' },
  { key: 'freshness', label: 'Freshness', icon: '🕐' },
  { key: 'fill', label: 'Fill Progress', icon: '📈' },
  { key: 'newgaps', label: 'New Gaps', icon: '⚡' },
  { key: 'transitions', label: 'Transitions', icon: '🔄' },
  { key: 'volume', label: 'Volume', icon: '📶' },
]

const MA_TABS: { key: MaTab; label: string; icon: string }[] = [
  { key: 'overview', label: 'Overview', icon: '📊' },
  { key: 'direction', label: 'Direction', icon: '🧭' },
  { key: 'spread', label: 'Spread', icon: '📏' },
  { key: 'momentum', label: 'Momentum', icon: '🚀' },
  { key: 'signals', label: 'Signals', icon: '🔀' },
  { key: 'volume', label: 'Volume', icon: '📶' },
]

type FibTab = 'overview' | 'levels' | 'proximity' | 'depth' | 'signals' | 'volume'

const FIB_TABS: { key: FibTab; label: string; icon: string }[] = [
  { key: 'overview', label: 'Overview', icon: '📊' },
  { key: 'levels', label: 'Levels', icon: '📐' },
  { key: 'proximity', label: 'Proximity', icon: '🎯' },
  { key: 'depth', label: 'Depth', icon: '📉' },
  { key: 'signals', label: 'Signals', icon: '🔀' },
  { key: 'volume', label: 'Volume', icon: '📶' },
]

// ── Collapsible Section ──
function CollapsibleSection({ title, count, hint, defaultOpen = true, children }: {
  title: string; count: number; hint?: string; defaultOpen?: boolean; children: ReactNode
}) {
  const [expanded, setExpanded] = useState(defaultOpen)
  return (
    <div style={{ marginBottom: '2px' }}>
      <div
        onClick={() => setExpanded(e => !e)}
        style={{
          padding: '8px 12px', background: '#f8fafc', borderBottom: '1px solid var(--border-color)',
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          cursor: 'pointer', userSelect: 'none',
        }}
      >
        <span style={{ fontWeight: 700, fontSize: '0.85rem', display: 'flex', alignItems: 'center', gap: '6px' }}>
          <span style={{ display: 'inline-block', transition: 'transform 0.2s', transform: expanded ? 'rotate(90deg)' : 'rotate(0deg)', fontSize: '0.7rem' }}>▶</span>
          {title} ({count})
        </span>
        {hint && <span style={{ fontSize: '0.72rem', color: 'var(--text-secondary)' }}>{hint}</span>}
      </div>
      {expanded && children}
    </div>
  )
}

// ── Sortable Table ──
type SortDir = 'asc' | 'desc'
interface ColDef<T> {
  key: string
  label: string
  align?: 'left' | 'center'
  sortVal?: (row: T) => number | string
  render: (row: T) => ReactNode
}

function SortableTable<T extends { ticker: string }>({ columns, rows, defaultSortKey }: {
  columns: ColDef<T>[]; rows: T[]; defaultSortKey?: string
}) {
  const [sortKey, setSortKey] = useState(defaultSortKey || '')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const handleSort = useCallback((key: string) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }, [sortKey])

  const sorted = useMemo(() => {
    if (!sortKey) return rows
    const col = columns.find(c => c.key === sortKey)
    if (!col?.sortVal) return rows
    const getter = col.sortVal
    return [...rows].sort((a, b) => {
      const va = getter(a)
      const vb = getter(b)
      if (typeof va === 'number' && typeof vb === 'number') return sortDir === 'asc' ? va - vb : vb - va
      const sa = String(va), sb = String(vb)
      return sortDir === 'asc' ? sa.localeCompare(sb) : sb.localeCompare(sa)
    })
  }, [rows, sortKey, sortDir, columns])

  if (rows.length === 0) {
    return <div style={{ padding: '12px', color: 'var(--text-secondary)', fontSize: '0.8rem', textAlign: 'center' }}>None</div>
  }

  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
      <thead>
        <tr style={{ borderBottom: '1px solid var(--border-color)', background: '#fafafa' }}>
          {columns.map(c => (
            <th
              key={c.key}
              onClick={c.sortVal ? () => handleSort(c.key) : undefined}
              style={{
                textAlign: c.align || 'center', padding: '6px 10px', fontWeight: 600,
                cursor: c.sortVal ? 'pointer' : 'default', userSelect: 'none', whiteSpace: 'nowrap',
              }}
            >
              {c.label}
              {c.sortVal && (
                <span style={{ marginLeft: 3, fontSize: '0.65rem', color: sortKey === c.key ? 'var(--primary-color)' : '#cbd5e1' }}>
                  {sortKey === c.key ? (sortDir === 'asc' ? '▲' : '▼') : '⇅'}
                </span>
              )}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {sorted.map(row => (
          <tr key={row.ticker} style={{ borderBottom: '1px solid var(--border-color)' }}>
            {columns.map(c => (
              <td key={c.key} style={{ textAlign: c.align || 'center', padding: '5px 10px' }}>
                {c.render(row)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

// -- Badge helpers --
const freshnessBadge = (f: string) => {
  const map: Record<string, { bg: string; fg: string }> = {
    Fresh: { bg: '#dcfce7', fg: '#15803d' },
    Aging: { bg: '#fef3c7', fg: '#92400e' },
    Stale: { bg: '#fee2e2', fg: '#991b1b' },
  }
  const c = map[f] || { bg: '#f1f5f9', fg: '#475569' }
  return (
    <span style={{ padding: '1px 8px', borderRadius: 10, fontWeight: 600, fontSize: '0.76rem', background: c.bg, color: c.fg }}>
      {f}
    </span>
  )
}

const fillBadge = (f: string) => {
  const map: Record<string, { bg: string; fg: string }> = {
    Converging: { bg: '#dcfce7', fg: '#15803d' },
    Stable: { bg: '#fef3c7', fg: '#92400e' },
    Diverging: { bg: '#fee2e2', fg: '#991b1b' },
  }
  const c = map[f] || { bg: '#f1f5f9', fg: '#475569' }
  return (
    <span style={{ padding: '1px 8px', borderRadius: 10, fontWeight: 600, fontSize: '0.76rem', background: c.bg, color: c.fg }}>
      {f}
    </span>
  )
}

const volBadge = (v: number | null) => {
  if (v == null) return <span style={{ color: 'var(--text-secondary)', fontSize: '0.78rem' }}>—</span>
  const bg = v >= 1.5 ? '#dcfce7' : v >= 1.0 ? '#fef3c7' : '#fee2e2'
  const fg = v >= 1.5 ? '#15803d' : v >= 1.0 ? '#92400e' : '#991b1b'
  return (
    <span style={{ padding: '1px 8px', borderRadius: 10, fontWeight: 600, fontSize: '0.76rem', background: bg, color: fg }}>
      {v}x
    </span>
  )
}

// -- MA analysis badge helpers --
const directionBadge = (d: string) => {
  const map: Record<string, { bg: string; fg: string }> = {
    Bullish: { bg: '#dcfce7', fg: '#15803d' },
    Bearish: { bg: '#fee2e2', fg: '#991b1b' },
    Mixed: { bg: '#fef3c7', fg: '#92400e' },
  }
  const c = map[d] || { bg: '#f1f5f9', fg: '#475569' }
  return <span style={{ padding: '1px 8px', borderRadius: 10, fontWeight: 600, fontSize: '0.76rem', background: c.bg, color: c.fg }}>{d}</span>
}

const spreadBadge = (s: string) => {
  const map: Record<string, { bg: string; fg: string }> = {
    Widening: { bg: '#dcfce7', fg: '#15803d' },
    Stable: { bg: '#fef3c7', fg: '#92400e' },
    Narrowing: { bg: '#fee2e2', fg: '#991b1b' },
  }
  const c = map[s] || { bg: '#f1f5f9', fg: '#475569' }
  return <span style={{ padding: '1px 8px', borderRadius: 10, fontWeight: 600, fontSize: '0.76rem', background: c.bg, color: c.fg }}>{s}</span>
}

const momentumBadge = (m: string) => {
  const map: Record<string, { bg: string; fg: string }> = {
    Accelerating: { bg: '#dcfce7', fg: '#15803d' },
    Steady: { bg: '#dbeafe', fg: '#1d4ed8' },
    Stalling: { bg: '#fef3c7', fg: '#92400e' },
    Choppy: { bg: '#fee2e2', fg: '#991b1b' },
  }
  const c = map[m] || { bg: '#f1f5f9', fg: '#475569' }
  return <span style={{ padding: '1px 8px', borderRadius: 10, fontWeight: 600, fontSize: '0.76rem', background: c.bg, color: c.fg }}>{m}</span>
}

const weeklyAlignBadge = (a: string) => {
  const map: Record<string, { bg: string; color: string }> = {
    'Confirmed Bullish': { bg: '#dcfce7', color: '#15803d' },
    'Confirmed Bearish': { bg: '#fce4ec', color: '#991b1b' },
    'Counter-trend Bullish': { bg: '#fff7ed', color: '#c2410c' },
    'Counter-trend Bearish': { bg: '#eff6ff', color: '#1d4ed8' },
    'Mixed': { bg: '#fef9c3', color: '#854d0e' },
    'Neutral': { bg: '#f3f4f6', color: '#6b7280' },
    'N/A': { bg: '#f3f4f6', color: '#9ca3af' },
  }
  const style = map[a] || map['N/A']
  return <span style={{ display: 'inline-block', padding: '2px 7px', borderRadius: 4, fontSize: '0.72rem', fontWeight: 600, background: style.bg, color: style.color, whiteSpace: 'nowrap' }}>{a}</span>
}

const weeklySignalBadge = (s: string | null | undefined) => {
  if (!s) return <span style={{ color: '#9ca3af', fontSize: '0.78rem' }}>—</span>
  const bull = s.includes('Bullish') || s === 'W-Above'
  const cross = s.includes('Cross')
  const color = bull ? '#15803d' : '#991b1b'
  const bg = cross ? (bull ? '#dcfce7' : '#fce4ec') : 'transparent'
  return <span style={{ display: 'inline-block', padding: '2px 7px', borderRadius: 4, fontSize: '0.72rem', fontWeight: 600, background: bg, color, border: cross ? 'none' : `1.5px solid ${color}`, whiteSpace: 'nowrap' }}>{s}</span>
}

const signalBadge = (s: string) => {
  const isBullish = s.includes('Bullish')
  return (
    <span style={{
      padding: '1px 5px', borderRadius: 4, fontSize: '0.72rem', fontWeight: 600,
      background: isBullish ? '#dcfce7' : '#fee2e2',
      color: isBullish ? '#15803d' : '#991b1b',
    }}>{s.replace('Bullish ', '↑ ').replace('Bearish ', '↓ ').replace('Recent ', 'R.')}</span>
  )
}

// -- Mini spark chart for fill distances --
function MiniSpark({ values }: { values: number[] }) {
  if (values.length < 2) return <span style={{ color: 'var(--text-secondary)', fontSize: '0.75rem' }}>—</span>
  const absVals = values.map(Math.abs)
  const max = Math.max(...absVals, 0.01)
  const w = 60
  const h = 20
  const points = absVals.map((v, i) => `${(i / (absVals.length - 1)) * w},${h - (v / max) * h}`).join(' ')
  const trending = absVals[absVals.length - 1] < absVals[0] * 0.8
  return (
    <svg width={w} height={h} style={{ verticalAlign: 'middle' }}>
      <polyline points={points} fill="none" stroke={trending ? '#22c55e' : '#ef4444'} strokeWidth="1.5" />
    </svg>
  )
}

// -- Clickable ticker helper --
function TickerLink({ ticker, navigate }: { ticker: string; navigate: ReturnType<typeof useNavigate> }) {
  return (
    <span
      style={{ cursor: 'pointer', color: 'var(--primary-color)', fontWeight: 600, fontSize: '0.82rem' }}
      onClick={() => navigate(`/ticker/${ticker}`)}
    >
      {ticker}
    </span>
  )
}

const STRATEGY_OPTIONS = [
  { value: 'gaps', label: 'Gap Strategies' },
  { value: 'ma-crossover', label: 'MA Crossover' },
  { value: 'momentum-pullback', label: 'Momentum Pullback' },
  { value: 'bearish-bounce', label: 'Bearish Bounce' },
  { value: 'fibonacci', label: 'Fibonacci' },
]

function StreakPanel({ strategy: fixedStrategy, shortPeriod, longPeriod }: StreakPanelProps) {
  const navigate = useNavigate()
  const [days, setDays] = useState(5)
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<StreakResponse | null>(null)
  const [open, setOpen] = useState(false)
  const [maximized, setMaximized] = useState(false)
  const [perfectOnly, setPerfectOnly] = useState(false)
  const [filter, setFilter] = useState('')
  const [gapTab, setGapTab] = useState<GapTab>('overview')
  const [maTab, setMaTab] = useState<MaTab>('overview')
  const [fibTab, setFibTab] = useState<FibTab>('overview')
  const [selectedStrategy, setSelectedStrategy] = useState(fixedStrategy || 'gaps')
  const [localShort, setLocalShort] = useState(shortPeriod ?? 9)
  const [localLong, setLocalLong] = useState(longPeriod ?? 21)

  const canSelectStrategy = !fixedStrategy
  const strategy = fixedStrategy || selectedStrategy
  const isGaps = strategy === 'gaps'
  const isMa = strategy === 'ma-crossover'
  const isFib = strategy === 'fibonacci'

  const runStreak = async () => {
    setLoading(true)
    try {
      const sp = strategy === 'ma-crossover' ? (fixedStrategy ? shortPeriod : localShort) : undefined
      const lp = strategy === 'ma-crossover' ? (fixedStrategy ? longPeriod : localLong) : undefined
      const result = await scanStreak(strategy, days, sp, lp)
      setData(result)
    } catch (err) {
      console.error('Streak scan failed:', err)
    }
    setLoading(false)
  }

  const filtered = useMemo(() => {
    if (!data?.results) return []
    let items = data.results
    if (perfectOnly) items = items.filter(r => r.days_matched === r.total_days)
    if (filter) items = items.filter(r => r.ticker.toLowerCase().includes(filter.toLowerCase()))
    return items
  }, [data, perfectOnly, filter])

  const perfectCount = useMemo(() => {
    if (!data?.results) return 0
    return data.results.filter(r => r.days_matched === r.total_days).length
  }, [data])

  // Gap-specific filtered lists
  const gapFiltered = useMemo(() => {
    return filtered.filter(r => r.gap_analysis)
  }, [filtered])

  // MA-specific filtered lists
  const maFiltered = useMemo(() => {
    return filtered.filter(r => r.ma_analysis)
  }, [filtered])

  // Fib-specific filtered lists
  const fibFiltered = useMemo(() => {
    return filtered.filter(r => r.fib_analysis)
  }, [filtered])

  const DRAWER_WIDTH = maximized ? 800 : 540

  // -- Tab renderers for gap analysis --
  const renderOverviewTab = () => (
    <>
      {filtered.length === 0 ? (
        <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
          No tickers matched{perfectOnly ? ' on all days' : ''}.
          {perfectOnly && <><br />Try unchecking "Perfect only".</>}
        </div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
          <thead>
            <tr style={{ borderBottom: '2px solid var(--border-color)', background: '#f8fafc', position: 'sticky', top: 0, zIndex: 1 }}>
              <th style={{ textAlign: 'left', padding: '7px 10px', fontWeight: 600 }}>Ticker</th>
              <th style={{ textAlign: 'center', padding: '7px 4px', fontWeight: 600, fontSize: '0.75rem' }}>Freq</th>
              {data!.scan_dates.map((d) => (
                <th key={d} style={{ textAlign: 'center', padding: '7px 2px', fontWeight: 500, fontSize: '0.7rem', whiteSpace: 'nowrap' }}>
                  {d.slice(5)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((r) => (
              <tr key={r.ticker} style={{ borderBottom: '1px solid var(--border-color)', background: r.days_matched === r.total_days ? '#f0fdf4' : 'transparent' }}>
                <td style={{ padding: '5px 10px' }}><TickerLink ticker={r.ticker} navigate={navigate} /></td>
                <td style={{ textAlign: 'center', padding: '5px 4px' }}>
                  <span style={{
                    display: 'inline-block', padding: '1px 7px', borderRadius: '10px', fontWeight: 700, fontSize: '0.78rem',
                    background: r.days_matched === r.total_days ? '#dcfce7' : r.consistency >= 60 ? '#fef3c7' : '#fee2e2',
                    color: r.days_matched === r.total_days ? '#15803d' : r.consistency >= 60 ? '#92400e' : '#991b1b',
                  }}>
                    {r.days_matched}/{r.total_days}
                  </span>
                </td>
                {data!.scan_dates.map((d) => (
                  <td key={d} style={{ textAlign: 'center', padding: '5px 2px' }}>
                    <span style={{
                      display: 'inline-block', width: '9px', height: '9px', borderRadius: '50%',
                      background: r.dates_matched.includes(d) ? '#22c55e' : '#e5e7eb',
                      boxShadow: r.dates_matched.includes(d) ? '0 0 3px rgba(34,197,94,0.4)' : 'none',
                    }} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {/* Legend */}
      <div style={{ padding: '10px 12px', fontSize: '0.73rem', color: 'var(--text-secondary)', display: 'flex', gap: '12px', borderTop: '1px solid var(--border-color)' }}>
        <span><span style={{ display: 'inline-block', width: 7, height: 7, borderRadius: '50%', background: '#22c55e', marginRight: 3, verticalAlign: 'middle' }} />Signal</span>
        <span><span style={{ display: 'inline-block', width: 7, height: 7, borderRadius: '50%', background: '#e5e7eb', marginRight: 3, verticalAlign: 'middle' }} />None</span>
        <span><span style={{ display: 'inline-block', padding: '0 5px', borderRadius: 6, background: '#dcfce7', color: '#15803d', fontWeight: 600, fontSize: '0.68rem', marginRight: 3 }}>N/N</span>Perfect</span>
      </div>
    </>
  )

  const renderFreshnessTab = () => {
    const fresh = gapFiltered.filter(r => r.gap_analysis!.freshness === 'Fresh')
    const aging = gapFiltered.filter(r => r.gap_analysis!.freshness === 'Aging')
    const stale = gapFiltered.filter(r => r.gap_analysis!.freshness === 'Stale')
    const cols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'age', label: 'Age', sortVal: r => r.gap_analysis!.freshest_gap_age ?? 999, render: r => <span style={{ fontWeight: 600, fontSize: '0.82rem' }}>{r.gap_analysis!.freshest_gap_age != null ? `${r.gap_analysis!.freshest_gap_age}d` : '—'}</span> },
      { key: 'freshness', label: 'Freshness', render: r => freshnessBadge(r.gap_analysis!.freshness) },
      { key: 'freq', label: 'Freq', sortVal: r => r.days_matched, render: r => <span style={{ padding: '1px 7px', borderRadius: 10, fontWeight: 700, fontSize: '0.76rem', background: r.days_matched === r.total_days ? '#dcfce7' : '#fef3c7', color: r.days_matched === r.total_days ? '#15803d' : '#92400e' }}>{r.days_matched}/{r.total_days}</span> },
      { key: 'dist', label: 'Dist %', sortVal: r => { const d = r.gap_analysis!.fill_distances; return d.length > 0 ? Math.abs(d[d.length - 1]) : 999 }, render: r => <span style={{ fontSize: '0.8rem' }}>{r.gap_analysis!.fill_distances.length > 0 ? `${r.gap_analysis!.fill_distances[r.gap_analysis!.fill_distances.length - 1]}%` : '—'}</span> },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#fffbeb', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#92400e' }}>
          <strong>Fresh</strong> (≤3d): Most actionable — gap fill or continuation imminent.
          <strong> Aging</strong> (4-15d): Support/resistance zones forming.
          <strong> Stale</strong> (15+d): Structural — less immediate edge.
        </div>
        <CollapsibleSection title="🟢 Fresh Gaps (0-3 days)" count={fresh.length} hint="Highest trade potential">
          <SortableTable columns={cols} rows={fresh} defaultSortKey="age" />
        </CollapsibleSection>
        <CollapsibleSection title="🟡 Aging Gaps (4-15 days)" count={aging.length} hint="S/R zone forming">
          <SortableTable columns={cols} rows={aging} defaultSortKey="age" />
        </CollapsibleSection>
        <CollapsibleSection title="🔴 Stale Gaps (15+ days)" count={stale.length} hint="Structural" defaultOpen={false}>
          <SortableTable columns={cols} rows={stale} defaultSortKey="age" />
        </CollapsibleSection>
      </>
    )
  }

  const renderFillProgressTab = () => {
    const converging = gapFiltered.filter(r => r.gap_analysis!.fill_progress === 'Converging')
    const stable = gapFiltered.filter(r => r.gap_analysis!.fill_progress === 'Stable')
    const diverging = gapFiltered.filter(r => r.gap_analysis!.fill_progress === 'Diverging')
    const na = gapFiltered.filter(r => r.gap_analysis!.fill_progress === 'N/A')
    const cols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'progress', label: 'Progress', render: r => fillBadge(r.gap_analysis!.fill_progress) },
      { key: 'spark', label: 'Spark', render: r => <MiniSpark values={r.gap_analysis!.fill_distances} /> },
      { key: 'dist', label: 'Latest Dist', sortVal: r => { const d = r.gap_analysis!.fill_distances; return d.length > 0 ? Math.abs(d[d.length - 1]) : 999 }, render: r => { const d = r.gap_analysis!.fill_distances; return <span style={{ fontWeight: 600, fontSize: '0.82rem' }}>{d.length > 0 ? `${d[d.length - 1]}%` : '—'}</span> } },
      { key: 'freshness', label: 'Freshness', render: r => freshnessBadge(r.gap_analysis!.freshness) },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#f0fdf4', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#15803d' }}>
          <strong>Converging</strong>: Price moving toward gap zone — fill attempt likely.{' '}
          <strong>Stable</strong>: Holding distance.{' '}
          <strong>Diverging</strong>: Moving away — gap becoming irrelevant.
        </div>
        <CollapsibleSection title="🎯 Converging" count={converging.length} hint="Fill attempt likely">
          <SortableTable columns={cols} rows={converging} defaultSortKey="dist" />
        </CollapsibleSection>
        <CollapsibleSection title="➡️ Stable" count={stable.length} hint="Holding steady">
          <SortableTable columns={cols} rows={stable} defaultSortKey="dist" />
        </CollapsibleSection>
        <CollapsibleSection title="↗️ Diverging" count={diverging.length} hint="Moving away" defaultOpen={false}>
          <SortableTable columns={cols} rows={diverging} defaultSortKey="dist" />
        </CollapsibleSection>
        {na.length > 0 && (
          <CollapsibleSection title="❓ Insufficient Data" count={na.length} hint="Need more days" defaultOpen={false}>
            <SortableTable columns={cols} rows={na} />
          </CollapsibleSection>
        )}
      </>
    )
  }

  const renderNewGapsTab = () => {
    const withNew = gapFiltered.filter(r => r.gap_analysis!.new_gaps_in_window > 0)
    const noNew = gapFiltered.filter(r => r.gap_analysis!.new_gaps_in_window === 0)
    const cols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'newgaps', label: 'New Gaps', sortVal: r => r.gap_analysis!.new_gaps_in_window, render: r => <span style={{ padding: '1px 8px', borderRadius: 10, fontWeight: 700, fontSize: '0.82rem', background: '#dbeafe', color: '#1d4ed8' }}>{r.gap_analysis!.new_gaps_in_window}</span> },
      { key: 'freq', label: 'Total Sigs', sortVal: r => r.days_matched, render: r => <span style={{ fontWeight: 600 }}>{r.days_matched}/{r.total_days}</span> },
      { key: 'freshness', label: 'Freshness', render: r => freshnessBadge(r.gap_analysis!.freshness) },
      { key: 'vol', label: 'Vol', sortVal: r => r.gap_analysis!.avg_volume_ratio ?? 0, render: r => volBadge(r.gap_analysis!.avg_volume_ratio) },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#eff6ff', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#1d4ed8' }}>
          Tickers that formed <strong>new gaps</strong> within the streak window. New gaps are the most actionable — they indicate fresh momentum events. Multiple new gaps suggest <strong>momentum stacking</strong>.
        </div>
        <CollapsibleSection title="⚡ New Gaps Formed" count={withNew.length} hint="Fresh momentum events">
          {withNew.length === 0 ? (
            <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
              No new gaps formed in the last {data?.streak_days} trading days.
            </div>
          ) : (
            <SortableTable columns={cols} rows={withNew} defaultSortKey="newgaps" />
          )}
        </CollapsibleSection>
        {noNew.length > 0 && (
          <CollapsibleSection title="📋 Existing Gaps Only" count={noNew.length} hint="No new formations" defaultOpen={false}>
            <SortableTable columns={cols} rows={noNew} defaultSortKey="ticker" />
          </CollapsibleSection>
        )}
      </>
    )
  }

  const renderTransitionsTab = () => {
    const changing = gapFiltered.filter(r => !r.gap_analysis!.transition_summary.startsWith('Steady'))
    const steady = gapFiltered.filter(r => r.gap_analysis!.transition_summary.startsWith('Steady'))
    const changingCols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      {
        key: 'transition', label: 'Transition', align: 'left',
        sortVal: r => new Set(r.gap_analysis!.type_sequence).size,
        render: r => (
          <span style={{ fontSize: '0.78rem' }}>
            {r.gap_analysis!.type_sequence.map((s, i) => (
              <span key={i}>
                {i > 0 && <span style={{ color: 'var(--text-secondary)', margin: '0 3px' }}>→</span>}
                <span style={{
                  padding: '1px 5px', borderRadius: 4, fontSize: '0.72rem', fontWeight: 600,
                  background: s === 'In Gap' ? '#fef3c7' : s === 'At Edge' ? '#dbeafe' : '#f1f5f9',
                  color: s === 'In Gap' ? '#92400e' : s === 'At Edge' ? '#1d4ed8' : '#475569',
                }}>{s}</span>
              </span>
            ))}
          </span>
        ),
      },
      { key: 'fill', label: 'Fill', render: r => fillBadge(r.gap_analysis!.fill_progress) },
    ]
    const steadyCols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      {
        key: 'status', label: 'Status',
        sortVal: r => r.gap_analysis!.type_sequence[0] || '',
        render: r => <span style={{ padding: '1px 6px', borderRadius: 4, fontSize: '0.75rem', fontWeight: 600, background: '#f1f5f9', color: '#475569' }}>{r.gap_analysis!.type_sequence[0] || '—'}</span>,
      },
      { key: 'age', label: 'Age', sortVal: r => r.gap_analysis!.freshest_gap_age ?? 999, render: r => <span style={{ fontSize: '0.82rem' }}>{r.gap_analysis!.freshest_gap_age != null ? `${r.gap_analysis!.freshest_gap_age}d` : '—'}</span> },
      { key: 'vol', label: 'Vol', sortVal: r => r.gap_analysis!.avg_volume_ratio ?? 0, render: r => volBadge(r.gap_analysis!.avg_volume_ratio) },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#faf5ff', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#7c3aed' }}>
          How each ticker's gap status <strong>changes</strong> across the streak days.{' '}
          <strong>Changing</strong> status (e.g., Unfilled → In Gap → At Edge) signals developing price action.{' '}
          <strong>Steady</strong> means the same status persisted.
        </div>
        <CollapsibleSection title="🔄 Changing Status" count={changing.length} hint="Developing price action">
          <SortableTable columns={changingCols} rows={changing} defaultSortKey="transition" />
        </CollapsibleSection>
        <CollapsibleSection title="➡️ Steady Status" count={steady.length} hint="No change" defaultOpen={false}>
          <SortableTable columns={steadyCols} rows={steady} defaultSortKey="ticker" />
        </CollapsibleSection>
      </>
    )
  }

  const renderVolumeTab = () => {
    const allSorted = [...gapFiltered].sort((a, b) => (b.gap_analysis!.avg_volume_ratio ?? 0) - (a.gap_analysis!.avg_volume_ratio ?? 0))
    const highVol = allSorted.filter(r => (r.gap_analysis!.avg_volume_ratio ?? 0) >= 1.5)
    const normalVol = allSorted.filter(r => { const v = r.gap_analysis!.avg_volume_ratio ?? 0; return v >= 1.0 && v < 1.5 })
    const lowVol = allSorted.filter(r => (r.gap_analysis!.avg_volume_ratio ?? 0) < 1.0 && r.gap_analysis!.avg_volume_ratio != null)
    const noVol = allSorted.filter(r => r.gap_analysis!.avg_volume_ratio == null)
    const cols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'vol', label: 'Avg Vol Ratio', sortVal: r => r.gap_analysis!.avg_volume_ratio ?? 0, render: r => volBadge(r.gap_analysis!.avg_volume_ratio) },
      { key: 'freshness', label: 'Freshness', render: r => freshnessBadge(r.gap_analysis!.freshness) },
      { key: 'fill', label: 'Fill', render: r => fillBadge(r.gap_analysis!.fill_progress) },
      { key: 'freq', label: 'Freq', sortVal: r => r.days_matched, render: r => <span style={{ fontWeight: 700, fontSize: '0.78rem' }}>{r.days_matched}/{r.total_days}</span> },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#f0fdf4', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#15803d' }}>
          Volume ratio = average daily volume / 20-day average. <strong>High volume</strong> (≥1.5x) confirms gap significance.{' '}
          <strong>Low volume</strong> ({'<'}1x) suggests weaker conviction.
        </div>
        <CollapsibleSection title="📶 High Volume (≥1.5x)" count={highVol.length} hint="Strong conviction">
          <SortableTable columns={cols} rows={highVol} defaultSortKey="vol" />
        </CollapsibleSection>
        <CollapsibleSection title="➡️ Normal Volume (1.0-1.5x)" count={normalVol.length} hint="Average activity">
          <SortableTable columns={cols} rows={normalVol} defaultSortKey="vol" />
        </CollapsibleSection>
        <CollapsibleSection title="📉 Low Volume (<1.0x)" count={lowVol.length} hint="Weak conviction" defaultOpen={false}>
          <SortableTable columns={cols} rows={lowVol} defaultSortKey="vol" />
        </CollapsibleSection>
        {noVol.length > 0 && (
          <CollapsibleSection title="❓ No Volume Data" count={noVol.length} defaultOpen={false}>
            <SortableTable columns={cols} rows={noVol} />
          </CollapsibleSection>
        )}
      </>
    )
  }

  const renderGapTabContent = () => {
    if (gapFiltered.length === 0 && gapTab !== 'overview') {
      return (
        <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
          No gap analysis data available. Run Analyze first.
        </div>
      )
    }
    switch (gapTab) {
      case 'overview': return renderOverviewTab()
      case 'freshness': return renderFreshnessTab()
      case 'fill': return renderFillProgressTab()
      case 'newgaps': return renderNewGapsTab()
      case 'transitions': return renderTransitionsTab()
      case 'volume': return renderVolumeTab()
    }
  }

  // ── MA Crossover tab renderers ──
  const renderMaDirectionTab = () => {
    const bullish = maFiltered.filter(r => r.ma_analysis!.direction === 'Bullish')
    const bearish = maFiltered.filter(r => r.ma_analysis!.direction === 'Bearish')
    const mixed = maFiltered.filter(r => r.ma_analysis!.direction === 'Mixed')
    const cols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'dir', label: 'Direction', render: r => directionBadge(r.ma_analysis!.direction) },
      { key: 'wk', label: 'Weekly', sortVal: r => r.ma_analysis!.weekly_alignment === 'Confirmed Bullish' ? 2 : r.ma_analysis!.weekly_alignment === 'Confirmed Bearish' ? 2 : r.ma_analysis!.weekly_alignment?.includes('Counter') ? 1 : 0, render: r => weeklyAlignBadge(r.ma_analysis!.weekly_alignment) },
      { key: 'age', label: 'Cross Age', sortVal: r => r.ma_analysis!.days_since_cross ?? 999, render: r => <span style={{ fontWeight: 600, fontSize: '0.82rem' }}>{r.ma_analysis!.days_since_cross != null ? `${r.ma_analysis!.days_since_cross}d` : '—'}</span> },
      { key: 'spread', label: 'Spread %', sortVal: r => { const s = r.ma_analysis!.spreads; return s.length > 0 ? Math.abs(s[s.length - 1]) : 0 }, render: r => { const s = r.ma_analysis!.spreads; return <span style={{ fontWeight: 600, fontSize: '0.82rem' }}>{s.length > 0 ? `${s[s.length - 1] > 0 ? '+' : ''}${s[s.length - 1]}%` : '—'}</span> } },
      { key: 'freq', label: 'Freq', sortVal: r => r.days_matched, render: r => <span style={{ padding: '1px 7px', borderRadius: 10, fontWeight: 700, fontSize: '0.76rem', background: r.days_matched === r.total_days ? '#dcfce7' : '#fef3c7', color: r.days_matched === r.total_days ? '#15803d' : '#92400e' }}>{r.days_matched}/{r.total_days}</span> },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#f0fdf4', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#15803d' }}>
          <strong>Bullish</strong>: All streak days show bullish crossover signals.{' '}
          <strong>Bearish</strong>: All bearish — short/exit setup.{' '}
          <strong>Weekly</strong>: Shows daily+weekly alignment. <em>Confirmed</em> = both agree (highest conviction). <em>Counter-trend</em> = daily fights weekly (risky).
        </div>
        <CollapsibleSection title="🟢 Bullish" count={bullish.length} hint="Consistent buy signal">
          <SortableTable columns={cols} rows={bullish} defaultSortKey="wk" />
        </CollapsibleSection>
        <CollapsibleSection title="🔴 Bearish" count={bearish.length} hint="Consistent sell signal">
          <SortableTable columns={cols} rows={bearish} defaultSortKey="wk" />
        </CollapsibleSection>
        <CollapsibleSection title="🟡 Mixed (Whipsaw)" count={mixed.length} hint="Unreliable" defaultOpen={false}>
          <SortableTable columns={cols} rows={mixed} defaultSortKey="ticker" />
        </CollapsibleSection>
      </>
    )
  }

  const renderMaSpreadTab = () => {
    const widening = maFiltered.filter(r => r.ma_analysis!.spread_trend === 'Widening')
    const stable = maFiltered.filter(r => r.ma_analysis!.spread_trend === 'Stable')
    const narrowing = maFiltered.filter(r => r.ma_analysis!.spread_trend === 'Narrowing')
    const na = maFiltered.filter(r => r.ma_analysis!.spread_trend === 'N/A')
    const cols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'spread', label: 'Daily', render: r => spreadBadge(r.ma_analysis!.spread_trend) },
      { key: 'spark', label: 'Spread', render: r => <MiniSpark values={r.ma_analysis!.spreads.map(Math.abs)} /> },
      { key: 'latest', label: 'Latest', sortVal: r => { const s = r.ma_analysis!.spreads; return s.length > 0 ? Math.abs(s[s.length - 1]) : 0 }, render: r => { const s = r.ma_analysis!.spreads; return <span style={{ fontWeight: 600, fontSize: '0.82rem' }}>{s.length > 0 ? `${s[s.length - 1] > 0 ? '+' : ''}${s[s.length - 1]}%` : '—'}</span> } },
      { key: 'wkSpread', label: 'W-Spread', sortVal: r => Math.abs(r.ma_analysis!.weekly_spread_pct ?? 0), render: r => { const w = r.ma_analysis!.weekly_spread_pct; return w != null ? <span style={{ fontWeight: 600, fontSize: '0.82rem', color: w >= 0 ? '#15803d' : '#991b1b' }}>{w > 0 ? '+' : ''}{w.toFixed(2)}%</span> : <span style={{ color: '#9ca3af', fontSize: '0.78rem' }}>—</span> } },
      { key: 'wkTrend', label: 'W-Trend', render: r => spreadBadge(r.ma_analysis!.weekly_spread_trend) },
      { key: 'dir', label: 'Dir', render: r => directionBadge(r.ma_analysis!.direction) },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#eff6ff', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#1d4ed8' }}>
          <strong>Widening</strong>: Short MA pulling away from long — trend strengthening.{' '}
          <strong>Stable</strong>: Consistent spread — trend holding.{' '}
          <strong>Narrowing</strong>: MAs converging — possible reversal.{' '}
          <strong>W-Spread/W-Trend</strong>: Weekly MA spread and trend — confirms or contradicts daily spread direction.
        </div>
        <CollapsibleSection title="📈 Widening (Strengthening)" count={widening.length} hint="Trend accelerating">
          <SortableTable columns={cols} rows={widening} defaultSortKey="latest" />
        </CollapsibleSection>
        <CollapsibleSection title="➡️ Stable" count={stable.length} hint="Trend holding">
          <SortableTable columns={cols} rows={stable} defaultSortKey="latest" />
        </CollapsibleSection>
        <CollapsibleSection title="📉 Narrowing (Weakening)" count={narrowing.length} hint="Possible reversal" defaultOpen={false}>
          <SortableTable columns={cols} rows={narrowing} defaultSortKey="latest" />
        </CollapsibleSection>
        {na.length > 0 && (
          <CollapsibleSection title="❓ Insufficient Data" count={na.length} defaultOpen={false}>
            <SortableTable columns={cols} rows={na} />
          </CollapsibleSection>
        )}
      </>
    )
  }

  const renderMaMomentumTab = () => {
    const accel = maFiltered.filter(r => r.ma_analysis!.price_momentum === 'Accelerating')
    const steady = maFiltered.filter(r => r.ma_analysis!.price_momentum === 'Steady')
    const stalling = maFiltered.filter(r => r.ma_analysis!.price_momentum === 'Stalling')
    const choppy = maFiltered.filter(r => r.ma_analysis!.price_momentum === 'Choppy')
    const na = maFiltered.filter(r => r.ma_analysis!.price_momentum === 'N/A')
    const cols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'mom', label: 'Momentum', render: r => momentumBadge(r.ma_analysis!.price_momentum) },
      { key: 'spark', label: 'Price Chg', render: r => <MiniSpark values={r.ma_analysis!.price_changes.map(Math.abs)} /> },
      { key: 'latest', label: 'Since Cross', sortVal: r => { const c = r.ma_analysis!.price_changes; return c.length > 0 ? c[c.length - 1] : 0 }, render: r => { const c = r.ma_analysis!.price_changes; const v = c.length > 0 ? c[c.length - 1] : null; return <span style={{ fontWeight: 600, fontSize: '0.82rem', color: v != null ? (v >= 0 ? '#15803d' : '#991b1b') : undefined }}>{v != null ? `${v > 0 ? '+' : ''}${v}%` : '—'}</span> } },
      { key: 'wk', label: 'Weekly', render: r => weeklyAlignBadge(r.ma_analysis!.weekly_alignment) },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#faf5ff', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#7c3aed' }}>
          Price change since crossover tracked across streak days.{' '}
          <strong>Accelerating</strong>: Move gaining steam.{' '}
          <strong>Steady</strong>: Holding pace.{' '}
          <strong>Stalling</strong>: Losing momentum.{' '}
          <strong>Weekly</strong>: Confirmed = momentum aligned with weekly trend (highest conviction).
        </div>
        <CollapsibleSection title="🚀 Accelerating" count={accel.length} hint="Gaining steam">
          <SortableTable columns={cols} rows={accel} defaultSortKey="latest" />
        </CollapsibleSection>
        <CollapsibleSection title="➡️ Steady" count={steady.length} hint="Holding pace">
          <SortableTable columns={cols} rows={steady} defaultSortKey="latest" />
        </CollapsibleSection>
        <CollapsibleSection title="⏸️ Stalling" count={stalling.length} hint="Losing momentum">
          <SortableTable columns={cols} rows={stalling} defaultSortKey="latest" />
        </CollapsibleSection>
        {choppy.length > 0 && (
          <CollapsibleSection title="🔀 Choppy (Mixed)" count={choppy.length} hint="Direction unclear" defaultOpen={false}>
            <SortableTable columns={cols} rows={choppy} defaultSortKey="ticker" />
          </CollapsibleSection>
        )}
        {na.length > 0 && (
          <CollapsibleSection title="❓ Insufficient Data" count={na.length} defaultOpen={false}>
            <SortableTable columns={cols} rows={na} />
          </CollapsibleSection>
        )}
      </>
    )
  }

  const renderMaSignalsTab = () => {
    const changing = maFiltered.filter(r => !r.ma_analysis!.signal_flow.startsWith('Steady'))
    const steady = maFiltered.filter(r => r.ma_analysis!.signal_flow.startsWith('Steady'))
    const changingCols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      {
        key: 'flow', label: 'Signal Flow', align: 'left',
        sortVal: r => new Set(r.ma_analysis!.signal_sequence).size,
        render: r => (
          <span style={{ fontSize: '0.78rem' }}>
            {r.ma_analysis!.signal_sequence.map((s, i) => (
              <span key={i}>
                {i > 0 && <span style={{ color: 'var(--text-secondary)', margin: '0 3px' }}>→</span>}
                {signalBadge(s)}
              </span>
            ))}
          </span>
        ),
      },
      { key: 'wkSig', label: 'W-Signal', render: r => weeklySignalBadge(r.ma_analysis!.weekly_signal) },
      { key: 'spread', label: 'Spread', render: r => spreadBadge(r.ma_analysis!.spread_trend) },
    ]
    const steadyCols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'signal', label: 'Signal', render: r => signalBadge(r.ma_analysis!.signal_sequence[0] || '—') },
      { key: 'wkSig', label: 'W-Signal', render: r => weeklySignalBadge(r.ma_analysis!.weekly_signal) },
      { key: 'age', label: 'Cross Age', sortVal: r => r.ma_analysis!.days_since_cross ?? 999, render: r => <span style={{ fontSize: '0.82rem' }}>{r.ma_analysis!.days_since_cross != null ? `${r.ma_analysis!.days_since_cross}d` : '—'}</span> },
      { key: 'vol', label: 'Vol', sortVal: r => r.ma_analysis!.avg_volume_ratio ?? 0, render: r => volBadge(r.ma_analysis!.avg_volume_ratio) },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#fffbeb', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#92400e' }}>
          Signal type evolution across streak days.{' '}
          <strong>Steady</strong> = same signal each day (reliable).{' '}
          <strong>Changing</strong> = signal type shifted (e.g., Crossover → Recent).{' '}
          <strong>W-Signal</strong>: Weekly crossover signal — if weekly agrees with daily flow direction, signal is more reliable.
        </div>
        <CollapsibleSection title="🔀 Changing Signal" count={changing.length} hint="Signal type shifted">
          <SortableTable columns={changingCols} rows={changing} defaultSortKey="flow" />
        </CollapsibleSection>
        <CollapsibleSection title="➡️ Steady Signal" count={steady.length} hint="Same signal each day" defaultOpen={false}>
          <SortableTable columns={steadyCols} rows={steady} defaultSortKey="ticker" />
        </CollapsibleSection>
      </>
    )
  }

  const renderMaVolumeTab = () => {
    const allSorted = [...maFiltered].sort((a, b) => (b.ma_analysis!.avg_volume_ratio ?? 0) - (a.ma_analysis!.avg_volume_ratio ?? 0))
    const highVol = allSorted.filter(r => (r.ma_analysis!.avg_volume_ratio ?? 0) >= 1.5)
    const normalVol = allSorted.filter(r => { const v = r.ma_analysis!.avg_volume_ratio ?? 0; return v >= 1.0 && v < 1.5 })
    const lowVol = allSorted.filter(r => (r.ma_analysis!.avg_volume_ratio ?? 0) < 1.0 && r.ma_analysis!.avg_volume_ratio != null)
    const noVol = allSorted.filter(r => r.ma_analysis!.avg_volume_ratio == null)
    const cols: ColDef<StreakResult>[] = [
      { key: 'ticker', label: 'Ticker', align: 'left', sortVal: r => r.ticker, render: r => <TickerLink ticker={r.ticker} navigate={navigate} /> },
      { key: 'vol', label: 'Avg Vol', sortVal: r => r.ma_analysis!.avg_volume_ratio ?? 0, render: r => volBadge(r.ma_analysis!.avg_volume_ratio) },
      { key: 'dir', label: 'Dir', render: r => directionBadge(r.ma_analysis!.direction) },
      { key: 'wk', label: 'Weekly', render: r => weeklyAlignBadge(r.ma_analysis!.weekly_alignment) },
      { key: 'freq', label: 'Freq', sortVal: r => r.days_matched, render: r => <span style={{ fontWeight: 700, fontSize: '0.78rem' }}>{r.days_matched}/{r.total_days}</span> },
    ]
    return (
      <>
        <div style={{ padding: '10px 12px', background: '#f0fdf4', borderBottom: '1px solid var(--border-color)', fontSize: '0.78rem', color: '#15803d' }}>
          Volume ratio = daily volume / 20-day average. <strong>High volume</strong> (≥1.5x) confirms crossover conviction.{' '}
          <strong>Low volume</strong> ({'<'}1x) = weak crossover, higher whipsaw risk.{' '}
          <strong>Weekly</strong>: Alignment with weekly trend adds conviction context beyond volume alone.
        </div>
        <CollapsibleSection title="📶 High Volume (≥1.5x)" count={highVol.length} hint="Strong conviction">
          <SortableTable columns={cols} rows={highVol} defaultSortKey="vol" />
        </CollapsibleSection>
        <CollapsibleSection title="➡️ Normal Volume (1.0-1.5x)" count={normalVol.length} hint="Average activity">
          <SortableTable columns={cols} rows={normalVol} defaultSortKey="vol" />
        </CollapsibleSection>
        <CollapsibleSection title="📉 Low Volume (<1.0x)" count={lowVol.length} hint="Weak conviction" defaultOpen={false}>
          <SortableTable columns={cols} rows={lowVol} defaultSortKey="vol" />
        </CollapsibleSection>
        {noVol.length > 0 && (
          <CollapsibleSection title="❓ No Volume Data" count={noVol.length} defaultOpen={false}>
            <SortableTable columns={cols} rows={noVol} />
          </CollapsibleSection>
        )}
      </>
    )
  }

  const renderMaTabContent = () => {
    if (maFiltered.length === 0 && maTab !== 'overview') {
      return (
        <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
          No MA crossover analysis data available. Run Analyze first.
        </div>
      )
    }
    switch (maTab) {
      case 'overview': return renderOverviewTab()
      case 'direction': return renderMaDirectionTab()
      case 'spread': return renderMaSpreadTab()
      case 'momentum': return renderMaMomentumTab()
      case 'signals': return renderMaSignalsTab()
      case 'volume': return renderMaVolumeTab()
    }
  }

  // ── Fibonacci tab renderers ──

  const renderFibLevelsTab = () => {
    const groups: Record<string, typeof fibFiltered> = {}
    fibFiltered.forEach(r => {
      const stab = r.fib_analysis!.level_stability
      if (!groups[stab]) groups[stab] = []
      groups[stab].push(r)
    })
    const order = ['Locked', 'Sticky', 'Drifting']
    const hints: Record<string, string> = {
      'Locked': 'Same fib level every day — strong zone',
      'Sticky': 'Mostly same fib level — testing the zone',
      'Drifting': 'Changing levels — price is moving through fibs',
    }
    return (
      <>
        {order.filter(g => groups[g]).map(g => (
          <CollapsibleSection key={g} title={`${g}`} count={groups[g].length} hint={hints[g]}>
            <SortableTable
              columns={[
                { key: 'ticker', label: 'Ticker', render: r => <span className="ticker" onClick={() => navigate(`/ticker/${r.ticker}`)}>{r.ticker}</span>, sortVal: r => r.ticker },
                { key: 'dominant', label: 'Dominant Level', align: 'center', render: r => <span style={{ fontWeight: 700, color: r.fib_analysis!.dominant_level === '61.8%' ? '#b8860b' : 'inherit' }}>{r.fib_analysis!.dominant_level}</span>, sortVal: r => r.fib_analysis!.dominant_level },
                { key: 'sequence', label: 'Level Flow', render: r => <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>{r.fib_analysis!.level_sequence.join(' → ')}</span> },
                { key: 'consistency', label: 'Match', align: 'center', render: r => <span>{r.consistency}%</span>, sortVal: r => r.consistency },
              ]}
              rows={groups[g]}
              defaultSortKey="consistency"
            />
          </CollapsibleSection>
        ))}
      </>
    )
  }

  const renderFibProximityTab = () => {
    const groups: Record<string, typeof fibFiltered> = {}
    fibFiltered.forEach(r => {
      const pt = r.fib_analysis!.proximity_trend
      if (!groups[pt]) groups[pt] = []
      groups[pt].push(r)
    })
    const order = ['Converging', 'Hovering', 'Diverging', 'N/A']
    const hints: Record<string, string> = {
      'Converging': 'Price moving closer to fib level — testing imminent',
      'Hovering': 'Price staying near the level — coiling for a move',
      'Diverging': 'Price moving away from fib level',
    }
    return (
      <>
        {order.filter(g => groups[g]).map(g => (
          <CollapsibleSection key={g} title={g} count={groups[g].length} hint={hints[g]}>
            <SortableTable
              columns={[
                { key: 'ticker', label: 'Ticker', render: r => <span className="ticker" onClick={() => navigate(`/ticker/${r.ticker}`)}>{r.ticker}</span>, sortVal: r => r.ticker },
                { key: 'level', label: 'Near Level', align: 'center', render: r => <span style={{ fontWeight: 700 }}>{r.fib_analysis!.dominant_level}</span> },
                { key: 'dist', label: 'Distance Trend', render: r => {
                  const dists = r.fib_analysis!.distances
                  return <span style={{ fontSize: '0.75rem' }}>{dists.map(d => `${d > 0 ? '+' : ''}${d.toFixed(1)}%`).join(' → ')}</span>
                }},
                { key: 'latest_dist', label: 'Latest', align: 'center', render: r => {
                  const d = r.fib_analysis!.distances
                  const last = d[d.length - 1]
                  return <span style={{ fontWeight: 600, color: Math.abs(last) <= 1.5 ? 'var(--success)' : 'var(--text-secondary)' }}>{last > 0 ? '+' : ''}{last.toFixed(2)}%</span>
                }, sortVal: r => Math.abs(r.fib_analysis!.distances[r.fib_analysis!.distances.length - 1]) },
              ]}
              rows={groups[g]}
              defaultSortKey="latest_dist"
            />
          </CollapsibleSection>
        ))}
      </>
    )
  }

  const renderFibDepthTab = () => {
    const groups: Record<string, typeof fibFiltered> = {}
    fibFiltered.forEach(r => {
      const dt = r.fib_analysis!.depth_trend
      if (!groups[dt]) groups[dt] = []
      groups[dt].push(r)
    })
    const order = ['Deepening', 'Stable', 'Recovering', 'N/A']
    const hints: Record<string, string> = {
      'Deepening': 'Retracement getting deeper — more risk but better entry',
      'Stable': 'Retracement depth holding steady',
      'Recovering': 'Price rebounding — retracement reversing',
    }
    return (
      <>
        {order.filter(g => groups[g]).map(g => (
          <CollapsibleSection key={g} title={g} count={groups[g].length} hint={hints[g]}>
            <SortableTable
              columns={[
                { key: 'ticker', label: 'Ticker', render: r => <span className="ticker" onClick={() => navigate(`/ticker/${r.ticker}`)}>{r.ticker}</span>, sortVal: r => r.ticker },
                { key: 'trend', label: 'Trend', align: 'center', render: r => <span style={{ fontSize: '0.78rem' }}>{r.fib_analysis!.trend_consistency}</span> },
                { key: 'retrace', label: 'Retrace Trend', render: r => {
                  const rp = r.fib_analysis!.retrace_pcts
                  return <span style={{ fontSize: '0.75rem' }}>{rp.map(p => `${p.toFixed(0)}%`).join(' → ')}</span>
                }},
                { key: 'pivots', label: 'Pivots', align: 'center', render: r => <span style={{ fontSize: '0.78rem' }}>{r.fib_analysis!.pivot_stable ? '🔒 Stable' : '🔄 Shifted'}</span> },
                { key: 'latest_retrace', label: 'Latest', align: 'center', render: r => {
                  const rp = r.fib_analysis!.retrace_pcts
                  return <span style={{ fontWeight: 600 }}>{rp[rp.length - 1].toFixed(1)}%</span>
                }, sortVal: r => r.fib_analysis!.retrace_pcts[r.fib_analysis!.retrace_pcts.length - 1] },
              ]}
              rows={groups[g]}
              defaultSortKey="latest_retrace"
            />
          </CollapsibleSection>
        ))}
      </>
    )
  }

  const renderFibSignalsTab = () => {
    return (
      <SortableTable
        columns={[
          { key: 'ticker', label: 'Ticker', render: r => <span className="ticker" onClick={() => navigate(`/ticker/${r.ticker}`)}>{r.ticker}</span>, sortVal: r => r.ticker },
          { key: 'flow', label: 'Signal Flow', render: r => <span style={{ fontSize: '0.75rem' }}>{r.fib_analysis!.signal_flow}</span> },
          { key: 'trend', label: 'Trend', align: 'center', render: r => <span style={{ fontSize: '0.78rem' }}>{r.fib_analysis!.trend_consistency}</span> },
          { key: 'match', label: 'Match', align: 'center', render: r => <span>{r.consistency}%</span>, sortVal: r => r.consistency },
        ]}
        rows={fibFiltered}
        defaultSortKey="match"
      />
    )
  }

  const renderFibVolumeTab = () => {
    const withVol = fibFiltered.filter(r => r.fib_analysis!.avg_volume_ratio != null)
    const high = withVol.filter(r => r.fib_analysis!.avg_volume_ratio! >= 1.5)
    const normal = withVol.filter(r => r.fib_analysis!.avg_volume_ratio! >= 0.8 && r.fib_analysis!.avg_volume_ratio! < 1.5)
    const low = withVol.filter(r => r.fib_analysis!.avg_volume_ratio! < 0.8)

    const volCols = [
      { key: 'ticker', label: 'Ticker', render: (r: StreakResult) => <span className="ticker" onClick={() => navigate(`/ticker/${r.ticker}`)}>{r.ticker}</span>, sortVal: (r: StreakResult) => r.ticker },
      { key: 'vol', label: 'Avg Vol Ratio', align: 'center' as const, render: (r: StreakResult) => <span style={{ fontWeight: 600 }}>{r.fib_analysis!.avg_volume_ratio!.toFixed(2)}x</span>, sortVal: (r: StreakResult) => r.fib_analysis!.avg_volume_ratio! },
      { key: 'level', label: 'Level', align: 'center' as const, render: (r: StreakResult) => <span>{r.fib_analysis!.dominant_level}</span> },
      { key: 'match', label: 'Match', align: 'center' as const, render: (r: StreakResult) => <span>{r.consistency}%</span>, sortVal: (r: StreakResult) => r.consistency },
    ]

    return (
      <>
        {high.length > 0 && <CollapsibleSection title="High Volume (≥1.5x)" count={high.length} hint="Volume confirms fib level interest">
          <SortableTable columns={volCols} rows={high} defaultSortKey="vol" />
        </CollapsibleSection>}
        {normal.length > 0 && <CollapsibleSection title="Normal Volume" count={normal.length}>
          <SortableTable columns={volCols} rows={normal} defaultSortKey="vol" />
        </CollapsibleSection>}
        {low.length > 0 && <CollapsibleSection title="Low Volume (<0.8x)" count={low.length} hint="Weak conviction at fib level">
          <SortableTable columns={volCols} rows={low} defaultSortKey="vol" />
        </CollapsibleSection>}
      </>
    )
  }

  const renderFibTabContent = () => {
    if (fibFiltered.length === 0 && fibTab !== 'overview') {
      return (
        <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
          No Fibonacci analysis data available. Run Analyze first.
        </div>
      )
    }
    switch (fibTab) {
      case 'overview': return renderOverviewTab()
      case 'levels': return renderFibLevelsTab()
      case 'proximity': return renderFibProximityTab()
      case 'depth': return renderFibDepthTab()
      case 'signals': return renderFibSignalsTab()
      case 'volume': return renderFibVolumeTab()
    }
  }

  return (
    <>
      {/* Toggle tab — fixed on right edge */}
      <div
        onClick={() => setOpen(!open)}
        style={{
          position: 'fixed',
          right: open ? DRAWER_WIDTH : 0,
          top: '50%',
          transform: 'translateY(-50%)',
          zIndex: 1001,
          background: 'var(--primary-color)',
          color: '#fff',
          padding: '12px 6px',
          borderRadius: '8px 0 0 8px',
          cursor: 'pointer',
          writingMode: 'vertical-rl',
          textOrientation: 'mixed',
          fontSize: '0.8rem',
          fontWeight: 700,
          letterSpacing: '0.5px',
          boxShadow: '-2px 0 8px rgba(0,0,0,0.15)',
          transition: 'right 0.3s ease',
          userSelect: 'none',
        }}
        title={open ? 'Close Streak Analysis' : 'Open Streak Analysis'}
      >
        📊 Streak{data ? ` (${data.total_with_signals})` : ''}
      </div>

      {/* Backdrop overlay */}
      {open && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(0,0,0,0.08)',
            zIndex: 999,
            pointerEvents: 'none',
            transition: 'opacity 0.3s ease',
          }}
        />
      )}

      {/* Drawer panel */}
      <div
        style={{
          position: 'fixed',
          top: 0,
          right: open ? 0 : -DRAWER_WIDTH,
          width: DRAWER_WIDTH,
          height: '100vh',
          background: 'var(--card-bg)',
          borderLeft: '1px solid var(--border-color)',
          boxShadow: open ? '-4px 0 20px rgba(0,0,0,0.12)' : 'none',
          zIndex: 1000,
          transition: 'right 0.3s ease, width 0.3s ease',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
      >
        {/* Drawer header */}
        <div style={{
          padding: '16px 16px 12px',
          borderBottom: '1px solid var(--border-color)',
          background: '#f8fafc',
          flexShrink: 0,
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
            <h3 style={{ margin: 0, fontSize: '1rem', fontWeight: 700 }}>📊 Streak Analysis</h3>
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
              <span
                onClick={() => setMaximized(m => !m)}
                style={{ cursor: 'pointer', fontSize: '1rem', color: 'var(--text-secondary)', lineHeight: 1 }}
                title={maximized ? 'Restore size' : 'Full page'}
              >
                {maximized ? '⧉' : '⛶'}
              </span>
              <span
                onClick={() => setOpen(false)}
                style={{ cursor: 'pointer', fontSize: '1.2rem', color: 'var(--text-secondary)', lineHeight: 1 }}
                title="Close"
              >
                ✕
              </span>
            </div>
          </div>
          <p style={{ color: 'var(--text-secondary)', fontSize: '0.78rem', margin: 0 }}>
            {isGaps
              ? 'Gap consistency analysis with freshness, fill progress, transitions & volume.'
              : isMa
              ? 'MA crossover analysis with direction, spread trend, momentum & volume.'
              : isFib
              ? 'Fibonacci level consistency with proximity trends, retracement depth & volume.'
              : 'Tickers that appear in scan results consistently across multiple days have stronger signals.'}
          </p>
        </div>

        {/* Controls */}
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-color)', flexShrink: 0 }}>
          {/* Strategy selector — only when no fixed strategy */}
          {canSelectStrategy && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' }}>
              <label style={{ fontSize: '0.82rem', fontWeight: 500, whiteSpace: 'nowrap' }}>Strategy:</label>
              <select
                value={selectedStrategy}
                onChange={(e) => { setSelectedStrategy(e.target.value); setData(null) }}
                style={{
                  flex: 1, padding: '4px 8px', border: '1px solid var(--border-color)',
                  borderRadius: '4px', fontSize: '0.82rem', background: '#fff',
                }}
              >
                {STRATEGY_OPTIONS.map(o => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            </div>
          )}
          {/* MA periods — show when strategy is ma-crossover and no fixed strategy */}
          {canSelectStrategy && selectedStrategy === 'ma-crossover' && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' }}>
              <label style={{ fontSize: '0.8rem', whiteSpace: 'nowrap' }}>Short:</label>
              <input type="number" min={2} max={50} value={localShort} onChange={e => setLocalShort(Number(e.target.value))}
                style={{ width: 50, padding: '3px 6px', border: '1px solid var(--border-color)', borderRadius: 4, fontSize: '0.82rem' }} />
              <label style={{ fontSize: '0.8rem', whiteSpace: 'nowrap' }}>Long:</label>
              <input type="number" min={5} max={200} value={localLong} onChange={e => setLocalLong(Number(e.target.value))}
                style={{ width: 50, padding: '3px 6px', border: '1px solid var(--border-color)', borderRadius: 4, fontSize: '0.82rem' }} />
            </div>
          )}
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' }}>
            <label style={{ fontSize: '0.82rem', fontWeight: 500, whiteSpace: 'nowrap' }}>Days:</label>
            <input
              type="range"
              min={2}
              max={10}
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              style={{ flex: 1 }}
            />
            <span style={{ fontWeight: 700, fontSize: '0.95rem', width: '20px', textAlign: 'center' }}>{days}</span>
            <button
              className="btn btn-primary"
              onClick={runStreak}
              disabled={loading}
              style={{ padding: '5px 14px', fontSize: '0.8rem', whiteSpace: 'nowrap' }}
            >
              {loading ? '...' : 'Analyze'}
            </button>
          </div>

          {data && !loading && (
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
              <input
                type="text"
                placeholder="Filter..."
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                style={{
                  padding: '4px 8px',
                  border: '1px solid var(--border-color)',
                  borderRadius: '4px',
                  fontSize: '0.8rem',
                  width: '100px',
                }}
              />
              <label style={{ display: 'flex', alignItems: 'center', gap: '3px', fontSize: '0.8rem', cursor: 'pointer' }}>
                <input type="checkbox" checked={perfectOnly} onChange={(e) => setPerfectOnly(e.target.checked)} />
                Perfect only
              </label>
              <span style={{ fontSize: '0.78rem', color: 'var(--text-secondary)', marginLeft: 'auto' }}>
                {filtered.length} tickers
                {perfectCount > 0 && ` · ${perfectCount} perfect`}
              </span>
            </div>
          )}
        </div>

        {/* Gap analysis tabs — only for gaps strategy */}
        {isGaps && data && !loading && (
          <div style={{
            display: 'flex',
            borderBottom: '1px solid var(--border-color)',
            background: '#f8fafc',
            flexShrink: 0,
            overflowX: 'auto',
          }}>
            {GAP_TABS.map(t => (
              <button
                key={t.key}
                onClick={() => setGapTab(t.key)}
                style={{
                  flex: '0 0 auto',
                  padding: '8px 12px',
                  border: 'none',
                  borderBottom: gapTab === t.key ? '2px solid var(--primary-color)' : '2px solid transparent',
                  background: gapTab === t.key ? '#fff' : 'transparent',
                  color: gapTab === t.key ? 'var(--primary-color)' : 'var(--text-secondary)',
                  fontSize: '0.78rem',
                  fontWeight: gapTab === t.key ? 700 : 500,
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                  transition: 'all 0.15s ease',
                }}
              >
                {t.icon} {t.label}
              </button>
            ))}
          </div>
        )}

        {/* MA analysis tabs — only for ma-crossover strategy */}
        {isMa && data && !loading && (
          <div style={{
            display: 'flex',
            borderBottom: '1px solid var(--border-color)',
            background: '#f8fafc',
            flexShrink: 0,
            overflowX: 'auto',
          }}>
            {MA_TABS.map(t => (
              <button
                key={t.key}
                onClick={() => setMaTab(t.key)}
                style={{
                  flex: '0 0 auto',
                  padding: '8px 12px',
                  border: 'none',
                  borderBottom: maTab === t.key ? '2px solid var(--primary-color)' : '2px solid transparent',
                  background: maTab === t.key ? '#fff' : 'transparent',
                  color: maTab === t.key ? 'var(--primary-color)' : 'var(--text-secondary)',
                  fontSize: '0.78rem',
                  fontWeight: maTab === t.key ? 700 : 500,
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                  transition: 'all 0.15s ease',
                }}
              >
                {t.icon} {t.label}
              </button>
            ))}
          </div>
        )}

        {/* Fibonacci analysis tabs — only for fibonacci strategy */}
        {isFib && data && !loading && (
          <div style={{
            display: 'flex',
            borderBottom: '1px solid var(--border-color)',
            background: '#f8fafc',
            flexShrink: 0,
            overflowX: 'auto',
          }}>
            {FIB_TABS.map(t => (
              <button
                key={t.key}
                onClick={() => setFibTab(t.key)}
                style={{
                  flex: '0 0 auto',
                  padding: '8px 12px',
                  border: 'none',
                  borderBottom: fibTab === t.key ? '2px solid var(--primary-color)' : '2px solid transparent',
                  background: fibTab === t.key ? '#fff' : 'transparent',
                  color: fibTab === t.key ? 'var(--primary-color)' : 'var(--text-secondary)',
                  fontSize: '0.78rem',
                  fontWeight: fibTab === t.key ? 700 : 500,
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                  transition: 'all 0.15s ease',
                }}
              >
                {t.icon} {t.label}
              </button>
            ))}
          </div>
        )}

        {/* Scrollable results area */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '0' }}>
          {/* Loading */}
          {loading && (
            <div style={{ padding: '3rem 1rem', textAlign: 'center', color: 'var(--text-secondary)' }}>
              <div className="spinner" style={{ margin: '0 auto 12px' }}></div>
              <div style={{ fontSize: '0.82rem' }}>Analyzing {days}-day consistency...</div>
              <div style={{ fontSize: '0.75rem', marginTop: 4 }}>This may take 15-40 seconds</div>
            </div>
          )}

          {/* No data yet */}
          {!data && !loading && (
            <div style={{ padding: '3rem 1rem', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
              Select the number of past trading days and click <strong>Analyze</strong> to find tickers with consistent signals.
            </div>
          )}

          {/* Results */}
          {data && !loading && (
            isGaps ? renderGapTabContent() : isMa ? renderMaTabContent() : isFib ? renderFibTabContent() : renderOverviewTab()
          )}
        </div>
      </div>
    </>
  )
}

export default StreakPanel
