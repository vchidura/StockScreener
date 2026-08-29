import { useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ExternalLink, RefreshCw, Search, X } from 'lucide-react'
import { Link, useNavigate } from 'react-router-dom'
import { CrossFramePatternSummary, FormingChartPattern, PriceChannel, getTickers, scanChartPatterns, scanTickerChartPatterns } from '../services/api'
import { formingPatternRead } from '../utils/formingPatterns'

const colors = {
  ink: '#172033', muted: '#667085', line: '#d8dee8', panel: '#ffffff',
  canvas: '#f5f7fa', green: '#147d64', greenSoft: '#e7f5f0',
  red: '#bd3c3c', redSoft: '#faeceb', amber: '#9a6700', amberSoft: '#fff4d6',
  blue: '#245f9e', blueSoft: '#eaf2fb',
}

const intervalLabels: Record<string, string> = {
  all: 'All intervals',
  '5m': '5 minutes', '15m': '15 minutes',
  '30m': '30 minutes', '1h': 'Hourly', '1d': 'Daily', '1wk': 'Weekly',
}

const patternLabels: Record<FormingChartPattern['type'], string> = {
  ASCENDING_TRIANGLE: 'Ascending triangle',
  DESCENDING_TRIANGLE: 'Descending triangle',
  SYMMETRICAL_TRIANGLE: 'Symmetrical triangle',
  RISING_WEDGE: 'Rising wedge',
  FALLING_WEDGE: 'Falling wedge',
  BULL_PENNANT: 'Bull pennant',
  BEAR_PENNANT: 'Bear pennant',
  BULL_FLAG: 'Bull flag',
  BEAR_FLAG: 'Bear flag',
  CUP_AND_HANDLE: 'Cup and handle',
  HEAD_AND_SHOULDERS: 'Head and shoulders',
  INVERSE_HEAD_AND_SHOULDERS: 'Inverse head and shoulders',
  TRIPLE_TOP: 'Triple top',
  TRIPLE_BOTTOM: 'Triple bottom',
}

const biasTone = (bias: FormingChartPattern['bias']) => (
  bias === 'BULLISH' ? colors.green : bias === 'BEARISH' ? colors.red : colors.blue
)

const numberOrDash = (value: number | null, suffix = '') => (
  value === null || value === undefined || !Number.isFinite(value) ? '—' : `${value.toFixed(0)}${suffix}`
)

const edgePercent = (value: number | null | undefined) => (
  value === null || value === undefined || !Number.isFinite(value) ? '—' : `${value.toFixed(2)}%`
)

const money = (value: number | null) => (
  value === null || value === undefined || !Number.isFinite(value) ? '—' : `$${value.toFixed(2)}`
)

const readinessView = (pattern: FormingChartPattern) => {
  if (pattern.readiness === 'AT_EDGE') {
    return { label: 'At edge', color: colors.amber, background: colors.amberSoft }
  }
  if (pattern.readiness === 'NEAR_EDGE') {
    return { label: 'Near edge', color: colors.blue, background: colors.blueSoft }
  }
  return { label: 'Forming', color: colors.muted, background: colors.canvas }
}

const channelView = (pattern: FormingChartPattern, channel: PriceChannel | null) => {
  if (!channel) return null
  const relation = pattern.bias === 'NEUTRAL'
    ? 'CONTEXT' : pattern.bias === channel.bias ? 'ALIGNED' : 'OPPOSING'
  const position = channel.position === 'NEAR_SUPPORT'
    ? `Near support ${money(channel.support_price)}`
    : channel.position === 'NEAR_RESISTANCE'
      ? `Near resistance ${money(channel.resistance_price)}`
      : `Mid-channel ${money(channel.support_price)}–${money(channel.resistance_price)}`
  return {
    relation,
    relationLabel: relation === 'ALIGNED' ? 'Aligned context' : relation === 'OPPOSING' ? 'Opposing context' : 'Channel context',
    relationColor: relation === 'ALIGNED' ? colors.green : relation === 'OPPOSING' ? colors.amber : colors.blue,
    position,
  }
}

const crossFrameRead = (summary: CrossFramePatternSummary | null | undefined) => {
  if (!summary) return 'No directional cross-frame reading is available.'
  if (summary.state === 'ALIGNED_BULLISH' || summary.state === 'ALIGNED_BEARISH') {
    return `${summary.state === 'ALIGNED_BULLISH' ? 'Aligned bullish' : 'Aligned bearish'} across ${summary.directional_frames} timeframes; the highest active frame leads.`
  }
  if (summary.state === 'COUNTERTREND') {
    const highest = summary.frames.find(frame => frame.bias === summary.dominant_bias)
    return `Countertrend: ${highest ? intervalLabels[highest.interval] : 'the higher frame'} is ${summary.dominant_bias?.toLowerCase()}, while lower frames oppose it.`
  }
  if (summary.state === 'MIXED') return 'Mixed: opposing frame biases are present; do not count them as confirmation.'
  if (summary.state === 'SINGLE_FRAME') return 'Single frame only; there is no cross-frame confirmation yet.'
  return 'Neutral: no directional agreement is present across frames.'
}

export default function PatternWatch() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [interval, setInterval] = useState('1d')
  const [patternType, setPatternType] = useState('all')
  const [readinessFilter, setReadinessFilter] = useState('all')
  const [channelFilter, setChannelFilter] = useState('all')
  const [sector, setSector] = useState('all')
  const [search, setSearch] = useState('')
  const normalizedSearch = search.trim().toUpperCase()

  const tickerUniverse = useQuery({
    queryKey: ['tickers'],
    queryFn: () => getTickers(),
    staleTime: 86_400_000,
  })
  const exactTicker = tickerUniverse.data?.includes(normalizedSearch) ? normalizedSearch : ''
  const allTicker = interval === 'all' ? exactTicker : ''
  const awaitingAllTicker = interval === 'all' && !allTicker

  const queryKey = ['chart-patterns', 'scan', interval, allTicker]
  const patterns = useQuery({
    queryKey,
    queryFn: () => interval === 'all'
      ? scanTickerChartPatterns(allTicker)
      : scanChartPatterns(interval),
    enabled: !awaitingAllTicker,
    staleTime: interval === '1d' || interval === '1wk' ? 300_000 : 60_000,
  })

  const sectors = useMemo(() => (
    [...new Set((patterns.data?.results ?? []).map(row => row.sector).filter((value): value is string => !!value))]
      .sort((a, b) => a.localeCompare(b))
  ), [patterns.data])
  const rows = (patterns.data?.results ?? []).filter(row => {
    const term = search.trim().toUpperCase()
    const channel = row.channel ?? null
    const context = channelView(row.pattern, channel)
    const matchesChannel = channelFilter === 'all'
      || (channelFilter === 'has' && channel !== null)
      || (channelFilter === 'none' && channel === null)
      || (channelFilter === 'aligned' && context?.relation === 'ALIGNED')
      || (channelFilter === 'opposing' && context?.relation === 'OPPOSING')
      || (channelFilter === 'near_support' && channel?.position === 'NEAR_SUPPORT')
      || (channelFilter === 'near_resistance' && channel?.position === 'NEAR_RESISTANCE')
    return (!term || row.ticker.includes(term) || row.pattern.name.toUpperCase().includes(term))
      && (patternType === 'all' || row.pattern.type === patternType)
      && (readinessFilter === 'all' || row.pattern.readiness === readinessFilter)
      && matchesChannel
      && (sector === 'all' || (sector === 'unclassified' ? !row.sector : row.sector === sector))
  })
  const visibleTickers = new Set(rows.map(row => row.ticker)).size

  const refresh = async () => {
    if (awaitingAllTicker) return
    queryClient.removeQueries({ queryKey, exact: true })
    await queryClient.fetchQuery({
      queryKey,
      queryFn: () => interval === 'all'
        ? scanTickerChartPatterns(allTicker, true)
        : scanChartPatterns(interval, true),
    })
  }
  const patternUrl = (ticker: string, pattern: FormingChartPattern, rowInterval: string, channel: PriceChannel | null) => {
    const params = new URLSearchParams({ interval: rowInterval, patterns: 'on', pattern: pattern.type })
    if (channel) params.set('channel', 'on')
    return `/ticker/${ticker}?${params.toString()}`
  }
  const openPattern = (ticker: string, pattern: FormingChartPattern, rowInterval: string, channel: PriceChannel | null) => (
    navigate(patternUrl(ticker, pattern, rowInterval, channel))
  )

  const control: React.CSSProperties = {
    height: 34, border: `1px solid ${colors.line}`, borderRadius: 5,
    background: '#fff', color: colors.ink, padding: '0 9px', fontSize: 12.5,
  }

  return (
    <div style={{ color: colors.ink }}>
      <header style={{ padding: '10px 2px 16px', borderBottom: `1px solid ${colors.line}`, marginBottom: 14 }}>
        <div style={{ color: colors.blue, fontSize: 11, fontWeight: 700, textTransform: 'uppercase' }}>Chart research · forming geometry</div>
        <h1 style={{ fontSize: 25, lineHeight: 1.15, margin: '4px 0 5px', letterSpacing: 0 }}>Pattern Watch</h1>
        <p style={{ margin: 0, color: colors.muted, fontSize: 13.5 }}>
          Current unconfirmed formations measured from completed candles. Open a candidate to inspect its automatic chart lines; these are not scanner signals or recommendations.
        </p>
      </header>

      <section style={{ background: colors.panel, border: `1px solid ${colors.line}`, borderRadius: 7, marginBottom: 14 }}>
        <div style={{ padding: 12, display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap', borderBottom: `1px solid ${colors.line}` }}>
          <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
            <Search size={14} aria-hidden="true" style={{ position: 'absolute', left: 9, color: exactTicker ? colors.green : colors.muted }} />
            <input aria-label="Search forming patterns" value={search} onChange={event => setSearch(event.target.value)} placeholder="Ticker or pattern" style={{ ...control, width: 170, paddingLeft: 29, paddingRight: search ? 30 : 9, borderColor: exactTicker ? colors.green : colors.line }} />
            {search && (
              <button
                type="button"
                aria-label="Clear pattern search"
                title="Clear search"
                onClick={() => setSearch('')}
                style={{ position: 'absolute', right: 3, width: 26, height: 28, display: 'grid', placeItems: 'center', border: 0, background: 'transparent', color: colors.muted, cursor: 'pointer', padding: 0 }}
              >
                <X size={14} aria-hidden="true" />
              </button>
            )}
          </div>
          <select aria-label="Pattern interval" value={interval} onChange={event => { setInterval(event.target.value); setPatternType('all'); setReadinessFilter('all'); setChannelFilter('all'); setSector('all') }} style={control}>
            {Object.entries(intervalLabels).map(([value, label]) => (
              <option key={value} value={value} disabled={value === 'all' && !exactTicker}>
                {label}{value === 'all' && !exactTicker ? ' · enter ticker first' : ''}
              </option>
            ))}
          </select>
          <select aria-label="Pattern type" value={patternType} onChange={event => setPatternType(event.target.value)} style={control}>
            <option value="all">All pattern types</option>
            {Object.entries(patternLabels)
              .sort((left, right) => left[1].localeCompare(right[1]))
              .map(([value, label]) => <option key={value} value={value}>{label}</option>)}
          </select>
          <select aria-label="Pattern readiness" value={readinessFilter} onChange={event => setReadinessFilter(event.target.value)} style={control}>
            <option value="all">All readiness</option>
            <option value="AT_EDGE">At edge</option>
            <option value="NEAR_EDGE">Near edge</option>
            <option value="FORMING">Forming</option>
          </select>
          <select aria-label="Price channel context" value={channelFilter} onChange={event => setChannelFilter(event.target.value)} style={control}>
            <option value="all">All channel context</option>
            <option value="has">Has price channel</option>
            <option value="aligned">Aligned context</option>
            <option value="opposing">Opposing context</option>
            <option value="near_support">Channel near support</option>
            <option value="near_resistance">Channel near resistance</option>
            <option value="none">No price channel</option>
          </select>
          <select aria-label="Pattern sector" value={sector} onChange={event => setSector(event.target.value)} style={{ ...control, maxWidth: 220 }}>
            <option value="all">All sectors</option>
            {sectors.map(value => <option key={value} value={value}>{value}</option>)}
            <option value="unclassified">Unclassified / ETF</option>
          </select>
          <button type="button" onClick={refresh} disabled={patterns.isFetching || awaitingAllTicker} title="Refresh forming patterns" style={{ ...control, display: 'inline-flex', alignItems: 'center', gap: 6, cursor: 'pointer', color: colors.blue }}>
            <RefreshCw size={14} aria-hidden="true" />
            {patterns.isFetching ? 'Measuring…' : 'Refresh'}
          </button>
          <span style={{ marginLeft: 'auto', color: colors.muted, fontSize: 11.5, whiteSpace: 'nowrap' }}>
            {patterns.data
              ? `${rows.length} candidates · ${visibleTickers} tickers shown · ${patterns.data.matched_tickers}/${patterns.data.scanned} matched`
              : awaitingAllTicker ? 'Enter an exact active ticker' : 'Loading universe…'}
          </span>
        </div>

        <div style={{ padding: '7px 12px', background: colors.amberSoft, color: colors.amber, fontSize: 11, borderBottom: `1px solid ${colors.line}` }}>
          {interval === 'all'
            ? `Cross-frame · ${crossFrameRead(patterns.data?.cross_frame)} Rows remain ordered by readiness; react only after a completed boundary break.`
            : 'Ordered by readiness: At edge → Near edge → Forming, then geometry quality, boundary distance, and touches. React only after a completed close confirms the boundary break.'}
        </div>

        <div style={{ overflowX: 'auto', maxHeight: 690, overflowY: 'auto' }}>
          <table style={{ width: '100%', minWidth: 1510, borderCollapse: 'collapse', fontSize: 12 }}>
            <thead style={{ position: 'sticky', top: 0, zIndex: 1 }}>
              <tr style={{ background: colors.canvas, color: colors.muted }}>
                {['Ticker', 'Sector', 'Pattern', 'Frame', 'Bias', 'Readiness', 'Brief read', 'Price channel', 'Geometry', 'Touches', 'Contraction', 'Apex', 'Age', 'Analyzed close', ''].map(label => (
                  <th key={label} style={{ textAlign: ['Ticker', 'Sector', 'Pattern', 'Frame', 'Bias', 'Readiness', 'Brief read', 'Price channel', 'Geometry'].includes(label) ? 'left' : 'right', padding: '8px 9px', borderBottom: `1px solid ${colors.line}`, whiteSpace: 'nowrap' }}>{label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {awaitingAllTicker && <tr><td colSpan={15} style={{ padding: 18, color: colors.muted }}>Enter an exact active ticker to compare its forming patterns across all intervals.</td></tr>}
              {!awaitingAllTicker && patterns.isLoading && <tr><td colSpan={15} style={{ padding: 18, color: colors.muted }}>Measuring the active universe…</td></tr>}
              {patterns.isError && <tr><td colSpan={15} style={{ padding: 18, color: colors.red }}>Pattern Watch could not be loaded.</td></tr>}
              {!awaitingAllTicker && !patterns.isLoading && !patterns.isError && rows.length === 0 && (
                <tr><td colSpan={15} style={{ padding: 18, color: colors.muted }}>No forming candidates match this interval and filter set.</td></tr>
              )}
              {rows.map(row => {
                const pattern = row.pattern
                const read = formingPatternRead(pattern)
                const readiness = readinessView(pattern)
                const channel = row.channel ?? null
                const channelContext = channelView(pattern, channel)
                return (
                  <tr key={`${row.ticker}-${row.interval}-${pattern.type}-${pattern.start_time}`} style={{ borderBottom: `1px solid ${colors.line}` }}>
                    <td style={{ padding: '8px 9px' }}>
                      <Link
                        to={patternUrl(row.ticker, pattern, row.interval, channel)}
                        title={`Open ${row.ticker} ${pattern.name} chart`}
                        style={{ color: colors.blue, fontWeight: 800, textDecoration: 'none' }}
                      >
                        {row.ticker}
                      </Link>
                    </td>
                    <td style={{ padding: '8px 9px', color: colors.muted, maxWidth: 180 }}>{row.sector ?? 'Unclassified / ETF'}</td>
                    <td style={{ padding: '8px 9px', fontWeight: 700 }}>{pattern.name}</td>
                    <td style={{ padding: '8px 9px', color: colors.blue, fontWeight: 700, whiteSpace: 'nowrap' }}>{intervalLabels[row.interval] ?? row.interval}</td>
                    <td style={{ padding: '8px 9px', color: biasTone(pattern.bias), fontWeight: 700 }}>{pattern.bias.toLowerCase()}</td>
                    <td style={{ padding: '8px 9px', minWidth: 175 }}>
                      <span style={{ padding: '2px 5px', borderRadius: 4, color: readiness.color, background: readiness.background, fontWeight: 700 }}>
                        {readiness.label}
                      </span>
                      <div style={{ color: colors.muted, fontSize: 10.5, marginTop: 3 }}>
                        {pattern.edge_distance_atr?.toFixed(2) ?? '—'} ATR ({edgePercent(pattern.edge_distance_pct)}) to {pattern.boundary_role ?? 'boundary'} {money(pattern.boundary_price)}
                      </div>
                    </td>
                    <td style={{ padding: '8px 9px', minWidth: 235, lineHeight: 1.35 }}>
                      <div>{read.watch}</div>
                      <div style={{ color: colors.muted, fontSize: 10.5, marginTop: 2 }}>{read.outcome}</div>
                      <div style={{ color: colors.muted, fontSize: 10.5, marginTop: 2 }}>{read.invalidation} · then remove from watch</div>
                    </td>
                    <td style={{ padding: '8px 9px', minWidth: 190, lineHeight: 1.35 }}>
                      {channel && channelContext ? (
                        <>
                          <div style={{ fontWeight: 700 }}>{channel.name}</div>
                          <div style={{ color: colors.muted, fontSize: 10.5 }}>{channelContext.position}</div>
                          <div style={{ color: channelContext.relationColor, fontSize: 10.5, fontWeight: 700 }}>{channelContext.relationLabel}</div>
                        </>
                      ) : <span style={{ color: colors.muted }}>—</span>}
                    </td>
                    <td style={{ padding: '8px 9px' }}>
                      <span style={{ padding: '2px 5px', borderRadius: 4, background: pattern.grade === 'STRONG_GEOMETRY' ? colors.greenSoft : colors.blueSoft, color: pattern.grade === 'STRONG_GEOMETRY' ? colors.green : colors.blue, fontWeight: 700 }}>
                        {pattern.grade === 'STRONG_GEOMETRY' ? 'Strong' : 'Valid'}
                      </span>
                    </td>
                    <td style={{ padding: '8px 9px', textAlign: 'right' }}>{pattern.upper_touches + pattern.lower_touches}</td>
                    <td style={{ padding: '8px 9px', textAlign: 'right' }}>{numberOrDash(pattern.contraction_pct, '%')}</td>
                    <td style={{ padding: '8px 9px', textAlign: 'right' }}>{numberOrDash(pattern.apex_bars_ahead, ' bars')}</td>
                    <td style={{ padding: '8px 9px', textAlign: 'right' }}>{pattern.formation_bars} bars</td>
                    <td style={{ padding: '8px 9px', textAlign: 'right', fontWeight: 700 }}>{money(row.last_close)}</td>
                    <td style={{ padding: '8px 9px', textAlign: 'right' }}>
                      <button type="button" onClick={() => openPattern(row.ticker, pattern, row.interval, channel)} title={`Open ${row.ticker} ${pattern.name} chart`} aria-label={`Open ${row.ticker} ${pattern.name} chart`} style={{ border: 0, background: 'transparent', color: colors.blue, cursor: 'pointer', display: 'inline-grid', placeItems: 'center' }}>
                        <ExternalLink size={16} aria-hidden="true" />
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}