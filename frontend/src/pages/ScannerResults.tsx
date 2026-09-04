import { useMemo, useState } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { NavLink, useLocation, useNavigate } from 'react-router-dom'
import { BarChart3, Radar, X } from 'lucide-react'
import {
  getScannerEventBacklog,
  getLatestScannerSignals,
  getScannerQualification,
  ScannerQualificationRow,
  ScannerInterval,
} from '../services/api'

const colors = {
  ink: '#172033', muted: '#667085', line: '#d8dee8', panel: '#ffffff',
  canvas: '#f5f7fa', green: '#147d64', greenSoft: '#e7f5f0',
  red: '#bd3c3c', redSoft: '#faeceb', amber: '#9a6700', amberSoft: '#fff4d6',
  blue: '#245f9e', blueSoft: '#eaf2fb',
}

const scannerLabels: Record<string, string> = {
  breakout_expansion: 'Breakout expansion',
  compression_breakout: 'Compression breakout',
  failed_breakout_reversal: 'Failed-breakout reversal',
  level_retest_rejection: 'Level retest / rejection',
  sma200_reclaim_rejection: 'SMA200 reclaim rejection',
  structure_reversal: 'Structure reversal',
  structured_trend_pullback: 'Structured trend pullback',
}

const researchScannerLabels: Record<string, string> = {
  ...scannerLabels,
  gap_breakaway_hold: 'Gap breakaway hold',
  gap_breakaway_confirmation: 'Gap breakaway confirmation',
  gap_continuation_hold: 'Gap continuation hold',
  gap_entry_fill: 'Gap entry fill',
  gap_fade_reversal: 'Gap fade reversal',
  ma_crossover_9_21: 'MA crossover 9/21',
  momentum_pullback: 'Momentum pullback',
  bearish_bounce: 'Bearish bounce',
}

const scannerMeta: Record<string, { direction: string; description: string }> = {
  structured_trend_pullback: {
    direction: 'Long or short, with trend',
    description: 'A pullback inside an established SMA20/SMA50 trend that finds swing support (longs) or resistance (shorts), then triggers a continuation candle back in the trend direction.',
  },
  level_retest_rejection: {
    direction: 'Long or short',
    description: 'Price retests a pre-existing structural level (swing high/low, gap, fair-value gap, or Fibonacci retracement) and rejects it with a directional candle plus volume/range participation.',
  },
  breakout_expansion: {
    direction: 'Long or short',
    description: 'A fresh, wide-range close beyond a prior confirmed swing high/low on expanding volume — a breakout through structure, not a retest.',
  },
  compression_breakout: {
    direction: 'Long or short',
    description: 'An expansion close out of a contracting ten-bar price channel — a low-volatility squeeze releasing in one direction.',
  },
  failed_breakout_reversal: {
    direction: 'Long or short, fading the break',
    description: 'A breakout beyond a confirmed swing level fails within the next bar and closes back inside the level — trades the reversal of a failed breakout, not the breakout itself.',
  },
  sma200_reclaim_rejection: {
    direction: 'Short only',
    description: 'After price reclaims the 200-day SMA from below, it stalls near old resistance and prints a bearish strong-close rejection — a short against a reclaim attempt that is losing conviction.',
  },
  structure_reversal: {
    direction: 'Long or short, against prior trend',
    description: 'Trend structure flips: price reclaims EMA21 against the prior established trend, with a higher swing low (bull) or lower swing high (bear) plus a reversal candle — an early trend-change signal.',
  },
}

const pct = (value: number | null, digits = 2) =>
  value == null ? '—' : `${(value * 100).toFixed(digits)}%`

const compact = (value: number) => new Intl.NumberFormat('en-US', { notation: 'compact' }).format(value)

const money = (value: number | null) => value == null
  ? '—'
  : new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value)

const intervalLabel = (interval: ScannerInterval) => (
  interval === '1d' ? 'Daily' : interval === '1wk' ? 'Weekly' : interval === '1h' ? 'Hourly' : '30 minute'
)

const intraday = (interval: ScannerInterval) => interval === '1h' || interval === '30m'

const horizonLabel = (interval: ScannerInterval, horizon: number) => (
  `${horizon} ${intraday(interval) ? (horizon === 1 ? 'bar' : 'bars') : (horizon === 1 ? 'session' : 'sessions')}`
)

const signalTime = (value: string, interval: ScannerInterval) => new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York', month: 'short', day: 'numeric', year: 'numeric',
  ...(intraday(interval) ? { hour: 'numeric', minute: '2-digit' } as const : {}),
}).format(new Date(value))

const reviewPriorityBadge = (tier: 'HIGHER' | 'STANDARD' | 'LOWER' | 'UNRANKED') => {
  if (tier === 'HIGHER') return { label: 'Higher', color: colors.green, bg: colors.greenSoft }
  if (tier === 'LOWER') return { label: 'Lower', color: colors.red, bg: colors.redSoft }
  if (tier === 'STANDARD') return { label: 'Standard', color: colors.blue, bg: colors.blueSoft }
  return { label: 'Unranked', color: colors.muted, bg: colors.canvas }
}

const evidenceBadge = (state: ScannerQualificationRow['evidence_status']) => {
  if (state === 'ROBUST_PASS') return { label: 'Robust', color: colors.green, bg: colors.greenSoft }
  if (state === 'MONITOR_ONLY') return { label: 'Monitor', color: colors.amber, bg: colors.amberSoft }
  return { label: 'Unranked', color: colors.muted, bg: colors.canvas }
}

export default function ScannerResults() {
  const navigate = useNavigate()
  const location = useLocation()
  const researchView = location.pathname.endsWith('/research')
  const [signalInterval, setSignalInterval] = useState<'all' | ScannerInterval>('all')
  const [signalSide, setSignalSide] = useState<'all' | 'long' | 'short'>('all')
  const [signalEvidence, setSignalEvidence] = useState<'all' | 'robust' | 'monitor' | 'unranked'>('all')
  const [signalPriority, setSignalPriority] = useState<'all' | 'HIGHER' | 'STANDARD' | 'LOWER' | 'UNRANKED'>('all')
  const [signalSector, setSignalSector] = useState('all')
  const [signalScanner, setSignalScanner] = useState('all')
  const [signalSearch, setSignalSearch] = useState('')
  const [researchInterval, setResearchInterval] = useState<ScannerInterval>('1d')
  const [researchEvidence, setResearchEvidence] = useState<'all' | 'ROBUST_PASS' | 'MONITOR_ONLY' | 'UNRANKED'>('all')
  const [researchScanner, setResearchScanner] = useState('all')

  const qualification = useQuery({
    queryKey: ['scanner-qualification'],
    queryFn: () => getScannerQualification(),
  })
  const backlog = useQuery({
    queryKey: ['scanner-events', 'backlog'],
    queryFn: () => getScannerEventBacklog(),
    enabled: researchView,
  })
  const latestSignals = useQuery({
    queryKey: ['scanner-events', 'latest-by-ticker', signalInterval],
    queryFn: () => getLatestScannerSignals(signalInterval === 'all' ? undefined : signalInterval),
    placeholderData: keepPreviousData,
    enabled: !researchView,
  })

  const allRows = qualification.data?.results ?? []
  const robustRows = allRows.filter(row => row.evidence_status === 'ROBUST_PASS')
  const monitorRows = allRows.filter(row => row.evidence_status === 'MONITOR_ONLY')
  const robustKeys = new Set(robustRows.map(row => `${row.scanner_name}|${row.interval}|${row.direction}`))
  const monitorKeys = new Set(monitorRows.map(row => `${row.scanner_name}|${row.interval}|${row.direction}`))
  const sectorOptions = useMemo(() => {
    const set = new Set<string>()
    for (const row of latestSignals.data?.results ?? []) if (row.sector) set.add(row.sector)
    return Array.from(set).sort()
  }, [latestSignals.data])
  const latestRows = (latestSignals.data?.results ?? []).filter(row => {
    const evidenceKey = `${row.scanner_name}|${row.interval}|${row.direction}`
    const evidence = robustKeys.has(evidenceKey) ? 'robust' : monitorKeys.has(evidenceKey) ? 'monitor' : 'unranked'
    const search = signalSearch.trim().toUpperCase()
    // Ticker matches by prefix (MU should not surface TMUS); scanner name still matches anywhere.
    const matchesSearch = !search || row.ticker.startsWith(search) || row.scanner_name.toUpperCase().includes(search)
    return matchesSearch
      && (signalSide === 'all' || (signalSide === 'long' ? row.direction === 1 : row.direction === -1))
      && (signalEvidence === 'all' || signalEvidence === evidence)
      && (signalPriority === 'all' || row.review_priority_tier === signalPriority)
      && (signalSector === 'all' || (signalSector === 'unclassified' ? !row.sector : row.sector === signalSector))
      && (signalScanner === 'all' || row.scanner_name === signalScanner)
  })
  const totalEvents = allRows.reduce((sum, row) => sum + row.events, 0)
  const pending = (backlog.data?.results ?? []).reduce((sum, row) => sum + row.pending, 0)
  const researchRows = allRows.filter(row => (
    row.interval === researchInterval
    && (researchEvidence === 'all' || row.evidence_status === researchEvidence)
    && (researchScanner === 'all' || row.scanner_name === researchScanner)
  ))
  const researchHorizons = Array.from(new Set(
    allRows.filter(row => row.interval === researchInterval).map(row => row.horizon_bars),
  )).sort((left, right) => left - right)
  const researchGroups = Array.from(researchRows.reduce((groups, row) => {
    const key = `${row.scanner_name}|${row.scanner_version}|${row.direction}|${row.return_mode}`
    const group = groups.get(key) ?? {
      scannerName: row.scanner_name,
      scannerVersion: row.scanner_version,
      direction: row.direction,
      returnMode: row.return_mode,
      rows: [] as ScannerQualificationRow[],
    }
    group.rows.push(row)
    groups.set(key, group)
    return groups
  }, new Map<string, {
    scannerName: string
    scannerVersion: string
    direction: 1 | -1
    returnMode: ScannerQualificationRow['return_mode']
    rows: ScannerQualificationRow[]
  }>()).values())
  const researchScanners = Array.from(new Set(allRows.map(row => row.scanner_name))).sort()
  const qualifiedPeriods = allRows.filter(row => row.events >= (qualification.data?.gates.minimum_events ?? 100)
    && row.independent_periods >= (qualification.data?.gates.minimum_independent_periods ?? 40)).length

  const panel: React.CSSProperties = {
    background: colors.panel, border: `1px solid ${colors.line}`, borderRadius: 7,
  }
  const control: React.CSSProperties = {
    border: `1px solid ${colors.line}`, borderRadius: 5, background: '#fff',
    color: colors.ink, padding: '7px 10px', fontSize: 13,
  }

  const loading = researchView ? qualification.isLoading || backlog.isLoading : latestSignals.isLoading
  const errored = researchView ? qualification.isError || backlog.isError : latestSignals.isError

  if (loading) return <div style={{ padding: 24, color: colors.muted }}>Loading stock research…</div>
  if (errored) return <div style={{ padding: 24, color: colors.red }}>Stock research could not be loaded.</div>

  return (
    <div style={{ color: colors.ink, opacity: latestSignals.isFetching || qualification.isFetching ? 0.6 : 1, transition: 'opacity 0.15s' }}>
      <header style={{ padding: '10px 2px 18px', borderBottom: `1px solid ${colors.line}`, marginBottom: 16 }}>
        <div style={{ color: colors.blue, fontSize: 12, fontWeight: 700, textTransform: 'uppercase' }}>Equity signal research</div>
        <h1 style={{ fontSize: 26, lineHeight: 1.15, margin: '4px 0 5px', letterSpacing: 0 }}>Stock Research</h1>
        <p style={{ margin: 0, color: colors.muted, fontSize: 14 }}>
          Review current scanner opportunities separately from historical outcome evidence and statistical qualification.
        </p>
      </header>

      <nav aria-label="Stock research views" style={{ display: 'flex', gap: 4, borderBottom: `1px solid ${colors.line}`, marginBottom: 16 }}>
        {[
          { to: '/stock-research', label: 'Opportunity Board', icon: Radar, end: true },
          { to: '/stock-research/research', label: 'Research', icon: BarChart3, end: false },
        ].map(item => {
          const Icon = item.icon
          return <NavLink key={item.to} to={item.to} end={item.end} style={({ isActive }) => ({
            display: 'inline-flex', alignItems: 'center', gap: 7, padding: '10px 14px',
            color: isActive ? colors.blue : colors.muted, fontSize: 13, fontWeight: 750,
            textDecoration: 'none', borderBottom: `2px solid ${isActive ? colors.blue : 'transparent'}`,
            marginBottom: -1,
          })}><Icon size={15} aria-hidden="true" />{item.label}</NavLink>
        })}
      </nav>

      {researchView ? <>
      <section style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(175px, 1fr))', gap: 10, marginBottom: 16 }}>
        {[
          ['Combinations', allRows.length.toString(), 'scanner × side × horizon'],
          ['Robust scanners', robustRows.length.toString(), 'after family-wise FDR'],
          ['Monitor only', monitorRows.length.toString(), 'raw pass; not robust'],
          ['Sample-ready', qualifiedPeriods.toString(), 'event and independent-period floors'],
          ['Event outcomes', compact(totalEvents), 'across all combinations'],
          ['Pending', compact(pending), pending === 0 ? 'all due outcomes complete' : 'future bars required'],
        ].map(([label, value, detail]) => (
          <div key={label} style={{ ...panel, padding: '13px 14px' }}>
            <div style={{ color: colors.muted, fontSize: 11, textTransform: 'uppercase', fontWeight: 700 }}>{label}</div>
            <div style={{ fontSize: 24, lineHeight: 1.2, fontWeight: 750, marginTop: 4 }}>{value}</div>
            <div style={{ color: colors.muted, fontSize: 11, marginTop: 2 }}>{detail}</div>
          </div>
        ))}
      </section>

      <section style={{ ...panel, marginBottom: 16 }}>
        <div style={{ padding: '12px 14px', borderBottom: `1px solid ${colors.line}`, display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <h2 style={{ fontSize: 16, margin: 0, letterSpacing: 0 }}>Historical qualification matrix</h2>
            <div style={{ color: colors.muted, fontSize: 11, marginTop: 2 }}>One scanner, side and return mode per row, with frame-specific horizons kept separate. Returns are net of the recorded cost model and sector-primary benchmark.</div>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <select aria-label="Research interval" value={researchInterval} onChange={event => setResearchInterval(event.target.value as ScannerInterval)} style={control}>
              <option value="1d">Daily frame</option><option value="1wk">Weekly frame</option><option value="1h">Hourly frame</option><option value="30m">30 minute frame</option>
            </select>
            <select aria-label="Research scanner" value={researchScanner} onChange={event => setResearchScanner(event.target.value)} style={control}>
              <option value="all">All scanners</option>
              {researchScanners.map(name => <option key={name} value={name}>{researchScannerLabels[name] ?? name.replace(/_/g, ' ')}</option>)}
            </select>
            <select aria-label="Research evidence" value={researchEvidence} onChange={event => setResearchEvidence(event.target.value as typeof researchEvidence)} style={control}>
              <option value="all">All evidence states</option><option value="ROBUST_PASS">Robust</option><option value="MONITOR_ONLY">Monitor only</option><option value="UNRANKED">Unranked</option>
            </select>
          </div>
        </div>
        <div style={{ overflowX: 'auto', maxHeight: 690, overflowY: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead style={{ position: 'sticky', top: 0, zIndex: 1 }}><tr style={{ background: '#f8fafc', color: colors.muted }}>
              <th style={{ textAlign: 'left', padding: '9px 10px', whiteSpace: 'nowrap', borderBottom: `1px solid ${colors.line}` }}>Scanner</th>
              <th style={{ textAlign: 'left', padding: '9px 10px', whiteSpace: 'nowrap', borderBottom: `1px solid ${colors.line}` }}>Side</th>
              <th style={{ textAlign: 'left', padding: '9px 10px', whiteSpace: 'nowrap', borderBottom: `1px solid ${colors.line}` }}>Return</th>
              {researchHorizons.map(horizon => <th key={horizon} style={{ textAlign: 'left', padding: '9px 10px', minWidth: 205, whiteSpace: 'nowrap', borderBottom: `1px solid ${colors.line}` }}>{horizonLabel(researchInterval, horizon)}</th>)}
            </tr></thead>
            <tbody>
              {researchGroups.length === 0 && <tr><td colSpan={3 + researchHorizons.length} style={{ padding: 18, color: colors.muted }}>No historical combinations match these filters.</td></tr>}
              {researchGroups.map(group => <tr key={`${group.scannerName}-${group.scannerVersion}-${group.direction}-${group.returnMode}`} style={{ borderBottom: `1px solid ${colors.line}` }}>
                <td style={{ padding: '10px', minWidth: 180, verticalAlign: 'top' }}><strong>{researchScannerLabels[group.scannerName] ?? group.scannerName.replace(/_/g, ' ')}</strong><div style={{ color: colors.muted, fontSize: 10 }}>{group.scannerVersion}</div></td>
                <td style={{ padding: '10px', verticalAlign: 'top', color: group.direction === 1 ? colors.green : colors.red, fontWeight: 700 }}>{group.direction === 1 ? 'Long' : 'Short'}</td>
                <td style={{ padding: '10px', minWidth: 105, verticalAlign: 'top', color: colors.muted }}>{group.returnMode === 'RECOMMENDATION_PLAN' ? 'Stop / target plan' : 'Horizon close'}</td>
                {researchHorizons.map(horizon => {
                  const row = group.rows.find(item => item.horizon_bars === horizon)
                  if (!row) return <td key={horizon} style={{ padding: '10px', color: colors.muted, verticalAlign: 'top' }}>No matching result</td>
                  const badge = evidenceBadge(row.evidence_status)
                  return <td key={horizon} style={{ padding: '10px', verticalAlign: 'top' }}>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                      <strong style={{ color: (row.mean_net_alpha ?? 0) > 0 ? colors.green : colors.red }}>α {pct(row.mean_net_alpha)}</strong>
                      <span style={{ background: badge.bg, color: badge.color, borderRadius: 4, padding: '2px 5px', fontSize: 10, fontWeight: 700 }}>{badge.label}</span>
                    </div>
                    <div style={{ marginTop: 5 }}>Return {pct(row.mean_net_return)} · hit {pct(row.hit_rate, 1)}</div>
                    <div style={{ color: colors.muted, marginTop: 3 }}>{row.events.toLocaleString()} events · {row.independent_periods} periods</div>
                    <div style={{ color: colors.muted, marginTop: 3 }}>t {row.alpha_t_stat == null ? '—' : row.alpha_t_stat.toFixed(2)} · q {row.alpha_fdr_q == null ? '—' : row.alpha_fdr_q.toFixed(3)}</div>
                    <div style={{ color: colors.muted, marginTop: 3 }}>Early/late α {pct(row.early_alpha)} / {pct(row.late_alpha)}</div>
                    <div style={{ color: colors.muted, marginTop: 3 }}>MAE/MFE {pct(row.mean_mae_pct)} / {pct(row.mean_mfe_pct)}</div>
                    {(row.stop_hit_rate != null || row.target_hit_rate != null) && <div style={{ color: colors.muted, marginTop: 3 }}>Stop/target hit {pct(row.stop_hit_rate ?? null, 1)} / {pct(row.target_hit_rate ?? null, 1)}</div>}
                    {(
                      row.stop_hit_rate === undefined
                        ? row.stop_first_rate != null || row.target_first_rate != null
                        : row.stop_hit_rate != null || row.target_hit_rate != null
                    ) && <div style={{ color: colors.muted, marginTop: 3 }}>Stop/target first {pct(row.stop_first_rate, 1)} / {pct(row.target_first_rate, 1)}</div>}
                  </td>
                })}
              </tr>)}
            </tbody>
          </table>
        </div>
        <div style={{ padding: '8px 14px', color: colors.muted, fontSize: 11, borderTop: `1px solid ${colors.line}` }}>
          Showing {researchGroups.length} scanner/side rows containing {researchRows.length} of {allRows.length} combinations · {intervalLabel(researchInterval)} frame · entry model {qualification.data?.entry_model?.replace(/_/g, ' ')}.
        </div>
      </section>

      <section style={{ ...panel, padding: 14 }}>
        <h2 style={{ fontSize: 16, margin: '0 0 8px', letterSpacing: 0 }}>Qualification policy</h2>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(185px, 1fr))', gap: 10, color: colors.muted, fontSize: 12 }}>
          <div><strong style={{ color: colors.ink }}>Sample floors</strong><br />{qualification.data?.gates.minimum_events} events · {qualification.data?.gates.minimum_independent_periods} independent periods</div>
          <div><strong style={{ color: colors.ink }}>Alpha evidence</strong><br />t-stat &gt; {qualification.data?.gates.minimum_alpha_t_stat} · positive early and late halves</div>
          <div><strong style={{ color: colors.ink }}>Multiplicity</strong><br />FDR q ≤ {qualification.data?.gates.maximum_false_discovery_rate}</div>
          <div><strong style={{ color: colors.ink }}>Calibration</strong><br />{qualification.data?.gates.minimum_calibration_oos_periods} out-of-sample periods required</div>
        </div>
      </section>
      </> : <>

      <section style={{ ...panel, marginBottom: 16 }}>
        <div style={{ padding: '12px 14px', borderBottom: `1px solid ${colors.line}`, display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <h2 style={{ fontSize: 16, margin: 0, letterSpacing: 0 }}>
              Latest scanner signal by ticker
              {latestSignals.isFetching && <span style={{ color: colors.blue, fontSize: 11, fontWeight: 500, marginLeft: 8 }}>Updating…</span>}
            </h2>
            <div style={{ color: colors.muted, fontSize: 11, marginTop: 2 }}>
              One newest observed setup per matched ticker, from the last 10 daily/weekly sessions or 2 days of hourly bars. These are research signals, not recommendations; open a ticker for its complete evidence and outcomes.
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
              <input
                aria-label="Search latest scanner signals"
                value={signalSearch}
                onChange={event => setSignalSearch(event.target.value)}
                placeholder="Ticker or scanner"
                style={{ ...control, width: 150, paddingRight: signalSearch ? 32 : 10 }}
              />
              {signalSearch && (
                <button
                  type="button"
                  aria-label="Clear scanner search"
                  title="Clear search"
                  onClick={() => setSignalSearch('')}
                  style={{ position: 'absolute', right: 3, width: 28, height: 28, display: 'grid', placeItems: 'center', border: 0, background: 'transparent', color: colors.muted, cursor: 'pointer', padding: 0 }}
                >
                  <X size={14} aria-hidden="true" />
                </button>
              )}
            </div>
            <select aria-label="Signal interval" value={signalInterval} onChange={event => setSignalInterval(event.target.value as typeof signalInterval)} style={control}>
              <option value="all">Latest any interval</option><option value="1d">Latest daily</option><option value="1wk">Latest weekly</option><option value="1h">Latest hourly</option><option value="30m">Latest 30 minute</option>
            </select>
            <select aria-label="Signal setup" value={signalScanner} onChange={event => setSignalScanner(event.target.value)} style={control}>
              <option value="all">All setups</option>
              {Object.entries(scannerLabels).map(([name, label]) => <option key={name} value={name}>{label}</option>)}
            </select>
            <select aria-label="Signal side" value={signalSide} onChange={event => setSignalSide(event.target.value as typeof signalSide)} style={control}>
              <option value="all">Both sides</option><option value="long">Long</option><option value="short">Short</option>
            </select>
            <select aria-label="Signal sector" value={signalSector} onChange={event => setSignalSector(event.target.value)} style={control}>
              <option value="all">All sectors</option>
              {sectorOptions.map(sector => <option key={sector} value={sector}>{sector}</option>)}
              <option value="unclassified">Unclassified / ETF</option>
            </select>
            <select aria-label="Signal evidence" value={signalEvidence} onChange={event => setSignalEvidence(event.target.value as typeof signalEvidence)} style={control}>
              <option value="all">All evidence</option><option value="robust">Robust evidence</option><option value="monitor">Monitor only</option><option value="unranked">Unranked</option>
            </select>
            <select aria-label="Review priority" value={signalPriority} onChange={event => setSignalPriority(event.target.value as typeof signalPriority)} style={control}>
              <option value="all">All review priorities</option><option value="HIGHER">Higher priority</option><option value="STANDARD">Standard priority</option><option value="LOWER">Lower priority</option><option value="UNRANKED">Unranked</option>
            </select>
          </div>
        </div>
        <div style={{ padding: '8px 14px', background: colors.amberSoft, color: colors.amber, fontSize: 11, borderBottom: `1px solid ${colors.line}` }}>
          Hourly review priority reflects directional discovery-state alignment, not conviction or trade approval. Daily and weekly signals remain unranked. Planned levels use the signal close; next-bar open is the first no-look-ahead evaluation price.
        </div>
        <div style={{ overflowX: 'auto', maxHeight: 640, overflowY: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead style={{ position: 'sticky', top: 0, zIndex: 1 }}><tr style={{ background: '#f8fafc', color: colors.muted }}>
              {['Ticker', 'Latest signal', 'Frame', 'Side / setup', 'Signal open', 'Signal close', 'Planned stop', 'Planned target', 'Next-bar open', 'Review priority', 'Evidence'].map(label => (
                <th key={label} style={{ textAlign: label === 'Ticker' || label === 'Side / setup' || label === 'Review priority' || label === 'Evidence' ? 'left' : 'right', padding: '9px 10px', whiteSpace: 'nowrap', borderBottom: `1px solid ${colors.line}` }}>{label}</th>
              ))}
            </tr></thead>
            <tbody>
              {latestSignals.isLoading && <tr><td colSpan={11} style={{ padding: 18, color: colors.muted }}>Loading latest scanner signals…</td></tr>}
              {latestSignals.isError && <tr><td colSpan={11} style={{ padding: 18, color: colors.red }}>Latest scanner signals could not be loaded.</td></tr>}
              {!latestSignals.isLoading && !latestSignals.isError && latestRows.length === 0 && (
                <tr><td colSpan={11} style={{ padding: 18, color: colors.muted }}>No scanner signals match these filters.</td></tr>
              )}
              {latestRows.map(row => {
                const evidenceKey = `${row.scanner_name}|${row.interval}|${row.direction}`
                const hasRobustEvidence = robustKeys.has(evidenceKey)
                const hasMonitorEvidence = monitorKeys.has(evidenceKey)
                const priority = reviewPriorityBadge(row.review_priority_tier)
                const evidenceRows = hasRobustEvidence ? robustRows : monitorRows
                const evidenceHorizons = evidenceRows
                  .filter(result => result.scanner_name === row.scanner_name && result.interval === row.interval && result.direction === row.direction)
                  .map(result => result.horizon_bars)
                const calibratedEvidence = evidenceRows.find(result => (
                  result.scanner_name === row.scanner_name
                  && result.interval === row.interval
                  && result.direction === row.direction
                  && result.calibration_status === 'RESEARCH_CALIBRATED'
                ))
                const breadthEvidence = evidenceRows.find(result => (
                  result.scanner_name === row.scanner_name
                  && result.interval === row.interval
                  && result.direction === row.direction
                ))
                return (
                  <tr key={row.ticker} style={{ borderBottom: `1px solid ${colors.line}` }}>
                    <td style={{ padding: '9px 10px' }}>
                      <button
                        type="button"
                        onClick={() => navigate(`/ticker/${row.ticker}`)}
                        style={{ border: 0, padding: 0, background: 'transparent', color: colors.blue, fontWeight: 800, cursor: 'pointer' }}
                      >
                        {row.ticker}
                      </button>
                    </td>
                    <td style={{ textAlign: 'right', padding: '9px 10px', whiteSpace: 'nowrap' }}>{signalTime(row.signal_time, row.interval)}</td>
                    <td style={{ textAlign: 'right', padding: '9px 10px' }}>{intervalLabel(row.interval)}</td>
                    <td style={{ padding: '9px 10px', minWidth: 185 }}>
                      <div style={{ color: row.direction === 1 ? colors.green : colors.red, fontWeight: 700 }}>{row.direction === 1 ? 'Long' : 'Short'} · {(row.trigger_type || row.scanner_name).replace(/_/g, ' ')}</div>
                      <div style={{ color: colors.muted, fontSize: 11 }}>{scannerLabels[row.scanner_name] ?? row.scanner_name}</div>
                    </td>
                    <td style={{ textAlign: 'right', padding: '9px 10px', whiteSpace: 'nowrap' }}>{money(row.signal_open_price)}</td>
                    <td style={{ textAlign: 'right', padding: '9px 10px', whiteSpace: 'nowrap', fontWeight: 700 }}>{money(row.signal_close_price)}</td>
                    <td style={{ textAlign: 'right', padding: '9px 10px', whiteSpace: 'nowrap', color: colors.red }}>{money(row.stop_price)}</td>
                    <td style={{ textAlign: 'right', padding: '9px 10px', whiteSpace: 'nowrap', color: colors.green }}>{money(row.target_price)}</td>
                    <td style={{ textAlign: 'right', padding: '9px 10px', whiteSpace: 'nowrap' }}>{row.next_open_price == null ? <span style={{ color: colors.muted }}>Awaiting bar</span> : money(row.next_open_price)}</td>
                    <td style={{ padding: '9px 10px', minWidth: 185 }}>
                      <span style={{ background: priority.bg, color: priority.color, borderRadius: 4, padding: '3px 6px', fontWeight: 700 }}>
                        {priority.label}
                      </span>
                      <div style={{ color: colors.muted, fontSize: 10, marginTop: 4 }}>{row.review_priority_reasons[0]}</div>
                      {row.extension_risk && row.extension_risk !== 'NORMAL' && (
                        <div style={{ color: row.extension_risk === 'EXHAUSTION_WATCH' ? colors.red : colors.amber, fontSize: 10, marginTop: 4 }} title={row.position_guidance ?? undefined}>
                          Current: {row.trend_state?.replace(/_/g, ' ')} · {row.extension_risk.replace(/_/g, ' ')}
                          {row.reversal_trigger && row.reversal_trigger !== 'NONE' ? ` · ${row.reversal_trigger.replace(/_/g, ' ')}` : ''}
                        </div>
                      )}
                    </td>
                    <td style={{ padding: '9px 10px', whiteSpace: 'nowrap' }}>
                      <span style={{ background: hasRobustEvidence ? colors.greenSoft : colors.amberSoft, color: hasRobustEvidence ? colors.green : colors.amber, borderRadius: 4, padding: '3px 6px', fontWeight: 700 }}>
                        {hasRobustEvidence
                          ? `Robust · ${evidenceHorizons.map(value => horizonLabel(row.interval, value)).join(' / ')}`
                          : hasMonitorEvidence
                            ? `Monitor · ${evidenceHorizons.map(value => horizonLabel(row.interval, value)).join(' / ')}`
                            : 'Unranked'}
                      </span>
                      {calibratedEvidence && (
                        <div style={{ color: colors.muted, fontSize: 10, marginTop: 4 }}>
                          P(win) {pct(calibratedEvidence.calibrated_win_probability, 1)} · expected α {pct(calibratedEvidence.live_expected_alpha, 2)}
                        </div>
                      )}
                      {breadthEvidence && (hasRobustEvidence || hasMonitorEvidence) && (
                        <div style={{ color: colors.muted, fontSize: 10, marginTop: 4 }} title="Sector α nets out sector rotation, not just broad market; low concentration means the edge isn't riding a handful of tickers">
                          {breadthEvidence.distinct_tickers ?? '—'} tickers ({pct(breadthEvidence.top5_concentration, 0)} top-5) · sector α {pct(breadthEvidence.mean_sector_alpha, 2)}
                        </div>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        <div style={{ padding: '8px 14px', color: colors.muted, fontSize: 11, borderTop: `1px solid ${colors.line}` }}>
          Showing {latestRows.length} of {latestSignals.data?.results.length ?? 0} tickers with stored scanner signals.
        </div>
      </section>

      <section style={{ ...panel, padding: 14 }}>
        <h2 style={{ fontSize: 16, margin: '0 0 4px', letterSpacing: 0 }}>Signal setups</h2>
        <div style={{ color: colors.muted, fontSize: 11, marginBottom: 10 }}>What each of the 7 shadow scanners is looking for. None of these are recommendations on their own — see Evidence in the table above.</div>
        <ul style={{ margin: 0, padding: 0, listStyle: 'none', display: 'grid', gap: 8 }}>
          {Object.entries(scannerLabels).map(([name, label]) => (
            <li key={name} style={{ display: 'flex', gap: 8, alignItems: 'baseline' }}>
              <span style={{ color: colors.blue, flexShrink: 0 }}>•</span>
              <span style={{ fontSize: 12.5, lineHeight: 1.5 }}>
                <strong>{label}</strong>{' '}
                <span style={{ color: colors.blue, fontSize: 10.5, fontWeight: 700 }}>({scannerMeta[name].direction})</span>
                {' — '}
                <span style={{ color: colors.muted }}>{scannerMeta[name].description}</span>
              </span>
            </li>
          ))}
        </ul>
      </section>
      </>}
    </div>
  )
}
