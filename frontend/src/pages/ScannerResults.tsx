import { useMemo, useState } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  getScannerEventBacklog,
  getLatestScannerSignals,
  getScannerQualification,
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
  interval === '1d' ? 'Daily' : interval === '1wk' ? 'Weekly' : 'Hourly'
)

const horizonLabel = (interval: ScannerInterval, horizon: number) => (
  `${horizon} ${interval === '1wk' ? 'sessions' : 'bars'}`
)

const signalTime = (value: string, interval: ScannerInterval) => new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York', month: 'short', day: 'numeric', year: 'numeric',
  ...(interval === '1h' ? { hour: 'numeric', minute: '2-digit' } as const : {}),
}).format(new Date(value))

const reviewPriorityBadge = (tier: 'HIGHER' | 'STANDARD' | 'LOWER' | 'UNRANKED') => {
  if (tier === 'HIGHER') return { label: 'Higher', color: colors.green, bg: colors.greenSoft }
  if (tier === 'LOWER') return { label: 'Lower', color: colors.red, bg: colors.redSoft }
  if (tier === 'STANDARD') return { label: 'Standard', color: colors.blue, bg: colors.blueSoft }
  return { label: 'Unranked', color: colors.muted, bg: colors.canvas }
}

export default function ScannerResults() {
  const navigate = useNavigate()
  const [signalInterval, setSignalInterval] = useState<'all' | ScannerInterval>('all')
  const [signalSide, setSignalSide] = useState<'all' | 'long' | 'short'>('all')
  const [signalEvidence, setSignalEvidence] = useState<'all' | 'robust' | 'monitor' | 'unranked'>('all')
  const [signalPriority, setSignalPriority] = useState<'all' | 'HIGHER' | 'STANDARD' | 'LOWER' | 'UNRANKED'>('all')
  const [signalSector, setSignalSector] = useState('all')
  const [signalScanner, setSignalScanner] = useState('all')
  const [signalSearch, setSignalSearch] = useState('')

  const qualification = useQuery({
    queryKey: ['scanner-qualification'],
    queryFn: () => getScannerQualification(),
  })
  const backlog = useQuery({
    queryKey: ['scanner-events', 'backlog'],
    queryFn: () => getScannerEventBacklog(),
  })
  const latestSignals = useQuery({
    queryKey: ['scanner-events', 'latest-by-ticker', signalInterval],
    queryFn: () => getLatestScannerSignals(signalInterval === 'all' ? undefined : signalInterval),
    placeholderData: keepPreviousData,
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

  const panel: React.CSSProperties = {
    background: colors.panel, border: `1px solid ${colors.line}`, borderRadius: 7,
  }
  const control: React.CSSProperties = {
    border: `1px solid ${colors.line}`, borderRadius: 5, background: '#fff',
    color: colors.ink, padding: '7px 10px', fontSize: 13,
  }

  if (latestSignals.isLoading) return <div style={{ padding: 24, color: colors.muted }}>Loading scanner results…</div>
  if (latestSignals.isError) return <div style={{ padding: 24, color: colors.red }}>Scanner results could not be loaded.</div>

  return (
    <div style={{ color: colors.ink, opacity: latestSignals.isFetching ? 0.6 : 1, transition: 'opacity 0.15s' }}>
      <header style={{ padding: '10px 2px 18px', borderBottom: `1px solid ${colors.line}`, marginBottom: 16 }}>
        <div style={{ color: colors.blue, fontSize: 12, fontWeight: 700, textTransform: 'uppercase' }}>Signal review · developing page</div>
        <h1 style={{ fontSize: 26, lineHeight: 1.15, margin: '4px 0 5px', letterSpacing: 0 }}>Scanner Results</h1>
        <p style={{ margin: 0, color: colors.muted, fontSize: 14 }}>
          Each ticker's latest scanner signal, for understanding what the shadow scanners are currently finding.
          This page is intentionally detailed while the presentation is still evolving; expect it to be simplified later.
        </p>
      </header>

      <section style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(175px, 1fr))', gap: 10, marginBottom: 16 }}>
        {[
          ['Combinations', allRows.length.toString(), 'scanner × side × horizon'],
          ['Robust scanners', robustRows.length.toString(), 'after family-wise FDR'],
          ['Monitor only', monitorRows.length.toString(), 'raw pass; not robust'],
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
            <h2 style={{ fontSize: 16, margin: 0, letterSpacing: 0 }}>
              Latest scanner signal by ticker
              {latestSignals.isFetching && <span style={{ color: colors.blue, fontSize: 11, fontWeight: 500, marginLeft: 8 }}>Updating…</span>}
            </h2>
            <div style={{ color: colors.muted, fontSize: 11, marginTop: 2 }}>
              One newest observed setup per matched ticker, from the last 10 daily/weekly sessions or 2 days of hourly bars. These are research signals, not recommendations; open a ticker for its complete evidence and outcomes.
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <input
              aria-label="Search latest scanner signals"
              value={signalSearch}
              onChange={event => setSignalSearch(event.target.value)}
              placeholder="Ticker or scanner"
              style={{ ...control, width: 150 }}
            />
            <select aria-label="Signal interval" value={signalInterval} onChange={event => setSignalInterval(event.target.value as typeof signalInterval)} style={control}>
              <option value="all">Latest any interval</option><option value="1d">Latest daily</option><option value="1wk">Latest weekly</option><option value="1h">Latest hourly</option>
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
                      <div style={{ color: row.direction === 1 ? colors.green : colors.red, fontWeight: 700 }}>{row.direction === 1 ? 'Long' : 'Short'} · {row.trigger_type.replace(/_/g, ' ')}</div>
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
    </div>
  )
}
