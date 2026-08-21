import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  getScannerEventBacklog,
  getLatestScannerSignals,
  getScannerQualification,
  getScannerSectorPerformance,
  ScannerQualificationRow,
  ScannerInterval,
  SectorPerformanceSessions,
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

const sectorPeriods: Array<{ sessions: SectorPerformanceSessions; label: string }> = [
  { sessions: 1, label: '1 day' },
  { sessions: 5, label: '1 week' },
  { sessions: 10, label: '2 weeks' },
  { sessions: 21, label: '1 month' },
]

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

const badge = (row: ScannerQualificationRow) => {
  if (row.evidence_status === 'ROBUST_PASS') {
    return { label: 'Robust evidence', color: colors.green, bg: colors.greenSoft }
  }
  if (row.evidence_status === 'MONITOR_ONLY') {
    return { label: 'Monitor only', color: colors.amber, bg: colors.amberSoft }
  }
  if ((row.mean_net_alpha ?? 0) > 0 && (row.alpha_t_stat ?? 0) > 0) {
    return { label: 'Positive, weak', color: colors.amber, bg: colors.amberSoft }
  }
  return { label: 'Not qualified', color: colors.red, bg: colors.redSoft }
}

const reviewPriorityBadge = (tier: 'HIGHER' | 'STANDARD' | 'LOWER' | 'UNRANKED') => {
  if (tier === 'HIGHER') return { label: 'Higher', color: colors.green, bg: colors.greenSoft }
  if (tier === 'LOWER') return { label: 'Lower', color: colors.red, bg: colors.redSoft }
  if (tier === 'STANDARD') return { label: 'Standard', color: colors.blue, bg: colors.blueSoft }
  return { label: 'Unranked', color: colors.muted, bg: colors.canvas }
}

export default function ScannerEvaluation() {
  const navigate = useNavigate()
  const [interval, setInterval] = useState<'all' | ScannerInterval>('all')
  const [side, setSide] = useState<'all' | 'long' | 'short'>('all')
  const [status, setStatus] = useState<'all' | 'robust' | 'monitor' | 'unranked'>('all')
  const [signalInterval, setSignalInterval] = useState<'all' | ScannerInterval>('all')
  const [signalSide, setSignalSide] = useState<'all' | 'long' | 'short'>('all')
  const [signalEvidence, setSignalEvidence] = useState<'all' | 'robust' | 'monitor' | 'unranked'>('all')
  const [signalPriority, setSignalPriority] = useState<'all' | 'HIGHER' | 'STANDARD' | 'LOWER' | 'UNRANKED'>('all')
  const [signalSector, setSignalSector] = useState('all')
  const [signalSearch, setSignalSearch] = useState('')
  const [sectorSessions, setSectorSessions] = useState<SectorPerformanceSessions>(1)

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
  })
  const sectorPerformance = useQuery({
    queryKey: ['scanner-events', 'sector-performance', sectorSessions],
    queryFn: () => getScannerSectorPerformance(sectorSessions),
  })

  const rows = useMemo(() => {
    const source = qualification.data?.results ?? []
    return source.filter(row => (
      (interval === 'all' || row.interval === interval)
      && (side === 'all' || (side === 'long' ? row.direction === 1 : row.direction === -1))
      && (status === 'all'
        || (status === 'robust' && row.evidence_status === 'ROBUST_PASS')
        || (status === 'monitor' && row.evidence_status === 'MONITOR_ONLY')
        || (status === 'unranked' && row.evidence_status === 'UNRANKED'))
    )).sort((a, b) => {
      const evidenceRank = { ROBUST_PASS: 2, MONITOR_ONLY: 1, UNRANKED: 0 }
      if (a.evidence_status !== b.evidence_status) return evidenceRank[b.evidence_status] - evidenceRank[a.evidence_status]
      return (b.alpha_t_stat ?? -99) - (a.alpha_t_stat ?? -99)
    })
  }, [qualification.data, interval, side, status])

  const allRows = qualification.data?.results ?? []
  const robustRows = allRows.filter(row => row.evidence_status === 'ROBUST_PASS')
  const monitorRows = allRows.filter(row => row.evidence_status === 'MONITOR_ONLY')
  const robustKeys = new Set(robustRows.map(row => `${row.scanner_name}|${row.interval}|${row.direction}`))
  const monitorKeys = new Set(monitorRows.map(row => `${row.scanner_name}|${row.interval}|${row.direction}`))
  const sectorRows = sectorPerformance.data?.results ?? []
  const sectorOptions = sectorRows.map(row => row.sector)
  const latestRows = (latestSignals.data?.results ?? []).filter(row => {
    const evidenceKey = `${row.scanner_name}|${row.interval}|${row.direction}`
    const evidence = robustKeys.has(evidenceKey) ? 'robust' : monitorKeys.has(evidenceKey) ? 'monitor' : 'unranked'
    const search = signalSearch.trim().toUpperCase()
    return (!search || row.ticker.includes(search) || row.scanner_name.toUpperCase().includes(search))
      && (signalSide === 'all' || (signalSide === 'long' ? row.direction === 1 : row.direction === -1))
      && (signalEvidence === 'all' || signalEvidence === evidence)
      && (signalPriority === 'all' || row.review_priority_tier === signalPriority)
      && (signalSector === 'all' || (signalSector === 'unclassified' ? !row.sector : row.sector === signalSector))
  })
  const totalEvents = allRows.reduce((sum, row) => sum + row.events, 0)
  const pending = (backlog.data?.results ?? []).reduce((sum, row) => sum + row.pending, 0)
  const best = robustRows[0]

  if (qualification.isLoading) return <div style={{ padding: 24, color: colors.muted }}>Loading scanner evaluation…</div>
  if (qualification.isError) return <div style={{ padding: 24, color: colors.red }}>Scanner evaluation could not be loaded.</div>

  const panel: React.CSSProperties = {
    background: colors.panel, border: `1px solid ${colors.line}`, borderRadius: 7,
  }
  const control: React.CSSProperties = {
    border: `1px solid ${colors.line}`, borderRadius: 5, background: '#fff',
    color: colors.ink, padding: '7px 10px', fontSize: 13,
  }

  return (
    <div style={{ color: colors.ink }}>
      <header style={{ padding: '10px 2px 18px', borderBottom: `1px solid ${colors.line}`, marginBottom: 16 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 20, alignItems: 'flex-end', flexWrap: 'wrap' }}>
          <div>
            <div style={{ color: colors.blue, fontSize: 12, fontWeight: 700, textTransform: 'uppercase' }}>Signal review and research control</div>
            <h1 style={{ fontSize: 26, lineHeight: 1.15, margin: '4px 0 5px', letterSpacing: 0 }}>Scanner Evaluation</h1>
            <p style={{ margin: 0, color: colors.muted, fontSize: 14 }}>
              Review each ticker's latest scanner signal, then use full-history outcomes to judge whether its scanner is credible.
            </p>
          </div>
          <div style={{ color: colors.muted, fontSize: 12, textAlign: 'right' }}>
            Daily: Mar 2023–Aug 2026 · Weekly: collecting · Hourly: Aug 2024–Aug 2026<br />
            Execution: {qualification.data?.entry_model.replace(/_/g, ' ')}
          </div>
        </div>
      </header>

      <section style={{ ...panel, marginBottom: 16 }}>
        <div style={{ padding: '12px 14px', borderBottom: `1px solid ${colors.line}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <h2 style={{ fontSize: 16, margin: 0, letterSpacing: 0 }}>Sector performance</h2>
            <div style={{ color: colors.muted, fontSize: 11, marginTop: 2 }}>
              Equal-weight close-to-close performance over {sectorSessions} trading session{sectorSessions === 1 ? '' : 's'}, ending {sectorRows[0]?.trade_date ?? 'on the latest daily session'}.
            </div>
          </div>
          <div role="group" aria-label="Sector performance period" style={{ display: 'flex', border: `1px solid ${colors.line}`, borderRadius: 5, overflow: 'hidden' }}>
            {sectorPeriods.map(period => {
              const selected = period.sessions === sectorSessions
              return (
                <button
                  key={period.sessions}
                  type="button"
                  aria-pressed={selected}
                  onClick={() => setSectorSessions(period.sessions)}
                  style={{
                    border: 0,
                    borderRight: period.sessions === 21 ? 0 : `1px solid ${colors.line}`,
                    background: selected ? colors.blue : '#fff',
                    color: selected ? '#fff' : colors.ink,
                    padding: '7px 11px',
                    minWidth: 66,
                    fontSize: 12,
                    fontWeight: selected ? 700 : 500,
                    cursor: 'pointer',
                  }}
                >
                  {period.label}
                </button>
              )
            })}
          </div>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead><tr style={{ background: '#f8fafc', color: colors.muted }}>
              {['Rank', 'Sector', 'Average', 'Median', 'Positive breadth', 'Advancing / declining', 'Leader', 'Laggard'].map(label => (
                <th key={label} style={{ textAlign: label === 'Sector' || label === 'Leader' || label === 'Laggard' ? 'left' : 'right', padding: '9px 10px', whiteSpace: 'nowrap', borderBottom: `1px solid ${colors.line}` }}>{label}</th>
              ))}
            </tr></thead>
            <tbody>
              {sectorPerformance.isLoading && <tr><td colSpan={8} style={{ padding: 18, color: colors.muted }}>Loading sector performance…</td></tr>}
              {sectorPerformance.isError && <tr><td colSpan={8} style={{ padding: 18, color: colors.red }}>Sector performance could not be loaded.</td></tr>}
              {sectorRows.map((row, index) => (
                <tr key={row.sector} style={{ borderBottom: `1px solid ${colors.line}` }}>
                  <td style={{ textAlign: 'right', padding: '9px 10px', fontWeight: 750 }}>{index + 1}</td>
                  <td style={{ padding: '9px 10px', fontWeight: 700 }}>{row.sector}<div style={{ color: colors.muted, fontSize: 10, fontWeight: 400 }}>{row.tickers} stocks</div></td>
                  <td style={{ textAlign: 'right', padding: '9px 10px', fontWeight: 750, color: row.average_return >= 0 ? colors.green : colors.red }}>{pct(row.average_return, 2)}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px', color: row.median_return >= 0 ? colors.green : colors.red }}>{pct(row.median_return, 2)}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{pct(row.positive_breadth, 1)}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{row.positive_tickers} / {row.negative_tickers}</td>
                  <td style={{ padding: '9px 10px', whiteSpace: 'nowrap' }}><strong>{row.best_ticker}</strong> <span style={{ color: colors.green }}>{pct(row.best_return, 2)}</span></td>
                  <td style={{ padding: '9px 10px', whiteSpace: 'nowrap' }}><strong>{row.worst_ticker}</strong> <span style={{ color: colors.red }}>{pct(row.worst_return, 2)}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section style={{ ...panel, marginBottom: 16 }}>
        <div style={{ padding: '12px 14px', borderBottom: `1px solid ${colors.line}`, display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <h2 style={{ fontSize: 16, margin: 0, letterSpacing: 0 }}>Latest scanner signal by ticker</h2>
            <div style={{ color: colors.muted, fontSize: 11, marginTop: 2 }}>
              One newest observed setup per matched ticker. These are research signals, not recommendations; open a ticker for its complete evidence and outcomes.
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
        <div style={{ overflowX: 'auto', maxHeight: 560, overflowY: 'auto' }}>
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

      {best && (
        <section style={{ ...panel, borderLeft: `4px solid ${colors.green}`, padding: 16, marginBottom: 16 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
            <div>
              <div style={{ color: colors.green, fontSize: 11, fontWeight: 800, textTransform: 'uppercase' }}>Robust primary evidence</div>
              <h2 style={{ fontSize: 18, margin: '3px 0 4px', letterSpacing: 0 }}>
                {intervalLabel(best.interval)} {best.direction === 1 ? 'long' : 'short'} {scannerLabels[best.scanner_name] ?? best.scanner_name} · {best.horizon_bars}-bar horizon
              </h2>
              <div style={{ color: colors.muted, fontSize: 13 }}>
                Passed sample, alpha, early/late stability, and family-wise false-discovery gates. This is evidence for the scanner/horizon combination, not a trade probability.
              </div>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(80px, 1fr))', gap: 18 }}>
              {[
                ['Net alpha', pct(best.mean_net_alpha, 3), `${pct(best.alpha_ci_low, 2)} to ${pct(best.alpha_ci_high, 2)}`],
                ['Alpha t', best.alpha_t_stat?.toFixed(2) ?? '—', ''],
                ['Periods', best.independent_periods.toString(), ''],
                ['Hit rate', pct(best.hit_rate, 1), `${pct(best.hit_rate_ci_low, 1)} to ${pct(best.hit_rate_ci_high, 1)}`],
                ['Calibrated P(win)', best.calibration_status === 'RESEARCH_CALIBRATED' ? pct(best.calibrated_win_probability, 1) : 'Unavailable', best.calibration_status === 'RESEARCH_CALIBRATED' ? `${pct(best.calibrated_win_probability_ci_low, 1)} to ${pct(best.calibrated_win_probability_ci_high, 1)}` : ''],
                ['Live expected α', best.calibration_status === 'RESEARCH_CALIBRATED' ? pct(best.live_expected_alpha, 3) : 'Unavailable', best.calibration_status === 'RESEARCH_CALIBRATED' ? `${pct(best.live_expected_alpha_ci_low, 2)} to ${pct(best.live_expected_alpha_ci_high, 2)}` : ''],
                ['Brier', best.brier_score?.toFixed(3) ?? '—', ''],
                ['Calibration error', pct(best.expected_calibration_error, 1), ''],
              ].map(([label, value, detail]) => (
                <div key={label} style={{ textAlign: 'right' }}>
                  <div style={{ color: colors.muted, fontSize: 10, textTransform: 'uppercase' }}>{label}</div>
                  <div style={{ fontWeight: 750, fontSize: 17 }}>{value}</div>
                  {detail && <div style={{ color: colors.muted, fontSize: 10 }}>{detail}</div>}
                </div>
              ))}
            </div>
          </div>
          {best.calibration_curve.length > 0 && (
            <div style={{ marginTop: 16, paddingTop: 14, borderTop: `1px solid ${colors.line}`, display: 'grid', gridTemplateColumns: 'minmax(220px, 360px) 1fr', gap: 20, alignItems: 'center' }}>
              <svg viewBox="0 0 100 100" role="img" aria-label="Calibration reliability curve" style={{ width: '100%', maxHeight: 220, overflow: 'visible' }}>
                <line x1="0" y1="100" x2="100" y2="0" stroke={colors.line} strokeWidth="1" />
                <polyline
                  fill="none"
                  stroke={colors.blue}
                  strokeWidth="2"
                  points={best.calibration_curve
                    .slice()
                    .sort((a, b) => a.mean_predicted - b.mean_predicted)
                    .map(point => `${point.mean_predicted * 100},${100 - point.observed_frequency * 100}`)
                    .join(' ')}
                />
                {best.calibration_curve.map(point => (
                  <circle key={`${point.mean_predicted}-${point.observed_frequency}`} cx={point.mean_predicted * 100} cy={100 - point.observed_frequency * 100} r="2.2" fill={colors.blue} />
                ))}
              </svg>
              <div>
                <div style={{ fontWeight: 750, fontSize: 13 }}>Walk-forward reliability curve</div>
                <div style={{ color: colors.muted, fontSize: 11, marginTop: 3 }}>
                  Predicted frequency on the horizontal axis, observed frequency on the vertical axis. {best.calibration_oos_periods} later periods were scored using prior history only.
                </div>
              </div>
            </div>
          )}
        </section>
      )}

      <section style={{ ...panel, marginBottom: 16 }}>
        <div style={{ padding: '12px 14px', borderBottom: `1px solid ${colors.line}`, display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <h2 style={{ fontSize: 16, margin: 0, letterSpacing: 0 }}>Qualification matrix</h2>
            <div style={{ color: colors.muted, fontSize: 11 }}>100 events · 40 horizon-spaced periods · positive early/late alpha · alpha t &gt; 2</div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <select aria-label="Interval" value={interval} onChange={e => setInterval(e.target.value as typeof interval)} style={control}>
              <option value="all">All intervals</option><option value="1d">Daily</option><option value="1wk">Weekly</option><option value="1h">Hourly</option>
            </select>
            <select aria-label="Side" value={side} onChange={e => setSide(e.target.value as typeof side)} style={control}>
              <option value="all">Both sides</option><option value="long">Long</option><option value="short">Short</option>
            </select>
            <select aria-label="Status" value={status} onChange={e => setStatus(e.target.value as typeof status)} style={control}>
              <option value="all">All results</option><option value="robust">Robust evidence</option><option value="monitor">Monitor only</option><option value="unranked">Unranked</option>
            </select>
          </div>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead><tr style={{ background: '#f8fafc', color: colors.muted }}>
              {['Scanner', 'Frame', 'Side', 'Horizon', 'Events', 'Periods', 'Net α (95% CI)', 't-stat', 'FDR q', 'Early / Late α', 'Hit (95% CI)', 'Calibration', 'Status'].map(label => (
                <th key={label} style={{ textAlign: label === 'Scanner' ? 'left' : 'right', padding: '9px 10px', whiteSpace: 'nowrap', borderBottom: `1px solid ${colors.line}` }}>{label}</th>
              ))}
            </tr></thead>
            <tbody>{rows.map(row => {
              const state = badge(row)
              return (
                <tr key={`${row.scanner_name}-${row.interval}-${row.direction}-${row.horizon_bars}`} style={{ borderBottom: `1px solid ${colors.line}` }}>
                  <td style={{ padding: '9px 10px', fontWeight: 650 }}>{scannerLabels[row.scanner_name] ?? row.scanner_name} <span style={{ color: colors.muted, fontWeight: 400 }}>v{row.scanner_version}</span></td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{intervalLabel(row.interval)}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px', color: row.direction === 1 ? colors.green : colors.red }}>{row.direction === 1 ? 'Long' : 'Short'}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{horizonLabel(row.interval, row.horizon_bars)}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{compact(row.events)}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{row.independent_periods}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px', fontWeight: 700, color: (row.mean_net_alpha ?? 0) >= 0 ? colors.green : colors.red }}>
                    {pct(row.mean_net_alpha, 3)}<div style={{ color: colors.muted, fontSize: 10, fontWeight: 400 }}>{pct(row.alpha_ci_low, 2)} to {pct(row.alpha_ci_high, 2)}</div>
                  </td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{row.alpha_t_stat?.toFixed(2) ?? '—'}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{row.alpha_fdr_q?.toFixed(3) ?? '—'}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{pct(row.early_alpha, 2)} / {pct(row.late_alpha, 2)}</td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}>{pct(row.hit_rate, 1)}<div style={{ color: colors.muted, fontSize: 10 }}>{pct(row.hit_rate_ci_low, 1)} to {pct(row.hit_rate_ci_high, 1)}</div></td>
                  <td style={{ textAlign: 'right', padding: '9px 10px', whiteSpace: 'nowrap' }}>
                    {row.calibration_status === 'RESEARCH_CALIBRATED' ? <>
                      {pct(row.calibrated_win_probability, 1)}
                      <div style={{ color: colors.muted, fontSize: 10 }}>{pct(row.calibrated_win_probability_ci_low, 1)} to {pct(row.calibrated_win_probability_ci_high, 1)}</div>
                      <div style={{ color: colors.muted, fontSize: 10 }}>Brier {row.brier_score?.toFixed(3) ?? '—'}</div>
                    </> : 'Unavailable'}
                  </td>
                  <td style={{ textAlign: 'right', padding: '9px 10px' }}><span style={{ background: state.bg, color: state.color, borderRadius: 4, padding: '3px 6px', fontWeight: 700, whiteSpace: 'nowrap' }}>{state.label}</span></td>
                </tr>
              )
            })}</tbody>
          </table>
        </div>
      </section>

      <section style={{ ...panel, padding: 14 }}>
          <h2 style={{ fontSize: 15, margin: '0 0 10px', letterSpacing: 0 }}>Decision boundary</h2>
          <dl style={{ margin: 0, display: 'grid', gap: 10, fontSize: 12 }}>
            <div><dt style={{ color: colors.muted }}>Robust evidence</dt><dd style={{ margin: 0, fontWeight: 650 }}>Primary gates plus family-wise FDR at 5%</dd></div>
            <div><dt style={{ color: colors.muted }}>Monitor only</dt><dd style={{ margin: 0, fontWeight: 650 }}>Raw primary pass that did not survive correction</dd></div>
            <div><dt style={{ color: colors.muted }}>Unranked</dt><dd style={{ margin: 0, fontWeight: 650 }}>Insufficient or unstable evidence; no confidence claim</dd></div>
            <div><dt style={{ color: colors.muted }}>Research calibrated</dt><dd style={{ margin: 0, fontWeight: 650 }}>Robust evidence plus at least 100 walk-forward periods, Brier &lt; 0.25, and calibration error ≤ 5%</dd></div>
            <div><dt style={{ color: colors.muted }}>Portal effect</dt><dd style={{ margin: 0, fontWeight: 650 }}>No scanner currently changes recommendations</dd></div>
          </dl>
      </section>
    </div>
  )
}
