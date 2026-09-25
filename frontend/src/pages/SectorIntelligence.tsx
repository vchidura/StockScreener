import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  getSectorIntelligence,
  getMarketRegime,
  getTickersOverview,
} from '../services/api'
import { usePublishPageContext } from '../layout/pageContext'
import { SectorEquityContext } from './EquityContextPanels'

const colors = {
  ink: 'var(--tm-ink)', muted: 'var(--tm-muted)', line: 'var(--tm-line)', panel: 'var(--tm-surface)',
  canvas: 'var(--tm-canvas)', green: 'var(--tm-pos)', greenSoft: 'var(--tm-pos-soft)',
  red: 'var(--tm-neg)', redSoft: 'var(--tm-neg-soft)', amber: 'var(--tm-warn)', amberSoft: 'var(--tm-warn-soft)',
  blue: 'var(--tm-accent)', blueSoft: 'var(--tm-accent-soft)',
}

const rotationWindows = ['1', '5', '10', '21', '63']
const leaderWindows = ['1', '2', '3', '5', '10', '21', '63']
const windowLabels: Record<string, string> = {
  '1': '1 day', '2': '2 days', '3': '3 days', '5': '1 week', '10': '2 weeks', '21': '1 month', '63': '3 months',
}

const pct = (value: number | null | undefined, digits = 2) =>
  value == null ? '—' : `${(value * 100).toFixed(digits)}%`

const REGIME_STYLE: Record<string, { icon: string; color: string; bg: string }> = {
  'Strong Bull': { icon: '🟢', color: 'var(--tm-pos)', bg: 'var(--tm-pos-soft)' },
  'Bull':        { icon: '🟩', color: 'var(--tm-pos)', bg: 'var(--tm-pos-soft)' },
  'Caution':     { icon: '🟡', color: 'var(--tm-warn)', bg: 'var(--tm-warn-soft)' },
  'Bear Rally':  { icon: '🟠', color: 'var(--tm-warn)', bg: 'var(--tm-warn-soft)' },
  'Bear':        { icon: '🔴', color: 'var(--tm-neg)', bg: 'var(--tm-neg-soft)' },
  'Strong Bear': { icon: '⛔', color: 'var(--tm-neg)', bg: 'var(--tm-neg-soft)' },
  'Unknown':     { icon: '⚪', color: 'var(--tm-muted)', bg: 'var(--tm-surface-sunken)' },
}

export default function SectorIntelligence() {
  const navigate = useNavigate()
  const [selectedSector, setSelectedSector] = useState<string | null>(null)
  const [horizon, setHorizon] = useState('1')

  const intelligence = useQuery({
    queryKey: ['sector-intelligence'],
    queryFn: () => getSectorIntelligence(5),
  })
  const marketRegime = useQuery({
    queryKey: ['market-regime'],
    queryFn: () => getMarketRegime(),
  })
  const overview = useQuery({
    queryKey: ['tickers', 'overview'],
    queryFn: () => getTickersOverview(),
  })

  const breadth = useMemo(() => {
    const withSma = (overview.data ?? []).filter(r => r.sma_200 != null && r.close != null)
    if (withSma.length === 0) return null
    const above200 = withSma.filter(r => (r.close ?? 0) > (r.sma_200 ?? 0)).length
    const above50 = withSma.filter(r => (r.close ?? 0) > (r.sma_50 ?? 0)).length
    const above20 = withSma.filter(r => (r.close ?? 0) > (r.sma_20 ?? 0)).length
    return {
      total: withSma.length,
      pct_200: Math.round(above200 / withSma.length * 100),
      pct_50: Math.round(above50 / withSma.length * 100),
      pct_20: Math.round(above20 / withSma.length * 100),
    }
  }, [overview.data])

  const rows = intelligence.data?.results ?? []
  const detail = useMemo(
    () => rows.find(row => row.sector === selectedSector) ?? rows[0] ?? null,
    [rows, selectedSector],
  )

  const panel: React.CSSProperties = {
    background: colors.panel, border: `1px solid ${colors.line}`, borderRadius: 7,
  }

  // Published above the early returns so the hook order stays stable while loading.
  usePublishPageContext({
    eyebrow: 'Market · sector intelligence',
    title: 'Sector Intelligence',
    detail: 'Sector returns, rotation, breadth and published equity context.',
    status: [
      {
        label: 'Market',
        value: marketRegime.data?.regime && marketRegime.data.regime !== 'Unknown' ? marketRegime.data.regime : '—',
      },
    ],
  })

  if (intelligence.isLoading) return <div style={{ padding: 24, color: colors.muted }}>Loading sector intelligence…</div>
  if (intelligence.isError) return <div style={{ padding: 24, color: colors.red }}>Sector intelligence could not be loaded.</div>

  return (
    <div style={{ color: colors.ink }}>

      {marketRegime.data && marketRegime.data.regime !== 'Unknown' && (() => {
        const rs = REGIME_STYLE[marketRegime.data.regime] || REGIME_STYLE['Unknown']
        const indices = [marketRegime.data.spy, marketRegime.data.qqq].filter(Boolean) as NonNullable<typeof marketRegime.data.spy>[]
        const TH: React.CSSProperties = { fontSize: 10, color: colors.muted, fontWeight: 700, textTransform: 'uppercase', padding: '0 6px 3px', whiteSpace: 'nowrap', textAlign: 'center' }
        const TD: React.CSSProperties = { fontSize: 12, fontWeight: 700, padding: '2px 6px', whiteSpace: 'nowrap', textAlign: 'center' }
        const colColor = (value: number) => value >= 0 ? colors.green : colors.red

        return (
          <section style={{ ...panel, marginBottom: 16, padding: '12px 14px', background: rs.bg, border: `1px solid ${rs.color}` }}>
            <div style={{ color: rs.color, fontSize: 11, fontWeight: 800, textTransform: 'uppercase' }}>Market regime</div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap', marginTop: 4 }}>
              <div style={{ flex: '0 1 auto', minWidth: 200 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6, flexWrap: 'wrap' }}>
                  <span style={{ fontSize: 20 }}>{rs.icon}</span>
                  <span style={{ fontSize: 15, fontWeight: 800, color: rs.color }}>{marketRegime.data.regime}</span>
                </div>
                <div style={{ fontSize: 12, color: rs.color, fontWeight: 500, lineHeight: 1.4, marginBottom: 6 }}>{marketRegime.data.description}</div>
                {(marketRegime.data.caution_buy || marketRegime.data.caution_sell) && (
                  <div style={{ fontSize: 11, background: colors.amberSoft, color: colors.amber, padding: '2px 8px', borderRadius: 4, fontWeight: 700, display: 'inline-block', marginBottom: 4 }}>
                    ⚠️ {marketRegime.data.caution_buy ? 'Buy signals may trap' : 'Sell signals may trap'}
                  </div>
                )}
                {marketRegime.data.divergence && (
                  <div style={{ fontSize: 11, background: colors.blueSoft, color: colors.blue, padding: '2px 8px', borderRadius: 4, fontWeight: 700, display: 'inline-block' }}>
                    🔀 {marketRegime.data.divergence}
                  </div>
                )}
              </div>

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
                        <tr key={idx.ticker} style={{ borderTop: '1px solid rgba(0,0,0,0.06)' }}>
                          <td style={{ ...TD, textAlign: 'left', color: rs.color }}>{idx.ticker}</td>
                          <td style={{ ...TD, color: rs.color }}>${idx.price}</td>
                          <td style={{ ...TD, color: idx.rsi < 30 ? colors.red : idx.rsi > 70 ? colors.amber : colors.ink }}>{idx.rsi}</td>
                          <td style={{ ...TD, color: idx.ema_bullish ? colors.green : colors.red }}>{idx.ema_bullish ? '▲ Bull' : '▼ Bear'}</td>
                          <td style={{ ...TD, color: colColor(idx.macd_histogram) }}>{idx.macd_histogram > 0 ? '+' : ''}{idx.macd_histogram}</td>
                          <td style={{ ...TD, color: idx.macd_hist_trend === 'Rising' ? colors.green : colors.red }}>{idx.macd_hist_trend === 'Rising' ? '▲' : '▼'}</td>
                          <td style={{ ...TD, color: colColor(idx.dist_from_200) }}>{idx.dist_from_200 > 0 ? '+' : ''}{idx.dist_from_200}%</td>
                          <td style={{ ...TD, color: colColor(idx.chg_20d) }}>{idx.chg_20d > 0 ? '+' : ''}{idx.chg_20d}%</td>
                          <td style={{ ...TD, color: idx.drawdown_from_52w_high < -10 ? colors.red : colors.ink }}>{idx.drawdown_from_52w_high}%</td>
                          <td style={{ ...TD, color: idx.golden_cross ? colors.green : colors.red }}>{idx.golden_cross ? 'Golden' : 'Death'}</td>
                          <td style={{ ...TD, color: idx.wsma_50 != null ? (idx.price > idx.wsma_50 ? colors.green : colors.red) : colors.muted }}>{idx.wsma_50 != null ? `$${idx.wsma_50}` : '—'}</td>
                          <td style={{ ...TD, color: idx.wsma_200 != null ? (idx.price > idx.wsma_200 ? colors.green : colors.red) : colors.muted }}>{idx.wsma_200 != null ? `$${idx.wsma_200}` : '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            {breadth && (
              <div style={{ marginTop: 8, paddingTop: 8, borderTop: `1px solid ${rs.color}20` }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 14, flexWrap: 'wrap' }}>
                  <span style={{ fontSize: 11, fontWeight: 700, color: colors.ink }}>📊 Market Breadth</span>
                  {([
                    ['> 200 SMA', breadth.pct_200],
                    ['> 50 SMA', breadth.pct_50],
                    ['> 20 SMA', breadth.pct_20],
                  ] as [string, number][]).map(([label, value]) => (
                    <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                      <span style={{ fontSize: 11, color: colors.muted, fontWeight: 600 }}>{label}</span>
                      <div style={{ width: 60, height: 8, background: 'var(--tm-line)', borderRadius: 4, overflow: 'hidden' }}>
                        <div style={{ width: `${value}%`, height: '100%', background: value >= 60 ? colors.green : value >= 40 ? colors.amber : colors.red }} />
                      </div>
                      <span style={{ fontSize: 11, fontWeight: 700, color: value >= 60 ? colors.green : value >= 40 ? colors.amber : colors.red }} title={`of ${breadth.total} tickers`}>{value}%</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </section>
        )
      })()}

      <section style={{ ...panel, marginBottom: 16 }}>
        <div style={{ padding: '12px 14px', borderBottom: `1px solid ${colors.line}` }}>
          <h2 style={{ fontSize: 16, margin: 0, letterSpacing: 0 }}>Rotation across horizons</h2>
          <div style={{ color: colors.muted, fontSize: 11, marginTop: 2 }}>
            Equal-weight average return per horizon, ranked fastest-first. Rotation compares 1-day rank to 3-month rank:
            a positive value means the sector has climbed since 3 months ago; 0 means its rank is unchanged, not that data is missing.
          </div>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead><tr style={{ background: 'var(--tm-surface-sunken)', color: colors.muted }}>
              {['Sector', ...rotationWindows.map(session => windowLabels[session]), 'Rotation', 'Breadth (1d)', 'Breadth (1w)', 'Breadth (1mo)'].map(label => (
                <th key={label} style={{ textAlign: label === 'Sector' ? 'left' : 'right', padding: '9px 10px', whiteSpace: 'nowrap', borderBottom: `1px solid ${colors.line}` }}>{label}</th>
              ))}
            </tr></thead>
            <tbody>
              {rows.map(row => {
                const selected = detail?.sector === row.sector
                const oneDay = row.rotation['1']
                const oneWeek = row.rotation['5']
                const oneMonth = row.rotation['21']
                return (
                  <tr
                    key={row.sector}
                    onClick={() => setSelectedSector(row.sector)}
                    style={{
                      borderBottom: `1px solid ${colors.line}`, cursor: 'pointer',
                      background: selected ? colors.blueSoft : 'transparent',
                    }}
                  >
                    <td style={{ padding: '9px 10px', fontWeight: 700 }}>{row.sector}<div style={{ color: colors.muted, fontSize: 10, fontWeight: 400 }}>{oneDay?.tickers ?? 0} stocks</div></td>
                    {rotationWindows.map(session => {
                      const window = row.rotation[session]
                      return (
                        <td key={session} style={{ textAlign: 'right', padding: '9px 10px', fontWeight: session === '1' ? 750 : 400, color: (window?.average_return ?? 0) >= 0 ? colors.green : colors.red }}>
                          {pct(window?.average_return)}
                        </td>
                      )
                    })}
                    <td style={{ textAlign: 'right', padding: '9px 10px', fontWeight: 700 }}>
                      {row.rotation_delta == null ? '—' : row.rotation_delta === 0 ? (
                        <span style={{ color: colors.muted }}>No change</span>
                      ) : (
                        <span style={{ color: row.rotation_delta > 0 ? colors.green : colors.red }}>
                          {row.rotation_delta > 0 ? `▲ +${row.rotation_delta}` : `▼ ${row.rotation_delta}`}
                        </span>
                      )}
                    </td>
                    <td style={{ textAlign: 'right', padding: '9px 10px' }}>{pct(oneDay?.positive_breadth, 0)}</td>
                    <td style={{ textAlign: 'right', padding: '9px 10px' }}>{pct(oneWeek?.positive_breadth, 0)}</td>
                    <td style={{ textAlign: 'right', padding: '9px 10px' }}>{pct(oneMonth?.positive_breadth, 0)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        <div style={{ padding: '10px 14px', borderTop: `1px solid ${colors.line}`, fontSize: 11, color: colors.muted, display: 'grid', gap: 4 }}>
          <div><strong style={{ color: colors.ink }}>Rotation</strong> — the sector's rank move from 1-day performance to 3-month performance (rank 1 = best average return). ▲ means it has risen in the standings over 3 months (a sector that lagged is now leading); ▼ means it has fallen. "No change" (0) is a real result, not missing data.</div>
          <div><strong style={{ color: colors.ink }}>Breadth</strong> — the share of a sector's stocks with a positive return over that window, shown at 1-day, 1-week, and 1-month so you can see whether participation is building or narrowing: a sector can be up on strong 1-day breadth but weak 1-month breadth (a recent, still-narrow move), or the reverse (a broad move that just had a soft day).</div>
        </div>
      </section>

      {detail && (
        <section style={{ ...panel, marginBottom: 16, padding: 14 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: 8, marginBottom: 12 }}>
            <h2 style={{ fontSize: 16, margin: 0, letterSpacing: 0 }}>{detail.sector} detail</h2>
            <div style={{ color: colors.muted, fontSize: 11 }}>
              Price results as of {intelligence.data?.trade_date ?? '—'}
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 20 }}>
            <SectorEquityContext sector={detail.sector} />

            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                <div style={{ color: colors.muted, fontSize: 11, textTransform: 'uppercase', fontWeight: 700 }}>Leaders / laggards</div>
                <select
                  aria-label="Leaders/laggards horizon"
                  value={horizon}
                  onChange={event => setHorizon(event.target.value)}
                  style={{ border: `1px solid ${colors.line}`, borderRadius: 5, background: 'var(--tm-surface)', color: colors.ink, padding: '3px 6px', fontSize: 11 }}
                >
                  {leaderWindows.map(session => (
                    <option key={session} value={session}>{windowLabels[session]}</option>
                  ))}
                </select>
              </div>
              <div style={{ display: 'flex', gap: 20 }}>
                <div style={{ flex: 1 }}>
                  <div style={{ color: colors.muted, fontSize: 10, fontWeight: 700, marginBottom: 6 }}>LEADERS</div>
                  {(detail.leaders[horizon] ?? []).map(row => (
                    <div key={row.ticker} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
                      <button type="button" onClick={() => navigate(`/ticker/${row.ticker}`)} style={{ border: 0, background: 'transparent', padding: 0, color: colors.blue, fontWeight: 700, cursor: 'pointer' }}>{row.ticker}</button>
                      <span style={{ color: colors.green, fontWeight: 700 }}>{pct(row.return_pct)}</span>
                    </div>
                  ))}
                </div>
                <div style={{ flex: 1 }}>
                  <div style={{ color: colors.muted, fontSize: 10, fontWeight: 700, marginBottom: 6 }}>LAGGARDS</div>
                  {(detail.laggards[horizon] ?? []).map(row => (
                    <div key={row.ticker} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
                      <button type="button" onClick={() => navigate(`/ticker/${row.ticker}`)} style={{ border: 0, background: 'transparent', padding: 0, color: colors.blue, fontWeight: 700, cursor: 'pointer' }}>{row.ticker}</button>
                      <span style={{ color: colors.red, fontWeight: 700 }}>{pct(row.return_pct)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>

        </section>
      )}
    </div>
  )
}
