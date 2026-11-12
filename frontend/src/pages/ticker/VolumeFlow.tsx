// Nov 12 (TP11): Volume flow as two labelled reads, each with one picture.
// Participation: the last completed bars against their 20-bar average, RVOL and the 5-vs-20 trend.
// Pressure: Chaikin Money Flow as a sell/buy bar.
import type { TradeSetup } from '../../services/api'
import { INFO, INK, LINE, MUTED, NEG, POS, WARN, glossaryTitle, signedPct } from './tickerShared'

type Technicals = TradeSetup['technicals']

/** Bars of volume relative to the 20-bar average, a dashed 1× line, the newest bar highlighted. */
export function VolumeBars({ values }: { values: number[] }) {
  const width = 92
  const height = 28
  if (values.length < 2) return <span style={{ color: MUTED, fontSize: '0.66rem' }}>No history</span>
  const top = Math.max(1.5, ...values)
  const gap = 2
  const barWidth = (width - gap * (values.length - 1)) / values.length
  const y = (value: number) => height - (Math.min(value, top) / top) * (height - 1)
  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={`Last ${values.length} completed bars against the 20-bar average: ${values.map(value => `${value.toFixed(2)}×`).join(', ')}`}
      style={{ display: 'block', flexShrink: 0 }}
    >
      {values.map((value, index) => (
        <rect
          key={index}
          x={index * (barWidth + gap)}
          y={y(value)}
          width={barWidth}
          height={Math.max(1, height - y(value))}
          rx={1}
          fill={index === values.length - 1 ? INFO : MUTED}
          opacity={index === values.length - 1 ? 1 : 0.45}
        />
      ))}
      <line x1={0} x2={width} y1={y(1)} y2={y(1)} stroke={INK} strokeOpacity={0.55} strokeDasharray="3 2" />
    </svg>
  )
}

/** CMF from −0.5 (sell) to +0.5 (buy); values beyond are drawn at the end of the bar. */
export function CmfBar({ value }: { value: number }) {
  const width = 92
  const height = 8
  const scale = 0.5
  const clamped = Math.max(-scale, Math.min(scale, value))
  const half = width / 2
  const fill = (Math.abs(clamped) / scale) * half
  const tone = value > 0.1 ? POS : value < -0.1 ? NEG : MUTED
  return (
    <svg width={width} height={height + 4} viewBox={`0 -2 ${width} ${height + 4}`} role="img"
      aria-label={`Chaikin Money Flow ${value.toFixed(2)}`} style={{ display: 'block', flexShrink: 0 }}>
      <rect x={0} y={0} width={width} height={height} rx={4} fill={LINE} />
      <rect x={clamped >= 0 ? half : half - fill} y={0} width={fill} height={height} fill={tone} />
      <line x1={half} x2={half} y1={-2} y2={height + 2} stroke={INK} strokeOpacity={0.7} />
    </svg>
  )
}

export function VolumeFlowRead({ technicals, intraday }: { technicals: Technicals; intraday: boolean }) {
  const {
    volume_sparkline: bars, relative_volume: rvol, volume_trend_pct: trendPct,
    volume_trend_state: trendState, cmf_20: cmf, volume_pressure: pressure,
  } = technicals
  const trendTone = trendState === 'EXPANDING' ? INFO : trendState === 'CONTRACTING' ? WARN : MUTED
  const pressureTone = pressure === 'ACCUMULATION' ? POS : pressure === 'DISTRIBUTION' ? NEG : MUTED
  const rvolTitle = glossaryTitle('RVOL')
  return (
    <div className="tk-volflow">
      <div className="tk-volflow__read">
        <span className="tk-volflow__label">Participation</span>
        <div className="tk-volflow__picture">
          <VolumeBars values={bars} />
          <strong title={rvolTitle}>{rvol === null ? '—' : `${rvol.toFixed(2)}×`}{intraday ? '*' : ''}</strong>
        </div>
        <span className="tk-volflow__caption" style={{ color: trendTone }}
          title="Average volume of the last 5 completed bars against the last 20.">
          {trendPct === null ? 'trend unavailable' : `${signedPct(trendPct, 0)} 5 vs 20 bars · ${trendState.toLowerCase()}`}
        </span>
      </div>
      <div className="tk-volflow__read">
        <span className="tk-volflow__label">Pressure</span>
        <div className="tk-volflow__picture">
          {cmf === null ? <span style={{ color: MUTED }}>—</span> : <CmfBar value={cmf} />}
          <strong style={{ color: pressureTone }} title={glossaryTitle('CMF')}>
            {cmf === null ? '' : `${cmf > 0 ? '+' : ''}${cmf.toFixed(2)}`}
          </strong>
        </div>
        <span className="tk-volflow__caption" style={{ color: pressureTone }}>
          <abbr className="tk-abbr" title={glossaryTitle('CMF')}>CMF</abbr> · {pressure.toLowerCase()}
        </span>
      </div>
      {intraday && (
        <span className="tk-volflow__footnote">* last bar against the previous 20 bars of any time of day</span>
      )}
    </div>
  )
}
