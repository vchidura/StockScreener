import type { PersistenceStrategyRow, PersistenceSession } from '../../services/api'

export type PersistenceAxisKey = 'stability' | 'distance' | 'magnitude' | 'maturity' | 'volume'
export type PersistenceAxis = { label: string; note?: string; series?: Array<number | null> }
export type PersistenceAxes = Record<PersistenceAxisKey, PersistenceAxis> & { context: string; direction: string }
type NativeRow = Record<string, unknown>

const numeric = (value: unknown): number | null => typeof value === 'number' && Number.isFinite(value) ? value : null
const text = (value: unknown) => typeof value === 'string' ? value : ''
const native = (cell: PersistenceSession): NativeRow[] => cell.state === 'MATCH' ? cell.native_rows as unknown as NativeRow[] : []
const last = <Value,>(values: Value[]) => values[values.length - 1]
const trend = (series: Array<number | null>, low: number, high: number, down: string, up: string, steady: string) => {
  const values = series.filter((value): value is number => value != null)
  if (values.length < 2) return values.length ? 'One observation' : 'Unavailable'
  return last(values) < values[0] * low ? down : last(values) > values[0] * high ? up : steady
}

export function persistenceAxes(row: PersistenceStrategyRow): PersistenceAxes {
  const cells = row.sessions, matches = cells.filter(cell => cell.state === 'MATCH')
  const latest = last(matches), current = latest ? native(latest)[0] ?? {} : {}
  const partial = row.evaluated < cells.length
  const qualify = (note: string) => `${note}${partial ? ' | Partial coverage' : ''}`
  const distance = cells.map(cell => cell.state === 'MATCH' ? cell.distance : null)
  const volumes = cells.map(cell => cell.volume).filter((value): value is number => value != null && Number.isFinite(value))
  const volume = volumes.length ? volumes.reduce((sum, value) => sum + value, 0) / volumes.length : null
  const axes: PersistenceAxes = {
    stability: { label: !matches.length ? row.evaluated ? 'No matches' : 'Unavailable' : partial ? 'Partial coverage' : 'Steady', note: partial ? 'Unknown sessions retained' : undefined },
    distance: { label: trend(distance, .8, 1.2, 'Converging', 'Diverging', 'Stable'), series: distance, note: qualify('distance %') },
    magnitude: { label: trend(cells.map(cell => cell.magnitude), .8, 1.2, 'Decreasing', 'Increasing', 'Stable'), series: cells.map(cell => cell.magnitude), note: qualify('native magnitude') },
    maturity: { label: latest?.maturity == null ? 'Unavailable' : String(latest.maturity) },
    volume: { label: volume == null ? 'No data' : volume >= 1.5 ? 'High' : volume >= 1 ? 'Normal' : 'Low',
      note: volume == null ? undefined : qualify(`${volume.toFixed(2)}x`), series: cells.map(cell => cell.volume) },
    context: 'Unavailable', direction: row.contract.startsWith('SCAN_MOMENTUM_') ? 'Long' : row.contract.startsWith('SCAN_BEARISH_') ? 'Short' : 'Both',
  }
  if (!matches.length) return axes
  if (row.contract.startsWith('SCAN_GAPS_') || row.contract.startsWith('SCAN_FVG_')) {
    const fvg = row.contract.startsWith('SCAN_FVG_')
    const state = (cell: PersistenceSession) => {
      if (cell.state !== 'MATCH') return cell.state === 'NO_MATCH' ? 'No match' : 'Unknown'
      const types = native(cell).map(value => text(value[fvg ? 'status' : 'gap_type']))
      return fvg ? types[0] || 'Zone' : types.some(value => value.includes('In Gap')) ? 'In Gap'
        : types.some(value => /At Support|At Resistance/.test(value)) ? 'At Edge'
        : types.some(value => value.includes('Unfilled')) ? 'Unfilled' : types.some(value => value.includes('New Gap')) ? 'New gap' : 'Signal'
    }
    const sequence = cells.map(state), known = matches.map(state)
    axes.stability = { label: new Set(known).size === 1 ? `Steady (${known[0]})` : sequence.join(' -> '), note: qualify(sequence.join(' -> ')) }
    axes.distance.note = qualify(fvg ? 'distance to FVG %' : 'distance to gap %')
    const formed = new Set(matches.flatMap(cell => native(cell).filter(value => text(value.gap_date) === cell.session)
      .map(value => JSON.stringify([value.gap_date, value[fvg ? 'fvg_low' : 'gap_low'], value[fvg ? 'fvg_high' : 'gap_high']]))))
    axes.magnitude = { label: `${formed.size} new`, note: qualify(fvg ? 'zones formed in window' : 'gaps formed in window') }
    const age = numeric(current.gap_age_sessions) ?? latest?.maturity
    axes.maturity = age == null ? { label: 'Unknown' } : { label: fvg ? `${age} bars old` : age <= 5 ? 'Fresh' : age <= 15 ? 'Aging' : 'Stale', note: fvg ? undefined : `${age} sessions old` }
    axes.context = state(latest!)
  } else if (row.contract.startsWith('SCAN_MA_')) {
    const sequence = cells.map(cell => cell.state === 'MATCH' ? text(native(cell)[0]?.signal) : cell.state === 'NO_MATCH' ? 'No match' : 'Unknown')
    axes.stability = { label: new Set(matches.map(cell => text(native(cell)[0]?.signal))).size === 1 ? `Steady (${text(current.signal)})` : sequence.join(' -> '), note: qualify(sequence.join(' -> ')) }
    axes.distance = { label: trend(distance, .8, 1.2, 'Narrowing', 'Widening', 'Stable'), series: distance, note: qualify('MA spread %') }
    const movement = cells.map(cell => numeric(native(cell)[0]?.price_change_since_cross_pct))
    axes.magnitude = { label: trend(movement.map(value => value == null ? null : Math.abs(value)), .8, 1.2, 'Fading', 'Accelerating', 'Stable'), series: movement, note: qualify('move since cross %') }
    axes.maturity = { label: current.days_since_cross == null ? 'Unknown' : `${current.days_since_cross} bars since cross` }
    axes.context = text(current.weekly_signal) || 'Confirmation unavailable'
  } else if (row.contract.startsWith('SCAN_FIBONACCI_')) {
    const levels = matches.map(cell => text(native(cell)[0]?.nearest_level))
    const counts = new Map<string, number>()
    levels.forEach(level => counts.set(level, (counts.get(level) ?? 0) + 1))
    const dominant = [...counts].sort((left, right) => right[1] - left[1])[0]
    axes.stability = { label: matches.length < 2 ? 'One observation' : dominant[1] === levels.length ? 'Locked' : dominant[1] >= levels.length * .6 ? 'Sticky' : 'Drifting', note: qualify(cells.map(cell => cell.state === 'MATCH' ? text(native(cell)[0]?.nearest_level) : cell.state === 'NO_MATCH' ? 'No match' : 'Unknown').join(' -> ')) }
    axes.distance = { label: trend(distance, .7, 1.3, 'Converging', 'Diverging', 'Hovering'), series: distance, note: qualify('distance to level %') }
    const depths = cells.map(cell => numeric(native(cell)[0]?.retracement_pct)), available = depths.filter((value): value is number => value != null)
    axes.magnitude = { label: available.length < 2 ? available.length ? 'One observation' : 'Unavailable' : last(available) > available[0] + 5 ? 'Deepening' : last(available) < available[0] - 5 ? 'Recovering' : 'Stable', series: depths, note: qualify('retracement depth %') }
    axes.maturity = { label: matches.length < 2 ? 'One pivot observation' : new Set(matches.map(cell => cell.event_ids.join(','))).size === 1 ? 'Pivots stable' : 'Pivots moved', note: partial ? 'Partial coverage' : undefined }
    axes.context = dominant[0]
  } else {
    axes.stability = { label: row.sessions.some(cell => cell.direction_changed) ? 'Direction changed' : 'Steady setup', note: qualify(matches.map(cell => text(native(cell)[0]?.grade)).join(' -> ')) }
    axes.distance.note = qualify('distance to EMA 21 %')
    axes.magnitude.note = qualify('heuristic score, not probability')
    axes.context = text(current.grade) ? `Grade ${current.grade}` : 'Unavailable'
  }
  return axes
}