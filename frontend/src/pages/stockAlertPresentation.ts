import type { ColumnSpec } from '../layout/PageChrome'
import type { AlertPlanRow } from '../services/stockDiscovery'

const requiredLatestColumns = new Set(['triggered_at', 'stop', 'target', 'reward_risk', 'success_probability', 'risk_assessment'])
const requiredHistoryColumns = new Set(['triggered_at', 'latest_price', 'price_return'])

export const unavailableAlertProbability = {
  label: 'Unavailable',
  reason: 'No validated, calibrated success estimate for this model, direction, interval and execution policy. Reward/risk and hits are not probabilities.',
}

export function earliestAlertWindow(streams: Array<{ publication_window_start?: string; publication_deadline?: string }> | undefined, now = Date.now()) {
  const windows = (streams || []).filter(stream => Number.isFinite(Date.parse(stream.publication_window_start || ''))
    && Number.isFinite(Date.parse(stream.publication_deadline || ''))
    && Date.parse(stream.publication_window_start!) <= Date.parse(stream.publication_deadline!))
  const upcoming = windows.filter(stream => Date.parse(stream.publication_deadline!) >= now)
  return upcoming.sort((left, right) => Date.parse(left.publication_window_start!) - Date.parse(right.publication_window_start!))[0]
    || windows.sort((left, right) => Date.parse(right.publication_deadline!) - Date.parse(left.publication_deadline!))[0] || null
}

const factNumber = (value: number, digits = 2) => Number.isFinite(value)
  ? value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits }) : 'N/A'
const factPercent = (value: number | undefined) => typeof value === 'number' && Number.isFinite(value)
  ? `${value > 0 ? '+' : ''}${factNumber(value * 100)}%` : 'N/A'
const factPoints = (value: number | undefined) => typeof value === 'number' && Number.isFinite(value)
  ? `${value > 0 ? '+' : ''}${factNumber(value * 100, 1)} pp` : 'N/A'
const factReadable = (value: string) => value.toLowerCase().replace(/_/g, ' ')
const factTime = (value: string) => new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }).format(new Date(value))

export function alertPublicationFacts(row: Pick<AlertPlanRow, 'context' | 'hits' | 'first_seen' | 'last_seen' | 'hit_models' | 'hit_intervals'>) {
  const factors = row.context?.status === 'AVAILABLE' ? row.context.factors : {}
  const facts: Array<{ label: string; value: string }> = []
  const stock = factors.stock_daily?.value
  if (stock?.direction || typeof stock?.return20 === 'number') {
    facts.push({ label: 'Ticker trend', value: `${stock.direction ? factReadable(stock.direction) : 'Direction unavailable'} / 20-session return ${factPercent(stock.return20)}` })
  }
  const relative = factors.stock_relative_rotation?.value
  if (relative?.state || typeof relative?.relative5 === 'number' || typeof relative?.relative20 === 'number') {
    facts.push({ label: 'Relative rotation', value: `${relative.state ? factReadable(relative.state) : 'State unavailable'} / 5-session ${factPoints(relative.relative5)} / 20-session ${factPoints(relative.relative20)} / change ${factPoints(relative.relative_change5)}` })
  }
  const sector = factors.sector?.value
  if (sector?.direction || typeof sector?.stock_minus_sector20 === 'number' || typeof sector?.sector_minus_spy20 === 'number') {
    facts.push({ label: 'Sector influence', value: `${sector.direction ? factReadable(sector.direction) : 'Direction unavailable'} / stock vs sector ${factPoints(sector.stock_minus_sector20)} / sector vs SPY ${factPoints(sector.sector_minus_spy20)}` })
  }
  const divergence = factors.stock_divergence?.value
  if (divergence) facts.push({ label: 'Divergence', value: divergence.labels?.length ? divergence.labels.map(factReadable).join('; ') : 'No named ticker divergence' })
  const eventFactors = [factors.earnings, factors.fomc].filter(Boolean)
  const events = eventFactors.flatMap(factor => factor?.value?.events || []).sort((left, right) => Date.parse(left.scheduled_time) - Date.parse(right.scheduled_time))
  if (events.length) {
    const event = events[0]
    facts.push({ label: 'Holding-window event', value: `${factReadable(event.type)} / ${factTime(event.scheduled_time)} ET${event.relative_timing ? ` / ${factReadable(event.relative_timing)}` : ''}` })
  } else if (eventFactors.length && eventFactors.every(factor => factor?.value?.coverage_complete)) {
    facts.push({ label: 'Holding-window event', value: 'No known earnings or FOMC event during hold' })
  } else if (eventFactors.length) {
    facts.push({ label: 'Holding-window event', value: 'Coverage incomplete' })
  }
  if (row.hits != null || row.first_seen || row.last_seen || row.hit_models.length || row.hit_intervals.length) {
    const persistence = [row.hits == null ? 'Windows unavailable' : `${row.hits} retained ${row.hits === 1 ? 'window' : 'windows'}`]
    const confirmations = [...row.hit_models.map(factReadable), ...row.hit_intervals]
    if (confirmations.length) persistence.push(confirmations.join(', '))
    if (row.first_seen || row.last_seen) persistence.push(`${row.first_seen ? factTime(row.first_seen) : 'N/A'}-${row.last_seen ? factTime(row.last_seen) : 'N/A'} ET`)
    facts.push({ label: 'Alert persistence', value: persistence.join(' / ') })
  }
  return facts.slice(0, 6)
}

export function alertPlanTiming(row: Pick<AlertPlanRow, 'interval' | 'lane' | 'hold' | 'trade_style' | 'holding_sessions' | 'confirmation_interval'>) {
  if (row.lane === 'WATCH') return { style: 'Watch', interval: row.interval, hold: 'Watch only' }
  if (row.trade_style === 'SWING' && row.confirmation_interval) return {
    style: 'Swing', interval: `Swing / 1d setup / ${row.confirmation_interval || row.interval} confirmation`,
    hold: typeof row.holding_sessions === 'number' && Number.isInteger(row.holding_sessions) && row.holding_sessions > 0
      ? `${row.holding_sessions} trading sessions` : row.hold || 'Unavailable',
  }
  return { style: row.interval === '1d' ? 'Swing' : 'Intraday',
    interval: `${row.interval === '1d' ? 'Swing' : 'Intraday'} / ${row.interval}`, hold: row.hold || 'Unavailable' }
}

export function alertColumnLayout<Column extends ColumnSpec & { history?: boolean }>(
  columns: Column[], storedHidden: Set<string>, view: 'latest' | 'history' | 'open' | 'eod',
) {
  const required = view === 'latest' ? requiredLatestColumns : view === 'eod' ? new Set<string>() : requiredHistoryColumns
  const effective = columns.map(column => ({ ...column, locked: column.locked || required.has(column.key) }))
  const locked = new Set(effective.filter(column => column.locked).map(column => column.key))
  const hidden = new Set([...storedHidden].filter(key => !locked.has(key)))
  const selectable = effective.filter(column => !column.history || view !== 'latest')
  return { columns: effective, hidden, selectable, visible: selectable.filter(column => !hidden.has(column.key)) }
}

export function alertRiskAssessment(row: Pick<AlertPlanRow, 'direction' | 'trigger_price' | 'stop' | 'target' | 'status' | 'reason' | 'warnings'>) {
  const { direction, trigger_price: trigger, stop, target } = row
  const knownPrices = [trigger, stop, target].every(value => typeof value === 'number' && Number.isFinite(value) && value > 0)
  const risk = knownPrices && (direction === 1 || direction === -1)
    ? direction * (trigger! - stop!) / trigger! : null
  const room = knownPrices ? direction * (target! - trigger!) / trigger! : null
  const validPlan = risk !== null && risk > 0 && room !== null && room > 0
  const dataIssue = /UNRESOLVED|UNAVAILABLE/.test(row.status)
  const staticWarnings = new Set(['Paper only', 'Forward paper only', 'Stop gaps can exceed planned risk',
    'Action coverage not certified', 'Short borrow unverified', 'Countertrend context',
    'Latest stored price may be stale; inspect its timestamp'])
  const warnings = [...new Set(row.warnings.filter(warning => !staticWarnings.has(warning)))]
  const context = warnings.filter(warning => warning.startsWith('Directional context:'))
  const cautions = [...context, ...warnings.filter(warning => !context.includes(warning))]
  const countertrend = context.some(warning => /^Directional context: countertrend (SHORT|LONG) against/.test(warning))
  const dataLimited = context.some(warning => warning.includes('trigger metrics unavailable:'))
  if (row.reason) cautions.unshift(row.reason.toLowerCase().replace(/_/g, ' '))
  return {
    label: !validPlan ? 'Unavailable' : dataIssue ? 'Data issue' : countertrend ? 'Countertrend' : dataLimited ? 'Data limited' : 'Unrated',
    stopDistance: validPlan ? risk : null,
    cautions,
    reason: !validPlan ? 'Missing or invalid original price, direction, stop or target.'
      : `Original bracket: ${(risk! * 100).toFixed(2)}% to stop and ${(room! * 100).toFixed(2)}% to target from trigger; not a maximum-loss estimate.`,
  }
}