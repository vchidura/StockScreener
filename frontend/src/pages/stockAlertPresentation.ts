import type { ColumnSpec } from '../layout/PageChrome'
import type { AlertPlanRow } from '../services/stockDiscovery'

const requiredLatestColumns = new Set(['stop', 'target', 'reward_risk', 'success_probability', 'risk_assessment'])
const requiredHistoryColumns = new Set(['triggered_at', 'latest_price', 'price_return'])

export const unavailableAlertProbability = {
  label: 'Unavailable',
  reason: 'No validated, calibrated success estimate for this model, direction, interval and execution policy. Reward/risk and hits are not probabilities.',
}

export function alertColumnLayout<Column extends ColumnSpec & { history?: boolean }>(
  columns: Column[], storedHidden: Set<string>, view: 'latest' | 'history',
) {
  const required = view === 'latest' ? requiredLatestColumns : requiredHistoryColumns
  const effective = columns.map(column => ({ ...column, locked: column.locked || required.has(column.key) }))
  const locked = new Set(effective.filter(column => column.locked).map(column => column.key))
  const hidden = new Set([...storedHidden].filter(key => !locked.has(key)))
  const selectable = effective.filter(column => !column.history || view === 'history')
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
  const cautions = [...new Set(row.warnings.filter(warning => !/^(Forward )?Paper only$/i.test(warning)))]
  if (row.reason) cautions.unshift(row.reason.toLowerCase().replace(/_/g, ' '))
  return {
    label: !validPlan ? 'Unavailable' : dataIssue ? 'Data issue' : 'Unrated',
    stopDistance: validPlan ? risk : null,
    cautions,
    reason: !validPlan ? 'Missing or invalid original price, direction, stop or target.'
      : 'Trigger-to-stop distance is not account risk or maximum loss. Gaps, slippage and events can increase losses; no validated aggregate risk rating is available.',
  }
}