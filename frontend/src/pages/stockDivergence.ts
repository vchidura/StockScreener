import type { StockDivergenceRow } from '../services/api'

export type StockDivergenceSort = 'ticker' | 'close' | 'return5' | 'relative5' | 'relative20' | 'relative_change5'

export const stockDivergenceColumns = [
  { key: 'ticker', label: 'Stock', help: 'Stock ticker in the tracked cohort. Opens the stock workspace. This table describes completed-session price behavior, not an entry signal.' },
  { key: 'close', label: 'Close', help: 'Last completed regular-session closing price in USD, as of the date shown below. This is a stored daily close, not a live quote. Unavailable means the required price history did not pass its coverage or quality checks.' },
  { key: 'proxy', label: 'Sector proxy', help: 'ETF used as this stock\'s sector benchmark, assigned from its dated SIC classification. It is an approximate sector comparison, not a licensed GICS classification or an exact industry peer group.' },
  { key: 'return5', label: 'Stock 5 sessions', help: 'Stock price change over the latest five completed trading sessions: ending close divided by the close five sessions earlier, minus one. Positive means rising; negative means falling. Price returns exclude dividend reinvestment and apply only explicitly reviewed splits.' },
  { key: 'benchmark_return5', label: 'Sector 5 sessions', help: 'Sector ETF price change over the same five completed sessions, from the paired stock-sector calculation. Compare this with Stock 5 sessions to distinguish broad sector movement from stock outperformance. Not a total return; unavailable pairs are not filled with zero.' },
  { key: 'relative5', label: '5 vs sector (pp)', help: 'Stock five-session return minus sector five-session return, in percentage points (pp). A stock at +2% versus a sector at +5% is -3 pp. Positive means outperformance, even if both fell. This is not a predicted return.' },
  { key: 'relative20', label: '20 vs sector (pp)', help: 'Stock 20-session price return minus sector 20-session price return, in percentage points. Positive defines Leading; negative defines Lagging in Relative state. This longer window can disagree with the latest five-session comparison.' },
  { key: 'relative_change5', label: 'Relative change 5 (pp)', help: 'Latest five-session stock-minus-sector return minus the preceding, non-overlapping five-session stock-minus-sector return. Positive means improving relative performance; negative means deteriorating. It is a change in relative performance, not the stock\'s own return.' },
  { key: 'state', label: 'Relative state', help: 'Leading or Lagging comes from the sign of 20 vs sector. Strengthening or Improving means Relative change 5 is positive; Weakening or Deteriorating means it is negative. Transition means either measure is exactly zero. Improving does not necessarily mean rising, and the label is not a confirmed reversal or trade signal.' },
  { key: 'divergence', label: '5-session divergence', help: 'Compares the stock, sector, SPY and QQQ over five completed sessions. Counter-market sector leadership: stock and sector rise while both benchmarks fall. Stock-specific strength: only the stock rises. Relative resilience: stock falls less than all three. Stock vs sector divergence: stock and benchmarks rise while sector falls. Sector weakness: stock and sector fall while benchmarks rise. Rising relative laggard: stock rises less than all three. Mixed benchmark movement: SPY/QQQ do not share a direction. No named divergence means no rule matched, not neutral or safe. Unavailable means a required input failed its checks.' },
] as const

export function compareStockDivergence(left: StockDivergenceRow, right: StockDivergenceRow, sort: StockDivergenceSort, descending: boolean) {
  const tickerOrder = left.ticker.localeCompare(right.ticker)
  if (sort === 'ticker') return tickerOrder * (descending ? -1 : 1)
  const value = (row: StockDivergenceRow) => {
    const metric = sort === 'close' || sort === 'return5' ? row.context.value?.[sort] : row.relative.value?.[sort]
    return typeof metric === 'number' && Number.isFinite(metric) ? metric : null
  }
  const leftValue = value(left), rightValue = value(right)
  if (leftValue == null || rightValue == null) return leftValue == null && rightValue == null ? tickerOrder : leftValue == null ? 1 : -1
  return (leftValue - rightValue) * (descending ? -1 : 1) || tickerOrder
}