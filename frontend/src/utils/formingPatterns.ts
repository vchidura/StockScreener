import type { FormingChartPattern } from '../services/api'

const money = (value: number) => `$${value.toFixed(2)}`

const outcomes: Record<FormingChartPattern['type'], string> = {
  ASCENDING_TRIANGLE: 'Possible upside continuation',
  DESCENDING_TRIANGLE: 'Possible downside continuation',
  SYMMETRICAL_TRIANGLE: 'Direction remains neutral until either boundary breaks',
  RISING_WEDGE: 'Possible bearish break from the rising wedge',
  FALLING_WEDGE: 'Possible bullish break from the falling wedge',
  BULL_PENNANT: 'Possible upside continuation of the flagpole',
  BEAR_PENNANT: 'Possible downside continuation of the flagpole',
  BULL_FLAG: 'Possible upside continuation after the pullback',
  BEAR_FLAG: 'Possible downside continuation after the bounce',
  CUP_AND_HANDLE: 'Possible upside break from the handle',
  HEAD_AND_SHOULDERS: 'Possible bearish neckline break',
  INVERSE_HEAD_AND_SHOULDERS: 'Possible bullish neckline break',
  TRIPLE_TOP: 'Possible bearish break after three resistance tests',
  TRIPLE_BOTTOM: 'Possible bullish break after three support tests',
}

const lineEndPrice = (pattern: FormingChartPattern, role: 'support' | 'resistance') => {
  const points = pattern.lines.find(line => line.role === role)?.points
  return points?.length ? points[points.length - 1].price : null
}

export const formingPatternRead = (pattern: FormingChartPattern) => {
  if (pattern.bias === 'NEUTRAL') {
    const resistance = lineEndPrice(pattern, 'resistance')
    const support = lineEndPrice(pattern, 'support')
    const watch = resistance !== null && support !== null
      ? `Completed close above resistance ${money(resistance)} or below support ${money(support)}`
      : `Completed close through ${pattern.boundary_role} ${money(pattern.boundary_price)}`
    return {
      watch,
      outcome: outcomes[pattern.type],
      invalidation: 'Either confirmed boundary break ends the neutral forming state',
    }
  }

  const breakDirection = pattern.bias === 'BULLISH' ? 'above' : 'below'
  const failureDirection = pattern.bias === 'BULLISH' ? 'below' : 'above'
  return {
    watch: `Completed close ${breakDirection} ${pattern.boundary_role} ${money(pattern.boundary_price)}`,
    outcome: outcomes[pattern.type],
    invalidation: pattern.invalidation_price === null
      ? 'A confirmed opposite-boundary break invalidates the setup'
      : `Completed close ${failureDirection} ${money(pattern.invalidation_price)} invalidates`,
  }
}