import type { ConfluenceZone, TradeSetup } from '../../services/api'

type Fibonacci = NonNullable<TradeSetup['strategy_results']['fibonacci']>

export function confluenceZonesAtPrice(zones: ConfluenceZone[], price: number | null): ConfluenceZone[] {
  if (price === null || !Number.isFinite(price) || price <= 0) return zones
  return zones.map(zone => ({
    ...zone,
    role: price >= zone.low && price <= zone.high ? 'ACTIVE'
      : price > zone.high ? 'SUPPORT' : 'RESISTANCE',
    distance_pct: (zone.midpoint - price) / price * 100,
  }))
}

export function fibonacciPriceContext(fib: Fibonacci, price: number | null) {
  const validPrice = price !== null && Number.isFinite(price) && price > 0
  const confirmedRange = fib.swing_high - fib.swing_low
  const progressCurrentPct = validPrice && Number.isFinite(confirmedRange) && confirmedRange > 0
    ? Math.max(0, (fib.trend_direction === 'uptrend_retracement'
      ? fib.swing_high - price : price - fib.swing_low) / confirmedRange * 100)
    : null
  const active = fib.active_leg
  const activeRange = active ? Math.abs(active.end.price - active.start.price) : 0
  const activeRetracementPct = validPrice && active && Number.isFinite(activeRange) && activeRange > 0
    ? (active.end.type === 'high' ? active.end.price - price : price - active.end.price) / activeRange * 100
    : null
  const nearestLevel = validPrice && active
    ? active.levels.filter(level => Number.isFinite(level.price) && level.price > 0)
      .reduce<{ name: string; price: number } | null>((nearest, level) =>
        nearest === null || Math.abs(level.price - price) < Math.abs(nearest.price - price)
          ? level : nearest, null)
    : null
  const activeState = activeRetracementPct === null ? 'UNAVAILABLE'
    : activeRetracementPct < 0 ? 'EXTENDED'
      : activeRetracementPct >= 100 ? 'RETRACED' : 'WITHIN'
  return { progressCurrentPct, activeRetracementPct, nearestLevel: nearestLevel?.name ?? null, activeState }
}