import { Radar } from 'lucide-react'
import { Link, useLocation } from 'react-router-dom'

export function patternWatchPath(ticker: string, search: string): string {
  const symbol = ticker.trim().toUpperCase()
  const params = new URLSearchParams(search)
  params.set('ticker', symbol)
  params.set('interval', 'all')
  return `/pattern-watch?${params.toString()}`
}

export default function PatternWatchLink({
  ticker,
  className,
}: {
  ticker: string
  className?: string
}) {
  const location = useLocation()
  return (
    <Link
      to={patternWatchPath(ticker, location.search)}
      className={className}
      title={`Open ${ticker} in Pattern Watch`}
      aria-label={`Open ${ticker} in Pattern Watch`}
    >
      <Radar size={13} aria-hidden="true" />
    </Link>
  )
}
