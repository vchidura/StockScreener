import {
  Activity,
  ArrowLeftRight,
  BarChart3,
  Bell,
  FlaskConical,
  Layers,
  LayoutDashboard,
  Radar,
  Sigma,
  Star,
  Table2,
  TrendingDown,
  TrendingUp,
  type LucideIcon,
} from 'lucide-react'
import { ACCOUNT_UNAVAILABLE_REASON } from '../auth/accountState'

export const BRAND_NAME = 'AlphaScreener'
export const BRAND_SUFFIX = 'Pro'
export const BRAND_TAGLINE = 'Research Terminal'
export const BRAND_MARK = 'AS'

export type NavItem = {
  to: string
  label: string
  icon: LucideIcon
  end?: boolean
}

export type NavGroup = {
  id: string
  label: string
  items: NavItem[]
}

export const NAV_GROUPS: NavGroup[] = [
  {
    id: 'console',
    label: 'Console',
    items: [{ to: '/', label: 'Dashboard', icon: LayoutDashboard, end: true }],
  },
  {
    id: 'market',
    label: 'Market',
    items: [
      { to: '/overview', label: 'All Tickers', icon: Table2 },
      { to: '/sector-intelligence', label: 'Sector Intelligence', icon: Layers },
      { to: '/pattern-watch', label: 'Pattern Watch', icon: Radar },
    ],
  },
  {
    id: 'strategies',
    label: 'Strategies',
    items: [
      { to: '/gaps', label: 'Gap & Imbalance', icon: ArrowLeftRight },
      { to: '/ma-crossover', label: 'MA Crossover', icon: TrendingUp },
      { to: '/momentum-pullback', label: 'Momentum Pullback', icon: Activity },
      { to: '/bearish-bounce', label: 'Bearish Bounce', icon: TrendingDown },
      { to: '/fibonacci', label: 'Fibonacci', icon: Sigma },
    ],
  },
  {
    id: 'research',
    label: 'Research',
    items: [
      { to: '/stock-research', label: 'Stock Research', icon: BarChart3 },
      { to: '/options', label: 'Options Research', icon: FlaskConical },
    ],
  },
]

export const PINNED_ITEMS: Array<{ label: string; icon: LucideIcon; reason: string }> = [
  { label: 'Watchlists', icon: Star, reason: ACCOUNT_UNAVAILABLE_REASON },
  { label: 'Alerts', icon: Bell, reason: ACCOUNT_UNAVAILABLE_REASON },
]

export const NAV_ITEMS: NavItem[] = NAV_GROUPS.flatMap(group => group.items)

export function activeTickerSymbol(pathname: string): string | null {
  const match = /^\/ticker\/([^/]+)/.exec(pathname)
  return match ? decodeURIComponent(match[1]).toUpperCase() : null
}

export function breadcrumbFor(pathname: string): string[] {
  const symbol = activeTickerSymbol(pathname)
  if (symbol) return ['Ticker', symbol]
  if (pathname.startsWith('/account')) return ['Account']
  for (const group of NAV_GROUPS) {
    const item = group.items.find(candidate =>
      candidate.end ? pathname === candidate.to : pathname.startsWith(candidate.to),
    )
    if (item) return [group.label, item.label]
  }
  return []
}
