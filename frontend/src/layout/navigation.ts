import {
  Activity,
  ArrowLeftRight,
  BarChart3,
  Bell,
  FlaskConical,
  Layers,
  LayoutDashboard,
  Radar,
  Route,
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
export const DEFAULT_STOCK_SYMBOL = 'SPY'
export const LAST_STOCK_STORAGE_KEY = 'alphascreener.stocks.last-symbol.v1'

const STOCK_SYMBOL_PATTERN = /^[A-Z][A-Z0-9.\-]{0,9}$/

export function normalizeStockSymbol(value: string | null | undefined): string | null {
  const symbol = value?.trim().toUpperCase() || ''
  return STOCK_SYMBOL_PATTERN.test(symbol) ? symbol : null
}

export function readLastStockSymbol(): string {
  try {
    return normalizeStockSymbol(localStorage.getItem(LAST_STOCK_STORAGE_KEY)) ?? DEFAULT_STOCK_SYMBOL
  } catch {
    return DEFAULT_STOCK_SYMBOL
  }
}

export function rememberStockSymbol(value: string): string {
  const symbol = normalizeStockSymbol(value) ?? DEFAULT_STOCK_SYMBOL
  try {
    localStorage.setItem(LAST_STOCK_STORAGE_KEY, symbol)
  } catch {
    // Storage can be unavailable; callers still receive a valid in-session symbol.
  }
  return symbol
}

export type NavItem = {
  to: string
  label: string
  icon: LucideIcon
  end?: boolean
  activeFor?: 'ticker'
}

export type NavGroup = {
  id: string
  label: string
  icon: LucideIcon
  collapsible?: boolean
  items: NavItem[]
}

export const NAV_GROUPS: NavGroup[] = [
  {
    id: 'console',
    label: 'Console',
    icon: LayoutDashboard,
    collapsible: false,
    items: [{ to: '/', label: 'Dashboard', icon: LayoutDashboard, end: true }],
  },
  {
    id: 'market',
    label: 'Market',
    icon: Layers,
    items: [
      { to: '/overview', label: 'All Tickers', icon: Table2 },
      { to: '/sector-intelligence', label: 'Sector Intelligence', icon: Layers },
      { to: '/pattern-watch', label: 'Pattern Watch', icon: Radar },
    ],
  },
  {
    id: 'stocks',
    label: 'Stocks',
    icon: BarChart3,
    items: [
      { to: `/ticker/${DEFAULT_STOCK_SYMBOL}`, label: 'Overview', icon: Activity, activeFor: 'ticker' },
      { to: '/stock-research', label: 'Stock Research', icon: BarChart3 },
    ],
  },
  {
    id: 'strategies',
    label: 'Strategies',
    icon: Sigma,
    items: [
      { to: '/gaps', label: 'Gap & Imbalance', icon: ArrowLeftRight },
      { to: '/ma-crossover', label: 'MA Crossover', icon: TrendingUp },
      { to: '/momentum-pullback', label: 'Momentum Pullback', icon: Activity },
      { to: '/bearish-bounce', label: 'Bearish Bounce', icon: TrendingDown },
      { to: '/fibonacci', label: 'Fibonacci', icon: Sigma },
    ],
  },
  {
    id: 'options',
    label: 'Options',
    icon: FlaskConical,
    items: [
      { to: '/options', label: 'Options Research', icon: FlaskConical, end: true },
      { to: '/options/activity', label: 'Option Activity', icon: Activity },
      { to: '/options/flow', label: 'Options Flow', icon: Route },
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
  if (symbol) return ['Stocks', symbol]
  if (pathname.startsWith('/account')) return ['Account']
  for (const group of NAV_GROUPS) {
    const item = group.items
      .filter(candidate => candidate.end ? pathname === candidate.to : pathname.startsWith(candidate.to))
      .sort((left, right) => right.to.length - left.to.length)[0]
    if (item) return [group.label, item.label]
  }
  return []
}
