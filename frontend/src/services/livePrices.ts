import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  getAllTickersClosingSessions,
  getChartData,
  getLatestQuote,
  type ChartDataPoint,
  type LatestQuote,
  type TickerOverviewRow,
} from './api'

/**
 * Daily pages hold the last close until today's close is saved. During a session this adds today's
 * price from the latest published window, so a page never shows yesterday's price as current.
 * Moving averages, momentum, setups and screen criteria stay on the last close: they are daily results.
 */
export interface LivePrice {
  ticker: string
  price: number
  changePercent: number | null
  asOf: string
  open: number | null
  high: number | null
  low: number | null
  volume: number | null
}

const QUOTE_POLL_MS = 60_000
const newYorkDate = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' })

/** Today's price and session range, or null when the quote is from an earlier session. */
export function toLivePrice(quote: LatestQuote, bars: ChartDataPoint[] | undefined, session: string): LivePrice | null {
  if (quote.trade_date !== session) return null
  const today = (bars ?? []).filter(bar => newYorkDate.format(new Date(bar.time * 1000)) === session)
  return {
    ticker: quote.ticker,
    price: quote.price,
    changePercent: quote.change_percent,
    asOf: quote.as_of,
    open: today.length ? today[0].open : null,
    high: today.length ? Math.max(...today.map(bar => bar.high)) : null,
    low: today.length ? Math.min(...today.map(bar => bar.low)) : null,
    volume: today.length ? today.reduce((total, bar) => total + bar.volume, 0) : null,
  }
}

const percentFrom = (price: number, base: number | null) => (base ? (price / base - 1) * 100 : null)

export function withLiveOverview(row: TickerOverviewRow, live: LivePrice | undefined, session: string): TickerOverviewRow {
  if (!live) return row
  return {
    ...row,
    date: session,
    open: live.open ?? row.open,
    high: live.high ?? row.high,
    low: live.low ?? row.low,
    close: live.price,
    volume: live.volume ?? row.volume,
    chg_pct: live.changePercent ?? row.chg_pct,
    // A part-day volume against a full-day average would read as a collapse in volume.
    rel_vol: null,
    pct_from_high: percentFrom(live.price, row.high_52w),
    pct_from_low: percentFrom(live.price, row.low_52w),
    dist_200: percentFrom(live.price, row.sma_200),
    dist_200w: percentFrom(live.price, row.wsma_200),
  }
}

export function withLiveScreeningRow<T extends { ticker: string; values: Record<string, number | string | null> }>(
  row: T, live: LivePrice | undefined,
): T {
  if (!live) return row
  return {
    ...row,
    values: {
      ...row.values,
      price: live.price,
      change: live.changePercent === null ? row.values.change : live.changePercent / 100,
      volume: live.volume ?? row.values.volume,
    },
  } as T
}

/**
 * A stored alert holds no current price. During its session the latest published price fills that in,
 * with the direction-adjusted move from the trigger. It is a comparison, not a fill: paper entry, exit
 * and paper return stay empty until something produces them.
 */
export function withLiveAlert<T extends {
  direction: number
  trigger_price: number | null
  latest_price: number | null
  latest_price_at: string | null
  price_return: number | null
  price_return_status: string
}>(row: T, live: LivePrice | undefined): T {
  if (!live || row.latest_price !== null || !row.trigger_price || Math.abs(row.direction) !== 1) return row
  return {
    ...row,
    latest_price: live.price,
    latest_price_at: live.asOf,
    price_return: row.direction * (live.price / row.trigger_price - 1),
    price_return_status: 'LATEST_PUBLISHED_WINDOW',
  }
}

/** Market Conditions pairs each price with its day change, so the two must come from the same moment. */
export function withLiveFact<T extends { value?: { close?: number | null; return1?: number | null } | null }>(
  fact: T, live: LivePrice | undefined,
): T {
  if (!live || !fact.value) return fact
  return {
    ...fact,
    value: {
      ...fact.value,
      close: live.price,
      return1: live.changePercent === null ? fact.value.return1 : live.changePercent / 100,
    },
  } as T
}

export const formatLiveTime = (asOf: string) => `${new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit',
}).format(new Date(asOf))} ET`

/** Today while its session runs, on any page. Null once its close is saved, or outside the realtime portal. */
export function useRunningSession(): string | null {
  const { data } = useQuery({
    queryKey: ['running-session'],
    queryFn: getAllTickersClosingSessions,
    staleTime: QUOTE_POLL_MS,
    refetchInterval: QUOTE_POLL_MS,
    enabled: import.meta.env.VITE_REALTIME_ONLY === 'true',
  })
  return data?.current_is_session_close === false ? data.current_session ?? null : null
}

/** One ticker's failed read leaves that row on the last close; it never blanks the page. */
const settled = async <T,>(tickers: string[], read: (ticker: string) => Promise<T>) => {
  const results = await Promise.allSettled(tickers.map(read))
  return new Map(tickers.flatMap((ticker, index) => {
    const result = results[index]
    return result.status === 'fulfilled' ? [[ticker, result.value] as const] : []
  }))
}

export function useLivePrices(tickers: string[], session: string | null) {
  const scope = tickers.join(',')
  const active = session !== null && tickers.length > 0
  const { data: quotes } = useQuery({
    queryKey: ['live-quotes', session, scope],
    queryFn: () => settled(tickers, ticker => getLatestQuote(ticker)),
    enabled: active,
    refetchInterval: QUOTE_POLL_MS,
    placeholderData: previous => previous,
  })
  // Quotes are cheap and tell us when a window has published; bars are read only then.
  const asOf = useMemo(() => {
    const times = [...(quotes?.values() ?? [])].filter(quote => quote.trade_date === session).map(quote => quote.as_of)
    return times.length ? times.reduce((newest, time) => (Date.parse(time) > Date.parse(newest) ? time : newest)) : null
  }, [quotes, session])
  const { data: bars } = useQuery({
    queryKey: ['live-bars', session, scope, asOf],
    queryFn: () => settled(tickers, ticker => getChartData(ticker, '1d', '5m')),
    enabled: active && asOf !== null,
    staleTime: Infinity,
    placeholderData: previous => previous,
  })
  const prices = useMemo(() => {
    const result = new Map<string, LivePrice>()
    if (!active || !session) return result
    quotes?.forEach((quote, ticker) => {
      const live = toLivePrice(quote, bars?.get(ticker), session)
      if (live) result.set(ticker, live)
    })
    return result
  }, [active, session, quotes, bars])
  return { prices, asOf: active ? asOf : null }
}
