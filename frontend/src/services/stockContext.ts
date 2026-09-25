import api from './api'

export interface StockContextCohort {
  snapshot_id: string
  generation: string
  payload_sha256: string
  universe: string
  expected_members: number
  eligible_ranked: number
  session: string
  market_time: string
  available_at: string
  ranking: string
  status: 'READY' | 'STALE'
}

export interface StockContextFrame {
  status: 'READY' | 'STALE' | 'UNAVAILABLE'
  reason?: string | null
  evidence_id?: string
  market_time?: string
  available_at?: string
  trend?: string | null
  momentum?: string | null
  close?: number | null
}

export interface StockContextRow {
  security_id: string
  ticker: string
  company_name?: string | null
  sector: string | null
  daily: {
    status: 'READY' | 'STALE' | 'UNAVAILABLE'
    percentile: number | null
    momentum: number | null
    price: number | null
    trend: string | null
    state: string | null
  }
  frames: Record<'1d' | '1h' | '30m', StockContextFrame>
}

export interface StockContextResponse {
  schema: 'stock_equity_context_v1'
  as_of: string
  status: string
  cohort: StockContextCohort | null
  rows: StockContextRow[]
  execution_permission: false
}

export async function getStockContext(ticker?: string): Promise<StockContextResponse> {
  const response = await api.get('/stocks/context', { params: ticker ? { ticker } : undefined })
  return response.data
}