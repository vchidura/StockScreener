import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getStockContext, type StockContextResponse } from '../services/stockContext'

function useEquityContext() {
  return useQuery({ queryKey: ['stock-context'], queryFn: () => getStockContext(), staleTime: 30_000, refetchInterval: 60_000 })
}

const percent = (value: number | null | undefined) => value == null ? 'Unavailable' : `${(value * 100).toFixed(1)}%`
const human = (value: string) => value.replace(/_/g, ' ').toLowerCase()

export function MomentumRanks() {
  const query = useEquityContext()
  const [sector, setSector] = useState('')
  const cohort = query.isError ? null : query.data?.cohort
  const rows = (query.data?.rows ?? []).filter(row => row.daily.percentile != null && (!sector || row.sector === sector))
  const sectors = [...new Set((query.data?.rows ?? []).map(row => row.sector).filter((value): value is string => !!value))].sort()
  return <section aria-label="Daily momentum ranks" style={{ marginBottom: 20 }}>
    <div style={{ display: 'flex', alignItems: 'baseline', flexWrap: 'wrap', gap: 12, marginBottom: 12 }}>
      <h3 style={{ margin: 0, fontSize: 16 }}>Daily momentum ranks</h3>
      <span style={{ color: 'var(--tm-muted)', fontSize: 12 }}>{cohort ? `${cohort.session} / ${cohort.eligible_ranked} ranked of ${cohort.expected_members} / ${cohort.status.toLowerCase()}` : 'Context unavailable'}</span>
      <label style={{ marginLeft: 'auto', fontSize: 12 }}>Sector <select value={sector} onChange={event => setSector(event.target.value)}>
        <option value="">All sectors</option>{sectors.map(value => <option key={value}>{value}</option>)}
      </select></label>
    </div>
    {query.isLoading ? <p>Loading published ranks...</p> : query.isError || !cohort ? <p>Published ranks unavailable.</p> :
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 280px), 1fr))', gap: 20 }}>
        {([{ label: 'Leading momentum', leading: true }, { label: 'Lagging momentum', leading: false }]).map(group => {
          const selected = rows.filter(row => group.leading ? row.daily.percentile! >= .9 : row.daily.percentile! <= .1)
            .sort((left, right) => (group.leading ? -1 : 1) * (left.daily.percentile! - right.daily.percentile!) || left.ticker.localeCompare(right.ticker)).slice(0, 10)
          return <div key={group.label}>
            <h4 style={{ fontSize: 14, margin: '0 0 8px' }}>{group.label}</h4>
            <table className="data-table" style={{ width: '100%' }}><thead><tr><th>Stock</th><th>Universe percentile</th><th>12-1 return</th></tr></thead>
              <tbody>{selected.map(row => <tr key={row.security_id}><td><Link to={`/ticker/${row.ticker}`}>{row.ticker}</Link></td>
                <td>{percent(row.daily.percentile)}</td><td>{percent(row.daily.momentum)}</td></tr>)}</tbody></table>
            {!selected.length && <p>No covered names in this group.</p>}
          </div>
        })}
      </div>}
  </section>
}

export function SectorEquityContext({ sector }: { sector: string }) {
  const query = useEquityContext()
  const cohort = query.isError ? null : query.data?.cohort
  const rows = (query.isError ? [] : query.data?.rows ?? []).filter(row => row.sector === sector)
  const ranked = rows.filter(row => row.daily.percentile != null)
  const states = rows.reduce<Record<string, number>>((counts, row) => {
    const state = row.daily.state ?? 'UNAVAILABLE'
    counts[state] = (counts[state] ?? 0) + 1
    return counts
  }, {})
  return <div aria-label={`${sector} equity context`}>
    <h3 style={{ fontSize: 14, margin: '0 0 8px' }}>Published equity context</h3>
    <div style={{ color: 'var(--tm-muted)', fontSize: 12, marginBottom: 8 }}>{cohort ? `${cohort.session} / ${cohort.status.toLowerCase()} / ${ranked.length} ranked, ${rows.length - ranked.length} unknown` : 'Daily context unavailable'}</div>
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px 14px', marginBottom: 8 }}>
      {Object.entries(states).map(([state, count]) => <span key={state} style={{ fontSize: 12 }}>{human(state)}: <strong>{count}</strong></span>)}
    </div>
    <div style={{ fontSize: 12 }}>Universe top 10%: <strong>{ranked.filter(row => row.daily.percentile! >= .9).length}</strong> / bottom 10%: <strong>{ranked.filter(row => row.daily.percentile! <= .1).length}</strong></div>
    {(['30m', '1h'] as const).map(interval => {
      const ready = rows.filter(row => row.frames[interval].status === 'READY')
      return <div key={interval} style={{ fontSize: 12, marginTop: 8 }}>
        <strong>{interval}</strong>: {ready.length} ready / {rows.length - ready.length} stale or unavailable
        {ready[0]?.frames[interval].market_time && <div style={{ color: 'var(--tm-muted)', overflowWrap: 'anywhere' }}>{ready[0].frames[interval].market_time}</div>}
      </div>
    })}
    {query.isError && <p>Optional context unavailable; sector price results are retained.</p>}
  </div>
}

export function TickerEquityContext({ context }: { context: StockContextResponse | null }) {
  const row = context?.rows.length === 1 ? context.rows[0] : null
  return <div aria-label="Published stock context" style={{ display: 'flex', flexWrap: 'wrap', gap: '6px 16px', fontSize: 12, color: 'var(--tm-muted)', margin: '8px 0' }}>
    <span>Daily momentum percentile: <strong>{percent(row?.daily.percentile)}</strong>{context?.cohort ? ` / ${context.cohort.session} / ${context.cohort.status.toLowerCase()}` : ''}</span>
    {(['30m', '1h'] as const).map(interval => <span key={interval}>{interval}: {row?.frames[interval].status === 'READY' ? row.frames[interval].trend ?? 'Unclassified' : 'Stale or unavailable'}</span>)}
  </div>
}