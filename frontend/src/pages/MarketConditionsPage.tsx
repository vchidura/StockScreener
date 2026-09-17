import { useDeferredValue, useId, useLayoutEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { ArrowDownWideNarrow, ArrowUpWideNarrow, ChevronRight, Info, RefreshCw, Search, X } from 'lucide-react'
import { getMarketConditions, type RotationFact, type RotationPrices, type SectorRotationRow } from '../services/api'
import { usePublishPageContext } from '../layout/pageContext'
import { compareStockDivergence, stockDivergenceColumns, type StockDivergenceSort } from './stockDivergence'
import './MarketConditionsPage.css'

const states: Record<string, string> = {
  LEADING_STRENGTHENING: 'Leading / strengthening', LEADING_WEAKENING: 'Leading / weakening',
  LAGGING_IMPROVING: 'Lagging / improving', LAGGING_DETERIORATING: 'Lagging / deteriorating', TRANSITION: 'Transition',
}
const words = (value: string) => value.toLowerCase().replace(/_/g, ' ').replace(/^./, first => first.toUpperCase())
const percent = (value: number | null | undefined, unit = '%') => value == null || !Number.isFinite(value) ? 'Unavailable' : `${value > 0 ? '+' : ''}${(value * 100).toFixed(2)}${unit}`
const price = (value: number | undefined) => value == null ? 'Unavailable' : value.toLocaleString('en-US', { style: 'currency', currency: 'USD' })
const tone = (value: number | null | undefined) => value == null || value === 0 ? '' : value > 0 ? 'mc-positive' : 'mc-negative'
const compactNumber = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })
const tabs = [['rotation', 'Sector Rotation'], ['stocks', 'Stock Divergence'], ['indicators', 'Indicators'], ['activity', 'ETF Activity']] as const
type ConditionsTab = typeof tabs[number][0]
const scoreLabels: Record<string, string> = { momentum: 'Price momentum', participation: 'Tracked participation', volatility: 'VIX stress (inverted)', credit: 'Credit stress (inverted)' }
const contextLabels: Record<string, string> = {
  tracked_breadth: 'Tracked-stock breadth', realized_volatility: 'SPY realized volatility', intraday_volume: 'Same-time intraday volume',
  bond_etf_relative_performance: 'Bond ETF relative performance (price only)',
  vix: 'VIX stress', credit: 'High-yield credit spread', sentiment: 'Development conditions score', weights: 'Fixed development weights',
  sector_options: 'Sector options premium', etf_flows: 'ETF net creations / redemptions',
}
const statuses: Record<string, string> = { READY: 'Ready', UNAVAILABLE: 'Unavailable', PARTIAL: 'Partial coverage', NEEDS_CONFIGURATION: 'Needs configuration',
  NEEDS_APPROVAL: 'Needs usage approval', NEEDS_SOURCE: 'Needs source', NEEDS_COVERAGE: 'Needs coverage',
  NOT_IMPLEMENTED: 'Not implemented', STALE: 'Stale', INSUFFICIENT_HISTORY: 'Insufficient history', FETCH_FAILED: 'Fetch failed', CONFIGURED: 'Configured' }
const stamp = (value?: string | null) => value ? new Date(value).toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) + ' ET' : 'Not attempted'

function contextValue(key: string, fact?: RotationFact<Record<string, number | string | null>>) {
  const value = fact?.value
  if (!value) return null
  const number = (field: string) => typeof value[field] === 'number' ? value[field] as number : null
  const fraction = (field: string) => number(field) == null ? 'Unavailable' : `${(number(field)! * 100).toFixed(1)}%`
  if (key === 'tracked_breadth') return `${value.advancing} advancing / ${value.declining} declining / ${value.unchanged} unchanged. Above SMA50: ${fraction('above_sma50_fraction')}; SMA200: ${fraction('above_sma200_fraction')}.`
  if (key === 'realized_volatility') return `${fraction('annualized_volatility20')} annualized | 20 daily returns`
  if (key === 'intraday_volume') return `${value.ready_proxies} / ${value.expected_proxies} proxies with 20 comparable sessions`
  if (key === 'sentiment') return `${number('score')?.toFixed(1) ?? 'Unavailable'} / 100 | Development calculation`
  if (key === 'weights') return Object.keys(scoreLabels).map(name => `${scoreLabels[name]} ${fraction(name)}`).join(' | ')
  if (key === 'sector_options') return `${value.covered_sectors} / ${value.expected_sectors} sectors with timely retained option activity`
  if (key === 'etf_flows') return `${value.covered_proxies} / ${value.expected_proxies} proxies with paired NAV/share records`
  if (key === 'bond_etf_relative_performance') return [1, 5, 20].map(horizon => `${horizon} session${horizon === 1 ? '' : 's'}: ${percent(number(`relative${horizon}`), ' pp')} (HYG ${percent(number(`hyg_return${horizon}`))}; LQD ${percent(number(`lqd_return${horizon}`))})`).join(' | ')
  if (key === 'vix' || key === 'credit') return `${number('level')?.toFixed(2) ?? 'Unavailable'} ${key === 'vix' ? 'points' : 'bps'} | Change: ${number('change')?.toFixed(2) ?? 'Unavailable'} | Percentile: ${fraction('percentile')}`
  return null
}

function Range({ values }: { values: RotationPrices | null }) {
  if (!values) return <span className="mc-muted">Unavailable</span>
  const span = values.range252_high - values.range252_low
  const position = span > 0 ? Math.max(0, Math.min(100, 100 * (values.close - values.range252_low) / span)) : 50
  return <div className="mc-range" role="img" aria-label={`252-session range ${price(values.range252_low)} to ${price(values.range252_high)}; close ${price(values.close)}`}>
    <div className="mc-range-track"><i style={{ left: `${position}%` }} /></div>
    <div><span>{price(values.range252_low)}</span><span>{price(values.range252_high)}</span></div>
  </div>
}

function ColumnHelp({ label, help }: { label: string; help: string }) {
  const id = useId()
  const trigger = useRef<HTMLButtonElement>(null)
  const tooltip = useRef<HTMLDivElement>(null)
  const [open, setOpen] = useState(false)
  const [position, setPosition] = useState({ left: 0, top: 0 })
  useLayoutEffect(() => {
    if (!open || !trigger.current || !tooltip.current) return
    const dismiss = () => setOpen(false)
    const place = () => {
      if (!trigger.current || !tooltip.current) return
      const anchor = trigger.current.getBoundingClientRect()
      const bounds = tooltip.current.getBoundingClientRect()
      if (anchor.right <= 0 || anchor.left >= window.innerWidth || anchor.bottom <= 0 || anchor.top >= window.innerHeight) {
        dismiss()
        return
      }
      setPosition({ left: Math.max(8, Math.min(anchor.left, window.innerWidth - bounds.width - 8)),
        top: anchor.bottom + bounds.height <= window.innerHeight - 8 ? anchor.bottom : Math.max(8, anchor.top - bounds.height) })
    }
    place()
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') dismiss() }
    const outside = (event: Event) => {
      if (event.target instanceof Node && !trigger.current?.contains(event.target) && !tooltip.current?.contains(event.target)) dismiss()
    }
    document.addEventListener('keydown', escape)
    document.addEventListener('pointerdown', outside)
    window.addEventListener('scroll', place, true)
    window.addEventListener('resize', dismiss)
    return () => {
      document.removeEventListener('keydown', escape)
      document.removeEventListener('pointerdown', outside)
      window.removeEventListener('scroll', place, true)
      window.removeEventListener('resize', dismiss)
    }
  }, [open])
  const leave = (target: EventTarget | null) => {
    if (document.activeElement !== trigger.current && !(target instanceof Node && (trigger.current?.contains(target) || tooltip.current?.contains(target)))) setOpen(false)
  }
  return <>
    <button ref={trigger} type="button" className="mc-column-help" aria-label={`About ${label}`} aria-describedby={open ? id : undefined}
      onMouseEnter={() => setOpen(true)} onMouseLeave={event => leave(event.relatedTarget)}
      onFocus={() => setOpen(true)} onBlur={() => setOpen(false)} onClick={() => setOpen(true)}><Info size={13} aria-hidden="true" /></button>
    {open && createPortal(<div ref={tooltip} id={id} role="tooltip" className="mc-column-tooltip" style={position}
      onMouseLeave={event => leave(event.relatedTarget)}><strong>{label}</strong><p>{help}</p></div>, document.body)}
  </>
}

export default function MarketConditionsPage() {
  const query = useQuery({ queryKey: ['stored-market-conditions'], queryFn: getMarketConditions, staleTime: 60_000, refetchInterval: 60_000 })
  const [tab, setTab] = useState<ConditionsTab>('rotation')
  const [search, setSearch] = useState('')
  const deferredSearch = useDeferredValue(search.trim().toLowerCase())
  const [sort, setSort] = useState('relative5')
  const [descending, setDescending] = useState(true)
  const [selected, setSelected] = useState<string | null>(null)
  const [sectorFilter, setSectorFilter] = useState('')
  const [stockSort, setStockSort] = useState<StockDivergenceSort>('relative5')
  const [stockDescending, setStockDescending] = useState(true)
  const [comparisonSample, setComparisonSample] = useState<'rolling' | 'nonoverlapping' | 'first_half' | 'second_half'>('rolling')
  const data = query.data
  const comparison = data?.bond_comparison
  const sectors = data?.sectors ?? []
  const componentSources = (name: string) => name === 'momentum' ? ['SPY', 'QQQ'].map(ticker => data?.benchmarks[ticker])
    : [data?.additional_context?.[name === 'participation' ? 'tracked_breadth' : name === 'volatility' ? 'vix' : 'credit']]
  const detail = sectors.find(row => row.ticker === selected) ?? sectors.find(row => row.rotation.status === 'READY') ?? sectors[0]
  const sortValue = (row: SectorRotationRow): number | null => sort === 'return1' ? row.context.value?.return1 ?? null
    : sort === 'relative20' ? row.rotation.value?.relative20 ?? null : row.rotation.value?.relative5 ?? null
  const filteredSectors = sectors.filter(row => `${row.ticker} ${row.sector}`.toLowerCase().includes(deferredSearch)).sort((left, right) => {
    if (sort === 'ticker') return left.ticker.localeCompare(right.ticker) * (descending ? -1 : 1)
    const leftValue = sortValue(left), rightValue = sortValue(right)
    if (leftValue == null || rightValue == null) return leftValue == null && rightValue == null ? left.ticker.localeCompare(right.ticker) : leftValue == null ? 1 : -1
    return (leftValue - rightValue) * (descending ? -1 : 1) || left.ticker.localeCompare(right.ticker)
  })
  const stocks = (data?.stocks ?? []).filter(row => row.security_type !== 'ETF' && (!sectorFilter || row.proxy === sectorFilter)
    && `${row.ticker} ${row.sector ?? ''} ${row.proxy ?? ''}`.toLowerCase().includes(deferredSearch))
    .sort((left, right) => compareStockDivergence(left, right, stockSort, stockDescending))
  usePublishPageContext({ eyebrow: 'Market', title: 'Market Conditions', status: [
    { label: 'Source session', value: data?.session ?? 'Unavailable' },
    { label: 'Market', value: data?.market?.value?.direction ?? 'Unavailable' },
  ] })

  if (query.isLoading) return <div className="loading" role="status">Loading market conditions...</div>
  if (query.isError) return <div className="mc-empty" role="alert">Stored market conditions could not be loaded.
    <button type="button" onClick={() => void query.refetch()}><RefreshCw size={16} /> Retry</button></div>
  if (!data?.session) return <div className="mc-empty" role="status">Awaiting first market snapshot.</div>

  return <div className="market-conditions">
    <header className="mc-capture">
      <span>Daily close <strong>{data.session}</strong></span>
      <span>Captured {data.as_of ? new Date(data.as_of).toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) : 'Unavailable'} ET</span>
      <span className={data.stale ? 'mc-negative' : 'mc-muted'}>{data.stale ? `Stale: expected ${data.expected_session}` : 'Stored capture'}</span>
      <button className="mc-icon" type="button" title="Reload stored snapshot" aria-label="Reload stored snapshot" disabled={query.isFetching} onClick={() => void query.refetch()}><RefreshCw size={16} /></button>
    </header>

    <section className="mc-market-strip" aria-label="Market summary">
      {['SPY', 'QQQ'].map(ticker => {
        const values = data.benchmarks[ticker]?.value
        return <div className="mc-market-metric" key={ticker}>
          <Link to={`/ticker/${ticker}`}>{ticker}</Link><strong>{price(values?.close)}</strong>
          <span className={tone(values?.return1)}>{percent(values?.return1)} <small>1 session</small></span>
          <span className="mc-muted">{values?.direction ?? 'Unavailable'} | EMA50 trend</span>
        </div>
      })}
      <div className="mc-market-metric"><span>Sector coverage</span><strong>{data.coverage?.ready_sectors} / {data.coverage?.expected_sectors}</strong><span>Price comparisons ready</span><span className="mc-muted">12 fixed proxy buckets</span></div>
      <div className="mc-market-metric"><span>Stock coverage</span><strong>{data.coverage?.stock_states.READY ?? 0} / {(data.stocks ?? []).filter(row => row.security_type === 'CS').length}</strong><span>Common stocks with sector context</span><span className="mc-muted">Tracked cohort</span></div>
    </section>

    <div className="mc-tabs" role="tablist" aria-label="Market conditions views">
      {tabs.map(([key, label], index) =>
        <button type="button" key={key} id={`mc-tab-${key}`} role="tab" tabIndex={tab === key ? 0 : -1} aria-selected={tab === key} aria-controls="mc-panel" onClick={() => setTab(key)} onKeyDown={event => {
          if (!['ArrowRight', 'ArrowLeft', 'Home', 'End'].includes(event.key)) return
          event.preventDefault()
          const next = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : (index + (event.key === 'ArrowRight' ? 1 : tabs.length - 1)) % tabs.length
          setTab(tabs[next][0])
          event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>('[role=tab]')[next]?.focus()
        }}>{label}</button>)}
    </div>
    <section id="mc-panel" role="tabpanel" aria-labelledby={`mc-tab-${tab}`}>
      {(tab === 'rotation' || tab === 'stocks') && <div className="mc-toolbar">
        <label className="mc-search"><Search size={16} /><input aria-label="Search ticker or sector" placeholder="Ticker or sector" value={search} onChange={event => setSearch(event.target.value)} />
          <button className="mc-icon" type="button" title="Clear search" aria-label="Clear search" disabled={!search} onClick={() => setSearch('')}><X size={14} /></button></label>
        {tab === 'rotation' ? <>
          <select aria-label="Sort sectors" value={sort} onChange={event => setSort(event.target.value)}>
            <option value="relative5">5-session vs SPY</option><option value="relative20">20-session vs SPY</option><option value="return1">Day change</option><option value="ticker">Ticker</option>
          </select>
          <button className="mc-icon" type="button" title={descending ? 'Sort ascending' : 'Sort descending'} aria-label={descending ? 'Sort ascending' : 'Sort descending'} onClick={() => setDescending(!descending)}>{descending ? <ArrowDownWideNarrow size={18} /> : <ArrowUpWideNarrow size={18} />}</button>
        </> : <><select aria-label="Filter stock sector" value={sectorFilter} onChange={event => setSectorFilter(event.target.value)}>
          <option value="">All sectors</option>{sectors.map(row => <option key={row.ticker} value={row.ticker}>{row.sector} ({row.ticker})</option>)}
        </select>
          <select aria-label="Sort stocks" value={stockSort} onChange={event => setStockSort(event.target.value as StockDivergenceSort)}>
            <option value="relative5">5-session vs sector</option><option value="relative20">20-session vs sector</option>
            <option value="relative_change5">Relative change 5</option><option value="return5">Stock 5-session return</option>
            <option value="close">Closing price</option><option value="ticker">Ticker</option>
          </select>
          <button className="mc-icon" type="button" title={stockDescending ? 'Sort stocks ascending' : 'Sort stocks descending'} aria-label={stockDescending ? 'Sort stocks ascending' : 'Sort stocks descending'} onClick={() => setStockDescending(!stockDescending)}>{stockDescending ? <ArrowDownWideNarrow size={18} /> : <ArrowUpWideNarrow size={18} />}</button>
        </>}
        <span className="mc-muted">{tab === 'rotation' ? filteredSectors.length : stocks.length} {(tab === 'rotation' ? filteredSectors.length : stocks.length) === 1 ? 'row' : 'rows'}</span>
      </div>}

      {tab === 'rotation' && <>
        <div className="mc-table-scroll" tabIndex={0} aria-label="Sector rotation table">
          <table className="mc-table"><thead><tr><th scope="col">Sector proxy</th><th scope="col">Close</th><th scope="col">1 session</th><th scope="col">5 sessions</th><th scope="col">5 vs SPY (pp)</th><th scope="col">20 vs SPY (pp)</th><th scope="col">Rotation / persistence</th><th scope="col">Volume</th><th scope="col">252-session range</th></tr></thead>
            <tbody>{filteredSectors.map(row => <tr key={row.ticker} className={detail?.ticker === row.ticker ? 'mc-selected' : ''}>
              <th scope="row"><button className="mc-sector-select" type="button" aria-label={`Inspect ${row.ticker} rotation`} onClick={() => setSelected(row.ticker)}><span><b>{row.ticker}</b><small>{row.sector}</small></span><ChevronRight size={14} /></button></th>
              <td>{price(row.context.value?.close)}</td><td className={tone(row.context.value?.return1)}>{percent(row.context.value?.return1)}</td><td className={tone(row.context.value?.return5)}>{percent(row.context.value?.return5)}</td>
              <td className={tone(row.rotation.value?.relative5)}>{percent(row.rotation.value?.relative5, '')}</td><td className={tone(row.rotation.value?.relative20)}>{percent(row.rotation.value?.relative20, '')}</td>
              <td className="mc-state">{row.rotation.value ? states[row.rotation.value.state] : 'Unavailable'}<small>{row.rotation.value ? `${words(row.rotation.persistence?.status ?? 'PROVISIONAL')} (${row.rotation.persistence?.observations ?? 1})` : row.context.reason_codes.map(words).join(', ')}</small></td>
              <td>{row.context.value ? compactNumber.format(row.context.value.volume) : 'Unavailable'}</td><td><Range values={row.context.value} /></td>
            </tr>)}</tbody></table>
          {!filteredSectors.length && <div className="mc-empty">No matching sectors.</div>}
        </div>
        {detail && <section className="mc-detail" aria-label="Selected sector history">
          <div className="mc-detail-heading"><h2>{detail.ticker} <span>{detail.sector}</span></h2><span className="mc-muted">Reconstructed five-session trail</span></div>
          {detail.context.status !== 'READY' && <div className="mc-action-review" role="note">
            <strong>{detail.context.reason_codes.map(words).join('; ')}</strong>
            {(detail.context.action_review ?? []).map(action => <p key={action.revision_id}>
              {words(action.type)} / {action.effective_date}{action.split_from && action.split_to ? ` / ${action.split_to} for ${action.split_from}` : ''}
              {action.source ? ` / ${action.source}` : ''}<br /><code>{action.revision_id}</code>
            </p>)}
            <p>Adjustment review pending. Original price history retained.</p>
          </div>}
          {detail.context.split_adjustments?.map(adjustment => <div className="mc-action-review" key={adjustment.review_sha256}>
            <strong>Reviewed split / {adjustment.effective_date} / price factor {adjustment.price_factor}</strong>
            <p>Reviewed {adjustment.reviewed_at} / <a href={adjustment.evidence_url} target="_blank" rel="noreferrer">Issuer Form 8937</a></p>
            <p>Source action <code>{adjustment.action_revision_id}</code></p>
          </div>)}
          <div className="mc-table-scroll" tabIndex={0} aria-label={`${detail.ticker} rotation history`}><table className="mc-table mc-history"><thead><tr><th>Session</th><th>Absolute 5-session return</th><th>20-session vs SPY (pp)</th><th>Relative change 5 (pp)</th><th>State</th></tr></thead>
            <tbody>{detail.history.map(row => <tr key={row.session}><th scope="row">{row.session}</th><td className={tone(row.value?.absolute_return5)}>{percent(row.value?.absolute_return5)}</td><td className={tone(row.value?.relative20)}>{percent(row.value?.relative20, '')}</td><td className={tone(row.value?.relative_change5)}>{percent(row.value?.relative_change5, '')}</td><td>{row.value ? states[row.value.state] : 'Unavailable'}</td></tr>)}</tbody>
          </table></div>
        </section>}
      </>}

      {tab === 'stocks' && <div className="mc-table-scroll mc-stock-scroll" tabIndex={0} aria-label="Stock divergence table"><table className="mc-table mc-stocks"><thead><tr>
        {stockDivergenceColumns.map(column => <th scope="col" key={column.key} aria-sort={column.key === stockSort ? stockDescending ? 'descending' : 'ascending' : undefined}>
          <span className="mc-column-label">{column.label}<ColumnHelp label={column.label} help={column.help} /></span>
          {column.key === 'close' && <time className="mc-close-date" dateTime={data.session}>{data.session}</time>}
        </th>)}
      </tr></thead>
        <tbody>{stocks.map(row => <tr key={row.security_id}>
          <th scope="row"><Link to={`/ticker/${row.ticker}`}>{row.ticker}</Link></th><td>{price(row.context.value?.close)}</td><td>{row.proxy ?? 'Unavailable'}</td>
          <td className={tone(row.context.value?.return5)}>{percent(row.context.value?.return5)}</td>
          <td className={tone(row.relative.value?.benchmark_return5)}>{percent(row.relative.value?.benchmark_return5)}</td>
          <td className={tone(row.relative.value?.relative5)}>{percent(row.relative.value?.relative5, '')}</td>
          <td className={tone(row.relative.value?.relative20)}>{percent(row.relative.value?.relative20, '')}</td>
          <td className={tone(row.relative.value?.relative_change5)}>{percent(row.relative.value?.relative_change5, '')}</td>
          <td className="mc-state">{row.relative.value ? states[row.relative.value.state] : 'Unavailable'}</td>
          <td className="mc-state">{row.divergence.value?.labels.map(words).join('; ') ?? 'Unavailable'}{!row.relative.value && <small>{row.context.reason_codes.map(words).join(', ') || row.relative.reason_codes.map(words).join(', ')}</small>}</td>
        </tr>)}</tbody>
      </table>{!stocks.length && <div className="mc-empty">No matching stocks.</div>}</div>}

      {tab === 'indicators' && <div className="mc-indicators">
        <section className="mc-score" aria-label="Development conditions score">
          <header className="mc-detail-heading"><h2>Development Conditions</h2><span className="mc-muted">Four fixed categories | Not a probability</span></header>
          <div className="mc-score-heading">
            <strong>{typeof data.additional_context?.sentiment?.value?.score === 'number' ? data.additional_context.sentiment.value.score.toFixed(1) : 'Unavailable'}<small> / 100</small></strong>
            <span>{statuses[data.additional_context?.sentiment?.status ?? ''] ?? 'Not captured'}</span>
          </div>
          {!data.additional_context?.sentiment?.value && <p className="mc-muted" role="status">
            Required session: {data.session}. {Object.entries(scoreLabels).filter(([name]) =>
              !['READY', 'PARTIAL'].includes(data.score_components?.[name]?.status ?? '')).map(([name, label]) =>
                `${label}: ${componentSources(name).map(fact => `${statuses[fact?.status ?? ''] ?? 'Unavailable'} / source ${fact?.session ?? fact?.market_time?.slice(0, 10) ?? 'unknown'}`).join(', ')}`).join('; ')}
          </p>}
          <div className="mc-table-scroll" tabIndex={0} aria-label="Score components and weights"><table className="mc-table mc-score-table"><thead><tr><th>Component</th><th>Weight</th><th>Score</th><th>Contribution vs 50</th><th>Calculation / Status</th></tr></thead><tbody>
            {Object.entries(scoreLabels).map(([name, label]) => { const fact = data.score_components?.[name]; return <tr key={name}><th scope="row">{label}</th><td>{fact?.weight == null ? 'Unavailable' : `${(fact.weight * 100).toFixed(0)}%`}</td><td><div className="mc-score-value"><span>{fact?.value?.score == null ? 'Unavailable' : fact.value.score.toFixed(1)}</span><meter min={0} max={100} value={fact?.value?.score ?? 0} aria-label={`${label} score`} hidden={!fact?.value} /></div></td><td>{fact?.value ? `${fact.value.contribution > 0 ? '+' : ''}${fact.value.contribution.toFixed(2)}` : 'Unavailable'}</td><td className="mc-state">{fact?.method}<small>{statuses[fact?.status ?? ''] ?? 'Not captured'}{fact?.reason_codes.length ? `: ${fact.reason_codes.map(words).join('; ')}` : ''}</small><small>Source: {componentSources(name).map(source => source?.session ?? source?.market_time?.slice(0, 10) ?? 'Unknown').join(', ')}</small>{fact?.coverage && <small>Coverage: {fact.coverage.map(row => `${row.available ?? 'Unknown'} / ${row.expected ?? 'Unknown'}`).join(', ')}</small>}</td></tr> })}
          </tbody></table></div>
          <p className="mc-muted">Higher values combine stronger relative momentum, broader participation, and lower implied-volatility/credit stress. Not a vendor Fear &amp; Greed index or trading signal. Missing components are not reweighted.</p>
        </section>
        <h2>Price Momentum</h2>
        <div className="mc-table-scroll" tabIndex={0} aria-label="Market indicators"><table className="mc-table"><thead><tr><th>Benchmark</th><th>1 session</th><th>5 sessions</th><th>20 sessions</th><th>Distance to EMA50</th><th>EMA50 change (10)</th></tr></thead><tbody>
          {['SPY', 'QQQ'].map(ticker => { const values = data.benchmarks[ticker]?.value; return <tr key={ticker}><th scope="row">{ticker}</th><td>{percent(values?.return1)}</td><td>{percent(values?.return5)}</td><td>{percent(values?.return20)}</td><td>{percent(values ? values.close / values.ema50 - 1 : null)}</td><td>{percent(values?.ema50_change10)}</td></tr> })}
        </tbody></table></div>
        <h2>Additional Context</h2>
        <dl className="mc-capabilities">{Object.entries(contextLabels).filter(([key]) => !['sentiment', 'weights'].includes(key)).map(([key, label]) => {
          const fact = data.additional_context?.[key]
          const status = fact?.status ?? 'NOT_CAPTURED'
          return <div key={key} data-context={key}>
            <dt>{label}<span className={status === 'READY' ? 'mc-positive' : status === 'STALE' || status === 'FETCH_FAILED' ? 'mc-negative' : 'mc-muted'}>{statuses[status] ?? words(status)}</span></dt>
            <dd>{contextValue(key, fact) && <strong>{contextValue(key, fact)}</strong>}
              {fact?.reason_codes.length ? <span>{fact.reason_codes.map(words).join('; ')}</span> : null}
              {fact?.expected_observations != null && <span>Coverage: {fact.timely_observations ?? 0} / {fact.expected_observations}</span>}
              {fact?.market_time && <span>Source: {stamp(fact.market_time)}</span>}
              {fact?.available_at && <span>Available: {stamp(fact.available_at)}</span>}
              {key === 'bond_etf_relative_performance' && <span>HYG minus LQD; distributions excluded. Not duration neutral, a credit spread, or fund flow.</span>}
              {['vix', 'credit'].includes(key) && <span>Last fetch: {stamp(fact?.last_attempt_at)}{fact?.last_fetch_error ? ` | ${words(fact.last_fetch_error)}` : ''}</span>}
              {['vix', 'credit'].includes(key) && <span>{key === 'vix' ? 'Cboe via FRED' : 'ICE Data Indices via FRED'}: provider level, locally calculated change and percentile. {fact?.source_url && <a href={fact.source_url} target="_blank" rel="noreferrer">Source and usage terms</a>}</span>}
            </dd>
          </div>
        })}</dl>
        <section className="mc-detail" aria-label="Frozen ICE OAS comparison">
          <div className="mc-detail-heading"><h2>Bond ETF Proxy / ICE OAS</h2><span className="mc-muted">Frozen exploratory comparison</span></div>
          {comparison?.summary ? <>
            <div className="mc-capture"><span>{comparison.period_start} to {comparison.period_end}</span><span>Reference: {comparison.reference?.series_id}</span><span>{comparison.status === 'COMPARED' ? 'Historical association' : 'Comparison pending'}</span></div>
            <div className="mc-toolbar"><label htmlFor="bond-comparison-sample">Sample</label><select id="bond-comparison-sample" value={comparisonSample} onChange={event => setComparisonSample(event.target.value as typeof comparisonSample)}>
              <option value="rolling">Rolling windows</option><option value="nonoverlapping">Non-overlapping windows</option><option value="first_half">First chronological half</option><option value="second_half">Second chronological half</option>
            </select></div>
            <div className="mc-table-scroll" tabIndex={0} aria-label="Bond proxy association with OAS tightening"><table className="mc-table"><thead><tr><th scope="col">Horizon</th><th scope="col">Pearson</th><th scope="col">Spearman</th><th scope="col">Directional agreement</th><th scope="col">Nonzero pairs</th><th scope="col">Zero / tied</th><th scope="col">Matched / expected</th><th scope="col">Coverage</th></tr></thead><tbody>
              {[1, 5, 20].map(horizon => { const stats = comparison.summary?.[String(horizon)]?.[comparisonSample]; return <tr key={horizon}><th scope="row">{horizon} session{horizon === 1 ? '' : 's'}</th><td>{stats?.pearson == null ? 'Not estimable' : stats.pearson.toFixed(3)}</td><td>{stats?.spearman == null ? 'Not estimable' : stats.spearman.toFixed(3)}</td><td>{stats?.directional_agreement == null ? 'Not estimable' : `${(stats.directional_agreement * 100).toFixed(1)}%`}</td><td>{stats?.nonzero_pairs ?? 0}</td><td>{stats?.tied_or_zero_pairs ?? 0}</td><td>{stats?.matched_pairs ?? 0} / {stats?.expected_pairs ?? 0}</td><td>{stats?.coverage_fraction == null ? 'Unavailable' : `${(stats.coverage_fraction * 100).toFixed(1)}%`}</td></tr> })}
            </tbody></table></div>
            <p className="mc-muted">HYG-minus-LQD price returns versus negative OAS change (spread tightening). Different units; association is not OAS equivalence or a trading success rate.</p>
            <details className="mc-provenance"><summary>Largest direction disagreements</summary>
              <div className="mc-table-scroll" tabIndex={0} aria-label="Proxy and OAS disagreements"><table className="mc-table"><thead><tr><th>Ending session</th><th>Horizon</th><th>HYG return</th><th>LQD return</th><th>Computed difference (pp)</th><th>OAS change (bps)</th><th>Known distribution dates</th></tr></thead><tbody>
                {(comparison.largest_direction_disagreements ?? []).map(row => <tr key={`${row.session}-${row.horizon}`}><th scope="row">{row.session}</th><td>{row.horizon} sessions</td><td>{percent(row.hyg_return)}</td><td>{percent(row.lqd_return)}</td><td>{row.proxy_pp > 0 ? '+' : ''}{row.proxy_pp.toFixed(2)}</td><td>{row.oas_change_bps > 0 ? '+' : ''}{row.oas_change_bps.toFixed(2)}</td><td>{row.known_distribution_dates.join(', ') || 'None retained'}</td></tr>)}
              </tbody></table>{!comparison.largest_direction_disagreements?.length && <div className="mc-empty">No disagreement details retained.</div>}</div>
              <p>Positive OAS change means widening. Different fund durations and omitted distributions can affect ETF performance; event coverage is not certified.</p>
            </details>
            {comparison.current_proxy?.value && <details className="mc-provenance"><summary>Frozen endpoint returns ({comparison.period_end})</summary>
              <div className="mc-table-scroll" tabIndex={0}><table className="mc-table"><thead><tr><th>Horizon</th><th>HYG return</th><th>LQD return</th><th>HYG minus LQD (pp)</th></tr></thead><tbody>{[1, 5, 20].map(horizon => {
                const values = comparison.current_proxy?.value
                const number = (key: string) => typeof values?.[key] === 'number' ? values[key] as number : null
                return <tr key={horizon}><th scope="row">{horizon} sessions</th><td>{percent(number(`hyg_return${horizon}`))}</td><td>{percent(number(`lqd_return${horizon}`))}</td><td>{percent(number(`relative${horizon}`), '')}</td></tr>
              })}</tbody></table></div>
            </details>}
            <details className="mc-provenance"><summary>Comparison provenance and limits</summary><dl><div><dt>Captured</dt><dd>{stamp(comparison.cutoff)}</dd></div><div><dt>Reference received</dt><dd>{stamp(comparison.reference?.received_at)}</dd></div><div><dt>Last reference date</dt><dd>{comparison.reference?.last_observation ?? 'Unavailable'}</dd></div><div><dt>Report hash</dt><dd className="mc-hash">{comparison.report_sha256}</dd></div></dl><ul>{comparison.limitations?.map(value => <li key={value}>{value}</li>)}</ul></details>
          </> : <div className="mc-empty" role="status">{comparison?.status === 'UNAVAILABLE' ? 'Frozen comparison unavailable.' : 'Comparison pending: approved ICE OAS history and a frozen study are required.'}</div>}
        </section>
        <h2>Comparable Intraday Volume</h2>
        <div className="mc-table-scroll" tabIndex={0} aria-label="Comparable intraday volume"><table className="mc-table"><thead><tr><th>Proxy</th><th>Through (ET)</th><th>Cumulative volume</th><th>20-session average</th><th>Relative volume</th><th>Coverage</th><th>Status</th></tr></thead>
          <tbody>{Object.entries(data.intraday_volumes ?? {}).map(([ticker, fact]) => <tr key={ticker}><th scope="row">{ticker}</th><td>{fact.boundary ? stamp(fact.boundary) : 'Not captured'}</td><td>{fact.value ? compactNumber.format(fact.value.cumulative_volume) : 'Unavailable'}</td><td>{fact.value ? compactNumber.format(fact.value.average_volume20) : 'Unavailable'}</td><td>{fact.value ? `${fact.value.relative_volume.toFixed(2)}x` : 'Unavailable'}</td><td>{fact.timely_observations ?? 0} / {fact.expected_observations ?? 20}</td><td className="mc-state">{statuses[fact.status] ?? words(fact.status)}<small>{fact.reason_codes.map(words).join('; ')}</small></td></tr>)}</tbody>
        </table>{!Object.keys(data.intraday_volumes ?? {}).length && <div className="mc-empty">Volume comparison not yet captured.</div>}</div>
        <p className="mc-muted">This product uses the FRED API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.</p>
      </div>}
      {tab === 'activity' && <div className="mc-indicators">
        <h2>ETF Options Premium Activity</h2>
        <div className="mc-table-scroll" tabIndex={0} aria-label="ETF options premium activity"><table className="mc-table"><thead><tr><th>ETF</th><th>Observed market time</th><th>Call volume</th><th>Put volume</th><th>Estimated call premium</th><th>Estimated put premium</th><th>Put/call volume</th><th>Valid / retained</th><th>Status</th></tr></thead><tbody>
          {Object.entries(data.sector_option_activity ?? {}).map(([ticker, fact]) => <tr key={ticker}><th scope="row"><Link to={`/ticker/${ticker}`}>{ticker}</Link></th><td>{fact.market_time ? stamp(fact.market_time) : 'Not captured'}</td><td>{fact.value ? compactNumber.format(fact.value.call_volume) : 'Unavailable'}</td><td>{fact.value ? compactNumber.format(fact.value.put_volume) : 'Unavailable'}</td><td>{fact.value ? price(fact.value.call_premium) : 'Unavailable'}</td><td>{fact.value ? price(fact.value.put_premium) : 'Unavailable'}</td><td>{fact.value?.put_call_volume_ratio == null ? 'Unavailable' : fact.value.put_call_volume_ratio.toFixed(2)}</td><td>{fact.timely_observations ?? 0} / {fact.expected_observations ?? 'Unknown'}</td><td className="mc-state">{statuses[fact.status] ?? words(fact.status)}<small>{fact.reason_codes.map(words).join('; ')}</small></td></tr>)}
        </tbody></table>{!Object.keys(data.sector_option_activity ?? {}).length && <div className="mc-empty">Awaiting retained option activity capture.</div>}</div>
        <p className="mc-muted">Single retained matrix per ETF: cumulative day volume times retained mark and contract multiplier. Not actual traded premium, aggressor direction, or net fund flow. Calls and puts are not bullish and bearish classifications.</p>
        <h2>ETF Net Creations / Redemptions</h2>
        <div className="mc-table-scroll" tabIndex={0} aria-label="ETF net creation estimates"><table className="mc-table"><thead><tr><th>ETF</th><th>Valuation session</th><th>NAV</th><th>Shares outstanding</th><th>Adjusted share change</th><th>Estimated net creations</th><th>Status</th></tr></thead><tbody>
          {Object.entries(data.etf_creations ?? {}).map(([ticker, fact]) => <tr key={ticker}><th scope="row">{ticker}</th><td>{fact.session ?? 'Unavailable'}</td><td>{price(fact.value?.nav)}</td><td>{fact.value ? compactNumber.format(fact.value.shares_outstanding) : 'Unavailable'}</td><td>{fact.value ? fact.value.share_change.toLocaleString('en-US') : 'Unavailable'}</td><td>{price(fact.value?.net_creations_usd)}</td><td className="mc-state">{statuses[fact.status] ?? words(fact.status)}<small>{fact.reason_codes.map(words).join('; ')}</small></td></tr>)}
        </tbody></table>{!Object.keys(data.etf_creations ?? {}).length && <div className="mc-empty">Dated NAV and shares-outstanding records are required.</div>}</div>
        <p className="mc-muted">Split-adjusted share-count change times same-session NAV. Daily estimate, not all capital entering a sector; unavailable without authorized paired source records.</p>
      </div>}
    </section>
    <details className="mc-provenance"><summary>Coverage and provenance</summary><dl><div><dt>Price basis</dt><dd>{data.price_basis === 'REVIEWED_SPLIT_ADJUSTED_PRICE_V1' ? 'Reviewed split-adjusted prices; distributions excluded, not total returns' : 'Raw, action-gated; not total returns'}</dd></div><div><dt>Sector mapping</dt><dd>Dated SIC-derived proxies; not licensed GICS</dd></div><div><dt>Refresh mode</dt><dd>{words(data.refresh_mode ?? 'ONE_SHOT_CAPTURE')}</dd></div><div><dt>Snapshot</dt><dd className="mc-hash">{data.snapshot_sha256}</dd></div></dl><ul>{data.warnings?.map(warning => <li key={warning}>{warning}</li>)}</ul></details>
  </div>
}