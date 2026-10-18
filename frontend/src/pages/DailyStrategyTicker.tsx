import type { ReactNode } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { usePublishPageContext } from '../layout/pageContext'
import { CommandBar, CommandSpacer } from '../layout/PageChrome'
import { getDailyStrategyTickerPublication, type SetupSummaryRow, type ScannerMemberAnalysis } from '../services/api'
import { publicationPath, navigationPublicationPath, usePublicationQuery, useStrategyInterval } from './publicationNavigation'
import { StrategyPublication, StrategyActionContext, strategyEmptyMessage } from './StrategyPublication'
import './DailyStrategyTicker.css'
import './strategyPages.css'

type Metric = string | number | boolean | null | undefined
const value = (metric: Metric): ReactNode => metric == null ? 'Unavailable' : typeof metric === 'boolean' ? (metric ? 'Yes' : 'No') : metric
const money = (metric: number | null | undefined) => metric == null ? 'Unavailable' : `$${metric.toFixed(2)}`
const percent = (metric: number | null | undefined) => metric == null ? 'Unavailable' : `${metric.toFixed(2)}%`

const SETUP_FIELDS: Array<[keyof Omit<SetupSummaryRow, 'ticker' | 'signals'>, string]> = [
  ['trend', 'Trend'], ['trend_detail', 'Trend detail'], ['momentum', 'Momentum'],
  ['momentum_detail', 'Momentum detail'], ['bias', 'Descriptive bias'], ['conviction', 'Heuristic conviction'],
  ['confirm_interval', 'Confirmation interval'], ['confirm_trend', 'Confirmation trend'],
  ['multi_tf_agree', 'Confirmation agreement'], ['bull_signals', 'Bullish descriptors'], ['bear_signals', 'Bearish descriptors'],
  ['rsi', 'RSI'], ['rsi_state', 'RSI state'], ['stoch_k', 'Stochastic K'], ['adx', 'ADX'],
  ['macd_state', 'MACD state'], ['macd_histogram', 'MACD histogram'], ['trend_consistency', 'Trend consistency'],
  ['atr_pct', 'ATR %'], ['relative_volume', 'Scanner relative volume'],
  ['volume_trend_state', 'Adjusted-volume trend'], ['volume_pressure', 'Adjusted-volume pressure'],
  ['historical_volatility_pct', 'Retained volatility %'], ['historical_volatility_state', 'Volatility state'],
  ['range_position_pct', 'Range position %'], ['price_vs_vwap', 'Price vs VWAP'], ['golden_cross', '50/200 SMA state'],
]
const PAGES = [
  ['/gaps', 'Gap & FVG'], ['/ma-crossover', 'SMA 9/21'], ['/momentum-pullback', 'Momentum'],
  ['/bearish-bounce', 'Bearish'], ['/fibonacci', 'Fibonacci 5%'],
]

function Metrics({ items }: { items: Array<[string, Metric]> }) {
  return <dl className="daily-research__metrics">
    {items.map(([label, metric]) => <div key={label}><dt>{label}</dt><dd>{value(metric)}</dd></div>)}
  </dl>
}

function ResearchSection<T>({ title, rows, columns, analysis }: {
  title: string; rows: T[]; analysis?: ScannerMemberAnalysis; columns: Array<{ label: string; render: (row: T) => ReactNode }>
}) {
  return <section className="daily-research__section" aria-label={title}>
    <h2>{title} <small>({rows.length})</small></h2>
    {analysis && <p>{analysis.status} | {analysis.available_bars}/{analysis.required_bars} input bars</p>}
    {analysis && <StrategyActionContext member={analysis} />}
    {rows.length === 0
      ? <p>{strategyEmptyMessage(analysis, 'No matches for this ticker in this complete retained publication.')}</p>
      : <div className="daily-research__table tm-fit">
        <table className="data-table" aria-label={`${title} retained rows`}>
          <thead><tr>{columns.map(column => <th key={column.label}>{column.label}</th>)}</tr></thead>
          <tbody>{rows.map((row, index) => <tr key={`${JSON.stringify(row)}:${index}`}>
            {columns.map(column => <td key={column.label}>{column.render(row)}</td>)}
          </tr>)}</tbody>
        </table>
      </div>}
  </section>
}

export default function DailyStrategyTicker() {
  const { symbol = '' } = useParams<{ symbol: string }>()
  const ticker = symbol.toUpperCase()
  const [params] = useSearchParams()
  const { interval } = useStrategyInterval()
  const evaluationSession = params.get('evaluation_session') ?? undefined
  const query = usePublicationQuery(`daily-strategy-ticker:${ticker}${evaluationSession ? `:${evaluationSession}` : ''}`,
    pin => getDailyStrategyTickerPublication(ticker, pin, interval, evaluationSession), true)
  const data = query.data
  const overview = data?.overview
  const setup = data?.setup
  const pin = data?.publication.snapshot_id ?? params.get('snapshot_id') ?? undefined
  const cohort = data?.publication.cohort_id ?? params.get('cohort_id') ?? undefined

  usePublishPageContext({
    eyebrow: 'Strategies - retained research',
    title: `${ticker || 'Ticker'} - ${interval === '1h' ? 'hourly' : 'daily'} research`,
    detail: 'One explicit publication; no independent ticker, profile, options or calendar reads.',
    status: [{ label: 'Session', value: data?.evaluation_session ?? data?.publication.session ?? 'Unavailable' }],
    session: interval,
  })

  return <div className="daily-research strategy-workspace">
    <StrategyPublication publication={data?.publication} basis={data?.scanner_basis} error={query.error} loading={query.loading} />
    <CommandBar>
      <span>{data ? `${ticker} in ${data.cohort_count}-ticker cohort` : 'Publication unavailable'}</span>
      <CommandSpacer />
      <button type="button" className="tm-commandbar__action" onClick={query.refresh}
        disabled={query.loading || !params.get('cohort_id') || !params.get('snapshot_id')}>
        {query.loading ? 'Reading...' : 'Refresh publication'}
      </button>
    </CommandBar>
    {data?.interpretation === 'RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS' && <p>Reconstructed research | Not original on-time decisions</p>}
    {pin && cohort && <nav className="daily-research__navigation" aria-label="Daily strategy pages">
      <Link to={publicationPath('/strategies/overview', pin, cohort, interval, evaluationSession)}>Strategy Overview</Link>
      <Link to={publicationPath('/stocks/persistence', pin, cohort, interval, evaluationSession)}>Persistence</Link>
      {PAGES.map(([path, label]) => <Link key={path} to={publicationPath(path, pin, cohort, interval, evaluationSession)}>{label}</Link>)}
      <Link to={navigationPublicationPath(`/ticker/${ticker}`, `/strategy-research/${ticker}`, pin, cohort, interval)}>Full ticker workspace</Link>
    </nav>}
    {query.loading && !data && <p role="status">Reading complete publication...</p>}
    {data && <>
      <div className="daily-research__summary">
        <section className="daily-research__section" aria-label="Retained quote and raw liquidity">
          <h2>Retained quote and raw liquidity</h2>
          {overview ? <Metrics items={[
            ['Raw close', money(overview.close)], ['Raw open', money(overview.open)],
            ['Raw high', money(overview.high)], ['Raw low', money(overview.low)],
            ['Raw volume', overview.volume.toLocaleString()], ['Raw relative volume', overview.rel_vol],
            [interval === '1d' ? 'Daily price change' : 'Hourly price change', percent(overview.chg_pct)],
          ]} /> : <p>Retained quote unavailable.</p>}
          <p>{interval === '1h' ? 'Raw RTH hourly price and volume basis.' : 'Overview raw liquidity is distinct from adjusted-series scanner volume.'}</p>
        </section>
        <section className="daily-research__section" aria-label="Published daily setup">
          <h2>Published {interval === '1h' ? 'hourly' : 'daily'} setup</h2>
          {setup ? <><Metrics items={[
            ['Trend', setup.trend], ['Momentum', setup.momentum], ['RSI', setup.rsi],
            ['ADX', setup.adx], ['Confirmation agreement', setup.multi_tf_agree],
          ]} />
          <details><summary>All stored setup descriptors</summary>
            <Metrics items={SETUP_FIELDS.map(([key, label]) => [label, setup[key]])} />
            <p>Stored descriptors: {setup.signals.length ? setup.signals.join(', ') : 'None'}</p>
          </details></> : <p>Daily setup analysis unavailable.</p>}
        </section>
      </div>
      <div className="daily-research__sections">
        <ResearchSection title="Opening gaps" rows={data.scans.gaps} analysis={data.analysis?.gaps} columns={[
          { label: 'Status', render: row => row.gap_type }, { label: 'Class', render: row => value(row.gap_classification) },
          { label: 'Lifecycle', render: row => value(row.gap_lifecycle) }, { label: 'Date', render: row => row.gap_date },
          { label: 'Zone', render: row => `${money(row.gap_low)} - ${money(row.gap_high)}` },
          { label: 'Scanner close', render: row => money(row.last_close) }, { label: 'Fill', render: row => percent(row.fill_pct) },
        ]} />
        <ResearchSection title="Fair value gaps - lookback 50" rows={data.scans.fvg} analysis={data.analysis?.fvg} columns={[
          { label: 'Type', render: row => row.fvg_type }, { label: 'Status', render: row => row.status },
          { label: 'Date', render: row => row.gap_date },
          { label: 'Zone', render: row => `${money(row.fvg_low)} - ${money(row.fvg_high)}` },
          { label: 'Scanner close', render: row => money(row.last_close) }, { label: 'Streak', render: row => row.streak_count },
          { label: 'Proximity', render: row => row.proximity },
        ]} />
        <ResearchSection title="SMA 9/21" rows={data.scans.ma} analysis={data.analysis?.ma} columns={[
          { label: 'Signal', render: row => row.signal }, { label: 'Scanner close', render: row => money(row.last_close) },
          { label: 'SMA 9', render: row => money(row.short_ma) }, { label: 'SMA 21', render: row => money(row.long_ma) },
          { label: 'Spread', render: row => percent(row.ma_spread_pct) },
          { label: 'Weekly descriptive state', render: row => value(row.weekly_signal) },
        ]} />
        <ResearchSection title="Momentum Pullback" rows={data.scans.momentum} analysis={data.analysis?.momentum} columns={[
          { label: 'Heuristic grade', render: row => row.grade }, { label: 'Score (not probability)', render: row => row.score },
          { label: 'Scanner close', render: row => money(row.last_close) }, { label: 'RSI', render: row => row.rsi },
          { label: 'ADX', render: row => row.adx }, { label: 'Stochastic K', render: row => row.stoch_k },
          { label: 'Distance to EMA 21', render: row => percent(row.dist_to_ema21_pct) },
        ]} />
        <ResearchSection title="Bearish Bounce" rows={data.scans.bearish} analysis={data.analysis?.bearish} columns={[
          { label: 'Heuristic grade', render: row => row.grade }, { label: 'Score (not probability)', render: row => row.score },
          { label: 'Scanner close', render: row => money(row.last_close) }, { label: 'RSI', render: row => row.rsi },
          { label: 'ADX', render: row => row.adx }, { label: 'Stochastic K', render: row => row.stoch_k },
          { label: 'Distance to EMA 21', render: row => percent(row.dist_to_ema21_pct) },
        ]} />
        <ResearchSection title={interval === '1h' ? 'Fibonacci - interval-aware ATR swing' : 'Fibonacci - minimum swing 5%'} rows={data.scans.fibonacci} analysis={data.analysis?.fibonacci} columns={[
          { label: 'Signal', render: row => row.signal }, { label: 'Trend', render: row => row.trend_direction },
          { label: 'Scanner close', render: row => money(row.last_close) }, { label: 'Zone', render: row => row.zone },
          { label: 'Nearest level', render: row => row.nearest_level }, { label: 'Distance', render: row => percent(row.distance_pct) },
          { label: 'Retracement', render: row => percent(row.retracement_pct) },
        ]} />
      </div>
    </>}
  </div>
}
