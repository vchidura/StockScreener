export const SCREENING_VERSION = 'stock_screener_workspace_v1' as const
export const GAP_VERSION = 'screening_daily_gap_context_v1' as const
export const HOURLY_VERSION = 'screening_hourly_context_v1' as const
export interface FieldSpec {
  label: string; group: string; unit: string; type: 'number' | 'category'; options: string[]
  warmup_sessions: number; source: string; interval: string; price_basis: string; versions: string[]
}
export interface PatternSpec { label: string; direction: -1 | 0 | 1; interval: '1d'; state: 'OCCURRENCE'; source: string; version: string }
export interface ScreeningCatalog {
  version: string; fields: Record<string, FieldSpec>; patterns: Record<string, PatternSpec>; deferred: Record<string, string>
  publications: { generation: string; session: string; observed_at: string; fields?: string[] }[]; status: string
  gaps?: { version: typeof GAP_VERSION; interval: '1d'; max_formation_age: number; warmup_sessions: number; fields: Record<string, FieldSpec> } | null
  hourly?: { version: typeof HOURLY_VERSION; interval: '1h'; fields: Record<string, FieldSpec> } | null
}
export interface ScreeningFilter { field: string; min?: number | null; max?: number | null; values?: string[] }
export interface GapPredicate { version: typeof GAP_VERSION; interval: '1d'; filters: ScreeningFilter[] }
export interface HourlyPredicate { version: typeof HOURLY_VERSION; interval: '1h'; filters: Omit<ScreeningFilter, 'values'>[] }
export interface PatternFilter { id: string; direction: -1 | 0 | 1; interval: '1d'; state: 'OCCURRENCE' }
export interface Predicate {
  version: typeof SCREENING_VERSION; universe: 'CONFIGURED_TRACKED_EQUITIES'; interval: '1d'
  filters: ScreeningFilter[]; patterns: PatternFilter[]; pattern_mode: 'ANY' | 'ALL' | 'NONE'
  gap?: GapPredicate | null
  hourly?: HourlyPredicate | null
}
export interface Draft { fields: Record<string, { min: string; max: string; values: string[] }>; patterns: string[]; pattern_mode: Predicate['pattern_mode']; gap?: { enabled: boolean; fields: Draft['fields'] }; hourly?: { enabled: boolean; fields: Draft['fields'] } }
export interface ScreeningRequest { predicate: Predicate; generation: string | null; view: 'CURRENT'; new_only?: boolean; sort: string; descending: boolean; offset: number; limit: number }
export interface Observation { present: boolean; direction: number; interval: string; state: string; age: number | null; session: string; version: string }
export interface Explanation { field: string; value?: number | string | null; state: string; unit?: string; reason?: string; mode?: string; observations?: { field: string; state: string; observation: Observation | null }[]; conditions?: Explanation[] }
export interface ScreeningRow {
  security_id: string; ticker: string; company_name: string | null; session: string; source_bar_id: string; reference_id: string | null
  eligible: boolean; values: Record<string, number | string | null>; missing: Record<string, string>; patterns: Record<string, Observation>; quality: string[]; explanations: Explanation[]
  new_status?: 'NEW' | 'NOT_NEW' | 'UNAVAILABLE'; new_reason?: string
  gap_summary?: { status: string; reason: string; episode_count: number; matching_count: number | null; representative: Pick<GapEpisode, 'episode_id' | 'formation_session' | 'values'> | null }
  hourly?: { values: Record<string, number | null>; missing: Record<string, string> }
}
export interface HourlySource { status: string; market_time: string | null; expected_market_time: string; source_cutoff: string | null; source_publication_id: string | null; slot_start: string; slot_minutes: number; snapshot_cutoff: string; capture?: string }
export interface HourlyDetails { generation: string; security_id: string; source: HourlySource; values: Record<string, number | null>; missing: Record<string, string>; lineage: { bars: string[]; selected_bar_id: string | null; reconstructed_from_30m: Record<string, string[]> } }
export interface GapEpisode {
  episode_id: string; formation_session: string; formation_bar_id: string; previous_bar_id: string
  zone_lower: number; zone_upper: number; zone_basis: string; nearest_boundary: string; boundary_price: number
  opening_price: number; fill_target: number; first_fill_session: string | null
  values: { direction: string; state: string; formation_age: number; fill_fraction: number; price_location: string; distance_fraction: number }
}
export interface GapDetails { generation: string; security_id: string; session: string; source_cutoff: string; status: string; reason: string; episode_count: number; matching_episodes: GapEpisode[] }
export interface ScreeningResult {
  generation: { generation: string; daily_generation?: string; session: string; market_time: string; observed_at: string; source_cutoff: string; source_publication_id: string; source_publication_status?: string; source_selected_members?: number; source_unavailable_members?: Array<{ ticker: string; security_id: string; status: string }>; rank_population: number; expected_members: number; field_coverage: Record<string, number>; pattern_coverage: Record<string, number>; capture_mode: string; action_coverage: string; persistence_status: string; hourly_source?: HourlySource; hourly_coverage?: Record<string, number> }
  rows: ScreeningRow[]; matched_count: number; unknown_count: number; nonmatch_count: number; universe_count: number; predicate_hash: string; stale: boolean
  result_count?: number; new_only?: boolean
  hourly_stale?: boolean
  comparison?: { status: string; current_session: string; previous_session: string | null; previous_generation: string | null; new_count: number | null; unavailable_count: number }
}

export function dailyCoverageNotice(generation?: Pick<ScreeningResult['generation'], 'session' | 'expected_members' | 'source_publication_status' | 'source_selected_members' | 'source_unavailable_members'>): string | null {
  if (generation?.source_publication_status !== 'DEGRADED') return null
  const selected = generation.source_selected_members
  const count = typeof selected === 'number' && Number.isInteger(selected) && selected >= 0 && selected <= generation.expected_members ? selected : 'Unknown'
  const missing = generation.source_unavailable_members?.map(member => member.ticker).join(', ')
  return `Partial daily coverage (${generation.session}): ${count} / ${generation.expected_members} source members available. ${missing ? `${missing} unavailable.` : 'Unavailable members not identified.'}`
}

export function newOnlyRequest(request: ScreeningRequest, enabled: boolean): ScreeningRequest {
  return { ...request, new_only: enabled, offset: 0 }
}

export function newComparisonReason(reason?: string): string {
  return ({ PRIOR_NONMATCH: 'New match under the same rules', PRIOR_MATCH: 'Also matched the previous session',
    NO_PRIOR_MEMBERSHIP: 'No comparable prior universe membership', ELIGIBILITY_UNAVAILABLE: 'Eligibility is not comparable',
    INSTRUMENT_TYPE_CHANGED: 'Instrument classification changed', PRIOR_FIELDS_UNAVAILABLE: 'Required prior data is unavailable',
    SOURCE_LINEAGE_UNAVAILABLE: 'Comparable source lineage is unavailable', SOURCE_HISTORY_CHANGED: 'Source history or corporate actions changed',
    PRIOR_SESSION_UNAVAILABLE: 'Previous expected session is unavailable', INCOMPATIBLE_PUBLICATIONS: 'Publication contracts are not comparable',
    REVISION_POLICY_UNAVAILABLE: 'Source revisions cannot be compared under this policy',
    HOURLY_COMPARISON_NOT_ENABLED: 'New comparison is not enabled for hourly-filtered rules',
  } as Record<string, string>)[reason || ''] || 'New comparison unavailable'
}

export function comparisonDate(session?: string | null): string {
  return session ? new Date(`${session}T00:00:00Z`).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }) : 'Unavailable'
}
export function hourlyTimestamp(value?: string | null): string {
  return value ? new Date(value).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York', timeZoneName: 'short' }) : 'Unavailable'
}
export function hourlyContextTiming(dailyTime: string | undefined, source: HourlySource): string {
  const capture = source.capture === 'LATEST_COMPLETED_HOURLY_WITH_FROZEN_DAILY' ? 'Refreshed completed hour' : 'Frozen at daily cutoff'
  return `Daily: ${hourlyTimestamp(dailyTime)} / 1h: ${hourlyTimestamp(source.market_time)} / ${source.slot_minutes}-minute slot / ${capture}`
}
export const emptyPredicate = (): Predicate => ({ version: SCREENING_VERSION, universe: 'CONFIGURED_TRACKED_EQUITIES', interval: '1d', filters: [], patterns: [], pattern_mode: 'ANY' })
export const emptyDraft = (): Draft => ({ fields: {}, patterns: [], pattern_mode: 'ANY' })
export const DEFAULT_COLUMNS = ['ticker', 'price', 'change', 'volume']

export function hasDraftFilter(input?: Draft['fields'][string]): boolean {
  return !!input && (input.min.trim() !== '' || input.max.trim() !== '' || input.values.length > 0)
}

export type FieldHelpScope = 'daily' | 'hourly' | 'gap' | 'pattern'
export interface ScreeningFieldHelp { title: string; purpose: string; meaning: string; formula?: string; example?: string; note: string }

function averageDistanceHelp(period: number, kind: 'EMA' | 'SMA'): ScreeningFieldHelp {
  const name = `${kind}${period}`
  return {
    title: `Close / ${name} - 1`,
    purpose: period === 20 ? 'Spot short-term pullbacks or how far price has stretched from its recent average, to prioritize chart review.'
      : period === 50 ? 'Check whether price is holding above or slipping below its medium-term baseline when reviewing trend continuation or deterioration.'
      : 'Separate stocks above and below their longer-term price baseline, to give a shortlist broader trend context.',
    meaning: `The percentage distance between the daily close and its ${period}-session ${kind === 'EMA' ? 'exponential' : 'simple'} moving average. ${kind === 'EMA' ? 'EMA gives more weight to recent closes.' : 'SMA gives each close equal weight.'}`,
    formula: `(Latest daily close / ${name} - 1) x 100`,
    example: `Close $110 and ${name} $100 gives +10%. A close of $95 gives -5%. Zero means the close equals the average.`,
    note: `The "- 1" converts a price ratio into a return-like distance; it does not change the ${period}-session period. Positive means above, negative below, not that the average is rising or falling. ${kind === 'EMA' ? `This screener seeds its EMA from the first close in the exact ${period}-session window (adjust=False); a chart with a longer seed history can differ.` : `The average uses the latest ${period} completed daily closes.`}`,
  }
}

const fieldHelp: Record<FieldHelpScope, Record<string, ScreeningFieldHelp>> = {
  daily: {
    ticker: { purpose: 'Identify the instrument behind a result and open the correct chart for further review.', title: 'Stock', meaning: 'The trading symbol and company name for the instrument in the selected snapshot.', note: 'Symbols can change or be reused. Screening membership and source lineage use a stable security ID, not the symbol alone.' },
    instrument_type: { purpose: 'Keep company shares separate from funds and other vehicles so your screening list contains the kind of exposure you intend to review.', title: 'Instrument type', meaning: 'The dated security classification: common stock (CS), exchange-traded fund (ETF), or exchange-traded vehicle (ETV).', note: 'This is instrument identity, not a quality or risk rating.' },
    price: { purpose: 'Keep candidates within a chosen price range and relate percentage changes to an actual price level.', title: 'Close', meaning: 'The closing price in US dollars from the selected completed daily bar.', example: '$110 means the retained daily close was $110 per share or unit.', note: 'This is not a live quote, bid/ask midpoint, or guaranteed executable price.' },
    volume: { purpose: 'See how actively an instrument traded that day. Pair it with dollar volume or relative volume for a more useful comparison.', title: 'Session volume', meaning: 'The number of shares or units reported traded during the completed daily session.', example: '1,000,000 means one million shares or units, not one million dollars.', note: 'Volume is activity, not a measure of buying versus selling pressure.' },
    change: { purpose: 'Find the latest daily movers and distinguish a recent advance from a decline within your longer-term shortlist.', title: 'Session change', meaning: 'The percentage price change from the previous completed daily close to the latest daily close.', formula: '(Latest close / previous close - 1) x 100', example: '$100 to $103 is +3%; $100 to $98 is -2%.', note: 'This is close-to-close price change, including any overnight move, not open-to-close performance or dividend-reinvested total return.' },
    dollar_volume_20: { purpose: 'Screen out lightly traded names and compare typical trading activity across different share prices. It is a liquidity proxy, not an execution guarantee.', title: 'Prior 20-session average dollar volume', meaning: 'A liquidity proxy: the arithmetic mean of daily close times daily volume over the 20 sessions before the latest session.', formula: 'Mean of (daily close x daily volume) over the previous 20 sessions', example: '$20,000,000 means roughly $20 million of daily trading activity by this proxy.', note: 'The latest session is excluded. This is not market capitalization or an exact sum of trade-by-trade dollar turnover.' },
    relative_volume_20: { purpose: 'Spot unusually active or quiet sessions relative to the stock\'s own recent history, then review the accompanying price move.', title: 'Volume / prior 20-session mean', meaning: 'Latest daily volume divided by the average volume of the preceding 20 sessions.', formula: 'Latest session volume / mean volume of the previous 20 sessions', example: '1x is average volume, 2x is twice the average, and 0.5x is half.', note: 'This is a ratio, not a percentage or an intraday time-of-day volume comparison. A zero denominator is unavailable.' },
    vs_ema20: averageDistanceHelp(20, 'EMA'),
    vs_ema50: averageDistanceHelp(50, 'EMA'),
    vs_sma200: averageDistanceHelp(200, 'SMA'),
    momentum_12_1: { purpose: 'Shortlist stocks with medium-term price gains or losses without the latest month dominating the comparison. Useful for trend context, not entry timing.', title: '12-1 momentum', meaning: 'Price performance from approximately 12 months ago to approximately one month ago. The most recent month is skipped to describe the earlier medium-term move.', formula: '(Close 21 trading sessions ago / close 252 trading sessions ago - 1) x 100', example: 'From about 12 months ago to one month ago, $100 to $105 is +5%. The latest month is excluded. Minimum 5 means at least 5%.', note: 'Here 12-1 means "12 months excluding the latest month", approximated with trading sessions. It requires 253 consecutive valid daily closes and is not the trailing 12-month return, a forecast, or a total-return calculation.' },
    momentum_6_1: { purpose: 'Check whether longer-term price strength also appears in the more recent half-year window, or whether that period shows weakness.', title: '6-1 momentum', meaning: 'Price performance from approximately six months ago to one month ago, excluding the latest 21 trading sessions.', formula: '(Close 21 trading sessions ago / close 126 trading sessions ago - 1) x 100', example: 'From about six months ago to one month ago, $100 to $105 is +5%. The latest month is excluded. Minimum 5 means at least 5%.', note: 'Requires 127 consecutive valid daily closes. This is a price return, not a percentile, annualized return or forecast. Raw returns over unequal horizons do not directly measure acceleration. Missing history, identity changes or known corporate actions can make it unavailable.' },
    momentum_3_1: { purpose: 'Spot shorter-term strength or deterioration within a longer trend. This narrower window is more sensitive to temporary price moves.', title: '3-1 momentum', meaning: 'Price performance from approximately three months ago to one month ago, excluding the latest 21 trading sessions.', formula: '(Close 21 trading sessions ago / close 63 trading sessions ago - 1) x 100', example: 'From about three months ago to one month ago, $100 to $105 is +5%. The latest month is excluded. Minimum 5 means at least 5%.', note: 'Requires 64 consecutive valid daily closes. It does not detect a reversal in the latest month or provide an entry signal. This is a price return, not a percentile or total return. Missing history, identity changes or known corporate actions can make it unavailable.' },
    momentum_percentile: { purpose: 'Narrow a large list to relative momentum leaders or laggards, such as the top 10% of your eligible tracked universe.', title: '12-1 universe percentile', meaning: 'The stock\'s position in the 12-1 momentum ranking of the full eligible tracked universe, before your filters or pagination are applied.', formula: 'Zero-based ascending rank / (eligible population - 1) x 100', example: '90% means roughly top-10% relative momentum within that eligible cohort, not a 90% price gain or chance of success.', note: '0% is the lowest and 100% the highest. It is not a whole-market rank. Equal momentum values are ordered by stable security ID, so ties can get different percentiles; a single-member cohort gets 50%.' },
    realized_volatility_21: { purpose: 'Compare recent price variability and screen for calmer or more volatile names. Higher volatility is not automatically better.', title: '21-session annualized realized volatility', meaning: 'The variability of the last 21 simple daily close-to-close returns, scaled to an annual basis.', formula: 'Sample standard deviation of 21 daily returns x square root of 252 x 100', example: '30% is an annualized historical variability estimate, not a promised 30% move.', note: 'Requires 22 daily closes. It describes past variability, not direction, implied volatility, expected return, or a guaranteed risk limit.' },
    vs_prior_high20: { purpose: 'Find stocks approaching or exceeding their recent trading range for closer breakout or resistance review, without assuming the move will hold.', title: 'Close / prior 20-session high - 1', meaning: 'The percentage distance between the latest close and the highest daily high in the preceding 20 sessions.', formula: '(Latest close / highest high of the previous 20 sessions - 1) x 100', example: 'A $102 close versus a prior high of $100 gives +2%. A $98 close gives -2%.', note: 'The latest session is excluded from the reference high. Positive means a close above that prior range, not a confirmed or profitable breakout.' },
    discovery_state: { purpose: 'Organize your review around pullbacks, bounces and resumptions instead of inspecting every stock from scratch. Confirm the price context on its chart.', title: 'Daily discovery state', meaning: 'The existing daily discovery calculation classifies the instrument as Pullback, Bounce, Resuming up, Resuming down, Trending, or Mixed.', example: 'Pullback is a derived state. Selecting it does not independently select every moving-average filter in the panel.', note: 'These are completed-session observations, not entry signals or permanent lifecycle states. The original calculation requires 253 valid sessions, price at least $5, and median dollar volume at least $20 million.' },
    discovery_trend: { purpose: 'Keep a shortlist aligned with a chosen broad daily direction before adding more specific price, gap or hourly conditions.', title: 'Daily discovery trend', meaning: 'Up, Down, or Mixed according to the existing daily discovery trend calculation.', note: 'A derived category, not a slope, strength score or prediction. It shares the original discovery state\'s 253-session, price and liquidity eligibility requirements.' },
    patterns: { purpose: 'Flag recent candle formations for chart review alongside trend and price location. A candle observation alone does not confirm a reversal.', title: 'Candle occurrences', meaning: 'Candlestick observations detected on completed daily bars, such as engulfing candles, hammer, shooting star and doji.', note: 'These are occurrences, not active setups or verified trade signals. Any/All/None applies to the selected observations; unavailable pattern data is not the same as a known absence.' },
    gaps: { purpose: 'Find stocks near a recent gap zone and distinguish remaining gaps from completed fills, so you can review meaningful price levels.', title: 'Daily gap context', meaning: 'Opening gaps formed within 0-20 trading sessions. An up gap opens at least 1% above the prior high; a down gap opens at least 1% below the prior low.', note: 'All gap conditions must match the same episode. The summary selects the matching episode nearest its fixed zone edge, then the youngest, then episode ID. Fill percentage and current distance to the zone measure different things.' },
    hourly: { purpose: 'Check recent completed-hour price action alongside the last completed daily trend. This is delayed context, not a live quote or reversal confirmation.', title: 'Hourly context (1h)', meaning: 'Completed hourly close, last-bar change, distance from a 20-bar EMA and that EMA\'s percentage change over three bars.', note: 'The hourly publisher pairs newer completed hours with an unchanged daily anchor; both source times are displayed. Older snapshots can remain frozen at their daily cutoff. The final regular-session slot can be 30 minutes. New comparisons remain unavailable for hourly-filtered rules.' },
  },
  hourly: {
    close: { purpose: 'Anchor the captured hourly percentages to a dated price and check the level at the end of that slot.', title: '1h close', meaning: 'The closing price in US dollars of the exact hourly bar selected by the retained hourly publication.', note: 'This is a completed regular-session slot, not a live price. The closing slot may be shorter than 60 minutes; its timestamp and duration are displayed.' },
    change: { purpose: 'Distinguish a daily candidate\'s latest captured short-term advance from a decline; a positive slot alone does not prove a reversal.', title: '1h last-bar change', meaning: 'The percentage change from the preceding completed hourly slot\'s close to the latest captured slot\'s close.', formula: '(Latest hourly close / previous slot close - 1) x 100', example: '$100 to $101 gives +1%. A minimum of 0% includes unchanged prices.', note: 'This is not the daily session change. At the first slot of a session it includes the overnight move; the final slot may last only 30 minutes.' },
    vs_ema20: { purpose: 'See whether captured hourly price is above, below or stretched away from its recent hourly baseline while reviewing a daily candidate.', title: '1h close / EMA20 - 1', meaning: 'The percentage distance from the latest hourly close to its 20-bar exponential moving average.', formula: '(Latest hourly close / hourly EMA20 - 1) x 100', example: '+2% means the close is 2% above the hourly average; -2% means below.', note: '20 means completed hourly slots, not 20 days. The EMA is seeded from the first close in exactly 20 valid slots (adjust=False). "- 1" removes the ratio baseline, not one bar. Distance alone does not say whether the average is rising.' },
    ema20_change_3: { purpose: 'Check whether the hourly average itself has risen or fallen, rather than mistaking price above an average for a rising trend.', title: '1h EMA20 change over 3 bars', meaning: 'The percentage change of the hourly 20-bar EMA between the latest slot and three slots earlier.', formula: '(Latest EMA20 / EMA20 three slots earlier - 1) x 100', example: 'An EMA rising from $100 to $100.50 gives +0.5%; zero means unchanged.', note: 'Both EMA values use one shared 23-bar seeded sequence (adjust=False), unlike the 20-bar seed of the separate distance field. Three slots may cross overnight gaps and include shortened bars; they are not necessarily three elapsed hours.' },
  },
  gap: {
    direction: { purpose: 'Separate upward and downward opening gaps before reviewing their current fill and location.', title: 'Gap direction', meaning: 'Up or Down describes the direction of the original opening gap beyond the preceding daily high or low.', note: 'It is the formation direction, not today\'s price move or a buy/sell recommendation. All selected gap conditions apply to one episode.' },
    state: { purpose: 'Distinguish gaps that still have an unfilled portion from completed fills, instead of treating all gap observations alike.', title: 'Gap fill state', meaning: 'Open, Partially filled, Filled, Formation-session fade, or Failed at current close, using the existing gap fill calculation.', note: 'Failed at current close is reversible after subsequent closes. It does not mean permanently invalidated, and a gap leaving the 20-session window is not proof of failure.' },
    formation_age: { purpose: 'Focus on recent gap formations or deliberately include older ones; age alone does not establish how relevant the price level remains.', title: 'Formation age', meaning: 'The number of expected trading sessions since the gap formed, within the supported 0-20-session window.', example: '0 means formed in the latest session; 5 means five trading sessions have elapsed, not five calendar days.', note: 'The source window must be complete. Missing sessions do not count as unchanged observations or make the gap appear younger.' },
    fill_fraction: { purpose: 'Measure how much of the opening move has been retraced, separating shallow retracements from nearly or fully filled gaps.', title: 'Opening gap filled', meaning: 'The greatest retracement from the formation open toward the previous close, including the formation bar, clamped to 0-100%.', formula: 'Maximum retracement toward previous close / original open-to-previous-close gap x 100', example: 'For an up gap from a $100 prior close to a $110 open, a subsequent low of $105 means 50% filled.', note: 'This measures the original opening gap, not necessarily the fixed zone shown in the detail. Once 100% has been reached, a later bounce does not undo that historical fill.' },
    price_location: { purpose: 'Understand which side of the retained zone price occupies, so a small boundary distance is not mistaken for support or resistance by itself.', title: 'Price vs gap zone', meaning: 'Whether the latest daily close is Above, Inside, or Below the fixed formation zone.', note: 'Equality to either boundary counts as Inside. The zone is the surviving formation range gap, or the open-to-previous-close range when no range gap survives; the episode details name its basis.' },
    distance_fraction: { purpose: 'Prioritize gaps close to the current price and filter out distant zones when building a price-level review list.', title: 'Distance to nearest zone edge', meaning: 'The absolute percentage distance from the latest daily close to the nearest fixed gap-zone boundary.', formula: 'Absolute value of (close - nearest edge) / nearest edge x 100', example: 'Close $102 and nearest edge $100 gives a 2% distance.', note: 'Distance is never negative; use Price vs gap zone for location. The lower edge wins an equal-distance tie. It is not a stop-loss distance or expected return.' },
  },
  pattern: {},
}

export function screeningFieldHelp(field: string, scope: FieldHelpScope = 'daily'): ScreeningFieldHelp | undefined {
  if (scope === 'daily' && field === 'hourly_behavior') return {
    title: '1h filter context',
    purpose: 'See the observed hourly values against this screen\'s selected conditions, without assuming every screener uses the same indicators.',
    meaning: 'Each selected hourly field is described with its observed value and applied inclusive bounds. Daily state or trend is mentioned only when it is a selected daily condition.',
    note: 'Describes already-published observations, not a prediction, confirmed reversal, crossover, or change since the previous screen refresh. Unselected indicators are omitted. Missing values remain unavailable. Daily and hourly observations can have different source dates.',
  }
  if (scope === 'pattern') {
    const label = ({ bullish_engulfing: 'Bullish engulfing', bearish_engulfing: 'Bearish engulfing', shooting_star: 'Shooting star', hammer: 'Hammer', doji: 'Doji' } as Record<string, string>)[field]
    return label ? { ...fieldHelp.daily.patterns, title: label, meaning: `${label} is a completed daily candlestick observation from the existing detector. Direction is descriptive, not a forecast.` } : undefined
  }
  return fieldHelp[scope][field]
}

export function screeningColumns(columns: readonly string[]): string[] {
  return [...DEFAULT_COLUMNS, ...new Set(columns.filter(column => !DEFAULT_COLUMNS.includes(column)))]
}

export function appliedScreeningColumns(columns: readonly string[], predicate: Predicate): string[] {
  return screeningColumns([...columns, ...predicate.filters.map(filter => filter.field),
    ...(predicate.patterns.length ? ['patterns'] : []), ...(predicate.gap ? ['gaps'] : []),
    ...(predicate.hourly ? ['hourly'] : [])])
}

export function screeningDisplayColumns(columns: readonly string[], predicate: Predicate): string[] {
  const selected = screeningColumns(columns.filter(column => column !== 'hourly_behavior'))
  if (!predicate.hourly) return selected
  if (!selected.includes('hourly')) selected.push('hourly')
  selected.splice(selected.indexOf('hourly') + 1, 0, 'hourly_behavior')
  return selected
}

export function screeningTimeframeLabel(field: string, label: string, hasHourly: boolean): string {
  if (!hasHourly) return label
  if (field === 'price') return 'Daily close'
  if (field === 'change') return 'Daily change'
  if (field === 'volume') return 'Daily volume'
  if (['vs_ema20', 'vs_ema50', 'vs_sma200', 'vs_prior_high20'].includes(field)) return `Daily ${label}`
  return label
}

const hourlyObservationNumbers = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 })
const hourlyBoundNumbers = new Intl.NumberFormat('en-US', { maximumSignificantDigits: 15 })

export function hourlyObservedBehavior(row: Pick<ScreeningRow, 'values' | 'hourly'>, predicate: Predicate, stale = false): string {
  if (!predicate.hourly) return ''
  const values = row.hourly?.values
  const known = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value)
  const observations: string[] = stale ? ['Stale hourly context.'] : []
  const state = row.values.discovery_state
  if (predicate.filters.some(filter => filter.field === 'discovery_state') && typeof state === 'string' && ['PULLBACK', 'BOUNCE', 'RESUMING_UP', 'RESUMING_DOWN', 'TRENDING', 'MIXED'].includes(state)) {
    observations.push(`Daily ${formatValue(state).toLowerCase()}.`)
  } else if (predicate.filters.some(filter => filter.field === 'discovery_trend') && ['UP', 'DOWN', 'MIXED'].includes(String(row.values.discovery_trend))) {
    observations.push(`Daily trend ${formatValue(row.values.discovery_trend).toLowerCase()}.`)
  }
  for (const filter of predicate.hourly.filters) {
    const value = values?.[filter.field]
    const spec = { unit: filter.field === 'close' ? 'USD' : 'fraction' }
    const formatted = (amount: number, bound = false) => {
      const formatter = bound ? hourlyBoundNumbers : hourlyObservationNumbers
      return spec.unit === 'USD' ? `$${formatter.format(amount)}` : `${formatter.format(amount * 100)}%`
    }
    const bounds = [filter.min != null ? `min ${formatted(filter.min, true)}` : '', filter.max != null ? `max ${formatted(filter.max, true)}` : ''].filter(Boolean).join(', ')
    let observation = `${fieldHelp.hourly[filter.field]?.title || '1h field'} unavailable`
    if (known(value)) {
      const magnitude = value !== 0 && Math.abs(value) < .0001 ? '<0.01%' : formatted(Math.abs(value))
      if (filter.field === 'close') observation = `1h close ${formatted(value)}`
      else if (filter.field === 'change') observation = value === 0 ? '1h close unchanged' : `1h close ${value > 0 ? 'rose' : 'fell'} ${magnitude}`
      else if (filter.field === 'vs_ema20') observation = value === 0 ? 'Price at 1h EMA20' : `Price ${magnitude} ${value > 0 ? 'above' : 'below'} 1h EMA20`
      else if (filter.field === 'ema20_change_3') observation = value === 0 ? '1h EMA20 flat over 3 bars' : `1h EMA20 ${value > 0 ? 'rose' : 'fell'} ${magnitude} over 3 bars`
    }
    observations.push(`${observation} (${bounds}).`)
  }
  return observations.join(' ')
}

export function screeningColumnGroup(field: string, spec?: Pick<FieldSpec, 'group'>): string {
  if (['ticker', 'instrument_type', 'price', 'change'].includes(field)) return 'Stock & price'
  if (['volume', 'dollar_volume_20', 'relative_volume_20'].includes(field)) return 'Liquidity & activity'
  if (['momentum_12_1', 'momentum_6_1', 'momentum_3_1', 'momentum_percentile'].includes(field)) return 'Momentum'
  if (['vs_ema20', 'vs_ema50', 'vs_sma200', 'discovery_trend'].includes(field)) return 'Trend'
  if (['discovery_state', 'patterns', 'gaps', 'hourly', 'hourly_behavior'].includes(field)) return 'Setups & context'
  return spec?.group || 'Other'
}

export function constantScreeningColumns(predicate?: Predicate): string[] {
  const fixed = new Set<string>()
  for (const filter of predicate?.filters || []) {
    if (filter.values?.length && new Set(filter.values).size === 1 || filter.min != null && filter.max != null && filter.min === filter.max) fixed.add(filter.field)
  }
  const states = predicate?.filters.find(filter => filter.field === 'discovery_state')?.values
  if (states?.length && (states.every(state => ['PULLBACK', 'RESUMING_UP'].includes(state)) || states.every(state => ['BOUNCE', 'RESUMING_DOWN'].includes(state)))) fixed.add('discovery_trend')
  return [...fixed].filter(column => !DEFAULT_COLUMNS.includes(column))
}

export function screeningColumnPresets(catalog: ScreeningCatalog | undefined, screenColumns: string[] = DEFAULT_COLUMNS, predicate?: Predicate): Record<string, { label: string; columns: string[] }> {
  const fixed = constantScreeningColumns(predicate)
  const presets = {
    screen: { label: 'Screen default', columns: screenColumns },
    overview: { label: 'Overview', columns: DEFAULT_COLUMNS },
    trend: { label: 'Trend', columns: ['ticker', 'price', 'discovery_state', 'discovery_trend', 'vs_ema20', 'vs_ema50', 'vs_sma200'] },
    momentum: { label: 'Momentum', columns: ['ticker', 'price', 'change', 'momentum_12_1', 'momentum_percentile', 'relative_volume_20'] },
    liquidity: { label: 'Liquidity', columns: ['ticker', 'price', 'volume', 'dollar_volume_20', 'relative_volume_20', 'realized_volatility_21'] },
  }
  return Object.fromEntries(Object.entries(presets).map(([key, preset]) => [key, { ...preset, columns: screeningColumns(preset.columns.filter(column => !fixed.includes(column) && (column === 'patterns' || column === 'hourly' && !!catalog?.hourly || !!catalog?.fields[column]))) }]))
}

export function selectedScreeningColumnPreset(columns: string[], presets: ReturnType<typeof screeningColumnPresets>): string {
  const displayed = screeningColumns(columns)
  return Object.entries(presets).find(([, preset]) => preset.columns.length === displayed.length && preset.columns.every((column, index) => column === displayed[index]))?.[0] || ''
}

export type ScreenLayout = Pick<SavedScreen, 'columns' | 'sort'>
export interface BuiltInScreen extends ScreenLayout {
  id: string; name: string; group: 'DEFAULT' | 'RECIPE'; summary: string; qualification: string
  predicate: Predicate | null; unavailable: string | null
}

const momentumColumns = ['ticker', 'price', 'momentum_12_1', 'momentum_percentile', 'dollar_volume_20', 'relative_volume_20']
const builtInScreens: BuiltInScreen[] = [
  { id: 'all', name: 'All equities', group: 'DEFAULT', summary: 'All eligible common stocks, ETFs and ETVs.', qualification: 'Current field-level universe',
    predicate: emptyPredicate(), sort: { field: 'ticker', descending: false }, columns: DEFAULT_COLUMNS, unavailable: null },
  { id: 'leaders', name: 'Long interest', group: 'DEFAULT', summary: 'Top 10% momentum percentile; 12-1 return >= 0.01%; price >= $5; prior average dollar volume >= $20M.', qualification: 'Current daily recipe, not the former discovery cohort',
    predicate: { ...emptyPredicate(), filters: [{ field: 'price', min: 5 }, { field: 'dollar_volume_20', min: 20_000_000 }, { field: 'momentum_percentile', min: .9 }, { field: 'momentum_12_1', min: .0001 }] },
    sort: { field: 'momentum_12_1', descending: true }, columns: momentumColumns, unavailable: null },
  { id: 'laggards', name: 'Bearish risk', group: 'DEFAULT', summary: 'Bottom 10% momentum percentile; 12-1 return <= -0.01%; price >= $5; prior average dollar volume >= $20M.', qualification: 'Current daily recipe, not the former discovery cohort',
    predicate: { ...emptyPredicate(), filters: [{ field: 'price', min: 5 }, { field: 'dollar_volume_20', min: 20_000_000 }, { field: 'momentum_percentile', max: .1 }, { field: 'momentum_12_1', max: -.0001 }] },
    sort: { field: 'momentum_12_1', descending: false }, columns: momentumColumns, unavailable: null },
  ...[
    { id: 'pullbacks', name: 'Pullbacks', state: 'PULLBACK' },
    { id: 'bounces', name: 'Bounces', state: 'BOUNCE' },
    { id: 'resuming-up', name: 'Resuming up', state: 'RESUMING_UP' },
    { id: 'resuming-down', name: 'Resuming down', state: 'RESUMING_DOWN' },
  ].map(({ id, name, state }): BuiltInScreen => ({ id, name, group: 'DEFAULT', summary: `Daily discovery state: ${state.replace(/_/g, ' ').toLowerCase()}.`, qualification: 'Original state rules; 253 valid sessions, price >= $5, median dollar volume >= $20M',
    predicate: { ...emptyPredicate(), filters: [{ field: 'discovery_state', values: [state] }] },
    sort: { field: 'momentum_12_1', descending: state === 'PULLBACK' || state === 'RESUMING_UP' },
    columns: ['ticker', 'price', 'discovery_state', 'discovery_trend', 'momentum_12_1', 'relative_volume_20'], unavailable: null })),
  { id: 'pullbacks-hourly', name: 'Pullbacks: 1h change >= 0', group: 'DEFAULT',
    summary: 'Daily Pullback state with a nonnegative change in the last completed hourly slot.',
    qualification: 'Completed-hour context paired with the last completed daily snapshot; check both source times. Not a confirmed reversal or live quote. New comparison is unavailable.',
    predicate: { ...emptyPredicate(), filters: [{ field: 'discovery_state', values: ['PULLBACK'] }],
      hourly: { version: HOURLY_VERSION, interval: '1h', filters: [{ field: 'change', min: 0 }] } },
    sort: { field: 'ticker', descending: false }, columns: [...DEFAULT_COLUMNS, 'vs_ema20', 'hourly'], unavailable: null },
  { id: 'uptrend-hourly', name: 'Uptrend: 1h EMA alignment', group: 'DEFAULT',
    summary: 'Daily discovery trend Up, hourly close at or above its EMA20, and hourly EMA20 change over three bars >= 0%.',
    qualification: 'Completed daily and hourly conditions with separate source times, including equality; not a trend-strength score or entry recommendation. New comparison is unavailable.',
    predicate: { ...emptyPredicate(), filters: [{ field: 'discovery_trend', values: ['UP'] }],
      hourly: { version: HOURLY_VERSION, interval: '1h', filters: [{ field: 'vs_ema20', min: 0 }, { field: 'ema20_change_3', min: 0 }] } },
    sort: { field: 'ticker', descending: false }, columns: [...DEFAULT_COLUMNS, 'vs_ema20', 'hourly'], unavailable: null },
  { id: 'liquid-pullbacks', name: 'Liquid pullbacks', group: 'RECIPE', summary: 'Price >= $5; prior average dollar volume >= $20M; close from 0% to 5% below EMA20.', qualification: 'EMA-distance recipe, not the former Pullbacks state',
    predicate: { ...emptyPredicate(), filters: [{ field: 'price', min: 5 }, { field: 'dollar_volume_20', min: 20_000_000 }, { field: 'vs_ema20', min: -.05, max: 0 }] },
    sort: { field: 'dollar_volume_20', descending: true }, columns: ['ticker', 'price', 'vs_ema20', 'dollar_volume_20', 'relative_volume_20'], unavailable: null },
  { id: 'above-prior-range', name: 'Above prior range', group: 'RECIPE', summary: 'Close at least 0.0001% above the prior 20-session high.', qualification: 'Daily range observation',
    predicate: { ...emptyPredicate(), filters: [{ field: 'vs_prior_high20', min: .000001 }] },
    sort: { field: 'vs_prior_high20', descending: true }, columns: ['ticker', 'price', 'vs_prior_high20', 'relative_volume_20', 'change'], unavailable: null },
]

export function getBuiltInScreens(catalog: ScreeningCatalog | undefined, search = ''): BuiltInScreen[] {
  const query = search.trim().toLowerCase()
  return structuredClone(builtInScreens).filter(screen => `${screen.name} ${screen.summary}`.toLowerCase().includes(query)).map(screen => {
    if (screen.unavailable) return screen
    const required = new Set([...(screen.predicate?.filters.map(filter => filter.field) || []), ...screen.columns.filter(column => !['ticker', 'patterns', 'hourly'].includes(column)), ...(screen.sort.field === 'ticker' ? [] : [screen.sort.field])])
    const hourly = screen.predicate?.hourly
    const hourlyUnavailable = !!hourly && (catalog?.hourly?.version !== HOURLY_VERSION || catalog.hourly.interval !== '1h'
      || hourly.filters.some(filter => !catalog.hourly!.fields[filter.field]?.versions.includes(SCREENING_VERSION) || catalog.hourly!.fields[filter.field]?.interval !== '1h'))
    const unavailable = !catalog ? 'Daily field catalog is loading.' : catalog.version !== SCREENING_VERSION ? 'Requires a compatible daily screening catalog.'
      : [...required].some(field => !catalog.fields[field]?.versions.includes(SCREENING_VERSION)) ? 'Required fields are not supported by this daily screening catalog.'
      : hourlyUnavailable ? 'Requires compatible published hourly context and fields.' : null
    const fixed = constantScreeningColumns(screen.predicate || undefined)
    return { ...screen, columns: screeningColumns(screen.columns.filter(column => !fixed.includes(column))), unavailable }
  })
}

export function toDraft(predicate: Predicate, catalog: ScreeningCatalog): Draft {
  return { fields: Object.fromEntries(predicate.filters.map(filter => {
    const scale = catalog.fields[filter.field]?.unit === 'fraction' ? 100 : 1
    const display = (value: number | null | undefined) => value == null ? '' : String(Number((value * scale).toPrecision(12)))
    return [filter.field, { min: display(filter.min), max: display(filter.max), values: filter.values || [] }]
  })), patterns: predicate.patterns.map(pattern => pattern.id), pattern_mode: predicate.pattern_mode,
    ...(predicate.gap ? { gap: { enabled: true, fields: toDraft({ ...emptyPredicate(), filters: predicate.gap.filters }, { ...catalog, fields: catalog.gaps?.fields || {} }).fields } } : {}),
    ...(predicate.hourly ? { hourly: { enabled: true, fields: toDraft({ ...emptyPredicate(), filters: predicate.hourly.filters }, { ...catalog, fields: catalog.hourly?.fields || {} }).fields } } : {}) }
}

export function compileDraft(draft: Draft, catalog: ScreeningCatalog): { predicate: Predicate; errors: Record<string, string> } {
  const predicate = emptyPredicate()
  const errors: Record<string, string> = {}
  for (const [field, input] of Object.entries(draft.fields)) {
    const spec = catalog.fields[field]
    if (!spec) { errors[field] = 'Unsupported field'; continue }
    if (spec.type === 'category') {
      if (input.values.some(value => !spec.options.includes(value))) errors[field] = 'Unsupported value'
      else if (input.values.length) predicate.filters.push({ field, values: [...input.values] })
      continue
    }
    const scale = spec.unit === 'fraction' ? 100 : 1
    const minimum = input.min.trim() === '' ? undefined : Number(input.min) / scale
    const maximum = input.max.trim() === '' ? undefined : Number(input.max) / scale
    if ((minimum !== undefined && !Number.isFinite(minimum)) || (maximum !== undefined && !Number.isFinite(maximum))) errors[field] = 'Enter finite numbers'
    else if (minimum !== undefined && maximum !== undefined && minimum > maximum) errors[field] = 'Minimum must not exceed maximum'
    else if (minimum !== undefined || maximum !== undefined) predicate.filters.push({ field, min: minimum, max: maximum })
  }
  predicate.pattern_mode = draft.pattern_mode
  for (const id of draft.patterns) {
    const spec = catalog.patterns[id]
    if (!spec) errors.patterns = 'Unsupported pattern'
    else predicate.patterns.push({ id, direction: spec.direction, interval: '1d', state: 'OCCURRENCE' })
  }
  if (draft.gap?.enabled) {
    if (!catalog.gaps || catalog.gaps.version !== GAP_VERSION) errors.gap = 'Daily gap context is unavailable'
    else {
      const gap = compileDraft({ ...emptyDraft(), fields: draft.gap.fields }, { ...catalog, fields: catalog.gaps.fields })
      for (const [field, error] of Object.entries(gap.errors)) errors[`gap.${field}`] = error
      predicate.gap = { version: GAP_VERSION, interval: '1d', filters: gap.predicate.filters }
      try { validateGapPredicate(predicate.gap, catalog) } catch (error) { errors.gap = error instanceof Error ? error.message : 'Invalid gap conditions' }
    }
  }
  if (draft.hourly?.enabled) {
    if (!catalog.hourly) errors.hourly = 'Hourly context is unavailable'
    else {
      const hourly = compileDraft({ ...emptyDraft(), fields: draft.hourly.fields }, { ...catalog, fields: catalog.hourly.fields })
      for (const [field, error] of Object.entries(hourly.errors)) errors[`hourly.${field}`] = error
      predicate.hourly = { version: HOURLY_VERSION, interval: '1h', filters: hourly.predicate.filters.map(({ field, min, max }) => ({ field, min, max })) }
      if (!Object.keys(hourly.errors).length) {
        if (!predicate.hourly.filters.length) errors.hourly = 'Enter a Min or Max for at least one 1h field, or turn off Filter hourly context.'
        else try { validateHourlyPredicate(predicate.hourly, catalog) } catch (error) { errors.hourly = error instanceof Error ? error.message : 'Invalid hourly conditions' }
      }
    }
  }
  return { predicate, errors }
}

export function ruleIdentity(predicate: Predicate): string {
  return JSON.stringify({ version: predicate.version, universe: predicate.universe, interval: predicate.interval,
    filters: [...predicate.filters].sort((first, second) => first.field.localeCompare(second.field)).map(filter => ({ field: filter.field, min: filter.min ?? null, max: filter.max ?? null, values: [...(filter.values || [])].sort() })),
    patterns: [...predicate.patterns].sort((first, second) => first.id.localeCompare(second.id)).map(pattern => ({ id: pattern.id, direction: pattern.direction, interval: pattern.interval, state: pattern.state })), pattern_mode: predicate.patterns.length ? predicate.pattern_mode : 'ANY',
    ...(predicate.gap ? { gap: { version: predicate.gap.version, interval: predicate.gap.interval,
      filters: [...predicate.gap.filters].sort((first, second) => first.field.localeCompare(second.field)).map(filter => ({ field: filter.field, min: filter.min ?? null, max: filter.max ?? null, values: [...(filter.values || [])].sort() })) } } : {}),
    ...(predicate.hourly ? { hourly: { version: predicate.hourly.version, interval: predicate.hourly.interval,
      filters: [...predicate.hourly.filters].sort((first, second) => first.field.localeCompare(second.field)).map(filter => ({ field: filter.field, min: filter.min ?? null, max: filter.max ?? null })) } } : {}) })
}

export function removeDraftFilter(draft: Draft, field: string): Draft {
  if (field === 'hourly') return { ...draft, hourly: undefined }
  if (field === 'gap') return { ...draft, gap: undefined }
  if (field === 'patterns') return { ...draft, patterns: [] }
  return { ...draft, fields: Object.fromEntries(Object.entries(draft.fields).filter(([key]) => key !== field)) }
}

const signedScreeningFields = new Set(['change', 'momentum_12_1', 'momentum_6_1', 'momentum_3_1', 'vs_ema20', 'vs_ema50', 'vs_sma200', 'vs_prior_high20'])

export function screeningValueClass(field: string, value: unknown): string {
  if (!signedScreeningFields.has(field) || typeof value !== 'number' || !Number.isFinite(value) || value === 0) return ''
  return value > 0 ? 'sw-value-positive' : 'sw-value-negative'
}

export function formatValue(value: number | string | null | undefined, spec?: Pick<FieldSpec, 'unit'>): string {
  if (value == null) return 'Unavailable'
  if (typeof value === 'string') return ({ CS: 'Common stock', ETF: 'ETF', ETV: 'ETV', PULLBACK: 'Pullback', BOUNCE: 'Bounce', RESUMING_UP: 'Resuming up', RESUMING_DOWN: 'Resuming down', TRENDING: 'Trending', MIXED: 'Mixed', UP: 'Up', DOWN: 'Down', OPEN: 'Open', PARTIALLY_FILLED: 'Partially filled', FILLED: 'Filled', SAME_SESSION_FADE: 'Formation-session fade', FAILED: 'Failed at current close', ABOVE: 'Above', INSIDE: 'Inside', BELOW: 'Below' } as Record<string, string>)[value] || value
  const numeric = spec?.unit === 'fraction' ? value * 100 : value
  return numeric.toLocaleString('en-US', { maximumFractionDigits: 2 }) + (spec?.unit === 'fraction' ? '%' : spec?.unit === 'ratio' ? 'x' : '')
}

export function filterLabel(filter: ScreeningFilter, catalog: ScreeningCatalog): string {
  const spec = catalog.fields[filter.field]
  if (filter.values?.length) return `${spec.label}: ${filter.values.map(value => formatValue(value)).join(', ')}`
  return `${spec.label}: ${filter.min == null ? 'any' : formatValue(filter.min, spec)} to ${filter.max == null ? 'any' : formatValue(filter.max, spec)}`
}

export function downloadText(text: string, filename: string, type: string) {
  const url = URL.createObjectURL(new Blob([text], { type }))
  const anchor = document.createElement('a')
  anchor.href = url; anchor.download = filename; anchor.click()
  URL.revokeObjectURL(url)
}

export function resultCsv(result: ScreeningResult, columns: string[], predicate?: Predicate): string {
  const escape = (value: unknown) => {
    const text = String(value ?? '')
    return `"${(/^[=+\-@\t\r]/.test(text) ? "'" + text : text).replace(/"/g, '""')}"`
  }
  const comparison = result.comparison
  const hourly = result.generation.hourly_source
  const headers = ['generation', 'session', 'security_id', ...columns, ...(comparison ? ['new_status', 'new_reason', 'new_only', 'comparison_status', 'previous_session', 'previous_generation'] : []), ...(hourly ? ['hourly_market_time', 'hourly_source_cutoff', 'hourly_publication_id', 'hourly_slot_minutes', 'daily_generation', 'daily_market_time', 'daily_source_cutoff', 'hourly_capture'] : [])]
  return [headers.map(escape).join(','), ...result.rows.map(row => [result.generation.generation, row.session, row.security_id, ...columns.map(column => column === 'ticker' ? row.ticker : column === 'hourly' ? JSON.stringify(row.hourly || null) : column === 'hourly_behavior' ? hourlyObservedBehavior(row, predicate || emptyPredicate(), !!result.hourly_stale) : column === 'gaps' ? JSON.stringify(row.gap_summary || null) : column === 'patterns' ? Object.entries(row.patterns).filter(([, observation]) => observation.present).map(([id]) => id).join(';') : row.values[column]), ...(comparison ? [row.new_status, row.new_reason, !!result.new_only, comparison.status, comparison.previous_session, comparison.previous_generation] : []), ...(hourly ? [hourly.market_time, hourly.source_cutoff, hourly.source_publication_id, hourly.slot_minutes, result.generation.daily_generation || result.generation.generation, result.generation.market_time, result.generation.source_cutoff, hourly.capture || 'AT_DAILY_SNAPSHOT_CUTOFF_NOT_LIVE'] : [])].map(escape).join(','))].join('\r\n')
}

export const WORKSPACE_KEY = 'alphascreener.screening.workspace.v1'
export const DRAFT_KEY = 'alphascreener.screening.draft.v1'
export const MAX_IMPORT_BYTES = 256 * 1024
export interface SavedScreen {
  schema_version: 1; id: string; name: string; predicate_revision: number; predicate_hash: string
  created_at: string; updated_at: string; time_mode: 'LATEST_COMPLETE'; predicate: Predicate
  sort: { field: string; descending: boolean }; columns: string[]; view: 'CURRENT'
}
export interface ScreenWorkspace { schema_version: 1; screens: SavedScreen[]; pins: string[]; active_id: string | null; tabs: string[]; active_tab: string | null; builtin_columns?: Record<string, string[]>; builtin_columns_version?: 2 }
export const emptyWorkspace = (): ScreenWorkspace => ({ schema_version: 1, screens: [], pins: [], active_id: null, tabs: [], active_tab: null })

export const canonicalScreenTab = (key: string | null): string | null => key === 'builtin:all' ? null : key

export function openScreenTab(workspace: ScreenWorkspace, key: string | null): ScreenWorkspace {
  key = canonicalScreenTab(key)
  if (key !== null && !(key.startsWith('saved:') && workspace.screens.some(screen => `saved:${screen.id}` === key)) && !builtInScreens.some(screen => `builtin:${screen.id}` === key)) throw new Error('Unknown screen tab')
  const tabs = workspace.tabs.filter(tab => canonicalScreenTab(tab) !== null)
  return { ...workspace, tabs: key && !tabs.includes(key) ? [...tabs, key] : tabs, active_tab: key, active_id: key?.startsWith('saved:') ? key.slice(6) : null }
}

export function closeScreenTab(workspace: ScreenWorkspace, key: string): ScreenWorkspace {
  const index = workspace.tabs.indexOf(key)
  const tabs = workspace.tabs.filter(tab => tab !== key)
  const active = workspace.active_tab === key ? tabs[Math.min(Math.max(index - 1, 0), tabs.length - 1)] || null : workspace.active_tab
  return openScreenTab({ ...workspace, tabs, pins: key.startsWith('saved:') ? workspace.pins.filter(id => `saved:${id}` !== key) : workspace.pins }, active)
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected a JSON object')
  return value as Record<string, unknown>
}
function onlyKeys(value: Record<string, unknown>, keys: string[]) {
  if (Object.keys(value).some(key => !keys.includes(key))) throw new Error('Unsupported definition property')
}
function validName(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !value.trim() || value.length > 80) throw new Error('Screen name must contain 1-80 characters')
}
function validateColumns(value: unknown, catalog: ScreeningCatalog): asserts value is string[] {
  if (!Array.isArray(value) || value.length > 30 || !value.includes('ticker') || new Set(value).size !== value.length || value.some(key => typeof key !== 'string' || !['ticker', 'patterns', ...(catalog.gaps ? ['gaps'] : []), ...(catalog.hourly ? ['hourly'] : []), ...Object.keys(catalog.fields)].includes(key))) throw new Error('Invalid visible columns')
}

export function validateGapPredicate(value: unknown, catalog: ScreeningCatalog): GapPredicate {
  const gap = object(value)
  onlyKeys(gap, ['version', 'interval', 'filters'])
  if (!catalog.gaps || gap.version !== GAP_VERSION || gap.interval !== '1d' || !Array.isArray(gap.filters) || gap.filters.length > 6) throw new Error('Unsupported daily gap conditions')
  const validated = validatePredicate({ ...emptyPredicate(), filters: gap.filters }, { ...catalog, fields: catalog.gaps.fields })
  for (const filter of validated.filters) for (const bound of [filter.min, filter.max]) {
    if (bound != null && (bound < 0 || filter.field === 'formation_age' && (!Number.isInteger(bound) || bound > catalog.gaps.max_formation_age) || filter.field === 'fill_fraction' && bound > 1)) throw new Error('Gap age must be 0-20 sessions; fill 0-100%; distance nonnegative')
  }
  return structuredClone(gap) as unknown as GapPredicate
}

export function validateGapDraft(value: unknown, catalog: ScreeningCatalog): void {
  const gap = object(value)
  onlyKeys(gap, ['enabled', 'fields'])
  if (typeof gap.enabled !== 'boolean' || !catalog.gaps) throw new Error('Invalid recovered gap draft')
  const fields = object(gap.fields)
  if (Object.keys(fields).length > 6) throw new Error('Too many gap draft fields')
  for (const [field, raw] of Object.entries(fields)) {
    const input = object(raw)
    onlyKeys(input, ['min', 'max', 'values'])
    const spec = catalog.gaps.fields[field]
    if (!spec || typeof input.min !== 'string' || typeof input.max !== 'string' || input.min.length > 64 || input.max.length > 64 || !Array.isArray(input.values) || input.values.length > 5 || input.values.some(value => typeof value !== 'string' || !spec.options.includes(value))) throw new Error('Invalid recovered gap field')
  }
}
export function validatePredicate(value: unknown, catalog: ScreeningCatalog): Predicate {
  const data = object(value)
  onlyKeys(data, ['version', 'universe', 'interval', 'filters', 'patterns', 'pattern_mode', 'gap', 'hourly'])
  if (data.version !== SCREENING_VERSION || data.universe !== 'CONFIGURED_TRACKED_EQUITIES' || data.interval !== '1d') throw new Error('Unsupported rule version, universe or interval')
  if (!Array.isArray(data.filters) || data.filters.length > 30 || !Array.isArray(data.patterns) || data.patterns.length > 10 || !['ANY', 'ALL', 'NONE'].includes(String(data.pattern_mode))) throw new Error('Invalid filter or pattern list')
  const fields = new Set<string>()
  for (const item of data.filters) {
    const filter = object(item)
    onlyKeys(filter, ['field', 'min', 'max', 'values'])
    if (typeof filter.field !== 'string' || !Object.prototype.hasOwnProperty.call(catalog.fields, filter.field) || fields.has(filter.field)) throw new Error('Unsupported or duplicate field')
    fields.add(filter.field)
    const spec = catalog.fields[filter.field]
    const values = filter.values ?? []
    if (!Array.isArray(values) || values.length > 20 || values.some(value => typeof value !== 'string' || !spec.options.includes(value))) throw new Error('Invalid category values')
    for (const bound of [filter.min, filter.max]) if (bound != null && (typeof bound !== 'number' || !Number.isFinite(bound))) throw new Error('Bounds must be finite numbers')
    if (spec.type === 'number' && (values.length || filter.min == null && filter.max == null)) throw new Error('Numeric filter requires bounds')
    if (spec.type === 'category' && (!values.length || filter.min != null || filter.max != null)) throw new Error('Category filter requires selected values only')
    if (typeof filter.min === 'number' && typeof filter.max === 'number' && filter.min > filter.max) throw new Error('Minimum must not exceed maximum')
  }
  const patterns = new Set<string>()
  for (const item of data.patterns) {
    const pattern = object(item)
    onlyKeys(pattern, ['id', 'direction', 'interval', 'state'])
    if (typeof pattern.id !== 'string' || !Object.prototype.hasOwnProperty.call(catalog.patterns, pattern.id) || patterns.has(pattern.id)) throw new Error('Unsupported or duplicate pattern')
    const spec = catalog.patterns[pattern.id]
    if (pattern.direction !== spec.direction || pattern.interval !== '1d' || pattern.state !== 'OCCURRENCE') throw new Error('Unsupported pattern direction or state')
    patterns.add(pattern.id)
  }
  if (data.gap != null) validateGapPredicate(data.gap, catalog)
  if (data.hourly != null) validateHourlyPredicate(data.hourly, catalog)
  return structuredClone(value) as Predicate
}

export function validateScreen(value: unknown, catalog: ScreeningCatalog): SavedScreen {
  const data = object(value)
  onlyKeys(data, ['schema_version', 'id', 'name', 'predicate_revision', 'predicate_hash', 'created_at', 'updated_at', 'time_mode', 'predicate', 'sort', 'columns', 'view'])
  if (data.schema_version !== 1 || data.time_mode !== 'LATEST_COMPLETE' || data.view !== 'CURRENT') throw new Error('Unsupported screen schema or time mode')
  if (typeof data.id !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(data.id)) throw new Error('Invalid screen ID')
  validName(data.name)
  if (!Number.isInteger(data.predicate_revision) || Number(data.predicate_revision) < 1 || Number(data.predicate_revision) > 1000000 || typeof data.predicate_hash !== 'string' || !/^[0-9a-f]{64}$/.test(data.predicate_hash)) throw new Error('Invalid predicate revision or hash')
  for (const stamp of [data.created_at, data.updated_at]) if (typeof stamp !== 'string' || !/^\d{4}-\d{2}-\d{2}T/.test(stamp) || !Number.isFinite(Date.parse(stamp))) throw new Error('Invalid screen timestamp')
  if (Date.parse(String(data.created_at)) > Date.parse(String(data.updated_at))) throw new Error('Updated time precedes creation')
  validatePredicate(data.predicate, catalog)
  const sort = object(data.sort)
  onlyKeys(sort, ['field', 'descending'])
  if (typeof sort.descending !== 'boolean' || typeof sort.field !== 'string' || sort.field !== 'ticker' && !Object.prototype.hasOwnProperty.call(catalog.fields, sort.field)) throw new Error('Unsupported sort')
  validateColumns(data.columns, catalog)
  return structuredClone(value) as SavedScreen
}

export async function hashRule(predicate: Predicate): Promise<string> {
  const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(ruleIdentity(predicate)))
  return [...new Uint8Array(hash)].map(value => value.toString(16).padStart(2, '0')).join('')
}

export async function makeScreen(name: string, predicate: Predicate, columns: string[], sort: SavedScreen['sort'], previous?: SavedScreen): Promise<SavedScreen> {
  validName(name)
  const stamp = new Date().toISOString()
  const changed = !previous || ruleIdentity(previous.predicate) !== ruleIdentity(predicate)
  return { schema_version: 1, id: previous?.id || crypto.randomUUID(), name: name.trim(), predicate_revision: previous ? previous.predicate_revision + Number(changed) : 1,
    predicate_hash: await hashRule(predicate), created_at: previous?.created_at || stamp, updated_at: stamp, time_mode: 'LATEST_COMPLETE',
    predicate: structuredClone(predicate), columns: [...columns], sort: { ...sort }, view: 'CURRENT' }
}

export function exportDefinitions(screens: SavedScreen[]): string {
  return JSON.stringify({ schema_version: 1, screens })
}

export async function importDefinitions(text: string, catalog: ScreeningCatalog): Promise<SavedScreen[]> {
  if (new TextEncoder().encode(text).length > MAX_IMPORT_BYTES) throw new Error('Import exceeds 256 KiB')
  const data = object(JSON.parse(text))
  onlyKeys(data, ['schema_version', 'screens'])
  if (data.schema_version !== 1 || !Array.isArray(data.screens) || !data.screens.length || data.screens.length > 100) throw new Error('Import must contain 1-100 supported screen definitions')
  const screens = data.screens.map(screen => validateScreen(screen, catalog))
  if (new Set(screens.map(screen => screen.id)).size !== screens.length) throw new Error('Duplicate screen IDs in import')
  for (const screen of screens) if (await hashRule(screen.predicate) !== screen.predicate_hash) throw new Error(`Predicate hash mismatch: ${screen.name}`)
  return screens
}

export function mergeDefinitions(workspace: ScreenWorkspace, imported: SavedScreen[], conflict: 'REPLACE' | 'COPY'): ScreenWorkspace {
  const screens = [...workspace.screens]
  for (const screen of imported) {
    const index = screens.findIndex(existing => existing.id === screen.id)
    if (index >= 0 && conflict === 'REPLACE') screens[index] = screen
    else screens.push(index >= 0 ? { ...screen, id: crypto.randomUUID(), name: `${screen.name.slice(0, 73)} (copy)` } : screen)
  }
  if (screens.length > 100) throw new Error('Browser library is limited to 100 screens')
  return { ...workspace, screens }
}

export async function readWorkspace(storage: Pick<Storage, 'getItem'>, catalog: ScreeningCatalog): Promise<ScreenWorkspace> {
  const text = storage.getItem(WORKSPACE_KEY)
  if (!text) return emptyWorkspace()
  if (new TextEncoder().encode(text).length > MAX_IMPORT_BYTES) throw new Error('Stored workspace exceeds 256 KiB')
  const data = object(JSON.parse(text))
  onlyKeys(data, ['schema_version', 'screens', 'pins', 'active_id', 'tabs', 'active_tab', 'builtin_columns', 'builtin_columns_version'])
  if (data.schema_version !== 1 || !Array.isArray(data.screens) || data.screens.length > 100) throw new Error('Unsupported browser workspace')
  const screens = data.screens.length ? await importDefinitions(exportDefinitions(data.screens as SavedScreen[]), catalog) : []
  if (!Array.isArray(data.pins) || data.pins.length > 100 || new Set(data.pins).size !== data.pins.length || data.pins.some(id => !screens.some(screen => screen.id === id))) throw new Error('Invalid pin list')
  if (data.active_id !== null && !screens.some(screen => screen.id === data.active_id)) throw new Error('Invalid active screen')
  const legacyTabs = [...new Set([...(data.pins as string[]), ...(data.active_id ? [String(data.active_id)] : [])])].map(id => `saved:${id}`)
  const tabs = data.tabs ?? legacyTabs
  const activeTab = data.active_tab === undefined ? data.active_id ? `saved:${data.active_id}` : null : data.active_tab
  const validTab = (key: unknown) => typeof key === 'string' && (screens.some(screen => `saved:${screen.id}` === key) || builtInScreens.some(screen => `builtin:${screen.id}` === key))
  if (!Array.isArray(tabs) || tabs.length > 120 || new Set(tabs).size !== tabs.length || tabs.some(key => !validTab(key))) throw new Error('Invalid open screen tabs')
  if (activeTab !== null && (!validTab(activeTab) || !tabs.includes(activeTab))) throw new Error('Invalid active tab')
  if ((typeof activeTab === 'string' && activeTab.startsWith('saved:') ? activeTab.slice(6) : null) !== data.active_id) throw new Error('Active screen and tab disagree')
  const builtinColumns = data.builtin_columns === undefined ? undefined : object(data.builtin_columns)
  if (data.builtin_columns_version !== undefined && data.builtin_columns_version !== 2) throw new Error('Unsupported built-in column layout version')
  if (builtinColumns) {
    for (const [id, columns] of Object.entries(builtinColumns)) {
      const definition = builtInScreens.find(screen => screen.id === id)
      if (!definition) throw new Error('Unknown built-in column layout')
      validateColumns(columns, catalog)
      if (data.builtin_columns_version === undefined && constantScreeningColumns(definition.predicate || undefined).length) {
        const previousPresets = screeningColumnPresets(catalog, definition.columns)
        const preset = selectedScreeningColumnPreset(columns, previousPresets)
        if (preset) builtinColumns[id] = screeningColumnPresets(catalog, definition.columns, definition.predicate || undefined)[preset].columns
      }
    }
  }
  return openScreenTab({ schema_version: 1, screens, pins: data.pins as string[], active_id: data.active_id as string | null, tabs: tabs as string[], active_tab: activeTab as string | null,
    ...(builtinColumns ? { builtin_columns: builtinColumns as Record<string, string[]>, builtin_columns_version: 2 } : {}) }, activeTab as string | null)
}

export function writeWorkspace(storage: Pick<Storage, 'setItem'>, workspace: ScreenWorkspace): void {
  const text = JSON.stringify(workspace)
  if (new TextEncoder().encode(text).length > MAX_IMPORT_BYTES) throw new Error('Browser workspace exceeds 256 KiB')
  storage.setItem(WORKSPACE_KEY, text)
}

export function validateHourlyPredicate(value: unknown, catalog: ScreeningCatalog): HourlyPredicate {
  const hourly = object(value)
  onlyKeys(hourly, ['version', 'interval', 'filters'])
  if (!catalog.hourly || hourly.version !== HOURLY_VERSION || hourly.interval !== '1h' || !Array.isArray(hourly.filters) || !hourly.filters.length || hourly.filters.length > 4) throw new Error('Hourly context requires 1-4 supported numeric conditions')
  for (const filter of hourly.filters) onlyKeys(object(filter), ['field', 'min', 'max'])
  validatePredicate({ ...emptyPredicate(), filters: hourly.filters }, { ...catalog, fields: catalog.hourly.fields })
  return structuredClone(value) as HourlyPredicate
}

export function validateHourlyDraft(value: unknown, catalog: ScreeningCatalog): void {
  const hourly = object(value)
  onlyKeys(hourly, ['enabled', 'fields'])
  if (!catalog.hourly || typeof hourly.enabled !== 'boolean') throw new Error('Invalid recovered hourly draft')
  const fields = object(hourly.fields)
  if (Object.keys(fields).length > 4) throw new Error('Too many hourly fields')
  for (const [field, raw] of Object.entries(fields)) {
    const input = object(raw)
    onlyKeys(input, ['min', 'max', 'values'])
    if (!Object.prototype.hasOwnProperty.call(catalog.hourly.fields, field) || typeof input.min !== 'string' || typeof input.max !== 'string' || input.min.length > 64 || input.max.length > 64 || !Array.isArray(input.values) || input.values.length) throw new Error('Invalid recovered hourly field')
  }
}