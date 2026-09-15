# Daily Screening v1 Capability Inventory

Implementation contract: `stock_screener_workspace_v1`. This is functional
screening, not a strategy qualification. Alerts and discovery remain unchanged.

Current field set: `screening_fields_v3_momentum_horizons`. The original seven default
screeners, two hourly-context examples and two additional daily recipes are supported
by the current catalog. Earlier library counts and unavailable-state lists are
historical checkpoints.

## Reading Hourly-Filtered Results

Enabling **Filter hourly context** requires a Min or Max on at least one hourly
field; the toggle alone does not choose a condition. Empty groups now prompt for
an explicit bound, while invalid numbers and reversed ranges retain their own
field errors. For example, hourly last-bar change with Max 0 selects nonpositive
changes; Min 0 selects nonnegative changes. Zero is a valid inclusive bound.
To inspect hourly values without restricting matches, select **1h values** in the
column picker and leave hourly filtering off. No directional rule is added
automatically, including for the Bearish risk built-in.

September 14: when an hourly predicate is applied, the table automatically shows
**1h values** and **1h filter context**, alongside explicit **Daily close**, **Daily
change**, **Daily volume** and daily moving-average distance labels. The hourly
value rows and context text follow the actual applied hourly fields and inclusive
bounds, not a static indicator recipe:

- Change-only: `Daily pullback. 1h close rose 0.8% (min 0%).`
- EMA alignment: `Daily trend up. Price 1.67% above 1h EMA20 (min 0%). 1h EMA20 rose
	0.55% over 3 bars (min 0%).`
- Price-only: `1h close $59.62 (min $10).`

Daily state/trend is mentioned only when that daily field is part of the screen's
rules. Unselected hourly indicators are omitted. Draft edits do not change this
view until Apply; unsaved applied rules are supported. The two context columns
stay visible while hourly filtering is active, including when choosing a column
preset. The derived text column is display-only, not a stored screener field or
new filter. Existing IDs, predicates, saved definitions and result ordering remain
unchanged. CSV includes the description and existing source timestamps. Removing
hourly filtering removes the derived text; a manually selected hourly values
column without hourly filters still shows the full four-field context.

Descriptions are deterministic formatting of already-returned row values, not
AI output, predictions, crossover detection or previous-hour membership changes.
Unavailable values stay explicit and stale hourly data is labeled. No new request,
SQL query, provider call, worker, or polling interval is introduced. Formatting
runs only for displayed rows (100 per page). A local 30-sample browser measurement
with all four hourly conditions took about 2 ms median / 4 ms p95 for 100
descriptions after reusing number formatters; this excludes normal table rendering
and is not a platform-wide latency guarantee. The main layout cost is another
wrapped column, with horizontal table scrolling on narrow screens.

Validation: 79 frontend tests, TypeScript and production build passed. Live checks
covered both hourly built-ins and Long interest with an unsaved price-only hourly
rule, no unrelated commentary, unchanged result counts, no stored derived-column
ID, and zero additional hourly/gap-detail requests. No backend or source changes.

## Advancing Hourly Publications

September 14, 2026: approved ongoing hourly updates are implemented for all
hourly-filtered screens, including the two existing built-ins. The new resident
`run_screening_worker.py` is included in Equity/All startup and checks every 60
seconds. It prepares a new daily anchor only when a new complete canonical daily
source arrives; otherwise daily rows, ranks, gaps and cutoffs remain unchanged.
Each new hourly publication is paired with that anchor in an append-only screening
revision. No source bars, old snapshots or saved definitions are rewritten.

The calculation version stays `screening_hourly_context_v1`; the independent
capture policy is `screening_hourly_refresh_v1`, with
`LATEST_COMPLETED_HOURLY_WITH_FROZEN_DAILY`. Original frozen contracts remain
readable. Pair identity includes daily generation, hourly publication ID,
calculation contract and projector/derivation hashes. Worker leadership prevents
concurrent publishers, and repeated inputs return ALREADY_PUBLISHED.

Readers remain snapshot-only. The active page polls every 60 seconds, retains
separate daily/hourly timestamps, and exports daily anchor and hourly lineage
metadata in CSV. Hourly freshness uses the configured provider delay; the worker
cannot publish an hour that ingestion has not yet published. Missing data remains
UNKNOWN, and a corporate action between the anchor and hourly dates blocks the
combined context. Hourly New comparisons remain disabled.

Initial validation paired daily September 11 close with September 14 10:30 AM ET:
386/386 hourly close/change values and 385/386 EMA values; ARKW's incomplete
historical slot remains unavailable. Source reconciliation checked 11,192 retained
revisions and exact daily-anchor preservation. Pullbacks + hourly change >=0 gave
49 matches/13 unknown; Uptrend + hourly EMA alignment gave 66/11. Base daily
Pullbacks remains 83. Initial paired generation:
`651d56d3eaa3784aabec4b54ceb4e3d9ce66d4b2a7e5709f5bffc6e219bf9aee`.
All 199 prior snapshots were preserved; a second cycle inserted nothing.

The running publisher then automatically advanced when ingestion published the
11:30 AM ET hour at 11:55:31 AM. The paired snapshot was retained at 11:56:26 AM,
generation `da9a425ea4dac146a2dc8236eeeac08e5affa6aad71626b90d0a76af16c0d070`.
Its source audit passed with the same daily anchor and hourly coverage. Pullbacks
+ nonnegative hourly change became 54 matches/12 unknown; Uptrend + EMA alignment
became 74/12. The API hourly-stale flag cleared without a manual publication.

Verification: 92 focused backend tests and 74 frontend tests passed, alongside
TypeScript and production build. Checks cover separate cutoffs, action gates,
unchanged daily fields, both historical/current contracts, next-hour identity,
idempotency, mocked launcher behavior, and provider-delay freshness. Both built-ins
and pinned hourly details were exercised in the browser; daily-only Pullbacks
stayed 83 matches/15 New and the original pinned hourly query stayed 37 matches.
Desktop/mobile timestamp layouts fit, and scheduled foreground result polling was
observed. Longer multi-session operation remains to be observed.

Use `--status` for retained publication metadata (not heartbeat), `--measure` for
read-only preparation, or `--once` for one publication cycle. No provider requests
are made by this worker. Earlier frozen-context descriptions below are historical
checkpoints and do not describe the current advancing mode.

## Grouped Column Selectors

Stock Screener and Stock Alerts now organize their column pickers under persistent
group headings with selected/available counts. Screener groups separate Stock &
price, Liquidity & activity, Trend, Momentum, Volatility / structure and Setups &
context. Alerts separate Alert details, Trade plan, Paper outcomes, Momentum,
Trend, Liquidity & activity and Volatility & extension. Paper outcomes appears
only in Day History, matching the existing history-only column policy.

Grouping changes the picker presentation, not table order, field identity, saved
preferences or presets. All groups remain expanded inside the scrollable menu.
Individual checkboxes, locked columns, Show all and Reset to fit retain their
behavior. Other pages keep their ungrouped picker unless they supply group metadata.

Validation: 72 frontend tests, TypeScript and production bundle passed. Browser
checks covered group membership/counts, keyboard selection, Show all/reset,
21 screener columns and 28 alert-history columns, and desktop/mobile fit. The
alert menu's horizontal alignment was adjusted for mobile. Original screener
workspace/draft and alert column selections were restored after testing; no
backend, snapshot or worker changes were needed.

## Optional Momentum Horizons

September 13, 2026: **6-1 momentum** and **3-1 momentum** are optional numeric
filters under **Trend / momentum**, with optional columns and sorting. Both work
in new saved screens, Apply, draft recovery, definition import/export and CSV.
Neither filter is enabled automatically; existing recipes, saved rules, column presets
and the explicitly labeled **12-1 universe percentile** remain unchanged.

Confirming **Apply**, **Save and apply**, or saving before leaving a screen now
also enables the applied filters' columns in the picker and table. Existing column
order and unrelated selections are retained; missing filter columns are appended
once. Candle, gap and hourly filters enable their respective grouped summary
columns. Unapplied drafts, invalid inputs and Cancel do not change the layout.
Manual hiding and layout presets still work afterward; removing a filter does not
remove its column. Reapplying enables the active filters' columns again. Merely
opening an existing screen keeps its saved or built-in layout.

Validation: 71 frontend tests, TypeScript and build passed. Browser checks confirmed
momentum and liquidity columns on Apply, unchanged columns on Cancel, manual hiding
without a result request, and automatic columns retained through Save and reload.

| Field | Fraction Stored | Required Valid Sessions | Tracking Value |
|---|---|---|---|
| 6-1 momentum | Close 21 sessions ago / close 126 sessions ago - 1 | 127 | Compare the more recent half-year window with longer-term strength or weakness |
| 3-1 momentum | Close 21 sessions ago / close 63 sessions ago - 1 | 64 | Review shorter-term strength or deterioration, with more sensitivity to temporary moves |

Inputs and displayed values are percentages: entering 5 means 5%, stored as 0.05.
Zero bounds are inclusive. Both exclude the latest 21 sessions, so neither captures
a reversal within the latest month. These are nonannualized price returns, not
percentiles, total returns or entry signals. Raw magnitudes across unequal windows
do not directly measure acceleration. Help leads with tracking value and a short
example; calculation details stay collapsed. All three momentum summaries now
explain the threshold directly: minimum 5 means at least 5% ($100 to $105).
The horizon and skipped
latest month are stated alongside that example in both filter and column help.

Each field independently requires its full contiguous history, valid OHLCV,
stable security identity and known-action review. Missing data stays UNKNOWN,
never zero. A shorter window can be available when 12-1 is not; selecting a
253-session discovery field alongside it still imposes that field's requirements.

Two additive revisions were published from the existing September 10/11 source
publications and their original cutoffs. Earlier snapshots, raw data and current
pointers were not replaced. Readers select the latest retained revision per date;
catalog/filter/sort availability is gated on published fields. Older revisions
reject unsupported horizon requests. No provider backfill or worker was enabled.

- September 10 generation: `b2d26d3707e76cf729fa414eb25b6358ed66c81d7601bfb3cb3462c9e2d1dfe6`.
- September 11 generation: `f0e4230be1fd77cf263d64325e84ea766bf60e9ccaa6fe4b08c98ca1f44267f1`.
- Latest coverage: 373/386 for 6-1, 378/386 for 3-1; 12-1 remains 361/386.
- Optional example, both horizons >=0%: 190 matches, 13 unknown, 11 New. Raising
	only 6-1 to >=5% gives 169 matches. Neither example was added as a built-in.

Verification: 138 backend and 68 frontend tests, TypeScript and production build
passed. The read-only verifier reconciled 1,112 momentum values to 1,490 retained
close revisions with exact session offsets, identities and cutoffs. All six
retained sessions passed mixed-field-set history verification; old fields,
discovery membership, New counts, gap/hourly examples and frozen-study source
hashes remain unchanged. Browser checks covered new-screen creation, percentage
bounds, save/update/reload, columns, sort, concise help and desktop/mobile fit.
Draft/help/column changes made no result requests. The temporary test definition
was removed after validation.

## Field Explanations

September 13, 2026: contextual help is available beside filter labels and table
headings, including daily metrics, candle observations, gap conditions and hourly
fields. Hovering a help icon now explains why the field is useful to track.
Clicking, tapping, or keyboard-activating it opens a concise **Why track it?**
section and a **How to read it** example. Formulas, full definitions, required
history, input units and caveats remain under **Calculation details**, collapsed
by default. Hourly detail labels link to the same hourly explanations.

Tracking value is specific to the field: 12-1 momentum supports medium-term trend
shortlisting; its percentile narrows that list to relative leaders or laggards.
EMA20 distance gives short-term pullback/extension context, EMA50 distance gives a
medium-term baseline, and SMA200 distance gives a longer-term baseline. Relative
volume highlights unusual activity, while realized volatility helps compare past
price variability. These uses support review, not an automatic trading decision.

| Term | Meaning | Example |
|---|---|---|
| 12-1 momentum | Close 21 trading sessions ago divided by close 252 sessions ago, minus 1; roughly the prior year excluding the most recent month | $100 to $120 over that window is +20% |
| 12-1 universe percentile | Position in the full eligible tracked cohort's momentum ranking before filtering | 90% is roughly top 10%, not a 90% return or success probability |
| Close / EMA20 - 1 | Percentage distance from the 20-session exponential moving average | Close $110 and average $100 is +10%; close $95 is -5% |
| Close / EMA50 - 1 | The same distance calculation using a 50-session EMA | The period remains 50; subtracting 1 removes the ratio baseline |
| Close / SMA200 - 1 | Distance from the simple average of the latest 200 daily closes | Positive is above, negative below; neither establishes the average's direction |

The help follows actual screener contracts: 253-session 12-1 momentum eligibility,
rank tie-breaking by stable security ID, exact-window seeded daily/hourly EMAs,
prior-session-only volume/range references, and distinct gap-fill versus zone-distance
meanings. Percentage inputs use percent units (5 for 5%); unavailable is not zero.
No predictor quality, confirmed trade signal or total-return claim is implied.

Help is local display content. Opening/closing it makes no screening or detail
request and does not modify drafts, saved definitions, results or source data.
Keyboard focus returns to the invoking icon. Escape closes only the help dialog,
including when opened above the mobile Filters drawer. The parent drawer stays open.
The existing shared modal also explicitly handles Escape where a browser does not
emit its native cancel event.

Verification: 65 frontend model tests, TypeScript and production bundle passed.
Browser checks covered momentum/percentile formulas, daily filter/header parity,
hourly distinctions, hover summaries, keyboard activation/dismissal/focus return,
unchanged filters and zero requests. Desktop light and mobile light/dark dialogs
fit without horizontal overflow. The purpose-first EMA20 popup measured about
280px tall on desktop and 312px on mobile with details closed; the technical text
remains available on expansion. Every supported field has a short tracking-purpose
description. Field names, IDs and calculations are unchanged.

## Hourly Built-In Examples

September 13, 2026: added two built-in library recipes using the already-published
hourly context. No backend calculation, source data or snapshot was changed.

| Built-In | Daily Conditions | Hourly Conditions | Current Matches / Unknown |
|---|---|---|---|
| Pullbacks: 1h change >= 0 | Discovery state = PULLBACK | Last completed slot's close-to-close change >=0% | 37 / 10 |
| Uptrend: 1h EMA alignment | Discovery trend = UP | Close / EMA20 - 1 >=0%; EMA20 change over three bars >=0% | 74 / 13 |

Zero bounds are inclusive: unchanged values can qualify. These are descriptive
daily-plus-hourly examples, not confirmed reversals, entry signals or trend-strength
scores. They use frozen hourly facts at the daily cutoff; New remains unavailable
for their hourly-filtered rules. Each sorts by ticker and defaults to the fixed
four columns, daily EMA20 distance and the Hourly context column. Screen default
restores that layout without a result query. Rule-constant state/trend columns are
not duplicated in the default table.

The recipes are disabled if the required hourly contract or fields are absent,
incompatible or not hourly. The original nine available recipes remain usable
with a daily-only catalog. The full current library has eleven built-ins; new
recipes do not automatically create saved user definitions or pinned tabs.

**Selected filters:** opening any built-in loads its predicate into both the
applied request and the editable filter draft. Opening Filters shows its selected
categories and numeric bounds, including the enabled hourly group. Groups with
selected fields now expand automatically on panel open and screen changes, show
selection counts, and highlight the active fields (including zero numeric bounds).
Users can still collapse groups manually; reopening the panel reveals selections
again. Old filter searches are cleared on panel open and screen changes so they do
not hide the selected recipe fields. Merely previewing a recipe in the library
does not activate it or replace current filters.
After edits, the panel shows the current draft, not an immutable recipe preview;
Apply commits the draft and saving creates a user copy. Switching away still uses
the existing dirty-draft guard. Selecting another built-in does not mutate the
original recipe or any saved definition.

Verification: 61 frontend model tests, TypeScript, production build and browser
checks. Both recipes reproduce the counts above and prepopulate all daily/hourly
conditions. Existing Pullbacks also prepopulates its state. Zero thresholds survive
draft and saved-definition roundtrips; missing-hourly catalogs fail closed. Desktop
and 390px mobile navigation, reload and filter drawer checks passed without page
overflow. Library previews and filter/layout inspection make zero result queries.
No workers, provider requests or market-data writes were involved.

Filter visibility follow-up: daily, candle, gap and hourly applied conditions now
share one wrapping chip row above the table, rather than separate daily/hourly
rows. The Pullbacks hourly example places Daily discovery state: Pullback beside
1h last-bar change: 0% to any. The group removal actions and same-episode gap
semantics are unchanged. The Filters panel is still closed by default.

These presets use ordinary catalog filters, not hidden per-built-in filter code.
Some catalog fields are derived facts: selecting Daily discovery state = Pullback
does not select all independent EMA controls or expose every underlying state
calculation as another applied rule. Verified every current built-in's actual
panel selections, auto-expansion, reopen/search reset and active highlights.
62 frontend tests, TypeScript and production build passed; desktop light and
390px mobile dark checks confirmed adjacent chips, no body overflow, no inspection
queries and that removing the hourly chip preserves the daily condition.

## Hourly Context: Implemented Pilot

September 13, 2026: the approved small 1h context extension is implemented in the
daily screener. It is **frozen hourly context at the daily snapshot cutoff**, not
a live hourly scanner or a global interval switch. Daily prices, liquidity,
momentum, state rules and gap episodes retain their existing meanings. Automatic
intraday publication and cross-hour membership transitions remain outside this pilot.

### Fields And Timing

The independent `screening_hourly_context_v1` contract supports four numeric fields:

| Hourly Field | Required Completed Slots | Calculation |
|---|---|---|
| 1h close | 1 | Latest published hourly close |
| 1h last-bar change | 2 | Latest close / previous slot close - 1 |
| 1h close / EMA20 - 1 | 20 | Latest close divided by the exact-window seeded 20-bar EMA, minus 1 |
| 1h EMA20 change over 3 bars | 23 | EMA at last slot / EMA three slots earlier - 1, calculated on one shared 23-bar sequence |

Both EMA calculations use pandas EWM span 20, `adjust=False`, initialized from
the first close in their declared window. The 20- and 23-bar calculations have
different seed windows; neither is an unlimited-history chart EMA. The EMA-change
field is a percentage, not a categorical rising/falling or strength claim. Three
bars means three exchange slots, not necessarily three elapsed clock hours.
Last-bar change can include the overnight move when the latest slot opens a session.

Slots are anchored to the XNYS regular-session open. Normal sessions have six
60-minute slots plus a 30-minute closing slot; early closes are calendar-aware.
Weekends, holidays and DST are handled through the exchange calendar. The final
short slot participates as one bar, and the UI reports its actual duration.
Forming bars, missing slots and after-cutoff inputs are never silently included.
No hourly volume ratio is added, because slot-length and time-of-day volume
comparability would need a separate contract.

The offline reader selects the latest retained 1h publication no later than the
daily market time and availability cutoff. Its market time must equal the daily
close; an older or absent publication marks hourly values unavailable rather than
substituting a stale latest price. Each latest bar must match that publication's
selected member revision. History is gated independently for each field by exact
expected slots, identity, finite valid OHLCV and known corporate actions. A missing
long window does not hide valid hourly close/change or exclude a base-only screen.

### Retained History Reconstruction

The initial preflight found valid latest close/change for all 386 instruments but
no complete stored 20-slot hourly windows. AAPL's representative audit showed
missing afternoon hourly slots on September 9 and 10, not shifted timestamps.
The existing canonical `derive_canonical_bars` aggregator reconstructed missing
historical slots from complete retained 30m inputs available by the same hourly
cutoff. It rejects incomplete buckets and cannot substitute the latest published
slot. Existing hourly revisions take precedence and are not replaced.

There were 2,315 reconstructed history slots across the cohort. Their deterministic
IDs and exact 30m source revision IDs are retained in the snapshot's shared
`hourly_lineage`; they were **not inserted into the canonical bar store**. This
is a labeled retrospective calculation from known retained inputs, not a claim
that those hourly records were published at the historical time. No provider
retrieval, broad source repair, raw-price write or worker activation was needed.

### Controls And Provenance

- Filters contains an off-by-default **Hourly context (1h)** group. Enabling it
	requires at least one bounded numeric condition; percent inputs use percent
	units and serialize as fractions. All hourly conditions apply to one security's
	single captured context and combine with daily/gap rules through tri-state AND.
- The optional **Hourly context (1h)** column displays the four values, with
	signed percentage colors and explicit unavailable values. Selecting the column
	makes no extra request and does not change any predicate.
- The visible status reports hourly market time in Eastern time and final slot
	length. Staleness is evaluated against the currently expected completed hourly
	market time, separately from daily staleness. A snapshot does not become live
	simply because the browser refreshes.
- Expanded row details use read-only `POST /api/stocks/screening/hourly-details`,
	pinned to generation/security ID. They show hourly publication/cutoff, daily
	cutoff, selected latest-bar ID, and reconstructed-slot source IDs. Closed rows
	perform no detail queries; full lineage is excluded from ordinary result pages.
- `predicate.hourly` is an optional versioned 1h group, while the base predicate
	remains `interval=1d`. Saved rules, JSON import/export and draft recovery preserve
	it without changing old daily/gap hashes. CSV results include hourly market time,
	source cutoff/publication and slot duration, plus values if the column is selected.
- **New is unavailable for hourly-filtered rules** in this pilot. Applying such a
	rule clears New only, avoiding a misleading empty view. Daily/gap-only New
	behavior is unchanged. Cross-hour comparison requires its own revision and
	expected-slot policy and has not been inferred from the daily comparison.
- Old snapshots reject hourly predicates/details with 422. The catalog advertises
	the group only when a matching hourly contract is published.

### Published Evidence

Appended snapshot `4cce3b2f-1940-587d-ada9-bb197a628805`, generation
`cae5df0cb702bb2a4e61cbe3f0b200823529ff81ffd9d79092c0ec340beeee05`, for September 11.
Ten screening generations now cover six daily sessions; all nine older screening
generations and all 196 pre-existing portal snapshots were preserved. Only the
SCREENING current pointer was promoted; other pointers and portal source state
were unchanged.

Hourly source publication: `e50187e5-67b3-587c-bec0-78c415f85447`, market time
`2026-09-11T20:00:00Z`, hourly availability cutoff
`2026-09-11T20:26:13.601536Z`, inside the daily cutoff
`2026-09-11T20:35:11.575524Z`. The latest slot is 19:30-20:00 UTC, or 3:30-4:00 PM
EDT, and is explicitly labeled 30 minutes long.

Coverage: **386/386 close and last-bar change; 385/386 EMA distance and EMA change**.
ARKW still lacks a required hourly history slot even after complete-bucket
reconstruction; its EMA fields remain UNKNOWN. This does not undo the earlier
targeted daily-source repair, and no later observations were backdated.

| Retained Review Example | Matches | Unknown |
|---|---|---|
| Hourly close at/above hourly EMA20 | 198 | 1 |
| Daily discovery trend UP plus hourly close/EMA20 and 3-bar EMA change both >=0 | 74 | 13 |
| Daily Pullback plus hourly last-bar change >=0 | 37 | 10 |

The unknown counts follow combined daily/hourly tri-state rules, not only hourly
source coverage. These are descriptive review examples, not calibrated entry
signals or backtested performance claims. Original base/gap values, gaps and New
counts were verified unchanged, including 83 Pullbacks and 15 daily New markers.

Validation: 128 focused backend tests, 60 frontend tests, TypeScript and production
build passed. The read-only verifier checked 11,192 stored source revisions, source
observation/creation cutoffs, selected latest IDs, reconstructed continuity and
recomputed each available field. It preserved snapshot state and frozen-study
source-file hashes. Reader cold 483 ms, warm p95 10.65 ms, default response 238,617
bytes, within the existing gates. Browser checks covered draft/Cancel zero-query
behavior, one Apply request, lazy details, saved rule update/reload, 1% -> 0.01,
invalid ranges, unavailable ARKW history, old-generation rejection, stale labeling
and desktop/mobile light/dark layouts. Temporary verification screens and the
one-shot audit/publication tasks were removed. Only the API reader was restarted;
the existing frontend and independently running paper watcher were left alone.

## Daily Gap Context: Implemented And Assessed

September 13, 2026: completed the first two steps of the approved order: a bounded
daily gap-context extension and an assessment of the resulting retained-data
review lists. The later hourly-context pilot above adds a separate frozen 1h group;
the base screener remains daily-first with no interval selector that silently
changes daily indicator semantics.
Generic Fresh / Active / Confirmed / Invalidated lifecycle filters remain deferred.

### Bounded Episode Contract

`screening_daily_gap_context_v1` is an independent additive gap contract alongside
the unchanged base field set. The offline projector reuses `identify_gap_up`,
`identify_gap_down` and `_gap_lifecycle` from the existing detector implementation.
It does not use the proximity-pruned output of `scan_gap_strategies` as a complete
episode inventory, and it does not inherit that scanner's ATR gate, classifications,
qualifications or strategy results.

The supported universe is all opening-gap formations of age **0-20 expected XNYS
sessions inclusive**, using 22 consecutive verified daily bars: a prior bar plus
the 21 possible formation sessions. Up requires an open at least 1% above the
previous high; Down requires at least 1% below the previous low. No minimum ATR
ratio or current-price proximity pruning is applied to this context inventory.
The full 22-bar window must have valid OHLCV, stable security identity and no
intervening known split/merger/spinoff/symbol-change gate. Failure makes gap
coverage UNKNOWN, not no-gap, without rejecting otherwise valid base screens.

- Episode IDs use version, security ID, daily interval, formation session and
	direction. They do not change when a sliding-window index or current fill state
	changes. Formation and preceding bar IDs remain pinned in the episode record.
- Formation age is 0 on the formation session. The contiguous-window gate makes
	observed-date age equivalent to expected-session age; missing days cannot shorten
	the apparent age. An episode leaving the 20-session scope is not invalidation.
- **Opening gap filled** is the maximum low/high retracement from the formation
	open toward the previous close, clamped to 0-100%. It includes the formation bar,
	retains the existing detector's two-decimal percent precision, and is not a
	percentage of the retained zone. First-fill session is retained separately.
- If the formation bar preserves a full-range gap, the fixed zone lies between
	the prior high and formation low for Up, or formation high and prior low for
	Down. Otherwise the fixed zone is formation open to previous close. The episode
	explicitly names this basis; later price moves do not redraw its boundaries.
- Price location is Above / Inside / Below the fixed zone; equality to either
	boundary counts as Inside. Distance is the absolute current-close distance to
	the nearest fixed edge divided by that edge's price. Ties choose the lower edge.
- Fill states preserve OPEN, PARTIALLY_FILLED, FILLED, SAME_SESSION_FADE and FAILED.
	The UI calls the last two Formation-session fade and Failed at current close.
	FAILED is reversible; a unit test verifies FAILED -> FILLED after a later close.
	It must not be described as permanent invalidation or an exit recommendation.

### Filtering And Presentation

The Filters panel has a **Daily gap context** group, off by default. Enabling
Has matching daily gap adds an optional nested `predicate.gap` with its own daily
version and up to six conditions: direction, fill state, formation age, opening
fill percentage, price location, and nearest-edge distance. All conditions are
ANDed on **one episode**, then eligible episodes are ORed. A direction from one
episode cannot satisfy an age/fill/distance condition on another. With no conditions,
the group means any verified gap inside the bounded formation window.

READY with zero matching episodes is NO_MATCH; incomplete coverage is UNKNOWN.
Base and candle rules continue to combine with this group under existing tri-state
semantics. Gap-filtered New comparisons additionally require matching gap contracts
and projector/detector hashes and the 22-bar lineage checks. A new matching gap
screen is not asserted to be a new gap formation or a confirmed setup transition.

The optional **Daily gap** column shows a representative of the matching episodes:
nearest edge first, then youngest formation age, then episode ID. It includes the
matching count. Expanding the stock row loads all matching episode details on
demand: formation date, fixed zone/basis, opening price, prior-close fill target,
first fill, distance and source IDs. No new fixed columns or tabs were added.
Ordinary result pages exclude the full episode arrays to bound response size.

`POST /api/stocks/screening/gap-details` requires the exact generation and security
ID and reads only that stored snapshot. Catalog gap controls appear only when
the latest publication contains the matching gap contract. Older generations
reject gap predicates/details with 422, while base screening remains supported.
Saved definitions, imports, draft recovery, Apply/save and CSV exports support
the optional group; old rules without it keep their existing hash and revision.
The optional CSV gap column carries the representative episode ID and summary.

### Published Pilot

Only September 10/11 received additive gap-enabled revisions. All seven older
screening generations remain stored; nine generations now span six sessions.

| Session | New Snapshot ID | Generation | Gap Coverage / Episodes |
|---|---|---|---|
| 2026-09-10 | `20d164c6-969f-5a72-b849-4c9d7a5807ec` | `80c75d19e160d35ea452f2b120b0231d93fc6e748a1f9e959e7ed355f39b0b46` | 383 / 897 |
| 2026-09-11 | `0f3fbb63-9bbe-569f-9ee7-cd4d16dfc2fb` | `dd22f937d4753eac3fcb41a0d2a2264c233de2a4756efd89364b1ebde9721257` | 383 / 830 |

September 10 was inserted without pointer promotion; September 11 is the current
SCREENING pointer. All 194 pre-existing portal snapshots/hashes/timestamps,
non-screening pointers and portal source state were preserved. The verifier
recomputed generation digests and confirmed original fields/candles/bar IDs/lineage
are unchanged against earlier revisions of the same source publication.
The current ordinary counts remain All equities 386, Pullbacks 83/New 15,
Bounces 38/New 31, Resuming up 27/New 23, and Resuming down 6/New 2.

Gap coverage is unavailable for APH (corporate-action review), ARKW (missing
expected session at the original source cutoff) and VMRK (insufficient history).
Later repaired source prices were not backdated into this pilot. Action coverage
still means known actions only, not a certified complete corporate-action feed.

September 11 assessment, with example thresholds fixed for inspection, not tuned
for returns:

| Example Screen | Matches | Unknown | Verified New |
|---|---|---|---|
| Any bounded daily gap | 279 | 3 | 4 |
| Up; Open/Partially filled; age <=5 sessions | 46 | 3 | 6 |
| Above, plus price >=$5, prior mean dollar volume >=$20M, edge distance <=2% | 20 | 3 | 8 |
| Recent unfilled Up gap, edge distance <=2%, daily discovery trend UP | 9 | 3 | 2 |

The last row is a separate trend-context combination, not the preceding liquidity
screen plus another condition. These examples demonstrate a manageable chart-review
list and same-episode explanations, not predictive value. For instance AXTI's
September 8 episode has 76.41% opening fill while price is above its fixed
$61.64-$64.48 zone, 0.45% from the upper edge. Fill history and current proximity
answer different questions; neither is an automatic entry.

### Verification And Next Step

123 focused backend tests, 58 frontend model tests, TypeScript, production bundle
and retained-reader verification passed. Checks include same-episode conjunction,
unknown versus known-empty coverage, reversible FAILED, stable IDs/boundaries,
safe windows, old-hash/definition compatibility, strict bounds, API provenance and
unchanged frozen-study source hashes. Reader measurement: cold 336 ms, warm p95
7.79 ms, default response 219,568 bytes (under the 250 KiB gate). Episode details
are fetched separately, only on expansion.

Browser verification covered draft/Cancel zero-query behavior, Apply once,
46-to-20 match refinement, 2% -> 0.02 conversion, rejecting age 21, saving/updating
and reloading nested gap rules, saved-rule preview, column changes without fetches,
one lazy detail request, and mobile light/dark layouts with no body overflow.
Temporary verification definitions and the one-off publication task were removed.
Existing Gap/research pages were not retired or changed.

Next: assess the daily extension during real review. A separately bounded 1h
context pilot remains conditional on a need for intraday monitoring. Preserve
daily trend/liquidity/gap definitions; an hourly field would need its own completed
timestamp, freshness, warm-up and session-boundary contract. Do not add a global
interval switch that reinterprets daily momentum, and do not infer generic
Fresh/Active/Confirmed/Invalidated states from rolling record disappearance.
No data worker was started by this work. During verification a paper-study watcher
was already running after an external environment change and was left untouched;
only the API and frontend were brought up for inspection.

## Optional New Feature: Implemented

September 13, 2026: user-approved lightweight New comparison is implemented and
verified. This is a daily-review aid, not a trend-strength measurement, first-ever
signal, alert, entry recommendation or predictive qualification. Full Current /
New / Still matching / Dropped views remain deferred.

- The full screen remains the default. New only is an unchecked checkbox beside
	the existing match count, with the verified New count and comparison dates.
- New markers appear beside tickers without adding a table column. The existing
	column picker has a New markers checkbox to hide them without making a query.
- New only is a transient request option: it resets on screen load/switch and
	page reload. It is not part of a saved predicate, predicate revision, definition
	export or filter draft. Marker visibility is also a page-local display choice.
- The original match/unknown counts remain screen-wide. New-only rows are filtered
	before sorting/pagination and use a separate result count. Empty results say
	No verified new matches; missing comparison coverage is explicitly unavailable.
- Row explanations include the comparison reason, prior session and exact prior
	generation. CSV exports retain the original generation/session/security IDs and
	add New status/reason, New-only selection and prior comparison provenance.

### Comparison Contract

`Query.new_only` defaults to false; `view` remains CURRENT. The read-only API loads
the current snapshot and the latest stored revision for the exact preceding XNYS
session, including weekend/holiday handling. If the latest prior revision is
incompatible, it does not fall back to a more convenient older revision or day.
Only published SCREENING storage is read; no detectors, raw-price reads, provider
requests, source writes or snapshot preparation run in page requests.

The pure evaluator applies the same current rules to both populations and joins
by security ID. New requires a current MATCH and a prior NO_MATCH with comparable
eligibility, instrument type and known required prior fields. An absent prior
member, unavailable field or eligibility transition cannot become New. A re-entry
can be New; it is not asserted to be the security's first historical match.

Publication version, field set, full field/pattern contract hash, universe,
interval, capture mode and canonical detector hashes must agree. This first
policy supports only original `RECONSTRUCTED_FROM_RETAINED_PUBLICATION` snapshots
with ordered source cutoffs. Corrected capture modes or a prior cutoff at/after
the current cutoff are unavailable rather than mixed silently into a comparison.
Within the requested lookback (at least the preceding bar), shared bar revision
IDs must align and newly observed action IDs make the comparison unavailable.
This conservative gate may omit real transitions when source history changed;
it does not label a source correction as a new market-driven match. Unreferenced
display columns and sort choices do not affect New membership.

Response additions: row `new_status` (NEW / NOT_NEW / UNAVAILABLE) and `new_reason`;
`result_count` for the selected view; `comparison` with status, current/prior dates,
prior generation, `new_count` and unavailable count among current matches. A missing
comparison has `new_count=null`, not a fabricated zero. New only can always be
unchecked if coverage becomes unavailable while that view is active.

### Verified Results

September 11 vs September 10, using the unchanged current generation
`c8f22be310fdeeb1cb30783b05a677160fa8630251130f95e66b1d1ac7b6f1e4` and prior
`7a05907b2e9953fe361a3f4ab285277345e2ac5ad64ea40819b432753fa101c5`:

| Screen | Current Matches | Verified New | Comparison Unavailable Among Matches |
|---|---|---|---|
| Pullbacks | 83 | 15 | 11 |
| Bounces | 38 | 31 | 1 |
| Resuming up | 27 | 23 | 1 |
| Resuming down | 6 | 2 | 1 |

All equities remains 386 matches with zero New and 44 comparison-unavailable rows.
All 11 unavailable Pullback comparisons report SOURCE_HISTORY_CHANGED. They stay
in the full screen but not in New only. These are coverage limitations, not another
request to backfill or permission to overwrite old snapshots.

Validation: 118 focused backend tests, 56 frontend model tests, TypeScript and
production bundle passed. Tests cover same-rule transitions, missing sessions,
holidays, identity/eligibility changes, unknown fields, detector/contract/revision
differences, source-history/action changes, sorting/pagination, strict HTTP input,
and saved-definition/CSV contracts. The existing read-only verifier checks New
counts and paged identity parity; stored snapshots/pointers and frozen-study source
hashes remain unchanged. Its VS Code task now defaults to read-only verification;
publication transaction checks still require explicit `--check-publication`.

Measured reader cold 285 ms, warm p95 4.81 ms, default 100-row response 184,827
bytes. Browser HTTP Pullbacks p95 was 28.2 ms across 20 requests. Desktop 1440 and
mobile 390 checks in both themes passed: marker visibility performs zero queries,
New only preserves saved workspace data, screen switching/reload resets it,
zero-New pagination is disabled, and a missing-prior fixture displays unavailable
without markers while allowing return to the full list. Existing tabs, fixed four
columns, closed filters and layouts remain intact. Only the API reader was
restarted; frontend stays available on port 5174 and paused workers remain stopped.

## Future Enhancements: Revised Recommendation

Recommendation updated September 13, 2026 after reviewing the value of Membership
Changes for trend interpretation. This section supersedes the earlier
comparisons-first delivery order. It documents future options only; it does not
authorize implementation, new publications, provider retrieval or worker startup.

The primary goal is to make a stock's recent trend context easier to understand.
Membership Changes answers "What changed in my screen's list?", not "Is the
trend strengthening?" It has useful daily-review value but limited standalone
trend-reading value. There is no demonstrated improvement in stock selection or
predictive performance from adding these views.

| Priority | Enhancement | Purpose | Initial Scope |
|---|---|---|---|
| 1 | Recent state progression | Show how the observed state evolved, rather than only today's label | Optional compact five-session history in the row detail; dated original discovery states and explicit unknowns |
| 2 | Moving-average direction alongside price distance | Distinguish price above a falling average from price above a rising average | Inventory and version the slope/lookback/flat-tolerance contract before projecting any new field; retain existing price-versus-average fields |
| 3 | Price and volume context around transitions | Make the size and participation of a move inspectable | Reuse change, relative volume, prior-range distance and chart links; no new composite confirmation or confidence score |
| Implemented | New since previous session | Reduce repeated review of a large daily result list | Optional New marker and New only checkbox; assess actual review value before expanding it |
| Deferred | Full Current / New / Still matching / Dropped views | Organize screen membership changes and explain departures | Revisit only if the lightweight marker is insufficient and users need the additional workflow |

### Trend Context Semantics

A sequence such as Pullback -> Pullback -> Resuming up is an example of descriptive
state progression, not proof of a confirmed breakout or a profitable entry.
Render the existing detector's exact dated states; do not infer intermediate
states, episode continuity or future direction. A recent-state timeline is not
screen-match persistence: one shows detector states, the other evaluates the
screen's base rules on each expected session.

Use one compatible generation per expected exchange session and stable security
identity. Missing observations stay UNKNOWN, not flat or unchanged, and must not
be replaced by older available sessions. Start with bounded display-only history;
do not add streak thresholds or expand the filter catalog as a side effect.
Reuse existing tabs, closed-by-default filters and the four fixed table columns.
Prefer optional columns or row details over another permanent toolbar or page.

Moving-average distance alone cannot establish trend direction. Any new direction
field needs a declared average definition, sufficient prior values, slope window,
units and flat threshold, with causal warm-up/action/identity tests. Until those
facts are projected, show the existing distance and chart context rather than
inventing rising/falling labels. Price and volume observations remain context,
not independently validated trade confirmation.

### Membership Value And Boundaries

New means newly satisfying the same rules since the prior expected session; it
can also be a re-entry, not a first-ever trend. Still matching proves only that
both observations match, not that a trend is strengthening. Dropped is not a sell
signal: leaving Pullbacks because the state becomes Resuming up can be positive.
Threshold fluctuations, universe changes and data corrections are not evidence
of a new economic trend. The implemented marker retains the same-rule, full-population,
stable-identity and UNKNOWN safeguards in the deferred comparison contract below.

### Evidence And Release Gate

Assess usefulness on a small representative set of retained rows: can the user
explain state changes with fewer chart lookups, distinguish trend direction from
price distance, and identify unknown or corrected observations? For a New marker,
check whether it actually reduces repeated daily review. These are workflow
criteria, not a substitute for a separately approved predictive study.

The six retained screening sessions provide a bounded history pilot, not an
ongoing live service. Repaired source bars retain their September 13 observation
times; original screening snapshots still reflect earlier knowledge. To display
repaired historical facts, append explicitly corrected generations under a
consistent revision policy. Never silently mix original and corrected histories
and present the resulting differences as market-driven transitions.

Keep page requests read-only and benchmark the bounded multi-session reader before
release. Five-session screen persistence, lifecycle filters and an ongoing daily
producer remain separate later proposals. Producer activation, broader data
retrieval, research-page retirement and any trading qualification require their
own approval. Only the lightweight New marker/filter is now implemented; the trend
context and broader membership enhancements remain proposals.

## Targeted Source Repair: Completed

September 13, 2026: following explicit approval to address the remaining price
gaps, a bounded Polygon/Massive daily-range retrieval repaired the canonical
source history. This supersedes the unresolved-source status in the earlier
snapshot-repair record below. No general backfill or worker activation was used.

| Instrument | Completed Repair |
|---|---|
| ARKW | Retrieved and inserted all five missing daily bars: July 22, August 14/21, September 1/9, 2026 |
| BKNG | Retrieved and inserted all four missing daily bars: September 9/18/26 and October 14, 2025 |
| BNY | Filled 132 missing dates and appended preferred corrections for 48 existing wrong-identity price rows |

BNY was not merely sparse history. Dated provider reference evidence identifies
`BNY` on September 5, 2025 as **BlackRock New York Municipal Income Trust**, a fund
with composite FIGI `BBG000BZF6G2` and CIK `0001137390`. The bank traded as `BK`
through May 20, 2026 and as `BNY` from May 21. Its composite FIGI
`BBG000BD8PN9`, share-class FIGI `BBG001S5P6Q6`, and retained security ID
`7fcefd3c-becf-53c4-917a-33838f71d14f` establish the bank's identity continuity.
The old fund's prices must not be attached to that bank identity.

The repair retrieved the bank's 180 daily bars under the original `BK` ticker
for September 3, 2025 through May 20, 2026. It retained those native bars and
appended 180 `RECONCILED` BNY continuity revisions, each pointing to its exact
native BK source bar. The 132 holes are now filled; the other 48 revisions take
precedence over erroneous fund-price inputs without deleting them. For example,
September 3, 2025 previously held a $9.60 fund close under BNY's bank identity;
the bank's verified BK close is $104.68. Continuity revisions carry
`DATED_SYMBOL_CONTINUITY_BK_TO_BNY`, `SOURCE_TICKER_BK`, and
`TARGETED_IDENTITY_REPAIR` quality tags.

The transaction inserted **369 revisions**: nine direct gap fills, 180 native BK
bars, and 180 linked BNY corrections. It did not rewrite any old bar, identity
reference, source publication, screening snapshot, or current pointer. No raw
price was split-adjusted, and no corporate-action exclusion was relaxed. BNY
history outside this bounded lookback has not been certified or repaired; this
operation is not a general historical symbol-resolution change.

Staged raw bars and dated identity evidence are checksummed in
`backend/backups/screening_source_gap_repair_v1.json`. The completed canonical
ingestion segment is `533ec3dc-96fb-56f1-8e48-e6b0d6c03e6b`. The bounded repair
script stages with `--fetch`, applies retained evidence with `--apply`, and offers
read-only verification:

```powershell
backend/.venv/Scripts/python.exe backend/scripts/repair_screening_source_gaps.py --verify
```

Verification: 105 focused backend tests passed, including old-fund identity
rejection, deterministic BK source lineage, actual availability timestamps, and
transaction rollback. Exact database readback verified OHLCV, security IDs,
payload hashes and source links. Repeat application inserted zero rows. The
preferred-series check found **258/258 expected sessions for each of ARKW, BKNG
and BNY**, September 3, 2025 through September 11, 2026, with zero source gaps and
all 180 preferred BNY pre-transition revisions linked to native bank prices.
The six-session screening integrity check and frozen-study source hashes passed.

**Availability boundary:** these bars were retrieved on September 13 and retain
that actual observation time, with `replay_available_at=NULL`; they are not
backdated into September 3-11 knowledge. Consequently the seven existing immutable
screening generations retain their original gaps and counts, including ARKW's
September 9 UNKNOWN. Serving revised historical screening facts would require
an explicitly corrected generation with a later availability cutoff, not silently
editing those snapshots. The current September 11 screen remains unchanged.
Membership Changes, persistence, cleanup of older scripts, and worker activation
were not pursued during this source-repair task. The temporary provider/identity
probe tasks from this repair were replaced by one read-only verification task.

## Missing Snapshot Repair: Completed

September 13, 2026: the bounded September 3-11 audit found five missing
SCREENING sessions, distinct from missing market bars. Those five projections
have now been appended from retained inputs at their original source publication
cutoffs. There are seven immutable generations across six expected sessions;
the two existing September 11 revisions were preserved, not rebuilt.

| Session | Snapshot ID | Expected / Basic Eligible | Outcome |
|---|---|---|---|
| 2026-09-03 | `412cbff9-3ce2-5034-9494-bf96801ba2aa` | 386 / 386 | Restored |
| 2026-09-04 | `c141bff5-e65b-54d4-8d66-faf40186e630` | 386 / 386 | Restored |
| 2026-09-08 | `4a2019fd-9677-5538-acd8-016251cf7e3d` | 386 / 386 | Restored |
| 2026-09-09 | `e00e1abf-da5f-5740-b897-367e4b032c17` | 386 / 385 | Restored; ARKW explicitly UNKNOWN |
| 2026-09-10 | `7ec26368-5f5c-5d3a-88d9-5a082a3f34dc` | 386 / 386 | Restored |
| 2026-09-11 | `51a64839-1005-54e9-9164-5c7980f6b1b9` | 386 / 386 | Existing v2 retained |

September 7 was an exchange holiday, not a missing snapshot. Every restored
snapshot contains the full expected membership, uses exact selected latest-bar
IDs, retains actual preparation time, and is labeled
`RECONSTRUCTED_FROM_RETAINED_PUBLICATION`. The new processing policy is
`screening_complete_expected_members_v1`. A degraded source requires an explicit
dated request; unavailable source members are preserved with null price/features
and explicit reasons, never borrowed prices or fabricated bars.

### Remaining Source And Field Gaps

The initial projection repair did **not** mean the underlying price history was
complete. The following records the original audit; the targeted source repair
above now resolves these source holes, while the earlier snapshots intentionally
retain their original knowledge cutoffs. Across their retained lookbacks, the
read-only lineage audit found:

| Instrument | Missing Internal Daily Dates | Consequence |
|---|---|---|
| ARKW | 2026-07-22, 2026-08-14, 2026-08-21, 2026-09-01, 2026-09-09 | Missing September 9 price/volume; September 10 change/candles unavailable; longer lookbacks also affected |
| BKNG | 2025-09-09, 2025-09-18, 2025-09-26, 2025-10-14 | Long-window history incomplete; known split also gates affected windows |
| BNY | 132 internal dates between 2025-09-05 and 2026-05-20 across the six lookbacks | Retained symbol/identity history requires review; not assumed to be ordinary listing warm-up |

At that audit, these dates had no retained final unadjusted daily RTH revision under
the same ticker or an alternate ticker carrying the same security ID. This is a
bounded canonical-store check, not proof that no external source has the data or
that differently identified predecessor symbols can safely be merged. No identity
continuity rule was changed.

ARKW September 9 is the only unavailable latest-session source member in this
six-day interval. Retained intraday coverage at the original cutoff and in latest
storage is also incomplete: 49/78 unique 5m starts, 22/26 at 15m, 12/13 at 30m,
and 4/7 at 1h. In particular, the 19:00-19:30 UTC interval is absent from 30m and
the finer grids. A safe complete daily candle cannot be derived from these
inputs. The subsequently approved targeted provider retrieval supplied the daily
bar directly and retained its actual observation time; it was not inserted into
the historical knowledge cutoff. Intraday grids themselves were not backfilled.

Momentum and original daily discovery states are available for 361/386 members
on every session. The 25 unavailable long-window results reflect action gates
and insufficient retained history, including the holes above. The insufficient
history group is ARKW, BKNG, BNY, CBRS, FISV, HONA, MRSH, P, SPCX, SUNB and VMRK
(ARKW is instead source-unavailable on September 9). Do not describe all 11 as
new listings. Fourteen other members are action-gated for the 253-session fields;
15 unique tickers have an action gate on at least one field across this interval.
No action-adjustment or identity gate was relaxed to improve coverage.

### Repair And Verification

The bounded CLI accepts one session or at most six exchange sessions. Without
`--publish` it only measures. Explicit dated publication always uses
`promote_current=False`; `--missing-only` skips existing compatible field-contract
generations. The historical run used:

```powershell
backend/.venv/Scripts/python.exe backend/scripts/prepare_stock_screening.py --start-session 2026-09-03 --end-session 2026-09-11 --missing-only --allow-degraded --publish
backend/.venv/Scripts/python.exe backend/scripts/verify_stock_screening.py --start-session 2026-09-03 --end-session 2026-09-11
```

- Focused backend suite: 102 passed; only existing FastAPI deprecation warnings.
- Publication preservation: all 189 pre-existing snapshot hashes/timestamps,
	every current pointer, and portal source generation/timestamp unchanged.
- Exact missing-only rerun: all six sessions skipped, zero inserts, 194 total
	portal snapshots preserved. No duplicate September 11 generation.
- Read-only six-session verifier: stored payload/manifest hashes, generation
	digests, full unique membership, field/pattern coverage, selected source IDs,
	original source cutoffs and UNKNOWN counts passed. Frozen independent-study
	source-file hashes still match their manifest.
- Live API still serves September 11 generation
	`c8f22be310fdeeb1cb30783b05a677160fa8630251130f95e66b1d1ac7b6f1e4`:
	386 basic matches, Pullbacks 83, Bounces 38, Resuming up 27/down 6. Historical
	September 9 serves 385 basic matches plus one UNKNOWN through the same reader.
- Only the existing API and Vite/esbuild processes remain. No reader restart,
	paused-worker activation, provider request, source-bar mutation, study change,
	alert regeneration, or new UI feature was performed.

The snapshot-coverage prerequisite is now met, with explicit field limitations.
Membership comparisons, persistence calculations/filters, lifecycle adapters and
ongoing publication remain unimplemented or disabled. Existing persistence status
text on immutable payloads is not a newly enabled capability. The earlier assessed
delivery sequence below is superseded only for this completed preparation step.

## Deferred Capabilities: Assessed Plan

Assessment date: September 13, 2026. This section is a recommendation, not an
implementation or authorization to start workers. The assessment used code reads,
the published screening reader, and two bounded PostgreSQL metadata queries in
READ ONLY transactions with five-second statement limits. No screening build,
source download, provider call, lifecycle evaluation, or data write was performed.

### Current Evidence

At the original assessment, the reader exposed two immutable generations for one
distinct session, September 11, and no prior September 10 SCREENING generation.
The completed repair above supersedes that coverage gap. Revisions of September
11 still cannot count twice toward persistence.

Recent retained canonical daily RTH unadjusted source publication metadata:

| Session | Source Publication | Selected / Expected |
|---|---|---|
| 2026-09-03 | COMPLETE | 386 / 386 |
| 2026-09-04 | COMPLETE | 386 / 386 |
| 2026-09-08 | COMPLETE | 386 / 386 |
| 2026-09-09 | DEGRADED | 385 / 386; ARKW is MISSING |
| 2026-09-10 | COMPLETE | 386 / 386 |
| 2026-09-11 | COMPLETE | 386 / 386 |

These are the six expected exchange sessions in this interval; weekends and the
September 7 holiday are not missing sessions. Publication timestamps can be after
midnight or the following day, so availability must use timestamps, not date casts.
Metadata completeness does not establish full per-field lookback/reference/action
coverage; the bounded builder must still verify those dependencies at each cutoff.

The same bounded interval contains 155 AT_EDGE and 519 FORMING daily canonical
PATTERN_OBSERVATION records from forming_patterns_v1, spread over six sessions.
The pattern producer maps readiness only to those two states; this is not a
retained CONFIRMED/INVALIDATED transition history. There are also 1,258 daily
GAP_STRATEGIES bundle records from portal_strategy_bundle_v3 across six sessions,
covering 279 distinct securities. Those are per-security bundles, not 1,258 unique
gap episodes, and their record lifecycle_status is MATCH. Missing records do not
prove that a detector evaluated a security and found no setup.

### 1. Membership Comparisons

Deferred design, not the next recommended delivery. The revised recommendation
above prioritizes trend context; the separately approved lightweight New marker is
now implemented without these additional views.
If the full workflow is later justified, use Current / New / Still matching /
Dropped as a small results-view selector inside the active screener tab. Keep Current as the default.
Show counts only after evaluating the full population; pagination and display
presets must not change membership counts. Do not add per-strategy page tabs or
reintroduce the historical-date dropdown.

Compare the same applied predicate against current and prior expected daily
generations, joining by stable security ID. Recompute both sides when rules change;
do not compare old-rule membership with new-rule membership. Use backend canonical
predicate identity; browser names, layout changes and revision labels alone are
not evidence of equivalent rules. Referenced field/detector semantics, interval,
price basis and universe policy must be compatible, not merely share field names.

- NO_MATCH to MATCH: New; MATCH to MATCH: Still matching.
- MATCH to NO_MATCH with valid comparable eligibility/data: Dropped.
- UNKNOWN or absent prior to MATCH: Current, with change unavailable; not New.
- MATCH to UNKNOWN or missing current identity/data: change unavailable; not Dropped.
- Universe admissions/removals and eligibility changes are separate reasons, not
  price-pattern entry/exit events. Never substitute the next available older day.
- Drop rows display last-matching session/values, plus separately dated current
  values in the explanation showing which condition failed. They are excluded
  from the Current matching denominator. No invented P/L, entry or stop is added.

Useful presentation: New narrows the review queue; Still matching keeps ongoing
research candidates visible; Dropped explains which requirement stopped passing.
Inside Current, an optional change column can distinguish these states. Do not
repeat an all-identical New/Dropped column inside an already-selected change view.
Comparison gaps are coverage warnings, not red trade signals. Chart links and
source-pinned explanations are the actions; no automatic Stock Alerts record.

The cheap real-data preparation gate is now complete: September 10 was preflighted
read-only and reconstructed at its retained cutoff using the September 11 v2 field
contract. The lightweight New comparison is now implemented; full membership views
remain deferred. These reconstructed
generations are not proof that a historical live alert existed.

### 2. Five-Session Persistence

Start with persistence of the screen's base rules, not a composite detector score:
evaluate the same non-persistence predicate on each expected session. Add optional
table fields such as Matches in last 5, Verified streak (5-session cap), and Last
valid match, with five dated MATCH/NO_MATCH/UNKNOWN cells in the explanation.
Do not call observation age or days in a pullback a screen-match streak.

The September 11 five-session window is September 4, 8, 9, 10 and 11. Count one
selected compatible revision per expected session. For partial coverage, show
known matches and coverage separately (for example, 3 known matches / 4 observed
of 5 expected), not 3/5 with the missing session silently counted as a nonmatch.
An unknown session breaks proof of a consecutive streak; verified suffix length
can be shown as a lower bound with coverage, but never as an exact longer streak.
If no match is found within retained history, report that bounded result rather
than inventing an unlimited age. A streak reaching the five-session boundary is
5+ or explicitly capped, not asserted to have started exactly five sessions ago.

Keep persistence display-only first. Add filters only after their semantics are
frozen: history measures the base rule before persistence conditions, avoiding a
self-referential test. Thresholds must use tri-state coverage: a known lower bound
can prove a minimum satisfied, and the maximum possible count can prove failure;
otherwise unknown observations make the threshold UNKNOWN, not an automatic pass.
Membership comparisons for such a five-session-filtered rule require six distinct
session generations to evaluate both adjacent five-session windows.

The bounded builder now implements the versioned complete-processing/degraded-source
contract and has retained an explicit unavailable ARKW row for September 9.
Do not omit that row, repair source history silently, or discard the whole day for
the other 385 members. Expected source omissions, field warm-up failures and
unexpected processing failures retain distinct reasons.

The current four-generation LRU cannot hold a five/six-generation working set.
Use an explicitly bounded cache of compact screening facts, with shared lineage
loaded separately when needed; benchmark the multi-session path rather than
assuming the existing one-generation timing still applies. No per-screen price
store or saved-result ledger is needed, and no automatic data purge is proposed.

### 3. Lifecycle Filters

First candidate: a small daily gap-state adapter. Existing gap payloads provide
direction, formation geometry, fill percentage, first-fill date and age. The
source code supports OPEN/PARTIALLY_FILLED/FILLED/SAME_SESSION_FADE/FAILED, but
FAILED depends on the current close after a fill and can later change. It is not
an absorbing invalidation event. Preserve the source wording or version a separate
true invalidation contract; do not relabel it as permanent failure.

Before enabling filters, verify per-episode identity, stable anchors, detector
version, expected evaluation coverage and terminal-event retention. An episode
falling out of a rolling scan window is not invalidation. Reuse canonical evidence
and snapshot storage rather than frozen alert-study results. Exact point-in-time
adapter parity is still unverified by the metadata counts above.

Suggested controls: Gap direction, Gap state, Formation age (sessions), Fill %,
and distance to the named retained boundary. All conditions must hold for the
SAME gap episode; do not satisfy age on one gap and direction/state on another.
If several episodes qualify, retain all matching IDs in the explanation and choose
a documented deterministic representative for optional table columns. Geometry
provides chart context, not an automatically executable entry/stop/target plan.

Forming compression/structure observations are a subsequent candidate. Existing
FORMING/AT_EDGE can support explicitly named observation filters after coverage
validation, but cannot authorize a generic Fresh / Active / Invalidated selector.
Confirmed breakout, invalidation and re-armed filters need an actual versioned
transition producer with causal trigger/invalidation rules and episode identity.
Keep such controls disabled until that contract exists. Candle occurrences remain
one-bar events, and daily discovery Resuming state is not itself a fresh trigger.

### 4. Ongoing Publication

Prepare an opt-in daily producer, but enable it only with separate approval.
Trigger on a newly eligible canonical daily source publication, not a browser
refresh or an assumed wall-clock close. The screening producer consumes retained
data only; if upstream feeds/materializers remain paused, it must wait and report
staleness rather than start them or call providers.

Add a publication-wide leadership lock, processed source/version identity,
bounded retry/resume, expected-member reconciliation and source-delay/grace handling.
Reprocessing an identical input is idempotent; late source correction appends an
identifiable revision instead of overwriting an earlier generation. Define the
comparison revision policy once: latest compatible revision known at the request's
captured cutoff, while preserving prior generation IDs for audit.

The default portal publisher promotes the current pointer by publish wall time.
Historical preparation now uses the verified explicit no-promotion path. Before
live activation, promotion must not move backward in source session during
concurrent/late completion. Keep atomic snapshot/manifest/pointer visibility and verify crash
rollback. Missing source members can be represented explicitly under the new
contract; unexpected processing failure must never expose a half-built cohort.

Normal page status stays compact: Daily / source session, with warnings only when
publication is missing, delayed or stale. Detailed source cutoff, revision and
coverage stay collapsed. Browser-local screens do not become unattended monitoring
or notifications merely because a daily shared dataset is being produced.

### Recommended Delivery And Acceptance

The original comparisons-first sequence is superseded. Bounded six-session
snapshot preparation and targeted source repair are complete; they do not enable
new UI features or make original snapshots reflect later corrections.

1. Propose display-only recent-state history over the retained sessions, with
	explicit coverage and a bounded compact history reader.
2. Inventory moving-average direction and improve price/volume context using
	existing facts first. Validate usefulness before adding new fields or filters.
3. Assess the implemented optional New marker/filter in daily review. Keep the
	full four-view Membership Changes interface deferred unless more workflow is needed.
4. Evaluate screen-rule persistence and lifecycle adapters separately. Persistence
	starts display-only; filters follow only after base-rule semantics are frozen.
5. Validate producer restart/correction/freshness behavior before requesting
	activation approval. Do not restart paused upstream workers implicitly.

Acceptance for each implemented slice must cover its relevant contracts, including
exact dated state history, moving-average semantics, and, if comparisons are added,
the full state-transition truth table, missing expected days,
same-day corrections, eligibility/identity changes, shared predicate on both sides,
unchanged counts across sort/page, dropped-value provenance, 5/6-session windows,
no counting duplicate revisions, no false lifecycle invalidation, old-publication
compatibility, and unchanged saved-definition roundtrips. Desktop/mobile checks
must keep existing tabs, closed-by-default filters and the four fixed base columns.
Readers must still make zero detector/provider/outcome calls or source writes;
only the active tab queries. Re-measure cold <=1s, warm p95 <=250ms and the default
100-row response <=250 KiB against the new history path. Report unmet gates rather
than hide unavailable coverage or imply predictive qualification.

## Sources And Dependency Boundary

`equity_bar_publications` daily RTH unadjusted membership (COMPLETE by default;
explicit dated DEGRADED opt-in) -> bounded
32-security batches of canonical bars, dated reference revisions and known actions
-> field-level calculation and canonical candle detector -> one immutable
`SCREENING_DAILY_V1` portal snapshot -> pure predicate evaluator -> active UI.

The builder is opt-in, max 1,000 expected members and six explicitly bounded
exchange sessions per CLI run. The default still selects the latest complete
source publication; dated runs can explicitly permit degraded membership.
It fixes the source publication's published-at availability cutoff,
and labels the output RECONSTRUCTED_FROM_RETAINED_PUBLICATION. Publication time is
the actual new storage timestamp, never the historical source date. Exact selected
latest-bar IDs must agree; otherwise the affected security is unavailable. Dated
references require same security/ticker, effective date no later than the session,
and observed/created times no later than the fixed cutoff. A populated source date
must also be no later than the session. The retained reference producer leaves
source_as_of_date empty; effective_from is its dated contract (verified on retained
CS/ETF/ETV records), not a reason to join current metadata.

Existing portal publication transactions provide atomic cohort/manifest/pointer
writes and content-addressed retry identity. Migration 041 only expands the
snapshot-type constraint; it changes no source rows. Apply it after the canonical
baseline for a fresh installation. The standard worker's snapshot list is unchanged.
Historical generations remain in the existing store; there is no automatic purge.

## Field Contracts

All percentages cross the API as fractions. UI inputs and labels use percent.
Bounds are inclusive; zero is a bound, an empty input is not. All price fields use
unadjusted canonical daily RTH prices. Windows require same security identity,
all expected XNYS sessions and valid OHLC/volume geometry. Known split, merger,
spinoff or symbol changes inside a lookback make that field UNKNOWN; they do not
invalidate independent current price/volume. Action completeness is NOT certified;
every publication explicitly declares KNOWN_ACTIONS_ONLY_NOT_CERTIFIED.

| Field | Definition | Required Sessions |
|---|---|---|
| Instrument | Dated security reference CS / ETF / ETV, not current ticker metadata | 0 |
| Price / volume | Exact published completed daily close / shares | 1 |
| Change | Close / previous session close - 1 | 2 |
| Average dollar volume | Arithmetic mean of close * shares, prior 20 excluding current | 21 |
| Relative volume | Current shares / arithmetic mean of prior 20 shares; zero denominator UNKNOWN | 21 |
| vs EMA20 / EMA50 | Close / EMA - 1; pandas adjust=False, seeded first close of exact 20/50-session window | 20 / 50 |
| vs SMA200 | Close / inclusive 200-session mean - 1 | 200 |
| 12-1 momentum | Close 21 sessions ago / close 252 sessions ago - 1 | 253 |
| Momentum percentile | Ascending position / (eligible count - 1), ID tie-break, singleton 0.5; full field-eligible CS/ETF/ETV cohort before filters | 253 |
| Realized volatility | Sample standard deviation of 21 simple returns * sqrt(252) | 22 |
| Prior range distance | Close / maximum high of prior 20 sessions - 1 | 21 |
| Daily discovery state / trend | Unmodified stock_discovery_v1.stock_features state/trend, subject to its price >= $5 and inclusive 20-session median dollar volume >= $20M eligibility and the screening action/identity gates | 253 |

These are independent screener features, not aliases for the discovery service's
median dollar volume or globally warmed-up feature bundle. No discovery calculation
or alert policy was modified. Filters AND across fields; missing optional displays
do not exclude stocks. Known failure dominates UNKNOWN in AND, known success in OR.

## Pattern Contracts

`equity.technicals.detect_setup_candlesticks(input_includes_forming_bar=False)` is
the existing producer. v1 admits Bullish engulfing (+1), Bearish engulfing (-1),
Shooting star (-1), Hammer (+1), and Doji (0). Exact detector source hash is frozen
in the manifest. Two valid completed bars are required even for single-bar shapes,
matching the producer's guard. Detection runs only during explicit preparation.
Present observations have age 0 and OCCURRENCE state; absence is known only when
the detector could be evaluated. ANY / ALL / NONE preserve UNKNOWN, interval and
direction. Candle occurrences are not continuing setups or trade recommendations.

Daily discovery Pullback/Bounce/Resuming states are supported as completed-session
observations, not lifecycle episodes. Deferred: compression/breakout lifecycle,
gaps and active lifecycle states until compatible retained coverage is verified. Sector,
industry, market cap, ADX, RSI, ATR and RS63 remain disabled rather than borrowing
current metadata or inventing defaults. Five-session persistence also remains
disabled until all five expected compatible screening publications are retained.
No live rescan fallback or skipped-session count is permitted.

## Verification Commands

From the repository root on Windows:

```powershell
backend\.venv\Scripts\python.exe -m pytest backend/tests/test_screening.py -q
backend\.venv\Scripts\python.exe backend/scripts/prepare_stock_screening.py
backend\.venv\Scripts\python.exe backend/scripts/prepare_stock_screening.py --publish
backend\.venv\Scripts\python.exe backend/scripts/verify_stock_screening.py
backend\.venv\Scripts\python.exe backend/scripts/verify_stock_screening.py --check-publication
npm.cmd --prefix frontend run build
node --test frontend/tests/*.test.mjs
```

Preparation and publication are separate from reader timing. Reader tests must
verify no source/detector/provider work, strict unsupported-generation errors,
fixed full-population counts and bounded four-generation caching. Live measurement,
coverage and browser gate results are recorded below as verification proceeds.

## Verified Checkpoint: September 13, 2026

Steps 1-4 are implemented. The first usable unsaved slice was verified before
adding saved definitions and pinned navigation. The database owner applied
`041_screening_snapshot_type` manually through a one-time helper, since removed;
the normal application role then published the bounded generation. No elevated
runtime role, continuous worker, provider request or backfill was introduced.

- Source session: 2026-09-11; source cutoff: 2026-09-11T20:35:11.575524+00:00.
- Screening snapshot ID: `7d0f13fc-b584-5193-98aa-1b49a8899af1`.
- Generation: `ddd811949c09099d9c4ff3dc1d6e4e1be7aa398693378404777cd62505e8fc1b`.
- Expected and eligible cohort: 386 common stocks, ETFs and ETVs. No quota or
	confluence ranking is applied. Five candle detectors are evaluable for all 386;
	this is coverage, not 386 occurrences of each pattern.
- Field coverage: price/volume/change 386; prior dollar volume/relative volume/
	EMA20/realized volatility/prior-range distance 383; EMA50 382; SMA200 364;
	12-1 momentum and its full-population percentile 361. A price-only screen includes
	all valid short-history stocks; requiring momentum produces 25 UNKNOWNs.
- Explicit preparation: 34.2 seconds, 4,575,264-byte generation including shared
	lineage. No second price store or per-screen fact duplication.
- Final direct-reader measurement: cold 246 ms; warm p95 1.74 ms over 40 queries.
	Four-generation LRU limit; no server-side per-predicate result cache.
- Browser HTTP measurement through Vite: warm p95 83.2 ms over 20 queries;
	100-row wire response 160,024 bytes (156.3 KiB), uncompressed. Python's
	whitespace-expanded JSON diagnostic was 172,484 bytes. Both meet the 250 KiB
	default-response target. Shell/global metadata calls are excluded from these
	screening-only timings; preparation is measured separately.

### Functional And Isolation Gates

- 91 focused backend tests pass, including screening, existing discovery/alerts,
	stock idea engine/replay, strategy v2 and portal snapshot readers/worker tests.
	Existing FastAPI deprecation warnings remain unrelated to this change.
- 36 frontend Node tests pass across screening definitions, alert navigation and
	research evidence contracts. TypeScript and the production Vite bundle pass.
- The stored generation digest, complete unique membership, field coverage,
	total/UNKNOWN accounting and exact source dates verify successfully.
- Repeating publication returns the same snapshot ID. A simulated interruption
	after insertion but before pointer publication rolls back completely. Existing
	snapshot IDs/content hashes and current-pointer identities remain unchanged.
	This optional transaction gate is `--check-publication`; the verifier's default
	mode is read-only. The successful retry updates only screening pointer timing.
- The frozen independent stock-idea study's recorded source hashes remain intact.
	Existing alert/discovery services, frozen inputs/results, research navigation,
	and the normal daily worker code were not changed by this implementation.
- Reader ASGI tests prohibit bar/evidence queries and writes, and replace detector
	and preparation entry points with failures. Missing generations return 404;
	unsupported predicates return 422; no current-generation fallback is used.
- Browser checks confirm typing sends zero screening queries, invalid ranges
	disable Apply, Clear changes only the draft, and Apply updates chips/results
	together. Price >= 20 produced 369 matches; price >= 30 produced 353. Zero-result
	and 25-UNKNOWN momentum cases were exercised. Stale-state signaling is covered
	at the API boundary; no stale historical generation was fabricated for UI testing.

### Browser Library And Layout

`alphascreener.screening.workspace.v1` holds up to 100 definitions, pin order and
the active ID, subject to a 256 KiB serialized limit. Definitions contain no
generation/date pin, rows, outcomes or API cache. Sort and ordered columns persist
per screen without saving draft predicates or advancing predicate revision.
`alphascreener.screening.draft.v1` is separate reload recovery. Save validates and
persists the draft, then makes it the applied rule; Apply alone never saves it.

Imports are bounded to 256 KiB and 100 definitions, strictly validate allowlisted
fields/types/directions/schema, reject duplicate IDs inside a file and verify
predicate hashes. Conflicting library IDs require explicit replace or create-copy.
Exports are compact JSON so a valid local library remains within its import budget.
CSV export is explicitly the displayed page, with source session/generation IDs
and spreadsheet-formula escaping; it is not a saved definition.

Verified in the browser:

- Save/revision changes; rename, duplicate and column reordering without changing
	the rule revision; pin/unpin without queries; six pins limited to four desktop
	shortcuts and a More menu, with an active-screen picker on mobile.
- Unsaved $30 draft survives reload while the saved $20 rule stays unchanged.
	Save/Discard/Cancel protects screen switching, internal links and SPA Back;
	browser reload uses the native unsaved-change prompt plus separate recovery.
- Invalid import messages, explicit conflict-copy import, cancellation of delete,
	confirmed local-only deletion, and cleanup of all temporary verification screens.
- An incompatible local workspace remains byte-for-byte untouched and disables
	saving, while the unsaved read-only screener continues to show published results.
	Storage write failures are also covered by model tests and remain visible.
- Light/dark themes at 1280/1440 desktop and 390 mobile, no body-width overflow,
	bounded filter drawer with focus inside, accessible native dialogs, expanded
	source-linked match explanations and persisted column order.

API and frontend are available at `127.0.0.1:8001` and `127.0.0.1:5174`. Final
process inspection found only these reader/dev-server trees, not paused equity,
discovery or paper workers. No production worker or scheduler was activated.

### Remaining Gates

At this earlier checkpoint only one screening session was retained and comparison
was disabled. The six-session repair and optional New feature above supersede
that status; the full four-view comparison remains deferred. No initial New events
or Dropped UNKNOWNs are invented. Five-session persistence and retained active-setup lifecycle filters
remain disabled, as do the uninventoried fields listed above. Historical
reconstruction and an ongoing producer require their documented scope/approval;
the implementation does not restart workers to fill these gaps. No predictive
qualification, win probability, recommendation plan or alert record is created.

## Default Screener Library: September 13, 2026

The seven former daily preset names now appear in the searchable **Default
screeners** section, separate from browser-saved definitions. This is a frontend
recipe addition only: no projection rebuild, schema change, provider call, backfill
or worker restart was performed.

| Default | Availability And Definition |
|---|---|
| All eligible | Available over the current field-level CS/ETF/ETV universe, with no extra filters |
| Long interest | Available as an explicitly adapted recipe: price >= $5, prior 20-session average dollar volume >= $20M, full-universe momentum percentile >= 90%, 12-1 return >= 0.01%; strongest momentum first |
| Bearish risk | Available as an explicitly adapted recipe: same price/activity filters, percentile <= 10%, 12-1 return <= -0.01%; weakest momentum first |
| Pullbacks | Unavailable until original PULLBACK state facts are published |
| Bounces | Unavailable until original BOUNCE state facts are published |
| Resuming up / Resuming down | Unavailable until original RESUMING_UP / RESUMING_DOWN facts are published |

The momentum recipes are not exact replicas of the former discovery cohorts:
the former service applied global history eligibility, inclusive median dollar
volume, split-adjusted momentum and rank buckets inside that eligible population.
The new recipes use the documented field-level history/action gates, prior-window
arithmetic average dollar volume and the pre-filter field-eligible percentile.
Their explicit +/-0.01% thresholds preserve direction using the v1 inclusive
range contract without calling zero momentum directional interest. The library
labels them as current daily recipes, not the former discovery cohort.

The existing **Liquid pullbacks** and **Above prior range** recipes remain under
**More daily recipes**. Liquid pullbacks is explicitly an EMA-distance recipe,
not a substitute for the original multi-condition PULLBACK state. Unavailable
defaults have disabled controls and a visible source-capability reason.

Selecting a built-in applies its rules, relevant columns, default sort and
LATEST_COMPLETE mode without creating a saved definition or pin. Untouched
built-ins can be browsed without Save/Discard prompts; actual filter edits still
receive protection. The selected built-in ID and unsaved draft use the separate
recovery record, so reload restores the selected recipe without replacing a saved
screen. Save creates a user-owned copy; built-ins are not renamed or deleted.

Verification: 39 frontend tests, TypeScript and production build pass. Browser
checks covered all seven names, capability gating/search, zero screening requests
while opening/searching the library, preset columns/sort, direct switching,
reload recovery, dirty-edit prompts, saving a separate copy and its safe deletion,
and light/dark desktop/mobile layout with no page or row overflow. On the unchanged
September 11 generation, the adapted Long interest and Bearish risk recipes each
returned 37 matches. No test definitions remain in the browser library.

## Screen Tabs And State Support: September 13, 2026

This user-requested extension supersedes the preceding unavailable-state list.
It does not authorize ongoing workers, wider historical reconstruction, new
market-data downloads, research retirement or lifecycle qualification.

### Presentation And Selection

- The active-screen name is the **Choose screener** control. The persistent header
	Save/Save as/Library row is removed. Filter actions contain Apply, Clear, Save
	and Save as; unsaved-change confirmation offers Save as as well as Save,
	Discard and Cancel. Apply still does not silently save a definition.
- The screen library is a searchable list with All screens / My screens /
	Built-ins selection and a separate selected-screen details pane. Previewing
	shows exact rules, sort and time mode without making a screening query.
	**Open tab** adds the selected saved or built-in screen; **Go to tab** activates
	an existing one without duplication.
- Local workspace records now additionally retain `tabs` and `active_tab`.
	Old records without these properties migrate existing saved pins/active IDs
	without changing saved predicates, IDs, timestamps or revisions. Keys distinguish
	`saved:<uuid>` and `builtin:<id>`; invalid keys are rejected. Definition exports
	still contain only portable rules/layouts, not open tabs or result rows.
- Tabs have close controls, bounded desktop overflow, a mobile active-tab picker,
	and arrow/Home/End keyboard activation. Closing an edited active tab receives
	the same dirty-change protection. Closing a saved tab removes its shortcut/pin,
	not the saved definition. It remains available to reopen from My screens.
- Only the active tab has a query observer. Opening the picker, searching,
	previewing and pin ordering do not evaluate hidden tabs. A four-state-tab reload
	was verified to issue exactly one screening query. Saving a rule creates or
	activates its saved-screen tab; built-ins remain unchanged templates.

### State Calculation And Publication

The builder reuses `equity.stock_discovery.stock_features` unchanged for
`discovery_state` and `discovery_trend`. It does not copy or reinterpret the
detector in the frontend or call the legacy screener endpoint. It supplies exactly
253 dated canonical bars with same-security identity, expected exchange-session
continuity, valid OHLCV and no unresolved known corporate action within that
lookback. The state calculation's source code hash is stored in the new manifest.

This conservative 253-session window preserves the original EMA20 seed and
the full existing state rules. Trend is the original close/SMA50 quadrant with
SMA50 slope versus ten sessions earlier. Pullback/Bounce use the original recent
five-session close move in the corresponding trend; Resuming up/down retain the
original prior-bar high/low, EMA20 and previous pullback/bounce tests. The original
price and inclusive median-dollar-volume eligibility still applies to these two
fields. Failed eligibility yields UNKNOWN for the state fields, not a fabricated
neutral state and not global rejection by simpler price/volume screens.

The screening lookback keeps its existing unadjusted, known-action-gated basis:
an action-crossing path is UNKNOWN rather than silently borrowing differently
adjusted legacy facts. These are completed-session observations only, not fresh
alerts, persistent episodes, invalidation histories or proven predictive edges.

The same retained source publication/cutoff was used for one new generation:

- Source session: 2026-09-11; cutoff: 2026-09-11T20:35:11.575524+00:00.
- New snapshot: `51a64839-1005-54e9-9164-5c7980f6b1b9`.
- New generation: `c8f22be310fdeeb1cb30783b05a677160fa8630251130f95e66b1d1ac7b6f1e4`.
- State/trend coverage: 361 of 386; 25 UNKNOWN. All 386 retain basic eligibility.
- Preset matches: Pullbacks 83, Bounces 38, Resuming up 27, Resuming down 6.
- Preparation: 49.3 seconds; full payload including lineage 4,599,453 bytes.
- Direct reader: cold 215 ms, warm p95 1.69 ms; default 100-row diagnostic JSON
	179,009 bytes (174.8 KiB), within the original performance budgets.

The older generation remains addressable and its digest still verifies. Every
pre-existing field value, candle observation, selected-bar identity and shared
lineage entry agrees between the two generations. Querying new state fields on
the old generation returns a clear 422 rather than joining current facts; old
price-only reads still return 386. Catalog metadata advertises state fields only
after a publication with those capabilities exists.

### Verification And Boundaries

96 focused backend tests and 41 frontend tests pass; TypeScript and the production
bundle pass. Four explicit fixtures compare every state to the original function;
short history, low liquidity, identity/action safety and old-publication rejection
remain covered. Reader tests forbid both the candle detector and reused state
calculator, preparation work, raw-bar/evidence queries and data writes.

Browser checks cover state counts and UNKNOWNs, preview-without-query behavior,
saved/built-in tabs, reopen without duplication, reload with one active request,
closing without deletion, keyboard activation, Save as from the filter panel,
dirty-tab Cancel/Discard behavior, preservation of the last saved rule, and the
presence of Save as in dirty confirmation. Light/dark 390px mobile and
1280/1440px desktop layouts have no page-width overflow; filter save controls and
table paging fit the normal desktop viewport. The mobile save modal receives
focus above the filter drawer. Temporary verification definitions were removed.

Publication retry and simulated interrupted insert-before-pointer rollback still
pass. Frozen independent-study source hashes are unchanged, and the retained
August 18 REPLAY TRADE alert view still returns 42 alerts. Only the API reader was
restarted to load the new catalog; Vite stayed running and process inspection
confirmed no equity, discovery, paper, or research worker was restarted. No new
migration was required. Five-session persistence, membership deltas, richer
lifecycle setups and the other uninventoried field families remain deferred.

## Navigation And Column Presentation Follow-Up

The September 13 UI refinement consolidates the unfiltered view as **All equities**.
The former `builtin:all` / All eligible choice maps to the permanent base tab rather
than creating a second tab. Existing browser tab records and recovered built-in
selection are normalized; user-saved definitions, including similarly named ones,
are not deleted or renamed.

Screen library selection and open tabs now share the same desktop navigation row.
The compact mobile screen selector remains beside the library control. The daily
session dropdown has been removed and UI queries explicitly use the latest complete
generation. The source session remains visible in the page header and result footer;
the backend still retains both immutable generations and historical audit reads.

Column selection is retained for inspecting relevant stored values without changing
screen rules. It follows the existing Stock Alerts presentation: a named column
preset dropdown alongside the shared checkbox picker. Presets provide Screen default,
Overview, Trend, Momentum and Liquidity layouts. Individual changes display **Custom
columns**; Stock remains locked. Optional **Column order** controls are inside the
picker instead of taking a page row. Named presets and checkbox/order changes update
only the local display choices; saved-screen layout persistence remains separate
from predicate saving.

The available fields differ deliberately by page: Stock Screener displays completed
daily stock facts (price, trend, momentum, activity and supported observations),
while Stock Alerts displays retained plan/trigger facts and paper outcomes. The
screener does not invent alert stops, targets, confidence or P/L for arbitrary matches.

Verification: 43 frontend tests, TypeScript and the production bundle pass. Browser
checks confirmed one All equities tab after repeated library selection and reload,
the data-date status without a session selector, picker/tabs/tools on one row at
1280/1440px, and a contained column menu at 390px in both themes. Trend/Momentum/
Liquidity layout changes kept the 83 Pullbacks matches and serialized rule unchanged
with zero screening queries. Checkbox customization, stock-column locking, column
ordering and reset were exercised. Stock Alerts retains its own presets and has no
screener-only ordering section. No backend source, publication, worker or data was
changed for this refinement.

## Explicit Apply And Simpler Library

The next September 13 refinement makes the filters toggle the first toolbar button,
before Screen library. The panel/drawer starts closed on both desktop and mobile,
including reload, and opens only when selected. Switching viewport size does not
automatically open it.

Apply now opens an explicit **Apply filters** dialog instead of immediately issuing
a query. Its default is **Apply without saving**. **Save as a new screen** asks for a
name and creates a separate browser-local definition/tab. When a saved screen is
active, **Update saved screen** updates that definition and advances its predicate
revision only if the rules changed. Save choices confirm with **Save and apply**;
Apply without saving confirms with **Apply**. Opening/Cancel do not change results
or saved definitions, and Cancel keeps the edited draft. A successful confirmation
closes the filters. The filter footer contains only Apply and Clear. The former
direct Save/Save as shortcuts and their unused intent handlers have been removed;
saving filter edits is handled inside Apply confirmation. Saving remains explicit,
never an automatic side effect of merely opening the dialog. Separate library
rename/duplicate actions and dirty-navigation protection remain unchanged.

The library now has only **Built-in screens** and **My saved screens**. The former
All screens collection was their union and looked identical to Built-ins with an
empty saved library. The redundant New screen button is removed: All equities and
the filter editor already provide the starting point for a new definition.

Import/export definition icons remain, with tooltips naming saved screen rules,
columns and JSON. Export writes portable saved definitions (including names,
versions, predicates, sort and columns), not stock rows, outcomes, open tabs or
market data. Import validates JSON and adds definitions to this browser's library;
ID conflicts require explicit copy/replace choices. This supports backup and manual
transfer between browser stores, not account synchronization or data ingestion.
The table's CSV export remains a separate snapshot of displayed result rows.

Verification: TypeScript, 43 frontend tests and the production bundle pass. Browser
checks confirmed default-closed filters and first-button ordering; no query/write
when opening Apply; cancellation preserving the draft; Apply-only changing an
83-match Pullbacks view to 81 matches for price >= $20 without saving; creation of
a new definition at revision 1; updating the same ID to price >= $30 at revision 2;
and saving a separate price >= $40 copy without changing the original. Library
collection switching makes no screening query, and New screen is absent. Mobile
light/dark Apply dialogs fit at 390px and receive focus above the filter drawer.
Temporary verification definitions were removed. No backend data or workers changed.

## New Screen Creation Shortcut

The approved follow-up restores **New screen** only inside **My saved screens**,
including its empty state. It is absent from Built-in screens. This supersedes the
earlier blanket removal: the action is now a guarded creation workflow, not just
navigation to the base tab.

On New screen, any unsaved edits receive the existing Save/Discard/Cancel guard.
Only after that succeeds does the app switch to All equities, clear draft/applied
filters and filter search, reset sort/pagination, restore default columns, close
the library and open the desktop filter panel or mobile drawer. The reset is
explicit even if All equities is already active. Existing saved definitions and
tabs are retained; no blank definition is created by starting this workflow.

Apply then provides the existing saving choices. Save as a new screen creates a
named definition and tab. Apply without saving updates the current results without
creating a definition. Save in the pre-reset dirty guard persists current edits
first, then starts the fresh editor; Cancel changes neither rules nor saved data.

Verification: TypeScript, 43 frontend tests and production build pass. Browser
checks covered category-only/empty-state availability, reset from a built-in and
from an already-active All equities view with custom columns/sort/page, no saved
entry on start, Cancel preserving workspace/draft bytes and issuing no query,
Discard starting clean, Save preserving the current screen ID and revision update
before reset, Apply saving a named tab, and Apply-only leaving saved definitions
unchanged. At 390px the New screen button fits and opens a focused, non-overflowing
filter drawer. The temporary verification screen was deleted. No backend,
source-data or worker changes were made.

## Fixed Daily Table Columns

Every screener table layout now starts with **Stock, Close, Session change, Volume**,
in that order, after the explanation toggle. These four base
columns are checked/locked in the picker and excluded from optional reordering.
Overview shows only this base; other presets and built-in screens append relevant
columns afterward, with duplicates removed. Close displays USD currency values
under the short **Close** heading. Volume is the completed session's share volume,
not relative volume; relative volume remains an optional activity field.

Candle occurrences is optional, unchecked by default, and can be added/removed in
the picker. Custom screen layouts can retain it. Instrument type identifies common
stocks, ETFs and ETVs; it is also optional. Explicit existing user choices
of these fields remain respected. Loading older column selections adds/reorders only
the mandatory display prefix and preserves the optional selection order. Saved
rules, predicate hashes and revisions are unchanged by layout normalization; CSV
export uses the same ordered columns displayed by the table.

Verification: 48 frontend tests, TypeScript and the production bundle pass. Browser
checks covered every column preset, locked base fields, optional instrument type,
optional-only ordering, and saved-layout reload without changes to screen identity,
predicate or revision. All layout changes retained 83 Pullbacks matches and issued
no screening queries. Desktop 1280/1440px and mobile 390px views fit without body
overflow; wide optional layouts scroll inside the table. The temporary verification
screen was removed. Backend calculations, source data and workers were untouched.

The subsequent preset investigation confirmed that immediate Trend/Momentum/
Liquidity selection added the correct columns, but leaving and returning to a
built-in tab reset them to Screen default. Built-in column preferences now persist
in the bounded local workspace's optional `builtin_columns` map (keyed by built-in
ID, including `all`). The map validates known IDs and column allowlists and remains
separate from saved definitions, rule hashes, and portable definition exports.
Tab activation, reload recovery, and discarding a recovered filter draft reuse the
stored layout. The New screen workflow still explicitly restores default columns.

Browser regression: Pullbacks retained Liquidity while Bounces retained Momentum;
returning to Pullbacks and reloading both kept Liquidity instead of reverting to
Screen default. Optional candles could be checked and unchecked, and all preset
changes preserved match counts/rules with zero query requests. Final review view
is Pullbacks with Trend selected, showing its state/trend and EMA20/EMA50/SMA200
fields after the four base columns, with Candle occurrences hidden.

## Rule-Constant Column Cleanup

The subsequent built-in review removes redundant **Daily discovery state** and
**Daily discovery trend** columns from defaults and contextual presets for
Pullbacks, Bounces, Resuming up and Resuming down. Each fixes its state; the existing
state calculation also implies Up for Pullbacks/Resuming up and Down for
Bounces/Resuming down. The applied-state chip and match explanation retain the
filter evidence, and the column selector still permits explicitly adding either
field back.

The other built-ins use numeric ranges, not constant-value rules: their momentum,
percentile, liquidity, relative-volume and EMA/range-distance values remain useful
comparison columns. These remain unchanged. Display omission is derived from the
rule contract, never from equal values observed on one result page. Broadened or
opposing state selections do not imply a single trend; core daily columns remain
fixed even when a rule constrains them to an exact value.

Recognizable older built-in standard layouts are upgraded once using
`builtin_columns_version=2`. Nonstandard custom layouts and all user-saved screen
definitions are left unchanged. Subsequent manual re-additions persist and are
not removed on reload. Rule hashes, revisions, eligibility and result sorting are
unaffected; no backend detector or publication changed.

Verification: 52 frontend tests, TypeScript and the production bundle pass. Browser
checks confirmed the updated layouts for all four screens with unchanged counts
(83/38/27/6), state chips/explanations, retained varying Trend columns, and manual
re-addition surviving reload. Column changes made no screening queries. Desktop
1280/1440px and mobile 390px light/dark checks have no body overflow. Pullbacks is
left on Trend with the four base columns followed by EMA20/EMA50/SMA200 distances;
its redundant state/trend columns are absent. No test definitions were created.