# Stock Discovery And Alerts

## Multi-Model Upgrade Review

The proposed market/VIX, sector, event-risk and options-data context extension is
recorded separately in
[STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md](STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md).
It starts with source-qualified annotations and new incremental research, not
automatic gates, inherited confidence or a live-policy change. Existing Screener
implementation and completed alert/replay contracts remain unchanged.

Status: **ISOLATED ENGINE, INDEPENDENT EVALUATION AND PINNED PILOT INPUTS. NO LIVE CUTOVER.**
The daily `stock_discovery_v1` service described below remains unchanged. This
section records the reviewed multi-model, multi-timeframe contract. The first
backend implementation and its commands are documented below. The independent
pilot readiness/execution checkpoint below supersedes the initial synthetic-only
checkpoint; completed results must not be inferred from input readiness alone.
Existing research and paper records must not be purged or rewritten.

### Review 1: Runtime Contract

Use three independent trade-hypothesis lanes (Relative Trend Resumption, Range
Breakout Acceptance, Failed Extension Reversal), plus one visibly separate
Momentum Discovery watchlist lane. Daily v2 evidence does not qualify a new
intraday adaptation. Pattern Watch, persistence and higher-timeframe context are
attributes, not extra directional votes or additional quota-bearing models.

The following decisions resolve ambiguities in the brainstorming:

1. **Explicit identities.** Stock grouping uses security ID, not ticker alone.
  A model episode key includes policy/version, trigger interval, direction,
  holding-horizon class and a stable structural anchor. An updated bar or rounded
  boundary value cannot generate a new episode. Different model plans stay
  independent even when the UI groups them under one stock and direction. Merge
  execution only when reference episode, horizon and execution policy genuinely
  agree; otherwise visual grouping must not silently merge paper positions.
2. **Separate state dimensions.** Setup state is WATCH, TRIGGERED, INVALIDATED or
  EXPIRED; selection is ELIGIBLE, SELECTED or SUPPRESSED with reasons; data health
  is READY or STALE/UNAVAILABLE; paper position state is PENDING, OPEN, CLOSED,
  NO_FILL or UNRESOLVED. Conflicts are flags with provenance. A lost setup or stale
  mark does not invent a fill, close a position or change its frozen exit rules.
3. **Per-model cap.** Each of three trade lanes selects at most three fresh ideas
  across directions and trigger intervals COMBINED in each exchange-aligned
  30-minute publication window. The fourth lane selects at most three discovery
  watches separately. Zero is valid. Apply gates/conflicts and distinct-stock
  selection within each model before its top-three cutoff; then union/deduplicate
  selected ideas for display. Do NOT refill after cross-model deduplication in
  the first version: the trade-feed ceiling stays nine, and duplication reduces
  notifications instead of bringing lower-ranked replacements into the test.
4. **Persistent episodes.** Continuing matches update last-seen time and model
  attribution without new notifications or quota use. Persistence count is not
  confidence. Re-arm only after episode termination, two consecutive completed
  trigger-interval bars confirming the model's reset condition, and a later
  fresh trigger. Freeze each model's reset predicate before coding. Midnight,
  re-ranking, a process restart or a changed indicator alone does not re-arm.
5. **Freshness.** Candidate lifetime is one subsequent completed trigger bar,
  bounded by session close for intraday candidates. A suppressed candidate may
  be selected later only inside that original lifetime, with current entry gates
  still valid; its trigger time is never refreshed. Daily entries retain their
  separately declared next-session rule. No backlog is republished as new live
  alerts following an outage; retain MISSED_PUBLICATION coverage instead.
6. **Conflicts.** Opposing fresh models in the same holding-horizon class suppress
  new trade entry and remain visible in review. Lower/higher-timeframe opposition
  is COUNTERTREND context, not automatically a contradictory position. After
  entry, new opposition is a risk update, not an automatic reversal. Losing one
  model's support does not cancel another plan. Invalidation/data-risk updates
  bypass the new-idea quota and routine updates are coalesced.
7. **Immutable publication.** A publication has a stable market-window key and
  policy hash, fixed expected universe, actual decision deadline, input revision
  IDs and complete candidate dispositions. Late data cannot revise its winners;
  material late corrections append an audit update. Retries reuse the same
  committed publication. Selection plus notification outbox must be atomic so
  restarts cannot duplicate quota, notifications or paper positions.
8. **Direction is not confidence.** Agreement adds attribution only. Initial
  lexicographic priority: resumption uses direction-adjusted relative-strength
  rank, then less entry extension, then liquidity; acceptance uses less chase,
  then more valid target room/risk, then liquidity; failure uses more valid
  target room/risk, then less rejection extension, then liquidity. Ties use the
  security key. Discovery uses signed daily momentum then liquidity. All values
  must be observable at the publication deadline; no future entry fill is used
  to rank candidates. Actual-open gates may still reject a selected idea.

Intervals monitored are 30m, 1h, 1d, 1wk and 1mo. Daily eligibility and momentum
remain daily. Each interval's condition changes are evaluated only at its own
finalized boundaries. Higher-timeframe states refer to their latest COMPLETE
bar, never the final value of a still-forming week/month. Provisional watches can
be displayed separately but cannot satisfy confirmation gates or trigger paper
entries in the first version. Missing monthly warm-up stays unavailable rather
than silently becoming neutral or a 200-month entry requirement.

For the first executable comparison, preserve daily v2 contracts as daily arms;
define new versions for 30m/1h timing arms with explicit bar-based lookbacks,
EMA/ATR reset conventions, reference freezing and trading-session holding caps.
Weekly and monthly changes generate context/watch updates initially, not new
trade entries. This covers their behavior without pretending five years of data
can qualify many monthly strategy variants. Do not call the unchanged daily
detector with a different interval label and treat it as a tested new model.

The live worker's 60-second polling is not its signal frequency or feed latency.
Publication windows are anchored to XNYS session open, including DST and shortened
sessions. Intraday holding/expiry counts use expected exchange bars, never the
next N available database rows. The shortened final 1h bar must be explicitly
defined consistently with the canonical derivation before release.

### Review 2: Historical Contract

Proposed initial arm specification, to encode verbatim in the first versioned
configuration before replay (these defaults are not optimized or proven):

| Model | 30m / 1h Adaptation | Primary Paper Exit |
|---|---|---|
| Relative Trend Resumption | Prior completed daily RS63 top/bottom 30% and EMA50 slope context; prior 20 trigger-bar extreme; 1-3 prior-trigger-ATR retracement, 2-10 trigger-bar age; close through preceding high/low and trigger EMA20 | Frozen pullback stop / prior extreme target, else 240 trading minutes after entry or session close, whichever comes first |
| Range Breakout Acceptance | Prior 10 trigger-bar range <=4 prior ATR, preceding 5/20 true-range ratio <=0.75; fresh close 0.15 ATR beyond range and next completed trigger-bar acceptance | Frozen episode stop / range-width target, else 120 trading minutes after entry or session close |
| Failed Extension Reversal | Prior 20 trigger-bar range; extension >=0.50 prior ATR followed within 3 trigger bars by a directional close back inside by >=0.10 activation ATR | Frozen excursion stop / original midpoint target, else 60 trading minutes after entry or session close |
| Momentum Discovery | Daily 12-minus-1-month rank transitions; intraday state changes annotate the watch, not a new model entry | No trade-model position; any hypothetical daily watch outcomes stay separately labeled |

Intraday adaptations use prior completed trigger-bar ATR14 and 200 contiguous
trigger bars of warm-up. Last fully completed daily data supplies relative
strength/context; no unfinished daily or weekly close is substituted. Raw
intraday chronology, including overnight boundaries, is validated using the
exchange bar schedule. A data/identity break resets indicator and episode state.
Time caps count regular-session trading minutes from hypothetical entry and exit
at the first fixed execution-bar boundary reaching the cap; the session close
always overrides it. This is a deliberately new intraday contract, not daily v2
with its horizon silently reinterpreted. Each daily arm keeps its original
21/10/5-session plan; per-model quota pools that model's interval arms.

Entry must be strictly inside the frozen structural bracket with remaining target
room at least 1.0 times stop risk; acceptance additionally limits chase to one
activation ATR. This is an execution filter, not proof of positive expectancy.
Keep the existing v2 structural stop buffers and conservative gap/ambiguity
handling. Use no trailing, averaging down, automatic model switching or pyramiding.

Reset predicates after termination are also model-specific: resumption requires
two closes less than 0.50 activation ATR back from its frozen prior extreme, then
a new >=1 ATR retracement and fresh trigger; acceptance requires two closes inside
its frozen range before a new break; failure requires two closes back inside its
original range before a new extension. Count only consecutive valid completed
trigger bars. A truly different anchor may create a new episode only after the
model defines and records that anchor transition; price rounding cannot do it.

Pattern Watch uses only causally retained or replayed forming-boundary snapshots
available before the trigger. A later confirmed boundary cross is an attribute
of the matching model episode. Current Pattern Watch projections cannot be joined
back onto historical decisions. Persistence is measured from the episode's own
valid observations, not from the number of other scanners matching the ticker.

First-run budget: one frozen configuration per model/interval, cap three, no
agreement-weight tuning, and the four selection arms below. The independent
long/short results and full candidate audit are retained even if no trade passes.
Use 10 bps round-trip costs as the primary assumption and 4/25 bps as diagnostic
stress views; these do not replace missing bid/ask or short-borrow information.

Implement one pure candidate/lifecycle/selection pipeline used by live and replay
drivers. Historical replay injects a controlled clock and pinned inputs; it never
calls the live capture function with a backdated timestamp or writes replay ideas
into the forward alert feed. Existing daily and v2 backtests are baselines, not
results for the top-three combined feed.

- **Two clocks.** Pin actual observed/created revisions at a research source
  cutoff. For recent retained live observations, respect their actual availability.
  Old downloaded bars cannot be made historically observed by changing timestamps:
  reconstructed replay must explicitly model a minimum provider delay plus
  computation/publication latency, and report that limitation separately.
- **Publication cohort.** Freeze expected members before data arrives. Publish
  at a deterministic deadline after each 30m market boundary, admitting only
  visible complete data and recording missing members. Never select whichever
  workers happen to finish first. The exact delay/grace values must be fixed in
  configuration before inspecting returns.
- **Entry.** Use the first eligible execution-bar OPEN strictly after actual or
  simulated publication, not the already-finished trigger open/close. Choose one
  execution interval for the whole experiment. Prefer 1m/5m only where full path
  coverage supports it; otherwise explicitly use 30m execution and label the
  additional latency. Do not switch intervals opportunistically per winning trade.
  Daily-confirmed ideas may enter next session; intraday ideas unfilled at close
  expire rather than silently rolling overnight.
- **Exit and marks.** Freeze each model's entry/stop/target/time policy and record
  next-open rejection, non-fill, partial path and corporate-action uncertainty.
  No fill or target/stop sequence may be inferred inside a coarser bar than the
  experiment declares. Continue timestamped paper marks but freeze terminal exits.
- **Comparisons.** Run all qualifying candidates, model-priority top three,
  simple momentum top three, and a seeded random size-matched selection from the
  SAME eligible pre-selection candidates. All arms share timestamps, gates, fills,
  cost models, lifecycle rules and maximum active positions. Random controls may
  use multiple predeclared seeds and report their distribution, not the best seed.
- **Accounting.** Retain per-model plan outcomes, including suppressed candidates,
  separately from a deduplicated combined feed. Start with per-opportunity return
  and paired selection lift. Do not sum overlapping $1,000 scenarios into portfolio
  CAGR/drawdown. A combined portfolio requires a separately fixed capital, sizing,
  owner-plan and conflict policy before performance is claimed.
- **Inference.** Use every eligible decision window, including no-alert windows
  in activity/coverage denominators. Group or resample complete calendar blocks
  across stocks and rules to preserve dependence. Do not require both ticker
  samples to signal simultaneously. Separate long/short, model and trigger interval;
  count all predeclared model/priority/cost tests in multiplicity accounting.
  Report date/block coverage and confidence intervals; a large trade count is not
  a large independent sample. Intraday bars do not supply thousands of independent
  market regimes.

### Bounded Delivery Plan

1. Audit native 30m, derived 1h and completed daily/weekly/monthly history, dated
  memberships, identity continuity, action coverage and execution-bar availability
  for the requested dates. Distinguish long histories of CURRENT survivors from
  a complete point-in-time historical universe. No large download or data purge
  is authorized by this readiness check.
2. Freeze interval adapters, stable episode/reset rules, position policies,
  publication deadline, lexicographic priorities and the top-three quota. Add a
  replay plan/validate mode that checks inputs before any expensive detection.
3. Test event-prefix causality, future higher-timeframe isolation, delayed-bar
  entries, duplicate workers, missing bars, correction/outage recovery, quota
  conservation, cross-model conflict/attribution, and no forced alerts. Replay the
  same fixture with different worker counts/orders and require identical outputs.
4. Pilot: August 3-September 3, 2026, with preceding warm-up and subsequent exit
  coverage kept separate. Include synthetic DST/half-day/month-end fixtures;
  those boundary cases cannot be inferred from a single ordinary-month pilot.
5. Historical diagnostic windows: calendar 2025 and January 2-September 3, 2026;
  use full 2022-09-07..2026-09-03 history only for arms with verified interval and
  universe coverage. Rank over each full eligible date-universe before selecting
  the two frozen 300-name evaluation samples. A current-universe-only intraday
  replay is a survivorship-biased diagnostic, not replication of those samples.
6. Run the fixed arms once, examine coverage/execution first and returns second,
  then decide keep, revise with a new contract, or reject. Do not tune the quota,
  agreement weight, reset interval or winning date subset after seeing results
  without recording a new trial. All examined historical dates remain development
  evidence; fresh prospective observation is necessary before stronger claims.
7. Enable the new engine in forward shadow mode only after live/replay parity and
  persistence checks. Extend Stock Alerts with New/Active/Updates/Conflicts/Closed
  views, per-model attribution and explicit interval/observation times. Keep the
  old daily alert ledger readable. Review any data cleanup separately afterward.

The isolated pilot driver below now implements the first backend slice. It is
separate from the generic historical signal runner: `--scope all-adapters` still
does not backtest this proposed feed. Forward collection, feed activation and UI
changes remain outside this slice.

### Historical Readiness Findings

Read-only checks on September 13, 2026 established the following. The initial
whole-history count exceeded a 120-second statement limit and was cancelled;
the findings below come from narrower indexed queries with a 30-second limit.
No database data, worker, live configuration or strategy was changed.

For each of AAPL, MSFT and PNC, August 3-September 3, 2026 contains 312 distinct
30-minute RTH bars over 24 sessions, 24 daily bars, four completed weekly bars,
and one completed monthly bar. This verifies observed counts, not full OHLC,
identity, corporate-action, historical-availability or universe completeness.
There are duplicate revisions that must be pinned rather than counted as new bars.

Stored hourly bars for each probe total 158 over 23 sessions, ending September 2,
despite newer published watermarks. Five-minute history also stops September 2:
AAPL/MSFT have 1,794 distinct bars over 23 sessions; PNC has 1,793. Consequently,
do not assume a current publication proves complete historical storage. Use the
canonical 30m-to-1h derivation in an isolated replay input build, which already
retains a shortened final hour (15:30-16:00 ET on an ordinary session). Verify all
underlying windows; do not fill missing prices by interpolation.

| Historical Session | Dated Universe Members | Matching-ID Members With 13 Distinct 30m Bars |
|---|---:|---:|
| 2025-01-02 | 1,566 | 325 (20.8%) |
| 2026-01-02 | 1,746 | 339 (19.4%) |
| 2026-08-03 | 1,875 | 349 (18.6%) |

These three-date probes demonstrate incomplete point-in-time intraday coverage;
they are not a census of every missing date or security. Ranking only those with
intraday prices changes the universe and the top-three contest. Do not silently
replace the missing population with today's survivors or claim the original two
300-name samples have complete intraday replication.

The AAPL history probe has 16,255 distinct 30m bars spanning September 3, 2021 to
September 3, 2026. Its earliest stored observation is September 2, 2026, and 16,223
revision rows were observed more than one day after their market-bar end. The
monthly probe has 59 completed bars from October 2021 to August 2026, all observed
more than one day later. Their `LIVE_OBSERVED` origin label alone therefore does
not establish original historical availability. Simulated provider timing must
be separate from actual capture-time validation. A 200-month requirement cannot
be supported from these inputs.

**Recommended first implementation/test boundary:**

- Build all four lanes, episode transitions, model attribution and selection
  auditing, but keep weekly/monthly outputs as context/watch updates initially.
- Use 30m as the fixed execution-bar interval for the initial historical diagnostic;
  hourly triggers use verified 30m-derived inputs. A finer execution study is a
  separate arm after its coverage is validated, not a per-trade fallback.
- Proposed reconstructed timing contract: source bars available no earlier than
  bar end + 15 minutes, publication at end + 17 minutes (two minutes of processing
  allowance), next execution-bar open strictly after publication. Example: a
  10:00 ET completed 30m bar can publish at 10:17 and first enter at 10:30, not
  09:30 or 10:00. This is an explicit latency scenario, not measured historical
  network performance. Live operation uses actual availability and logs missed
  deadlines; late end-of-session intraday ideas cannot enter that same session.
- Run the short pilot first, then 2025 and 2026 diagnostic windows on a frozen
  coverage manifest. Label results `COVERED_UNIVERSE_RECONSTRUCTED_DIAGNOSTIC` and
  retain exclusions. These dates are deliberately familiar development history,
  not a new holdout. Extended 2022-2026 population claims stay blocked until a
  chunked per-session coverage audit and any separately approved backfill pass.
- Full-market daily ranks may use their verified historical universe, but the
  candidate pool's intraday-coverage restriction remains visible in every output.
  Do not apply the quota separately to ticker shards; merge all candidates first.
- No new source-data purchase, broad backfill, purge or live switch is implied.
  The first decision from the diagnostic is whether the selection/lifecycle layer
  works reproducibly and improves over matched simple selections on the covered
  population, not whether an intraday trading strategy is production-ready.

Two release gates follow from this double review: (1) functional/causal parity of
live and replay decision pipelines, and (2) data/coverage suitability for the
strength of the claimed result. Passing the first does not waive the second.

### Implemented Backend Slice

The new code is deliberately outside the existing equity package's eager runtime
initialization. Offline planning, validation and replay do not import the database
or load provider credentials:

- [stock_idea_engine.py](../backend/research/stock_idea_engine.py) owns immutable
  candidates, separate lifecycle/health/selection states, structural episode keys,
  reset confirmation, lexicographic priorities, pooled distinct-stock quotas,
  conflict provenance, display-only deduplication and an atomic local publication
  ledger/outbox. Independent model plans are not merged into one execution.
- [stock_idea_models.py](../backend/research/stock_idea_models.py) supplies the three
  separate intraday adapters, central daily v2 detector arms, daily momentum rank
  transitions and complete weekly/monthly context. It does not join current
  Pattern Watch projections into historical observations.
- [stock_idea_replay.py](../backend/research/stock_idea_replay.py) validates pinned
  inputs, derives complete hourly windows, drives every publication boundary and
  evaluates fixed-30m paper paths. Unknown entry/path/action states are retained;
  no fills or missing prices are interpolated.
- [stock_idea_pilot_config.json](stock_idea_pilot_config.json) freezes the reviewed
  model constants, 15-minute provider/17-minute publication clocks, cap three,
  independent plan ownership, 1,000 active-plan ceiling per comparison arm and
  random seeds 1729/2718/31415. The ceiling is a fixed experiment parameter, not
  a portfolio sizing policy. Resumption's extension tie-break is distance from
  trigger EMA20 in activation ATR units; acceptance/failure use distance from
  their frozen reference. Daily adjusted plans require a pinned same-session raw
  price scale before 30m execution can be evaluated.

The local SQLite ledger atomically commits state, winners, paper reservations and
notification outbox rows. It is not connected to the production alert feed and
does not send external notifications. Retries reuse the committed publication;
different retry inputs append correction audit evidence. Missed deadlines cannot
be republished as new alerts, and post-entry opposition remains a risk update.
Actual forward-worker hydration and activation are still release-gated; synthetic
parity is not a completed forward-market soak.

### Independent Evaluation Protocol

Study ID: `stock_idea_independent_evaluation_v1`. The machine-readable protocol is
[stock_idea_evaluation_plan.json](stock_idea_evaluation_plan.json); reporting lives
in [stock_idea_evaluation.py](../backend/research/stock_idea_evaluation.py).
It is independently versioned from the unchanged signal policy. Input freezes,
run manifests and evaluation artifacts carry the protocol hash and study ID.

Previous cross-sector/alpha studies cannot supply this study's confidence scores,
p-values, pass/fail labels or qualification IDs. Prior evidence remains readable
as research history, and verified prices, dated universes, calendars and daily
detectors can be reused. A new study identity does not make already examined dates
a new holdout. The current pilot and extended historical dates remain development
evidence; forward shadow validation is a separate release gate.

The primary policy is `PRIORITY` at 10 bps round trip. Each model, direction and
trigger interval has three predeclared one-sided claims: positive mean net return,
positive paired lift versus `MOMENTUM`, and positive paired lift versus the equal
mean of ALL three declared random seeds. The model's pooled interval contest is
also tested. This gives 3 models x 2 directions x 4 interval groups x 3 claims =
72 primary claims. All stay in one Holm family, including unestimable claims
assigned p=1 for adjustment. Four/25 bps and all-candidate comparisons are
diagnostics, not alternative primary tests chosen after results are seen.

The primary estimand is the mean complete publication-window return or paired
difference. Comparisons use the same publication and model/direction/interval cell;
different stocks need not signal together. Each arm evolves under its own selected
positions and the shared rules, so this compares sequential selection policies,
not a causal estimate of an isolated ranking score. Missing, empty and immature
windows remain counted in activity/coverage; they do not become zero-return trades.
Random comparison requires every declared seed, not the best available seed.

| Gate | Frozen Rule |
|---|---|
| Publication integrity | Every expected arm/window; exact deadline; consistent source candidate/cohort sets; valid dispositions; pooled distinct-stock cap; no repeated selected episode, display refill, early entry or no-fill costs |
| Outcome coverage | At least 95% resolved outcomes among mature selected opportunities in EACH required comparison arm; all missing position records block approval |
| Maturity | Use the plan's maximum scheduled exit at the source cutoff, independent of whether a profitable stop/target exit happened earlier; report all immature positions separately |
| Independent time coverage | At least 40 populated complete calendar blocks for each required claim; this is a guardrail, not a claim of statistical independence or sufficient power |
| Inference | Resample complete calendar blocks shared across stocks/windows, 10,000 fixed-seed draws; one-sided centered-null bootstrap p-values; positive marginal 95% interval lower bound AND Holm-adjusted p <= .05 |
| Block length | Intraday: 5 sessions. Daily and pooled: resumption 21, acceptance 10, failure 5 sessions; report 2x/3x length sensitivity and withhold a pass if an estimable sensitivity loses the positive interval |
| Missing-path stress | Assign unresolved mature paths minus two initial stop risks and full costs; use signal-price risk only when actual entry is unknown. This is an adverse scenario, NOT a guaranteed loss bound, especially for gaps/shorts |
| Concentration | Positive estimates after removing the best-contributing security without refill and after removing the best calendar block; no claim that this replaces corporate-event review |
| Release | `PASS_RESEARCH_ONLY` cannot activate live alerts, create canonical qualifications, certify borrow/spreads/actions, or claim full-market coverage |

The 24-session pilot supplies at most four complete five-session blocks, two
10-session blocks, or one 21-session block. It CANNOT statistically pass a model,
regardless of how many correlated trades it generates. Its purpose is to check
functional/causal behavior, coverage and execution, then identify which fixed
hypotheses merit additional history. Underpowered cells remain
`INSUFFICIENT_INDEPENDENT_BLOCKS`; low-coverage cells remain
`INSUFFICIENT_OUTCOME_COVERAGE`, not zeros or statistical rejections.

Approval is scoped to the exact model/direction/interval and pooled policy. A
successful sub-arm cannot inherit approval for its siblings. Dropping failed
sub-arms changes the top-three contest and requires a new declared policy trial.
Within-candidate momentum/random controls test selection value, not whether the
pattern detector itself has alpha against comparable nonsignaling opportunities.
Momentum Discovery remains a context/watch-quality evaluation, never a trade-model
performance pass. No old alpha or cross-sector evidence is joined into this report.

### Pilot Commands

[run_stock_idea_replay.py](../backend/scripts/run_stock_idea_replay.py) defaults to
plan-only mode. It reports 312 expected publication windows over 24 XNYS sessions:

```powershell
& .\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_replay.py --plan
```

Input freezing is opt-in and performs only bounded, repeatable-read, read-only
database queries with a 30-second statement limit per query. It does not download
or backfill anything. The following cutoff is an example; fix the intended source
vintage before freezing, and use a new artifact path for a different vintage:

```powershell
& .\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_replay.py --freeze-inputs --source-cutoff 2026-09-13T00:00:00Z --inputs .\backend\backups\stock-ideas-pilot-v1\inputs.json
& .\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_replay.py --validate --inputs .\backend\backups\stock-ideas-pilot-v1\inputs.json
& .\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_replay.py --replay --inputs .\backend\backups\stock-ideas-pilot-v1\inputs.json --output .\backend\backups\stock-ideas-pilot-v1\replay --workers 4
```

The default covered cohort is frozen from the first dated universe and identities
with 200 contiguous derived hourly bars before the pilot opens. `--cohort` can
instead restrict that contest using an explicit JSON array of security IDs.
Missing subsequent bars do not remove a stock from the expected cohort or create
a replacement winner. Daily ranks are computed over the pinned dated daily
population before covered-cohort filtering; missing/ineligible rank members are
reported separately. No current-universe or ticker-only fallback is used.

The input artifact contains checksummed `bars`, `memberships` and `actions`, the
source cutoff, study/protocol identity, fixed covered identities and action-coverage caveat. Bar records
retain security ID, ticker, interval, session, start/end, OHLCV, revision ID and
actual observed/created clocks. Membership records retain session, security ID,
ticker, universe run ID and observed/created clocks. Daily raw execution scale
evidence has its own revision IDs and source clocks. Invalid policy values or
unpinned revisions fail validation before detection; missing coverage is reported
as an exclusion or unresolved result rather than repaired implicitly.

Validation saves a separate immutable report beside the inputs (or at
`--validation-report`). Stored-valid and publication-deadline-visible coverage
are separate: a late stored revision is not proof of an on-time observation.
`--evaluation-plan` explicitly selects the new protocol; the default is the file
above, not an earlier research qualification.

Replay retains the input/config/code manifest, validation and daily-rank coverage,
all publications/dispositions, independent outcomes for observed candidates
(including suppressed plans), arm-specific selected outcomes and a summary. The
summary includes all-candidate, priority, momentum and all three random arms;
long/short/model/interval/cost cells; paired window-level selection lift; random
seed distribution; and horizon-aware block intervals when estimable. Empty windows remain in
activity denominators. All examined cells are counted, without a qualification
claim or selecting the best seed. The separate `evaluation.json` retains all 72
primary claims, direct control comparisons, independent research decisions and
Holm adjustment. Overlapping opportunity returns are never
converted into portfolio CAGR or drawdown.

Warm-up, pilot and exit coverage are separate. The longest daily arm's September 3
signal can require prices through October 5, 2026. A September 13 source vintage
cannot supply that future exit; pending or incomplete paths remain explicit.
Known corporate actions are checked, but independent action completeness,
bid/ask execution, borrow and full-market coverage are not certified.

Focused validation at this checkpoint: 39 tests pass across the new engine/replay
tests and existing daily v2/discovery suites. Fixtures cover exchange DST/half-day
and month boundaries, causal prefixes, unavailable context, delayed entries,
missing paths, conservative ambiguity, distinct-stock quotas, cross-model
deduplication without refill, concurrent retries, rollback, corrections, outages,
post-entry risk updates and byte-identical one/four-worker replay artifacts.
That was the initial implementation checkpoint. The independent evaluation
extension now has 50 passing focused tests, including direct momentum/random
comparisons, fixed maturity, full-family multiplicity, stored-versus-visible
coverage, immutable lineage/catalog snapshots, offline rank readiness, rejection
of inherited qualifications, and a positive synthetic research-pass control.
No source data, frozen study, worker, API or
live feed was changed.

### Independent Pilot Readiness: September 13

The read-only source freeze completed at the fixed source cutoff
`2026-09-13T09:29:27.225526+00:00`. The new local artifact is
[inputs.json](../backend/backups/stock-idea-independent-v1/inputs.json), with
semantic SHA-256 `e48da874e8822717574dd41e4929ee29ea71d4c4f53125876353aa2a1963a70d`.
It binds the existing signal policy to the new independent evaluation protocol.
No provider requests, broad backfill, canonical writes or old-study mutations were
performed.

Readiness evidence:
[deadline validation](../backend/backups/stock-idea-independent-v1/inputs.deadline-validation.json)
and [daily rank readiness](../backend/backups/stock-idea-independent-v1/inputs.rank-readiness.json).

- Pinned inputs: 975,326 bar revisions, 108,642 dated memberships and 6,510 known
  action records. Integrity validation found no invalid OHLC/clock exclusions or
  source-cutoff errors. Known-action coverage remains uncertified.
- Frozen contest: 348 of the first session's 1,875 dated members, selected using
  pre-pilot contiguous hourly warm-up. All 348 remain dated members on each pilot
  session. This is about 18.6% of that initial population, not a market-wide test.
- All 108,576 expected covered-security 30m windows are stored with valid exchange
  clocks. Only 100,572 (92.63%) are visible by the frozen publication deadlines.
  These figures measure input visibility, not mature-outcome coverage; the 95%
  outcome-resolution gate must not be confused with this denominator.
- August 3-September 1: all covered native windows meet the reconstructed timing
  scenario. September 2: only 1,044 of 4,524 windows meet actual availability.
  September 3: zero meet actual availability. These sessions stay in the pilot,
  with no relaxed deadline, interpolation, current-data fallback or date removal.
- Daily ranks use the full pinned dated daily population before covered-cohort
  filtering. August 3-September 1 ranks contain 1,770-1,819 eligible stocks, with
  daily context available for 341 of the 348 covered identities. September 2-3
  have no deadline-visible daily rank cohort. Rank readiness evaluates no returns.

The readiness decision is to permit the bounded functional/execution diagnostic
with those gaps explicitly retained, not to approve any trading model. The
24-session independent-block limit already prevents statistical approval.

The first real replay attempt was stopped during detection at approximately
35 GB resident memory to protect the existing daily services. Its incomplete
output directory is preserved. The cause was repeated JSON deep-copying of the
same immutable source-revision lineage into per-bar lifecycle snapshots. The
implementation now shares immutable revision tuples and retains only pilot
observations after computing required warm-up. Prefix nonmutation and serial/
parallel artifact parity tests pass. A retry uses a separate `replay-memoryfix`
directory with the same pinned inputs, signal policy and evaluation protocol;
the source-code hash records the execution fix. No returns from the interrupted
attempt were inspected or used to tune the research rules.

The memory-fixed attempt completed all 1,044 detector histories, but was stopped
after 28 of 1,872 expected arm/window publications: repeated full lifecycle
lineage in six SQLite arm states already occupied about 1.3 GB. The second
storage-only fix catalogs compressed immutable candidate records once and saves
content-hash references in lifecycle/position state. Active records are resolved
before decisions; selected positions and outbox writes remain atomic. Tests cover
catalog deduplication, exact hydration, rollback and worker-order parity. The
catalog retry uses a new `replay-catalog` directory with the SAME input/protocol
hashes. Both incomplete attempts remain preserved as execution diagnostics, not
performance studies. No old alpha evidence is used to compensate for these
implementation defects or the short independent-time sample.

After this pilot, existing 2025/2026 history may be considered in separately
frozen coverage manifests; no broader source-data retrieval is authorized by this
plan. Forty populated five-session blocks require at least 200 sessions, while
forty populated 21-session blocks require at least 840, before accounting for
sparse signals, maturity, missing paths or longer-block sensitivity. Those are
minimum calendar spans, not guarantees of independence, statistical power or a
pass. A stronger claim cannot be obtained by reusing earlier qualification IDs
or counting more tickers in the same market weeks.

### Completed Independent Pilot Evaluation

The `replay-catalog` run is complete. The saved
[evaluation](../backend/backups/stock-idea-independent-v1/replay-catalog/evaluation.json)
was recomputed from the saved publications and matched exactly. Runtime source
hashes, the signal policy and independent evaluation protocol also matched the
manifest. All 1,872 expected publications (312 windows x 6 arms) are present and
the implemented publication audit reports zero defects. Across all arms, entry
timestamps and entry gates, maximum exit times, signed-return arithmetic,
4/10/25 bps closed costs and zero-exposure/no-cost no-fills reconcile.
These are implementation/consistency checks, not independent verification of
vendor prices, corporate-action completeness or executable bid/ask fills.

Formal result: ALL 24 model/direction/interval groups are
`INSUFFICIENT_INDEPENDENT_BLOCKS`. None of the 72 primary claims is estimable under
the frozen 40-block gate. No model passed, and this is not a statistical rejection
of every model. No earlier qualifications were inherited or new canonical
qualifications published. Only one to four complete populated blocks are present.

Priority selection retained 1,175 independent trade plans and 48 separate discovery
watches. Trades appeared in 273 of 312 windows. There are 783 closed plans, 390
no-fills and two open plans in the source-vintage marks. The no-fills comprise
200 insufficient-target-room, 69 chase/boundary, 34 outside-bracket rejections and
87 cases with no eligible same-session entry open. Thus 33.2% of selected trade
plans never filled. Each counts as zero exposure/cost, not a lost trade.

For qualification maturity, 1,149 of 1,149 mature priority plans are resolved;
26 remain outside their maximum scheduled horizon cutoff. Some of those 26
already closed early or were no-fills, but the fixed maturity rule still excludes
them from the primary estimate to avoid selecting early outcomes. The complete
observed candidate census, including suppressed plans, separately contains 1,481
closed, 3,370 no-fill and five open hypothetical paths; do not confuse that census
with selected feed positions or sum its returns into portfolio performance.

The following are the primary window-weighted means on mature complete windows
at 10 bps round trip, pooled over trigger intervals. All values are BASIS POINTS,
not annualized or portfolio returns. Paired control lifts can use different
matched-window counts, so they are not differences between unconditional means.

| Model | Direction | Mean Net | Lift Vs Momentum | Lift Vs Random Mean |
|---|---|---:|---:|---:|
| Relative Trend Resumption | Long | -0.11 | +1.19 | -0.66 |
| Relative Trend Resumption | Short | -14.82 | +1.51 | +6.00 |
| Range Breakout Acceptance | Long | -13.35 | -3.99 | +2.31 |
| Range Breakout Acceptance | Short | -9.40 | +1.87 | -5.36 |
| Failed Extension Reversal | Long | -18.04 | -2.55 | -0.44 |
| Failed Extension Reversal | Short | +8.90 | +6.85 | +2.92 |

Ten of the 12 intraday model/direction/interval cells have negative primary net
means. The two positive ones are hourly short resumption (+14.77 bps, 41 selected
plans) and 30m long acceptance (+1.77 bps, 53 plans). Hourly short resumption falls
to -6.77 bps when its best calendar block is removed. The small positive acceptance
mean trails both momentum and random selection; it is not ranking evidence.

Daily short failed-extension reversal is the most promising follow-up diagnostic:
30 selected plans, 27 closed and three no-fills over 16 primary windows, mean net
+151.20 bps and paired lifts +77.72 bps versus momentum and +35.26 bps versus random.
Its mean stays positive after removing its best security or block, but the random
lift falls to about +1.13 bps without its best lift block. The pooled short-family
random lift becomes negative under that block-removal test. Largest daily-short
gains include AXTI, CRDO and SNDK; validate their retained price/identity/action paths
before interpreting the apparent edge. No production promotion or parameter tuning
is supported by this pilot. Any broader history or revised model mix is a separately
declared follow-up, not an automatic continuation of this run.

Replay and completion verifier processes have exited. The API, frontend servers,
daily discovery worker and paper watcher remain stopped at the user's request;
checking these results did not restart services or mutate the frozen artifacts.

## Pages

The agreed daily-first saved-screen workspace, browser-local screen library,
pinned screener tabs, publication architecture and phased implementation are
specified in [STOCK_SCREENER_WORKSPACE_DESIGN.md](STOCK_SCREENER_WORKSPACE_DESIGN.md).
That document is a future implementation plan, not a change to the current
Screener, daily worker or research/alert policies.

- Stock Screener and Stock Alerts are independent sidebar destinations and pages.
  Neither page contains a cross-page Screener/Alerts tab strip. Alerts owns only
  its Latest Run and Day History tabs. Customizable Overview/workspace tabs are
  future work and are not part of this implementation.
- `/stocks/screener`: the currently published tracked universe, ranked by transparent
  12-minus-1-month momentum. Filters retain each stock's original universe rank.
  Presets cover long interest, bearish risk, pullback/bounce watches and resumption.
  Search, sector, price, liquidity, relative-volume filters, sorting, pagination,
  column selection, CSV export and ticker links follow the existing terminal UI.
- `/stocks/alerts`: source-separated retained publications and plan history. Latest
  Run shows only newly selected plans from the selected session's latest committed
  publication, with a run selector under Run details for earlier publications. An empty run never
  falls back to the last nonempty one. Day History includes the selected session's
  earlier forward runs, excluding the source's newest committed publication until
  the next publication arrives. Frozen replay history includes all retained runs.
  It defaults to original trigger timestamp, newest first.

### Alerts Workspace Implementation

[StockAlertsPage.tsx](../frontend/src/pages/StockAlertsPage.tsx) uses the new
read-only `/api/stocks/alert-view` endpoint. The old `/api/stocks/alerts` endpoint
and daily worker contracts are preserved. Opening either page does not capture
signals, compute outcomes, request provider data or start workers.

The header offers exactly 21 XNYS sessions ending at the source's latest retained
session, with previous/next, direct date selection and return-to-latest controls.
Zero-alert and missing-publication sessions remain navigable. The navigation
limit is not a deletion or position-tracking policy. Explicit historical-record
links reset the date/run/filter selection and open Day History; a run cannot be
queried under a different session. Record types remain separate in the reader API.

The main workspace shows only Stock, Direction, Model, Trigger interval and Status
filters, the retained column preset/picker, export and refresh tools, and a matched
alert count (for example, `42 alerts`). Source and Lane dropdowns and the permanent
publication/status summary strip are removed. The compact header identifies
Backtested history, Forward shadow or Legacy daily history beside the date selector.
Run details starts collapsed and retains source/cutoff, study/policy identity,
publication counts, matched outcome counts, detailed research caveats and links to
legacy/backtested records. Latest Run's earlier-publication selector and coverage
diagnostics are also inside Run details. Only relevant incomplete-input/stale-data
notices remain above the table; plan-specific risk warnings stay in each row's details.

The main workspace requests trade alerts only. Each row remains an independent
model plan even though the count is labeled alerts. Discovery watches are preserved
in the retained data and API but are not mixed into this table; a dedicated watchlist
experience is future work. Old watch-only URL parameters cannot apply invisible
lane/model/status/direction filters. Long/Short query values are parsed as bounded
integers by the API; unsupported directions return 422 without changing stored data.

When no source was explicitly selected, Latest Run defaults to `SHADOW` and direct
Day History links default to the clearly labeled `REPLAY` source. Entering Day
History from an enrolled Latest Run keeps the normal forward date-navigation route,
but it is not a forward-only restriction. On that route, choosing a covered date
before forward enrollment displays the original retained backtest for that date.
The API response and page label identify it as `REPLAY` / Backtested history and
prices remain frozen at their original cutoff. The shared 21-session date window
still allows returning to today without manually changing sources. Explicit
REPLAY and LEGACY links remain source-specific; the Backtested history link also
opens the replay's own older 21-session window.

Real forward publications, including empty or incomplete ones, take precedence
over backtests. Dates on/after enrollment or outside retained replay coverage do
not substitute a backtest or the last nonempty day. The forward latest-run rollover
rule does not apply to frozen replay history: its final run is shown as-is without
waiting for a hypothetical next replay run. No source data, prices, outcomes or
qualification metadata are relabeled, recomputed or merged by this date routing.
The replay date selector exposes August 6-September 3, 2026. September 3 remains
an honest zero-alert session, and August 18 contains 42 trade plans. Six focused
navigation tests originally covered this default path; eight now include trade-only
filter handling. Browser checks cover record switching and
the enabled 21-session selector.

Stock and Direction are locked columns. Default columns include model/interval,
publication time, trigger price, original stop/target, trigger-price risk to stop,
reward/risk, maximum hold, hits and status. Latest Run has no paper-entry or P/L
columns, even under Show All. Day History adds the executable paper entry, latest
retained stock price/time and percentage paper P/L. Fixed exits can be selected as
an extra column or inspected in plan details. There is no assumed dollar notional
or summed portfolio performance in this workspace.

Rows remain independent model plans. Repeated same-stock plans do not silently
share stops, targets, entries or exits. Hits count distinct retained valid stock/
direction publication windows in the rolling 21-session period; simultaneous
models count once and opposed directions stay separate. Historical views stop
counting at the selected session, while a selected run stops at that run's
publication. Polls, retries, expired/stale and missed-publication observations do
not add hits. Model/interval attribution and first/last seen appear in details.
Replay recurrence is limited to valid retained candidate observations and cannot
claim unseen pre-pilot matches. Legacy recurrence is unavailable, not fabricated.

Column selection persists in browser storage. Trade Plan, Trend, Momentum and
Liquidity presets support manual inspection; sorting never changes the original
selected plans. The dropdown retains the matching preset name across reloads and
tab changes, showing Custom columns for a manually customized layout. Retained
replay indicators include activation ATR/ATR%, entry
extension, daily RS63 percentile, daily 12-minus-1 momentum and daily liquidity.
The offline projection additionally reconstructs causal trigger-bar EMA20/EMA50
distances, EMA50 10-bar slope, simple rolling RSI14, prior-20-bar relative volume
and nonannualized 21-bar return volatility from pinned inputs. Each row retains
indicator time/interval and revision provenance. Future prices cannot change these
fields. ADX, market cap, earnings and other unretained fields are not invented or
fetched on column selection. Legacy fields are shown only where originally retained.

Source handling is explicit:

- `SHADOW` is the API/Latest Run default, now backed by the separately enrolled
  multi-model forward worker described below.
  `STOCK_ALERT_SHADOW_VIEW` may point to a producer-owned `stock_alert_view_v1`
  snapshot with `source=SHADOW`; a replay snapshot is rejected under that source.
  The default is `backend/backups/equity-shadow/stock-ideas-forward-v1/alerts-view.json`.
  An enrolled Latest Run keeps the forward date-navigation route in Day History;
  covered pre-enrollment dates resolve to the retained REPLAY source and keep that
  identity in the response and page header. Explicit replay and legacy links retain
  their source. A waiting forward view shows the source-ready publication window.
- `REPLAY` reads the isolated
  [alerts-view.json](../backend/backups/stock-idea-independent-v1/alerts-view.json)
  projection, not the frozen ledger in place. It includes 1,175 trade plans and
  48 discovery watches across 312 publications; the main page displays only trade
  plans and the navigation window exposes only the last 21 sessions. Prices are explicitly labeled Price at cutoff and
  never refreshed with live data. Closed paper returns remain frozen.
- `LEGACY` reads existing daily captures/marks and recent canonical stock prices
  in a bounded read-only transaction. Closed paper P/L stays fixed; current stock
  price has its own time. Stale price/open-paper marks are flagged. Legacy alerts
  lack structural stops/targets and valid recurrence observations, so these show
  unavailable. No model risk score is inferred from the old qualification ledger.

The replay projection can be rebuilt to a NEW output outside the frozen run:

```powershell
& .\backend\.venv\Scripts\python.exe .\backend\scripts\prepare_stock_alert_view.py --replay .\backend\backups\stock-idea-independent-v1\replay-catalog --inputs .\backend\backups\stock-idea-independent-v1\inputs.json --output .\backend\backups\stock-idea-independent-v1\alerts-view-new.json
```

Set `STOCK_ALERT_REPLAY_VIEW` to use a nondefault projection location. The builder
verifies the pinned input hash, refuses to overwrite a file, and records input,
publication and builder hashes. Its generated view is about 3.8 MB. No existing
study, outcome, strategy policy or quota was changed to produce this page.

Validation: 66 focused backend tests pass, including actual HTTP direction parsing;
eight frontend navigation/filter tests and the frontend production build pass.
all three source endpoints return their expected ready/waiting states, and the
generated view retains 1,223 rows with unchanged frozen runtime/publication hashes.
Browser checks covered separate pages, empty latest runs, 21-session limits,
indicator-column persistence, locked direction, keyboard tabs, expanded plans,
the five simplified filters, collapsed Run details, legacy access, old watch links,
and desktop/mobile layout. API/frontend are running for user inspection; only the
reader API was restarted to load the direction parsing fix. Normal workers remain paused.

The initial September 11, 2026 snapshot has 386 tracked instruments and 375 eligible
ranked rows. This is the application's existing published universe, not every US
listing. It can include tracked ETFs; the page does not imply common-stock-only
membership. The initial snapshot is a baseline and generates no historical alerts.

## Legacy Ranking And Alerts

Policy version: `stock_discovery_v1`. This is discovery context, not a qualified
trading strategy or individual-stock probability. Old research detectors and
their results remain unchanged.

Eligibility requires 253 contiguous daily sessions, consistent security identity,
finite positive prices, nonnegative volume, price >= $5 and median 20-session
dollar volume >= $20 million. Known splits are adjusted from stored action terms
for feature computation; unsupported actions or invalid terms produce explicit
exclusions. Corporate-action coverage is not independently certified. Historical
inputs are read only through the actual capture time and the snapshot's session.

Momentum is close[t-21] / close[t-252] - 1. Strongest momentum receives rank 1,
with ticker tie-breaking. The top decile with positive momentum is `LONG_INTEREST`;
the bottom decile with negative momentum is `BEARISH_RISK`. These are review
categories, not instructions to buy or short. Trend is price above/below a rising/
falling SMA50; 5-session movement distinguishes retracement states. Resumption
requires a close beyond the prior high/low and EMA20. A range break uses the prior
20 completed sessions. No confluence score or grade is computed.

Alerts are changes from the previous captured session: entry into a leading/lagging
group, a new pullback/bounce watch or resumption, a fresh range break, and trend
invalidation. Multiple same-direction criteria produce one ticker/session alert;
opposed criteria produce a context-only conflict. Alert payloads retain the rank,
criteria, direction, price and prior-session context known at capture.

First capture establishes a baseline. Subsequent alert capture requires consecutive
XNYS daily snapshots and must occur before the next session's open. A missed
capture window does not generate backdated alerts. Opening a page does not capture
signals or mutate marks. Watches are not confirmed entries: their paper evaluation
answers what a hypothetical next-open position would have done, not whether the
watch itself recommended a trade.

## Legacy Paper Accounting

The user requested both automatic fixed-horizon outcomes and ongoing mark-to-market
tracking. The initial convention is $1,000 hypothetical entry notional per alert,
fractional fixed shares, next XNYS session open, and 10 bps round-trip cost. Open
paper P/L includes 5 bps entry cost; each closed horizon includes the full 10 bps.
Short returns are signed price-return proxies, without borrow availability/fees,
dividend obligations or financing. Cash interest, taxes and equity dividends are
not included. No brokerage orders or fill integrations are implemented.

An entry uses the earliest finalized stored bar starting at the exact session open.
A missing entry stays unavailable; it is never replaced with a later open. A valid
zero-volume entry is a no-fill with zero exposure and zero trading cost. Current
paper P/L uses the latest stored finalized RTH price after entry, respecting actual
observed/created timestamps. Closed results use the exact daily close at sessions
5, 10 and 21. Tracking for this policy ends at the 21-session close; the last mark
then remains the closed paper result rather than floating forever.

Known splits/mergers/symbol changes/spinoffs during an open paper path, identity
conflicts, missing completed sessions and non-trading paths remain unresolved until
adequate handling/data exists. The implementation does not fabricate settlement
or interpolate prices. Mark timestamps and stale flags remain visible. The market
feed may be delayed; a 60-second worker interval does not make it real-time data.

Current paper P/L is an unrealized simulation mark. Closed paper P/L is a simulated
completed outcome. **Actual realized P/L requires actual quantities, execution
fills, exit fills and fees from a trade ledger.** None of these pages claims that
the user actually traded an alert. Do not sum per-alert $1,000 scenarios into a
portfolio return: dates and symbols can overlap and capital allocation is undefined.

## Forward Worker Cutover: September 14, 2026

At the user's request, Equity/All in
[start_workers.ps1](../backend/scripts/start_workers.ps1) now launches
[run_stock_idea_worker.py](../backend/scripts/run_stock_idea_worker.py), not the
legacy daily discovery worker. The legacy process was already absent when the
targeted stop was checked. Other ingestion, corporate-action, options, calendar,
and portal snapshot workers were left running; no frozen study or old record was
rewritten. The reader API was restarted to load the new default shadow path.

The initial enrollment was `2026-09-14T14:50:39.185775+00:00`, with 386 securities
from the latest complete tracked daily publication. This fixed cohort includes
ETFs and is not the historical full-market ranking universe. Bootstrap establishes
state only; pre-enrollment signals are not published as new alerts. The worker
reuses shared model adapters, lifecycle, selection, plan and paper-exit logic.
It retains the three-per-model combined quota and display deduplication without
refill. No old statistical qualification transfers to `stock_ideas_forward_shadow_v1`.

Native 30m bars and completed daily bars are read from canonical storage in bounded,
read-only transactions, with both observed and created timestamps visible at the
actual read cutoff. Hours are derived from completed native slots. The initial
version used a fixed bar-end +17-minute cutoff; the approved source-ready revision
below supersedes that schedule for this enrolled worker. Late worker windows are recorded as
`MISSED_PUBLICATION`; late data is never relabeled as timely. Actual publication
time, not scheduled time, controls the next eligible paper entry. Half-days and
holidays use the exchange calendar.

Daily context refreshes every five minutes even without open positions, so a daily
bar arriving after the close-publication deadline can inform later windows. It
cannot reopen that missed daily alert window. Active paper paths refresh from
already-retained final bars, with no broker orders or provider fetches. Closed and
no-fill results stay fixed. Missing paths and known identity/action problems remain
unavailable or unresolved. Input revision corrections are retained for review,
not silently substituted into earlier plans.

Durable state, publications and an idempotent internal outbox use an isolated SQLite
ledger at `backend/backups/equity-shadow/stock-ideas-forward-v1/forward.sqlite`.
The reader view is replaced atomically in the same directory. A PostgreSQL advisory
lock permits only one resident publisher. Policy/model-hash mismatches refuse to
reuse the old store; preserve it and enroll a separate policy explicitly.

```powershell
& .\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_worker.py --plan
& .\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_worker.py --status
& .\backend\.venv\Scripts\python.exe -u .\backend\scripts\run_stock_idea_worker.py
```

`--once` performs bootstrap/resume and one cycle, not a wait until publication.
`--status` reports the retained view timestamp, not a process-liveness guarantee.
Checkpoint restart and synthetic incremental/batch parity are tested, but do not
constitute a completed multi-session forward soak. Source readiness and model
readiness remain separate: complete ingestion can still leave some members
unready because of warm-up or input integrity gates. No VIX, options-context gate, backfill, email delivery,
automatic saved-screener alerts, or brokerage execution was enabled.

The shared model adapter gained optional incremental checkpoint arguments while
keeping batch defaults. Older frozen replay source hashes therefore refer to the
older implementation; their manifests and results remain unchanged. State and
publication retention is currently append-only without automatic pruning; monitor
disk/memory during the shadow soak. This is an operational cutover, not an
effectiveness or full-market validation claim.

### Initial Operational Result

The 11:00 ET boundary's first retained run recorded a decision at
`2026-09-14T15:17:00.619890+00:00`: zero candidates/selections and 386 missing
members. A read-only canonical audit found all 386 bars now stored, but zero visible
under both timestamps at the 11:17 cutoff. First observation was 11:15:09 ET;
creation ranged from 11:18:18.132629 to 11:18:27.663008 ET. Observation alone was
not sufficient to admit those inputs. This empty run is not evidence of no setups.

That first implementation finished publication persistence at 11:17:13.420976 ET,
outside the five-second dispatch tolerance. Its original record remains intact
and is not certified as an on-time publication. No alert or paper position was
created. The final implementation stores large prepared inputs separately from
the atomic decision checkpoint, checks the clock inside the publication
transaction, and rolls back a late selection/outbox before recording a missed run.
An isolated real-sized state test reduced the decision/persistence path to 15 ms;
this is a timing test, not another live publication. Runtime source hashes are
retained per startup/publication so the initial implementation is distinguishable.

Final focused backend suite: 77 passed. Frontend regression suite: 73 passed;
TypeScript/Vite build passed. Browser checks verified forward tab routing, the
next publication display, mobile fit, and the unchanged August 18 replay count
of 42 trade alerts. The final resident resumed the same cohort with the next
publication scheduled for 11:47 ET. A successful publication with usable live
inputs and a multi-session timing soak remain unverified. The +17-minute input
cutoff was not relaxed to conceal upstream lateness.

### Source-Ready Fix And Current-Time Retry

The subsequent 11:47 ET run completed on time at 11:47:00.731923, but again had
zero deadline-visible inputs. All 386 native bars were created at
11:48:17.018264 through 11:48:24.721627, and the COMPLETE cohort published at
11:48:55.723092. The user explicitly approved troubleshooting/fixing the scheduling
mismatch and retrying the current window.

`stock_ideas_source_ready_v2` now gates on the exact boundary's COMPLETE native30m
publication and inclusion of every enrolled security ID. Both source publication
and creation times must be visible; source IDs/timestamps are recorded with the
actual input-read cutoff. Native hours continue to derive from stored30m slots.
The closing boundary additionally waits for its complete daily publication.
Checks begin at the configured provider delay (15 minutes). The hard dispatch
deadline is boundary +29m55s intraday, or exchange close +59m55s for the closing
window. This is a readiness window, not a promise to publish at its first minute.
No source completion before the deadline means `MISSED_PUBLICATION`. Preparation
or persistence overruns also fail closed. All original candidate expiries, entry
gates, quotas, deduplication and actual publication-time paper accounting remain.

Activation is retained in `dispatch_policy_history`; each new publication has the
timing-policy payload and a derived policy hash separate from the old fixed-cutoff
records. New enrollments use source-readiness by default. Migrating an existing
fixed-cutoff enrollment requires `--enable-source-readiness`; unsupported saved
policies fail closed instead of silently reverting. `--retry-window <UTC boundary>`
is an explicit one-time operation: only a retained empty incomplete run qualifies,
one retry is appended under a distinct key with `retry_of`, its prior record is
unchanged, and the next normal boundary is unchanged. It does not extend expiry.
Do not reuse the retry flags on subsequent resident starts.

The approved retry of the 11:30 ET boundary used source publication
`2f8683b9-94e3-5370-8381-19e9998c0126`, read inputs at 11:55:17.377669 ET, and
published at **11:55:41.077175 ET**, with the view complete at 11:55:41.393655.
It evaluated 149 retained candidates with 376/386 model-ready members and selected
seven plans: resumption ABBV long; acceptance PLD short, RMD long, USO long;
reversal HON long, FCX long, BND long. All seven were PENDING paper entry, with no
backdated fills or paper P/L. Native ingestion was complete for all 386; ten
model-unready instruments remained excluded (CBRS, MRSH, SPCX, APH, SUNB, P, FISV,
VMRK, HONA, ARKW). The two original empty runs remain intact, and the first
record's SHA-256 was reverified unchanged. Three-per-model caps, source timestamps,
original plan expiry and actual-time position stamps were verified.

The next normal boundary remains 12:00 ET, with a 12:15:00 through 12:29:55 ET
source-ready publication window. API/UI now display this window instead of an
exact +17-minute promise. Latest Run and Day History retain source separation.
Focused backend tests: 84 passed; TypeScript/Vite build passed. The single retry
demonstrates usable-input publication, not a completed multi-session forward soak
or a claim of predictive effectiveness. No broker orders or external context
gates were enabled.

## Day History Current-Price Comparison

Forward Day History withholds the latest publication's alert records until another run
commits, including a zero-alert run. The previous run then appears on the next
normal refresh without any ledger mutation, recapture or change to its plans/P&L.
The exclusion is chosen before filters and pagination, and independently of the
earlier-run selector under Latest Run. It is per run, not a blanket ticker ban:
an older occurrence remains visible even if that ticker appears again in the
latest run. Runs on earlier sessions are all retained once a later source
publication exists; the final run of each past day is not permanently omitted.
Source isolation and the 21-session navigation limit remain unchanged.
Frozen REPLAY history is exempt from withholding and displays every retained run,
including its final publication. In the normal date-navigation route it is also
available for covered dates before forward enrollment. A September 14 development
check verified selecting August 18 returns all 42 trade alerts with identical rows,
run records and frozen cutoff to explicit REPLAY, while returning to September 14
restores SHADOW history and its latest-run exclusion. Backend regression checks:
107 passed; TypeScript/Vite build passed. Only the reader API was reloaded; the
resident workers, frozen studies and stored paper outcomes were untouched.

The history table always includes **Triggered (ET)**, sourced from each alert's
original trigger timestamp rather than publication time. Default sorting is
descending trigger time, with deterministic ties and unknown timestamps last.
Timestamp sorting compares actual instants, including timezone offsets, not raw
timestamp strings. Manual sort choices remain available within the view; switching
between Latest Run and Day History resets to their publication/trigger defaults.
Run details identifies the withheld latest publication. A lone latest run produces
an explicit no-earlier-runs state rather than appearing prematurely in history.

Day History keeps **Current price** and **Price P/L %** visible together across
presets and saved layouts. This answers the user's current-price question without
waiting for a simulated executable entry. The displayed fraction is
`direction * (current_price / original_trigger_price - 1)`; the table shows it as
a percentage with the label **From trigger, gross**. Long price rises are positive;
short price rises are negative. An unchanged comparable price is a genuine zero.

This price comparison is not executed or realized P/L. It ignores paper fills,
stop/target exits, holding caps, fees, slippage, dividends and borrow costs. It can
remain available for PENDING, NO_FILL or CLOSED plans without changing their paper
accounting. **Paper P/L %** remains a separate column based on simulated entry and
exit/mark data; closed outcomes stay fixed and pending paper returns remain N/A.
Latest Run still excludes both price-comparison and paper-P/L columns.

For SHADOW Day History, the reader performs bounded read-only lookups for the
included earlier runs' security IDs and tickers (at most 1,000 identities). It chooses
the most recent eligible final, unadjusted RTH close among stored 5m/15m/30m/1h/1d
bars in the last seven days, preferring the finer interval at an equal bar end.
The configured provider-safe market-time cutoff and actual observed/created times
must all permit the price. The interval, market timestamp, revision ID and receipt
times accompany the price; the reader's price cutoff is separate from the worker
snapshot time. These are delayed stored prices, not real-time quotes or new
provider requests. Foreground refresh remains every 30 seconds and depends on
ingestion advancing the eligible price.

Invalid/missing prices, unknown timestamps, a price preceding the trigger, or known
identity/corporate-action comparability problems yield N/A rather than a fabricated
return. A quote-read failure leaves alerts and retained paper outcomes available,
with current prices unavailable. The cached snapshot, publication ledger, plans
and paper marks are never rewritten by this reader. Selecting an earlier forward
session compares its alerts with prices available now, not that session's close.
REPLAY retains its frozen **Price at cutoff**, price comparison and paper outcomes;
it does not receive live-price enrichment.

Validation on September 14 at 12:44 ET found 13 numeric price comparisons, with
eligible 5m prices ending 12:25 ET, while all 13 paper entries remained pending.
RMD long: trigger221.355/current223.02 => +0.7522%; PLD short:
trigger134.31/current135.18 => -0.6478%. Long/short arithmetic, return sorting,
locked column pairing, provider-delay/identity/action guards, preserved paper
results and the unchanged 42-row August 18 replay were checked. Focused backend
suite: 101 passed; frontend suite: 80 passed; production build passed. No alert
worker, execution policy, calibration model or provider ingestion was changed.

## Required Plan Columns And Probability Analysis

Latest Run always includes Target price, Original stop, Reward/risk, Success
probability and Risk assessment. These columns cannot be hidden by a preset,
the picker, or an older saved hidden-column list. This does not force the same
locks on Day History or change stored plans, selections, paper fills or returns.
The original price geometry remains frozen; target price is not a price forecast.

Success probability currently displays **Unavailable**. Risk assessment displays
the signed-direction trigger-to-stop distance, an **Unrated** label and retained
cautions; invalid/missing brackets are unavailable, and unresolved input states
are flagged as data issues. Full warnings remain in the tooltip and plan details.
Stop distance is neither account-level risk nor a maximum-loss guarantee. A small
stop, large reward/risk, strong momentum, many hits, or fewer recorded cautions
does not establish a low-risk trade or a high success probability.

### Evidence Available Now

- The alert row contract has no validated per-alert probability or calibrated
  aggregate risk category. The forward worker publishes deterministic plans.
- The independent pilot has 1,175 selected plans, but only 783 closed paths,
  390 no-fills and two open paths in its retained source vintage. All 24 evaluation
  groups have only 1-4 populated calendar blocks and fail that study's 40-block
  inference gate. Hundreds of correlated trades are not hundreds of independent
  trials, and passing an alpha test would not itself establish probability calibration.
- `raw_p` and `holm_adjusted_p` in the stock-idea evaluation are hypothesis-test
  p-values, not probabilities that an individual alert succeeds.
- [scanner_calibration.py](../backend/research/scanner_calibration.py) already has
  strictly earlier label-availability checks, walk-forward frequency estimates,
  Brier scores and reliability curves. Its target is positive net return on
  independent scanner periods, with a 40-period training minimum. Its output and
  existing qualification records must not be transplanted into individual alerts.
- The source-ready timing policy and tracked 386-instrument universe differ from
  the frozen replay's timing and historical ranking universe. A comparable-policy
  dataset is required; changing the old frozen study or relabeling its results is
  not an acceptable shortcut.

### Recommended Success Contract

Use **target reached before stop, within the original maximum holding period,
conditional on a valid executable entry** as the primary target for this column.
This is distinct from positive net P/L, beating SPY, or the probability of filling.
The choice is a proposed calibration contract, not an enabled model.

[mark_position](../backend/research/stock_idea_replay.py) already retains useful
labels: `TARGET` and `TARGET_GAP_CONSERVATIVE` are successes; `STOP`, `STOP_GAP`,
`STOP_FIRST_AMBIGUOUS`, and a resolved `TIME_OR_SESSION_CLOSE` without a target
are failures for this particular target. Preserve conservative stop-first handling
when both barriers occur in one 30m bar. This estimates the recorded paper policy,
not tick-perfect execution or real brokerage outcomes.

No-fills are excluded from the conditional-on-entry denominator and reported
separately, never silently treated as losses. Open/unresolved/missing/action-affected
paths are not failures or successes: retain their coverage and maturity states,
and evaluate adverse missing-outcome sensitivity. Use fixed maximum-horizon
maturity at evaluation checkpoints to avoid selecting only early-resolving paths.
A later fill-probability model may estimate the joint chance of a fill and target
success; do not present the conditional target estimate as that joint probability.

### Bounded Implementation Path

1. Freeze a separate comparable-policy study on the bounded tracked cohort, using
   the actual source-ready timing, price basis, identity/action rules, quota,
   display selection, next-open entry, barriers and holding periods. Keep stocks
   and ETFs distinguishable. Start with the selected feed, not a concatenation of
   overlapping PRIORITY/ALL/random arms or repeated rows for the same episode.
2. Build auditable binary outcome rows from the retained plans/path revisions.
   Retain prediction time and an immutable label-available time when the outcome
   was actually observable/materialized. Market exit time alone is not label
   availability; delayed or corrected data must not leak into earlier training.
   If historical availability cannot be established, exclude or conservatively
   delay the label rather than backdating it.
3. Start with a training-only, partially pooled model/direction/interval/horizon
   base rate. Report support and uncertainty before adding features. Then compare
   one small regularized logistic model using only facts frozen at publication:
   stop distance/ATR, reward-risk geometry, extension, liquidity and available
   relative strength/momentum. Do not use later fills, final status, future hits,
   later prices, or context fields that were not available at that publication.
4. Use chronological train/calibration/test folds with label-availability gating,
   purging and embargo for overlapping holding periods. Keep same-window correlated
   observations together; evaluate uncertainty with time blocks and security
   concentration checks, not a random row split. Calibration fitting must use a
   separate earlier fold; keep the final test period untouched. Reuse the existing
   availability/reliability patterns and installed scikit-learn rather than
   changing the old scanner-return label semantics.
5. Compare out-of-sample Brier score and log loss against the training-only cohort
   base-rate forecast, not only a fixed 50% predictor. Check reliability curves,
   interval widths, class coverage and stability across time/model/direction/
   interval, plus concentration, missing outcomes and policy drift. Accuracy or
   AUC alone is insufficient. Freeze support/precision and freshness gates before
   reviewing the test results. The old 40-block alpha gate is not a calibration
   certificate; even roughly 385 IID outcomes only give about +/-5 percentage
   points at 95% confidence near a 50% rate, and correlated data need more support.
6. First retain prospective predictions in shadow with model version, exact
   prediction contract, training cutoff, support, uncertainty and source hashes.
   Only publish a numeric table value after those gates pass. Unknown contracts,
   insufficient support, stale models or drift remain Unavailable. Historical
   predictions must stay as originally published, not be recomputed by later models.

For a later aggregate risk category, predeclare what is being rated (for example
probability of losing more than the planned stop distance), validate loss-tail and
gap/slippage estimates separately, and keep event/borrow/liquidity uncertainty
explicit. No account-level category is defensible without position sizing and
portfolio exposure. The current quantified stop distance and cautions avoid
inventing those missing inputs.

Reward/risk does not supply the missing probability. In the idealized two-outcome,
zero-cost case, reward/risk 2:1 implies a 33.3% break-even win rate, not a forecast
of 33.3% or 66.7% success. Time exits, costs and gaps further change that calculation.
No calibration model was trained, new backtest run, probability gate enabled, or
worker/data policy changed as part of this UI and analysis request.

## Legacy Worker And Storage

The original daily worker is now an offline restoration copy in the external
`equity-retirement-2026-09-14` batch; see the
[archive instructions](LEGACY_CLEANUP_AUDIT.md#external-archive).
It is not included in `start_workers.ps1` and must not be run from its archive path.
The current producer is [run_stock_idea_worker.py](../backend/scripts/run_stock_idea_worker.py).
Archival did not activate that producer or change the retained reader policy.

The retired worker checked once per minute, captured new complete daily
publications, and updated paper marks from already-ingested canonical bars.
Its existing shared service, read-only APIs and stored evidence remain in place.
The legacy service used an advisory lock, transactional cycles and idempotent
snapshot/alert/mark keys. No automatic purge is configured.

The existing `equity_evidence` ledger stores new `STOCK_DISCOVERY`,
`STOCK_DISCOVERY_ALERT` and `STOCK_DISCOVERY_MARK` sources under the explicit policy
version. Snapshots, alerts and mark revisions are append-only. These records do
not inherit old scanner qualification IDs. No new schema migration is required,
and old research artifacts, historical bars, options data and the separate
prospective portfolio study are preserved.

The APIs `/api/stocks/screener` and `/api/stocks/alerts` read stored results. Screener
responses include the published universe/exclusion counts, original ranks and
snapshot times; alerts include mark evaluation time, pending/failure reasons and
each horizon's independent outcome. CSV export is for the displayed page only.

## Why Backtesting Still Matters

The screener asks which stocks meet a transparent discovery definition now.
Forward alert monitoring asks how those observations subsequently behaved under
a specified accounting policy. Backtesting asks whether that policy adds value
relative to controls across market conditions, after costs, missing data,
overlapping exposures and statistical uncertainty.

A profitable alert, a rising stock, or a green open P/L does not establish a useful
strategy. The completed baseline and v2 tests did not justify promoting a trading
rule. That finding limits what the discovery pages claim; it does not prevent
displaying factual rankings or retaining observations for future evaluation.
Industry backtesting supports hypothesis rejection, risk estimation, parameter
discipline and robustness checks, followed by forward monitoring and execution
validation. It is not a guarantee of future profit or a reason to optimize until
one historical result looks attractive.

Validation includes rank/filter stability, no initial/repeated alerts, causal
price visibility, missing entry and corporate-action guards, correct open/closed
fees and notional arithmetic, fixed terminal exits and no-fill outcomes. The
initial database capture was repeated with zero new records. Browser validation
covered desktop/mobile screener and alert layouts, rank-preserving search,
pagination, empty states, and browser-only synthetic open/closed alert details.
Synthetic browser fixtures were removed and were never persisted in the database.