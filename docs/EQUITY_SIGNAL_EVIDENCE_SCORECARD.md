# Equity Signal Evidence Scorecard

Status: **STRICT BATCH AND MATCHED-CONTROL REVIEW COMPLETE. NO NEW LIVE QUALIFICATION PUBLISHED.**

September15 maintenance: the original daily batch/replay/merge commands are now
in the external `legacy-daily-harness` batch; see the
[archive instructions](LEGACY_CLEANUP_AUDIT.md#external-archive). Command examples below
describe completed historical work, not current launch instructions. Shared
V2 report/outcome utilities and all original studies remain retained; see the
[cleanup audit](LEGACY_CLEANUP_AUDIT.md). Do not execute commands from the archive
or rewrite frozen hashes to resume changed code.

The separately authorized v2 research phase is documented in
[EQUITY_STRATEGY_V2_RESEARCH_DESIGN.md](EQUITY_STRATEGY_V2_RESEARCH_DESIGN.md): three
implemented research-only long/short strategy contracts, bounded tuning and revised
date-dependence handling. Their pilot and bounded study are now complete, with
no strategy promoted; their artifacts are separate from this baseline scorecard.
The user chose to retain existing data until the strategies
have been compared. No old detector replacement or data purge is part of this scorecard.

## Completed Strict Batch

The user completed both replay and outcome stages for `all-daily-strict-v2`.
The final report command has now completed successfully. Merging belonged to the
replay stage; there is no further merge or detection run required before review.

- [Batch scorecard JSON](../backend/backups/equity-signal-backtests/all-daily-strict-v2/scorecard.json)
- [Batch scorecard CSV](../backend/backups/equity-signal-backtests/all-daily-strict-v2/scorecard.csv)
- [Frozen batch plan](../backend/backups/equity-signal-backtests/all-daily-strict-v2/plan.json)

Scope: eight existing daily adapters, including the seven composite scanners,
on the two original disjoint 300-ticker samples. Signal dates span September 7,
2022 through September 3, 2026: 1,002 XNYS sessions. Source observation cutoff is
`2026-09-13T03:59:43.432904+00:00`. These dates were previously examined research
history, not a pristine temporal holdout. No detector or outcome implementation
was changed during this evaluation.

| Batch Measurement | Count |
|---|---:|
| Completed replay shards / merged groups | 64 / 16 |
| Completed outcome groups | 16 |
| Replay events, including non-eligible gap controls | 105,856 |
| Exact eligible evidence records read by scorecard | 102,822 |
| Latest batch-policy outcomes read | 494,179 |
| Repeated lifecycle records removed for evaluation | 9,735 |
| Declared source/direction/horizon/mode cells | 246 |
| Scorecard rows across pooled/sample-1/sample-2 scopes | 738 |

The outcome count includes unavailable and non-entered cases, not just measured
trades. The scorecard retains nominally immature/missing outcomes in its coverage
states. All 16 outcome completion reports have zero pending work under their
chosen maturity cutoffs; that is not complete forward coverage for the last
signal dates. The audited March 4, 2024 count discrepancy remains explicit.

### Evidence Decision

**No cell passed the full research evidence screen.** The report contains 401
`NO_STABLE_POSITIVE_EDGE`, 130 `INSUFFICIENT_EVIDENCE`, 36
`NOT_SUPPORTED_AFTER_UNCERTAINTY_CHECKS`, and 171 `NO_USABLE_EVIDENCE` rows.
The minimum reported BH-adjusted alpha q-value is approximately 0.158, above the
0.05 threshold. Declared empty directions and unavailable cells remain in the
family; the 738 rows are not 738 independent tests or strategies.

This is evidence against promoting these exact standalone implementations now,
not evidence that all their inputs are useless. Numerical quality scores and
individual-stock probabilities remain null. Nothing was published to live ranks,
equity/option gates, or the paper tracker.

Illustrative **21-session directional** results, reported separately by sample:

| Definition / Side | Mean Net S1 / S2 | Mean Sector Excess S1 / S2 | Non-Overlapping Periods S1 / S2 |
|---|---|---|---|
| Structure reversal, long | +3.667% / +2.414% | +2.808% / +1.348% | 42 / 43 |
| Momentum pullback, long | +0.611% / +1.017% | +0.566% / +0.271% | 47 / 47 |
| Structured trend pullback, long | +3.461% / +1.192% | +1.429% / -0.161% | 45 / 46 |
| Breakout expansion, long | +3.776% / +1.343% | +2.292% / -0.291% | 45 / 45 |
| Level retest rejection, long | +1.013% / -0.251% | -0.274% / -1.009% | 47 / 47 |
| MA9/21 crossover, long | +1.270% / +1.231% | -0.406% / -0.637% | 47 / 47 |
| Bearish bounce, short | -3.850% / -2.797% | -2.721% / -1.899% | 46 / 47 |

These are measured non-overlapping signal-date cohort means after the existing
4 bps cost assumption, not annual returns, compounded portfolios, total returns,
or fully costed short-sale results. Positive means alone do not pass stability and
uncertainty gates. Structure reversal and momentum pullback are possible research
leads, not selected deployable winners; focusing on them after seeing results is
post-selection and needs a separately declared comparison. The product pullback
ADX equal-directional-movement defect found in the definition review remains in
this frozen baseline and must be disclosed before interpreting its grades.

**Do not average or rank from the pooled mean alone.** Each scope independently
chooses horizon-spaced, non-overlapping signal dates. Combining the samples can
therefore change the selected dates, so `ALL_BATCH_TICKERS` is not an average of
the two sample statistics and can even have a different sign from both. A paired
incremental comparison must use common decision dates and execution windows.

### Provisional Consolidation

Keep the frozen definitions and results as the baseline. Consolidate their
presentation and future evaluation into a few hypothesis families, without
treating multiple labels as independent confidence votes:

| Proposed Family | Existing Definitions | Intended Role |
|---|---|---|
| Trend continuation | Structured pullback, momentum pullback, bearish bounce | Separate setup conditions from actual entry triggers; evaluate sides independently |
| Breakout / expansion | Swing breakout, compression breakout, continuation patterns, gap continuation/confirmation | One event family with compression, gap and geometry attributes |
| Reversal / failure | Structure reversal, failed breakout, gap fade, reversal patterns, SMA200 failed reclaim | Separate confirmed failure from watch states; retain horizons explicitly |
| Location / context | Gap/FVG/Fibonacci retests, gap-entry geometry | Conditional location or target/invalidation context, not an independent directional vote |
| Baselines | MA9/21 and the previously frozen momentum benchmark | Test whether complexity adds value beyond simple trend information |

An exact same-ticker/date/direction overlap check on the retained eligible replay
events, collapsing multiple same-family events on a date, found:

- Compression breakout coincides with breakout expansion on **383 of 499**
  compression dates (76.8%; pairwise Jaccard 7.1%). Treat compression as a
  candidate breakout attribute; the reverse overlap is much smaller.
- Structure reversal coincides with level retest on **1,484 of 2,500** reversal
  dates (59.4%; Jaccard 4.4%). This argues against counting both as two independent
  confirmations, not against keeping their different timing/location meanings.
- Structured pullback coincides with level retest on **2,579 of 7,055** pullback
  dates (36.6%). Shared location is not proven incremental alpha.

These overlaps combine the two disjoint ticker samples and use detection dates,
not the scorecard's first-lifecycle/non-overlapping outcome sample. They measure
co-occurrence, not causal contribution, return correlation or redundancy of all
information. Many rare pattern directions are underpowered; the SMA200 family
has only 20 distinct detection keys and is not a sensible calibration candidate.

The separately authorized **common-date matched-control** comparison is now
complete and documented below. It uses the retained eligibility records without
another detection replay and distinguishes setup, trigger and location roles. Do not alter
thresholds, combine the largest observed means into a new score, discard failed
samples, or start more parameter searches as part of this closeout. No strategy
deletion, rule consolidation, or production change has been applied. The initial
scorecard and the subsequent matched-control report remain separate artifacts.

Verification: frozen batch/code and completion hashes passed the report's checks;
CSV checksum, unique cell keys, count conservation, null quality/probability
fields and actual database read-only mode were checked. The original retained-
ledger audit below remains a separate historical artifact, not the strict result.

## Matched-Control Comparison

The separately authorized follow-up uses
[report_equity_matched_controls.py](../backend/scripts/report_equity_matched_controls.py)
without changing the frozen detectors or their stored outcomes. The analysis
contract is written before its control returns are loaded. It is a post-baseline
exploratory comparison, not an untouched validation sample or a causal experiment.

The same 600 tickers, source cutoff, original memberships and first-lifecycle
signals are retained. Price reads verify that all original replay price revision
IDs are reproduced. Twelve-minus-one-month momentum uses a contiguous, valid
253-session security history; feature exclusions remain explicit. Known unsupported
corporate actions, identity changes and missing sessions prevent a measured price
path, while a valid zero-volume intended entry is a no-fill cash observation.

Three controls are fixed for each source, direction and signal date:

1. Equal-weight stocks with explicit `NO_SIGNAL` coverage from the owning adapter.
  For the combined composite adapter, these stocks have no composite match,
  not merely an absence of the particular scanner under examination. Rejected
  candidates and continuing matches are not treated as clean controls.
2. The strongest prior-momentum decile of the same feature-ready eligible pool
  for long signals, or the weakest decile for short signals. This benchmark may
  include stocks selected by the detector; it is a simple competing selection rule.
3. For each signal, the nearest no-signal momentum percentile within 0.10 rank
  distance, with replacement and deterministic ticker tie-breaking. Reused
  controls retain their repeated weight. Unmatched signals are counted explicitly.

Both sides use next-session open to the declared horizon close, 4 bps round-trip
cost and the same direction. No-fill positions incur no trading cost. Controls
are selected before their returns are examined; missing positions do not trigger
replacement or partial-portfolio weight renormalization. The matched subset can
exclude feature-unready or unmatched signals, so any apparent benefit with those
exclusions is qualified as incomplete rather than applied to every detection.

The primary horizon is 21 sessions; 5 and 10 are diagnostics. Each horizon has a
fixed XNYS grid anchored to September 7, 2022. Inference uses only grid dates with
signals in both samples, and exactly the same complete-pair dates in both sample
summaries. Missing pair dates are counted. All signal-date comparisons are retained
as descriptive data, but overlapping daily returns are not counted as independent
periods. Confidence intervals and BH diagnostics use the existing 100-signal /
40-period minimum and the complete declared comparison family within each sample.

This reduces the available sample for rare detectors and does not make missingness
ignorable. It does not match sectors, volatility, news, liquidity or all other risk
exposures. Price returns exclude dividends and realistic short borrow costs. The
known product-scanner ADX tie defect remains part of the frozen baseline. Any
retain/simplify/context/park labels are review proposals, not production changes.

### Matched Results

The analysis completed and retained **267,255 daily comparison rows** and **1,044
summary cells**, including empty declared directions. It read 640,806 frozen price
revisions and verified all 638,298 revisions referenced by the original replay.
The three control types and repeated horizons/sample rows are not independent
strategies or independent observations.

- [Matched-control summary CSV](../backend/backups/equity-signal-backtests/all-daily-strict-v2/matched-controls-v1/summary.csv)
- [Complete report and proposed dispositions](../backend/backups/equity-signal-backtests/all-daily-strict-v2/matched-controls-v1/report.json)
- [Fixed comparison contract](../backend/backups/equity-signal-backtests/all-daily-strict-v2/matched-controls-v1/contract.json)
- [Daily paired comparisons](../backend/backups/equity-signal-backtests/all-daily-strict-v2/matched-controls-v1/daily.csv)
- [Decision-time signal/control selections](../backend/backups/equity-signal-backtests/all-daily-strict-v2/matched-controls-v1/selections.jsonl)

**No definition established replicated primary-horizon support against the three
controls.** Across all summary cells, 900 are `INSUFFICIENT_COMMON_DATE_EVIDENCE`,
127 are `NO_STABLE_INCREMENTAL_EDGE`, and 17 are
`NOT_SUPPORTED_AFTER_UNCERTAINTY`. At the primary 21-session horizon, **346 of 348
rows are underpowered**, and the remaining two have no stable incremental edge.
This conservative fixed-grid, shared-signal-date design removes much of the
available sample, especially for rare events; these counts must not be described
as hundreds of statistically disproven strategies.

The only q-value below 0.05 is approximately 0.0393 for sample 1's long breakout
expansion versus equal-weight no-signal controls at 10 sessions. Its incremental
mean is **negative** (-4.296 percentage points) and it has only 29 paired periods.
It is not a positive strategy discovery, and there is no threshold-qualified
replicated positive result.

Illustrative 21-session results against momentum-matched no-signal controls:

| Definition / Side | Mean Incremental S1 / S2 | Shared Measured Periods | Interpretation |
|---|---|---:|---|
| Momentum pullback, long | +0.372 pp / -1.635 pp | 37 | Positive lift does not replicate; both below 40 periods |
| Structured trend pullback, long | +0.388 pp / -0.749 pp | 22 | Mixed direction and underpowered |
| Breakout expansion, long | +5.817 pp / -4.154 pp | 21 | Large sample disagreement; not a stable selection advantage |
| Structure reversal, long | +11.530 pp / +1.794 pp | 5 | Too few periods to interpret the positive means as reliable |
| Failed breakout reversal, short | +1.980 pp / +1.790 pp | 21 | Positive descriptive lead, insufficient shared periods |
| Level retest rejection, long | +0.869 pp / -0.528 pp | 37 | No replicated positive lift |

These are paired stock-price return differences on the pre-set common dates, not
annualized returns, sector excess, or option returns. They are not numerically
interchangeable with the initial scorecard's separately selected cohorts. The
all-signal-date descriptive view is also retained, but its overlapping returns
cannot be used to replace the independent-period count: for example, momentum
pullback's long all-date lift is -0.358 pp / -0.214 pp, and structure reversal's
is -0.168 pp / -0.501 pp. These descriptive means do not certify a negative effect.

### Consolidation Decision

The defensible immediate proposal is **simpler roles and no new confidence
promotion**, rather than selecting a profitable winner from insufficient data:

| Disposition | Definitions / Scope | Meaning |
|---|---|---|
| Keep baseline | MA9/21 | Preserve a simple comparison rule, not a qualified trade recommendation |
| Simplify to attribute | Compression breakout | Retain the compression attribute within the breakout family; the earlier 76.8% one-way overlap supports presentation simplification, not proof of redundant information |
| Keep context | Level retests and gap-entry geometry | Retain location/invalidation information without standalone directional grades |
| Keep geometry; park standalone claims | Named pattern breaks | Preserve shapes as descriptive attributes; rare-pattern performance remains unresolved |
| Park standalone promotion | Other trend, breakout, reversal and gap definitions | Keep frozen evidence and descriptive detection, but do not grant live confidence weights or claim a reliable incremental edge |

The machine-readable dispositions are proposals (`applied=false`) based on these
roles and the fixed evidence gates, not an optimized or empirically proven strategy
combination. "Park" does not mean erase historical data, disable every detector,
or conclude a rare setup never works. No combined family trading rule has been
backtested here. A presentation/consolidation change requires a separate explicit
implementation decision; this bounded evaluation does not launch more parameter
searches or automatically expand the historical programme.

Validation: six focused tests cover outcome-independent matching, deterministic
ties, explicit missing/unmatched weights, common schedules, causal momentum,
strict missing/action/no-fill paths and agreement with the existing outcome
evaluator at all three horizons. Artifact hashes, unique cells, identical paired
dates across samples, date-count conservation and the frozen script fingerprint
were verified. Only local analysis artifacts and documentation were written;
no database research facts, models, qualifications, option gates or live ranks
were changed.

## Initial Retained Audit

The manual runner now uses the strict replay corrections documented below. The
initial audit figures in this document have **not** been regenerated under those
corrections, and the number of older outcomes affected has not been measured.

This is the first consolidated coverage matrix and daily 5/10/21-session scorecard
for the existing signal ledger. It inventories what is stored and measures usable
outcomes; it does **not** claim that every detector has been replayed across the full
historical universe. The earlier momentum/ridge portfolio studies remain unchanged.

## Repair Verification

The administrator applied the prepared recovery for one damaged PNC 5-minute feature
snapshot, evidence ID `92460ab7-aefd-59e3-906d-9cba058eb118`. Its market time is
2026-09-10 14:20 UTC. Reconstruction from all 400 original bars matched the original
evidence ID, key, source-window hash and payload hash exactly.

Readback matched every reconstructed field, and the 8,192-byte pre-repair page backup
matched its recorded checksum. The original market/observation timestamps are retained.
The unreadable insertion timestamp was replaced with the actual repair transaction
time, **2026-09-13 02:58:26.133681 UTC**, rather than an invented historical timestamp.
This makes later creation-time-sensitive reads conservative.

The post-repair quality-code audit decoded all **595,488 evidence rows** with no
affected rows or pages. This verifies the specific read failure is resolved; it is
not a full database, disk or memory integrity certification. The original storage
damage cause remains unknown. PostgreSQL reported data checksums disabled. Database
backup/integrity and host diagnostics remain a separate operational follow-up, not
a reason to rewrite more research records or change permissions automatically.

Preserved local repair evidence:

- [Original damage audit](../backend/backups/equity_evidence_storage_audit.json)
- [Applied recovery record](../backend/backups/equity_feature_repair_applied.json)
- [Post-repair audit](../backend/backups/equity_evidence_storage_audit_after_repair.json)

The scorecard continuation itself was read-only. No further database repair, model
fit, qualification publication, option-gate change or paper-tracker update was made.

## Coverage Delivered

| Surface | Retained Coverage |
|---|---:|
| Top-level evidence records inventoried | 595,488 |
| Distinct retained source names | 30 |
| Source/version/type/role/timeframe/direction/origin groups | 213 |
| Registered composite-scanner evaluation cells | 312 |
| Registered cells without retained events | 30 |
| Current-version daily scanner records read | 172,336 |
| Latest visible subject/policy/horizon outcomes read | 1,020,540 |
| Repeated daily lifecycle records removed | 40,341 |
| Daily scorecard cells | 504 |

The 312 registry cells cover supported intervals only. They include **84 daily
cells**: seven scanners, two directions, three horizons and two outcome modes.
The 504 daily rows separate those 84 cells across three ticker scopes (all retained
tickers, original sample 1 and original sample 2) and two origins (live and reconstructed).
Exact policy IDs remain separate and would add rows if multiple revisions were present.
Repeated horizon/mode/sample rows are not independent observations.

Inventory counts span features, fundamentals, regime observations, patterns, channels,
directional scanners, location-only scanners and composite setups. Nested signals
inside a bundled payload are not automatically independent registered detectors.
Feature columns are listed as inputs, not claimed individually evaluated predictors.

### Daily Scanner Coverage

| Current Scanner | Historical Daily Replay In Ledger | Live Daily Records | Usable Live 5-Session Directional Outcomes |
|---|---:|---:|---:|
| Structured trend pullback | None retained | 66 | 11 |
| Level retest rejection | 171,804 records | 310 | 72 |
| Breakout expansion | None retained | 48 | 9 |
| Compression breakout | None retained | 9 | 1 |
| Failed breakout reversal | None retained | 72 | 34 |
| Structure reversal | None retained | 27 | 7 |
| SMA200 reclaim/rejection | None retained | 0 | 0 |

Live counts combine both directions before lifecycle deduplication; the outcome
column counts usable first-lifecycle outcomes in the all-ticker scope. The live
scorecard has **at most two non-overlapping periods per cell**. Therefore it cannot
support a reliable evidence grade or calibrated probability yet.

Only `level_retest_rejection:1.2` currently has a multi-year daily replay in this
ledger, spanning July 22, 2022 through September 3, 2026. Other detectors did appear
in the earlier bounded portfolio experiment, but that is not the same as a complete
all-detection-date persisted replay for each family.

## Policy Timeline Finding

Historical level-retest outcomes explicitly reference the current directional and
recommendation-plan policy IDs. Those policy rows record an effective start of
August 30, 2026, even though the replay outcomes start in 2022. The historical
evaluator constructs policies from the earliest replay signal and passes explicit
subject IDs; policy identity is content-derived and does not include that effective
start date. A stored live-policy date is therefore not a reliable replay-start filter.

The initial scorecard incorrectly applied the live window to reconstructed events,
hiding most of their measurements. The report was corrected, **not the database**:

- Live evidence must still fall inside its recorded policy effective window.
- A reconstructed ledger group is assessed under an exact policy/horizon only when
  retained outcomes explicitly link it to that policy.
- The whole deduplicated group stays in the denominator, including events without
  usable outcomes. Linking measured rows does not remove the missing-event population.
- `policy_application`, `outside_policy_window` and `retrospective_policy_events`
  disclose the discrepancy. It prevents certification even if statistical gates pass.
- Origin, run purpose, source version, sample, cost and benchmark policy are never pooled.

This does not backdate live authorization or qualify previously unqualified evidence.

## Historical Evidence Results

For the all-retained-ticker level-retest **directional** policy, after first-lifecycle
deduplication and non-overlapping cohort sampling:

| Direction | Horizon | Usable Event Outcomes | Non-Overlapping Periods | Mean Net Return | Mean Sector Excess |
|---|---:|---:|---:|---:|---:|
| Bullish | 5 sessions | 67,179 | 206 | +0.154% | -0.088% |
| Bullish | 10 sessions | 66,844 | 103 | +0.516% | -0.007% |
| Bullish | 21 sessions | 66,094 | 49 | -0.287% | -1.478% |
| Bearish | 5 sessions | 63,529 | 206 | -0.218% | -0.048% |
| Bearish | 10 sessions | 63,286 | 103 | -0.554% | -0.114% |
| Bearish | 21 sessions | 62,579 | 45 | -1.934% | -0.569% |

These are means of retained measured signal-date cohorts under the existing 4 bps
cost policy, **not compounded portfolio returns, annual returns, or guaranteed alpha**.
Bearish signed outcomes are not a fully costed short-sale/borrow simulation.
Unavailable entries or benchmarks remain in coverage counts but not in the means;
thus measured-only statistics retain missingness bias.

All twelve all-ticker historical cells, including recommendation-plan stop/target
outcomes, fail the stable-positive-edge screen. Both original 300-ticker subsets are
also reported separately. A positive result in one subset does not qualify the signal:
for example, sample 2's bullish 21-session directional mean sector excess is positive,
but fails the uncertainty/multiple-testing screen; sample 1 has only 37 non-overlapping
periods in that cell, below the existing 40-period minimum.

Across the **504 scorecard rows**:

| Evidence State | Cells |
|---|---:|
| No usable evidence | 382 |
| Insufficient evidence | 87 |
| No stable positive edge | 33 |
| Not supported after uncertainty checks | 2 |
| Research pass | 0 |

This is **not** a verdict that every signal is useless. It separates an unsupported
standalone result in the available historical replay from the much larger set that
has not been adequately measured. Location and participation signals may add value
conditionally without having a standalone directional return advantage.

## What The Scorecard Means

Each cell carries raw and deduplicated event counts, exact policy identity and cost,
nominal maturity, evaluated/usable outcomes, entry/missingness reasons, quality-code
occurrences, distinct tickers, non-overlapping period counts, net return, primary
benchmark excess, MAE/MFE, cohort hit-rate interval, early/late stability and the
separate fixed development/later views.

The statistical calculation reuses the existing daily qualification utilities:
equal-weight signal-date aggregation, exchange-session spacing, non-overlapping
execution paths, Student-t diagnostics, Wilson hit-rate intervals, and BH correction
over the complete declared daily family within each origin/run-purpose/sample scope.
Unmeasurable family cells count as p=1 internally; they do not disappear from the
test family. Non-overlap reduces dependence but does not prove observations independent.

No new `ROBUST_PASS` revision is published. The 12 existing published qualification
records are attached separately with their identities; their grades are not inherited
by a new ticker subset or a different outcome policy. Individual probability and
numeric `quality_score` stay **null**, rather than arbitrary values inferred from
pattern strength or insufficient history. The 20% portfolio drawdown limit is not
used as a detector-quality threshold.

## Next Bounded Work

The immediate data-read blocker is resolved and the retained-ledger audit is complete.
The remaining work is an evidence-coverage task, not more repairs to this PNC row:

1. Freeze a daily all-detection-date replay for the seven registered scanners on the
   original sample, retaining no-signal controls, exact source lineage and all
   5/10/21-session outcomes. Reuse and verify the existing level-retest work; do not
   infer complete coverage merely from a large event count. Replicate supported
   findings on sample 2 under the same settings.
2. Report incremental value against contemporaneous momentum and eligible-stock
   controls, with missing paths and date dependence explicit. This scorecard's
   benchmark-excess statistics do not replace a matched no-signal comparison.
3. Define role-appropriate outcome contracts for the other retained families:
   pattern triggers versus immediate/delayed entry; gaps/FVG/Fibonacci/channels as
   conditional location; confirmation/participation as incremental context; and
   composite setups under their own policy. The inventory flags absent standalone
   policies rather than inventing a directional win rate for every source.
4. Publish reviewed versioned evidence states only after coverage and validation,
   then separately test individual-outcome calibration and option-conditioning value.
   A stock signal grade must not override option quote, event or execution gates.

No replay, new detector variant, worker restart or production publication is included
in this audit continuation. The equity paper study and option shadow comparison stay
separate and unchanged. Broader database/host integrity investigation is also distinct.

## Artifacts And Validation

### Manual Historical Runs

The strict workflow has passed the focused regression suites, a four-stock
serial-versus-four-shard replay, and a rollback-only persistence check. The replay
produced identical event and coverage hashes: **17 events and 96 ticker-date
records** (14 matching dates and 82 no-signal dates). A separate MA crossover smoke
produced 4 events and 48 coverage records. The rollback check inserted 17 evidence
records and 40 mature outcomes, read back exactly those subjects/policies, and
inserted zero outcomes on an identical repeat. The transaction was rolled back;
no pilot research facts or qualifications were retained in PostgreSQL.

Corrections applied without changing detector parameters:

- Daily paths must preserve security identity, consecutive XNYS sessions and exact
  session clocks through the actual exit. A plan exit before a later gap remains
  valid. Known unsupported merger/symbol-change/spinoff paths are unavailable.
- Zero-volume entries are explicit non-entries, not filled trades or imputed
  zero-return observations. Missing benchmark endpoints/interior sessions yield
  unavailable benchmark returns, never a shortened comparison window.
- Actual source observation/creation cutoffs are shared across the batch and
  reused on resume. Features restart at missing/invalid bars and identity changes;
  exact price revision IDs and action fingerprints are retained.
- `DAILY_IDENTITY_REPLAY_V3` event identities include source fingerprints.
  `historical_daily_integrity_v1` outcome policies include the frozen cutoff,
  study start and calculator fingerprint, separately from old results. Existing
  evidence is verified against its requested payload, identities and source IDs
  before reuse; conflicts fail rather than silently accepting older contents.
- Coverage JSONL records retain every dated universe member, including missing
  bars, insufficient history, identity/clock failures, no signals, rejected
  candidates, fresh matches and continuing matches. A rejected candidate is not
  automatically a clean no-signal control. Combined-scanner matching dates can
  contain more than one event.
- Strict outcome paths are loaded once per ticker for each adapter/sample group,
  then reused across subjects, horizons and modes. JSONL reads/writes are streamed;
  coverage merges are ordered across shards. Event sorting still uses memory.

The plan-first launcher is
the original batch launcher retained in the external `legacy-daily-harness` batch.
It calls the existing replay, shard merge and outcome CLIs; it does not implement
new detector rules or a second outcome engine. The commands below are for Windows
PowerShell from the repository root, using the existing backend virtual environment
and database settings in `backend/.env`. No administrator account is required.
PostgreSQL must be running and the historical inputs must already be present.

`composites` runs all seven registered daily scanners in one adapter pass per
ticker shard. `all-adapters` runs that combined adapter plus the current gap
formation, gap breakaway confirmation, gap entry/fill, MA9/21 crossover, momentum
pullback, bearish bounce and pattern boundary-break adapters. This is **eight
adapters**, not eight scanners and not every inventoried context/feature source.
Old aliases are excluded to avoid replaying the same adapter twice.

The defaults use the two original, disjoint 300-ticker samples, September 7, 2022
through September 3, 2026, split-adjusted daily bars and original reconstructed
universe revisions. The last signal dates can lack mature forward outcomes.
The first execution freezes the source cutoff in the batch plan; both the replay
and outcome stages use it. `--source-cutoff` may be supplied explicitly for a
same-cutoff parity experiment. A missing requested universe session aborts the run
rather than silently shortening the study. These are reconstructed histories with
a fixed observation cutoff, not a claim to original historical vendor vintages.
No new historical data is downloaded. `--sample 1` or `--sample 2` narrows the run;
`--start`/`--end` can narrow a separate pilot. These dates are previously examined
research history, not a new untouched holdout.

#### Run All Current Daily Adapters

```powershell
$python = ".\backend\.venv\Scripts\python.exe"
$runner = ".\backend\scripts\run_daily_strategy_backtests.py"
$batch = @("--scope", "all-adapters", "--sample", "both",
       "--workers", "4", "--shards", "4",
           "--output-dir", ".\backend\backups\equity-signal-backtests\all-daily-strict-v1")

# Preview only: no database access or output writes.
& $python $runner @batch

# Replay all adapters, then merge each sample/adapter's ticker shards.
& $python -u $runner @batch --stage replay --execute
```

Wait for successful replay completion before running the separate outcome stage:

```powershell
& $python -u $runner @batch --stage outcomes --execute
```

The outcome stage writes replay evidence, policies and 5/10/21-session outcomes
to the existing canonical tables, with the existing 4 bps cost setting. Composite
scanners receive the existing directional and recommendation-plan policies; other
adapters use their existing supported policies, not an invented universal trade
plan. It deliberately never passes `--qualify` or `--all` to the outcome runner.
No grades are published, and paper tracking and option gates are untouched.

For only the seven daily scanners, use `--scope composites` and a different output
directory in `$batch`. For a short replay timing pilot, use the same scope and add
`--sample 1 --start 2026-08-03 --end 2026-09-03` in a separate invocation, with a
separate output directory. Do not pass a second sample option on the same command.
A pilot still loads the strategy's required preceding warm-up history.

#### Parallelism And Recovery

| Stage | Concurrency | Reason |
|---|---:|---|
| Historical detection | Start with 4 processes; try 6 after a pilot | Independent deterministic ticker shards |
| Shard merge | 1 | Avoid several large in-memory merges competing for RAM |
| Evidence/outcome persistence | 1 | Existing historical outcome runner has a global advisory lock |
| Scorecard | 1, after writes finish | Read a completed ledger snapshot |

The local host reports **16 logical CPUs and 47.7 GiB RAM**. Four workers is a
conservative starting recommendation while the application and other workers run,
not a measured throughput optimum. Six may improve throughput if CPU, memory and
database latency remain acceptable; the launcher caps concurrency at eight.
It sets common BLAS/OpenMP thread counts to one per child to avoid nested thread
oversubscription. Do not run 16 replay workers or several copies of the batch.

`--workers` is the **global process cap**, not a per-strategy multiplier.
`--shards` controls work partitioning per sample/adapter, not concurrency.
With both samples and four shards, `composites` schedules 8 jobs; `all-adapters`
schedules 64 jobs, still only four at a time with `--workers 4`.
Do not split by date: ticker shards preserve each adapter's warm-up and lifecycle
history. Do not run historical input preparation/backfills concurrently, since
the existing price reader selects retained revisions separately in each process.

Each shard, merge and outcome command has a `.log` file beside its output. The
launcher prints START/DONE progress and elapsed time; completion records retain
per-job timings and output hashes. There is no measured full-run ETA yet. Estimate
it from a pilot of the same adapter and worker count; rare signals, action
exclusions, cached prices and different history lengths make linear extrapolation
only approximate. Outcome evaluation may dominate even when detection scales well.

Rerun the **same command and output directory** to resume. Completed outputs are
checksum-verified and skipped; failed/incomplete jobs are retried. Changing worker
count is allowed, but changing dates, sample, scope, shard count, sample files or
recorded runner code requires a new output directory. An existing unrelated
nonempty directory is refused. No original experiment artifacts are overwritten.
The checks cover the saved plan, coverage/event/output files, and the relevant
equity/research modules and entry scripts, including outcome and repository code.
Keep the virtual environment and dependencies unchanged during a batch. The
database readers use the common observed/created cutoff and retained immutable
revision IDs; they do not claim a complete physical database snapshot.

Use a fresh directory for strict runs. An older launcher plan cannot be resumed
as a strict plan. New event/policy identities preserve old evidence and outcomes;
starting a new directory alone was not sufficient in the legacy workflow.

The first full-period launch exposed a previously audited universe count mismatch:
March 4, 2024, original run `f14261d2-dbb3-5ffa-af7c-2085aff82ab3` declares 1,504
members but stores 1,503. The earlier exploratory contract retained the stored
population and flagged this discrepancy; the strict runner initially failed to
carry that exception forward. No jobs completed in the failed batch.

Strict exploratory replay now accepts only that exact original run, date, policy
hash and count pair. It does not edit the database, fabricate the missing member,
skip the date, accept DEGRADED/corrected revisions, or relax other completeness
checks. The discrepancy is part of the checksummed universe manifest and is
included in the batch scorecard's eligibility-coverage notes. It is not certified
complete membership. The default repository reader still rejects the mismatch.

Validation after this correction covered all 1,002 requested universe sessions
and a two-ticker replay across March 1-8, 2024 (4 events, 12 ticker-date records).
The launcher's terminal error now includes the child's last error line and log
path. A START for a queued job immediately after a failure does not indicate
extra concurrency; remaining children are cancelled when the failure is received.

To retry this failed launch, preserve its original cutoff and logs, but use a new
output directory because the corrected code fingerprint differs:

```powershell
$previous = Get-Content -Raw .\backend\backups\equity-signal-backtests\all-daily-strict-v1\plan.json | ConvertFrom-Json
$python = ".\backend\.venv\Scripts\python.exe"
$runner = ".\backend\scripts\run_daily_strategy_backtests.py"
$batch = @("--scope", "all-adapters", "--sample", "both",
           "--workers", "4", "--shards", "4",
           "--source-cutoff", $previous.source_cutoff,
           "--output-dir", ".\backend\backups\equity-signal-backtests\all-daily-strict-v2")
& $python -u $runner @batch --stage replay --execute
```

After successful replay, run the outcome stage with the same `$batch`. After
outcomes complete, use `--batch-dir` pointing to `all-daily-strict-v2` for the
scorecard. Do not delete or modify the failed batch's frozen plan to resume it.

Ctrl+C cancels this launcher's children; a hard process termination can leave
`.batch.lock`. Check its recorded PID and any surviving child processes before
removing a stale lock manually. Outcomes already committed are not rolled back
across the whole batch; reruns use the existing deterministic persistence rules.
A completed outcome stage is skipped on resume, so later maturity updates require
an explicit rerun of the underlying outcome command retained in `plan.json`.

#### Evaluate The Results

After outcomes finish, write a **new** retained-ledger report without replacing
the initial audit:

```powershell
& $python -u .\backend\scripts\report_equity_signal_scorecard.py `
  --batch-dir .\backend\backups\equity-signal-backtests\all-daily-strict-v1
```

The JSON and CSV are written inside the batch directory. `--batch-dir` verifies
completed artifacts, reads only the batch's exact evidence and outcome-policy
IDs in 5,000-subject chunks, and includes the declared evaluation cells for all
adapters selected in that batch, including zero-event cells. Eligibility coverage
is attached per sample/adapter. No existing published grade is inherited. The
original unscoped audit command remains available with its original safety bounds;
those bounds have not been increased or bypassed for a global ledger scan.

These commands fill **existing-adapter event/outcome coverage**. They do not yet
produce the matched no-signal/momentum control panel described above, certify
all merger settlements, adopt the earlier portfolio study's exact price cutoff,
or calibrate individual probabilities. They now retain the dated eligibility
records needed to construct those controls without repeating detection. Each
adapter's existing action exclusions remain unchanged; strict identity/path
checks may additionally classify defective inputs as unavailable. Therefore the
generated data is input to the next evaluation, not automatic evidence of a
tradable edge. Shared input loading across separate adapter processes remains an
optional optimization; no unmeasured speedup is claimed.

### Initial Audit Artifacts

- [Full coverage and provenance JSON](equity_signal_scorecard_results.json)
- [Flat daily scorecard CSV](equity_signal_scorecard_results.csv)
- [Declared report configuration](equity_signal_scorecard_config.json)
- [Report command](../backend/scripts/report_equity_signal_scorecard.py)

Config SHA256: `69b3d7d203f3a3ddf83e45a69c8661b3271f7f94243c13f7f5892405c4579598`.

Run the existing VS Code task **Report equity signal coverage and daily evidence**,
or from the workspace root:

```powershell
& .\backend\.venv\Scripts\python.exe -u .\backend\scripts\report_equity_signal_scorecard.py
```

The JSON contains the exact evidence/outcome IDs and hashes and is intentionally
large; use the CSV for inspection. `--no-write` previews without writing artifacts.
All reads use one `REPEATABLE READ, READ ONLY` transaction with bounded query times
and row limits. The report aborts on a breached limit rather than silently truncating.

**12 focused tests passed**, covering the registry, policy revision/mode/horizon
separation, reconstructed versus live policy application, missing denominators,
lifecycle deduplication, causal outcomes and storage-recovery identity/hash guards.
Artifact checks verified config/CSV hashes, 312 registry cells, 504 unique scorecard
keys, count conservation, null probability/quality outputs and database read-only mode.