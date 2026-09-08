# Research Harness Enhancements

Work plan derived from operating the scanner research pipeline end to end on 2026-09-03/04: two
full runs of the `level_retest_rejection` daily study, one of which produced zero publishable
output. Every item below is something that actually happened, not a hypothetical.

Priorities are by consequence, not effort:

- **P0** produced, or would have produced, a wrong or empty result without saying so.
- **P1** cost hours of wall-clock time to a crash or a restart.
- **P2** is throughput.
- **P3** is methodology strength.
- **P4** is ergonomics and retention.

The architectural target - one manifest, one driver, `study_id` on every working row - is described
in the "Study Driver Simplification" section of
[SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md](SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md). This document
is the ordered backlog that gets there.

---

## P0 - Silent wrongness

### 1. Preflight the study before committing to a long run

The first full run spent about 90 minutes evaluating 1,019,802 outcomes and published **zero**
qualification revisions. Cause: benchmark ETFs were missing from the adjusted lineage, so every
`sector_net_alpha` was null and `dropna` emptied the frame. Nothing failed loudly until the very
end.

Add a `preflight` subcommand that refuses to start unless:

- every benchmark ticker resolves in the lineage the study will read;
- the cohort has bar coverage over the requested window in that lineage;
- the entitlement window covers the first requested session;
- the supporting foreign-key indexes exist;
- no policy conflict exists for the declared horizons.

Then, once evaluation begins, verify on the first batch that `sector_net_alpha` is actually
populated and abort within seconds if it is not. A 100-row check would have caught this instantly.

### 2. Benchmarks are not cohort members

`ingest_adjusted_daily_bars.py` filtered to `security_type = 'CS'`, which is correct for study
subjects and silently excluded SPY, QQQ and all twelve sector ETFs - the series alpha is measured
against. `--from-reconstructed-universes` compounded it, because the universe policy is
`security_types=("CS",)` so benchmarks were never candidates.

Fixed by always unioning `BENCHMARK_TICKERS` and hard-failing when a benchmark reference is
missing. Generalise the rule: any ticker required to *score* a study must be ingested regardless of
cohort filters, and its absence must be an error rather than a null.

### 3. Pin the bar lineage explicitly everywhere

`run_historical_signal_research.py` selected daily bars with no `adjusted` predicate. Because
`replay_available_at` is identical across lineages, `DISTINCT ON ... ORDER BY created_at DESC`
would have silently mixed adjusted and unadjusted bars across tickers once both existed - invisible
in any output.

Fixed with an explicit `adjusted = %s` and a `--adjusted` flag, and the run report now records
`bar_lineage`. Audit every remaining bar read for the same omission, and consider making `adjusted`
a required argument rather than a defaulted one.

### 4. Reconcile bar identity with the uniqueness constraint

`normalize_grouped_daily_bars` derives `bar_revision_id` without `adjusted`, while
`EquityBarRepository.persist` upserts on a key that *includes* `adjusted`. The schema intends both
lineages to coexist; the identity prevents it. Polygon returns byte-identical payloads for both
lineages on any unsplit ticker, so the second insert dies on the primary key.

Include `adjusted` in the identity. This renames every existing grouped-daily row, so it belongs in
a rebuild - see [FRESH_DATABASE_SETUP.md](FRESH_DATABASE_SETUP.md). Until then, one lineage per
study, enforced by preflight.

### 5. Do not let defaults write into the permanent record

`--evaluation-version` defaults to `gap_formation_daily_qualification_v1` and is stored verbatim in
the retained qualification revision. A composite study would have been published permanently
labelled as gap formation. `--source-version` has the same gap-shaped default, and
`--horizon-sessions` is accepted then ignored on the composite path.

A guard now rejects gap-prefixed evaluation versions for composite sources. Better: derive these
from the study manifest so they cannot be wrong, and reject arguments a code path ignores rather
than accepting them silently.

### 6. Declare the FDR family, and enforce it

`CompositeScannersDailyAdapter` emits all seven registered scanners. Qualifying the unfiltered
events file would have produced a 112-lane family instead of the declared 12, making a real effect
roughly seven times harder to detect - with no warning.

The runner filters events to the studied scanner at stage 5. Move this into the manifest so the
family size is declared, checked against what is actually present, and recorded in the published
metrics.

### 7. Escape literal `%` in parameterised SQL

`qualification_report` raised `IndexError: tuple index out of range` on every call with an interval
because `'CONTROL\_%'` contains a literal `%` that psycopg2 treats as a placeholder. The
scanner-research endpoint returned 500 to the UI. Needs `%%`, and a raw string so `\_` is not an
invalid Python escape.

Add a lint or test that exercises every parameterised query with arguments; the unit suite passed
throughout because nothing called this path with a database.

---

## P1 - Long runs that fall over

### 8. Retry transport failures, not just HTTP status codes

`prepare_historical_signal_research.py` contained no error handling whatsoever - a search for
`retry|backoff|HTTPError|except|sleep` returned nothing - across roughly 2,300 Polygon calls. It
died at universe 174 of 1,034.

The first fix caught only `requests.HTTPError`; the run then died again on a
`ChunkedEncodingError` when a connection dropped mid-stream. Retry `ChunkedEncodingError`,
`ConnectionError` and `Timeout` alongside 429 and 5xx, and treat 403 as terminal with an actionable
message.

Two related lessons worth keeping: retry code only executes when something has already gone wrong,
so it is never exercised on the happy path and needs its own test. And `time` in that module is
`datetime.time`, so `time.sleep` would have failed precisely when the retry mattered.

### 9. Guard the numeric range on adjusted prices

Cumulative reverse splits can push back-adjusted prices past `numeric(20,8)`. Mullen Automotive has
split roughly 1:1e12, so its 2021 sessions adjust to about 17.5 trillion dollars per share and the
ingest died with `numeric field overflow`.

Now skipped and reported as `bars_out_of_range` with a per-ticker breakdown rather than crashing or
rounding into range. Apply the same treatment to any other numeric column fed by provider data.

### 10. Never swallow a traceback

The study runner used `$ErrorActionPreference = 'Stop'` with `2>&1`, so the first stderr line
became a terminating error and killed the pipeline before `Tee-Object` wrote it. A failure at stage
1 looked like a clean finish at universe 1034 with no error anywhere in the log.

Fixed by scoping `ErrorActionPreference` to `Continue` around the native call and relying on
`$LASTEXITCODE`. Any future runner needs the same care.

### 11. Stream large intermediates

`load_events` read the entire events file with `read_text().splitlines()`, and
`publication_metadata` then read it again via `read_bytes()`. Against the 199 MB filtered file that
is roughly 1.3 GB of peak memory for no reason. Now streamed and hashed in chunks. Better still,
remove the file: keep events in the working table.

---

## P2 - Throughput

### 12. Batch the sector lookup

The outcome loop issues `SELECT * FROM equity_security_reference_revisions WHERE
security_revision_id = ...` once per subject. At 171,804 subjects across six policy/horizon
combinations that is a large multiple of a single batched query, and it is the main reason
evaluation takes about 90 minutes.

### 13. Fix the outcome path cache eviction

`outcome_path_cache_size` is 100,000, but a study of this shape needs roughly 515,000 distinct keys
- subject times three benchmark tickers. When full, the cache calls `.clear()` and discards
everything rather than evicting the oldest entry, so hit rates collapse. Use an LRU, and size it
from the study rather than a constant.

### 14. Index the foreign keys research deletes through

PostgreSQL does not index foreign keys automatically. A 1.9M row purge ran 2,847 seconds without
completing; after adding two indexes the same purge finished in 263 seconds.
`scripts/check_unindexed_foreign_keys.py` reports 49 such keys. Six are now in the canonical schema;
verify the set covers every delete path research uses.

---

## P3 - Methodology

### 15. Reproduce a known anomaly

The single highest-value validation, and not yet done. Run the harness over the same cohort and
window on a documented effect - 12-1 momentum or short-term reversal - and check it recovers the
published sign and rough magnitude.

If it cannot detect momentum, the harness has a defect. If it can, every `UNRANKED` verdict becomes
substantially more credible, because the machinery has been shown to find real effects in data
where the answer is externally known. This is stronger evidence than any additional statistic and
costs one study run.

### 16. Factor-adjusted alpha, not only sector-adjusted

Sector-ETF alpha does not remove size, value, momentum or short-term reversal loadings. A
level-retest scanner very likely loads on short-term reversal, which is a known and freely
harvestable factor. Regressing on Fama-French 5 plus momentum and reporting the intercept *and*
loadings is the institutional standard. Ken French's data library is free, so this is cheap.

### 17. Raise the hurdle, and account for the real search space

Harvey, Liu and Zhu argue `t > 3.0` for a new factor. The current gate is `t > 2` plus BH across the
declared family, but the declared family is far smaller than the search actually performed across
this codebase's history - seven scanners, four intervals, multiple versions. Consider a higher
hurdle, and implement the Deflated Sharpe Ratio and PBO via CSCV, which
[SCANNER_LITERATURE_REVIEW.md](SCANNER_LITERATURE_REVIEW.md) already acknowledges are absent.

### 18. Standard errors that respect overlap

Overlapping horizons induce autocorrelation across periods. Plain t-statistics assume independence
once signals are collapsed by instant. Newey-West or a stationary bootstrap is the standard remedy.

### 19. Costs beyond a flat 4 bps

No spread, market impact, borrow cost or capacity. The short lanes on small caps are the most
exposed: hard-to-borrow fees can invert a marginal result outright.

### 20. Port regime-conditioned alpha

[SCANNER_ENHANCEMENTS_BACKLOG.md](SCANNER_ENHANCEMENTS_BACKLOG.md) records this as implemented, but
that was in the retired `research/scanner_events.py`. It was lost in the migration and
`regime_alpha` is still returned empty. The prior implementation found that this very scanner had
positive alpha in trending regimes and negative alpha in choppy ones - exactly the kind of finding
the current runtime cannot reproduce.

---

## P4 - Retention and ergonomics

### 21. Stamp `study_id` on every working row

Purge is currently keyed by `source_name` prefix, which collides with production for every
composite scanner because live capture and historical replay share detector names. The guard
correctly refuses, so `--exclude-production` was added as a workaround. A `study_id` makes purge
exact, safe and independent of naming.

### 22. Per-study partitions

With `study_id` present, cleanup becomes `DROP PARTITION`: constant time, no foreign-key
revalidation, none of the index work in item 14.

### 23. Recomputation requires marking stale

`list_pending_directional_subjects` skips any subject holding a non-stale outcome, so correcting an
input is invisible until the affected rows are marked stale.
`scripts/mark_research_outcomes_stale.py` now does this by source name. Preflight should detect the
condition and tell the operator, rather than leaving a re-run to silently do nothing.

### 24. Version the study artifacts

`composite_qualification.json` is unversioned, so the next composite study overwrites it with no
supersession trail - unlike the database, which revisions. Include the scanner, version and report
identity in the filename.

### 25. Compute for the future at qualification time

Because working rows are discarded after publication, anything a later consumer needs must be in
`metrics` when the study runs. `equity_qualification_metrics_v3` added breadth, concentration,
Wilson hit-rate interval, sector t-statistic and the tested window. The option-relevant additions
in [OPTION_RESEARCH_DESIGN.md](OPTION_RESEARCH_DESIGN.md) - move-size distribution, realised versus
implied volatility, time to target - should be added *before* the next study we intend to keep,
otherwise every study between now and then is unusable for that purpose.

---

## Suggested order

1. Items 1-7. Nothing else matters if results can be silently wrong.
2. Item 15. Establish that the harness detects a known effect.
3. Items 8-11, then 21 and 23. Make long runs survivable and repeatable.
4. Items 12-14. Turn 90-minute evaluations into minutes.
5. Items 16-20 and 25. Strengthen the claim and future-proof the retained record.
6. Items 22 and 24. Cleanup ergonomics.
