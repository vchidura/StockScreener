# Fresh Database Setup

This is the authoritative database initialization path. The repository contains one schema file:
`backend/migrations/000_canonical_schema.sql`. It creates the final canonical equity, research,
portal, and options schema without legacy price or scanner tables and without market data.

## Safety

Creating a fresh database is destructive when an existing PostgreSQL volume or database is
removed. Keep the verified external backup until the replacement database has completed storage,
API, and restore validation. Database dumps are operator media and must not be committed.

Do not run the API or workers against a partially initialized database. Runtime roles cannot create
or alter schema.

## Native or Docker Compose

Both modes run the same Python code, PostgreSQL 17 schema, Polygon requests, and statistical
computations. Docker Compose is provided for reproducible versions, isolated services, restart
policies, health checks, and first-boot ordering. It is not a performance requirement.

On Windows, Docker Desktop runs Linux containers in a WSL2 virtual machine. PostgreSQL on a Docker
named volume can have predictable Linux filesystem behavior, but container/VM overhead and Windows
bind mounts can offset that benefit. Polygon network latency, PostgreSQL writes, and analysis work
dominate this application; do not assume Docker is faster without measuring the same ingestion on
the same machine. Native PostgreSQL 17 is a valid and usually simpler choice for a staged local
rebuild.

For the safest rebuild, use a new database name such as `stocks_db_fresh`. Keep the current
database and external backup unchanged, ingest and validate the new database, then cut over by
changing `DB_NAME`. A name that does not yet exist is recommended, but the initializer also accepts
an existing empty database or an already complete canonical database. It rejects partial schemas.

## Docker Initialization

An empty `postgres_data` volume initializes in this order:

1. PostgreSQL 17 creates the database.
2. `000_canonical_schema.sql` installs the final schema transactionally.
3. `01_configure_app_role.sh` creates the restricted runtime role and grants data access.
4. `equity-universe-bootstrap` discovers the initial stock and ETF universe from Polygon when
   `selected_tickers` is empty.
5. Equity workers may start after universe discovery completes successfully.

The option worker maintains current and next month market-data partitions once per UTC month
through a constrained security-definer function. The runtime role cannot create arbitrary tables
or partitions.

Configure `backend/.env`, then create a new volume:

```powershell
# Stop old containers but preserve their database volume.
docker compose --env-file backend/.env down

# A different project name creates a separate postgres_data volume.
$ComposeProject = "stockscreener-fresh"
docker compose -p $ComposeProject --env-file backend/.env up -d db
docker compose -p $ComposeProject --env-file backend/.env `
  run --rm equity-universe-bootstrap
```

Use the same `-p $ComposeProject` on later Compose commands. The old project volume remains
available until you deliberately remove it. `docker compose down -v` is destructive and is not the
recommended staging path.

The universe bootstrap defaults to 350 common stocks plus the curated ETF set. Configure it with:

```env
EQUITY_UNIVERSE_TARGET_SIZE=350
EQUITY_UNIVERSE_LOOKBACK_DAYS=20
```

`--if-empty` makes the Compose bootstrap idempotent; it performs no provider calls when an active
universe already exists.

## Native PostgreSQL Initialization

Native PostgreSQL is fully supported; Docker is only a reproducible process supervisor and first-
boot convenience. Configure both the bootstrap administrator and restricted runtime role in
`backend/.env`:

```env
APP_ENV=development
DB_NAME=stocks_db_fresh
DB_USER=stock_screener_app
DB_PASSWORD=<runtime-role-password>
DB_HOST=localhost
DB_PORT=5432
POSTGRES_ADMIN_USER=postgres
POSTGRES_ADMIN_PASSWORD=<administrator-password>
POLYGON_API_KEY=<polygon-key>
CORS_ORIGINS=http://127.0.0.1:5174,http://localhost:5174

EQUITY_PROVIDER_DELAY_MINUTES=15
OPTION_PROVIDER_DELAY_SECONDS=900
OPTION_START_READ_ONLY=true
OPTION_EQUITY_CONTEXT_ENABLED=false
OPTION_RAW_ARCHIVE_ENABLED=false
```

Configuration policy:

| Category | Keep in `.env` | Reason |
|---|---|---|
| Required runtime | `APP_ENV`, `DB_*`, `POLYGON_API_KEY`, `CORS_ORIGINS` | Machine, credential, and deployment-specific values |
| Initialization only | `POSTGRES_ADMIN_USER`, `POSTGRES_ADMIN_PASSWORD` | Database/schema/role creation; never used by runtime workers |
| Provider contract | Equity and option provider delays | Must match the subscribed Polygon entitlement |
| Safety gates | Option read-only, equity-context, and raw-archive flags | Make intentionally disabled capabilities explicit |
| Material model assumptions | Starting cash and risk-free rate/source when overriding defaults | Changes option outputs and configuration fingerprints |
| Optional tuning | Worker counts, polling, grace periods, stale timeout, intervals, lock names, policy paths, fixed underlyers | Tested defaults already exist; override only for a measured operational reason |

Canonical read flags default to enabled and do not need entries. Removing a default from `.env`
does not remove flexibility: every documented tuning value can still be supplied as an override,
and Docker Compose also supplies matching defaults when the variable is absent.

`DB_USER` and `POSTGRES_ADMIN_USER` must differ. Do not put either password on the command line.
`POSTGRES_ADMIN_USER` must already exist in PostgreSQL 17, normally as the administrator created
during PostgreSQL installation. The initializer creates or hardens `DB_USER`; no manual role SQL is
required.
If the same checkout currently runs the old API or workers, stop them before changing
`backend/.env`; existing processes retain old settings while newly launched scripts read the new
database name.
Create or adopt the database, install the baseline, and configure the restricted role:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\initialize_database.py
```

The command is nondestructive by default. It returns the database state and one of these baseline
states:

- `APPLIED_TO_EMPTY_DATABASE`
- `ALREADY_APPLIED`
- `ADOPTED_EXISTING_SCHEMA`

It rejects partially initialized schemas. To intentionally replace an existing database, stop the
API and all workers first, keep the external backup, and provide the exact database name twice:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\initialize_database.py `
  --recreate --confirm-database-name stocks_db_fresh
```

`--recreate` terminates sessions and drops `DB_NAME`; a missing or different confirmation fails
before destructive work. After installation, every API, universe, ingestion, research, and worker
process uses the restricted `DB_USER` from `backend/.env`.

When `DB_NAME=stocks_db_fresh` does not exist, run the initializer without `--recreate`. Reserve
`--recreate` for discarding an earlier attempt at that same staging database. Do not point it at the
current `stocks_db` unless destroying the current database is the explicit intent.

Discover the initial universe before starting the equity worker:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\discover_universe_polygon.py `
  --dry-run --target-size 350 --lookback-days 20

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\discover_universe_polygon.py `
  --if-empty --target-size 350 --lookback-days 20
```

This selects the configured number of liquid active US common stocks plus curated ETFs; it does
not ingest every Polygon-listed security. Review the dry run before persisting a new universe.

## Manual Bootstrap Driver

The detailed commands below can be run one by one, or planned and executed through the resumable
driver. Plan mode is the default and does not run subprocesses:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\bootstrap_fresh_data.py `
  --confirm-database-name stocks_db_fresh `
  --history-start 2021-09-01 `
  --intraday-start 2026-06-01 `
  --end 2026-09-01 `
  --phase all --include-fundamentals
```

Review the printed commands, then append `--execute`. The driver initializes the nondestructive
schema path, creates the universe only when empty, ingests and checks native bars, derives aggregate
history, publishes, generates current cross-sectional/discovery state, analyzes, refreshes
snapshots, and validates. It does not start continuous workers.

Phases are independently resumable:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\bootstrap_fresh_data.py `
  --confirm-database-name stocks_db_fresh `
  --history-start 2021-09-01 `
  --intraday-start 2026-06-01 `
  --end 2026-09-01 `
  --phase native-bars --phase coverage --execute
```

Add `--include-research-inputs` with `--phase all`, or select `--phase research-inputs`, only when
the point-in-time scanner research dataset is required. The destructive `--recreate` operation is
intentionally available only through `initialize_database.py`, not through the ingestion driver.

## Portal and Chart History

Stop continuous equity workers while running a historical bootstrap because all commands share the
equity advisory lock. Choose explicit ranges that fit the Polygon entitlement and desired storage
policy. A practical setup retains the longest history at 30-minute resolution and uses shorter
ranges for dense minute data.

### Why 30-Minute History Is Long-Lived

The canonical derivation chain is:

```text
30m -> 1h
30m -> 1d -> 1wk
          -> 1mo
```

`30m` is the Polygon-native long-history root. The `1h`, `1d`, `1wk`, and `1mo` bars
are computed from canonical sources and persisted as separate immutable rows, so APIs and scanners
read materialized interval rows rather than aggregating during a request. Normal hourly and daily
materialization does not fetch Polygon `1h` or `1d` bars for comparison. Native-versus-derived
monthly reconciliation is an explicit `--reconcile-monthly` operation; stream-versus-native
intraday reconciliation is a separate `--reconcile` operation.

Persistence is content-idempotent. Repeating a native fetch with the same Polygon payload or a
derivation with the same source revision IDs inserts nothing. A changed native payload creates a
new immutable native revision; deriving from changed source revision IDs creates a new immutable
higher-interval revision. This is revision selection, not a price-only `R2` comparison, and ordinary
native/derived revisions do not populate a sequential `R2` label. Explicit reconciliation creates
a `RECONCILED` revision labeled `MATCHED`, `CORRECTED`, or missing-source as appropriate.

Each derived bar records the exact source revision IDs. Using the same finalized, XNYS-session-
bounded `30m` revisions for intraday and higher-timeframe bars provides consistent OHLCV values,
early-close handling, correction propagation, and auditable lineage. It also preserves enough
source history for future point-in-time `30m` and derived `1h` research.

### Research Retention: Keep The Verdict, Discard The Bulk

A study's cost is dominated by rows nobody reads twice. The intraday study held 327,553 evidence
rows and 1,885,969 outcomes to support 108 qualification revisions - roughly 20,000 rows per
published verdict. Those verdicts are what the UI reports; the rows underneath exist to compute
them once.

The retained record is `equity_qualification_revisions`. `qualification_report()` and
`event_summary()` read only that table joined to `equity_outcome_policies`, so both survive a purge
of evidence and outcomes intact. The live-signal views (`recent_events`, `latest_ticker_signals`,
`ticker_events`, `pending_outcome_counts`) read production evidence, which research purges never
touch.

Purging is therefore the normal end of a study, not an exception:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\purge_research_scanner_data.py `
  --source-prefix INTRADAY_ --source-prefix CONTROL_ --apply
```

Keep `--drop-qualification` off. Dropping it deletes the verdict too, which is only correct when
discarding a study judged invalid.

Two properties make this safe to repeat. Event and evidence identifiers are deterministic `uuid5`
hashes, so re-running reproduces identical rows from the retained events file rather than
duplicating. And `--exclude-production` scopes deletion to rows with no analysis run, which is
required for composite scanners because the live pipeline and research replay share their source
names; without it the script aborts rather than touch production lineage.

Because the supporting rows are discarded, the qualification revision has to stand alone.
`equity_qualification_metrics_v3` stores the cohort breadth and dispersion that previously could
only be recovered by querying the outcomes: `distinct_tickers`, `top5_concentration`,
`sector_alpha_t_stat`, the Wilson `hit_rate_ci_low`/`hit_rate_ci_high`, and the tested window as
`first_signal_time`/`last_signal_time`. Anything a report needs must be computed at qualification
time; after the purge it cannot be recovered at any price.

Retain per study, outside the database: the events JSONL, the outcome report, and the qualification
report JSON. They are the only reproducible input if a verdict is ever challenged.

### Recommended Retention Change For The Next Rebuild

Measured on the 2021-09-03 to 2026-09-02 build of 386 tickers, `30m` is 5,974,712 of roughly
12.7M bar rows and `equity_bar_revisions` totals 9,775 MB. Daily is only 459,759 rows, about 4%.
The intraday tail carries 95% of the storage while the qualified research to date is daily.

A `30m` fixed-cohort study of four predeclared intraday scanners over this history produced no
qualifying lane and proved statistically indistinguishable from a random-direction control. That
result is recorded in [INTRADAY_STRATEGIES_DESIGN.md](INTRADAY_STRATEGIES_DESIGN.md). It does not
prove intraday scanning is impossible, but it does mean five years of full-universe `30m` history
is not currently earning its storage.

Two facts constrain any retention change:

1. `1d`, `1h`, `1wk`, and `1mo` are all `DERIVED`; every daily row in the current build carries
   `source_bar_revision_ids` pointing at `30m`. Dropping `30m` history without another daily source
   removes the ability to derive or re-derive daily history at all.
2. The one untested explanation for the intraday null is the execution-lag convention, which
   requires `30m` history to test. Discarding all of it forecloses that experiment permanently.

Recommended shape for a rebuilt database:

- Ingest `1d` natively for the full history rather than deriving it, using
  `fetch_native_bars(interval="1d")` or grouped daily. Daily then stands alone as `NATIVE_REST`.
  Higher-interval selection already prefers `DERIVED` over `NATIVE_REST`, so a later derived daily
  revision would supersede the native one without a schema change.
- Retain full-history `30m` for a predeclared liquid subset of roughly 50 to 100 tickers rather
  than the whole universe. The measured power floor for an intraday study is about 50 tickers, so
  this preserves the lag experiment at roughly 775k to 1.55M rows instead of 5.97M.
- Retain full-universe `30m` only for a recent window, sized to prospective collection needs, and
  derive `1h` from it.
- Derive `1wk` and `1mo` from daily.

That shape lands near 1.5M bar rows instead of 12.7M, roughly an 88% reduction, and improves query
latency for a second reason: the canonical read is a `DISTINCT ON` with a multi-branch `CASE`
ordering whose sort cost scales with rows per ticker, so reducing per-ticker depth from about 16k
to 1.6k matters more than the raw row count.

### Split Adjustment Is Required Before Any Daily Study

The current build stores only `adjusted = false` bars and `equity_corporate_actions` is empty.
Because daily is derived from unadjusted `30m`, split discontinuities propagate into every higher
interval. A scan for single-session moves beyond -38% or +60% finds 78 occurrences across 64 of
386 tickers, including NVDA -89.9% (2024-06-10), AMZN -94.9% (2022-06-06), GOOGL -95.1%
(2022-07-18), TSLA -66.8% (2022-08-25), and WMT -66.1% (2024-02-26). Symbol changes contaminate
too: META shows +1395.7% on 2022-06-09, the FB ticker transition.

For daily research this is disqualifying rather than cosmetic. Splits read as catastrophic gap
downs, so gap, breakdown, SMA200-break, and EMA-stack detectors fire spuriously; ATR and EMA
windows stay corrupted for 14 to 55 bars; volume also splits, distorting relative-volume baselines
for 20 sessions; and any outcome spanning a split computes an approximately -90% return.

A rebuild should therefore:

- Backfill `equity_corporate_actions` from `fetch_splits` and `fetch_dividends`, which already
  exist in `equity/polygon.py`.
- Ingest a split-adjusted lineage. Polygon `adjusted=true` is split-only, not dividend-adjusted,
  which is the correct choice because splits are the discontinuity and dividend drops are
  economically real.
- Treat symbol changes as a separate gap. `normalize_corporate_actions` supports a `SYMBOL_CHANGE`
  type but no client method fetches them, so ticker transitions need an explicit source.
- Add a discontinuity check to acceptance validation. It should report zero split-sized moves
  other than genuine ones on the adjusted lineage.

`equity_canonical_bars` currently pins `adjusted = false`, though its `row_number()` partition
already includes `adjusted`. Research can read the adjusted lineage directly through
`list_final_after(..., adjusted=True)` without a migration. Flipping the view is a separate
decision: for current dates the adjustment factor is 1, so live output is unchanged, but historical
charts rebase and any historical comparison of an adjusted underlying against unadjusted option
strikes would misstate moneyness. Audit the options path before changing the view.

### Ingestion Constraints Found While Backfilling

These were discovered running the backfill against a live Polygon key on 2026-09-03. A rebuild
should plan for them rather than rediscover them.

**Daily cannot be fetched as a native aggregate.** `_NATIVE_AGGREGATES` in `equity/polygon.py`
contains `1m`, `5m`, `15m`, `30m`, and `1mo` only. `fetch_native_bars(interval="1d")` raises
`unsupported native interval`. This is deliberate: `30m` is the sole native root and daily is
always `DERIVED`. To obtain an independent daily lineage use `fetch_grouped_daily(session_date,
adjusted=True)` with `normalize_grouped_daily_bars(...)`, which is the designed daily path, already
accepts `adjusted`, and stamps `GROUPED_DAILY_EXACT_TICKER_V2`. That is also the lineage the
point-in-time replay pipeline requires, so ingesting it serves both purposes. It costs one API call
per session rather than per ticker.

**Polygon grouped daily is limited to a rolling five-year window.** The cutoff is not a fixed
calendar date; it tracks the current date. A backfill requesting 2021-08-01 through 2026-09-03 had
its first 25 sessions rejected with HTTP 403 and returned data from 2021-09-07, five years and a
few days before the run. Treat 403 as unavailable and record it, consistent with how financial
statements are handled elsewhere in this document; do not fabricate or interpolate the missing
sessions.

The consequence for a rebuild is that history is perishable. A rebuild performed later gets
strictly less history than one performed today, and no amount of retry recovers it. Ingest the
adjusted daily lineage early in a rebuild and never prune it, because the sessions it holds cannot
be re-fetched once they age out. Size horizon floors against the entitled window: at ~1,254
sessions a `21d` horizon yields ~59 independent periods against the 40-period floor, and `2d`
yields ~627.


**The Polygon client has no rate-limit handling.** `_request` raises on any non-200 and
`http_client.py` states explicitly that callers implement their own backoff. Both backfill scripts
therefore wrap fetches in exponential backoff for 429, 502, 503, and 504, and treat 403 as a
terminal entitlement signal rather than a retryable error. Any new ingestion script needs the same
treatment.

Scripts added for this work, both dry-run by default:

- `scripts/backfill_corporate_actions.py --start --end [--dividends] --apply`
- `scripts/ingest_adjusted_daily_bars.py --start --end [--from-reconstructed-universes] --apply`
- `scripts/check_bar_discontinuities.py [--adjusted]`
- `scripts/check_unindexed_foreign_keys.py`

Splits alone are sufficient for adjustment because Polygon `adjusted=true` is split-only.
Dividends are optional lineage and materially larger to fetch.

**Exclude ETFs and ETVs from research cohorts.** The 386-member universe contains 33 ETFs and 3
ETVs, and the ETF set includes SPY, QQQ, and the sector benchmarks themselves. Benchmarking those
against themselves falls back through `resolve_benchmark_ticker`, and GLD, SLV, and USO are
classified `Financials`, so commodity vehicles get benchmarked against XLF. `ingest_adjusted_daily_bars.py`
filters to `security_type = 'CS'` by default, leaving 350 names.

**Bar identity ignores `adjusted`, so the two lineages collide.** `normalize_grouped_daily_bars`
derives `bar_revision_id` from ticker, interval, bar start, source kind, availability mode,
normalizer version and a hash of the provider payload - but not from `adjusted`. The upsert in
`EquityBarRepository.persist` does the opposite: its conflict target is
`(ticker, interval, bar_start, session_scope, adjusted, source_kind, availability_mode,
payload_sha256)`, which includes `adjusted`. The schema therefore intends the two lineages to
coexist while the identity prevents it.

The practical effect: Polygon returns byte-identical payloads for `adjusted=true` and
`adjusted=false` on any ticker with no split, so the unadjusted twin misses the conflict target,
attempts a real insert, and fails on the primary key with `duplicate key value violates unique
constraint "equity_bar_revisions_pkey"`. Re-inserting the *same* lineage is properly idempotent.

Consequences for a rebuild: pick one daily grouped lineage and stay on it. Research should use the
adjusted one and skip `prepare_historical_signal_research.py --backfill-bars`, because
`ingest_adjusted_daily_bars.py` writes the same `GROUPED_DAILY_EXACT_TICKER_V2` /
`HISTORICAL_RECONSTRUCTED` rows that the replay contract requires. Do not resolve the collision
with a broader `ON CONFLICT DO NOTHING`: that reports an unadjusted ingest while silently retaining
adjusted rows, which is the lineage mixing this section exists to prevent. The durable fix is to
include `adjusted` in the identity, which renames every existing grouped-daily row and therefore
belongs in a rebuild rather than in a live database.

**Back-adjusted prices can exceed the column type.** Prices and volume are `numeric(20,8)`, so any
value at or above 10^12 fails with `numeric field overflow`. Cumulative reverse splits reach that
range: Mullen Automotive (`MULN`) has split roughly 1:1e12, so its 2021 sessions back-adjust to
about 17.5 trillion dollars per share. The series is arithmetically sound - returns are ratios -
but unstorable. `ingest_adjusted_daily_bars.py` skips those bars and reports them as
`bars_out_of_range` with a per-ticker breakdown, rather than rounding them into range. A ticker
listed there simply starts later in the adjusted lineage; treat a large count as a signal to
exclude that ticker from the study rather than as an ingestion failure.

**The point-in-time replay path still reads unadjusted bars.** Ingesting the adjusted lineage is
necessary but not sufficient. `prepare_historical_signal_research.py --backfill-bars` normalizes
grouped daily with `adjusted=False`, and the outcome path in `orchestration.py` calls
`list_final_after` without an `adjusted` argument, so it takes the `False` default. A daily study
run through this pipeline therefore measures split-contaminated returns even when an adjusted
lineage exists alongside. Reconstructed universes hold roughly 1,870 members per session against
the 350-name fixed cohort, so the exposure is several hundred splits rather than 59. Resolve this
before running a daily study: either plumb `adjusted` through the outcome path and ingest the
adjusted lineage for the full eligible union, or backfill reconstructed corporate actions and
exclude any subject whose horizon window contains a split. Contaminated returns are rare but
enormous, so they distort both the mean and the variance the t-statistic depends on.

The first option is now implemented. `evaluate_directional_outcomes` accepts `adjusted`, and
`run_historical_signal_outcomes.py` exposes it as `--adjusted`; the default stays `False` so live
paths are unchanged. The sequence for a daily study is therefore:

1. `prepare_historical_signal_research.py --persist ...` to build reconstructed universes.
2. `ingest_adjusted_daily_bars.py --from-reconstructed-universes --apply` so the adjusted lineage
   covers every ticker the study can reference, not just the live universe.
3. `run_historical_signal_outcomes.py --adjusted ...`.

Skipping step 2 while passing `--adjusted` yields missing paths rather than wrong ones, because
`list_final_after` finds no adjusted bars and the outcome is recorded `UNAVAILABLE`.

**Index the foreign keys that research deletes depend on.** PostgreSQL does not index foreign keys
automatically, so every delete on a parent row sequentially scans each child table whose
referencing column is unindexed. This database has 49 such foreign keys, and two of them made
research cleanup impossible: `equity_research_outcomes.supersedes_outcome_id` and
`equity_evidence.supersedes_evidence_id`, both self-referential against tables of 851 MB and
368 MB. A 1.9M row purge ran 2,847 seconds without completing, and after indexing the first the
delete simply stalled on the second, inside
`SELECT 1 FROM ONLY equity_evidence x WHERE $1 = supersedes_evidence_id FOR KEY SHARE OF x`.

`scripts/check_unindexed_foreign_keys.py` lists them, ordered by child table size. The six the
canonical schema now creates are the ones on the evidence and outcome delete paths; the rest are on
tables that research never deletes from and can stay unindexed rather than pay the write cost.
Creating them requires table ownership, which the application role does not have - use
`POSTGRES_ADMIN_USER` from `.env`.

**Acceptance test for the adjusted lineage.** After ingestion, run
`scripts/check_bar_discontinuities.py`, which reports session-over-session moves beyond -38% or
+60% and separates those coinciding with a recorded split from those that do not. The 2026-09-03
backfill produced 414,648 bars over 1,254 sessions for 350 common stocks and moved the counts from
78 gaps on 64 tickers, 49 of them split-coincident, to 28 gaps on 21 tickers with **zero**
split-coincident. Zero split-coincident gaps is the acceptance criterion; a nonzero count means the
adjustment did not apply.

The residual 28 are not defects, and a rebuild should expect them:

- Seven are ticker reuse or renames, identifiable because the ticker has a hole in its history:
  META 1,164 sessions against a 1,254 maximum (FB rename), BNY 1,183, B 1,182, COHR 1,207,
  SPCX 1,207, FISV 644, FIG 1,034. Polygon grouped daily is exact-ticker, so two different issuers
  share one symbol series and the seam reads as a price gap. `normalize_corporate_actions` models
  `SYMBOL_CHANGE` but nothing fetches it, so these cannot currently be repaired — treat a history
  hole plus a large gap as the detection rule and exclude the affected span from studies.
- The rest are genuine single-name moves, concentrated in small caps, biotech, and crypto miners
  (APLD, ASTS, BMNR, CIFR, CRDO, IREN, IONQ, AAOI, AXTI, INSM), plus real large-cap crashes
  (CVNA 2022, DXCM 2024-07-26, TTD 2025-08-08). Wide thresholds are expected to catch these; the
  test is for split contamination, not for outlier removal.


Five years is a retention and feature-coverage choice, not a schema requirement. It provides
roughly 1,260 daily bars, 260 weekly bars, and 60 monthly bars, which supports 252-session momentum,
daily SMA200, and weekly SMA200 calculations for securities old enough to have that history. A
shorter `30m` range can support current intraday analysis, but it cannot rebuild the same long
daily/weekly/monthly history in a fresh database. More `30m` history does not make an indicator
mathematically better; it improves coverage, cross-timeframe consistency, reproducibility, and
future research options at the cost of additional provider time and storage.

Two years of `30m` bars is more than the current scanners need for live calculations: current
intraday scanners use at most a few hundred recent bars, while daily and weekly scanners consume
their separately materialized `1d` and `1wk` rows. That does not make older `30m` source revisions
deletable under the canonical contract. Derived `1h` and `1d` rows retain the exact `30m` source
revision IDs used to compute them, and the storage validator requires every lineage ID to resolve.
The current database has about 3.52 million `30m` revisions older than two years. Deleting them
would leave retained higher-interval history numerically readable but unauditable, prevent exact
rebuilds, and fail canonical storage validation. Pruning requires a designed cold archive or
lineage-manifest tier plus validator and restore support; until then, retain the five-year `30m`
root.

Run one mutating command at a time. Before beginning, confirm that no materialization run or worker
lock is active:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\report_equity_analysis_status.py
```

```powershell
$HistoryStart = "2021-09-01"
$IntradayStart = "2026-06-01"
$EndDate = "2026-09-01"

# Persist canonical security references and an effective universe revision.
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_equity_materialization.py --reference --date $EndDate

# Optional Polygon financial statements. A 403 is recorded as unavailable, not fabricated.
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_equity_materialization.py --fundamentals

# Long history used to derive hourly, daily, weekly, and monthly bars.
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_equity_materialization.py `
  --bars --interval 30m --from-date $HistoryStart --date $EndDate

# Dense intraday history. Extend the start date only after capacity planning.
foreach ($Interval in @("1m", "5m", "15m")) {
  .\backend\.venv\Scripts\python.exe `
    .\backend\scripts\run_equity_materialization.py `
    --bars --interval $Interval --from-date $IntradayStart --date $EndDate
}

# Confirm persisted native coverage before deriving or publishing.
foreach ($Interval in @("1m", "5m", "15m", "30m")) {
  $StartDate = if ($Interval -eq "30m") { $HistoryStart } else { $IntradayStart }
  .\backend\.venv\Scripts\python.exe `
    .\backend\scripts\run_equity_materialization.py `
    --coverage-report --interval $Interval `
    --from-date $StartDate --date $EndDate
}

# Derive aggregate history from canonical source revisions.
# Run in this order because weekly/monthly bars consume the derived daily history.
foreach ($Interval in @("1h", "1d", "1wk", "1mo")) {
  .\backend\.venv\Scripts\python.exe `
    .\backend\scripts\run_equity_materialization.py `
    --derive-history --interval $Interval --date $EndDate
}

# Publish the latest complete/degraded cohort for every interval.

`--derive-history` must load the complete visible source range through `--date`; it must not use
the small recent-bar limits reserved for current analysis. After derivation, verify that the first
derived `1d` date is near `$HistoryStart`. If cross-sectional or discovery generation reports
insufficient data and logs only a few weeks of daily rows, rerun `1h`, `1d`, `1wk`, and `1mo` in
that order before generating signals or snapshots.
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_equity_materialization.py `
  --publish-bars --interval 1m --interval 5m --interval 15m --interval 30m `
  --interval 1h --interval 1d --interval 1wk --interval 1mo

# Populate current cross-sectional and discovery overlays used by sector/portal reads.
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\generate_cross_sectional_signal.py --date $EndDate
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\generate_market_discovery.py --date $EndDate

# Materialize current evidence and setups. Analysis intentionally excludes 1m.
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_equity_materialization.py `
  --analyze --interval 5m --interval 15m --interval 30m --interval 1h `
  --interval 1d --interval 1wk --interval 1mo

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\refresh_equity_portal_snapshots.py
```

These commands persist historical bars observed during this bootstrap as `LIVE_OBSERVED`. They are
valid for charts and current analysis, but they do not claim that the application possessed them
at their historical timestamps.

Each ingestion and derivation step is content-idempotent. If a process is interrupted, do not start
a parallel replacement. Confirm the old process is gone, terminalize work older than the configured
stale threshold, and rerun the same bounded command:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_equity_materialization.py `
  --recover-stale-runs --stale-after-minutes 60
```

Recent data may change at the provider; a later rerun can append a new immutable revision rather
than overwriting prior facts.

## Point-in-Time Scanner Research Inputs

Causal historical scanner replay is a separate, optional ingestion track. It reconstructs dated
universes, corporate actions, sector references, and grouped daily bars with explicit
`HISTORICAL_RECONSTRUCTED` availability. Do not substitute the portal history above for this
research contract.

Run a dry-run plan before persistence:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py `
  --dry-run --sessions 300 --bar-warmup-sessions 210 --end $EndDate
```

Then persist resumable point-in-time inputs:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py `
  --persist --sessions 300 --bar-warmup-sessions 210 --end $EndDate `
  --backfill-actions --backfill-bars --backfill-sector-references `
  --output .\backend\.cache\historical-signal-research\bootstrap-report.json
```

The ignored cache is resumable and can be deleted after required studies have published verified
qualification revisions. Signal replay and outcome publication remain explicit later steps; data
ingestion alone does not qualify any scanner.

## Options Data Scope

The option worker begins collecting coherent delayed chain/trade observations from its first
observable slot forward. The current Developer pipeline does not reconstruct a complete historical
option chain from equity bars. Keep `OPTION_START_READ_ONLY=true`; option-conditioning research
remains pending until enough coherent forward observations mature.

For Docker, run the ingestion commands through `docker compose run --rm backend`. Native and Docker
execution call the same Python services and persist the same schema contracts. Database/role
initialization differs: Docker uses first-boot scripts, while native setup uses
`initialize_database.py`.

## Start Continuous Services

After historical ingestion and initial publication, choose either native processes or Docker.

### Native Processes

Before starting continuous operation, one production-equivalent foreground sweep can verify the
currently due Polygon ingestion, canonical publication, outcome maturity, and analysis paths for
every configured interval:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_equity_worker.py --once
```

The command processes each configured interval once and exits. Do not run it while another equity
worker or mutating materialization command is active. One-shot mode is intentionally strict: a
provider request or greater-than-5% ingestion coverage failure exits nonzero after recording the
failed segment.

Run each long-lived command in its own terminal:

```powershell
# FastAPI
.\backend\.venv\Scripts\python.exe -m uvicorn main:app `
  --app-dir backend --host 127.0.0.1 --port 8001

# Canonical equity ingestion/materialization
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_equity_worker.py

# Portal snapshots
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\refresh_equity_portal_snapshots.py --continuous

# Forward-only delayed option research
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_option_worker.py

# Frontend
npm.cmd --prefix .\frontend run dev
```

On startup, the equity worker resumes from the latest durably published analysis watermark for
each interval and processes only intervals whose expected market boundary is newer. When an
intraday interval is overdue, Pattern Watch fails closed with HTTP 503 until that interval's bar
publication and full-universe analysis projection are atomically complete; retry after the worker
logs the corresponding `ingestion=... publication=... analysis=...` result. Keep the continuous
portal snapshot process running as well so overview and sector pages refresh after source changes.

In continuous mode, transient Polygon request failures and greater-than-5% ingestion coverage
failures do not advance the interval watermark or publish a partial cohort. The failed segment is
retained for audit, the current cycle stops, and the same due slot is retried after the normal
worker poll. Polygon authentication is sent through the `Authorization: Bearer` header so retry
URLs remain credential-free. Rotate any key that has previously appeared in console output.

Long-running cycles log each ingestion, publication, outcome-maturity, and analysis stage. Once
all due intervals are current, the resident process remains intentionally idle and emits a
watermark heartbeat every five minutes by default. Override that cadence with
`EQUITY_WORKER_HEARTBEAT_SECONDS` when operational monitoring requires a different interval.

The option worker has an equivalent one-slot delayed-data smoke test:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_option_worker.py --once
```

It processes the latest observable 15-minute-delayed slot for the configured option underlyers,
matures due follow-up outcomes, and exits. A per-underlyer `MODEL_QUALITY_FAILED` result is an
intentional fail-closed analytical outcome when IV or chain-quality thresholds are not met; it is
not an ingestion corruption. Keep `OPTION_START_READ_ONLY=true`, raw archive disabled, and equity
context disabled until their separate gates pass. Start continuous option collection with the same
script without `--once`.

### Docker Compose

```powershell
docker compose -p $ComposeProject --env-file backend/.env `
  --profile equity up -d --build
docker compose -p $ComposeProject --env-file backend/.env `
  --profile options up -d --build
```

Do not enable `equity-stream` until its separate entitlement and reconnect gate passes.

## Validation

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_equity_storage.py
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_cutover_environment.py
Invoke-RestMethod http://localhost:8001/api/health
```

The storage validator is expected to fail before initial bars, analyses, and all portal snapshots
have been published. That is a readiness signal, not permission to weaken its checks.

Before deleting the retained external backup, create and independently restore-verify a new backup
from the fresh database.
