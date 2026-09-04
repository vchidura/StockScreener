# Stock Screener Portal

A FastAPI and React research portal backed by immutable, revision-aware equity
bars, reproducible analysis evidence, and worker-published scanner views.

## Current Architecture

- PostgreSQL stores immutable bar revisions, publications, analysis runs,
  evidence, contexts, and current pointers.
- The equity materialization worker ingests Polygon REST bars and publishes
  `5m`, `15m`, `30m`, `1h`, `1d`, `1wk`, and `1mo` cohorts.
- The portal worker publishes 20 generation-aware snapshots so expensive GET
  routes remain read-only and fast.
- A single transactional baseline creates the final schema for fresh databases;
  legacy price and scanner relations are absent.
- Options remain read-only with equity context and raw archival disabled.
- The Polygon Advanced stream worker is defined but must remain disabled.

See [Equity Materialization Design](docs/EQUITY_ANALYSIS_MATERIALIZATION_DESIGN.md)
and [Option Chain Scanner Design](docs/OPTION_CHAIN_SCANNER_DESIGN.md) for system
design. The authoritative setup and production runbook is
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Prerequisites

- Python 3.11
- Node.js 20+
- PostgreSQL 17 client and server tools
- Docker with Compose for container deployment only
- A Polygon/Massive API key

## Environment

Create the ignored active environment from the complete tracked template:

```powershell
Copy-Item .\backend\.env.example .\backend\.env
```

At minimum, provide database connection values, `POLYGON_API_KEY`, and exact
`CORS_ORIGINS`. Canonical reads and worker cadence use tested defaults when
omitted. Keep these option gates explicit and unchanged:

```env
OPTION_START_READ_ONLY=true
OPTION_EQUITY_CONTEXT_ENABLED=false
OPTION_RAW_ARCHIVE_ENABLED=false
```

Use `APP_ENV=development` for the native local database. Production requires
`APP_ENV=production` and a restricted, non-owner, non-superuser `DB_USER`.
Fresh native or Compose initialization also requires separate
`POSTGRES_ADMIN_*` credentials. Database dumps are external operator media and
are intentionally not tracked or required for a fresh deployment.

Validate the active configuration without displaying secrets:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_cutover_environment.py
```

## Native Development

Install dependencies from the repository root:

```powershell
python -m venv .\backend\.venv
.\backend\.venv\Scripts\python.exe -m pip install -r .\backend\requirements.txt
Set-Location frontend
npm.cmd install
Set-Location ..
```

After PostgreSQL is running and `backend/.env` is configured, initialize the
database and selected universe once:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\initialize_database.py
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\discover_universe_polygon.py `
  --if-empty --target-size 350 --lookback-days 20
```

For a complete historical bootstrap before starting workers, follow
[Fresh Database Setup](docs/FRESH_DATABASE_SETUP.md). Its plan-first
`bootstrap_fresh_data.py` driver can execute the same ingestion phases manually
without Docker.

After bootstrap, start these in separate terminals:

```powershell
# API
.\backend\.venv\Scripts\python.exe -m uvicorn main:app `
  --app-dir backend --reload --host 127.0.0.1 --port 8001

# Canonical ingestion/materialization
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_equity_worker.py

# Portal snapshots
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\refresh_equity_portal_snapshots.py --continuous

# Delayed options ingestion, analysis, strategies, and recommendation publication
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_option_worker.py

# Frontend
Set-Location frontend
npm.cmd run dev
```

`run_equity_worker.py` performs ingestion, canonical publication, and the v16
analysis run for every due interval; do not launch a separate analysis job.
Monitor run/member status from another terminal without starting work:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\report_equity_analysis_status.py
```

`run_option_worker.py` performs delayed option-chain ingestion, normalization,
local IV/Greeks, chain and expiration analysis, six strategy modules, payoff and
scenario analysis, and atomic candidate/recommendation persistence. It runs on
XNYS-open-anchored 15-minute slots after the configured Developer delay. The
worker remains read-only for execution; only backend-published rows appear in the
Opportunity Board and Decisions views.

The default `/options` route opens the all-universe Opportunity Board. It presents
the leading persisted structure from each strategy and underlying separately from
research-only detector highlights. Ranking remains the backend strategy rank; the
portal does not fabricate a score across strategies. Raw contract research remains
available at `/options/research`. Decisions combines Candidate Audit and Signal Ledger;
the full matrix Explorer remains available contextually from Research rather than as a
primary tab.

Open `http://127.0.0.1:5174`. The Vite development server proxies `/api` to
`http://127.0.0.1:8001`.

Do not start a legacy scheduler or the Advanced stream worker.

## Historical Signal Research Inputs

The portal snapshot worker publishes current UI payloads only. Historical signal
studies use a separate signal-agnostic input builder that reconstructs the universe
effective on each trading session, applies a causal prior-session liquidity policy,
and records that the data was downloaded later for replay.

Run a non-persistent 100-session pilot first:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py `
  --dry-run --sessions 100 --end 2026-08-31
```

Persist resumable universe checkpoints and split/dividend facts:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py `
  --persist --sessions 100 --end 2026-08-31 `
  --backfill-actions `
  --output .\backend\.cache\historical-signal-research\pilot-100-report.json
```

The default `liquid_us_common_stocks_v2` policy admits historically active US common
stocks whose latest prior-session close is at least `$5`, whose median dollar volume
over the preceding 20 sessions is at least `$20M`, and whose lookback coverage is at
least 90%. Provider responses are checksummed under the ignored `backend/.cache`
directory, and identical reruns resume completed session checkpoints.

Version 2 preserves Massive ticker case while joining grouped bars to uppercase common-stock
reference symbols. This prevents preferred-share notation such as `BCpC` from colliding with the
different common stock `BCPC`. Version 1 is retained only as superseded audit evidence and its
grouped bars are excluded from canonical selection.

`--backfill-bars` is an optional second phase. It reuses the checksummed grouped-daily
cache to persist provider-native, unadjusted reconstructed daily bars for the eligible
union with exact XNYS session bounds and replay availability. It makes no additional
provider requests. Capacity-plan that phase after reviewing `eligible_union` in the
pilot report. The builder prepares reusable inputs; signal-specific event replay,
outcomes, and qualification remain separate versioned jobs.

Verify reconstructed-universe, corporate-action, daily-bar, and live-pointer
boundaries without starting work:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py --status
```

Run the bounded gap adapter against the qualification-sized 300-session input:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal gap-formation-v2 --start 2025-06-23 --end 2026-08-31 `
  --output .\backend\.cache\historical-signal-research\gap-formation-300-v2-summary.json `
  --events-output .\backend\.cache\historical-signal-research\gap-formation-300-v2-events.jsonl

# Future adapters implement HistoricalSignalAdapter and can be loaded as:
# --adapter package.module:adapter_instance
```

Adapters receive exact session membership, reconstructed daily bars, corporate-action
context, and deterministic event identity. Their JSONL output is immutable research
input, not a qualified outcome or a live recommendation. Gap adapter v2 evaluates the
formation bar plus exactly 20 prior bars so extending the research range cannot change
an overlapping event. Gap adapter v1 remains audit evidence only.

Persist event evidence, evaluate the predeclared 5/10/21-session next-open policy,
and run the 18-cell FDR family with an explicit reviewed timestamp:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --all `
  --events .\backend\.cache\historical-signal-research\gap-formation-300-v2-events.jsonl `
  --horizon-sessions 5 10 21 --round-trip-cost-bps 4 `
  --evaluation-version gap_formation_daily_qualification_v5_portfolio_metrics `
  --qualification-effective-from 2026-09-01T06:13:45Z

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --status `
  --source-version gap_formation_v2 `
  --evaluation-version gap_formation_daily_qualification_v5_portfolio_metrics
```

The three primary source lanes are breakaway hold, continuation hold, and fade
reversal. Direction and horizon create 18 qualification cells. Common, exhaustion-watch, and
unclassified formation controls are persisted as evidence but receive no outcome
policy. An identical run inserts no duplicate evidence, outcomes, or qualification
revisions. Qualification reads only evidence IDs declared by the supplied event file;
retained subjects from superseded replays cannot enter a newer cohort.

Run the separately versioned confirmation and first-entry fill studies against the same inputs:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal gap-breakaway-confirmation-v2 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\gap-breakaway-confirmation-300-events.jsonl

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal gap-entry-fill-v2 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\gap-entry-fill-300-events.jsonl
```

Confirmation evaluates 5/10/21 sessions after the first close beyond the formation extreme. Gap
entry evaluates 1/3/5/10/21 sessions after the first close inside a still-unfilled gap. Both retain
explicit stop/target path evidence and remain research-only.

Exact product-strategy studies keep the 20-session universe liquidity policy unchanged while
extending only the indicator bar/action warm-up:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py `
  --persist --sessions 300 --end 2026-08-31 `
  --bar-warmup-sessions 210 --backfill-actions --backfill-bars `
  --output .\backend\.cache\historical-signal-research\pilot-300-strategy-warmup.json

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal ma-crossover-9-21-v1 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\ma-crossover-9-21-300-events.jsonl

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal momentum-pullback-v2 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\momentum-pullback-300-events.jsonl

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal bearish-bounce-v2 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\bearish-bounce-300-events.jsonl
```

Momentum Pullback and Bearish Bounce reuse the exact page scanners on deterministic 210-bar daily
windows. Version 2 emits only the first session of each contiguous match episode. Grade and score
are retained as diagnostics; the primary studies do not select a grade after observing outcomes.

Evaluate the exact product cohorts as separate FDR families:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --all `
  --events .\backend\.cache\historical-signal-research\ma-crossover-9-21-300-events.jsonl `
  --horizon-sessions 5 10 21 --round-trip-cost-bps 4 `
  --evaluation-version ma_crossover_9_21_qualification_v2_bracket_aware `
  --qualification-effective-from 2026-09-01T14:34:05Z

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --all `
  --events .\backend\.cache\historical-signal-research\momentum-pullback-v2-300-events.jsonl `
  --horizon-sessions 5 10 21 --round-trip-cost-bps 4 `
  --evaluation-version momentum_pullback_episode_qualification_v3_bracket_aware `
  --qualification-effective-from 2026-09-01T14:34:10Z

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --all `
  --events .\backend\.cache\historical-signal-research\bearish-bounce-v2-300-events.jsonl `
  --horizon-sessions 5 10 21 --round-trip-cost-bps 4 `
  --evaluation-version bearish_bounce_episode_qualification_v3_bracket_aware `
  --qualification-effective-from 2026-09-01T14:34:15Z
```

## Production Compose

Set the production values documented in
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md), then run:

```powershell
docker compose --env-file backend/.env --profile equity up -d --build
docker compose --env-file backend/.env ps
```

The `equity` profile starts both canonical workers. Do not enable the
`equity-stream` profile.

## Verification

Run the storage validator after schema installation, ingestion, restoration, or
worker recovery:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_equity_storage.py
```

Expected state includes:

- Eight current interval cohorts with 386 members each.
- Twenty fresh portal snapshot types.
- No invalid bars, unresolved derived lineage, publication-count mismatch,
  analysis evidence mismatch, or current projection/run mismatch.
- No retired legacy price or scanner relations.

Run regression checks:

```powershell
.\backend\.venv\Scripts\python.exe -m pytest .\backend\tests -q
Set-Location frontend
npm.cmd run build
```

The API readiness endpoint is `GET /api/health`. In production it is healthy
only when canonical storage is ready, all 20 snapshot pointers match the source
generation, and the database login is restricted.

## Backup

Create and isolated-restore a versioned full backup:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\backup_database.ps1 -Version fresh-canonical -Mode Full

powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\verify_database_backup.ps1 `
  -BackupPath C:\Backups\StockScreener\stocks_db_backup_current.dump
```

Keep the previous known-good backup until the replacement database passes restore
verification. The verifier must report both `CANONICAL_RESTORE_VALIDATED` and
`RESTORE_VERIFIED`.

## Documentation

- [Deployment](docs/DEPLOYMENT.md)
- [Fresh database setup](docs/FRESH_DATABASE_SETUP.md)
- [Feature catalog](docs/FEATURES.md)
- [Strategies](docs/STRATEGIES.md)
- [Scheduler execution contract](docs/SCHEDULER_EXECUTION.md)
- [Scanner evaluation](docs/SCANNER_EVENT_EVALUATION.md)
- [Scanner research consolidation](docs/SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md)
- [Options pipeline state](docs/OPTION_PIPELINE_CURRENT_STATE.md)
- [Option chain implementation guide](docs/OPTION_CHAIN_SCANNER_IMPLEMENTATION_GUIDE.md)
- [Model registry](docs/MODEL_REGISTRY.md)