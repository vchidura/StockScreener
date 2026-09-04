# Stock Screener Canonical Deployment

This guide is the production source of truth for the immutable equity platform.
Fresh PostgreSQL 17 databases are initialized from `000_canonical_schema.sql`.
Legacy price, scanner-event, archive, and cutover-only relations are not part of
the final schema.

## Runtime Topology

| Service | Compose profile | Responsibility |
|---|---|---|
| `db` | default | PostgreSQL 17 and transactional baseline initialization |
| `backend` | default | FastAPI canonical read API |
| `frontend` | default | Nginx frontend and same-origin `/api` proxy |
| `equity-universe-bootstrap` | `equity` | Idempotent Polygon stock/ETF universe discovery |
| `equity-worker` | `equity` | REST ingestion, derivation, analysis, and publication |
| `equity-portal-worker` | `equity` | Generation-aware portal snapshots |
| `option-worker` | `options` | Delayed option ingestion, factor analysis, strategies, and recommendation publication |
| `equity-migrate` | `maintenance` | One-shot migrations with the bootstrap administrator |
| `equity-stream-worker` | `equity-stream` | Reserved Advanced stream; keep disabled |

Options remain a read-only research surface. Do not enable equity option
context, raw option archival, broker execution, or the Advanced stock stream
until their separate acceptance gates pass.

## Required Environment

Copy the tracked template and edit only the ignored file:

```powershell
Copy-Item .\backend\.env.example .\backend\.env
```

Never commit `backend/.env`. The validator reports presence and state without
printing credentials or API keys.

### Core Settings

| Variable | Requirement |
|---|---|
| `APP_ENV` | `development` locally; `production` for deployment |
| `DB_NAME` | Application database name |
| `DB_USER` | Restricted runtime login; never a production superuser |
| `DB_PASSWORD` | Strong runtime-role password |
| `DB_HOST` | `127.0.0.1` locally; Compose overrides it to `db` |
| `DB_PORT` | Native listener port; Compose overrides it to `5432` |
| `POLYGON_API_KEY` | Required for equity ingestion and options metadata |
| `CORS_ORIGINS` | Comma-separated exact browser origins |

Use the deployed HTTPS origin in production CORS. Development validation
requires both `http://127.0.0.1:5174` and `http://localhost:5174`.

### Compose Bootstrap Settings

These are used only by PostgreSQL bootstrap and the migration service:

| Variable | Requirement |
|---|---|
| `POSTGRES_ADMIN_USER` | Bootstrap administrator, normally `postgres` |
| `POSTGRES_ADMIN_PASSWORD` | Strong password distinct from the runtime password |

`POSTGRES_ADMIN_USER` and `DB_USER` must differ. On an empty volume, Compose
applies the canonical baseline as the administrator, creates `DB_USER` as
`NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION`, and grants public-schema
data access without schema ownership.

### Canonical Worker Settings

The only common worker overrides are universe sizing and provider entitlement delay:

```env
EQUITY_UNIVERSE_TARGET_SIZE=350
EQUITY_UNIVERSE_LOOKBACK_DAYS=20
EQUITY_PROVIDER_DELAY_MINUTES=15
```

Intervals, polling, worker counts, publication grace, stale-run age, locks, and stream behavior
have tested defaults. Omit them unless a measured capacity or entitlement decision requires an
override. The `equity-stream` profile must remain disabled.

The options worker uses XNYS-open-anchored slots, processes the latest observable boundary, and
prioritizes any due durable retry from the current session. Keep provider delay explicit when it
reflects the subscription; other cadence values may use defaults:

```env
OPTION_PROVIDER_DELAY_SECONDS=900
```

Each due slot runs the configured option universe through references, chain
ingestion, normalization, local IV/Greeks, chain/expiration analysis, strategy
context, six strategy modules, payoff/scenarios, and atomic persistence.
`option_strategy_candidates` retains every selected/suppressed/rejected decision;
selected structured candidates publish `option_signal_events` and ordered legs for
the Recommendations portal. Read-only mode prevents broker execution.
The worker maintains only current and next month option partitions once per UTC month through the
baseline's constrained security-definer function; the runtime role retains no general schema
creation privilege.

### Canonical Reads

Canonical setup, Pattern Watch, portal snapshot, and scanner-page reads default to enabled and do
not need `.env` entries. The readiness validator rejects an explicit `false` override.

### Option Safety Gates

Keep these values until the option platform is explicitly promoted:

```env
OPTION_DATA_ENGINE=polygon_developer
OPTION_EXECUTION_ENGINE=paper_proxy
OPTION_START_READ_ONLY=true
OPTION_EQUITY_CONTEXT_ENABLED=false
OPTION_RAW_ARCHIVE_ENABLED=false
```

The remaining option universe, policy, cadence, and rate settings are listed
in `backend/.env.example` and forwarded to the API container.

## Preflight Validation

With the target database reachable through `backend/.env`, run:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_cutover_environment.py
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_equity_storage.py
```

Expected results:

- Environment status is `PASS`.
- Production reports `role_is_superuser: false`.
- All materialized flags and option guards pass.
- All eight canonical intervals contain 386 current members.
- All 20 portal snapshots are fresh.
- Analysis evidence and current projection mismatch lists are empty.
- Retired legacy price, scanner-event, and cutover-only relations are absent.

The current development database may warn that its local role is a superuser.
That warning is permitted only while `APP_ENV=development`.

## Production Compose Startup

Set `APP_ENV=production`, use a restricted `DB_USER`, and run from the
repository root:

```powershell
docker compose --env-file backend/.env --profile equity up -d --build
docker compose --env-file backend/.env --profile options up -d --build
docker compose --env-file backend/.env ps
```

Do not add `--profile equity-stream`.

The baseline and role scripts run only when `postgres_data` is first initialized.
The equity profile runs Polygon universe discovery only while `selected_tickers`
is empty. For long-range historical ingestion before continuous workers start,
follow [Fresh Database Setup](FRESH_DATABASE_SETUP.md). Treat volume deletion as
destructive and take a verified backup first.

Validate inside the deployed API container:

```powershell
docker compose --env-file backend/.env exec backend `
  python scripts/validate_cutover_environment.py

Invoke-RestMethod http://localhost:8001/api/health
```

`/api/health` must report `healthy`, all canonical storage fields must be
`true`, and `restricted_role_ready` must be `true` in production.

## Schema Installation

Runtime services cannot alter schema. The maintenance service applies the
baseline to an empty database, records an already-complete schema, or exits
without work when the baseline version is present:

```powershell
docker compose --env-file backend/.env --profile maintenance run --rm equity-migrate
docker compose --env-file backend/.env restart backend equity-worker equity-portal-worker
```

It rejects partially initialized databases. Never grant the runtime role schema
ownership merely to make installation convenient. Future schema changes must be
added as reviewed migrations after the baseline rather than editing a deployed
database manually.

## Native Development Startup

For Windows development, use the DB host and port in `backend/.env`, then start
these processes in separate terminals:

```powershell
# API
.\backend\.venv\Scripts\python.exe -m uvicorn main:app `
  --app-dir backend --host 127.0.0.1 --port 8001

# Canonical materialization worker
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_equity_worker.py

# Portal snapshot worker
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\refresh_equity_portal_snapshots.py --continuous

# Delayed option analysis and recommendation worker
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_option_worker.py

# Frontend
Set-Location frontend
npm.cmd run dev
```

Do not start the removed legacy scheduler. The canonical equity worker owns
ongoing equity ingestion and materialization.

## Backup and Restore Verification

Create a new versioned full dump; do not overwrite the previous known-good
dump:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\backup_database.ps1 `
  -Version fresh-canonical -Mode Full
```

The backup script writes a `.partial` file, validates its catalog, atomically
publishes the dump, and prints its SHA-256 checksum.

Restore it into a temporary isolated database and compare critical canonical,
research, portal, and option table counts:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\verify_database_backup.ps1 `
  -BackupPath C:\Backups\StockScreener\stocks_db_backup_current.dump
```

The verifier also runs the canonical storage validator inside the temporary
database and always drops that database in a `finally` block. Retain the
checksum, `CANONICAL_RESTORE_VALIDATED`, and `RESTORE_VERIFIED` output with
deployment evidence.

## Frontend and Network

Production frontend builds use `VITE_API_BASE_URL=/api`. Nginx proxies that
same-origin path to `backend:8001`, so no browser-visible backend hostname or
API secret is required. Terminate TLS at the deployment ingress and set
`CORS_ORIGINS` to exact allowed origins.

Expose PostgreSQL only when operationally required. The API port may also be
kept private when all traffic enters through frontend Nginx or another reverse
proxy.