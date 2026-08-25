# Stock Screener Portal

A full-stack stock screening application with multiple trading strategies.

## Features

- **Gap Strategies**: Identify unfilled gaps that act as support/resistance levels
- **MA Crossover**: Detect bullish/bearish moving average crossovers
- **RSI Signals**: Find oversold and overbought conditions
- **Volume Breakout**: Spot unusual volume spikes
- **Interactive Charts**: Click any ticker to view detailed candlestick charts

## Prerequisites

- Git, or another way to copy the repository to the new machine
- Python 3.10+ (Python 3.12 recommended)
- Node.js 20+
- PostgreSQL 17, including the `psql`, `createdb`, and `pg_restore` tools

Verify the installed tools in PowerShell:

```powershell
git --version
python --version
node --version
npm.cmd --version
```

## New Machine Setup (Windows)

These steps create a clean native development environment. Do not copy
`.venv` or `frontend/node_modules` from the old machine; they can contain
machine-specific executables and paths.

### 1. Transfer the Repository

Clone the repository or copy the complete `stock-screener-portal` folder,
including `backend/backups/stocks_db_backup_v4_2026-08-21.dump`, to the new machine.

The current full dump is about 101 MiB, which exceeds GitHub's 100 MB limit
for regular Git objects. Store it with Git LFS or transfer it separately rather
than committing it as a normal Git blob.

```powershell
git clone <repository-url>
cd stock-screener-portal
```

When copying the folder directly, open PowerShell in the copied repository root
before continuing.

On Windows you can install all three with winget (run from the repo root):

```powershell
winget install Python.Python.3.11
winget install OpenJS.NodeJS.LTS
winget install PostgreSQL.PostgreSQL.17 --custom "--superpassword <postgres_admin_password> --serverport 5432"
```

### 2. Create and Restore PostgreSQL

The bundled backup contains the schema and existing market data. The example
below assumes PostgreSQL 17 is installed in its default Windows location and
uses the `vamsh100` account created by the installer. Adjust the path, port, or
username when your installation differs.

```powershell
$pg = "C:\Program Files\PostgreSQL\17\bin"

# Confirm PostgreSQL is installed and running.
& "$pg\psql.exe" --version
Get-Service 'postgresql*'

# Create the app role and database (run as the postgres superuser)
$env:PGPASSWORD = "<postgres_admin_password>"
& "$pg\psql.exe" -U postgres -h localhost -c "CREATE ROLE vamsh100 LOGIN SUPERUSER PASSWORD 'password1234';"
& "$pg\psql.exe" -U postgres -h localhost -c "CREATE DATABASE stocks_db OWNER vamsh100;"

# Restore the data
$env:PGPASSWORD = "#password1234"
& "$pg\pg_restore.exe" -U vamsh100 -h localhost -p 5432 -d stocks_db `
	--no-owner --no-privileges ".\backend\backups\stocks_db_backup_v4_2026-08-21.dump"
```

Verify that the restore contains application tables and data:

```powershell
& "$pg\psql.exe" `
	--host localhost `
	--port 5432 `
	--username vamsh100 `
	--dbname stocks_db `
	--command "SELECT COUNT(*) AS tables FROM pg_tables WHERE schemaname = 'public';"

& "$pg\psql.exe" `
	--host localhost `
	--port 5432 `
	--username postgres `
	--dbname stocks_db `
	--command "SELECT COUNT(*) AS daily_rows FROM stock_prices_daily;"
```

### 3. Configure the Backend

Create a new virtual environment and install dependencies from the repository
root:

```powershell
python -m venv .\backend\.venv
.\backend\.venv\Scripts\python.exe -m pip install --upgrade pip
.\backend\.venv\Scripts\python.exe -m pip install -r .\backend\requirements.txt
Copy-Item .\backend\.env.example .\backend\.env
```

Edit `backend/.env` and use the same database settings used during the restore:

```env
DB_NAME=stocks_db
DB_USER=vamsh100
DB_PASSWORD=password1234
DB_HOST=localhost
DB_PORT=5432
TWELVEDATA_API_KEY=
```

Keep `backend/.env` private; do not commit database passwords or API keys.

### 4. Configure the Frontend

Install frontend dependencies from the repository root:

```powershell
cd frontend
npm.cmd install
cd ..
```

No frontend environment file is required for local development. Vite proxies
`/api` requests to the backend on port `8001`.

### 5. Open the Workspace in VS Code

```powershell
code .
```

In VS Code, run **Python: Select Interpreter** from the Command Palette and
choose `backend/.venv/Scripts/python.exe`. Open new integrated terminals from
the repository root so the paths below resolve consistently.

### 6. Run the Application

Start the API in the first PowerShell terminal:

```powershell
cd backend
.\.venv\Scripts\python.exe -m uvicorn main:app --reload --host 127.0.0.1 --port 8001
```

The API runs at `http://127.0.0.1:8001`; interactive API documentation is at
`http://127.0.0.1:8001/docs`.

Keep the API running and start the portal in a second PowerShell terminal:

```powershell
cd frontend
npm.cmd run dev
```

Open `http://127.0.0.1:5174`.

### 7. Verify the Application

Run these checks from another PowerShell terminal while both services are
running:

```powershell
Invoke-WebRequest http://127.0.0.1:8001/docs -UseBasicParsing |
	Select-Object StatusCode

Invoke-WebRequest http://127.0.0.1:5174 -UseBasicParsing |
	Select-Object StatusCode
```

Both requests should return status code `200`. Open a ticker page and confirm
that chart data loads to verify the frontend, API, and database together.

## Daily Startup

After the first-time setup, start PostgreSQL and run only these two commands,
each in its own terminal:

```powershell
# Terminal 1
cd backend
.\.venv\Scripts\python.exe -m uvicorn main:app --reload --host 127.0.0.1 --port 8001
```

```powershell
# Terminal 2
cd frontend
npm.cmd run dev
```

## Market Data Updates

`backend/scripts/run_scheduler.py` keeps the database current. Run it from the
repository root so the relative paths resolve.

### Create a full database backup

The backup script writes to a `.partial` file, validates the PostgreSQL catalog,
and only then publishes the final versioned dump:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
	.\backend\scripts\backup_database.ps1 -Version v4
```

Use a new version for each retained backup. Add `-Replace` only when intentionally
rebuilding the same version; the script refuses to run while another dump is writing it.

Create a complete empty portal schema containing all application tables, indexes,
constraints, and sequences but no rows:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
	.\backend\scripts\backup_database.ps1 -Version v2 -Mode SchemaOnly
```

Use this archive to bootstrap an empty database on another machine before regenerating
all configuration, market data, signals, and outcomes. Use the full backup when the target
must preserve current application data.

The narrower `MarketSchema` mode contains only the `selected_tickers`, daily, hourly,
and intraday schemas:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
	.\backend\scripts\backup_database.ps1 -Version v1 -Mode MarketSchema
```

Restore either schema archive into an empty database with `pg_restore`. Neither contains
ticker configuration, prices, signals, or outcomes.

### Run today's end-of-day update

The usual command. Run it after 16:00 ET on a trading day:

```powershell
.\backend\.venv\Scripts\python.exe -u .\backend\scripts\run_scheduler.py --eod-once
```

Use `-u` and avoid piping through `Select-String` or `Select-Object`; PowerShell
buffers piped output until the process exits, which hides progress and makes a
crashed run look identical to a healthy one. Redirect to a file instead when you
want a record:

```powershell
$env:PYTHONIOENCODING = 'utf-8'   # required: log lines contain non-ASCII characters
.\backend\.venv\Scripts\python.exe -u .\backend\scripts\run_scheduler.py --eod-once *> eod.log
```

`--eod-once` runs this sequence and exits:

| # | Stage | Notes |
|---|-------|-------|
| 1 | Daily close | Fetches today's OHLCV for every active ticker |
| 2 | Final hourly update | Tops up `stock_prices_hourly` |
| 3 | Final 5m intraday update | Tops up `stock_prices_intraday` |
| 4 | Validation | `validate_all(days=7)` checks the last 7 sessions |
| 5 | Conditional backfill | Runs **only** if validation reports issues |
| 6 | Daily-bar guard | Blocks stages 7–9 unless each daily bar envelops its own hourly tape |
| 7 | Cross-sectional signal | Scores the universe with `xsmom-1.0` |
| 8 | Market discovery | Persists shadow discovery states and the current-position overlay |
| 9 | Deep hourly backfill | **Saturday only** — repairs the 730-day window |

Stages 3, 7, 8 and 9 are wrapped in `try/except`: none of them are inputs to the
price tables written in stages 1–2, so a failure is logged without invalidating
the run.

Stage 6 exists because a skipped end-of-day overwrite is silent: the intraday
running-daily bar stays in place and looks official. A finalized daily bar always
contains its own session, so the guard fails any ticker whose daily high/low does
not envelop that session's hourly bars or whose volume falls below 90% of the
hourly total. It retries the daily close once for the offending tickers, then
queues anything still provisional in `data_ingestion_failures` and skips the
derived stages rather than building signals from mid-session prices.

The structured daily and hourly pullback scanners are **not** scheduler stages.
They are manual, descriptive watch scans shown later in this README.

### Options

| Flag | Purpose |
|------|---------|
| `--eod-once` | Run the post-close sequence once, then exit |
| `--tickers AAPL,MSFT` | Limit to specific tickers (default: all active) |
| `--provider polygon` | Data provider (`polygon`, `yahoo`, or `twelvedata`); `polygon` is the default |
| `--hourly-deep-once` | Repair eroded hourly history now, then exit |
| `--retry-daily-failures` | Retry unresolved official daily rows, then exit |
| `--force` | With `--hourly-deep-once`, refetch every ticker |

Repair a subset after a partial failure:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_scheduler.py --eod-once --tickers AMT,BWA,CCI
```

### Continuous scheduler

To keep data current automatically, run without flags in its own terminal. It
stays resident and fires each job on schedule:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_scheduler.py
```

### Hourly history maintenance

Yahoo serves 1-hour candles for a rolling 730-day window, so deep history erodes
as that window advances. The Saturday job repairs it, and only refetches tickers
that have fallen below 400 days — a healthy run skips almost everything:

```
[hourly-deep] 398/400 tickers have >=400 days; refetching 2
```

Trigger it manually with `--hourly-deep-once`.

> **Known issue:** `--hourly-deep-once` completes its work and logs `Done`, but
> the process does not always exit. Confirm it has terminated before using the
> flag in any automated wrapper.

### Verifying a run

Stage 4 validates all three price tables and reports per-table issues. Alongside
the row-level checks (duplicates, NULL/zero OHLCV, weekend rows, invalid times)
it checks for data that should be there but is not:

| Check | Catches |
|-------|---------|
| `missing sessions` | A trading day with no rows at all |
| `sparse sessions` | A partial write — rows exist, but most of the universe is absent |

Both compare a table against a sibling table rather than a weekday rule, so
market holidays are not reported as gaps and no holiday calendar is maintained.
Hourly is the reference for daily, and daily is the reference for the others.

```powershell
.\backend\.venv\Scripts\python.exe -c "import sys; sys.path.insert(0, r'.\backend'); import database; ctx = database.get_db_cursor(); cur = ctx.__enter__(); cur.execute('SELECT MAX(datetime)::date AS latest, COUNT(DISTINCT ticker) AS tickers FROM stock_prices_daily WHERE datetime >= CURRENT_DATE - 5'); print(dict(cur.fetchone())); ctx.__exit__(None, None, None)"
```

Tickers that consistently return HTTP 404 have usually been renamed or delisted;
mark them inactive in `selected_tickers` rather than letting them fail nightly.

### Structured pullback watch scan

Scan the daily universe for a recent SMA20/SMA50 cross, confirmed higher-high/lower-low
structure, pullback to support/resistance, and a reversal candle:

```powershell
.\backend\.venv\Scripts\python.exe -X utf8 -u `
	.\backend\scripts\scan_trend_pullback_daily.py
```

Results are labeled `UNVALIDATED_TIMING`. The pattern and its cross-sectional ranking overlay
did not clear the predictive significance gate, so these are review/watch candidates rather than
daily BUY/SHORT recommendations. See [docs/SIGNAL_RESEARCH.md](docs/SIGNAL_RESEARCH.md).

Scan tickers matching the latest stored regular-session 1-hour bar:

```powershell
.\backend\.venv\Scripts\python.exe -X utf8 -u `
	.\backend\scripts\scan_hourly_trend_pullback.py
```

Latest-hour results are also labeled `UNVALIDATED_TIMING` and must not be treated as automated
recommendations.

Daily/hourly scanner matches and their future outcomes are tracked by the shadow scanner-event
pipeline. See [docs/SCANNER_EVENT_EVALUATION.md](docs/SCANNER_EVENT_EVALUATION.md) for event
deduplication, outcome horizons, MAE/MFE, scheduler flow, portal display and promotion gates.
Friday EOD runs also capture completed weekly scanner bars as separately qualified shadow events.
Monthly scanning is deferred until sufficient monthly history is stored.

### Market discovery states

After a complete EOD close, the scheduler stores separate continuation and reversal-discovery
lanes in `market_discovery_states`. Generate the shadow snapshot manually with:

```powershell
.\backend\.venv\Scripts\python.exe -X utf8 -u `
	.\backend\scripts\generate_market_discovery.py
```

`CONTINUATION` is a monitored 21-day candidate-alpha lane. `REVERSAL_WATCH`,
`EMERGING_REVERSAL`, `REVERSAL_CONFIRMED`, `CONFLICT`, and `LAGGARD` are discovery-only states,
not BUY/SHORT recommendations. The Dashboard exposes the evidence behind each classification.

Each snapshot also stores the additive `extension-0.1-shadow` current-position overlay. It keeps
three questions separate: `trend_state` (`UPTREND`, `DOWNTREND`, `NEUTRAL`), `extension_risk`
(`NORMAL`, `EXTENDED`, `EXHAUSTION_WATCH`), and `reversal_trigger` (early, confirmed, or `NONE`).
An extreme 21-day move must also have an RSI, SMA20-distance, or directional-streak vote before it
is extended. Extension alone does not imply reversal; an early trigger requires deterioration,
while confirmation requires a SMA20 break/reclaim and reversed swing structure. The overlay is
descriptive, does not overwrite scanner evidence or Review Priority, and is persisted only when
the current EOD snapshot runs. Existing historical rows are not rewritten.

Discovery snapshots retain the latest 252 trading sessions. Older discovery rows are derived
data and can be reconstructed from daily price history during qualification backfills. Scanner
events and outcomes remain full-history because they supply the qualification sample. Occurrence
detail retains 252 dates plus the latest ticker/interval row, while `scanner_events` preserves the
lifetime observation count.

Use `--dry-run` to inspect the state and overlay counts without persistence. The Dashboard, ticker
detail and Scanner Evaluation views show current extension risk separately from historical signal
evidence.

| Endpoint | Purpose |
|---|---|
| `GET /api/discovery/states?state=EMERGING_REVERSAL` | Filter the latest discovery snapshot |
| `GET /api/stock/{ticker}/discovery-state` | Latest state, current-position overlay and transition history |

### Signal research and validation

`backend/scripts/run_alpha_research.py` validates a candidate cross-sectional feature/weight set
before it can ever be promoted into the production `xsmom-1.0` signal. It is a manual research
tool — never run by the scheduler — and only logs to the internal `research_runs` audit table.
See [docs/SIGNAL_RESEARCH.md](docs/SIGNAL_RESEARCH.md) and
[docs/MODEL_REGISTRY.md](docs/MODEL_REGISTRY.md) for the full promotion contract.

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_alpha_research.py --features mom_12_1 --horizon 21
```

| Flag | Purpose |
|------|---------|
| `--start YYYY-MM-DD` | Start date for the evaluation window |
| `--end YYYY-MM-DD` | End date for the evaluation window |
| `--horizon N` | Forward return horizon in trading days (default `1`) |
| `--train-days N` | Walk-forward training window length (default `252`) |
| `--test-days N` | Walk-forward test window length (default `21`) |
| `--embargo-days N` | Bars dropped between train/test to prevent label overlap leakage (default `2`) |
| `--alpha N` | Ridge penalty, only used when `--model ridge` (default `10.0`) |
| `--rolling` | Use a rolling training window instead of the default expanding window |
| `--cost-bps N` | One-way transaction cost in bps applied to traded notional (default `2.0`) |
| `--rebalance-days N` | Hold period; `1` rebalances daily (default `1`) |
| `--features a,b,c` | Comma-separated feature subset to test (default: all daily features) |
| `--hourly` | Include intraday features derived from hourly bars |
| `--activity-filter {none,liquidity,composite}` | Restrict each test cross-section to its top-half by activity (default `none`) |
| `--model {ridge,lgbm}` | `ridge` (default, linear) or `lgbm` (gradient-boosted trees; `pip install lightgbm scikit-learn` first) |
| `--no-log` | Do not record this run in `research_runs` |

The verdict is one of `ALPHA`, `PROMISING BUT UNDERPOWERED`, `RISK EXPOSURE, NOT ALPHA`, or
`NO SIGNAL`. Only an `ALPHA` verdict justifies manually copying the validated weights into
`backend/research/xsmom.py`'s `MODEL_WEIGHTS` — there is no automatic promotion path.


## Docker Alternative

On a machine with Docker Desktop, the repository can initialize PostgreSQL from
the bundled backup and run the database, API, frontend, and scheduler together:

```powershell
Copy-Item .env.example .env
# Edit .env and set a secure DB_PASSWORD.
docker compose up --detach --build
docker compose ps
```

Open `http://localhost` for the portal and `http://localhost:8001/docs` for API
documentation. The automatic database restore runs only when the
`postgres_data` Docker volume is first created. See
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for deployment details.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Health check |
| `GET /api/tickers` | List all tickers |
| `GET /api/tickers/overview` | Ticker overview with metadata |
| `GET /api/latest-price-date` | Latest trading date in the DB |
| `GET /api/stock/{ticker}/prices` | Raw price rows |
| `GET /api/stock/{ticker}/chart` | Candlestick chart data |
| `GET /api/stock/{ticker}/trade-setup` | Suggested trade setup |
| `GET /api/scan/gaps` | Scan for gap strategies |
| `GET /api/scan/fvg` | Scan for fair value gaps |
| `GET /api/scan/ma-crossover` | Scan for MA crossovers |
| `GET /api/scan/momentum-pullback` | Scan for momentum pullbacks |
| `GET /api/scan/bearish-bounce` | Scan for bearish bounces |
| `GET /api/scan/fibonacci` | Scan for Fibonacci retracements |
| `GET /api/scan/streak` | Scan for up/down streaks |
| `GET /api/scan/all` | Run all screeners |
| `GET /api/market-regime` | Current market regime |
| `GET /api/strategies` | List available strategies |
| `GET /api/stock/{ticker}/chart` | Get chart data |

## Tech Stack

**Backend:**
- FastAPI
- PostgreSQL + psycopg2
- yfinance for market data

**Frontend:**
- React 19 + TypeScript
- Vite
- Lightweight Charts (TradingView)
- React Router

## Future Enhancements

- [ ] Real-time data with Polygon.io WebSocket
- [ ] Drawing tools on charts
- [ ] Custom watchlists
- [ ] Alert notifications
- [ ] Backtesting module
