# Stock Screener API

FastAPI backend, canonical equity materialization workers, and read-only option
research APIs for the Stock Screener Portal.

Use the root [README](../README.md) for development startup and the
[deployment guide](../docs/DEPLOYMENT.md) for production configuration,
database role isolation, backups, and Compose profiles.

## Install

From the repository root:

```powershell
python -m venv .\backend\.venv
.\backend\.venv\Scripts\python.exe -m pip install --upgrade pip
.\backend\.venv\Scripts\python.exe -m pip install -r .\backend\requirements.txt
Copy-Item .\backend\.env.example .\backend\.env
```

Set real database credentials and `POLYGON_API_KEY` in `backend/.env`. Do not
commit that file. The complete worker, materialized read, options safety, and
CORS contract is documented in the template.

Production must use `APP_ENV=production` and a non-owner, non-superuser
`DB_USER`. Options must remain read-only and the Advanced stream profile must
remain disabled until their explicit gates pass.

## Validate Configuration

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_cutover_environment.py
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_equity_storage.py
```

The first command validates the active environment without printing secrets.
The second validates immutable bars, publications, evidence ownership, and all
20 portal snapshots.

## Run From Source

Start each long-running process separately from the repository root:

```powershell
# API
.\backend\.venv\Scripts\python.exe -m uvicorn main:app `
  --app-dir backend --reload --host 127.0.0.1 --port 8001

# Canonical equity worker
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_equity_worker.py

# Portal snapshot worker
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\refresh_equity_portal_snapshots.py --continuous
```

The API is available at `http://127.0.0.1:8001`; OpenAPI documentation is at
`http://127.0.0.1:8001/docs`.

## Build Package

The backend root modules are the source of truth. Preview and build the wheel:

```powershell
Set-Location backend
.\.venv\Scripts\python.exe build_wheel.py --check
.\.venv\Scripts\python.exe build_wheel.py
```

The backend root modules and package directories are the only wheel inputs; no
generated source copy is tracked.

## Key Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /api/health` | Canonical storage, portal snapshot, and role readiness |
| `GET /api/tickers` | Active ticker universe |
| `GET /api/stock/{ticker}/chart` | Canonical chart bars |
| `GET /api/stock/{ticker}/trade-setup/multi` | Materialized multi-interval setup |
| `GET /api/chart-patterns/scan` | Materialized Pattern Watch scan |
| `GET /api/scan/*` | Worker-published scanner pages |
| `GET /api/sector-intelligence` | Worker-published sector view |
| `GET /api/options/health` | Read-only option research readiness |

Legacy `stock_prices_*` and scanner-event tables are not part of the canonical
baseline and must not be recreated.