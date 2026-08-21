# Stock Screener API

FastAPI backend for technical stock analysis and screening strategies.

## Installation

For complete new-machine setup, including PostgreSQL installation, database
restore, frontend dependencies, and startup verification, follow the
[root setup guide](../README.md#new-machine-setup-windows).

## Local Database Topology

The verified Windows development environment uses the native PostgreSQL 17
binaries and a project-owned cluster:

- Data directory: `%LOCALAPPDATA%\stock-screener-portal\postgres-data`
- Application listener: `127.0.0.1:5433`
- Backend database and user: `stocks_db` / `vamsh100`
- Cluster process: started with `pg_ctl`, not Docker or the Windows service

The installer-managed PostgreSQL Windows service listens separately on port
`5432` and is not used by this backend. Docker Compose is another independent
alternative: containers use `db:5432`. Do not point the native backend at the
Windows service or mix native and Compose connection settings.

### From Wheel Package
```bash
pip install stock_screener_api-1.0.0-py3-none-any.whl
```

### From Source
```bash
pip install .
```

## Usage

### Run Backend and Frontend Locally

The backend requires PostgreSQL and a configured `backend/.env` file. From the
repository root, copy `backend/.env.example` to `backend/.env` and update the
database credentials. For the project-owned native cluster, set the host to
`127.0.0.1` and the port to `5433` as shown below.

Start the isolated database after a reboot if it is not already running:

```powershell
$pg = "C:\Program Files\PostgreSQL\17\bin"
$dataDir = Join-Path $env:LOCALAPPDATA "stock-screener-portal\postgres-data"
$logFile = Join-Path $env:LOCALAPPDATA "stock-screener-portal\postgres.log"
& "$pg\pg_ctl.exe" start -D $dataDir -l $logFile -w
& "$pg\pg_isready.exe" -h 127.0.0.1 -p 5433 -d stocks_db
```

Start the API in one PowerShell terminal:

```powershell
cd backend
.\.venv\Scripts\python.exe -m uvicorn main:app --reload --host 127.0.0.1 --port 8001
```

Start the portal in a second PowerShell terminal:

```powershell
cd frontend
npm.cmd run dev
```

Open `http://127.0.0.1:5174`. The frontend proxies API requests to
`http://127.0.0.1:8001`.

### Run API Server
```bash
# Default (port 8001)
stock-screener

# Custom port
stock-screener --port 8080

# Production with multiple workers
stock-screener --workers 4

# Development with auto-reload
stock-screener --reload

# Custom .env file
stock-screener --env-file /path/to/.env
```

### Environment Configuration
Create a `.env` file:
```env
DB_NAME=stocks_db
DB_USER=vamsh100
DB_PASSWORD=<your_app_db_password>
DB_HOST=127.0.0.1
DB_PORT=5433
TWELVEDATA_API_KEY=your_api_key
```

For Docker Compose only, the backend container receives `DB_HOST=db` and
`DB_PORT=5432` from `docker-compose.yml`; these are internal container-network
settings and should not replace the native values above.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/scan/gaps` | Gap up/down scanner |
| `GET /api/scan/ma-crossover` | Moving average crossover |
| `GET /api/scan/momentum-pullback` | Momentum pullback signals |
| `GET /api/scan/bearish-bounce` | Bearish bounce setups |
| `GET /api/scan/fibonacci` | Fibonacci retracement levels |
| `GET /api/tickers` | List available tickers |
| `GET /docs` | Interactive API documentation |

## License

MIT
