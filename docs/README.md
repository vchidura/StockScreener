# Stock Screener Portal

A full-stack stock screening application with a **FastAPI** backend and **React + TypeScript** frontend.  
All screener pages read exclusively from a local PostgreSQL database — no external API calls at render time.

---

## Project Structure

```
stock-screener-portal/
├── backend/
│   ├── main.py                 # FastAPI app — all API endpoints, middleware, startup
│   ├── database.py             # PostgreSQL functions (CRUD, bulk load, overview CTE)
│   ├── screeners.py            # Scan logic (gap, MA crossover, RSI, volume breakout)
│   ├── models.py               # Pydantic models
│   ├── requirements.txt        # Python dependencies
│   ├── .env                    # DB credentials & API keys (not committed)
│   ├── venv/                   # Python virtual environment
│   └── scripts/                # Utility & data pipeline scripts (see below)
│
├── frontend/
│   ├── src/
│   │   ├── App.tsx             # Router & navigation
│   │   ├── main.tsx            # React entry point
│   │   ├── index.css           # Global styles
│   │   ├── pages/
│   │   │   ├── Dashboard.tsx       # Landing page
│   │   │   ├── GapScreener.tsx     # Gap strategies scanner
│   │   │   ├── MAScreener.tsx      # Moving average crossover scanner
│   │   │   ├── RSIScreener.tsx     # RSI oversold/overbought scanner
│   │   │   ├── VolumeScreener.tsx  # Volume breakout scanner
│   │   │   ├── TickersOverview.tsx  # All tickers table (OHLCV + daily/weekly MAs)
│   │   │   └── TickerDetail.tsx    # Per-ticker chart (candlestick, Bollinger, MAs)
│   │   └── services/
│   │       └── api.ts          # Axios API client & TypeScript interfaces
│   ├── vite.config.ts          # Vite config (port 5174, proxy /api → localhost:8001)
│   ├── package.json
│   └── tsconfig.json
│
└── docs/
    ├── README.md               # ← You are here
    ├── FEATURES.md
    ├── HIGH_LEVEL_DESIGN.md
    ├── LOW_LEVEL_DESIGN.md
   ├── SIGNAL_RESEARCH.md      # Signal validation methodology & production signal
   ├── SCANNER_EVENT_EVALUATION.md # Shadow scanner lifecycle and outcome evaluation
   ├── SCHEDULER_EXECUTION.md  # Exact runtime flow: market-hours loop, EOD, scanner lanes
   ├── EXTENDED_HOURS_RESEARCH_DESIGN.md # Separate premarket/after-hours research pipeline
   ├── OPTION_CHAIN_SCANNER_IMPLEMENTATION_GUIDE.md # Start here: phased options build order
   ├── OPTION_CHAIN_SCANNER_DESIGN.md # Normative detailed options specification
   ├── OPTION_PHASE0_VALIDATION_2026-08-29.md # Developer entitlement and weekend data evidence
   ├── OPTION_PLATFORM_CAPACITY_DECISION_2026-08-29.md # Measured host/database capacity record
   └── MODEL_REGISTRY.md       # Model/scanner version registry & script run cadence
```

---

## Prerequisites

| Dependency   | Version  | Notes                        |
|-------------|----------|------------------------------|
| Python      | 3.10+    | Backend runtime              |
| Node.js     | 18+      | Frontend build               |
| PostgreSQL  | 14+      | Database                     |
| npm         | 9+       | Frontend package manager     |

---

## Database Setup

1. Create the PostgreSQL database:
   ```sql
   CREATE DATABASE stocks_db;
   ```

2. Configure credentials in `backend/.env`:
   ```env
   DB_NAME=stocks_db
   DB_USER=your_user
   DB_PASSWORD=your_password
   DB_HOST=localhost
   DB_PORT=5432
   TWELVEDATA_API_KEY=your_key_here
   ```

3. The `selected_tickers` table is created on backend startup. Restore the schema archive or run migrations for the remaining tables.

---

## Running the Backend

```powershell
cd stock-screener-portal\backend

# Create virtual environment (one-time)
python -m venv venv

# Activate venv
.\venv\Scripts\Activate.ps1

# Install dependencies (one-time)
pip install -r requirements.txt

# Start the API server on port 8001
python -m uvicorn main:app --host 0.0.0.0 --port 8001
```

The API is now available at `http://localhost:8001`.  
Swagger docs at `http://localhost:8001/docs`.

### Key API Endpoints

| Endpoint                       | Method | Description                                    |
|-------------------------------|--------|------------------------------------------------|
| `/api/tickers`                | GET    | List all active selected tickers               |
| `/api/tickers/overview`       | GET    | OHLCV + daily MAs + weekly MAs for all tickers |
| `/api/stock/{ticker}/chart`   | GET    | OHLCV chart data (candlestick-ready)           |
| `/api/stock/{ticker}/prices`  | GET    | Raw price rows from DB                         |
| `/api/scan/gaps`              | GET    | Gap strategies scan                            |
| `/api/scan/ma-crossover`      | GET    | Moving average crossover scan                  |
| `/api/scan/rsi`               | GET    | RSI oversold/overbought scan                   |
| `/api/scan/volume-breakout`   | GET    | Volume breakout scan                           |
| `/api/strategies`             | GET    | List available screening strategies            |

---

## Running the Frontend

```powershell
cd stock-screener-portal\frontend

# Install dependencies (one-time)
npm install

# Start dev server on port 5174
npm run dev
```

Open `http://localhost:5174` in a browser.  
The Vite dev server proxies all `/api/*` requests to `http://localhost:8001`.

---

## Utility Scripts

All scripts live in `backend/scripts/` and should be run from the `backend/` directory with the venv activated.

### Data Pipeline

| Script | Purpose | Usage |
|--------|---------|-------|
| `update_daily_prices.py` | **Daily job** — fetches latest candles from Twelve Data and upserts into DB. Run after market close. | `python scripts/update_daily_prices.py` |
| `backfill_sample_tickers_4y.py` | One-time bulk backfill of 4 years of historical data. Supports Yahoo, Twelve Data, Tiingo, Alpha Vantage sources. | `python scripts/backfill_sample_tickers_4y.py --use-selected --years 4 --source twelvedata` |
| `filter_top_tickers.py` | Rank tickers by volume/liquidity/volatility and populate `selected_tickers` table. | `python scripts/filter_top_tickers.py` |

### Diagnostics & Validation

| Script | Purpose | Usage |
|--------|---------|-------|
| `check_backfill_status.py` | Show which tickers have enough historical data and which need backfill. | `python scripts/check_backfill_status.py` |
| `check_ticker_date_range.py` | Inspect OHLCV rows for a specific ticker and date range. | `python scripts/check_ticker_date_range.py MSFT 2024-01-01 2024-01-31` |
| `compare_ticker_sources.py` | Compare DB prices vs Yahoo, Alpha Vantage, Twelve Data for validation. | `python scripts/compare_ticker_sources.py MSFT` |
| `validate_true_200w_ma.py` | Verify 200-week SMA calculation accuracy against known values. | `python scripts/validate_true_200w_ma.py` |

### Daily Update Example (Scheduled Task)

```powershell
# Update all 185 active tickers with last 5 trading days
cd stock-screener-portal\backend
.\venv\Scripts\Activate.ps1
$env:TWELVEDATA_API_KEY = "your_key_here"
python scripts/update_daily_prices.py

# Or update specific tickers / more days
python scripts/update_daily_prices.py --tickers AAPL,MSFT --days 10
```

Schedule via Windows Task Scheduler to run weekdays after market close (e.g. 5:30 PM ET).

---

## Architecture Notes

- **All portal endpoints are DB-only** — no external API calls during page rendering. This ensures fast, reliable page loads.
- **External data fetching** happens only via the utility scripts (run separately as batch jobs).
- **Bulk loading** — scan endpoints fetch all ticker data in a single SQL query, then process in-memory with vectorized numpy operations for speed (~4s for 185 tickers).
- **Data source priority**: Twelve Data (primary, validated accurate) → Tiingo → Alpha Vantage. Yahoo Finance and Stooq have been removed from the portal pipeline.
- **185 curated tickers** in `selected_tickers` table, ranked by avg volume (50%), dollar volume (30%), and ATR% (20%).
