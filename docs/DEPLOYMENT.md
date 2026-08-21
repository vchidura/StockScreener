# Stock Screener Portal - Deployment Guide

## Quick Start with Docker Compose

### Prerequisites
- Docker and Docker Compose installed
- Git (to clone the repository)

### 1. Configure Environment
```bash
cd stock-screener-portal
cp .env.example .env
# Edit .env with your database credentials
```

### 2. Build and Run
```bash
# Build and start all services
docker-compose up -d --build

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### 3. Access the Application
- **Frontend**: http://localhost
- **Backend API**: http://localhost:8001/docs (Swagger UI)

---

## Production Deployment Options

### Option 1: VPS/Cloud VM (DigitalOcean, Linode, AWS EC2)

1. **Provision a VM** with Docker installed (Ubuntu recommended)
2. **Clone the repo** and configure `.env`
3. **Run Docker Compose**:
   ```bash
   docker-compose up -d --build
   ```
4. **Configure firewall** to allow ports 80 (HTTP), 443 (HTTPS)
5. **Add SSL** with nginx reverse proxy + Let's Encrypt

### Option 2: Cloud Container Services

#### AWS (ECS/Fargate)
- Push images to ECR
- Create ECS task definitions for frontend/backend
- Use RDS PostgreSQL for database
- Use ALB for load balancing

#### Azure Container Apps
- Push images to Azure Container Registry
- Deploy as Container Apps
- Use Azure Database for PostgreSQL

#### Google Cloud Run
- Push images to Artifact Registry
- Deploy as Cloud Run services
- Use Cloud SQL for PostgreSQL

### Option 3: Kubernetes (Advanced)
```bash
# Create namespace
kubectl create namespace stock-screener

# Apply manifests
kubectl apply -f k8s/ -n stock-screener
```

---

## Standalone Deployment (Wheel Package — No Source Code)

Deploy the portal using only pre-built artifacts. No repository clone or source code needed.

### Building the Wheel Package

The backend root files (`database.py`, `screeners.py`, `main.py`, `models.py`) are the
production source of truth. The build script syncs them into `src/stock_screener/`,
converts bare imports to relative package imports, and produces the `.whl`.

```bash
cd stock-screener-portal/backend

# Preview what will be synced (no files changed)
python build_wheel.py --check

# Build the wheel
python build_wheel.py
```

Output lands in `backend/dist/`:
```
dist/
  stock_screener_api-1.1.0-py3-none-any.whl
  stock_screener_api-1.1.0.tar.gz
```

> **Note**: Always edit the root-level files (`backend/database.py`, etc.) during
> development. Never edit `src/stock_screener/` directly — those copies are
> overwritten on every build.

### Artifacts to Ship

| File | Description | Location |
|------|-------------|----------|
| `stock_screener_api-1.1.0-py3-none-any.whl` | Backend API package | `backend/dist/` |
| `frontend/dist/` | Pre-built frontend (run `npm run build` first) | `frontend/dist/` |
| `stocks_db_backup_v4_2026-08-21.dump` | Database backup with all data and the cleaned 18-table schema | `backend/backups/` |
| `stocks_db_schema_v2_2026-08-21.dump` | Complete empty 18-table application database schema | `backend/backups/` |
| `stocks_market_schema_v1_2026-08-21.dump` | Empty ticker and market-price table schemas | `backend/backups/` |

### Prerequisites on Target Machine

| Dependency | Version | Purpose |
|-----------|---------|---------|
| Python | 3.10+ | Backend runtime |
| PostgreSQL | 14+ | Database (includes `pg_restore`, `psql`) |
| nginx (optional) | any | Serve frontend + proxy API |

### Step 1: Restore Database

```bash
createdb -U postgres stocks_db
pg_restore -U postgres -h localhost -d stocks_db stocks_db_backup_v4_2026-08-21.dump
```

### Step 2: Install Backend API

```bash
python -m venv venv

# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

pip install stock_screener_api-1.1.0-py3-none-any.whl
```

### Step 3: Configure Environment

Create a `.env` file in the directory you'll run from:

```env
DB_NAME=stocks_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

### Step 4: Start Backend API

```bash
# Default (port 8001)
stock-screener

# Custom port
stock-screener --port 8080

# Development mode with auto-reload
stock-screener --reload

# Custom .env file path
stock-screener --env-file /path/to/.env
```

API available at `http://localhost:8001/docs` (Swagger UI).

### Step 5: Serve Frontend

Copy the `frontend/dist/` folder to the target machine, then:

```bash
# Quick option — Python built-in server
cd dist
python -m http.server 5174
```

For production, use nginx to serve the frontend and proxy API calls:

```nginx
server {
    listen 80;

    location / {
        root /path/to/frontend/dist;
        try_files $uri $uri/ /index.html;
    }

    location /api/ {
        proxy_pass http://localhost:8001;
    }
}
```

### Access the Application

- **Frontend**: http://localhost (nginx) or http://localhost:5174 (Python server)
- **Backend API**: http://localhost:8001/docs

---

## Patching / Upgrading After Code Changes

Use these steps to deploy new code changes to a running server.

### Standalone (Wheel) Deployment

#### 1. Rebuild the wheel after code changes

```bash
cd stock-screener-portal/backend
python build_wheel.py
```

#### 2. Copy artifacts to the server

Manually copy the built artifacts to the server

- **Backend wheel**: `backend/stock_screener_api-<version>-py3-none-any.whl` → server deploy directory
- **Frontend files**: `portal/` folder → server frontend directory

#### 3. Upgrade the backend on the server

```bash
# Activate the virtual environment
source venv/bin/activate          # Linux/Mac
# or: venv\Scripts\activate       # Windows

# Reinstall the wheel (--force-reinstall overwrites same version number)
pip install --force-reinstall stock_screener_api-*.whl

# Restart the backend process
# If using systemd:
sudo systemctl restart stock-screener

# If running manually:
stock-screener                    # or: uvicorn main:app --host 0.0.0.0 --port 8001
```

#### 4. Frontend

No restart needed — nginx serves the new static files automatically.
If using Python's built-in server, restart it:

```bash
cd dist
python -m http.server 5174
```

---

## Database Backup & Data Export

Use these commands to backup your local database and restore it on the hosted server.

### Export Local Database (with all data)

```bash
# From your local machine - create a full backup (schema + data)
cd stock-screener-portal/backend

# Option 1: Custom format (recommended - compressed, flexible restore)
pg_dump -U vamsh100 -h localhost -d stocks_db -F c -f stocks_db_backup.dump

# Option 2: Plain SQL format (portable, human-readable)
pg_dump -U vamsh100 -h localhost -d stocks_db > stocks_db_backup.sql

# Option 3: Data only (if schema already exists on target)
pg_dump -U vamsh100 -h localhost -d stocks_db --data-only > stocks_db_data.sql
```

### Restore to Hosted Database

```bash
# For Docker Compose deployment - copy backup to server first
scp stocks_db_backup.sql user@your-server:/path/to/stock-screener-portal/

# On server: Start DB container, then restore
docker-compose up -d db
docker-compose exec -T db psql -U postgres -d stocks_db < stocks_db_backup.sql

# For managed PostgreSQL (Neon, Supabase, RDS, etc.)
psql "postgresql://user:password@your-host.com/stocks_db" < stocks_db_backup.sql

# For custom format (.dump files)
pg_restore -U postgres -h your-host.com -d stocks_db stocks_db_backup.dump
```

### Verify Data After Restore

```bash
# Check row counts
psql -U postgres -d stocks_db -c "SELECT COUNT(*) FROM stock_prices_hourly;"
psql -U postgres -d stocks_db -c "SELECT COUNT(DISTINCT ticker) FROM stock_prices_hourly;"
```

---

## Database Migrations (Schema Versioning)

Migration files are plain SQL under `backend/migrations/` and are applied manually in
numeric order. Back up the database first, then stop application writers before applying
a schema-changing migration.

```bash
# Apply the unused-table cleanup to an existing database.
psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" \
  -f backend/migrations/013_remove_unused_tables.sql
```

Migration `013` is idempotent. It removes only `stock_data`, `gap_scan_results`, and
`pattern_weight_adjustments`; use the v4 full backup as the rollback point. New databases
can instead be restored from the complete schema archive.

---

## Scheduled Data Updates

The scheduler runs four jobs to keep all price tables fresh. One unified background process handles everything.

### Data Architecture

| Interval | Source Table | Update Frequency | Notes |
|----------|-------------|-----------------|-------|
| 5m | `stock_prices_intraday` | Every 5 min | Base intraday interval (15m/30m aggregated on the fly) |
| 1h | `stock_prices_hourly` | Every 60 min | 2+ years of history |
| 1d (running bar) | `stock_prices_daily` | Every 5 min | Today's candle updated throughout the day |
| 1d (final close) | `stock_prices_daily` | Once at ~4:15 PM ET | Final daily candle after market close |

### Option 1: Unified Scheduler (Recommended)

Single background process that runs all jobs automatically:

```bash
cd stock-screener-portal/backend

# Start with Yahoo Finance (default, recommended — no API key needed)
python scripts/run_scheduler.py

# Start with Twelve Data (5m candles skipped due to rate limit)
python scripts/run_scheduler.py --provider twelvedata

# Specific tickers only
python scripts/run_scheduler.py --tickers AAPL,MSFT,NVDA

# Run as background process (Windows)
start /B python scripts/run_scheduler.py

# Run as background process (Linux/Mac)
nohup python scripts/run_scheduler.py >> /var/log/scheduler.log 2>&1 &
```

The scheduler handles market hours automatically:
- **9:30 AM - 4:00 PM ET (weekdays)**: Runs 5m, daily candle, and hourly jobs
- **~4:15 PM ET**: Runs final daily close + last hourly update
- **After hours/weekends**: Sleeps and logs status

### Option 2: Individual Scripts (Standalone)

Each script can also run independently with `--continuous` mode:

```bash
# 5m intraday candles — every 5 min during market hours
python scripts/backfill_intraday.py --continuous

# Hourly candles — every 60 min during market hours
python scripts/update_hourly_prices.py --continuous

# Daily candle update — every 5 min during market hours
python scripts/update_intraday_prices.py --continuous

# Daily close — runs after market close each day
python scripts/update_daily_prices.py --continuous
```

### Option 3: Windows Task Scheduler

1. Open Task Scheduler, Create Task
2. **Triggers**: Daily, Start: 9:25 AM, Stop: 6:00 PM, Days: Monday-Friday
3. **Actions**:
   ```
   Program: C:\path\to\python.exe
   Arguments: scripts/run_scheduler.py
   Start in: C:\path\to\stock-screener-portal\backend
   ```

### Option 4: Linux/Mac Cron

```bash
crontab -e

# Start scheduler at 9:25 AM ET, Mon-Fri (it handles its own schedule)
25 9 * * 1-5 cd /path/to/backend && /path/to/python scripts/run_scheduler.py >> /var/log/scheduler.log 2>&1
```

### Option 5: Docker Compose

Add to `docker-compose.yml`:
```yaml
  scheduler:
    build:
      context: ./backend
      dockerfile: Dockerfile
    container_name: stock-screener-scheduler
    command: python scripts/run_scheduler.py
    depends_on:
      db:
        condition: service_healthy
    environment:
      DB_NAME: ${DB_NAME:-stocks_db}
      DB_USER: ${DB_USER:-postgres}
      DB_PASSWORD: ${DB_PASSWORD:-changeme}
      DB_HOST: db
      DB_PORT: 5432
      TWELVEDATA_API_KEY: ${TWELVEDATA_API_KEY:-}
    restart: unless-stopped
```

### Backfill Commands

One-time commands to populate historical data:

```bash
cd stock-screener-portal/backend

# Daily prices — last 30 days (or any range)
python scripts/update_daily_prices.py --days 30 --force

# Hourly prices — last 90 days
python scripts/update_hourly_prices.py --backfill --days 90 --force

# 5m intraday candles — last 7 days (yfinance limit)
python scripts/backfill_intraday.py --interval 5m --days 7 --force
```

### Provider Comparison

| Provider | API Key | Rate Limit | 5m Candles | Best For |
|----------|---------|------------|------------|----------|
| Yahoo Finance | Not needed | Generous | Yes (batch 50) | Recommended for all jobs |
| Twelve Data | Required (free: 800/day) | 8 req/min | Too slow (skipped) | Fallback only |

All scripts accept `--provider {yahoo,twelvedata}` to choose at runtime.

### Environment Variables for Schedulers

| Variable | Description | Required |
|----------|-------------|----------|
| `TWELVEDATA_API_KEY` | API key from twelvedata.com | Only for `--provider twelvedata` |
| `DB_*` | Database credentials | Yes |

### Database Setup (Intraday Table)

Run the migration to create the intraday table (if not already done):
```bash
psql -U $DB_USER -d $DB_NAME -f migrations/001_add_intraday_table.sql
```

---

## SSL/HTTPS Configuration

### Using Nginx + Let's Encrypt (Certbot)

1. Install certbot on your server
2. Update nginx.conf:
```nginx
server {
    listen 443 ssl;
    server_name yourdomain.com;
    
    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
    
    # ... rest of config
}

server {
    listen 80;
    server_name yourdomain.com;
    return 301 https://$server_name$request_uri;
}
```

---

## Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_NAME` | PostgreSQL database name | `stocks_db` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD` | Database password | - |
| `DB_HOST` | Database host | `db` (Docker) / `localhost` |
| `DB_PORT` | Database port | `5432` |
| `CORS_ORIGINS` | Allowed origins (comma-separated) | localhost URLs |

---

## Useful Commands

```bash
# Stop all services
docker-compose down

# Rebuild specific service
docker-compose build backend
docker-compose up -d backend

# View backend logs
docker-compose logs -f backend

# Access PostgreSQL CLI
docker-compose exec db psql -U postgres -d stocks_db

# Backup database
docker-compose exec db pg_dump -U postgres stocks_db > backup.sql

# Restore database
cat backup.sql | docker-compose exec -T db psql -U postgres stocks_db
```

---

## Monitoring (Optional)

Add monitoring with Prometheus + Grafana:

```yaml
# Add to docker-compose.yml
prometheus:
  image: prom/prometheus
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml
  ports:
    - "9090:9090"
```
