# Stock Screener Portal - Server Migration Guide

Simple step-by-step guide for server deployment (no Docker).

---

## Step 1: Drop and Recreate Database

> **WARNING**: Ensure you have a valid backup before proceeding.

```bash
# Terminate active connections
psql -h localhost -p 5432 -U <DB_USER> -d postgres -c "
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE datname = 'stocks_db' AND pid <> pg_backend_pid();
"

# Drop and recreate
psql -h localhost -p 5432 -U <DB_USER> -d postgres -c "DROP DATABASE IF EXISTS stocks_db;"
psql -h localhost -p 5432 -U <DB_USER> -d postgres -c "CREATE DATABASE stocks_db;"
```

---

## Step 2: Restore Database from Backup

```bash
pg_restore -h localhost -p 5432 -U <DB_USER> -d stocks_db \
  --no-owner --no-privileges --clean --if-exists \
  StockScreener/DBBackup/stocks_db_backup.dump
```

---

## Step 3: Verify Restore

```bash
# Check tables exist
psql -h localhost -p 5432 -U <DB_USER> -d stocks_db -c "
  SELECT tablename FROM pg_tables WHERE schemaname = 'public';
"

# Quick row count check
psql -h localhost -p 5432 -U <DB_USER> -d stocks_db -c "
  SELECT relname AS table, n_live_tup AS row_count
  FROM pg_stat_user_tables ORDER BY n_live_tup DESC;
"
```

---

## Step 4: Install Backend Wheel Package

```bash
cd stock-screener-portal/backend

# Stop the running backend service
sudo systemctl stop stock-screener
# (or: kill the running uvicorn process)

# Install updated wheel
pip install --upgrade StockScreener/Backend/stock_screener_api-1.2.0-py3-none-any.whl

# Verify
pip show stock-screener-api

# Start the backend service
uvicorn stock_screener.app:app --host 0.0.0.0 --port 8001
# (or: sudo systemctl start stock-screener)
```

---

## Step 5: Deploy Frontend to Nginx

```bash
# Copy build output from git repo to nginx serve directory
sudo cp -r StockScreener/Portal/2.0/* /var/www/stock-screener/

# Verify files
ls -la /var/www/stock-screener/

# Reload nginx
sudo nginx -t && sudo nginx -s reload
```

---

## Step 6: Update Running Scheduler

The scheduler (`run_scheduler.py`) runs as a background process. After updating the script, restart it.

```bash
# Find the running scheduler process
ps aux | grep run_scheduler.py

# Kill the existing process
kill <PID>

# Copy updated script to the server (if not already done)
cp StockScreener/Backend/scripts/run_scheduler.py /path/to/deployed/backend/scripts/

# Restart the scheduler in background
cd /path/to/deployed/backend
nohup python scripts/run_scheduler.py > logs/scheduler.log 2>&1 &

# Verify it's running
ps aux | grep run_scheduler.py
tail -f logs/scheduler.log
```

---

## Quick Reference (All Steps)

```bash
# 1. Drop & recreate DB
psql -h localhost -p 5432 -U <DB_USER> -d postgres -c "DROP DATABASE IF EXISTS stocks_db;"
psql -h localhost -p 5432 -U <DB_USER> -d postgres -c "CREATE DATABASE stocks_db;"

# 2. Restore DB
pg_restore -h localhost -p 5432 -U <DB_USER> -d stocks_db \
  --no-owner --no-privileges --clean --if-exists \
  StockScreener/DBBackup/stocks_db_backup.dump

# 3. Verify
psql -h localhost -p 5432 -U <DB_USER> -d stocks_db -c "\dt"

# 4. Install backend wheel
pip install --upgrade StockScreener/Backend/stock_screener_api-1.2.0-py3-none-any.whl
uvicorn stock_screener.app:app --host 0.0.0.0 --port 8001

# 5. Deploy frontend
sudo cp -r StockScreener/Portal/2.0/* /var/www/stock-screener/
sudo nginx -t && sudo nginx -s reload

# 6. Restart scheduler
kill $(pgrep -f run_scheduler.py)
cd ../backend
nohup python scripts/run_scheduler.py > logs/scheduler.log 2>&1 &
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `pg_restore: error: could not connect` | Verify DB_HOST, DB_PORT, DB_USER in `.env` |
| `database "stocks_db" already exists` | Use `DROP DATABASE IF EXISTS` before creating |
| `permission denied` on restore | Add `--no-owner --no-privileges` flags |
| `build_wheel.py` fails | Run `pip install build` first |
| Frontend build fails | Run `npm install` to update dependencies |
| Active connections prevent DROP | Terminate connections first (see Step 1) |
| `nginx -t` fails | Check `/etc/nginx/nginx.conf` syntax |
