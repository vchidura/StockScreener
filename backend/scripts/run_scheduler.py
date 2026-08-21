#!/usr/bin/env python3
"""
Unified price update scheduler — single background process for all price jobs.
Scripts used:
  update_intraday_prices.py  — 5m candles  → stock_prices_intraday
  update_running_daily.py    — live quote   → stock_prices_daily (running bar)
  update_hourly_prices.py    — hourly bars  → stock_prices_hourly
  update_daily_prices.py     — EOD candle   → stock_prices_daily (final bar)
  validate_eod.py            — post-close data integrity check
Orchestrates four update jobs during US market hours:
  ┌──────────────────────┬────────────┬──────────────────────────────────┐
  │ Job                  │ Every      │ Target Table                     │
  ├──────────────────────┼────────────┼──────────────────────────────────┤
  │ Intraday 5m candles  │ 5 min      │ stock_prices_intraday            │
  │ Today's daily candle │ 5 min      │ stock_prices_daily (running bar) │
  │ Hourly candles       │ 60 min     │ stock_prices_hourly              │
  │ Daily close (EOD)    │ once/day   │ stock_prices_daily (final bar)   │
  └──────────────────────┴────────────┴──────────────────────────────────┘

All intraday jobs run only during US market hours (9:30 AM – 4:00 PM ET).
Daily close runs once at ~4:15 PM ET after market closes.

Provider notes:
  Yahoo Finance  — batch 50 tickers, no rate limit. Supports all jobs.
  Twelve Data    — 8 req/min free tier. Skips 5m candles (too slow for 400 tickers).

Usage:
    # Start scheduler (Yahoo Finance, recommended)
    python scripts/run_scheduler.py

    # Start with Twelve Data
    python scripts/run_scheduler.py --provider twelvedata

    # Specific tickers only
    python scripts/run_scheduler.py --tickers AAPL,MSFT,NVDA

    # Run as background process (Windows)
    start /B python scripts/run_scheduler.py

    # Run as background process (Linux/Mac)
    nohup python scripts/run_scheduler.py &
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging

SCRIPTS_DIR = Path(__file__).resolve().parent
BACKEND_DIR = SCRIPTS_DIR.parent

for p in [str(BACKEND_DIR), str(SCRIPTS_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor, get_selected_tickers

# Import reusable functions from sibling scripts
from update_daily_prices import (
    fetch_daily_yahoo_direct, fetch_daily_twelvedata,
    get_existing_dates, upsert_prices,
)
from update_hourly_prices import (
    fetch_hourly_yahoo_direct, fetch_hourly_candles,
    upsert_hourly_prices,
)
from update_running_daily import (
    fetch_batch_quotes,
    upsert_intraday_price,
)
from update_intraday_prices import fetch_intraday_yahoo_direct, upsert_intraday
from validate_eod import validate_all

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scheduler")

# ── Schedule intervals (seconds) ────────────────────────────────────
INTRADAY_5M_INTERVAL = 5 * 60       # 5 minutes
DAILY_CANDLE_INTERVAL = 5 * 60      # 5 minutes (keep today's daily bar fresh)
HOURLY_INTERVAL = 60 * 60           # 60 minutes
LOOP_SLEEP = 30                     # main loop checks every 30 seconds

# Twelve Data rate-limit sleep
TD_RATE_LIMIT = 8
TD_BATCH_SIZE = 8


# ── Timezone helper ─────────────────────────────────────────────────
def _get_et_now() -> datetime:
    """Return current time in US Eastern."""
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except ImportError:
        import pytz
        et = pytz.timezone("America/New_York")
    return datetime.now(et)


def is_market_hours() -> bool:
    """Check if US stock market is currently open (9:30 AM – 4:00 PM ET, weekdays)."""
    now_et = _get_et_now()
    if now_et.weekday() > 4:
        return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close


def is_post_close_window() -> bool:
    """Check if we're in the post-close window (4:15 PM – 6:00 PM ET, weekdays)."""
    now_et = _get_et_now()
    if now_et.weekday() > 4:
        return False
    return 16 <= now_et.hour < 18 and (now_et.hour > 16 or now_et.minute >= 15)


# ── Job: 5-minute intraday candles ──────────────────────────────────
def job_intraday_5m(tickers: list[str], provider: str):
    """Fetch today's 5m candles → stock_prices_intraday. Yahoo only."""
    if provider != "yahoo":
        logger.info("  [5m] Skipped — Twelve Data too slow for 5m with %d tickers", len(tickers))
        return

    logger.info("  [5m] Fetching 5m candles from Yahoo Chart API...")
    ticker_data = fetch_intraday_yahoo_direct(tickers, "5m", days=1)
    total = 0
    success = 0
    for ticker in tickers:
        df = ticker_data.get(ticker)
        if df is not None and not df.empty:
            total += upsert_intraday(ticker, df, "5m")
            success += 1
    logger.info("  [5m] Done — %d candles upserted (%d/%d tickers)", total, success, len(tickers))


# ── Job: Today's running daily candle ───────────────────────────────
def job_daily_candle(tickers: list[str], provider: str):
    """Update today's running daily candle → stock_prices_daily."""
    if provider == "yahoo":
        logger.info("  [daily-candle] Fetching from Yahoo Chart API...")
        ticker_data = fetch_daily_yahoo_direct(tickers, days=1)
        quotes = {}
        for ticker, df in ticker_data.items():
            if df.empty:
                continue
            row = df.iloc[-1]
            quotes[ticker] = {
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
                "datetime": df.index[-1].strftime("%Y-%m-%d"),
            }
    else:
        logger.info("  [daily-candle] Fetching from Twelve Data (batch %d)...", TD_BATCH_SIZE)
        quotes = {}
        for i in range(0, len(tickers), TD_BATCH_SIZE):
            batch = tickers[i : i + TD_BATCH_SIZE]
            quotes.update(fetch_batch_quotes(batch))
            if i + TD_BATCH_SIZE < len(tickers):
                time.sleep(TD_RATE_LIMIT)

    success = 0
    for ticker in tickers:
        if ticker in quotes and upsert_intraday_price(ticker, quotes[ticker]):
            success += 1
    logger.info("  [daily-candle] Done — %d/%d tickers updated in stock_prices_daily", success, len(tickers))


# ── Job: Hourly candles ─────────────────────────────────────────────
def job_hourly(tickers: list[str], provider: str):
    """Fetch recent hourly candles → stock_prices_hourly."""
    if provider == "yahoo":
        logger.info("  [hourly] Fetching from Yahoo Chart API...")
        ticker_data = fetch_hourly_yahoo_direct(tickers, days=2)
    else:
        logger.info("  [hourly] Fetching from Twelve Data (1 ticker at a time)...")
        ticker_data = {}
        for i, t in enumerate(tickers, 1):
            df = fetch_hourly_candles(t, days=2)
            if not df.empty:
                ticker_data[t] = df
            if i < len(tickers):
                time.sleep(TD_RATE_LIMIT)

    total = 0
    success = 0
    for ticker in tickers:
        df = ticker_data.get(ticker)
        if df is not None and not df.empty:
            total += upsert_hourly_prices(ticker, df)
            success += 1
    logger.info("  [hourly] Done — %d candles upserted (%d/%d tickers)", total, success, len(tickers))


# ── Job: End-of-day daily close ─────────────────────────────────────
def job_daily_close(tickers: list[str], provider: str):
    """Final daily candle after market close → stock_prices_daily."""
    logger.info("  [daily-close] Fetching final candles from %s...", provider)
    if provider == "yahoo":
        ticker_data = fetch_daily_yahoo_direct(tickers, days=5)
    else:
        ticker_data = fetch_daily_twelvedata(tickers, days=5)

    end_dt = datetime.utcnow() + timedelta(days=1)
    start_dt = end_dt - timedelta(days=15)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    total_inserted = 0
    success = 0
    for ticker in tickers:
        df = ticker_data.get(ticker)
        if df is not None and not df.empty:
            existing = get_existing_dates(ticker, start_str, end_str)
            existing.discard(_get_et_now().date())
            inserted, _ = upsert_prices(ticker, df, existing)
            total_inserted += inserted
            success += 1
    logger.info("  [daily-close] Done — +%d new rows (%d/%d tickers)", total_inserted, success, len(tickers))


def _ensure_ingestion_failures_table() -> None:
    """Create the durable retry queue when migration 008 has not run yet."""
    with get_db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS data_ingestion_failures (
                failure_id BIGSERIAL PRIMARY KEY,
                dataset VARCHAR(32) NOT NULL,
                ticker VARCHAR(16) NOT NULL,
                trade_date DATE NOT NULL,
                provider VARCHAR(32) NOT NULL,
                failure_type VARCHAR(64) NOT NULL,
                details TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                resolved_at TIMESTAMPTZ,
                UNIQUE (dataset, ticker, trade_date)
            )
        """)


def _record_daily_failures(gaps: dict[str, list], provider: str, attempts: int) -> int:
    """Upsert unresolved official daily rows into the manual retry queue."""
    if not gaps:
        return 0
    _ensure_ingestion_failures_table()
    rows = [
        (ticker, trade_date, provider, attempts)
        for ticker, dates in gaps.items()
        for trade_date in dates
    ]
    with get_db_cursor() as cur:
        cur.executemany("""
            INSERT INTO data_ingestion_failures (
                dataset, ticker, trade_date, provider, failure_type, details, attempts
            ) VALUES ('daily', %s, %s, %s, 'provider_row_missing',
                      'Official daily candle absent after bounded retries', %s)
            ON CONFLICT (dataset, ticker, trade_date) DO UPDATE SET
                provider = EXCLUDED.provider,
                failure_type = EXCLUDED.failure_type,
                details = EXCLUDED.details,
                attempts = data_ingestion_failures.attempts + EXCLUDED.attempts,
                last_attempt_at = NOW(),
                resolved_at = NULL
        """, rows)
    return len(rows)


def _resolve_daily_failures(tickers: list[str] | None = None) -> int:
    """Mark queued daily failures resolved when the official row now exists."""
    _ensure_ingestion_failures_table()
    ticker_clause = "AND f.ticker = ANY(%s)" if tickers else ""
    params = [tickers] if tickers else []
    with get_db_cursor() as cur:
        cur.execute(f"""
            UPDATE data_ingestion_failures f
            SET resolved_at = NOW(), last_attempt_at = NOW()
            WHERE f.dataset = 'daily' AND f.resolved_at IS NULL
              {ticker_clause}
              AND EXISTS (
                  SELECT 1 FROM stock_prices_daily d
                  WHERE d.ticker = f.ticker AND d.datetime::date = f.trade_date
              )
        """, params)
        return cur.rowcount


def _upsert_required_daily_rows(
    ticker_data: dict, required: dict[str, list]
) -> tuple[int, int]:
    """Persist only explicitly missing dates; fetched history is context, not progress."""
    inserted = 0
    found = 0
    for ticker, required_dates in required.items():
        df = ticker_data.get(ticker)
        wanted = set(required_dates)
        if df is None or df.empty:
            logger.warning("  [daily-retry] %s: provider returned no rows; need %s",
                           ticker, ", ".join(str(date) for date in sorted(wanted)))
            continue

        frame_dates = {index.date() for index in df.index}
        available = wanted & frame_dates
        latest = max(frame_dates) if frame_dates else None
        logger.info(
            "  [daily-retry] %s: fetched=%d latest=%s target=%d/%d",
            ticker, len(df), latest, len(available), len(wanted),
        )
        if not available:
            continue

        filtered = df[[index.date() in available for index in df.index]]
        ticker_inserted, _ = upsert_prices(ticker, filtered, set())
        inserted += ticker_inserted
        found += len(available)
    return inserted, found


def _fetch_and_upsert_daily_batches(
    required: dict[str, list], provider: str, days: int, batch_size: int = 10
) -> tuple[int, int]:
    """Fetch, filter and commit each batch before requesting the next one."""
    inserted = 0
    found = 0
    tickers = sorted(required)
    for start in range(0, len(tickers), batch_size):
        batch = tickers[start:start + batch_size]
        logger.info("  [daily-retry] Batch %d-%d/%d",
                    start + 1, start + len(batch), len(tickers))
        if provider == "yahoo":
            ticker_data = fetch_daily_yahoo_direct(batch, days=days)
        else:
            ticker_data = fetch_daily_twelvedata(batch, days=days)
        batch_inserted, batch_found = _upsert_required_daily_rows(
            ticker_data, {ticker: required[ticker] for ticker in batch}
        )
        inserted += batch_inserted
        found += batch_found
    return inserted, found


def retry_daily_gaps(tickers: list[str], provider: str, max_attempts: int = 2,
                     wait_seconds: int = 15) -> dict[str, list]:
    """Retry only missing official daily rows, then queue what remains unresolved."""
    attempts = 0
    gaps = _detect_daily_gaps(tickers, days=7)
    while gaps and attempts < max_attempts:
        attempts += 1
        targets = sorted(gaps)
        logger.info("  [daily-retry] Attempt %d/%d for %d ticker(s)",
                    attempts, max_attempts, len(targets))
        inserted, found = _fetch_and_upsert_daily_batches(gaps, provider, days=10)
        logger.info("  [daily-retry] Attempt %d persisted %d/%d target row(s)",
                    attempts, inserted, sum(len(dates) for dates in gaps.values()))

        gaps = _detect_daily_gaps(tickers, days=7)
        if gaps and attempts < max_attempts and wait_seconds > 0:
            logger.info("  [daily-retry] %d ticker(s) still missing; waiting %ss",
                        len(gaps), wait_seconds)
            time.sleep(wait_seconds)

    resolved = _resolve_daily_failures(tickers)
    queued = _record_daily_failures(gaps, provider, attempts) if gaps else 0
    if resolved:
        logger.info("  [daily-retry] Resolved %d queued failure(s)", resolved)
    if queued:
        logger.error("  [daily-retry] %d row(s) remain unresolved; queued for manual retry", queued)
        for ticker, dates in sorted(gaps.items()):
            logger.error("    %s: %s", ticker, ", ".join(str(d) for d in dates))
    else:
        logger.info("  [daily-retry] All official daily rows are present")
    return gaps


def retry_queued_daily_failures(provider: str, tickers: list[str] | None = None) -> int:
    """Retry unresolved daily queue entries and return the remaining row count."""
    _ensure_ingestion_failures_table()
    ticker_clause = "AND ticker = ANY(%s)" if tickers else ""
    params = [tickers] if tickers else []
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT ticker, MIN(trade_date) AS first_date
            FROM data_ingestion_failures
            WHERE dataset = 'daily' AND resolved_at IS NULL {ticker_clause}
            GROUP BY ticker ORDER BY ticker
        """, params)
        queued = cur.fetchall()
    targets = [row["ticker"] for row in queued]
    if not targets:
        logger.info("[daily-retry] No unresolved daily failures")
        return 0

    oldest = min(row["first_date"] for row in queued)
    days = max(10, (_get_et_now().date() - oldest).days + 5)
    logger.info("[daily-retry] Retrying %d ticker(s), oldest queued date %s",
                len(targets), oldest)

    required: dict[str, list] = {}
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT ticker, trade_date
            FROM data_ingestion_failures
            WHERE dataset = 'daily' AND resolved_at IS NULL {ticker_clause}
            ORDER BY ticker, trade_date
        """, params)
        for row in cur.fetchall():
            required.setdefault(row["ticker"], []).append(row["trade_date"])
    inserted, _ = _fetch_and_upsert_daily_batches(required, provider, days=days)
    logger.info("[daily-retry] Manual retry persisted %d target row(s)", inserted)
    _resolve_daily_failures(targets)

    with get_db_cursor() as cur:
        cur.execute(f"""
            UPDATE data_ingestion_failures
            SET attempts = attempts + 1, last_attempt_at = NOW()
            WHERE dataset = 'daily' AND resolved_at IS NULL {ticker_clause}
        """, params)
        cur.execute(f"""
            SELECT ticker, trade_date, attempts
            FROM data_ingestion_failures
            WHERE dataset = 'daily' AND resolved_at IS NULL {ticker_clause}
            ORDER BY trade_date, ticker
        """, params)
        unresolved = cur.fetchall()
    if unresolved:
        logger.error("[daily-retry] Manual retry left %d unresolved row(s):", len(unresolved))
        for row in unresolved:
            logger.error("  %s %s (attempts=%s)",
                         row["trade_date"], row["ticker"], row["attempts"])
    else:
        logger.info("[daily-retry] Manual retry resolved all queued rows")
    return len(unresolved)


# ── Job: Deep hourly backfill ───────────────────────────────────────
def job_hourly_deep_backfill(tickers: list[str], provider: str, min_days: int = 400,
                             force: bool = False):
    """Re-pull Yahoo's full 730-day hourly window for tickers whose history has eroded.

    The routine hourly job only fetches a few days, so as Yahoo's rolling limit
    advances the deep history erodes. This repairs it. Tickers already at or above
    `min_days` are skipped, so the weekly run is usually near-free; `force` re-pulls
    everything regardless.
    """
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT ticker FROM stock_prices_hourly
            GROUP BY ticker
            HAVING COUNT(DISTINCT (datetime AT TIME ZONE 'America/New_York')::date) >= %s
        """, (min_days,))
        deep = {r["ticker"] for r in cur.fetchall()}

    targets = tickers if force else [t for t in tickers if t not in deep]
    logger.info("  [hourly-deep] %s/%s tickers have >=%s days; refetching %s",
                len(deep & set(tickers)), len(tickers), min_days, len(targets))

    if not targets:
        logger.info("  [hourly-deep] Nothing to repair — skipping fetch")
        return

    ticker_data = fetch_hourly_yahoo_direct(targets, days=730)
    total = 0
    for ticker, df in ticker_data.items():
        if df is not None and not df.empty:
            total += upsert_hourly_prices(ticker, df)
    logger.info("  [hourly-deep] Done — %s rows upserted across %s tickers",
                total, len(ticker_data))


# ── Job: Cross-sectional signal ─────────────────────────────────────
def job_cross_sectional_signal():
    """Score the universe with the validated momentum model → cross_sectional_signals."""
    try:
        from generate_cross_sectional_signal import compute_signal, ensure_table, persist
    except ImportError:
        sys.path.insert(0, str(Path(__file__).parent))
        from generate_cross_sectional_signal import compute_signal, ensure_table, persist

    cross = compute_signal(None)
    if cross.empty:
        logger.warning("  [xs-signal] No signal produced — insufficient data")
        return
    ensure_table()
    written = persist(cross)
    longs = int((cross["side"] == "LONG").sum())
    shorts = int((cross["side"] == "SHORT").sum())
    logger.info("  [xs-signal] Done — %d rows (%d LONG / %d SHORT) for %s",
                written, longs, shorts, cross["date"].iloc[0].date())


def job_market_discovery():
    """Persist continuation and reversal discovery states in shadow mode."""
    try:
        from generate_market_discovery import compute, ensure_table, persist
    except ImportError:
        sys.path.insert(0, str(Path(__file__).parent))
        from generate_market_discovery import compute, ensure_table, persist

    states = compute(None)
    if states.empty:
        logger.warning("  [discovery] No states produced — insufficient data")
        return
    ensure_table()
    written = persist(states)
    summary = states["state"].value_counts().to_dict()
    logger.info("  [discovery] Done — %d rows for %s | %s",
                written, states["trade_date"].iloc[0], summary)


def job_scanner_events():
    """Capture shadow timing events and evaluate only newly due horizons."""
    try:
        from run_scanner_event_pipeline import run_pipeline
    except ImportError:
        sys.path.insert(0, str(Path(__file__).parent))
        from run_scanner_event_pipeline import run_pipeline
    intervals = ("1d", "1h", "1wk") if _get_et_now().weekday() == 4 else ("1d", "1h")
    results = run_pipeline(intervals=intervals)
    logger.info("  [scanner-events] Done — %s", results)


def run_eod_once(tickers: list[str], provider: str):
    """Run the scheduler's post-close sequence once, then return validation results."""
    now_et = _get_et_now()
    logger.info("[JOB] End-of-day daily close @ %s ET", now_et.strftime("%H:%M"))
    job_daily_close(tickers, provider)

    logger.info("[JOB] Final hourly update @ %s ET", _get_et_now().strftime("%H:%M"))
    job_hourly(tickers, provider)

    # Not needed by the signal, so a failure here must not block it.
    logger.info("[JOB] Final 5m intraday update @ %s ET", _get_et_now().strftime("%H:%M"))
    try:
        job_intraday_5m(tickers, provider)
    except Exception:
        logger.exception("[JOB] 5m intraday update failed — daily/hourly are still valid")

    logger.info("[JOB] EOD validation @ %s ET", _get_et_now().strftime("%H:%M"))
    results = validate_all(days=7)

    if any(not result.clean for result in results):
        logger.info("[JOB] Validation found issues — running backfill @ %s ET", _get_et_now().strftime("%H:%M"))
        job_backfill(tickers, provider)
        retry_daily_gaps(tickers, provider)
        logger.info("[JOB] Re-validating after backfill @ %s ET", _get_et_now().strftime("%H:%M"))
        results = validate_all(days=7)

    # Runs last: the signal reads the daily closes written above.
    if any(not result.clean for result in results):
        logger.error("[JOB] Signal skipped — EOD data remains incomplete after backfill")
    else:
        logger.info("[JOB] Cross-sectional signal @ %s ET", _get_et_now().strftime("%H:%M"))
        try:
            job_cross_sectional_signal()
        except Exception:
            logger.exception("[JOB] Cross-sectional signal failed — prices are still valid")

        logger.info("[JOB] Market discovery (shadow) @ %s ET", _get_et_now().strftime("%H:%M"))
        try:
            job_market_discovery()
        except Exception:
            logger.exception("[JOB] Market discovery failed — validated signal is unchanged")

        logger.info("[JOB] Scanner events (shadow) @ %s ET", _get_et_now().strftime("%H:%M"))
        try:
            job_scanner_events()
        except Exception:
            logger.exception("[JOB] Scanner event pipeline failed — recommendations are unchanged")

    # Saturday only: repair the deep hourly window before it rolls out of Yahoo's limit.
    if _get_et_now().weekday() == 5:
        logger.info("[JOB] Weekly deep hourly backfill @ %s ET", _get_et_now().strftime("%H:%M"))
        try:
            job_hourly_deep_backfill(tickers, provider)
        except Exception:
            logger.exception("[JOB] Deep hourly backfill failed — recent bars are still valid")

    logger.info("Daily close complete. Next jobs resume tomorrow at market open.")
    return results


# ── Gap detection & auto-backfill ────────────────────────────────────
def _get_trading_days(days: int) -> set:
    """Return recent sessions observed by any price table."""
    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute("""
            SELECT d FROM (
                SELECT DISTINCT datetime::date AS d FROM stock_prices_daily
                UNION
                SELECT DISTINCT (datetime AT TIME ZONE 'America/New_York')::date AS d
                FROM stock_prices_hourly
                UNION
                SELECT DISTINCT (datetime AT TIME ZONE 'America/New_York')::date AS d
                FROM stock_prices_intraday
            ) sessions
            ORDER BY d DESC
            LIMIT %s
        """, (days,))
        return {row[0] for row in cur.fetchall()}


def _detect_daily_gaps(tickers: list[str], days: int = 7) -> dict[str, list]:
    """Find tickers missing daily candles for recent trading days."""
    expected_dates = _get_trading_days(days)
    if not expected_dates:
        return {}

    cutoff = min(expected_dates).strftime("%Y-%m-%d")
    gaps = {}

    with get_db_cursor(dict_cursor=False) as cur:
        for ticker in tickers:
            cur.execute(
                "SELECT DISTINCT datetime::date FROM stock_prices_daily "
                "WHERE ticker = %s AND datetime >= %s",
                (ticker, cutoff),
            )
            existing = {row[0] for row in cur.fetchall()}
            missing = sorted(expected_dates - existing)
            if missing:
                gaps[ticker] = missing

    return gaps


def _detect_hourly_gaps(tickers: list[str], days: int = 3) -> dict[str, list]:
    """Find tickers missing hourly candles for recent trading days.
    Expects 7 hourly bars per trading day (9:30, 10:30, ..., 15:30 ET)."""
    expected_dates = _get_trading_days(days)
    if not expected_dates:
        return {}

    cutoff = min(expected_dates).strftime("%Y-%m-%d")
    gaps = {}

    with get_db_cursor(dict_cursor=False) as cur:
        for ticker in tickers:
            cur.execute(
                "SELECT (datetime AT TIME ZONE 'America/New_York')::date AS d, COUNT(*) "
                "FROM stock_prices_hourly "
                "WHERE ticker = %s AND datetime >= %s "
                "GROUP BY d",
                (ticker, cutoff),
            )
            date_counts = {row[0]: row[1] for row in cur.fetchall()}
            missing = sorted(
                d for d in expected_dates
                if date_counts.get(d, 0) < 5  # at least 5 of 7 bars expected
            )
            if missing:
                gaps[ticker] = missing

    return gaps


def _detect_intraday_gaps(tickers: list[str], days: int = 2) -> dict[str, list]:
    """Find tickers missing 5m intraday candles for recent trading days.
    Expects ~78 bars per day for 5m interval (9:30-15:55)."""
    expected_dates = _get_trading_days(days)
    if not expected_dates:
        return {}

    cutoff = min(expected_dates).strftime("%Y-%m-%d")
    gaps = {}

    with get_db_cursor(dict_cursor=False) as cur:
        for ticker in tickers:
            cur.execute(
                "SELECT (datetime AT TIME ZONE 'America/New_York')::date AS d, COUNT(*) "
                "FROM stock_prices_intraday "
                "WHERE ticker = %s AND interval = '5m' AND datetime >= %s "
                "GROUP BY d",
                (ticker, cutoff),
            )
            date_counts = {row[0]: row[1] for row in cur.fetchall()}
            missing = sorted(
                d for d in expected_dates
                if date_counts.get(d, 0) < 50  # at least 50 of ~78 bars expected
            )
            if missing:
                gaps[ticker] = missing

    return gaps


def job_backfill(tickers: list[str], provider: str):
    """Detect and backfill gaps across all three price tables."""
    logger.info("  [backfill] Scanning for data gaps...")

    # ── Daily gaps ───────────────────────────────────────────────
    daily_gaps = _detect_daily_gaps(tickers, days=7)
    if daily_gaps:
        gap_tickers = list(daily_gaps.keys())
        logger.info("  [backfill] Daily gaps: %d tickers missing data", len(gap_tickers))
        # Fetch last 10 days to cover any gaps within the 7-day window
        if provider == "yahoo":
            ticker_data = fetch_daily_yahoo_direct(gap_tickers, days=10)
        else:
            ticker_data = fetch_daily_twelvedata(gap_tickers, days=10)

        end_dt = datetime.utcnow() + timedelta(days=1)
        start_dt = end_dt - timedelta(days=15)
        total_filled = 0
        for ticker in gap_tickers:
            df = ticker_data.get(ticker)
            if df is not None and not df.empty:
                existing = get_existing_dates(
                    ticker, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
                )
                inserted, _ = upsert_prices(ticker, df, existing)
                total_filled += inserted
        logger.info("  [backfill] Daily: +%d rows filled for %d tickers", total_filled, len(gap_tickers))
    else:
        logger.info("  [backfill] Daily: no gaps found")

    # ── Hourly gaps ──────────────────────────────────────────────
    hourly_gaps = _detect_hourly_gaps(tickers, days=3)
    if hourly_gaps:
        gap_tickers = list(hourly_gaps.keys())
        logger.info("  [backfill] Hourly gaps: %d tickers missing data", len(gap_tickers))
        if provider == "yahoo":
            ticker_data = fetch_hourly_yahoo_direct(gap_tickers, days=5)
        else:
            ticker_data = {}
            for i, t in enumerate(gap_tickers, 1):
                df = fetch_hourly_candles(t, days=5)
                if not df.empty:
                    ticker_data[t] = df
                if i < len(gap_tickers):
                    time.sleep(TD_RATE_LIMIT)

        total_filled = 0
        for ticker in gap_tickers:
            df = ticker_data.get(ticker)
            if df is not None and not df.empty:
                total_filled += upsert_hourly_prices(ticker, df)
        logger.info("  [backfill] Hourly: +%d rows filled for %d tickers", total_filled, len(gap_tickers))
    else:
        logger.info("  [backfill] Hourly: no gaps found")

    # ── Intraday 5m gaps (Yahoo only) ───────────────────────────
    if provider == "yahoo":
        intraday_gaps = _detect_intraday_gaps(tickers, days=2)
        if intraday_gaps:
            gap_tickers = list(intraday_gaps.keys())
            logger.info("  [backfill] Intraday gaps: %d tickers missing data", len(gap_tickers))
            ticker_data = fetch_intraday_yahoo_direct(gap_tickers, "5m", days=3)
            total_filled = 0
            for ticker in gap_tickers:
                df = ticker_data.get(ticker)
                if df is not None and not df.empty:
                    total_filled += upsert_intraday(ticker, df, "5m")
            logger.info("  [backfill] Intraday: +%d rows filled for %d tickers", total_filled, len(gap_tickers))
        else:
            logger.info("  [backfill] Intraday: no gaps found")
    else:
        logger.info("  [backfill] Intraday: skipped (Twelve Data provider)")

    logger.info("  [backfill] Complete.")


# ── Main scheduler loop ─────────────────────────────────────────────
def run_scheduler(tickers: list[str], provider: str):
    """Main scheduling loop — runs until Ctrl+C."""
    logger.info("=" * 60)
    logger.info("SCHEDULER STARTED")
    logger.info("  Provider : %s", provider)
    logger.info("  Tickers  : %d", len(tickers))
    logger.info("  Jobs     :")
    logger.info("    5m candles   → every 5 min  (market hours) %s",
                "[yahoo only]" if provider != "yahoo" else "")
    logger.info("    daily candle → every 5 min  (market hours)")
    logger.info("    hourly       → every 60 min (market hours)")
    logger.info("    daily close  → once after 4:15 PM ET")
    logger.info("    eod validate → once after daily close")
    logger.info("    backfill     → on startup + after eod validate")
    logger.info("  Press Ctrl+C to stop")
    logger.info("=" * 60)

    # Run backfill on startup to fill any gaps from downtime
    logger.info("[JOB] Startup backfill — checking for data gaps...")
    try:
        job_backfill(tickers, provider)
    except Exception:
        logger.exception("Startup backfill failed — continuing with scheduler")

    # Track last-run timestamps (epoch seconds)
    last_5m = 0
    last_daily_candle = 0
    last_hourly = 0
    daily_close_done_today = None  # date when daily close was last completed

    while True:
        try:
            now = time.time()
            now_et = _get_et_now()
            today_date = now_et.date()

            if is_market_hours():
                ran_job = False

                # ── 5m candles (every 5 min) ─────────────────────
                if now - last_5m >= INTRADAY_5M_INTERVAL:
                    logger.info("[JOB] 5m intraday candles @ %s ET", now_et.strftime("%H:%M"))
                    job_intraday_5m(tickers, provider)
                    last_5m = now
                    ran_job = True

                # ── Daily candle update (every 5 min) ────────────
                if now - last_daily_candle >= DAILY_CANDLE_INTERVAL:
                    logger.info("[JOB] Daily candle update @ %s ET", now_et.strftime("%H:%M"))
                    job_daily_candle(tickers, provider)
                    last_daily_candle = now
                    ran_job = True

                # ── Hourly candles (every 60 min) ────────────────
                if now - last_hourly >= HOURLY_INTERVAL:
                    logger.info("[JOB] Hourly candle update @ %s ET", now_et.strftime("%H:%M"))
                    job_hourly(tickers, provider)
                    last_hourly = now
                    ran_job = True

                # Reset daily close flag on new trading day
                if daily_close_done_today != today_date:
                    daily_close_done_today = None

                if not ran_job:
                    logger.debug("Market open — no jobs due. Next check in %ds", LOOP_SLEEP)

            elif is_post_close_window() and daily_close_done_today != today_date:
                # ── Daily close (once after market close) ────────
                run_eod_once(tickers, provider)
                daily_close_done_today = today_date

            else:
                day_name = now_et.strftime("%A")
                logger.info("Market closed (%s %s ET). Sleeping...",
                            day_name, now_et.strftime("%H:%M"))

            time.sleep(LOOP_SLEEP)

        except KeyboardInterrupt:
            logger.info("\nScheduler stopped by user.")
            break
        except Exception:
            logger.exception("Unexpected error in scheduler loop — retrying in 60s")
            time.sleep(60)


# ── CLI ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Unified price update scheduler — background process for all price jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Schedule (per design):
  5m candles   → stock_prices_intraday   (every 5 min, market hours, yahoo only)
  daily candle → stock_prices_daily      (every 5 min, market hours)
  hourly       → stock_prices_hourly     (every 60 min, market hours)
  daily close  → stock_prices_daily      (once after 4:15 PM ET)
        """,
    )
    parser.add_argument(
        "--provider", type=str, default="yahoo",
        choices=["yahoo", "twelvedata"],
        help="Data provider: yahoo (default, recommended) or twelvedata",
    )
    parser.add_argument(
        "--tickers", type=str, default=None,
        help="Comma-separated tickers. Default: all active selected_tickers",
    )
    parser.add_argument(
        "--eod-once", action="store_true",
        help="Run daily close, final hourly update, validation, and conditional backfill once, then exit",
    )
    parser.add_argument(
        "--hourly-deep-once", action="store_true",
        help="Repair eroded 730-day hourly history once, then exit (normally Saturday-only)",
    )
    parser.add_argument(
        "--retry-daily-failures", action="store_true",
        help="Retry unresolved official daily rows recorded by prior EOD runs, then exit",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="With --hourly-deep-once, refetch every ticker instead of only eroded ones",
    )
    args = parser.parse_args()

    provider = args.provider

    # Validate Twelve Data API key
    if provider == "twelvedata":
        api_key = os.getenv("TWELVEDATA_API_KEY", "")
        if not api_key:
            logger.error("TWELVEDATA_API_KEY not set. Get a free key at https://twelvedata.com/")
            sys.exit(1)
        logger.warning("Twelve Data provider: 5m intraday candles will be SKIPPED (rate limit too slow).")
        logger.warning("Use --provider yahoo for full 5m + hourly + daily support.")

    # Get tickers
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = get_selected_tickers(active_only=True)
        if not tickers:
            logger.error("No active tickers in selected_tickers table.")
            sys.exit(1)

    if args.retry_daily_failures:
        retry_queued_daily_failures(provider, tickers if args.tickers else None)
    elif args.hourly_deep_once:
        job_hourly_deep_backfill(tickers, provider, force=args.force)
    elif args.eod_once:
        run_eod_once(tickers, provider)
    else:
        run_scheduler(tickers, provider)


if __name__ == "__main__":
    main()
