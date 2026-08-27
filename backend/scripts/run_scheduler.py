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
  Polygon.io     — unlimited calls on Stocks Starter+. 15-min delayed data. Supports all jobs.
  Twelve Data    — 8 req/min free tier. Skips 5m candles (too slow for 400 tickers).

Usage:
    # Start scheduler (Polygon.io, default)
    python scripts/run_scheduler.py

    # Start with Yahoo Finance
    python scripts/run_scheduler.py --provider yahoo

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
from update_daily_prices import get_existing_dates, upsert_prices
from update_hourly_prices import fetch_hourly_yahoo_direct, upsert_hourly_prices
from update_running_daily import upsert_intraday_price
from update_intraday_prices import upsert_intraday
from validate_eod import validate_all

from providers import get_provider

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
CLOSED_LOG_INTERVAL = 10 * 60       # log closed-market heartbeat every 10 minutes
SCANNER_INTRADAY_INTERVAL = 60 * 60  # run 1h scanner pipeline every hour during market hours
HOURLY_SCANNER_REPAIR_SESSIONS = 2   # rebuild recent 1h outcomes from final bars at EOD
OUTCOME_REPAIR_EVAL_LIMIT = 5000


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
    """Fetch today's 5m candles → stock_prices_intraday."""
    provider_obj = get_provider(provider)
    if not provider_obj.supports_intraday():
        logger.info("  [5m] Skipped — %s does not support 5m with %d tickers", provider, len(tickers))
        return

    logger.info("  [5m] Fetching 5m candles from %s...", provider)
    ticker_data = provider_obj.fetch_intraday(tickers, "5m", days=1)
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
    provider_obj = get_provider(provider)
    logger.info("  [daily-candle] Fetching from %s...", provider)
    ticker_data = provider_obj.fetch_daily(tickers, days=1)
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

    success = 0
    for ticker in tickers:
        if ticker in quotes and upsert_intraday_price(ticker, quotes[ticker]):
            success += 1
    logger.info("  [daily-candle] Done — %d/%d tickers updated in stock_prices_daily", success, len(tickers))


# ── Job: Hourly candles ─────────────────────────────────────────────
def job_hourly(tickers: list[str], provider: str):
    """Fetch recent hourly candles → stock_prices_hourly."""
    provider_obj = get_provider(provider)
    logger.info("  [hourly] Fetching from %s...", provider)
    ticker_data = provider_obj.fetch_hourly(tickers, days=2)

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
    ticker_data = get_provider(provider).fetch_daily(tickers, days=5)

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
            # upsert_prices skips dates already stored, so the latest session must be
            # dropped from `existing` to overwrite the provisional running-daily bar.
            # Derive it from the provider frame: keying off "today" silently no-ops
            # whenever the run happens after midnight ET.
            latest_session = max(
                (idx.replace(tzinfo=None).date() if getattr(idx, "tzinfo", None) else idx.date())
                for idx in df.index
            )
            existing.discard(latest_session)
            inserted, _ = upsert_prices(ticker, df, existing)
            total_inserted += inserted
            success += 1
    logger.info("  [daily-close] Done — +%d new rows (%d/%d tickers)", total_inserted, success, len(tickers))


# A finalized daily bar always contains its own session; a provisional running-daily
# bar written mid-session does not. These bounds were measured against 12 clean
# sessions: zero high/low violations and a minimum daily/hourly volume ratio of 1.006.
DAILY_ENVELOPE_TOLERANCE = 0.01
DAILY_VOLUME_FLOOR = 0.90


def _latest_daily_session():
    """Most recent trading date in stock_prices_daily.

    Derived from stored data rather than the wall clock so the guard stays correct
    when the run happens after midnight ET.
    """
    with get_db_cursor() as cur:
        cur.execute("SELECT MAX((datetime AT TIME ZONE 'UTC')::date) AS d "
                    "FROM stock_prices_daily")
        return cur.fetchone()["d"]


def find_provisional_daily_rows(trade_date, tickers: list[str] | None = None) -> dict:
    """Detect daily bars that do not envelop their own session's hourly tape.

    Catches an end-of-day overwrite that was silently skipped rather than failed,
    which leaves the intraday running-daily bar in place as if it were official.
    """
    ticker_clause = "AND d.ticker = ANY(%s)" if tickers else ""
    params = [trade_date, trade_date]
    if tickers:
        params.append(tickers)
    params.extend([DAILY_ENVELOPE_TOLERANCE, DAILY_ENVELOPE_TOLERANCE,
                   DAILY_VOLUME_FLOOR])
    with get_db_cursor() as cur:
        cur.execute(f"""
            WITH h AS (
                SELECT ticker, MAX(high) AS hi, MIN(low) AS lo, SUM(volume) AS vol
                FROM stock_prices_hourly
                WHERE (datetime AT TIME ZONE 'America/New_York')::date = %s
                GROUP BY ticker
            ), d AS (
                SELECT d.ticker, d.high, d.low, d.volume
                FROM stock_prices_daily d
                WHERE (d.datetime AT TIME ZONE 'UTC')::date = %s
                  {ticker_clause}
            )
            SELECT d.ticker
            FROM d JOIN h USING (ticker)
            WHERE d.high < h.hi - %s
               OR d.low  > h.lo + %s
               OR d.volume < h.vol * %s
            ORDER BY d.ticker
        """, params)
        provisional = [row["ticker"] for row in cur.fetchall()]

        unverifiable_params = [trade_date]
        if tickers:
            unverifiable_params.append(tickers)
        unverifiable_params.append(trade_date)
        cur.execute(f"""
            SELECT COUNT(*) AS n FROM stock_prices_daily d
            WHERE (d.datetime AT TIME ZONE 'UTC')::date = %s
              {ticker_clause}
              AND NOT EXISTS (
                  SELECT 1 FROM stock_prices_hourly h
                  WHERE h.ticker = d.ticker
                    AND (h.datetime AT TIME ZONE 'America/New_York')::date = %s
              )
        """, unverifiable_params)
        unverifiable = int(cur.fetchone()["n"])

    return {"provisional": provisional, "unverifiable": unverifiable}


def _guard_daily_session(trade_date, tickers: list[str], provider: str) -> bool:
    """Verify the session's daily bars are final, repairing once before giving up."""
    check = find_provisional_daily_rows(trade_date, tickers)
    if check["unverifiable"]:
        logger.warning("  [guard] %d tickers have no hourly bars for %s — daily bar "
                       "cannot be verified", check["unverifiable"], trade_date)
    if not check["provisional"]:
        logger.info("  [guard] Daily bars for %s are final (%d tickers verified)",
                    trade_date, len(tickers) - check["unverifiable"])
        return True

    logger.warning("  [guard] %d daily bars for %s are still provisional — retrying "
                   "daily close", len(check["provisional"]), trade_date)
    job_daily_close(check["provisional"], provider)

    recheck = find_provisional_daily_rows(trade_date, check["provisional"])
    if not recheck["provisional"]:
        logger.info("  [guard] Daily bars repaired for %s", trade_date)
        return True

    logger.error("  [guard] %d daily bars remain provisional for %s: %s",
                 len(recheck["provisional"]), trade_date,
                 ",".join(recheck["provisional"][:20]))
    _record_daily_failures(
        {ticker: [trade_date] for ticker in recheck["provisional"]}, provider, 1
    )
    return False


def _post_run_audit(trade_date, tickers: list[str], provider: str, session_final: bool) -> dict:
    """Final EOD sanity sweep: coverage, guard invariant, and derived-table output.

    Logs one summary line every run so a silent partial failure surfaces immediately
    instead of being discovered days later. Self-heals the two derived stages that
    should never legitimately be empty by retrying them once; scanner events are only
    reported since zero matches on a given day is a normal outcome, not a defect.
    """
    active = len(tickers)
    with get_db_cursor() as cur:
        cur.execute("""
                WITH active AS (
                     SELECT UNNEST(%s::text[]) AS ticker
                )
            SELECT
                  (SELECT COUNT(DISTINCT p.ticker) FROM stock_prices_daily p
                      JOIN active a USING (ticker)
                      WHERE (p.datetime AT TIME ZONE 'UTC')::date = %s)               AS daily,
                  (SELECT COUNT(DISTINCT p.ticker) FROM stock_prices_hourly p
                      JOIN active a USING (ticker)
                      WHERE (p.datetime AT TIME ZONE 'America/New_York')::date = %s)  AS hourly,
                  (SELECT COUNT(DISTINCT p.ticker) FROM stock_prices_intraday p
                      JOIN active a USING (ticker)
                      WHERE (p.datetime AT TIME ZONE 'America/New_York')::date = %s)  AS intraday,
                  (SELECT COUNT(*) FROM cross_sectional_signals s
                      JOIN active a USING (ticker)
                      WHERE s.trade_date = %s)                                       AS signals,
                  (SELECT COUNT(*) FROM market_discovery_states m
                      JOIN active a USING (ticker)
                      WHERE m.trade_date = %s)                                       AS discovery,
                  (SELECT COUNT(*) FROM scanner_events e
                      JOIN active a USING (ticker)
                      WHERE e.trade_date = %s)                                       AS events,
                  (SELECT COUNT(*) FROM data_ingestion_failures f
                      JOIN active a USING (ticker)
                      WHERE f.resolved_at IS NULL)                                   AS unresolved
          """, (tickers, trade_date, trade_date, trade_date, trade_date, trade_date, trade_date))
        counts = dict(cur.fetchone())

    recheck = find_provisional_daily_rows(trade_date, tickers)
    counts["provisional"] = len(recheck["provisional"])

    issues = []
    if counts["daily"] < active:
        issues.append(f"daily coverage {counts['daily']}/{active}")
    if counts["hourly"] < active:
        issues.append(f"hourly coverage {counts['hourly']}/{active}")
    if counts["provisional"]:
        issues.append(f"{counts['provisional']} daily bars still provisional")
    if counts["unresolved"]:
        issues.append(f"{counts['unresolved']} unresolved ingestion failures")

    if session_final:
        if counts["signals"] == 0:
            logger.warning("  [audit] Cross-sectional signal produced 0 rows — retrying once")
            try:
                job_cross_sectional_signal()
            except Exception:
                logger.exception("  [audit] Signal retry failed")
                issues.append("cross-sectional signal retry failed")
            else:
                with get_db_cursor() as cur:
                    cur.execute("SELECT COUNT(*) AS n FROM cross_sectional_signals "
                               "WHERE trade_date = %s AND ticker = ANY(%s)", (trade_date, tickers))
                    counts["signals"] = cur.fetchone()["n"]
                if counts["signals"] == 0:
                    issues.append("cross-sectional signal remained empty after retry")
        if counts["discovery"] == 0:
            logger.warning("  [audit] Market discovery produced 0 rows — retrying once")
            try:
                job_market_discovery()
            except Exception:
                logger.exception("  [audit] Discovery retry failed")
                issues.append("market discovery retry failed")
            else:
                with get_db_cursor() as cur:
                    cur.execute("SELECT COUNT(*) AS n FROM market_discovery_states "
                               "WHERE trade_date = %s AND ticker = ANY(%s)", (trade_date, tickers))
                    counts["discovery"] = cur.fetchone()["n"]
                if counts["discovery"] == 0:
                    issues.append("market discovery remained empty after retry")

    summary = (
        f"daily={counts['daily']}/{active} hourly={counts['hourly']}/{active} "
        f"intraday={counts['intraday']}/{active} provisional={counts['provisional']} "
        f"signals={counts['signals']} discovery={counts['discovery']} "
        f"events={counts['events']} unresolved_failures={counts['unresolved']}"
    )
    if issues:
        logger.error("  [audit] EOD issues for %s: %s | %s", trade_date, "; ".join(issues), summary)
    else:
        logger.info("  [audit] EOD clean for %s | %s", trade_date, summary)

    return {"counts": counts, "issues": issues}


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
    provider_obj = get_provider(provider)
    for start in range(0, len(tickers), batch_size):
        batch = tickers[start:start + batch_size]
        logger.info("  [daily-retry] Batch %d-%d/%d",
                    start + 1, start + len(batch), len(tickers))
        ticker_data = provider_obj.fetch_daily(batch, days=days)
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

    This is a Yahoo-specific workaround for its rolling 730-day window; providers
    with a fixed multi-year history window (e.g. Polygon Starter's 5 years) don't
    erode the same way, so this job is a no-op for them.
    """
    if provider != "yahoo":
        logger.info("  [hourly-deep] Skipped — only needed for Yahoo's rolling 730-day window")
        return

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


def job_scanner_events(intervals: tuple[str, ...]):
    """Capture/evaluate scanner events for explicit intervals only."""
    try:
        from run_scanner_event_pipeline import run_pipeline
    except ImportError:
        sys.path.insert(0, str(Path(__file__).parent))
        from run_scanner_event_pipeline import run_pipeline

    results = run_pipeline(intervals=intervals)
    logger.info("  [scanner-events %s] Done — %s", ",".join(intervals), results)


def job_repair_recent_scanner_outcomes(
    interval: str = "1h",
    sessions: int = HOURLY_SCANNER_REPAIR_SESSIONS,
    evaluation_limit: int = OUTCOME_REPAIR_EVAL_LIMIT,
):
    """Delete and regenerate recent scanner outcomes for one interval."""
    try:
        from research.scanner_events import evaluate_outcomes, repair_recent_outcomes
    except ImportError:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from research.scanner_events import evaluate_outcomes, repair_recent_outcomes

    repair = repair_recent_outcomes(interval, sessions=sessions)
    logger.info("  [scanner-repair %s] deleted=%s sessions=%s dates=%s",
                interval, repair["deleted"], repair["sessions"], ",".join(repair["session_dates"]))

    total_due = 0
    total_inserted = 0
    batches = 0
    while True:
        batch = evaluate_outcomes(interval, limit=evaluation_limit)
        batches += 1
        total_due += int(batch.get("due", 0))
        total_inserted += int(batch.get("inserted", 0))
        if int(batch.get("due", 0)) < evaluation_limit:
            break

    logger.info("  [scanner-repair %s] rebuilt inserted=%s due=%s batches=%s",
                interval, total_inserted, total_due, batches)


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

    logger.info("[JOB] Daily-bar guard @ %s ET", _get_et_now().strftime("%H:%M"))
    session_final = _guard_daily_session(_latest_daily_session(), tickers, provider)

    # Runs last: the signal reads the daily closes written above.
    if any(not result.clean for result in results):
        logger.error("[JOB] Signal skipped — EOD data remains incomplete after backfill")
    elif not session_final:
        logger.error("[JOB] Signal skipped — daily bars are provisional; derived data "
                     "would be built from mid-session prices")
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

        logger.info("[JOB] Repair hourly scanner outcomes @ %s ET", _get_et_now().strftime("%H:%M"))
        try:
            job_repair_recent_scanner_outcomes()
        except Exception:
            logger.exception("[JOB] Hourly scanner repair failed — continuing with standard pipeline")

        eod_intervals = ("1d", "1h", "1wk") if _get_et_now().weekday() == 4 else ("1d", "1h")
        logger.info("[JOB] Scanner events (shadow) %s @ %s ET",
                    ",".join(eod_intervals), _get_et_now().strftime("%H:%M"))
        try:
            job_scanner_events(eod_intervals)
        except Exception:
            logger.exception("[JOB] Scanner event pipeline failed — recommendations are unchanged")

    # Saturday only: repair the deep hourly window before it rolls out of Yahoo's limit.
    if _get_et_now().weekday() == 5:
        logger.info("[JOB] Weekly deep hourly backfill @ %s ET", _get_et_now().strftime("%H:%M"))
        try:
            job_hourly_deep_backfill(tickers, provider)
        except Exception:
            logger.exception("[JOB] Deep hourly backfill failed — recent bars are still valid")

    logger.info("[JOB] Post-run audit @ %s ET", _get_et_now().strftime("%H:%M"))
    try:
        _post_run_audit(_latest_daily_session(), tickers, provider, session_final)
    except Exception:
        logger.exception("[JOB] Post-run audit failed — see prior stage logs for the true state")

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
    provider_obj = get_provider(provider)

    # ── Daily gaps ───────────────────────────────────────────────
    daily_gaps = _detect_daily_gaps(tickers, days=7)
    if daily_gaps:
        gap_tickers = list(daily_gaps.keys())
        logger.info("  [backfill] Daily gaps: %d tickers missing data", len(gap_tickers))
        # Fetch last 10 days to cover any gaps within the 7-day window
        ticker_data = provider_obj.fetch_daily(gap_tickers, days=10)

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
        ticker_data = provider_obj.fetch_hourly(gap_tickers, days=5)

        total_filled = 0
        for ticker in gap_tickers:
            df = ticker_data.get(ticker)
            if df is not None and not df.empty:
                total_filled += upsert_hourly_prices(ticker, df)
        logger.info("  [backfill] Hourly: +%d rows filled for %d tickers", total_filled, len(gap_tickers))
    else:
        logger.info("  [backfill] Hourly: no gaps found")

    # ── Intraday 5m gaps ─────────────────────────────────────────
    if provider_obj.supports_intraday():
        intraday_gaps = _detect_intraday_gaps(tickers, days=2)
        if intraday_gaps:
            gap_tickers = list(intraday_gaps.keys())
            logger.info("  [backfill] Intraday gaps: %d tickers missing data", len(gap_tickers))
            ticker_data = provider_obj.fetch_intraday(gap_tickers, "5m", days=3)
            total_filled = 0
            for ticker in gap_tickers:
                df = ticker_data.get(ticker)
                if df is not None and not df.empty:
                    total_filled += upsert_intraday(ticker, df, "5m")
            logger.info("  [backfill] Intraday: +%d rows filled for %d tickers", total_filled, len(gap_tickers))
        else:
            logger.info("  [backfill] Intraday: no gaps found")
    else:
        logger.info("  [backfill] Intraday: skipped (%s provider does not support 5m)", provider)

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
                "" if get_provider(provider).supports_intraday() else "[unsupported by this provider]")
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
    last_scanner_intraday = 0
    last_closed_log = 0
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

                # ── Intraday scanner events for day trading (hourly cadence) ──
                if now - last_scanner_intraday >= SCANNER_INTRADAY_INTERVAL:
                    logger.info("[JOB] Scanner events (1h intraday) @ %s ET", now_et.strftime("%H:%M"))
                    try:
                        job_scanner_events(("1h",))
                    except Exception:
                        logger.exception("[JOB] Intraday scanner event pipeline failed — continuing")
                    last_scanner_intraday = now
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
                if now - last_closed_log >= CLOSED_LOG_INTERVAL:
                    day_name = now_et.strftime("%A")
                    logger.info("Market closed (%s %s ET). Sleeping...",
                                day_name, now_et.strftime("%H:%M"))
                    last_closed_log = now

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
  5m candles   → stock_prices_intraday   (every 5 min, market hours, yahoo/polygon only)
  daily candle → stock_prices_daily      (every 5 min, market hours)
  hourly       → stock_prices_hourly     (every 60 min, market hours)
  daily close  → stock_prices_daily      (once after 4:15 PM ET)
        """,
    )
    parser.add_argument(
        "--provider", type=str, default="polygon",
        choices=["yahoo", "twelvedata", "polygon"],
        help="Data provider: polygon (default), yahoo, or twelvedata",
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

    # Validate provider API keys
    if provider == "twelvedata":
        api_key = os.getenv("TWELVEDATA_API_KEY", "")
        if not api_key:
            logger.error("TWELVEDATA_API_KEY not set. Get a free key at https://twelvedata.com/")
            sys.exit(1)
        logger.warning("Twelve Data provider: 5m intraday candles will be SKIPPED (rate limit too slow).")
        logger.warning("Use --provider yahoo or --provider polygon for full 5m + hourly + daily support.")
    elif provider == "polygon":
        api_key = os.getenv("POLYGON_API_KEY", "")
        if not api_key:
            logger.error("POLYGON_API_KEY not set. Add it to backend/.env after subscribing at massive.com.")
            sys.exit(1)
        logger.warning("Polygon Stocks Starter plan data is 15-minute delayed, not real-time.")

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
