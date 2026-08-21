"""End-of-day data integrity validator — checks last N days across all three tables.

Run automatically by run_scheduler.py after daily close, or standalone:

    python scripts/validate_eod.py              # last 7 days (default)
    python scripts/validate_eod.py --days 14    # last 14 days
    python scripts/validate_eod.py --fix        # delete corrupt rows (requires confirmation)

Validates:
  stock_prices_daily     — duplicates, NULL/zero OHLCV, weekends, non-midnight time
  stock_prices_hourly    — duplicates, NULL/zero OHLCV, weekends, invalid hours, ghost bars
  stock_prices_intraday  — duplicates, NULL/zero OHLCV, weekends, invalid times, bad intervals
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field
import logging

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("validate-eod")


@dataclass
class ValidationResult:
    """Accumulates issues found during validation."""
    table: str
    total_rows: int = 0
    duplicates: int = 0
    null_zero_close: int = 0
    null_zero_ohlv: int = 0
    negative_volume: int = 0
    weekend_rows: int = 0
    invalid_time: int = 0
    bad_intervals: int = 0
    missing_sessions: int = 0
    sparse_sessions: int = 0
    details: list = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return (self.duplicates == 0 and self.null_zero_close == 0 and
                self.null_zero_ohlv == 0 and self.negative_volume == 0 and
                self.weekend_rows == 0 and self.invalid_time == 0 and
                self.bad_intervals == 0 and self.missing_sessions == 0 and
                self.sparse_sessions == 0)

    @property
    def total_issues(self) -> int:
        return (self.duplicates + self.null_zero_close + self.null_zero_ohlv +
                self.negative_volume + self.weekend_rows + self.invalid_time +
                self.bad_intervals + self.missing_sessions + self.sparse_sessions)


def _session_date_expr(table: str) -> str:
    """Daily rows are stored at midnight with no zone; the others are tz-aware."""
    if table == "stock_prices_daily":
        return "datetime::date"
    return "(datetime AT TIME ZONE 'America/New_York')::date"


def _missing_sessions(table: str, days: int, result: ValidationResult,
                      calendar_table: str = "stock_prices_daily"):
    """Flag sessions present in `calendar_table` but absent from `table`.

    Every other check counts defective rows, so a table with no rows at all scores
    perfectly. The calendar comes from a sibling table rather than a weekday rule so
    market holidays do not register as gaps and no holiday list needs maintaining.
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    cal_expr = _session_date_expr(calendar_table)
    tbl_expr = _session_date_expr(table)
    sql = f"""
        WITH calendar AS (
            SELECT DISTINCT {cal_expr} AS d
            FROM {calendar_table} WHERE datetime >= %s
        ), present AS (
            SELECT DISTINCT {tbl_expr} AS d
            FROM {table} WHERE datetime >= %s
        )
        SELECT c.d FROM calendar c
        LEFT JOIN present p ON p.d = c.d
        WHERE p.d IS NULL
          AND c.d >= (SELECT MIN({tbl_expr}) FROM {table})
        ORDER BY c.d
    """
    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute(sql, (cutoff, cutoff))
        missing = [row[0] for row in cur.fetchall()]

    result.missing_sessions = len(missing)
    if missing:
        result.details.append(
            "missing sessions: " + ", ".join(str(d) for d in missing))


def _sparse_sessions(table: str, days: int, result: ValidationResult,
                     threshold: float = 0.9):
    """Flag sessions covering far fewer tickers than the busiest day in the window.

    Catches a partial write, where rows exist and are individually valid but most of
    the universe is missing.
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    date_expr = _session_date_expr(table)
    sql = f"""
        SELECT {date_expr} AS d, COUNT(DISTINCT ticker) AS tickers
        FROM {table} WHERE datetime >= %s
        GROUP BY 1 ORDER BY 1
    """
    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute(sql, (cutoff,))
        rows = cur.fetchall()

    if len(rows) < 2:
        return

    peak = max(r[1] for r in rows)
    sparse = [(d, tk) for d, tk in rows if tk < peak * threshold]
    result.sparse_sessions = len(sparse)
    if sparse:
        result.details.append(
            "sparse sessions (peak {}): ".format(peak)
            + ", ".join("{}={}".format(d, tk) for d, tk in sparse))


def _validate_daily(days: int) -> ValidationResult:
    """Validate stock_prices_daily for the last N days."""
    result = ValidationResult(table="stock_prices_daily")
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    with get_db_cursor(dict_cursor=False) as cur:
        # Total rows
        cur.execute("SELECT COUNT(*) FROM stock_prices_daily WHERE datetime >= %s", (cutoff,))
        result.total_rows = cur.fetchone()[0]

        # Duplicate (ticker, datetime)
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT ticker, datetime FROM stock_prices_daily
                WHERE datetime >= %s
                GROUP BY ticker, datetime HAVING COUNT(*) > 1
            ) d
        """, (cutoff,))
        result.duplicates = cur.fetchone()[0]

        # NULL or zero close
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_daily
            WHERE datetime >= %s AND (close_price IS NULL OR close_price = 0)
        """, (cutoff,))
        result.null_zero_close = cur.fetchone()[0]

        # NULL or zero open/high/low
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_daily
            WHERE datetime >= %s AND (
                open_price IS NULL OR open_price = 0 OR
                high IS NULL OR high = 0 OR
                low IS NULL OR low = 0
            )
        """, (cutoff,))
        result.null_zero_ohlv = cur.fetchone()[0]

        # Negative volume
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_daily
            WHERE datetime >= %s AND volume < 0
        """, (cutoff,))
        result.negative_volume = cur.fetchone()[0]

        # Weekend rows
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_daily
            WHERE datetime >= %s AND EXTRACT(ISODOW FROM datetime) IN (6, 7)
        """, (cutoff,))
        result.weekend_rows = cur.fetchone()[0]

        # Non-midnight time component
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_daily
            WHERE datetime >= %s AND datetime::time != '00:00:00'
        """, (cutoff,))
        result.invalid_time = cur.fetchone()[0]

    # Hourly is the calendar here: it only has bars on real trading days, so daily
    # cannot silently vouch for its own completeness.
    _missing_sessions("stock_prices_daily", days, result,
                      calendar_table="stock_prices_hourly")
    _sparse_sessions("stock_prices_daily", days, result)

    return result


def _validate_hourly(days: int) -> ValidationResult:
    """Validate stock_prices_hourly for the last N days."""
    result = ValidationResult(table="stock_prices_hourly")
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    with get_db_cursor(dict_cursor=False) as cur:
        # Total rows
        cur.execute("SELECT COUNT(*) FROM stock_prices_hourly WHERE datetime >= %s", (cutoff,))
        result.total_rows = cur.fetchone()[0]

        # Duplicate (ticker, datetime)
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT ticker, datetime FROM stock_prices_hourly
                WHERE datetime >= %s
                GROUP BY ticker, datetime HAVING COUNT(*) > 1
            ) d
        """, (cutoff,))
        result.duplicates = cur.fetchone()[0]

        # NULL or zero close
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_hourly
            WHERE datetime >= %s AND (close_price IS NULL OR close_price = 0)
        """, (cutoff,))
        result.null_zero_close = cur.fetchone()[0]

        # NULL or zero OHL
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_hourly
            WHERE datetime >= %s AND (
                open_price IS NULL OR open_price = 0 OR
                high IS NULL OR high = 0 OR
                low IS NULL OR low = 0
            )
        """, (cutoff,))
        result.null_zero_ohlv = cur.fetchone()[0]

        # Negative volume
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_hourly
            WHERE datetime >= %s AND volume < 0
        """, (cutoff,))
        result.negative_volume = cur.fetchone()[0]

        # Weekend rows
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_hourly
            WHERE datetime >= %s AND EXTRACT(ISODOW FROM datetime) IN (6, 7)
        """, (cutoff,))
        result.weekend_rows = cur.fetchone()[0]

        # Invalid hours — valid: minute=30, hour in 9..15 ET (7 bars/day)
        # Column is timestamptz; EXTRACT uses server TZ, so convert to ET first
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_hourly
            WHERE datetime >= %s AND (
                EXTRACT(MINUTE FROM datetime AT TIME ZONE 'America/New_York') != 30
                OR EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') NOT BETWEEN 9 AND 15
            )
        """, (cutoff,))
        result.invalid_time = cur.fetchone()[0]

    _missing_sessions("stock_prices_hourly", days, result)
    _sparse_sessions("stock_prices_hourly", days, result)

    return result


def _validate_intraday(days: int) -> ValidationResult:
    """Validate stock_prices_intraday for the last N days."""
    result = ValidationResult(table="stock_prices_intraday")
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    with get_db_cursor(dict_cursor=False) as cur:
        # Total rows
        cur.execute("SELECT COUNT(*) FROM stock_prices_intraday WHERE datetime >= %s", (cutoff,))
        result.total_rows = cur.fetchone()[0]

        # Duplicate (ticker, datetime, interval)
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT ticker, datetime, interval FROM stock_prices_intraday
                WHERE datetime >= %s
                GROUP BY ticker, datetime, interval HAVING COUNT(*) > 1
            ) d
        """, (cutoff,))
        result.duplicates = cur.fetchone()[0]

        # NULL or zero close
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_intraday
            WHERE datetime >= %s AND (close_price IS NULL OR close_price = 0)
        """, (cutoff,))
        result.null_zero_close = cur.fetchone()[0]

        # NULL or zero OHL
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_intraday
            WHERE datetime >= %s AND (
                open_price IS NULL OR open_price = 0 OR
                high IS NULL OR high = 0 OR
                low IS NULL OR low = 0
            )
        """, (cutoff,))
        result.null_zero_ohlv = cur.fetchone()[0]

        # Negative volume
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_intraday
            WHERE datetime >= %s AND volume < 0
        """, (cutoff,))
        result.negative_volume = cur.fetchone()[0]

        # Weekend rows
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_intraday
            WHERE datetime >= %s AND EXTRACT(ISODOW FROM datetime) IN (6, 7)
        """, (cutoff,))
        result.weekend_rows = cur.fetchone()[0]

        # Invalid times — valid 5m bars: 9:30-15:55 ET, minute%5==0
        # Column is timestamptz; EXTRACT uses server TZ, so convert to ET first
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_intraday
            WHERE datetime >= %s AND interval = '5m' AND (
                EXTRACT(MINUTE FROM datetime AT TIME ZONE 'America/New_York')::int %% 5 != 0
                OR (EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') = 9
                    AND EXTRACT(MINUTE FROM datetime AT TIME ZONE 'America/New_York') < 30)
                OR EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') < 9
                OR (EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') = 15
                    AND EXTRACT(MINUTE FROM datetime AT TIME ZONE 'America/New_York') > 55)
                OR EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') > 15
            )
        """, (cutoff,))
        result.invalid_time = cur.fetchone()[0]

        # Bad intervals — only 1m and 5m allowed
        cur.execute("""
            SELECT COUNT(*) FROM stock_prices_intraday
            WHERE datetime >= %s AND interval NOT IN ('1m', '5m')
        """, (cutoff,))
        result.bad_intervals = cur.fetchone()[0]

    _missing_sessions("stock_prices_intraday", days, result)
    _sparse_sessions("stock_prices_intraday", days, result)

    return result


def validate_all(days: int = 7) -> list[ValidationResult]:
    """Run all three table validators concurrently. Returns results in table order."""
    validators = [
        ("daily", _validate_daily),
        ("hourly", _validate_hourly),
        ("intraday", _validate_intraday),
    ]
    for name, _ in validators:
        logger.info("Validating %s (last %d days)...", name, days)

    with ThreadPoolExecutor(max_workers=len(validators), thread_name_prefix="validate-eod") as executor:
        futures = [executor.submit(fn, days) for _, fn in validators]
        results = [future.result() for future in futures]

    for r in results:
        if r.clean:
            logger.info("  %s — %d rows — CLEAN", r.table, r.total_rows)
        else:
            logger.warning("  %s — %d rows — %d ISSUES FOUND", r.table, r.total_rows, r.total_issues)
            if r.duplicates:
                logger.warning("    duplicates       : %d", r.duplicates)
            if r.null_zero_close:
                logger.warning("    null/zero close  : %d", r.null_zero_close)
            if r.null_zero_ohlv:
                logger.warning("    null/zero OHL    : %d", r.null_zero_ohlv)
            if r.negative_volume:
                logger.warning("    negative volume  : %d", r.negative_volume)
            if r.weekend_rows:
                logger.warning("    weekend rows     : %d", r.weekend_rows)
            if r.invalid_time:
                logger.warning("    invalid time     : %d", r.invalid_time)
            if r.bad_intervals:
                logger.warning("    bad intervals    : %d", r.bad_intervals)
            if r.missing_sessions:
                logger.warning("    missing sessions : %d", r.missing_sessions)
            if r.sparse_sessions:
                logger.warning("    sparse sessions  : %d", r.sparse_sessions)
            for detail in r.details:
                logger.warning("    %s", detail)

    all_clean = all(r.clean for r in results)
    total_issues = sum(r.total_issues for r in results)
    if all_clean:
        logger.info("ALL TABLES CLEAN — 0 issues across %d rows",
                     sum(r.total_rows for r in results))
    else:
        logger.warning("VALIDATION FAILED — %d total issues across %d rows",
                        total_issues, sum(r.total_rows for r in results))
    return results


def fix_issues(days: int = 7):
    """Delete rows that violate data integrity rules."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    with get_db_cursor(dict_cursor=False) as cur:
        # Delete weekend rows from all tables
        for table in ["stock_prices_daily", "stock_prices_hourly", "stock_prices_intraday"]:
            cur.execute(f"""
                DELETE FROM {table}
                WHERE datetime >= %s AND EXTRACT(ISODOW FROM datetime) IN (6, 7)
            """, (cutoff,))
            if cur.rowcount:
                logger.info("  Deleted %d weekend rows from %s", cur.rowcount, table)

        # Delete invalid hourly times (convert to ET since column is timestamptz)
        cur.execute("""
            DELETE FROM stock_prices_hourly
            WHERE datetime >= %s AND (
                EXTRACT(MINUTE FROM datetime AT TIME ZONE 'America/New_York') != 30
                OR EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') NOT BETWEEN 9 AND 15
            )
        """, (cutoff,))
        if cur.rowcount:
            logger.info("  Deleted %d invalid-time hourly rows", cur.rowcount)

        # Delete invalid intraday times (5m bars outside 9:30-15:55 ET)
        cur.execute("""
            DELETE FROM stock_prices_intraday
            WHERE datetime >= %s AND interval = '5m' AND (
                EXTRACT(MINUTE FROM datetime AT TIME ZONE 'America/New_York')::int %% 5 != 0
                OR (EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') = 9
                    AND EXTRACT(MINUTE FROM datetime AT TIME ZONE 'America/New_York') < 30)
                OR EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') < 9
                OR (EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') = 15
                    AND EXTRACT(MINUTE FROM datetime AT TIME ZONE 'America/New_York') > 55)
                OR EXTRACT(HOUR FROM datetime AT TIME ZONE 'America/New_York') > 15
            )
        """, (cutoff,))
        if cur.rowcount:
            logger.info("  Deleted %d invalid-time intraday rows", cur.rowcount)

        # Delete bad intervals
        cur.execute("""
            DELETE FROM stock_prices_intraday
            WHERE datetime >= %s AND interval NOT IN ('1m', '5m')
        """, (cutoff,))
        if cur.rowcount:
            logger.info("  Deleted %d bad-interval intraday rows", cur.rowcount)

        # Delete non-midnight daily rows
        cur.execute("""
            DELETE FROM stock_prices_daily
            WHERE datetime >= %s AND datetime::time != '00:00:00'
        """, (cutoff,))
        if cur.rowcount:
            logger.info("  Deleted %d non-midnight daily rows", cur.rowcount)

    logger.info("Fix complete.")


def main():
    parser = argparse.ArgumentParser(
        description="EOD data integrity validator — checks all three price tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--days", type=int, default=7,
                        help="Number of days to look back (default: 7)")
    parser.add_argument("--fix", action="store_true",
                        help="Delete corrupt rows (prompts for confirmation)")
    args = parser.parse_args()

    results = validate_all(args.days)

    if args.fix:
        total_issues = sum(r.total_issues for r in results)
        if total_issues == 0:
            logger.info("Nothing to fix.")
            return
        confirm = input(f"\nDelete {total_issues} corrupt rows? [y/N] ").strip().lower()
        if confirm == "y":
            fix_issues(args.days)
            logger.info("\nRe-validating...")
            validate_all(args.days)
        else:
            logger.info("Aborted.")


if __name__ == "__main__":
    main()
