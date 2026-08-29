#!/usr/bin/env python3
"""Report PostgreSQL statements responsible for temporary-file I/O."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from database import get_db_cursor  # noqa: E402


def _compact_query(value: str, limit: int = 320) -> str:
    compact = " ".join((value or "").split())
    return compact if len(compact) <= limit else compact[: limit - 3] + "..."


def _format_setting(setting: str, unit: str) -> str:
    byte_multipliers = {
        "8kB": 8 * 1024,
        "kB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
    }
    if unit not in byte_multipliers:
        return f"{setting}{unit}"

    size_bytes = int(setting) * byte_multipliers[unit]
    for suffix, divisor in (("GB", 1024**3), ("MB", 1024**2), ("kB", 1024)):
        if size_bytes >= divisor and size_bytes % divisor == 0:
            return f"{size_bytes // divisor}{suffix}"
    return f"{size_bytes}B"


def _print_settings() -> None:
    names = [
        "shared_buffers",
        "effective_cache_size",
        "work_mem",
        "maintenance_work_mem",
        "max_wal_size",
        "checkpoint_timeout",
        "track_io_timing",
        "log_temp_files",
        "log_min_duration_statement",
        "shared_preload_libraries",
    ]
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT name, setting, COALESCE(unit, '') AS unit
            FROM pg_settings
            WHERE name = ANY(%s)
            ORDER BY name
            """,
            (names,),
        )
        rows = cursor.fetchall()

    print("\nEffective settings")
    for row in rows:
        value = _format_setting(row["setting"], row["unit"])
        print(f"  {row['name']:<30} {value}")


def _print_database_totals() -> None:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT pg_postmaster_start_time() AS server_started_at,
                   stats_reset,
                   temp_files,
                   temp_bytes,
                   pg_size_pretty(temp_bytes) AS temp_pretty,
                   deadlocks
            FROM pg_stat_database
            WHERE datname = current_database()
            """
        )
        row = cursor.fetchone()

    print("PostgreSQL temporary-file report")
    print(f"  server_started_at: {row['server_started_at']}")
    print(f"  database_stats_reset: {row['stats_reset'] or 'never recorded'}")
    print(f"  cumulative_temp_files: {row['temp_files']:,}")
    print(f"  cumulative_temp_bytes: {row['temp_bytes']:,} ({row['temp_pretty']})")
    print(f"  deadlocks: {row['deadlocks']:,}")


def _print_statement_spill(limit: int, min_temp_mb: float) -> None:
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') AS installed"
        )
        if not cursor.fetchone()["installed"]:
            raise SystemExit(
                "pg_stat_statements is not installed in this database; enable its preload library, "
                "restart PostgreSQL, and create the extension first."
            )

        cursor.execute(
            "SELECT stats_reset, dealloc FROM pg_stat_statements_info"
        )
        info = cursor.fetchone()
        cursor.execute(
            """
            SELECT queryid,
                   calls,
                   rows,
                   total_exec_time,
                   mean_exec_time,
                   temp_blks_read,
                   temp_blks_written,
                   temp_blk_read_time,
                   temp_blk_write_time,
                   temp_blks_written * current_setting('block_size')::bigint AS temp_bytes,
                   shared_blks_read,
                   shared_blks_hit,
                   query
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND temp_blks_written * current_setting('block_size')::bigint >= %s
            ORDER BY temp_blks_written DESC, total_exec_time DESC
            LIMIT %s
            """,
            (int(min_temp_mb * 1024 * 1024), limit),
        )
        rows = cursor.fetchall()

    print("\nStatement statistics")
    print(f"  stats_reset: {info['stats_reset']}")
    print(f"  deallocated_entries: {info['dealloc']:,}")
    print(f"  minimum_temp_mb: {min_temp_mb:g}")
    if not rows:
        print("  No statements meet the threshold yet.")
        return

    for index, row in enumerate(rows, 1):
        temp_mb = row["temp_bytes"] / (1024 * 1024)
        hit_total = row["shared_blks_hit"] + row["shared_blks_read"]
        hit_ratio = row["shared_blks_hit"] / hit_total if hit_total else 1.0
        print(f"\n  {index}. queryid={row['queryid']}")
        print(
            "     calls={:,} rows={:,} total_ms={:.1f} mean_ms={:.1f}".format(
                row["calls"], row["rows"], row["total_exec_time"], row["mean_exec_time"]
            )
        )
        print(
            "     temp_mb={:.1f} temp_blocks_read={:,} temp_blocks_written={:,} "
            "temp_read_ms={:.1f} temp_write_ms={:.1f}".format(
                temp_mb,
                row["temp_blks_read"],
                row["temp_blks_written"],
                row["temp_blk_read_time"],
                row["temp_blk_write_time"],
            )
        )
        print(
            "     shared_blocks_read={:,} shared_blocks_hit={:,} cache_hit={:.2%}".format(
                row["shared_blks_read"], row["shared_blks_hit"], hit_ratio
            )
        )
        print(f"     sql={_compact_query(row['query'])}")


def _print_scan_pressure(limit: int) -> None:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT relname,
                   seq_scan,
                   seq_tup_read,
                   idx_scan,
                   n_live_tup,
                   n_dead_tup,
                   pg_total_relation_size(relid) AS total_bytes,
                   pg_size_pretty(pg_total_relation_size(relid)) AS total_pretty
            FROM pg_stat_user_tables
            ORDER BY seq_tup_read DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cursor.fetchall()

    print("\nTables by cumulative sequential rows read")
    for row in rows:
        print(
            "  {:<32} seq_scans={:>7,} seq_rows={:>14,} idx_scans={:>12,} "
            "live={:>10,} dead={:>9,} size={}".format(
                row["relname"],
                row["seq_scan"],
                row["seq_tup_read"],
                row["idx_scan"] or 0,
                row["n_live_tup"],
                row["n_dead_tup"],
                row["total_pretty"],
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Show query-level PostgreSQL temporary-file usage and table scan pressure."
    )
    parser.add_argument("--limit", type=int, default=15, help="Rows per report section")
    parser.add_argument(
        "--min-temp-mb",
        type=float,
        default=1.0,
        help="Only show statements with at least this much cumulative temp output",
    )
    args = parser.parse_args()
    if args.limit < 1 or args.min_temp_mb < 0:
        parser.error("--limit must be positive and --min-temp-mb cannot be negative")

    _print_database_totals()
    _print_settings()
    _print_statement_spill(args.limit, args.min_temp_mb)
    _print_scan_pressure(args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())