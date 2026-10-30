"""Bounded read-only inventory/export for the offline roadmap detector study."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import gzip
from pathlib import Path
import sys

from dotenv import dotenv_values
import psycopg2
from psycopg2.extras import RealDictCursor


ENV = Path(r"C:\Users\vachi\source\repo\AlphaResearchPro\backend\.env")


def connect(env_file=ENV):
    settings = dotenv_values(env_file)
    if not settings.get("DB_USER") or not settings.get("DB_PASSWORD"):
        raise ValueError("DB_USER/DB_PASSWORD missing from configured credential file")
    conn = psycopg2.connect(dbname="stocks_db_fresh", user=settings["DB_USER"],
        password=settings["DB_PASSWORD"], host=settings.get("DB_HOST", "localhost"),
        port=settings.get("DB_PORT", "5432"), connect_timeout=10,
        options="-c default_transaction_read_only=on -c statement_timeout=60000 -c timezone=UTC")
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    return conn


def write_once(path, payload):
    text = json.dumps(payload, indent=2, default=str, sort_keys=True, allow_nan=False) + "\n"
    path = Path(path)
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise ValueError("refusing to overwrite different research artifact")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(text)


def inventory(env_file):
    with connect(env_file) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT current_database() AS database, current_setting('transaction_read_only') AS read_only, "
                           "current_setting('transaction_isolation') AS isolation, transaction_timestamp() AS snapshot_at")
            guard = dict(cursor.fetchone())
            if guard["database"] != "stocks_db_fresh" or guard["read_only"] != "on":
                raise RuntimeError("database/read-only verification failed")
            cursor.execute("""SELECT c.relname AS table_name,c.reltuples::bigint AS estimated_rows
                FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
                WHERE n.nspname='public' AND c.relkind='r'
                AND (c.relname ~ 'equity|stock|quote|trade|fundamental|event|earning|macro|consensus')
                ORDER BY c.relname""")
            tables = [dict(row) for row in cursor.fetchall()]
            names = [r["table_name"] for r in tables]
            cursor.execute("""SELECT table_name,column_name,data_type FROM information_schema.columns
                WHERE table_schema='public' AND table_name=ANY(%s) ORDER BY table_name,ordinal_position""", (names,))
            columns = [dict(row) for row in cursor.fetchall()]
    return dict(schema="legacy_detector_input_inventory_v1", transaction=guard, tables=tables, columns=columns)


def profile(env_file):
    queries = {
        "bars": """SELECT interval,adjusted,availability_mode,session_scope,count(*) AS rows,
            min(session_date) AS first_session,max(session_date) AS last_session,
            count(*) FILTER (WHERE vwap IS NOT NULL) AS with_vwap
            FROM equity_bar_revisions WHERE is_final GROUP BY 1,2,3,4 ORDER BY 1,2,3,4""",
        "fundamentals": "SELECT count(*) AS reports,min(filing_date) AS first_filing,max(filing_date) AS last_filing FROM equity_fundamental_reports",
        "event_coverage": """SELECT event_type,source,count(*) AS rows,min(window_start) AS first_window,
            max(window_end) AS last_window,min(first_observed_at) AS first_observed,
            max(first_observed_at) AS last_observed FROM option_event_calendar_coverage GROUP BY 1,2""",
        "events": """SELECT event_type,source,status,count(*) AS rows,min(scheduled_time) AS first_event,
            max(scheduled_time) AS last_event FROM option_market_events GROUP BY 1,2,3""",
        "sector_mapping": """SELECT count(*) AS rows,count(*) FILTER (WHERE sector IS NOT NULL AND sector<>'') AS with_sector,
            min(effective_from) AS first_effective,max(effective_from) AS last_effective FROM equity_security_reference_revisions""",
        "universes": """SELECT availability_mode,status,count(*) AS runs,min(effective_from) AS first_effective,
            max(effective_from) AS last_effective FROM equity_universe_runs GROUP BY 1,2""",
        "macro_coverage_records": "SELECT * FROM option_event_calendar_coverage WHERE event_type='FED_RATE_DECISION' ORDER BY first_observed_at",
        "macro_event_records": "SELECT * FROM option_market_events WHERE event_type='FED_RATE_DECISION' ORDER BY scheduled_time",
        "recent_universe": "SELECT universe_run_id,effective_from,observed_at,status,admitted_members,availability_mode FROM equity_universe_runs ORDER BY effective_from DESC LIMIT 5",
        "relations": """SELECT c.relname,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='public' AND c.relname ~ 'stock.*(quote|trade)|equity.*(quote|trade)|consensus|macro|earning'""",
        "bar_examples": """(SELECT ticker,security_id,interval,bar_start,bar_end,session_date,system_observed_at,created_at,
            availability_mode,source_kind,adjusted,quality_codes FROM equity_bar_revisions
            WHERE ticker='AAPL' AND interval='1d' AND NOT adjusted AND is_final ORDER BY bar_start DESC LIMIT 3)
            UNION ALL (SELECT ticker,security_id,interval,bar_start,bar_end,session_date,system_observed_at,created_at,
            availability_mode,source_kind,adjusted,quality_codes FROM equity_bar_revisions
            WHERE ticker='AAPL' AND interval='30m' AND NOT adjusted AND is_final ORDER BY bar_start DESC LIMIT 3)""",
        "action_coverage": """SELECT action_type,availability_mode,count(*) AS rows,min(window_start) AS first_window,
            max(window_end) AS last_window,min(first_observed_at) AS first_observed,
            count(*) FILTER(WHERE response_sha256 IS NOT NULL) AS certified_responses
            FROM equity_corporate_action_coverage GROUP BY 1,2""",
        "benchmark_bars": """SELECT ticker,interval,count(*) AS rows,min(session_date) AS first_session,
            max(session_date) AS last_session FROM equity_bar_revisions
            WHERE ticker=ANY(ARRAY['SPY','XLK','XLF','XLY','XLP','XLE','XLV','XLI','XLB','XLU','XLRE','XLC'])
            AND interval='1d' AND NOT adjusted AND is_final GROUP BY 1,2""",
    }
    result = {"queries": {}}
    conn = connect(env_file)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT current_database() AS database,current_setting('transaction_read_only') AS read_only,transaction_timestamp() AS snapshot_at")
            result["transaction"] = dict(cursor.fetchone())
            if result["transaction"]["database"] != "stocks_db_fresh" or result["transaction"]["read_only"] != "on":
                raise RuntimeError("read-only database guard failed")
            for name, sql in queries.items():
                cursor.execute("SAVEPOINT inventory_query")
                try:
                    cursor.execute(sql)
                    result["queries"][name] = [dict(r) for r in cursor.fetchall()]
                except psycopg2.errors.QueryCanceled:
                    cursor.execute("ROLLBACK TO SAVEPOINT inventory_query")
                    result["queries"][name] = {"status": "UNAVAILABLE", "reason": "BOUNDED_QUERY_TIMEOUT"}
                cursor.execute("RELEASE SAVEPOINT inventory_query")
                print(f"Profiled {name}", flush=True)
    finally:
        conn.rollback()
        conn.close()
    return result


def export_readiness(env_file, directory):
    """Freeze the actual source evidence; do not reconstruct missing history."""
    directory = Path(directory)
    conn = connect(env_file)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT current_database() AS database,current_setting('transaction_read_only') AS read_only,"
                           "current_setting('transaction_isolation') AS isolation,transaction_timestamp() AS snapshot_at")
            guard = dict(cursor.fetchone())
            if guard["database"] != "stocks_db_fresh" or guard["read_only"] != "on":
                raise RuntimeError("read-only database guard failed")
            cursor.execute("""SELECT * FROM equity_universe_runs WHERE availability_mode='LIVE_OBSERVED'
                AND status='COMPLETE' ORDER BY effective_from,observed_at""")
            runs = [dict(r) for r in cursor.fetchall()]
            cursor.execute("SELECT * FROM equity_universe_members WHERE universe_run_id=ANY(%s::uuid[])",
                           ([str(r["universe_run_id"]) for r in runs],))
            members = [dict(r) for r in cursor.fetchall()]
            securities = sorted({str(r["security_id"]) for r in members})
            cursor.execute("""SELECT * FROM equity_corporate_action_coverage
                WHERE security_id=ANY(%s::uuid[]) OR ticker=ANY(%s) ORDER BY first_observed_at""",
                           (securities, sorted({r["ticker"] for r in members})))
            coverage = [dict(r) for r in cursor.fetchall()]
            cursor.execute("""SELECT * FROM equity_corporate_action_coverage_members
                WHERE coverage_id=ANY(%s::uuid[])""", ([str(r["coverage_id"]) for r in coverage],))
            coverage_members = [dict(r) for r in cursor.fetchall()]
            cursor.execute("""SELECT * FROM equity_corporate_actions WHERE corporate_action_id=ANY(%s::uuid[])""",
                           ([str(r["corporate_action_id"]) for r in coverage_members],))
            actions = [dict(r) for r in cursor.fetchall()]
            cursor.execute("SELECT * FROM option_event_calendar_coverage ORDER BY first_observed_at")
            event_coverage = [dict(r) for r in cursor.fetchall()]
            cursor.execute("SELECT * FROM option_market_events ORDER BY first_observed_at")
            events = [dict(r) for r in cursor.fetchall()]
            cursor.execute("SELECT count(*) AS reports FROM equity_fundamental_reports")
            fundamentals = dict(cursor.fetchone())
    finally:
        conn.rollback()
        conn.close()
    payload = dict(schema="legacy_detector_readiness_evidence_v1", transaction=guard,
                   universe_runs=runs, universe_members=members, action_coverage=coverage,
                   action_coverage_members=coverage_members, actions=actions,
                   event_coverage=event_coverage, events=events, fundamentals=fundamentals)
    raw = (json.dumps(payload, default=str, sort_keys=True, allow_nan=False) + "\n").encode()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "source-readiness-evidence.json.gz"
    # gzip mtime=0 makes the immutable byte identity reproducible.
    compressed = gzip.compress(raw, mtime=0)
    if path.exists() and path.read_bytes() != compressed:
        raise ValueError("refusing to overwrite source readiness evidence")
    if not path.exists():
        with path.open("xb") as stream:
            stream.write(compressed)
    manifest = dict(transaction=guard, evidence_file=path.name, raw_sha256=hashlib.sha256(raw).hexdigest(),
                    compressed_sha256=hashlib.sha256(compressed).hexdigest(),
                    counts={k: len(v) for k, v in payload.items() if isinstance(v, list)})
    write_once(directory / "source-evidence-manifest.json", manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=ENV)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--export-readiness", action="store_true")
    args = parser.parse_args()
    if args.export_readiness:
        result = export_readiness(args.env_file, args.output)
        print(json.dumps(result, default=str, indent=2))
        return
    result = profile(args.env_file) if args.profile else inventory(args.env_file)
    write_once(args.output, result)
    print(json.dumps(dict(transaction=result["transaction"], tables=result.get("tables"), output=str(args.output)), default=str))


if __name__ == "__main__":
    main()
