#!/usr/bin/env python3
"""Benchmark behavior-equivalent Options strategy trade reads without writes."""
from __future__ import annotations

import argparse
from datetime import date, timedelta
import json
import os
from pathlib import Path
import sys
from time import perf_counter

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402


SELECT_COLUMNS = """
    trade_event_id, provider, contract_id, contract_ticker, underlying,
    sip_timestamp, sequence_number, participant_timestamp,
    first_observed_at, revised_observed_at, exchange, conditions,
    correction, provider_trade_id, price, size, shares_per_contract,
    notional, payload_sha256, raw_batch_id, classification_status,
    classification_reasons, semantics_version
"""
ORDER_BY = "sip_timestamp, sequence_number, participant_timestamp NULLS FIRST, trade_event_id"


def benchmark(cursor, *, underlyer, market_time, observed_time, contract_ids, minimum_notional):
    start_time = market_time - timedelta(hours=8)
    started = perf_counter()
    cursor.execute(f"""SELECT {SELECT_COLUMNS} FROM option_trade_events
        WHERE underlying=%s AND sip_timestamp BETWEEN %s AND %s
          AND COALESCE(revised_observed_at,first_observed_at)<=%s
          AND classification_status='INCLUDED'
        ORDER BY {ORDER_BY}""", (underlyer, start_time, market_time, observed_time))
    legacy = cursor.fetchall()
    legacy_seconds = perf_counter() - started

    started = perf_counter()
    cursor.execute(f"""SELECT {SELECT_COLUMNS} FROM option_trade_events
        WHERE contract_id=ANY(%s::bigint[]) AND sip_timestamp BETWEEN %s AND %s
          AND COALESCE(revised_observed_at,first_observed_at)<=%s
          AND classification_status='INCLUDED'
        ORDER BY {ORDER_BY}""", (contract_ids, start_time, market_time, observed_time))
    bounded = cursor.fetchall()
    bounded_seconds = perf_counter() - started

    started = perf_counter()
    cursor.execute(f"""SELECT {SELECT_COLUMNS} FROM option_trade_events
        WHERE contract_id=ANY(%s::bigint[]) AND sip_timestamp BETWEEN %s AND %s
          AND COALESCE(revised_observed_at,first_observed_at)<=%s
          AND classification_status='INCLUDED' AND notional>=%s
        ORDER BY {ORDER_BY}""", (contract_ids, start_time, market_time, observed_time, minimum_notional))
    filtered = cursor.fetchall()
    filtered_seconds = perf_counter() - started

    contract_id_set = set(contract_ids)
    expected = {row["trade_event_id"] for row in legacy if row["contract_id"] in contract_id_set}
    engine_expected = {row["trade_event_id"] for row in legacy
        if row["contract_id"] in contract_id_set and row["notional"] >= minimum_notional}
    actual = {row["trade_event_id"] for row in bounded}
    filtered_actual = {row["trade_event_id"] for row in filtered}
    return {
        "underlyer": underlyer,
        "market_time": market_time,
        "observed_time": observed_time,
        "contract_ids": len(contract_ids),
        "legacy_rows": len(legacy),
        "engine_relevant_legacy_rows": len(expected),
        "bounded_rows": len(bounded),
        "legacy_seconds": round(legacy_seconds, 4),
        "bounded_seconds": round(bounded_seconds, 4),
        "speedup": round(legacy_seconds / bounded_seconds, 3) if bounded_seconds else None,
        "matching_event_ids": expected == actual,
        "minimum_print_notional": str(minimum_notional),
        "engine_input_rows": len(engine_expected),
        "filtered_bounded_rows": len(filtered),
        "filtered_bounded_seconds": round(filtered_seconds, 4),
        "filtered_speedup": round(legacy_seconds / filtered_seconds, 3) if filtered_seconds else None,
        "matching_engine_input_event_ids": engine_expected == filtered_actual,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--underlyers", default="SPY,QQQ")
    args = parser.parse_args()
    underlyers = tuple(dict.fromkeys(value.strip().upper() for value in args.underlyers.split(",") if value.strip()))
    if not 1 <= len(underlyers) <= 20:
        raise ValueError("underlyers must contain 1-20 distinct symbols")
    configuration = load_option_runtime_configuration(dict(os.environ), BACKEND_DIR)
    minimum_notional = configuration.strategy_policy.flow.minimum_print_notional
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='60s'")
        cursor.execute("""
            SELECT DISTINCT ON (analysis.underlying)
                   analysis.underlying,analysis.batch_id,analysis.market_time,analysis.observed_time
            FROM option_analysis_runs AS analysis
            JOIN option_ingestion_runs AS ingestion USING(batch_id)
            WHERE analysis.underlying=ANY(%s::text[]) AND analysis.status='COMPLETE'
              AND ingestion.scheduled_cycle>=%s::date
              AND ingestion.scheduled_cycle<%s::date+INTERVAL '1 day'
            ORDER BY analysis.underlying,ingestion.scheduled_cycle DESC,analysis.observed_time DESC
        """, (list(underlyers), args.session_date, args.session_date))
        sources = cursor.fetchall()
        results = []
        for source in sources:
            cursor.execute("""SELECT DISTINCT contract_id FROM option_chain_snapshots
                WHERE batch_id=%s AND contract_type='CALL' AND strike>spot
                ORDER BY contract_id""", (source["batch_id"],))
            contract_ids = [row["contract_id"] for row in cursor.fetchall()]
            results.append(benchmark(cursor, underlyer=source["underlying"],
                market_time=source["market_time"], observed_time=source["observed_time"],
                contract_ids=contract_ids, minimum_notional=minimum_notional))
    print(json.dumps({"version": "option_trade_read_latency_v1", "mode": "READ_ONLY",
        "session_date": args.session_date, "results": results,
        "all_matching_event_ids": len(results) == len(underlyers) and all(
            row["matching_event_ids"] and row["matching_engine_input_event_ids"] for row in results),
        "source_writes": 0}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
