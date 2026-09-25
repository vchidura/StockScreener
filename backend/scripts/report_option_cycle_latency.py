#!/usr/bin/env python3
"""Summarize retained Options worker cycle latency without writes or provider calls."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime
import json
import math
from pathlib import Path
import re
import sys

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
DEFAULT_LOG = BACKEND_DIR / "backups/options-worker/worker.log"
_TIMESTAMP = r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})"
_START = re.compile(_TIMESTAMP + r" .* materializing option slot=(?P<slot>\S+)")
_COMPLETE = re.compile(_TIMESTAMP + r" .* option slot complete slot=(?P<slot>\S+) statuses=(?P<statuses>.*?)(?: stock_behavior_shadow=|$)")
_DETECTOR = re.compile(_TIMESTAMP + r" .* option detector evaluation slot=(?P<slot>\S+)")
_OUTCOMES = re.compile(_TIMESTAMP + r" .* option outcomes candidates=")
_VALIDATION = re.compile(_TIMESTAMP + r" .* option continuous validation=")
_BUDGET = re.compile(r"option trade ingestion budget exceeded underlyer=(?P<underlyer>\S+)")
_FETCH_FAILURE = re.compile(r"option trade fetch skipped; continuing .* underlyer=(?P<underlyer>\S+)")


def _stamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S,%f")


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def parse_cycles(lines: list[str]) -> list[dict]:
    cycles: dict[str, dict] = {}
    active_slot: str | None = None
    for line in lines:
        start = _START.search(line)
        if start:
            active_slot = start["slot"]
            cycles[active_slot] = {
                "slot": active_slot,
                "started_at_log_time": start["timestamp"],
                "trade_budget_exceeded_underlyers": [],
                "trade_fetch_failed_underlyers": [],
            }
            continue
        if active_slot is not None:
            budget = _BUDGET.search(line)
            if budget:
                cycles[active_slot]["trade_budget_exceeded_underlyers"].append(budget["underlyer"])
            failure = _FETCH_FAILURE.search(line)
            if failure:
                cycles[active_slot]["trade_fetch_failed_underlyers"].append(failure["underlyer"])
        complete = _COMPLETE.search(line)
        if complete and complete["slot"] in cycles:
            row = cycles[complete["slot"]]
            row["completed_at_log_time"] = complete["timestamp"]
            row["statuses"] = complete["statuses"]
            row["materialization_seconds"] = round(
                (_stamp(complete["timestamp"]) - _stamp(row["started_at_log_time"])).total_seconds(), 3
            )
            active_slot = complete["slot"]
            continue
        detector = _DETECTOR.search(line)
        if detector and detector["slot"] in cycles:
            row = cycles[detector["slot"]]
            row["detector_logged_at"] = detector["timestamp"]
            if row.get("completed_at_log_time"):
                row["detector_log_tail_seconds"] = round(
                    (_stamp(detector["timestamp"]) - _stamp(row["completed_at_log_time"])).total_seconds(), 3
                )
            continue
        outcomes = _OUTCOMES.search(line)
        if outcomes and active_slot in cycles:
            row = cycles[active_slot]
            row["outcomes_logged_at"] = outcomes["timestamp"]
            if row.get("completed_at_log_time"):
                row["outcomes_tail_seconds"] = round(
                    (_stamp(outcomes["timestamp"]) - _stamp(row["completed_at_log_time"])).total_seconds(), 3
                )
            continue
        validation = _VALIDATION.search(line)
        if validation and active_slot in cycles:
            row = cycles[active_slot]
            row["validation_logged_at"] = validation["timestamp"]
            if row.get("outcomes_logged_at"):
                row["validation_tail_seconds"] = round(
                    (_stamp(validation["timestamp"]) - _stamp(row["outcomes_logged_at"])).total_seconds(), 3
                )
    return [cycles[key] for key in sorted(cycles)]


def _safe_settings(path: Path) -> dict:
    allowed = {
        "OPTION_TRADE_INGESTION_ENABLED": "false",
        "OPTION_TRADE_WATCHLIST_PER_UNDERLYER": "15",
        "OPTION_TRADE_INGESTION_BUDGET_SECONDS": "120",
    }
    if path.is_file():
        for raw in path.read_text(encoding="utf-8").splitlines():
            key, separator, value = raw.partition("=")
            if separator and key.strip() in allowed:
                allowed[key.strip()] = value.strip()
    return {
        "trade_ingestion_enabled": allowed["OPTION_TRADE_INGESTION_ENABLED"].lower() in {"1", "true", "yes", "on"},
        "trade_watchlist_per_underlyer": int(allowed["OPTION_TRADE_WATCHLIST_PER_UNDERLYER"]),
        "trade_ingestion_budget_seconds": int(allowed["OPTION_TRADE_INGESTION_BUDGET_SECONDS"]),
    }


def _seconds(later, earlier) -> float | None:
    if later is None or earlier is None:
        return None
    return round((later - earlier).total_seconds(), 3)


def summarize_persisted_phases(rows: list[dict]) -> dict:
    phases = (
        "pre_chain_seconds", "chain_seconds", "post_chain_to_analysis_seconds",
        "analysis_persist_seconds", "post_analysis_strategy_seconds", "batch_to_work_seconds",
    )
    grouped: dict[str, list[dict]] = {}
    for source in rows:
        row = dict(source)
        grouped.setdefault(row["scheduled_cycle"].isoformat(), []).append(row)
    cycles = []
    all_rows = []
    for slot, members in sorted(grouped.items()):
        members.sort(key=lambda row: (row["ingestion_started_at"], row["underlying"]))
        previous_completed = None
        for row in members:
            row["pre_chain_seconds"] = _seconds(row["ingestion_started_at"], previous_completed)
            row["chain_seconds"] = round(row["latency_ms"] / 1000, 3) if row.get("latency_ms") is not None else _seconds(row.get("ingestion_completed_at"), row["ingestion_started_at"])
            row["post_chain_to_analysis_seconds"] = _seconds(row.get("analysis_started_at"), row.get("ingestion_completed_at"))
            row["analysis_persist_seconds"] = _seconds(row.get("analysis_completed_at"), row.get("analysis_started_at"))
            row["post_analysis_strategy_seconds"] = _seconds(row.get("work_completed_at"), row.get("analysis_completed_at"))
            row["batch_to_work_seconds"] = _seconds(row.get("work_completed_at"), row["ingestion_started_at"])
            previous_completed = row.get("work_completed_at") or previous_completed
            all_rows.append(row)
        starts = [row["ingestion_started_at"] for row in members]
        completions = [row["work_completed_at"] for row in members if row.get("work_completed_at")]
        cycles.append({
            "slot": slot,
            "underlyers": len(members),
            "persisted_underlyer_span_seconds": _seconds(max(completions), min(starts)) if completions else None,
            "received_contracts": sum(row.get("received_row_count") or 0 for row in members),
            "retained_contracts": sum(row.get("retained_row_count") or 0 for row in members),
            "chain_pages": sum(row.get("page_count") or 0 for row in members),
            "chain_response_bytes": sum(row.get("chain_response_bytes") or 0 for row in members),
        })
    phase_summary = {}
    for phase in phases:
        values = [row[phase] for row in all_rows if row.get(phase) is not None and row[phase] >= 0]
        phase_summary[phase] = {"samples": len(values), "median": _percentile(values, 0.5),
            "p95": _percentile(values, 0.95), "maximum": max(values) if values else None}
    slowest = sorted((row for row in all_rows if row.get("batch_to_work_seconds") is not None),
        key=lambda row: row["batch_to_work_seconds"], reverse=True)[:20]
    return {
        "phase_seconds_per_underlyer": phase_summary,
        "cycles": cycles,
        "slowest_underlyers": [{key: (value.isoformat() if isinstance(value, datetime) else value)
            for key, value in row.items() if key in {"scheduled_cycle", "underlying", "status",
                "received_row_count", "retained_row_count", "page_count", *phases}} for row in slowest],
        "source_rows": len(all_rows),
        "source_writes": 0,
        "limitations": [
            "Pre-chain timing is available only after the first underlyer in each sequential cycle.",
            "Post-chain-to-analysis includes catalog reads, bar/rate/dividend inputs, normalization, snapshot/OI persistence, trade ingestion, and analysis computation.",
            "Analysis started_at is recorded after analysis computation and therefore measures analysis persistence, not CPU calculation.",
            "Trade fetch batches do not retain a parent matrix ID, so trade request/event counts cannot be attributed to one chain cycle from storage alone.",
        ],
    }


def _plan_nodes(value) -> list[dict]:
    nodes = []
    if isinstance(value, dict):
        if "Node Type" in value:
            nodes.append({key: value[key] for key in (
                "Node Type", "Relation Name", "Index Name", "Plan Rows", "Total Cost",
            ) if key in value})
        for child in value.values():
            nodes.extend(_plan_nodes(child))
    elif isinstance(value, list):
        for child in value:
            nodes.extend(_plan_nodes(child))
    return nodes


def read_trade_query_diagnostics(cursor, session_date: date) -> dict:
    cursor.execute("""
        SELECT tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename LIKE 'option_trade_events%'
        ORDER BY tablename, indexname
    """)
    indexes = [dict(row) for row in cursor.fetchall()]
    cursor.execute("""
        SELECT relname, seq_scan, seq_tup_read, idx_scan, n_live_tup
        FROM pg_stat_user_tables
        WHERE relname LIKE 'option_trade_events%'
        ORDER BY relname
    """)
    table_stats = [dict(row) for row in cursor.fetchall()]
    cursor.execute("""
        EXPLAIN (FORMAT JSON)
        SELECT trade_event_id, provider, contract_id, contract_ticker, underlying,
               sip_timestamp, sequence_number, participant_timestamp,
               first_observed_at, revised_observed_at, exchange, conditions,
               correction, provider_trade_id, price, size, shares_per_contract,
               notional, payload_sha256, raw_batch_id, classification_status,
               classification_reasons, semantics_version
        FROM option_trade_events
        WHERE underlying = %s
          AND sip_timestamp BETWEEN %s::date AND %s::date + INTERVAL '1 day'
          AND COALESCE(revised_observed_at, first_observed_at) <= %s::date + INTERVAL '1 day'
          AND classification_status = 'INCLUDED'
        ORDER BY sip_timestamp, sequence_number,
                 participant_timestamp NULLS FIRST, trade_event_id
    """, ("SPY", session_date, session_date, session_date))
    plan = cursor.fetchone()["QUERY PLAN"]
    return {
        "representative_underlyer": "SPY",
        "planner_nodes": _plan_nodes(plan),
        "indexes": indexes,
        "table_stats": table_stats,
        "explain_analyze": False,
        "source_writes": 0,
    }


def read_statement_diagnostics(cursor) -> dict:
    cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements') AS installed")
    if not cursor.fetchone()["installed"]:
        return {"available": False, "reason": "PG_STAT_STATEMENTS_NOT_INSTALLED", "source_writes": 0}
    cursor.execute("SELECT stats_reset FROM pg_stat_statements_info")
    reset = cursor.fetchone()["stats_reset"]
    cursor.execute("""
        SELECT queryid, calls, rows, total_exec_time, mean_exec_time,
               temp_blks_written, shared_blks_read, shared_blks_hit,
               LEFT(REGEXP_REPLACE(query, '\\s+', ' ', 'g'), 320) AS query
        FROM pg_stat_statements
        WHERE dbid = (SELECT oid FROM pg_database WHERE datname=current_database())
          AND (
              query LIKE '%%FROM option_trade_events%%WHERE underlying =%%'
              OR query LIKE '%%WITH candidate_legs AS%%eligible_batches AS%%'
              OR query LIKE '%%WITH candidate_queue AS%%option_signal_current_marks%%'
              OR query LIKE '%%WITH session_clocks AS%%option_strategy_candidates%%'
          )
        ORDER BY total_exec_time DESC
        LIMIT 30
    """)
    return {"available": True, "stats_reset": reset.isoformat() if reset else None,
        "statements": [dict(row) for row in cursor.fetchall()], "source_writes": 0}


def read_persisted_phases(session_date: date) -> dict:
    load_dotenv(BACKEND_DIR / ".env")
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '30s'")
        cursor.execute("""
            WITH pages AS (
                SELECT batch_id, SUM(byte_count)::bigint AS chain_response_bytes
                FROM option_raw_batch_pages
                GROUP BY batch_id
            )
            SELECT ingestion.scheduled_cycle, ingestion.underlying, ingestion.status,
                   ingestion.started_at AS ingestion_started_at,
                   ingestion.completed_at AS ingestion_completed_at,
                   ingestion.latency_ms, ingestion.page_count,
                   ingestion.received_row_count, ingestion.retained_row_count,
                   COALESCE(pages.chain_response_bytes, 0) AS chain_response_bytes,
                   analysis.started_at AS analysis_started_at,
                   analysis.completed_at AS analysis_completed_at,
                   work.completed_at AS work_completed_at
            FROM option_ingestion_runs AS ingestion
            LEFT JOIN option_analysis_runs AS analysis USING (batch_id)
            LEFT JOIN option_work_items AS work
              ON work.stage = 'NORMALIZE'
             AND work.business_key = 'normalize:' || ingestion.batch_id::text
            LEFT JOIN pages USING (batch_id)
            WHERE ingestion.scheduled_cycle >= %s::date
              AND ingestion.scheduled_cycle < %s::date + INTERVAL '1 day'
                            AND (analysis.batch_id IS NOT NULL OR work.work_id IS NOT NULL)
            ORDER BY ingestion.scheduled_cycle, ingestion.started_at, ingestion.underlying
                """, (session_date, session_date))
        rows = [dict(row) for row in cursor.fetchall()]
        trade_diagnostics = read_trade_query_diagnostics(cursor, session_date)
        statement_diagnostics = read_statement_diagnostics(cursor)
    result = summarize_persisted_phases(rows)
    result["trade_query_diagnostics"] = trade_diagnostics
    result["statement_diagnostics"] = statement_diagnostics
    return result


def build_report(lines: list[str], *, session_date: date | None, settings: dict, underlyers: int) -> dict:
    cycles = parse_cycles(lines)
    if session_date is not None:
        cycles = [row for row in cycles if datetime.fromisoformat(row["slot"]).date() == session_date]
    complete = [row for row in cycles if "materialization_seconds" in row]
    materialization = [row["materialization_seconds"] for row in complete]
    outcomes = [row["outcomes_tail_seconds"] for row in complete if "outcomes_tail_seconds" in row]
    statuses = Counter()
    for row in complete:
        for item in row.get("statuses", "").split(","):
            name, separator, count = item.partition(":")
            if separator and count.isdigit():
                statuses[name] += int(count)
    trade_cap = underlyers * settings["trade_watchlist_per_underlyer"] if settings["trade_ingestion_enabled"] else 0
    return {
        "version": "option_cycle_latency_report_v1",
        "mode": "READ_ONLY_LOG_EVIDENCE",
        "session_date": session_date.isoformat() if session_date else None,
        "cycle_count": len(cycles),
        "complete_cycle_count": len(complete),
        "materialization_seconds": {
            "minimum": min(materialization) if materialization else None,
            "median": _percentile(materialization, 0.5),
            "p95": _percentile(materialization, 0.95),
            "maximum": max(materialization) if materialization else None,
        },
        "outcomes_tail_seconds": {
            "median": _percentile(outcomes, 0.5),
            "p95": _percentile(outcomes, 0.95),
            "maximum": max(outcomes) if outcomes else None,
        },
        "cycles_exceeding_15_minutes": sum(value > 900 for value in materialization),
        "status_counts": dict(sorted(statuses.items())),
        "trade_ingestion": settings | {
            "underlyers": underlyers,
            "maximum_serial_contract_requests_per_cycle": trade_cap,
            "maximum_serial_budget_seconds_per_cycle": (
                underlyers * settings["trade_ingestion_budget_seconds"]
                if settings["trade_ingestion_enabled"] else 0
            ),
            "observed_budget_exceeded_cycles": sum(bool(row["trade_budget_exceeded_underlyers"]) for row in cycles),
            "observed_fetch_failure_cycles": sum(bool(row["trade_fetch_failed_underlyers"]) for row in cycles),
        },
        "cycles": complete,
        "limitations": [
            "Historical logs expose cycle boundaries but not provider, normalization, persistence, strategy, or detector sub-phases.",
            "Request ceilings are configured maxima, not proof every request was issued.",
        ],
        "source_writes": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--underlyers", type=int, default=13)
    parser.add_argument("--database-breakdown", action="store_true")
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.underlyers <= 1000:
        raise ValueError("underlyers must be between 1 and 1000")
    lines = args.log.read_text(encoding="utf-8", errors="replace").splitlines()
    report = build_report(lines, session_date=args.session_date,
        settings=_safe_settings(BACKEND_DIR / ".env"), underlyers=args.underlyers)
    if args.database_breakdown:
        if args.session_date is None:
            raise ValueError("--database-breakdown requires --session-date")
        report["persisted_phase_breakdown"] = read_persisted_phases(args.session_date)
    if args.compact:
        report["cycles"] = report["cycles"][-8:]
        breakdown = report.get("persisted_phase_breakdown")
        if breakdown:
            breakdown["cycles"] = breakdown["cycles"][-8:]
            breakdown["slowest_underlyers"] = breakdown["slowest_underlyers"][:15]
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
