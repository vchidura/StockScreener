"""Read-only B1 inventory for legacy equity-model backtesting readiness."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

import exchange_calendars


BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = Path(r"C:\Users\vachi\source\repo\AlphaResearchPro\backend\.env")
DEFAULT_EXPECTED_DATABASE = "stocks_db_fresh"
DEFAULT_POLICY_VERSION = "liquid_us_common_stocks_v2"


LANE_REQUIREMENTS: dict[str, tuple[str, ...]] = {
    "controls_r2_b3_f2": (
        "dated_universe_membership",
        "adjusted_daily_features",
        "raw_30m_history",
        "full_hour_derivation",
        "benchmark_context",
        "action_coverage",
        "warmup_continuity",
    ),
    "sw3_r_b_f": (
        "dated_universe_membership",
        "adjusted_daily_features",
        "raw_30m_history",
        "full_hour_derivation",
        "benchmark_context",
        "action_coverage",
        "warmup_continuity",
        "prior_session_context",
        "multi_session_paths",
    ),
    "sw1": (
        "pit_sector_mapping",
        "sector_benchmark_daily_hourly",
        "earnings_coverage",
    ),
    "d1_d2": (
        "raw_5m_history",
        "prior_daily_atr",
        "opening_state",
        "volume_baselines",
        "aggregate_vwap",
    ),
    "p1_p2": (
        "survivorship_safe_universe",
        "split_dividend_consistency",
        "pit_fundamentals",
        "filing_availability",
        "sector_ranks",
        "long_mature_outcomes",
    ),
}

BLOCKER_CODE = {
    "dated_universe_membership": "UNIVERSE_MEMBERSHIP_COVERAGE_GAP",
    "adjusted_daily_features": "ADJUSTED_DAILY_FEATURE_COVERAGE_GAP",
    "raw_30m_history": "RAW_30M_HISTORY_GAP",
    "full_hour_derivation": "FULL_HOUR_DERIVATION_GAP",
    "benchmark_context": "BENCHMARK_CONTEXT_GAP",
    "action_coverage": "CORPORATE_ACTION_COVERAGE_GAP",
    "warmup_continuity": "DAILY_WARMUP_CONTINUITY_GAP",
    "prior_session_context": "PRIOR_SESSION_CONTEXT_GAP",
    "multi_session_paths": "MULTI_SESSION_PATH_GAP",
    "pit_sector_mapping": "PIT_SECTOR_MAPPING_GAP",
    "sector_benchmark_daily_hourly": "SECTOR_BENCHMARK_DAILY_HOURLY_GAP",
    "earnings_coverage": "EARNINGS_EVENT_COVERAGE_GAP",
    "raw_5m_history": "RAW_5M_HISTORY_GAP",
    "prior_daily_atr": "PRIOR_DAILY_ATR_GAP",
    "opening_state": "OPENING_STATE_GAP",
    "volume_baselines": "VOLUME_BASELINE_GAP",
    "aggregate_vwap": "AGGREGATE_VWAP_GAP",
    "survivorship_safe_universe": "SURVIVORSHIP_SAFE_UNIVERSE_GAP",
    "split_dividend_consistency": "SPLIT_DIVIDEND_CONSISTENCY_GAP",
    "pit_fundamentals": "PIT_FUNDAMENTAL_CONCEPT_GAP",
    "filing_availability": "PIT_FILING_AVAILABILITY_GAP",
    "sector_ranks": "SECTOR_RANK_INPUT_GAP",
    "long_mature_outcomes": "MATURE_OUTCOME_HISTORY_GAP",
}


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )


def sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("ascii")).hexdigest()


def classify_lane_readiness(gates: dict[str, bool]) -> dict[str, Any]:
    lanes: dict[str, Any] = {}
    for lane, required in LANE_REQUIREMENTS.items():
        missing = [name for name in required if not gates.get(name, False)]
        lanes[lane] = {
            "status": "INPUT_READY" if not missing else "BLOCKED_INPUT_READINESS",
            "required_gates": list(required),
            "missing_gates": missing,
            "blockers": [BLOCKER_CODE[name] for name in missing],
        }
    return lanes


def _expected_sessions(start: date, end: date) -> list[date]:
    calendar = exchange_calendars.get_calendar("XNYS")
    return [value.date() for value in calendar.sessions_in_range(start, end)]


def _load_daily_artifact(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("daily coverage artifact must be a JSON object")
    return payload


def _read_only_guard(cursor, expected_database: str) -> dict[str, Any]:
    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    cursor.execute("SET LOCAL statement_timeout = '300s'")
    cursor.execute(
        """
        SELECT current_database() AS database_name,
               current_setting('transaction_read_only') AS transaction_read_only,
               current_setting('transaction_isolation') AS transaction_isolation,
               transaction_timestamp() AS cutoff
        """
    )
    metadata = dict(cursor.fetchone())
    if metadata["database_name"] != expected_database:
        raise RuntimeError(
            f"unexpected database '{metadata['database_name']}', expected '{expected_database}'"
        )
    if metadata["transaction_read_only"] != "on":
        raise RuntimeError("read-only guard failed: transaction_read_only is not on")
    return metadata


def _inventory_intraday(cursor, first_session: date, last_session: date) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT interval, COUNT(*) AS revisions,
               COUNT(DISTINCT (ticker, session_date)) AS ticker_sessions,
               COUNT(DISTINCT security_id) AS securities,
               MIN(session_date) AS first_session, MAX(session_date) AS last_session,
               COUNT(*) FILTER (
                    WHERE replay_available_at IS NULL OR replay_available_at <> bar_end
               ) AS invalid_replay_availability,
               COUNT(*) FILTER (WHERE volume IS NULL) AS missing_volume
        FROM equity_bar_revisions
        WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
          AND interval = ANY(%s)
          AND session_scope = 'RTH'
          AND adjusted = FALSE
          AND is_final
          AND session_date BETWEEN %s AND %s
        GROUP BY interval
        ORDER BY interval
        """,
        (["30m", "5m"], first_session, last_session),
    )
    summary_rows = [dict(row) for row in cursor.fetchall()]
    cursor.execute(
        """
        SELECT interval, session_date, COUNT(DISTINCT security_id) AS securities
        FROM equity_bar_revisions
        WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
          AND interval = ANY(%s)
          AND session_scope = 'RTH'
          AND adjusted = FALSE
          AND is_final
          AND session_date BETWEEN %s AND %s
        GROUP BY interval, session_date
        ORDER BY interval, session_date
        """,
        (["30m", "5m"], first_session, last_session),
    )
    coverage_rows = [dict(row) for row in cursor.fetchall()]
    expected = _expected_sessions(first_session, last_session)
    by_interval = {}
    for interval in ("30m", "5m"):
        covered = {row["session_date"] for row in coverage_rows if row["interval"] == interval}
        missing = sorted(set(expected) - covered)
        by_interval[interval] = {
            "covered_sessions": len(covered),
            "expected_sessions": len(expected),
            "missing_sessions": len(missing),
            "missing_session_examples": [value.isoformat() for value in missing[:20]],
            "minimum_securities_per_session": min(
                (row["securities"] for row in coverage_rows if row["interval"] == interval), default=0
            ),
            "maximum_securities_per_session": max(
                (row["securities"] for row in coverage_rows if row["interval"] == interval), default=0
            ),
        }
    return {"summary": summary_rows, "session_coverage": by_interval}


def _inventory_full_hour_feasibility(cursor, first_session: date, last_session: date) -> dict[str, Any]:
    cursor.execute(
        """
        WITH bars AS (
            SELECT security_id, ticker, session_date,
                   (bar_start AT TIME ZONE 'America/New_York')::time AS local_start
            FROM equity_bar_revisions
            WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND interval = '30m'
              AND session_scope = 'RTH'
              AND adjusted = FALSE
              AND is_final
              AND session_date BETWEEN %s AND %s
        ), scored AS (
            SELECT security_id, ticker, session_date,
                   MIN(local_start) AS first_local_start,
                   BOOL_AND(EXTRACT(MINUTE FROM local_start)::int IN (0, 30)) AS half_hour_aligned,
                   COUNT(*) FILTER (
                       WHERE local_start >= TIME '09:30' AND local_start < TIME '15:30'
                   ) AS first_twelve_half_hours
            FROM bars
            GROUP BY security_id, ticker, session_date
        )
        SELECT COUNT(*) AS total_security_sessions,
               COUNT(*) FILTER (
                   WHERE half_hour_aligned
                     AND first_local_start = TIME '09:30'
                     AND first_twelve_half_hours >= 12
               ) AS full_hour_feasible_security_sessions,
               COUNT(*) FILTER (
                   WHERE NOT half_hour_aligned OR first_local_start <> TIME '09:30'
               ) AS alignment_violations,
               COUNT(*) FILTER (WHERE first_twelve_half_hours < 12) AS missing_first_twelve_half_hours
        FROM scored
        """
        ,
        (first_session, last_session),
    )
    return dict(cursor.fetchone())


def _inventory_sw1_event_coverage(cursor, first_session: date, last_session: date) -> dict[str, Any]:
    cursor.execute(
        """
        WITH universe_tickers AS (
            SELECT DISTINCT member.ticker
            FROM equity_original_universe_runs AS run
            JOIN equity_universe_members AS member USING (universe_run_id)
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND run.policy_version = %s
              AND run.effective_from::date BETWEEN %s AND %s
        ), coverage AS (
            SELECT coverage.affected_underlying AS ticker,
                   MIN(coverage.window_start)::date AS first_window_start,
                   MAX(coverage.window_end)::date AS last_window_end,
                   COUNT(*) AS windows
            FROM option_event_calendar_coverage AS coverage
            JOIN universe_tickers AS u ON u.ticker = coverage.affected_underlying
            WHERE coverage.event_type = 'EARNINGS'
            GROUP BY coverage.affected_underlying
        ), events AS (
            SELECT event.affected_underlying AS ticker,
                   COUNT(*) AS events,
                   MIN(event.scheduled_time)::date AS first_event_date,
                   MAX(event.scheduled_time)::date AS last_event_date
            FROM option_market_events AS event
            JOIN universe_tickers AS u ON u.ticker = event.affected_underlying
            WHERE event.event_type = 'EARNINGS'
              AND event.scheduled_time::date BETWEEN %s AND %s
            GROUP BY event.affected_underlying
        )
        SELECT (SELECT COUNT(*) FROM universe_tickers) AS universe_tickers,
               COUNT(*) AS tickers_with_coverage_windows,
               COUNT(*) FILTER (
                   WHERE coverage.first_window_start <= %s AND coverage.last_window_end >= %s
               ) AS tickers_with_full_span_coverage,
               COUNT(events.ticker) AS tickers_with_events_in_range,
               COALESCE(SUM(events.events), 0) AS total_events_in_range
        FROM coverage
        LEFT JOIN events USING (ticker)
        """,
        (DEFAULT_POLICY_VERSION, first_session, last_session, first_session, last_session, first_session, last_session),
    )
    return dict(cursor.fetchone())


def _inventory_fundamentals(cursor, first_session: date, last_session: date) -> dict[str, Any]:
    concepts = (
        "revenue", "operating_income", "net_income", "diluted_eps",
        "cash_and_equivalents", "current_debt", "long_term_debt",
        "operating_cash_flow", "capital_expenditures", "free_cash_flow",
    )
    select_parts = [
        "COUNT(*) AS reports",
        "COUNT(DISTINCT report.security_id) AS securities_with_reports",
        "COUNT(*) FILTER (WHERE report.filing_date IS NOT NULL) AS reports_with_filing_date",
        "COUNT(*) FILTER (WHERE report.availability_time IS NOT NULL) AS reports_with_availability_time",
        "COUNT(*) FILTER (WHERE report.observed_at IS NOT NULL AND report.created_at IS NOT NULL) AS reports_with_observation_and_creation",
        "COUNT(DISTINCT report.security_id) FILTER (WHERE report.filing_date IS NOT NULL AND report.availability_time IS NOT NULL AND report.observed_at IS NOT NULL AND report.created_at IS NOT NULL) AS securities_with_complete_pit_report",
    ] + [
        f"COUNT(*) FILTER (WHERE report.{concept} IS NOT NULL) AS {concept}_reports"
        for concept in concepts
    ]
    cursor.execute(
        f"""
        WITH universe_security AS (
            SELECT DISTINCT member.security_id
            FROM equity_original_universe_runs AS run
            JOIN equity_universe_members AS member USING (universe_run_id)
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND run.policy_version = %s
              AND run.effective_from::date BETWEEN %s AND %s
        )
        SELECT {",".join(select_parts)}
        FROM equity_fundamental_reports AS report
        JOIN universe_security AS member USING (security_id)
        WHERE report.timeframe IN ('quarterly', 'annual', 'trailing_twelve_months')
          AND report.period_end <= %s
        """,
        (DEFAULT_POLICY_VERSION, first_session, last_session, last_session),
    )
    summary = dict(cursor.fetchone())
    summary["required_concepts"] = list(concepts)
    summary["concepts_with_any_coverage"] = [
        concept for concept in concepts if summary.get(f"{concept}_reports", 0) > 0
    ]
    return summary


def derive_gates(daily: dict[str, Any], inventory: dict[str, Any]) -> dict[str, bool]:
    universe = daily.get("universe", {})
    lineage = (daily.get("bar_lineages") or [{}])[0]
    coverage = (daily.get("price_coverage") or [{}])[0]
    totals = coverage.get("totals", {})
    benchmarks = coverage.get("benchmarks", {})
    refs = daily.get("reference_actions", {}).get("references", {})
    actions = daily.get("reference_actions", {}).get("historical_action_inventory", [])
    action_types = {row.get("action_type") for row in actions}
    intraday_summary = {row["interval"]: row for row in inventory["intraday"]["summary"]}
    fundamentals = inventory["fundamentals"]
    sw1 = inventory["sw1_earnings"]
    full_hour = inventory["full_hour"]
    maturity_21 = coverage.get("maturity", {}).get("21", {})
    mature = int(maturity_21.get("mature_memberships", 0) or 0)
    missing_paths = int(maturity_21.get("missing_forward_paths", 0) or 0)
    mature_ratio = 0.0 if mature <= 0 else (mature - missing_paths) / mature
    return {
        "dated_universe_membership": int(universe.get("runs", 0) or 0) > 0,
        "adjusted_daily_features": bool(lineage.get("adjusted")) and int(lineage.get("ticker_sessions", 0) or 0) > 0,
        "raw_30m_history": int(intraday_summary.get("30m", {}).get("ticker_sessions", 0) or 0) > 0,
        "full_hour_derivation": int(full_hour.get("full_hour_feasible_security_sessions", 0) or 0) > 0,
        "benchmark_context": len(benchmarks.get("missing_or_incomplete", [])) == 0,
        "action_coverage": {"SPLIT", "DIVIDEND"}.issubset(action_types),
        "warmup_continuity": int(totals.get("with_contiguous_warmup_252", 0) or 0) > 0,
        "prior_session_context": int(intraday_summary.get("30m", {}).get("ticker_sessions", 0) or 0) > 0,
        "multi_session_paths": int(totals.get("with_contiguous_forward_21", 0) or 0) > 0,
        "pit_sector_mapping": int(refs.get("dated_effective_sector", 0) or 0) > 0 and int(refs.get("sector_with_source_as_of_date", 0) or 0) > 0,
        "sector_benchmark_daily_hourly": len(benchmarks.get("missing_or_incomplete", [])) == 0 and int(intraday_summary.get("30m", {}).get("ticker_sessions", 0) or 0) > 0,
        "earnings_coverage": int(sw1.get("tickers_with_full_span_coverage", 0) or 0) > 0,
        "raw_5m_history": int(intraday_summary.get("5m", {}).get("ticker_sessions", 0) or 0) > 0,
        "prior_daily_atr": int(totals.get("with_contiguous_warmup_252", 0) or 0) > 0,
        "opening_state": int(intraday_summary.get("5m", {}).get("covered_sessions", 0) or 0) > 0,
        "volume_baselines": int(intraday_summary.get("5m", {}).get("missing_volume", 0) or 0) == 0 if "missing_volume" in intraday_summary.get("5m", {}) else int(intraday_summary.get("5m", {}).get("ticker_sessions", 0) or 0) > 0,
        "aggregate_vwap": int(intraday_summary.get("5m", {}).get("ticker_sessions", 0) or 0) > 0,
        "survivorship_safe_universe": int(universe.get("runs", 0) or 0) > 0,
        "split_dividend_consistency": {"SPLIT", "DIVIDEND"}.issubset(action_types),
        "pit_fundamentals": int(fundamentals.get("securities_with_complete_pit_report", 0) or 0) > 0 and len(fundamentals.get("concepts_with_any_coverage", [])) == len(fundamentals.get("required_concepts", [])),
        "filing_availability": int(fundamentals.get("reports_with_filing_date", 0) or 0) > 0,
        "sector_ranks": int(refs.get("dated_effective_sector", 0) or 0) > 0 and int(refs.get("sector_with_source_as_of_date", 0) or 0) > 0,
        "long_mature_outcomes": mature_ratio >= 0.95,
    }


def _write_immutable(path: Path, payload: str) -> str:
    content = payload.encode("utf-8")
    digest = hashlib.sha256(content).hexdigest()
    if path.exists():
        existing = path.read_bytes()
        existing_digest = hashlib.sha256(existing).hexdigest()
        if existing_digest != digest:
            raise FileExistsError(f"immutable artifact conflict at {path}")
        return digest
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return digest


def run_audit(*, env_file: Path, daily_artifact: Path, output: Path, expected_database: str) -> dict[str, Any]:
    from dotenv import load_dotenv

    if str(BACKEND_DIR) not in sys.path:
        sys.path.insert(0, str(BACKEND_DIR))
    load_dotenv(env_file)
    from database import get_db_cursor

    daily = _load_daily_artifact(daily_artifact)
    first_session = date.fromisoformat(daily["universe"]["first_session"])
    last_session = date.fromisoformat(daily["universe"]["last_session"])
    with get_db_cursor() as cursor:
        transaction = _read_only_guard(cursor, expected_database)
        intraday = _inventory_intraday(cursor, first_session, last_session)
        full_hour = _inventory_full_hour_feasibility(cursor, first_session, last_session)
        sw1_earnings = _inventory_sw1_event_coverage(cursor, first_session, last_session)
        fundamentals = _inventory_fundamentals(cursor, first_session, last_session)
    report = {
        "audit_version": "legacy_equity_b1_inventory_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_daily_artifact": {
            "path": str(daily_artifact),
            "sha256": hashlib.sha256(daily_artifact.read_bytes()).hexdigest(),
        },
        "transaction": transaction,
        "window": {"first_session": first_session.isoformat(), "last_session": last_session.isoformat()},
        "intraday": intraday,
        "full_hour": full_hour,
        "sw1_earnings": sw1_earnings,
        "fundamentals": fundamentals,
    }
    gates = derive_gates(daily, report)
    report["model_gate_inputs"] = gates
    report["lane_readiness"] = classify_lane_readiness(gates)
    report["readiness_sha256"] = sha256_json({
        key: value for key, value in report.items() if key != "readiness_sha256"
    })
    serialized = json.dumps(report, indent=2, default=str, ensure_ascii=True) + "\n"
    artifact_sha = _write_immutable(output, serialized)
    _write_immutable(output.with_suffix(output.suffix + ".sha256"), artifact_sha + "\n")
    return report | {"artifact_sha256": artifact_sha, "artifact_path": str(output)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--daily-artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-database", default=DEFAULT_EXPECTED_DATABASE)
    args = parser.parse_args()
    report = run_audit(
        env_file=args.env_file,
        daily_artifact=args.daily_artifact,
        output=args.output,
        expected_database=args.expected_database,
    )
    print(
        json.dumps(
            {
                "output": report["artifact_path"],
                "sha256": report["artifact_sha256"],
                "database": report["transaction"]["database_name"],
                "transaction_read_only": report["transaction"]["transaction_read_only"],
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
