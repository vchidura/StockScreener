"""Autonomous legacy-equity backtest execution over causally valid retained windows."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

import exchange_calendars

BACKEND_DIR = Path(__file__).resolve().parents[1]
ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

from research.legacy_backtest_variants import lane_variants
from research.stock_idea_engine import digest
from research.stock_idea_evaluation import evaluate_publications, validate_plan
from research.stock_idea_replay import build_observations, replay_observations, summarize, validate_inputs, write_once
from scripts.run_stock_idea_replay import freeze_inputs, publication_windows


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_only_guard(cursor, expected_database: str) -> dict[str, Any]:
    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    cursor.execute("SET LOCAL statement_timeout='300s'")
    cursor.execute(
        """
        SELECT current_database() AS database_name,
               current_setting('transaction_read_only') AS transaction_read_only,
               current_setting('transaction_isolation') AS transaction_isolation,
               transaction_timestamp() AS cutoff
        """
    )
    row = dict(cursor.fetchone())
    if row["database_name"] != expected_database:
        raise RuntimeError(f"unexpected database {row['database_name']!r}, expected {expected_database!r}")
    if row["transaction_read_only"] != "on":
        raise RuntimeError("transaction_read_only is not on")
    return row


def inventory_modes(*, expected_database: str) -> dict[str, Any]:
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        transaction = _read_only_guard(cursor, expected_database)
        cursor.execute(
            """
            SELECT effective_from::date AS session_date
            FROM equity_original_universe_runs
            WHERE availability_mode='HISTORICAL_RECONSTRUCTED'
              AND policy_version='liquid_us_common_stocks_v2'
            ORDER BY session_date
            """
        )
        universe_sessions = [row["session_date"] for row in cursor.fetchall()]
        if not universe_sessions:
            raise RuntimeError("no reconstructed universe sessions found")
        universe_first, universe_last = universe_sessions[0], universe_sessions[-1]
        cursor.execute(
            """
            SELECT interval, availability_mode,
                   MIN(session_date) AS min_session,
                   MAX(session_date) AS max_session,
                   COUNT(*) AS revisions,
                   MIN(bar_end) AS min_bar_end,
                   MAX(bar_end) AS max_bar_end,
                   MIN(system_observed_at) AS min_observed_at,
                   MAX(system_observed_at) AS max_observed_at,
                   MIN(created_at) AS min_created_at,
                   MAX(created_at) AS max_created_at,
                   COUNT(*) FILTER (WHERE is_final) AS final_revisions,
                   COUNT(*) FILTER (WHERE is_final AND session_scope='RTH') AS final_rth_revisions
            FROM equity_bar_revisions
            WHERE interval IN ('30m', '5m')
              AND session_scope='RTH'
              AND adjusted=FALSE
            GROUP BY interval, availability_mode
            ORDER BY interval, availability_mode
            """
        )
        rows = [dict(row) for row in cursor.fetchall()]
        coverage = []
        for row in rows:
            interval = row["interval"]
            mode = row["availability_mode"]
            cursor.execute(
                """
                SELECT session_date, COUNT(DISTINCT security_id) AS securities,
                       COUNT(*) AS revisions
                FROM equity_bar_revisions
                WHERE interval=%s
                  AND availability_mode=%s
                  AND session_scope='RTH'
                  AND adjusted=FALSE
                  AND is_final
                  AND session_date BETWEEN %s AND %s
                GROUP BY session_date
                ORDER BY session_date
                """,
                (interval, mode, universe_first, universe_last),
            )
            sessions = [dict(item) for item in cursor.fetchall()]
            coverage.append(
                {
                    "interval": interval,
                    "availability_mode": mode,
                    "sessions_in_universe_window": len(sessions),
                    "first_session_in_universe_window": None if not sessions else sessions[0]["session_date"].isoformat(),
                    "last_session_in_universe_window": None if not sessions else sessions[-1]["session_date"].isoformat(),
                    "minimum_securities_per_session": min((item["securities"] for item in sessions), default=0),
                    "maximum_securities_per_session": max((item["securities"] for item in sessions), default=0),
                }
            )
        cursor.execute(
            """
            SELECT
              EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name='equity_bar_revisions' AND column_name='vwap'
              ) AS has_vwap,
              EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name='equity_bar_revisions' AND column_name='transaction_count'
              ) AS has_transaction_count
            """
        )
        d1_d2_columns = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT availability_mode,
                   COUNT(*) AS rows_5m_final_rth,
                   COUNT(*) FILTER (WHERE vwap IS NULL) AS vwap_null_rows,
                   COUNT(*) FILTER (WHERE transaction_count IS NULL) AS transaction_count_null_rows
            FROM equity_bar_revisions
            WHERE interval='5m'
              AND session_scope='RTH'
              AND adjusted=FALSE
              AND is_final
            GROUP BY availability_mode
            ORDER BY availability_mode
            """
        )
        d1_d2_mode_rows = [dict(row) for row in cursor.fetchall()]
    return {
        "inventory_version": "legacy_mode_inventory_v1",
        "generated_at": _now(),
        "transaction": transaction,
        "universe_window": {
            "first_session": universe_first.isoformat(),
            "last_session": universe_last.isoformat(),
            "session_count": len(universe_sessions),
        },
        "mode_rows": rows,
        "mode_coverage_in_universe_window": coverage,
        "d1_d2_required_inputs": {
            "columns": d1_d2_columns,
            "mode_null_coverage": d1_d2_mode_rows,
        },
    }


def _best_mode_window(inventory: dict[str, Any], interval: str) -> tuple[str, str, str] | None:
    eligible = [
        row for row in inventory["mode_coverage_in_universe_window"]
        if row["interval"] == interval and row["sessions_in_universe_window"] > 0
    ]
    if not eligible:
        return None
    best = max(
        eligible,
        key=lambda row: (
            row["sessions_in_universe_window"],
            row["maximum_securities_per_session"],
            row["availability_mode"],
        ),
    )
    return (
        best["availability_mode"],
        best["first_session_in_universe_window"],
        best["last_session_in_universe_window"],
    )


def _build_config_for_window(base: dict[str, Any], start: str, end: str) -> dict[str, Any]:
    config = json.loads(json.dumps(base))
    calendar = exchange_calendars.get_calendar(config["calendar"])
    start_session = calendar.date_to_session(start)
    end_session = calendar.date_to_session(end)
    warmup_start = calendar.session_offset(start_session, -253).date().isoformat()
    intraday_warmup_start = calendar.session_offset(start_session, -80).date().isoformat()
    exit_end = calendar.session_offset(end_session, 30).date().isoformat()
    config["start"] = str(start_session.date())
    config["end"] = str(end_session.date())
    config["warmup_start"] = warmup_start
    config["intraday_warmup_start"] = intraday_warmup_start
    config["exit_end"] = exit_end
    config["retained_live_from"] = "1900-01-01T00:00:00+00:00"
    return config


def _d1_d2_input_readiness(inventory: dict[str, Any], mode_window_5m: tuple[str, str, str] | None) -> dict[str, Any]:
    if mode_window_5m is None:
        return {
            "status": "BLOCKED_INPUT_READINESS",
            "reason": "RAW_5M_HISTORY_ABSENT_ALL_MODES",
        }
    requirements = inventory.get("d1_d2_required_inputs", {})
    columns = requirements.get("columns", {})
    if not columns.get("has_vwap"):
        return {
            "status": "BLOCKED_INPUT_READINESS",
            "reason": "RAW_5M_VWAP_COLUMN_ABSENT",
        }
    if not columns.get("has_transaction_count"):
        return {
            "status": "BLOCKED_INPUT_READINESS",
            "reason": "RAW_5M_TRANSACTION_COUNT_COLUMN_ABSENT",
        }
    selected_mode = mode_window_5m[0]
    mode_rows = requirements.get("mode_null_coverage", [])
    selected = next((row for row in mode_rows if row["availability_mode"] == selected_mode), None)
    if selected is None:
        return {
            "status": "BLOCKED_INPUT_READINESS",
            "reason": "RAW_5M_MODE_COVERAGE_ROW_ABSENT",
        }
    if int(selected["vwap_null_rows"]) > 0:
        return {
            "status": "BLOCKED_INPUT_READINESS",
            "reason": "RAW_5M_VWAP_NULL_ROWS_PRESENT",
            "details": {"availability_mode": selected_mode, "null_rows": int(selected["vwap_null_rows"])},
        }
    if int(selected["transaction_count_null_rows"]) > 0:
        return {
            "status": "BLOCKED_INPUT_READINESS",
            "reason": "RAW_5M_TRANSACTION_COUNT_NULL_ROWS_PRESENT",
            "details": {"availability_mode": selected_mode, "null_rows": int(selected["transaction_count_null_rows"])},
        }
    return {
        "status": "READY",
        "reason": "RAW_5M_VWAP_TRANSACTION_COUNT_PRESENT",
        "details": {"availability_mode": selected_mode},
    }


def _clip_window(start: str, end: str, max_sessions: int, calendar_name: str) -> tuple[str, str]:
    if max_sessions < 1:
        return start, end
    calendar = exchange_calendars.get_calendar(calendar_name)
    start_session = calendar.date_to_session(start)
    end_session = calendar.date_to_session(end)
    sessions = list(calendar.sessions_in_range(start_session, end_session))
    if len(sessions) <= max_sessions:
        return str(start_session.date()), str(end_session.date())
    clipped_start = sessions[-max_sessions]
    return str(clipped_start.date()), str(end_session.date())


def _study_status(evaluation: dict[str, Any]) -> str:
    cells = evaluation["cells"]
    statuses = {cell["status"] for cell in cells}
    if statuses == {"PASS_RESEARCH_ONLY"}:
        return "QUALIFIED_RESEARCH_ONLY"
    if "INSUFFICIENT_INDEPENDENT_BLOCKS" in statuses:
        return "INCONCLUSIVE_INSUFFICIENT_BLOCKS"
    if statuses == {"INSUFFICIENT_OUTCOME_COVERAGE"}:
        return "INCONCLUSIVE_INSUFFICIENT_COVERAGE"
    if "INVALID_PUBLICATION_AUDIT" in statuses:
        return "INVALID_PUBLICATION_AUDIT"
    return "REJECTED_OR_MIXED"


def _sanitize_bundle(bundle: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    bars = bundle["bars"]
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    source_rank = {"RECONCILED": 0, "DERIVED": 1}

    def rank(bar: dict[str, Any]) -> tuple[int, str, str, str]:
        return (
            -source_rank.get(str(bar.get("source_kind") or ""), 2),
            str(bar.get("system_observed_at") or ""),
            str(bar.get("created_at") or ""),
            str(bar.get("revision_id") or ""),
        )

    for bar in bars:
        key = (str(bar["security_id"]), str(bar["interval"]), str(bar["bar_start"]))
        current = by_key.get(key)
        if current is None or rank(bar) > rank(current):
            by_key[key] = bar
    sanitized = json.loads(json.dumps(bundle))
    sanitized["bars"] = sorted(by_key.values(), key=lambda bar: (bar["security_id"], bar["interval"], bar["bar_start"]))
    sanitized["bars_sha256"] = digest(sanitized["bars"])
    return sanitized, {
        "original_bar_rows": len(bars),
        "deduplicated_bar_rows": len(sanitized["bars"]),
        "dropped_rows": len(bars) - len(sanitized["bars"]),
    }


def _run_single_study(*, bundle: dict[str, Any], config: dict[str, Any], evaluation_plan: dict[str, Any], out_dir: Path, workers: int) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    study_bundle = json.loads(json.dumps(bundle))
    study_bundle["config_sha256"] = digest(config)
    validation = validate_inputs(study_bundle, config)
    observations, rank_coverage = build_observations(study_bundle, config, workers)
    from research.stock_idea_engine import PublicationLedger

    ledger = PublicationLedger(out_dir / "replay.sqlite")
    publications, outcomes = replay_observations(study_bundle, config, observations, ledger)
    evaluation = evaluate_publications(publications, config, evaluation_plan, study_bundle["source_cutoff"])
    summary = summarize(publications, outcomes, config, evaluation_plan)
    summary["status"] = _study_status(evaluation)
    summary["evaluation_plan_sha256"] = digest(evaluation_plan)
    summary["config_sha256"] = digest(config)
    summary["inputs_sha256"] = digest(study_bundle)

    write_once(out_dir / "manifest.json", {
        "config": config,
        "evaluation_plan": evaluation_plan,
        "config_sha256": digest(config),
        "inputs_sha256": digest(study_bundle),
        "source_cutoff": study_bundle["source_cutoff"],
    })
    write_once(out_dir / "validation.json", validation)
    write_once(out_dir / "observations.json", observations)
    write_once(out_dir / "daily_rank_coverage.json", rank_coverage)
    write_once(out_dir / "publications.json", publications)
    write_once(out_dir / "candidate_outcomes.json", outcomes)
    write_once(out_dir / "evaluation.json", evaluation)
    write_once(out_dir / "summary.json", summary)
    return {
        "status": summary["status"],
        "selected_windows": len(publication_windows(config)),
        "publication_audit_valid": evaluation["publication_audit"]["valid"],
        "study_artifacts": {
            name: {"path": str(out_dir / name), "sha256": _sha256_file(out_dir / name)}
            for name in (
                "manifest.json",
                "validation.json",
                "observations.json",
                "daily_rank_coverage.json",
                "publications.json",
                "candidate_outcomes.json",
                "evaluation.json",
                "summary.json",
            )
        },
    }


def run(*, artifact_root: Path, supersedes: str, env_file: Path, expected_database: str, workers: int, max_sessions: int) -> dict[str, Any]:
    workers = max(1, min(int(workers), 8))
    load_dotenv(env_file)
    base_config = json.loads((ROOT / "backend/research/inputs/stock_idea_pilot_config.json").read_text(encoding="utf-8"))
    evaluation_plan = json.loads((ROOT / "backend/research/inputs/stock_idea_evaluation_plan.json").read_text(encoding="utf-8"))
    validate_plan(evaluation_plan, base_config)

    inventory = inventory_modes(expected_database=expected_database)
    write_once(artifact_root / "availability-mode-inventory.json", inventory)
    mode_window_30m = _best_mode_window(inventory, "30m")
    mode_window_5m = _best_mode_window(inventory, "5m")
    if mode_window_30m is None:
        raise RuntimeError("no 30m retained bars found across any availability mode")

    mode_30m, start_30m, end_30m = mode_window_30m
    start_30m, end_30m = _clip_window(start_30m, end_30m, max_sessions, base_config["calendar"])
    lane_windows = {
        "controls_r2_b3_f2": {"interval": "30m", "availability_mode": mode_30m, "start": start_30m, "end": end_30m},
        "sw3_r_b_f": {"interval": "30m", "availability_mode": mode_30m, "start": start_30m, "end": end_30m},
        "d1_d2": None if mode_window_5m is None else {
            "interval": "5m",
            "availability_mode": mode_window_5m[0],
            "start": _clip_window(mode_window_5m[1], mode_window_5m[2], max_sessions, base_config["calendar"])[0],
            "end": _clip_window(mode_window_5m[1], mode_window_5m[2], max_sessions, base_config["calendar"])[1],
        },
    }
    d1_d2_readiness = _d1_d2_input_readiness(inventory, mode_window_5m)

    source_cutoff = inventory["transaction"]["cutoff"]
    control_config = _build_config_for_window(base_config, start_30m, end_30m)
    bundle = freeze_inputs(control_config, source_cutoff, requested_cohort=None, evaluation_plan=evaluation_plan)
    bundle, sanitation = _sanitize_bundle(bundle)
    bundle["sanitation"] = sanitation
    write_once(artifact_root / "controls-input-bundle.json", bundle)

    results = []
    studies_root = artifact_root / "studies"
    for lane, variant_name, variant_config in lane_variants(control_config):
        variant_id = variant_config.get("variant_policy_version", variant_name)
        study_dir = studies_root / lane / variant_name / variant_id
        study = _run_single_study(
            bundle=bundle,
            config=variant_config,
            evaluation_plan=evaluation_plan,
            out_dir=study_dir,
            workers=workers,
        )
        results.append({
            "lane": lane,
            "variant": variant_name,
            "variant_policy_version": variant_id,
            "policy_version": variant_config["policy_version"],
            "window": lane_windows[lane],
            **study,
        })

    blockers = []
    unexecuted = []
    if d1_d2_readiness["status"] == "BLOCKED_INPUT_READINESS":
        blockers.append({"lane": "d1_d2", **d1_d2_readiness})
    else:
        unexecuted.append(
            {
                "lane": "d1_d2",
                "status": "NOT_EXECUTED_ENGINE_CONTRACT",
                "reason": "LEGACY_REPLAY_V1_ACCEPTS_ONLY_30M_NATIVE_BARS",
                "input_readiness": d1_d2_readiness,
            }
        )
    blockers.extend([
        {"lane": "sw1", "status": "BLOCKED_INPUT_READINESS", "reason": "EARNINGS_EVENT_COVERAGE_ABSENT"},
        {"lane": "p1_p2", "status": "BLOCKED_INPUT_READINESS", "reason": "PIT_FUNDAMENTALS_ABSENT"},
    ])

    final = {
        "run_version": "legacy_autonomous_backtest_run_v2",
        "generated_at": _now(),
        "artifact_root": str(artifact_root),
        "supersedes_readiness_only_run": supersedes,
        "safety": {
            "database_name": inventory["transaction"]["database_name"],
            "transaction_read_only": inventory["transaction"]["transaction_read_only"],
            "provider_calls": False,
            "database_writes": False,
            "services_or_v2_mutations": False,
            "workers": workers,
            "max_sessions_per_lane_window": max_sessions,
        },
        "lane_windows": lane_windows,
        "d1_d2_input_readiness": d1_d2_readiness,
        "study_results": results,
        "blocked_lanes": blockers,
        "unexecuted_lanes": unexecuted,
        "overall_status": "PARTIAL_EXECUTION",
    }
    write_once(artifact_root / "final-qualification-and-v2-promotion-report.json", final)
    return final


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--supersedes-run", required=True)
    parser.add_argument("--env-file", type=Path, default=Path(r"C:\Users\vachi\source\repo\AlphaResearchPro\backend\.env"))
    parser.add_argument("--expected-database", default="stocks_db_fresh")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-sessions", type=int, default=70)
    args = parser.parse_args()
    report = run(
        artifact_root=args.artifact_root,
        supersedes=args.supersedes_run,
        env_file=args.env_file,
        expected_database=args.expected_database,
        workers=args.workers,
        max_sessions=args.max_sessions,
    )
    print(
        json.dumps(
            {
                "artifact_root": report["artifact_root"],
                "study_count": len(report["study_results"]),
                "blocked_lane_count": len(report["blocked_lanes"]),
                "final_report": str(Path(report["artifact_root"]) / "final-qualification-and-v2-promotion-report.json"),
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
