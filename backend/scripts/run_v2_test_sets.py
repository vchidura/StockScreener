#!/usr/bin/env python3
"""Run a named V2 test set and print only the result tail.

    python -B backend/scripts/run_v2_test_sets.py fast   # offline, I/O guard plugin (about 7 minutes)
    python -B backend/scripts/run_v2_test_sets.py db     # disposable PostgreSQL, no guard (about 10 minutes)
    python -B backend/scripts/run_v2_test_sets.py slow   # vertical-slice and neighbour set (about 7 minutes)
    python -B backend/scripts/run_v2_test_sets.py fast --list

Only V2-related tests: never the whole backend/tests/realtime folder. After `db`, run
backend/scripts/check_realtime_v2_disposable_leftovers.py (no test_realtime_v2_* database or test_rt_* role may remain).
Known unrelated failure in `slow`: test_all_tickers_vertical_slice[10-None-None] (expects 503, gets 200).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
TESTS = BACKEND_DIR / "tests" / "realtime"

FAST = (
    "test_stage_*", "test_schema_v2_registry", "test_security_v2_roles", "test_normalization_diagnostics_and_access_v2",
    "test_v2_service", "test_session_windows", "test_provision_realtime_v2", "test_option_flow_*",
    "test_session_archive*", "test_portal_*", "test_acquisition_*", "test_materialization_jobs",
    "test_equity_evidence_repository", "test_equity_bar_repository", "test_equity_bar_drain",
    "test_equity_interval_derivation", "test_equity_worker_service", "test_equity_interval_finalizer",
    "test_detector_evidence_repository", "test_canonical_fast_path", "test_lineage_head", "test_contracts",
    "test_pattern_watch_realtime", "test_dashboard_manifest_bounds", "test_stock_alert_manifest_bounds",
    "test_option_discovery_repository", "test_reference_copy_plan", "test_forward_ingestion",
    "test_option_discovery_pages_v2_db", "test_partition_retention*", "test_retention_maintenance",
    "test_realtime_v2_retention_service",
    "test_option_slot_set*",
    "test_shadow_and_tier_cohort_v2_db", "test_shadow_control_plane", "test_option_manifest_repository",
    "test_options_evidence_identity", "test_option_cycle_input*", "test_screening_delta_loader*",
    "test_screening_known_by", "test_ticker_references", "test_ticker_screening_generation",
    "test_ticker_projection_golden", "test_check_scripts_against_v2_registry", "test_realtime_v2_audit_scripts",
    "test_row_value_sharing", "test_rows_by_security", "test_run_v2_test_sets",
    "test_realtime_v2_finalizer_worker_script", "test_advanced_equity_journal_bridge",
    "test_realtime_v2_advanced_equity_bridge_script", "test_advanced_options_p6_harness",
    "test_advanced_options_flow_policy",
)
SLOW = (
    "test_ticker_vertical_slice", "test_ticker_projection_state", "test_realtime_architecture_profile",
    "test_dashboard_vertical_slice", "test_all_tickers_vertical_slice", "test_daily_scanner_vertical_slice",
    "test_stock_alert_vertical_slice", "test_screening_vertical_slice", "test_equity_portal_history",
    "test_equity_screening_history", "test_option_pre_session_*", "test_normalizer", "test_equity_validation_*",
    "test_detector_*",
)
DB = (
    "test_acquisition_evidence_v2_db", "test_normalization_diagnostics_and_access_v2", "test_option_flow_writer",
    "test_provision_realtime_v2", "test_portal_v2_database", "test_session_archive_postgres", "test_stage_orchestrator",
    "test_stage_write_fence", "test_option_discovery_pages_v2_db", "test_reference_copy_plan",
    "test_partition_retention_db", "test_retention_maintenance_db", "test_option_slot_sets_v2_db",
    "test_shadow_and_tier_cohort_v2_db",
    "test_option_cycle_input_v2_db", "test_screening_delta_loader_db", "test_ticker_reference_v2_db",
    "test_realtime_v2_audit_scripts", "test_stage_runners_bind_v2_db", "test_option_flow_runner_v2_db",
    "test_advanced_options_p5_v2_db",
    "test_v2_pipeline_rehearsal_db", "test_equity_interval_finalizer_v2_db",
    "test_advanced_equity_journal_bridge_v2_db",
)
SETS = {"fast": FAST, "slow": SLOW, "db": DB}


def collect(patterns, *, exclude=()) -> list[Path]:
    found = {path for pattern in patterns for path in TESTS.glob(pattern + ".py")}
    skipped = {path for pattern in exclude for path in TESTS.glob(pattern + ".py")}
    return sorted(found - skipped - {path for path in found if "benchmark" in path.name})


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("name", choices=sorted(SETS))
    parser.add_argument("--list", action="store_true", help="print the files and exit")
    parser.add_argument("--tail", type=int, default=12, help="lines of pytest output to print")
    args = parser.parse_args(argv)
    files = collect(SETS[args.name], exclude=FAST if args.name == "slow" else ())
    if args.list:
        print("\n".join(str(path.relative_to(BACKEND_DIR.parent)) for path in files))
        return 0
    environment = dict(os.environ, PYTHONWARNINGS="ignore")
    command = [sys.executable, "-B", "-m", "pytest", "-q", "-W", "ignore"]
    if args.name == "db":
        environment.update(STOCKS_V2_DB_TESTS="1", SESSION_ARCHIVE_POSTGRES_TESTS="1")
    else:
        command[4:4] = ["-p", "backend.tests.realtime.offline_io_guard"]
    completed = subprocess.run(
        [*command, *map(str, files)], cwd=BACKEND_DIR.parent, env=environment, capture_output=True, text=True)
    lines = (completed.stdout + completed.stderr).splitlines()
    print(f"{args.name}: {len(files)} files, exit {completed.returncode}")
    print("\n".join(lines[-args.tail:]))
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
