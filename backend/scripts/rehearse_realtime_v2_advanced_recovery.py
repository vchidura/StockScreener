#!/usr/bin/env python3
"""Plan or run the provider-free P8 recovery contract rehearsal from a source checkout."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree


BACKEND_DIR = Path(__file__).resolve().parent.parent
ROOT = BACKEND_DIR.parent
TEST_DIR = BACKEND_DIR / "tests" / "realtime"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.realtime_script_support import write_report  # noqa: E402


FAULT_TESTS = "test_advanced_equity_recovery_faults.py"
SCENARIOS = (
    ("io-guard", "Socket/DNS, requests and PostgreSQL connection denial", (
        f"{FAULT_TESTS}::test_rehearsal_guard_blocks_network_and_database",
    )),
    ("transport-recovery", "Injected DNS/TCP/socket/ping-timeout failures and fresh session retry", (
        f"{FAULT_TESTS}::test_transient_transport_fault_preserves_input_and_fresh_session",
    )),
    ("malformed-message", "Raw malformed bytes retained; normalized progress does not advance", (
        f"{FAULT_TESTS}::test_malformed_market_bytes_remain_durable_without_normalized_progress",
    )),
    ("oversized-message", "Synthetic oversized frame rejected with protocol size close", (
        "test_advanced_gateway.py::test_gateway_oversized_message_closes_with_protocol_size_code",
    )),
    ("full-buffer", "Bounded handoff rejects new input without evicting retained bytes", (
        "test_advanced_gateway.py::test_gateway_handoff_overload_disconnects_without_evicting",
    )),
    ("stalled-bridge", "Held buffer claim applies backpressure; release drains retained input", (
        f"{FAULT_TESTS}::test_stalled_bridge_claim_applies_backpressure_without_eviction",
    )),
    ("postgresql-progress-outage", "Injected progress callback outage does not stop SQLite ingestion", (
        "test_advanced_equity_ingress.py::test_journal_pump_survives_postgresql_progress_outage",
    )),
    ("journal-capacity", "Synthetic low disk headroom and SQLITE_FULL preserve admission/claim gates", (
        "test_advanced_journal_registry.py::test_disk_headroom_blocks_when_either_reviewed_threshold_fails",
        f"{FAULT_TESTS}::test_full_journal_releases_claim_and_replays_without_loss",
    )),
    ("journal-corrupt", "Invalid local SQLite file is quarantined without replacement", (
        f"{FAULT_TESTS}::test_corrupt_registered_journal_is_quarantined_without_replacement",
    )),
    ("journal-missing", "Missing registered local journal fails explicitly", (
        "test_advanced_journal_registry.py::test_missing_registered_journal_is_explicit_quarantine_evidence",
    )),
    ("journal-process-crash", "Local child-process crash preserves committed and rejects uncommitted bytes", (
        "test_journal.py::test_acknowledged_commit_survives_process_crash",
        "test_journal.py::test_uncommitted_process_crash_is_not_acknowledged",
        "test_normalized_journal.py::test_normalized_commit_survives_process_crash",
    )),
    ("gap-isolation", "Fake repository unresolved gap blocks bar/checkpoint advancement", (
        "test_advanced_equity_journal_bridge.py::test_unresolved_relevant_gap_prevents_bar_and_checkpoint_advancement",
        "test_realtime_v2_advanced_journal_replay_script.py::test_replay_isolates_quarantine_gap_and_infrastructure_failures",
    )),
    ("scheduler-recovery", "Fake durable state controls retry identity and latest-slot priority", (
        "test_advanced_scheduler.py::test_database_state_not_report_files_controls_completion_and_retry",
        "test_advanced_scheduler.py::test_complete_durable_window_is_never_retried",
        "test_advanced_scheduler.py::test_retry_identity_derives_report_path_and_receipt_reuse",
        "test_advanced_scheduler.py::test_latest_slot_runs_first_and_oldest_retry_is_bounded_second",
    )),
    ("authorization-bounds", "Pure plan target/budget limits and bounded retry classification", (
        "test_advanced_ingress_plan.py::test_daily_plan_cannot_widen_target_or_budget",
        "test_advanced_ingress_controller.py::test_entitlement_configuration_gap_and_budget_failures_are_terminal",
        "test_advanced_ingress_controller.py::test_full_jitter_exponential_backoff_is_bounded",
    )),
    ("bounded-canary-recovery", "Hashed intraday deadline, reserved shutdown, restart counters and eligible cutoff contracts", (
        "test_advanced_ingress_plan.py::test_canary_bounds_are_hashed_and_restart_cannot_extend_them",
        "test_advanced_ingress_plan.py::test_canary_rejects_overlong_outside_empty_naive_or_partial_window",
        "test_advanced_ingress_plan.py::test_canary_rejects_expansion_of_the_approved_envelope",
        "test_advanced_ingress_plan.py::test_canary_request_normalizes_timezones_and_keeps_daily_deadline_behavior",
        "test_realtime_v2_advanced_controller_script.py::test_controller_pins_canary_window_from_cli_or_container_environment",
        "test_realtime_v2_advanced_equity_ingress_script.py::test_expired_persisted_deadline_never_starts_a_gateway_even_after_restart",
        "test_realtime_v2_advanced_equity_ingress_script.py::test_monotonic_deadline_stops_provider_when_wall_clock_stalls",
        "test_realtime_v2_p7_container.py::test_ingress_service_can_bootstrap_a_ready_plan_without_resetting_identity",
        "test_realtime_v2_p7_container.py::test_ingress_supervisor_enforces_absolute_deadline_without_a_stop_signal",
        "test_advanced_scheduler.py::test_scheduler_reloads_persisted_window_instead_of_resetting_its_duration",
        "test_realtime_v2_advanced_equity_scheduler_script.py::test_scheduler_drains_final_cutoff_after_provider_window_ends",
    )),
    ("date-scoped-reference", "New reference date requires exact explicit approval; plan mode performs zero I/O", (
        "test_refresh_realtime_v2_advanced_spy_reference.py::test_new_reference_session_requires_exact_explicit_authority",
    )),
)
NOT_QUALIFIED = (
    "Live provider entitlement, WebSocket faults and real ping/pong timing",
    "Docker SIGKILL/restarts, host reboot and loaded SIGTERM drain",
    "Real PostgreSQL outages, stale-owner fencing and cumulative counters across restarts",
    "Target-1/target-9 throughput, worker counts, lane deadlines and 1.3x headroom",
    "Market-hours soak and next-session autonomous operation",
    "Advanced event-driven alerts, outbox delivery, publication and platform promotion",
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.execute and args.report_path is None:
        parser.error("--execute requires --report-path")
    if args.report_path is not None:
        args.report_path = args.report_path.resolve()
        if args.report_path.exists() or args.report_path.with_name(args.report_path.name + ".tmp").exists():
            parser.error("--report-path and its temporary path must be new")
    return args


def _case_results(path):
    results = {}
    if not path.is_file():
        return results
    for case in ElementTree.parse(path).findall(".//testcase"):
        module = case.get("classname", "").rsplit(".", 1)[-1]
        name = case.get("name", "").split("[", 1)[0]
        state = "PASSED"
        for tag, outcome in (("failure", "FAILED"), ("error", "ERROR"), ("skipped", "SKIPPED")):
            if case.find(tag) is not None:
                state = outcome
                break
        results.setdefault(f"{module}.py::{name}", []).append(state)
    return results


def run_rehearsal():
    nodes = tuple(dict.fromkeys(node for _, _, cases in SCENARIOS for node in cases))
    for node in nodes:
        if not (TEST_DIR / node.split("::", 1)[0]).is_file():
            raise RuntimeError("Offline P8 rehearsal requires the repository test source checkout")
    environment = dict(os.environ)
    environment.update(
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        PYTEST_ADDOPTS="",
        PYTEST_PLUGINS="",
        PYTHONDONTWRITEBYTECODE="1",
    )
    with tempfile.TemporaryDirectory(prefix="advanced-equity-p8-") as temporary:
        junit = Path(temporary) / "results.xml"
        command = [
            sys.executable, "-B", "-m", "pytest",
            "-p", "backend.tests.realtime.offline_io_guard",
            "--realtime-no-network",
            "-q", "--tb=no", "--disable-warnings",
            "-p", "no:cacheprovider",
            f"--junitxml={junit}",
            *(
                str(TEST_DIR / node.split("::", 1)[0]) + "::" + node.split("::", 1)[1]
                for node in nodes
            ),
        ]
        runner_failure = None
        exit_code = None
        try:
            completed = subprocess.run(
                command,
                cwd=ROOT,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=120,
                check=False,
            )
            exit_code = completed.returncode
        except subprocess.TimeoutExpired:
            runner_failure = "REHEARSAL_TIMEOUT"
        if not junit.is_file():
            runner_failure = runner_failure or "JUNIT_REPORT_MISSING"
        try:
            results = _case_results(junit)
        except ElementTree.ParseError:
            runner_failure = runner_failure or "JUNIT_REPORT_INVALID"
            results = {}
    scenarios = []
    for scenario_id, evidence, cases in SCENARIOS:
        tests = [
            {"node": node, "outcomes": results.get(node, ["NOT_COLLECTED"])}
            for node in cases
        ]
        passed = all(test["outcomes"] and set(test["outcomes"]) == {"PASSED"} for test in tests)
        scenarios.append({
            "id": scenario_id,
            "status": "PASS_OFFLINE" if passed else "FAIL_OFFLINE",
            "evidence": evidence,
            "tests": tests,
        })
    passed = exit_code == 0 and runner_failure is None and all(
        scenario["status"] == "PASS_OFFLINE" for scenario in scenarios
    )
    return {
        "contract_version": "advanced-equity-p8-offline-v2",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "status": "PASS_OFFLINE" if passed else "FAIL_OFFLINE",
        "mode": "PROVIDER_FREE_CONTRACT_REHEARSAL",
        "live_qualified": False,
        "production_ready": False,
        "external_network_allowed": False,
        "postgresql_connections_allowed": False,
        "publication_allowed": False,
        "runner_exit_code": exit_code,
        "runner_failure": runner_failure,
        "test_case_count": sum(len(states) for states in results.values()),
        "scenarios": scenarios,
        "not_qualified": list(NOT_QUALIFIED),
    }


def main(argv=None):
    args = parse_args(argv)
    if not args.execute:
        print(json.dumps({
            "status": "PLAN",
            "mode": "PROVIDER_FREE_CONTRACT_REHEARSAL",
            "live_qualified": False,
            "scenarios": [
                {"id": scenario_id, "evidence": evidence, "tests": list(cases)}
                for scenario_id, evidence, cases in SCENARIOS
            ],
            "not_qualified": list(NOT_QUALIFIED),
        }, indent=2))
        return 0
    report = run_rehearsal()
    write_report(args.report_path, report)
    print(json.dumps({
        "status": report["status"],
        "test_case_count": report["test_case_count"],
        "failed_scenarios": [
            row["id"] for row in report["scenarios"]
            if row["status"] != "PASS_OFFLINE"
        ],
        "runner_exit_code": report["runner_exit_code"],
        "runner_failure": report["runner_failure"],
        "live_qualified": False,
        "report_path": str(args.report_path),
    }))
    return 0 if report["status"] == "PASS_OFFLINE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
