#!/usr/bin/env python3
"""Qualify the Advanced daily scheduler without provider or production effects."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p8_canary_authorization import (  # noqa: E402
    source_binding_identity,
)
from realtime.advanced_options_v3_deployment import (  # noqa: E402
    ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS,
)
from realtime.advanced_realtime_session_scheduler import (  # noqa: E402
    ADVANCED_SESSION_EQUITY_SYMBOLS,
    ADVANCED_SESSION_MAXIMUM_E_O_SECONDS,
    ADVANCED_SESSION_OPTIONS_ENDPOINT,
    ADVANCED_SESSION_OPTIONS_UNDERLYINGS,
    ADVANCED_SESSION_WINDOW_MINUTES,
    build_advanced_session_windows,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402


FOCUSED_TESTS = (
    "backend/tests/realtime/test_advanced_options_acquisition_runtime.py",
    "backend/tests/realtime/test_advanced_realtime_session_scheduler.py",
    "backend/tests/realtime/test_advanced_realtime_session_operations.py",
    "backend/tests/realtime/test_advanced_options_p8_selection_source.py",
    "backend/tests/realtime/test_advanced_options_read_pin.py",
    "backend/tests/realtime/test_advanced_options_p8_docker_runtime.py",
    "backend/tests/realtime/test_run_advanced_options_v3_production.py",
    "backend/tests/realtime/test_run_advanced_realtime_one_shot.py",
    "backend/tests/realtime/test_realtime_v2_p7_container.py",
    "backend/tests/realtime/test_advanced_options_v3_deployment.py",
    "backend/tests/realtime/test_advanced_options_validation_mode.py",
    "backend/tests/realtime/test_advanced_ingress_controller.py",
    "backend/tests/realtime/test_prepare_realtime_v2_advanced_ingress.py",
    "backend/tests/realtime/test_advanced_equity_ingress.py",
    "backend/tests/realtime/test_advanced_equity_journal_bridge.py",
    "backend/tests/realtime/test_advanced_equity_recovery_faults.py",
    "backend/tests/realtime/test_advanced_journal_registry.py",
    "backend/tests/realtime/test_normalizer.py",
    "backend/tests/realtime/test_normalized_journal.py",
    "backend/tests/realtime/test_equity_interval_finalizer.py",
    "backend/tests/realtime/test_realtime_v2_advanced_controller_script.py",
    (
        "backend/tests/realtime/"
        "test_realtime_v2_advanced_equity_ingress_script.py"
    ),
    (
        "backend/tests/realtime/"
        "test_realtime_v2_advanced_equity_bridge_script.py"
    ),
    (
        "backend/tests/realtime/"
        "test_realtime_v2_advanced_journal_replay_script.py"
    ),
    (
        "backend/tests/realtime/"
        "test_realtime_v2_finalizer_worker_script.py"
    ),
    "backend/tests/realtime/test_equity_bulk_backfill.py",
    "backend/tests/realtime/test_realtime_v2_audit_scripts.py",
    "backend/tests/options/test_model_input_rates.py",
    "backend/tests/test_equity_model_detection.py",
    "backend/tests/realtime/test_canonical_stock_alert_product.py",
    "backend/tests/realtime/test_stage_runners_alerts.py",
    "backend/tests/realtime/test_stock_alert_vertical_slice.py",
    "backend/tests/realtime/test_run_realtime_v2_canonical_stock_alerts.py",
    (
        "backend/tests/realtime/"
        "test_prepare_advanced_options_v3_pre_session_authority.py"
    ),
)
DISPOSABLE_TESTS = (
    "backend/tests/realtime/test_advanced_ingress_plan_v2_db.py",
    "backend/tests/realtime/test_advanced_equity_journal_bridge_v2_db.py",
    "backend/tests/realtime/test_equity_interval_finalizer_v2_db.py",
    "backend/tests/realtime/test_equity_recent_repair_v2_db.py",
    "backend/tests/realtime/test_advanced_options_reference_producer_v2_db.py",
    "backend/tests/realtime/test_advanced_options_v3_authority_v2_db.py",
    "backend/tests/realtime/test_advanced_options_v3_integration_v2_db.py",
    "backend/tests/realtime/test_stock_alert_source_v2_db.py",
)
DENIED_PROVIDER_ENVIRONMENT = (
    "POLYGON_API_KEY",
    "MASSIVE_API_KEY",
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--session-date",
        type=date.fromisoformat,
        required=True,
    )
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.output_directory = args.output_directory.resolve()
    try:
        build_advanced_session_windows(args.session_date)
    except ValueError as exc:
        parser.error(f"--session-date must be an XNYS session: {exc}")
    if args.output_directory.exists() or not args.execute:
        parser.error(
            "qualification requires a new output directory and --execute"
        )
    return args


def _run_tests(paths, *, environment, offline):
    command = [
        sys.executable,
        "-B",
        "-m",
        "pytest",
    ]
    if offline:
        command.extend((
            "-p",
            "backend.tests.realtime.offline_io_guard",
        ))
    command.extend(paths)
    command.extend((
        "-q",
        "--tb=short",
        "-p",
        "no:cacheprovider",
    ))
    if offline:
        command.append("--realtime-no-network")
    completed = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=600,
    )
    output = completed.stdout.decode("utf-8", errors="replace")
    if completed.returncode:
        raise RuntimeError(output[-4000:])
    summary = next(
        (
            line.strip()
            for line in reversed(output.splitlines())
            if " passed" in line
        ),
        None,
    )
    if summary is None:
        raise RuntimeError("pytest completion summary is unavailable")
    return {
        "command_sha256": hashlib.sha256(
            "\0".join(command).encode("utf-8")
        ).hexdigest(),
        "summary": summary,
        "returncode": completed.returncode,
    }


def execute(args):
    environment = dict(os.environ)
    for name in DENIED_PROVIDER_ENVIRONMENT:
        environment.pop(name, None)
    focused = _run_tests(
        FOCUSED_TESTS,
        environment=environment,
        offline=True,
    )
    disposable_environment = dict(environment)
    disposable_environment["STOCKS_V2_DB_TESTS"] = "1"
    disposable = _run_tests(
        DISPOSABLE_TESTS,
        environment=disposable_environment,
        offline=False,
    )
    windows = build_advanced_session_windows(args.session_date)
    calendar = OptionExchangeCalendar("XNYS")
    session_open = calendar.session_open(args.session_date)
    session_close = calendar.session_close(args.session_date)
    expected_windows = int(
        (session_close - session_open).total_seconds()
        // (ADVANCED_SESSION_WINDOW_MINUTES * 60)
    )
    if (
        len(windows) != expected_windows
        or windows[0].equity_cutoff
        != session_open
        + timedelta(minutes=ADVANCED_SESSION_WINDOW_MINUTES)
        or windows[-1].equity_cutoff != session_close
        or windows[-1].latest_options_cutoff
        != session_close
        + timedelta(seconds=ADVANCED_SESSION_MAXIMUM_E_O_SECONDS)
    ):
        raise RuntimeError("qualified scheduler window scope changed")
    report = {
        "contract": (
            "advanced-realtime-session-scheduler-qualification-v1"
        ),
        "verdict": "PASS",
        "session_date": args.session_date.isoformat(),
        "scope": {
            "equity_symbols": list(
                ADVANCED_SESSION_EQUITY_SYMBOLS
            ),
            "options_underlyings": list(
                ADVANCED_SESSION_OPTIONS_UNDERLYINGS
            ),
            "options_contract_count": 4,
            "decision_cycles": len(windows),
            "first_e_utc": windows[0].equity_cutoff.isoformat(),
            "last_e_utc": windows[-1].equity_cutoff.isoformat(),
            "decision_stop_utc": (
                windows[-1].latest_options_cutoff.isoformat()
            ),
            "options_rest_endpoint": (
                ADVANCED_SESSION_OPTIONS_ENDPOINT
            ),
            "options_rest_requests_per_cycle": 1,
            "options_rest_session_request_ceiling": len(windows),
        },
        "checks": {
            "focused_provider_free": focused,
            "owned_disposable": disposable,
        },
        "source": {
            "files": source_binding_identity(
                REPOSITORY_ROOT,
                ADVANCED_OPTIONS_V3_SOURCE_BINDING_PATHS,
            ),
        },
        "effects": {
            "provider_rest": 0,
            "provider_websocket": 0,
            "production_database_writes": 0,
            "production_service_actions": 0,
            "release": 0,
            "pointer": 0,
            "stored_read": 0,
            "host_schedule": 0,
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
    }
    report["source"]["file_count"] = len(report["source"]["files"])
    report["semantic_sha256"] = canonical_sha256(report)
    args.output_directory.mkdir()
    path = args.output_directory / (
        "advanced-realtime-session-scheduler-qualification-"
        f"{report['semantic_sha256'][:16]}.json"
    )
    encoded = (
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    path.write_bytes(encoded)
    return {
        "verdict": report["verdict"],
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "report": str(path),
        "effects": report["effects"],
    }


def main(argv=None) -> int:
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
