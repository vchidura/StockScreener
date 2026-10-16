#!/usr/bin/env python3
"""Continuously catch up authorized V2 Developer windows for one XNYS session."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, verify_pool_target_v2  # noqa: E402
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.orchestration_repository import (  # noqa: E402
    EQUITY_WINDOW_BUILD_ID, equity_window_recovery_cursor,
)
from realtime.session_windows import developer_session_windows  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.run_realtime_v2_developer_window import main as run_window  # noqa: E402

REQUESTS_PER_WINDOW = 18
RETRY_SECONDS = 300


def _service_identity(args):
    return {
        "database": args.database, "cohort_id": str(args.cohort_id),
        "authorized_session_date": args.authorized_session_date.isoformat(),
        "first_authorized_cutoff": args.first_authorized_cutoff.isoformat(),
        "daily_request_ceiling": args.daily_request_ceiling,
        "authorization_reference": args.authorization_reference,
        "input_build_id": EQUITY_WINDOW_BUILD_ID,
        "requests_per_window": REQUESTS_PER_WINDOW,
        "cutoffs": [window.cutoff.isoformat() for window in args.windows],
    }


def _authority_prefix(args):
    identity = canonical_sha256({
        "cohort_id": args.cohort_id, "session": args.authorized_session_date,
        "authorization_reference": args.authorization_reference,
    })
    return f"continuous-equity-authority:{identity}:"


@contextmanager
def _leadership(dsn, cohort_id, session_date):
    pool = SimpleConnectionPool(1, 1, dsn=dsn, options="-c timezone=UTC")
    try:
        verify_pool_target_v2(
            pool, V2AccessConfig(security_mode="hardened", role_prefix=EXPECTED_V2_DATABASE_NAME),
            APP_ROLE,
        )
        connection = pool.getconn()
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SET ROLE stocks_realtime_v2_app")
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s,0))",
                (f"v2-continuous-equity:{cohort_id}:{session_date.isoformat()}",),
            )
            acquired = bool(cursor.fetchone()[0])
        yield acquired
    finally:
        pool.closeall()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--authorized-session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--first-authorized-cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--daily-request-ceiling", type=int, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--report-directory", type=Path, required=True)
    parser.add_argument("--summary-report", type=Path, required=True)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    args.authorization_reference = args.authorization_reference.strip()
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("continuous ingestion targets only stocks_realtime_v2")
    if args.daily_request_ceiling <= 0 or args.daily_request_ceiling % REQUESTS_PER_WINDOW:
        parser.error("--daily-request-ceiling must be a positive multiple of 18")
    if (
        args.first_authorized_cutoff.tzinfo is None
        or args.first_authorized_cutoff.utcoffset() is None
    ):
        parser.error("--first-authorized-cutoff must be timezone-aware")
    args.first_authorized_cutoff = args.first_authorized_cutoff.astimezone(timezone.utc)
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    if args.execute and not args.authorization_reference.strip():
        parser.error("--execute requires --authorization-reference")
    all_windows = developer_session_windows(
        args.authorized_session_date,
        completion_budget_seconds=300,
        serving_lag_seconds=30,
    )
    args.session_open = all_windows[0].cutoff - timedelta(minutes=15)
    maximum_windows = args.daily_request_ceiling // REQUESTS_PER_WINDOW
    if maximum_windows > len(all_windows):
        parser.error("daily request ceiling exceeds one XNYS session")
    args.windows = tuple(
        window for window in all_windows
        if window.cutoff.astimezone(timezone.utc) >= args.first_authorized_cutoff
    )
    if not args.windows or args.windows[0].cutoff.astimezone(timezone.utc) != args.first_authorized_cutoff:
        parser.error("first authorized cutoff is not a Developer window in the selected session")
    if args.execute and args.summary_report.exists():
        try:
            if args.summary_report.stat().st_size > 1024 * 1024:
                parser.error("existing recovery status exceeds its verification bound")
            previous = json.loads(args.summary_report.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            parser.error("existing recovery status is unreadable; choose a new status path")
        if (
            not isinstance(previous, dict) or previous.get("mode") != "EXECUTE"
            or any(previous.get(key) != value for key, value in _service_identity(args).items())
        ):
            parser.error("existing recovery status belongs to a different or unverifiable scope")
    return args


def _window_recovery_state(dsn, cohort_id, cutoff):
    with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL ROLE stocks_realtime_v2_app")
        return equity_window_recovery_cursor(cursor, cohort_id, cutoff)


def latest_due_window(windows, now):
    return max(
        (window for window in windows if window.available_after <= now),
        key=lambda window: window.cutoff,
        default=None,
    )


def _charged_window_attempts(dsn, args):
    with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL ROLE stocks_realtime_v2_app")
        cursor.execute(
            "SELECT plan.scope_reference, "
            "(window_run.window_key !~ '-receipt-retry[0-9]+$') FROM rt.window_runs window_run "
            "JOIN rt.service_plans plan USING (plan_id) "
            "WHERE window_run.cohort_id=%s AND (window_run.cutoff AT TIME ZONE 'UTC')::date=%s "
            "AND (starts_with(plan.scope_reference,%s) OR starts_with(plan.scope_reference,%s))",
            (
                str(args.cohort_id), args.authorized_session_date,
                _authority_prefix(args),
                args.authorization_reference + ":",
            ),
        )
        rows = tuple(cursor.fetchall())
        expected = _authority_prefix(args) + canonical_sha256(_service_identity(args)) + ":"
        if any(not row[0].startswith(expected) for row in rows):
            raise RuntimeError("provider approval scope changed or lacks frozen budget evidence")
        return sum(bool(row[1]) for row in rows) * REQUESTS_PER_WINDOW


def _run_target(args, cutoff, recovery):
    suffix = recovery["retry_suffix"]
    report_path = args.report_directory / f"window-{cutoff:%H%M}{suffix}.json"
    if report_path.exists():
        raise RuntimeError("recovery report already exists; refusing to overwrite evidence")
    command = [
        "--database", args.database, "--confirm-database-name", args.confirm_database_name,
        "--cohort-id", str(args.cohort_id), "--window-start", args.session_open.isoformat(),
        "--cutoff", cutoff.isoformat(),
        "--cache-directory", str(args.cache_root / f"window-{cutoff:%H%M}"),
        "--report-path", str(report_path), "--authorization-reference",
        _authority_prefix(args) + canonical_sha256(_service_identity(args)) + ":" + cutoff.isoformat(),
        "--wait-for-input-receipt", "--execute",
    ]
    if suffix:
        command.append(f"--window-key-suffix={suffix}")
    if recovery["has_input_receipt"]:
        command.append("--reuse-current-input")
    code = run_window(command)
    if code != 0 or not report_path.is_file():
        raise RuntimeError(f"continuous ingestion window failed: {cutoff.isoformat()}")
    return json.loads(report_path.read_text(encoding="utf-8"))


def _run_authorized(args, dsn, plan):
    stop = False

    def request_stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    args.cache_root.mkdir(parents=True, exist_ok=True)
    args.report_directory.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc)
    reports = []
    provider_requests = 0
    provider_requests_unknown = False
    retry_not_before = {}
    while not stop:
        now = datetime.now(timezone.utc)
        window = latest_due_window(args.windows, now)
        state = "WAITING_FOR_NEXT_WINDOW"
        if window is not None:
            cutoff = window.cutoff.astimezone(timezone.utc)
            recovery = _window_recovery_state(
                dsn, args.cohort_id, cutoff)
            waits = [
                value for value in (
                    retry_not_before.get(cutoff), recovery["retry_not_before"]
                ) if value is not None
            ]
            if recovery["complete"]:
                state = (
                    "SESSION_COMPLETE" if window == args.windows[-1]
                    else "WAITING_FOR_NEXT_WINDOW"
                )
            elif waits and now < max(waits):
                state = "WAITING_FOR_RETRY"
            else:
                state = "ATTEMPTING_WINDOW"
        if state == "ATTEMPTING_WINDOW":
            provider_window = not recovery["has_input_receipt"]
            if (
                provider_window
                and _charged_window_attempts(dsn, args) + REQUESTS_PER_WINDOW
                > args.daily_request_ceiling
            ):
                raise RuntimeError("continuous ingestion reached its daily request ceiling")
            try:
                report = _run_target(args, cutoff, recovery)
            except (RuntimeError, ValueError, psycopg2.Error) as exc:
                retry_not_before[cutoff] = datetime.now(timezone.utc) + timedelta(seconds=RETRY_SECONDS)
                provider_requests_unknown = provider_requests_unknown or provider_window
                failure = {
                    "state": "WINDOW_FAILED", "cutoff": cutoff.isoformat(),
                    "failure_type": type(exc).__name__,
                    "retry_not_before": retry_not_before[cutoff].isoformat(),
                }
                write_report(args.summary_report.resolve(), {
                    **plan, "status": "WINDOW_FAILED", "failure": failure,
                    "provider_requests": None if provider_requests_unknown else provider_requests,
                    "completed_report_provider_requests": provider_requests,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                print(json.dumps(failure, sort_keys=True), flush=True)
                time.sleep(args.poll_seconds)
                continue
            provider_requests += int(report["provider_requests"])
            retry_not_before.pop(cutoff, None)
            reports.append(report)
            summary = {
                **plan,
                "status": "RUNNING",
                "provider_requests": None if provider_requests_unknown else provider_requests,
                "completed_report_provider_requests": provider_requests,
                "windows_completed_by_process": len(reports),
                "windows": reports,
                "started_at": started_at.isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            write_report(args.summary_report.resolve(), summary)
            print(json.dumps({
                "state": "WINDOW_COMPLETE",
                "cutoff": cutoff.isoformat(),
                "provider_requests": None if provider_requests_unknown else provider_requests,
            }, sort_keys=True), flush=True)
            continue
        print(json.dumps({
            "state": state,
            "checked_at": now.isoformat(),
            "provider_requests": None if provider_requests_unknown else provider_requests,
        }, sort_keys=True), flush=True)
        time.sleep(args.poll_seconds)
    completed_at = datetime.now(timezone.utc)
    summary = {
        **plan,
        "status": "STOPPED",
        "provider_requests": None if provider_requests_unknown else provider_requests,
        "completed_report_provider_requests": provider_requests,
        "windows_completed_by_process": len(reports),
        "windows": reports,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
    }
    write_report(args.summary_report.resolve(), summary)
    return 0


def main(argv=None):
    args = parse_args(argv)
    plan = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        **_service_identity(args),
        "maximum_windows": args.daily_request_ceiling // REQUESTS_PER_WINDOW,
    }
    if not args.execute:
        print(json.dumps({**plan, "provider_requests": 0, "database_writes": 0}, sort_keys=True))
        return 0
    load_dotenv(BACKEND_DIR / ".env", override=True)
    dsn = os.getenv("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("continuous ingestion requires REALTIME_V2_APP_DSN")
    with _leadership(dsn, args.cohort_id, args.authorized_session_date) as acquired:
        if not acquired:
            print(json.dumps({"state": "STANDBY", "reason": "LEADERSHIP_HELD"}), flush=True)
            return 0
        return _run_authorized(args, dsn, plan)


if __name__ == "__main__":
    raise SystemExit(main())
