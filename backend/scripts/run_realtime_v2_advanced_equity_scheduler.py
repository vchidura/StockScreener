#!/usr/bin/env python3
"""Run due provider-free Advanced five-minute V2 windows for one session."""
from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
)
from realtime.advanced_scheduler import (  # noqa: E402
    audit_completed_session_coverage_cursor,
    discover_advanced_window_recovery_cursor,
    ensure_advanced_scheduler_session_cursor,
    ensure_advanced_window_retry_cursor,
    load_advanced_scheduler_scope_cursor,
    order_advanced_due_windows,
    persist_advanced_scheduler_lifecycle_cursor,
    prioritize_live_over_blocked_backfill_cursor,
    set_advanced_window_retry_status_cursor,
)
from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.session_windows import advanced_session_windows  # noqa: E402
from scripts.run_realtime_v2_advanced_equity_window import (  # noqa: E402
    _provider,
    main as run_window,
)


PROJECTION_ONLY = "projection-only"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--authorization-id", type=UUID, required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--first-cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--finality-delay-seconds", type=int, required=True)
    parser.add_argument("--completion-budget-seconds", type=int, default=300)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument(
        "--publication-mode",
        choices=(PROJECTION_ONLY,),
        required=True,
    )
    parser.add_argument("--report-directory", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=1)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if (
        args.first_cutoff.tzinfo is None
        or args.first_cutoff.utcoffset() is None
    ):
        parser.error("--first-cutoff must be timezone-aware")
    args.first_cutoff = args.first_cutoff.astimezone(timezone.utc)
    if not 1 <= args.finality_delay_seconds <= 60:
        parser.error("--finality-delay-seconds must be in [1, 60]")
    if not 1 <= args.completion_budget_seconds <= 300:
        parser.error("--completion-budget-seconds must be in [1, 300]")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    if not args.continuous or not args.execute:
        parser.error("Advanced scheduler requires --continuous --execute")
    args.report_directory = args.report_directory.resolve()
    windows = advanced_session_windows(
        args.session_date,
        finality_delay_seconds=args.finality_delay_seconds,
        completion_budget_seconds=args.completion_budget_seconds,
        serving_lag_seconds=30,
    )
    args.windows = tuple(
        window
        for window in windows
        if window.cutoff.astimezone(timezone.utc) >= args.first_cutoff
    )
    if (
        not args.windows
        or args.windows[0].cutoff.astimezone(timezone.utc)
        != args.first_cutoff
    ):
        parser.error("--first-cutoff is not an Advanced five-minute boundary")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    args.report_directory.mkdir(parents=True, exist_ok=True)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.SCHEDULER,
    )
    catalog = load_advanced_ingress_runtime_catalog(
        config.advanced_ingress_policy_path,
        expected_version=config.advanced_ingress_policy_version,
    )
    provider, pool = _provider(config)
    calendar = OptionExchangeCalendar("XNYS")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    session_date = args.session_date
    first_cutoff = args.first_cutoff
    initial_session = True
    audited_sessions: set[UUID] = set()
    try:
        while not stop.is_set():
            now = datetime.now(timezone.utc)
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                scope = load_advanced_scheduler_scope_cursor(
                    unit.cursor,
                    authorization_id=args.authorization_id,
                    session_date=session_date,
                )
            if scope is None:
                stop.wait(args.poll_seconds)
                continue
            if initial_session and scope["cohort_id"] != args.cohort_id:
                raise ValueError("initial scheduler cohort differs from plan")
            runtime_profile = catalog.profile_for_sha(
                scope["runtime_profile_sha256"]
            )
            session_close = calendar.session_close(session_date)
            drain_deadline = session_close + timedelta(
                seconds=runtime_profile.settings.drain_deadline_seconds
            )
            all_windows = advanced_session_windows(
                session_date,
                finality_delay_seconds=args.finality_delay_seconds,
                completion_budget_seconds=args.completion_budget_seconds,
                serving_lag_seconds=30,
            )
            own_first_cutoff = (
                first_cutoff
                if initial_session
                else all_windows[0].cutoff
            )
            windows = tuple(
                window
                for window in all_windows
                if window.cutoff >= own_first_cutoff
            )
            with provider.transaction(APP_ROLE) as unit:
                scheduler_session = ensure_advanced_scheduler_session_cursor(
                    unit.cursor,
                    authorization_id=args.authorization_id,
                    ingress_plan_id=scope["plan_id"],
                    cohort_id=scope["cohort_id"],
                    session_date=session_date,
                    first_authorized_cutoff=own_first_cutoff,
                    session_close_at=session_close,
                    drain_deadline=drain_deadline,
                    checked_at=now,
                )
                prioritize_live_over_blocked_backfill_cursor(
                    unit.cursor,
                    scope["plan_id"],
                    checked_at=now,
                )
            if scheduler_session.scheduler_session_id not in audited_sessions:
                previous = calendar.previous_session_date(session_date)
                with provider.transaction(APP_ROLE, read_only=True) as unit:
                    coverage = audit_completed_session_coverage_cursor(
                        unit.cursor,
                        cohort_id=scope["cohort_id"],
                        sessions=(previous,),
                        checked_at=now,
                    )
                audited_sessions.add(scheduler_session.scheduler_session_id)
                if coverage["status"] != "COMPLETE":
                    with provider.transaction(APP_ROLE) as unit:
                        persist_advanced_scheduler_lifecycle_cursor(
                            unit.cursor,
                            scheduler_session.scheduler_session_id,
                            status="ACTIVE",
                            reason_code="STARTUP_COVERAGE_GAPS",
                            next_action=(
                                "PRIORITIZE_LIVE_KEEP_HISTORY_BLOCKED"
                            ),
                            last_discovered_cutoff=None,
                            checked_at=now,
                        )
            if now > session_close:
                with provider.transaction(APP_ROLE) as unit:
                    persist_advanced_scheduler_lifecycle_cursor(
                        unit.cursor,
                        scheduler_session.scheduler_session_id,
                        status=(
                            "IDLE" if now >= drain_deadline else "DRAINING"
                        ),
                        reason_code=(
                            "SESSION_DRAINED"
                            if now >= drain_deadline
                            else "SESSION_CLOSE_DRAINING"
                        ),
                        next_action=(
                            "WAIT_NEXT_SESSION"
                            if now >= drain_deadline
                            else "DRAIN_BOUNDED_WORK"
                        ),
                        last_discovered_cutoff=session_close,
                        checked_at=now,
                    )
                if now >= drain_deadline:
                    session_date = calendar.next_session_date(session_date)
                    initial_session = False
                stop.wait(args.poll_seconds)
                continue
            due = order_advanced_due_windows(
                window for window in windows
                if window.available_after <= now
            )
            for window in due:
                with provider.transaction(APP_ROLE) as unit:
                    recovery = discover_advanced_window_recovery_cursor(
                        unit.cursor,
                        cohort_id=scope["cohort_id"],
                        cutoff=window.cutoff,
                        checked_at=now,
                        retry_initial_backoff_seconds=(
                            runtime_profile.settings
                            .scheduler_window_retry_initial_backoff_seconds
                        ),
                        retry_max_backoff_seconds=(
                            runtime_profile.settings
                            .scheduler_window_retry_max_backoff_seconds
                        ),
                    )
                    if recovery.complete:
                        continue
                    if now < recovery.retry_not_before:
                        continue
                    if recovery.next_attempt > (
                        runtime_profile.settings
                        .scheduler_window_retry_max_attempts
                    ):
                        persist_advanced_scheduler_lifecycle_cursor(
                            unit.cursor,
                            scheduler_session.scheduler_session_id,
                            status="BLOCKED",
                            reason_code="WINDOW_RETRY_EXHAUSTED",
                            next_action="OPERATOR_REVIEW",
                            last_discovered_cutoff=window.cutoff,
                            checked_at=now,
                        )
                        continue
                    retry = ensure_advanced_window_retry_cursor(
                        unit.cursor,
                        scheduler_session.scheduler_session_id,
                        cohort_id=scope["cohort_id"],
                        cutoff=window.cutoff,
                        attempt=recovery.next_attempt,
                        retry_not_before=recovery.retry_not_before,
                        input_receipt=recovery.input_receipt,
                        portal_receipt=recovery.portal_receipt,
                    )
                    set_advanced_window_retry_status_cursor(
                        unit.cursor,
                        retry.retry_id,
                        status="RUNNING",
                        reason_code="WINDOW_EXECUTION_STARTED",
                        checked_at=now,
                    )
                window_args = [
                    "--database",
                    args.database,
                    "--confirm-database-name",
                    args.confirm_database_name,
                    "--cohort-id",
                    str(scope["cohort_id"]),
                    "--session-date",
                    session_date.isoformat(),
                    "--cutoff",
                    window.cutoff.isoformat(),
                    "--finality-delay-seconds",
                    str(args.finality_delay_seconds),
                    "--completion-budget-seconds",
                    str(args.completion_budget_seconds),
                    "--authorization-reference",
                    args.authorization_reference,
                    "--report-path",
                    str(
                        args.report_directory
                        / retry.report_relative_path
                    ),
                    "--execute",
                ]
                if retry.attempt:
                    window_args.extend((
                        f"--window-key-suffix=-retry{retry.attempt}",
                        "--retry-release-now",
                    ))
                if retry.reused_input_receipt:
                    window_args.append("--reuse-input-receipt")
                if retry.reused_portal_receipt:
                    window_args.append("--reuse-portal-receipt")
                result = run_window(window_args)
                with provider.transaction(APP_ROLE) as unit:
                    after = discover_advanced_window_recovery_cursor(
                        unit.cursor,
                        cohort_id=scope["cohort_id"],
                        cutoff=window.cutoff,
                        checked_at=datetime.now(timezone.utc),
                        retry_initial_backoff_seconds=(
                            runtime_profile.settings
                            .scheduler_window_retry_initial_backoff_seconds
                        ),
                        retry_max_backoff_seconds=(
                            runtime_profile.settings
                            .scheduler_window_retry_max_backoff_seconds
                        ),
                    )
                    set_advanced_window_retry_status_cursor(
                        unit.cursor,
                        retry.retry_id,
                        status="COMPLETE" if after.complete else "FAILED",
                        reason_code=(
                            "DURABLE_WINDOW_COMPLETE"
                            if after.complete
                            else f"WINDOW_EXIT_{result}"
                        ),
                        checked_at=datetime.now(timezone.utc),
                    )
                    persist_advanced_scheduler_lifecycle_cursor(
                        unit.cursor,
                        scheduler_session.scheduler_session_id,
                        status="ACTIVE",
                        reason_code=(
                            "WINDOW_COMPLETE"
                            if after.complete
                            else "WINDOW_FAILED_CONTINUE_SIBLINGS"
                        ),
                        next_action="DISCOVER_DUE_WINDOWS",
                        last_discovered_cutoff=window.cutoff,
                        checked_at=datetime.now(timezone.utc),
                    )
            stop.wait(args.poll_seconds)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
