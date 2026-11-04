#!/usr/bin/env python3
"""Plan or re-run the windows a closed Advanced session never completed.

Original windows stay as evidence. Each re-run is a new retry identity released now,
so no clock is backdated. The scheduler stops retrying at the drain deadline, which
leaves a repaired session with no owner for these windows.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
)
from realtime.advanced_realtime_v3_runtime import (  # noqa: E402
    ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS,
    ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
)
from realtime.advanced_scheduler import (  # noqa: E402
    discover_advanced_window_recovery_cursor,
    ensure_advanced_scheduler_session_cursor,
    ensure_advanced_window_retry_cursor,
    load_continuous_advanced_scheduler_scope_cursor,
)
from realtime.advanced_session_recovery import (  # noqa: E402
    plan_session_windows,
)
from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.run_realtime_v2_advanced_equity_scheduler import (  # noqa: E402
    CONTINUOUS_PUBLICATION,
    _window_arguments,
)
from scripts.run_realtime_v2_advanced_equity_window import (  # noqa: E402
    _provider,
    main as run_window,
)


UTC = timezone.utc
MAXIMUM_ATTEMPTS_PER_WINDOW = 2


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--report-directory", type=Path, required=True)
    parser.add_argument("--maximum-windows", type=int, default=78)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced window recovery targets stocks_realtime_v2")
    if not 1 <= args.maximum_windows <= 78:
        parser.error("--maximum-windows must be in [1, 78]")
    args.report_directory = args.report_directory.resolve()
    return args


def session_is_closed(session_date, *, calendar, checked_at, drain_seconds):
    closed = calendar.session_close(session_date)
    return checked_at >= closed + timedelta(seconds=drain_seconds)


def _serialized(rows):
    return [
        {**row, "cutoff": row["cutoff"].isoformat()}
        for row in rows
    ]


def _run_cutoff(provider, window_args, scope, session_date, row, *, session_id):
    outcomes = []
    for offset in range(MAXIMUM_ATTEMPTS_PER_WINDOW):
        attempt = row["next_attempt"] + offset
        now = datetime.now(UTC)
        with provider.transaction(APP_ROLE) as unit:
            recovery = discover_advanced_window_recovery_cursor(
                unit.cursor,
                cohort_id=scope["cohort_id"],
                cutoff=row["cutoff"],
                checked_at=now,
            )
            retry = ensure_advanced_window_retry_cursor(
                unit.cursor,
                session_id,
                cohort_id=scope["cohort_id"],
                cutoff=row["cutoff"],
                attempt=attempt,
                retry_not_before=now,
                input_receipt=recovery.input_receipt,
                portal_receipt=recovery.portal_receipt,
            )
        try:
            exit_code = run_window(
                _window_arguments(
                    window_args,
                    scope,
                    session_date=session_date,
                    cutoff=row["cutoff"],
                    retry=retry,
                )
            )
        except Exception as error:
            exit_code = f"{type(error).__name__}:{str(error)[:200]}"
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            complete = discover_advanced_window_recovery_cursor(
                unit.cursor,
                cohort_id=scope["cohort_id"],
                cutoff=row["cutoff"],
                checked_at=datetime.now(UTC),
            ).complete
        outcomes.append({
            "attempt": attempt,
            "exit": exit_code,
            "complete": complete,
        })
        if complete:
            break
    return outcomes


def main(argv=None) -> int:
    args = parse_args(argv)
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
    try:
        checked_at = datetime.now(UTC)
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            scope = load_continuous_advanced_scheduler_scope_cursor(
                unit.cursor,
                session_date=args.session_date,
            )
        if scope is None:
            raise ValueError("session has no durable scheduler scope")
        settings = catalog.profile_for_sha(
            scope["runtime_profile_sha256"]
        ).settings
        if not session_is_closed(
            args.session_date,
            calendar=calendar,
            checked_at=checked_at,
            drain_seconds=settings.drain_deadline_seconds,
        ):
            raise ValueError("window recovery requires a closed session")
        rows = plan_session_windows(
            provider,
            scope,
            args.session_date,
            checked_at=checked_at,
        )
        report = {
            "mode": "PLAN" if args.plan else "EXECUTE",
            "checked_at": checked_at.isoformat(),
            "session_date": args.session_date.isoformat(),
            "cohort_id": str(scope["cohort_id"]),
            "incomplete": _serialized(rows),
        }
        exit_code = 0
        if args.execute:
            args.report_directory.mkdir(parents=True, exist_ok=True)
            window_args = SimpleNamespace(
                database=args.database,
                confirm_database_name=args.confirm_database_name,
                finality_delay_seconds=(
                    ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS
                ),
                completion_budget_seconds=(
                    ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS
                ),
                authorization_reference=None,
                report_directory=args.report_directory,
                publication_mode=CONTINUOUS_PUBLICATION,
            )
            with provider.transaction(APP_ROLE) as unit:
                session = ensure_advanced_scheduler_session_cursor(
                    unit.cursor,
                    authorization_id=scope["authorization_id"],
                    ingress_plan_id=scope["plan_id"],
                    cohort_id=scope["cohort_id"],
                    session_date=args.session_date,
                    first_authorized_cutoff=rows[0]["cutoff"] if rows else (
                        calendar.session_close(args.session_date)
                    ),
                    session_close_at=calendar.session_close(args.session_date),
                    drain_deadline=checked_at,
                    checked_at=checked_at,
                )
            results = []
            for row in [row for row in rows if row["ready"]][
                :args.maximum_windows
            ]:
                outcomes = _run_cutoff(
                    provider,
                    window_args,
                    scope,
                    args.session_date,
                    row,
                    session_id=session.scheduler_session_id,
                )
                results.append({
                    "cutoff": row["cutoff"].isoformat(),
                    "outcomes": outcomes,
                })
                print(json.dumps(results[-1], sort_keys=True), flush=True)
            report["results"] = results
            remaining = plan_session_windows(
                provider,
                scope,
                args.session_date,
                checked_at=datetime.now(UTC),
            )
            report["remaining"] = _serialized(remaining)
            exit_code = 0 if not remaining else 2
        write_report(
            args.report_directory
            / f"session-window-recovery-{args.session_date}-"
            f"{checked_at:%Y%m%dT%H%M%S}.json",
            report,
        )
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return exit_code
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
