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
from typing import Callable
from uuid import UUID


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
    audit_completed_session_coverage_cursor,
    discover_advanced_window_recovery_cursor,
    ensure_advanced_scheduler_session_cursor,
    ensure_advanced_window_retry_cursor,
    load_advanced_scheduler_progress_cursor,
    load_advanced_scheduler_scope_cursor,
    load_continuous_advanced_scheduler_scope_cursor,
    order_advanced_due_windows,
    persist_advanced_scheduler_lifecycle_cursor,
    prioritize_live_over_blocked_backfill_cursor,
    set_advanced_window_retry_status_cursor,
)
from realtime.advanced_equity_lifecycle import (  # noqa: E402
    AdvancedEquityLifecyclePhase,
    AdvancedEquityLifecyclePlanner,
)
from realtime.advanced_session_recovery import (  # noqa: E402
    plan_session_windows,
)
from realtime.advanced_ingress_plan import (  # noqa: E402
    advanced_ingress_provider_deadline,
    is_advanced_canary_window,
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
CONTINUOUS_PUBLICATION = "continuous-publication"
COMPLETED_SESSION_RECOVERY_POLL_SECONDS = 60
COMPLETED_SESSION_EXTRA_ATTEMPTS = 4
# B20 (Nov 6): one recovery attempt ran before the missed 5m bars were derived, failed, and nothing tried again.
# Attempts now repeat, spaced, while the close is incomplete and its inputs read ready.
COMPLETED_SESSION_RETRY_SPACING = timedelta(minutes=10)
# A window can take its whole budget, so stop early enough to be idle by the
# activation boundary at open minus 15 minutes.
COMPLETED_SESSION_RECOVERY_LEAD = timedelta(minutes=25)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument(
        "--runtime-mode",
        choices=("continuous", "legacy-diagnostic"),
        default=(
            os.getenv("REALTIME_V2_ADVANCED_RUNTIME_MODE")
            or "continuous"
        ),
    )
    parser.add_argument("--authorization-id", type=UUID)
    parser.add_argument("--cohort-id", type=UUID)
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument("--first-cutoff", type=datetime.fromisoformat)
    parser.add_argument(
        "--finality-delay-seconds",
        type=int,
        default=int(
            os.getenv("REALTIME_V2_ADVANCED_FINALITY_DELAY_SECONDS")
            or ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS
        ),
    )
    parser.add_argument(
        "--completion-budget-seconds",
        type=int,
        default=ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS,
    )
    parser.add_argument("--authorization-reference")
    parser.add_argument(
        "--publication-mode",
        choices=(PROJECTION_ONLY, CONTINUOUS_PUBLICATION),
        default=PROJECTION_ONLY,
    )
    parser.add_argument(
        "--report-directory",
        type=Path,
        default=(
            BACKEND_DIR
            / ".realtime-canary"
            / "v2-advanced-windows"
        ),
    )
    parser.add_argument("--poll-seconds", type=float, default=1)
    parser.add_argument(
        "--scope-reconcile-seconds",
        type=float,
        default=15,
    )
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    explicit = (
        args.authorization_id,
        args.cohort_id,
        args.session_date,
        args.first_cutoff,
        args.authorization_reference,
    )
    if (
        args.publication_mode == CONTINUOUS_PUBLICATION
        and args.runtime_mode != "continuous"
    ):
        parser.error(
            "continuous publication requires the continuous runtime mode"
        )
    if args.runtime_mode == "continuous":
        if any(value is not None for value in explicit):
            parser.error(
                "continuous scheduler derives authorization, cohort, "
                "session, first cutoff and reference; use "
                "--runtime-mode legacy-diagnostic for explicit scope"
            )
    elif any(value is None for value in explicit):
        parser.error(
            "legacy diagnostic scheduler requires authorization, cohort, "
            "session, first cutoff and authorization reference"
        )
    if args.first_cutoff is not None:
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
    if (
        args.poll_seconds <= 0
        or args.scope_reconcile_seconds < args.poll_seconds
    ):
        parser.error("--poll-seconds must be positive")
    if (
        args.authorization_reference is not None
        and not args.authorization_reference.strip()
    ):
        parser.error("--authorization-reference cannot be blank")
    if not args.continuous or not args.execute:
        parser.error("Advanced scheduler requires --continuous --execute")
    args.report_directory = args.report_directory.resolve()
    args.windows = ()
    if args.runtime_mode == "legacy-diagnostic":
        windows = advanced_session_windows(
            args.session_date,
            finality_delay_seconds=args.finality_delay_seconds,
            completion_budget_seconds=args.completion_budget_seconds,
            serving_lag_seconds=30,
        )
        args.windows = tuple(
            window
            for window in windows
            if window.cutoff.astimezone(timezone.utc)
            >= args.first_cutoff
        )
        if (
            not args.windows
            or args.windows[0].cutoff.astimezone(timezone.utc)
            != args.first_cutoff
        ):
            parser.error(
                "--first-cutoff is not an Advanced five-minute boundary"
            )
    return args


def _authorized_windows(windows, *, first_cutoff, started, ended):
    if not started < first_cutoff <= ended:
        raise ValueError("first cutoff is outside the durable ingress window")
    selected = tuple(
        window for window in windows
        if first_cutoff <= window.cutoff <= ended
    )
    if not selected:
        raise ValueError("durable ingress window has no authorized cutoffs")
    return selected


def _window_arguments(
    args,
    scope,
    *,
    session_date,
    cutoff,
    retry,
    completion_budget_seconds=None,
    execution_deadline=None,
):
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
        cutoff.isoformat(),
        "--finality-delay-seconds",
        str(args.finality_delay_seconds),
        "--completion-budget-seconds",
        str(
            args.completion_budget_seconds
            if completion_budget_seconds is None
            else completion_budget_seconds
        ),
        "--authorization-reference",
        scope.get(
            "authorization_reference",
            args.authorization_reference,
        ),
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
    if execution_deadline is not None:
        window_args.extend((
            "--execution-deadline",
            execution_deadline.isoformat(),
        ))
    if retry.reused_input_receipt:
        window_args.append("--reuse-input-receipt")
    if retry.reused_portal_receipt:
        window_args.append("--reuse-portal-receipt")
    if args.publication_mode == CONTINUOUS_PUBLICATION:
        window_args.append("--continuous-publication")
    return window_args


def _recover_completed_session_window(
    provider,
    args,
    catalog,
    calendar,
    *,
    lifecycle,
    now,
    state,
    window_runner=None,
) -> bool:
    """Run one window of the last closed session once its inputs are stored.

    Retries stop at the drain deadline, but a closing window needs the adjusted
    daily bar that only arrives after it, and repaired bars can arrive later too.
    """
    if lifecycle.phase not in {
        AdvancedEquityLifecyclePhase.IDLE,
        AdvancedEquityLifecyclePhase.BEFORE_OPEN,
    }:
        return False
    target = lifecycle.target_session_date
    if now >= calendar.session_open(target) - COMPLETED_SESSION_RECOVERY_LEAD:
        return False
    session = calendar.previous_session_date(target)
    if state.get("finished") == session or (
        state.get("next_check_at") is not None
        and now < state["next_check_at"]
    ):
        return False
    state["next_check_at"] = now + timedelta(
        seconds=COMPLETED_SESSION_RECOVERY_POLL_SECONDS
    )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        scope = load_continuous_advanced_scheduler_scope_cursor(
            unit.cursor,
            session_date=session,
        )
    if scope is None:
        state["finished"] = session
        return False
    # Recovering a past session must never stop today's scheduler. A session closed before
    # migration 048 has receipts with no visibility seal, which raised here until 25 minutes
    # before the open (found Nov 5, G61).
    try:
        rows = plan_session_windows(provider, scope, session, checked_at=now)
    except ValueError as error:
        print(
            f"Advanced completed-session recovery skipped for {session}: {error}",
            file=sys.stderr,
            flush=True,
        )
        state["finished"] = session
        return False
    if not rows:
        state["finished"] = session
        return False
    settings = catalog.profile_for_sha(
        scope["runtime_profile_sha256"]
    ).settings
    final_cutoff = calendar.session_close(session)
    row = next(
        (row for row in rows if row["cutoff"] == final_cutoff),
        None,
    )
    if row is None:
        state["finished"] = session
        return False
    first_recovery_attempt = settings.scheduler_window_retry_max_attempts + 1
    last_recovery_attempt = (
        settings.scheduler_window_retry_max_attempts
        + COMPLETED_SESSION_EXTRA_ATTEMPTS
    )
    if row["next_attempt"] > last_recovery_attempt:
        state["finished"] = session
        return False
    if not row["ready"] or row["next_attempt"] < first_recovery_attempt:
        return False
    with provider.transaction(APP_ROLE) as unit:
        recovery = discover_advanced_window_recovery_cursor(
            unit.cursor,
            cohort_id=scope["cohort_id"],
            cutoff=row["cutoff"],
            checked_at=now,
        )
        scheduler_session = ensure_advanced_scheduler_session_cursor(
            unit.cursor,
            authorization_id=scope["authorization_id"],
            ingress_plan_id=scope["plan_id"],
            cohort_id=scope["cohort_id"],
            session_date=session,
            first_authorized_cutoff=row["cutoff"],
            session_close_at=calendar.session_close(session),
            drain_deadline=now,
            checked_at=now,
        )
        retry = ensure_advanced_window_retry_cursor(
            unit.cursor,
            scheduler_session.scheduler_session_id,
            cohort_id=scope["cohort_id"],
            cutoff=row["cutoff"],
            attempt=recovery.next_attempt,
            retry_not_before=now,
            input_receipt=recovery.input_receipt,
            portal_receipt=recovery.portal_receipt,
        )
    (window_runner or run_window)(
        _window_arguments(
            args,
            scope,
            session_date=session,
            cutoff=row["cutoff"],
            retry=retry,
        )
    )
    # Finished only when a later check finds the close complete (no row) or the attempts are spent.
    state["next_check_at"] = now + COMPLETED_SESSION_RETRY_SPACING
    return True


def _retry_completion_budget(
    args,
    windows,
    *,
    attempt,
    now,
    drain_deadline,
) -> tuple[int, datetime]:
    """Bound retries before the next live slot and the session drain."""
    cap = drain_deadline
    if attempt > 0:
        next_live = min(
            (
                window.available_after
                for window in windows
                if window.available_after > now
            ),
            default=None,
        )
        if next_live is not None:
            cap = min(cap, next_live)
    budget = min(
        args.completion_budget_seconds,
        max(0, int((cap - now).total_seconds())),
    )
    return budget, cap


def _next_progress_check(
    windows,
    *,
    first_cutoff,
    now,
    retry_not_before,
    reconciliation_expires_at,
):
    candidates = [reconciliation_expires_at]
    if retry_not_before is not None:
        candidates.append(max(now, retry_not_before))
    if first_cutoff is not None:
        window = next(
            (
                candidate
                for candidate in windows
                if candidate.cutoff == first_cutoff
            ),
            None,
        )
        if window is not None and window.available_after > now:
            candidates.append(window.available_after)
    return min(candidates)


def main(
    argv=None,
    *,
    clock: Callable[[], datetime] | None = None,
) -> int:
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
    utc_now = clock or (lambda: datetime.now(timezone.utc))
    planner = AdvancedEquityLifecyclePlanner(
        calendar=calendar,
        clock=utc_now,
    )
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    continuous = args.runtime_mode == "continuous"
    session_date = args.session_date
    first_cutoff = args.first_cutoff
    initial_session = True
    audited_sessions: set[UUID] = set()
    cached_session_date = None
    cached_scope = None
    cached_windows = None
    cached_progress = None
    next_scope_reconcile_at = None
    next_progress_check_at = None
    pending_retry_not_before = None
    completed_session_recovery = {}
    try:
        while not stop.is_set():
            now = utc_now()
            if continuous:
                bootstrap_profile = catalog.profile_for_target(9)
                lifecycle = planner.plan(
                    drain_seconds=(
                        bootstrap_profile.settings.drain_deadline_seconds
                    ),
                    finality_delay_seconds=args.finality_delay_seconds,
                    completion_budget_seconds=(
                        args.completion_budget_seconds
                    ),
                )
                session_date = lifecycle.target_session_date
                if _recover_completed_session_window(
                    provider,
                    args,
                    catalog,
                    calendar,
                    lifecycle=lifecycle,
                    now=now,
                    state=completed_session_recovery,
                ):
                    continue
            refresh_scope = (
                not continuous
                or cached_session_date != session_date
                or next_scope_reconcile_at is None
                or now >= next_scope_reconcile_at
            )
            if refresh_scope:
                prior_session_date = cached_session_date
                prior_scope = cached_scope
                with provider.transaction(
                    APP_ROLE,
                    read_only=True,
                ) as unit:
                    scope = (
                        load_continuous_advanced_scheduler_scope_cursor(
                            unit.cursor,
                            session_date=session_date,
                        )
                        if continuous
                        else load_advanced_scheduler_scope_cursor(
                            unit.cursor,
                            authorization_id=args.authorization_id,
                            session_date=session_date,
                        )
                    )
                if continuous:
                    scope_changed = (
                        prior_session_date != session_date
                        or prior_scope is None
                        or scope is None
                        or prior_scope["cohort_id"] != scope["cohort_id"]
                        or prior_scope["plan_id"] != scope["plan_id"]
                    )
                    cached_session_date = session_date
                    cached_scope = scope
                    if scope_changed:
                        cached_windows = None
                        cached_progress = None
                        first_cutoff = None
                        next_progress_check_at = None
                        pending_retry_not_before = None
                    next_scope_reconcile_at = (
                        now
                        + timedelta(
                            seconds=args.scope_reconcile_seconds
                        )
                    )
            else:
                scope = cached_scope
            if scope is None:
                stop.wait(
                    args.scope_reconcile_seconds
                    if continuous
                    else args.poll_seconds
                )
                continue
            if (
                not continuous
                and initial_session
                and scope["cohort_id"] != args.cohort_id
            ):
                raise ValueError("initial scheduler cohort differs from plan")
            runtime_profile = catalog.profile_for_sha(
                scope["runtime_profile_sha256"]
            )
            session_close = scope["authorized_end_at"]
            bounded = is_advanced_canary_window(
                scope["authorized_start_at"], session_close,
            )
            if bounded:
                session_close = advanced_ingress_provider_deadline(
                    scope["authorized_start_at"], session_close,
                    drain_seconds=runtime_profile.settings.drain_deadline_seconds,
                    shutdown_seconds=runtime_profile.settings.shutdown_deadline_seconds,
                )
            drain_deadline = (
                scope["authorized_end_at"] if bounded
                else session_close + timedelta(seconds=runtime_profile.settings.drain_deadline_seconds)
            )
            all_windows = cached_windows or advanced_session_windows(
                session_date,
                finality_delay_seconds=args.finality_delay_seconds,
                completion_budget_seconds=args.completion_budget_seconds,
                serving_lag_seconds=30,
            )
            if continuous:
                cached_windows = all_windows
            if continuous:
                refresh_progress = (
                    cached_progress is None
                    or refresh_scope
                    or next_progress_check_at is None
                    or now >= next_progress_check_at
                )
                if refresh_progress:
                    with provider.transaction(
                        APP_ROLE,
                        read_only=True,
                    ) as unit:
                        cached_progress = (
                            load_advanced_scheduler_progress_cursor(
                                unit.cursor,
                                cohort_id=scope["cohort_id"],
                                session_date=session_date,
                                first_cutoff=all_windows[0].cutoff,
                                final_cutoff=all_windows[-1].cutoff,
                            )
                        )
                progress = cached_progress
                lifecycle = planner.plan(
                    drain_seconds=(
                        runtime_profile.settings.drain_deadline_seconds
                    ),
                    finality_delay_seconds=args.finality_delay_seconds,
                    completion_budget_seconds=(
                        args.completion_budget_seconds
                    ),
                    last_completed_cutoff=(
                        progress.last_completed_cutoff
                    ),
                )
                if lifecycle.target_session_date != session_date:
                    stop.wait(args.poll_seconds)
                    continue
                first_cutoff = lifecycle.first_due_cutoff
                drain_deadline = lifecycle.processing_deadline
                if (
                    pending_retry_not_before is not None
                    and pending_retry_not_before <= now
                ):
                    pending_retry_not_before = None
                next_progress_check_at = _next_progress_check(
                    all_windows,
                    first_cutoff=first_cutoff,
                    now=now,
                    retry_not_before=pending_retry_not_before,
                    reconciliation_expires_at=next_scope_reconcile_at,
                )
            own_first_cutoff = (
                first_cutoff
                if continuous or initial_session
                else all_windows[0].cutoff
            )
            if own_first_cutoff is None:
                stop.wait(
                    args.scope_reconcile_seconds
                    if continuous
                    else args.poll_seconds
                )
                continue
            windows = _authorized_windows(
                all_windows,
                first_cutoff=own_first_cutoff,
                started=scope["authorized_start_at"],
                ended=session_close,
            )
            with provider.transaction(APP_ROLE) as unit:
                scheduler_session = ensure_advanced_scheduler_session_cursor(
                    unit.cursor,
                    authorization_id=scope.get(
                        "authorization_id",
                        args.authorization_id,
                    ),
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
                    if not is_advanced_canary_window(
                        scope["authorized_start_at"], session_close,
                    ):
                        if not continuous:
                            session_date = calendar.next_session_date(
                                session_date
                            )
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
                        next_progress_check_at = now
                        continue
                    if now < recovery.retry_not_before:
                        pending_retry_not_before = (
                            recovery.retry_not_before
                            if pending_retry_not_before is None
                            else min(
                                pending_retry_not_before,
                                recovery.retry_not_before,
                            )
                        )
                        next_progress_check_at = min(
                            next_progress_check_at,
                            pending_retry_not_before,
                        )
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
                        next_progress_check_at = (
                            next_scope_reconcile_at
                        )
                        continue
                    retry_budget, retry_cap = _retry_completion_budget(
                        args,
                        all_windows,
                        attempt=recovery.next_attempt,
                        now=now,
                        drain_deadline=drain_deadline,
                    )
                    if retry_budget < 1:
                        pending_retry_not_before = (
                            retry_cap
                            if pending_retry_not_before is None
                            else min(
                                pending_retry_not_before,
                                retry_cap,
                            )
                        )
                        next_progress_check_at = min(
                            next_progress_check_at,
                            retry_cap,
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
                result = run_window(
                    _window_arguments(
                        args,
                        scope,
                        session_date=session_date,
                        cutoff=window.cutoff,
                        retry=retry,
                        completion_budget_seconds=retry_budget,
                        execution_deadline=(
                            retry_cap
                            if recovery.next_attempt > 0
                            else None
                        ),
                    )
                )
                with provider.transaction(APP_ROLE) as unit:
                    after = discover_advanced_window_recovery_cursor(
                        unit.cursor,
                        cohort_id=scope["cohort_id"],
                        cutoff=window.cutoff,
                        checked_at=utc_now(),
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
                        checked_at=utc_now(),
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
                        checked_at=utc_now(),
                    )
                next_progress_check_at = now
                # Rediscover after one executed window, so a new live slot
                # always runs ahead of the rest of the backlog.
                break
            stop.wait(args.poll_seconds)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
