#!/usr/bin/env python3
"""Run the single Advanced ingress daily-plan and reconnect authority."""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random
from uuid import UUID

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.advanced_ingress_controller import (  # noqa: E402
    AdvancedReconnectState,
    IngressFailureDisposition,
    classify_advanced_ingress_failure,
    full_jitter_backoff_seconds,
)
from realtime.advanced_gap_repair import (  # noqa: E402
    plan_advanced_cold_start_cursor,
    transition_backfill_state_cursor,
)
from realtime.advanced_ingress_plan import (  # noqa: E402
    AdvancedIngressBudgets,
    AdvancedIngressPlanRequest,
)
from realtime.advanced_ingress_repository import (  # noqa: E402
    AdvancedCohortResolutionError,
    advanced_ingress_controller_status_cursor,
    finish_advanced_ingress_plan_cursor,
    load_advanced_ingress_runtime_plan_cursor,
    load_advanced_stream_authorization_cursor,
    prepare_advanced_ingress_plan_cursor,
    resolve_advanced_ingress_plan_cursor,
)
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
)
from realtime.advanced_journal_registry import (  # noqa: E402
    advanced_journal_recovery_gate_cursor,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
    OptionsPipeline,
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.run_realtime_v2_advanced_equity_ingress import (  # noqa: E402
    _derived_runtime_paths,
)


UTC = timezone.utc


def _now() -> datetime:
    return datetime.now(UTC)


def _budget_arguments(parser) -> None:
    parser.add_argument("--stream-sessions", type=int, required=True)
    parser.add_argument("--connection-attempts", type=int, required=True)
    parser.add_argument("--decoded-bytes", type=int, required=True)
    parser.add_argument("--rest-repair-requests", type=int, required=True)
    parser.add_argument(
        "--rest-repair-decoded-bytes",
        type=int,
        required=True,
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--authorization-id", type=UUID, required=True)
    parser.add_argument("--universe-target", type=int, required=True)
    _budget_arguments(parser)
    parser.add_argument(
        "--startup-backfill-sessions",
        type=int,
        choices=(0, 1, 2),
        default=0,
    )
    parser.add_argument("--startup-request-ceiling", type=int)
    parser.add_argument("--startup-decoded-bytes", type=int)
    parser.add_argument("--startup-expected-cost")
    parser.add_argument("--debug-single-session", action="store_true")
    parser.add_argument("--external-ingress", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced controller targets only stocks_realtime_v2")
    if not args.execute:
        parser.error("Advanced controller requires --execute")
    args.budgets = AdvancedIngressBudgets(
        args.stream_sessions,
        args.connection_attempts,
        args.decoded_bytes,
        args.rest_repair_requests,
        args.rest_repair_decoded_bytes,
    )
    startup_values = (
        args.startup_request_ceiling,
        args.startup_decoded_bytes,
        args.startup_expected_cost,
    )
    if args.startup_backfill_sessions:
        if (
            any(value is None for value in startup_values)
            or args.startup_request_ceiling < 0
            or args.startup_decoded_bytes < 0
            or not args.startup_expected_cost.strip()
        ):
            parser.error(
                "startup backfill requires exact request/byte/cost allocation"
            )
    elif any(value is not None for value in startup_values):
        parser.error(
            "startup allocation requires --startup-backfill-sessions"
        )
    return args


def _provider(config):
    if config.app_dsn is None:
        raise ValueError("Advanced controller app DSN is unavailable")
    pool = ThreadedConnectionPool(
        1,
        4,
        dsn=config.app_dsn,
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def _read_child_report(path: Path) -> dict[str, object]:
    if not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError("Advanced ingress child report is unavailable")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("Advanced ingress child report is invalid")
    return value


def _backfill_delays_live(backfill, *, now: datetime, warmup_at: datetime) -> bool:
    if now >= warmup_at:
        return False
    return bool(
        backfill.work
        or backfill.state in {"BACKFILL_BLOCKED", "WAIT_NEXT_SESSION"}
    )


def _recovery_blocks_reconnect(recovery_gate) -> bool:
    return bool(
        recovery_gate.registered
        or recovery_gate.writing
        or recovery_gate.quarantined
    )


def _allocate_runtime_paths(
    journal_root: Path,
    *,
    plan_id: UUID,
    session_date,
    connection_attempt: int,
) -> tuple[Path, Path, int]:
    for runtime_path_attempt in range(1, 1_001):
        try:
            journal_path, report_path = _derived_runtime_paths(
                journal_root,
                plan_id=plan_id,
                session_date=session_date,
                connection_attempt=connection_attempt,
                runtime_path_attempt=runtime_path_attempt,
            )
        except RuntimeError:
            continue
        return journal_path, report_path, runtime_path_attempt
    raise RuntimeError("Advanced ingress runtime path attempts are exhausted")


def _load_child_evidence(provider, plan_id, connection_attempt):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT session.status,session.failure_code,"
            "session.started_at,session.ended_at,registry.status,"
            "registry.quarantine_reason "
            "FROM rt.stream_sessions AS session "
            "LEFT JOIN rt.advanced_journal_registry AS registry "
            "ON registry.journal_generation=session.journal_generation "
            "WHERE session.advanced_ingress_plan_id=%s "
            "AND session.connection_attempt=%s "
            "ORDER BY session.owner_epoch DESC LIMIT 1",
            (str(plan_id), connection_attempt),
        )
        row = unit.cursor.fetchone()
    if row is None:
        return None
    return {
        "session_status": str(row[0]),
        "failure_code": str(row[1]) if row[1] is not None else None,
        "started_at": row[2],
        "ended_at": row[3],
        "journal_status": str(row[4]) if row[4] is not None else None,
        "quarantine_reason": (
            str(row[5]) if row[5] is not None else None
        ),
    }


def _classify_child_result(
    *,
    return_code: int,
    intentional: bool,
    report: dict[str, object] | None,
    durable: dict[str, object] | None,
    stderr: str,
    checked_at: datetime,
):
    if return_code == 0 and (
        report is not None
        and str(report.get("status") or "").upper() == "STOPPED"
        or durable is not None
        and durable.get("session_status") == "STOPPED"
        and durable.get("journal_status") in {"DRAINING", "DRAINED"}
    ):
        return None
    failure_code = (
        str(report.get("failure_code") or "")
        if report is not None
        else ""
    )
    if not failure_code and durable is not None:
        failure_code = str(durable.get("failure_code") or "")
        if not failure_code and durable.get("quarantine_reason"):
            failure_code = (
                f"JOURNAL_{durable['quarantine_reason']}"
            )
    if not failure_code:
        failure_code = f"CHILD_EXIT_{return_code}"
    detail_parts = []
    if report is not None and report.get("failure_detail"):
        detail_parts.append(str(report["failure_detail"]))
    if durable is not None:
        detail_parts.append(
            "durable="
            f"{durable.get('session_status')}/"
            f"{durable.get('journal_status')}"
        )
    if stderr:
        detail_parts.append(stderr[-160:])
    live_seconds = (
        float(report.get("live_seconds") or 0)
        if report is not None
        else 0.0
    )
    if (
        live_seconds == 0
        and durable is not None
        and durable.get("started_at") is not None
    ):
        ended_at = durable.get("ended_at") or checked_at
        live_seconds = max(
            0.0,
            (ended_at - durable["started_at"]).total_seconds(),
        )
    return classify_advanced_ingress_failure(
        failure_code,
        detail="; ".join(detail_parts),
        live_seconds=live_seconds,
        intentional_shutdown=intentional,
    )


def _run_child(
    command: list[str],
    *,
    stop: threading.Event,
    deadline: datetime,
    poll_seconds: float,
    shutdown_seconds: int,
    utc_clock=_now,
    popen_factory=subprocess.Popen,
):
    process = popen_factory(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    intentional = False
    while process.poll() is None:
        if stop.wait(poll_seconds) or utc_clock() >= deadline:
            intentional = True
            process.terminate()
            try:
                process.wait(timeout=shutdown_seconds)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=shutdown_seconds)
            break
    stdout, stderr = process.communicate()
    return process.returncode, intentional, stdout, stderr


def _child_command(
    args,
    plan_id: UUID,
    *,
    runtime_path_attempt: int = 1,
) -> list[str]:
    return [
        sys.executable,
        "-B",
        str(
            BACKEND_DIR
            / "scripts"
            / "run_realtime_v2_advanced_equity_ingress.py"
        ),
        "--database",
        args.database,
        "--confirm-database-name",
        args.confirm_database_name,
        "--ingress-plan-id",
        str(plan_id),
        "--runtime-path-attempt",
        str(runtime_path_attempt),
        "--execute",
    ]


def _record_terminal(provider, plan_id, outcome, checked_at):
    with provider.transaction(APP_ROLE) as unit:
        return finish_advanced_ingress_plan_cursor(
            unit.cursor,
            plan_id,
            terminal_outcome=outcome,
            completed_at=checked_at,
        )


def main(argv=None) -> int:
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.CONTROLLER,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced controller requires Advanced equity and disabled options"
        )
    journal_root = config.advanced_journal_dir
    if journal_root is None or not journal_root.is_dir():
        raise RuntimeError("Advanced controller journal directory is missing")
    catalog = load_advanced_ingress_runtime_catalog(
        config.advanced_ingress_policy_path,
        expected_version=config.advanced_ingress_policy_version,
    )
    runtime_profile = catalog.profile_for_target(args.universe_target)
    settings = runtime_profile.settings
    calendar = OptionExchangeCalendar("XNYS")
    provider, pool = _provider(config)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    reconnect = AdvancedReconnectState()
    random_source = Random()
    try:
        while not stop.is_set():
            now = _now()
            session_date = calendar.session_on_or_after(now.date())
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                authorization = load_advanced_stream_authorization_cursor(
                    unit.cursor,
                    args.authorization_id,
                )
            if session_date < authorization.valid_from:
                session_date = calendar.session_on_or_after(
                    authorization.valid_from
                )
            if (
                authorization.status != "READY"
                or session_date > authorization.valid_through
            ):
                stop.wait(settings.bridge_poll_interval_seconds)
                continue
            session_open = calendar.session_open(session_date)
            session_close = calendar.session_close(session_date)
            warmup_at = session_open - timedelta(
                seconds=settings.controller_warmup_seconds
            )
            drain_deadline = session_close + timedelta(
                seconds=settings.drain_deadline_seconds
            )
            if now > drain_deadline:
                session_date = calendar.next_session_date(session_date)
                session_open = calendar.session_open(session_date)
                session_close = calendar.session_close(session_date)
                warmup_at = session_open - timedelta(
                    seconds=settings.controller_warmup_seconds
                )
                drain_deadline = session_close + timedelta(
                    seconds=settings.drain_deadline_seconds
                )
                if session_date > authorization.valid_through:
                    stop.wait(settings.bridge_poll_interval_seconds)
                    continue
            try:
                with provider.transaction(APP_ROLE) as unit:
                    plan = resolve_advanced_ingress_plan_cursor(
                        unit.cursor,
                        AdvancedIngressPlanRequest(
                            args.authorization_id,
                            session_date,
                            args.universe_target,
                            args.budgets,
                        ),
                        runtime_catalog=catalog,
                        calendar=calendar,
                    )
                    prepare_advanced_ingress_plan_cursor(
                        unit.cursor,
                        plan,
                        checked_at=now,
                    )
            except AdvancedCohortResolutionError:
                stop.wait(settings.bridge_poll_interval_seconds)
                continue
            if args.startup_backfill_sessions:
                own_sessions = []
                previous = session_date
                for _index in range(args.startup_backfill_sessions):
                    previous = calendar.previous_session_date(previous)
                    own_sessions.append(previous)
                with provider.transaction(APP_ROLE) as unit:
                    backfill = plan_advanced_cold_start_cursor(
                        unit.cursor,
                        plan_id=plan.plan_id,
                        sessions=tuple(sorted(own_sessions)),
                        known_by=now,
                        request_ceiling=args.startup_request_ceiling,
                        decoded_byte_ceiling=args.startup_decoded_bytes,
                        expected_cost=args.startup_expected_cost,
                    )
                    if (
                        backfill.state == "BACKFILL_PLANNED"
                        and not backfill.work
                    ):
                        transition_backfill_state_cursor(
                            unit.cursor,
                            backfill.backfill_id,
                            state="BACKFILL_COMPLETE",
                            reason_code="COVERAGE_ALREADY_COMPLETE",
                            next_action="WAIT_FOR_NEXT_XNYS_SESSION",
                            checked_at=now,
                        )
                if _backfill_delays_live(
                    backfill,
                    now=now,
                    warmup_at=warmup_at,
                ):
                    stop.wait(settings.bridge_poll_interval_seconds)
                    continue
            if now < warmup_at:
                stop.wait(
                    min(
                        settings.bridge_poll_interval_seconds,
                        max(0.0, (warmup_at - now).total_seconds()),
                    )
                )
                continue
            if now < plan.authorized_start_at:
                stop.wait(
                    min(
                        settings.bridge_poll_interval_seconds,
                        max(
                            0.0,
                            (plan.authorized_start_at - now).total_seconds(),
                        ),
                    )
                )
                continue
            while not stop.is_set():
                now = _now()
                with provider.transaction(APP_ROLE, read_only=True) as unit:
                    controller_status = (
                        advanced_ingress_controller_status_cursor(
                            unit.cursor,
                            plan.plan_id,
                        )
                    )
                if controller_status.terminal_outcome is not None:
                    break
                if (
                    controller_status.authorization_status != "READY"
                    or controller_status.connection_attempt_count
                    >= controller_status.maximum_connection_attempts
                ):
                    _record_terminal(
                        provider,
                        plan.plan_id,
                        "AUTHORIZATION_EXHAUSTED",
                        now,
                    )
                    break
                if now >= drain_deadline:
                    with provider.transaction(
                        APP_ROLE,
                        read_only=True,
                    ) as unit:
                        recovery_gate = (
                            advanced_journal_recovery_gate_cursor(
                                unit.cursor,
                                plan.plan_id,
                            )
                        )
                    if (
                        recovery_gate.registered
                        or recovery_gate.writing
                        or recovery_gate.draining
                    ):
                        stop.wait(
                            settings.bridge_poll_interval_seconds
                        )
                        continue
                    if recovery_gate.quarantined:
                        _record_terminal(
                            provider,
                            plan.plan_id,
                            "BLOCKED_CONFIGURATION",
                            now,
                        )
                    else:
                        _record_terminal(
                            provider,
                            plan.plan_id,
                            "SESSION_COMPLETE",
                            now,
                        )
                    break
                with provider.transaction(
                    APP_ROLE,
                    read_only=True,
                ) as unit:
                    recovery_gate = advanced_journal_recovery_gate_cursor(
                        unit.cursor,
                        plan.plan_id,
                    )
                if recovery_gate.quarantined:
                    _record_terminal(
                        provider,
                        plan.plan_id,
                        "BLOCKED_CONFIGURATION",
                        now,
                    )
                    break
                if _recovery_blocks_reconnect(recovery_gate):
                    if recovery_gate.quarantined:
                        _record_terminal(
                            provider,
                            plan.plan_id,
                            "BLOCKED_CONFIGURATION",
                            now,
                        )
                        break
                    stop.wait(settings.bridge_poll_interval_seconds)
                    continue
                with provider.transaction(APP_ROLE, read_only=True) as unit:
                    runtime_scope = load_advanced_ingress_runtime_plan_cursor(
                        unit.cursor,
                        plan.plan_id,
                    )
                if args.external_ingress:
                    stop.wait(settings.bridge_poll_interval_seconds)
                    continue
                (
                    _journal_path,
                    child_report_path,
                    runtime_path_attempt,
                ) = _allocate_runtime_paths(
                    journal_root,
                    plan_id=plan.plan_id,
                    session_date=plan.session_date,
                    connection_attempt=(
                        runtime_scope.next_connection_attempt
                    ),
                )
                (
                    return_code,
                    intentional,
                    _stdout,
                    stderr,
                ) = _run_child(
                    _child_command(
                        args,
                        plan.plan_id,
                        runtime_path_attempt=runtime_path_attempt,
                    ),
                    stop=stop,
                    deadline=drain_deadline,
                    poll_seconds=settings.ingress_poll_interval_seconds,
                    shutdown_seconds=settings.shutdown_deadline_seconds,
                )
                if intentional:
                    if not stop.is_set() and _now() >= drain_deadline:
                        _record_terminal(
                            provider,
                            plan.plan_id,
                            "SESSION_COMPLETE",
                            _now(),
                        )
                    break
                try:
                    report = _read_child_report(child_report_path)
                except (OSError, ValueError, json.JSONDecodeError):
                    report = None
                durable = _load_child_evidence(
                    provider,
                    plan.plan_id,
                    runtime_scope.next_connection_attempt,
                )
                failure = _classify_child_result(
                    return_code=return_code,
                    intentional=intentional,
                    report=report,
                    durable=durable,
                    stderr=stderr,
                    checked_at=_now(),
                )
                if failure is None:
                    stop.wait(settings.bridge_poll_interval_seconds)
                    continue
                if (
                    failure.disposition
                    is IngressFailureDisposition.TERMINAL
                ):
                    _record_terminal(
                        provider,
                        plan.plan_id,
                        failure.terminal_outcome,
                        _now(),
                    )
                    break
                reconnect = reconnect.after_failure(
                    failure,
                    stable_live_reset_seconds=(
                        settings.stable_live_reset_seconds
                    ),
                )
                delay = full_jitter_backoff_seconds(
                    reconnect,
                    initial_seconds=(
                        settings.reconnect_initial_backoff_seconds
                    ),
                    multiplier=settings.reconnect_backoff_multiplier,
                    maximum_seconds=(
                        settings.reconnect_maximum_backoff_seconds
                    ),
                    jitter_ceiling_seconds=(
                        settings.reconnect_jitter_ceiling_seconds
                    ),
                    random_source=random_source,
                )
                if _now() + timedelta(seconds=delay) >= drain_deadline:
                    _record_terminal(
                        provider,
                        plan.plan_id,
                        "SESSION_COMPLETE",
                        _now(),
                    )
                    break
                if stop.wait(delay):
                    break
            terminal_report = {
                "status": "IDLE",
                "plan_id": str(plan.plan_id),
                "session_date": plan.session_date.isoformat(),
                "budgets": asdict(plan.budgets),
                "provider_requests": 0,
                "credentials_exposed": False,
            }
            controller_report = (
                journal_root
                / f"advanced-controller-{plan.session_date}-{plan.plan_id}.json"
            )
            if not controller_report.exists():
                write_report(controller_report, terminal_report)
            if args.debug_single_session:
                return 0
            stop.wait(settings.bridge_poll_interval_seconds)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
