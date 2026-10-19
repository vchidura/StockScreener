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
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.advanced_ingress_controller import (  # noqa: E402
    AdvancedReconnectState,
    HybridBarIdentity,
    HybridRepairIdentity,
    HybridStartupState,
    IngressFailureDisposition,
    classify_advanced_ingress_failure,
    full_jitter_backoff_seconds,
    hybrid_repair_identity,
    plan_hybrid_startup,
    reconcile_hybrid_minute_closure,
)
from realtime.advanced_gap_repair import (  # noqa: E402
    plan_advanced_cold_start_cursor,
    transition_backfill_state_cursor,
)
from realtime.advanced_ingress_plan import (  # noqa: E402
    AdvancedIngressBudgets,
    AdvancedIngressPlanRequest,
    advanced_ingress_provider_deadline,
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
from realtime.acquisition_ledger import (  # noqa: E402
    AcquisitionWatermark,
    advance_acquisition_watermark_cursor,
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
from realtime.contracts import canonical_sha256  # noqa: E402
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
    parser.add_argument("--rest-repair-response-byte-limit", type=int)
    parser.add_argument("--rest-repair-expected-cost")
    parser.add_argument(
        "--rest-repair-minimum-spacing-seconds",
        type=float,
        default=0.5,
    )
    parser.add_argument(
        "--rest-repair-timeout-seconds",
        type=float,
        default=30,
    )
    parser.add_argument("--debug-single-session", action="store_true")
    parser.add_argument("--external-ingress", action="store_true")
    parser.add_argument(
        "--canary-start-at",
        type=datetime.fromisoformat,
        default=os.getenv("REALTIME_V2_ADVANCED_CANARY_START_AT") or None,
    )
    parser.add_argument(
        "--canary-end-at",
        type=datetime.fromisoformat,
        default=os.getenv("REALTIME_V2_ADVANCED_CANARY_END_AT") or None,
    )
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
    if args.canary_start_at is not None or args.canary_end_at is not None:
        try:
            request = AdvancedIngressPlanRequest(
                args.authorization_id,
                (args.canary_start_at or args.canary_end_at).date(),
                args.universe_target,
                args.budgets,
                canary_start_at=args.canary_start_at,
                canary_end_at=args.canary_end_at,
            )
        except (TypeError, ValueError) as error:
            parser.error(str(error))
        args.canary_start_at = request.canary_start_at
        args.canary_end_at = request.canary_end_at
        if args.startup_backfill_sessions:
            parser.error("bounded canary does not authorize startup backfill")
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
    repair_enabled = args.budgets.rest_repair_requests > 0
    if repair_enabled:
        if (
            args.rest_repair_response_byte_limit is None
            or args.rest_repair_response_byte_limit < 1_024
            or args.rest_repair_response_byte_limit + 1
            > args.budgets.rest_repair_decoded_bytes
            or not (args.rest_repair_expected_cost or "").strip()
        ):
            parser.error(
                "REST repair requires exact response bytes and expected cost"
            )
    elif (
        args.rest_repair_response_byte_limit is not None
        or args.rest_repair_expected_cost is not None
    ):
        parser.error("REST repair allocation is zero")
    if (
        args.rest_repair_minimum_spacing_seconds <= 0
        or not 1 <= args.rest_repair_timeout_seconds <= 60
    ):
        parser.error("REST repair delay, pacing or timeout is invalid")
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
    tick=None,
):
    process = popen_factory(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    intentional = False
    while process.poll() is None:
        if tick is not None and tick() is False:
            intentional = True
            process.terminate()
            try:
                process.wait(timeout=shutdown_seconds)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=shutdown_seconds)
            break
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


@dataclass(frozen=True, slots=True)
class _HybridRepairWindow:
    identity: HybridRepairIdentity
    state: str
    report_path: Path


def _load_hybrid_progress_cursor(cursor, plan_id, cohort_id):
    cursor.execute(
        "SELECT min(started_at) FROM rt.stream_sessions "
        "WHERE advanced_ingress_plan_id=%s AND subscribed_at IS NOT NULL",
        (str(plan_id),),
    )
    stream_started_at = cursor.fetchone()[0]
    cursor.execute(
        "SELECT member.instrument_id "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_members AS member USING(manifest_id) "
        "WHERE cohort.cohort_id=%s AND member.channel='AM' "
        "ORDER BY member.instrument_id",
        (str(cohort_id),),
    )
    instrument_ids = tuple(int(row[0]) for row in cursor.fetchall())
    if not instrument_ids:
        raise ValueError("Advanced hybrid repair has no AM members")
    cursor.execute(
        "SELECT instrument_id,last_complete_bar_end "
        "FROM rt.acquisition_watermarks "
        "WHERE cohort_id=%s AND interval='1m' "
        "AND source_kind='RECONCILED' AND terminal_state='COMPLETE' "
        "AND instrument_id=ANY(%s::bigint[])",
        (str(cohort_id), list(instrument_ids)),
    )
    retained = {
        int(row[0]): row[1]
        for row in cursor.fetchall()
    }
    complete_through = (
        min(retained.values())
        if set(retained) == set(instrument_ids)
        else None
    )
    return stream_started_at, complete_through, instrument_ids


def _ensure_hybrid_window_cursor(
    cursor,
    *,
    runtime_scope,
    identity,
    deadline,
    journal_root,
):
    cutoff = identity.repair_end or identity.gap_end
    if identity.release_at >= deadline:
        raise ValueError("hybrid repair release is outside authorization")
    configuration_sha256 = canonical_sha256({
        "contract": "advanced-hybrid-repair-window-v1",
        "ingress_plan_id": runtime_scope.plan_id,
        "repair_id": identity.repair_id,
        "cohort_id": runtime_scope.cohort_id,
        "session_date": runtime_scope.session_date,
        "gap_start": identity.gap_start,
        "gap_end": identity.gap_end,
        "repair_start": identity.repair_start,
        "repair_end": identity.repair_end,
        "release_at": identity.release_at,
        "endpoint_class": identity.endpoint_class,
        "eligibility_policy_version": (
            identity.eligibility_policy_version
        ),
        "provider_delay_seconds": identity.provider_delay.total_seconds(),
        "deadline": deadline,
    })
    service_plan_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-hybrid-service:{configuration_sha256}",
    )
    cursor.execute(
        "INSERT INTO rt.service_plans ("
        "plan_id,cohort_id,session_date,stage_catalog_version,"
        "enabled_stages,config_sha256,deadline,scope_reference"
        ") VALUES (%s,%s,%s,'stages_v2',%s,%s,%s,%s) "
        "ON CONFLICT (cohort_id,session_date,config_sha256) DO NOTHING",
        (
            str(service_plan_id),
            str(runtime_scope.cohort_id),
            runtime_scope.session_date,
            ["EQUITY_INGEST"],
            configuration_sha256,
            deadline,
            runtime_scope.authorization_reference,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.window_runs ("
        "window_id,plan_id,cohort_id,provider_tier,window_key,"
        "release_at,cutoff,deadline"
        ") VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s) "
        "ON CONFLICT (cohort_id,window_key) DO NOTHING",
        (
            str(identity.repair_id),
            str(service_plan_id),
            str(runtime_scope.cohort_id),
            identity.window_key,
            identity.release_at,
            cutoff,
            deadline,
        ),
    )
    cursor.execute(
        "SELECT plan_id,state,release_at,cutoff,deadline "
        "FROM rt.window_runs WHERE window_id=%s",
        (str(identity.repair_id),),
    )
    row = cursor.fetchone()
    expected = (
        str(service_plan_id),
        identity.release_at,
        cutoff,
        deadline,
    )
    if row is None or (
        str(row[0]),
        row[2],
        row[3],
        row[4],
    ) != expected:
        raise ValueError("hybrid repair window identity changed")
    return _HybridRepairWindow(
        identity,
        str(row[1]),
        journal_root / f"advanced-hybrid-repair-{identity.repair_id}.json",
    )


def _claim_hybrid_window_cursor(
    cursor,
    repair_id,
    owner_id,
    *,
    checked_at,
    lease_seconds,
):
    cursor.execute(
        "UPDATE rt.window_runs SET state='RUNNING',owner_id=%s,"
        "owner_epoch=owner_epoch+1,lease_until=%s,updated_at=%s "
        "WHERE window_id=%s AND (state='PENDING' OR "
        "(state='RUNNING' AND lease_until<=%s)) RETURNING owner_epoch",
        (
            str(owner_id),
            checked_at + timedelta(seconds=lease_seconds),
            checked_at,
            str(repair_id),
            checked_at,
        ),
    )
    row = cursor.fetchone()
    return int(row[0]) if row is not None else None


def _finish_hybrid_window_cursor(
    cursor,
    repair_id,
    *,
    state,
    checked_at,
):
    if state not in {"COMPLETE", "FAILED", "RETAINED_GAP"}:
        raise ValueError("hybrid repair terminal state is invalid")
    cursor.execute(
        "UPDATE rt.window_runs SET state=%s,lease_until=NULL,updated_at=%s "
        "WHERE window_id=%s AND state IN ('PENDING','RUNNING')",
        (state, checked_at, str(repair_id)),
    )


def _load_hybrid_bars_cursor(
    cursor,
    *,
    cohort_id,
    instrument_ids,
    requested_from,
    requested_to,
):
    cursor.execute(
        "SELECT current.instrument_id,current.bar_start,current.bar_end,"
        "revision.source_kind,revision.quality_codes "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "AND revision.cohort_id=current.cohort_id "
        "AND revision.provider_tier=current.provider_tier "
        "AND revision.instrument_id=current.instrument_id "
        "AND revision.interval=current.interval "
        "WHERE current.cohort_id=%s "
        "AND current.provider_tier='MASSIVE_ADVANCED' "
        "AND current.instrument_id=ANY(%s::bigint[]) "
        "AND current.interval='1m' "
        "AND current.session_scope='RTH' "
        "AND current.adjustment_basis='RAW_UNADJUSTED' "
        "AND current.bar_start>=%s AND current.bar_end<=%s "
        "AND revision.is_final "
        "ORDER BY current.instrument_id,current.bar_start",
        (
            str(cohort_id),
            list(instrument_ids),
            requested_from,
            requested_to,
        ),
    )
    return tuple(
        HybridBarIdentity(
            int(row[0]),
            row[1],
            row[2],
            str(row[3]),
            tuple(row[4] or ()),
        )
        for row in cursor.fetchall()
    )


def _incremental_repair_command(
    args,
    runtime_scope,
    identity,
    report_path,
    settings,
):
    cutoff = identity.repair_end or identity.gap_end
    return [
        sys.executable,
        "-B",
        str(
            BACKEND_DIR
            / "scripts"
            / "run_realtime_v2_incremental_equity.py"
        ),
        "--database",
        args.database,
        "--confirm-database-name",
        args.confirm_database_name,
        "--cohort-id",
        str(runtime_scope.cohort_id),
        "--advanced-ingress-plan-id",
        str(runtime_scope.plan_id),
        "--session-date",
        runtime_scope.session_date.isoformat(),
        "--interval",
        "1m",
        "--cutoff",
        cutoff.isoformat(),
        "--provider-delay-seconds",
        str(settings.rest_provider_delay_seconds),
        "--rest-endpoint-class",
        settings.rest_aggregate_endpoint_class,
        "--eligibility-policy-version",
        settings.rest_eligibility_policy_version,
        "--response-byte-limit",
        str(args.rest_repair_response_byte_limit),
        "--account-request-ceiling",
        str(runtime_scope.budgets.rest_repair_requests),
        "--account-decoded-byte-ceiling",
        str(runtime_scope.budgets.rest_repair_decoded_bytes),
        "--authorized-until",
        runtime_scope.authorized_end_at.isoformat(),
        "--authorization-reference",
        runtime_scope.authorization_reference,
        "--provider-authorization-reference",
        "stocks-advanced",
        "--expected-cost",
        args.rest_repair_expected_cost,
        "--minimum-spacing-seconds",
        str(args.rest_repair_minimum_spacing_seconds),
        "--timeout-seconds",
        str(args.rest_repair_timeout_seconds),
        "--report-path",
        str(report_path),
        "--once",
        "--execute-shadow",
    ]


class _HybridRepairCoordinator:
    def __init__(
        self,
        provider,
        args,
        runtime_scope,
        *,
        session_open,
        deadline,
        journal_root,
        lease_seconds,
        settings,
        utc_clock=_now,
        popen_factory=subprocess.Popen,
    ):
        self.provider = provider
        self.args = args
        self.runtime_scope = runtime_scope
        self.session_open = session_open
        self.deadline = deadline
        self.journal_root = journal_root
        self.lease_seconds = lease_seconds
        self.settings = settings
        self.utc_clock = utc_clock
        self.popen_factory = popen_factory
        self.owner_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-hybrid-owner:{runtime_scope.plan_id}",
        )
        self.window = None
        self.instrument_ids = ()
        self.process = None
        self.terminal_outcome = None

    @property
    def unresolved(self):
        return self.process is not None or self.window is not None

    def _plan_next(self, now):
        with self.provider.transaction(APP_ROLE, read_only=True) as unit:
            (
                stream_started_at,
                complete_through,
                instrument_ids,
            ) = _load_hybrid_progress_cursor(
                unit.cursor,
                self.runtime_scope.plan_id,
                self.runtime_scope.cohort_id,
            )
        if stream_started_at is None:
            return
        startup = plan_hybrid_startup(
            session_open=self.session_open,
            stream_started_at=stream_started_at,
            checked_at=now,
            provider_delay=timedelta(
                seconds=self.settings.rest_provider_delay_seconds
            ),
            complete_through=complete_through,
        )
        if startup.state is HybridStartupState.NO_GAP:
            return
        identity = hybrid_repair_identity(
            ingress_plan_id=self.runtime_scope.plan_id,
            cohort_id=self.runtime_scope.cohort_id,
            startup=startup,
            checked_at=now,
            provider_delay=timedelta(
                seconds=self.settings.rest_provider_delay_seconds
            ),
            endpoint_class=self.settings.rest_aggregate_endpoint_class,
            eligibility_policy_version=(
                self.settings.rest_eligibility_policy_version
            ),
        )
        if identity is None:
            return
        if self.runtime_scope.budgets.rest_repair_requests == 0:
            self.terminal_outcome = "AUTHORIZATION_EXHAUSTED"
            return
        if identity.release_at >= self.deadline:
            self.terminal_outcome = "AUTHORIZATION_EXHAUSTED"
            return
        with self.provider.transaction(APP_ROLE) as unit:
            self.window = _ensure_hybrid_window_cursor(
                unit.cursor,
                runtime_scope=self.runtime_scope,
                identity=identity,
                deadline=self.deadline,
                journal_root=self.journal_root,
            )
        self.instrument_ids = instrument_ids

    def _verify_completed(self, now):
        identity = self.window.identity
        through = identity.repair_end or identity.gap_end
        with self.provider.transaction(APP_ROLE) as unit:
            bars = _load_hybrid_bars_cursor(
                unit.cursor,
                cohort_id=self.runtime_scope.cohort_id,
                instrument_ids=self.instrument_ids,
                requested_from=identity.gap_start,
                requested_to=through,
            )
            closure = reconcile_hybrid_minute_closure(
                requested_from=identity.gap_start,
                requested_to=through,
                instrument_ids=self.instrument_ids,
                bars=bars,
            )
            if closure.complete_through != through:
                _finish_hybrid_window_cursor(
                    unit.cursor,
                    identity.repair_id,
                    state="RETAINED_GAP",
                    checked_at=now,
                )
                self.terminal_outcome = "BLOCKED_GAP"
                return
            for instrument_id in self.instrument_ids:
                advance_acquisition_watermark_cursor(
                    unit.cursor,
                    AcquisitionWatermark(
                        cohort_id=self.runtime_scope.cohort_id,
                        instrument_id=instrument_id,
                        interval="1m",
                        source_kind="RECONCILED",
                        last_complete_bar_end=through,
                        overlap_policy="advanced_hybrid_exact_closure_v1",
                    ),
                )
            _finish_hybrid_window_cursor(
                unit.cursor,
                identity.repair_id,
                state="COMPLETE",
                checked_at=now,
            )
        self.window = None

    def _finish_process(self, now):
        return_code = self.process.poll()
        if return_code is None:
            return
        _stdout, stderr = self.process.communicate()
        self.process = None
        if return_code:
            failure = classify_advanced_ingress_failure(
                f"CHILD_EXIT_{return_code}",
                detail=stderr[-320:],
            )
            with self.provider.transaction(APP_ROLE) as unit:
                _finish_hybrid_window_cursor(
                    unit.cursor,
                    self.window.identity.repair_id,
                    state="FAILED",
                    checked_at=now,
                )
            self.terminal_outcome = failure.terminal_outcome
            return
        self._verify_completed(now)

    def tick(self):
        now = self.utc_clock()
        if self.process is not None:
            self._finish_process(now)
        if self.terminal_outcome is not None or self.process is not None:
            return self.terminal_outcome is None
        if self.window is None:
            self._plan_next(now)
        if self.window is None or self.terminal_outcome is not None:
            return self.terminal_outcome is None
        if self.window.state == "COMPLETE":
            self.window = None
            return True
        if self.window.state in {"FAILED", "MISSED", "RETAINED_GAP"}:
            self.terminal_outcome = "BLOCKED_GAP"
            return False
        if self.window.report_path.exists():
            self._verify_completed(now)
            return self.terminal_outcome is None
        if now < self.window.identity.release_at:
            return True
        with self.provider.transaction(APP_ROLE) as unit:
            epoch = _claim_hybrid_window_cursor(
                unit.cursor,
                self.window.identity.repair_id,
                self.owner_id,
                checked_at=now,
                lease_seconds=self.lease_seconds,
            )
        if epoch is None:
            return True
        self.process = self.popen_factory(
            _incremental_repair_command(
                self.args,
                self.runtime_scope,
                self.window.identity,
                self.window.report_path,
                self.settings,
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return True

    def close(self):
        if self.process is None or self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=self.lease_seconds)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=self.lease_seconds)


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
            if args.canary_end_at is not None and now >= args.canary_end_at:
                stop.wait(settings.bridge_poll_interval_seconds)
                continue
            session_date = (
                args.canary_start_at.date()
                if args.canary_start_at is not None
                else calendar.session_on_or_after(now.date())
            )
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
                            canary_start_at=args.canary_start_at,
                            canary_end_at=args.canary_end_at,
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
            provider_deadline = advanced_ingress_provider_deadline(
                plan.authorized_start_at,
                plan.authorized_end_at,
                drain_seconds=settings.drain_deadline_seconds,
                shutdown_seconds=settings.shutdown_deadline_seconds,
            )
            if args.canary_end_at is not None:
                drain_deadline = plan.authorized_end_at
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
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                initial_runtime_scope = (
                    load_advanced_ingress_runtime_plan_cursor(
                        unit.cursor,
                        plan.plan_id,
                    )
                )
            hybrid = _HybridRepairCoordinator(
                provider,
                args,
                initial_runtime_scope,
                session_open=session_open,
                deadline=provider_deadline,
                journal_root=journal_root,
                lease_seconds=max(
                    settings.shutdown_deadline_seconds,
                    int(args.rest_repair_timeout_seconds) + 300,
                ),
                settings=settings,
            )
            while not stop.is_set():
                now = _now()
                hybrid.tick()
                if hybrid.terminal_outcome is not None:
                    _record_terminal(
                        provider,
                        plan.plan_id,
                        hybrid.terminal_outcome,
                        now,
                    )
                    break
                with provider.transaction(APP_ROLE, read_only=True) as unit:
                    controller_status = (
                        advanced_ingress_controller_status_cursor(
                            unit.cursor,
                            plan.plan_id,
                        )
                    )
                if controller_status.terminal_outcome is not None:
                    break
                if controller_status.authorization_status != "READY":
                    _record_terminal(
                        provider,
                        plan.plan_id,
                        "AUTHORIZATION_EXHAUSTED",
                        now,
                    )
                    break
                if now >= provider_deadline:
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
                        if args.canary_end_at is not None and now >= drain_deadline:
                            _record_terminal(
                                provider, plan.plan_id, "FAILED_INFRASTRUCTURE", now,
                            )
                            break
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
                    elif hybrid.unresolved:
                        _record_terminal(
                            provider,
                            plan.plan_id,
                            "BLOCKED_GAP",
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
                if (
                    controller_status.connection_attempt_count
                    >= controller_status.maximum_connection_attempts
                ):
                    _record_terminal(
                        provider, plan.plan_id, "AUTHORIZATION_EXHAUSTED", now,
                    )
                    break
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
                    deadline=provider_deadline,
                    poll_seconds=settings.ingress_poll_interval_seconds,
                    shutdown_seconds=settings.shutdown_deadline_seconds,
                    tick=hybrid.tick,
                )
                if hybrid.terminal_outcome is not None:
                    _record_terminal(
                        provider,
                        plan.plan_id,
                        hybrid.terminal_outcome,
                        _now(),
                    )
                    break
                if intentional:
                    if args.canary_end_at is not None and not stop.is_set():
                        continue
                    if not stop.is_set() and _now() >= drain_deadline:
                        _record_terminal(
                            provider,
                            plan.plan_id,
                            (
                                "BLOCKED_GAP"
                                if hybrid.unresolved
                                else "SESSION_COMPLETE"
                            ),
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
                if _now() + timedelta(seconds=delay) >= provider_deadline:
                    _record_terminal(
                        provider,
                        plan.plan_id,
                        "SESSION_COMPLETE",
                        _now(),
                    )
                    break
                if stop.wait(delay):
                    break
            hybrid.close()
            terminal_report = {
                "status": "IDLE",
                "plan_id": str(plan.plan_id),
                "session_date": plan.session_date.isoformat(),
                "budgets": asdict(plan.budgets),
                "provider_requests": 0,
                "hybrid_repair_pending": hybrid.unresolved,
                "hybrid_repair_terminal_outcome": (
                    hybrid.terminal_outcome
                ),
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
