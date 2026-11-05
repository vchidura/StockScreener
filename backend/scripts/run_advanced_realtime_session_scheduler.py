#!/usr/bin/env python3
"""Run one authorized unattended Advanced Equity and Options session."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import load_dotenv
from requests import RequestException, Timeout


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import (  # noqa: E402
    PolygonHttpResponse,
    PolygonResponseTooLarge,
    RequestsPolygonHttpTransport,
)
from realtime.acquisition_ledger import (  # noqa: E402
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RequestCompleteness,
    finish_provider_request_cursor,
    load_raw_payload_cursor,
    load_recorded_provider_request_cursor,
    persist_raw_payload_cursor,
)
from realtime.advanced_options_p8_selection_source import (  # noqa: E402
    MassiveAdvancedOptionsExactContractSource,
)
from realtime.advanced_options_acquisition_runtime import (  # noqa: E402
    acquisition_binding_from_payload,
)
from realtime.advanced_options_control_plane import (  # noqa: E402
    load_advanced_options_equity_input_binding_cursor,
)
from realtime.advanced_options_p8_production import (  # noqa: E402
    build_advanced_options_p8_production_components,
    p8_options_cycle_cutoff_from_capture,
)
from realtime.advanced_options_read_pin import (  # noqa: E402
    PostgresAdvancedOptionsStoredReadPort,
)
from realtime.advanced_options_service import (  # noqa: E402
    AdvancedOptionsProcessKind,
)
from realtime.advanced_options_v3_authority import (  # noqa: E402
    session_acquisition_authorization_from_payload,
)
from realtime.advanced_options_p8_runtime import (  # noqa: E402
    LegacyArtifactSessionStateAdapter,
    SessionRuntimeStatePort,
    build_p8_downstream_binding,
    p8_preopen_session_binding_from_payload,
    validate_p8_execution_bundle,
)
from realtime.advanced_options_session_state import (  # noqa: E402
    LEGACY_DIAGNOSTIC_STATE_SOURCE,
    POSTGRES_STATE_SOURCE,
    select_options_session_state,
)
from realtime.advanced_options_v3_deployment import (  # noqa: E402
    validate_advanced_options_v3_runtime_identity,
)
from realtime.advanced_realtime_session_scheduler import (  # noqa: E402
    ADVANCED_SESSION_EQUITY_SYMBOLS,
    ADVANCED_SESSION_OPTIONS_LANE,
    ADVANCED_SESSION_SCHEDULER_VERSION,
    AdvancedRealtimeSessionAuthorization,
    AdvancedRealtimeSessionScheduler,
    AdvancedSessionDecisionWindow,
    AdvancedSessionProviderContract,
    SchedulerEquityReceipt,
    SchedulerOptionsCapture,
    SchedulerPublishedCycle,
    advanced_realtime_session_authorization_from_payload,
    advanced_realtime_session_authorization_payload,
    build_advanced_realtime_session_authorization,
    build_advanced_session_windows,
)
from realtime.advanced_realtime_session_operations import (  # noqa: E402
    ACQUISITION_SERVICES,
    DOWNSTREAM_SERVICES,
    AdvancedSessionOperation,
    DownstreamBuildSegment,
    build_diagnostic_authorization,
    build_downstream_transition,
    build_preopen_freeze_report,
    diagnostic_authorization_payload,
    downstream_transition_from_payload,
    downstream_transition_payload,
    evaluate_session_evidence,
    preopen_freeze_template,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from realtime.provider_budget import (  # noqa: E402
    MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING,
    MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING,
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)
from scripts import run_advanced_options_v3_production as production_cli  # noqa: E402
from scripts.run_advanced_options_v3_market_loop import (  # noqa: E402
    _current_read_pin,
    _portal_pointer,
    _rollback_post_publication,
    _validate_stored_readers,
)


UTC = timezone.utc
EQUITY_SERVICES = (
    "realtime-v2-advanced-controller",
    "realtime-v2-advanced-ingress",
    "realtime-v2-advanced-bridge",
    "realtime-v2-advanced-scheduler",
    "realtime-v2-advanced-finalizer",
    "realtime-v2-canonical-stock-alerts",
)
OPTIONS_RESIDENT_KINDS = (
    AdvancedOptionsProcessKind.INGRESS,
    AdvancedOptionsProcessKind.MARKET_STATE,
    AdvancedOptionsProcessKind.FLOW,
    AdvancedOptionsProcessKind.WINDOW,
)
OPTIONS_ACQUISITION_SERVICE = "realtime-v3-options-acquisition"
OPTIONS_SERVICES = (
    OPTIONS_ACQUISITION_SERVICE,
    "realtime-v3-options-p8-ingress",
    "realtime-v3-options-p8-market-state",
    "realtime-v3-options-p8-flow",
    "realtime-v3-options-p8-window",
)
PROVIDER_SERVICES = (
    "realtime-v2-advanced-controller",
    OPTIONS_ACQUISITION_SERVICE,
)
PREOPEN_HEALTH_OFFSET = timedelta(minutes=5)
PREOPEN_IDENTITY_OFFSET = timedelta(minutes=10)
PREOPEN_FINAL_GO_OFFSET = timedelta(minutes=15)
MONITOR_INTERVAL_SECONDS = 15
MAX_SERVICE_RESTARTS = 3
MAX_CONSECUTIVE_MONITOR_FAILURES = 3
COORDINATOR_IDENTITY_SCHEMA = (
    "advanced-realtime-v3-coordinator-identity-v1"
)
EXPECTED_SECRET_FILES = frozenset({
    "realtime_v2_app_dsn",
    "realtime_v2_api_dsn",
    "realtime_v2_provider_key",
})


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid JSON artifact: {path}") from error
    if not isinstance(value, dict):
        raise TypeError(f"JSON artifact must be an object: {path}")
    return value


def _write_json(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _cleanup_report_path(
    output_directory: Path,
    cleanup: dict[str, object],
) -> Path:
    if cleanup.get("status") == "PRESERVED_FOR_RESUME":
        return output_directory / (
            f"COORDINATOR-INTERRUPTED-{time.time_ns()}.json"
        )
    return output_directory / "CLEANUP.json"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _load_authority(path: Path) -> AdvancedRealtimeSessionAuthorization:
    return advanced_realtime_session_authorization_from_payload(
        _read_json(path)
    )


def _session_state(args) -> SessionRuntimeStatePort:
    state = getattr(args, "session_state", None)
    if state is not None:
        return state
    source = getattr(
        args,
        "state_source",
        LEGACY_DIAGNOSTIC_STATE_SOURCE,
    )
    if source == LEGACY_DIAGNOSTIC_STATE_SOURCE:
        return LegacyArtifactSessionStateAdapter(
            package_path=getattr(args, "package", None),
            bundle_path=getattr(args, "bundle", None),
            acquisition_binding_path=getattr(
                args,
                "acquisition_binding",
                None,
            ),
            downstream_binding_directory=getattr(
                args,
                "downstream_binding_directory",
                None,
            ),
            json_loader=_read_json,
            bundle_validator=validate_p8_execution_bundle,
            acquisition_parser=acquisition_binding_from_payload,
        )
    return select_options_session_state(
        environment=os.environ,
        provider=getattr(args, "state_provider", None),
        state_source=source,
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--authorization", type=Path, required=True)

    prepare = subparsers.add_parser("prepare-authorization")
    prepare.add_argument(
        "--authorization-reference",
        required=True,
    )
    prepare.add_argument(
        "--provider-authorization-reference",
        required=True,
    )
    prepare.add_argument("--expected-cost", required=True)
    prepare.add_argument(
        "--session-date",
        type=date.fromisoformat,
        required=True,
    )
    prepare.add_argument(
        "--prepared-at",
        type=datetime.fromisoformat,
        required=True,
    )
    prepare.add_argument(
        "--acquisition-start",
        type=datetime.fromisoformat,
        required=True,
    )
    prepare.add_argument(
        "--equity-authorization-id",
        type=UUID,
        required=True,
    )
    prepare.add_argument(
        "--equity-cohort-id",
        type=UUID,
        required=True,
    )
    prepare.add_argument(
        "--response-byte-ceiling",
        type=int,
        required=True,
    )
    prepare.add_argument("--package", type=Path, required=True)
    prepare.add_argument("--bundle", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--execute", action="store_true")

    freeze_template = subparsers.add_parser("freeze-template")
    freeze_template.add_argument(
        "--session-date",
        type=date.fromisoformat,
        required=True,
    )
    freeze_template.add_argument("--output", type=Path, required=True)
    freeze_template.add_argument("--execute", action="store_true")

    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--freeze-record", type=Path, required=True)
    freeze.add_argument("--output", type=Path, required=True)
    freeze.add_argument("--execute", action="store_true")

    diagnostic = subparsers.add_parser("diagnostic")
    diagnostic.add_argument(
        "--authorization-reference",
        required=True,
    )
    diagnostic.add_argument("--owner", required=True)
    diagnostic.add_argument(
        "--session-date",
        type=date.fromisoformat,
        required=True,
    )
    diagnostic.add_argument("--cohort-id", type=UUID, required=True)
    diagnostic.add_argument("--source-sha256", required=True)
    diagnostic.add_argument("--backend-image-id", required=True)
    diagnostic.add_argument(
        "--input-evidence",
        type=Path,
        required=True,
    )
    diagnostic.add_argument("--output", type=Path, required=True)
    diagnostic.add_argument("--execute", action="store_true")

    transition_contract = subparsers.add_parser(
        "transition-contract"
    )
    transition_contract.add_argument(
        "--authorization-reference",
        required=True,
    )
    transition_contract.add_argument(
        "--session-date",
        type=date.fromisoformat,
        required=True,
    )
    transition_contract.add_argument(
        "--completed-window-index",
        type=int,
        required=True,
    )
    transition_contract.add_argument(
        "--completed-cycle-report",
        type=Path,
        required=True,
    )
    transition_contract.add_argument(
        "--preserved-acquisition-identity-sha256",
        required=True,
    )
    transition_contract.add_argument(
        "--preserved-package-sha256",
        required=True,
    )
    transition_contract.add_argument(
        "--preserved-bundle-sha256",
        required=True,
    )
    transition_contract.add_argument(
        "--old-segment-id",
        required=True,
    )
    transition_contract.add_argument(
        "--old-source-sha256",
        required=True,
    )
    transition_contract.add_argument(
        "--old-backend-image-id",
        required=True,
    )
    transition_contract.add_argument(
        "--new-segment-id",
        required=True,
    )
    transition_contract.add_argument(
        "--new-source-sha256",
        required=True,
    )
    transition_contract.add_argument(
        "--new-backend-image-id",
        required=True,
    )
    transition_contract.add_argument(
        "--service",
        action="append",
        required=True,
    )
    transition_contract.add_argument(
        "--tier-a-qualification-sha256",
        required=True,
    )
    transition_contract.add_argument(
        "--tier-b-qualification-sha256",
        required=True,
    )
    transition_contract.add_argument(
        "--output",
        type=Path,
        required=True,
    )
    transition_contract.add_argument("--execute", action="store_true")

    transition = subparsers.add_parser("transition")
    transition.add_argument("--contract", type=Path, required=True)
    transition.add_argument(
        "--freeze-report",
        type=Path,
        required=True,
    )
    transition.add_argument(
        "--completed-cycle-report",
        type=Path,
        required=True,
    )
    transition.add_argument("--package", type=Path, required=True)
    transition.add_argument("--bundle", type=Path, required=True)
    transition.add_argument(
        "--docker-executable",
        type=Path,
        required=True,
    )
    transition.add_argument("--output", type=Path, required=True)
    transition.add_argument("--execute", action="store_true")

    status = subparsers.add_parser("status")
    status.add_argument("--evidence", type=Path, required=True)
    status.add_argument("--output", type=Path, required=True)
    status.add_argument("--execute", action="store_true")

    runtime_status = subparsers.add_parser("runtime-status")
    runtime_status.add_argument(
        "--output-directory",
        type=Path,
        required=True,
    )

    run = subparsers.add_parser("run")
    run.add_argument("--database", required=True)
    run.add_argument("--confirm-database-name", required=True)
    run.add_argument("--authorization", type=Path, required=True)
    run.add_argument(
        "--state-source",
        choices=(
            POSTGRES_STATE_SOURCE,
            LEGACY_DIAGNOSTIC_STATE_SOURCE,
        ),
        default=POSTGRES_STATE_SOURCE,
    )
    run.add_argument("--package", type=Path)
    run.add_argument("--bundle", type=Path)
    run.add_argument(
        "--acquisition-binding",
        type=Path,
    )
    run.add_argument("--freeze-report", type=Path, required=True)
    run.add_argument("--env-file", type=Path, required=True)
    run.add_argument(
        "--secret-directory",
        type=Path,
        required=True,
    )
    run.add_argument("--docker-executable", type=Path, required=True)
    run.add_argument("--output-directory", type=Path, required=True)
    run.add_argument("--poll-seconds", type=float, default=1)
    run.add_argument(
        "--monitor-seconds",
        type=int,
        default=MONITOR_INTERVAL_SECONDS,
    )
    run.add_argument("--continuous", action="store_true")
    run.add_argument("--resume", action="store_true")
    run.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in (
        "authorization",
        "freeze_record",
        "freeze_report",
        "input_evidence",
        "contract",
        "completed_cycle_report",
        "evidence",
        "output",
        "env_file",
        "package",
        "bundle",
        "acquisition_binding",
        "secret_directory",
        "docker_executable",
        "output_directory",
    ):
        value = getattr(args, name, None)
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    if hasattr(args, "session_date"):
        try:
            build_advanced_session_windows(args.session_date)
        except ValueError as exc:
            parser.error(f"--session-date must be an XNYS session: {exc}")
    if args.command in {
        "freeze-template",
        "freeze",
        "prepare-authorization",
        "diagnostic",
        "transition-contract",
        "status",
    }:
        if not args.execute or args.output.exists():
            parser.error(
                f"{args.command} requires a new output path and --execute"
            )
    if args.command == "freeze" and not args.freeze_record.is_file():
        parser.error("--freeze-record must be an existing file")
    if args.command == "prepare-authorization":
        if (
            args.response_byte_ceiling <= 0
            or not args.package.is_file()
            or not args.bundle.is_file()
        ):
            parser.error(
                "scheduler authorization preparation scope is invalid"
            )
    if args.command == "diagnostic":
        if not args.input_evidence.is_file():
            parser.error(
                "diagnostic requires owned input evidence"
            )
    if args.command == "transition-contract":
        if (
            not args.completed_cycle_report.is_file()
        ):
            parser.error(
                "transition contract requires one completed cycle report"
            )
    if args.command == "status" and not args.evidence.is_file():
        parser.error("--evidence must be an existing file")
    if (
        args.command == "runtime-status"
        and not args.output_directory.is_dir()
    ):
        parser.error("--output-directory must be an existing directory")
    if args.command == "transition":
        if (
            not args.execute
            or args.output.exists()
            or not all(
                path.is_file()
                for path in (
                    args.contract,
                    args.freeze_report,
                    args.completed_cycle_report,
                    args.package,
                    args.bundle,
                    args.docker_executable,
                )
            )
        ):
            parser.error(
                "transition requires existing exact artifacts, a new "
                "output path and --execute"
            )
        args.transition_contract = (
            downstream_transition_from_payload(
                _read_json(args.contract)
            )
        )
    if args.command in {"plan", "run"}:
        authority = _load_authority(args.authorization)
        args.scheduler_authority = authority
    if args.command == "run":
        if (
            args.database != args.confirm_database_name
            or args.database != "stocks_realtime_v2"
            or args.poll_seconds <= 0
            or args.monitor_seconds != MONITOR_INTERVAL_SECONDS
            or not args.continuous
            or not args.execute
            or (
                args.output_directory.exists()
                and (
                    not args.resume
                    or not args.output_directory.is_dir()
                )
            )
            or (
                args.state_source == POSTGRES_STATE_SOURCE
                and any(
                    path is not None
                    for path in (
                        args.package,
                        args.bundle,
                        args.acquisition_binding,
                    )
                )
            )
            or (
                args.state_source == LEGACY_DIAGNOSTIC_STATE_SOURCE
                and not all(
                    path is not None and path.is_file()
                    for path in (
                        args.package,
                        args.bundle,
                        args.acquisition_binding,
                    )
                )
            )
            or not args.secret_directory.is_dir()
            or not args.freeze_report.is_file()
            or not args.env_file.is_file()
        ):
            parser.error(
                "session scheduler requires exact target database, a new "
                "output directory (or owned --resume directory) and "
                "--continuous --execute"
            )
    return args


def _plan(authority: AdvancedRealtimeSessionAuthorization):
    return {
        "contract": ADVANCED_SESSION_SCHEDULER_VERSION,
        "mode": "PROVIDER_FREE_PLAN",
        "status": "ACTIVATION_AND_BUDGET_GRANT_REQUIRED",
        "authorization_id": str(authority.authorization_id),
        "session_date": authority.session_date.isoformat(),
        "acquisition_start_utc": (
            authority.acquisition_start.isoformat()
        ),
        "market_capture_stop_utc": (
            authority.market_capture_stop.isoformat()
        ),
        "decision_stop_utc": authority.decision_stop.isoformat(),
        "equity_symbols": 9,
        "options_underlyings": ["QQQ", "SPY"],
        "options_contracts": list(
            authority.provider_contract.contract_tickers
        ),
        "decision_cycles": len(authority.windows),
        "options_rest_requests": (
            authority.provider_contract.budget.request_ceiling
        ),
        "options_rest_endpoint": (
            authority.provider_contract.endpoint_path
        ),
        "provider_effects": 0,
        "production_effects": 0,
        "host_schedule_installed": False,
    }


def _prepare_authorization(args):
    state = _session_state(args)
    package = state.load_package()
    bundle = state.load_execution_bundle()
    session_payload = bundle.get("v3_authorities", {}).get(
        "session_acquisition"
    )
    session = session_acquisition_authorization_from_payload(
        session_payload
    )
    package_sha256 = package.get("package_sha256")
    bundle_sha256 = bundle.get("bundle_sha256")
    if (
        not isinstance(package_sha256, str)
        or not isinstance(bundle_sha256, str)
        or session.session_date != args.session_date
    ):
        raise ValueError(
            "scheduler package or session identity is invalid"
        )
    windows = build_advanced_session_windows(args.session_date)
    decision_stop = windows[-1].latest_options_cutoff
    provider_contract = AdvancedSessionProviderContract(
        budget=ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=args.session_date,
            authorization_reference=(
                args.authorization_reference
            ),
            provider_authorization_reference=(
                args.provider_authorization_reference
            ),
            expected_cost=args.expected_cost,
            request_ceiling=len(windows),
            decoded_byte_ceiling=(
                len(windows) * args.response_byte_ceiling
            ),
            account_request_ceiling=(
                MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING
            ),
            account_decoded_byte_ceiling=(
                MASSIVE_ADVANCED_ACCOUNT_DECODED_BYTE_CEILING
            ),
            lanes=frozenset({ADVANCED_SESSION_OPTIONS_LANE}),
            authorized_until=decision_stop,
        ),
        response_byte_ceiling=args.response_byte_ceiling,
        contract_tickers=tuple(
            str(ticker)
            for ticker in package.get(
                "v3_admitted_contracts",
                (),
            )
        ),
    )
    authority = build_advanced_realtime_session_authorization(
        authorization_reference=args.authorization_reference,
        session_date=args.session_date,
        prepared_at=args.prepared_at,
        acquisition_start=args.acquisition_start,
        equity_authorization_id=args.equity_authorization_id,
        equity_cohort_id=args.equity_cohort_id,
        options_session_authority=session,
        package_sha256=package_sha256,
        bundle_sha256=bundle_sha256,
        provider_contract=provider_contract,
    )
    payload = advanced_realtime_session_authorization_payload(
        authority
    )
    _write_json(args.output, payload)
    return {
        "status": "SCHEDULER_AUTHORIZATION_PREPARED",
        "authorization_id": str(authority.authorization_id),
        "authorization_sha256": authority.authorization_sha256,
        "provider_contract_sha256": provider_contract.sha256,
        "package_sha256": package_sha256,
        "bundle_sha256": bundle_sha256,
        "options_rest_request_ceiling": len(windows),
        "options_rest_decoded_byte_ceiling": (
            provider_contract.budget.decoded_byte_ceiling
        ),
        "provider_effects": 0,
        "production_effects": 0,
        "output": str(args.output),
    }


class _RecordingTransport:
    def __init__(self, inner) -> None:
        self._inner = inner
        self.response: PolygonHttpResponse | None = None

    def get(self, url, params):
        self.response = self._inner.get(url, params)
        return self.response


class _StaticTransport:
    def __init__(self, response: PolygonHttpResponse) -> None:
        self._response = response

    def get(self, _url, _params):
        return self._response


class _ProductionPort:
    def __init__(
        self,
        *,
        args,
        authority: AdvancedRealtimeSessionAuthorization,
        provider,
        output_directory: Path,
    ) -> None:
        self._args = args
        self._authority = authority
        self._provider = provider
        self._output = output_directory
        self._selection_captures = {}
        self._session_state = _session_state(args)
        bundle = self._session_state.load_bundle_document()
        self._session_binding = (
            self._session_state.load_preopen_session_binding(bundle)
        )

    def _report_path(
        self,
        window: AdvancedSessionDecisionWindow,
    ) -> Path:
        return self._output / f"{window.key}.json"

    def existing_cycle(self, window):
        path = self._report_path(window)
        if not path.exists():
            return None
        value = _read_json(path)
        if (
            value.get("status") != "COMPLETE_PUBLISHED_PINNED"
            or value.get("window", {}).get("equity_cutoff")
            != window.equity_cutoff.isoformat()
            or value.get("semantic_sha256")
            != canonical_sha256({
                key: item
                for key, item in value.items()
                if key != "semantic_sha256"
            })
        ):
            raise RuntimeError(
                "stored scheduler cycle report identity changed"
            )
        return SchedulerPublishedCycle(
            target_window_id=UUID(str(value["target_window_id"])),
            release_id=UUID(str(value["release_id"])),
            release_cutoff=datetime.fromisoformat(
                str(value["release_cutoff"])
            ),
            read_pin_release_id=UUID(
                str(value["read_pin_release_id"])
            ),
            stored_reader_count=int(value["stored_reader_count"]),
            stock_alert_batch_id=UUID(
                str(value["stock_alert_batch_id"])
            ),
            stock_alert_batch_sha256=str(
                value["stock_alert_batch_sha256"]
            ),
            stock_alert_evaluation_count=int(
                value["stock_alert_evaluation_count"]
            ),
            stock_alert_unavailable_count=int(
                value["stock_alert_unavailable_count"]
            ),
            stock_alert_reason_codes_exact=(
                value["stock_alert_reason_codes_exact"] is True
            ),
            expected_stored_reader_count=int(
                value.get("expected_stored_reader_count", 4)
            ),
        )

    def load_equity_receipt(self, window):
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT receipt.receipt_id::text,receipt.cutoff,"
                "receipt.created_at,receipt.input_sha256 "
                "FROM rt.cycle_receipts AS receipt "
                "JOIN rt.window_runs AS window_run "
                "ON window_run.window_id=receipt.window_id "
                "WHERE window_run.cohort_id=%s "
                "AND receipt.receipt_kind='INPUT_READY' "
                "AND receipt.cutoff=%s "
                "AND EXISTS (SELECT 1 FROM rt.stage_runs AS stage "
                "WHERE stage.window_id=receipt.window_id "
                "AND stage.stage_id='EQUITY_MATERIALIZE' "
                "AND stage.state='COMPLETE') "
                "ORDER BY receipt.created_at DESC LIMIT 1",
                (
                    str(self._authority.equity_cohort_id),
                    window.equity_cutoff,
                ),
            )
            row = unit.cursor.fetchone()
        if row is None:
            return None
        return SchedulerEquityReceipt(
            receipt_id=UUID(str(row[0])),
            cutoff=row[1],
            created_at=row[2],
            input_sha256=str(row[3]),
        )

    def _reservation(
        self,
        window: AdvancedSessionDecisionWindow,
        reserved_at: datetime,
    ) -> ProviderRequestReservation:
        provider_contract = self._authority.provider_contract
        request_sha256 = canonical_sha256({
            "schema": provider_contract.policy_version,
            "authorization_id": str(self._authority.authorization_id),
            "equity_cutoff": window.equity_cutoff,
            "endpoint": provider_contract.endpoint_path,
            "contracts": provider_contract.contract_tickers,
        })
        return ProviderRequestReservation(
            request_id=uuid5(
                NAMESPACE_URL,
                (
                    "stockscreener:advanced-session-options-refresh:"
                    f"{self._authority.authorization_id}:"
                    f"{window.equity_cutoff.isoformat()}"
                ),
            ),
            account_alias=provider_contract.budget.account_alias,
            lane=next(iter(provider_contract.budget.lanes)),
            endpoint_class="OPTIONS_UNIFIED_SNAPSHOT",
            request_sha256=request_sha256,
            reserved_at=reserved_at,
            cohort_id=self._authority.options_cohort_id,
        )

    def _finish_failed_request(
        self,
        request_id: UUID,
        *,
        started_at: datetime,
        error: Exception,
    ) -> None:
        outcome = (
            ProviderRequestOutcome.TIMEOUT
            if isinstance(error, Timeout)
            else ProviderRequestOutcome.INVALID
        )
        with self._provider.transaction(APP_ROLE) as unit:
            finish_provider_request_cursor(
                unit.cursor,
                request_id,
                ProviderRequestCompletion(
                    state=ProviderRequestState.FAILED,
                    completed_at=datetime.now(UTC),
                    started_at=started_at,
                    outcome=outcome,
                    completeness=RequestCompleteness.INCOMPLETE,
                    failure_code=type(error).__name__[:96],
                    price_basis=PriceBasis.NOT_APPLICABLE,
                ),
            )

    def capture_options(self, window, receipt):
        checked_at = datetime.now(UTC)
        reservation = self._reservation(window, checked_at)
        contract = self._authority.provider_contract
        with self._provider.transaction(APP_ROLE) as unit:
            admission = reserve_coordinated_provider_request_cursor(
                unit.cursor,
                contract.budget,
                reservation,
                reserved_response_bytes=contract.response_byte_ceiling,
                account_wide=True,
            )
            recorded = (
                None
                if admission.newly_reserved
                else load_recorded_provider_request_cursor(
                    unit.cursor,
                    reservation.request_id,
                )
            )
            if not admission.newly_reserved:
                if (
                    recorded is None
                    or recorded.completion.state
                    is not ProviderRequestState.COMPLETE
                    or recorded.completion.completeness
                    is not RequestCompleteness.COMPLETE
                    or recorded.completion.payload_id is None
                ):
                    raise RuntimeError(
                        "exact Options refresh is not replayable"
                    )
                body = load_raw_payload_cursor(
                    unit.cursor,
                    recorded.completion.payload_id,
                )
                checked_at = recorded.completion.completed_at
        if not admission.newly_reserved:
            source = MassiveAdvancedOptionsExactContractSource(
                _StaticTransport(PolygonHttpResponse(200, body)),
                maximum_decoded_bytes=contract.response_byte_ceiling,
                clock=lambda: checked_at,
            )
            capture = source.capture(
                contract_tickers=contract.contract_tickers,
                reviewed_at=checked_at,
                session_date=self._authority.session_date,
            )
        else:
            api_key = str(os.environ.get("POLYGON_API_KEY") or "").strip()
            if not api_key:
                error = RuntimeError("POLYGON_API_KEY is required")
                self._finish_failed_request(
                    reservation.request_id,
                    started_at=checked_at,
                    error=error,
                )
                raise error
            transport = _RecordingTransport(
                RequestsPolygonHttpTransport(
                    api_key,
                    maximum_response_bytes=(
                        contract.response_byte_ceiling
                    ),
                )
            )
            source = MassiveAdvancedOptionsExactContractSource(
                transport,
                maximum_decoded_bytes=contract.response_byte_ceiling,
                clock=lambda: datetime.now(UTC),
            )
            try:
                capture = source.capture(
                    contract_tickers=contract.contract_tickers,
                    reviewed_at=checked_at,
                    session_date=self._authority.session_date,
                )
            except (
                PolygonResponseTooLarge,
                RequestException,
                RuntimeError,
                TypeError,
                ValueError,
            ) as error:
                self._finish_failed_request(
                    reservation.request_id,
                    started_at=checked_at,
                    error=error,
                )
                raise
            response = transport.response
            if response is None:
                raise RuntimeError(
                    "exact Options refresh response is unavailable"
                )
            completed_at = datetime.now(UTC)
            with self._provider.transaction(APP_ROLE) as unit:
                payload_id = persist_raw_payload_cursor(
                    unit.cursor,
                    response.body,
                    arrived_at=completed_at,
                    request_id=reservation.request_id,
                )
                finish_provider_request_cursor(
                    unit.cursor,
                    reservation.request_id,
                    ProviderRequestCompletion(
                        state=ProviderRequestState.COMPLETE,
                        completed_at=completed_at,
                        started_at=checked_at,
                        outcome=ProviderRequestOutcome.OK,
                        http_status=response.status_code,
                        response_bytes=len(response.body),
                        row_count=len(capture.selection.contracts),
                        pages_fetched=1,
                        requested_from=window.equity_cutoff,
                        requested_to=capture.selection.reviewed_at,
                        price_basis=PriceBasis.NOT_APPLICABLE,
                        completeness=RequestCompleteness.COMPLETE,
                        payload_id=payload_id,
                    ),
                )
        evidence_sha256 = str(
            capture.evidence["selection_sha256"]
        )
        self._selection_captures[evidence_sha256] = capture
        options_cutoff = p8_options_cycle_cutoff_from_capture(
            capture,
            checked_at=capture.selection.reviewed_at,
            stop=self._authority.decision_stop,
        )
        return SchedulerOptionsCapture(
            evidence_sha256=evidence_sha256,
            reviewed_at=capture.selection.reviewed_at,
            contract_tickers=tuple(capture.selection.contracts),
            request_count=capture.request_count,
            pagination_requests=capture.pagination_requests,
            options_cutoff=options_cutoff,
        )

    def _attach_downstream_binding(
        self,
        *,
        window,
        receipt,
        capture,
        authorized,
        target_window_id,
    ):
        if self._session_binding is None:
            return None
        selection_capture = self._selection_captures.get(
            capture.evidence_sha256
        )
        if selection_capture is None:
            raise RuntimeError(
                "exact Options capture is unavailable for E attachment"
            )
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            equity_input = (
                load_advanced_options_equity_input_binding_cursor(
                    unit.cursor,
                    target_window_id,
                )
            )
        if (
            equity_input is None
            or equity_input.equity_receipt_id != receipt.receipt_id
            or equity_input.input_sha256 != receipt.input_sha256
            or equity_input.equity_cutoff != window.equity_cutoff
        ):
            raise RuntimeError(
                "per-E Equity input binding is unavailable"
            )
        package = self._session_state.load_package()
        components = None
        try:
            components = build_advanced_options_p8_production_components(
                package=package,
                repository_root=REPOSITORY_ROOT,
                stored_reads_authorized=False,
                provider_free_selection=True,
            )
            _observation, binding = (
                components.capture.capture_from_selection(
                    package,
                    checked_at=capture.reviewed_at,
                    selection_capture=selection_capture,
                    equity_input_binding=equity_input,
                    target_window_id=target_window_id,
                )
            )
            production_binding = (
                components.capture.production_binding_payload
            )
            if (
                production_binding is None
                or binding.window_id != target_window_id
                or binding.equity_input != equity_input
            ):
                raise RuntimeError(
                    "per-E P8 production binding is incomplete"
                )
            with self._provider.transaction(APP_ROLE) as unit:
                components.capture.persist_selection_cursor(
                    unit.cursor,
                    binding,
                )
        finally:
            if components is not None:
                components.close()
        latest = self._session_state.load_latest_downstream_binding(
            self._session_binding
        )
        downstream = build_p8_downstream_binding(
            session_binding=self._session_binding,
            sequence=(1 if latest is None else latest.sequence + 1),
            previous_binding_sha256=(
                None if latest is None else latest.binding_sha256
            ),
            target_window_id=target_window_id,
            equity_receipt_id=receipt.receipt_id,
            equity_input_sha256=receipt.input_sha256,
            finalized_cutoff=receipt.cutoff,
            receipt_created_at=receipt.created_at,
            options_cutoff=capture.options_cutoff,
            history_binding_sha256=(
                binding.history.binding_sha256
            ),
            equity_binding_sha256=equity_input.binding_sha256,
            options_activity_evidence_sha256=(
                capture.evidence_sha256
            ),
            window_cycle_authorization_sha256=str(
                authorized[
                    "window_cycle_authorization_sha256"
                ]
            ),
            publication_authorization_reference=str(
                authorized[
                    "publication_authorization_reference"
                ]
            ),
            production_binding=production_binding,
        )
        attached = self._session_state.attach_downstream_binding(
            self._session_binding,
            downstream,
        )
        self._selection_captures.pop(
            capture.evidence_sha256,
            None,
        )
        return attached

    def _canonical_stock_alert_evidence(self, window, receipt):
        while datetime.now(UTC) <= window.latest_options_cutoff:
            with self._provider.transaction(
                APP_ROLE,
                read_only=True,
            ) as unit:
                unit.cursor.execute(
                    "SELECT batch_id::text,batch_sha256,"
                    "evaluation_count,unavailable_count,error_count "
                    "FROM rt.canonical_stock_alert_batches "
                    "WHERE input_receipt_id=%s AND cohort_id=%s "
                    "AND cutoff=%s",
                    (
                        str(receipt.receipt_id),
                        str(self._authority.equity_cohort_id),
                        window.equity_cutoff,
                    ),
                )
                batches = unit.cursor.fetchall()
                if len(batches) == 1:
                    unit.cursor.execute(
                        "SELECT ticker,disposition,reason_codes "
                        "FROM rt.canonical_stock_alert_evaluations "
                        "WHERE batch_id=%s ORDER BY ticker",
                        (batches[0][0],),
                    )
                    evaluations = unit.cursor.fetchall()
                else:
                    evaluations = []
            if not batches:
                threading.Event().wait(self._args.poll_seconds)
                continue
            if len(batches) != 1:
                raise RuntimeError(
                    "canonical Stock Alert batch is not exact"
                )
            tickers = tuple(str(row[0]) for row in evaluations)
            reason_codes_exact = all(
                str(row[1]) == "UNAVAILABLE"
                and set(row[2])
                == {
                    "MISSING_SECTOR_CONTEXT",
                    "MISSING_EVENT_CONTEXT",
                }
                for row in evaluations
            )
            if (
                int(batches[0][2]) != 9
                or int(batches[0][3]) != 9
                or int(batches[0][4]) != 0
                or len(evaluations) != 9
                or tickers
                != tuple(sorted(ADVANCED_SESSION_EQUITY_SYMBOLS))
                or not reason_codes_exact
            ):
                raise RuntimeError(
                    "canonical Stock Alert evidence is not exact"
                )
            return {
                "batch_id": UUID(str(batches[0][0])),
                "batch_sha256": str(batches[0][1]),
                "evaluation_count": int(batches[0][2]),
                "unavailable_count": int(batches[0][3]),
                "reason_codes_exact": True,
            }
        raise RuntimeError(
            "canonical Stock Alert batch missed the E/O deadline"
        )

    def run_cycle(self, window, receipt, capture):
        previous_pointer = _portal_pointer(
            self._provider,
            self._authority.options_cohort_id,
        )
        previous_pin = _current_read_pin(
            self._provider,
            self._authority.options_cohort_id,
        )
        options_cutoff = capture.options_cutoff
        if options_cutoff > window.latest_options_cutoff:
            raise RuntimeError("Options cutoff exceeds E/O deadline")
        authorize_args = SimpleNamespace(
            package=self._args.package,
            bundle=self._args.bundle,
            window_key=window.key,
            options_cutoff=options_cutoff,
            window_deadline=options_cutoff + timedelta(seconds=300),
            equity_receipt_id=receipt.receipt_id,
            authorization_reference=(
                f"{self._authority.authorization_reference}:"
                f"{window.key}"
            ),
            publication_authorization_reference=(
                f"{self._authority.authorization_reference}:"
                f"{window.key}:PUBLICATION"
            ),
            activity_evidence_sha256=capture.evidence_sha256,
        )
        authorized = production_cli._authorize_window(authorize_args)
        target_window_id = UUID(str(authorized["window_id"]))
        self._attach_downstream_binding(
            window=window,
            receipt=receipt,
            capture=capture,
            authorized=authorized,
            target_window_id=target_window_id,
        )
        deadline = min(
            authorize_args.window_deadline,
            window.latest_options_cutoff,
        )
        status_args = SimpleNamespace(
            package=self._args.package,
            bundle=self._args.bundle,
        )
        while datetime.now(UTC) <= deadline:
            status = production_cli._status(status_args)["session"]
            target = next(
                (
                    row
                    for row in status["windows"]
                    if row["target_window_id"]
                    == str(target_window_id)
                ),
                None,
            )
            if (
                target is not None
                and target["state"] == "COMPLETE"
                and target["publication_count"] == 1
            ):
                break
            threading.Event().wait(self._args.poll_seconds)
        else:
            raise RuntimeError("Options WINDOW completion deadline expired")
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
        ) as unit:
            unit.cursor.execute(
                "SELECT release.release_id::text,release.cutoff "
                "FROM rt.portal_releases AS release "
                "JOIN rt.cycle_receipts AS receipt "
                "ON receipt.receipt_id=release.receipt_id "
                "WHERE receipt.window_id=%s",
                (str(target_window_id),),
            )
            releases = unit.cursor.fetchall()
        if len(releases) != 1:
            raise RuntimeError(
                "Options window publication release is not exact"
            )
        release_id = UUID(str(releases[0][0]))
        release_cutoff = releases[0][1]
        pointer = _portal_pointer(
            self._provider,
            self._authority.options_cohort_id,
        )
        if pointer is None or pointer.release_id != release_id:
            raise RuntimeError(
                "canonical publication pointer did not select the cycle"
            )
        read_port = PostgresAdvancedOptionsStoredReadPort(
            self._provider,
            package_sha256=self._authority.package_sha256,
        )
        try:
            pinned = read_port.advance(
                cohort_id=(
                    self._authority.options_cohort_id
                ),
                release_id=release_id,
                previous=previous_pin,
            )
            readers = _validate_stored_readers(
                self._provider,
                self._authority.options_cohort_id,
                release_id,
            )
        except (
            RuntimeError,
            TypeError,
            ValueError,
        ):
            _rollback_post_publication(
                self._provider,
                read_port,
                cohort_id=(
                    self._authority.options_cohort_id
                ),
                published_release_id=release_id,
                prior_pointer=previous_pointer,
                prior_read_pin=previous_pin,
            )
            raise
        stock_alert = self._canonical_stock_alert_evidence(
            window,
            receipt,
        )
        return SchedulerPublishedCycle(
            target_window_id=target_window_id,
            release_id=release_id,
            release_cutoff=release_cutoff,
            read_pin_release_id=UUID(str(pinned["release_id"])),
            stored_reader_count=len(readers["responses"]),
            stock_alert_batch_id=stock_alert["batch_id"],
            stock_alert_batch_sha256=stock_alert["batch_sha256"],
            stock_alert_evaluation_count=(
                stock_alert["evaluation_count"]
            ),
            stock_alert_unavailable_count=(
                stock_alert["unavailable_count"]
            ),
            stock_alert_reason_codes_exact=(
                stock_alert["reason_codes_exact"]
            ),
        )


def _compose_command(
    args,
    *values,
    check=True,
    backend_image_id=None,
):
    state_source = getattr(
        args,
        "state_source",
        LEGACY_DIAGNOSTIC_STATE_SOURCE,
    )
    runtime_environment = {
        "REALTIME_V3_OPTIONS_STATE_SOURCE": state_source,
    }
    acquisition_binding = getattr(args, "acquisition_binding", None)
    if (
        state_source == LEGACY_DIAGNOSTIC_STATE_SOURCE
        and acquisition_binding is not None
    ):
        runtime_environment[
            "REALTIME_V3_OPTIONS_ACQUISITION_BINDING_PATH"
        ] = str(acquisition_binding)
    secret_directory = getattr(args, "secret_directory", None)
    if secret_directory is not None:
        runtime_environment["REALTIME_V2_DOCKER_SECRET_DIR"] = str(
            secret_directory
        )
    command = (
        str(args.docker_executable),
        "compose",
        "--file",
        str(REPOSITORY_ROOT / "docker-compose.realtime-v2.yml"),
        "--profile",
        "realtime-v2-advanced",
        "--profile",
        "realtime-v3-options",
        "--profile",
        "realtime-v3-options-acquisition",
        *values,
    )
    if state_source == LEGACY_DIAGNOSTIC_STATE_SOURCE:
        runtime_environment.update({
            "REALTIME_V2_P8_PACKAGE_PATH": str(args.package),
            "REALTIME_V2_P8_BUNDLE_PATH": str(args.bundle),
        })
    completed = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env={
            **os.environ,
            **(
                {"REALTIME_V2_BACKEND_IMAGE": backend_image_id}
                if backend_image_id is not None
                else {}
            ),
            **runtime_environment,
        },
        check=False,
        timeout=180,
        capture_output=True,
    )
    if check and completed.returncode:
        raise RuntimeError(
            f"Docker Compose failed with {completed.returncode}"
        )
    return completed


def _validate_semantic_report(
    value: dict[str, object],
    *,
    name: str,
) -> str:
    document = dict(value)
    semantic_sha256 = document.pop("semantic_sha256", None)
    if (
        not isinstance(semantic_sha256, str)
        or canonical_sha256(document) != semantic_sha256
    ):
        raise ValueError(f"{name} semantic identity changed")
    return semantic_sha256


def _load_go_freeze_report(path: Path) -> dict[str, object]:
    report = _read_json(path)
    _validate_semantic_report(report, name="pre-open freeze report")
    if (
        report.get("decision") != "GO"
        or report.get("provider_effects") != 0
        or report.get("production_effects") != 0
        or report.get("failed_fields") != []
    ):
        raise ValueError("pre-open freeze decision is NO_GO")
    return report


def _container_state(
    args,
    services,
    *,
    backend_image_id,
):
    service_ids = {}
    for service in services:
        completed = _compose_command(
            args,
            "ps",
            "--all",
            "--quiet",
            service,
            backend_image_id=backend_image_id,
        )
        identifiers = completed.stdout.decode(
            "utf-8",
            errors="replace",
        ).split()
        if len(identifiers) != 1:
            raise RuntimeError(
                f"expected one owned container for {service}"
            )
        service_ids[service] = identifiers[0]
    command = (
        str(args.docker_executable),
        "inspect",
        "--format",
        "{{.Id}}\t{{.Image}}\t{{.State.Status}}",
        *service_ids.values(),
    )
    completed = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        check=False,
        timeout=60,
        capture_output=True,
    )
    if completed.returncode:
        raise RuntimeError(
            f"Docker inspect failed with {completed.returncode}"
        )
    inspected = {}
    for line in completed.stdout.decode(
        "utf-8",
        errors="replace",
    ).splitlines():
        values = line.split("\t")
        if len(values) != 3:
            raise RuntimeError("Docker inspect output changed")
        inspected[values[0]] = {
            "container_id": values[0],
            "image_id": values[1],
            "state": values[2],
        }
    state = {}
    for service, identifier in service_ids.items():
        row = inspected.get(identifier)
        if row is None or row["state"] != "running":
            raise RuntimeError(
                f"owned container is not running for {service}"
            )
        state[service] = row
    return state


def _assert_local_image(args, image_id):
    completed = subprocess.run(
        (
            str(args.docker_executable),
            "image",
            "inspect",
            image_id,
            "--format",
            "{{.Id}}",
        ),
        cwd=REPOSITORY_ROOT,
        check=False,
        timeout=60,
        capture_output=True,
    )
    if (
        completed.returncode
        or completed.stdout.decode(
            "utf-8",
            errors="replace",
        ).strip()
        != image_id
    ):
        raise RuntimeError("new downstream image identity is unavailable")


def _run_transition(args):
    transition = args.transition_contract
    freeze_report = _load_go_freeze_report(args.freeze_report)
    if (
        freeze_report.get("freeze_sha256")
        != transition.preserved_acquisition_identity_sha256
    ):
        raise ValueError(
            "transition acquisition identity differs from the freeze"
        )
    state = _session_state(args)
    package = state.load_package()
    bundle = state.load_bundle_document()
    if (
        package.get("package_sha256")
        != transition.preserved_package_sha256
        or bundle.get("bundle_sha256")
        != transition.preserved_bundle_sha256
    ):
        raise ValueError(
            "transition package or bundle identity changed"
        )
    cycle = _read_json(args.completed_cycle_report)
    cycle_sha256 = _validate_semantic_report(
        cycle,
        name="completed cycle report",
    )
    if (
        cycle_sha256
        != transition.completed_cycle_report_sha256
        or cycle.get("status") != "COMPLETE_PUBLISHED_PINNED"
        or not isinstance(cycle.get("window"), dict)
        or cycle["window"].get("index")
        != transition.completed_window_index
    ):
        raise ValueError(
            "transition is not bound to a completed cycle boundary"
        )
    if any(
        service in ACQUISITION_SERVICES
        or service not in DOWNSTREAM_SERVICES
        for service in transition.services
    ):
        raise ValueError("transition contains a forbidden service")
    old_image = transition.old_segment.backend_image_id
    new_image = transition.new_segment.backend_image_id
    _assert_local_image(args, new_image)
    acquisition_before = _container_state(
        args,
        ACQUISITION_SERVICES,
        backend_image_id=old_image,
    )
    downstream_before = _container_state(
        args,
        transition.services,
        backend_image_id=old_image,
    )
    if any(
        row["image_id"] != old_image
        for row in downstream_before.values()
    ):
        raise RuntimeError(
            "old downstream role image does not match the contract"
        )
    _compose_command(
        args,
        "up",
        "--detach",
        "--no-deps",
        "--no-build",
        "--force-recreate",
        "--wait",
        *transition.services,
        backend_image_id=new_image,
    )
    acquisition_after = _container_state(
        args,
        ACQUISITION_SERVICES,
        backend_image_id=new_image,
    )
    downstream_after = _container_state(
        args,
        transition.services,
        backend_image_id=new_image,
    )
    if acquisition_after != acquisition_before:
        raise RuntimeError(
            "acquisition containers changed during downstream transition"
        )
    if any(
        row["image_id"] != new_image
        for row in downstream_after.values()
    ):
        raise RuntimeError(
            "new downstream roles do not use the contracted image"
        )
    report = {
        "contract": transition.transition_sha256,
        "operation": (
            AdvancedSessionOperation
            .CONTROLLED_DOWNSTREAM_TRANSITION.value
        ),
        "status": "COMPLETE",
        "transition_id": str(transition.transition_id),
        "completed_window_index": (
            transition.completed_window_index
        ),
        "old_segment_id": transition.old_segment.segment_id,
        "new_segment_id": transition.new_segment.segment_id,
        "old_image_id": old_image,
        "new_image_id": new_image,
        "preserved_acquisition_identity_sha256": (
            transition.preserved_acquisition_identity_sha256
        ),
        "preserved_package_sha256": (
            transition.preserved_package_sha256
        ),
        "preserved_bundle_sha256": (
            transition.preserved_bundle_sha256
        ),
        "acquisition_containers_preserved": True,
        "cycle_backdated": False,
        "acquisition_before": acquisition_before,
        "acquisition_after": acquisition_after,
        "downstream_before": downstream_before,
        "downstream_after": downstream_after,
        "effects": {
            "provider_rest": 0,
            "provider_websocket": 0,
            "acquisition_restart": 0,
            "journal_change": 0,
            "normalizer_change": 0,
            "finalizer_change": 0,
            "budget_change": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    _write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _preopen_gate_times(authority):
    return {
        "health": authority.acquisition_start + PREOPEN_HEALTH_OFFSET,
        "identity": (
            authority.acquisition_start + PREOPEN_IDENTITY_OFFSET
        ),
        "final_go": (
            authority.acquisition_start + PREOPEN_FINAL_GO_OFFSET
        ),
    }


def _wait_until(target, stop, poll_seconds, *, clock=None):
    now = clock or (lambda: datetime.now(UTC))
    while now() < target:
        if stop.wait(min(
            poll_seconds,
            max(0.0, (target - now()).total_seconds()),
        )):
            raise RuntimeError("session stop requested before required gate")


def _runtime_container_state(
    args,
    *,
    backend_image_id,
    allow_provider_stopped=False,
):
    services = (*EQUITY_SERVICES, *OPTIONS_SERVICES)
    service_ids = {}
    for service in services:
        completed = _compose_command(
            args,
            "ps",
            "--all",
            "--quiet",
            service,
            backend_image_id=backend_image_id,
        )
        identifiers = completed.stdout.decode(
            "utf-8",
            errors="replace",
        ).split()
        if len(identifiers) != 1:
            raise RuntimeError(
                f"expected one owned container for {service}"
            )
        service_ids[service] = identifiers[0]
    completed = subprocess.run(
        (
            str(args.docker_executable),
            "inspect",
            *service_ids.values(),
        ),
        cwd=REPOSITORY_ROOT,
        check=False,
        timeout=60,
        capture_output=True,
    )
    if completed.returncode:
        raise RuntimeError(
            f"Docker inspect failed with {completed.returncode}"
        )
    try:
        inspected = {
            str(row["Id"]): row
            for row in json.loads(completed.stdout)
        }
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("Docker inspect output changed") from error
    result = {}
    for service, identifier in service_ids.items():
        row = inspected.get(identifier)
        if not isinstance(row, dict):
            raise RuntimeError(
                f"Docker inspect omitted {service}"
            )
        state = row.get("State")
        if not isinstance(state, dict):
            raise RuntimeError(
                f"Docker state is unavailable for {service}"
            )
        status = str(state.get("Status") or "")
        health_value = state.get("Health")
        health = (
            None
            if not isinstance(health_value, dict)
            else str(health_value.get("Status") or "")
        )
        restarts = int(row.get("RestartCount") or 0)
        stopped_provider = (
            allow_provider_stopped
            and service in PROVIDER_SERVICES
            and status == "exited"
        )
        if (
            str(row.get("Image")) != backend_image_id
            or restarts > MAX_SERVICE_RESTARTS
            or (
                not stopped_provider
                and (status != "running" or health != "healthy")
            )
        ):
            raise RuntimeError(
                f"runtime container health changed for {service}"
            )
        result[service] = {
            "container_id": identifier,
            "image_id": str(row["Image"]),
            "state": status,
            "health": health,
            "restart_count": restarts,
        }
    return result


def _container_stats(args, containers):
    running = [
        row["container_id"]
        for row in containers.values()
        if row["state"] == "running"
    ]
    if not running:
        return []
    completed = subprocess.run(
        (
            str(args.docker_executable),
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
            *running,
        ),
        cwd=REPOSITORY_ROOT,
        check=False,
        timeout=60,
        capture_output=True,
    )
    if completed.returncode:
        raise RuntimeError(
            f"Docker stats failed with {completed.returncode}"
        )
    try:
        return [
            json.loads(line)
            for line in completed.stdout.decode(
                "utf-8",
                errors="replace",
            ).splitlines()
            if line.strip()
        ]
    except json.JSONDecodeError as error:
        raise RuntimeError("Docker stats output changed") from error


def _options_acquisition_health(args):
    completed = _compose_command(
        args,
        "exec",
        "--no-TTY",
        OPTIONS_ACQUISITION_SERVICE,
        "python",
        "-B",
        "scripts/run_advanced_options_v3_acquisition.py",
        "health",
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--binding",
        "/run/advanced-options-acquisition/binding.json",
        "--status-path",
        "/var/lib/realtime-v2-reports/options-acquisition-status.json",
    )
    try:
        health = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            "Options acquisition health output changed"
        ) from error
    if not isinstance(health, dict) or health.get("status") != "READY":
        raise RuntimeError("Options acquisition is not ready")
    return health


def _database_runtime_status(
    provider,
    authority,
    *,
    require_preopen,
    allow_provider_stopped,
):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        cursor = unit.cursor
        cursor.execute(
            "SELECT auth.status,plan.plan_id::text,"
            "plan.manifest_id::text,plan.status,plan.universe_target,"
            "plan.session_date "
            "FROM rt.advanced_stream_authorizations AS auth "
            "JOIN rt.advanced_ingress_plans AS plan "
            "ON plan.authorization_id=auth.authorization_id "
            "WHERE auth.authorization_id=%s AND plan.cohort_id=%s "
            "AND plan.session_date=%s "
            "ORDER BY plan.created_at DESC LIMIT 2",
            (
                str(authority.equity_authorization_id),
                str(authority.equity_cohort_id),
                authority.session_date,
            ),
        )
        plans = cursor.fetchall()
        if len(plans) != 1:
            raise RuntimeError("exact Equity runtime plan is unavailable")
        plan = plans[0]
        cursor.execute(
            "SELECT revision.ticker,member.channel "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_members AS member USING(manifest_id) "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "JOIN LATERAL ("
            "SELECT ticker FROM ref.security_revisions "
            "WHERE security_id=instrument.security_id "
            "ORDER BY effective_from DESC,observed_at DESC LIMIT 1"
            ") AS revision ON true "
            "WHERE cohort.cohort_id=%s "
            "ORDER BY revision.ticker,member.channel",
            (str(authority.equity_cohort_id),),
        )
        members = cursor.fetchall()
        cursor.execute(
            "SELECT session.session_id::text,session.owner_id::text,"
            "session.owner_epoch,session.status,"
            "session.journal_generation::text,session.manifest_id::text,"
            "session.connection_attempt,session.connection_ceiling,"
            "session.received_frame_count,session.received_byte_count,"
            "session.heartbeat_at,session.subscribed_at,registry.status,"
            "registry.raw_committed_offset,"
            "registry.normalized_committed_offset,"
            "registry.bridge_committed_offset "
            "FROM rt.stream_sessions AS session "
            "JOIN rt.advanced_journal_registry AS registry "
            "ON registry.journal_generation=session.journal_generation "
            "WHERE session.advanced_ingress_plan_id=%s "
            "ORDER BY session.owner_epoch DESC LIMIT 1",
            (str(plan[1]),),
        )
        streams = cursor.fetchall()
        cursor.execute(
            "SELECT count(*) FROM "
            "rt.advanced_options_window_cycle_authorizations "
            "WHERE session_authorization_id=%s",
            (str(authority.options_session_authorization_id),),
        )
        cycle_authorities = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT session.journal_generation::text,"
            "registry.status,registry.raw_committed_offset,"
            "registry.normalized_committed_offset "
            "FROM rt.advanced_options_session_acquisition_authorizations "
            "AS session "
            "LEFT JOIN rt.advanced_journal_registry AS registry "
            "ON registry.journal_generation=session.journal_generation "
            "WHERE session.authorization_id=%s",
            (str(authority.options_session_authorization_id),),
        )
        options_journal = cursor.fetchone()
        if options_journal is None:
            raise RuntimeError(
                "Options session journal identity is unavailable"
            )
        cursor.execute(
            "SELECT checkpoint.consumer_name,"
            "max(checkpoint.committed_offset) "
            "FROM rt.consumer_checkpoints AS checkpoint "
            "WHERE checkpoint.cohort_id=%s "
            "AND checkpoint.journal_generation=%s "
            "GROUP BY checkpoint.consumer_name "
            "ORDER BY checkpoint.consumer_name",
            (
                str(authority.options_cohort_id),
                str(options_journal[0]),
            ),
        )
        checkpoints = {
            str(name): int(offset)
            for name, offset in cursor.fetchall()
        }
        cursor.execute(
            "SELECT completion.member,"
            "max(completion.ending_offset) "
            "FROM rt.advanced_options_resident_completions AS completion "
            "JOIN rt.advanced_options_window_cycle_authorizations AS cycle "
            "ON cycle.target_window_id=completion.target_window_id "
            "WHERE cycle.session_authorization_id=%s "
            "GROUP BY completion.member ORDER BY completion.member",
            (str(authority.options_session_authorization_id),),
        )
        resident_barriers = {
            str(member): int(offset)
            for member, offset in cursor.fetchall()
        }
        cursor.execute(
            "SELECT count(*) FILTER (WHERE state IN "
            "('RESERVED','AMBIGUOUS')),"
            "count(*) FILTER (WHERE state='COMPLETE'),"
            "COALESCE(sum(response_bytes) FILTER "
            "(WHERE state='COMPLETE'),0) "
            "FROM rt.provider_request_ledger "
            "WHERE account_alias='MASSIVE_ADVANCED' "
            "AND budget_session_date=%s "
            "AND lane='OPTIONS_ALIGNMENT'",
            (authority.session_date,),
        )
        request_status = cursor.fetchone()
    expected_members = [
        (symbol, "AM")
        for symbol in sorted(ADVANCED_SESSION_EQUITY_SYMBOLS)
    ]
    allowed_plan_statuses = (
        {"READY", "ACTIVE", "COMPLETE"}
        if allow_provider_stopped
        else {"READY", "ACTIVE"}
    )
    if (
        str(plan[0]) != "READY"
        or str(plan[3]) not in allowed_plan_statuses
        or int(plan[4]) != 9
        or plan[5] != authority.session_date
        or [(str(row[0]), str(row[1])) for row in members]
        != expected_members
        or len(streams) != 1
    ):
        raise RuntimeError("Equity runtime identity changed")
    stream = streams[0]
    allowed_stream_statuses = (
        {"LIVE", "STOPPED"}
        if allow_provider_stopped
        else {"LIVE"}
    )
    allowed_journal_statuses = (
        {"WRITING", "DRAINING", "DRAINED"}
        if allow_provider_stopped
        else {"WRITING"}
    )
    if (
        str(stream[3]) not in allowed_stream_statuses
        or str(stream[5]) != str(plan[2])
        or stream[11] is None
        or str(stream[12]) not in allowed_journal_statuses
        or (require_preopen and cycle_authorities != 0)
    ):
        raise RuntimeError("live Equity subscription identity changed")
    return {
        "equity": {
            "authorization_id": str(authority.equity_authorization_id),
            "cohort_id": str(authority.equity_cohort_id),
            "plan_id": str(plan[1]),
            "manifest_id": str(plan[2]),
            "symbols": [row[0] for row in expected_members],
            "channel": "AM",
            "stream_session_id": str(stream[0]),
            "owner_id": str(stream[1]),
            "owner_epoch": int(stream[2]),
            "stream_status": str(stream[3]),
            "journal_generation": str(stream[4]),
            "connection_attempt": int(stream[6]),
            "connection_ceiling": int(stream[7]),
            "received_frames": int(stream[8]),
            "received_bytes": int(stream[9]),
            "heartbeat_at": str(stream[10]),
            "subscribed_at": str(stream[11]),
            "journal_status": str(stream[12]),
            "raw_offset": int(stream[13]),
            "normalized_offset": int(stream[14]),
            "bridge_offset": int(stream[15]),
        },
        "options": {
            "cycle_authority_count": cycle_authorities,
            "journal_generation": str(options_journal[0]),
            "journal_status": (
                None
                if options_journal[1] is None
                else str(options_journal[1])
            ),
            "raw_offset": int(options_journal[2] or 0),
            "normalized_offset": int(options_journal[3] or 0),
            "consumer_checkpoints": checkpoints,
            "resident_barriers": resident_barriers,
            "open_or_ambiguous_requests": int(request_status[0]),
            "completed_requests": int(request_status[1]),
            "response_bytes": int(request_status[2]),
        },
    }


def _runtime_snapshot(
    args,
    provider,
    authority,
    bundle,
    *,
    backend_image_id,
    require_preopen=False,
    allow_provider_stopped=False,
):
    containers = _runtime_container_state(
        args,
        backend_image_id=backend_image_id,
        allow_provider_stopped=allow_provider_stopped,
    )
    state = _session_state(args)
    acquisition = state.load_acquisition_binding()
    session_binding = state.load_preopen_session_binding(bundle)
    if session_binding is None:
        raise TypeError("P8 pre-open session binding must be an object")
    database = _database_runtime_status(
        provider,
        authority,
        require_preopen=require_preopen,
        allow_provider_stopped=allow_provider_stopped,
    )
    acquisition_health = (
        None
        if allow_provider_stopped
        and containers[OPTIONS_ACQUISITION_SERVICE]["state"] == "exited"
        else _options_acquisition_health(args)
    )
    expected_contracts = tuple(
        authority.provider_contract.contract_tickers
    )
    if (
        acquisition.contract_tickers != expected_contracts
        or session_binding.contract_tickers != expected_contracts
        or acquisition.owner_id
        != UUID(str(
            acquisition_health["owner_id"]
            if acquisition_health is not None
            else acquisition.owner_id
        ))
        or acquisition.authority.journal_generation
        != session_binding.journal_generation
        or database["options"]["journal_generation"]
        != str(session_binding.journal_generation)
        or database["options"]["journal_status"]
        not in (
            {"WRITING", "DRAINING", "DRAINED"}
            if allow_provider_stopped
            else {"WRITING"}
        )
        or (
            acquisition_health is not None
            and acquisition_health["binding_sha256"]
            != acquisition.binding_sha256
        )
    ):
        raise RuntimeError("Options runtime identity changed")
    stable_identity = {
        "containers": {
            service: {
                "container_id": row["container_id"],
                "image_id": row["image_id"],
            }
            for service, row in sorted(containers.items())
        },
        "equity": {
            name: database["equity"][name]
            for name in (
                "authorization_id",
                "cohort_id",
                "plan_id",
                "manifest_id",
                "symbols",
                "channel",
                "stream_session_id",
                "owner_id",
                "owner_epoch",
                "journal_generation",
            )
        },
        "options": {
            "authorization_id": str(
                authority.options_session_authorization_id
            ),
            "cohort_id": str(authority.options_cohort_id),
            "contracts": list(expected_contracts),
            "acquisition_binding_sha256": acquisition.binding_sha256,
            "acquisition_owner_id": str(acquisition.owner_id),
            "journal_generation": str(
                acquisition.authority.journal_generation
            ),
            "service_owner_ids": [
                {"kind": kind, "owner_id": str(owner_id)}
                for kind, owner_id in session_binding.service_owner_ids
            ],
        },
    }
    disk = shutil.disk_usage(args.output_directory)
    return {
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "identity": stable_identity,
        "identity_sha256": canonical_sha256(stable_identity),
        "containers": containers,
        "container_stats": _container_stats(args, containers),
        "database": database,
        "options_acquisition": acquisition_health,
        "capacity": {
            "disk_total_bytes": disk.total,
            "disk_used_bytes": disk.used,
            "disk_free_bytes": disk.free,
        },
    }


def _numeric_runtime_metrics(snapshot):
    equity = snapshot["database"]["equity"]
    options = snapshot["database"]["options"]
    acquisition = snapshot.get("options_acquisition") or {}
    metrics = {
        "equity_owner_epoch": equity["owner_epoch"],
        "equity_received_frames": equity["received_frames"],
        "equity_received_bytes": equity["received_bytes"],
        "equity_raw_offset": equity["raw_offset"],
        "equity_normalized_offset": equity["normalized_offset"],
        "equity_bridge_offset": equity["bridge_offset"],
        "options_cycle_authorities": options["cycle_authority_count"],
        "options_raw_offset": options["raw_offset"],
        "options_normalized_offset": options["normalized_offset"],
        "options_completed_requests": options["completed_requests"],
        "options_response_bytes": options["response_bytes"],
    }
    for name, value in options["consumer_checkpoints"].items():
        metrics[f"checkpoint:{name}"] = value
    for name, value in options["resident_barriers"].items():
        metrics[f"barrier:{name}"] = value
    for name in (
        "connection_attempts",
        "decoded_messages",
        "decoded_bytes",
        "last_raw_offset",
        "last_event_offset",
    ):
        if name in acquisition:
            metrics[f"options_{name}"] = int(acquisition[name])
    for service, row in snapshot["containers"].items():
        metrics[f"restart:{service}"] = row["restart_count"]
    return metrics


def _assert_monotonic_runtime(previous, current):
    if previous is None:
        return
    before = _numeric_runtime_metrics(previous)
    after = _numeric_runtime_metrics(current)
    regressed = [
        name
        for name, value in before.items()
        if name in after and after[name] < value
    ]
    if regressed:
        raise RuntimeError(
            "runtime monotonic counters regressed: "
            + ", ".join(sorted(regressed))
        )


def _write_runtime_report(path, value):
    report = dict(value)
    report["semantic_sha256"] = canonical_sha256(report)
    _write_json(path, report)
    return report


def _read_verified_runtime_report(path: Path, schema: str):
    report = _read_json(path)
    semantic = report.get("semantic_sha256")
    unsigned = {
        key: value
        for key, value in report.items()
        if key != "semantic_sha256"
    }
    if (
        report.get("schema") != schema
        or semantic != canonical_sha256(unsigned)
    ):
        raise RuntimeError(f"runtime report identity changed: {path.name}")
    return report


def _initialize_coordinator_identity(args, authority):
    existed = args.output_directory.exists()
    if existed and not args.resume:
        raise RuntimeError("existing coordinator output requires --resume")
    if not existed:
        args.output_directory.mkdir()
    acquisition = _session_state(args).load_acquisition_binding()
    identity = {
        "schema": COORDINATOR_IDENTITY_SCHEMA,
        "coordinator_id": str(uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:advanced-realtime-v3-coordinator:"
                f"{authority.authorization_sha256}"
            ),
        )),
        "authorization_id": str(authority.authorization_id),
        "authorization_sha256": authority.authorization_sha256,
        "session_date": authority.session_date.isoformat(),
        "package_sha256": authority.package_sha256,
        "bundle_sha256": authority.bundle_sha256,
        "options_session_authorization_sha256": (
            authority.options_session_authorization_sha256
        ),
        "acquisition_binding_sha256": acquisition.binding_sha256,
    }
    identity["semantic_sha256"] = canonical_sha256(identity)
    path = args.output_directory / "COORDINATOR.json"
    if existed:
        if not path.is_file() or _read_json(path) != identity:
            raise RuntimeError("coordinator resume identity changed")
        if (args.output_directory / "CLEANUP.json").exists():
            raise RuntimeError("completed coordinator scope cannot resume")
        return True
    _write_json(path, identity)
    return False


def _monitor_failure_is_fatal(error: BaseException) -> bool:
    message = str(error).lower()
    return any(
        marker in message
        for marker in (
            "identity changed",
            "identity diverged",
            "binding changed",
            "journal identity",
            "monotonic counters regressed",
            "unauthorized ownership",
        )
    )


def _monitor_resume_state(directory: Path, prefix: str):
    samples = sorted(directory.glob(f"{prefix}sample-*.json"))
    failures = sorted(directory.glob(f"{prefix}failure-*.json"))

    def index(path):
        try:
            return int(path.stem.rsplit("-", 1)[1])
        except (IndexError, ValueError) as error:
            raise RuntimeError("monitor evidence filename changed") from error

    previous = None
    if samples:
        previous = _read_verified_runtime_report(
            samples[-1],
            (
                "advanced-realtime-v3-preopen-monitor-sample-v1"
                if prefix == "preopen-"
                else "advanced-realtime-v3-runtime-monitor-sample-v1"
            ),
        )
    return (
        previous,
        max((index(path) for path in samples), default=0),
        max((index(path) for path in failures), default=0),
    )


def _monitor_runtime(
    args,
    provider,
    authority,
    bundle,
    backend_image_id,
    baseline_identity_sha256,
    session_stop,
    monitor_stop,
    failures,
):
    monitoring_directory = args.output_directory / "monitoring"
    monitoring_directory.mkdir(exist_ok=True)
    previous, sample_index, failure_index = _monitor_resume_state(
        monitoring_directory,
        "",
    )
    consecutive_failures = 0
    while not monitor_stop.wait(args.monitor_seconds):
        try:
            allow_provider_stopped = (
                datetime.now(UTC) >= authority.market_capture_stop
            )
            sample = _runtime_snapshot(
                args,
                provider,
                authority,
                bundle,
                backend_image_id=backend_image_id,
                allow_provider_stopped=allow_provider_stopped,
            )
            if sample["identity_sha256"] != baseline_identity_sha256:
                raise RuntimeError(
                    "runtime identity diverged from final GO"
                )
            _assert_monotonic_runtime(previous, sample)
            sample_index += 1
            _write_runtime_report(
                monitoring_directory
                / f"sample-{sample_index:05d}.json",
                {
                    "schema": (
                        "advanced-realtime-v3-runtime-monitor-sample-v1"
                    ),
                    "interval_seconds": args.monitor_seconds,
                    **sample,
                },
            )
            previous = sample
            consecutive_failures = 0
        except Exception as error:
            failure_index += 1
            consecutive_failures += 1
            fatal = _monitor_failure_is_fatal(error)
            terminal = (
                fatal
                or consecutive_failures
                >= MAX_CONSECUTIVE_MONITOR_FAILURES
            )
            _write_runtime_report(
                monitoring_directory
                / f"failure-{failure_index:05d}.json",
                {
                    "schema": (
                        "advanced-realtime-v3-runtime-monitor-failure-v1"
                    ),
                    "failed_at_utc": datetime.now(UTC).isoformat(),
                    "error_type": type(error).__name__,
                    "message": str(error),
                    "classification": (
                        "FATAL_IDENTITY_OR_INTEGRITY"
                        if fatal
                        else "TRANSIENT_TELEMETRY_OR_READINESS"
                    ),
                    "consecutive_failures": consecutive_failures,
                    "terminal": terminal,
                },
            )
            if terminal:
                failures.append(error)
                session_stop.set()
                return


def _monitor_preopen_runtime(
    args,
    backend_image_id,
    session_stop,
    monitor_stop,
    failures,
):
    monitoring_directory = args.output_directory / "monitoring"
    monitoring_directory.mkdir(exist_ok=True)
    _previous, sample_index, failure_index = _monitor_resume_state(
        monitoring_directory,
        "preopen-",
    )
    consecutive_failures = 0
    while not monitor_stop.wait(args.monitor_seconds):
        try:
            containers = _runtime_container_state(
                args,
                backend_image_id=backend_image_id,
            )
            sample_index += 1
            disk = shutil.disk_usage(args.output_directory)
            _write_runtime_report(
                monitoring_directory
                / f"preopen-sample-{sample_index:05d}.json",
                {
                    "schema": (
                        "advanced-realtime-v3-preopen-monitor-sample-v1"
                    ),
                    "interval_seconds": args.monitor_seconds,
                    "checked_at_utc": datetime.now(UTC).isoformat(),
                    "containers": containers,
                    "container_stats": _container_stats(
                        args,
                        containers,
                    ),
                    "capacity": {
                        "disk_total_bytes": disk.total,
                        "disk_used_bytes": disk.used,
                        "disk_free_bytes": disk.free,
                    },
                },
            )
            consecutive_failures = 0
        except Exception as error:
            failure_index += 1
            consecutive_failures += 1
            fatal = _monitor_failure_is_fatal(error)
            terminal = (
                fatal
                or consecutive_failures
                >= MAX_CONSECUTIVE_MONITOR_FAILURES
            )
            _write_runtime_report(
                monitoring_directory
                / f"preopen-failure-{failure_index:05d}.json",
                {
                    "schema": (
                        "advanced-realtime-v3-preopen-monitor-failure-v1"
                    ),
                    "failed_at_utc": datetime.now(UTC).isoformat(),
                    "error_type": type(error).__name__,
                    "message": str(error),
                    "classification": (
                        "FATAL_IDENTITY_OR_INTEGRITY"
                        if fatal
                        else "TRANSIENT_TELEMETRY_OR_READINESS"
                    ),
                    "consecutive_failures": consecutive_failures,
                    "terminal": terminal,
                },
            )
            if terminal:
                failures.append(error)
                session_stop.set()
                return


def _read_runtime_status(output_directory):
    monitoring_directory = output_directory / "monitoring"
    samples = sorted(
        (
            path
            for path in monitoring_directory.glob("*.json")
            if "failure" not in path.name
        ),
        key=lambda path: path.stat().st_mtime_ns,
    )
    failures = sorted(monitoring_directory.glob("*failure*.json"))
    gates = {}
    for name in (
        "PREOPEN-HEALTH.json",
        "PREOPEN-IDENTITY.json",
        "FINAL-GO.json",
        "CAPTURE-STOP.json",
        "CLOSE-SEAL.json",
        "CLEANUP.json",
    ):
        path = output_directory / name
        if path.is_file():
            gates[name] = _read_json(path)
    return {
        "schema": "advanced-realtime-v3-live-runtime-status-v1",
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "latest_sample": (
            None if not samples else _read_json(samples[-1])
        ),
        "sample_count": len(samples),
        "failures": [
            _read_json(path)
            for path in failures
        ],
        "gates": gates,
        "provider_effects": 0,
        "production_effects": 0,
    }


def _assert_services_absent(args):
    completed = _compose_command(
        args,
        "ps",
        "--all",
        "--quiet",
        *EQUITY_SERVICES,
        *OPTIONS_SERVICES,
    )
    if completed.stdout.strip():
        raise RuntimeError(
            "session scheduler refuses pre-existing target containers"
        )


def _reconcile_or_start_services(args, *, resuming):
    completed = _compose_command(
        args,
        "ps",
        "--all",
        "--quiet",
        *EQUITY_SERVICES,
        *OPTIONS_SERVICES,
    )
    identifiers = completed.stdout.decode(
        "utf-8",
        errors="replace",
    ).split()
    if not identifiers:
        _start_services(args)
        return "STARTED"
    if not resuming:
        raise RuntimeError(
            "session scheduler refuses pre-existing target containers"
        )
    expected = len(EQUITY_SERVICES) + len(OPTIONS_SERVICES)
    if len(set(identifiers)) != expected:
        raise RuntimeError(
            "coordinator resume found a partial container topology"
        )
    return "RECONCILED_EXISTING"


def _assert_resume_can_start_services(provider, authority, bundle):
    state = _cleanup_database_state(provider, authority, bundle)
    if any(state.values()):
        raise RuntimeError(
            "coordinator resume refuses new containers while durable "
            "streams, leases, or provider requests remain active"
        )
    return state


def _start_services(args):
    _compose_command(
        args,
        "up",
        "--detach",
        "--no-build",
        "--wait",
        *EQUITY_SERVICES,
        OPTIONS_ACQUISITION_SERVICE,
    )
    production_cli._compose(
        SimpleNamespace(
            package=args.package,
            bundle=args.bundle,
            docker_executable=args.docker_executable,
        ),
        OPTIONS_RESIDENT_KINDS,
    )


def _stop_provider_acquisition(args):
    completed = _compose_command(
        args,
        "stop",
        "--timeout",
        "60",
        *PROVIDER_SERVICES,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(
            "provider acquisition did not stop at the capture boundary"
        )
    return {
        "stopped_at_utc": datetime.now(UTC).isoformat(),
        "services": list(PROVIDER_SERVICES),
        "returncode": completed.returncode,
    }


def _close_seal(
    args,
    provider,
    authority,
    bundle,
    *,
    backend_image_id,
    baseline_identity_sha256,
    results,
):
    snapshot = _runtime_snapshot(
        args,
        provider,
        authority,
        bundle,
        backend_image_id=backend_image_id,
        allow_provider_stopped=True,
    )
    if (
        snapshot["identity_sha256"] != baseline_identity_sha256
        or len(results) != len(authority.windows)
        or any(
            row.get("status") != "COMPLETE_PUBLISHED_PINNED"
            for row in results
        )
        or results[-1].get("window", {}).get("index")
        != len(authority.windows)
    ):
        raise RuntimeError("close seal evidence is incomplete")
    expected_release_id = str(results[-1]["release_id"])
    pointer = _portal_pointer(
        provider,
        authority.options_cohort_id,
    )
    pin = _current_read_pin(
        provider,
        authority.options_cohort_id,
    )
    if (
        pointer is None
        or str(pointer.release_id) != expected_release_id
        or pin is None
        or str(pin["release_id"]) != expected_release_id
    ):
        raise RuntimeError("close pointer or read pin identity changed")
    return _write_runtime_report(
        args.output_directory / "CLOSE-SEAL.json",
        {
            "schema": "advanced-realtime-v3-close-seal-v1",
            "sealed_at_utc": datetime.now(UTC).isoformat(),
            "decision_stop_utc": authority.decision_stop.isoformat(),
            "cycle_count": len(results),
            "final_window": results[-1]["window"],
            "final_release_id": expected_release_id,
            "pointer_release_id": str(pointer.release_id),
            "read_pin_release_id": str(pin["release_id"]),
            "identity_sha256": snapshot["identity_sha256"],
            "database": snapshot["database"],
            "containers": snapshot["containers"],
        },
    )


def _remove_secret_directory(path):
    actual = {
        child.name
        for child in path.iterdir()
        if child.is_file()
    }
    if actual != EXPECTED_SECRET_FILES or any(
        not child.is_file() for child in path.iterdir()
    ):
        raise RuntimeError(
            "Docker secret directory ownership changed; refusing cleanup"
        )
    for name in sorted(actual):
        (path / name).unlink()
    path.rmdir()
    return sorted(actual)


def _cleanup_database_state(provider, authority, bundle):
    session_payload = bundle.get("session_binding")
    session_binding = (
        None
        if session_payload is None
        else p8_preopen_session_binding_from_payload(session_payload)
    )
    if session_binding is None:
        raise TypeError("P8 pre-open session binding must be an object")
    owner_ids = [
        str(owner_id)
        for _kind, owner_id in session_binding.service_owner_ids
    ]
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        cursor = unit.cursor
        cursor.execute(
            "SELECT count(*) FROM rt.stream_sessions AS session "
            "JOIN rt.advanced_ingress_plans AS plan "
            "ON plan.plan_id=session.advanced_ingress_plan_id "
            "WHERE plan.authorization_id=%s AND session.ended_at IS NULL",
            (str(authority.equity_authorization_id),),
        )
        active_streams = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT count(*) FROM rt.worker_leases "
            "WHERE owner_id=ANY(%s::uuid[]) "
            "AND lease_until>clock_timestamp()",
            (owner_ids,),
        )
        active_leases = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT count(*) FROM rt.provider_request_ledger "
            "WHERE account_alias='MASSIVE_ADVANCED' "
            "AND budget_session_date=%s "
            "AND state IN ('RESERVED','AMBIGUOUS')",
            (authority.session_date,),
        )
        open_requests = int(cursor.fetchone()[0])
    return {
        "active_stream_sessions": active_streams,
        "active_worker_leases": active_leases,
        "open_or_ambiguous_provider_requests": open_requests,
    }


def _cleanup(args, provider, authority, bundle):
    services = (*EQUITY_SERVICES, *OPTIONS_SERVICES)
    remaining_services = (
        *EQUITY_SERVICES,
        *(
            service
            for service in OPTIONS_SERVICES
            if service != OPTIONS_ACQUISITION_SERVICE
        ),
    )
    acquisition_stopped = _compose_command(
        args,
        "stop",
        "--timeout",
        "60",
        OPTIONS_ACQUISITION_SERVICE,
        check=False,
    )
    stopped = _compose_command(
        args,
        "stop",
        "--timeout",
        "60",
        *remaining_services,
        check=False,
    )
    removed = _compose_command(
        args,
        "rm",
        "--force",
        *services,
        check=False,
    )
    remaining = _compose_command(
        args,
        "ps",
        "--all",
        "--quiet",
        *services,
        check=False,
    )
    removed_secrets = _remove_secret_directory(
        args.secret_directory
    )
    database = _cleanup_database_state(
        provider,
        authority,
        bundle,
    )
    return {
        "stop_returncode": (
            acquisition_stopped.returncode or stopped.returncode
        ),
        "remove_returncode": removed.returncode,
        "containers": remaining.stdout.decode(
            "utf-8",
            errors="replace",
        ).split(),
        "removed_secret_files": removed_secrets,
        "secret_directory_exists": args.secret_directory.exists(),
        "database": database,
        "run_owned_volumes": [],
        "run_owned_networks": [],
    }


def _write_cycle_report(path: Path, report: dict[str, object]):
    document = dict(report)
    document["semantic_sha256"] = canonical_sha256(document)
    _write_json(path, document)


def _existing_terminal_cycle_failure(output_directory, window):
    path = output_directory / f"{window.key}-failure.json"
    if not path.is_file():
        return None
    failure = _read_json(path)
    if (
        failure.get("contract") != ADVANCED_SESSION_SCHEDULER_VERSION
        or failure.get("status") != "FAILED_CYCLE"
        or failure.get("window") != window.key
    ):
        raise RuntimeError("stored scheduler failure identity changed")
    return failure


def _write_cycle_deadline_report(
    args,
    window,
    *,
    threshold_seconds,
    classification,
    error,
):
    return _write_runtime_report(
        args.output_directory
        / f"{window.key}-E+{threshold_seconds}.json",
        {
            "schema": "advanced-realtime-v3-cycle-deadline-v1",
            "window": window.key,
            "equity_cutoff": window.equity_cutoff.isoformat(),
            "threshold_seconds": threshold_seconds,
            "observed_at_utc": datetime.now(UTC).isoformat(),
            "classification": classification,
            "error_type": type(error).__name__,
            "message": str(error),
            "retry": (
                "STOPPED"
                if threshold_seconds >= 240
                else "BOUNDED_UNTIL_E_PLUS_240"
            ),
            "terminal_at_utc": (
                window.latest_options_cutoff.isoformat()
            ),
        },
    )


def _run(args):
    load_dotenv(args.env_file, override=False)
    provider, pool = production_cli._provider()
    args.state_provider = (
        provider
        if args.state_source == POSTGRES_STATE_SOURCE
        else None
    )
    authority = args.scheduler_authority
    freeze_report = _load_go_freeze_report(args.freeze_report)
    frozen_identities = freeze_report.get("frozen_identities")
    if not isinstance(frozen_identities, dict):
        raise ValueError("pre-open frozen identities are unavailable")
    try:
        initial_session_state = _session_state(args)
        args.session_state = initial_session_state
        package = initial_session_state.load_package()
        bundle = initial_session_state.load_execution_bundle()
    except Exception:
        pool.closeall()
        raise
    validate_advanced_options_v3_runtime_identity(package, bundle)
    session = session_acquisition_authorization_from_payload(
        bundle["v3_authorities"]["session_acquisition"]
    )
    expected_environment = {
        "REALTIME_V2_ADVANCED_AUTHORIZATION_ID": str(
            authority.equity_authorization_id
        ),
        "REALTIME_V2_ADVANCED_COHORT_ID": str(
            authority.equity_cohort_id
        ),
        "REALTIME_V2_ADVANCED_SESSION_DATE": (
            authority.session_date.isoformat()
        ),
        "REALTIME_V2_ADVANCED_UNIVERSE_TARGET": "9",
        "REALTIME_V2_BACKEND_IMAGE": str(
            frozen_identities.get("acquisition_image_id")
        ),
    }
    conflicts = [
        name
        for name, value in expected_environment.items()
        if os.environ.get(name, "").strip()
        and os.environ[name] != value
    ]
    if conflicts:
        raise ValueError(
            "session scheduler environment identity changed: "
            + ", ".join(sorted(conflicts))
        )
    os.environ.update(expected_environment)
    if (
        package.get("package_sha256") != authority.package_sha256
        or bundle.get("bundle_sha256") != authority.bundle_sha256
        or frozen_identities.get("package_sha256")
        != authority.package_sha256
        or frozen_identities.get("bundle_sha256")
        != authority.bundle_sha256
        or frozen_identities.get(
            "options_session_authority_sha256"
        )
        != authority.options_session_authorization_sha256
        or sorted(package.get("v3_admitted_contracts") or ())
        != list(authority.provider_contract.contract_tickers)
        or session.authorization_id
        != authority.options_session_authorization_id
        or session.authorization_sha256
        != authority.options_session_authorization_sha256
        or session.cohort_id != authority.options_cohort_id
        or session.acquisition_start > authority.acquisition_start
        or session.acquisition_stop < authority.decision_stop
    ):
        pool.closeall()
        raise ValueError("session scheduler artifact identity changed")
    try:
        resuming = _initialize_coordinator_identity(args, authority)
    except Exception:
        pool.closeall()
        raise
    if args.state_source == LEGACY_DIAGNOSTIC_STATE_SOURCE:
        args.downstream_binding_directory = (
            args.output_directory / "downstream-bindings"
        )
        if resuming:
            if not args.downstream_binding_directory.is_dir():
                pool.closeall()
                raise RuntimeError(
                    "coordinator downstream binding state is unavailable"
                )
        else:
            args.downstream_binding_directory.mkdir()
        os.environ[
            "REALTIME_V3_OPTIONS_DOWNSTREAM_BINDING_PATH"
        ] = str(args.downstream_binding_directory)
        args.session_state = LegacyArtifactSessionStateAdapter(
            package_path=args.package,
            bundle_path=args.bundle,
            acquisition_binding_path=args.acquisition_binding,
            downstream_binding_directory=(
                args.downstream_binding_directory
            ),
            json_loader=_read_json,
            bundle_validator=validate_p8_execution_bundle,
            acquisition_parser=acquisition_binding_from_payload,
        )
    stop = threading.Event()
    monitor_stop = threading.Event()
    preopen_monitor_stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    results = []
    cleanup = {"status": "NOT_STARTED"}
    exit_code = 1
    owns_scope = False
    cleanup_authorized = False
    monitor_failures = []
    monitor_thread = None
    preopen_monitor_thread = None
    provider_stopped = False
    baseline_identity_sha256 = None
    backend_image_id = str(
        frozen_identities["acquisition_image_id"]
    )
    try:
        _wait_until(
            authority.acquisition_start,
            stop,
            args.poll_seconds,
        )
        if resuming:
            existing = _compose_command(
                args,
                "ps",
                "--all",
                "--quiet",
                *EQUITY_SERVICES,
                *OPTIONS_SERVICES,
            ).stdout.strip()
            if not existing:
                _assert_resume_can_start_services(
                    provider,
                    authority,
                    bundle,
                )
        service_reconciliation = _reconcile_or_start_services(
            args,
            resuming=resuming,
        )
        owns_scope = service_reconciliation == "STARTED"
        preopen_monitor_thread = threading.Thread(
            target=_monitor_preopen_runtime,
            args=(
                args,
                backend_image_id,
                stop,
                preopen_monitor_stop,
                monitor_failures,
            ),
            name="advanced-realtime-v3-preopen-monitor",
            daemon=True,
        )
        preopen_monitor_thread.start()
        gate_times = _preopen_gate_times(authority)
        _wait_until(
            gate_times["health"],
            stop,
            args.poll_seconds,
        )
        health_path = args.output_directory / "PREOPEN-HEALTH.json"
        if resuming and health_path.is_file():
            health_report = _read_verified_runtime_report(
                health_path,
                "advanced-realtime-v3-preopen-health-v1",
            )
            if health_report.get("status") != "PASS":
                raise RuntimeError("resumed pre-open health is not PASS")
        else:
            health_containers = _runtime_container_state(
                args,
                backend_image_id=backend_image_id,
            )
            _write_runtime_report(
                health_path,
                {
                    "schema": "advanced-realtime-v3-preopen-health-v1",
                    "gate_utc": gate_times["health"].isoformat(),
                    "checked_at_utc": datetime.now(UTC).isoformat(),
                    "status": "PASS",
                    "containers": health_containers,
                },
            )
        _wait_until(
            gate_times["identity"],
            stop,
            args.poll_seconds,
        )
        identity_path = args.output_directory / "PREOPEN-IDENTITY.json"
        if resuming and identity_path.is_file():
            identity_snapshot = _read_verified_runtime_report(
                identity_path,
                "advanced-realtime-v3-preopen-identity-v1",
            )
            if identity_snapshot.get("status") != "PASS":
                raise RuntimeError("resumed pre-open identity is not PASS")
        else:
            identity_snapshot = _runtime_snapshot(
                args,
                provider,
                authority,
                bundle,
                backend_image_id=backend_image_id,
                require_preopen=True,
            )
            owns_scope = True
            _write_runtime_report(
                identity_path,
                {
                    "schema": "advanced-realtime-v3-preopen-identity-v1",
                    "gate_utc": gate_times["identity"].isoformat(),
                    "status": "PASS",
                    **identity_snapshot,
                },
            )
        _wait_until(
            gate_times["final_go"],
            stop,
            args.poll_seconds,
        )
        final_go_path = args.output_directory / "FINAL-GO.json"
        recovered_snapshot = _runtime_snapshot(
            args,
            provider,
            authority,
            bundle,
            backend_image_id=backend_image_id,
            require_preopen=not (
                resuming and final_go_path.is_file()
            ),
        )
        owns_scope = True
        final_go_snapshot = recovered_snapshot
        if resuming and final_go_path.is_file():
            prior_final_go = _read_verified_runtime_report(
                final_go_path,
                "advanced-realtime-v3-final-go-v1",
            )
            if (
                prior_final_go.get("decision") != "GO"
                or prior_final_go.get("identity_sha256")
                != recovered_snapshot["identity_sha256"]
            ):
                raise RuntimeError(
                    "resumed runtime identity diverged from final GO"
                )
        if (
            final_go_snapshot["identity_sha256"]
            != identity_snapshot["identity_sha256"]
        ):
            raise RuntimeError(
                "pre-open runtime identity changed before final GO"
            )
        baseline_identity_sha256 = final_go_snapshot[
            "identity_sha256"
        ]
        if not final_go_path.exists():
            _write_runtime_report(
                final_go_path,
                {
                    "schema": "advanced-realtime-v3-final-go-v1",
                    "gate_utc": gate_times["final_go"].isoformat(),
                    "decision": "GO",
                    "scheduler_armed": True,
                    "no_prewritten_cycle_authority": (
                        final_go_snapshot["database"]["options"][
                            "cycle_authority_count"
                        ]
                        == 0
                    ),
                    **final_go_snapshot,
                },
            )
        preopen_monitor_stop.set()
        preopen_monitor_thread.join(
            timeout=args.monitor_seconds + 5
        )
        if preopen_monitor_thread.is_alive():
            raise RuntimeError("pre-open monitor did not stop")
        if monitor_failures:
            raise RuntimeError(
                "pre-open monitoring failed"
            ) from monitor_failures[0]
        port = _ProductionPort(
            args=args,
            authority=authority,
            provider=provider,
            output_directory=args.output_directory,
        )
        scheduler = AdvancedRealtimeSessionScheduler(authority, port)
        monitor_thread = threading.Thread(
            target=_monitor_runtime,
            args=(
                args,
                provider,
                authority,
                bundle,
                backend_image_id,
                baseline_identity_sha256,
                stop,
                monitor_stop,
                monitor_failures,
            ),
            name="advanced-realtime-v3-runtime-monitor",
            daemon=True,
        )
        monitor_thread.start()
        for window in authority.windows:
            if stop.is_set():
                break
            terminal_failure = _existing_terminal_cycle_failure(
                args.output_directory,
                window,
            )
            if (
                terminal_failure is not None
                and not port._report_path(window).exists()
                and datetime.now(UTC) > window.latest_options_cutoff
            ):
                results.append(terminal_failure)
                continue
            while datetime.now(UTC) < window.equity_available_after:
                if (
                    not provider_stopped
                    and datetime.now(UTC)
                    >= authority.market_capture_stop
                ):
                    capture_stop = _stop_provider_acquisition(args)
                    provider_stopped = True
                    _write_runtime_report(
                        args.output_directory / "CAPTURE-STOP.json",
                        {
                            "schema": (
                                "advanced-realtime-v3-capture-stop-v1"
                            ),
                            "market_capture_stop_utc": (
                                authority.market_capture_stop.isoformat()
                            ),
                            **capture_stop,
                        },
                    )
                if stop.wait(args.poll_seconds):
                    break
            if stop.is_set():
                break
            classification_written = False
            retry_stop_written = False
            while not stop.is_set():
                try:
                    report = dict(scheduler.run_window(window))
                    if not report["replayed"]:
                        report["acquisition_image_id"] = (
                            frozen_identities[
                                "acquisition_image_id"
                            ]
                        )
                        report["downstream_build_segment_id"] = (
                            "primary-"
                            f"{str(frozen_identities['acquisition_image_id'])[7:23]}"
                        )
                        _write_cycle_report(
                            port._report_path(window),
                            report,
                        )
                    results.append(report)
                    break
                except RuntimeError as error:
                    if (
                        str(error) in {
                            "Equity window is not finalized",
                            (
                                "finalized Equity INPUT_READY is "
                                "unavailable"
                            ),
                        }
                        and datetime.now(UTC)
                        <= window.latest_options_cutoff
                    ):
                        elapsed = (
                            datetime.now(UTC) - window.equity_cutoff
                        ).total_seconds()
                        if elapsed >= 120 and not classification_written:
                            _write_cycle_deadline_report(
                                args,
                                window,
                                threshold_seconds=120,
                                classification="FINALIZATION",
                                error=error,
                            )
                            classification_written = True
                        if elapsed >= 240:
                            if not retry_stop_written:
                                _write_cycle_deadline_report(
                                    args,
                                    window,
                                    threshold_seconds=240,
                                    classification="FINALIZATION",
                                    error=error,
                                )
                                retry_stop_written = True
                            _wait_until(
                                window.latest_options_cutoff,
                                stop,
                                args.poll_seconds,
                            )
                            cycle_error = RuntimeError(
                                "cycle reached terminal E+300 "
                                "without finalized Equity input"
                            )
                            break
                        stop.wait(args.poll_seconds)
                        continue
                    cycle_error = error
                    break
                except (
                    PolygonResponseTooLarge,
                    RequestException,
                    TypeError,
                    ValueError,
                ) as error:
                    cycle_error = error
                    break
            else:
                break
            if stop.is_set():
                break
            if "cycle_error" in locals():
                failure = {
                    "contract": ADVANCED_SESSION_SCHEDULER_VERSION,
                    "status": "FAILED_CYCLE",
                    "window": window.key,
                    "error_type": type(cycle_error).__name__,
                    "message": str(cycle_error),
                }
                _write_json(
                    args.output_directory / f"{window.key}-failure.json",
                    failure,
                )
                results.append(failure)
                del cycle_error
        if not provider_stopped:
            _wait_until(
                authority.market_capture_stop,
                stop,
                args.poll_seconds,
            )
            capture_stop = _stop_provider_acquisition(args)
            provider_stopped = True
            _write_runtime_report(
                args.output_directory / "CAPTURE-STOP.json",
                {
                    "schema": "advanced-realtime-v3-capture-stop-v1",
                    "market_capture_stop_utc": (
                        authority.market_capture_stop.isoformat()
                    ),
                    **capture_stop,
                },
            )
        exit_code = 0 if len(results) == len(authority.windows) and all(
            row["status"] == "COMPLETE_PUBLISHED_PINNED"
            for row in results
        ) else 1
        cleanup_authorized = True
        if monitor_failures:
            raise RuntimeError(
                "runtime monitoring failed"
            ) from monitor_failures[0]
        if exit_code == 0:
            _close_seal(
                args,
                provider,
                authority,
                bundle,
                backend_image_id=backend_image_id,
                baseline_identity_sha256=baseline_identity_sha256,
                results=results,
            )
            cleanup_at = (
                authority.market_capture_stop + timedelta(minutes=20)
            )
            _wait_until(
                cleanup_at,
                stop,
                args.poll_seconds,
            )
    finally:
        try:
            preopen_monitor_stop.set()
            if preopen_monitor_thread is not None:
                preopen_monitor_thread.join(
                    timeout=args.monitor_seconds + 5
                )
                if preopen_monitor_thread.is_alive():
                    raise RuntimeError(
                        "pre-open monitor did not stop"
                    )
            monitor_stop.set()
            if monitor_thread is not None:
                monitor_thread.join(timeout=args.monitor_seconds + 5)
                if monitor_thread.is_alive():
                    raise RuntimeError(
                        "runtime monitor did not stop"
                    )
            cleanup = (
                _cleanup(
                    args,
                    provider,
                    authority,
                    bundle,
                )
                if owns_scope and (
                    cleanup_authorized or monitor_failures
                )
                else {
                    "status": (
                        "PRESERVED_FOR_RESUME"
                        if owns_scope
                        else "NOT_OWNED"
                    ),
                    "containers": [],
                }
            )
        finally:
            pool.closeall()
            _write_json(
                _cleanup_report_path(
                    args.output_directory,
                    cleanup,
                ),
                cleanup,
            )
    if (
        cleanup.get("containers")
        or cleanup.get("stop_returncode", 0)
        or cleanup.get("remove_returncode", 0)
        or cleanup.get("secret_directory_exists")
        or any(cleanup.get("database", {}).values())
    ):
        return 1
    return exit_code


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.command == "freeze-template":
        template = preopen_freeze_template(args.session_date)
        _write_json(args.output, template)
        print(json.dumps({
            "status": "TEMPLATE_WRITTEN",
            "output": str(args.output),
            "provider_effects": 0,
            "production_effects": 0,
        }, indent=2, sort_keys=True))
        return 0
    if args.command == "freeze":
        report = build_preopen_freeze_report(
            _read_json(args.freeze_record)
        )
        _write_json(args.output, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["decision"] == "GO" else 2
    if args.command == "prepare-authorization":
        result = _prepare_authorization(args)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "diagnostic":
        authorization = build_diagnostic_authorization(
            authorization_reference=args.authorization_reference,
            owner=args.owner,
            session_date=args.session_date,
            cohort_id=args.cohort_id,
            source_sha256=args.source_sha256,
            backend_image_id=args.backend_image_id,
            input_evidence_sha256=_file_sha256(
                args.input_evidence
            ),
        )
        payload = diagnostic_authorization_payload(authorization)
        _write_json(args.output, payload)
        print(json.dumps({
            "status": "DIAGNOSTIC_NON_SERVING_AUTHORIZED",
            "authorization_id": str(
                authorization.authorization_id
            ),
            "authorization_sha256": (
                authorization.authorization_sha256
            ),
            "output": str(args.output),
            "effects": payload["effects"],
        }, indent=2, sort_keys=True))
        return 0
    if args.command == "transition-contract":
        cycle = _read_json(args.completed_cycle_report)
        cycle_sha256 = _validate_semantic_report(
            cycle,
            name="completed cycle report",
        )
        if (
            cycle.get("status") != "COMPLETE_PUBLISHED_PINNED"
            or not isinstance(cycle.get("window"), dict)
            or cycle["window"].get("index")
            != args.completed_window_index
        ):
            raise ValueError(
                "transition contract requires an exact completed boundary"
            )
        transition = build_downstream_transition(
            authorization_reference=args.authorization_reference,
            session_date=args.session_date,
            completed_window_index=args.completed_window_index,
            completed_cycle_report_sha256=cycle_sha256,
            preserved_acquisition_identity_sha256=(
                args.preserved_acquisition_identity_sha256
            ),
            preserved_package_sha256=(
                args.preserved_package_sha256
            ),
            preserved_bundle_sha256=(
                args.preserved_bundle_sha256
            ),
            old_segment=DownstreamBuildSegment(
                segment_id=args.old_segment_id,
                source_sha256=args.old_source_sha256,
                backend_image_id=args.old_backend_image_id,
                first_window_index=1,
                last_window_index=args.completed_window_index,
            ),
            new_segment=DownstreamBuildSegment(
                segment_id=args.new_segment_id,
                source_sha256=args.new_source_sha256,
                backend_image_id=args.new_backend_image_id,
                first_window_index=(
                    args.completed_window_index + 1
                ),
                last_window_index=len(
                    build_advanced_session_windows(args.session_date)
                ),
            ),
            services=tuple(args.service),
            tier_a_qualification_sha256=(
                args.tier_a_qualification_sha256
            ),
            tier_b_qualification_sha256=(
                args.tier_b_qualification_sha256
            ),
        )
        payload = downstream_transition_payload(transition)
        _write_json(args.output, payload)
        print(json.dumps({
            "status": "CONTROLLED_TRANSITION_CONTRACT_WRITTEN",
            "transition_id": str(transition.transition_id),
            "transition_sha256": transition.transition_sha256,
            "output": str(args.output),
            "provider_effects": 0,
            "production_effects": 0,
        }, indent=2, sort_keys=True))
        return 0
    if args.command == "transition":
        return _run_transition(args)
    if args.command == "runtime-status":
        report = _read_runtime_status(args.output_directory)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1 if report["failures"] else 0
    if args.command == "status":
        report = evaluate_session_evidence(
            _read_json(args.evidence)
        )
        _write_json(args.output, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    if args.command == "plan":
        print(json.dumps(
            _plan(args.scheduler_authority),
            indent=2,
            sort_keys=True,
        ))
        return 0
    return _run(args)


if __name__ == "__main__":
    raise SystemExit(main())
