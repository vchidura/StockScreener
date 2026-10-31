#!/usr/bin/env python3
"""Prepare and operate the single-bundle Advanced Options V3 handoff."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_control_plane import (  # noqa: E402
    advanced_options_history_binding_payload,
    build_advanced_options_equity_input_binding,
    persist_advanced_options_enrollment_cursor,
    persist_advanced_options_equity_input_binding_cursor,
)
from realtime.advanced_equity_control_plane import (  # noqa: E402
    load_advanced_equity_control_plane_plan_cursor,
)
from realtime.advanced_options_p8_docker import (  # noqa: E402
    P8_SERVICE_NAMES,
)
from realtime.advanced_options_p8_production import (  # noqa: E402
    p8_production_binding_from_payload,
)
from realtime.advanced_options_p8_runtime import (  # noqa: E402
    LegacyArtifactSessionStateAdapter,
    P8ExecutionBundleStore,
    build_p8_preopen_session_binding,
    check_p8_docker_service_health,
    run_p8_docker_service,
    validate_p8_execution_bundle,
)
from realtime.advanced_options_acquisition_runtime import (  # noqa: E402
    acquisition_binding_from_payload,
)
from realtime.advanced_options_resident_barrier import (  # noqa: E402
    advanced_options_v3_session_status_cursor,
)
from realtime.advanced_options_service import (  # noqa: E402
    AdvancedOptionsProcessKind,
)
from realtime.advanced_options_v3_authority import (  # noqa: E402
    TargetWindowAgePolicy,
    build_window_cycle_authorization,
    load_window_cycle_authorization_cursor,
    persist_session_acquisition_authorization_cursor,
    persist_window_cycle_authorization_cursor,
    session_acquisition_authorization_from_payload,
)
from realtime.advanced_options_v3_deployment import (  # noqa: E402
    ADVANCED_OPTIONS_V3_COMPOSE_PROFILE,
    validate_advanced_options_v3_runtime_identity,
)
from realtime.advanced_options_v3_history import (  # noqa: E402
    AdvancedOptionsHistoryBindingIdentityError,
    advanced_options_v3_history_binding_from_payload,
    verify_advanced_options_v3_history_binding_cursor,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.orchestration_repository import (  # noqa: E402
    PostgresOrchestrationRepository,
    WindowSpec,
)


UTC = timezone.utc
RESIDENT_KINDS = (
    AdvancedOptionsProcessKind.INGRESS,
    AdvancedOptionsProcessKind.MARKET_STATE,
    AdvancedOptionsProcessKind.FLOW,
)
ZERO_EFFECTS = {
    "provider_rest": 0,
    "provider_websocket": 0,
    "publication": 0,
    "stored_read_changes": 0,
    "dispatch": 0,
    "delivery": 0,
    "broker": 0,
    "trading": 0,
}


def _add_database_scope(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)


def _add_artifacts(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare")
    _add_database_scope(prepare)
    prepare.add_argument("--package-source", type=Path, required=True)
    prepare.add_argument(
        "--production-binding",
        type=Path,
        required=True,
    )
    prepare.add_argument("--live-observation", type=Path, required=True)
    prepare.add_argument("--output-directory", type=Path, required=True)
    prepare.add_argument("--execute", action="store_true")

    waiting = subparsers.add_parser("prepare-waiting")
    _add_database_scope(waiting)
    waiting.add_argument("--package-source", type=Path, required=True)
    waiting.add_argument(
        "--acquisition-binding",
        type=Path,
        required=True,
    )
    waiting.add_argument("--output-directory", type=Path, required=True)
    waiting.add_argument("--execute", action="store_true")

    residents = subparsers.add_parser("start-residents")
    _add_artifacts(residents)
    residents.add_argument(
        "--docker-executable",
        type=Path,
        required=True,
    )
    residents.add_argument("--execute", action="store_true")

    authorize = subparsers.add_parser("authorize-window")
    _add_database_scope(authorize)
    _add_artifacts(authorize)
    authorize.add_argument(
        "--authorization-reference",
        required=True,
    )
    authorize.add_argument(
        "--publication-authorization-reference",
        required=True,
    )
    authorize.add_argument("--window-key", required=True)
    authorize.add_argument(
        "--options-cutoff",
        type=datetime.fromisoformat,
        required=True,
    )
    authorize.add_argument(
        "--window-deadline",
        type=datetime.fromisoformat,
        required=True,
    )
    authorize.add_argument(
        "--equity-receipt-id",
        type=UUID,
        required=True,
    )
    authorize.add_argument(
        "--activity-evidence-sha256",
    )
    authorize.add_argument("--execute", action="store_true")

    window = subparsers.add_parser("run-window")
    _add_artifacts(window)
    window.add_argument(
        "--docker-executable",
        type=Path,
        required=True,
    )
    window.add_argument("--execute", action="store_true")

    status = subparsers.add_parser("status")
    _add_database_scope(status)
    _add_artifacts(status)

    args = parser.parse_args(argv)
    for name in (
        "package",
        "bundle",
        "package_source",
        "production_binding",
        "acquisition_binding",
        "live_observation",
        "output_directory",
        "docker_executable",
    ):
        value = getattr(args, name, None)
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    if hasattr(args, "database"):
        validate_target_database_v2(
            args.database,
            args.confirm_database_name,
            security_mode="hardened",
        )
        if args.database != EXPECTED_V2_DATABASE_NAME:
            parser.error("V3 production targets stocks_realtime_v2")
    if (
        args.command in {"prepare", "prepare-waiting"}
        and args.output_directory.exists()
    ):
        parser.error("--output-directory must be new")
    if (
        args.command in {
            "prepare",
            "prepare-waiting",
            "start-residents",
            "authorize-window",
            "run-window",
        }
        and not args.execute
    ):
        parser.error(f"{args.command} requires explicit --execute")
    if args.command == "authorize-window":
        if (
            args.options_cutoff.tzinfo is None
            or args.options_cutoff.utcoffset() is None
            or args.window_deadline.tzinfo is None
            or args.window_deadline.utcoffset() is None
            or args.window_deadline <= args.options_cutoff
            or not args.window_key.strip()
        ):
            parser.error("authorize-window clocks or key are invalid")
        args.options_cutoff = args.options_cutoff.astimezone(UTC)
        args.window_deadline = args.window_deadline.astimezone(UTC)
        if (
            args.activity_evidence_sha256 is not None
            and (
                len(args.activity_evidence_sha256) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in args.activity_evidence_sha256
                )
            )
        ):
            parser.error(
                "--activity-evidence-sha256 must be a SHA-256 digest"
            )
    return args


def _docker_service_main(argv) -> int | None:
    values = list(sys.argv[1:] if argv is None else argv)
    service_mode = "--docker-service-kind" in values
    health_mode = "--docker-health-kind" in values
    if not service_mode and not health_mode:
        return None
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument(
        "--docker-service-kind",
        choices=tuple(kind.value for kind in AdvancedOptionsProcessKind),
    )
    modes.add_argument(
        "--docker-health-kind",
        choices=tuple(kind.value for kind in AdvancedOptionsProcessKind),
    )
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--execution-bundle", type=Path, required=True)
    args = parser.parse_args(values)
    kind = AdvancedOptionsProcessKind(
        args.docker_service_kind or args.docker_health_kind
    )
    if service_mode:
        return run_p8_docker_service(
            kind,
            args.package.resolve(),
            args.execution_bundle.resolve(),
        )
    health = check_p8_docker_service_health(
        kind,
        args.package.resolve(),
        args.execution_bundle.resolve(),
    )
    print(json.dumps(health, indent=2, sort_keys=True))
    return 0


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain an object")
    return value


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(
        1,
        4,
        dsn=dsn,
        options="-c timezone=UTC",
    )
    qualification = (
        os.environ.get("REALTIME_V3_VALIDATION_MODE")
        == "DEVELOPMENT_DIAGNOSTIC"
        and os.environ.get(
            "REALTIME_V3_PROVIDER_FREE_QUALIFICATION"
        )
        == "ENABLED"
    )
    role_prefix = (
        os.environ.get("REALTIME_V2_ROLE_PREFIX", "").strip()
        if qualification
        else EXPECTED_V2_DATABASE_NAME
    )
    if not role_prefix:
        raise RuntimeError("V3 qualification role prefix is unavailable")
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=role_prefix,
            statement_timeout_ms=300_000,
            allow_disposable=qualification,
        ),
    )
    provider.verify()
    return provider, pool


def _write_json(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _report(value: dict[str, object]) -> dict[str, object]:
    report = dict(value)
    report["semantic_sha256"] = canonical_sha256(report)
    return report


def _validate_pre_session_identity(package, binding, session) -> None:
    control = binding.enrollment.control_plane
    admitted_contracts = package.get("v3_admitted_contracts")
    pre_session = package.get("v3_pre_session_authority")
    selected_contracts = sorted(
        row.contract_ticker
        for row in binding.enrollment.activity_selection.contracts
    )
    history_package = package.get("v3_history_binding")
    history_payload = (
        history_package.get("binding")
        if isinstance(history_package, dict)
        else None
    )
    history_evidence = (
        history_package.get("evidence")
        if isinstance(history_package, dict)
        else None
    )
    if (
        not isinstance(history_payload, dict)
        or not isinstance(history_evidence, dict)
        or advanced_options_history_binding_payload(binding.history)
        != history_payload
        or binding.history.evidence_sha256
        != history_evidence.get("evidence_sha256")
        or binding.history.source_cohort_id
        == binding.enrollment.equity_plan.cohort_id
    ):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding package identity is absent or divergent"
        )
    if (
        session.cohort_id != control.cohort_id
        or session.journal_generation
        != control.source_flow.journal_generation
        or session.slot_set_member_sha256
        != control.slot_set.member_sha256
        or not isinstance(pre_session, dict)
        or set(pre_session) != {
            "semantic_sha256",
            "file_sha256",
            "admission_sha256",
            "source_capture_semantic_sha256",
        }
        or any(
            not isinstance(pre_session.get(name), str)
            or len(pre_session[name]) != 64
            or any(
                character not in "0123456789abcdef"
                for character in pre_session[name]
            )
            for name in pre_session
        )
        or pre_session.get("admission_sha256")
        != session.admission_sha256
        or not isinstance(admitted_contracts, list)
        or sorted(str(value) for value in admitted_contracts)
        != selected_contracts
        or len(binding.enrollment.activity_selection.contracts) != 4
        or control.slot_set.contract_count != 4
    ):
        raise ValueError("V3 pre-open acquisition identity changed")


def _prepare(args) -> dict[str, object]:
    package = LegacyArtifactSessionStateAdapter(
        package_path=args.package_source,
        json_loader=_load,
    ).load_package()
    production = _load(args.production_binding)
    observation = _load(args.live_observation)
    binding = p8_production_binding_from_payload(production)
    authorities = package.get("v3_authorities")
    if (
        not isinstance(authorities, dict)
        or set(authorities) != {"session_acquisition"}
    ):
        raise ValueError(
            "V3 session package must contain only session acquisition authority"
        )
    session = session_acquisition_authorization_from_payload(
        authorities["session_acquisition"]
    )
    _validate_pre_session_identity(package, binding, session)
    control = binding.enrollment.control_plane
    provider, pool = _provider()
    try:
        with provider.transaction(APP_ROLE) as unit:
            verified_history = (
                verify_advanced_options_v3_history_binding_cursor(
                    unit.cursor,
                    binding=binding.history,
                    evidence=package["v3_history_binding"][
                        "evidence"
                    ],
                )
            )
            persist_advanced_options_enrollment_cursor(
                unit.cursor,
                binding.enrollment,
                history_binding=binding.history,
            )
            persist_session_acquisition_authorization_cursor(
                unit.cursor,
                session,
            )
            unit.cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_window_cycle_authorizations "
                "WHERE session_authorization_id=%s",
                (str(session.authorization_id),),
            )
            if unit.cursor.fetchone()[0] != 0:
                raise ValueError(
                    "V3 prepare refuses a prewritten cycle authority"
                )
    finally:
        pool.closeall()
    args.output_directory.mkdir()
    package_path = args.output_directory / "PACKAGE.json"
    bundle_path = args.output_directory / "SESSION-BUNDLE.json"
    with package_path.open("xb") as stream:
        stream.write(args.package_source.read_bytes())
    store = P8ExecutionBundleStore(
        bundle_path,
        package=package,
        package_path=package_path,
        repository_root=REPOSITORY_ROOT,
    )
    bundle = store.write(
        observation,
        binding,
        written_at=datetime.now(UTC),
        production_binding=production,
    )
    validate_advanced_options_v3_runtime_identity(package, bundle)
    report = _report({
        "contract": "advanced-options-v3-production-prepare-v1",
        "status": "PREPARED",
        "package_sha256": package["package_sha256"],
        "bundle_sha256": bundle["bundle_sha256"],
        "enrollment_id": str(binding.enrollment.enrollment_id),
        "enrollment_sha256": binding.enrollment.enrollment_sha256,
        "cohort_id": str(control.cohort_id),
        "window_id": str(binding.window_id),
        "journal_generation": str(
            control.source_flow.journal_generation
        ),
        "history_binding": {
            "binding_sha256": (
                verified_history.binding.binding_sha256
            ),
            "evidence_sha256": (
                verified_history.binding.evidence_sha256
            ),
            "source_cohort_id": str(
                verified_history.binding.source_cohort_id
            ),
            "source_manifest_sha256": (
                verified_history.binding.source_manifest_sha256
            ),
            "source_manifest_member_sha256": (
                verified_history.evidence["source_manifest"][
                    "member_sha256"
                ]
            ),
            "known_at_utc": verified_history.evidence[
                "known_at_utc"
            ],
            "adjusted_daily_sessions": (
                verified_history.evidence["coverage"][
                    "session_count"
                ]
            ),
            "raw_daily_sessions": (
                verified_history.evidence["coverage"][
                    "raw_daily_sessions_required"
                ]
            ),
            "symbol_members": (
                verified_history.evidence["coverage"]["symbols"]
            ),
            "persisted": True,
        },
        "session_authorization_id": str(session.authorization_id),
        "window_cycle": "NOT_PREPARED",
        "stored_reads": "NOT_AUTHORIZED",
        "effects": dict(ZERO_EFFECTS),
    })
    _write_json(args.output_directory / "PREPARE-REPORT.json", report)
    return report


def _compose(args, kinds) -> dict[str, object]:
    state = LegacyArtifactSessionStateAdapter(
        package_path=args.package,
        bundle_path=args.bundle,
        json_loader=_load,
        bundle_validator=validate_p8_execution_bundle,
    )
    package = state.load_package()
    bundle = state.load_execution_bundle()
    validate_advanced_options_v3_runtime_identity(package, bundle)
    services = [P8_SERVICE_NAMES[kind] for kind in kinds]
    command = (
        str(args.docker_executable),
        "compose",
        "--file",
        str(REPOSITORY_ROOT / "docker-compose.realtime-v2.yml"),
        "--profile",
        ADVANCED_OPTIONS_V3_COMPOSE_PROFILE,
        "up",
        "--detach",
        "--no-build",
        "--wait",
        *services,
    )
    completed = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env={
            **os.environ,
            "REALTIME_V2_P8_PACKAGE_PATH": str(args.package),
            "REALTIME_V2_P8_BUNDLE_PATH": str(args.bundle),
        },
        check=False,
        timeout=120,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"V3 Compose start failed with {completed.returncode}"
        )
    return _report({
        "contract": "advanced-options-v3-production-compose-v1",
        "status": "STARTED",
        "package_sha256": package["package_sha256"],
        "bundle_sha256": bundle["bundle_sha256"],
        "roles": [kind.value for kind in kinds],
        "command_sha256": hashlib.sha256(
            "\0".join(command).encode("utf-8")
        ).hexdigest(),
        "stored_reads": "NOT_AUTHORIZED",
        "effects": dict(ZERO_EFFECTS),
    })


def _prepare_waiting(args) -> dict[str, object]:
    state = LegacyArtifactSessionStateAdapter(
        package_path=args.package_source,
        acquisition_binding_path=args.acquisition_binding,
        json_loader=_load,
        acquisition_parser=acquisition_binding_from_payload,
    )
    package = state.load_package()
    acquisition = state.load_acquisition_binding()
    authorities = package.get("v3_authorities")
    session_payload = (
        authorities.get("session_acquisition")
        if isinstance(authorities, dict)
        else None
    )
    session = session_acquisition_authorization_from_payload(
        session_payload
    )
    history_value = package.get("v3_history_binding")
    if (
        acquisition.authority != session
        or acquisition.authority.cohort_id
        != UUID(str(package["cohort"]["cohort_id"]))
        or acquisition.contract_tickers
        != tuple(sorted(package.get("v3_admitted_contracts") or ()))
        or not isinstance(history_value, dict)
    ):
        raise ValueError("V3 waiting preparation identity changed")
    history = advanced_options_v3_history_binding_from_payload(
        history_value.get("binding")
    )
    session_binding = build_p8_preopen_session_binding(
        package=package,
        acquisition_binding=acquisition,
    )
    provider, pool = _provider()
    try:
        with provider.transaction(APP_ROLE) as unit:
            verified_history = (
                verify_advanced_options_v3_history_binding_cursor(
                    unit.cursor,
                    binding=history,
                    evidence=history_value.get("evidence"),
                )
            )
            persist_session_acquisition_authorization_cursor(
                unit.cursor,
                session,
            )
            orchestration = package["orchestration"]
            unit.cursor.execute(
                "INSERT INTO rt.service_plans ("
                "plan_id,cohort_id,session_date,stage_catalog_version,"
                "enabled_stages,config_sha256,deadline,scope_reference"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (plan_id) DO NOTHING",
                (
                    str(session_binding.plan_id),
                    str(session_binding.cohort_id),
                    session.session_date,
                    orchestration["catalog_version"],
                    list(orchestration["stage_order"]),
                    session_binding.binding_sha256,
                    session_binding.decision_stop,
                    (
                        "advanced-options-v3-waiting:"
                        f"{session_binding.binding_id}"
                    ),
                ),
            )
            unit.cursor.execute(
                "SELECT cohort_id::text,session_date,"
                "stage_catalog_version,enabled_stages,config_sha256,"
                "deadline,scope_reference FROM rt.service_plans "
                "WHERE plan_id=%s",
                (str(session_binding.plan_id),),
            )
            if unit.cursor.fetchone() != (
                str(session_binding.cohort_id),
                session.session_date,
                orchestration["catalog_version"],
                list(orchestration["stage_order"]),
                session_binding.binding_sha256,
                session_binding.decision_stop,
                (
                    "advanced-options-v3-waiting:"
                    f"{session_binding.binding_id}"
                ),
            ):
                raise ValueError(
                    "V3 waiting service plan identity changed"
                )
            unit.cursor.execute(
                "SELECT count(*) FROM "
                "rt.advanced_options_window_cycle_authorizations "
                "WHERE session_authorization_id=%s",
                (str(session.authorization_id),),
            )
            if unit.cursor.fetchone()[0] != 0:
                raise ValueError(
                    "V3 waiting prepare refuses a prewritten cycle authority"
                )
    finally:
        pool.closeall()
    args.output_directory.mkdir()
    package_path = args.output_directory / "PACKAGE.json"
    bundle_path = args.output_directory / "SESSION-BUNDLE.json"
    with package_path.open("xb") as stream:
        stream.write(args.package_source.read_bytes())
    store = P8ExecutionBundleStore(
        bundle_path,
        package=package,
        package_path=package_path,
        repository_root=REPOSITORY_ROOT,
    )
    bundle = store.write_waiting(
        acquisition,
        written_at=datetime.now(UTC),
    )
    validate_advanced_options_v3_runtime_identity(package, bundle)
    report = _report({
        "contract": "advanced-options-v3-waiting-prepare-v1",
        "status": "READY_WAITING_FOR_E",
        "package_sha256": package["package_sha256"],
        "bundle_sha256": bundle["bundle_sha256"],
        "session_binding_sha256": (
            bundle["session_binding"]["binding_sha256"]
        ),
        "acquisition_binding_sha256": (
            acquisition.binding_sha256
        ),
        "cohort_id": str(session.cohort_id),
        "journal_generation": str(session.journal_generation),
        "history_binding_sha256": (
            verified_history.binding.binding_sha256
        ),
        "prewritten_cycle_authorities": 0,
        "publication_authority": False,
        "read_pin_authority": False,
        "effects": dict(ZERO_EFFECTS),
    })
    _write_json(args.output_directory / "PREPARE-REPORT.json", report)
    return report


def _authorize_window(args) -> dict[str, object]:
    state = LegacyArtifactSessionStateAdapter(
        package_path=args.package,
        bundle_path=args.bundle,
        json_loader=_load,
        bundle_validator=validate_p8_execution_bundle,
    )
    package = state.load_package()
    bundle = state.load_execution_bundle()
    validate_advanced_options_v3_runtime_identity(package, bundle)
    production_binding = bundle.get("production_binding")
    binding = (
        None
        if production_binding is None
        else p8_production_binding_from_payload(production_binding)
    )
    session = session_acquisition_authorization_from_payload(
        bundle["v3_authorities"]["session_acquisition"]
    )
    if binding is None:
        session_binding = state.load_preopen_session_binding(bundle)
        if session_binding is None:
            raise TypeError(
                "P8 pre-open session binding must be an object"
            )
        history = advanced_options_v3_history_binding_from_payload(
            package["v3_history_binding"]["binding"]
        )
        target_plan_id = session_binding.plan_id
        target_cohort_id = session_binding.cohort_id
        history_binding_sha256 = history.binding_sha256
    else:
        control = binding.enrollment.control_plane
        target_plan_id = control.plan_id
        target_cohort_id = control.cohort_id
        history_binding_sha256 = binding.history.binding_sha256
    provider, pool = _provider()
    try:
        window = PostgresOrchestrationRepository(
            provider
        ).ensure_window(
            target_plan_id,
            WindowSpec(
                cohort_id=target_cohort_id,
                provider_tier="MASSIVE_ADVANCED",
                window_key=args.window_key.strip(),
                release_at=args.options_cutoff,
                cutoff=args.options_cutoff,
                deadline=args.window_deadline,
            ),
        )
        with provider.transaction(APP_ROLE) as unit:
            unit.cursor.execute(
                "SELECT receipt.created_at,receipt.cutoff,"
                "receipt.receipt_kind,window.state,"
                "receipt.reference_generation,receipt.universe_sha256,"
                "receipt.input_sha256,receipt.build_id,"
                "window.window_id::text,window.cohort_id::text,"
                "(SELECT stage.state FROM rt.stage_runs AS stage "
                "WHERE stage.window_id=receipt.window_id "
                "AND stage.stage_id='EQUITY_MATERIALIZE' "
                "ORDER BY stage.attempt DESC LIMIT 1) "
                "AS materialize_state "
                "FROM rt.cycle_receipts AS receipt "
                "JOIN rt.window_runs AS window USING(window_id) "
                "WHERE receipt.receipt_id=%s",
                (str(args.equity_receipt_id),),
            )
            equity_receipt = unit.cursor.fetchone()
            if (
                equity_receipt is None
                or str(equity_receipt[2]) != "INPUT_READY"
                or str(equity_receipt[10]) != "COMPLETE"
            ):
                raise ValueError(
                    "V3 authorize-window requires exact materialized INPUT_READY"
                )
            equity_plan = load_advanced_equity_control_plane_plan_cursor(
                unit.cursor,
                UUID(str(equity_receipt[9])),
            )
            if equity_plan is None:
                raise ValueError(
                    "V3 authorize-window Equity source is unavailable"
                )
            equity_input = build_advanced_options_equity_input_binding(
                target_options_window_id=window.window_id,
                target_cohort_id=target_cohort_id,
                source_cohort_id=equity_plan.cohort_id,
                source_cohort_sha256=equity_plan.cohort_sha256,
                source_manifest_id=equity_plan.manifest_id,
                source_manifest_sha256=equity_plan.manifest.sha256,
                source_profile_id=equity_plan.profile_id,
                source_profile_sha256=equity_plan.profile_sha256,
                equity_window_id=UUID(str(equity_receipt[8])),
                equity_receipt_id=args.equity_receipt_id,
                equity_cutoff=equity_receipt[1],
                reference_generation=int(equity_receipt[4]),
                universe_sha256=str(equity_receipt[5]),
                input_sha256=str(equity_receipt[6]),
                build_id=str(equity_receipt[7]),
                maximum_age_seconds=300,
                authorization_reference=args.authorization_reference,
            )
            prepared_at = datetime.now(UTC)
            options_cutoff = args.options_cutoff
            authority = build_window_cycle_authorization(
                session,
                authorization_reference=args.authorization_reference,
                target_window_id=window.window_id,
                target_cohort_id=target_cohort_id,
                prepared_at=prepared_at,
                history_binding_sha256=(
                    history_binding_sha256
                ),
                equity_binding_sha256=(
                    equity_input.binding_sha256
                ),
                equity_receipt_id=args.equity_receipt_id,
                equity_cutoff=equity_input.equity_cutoff,
                equity_receipt_created_at=equity_receipt[0],
                options_cutoff=options_cutoff,
                age_policy=TargetWindowAgePolicy(
                    "advanced-options-e-o-age-v1",
                    300,
                ),
                evidence_sha256=canonical_sha256({
                    "schema": (
                        "advanced-options-v3-window-authority-evidence-v1"
                    ),
                    "package_sha256": package["package_sha256"],
                    "bundle_sha256": bundle["bundle_sha256"],
                    "equity_binding_sha256": (
                        equity_input.binding_sha256
                    ),
                    "equity_receipt_id": str(
                        args.equity_receipt_id
                    ),
                    "options_cutoff": options_cutoff,
                    **(
                        {}
                        if args.activity_evidence_sha256 is None
                        else {
                            "activity_evidence_sha256": (
                                args.activity_evidence_sha256
                            )
                        }
                    ),
                }),
                build_id=equity_input.build_id,
                publication_authorization_reference=(
                    args.publication_authorization_reference
                ),
            )
            persist_advanced_options_equity_input_binding_cursor(
                unit.cursor,
                equity_input,
            )
            persist_window_cycle_authorization_cursor(
                unit.cursor,
                authority,
            )
            stored = load_window_cycle_authorization_cursor(
                unit.cursor,
                target_window_id=window.window_id,
                session_authorization_id=session.authorization_id,
            )
            if stored != authority:
                raise ValueError(
                    "V3 stored window authority identity changed"
                )
    finally:
        pool.closeall()
    return _report({
        "contract": "advanced-options-v3-window-authorization-v1",
        "status": "AUTHORIZED",
        "package_sha256": package["package_sha256"],
        "bundle_sha256": bundle["bundle_sha256"],
        "window_id": str(window.window_id),
        "window_key": window.window_key,
        "equity_receipt_id": str(args.equity_receipt_id),
        "equity_cutoff": equity_input.equity_cutoff,
        "options_cutoff": options_cutoff,
        "window_cycle_authorization_id": str(
            authority.authorization_id
        ),
        "window_cycle_authorization_sha256": (
            authority.authorization_sha256
        ),
        "publication_authorization_reference": (
            authority.publication_authorization_reference
        ),
        "activity_evidence_sha256": (
            args.activity_evidence_sha256
        ),
        "stored_reads": "NOT_AUTHORIZED",
        "effects": dict(ZERO_EFFECTS),
    })


def _status(args) -> dict[str, object]:
    state = LegacyArtifactSessionStateAdapter(
        package_path=args.package,
        bundle_path=args.bundle,
        json_loader=_load,
        bundle_validator=validate_p8_execution_bundle,
    )
    package = state.load_package()
    bundle = state.load_execution_bundle()
    validate_advanced_options_v3_runtime_identity(package, bundle)
    session = session_acquisition_authorization_from_payload(
        bundle["v3_authorities"]["session_acquisition"]
    )
    provider, pool = _provider()
    try:
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            status = advanced_options_v3_session_status_cursor(
                unit.cursor,
                session_authorization_id=session.authorization_id,
            )
    finally:
        pool.closeall()
    return _report({
        "contract": "advanced-options-v3-production-status-v1",
        "status": "AVAILABLE",
        "package_sha256": package["package_sha256"],
        "bundle_sha256": bundle["bundle_sha256"],
        "session": status,
        "stored_reads": "NOT_AUTHORIZED",
        "effects": dict(ZERO_EFFECTS),
    })


def main(argv=None) -> int:
    docker_result = _docker_service_main(argv)
    if docker_result is not None:
        return docker_result
    args = parse_args(argv)
    if args.command == "prepare":
        result = _prepare(args)
    elif args.command == "prepare-waiting":
        result = _prepare_waiting(args)
    elif args.command == "start-residents":
        result = _compose(args, RESIDENT_KINDS)
    elif args.command == "authorize-window":
        result = _authorize_window(args)
    elif args.command == "run-window":
        result = _compose(
            args,
            (AdvancedOptionsProcessKind.WINDOW,),
        )
    else:
        result = _status(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
