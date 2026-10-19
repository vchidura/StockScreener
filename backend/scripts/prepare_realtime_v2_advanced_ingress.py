#!/usr/bin/env python3
"""Create, prepare, revoke, or report durable Advanced ingress authority."""
from __future__ import annotations

import argparse
import json
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

import psycopg2
from dotenv import dotenv_values
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_ingress_plan import (  # noqa: E402
    AdvancedIngressBudgets,
    AdvancedIngressPlanRequest,
    build_advanced_stream_authorization,
)
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
)
from realtime.advanced_ingress_repository import (  # noqa: E402
    activate_advanced_runtime_profile_cursor,
    advanced_ingress_status_cursor,
    create_advanced_stream_authorization_cursor,
    prepare_advanced_ingress_plan_cursor,
    register_advanced_runtime_profile_cursor,
    resolve_advanced_ingress_plan_cursor,
    revoke_advanced_stream_authorization_cursor,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.relation_registry import ADMIN_ROLE  # noqa: E402
from realtime.security_v2 import build_role_names_v2  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc


def _common(subparser) -> None:
    subparser.add_argument("--database", required=True)
    subparser.add_argument("--confirm-database-name", required=True)
    subparser.add_argument(
        "--env-file",
        type=Path,
        default=BACKEND_DIR / ".env",
    )
    subparser.add_argument("--report-path", type=Path, required=True)
    subparser.add_argument("--ingress-policy-path", type=Path)
    subparser.add_argument("--ingress-policy-version")


def _budget_arguments(subparser) -> None:
    subparser.add_argument("--stream-sessions", type=int, required=True)
    subparser.add_argument(
        "--connection-attempts",
        type=int,
        required=True,
    )
    subparser.add_argument("--decoded-bytes", type=int, required=True)
    subparser.add_argument(
        "--rest-repair-requests",
        type=int,
        required=True,
    )
    subparser.add_argument(
        "--rest-repair-decoded-bytes",
        type=int,
        required=True,
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    authorize = commands.add_parser("authorize")
    _common(authorize)
    _budget_arguments(authorize)
    authorize.add_argument("--parent-authorization-id", type=UUID)
    authorize.add_argument(
        "--universe-target",
        type=int,
        action="append",
        required=True,
    )
    authorize.add_argument("--valid-from", type=date.fromisoformat, required=True)
    authorize.add_argument(
        "--valid-through",
        type=date.fromisoformat,
        required=True,
    )
    authorize.add_argument("--approval-source", required=True)
    authorize.add_argument("--approval-reference", required=True)
    authorize.add_argument("--execute", action="store_true")

    prepare = commands.add_parser("prepare")
    _common(prepare)
    _budget_arguments(prepare)
    prepare.add_argument("--authorization-id", type=UUID, required=True)
    prepare.add_argument(
        "--session-date",
        type=date.fromisoformat,
        required=True,
    )
    prepare.add_argument("--universe-target", type=int, required=True)
    prepare.add_argument("--preferred-cohort-id", type=UUID)
    prepare.add_argument("--cohort-selection-reference")
    prepare.add_argument("--canary-start-at", type=datetime.fromisoformat)
    prepare.add_argument("--canary-end-at", type=datetime.fromisoformat)
    prepare.add_argument("--execute", action="store_true")

    sync_profiles = commands.add_parser("sync-profiles")
    _common(sync_profiles)
    sync_profiles.add_argument("--universe-target", type=int, action="append")
    sync_profiles.add_argument("--execute", action="store_true")

    activate_profile = commands.add_parser("activate-profile")
    _common(activate_profile)
    activate_profile.add_argument(
        "--runtime-profile-sha256",
        required=True,
    )
    activate_profile.add_argument(
        "--stability-evidence-sha256",
        required=True,
    )
    activate_profile.add_argument(
        "--capacity-evidence-sha256",
        required=True,
    )
    activate_profile.add_argument(
        "--approval-reference",
        required=True,
    )
    activate_profile.add_argument("--execute", action="store_true")

    revoke = commands.add_parser("revoke")
    _common(revoke)
    revoke.add_argument("--authorization-id", type=UUID, required=True)
    revoke.add_argument("--revocation-reference", required=True)
    revoke.add_argument("--execute", action="store_true")

    status = commands.add_parser("status")
    _common(status)
    status.add_argument("--plan-id", type=UUID, required=True)

    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced ingress authority targets stocks_realtime_v2")
    args.env_file = args.env_file.resolve()
    if not args.env_file.is_file():
        parser.error("--env-file must exist")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if args.command == "prepare":
        try:
            AdvancedIngressPlanRequest(
                args.authorization_id,
                args.session_date,
                args.universe_target,
                _budgets(args),
                canary_start_at=args.canary_start_at,
                canary_end_at=args.canary_end_at,
            )
        except (TypeError, ValueError) as error:
            parser.error(str(error))
    return args


def _budgets(args) -> AdvancedIngressBudgets:
    return AdvancedIngressBudgets(
        args.stream_sessions,
        args.connection_attempts,
        args.decoded_bytes,
        args.rest_repair_requests,
        args.rest_repair_decoded_bytes,
    )


def _settings(path: Path) -> dict[str, str]:
    mapping = {
        str(key): str(value)
        for key, value in dotenv_values(path).items()
        if value is not None
    }
    return mapping


def _required(settings: dict[str, str], name: str) -> str:
    value = settings.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _runtime_catalog(settings: dict[str, str]):
    return load_advanced_ingress_runtime_catalog(
        Path(
            _required(
                settings,
                "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH",
            )
        ),
        expected_version=_required(
            settings,
            "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION",
        ),
    )


@contextmanager
def _admin_cursor(
    settings: dict[str, str],
    *,
    database: str,
):
    connection = psycopg2.connect(
        dbname=database,
        user=_required(settings, "POSTGRES_ADMIN_USER"),
        password=_required(settings, "POSTGRES_ADMIN_PASSWORD"),
        host=_required(settings, "DB_HOST"),
        port=_required(settings, "DB_PORT"),
        application_name="prepare_realtime_v2_advanced_ingress",
        options="-c timezone=UTC",
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                role = build_role_names_v2(database).name(ADMIN_ROLE)
                cursor.execute(
                    sql.SQL("SET LOCAL ROLE {}").format(
                        sql.Identifier(role)
                    )
                )
                yield cursor
    finally:
        connection.close()


def _app_provider(
    settings: dict[str, str],
    *,
    database: str,
):
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=_required(settings, "REALTIME_V2_APP_DSN"),
        options="-c timezone=UTC",
    )
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=database,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()
    return provider, pool


def _authorization_report(authorization, *, executed: bool) -> dict[str, object]:
    return {
        "command": "authorize",
        "mode": "EXECUTE" if executed else "PLAN",
        "status": authorization.status if executed else "PLANNED",
        "authorization_id": str(authorization.authorization_id),
        "authorization_reference": authorization.authorization_reference,
        "parent_authorization_id": (
            str(authorization.parent_authorization_id)
            if authorization.parent_authorization_id is not None
            else None
        ),
        "scope_sha256": authorization.scope_sha256,
        "allowed_universe_targets": (
            authorization.allowed_universe_targets
        ),
        "valid_from": authorization.valid_from.isoformat(),
        "valid_through": authorization.valid_through.isoformat(),
        "maximum_budgets": asdict(authorization.maximum_budgets),
        "runtime_policy_sha256": authorization.runtime_policy_sha256,
        "provider_requests": 0,
        "credentials_exposed": False,
    }


def _plan_report(plan, *, executed: bool) -> dict[str, object]:
    return {
        "command": "prepare",
        "mode": "EXECUTE" if executed else "PLAN",
        "status": plan.status if executed else "PLANNED",
        "plan_id": str(plan.plan_id),
        "plan_sha256": plan.plan_sha256,
        "authorization_id": str(plan.authorization_id),
        "authorization_reference": plan.authorization_reference,
        "cohort_id": str(plan.cohort_id),
        "manifest_id": str(plan.manifest_id),
        "session_date": plan.session_date.isoformat(),
        "universe_target": plan.universe_target,
        "ingress_mode": plan.mode,
        "runtime_profile_version": plan.runtime_profile_version,
        "runtime_profile_sha256": plan.runtime_profile_sha256,
        "configuration_sha256": plan.configuration_sha256,
        "runtime_policy_sha256": plan.runtime_policy_sha256,
        "authorized_start_at": plan.authorized_start_at.isoformat(),
        "authorized_end_at": plan.authorized_end_at.isoformat(),
        "budgets": asdict(plan.budgets),
        "provider_requests": 0,
        "credentials_exposed": False,
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    settings = _settings(args.env_file)
    if args.ingress_policy_path is not None:
        settings["REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH"] = str(args.ingress_policy_path.resolve())
    if args.ingress_policy_version is not None:
        settings["REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION"] = args.ingress_policy_version
    now = datetime.now(UTC)
    if args.command == "authorize":
        catalog = _runtime_catalog(settings)
        target = tuple(args.universe_target)
        if len(target) != 1:
            raise ValueError(
                "standing authorization requires one universe target"
            )
        catalog.profile_for_target(target[0])
        ladder_index = catalog.scale_policy.universe_ladder.index(target[0])
        if (ladder_index == 0) != (
            args.parent_authorization_id is None
        ):
            raise ValueError(
                "only the first ladder target omits a parent authorization"
            )
        authorization = build_advanced_stream_authorization(
            parent_authorization_id=args.parent_authorization_id,
            allowed_universe_targets=target,
            valid_from=args.valid_from,
            valid_through=args.valid_through,
            maximum_budgets=_budgets(args),
            runtime_policy_sha256=catalog.scale_policy.sha256,
            approval_source=args.approval_source,
            approval_reference=args.approval_reference,
        )
        if args.execute:
            from options.calendar import OptionExchangeCalendar

            with _admin_cursor(settings, database=args.database) as cursor:
                authorization = create_advanced_stream_authorization_cursor(
                    cursor,
                    authorization,
                    promotion_ladder=(
                        catalog.scale_policy.universe_ladder
                    ),
                    calendar=OptionExchangeCalendar("XNYS"),
                )
        report = _authorization_report(
            authorization,
            executed=args.execute,
        )
    elif args.command == "sync-profiles":
        catalog = _runtime_catalog(settings)
        targets = None if args.universe_target is None else set(args.universe_target)
        if targets is not None:
            for target in targets:
                catalog.profile_for_target(target)
        selected = tuple(
            (index, profile) for index, profile in enumerate(catalog.profiles)
            if targets is None or profile.universe_target in targets
        )
        registrations = []
        if args.execute:
            with _admin_cursor(settings, database=args.database) as cursor:
                for index, profile in selected:
                    registrations.append(
                        register_advanced_runtime_profile_cursor(
                            cursor,
                            profile,
                            runtime_policy_sha256=(
                                catalog.scale_policy.sha256
                            ),
                            ladder_position=index,
                            previous_universe_target=(
                                catalog.profiles[index - 1].universe_target
                                if index > 0
                                else None
                            ),
                        )
                    )
        report = {
            "command": "sync-profiles",
            "mode": "EXECUTE" if args.execute else "PLAN",
            "status": "READY" if args.execute else "PLANNED",
            "catalog_version": catalog.version,
            "runtime_policy_sha256": catalog.scale_policy.sha256,
            "profiles": [
                {
                    "version": profile.version,
                    "universe_target": profile.universe_target,
                    "runtime_profile_sha256": profile.sha256,
                    "configuration_sha256": (
                        profile.configuration_sha256(
                            catalog.scale_policy.sha256
                        )
                    ),
                    "status": (
                        registrations[index].status
                        if registrations
                        else "CANDIDATE"
                    ),
                }
                for index, (_, profile) in enumerate(selected)
            ],
            "provider_requests": 0,
            "credentials_exposed": False,
        }
    elif args.command == "activate-profile":
        catalog = _runtime_catalog(settings)
        profile = catalog.profile_for_sha(
            args.runtime_profile_sha256
        )
        status = "PLANNED"
        if args.execute:
            with _admin_cursor(settings, database=args.database) as cursor:
                registration = activate_advanced_runtime_profile_cursor(
                    cursor,
                    profile.sha256,
                    stability_evidence_sha256=(
                        args.stability_evidence_sha256
                    ),
                    capacity_evidence_sha256=(
                        args.capacity_evidence_sha256
                    ),
                    approval_reference=args.approval_reference,
                    activated_at=now,
                )
                status = registration.status
        report = {
            "command": "activate-profile",
            "mode": "EXECUTE" if args.execute else "PLAN",
            "status": status,
            "runtime_profile_sha256": profile.sha256,
            "runtime_profile_version": profile.version,
            "universe_target": profile.universe_target,
            "provider_requests": 0,
            "credentials_exposed": False,
        }
    elif args.command == "revoke":
        if not args.execute:
            report = {
                "command": "revoke",
                "mode": "PLAN",
                "status": "PLANNED",
                "authorization_id": str(args.authorization_id),
                "provider_requests": 0,
                "credentials_exposed": False,
            }
        else:
            with _admin_cursor(settings, database=args.database) as cursor:
                authorization = (
                    revoke_advanced_stream_authorization_cursor(
                        cursor,
                        args.authorization_id,
                        revoked_at=now,
                        revocation_reference=args.revocation_reference,
                    )
                )
            report = {
                "command": "revoke",
                "mode": "EXECUTE",
                "status": authorization.status,
                "authorization_id": str(authorization.authorization_id),
                "provider_requests": 0,
                "credentials_exposed": False,
            }
    else:
        provider, pool = _app_provider(settings, database=args.database)
        try:
            if args.command == "prepare":
                catalog = _runtime_catalog(settings)
                with provider.transaction(
                    APP_ROLE,
                    read_only=not args.execute,
                ) as unit:
                    plan = resolve_advanced_ingress_plan_cursor(
                        unit.cursor,
                        AdvancedIngressPlanRequest(
                            authorization_id=args.authorization_id,
                            session_date=args.session_date,
                            universe_target=args.universe_target,
                            budgets=_budgets(args),
                            preferred_cohort_id=(
                                args.preferred_cohort_id
                            ),
                            cohort_selection_reference=(
                                args.cohort_selection_reference
                            ),
                            canary_start_at=args.canary_start_at,
                            canary_end_at=args.canary_end_at,
                        ),
                        runtime_catalog=catalog,
                    )
                    if args.execute:
                        prepare_advanced_ingress_plan_cursor(
                            unit.cursor,
                            plan,
                            checked_at=now,
                        )
                report = _plan_report(plan, executed=args.execute)
            else:
                with provider.transaction(
                    APP_ROLE,
                    read_only=True,
                ) as unit:
                    status = advanced_ingress_status_cursor(
                        unit.cursor,
                        args.plan_id,
                        checked_at=now,
                    )
                report = {
                    "command": "status",
                    "mode": "STATUS",
                    "status": status.plan_status,
                    "authorization_status": (
                        status.authorization_status
                    ),
                    "plan_id": str(status.plan_id),
                    "session_date": status.session_date.isoformat(),
                    "universe_target": status.universe_target,
                    "usage": {
                        "stream_sessions": status.stream_sessions,
                        "connection_attempts": (
                            status.connection_attempts
                        ),
                        "decoded_bytes": status.decoded_bytes,
                        "rest_repair_requests": (
                            status.rest_repair_requests
                        ),
                        "rest_repair_decoded_bytes": (
                            status.rest_repair_decoded_bytes
                        ),
                    },
                    "provider_requests": 0,
                    "credentials_exposed": False,
                }
        finally:
            pool.closeall()
    write_report(args.report_path, report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
