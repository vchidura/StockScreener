#!/usr/bin/env python3
"""Replay one exact stored native-repair plan without a provider transport."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.acquisition_ledger import load_raw_payload_cursor  # noqa: E402
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_recent_repair import (  # noqa: E402
    POLICY,
    NativeRepairPlan,
    _native_request_spec,
    execute_recent_native_repair,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import (  # noqa: E402
    require_new_workspace_report,
)
from scripts.repair_realtime_v2_recent_sessions import (  # noqa: E402
    _load_scope,
    _load_scope_with_identity_continuity,
    _repair_identity_cutoff,
)


UTC = timezone.utc
_SHA256_HEX = frozenset("0123456789abcdef")


class _DeniedTransport:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, url, params):
        self.calls += 1
        raise RuntimeError(
            "provider transport is forbidden during retained-plan replay"
        )


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("stored plan timestamp must include an offset")
    return parsed.astimezone(UTC)


def _select_stored_plan(rows, plan_sha256: str) -> dict[str, object]:
    matches = []
    for config_sha256, scope_reference in rows:
        if str(config_sha256) != plan_sha256:
            continue
        if isinstance(scope_reference, dict):
            identity = scope_reference
        else:
            try:
                identity = json.loads(scope_reference)
            except (json.JSONDecodeError, TypeError):
                continue
        if (
            identity.get("policy") == POLICY
            and canonical_sha256(identity) == plan_sha256
        ):
            matches.append(identity)
    if len(matches) != 1:
        raise ValueError(
            f"expected one exact stored native-repair plan, found {len(matches)}"
        )
    return matches[0]


def _load_stored_plan(provider, cohort_id: UUID, plan_sha256: str):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT config_sha256,scope_reference "
            "FROM rt.service_plans "
            "WHERE cohort_id=%s AND config_sha256=%s",
            (str(cohort_id), plan_sha256),
        )
        rows = tuple(unit.cursor.fetchall())
    return _select_stored_plan(rows, plan_sha256)


def _reconstruct_plan(provider, cohort_id: UUID, identity):
    sessions = tuple(
        date.fromisoformat(str(value))
        for value in identity["sessions"]
    )
    cutoff = _repair_identity_cutoff(sessions[-1])
    try:
        identities, _ = _load_scope(provider, cohort_id, cutoff)
        continuity = ()
    except ValueError:
        identities, continuity = _load_scope_with_identity_continuity(
            provider,
            cohort_id,
            cutoff,
            known_by=datetime.now(UTC),
        )
    plan = NativeRepairPlan(
        cohort_id=cohort_id,
        identities=identities,
        sessions=sessions,
        authorized_until=_timestamp(identity["authorized_until"]),
        request_ceiling=int(identity["request_ceiling"]),
        byte_ceiling=int(identity["byte_ceiling"]),
        response_byte_limit=int(identity["response_byte_limit"]),
        authorization_reference=str(identity["authorization_reference"]),
        provider_authorization_reference=str(
            identity["provider_authorization_reference"]
        ),
        expected_cost=str(identity["expected_cost"]),
        native_intervals=tuple(identity["native_intervals"]),
        identity_continuity=continuity,
    )
    if plan.sha256 != canonical_sha256(identity):
        raise ValueError("stored native-repair plan identity changed")
    return plan


def _verify_retained_requests(provider, plan: NativeRepairPlan) -> int:
    expected = {}
    for session in plan.sessions:
        for interval in plan.native_intervals:
            for identity in plan.identities:
                request = _native_request_spec(
                    plan,
                    identity,
                    session,
                    interval,
                )
                request_id = uuid5(
                    NAMESPACE_URL,
                    f"{POLICY}:{request.request_sha256}",
                )
                expected[request_id] = request.request_sha256
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT request_id,request_sha256,account_alias,cohort_id,state,"
            "outcome,completeness,http_status,payload_id "
            "FROM rt.provider_request_ledger "
            "WHERE request_id=ANY(%s::uuid[])",
            ([str(value) for value in expected],),
        )
        rows = tuple(unit.cursor.fetchall())
        if len(rows) != len(expected):
            raise ValueError(
                f"retained request coverage is {len(rows)}/{len(expected)}"
            )
        for row in rows:
            request_id = UUID(str(row[0]))
            if (
                row[1] != expected[request_id]
                or row[2] != "MASSIVE_DEVELOPER"
                or str(row[3]) != str(plan.cohort_id)
                or tuple(row[4:8]) != ("COMPLETE", "OK", "COMPLETE", 200)
                or row[8] is None
            ):
                raise ValueError(
                    f"retained request is not terminal and exact: {request_id}"
                )
            load_raw_payload_cursor(unit.cursor, UUID(str(row[8])))
    return len(expected)


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(1, 2, dsn=dsn, options="-c timezone=UTC")
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
        ),
    )
    provider.verify()
    return provider, pool


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("retained-plan replay targets stocks_realtime_v2")
    if (
        len(args.plan_sha256) != 64
        or any(value not in _SHA256_HEX for value in args.plan_sha256)
    ):
        parser.error("--plan-sha256 must be a lowercase SHA-256 digest")
    args.report_path = require_new_workspace_report(
        args.report_path,
        WORKSPACE_DIR,
    )
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    provider, pool = _provider()
    try:
        identity = _load_stored_plan(
            provider,
            args.cohort_id,
            args.plan_sha256,
        )
        plan = _reconstruct_plan(provider, args.cohort_id, identity)
        retained_request_count = _verify_retained_requests(provider, plan)
        if not args.execute:
            report = {
                "contract": "retained-native-plan-replay-v1",
                "mode": "PLAN",
                "status": "READY",
                "cohort_id": str(args.cohort_id),
                "plan_sha256": plan.sha256,
                "retained_request_count": retained_request_count,
                "provider_requests": 0,
                "provider_transport_calls": 0,
                "database_writes": 0,
            }
        else:
            transport = _DeniedTransport()
            outcome = execute_recent_native_repair(
                provider,
                transport,
                plan,
                clock=lambda: datetime.now(UTC),
            )
            report = {
                "contract": "retained-native-plan-replay-v1",
                "mode": "EXECUTE",
                **outcome,
                "retained_request_count": retained_request_count,
                "provider_transport_calls": transport.calls,
            }
            if (
                report["status"] != "COMPLETE"
                or report["provider_requests"] != 0
                or report["provider_response_replays"]
                != retained_request_count
                or report["provider_transport_calls"] != 0
                or report["release_advanced"] is not False
            ):
                raise RuntimeError(
                    "retained native-plan replay did not remain provider-free"
                )
        report["semantic_sha256"] = canonical_sha256(report)
        write_report(args.report_path, report)
        print(json.dumps(report, default=str, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
