#!/usr/bin/env python3
"""Run the receipt-bound canonical V3 Stock Alert detector resident."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.canonical_stock_alert_repository import (  # noqa: E402
    build_canonical_stock_alert_batch_cursor,
    canonical_stock_alert_health_cursor,
    load_due_canonical_stock_alert_receipts_cursor,
    persist_canonical_stock_alert_batch_cursor,
)
from realtime.advanced_realtime_session_operations import (  # noqa: E402
    AdvancedSessionOperation,
    diagnostic_authorization_from_payload,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.advanced_realtime_v3_runtime import (  # noqa: E402
    load_current_advanced_realtime_session_scope_cursor,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID)
    parser.add_argument("--session-date", type=date.fromisoformat)
    parser.add_argument("--dynamic-scope", action="store_true")
    parser.add_argument(
        "--operation",
        type=AdvancedSessionOperation,
        choices=(
            AdvancedSessionOperation.PRIMARY_SERVING,
            AdvancedSessionOperation.DIAGNOSTIC_NON_SERVING,
        ),
        required=True,
    )
    parser.add_argument(
        "--diagnostic-authorization",
        type=Path,
    )
    parser.add_argument("--poll-seconds", type=float, default=1)
    parser.add_argument("--claim-limit", type=int, default=8)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--health", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.diagnostic_authorization is not None:
        args.diagnostic_authorization = (
            args.diagnostic_authorization.resolve()
        )
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.poll_seconds <= 0
        or not 1 <= args.claim_limit <= 32
        or (
            args.dynamic_scope
            and (
                args.cohort_id is not None
                or args.session_date is not None
            )
        )
        or (
            not args.dynamic_scope
            and (
                args.cohort_id is None
                or args.session_date is None
                or args.cohort_id.int == 0
            )
        )
        or (args.health and (args.continuous or args.execute))
        or (
            not args.health
            and (not args.continuous or not args.execute)
        )
        or (
            args.operation
            is AdvancedSessionOperation.DIAGNOSTIC_NON_SERVING
            and (
                args.diagnostic_authorization is None
                or not args.diagnostic_authorization.is_file()
            )
        )
        or (
            args.operation
            is AdvancedSessionOperation.PRIMARY_SERVING
            and args.diagnostic_authorization is not None
        )
    ):
        parser.error("canonical Stock Alert resident scope is invalid")
    args.diagnostic_identity = None
    if (
        args.operation
        is AdvancedSessionOperation.DIAGNOSTIC_NON_SERVING
    ):
        try:
            payload = json.loads(
                args.diagnostic_authorization.read_text(
                    encoding="utf-8"
                )
            )
            identity = diagnostic_authorization_from_payload(
                payload
            )
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as error:
            parser.error(
                f"diagnostic authorization is invalid: {error}"
            )
        if (
            identity.session_date != args.session_date
            or identity.cohort_id != args.cohort_id
        ):
            parser.error(
                "diagnostic authorization scope does not match"
            )
        args.diagnostic_identity = identity
    return args


def _provider(args):
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.APP_WORKER,
    )
    qualification = (
        os.environ.get("REALTIME_V3_VALIDATION_MODE")
        == "DEVELOPMENT_DIAGNOSTIC"
        and os.environ.get(
            "REALTIME_V3_PROVIDER_FREE_QUALIFICATION"
        )
        == "ENABLED"
    )
    diagnostic = (
        args.operation
        is AdvancedSessionOperation.DIAGNOSTIC_NON_SERVING
    )
    role_prefix = (
        os.environ.get("REALTIME_V2_ROLE_PREFIX", "").strip()
        if qualification
        else EXPECTED_V2_DATABASE_NAME
    )
    if not role_prefix:
        raise RuntimeError(
            "canonical Stock Alert role prefix is unavailable"
        )
    if diagnostic and (
        not qualification
        or role_prefix == EXPECTED_V2_DATABASE_NAME
        or os.environ.get(
            "REALTIME_V3_DIAGNOSTIC_AUTHORIZATION_ID",
            "",
        ).strip()
        != str(args.diagnostic_identity.authorization_id)
        or any(
            os.environ.get(name, "").strip()
            for name in ("POLYGON_API_KEY", "MASSIVE_API_KEY")
        )
    ):
        raise RuntimeError(
            "diagnostic operation is not an owned provider-free scope"
        )
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=config.app_dsn,
        options="-c timezone=UTC",
    )
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


def _runtime_scope(provider, args):
    if not args.dynamic_scope:
        return args.cohort_id, args.session_date
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        scope = load_current_advanced_realtime_session_scope_cursor(
            unit.cursor,
            checked_at=datetime.now(UTC),
        )
    return (
        (None, None)
        if scope is None
        else (scope.cohort_id, scope.session_date)
    )


def _health(provider, args) -> int:
    cohort_id, _session_date = _runtime_scope(provider, args)
    if cohort_id is None:
        print(json.dumps({
            "status": "READY_WAITING_FOR_SESSION",
            "operation": args.operation.value,
            "effects": {
                "provider": 0,
                "publication": 0,
                "pointer": 0,
                "read_pin": 0,
                "delivery": 0,
                "execution": 0,
            },
        }, sort_keys=True), flush=True)
        return 0
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        status = canonical_stock_alert_health_cursor(
            unit.cursor,
            cohort_id=cohort_id,
        )
    status["operation"] = args.operation.value
    status["diagnostic_authorization_id"] = (
        str(args.diagnostic_identity.authorization_id)
        if args.diagnostic_identity is not None
        else None
    )
    if (
        args.operation is AdvancedSessionOperation.PRIMARY_SERVING
        and status.get("status") == "READY"
        and status.get("completed_batches") == 0
    ):
        status["status"] = "READY_WAITING_FOR_E"
        status["reason"] = "INPUT_READY_UNAVAILABLE"
    status["effects"] = {
        "provider": 0,
        "publication": 0,
        "pointer": 0,
        "read_pin": 0,
        "delivery": 0,
        "execution": 0,
    }
    print(json.dumps(status, sort_keys=True), flush=True)
    return (
        0
        if status["status"] in {"READY", "READY_WAITING_FOR_E"}
        else 1
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    provider, pool = _provider(args)
    try:
        if args.health:
            return _health(provider, args)
        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        while not stop.is_set():
            cohort_id, session_date = _runtime_scope(provider, args)
            if cohort_id is None:
                stop.wait(args.poll_seconds)
                continue
            reports = []
            with provider.transaction(
                APP_ROLE,
                dict_cursor=True,
                isolation_level="SERIALIZABLE",
            ) as unit:
                receipts = load_due_canonical_stock_alert_receipts_cursor(
                    unit.cursor,
                    cohort_id=cohort_id,
                    session_date=session_date,
                    limit=args.claim_limit,
                )
                for receipt in receipts:
                    batch = build_canonical_stock_alert_batch_cursor(
                        unit.cursor,
                        receipt,
                        session_date=session_date,
                    )
                    reports.append(
                        persist_canonical_stock_alert_batch_cursor(
                            unit.cursor,
                            batch,
                        )
                    )
            if reports:
                print(json.dumps({
                    "status": "COMPLETE",
                    "operation": args.operation.value,
                    "diagnostic_authorization_id": (
                        str(
                            args.diagnostic_identity.authorization_id
                        )
                        if args.diagnostic_identity is not None
                        else None
                    ),
                    "batches": reports,
                    "provider_fetch_performed": False,
                    "publication_performed": False,
                    "pointer_performed": False,
                    "read_pin_performed": False,
                    "delivery_performed": False,
                    "execution_performed": False,
                }, default=str, sort_keys=True), flush=True)
            stop.wait(args.poll_seconds)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
