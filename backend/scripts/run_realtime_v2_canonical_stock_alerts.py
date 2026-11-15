#!/usr/bin/env python3
"""Run the receipt-bound canonical V3 Stock Alert detector resident."""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

from psycopg2 import DatabaseError
from psycopg2.pool import ThreadedConnectionPool


_PUBLICATION_RETRY_SECONDS = 60
# Nov 9: a SERIALIZABLE batch that lost to another transaction crashed the role. The batch stays due, so the next
# poll repeats it. A conflict that never clears still fails.
_RETRYABLE_POSTGRES_SQLSTATES = frozenset({"40001", "40P01"})
MAXIMUM_CONSECUTIVE_CONFLICTS = 30
BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.canonical_stock_alert_publication import (  # noqa: E402
    publish_due_canonical_stock_alerts,
)
from realtime.canonical_stock_alert_repository import (  # noqa: E402
    build_canonical_stock_alert_batch_cursor,
    canonical_stock_alert_health_cursor,
    canonical_stock_alert_state_fingerprint_cursor,
    claim_canonical_stock_alert_receipt_cursor,
    list_due_canonical_stock_alert_receipts_cursor,
    load_due_canonical_stock_alert_receipts_cursor,
    persist_canonical_stock_alert_batch_cursor,
)
from realtime.advanced_realtime_session_operations import (  # noqa: E402
    AdvancedSessionOperation,
    diagnostic_authorization_from_payload,
)
from realtime.database_access import (  # noqa: E402
    RealtimeDatabasePrincipal,
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
    RoleScopedConnectionProvider,
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
from realtime.worker_database import RealtimeProcessDatabase  # noqa: E402
from realtime.advanced_realtime_v3_runtime import (  # noqa: E402
    load_current_advanced_realtime_session_scope_cursor,
)


UTC = timezone.utc


def should_retry_conflict(error: DatabaseError, consecutive_conflicts: int) -> bool:
    return (
        error.pgcode in _RETRYABLE_POSTGRES_SQLSTATES
        and consecutive_conflicts <= MAXIMUM_CONSECUTIVE_CONFLICTS
    )


# R71 (Nov 13-15): the whole batch ran SERIALIZABLE, so every bar, reference and context row it read took a predicate
# lock. At 38 members with every rule evaluated one batch held ~38,600 SIReadLocks and exhausted Postgres
# max_pred_locks_per_transaction (64): "out of shared memory", a crash loop, and a database-wide slowdown that pushed
# Equity into cold windows. SNAPSHOT (default) builds each batch in a READ ONLY REPEATABLE READ snapshot (one consistent
# view, no predicate locks; the inputs are as-of reads bounded by the receipt's cutoff and known_by), then claims the
# receipt and writes the batch in a short SERIALIZABLE transaction that first checks the detector state is still the one
# the batch was built from. SERIALIZABLE is the r102 behaviour (one transaction for claim, reads and writes), kept as a
# switch-only fallback. The rule module is not touched (its source is the detector version: memory stock-alerts-version-reset).
STOCK_ALERT_BATCH_READS_ENV = "REALTIME_V2_STOCK_ALERT_BATCH_READS"
BATCH_READS_SNAPSHOT = "SNAPSHOT"
BATCH_READS_SERIALIZABLE = "SERIALIZABLE"


class StockAlertBatchInputsChanged(RuntimeError):
    """The detector state changed between the snapshot build and the write: the batch is rebuilt on the next poll."""

    pgcode = "40001"


def batch_reads(environ=None) -> str:
    value = (environ if environ is not None else os.environ).get(
        STOCK_ALERT_BATCH_READS_ENV,
        BATCH_READS_SNAPSHOT,
    ).strip().upper() or BATCH_READS_SNAPSHOT
    if value not in {BATCH_READS_SNAPSHOT, BATCH_READS_SERIALIZABLE}:
        raise ValueError(
            f"{STOCK_ALERT_BATCH_READS_ENV} must be "
            f"{BATCH_READS_SNAPSHOT} or {BATCH_READS_SERIALIZABLE}"
        )
    return value


def run_due_batches(
    provider,
    *,
    cohort_id,
    session_date,
    claim_limit: int,
    reads: str = BATCH_READS_SNAPSHOT,
) -> list:
    """Build and persist the due batches in cutoff order; returns the persisted batch documents."""
    if reads == BATCH_READS_SERIALIZABLE:
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
                limit=claim_limit,
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
        return reports
    if reads != BATCH_READS_SNAPSHOT:
        raise ValueError("canonical Stock Alert batch read mode is invalid")
    with provider.transaction(
        APP_ROLE,
        dict_cursor=True,
        read_only=True,
        isolation_level="REPEATABLE READ",
    ) as unit:
        receipts = list_due_canonical_stock_alert_receipts_cursor(
            unit.cursor,
            cohort_id=cohort_id,
            session_date=session_date,
            limit=claim_limit,
        )
    reports = []
    for receipt in receipts:
        # A receipt whose window row another transaction holds is skipped by the claim (SKIP LOCKED): probe that in
        # a tiny transaction first, so a held receipt does not cost a full build on every poll.
        with provider.transaction(
            APP_ROLE,
            dict_cursor=True,
        ) as unit:  # the server default, READ COMMITTED
            claimable =claim_canonical_stock_alert_receipt_cursor(
                unit.cursor,
                receipt_id=str(receipt["receipt_id"]),
                cohort_id=cohort_id,
                session_date=session_date,
            ) is not None
        if not claimable:
            break
        # One snapshot per receipt, taken after the previous receipt's write committed: the next batch's prior state
        # is the state the previous batch wrote, exactly as in the single SERIALIZABLE transaction.
        with provider.transaction(
            APP_ROLE,
            dict_cursor=True,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            batch = build_canonical_stock_alert_batch_cursor(
                unit.cursor,
                receipt,
                session_date=session_date,
            )
            built_from = canonical_stock_alert_state_fingerprint_cursor(
                unit.cursor,
                detector_sha256=batch.version.detector_sha256,
                cohort_id=batch.cohort_id,
            )
        with provider.transaction(
            APP_ROLE,
            dict_cursor=True,
            isolation_level="SERIALIZABLE",
        ) as unit:
            claimed = claim_canonical_stock_alert_receipt_cursor(
                unit.cursor,
                receipt_id=str(receipt["receipt_id"]),
                cohort_id=cohort_id,
                session_date=session_date,
            )
            if claimed is None:
                # Written meanwhile or held by another claimer: later receipts depend on its state, so stop here
                # and start again from a fresh snapshot on the next poll.
                break
            if canonical_stock_alert_state_fingerprint_cursor(
                unit.cursor,
                detector_sha256=batch.version.detector_sha256,
                cohort_id=batch.cohort_id,
            ) != built_from:
                raise StockAlertBatchInputsChanged(
                    "canonical Stock Alert detector state changed after the snapshot build"
                )
            reports.append(
                persist_canonical_stock_alert_batch_cursor(
                    unit.cursor,
                    batch,
                )
            )
    return reports


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


def _publication_databases(pool):
    """Serving publication role plans, under the same scoped app login."""
    app_role = f"{EXPECTED_V2_DATABASE_NAME}_app"
    roles = RoleScopedConnectionProvider(
        {RealtimeDatabasePrincipal.RUNTIME: pool},
        {
            RealtimeDatabaseRole.COMPUTE: app_role,
            RealtimeDatabaseRole.PUBLICATION: app_role,
        },
    )
    return tuple(
        RealtimeProcessDatabase(
            RealtimeProcessDatabasePlan(
                f"worker:canonical_stock_alerts:{role.value.lower()}",
                role,
                RealtimePoolBudget(0, 1),
            ),
            roles,
        )
        for role in (
            RealtimeDatabaseRole.COMPUTE,
            RealtimeDatabaseRole.PUBLICATION,
        )
    )


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
        serving = args.operation is AdvancedSessionOperation.PRIMARY_SERVING
        databases = _publication_databases(pool) if serving else None
        next_publication_at = 0.0
        conflicts = 0
        while not stop.is_set():
            cohort_id, session_date = _runtime_scope(provider, args)
            if cohort_id is None:
                stop.wait(args.poll_seconds)
                continue
            reports = []
            try:
                reports = run_due_batches(
                    provider,
                    cohort_id=cohort_id,
                    session_date=session_date,
                    claim_limit=args.claim_limit,
                    reads=batch_reads(),
                )
            except (DatabaseError, StockAlertBatchInputsChanged) as error:
                conflicts += 1
                if not should_retry_conflict(error, conflicts):
                    raise
                print(json.dumps({
                    "status": "RETRYING_WRITE_CONFLICT",
                    "sqlstate": error.pgcode,
                    "consecutive_conflicts": conflicts,
                    "provider_fetch_performed": False,
                }, sort_keys=True), flush=True)
                stop.wait(args.poll_seconds)
                continue
            conflicts = 0
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
            if serving and time.monotonic() >= next_publication_at:
                # Publication must never stop batch production; an unpublished
                # batch stays due and is retried after a bounded pause.
                try:
                    published = publish_due_canonical_stock_alerts(
                        provider,
                        compute=databases[0],
                        publication=databases[1],
                        cohort_id=cohort_id,
                        session_date=session_date,
                    )
                except Exception as error:
                    next_publication_at = (
                        time.monotonic() + _PUBLICATION_RETRY_SECONDS
                    )
                    print(json.dumps({
                        "status": "PUBLICATION_FAILED",
                        "reason": f"{type(error).__name__}:{error}"[:300],
                        "retry_seconds": _PUBLICATION_RETRY_SECONDS,
                        "provider_fetch_performed": False,
                    }, sort_keys=True), flush=True)
                else:
                    if published:
                        print(json.dumps({
                            "status": "STOCK_ALERTS_PUBLICATION",
                            "publications": published,
                            "provider_fetch_performed": False,
                        }, default=str, sort_keys=True), flush=True)
            stop.wait(args.poll_seconds)
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
