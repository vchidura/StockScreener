#!/usr/bin/env python3
"""Audit durable target-9 evidence and derive same-session stream budgets."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

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


TARGET = 9
HEADROOM = 1.30


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--before", type=datetime.fromisoformat, required=True)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.before.tzinfo is None
        or args.before.utcoffset() is None
        or not 1 <= args.limit <= 50
        or args.output.exists()
    ):
        parser.error("target-9 promotion audit scope is invalid")
    args.before = args.before.astimezone(timezone.utc)
    args.output = args.output.resolve()
    return args


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
        2,
        dsn=dsn,
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


def _rows(cursor, statement, parameters=()):
    cursor.execute(statement, parameters)
    return tuple(dict(row) for row in cursor.fetchall())


def _ceil_headroom(value: int) -> int:
    return int(math.ceil(value * HEADROOM))


def main(argv=None) -> int:
    args = parse_args(argv)
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            profiles = _rows(
                unit.cursor,
                "SELECT runtime_profile_sha256,runtime_profile_version,"
                "configuration_sha256,runtime_policy_sha256,"
                "stability_evidence_sha256,capacity_evidence_sha256,"
                "activation_approval_reference,activated_at "
                "FROM rt.advanced_ingress_runtime_profiles "
                "WHERE universe_target=%s AND provider_tier='MASSIVE_ADVANCED' "
                "AND feed='STOCKS' AND channels=ARRAY['AM']::text[] "
                "AND status='ACTIVE'",
                (TARGET,),
            )
            plans = _rows(
                unit.cursor,
                "SELECT plan.plan_id::text,plan.service_plan_id::text,"
                "plan.cohort_id::text,plan.session_date,plan.status,"
                "plan.terminal_outcome,plan.completed_at,"
                "usage.stream_session_count,"
                "usage.connection_attempt_count,usage.decoded_byte_count,"
                "usage.rest_repair_request_count,"
                "usage.rest_repair_decoded_byte_count "
                "FROM rt.advanced_ingress_plans AS plan "
                "JOIN rt.advanced_ingress_plan_usage AS usage "
                "USING(plan_id) "
                "WHERE plan.universe_target=%s "
                "AND plan.status='COMPLETE' "
                "AND plan.completed_at<%s "
                "ORDER BY plan.completed_at DESC LIMIT %s",
                (TARGET, args.before, args.limit),
            )
            service_plan_ids = [
                UUID(row["service_plan_id"])
                for row in plans
                if row["service_plan_id"] is not None
            ]
            plan_ids = [UUID(row["plan_id"]) for row in plans]
            windows = (
                _rows(
                    unit.cursor,
                    "SELECT window_id::text,plan_id::text,"
                    "cohort_id::text,window_key,cutoff,state "
                    "FROM rt.window_runs WHERE plan_id=ANY(%s) "
                    "AND cutoff<%s ORDER BY cutoff,window_id",
                    (service_plan_ids, args.before),
                )
                if service_plan_ids
                else ()
            )
            window_ids = [UUID(row["window_id"]) for row in windows]
            receipts = (
                _rows(
                    unit.cursor,
                    "SELECT window_id::text,receipt_kind,COUNT(*) AS count "
                    "FROM rt.cycle_receipts WHERE window_id=ANY(%s) "
                    "GROUP BY window_id,receipt_kind "
                    "ORDER BY window_id,receipt_kind",
                    (window_ids,),
                )
                if window_ids
                else ()
            )
            latest_stages = (
                _rows(
                    unit.cursor,
                    "WITH latest AS (SELECT DISTINCT ON (window_id,stage_id) "
                    "window_id,stage_id,state,attempt,reason_codes "
                    "FROM rt.stage_runs WHERE window_id=ANY(%s) "
                    "ORDER BY window_id,stage_id,attempt DESC) "
                    "SELECT window_id::text,stage_id,state,attempt,"
                    "reason_codes FROM latest "
                    "ORDER BY window_id,stage_id",
                    (window_ids,),
                )
                if window_ids
                else ()
            )
            gaps = (
                _rows(
                    unit.cursor,
                    "SELECT gap.recovery_status,COUNT(*) AS count "
                    "FROM rt.advanced_journal_registry AS journal "
                    "JOIN rt.stream_gaps AS gap "
                    "ON gap.session_id=journal.stream_session_id "
                    "WHERE journal.plan_id=ANY(%s) "
                    "GROUP BY gap.recovery_status "
                    "ORDER BY gap.recovery_status",
                    (plan_ids,),
                )
                if plan_ids
                else ()
            )
            quarantines = (
                _rows(
                    unit.cursor,
                    "SELECT COUNT(*) AS count "
                    "FROM rt.advanced_journal_registry "
                    "WHERE plan_id=ANY(%s) AND status='QUARANTINED'",
                    (plan_ids,),
                )
                if plan_ids
                else ({"count": 0},)
            )
    finally:
        pool.closeall()

    complete_windows = tuple(
        row for row in windows if row["state"] == "COMPLETE"
    )
    noncomplete_windows = tuple(
        row for row in windows if row["state"] != "COMPLETE"
    )
    failed_latest_stages = tuple(
        row for row in latest_stages
        if row["state"] not in {"COMPLETE", "SKIPPED"}
    )
    unresolved_gaps = sum(
        int(row["count"]) for row in gaps
        if row["recovery_status"] in {"UNRESOLVED", "WARMING"}
    )
    quarantined = int(quarantines[0]["count"])
    maximums = {
        "stream_sessions": max(
            (int(row["stream_session_count"]) for row in plans),
            default=0,
        ),
        "connection_attempts": max(
            (int(row["connection_attempt_count"]) for row in plans),
            default=0,
        ),
        "decoded_bytes": max(
            (int(row["decoded_byte_count"]) for row in plans),
            default=0,
        ),
        "rest_repair_requests": max(
            (int(row["rest_repair_request_count"]) for row in plans),
            default=0,
        ),
        "rest_repair_decoded_bytes": max(
            (
                int(row["rest_repair_decoded_byte_count"])
                for row in plans
            ),
            default=0,
        ),
    }
    proposed = {
        key: (
            0 if value == 0 and key.startswith("rest_repair")
            else max(1, _ceil_headroom(value))
        )
        for key, value in maximums.items()
    }
    profile_ready = (
        len(profiles) == 1
        and all(
            profiles[0].get(name)
            for name in (
                "stability_evidence_sha256",
                "capacity_evidence_sha256",
                "activation_approval_reference",
                "activated_at",
            )
        )
    )
    status = (
        "PASS"
        if profile_ready
        and plans
        and complete_windows
        and not noncomplete_windows
        and not failed_latest_stages
        and unresolved_gaps == 0
        and quarantined == 0
        and maximums["decoded_bytes"] > 0
        else "BLOCKED"
    )
    evidence = {
        "active_profile": profiles[0] if len(profiles) == 1 else None,
        "completed_plan_count": len(plans),
        "completed_window_count": len(complete_windows),
        "distinct_session_count": len({
            row["session_date"] for row in plans
        }),
        "noncomplete_windows": noncomplete_windows,
        "failed_latest_stages": failed_latest_stages,
        "receipts": receipts,
        "gap_status_counts": gaps,
        "unresolved_gap_count": unresolved_gaps,
        "quarantine_count": quarantined,
        "measured_maximums": maximums,
        "headroom_multiplier": HEADROOM,
        "proposed_same_session_budgets": proposed,
        "plan_ids": [row["plan_id"] for row in plans],
        "window_ids": [row["window_id"] for row in complete_windows],
    }
    evidence_sha256 = canonical_sha256(evidence)
    report = {
        "contract": "target9-continuous-promotion-audit-v1",
        "mode": "READ_ONLY",
        "status": status,
        "database": args.database,
        "before_utc": args.before.isoformat(),
        "target": TARGET,
        "evidence_sha256": evidence_sha256,
        "evidence": evidence,
        "effects": {
            "provider_requests": 0,
            "production_database_writes": 0,
            "service_actions": 0,
            "publication": 0,
            "stored_read_changes": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    body = (
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({
        "status": status,
        "evidence_sha256": evidence_sha256,
        "proposed_same_session_budgets": proposed,
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": hashlib.sha256(body).hexdigest(),
        "output": str(args.output),
    }, indent=2, sort_keys=True))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
