#!/usr/bin/env python3
"""Report one stored-only Advanced Realtime V3 operating summary."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_lifecycle import (  # noqa: E402
    AdvancedEquityLifecyclePlanner,
)
from realtime.advanced_realtime_v3_runtime import (  # noqa: E402
    ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS,
    ADVANCED_REALTIME_V3_DRAIN_SECONDS,
    ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
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


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=EXPECTED_V2_DATABASE_NAME,
    )
    parser.add_argument(
        "--confirm-database-name",
        default=EXPECTED_V2_DATABASE_NAME,
    )
    parser.add_argument("--native-status-path", type=Path)
    parser.add_argument("--compose-services-path", type=Path)
    parser.add_argument("--retention-status-path", type=Path)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced V3 status targets stocks_realtime_v2")
    return args


def _optional_json(path: Path | None):
    if path is None or not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _compose_services(path: Path | None) -> list[dict[str, object]]:
    if path is None or not path.is_file():
        return []
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError("Compose service status is empty")
    try:
        value = json.loads(text)
        rows = value if isinstance(value, list) else [value]
    except json.JSONDecodeError:
        rows = [
            json.loads(line)
            for line in text.splitlines()
            if line.strip()
        ]
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("Compose service status must contain objects")
    return [
        {
            "service": row.get("Service") or row.get("Name"),
            "name": row.get("Name"),
            "state": row.get("State"),
            "health": row.get("Health"),
            "exit_code": row.get("ExitCode"),
        }
        for row in rows
    ]


def advanced_realtime_v3_status_lifecycle(checked_at: datetime):
    planner = AdvancedEquityLifecyclePlanner(clock=lambda: checked_at)
    return planner.plan(
        drain_seconds=ADVANCED_REALTIME_V3_DRAIN_SECONDS,
        finality_delay_seconds=(
            ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS
        ),
        completion_budget_seconds=(
            ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS
        ),
    )


def build_advanced_realtime_v3_status_cursor(
    cursor,
    *,
    checked_at: datetime,
) -> dict[str, object]:
    lifecycle = advanced_realtime_v3_status_lifecycle(checked_at)
    cursor.execute(
        "SELECT revision.revision_id::text,"
        "revision.effective_session_date,intent.ticker,"
        "intent.member_kind,intent.enabled,readiness.status,"
        "readiness.reasons "
        "FROM rt.realtime_desired_current AS current "
        "JOIN rt.realtime_desired_revisions AS revision "
        "USING(revision_id) "
        "JOIN rt.realtime_desired_member_intents AS intent "
        "USING(revision_id) "
        "JOIN rt.realtime_desired_intent_readiness AS readiness "
        "USING(intent_id) "
        "WHERE current.scope_key='ADVANCED_REALTIME' "
        "ORDER BY intent.ticker,intent.member_kind"
    )
    member_rows = tuple(cursor.fetchall())
    cursor.execute(
        "SELECT session_date,status,current_stage,option_underlyings,"
        "desired_snapshot_sha256,desired_cohort_id::text,"
        "activated_session_binding_id::text,last_error,checked_at,"
        "expiration_state,expired_at,expiration_reason "
        "FROM rt.advanced_options_preparation_sessions "
        "WHERE session_date IN (%s,%s) ORDER BY session_date",
        (
            lifecycle.processing_session_date,
            lifecycle.preparation_session_date,
        ),
    )
    preparations = tuple(cursor.fetchall())
    cursor.execute(
        "SELECT session_date,status,current_stage,option_underlyings,"
        "desired_snapshot_sha256,desired_cohort_id::text,"
        "activated_session_binding_id::text,last_error,checked_at,"
        "expiration_state,expired_at,expiration_reason "
        "FROM rt.advanced_options_preparation_sessions "
        "WHERE expiration_state IS NOT NULL "
        "ORDER BY session_date DESC,expired_at DESC LIMIT 10"
    )
    expired_preparations = tuple(cursor.fetchall())
    cursor.execute(
        "SELECT generation.selection_generation_id::text,"
        "generation.session_date,generation.broad_underlyings,"
        "generation.flow_underlyings,"
        "count(DISTINCT shard.shard_id),"
        "count(DISTINCT member.contract_id) "
        "FROM rt.advanced_options_selection_generations AS generation "
        "LEFT JOIN rt.advanced_options_subscription_shards AS shard "
        "USING(selection_generation_id) "
        "LEFT JOIN rt.advanced_options_selection_members AS member "
        "USING(selection_generation_id) "
        "WHERE generation.session_date IN (%s,%s) "
        "GROUP BY generation.selection_generation_id,"
        "generation.session_date,generation.broad_underlyings,"
        "generation.flow_underlyings "
        "ORDER BY generation.session_date",
        (
            lifecycle.processing_session_date,
            lifecycle.preparation_session_date,
        ),
    )
    selection_rows = tuple(cursor.fetchall())
    cursor.execute(
        "SELECT shard.shard_id::text,shard.shard_slot,"
        "cardinality(shard.contract_tickers),"
        "COALESCE(state.state,'PLANNED'),"
        "COALESCE(state.message_count,0),"
        "COALESCE(state.byte_count,0),"
        "COALESCE(state.reason_codes,ARRAY[]::text[]) "
        "FROM rt.advanced_options_subscription_shards AS shard "
        "JOIN rt.advanced_options_selection_generations AS generation "
        "USING(selection_generation_id) "
        "LEFT JOIN rt.advanced_options_shard_states AS state "
        "USING(selection_generation_id,shard_id) "
        "WHERE generation.session_date=%s "
        "ORDER BY shard.shard_slot",
        (lifecycle.processing_session_date,),
    )
    logical_shards = tuple(cursor.fetchall())
    cursor.execute(
        "SELECT subscription.subscription_id::text,"
        "subscription.owner_id::text,"
        "subscription.journal_generation::text,"
        "cardinality(subscription.contract_tickers),"
        "COALESCE(state.state,'PLANNED'),"
        "COALESCE(state.message_count,0),"
        "COALESCE(state.byte_count,0),"
        "COALESCE(state.reconnect_count,0),"
        "COALESCE(state.reason_codes,ARRAY[]::text[]) "
        "FROM rt.advanced_options_physical_subscriptions AS subscription "
        "JOIN rt.advanced_options_selection_generations AS generation "
        "USING(selection_generation_id) "
        "LEFT JOIN rt.advanced_options_physical_subscription_states "
        "AS state USING(selection_generation_id,subscription_id) "
        "WHERE generation.session_date=%s",
        (lifecycle.processing_session_date,),
    )
    physical_feeds = tuple(cursor.fetchall())
    cursor.execute(
        "SELECT state,count(*) FROM rt.portal_publication_work "
        "GROUP BY state ORDER BY state"
    )
    publication = {
        str(row[0]): int(row[1]) for row in cursor.fetchall()
    }
    cursor.execute(
        "SELECT role_name,runtime_kind,owner_id::text,owner_epoch,"
        "process_identity,state,health,lease_until,observed_at,detail "
        "FROM rt.advanced_realtime_service_states "
        "ORDER BY role_name"
    )
    service_rows = tuple(cursor.fetchall())
    return {
        "checked_at_utc": checked_at.isoformat(),
        "phase": lifecycle.phase.value,
        "current_processing": {
            "session_date": (
                lifecycle.processing_session_date.isoformat()
            ),
            "deadline_utc": (
                lifecycle.processing_deadline.isoformat()
            ),
        },
        "future_preparation": {
            "session_date": (
                lifecycle.preparation_session_date.isoformat()
            ),
            "records": [
                {
                    "session_date": row[0].isoformat(),
                    "status": row[1],
                    "current_stage": row[2],
                    "option_underlyings": list(row[3]),
                    "desired_snapshot_sha256": row[4],
                    "desired_cohort_id": row[5],
                    "session_binding_id": row[6],
                    "last_error": row[7],
                    "checked_at_utc": row[8].isoformat(),
                    "expiration_state": row[9],
                    "expired_at_utc": (
                        None
                        if row[10] is None
                        else row[10].isoformat()
                    ),
                    "expiration_reason": row[11],
                }
                for row in preparations
            ],
        },
        "expired_preparation": [
            {
                "session_date": row[0].isoformat(),
                "status": row[1],
                "current_stage": row[2],
                "option_underlyings": list(row[3]),
                "desired_snapshot_sha256": row[4],
                "desired_cohort_id": row[5],
                "session_binding_id": row[6],
                "last_error": row[7],
                "checked_at_utc": row[8].isoformat(),
                "expiration_state": row[9],
                "expired_at_utc": row[10].isoformat(),
                "expiration_reason": row[11],
            }
            for row in expired_preparations
        ],
        "members": [
            {
                "revision_id": row[0],
                "effective_session_date": row[1].isoformat(),
                "ticker": row[2],
                "kind": row[3],
                "enabled": bool(row[4]),
                "status": row[5],
                "reasons": list(row[6]),
            }
            for row in member_rows
        ],
        "selection": [
            {
                "selection_generation_id": row[0],
                "session_date": row[1].isoformat(),
                "broad_underlyings": list(row[2]),
                "flow_underlyings": list(row[3]),
                "logical_shards": int(row[4]),
                "contracts": int(row[5]),
            }
            for row in selection_rows
        ],
        "logical_shards": [
            {
                "shard_id": row[0],
                "slot": int(row[1]),
                "contracts": int(row[2]),
                "state": row[3],
                "messages": int(row[4]),
                "bytes": int(row[5]),
                "reasons": list(row[6]),
            }
            for row in logical_shards
        ],
        "physical_feeds": [
            {
                "subscription_id": row[0],
                "owner_id": row[1],
                "journal_generation": row[2],
                "contracts": int(row[3]),
                "state": row[4],
                "messages": int(row[5]),
                "bytes": int(row[6]),
                "reconnects": int(row[7]),
                "reasons": list(row[8]),
            }
            for row in physical_feeds
        ],
        "publication": publication,
        "runtime_services": [
            {
                "role": row[0],
                "runtime_kind": row[1],
                "owner_id": row[2],
                "owner_epoch": int(row[3]),
                "process_identity": row[4],
                "state": row[5],
                "health": row[6],
                "lease_until_utc": row[7].isoformat(),
                "observed_at_utc": row[8].isoformat(),
                "stale": row[7] <= checked_at,
                "detail": dict(row[9]),
            }
            for row in service_rows
        ],
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping({
        **{
            key: value
            for key, value in dotenv_values(
                BACKEND_DIR / ".env"
            ).items()
            if value is not None
        },
        **os.environ,
    },
        process=RealtimeV2Process.APP_WORKER,
    )
    pool = ThreadedConnectionPool(
        1,
        1,
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
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            report = build_advanced_realtime_v3_status_cursor(
                unit.cursor,
                checked_at=datetime.now(UTC),
            )
        native_services = _optional_json(args.native_status_path)
        compose_services = _compose_services(
            args.compose_services_path
        )
        database_services = report.pop("runtime_services")
        report.update({
            "contract": "advanced-realtime-v3-status-v1",
            "status": "AVAILABLE",
            "services": (
                native_services
                if native_services is not None
                else {
                    "contract": (
                        "advanced-realtime-v3-compose-services-v1"
                    ),
                    "source": (
                        "COMPOSE"
                        if compose_services
                        else "DATABASE"
                    ),
                    "containers": compose_services,
                    "roles": database_services,
                }
            ),
            "retention": (
                _optional_json(args.retention_status_path)
                or {
                    "status": "UNKNOWN",
                    "reason": "RETENTION_STATUS_PATH_UNAVAILABLE",
                }
            ),
            "effects": {
                "provider_requests": 0,
                "database_writes": 0,
                "publication_writes": 0,
                "delivery": 0,
                "broker": 0,
                "trading": 0,
            },
        })
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
