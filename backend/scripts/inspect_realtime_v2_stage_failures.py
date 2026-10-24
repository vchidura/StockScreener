#!/usr/bin/env python3
"""Read-only report of latest failed stage attempts for one cohort/session."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from datetime import timedelta
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--stage-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    args.output = args.output.resolve()
    if (
        args.database != EXPECTED_V2_DATABASE_NAME
        or args.output.exists()
        or not args.stage_id.strip()
    ):
        parser.error("stage failure inspection scope is invalid")
    return args


def _provider():
    dsn = str(
        dotenv_values(BACKEND_DIR / ".env").get(
            "REALTIME_V2_APP_DSN"
        )
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


def main(argv=None):
    args = parse_args(argv)
    provider, pool = _provider()
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            unit.cursor.execute(
                "SELECT window_run.window_id::text,window_run.window_key,"
                "window_run.cutoff,window_run.deadline,"
                "window_run.state AS window_state,"
                "stage.stage_run_id::text,stage.attempt,stage.state,"
                "stage.reason_codes,stage.started_at,stage.finished_at "
                "FROM rt.window_runs AS window_run "
                "JOIN rt.service_plans AS plan USING(plan_id) "
                "JOIN rt.stage_runs AS stage USING(window_id) "
                "WHERE window_run.cohort_id=%s AND plan.session_date=%s "
                "AND stage.stage_id=%s "
                "ORDER BY window_run.cutoff,window_run.release_at,"
                "stage.attempt",
                (
                    str(args.cohort_id),
                    args.session_date,
                    args.stage_id.strip(),
                ),
            )
            attempts = [dict(row) for row in unit.cursor.fetchall()]
            unit.cursor.execute(
                "SELECT stage_id,status,reason_codes,evidence_ref,"
                "checked_at,updated_at FROM rt.stage_readiness_current "
                "WHERE cohort_id=%s AND stage_id=%s "
                "ORDER BY checked_at DESC LIMIT 1",
                (str(args.cohort_id), args.stage_id.strip()),
            )
            readiness = unit.cursor.fetchone()
            diagnostics = []
            if args.stage_id.strip() == "EQUITY_INPUT_GATE":
                unit.cursor.execute(
                    "SELECT DISTINCT member.instrument_id "
                    "FROM rt.cohorts AS cohort "
                    "JOIN rt.subscription_members AS member USING(manifest_id) "
                    "JOIN rt.instruments AS instrument USING(instrument_id) "
                    "WHERE cohort.cohort_id=%s AND member.channel='AM' "
                    "AND instrument.asset_class='EQUITY' "
                    "ORDER BY member.instrument_id",
                    (str(args.cohort_id),),
                )
                members = tuple(
                    int(row["instrument_id"])
                    for row in unit.cursor.fetchall()
                )
                unit.cursor.execute(
                    "SELECT session.session_id::text,"
                    "session.journal_generation::text,session.status,"
                    "session.failure_code,"
                    "session.started_at,session.subscribed_at,"
                    "session.ended_at,session.received_frame_count,"
                    "session.received_byte_count,"
                    "session.admitted_market_message_count,"
                    "session.admitted_market_byte_count,"
                    "session.last_raw_offset,session.last_event_offset,"
                    "session.event_time_watermark,"
                    "session.advanced_ingress_plan_id::text "
                    "FROM rt.stream_sessions AS session "
                    "JOIN rt.cohorts AS cohort "
                    "ON cohort.manifest_id=session.manifest_id "
                    "WHERE cohort.cohort_id=%s AND session.feed='STOCKS' "
                    "ORDER BY session.started_at,session.session_id",
                    (str(args.cohort_id),),
                )
                stream_sessions = [
                    dict(row) for row in unit.cursor.fetchall()
                ]
                unit.cursor.execute(
                    "SELECT journal_generation::text,"
                    "stream_session_id::text,plan_id::text,status,"
                    "owner_epoch,shard_ids,shard_map_sha256,"
                    "raw_committed_offset,normalized_committed_offset,"
                    "bridge_committed_offset,quarantine_reason,"
                    "quarantine_detail,quarantined_at "
                    "FROM rt.advanced_journal_registry "
                    "WHERE stream_session_id=ANY(%s::uuid[]) "
                    "ORDER BY owner_epoch,journal_generation",
                    ([
                        row["session_id"] for row in stream_sessions
                    ],),
                )
                journals = [
                    dict(row) for row in unit.cursor.fetchall()
                ]
                unit.cursor.execute(
                    "SELECT gap_id::text,session_id::text,"
                    "journal_generation::text,plan_id::text,"
                    "instrument_id,channel,gap_start,gap_end,reason,"
                    "recovery_status,continuity_class,"
                    "last_durable_offset,last_durable_watermark "
                    "FROM rt.stream_gaps "
                    "WHERE session_id=ANY(%s::uuid[]) "
                    "ORDER BY gap_start,gap_id",
                    ([
                        row["session_id"] for row in stream_sessions
                    ],),
                )
                stream_gaps = [
                    dict(row) for row in unit.cursor.fetchall()
                ]
                unit.cursor.execute(
                    "SELECT checkpoint.journal_generation::text,"
                    "checkpoint.cohort_id::text,checkpoint.shard_id,"
                    "checkpoint.committed_offset,"
                    "checkpoint.state_hash "
                    "FROM rt.consumer_checkpoints AS checkpoint "
                    "WHERE checkpoint.cohort_id=%s "
                    "ORDER BY checkpoint.journal_generation,"
                    "checkpoint.shard_id",
                    (str(args.cohort_id),),
                )
                checkpoints = [
                    dict(row) for row in unit.cursor.fetchall()
                ]
                for cutoff in sorted({
                    attempt["cutoff"] for attempt in attempts
                }):
                    start = cutoff - timedelta(minutes=5)
                    unit.cursor.execute(
                        "SELECT current.instrument_id,revision.bar_start,"
                        "revision.bar_end,revision.source_kind,"
                        "revision.journal_generation::text,"
                        "revision.stream_session_id::text,"
                        "revision.source_offset "
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
                        "AND current.instrument_id=ANY(%s) "
                        "AND current.interval='1m' "
                        "AND current.adjustment_basis='RAW_UNADJUSTED' "
                        "AND current.session_scope='RTH' "
                        "AND revision.session_date=%s "
                        "AND revision.is_final "
                        "AND current.bar_start>=%s "
                        "AND current.bar_end<=%s "
                        "ORDER BY current.instrument_id,revision.bar_start",
                        (
                            str(args.cohort_id),
                            list(members),
                            cutoff.date(),
                            start,
                            cutoff,
                        ),
                    )
                    bars = [dict(row) for row in unit.cursor.fetchall()]
                    expected = {
                        (instrument_id, start + timedelta(minutes=index))
                        for instrument_id in members
                        for index in range(5)
                    }
                    actual = {
                        (int(row["instrument_id"]), row["bar_start"])
                        for row in bars
                    }
                    diagnostics.append({
                        "cutoff": cutoff,
                        "member_count": len(members),
                        "expected_bar_count": len(expected),
                        "actual_bar_count": len(bars),
                        "missing_bar_count": len(expected - actual),
                        "unexpected_bar_count": len(actual - expected),
                        "stream_bar_count": sum(
                            row["source_kind"] == "REALTIME_STREAM"
                            for row in bars
                        ),
                        "journal_generations": sorted({
                            row["journal_generation"]
                            for row in bars
                            if row["journal_generation"] is not None
                        }),
                        "stream_session_ids": sorted({
                            row["stream_session_id"]
                            for row in bars
                            if row["stream_session_id"] is not None
                        }),
                        "maximum_source_offset": max(
                            (
                                int(row["source_offset"])
                                for row in bars
                                if row["source_offset"] is not None
                            ),
                            default=None,
                        ),
                    })
        reason_counts = {}
        for attempt in attempts:
            for reason in attempt["reason_codes"]:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
        report = {
            "contract": "realtime-v2-stage-failure-inspection-v1",
            "mode": "READ_ONLY",
            "cohort_id": str(args.cohort_id),
            "session_date": args.session_date.isoformat(),
            "stage_id": args.stage_id.strip(),
            "attempts": attempts,
            "reason_counts": reason_counts,
            "latest_readiness": (
                None if readiness is None else dict(readiness)
            ),
            "input_gate_diagnostics": diagnostics,
            "stream_sessions": (
                stream_sessions
                if args.stage_id.strip() == "EQUITY_INPUT_GATE"
                else []
            ),
            "journals": (
                journals
                if args.stage_id.strip() == "EQUITY_INPUT_GATE"
                else []
            ),
            "stream_gaps": (
                stream_gaps
                if args.stage_id.strip() == "EQUITY_INPUT_GATE"
                else []
            ),
            "consumer_checkpoints": (
                checkpoints
                if args.stage_id.strip() == "EQUITY_INPUT_GATE"
                else []
            ),
            "effects": {
                "provider_requests": 0,
                "provider_websocket": 0,
                "database_writes": 0,
                "service_actions": 0,
            },
        }
        report["semantic_sha256"] = canonical_sha256(report)
        encoded = (
            json.dumps(
                report,
                indent=2,
                sort_keys=True,
                default=str,
            )
            + "\n"
        ).encode("utf-8")
        with args.output.open("xb") as stream:
            stream.write(encoded)
        print(json.dumps({
            "attempt_count": len(attempts),
            "reason_counts": reason_counts,
            "semantic_sha256": report["semantic_sha256"],
            "file_sha256": hashlib.sha256(encoded).hexdigest(),
            "output": str(args.output),
        }, indent=2, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
