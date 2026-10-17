#!/usr/bin/env python3
"""Replay a captured SPY/AM journal through a disposable V2 database."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4


BACKEND_DIR = Path(__file__).resolve().parent.parent
TEST_SUPPORT_DIR = BACKEND_DIR / "tests" / "realtime"
for path in (BACKEND_DIR, TEST_SUPPORT_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from realtime.advanced_equity_control_plane import (  # noqa: E402
    build_advanced_equity_control_plane_plan,
    persist_advanced_equity_control_plane_cursor,
)
from realtime.advanced_equity_journal_bridge import (  # noqa: E402
    AdvancedEquityJournalBridgeConfig,
    process_advanced_equity_journal_once,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.journal import RawJournalReader  # noqa: E402
from realtime.normalizer import NORMALIZER_VERSION  # noqa: E402
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from v2_database import disposable_v2_database  # noqa: E402


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--capture-report", type=Path, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--authorization-reference", required=True)
    args = parser.parse_args(argv)
    args.journal = args.journal.resolve()
    args.capture_report = args.capture_report.resolve()
    args.report_path = args.report_path.resolve()
    if not args.journal.is_file() or not args.capture_report.is_file():
        parser.error("journal and capture report must exist")
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    return args


def main(argv=None):
    args = parse_args(argv)
    capture_body = args.capture_report.read_bytes()
    capture = json.loads(capture_body)
    if (
        capture.get("status") != "PASS"
        or capture.get("normalizer_version") != NORMALIZER_VERSION
        or capture.get("symbols") != ["SPY"]
        or capture.get("channels") != ["AM"]
        or capture.get("production_writes") != 0
    ):
        raise ValueError("capture report is not exact provider-only SPY/AM evidence")
    generation = UUID(capture["journal_generation"])
    stream_session_id = UUID(capture["stream_session_id"])
    watermark = datetime.fromisoformat(
        capture["event_time_watermark"]
    ).astimezone(UTC)
    raw_reader = RawJournalReader(args.journal)
    raw_records = raw_reader.read_after(0)
    normalized = raw_reader.read_normalized_after(0)
    if len(raw_records) != 2 or len(normalized) != 2:
        raise ValueError("captured journal cardinality changed")
    database_name = f"test_realtime_v2_advanced_live_{uuid4().hex[:8]}"
    result = None
    with disposable_v2_database(database_name) as pool:
        provider = V2ConnectionProvider(
            {OWNER_POOL: pool},
            V2AccessConfig(allow_disposable=True),
        )
        provider.verify()
        database = V2WorkerDatabase(
            provider,
            RealtimeDatabaseRole.COMPUTE,
        )
        security_id = uuid4()
        checked_at = datetime.now(UTC)
        with database.transaction() as cursor:
            cursor.execute(
                "INSERT INTO ref.securities ("
                "security_id,asset_type,source_origin,created_at) "
                "VALUES (%s,'STOCK','advanced-live-rehearsal',%s)",
                (str(security_id), watermark - timedelta(days=1)),
            )
            cursor.execute(
                "INSERT INTO ref.security_revisions ("
                "revision_id,security_id,ticker,exchange,calendar_id,"
                "effective_from,observed_at,payload_sha256,"
                "company_name,security_type,active) VALUES ("
                "%s,%s,'SPY','ARCX','XNYS',%s,%s,%s,"
                "'SPY','ETF',true)",
                (
                    str(uuid4()),
                    str(security_id),
                    watermark - timedelta(days=1),
                    watermark - timedelta(days=1),
                    uuid4().hex + uuid4().hex,
                ),
            )
            cursor.execute(
                "SELECT version,checksum_sha256 "
                "FROM rt.schema_migrations ORDER BY version"
            )
            schema_sha256 = canonical_sha256(tuple(cursor.fetchall()))
            cursor.execute(
                "SELECT policy_sha256 FROM rt.security_phase_events "
                "ORDER BY event_id DESC LIMIT 1"
            )
            security_policy_sha256 = cursor.fetchone()[0]
            plan = build_advanced_equity_control_plane_plan(
                session_date=watermark.date(),
                security_ids={"SPY": security_id},
                entitlement_evidence_sha256=hashlib.sha256(
                    capture_body
                ).hexdigest(),
                authorization_reference=args.authorization_reference,
                checked_at=watermark - timedelta(minutes=5),
                expires_at=checked_at + timedelta(hours=1),
                schema_sha256=schema_sha256,
                security_policy_sha256=security_policy_sha256,
            )
            persist_advanced_equity_control_plane_cursor(cursor, plan)
            cursor.execute(
                "SELECT instrument_id FROM rt.subscription_members "
                "WHERE manifest_id=%s AND channel='AM'",
                (str(plan.manifest_id),),
            )
            instrument_id = int(cursor.fetchone()[0])
            if instrument_id != normalized[0].instrument_id:
                raise ValueError(
                    "captured journal instrument identity is not reproducible"
                )
            cursor.execute(
                "INSERT INTO rt.stream_sessions ("
                "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
                "started_at,authenticated_at,subscribed_at,ended_at,status,"
                "journal_generation,normalizer_version,"
                "authorization_reference,budget_scope_sha256,"
                "connection_attempt,connection_ceiling,decoded_byte_ceiling,"
                "received_frame_count,received_byte_count,"
                "admitted_market_message_count,admitted_market_byte_count,"
                "last_raw_offset,last_event_offset,event_time_watermark,"
                "heartbeat_at) VALUES ("
                "%s,'massive','STOCKS',%s,%s,1,%s,%s,%s,%s,'STOPPED',"
                "%s,%s,%s,%s,1,1,10485760,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    str(stream_session_id),
                    str(plan.manifest_id),
                    str(uuid4()),
                    watermark - timedelta(minutes=5),
                    watermark - timedelta(minutes=5),
                    watermark - timedelta(minutes=5),
                    watermark,
                    str(generation),
                    NORMALIZER_VERSION,
                    args.authorization_reference,
                    canonical_sha256({
                        "capture_sha256": hashlib.sha256(
                            capture_body
                        ).hexdigest(),
                        "authorization_reference": (
                            args.authorization_reference
                        ),
                    }),
                    capture["received_frames"],
                    capture["received_bytes"],
                    capture["market_messages"],
                    capture["market_bytes"],
                    capture["last_raw_offset"],
                    capture["last_event_offset"],
                    watermark,
                    watermark,
                ),
            )
        config = AdvancedEquityJournalBridgeConfig(
            cohort_id=plan.cohort_id,
            journal_generation=generation,
            stream_session_id=stream_session_id,
            owner_epoch=1,
            normalizer_version=NORMALIZER_VERSION,
            shard_ids=(0,),
            batch_limit=10,
        )
        first = process_advanced_equity_journal_once(
            database,
            raw_reader,
            config,
            checked_at=checked_at,
        )
        replay = process_advanced_equity_journal_once(
            database,
            raw_reader,
            config,
            checked_at=checked_at,
        )
        with database.transaction(read_only=True) as cursor:
            cursor.execute(
                "SELECT volume FROM rt.bar_revisions "
                "WHERE cohort_id=%s AND interval='1m' ORDER BY bar_start",
                (str(plan.cohort_id),),
            )
            stored_volumes = tuple(
                Decimal(str(row[0])) for row in cursor.fetchall()
            )
            cursor.execute(
                "SELECT committed_offset FROM rt.consumer_checkpoints "
                "WHERE cohort_id=%s",
                (str(plan.cohort_id),),
            )
            committed_offset = int(cursor.fetchone()[0])
        normalized_volumes = tuple(row.size for row in normalized)
        result = {
            "status": "PASS",
            "database": database_name,
            "database_dropped": True,
            "raw_records": len(raw_records),
            "normalized_events": len(normalized),
            "persisted_bars": first.persisted_bars,
            "replay_persisted_bars": replay.persisted_bars,
            "checkpoint_offset": committed_offset,
            "fractional_volume_preserved": (
                stored_volumes == normalized_volumes
                and all(
                    volume != volume.to_integral_value()
                    for volume in stored_volumes
                )
            ),
            "publication_advanced": False,
            "production_writes": 0,
            "provider_requests": 0,
            "credentials_exposed": False,
        }
        if (
            result["persisted_bars"] != 2
            or result["replay_persisted_bars"] != 0
            or result["checkpoint_offset"] != 2
            or not result["fractional_volume_preserved"]
        ):
            raise ValueError("disposable Advanced journal rehearsal failed")
    write_report(args.report_path, result)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
