#!/usr/bin/env python3
"""Plan or execute one checksum-only Advanced journal recovery."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_journal_registry import (  # noqa: E402
    AdvancedJournalRecoveryEvent,
    load_advanced_journal_recovery_events_cursor,
    load_advanced_journal_registration_cursor,
    record_advanced_journal_recovery_event_cursor,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
    OptionsPipeline,
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.journal import RawJournalReader, journal_files_sha256  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.run_realtime_v2_advanced_journal_replay import _provider  # noqa: E402


UTC = timezone.utc
CONTRACT = "advanced-journal-checksum-recovery-v1"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: str) -> str:
    normalized = value.strip().lower()
    if SHA256_PATTERN.fullmatch(normalized) is None:
        raise argparse.ArgumentTypeError("value must be a SHA256 digest")
    return normalized


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--journal-root", type=Path, required=True)
    parser.add_argument("--plan-id", type=UUID, required=True)
    parser.add_argument("--prior-gap-id", type=UUID, required=True)
    parser.add_argument(
        "--prior-recovered-generation",
        type=UUID,
        required=True,
    )
    parser.add_argument("--report-path", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-files-sha256", type=_sha256)
    parser.add_argument("--expected-offset", type=int)
    parser.add_argument("--expected-bars", type=int)
    parser.add_argument("--plan-report", type=Path)
    parser.add_argument("--plan-report-sha256", type=_sha256)
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced checksum recovery targets stocks_realtime_v2")
    args.journal_root = args.journal_root.resolve()
    if not args.journal_root.is_dir():
        parser.error("--journal-root must be an existing mounted directory")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    plan_values = (
        args.expected_files_sha256,
        args.expected_offset,
        args.expected_bars,
    )
    execute_values = (args.plan_report, args.plan_report_sha256)
    if args.plan and (
        any(value is None for value in plan_values)
        or any(value is not None for value in execute_values)
        or args.expected_offset <= 0
        or args.expected_bars <= 0
    ):
        parser.error("--plan requires positive expected hash/offset/bar evidence")
    if args.execute and (
        any(value is not None for value in plan_values)
        or any(value is None for value in execute_values)
    ):
        parser.error("--execute requires only a plan report and SHA256")
    if args.plan_report is not None:
        args.plan_report = args.plan_report.resolve()
        if not args.plan_report.is_file():
            parser.error("--plan-report must exist")
    return args


def _iso(value):
    return value.astimezone(UTC).isoformat() if value is not None else None


def _registration_report(registration):
    return {
        "journal_generation": str(registration.journal_generation),
        "plan_id": str(registration.plan_id),
        "stream_session_id": str(registration.stream_session_id),
        "cohort_id": str(registration.cohort_id),
        "relative_path": registration.relative_path,
        "metadata_sha256": registration.metadata_sha256,
        "status": registration.status,
        "raw_committed_offset": registration.raw_committed_offset,
        "normalized_committed_offset": (
            registration.normalized_committed_offset
        ),
        "bridge_committed_offset": registration.bridge_committed_offset,
        "sealed_files_sha256": registration.sealed_files_sha256,
    }


def _event_report(event):
    return {
        "event_type": event.event_type,
        "recovery_event_id": str(event.recovery_event_id),
        "journal_generation": str(event.journal_generation),
        "plan_id": str(event.plan_id),
        "plan_report_sha256": event.plan_report_sha256,
        "journal_files_sha256": event.journal_files_sha256,
        "raw_committed_offset": event.raw_committed_offset,
        "normalized_committed_offset": event.normalized_committed_offset,
        "bridge_committed_offset": event.bridge_committed_offset,
        "persisted_bars": event.persisted_bars,
        "created_at": _iso(event.created_at),
    }


def _load_snapshot(provider, root, plan_id, prior_gap_id):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        cursor = unit.cursor
        cursor.execute(
            "SELECT journal_generation FROM rt.advanced_journal_registry "
            "WHERE plan_id=%s",
            (str(plan_id),),
        )
        rows = cursor.fetchall()
        if len(rows) != 1:
            raise ValueError("checksum recovery requires one journal row")
        registration = load_advanced_journal_registration_cursor(
            cursor,
            UUID(str(rows[0][0])),
        )
        cursor.execute(
            "SELECT quarantine_reason,quarantine_detail,quarantined_at "
            "FROM rt.advanced_journal_registry "
            "WHERE journal_generation=%s",
            (str(registration.journal_generation),),
        )
        quarantine = cursor.fetchone()
        cursor.execute(
            "SELECT plan.status,plan.terminal_outcome,"
            "session.status,session.ended_at,"
            "usage.rest_repair_request_count,"
            "usage.rest_repair_decoded_byte_count "
            "FROM rt.advanced_ingress_plans AS plan "
            "JOIN rt.advanced_ingress_plan_usage AS usage USING(plan_id) "
            "JOIN rt.stream_sessions AS session "
            "ON session.advanced_ingress_plan_id=plan.plan_id "
            "WHERE plan.plan_id=%s",
            (str(plan_id),),
        )
        durable = cursor.fetchone()
        cursor.execute(
            "SELECT count(*),min(source_offset),max(source_offset) "
            "FROM rt.bar_revisions WHERE journal_generation=%s",
            (str(registration.journal_generation),),
        )
        bars = cursor.fetchone()
        cursor.execute(
            "SELECT committed_offset FROM rt.consumer_checkpoints "
            "WHERE journal_generation=%s ORDER BY shard_id",
            (str(registration.journal_generation),),
        )
        checkpoints = tuple(int(row[0]) for row in cursor.fetchall())
        cursor.execute(
            "SELECT count(*) FROM rt.stream_gaps "
            "WHERE journal_generation=%s",
            (str(registration.journal_generation),),
        )
        local_gap_count = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT gap.journal_generation,gap.reason,"
            "gap.recovery_status,"
            "EXISTS (SELECT 1 FROM "
            "rt.advanced_journal_recovery_events AS recovery "
            "WHERE recovery.journal_generation=gap.journal_generation "
            "AND recovery.event_type='RECOVERED') "
            "FROM rt.stream_gaps AS gap WHERE gap.gap_id=%s",
            (str(prior_gap_id),),
        )
        prior_gap = cursor.fetchone()
        recovery_events = load_advanced_journal_recovery_events_cursor(
            cursor,
            registration.journal_generation,
        )
        cursor.execute(
            "SELECT count(*) FROM rt.portal_cohort_current "
            "WHERE cohort_id=%s",
            (str(registration.cohort_id),),
        )
        portal_count = int(cursor.fetchone()[0])
    path = root / registration.relative_path
    reader = RawJournalReader(path)
    state = reader.recovery_state(registration.normalizer_version)
    return {
        "registration": registration,
        "quarantine": {
            "reason": str(quarantine[0]),
            "detail": str(quarantine[1]),
            "quarantined_at": _iso(quarantine[2]),
        },
        "durable": {
            "plan_status": str(durable[0]),
            "terminal_outcome": str(durable[1]),
            "stream_status": str(durable[2]),
            "stream_ended_at": _iso(durable[3]),
            "rest_repair_request_count": int(durable[4]),
            "rest_repair_decoded_byte_count": int(durable[5]),
        },
        "bars": {
            "count": int(bars[0]),
            "minimum_offset": int(bars[1]),
            "maximum_offset": int(bars[2]),
        },
        "checkpoints": checkpoints,
        "local_gap_count": local_gap_count,
        "prior_gap": {
            "journal_generation": str(prior_gap[0]),
            "reason": str(prior_gap[1]),
            "recovery_status": str(prior_gap[2]),
            "journal_recovered": bool(prior_gap[3]),
        },
        "recovery_events": tuple(
            _event_report(event) for event in recovery_events
        ),
        "portal_count": portal_count,
        "journal": {
            "generation": str(state.generation),
            "schema_version": state.schema_version,
            "integrity_check": state.integrity_check,
            "raw_committed_offset": state.raw_committed_offset,
            "normalized_committed_raw_offset": (
                state.normalized_committed_raw_offset
            ),
            "normalized_committed_event_offset": (
                state.normalized_committed_event_offset
            ),
            "quarantine_count": state.quarantine_count,
            "current_files_sha256": journal_files_sha256(path),
        },
    }


def _validate(
    snapshot,
    *,
    prior_recovered_generation,
    expected_files_sha256,
    expected_offset,
    expected_bars,
):
    registration = snapshot["registration"]
    if (
        registration.status != "QUARANTINED"
        or snapshot["quarantine"]["reason"]
        != "JOURNAL_CHECKSUM_MISMATCH"
        or snapshot["quarantine"]["detail"] != registration.relative_path
        or registration.sealed_files_sha256 is None
        or registration.sealed_files_sha256
        == snapshot["journal"]["current_files_sha256"]
        or snapshot["journal"]["current_files_sha256"]
        != expected_files_sha256
        or snapshot["journal"]["generation"]
        != str(registration.journal_generation)
        or snapshot["journal"]["integrity_check"].lower() != "ok"
        or snapshot["journal"]["raw_committed_offset"] != expected_offset
        or snapshot["journal"]["normalized_committed_raw_offset"]
        != expected_offset
        or snapshot["journal"]["normalized_committed_event_offset"]
        != expected_offset
        or snapshot["journal"]["quarantine_count"] != 0
        or registration.raw_committed_offset != expected_offset
        or registration.normalized_committed_offset != expected_offset
        or registration.bridge_committed_offset != expected_offset
        or snapshot["bars"]
        != {
            "count": expected_bars,
            "minimum_offset": 1,
            "maximum_offset": expected_offset,
        }
        or snapshot["checkpoints"] != (expected_offset,)
        or snapshot["local_gap_count"] != 0
        or snapshot["durable"]["plan_status"] != "BLOCKED"
        or snapshot["durable"]["terminal_outcome"]
        != "BLOCKED_CONFIGURATION"
        or snapshot["durable"]["stream_status"] != "STOPPED"
        or snapshot["durable"]["stream_ended_at"] is None
        or snapshot["durable"]["rest_repair_request_count"] != 0
        or snapshot["durable"]["rest_repair_decoded_byte_count"] != 0
        or snapshot["recovery_events"]
        or snapshot["portal_count"] != 0
        or snapshot["prior_gap"]["journal_generation"]
        != str(prior_recovered_generation)
        or snapshot["prior_gap"]["reason"] != "STALE_OWNER_RECLAIMED"
        or snapshot["prior_gap"]["recovery_status"] != "UNRESOLVED"
        or snapshot["prior_gap"]["journal_recovered"] is not True
    ):
        raise ValueError("checksum recovery evidence differs from authorization")


def _snapshot_report(snapshot):
    return {
        **{
            key: value
            for key, value in snapshot.items()
            if key != "registration"
        },
        "registration": _registration_report(snapshot["registration"]),
    }


def _load_plan(args):
    if args.plan_report.stat().st_size > 1024 * 1024:
        raise ValueError("checksum recovery plan exceeds its bound")
    body = args.plan_report.read_bytes()
    if hashlib.sha256(body).hexdigest() != args.plan_report_sha256:
        raise ValueError("checksum recovery plan SHA256 mismatch")
    report = json.loads(body)
    if (
        report.get("contract") != CONTRACT
        or report.get("mode") != "PLAN"
        or report.get("status") != "READY"
        or report.get("plan_id") != str(args.plan_id)
    ):
        raise ValueError("checksum recovery plan identity is invalid")
    return report


def main(argv=None):
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.BRIDGE,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError("checksum recovery requires Advanced equity")
    provider, pool = _provider(config)
    checked_at = datetime.now(UTC)
    try:
        if args.plan:
            expected_files = args.expected_files_sha256
            expected_offset = args.expected_offset
            expected_bars = args.expected_bars
        else:
            approved = _load_plan(args)
            expected_files = approved["expected"]["files_sha256"]
            expected_offset = approved["expected"]["offset"]
            expected_bars = approved["expected"]["bars"]
        snapshot = _load_snapshot(
            provider,
            args.journal_root,
            args.plan_id,
            args.prior_gap_id,
        )
        _validate(
            snapshot,
            prior_recovered_generation=args.prior_recovered_generation,
            expected_files_sha256=expected_files,
            expected_offset=expected_offset,
            expected_bars=expected_bars,
        )
        payload = _snapshot_report(snapshot)
        snapshot_sha256 = canonical_sha256(payload)
        if args.plan:
            report = {
                "contract": CONTRACT,
                "mode": "PLAN",
                "status": "READY",
                "checked_at": checked_at.isoformat(),
                "plan_id": str(args.plan_id),
                "prior_gap_id": str(args.prior_gap_id),
                "prior_recovered_generation": str(
                    args.prior_recovered_generation
                ),
                "expected": {
                    "files_sha256": expected_files,
                    "offset": expected_offset,
                    "bars": expected_bars,
                },
                "snapshot": payload,
                "snapshot_sha256": snapshot_sha256,
                "provider_requests": 0,
                "publication_advanced": False,
                "credentials_exposed": False,
            }
        else:
            if snapshot_sha256 != approved["snapshot_sha256"]:
                raise ValueError(
                    "checksum recovery state differs from approved plan"
                )
            registration = snapshot["registration"]
            events = []
            for event_type in ("PREPARED", "RECOVERED"):
                event = AdvancedJournalRecoveryEvent(
                    recovery_event_id=uuid5(
                        NAMESPACE_URL,
                        f"{CONTRACT}:{registration.journal_generation}:"
                        f"{args.plan_report_sha256}:{event_type}",
                    ),
                    journal_generation=registration.journal_generation,
                    plan_id=registration.plan_id,
                    event_type=event_type,
                    recovery_contract=(
                        "advanced-journal-root-recovery-v1"
                    ),
                    plan_report_sha256=args.plan_report_sha256,
                    journal_files_sha256=expected_files,
                    raw_committed_offset=expected_offset,
                    normalized_committed_offset=expected_offset,
                    bridge_committed_offset=(
                        0 if event_type == "PREPARED" else expected_offset
                    ),
                    persisted_bars=(
                        0 if event_type == "PREPARED" else expected_bars
                    ),
                    created_at=datetime.now(UTC),
                )
                with provider.transaction(APP_ROLE) as unit:
                    stored = record_advanced_journal_recovery_event_cursor(
                        unit.cursor,
                        event,
                        expected_relative_path=registration.relative_path,
                        expected_metadata_sha256=(
                            registration.metadata_sha256
                        ),
                        expected_quarantine_reason=(
                            "JOURNAL_CHECKSUM_MISMATCH"
                        ),
                        expected_quarantine_detail=(
                            registration.relative_path
                        ),
                    )
                events.append(_event_report(stored))
            with provider.transaction(APP_ROLE) as unit:
                unit.cursor.execute(
                    "UPDATE rt.stream_gaps SET recovery_status='REPAIRED' "
                    "WHERE gap_id=%s AND journal_generation=%s "
                    "AND reason='STALE_OWNER_RECLAIMED' "
                    "AND recovery_status='UNRESOLVED' "
                    "AND EXISTS (SELECT 1 FROM "
                    "rt.advanced_journal_recovery_events AS recovery "
                    "WHERE recovery.journal_generation=%s "
                    "AND recovery.event_type='RECOVERED') "
                    "RETURNING gap_id",
                    (
                        str(args.prior_gap_id),
                        str(args.prior_recovered_generation),
                        str(args.prior_recovered_generation),
                    ),
                )
                if unit.cursor.fetchone() is None:
                    raise ValueError("prior recovered gap repair was rejected")
            report = {
                "contract": CONTRACT,
                "mode": "EXECUTE",
                "status": "RECOVERED",
                "checked_at": datetime.now(UTC).isoformat(),
                "plan_id": str(args.plan_id),
                "plan_report_sha256": args.plan_report_sha256,
                "recovery_events": events,
                "repaired_gap_id": str(args.prior_gap_id),
                "offset": expected_offset,
                "bars": expected_bars,
                "provider_requests": 0,
                "publication_advanced": False,
                "credentials_exposed": False,
            }
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
