#!/usr/bin/env python3
"""Plan or execute one evidence-preserving Advanced journal recovery."""
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
    AdvancedJournalRegistration,
    AdvancedJournalRecoveryEvent,
    discover_missing_advanced_journals_cursor,
    load_advanced_journal_registration_cursor,
    load_advanced_journal_recovery_events_cursor,
    record_advanced_journal_recovery_event_cursor,
)
from realtime.advanced_journal_replay import (  # noqa: E402
    _load_admissions_cursor,
    inspect_registered_journal,
)
from realtime.advanced_equity_journal_bridge import (  # noqa: E402
    AdvancedEquityJournalBridgeConfig,
    process_advanced_equity_journal_once,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
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
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from realtime.normalizer import normalize_raw_message  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.run_realtime_v2_advanced_journal_replay import (  # noqa: E402
    _provider,
)


UTC = timezone.utc
CONTRACT = "advanced-journal-root-recovery-v1"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: str, name: str) -> str:
    normalized = value.strip().lower()
    if SHA256_PATTERN.fullmatch(normalized) is None:
        raise argparse.ArgumentTypeError(f"{name} must be a SHA256 digest")
    return normalized


def _positive_int(value: str) -> int:
    normalized = int(value)
    if normalized <= 0:
        raise argparse.ArgumentTypeError("expected count must be positive")
    return normalized


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--journal-root", type=Path, required=True)
    parser.add_argument("--plan-id", type=UUID, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--expected-journal-sha256",
        type=lambda value: _sha256(value, "--expected-journal-sha256"),
    )
    parser.add_argument(
        "--expected-wal-sha256",
        type=lambda value: _sha256(value, "--expected-wal-sha256"),
    )
    parser.add_argument("--expected-raw-offset", type=_positive_int)
    parser.add_argument(
        "--expected-normalized-events",
        type=_positive_int,
    )
    parser.add_argument("--expected-persisted-bars", type=_positive_int)
    parser.add_argument("--plan-report", type=Path)
    parser.add_argument(
        "--plan-report-sha256",
        type=lambda value: _sha256(value, "--plan-report-sha256"),
    )
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Advanced journal recovery targets stocks_realtime_v2")
    args.journal_root = args.journal_root.resolve()
    if not args.journal_root.is_dir():
        parser.error("--journal-root must be an existing mounted directory")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    plan_values = (
        args.expected_journal_sha256,
        args.expected_wal_sha256,
        args.expected_raw_offset,
        args.expected_normalized_events,
        args.expected_persisted_bars,
    )
    execute_values = (
        args.plan_report,
        args.plan_report_sha256,
    )
    if args.plan and (
        any(value is None for value in plan_values)
        or any(value is not None for value in execute_values)
    ):
        parser.error(
            "--plan requires expected hashes/counts and no plan report"
        )
    if args.execute and (
        any(value is not None for value in plan_values)
        or any(value is None for value in execute_values)
    ):
        parser.error(
            "--execute requires a plan report and SHA256 only"
        )
    if args.plan_report is not None:
        args.plan_report = args.plan_report.resolve()
        if not args.plan_report.is_file():
            parser.error("--plan-report must exist")
    return args


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _iso(value):
    return value.astimezone(UTC).isoformat() if value is not None else None


def _registration_report(
    registration: AdvancedJournalRegistration,
) -> dict[str, object]:
    return {
        "journal_generation": str(registration.journal_generation),
        "plan_id": str(registration.plan_id),
        "stream_session_id": str(registration.stream_session_id),
        "cohort_id": str(registration.cohort_id),
        "manifest_id": str(registration.manifest_id),
        "session_date": registration.session_date.isoformat(),
        "owner_epoch": registration.owner_epoch,
        "relative_path": registration.relative_path,
        "path_sha256": registration.path_sha256,
        "metadata_sha256": registration.metadata_sha256,
        "journal_codec": registration.journal_codec,
        "journal_schema_version": registration.journal_schema_version,
        "normalizer_version": registration.normalizer_version,
        "shard_ids": registration.shard_ids,
        "shard_map_sha256": registration.shard_map_sha256,
        "status": registration.status,
        "raw_committed_offset": registration.raw_committed_offset,
        "normalized_committed_offset": (
            registration.normalized_committed_offset
        ),
        "bridge_committed_offset": registration.bridge_committed_offset,
        "sealed_files_sha256": registration.sealed_files_sha256,
    }


def _recovery_event_report(
    event: AdvancedJournalRecoveryEvent,
) -> dict[str, object]:
    return {
        "recovery_event_id": str(event.recovery_event_id),
        "journal_generation": str(event.journal_generation),
        "plan_id": str(event.plan_id),
        "event_type": event.event_type,
        "recovery_contract": event.recovery_contract,
        "plan_report_sha256": event.plan_report_sha256,
        "journal_files_sha256": event.journal_files_sha256,
        "raw_committed_offset": event.raw_committed_offset,
        "normalized_committed_offset": (
            event.normalized_committed_offset
        ),
        "bridge_committed_offset": event.bridge_committed_offset,
        "persisted_bars": event.persisted_bars,
        "created_at": event.created_at.astimezone(UTC).isoformat(),
    }


def _load_snapshot(provider, plan_id: UUID):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        missing = discover_missing_advanced_journals_cursor(
            unit.cursor,
            plan_id,
        )
        if len(missing) != 1:
            raise ValueError(
                "recovery requires exactly one MISSING_JOURNAL registration"
            )
        registration = missing[0]
        unit.cursor.execute(
            "SELECT quarantine_reason,quarantine_detail,quarantined_at "
            "FROM rt.advanced_journal_registry "
            "WHERE journal_generation=%s",
            (str(registration.journal_generation),),
        )
        quarantine = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT status,terminal_outcome,completed_at "
            "FROM rt.advanced_ingress_plans WHERE plan_id=%s",
            (str(plan_id),),
        )
        plan = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT status,ended_at,received_frame_count,"
            "received_byte_count,admitted_market_message_count,"
            "admitted_market_byte_count,last_raw_offset,"
            "last_event_offset,event_time_watermark,heartbeat_at "
            "FROM rt.stream_sessions WHERE session_id=%s",
            (str(registration.stream_session_id),),
        )
        session = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT stream_session_count,connection_attempt_count,"
            "decoded_byte_count,rest_repair_request_count,"
            "rest_repair_decoded_byte_count "
            "FROM rt.advanced_ingress_plan_usage WHERE plan_id=%s",
            (str(plan_id),),
        )
        usage = unit.cursor.fetchone()
        unit.cursor.execute(
            "SELECT count(*) FROM rt.bar_revisions "
            "WHERE journal_generation=%s",
            (str(registration.journal_generation),),
        )
        bar_count = int(unit.cursor.fetchone()[0])
        unit.cursor.execute(
            "SELECT count(*) FROM rt.advanced_journal_registry "
            "WHERE status IN ('REGISTERED','WRITING','DRAINING')"
        )
        incomplete_count = int(unit.cursor.fetchone()[0])
        recovery_events = load_advanced_journal_recovery_events_cursor(
            unit.cursor,
            registration.journal_generation,
        )
    if None in {quarantine, plan, session, usage}:
        raise ValueError("recovery durable state is incomplete")
    return {
        "registration": registration,
        "quarantine": {
            "reason": str(quarantine[0]),
            "detail": str(quarantine[1]),
            "quarantined_at": _iso(quarantine[2]),
        },
        "plan": {
            "status": str(plan[0]),
            "terminal_outcome": str(plan[1]),
            "completed_at": _iso(plan[2]),
        },
        "session": {
            "status": str(session[0]),
            "ended_at": _iso(session[1]),
            "received_frame_count": int(session[2]),
            "received_byte_count": int(session[3]),
            "admitted_market_message_count": int(session[4]),
            "admitted_market_byte_count": int(session[5]),
            "last_raw_offset": int(session[6]),
            "last_event_offset": int(session[7]),
            "event_time_watermark": _iso(session[8]),
            "heartbeat_at": _iso(session[9]),
        },
        "usage": {
            "stream_session_count": int(usage[0]),
            "connection_attempt_count": int(usage[1]),
            "decoded_byte_count": int(usage[2]),
            "rest_repair_request_count": int(usage[3]),
            "rest_repair_decoded_byte_count": int(usage[4]),
        },
        "bar_revision_count": bar_count,
        "incomplete_journal_count": incomplete_count,
        "recovery_events": tuple(
            _recovery_event_report(event)
            for event in recovery_events
        ),
    }


def _validate_snapshot(snapshot) -> None:
    registration = snapshot["registration"]
    quarantine = snapshot["quarantine"]
    plan = snapshot["plan"]
    session = snapshot["session"]
    usage = snapshot["usage"]
    counters = (
        registration.raw_committed_offset,
        registration.normalized_committed_offset,
        registration.bridge_committed_offset,
        session["received_frame_count"],
        session["received_byte_count"],
        session["admitted_market_message_count"],
        session["admitted_market_byte_count"],
        session["last_raw_offset"],
        session["last_event_offset"],
        usage["decoded_byte_count"],
        usage["rest_repair_request_count"],
        usage["rest_repair_decoded_byte_count"],
        snapshot["bar_revision_count"],
        snapshot["incomplete_journal_count"],
    )
    if (
        registration.status != "QUARANTINED"
        or quarantine["reason"] != "MISSING_JOURNAL"
        or quarantine["detail"] != registration.relative_path
        or registration.sealed_files_sha256 is not None
        or plan["status"] != "BLOCKED"
        or plan["terminal_outcome"] != "BLOCKED_CONFIGURATION"
        or session["status"] != "CONNECTING"
        or session["ended_at"] is not None
        or session["event_time_watermark"] is not None
        or usage["stream_session_count"] != 1
        or usage["connection_attempt_count"] != 1
        or snapshot["recovery_events"]
        or any(counters)
    ):
        raise ValueError(
            "durable state is not the zero-message MISSING_JOURNAL incident"
        )


def _journal_evidence(
    provider,
    root: Path,
    registration: AdvancedJournalRegistration,
    *,
    checked_at: datetime,
) -> dict[str, object]:
    path, reader, state = inspect_registered_journal(root, registration)
    wal_path = Path(str(path) + "-wal")
    if not wal_path.is_file():
        raise ValueError("recovery working copy is missing its WAL")
    if (
        state.raw_committed_offset <= 0
        or state.normalized_committed_raw_offset
        != state.raw_committed_offset
        or state.normalized_committed_event_offset <= 0
        or state.quarantine_count != 0
    ):
        raise ValueError(
            "recovery journal is not a fully normalized committed journal"
        )
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        admissions = _load_admissions_cursor(
            unit.cursor,
            registration,
            checked_at=checked_at,
        )
    raw_records = reader.read_after(0, limit=1_000)
    normalized_events = []
    normalization_quarantines = []
    for raw in raw_records:
        batch = normalize_raw_message(raw, admissions)
        normalized_events.extend(batch.events)
        normalization_quarantines.extend(batch.quarantines)
    if normalization_quarantines:
        raise ValueError(
            "recovery journal contains normalization quarantines"
        )
    stored_events = reader.read_normalized_after(0, limit=1_000)
    if (
        len(stored_events) != state.normalized_committed_event_offset
        or len(stored_events) != len(normalized_events)
    ):
        raise ValueError(
            "stored normalized journal event count changed"
        )
    event_report = tuple({
        "event_offset": event_offset,
        "raw_offset": event.raw_offset,
        "event_index": event.event_index,
        "instrument_id": event.instrument_id,
        "provider_symbol": event.provider_symbol,
        "asset_class": event.asset_class.value,
        "event_kind": event.event_kind.value,
        "event_at": event.event_at.isoformat(),
        "quality": event.quality.value,
        "details_json": event.details_json,
        "payload_sha256": event.payload_sha256,
    } for event_offset, event in enumerate(normalized_events, start=1))
    for expected, stored in zip(event_report, stored_events):
        if (
            expected["event_offset"] != stored.event_offset
            or expected["raw_offset"] != stored.raw_offset
            or expected["event_index"] != stored.event_index
            or expected["instrument_id"] != stored.instrument_id
            or expected["provider_symbol"] != stored.provider_symbol
            or expected["asset_class"] != stored.asset_class.value
            or expected["event_kind"] != stored.event_kind.value
            or expected["event_at"] != stored.event_at.isoformat()
            or expected["quality"] != stored.quality
            or expected["details_json"] != stored.details_json
            or expected["payload_sha256"] != stored.payload_sha256
        ):
            raise ValueError(
                "stored normalized journal event differs from raw evidence"
            )
    persistable_events = tuple(
        event
        for event in event_report
        if event["asset_class"] == "EQUITY"
        and event["event_kind"] == "AM"
        and event["quality"] == "GOOD"
    )
    return {
        "journal_path": str(path),
        "journal_sha256": _file_sha256(path),
        "journal_size": path.stat().st_size,
        "wal_sha256": _file_sha256(wal_path),
        "wal_size": wal_path.stat().st_size,
        "files_sha256": journal_files_sha256(path),
        "integrity_check": state.integrity_check,
        "generation": str(state.generation),
        "schema_version": state.schema_version,
        "raw_committed_offset": state.raw_committed_offset,
        "raw_record_count": len(raw_records),
        "normalized_committed_raw_offset": (
            state.normalized_committed_raw_offset
        ),
        "normalized_committed_event_offset": (
            state.normalized_committed_event_offset
        ),
        "quarantine_count": state.quarantine_count,
        "normalized_event_count": len(normalized_events),
        "normalization_quarantine_count": len(
            normalization_quarantines
        ),
        "persistable_bar_count": len(persistable_events),
        "events": event_report,
    }


def _plan_report(args, snapshot, evidence, checked_at):
    if (
        evidence["journal_sha256"] != args.expected_journal_sha256
        or evidence["wal_sha256"] != args.expected_wal_sha256
        or evidence["raw_committed_offset"]
        != args.expected_raw_offset
        or evidence["raw_record_count"] != args.expected_raw_offset
        or evidence["normalized_event_count"]
        != args.expected_normalized_events
        or evidence["persistable_bar_count"]
        != args.expected_persisted_bars
    ):
        raise ValueError(
            "working journal differs from the authorized evidence"
        )
    registration = snapshot["registration"]
    if evidence["generation"] != str(registration.journal_generation):
        raise ValueError("working journal generation differs from registry")
    report = {
        "contract": CONTRACT,
        "mode": "PLAN",
        "status": "READY",
        "checked_at": checked_at.isoformat(),
        "journal_root": str(args.journal_root),
        "registration": _registration_report(registration),
        "quarantine": snapshot["quarantine"],
        "plan": snapshot["plan"],
        "session": snapshot["session"],
        "usage": snapshot["usage"],
        "bar_revision_count": snapshot["bar_revision_count"],
        "incomplete_journal_count": snapshot["incomplete_journal_count"],
        "recovery_events": snapshot["recovery_events"],
        "journal_evidence": evidence,
        "authorized_outcome": {
            "raw_committed_offset": args.expected_raw_offset,
            "normalized_event_count": args.expected_normalized_events,
            "persisted_bars": args.expected_persisted_bars,
        },
        "snapshot_sha256": canonical_sha256({
            "registration": _registration_report(registration),
            "quarantine": snapshot["quarantine"],
            "plan": snapshot["plan"],
            "session": snapshot["session"],
            "usage": snapshot["usage"],
            "bar_revision_count": snapshot["bar_revision_count"],
            "incomplete_journal_count": snapshot[
                "incomplete_journal_count"
            ],
            "recovery_events": snapshot["recovery_events"],
            "journal_evidence": evidence,
        }),
        "provider_requests": 0,
        "publication_advanced": False,
        "credentials_exposed": False,
    }
    return report


def _load_plan_report(args):
    if args.plan_report.stat().st_size > 1024 * 1024:
        raise ValueError("recovery plan report exceeds its bound")
    payload = args.plan_report.read_bytes()
    if hashlib.sha256(payload).hexdigest() != args.plan_report_sha256:
        raise ValueError("recovery plan report SHA256 mismatch")
    report = json.loads(payload)
    if (
        report.get("contract") != CONTRACT
        or report.get("mode") != "PLAN"
        or report.get("status") != "READY"
        or report.get("registration", {}).get("plan_id")
        != str(args.plan_id)
    ):
        raise ValueError("recovery plan report identity is invalid")
    return report


def main(argv=None) -> int:
    args = parse_args(argv)
    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.BRIDGE,
    )
    if (
        config.equity_pipeline is not EquityPipeline.MASSIVE_ADVANCED
        or config.options_pipeline is not OptionsPipeline.DISABLED
    ):
        raise RuntimeError(
            "Advanced journal recovery requires Advanced equity "
            "and disabled options"
        )
    provider, pool = _provider(config)
    checked_at = datetime.now(UTC)
    try:
        snapshot = _load_snapshot(provider, args.plan_id)
        _validate_snapshot(snapshot)
        registration = snapshot["registration"]
        evidence = _journal_evidence(
            provider,
            args.journal_root,
            registration,
            checked_at=checked_at,
        )
        if args.plan:
            report = _plan_report(
                args,
                snapshot,
                evidence,
                checked_at,
            )
        else:
            approved = _load_plan_report(args)
            current = _plan_report(
                argparse.Namespace(
                    expected_journal_sha256=(
                        approved["journal_evidence"]["journal_sha256"]
                    ),
                    expected_wal_sha256=(
                        approved["journal_evidence"]["wal_sha256"]
                    ),
                    expected_raw_offset=(
                        approved["authorized_outcome"][
                            "raw_committed_offset"
                        ]
                    ),
                    expected_normalized_events=(
                        approved["authorized_outcome"][
                            "normalized_event_count"
                        ]
                    ),
                    expected_persisted_bars=(
                        approved["authorized_outcome"][
                            "persisted_bars"
                        ]
                    ),
                    journal_root=args.journal_root,
                ),
                snapshot,
                evidence,
                checked_at,
            )
            if current["snapshot_sha256"] != approved["snapshot_sha256"]:
                raise ValueError(
                    "recovery durable state differs from approved plan"
                )
            expected = approved["authorized_outcome"]
            prepared = AdvancedJournalRecoveryEvent(
                recovery_event_id=uuid5(
                    NAMESPACE_URL,
                    f"{CONTRACT}:{registration.journal_generation}:"
                    f"{args.plan_report_sha256}:PREPARED",
                ),
                journal_generation=registration.journal_generation,
                plan_id=registration.plan_id,
                event_type="PREPARED",
                recovery_contract=CONTRACT,
                plan_report_sha256=args.plan_report_sha256,
                journal_files_sha256=evidence["files_sha256"],
                raw_committed_offset=expected[
                    "raw_committed_offset"
                ],
                normalized_committed_offset=expected[
                    "normalized_event_count"
                ],
                bridge_committed_offset=0,
                persisted_bars=0,
                created_at=checked_at,
            )
            with provider.transaction(APP_ROLE) as unit:
                record_advanced_journal_recovery_event_cursor(
                    unit.cursor,
                    prepared,
                    expected_relative_path=registration.relative_path,
                    expected_metadata_sha256=registration.metadata_sha256,
                    expected_quarantine_reason="MISSING_JOURNAL",
                    expected_quarantine_detail=registration.relative_path,
                )
            bridge_config = AdvancedEquityJournalBridgeConfig(
                cohort_id=registration.cohort_id,
                journal_generation=registration.journal_generation,
                stream_session_id=registration.stream_session_id,
                owner_epoch=registration.owner_epoch,
                normalizer_version=registration.normalizer_version,
                shard_ids=registration.shard_ids,
                batch_limit=1_000,
            )
            status = process_advanced_equity_journal_once(
                V2WorkerDatabase(
                    provider,
                    RealtimeDatabaseRole.COMPUTE,
                ),
                RawJournalReader(
                    args.journal_root / registration.relative_path
                ),
                bridge_config,
                checked_at=checked_at,
            )
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                final = load_advanced_journal_registration_cursor(
                    unit.cursor,
                    registration.journal_generation,
                )
                unit.cursor.execute(
                    "SELECT count(*) FROM rt.bar_revisions "
                    "WHERE journal_generation=%s",
                    (str(registration.journal_generation),),
                )
                final_bars = int(unit.cursor.fetchone()[0])
            if (
                status.committed_offset
                != expected["normalized_event_count"]
                or status.persisted_bars != expected["persisted_bars"]
                or final_bars != expected["persisted_bars"]
            ):
                raise RuntimeError(
                    "Advanced journal recovery bridge outcome failed"
                )
            recovered_event = AdvancedJournalRecoveryEvent(
                recovery_event_id=uuid5(
                    NAMESPACE_URL,
                    f"{CONTRACT}:{registration.journal_generation}:"
                    f"{args.plan_report_sha256}:RECOVERED",
                ),
                journal_generation=registration.journal_generation,
                plan_id=registration.plan_id,
                event_type="RECOVERED",
                recovery_contract=CONTRACT,
                plan_report_sha256=args.plan_report_sha256,
                journal_files_sha256=evidence["files_sha256"],
                raw_committed_offset=expected[
                    "raw_committed_offset"
                ],
                normalized_committed_offset=expected[
                    "normalized_event_count"
                ],
                bridge_committed_offset=status.committed_offset,
                persisted_bars=final_bars,
                created_at=datetime.now(UTC),
            )
            with provider.transaction(APP_ROLE) as unit:
                record_advanced_journal_recovery_event_cursor(
                    unit.cursor,
                    recovered_event,
                    expected_relative_path=registration.relative_path,
                    expected_metadata_sha256=(
                        registration.metadata_sha256
                    ),
                    expected_quarantine_reason="MISSING_JOURNAL",
                    expected_quarantine_detail=registration.relative_path,
                )
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                recovery_events = (
                    load_advanced_journal_recovery_events_cursor(
                        unit.cursor,
                        registration.journal_generation,
                    )
                )
            if (
                final is None
                or final.status != "QUARANTINED"
                or final.raw_committed_offset != 0
                or final.normalized_committed_offset != 0
                or final.bridge_committed_offset != 0
                or tuple(
                    event.event_type for event in recovery_events
                ) != ("PREPARED", "RECOVERED")
            ):
                raise RuntimeError(
                    "Advanced journal recovery postcondition failed"
                )
            report = {
                "contract": CONTRACT,
                "mode": "EXECUTE",
                "status": "RECOVERED",
                "checked_at": checked_at.isoformat(),
                "plan_report_sha256": args.plan_report_sha256,
                "registration_before": _registration_report(registration),
                "registration_after": _registration_report(final),
                "recovery_events": tuple(
                    _recovery_event_report(event)
                    for event in recovery_events
                ),
                "journal_evidence_before": evidence,
                "authorized_outcome": expected,
                "committed_offset": status.committed_offset,
                "persisted_bars": status.persisted_bars,
                "bar_revision_count": final_bars,
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
