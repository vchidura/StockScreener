#!/usr/bin/env python3
"""Plan or replay frames a falsely drained Advanced journal never delivered.

A journal could be sealed while its stream was still live. Ingress kept writing
frames that no reader would ever visit, because DRAINED is terminal. This replays
exactly those frames through the normal bridge, so they keep their stream lineage
and original clocks. It never changes the registry row, which stays as evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_journal_bridge import (  # noqa: E402
    AdvancedEquityJournalBridgeConfig,
    process_advanced_equity_journal_once,
    scan_advanced_equity_journal_batch,
)
from realtime.advanced_equity_preparation import (  # noqa: E402
    advanced_equity_live_priority_cursor,
)
from realtime.advanced_journal_registry import (  # noqa: E402
    load_advanced_journal_registration_cursor,
    resolve_registered_journal_path,
)
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from realtime.equity_pipeline_config import (  # noqa: E402
    RealtimeV2PipelineConfig,
    RealtimeV2Process,
)
from realtime.journal import RawJournalReader  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.run_realtime_v2_advanced_journal_replay import (  # noqa: E402
    _provider,
)


UTC = timezone.utc
CONTRACT = "advanced-drained-journal-replay-v1"
# The stream must outlive the seal by more than the stale limit to prove the
# seal was false, rather than the ordinary ordering of a normal shutdown.
MINIMUM_POST_SEAL_LIVE = timedelta(seconds=60)
SCAN_BATCH_LIMIT = 1_000


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--journal-root", type=Path, required=True)
    parser.add_argument("--journal-generation", type=UUID, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--env-file", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--plan-report", type=Path)
    parser.add_argument("--plan-report-sha256")
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
        parser.error("--journal-root must be an existing directory")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    if args.execute and (
        args.plan_report is None or args.plan_report_sha256 is None
    ):
        parser.error("--execute requires --plan-report and its SHA256")
    if args.plan and (
        args.plan_report is not None or args.plan_report_sha256 is not None
    ):
        parser.error("--plan does not accept a plan report")
    return args


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bridge_config(registration) -> AdvancedEquityJournalBridgeConfig:
    return AdvancedEquityJournalBridgeConfig(
        cohort_id=registration.cohort_id,
        journal_generation=registration.journal_generation,
        stream_session_id=registration.stream_session_id,
        owner_epoch=registration.owner_epoch,
        normalizer_version=registration.normalizer_version,
        shard_ids=registration.shard_ids,
        batch_limit=SCAN_BATCH_LIMIT,
    )


def _load_snapshot(provider, generation: UUID, *, checked_at: datetime):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        cursor = unit.cursor
        registration = load_advanced_journal_registration_cursor(
            cursor,
            generation,
        )
        if registration is None:
            raise ValueError("journal generation is not registered")
        cursor.execute(
            "SELECT drained_at FROM rt.advanced_journal_registry "
            "WHERE journal_generation=%s",
            (str(generation),),
        )
        drained_at = cursor.fetchone()[0]
        cursor.execute(
            "SELECT ended_at,last_raw_offset,last_event_offset "
            "FROM rt.stream_sessions WHERE session_id=%s",
            (str(registration.stream_session_id),),
        )
        session = cursor.fetchone()
        cursor.execute(
            "SELECT status FROM rt.advanced_ingress_plans WHERE plan_id=%s",
            (str(registration.plan_id),),
        )
        plan_status = cursor.fetchone()[0]
        cursor.execute(
            "SELECT DISTINCT committed_offset FROM rt.consumer_checkpoints "
            "WHERE cohort_id=%s AND journal_generation=%s",
            (str(registration.cohort_id), str(generation)),
        )
        checkpoints = tuple(int(row[0]) for row in cursor.fetchall())
        cursor.execute(
            "SELECT count(*),COALESCE(max(source_offset),0) "
            "FROM rt.bar_revisions WHERE journal_generation=%s",
            (str(generation),),
        )
        bars, canonical_offset = cursor.fetchone()
        live = advanced_equity_live_priority_cursor(
            cursor,
            checked_at=checked_at,
        )
    return {
        "registration": registration,
        "drained_at": drained_at,
        "session_ended_at": session[0] if session else None,
        "session_last_raw_offset": int(session[1]) if session else 0,
        "session_last_event_offset": int(session[2]) if session else 0,
        "plan_status": plan_status,
        "checkpoints": checkpoints,
        "bar_count": int(bars),
        "canonical_offset": int(canonical_offset),
        "live_stream_active": live,
    }


def validate_false_drain(snapshot, state, *, resuming: bool = False) -> None:
    """Accept only a journal proven sealed early, with its frames still pending."""
    registration = snapshot["registration"]
    drained = snapshot["drained_at"]
    ended = snapshot["session_ended_at"]
    checkpoint = (
        snapshot["checkpoints"][0]
        if len(snapshot["checkpoints"]) == 1
        else None
    )
    failures = []
    if registration.status != "DRAINED" or drained is None:
        failures.append("journal is not DRAINED")
    if ended is None:
        failures.append("stream session has not ended")
    elif drained is not None and ended - drained <= MINIMUM_POST_SEAL_LIVE:
        failures.append("stream did not outlive the seal; drain was not false")
    if snapshot["live_stream_active"]:
        failures.append("a Stocks stream is live")
    if snapshot["plan_status"] in {"READY", "ACTIVE"}:
        failures.append("ingress plan is still active")
    if checkpoint is None:
        failures.append("journal checkpoint is unavailable or ambiguous")
    elif not resuming and checkpoint != registration.bridge_committed_offset:
        failures.append("checkpoint moved away from the drained offset")
    elif checkpoint < registration.bridge_committed_offset:
        failures.append("checkpoint is behind the drained offset")
    if snapshot["canonical_offset"] > (checkpoint or 0):
        failures.append("canonical bars are ahead of the checkpoint")
    if (
        state.generation != registration.journal_generation
        or state.schema_version != registration.journal_schema_version
    ):
        failures.append("journal file identity changed")
    if state.integrity_check.lower() != "ok" or state.quarantine_count:
        failures.append("journal file is damaged or quarantined")
    if (
        state.raw_committed_offset != snapshot["session_last_raw_offset"]
        or state.normalized_committed_event_offset
        != snapshot["session_last_event_offset"]
        or state.normalized_committed_raw_offset
        != state.raw_committed_offset
    ):
        failures.append("journal offsets disagree with the stream session")
    if failures:
        raise ValueError("; ".join(failures))


def _scan_pending(reader, registration, start_offset):
    """Read every undelivered event through the bridge's own scan rules."""
    config = _bridge_config(registration)
    offsets = {shard: start_offset for shard in config.shard_ids}
    records = []
    while True:
        batch = scan_advanced_equity_journal_batch(reader, config, offsets)
        if batch is None:
            return tuple(records)
        records.extend(batch.processable_records)
        offsets = {
            shard: batch.scanned_through_offset for shard in config.shard_ids
        }


def _pending_report(records):
    if not records:
        return {"events": 0}
    minutes = sorted({row.event_at for row in records})
    return {
        "events": len(records),
        "first_event_at": minutes[0].isoformat(),
        "last_event_at": minutes[-1].isoformat(),
        "distinct_event_times": len(minutes),
        "tickers": sorted({row.provider_symbol for row in records}),
    }


def _plan(args, provider, path, reader, snapshot, state, checked_at):
    registration = snapshot["registration"]
    validate_false_drain(snapshot, state)
    records = _scan_pending(
        reader,
        registration,
        registration.bridge_committed_offset,
    )
    if not records:
        raise ValueError("journal has no undelivered frames")
    return {
        "contract": CONTRACT,
        "mode": "PLAN",
        "checked_at": checked_at.isoformat(),
        "journal_generation": str(registration.journal_generation),
        "stream_session_id": str(registration.stream_session_id),
        "relative_path": registration.relative_path,
        "journal_sha256": _file_sha256(path),
        "sealed_files_sha256": registration.sealed_files_sha256,
        "drained_at": snapshot["drained_at"].isoformat(),
        "session_ended_at": snapshot["session_ended_at"].isoformat(),
        "bridge_committed_offset": registration.bridge_committed_offset,
        "target_event_offset": state.normalized_committed_event_offset,
        "bars_before": snapshot["bar_count"],
        "pending": _pending_report(records),
        "provider_requests": 0,
        "database_writes": 0,
    }


def _execute(args, provider, path, reader, snapshot, state, checked_at):
    plan = json.loads(args.plan_report.read_text(encoding="utf-8"))
    registration = snapshot["registration"]
    if (
        _file_sha256(args.plan_report) != args.plan_report_sha256.lower()
        or plan["contract"] != CONTRACT
        or plan["journal_generation"] != str(registration.journal_generation)
        or plan["journal_sha256"] != _file_sha256(path)
        or plan["target_event_offset"]
        != state.normalized_committed_event_offset
    ):
        raise ValueError("plan report does not match this exact journal")
    validate_false_drain(snapshot, state, resuming=True)
    config = _bridge_config(registration)
    database = V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)
    target = state.normalized_committed_event_offset
    persisted = batches = 0
    offset = snapshot["checkpoints"][0]
    while offset < target:
        status = process_advanced_equity_journal_once(
            database,
            reader,
            config,
            checked_at=datetime.now(UTC),
        )
        if status.committed_offset <= offset:
            raise ValueError("journal replay stopped advancing")
        offset = status.committed_offset
        persisted += status.persisted_bars
        batches += 1
    after = _load_snapshot(
        provider,
        registration.journal_generation,
        checked_at=datetime.now(UTC),
    )
    if (
        after["checkpoints"] != (target,)
        or after["canonical_offset"] != target
        or after["registration"].status != "DRAINED"
    ):
        raise ValueError("journal replay did not reach its exact target")
    return {
        "contract": CONTRACT,
        "mode": "EXECUTE",
        "checked_at": checked_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "journal_generation": str(registration.journal_generation),
        "plan_report_sha256": args.plan_report_sha256.lower(),
        "journal_sha256": plan["journal_sha256"],
        "start_offset": snapshot["checkpoints"][0],
        "final_offset": offset,
        "batches": batches,
        "persisted_bars": persisted,
        "bars_before": snapshot["bar_count"],
        "bars_after": after["bar_count"],
        "provider_requests": 0,
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    env_file = args.env_file or BACKEND_DIR / ".env"
    file_environment = (
        {
            key: value
            for key, value in dotenv_values(env_file).items()
            if value is not None
        }
        if env_file.is_file()
        else {}
    )
    config = RealtimeV2PipelineConfig.from_mapping({
        **file_environment,
        **os.environ,
    },
        process=RealtimeV2Process.BRIDGE,
    )
    provider, pool = _provider(config)
    try:
        checked_at = datetime.now(UTC)
        snapshot = _load_snapshot(
            provider,
            args.journal_generation,
            checked_at=checked_at,
        )
        registration = snapshot["registration"]
        path = resolve_registered_journal_path(
            args.journal_root,
            registration.relative_path,
        )
        reader = RawJournalReader(path)
        state = reader.recovery_state(registration.normalizer_version)
        report = (
            _plan(args, provider, path, reader, snapshot, state, checked_at)
            if args.plan
            else _execute(
                args, provider, path, reader, snapshot, state, checked_at,
            )
        )
        write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
