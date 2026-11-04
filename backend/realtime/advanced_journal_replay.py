"""Ordered SQLite normalization and PostgreSQL bridge crash recovery."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

from .advanced_equity_capability_refresh import (
    MASSIVE_STOCKS_CONNECTION_RESOURCE,
)
from .advanced_equity_journal_bridge import (
    AdvancedEquityJournalBridgeConfig,
    AdvancedEquityJournalBridgeStatus,
    AdvancedJournalBlockedError,
    process_advanced_equity_journal_once,
)
from .advanced_journal_registry import (
    AdvancedJournalRegistration,
    mark_advanced_journal_drained_cursor,
    mark_advanced_journal_writing_cursor,
    resolve_registered_journal_path,
    seal_advanced_journal_cursor,
    update_advanced_journal_offsets_cursor,
)
from .contracts import AssetClass
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .journal import (
    RawJournalReader,
    RawJournalWriter,
    journal_files_sha256,
)
from .normalizer import InstrumentAdmission, normalize_raw_message
from .stage_database_v2 import V2WorkerDatabase


class JournalReplayQuarantine(ValueError):
    def __init__(self, reason: str, detail: str) -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}")


class JournalReplayPending(RuntimeError):
    pass


def _decoder_causal_checked_at(
    reader: RawJournalReader,
    normalizer_version: str,
    checked_at: datetime,
) -> datetime:
    progress = reader.decoder_progress(normalizer_version)
    return (
        checked_at
        if progress is None
        else max(checked_at, progress.updated_at)
    )


def _load_admissions_cursor(
    cursor,
    registration: AdvancedJournalRegistration,
    *,
    checked_at: datetime,
) -> dict[str, InstrumentAdmission]:
    cursor.execute(
        "SELECT DISTINCT ON (member.instrument_id) "
        "member.instrument_id,reference.ticker "
        "FROM rt.subscription_members AS member "
        "JOIN rt.instruments AS instrument USING(instrument_id) "
        "JOIN ref.security_revisions AS reference USING(security_id) "
        "WHERE member.manifest_id=%s AND member.channel='AM' "
        "AND instrument.asset_class='EQUITY' AND reference.active "
        "AND reference.effective_from<=%s "
        "AND (reference.effective_to IS NULL OR reference.effective_to>%s) "
        "AND reference.observed_at<=%s "
        "ORDER BY member.instrument_id,reference.observed_at DESC,"
        "reference.revision_id DESC",
        (
            str(registration.manifest_id),
            checked_at,
            checked_at,
            checked_at,
        ),
    )
    admissions = {}
    for instrument_id, ticker in cursor.fetchall():
        admissions[str(ticker)] = InstrumentAdmission(
            int(instrument_id),
            str(ticker),
            str(ticker),
            AssetClass.EQUITY,
            0,
        )
    if not admissions:
        raise JournalReplayQuarantine(
            "MANIFEST_MISMATCH",
            "registered AM manifest has no active admissions",
        )
    return admissions


def inspect_registered_journal(
    root: Path,
    registration: AdvancedJournalRegistration,
    *,
    allow_initializing: bool = False,
):
    path = resolve_registered_journal_path(
        root,
        registration.relative_path,
    )
    if not path.is_file():
        raise JournalReplayQuarantine(
            "MISSING_JOURNAL",
            registration.relative_path,
        )
    try:
        reader = RawJournalReader(path)
        state = reader.recovery_state(registration.normalizer_version)
    except Exception as error:
        if (
            registration.status == "REGISTERED"
            and registration.raw_committed_offset == 0
            and registration.normalized_committed_offset == 0
            and registration.bridge_committed_offset == 0
            and allow_initializing
            and str(error) in {
                "raw journal metadata is unavailable",
                "no such table: journal_metadata",
            }
        ):
            raise JournalReplayPending(
                "raw journal initialization is pending"
            ) from error
        raise JournalReplayQuarantine(
            "CORRUPT_JOURNAL",
            str(error)[:320],
        ) from error
    if state.integrity_check.lower() != "ok":
        raise JournalReplayQuarantine(
            "CORRUPT_JOURNAL",
            state.integrity_check[:320],
        )
    if (
        state.generation != registration.journal_generation
        or state.schema_version != registration.journal_schema_version
    ):
        raise JournalReplayQuarantine(
            "JOURNAL_IDENTITY_MISMATCH",
            "generation or schema version changed",
        )
    if state.quarantine_count:
        raise JournalReplayQuarantine(
            "JOURNAL_CONTAINS_QUARANTINE",
            f"{state.quarantine_count} quarantined rows",
        )
    if (
        state.raw_committed_offset < registration.raw_committed_offset
        or state.normalized_committed_event_offset
        < registration.normalized_committed_offset
    ):
        raise JournalReplayQuarantine(
            "JOURNAL_OFFSET_REGRESSION",
            "SQLite offsets are behind durable registry",
        )
    if (
        registration.sealed_files_sha256 is not None
        and journal_files_sha256(path)
        != registration.sealed_files_sha256
    ):
        raise JournalReplayQuarantine(
            "JOURNAL_CHECKSUM_MISMATCH",
            registration.relative_path,
        )
    return path, reader, state


def _normalize_committed_raw(
    provider: V2ConnectionProvider,
    path: Path,
    registration: AdvancedJournalRegistration,
    *,
    checked_at: datetime,
) -> None:
    reader = RawJournalReader(path)
    progress = reader.decoder_progress(registration.normalizer_version)
    raw_offset = progress.committed_raw_offset if progress is not None else 0
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        admissions = _load_admissions_cursor(
            unit.cursor,
            registration,
            checked_at=checked_at,
        )
    while True:
        rows = reader.read_after(raw_offset, limit=1_000)
        if not rows:
            return
        with RawJournalWriter(
            path,
            generation=registration.journal_generation,
            clock=lambda: checked_at,
        ) as writer:
            for raw in rows:
                batch = normalize_raw_message(
                    raw,
                    admissions,
                    finality_observed_at=checked_at,
                )
                if batch.quarantines:
                    raise JournalReplayQuarantine(
                        "NORMALIZATION_QUARANTINE",
                        batch.quarantines[0].reason_code,
                    )
                writer.commit_normalized(batch)
                raw_offset = raw.raw_offset


def _postgres_reconciliation_cursor(
    cursor,
    registration: AdvancedJournalRegistration,
    *,
    checked_at: datetime,
) -> tuple[int, int, bool]:
    # The heartbeat only moves when frames arrive, about once a minute on the AM
    # channel, so it can briefly look stale on a healthy stream. A session whose
    # owner still holds the provider connection lease is live, not abandoned.
    cursor.execute(
        "SELECT session.ended_at IS NOT NULL,session.heartbeat_at,"
        "(profile.profile_document->'settings'->>"
        "'stale_after_seconds')::integer,"
        "EXISTS (SELECT 1 FROM rt.worker_leases AS lease "
        "WHERE lease.resource_key=%s AND lease.owner_id=session.owner_id "
        "AND lease.lease_until>%s) "
        "FROM rt.stream_sessions AS session "
        "JOIN rt.advanced_ingress_plans AS plan ON plan.plan_id=%s "
        "JOIN rt.advanced_ingress_runtime_profiles AS profile "
        "USING(runtime_profile_sha256) "
        "WHERE session.session_id=%s "
        "AND session.owner_epoch=%s AND session.journal_generation=%s",
        (
            MASSIVE_STOCKS_CONNECTION_RESOURCE,
            checked_at,
            str(registration.plan_id),
            str(registration.stream_session_id),
            registration.owner_epoch,
            str(registration.journal_generation),
        ),
    )
    session = cursor.fetchone()
    if session is None:
        raise JournalReplayQuarantine(
            "STREAM_IDENTITY_MISMATCH",
            "registered stream session is unavailable",
        )
    cursor.execute(
        "SELECT shard_id,shard_map_sha256,owner_epoch,committed_offset "
        "FROM rt.consumer_checkpoints WHERE cohort_id=%s "
        "AND journal_generation=%s ORDER BY shard_id",
        (
            str(registration.cohort_id),
            str(registration.journal_generation),
        ),
    )
    checkpoints = tuple(cursor.fetchall())
    if checkpoints:
        if (
            tuple(int(row[0]) for row in checkpoints)
            != registration.shard_ids
            or any(
                row[1] != registration.shard_map_sha256
                or int(row[2]) > registration.owner_epoch
                for row in checkpoints
            )
            or len({int(row[3]) for row in checkpoints}) != 1
        ):
            raise JournalReplayQuarantine(
                "CHECKPOINT_MISMATCH",
                "checkpoint shard map, owner or offsets changed",
            )
        checkpoint_offset = int(checkpoints[0][3])
    else:
        checkpoint_offset = 0
    cursor.execute(
        "SELECT COALESCE(MAX(source_offset),0) "
        "FROM rt.bar_revisions WHERE journal_generation=%s "
        "AND stream_session_id=%s",
        (
            str(registration.journal_generation),
            str(registration.stream_session_id),
        ),
    )
    canonical_offset = int(cursor.fetchone()[0])
    if canonical_offset > checkpoint_offset:
        raise JournalReplayQuarantine(
            "CANONICAL_CHECKPOINT_MISMATCH",
            "canonical bars are ahead of their checkpoint",
        )
    stale = (
        not session[3]
        and session[1] is not None
        and session[1]
        <= checked_at - timedelta(seconds=int(session[2]))
    )
    return checkpoint_offset, canonical_offset, bool(session[0]) or stale


def replay_registered_journal_once(
    provider: V2ConnectionProvider,
    database: V2WorkerDatabase,
    root: Path,
    registration: AdvancedJournalRegistration,
    *,
    checked_at: datetime,
    batch_limit: int,
) -> AdvancedEquityJournalBridgeStatus:
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        _checkpoint, _canonical, initial_session_ended = (
            _postgres_reconciliation_cursor(
                unit.cursor,
                registration,
                checked_at=checked_at,
            )
        )
    path, _reader, state = inspect_registered_journal(
        root,
        registration,
        allow_initializing=not initial_session_ended,
    )
    if state.normalized_committed_raw_offset < state.raw_committed_offset:
        if not initial_session_ended:
            raise JournalReplayPending(
                "live journal normalization is owned by ingress"
            )
        _normalize_committed_raw(
            provider,
            path,
            registration,
            checked_at=checked_at,
        )
        path, _reader, state = inspect_registered_journal(root, registration)
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        checkpoint_offset, _canonical_offset, session_ended = (
            _postgres_reconciliation_cursor(
                unit.cursor,
                registration,
                checked_at=checked_at,
            )
        )
    if checkpoint_offset < registration.bridge_committed_offset:
        raise JournalReplayQuarantine(
            "CHECKPOINT_OFFSET_REGRESSION",
            "PostgreSQL checkpoint is behind registry",
        )
    if registration.status == "REGISTERED":
        with provider.transaction(APP_ROLE) as unit:
            mark_advanced_journal_writing_cursor(
                unit.cursor,
                registration.journal_generation,
                checked_at=checked_at,
            )
        registration = replace(registration, status="WRITING")
    if session_ended and registration.status == "WRITING":
        checksum = journal_files_sha256(path)
        with provider.transaction(APP_ROLE) as unit:
            seal_advanced_journal_cursor(
                unit.cursor,
                registration.journal_generation,
                raw_committed_offset=state.raw_committed_offset,
                normalized_committed_offset=(
                    state.normalized_committed_event_offset
                ),
                sealed_files_sha256=checksum,
                checked_at=checked_at,
            )
        registration = replace(
            registration,
            status="DRAINING",
            sealed_files_sha256=checksum,
        )
    config = AdvancedEquityJournalBridgeConfig(
        cohort_id=registration.cohort_id,
        journal_generation=registration.journal_generation,
        stream_session_id=registration.stream_session_id,
        owner_epoch=registration.owner_epoch,
        normalizer_version=registration.normalizer_version,
        shard_ids=registration.shard_ids,
        batch_limit=batch_limit,
    )
    try:
        reader = RawJournalReader(path)
        bridge_checked_at = _decoder_causal_checked_at(
            reader,
            registration.normalizer_version,
            checked_at,
        )
        status = process_advanced_equity_journal_once(
            database,
            reader,
            config,
            checked_at=bridge_checked_at,
        )
    except AdvancedJournalBlockedError:
        raise
    except ValueError as error:
        raise JournalReplayQuarantine(
            "BRIDGE_VALIDATION_FAILED",
            str(error)[:320],
        ) from error
    with provider.transaction(APP_ROLE) as unit:
        update_advanced_journal_offsets_cursor(
            unit.cursor,
            registration.journal_generation,
            raw_committed_offset=state.raw_committed_offset,
            normalized_committed_offset=(
                state.normalized_committed_event_offset
            ),
            bridge_committed_offset=status.committed_offset,
            checked_at=bridge_checked_at,
        )
        if (
            registration.status == "DRAINING"
            and status.committed_offset
            >= state.normalized_committed_event_offset
        ):
            mark_advanced_journal_drained_cursor(
                unit.cursor,
                registration.journal_generation,
                bridge_committed_offset=status.committed_offset,
                checked_at=bridge_checked_at,
            )
    return status
