from __future__ import annotations

from collections.abc import Sequence
from contextlib import nullcontext
from uuid import UUID

from psycopg2.extras import execute_batch, execute_values

from .contracts import canonical_json
from .detector_evidence import (
    RealtimeDetectorCheckpoint,
    RealtimeDetectorEvent,
    RealtimeDetectorState,
    RealtimeDetectorVersion,
)
from .portal_projection import RealtimeAlertProjection
from .portal_projection_repository import outbox_identity


DETECTOR_BATCH_PAGE_SIZE = 100


def _require_input_references(cursor, events: Sequence[RealtimeDetectorEvent]) -> None:
    """Event inputs hold soft references (no FK) and carry their values; verify the ids now."""
    inputs = tuple(row for event in events for row in event.inputs)
    checks = (
        (
            "feature_snapshot_id",
            "SELECT snapshot_id FROM rt.feature_snapshots WHERE snapshot_id=ANY(%s::uuid[])",
        ),
        (
            "bar_revision_id",
            "SELECT bar_revision_id FROM rt.bar_catalog WHERE bar_revision_id=ANY(%s::uuid[])",
        ),
        (
            "option_mark_evidence_id",
            "SELECT evidence_id FROM rt.option_mark_evidence WHERE evidence_id=ANY(%s::uuid[])",
        ),
    )
    for attribute, statement in checks:
        wanted = sorted({
            str(getattr(row, attribute)) for row in inputs
            if getattr(row, attribute) is not None
        })
        if not wanted:
            continue
        cursor.execute(statement, (wanted,))
        if {str(row[0]) for row in cursor.fetchall()} != set(wanted):
            raise ValueError(f"detector event input {attribute} is not available")
    source_bindings = {
        (str(row.source_flow_id), str(row.source_cohort_id))
        for row in inputs
        if row.source_flow_id is not None
    }
    if source_bindings:
        cursor.execute(
            "SELECT source_flow_id,source_cohort_id "
            "FROM rt.option_source_flows "
            "WHERE source_flow_id=ANY(%s::uuid[])",
            ([source_flow_id for source_flow_id, _ in source_bindings],),
        )
        stored = {(str(row[0]), str(row[1])) for row in cursor.fetchall()}
        if stored != source_bindings:
            raise ValueError(
                "detector event input source-flow binding is not available"
            )


def _persist_detector_evaluation_batch(
    connection,
    *,
    repository_cursor,
    lease_resource_key: str,
    owner_id: UUID,
    checkpoint: RealtimeDetectorCheckpoint,
    versions: Sequence[RealtimeDetectorVersion],
    states: Sequence[RealtimeDetectorState],
    events: Sequence[RealtimeDetectorEvent],
    alerts: Sequence[RealtimeAlertProjection] = (),
    values_executor=execute_values,
    batch_executor=execute_batch,
) -> None:
    version_rows = tuple(versions)
    state_rows = tuple(states)
    event_rows = tuple(events)
    alert_rows = tuple(alerts)
    resource_key = lease_resource_key.strip()
    if not resource_key:
        raise ValueError("detector lease resource key cannot be blank")
    versions_by_hash = {row.detector_sha256: row for row in version_rows}
    if len(versions_by_hash) != len(version_rows):
        raise ValueError("detector versions must be unique")
    if any(
        row.cohort_id != checkpoint.cohort_id
        or row.detector_sha256 not in versions_by_hash
        for row in (*state_rows, *event_rows)
    ):
        raise ValueError("detector batch identity does not match checkpoint or versions")
    if len({row.event_id for row in event_rows}) != len(event_rows):
        raise ValueError("detector event IDs must be unique")
    event_ids = {row.event_id for row in event_rows}
    if any(
        row.cohort_id != checkpoint.cohort_id or row.event_id not in event_ids
        for row in alert_rows
    ):
        raise ValueError("alert projections must belong to events in the detector batch")
    for event in event_rows:
        for input_row in event.inputs:
            if not input_row.retained_input:
                raise ValueError("detector event inputs must be retained by value")
            if (
                input_row.journal_generation is not None
                and (
                    input_row.journal_generation != checkpoint.journal_generation
                    or input_row.source_offset > checkpoint.committed_offset
                )
            ):
                raise ValueError("detector event input exceeds checkpoint boundary")

    expected_versions = {
        row.detector_sha256: (
            row.detector_id,
            row.version,
            row.code_sha256,
            row.policy_sha256,
            row.valuation_sha256,
        )
        for row in version_rows
    }
    expected_states = {
        (row.detector_sha256, str(row.cohort_id), row.state_key): (
            row.revision,
            row.state_schema,
            row.state_sha256,
        )
        for row in state_rows
    }
    expected_events = {
        str(row.event_id): (row.dedup_sha256, row.evidence_sha256)
        for row in event_rows
    }
    if (connection is None) == (repository_cursor is None):
        raise ValueError("exactly one connection or repository cursor is required")
    try:
        cursor_context = (
            connection.cursor()
            if connection is not None
            else nullcontext(repository_cursor)
        )
        with cursor_context as cursor:
            cursor.execute(
                "SELECT owner_id, owner_epoch, lease_until > clock_timestamp() "
                "FROM rt.worker_leases WHERE resource_key=%s FOR UPDATE",
                (resource_key,),
            )
            lease = cursor.fetchone()
            if lease is None or str(lease[0]) != str(owner_id):
                raise ValueError("detector lease owner does not match")
            if lease[1] != checkpoint.owner_epoch or not lease[2]:
                raise ValueError("detector lease is stale or expired")

            if version_rows:
                values_executor(
                    cursor,
                    "INSERT INTO rt.detector_versions ("
                    "detector_sha256, detector_id, version, code_sha256, policy_sha256, "
                    "valuation_sha256, requirements, output_kind) VALUES %s "
                    "ON CONFLICT (detector_sha256) DO NOTHING",
                    [
                        (
                            row.detector_sha256, row.detector_id, row.version,
                            row.code_sha256, row.policy_sha256, row.valuation_sha256,
                            canonical_json(row.requirements), row.output_kind,
                        )
                        for row in version_rows
                    ],
                    page_size=DETECTOR_BATCH_PAGE_SIZE,
                )
                cursor.execute(
                    "SELECT detector_sha256, detector_id, version, code_sha256, "
                    "policy_sha256, valuation_sha256 FROM rt.detector_versions "
                    "WHERE detector_sha256=ANY(%s::text[])",
                    ([row.detector_sha256 for row in version_rows],),
                )
                stored_versions = {
                    row[0]: tuple(row[1:]) for row in cursor.fetchall()
                }
                if stored_versions != expected_versions:
                    raise ValueError("persisted detector version identity conflict")

            if state_rows:
                batch_executor(
                    cursor,
                    "INSERT INTO rt.detector_state ("
                    "detector_sha256, cohort_id, state_key, revision, state_schema, "
                    "state_sha256, state_payload, updated_at) VALUES ("
                    "%s, %s, %s, %s, %s, %s, %s::jsonb, %s) ON CONFLICT ("
                    "detector_sha256, cohort_id, state_key) DO UPDATE SET "
                    "revision=EXCLUDED.revision, state_schema=EXCLUDED.state_schema, "
                    "state_sha256=EXCLUDED.state_sha256, state_payload=EXCLUDED.state_payload, "
                    "updated_at=EXCLUDED.updated_at "
                    "WHERE rt.detector_state.revision < EXCLUDED.revision",
                    [
                        (
                            row.detector_sha256, str(row.cohort_id), row.state_key,
                            row.revision, row.state_schema, row.state_sha256,
                            canonical_json(row.state_payload), row.updated_at,
                        )
                        for row in state_rows
                    ],
                    page_size=DETECTOR_BATCH_PAGE_SIZE,
                )
                cursor.execute(
                    "SELECT detector_sha256, cohort_id, state_key, revision, state_schema, "
                    "state_sha256 FROM rt.detector_state WHERE cohort_id=%s "
                    "AND (detector_sha256, state_key)=ANY(%s)",
                    (
                        str(checkpoint.cohort_id),
                        [(row.detector_sha256, row.state_key) for row in state_rows],
                    ),
                )
                stored_states = {
                    (row[0], str(row[1]), row[2]): tuple(row[3:])
                    for row in cursor.fetchall()
                }
                if stored_states != expected_states:
                    raise ValueError("persisted detector state identity conflict")

            if event_rows:
                values_executor(
                    cursor,
                    "INSERT INTO rt.detector_events ("
                    "event_id, dedup_sha256, cohort_id, provider_tier, detector_sha256, "
                    "instrument_id, trigger_identity, trigger_at, decision_at, valid_until, "
                    "disposition, direction, reason_codes, readiness_epoch, evidence_sha256, "
                    "payload_schema, payload) VALUES %s ON CONFLICT (event_id) DO NOTHING",
                    [
                        (
                            str(row.event_id), row.dedup_sha256, str(row.cohort_id),
                            row.provider_tier, row.detector_sha256, row.instrument_id,
                            row.trigger_identity, row.trigger_at, row.decision_at,
                            row.valid_until, row.disposition, row.direction,
                            list(row.reason_codes), row.readiness_epoch, row.evidence_sha256,
                            row.payload_schema, canonical_json(row.payload),
                        )
                        for row in event_rows
                    ],
                    page_size=DETECTOR_BATCH_PAGE_SIZE,
                )
                cursor.execute(
                    "SELECT event_id, dedup_sha256, evidence_sha256 FROM rt.detector_events "
                    "WHERE event_id=ANY(%s::uuid[])",
                    ([str(row.event_id) for row in event_rows],),
                )
                stored_events = {
                    str(row[0]): (row[1], row[2]) for row in cursor.fetchall()
                }
                if stored_events != expected_events:
                    raise ValueError("persisted detector event identity conflict")
                inputs = [
                    (
                        str(event.event_id), row.input_role, row.input_key, row.source_kind,
                        str(row.feature_snapshot_id) if row.feature_snapshot_id else None,
                        str(row.bar_revision_id) if row.bar_revision_id else None,
                        str(row.option_mark_evidence_id)
                        if row.option_mark_evidence_id else None,
                        str(row.source_record_id) if row.source_record_id else None,
                        str(row.journal_generation) if row.journal_generation else None,
                        str(row.stream_session_id) if row.stream_session_id else None,
                        row.source_offset,
                        str(row.source_flow_id) if row.source_flow_id else None,
                        str(row.source_cohort_id) if row.source_cohort_id else None,
                        row.input_sha256, row.available_at, row.received_at,
                        canonical_json(row.retained_input),
                    )
                    for event in event_rows for row in event.inputs
                ]
                if inputs:
                    _require_input_references(cursor, event_rows)
                    values_executor(
                        cursor,
                        "INSERT INTO rt.detector_event_inputs ("
                        "event_id, input_role, input_key, source_kind, feature_snapshot_id, "
                        "bar_revision_id, option_mark_evidence_id, source_record_id, "
                        "journal_generation, stream_session_id, source_offset, "
                        "source_flow_id, source_cohort_id, input_sha256, available_at, "
                        "received_at, retained_input) VALUES %s ON CONFLICT DO NOTHING",
                        inputs,
                        page_size=DETECTOR_BATCH_PAGE_SIZE,
                    )

            if alert_rows:
                values_executor(
                    cursor,
                    "INSERT INTO rt.alert_current ("
                    "alert_id, cohort_id, event_id, alert_kind, lifecycle, as_of, "
                    "valid_until, projection_schema, projection_sha256, projection) VALUES %s "
                    "ON CONFLICT (alert_id) DO UPDATE SET lifecycle=EXCLUDED.lifecycle, "
                    "as_of=EXCLUDED.as_of, valid_until=EXCLUDED.valid_until, "
                    "projection_schema=EXCLUDED.projection_schema, "
                    "projection_sha256=EXCLUDED.projection_sha256, "
                    "projection=EXCLUDED.projection, updated_at=clock_timestamp() "
                    "WHERE rt.alert_current.event_id=EXCLUDED.event_id "
                    "AND rt.alert_current.as_of < EXCLUDED.as_of",
                    [
                        (
                            str(row.alert_id), str(row.cohort_id), str(row.event_id),
                            row.alert_kind, row.lifecycle, row.as_of, row.valid_until,
                            row.projection_schema, row.projection_sha256,
                            canonical_json(row.projection),
                        )
                        for row in alert_rows
                    ],
                    page_size=DETECTOR_BATCH_PAGE_SIZE,
                )
                outbox_rows = []
                for alert in alert_rows:
                    for destination in alert.destinations:
                        payload = {
                            "alert_id": alert.alert_id,
                            "event_id": alert.event_id,
                            "alert_kind": alert.alert_kind,
                            "projection_schema": alert.projection_schema,
                            "projection_sha256": alert.projection_sha256,
                            "projection": alert.projection,
                        }
                        outbox_id, payload_sha256 = outbox_identity(
                            alert.event_id,
                            destination,
                            "ALERT_UPSERT",
                            payload,
                        )
                        outbox_rows.append((
                            str(outbox_id), str(alert.cohort_id), str(alert.event_id),
                            destination, "ALERT_UPSERT", payload_sha256,
                            canonical_json(payload), alert.as_of, alert.as_of,
                        ))
                values_executor(
                    cursor,
                    "INSERT INTO rt.alert_outbox ("
                    "outbox_id, cohort_id, event_id, destination, message_kind, "
                    "payload_sha256, payload, created_at, next_attempt_at) VALUES %s "
                    "ON CONFLICT (event_id, destination, message_kind) DO NOTHING",
                    outbox_rows,
                    page_size=DETECTOR_BATCH_PAGE_SIZE,
                )

            cursor.execute(
                "SELECT owner_epoch, committed_offset, checkpoint_id, state_hash "
                "FROM rt.consumer_checkpoints WHERE consumer_name=%s AND cohort_id=%s "
                "AND journal_generation=%s AND shard_id=%s FOR UPDATE",
                (
                    checkpoint.consumer_name, str(checkpoint.cohort_id),
                    str(checkpoint.journal_generation), checkpoint.shard_id,
                ),
            )
            current = cursor.fetchone()
            idempotent = current is not None and (
                current[0], current[1], str(current[2]), current[3]
            ) == (
                checkpoint.owner_epoch, checkpoint.committed_offset,
                str(checkpoint.checkpoint_id), checkpoint.state_sha256,
            )
            if current is not None and current[0] > checkpoint.owner_epoch:
                raise ValueError("stale owner epoch cannot advance detector checkpoint")
            if current is not None and current[1] > checkpoint.committed_offset:
                raise ValueError("detector checkpoint offset cannot move backward")
            if current is not None and current[1] == checkpoint.committed_offset and not idempotent:
                raise ValueError("detector checkpoint coordinate has conflicting state")
            cursor.execute(
                "INSERT INTO rt.consumer_checkpoints ("
                "consumer_name, cohort_id, journal_generation, shard_id, shard_map_sha256, "
                "owner_epoch, committed_offset, checkpoint_id, state_codec, state_hash, "
                "state_payload, committed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s) ON CONFLICT (consumer_name, cohort_id, "
                "journal_generation, shard_id) DO UPDATE SET "
                "shard_map_sha256=EXCLUDED.shard_map_sha256, "
                "owner_epoch=EXCLUDED.owner_epoch, committed_offset=EXCLUDED.committed_offset, "
                "checkpoint_id=EXCLUDED.checkpoint_id, state_codec=EXCLUDED.state_codec, "
                "state_hash=EXCLUDED.state_hash, state_payload=EXCLUDED.state_payload, "
                "committed_at=EXCLUDED.committed_at, updated_at=clock_timestamp() "
                "WHERE rt.consumer_checkpoints.owner_epoch <= EXCLUDED.owner_epoch "
                "AND rt.consumer_checkpoints.committed_offset < EXCLUDED.committed_offset "
                "RETURNING checkpoint_id",
                (
                    checkpoint.consumer_name, str(checkpoint.cohort_id),
                    str(checkpoint.journal_generation), checkpoint.shard_id,
                    checkpoint.shard_map_sha256, checkpoint.owner_epoch,
                    checkpoint.committed_offset, str(checkpoint.checkpoint_id),
                    checkpoint.state_codec, checkpoint.state_sha256,
                    checkpoint.state_payload, checkpoint.committed_at,
                ),
            )
            persisted = cursor.fetchone()
            if not idempotent and (
                persisted is None or str(persisted[0]) != str(checkpoint.checkpoint_id)
            ):
                raise ValueError("detector checkpoint advancement was rejected")
        if connection is not None:
            connection.commit()
    except Exception:
        if connection is not None:
            connection.rollback()
        raise


def persist_detector_evaluation_batch_cursor(
    cursor,
    *,
    lease_resource_key: str,
    owner_id: UUID,
    checkpoint: RealtimeDetectorCheckpoint,
    versions: Sequence[RealtimeDetectorVersion],
    states: Sequence[RealtimeDetectorState],
    events: Sequence[RealtimeDetectorEvent],
    alerts: Sequence[RealtimeAlertProjection] = (),
    values_executor=execute_values,
    batch_executor=execute_batch,
) -> None:
    _persist_detector_evaluation_batch(
        None,
        repository_cursor=cursor,
        lease_resource_key=lease_resource_key,
        owner_id=owner_id,
        checkpoint=checkpoint,
        versions=versions,
        states=states,
        events=events,
        alerts=alerts,
        values_executor=values_executor,
        batch_executor=batch_executor,
    )


def persist_detector_evaluation_batch(
    connection,
    *,
    lease_resource_key: str,
    owner_id: UUID,
    checkpoint: RealtimeDetectorCheckpoint,
    versions: Sequence[RealtimeDetectorVersion],
    states: Sequence[RealtimeDetectorState],
    events: Sequence[RealtimeDetectorEvent],
    alerts: Sequence[RealtimeAlertProjection] = (),
    values_executor=execute_values,
    batch_executor=execute_batch,
) -> None:
    _persist_detector_evaluation_batch(
        connection,
        repository_cursor=None,
        lease_resource_key=lease_resource_key,
        owner_id=owner_id,
        checkpoint=checkpoint,
        versions=versions,
        states=states,
        events=events,
        alerts=alerts,
        values_executor=values_executor,
        batch_executor=batch_executor,
    )