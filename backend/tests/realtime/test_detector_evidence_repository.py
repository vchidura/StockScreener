import json
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.contracts import canonical_sha256
from realtime.detector_adapters import StableDetectorAdapterResult
from realtime.detector_evidence import (
    RealtimeDetectorInput,
    RealtimeDetectorState,
    build_detector_checkpoint,
    build_detector_event,
    build_detector_version,
)
from realtime.detector_evidence_repository import (
    persist_detector_evaluation_batch,
    persist_detector_evaluation_batch_cursor,
)
from realtime.portal_projection import build_alert_projection
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, tzinfo=UTC)
OWNER = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
COHORT = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
GENERATION = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
SESSION = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
SOURCE = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")


def _batch():
    version = build_detector_version(
        detector_id="O1",
        version="option_participation_origin_v1",
        code_sha256="a" * 64,
        policy_sha256="b" * 64,
        requirements={"inputs": ["ACTIVITY", "OPEN_INTEREST"]},
        output_kind="OBSERVATION",
    )
    adapter = StableDetectorAdapterResult(
        adapter_id="options-o1-participation",
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        input_sha256="c" * 64,
        status="EXECUTED",
        output={"disposition": "DETECTED"},
    )
    input_row = RealtimeDetectorInput(
        input_role="ACTIVITY",
        input_key="contract:1",
        source_kind="JOURNAL_EVENT",
        input_sha256="d" * 64,
        available_at=NOW,
        received_at=NOW,
        retained_input={"contract_id": 1, "day_volume": 500},
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        source_offset=5,
    )
    event = build_detector_event(
        adapter,
        version,
        instrument_id=1,
        trigger_identity="O1:contract:1:2026-10-05",
        trigger_at=NOW,
        decision_at=NOW,
        valid_until=datetime(2026, 10, 5, 15, 30, tzinfo=UTC),
        disposition="DETECTED",
        direction=None,
        reason_codes=(),
        readiness_epoch=3,
        payload_schema="option_participation_finding_v1",
        payload={"disposition": "DETECTED"},
        inputs=(input_row,),
    )
    state_payload = {"episodes": [str(event.event_id)]}
    state = RealtimeDetectorState(
        detector_sha256=version.detector_sha256,
        cohort_id=COHORT,
        state_key="O1:contract:1",
        revision=1,
        state_schema="o1_episode_state_v1",
        state_sha256=canonical_sha256(state_payload),
        state_payload=state_payload,
        updated_at=NOW,
    )
    checkpoint = build_detector_checkpoint(
        {"states": {state.state_key: state.state_sha256}},
        consumer_name="detector-runner:0",
        cohort_id=COHORT,
        journal_generation=GENERATION,
        shard_id=0,
        shard_map_sha256="e" * 64,
        owner_epoch=7,
        committed_offset=5,
        committed_at=NOW,
    )
    return version, state, event, checkpoint


def _values_executor(cursor, statement, rows, *, page_size):
    assert page_size == 100
    cursor.operation_order.append(statement)
    cursor.value_batches.append((statement, tuple(rows)))


def _batch_executor(cursor, statement, rows, *, page_size):
    assert page_size == 100
    cursor.operation_order.append(statement)
    cursor.state_statement = statement
    cursor.state_rows = tuple(rows)


def _connection(version, state, event, checkpoint, *, active=True):
    cursor = MagicMock()
    cursor.value_batches = []
    cursor.operation_order = []
    cursor.execute.side_effect = lambda statement, parameters: cursor.operation_order.append(statement)
    cursor.fetchone.side_effect = (
        (str(OWNER), checkpoint.owner_epoch, active),
        None,
        (str(checkpoint.checkpoint_id),),
    )

    def fetchall():
        statement = cursor.execute.call_args.args[0]
        if "FROM rt.detector_versions" in statement:
            return ((
                version.detector_sha256, version.detector_id, version.version,
                version.code_sha256, version.policy_sha256, version.valuation_sha256,
            ),)
        if "FROM rt.detector_state" in statement:
            return ((
                state.detector_sha256, state.cohort_id, state.state_key,
                state.revision, state.state_schema, state.state_sha256,
            ),)
        if "FROM rt.detector_events" in statement:
            return ((event.event_id, event.dedup_sha256, event.evidence_sha256),)
        raise AssertionError(statement)

    cursor.fetchall.side_effect = fetchall
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


def test_detector_state_event_inputs_and_checkpoint_commit_atomically():
    version, state, event, checkpoint = _batch()
    connection, cursor = _connection(version, state, event, checkpoint)

    persist_detector_evaluation_batch(
        connection,
        lease_resource_key="detector:0",
        owner_id=OWNER,
        checkpoint=checkpoint,
        versions=(version,),
        states=(state,),
        events=(event,),
        values_executor=_values_executor,
        batch_executor=_batch_executor,
    )

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    checkpoint_index = next(
        index for index, statement in enumerate(statements)
        if statement.startswith("INSERT INTO rt.consumer_checkpoints")
    )
    event_select_index = next(
        index for index, statement in enumerate(statements)
        if "FROM rt.detector_events" in statement
    )
    assert event_select_index < checkpoint_index
    assert "rt.detector_state.revision < EXCLUDED.revision" in cursor.state_statement
    input_batch = next(
        rows for statement, rows in cursor.value_batches
        if "INSERT INTO rt.detector_event_inputs" in statement
    )
    assert input_batch[0][8:11] == (str(GENERATION), str(SESSION), 5)
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_expired_detector_lease_rolls_back_before_any_evidence_write():
    version, state, event, checkpoint = _batch()
    connection, cursor = _connection(
        version,
        state,
        event,
        checkpoint,
        active=False,
    )

    with pytest.raises(ValueError, match="stale or expired"):
        persist_detector_evaluation_batch(
            connection,
            lease_resource_key="detector:0",
            owner_id=OWNER,
            checkpoint=checkpoint,
            versions=(version,),
            states=(state,),
            events=(event,),
            values_executor=_values_executor,
            batch_executor=_batch_executor,
        )

    assert cursor.value_batches == []
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_alert_current_and_outbox_are_in_detector_checkpoint_transaction():
    version, state, event, checkpoint = _batch()
    alert = build_alert_projection(
        event,
        alert_kind="OPTION",
        lifecycle="OPEN",
        projection_schema="option_alert_v1",
        projection={"alert_id": "option-1", "underlyer": "SPY"},
        destinations=("PORTAL",),
    )
    connection, cursor = _connection(version, state, event, checkpoint)

    persist_detector_evaluation_batch(
        connection,
        lease_resource_key="detector:0",
        owner_id=OWNER,
        checkpoint=checkpoint,
        versions=(version,),
        states=(state,),
        events=(event,),
        alerts=(alert,),
        values_executor=_values_executor,
        batch_executor=_batch_executor,
    )

    value_statements = [statement for statement, _ in cursor.value_batches]
    assert any("INSERT INTO rt.alert_current" in row for row in value_statements)
    assert any("INSERT INTO rt.alert_outbox" in row for row in value_statements)
    outbox_index = max(
        index for index, statement in enumerate(cursor.operation_order)
        if "INSERT INTO rt.alert_outbox" in statement
    )
    checkpoint_index = next(
        index for index, statement in enumerate(cursor.operation_order)
        if "INSERT INTO rt.consumer_checkpoints" in statement
    )
    assert outbox_index < checkpoint_index
    assert connection.commit.call_count == 1


def test_detector_input_cannot_exceed_checkpoint_boundary():
    version, state, event, checkpoint = _batch()
    input_row = event.inputs[0]
    invalid = type(input_row)(
        input_role=input_row.input_role,
        input_key=input_row.input_key,
        source_kind=input_row.source_kind,
        input_sha256=input_row.input_sha256,
        available_at=input_row.available_at,
        received_at=input_row.received_at,
        retained_input=input_row.retained_input,
        journal_generation=input_row.journal_generation,
        stream_session_id=input_row.stream_session_id,
        source_offset=checkpoint.committed_offset + 1,
    )
    event = type(event)(
        **{field: getattr(event, field) for field in event.__dataclass_fields__ if field != "inputs"},
        inputs=(invalid,),
    )

    with pytest.raises(ValueError, match="exceeds checkpoint"):
        persist_detector_evaluation_batch(
            MagicMock(),
            lease_resource_key="detector:0",
            owner_id=OWNER,
            checkpoint=checkpoint,
            versions=(version,),
            states=(state,),
            events=(event,),
        )


def test_detector_evidence_cursor_api_leaves_transaction_to_worker():
    version, state, event, checkpoint = _batch()
    connection, cursor = _connection(version, state, event, checkpoint)

    persist_detector_evaluation_batch_cursor(
        cursor,
        lease_resource_key="detector:0",
        owner_id=OWNER,
        checkpoint=checkpoint,
        versions=(version,),
        states=(state,),
        events=(event,),
        values_executor=_values_executor,
        batch_executor=_batch_executor,
    )

    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()


SNAPSHOT = UUID("11111111-0000-0000-0000-000000000001")
BAR = UUID("11111111-0000-0000-0000-000000000002")
MARK = UUID("11111111-0000-0000-0000-000000000003")


def _soft_reference_event(event, **retained):
    def input_row(role, key, kind, **reference):
        return RealtimeDetectorInput(
            input_role=role, input_key=key, source_kind=kind, input_sha256="d" * 64,
            available_at=NOW, received_at=NOW, retained_input=retained or {"value": 1}, **reference,
        )

    inputs = (
        input_row("FEATURES", "snapshot", "FEATURE_SNAPSHOT", feature_snapshot_id=SNAPSHOT),
        input_row("BAR", "bar", "BAR_REVISION", bar_revision_id=BAR),
        input_row("MARK", "mark", "OPTION_MARK", option_mark_evidence_id=MARK),
    )
    return type(event)(
        **{field: getattr(event, field) for field in event.__dataclass_fields__ if field != "inputs"},
        inputs=inputs,
    )


def _reference_connection(version, state, event, checkpoint, *, available):
    connection, cursor = _connection(version, state, event, checkpoint)
    delegate = cursor.fetchall.side_effect
    tables = {
        "FROM rt.feature_snapshots": (SNAPSHOT, "snapshot_id"),
        "FROM rt.bar_catalog": (BAR, "bar_revision_id"),
        "FROM rt.option_mark_evidence": (MARK, "evidence_id"),
    }

    def fetchall():
        statement = cursor.execute.call_args.args[0]
        for fragment, (identity, _) in tables.items():
            if fragment in statement:
                assert statement.count("ANY(%s::uuid[])") == 1
                return ((identity,),) if identity in available else ()
        return delegate()

    cursor.fetchall.side_effect = fetchall
    return connection, cursor


def test_event_inputs_keep_soft_references_after_verifying_them_in_the_writing_transaction():
    version, state, event, checkpoint = _batch()
    event = _soft_reference_event(event)
    connection, cursor = _reference_connection(
        version, state, event, checkpoint, available={SNAPSHOT, BAR, MARK},
    )

    persist_detector_evaluation_batch(
        connection, lease_resource_key="detector:0", owner_id=OWNER, checkpoint=checkpoint,
        versions=(version,), states=(state,), events=(event,),
        values_executor=_values_executor, batch_executor=_batch_executor,
    )

    order = cursor.operation_order
    check_indexes = [
        index for index, s in enumerate(order)
        if "FROM rt.feature_snapshots" in s or "FROM rt.bar_catalog" in s
        or "FROM rt.option_mark_evidence" in s
    ]
    assert len(check_indexes) == 3
    assert not any("FROM rt.bar_revisions" in s for s in order)
    assert max(check_indexes) < next(
        index for index, s in enumerate(order) if "INSERT INTO rt.detector_event_inputs" in s
    )
    input_rows = next(
        rows for statement, rows in cursor.value_batches if "INSERT INTO rt.detector_event_inputs" in statement
    )
    assert {row[0:3] for row in input_rows} == {
        (str(event.event_id), "FEATURES", "snapshot"),
        (str(event.event_id), "BAR", "bar"),
        (str(event.event_id), "MARK", "mark"),
    }
    assert all(json.loads(row[-1]) == {"value": 1} for row in input_rows)
    assert connection.commit.call_count == 1


@pytest.mark.parametrize("missing", (SNAPSHOT, BAR, MARK))
def test_an_unavailable_soft_reference_rolls_back_the_whole_detector_batch(missing):
    version, state, event, checkpoint = _batch()
    event = _soft_reference_event(event)
    connection, cursor = _reference_connection(
        version, state, event, checkpoint, available={SNAPSHOT, BAR, MARK} - {missing},
    )

    with pytest.raises(ValueError, match="is not available"):
        persist_detector_evaluation_batch(
            connection, lease_resource_key="detector:0", owner_id=OWNER, checkpoint=checkpoint,
            versions=(version,), states=(state,), events=(event,),
            values_executor=_values_executor, batch_executor=_batch_executor,
        )

    assert not any("INSERT INTO rt.detector_event_inputs" in s for s in cursor.operation_order)
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_event_inputs_must_be_retained_by_value():
    version, state, event, checkpoint = _batch()
    empty = type(event.inputs[0])(
        input_role="ACTIVITY", input_key="contract:1", source_kind="JOURNAL_EVENT",
        input_sha256="d" * 64, available_at=NOW, received_at=NOW, retained_input={},
        journal_generation=GENERATION, stream_session_id=SESSION, source_offset=5,
    )
    event = type(event)(
        **{field: getattr(event, field) for field in event.__dataclass_fields__ if field != "inputs"},
        inputs=(empty,),
    )
    connection, cursor = _connection(version, state, event, checkpoint)

    with pytest.raises(ValueError, match="retained by value"):
        persist_detector_evaluation_batch(
            connection, lease_resource_key="detector:0", owner_id=OWNER, checkpoint=checkpoint,
            versions=(version,), states=(state,), events=(event,),
            values_executor=_values_executor, batch_executor=_batch_executor,
        )

    assert cursor.execute.call_count == 0