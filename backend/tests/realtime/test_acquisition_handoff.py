from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from realtime.acquisition_handoff import (
    DeveloperAcquisitionFactKind,
    DeveloperEquityBarAcquisitionFact,
    DeveloperOptionSnapshotAcquisitionFact,
    build_developer_acquisition_batch,
    developer_equity_bar_facts,
    developer_option_snapshots,
    load_developer_acquisition_batch,
    load_developer_acquisition_batch_cursor,
    persist_developer_acquisition_batch,
    persist_developer_acquisition_batch_cursor,
    persist_developer_equity_acquisition,
)
from realtime.acquisition_consumer import (
    DeveloperOptionPackageMaterialization,
    build_developer_acquisition_job,
    enqueue_pending_developer_acquisition_jobs,
    process_developer_acquisition_job,
)
from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.materialization_jobs import MATERIALIZATION_JOB_RETENTION_DAYS, ClaimedMaterializationJob
from realtime.preflight import ProviderTier
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_equity_bar_adapter import COHORT, NOW, _developer_revision
from tests.realtime.test_option_portal_adapter import (
    _developer_policy,
    _developer_snapshot,
    _package,
)


def _bar_batch():
    revision = _developer_revision()
    return build_developer_acquisition_batch(
        cohort_id=COHORT,
        provider="massive_developer",
        provider_batch_id=revision.ingestion_segment_id,
        started_at=NOW - timedelta(seconds=1),
        completed_at=NOW + timedelta(seconds=1),
        facts=(DeveloperEquityBarAcquisitionFact(1, revision),),
    )


def _option_batch():
    policy = _developer_policy()
    package = _package(policy.policy_sha256, ProviderTier.MASSIVE_DEVELOPER)
    batch_id = _developer_snapshot(
        package.legs[0],
        policy,
        batch_id=_developer_revision().ingestion_segment_id,
        mark=4,
        milliseconds=10,
    ).batch_id
    snapshots = tuple(
        _developer_snapshot(
            leg,
            policy,
            batch_id=batch_id,
            mark=4 - index,
            milliseconds=10 + index,
        )
        for index, leg in enumerate(package.legs)
    )
    return build_developer_acquisition_batch(
        cohort_id=COHORT,
        provider="polygon_developer",
        provider_batch_id=batch_id,
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=1),
        facts=tuple(
            DeveloperOptionSnapshotAcquisitionFact(leg.instrument_id, snapshot)
            for leg, snapshot in zip(package.legs, snapshots, strict=True)
        ),
    )


def test_equity_batch_preserves_lineage_and_uses_existing_repository():
    batch = _bar_batch()

    facts = developer_equity_bar_facts(batch)
    repository = MagicMock()
    connection = MagicMock()
    persist_developer_equity_acquisition(
        connection,
        batch,
        repository=repository,
    )

    assert batch.provider_tier is ProviderTier.MASSIVE_DEVELOPER
    assert batch.fact_kind is DeveloperAcquisitionFactKind.EQUITY_BAR
    assert facts[0].revision == _developer_revision()
    assert facts[0].revision.ingestion_segment_id == batch.provider_batch_id
    assert facts[0].revision.bar_revision_id == batch.facts[0].provider_revision_id
    repository.assert_called_once_with(connection, facts)


def test_option_batch_round_trip_preserves_all_snapshot_clocks_and_hashes():
    batch = _option_batch()

    snapshots = developer_option_snapshots(batch)

    assert batch.fact_kind is DeveloperAcquisitionFactKind.OPTION_SNAPSHOT
    assert len(snapshots) == 2
    for fact in batch.facts:
        snapshot = snapshots[fact.instrument_id]
        assert snapshot == fact.snapshot
        assert snapshot.batch_id == batch.provider_batch_id
        assert snapshot.snapshot_id == fact.provider_revision_id
        assert snapshot.market_data_time == fact.market_time
        assert (snapshot.revised_observed_at or snapshot.first_observed_at) == fact.observed_at
        assert snapshot.raw_payload_sha256 == fact.raw_payload_sha256
        assert snapshot.normalized_payload_sha256 == fact.normalized_payload_sha256


def _batch_row(batch, *, with_id=False, count=None):
    row = (
        batch.cohort_id,
        batch.provider_tier.value,
        batch.fact_kind.value,
        batch.fact_kind.schema_version,
        batch.provider,
        batch.provider_batch_id,
        batch.started_at,
        batch.completed_at,
        len(batch.facts) if count is None else count,
        batch.batch_payload_sha256,
    )
    return (batch.acquisition_batch_id, *row, batch.request_id) if with_id else row


def _persist_cursor(batch, *, new_batch=True):
    cursor = MagicMock()
    cursor.rowcount = 1 if new_batch else 0
    recorded_rows = []

    def values_executor(_cursor, statement, rows, *, page_size):
        assert "INSERT INTO rt.acquisition_facts" in statement
        assert "ON CONFLICT DO NOTHING" in statement
        assert page_size == 100
        recorded_rows.extend(tuple(rows))

    cursor.fetchone.return_value = _batch_row(batch)
    cursor.fetchall.side_effect = lambda: tuple(
        (*row[:-1], row[-1].adapted) for row in recorded_rows
    )
    return cursor, recorded_rows, values_executor


def test_completed_batch_and_facts_persist_atomically_and_idempotently():
    batch = _bar_batch()
    cursor, recorded_rows, values_executor = _persist_cursor(batch)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_developer_acquisition_batch(
        connection,
        batch,
        values_executor=values_executor,
    )

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0].startswith("INSERT INTO rt.acquisition_batches")
    assert "ON CONFLICT DO NOTHING" in statements[0]
    assert "developer_acquisition" not in " ".join(statements)
    assert "UPDATE rt." not in " ".join(statements)
    # The partition key (observed_at) travels with every fact row and bounds the read-back.
    assert recorded_rows[0][5] == batch.facts[0].observed_at
    facts_select = next(
        call for call in cursor.execute.call_args_list if "FROM rt.acquisition_facts" in call.args[0]
    )
    assert "observed_at BETWEEN %s AND %s" in facts_select.args[0]
    assert facts_select.args[1][1:] == (batch.facts[0].observed_at, batch.facts[0].observed_at)
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_replayed_batch_writes_no_facts_and_still_verifies_identity():
    batch = _bar_batch()
    cursor, recorded_rows, values_executor = _persist_cursor(batch, new_batch=False)
    cursor.fetchall.side_effect = lambda: tuple(
        (str(batch.acquisition_batch_id), index, fact.instrument_id, str(fact.provider_revision_id),
         fact.market_time, fact.observed_at, fact.raw_payload_sha256,
         fact.normalized_payload_sha256, fact.structured_fact())
        for index, fact in enumerate(batch.facts)
    )

    persist_developer_acquisition_batch_cursor(cursor, batch, values_executor=values_executor)

    assert recorded_rows == []

    cursor.fetchone.return_value = _batch_row(batch, count=2)
    with pytest.raises(ValueError, match="batch identity conflict"):
        persist_developer_acquisition_batch_cursor(cursor, batch, values_executor=values_executor)

    cursor.fetchone.return_value = _batch_row(batch)
    cursor.fetchall.side_effect = lambda: ()
    with pytest.raises(ValueError, match="fact identity conflict"):
        persist_developer_acquisition_batch_cursor(cursor, batch, values_executor=values_executor)


def test_request_id_is_stored_with_the_batch_without_changing_its_identity():
    plain = _bar_batch()
    linked = build_developer_acquisition_batch(
        cohort_id=plain.cohort_id,
        provider=plain.provider,
        provider_batch_id=plain.provider_batch_id,
        started_at=plain.started_at,
        completed_at=plain.completed_at,
        facts=plain.facts,
        request_id=REQUEST,
    )
    cursor, _, values_executor = _persist_cursor(linked)

    persist_developer_acquisition_batch_cursor(cursor, linked, values_executor=values_executor)

    assert linked.acquisition_batch_id == plain.acquisition_batch_id
    insert = cursor.execute.call_args_list[0]
    assert "request_id" in insert.args[0]
    assert str(REQUEST) in insert.args[1]


def test_cursor_level_handoff_persistence_does_not_own_transaction():
    batch = _bar_batch()
    cursor, _, values_executor = _persist_cursor(batch)

    persist_developer_acquisition_batch_cursor(
        cursor,
        batch,
        values_executor=values_executor,
    )

    assert cursor.execute.call_args_list[0].args[0].startswith(
        "INSERT INTO rt.acquisition_batches"
    )


def test_load_rejects_explicit_lineage_that_conflicts_with_structured_fact():
    batch = _bar_batch()
    fact = batch.facts[0]
    cursor = MagicMock()
    cursor.fetchone.return_value = _batch_row(batch, with_id=True, count=1)
    cursor.fetchall.return_value = (
        (
            0,
            fact.instrument_id,
            fact.provider_revision_id,
            fact.market_time + timedelta(minutes=1),
            fact.observed_at,
            fact.raw_payload_sha256,
            fact.normalized_payload_sha256,
            fact.structured_fact(),
        ),
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with pytest.raises(ValueError, match="explicit lineage conflicts"):
        load_developer_acquisition_batch(connection, batch.acquisition_batch_id)


@pytest.mark.parametrize("batch_factory", (_bar_batch, _option_batch))
def test_database_rows_round_trip_to_exact_domain_contracts(batch_factory):
    batch = batch_factory()
    cursor = MagicMock()
    cursor.fetchone.return_value = _batch_row(batch, with_id=True)
    cursor.fetchall.return_value = tuple(
        (
            index,
            fact.instrument_id,
            fact.provider_revision_id,
            fact.market_time,
            fact.observed_at,
            fact.raw_payload_sha256,
            fact.normalized_payload_sha256,
            fact.structured_fact(),
        )
        for index, fact in enumerate(batch.facts)
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    restored = load_developer_acquisition_batch_cursor(
        cursor,
        batch.acquisition_batch_id,
    )

    assert restored == batch


def test_batch_rejects_cross_batch_and_cross_tier_relabelling():
    batch = _bar_batch()
    fact = batch.facts[0]
    foreign_revision = _developer_revision()
    object.__setattr__(
        foreign_revision,
        "ingestion_segment_id",
        batch.acquisition_batch_id,
    )

    with pytest.raises(ValueError, match="cross provider batches"):
        build_developer_acquisition_batch(
            cohort_id=COHORT,
            provider=batch.provider,
            provider_batch_id=batch.provider_batch_id,
            started_at=batch.started_at,
            completed_at=batch.completed_at,
            facts=(fact, DeveloperEquityBarAcquisitionFact(2, foreign_revision)),
        )

    with pytest.raises(ValueError, match="cannot be relabelled"):
        type(batch)(
            acquisition_batch_id=batch.acquisition_batch_id,
            cohort_id=batch.cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            fact_kind=batch.fact_kind,
            provider=batch.provider,
            provider_batch_id=batch.provider_batch_id,
            started_at=batch.started_at,
            completed_at=batch.completed_at,
            facts=batch.facts,
            batch_payload_sha256=batch.batch_payload_sha256,
        )


def test_developer_bar_fact_accepts_stable_derived_lineage_only():
    native = _developer_revision()
    derived = replace(
        native,
        source_kind=type(native.source_kind).DERIVED,
        source_bar_revision_ids=(native.bar_revision_id,),
    )

    fact = DeveloperEquityBarAcquisitionFact(1, derived)

    assert fact.revision.source_kind.value == "DERIVED"
    with pytest.raises(ValueError, match="lineage-bound derived bar"):
        DeveloperEquityBarAcquisitionFact(
            1,
            replace(derived, source_bar_revision_ids=()),
        )

OWNER = UUID("12121212-1212-1212-1212-121212121212")
REQUEST = UUID("34343434-3434-3434-3434-343434343434")
OPTION_MARK = UUID("23232323-2323-2323-2323-232323232323")


def _compute_database(*, role=RealtimeDatabaseRole.COMPUTE):
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    )
    cursor.rowcount = 1
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    plan = RealtimeProcessDatabasePlan(
        "worker:developer_acquisition_consumer:active",
        role,
        RealtimePoolBudget(1, 1),
    )
    database = RealtimeProcessDatabase.wire(
        plan,
        principal_pool=pool,
        role_name=f"stocks_realtime_{role.value.lower()}",
    )
    cursor.reset_mock()
    connection.reset_mock()
    pool.reset_mock()
    cursor.fetchone.side_effect = None
    cursor.rowcount = 1
    connection.closed = False
    return database, connection, cursor


def _claimed(batch, *, lease_epoch=1):
    return ClaimedMaterializationJob(
        build_developer_acquisition_job(batch),
        OWNER,
        lease_epoch,
        batch.completed_at + timedelta(minutes=1),
        1,
    )


def test_compute_discovers_completed_batches_in_bounded_order(monkeypatch):
    batch = _bar_batch()
    database, connection, cursor = _compute_database()
    cursor.fetchall.return_value = ((batch.acquisition_batch_id,),)
    enqueued = []
    monkeypatch.setattr(
        "realtime.acquisition_consumer.load_developer_acquisition_batch_cursor",
        lambda active_cursor, batch_id: batch,
    )

    def enqueue(active_cursor, job):
        assert active_cursor is cursor
        enqueued.append(job)
        return job.job_id

    monkeypatch.setattr(
        "realtime.acquisition_consumer.enqueue_materialization_job_cursor",
        enqueue,
    )

    job_ids = enqueue_pending_developer_acquisition_jobs(
        database,
        cohort_id=batch.cohort_id,
        fact_kind=batch.fact_kind,
        limit=10,
    )

    discovery_sql = next(
        call.args[0]
        for call in cursor.execute.call_args_list
        if "FROM rt.acquisition_batches" in call.args[0]
    )
    discovery_parameters = next(
        call.args[1]
        for call in cursor.execute.call_args_list
        if "FROM rt.acquisition_batches" in call.args[0]
    )
    assert "NOT EXISTS" in discovery_sql
    assert "ORDER BY batch.completed_at, batch.acquisition_batch_id LIMIT %s" in discovery_sql
    # Completed jobs are pruned after 7 days, so discovery never looks further back.
    assert "batch.completed_at >= clock_timestamp()" in discovery_sql
    assert MATERIALIZATION_JOB_RETENTION_DAYS in discovery_parameters
    assert "developer_acquisition" not in discovery_sql
    assert job_ids == (enqueued[0].job_id,)
    assert enqueued[0] == build_developer_acquisition_job(batch)
    assert connection.commit.call_count == 1


def test_a_retried_job_still_matches_its_batch_because_scheduling_is_not_identity():
    from realtime.acquisition_consumer import developer_job_matches_batch

    batch = _bar_batch()
    job = build_developer_acquisition_job(batch)
    retried = replace(job, available_at=job.available_at + timedelta(seconds=30))
    assert developer_job_matches_batch(retried, batch)
    assert not developer_job_matches_batch(replace(job, closure_sha256="0" * 64), batch)
    assert not developer_job_matches_batch(replace(job, source_revision_id=uuid4()), batch)
    assert not developer_job_matches_batch(replace(job, cohort_id=uuid4()), batch)


def test_equity_job_output_and_completion_share_compute_transaction(monkeypatch):
    batch = _bar_batch()
    claimed = _claimed(batch)
    database, connection, cursor = _compute_database()
    cursor.fetchone.return_value = (OWNER, claimed.lease_epoch, True)
    monkeypatch.setattr(
        "realtime.acquisition_consumer.load_developer_acquisition_batch_cursor",
        lambda active_cursor, batch_id: batch,
    )
    operation_order = []

    def execute(statement, parameters=None):
        operation_order.append(statement)

    cursor.execute.side_effect = execute

    def persist_bars(active_cursor, facts):
        assert active_cursor is cursor
        operation_order.append("PERSIST_BARS")
        assert connection.commit.call_count == 0

    output_ids = process_developer_acquisition_job(
        database,
        claimed,
        bar_repository=persist_bars,
    )

    completion_index = next(
        index for index, statement in enumerate(operation_order)
        if isinstance(statement, str)
        and statement.startswith("UPDATE rt.materialization_jobs SET status='COMPLETE'")
    )
    assert operation_order.index("PERSIST_BARS") < completion_index
    assert output_ids == tuple(
        fact.provider_revision_id for fact in batch.facts
    )
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_option_job_uses_existing_adapter_and_repository_atomically(monkeypatch):
    batch = _option_batch()
    claimed = _claimed(batch)
    database, connection, cursor = _compute_database()
    cursor.fetchone.return_value = (OWNER, claimed.lease_epoch, True)
    monkeypatch.setattr(
        "realtime.acquisition_consumer.load_developer_acquisition_batch_cursor",
        lambda active_cursor, batch_id: batch,
    )
    policy = _developer_policy()
    package = replace(
        _package(policy.policy_sha256, ProviderTier.MASSIVE_DEVELOPER),
        cohort_id=batch.cohort_id,
    )
    persisted = []

    def persist_option(active_cursor, result):
        assert active_cursor is cursor
        assert result.status.value == "READY"
        persisted.append(result)
        return OPTION_MARK

    output_ids = process_developer_acquisition_job(
        database,
        claimed,
        option_materializations=(
            DeveloperOptionPackageMaterialization(
                package,
                policy,
                batch.completed_at,
            ),
        ),
        option_repository=persist_option,
    )

    assert output_ids == (OPTION_MARK,)
    assert len(persisted) == 1
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_output_failure_rolls_back_job_completion_for_retry(monkeypatch):
    batch = _bar_batch()
    claimed = _claimed(batch)
    database, connection, cursor = _compute_database()
    cursor.fetchone.return_value = (OWNER, claimed.lease_epoch, True)
    monkeypatch.setattr(
        "realtime.acquisition_consumer.load_developer_acquisition_batch_cursor",
        lambda active_cursor, batch_id: batch,
    )

    with pytest.raises(RuntimeError, match="forced output failure"):
        process_developer_acquisition_job(
            database,
            claimed,
            bar_repository=MagicMock(
                side_effect=RuntimeError("forced output failure")
            ),
        )

    completion_updates = [
        call for call in cursor.execute.call_args_list
        if isinstance(call.args[0], str)
        and call.args[0].startswith("UPDATE rt.materialization_jobs SET status='COMPLETE'")
    ]
    assert completion_updates == []
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_consumer_rejects_non_compute_role_before_database_use():
    batch = _bar_batch()
    database, connection, _ = _compute_database(role=RealtimeDatabaseRole.INGRESS)

    with pytest.raises(ValueError, match="requires the compute database role"):
        process_developer_acquisition_job(database, _claimed(batch))

    assert connection.cursor.call_count == 0


def _linked_bar_batch():
    plain = _bar_batch()
    return build_developer_acquisition_batch(
        cohort_id=plain.cohort_id,
        provider=plain.provider,
        provider_batch_id=plain.provider_batch_id,
        started_at=plain.started_at,
        completed_at=plain.completed_at,
        facts=plain.facts,
        request_id=REQUEST,
    )


@pytest.mark.parametrize(
    ("ledger_row", "message"),
    (
        (None, "not recorded in the ledger"),
        (("FAILED", "TIMEOUT", "NOT_APPLICABLE"), "did not complete successfully"),
        (("RESERVED", None, "NOT_APPLICABLE"), "did not complete successfully"),
        (("COMPLETE", "OK", "INCOMPLETE"), "incomplete and cannot be a source"),
    ),
)
def test_consumer_rejects_a_batch_whose_provider_request_was_not_fully_read(
    monkeypatch, ledger_row, message,
):
    batch = _linked_bar_batch()
    claimed = _claimed(batch)
    database, connection, cursor = _compute_database()
    cursor.fetchone.return_value = ledger_row
    monkeypatch.setattr(
        "realtime.acquisition_consumer.load_developer_acquisition_batch_cursor",
        lambda active_cursor, batch_id: batch,
    )
    persist_bars = MagicMock()

    with pytest.raises(ValueError, match=message):
        process_developer_acquisition_job(database, claimed, bar_repository=persist_bars)

    persist_bars.assert_not_called()
    assert "rt.provider_request_ledger" in cursor.execute.call_args.args[0]
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_consumer_accepts_a_batch_whose_provider_request_completed_fully(monkeypatch):
    batch = _linked_bar_batch()
    claimed = _claimed(batch)
    database, connection, cursor = _compute_database()
    cursor.fetchone.side_effect = (
        ("COMPLETE", "OK", "COMPLETE"),
        (OWNER, claimed.lease_epoch, True),
    )
    monkeypatch.setattr(
        "realtime.acquisition_consumer.load_developer_acquisition_batch_cursor",
        lambda active_cursor, batch_id: batch,
    )
    persist_bars = MagicMock()

    output_ids = process_developer_acquisition_job(database, claimed, bar_repository=persist_bars)

    persist_bars.assert_called_once()
    assert output_ids == tuple(fact.provider_revision_id for fact in batch.facts)
    assert connection.commit.call_count == 1