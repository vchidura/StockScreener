from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime.acquisition_handoff import (
    DeveloperAcquisitionFactKind,
    DeveloperEquityBarAcquisitionFact,
    DeveloperOptionSnapshotAcquisitionFact,
    build_developer_acquisition_batch,
)
from realtime.acquisition_ledger import (
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RecordedProviderRequest,
    RequestCompleteness,
)
from realtime.acquisition_producer import (
    DEVELOPER_ACQUISITION_OVERLAP_POLICY,
    CompletedDeveloperSourceBatch,
    DeveloperAcquisitionProducer,
    DeveloperAcquisitionRequest,
    DeveloperSourceResult,
    developer_equity_watermarks,
)
from realtime.contracts import CANARY_UNIVERSE
from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_equity_bar_adapter import COHORT, NOW, _developer_revision
from tests.realtime.test_option_portal_adapter import (
    _developer_policy,
    _developer_snapshot,
    _package,
)


class FakeSource:
    def __init__(self, fact_kind, result):
        self.fact_kind = fact_kind
        self.result = result
        self.requests = []

    def collect(self, request):
        self.requests.append(request)
        return self.result


def _facts():
    equity_batch_id = UUID("56565656-5656-5656-5656-565656565656")
    option_batch_id = UUID("67676767-6767-6767-6767-676767676767")
    policy = _developer_policy()
    template_package = _package(policy.policy_sha256)
    template_snapshot = _developer_snapshot(
        template_package.legs[0],
        policy,
        batch_id=option_batch_id,
        mark="4.00",
        milliseconds=10,
    )
    equity_facts = []
    option_facts = []
    for index, ticker in enumerate(CANARY_UNIVERSE.equity_tickers, start=1):
        revision = replace(
            _developer_revision(),
            bar_revision_id=uuid5(NAMESPACE_URL, f"bar:{ticker}"),
            security_id=uuid5(NAMESPACE_URL, f"security:{ticker}"),
            ticker=ticker,
            ingestion_segment_id=equity_batch_id,
            payload_sha256=f"{index:064x}",
        )
        equity_facts.append(DeveloperEquityBarAcquisitionFact(index, revision))
        snapshot = replace(
            template_snapshot,
            snapshot_id=uuid5(NAMESPACE_URL, f"option:{ticker}"),
            contract_id=1_000 + index,
            contract_ticker=f"O:{ticker}261120C00100000",
            underlyer=ticker,
            batch_id=option_batch_id,
            raw_payload_sha256=f"{index + 100:064x}",
            normalized_payload_sha256=f"{index + 200:064x}",
        )
        option_facts.append(
            DeveloperOptionSnapshotAcquisitionFact(100 + index, snapshot)
        )
    return equity_batch_id, option_batch_id, tuple(equity_facts), tuple(option_facts)


def _sources(*, omit_equity=None, add_equity=None):
    equity_batch_id, option_batch_id, equity_facts, option_facts = _facts()
    if omit_equity is not None:
        equity_facts = tuple(
            fact for fact in equity_facts if fact.revision.ticker != omit_equity
        )
    if add_equity is not None:
        template = equity_facts[0]
        equity_facts = (*equity_facts, DeveloperEquityBarAcquisitionFact(
            999,
            replace(
                template.revision,
                bar_revision_id=uuid5(NAMESPACE_URL, f"bar:{add_equity}"),
                security_id=uuid5(NAMESPACE_URL, f"security:{add_equity}"),
                ticker=add_equity,
            ),
        ))
    equity = FakeSource(
        DeveloperAcquisitionFactKind.EQUITY_BAR,
        DeveloperSourceResult(
            "massive_developer",
            (CompletedDeveloperSourceBatch(
                DeveloperAcquisitionFactKind.EQUITY_BAR,
                equity_batch_id,
                NOW - timedelta(seconds=1),
                NOW + timedelta(seconds=1),
                equity_facts,
            ),),
            0,
            8_000,
        ),
    )
    options = FakeSource(
        DeveloperAcquisitionFactKind.OPTION_SNAPSHOT,
        DeveloperSourceResult(
            "polygon_developer",
            (CompletedDeveloperSourceBatch(
                DeveloperAcquisitionFactKind.OPTION_SNAPSHOT,
                option_batch_id,
                NOW,
                NOW + timedelta(seconds=1),
                option_facts,
            ),),
            0,
            16_000,
        ),
    )
    return equity, options


def _database(role=RealtimeDatabaseRole.INGRESS):
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    )
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    plan = RealtimeProcessDatabasePlan(
        "worker:developer_acquisition_producer:canary",
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
    connection.closed = False
    return database, connection, cursor


def _request():
    return DeveloperAcquisitionRequest(COHORT, CANARY_UNIVERSE, NOW)


def test_fake_sources_persist_exact_canary_batches_in_one_ingress_transaction():
    equity, options = _sources()
    producer = DeveloperAcquisitionProducer(equity, options)
    database, connection, cursor = _database()
    persisted = []

    def repository(active_cursor, batch):
        assert active_cursor is cursor
        persisted.append(batch)
        assert connection.commit.call_count == 0

    result = producer.run_once(database, _request(), repository=repository)

    assert equity.requests == options.requests == [_request()]
    assert {fact.revision.ticker for fact in persisted[0].facts} == set(
        CANARY_UNIVERSE.equity_tickers
    )
    assert {fact.snapshot.underlyer for fact in persisted[1].facts} == set(
        CANARY_UNIVERSE.option_underlyings
    )
    assert result.equity_fact_count == result.option_fact_count == 8
    assert result.provider_requests == 0
    assert result.response_bytes == 24_000
    assert len(result.acquisition_batch_ids) == len(persisted) == 2
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"omit_equity": "AAPL"}, "exact canary"),
        ({"add_equity": "META"}, "exact canary"),
    ),
)
def test_producer_rejects_missing_or_unreviewed_ticker_before_persistence(
    changes,
    message,
):
    equity, options = _sources(**changes)
    producer = DeveloperAcquisitionProducer(equity, options)
    database, connection, cursor = _database()

    with pytest.raises(ValueError, match=message):
        producer.run_once(database, _request())

    assert cursor.execute.call_count == 0
    assert connection.commit.call_count == 0


def test_producer_rejects_non_ingress_role_before_collecting():
    equity, options = _sources()
    producer = DeveloperAcquisitionProducer(equity, options)
    database, connection, _ = _database(RealtimeDatabaseRole.COMPUTE)

    with pytest.raises(ValueError, match="requires the ingress database role"):
        producer.run_once(database, _request())

    assert equity.requests == options.requests == []
    assert connection.cursor.call_count == 0


def test_repository_failure_rolls_back_both_source_batches():
    equity, options = _sources()
    producer = DeveloperAcquisitionProducer(equity, options)
    database, connection, _ = _database()
    repository = MagicMock(side_effect=RuntimeError("forced handoff failure"))

    with pytest.raises(RuntimeError, match="forced handoff failure"):
        producer.run_once(database, _request(), repository=repository)

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_producer_rejects_source_counters_beyond_canary_bounds():
    equity, options = _sources()
    equity.result = replace(equity.result, provider_requests=89)
    producer = DeveloperAcquisitionProducer(equity, options)
    database, connection, cursor = _database()

    with pytest.raises(ValueError, match="request bound"):
        producer.run_once(database, _request())

    assert cursor.execute.call_count == 0
    assert connection.commit.call_count == 0


REQUEST_COMPLETE = UUID("78787878-7878-7878-7878-787878787878")
REQUEST_PARTIAL = UUID("89898989-8989-8989-8989-898989898989")


def _recorded(request_id, *, complete):
    reservation = ProviderRequestReservation(
        request_id=request_id, account_alias="developer", lane=ProviderRequestLane.EQUITY_HISTORY,
        endpoint_class="aggs", request_sha256="e" * 64, reserved_at=NOW, cohort_id=COHORT,
    )
    completion = ProviderRequestCompletion(
        state=ProviderRequestState.COMPLETE, completed_at=NOW + timedelta(seconds=1),
        outcome=ProviderRequestOutcome.OK, pages_fetched=1,
        completeness=(
            RequestCompleteness.COMPLETE if complete else RequestCompleteness.INCOMPLETE
        ),
        failure_code=None if complete else "PAGINATION_NOT_EXHAUSTED",
    )
    return RecordedProviderRequest(reservation, completion)


def _linked_sources(*, equity_complete):
    equity, options = _sources()
    equity_result = equity.result
    option_result = options.result
    equity.result = replace(
        equity_result,
        batches=(replace(equity_result.batches[0], request_id=REQUEST_COMPLETE),),
        requests=(_recorded(REQUEST_COMPLETE, complete=equity_complete),),
    )
    options.result = replace(
        option_result,
        batches=(replace(option_result.batches[0], request_id=REQUEST_PARTIAL),),
        requests=(_recorded(REQUEST_PARTIAL, complete=False),),
    )
    return equity, options


def test_requests_are_recorded_before_batches_and_only_complete_reads_advance_watermarks():
    equity, options = _linked_sources(equity_complete=True)
    producer = DeveloperAcquisitionProducer(equity, options)
    database, connection, cursor = _database()
    order = []
    watermarks = []
    batches = []

    def request_repository(active_cursor, reservation, completion):
        assert active_cursor is cursor
        order.append(("request", reservation.request_id, connection.commit.call_count))

    def repository(active_cursor, batch):
        order.append(("batch", batch.request_id, connection.commit.call_count))
        batches.append(batch)

    result = producer.run_once(
        database, _request(), repository=repository, request_repository=request_repository,
        watermark_repository=lambda active_cursor, watermark: watermarks.append(watermark),
    )

    assert [entry[:2] for entry in order] == [
        ("request", REQUEST_COMPLETE), ("request", REQUEST_PARTIAL),
        ("batch", REQUEST_COMPLETE), ("batch", REQUEST_PARTIAL),
    ]
    # The ledger transaction commits first, so a failed handoff keeps the pacing record.
    assert [entry[2] for entry in order] == [0, 0, 1, 1]
    assert connection.commit.call_count == 2
    assert len(result.acquisition_batch_ids) == 2
    equity_batch = batches[0]
    expected = {
        (fact.instrument_id, fact.revision.interval, fact.revision.source_kind.value): fact.revision.bar_end
        for fact in equity_batch.facts
    }
    assert {
        (item.instrument_id, item.interval, item.source_kind): item.last_complete_bar_end
        for item in watermarks
    } == expected
    assert {item.request_id for item in watermarks} == {REQUEST_COMPLETE}
    assert {item.overlap_policy for item in watermarks} == {DEVELOPER_ACQUISITION_OVERLAP_POLICY}


def test_an_incomplete_equity_read_never_advances_a_watermark():
    equity, options = _linked_sources(equity_complete=False)
    producer = DeveloperAcquisitionProducer(equity, options)
    database, _, _ = _database()
    watermarks = MagicMock()

    producer.run_once(
        database, _request(), repository=MagicMock(), request_repository=MagicMock(),
        watermark_repository=watermarks,
    )

    watermarks.assert_not_called()


def test_a_failed_batch_handoff_keeps_the_already_committed_request_record():
    equity, options = _linked_sources(equity_complete=True)
    producer = DeveloperAcquisitionProducer(equity, options)
    database, connection, _ = _database()

    with pytest.raises(RuntimeError, match="forced handoff failure"):
        producer.run_once(
            database, _request(), repository=MagicMock(side_effect=RuntimeError("forced handoff failure")),
            request_repository=MagicMock(), watermark_repository=MagicMock(),
        )

    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 1


def test_source_result_rejects_a_repeated_request_record():
    equity, _ = _linked_sources(equity_complete=True)
    with pytest.raises(ValueError, match="repeats a provider request"):
        replace(equity.result, requests=equity.result.requests * 2)


def test_equity_watermarks_group_by_instrument_interval_and_source():
    equity, _ = _linked_sources(equity_complete=True)
    batch = build_developer_acquisition_batch(
        cohort_id=COHORT, provider="massive_developer",
        provider_batch_id=equity.result.batches[0].provider_batch_id,
        started_at=equity.result.batches[0].started_at,
        completed_at=equity.result.batches[0].completed_at,
        facts=equity.result.batches[0].facts,
    )

    watermarks = developer_equity_watermarks(batch, request_id=REQUEST_COMPLETE)

    assert len(watermarks) == len(batch.facts)
    assert all(item.terminal_state.value == "COMPLETE" for item in watermarks)
    with pytest.raises(ValueError, match="equity bar batches only"):
        developer_equity_watermarks(_option_only_batch())


def _option_only_batch():
    _, options = _sources()
    result = options.result.batches[0]
    return build_developer_acquisition_batch(
        cohort_id=COHORT, provider="polygon_developer", provider_batch_id=result.provider_batch_id,
        started_at=result.started_at, completed_at=result.completed_at, facts=result.facts,
    )