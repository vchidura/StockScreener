from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime.contracts import CANARY_UNIVERSE
from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.portal_projection import build_portal_snapshot
from realtime.portal_projection_repository import (
    RELEASE_CLASS_SESSION_CLOSE,
    RELEASE_CLASS_INTRADAY,
    RETENTION_HOT_AND_KEPT,
    PortalRelease,
    PortalReleasePointer,
    PortalReleaseReceipt,
    session_close_snapshot,
)
from realtime.portal_publisher import (
    CanonicalPortalPublisher,
    PortalDeliveryDisposition,
    PortalDeliveryStatus,
    PortalPublicationBatch,
    PortalPublicationConfig,
    PortalPublicationStatus,
    PortalPublisherVerdict,
    run_portal_publication_once,
)
from realtime.preflight import ProviderTier
from realtime.worker_database import RealtimeProcessDatabase


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, tzinfo=UTC)
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OWNER = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _database(role=RealtimeDatabaseRole.PUBLICATION):
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
        "worker:portal_publisher:canary",
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


def _config():
    return PortalPublicationConfig(
        COHORT,
        ProviderTier.MASSIVE_DEVELOPER,
        OWNER,
        4,
    )


def _batch(*, omit=None):
    snapshots = tuple(
        build_portal_snapshot(
            cohort_id=COHORT,
            provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            snapshot_type="TICKER_PAGE",
            snapshot_key=ticker,
            source_generation=7,
            source_manifest={"universe": CANARY_UNIVERSE.sha256},
            payload={"ticker": ticker, "setups": []},
            generated_at=NOW,
            valid_until=NOW + timedelta(hours=1),
        )
        for ticker in CANARY_UNIVERSE.equity_tickers
        if ticker != omit
    )
    return PortalPublicationBatch(
        snapshots,
        expected_ticker_keys=CANARY_UNIVERSE.equity_tickers,
    )


def _outbox(index=1):
    return {
        "outbox_id": uuid5(NAMESPACE_URL, f"outbox:{index}"),
        "event_id": uuid5(NAMESPACE_URL, f"event:{index}"),
        "destination": "PORTAL",
        "message_kind": "ALERT_UPSERT",
        "payload_sha256": f"{index:064x}",
        "payload": {"alert_id": f"alert-{index}"},
        "attempts": 1,
    }


class FakeSink:
    def __init__(self, dispositions):
        self.dispositions = list(dispositions)
        self.messages = []

    def deliver(self, message):
        self.messages.append(message)
        return self.dispositions.pop(0)


def test_publication_promotes_exact_canary_then_delivers_fenced_outbox():
    database, connection, cursor = _database()
    sink = FakeSink((PortalDeliveryDisposition(PortalDeliveryStatus.DELIVERED),))
    snapshots = []
    acknowledgements = []

    def promote(active_cursor, snapshot):
        assert active_cursor is cursor
        snapshots.append(snapshot)
        assert connection.commit.call_count == 0

    def claim(active_cursor, **kwargs):
        assert active_cursor is cursor
        return (_outbox(),)

    def acknowledge(active_cursor, **kwargs):
        assert active_cursor is cursor
        acknowledgements.append(kwargs)

    status = run_portal_publication_once(
        database,
        _config(),
        _batch(),
        sink,
        checked_at=NOW,
        snapshot_repository=promote,
        claim_repository=claim,
        delivered_repository=acknowledge,
    )

    assert status.verdict is PortalPublisherVerdict.COMPLETE
    assert status.promoted_snapshots == 8
    assert status.claimed == status.delivered == 1
    assert {snapshot.snapshot_key for snapshot in snapshots} == set(
        CANARY_UNIVERSE.equity_tickers
    )
    assert sink.messages[0].outbox_id == _outbox()["outbox_id"]
    assert acknowledgements[0]["owner_epoch"] == 4
    assert connection.commit.call_count == 3
    assert connection.rollback.call_count == 0


@pytest.mark.parametrize(
    ("delivery_status", "expected_counter", "transition"),
    (
        (PortalDeliveryStatus.RETRY, "retried", "PENDING"),
        (PortalDeliveryStatus.ABANDONED, "abandoned", "ABANDONED"),
    ),
)
def test_publication_retries_or_abandons_without_losing_claim(
    delivery_status,
    expected_counter,
    transition,
):
    database, connection, _ = _database()
    sink = FakeSink((PortalDeliveryDisposition(delivery_status, "DELIVERY_FAILED"),))
    failures = []

    def fail(cursor, **kwargs):
        failures.append(kwargs)
        return transition

    status = run_portal_publication_once(
        database,
        _config(),
        PortalPublicationBatch(),
        sink,
        checked_at=NOW,
        claim_repository=lambda cursor, **kwargs: (_outbox(),),
        failed_repository=fail,
    )

    assert status.verdict is PortalPublisherVerdict.DEGRADED
    assert getattr(status, expected_counter) == 1
    assert status.reason_codes == ("DELIVERY_FAILED",)
    assert failures[0]["retryable"] is (
        delivery_status is PortalDeliveryStatus.RETRY
    )
    assert connection.commit.call_count == 2


def test_publication_rejects_membership_drift_and_nonpublication_role():
    with pytest.raises(
        ValueError,
        match="changed from declared membership",
    ):
        _batch(omit="AAPL")

    database, connection, _ = _database(RealtimeDatabaseRole.COMPUTE)
    sink = FakeSink(())
    with pytest.raises(ValueError, match="requires the publication database role"):
        run_portal_publication_once(
            database,
            _config(),
            PortalPublicationBatch(),
            sink,
            checked_at=NOW,
        )
    assert connection.cursor.call_count == 0


def test_publication_accepts_declared_variable_ticker_membership():
    snapshots = _batch().snapshots[:3]
    batch = PortalPublicationBatch(
        snapshots,
        expected_ticker_keys=tuple(
            row.snapshot_key for row in snapshots
        ),
    )

    assert len(batch.snapshots) == 3
    assert {
        row.snapshot_key for row in batch.snapshots
    } == set(batch.expected_ticker_keys)


def test_declared_ticker_membership_is_persisted_with_publication_work():
    database, _, _ = _database()
    receipt = _receipt()
    snapshots = _batch().snapshots[:3]
    expected = tuple(row.snapshot_key for row in snapshots)
    enqueued = []
    batch = PortalPublicationBatch(
        snapshots,
        receipt=receipt,
        expected_ticker_keys=expected,
    )

    run_portal_publication_once(
        database,
        replace(_config(), dispatch_authorized=False),
        batch,
        FakeSink(()),
        checked_at=NOW,
        snapshot_repository=lambda *_args, **_kwargs: None,
        enqueue_repository=(
            lambda _cursor, _receipt, _snapshots, **options: (
                enqueued.append(options)
                or {"work_id": uuid5(NAMESPACE_URL, "publication-work")}
            )
        ),
        publication_work_runner=_completed_work_runner(
            _release(receipt)
        ),
    )

    assert enqueued[0]["expected_ticker_keys"] == tuple(
        sorted(expected)
    )


def test_sink_exception_becomes_retryable_fenced_failure():
    database, _, _ = _database()
    sink = MagicMock()
    sink.deliver.side_effect = RuntimeError("receiver unavailable")
    failures = []

    def fail(cursor, **kwargs):
        failures.append(kwargs)
        return "PENDING"

    status = run_portal_publication_once(
        database,
        _config(),
        PortalPublicationBatch(),
        sink,
        checked_at=NOW,
        claim_repository=lambda cursor, **kwargs: (_outbox(),),
        failed_repository=fail,
    )

    assert status.retried == 1
    assert status.reason_codes == ("PORTAL_SINK_EXCEPTION",)
    assert failures[0]["retryable"] is True


def _receipt():
    return PortalReleaseReceipt(
        UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"), COHORT, ProviderTier.MASSIVE_DEVELOPER,
        NOW, "9" * 64,
    )


def _summary(session_date="2026-10-05"):
    return build_portal_snapshot(
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        snapshot_type="TICKER_WORKSPACE_SUMMARY_V1",
        snapshot_key="PLATFORM",
        source_generation=7,
        source_manifest={"universe": CANARY_UNIVERSE.sha256},
        payload={"session": session_date, "overview": []},
        generated_at=NOW,
    )


def _release(receipt=None, *, pointer_swapped=True):
    source = receipt or _receipt()
    manifest = {
        "version": "realtime-portal-release-v1",
        "release_class": RELEASE_CLASS_INTRADAY,
        "session_date": NOW.date().isoformat(),
        "source_receipt_ids": [str(source.receipt_id)],
        "snapshots": [],
    }
    return PortalRelease(
        uuid5(NAMESPACE_URL, f"release:{source.receipt_id}"),
        COHORT,
        ProviderTier.MASSIVE_DEVELOPER,
        RELEASE_CLASS_INTRADAY,
        source.receipt_id,
        NOW.date(),
        NOW,
        source.universe_sha256,
        manifest,
        "a" * 64,
        pointer_swapped,
    )


def _completed_work_runner(release, events=None):
    def run(database, config, work, **options):
        if events is not None:
            events.append(("work", work, options))
        return PortalPublicationStatus(
            NOW,
            0,
            0,
            0,
            0,
            0,
            (),
            release,
            uuid5(NAMESPACE_URL, "publication-work"),
        )

    return run


def test_release_is_published_in_the_snapshot_transaction_only_with_a_receipt():
    database, connection, cursor = _database()
    events = []
    receipt = _receipt()
    batch = PortalPublicationBatch(
        _batch().snapshots,
        receipt=receipt,
        product_key="TEST_EQUITY",
    )

    run_portal_publication_once(
        database, _config(), batch, FakeSink(()), checked_at=NOW,
        snapshot_repository=lambda active, snapshot: events.append(("snapshot", connection.commit.call_count)),
        enqueue_repository=lambda active, source, snapshots, **options: (
            events.append((
                "enqueue",
                len(snapshots),
                options,
                connection.commit.call_count,
            ))
            or {"work_id": uuid5(NAMESPACE_URL, "publication-work")}
        ),
        publication_work_runner=_completed_work_runner(
            _release(receipt),
            events,
        ),
        claim_repository=lambda active, **kwargs: (),
    )

    assert [event[0] for event in events[:9]] == [
        "snapshot"
    ] * 8 + ["enqueue"]
    assert events[8][1] == 8
    assert events[-1][0] == "work"
    run_portal_publication_once(
        database, _config(), _batch(), FakeSink(()), checked_at=NOW,
        snapshot_repository=lambda active, snapshot: None,
        release_repository=lambda *args, **options: pytest.fail("no receipt, no release"),
        claim_repository=lambda active, **kwargs: (),
    )


def test_disposable_canary_publication_never_claims_or_delivers_outbox():
    database, _, _ = _database()
    claimed = []
    delivered = []
    config = replace(_config(), dispatch_authorized=False)

    receipt = _receipt()
    status = run_portal_publication_once(
        database,
        config,
        PortalPublicationBatch(_batch().snapshots, receipt=receipt),
        FakeSink(delivered),
        checked_at=NOW,
        snapshot_repository=lambda active, snapshot: None,
        enqueue_repository=lambda active, source, snapshots, **options: {
            "work_id": uuid5(NAMESPACE_URL, "publication-work")
        },
        publication_work_runner=_completed_work_runner(
            _release(receipt)
        ),
        claim_repository=lambda active, **kwargs: claimed.append(kwargs),
    )

    assert status.promoted_snapshots == len(_batch().snapshots)
    assert status.claimed == 0
    assert claimed == []
    assert delivered == []


def test_non_ticker_lane_can_publish_without_synthesizing_ticker_pages():
    summary = _summary()

    batch = PortalPublicationBatch(
        (summary,),
        receipt=_receipt(),
    )

    assert batch.snapshots == (summary,)


def test_session_close_publishes_kept_variants_and_never_keeps_per_ticker_pages():
    database, connection, cursor = _database()
    summary = _summary()
    kept_summary = session_close_snapshot(summary)
    written, enqueued = [], []

    def persist(active, snapshot, **options):
        written.append((snapshot.snapshot_type, options.get("retention")))
        return kept_summary if options.get("retention") == RETENTION_HOT_AND_KEPT else None

    def enqueue(active, receipt, snapshots, **options):
        enqueued.append((
            tuple(row.snapshot_id for row in snapshots),
            options,
        ))
        return {"work_id": uuid5(NAMESPACE_URL, "publication-work")}

    receipt = _receipt()
    batch = PortalPublicationBatch(
        (*_batch().snapshots, summary),
        receipt=receipt,
        session_close=True,
    )
    run_portal_publication_once(
        database, _config(), batch, FakeSink(()), checked_at=NOW,
        snapshot_repository=persist,
        enqueue_repository=enqueue,
        publication_work_runner=_completed_work_runner(
            _release(receipt)
        ),
        claim_repository=lambda active, **kwargs: (),
    )

    assert written.count(("TICKER_PAGE", None)) == 8
    assert written[-1] == ("TICKER_WORKSPACE_SUMMARY_V1", RETENTION_HOT_AND_KEPT)
    assert len(enqueued) == 1
    assert len(enqueued[0][0]) == 9
    assert enqueued[0][1]["kept_snapshots"] == (kept_summary,)
    assert enqueued[0][1]["session_date"] == date(2026, 10, 5)


def test_receipt_and_session_close_batches_are_validated_before_any_write():
    with pytest.raises(ValueError, match="requires its cycle receipt"):
        PortalPublicationBatch(_batch().snapshots, session_close=True)
    with pytest.raises(ValueError, match="requires snapshots"):
        PortalPublicationBatch(receipt=_receipt())
    database, connection, cursor = _database()
    foreign = PortalReleaseReceipt(
        UUID(int=1), UUID(int=2), ProviderTier.MASSIVE_DEVELOPER, NOW, "9" * 64,
    )
    with pytest.raises(ValueError, match="conflicts with publisher identity"):
        run_portal_publication_once(
            database, _config(), PortalPublicationBatch(_batch().snapshots, receipt=foreign),
            FakeSink(()), checked_at=NOW,
        )
    assert connection.cursor.call_count == 0


def test_canonical_stage_publisher_returns_one_pointer_without_racy_rollback():
    receipt = _receipt()
    batch = PortalPublicationBatch(
        _batch().snapshots,
        receipt=receipt,
    )
    release_id = uuid5(NAMESPACE_URL, "canonical-options-release")
    manifest = {
        "version": "realtime-portal-release-v1",
        "release_class": RELEASE_CLASS_INTRADAY,
        "session_date": NOW.date().isoformat(),
        "snapshots": [],
    }
    release = PortalRelease(
        release_id,
        COHORT,
        ProviderTier.MASSIVE_DEVELOPER,
        RELEASE_CLASS_INTRADAY,
        receipt.receipt_id,
        NOW.date(),
        NOW,
        receipt.universe_sha256,
        manifest,
        "a" * 64,
        True,
    )
    previous = PortalReleasePointer(
        COHORT,
        ProviderTier.MASSIVE_DEVELOPER,
        uuid5(NAMESPACE_URL, "prior-release"),
        NOW - timedelta(minutes=1),
        NOW.date(),
    )
    current = PortalReleasePointer(
        COHORT,
        ProviderTier.MASSIVE_DEVELOPER,
        release_id,
        NOW,
        NOW.date(),
    )
    status = PortalPublicationStatus(
        NOW,
        len(batch.snapshots),
        0,
        0,
        0,
        0,
        (),
        release,
    )
    database, _, _ = _database()
    publisher = CanonicalPortalPublisher(
        database,
        replace(_config(), dispatch_authorized=False),
        publication_runner=lambda *_args, **_kwargs: status,
        pointer_loader=lambda *_args, **_kwargs: current,
    )

    result = publisher.publish(batch, checked_at=NOW)

    assert result.release.release_id == result.pointer.release_id
    assert result.status.release == result.release

    database, _, _ = _database()
    restored = []
    failing = CanonicalPortalPublisher(
        database,
        replace(_config(), dispatch_authorized=False),
        publication_runner=lambda *_args, **_kwargs: status,
        pointer_loader=lambda *_args, **_kwargs: None,
        pointer_restorer=lambda _cursor, **values: restored.append(values),
    )
    with pytest.raises(
        ValueError,
        match="did not retain its pointer outcome",
    ):
        failing.publish(batch, checked_at=NOW)

    assert restored == []


def test_canonical_publisher_reports_noncurrent_without_pointer_authority(
    monkeypatch,
):
    receipt = _receipt()
    batch = PortalPublicationBatch(
        _batch().snapshots,
        receipt=receipt,
    )
    release = replace(
        _release(receipt),
        pointer_swapped=False,
        publication_disposition="HISTORICAL_NONCURRENT",
        deliverable=False,
    )
    status = PortalPublicationStatus(
        NOW,
        len(batch.snapshots),
        0,
        0,
        0,
        0,
        ("PORTAL_PUBLICATION_HISTORICAL_NONCURRENT",),
        release,
    )
    database, _, _ = _database()
    publisher = CanonicalPortalPublisher(
        database,
        replace(_config(), dispatch_authorized=False),
        publication_runner=lambda *_args, **_kwargs: status,
        pointer_loader=lambda *_args, **_kwargs: None,
    )

    result = publisher.publish(batch, checked_at=NOW)
    monkeypatch.setattr(
        "realtime.portal_publisher.resume_portal_publication_once",
        lambda *_args, **_kwargs: status,
    )
    resumed = publisher.resume(
        source_receipt_id=receipt.receipt_id,
        checked_at=NOW + timedelta(seconds=1),
    )

    assert result.release == release
    assert result.pointer is None
    assert result.status.verdict is PortalPublisherVerdict.DEGRADED
    assert resumed.release == release
    assert resumed.pointer is None
    assert resumed.status.reason_codes == (
        "PORTAL_PUBLICATION_HISTORICAL_NONCURRENT",
    )