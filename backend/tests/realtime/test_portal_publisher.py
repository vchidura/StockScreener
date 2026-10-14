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
    RETENTION_HOT_AND_KEPT,
    PortalReleaseReceipt,
    session_close_snapshot,
)
from realtime.portal_publisher import (
    PortalDeliveryDisposition,
    PortalDeliveryStatus,
    PortalPublicationBatch,
    PortalPublicationConfig,
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
    return PortalPublicationBatch(snapshots)


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


def test_publication_rejects_partial_canary_and_nonpublication_role():
    with pytest.raises(ValueError, match="exact canary ticker pages"):
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


def test_release_is_published_in_the_snapshot_transaction_only_with_a_receipt():
    database, connection, cursor = _database()
    events = []
    batch = PortalPublicationBatch(_batch().snapshots, receipt=_receipt())

    run_portal_publication_once(
        database, _config(), batch, FakeSink(()), checked_at=NOW,
        snapshot_repository=lambda active, snapshot: events.append(("snapshot", connection.commit.call_count)),
        release_repository=lambda active, receipt, snapshots, **options: events.append(
            ("release", len(snapshots), options, connection.commit.call_count)),
        claim_repository=lambda active, **kwargs: (),
    )

    assert [event[0] for event in events] == ["snapshot"] * 8 + ["release"]
    assert events[-1][1:] == (8, {}, 0)
    run_portal_publication_once(
        database, _config(), _batch(), FakeSink(()), checked_at=NOW,
        snapshot_repository=lambda active, snapshot: None,
        release_repository=lambda *args, **options: pytest.fail("no receipt, no release"),
        claim_repository=lambda active, **kwargs: (),
    )


def test_session_close_publishes_kept_variants_and_never_keeps_per_ticker_pages():
    database, connection, cursor = _database()
    summary = _summary()
    kept_summary = session_close_snapshot(summary)
    written, releases = [], []

    def persist(active, snapshot, **options):
        written.append((snapshot.snapshot_type, options.get("retention")))
        return kept_summary if options.get("retention") == RETENTION_HOT_AND_KEPT else None

    def release(active, receipt, snapshots, **options):
        releases.append((tuple(row.snapshot_id for row in snapshots), options))

    batch = PortalPublicationBatch(
        (*_batch().snapshots, summary), receipt=_receipt(), session_close=True,
    )
    run_portal_publication_once(
        database, _config(), batch, FakeSink(()), checked_at=NOW,
        snapshot_repository=persist, release_repository=release,
        claim_repository=lambda active, **kwargs: (),
    )

    assert written.count(("TICKER_PAGE", None)) == 8
    assert written[-1] == ("TICKER_WORKSPACE_SUMMARY_V1", RETENTION_HOT_AND_KEPT)
    assert len(releases) == 2
    assert len(releases[0][0]) == 9 and releases[0][1] == {}
    assert releases[1][0] == (kept_summary.snapshot_id,)
    assert releases[1][1] == {"release_class": RELEASE_CLASS_SESSION_CLOSE, "session_date": date(2026, 10, 5)}


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