from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from psycopg2.extras import RealDictCursor

from realtime.contracts import canonical_sha256
from realtime.database_access import RealtimeDatabaseRole
from realtime.portal_projection import build_portal_snapshot
from realtime.portal_projection_repository import (
    PortalRelease,
    PortalReleaseReceipt,
    claim_portal_publication_work_cursor,
    complete_portal_publication_work_cursor,
    enqueue_portal_publication_work_cursor,
    fail_portal_publication_work_cursor,
    load_portal_publication_snapshots_cursor,
    load_portal_publication_work_cursor,
    load_portal_release_pointer_cursor,
    persist_portal_snapshot_cursor,
    publish_portal_release_cursor,
)
from realtime.portal_publisher import (
    CanonicalPortalPublisher,
    PortalPublicationBatch,
    PortalPublicationConfig,
    PortalPublisherVerdict,
)
from realtime.preflight import ProviderTier
from realtime.stage_database_v2 import V2WorkerDatabase
from tests.realtime.v2_database import (
    disposable_v2_database,
    insert_cohort,
    insert_service_plan,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc
NOW = datetime(2026, 10, 30, 15, tzinfo=UTC)
NEXT_SESSION = datetime(2026, 11, 2, 15, tzinfo=UTC)


@contextmanager
def transaction(pool, *, read_only=False):
    connection = pool.getconn()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            if read_only:
                cursor.execute("SET TRANSACTION READ ONLY")
            yield cursor
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        pool.putconn(connection)


class _Provider:
    def __init__(self, pool):
        self._pool = pool

    @contextmanager
    def transaction(
        self,
        *,
        read_only=False,
        dict_cursor=False,
        isolation_level=None,
    ):
        connection = self._pool.getconn()
        try:
            with connection.cursor(
                cursor_factory=RealDictCursor
            ) as cursor:
                if read_only:
                    cursor.execute("SET TRANSACTION READ ONLY")
                if isolation_level is not None:
                    cursor.execute(
                        "SET TRANSACTION ISOLATION LEVEL "
                        + isolation_level
                    )
                yield SimpleNamespace(cursor=cursor)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            self._pool.putconn(connection)


def _digest(value) -> str:
    return canonical_sha256(value)


def _receipt(cursor, cohort_id, plan_id, cutoff, generation):
    window_id = uuid4()
    receipt_id = uuid4()
    input_sha256 = _digest({
        "cohort_id": str(cohort_id),
        "cutoff": cutoff,
        "generation": generation,
    })
    universe_sha256 = _digest({
        "cohort_id": str(cohort_id),
        "generation": generation,
    })
    cursor.execute(
        "INSERT INTO rt.window_runs("
        "window_id,plan_id,cohort_id,provider_tier,window_key,"
        "release_at,cutoff,deadline) VALUES("
        "%s,%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s)",
        (
            str(window_id),
            str(plan_id),
            str(cohort_id),
            f"c7-{window_id}",
            cutoff,
            cutoff,
            cutoff + timedelta(minutes=10),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.cycle_receipts("
        "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
        "reference_generation,universe_sha256,input_sha256,build_id"
        ") VALUES(%s,%s,%s,'INPUT_READY',%s,%s,%s,%s,'c7-test')",
        (
            str(receipt_id),
            str(window_id),
            str(cohort_id),
            cutoff,
            generation,
            universe_sha256,
            input_sha256,
        ),
    )
    return PortalReleaseReceipt(
        receipt_id,
        cohort_id,
        ProviderTier.MASSIVE_DEVELOPER,
        cutoff,
        universe_sha256,
        source_receipt_sha256=input_sha256,
        source_generation=generation,
        desired_snapshot_sha256=_digest({
            "session": str(cutoff.date()),
            "generation": generation,
        }),
        membership_sha256=_digest({
            "members": ["AAPL"] if generation == 1 else ["AAPL", "MSFT"],
        }),
    )


def _snapshot(receipt, members, *, deliverable=True):
    manifest = {
        "version": "STOCK_ALERTS_V3",
        "desired_snapshot_sha256": receipt.desired_snapshot_sha256,
        "membership_sha256": receipt.membership_sha256,
        "observation_session": receipt.cutoff.date().isoformat(),
        "observation_cutoff": receipt.cutoff.isoformat(),
        "deliverable": deliverable,
    }
    payload = {
        "version": "STOCK_ALERTS_V3",
        "members": list(members),
        "provider_fetch_performed": False,
        "detector_recomputed": False,
        "deliverable": deliverable,
    }
    return build_portal_snapshot(
        cohort_id=receipt.cohort_id,
        provider_tier=receipt.provider_tier,
        snapshot_type="STOCK_ALERTS_V3",
        snapshot_key="PLATFORM",
        source_generation=receipt.source_generation,
        source_manifest=manifest,
        payload=payload,
        generated_at=receipt.cutoff,
    )


def _ticker_snapshot(receipt, ticker, *, marker):
    return build_portal_snapshot(
        cohort_id=receipt.cohort_id,
        provider_tier=receipt.provider_tier,
        snapshot_type="TICKER_PAGE",
        snapshot_key=ticker,
        source_generation=receipt.source_generation,
        source_manifest={
            "desired_snapshot_sha256": (
                receipt.desired_snapshot_sha256
            ),
            "membership_sha256": receipt.membership_sha256,
            "deliverable": True,
        },
        payload={"ticker": ticker, "marker": marker},
        generated_at=receipt.cutoff,
    )


def _independent_snapshot(receipt, *, marker):
    return build_portal_snapshot(
        cohort_id=receipt.cohort_id,
        provider_tier=receipt.provider_tier,
        snapshot_type="DAILY_STRATEGY_ROLLING_V1",
        snapshot_key="COHORT",
        source_generation=receipt.source_generation,
        source_manifest={"lane": "INDEPENDENT", "deliverable": True},
        payload={"lane": "INDEPENDENT", "marker": marker},
        generated_at=receipt.cutoff,
    )


def _claim(cursor, work, owner_id, epoch, claimed_at):
    return claim_portal_publication_work_cursor(
        cursor,
        work_id=UUID(str(work["work_id"])),
        owner_id=owner_id,
        owner_epoch=epoch,
        claimed_at=claimed_at,
        lease_until=claimed_at + timedelta(seconds=30),
    )


def test_c7_publication_failure_preserves_pointer_and_retries_stored_merge():
    database_name = (
        "test_realtime_v2_c7_publication_" + uuid4().hex[:8]
    )
    owner_id = uuid4()
    with disposable_v2_database(database_name) as pool:
        with transaction(pool) as cursor:
            cohort_id = insert_cohort(cursor, NOW - timedelta(hours=1))
            plan_id = insert_service_plan(
                cursor,
                cohort_id,
                {"STOCK_ALERTS"},
                NOW - timedelta(hours=1),
            )
            first_receipt = _receipt(
                cursor,
                cohort_id,
                plan_id,
                NOW,
                1,
            )
        first_snapshot = _snapshot(first_receipt, ("AAPL",))
        with transaction(pool) as cursor:
            persist_portal_snapshot_cursor(cursor, first_snapshot)
            first_release = publish_portal_release_cursor(
                cursor,
                first_receipt,
                (first_snapshot,),
            )
        assert first_release.pointer_swapped is True

        with transaction(pool) as cursor:
            next_plan_id = insert_service_plan(
                cursor,
                cohort_id,
                {"STOCK_ALERTS"},
                NEXT_SESSION - timedelta(hours=1),
            )
            second_receipt = _receipt(
                cursor,
                cohort_id,
                next_plan_id,
                NEXT_SESSION,
                2,
            )
        second_snapshot = _snapshot(
            second_receipt,
            ("AAPL", "MSFT"),
        )
        with transaction(pool) as cursor:
            persist_portal_snapshot_cursor(cursor, second_snapshot)
            work = enqueue_portal_publication_work_cursor(
                cursor,
                second_receipt,
                (second_snapshot,),
                product_key="STOCK_ALERTS",
                created_at=NEXT_SESSION,
            )
        divergent_snapshot = _snapshot(
            second_receipt,
            ("AAPL", "MSFT", "TSLA"),
        )
        with pytest.raises(
            ValueError,
            match="identity changed on replay",
        ):
            with transaction(pool) as cursor:
                persist_portal_snapshot_cursor(
                    cursor,
                    divergent_snapshot,
                )
                enqueue_portal_publication_work_cursor(
                    cursor,
                    second_receipt,
                    (divergent_snapshot,),
                    product_key="STOCK_ALERTS",
                    created_at=NEXT_SESSION,
                )
        with transaction(pool) as cursor:
            claimed = _claim(
                cursor,
                work,
                owner_id,
                1,
                NEXT_SESSION,
            )
        assert claimed is not None and claimed["attempts"] == 1
        with transaction(pool) as cursor:
            assert _claim(
                cursor,
                work,
                uuid4(),
                1,
                NEXT_SESSION,
            ) is None

        with pytest.raises(RuntimeError, match="injected commit failure"):
            with transaction(pool) as cursor:
                durable = load_portal_publication_snapshots_cursor(
                    cursor,
                    claimed,
                )
                assert durable[0].payload["members"] == ["AAPL", "MSFT"]
                assert durable[0].payload["provider_fetch_performed"] is False
                assert durable[0].payload["detector_recomputed"] is False
                publish_portal_release_cursor(
                    cursor,
                    second_receipt,
                    durable,
                )
                raise RuntimeError("injected commit failure")

        with transaction(pool) as cursor:
            pointer = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            )
            assert pointer is not None
            assert pointer.release_id == first_release.release_id
            assert fail_portal_publication_work_cursor(
                cursor,
                claimed,
                owner_id=owner_id,
                owner_epoch=1,
                failed_at=NEXT_SESSION + timedelta(seconds=1),
                next_attempt_at=NEXT_SESSION + timedelta(seconds=2),
                failure_reason="INJECTED_COMMIT_FAILURE",
            ) == "RETRY"

        with transaction(pool) as cursor:
            retry_work = load_portal_publication_work_cursor(
                cursor,
                product_key="STOCK_ALERTS",
                source_receipt_id=second_receipt.receipt_id,
            )
            retried = _claim(
                cursor,
                retry_work,
                owner_id,
                2,
                NEXT_SESSION + timedelta(seconds=2),
            )
        assert retried is not None and retried["attempts"] == 2
        with transaction(pool) as cursor:
            previous = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                for_update=True,
            )
            durable = load_portal_publication_snapshots_cursor(
                cursor,
                retried,
            )
            second_release = publish_portal_release_cursor(
                cursor,
                second_receipt,
                durable,
            )
            current = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                for_update=True,
            )
            complete_portal_publication_work_cursor(
                cursor,
                retried,
                second_release,
                owner_id=owner_id,
                owner_epoch=2,
                completed_at=NEXT_SESSION + timedelta(seconds=2),
                previous_release_id=previous.release_id,
                current_release_id=current.release_id,
            )

        assert second_release.pointer_swapped is True
        assert second_release.release_id != first_release.release_id
        with transaction(pool, read_only=True) as cursor:
            cursor.execute(
                "SELECT state,disposition,attempts,release_id::text "
                "FROM rt.portal_publication_work WHERE work_id=%s",
                (str(work["work_id"]),),
            )
            stored = cursor.fetchone()
            assert stored == {
                "state": "COMPLETE",
                "disposition": "CURRENT",
                "attempts": 2,
                "release_id": str(second_release.release_id),
            }
            cursor.execute(
                "SELECT outcome FROM rt.portal_publication_receipts "
                "WHERE work_id=%s ORDER BY attempt",
                (str(work["work_id"]),),
            )
            assert [row["outcome"] for row in cursor.fetchall()] == [
                "FAILED_RETRY",
                "CURRENT",
            ]

        with transaction(pool) as cursor:
            equivalent = publish_portal_release_cursor(
                cursor,
                second_receipt,
                (second_snapshot,),
            )
        assert equivalent.release_id == second_release.release_id
        assert equivalent.publication_disposition == "EQUIVALENT_CURRENT"

        with transaction(pool) as cursor:
            stale_receipt = _receipt(
                cursor,
                cohort_id,
                plan_id,
                NOW - timedelta(minutes=5),
                1,
            )
        stale_snapshot = _snapshot(stale_receipt, ("AAPL",))
        with transaction(pool) as cursor:
            persist_portal_snapshot_cursor(cursor, stale_snapshot)
            stale = publish_portal_release_cursor(
                cursor,
                stale_receipt,
                (stale_snapshot,),
            )
            pointer = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            )
        assert stale.publication_disposition == "HISTORICAL_STALE"
        assert stale.pointer_swapped is False
        assert stale.deliverable is False
        assert pointer.release_id == second_release.release_id


def test_c7_expired_claim_takeover_fences_old_owner_and_publishes_once():
    database_name = (
        "test_realtime_v2_c7_takeover_" + uuid4().hex[:8]
    )
    first_owner, second_owner = uuid4(), uuid4()
    with disposable_v2_database(database_name) as pool:
        with transaction(pool) as cursor:
            cohort_id = insert_cohort(cursor, NOW - timedelta(hours=1))
            plan_id = insert_service_plan(
                cursor,
                cohort_id,
                {"STOCK_ALERTS"},
                NOW - timedelta(hours=1),
            )
            receipt = _receipt(cursor, cohort_id, plan_id, NOW, 1)
        snapshot = _snapshot(receipt, ("AAPL",))
        with transaction(pool) as cursor:
            persist_portal_snapshot_cursor(cursor, snapshot)
            work = enqueue_portal_publication_work_cursor(
                cursor,
                receipt,
                (snapshot,),
                product_key="STOCK_ALERTS",
                created_at=NOW,
            )
        with transaction(pool) as cursor:
            original = _claim(
                cursor,
                work,
                first_owner,
                3,
                NOW,
            )
        assert original is not None
        assert original["attempts"] == 1
        assert original["lease_epoch"] == 3

        with transaction(pool) as cursor:
            assert _claim(
                cursor,
                work,
                second_owner,
                1,
                NOW + timedelta(seconds=29),
            ) is None
        with transaction(pool) as cursor:
            takeover = _claim(
                cursor,
                work,
                second_owner,
                1,
                NOW + timedelta(seconds=31),
            )
        assert takeover is not None
        assert takeover["attempts"] == 2
        assert str(takeover["lease_owner"]) == str(second_owner)
        assert takeover["lease_epoch"] == 4

        dummy_release = PortalRelease(
            uuid4(),
            cohort_id,
            ProviderTier.MASSIVE_DEVELOPER,
            "INTRADAY",
            receipt.receipt_id,
            NOW.date(),
            NOW,
            receipt.universe_sha256,
            {"version": "realtime-portal-release-v1"},
            "a" * 64,
            False,
        )
        with pytest.raises(ValueError, match="lost its work lease"):
            with transaction(pool) as cursor:
                complete_portal_publication_work_cursor(
                    cursor,
                    original,
                    dummy_release,
                    owner_id=first_owner,
                    owner_epoch=3,
                    completed_at=NOW + timedelta(seconds=32),
                    previous_release_id=None,
                    current_release_id=None,
                )
        with pytest.raises(ValueError, match="lost its work lease"):
            with transaction(pool) as cursor:
                fail_portal_publication_work_cursor(
                    cursor,
                    original,
                    owner_id=first_owner,
                    owner_epoch=3,
                    failed_at=NOW + timedelta(seconds=32),
                    next_attempt_at=NOW + timedelta(seconds=33),
                    failure_reason="OLD_OWNER",
                )

        with transaction(pool) as cursor:
            durable = load_portal_publication_snapshots_cursor(
                cursor,
                takeover,
            )
            release = publish_portal_release_cursor(
                cursor,
                receipt,
                durable,
            )
            current = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                for_update=True,
            )
            complete_portal_publication_work_cursor(
                cursor,
                takeover,
                release,
                owner_id=second_owner,
                owner_epoch=int(takeover["lease_epoch"]),
                completed_at=NOW + timedelta(seconds=32),
                previous_release_id=None,
                current_release_id=current.release_id,
            )
        with transaction(pool) as cursor:
            replay = enqueue_portal_publication_work_cursor(
                cursor,
                receipt,
                (snapshot,),
                product_key="STOCK_ALERTS",
                created_at=NOW,
            )
            assert replay["state"] == "COMPLETE"
            assert _claim(
                cursor,
                replay,
                uuid4(),
                1,
                NOW + timedelta(minutes=2),
            ) is None
            cursor.execute(
                "SELECT count(*) AS rows FROM rt.portal_releases "
                "WHERE cohort_id=%s",
                (str(cohort_id),),
            )
            assert cursor.fetchone()["rows"] == 1


def test_c7_non_deliverable_forward_work_never_gains_pointer_authority():
    database_name = (
        "test_realtime_v2_c7_noncurrent_" + uuid4().hex[:8]
    )
    owner_id = uuid4()
    with disposable_v2_database(database_name) as pool:
        with transaction(pool) as cursor:
            cohort_id = insert_cohort(cursor, NOW - timedelta(hours=1))
            first_plan = insert_service_plan(
                cursor,
                cohort_id,
                {"STOCK_ALERTS"},
                NOW - timedelta(hours=1),
            )
            historical_receipt = _receipt(
                cursor,
                cohort_id,
                first_plan,
                NOW,
                1,
            )
        historical_snapshot = _snapshot(
            historical_receipt,
            ("AAPL",),
            deliverable=False,
        )
        with transaction(pool) as cursor:
            persist_portal_snapshot_cursor(cursor, historical_snapshot)
            no_prior = publish_portal_release_cursor(
                cursor,
                historical_receipt,
                (historical_snapshot,),
            )
            assert load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            ) is None
        assert no_prior.publication_disposition == (
            "HISTORICAL_NONCURRENT"
        )
        assert no_prior.pointer_swapped is False
        assert no_prior.deliverable is False
        with transaction(pool) as cursor:
            equivalent_no_prior = publish_portal_release_cursor(
                cursor,
                historical_receipt,
                (historical_snapshot,),
            )
        assert equivalent_no_prior.release_id == no_prior.release_id
        assert equivalent_no_prior.publication_disposition == (
            "HISTORICAL_NONCURRENT"
        )

        with transaction(pool) as cursor:
            current_plan = insert_service_plan(
                cursor,
                cohort_id,
                {"STOCK_ALERTS"},
                NEXT_SESSION - timedelta(hours=1),
            )
            current_receipt = _receipt(
                cursor,
                cohort_id,
                current_plan,
                NEXT_SESSION,
                2,
            )
        current_snapshot = _snapshot(
            current_receipt,
            ("AAPL", "MSFT"),
        )
        with transaction(pool) as cursor:
            persist_portal_snapshot_cursor(cursor, current_snapshot)
            current_release = publish_portal_release_cursor(
                cursor,
                current_receipt,
                (current_snapshot,),
            )

        with transaction(pool) as cursor:
            late_receipt = _receipt(
                cursor,
                cohort_id,
                current_plan,
                NEXT_SESSION + timedelta(minutes=5),
                3,
            )
        late_receipt = replace(
            late_receipt,
            desired_snapshot_sha256=(
                current_receipt.desired_snapshot_sha256
            ),
            membership_sha256=current_receipt.membership_sha256,
        )
        late_snapshot = _snapshot(
            late_receipt,
            ("AAPL", "MSFT"),
            deliverable=False,
        )
        with transaction(pool) as cursor:
            persist_portal_snapshot_cursor(cursor, late_snapshot)
            work = enqueue_portal_publication_work_cursor(
                cursor,
                late_receipt,
                (late_snapshot,),
                product_key="STOCK_ALERTS",
                created_at=late_receipt.cutoff,
            )
        with transaction(pool) as cursor:
            claimed = _claim(
                cursor,
                work,
                owner_id,
                1,
                late_receipt.cutoff,
            )
        with transaction(pool) as cursor:
            previous = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                for_update=True,
            )
            durable = load_portal_publication_snapshots_cursor(
                cursor,
                claimed,
            )
            historical = publish_portal_release_cursor(
                cursor,
                late_receipt,
                durable,
            )
            current = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                for_update=True,
            )
            complete_portal_publication_work_cursor(
                cursor,
                claimed,
                historical,
                owner_id=owner_id,
                owner_epoch=int(claimed["lease_epoch"]),
                completed_at=late_receipt.cutoff,
                previous_release_id=previous.release_id,
                current_release_id=current.release_id,
            )
        assert historical.publication_disposition == (
            "HISTORICAL_NONCURRENT"
        )
        assert historical.deliverable is False
        assert historical.pointer_swapped is False
        assert current.release_id == current_release.release_id
        assert len(historical.manifest["snapshots"]) == 1
        with transaction(pool) as cursor:
            replay = enqueue_portal_publication_work_cursor(
                cursor,
                late_receipt,
                (late_snapshot,),
                product_key="STOCK_ALERTS",
                created_at=late_receipt.cutoff,
            )
            assert replay["state"] == "COMPLETE"
            assert replay["disposition"] == "HISTORICAL_NONCURRENT"
            equivalent = publish_portal_release_cursor(
                cursor,
                late_receipt,
                (late_snapshot,),
            )
        assert equivalent.release_id == historical.release_id
        assert equivalent.publication_disposition == (
            "HISTORICAL_NONCURRENT"
        )


def test_c7_public_publisher_resumes_first_historical_without_pointer():
    database_name = (
        "test_realtime_v2_c7_public_historical_" + uuid4().hex[:8]
    )
    with disposable_v2_database(database_name) as pool:
        with transaction(pool) as cursor:
            cohort_id = insert_cohort(cursor, NOW - timedelta(hours=1))
            first_plan = insert_service_plan(
                cursor,
                cohort_id,
                {"STOCK_ALERTS"},
                NOW - timedelta(hours=1),
            )
            historical_receipt = _receipt(
                cursor,
                cohort_id,
                first_plan,
                NOW,
                1,
            )
        historical_snapshot = _snapshot(
            historical_receipt,
            ("AAPL",),
            deliverable=False,
        )
        database = V2WorkerDatabase(
            _Provider(pool),
            RealtimeDatabaseRole.PUBLICATION,
        )
        publisher = CanonicalPortalPublisher(
            database,
            PortalPublicationConfig(
                cohort_id,
                ProviderTier.MASSIVE_DEVELOPER,
                uuid4(),
                1,
                dispatch_authorized=False,
            ),
        )
        historical_batch = PortalPublicationBatch(
            (historical_snapshot,),
            receipt=historical_receipt,
            product_key="STOCK_ALERTS",
        )

        first = publisher.publish(
            historical_batch,
            checked_at=NOW,
        )
        resumed = publisher.resume(
            source_receipt_id=historical_receipt.receipt_id,
            checked_at=NOW + timedelta(seconds=1),
            product_key="STOCK_ALERTS",
        )
        replay = publisher.publish(
            historical_batch,
            checked_at=NOW + timedelta(seconds=2),
        )

        for result in (first, resumed, replay):
            assert result.release.release_id == first.release.release_id
            assert result.release.publication_disposition == (
                "HISTORICAL_NONCURRENT"
            )
            assert result.release.deliverable is False
            assert result.pointer is None
            assert result.status.verdict is PortalPublisherVerdict.DEGRADED
            assert result.status.reason_codes == (
                "PORTAL_PUBLICATION_HISTORICAL_NONCURRENT",
            )
        with transaction(pool, read_only=True) as cursor:
            cursor.execute(
                "SELECT state,attempts FROM rt.portal_publication_work "
                "WHERE source_receipt_id=%s",
                (str(historical_receipt.receipt_id),),
            )
            assert cursor.fetchone() == {
                "state": "COMPLETE",
                "attempts": 1,
            }
            cursor.execute(
                "SELECT count(*) AS rows "
                "FROM rt.portal_publication_receipts "
                "WHERE work_id=%s",
                (str(first.status.work_id),),
            )
            assert cursor.fetchone()["rows"] == 1
            cursor.execute(
                "SELECT count(*) AS rows "
                "FROM rt.portal_release_current WHERE cohort_id=%s",
                (str(cohort_id),),
            )
            assert cursor.fetchone()["rows"] == 0

        with transaction(pool) as cursor:
            current_plan = insert_service_plan(
                cursor,
                cohort_id,
                {"STOCK_ALERTS"},
                NEXT_SESSION - timedelta(hours=1),
            )
            current_receipt = _receipt(
                cursor,
                cohort_id,
                current_plan,
                NEXT_SESSION,
                2,
            )
        current_snapshot = _snapshot(
            current_receipt,
            ("AAPL", "MSFT"),
        )
        current = publisher.publish(
            PortalPublicationBatch(
                (current_snapshot,),
                receipt=current_receipt,
                product_key="STOCK_ALERTS",
            ),
            checked_at=NEXT_SESSION,
        )
        assert current.release.pointer_swapped is True
        assert current.pointer.release_id == current.release.release_id

        with transaction(pool) as cursor:
            later_receipt = _receipt(
                cursor,
                cohort_id,
                current_plan,
                NEXT_SESSION + timedelta(minutes=5),
                3,
            )
        later_receipt = replace(
            later_receipt,
            desired_snapshot_sha256=(
                current_receipt.desired_snapshot_sha256
            ),
            membership_sha256=current_receipt.membership_sha256,
        )
        later_snapshot = _snapshot(
            later_receipt,
            ("AAPL", "MSFT"),
            deliverable=False,
        )
        later = publisher.publish(
            PortalPublicationBatch(
                (later_snapshot,),
                receipt=later_receipt,
                product_key="STOCK_ALERTS",
            ),
            checked_at=later_receipt.cutoff,
        )
        assert later.release.publication_disposition == (
            "HISTORICAL_NONCURRENT"
        )
        assert later.pointer.release_id == current.release.release_id
        assert later.release.pointer_swapped is False
        assert later.status.verdict is PortalPublisherVerdict.DEGRADED


def test_c7_ticker_replacement_scope_survives_failure_and_restart():
    database_name = (
        "test_realtime_v2_c7_replacement_" + uuid4().hex[:8]
    )
    owner_id = uuid4()
    with disposable_v2_database(database_name) as pool:
        with transaction(pool) as cursor:
            cohort_id = insert_cohort(cursor, NOW - timedelta(hours=1))
            first_plan = insert_service_plan(
                cursor,
                cohort_id,
                {"PORTAL_PUBLISH"},
                NOW - timedelta(hours=1),
            )
            first_receipt = _receipt(
                cursor,
                cohort_id,
                first_plan,
                NOW,
                1,
            )
        first_receipt = replace(
            first_receipt,
            membership_sha256=_digest({
                "members": ["AAPL", "MSFT"],
            }),
        )
        first_snapshots = (
            _ticker_snapshot(first_receipt, "AAPL", marker="first"),
            _ticker_snapshot(first_receipt, "MSFT", marker="first"),
            _independent_snapshot(first_receipt, marker="first"),
        )
        with transaction(pool) as cursor:
            for snapshot in first_snapshots:
                persist_portal_snapshot_cursor(cursor, snapshot)
            first_release = publish_portal_release_cursor(
                cursor,
                first_receipt,
                first_snapshots,
                replacement_ticker_keys=("AAPL", "MSFT"),
            )

        with transaction(pool) as cursor:
            next_plan = insert_service_plan(
                cursor,
                cohort_id,
                {"PORTAL_PUBLISH"},
                NEXT_SESSION - timedelta(hours=1),
            )
            next_receipt = _receipt(
                cursor,
                cohort_id,
                next_plan,
                NEXT_SESSION,
                2,
            )
        next_receipt = replace(
            next_receipt,
            membership_sha256=_digest({"members": ["AAPL"]}),
        )
        next_aapl = _ticker_snapshot(
            next_receipt,
            "AAPL",
            marker="next",
        )
        with transaction(pool) as cursor:
            persist_portal_snapshot_cursor(cursor, next_aapl)
            work = enqueue_portal_publication_work_cursor(
                cursor,
                next_receipt,
                (next_aapl,),
                product_key="TICKER_WORKSPACES",
                created_at=NEXT_SESSION,
                expected_ticker_keys=("AAPL",),
            )
        assert work["work_document"]["replacement_ticker_keys"] == [
            "AAPL"
        ]
        with transaction(pool) as cursor:
            claimed = _claim(
                cursor,
                work,
                owner_id,
                1,
                NEXT_SESSION,
            )

        with pytest.raises(RuntimeError, match="injected removal failure"):
            with transaction(pool) as cursor:
                durable = load_portal_publication_snapshots_cursor(
                    cursor,
                    claimed,
                )
                publish_portal_release_cursor(
                    cursor,
                    next_receipt,
                    durable,
                    replacement_ticker_keys=claimed[
                        "work_document"
                    ]["replacement_ticker_keys"],
                )
                raise RuntimeError("injected removal failure")
        with transaction(pool) as cursor:
            pointer = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            )
            assert pointer.release_id == first_release.release_id
            fail_portal_publication_work_cursor(
                cursor,
                claimed,
                owner_id=owner_id,
                owner_epoch=int(claimed["lease_epoch"]),
                failed_at=NEXT_SESSION + timedelta(seconds=1),
                next_attempt_at=NEXT_SESSION + timedelta(seconds=2),
                failure_reason="INJECTED_REMOVAL_FAILURE",
            )

        with transaction(pool) as cursor:
            retry_work = load_portal_publication_work_cursor(
                cursor,
                product_key="TICKER_WORKSPACES",
                source_receipt_id=next_receipt.receipt_id,
            )
            retried = _claim(
                cursor,
                retry_work,
                owner_id,
                2,
                NEXT_SESSION + timedelta(seconds=2),
            )
        with transaction(pool) as cursor:
            previous = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                for_update=True,
            )
            durable = load_portal_publication_snapshots_cursor(
                cursor,
                retried,
            )
            next_release = publish_portal_release_cursor(
                cursor,
                next_receipt,
                durable,
                replacement_ticker_keys=retried[
                    "work_document"
                ]["replacement_ticker_keys"],
            )
            current = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=cohort_id,
                provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                for_update=True,
            )
            complete_portal_publication_work_cursor(
                cursor,
                retried,
                next_release,
                owner_id=owner_id,
                owner_epoch=int(retried["lease_epoch"]),
                completed_at=NEXT_SESSION + timedelta(seconds=2),
                previous_release_id=previous.release_id,
                current_release_id=current.release_id,
            )

        identities = {
            (row["snapshot_type"], row["snapshot_key"])
            for row in next_release.manifest["snapshots"]
        }
        assert identities == {
            ("TICKER_PAGE", "AAPL"),
            ("DAILY_STRATEGY_ROLLING_V1", "COHORT"),
        }
        with transaction(pool, read_only=True) as cursor:
            cursor.execute(
                "SELECT manifest FROM rt.portal_releases "
                "WHERE release_id=%s AND cutoff=%s",
                (str(first_release.release_id), first_release.cutoff),
            )
            old_manifest = cursor.fetchone()["manifest"]
            assert (
                "TICKER_PAGE",
                "MSFT",
            ) in {
                (row["snapshot_type"], row["snapshot_key"])
                for row in old_manifest["snapshots"]
            }
        with transaction(pool) as cursor:
            equivalent = publish_portal_release_cursor(
                cursor,
                next_receipt,
                (next_aapl,),
                replacement_ticker_keys=("AAPL",),
            )
            replay = enqueue_portal_publication_work_cursor(
                cursor,
                next_receipt,
                (next_aapl,),
                product_key="TICKER_WORKSPACES",
                created_at=NEXT_SESSION,
                expected_ticker_keys=("AAPL",),
            )
        assert equivalent.release_id == next_release.release_id
        assert equivalent.publication_disposition == "EQUIVALENT_CURRENT"
        assert replay["state"] == "COMPLETE"
        assert replay["work_document"]["replacement_ticker_keys"] == [
            "AAPL"
        ]
