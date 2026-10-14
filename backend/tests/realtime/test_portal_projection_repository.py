from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.contracts import canonical_sha256
from realtime.portal_projection import build_portal_snapshot
from realtime.portal_projection_repository import (
    PortalReleaseReceipt,
    RELEASE_CLASS_SESSION_CLOSE,
    RETENTION_HOT_AND_KEPT,
    RETENTION_KEPT,
    claim_alert_outbox,
    claim_alert_outbox_cursor,
    fail_alert_outbox_delivery,
    mark_alert_outbox_delivered,
    persist_portal_snapshot,
    persist_portal_snapshot_cursor,
    portal_session_date,
    publish_portal_release_cursor,
    session_close_snapshot,
)
from realtime.preflight import ProviderTier


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, tzinfo=UTC)
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OWNER = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
RECEIPT = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")


def _snapshot(key="SPY", generation=7, *, payload=None, manifest=None, generated_at=NOW, snapshot_type="TICKER_PAGE"):
    return build_portal_snapshot(
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        snapshot_type=snapshot_type,
        snapshot_key=key,
        source_generation=generation,
        source_manifest=manifest or {"bars": ["1d", "1wk"]},
        payload=payload or {"ticker": key, "setups": []},
        generated_at=generated_at,
        valid_until=generated_at + timedelta(hours=1),
    )


def _catalog_row(snapshot, snapshot_class="EPHEMERAL_PORTAL"):
    return (
        snapshot.snapshot_id, snapshot.cohort_id, snapshot.provider_tier.value, snapshot_class,
        snapshot.snapshot_type, snapshot.snapshot_key, snapshot.source_generation,
        snapshot.source_manifest_sha256, snapshot.payload_sha256, snapshot.generated_at,
        NOW.date(),
    )


def _statements(cursor):
    return [call.args[0] for call in cursor.execute.call_args_list]


def test_hot_snapshot_writes_catalog_then_hot_body_and_never_a_pointer():
    snapshot = _snapshot()
    cursor = MagicMock()
    cursor.fetchone.return_value = _catalog_row(snapshot)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    assert persist_portal_snapshot(connection, snapshot) is None

    statements = _statements(cursor)
    assert statements[0].startswith("INSERT INTO rt.snapshot_catalog")
    assert "ON CONFLICT (snapshot_id) DO NOTHING" in statements[0]
    assert statements[2].startswith("INSERT INTO rt.portal_snapshot_hot")
    assert not any("portal_snapshots" in sql or "portal_snapshot_current" in sql
                   or "portal_release_current" in sql or "portal_snapshot_kept" in sql for sql in statements)
    catalog_values = cursor.execute.call_args_list[0].args[1]
    assert catalog_values[3] == "EPHEMERAL_PORTAL"
    assert catalog_values[6] == NOW.date()
    hot_values = cursor.execute.call_args_list[2].args[1]
    assert hot_values[1] == NOW and '"ticker":"SPY"' in hot_values[6].replace(" ", "")
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_cursor_snapshot_persistence_composes_without_owning_transaction():
    snapshot = _snapshot(payload={"ticker": "SPY"}, manifest={"bars": ["1d"]})
    cursor = MagicMock()
    cursor.fetchone.return_value = _catalog_row(snapshot)

    persist_portal_snapshot_cursor(cursor, snapshot)

    assert _statements(cursor)[0].startswith("INSERT INTO rt.snapshot_catalog")


def test_snapshot_identity_conflict_rolls_back_and_writes_no_body():
    snapshot = _snapshot()
    cursor = MagicMock()
    conflicting = list(_catalog_row(snapshot))
    conflicting[8] = "f" * 64
    cursor.fetchone.return_value = tuple(conflicting)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with pytest.raises(ValueError, match="identity conflict"):
        persist_portal_snapshot(connection, snapshot)

    assert not any("portal_snapshot_hot" in sql for sql in _statements(cursor) if sql.startswith("INSERT"))
    assert connection.rollback.call_count == 1 and connection.commit.call_count == 0


def test_hot_manifest_over_the_lineage_head_limit_is_rejected_before_any_sql():
    snapshot = _snapshot(manifest={"blob": "x" * 20000})
    cursor = MagicMock()
    with pytest.raises(ValueError, match="lineage-head limit"):
        persist_portal_snapshot_cursor(cursor, snapshot)
    assert cursor.execute.call_count == 0
    kept = _snapshot(manifest={"blob": "x" * 20000})
    cursor.fetchone.return_value = _catalog_row(kept, "SESSION_CLOSE")
    persist_portal_snapshot_cursor(cursor, kept, retention=RETENTION_KEPT)
    assert any(sql.startswith("INSERT INTO rt.portal_snapshot_kept") for sql in _statements(cursor))


def test_hot_manifest_limit_matches_the_schema_check():
    from pathlib import Path
    from realtime.portal_projection_repository import HOT_MANIFEST_LIMIT_BYTES

    schema = Path(__file__).parents[2].joinpath(
        "realtime", "sql_v2", "007_realtime_portal.sql").read_text(encoding="utf-8")
    assert f"octet_length(source_manifest::text) <= {HOT_MANIFEST_LIMIT_BYTES}" in schema


def test_session_close_variant_is_a_distinct_self_contained_kept_body():
    snapshot = _snapshot("PLATFORM", snapshot_type="TICKER_WORKSPACE_SUMMARY_V1",
                         payload={"session": "2026-10-05", "overview": []})
    kept = session_close_snapshot(snapshot)
    assert kept.snapshot_id != snapshot.snapshot_id
    assert (kept.payload_sha256, kept.source_manifest_sha256) == (snapshot.payload_sha256, snapshot.source_manifest_sha256)
    assert session_close_snapshot(snapshot).snapshot_id == kept.snapshot_id
    cursor = MagicMock()
    cursor.fetchone.side_effect = (_catalog_row(snapshot), _catalog_row(kept, "SESSION_CLOSE"))

    returned = persist_portal_snapshot_cursor(cursor, snapshot, retention=RETENTION_HOT_AND_KEPT)

    assert returned == kept
    inserts = [sql for sql in _statements(cursor) if sql.startswith("INSERT")]
    assert [sql.split(" (")[0] for sql in inserts] == [
        "INSERT INTO rt.snapshot_catalog", "INSERT INTO rt.portal_snapshot_hot",
        "INSERT INTO rt.snapshot_catalog", "INSERT INTO rt.portal_snapshot_kept",
    ]
    kept_values = [call.args[1] for call in cursor.execute.call_args_list
                   if call.args[0].startswith("INSERT INTO rt.snapshot_catalog")][1]
    assert kept_values[0] == str(kept.snapshot_id) and kept_values[3] == "SESSION_CLOSE"
    assert kept_values[6] == NOW.date()
    assert portal_session_date(snapshot) == NOW.date()


def test_kept_only_retention_keeps_the_original_identity_and_session_from_the_payload():
    snapshot = _snapshot("COHORT", snapshot_type="DAILY_STRATEGY_ROLLING_V1",
                         payload={"session": "2026-10-02"}, generated_at=NOW)
    cursor = MagicMock()
    cursor.fetchone.return_value = _catalog_row(snapshot, "SESSION_CLOSE")

    assert persist_portal_snapshot_cursor(cursor, snapshot, retention=RETENTION_KEPT) == snapshot

    inserts = [sql for sql in _statements(cursor) if sql.startswith("INSERT")]
    assert len(inserts) == 2 and "portal_snapshot_kept" in inserts[1]
    assert cursor.execute.call_args_list[0].args[1][6].isoformat() == "2026-10-02"
    with pytest.raises(ValueError, match="retention"):
        persist_portal_snapshot_cursor(cursor, snapshot, retention="FOREVER")
    with pytest.raises(ValueError, match="requires hot and kept"):
        persist_portal_snapshot_cursor(cursor, snapshot, kept_snapshot=session_close_snapshot(snapshot))


def _receipt(cutoff=NOW):
    return PortalReleaseReceipt(RECEIPT, COHORT, ProviderTier.MASSIVE_DEVELOPER, cutoff, "9" * 64)


def _recording_cursor(snapshots, *, classes=None, previous=None, swapped=True):
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (row.snapshot_id, (classes or {}).get(row.snapshot_key, "EPHEMERAL_PORTAL"),
         row.source_manifest_sha256, row.payload_sha256, row.generated_at)
        for row in snapshots
    ]
    calls, state = [], {}

    def execute(statement, parameters=()):
        calls.append((statement, parameters))
        state["statement"] = statement
        if statement.startswith("INSERT INTO rt.portal_releases"):
            state["release"] = parameters

    def fetchone():
        statement = state["statement"]
        if "FOR UPDATE OF pointer" in statement:
            return previous
        if statement.startswith("SELECT release_id, cohort_id"):
            values = state["release"]
            return (*values[:8], values[9])
        if "RETURNING release_id" in statement:
            return (state["release"][0],) if swapped else None
        return None

    cursor.execute = MagicMock(side_effect=execute)
    cursor.fetchone.side_effect = fetchone
    return cursor, calls


def test_release_publication_swaps_the_single_pointer_after_the_immutable_release():
    snapshots = [_snapshot("SPY"), _snapshot("AAPL")]
    cursor, calls = _recording_cursor(snapshots)

    release = publish_portal_release_cursor(cursor, _receipt(), snapshots)

    statements = [statement for statement, _ in calls]
    assert statements[0].startswith("SELECT pg_advisory_xact_lock(hashtextextended")
    assert [s.split(" (")[0].split(" FROM")[0] for s in statements if s.startswith("INSERT")] == [
        "INSERT INTO rt.portal_releases", "INSERT INTO rt.portal_release_current",
    ]
    assert "WHERE rt.portal_release_current.cutoff <= EXCLUDED.cutoff" in statements[-1]
    assert release.pointer_swapped is True and release.release_class == "INTRADAY"
    assert [entry["snapshot_key"] for entry in release.manifest["snapshots"]] == ["AAPL", "SPY"]
    assert release.manifest_sha256 == canonical_sha256(release.manifest)
    inserted = next(params for statement, params in calls if statement.startswith("INSERT INTO rt.portal_releases"))
    assert inserted[0] == str(release.release_id) and inserted[4] == str(RECEIPT) and inserted[6] == NOW
    assert inserted[5].isoformat() == "2026-10-05"
    pointer = next(params for statement, params in calls if statement.startswith("INSERT INTO rt.portal_release_current"))
    assert pointer == (str(COHORT), "MASSIVE_DEVELOPER", str(release.release_id), NOW, release.session_date)


def test_intraday_release_carries_forward_other_lanes_and_keeps_newer_generations():
    older_strategy = {"snapshot_id": str(UUID(int=1)), "snapshot_class": "SESSION_CLOSE",
                      "snapshot_type": "DAILY_STRATEGY_ROLLING_V1", "snapshot_key": "COHORT",
                      "source_generation": 3, "generated_at": NOW.isoformat()}
    newer_ticker = {"snapshot_id": str(UUID(int=2)), "snapshot_class": "EPHEMERAL_PORTAL",
                    "snapshot_type": "TICKER_PAGE", "snapshot_key": "SPY",
                    "source_generation": 9, "generated_at": NOW.isoformat()}
    previous = (NOW - timedelta(minutes=5), {"snapshots": [older_strategy, newer_ticker]})
    snapshots = [_snapshot("SPY", generation=7)]
    cursor, calls = _recording_cursor(snapshots, previous=previous)

    release = publish_portal_release_cursor(cursor, _receipt(), snapshots)

    by_identity = {(row["snapshot_type"], row["snapshot_key"]): row for row in release.manifest["snapshots"]}
    assert set(by_identity) == {("DAILY_STRATEGY_ROLLING_V1", "COHORT"), ("TICKER_PAGE", "SPY")}
    assert by_identity[("DAILY_STRATEGY_ROLLING_V1", "COHORT")]["snapshot_id"] == str(UUID(int=1))
    assert by_identity[("TICKER_PAGE", "SPY")]["snapshot_id"] == str(UUID(int=2))
    replay_cursor, _ = _recording_cursor(snapshots, previous=previous)
    assert publish_portal_release_cursor(replay_cursor, _receipt(), snapshots).release_id == release.release_id


def test_stale_cutoff_does_not_swap_the_pointer():
    snapshots = [_snapshot("SPY")]
    cursor, _ = _recording_cursor(snapshots, swapped=False)
    assert publish_portal_release_cursor(cursor, _receipt(), snapshots).pointer_swapped is False


def test_session_close_release_never_touches_the_pointer_and_requires_kept_snapshots():
    summary = _snapshot("PLATFORM", snapshot_type="TICKER_WORKSPACE_SUMMARY_V1", payload={"session": "2026-10-05"})
    kept = session_close_snapshot(summary)
    cursor, calls = _recording_cursor([kept], classes={"PLATFORM": "SESSION_CLOSE"})

    release = publish_portal_release_cursor(
        cursor, _receipt(), [kept], release_class=RELEASE_CLASS_SESSION_CLOSE, session_date=NOW.date())

    statements = [statement for statement, _ in calls]
    assert release.pointer_swapped is False and release.session_date == NOW.date()
    assert not any("portal_release_current" in statement for statement in statements)
    assert release.manifest["release_class"] == "SESSION_CLOSE"
    hot_cursor, _ = _recording_cursor([summary])
    with pytest.raises(ValueError, match="not persisted as published"):
        publish_portal_release_cursor(
            hot_cursor, _receipt(), [summary], release_class=RELEASE_CLASS_SESSION_CLOSE, session_date=NOW.date())
    with pytest.raises(ValueError, match="requires its session date"):
        publish_portal_release_cursor(cursor, _receipt(), [kept], release_class=RELEASE_CLASS_SESSION_CLOSE)


def test_release_rejects_unpersisted_foreign_or_duplicate_snapshots():
    snapshots = [_snapshot("SPY")]
    empty = MagicMock()
    empty.fetchall.return_value = []
    with pytest.raises(ValueError, match="not persisted as published"):
        publish_portal_release_cursor(empty, _receipt(), snapshots)
    with pytest.raises(ValueError, match="at least one snapshot"):
        publish_portal_release_cursor(MagicMock(), _receipt(), [])
    with pytest.raises(ValueError, match="identities must be unique"):
        publish_portal_release_cursor(MagicMock(), _receipt(), [snapshots[0], snapshots[0]])
    foreign = replace(snapshots[0], cohort_id=OWNER)
    with pytest.raises(ValueError, match="conflict with the receipt identity"):
        publish_portal_release_cursor(MagicMock(), _receipt(), [foreign])
    with pytest.raises(ValueError, match="timezone-aware"):
        PortalReleaseReceipt(RECEIPT, COHORT, ProviderTier.MASSIVE_DEVELOPER, datetime(2026, 10, 5), "9" * 64)


def test_outbox_claim_requires_lease_active_cohort_and_ready_epoch():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (str(OWNER), 4, True),
        (str(COHORT),),
        (4, "READY", True),
    )
    cursor.fetchall.return_value = ((
        UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        UUID("dddddddd-dddd-dddd-dddd-dddddddddddd"),
        "PORTAL",
        "ALERT_UPSERT",
        "e" * 64,
        {"alert_id": "test"},
        1,
    ),)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    rows = claim_alert_outbox(
        connection,
        cohort_id=COHORT,
        owner_id=OWNER,
        owner_epoch=4,
        claimed_at=NOW,
        lease_until=NOW + timedelta(seconds=30),
        limit=10,
    )

    assert rows[0]["destination"] == "PORTAL"
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("rt.portal_cohort_current" in statement for statement in statements)
    assert any("capability='PUBLICATION'" in statement for statement in statements)
    assert any("abandoned_at" in statement for statement in statements)
    assert any("ALERT_EXPIRED" in statement for statement in statements)
    assert any("event.valid_until IS NULL" in statement for statement in statements)
    assert any(
        "FOR UPDATE OF outbox SKIP LOCKED" in statement
        for statement in statements
    )
    assert connection.commit.call_count == 1


def test_outbox_claim_rejects_nonactive_cohort_before_claim_sql():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (str(OWNER), 4, True),
        (str(UUID(int=0)),),
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with pytest.raises(ValueError, match="not the active"):
        claim_alert_outbox(
            connection,
            cohort_id=COHORT,
            owner_id=OWNER,
            owner_epoch=4,
            claimed_at=NOW,
            lease_until=NOW + timedelta(seconds=30),
            limit=10,
        )

    assert not any(
        statement.startswith("WITH pending")
        for statement in (call.args[0] for call in cursor.execute.call_args_list)
    )
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_outbox_delivery_acknowledgement_is_lease_fenced():
    outbox_id = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    cursor = MagicMock()
    cursor.fetchone.return_value = (outbox_id,)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    mark_alert_outbox_delivered(
        connection,
        outbox_id=outbox_id,
        owner_id=OWNER,
        owner_epoch=4,
        delivered_at=NOW,
    )

    statement = cursor.execute.call_args.args[0]
    assert "delivered_at=%s" in statement
    assert "lease_owner=NULL" in statement
    assert connection.commit.call_count == 1


@pytest.mark.parametrize(
    ("retryable", "expected"),
    ((True, "PENDING"), (False, "ABANDONED")),
)
def test_outbox_failure_retries_or_abandons_under_lease_fence(
    retryable,
    expected,
):
    outbox_id = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (OWNER, 4, True),
        (outbox_id,),
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    status = fail_alert_outbox_delivery(
        connection,
        outbox_id=outbox_id,
        owner_id=OWNER,
        owner_epoch=4,
        failed_at=NOW,
        next_attempt_at=NOW + timedelta(seconds=30),
        failure_code="delivery_failed",
        retryable=retryable,
    )

    assert status == expected
    assert "abandoned_at=CASE" in cursor.execute.call_args_list[-1].args[0]
    assert connection.commit.call_count == 1


def test_cursor_outbox_claim_composes_without_owning_transaction():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (str(OWNER), 4, True),
        (str(COHORT),),
        (4, "READY", True),
    )
    cursor.fetchall.return_value = ()

    rows = claim_alert_outbox_cursor(
        cursor,
        cohort_id=COHORT,
        owner_id=OWNER,
        owner_epoch=4,
        claimed_at=NOW,
        lease_until=NOW + timedelta(seconds=30),
        limit=10,
    )

    assert rows == ()
    assert any(
        "FOR UPDATE OF outbox SKIP LOCKED" in call.args[0]
        for call in cursor.execute.call_args_list
    )