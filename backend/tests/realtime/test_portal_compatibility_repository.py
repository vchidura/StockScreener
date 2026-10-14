from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.portal_compatibility_repository import (
    BoundedCache,
    RealtimeOptionAlertRepository,
    RealtimeStockAlertRepository,
    RealtimeTickerScreenerRepository,
    load_release,
)
from tests.realtime.portal_v2_support import release_rows


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, tzinfo=UTC)
COHORT = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SNAPSHOT = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _factory(cursor):
    @contextmanager
    def context():
        yield cursor
    return context


def _active():
    return {
        "cohort_id": COHORT,
        "provider_tier": "MASSIVE_DEVELOPER",
        "effective_at": NOW - timedelta(hours=1),
    }


def _release(snapshot_type, snapshot_key):
    entry = {"snapshot_id": str(SNAPSHOT), "snapshot_class": "EPHEMERAL_PORTAL",
             "snapshot_type": snapshot_type, "snapshot_key": snapshot_key,
             "source_generation": 7, "generated_at": NOW.isoformat()}
    return release_rows(UUID(int=77), COHORT, [entry], cutoff=NOW, session_date=NOW.date())


def test_ticker_page_reads_only_the_current_release_snapshot_without_provider_fetch():
    cursor = MagicMock()
    pointer, release = _release("TICKER_PAGE", "SPY")
    cursor.fetchone.side_effect = (
        _active(),
        pointer,
        release,
        {
            "snapshot_id": SNAPSHOT,
            "payload": {"ticker": "SPY", "setups": []},
            "generated_at": NOW,
            "valid_until": NOW + timedelta(hours=1),
            "source_generation": 7,
            "source_manifest_sha256": "c" * 64,
        },
    )
    repository = RealtimeTickerScreenerRepository(_factory(cursor))

    result = repository.ticker_page("spy", available_by=NOW)

    assert result["payload"]["ticker"] == "SPY"
    assert result["provider_tier"] == "MASSIVE_DEVELOPER"
    assert result["provider_fetch_performed"] is False
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("rt.portal_cohort_current" in statement for statement in statements)
    assert any("FROM rt.portal_release_current" in statement for statement in statements)
    assert any("FROM rt.portal_releases" in statement for statement in statements)
    body = next(call for call in cursor.execute.call_args_list if "rt.portal_snapshot_hot" in call.args[0])
    assert "rt.portal_snapshot_kept" in body.args[0] and "snapshot.snapshot_id=%s" in body.args[0]
    assert body.args[1][:3] == (str(SNAPSHOT), str(COHORT), "SPY")
    assert not any("portal_snapshots" in statement or "portal_snapshot_current" in statement
                   for statement in statements)


def test_ticker_page_is_absent_when_the_current_release_does_not_name_it():
    cursor = MagicMock()
    pointer, release = _release("TICKER_PAGE", "AAPL")
    cursor.fetchone.side_effect = (_active(), pointer, release)
    repository = RealtimeTickerScreenerRepository(_factory(cursor))

    assert repository.ticker_page("SPY", available_by=NOW) is None
    cursor.fetchone.side_effect = (_active(), None)
    assert repository.ticker_page("SPY", available_by=NOW) is None


def test_ticker_page_can_read_ready_inert_cohort_without_active_pointer():
    cursor = MagicMock()
    pointer, release = _release("TICKER_PAGE", "SPY")
    cursor.fetchone.side_effect = (
        _active(),
        pointer,
        release,
        {
            "snapshot_id": SNAPSHOT,
            "payload": {"ticker": "SPY", "setups": []},
            "generated_at": NOW,
            "valid_until": NOW + timedelta(hours=1),
            "source_generation": 7,
            "source_manifest_sha256": "c" * 64,
        },
    )
    repository = RealtimeTickerScreenerRepository(_factory(cursor))

    result = repository.ticker_page(
        "SPY",
        available_by=NOW,
        cohort_id=COHORT,
    )

    assert result["payload"]["ticker"] == "SPY"
    identity_query = cursor.execute.call_args_list[2].args[0]
    assert "FROM rt.screening_current AS current" in identity_query
    assert "JOIN rt.screening_generations AS generation" in identity_query
    assert "rt.cohorts" not in identity_query
    assert "rt.subscription_manifests" not in identity_query
    assert not any(
        "rt.portal_cohort_current" in call.args[0]
        for call in cursor.execute.call_args_list
    )


def test_current_release_is_cached_by_immutable_release_identity():
    cursor = MagicMock()
    pointer, release = _release("TICKER_PAGE", "SPY")
    row = {
        "snapshot_id": SNAPSHOT, "payload": {"ticker": "SPY"}, "generated_at": NOW,
        "valid_until": None, "source_generation": 7, "source_manifest_sha256": "c" * 64,
    }
    cursor.fetchone.side_effect = (_active(), pointer, release, row, _active(), pointer, row)
    repository = RealtimeTickerScreenerRepository(_factory(cursor))

    repository.ticker_page("SPY", available_by=NOW)
    repository.ticker_page("SPY", available_by=NOW)

    manifest_reads = [call for call in cursor.execute.call_args_list if "FROM rt.portal_releases" in call.args[0]]
    assert len(manifest_reads) == 1


def test_release_manifest_is_hash_checked_and_the_release_cache_is_bounded():
    _, release = _release("TICKER_PAGE", "SPY")
    cursor = MagicMock()
    cursor.fetchone.return_value = release
    cache = BoundedCache(2)

    view = load_release(cursor, release["release_id"], NOW, cache)
    assert view.entry("TICKER_PAGE", "SPY")["snapshot_id"] == str(SNAPSHOT)
    assert view.entries_of("TICKER_PAGE") == (view.entry("TICKER_PAGE", "SPY"),)
    assert load_release(cursor, release["release_id"], NOW, cache) is view
    assert cursor.execute.call_count == 1

    tampered = dict(release, manifest=dict(release["manifest"], session_date="2020-01-01"))
    cursor.fetchone.return_value = tampered
    with pytest.raises(ValueError, match="checksum drift"):
        load_release(cursor, UUID(int=1), NOW)
    cursor.fetchone.return_value = None
    with pytest.raises(ValueError, match="not retained"):
        load_release(cursor, UUID(int=2), NOW)

    for index in range(4):
        cache.put(index, index)
    assert len(cache) == 2 and cache.get(0) is None and cache.get(3) == 3
    with pytest.raises(ValueError):
        BoundedCache(0)


def test_release_etag_follows_release_identity_and_availability_epoch():
    cursor = MagicMock()
    pointer, release = _release("TICKER_PAGE", "SPY")
    cursor.fetchone.side_effect = (pointer, release, pointer, pointer, None)
    repository = RealtimeTickerScreenerRepository(_factory(cursor))

    first = repository.release_etag(available_by=NOW, cohort_id=COHORT)
    assert first == repository.release_etag(available_by=NOW + timedelta(minutes=1), cohort_id=COHORT)
    assert first != repository.release_etag(available_by=NOW + timedelta(days=3), cohort_id=COHORT)
    assert repository.release_etag(available_by=NOW, cohort_id=COHORT) is None
    assert first.startswith('"') and first.endswith('"')


def test_screener_preserves_generation_manifest_and_projected_rows():
    generation_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        _active(),
        {
            "generation_id": generation_id,
            "session_date": date(2026, 10, 5),
            "status": "COMPLETE",
            "generated_at": NOW,
            "manifest": {"source_generation": 7},
            "expected_members": 1,
            "actual_members": 1,
        },
    )
    cursor.fetchall.return_value = ({
        "security_id": UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        "ticker": "SPY",
        "eligible": True,
        "payload": {"values": {"close": 600.0}, "missing": {}},
    },)
    repository = RealtimeTickerScreenerRepository(_factory(cursor))

    result = repository.build_latest(available_by=NOW)

    assert result["available"] is True
    assert result["rows"][0]["ticker"] == "SPY"
    assert result["manifest"] == {"source_generation": 7}


def test_stock_alert_view_preserves_alert_and_schedule_contract():
    cursor = MagicMock()
    pointer, release = _release("STOCK_ALERT_CONTEXT", "PLATFORM")
    cursor.fetchone.side_effect = (
        _active(),
        pointer,
        release,
        {"payload": {
            "schedule_streams": [{"stream": "INTRADAY"}],
            "sessions": ["2026-10-05"],
            "observations": [{"ticker": "SPY", "detector_id": "S1"}],
        }},
    )
    cursor.fetchall.return_value = ({
        "projection": {"alert_id": "alert-1", "ticker": "SPY"},
        "as_of": NOW,
        "valid_until": NOW + timedelta(hours=1),
        "lifecycle": "OPEN",
    },)
    repository = RealtimeStockAlertRepository(_factory(cursor))

    result = repository.alert_view(
        available_by=NOW,
        session_date=date(2026, 10, 5),
    )

    assert result["alerts"][0]["ticker"] == "SPY"
    assert result["schedule_streams"] == [{"stream": "INTRADAY"}]
    assert result["observations"] == [{"ticker": "SPY", "detector_id": "S1"}]
    assert result["source"] == "REALTIME"
    alert_statement = next(
        call.args[0] for call in cursor.execute.call_args_list
        if "FROM rt.alert_current" in call.args[0]
    )
    assert "as_of::date" not in alert_statement
    assert "AT TIME ZONE 'America/New_York'" in alert_statement


def test_option_alert_evaluations_preserve_envelope_and_fail_closed_management():
    dataset_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        _active(),
        {
            "dataset_id": dataset_id,
            "dataset_key": "options-canary-2026-10-05",
            "session_date": date(2026, 10, 5),
            "status": "COMPLETE",
            "detector_ids": ["O1"],
            "started_at": NOW - timedelta(minutes=1),
            "completed_at": NOW,
            "summary": {"evaluated": 1},
            "source_manifest_sha256": "d" * 64,
        },
    )
    cursor.fetchall.return_value = ({
        "evaluation_id": UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        "event_id": UUID("dddddddd-dddd-dddd-dddd-dddddddddddd"),
        "detector_id": "O1",
        "underlyer": "SPY",
        "selection_status": "OBSERVATION",
        "observed_at": NOW,
        "payload": {"category": "NEUTRAL_FLOW"},
    },)
    repository = RealtimeOptionAlertRepository(_factory(cursor))

    result = repository.option_alert_evaluations(available_by=NOW)

    assert result["available"] is True
    assert result["data_tier"] == "MASSIVE_DEVELOPER"
    assert result["data"]["evaluations"][0]["detector_id"] == "O1"
    assert result["data"]["management_status_ready"] is False
    assert result["data"]["provider_fetch_performed"] is False
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert not any("IS NULL OR detector_id" in statement for statement in statements)