from contextlib import contextmanager
from dataclasses import replace
import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from options.calendar import OptionExchangeCalendar
from realtime import advanced_equity_preparation as preparation
from realtime import advanced_equity_bootstrap as bootstrap
from realtime.advanced_equity_bootstrap import VerifiedEquityBootstrapHistory
from realtime.advanced_equity_preparation import (
    AdvancedEquityPreparationResident,
    EquityPreparationFetchRequest,
    EquityPreparationLimits,
    EquityPreparationStage,
    EquityPreparationUnavailableError,
    MassiveEquityPreparationSource,
)
from realtime.equity_history import (
    EquityHistoryIdentityError,
    EquityHistoryRead,
    equity_history_coordinates,
    load_equity_history_cursor,
)
from tests.realtime.test_advanced_equity_preparation import _claim
from scripts import run_realtime_v2_advanced_equity_preparation as command


UTC = timezone.utc
CHECKED = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
CALENDAR = OptionExchangeCalendar("XNYS")
SESSIONS = (date(2026, 10, 26), date(2026, 10, 27), date(2026, 10, 28))


def _row(session, interval="1d", **changes):
    start, end = equity_history_coordinates(CALENDAR, interval, session)[0]
    values = {
        "bar_revision_id": uuid4(),
        "cohort_id": UUID(int=9),
        "provider_tier": "MASSIVE_DEVELOPER",
        "session_date": session,
        "bar_start": start,
        "bar_end": end,
        "payload_sha256": "a" * 64,
        "source_revision_ids": [],
        "stale_adjustment_lineage": False,
    }
    values.update(changes)
    return values


def _resident(monkeypatch, rows, *, sources=()):
    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=MagicMock())

    def read(_cursor, **scope):
        return EquityHistoryRead(
            tuple(sources if scope["interval"] == "5m" else rows),
            "b" * 64,
        )

    monkeypatch.setattr(bootstrap, "load_equity_history_cursor", read)
    provider = SimpleNamespace(transaction=transaction)
    return AdvancedEquityPreparationResident(
        provider,
        MagicMock(),
        owner_id=uuid4(),
        clock=lambda: CHECKED,
        bootstrap_history=VerifiedEquityBootstrapHistory(provider),
        bootstrap_revision_id=UUID(int=1),
    )


def _adjusted_claim(**changes):
    values = {
        "stage": EquityPreparationStage.ADJUSTED_DAILY,
        "target": {"interval_sessions": {"1d": [str(value) for value in SESSIONS]}},
    }
    values.update(changes)
    return _claim(**values)


def test_adoption_advances_only_the_exact_complete_prefix(monkeypatch):
    rows = (_row(SESSIONS[0]), _row(SESSIONS[2]))
    resident = _resident(monkeypatch, rows)

    adopted, missing = resident.bootstrap_history(_adjusted_claim(), CHECKED)

    assert missing == ()
    assert adopted.watermark_after == {"interval_index": 0, "session_index": 1}
    assert adopted.coverage_after["available_intervals"] == {"1d:ADJUSTED": 1}
    assert adopted.provider_requests == adopted.response_bytes == 0
    assert adopted.bars == ()
    assert not adopted.complete
    assert adopted.source_lineage["adopted_history"]["bars"][0]["cohort_id"] == str(UUID(int=9))
    restarted, missing = resident.bootstrap_history(
        _adjusted_claim(
            watermark=adopted.watermark_after,
            coverage=adopted.coverage_after,
        ),
        CHECKED,
    )
    assert restarted is None
    assert missing == SESSIONS[1:2]


def test_stale_adjustments_and_missing_coordinates_are_not_adopted(monkeypatch):
    rows = tuple(_row(session) for session in SESSIONS)
    rows[0]["stale_adjustment_lineage"] = True
    resident = _resident(monkeypatch, rows)
    adopted, missing = resident.bootstrap_history(_adjusted_claim(), CHECKED)
    assert adopted is None
    assert missing == SESSIONS[:1]

    rows[0]["stale_adjustment_lineage"] = False
    rows[0]["bar_end"] -= timedelta(minutes=1)
    adopted, missing = resident.bootstrap_history(_adjusted_claim(), CHECKED)
    assert adopted is None
    assert missing == SESSIONS[:1]


def test_derived_adoption_requires_the_exact_current_source_closure(monkeypatch):
    session = SESSIONS[0]
    sources = tuple(
        _row(session, bar_start=start, bar_end=end)
        for start, end in equity_history_coordinates(CALENDAR, "5m", session)
    )
    rows = (
        _row(session, source_revision_ids=[row["bar_revision_id"] for row in sources]),
    )
    resident = _resident(monkeypatch, rows, sources=sources)
    claim = _claim(
        stage=EquityPreparationStage.DERIVED_INTERVALS,
        target={"target_intervals": ["1d"], "source_sessions": [str(session)]},
    )
    adopted, _missing = resident.bootstrap_history(claim, CHECKED)
    assert adopted.complete
    assert adopted.coverage_after["available_intervals"] == {"1d:RAW_UNADJUSTED": 1}

    sources[0]["bar_revision_id"] = uuid4()
    adopted, missing = resident.bootstrap_history(claim, CHECKED)
    assert adopted is None
    assert missing == (session,)


def test_stored_adoption_does_not_reserve_or_reset_provider_budget(monkeypatch):
    resident = _resident(monkeypatch, tuple(_row(session) for session in SESSIONS))
    claim = _adjusted_claim()
    monkeypatch.setattr(preparation, "advanced_equity_live_priority_cursor", lambda *_a, **_k: False)
    monkeypatch.setattr(preparation, "claim_equity_preparation_work_cursor", lambda *_a, **_k: claim)
    reserve = MagicMock(side_effect=AssertionError("adoption must not reserve provider effects"))
    monkeypatch.setattr(preparation, "reserve_equity_preparation_provider_requests_cursor", reserve)
    complete = MagicMock(return_value=None)
    monkeypatch.setattr(preparation, "complete_equity_preparation_chunk_cursor", complete)

    result = resident.run_once()

    assert result.status == "COMPLETE"
    assert reserve.call_count == resident.source.fetch.call_count == 0
    request, chunk = complete.call_args.args[1:]
    assert request.provider_reservations == ()
    assert chunk.provider_requests == chunk.response_bytes == 0


def test_native_provider_partial_session_does_not_advance_watermark():
    opened = CALENDAR.session_open(SESSIONS[0])
    client = SimpleNamespace(
        fetch_native_bars=lambda *_a, **_k: ({
            "t": int(opened.timestamp() * 1000),
            "o": 100, "h": 102, "l": 99, "c": 101, "v": 100,
        },),
        chunk_metrics=lambda: (1, 123),
    )
    request = EquityPreparationFetchRequest(
        _claim(target={"interval_sessions": {"1m": [str(SESSIONS[0])]}}),
        CHECKED,
        EquityPreparationLimits(),
    )
    with pytest.raises(EquityPreparationUnavailableError, match="COVERAGE_INCOMPLETE") as failure:
        MassiveEquityPreparationSource(client).fetch(request)
    assert failure.value.provider_requests == 1
    assert failure.value.response_bytes == 123


def test_normal_resident_does_not_enable_bootstrap_implicitly(monkeypatch):
    resident = _resident(monkeypatch, tuple(_row(session) for session in SESSIONS))
    resident.bootstrap_history = None
    resident.bootstrap_revision_id = None
    claim = _adjusted_claim()
    monkeypatch.setattr(preparation, "advanced_equity_live_priority_cursor", lambda *_a, **_k: False)
    monkeypatch.setattr(preparation, "claim_equity_preparation_work_cursor", lambda *_a, **_k: claim)
    reserve = MagicMock(return_value=())
    monkeypatch.setattr(preparation, "reserve_equity_preparation_provider_requests_cursor", reserve)
    monkeypatch.setattr(preparation, "complete_equity_preparation_chunk_cursor", MagicMock())
    resident.source.fetch.return_value = SimpleNamespace(
        complete=True, provider_requests=1, response_bytes=123, source_lineage={},
    )

    result = resident.run_once()

    assert result.status == "COMPLETE"
    assert reserve.call_count == resident.source.fetch.call_count == 1


def test_bootstrap_batches_stored_native_sessions_within_exact_fact_bound(monkeypatch):
    sessions = CALENDAR.trailing_sessions_before(date(2026, 11, 2), 13)
    rows = tuple(
        _row(session, bar_start=start, bar_end=end)
        for session in sessions
        for start, end in equity_history_coordinates(CALENDAR, "5m", session)
    )
    resident = _resident(monkeypatch, rows, sources=rows)
    claim = _claim(target={"interval_sessions": {"5m": [str(value) for value in sessions]}})

    first, _missing = resident.bootstrap_history(claim, CHECKED)
    second, _missing = resident.bootstrap_history(
        replace(claim, watermark=first.watermark_after, coverage=first.coverage_after),
        CHECKED,
    )

    assert first.watermark_after == {"interval_index": 0, "session_index": 12}
    assert len(first.source_lineage["adopted_history"]["bars"]) == 936
    assert second.complete
    assert len(second.source_lineage["adopted_history"]["bars"]) == 78
    assert second.coverage_after["available_intervals"] == {"5m:RAW_UNADJUSTED": 1014}
    assert first.provider_requests == second.provider_requests == 0


def test_bootstrap_cannot_count_a_session_beyond_its_configured_fact_bound(monkeypatch):
    resident = _resident(monkeypatch, ())
    resident.bootstrap_history.limits = EquityPreparationLimits(maximum_facts_per_chunk=10)
    with pytest.raises(EquityHistoryIdentityError, match="configured fact bound"):
        resident.bootstrap_history(
            _claim(target={"interval_sessions": {"5m": [str(SESSIONS[0])]}}),
            CHECKED,
        )


def _bootstrap_arguments(tmp_path):
    return [
        "--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
        "--execute", "--until-idle",
        "--bootstrap-revision-id", str(UUID(int=1)),
        "--bootstrap-authorization-reference", "BOOTSTRAP-TEST",
        "--bootstrap-authorized-until", (CHECKED + timedelta(hours=1)).isoformat(),
        "--bootstrap-maximum-chunks", "1000",
        "--bootstrap-report-path", str(tmp_path / "bootstrap.json"),
    ]


def test_bootstrap_command_requires_explicit_scope_and_unchanged_ceilings(monkeypatch, tmp_path):
    monkeypatch.setattr(command, "_now", lambda: CHECKED)
    arguments = _bootstrap_arguments(tmp_path)
    args = command.parse_args(arguments)
    assert args.bootstrap_revision_id == UUID(int=1)
    assert args.limits.maximum_requests_per_member == 160
    with pytest.raises(SystemExit):
        command.parse_args(arguments + ["--maximum-requests-per-member", "161"])
    with pytest.raises(SystemExit):
        command.parse_args([value for value in arguments if value != "--until-idle"])


@pytest.mark.parametrize("complete", (True, False))
def test_bootstrap_idle_is_success_only_when_durable_preparation_is_complete(
    monkeypatch, tmp_path, complete,
):
    monkeypatch.setattr(command, "_now", lambda: CHECKED)
    args = command.parse_args(_bootstrap_arguments(tmp_path))
    monkeypatch.setattr(command, "_bootstrap_summary", lambda *_a: {
        "revision_current": True, "preparation_complete": complete,
    })
    resident = SimpleNamespace(run_once=lambda: preparation.EquityPreparationRunResult("IDLE", CHECKED))
    stop = SimpleNamespace(is_set=lambda: False)

    status = command._run_bootstrap(args, SimpleNamespace(), resident, stop)

    assert status == (0 if complete else 2)
    report = json.loads(args.bootstrap_report_path.read_text())
    assert report["status"] == ("PREPARATION_COMPLETE" if complete else "INCOMPLETE")
    assert report["consumer_acceptance"] == "NOT_CHECKED"


def test_adopted_chunk_reporting_does_not_claim_provider_io():
    result = preparation.EquityPreparationRunResult(
        "COMPLETE", CHECKED, stage="NATIVE_INTRADAY", stored_history_adopted=True,
    )
    payload = json.loads(command._payload(result))
    assert payload["stored_history_adopted"] is True
    assert payload["provider_fetch_performed"] is False
    assert payload["provider_requests"] == 0


def test_bootstrap_claim_is_pinned_before_work_is_mutated():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(0,), None]
    assert preparation.claim_equity_preparation_work_cursor(
        cursor, owner_id=uuid4(), checked_at=CHECKED,
        limits=EquityPreparationLimits(), revision_id=UUID(int=1),
    ) is None
    statement, values = cursor.execute.call_args.args
    assert "AND work.revision_id=%s" in statement
    assert values[-1] == str(UUID(int=1))
    assert not any(call.args[0].lstrip().startswith("UPDATE") for call in cursor.execute.call_args_list)


def _scope(**changes):
    values = {
        "target_provider_tier": "MASSIVE_ADVANCED",
        "source_cohort_id": UUID(int=9),
        "source_provider_tier": "MASSIVE_DEVELOPER",
        "history_before": CALENDAR.session_open(SESSIONS[1]),
        "source_manifest_sha256": "a" * 64,
        "actual_source_manifest_sha256": "a" * 64,
        "actual_source_provider_tier": "MASSIVE_DEVELOPER",
        "binding_created_at": CHECKED - timedelta(days=1),
        "binding_sha256": "b" * 64,
    }
    values.update(changes)
    return values


@pytest.mark.parametrize("changes", (
    {"actual_source_manifest_sha256": "c" * 64},
    {"actual_source_provider_tier": "MASSIVE_ADVANCED"},
))
def test_history_source_identity_and_observation_clock_fail_closed(changes):
    cursor = MagicMock()
    cursor.fetchone.return_value = _scope(**changes)
    with pytest.raises(EquityHistoryIdentityError, match="diverged"):
        load_equity_history_cursor(
            cursor, cohort_id=UUID(int=4), security_id=UUID(int=2),
            ticker="XLK", interval="1d", adjusted=True,
            sessions=SESSIONS, checked_at=CHECKED,
        )
    assert cursor.execute.call_count == 1


def test_future_binding_is_not_used_to_resolve_as_of_history():
    cursor = MagicMock()
    cursor.fetchone.return_value = _scope(binding_created_at=CHECKED + timedelta(seconds=1))
    cursor.fetchall.return_value = []
    history = load_equity_history_cursor(
        cursor, cohort_id=UUID(int=4), security_id=UUID(int=2),
        ticker="XLK", interval="1d", adjusted=True,
        sessions=SESSIONS, checked_at=CHECKED,
    )
    statement, values = cursor.execute.call_args.args
    assert "revision.bar_end<%s" not in statement
    assert str(UUID(int=9)) not in values
    assert history.binding_sha256 is None
    assert history.rows == ()


def test_history_read_retains_binding_cutoff_and_causal_predicates():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [_scope(), _scope(source_cohort_id=None)]
    cursor.fetchall.return_value = []
    history = load_equity_history_cursor(
        cursor, cohort_id=UUID(int=4), security_id=UUID(int=2),
        ticker="XLK", interval="1d", adjusted=True,
        sessions=SESSIONS, checked_at=CHECKED, source_kind="NATIVE_REST",
        adjustments_through=date(2026, 10, 30),
    )
    statement, arguments = cursor.execute.call_args.args
    assert "revision.bar_end<%s" in statement
    assert "revision.source_kind=%s" in statement
    assert "revision.created_at<=%s" in statement
    assert "revision.replay_available_at<=%s" in statement
    assert "revision.provider_published_at<=%s" in statement
    assert "catalog.observed_at=revision.observed_at" in statement
    assert "NATIVE_REST" in arguments
    assert arguments[0] == date(2026, 10, 30)
    assert history.binding_sha256 == "b" * 64


@pytest.mark.parametrize("stale", (False, True))
def test_divergent_fresh_sources_fail_but_stale_adjustments_cannot_mask_repair(stale):
    base = _row(SESSIONS[0], stale_adjustment_lineage=stale)
    base.update({
        "security_id": UUID(int=2), "ticker": "XLK", "source_kind": "NATIVE_REST",
        "open_price": 100, "high_price": 101, "low_price": 99,
        "close_price": 100, "volume": 1000, "vwap": 100, "transaction_count": 10,
    })
    repaired = {
        **base, "cohort_id": UUID(int=4), "provider_tier": "MASSIVE_ADVANCED",
        "payload_sha256": "c" * 64, "stale_adjustment_lineage": False,
    }
    cursor = MagicMock()
    cursor.fetchone.side_effect = [_scope(), _scope(source_cohort_id=None)]
    cursor.fetchall.return_value = [base, repaired]
    arguments = dict(
        cohort_id=UUID(int=4), security_id=UUID(int=2), ticker="XLK",
        interval="1d", adjusted=True, sessions=SESSIONS, checked_at=CHECKED,
    )
    if stale:
        history = load_equity_history_cursor(cursor, **arguments)
        assert history.rows == (repaired,)
    else:
        with pytest.raises(EquityHistoryIdentityError, match="divergent"):
            load_equity_history_cursor(cursor, **arguments)


@pytest.mark.parametrize("old_proof,new_proof", ((False, False), (False, True), (True, False), (True, True)))
def test_weekly_source_replacement_requires_unique_immutable_c4_revalidation(old_proof, new_proof):
    session = date(2026, 10, 23)
    original = _row(
        session, "1wk", security_id=UUID(int=2), ticker="XLK", source_kind="NATIVE_REST",
        open_price=100, high_price=101, low_price=99, close_price=100, volume=1000,
        vwap=100, transaction_count=10, weekly_coordinate_revalidated=old_proof,
    )
    corrected = {
        **original, "cohort_id": UUID(int=4), "provider_tier": "MASSIVE_ADVANCED",
        "bar_revision_id": uuid4(), "close_price": 101, "payload_sha256": "c" * 64,
        "weekly_coordinate_revalidated": new_proof,
    }
    cursor = MagicMock()
    cursor.fetchone.side_effect = [_scope(), _scope(source_cohort_id=None)]
    cursor.fetchall.return_value = [original, corrected]
    arguments = dict(
        cohort_id=UUID(int=4), security_id=UUID(int=2), ticker="XLK", interval="1wk",
        adjusted=True, sessions=(session,), checked_at=CHECKED, source_kind="NATIVE_REST",
    )
    if old_proof == new_proof:
        with pytest.raises(EquityHistoryIdentityError, match="divergent"):
            load_equity_history_cursor(cursor, **arguments)
    else:
        history = load_equity_history_cursor(cursor, **arguments)
        assert history.rows == ((corrected if new_proof else original),)
    statement = cursor.execute.call_args.args[0]
    assert "chunk.committed_at=revision.observed_at" in statement
    assert "chunk.created_at<=%s" in statement
    assert "desired.source_cohort_id=revision.cohort_id" in statement
    assert "MASSIVE_WEEK_SUNDAY_START_V2" in statement
    assert "whole_interval_revalidated" in statement


def test_short_session_coordinates_preserve_the_final_hourly_closure():
    coordinates = equity_history_coordinates(CALENDAR, "1h", date(2026, 11, 27))
    assert len(coordinates) == 4
    assert coordinates[-1][1] - coordinates[-1][0] == timedelta(minutes=30)
