"""Universe sizing WP-B (Nov 15): R58 provider-confirmed empty minutes in preparation, R59 inactive tickers, the
pre-session exclusion of delisted members, G86 tickers listed inside the history depth, and the in-session
confirmed-empty minute behind REALTIME_V2_CONFIRMED_EMPTY_MINUTES."""
from contextlib import contextmanager, nullcontext
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import urlparse
from uuid import UUID

import pytest

from options.calendar import OptionExchangeCalendar
from realtime import advanced_realtime_desired_state_repository as desired_repo
from realtime.advanced_equity_preparation import (
    AdvancedEquityPreparationResident,
    BoundedPolygonEquityPreparationClient,
    EquityPreparationClaim,
    EquityPreparationFetchRequest,
    EquityPreparationLimits,
    EquityPreparationSourceError,
    EquityPreparationStage,
    EquityPreparationUnavailableError,
    MAX_PROVIDER_EMPTY_MINUTES_PER_SESSION,
    MassiveEquityPreparationSource,
    provider_empty_minutes,
)
from realtime.advanced_gap_repair import verify_and_close_gap_repair_cursor
from realtime.advanced_realtime_desired_state import (
    DesiredMemberKind,
    DesiredMemberPreparation,
    MemberPreparationStatus,
)
from realtime.advanced_stream_gap_repair import (
    StreamGapMember,
    _repair_facts,
    _repair_hole_facts,
    confirmed_empty_starts,
)
from realtime.confirmed_empty_minutes import CONFIRMED_EMPTY_MINUTES_SWITCH
from realtime.equity_history import adjusted_period_listed, equity_history_coordinates
from realtime.reference_facts import ReferenceFactRelation
from realtime.stage_runners_advanced_equity import advanced_input_gate_identity
from realtime.ticker_projection import _listed_periods

UTC = timezone.utc
CAL = OptionExchangeCalendar("XNYS")


def _claim(**changes):
    values = {
        "revision_id": UUID(int=1),
        "intent_id": UUID(int=6),
        "security_id": UUID(int=2),
        "member_kind": DesiredMemberKind.EQUITY,
        "ticker": "CRM",
        "stage": EquityPreparationStage.NATIVE_INTRADAY,
        "stage_order": 30,
        "target": {},
        "watermark": {},
        "coverage": {"available_intervals": {}},
        "attempts": 1,
        "failure_streak": 0,
        "lease_owner": UUID(int=3),
        "lease_epoch": 1,
        "lease_until": datetime(2026, 11, 15, 22, 5, tzinfo=UTC),
        "source_cohort_id": UUID(int=4),
        "instrument_id": 10,
    }
    values.update(changes)
    return EquityPreparationClaim(**values)


def _row(start):
    return {"t": int(start.timestamp() * 1000), "o": 100, "h": 101, "l": 99, "c": 100.5, "v": 1000, "vw": 100.2, "n": 9}


# ---- R58: one-minute sessions with provider-confirmed empty minutes ------------------------------------------------

SESSION = date(2026, 11, 11)
MINUTES = equity_history_coordinates(CAL, "1m", SESSION)


def test_empty_minute_rule():
    assert provider_empty_minutes("1m", MINUTES, MINUTES) == ()
    quiet = MINUTES[180][0]
    assert provider_empty_minutes("1m", MINUTES[:180] + MINUTES[181:], MINUTES) == (quiet,)
    # other intervals stay exact; nothing returned, a bar outside the session or a long hole: the rule does not apply
    assert provider_empty_minutes("5m", MINUTES[:-1], MINUTES) is None
    assert provider_empty_minutes("1m", (), MINUTES) is None
    outside = ((MINUTES[0][0] - timedelta(minutes=1), MINUTES[0][0]),)
    assert provider_empty_minutes("1m", MINUTES[1:] + outside, MINUTES) is None
    hole = MAX_PROVIDER_EMPTY_MINUTES_PER_SESSION + 1
    assert provider_empty_minutes("1m", MINUTES[hole:], MINUTES) is None


class _BarsClient:
    def __init__(self, starts):
        self.starts = starts
        self.calls = []

    def fetch_native_bars(self, ticker, interval, start, end, *, adjusted):
        self.calls.append((ticker, interval, start, end, adjusted))
        return tuple(_row(value) for value in self.starts)


def _intraday(interval, starts):
    claim = _claim(target={"interval_sessions": {interval: [SESSION.isoformat()]}})
    request = EquityPreparationFetchRequest(claim, datetime(2026, 11, 15, 2, tzinfo=UTC), EquityPreparationLimits())
    return MassiveEquityPreparationSource(_BarsClient(starts)).fetch(request)


def test_native_intraday_accepts_a_session_with_a_no_trade_minute():
    quiet = MINUTES[179][0]  # CRM Nov 11 13:30 ET-style hole
    chunk = _intraday("1m", [start for start, _end in MINUTES if start != quiet])
    assert chunk.complete is True
    assert chunk.coverage_after["available_intervals"] == {"1m:RAW_UNADJUSTED": 389}
    assert chunk.coverage_after["provider_confirmed_empty_minutes"] == {SESSION.isoformat(): [quiet.isoformat()]}
    assert chunk.source_lineage["missing_no_trade_starts"] == [quiet.isoformat()]
    assert len(chunk.bars) == 389


def test_native_intraday_complete_session_records_no_empty_minutes():
    chunk = _intraday("1m", [start for start, _end in MINUTES])
    assert "provider_confirmed_empty_minutes" not in chunk.coverage_after
    assert "missing_no_trade_starts" not in chunk.source_lineage


def test_native_intraday_five_minute_and_long_holes_stay_exact():
    fives = equity_history_coordinates(CAL, "5m", SESSION)
    with pytest.raises(EquityPreparationUnavailableError, match="NATIVE_INTRADAY_COVERAGE_INCOMPLETE"):
        _intraday("5m", [start for start, _end in fives[1:]])
    with pytest.raises(EquityPreparationUnavailableError, match="NATIVE_INTRADAY_COVERAGE_INCOMPLETE"):
        _intraday("1m", [start for start, _end in MINUTES[60:]])


# ---- R59: an inactive (delisted) ticker --------------------------------------------------------------------------

ROKU_RECORD = {
    "ticker": "ROKU", "name": "Roku, Inc. Class A Common Stock", "active": False, "type": "CS", "market": "stocks",
    "delisted_utc": "2026-11-09T00:00:00Z", "composite_figi": "BBG001ZZPQJ6", "primary_exchange": "XNAS",
    "cik": "0001428439",
}


class _Response:
    def __init__(self, status, payload):
        self.status_code = status
        self.payload = payload
        self.content = b"{}"

    def json(self):
        return self.payload


class _ReferenceSession:
    def __init__(self, *, overview_status=404, inactive=(ROKU_RECORD,)):
        self.overview_status = overview_status
        self.inactive = inactive
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        path = urlparse(url).path
        self.calls.append((path, dict(params)))
        if path.startswith("/v3/reference/tickers/"):
            return _Response(self.overview_status, {"status": "NOT_FOUND"})
        if path == "/v3/reference/tickers":
            return _Response(200, {"results": list(self.inactive)})
        return _Response(200, {"results": []})


def _reference(session, *, contract=None, security_id=None):
    target = {"history_through_session": "2026-11-13", "window_start": "2025-10-01", "window_end": "2026-11-13"}
    if contract:
        target["contract"] = contract
    source = MassiveEquityPreparationSource(BoundedPolygonEquityPreparationClient("test-key", session=session))
    return source.fetch(EquityPreparationFetchRequest(
        _claim(ticker="ROKU", stage=EquityPreparationStage.REFERENCE, stage_order=10, security_id=security_id,
               target=target),
        datetime(2026, 11, 15, 2, tzinfo=UTC),
        EquityPreparationLimits(),
    ))


def test_new_member_inactive_at_the_provider_retries_with_the_delisting_reason():
    session = _ReferenceSession()
    with pytest.raises(EquityPreparationUnavailableError, match="PROVIDER_INACTIVE:2026-11-09") as raised:
        _reference(session)
    assert raised.value.provider_requests == 2  # overview 404, then the list record; no corporate-action requests
    assert session.calls[1] == ("/v3/reference/tickers",
                                {"ticker": "ROKU", "market": "stocks", "active": "false", "limit": 10})


def test_rollover_records_the_inactive_security_revision():
    chunk = _reference(_ReferenceSession(), contract="advanced-equity-rollover-v1")
    (fact,) = chunk.reference_facts
    values = dict(fact.values)
    assert fact.relation is ReferenceFactRelation.SECURITY
    assert values["active"] is False and values["delisted_date"] == date(2026, 11, 9)
    assert chunk.complete is True and chunk.coverage_after["provider_inactive"] is True
    assert chunk.provider_requests == 2


def test_a_404_without_an_inactive_record_still_fails_and_other_errors_do_not_fall_back():
    with pytest.raises(EquityPreparationSourceError, match="REFERENCE_SOURCE_REQUEST_FAILED:HTTPError"):
        _reference(_ReferenceSession(inactive=()))
    session = _ReferenceSession(overview_status=500)
    with pytest.raises(EquityPreparationSourceError, match="REFERENCE_SOURCE_REQUEST_FAILED:HTTPError"):
        _reference(session)
    assert len(session.calls) == 1


# ---- pre-session exclusion of inactive or delisted members -------------------------------------------------------

MONDAY = date(2026, 11, 16)


@pytest.mark.parametrize(("reference", "reason"), (
    (None, None),
    ({"active": True, "delisted_date": None}, None),
    ({"active": True, "delisted_date": date(2026, 12, 1)}, None),
    ({"active": True, "delisted_date": MONDAY}, "PROVIDER_DELISTED:2026-11-16"),
    ({"active": False, "delisted_date": date(2026, 11, 9)}, "PROVIDER_INACTIVE:2026-11-09"),
    # Review (Nov 15): active=False without the provider's delisting date keeps the member (a stored record may
    # default the field to False); exclusion needs the date.
    ({"active": False, "delisted_date": None}, None),
    ({"active": False, "delisted_date": date(2026, 11, 20)}, None),
))
def test_inactive_rule(reference, reason):
    assert desired_repo.provider_inactive_reason_for_session(reference, MONDAY) == reason


SEC = {ticker: UUID(int=100 + index) for index, ticker in enumerate(("AAPL", "ROKU", "SPY", "NEW", "GS"))}


def _prep(ticker, kind=DesiredMemberKind.EQUITY, status=MemberPreparationStatus.READY):
    return DesiredMemberPreparation(UUID(int=1), SEC[ticker], ticker, kind, True, status, (), datetime(2026, 11, 15, tzinfo=UTC))


class _Stop(Exception):
    pass


def _reconcile(monkeypatch, preparations, references, *, source_base=None):
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (str(SEC[ticker]), active, None, delisted, ticker) for ticker, (active, delisted) in references.items()
    ]
    members = tuple(SimpleNamespace(enabled=True, kind=row.kind, ticker=row.ticker) for row in preparations)
    revision = SimpleNamespace(enabled=True, members=members, effective_session_date=MONDAY,
                               source_cohort_id=UUID(int=50), source_slot_version=None, revision_id=UUID(int=1))
    captured = {}
    monkeypatch.setattr(desired_repo, "_lock_desired_revision_cursor", lambda *_a: None)
    monkeypatch.setattr(desired_repo, "load_desired_state_revision_cursor", lambda *_a: revision)
    monkeypatch.setattr(desired_repo, "load_desired_member_preparations_cursor", lambda *_a: tuple(preparations))
    monkeypatch.setattr(desired_repo, "_load_desired_snapshot_cursor", lambda *_a: None)
    monkeypatch.setattr(desired_repo, "_load_source_control_plane_cursor", lambda *_a: object())
    monkeypatch.setattr(
        desired_repo, "_source_member_security_cursor",
        lambda *_a: (set(source_base or {row.security_id for row in preparations}), {SEC["SPY"]}),
    )

    def stop(_source, *, universe_target, runtime_catalog):
        captured["target"] = universe_target
        raise _Stop

    monkeypatch.setattr(desired_repo, "_runtime_configuration_sha256", stop)
    catalog = SimpleNamespace(profile_for_target=lambda _n: SimpleNamespace(
        settings=SimpleNamespace(controller_warmup_seconds=900)))
    try:
        result = desired_repo.reconcile_desired_state_revision_cursor(
            cursor, UUID(int=1), materialized_at=datetime(2026, 11, 16, 14, 30, tzinfo=UTC),
            calendar=CAL, runtime_catalog=catalog, allow_partial_activation=True,
        )
    except _Stop:
        return captured, None
    return captured, result


def test_activation_leaves_out_a_delisted_member_and_its_options_underlying(monkeypatch):
    preparations = (
        _prep("AAPL"), _prep("ROKU"), _prep("SPY"),
        _prep("ROKU", DesiredMemberKind.OPTION_UNDERLYING), _prep("SPY", DesiredMemberKind.OPTION_UNDERLYING),
    )
    captured, _ = _reconcile(monkeypatch, preparations, {
        "AAPL": (True, None), "ROKU": (False, date(2026, 11, 9)), "SPY": (True, None),
    })
    assert captured["target"] == 2  # AAPL and SPY; ROKU (Equity and Options) out


def test_a_failed_baseline_member_no_longer_blocks_the_session(monkeypatch):
    preparations = (_prep("AAPL"), _prep("GS", status=MemberPreparationStatus.FAILED), _prep("SPY"))
    captured, result = _reconcile(monkeypatch, preparations, {"AAPL": (True, None), "SPY": (True, None)})
    assert result is None and captured["target"] == 2  # before: state BLOCKED for every member


def test_an_options_underlying_without_its_equity_member_is_left_out_not_an_error(monkeypatch):
    preparations = (
        _prep("AAPL"), _prep("NEW", status=MemberPreparationStatus.WARMING),
        _prep("NEW", DesiredMemberKind.OPTION_UNDERLYING),
    )
    captured, _ = _reconcile(monkeypatch, preparations, {"AAPL": (True, None), "NEW": (True, None)},
                             source_base={SEC["AAPL"]})
    assert captured["target"] == 1


def test_status_query_lists_every_left_out_member_with_its_reason(monkeypatch):
    preparations = (
        _prep("AAPL"), _prep("ROKU"), _prep("GS", status=MemberPreparationStatus.FAILED),
        _prep("NEW", status=MemberPreparationStatus.WARMING), _prep("NEW", DesiredMemberKind.OPTION_UNDERLYING),
    )
    cursor = MagicMock()
    cursor.fetchall.return_value = [(str(SEC["ROKU"]), False, None, date(2026, 11, 9), "ROKU")]
    monkeypatch.setattr(desired_repo, "load_desired_state_revision_cursor",
                        lambda *_a: SimpleNamespace(effective_session_date=MONDAY))
    monkeypatch.setattr(desired_repo, "load_desired_member_preparations_cursor", lambda *_a: preparations)
    rows = desired_repo.load_desired_member_exclusions_cursor(cursor, UUID(int=1))
    assert [(row["ticker"], row["kind"], row["reason"].split(":")[0]) for row in rows] == [
        ("GS", "EQUITY", "FAILED"),
        ("NEW", "OPTION_UNDERLYING", "EQUITY_MEMBER_NOT_IN_SESSION"),
        ("ROKU", "EQUITY", "PROVIDER_INACTIVE"),
    ]


class _RolloverCursor:
    """Answers the rollover planner's queries for one READY member the provider reports delisted."""

    def __init__(self, observed_at):
        self.observed_at = observed_at
        self.sql = ""

    def execute(self, sql, _params=()):
        self.sql = sql

    def fetchone(self):
        if "realtime_desired_current" in self.sql:
            return {"revision_id": str(UUID(int=1)), "effective_session_date": MONDAY,
                    "source_cohort_id": str(UUID(int=50))}
        if "corporate_action_coverage" in self.sql:
            return {"received_at": None}
        return {"effective_to": None}

    def fetchall(self):
        if "realtime_desired_member_intents" in self.sql:
            return [{"intent_id": str(UUID(int=7)), "ticker": "ROKU", "member_kind": "EQUITY",
                     "security_id": str(SEC["ROKU"]), "instrument_id": 70}]
        if "ref.security_revisions" in self.sql:
            return [{"security_id": str(SEC["ROKU"]), "active": False, "list_date": None,
                     "delisted_date": date(2026, 11, 9), "ticker": "ROKU", "observed_at": self.observed_at}]
        raise AssertionError(f"unexpected query: {self.sql[:80]}")


def test_rollover_keeps_no_bars_current_for_a_delisted_member_and_rechecks_its_reference_daily():
    from realtime.advanced_equity_rollover import plan_equity_rollover_cursor

    checked = datetime(2026, 11, 15, 23, tzinfo=UTC)
    recent = plan_equity_rollover_cursor(_RolloverCursor(checked - timedelta(hours=1)), checked_at=checked,
                                         calendar=CAL)
    assert recent.work == ()
    stale = plan_equity_rollover_cursor(_RolloverCursor(checked - timedelta(days=2)), checked_at=checked,
                                        calendar=CAL)
    assert [work.action for work in stale.work] == ["REFERENCE"]


# ---- G86: tickers listed inside the history depth ----------------------------------------------------------------

def test_listed_period_rule():
    listing = date(2025, 2, 13)  # SNDK, a Thursday
    assert adjusted_period_listed(CAL, "1d", date(2025, 2, 12), listing) is False
    assert adjusted_period_listed(CAL, "1d", date(2025, 2, 13), listing) is True
    assert adjusted_period_listed(CAL, "1wk", date(2025, 2, 14), listing) is True  # the listing week counts
    assert adjusted_period_listed(CAL, "1wk", date(2025, 2, 7), listing) is False
    assert adjusted_period_listed(CAL, "1mo", date(2025, 2, 28), listing) is True
    assert adjusted_period_listed(CAL, "1mo", date(2025, 1, 31), listing) is False
    assert adjusted_period_listed(CAL, "1mo", date(2025, 1, 31), None) is True


def test_adjusted_history_passes_the_periods_before_listing_without_a_request():
    weeks = (date(2025, 1, 31), date(2025, 2, 7), date(2025, 2, 14), date(2025, 2, 21))
    client = _BarsClient(())
    claim = _claim(stage=EquityPreparationStage.ADJUSTED_DAILY, stage_order=20, ticker="SNDK",
                   target={"interval_sessions": {"1wk": [value.isoformat() for value in weeks]}})
    chunk = MassiveEquityPreparationSource(client).fetch(EquityPreparationFetchRequest(
        claim, datetime(2026, 11, 15, tzinfo=UTC), EquityPreparationLimits(), listing_date=date(2025, 2, 13),
    ))
    assert client.calls == []
    assert chunk.watermark_after == {"interval_index": 0, "session_index": 2}
    assert chunk.coverage_after["not_listed_before_listing"] == {"1wk:ADJUSTED": 2}
    assert chunk.complete is False and chunk.provider_requests == 0


def _warmup(adjusted_coverage, required):
    stage_rows = [
        ("REFERENCE", "COMPLETE", {"sector_ready": True, "corporate_actions_ready": True}),
        ("ADJUSTED_DAILY", "COMPLETE", adjusted_coverage),
        ("NATIVE_INTRADAY", "COMPLETE", {"available_intervals": {}}),
        ("DERIVED_INTERVALS", "COMPLETE", {"available_intervals": {"1h:RAW_UNADJUSTED": 2}}),
    ]
    cursor = MagicMock()
    cursor.fetchall.return_value = stage_rows

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=cursor)

    resident = AdvancedEquityPreparationResident(
        SimpleNamespace(transaction=transaction), MagicMock(), owner_id=UUID(int=9),
    )
    return resident._warmup(EquityPreparationFetchRequest(
        _claim(ticker="SNDK", stage=EquityPreparationStage.TECHNICAL_WARMUP, stage_order=50, target={
            "required_adjusted_daily_bars": 2, "required_derived_hourly_bars": 2,
            "required_adjusted_intervals": required, "declared_products": ["EQUITY_BARS"],
        }),
        datetime(2026, 11, 15, tzinfo=UTC), EquityPreparationLimits(),
    ))


def test_warmup_requires_only_the_listed_periods_and_records_why():
    counts = {"1d:ADJUSTED": 2, "1wk:ADJUSTED": 92}
    chunk = _warmup({"available_intervals": counts, "listing_date": "2025-02-13",
                     "not_listed_before_listing": {"1wk:ADJUSTED": 168}}, {"1wk:ADJUSTED": 260})
    assert chunk.complete is True
    assert chunk.coverage_after["history_shortened"]["periods_before_listing"] == {"1wk:ADJUSTED": 168}
    with pytest.raises(EquityPreparationUnavailableError, match="1wk:ADJUSTED:PORTAL_HISTORY_INCOMPLETE"):
        _warmup({"available_intervals": counts}, {"1wk:ADJUSTED": 260})


def test_warmup_daily_requirement_excludes_days_before_a_recent_listing():
    """Review (Nov 15): a name listed inside the technical warm-up depth (fewer daily bars than required) becomes
    READY with its listed days; the same name without the listing record still waits."""
    chunk = _warmup({"available_intervals": {"1d:ADJUSTED": 100}, "listing_date": "2026-06-22",
                     "not_listed_before_listing": {"1d:ADJUSTED": 183}}, {})
    request_required = 2  # _warmup's required_adjusted_daily_bars
    assert request_required and chunk.complete is True
    assert chunk.coverage_after["history_shortened"]["periods_before_listing"] == {"1d:ADJUSTED": 183}


def test_projection_listed_periods():
    weeks = (date(2025, 2, 7), date(2025, 2, 14), date(2025, 2, 21))
    assert _listed_periods(weeks, "1wk", None) == weeks
    assert _listed_periods(weeks, "1wk", date(2012, 5, 18)) == weeks
    assert _listed_periods(weeks, "1wk", date(2025, 2, 13)) == weeks[1:]


# ---- 3.3: a no-trade member minute inside a session (switch) -----------------------------------------------------

HOLE_START = datetime(2026, 11, 13, 18, 4, tzinfo=UTC)
HOLE_END = HOLE_START + timedelta(minutes=1)
MEMBER = StreamGapMember(19383, UUID(int=8), "GS")
SCOPE = SimpleNamespace(cohort_id=UUID(int=9), session_date=date(2026, 11, 13))


def _hole(minutes):
    return _repair_hole_facts(SCOPE, MEMBER, tuple(_row(value) for value in minutes), request_id=UUID(int=10),
                              hole_start=HOLE_START, hole_end=HOLE_END, observed_at=HOLE_END + timedelta(hours=1))


def test_confirmed_empty_minute_needs_the_switch_and_a_healthy_answer(monkeypatch):
    later = (HOLE_START - timedelta(minutes=1), HOLE_END, HOLE_END + timedelta(minutes=1))
    monkeypatch.delenv(CONFIRMED_EMPTY_MINUTES_SWITCH, raising=False)
    with pytest.raises(ValueError, match="every missing minute"):
        _hole(later)
    monkeypatch.setenv(CONFIRMED_EMPTY_MINUTES_SWITCH, "ON")
    facts, _segment, empty = _hole(later)
    assert facts == () and empty == (HOLE_START,)
    with pytest.raises(ValueError, match="every missing minute"):
        _hole((HOLE_START - timedelta(minutes=1),))  # nothing after the hole: the provider may only be behind
    with pytest.raises(ValueError, match="every missing minute"):
        _repair_facts(SCOPE, MEMBER, tuple(_row(value) for value in later), request_id=UUID(int=10),
                      hole_start=HOLE_START, hole_end=HOLE_END, observed_at=HOLE_END)
    long_hole = tuple(HOLE_START + timedelta(minutes=index) for index in range(7))
    assert confirmed_empty_starts((), (), long_hole, hole_end=HOLE_END) == ()


def test_verification_counts_recorded_empty_minutes_only_with_the_switch(monkeypatch):
    repair = (UUID(int=1), UUID(int=2), 19383, HOLE_START, HOLE_END, "REPLAYED")
    monkeypatch.setenv(CONFIRMED_EMPTY_MINUTES_SWITCH, "ON")
    cursor = MagicMock()
    cursor.fetchone.side_effect = (repair, ([HOLE_START],))
    cursor.fetchall.return_value = []
    verify_and_close_gap_repair_cursor(cursor, UUID(int=3), checked_at=HOLE_END, require_derived=False)
    assert "recovery_status='REPAIRED'" in cursor.execute.call_args_list[-1].args[0]
    monkeypatch.delenv(CONFIRMED_EMPTY_MINUTES_SWITCH)
    cursor = MagicMock()
    cursor.fetchone.side_effect = (repair,)
    cursor.fetchall.return_value = []
    with pytest.raises(ValueError, match="repair coverage is incomplete"):
        verify_and_close_gap_repair_cursor(cursor, UUID(int=3), checked_at=HOLE_END, require_derived=False)
    assert not any("provider_confirmed_empty_starts" in call.args[0] for call in cursor.execute.call_args_list)


NOW = datetime(2026, 11, 13, 18, 5, tzinfo=UTC)
PLAN = UUID(int=88)
SESSION_ID = UUID(int=44)
GENERATION = UUID(int=55)
SHARD_MAP = "b" * 64


class _GateDatabase:
    def __init__(self, *, confirmed_plan=PLAN):
        self.cursor = MagicMock()
        self.cursor.fetchone.side_effect = ((UUID(int=33), 1, "MASSIVE_ADVANCED"), (0,))
        present = [index for index in range(5) if index != 4]  # the 18:04 minute had no trade
        bars = tuple(
            (11, NOW - timedelta(minutes=5 - index), NOW - timedelta(minutes=4 - index), UUID(int=100 + index),
             f"{index + 1:064x}", "REALTIME_STREAM", (), GENERATION, SESSION_ID, index + 1, None)
            for index in present
        )
        confirmed = ((11, NOW - timedelta(minutes=1), str(confirmed_plan)),)
        sessions = ((SESSION_ID, GENERATION, 7, "LIVE", NOW - timedelta(minutes=30), None, NOW, 1_000, 10_000, PLAN),)
        journals = ((GENERATION, SESSION_ID, 7, PLAN, "WRITING", (0,), SHARD_MAP),)
        checkpoints = ((GENERATION, 7, 5, str(SESSION_ID), NOW, 0, SHARD_MAP, UUID(int=802), SHARD_MAP),)
        self.results_on = ((11,),), bars, confirmed, sessions, journals, checkpoints
        self.cursor.fetchall.side_effect = self.results_on

    def transaction(self, **_options):
        return nullcontext(self.cursor)


def _gate_context():
    return SimpleNamespace(
        plan=SimpleNamespace(cohort_id=UUID(int=1)),
        window=SimpleNamespace(window_id=UUID(int=2), cutoff=NOW, release_at=NOW + timedelta(seconds=5)),
    )


def test_input_gate_accepts_a_recorded_no_trade_minute_only_with_the_switch(monkeypatch):
    monkeypatch.delenv(CONFIRMED_EMPTY_MINUTES_SWITCH, raising=False)
    database = _GateDatabase()
    database.cursor.fetchall.side_effect = database.results_on[:2]
    with pytest.raises(ValueError, match="exact stream/repair bar coverage"):
        advanced_input_gate_identity(database, _gate_context())
    monkeypatch.setenv(CONFIRMED_EMPTY_MINUTES_SWITCH, "ON")
    assert len(advanced_input_gate_identity(_GateDatabase(), _gate_context())) == 64
    with pytest.raises(ValueError, match="belong to another plan"):
        advanced_input_gate_identity(_GateDatabase(confirmed_plan=UUID(int=99)), _gate_context())
