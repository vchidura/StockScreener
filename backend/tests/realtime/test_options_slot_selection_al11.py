"""G106 AL11 (Nov 18): pre-open selection of the Options watched set by the previous session's volume, RECORD first.

`REALTIME_V3_OPTIONS_SLOT_SELECTION` (options-lifecycle, CAPACITY_SHARDS stage, after AL8): OFF (default) = today's
allocation byte for byte, no read; RECORD = today's allocation stored, the rule's allocation computed and logged only;
ON = the rule's allocation (same per-name counts), today's on any failure; invalid values refuse to start."""
from __future__ import annotations

import json
from collections import Counter
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime import advanced_options_slot_selection as selection_module
from realtime.advanced_options_activity_momentum_c6 import (
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    activity_momentum_allocation_payload,
    allocate_activity_momentum,
    load_activity_momentum_profile,
)
from realtime.advanced_options_automatic_preparation import PostgresAutomaticOptionsPreparationExecutor
from realtime.advanced_options_p8_selection_source import SEALED_CAPTURE_GENERALIZED_C6_PROFILE
from realtime.advanced_options_selection_c6 import OptionSide
from realtime.advanced_options_slot_selection import (
    PREVIOUS_CHAIN_SQL,
    SLOT_SELECTION_POLICY,
    SLOT_SELECTION_RECORD_EVENT,
    SLOT_SELECTION_RECORD_FAILED_EVENT,
    SLOT_SELECTION_SETTING,
    SlotSelectionMode,
    allocate_with_slot_selection,
    load_previous_session_chain,
    previous_session_chain_from_rows,
    slot_selection_eligible,
    slot_selection_mode,
)
from realtime.advanced_options_slot_side_balance import SLOT_SIDE_BALANCE_SETTING, SlotSideBalanceMode
from realtime.contracts import canonical_sha256
from tests.realtime.test_options_slot_side_balance_al8 import (
    GOLDEN_OFF_ALLOCATION_SHA256,
    MONDAY,
    _arguments,
    _fixture_rows,
    _resolution,
)

UTC = timezone.utc
FRIDAY = date(2026, 11, 13)
FRIDAY_CLOSE = datetime(2026, 11, 13, 21, 0, tzinfo=UTC)


# --- fixture: the AL8 rows (SPY daily expiries 0-4/11/18/25 DTE, ACME weeklies 4/11/18/25/32) and a previous chain --

def _previous_volume(row) -> int:
    """Friday's volume, built to disagree with today's ranking: puts and the farther expiries traded most."""
    dte = (row.expiration - MONDAY).days
    side = 2 if row.option_side is OptionSide.PUT else 1
    return 1_000 * side + 40 * dte + int(row.strike)


def _chain_rows(rows, *, observed_at=FRIDAY_CLOSE - timedelta(minutes=4), skip=(), extra=()):
    out = [
        (row.underlying, row.contract_ticker, row.expiration, _previous_volume(row), Decimal("1.00"),
         Decimal("1.10"), observed_at)
        for row in rows
        if row.underlying not in skip
    ]
    return out + list(extra)


def _chain(rows, **kwargs):
    return previous_session_chain_from_rows(
        _chain_rows(rows, **kwargs), previous_session_date=FRIDAY, previous_close=FRIDAY_CLOSE, session_date=MONDAY)


@pytest.fixture(scope="module")
def setup():
    profile = load_activity_momentum_profile(version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    rows = _fixture_rows()
    resolution = _resolution(profile, rows)
    off = allocate_activity_momentum(profile, **_arguments(profile, resolution, rows))
    assert off.allocation_sha256 == GOLDEN_OFF_ALLOCATION_SHA256
    return profile, rows, resolution, off


def _run(setup, mode, *, previous=None, side_balance=False, allocate=None, rows=None, emit=None, resolve=None):
    profile, all_rows, resolution, off = setup
    rows = all_rows if rows is None else rows
    lines, calls = [], Counter()

    def load():
        calls["previous"] += 1
        return _chain(all_rows) if previous is None else previous()

    def resolve_restricted(restricted):
        calls["resolve"] += 1
        return _resolution(profile, restricted)

    def allocate_restricted(restricted):
        calls["allocate"] += 1
        arguments = {**_arguments(profile, restricted, restricted), "groups": restricted.groups,
                     "resolution": restricted}
        if side_balance:
            arguments["side_balance"] = True
        return allocate_activity_momentum(profile, **arguments)

    result = allocate_with_slot_selection(
        mode=mode, used=off, profile=profile, resolution=resolution, candidates=rows, session_date=MONDAY,
        side_balance=side_balance, previous=load, resolve=resolve or resolve_restricted,
        allocate=allocate or allocate_restricted,
        emit=emit or (lambda event, document: lines.append((event, document))))
    return result, lines, calls


def _by_ticker(setup):
    return {row.contract_ticker: row for row in setup[1]}


def _per_underlying(allocation, by_ticker):
    return Counter(by_ticker[ticker].underlying for ticker in allocation.selected_contract_tickers)


# --- OFF: byte for byte, nothing read -------------------------------------------------------------------------------

def test_off_returns_todays_allocation_without_reading_or_allocating(setup):
    result, lines, calls = _run(setup, SlotSelectionMode.OFF)
    assert result is setup[3]
    assert result.allocation_sha256 == GOLDEN_OFF_ALLOCATION_SHA256
    assert lines == [] and calls == Counter()
    for raw in (None, "", " off "):
        result, lines, calls = _run(setup, raw)
        assert result is setup[3] and lines == [] and calls == Counter()


# --- RECORD: today's allocation, the rule's only logged -------------------------------------------------------------

def test_record_returns_todays_allocation_and_logs_summary_and_one_line_per_underlying(setup):
    result, lines, calls = _run(setup, SlotSelectionMode.RECORD)
    on, _lines, _calls = _run(setup, SlotSelectionMode.ON)
    assert result is setup[3]
    assert canonical_sha256(activity_momentum_allocation_payload(result)) == canonical_sha256(
        activity_momentum_allocation_payload(setup[3]))
    assert calls == Counter({"previous": 1, "resolve": 1, "allocate": 1})
    assert [event for event, _document in lines] == [SLOT_SELECTION_RECORD_EVENT] * 3
    summary, *names = [document for _event, document in lines]
    assert summary["part"] == "summary" and summary["mode"] == "RECORD" and summary["applied"] is False
    assert summary["lines"] == 3 and summary["underlyings"] == 2 and summary["rule_applied_underlyings"] == 2
    assert summary["fallback_underlyings"] == {}
    assert summary["off_allocation_sha256"] == GOLDEN_OFF_ALLOCATION_SHA256
    assert summary["selection_allocation_sha256"] == on.allocation_sha256 != GOLDEN_OFF_ALLOCATION_SHA256
    assert summary["contracts"] == {"off": 48, "selection": 48}
    assert summary["overlap"] + summary["added"] == 48 and summary["added"] == summary["dropped"] > 0
    assert summary["previous_session"]["session_date"] == str(FRIDAY)
    assert summary["policy"]["version"] == SLOT_SELECTION_POLICY["version"]
    coverage = summary["previous_session_coverage"]
    assert coverage["selection"]["volume_share"] > coverage["off"]["volume_share"]
    assert [line["underlying"] for line in names] == ["ACME", "SPY"]
    for line in names:
        assert line["part"] == "underlying" and line["rule_applied"] is True and line["reason"] is None
        assert line["target"] == line["off_count"] == line["selection_count"] == 24
        assert line["overlap"] + line["added_count"] == 24
        assert line["added_count"] == len(line["added"]) and line["dropped_count"] == len(line["dropped"])
        assert set(line["added"][0]) == {"contract", "side", "dte", "abs_delta", "prior_oi", "previous_volume"}
        assert set(line["previous_session_coverage"]) == {"off", "selection"}
        assert set(line["dte_mix"]["selection"]) == {"0", "1-2", "3-4", "5-21", "22+"}


def test_record_lines_are_json_under_the_docker_line_limit(setup, capsys):
    profile, rows, resolution, off = setup
    allocate_with_slot_selection(
        mode="RECORD", used=off, profile=profile, resolution=resolution, candidates=rows, session_date=MONDAY,
        side_balance=False, previous=lambda: _chain(rows), resolve=lambda r: _resolution(profile, r),
        allocate=lambda r: allocate_activity_momentum(
            profile, **{**_arguments(profile, r, r), "groups": r.groups, "resolution": r}))
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 3
    for line in err:
        event, _, body = line.partition(" ")
        assert event == SLOT_SELECTION_RECORD_EVENT
        assert json.loads(body)["mode"] == "RECORD"
        assert len(line.encode()) < 16 * 1024


def test_record_never_raises(setup):
    def broken_allocate(_restricted):
        raise RuntimeError("restricted allocation exploded")

    result, lines, _calls = _run(setup, SlotSelectionMode.RECORD, allocate=broken_allocate)
    assert result is setup[3]
    assert [event for event, _document in lines] == [SLOT_SELECTION_RECORD_FAILED_EVENT]
    assert lines[0][1]["stage"] == "SELECTION" and "exploded" in lines[0][1]["error"]

    def broken_emit(_event, _document):
        raise OSError("stderr closed")

    result, _lines, _calls = _run(setup, SlotSelectionMode.RECORD, emit=broken_emit)
    assert result is setup[3]


# --- ON: the rule's set, same counts --------------------------------------------------------------------------------

def test_on_uses_the_rule_and_keeps_each_underlyings_count(setup):
    profile, rows, _resolution_, off = setup
    by_ticker = _by_ticker(setup)
    result, lines, _calls = _run(setup, SlotSelectionMode.ON)
    assert result.allocation_sha256 != off.allocation_sha256
    assert _per_underlying(result, by_ticker) == _per_underlying(off, by_ticker) == Counter({"ACME": 24, "SPY": 24})
    assert lines[0][1]["applied"] is True and lines[0][1]["mode"] == "ON"
    chain = _chain(rows)
    for underlying in ("ACME", "SPY"):
        picked = [by_ticker[t] for t in result.selected_contract_tickers if by_ticker[t].underlying == underlying]
        same_day = [row for row in picked if row.dte_for(MONDAY) == 0]
        rest = [row for row in picked if row.dte_for(MONDAY) > 0]
        # SPY (DAILY_EXPIRY) keeps today's 0DTE count (calls and puts alternately, from today's admitted 0DTE
        # contracts); ACME (weekly) none.
        today_same_day = [by_ticker[t] for t in off.selected_contract_tickers
                          if by_ticker[t].underlying == underlying and by_ticker[t].dte_for(MONDAY) == 0]
        if underlying == "SPY":
            assert len(today_same_day) == 6 and len(same_day) == len(today_same_day)
            assert Counter(row.option_side for row in same_day) == Counter({OptionSide.CALL: 3, OptionSide.PUT: 3})
            admitted = {row.contract_ticker for group in _resolution_.groups for row in group.candidates}
            assert {row.contract_ticker for row in same_day} <= admitted
        else:
            assert same_day == [] == today_same_day
        assert all(slot_selection_eligible(row, profile, MONDAY) for row in rest)
        assert not any(row.dte_for(MONDAY) in (3, 4) for row in picked)
        # Score order: no eligible contract left out had more previous-session volume than the weakest one taken
        # (except the EXTENSION side bound, 8 a side for DTE 22-60).
        weakest = min(chain.volumes[row.contract_ticker] for row in rest)
        left_out = [row for row in rows if row.underlying == underlying and row.contract_ticker not in
                    {r.contract_ticker for r in picked} and slot_selection_eligible(row, profile, MONDAY)]
        assert all(chain.volumes[row.contract_ticker] <= weakest or row.dte_for(MONDAY) >= 22 for row in left_out)
        extension = Counter(row.option_side for row in picked if row.dte_for(MONDAY) >= 22)
        assert all(count <= 8 for count in extension.values())


def test_the_same_day_quota_follows_todays_count(setup):
    """The 0DTE quota is today's 0DTE count for the name (whatever the AL8 switch made it), not a constant."""
    profile, rows, resolution, off = setup
    by_ticker = _by_ticker(setup)
    spy_same_day = sorted(t for t in off.selected_contract_tickers
                          if by_ticker[t].underlying == "SPY" and by_ticker[t].dte_for(MONDAY) == 0)
    kept = set(off.selected_contract_tickers) - set(spy_same_day[2:])  # today with only two SPY 0DTE contracts
    fewer = SimpleNamespace(
        selected_contract_tickers=tuple(sorted(kept)),
        selected_groups=tuple(g for g in off.selected_groups if all(r.contract_ticker in kept for r in g.candidates)))
    plan = selection_module.plan_slot_selection(
        profile=profile, candidates=rows, resolution=resolution, off=fewer, session_date=MONDAY,
        previous=_chain(rows), side_balance=False)
    spy = plan.underlyings["SPY"]
    assert spy.target == 24 - (len(spy_same_day) - 2) and len(spy.chosen) == spy.target
    assert spy.same_day == 2
    assert sum(1 for t in spy.chosen if by_ticker[t].dte_for(MONDAY) == 0) == 2
    assert plan.underlyings["ACME"].same_day == 0
    assert SLOT_SELECTION_POLICY["same_day_contracts"] == "TODAYS_COUNT"


def test_on_with_al8_on_alternates_calls_and_puts(setup):
    by_ticker = _by_ticker(setup)
    plain, _lines, _calls = _run(setup, SlotSelectionMode.ON)
    balanced, lines, _calls = _run(setup, SlotSelectionMode.ON, side_balance=True)
    for underlying in ("ACME", "SPY"):
        sides = Counter(by_ticker[t].option_side for t in balanced.selected_contract_tickers
                        if by_ticker[t].underlying == underlying)
        assert sides == Counter({OptionSide.CALL: 12, OptionSide.PUT: 12})
    plain_sides = Counter(by_ticker[t].option_side for t in plain.selected_contract_tickers)
    assert plain_sides[OptionSide.PUT] > plain_sides[OptionSide.CALL]  # the fixture's Friday favoured puts
    assert lines[0][1]["side_balance"] is True


def test_a_name_without_a_previous_chain_keeps_todays_picks(setup):
    profile, rows, _resolution_, off = setup
    by_ticker = _by_ticker(setup)
    result, lines, _calls = _run(
        setup, SlotSelectionMode.ON, previous=lambda: _chain(rows, skip=("ACME",)))
    acme_today = {t for t in off.selected_contract_tickers if by_ticker[t].underlying == "ACME"}
    acme_now = {t for t in result.selected_contract_tickers if by_ticker[t].underlying == "ACME"}
    assert acme_now == acme_today
    assert {t for t in result.selected_contract_tickers if by_ticker[t].underlying == "SPY"} != {
        t for t in off.selected_contract_tickers if by_ticker[t].underlying == "SPY"}
    summary = lines[0][1]
    assert summary["fallback_underlyings"] == {"ACME": "PREVIOUS_CHAIN_MISSING"}
    acme = next(line for _e, line in lines if line.get("underlying") == "ACME")
    assert acme["rule_applied"] is False and acme["added_count"] == acme["dropped_count"] == 0


def test_a_stale_or_empty_previous_chain_keeps_todays_picks(setup):
    rows = setup[1]
    stale_at = FRIDAY_CLOSE - timedelta(seconds=SLOT_SELECTION_POLICY["previous_chain_fresh_within_seconds"] + 60)
    result, lines, calls = _run(setup, SlotSelectionMode.ON, previous=lambda: _chain(rows, observed_at=stale_at))
    assert result is setup[3]  # no name applied: no restricted allocation at all
    assert calls["allocate"] == 0
    assert lines[0][1]["fallback_underlyings"] == {"ACME": "PREVIOUS_CHAIN_STALE", "SPY": "PREVIOUS_CHAIN_STALE"}
    zero = previous_session_chain_from_rows(
        [(u, t, e, 0, b, a, o) for u, t, e, _v, b, a, o in _chain_rows(rows)],
        previous_session_date=FRIDAY, previous_close=FRIDAY_CLOSE, session_date=MONDAY)
    result, lines, _calls = _run(setup, SlotSelectionMode.ON, previous=lambda: zero)
    assert result is setup[3]
    assert set(lines[0][1]["fallback_underlyings"].values()) == {"PREVIOUS_VOLUME_EMPTY"}


def test_a_failed_previous_read_keeps_today_and_logs_it(setup):
    def broken():
        raise ConnectionError("database unavailable")

    for mode in (SlotSelectionMode.RECORD, SlotSelectionMode.ON):
        result, lines, _calls = _run(setup, mode, previous=broken)
        assert result is setup[3]
        assert lines[0][0] == SLOT_SELECTION_RECORD_FAILED_EVENT and lines[0][1]["stage"] == "PREVIOUS_CHAIN"
        assert lines[1][0] == SLOT_SELECTION_RECORD_EVENT
        assert set(lines[1][1]["fallback_underlyings"].values()) == {"PREVIOUS_CHAIN_UNAVAILABLE"}


def test_a_second_shortfall_keeps_today(setup):
    profile = setup[0]

    def always_lossy(restricted):
        dropped = next(row for row in restricted if row.underlying == "SPY")
        return _resolution(profile, tuple(row for row in restricted if row is not dropped))

    result, lines, _calls = _run(setup, SlotSelectionMode.ON, resolve=always_lossy)
    assert result is setup[3]
    assert lines[-1][0] == SLOT_SELECTION_RECORD_FAILED_EVENT and "shortfall" in lines[-1][1]["error"]


def test_on_keeps_today_when_the_rule_fails(setup):
    def broken_allocate(_restricted):
        raise ValueError("restricted allocation refused")

    result, lines, _calls = _run(setup, SlotSelectionMode.ON, allocate=broken_allocate)
    assert result is setup[3]
    assert [event for event, _document in lines] == [SLOT_SELECTION_RECORD_FAILED_EVENT]
    assert lines[0][1]["kept"] == "TODAY"


def test_a_shortfall_falls_back_per_name_and_reallocates(setup):
    profile = setup[0]
    by_ticker = _by_ticker(setup)
    seen = []

    def lossy(restricted):
        seen.append(restricted)
        if len(seen) == 1:  # the first pass loses one of ACME's picks (a filter the rule did not mirror)
            dropped = next(row for row in restricted if row.underlying == "ACME")
            restricted = tuple(row for row in restricted if row is not dropped)
        return _resolution(profile, restricted)

    result, lines, _calls = _run(setup, SlotSelectionMode.ON, resolve=lossy)
    assert len(seen) == 2
    assert lines[0][1]["fallback_underlyings"] == {"ACME": "SELECTION_SHORTFALL"}
    assert {t for t in result.selected_contract_tickers if by_ticker[t].underlying == "ACME"} == {
        t for t in setup[3].selected_contract_tickers if by_ticker[t].underlying == "ACME"}
    assert _per_underlying(result, by_ticker) == Counter({"ACME": 24, "SPY": 24})


def test_a_thin_pool_is_topped_up_from_todays_picks(setup):
    profile, rows, _resolution_, off = setup
    by_ticker = _by_ticker(setup)
    # Only ACME's 11-DTE contracts carry a delta in the band: 12 eligible, 12 more from today's picks.
    thin = tuple(
        row if row.underlying != "ACME" or row.dte_for(MONDAY) == 11 else replace(row, delta=Decimal("0.95")
                                                                                   if row.option_side is OptionSide.CALL
                                                                                   else Decimal("-0.95"))
        for row in rows)
    result, lines, _calls = _run(setup, SlotSelectionMode.RECORD, rows=thin)
    acme = next(line for _e, line in lines if line.get("underlying") == "ACME")
    assert acme["rule_applied"] is True and acme["selection_count"] == 24
    assert acme["topped_up"] == 24 - sum(
        1 for row in thin if row.underlying == "ACME" and slot_selection_eligible(row, profile, MONDAY))
    assert result is off


def test_helper_refuses_a_non_v4_profile_unless_off(setup):
    _profile, rows, resolution, off = setup
    v3 = load_activity_momentum_profile(version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    assert allocate_with_slot_selection(
        mode="OFF", used=off, profile=v3, resolution=resolution, candidates=rows, session_date=MONDAY,
        side_balance=False, previous=None, resolve=None, allocate=None) is off
    with pytest.raises(ValueError, match="v4 admission profile"):
        allocate_with_slot_selection(
            mode="RECORD", used=off, profile=v3, resolution=resolution, candidates=rows, session_date=MONDAY,
            side_balance=False, previous=None, resolve=None, allocate=None)


# --- the previous chain -----------------------------------------------------------------------------------------------

class _Calendar:
    def previous_session_date(self, session_date):
        assert session_date == MONDAY
        return FRIDAY

    def session_close(self, session_date):
        assert session_date == FRIDAY
        return FRIDAY_CLOSE


class _Cursor:
    def __init__(self, rows):
        self.rows, self.executed = rows, []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self.rows)


def test_previous_chain_is_one_query_on_the_previous_session(setup):
    rows = setup[1]
    expired = ("SPY", "O:SPY261113C00100000", FRIDAY, 99_999, Decimal("1"), Decimal("1.2"), FRIDAY_CLOSE)
    cursor = _Cursor(_chain_rows(rows, extra=[expired]))
    chain = load_previous_session_chain(cursor, session_date=MONDAY, underlyings=("spy", "ACME"),
                                        calendar=_Calendar())
    assert cursor.executed == [(PREVIOUS_CHAIN_SQL, (FRIDAY, ["ACME", "SPY"]))]
    assert "rt.option_breadth_current" in PREVIOUS_CHAIN_SQL and "session_date=%s" in PREVIOUS_CHAIN_SQL
    assert chain.session_date == FRIDAY
    assert chain.fresh_after == FRIDAY_CLOSE - timedelta(seconds=3600)
    assert "O:SPY261113C00100000" not in chain.volumes  # expired before the session: freshness clock only
    assert chain.total_volume["SPY"] == sum(_previous_volume(r) for r in rows if r.underlying == "SPY")
    assert chain.state("SPY") is None and chain.state("ACME") is None and chain.state("QQQ") == (
        "PREVIOUS_CHAIN_MISSING")


# --- the switch -------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, SlotSelectionMode.OFF), ("", SlotSelectionMode.OFF), ("  ", SlotSelectionMode.OFF),
    ("OFF", SlotSelectionMode.OFF), ("record", SlotSelectionMode.RECORD), (" On ", SlotSelectionMode.ON),
])
def test_mode_values(raw, expected):
    environ = {} if raw is None else {SLOT_SELECTION_SETTING: raw}
    assert slot_selection_mode(environ) is expected


@pytest.mark.parametrize("raw", ["YES", "1", "true", "VOLUME", "RECORD_ONLY", "OFF,ON"])
def test_invalid_mode_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=SLOT_SELECTION_SETTING):
        slot_selection_mode({SLOT_SELECTION_SETTING: raw})
    with pytest.raises(ValueError, match=SLOT_SELECTION_SETTING):
        allocate_with_slot_selection(mode=raw, used=None, profile=None, resolution=None, candidates=(),
                                     session_date=MONDAY, side_balance=False, previous=None, resolve=None,
                                     allocate=None)


def test_executor_refuses_an_invalid_value_and_a_non_v4_profile():
    with pytest.raises(ValueError, match=SLOT_SELECTION_SETTING):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            slot_selection="MAYBE")
    for mode in ("RECORD", "ON"):
        with pytest.raises(ValueError, match=f"{SLOT_SELECTION_SETTING} requires the v4"):
            PostgresAutomaticOptionsPreparationExecutor(
                None, api_key="k", backend_image_id="i",
                admission_profile_version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION, slot_selection=mode)
    # A valid v4 value passes the AL11 check (the next check, the provider, then fails as before).
    with pytest.raises(ValueError, match=r"configuration is invalid$"):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            slot_selection="ON")


class _Constructed(Exception):
    pass


def _run_lifecycle_main(monkeypatch, value):
    from scripts import run_advanced_options_v3_lifecycle as resident

    seen = {}

    def executor(*_args, **kwargs):
        seen.update(kwargs)
        raise _Constructed

    monkeypatch.setenv("POLYGON_API_KEY", "offline-fixture")
    monkeypatch.setenv("REALTIME_V3_OPTIONS_ADMISSION_PROFILE", V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    monkeypatch.delenv(SLOT_SIDE_BALANCE_SETTING, raising=False)
    if value is None:
        monkeypatch.delenv(SLOT_SELECTION_SETTING, raising=False)
    else:
        monkeypatch.setenv(SLOT_SELECTION_SETTING, value)
    monkeypatch.setattr(resident, "dotenv_values", lambda *_: {})
    monkeypatch.setattr(resident, "_provider", lambda: (SimpleNamespace(), SimpleNamespace(closeall=lambda: None)))
    monkeypatch.setattr(resident, "PostgresAutomaticOptionsPreparationExecutor", executor)
    args = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2", "--continuous",
            "--execute"]
    return resident, seen, args


@pytest.mark.parametrize("value, expected", [
    (None, SlotSelectionMode.OFF), ("RECORD", SlotSelectionMode.RECORD), ("on", SlotSelectionMode.ON)])
def test_lifecycle_service_passes_the_switch(monkeypatch, value, expected):
    resident, seen, args = _run_lifecycle_main(monkeypatch, value)
    with pytest.raises(_Constructed):
        resident.main(args)
    assert seen["slot_selection"] is expected
    assert seen["slot_side_balance"] is SlotSideBalanceMode.OFF


def test_lifecycle_service_refuses_to_start_on_an_invalid_value(monkeypatch):
    resident, seen, args = _run_lifecycle_main(monkeypatch, "VOLUME")
    with pytest.raises(ValueError, match=SLOT_SELECTION_SETTING):
        resident.main(args)
    assert seen == {}


def test_setting_line(capsys):
    selection_module.log_slot_selection_setting(SlotSelectionMode.RECORD)
    event, _, body = capsys.readouterr().err.strip().partition(" ")
    assert event == "OPTIONS_SLOT_SELECTION_SETTING"
    assert json.loads(body) == {"mode": "RECORD", "setting": SLOT_SELECTION_SETTING,
                                "policy_version": SLOT_SELECTION_POLICY["version"]}


# --- the executor: _allocate_capture end to end on a synthetic v4 capture ----------------------------------------------

CAPTURED_AT = datetime(2026, 11, 16, 14, 33, tzinfo=UTC)
REVISION_ID = UUID("11111111-1111-1111-1111-111111111111")


def _nanos(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000_000)


def _raw_capture():
    """SPY (listed daily: 0-4 DTE plus 11/18/25) and AAPL (Friday weeklies 4/11/18/25/32) on Monday Nov 16, 21 strikes
    each 1% apart; deltas fall with the strike; the capture's day rows are mostly Friday's (PRIOR) volume."""
    snapshots = {}
    plans = {"SPY": (Decimal("700"), (0, 1, 2, 3, 4, 11, 18, 25)), "AAPL": (Decimal("300"), (4, 11, 18, 25, 32))}
    for underlying, (spot, dtes) in plans.items():
        for dte in dtes:
            expiration = MONDAY + timedelta(days=dte)
            for step in range(-10, 11):
                strike = spot * (1 + Decimal(step) / 100)
                call_delta = max(Decimal("0.03"), min(Decimal("0.97"), Decimal("0.50") - Decimal(step) * Decimal("0.06")))
                for kind, delta in (("call", call_delta), ("put", call_delta - 1)):
                    ticker = f"O:{underlying}{expiration:%y%m%d}{kind[0].upper()}{int(strike * 1000):08d}"
                    session_row = (step + dte) % 5 == 0
                    snapshots[ticker] = {
                        "details": {"ticker": ticker, "contract_type": kind, "exercise_style": "american",
                                    "expiration_date": expiration.isoformat(), "strike_price": float(strike),
                                    "shares_per_contract": 100},
                        "last_quote": {"bid": 2.0, "ask": 2.1,
                                       "last_updated": _nanos(CAPTURED_AT - timedelta(seconds=20))},
                        "day": {"volume": 30 if session_row else 2_000 - 90 * abs(step),
                                "last_updated": _nanos(CAPTURED_AT - timedelta(seconds=40) if session_row
                                                       else datetime(2026, 11, 13, 20, 59, tzinfo=UTC))},
                        "greeks": {"delta": float(delta)},
                        "implied_volatility": 0.3,
                        "open_interest": 1_000 + 50 * step,
                        "underlying_asset": {"ticker": underlying, "price": float(spot)},
                    }
    return {
        "contract": "advanced-options-v3-pre-session-capture-v1",
        "compatibility_profile": SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
        "configured_underlyings": ["AAPL", "SPY"],
        "captured_at_utc": CAPTURED_AT.isoformat(),
        "session_date": MONDAY.isoformat(),
        "provider_requests": 2,
        "decoded_bytes": 4096,
        "responses": [{"underlyer": "AAPL"}, {"underlyer": "SPY"}],
        "selected_snapshots": snapshots,
    }


def _friday_rows(capture):
    rows = []
    for ticker, raw in capture["selected_snapshots"].items():
        details = raw["details"]
        expiration = date.fromisoformat(details["expiration_date"])
        # Friday: puts and far strikes traded most (disagrees with Monday's capture-volume ranking).
        volume = (3_000 if details["contract_type"] == "put" else 1_000) + int(details["strike_price"]) % 37
        rows.append((raw["underlying_asset"]["ticker"], ticker, expiration, volume, Decimal("2.0"), Decimal("2.1"),
                     FRIDAY_CLOSE - timedelta(minutes=4)))
    return rows


class _Provider:
    def __init__(self, previous_rows):
        self.previous_rows, self.previous_reads = previous_rows, 0

    @contextmanager
    def transaction(self, *_args, **kwargs):
        provider = self

        class Cursor:
            rows = []

            def execute(self, sql, params=None):
                if sql == PREVIOUS_CHAIN_SQL:
                    assert kwargs.get("read_only") is True
                    provider.previous_reads += 1
                    self.rows = provider.previous_rows
                else:
                    self.rows = []

            def fetchall(self):
                return list(self.rows)

        yield SimpleNamespace(cursor=Cursor())


def _executor(capture, **attributes):
    executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)
    executor._exact_four_compatibility = False
    executor._admission_profile_version = V4_ACTIVITY_MOMENTUM_PROFILE_VERSION
    executor._enabled_strategies = ()
    executor._active_quote_contract_budget = None
    executor._per_underlying_contract_limit = None
    executor._measured_capacity_limit = None
    executor._effective_provider_project_ceiling = None
    executor._provider = _Provider(_friday_rows(capture))
    for name, value in attributes.items():
        setattr(executor, name, value)
    return executor


def _allocate(executor, capture):
    claim = SimpleNamespace(revision_id=REVISION_ID, session_date=MONDAY, option_underlyings=("AAPL", "SPY"))
    return executor._allocate_capture(claim, json.loads(json.dumps(capture)))


@pytest.fixture(scope="module")
def executor_runs():
    capture = _raw_capture()
    runs = {}
    for label, attributes in (
        ("legacy", {}),
        ("OFF", {"_slot_selection": SlotSelectionMode.OFF}),
        ("RECORD", {"_slot_selection": SlotSelectionMode.RECORD}),
        ("ON", {"_slot_selection": SlotSelectionMode.ON}),
        ("ON+AL8", {"_slot_selection": SlotSelectionMode.ON, "_slot_side_balance": SlotSideBalanceMode.ON}),
        ("AL8", {"_slot_side_balance": SlotSideBalanceMode.ON}),
    ):
        executor = _executor(capture, **attributes)
        filtered, allocation = _allocate(executor, capture)
        runs[label] = (filtered, allocation, executor._provider.previous_reads)
    return runs


def test_executor_off_is_byte_identical_and_reads_nothing(executor_runs):
    legacy_capture, legacy, _reads = executor_runs["legacy"]
    for label in ("OFF", "RECORD"):
        filtered, allocation, reads = executor_runs[label]
        assert allocation.allocation_sha256 == legacy.allocation_sha256
        assert filtered["semantic_sha256"] == legacy_capture["semantic_sha256"]
        assert canonical_sha256(filtered) == canonical_sha256(legacy_capture)
        assert reads == (0 if label == "OFF" else 1)
    assert executor_runs["legacy"][2] == 0


def test_executor_on_stores_the_rule_set_within_the_same_capacity(executor_runs):
    legacy_capture, legacy, _reads = executor_runs["legacy"]
    for label in ("ON", "ON+AL8"):
        filtered, allocation, reads = executor_runs[label]
        assert reads == 1
        assert allocation.allocation_sha256 != legacy.allocation_sha256
        assert set(filtered["selected_snapshots"]) == set(allocation.selected_contract_tickers)
        assert filtered["activity_momentum_allocation"]["allocation_sha256"] == allocation.allocation_sha256
        names = Counter(ticker[2:6].rstrip("0123456789") for ticker in allocation.selected_contract_tickers)
        legacy_names = Counter(ticker[2:6].rstrip("0123456789") for ticker in legacy.selected_contract_tickers)
        assert names == legacy_names
        assert len(allocation.selected_contract_tickers) == len(legacy.selected_contract_tickers)
        def dte(ticker):
            return (date(2000 + int(ticker[-15:-13]), int(ticker[-13:-11]), int(ticker[-11:-9])) - MONDAY).days

        dtes = Counter(dte(t) for t in allocation.selected_contract_tickers)
        assert dtes[3] == dtes[4] == 0
        # SPY (daily expiries) keeps today's 0DTE count; AAPL (weekly) has none either way.
        legacy_same_day = sum(1 for t in legacy.selected_contract_tickers if t.startswith("O:SPY") and dte(t) == 0)
        assert legacy_same_day > 2
        assert sum(1 for t in allocation.selected_contract_tickers if t.startswith("O:SPY") and dte(t) == 0) == (
            legacy_same_day)
    sides = Counter(t[-9] for t in executor_runs["ON+AL8"][1].selected_contract_tickers)
    assert sides["C"] == sides["P"]


def test_allocate_capture_routes_through_the_switch():
    """OFF skips the AL11 path entirely (no read, no call); executors built with __new__ have no attribute and read
    OFF; the AL8 helper still makes today's allocation first."""
    import inspect

    from realtime import advanced_options_automatic_preparation as preparation

    source = inspect.getsource(preparation.PostgresAutomaticOptionsPreparationExecutor._allocate_capture)
    assert source.index("allocate_with_side_balance(") < source.index("self._slot_selection_allocation(")
    assert '"_slot_selection"' in source and "SlotSelectionMode.OFF" in source
    assert "if slot_selection is not SlotSelectionMode.OFF:" in source
