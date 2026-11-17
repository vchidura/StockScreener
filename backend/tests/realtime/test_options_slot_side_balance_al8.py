"""G106 AL8 (Nov 17): call/put-balanced V4 watched set, RECORD first.

`REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE` (options-lifecycle, CAPACITY_SHARDS stage): OFF (default) = today's allocation
byte for byte; RECORD = today's allocation stored, the balanced one computed alongside and logged only; ON = the balanced
allocation (per (expiry, side) interleave, each side capped at half a bucket, later loops pick the side with fewer
contracts, side_priority neutral in the allocation sort)."""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from realtime.advanced_options_activity_momentum_c6 import (
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    _group_rank,
    _side_balanced_next,
    _side_counts,
    _v4_bucket_context,
    _v4_fill_product_buckets_side_balanced,
    activity_momentum_allocation_payload,
    allocate_activity_momentum,
    load_activity_momentum_profile,
    resolve_activity_momentum_selection,
    v4_contract_bucket,
)
from realtime.advanced_options_automatic_preparation import PostgresAutomaticOptionsPreparationExecutor
from realtime.advanced_options_slot_side_balance import (
    SLOT_SIDE_BALANCE_RECORD_EVENT,
    SLOT_SIDE_BALANCE_RECORD_FAILED_EVENT,
    SLOT_SIDE_BALANCE_SETTING,
    SlotSideBalanceMode,
    _print_line,
    allocate_with_side_balance,
    slot_side_balance_mode,
)
from realtime.advanced_options_selection_c6 import OptionSide
from realtime.contracts import canonical_sha256
from tests.realtime.test_advanced_options_activity_momentum_c6 import _candidate, _expiry_calendar

UTC = timezone.utc
MONDAY = date(2026, 11, 16)
PREPARATION_AT = datetime(2026, 11, 16, 14, 33, tzinfo=UTC)

# Pinned at HEAD 05ae39c (image r132) before AL8: the OFF allocation of the recorded fixture below.
GOLDEN_OFF_ALLOCATION_SHA256 = "9356dad432788661df7b399ecdbe2d6da501cd4eed73b46ba310a16f3b172a86"


def _row(underlying, index, *, dte, side, strike, volume, open_interest):
    """Deterministic candidate (the shared fixture's contract_id uses hash(), which is salted per process)."""
    call_delta = max(Decimal("0.05"), min(Decimal("0.95"), Decimal("0.50") - (strike - Decimal("100")) * Decimal("0.08")))
    delta = call_delta if side is OptionSide.CALL else call_delta - Decimal("1")
    row = _candidate(underlying, index, dte=dte, side=side, session_date=MONDAY, observed_at=PREPARATION_AT,
                     strike_millis=int(strike * 1000), prior_open_interest=open_interest, delta=delta)
    return replace(row, contract_id=1_000 + index + (0 if underlying == "SPY" else 50_000),
                   completed_session_volume=volume)


def _fixture_rows():
    """SPY (daily expiries 0-4 DTE plus 11/18/25) and ACME (Friday weeklies 4/11/18/25/32 DTE) on a Monday. Calls rank
    higher on activity at most strikes (the live pattern); a few puts are thin (OI 20, prior volume 40)."""
    rows, index = [], 0
    plans = {"SPY": (0, 1, 2, 3, 4, 11, 18, 25), "ACME": (4, 11, 18, 25, 32)}
    for underlying, dtes in plans.items():
        for dte in dtes:
            for step in range(-4, 5):
                strike = Decimal("100") + Decimal(step)
                for side in (OptionSide.CALL, OptionSide.PUT):
                    base = 4_000 - 150 * abs(step) - 60 * dte
                    volume = base if side is OptionSide.CALL else base - 700
                    thin = side is OptionSide.PUT and step in (3, 4)
                    rows.append(_row(underlying, index, dte=dte, side=side, strike=strike,
                                     volume=40 if thin else max(volume, 60),
                                     open_interest=20 if thin else 900 + 40 * step))
                    index += 1
    return tuple(rows)


def _resolution(profile, rows):
    calendars = []
    for underlying in sorted({row.underlying for row in rows}):
        expiries = tuple(sorted({row.expiration for row in rows
                                 if row.underlying == underlying and (row.expiration - MONDAY).days <= 5}))
        calendars.append(_expiry_calendar(
            underlying, session_date=MONDAY, week_end_session_date=MONDAY + timedelta(days=4), expiries=expiries,
            last_trading_at=datetime(2026, 11, 16, 21, 0, tzinfo=UTC)))
    return resolve_activity_momentum_selection(
        profile, session_date=MONDAY, candidates=rows, required_contract_tickers=(), preparation_at=PREPARATION_AT,
        expiry_calendars=tuple(calendars), same_day_enabled=None, short_delta_override=None)


def _arguments(profile, resolution, rows):
    return dict(session_date=MONDAY, rotation_key="al8-fixture", ready_underlyings=("ACME", "SPY"),
                groups=resolution.groups, resolution=resolution)


@pytest.fixture(scope="module")
def setup():
    profile = load_activity_momentum_profile(version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    rows = _fixture_rows()
    resolution = _resolution(profile, rows)
    return profile, rows, resolution


def test_off_allocation_matches_the_pinned_golden(setup):
    profile, rows, resolution = setup
    allocation = allocate_activity_momentum(profile, **_arguments(profile, resolution, rows))
    assert allocation.allocation_sha256 == GOLDEN_OFF_ALLOCATION_SHA256


# --- OFF: byte for byte ---------------------------------------------------------------------------------------------

def test_off_is_byte_identical_with_the_flag_false_and_through_the_service_helper(setup):
    profile, rows, resolution = setup
    default = allocate_activity_momentum(profile, **_arguments(profile, resolution, rows))
    explicit = allocate_activity_momentum(profile, **_arguments(profile, resolution, rows), side_balance=False)
    calls, lines = [], []

    def spy(profile_, **kwargs):
        calls.append(kwargs)
        return allocate_activity_momentum(profile_, **kwargs)

    via_helper = allocate_with_side_balance(
        spy, mode=SlotSideBalanceMode.OFF, profile=profile, resolution=resolution, session_date=MONDAY,
        arguments=_arguments(profile, resolution, rows), emit=lambda *line: lines.append(line))
    payload = canonical_sha256(activity_momentum_allocation_payload(default))
    for value in (explicit, via_helper):
        assert value.allocation_sha256 == GOLDEN_OFF_ALLOCATION_SHA256
        assert canonical_sha256(activity_momentum_allocation_payload(value)) == payload
    assert len(calls) == 1 and "side_balance" not in calls[0]
    assert lines == []


def test_off_keeps_the_call_heavy_shape_the_fix_is_about(setup):
    profile, rows, resolution = setup
    by_ticker = {row.contract_ticker: row for row in rows}
    allocation = allocate_activity_momentum(profile, **_arguments(profile, resolution, rows))
    short = Counter(by_ticker[t].option_side for t in allocation.selected_contract_tickers
                    if by_ticker[t].underlying == "SPY" and (by_ticker[t].expiration - MONDAY).days <= 2)
    assert short == Counter({OptionSide.CALL: 12})


def test_side_balance_needs_the_v4_profile_and_a_boolean(setup):
    profile, rows, resolution = setup
    v3 = load_activity_momentum_profile(version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    with pytest.raises(ValueError, match="side balance requires the v4 profile"):
        allocate_activity_momentum(v3, session_date=MONDAY, rotation_key="x", ready_underlyings=("SPY",), groups=(),
                                   resolution=resolution, side_balance=True)
    with pytest.raises(ValueError, match="side balance requires the v4 profile"):
        allocate_activity_momentum(profile, **_arguments(profile, resolution, rows), side_balance="ON")


def test_group_rank_is_unchanged_unless_neutral(setup):
    profile, _rows, resolution = setup
    short_put = next(group for group in resolution.groups
                     if group.underlying == "SPY" and len(group.candidates) == 1
                     and group.candidates[0].option_side is OptionSide.PUT
                     and (group.candidates[0].expiration - MONDAY).days == 1)
    default = _group_rank(short_put, profile, session_date=MONDAY)
    assert default[2] == 1
    assert _group_rank(short_put, profile, session_date=MONDAY, neutral_side=False) == default
    neutral = _group_rank(short_put, profile, session_date=MONDAY, neutral_side=True)
    assert neutral[2] == 0
    assert neutral[:2] + neutral[3:] == default[:2] + default[3:]


# --- ON: balance, caps, pairs, fills ----------------------------------------------------------------------------------

def _balanced(setup):
    profile, rows, resolution = setup
    return allocate_activity_momentum(profile, **_arguments(profile, resolution, rows), side_balance=True)


def test_on_balances_each_underlying_and_keeps_the_contract_count(setup):
    profile, rows, resolution = setup
    off = allocate_activity_momentum(profile, **_arguments(profile, resolution, rows))
    on = _balanced(setup)
    assert len(on.selected_contract_tickers) == len(off.selected_contract_tickers) == 48
    by_ticker = {row.contract_ticker: row for row in rows}
    for underlying in ("ACME", "SPY"):
        sides = Counter(by_ticker[t].option_side for t in on.selected_contract_tickers
                        if by_ticker[t].underlying == underlying)
        assert sides == Counter({OptionSide.CALL: 12, OptionSide.PUT: 12})
    near = Counter(by_ticker[t].option_side for t in on.selected_contract_tickers
                   if by_ticker[t].underlying == "SPY" and 1 <= (by_ticker[t].expiration - MONDAY).days <= 2)
    assert near[OptionSide.PUT] == near[OptionSide.CALL] > 0
    assert on.allocation_sha256 != off.allocation_sha256
    assert set(on.selected_contract_tickers) <= {row.contract_ticker for row in rows}


def _bucket_pass(setup, underlying, *, share_left=24):
    """The balanced bucket pass alone, recording what each bucket took (the later rounds are not run)."""
    profile, _rows, resolution = setup
    queue = sorted((g for g in resolution.groups if g.underlying == underlying),
                   key=lambda g: _group_rank(g, profile, session_date=MONDAY, neutral_side=True))
    evidence = {row.underlying: row for row in resolution.short_expiry_evidence}.get(underlying)
    taken = []

    def take(group):
        queue.remove(group)
        taken.append(group.candidates[0])
        return 1

    used = _v4_fill_product_buckets_side_balanced(
        profile, queue=queue, session_date=MONDAY, expiry_evidence=evidence, share_left=share_left, take=take,
        incremental=lambda group: 0 if group.candidates[0] in taken else 1)
    _product, ladder, ordinals, buckets = _v4_bucket_context(
        profile, groups=[g for g in resolution.groups if g.underlying == underlying], session_date=MONDAY,
        expiry_evidence=evidence)
    return used, taken, ladder, ordinals, buckets


def test_on_bucket_pass_caps_each_side_at_half_a_bucket(setup):
    for underlying in ("ACME", "SPY"):
        used, taken, ladder, ordinals, buckets = _bucket_pass(setup, underlying)
        assert used == len(taken) <= 24
        counts = Counter(
            (v4_contract_bucket(row, session_date=MONDAY, ladder=ladder, ordinals=ordinals, buckets=buckets),
             row.option_side)
            for row in taken)
        for bucket in buckets:
            calls, puts = counts[(bucket.bucket_id, OptionSide.CALL)], counts[(bucket.bucket_id, OptionSide.PUT)]
            assert calls <= (bucket.unique_contracts + 1) // 2 and puts <= (bucket.unique_contracts + 1) // 2
            assert calls + puts <= bucket.unique_contracts
    # The share bounds the pass.
    used, taken, *_ = _bucket_pass(setup, "SPY", share_left=5)
    assert used == len(taken) == 5


def test_on_caps_each_side_and_pairs_expiries(setup):
    profile, rows, resolution = setup
    on = _balanced(setup)
    by_ticker = {row.contract_ticker: row for row in rows}
    # SPY's NEAR bucket (Tuesday and Wednesday): calls and puts paired per expiry.
    for dte in (1, 2):
        sides = Counter(by_ticker[t].option_side for t in on.selected_contract_tickers
                        if by_ticker[t].underlying == "SPY" and (by_ticker[t].expiration - MONDAY).days == dte)
        assert sides[OptionSide.CALL] == sides[OptionSide.PUT]


def test_on_pairs_follow_the_same_ranking(setup):
    """Within a bucket expiry, the puts taken are the best puts of the one quality ranking (matched |delta|)."""
    profile, rows, resolution = setup
    on = _balanced(setup)
    selected = set(on.selected_contract_tickers)
    queue = sorted((g for g in resolution.groups if g.underlying == "SPY" and len(g.candidates) == 1),
                   key=lambda g: _group_rank(g, profile, session_date=MONDAY, neutral_side=True))
    for side in (OptionSide.CALL, OptionSide.PUT):
        same_day = [g.candidates[0].contract_ticker for g in queue
                    if g.candidates[0].option_side is side and g.candidates[0].expiration == MONDAY]
        taken = [ticker for ticker in same_day if ticker in selected]
        assert taken == same_day[:len(taken)] and len(taken) == 3


def test_on_fills_with_the_other_side_when_one_side_has_no_supply():
    profile = load_activity_momentum_profile(version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    rows = tuple(row for row in _fixture_rows() if row.underlying == "ACME" and row.option_side is OptionSide.CALL)
    resolution = _resolution(profile, rows)
    arguments = dict(session_date=MONDAY, rotation_key="al8-fixture", ready_underlyings=("ACME",),
                     groups=resolution.groups, resolution=resolution)
    off = allocate_activity_momentum(profile, **arguments)
    on = allocate_activity_momentum(profile, **arguments, side_balance=True)
    assert len(on.selected_contract_tickers) == len(off.selected_contract_tickers) == 24


def test_side_balanced_next_prefers_the_side_with_fewer_contracts(setup):
    _profile, _rows, resolution = setup
    spy = [g for g in resolution.groups if g.underlying == "SPY" and len(g.candidates) == 1]
    call = next(g for g in spy if g.candidates[0].option_side is OptionSide.CALL)
    put = next(g for g in spy if g.candidates[0].option_side is OptionSide.PUT)
    assert _side_balanced_next([call, put], Counter({OptionSide.CALL: 3, OptionSide.PUT: 1})) is put
    assert _side_balanced_next([put, call], Counter({OptionSide.CALL: 1, OptionSide.PUT: 3})) is call
    assert _side_balanced_next([call, put], Counter({OptionSide.CALL: 2, OptionSide.PUT: 2})) is call
    assert _side_balanced_next([call], Counter({OptionSide.CALL: 5})) is call
    assert _side_balanced_next([], Counter()) is None
    assert _side_counts([call, put, call]) == Counter({OptionSide.CALL: 1, OptionSide.PUT: 1})


# --- the switch ---------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, SlotSideBalanceMode.OFF), ("", SlotSideBalanceMode.OFF), ("  ", SlotSideBalanceMode.OFF),
    ("OFF", SlotSideBalanceMode.OFF), ("record", SlotSideBalanceMode.RECORD), (" On ", SlotSideBalanceMode.ON),
])
def test_mode_values(raw, expected):
    environ = {} if raw is None else {SLOT_SIDE_BALANCE_SETTING: raw}
    assert slot_side_balance_mode(environ) is expected


@pytest.mark.parametrize("raw", ["YES", "1", "true", "BALANCED", "RECORD_ONLY", "OFF,ON"])
def test_invalid_mode_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=SLOT_SIDE_BALANCE_SETTING):
        slot_side_balance_mode({SLOT_SIDE_BALANCE_SETTING: raw})


def test_executor_refuses_an_invalid_value_and_a_non_v4_profile():
    with pytest.raises(ValueError, match=SLOT_SIDE_BALANCE_SETTING):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            slot_side_balance="MAYBE")
    for mode in ("RECORD", "ON"):
        with pytest.raises(ValueError, match="slot side balance requires the v4"):
            PostgresAutomaticOptionsPreparationExecutor(
                None, api_key="k", backend_image_id="i",
                admission_profile_version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION, slot_side_balance=mode)
    # A valid v4 value passes the AL8 check (the next check, the provider, then fails as before).
    with pytest.raises(ValueError, match=r"configuration is invalid$"):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            slot_side_balance="RECORD")


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
    if value is None:
        monkeypatch.delenv(SLOT_SIDE_BALANCE_SETTING, raising=False)
    else:
        monkeypatch.setenv(SLOT_SIDE_BALANCE_SETTING, value)
    monkeypatch.setattr(resident, "dotenv_values", lambda *_: {})
    monkeypatch.setattr(resident, "_provider", lambda: (SimpleNamespace(), SimpleNamespace(closeall=lambda: None)))
    monkeypatch.setattr(resident, "PostgresAutomaticOptionsPreparationExecutor", executor)
    args = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2", "--continuous",
            "--execute"]
    return resident, seen, args


@pytest.mark.parametrize("value, expected", [
    (None, SlotSideBalanceMode.OFF), ("RECORD", SlotSideBalanceMode.RECORD), ("on", SlotSideBalanceMode.ON)])
def test_lifecycle_service_passes_the_switch(monkeypatch, value, expected):
    resident, seen, args = _run_lifecycle_main(monkeypatch, value)
    with pytest.raises(_Constructed):
        resident.main(args)
    assert seen["slot_side_balance"] is expected


def test_lifecycle_service_refuses_to_start_on_an_invalid_value(monkeypatch):
    resident, seen, args = _run_lifecycle_main(monkeypatch, "BALANCED")
    with pytest.raises(ValueError, match=SLOT_SIDE_BALANCE_SETTING):
        resident.main(args)
    assert seen == {}


# --- RECORD: OFF's allocation, the balanced one only logged -----------------------------------------------------------

def _record(setup, mode, *, allocate=allocate_activity_momentum, emit=None):
    profile, rows, resolution = setup
    lines = []
    allocation = allocate_with_side_balance(
        allocate, mode=mode, profile=profile, resolution=resolution, session_date=MONDAY,
        arguments=_arguments(profile, resolution, rows), emit=emit or (lambda event, doc: lines.append((event, doc))))
    return allocation, lines


def test_record_returns_offs_allocation_and_logs_the_balanced_one(setup):
    profile, rows, resolution = setup
    allocation, lines = _record(setup, SlotSideBalanceMode.RECORD)
    off = allocate_activity_momentum(profile, **_arguments(profile, resolution, rows))
    on = _balanced(setup)
    assert allocation.allocation_sha256 == GOLDEN_OFF_ALLOCATION_SHA256
    assert canonical_sha256(activity_momentum_allocation_payload(allocation)) == canonical_sha256(
        activity_momentum_allocation_payload(off))
    assert [event for event, _doc in lines] == [SLOT_SIDE_BALANCE_RECORD_EVENT] * 3
    summary, *per_underlying = [doc for _event, doc in lines]
    assert summary["part"] == "summary" and summary["mode"] == "RECORD" and summary["applied"] is False
    assert summary["off_allocation_sha256"] == GOLDEN_OFF_ALLOCATION_SHA256
    assert summary["balanced_allocation_sha256"] == on.allocation_sha256
    assert summary["contracts"] == {"off": {"calls": 39, "puts": 9}, "balanced": {"calls": 24, "puts": 24}}
    assert summary["dte_calls_puts"]["off"]["1-2"] == [6, 0]
    assert summary["dte_calls_puts"]["balanced"]["1-2"][1] > 0
    assert [doc["underlying"] for doc in per_underlying] == ["ACME", "SPY"]
    on_tickers, off_tickers = set(on.selected_contract_tickers), set(off.selected_contract_tickers)
    for doc in per_underlying:
        added = {row["contract"] for row in doc["added"]}
        removed = {row["contract"] for row in doc["removed"]}
        assert added == {t for t in on_tickers - off_tickers if doc["underlying"] in t}
        assert removed == {t for t in off_tickers - on_tickers if doc["underlying"] in t}
        assert doc["added_count"] == len(added) and doc["removed_count"] == len(removed)
        for row in doc["added"] + doc["removed"]:
            assert set(row) == {"contract", "side", "bucket", "dte", "abs_delta", "prior_oi", "day_volume",
                                "day_volume_session", "thin_oi", "thin_prior_volume", "thin"}
            # The fixture's volumes are stamped at 14:31 on the session (SESSION): no volume verdict.
            assert row["day_volume_session"] == "SESSION"
            assert row["thin_oi"] == (row["prior_oi"] < 100)
            assert row["thin_prior_volume"] is False
            assert row["thin"] == (row["thin_oi"] or row["thin_prior_volume"])
        assert sum(sum(value["off_calls_puts"]) for value in doc["buckets"].values()) == 24
        assert sum(sum(value["balanced_calls_puts"]) for value in doc["buckets"].values()) == 24
    assert summary["added"]["puts"] == sum(1 for doc in per_underlying for row in doc["added"] if row["side"] == "PUT")
    assert summary["thin_added"]["puts"] == sum(
        1 for doc in per_underlying for row in doc["added"] if row["side"] == "PUT" and row["thin"])
    assert summary["thin_policy"] == {"minimum_prior_open_interest": 100, "minimum_prior_session_volume": 500,
                                      "volume_judged_only_when_prior_session": True, "applied": False}


def test_record_flags_thin_balanced_puts():
    """Only thin puts listed: the balanced set adds them (ON does not filter) and the record flags every one."""
    profile = load_activity_momentum_profile(version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    rows = tuple(
        row if row.option_side is OptionSide.CALL else replace(
            row, prior_open_interest=20, completed_session_volume=40, volume_as_of=PREPARATION_AT - timedelta(days=3))
        for row in _fixture_rows() if row.underlying == "ACME")
    resolution = _resolution(profile, rows)
    lines = []
    allocate_with_side_balance(
        allocate_activity_momentum, mode=SlotSideBalanceMode.RECORD, profile=profile, resolution=resolution,
        session_date=MONDAY, arguments=dict(session_date=MONDAY, rotation_key="al8-fixture",
                                            ready_underlyings=("ACME",), groups=resolution.groups,
                                            resolution=resolution),
        emit=lambda event, doc: lines.append(doc))
    summary = lines[0]
    assert summary["added"]["puts"] > 0
    assert summary["thin_added"]["puts"] == summary["thin_added_oi"]["puts"] == summary["added"]["puts"]
    assert summary["thin_added_prior_volume"]["puts"] == summary["added"]["puts"]
    assert all(row["thin"] for row in lines[1]["added"] if row["side"] == "PUT")


def test_session_volume_is_not_judged_thin():
    """A capture after the open carries today's early volume for contracts that already traded: no volume verdict."""
    from realtime.advanced_options_slot_side_balance import _thin_flags, _volume_session

    row = _fixture_rows()[0]
    today = replace(row, completed_session_volume=7, volume_as_of=PREPARATION_AT, prior_open_interest=5_000)
    prior = replace(today, volume_as_of=PREPARATION_AT - timedelta(days=3))
    unknown = replace(today, completed_session_volume=None, volume_as_of=None)
    assert _volume_session(today, MONDAY) == "SESSION" and _thin_flags(today, MONDAY) == (False, False)
    assert _volume_session(prior, MONDAY) == "PRIOR" and _thin_flags(prior, MONDAY) == (False, True)
    assert _volume_session(unknown, MONDAY) == "UNKNOWN" and _thin_flags(unknown, MONDAY) == (False, False)
    assert _thin_flags(replace(today, prior_open_interest=None, open_interest_as_of=None), MONDAY) == (True, False)


def test_record_lines_are_json_under_the_docker_line_limit(setup, capsys):
    allocation, _ = _record(setup, SlotSideBalanceMode.RECORD, emit=_print_line)
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 3 and allocation.allocation_sha256 == GOLDEN_OFF_ALLOCATION_SHA256
    for line in err:
        event, document = line.split(" ", 1)
        assert event == SLOT_SIDE_BALANCE_RECORD_EVENT
        assert json.loads(document)["session_date"] == "2026-11-16"
        assert len(line.encode()) < 16_384


def test_record_never_fails_the_stage(setup):
    def balanced_fails(profile, **kwargs):
        if kwargs.get("side_balance"):
            raise RuntimeError("boom")
        return allocate_activity_momentum(profile, **kwargs)

    allocation, lines = _record(setup, SlotSideBalanceMode.RECORD, allocate=balanced_fails)
    assert allocation.allocation_sha256 == GOLDEN_OFF_ALLOCATION_SHA256
    assert [event for event, _doc in lines] == [SLOT_SIDE_BALANCE_RECORD_FAILED_EVENT]
    assert "RuntimeError: boom" in lines[0][1]["error"]

    def emit_fails(event, document):
        raise OSError("stderr closed")

    allocation, _ = _record(setup, SlotSideBalanceMode.RECORD, emit=emit_fails)
    assert allocation.allocation_sha256 == GOLDEN_OFF_ALLOCATION_SHA256


def test_on_through_the_helper_returns_the_balanced_allocation(setup):
    allocation, lines = _record(setup, SlotSideBalanceMode.ON)
    assert allocation.allocation_sha256 == _balanced(setup).allocation_sha256
    summary = lines[0][1]
    assert summary["applied"] is True and summary["mode"] == "ON"
    assert summary["balanced_allocation_sha256"] == allocation.allocation_sha256
    assert summary["off_allocation_sha256"] == GOLDEN_OFF_ALLOCATION_SHA256


def test_on_still_returns_the_balanced_allocation_when_the_record_fails(setup):
    def off_fails(profile, **kwargs):
        if not kwargs.get("side_balance"):
            raise RuntimeError("off failed")
        return allocate_activity_momentum(profile, **kwargs)

    allocation, lines = _record(setup, SlotSideBalanceMode.ON, allocate=off_fails)
    assert allocation.allocation_sha256 == _balanced(setup).allocation_sha256
    assert lines[0][0] == SLOT_SIDE_BALANCE_RECORD_FAILED_EVENT


def test_helper_refuses_a_non_v4_profile_unless_off(setup):
    _profile, _rows, resolution = setup
    v3 = load_activity_momentum_profile(version=SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION)
    with pytest.raises(ValueError, match="v4 admission profile"):
        allocate_with_side_balance(allocate_activity_momentum, mode="RECORD", profile=v3, resolution=resolution,
                                   session_date=MONDAY, arguments={})


def test_allocate_capture_routes_through_the_helper():
    """The lifecycle's CAPACITY_SHARDS allocation goes through the switch; executors built with __new__ (the lifecycle
    tests) have no attribute and read OFF."""
    import inspect

    from realtime import advanced_options_automatic_preparation as preparation

    source = inspect.getsource(preparation.PostgresAutomaticOptionsPreparationExecutor._allocate_capture)
    assert "allocate_with_side_balance(" in source
    assert "= allocate_activity_momentum(" not in source
    assert "SlotSideBalanceMode.OFF" in source and '"_slot_side_balance"' in source
