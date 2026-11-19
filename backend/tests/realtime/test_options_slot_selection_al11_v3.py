"""G106 AL11 v3 (Nov 19): `REALTIME_V3_OPTIONS_SLOT_SELECTION_POLICY` (options-lifecycle) picks the selection rule.

Unset / blank / v2 = the v2 rule byte for byte (its RECORD lines unchanged); v3 = v2's previous-volume ranking after
protecting, from today's picks, the legs predicted O1-ready from the 3x OI rule's pre-open inputs and their nearest
further-OTM vertical partner (at most half a name's slots); anything else refuses to start."""
from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal
from types import SimpleNamespace

import pytest

from realtime import advanced_options_slot_selection as selection_module
from realtime.advanced_options_activity_momentum_c6 import (
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    activity_momentum_allocation_payload,
    allocate_activity_momentum,
)
from realtime.advanced_options_automatic_preparation import PostgresAutomaticOptionsPreparationExecutor
from realtime.advanced_options_selection_c6 import OptionSide
from realtime.advanced_options_slot_selection import (
    SLOT_SELECTION_POLICY,
    SLOT_SELECTION_POLICY_SETTING,
    SLOT_SELECTION_POLICY_SHA256,
    SLOT_SELECTION_POLICY_V3,
    SLOT_SELECTION_POLICY_V3_SHA256,
    SLOT_SELECTION_RECORD_EVENT,
    SlotSelectionMode,
    allocate_with_slot_selection,
    parse_slot_selection_policy,
    plan_slot_selection,
    predicted_o1_ready,
    slot_selection_policy,
)
from realtime.contracts import canonical_sha256
from tests.realtime.test_options_slot_selection_al11 import (  # noqa: F401 - `setup` is the shared module fixture
    FRIDAY,
    FRIDAY_CLOSE,
    _Constructed,
    _chain,
    _chain_rows,
    _run_lifecycle_main,
    setup,
)
from tests.realtime.test_options_slot_side_balance_al8 import MONDAY, _arguments, _resolution


def _plan(setup, policy, *, previous=None, side_balance=False):
    profile, rows, resolution, off = setup
    return plan_slot_selection(profile=profile, candidates=rows, resolution=resolution, off=off, session_date=MONDAY,
                               previous=_chain(rows) if previous is None else previous, side_balance=side_balance,
                               policy=policy)


def _helper(setup, mode, policy, *, side_balance=False):
    profile, rows, resolution, off = setup
    lines = []

    def allocate(restricted):
        arguments = {**_arguments(profile, restricted, restricted), "groups": restricted.groups,
                     "resolution": restricted}
        if side_balance:
            arguments["side_balance"] = True
        return allocate_activity_momentum(profile, **arguments)

    kwargs = {} if policy is None else {"policy": policy}
    result = allocate_with_slot_selection(
        mode=mode, used=off, profile=profile, resolution=resolution, candidates=rows, session_date=MONDAY,
        side_balance=side_balance, previous=lambda: _chain(rows), resolve=lambda r: _resolution(profile, r),
        allocate=allocate, emit=lambda event, document: lines.append((event, document)), **kwargs)
    return result, lines


def _by_ticker(setup):
    return {row.contract_ticker: row for row in setup[1]}


# --- the setting ----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    (None, SLOT_SELECTION_POLICY), ("", SLOT_SELECTION_POLICY), (" ", SLOT_SELECTION_POLICY),
    ("v2", SLOT_SELECTION_POLICY), ("V2", SLOT_SELECTION_POLICY), ("options-slot-selection-v2", SLOT_SELECTION_POLICY),
    ("v3", SLOT_SELECTION_POLICY_V3), (" V3 ", SLOT_SELECTION_POLICY_V3),
    ("options-slot-selection-v3", SLOT_SELECTION_POLICY_V3),
])
def test_policy_values(raw, expected):
    environ = {} if raw is None else {SLOT_SELECTION_POLICY_SETTING: raw}
    assert slot_selection_policy(environ) is expected
    assert parse_slot_selection_policy(raw) is expected
    assert parse_slot_selection_policy(expected) is expected


@pytest.mark.parametrize("raw", ["v1", "v4", "ON", "RECORD", "3", "v2,v3", 3])
def test_invalid_policy_raises_naming_the_key(raw):
    with pytest.raises(ValueError, match=SLOT_SELECTION_POLICY_SETTING):
        parse_slot_selection_policy(raw)
    if isinstance(raw, str):
        with pytest.raises(ValueError, match=SLOT_SELECTION_POLICY_SETTING):
            slot_selection_policy({SLOT_SELECTION_POLICY_SETTING: raw})
    with pytest.raises(ValueError, match=SLOT_SELECTION_POLICY_SETTING):
        parse_slot_selection_policy(dict(SLOT_SELECTION_POLICY_V3))  # an equal copy is not a registered policy


def test_the_policies_differ_only_by_the_v3_keys():
    v2, v3 = dict(SLOT_SELECTION_POLICY), dict(SLOT_SELECTION_POLICY_V3)
    assert v2["version"] == "options-slot-selection-v2" and v3["version"] == "options-slot-selection-v3"
    assert {key: v3[key] for key in v2 if key != "version"} == {key: v2[key] for key in v2 if key != "version"}
    assert SLOT_SELECTION_POLICY_SHA256 == canonical_sha256(v2)
    assert SLOT_SELECTION_POLICY_V3_SHA256 == canonical_sha256(v3) != SLOT_SELECTION_POLICY_SHA256
    assert v3["protect_products"] == ("DAILY_EXPIRY", "WEEKLY_EXPIRY")
    assert v3["protect_maximum_share_of_target"] == "0.5"


# --- v2 stays byte for byte ------------------------------------------------------------------------------------------

@pytest.mark.parametrize("mode", [SlotSelectionMode.RECORD, SlotSelectionMode.ON])
def test_v2_unset_and_explicit_are_identical(setup, mode):
    default, default_lines = _helper(setup, mode, None)
    for policy in ("v2", "", SLOT_SELECTION_POLICY):
        explicit, lines = _helper(setup, mode, policy)
        assert explicit.allocation_sha256 == default.allocation_sha256
        assert json.dumps(lines, sort_keys=True, default=str) == json.dumps(default_lines, sort_keys=True, default=str)
    for _event, document in default_lines:
        assert document["policy_version"] == "options-slot-selection-v2"
        assert document["policy_sha256"] == SLOT_SELECTION_POLICY_SHA256
        assert not {"protected", "v2_minus_v3", "v3_minus_v2"} & set(document)
    plan = _plan(setup, None)
    assert all(entry.protected == () and entry.v2_chosen is None for entry in plan.underlyings.values())


def test_v2_setting_line_is_unchanged(capsys):
    selection_module.log_slot_selection_setting(SlotSelectionMode.RECORD)
    selection_module.log_slot_selection_setting(SlotSelectionMode.RECORD, SLOT_SELECTION_POLICY)
    selection_module.log_slot_selection_setting(SlotSelectionMode.RECORD, SLOT_SELECTION_POLICY_V3)
    first, second, third = capsys.readouterr().err.strip().splitlines()
    assert first == second
    setting = "REALTIME_V3_OPTIONS_SLOT_SELECTION"
    assert json.loads(first.partition(" ")[2]) == {
        "mode": "RECORD", "setting": setting, "policy_version": "options-slot-selection-v2"}
    assert json.loads(third.partition(" ")[2]) == {
        "mode": "RECORD", "setting": setting, "policy_version": "options-slot-selection-v3",
        "policy_setting": SLOT_SELECTION_POLICY_SETTING}


# --- v3: protection ---------------------------------------------------------------------------------------------------

def test_v3_protects_predicted_o1_ready_legs_from_todays_picks(setup):
    profile, rows, resolution, off = setup
    by_ticker = _by_ticker(setup)
    chain = _chain(rows)
    v2 = _plan(setup, "v2")
    v3 = _plan(setup, "v3")
    today = set(off.selected_contract_tickers)
    kept_from_v2_drop = 0
    for underlying, entry in v3.underlyings.items():
        assert entry.applied and len(entry.chosen) == entry.target == 24
        assert entry.v2_chosen == v2.underlyings[underlying].chosen
        assert 0 < len(entry.protected) <= 12  # at most half the name's slots
        protected = dict(entry.protected)
        assert set(protected) <= today and set(protected) <= set(entry.chosen)
        o1 = [by_ticker[t] for t, why in entry.protected if why == "O1_READY"]
        assert all(predicted_o1_ready(row, chain, MONDAY) for row in o1)
        # Every leg of today's picks predicted O1-ready is protected (the cap is not reached here), and the 3x rule
        # applies beyond 7 DTE, 1x up to it.
        predicted = {t for t in today if by_ticker[t].underlying == underlying
                     and predicted_o1_ready(by_ticker[t], chain, MONDAY)}
        assert predicted == {row.contract_ticker for row in o1}
        for row in o1:
            ratio = 1 if row.dte_for(MONDAY) <= 7 else 3
            assert chain.volumes[row.contract_ticker] >= ratio * row.prior_open_interest
            assert row.dte_for(MONDAY) not in (0, 3, 4)
        kept_from_v2_drop += len(set(protected) - set(entry.v2_chosen))
        # Protected in previous-volume order of the O1-ready legs.
        ranked = sorted(o1, key=lambda row: (-chain.volumes[row.contract_ticker], -row.prior_open_interest,
                                             row.contract_ticker))
        assert [row.contract_ticker for row in o1] == [row.contract_ticker for row in ranked]
    assert kept_from_v2_drop > 0  # v2's ranking (puts, far expiries) dropped legs v3 keeps


def _quiet_chain(rows, loud):
    """Every contract traded 150 the previous session (below its open interest: not O1-ready, yet a liquid partner)
    except `loud` (ticker -> volume)."""
    return selection_module.previous_session_chain_from_rows(
        [(underlying, ticker, expiration, loud.get(ticker, 150), bid, ask, observed_at)
         for underlying, ticker, expiration, _volume, bid, ask, observed_at in _chain_rows(rows)],
        previous_session_date=FRIDAY, previous_close=FRIDAY_CLOSE, session_date=MONDAY)


def _leg(rows, underlying, dte, side, strike):
    return next(row for row in rows if row.underlying == underlying and row.dte_for(MONDAY) == dte
                and row.option_side is side and row.strike == Decimal(strike))


def test_v3_protects_the_nearest_further_otm_partner(setup):
    profile, rows, resolution, off = setup
    today = set(off.selected_contract_tickers)
    long = _leg(rows, "SPY", 1, OptionSide.CALL, "99")
    partner = _leg(rows, "SPY", 1, OptionSide.CALL, "100")
    assert {long.contract_ticker, partner.contract_ticker} <= today
    chain = _quiet_chain(rows, {long.contract_ticker: 2_000})
    assert predicted_o1_ready(long, chain, MONDAY) and not predicted_o1_ready(partner, chain, MONDAY)
    plan = _plan(setup, "v3", previous=chain)
    spy = plan.underlyings["SPY"]
    assert spy.protected == ((long.contract_ticker, "O1_READY"), (partner.contract_ticker, "PARTNER"))
    assert {long.contract_ticker, partner.contract_ticker} <= set(spy.chosen)
    assert plan.underlyings["ACME"].protected == ()
    # A put's partner is the next lower strike; a partner below the policy's liquidity is skipped for the next one.
    put = _leg(rows, "SPY", 1, OptionSide.PUT, "101")
    put_partner = _leg(rows, "SPY", 1, OptionSide.PUT, "100")
    if {put.contract_ticker, put_partner.contract_ticker} <= today:
        chain = _quiet_chain(rows, {put.contract_ticker: 2_000, put_partner.contract_ticker: 50})
        found = selection_module.vertical_partner(put, [by for by in setup[1] if by.contract_ticker in today], chain)
        assert found is None or (found.strike < put_partner.strike and found.option_side is OptionSide.PUT)


def test_v3_cap_limits_protection_to_half_the_slots(setup):
    profile, rows, resolution, off = setup
    today = [row for row in rows if row.contract_ticker in set(off.selected_contract_tickers)]
    loud = {row.contract_ticker: 50_000 for row in today
            if row.dte_for(MONDAY) not in (0, 3, 4) and row.prior_open_interest >= 100}
    plan = _plan(setup, "v3", previous=_quiet_chain(rows, loud))
    for entry in plan.underlyings.values():
        assert len(entry.protected) == min(12, sum(1 for t in loud if t in entry.off))
        assert set(dict(entry.protected)) <= set(entry.chosen) and len(entry.chosen) == 24


def test_v3_keeps_the_0dte_count_and_the_shortlist_bounds(setup):
    profile, rows, resolution, off = setup
    by_ticker = _by_ticker(setup)
    result, lines = _helper(setup, SlotSelectionMode.ON, "v3")
    assert result.allocation_sha256 not in (off.allocation_sha256, _helper(setup, SlotSelectionMode.ON, "v2")[0]
                                            .allocation_sha256)
    assert lines[0][1]["fallback_underlyings"] == {}
    for underlying in ("ACME", "SPY"):
        picked = [by_ticker[t] for t in result.selected_contract_tickers if by_ticker[t].underlying == underlying]
        today = [by_ticker[t] for t in off.selected_contract_tickers if by_ticker[t].underlying == underlying]
        assert len(picked) == len(today) == 24
        assert sum(1 for r in picked if r.dte_for(MONDAY) == 0) == sum(1 for r in today if r.dte_for(MONDAY) == 0)
        if underlying == "SPY":
            # The protected legs (all calls here) do not tilt the 0DTE picks: calls and puts still alternate.
            assert Counter(r.option_side for r in picked if r.dte_for(MONDAY) == 0) == Counter(
                {OptionSide.CALL: 3, OptionSide.PUT: 3})
        assert not any(row.dte_for(MONDAY) in (3, 4) for row in picked)
        extension = Counter(row.option_side for row in picked if row.dte_for(MONDAY) >= 22)
        assert all(count <= 8 for count in extension.values())


def test_v3_cap_and_scope_follow_the_policy(setup, monkeypatch):
    rows = setup[1]
    narrow = selection_module.MappingProxyType({**SLOT_SELECTION_POLICY_V3, "protect_maximum_share_of_target": "0.25",
                                                "protect_products": ("DAILY_EXPIRY",)})
    monkeypatch.setattr(selection_module, "SLOT_SELECTION_POLICY_V3", narrow)
    monkeypatch.setattr(selection_module, "SLOT_SELECTION_POLICIES",
                        selection_module.MappingProxyType({"v2": SLOT_SELECTION_POLICY, "v3": narrow}))
    plan = plan_slot_selection(profile=setup[0], candidates=rows, resolution=setup[2], off=setup[3],
                               session_date=MONDAY, previous=_chain(rows), side_balance=False, policy="v3")
    assert plan.underlyings["ACME"].protected == ()  # weekly product, not protected here
    assert plan.underlyings["ACME"].chosen == plan.underlyings["ACME"].v2_chosen
    assert 0 < len(plan.underlyings["SPY"].protected) <= 6


def test_v3_without_predicted_legs_is_v2(setup):
    rows = setup[1]
    # Previous volume below every leg's open interest: nothing predicted O1-ready, nothing protected.
    quiet = selection_module.previous_session_chain_from_rows(
        [(underlying, ticker, expiration, 1, bid, ask, observed_at)
         for underlying, ticker, expiration, _volume, bid, ask, observed_at in _chain_rows(rows)],
        previous_session_date=FRIDAY, previous_close=FRIDAY_CLOSE, session_date=MONDAY)
    plan = _plan(setup, "v3", previous=quiet)
    for entry in plan.underlyings.values():
        if entry.applied:
            assert entry.protected == () and entry.chosen == entry.v2_chosen


# --- v3: RECORD -------------------------------------------------------------------------------------------------------

def test_v3_record_returns_todays_allocation_and_its_lines_carry_v3_and_rebuild_v2(setup):
    profile, rows, resolution, off = setup
    result, lines = _helper(setup, SlotSelectionMode.RECORD, "v3")
    assert result is off
    assert canonical_sha256(activity_momentum_allocation_payload(result)) == canonical_sha256(
        activity_momentum_allocation_payload(off))
    v2_on, _ = _helper(setup, SlotSelectionMode.ON, "v2")
    v3_on, _ = _helper(setup, SlotSelectionMode.ON, "v3")
    assert [event for event, _ in lines] == [SLOT_SELECTION_RECORD_EVENT] * 3
    summary, *names = [document for _event, document in lines]
    for document in (summary, *names):
        assert document["policy_version"] == "options-slot-selection-v3"
        assert document["policy_sha256"] == SLOT_SELECTION_POLICY_V3_SHA256
        assert len(json.dumps(document, sort_keys=True, default=str, separators=(",", ":")).encode()) < 16 * 1024 - 64
    assert json.loads(json.dumps(summary["policy"])) == json.loads(json.dumps(dict(SLOT_SELECTION_POLICY_V3)))
    assert summary["protected"]["o1_ready"] > 0 and set(summary["protected"]) == {"o1_ready", "partner"}
    assert summary["selection_allocation_sha256"] == v3_on.allocation_sha256
    by_ticker = _by_ticker(setup)
    rebuilt_v3, rebuilt_v2 = set(), set()
    for line in names:
        today = {t for t in off.selected_contract_tickers if by_ticker[t].underlying == line["underlying"]}
        v3 = (today - {x["contract"] for x in line["dropped"]}) | {x["contract"] for x in line["added"]}
        rebuilt_v3 |= v3
        rebuilt_v2 |= (v3 - set(line["v3_minus_v2"])) | set(line["v2_minus_v3"])
        assert all(why in ("O1_READY", "PARTNER") and ticker in today for ticker, why in line["protected"])
    assert rebuilt_v3 == set(v3_on.selected_contract_tickers)
    assert rebuilt_v2 == set(v2_on.selected_contract_tickers)


# --- wiring -----------------------------------------------------------------------------------------------------------

def test_executor_refuses_an_invalid_policy_and_keeps_v2_by_default():
    with pytest.raises(ValueError, match=SLOT_SELECTION_POLICY_SETTING):
        PostgresAutomaticOptionsPreparationExecutor(
            None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            slot_selection="RECORD", slot_selection_policy="v9")
    # A valid policy passes the AL11 checks (the next check, the provider, then fails as before).
    for policy in (None, "v2", "v3"):
        with pytest.raises(ValueError, match=r"configuration is invalid$"):
            PostgresAutomaticOptionsPreparationExecutor(
                None, api_key="k", backend_image_id="i", admission_profile_version=V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
                slot_selection="RECORD", slot_selection_policy=policy)


def test_executor_passes_its_policy_to_the_helper(setup, monkeypatch):
    from realtime import advanced_options_automatic_preparation as preparation

    seen = []
    monkeypatch.setattr(preparation, "allocate_with_slot_selection", lambda **kwargs: seen.append(kwargs) or "done")
    for attributes, expected in (({}, None), ({"_slot_selection_policy": SLOT_SELECTION_POLICY_V3},
                                               SLOT_SELECTION_POLICY_V3)):
        executor = object.__new__(PostgresAutomaticOptionsPreparationExecutor)
        executor._enabled_strategies = ()
        for name, value in attributes.items():
            setattr(executor, name, value)
        claim = SimpleNamespace(session_date=MONDAY)
        assert executor._slot_selection_allocation(
            claim, SlotSelectionMode.RECORD, allocation=None, profile=None, resolution=None, candidates=(),
            resolution_arguments={}, allocation_arguments={}, previous=lambda: None) == "done"
        assert seen[-1]["policy"] is expected


@pytest.mark.parametrize("value, expected", [
    (None, SLOT_SELECTION_POLICY), ("v2", SLOT_SELECTION_POLICY), ("V3", SLOT_SELECTION_POLICY_V3)])
def test_lifecycle_service_passes_the_policy(monkeypatch, value, expected):
    resident, seen, args = _run_lifecycle_main(monkeypatch, "RECORD")
    if value is None:
        monkeypatch.delenv(SLOT_SELECTION_POLICY_SETTING, raising=False)
    else:
        monkeypatch.setenv(SLOT_SELECTION_POLICY_SETTING, value)
    with pytest.raises(_Constructed):
        resident.main(args)
    assert seen["slot_selection_policy"] is expected
    assert seen["slot_selection"] is SlotSelectionMode.RECORD


def test_lifecycle_service_refuses_to_start_on_an_invalid_policy(monkeypatch):
    resident, seen, args = _run_lifecycle_main(monkeypatch, "RECORD")
    monkeypatch.setenv(SLOT_SELECTION_POLICY_SETTING, "v9")
    with pytest.raises(ValueError, match=SLOT_SELECTION_POLICY_SETTING):
        resident.main(args)
    assert seen == {}
