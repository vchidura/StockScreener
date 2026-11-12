"""R44 (Nov 12, night 1 of `G102_O3_INPUTS_DESIGN.md`): credit-vertical candidates for O3 from the window's watched
contracts. Pure functions; not wired into the window yet (switch `REALTIME_V3_O3_INPUTS`, off)."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from realtime import advanced_options_o3_inputs as o3
from realtime.advanced_options_o3_inputs import LegQuote, O3InputPolicy, WatchedContract

UTC = timezone.utc
SESSION = date(2026, 11, 11)
CUTOFF = datetime(2026, 11, 11, 18, 5, tzinfo=UTC)
EXPIRY = date(2026, 11, 27)  # 16 DTE
SPOT = {"SPY": Decimal("600")}


def contract(instrument_id, side, strike, expiry=EXPIRY, underlying="SPY"):
    return WatchedContract(
        instrument_id=instrument_id, contract_id=1000 + instrument_id, catalog_version_id=1,
        contract_ticker=f"O:{underlying}{expiry:%y%m%d}{side}{int(strike * 1000):08d}",
        underlying=underlying, call_put=side, strike=Decimal(str(strike)), expiration=expiry, multiplier=100,
    )


def quote(instrument_id, bid, ask, volume=100, oi=1000, at=CUTOFF - timedelta(seconds=5)):
    return LegQuote(instrument_id, Decimal(str(bid)), Decimal(str(ask)), at, volume, oi)


def test_switch_is_off_by_default(monkeypatch):
    monkeypatch.delenv(o3.O3_INPUTS_SWITCH, raising=False)
    assert o3.o3_inputs_enabled() is False
    monkeypatch.setenv(o3.O3_INPUTS_SWITCH, "on")
    assert o3.o3_inputs_enabled() is True


def test_pairs_follow_the_c6_pair_rule_and_roles():
    rows = [contract(1, "P", 590), contract(2, "P", 595), contract(3, "C", 605), contract(4, "C", 610)]
    pairs = o3.credit_vertical_pairs(rows, session_date=SESSION, spot_by_underlying=SPOT, policy=O3InputPolicy())
    by_side = {short.call_put: (short.strike, long.strike) for short, long in pairs}
    # Puts: short the higher strike, long the lower wing; calls: short the lower strike, long the higher wing.
    assert by_side == {"P": (Decimal("595"), Decimal("590")), "C": (Decimal("605"), Decimal("610"))}


def test_pairs_need_dte_inside_policy_width_inside_bound_and_a_spot():
    near = SESSION + timedelta(days=3)
    rows = [
        contract(1, "P", 590, expiry=near), contract(2, "P", 595, expiry=near),  # 3 DTE: outside 7-30
        contract(3, "C", 600), contract(4, "C", 615),  # width 15 / 600 = 2.5% > 2%
        contract(5, "C", 100, underlying="QQQ"), contract(6, "C", 101, underlying="QQQ"),  # no spot
    ]
    assert o3.credit_vertical_pairs(rows, session_date=SESSION, spot_by_underlying=SPOT, policy=O3InputPolicy()) == ()


def test_a_put_credit_vertical_is_priced_from_midpoints_at_the_cutoff():
    short, long = contract(2, "P", 595), contract(1, "P", 590)
    quotes = {2: quote(2, "3.00", "3.20", volume=400, oi=1000), 1: quote(1, "1.90", "2.10", volume=300, oi=500)}
    row = o3.price_credit_vertical(short, long, quotes, cutoff=CUTOFF, policy=O3InputPolicy())
    assert row.structure_type == "PUT_CREDIT_VERTICAL" and row.direction == 1
    assert (row.short_mark, row.long_mark, row.net_credit) == (Decimal("3.10"), Decimal("2.00"), Decimal("1.10"))
    assert row.width == Decimal("5") and row.maximum_loss == Decimal("3.90")
    assert row.breakeven == Decimal("593.90")
    assert row.return_on_risk == Decimal("1.10") / Decimal("3.90")
    # Activity: the higher volume / OI leg (long 300/500 = 0.6 beats short 400/1000 = 0.4).
    assert row.activity_instrument_id == 1


def test_a_call_credit_vertical_has_its_breakeven_above_the_short_strike():
    short, long = contract(3, "C", 605), contract(4, "C", 610)
    quotes = {3: quote(3, "4.00", "4.20"), 4: quote(4, "2.00", "2.20")}
    row = o3.price_credit_vertical(short, long, quotes, cutoff=CUTOFF, policy=O3InputPolicy())
    assert row.structure_type == "CALL_CREDIT_VERTICAL" and row.direction == -1
    assert row.breakeven == Decimal("607.00")


@pytest.mark.parametrize("quotes, reason", [
    ({2: quote(2, "3.00", "3.20")}, "O3_LEG_MARK_UNAVAILABLE"),
    ({2: quote(2, "3.00", "3.20"), 1: quote(1, "2.10", "1.90")}, "O3_LEG_MARK_UNAVAILABLE"),  # crossed
    ({2: quote(2, "3.00", "3.20"), 1: quote(1, "1.90", "2.10", at=CUTOFF + timedelta(seconds=1))},
     "O3_LEG_MARK_AFTER_CUTOFF"),
    ({2: quote(2, "3.00", "3.20", oi=499), 1: quote(1, "1.90", "2.10")}, "O3_LEG_ACTIVITY_BELOW_POLICY"),
    ({2: quote(2, "3.00", "3.20", volume=49), 1: quote(1, "1.90", "2.10")}, "O3_LEG_ACTIVITY_BELOW_POLICY"),
    ({2: quote(2, "1.90", "2.10"), 1: quote(1, "1.90", "2.10")}, "O3_NON_POSITIVE_CREDIT"),
    ({2: quote(2, "7.90", "8.10"), 1: quote(1, "1.90", "2.10")}, "O3_CREDIT_NOT_BOUNDED"),
])
def test_unbuildable_verticals_get_a_reason_and_never_raise(quotes, reason):
    row = o3.price_credit_vertical(contract(2, "P", 595), contract(1, "P", 590), quotes, cutoff=CUTOFF,
                                   policy=O3InputPolicy())
    assert row == reason


def test_selection_bounds_per_underlying_ranks_by_credit_per_width_and_counts_rejections():
    rows = [contract(i, "P", strike) for i, strike in enumerate((585, 588, 591, 594, 597), start=1)]
    marks = {1: "1.00", 2: "1.50", 3: "2.20", 4: "3.10", 5: "4.20"}
    quotes = {i: quote(i, Decimal(mark) - Decimal("0.05"), Decimal(mark) + Decimal("0.05")) for i, mark in marks.items()}
    selection = o3.select_o3_candidates(rows, quotes, session_date=SESSION, cutoff=CUTOFF, spot_by_underlying=SPOT)
    assert [row.rank for row in selection.candidates] == [1, 2]
    credits = [row.net_credit for row in selection.candidates]
    assert credits == [Decimal("1.10"), Decimal("0.90")]  # 597/594 then 594/591, both width 3
    assert dict(selection.rejections) == {"O3_CANDIDATE_BOUND": 2}
    assert selection.judged_instrument_ids == frozenset({5, 4, 3})


def test_selection_is_deterministic_and_identities_depend_on_marks_and_cutoff():
    rows = [contract(1, "P", 590), contract(2, "P", 595)]
    quotes = {2: quote(2, "3.00", "3.20"), 1: quote(1, "1.90", "2.10")}
    first = o3.select_o3_candidates(rows, quotes, session_date=SESSION, cutoff=CUTOFF, spot_by_underlying=SPOT)
    again = o3.select_o3_candidates(list(reversed(rows)), quotes, session_date=SESSION, cutoff=CUTOFF,
                                    spot_by_underlying=SPOT)
    later = o3.select_o3_candidates(rows, quotes, session_date=SESSION, cutoff=CUTOFF + timedelta(minutes=5),
                                    spot_by_underlying=SPOT)
    assert first == again
    assert first.candidates[0].identity_sha256 != later.candidates[0].identity_sha256


def test_policy_reads_the_strategy_document():
    from pathlib import Path
    import json

    document = json.loads((Path(o3.__file__).parents[1] / "options" / "policies" / "strategy_o3_credit_v2.json")
                          .read_text(encoding="utf-8"))
    policy = O3InputPolicy.from_strategy_document(document)
    assert (policy.minimum_entry_dte, policy.maximum_entry_dte) == (7, 30)
    assert (policy.minimum_leg_open_interest, policy.minimum_leg_day_volume) == (500, 50)
    assert policy.maximum_center_distance_fraction == Decimal("0.02")
