"""G106 OV1-F2: the chain capture solves our IV and Greeks at a spot consistent with the quotes (no provider calls).

REALTIME_V3_BREADTH_SPOT_SOURCE: unset/BAR = today's capture exactly; RECORD = the same rows plus one
BREADTH_SPOT_RECORD line per underlying; QUOTE = solved at the parity (else provider) spot, falling back to the bar
with a reason; anything else stops the Options scheduler at start."""
from __future__ import annotations

import importlib
import json
import math
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime import advanced_options_breadth as breadth
from realtime import options_breadth_spot as spotting

UTC = timezone.utc
SESSION = date(2026, 11, 12)
CUTOFF = datetime(2026, 11, 12, 19, 5, tzinfo=UTC)
NOW = datetime(2026, 11, 12, 19, 7, 20, tzinfo=UTC)
TRUE_SPOT = 689.12
BAR_SPOT = Decimal("688.00")
RATE = 0.0405
SWITCH = spotting.BREADTH_SPOT_SOURCE_SETTING
NANOS = int(NOW.timestamp() * 1_000_000_000)


def _price(kind, spot, strike, years, rate, dividend, volatility):
    root = math.sqrt(years)
    d1 = (math.log(spot / strike) + (rate - dividend + 0.5 * volatility * volatility) * years) / (volatility * root)
    d2 = d1 - volatility * root
    n = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))  # noqa: E731
    if kind == "call":
        return spot * math.exp(-dividend * years) * n(d1) - strike * math.exp(-rate * years) * n(d2)
    return strike * math.exp(-rate * years) * n(-d2) - spot * math.exp(-dividend * years) * n(-d1)


def _contract(kind, strike, expiration, *, spot=TRUE_SPOT, volatility=0.22, half_spread=0.02, underlying_price=None,
              timeframe="REAL-TIME", underlying_age_seconds=5):
    """A QQQ contract quoted around its Black-Scholes value at `spot` (the quotes' own spot)."""
    from realtime.advanced_options_projection import _time_to_expiration_years

    years = _time_to_expiration_years(date.fromisoformat(expiration), NOW)
    value = _price(kind, spot, strike, years, RATE, 0.0, volatility)
    bid, ask = max(round(value - half_spread, 2), 0.01), round(value + half_spread, 2)
    ticker = f"O:QQQ{expiration[2:4]}{expiration[5:7]}{expiration[8:10]}{kind[0].upper()}{int(strike * 1000):08d}"
    return {
        "details": {"contract_type": kind, "expiration_date": expiration, "strike_price": strike, "ticker": ticker},
        "day": {"volume": 500}, "open_interest": 1000,
        "greeks": {"delta": 0.5, "gamma": 0.05, "theta": -0.3, "vega": 0.2}, "implied_volatility": volatility,
        "last_quote": {"bid": bid, "ask": ask, "last_updated": NANOS - 3_000_000_000},
        "last_trade": {},
        "underlying_asset": {"price": underlying_price if underlying_price is not None else round(spot, 2),
                             "ticker": "QQQ", "timeframe": timeframe,
                             "last_updated": NANOS - int(underlying_age_seconds * 1_000_000_000)},
    }


def _chain(strikes=(684, 686, 688, 689, 690, 692, 694), expirations=("2026-11-12", "2026-11-13"), **kwargs):
    return [_contract(kind, strike, expiration, **kwargs)
            for expiration in expirations for strike in strikes for kind in ("call", "put")]


def _rows(contracts):
    rows, skipped = breadth.parse_breadth_page({"results": contracts}, underlying="QQQ")
    assert skipped == 0
    return rows


def _inputs():
    return tuple({"ticker": "QQQ", "expiration_date": expiration, "risk_free_rate": str(RATE), "dividend_yield": "0",
                  "model_version": "black-scholes-merton-v1"} for expiration in ("2026-11-12", "2026-11-13"))


class _Response:
    def __init__(self, document):
        self.status_code = 200
        self.body = json.dumps(document).encode()


class _Transport:
    def __init__(self, documents):
        self.pages = [_Response(item) for item in documents]

    def get(self, url, params):
        return self.pages.pop(0)


class _Provider:
    def __init__(self, inputs):
        self.cursor = MagicMock()
        self.cursor.fetchone.return_value = (list(inputs),)

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield SimpleNamespace(cursor=self.cursor)


def _capture(monkeypatch, contracts, *, mode=None, bar=BAR_SPOT, inputs=None):
    """One capture of QQQ through `capture_options_breadth`; the stored rows and the summary."""
    if mode is None:
        monkeypatch.delenv(SWITCH, raising=False)
    else:
        monkeypatch.setenv(SWITCH, mode)
    stored = []
    monkeypatch.setattr(breadth, "reserve_coordinated_provider_request_cursor", lambda *_a, **_k: None)
    monkeypatch.setattr(breadth, "finish_provider_request_cursor", lambda *_a, **_k: None)
    monkeypatch.setattr(breadth, "execute_values", lambda _cursor, _sql, rows, **_k: stored.extend(rows))
    summary = breadth.capture_options_breadth(
        _Provider(_inputs() if inputs is None else inputs), session_date=SESSION, underlyings=("QQQ",),
        spots={"QQQ": bar}, equity_cutoff=CUTOFF, cohort_id=UUID(int=7), authorization_reference="AUTH",
        authorized_until=NOW + timedelta(hours=2), transport=_Transport([{"results": contracts}]),
        deadline=NOW + timedelta(minutes=2), clock=lambda: NOW,
    )
    return {row[1]: row for row in stored}, summary


_COLUMNS = breadth._COLUMNS.split(",")


def _column(row, name):
    return row[_COLUMNS.index(name)]


def _records(captured_err):
    return [json.loads(line.split(" ", 1)[1]) for line in captured_err.splitlines()
            if line.startswith(spotting.BREADTH_SPOT_RECORD_EVENT + " ")]


# ---- the switch -----------------------------------------------------------------------------------------------------

def test_the_switch_defaults_to_bar_and_refuses_anything_unknown():
    assert spotting.breadth_spot_mode({}) is spotting.BreadthSpotMode.BAR
    assert spotting.breadth_spot_mode({SWITCH: "  "}) is spotting.BreadthSpotMode.BAR
    assert spotting.breadth_spot_mode({SWITCH: "bar"}) is spotting.BreadthSpotMode.BAR
    assert spotting.breadth_spot_mode({SWITCH: " record "}) is spotting.BreadthSpotMode.RECORD
    assert spotting.breadth_spot_mode({SWITCH: "Quote"}) is spotting.BreadthSpotMode.QUOTE
    for wrong in ("ON", "OFF", "PARITY", "1"):
        with pytest.raises(ValueError, match=SWITCH):
            spotting.breadth_spot_mode({SWITCH: wrong})


def test_the_options_scheduler_refuses_to_start_on_an_invalid_value(monkeypatch):
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    scheduler = importlib.import_module("scripts.run_advanced_options_v3_scheduler")
    argv = ["--database", EXPECTED_V2_DATABASE_NAME, "--confirm-database-name", EXPECTED_V2_DATABASE_NAME,
            "--continuous", "--execute"]
    monkeypatch.setenv(SWITCH, "PROVIDER")
    with pytest.raises(ValueError, match=SWITCH):
        scheduler.parse_args(argv)
    for good in ("", "BAR", "RECORD", "QUOTE"):
        monkeypatch.setenv(SWITCH, good)
        assert scheduler.parse_args(argv).continuous


# ---- the quote spot -------------------------------------------------------------------------------------------------

def test_parity_recovers_the_quotes_spot_from_the_nearest_expiry():
    rows = _rows(_chain())
    found = spotting.parity_spot(rows, underlying="QQQ", valuation_inputs=_inputs(), observed_at=NOW)
    assert found.reason is None and found.expiration_date == date(2026, 11, 12) and found.pairs == 5
    assert abs(float(found.spot) - TRUE_SPOT) < 0.02 and found.dispersion_fraction < 0.0002
    assert set(found.strikes) <= {Decimal(item) for item in ("686", "688", "689", "690", "692")}


def test_parity_skips_an_expiry_whose_pairs_disagree_or_are_too_few():
    contracts = _chain(expirations=("2026-11-12",), strikes=(688, 689)) + _chain(expirations=("2026-11-13",))
    found = spotting.parity_spot(_rows(contracts), underlying="QQQ", valuation_inputs=_inputs(), observed_at=NOW)
    assert found.expiration_date == date(2026, 11, 13) and abs(float(found.spot) - TRUE_SPOT) < 0.03
    # One-sided quotes are no pair: nothing passes within 14 days, and the reason says why.
    one_sided = []
    for item in _chain():
        item["last_quote"]["bid"] = 0
        one_sided.append(item)
    missing = spotting.parity_spot(_rows(one_sided), underlying="QQQ", valuation_inputs=_inputs(), observed_at=NOW)
    assert missing.spot is None and missing.reason.startswith("NO_EXPIRY_PASSED") and "PAIRS_0" in missing.reason


def test_a_same_day_expiry_in_its_last_minutes_must_agree_within_one_basis_point():
    contracts = _chain()
    for item in contracts:  # same-day puts off by 10 cents per strike dollar from 689: forwards 1.5-4 bp apart
        details = item["details"]
        if details["expiration_date"] == "2026-11-12" and details["contract_type"] == "put":
            shift = (689 - details["strike_price"]) * 0.10
            item["last_quote"]["bid"] = round(item["last_quote"]["bid"] + shift, 2)
            item["last_quote"]["ask"] = round(item["last_quote"]["ask"] + shift, 2)
    rows = _rows(contracts)
    midday = spotting.parity_spot(rows, underlying="QQQ", valuation_inputs=_inputs(), observed_at=NOW)
    assert midday.expiration_date == date(2026, 11, 12) and 0.0001 < midday.dispersion_fraction < 0.001
    late = datetime(2026, 11, 12, 20, 45, tzinfo=UTC)  # 15:45 ET: 15 minutes left
    closing = spotting.parity_spot(rows, underlying="QQQ", valuation_inputs=_inputs(), observed_at=late)
    assert closing.expiration_date == date(2026, 11, 13) and abs(float(closing.spot) - TRUE_SPOT) < 0.05
    tight = spotting.parity_spot(_rows(_chain()), underlying="QQQ", valuation_inputs=_inputs(), observed_at=late)
    assert tight.expiration_date == date(2026, 11, 12)


def test_the_choice_falls_back_to_the_provider_and_then_to_the_bar_with_a_reason():
    def choose(contracts, bar=BAR_SPOT):
        return spotting.choose_breadth_spot(_rows(contracts), underlying="QQQ", bar_spot=bar,
                                            valuation_inputs=_inputs(), observed_at=NOW)

    chosen = choose(_chain())
    assert chosen.source == "PARITY" and chosen.reason is None and chosen.quote_spot == chosen.parity.spot
    no_pairs = _chain()
    for item in no_pairs:
        if item["details"]["contract_type"] == "put":
            item["last_quote"]["bid"] = 0
    provider = choose(no_pairs)
    assert provider.source == "PROVIDER" and provider.quote_spot == Decimal("689.12")
    delayed = choose([{**item, "underlying_asset": {**item["underlying_asset"], "timeframe": "DELAYED"}}
                      for item in no_pairs])
    assert delayed.source == "BAR" and delayed.quote_spot is None and delayed.spot == BAR_SPOT
    assert "NOT_REAL_TIME:DELAYED" in delayed.reason
    stale = choose(_chain(underlying_age_seconds=600, expirations=("2026-11-12",), strikes=(688,)))
    assert stale.source == "BAR" and "STALE_" in stale.reason
    disagree = choose(_chain(underlying_price=700.0))
    assert disagree.source == "BAR" and disagree.reason.startswith("PARITY_PROVIDER_DISAGREE")
    far = choose(_chain(), bar=Decimal("600"))
    assert far.source == "BAR" and far.reason.startswith("PARITY_FAR_FROM_BAR")
    assert choose(_chain(), bar=None).reason == "BAR_SPOT_UNAVAILABLE"


# ---- the capture ----------------------------------------------------------------------------------------------------

def test_bar_is_todays_capture_exactly_and_logs_nothing(monkeypatch, capsys):
    contracts = _chain()
    unset, unset_summary = _capture(monkeypatch, contracts)
    explicit, explicit_summary = _capture(monkeypatch, contracts, mode="BAR")
    assert unset == explicit and unset_summary == explicit_summary
    assert "spot_source" not in unset_summary["QQQ"] and not _records(capsys.readouterr().err)
    # Today's capture: every row solved at the bar spot by the unchanged `breadth_local_greeks`, 32 columns.
    rows = _rows(contracts)
    local = breadth.breadth_local_greeks(rows, spot=BAR_SPOT, valuation_inputs=_inputs(), observed_at=NOW)
    for row in rows:
        stored = unset[row.contract_ticker]
        assert len(stored) == len(_COLUMNS) == 32
        assert _column(stored, "spot") == BAR_SPOT
        assert stored[19:30] == tuple(local[row.contract_ticker][name] for name in breadth._LOCAL_FIELDS)
        assert _column(stored, "underlying_price") == row.underlying_price


def test_the_providers_underlying_clock_and_timeframe_are_read_but_never_stored():
    row = _rows([_contract("call", 689, "2026-11-12")])[0]
    assert row.underlying_timeframe == "REAL-TIME"
    assert row.underlying_price_at == datetime.fromtimestamp((NANOS - 5_000_000_000) / 1e9, tz=UTC)
    assert "underlying_price_at" not in breadth._COLUMNS and "timeframe" not in breadth._COLUMNS
    bare = _contract("call", 689, "2026-11-12")
    bare["underlying_asset"] = {"price": 689.12}
    old_shape = _rows([bare])[0]
    assert old_shape.underlying_price_at is None and old_shape.underlying_timeframe is None


def test_record_stores_the_bar_capture_and_logs_one_line_per_underlying(monkeypatch, capsys):
    contracts = _chain()
    bar, _ = _capture(monkeypatch, contracts)
    capsys.readouterr()
    recorded, summary = _capture(monkeypatch, contracts, mode="RECORD")
    assert recorded == bar and "spot_source" not in summary["QQQ"]
    (line,) = _records(capsys.readouterr().err)
    assert line["mode"] == "RECORD" and line["underlying"] == "QQQ" and line["used_source"] == "BAR"
    assert line["bar_spot"] == "688.00" and line["used_spot"] == "688.00" and line["quote_source"] == "PARITY"
    assert abs(float(line["quote_spot"]) - TRUE_SPOT) < 0.02 and line["gap_bp"] < -15
    assert line["provider"]["spot"] == "689.12" and abs(line["provider"]["parity_gap_bp"]) < 1
    effect = line["effect"]
    before, after = effect["atm_iv_agreement"]["before"], effect["atm_iv_agreement"]["after"]
    assert after["median_gap_vol_points"] < 0.5 < (before["median_gap_vol_points"] or 99)
    assert effect["gamma_0dte"]["after"]["contracts"] > 0
    assert set(effect["no_arbitrage_failures"]) == {"before", "after"}
    assert line["error"] is None


def test_quote_solves_at_the_parity_spot_and_says_so(monkeypatch, capsys):
    contracts = _chain()
    quoted, summary = _capture(monkeypatch, contracts, mode="QUOTE")
    assert summary["QQQ"]["state"] == "COMPLETE" and summary["QQQ"]["spot_source"] == "PARITY"
    (line,) = _records(capsys.readouterr().err)
    assert line["mode"] == "QUOTE" and line["used_source"] == "PARITY" and line["used_spot"] == line["quote_spot"]
    spot = Decimal(line["quote_spot"])
    rows = _rows(contracts)
    local = breadth.breadth_local_greeks(rows, spot=spot, valuation_inputs=_inputs(), observed_at=NOW)
    for row in rows:
        stored = quoted[row.contract_ticker]
        assert _column(stored, "spot") == spot and _column(stored, "equity_cutoff") == CUTOFF
        assert stored[19:30] == tuple(local[row.contract_ticker][name] for name in breadth._LOCAL_FIELDS)
    # At the quotes' spot the same-strike call and put IVs agree (the bar spot split them).
    ivs = {(_column(item, "strike_price"), _column(item, "contract_type")): _column(item, "local_iv")
           for item in quoted.values() if _column(item, "expiration_date") == SESSION}
    assert abs(ivs[(Decimal("689"), "call")] - ivs[(Decimal("689"), "put")]) < 0.01


def test_quote_falls_back_to_the_bar_when_no_quote_spot_exists(monkeypatch, capsys):
    contracts = [{**item, "underlying_asset": {**item["underlying_asset"], "timeframe": "DELAYED"}}
                 for item in _chain()]
    for item in contracts:
        item["last_quote"]["bid"] = 0 if item["details"]["contract_type"] == "put" else item["last_quote"]["bid"]
    bar, _ = _capture(monkeypatch, contracts)
    capsys.readouterr()
    quoted, summary = _capture(monkeypatch, contracts, mode="QUOTE")
    assert quoted == bar and summary["QQQ"]["spot_source"] == "BAR"
    (line,) = _records(capsys.readouterr().err)
    assert line["used_source"] == "BAR" and line["used_spot"] == "688.00" and line["effect"] is None
    assert line["fallback_reason"].startswith("NO_QUOTE_SPOT")


def test_a_failing_quote_path_never_fails_the_capture(monkeypatch, capsys):
    contracts = _chain()
    bar, _ = _capture(monkeypatch, contracts)
    capsys.readouterr()
    monkeypatch.setattr(breadth, "choose_breadth_spot", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("x")))
    chosen, summary = _capture(monkeypatch, contracts, mode="QUOTE")
    assert chosen == bar and summary["QQQ"]["state"] == "COMPLETE" and summary["QQQ"]["spot_source"] == "BAR"
    (line,) = _records(capsys.readouterr().err)
    assert line["fallback_reason"] == "CHOICE_FAILED:RuntimeError"
    monkeypatch.undo()

    # The solve at the quote spot raising: solved again at the bar spot, the line says why.
    original = breadth.breadth_local_greeks

    def flaky(rows, *, spot, **kwargs):
        if spot != BAR_SPOT:
            raise FloatingPointError("solver")
        return original(rows, spot=spot, **kwargs)

    monkeypatch.setattr(breadth, "breadth_local_greeks", flaky)
    solved, summary = _capture(monkeypatch, contracts, mode="QUOTE")
    assert solved == bar and summary["QQQ"]["spot_source"] == "BAR"
    (line,) = _records(capsys.readouterr().err)
    assert line["fallback_reason"] == "QUOTE_SOLVE_FAILED:FloatingPointError" and line["used_source"] == "BAR"


def test_a_failing_record_never_fails_the_capture(monkeypatch, capsys):
    contracts = _chain()
    bar, _ = _capture(monkeypatch, contracts)
    capsys.readouterr()
    monkeypatch.setattr(breadth, "spot_effect", lambda *_a, **_k: (_ for _ in ()).throw(KeyError("effect")))
    recorded, summary = _capture(monkeypatch, contracts, mode="RECORD")
    assert recorded == bar and summary["QQQ"]["state"] == "COMPLETE"
    (line,) = _records(capsys.readouterr().err)
    assert line["effect"] is None and line["error"].startswith("KeyError")
    monkeypatch.setattr(breadth, "log_record", lambda *_a, **_k: (_ for _ in ()).throw(OSError("closed")))
    again, summary = _capture(monkeypatch, contracts, mode="RECORD")
    assert again == bar and summary["QQQ"]["state"] == "COMPLETE"


def test_an_invalid_value_at_capture_time_reads_as_bar(monkeypatch, capsys):
    contracts = _chain()
    bar, _ = _capture(monkeypatch, contracts)
    invalid, summary = _capture(monkeypatch, contracts, mode="SOMETIMES")
    assert invalid == bar and "spot_source" not in summary["QQQ"] and not _records(capsys.readouterr().err)


def test_a_failed_or_empty_capture_records_nothing(monkeypatch, capsys):
    stored, summary = _capture(monkeypatch, [], mode="QUOTE")
    assert stored == {} and summary["QQQ"]["contracts"] == 0 and "spot_source" not in summary["QQQ"]
    assert not _records(capsys.readouterr().err)
