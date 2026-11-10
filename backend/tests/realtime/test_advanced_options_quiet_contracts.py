"""Nov 9: one quiet contract no longer makes the whole per-window Options snapshot stale."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options.data.polygon_http import PolygonHttpResponse
from realtime.advanced_options_p8_selection_source import MassiveAdvancedOptionsExactContractSource
from realtime.advanced_options_quiet_contracts import (
    quiet_minority_is_valid, quiet_tolerant_valid_until,
)
from realtime.option_session_manifest import OptionActivityContract, OptionActivitySelectionRecord

UTC = timezone.utc
SESSION = date(2026, 10, 21)
REVIEWED = datetime(2026, 10, 21, 13, 29, tzinfo=UTC)
FRESH = datetime(2026, 10, 21, 13, 28, 30, tzinfo=UTC)
QUIET = datetime(2026, 10, 21, 13, 22, tzinfo=UTC)
AGE = timedelta(seconds=300)


def _ns(value):
    return int(value.timestamp() * 1_000_000_000)


def _tickers():
    return tuple(
        f"O:{underlyer}261021{side}{strike * 1000:08d}"
        for underlyer, base in (("QQQ", 600), ("SPY", 670))
        for side in ("C", "P")
        for strike in (base, base + 5)
    )


def _row(ticker, quoted_at):
    underlyer = ticker[2:5]
    return {
        "details": {
            "ticker": ticker, "contract_type": "call" if ticker[11] == "C" else "put",
            "expiration_date": SESSION.isoformat(), "strike_price": int(ticker[12:]) / 1000,
        },
        "last_quote": {"bid": 1, "ask": 1.1, "last_updated": _ns(quoted_at)},
        "day": {"volume": 500},
        "open_interest": 1_000,
        "underlying_asset": {
            "ticker": underlyer, "price": 600 if underlyer == "QQQ" else 670,
            "last_updated": _ns(FRESH), "timeframe": "REAL-TIME",
        },
    }


class _Transport:
    def __init__(self, rows):
        self.rows = rows

    def get(self, url, params):
        return PolygonHttpResponse(200, json.dumps({"status": "OK", "results": self.rows}).encode())


def _capture(quiet):
    rows = [_row(ticker, QUIET if ticker in quiet else FRESH) for ticker in _tickers()]
    return MassiveAdvancedOptionsExactContractSource(_Transport(rows)).capture(
        contract_tickers=_tickers(), reviewed_at=REVIEWED, session_date=SESSION,
    )


def test_all_fresh_contracts_keep_the_old_expiry():
    assert _capture(()).selection.valid_until == FRESH + AGE


def test_one_quiet_contract_no_longer_expires_the_snapshot():
    capture = _capture({"O:QQQ261021P00600000"})
    assert capture.selection.valid_until == FRESH + AGE
    assert capture.evidence["valid_until_utc"] == (FRESH + AGE).isoformat()


def test_a_side_with_no_fresh_contract_stays_stale():
    capture = _capture({"O:QQQ261021P00600000", "O:QQQ261021P00605000"})
    assert capture.selection.valid_until == QUIET + AGE
    assert capture.selection.valid_until <= REVIEWED


def test_more_than_a_quarter_quiet_stays_stale():
    rows = [("QQQ", "C", QUIET)] * 3 + [("QQQ", "C", FRESH)] * 5 + [("QQQ", "P", FRESH)] * 2
    assert quiet_tolerant_valid_until(rows, reviewed_at=REVIEWED, maximum_age=AGE) == QUIET + AGE
    rows = [("QQQ", "C", QUIET)] * 2 + [("QQQ", "C", FRESH)] * 6 + [("QQQ", "P", FRESH)] * 2
    assert quiet_tolerant_valid_until(rows, reviewed_at=REVIEWED, maximum_age=AGE) == FRESH + AGE


def test_a_quote_inside_the_margin_counts_as_quiet():
    nearly = REVIEWED + timedelta(seconds=30) - AGE
    rows = [("QQQ", "C", nearly), ("QQQ", "C", FRESH), ("QQQ", "P", FRESH), ("QQQ", "P", FRESH)]
    assert quiet_tolerant_valid_until(rows, reviewed_at=REVIEWED, maximum_age=AGE) == FRESH + AGE


def test_reload_check_matches_the_capture_rule():
    rows = [("QQQ", "C", QUIET), ("QQQ", "C", FRESH), ("QQQ", "P", FRESH), ("QQQ", "P", FRESH)]
    assert quiet_minority_is_valid(rows, valid_until=FRESH + AGE, maximum_age=AGE)
    assert quiet_minority_is_valid(rows, valid_until=QUIET + AGE, maximum_age=AGE)
    assert not quiet_minority_is_valid(rows, valid_until=FRESH + AGE + timedelta(seconds=1), maximum_age=AGE)
    lone = [("QQQ", "C", QUIET), ("QQQ", "P", FRESH), ("QQQ", "P", FRESH), ("QQQ", "P", FRESH)]
    assert not quiet_minority_is_valid(lone, valid_until=FRESH + AGE, maximum_age=AGE)


def _contract(index, ticker, quoted_at):
    return OptionActivityContract(
        instrument_id=index, contract_id=index, catalog_version_id=index, contract_ticker=ticker,
        underlyer=ticker[2:5], call_put=ticker[11], expiration_date=SESSION, strike=Decimal(ticker[12:]) / 1000,
        exercise_style="AMERICAN", multiplier=Decimal("100"), deliverables=(), eligibility="VALIDATED_ACTIVE",
        catalog_effective_from=QUIET - timedelta(days=1), catalog_effective_to=None,
        catalog_observed_at=QUIET - timedelta(days=1), catalog_payload_sha256="a" * 64,
        market_data_time=quoted_at, source_sha256="b" * 64,
    )


def _record(quiet, valid_until):
    return OptionActivitySelectionRecord(
        authorization_reference="nov9-quiet", reviewed_at=REVIEWED, valid_until=valid_until,
        policy_sha256="c" * 64, source_sha256="d" * 64, quote_ceiling=8,
        contracts=tuple(
            _contract(index, ticker, QUIET if ticker in quiet else FRESH)
            for index, ticker in enumerate(_tickers(), start=1)
        ),
    )


def test_every_service_accepts_a_selection_with_a_quiet_minority():
    assert _record({"O:QQQ261021P00600000"}, FRESH + AGE).valid_until == FRESH + AGE


def test_every_service_still_rejects_an_uncovered_side():
    with pytest.raises(ValueError, match="source clocks"):
        _record({"O:QQQ261021P00600000", "O:QQQ261021P00605000"}, FRESH + AGE)
