"""R16 (Nov 10, live): provider Greeks with float noise must not invalidate an activity candidate."""
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from realtime.advanced_options_flow_policy import load_advanced_options_flow_policy
from realtime.advanced_options_p7_selection import P7TransportCandidate
from scripts.prepare_advanced_options_v3_pre_session import _activity_candidate

SESSION = date(2026, 11, 10)
OBSERVED = datetime(2026, 11, 10, 15, 14, 34, tzinfo=timezone.utc)
QUOTED = datetime(2026, 11, 10, 14, 31, 47, tzinfo=timezone.utc)


def _row():
    return P7TransportCandidate(
        contract_ticker="O:NFLX261120C00058000", underlyer="NFLX", expiration_date=date(2026, 11, 20),
        option_type="C", strike=Decimal("58"), spot=Decimal("63.46"), bid=Decimal("5.35"), ask=Decimal("5.55"),
        day_volume=0, open_interest=4, calendar_dte=10, market_data_time=QUOTED, source_sha256="a" * 64,
    )


def _raw(delta, implied_volatility=0.0012098101691503437, greeks=True):
    return {
        "details": {"ticker": "O:NFLX261120C00058000", "contract_type": "call", "expiration_date": "2026-11-20",
                    "strike_price": 58, "shares_per_contract": 100},
        "last_quote": {"bid": 5.35, "ask": 5.55, "last_updated": int(QUOTED.timestamp() * 1e9)},
        "day": {}, "open_interest": 4, "underlying_asset": {"ticker": "NFLX", "price": 63.46},
        "implied_volatility": implied_volatility,
        **({"greeks": {"delta": delta, "gamma": 2.4e-10, "theta": -0.006, "vega": -1.1e-05}} if greeks else {}),
    }


@pytest.mark.parametrize("delta,expected", [
    (1.000000000002623, Decimal("1")),      # NFLX Nov 10, float noise above the bound
    (-1.0000000000047444, Decimal("-1")),
    (0.42, Decimal("0.42")),
    (1.5, None),                             # not a delta: dropped, not a failed allocation
])
def test_provider_delta_noise_is_the_bound_and_bad_values_are_dropped(delta, expected):
    candidate = _activity_candidate(_row(), _raw(delta), session_date=SESSION, observed_at=OBSERVED,
                                    flow_policy=load_advanced_options_flow_policy())
    assert candidate.delta == expected
    assert candidate.greeks_as_of == QUOTED


def test_no_usable_greek_leaves_no_greeks_clock():
    candidate = _activity_candidate(_row(), _raw(7.0, implied_volatility=-0.2), session_date=SESSION,
                                    observed_at=OBSERVED, flow_policy=load_advanced_options_flow_policy())
    assert candidate.delta is None and candidate.implied_volatility is None and candidate.greeks_as_of is None
