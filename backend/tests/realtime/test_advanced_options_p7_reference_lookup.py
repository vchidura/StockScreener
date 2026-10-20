from datetime import date
from decimal import Decimal

from scripts.run_advanced_options_p7_reference_lookup import _params, _select


def test_reference_lookup_parameters_are_exact_and_bounded():
    plan = {
        "request_scope": {
            "parameters": {
                "contract_type": "call",
                "expiration_date.gte": "2026-10-27",
                "expiration_date.lte": "2026-11-19",
                "expired": "false",
                "limit": "1000",
                "sort": "expiration_date",
                "order": "asc",
            }
        }
    }

    params = _params(plan, "SPY", Decimal("500"))

    assert params["underlying_ticker"] == "SPY"
    assert params["strike_price.gte"] == "475.00"
    assert params["strike_price.lte"] == "525.00"
    assert params["limit"] == "1000"


def test_reference_lookup_selects_earliest_nearest_standard_call():
    rows = [
        {
            "ticker": "O:SPY261120C00510000",
            "expiration_date": date(2026, 11, 20),
            "strike_price": 510,
            "contract_type": "call",
            "shares_per_contract": 100,
        },
        {
            "ticker": "O:SPY261030C00505000",
            "expiration_date": date(2026, 10, 30),
            "strike_price": 505,
            "contract_type": "call",
            "shares_per_contract": 100,
        },
        {
            "ticker": "O:SPY261030C00499000",
            "expiration_date": date(2026, 10, 30),
            "strike_price": 499,
            "contract_type": "call",
            "shares_per_contract": 100,
        },
    ]

    selected = _select("SPY", Decimal("500"), rows)

    assert selected["ticker"] == "O:SPY261030C00499000"
