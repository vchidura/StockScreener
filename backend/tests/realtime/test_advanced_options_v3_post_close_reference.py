from copy import deepcopy
from datetime import datetime, timezone

import pytest

from realtime.contracts import canonical_sha256
from scripts.prepare_advanced_options_v3_post_close_reference import (
    build_supplement,
)


UTC = timezone.utc
CONTRACTS = (
    "O:QQQ261030C00696000",
    "O:QQQ261030P00696000",
    "O:SPY261030C00761000",
    "O:SPY261030P00761000",
)


def _snapshot(ticker, contract_type, strike, open_interest):
    return {
        "ticker": ticker,
        "details": {
            "contract_type": contract_type,
            "exercise_style": "american",
            "expiration_date": "2026-10-30",
            "shares_per_contract": 100,
            "strike_price": strike,
        },
        "last_quote": {
            "last_updated": int(
                datetime(2026, 10, 30, 15, 19, tzinfo=UTC).timestamp()
                * 1_000_000_000
            ),
        },
        "open_interest": open_interest,
        "underlying_asset": {
            "ticker": ticker[2:5],
        },
    }


def _inputs():
    cycle = {
        "schema": "advanced-realtime-v3-host-segment-cycle-v1",
        "index": 1,
        "contracts": list(CONTRACTS),
        "completed_at_utc": "2026-10-30T15:20:00+00:00",
        "selected_snapshots": {
            CONTRACTS[0]: _snapshot(CONTRACTS[0], "call", 696, 10),
            CONTRACTS[1]: _snapshot(CONTRACTS[1], "put", 696, 20),
            CONTRACTS[2]: _snapshot(CONTRACTS[2], "call", 761, 30),
            CONTRACTS[3]: _snapshot(CONTRACTS[3], "put", 761, 40),
        },
    }
    cycle["semantic_sha256"] = canonical_sha256(cycle)
    history = {
        "semantic_sha256": "a" * 64,
        "warmup": {"verdict": "PASS"},
        "members": [
            {"ticker": "QQQ", "security_id": "1" * 32},
            {"ticker": "SPY", "security_id": "2" * 32},
        ],
    }
    manifest = {
        "status": "READY_FOR_READ_ONLY_HISTORY_EXPORT",
        "session_date": "2026-10-30",
        "history_valid_through": "2026-10-29",
        "semantic_sha256": "b" * 64,
    }
    return cycle, history, manifest


def test_sealed_options_reference_preserves_exact_four_and_prior_oi():
    result = build_supplement(*_inputs())

    assert result["status"] == "PASS"
    assert result["contract_count"] == 4
    assert result["oi_baseline_count"] == 4
    assert {
        row["contract"]["contract_ticker"]
        for row in result["rows"]
    } == set(CONTRACTS)
    assert {
        row["oi_baseline"]["open_interest"]
        for row in result["rows"]
    } == {10, 20, 30, 40}


def test_sealed_options_reference_rejects_tampered_cycle():
    cycle, history, manifest = _inputs()
    invalid = deepcopy(cycle)
    invalid["selected_snapshots"][CONTRACTS[0]]["open_interest"] = 999

    with pytest.raises(ValueError, match="inputs are invalid"):
        build_supplement(invalid, history, manifest)
