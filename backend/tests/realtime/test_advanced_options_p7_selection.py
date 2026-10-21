import importlib
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from realtime.advanced_options_p7_selection import (
    P7TransportCandidate,
    P7TransportSelectionStatus,
    p7_transport_candidate_payload,
    select_p7_transport_contracts,
    validate_p7_transport_selection_payload,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 20, 15, 0, tzinfo=UTC)
script = importlib.import_module(
    "scripts.prepare_advanced_options_p7_selection"
)


def _candidate(
    underlyer,
    option_type,
    strike,
    identity,
    *,
    dte=0,
    volume=100,
    open_interest=100,
    bid="1.00",
    ask="1.10",
    age_seconds=0,
):
    expiration = date(2026, 10, 20) + timedelta(days=dte)
    return P7TransportCandidate(
        contract_ticker=(
            f"O:{underlyer}{expiration.strftime('%y%m%d')}"
            f"{option_type}{int(Decimal(strike) * 1000):08d}"
        ),
        underlyer=underlyer,
        expiration_date=expiration,
        option_type=option_type,
        strike=Decimal(strike),
        spot=Decimal("100"),
        bid=Decimal(bid),
        ask=Decimal(ask),
        day_volume=volume,
        open_interest=open_interest,
        calendar_dte=dte,
        market_data_time=NOW - timedelta(seconds=age_seconds),
        source_sha256=f"{identity:064x}",
    )


def test_p7_transport_selection_prefers_activity_across_call_and_put():
    candidates = (
        _candidate("SPY", "C", "100", 1, volume=500, open_interest=900),
        _candidate("SPY", "P", "99", 2, volume=800, open_interest=700),
        _candidate("QQQ", "C", "100", 3, volume=600, open_interest=800),
        _candidate("QQQ", "P", "100", 4, volume=550, open_interest=1_000),
    )

    selection = select_p7_transport_contracts(
        candidates,
        reviewed_at=NOW,
    )

    assert selection.status is P7TransportSelectionStatus.READY
    assert selection.contracts == (
        candidates[2].contract_ticker,
        candidates[1].contract_ticker,
    )
    assert selection.valid_until == NOW + timedelta(minutes=5)


def test_p7_transport_selection_uses_oi_atm_and_spread_tiebreakers():
    candidates = (
        _candidate("SPY", "C", "102", 1, volume=500, open_interest=500),
        _candidate("SPY", "P", "101", 2, volume=500, open_interest=700),
        _candidate(
            "SPY",
            "C",
            "100",
            3,
            volume=500,
            open_interest=700,
            ask="1.20",
        ),
        _candidate(
            "SPY",
            "P",
            "100",
            4,
            volume=500,
            open_interest=700,
            ask="1.05",
        ),
        _candidate("QQQ", "C", "100", 5, volume=500, open_interest=500),
    )

    selection = select_p7_transport_contracts(
        candidates,
        reviewed_at=NOW,
    )

    assert selection.status is P7TransportSelectionStatus.READY
    assert selection.contracts[-1] == candidates[3].contract_ticker


def test_p7_transport_selection_blocks_stale_or_inactive_scope():
    selection = select_p7_transport_contracts(
        (
            _candidate("SPY", "C", "100", 1, age_seconds=301),
            _candidate("QQQ", "C", "106", 2),
        ),
        reviewed_at=NOW,
    )

    assert selection.status is P7TransportSelectionStatus.BLOCKED
    assert selection.valid_until is None
    assert selection.reason_codes == (
        "QQQ_ACTIVE_CONTRACT_UNAVAILABLE",
        "SPY_ACTIVE_CONTRACT_UNAVAILABLE",
    )


def test_p7_transport_selection_rejects_duplicate_or_future_evidence():
    candidate = _candidate("SPY", "C", "100", 1)

    with pytest.raises(ValueError, match="duplicate contracts"):
        select_p7_transport_contracts(
            (candidate, candidate),
            reviewed_at=NOW,
        )

    with pytest.raises(ValueError, match="from the future"):
        select_p7_transport_contracts(
            (
                _candidate(
                    "SPY",
                    "C",
                    "100",
                    2,
                    age_seconds=-1,
                ),
            ),
            reviewed_at=NOW,
        )


def test_selection_cli_freezes_fresh_activity_evidence(tmp_path):
    candidates = (
        _candidate("SPY", "C", "100", 1, volume=500),
        _candidate("QQQ", "P", "100", 2, volume=600),
    )
    source = tmp_path / "candidates.json"
    output = tmp_path / "selection.json"
    source.write_text(
        json.dumps([
            p7_transport_candidate_payload(row)
            for row in candidates
        ]),
        encoding="utf-8",
    )

    assert script.main([
        "--candidates",
        str(source),
        "--reviewed-at",
        NOW.isoformat(),
        "--output",
        str(output),
    ]) == 0

    result = validate_p7_transport_selection_payload(
        json.loads(output.read_text(encoding="utf-8")),
        required_at=NOW,
    )
    assert [
        row["contract_ticker"] for row in result["selected"]
    ] == [
        candidates[1].contract_ticker,
        candidates[0].contract_ticker,
    ]
