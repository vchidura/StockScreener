import importlib
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from realtime.advanced_options_p7_selection import (
    P7TransportCandidate,
    p7_transport_candidate_payload,
)
from realtime.advanced_options_v3_broad_stream import (
    BroadOptionsStreamPolicy,
    broad_options_stream_plan_payload,
    build_broad_options_stream_plan,
)
from realtime.advanced_gateway import AdvancedGateway, GatewayConfiguration
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import MarketChannel


UTC = timezone.utc
SESSION_DATE = date(2026, 11, 2)
REVIEWED_AT = datetime(2026, 11, 2, 15, 0, tzinfo=UTC)
CAPABILITY_SHA256 = "a" * 64
host_segment = importlib.import_module(
    "scripts.run_advanced_realtime_v3_host_market_segment"
)
broad_stream_script = importlib.import_module(
    "scripts.prepare_advanced_options_v3_broad_stream"
)
capacity_script = importlib.import_module(
    "scripts.qualify_advanced_options_v3_broad_stream_capacity"
)


def _candidate(
    underlyer,
    option_type,
    *,
    dte,
    strike="100",
    identity=1,
):
    expiration = SESSION_DATE + timedelta(days=dte)
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
        bid=Decimal("1.00"),
        ask=Decimal("1.10"),
        day_volume=0,
        open_interest=0,
        calendar_dte=dte,
        market_data_time=REVIEWED_AT - timedelta(seconds=1),
        source_sha256=f"{identity:064x}",
    )


def _minimum_scope():
    return (
        _candidate("QQQ", "C", dte=0, identity=1),
        _candidate("QQQ", "P", dte=30, identity=2),
        _candidate("SPY", "C", dte=60, identity=3),
        _candidate("SPY", "P", dte=1, identity=4),
    )


def _build(candidates):
    return build_broad_options_stream_plan(
        tuple(candidates),
        session_date=SESSION_DATE,
        reviewed_at=REVIEWED_AT,
        capability_profile_sha256=CAPABILITY_SHA256,
        authorization_reference="future-broad-stream-test",
    )


def test_broad_stream_plan_freezes_matching_trade_and_quote_membership():
    plan = _build(_minimum_scope())

    assert plan.policy == BroadOptionsStreamPolicy()
    assert len(plan.contracts) == 4
    assert len(plan.manifest.members) == 8
    assert plan.manifest.maximum_option_quote_contracts == 4
    assert {
        row.channel for row in plan.manifest.members
    } == {MarketChannel.TRADE, MarketChannel.QUOTE}
    for ticker in {row.contract_ticker for row in plan.contracts}:
        assert sum(
            member.provider_symbol == ticker
            for member in plan.manifest.members
        ) == 2
    payload = broad_options_stream_plan_payload(plan)
    assert payload["contract_count"] == 4
    assert payload["subscription_count"] == 8

    gateway = AdvancedGateway(
        GatewayConfiguration.options(),
        plan.manifest,
        RawMessageBuffer(
            BufferLimits(100, 1_000_000, 1_000_000_000),
            monotonic_ns=lambda: 1,
        ),
        api_key="provider-free-test",
        monotonic_ns=lambda: 1,
    )
    tickers = tuple(sorted(
        row.contract_ticker for row in plan.contracts
    ))
    assert gateway.subscriptions == (
        tuple(f"T.{ticker}" for ticker in tickers)
        + tuple(f"Q.{ticker}" for ticker in tickers)
    )


@pytest.mark.parametrize(
    "candidates, match",
    [
        (
            (
                _candidate("QQQ", "C", dte=0, identity=1),
                _candidate("SPY", "C", dte=0, identity=2),
                _candidate("SPY", "P", dte=0, identity=3),
            ),
            "call/put coverage for QQQ",
        ),
        (
            _minimum_scope()
            + (_candidate(
                "QQQ",
                "C",
                dte=61,
                identity=5,
            ),),
            "outside policy",
        ),
    ],
)
def test_broad_stream_plan_fails_closed_on_membership_drift(
    candidates,
    match,
):
    with pytest.raises(ValueError, match=match):
        _build(candidates)


def test_broad_stream_plan_rejects_more_than_1000_contracts():
    candidates = []
    identity = 1
    for underlyer in ("QQQ", "SPY"):
        for dte in range(61):
            for option_type in ("C", "P"):
                for strike_index in range(5):
                    candidates.append(_candidate(
                        underlyer,
                        option_type,
                        dte=dte,
                        strike=str(
                            Decimal("98")
                            + Decimal(strike_index) / Decimal("10")
                        ),
                        identity=identity,
                    ))
                    identity += 1

    with pytest.raises(ValueError, match="1000-contract quote ceiling"):
        _build(candidates)


def test_host_segment_separates_buffer_and_session_byte_budgets():
    host_segment._validate_options_byte_budgets(
        host_segment.OPTIONS_BYTES,
        host_segment.OPTIONS_BYTES + 1,
    )

    with pytest.raises(ValueError, match="larger explicit session ceiling"):
        host_segment._validate_options_byte_budgets(
            host_segment.OPTIONS_BYTES,
            host_segment.OPTIONS_BYTES,
        )


def test_prepare_broad_stream_cli_writes_frozen_plan(tmp_path):
    candidates_path = tmp_path / "candidates.json"
    output_path = tmp_path / "plan.json"
    candidates_path.write_text(
        json.dumps([
            p7_transport_candidate_payload(row)
            for row in _minimum_scope()
        ]),
        encoding="utf-8",
    )

    assert broad_stream_script.main([
        "--candidates",
        str(candidates_path),
        "--session-date",
        SESSION_DATE.isoformat(),
        "--reviewed-at",
        REVIEWED_AT.isoformat(),
        "--capability-profile-sha256",
        CAPABILITY_SHA256,
        "--authorization-reference",
        "future-broad-stream-test",
        "--output",
        str(output_path),
    ]) == 0

    result = json.loads(output_path.read_text(encoding="utf-8"))
    assert result["contract_count"] == 4
    assert result["subscription_count"] == 8
    assert len(result["manifest_sha256"]) == 64
    assert len(result["plan_sha256"]) == 64


def test_broad_stream_capacity_qualification_is_provider_free():
    report = capacity_script.run_qualification(
        message_count=200,
        batch_size=50,
    )

    assert report["status"] in {"PASS", "FAIL"}
    assert report["scope"]["contract_count"] == 1_000
    assert report["scope"]["subscription_count"] == 2_000
    assert report["load"]["raw_message_count"] == 200
    assert report["result"]["last_raw_offset"] == 200
    assert report["result"]["last_event_offset"] == 400
    assert report["result"]["cleanup_empty"] is True
    assert set(report["effects"].values()) == {0}
