from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime.advanced_options_rolling_repository import (
    decode_advanced_options_rolling_state,
)
from realtime.advanced_options_rolling_state import (
    build_advanced_options_rolling_state,
)
from realtime.advanced_options_mark_producer import (
    assemble_advanced_options_marks_cursor,
    build_advanced_options_mark_producer_plan,
)
from realtime.advanced_options_underlying_snapshot import (
    MAXIMUM_PRICE_AGE_SECONDS,
    UnderlyingSnapshotBinding,
    UnderlyingSnapshotPrice,
    UnderlyingSnapshotTick,
    binding_from_selection_capture,
    underlying_snapshot_binding_from_payload,
    underlying_snapshot_binding_payload,
)
from realtime.contracts import canonical_json, canonical_sha256
from test_advanced_options_p8_selection_source import (
    MARKET_NS,
    REVIEWED,
    SESSION,
)
from advanced_options_rolling_support import rolling_state
from test_advanced_options_mark_producer import _plan as mark_plan
from test_advanced_options_rolling_producer import (
    _head,
    _market_state,
    _plan as rolling_plan,
)


def _raw(underlyer, ticker, price, *, timeframe="REAL-TIME"):
    return {
        "details": {"ticker": ticker},
        "underlying_asset": {
            "ticker": underlyer,
            "price": price,
            "last_updated": MARKET_NS,
            "timeframe": timeframe,
        },
    }


def _capture(*, timeframe="REAL-TIME"):
    rows = (
        SimpleNamespace(
            underlyer="QQQ",
            contract_ticker="O:QQQ261021C00715000",
            source_sha256="8" * 64,
        ),
        SimpleNamespace(
            underlyer="SPY",
            contract_ticker="O:SPY261021P00770000",
            source_sha256="9" * 64,
        ),
    )
    evidence = {
        "selected_at_utc": REVIEWED.isoformat(),
        "valid_until_utc": (
            REVIEWED + timedelta(minutes=5)
        ).isoformat(),
        "selection_sha256": "a" * 64,
        "source_snapshot_sha256": "b" * 64,
    }
    return SimpleNamespace(
        selection=SimpleNamespace(
            selected=rows,
            policy=SimpleNamespace(
                maximum_snapshot_age_seconds=(
                    MAXIMUM_PRICE_AGE_SECONDS
                ),
            ),
        ),
        evidence=evidence,
        selected_snapshots={
            rows[0].contract_ticker: _raw(
                "QQQ",
                rows[0].contract_ticker,
                715,
                timeframe=timeframe,
            ),
            rows[1].contract_ticker: _raw(
                "SPY",
                rows[1].contract_ticker,
                770,
                timeframe=timeframe,
            ),
        },
    )


def test_snapshot_binding_round_trip_uses_existing_two_request_evidence():
    binding = binding_from_selection_capture(_capture())

    assert tuple(row.ticker for row in binding.prices) == ("QQQ", "SPY")
    assert binding.price_for("QQQ").price == Decimal("715")
    assert binding.valid_until <= REVIEWED + timedelta(minutes=5)
    assert underlying_snapshot_binding_from_payload(
        underlying_snapshot_binding_payload(binding)
    ) == binding


def test_snapshot_binding_rejects_delayed_or_stale_underlying_data():
    with pytest.raises(ValueError, match="identity changed"):
        binding_from_selection_capture(_capture(timeframe="DELAYED"))

    binding = binding_from_selection_capture(_capture())
    with pytest.raises(ValueError, match="stale or future"):
        binding.require_fresh(
            binding.valid_until + timedelta(microseconds=1)
        )


def test_snapshot_binding_accepts_bounded_provider_clock_skew():
    capture = _capture()
    future_ns = int(
        (
            REVIEWED + timedelta(seconds=4)
        ).timestamp()
        * 1_000_000_000
    )
    for row in capture.selected_snapshots.values():
        row["underlying_asset"]["last_updated"] = future_ns

    binding = binding_from_selection_capture(capture)

    assert binding.reviewed_at == REVIEWED + timedelta(seconds=4)


def test_snapshot_tick_survives_rolling_storage_without_equity_qt():
    old = rolling_state(
        cohort_id=UUID(int=50),
        journal_generation=UUID(int=51),
        session_date=SESSION,
        committed_at=REVIEWED,
    )
    evidence = UnderlyingSnapshotPrice(
        ticker="SPY",
        session_date=SESSION,
        price=Decimal("770"),
        market_time=REVIEWED - timedelta(seconds=1),
        timeframe="REAL-TIME",
        selected_contract_ticker="O:SPY261021P00770000",
        selected_contract_source_sha256="9" * 64,
        underlying_payload_sha256=canonical_sha256({"SPY": "snapshot"}),
    )
    tick = UnderlyingSnapshotTick(
        instrument_id=old.underlying_ticks[0].instrument_id,
        evidence=evidence,
        selected_at=REVIEWED,
        option_market_time=REVIEWED - timedelta(milliseconds=100),
        maximum_price_age_seconds=MAXIMUM_PRICE_AGE_SECONDS,
    )
    state = build_advanced_options_rolling_state(
        cohort_id=old.cohort_id,
        session_date=old.session_date,
        journal_generation=old.journal_generation,
        owner_epoch=old.owner_epoch,
        committed_offset=old.committed_offset,
        committed_at=old.committed_at,
        market_state_checkpoint_sha256=(
            old.market_state_checkpoint_sha256
        ),
        flow_checkpoint_sha256=old.flow_checkpoint_sha256,
        underlying_ticks=(tick,),
        contract_volumes=old.contract_volumes,
        order_windows=old.order_windows,
    )

    decoded = decode_advanced_options_rolling_state(
        state_id=state.state_id,
        state_sha256=state.state_sha256,
        document=json.loads(canonical_json(state.identity)),
    )
    assert decoded == state
    assert tick.quote is tick.trade is None


def test_snapshot_tick_supports_option_nbbo_marks_without_equity_heads():
    original = mark_plan()
    rolling = original.rolling_state
    old_tick = rolling.underlying_ticks[0]
    evidence = UnderlyingSnapshotPrice(
        ticker=old_tick.ticker,
        session_date=rolling.session_date,
        price=Decimal("500"),
        market_time=original.cutoff - timedelta(seconds=1),
        timeframe="REAL-TIME",
        selected_contract_ticker=(
            original.enrollment.activity_selection.contracts[
                0
            ].contract_ticker
        ),
        selected_contract_source_sha256="9" * 64,
        underlying_payload_sha256=canonical_sha256({
            "ticker": old_tick.ticker,
            "price": "500",
        }),
    )
    snapshot_tick = UnderlyingSnapshotTick(
        instrument_id=old_tick.instrument_id,
        evidence=evidence,
        selected_at=original.cutoff,
        option_market_time=original.cutoff - timedelta(milliseconds=100),
        maximum_price_age_seconds=MAXIMUM_PRICE_AGE_SECONDS,
    )
    snapshot_rolling = build_advanced_options_rolling_state(
        cohort_id=rolling.cohort_id,
        session_date=rolling.session_date,
        journal_generation=rolling.journal_generation,
        owner_epoch=rolling.owner_epoch,
        committed_offset=rolling.committed_offset,
        committed_at=rolling.committed_at,
        market_state_checkpoint_sha256=(
            rolling.market_state_checkpoint_sha256
        ),
        flow_checkpoint_sha256=rolling.flow_checkpoint_sha256,
        underlying_ticks=(snapshot_tick,),
        contract_volumes=rolling.contract_volumes,
        order_windows=rolling.order_windows,
        producer_binding=rolling.producer_binding,
    )
    plan = build_advanced_options_mark_producer_plan(
        enrollment=original.enrollment,
        reference_state=original.reference_state,
        rolling_state=snapshot_rolling,
        source_flow=original.source_flow,
        cutoff=original.cutoff,
        producer_build_id=original.producer_build_id,
        producer_build_sha256=original.producer_build_sha256,
        valuation_policy=original.valuation_policy,
    )
    rows = []
    for index, contract in enumerate(
        plan.enrollment.activity_selection.contracts,
        1,
    ):
        at = original.cutoff - timedelta(milliseconds=100)
        rows.append({
            "instrument_id": contract.instrument_id,
            "catalog_version_id": contract.catalog_version_id,
            "contract_id": contract.contract_id,
            "contract_ticker": contract.contract_ticker,
            "journal_generation": str(
                plan.source_flow.journal_generation
            ),
            "session_id": str(
                plan.source_flow.stream_session_ids[0]
            ),
            "source_session": original.cutoff.date(),
            "source_sequence": index,
            "source_offset": index,
            "event_time_raw": int(at.timestamp() * 1000),
            "event_time_unit": "ms",
            "event_at": at,
            "received_at": at,
            "quality": "GOOD",
            "reason_codes": [],
            "bid": Decimal("2"),
            "ask": Decimal("2.10"),
            "bid_size": 10,
            "ask_size": 11,
            "bid_exchange": 1,
            "ask_exchange": 2,
            "payload_sha256": "c" * 64,
        })

    result = assemble_advanced_options_marks_cursor(
        SimpleNamespace(
            execute=lambda *_args: None,
            fetchall=lambda: rows,
        ),
        plan,
    )

    assert result.status.value == "READY"
    assert all(mark.midpoint == Decimal("2.05") for mark in result.marks)


def test_market_state_queries_only_option_heads_with_snapshot_prices():
    original = rolling_plan()
    selected = original.enrollment.activity_selection.contracts[0]
    security_id = dict(
        original.enrollment.equity_plan.security_ids
    )[selected.underlyer]
    option_id = selected.instrument_id
    equity_id = option_id + 100_000
    binding = UnderlyingSnapshotBinding(
        selection_sha256="a" * 64,
        source_snapshot_sha256="b" * 64,
        reviewed_at=original.known_at - timedelta(seconds=1),
        valid_until=original.known_at + timedelta(minutes=4),
        prices=(
            UnderlyingSnapshotPrice(
                ticker="QQQ",
                session_date=original.control_plane.session_date,
                price=Decimal("715"),
                market_time=original.known_at - timedelta(seconds=1),
                timeframe="REAL-TIME",
                selected_contract_ticker="O:QQQ261021C00715000",
                selected_contract_source_sha256="8" * 64,
                underlying_payload_sha256="c" * 64,
            ),
            UnderlyingSnapshotPrice(
                ticker="SPY",
                session_date=original.control_plane.session_date,
                price=Decimal("770"),
                market_time=original.known_at - timedelta(seconds=1),
                timeframe="REAL-TIME",
                selected_contract_ticker="O:SPY261021P00770000",
                selected_contract_source_sha256="9" * 64,
                underlying_payload_sha256="d" * 64,
            ),
        ),
    )
    plan = replace(original, equity_live_head_binding=binding)
    at = original.known_at - timedelta(milliseconds=100)
    rows = (
        _head(
            "Q",
            option_id,
            plan.source_flow.journal_generation,
            plan.source_flow.stream_session_ids[0],
            at,
        ),
        _head(
            "T",
            option_id,
            plan.source_flow.journal_generation,
            plan.source_flow.stream_session_ids[0],
            at,
        ),
    )

    class Cursor:
        def __init__(self):
            self.parameters = None

        def execute(self, _statement, parameters):
            self.parameters = parameters

        def fetchall(self):
            return rows

    cursor = Cursor()
    evidence = []
    result = _market_state(
        cursor,
        plan,
        ({
            "instrument_id": option_id,
            "underlying_security_id": str(security_id),
        },),
        ({
            "instrument_id": equity_id,
            "security_id": str(security_id),
            "ticker": selected.underlyer,
        },),
        evidence,
    )

    assert result is not None
    assert isinstance(result[0][0], UnderlyingSnapshotTick)
    assert cursor.parameters == ([option_id], [option_id])
    assert evidence[-1].availability.value == "READY"
