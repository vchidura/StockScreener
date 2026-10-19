from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from uuid import UUID

import pytest

from option_flow_support import config, policy
from realtime.advanced_options_rolling_state import (
    CANONICAL_ORDER_WINDOWS_SECONDS,
    DARK_POOL_OWNER_NOT_ACCEPTED,
    O2_MODEL_NOT_ACCEPTED,
    AdvancedOptionsOrderWindowMember,
    AdvancedOptionsOrderWindowState,
    AdvancedOptionsOptionalRollingLane,
    AdvancedOptionsRollingStateOwner,
    RollingAvailability,
    build_advanced_options_rolling_state,
    build_underlying_tick,
    contract_volume_state,
    underlying_tick_from_materializer_states,
)
from realtime.advanced_options_rolling_repository import (
    decode_advanced_options_rolling_state,
)
from realtime.contracts import AssetClass, canonical_json
from realtime.materialization import (
    MaterializedQuote,
    MaterializedTrade,
    MaterializerShardState,
)
from realtime.option_flow.records import VolumeCheckpoint
from realtime.option_flow.volume import BaselineKind


UTC = timezone.utc
SESSION = date(2026, 10, 16)
NOW = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
GENERATION = UUID("11111111-1111-1111-1111-111111111111")
STREAM = UUID("22222222-2222-2222-2222-222222222222")
COHORT = UUID("33333333-3333-3333-3333-333333333333")
EVENT = UUID("44444444-4444-4444-4444-444444444444")


def _quote(*, event_at=NOW - timedelta(milliseconds=100), offset=10):
    return MaterializedQuote(
        journal_generation=GENERATION,
        stream_session_id=STREAM,
        instrument_id=101,
        provider_symbol="SPY",
        source_session=SESSION,
        source_sequence=7,
        source_timestamp=1,
        source_timestamp_unit="ms",
        event_at=event_at,
        received_at=event_at + timedelta(milliseconds=5),
        bid=Decimal("499.90"),
        ask=Decimal("500.10"),
        bid_size=100,
        ask_size=120,
        bid_exchange=1,
        ask_exchange=2,
        quality="VALID",
        reason_codes=(),
        source_offset=offset,
        payload_sha256="1" * 64,
    )


def _trade(*, event_at=NOW - timedelta(milliseconds=80), offset=11):
    return MaterializedTrade(
        journal_generation=GENERATION,
        stream_session_id=STREAM,
        instrument_id=101,
        provider_symbol="SPY",
        asset_class=AssetClass.EQUITY,
        source_session=SESSION,
        source_sequence=8,
        source_timestamp=2,
        source_timestamp_unit="ms",
        event_at=event_at,
        received_at=event_at + timedelta(milliseconds=5),
        price=Decimal("500"),
        size=Decimal("10"),
        exchange=1,
        conditions=(),
        quality="VALID",
        reason_codes=(),
        eligibility="ELIGIBLE",
        eligibility_reason_codes=(),
        semantics_version="equity-trade-v1",
        source_offset=offset,
        payload_sha256="2" * 64,
    )


def _tick(**changes):
    values = dict(
        instrument_id=101,
        ticker="SPY",
        session_date=SESSION,
        quote=_quote(),
        trade=_trade(),
        option_market_time=NOW - timedelta(milliseconds=90),
        selected_at=NOW,
        maximum_age_ms=1_000,
        maximum_option_spot_skew_ms=500,
    )
    values.update(changes)
    return build_underlying_tick(**values)


def _volume(*, baseline=BaselineKind.SESSION_COMPLETE):
    checkpoint = VolumeCheckpoint(
        cohort_id=COHORT,
        session_date=SESSION,
        contract_ticker="O:SPY261120C00500000",
        cumulative_volume=1_000,
        last_source_seq=10,
        last_event_time=NOW - timedelta(seconds=1),
        baseline_kind=baseline.value,
        snapshot_volume=(
            900 if baseline is BaselineKind.SNAPSHOT_RECONCILED else None
        ),
        snapshot_watermark_seq=(
            8 if baseline is BaselineKind.SNAPSHOT_RECONCILED else None
        ),
    )
    return contract_volume_state(
        checkpoint,
        reconcile_evidence_sha256=(
            "3" * 64
            if baseline is BaselineKind.SNAPSHOT_RECONCILED
            else None
        ),
    )


def _order_windows(*, offset=11):
    return AdvancedOptionsOrderWindowState(
        owner_epoch=3,
        committed_offset=offset,
        checkpoint_sha256="4" * 64,
        windows_seconds=CANONICAL_ORDER_WINDOWS_SECONDS,
        members=(
            AdvancedOptionsOrderWindowMember(
                EVENT,
                offset,
                NOW - timedelta(milliseconds=80),
                5,
            ),
        ),
    )


def _rolling_state(*, offset=11, committed_at=NOW, tick=None, volume=None):
    return build_advanced_options_rolling_state(
        cohort_id=COHORT,
        session_date=SESSION,
        journal_generation=GENERATION,
        owner_epoch=3,
        committed_offset=offset,
        committed_at=committed_at,
        market_state_checkpoint_sha256="5" * 64,
        flow_checkpoint_sha256="6" * 64,
        underlying_ticks=(tick or _tick(),),
        contract_volumes=(volume or _volume(),),
        order_windows=_order_windows(offset=offset),
    )


def test_underlying_tick_binds_canonical_current_evidence_and_clocks():
    tick = _tick()

    assert tick.availability is RollingAvailability.READY
    assert tick.trade.price == Decimal("500")
    assert tick.quote.bid == Decimal("499.90")
    assert tick.quote.ask == Decimal("500.10")
    assert tick.freshness_ms == 80
    assert tick.option_spot_skew_ms == 10
    assert tick.source_identity_sha256


def test_underlying_tick_becomes_unavailable_for_missing_stale_or_skewed_evidence():
    missing = _tick(trade=None)
    stale = _tick(
        quote=_quote(event_at=NOW - timedelta(seconds=10)),
        trade=_trade(event_at=NOW - timedelta(seconds=10)),
        option_market_time=NOW - timedelta(seconds=10),
    )
    skewed = _tick(
        option_market_time=NOW - timedelta(seconds=5),
    )

    assert missing.availability is RollingAvailability.UNAVAILABLE
    assert "UNDERLYING_TRADE_UNAVAILABLE" in missing.reason_codes
    assert stale.reason_codes == ("UNDERLYING_TICK_STALE_OR_FUTURE",)
    assert skewed.reason_codes == ("OPTION_SPOT_SKEW_EXCEEDED",)


def test_underlying_tick_reuses_materializer_state_without_a_parallel_current_owner():
    state = MaterializerShardState(
        shard_id=0,
        shard_map_sha256="7" * 64,
        scanned_offset=11,
        quotes=(_quote(),),
        trades=(_trade(),),
    )

    assert underlying_tick_from_materializer_states(
        (state,),
        instrument_id=101,
        ticker="SPY",
        session_date=SESSION,
        option_market_time=NOW - timedelta(milliseconds=90),
        selected_at=NOW,
        maximum_age_ms=1_000,
        maximum_option_spot_skew_ms=500,
    ) == _tick()


def test_volume_remains_incomplete_until_causal_reconcile_evidence():
    incomplete_checkpoint = replace(
        _volume().checkpoint,
        baseline_kind=BaselineKind.INCOMPLETE.value,
    )
    incomplete = contract_volume_state(
        incomplete_checkpoint,
        reason_codes=("UNKNOWN_CONDITION_VOLUME",),
    )
    reconciled = _volume(baseline=BaselineKind.SNAPSHOT_RECONCILED)

    assert incomplete.completeness is BaselineKind.INCOMPLETE
    assert incomplete.reconcile_evidence_sha256 is None
    assert reconciled.completeness is BaselineKind.SNAPSHOT_RECONCILED
    with pytest.raises(ValueError, match="causal evidence"):
        contract_volume_state(
            replace(
                reconciled.checkpoint,
                baseline_kind=BaselineKind.SNAPSHOT_RECONCILED.value,
            )
        )


def test_cluster_engine_owns_every_canonical_order_window():
    assert config(
        policy=policy(sweep_window_seconds=5)
    ).cluster_config().windows_seconds == (
        CANONICAL_ORDER_WINDOWS_SECONDS
    )


def test_rolling_state_is_canonical_and_keeps_optional_lanes_unavailable():
    first = _rolling_state()
    repeated = _rolling_state()

    assert first == repeated
    assert first.iv_surface.availability is RollingAvailability.UNAVAILABLE
    assert first.iv_surface.reason_codes == (O2_MODEL_NOT_ACCEPTED,)
    assert first.dark_pool.availability is RollingAvailability.UNAVAILABLE
    assert first.dark_pool.reason_codes == (DARK_POOL_OWNER_NOT_ACCEPTED,)
    with pytest.raises(ValueError, match="IV surface model/policy"):
        build_advanced_options_rolling_state(
            cohort_id=COHORT,
            session_date=SESSION,
            journal_generation=GENERATION,
            owner_epoch=3,
            committed_offset=11,
            committed_at=NOW,
            market_state_checkpoint_sha256="5" * 64,
            flow_checkpoint_sha256="6" * 64,
            underlying_ticks=(_tick(),),
            contract_volumes=(_volume(),),
            order_windows=_order_windows(),
            iv_surface=AdvancedOptionsOptionalRollingLane(
                "IV_SURFACE",
                RollingAvailability.READY,
                (),
                "8" * 64,
            ),
        )


def test_rolling_checkpoint_cannot_lag_current_evidence():
    with pytest.raises(ValueError, match="rolling state is incomplete"):
        build_advanced_options_rolling_state(
            cohort_id=COHORT,
            session_date=SESSION,
            journal_generation=GENERATION,
            owner_epoch=3,
            committed_offset=10,
            committed_at=NOW,
            market_state_checkpoint_sha256="5" * 64,
            flow_checkpoint_sha256="6" * 64,
            underlying_ticks=(_tick(),),
            contract_volumes=(_volume(),),
            order_windows=replace(
                _order_windows(),
                committed_offset=10,
                members=(),
            ),
        )


def test_persistence_document_round_trip_preserves_exact_rolling_state():
    state = _rolling_state()

    assert decode_advanced_options_rolling_state(
        state_id=state.state_id,
        state_sha256=state.state_sha256,
        document=json.loads(canonical_json(state.identity)),
    ) == state


def test_owner_advances_memory_only_after_atomic_persistence():
    first = _rolling_state()
    second = _rolling_state(
        offset=12,
        committed_at=NOW + timedelta(seconds=1),
    )
    owner = AdvancedOptionsRollingStateOwner(first)

    def fail(_candidate):
        raise RuntimeError("write failed")

    with pytest.raises(RuntimeError, match="write failed"):
        owner.commit(second, persist=fail)
    assert owner.state == first
    persisted = []
    owner.commit(second, persist=persisted.append)
    assert persisted == [second]
    assert owner.state == second
    restarted = AdvancedOptionsRollingStateOwner(second)
    assert restarted.state == owner.state


def test_owner_rejects_same_offset_divergence_and_offset_regression():
    current = _rolling_state(offset=12)
    owner = AdvancedOptionsRollingStateOwner(current)

    with pytest.raises(ValueError, match="regressed or diverged"):
        owner.commit(
            _rolling_state(offset=11),
            persist=lambda _state: None,
        )
    with pytest.raises(ValueError, match="regressed or diverged"):
        owner.commit(
            _rolling_state(
                offset=12,
                committed_at=NOW + timedelta(seconds=1),
            ),
            persist=lambda _state: None,
        )
