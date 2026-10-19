from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from realtime.advanced_options_rolling_state import (
    CANONICAL_ORDER_WINDOWS_SECONDS,
    AdvancedOptionsOrderWindowMember,
    AdvancedOptionsOrderWindowState,
    build_advanced_options_rolling_state,
    build_underlying_tick,
    contract_volume_state,
)
from realtime.contracts import AssetClass
from realtime.materialization import MaterializedQuote, MaterializedTrade
from realtime.option_flow.records import VolumeCheckpoint
from realtime.option_flow.volume import BaselineKind


UTC = timezone.utc
NOW = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
STREAM = UUID("12121212-1212-1212-1212-121212121212")
EVENT = UUID("23232323-2323-2323-2323-232323232323")


def rolling_state(
    *,
    cohort_id,
    journal_generation,
    session_date=date(2026, 10, 16),
    committed_offset=11,
    committed_at=NOW,
    owner_epoch=3,
    market_state_checkpoint_sha256="4" * 64,
    flow_checkpoint_sha256="5" * 64,
):
    quote = MaterializedQuote(
        journal_generation=journal_generation,
        stream_session_id=STREAM,
        instrument_id=101,
        provider_symbol="SPY",
        source_session=session_date,
        source_sequence=7,
        source_timestamp=1,
        source_timestamp_unit="ms",
        event_at=committed_at - timedelta(milliseconds=100),
        received_at=committed_at - timedelta(milliseconds=95),
        bid=Decimal("499.90"),
        ask=Decimal("500.10"),
        bid_size=100,
        ask_size=120,
        bid_exchange=1,
        ask_exchange=2,
        quality="VALID",
        reason_codes=(),
        source_offset=10,
        payload_sha256="1" * 64,
    )
    trade = MaterializedTrade(
        journal_generation=journal_generation,
        stream_session_id=STREAM,
        instrument_id=101,
        provider_symbol="SPY",
        asset_class=AssetClass.EQUITY,
        source_session=session_date,
        source_sequence=8,
        source_timestamp=2,
        source_timestamp_unit="ms",
        event_at=committed_at - timedelta(milliseconds=80),
        received_at=committed_at - timedelta(milliseconds=75),
        price=Decimal("500"),
        size=Decimal("10"),
        exchange=1,
        conditions=(),
        quality="VALID",
        reason_codes=(),
        eligibility="ELIGIBLE",
        eligibility_reason_codes=(),
        semantics_version="equity-trade-v1",
        source_offset=11,
        payload_sha256="2" * 64,
    )
    tick = build_underlying_tick(
        instrument_id=101,
        ticker="SPY",
        session_date=session_date,
        quote=quote,
        trade=trade,
        option_market_time=committed_at - timedelta(milliseconds=90),
        selected_at=committed_at,
        maximum_age_ms=1_000,
        maximum_option_spot_skew_ms=500,
    )
    volumes = tuple(
        contract_volume_state(VolumeCheckpoint(
            cohort_id=cohort_id,
            session_date=session_date,
            contract_ticker=contract_ticker,
            cumulative_volume=1_000,
            last_source_seq=10,
            last_event_time=committed_at - timedelta(seconds=1),
            baseline_kind=BaselineKind.SESSION_COMPLETE.value,
            snapshot_volume=None,
            snapshot_watermark_seq=None,
        ))
        for contract_ticker in (
            "O:SPY261120C00500000",
            "O:SPY261120P00450000",
        )
    )
    order_windows = AdvancedOptionsOrderWindowState(
        owner_epoch=owner_epoch,
        committed_offset=committed_offset,
        checkpoint_sha256="3" * 64,
        windows_seconds=CANONICAL_ORDER_WINDOWS_SECONDS,
        members=(
            AdvancedOptionsOrderWindowMember(
                EVENT,
                committed_offset,
                committed_at - timedelta(milliseconds=80),
                5,
            ),
        ),
    )
    return build_advanced_options_rolling_state(
        cohort_id=cohort_id,
        session_date=session_date,
        journal_generation=journal_generation,
        owner_epoch=owner_epoch,
        committed_offset=committed_offset,
        committed_at=committed_at,
        market_state_checkpoint_sha256=market_state_checkpoint_sha256,
        flow_checkpoint_sha256=flow_checkpoint_sha256,
        underlying_ticks=(tick,),
        contract_volumes=volumes,
        order_windows=order_windows,
    )
