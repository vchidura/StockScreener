"""PostgreSQL adapter for immutable Advanced Options rolling checkpoints."""
from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping
from uuid import UUID

from psycopg2.extras import Json

from .advanced_options_rolling_state import (
    AdvancedOptionsContractVolumeState,
    AdvancedOptionsOptionalRollingLane,
    AdvancedOptionsOrderWindowMember,
    AdvancedOptionsOrderWindowState,
    AdvancedOptionsRollingState,
    AdvancedOptionsUnderlyingTick,
    RollingAvailability,
)
from .contracts import AssetClass, canonical_json
from .database_access_v2 import V2ConnectionProvider
from .materialization import MaterializedQuote, MaterializedTrade
from .option_flow.records import VolumeCheckpoint
from .option_flow.volume import BaselineKind


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _date(value: str) -> date:
    return date.fromisoformat(value)


def _quote(value: Mapping[str, object] | None) -> MaterializedQuote | None:
    if value is None:
        return None
    return MaterializedQuote(
        journal_generation=UUID(value["journal_generation"]),
        stream_session_id=UUID(value["stream_session_id"]),
        instrument_id=value["instrument_id"],
        provider_symbol=value["provider_symbol"],
        source_session=_date(value["source_session"]),
        source_sequence=value["source_sequence"],
        source_timestamp=value["source_timestamp"],
        source_timestamp_unit=value["source_timestamp_unit"],
        event_at=_datetime(value["event_at"]),
        received_at=_datetime(value["received_at"]),
        bid=None if value["bid"] is None else Decimal(value["bid"]),
        ask=None if value["ask"] is None else Decimal(value["ask"]),
        bid_size=value["bid_size"],
        ask_size=value["ask_size"],
        bid_exchange=value["bid_exchange"],
        ask_exchange=value["ask_exchange"],
        quality=value["quality"],
        reason_codes=tuple(value["reason_codes"]),
        source_offset=value["source_offset"],
        payload_sha256=value["payload_sha256"],
    )


def _trade(value: Mapping[str, object] | None) -> MaterializedTrade | None:
    if value is None:
        return None
    return MaterializedTrade(
        journal_generation=UUID(value["journal_generation"]),
        stream_session_id=UUID(value["stream_session_id"]),
        instrument_id=value["instrument_id"],
        provider_symbol=value["provider_symbol"],
        asset_class=AssetClass(value["asset_class"]),
        source_session=_date(value["source_session"]),
        source_sequence=value["source_sequence"],
        source_timestamp=value["source_timestamp"],
        source_timestamp_unit=value["source_timestamp_unit"],
        event_at=_datetime(value["event_at"]),
        received_at=_datetime(value["received_at"]),
        price=Decimal(value["price"]),
        size=Decimal(value["size"]),
        exchange=value["exchange"],
        conditions=tuple(value["conditions"]),
        quality=value["quality"],
        reason_codes=tuple(value["reason_codes"]),
        eligibility=value["eligibility"],
        eligibility_reason_codes=tuple(value["eligibility_reason_codes"]),
        semantics_version=value["semantics_version"],
        source_offset=value["source_offset"],
        payload_sha256=value["payload_sha256"],
    )


def _tick(value: Mapping[str, object]) -> AdvancedOptionsUnderlyingTick:
    return AdvancedOptionsUnderlyingTick(
        instrument_id=value["instrument_id"],
        ticker=value["ticker"],
        session_date=_date(value["session_date"]),
        availability=RollingAvailability(value["availability"]),
        selected_at=_datetime(value["selected_at"]),
        option_market_time=_datetime(value["option_market_time"]),
        option_spot_skew_ms=value["option_spot_skew_ms"],
        freshness_ms=value["freshness_ms"],
        quote=_quote(value["quote"]),
        trade=_trade(value["trade"]),
        reason_codes=tuple(value["reason_codes"]),
    )


def _volume_checkpoint(value: Mapping[str, object]) -> VolumeCheckpoint:
    return VolumeCheckpoint(
        cohort_id=UUID(value["cohort_id"]),
        session_date=_date(value["session_date"]),
        contract_ticker=value["contract_ticker"],
        cumulative_volume=value["cumulative_volume"],
        last_source_seq=value["last_source_seq"],
        last_event_time=(
            None
            if value["last_event_time"] is None
            else _datetime(value["last_event_time"])
        ),
        baseline_kind=value["baseline_kind"],
        snapshot_volume=value["snapshot_volume"],
        snapshot_watermark_seq=value["snapshot_watermark_seq"],
    )


def _volume(
    value: Mapping[str, object],
) -> AdvancedOptionsContractVolumeState:
    return AdvancedOptionsContractVolumeState(
        checkpoint=_volume_checkpoint(value["checkpoint"]),
        completeness=BaselineKind(value["completeness"]),
        reconcile_evidence_sha256=value["reconcile_evidence_sha256"],
        reason_codes=tuple(value["reason_codes"]),
    )


def _order_window_member(
    value: Mapping[str, object],
) -> AdvancedOptionsOrderWindowMember:
    return AdvancedOptionsOrderWindowMember(
        event_id=UUID(value["event_id"]),
        source_offset=value["source_offset"],
        event_time=_datetime(value["event_time"]),
        window_seconds=value["window_seconds"],
    )


def _order_windows(
    value: Mapping[str, object],
) -> AdvancedOptionsOrderWindowState:
    return AdvancedOptionsOrderWindowState(
        owner_epoch=value["owner_epoch"],
        committed_offset=value["committed_offset"],
        checkpoint_sha256=value["checkpoint_sha256"],
        windows_seconds=tuple(value["windows_seconds"]),
        members=tuple(
            _order_window_member(row) for row in value["members"]
        ),
        maximum_members=value["maximum_members"],
    )


def _optional_lane(
    value: Mapping[str, object],
) -> AdvancedOptionsOptionalRollingLane:
    return AdvancedOptionsOptionalRollingLane(
        lane=value["lane"],
        availability=RollingAvailability(value["availability"]),
        reason_codes=tuple(value["reason_codes"]),
        evidence_sha256=value["evidence_sha256"],
    )


def decode_advanced_options_rolling_state(
    *,
    state_id: UUID,
    state_sha256: str,
    document: Mapping[str, object],
) -> AdvancedOptionsRollingState:
    if document.get("contract") != "advanced-options-rolling-state-v1":
        raise ValueError("stored rolling-state version changed")
    return AdvancedOptionsRollingState(
        state_id=state_id,
        state_sha256=state_sha256,
        version=document["contract"],
        cohort_id=UUID(document["cohort_id"]),
        session_date=_date(document["session_date"]),
        journal_generation=UUID(document["journal_generation"]),
        owner_epoch=document["owner_epoch"],
        committed_offset=document["committed_offset"],
        committed_at=_datetime(document["committed_at"]),
        market_state_checkpoint_sha256=(
            document["market_state_checkpoint_sha256"]
        ),
        flow_checkpoint_sha256=document["flow_checkpoint_sha256"],
        underlying_ticks=tuple(
            _tick(row) for row in document["underlying_ticks"]
        ),
        contract_volumes=tuple(
            _volume(row) for row in document["contract_volumes"]
        ),
        order_windows=_order_windows(document["order_windows"]),
        iv_surface=_optional_lane(document["iv_surface"]),
        dark_pool=_optional_lane(document["dark_pool"]),
    )


class PostgresAdvancedOptionsRollingStateRepository:
    def __init__(self, provider: V2ConnectionProvider) -> None:
        self._provider = provider

    def put(
        self,
        state: AdvancedOptionsRollingState,
    ) -> AdvancedOptionsRollingState:
        if not isinstance(state, AdvancedOptionsRollingState):
            raise TypeError("rolling state is invalid")
        document = json.loads(canonical_json(state.identity))
        with self._provider.transaction() as unit:
            cursor = unit.cursor
            cursor.execute(
                "INSERT INTO rt.advanced_options_rolling_checkpoints ("
                "state_id,state_sha256,state_version,cohort_id,session_date,"
                "journal_generation,owner_epoch,committed_offset,committed_at,"
                "market_state_checkpoint_sha256,flow_checkpoint_sha256,"
                "state_document) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (
                    str(state.state_id),
                    state.state_sha256,
                    state.version,
                    str(state.cohort_id),
                    state.session_date,
                    str(state.journal_generation),
                    state.owner_epoch,
                    state.committed_offset,
                    state.committed_at,
                    state.market_state_checkpoint_sha256,
                    state.flow_checkpoint_sha256,
                    Json(document),
                ),
            )
            cursor.execute(
                "SELECT state_id,state_sha256,state_document FROM "
                "rt.advanced_options_rolling_checkpoints "
                "WHERE cohort_id=%s AND session_date=%s "
                "AND journal_generation=%s AND owner_epoch=%s "
                "AND committed_offset=%s",
                (
                    str(state.cohort_id),
                    state.session_date,
                    str(state.journal_generation),
                    state.owner_epoch,
                    state.committed_offset,
                ),
            )
            stored = cursor.fetchone()
            if (
                stored is None
                or UUID(str(stored[0])) != state.state_id
                or str(stored[1]) != state.state_sha256
                or canonical_json(stored[2]) != canonical_json(document)
            ):
                raise ValueError("persisted rolling checkpoint conflicts")
        return state

    @staticmethod
    def _decode(row) -> AdvancedOptionsRollingState | None:
        if row is None:
            return None
        return decode_advanced_options_rolling_state(
            state_id=UUID(str(row[0])),
            state_sha256=str(row[1]),
            document=row[2],
        )

    def load_state(
        self,
        state_id: UUID,
    ) -> AdvancedOptionsRollingState | None:
        with self._provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT state_id,state_sha256,state_document FROM "
                "rt.advanced_options_rolling_checkpoints WHERE state_id=%s",
                (str(state_id),),
            )
            return self._decode(unit.cursor.fetchone())

    def load_latest(
        self,
        *,
        cohort_id: UUID,
        session_date: date,
        journal_generation: UUID,
    ) -> AdvancedOptionsRollingState | None:
        with self._provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT state_id,state_sha256,state_document FROM "
                "rt.advanced_options_rolling_checkpoints "
                "WHERE cohort_id=%s AND session_date=%s "
                "AND journal_generation=%s "
                "ORDER BY owner_epoch DESC,committed_offset DESC LIMIT 1",
                (
                    str(cohort_id),
                    session_date,
                    str(journal_generation),
                ),
            )
            return self._decode(unit.cursor.fetchone())
