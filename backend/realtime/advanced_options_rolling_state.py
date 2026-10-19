"""Source-only Tier B rolling state over existing canonical owners."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Callable
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import AssetClass, canonical_sha256
from .materialization import (
    MaterializedQuote,
    MaterializedTrade,
    MaterializerShardState,
)
from .option_flow.records import VolumeCheckpoint
from .option_flow.volume import COMPLETE_BASELINES, BaselineKind


ROLLING_STATE_VERSION = "advanced-options-rolling-state-v1"
CANONICAL_ORDER_WINDOWS_SECONDS = (1, 5, 60, 180, 900)
O2_MODEL_NOT_ACCEPTED = "O2_MODEL_NOT_ACCEPTED"
DARK_POOL_OWNER_NOT_ACCEPTED = "DARK_POOL_OWNER_NOT_ACCEPTED"


class RollingAvailability(str, Enum):
    READY = "READY"
    UNAVAILABLE = "UNAVAILABLE"


def _utc(value: datetime, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _reasons(values: tuple[str, ...]) -> tuple[str, ...]:
    reasons = tuple(sorted({value.strip().upper() for value in values}))
    if any(not value for value in reasons):
        raise ValueError("rolling-state reason codes cannot be blank")
    return reasons


@dataclass(frozen=True, slots=True)
class AdvancedOptionsUnderlyingTick:
    instrument_id: int
    ticker: str
    session_date: date
    availability: RollingAvailability
    selected_at: datetime
    option_market_time: datetime
    option_spot_skew_ms: int | None
    freshness_ms: int | None
    quote: MaterializedQuote | None
    trade: MaterializedTrade | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper()
        selected = _utc(self.selected_at, "selected_at")
        option_time = _utc(self.option_market_time, "option_market_time")
        reasons = _reasons(self.reason_codes)
        if (
            type(self.instrument_id) is not int
            or self.instrument_id <= 0
            or not ticker
            or not isinstance(self.session_date, date)
            or self.option_spot_skew_ms is not None
            and self.option_spot_skew_ms < 0
            or self.freshness_ms is not None
            and self.freshness_ms < 0
        ):
            raise ValueError("underlying tick identity is invalid")
        for evidence in (self.quote, self.trade):
            if evidence is not None and (
                evidence.instrument_id != self.instrument_id
                or evidence.provider_symbol != ticker
                or evidence.source_session != self.session_date
            ):
                raise ValueError("underlying current evidence scope changed")
        if (
            self.trade is not None
            and self.trade.asset_class is not AssetClass.EQUITY
        ):
            raise ValueError("underlying trade is not canonical Equity evidence")
        if self.availability is RollingAvailability.READY:
            if (
                self.quote is None
                or self.trade is None
                or reasons
                or self.option_spot_skew_ms is None
                or self.freshness_ms is None
                or self.quote.bid is None
                or self.quote.ask is None
                or self.trade.price <= 0
            ):
                raise ValueError("ready underlying tick is incomplete")
        elif (
            self.availability is not RollingAvailability.UNAVAILABLE
            or not reasons
        ):
            raise ValueError("unavailable underlying tick needs reasons")
        object.__setattr__(self, "ticker", ticker)
        object.__setattr__(self, "selected_at", selected)
        object.__setattr__(self, "option_market_time", option_time)
        object.__setattr__(self, "reason_codes", reasons)

    @property
    def source_identity_sha256(self) -> str:
        return canonical_sha256({
            "quote": self.quote,
            "trade": self.trade,
        })

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


def build_underlying_tick(
    *,
    instrument_id: int,
    ticker: str,
    session_date: date,
    quote: MaterializedQuote | None,
    trade: MaterializedTrade | None,
    option_market_time: datetime,
    selected_at: datetime,
    maximum_age_ms: int,
    maximum_option_spot_skew_ms: int,
) -> AdvancedOptionsUnderlyingTick:
    if (
        type(maximum_age_ms) is not int
        or maximum_age_ms <= 0
        or type(maximum_option_spot_skew_ms) is not int
        or maximum_option_spot_skew_ms < 0
    ):
        raise ValueError("underlying freshness and skew bounds are invalid")
    selected = _utc(selected_at, "selected_at")
    option_time = _utc(option_market_time, "option_market_time")
    reasons = []
    if quote is None:
        reasons.append("UNDERLYING_QUOTE_UNAVAILABLE")
    if trade is None:
        reasons.append("UNDERLYING_TRADE_UNAVAILABLE")
    current = tuple(
        row for row in (quote, trade) if row is not None
    )
    spot_time = max((row.event_at for row in current), default=None)
    freshness = (
        None
        if spot_time is None
        else int((selected - spot_time).total_seconds() * 1000)
    )
    skew = (
        None
        if spot_time is None
        else int(abs((option_time - spot_time).total_seconds()) * 1000)
    )
    if freshness is not None and (
        freshness < 0 or freshness > maximum_age_ms
    ):
        reasons.append("UNDERLYING_TICK_STALE_OR_FUTURE")
    if skew is not None and skew > maximum_option_spot_skew_ms:
        reasons.append("OPTION_SPOT_SKEW_EXCEEDED")
    if quote is not None and (
        quote.bid is None
        or quote.ask is None
        or quote.bid <= 0
        or quote.ask <= 0
        or quote.bid > quote.ask
    ):
        reasons.append("UNDERLYING_QUOTE_INVALID")
    if trade is not None and trade.price <= 0:
        reasons.append("UNDERLYING_TRADE_INVALID")
    return AdvancedOptionsUnderlyingTick(
        instrument_id=instrument_id,
        ticker=ticker,
        session_date=session_date,
        availability=(
            RollingAvailability.READY
            if not reasons
            else RollingAvailability.UNAVAILABLE
        ),
        selected_at=selected,
        option_market_time=option_time,
        option_spot_skew_ms=skew,
        freshness_ms=freshness,
        quote=quote,
        trade=trade,
        reason_codes=tuple(reasons),
    )


def underlying_tick_from_materializer_states(
    states: tuple[MaterializerShardState, ...],
    *,
    instrument_id: int,
    ticker: str,
    session_date: date,
    option_market_time: datetime,
    selected_at: datetime,
    maximum_age_ms: int,
    maximum_option_spot_skew_ms: int,
) -> AdvancedOptionsUnderlyingTick:
    quotes = tuple(
        quote
        for state in states
        for quote in state.quotes
        if quote.instrument_id == instrument_id
    )
    trades = tuple(
        trade
        for state in states
        for trade in state.trades
        if trade.instrument_id == instrument_id
    )
    if len(quotes) > 1 or len(trades) > 1:
        raise ValueError("canonical current owner returned duplicate evidence")
    return build_underlying_tick(
        instrument_id=instrument_id,
        ticker=ticker,
        session_date=session_date,
        quote=None if not quotes else quotes[0],
        trade=None if not trades else trades[0],
        option_market_time=option_market_time,
        selected_at=selected_at,
        maximum_age_ms=maximum_age_ms,
        maximum_option_spot_skew_ms=maximum_option_spot_skew_ms,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsContractVolumeState:
    checkpoint: VolumeCheckpoint
    completeness: BaselineKind
    reconcile_evidence_sha256: str | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.checkpoint, VolumeCheckpoint):
            raise TypeError("volume checkpoint is invalid")
        try:
            baseline = BaselineKind(self.checkpoint.baseline_kind)
        except ValueError:
            raise ValueError("volume baseline is invalid") from None
        reasons = _reasons(self.reason_codes)
        if baseline is not self.completeness:
            raise ValueError("volume completeness changed")
        if baseline in COMPLETE_BASELINES:
            if reasons:
                raise ValueError("complete volume state cannot carry gap reasons")
            if baseline is BaselineKind.SNAPSHOT_RECONCILED:
                if self.reconcile_evidence_sha256 is None:
                    raise ValueError(
                        "reconciled volume needs causal evidence"
                    )
                object.__setattr__(
                    self,
                    "reconcile_evidence_sha256",
                    _sha256(
                        self.reconcile_evidence_sha256,
                        "reconcile_evidence_sha256",
                    ),
                )
            elif self.reconcile_evidence_sha256 is not None:
                raise ValueError(
                    "session-complete volume cannot carry reconcile evidence"
                )
        elif not reasons or self.reconcile_evidence_sha256 is not None:
            raise ValueError("incomplete volume state needs unresolved reasons")
        object.__setattr__(self, "reason_codes", reasons)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


def contract_volume_state(
    checkpoint: VolumeCheckpoint,
    *,
    reason_codes: tuple[str, ...] = (),
    reconcile_evidence_sha256: str | None = None,
) -> AdvancedOptionsContractVolumeState:
    baseline = BaselineKind(checkpoint.baseline_kind)
    reasons = reason_codes
    if baseline not in COMPLETE_BASELINES and not reasons:
        reasons = (f"VOLUME_{baseline.value}",)
    return AdvancedOptionsContractVolumeState(
        checkpoint,
        baseline,
        reconcile_evidence_sha256,
        tuple(reasons),
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsOrderWindowMember:
    event_id: UUID
    source_offset: int
    event_time: datetime
    window_seconds: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.event_id, UUID)
            or type(self.source_offset) is not int
            or self.source_offset <= 0
            or self.window_seconds not in CANONICAL_ORDER_WINDOWS_SECONDS
        ):
            raise ValueError("order-window member identity is invalid")
        object.__setattr__(
            self,
            "event_time",
            _utc(self.event_time, "event_time"),
        )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsOrderWindowState:
    owner_epoch: int
    committed_offset: int
    checkpoint_sha256: str
    windows_seconds: tuple[int, ...]
    members: tuple[AdvancedOptionsOrderWindowMember, ...]
    maximum_members: int = 10_000

    def __post_init__(self) -> None:
        members = tuple(sorted(
            self.members,
            key=lambda row: (
                row.window_seconds,
                row.source_offset,
                str(row.event_id),
            ),
        ))
        if (
            type(self.owner_epoch) is not int
            or self.owner_epoch <= 0
            or type(self.committed_offset) is not int
            or self.committed_offset < 0
            or self.windows_seconds != CANONICAL_ORDER_WINDOWS_SECONDS
            or type(self.maximum_members) is not int
            or self.maximum_members <= 0
            or len(members) > self.maximum_members
            or len({(row.window_seconds, row.event_id) for row in members})
            != len(members)
            or any(row.source_offset > self.committed_offset for row in members)
        ):
            raise ValueError("order-window checkpoint is invalid or unbounded")
        object.__setattr__(
            self,
            "checkpoint_sha256",
            _sha256(self.checkpoint_sha256, "checkpoint_sha256"),
        )
        object.__setattr__(self, "members", members)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsOptionalRollingLane:
    lane: str
    availability: RollingAvailability
    reason_codes: tuple[str, ...]
    evidence_sha256: str | None = None

    def __post_init__(self) -> None:
        lane = self.lane.strip().upper()
        reasons = _reasons(self.reason_codes)
        if lane not in {"IV_SURFACE", "DARK_POOL"}:
            raise ValueError("optional rolling lane is invalid")
        if self.availability is RollingAvailability.READY:
            if reasons or self.evidence_sha256 is None:
                raise ValueError("ready optional lane needs exact evidence")
            object.__setattr__(
                self,
                "evidence_sha256",
                _sha256(self.evidence_sha256, "evidence_sha256"),
            )
        elif (
            self.availability is not RollingAvailability.UNAVAILABLE
            or not reasons
            or self.evidence_sha256 is not None
        ):
            raise ValueError("unavailable optional lane needs reasons only")
        object.__setattr__(self, "lane", lane)
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsRollingState:
    state_id: UUID
    state_sha256: str
    version: str
    cohort_id: UUID
    session_date: date
    journal_generation: UUID
    owner_epoch: int
    committed_offset: int
    committed_at: datetime
    market_state_checkpoint_sha256: str
    flow_checkpoint_sha256: str
    underlying_ticks: tuple[AdvancedOptionsUnderlyingTick, ...]
    contract_volumes: tuple[AdvancedOptionsContractVolumeState, ...]
    order_windows: AdvancedOptionsOrderWindowState
    iv_surface: AdvancedOptionsOptionalRollingLane
    dark_pool: AdvancedOptionsOptionalRollingLane

    def __post_init__(self) -> None:
        ticks = tuple(sorted(
            self.underlying_ticks,
            key=lambda row: row.instrument_id,
        ))
        volumes = tuple(sorted(
            self.contract_volumes,
            key=lambda row: row.checkpoint.contract_ticker,
        ))
        committed = _utc(self.committed_at, "committed_at")
        if (
            self.version != ROLLING_STATE_VERSION
            or not isinstance(self.cohort_id, UUID)
            or not isinstance(self.session_date, date)
            or not isinstance(self.journal_generation, UUID)
            or type(self.owner_epoch) is not int
            or self.owner_epoch <= 0
            or type(self.committed_offset) is not int
            or self.committed_offset < 0
            or not ticks
            or len({row.instrument_id for row in ticks}) != len(ticks)
            or not volumes
            or len({
                row.checkpoint.contract_ticker for row in volumes
            }) != len(volumes)
            or any(row.session_date != self.session_date for row in ticks)
            or any(
                evidence is not None
                and evidence.source_offset > self.committed_offset
                for row in ticks
                for evidence in (row.quote, row.trade)
            )
            or any(
                row.checkpoint.session_date != self.session_date
                or row.checkpoint.cohort_id != self.cohort_id
                for row in volumes
            )
            or self.order_windows.owner_epoch != self.owner_epoch
            or self.order_windows.committed_offset != self.committed_offset
            or self.iv_surface.lane != "IV_SURFACE"
            or self.dark_pool.lane != "DARK_POOL"
        ):
            raise ValueError("Advanced Options rolling state is incomplete")
        object.__setattr__(self, "committed_at", committed)
        for name in (
            "state_sha256",
            "market_state_checkpoint_sha256",
            "flow_checkpoint_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        object.__setattr__(self, "underlying_ticks", ticks)
        object.__setattr__(self, "contract_volumes", volumes)
        expected_sha256 = canonical_sha256(self.identity)
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-rolling-state:{expected_sha256}",
        )
        if self.state_sha256 != expected_sha256 or self.state_id != expected_id:
            raise ValueError("rolling state ID or SHA does not match its identity")

    @property
    def identity(self) -> dict[str, object]:
        return {
            "contract": ROLLING_STATE_VERSION,
            "cohort_id": self.cohort_id,
            "session_date": self.session_date,
            "journal_generation": self.journal_generation,
            "owner_epoch": self.owner_epoch,
            "committed_offset": self.committed_offset,
            "committed_at": self.committed_at,
            "market_state_checkpoint_sha256": (
                self.market_state_checkpoint_sha256
            ),
            "flow_checkpoint_sha256": self.flow_checkpoint_sha256,
            "underlying_ticks": self.underlying_ticks,
            "contract_volumes": self.contract_volumes,
            "order_windows": self.order_windows,
            "iv_surface": self.iv_surface,
            "dark_pool": self.dark_pool,
        }


def build_advanced_options_rolling_state(
    *,
    cohort_id: UUID,
    session_date: date,
    journal_generation: UUID,
    owner_epoch: int,
    committed_offset: int,
    committed_at: datetime,
    market_state_checkpoint_sha256: str,
    flow_checkpoint_sha256: str,
    underlying_ticks: tuple[AdvancedOptionsUnderlyingTick, ...],
    contract_volumes: tuple[AdvancedOptionsContractVolumeState, ...],
    order_windows: AdvancedOptionsOrderWindowState,
    iv_surface: AdvancedOptionsOptionalRollingLane | None = None,
    dark_pool: AdvancedOptionsOptionalRollingLane | None = None,
) -> AdvancedOptionsRollingState:
    if (
        iv_surface is not None
        and iv_surface.availability is RollingAvailability.READY
    ):
        raise ValueError("IV surface model/policy is not accepted")
    if (
        dark_pool is not None
        and dark_pool.availability is RollingAvailability.READY
    ):
        raise ValueError("dark-pool source/coverage is not accepted")
    normalized_ticks = tuple(sorted(
        underlying_ticks,
        key=lambda row: row.instrument_id,
    ))
    normalized_volumes = tuple(sorted(
        contract_volumes,
        key=lambda row: row.checkpoint.contract_ticker,
    ))
    iv = iv_surface or AdvancedOptionsOptionalRollingLane(
        "IV_SURFACE",
        RollingAvailability.UNAVAILABLE,
        (O2_MODEL_NOT_ACCEPTED,),
    )
    dark = dark_pool or AdvancedOptionsOptionalRollingLane(
        "DARK_POOL",
        RollingAvailability.UNAVAILABLE,
        (DARK_POOL_OWNER_NOT_ACCEPTED,),
    )
    identity = {
        "contract": ROLLING_STATE_VERSION,
        "cohort_id": cohort_id,
        "session_date": session_date,
        "journal_generation": journal_generation,
        "owner_epoch": owner_epoch,
        "committed_offset": committed_offset,
        "committed_at": committed_at,
        "market_state_checkpoint_sha256": market_state_checkpoint_sha256,
        "flow_checkpoint_sha256": flow_checkpoint_sha256,
        "underlying_ticks": normalized_ticks,
        "contract_volumes": normalized_volumes,
        "order_windows": order_windows,
        "iv_surface": iv,
        "dark_pool": dark,
    }
    digest = canonical_sha256(identity)
    return AdvancedOptionsRollingState(
        state_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-rolling-state:{digest}",
        ),
        state_sha256=digest,
        version=ROLLING_STATE_VERSION,
        cohort_id=cohort_id,
        session_date=session_date,
        journal_generation=journal_generation,
        owner_epoch=owner_epoch,
        committed_offset=committed_offset,
        committed_at=committed_at,
        market_state_checkpoint_sha256=market_state_checkpoint_sha256,
        flow_checkpoint_sha256=flow_checkpoint_sha256,
        underlying_ticks=normalized_ticks,
        contract_volumes=normalized_volumes,
        order_windows=order_windows,
        iv_surface=iv,
        dark_pool=dark,
    )


class AdvancedOptionsRollingStateOwner:
    """Advance memory only after the caller's atomic persistence callback succeeds."""

    def __init__(
        self,
        state: AdvancedOptionsRollingState | None = None,
    ) -> None:
        self._state = state

    @property
    def state(self) -> AdvancedOptionsRollingState | None:
        return self._state

    def commit(
        self,
        candidate: AdvancedOptionsRollingState,
        *,
        persist: Callable[[AdvancedOptionsRollingState], None],
    ) -> None:
        if not isinstance(candidate, AdvancedOptionsRollingState):
            raise TypeError("rolling-state candidate is invalid")
        current = self._state
        if current is not None and (
            candidate.cohort_id != current.cohort_id
            or candidate.session_date != current.session_date
            or candidate.journal_generation != current.journal_generation
            or candidate.owner_epoch != current.owner_epoch
            or candidate.committed_offset < current.committed_offset
            or (
                candidate.committed_offset == current.committed_offset
                and candidate != current
            )
        ):
            raise ValueError("rolling-state checkpoint regressed or diverged")
        persist(candidate)
        self._state = candidate
