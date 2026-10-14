from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from options.config import ValuationPolicy
from options.domain import ContractType, MarkSource, OptionContractSnapshot
from options.outcomes import (
    CURRENT_MARK_MEASUREMENT,
    OptionDecayOutcome,
    OptionOutcomeLeg,
    evaluate_delayed_proxy_outcome,
)
from options.strategies.domain import OptionSide

from .materialization import MaterializedQuote
from .preflight import ProviderTier


REALTIME_OPTION_MARK_ADAPTER_VERSION = "realtime_option_package_mark_v1"
MAX_PACKAGE_LEGS = 8
MAX_PACKAGE_QUOTE_SKEW_MS = 250


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class RealtimeOptionPackageLeg:
    leg_index: int
    instrument_id: int
    contract_id: int
    catalog_version_id: int
    contract_ticker: str
    side: OptionSide
    ratio: int
    multiplier: int
    strike: Decimal
    expiration_date: date
    contract_type: ContractType
    entry_mark: Decimal
    entry_market_time: datetime
    valuation_policy_sha256: str

    def __post_init__(self) -> None:
        ticker = self.contract_ticker.strip().upper()
        if min(
            self.instrument_id,
            self.contract_id,
            self.catalog_version_id,
            self.ratio,
            self.multiplier,
        ) <= 0:
            raise ValueError("option package leg identities, ratio and multiplier must be positive")
        if type(self.leg_index) is not int or self.leg_index < 0:
            raise ValueError("option package leg index must be nonnegative")
        if not ticker or not ticker.startswith("O:"):
            raise ValueError("option package leg must retain its provider ticker")
        if self.strike <= 0 or self.entry_mark <= 0:
            raise ValueError("option package leg strike and entry mark must be positive")
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(
            self,
            "entry_market_time",
            _utc(self.entry_market_time, "entry_market_time"),
        )
        object.__setattr__(
            self,
            "valuation_policy_sha256",
            _sha256(self.valuation_policy_sha256, "valuation_policy_sha256"),
        )


@dataclass(frozen=True, slots=True)
class RealtimeOptionPackage:
    candidate_id: UUID
    event_id: UUID | None
    matrix_id: UUID
    candidate_identity_sha256: str
    cohort_id: UUID
    provider_tier: ProviderTier
    entry_market_time: datetime
    entry_net_premium: Decimal
    capital_at_risk: Decimal
    exit_deadline: datetime | None
    legs: tuple[RealtimeOptionPackageLeg, ...]

    def __post_init__(self) -> None:
        if not 1 <= len(self.legs) <= MAX_PACKAGE_LEGS:
            raise ValueError("realtime option package requires 1-8 legs")
        instrument_ids = tuple(leg.instrument_id for leg in self.legs)
        contract_ids = tuple(leg.contract_id for leg in self.legs)
        leg_indices = tuple(leg.leg_index for leg in self.legs)
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("realtime option package instrument IDs must be unique")
        if len(contract_ids) != len(set(contract_ids)):
            raise ValueError("realtime option package contract IDs must be unique")
        if set(leg_indices) != set(range(len(self.legs))):
            raise ValueError("realtime option package leg indexes must be contiguous")
        if self.capital_at_risk <= 0 or not self.entry_net_premium.is_finite():
            raise ValueError("realtime option package economics are invalid")
        entry_time = _utc(self.entry_market_time, "entry_market_time")
        deadline = (
            _utc(self.exit_deadline, "exit_deadline")
            if self.exit_deadline is not None
            else None
        )
        if any(leg.entry_market_time > entry_time for leg in self.legs):
            raise ValueError("package entry time cannot precede a leg entry mark")
        if deadline is not None and deadline <= entry_time:
            raise ValueError("package exit deadline must follow entry time")
        object.__setattr__(self, "entry_market_time", entry_time)
        object.__setattr__(self, "exit_deadline", deadline)
        object.__setattr__(self, "legs", tuple(sorted(self.legs, key=lambda leg: leg.leg_index)))
        object.__setattr__(
            self,
            "candidate_identity_sha256",
            _sha256(self.candidate_identity_sha256, "candidate_identity_sha256"),
        )


class RealtimePackageMarkStatus(str, Enum):
    READY = "READY"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class RealtimeQuoteEvidence:
    evidence_id: UUID
    instrument_id: int
    contract_id: int
    catalog_version_id: int
    journal_generation: UUID
    stream_session_id: UUID
    source_offset: int
    market_time: datetime
    observed_time: datetime
    bid: Decimal
    ask: Decimal
    midpoint: Decimal
    payload_sha256: str

    @property
    def mark(self) -> Decimal:
        return self.midpoint

    @property
    def mark_source(self) -> MarkSource:
        return MarkSource.ADVANCED_NBBO_MIDPOINT


@dataclass(frozen=True, slots=True)
class RealtimeModelMarkEvidence:
    evidence_id: UUID
    instrument_id: int
    contract_id: int
    catalog_version_id: int
    source_batch_id: UUID
    source_revision_id: UUID
    market_time: datetime
    observed_time: datetime
    mark: Decimal
    mark_source: MarkSource
    payload_sha256: str
    quality_flags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RealtimeOptionPackageMark:
    package: RealtimeOptionPackage
    adapter_version: str
    computed_at: datetime
    outcome: OptionDecayOutcome
    evidence_batch_id: UUID
    mark_evidence: tuple[RealtimeQuoteEvidence | RealtimeModelMarkEvidence, ...]

    def retained_row(self) -> dict[str, object]:
        by_instrument = {
            row.instrument_id: row for row in self.mark_evidence
        }
        legs = []
        for leg in self.package.legs:
            evidence = by_instrument[leg.instrument_id]
            legs.append({
                "leg_index": leg.leg_index,
                "contract_id": leg.contract_id,
                "contract_ticker": leg.contract_ticker,
                "side": leg.side.value,
                "ratio": leg.ratio,
                "multiplier": leg.multiplier,
                "strike": leg.strike,
                "expiration_date": leg.expiration_date,
                "contract_type": leg.contract_type.value,
                "entry_mark": leg.entry_mark,
                "entry_valuation_policy_sha256": leg.valuation_policy_sha256,
                "snapshot_multiplier": leg.multiplier,
                "entry_mark_source": evidence.mark_source.value,
                "exit_mark": evidence.mark,
                "exit_mark_source": evidence.mark_source.value,
                "valuation_policy_sha256": self.outcome.valuation_policy_sha256,
                "snapshot_id": evidence.evidence_id,
                "batch_id": self.evidence_batch_id,
                "source_market_time": evidence.market_time,
                "source_observed_time": evidence.observed_time,
                "revised_observed_at": None,
            })
        return {
            "candidate_id": self.package.candidate_id,
            "candidate_identity": self.package.candidate_identity_sha256,
            "matrix_id": self.package.matrix_id,
            "event_id": self.package.event_id,
            "market_time": self.outcome.market_time,
            "observed_time": self.outcome.observed_time,
            "updated_at": self.computed_at,
            "mark": self.outcome.exit_net_premium,
            "entry_net_premium": self.outcome.entry_net_premium,
            "exit_net_premium": self.outcome.exit_net_premium,
            "gross_pnl": self.outcome.gross_pnl,
            "estimated_cost": self.outcome.estimated_cost,
            "net_pnl": self.outcome.net_pnl,
            "capital_at_risk": self.outcome.capital_at_risk,
            "net_return": self.outcome.net_return,
            "availability_flag": self.outcome.availability_flag,
            "quality_flags": list(self.outcome.quality_flags),
            "valuation_policy_version": self.outcome.valuation_policy_version,
            "valuation_policy_sha256": self.outcome.valuation_policy_sha256,
            "source_snapshot_ids": list(self.outcome.source_snapshot_ids),
            "source_batch_id": self.outcome.source_batch_id,
            "management_status": None,
            "legs": legs,
        }


@dataclass(frozen=True, slots=True)
class RealtimeOptionPackageMarkResult:
    status: RealtimePackageMarkStatus
    reason_codes: tuple[str, ...]
    mark: RealtimeOptionPackageMark | None


def _unavailable(*reasons: str) -> RealtimeOptionPackageMarkResult:
    return RealtimeOptionPackageMarkResult(
        RealtimePackageMarkStatus.UNAVAILABLE,
        tuple(dict.fromkeys(reasons)),
        None,
    )


def materialize_realtime_option_package_mark(
    package: RealtimeOptionPackage,
    quotes: Mapping[int, MaterializedQuote],
    *,
    checked_at: datetime,
    policy: ValuationPolicy,
    maximum_quote_skew_ms: int = MAX_PACKAGE_QUOTE_SKEW_MS,
) -> RealtimeOptionPackageMarkResult:
    checked = _utc(checked_at, "checked_at")
    if maximum_quote_skew_ms < 0:
        raise ValueError("maximum package quote skew cannot be negative")
    if package.provider_tier is not ProviderTier.MASSIVE_ADVANCED:
        return _unavailable("ADVANCED_PACKAGE_REQUIRES_ADVANCED_COHORT")
    required_source = MarkSource.ADVANCED_NBBO_MIDPOINT
    if (
        policy.primary_model_mark_source is not required_source
        or required_source not in policy.allowed_entry_mark_sources
        or required_source not in policy.allowed_exit_mark_sources
    ):
        return _unavailable("ADVANCED_NBBO_VALUATION_POLICY_REQUIRED")
    policy_hashes = {leg.valuation_policy_sha256 for leg in package.legs}
    if policy_hashes != {policy.policy_sha256}:
        return _unavailable("PACKAGE_ENTRY_VALUATION_POLICY_MISMATCH")
    evidence = []
    generations = set()
    stream_sessions = set()
    for leg in package.legs:
        quote = quotes.get(leg.instrument_id)
        if quote is None:
            return _unavailable(f"PACKAGE_LEG_QUOTE_MISSING:{leg.contract_id}")
        if quote.instrument_id != leg.instrument_id or quote.provider_symbol != leg.contract_ticker:
            return _unavailable(f"PACKAGE_LEG_QUOTE_IDENTITY_MISMATCH:{leg.contract_id}")
        if quote.quality != "GOOD":
            return _unavailable(f"PACKAGE_LEG_QUOTE_{quote.quality}:{leg.contract_id}")
        if (
            quote.bid is None
            or quote.ask is None
            or quote.bid_size is None
            or quote.ask_size is None
            or quote.bid <= 0
            or quote.ask <= 0
            or quote.ask < quote.bid
            or quote.bid_size <= 0
            or quote.ask_size <= 0
        ):
            return _unavailable(f"PACKAGE_LEG_QUOTE_UNUSABLE:{leg.contract_id}")
        if not package.entry_market_time <= quote.event_at <= quote.received_at <= checked:
            return _unavailable(f"PACKAGE_LEG_QUOTE_CLOCK_INVALID:{leg.contract_id}")
        generations.add(quote.journal_generation)
        stream_sessions.add(quote.stream_session_id)
        midpoint = (quote.bid + quote.ask) / Decimal(2)
        evidence_id = uuid5(
            NAMESPACE_URL,
            f"realtime-option-quote-evidence:{quote.journal_generation}:"
            f"{quote.source_offset}:{quote.payload_sha256}",
        )
        evidence.append(RealtimeQuoteEvidence(
            evidence_id=evidence_id,
            instrument_id=leg.instrument_id,
            contract_id=leg.contract_id,
            catalog_version_id=leg.catalog_version_id,
            journal_generation=quote.journal_generation,
            stream_session_id=quote.stream_session_id,
            source_offset=quote.source_offset,
            market_time=quote.event_at,
            observed_time=quote.received_at,
            bid=quote.bid,
            ask=quote.ask,
            midpoint=midpoint,
            payload_sha256=quote.payload_sha256,
        ))
    if len(generations) != 1:
        return _unavailable("PACKAGE_QUOTES_CROSS_JOURNAL_GENERATION")
    if len(stream_sessions) != 1:
        return _unavailable("PACKAGE_QUOTES_CROSS_STREAM_SESSION")
    market_times = tuple(row.market_time for row in evidence)
    if (max(market_times) - min(market_times)).total_seconds() * 1000 > maximum_quote_skew_ms:
        return _unavailable("PACKAGE_QUOTE_TIME_SKEW_EXCEEDED")
    batch_identity = ":".join(str(row.evidence_id) for row in evidence)
    evidence_batch_id = uuid5(
        NAMESPACE_URL,
        f"realtime-option-quote-batch:{package.candidate_id}:{batch_identity}",
    )
    outcome_legs = tuple(
        OptionOutcomeLeg(
            contract_id=leg.contract_id,
            side=leg.side,
            ratio=leg.ratio,
            multiplier=leg.multiplier,
            entry_mark=leg.entry_mark,
            exit_mark=evidence[index].midpoint,
            source_snapshot_id=evidence[index].evidence_id,
            source_batch_id=evidence_batch_id,
            source_market_time=evidence[index].market_time,
            source_observed_time=evidence[index].observed_time,
            entry_mark_source=required_source,
            exit_mark_source=required_source,
            entry_valuation_policy_sha256=leg.valuation_policy_sha256,
            exit_valuation_policy_sha256=policy.policy_sha256,
        )
        for index, leg in enumerate(package.legs)
    )
    outcome = evaluate_delayed_proxy_outcome(
        candidate_id=package.candidate_id,
        event_id=package.event_id,
        measurement_type=CURRENT_MARK_MEASUREMENT,
        market_time=max(row.market_time for row in evidence),
        observed_time=max(row.observed_time for row in evidence),
        capital_at_risk=package.capital_at_risk,
        legs=outcome_legs,
        policy=policy,
    )
    outcome = replace(
        outcome,
        availability_flag="REALTIME_ADVANCED_NBBO",
        quality_flags=(
            "COMMISSION_ONLY_COST_MODEL",
            MarkSource.ADVANCED_NBBO_MIDPOINT.value,
            "SLIPPAGE_UNAVAILABLE",
        ),
    )
    if outcome.entry_net_premium != package.entry_net_premium:
        return _unavailable("PACKAGE_ENTRY_PREMIUM_MISMATCH")
    mark = RealtimeOptionPackageMark(
        package=package,
        adapter_version=REALTIME_OPTION_MARK_ADAPTER_VERSION,
        computed_at=checked,
        outcome=outcome,
        evidence_batch_id=evidence_batch_id,
        mark_evidence=tuple(evidence),
    )
    return RealtimeOptionPackageMarkResult(
        RealtimePackageMarkStatus.READY,
        (),
        mark,
    )


def materialize_developer_option_package_mark(
    package: RealtimeOptionPackage,
    snapshots: Mapping[int, OptionContractSnapshot],
    *,
    checked_at: datetime,
    policy: ValuationPolicy,
) -> RealtimeOptionPackageMarkResult:
    checked = _utc(checked_at, "checked_at")
    required_source = MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE
    if package.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
        return _unavailable("DEVELOPER_PACKAGE_REQUIRES_DEVELOPER_COHORT")
    if (
        policy.primary_model_mark_source is not required_source
        or required_source not in policy.allowed_entry_mark_sources
        or required_source not in policy.allowed_exit_mark_sources
    ):
        return _unavailable("DEVELOPER_AGGREGATE_VALUATION_POLICY_REQUIRED")
    if {leg.valuation_policy_sha256 for leg in package.legs} != {policy.policy_sha256}:
        return _unavailable("PACKAGE_ENTRY_VALUATION_POLICY_MISMATCH")
    evidence = []
    batch_ids = set()
    for leg in package.legs:
        snapshot = snapshots.get(leg.instrument_id)
        if snapshot is None:
            return _unavailable(f"PACKAGE_LEG_MODEL_MARK_MISSING:{leg.contract_id}")
        if (
            snapshot.contract_id != leg.contract_id
            or snapshot.contract_ticker != leg.contract_ticker
        ):
            return _unavailable(f"PACKAGE_LEG_MODEL_MARK_IDENTITY_MISMATCH:{leg.contract_id}")
        if (
            snapshot.model_mark is None
            or snapshot.mark_source is not required_source
            or snapshot.valuation_policy_sha256 != policy.policy_sha256
        ):
            return _unavailable(f"PACKAGE_LEG_MODEL_MARK_UNAVAILABLE:{leg.contract_id}")
        observed_time = snapshot.revised_observed_at or snapshot.first_observed_at
        if not package.entry_market_time <= snapshot.mark_market_data_time <= observed_time <= checked:
            return _unavailable(f"PACKAGE_LEG_MODEL_MARK_CLOCK_INVALID:{leg.contract_id}")
        batch_ids.add(snapshot.batch_id)
        evidence.append(RealtimeModelMarkEvidence(
            evidence_id=snapshot.snapshot_id,
            instrument_id=leg.instrument_id,
            contract_id=leg.contract_id,
            catalog_version_id=leg.catalog_version_id,
            source_batch_id=snapshot.batch_id,
            source_revision_id=snapshot.snapshot_id,
            market_time=snapshot.mark_market_data_time,
            observed_time=observed_time,
            mark=snapshot.model_mark,
            mark_source=snapshot.mark_source,
            payload_sha256=snapshot.normalized_payload_sha256,
            quality_flags=tuple(flag.value for flag in snapshot.quality_flags),
        ))
    if len(batch_ids) != 1:
        return _unavailable("PACKAGE_MODEL_MARKS_CROSS_BATCH")
    market_times = tuple(row.market_time for row in evidence)
    if (
        max(market_times) - min(market_times)
    ).total_seconds() > policy.maximum_option_spot_skew_seconds:
        return _unavailable("PACKAGE_MODEL_MARK_TIME_SKEW_EXCEEDED")
    evidence_batch_id = next(iter(batch_ids))
    outcome_legs = tuple(
        OptionOutcomeLeg(
            contract_id=leg.contract_id,
            side=leg.side,
            ratio=leg.ratio,
            multiplier=leg.multiplier,
            entry_mark=leg.entry_mark,
            exit_mark=evidence[index].mark,
            source_snapshot_id=evidence[index].evidence_id,
            source_batch_id=evidence_batch_id,
            source_market_time=evidence[index].market_time,
            source_observed_time=evidence[index].observed_time,
            entry_mark_source=required_source,
            exit_mark_source=required_source,
            entry_valuation_policy_sha256=leg.valuation_policy_sha256,
            exit_valuation_policy_sha256=policy.policy_sha256,
        )
        for index, leg in enumerate(package.legs)
    )
    outcome = evaluate_delayed_proxy_outcome(
        candidate_id=package.candidate_id,
        event_id=package.event_id,
        measurement_type=CURRENT_MARK_MEASUREMENT,
        market_time=max(row.market_time for row in evidence),
        observed_time=max(row.observed_time for row in evidence),
        capital_at_risk=package.capital_at_risk,
        legs=outcome_legs,
        policy=policy,
    )
    outcome = replace(
        outcome,
        availability_flag="DEVELOPER_DELAYED_SNAPSHOT",
        quality_flags=tuple(dict.fromkeys((
            "COMMISSION_ONLY_COST_MODEL",
            MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE.value,
            "INDICATIVE_NO_OPTION_QUOTES",
            "SLIPPAGE_UNAVAILABLE",
            *(flag for row in evidence for flag in row.quality_flags),
        ))),
    )
    if outcome.entry_net_premium != package.entry_net_premium:
        return _unavailable("PACKAGE_ENTRY_PREMIUM_MISMATCH")
    mark = RealtimeOptionPackageMark(
        package=package,
        adapter_version=REALTIME_OPTION_MARK_ADAPTER_VERSION,
        computed_at=checked,
        outcome=outcome,
        evidence_batch_id=evidence_batch_id,
        mark_evidence=tuple(evidence),
    )
    return RealtimeOptionPackageMarkResult(
        RealtimePackageMarkStatus.READY,
        (),
        mark,
    )


def portal_current_quote_row(
    leg: RealtimeOptionPackageLeg,
    quote: MaterializedQuote,
) -> dict[str, object]:
    if quote.provider_symbol != leg.contract_ticker:
        raise ValueError("portal quote contract identity mismatch")
    return {
        "contract_id": leg.contract_id,
        "contract_ticker": leg.contract_ticker,
        "quote_time": quote.event_at,
        "received_at": quote.received_at,
        "bid": quote.bid,
        "ask": quote.ask,
        "bid_size": quote.bid_size,
        "ask_size": quote.ask_size,
        "updated_at": quote.received_at,
    }