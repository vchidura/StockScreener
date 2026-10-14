from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from options.strategies.domain import OptionSide

from .contracts import canonical_sha256
from .option_portal_adapter import (
    RealtimeOptionPackage,
    RealtimeOptionPackageMark,
    RealtimeQuoteEvidence,
)
from .preflight import ProviderTier


class LocalMockStatus(str, Enum):
    FILLED = "MOCK_FILLED"
    REJECTED = "MOCK_REJECTED"
    UNAVAILABLE = "MOCK_UNAVAILABLE"


class LocalMockEarningsStatus(str, Enum):
    CLEAR = "CLEAR"
    BLOCKED = "BLOCKED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class LocalMockEarningsAssessment:
    status: LocalMockEarningsStatus
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        reasons = tuple(reason.strip() for reason in self.reason_codes)
        if any(not reason for reason in reasons):
            raise ValueError("local mock earnings reasons cannot be blank")
        if self.status is LocalMockEarningsStatus.CLEAR and reasons:
            raise ValueError("clear local mock earnings cannot have reasons")
        if self.status is not LocalMockEarningsStatus.CLEAR and not reasons:
            raise ValueError("non-clear local mock earnings requires reasons")
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class LocalMockExecutionPolicy:
    version: str
    maximum_quote_age_ms: int
    maximum_quote_skew_ms: int
    slippage_per_share: Decimal
    fee_per_contract: Decimal
    maximum_capital_at_risk: Decimal
    maximum_total_contracts: int

    def __post_init__(self) -> None:
        version = self.version.strip()
        if not version:
            raise ValueError("local mock policy version cannot be blank")
        if self.maximum_quote_age_ms <= 0 or self.maximum_quote_skew_ms < 0:
            raise ValueError("local mock quote timing limits are invalid")
        if (
            not self.slippage_per_share.is_finite()
            or self.slippage_per_share < 0
            or not self.fee_per_contract.is_finite()
            or self.fee_per_contract < 0
            or not self.maximum_capital_at_risk.is_finite()
            or self.maximum_capital_at_risk <= 0
        ):
            raise ValueError("local mock price, fee or risk policy is invalid")
        if type(self.maximum_total_contracts) is not int or self.maximum_total_contracts <= 0:
            raise ValueError("local mock contract limit must be positive")
        object.__setattr__(self, "version", version)

    @property
    def sha256(self) -> str:
        return canonical_sha256({
            "version": self.version,
            "maximum_quote_age_ms": self.maximum_quote_age_ms,
            "maximum_quote_skew_ms": self.maximum_quote_skew_ms,
            "slippage_per_share": str(self.slippage_per_share),
            "fee_per_contract": str(self.fee_per_contract),
            "maximum_capital_at_risk": str(self.maximum_capital_at_risk),
            "maximum_total_contracts": self.maximum_total_contracts,
        })


@dataclass(frozen=True, slots=True)
class LocalMockFill:
    fill_id: UUID
    leg_index: int
    instrument_id: int
    contract_id: int
    contract_ticker: str
    side: OptionSide
    quantity: int
    multiplier: int
    bid: Decimal
    ask: Decimal
    fill_price: Decimal
    fee: Decimal
    source_evidence_id: UUID


@dataclass(frozen=True, slots=True)
class LocalMockExecutionResult:
    request_id: UUID
    alert_id: UUID
    package: RealtimeOptionPackage
    requested_at: datetime
    policy: LocalMockExecutionPolicy
    earnings: LocalMockEarningsAssessment
    evidence_batch_id: UUID
    source_evidence_ids: tuple[UUID, ...]
    status: LocalMockStatus
    reason_codes: tuple[str, ...]
    fills: tuple[LocalMockFill, ...] = ()
    gross_cash_flow: Decimal | None = None
    estimated_fees: Decimal | None = None
    net_cash_flow: Decimal | None = None
    execution_permission: bool = False

    def __post_init__(self) -> None:
        requested = _utc(self.requested_at, "requested_at")
        if self.execution_permission:
            raise ValueError("local mock execution never grants execution permission")
        if self.status is LocalMockStatus.FILLED:
            if self.reason_codes or len(self.fills) != len(self.package.legs):
                raise ValueError("filled local mock result requires every leg and no reasons")
            if any(value is None for value in (
                self.gross_cash_flow,
                self.estimated_fees,
                self.net_cash_flow,
            )):
                raise ValueError("filled local mock result requires complete economics")
        elif (
            not self.reason_codes
            or self.fills
            or any(value is not None for value in (
                self.gross_cash_flow,
                self.estimated_fees,
                self.net_cash_flow,
            ))
        ):
            raise ValueError("unfilled local mock result requires reasons and no fills")
        object.__setattr__(self, "requested_at", requested)

    @property
    def policy_version(self) -> str:
        return self.policy.version

    @property
    def policy_sha256(self) -> str:
        return self.policy.sha256


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _request_id(
    mark: RealtimeOptionPackageMark,
    alert_id: UUID,
    requested_at: datetime,
    earnings: LocalMockEarningsAssessment,
    policy: LocalMockExecutionPolicy,
) -> UUID:
    evidence_identity = tuple(
        (row.evidence_id, row.payload_sha256)
        for row in sorted(mark.mark_evidence, key=lambda value: value.instrument_id)
    )
    digest = canonical_sha256({
        "alert_id": alert_id,
        "candidate_id": mark.package.candidate_id,
        "candidate_identity_sha256": mark.package.candidate_identity_sha256,
        "cohort_id": mark.package.cohort_id,
        "requested_at": requested_at,
        "earnings": earnings,
        "policy_sha256": policy.sha256,
        "evidence": evidence_identity,
    })
    return uuid5(NAMESPACE_URL, f"realtime-local-mock:{digest}")


def _terminal_result(
    request_id: UUID,
    alert_id: UUID,
    mark: RealtimeOptionPackageMark,
    requested_at: datetime,
    earnings: LocalMockEarningsAssessment,
    policy: LocalMockExecutionPolicy,
    status: LocalMockStatus,
    reasons: tuple[str, ...],
) -> LocalMockExecutionResult:
    return LocalMockExecutionResult(
        request_id=request_id,
        alert_id=alert_id,
        package=mark.package,
        requested_at=requested_at,
        policy=policy,
        earnings=earnings,
        evidence_batch_id=mark.evidence_batch_id,
        source_evidence_ids=tuple(
            row.evidence_id
            for row in sorted(mark.mark_evidence, key=lambda value: value.instrument_id)
        ),
        status=status,
        reason_codes=reasons,
    )


def simulate_local_mock_execution(
    mark: RealtimeOptionPackageMark,
    *,
    alert_id: UUID,
    requested_at: datetime,
    earnings: LocalMockEarningsAssessment,
    policy: LocalMockExecutionPolicy,
) -> LocalMockExecutionResult:
    if not isinstance(alert_id, UUID):
        raise TypeError("local mock alert ID is invalid")
    requested = _utc(requested_at, "requested_at")
    request_id = _request_id(mark, alert_id, requested, earnings, policy)
    if earnings.status is LocalMockEarningsStatus.UNAVAILABLE:
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.UNAVAILABLE,
            earnings.reason_codes,
        )
    if earnings.status is LocalMockEarningsStatus.BLOCKED:
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.REJECTED,
            earnings.reason_codes,
        )
    package = mark.package
    if package.provider_tier is not ProviderTier.MASSIVE_ADVANCED or any(
        not isinstance(row, RealtimeQuoteEvidence) for row in mark.mark_evidence
    ):
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.UNAVAILABLE,
            ("COHERENT_QUOTE_EVIDENCE_REQUIRED",),
        )
    if mark.computed_at > requested:
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.UNAVAILABLE,
            ("MOCK_REQUEST_PRECEDES_MARK",),
        )
    if package.exit_deadline is not None and requested > package.exit_deadline:
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.REJECTED,
            ("PACKAGE_EXIT_DEADLINE_PASSED",),
        )
    total_contracts = sum(leg.ratio for leg in package.legs)
    if package.capital_at_risk > policy.maximum_capital_at_risk:
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.REJECTED,
            ("MOCK_CAPITAL_AT_RISK_LIMIT_EXCEEDED",),
        )
    if total_contracts > policy.maximum_total_contracts:
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.REJECTED,
            ("MOCK_CONTRACT_LIMIT_EXCEEDED",),
        )
    quotes = {row.instrument_id: row for row in mark.mark_evidence}
    if len(quotes) != len(package.legs):
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.UNAVAILABLE,
            ("PACKAGE_QUOTE_SET_INCOMPLETE",),
        )
    ordered_quotes = []
    for leg in package.legs:
        quote = quotes.get(leg.instrument_id)
        if quote is None:
            return _terminal_result(
                request_id,
                alert_id,
                mark,
                requested,
                earnings,
                policy,
                LocalMockStatus.UNAVAILABLE,
                (f"PACKAGE_LEG_QUOTE_MISSING:{leg.contract_id}",),
            )
        if (
            quote.contract_id != leg.contract_id
            or quote.catalog_version_id != leg.catalog_version_id
        ):
            return _terminal_result(
                request_id,
                alert_id,
                mark,
                requested,
                earnings,
                policy,
                LocalMockStatus.UNAVAILABLE,
                (f"PACKAGE_LEG_QUOTE_IDENTITY_MISMATCH:{leg.contract_id}",),
            )
        if quote.observed_time > requested or quote.market_time > requested:
            return _terminal_result(
                request_id,
                alert_id,
                mark,
                requested,
                earnings,
                policy,
                LocalMockStatus.UNAVAILABLE,
                (f"PACKAGE_LEG_QUOTE_CLOCK_INVALID:{leg.contract_id}",),
            )
        age_ms = int((requested - quote.market_time).total_seconds() * 1000)
        if age_ms > policy.maximum_quote_age_ms:
            return _terminal_result(
                request_id,
                alert_id,
                mark,
                requested,
                earnings,
                policy,
                LocalMockStatus.UNAVAILABLE,
                (f"PACKAGE_LEG_QUOTE_STALE:{leg.contract_id}",),
            )
        if quote.bid <= 0 or quote.ask <= 0 or quote.bid > quote.ask:
            return _terminal_result(
                request_id,
                alert_id,
                mark,
                requested,
                earnings,
                policy,
                LocalMockStatus.UNAVAILABLE,
                (f"PACKAGE_LEG_QUOTE_UNUSABLE:{leg.contract_id}",),
            )
        ordered_quotes.append(quote)
    if len({(row.journal_generation, row.stream_session_id) for row in ordered_quotes}) != 1:
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.UNAVAILABLE,
            ("PACKAGE_QUOTE_LINEAGE_INCOHERENT",),
        )
    market_times = tuple(row.market_time for row in ordered_quotes)
    if int((max(market_times) - min(market_times)).total_seconds() * 1000) > policy.maximum_quote_skew_ms:
        return _terminal_result(
            request_id,
            alert_id,
            mark,
            requested,
            earnings,
            policy,
            LocalMockStatus.UNAVAILABLE,
            ("PACKAGE_QUOTE_TIME_SKEW_EXCEEDED",),
        )
    fills = []
    gross_cash_flow = Decimal("0")
    estimated_fees = Decimal("0")
    for leg, quote in zip(package.legs, ordered_quotes, strict=True):
        fill_price = (
            quote.ask + policy.slippage_per_share
            if leg.side is OptionSide.BUY
            else quote.bid - policy.slippage_per_share
        )
        if fill_price <= 0:
            return _terminal_result(
                request_id,
                alert_id,
                mark,
                requested,
                earnings,
                policy,
                LocalMockStatus.UNAVAILABLE,
                (f"PACKAGE_LEG_MOCK_FILL_UNUSABLE:{leg.contract_id}",),
            )
        fee = policy.fee_per_contract * leg.ratio
        cash_flow = fill_price * leg.ratio * leg.multiplier
        if leg.side is OptionSide.BUY:
            cash_flow = -cash_flow
        gross_cash_flow += cash_flow
        estimated_fees += fee
        fills.append(LocalMockFill(
            fill_id=uuid5(request_id, f"leg:{leg.leg_index}"),
            leg_index=leg.leg_index,
            instrument_id=leg.instrument_id,
            contract_id=leg.contract_id,
            contract_ticker=leg.contract_ticker,
            side=leg.side,
            quantity=leg.ratio,
            multiplier=leg.multiplier,
            bid=quote.bid,
            ask=quote.ask,
            fill_price=fill_price,
            fee=fee,
            source_evidence_id=quote.evidence_id,
        ))
    return LocalMockExecutionResult(
        request_id=request_id,
        alert_id=alert_id,
        package=package,
        requested_at=requested,
        policy=policy,
        earnings=earnings,
        evidence_batch_id=mark.evidence_batch_id,
        source_evidence_ids=tuple(row.evidence_id for row in ordered_quotes),
        status=LocalMockStatus.FILLED,
        reason_codes=(),
        fills=tuple(fills),
        gross_cash_flow=gross_cash_flow,
        estimated_fees=estimated_fees,
        net_cash_flow=gross_cash_flow - estimated_fees,
    )