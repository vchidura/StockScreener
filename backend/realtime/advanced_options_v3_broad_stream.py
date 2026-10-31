"""Provider-free planning for a bounded broad Options WebSocket stream."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from .advanced_options_p7_selection import (
    P7TransportCandidate,
    p7_transport_candidate_payload,
)
from .contracts import (
    AssetClass,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)


UTC = timezone.utc


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class BroadOptionsStreamPolicy:
    version: str = "advanced-options-v3-broad-stream-v1"
    underlyings: tuple[str, ...] = ("QQQ", "SPY")
    minimum_dte: int = 0
    maximum_dte: int = 60
    maximum_moneyness_fraction: Decimal = Decimal("0.05")
    maximum_snapshot_age_seconds: int = 300
    maximum_option_quote_contracts: int = 1_000

    def __post_init__(self) -> None:
        version = self.version.strip()
        underlyings = tuple(sorted({
            value.strip().upper()
            for value in self.underlyings
            if value.strip()
        }))
        if (
            not version
            or underlyings != ("QQQ", "SPY")
            or type(self.minimum_dte) is not int
            or type(self.maximum_dte) is not int
            or not 0 <= self.minimum_dte <= self.maximum_dte <= 60
            or not Decimal("0")
            < self.maximum_moneyness_fraction
            <= Decimal("0.10")
            or type(self.maximum_snapshot_age_seconds) is not int
            or self.maximum_snapshot_age_seconds <= 0
            or type(self.maximum_option_quote_contracts) is not int
            or not 1 <= self.maximum_option_quote_contracts <= 1_000
        ):
            raise ValueError("broad Options stream policy is invalid")
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "underlyings", underlyings)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class BroadOptionsStreamPlan:
    authorization_reference: str
    session_date: date
    reviewed_at: datetime
    valid_until: datetime
    policy: BroadOptionsStreamPolicy
    contracts: tuple[P7TransportCandidate, ...]
    manifest: SubscriptionManifest
    source_sha256: str
    plan_sha256: str


def build_broad_options_stream_plan(
    candidates: tuple[P7TransportCandidate, ...],
    *,
    session_date: date,
    reviewed_at: datetime,
    capability_profile_sha256: str,
    authorization_reference: str,
    policy: BroadOptionsStreamPolicy | None = None,
) -> BroadOptionsStreamPlan:
    selected_policy = policy or BroadOptionsStreamPolicy()
    reviewed = _utc(reviewed_at, "reviewed_at")
    authorization = authorization_reference.strip()
    ordered = tuple(sorted(
        candidates,
        key=lambda row: (
            row.underlyer,
            row.expiration_date,
            row.option_type,
            row.strike,
            row.contract_ticker,
        ),
    ))
    if (
        not authorization
        or reviewed.date() != session_date
        or not ordered
    ):
        raise ValueError("broad Options stream scope is incomplete")
    tickers = tuple(row.contract_ticker for row in ordered)
    if len(tickers) != len(set(tickers)):
        raise ValueError("broad Options stream contains duplicate contracts")
    if len(ordered) > selected_policy.maximum_option_quote_contracts:
        raise ValueError("broad Options stream exceeds the 1000-contract quote ceiling")

    maximum_age = timedelta(
        seconds=selected_policy.maximum_snapshot_age_seconds,
    )
    for row in ordered:
        expected_dte = (row.expiration_date - session_date).days
        if (
            row.underlyer not in selected_policy.underlyings
            or row.calendar_dte != expected_dte
            or not selected_policy.minimum_dte
            <= row.calendar_dte
            <= selected_policy.maximum_dte
            or row.moneyness_fraction
            > selected_policy.maximum_moneyness_fraction
            or row.market_data_time > reviewed
            or reviewed - row.market_data_time > maximum_age
        ):
            raise ValueError(
                f"broad Options stream candidate is outside policy: "
                f"{row.contract_ticker}"
            )
    for underlyer in selected_policy.underlyings:
        option_types = {
            row.option_type
            for row in ordered
            if row.underlyer == underlyer
        }
        if option_types != {"C", "P"}:
            raise ValueError(
                f"broad Options stream requires call/put coverage for {underlyer}"
            )

    universe = UniverseManifest(
        version=selected_policy.version,
        equity_tickers=selected_policy.underlyings,
        option_underlyings=selected_policy.underlyings,
    )
    members = tuple(
        SubscriptionMember(
            AssetClass.OPTION,
            row.contract_ticker,
            row.underlyer,
            channel,
            catalog_version_id=index,
        )
        for index, row in enumerate(ordered, start=1)
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    manifest = SubscriptionManifest(
        version=selected_policy.version,
        session_date=session_date,
        universe=universe,
        capability_profile_sha256=capability_profile_sha256,
        allocation_policy_sha256=selected_policy.sha256,
        members=members,
        maximum_option_quote_contracts=len(ordered),
    )
    source_sha256 = canonical_sha256(tuple(
        p7_transport_candidate_payload(row)
        for row in ordered
    ))
    valid_until = min(
        row.market_data_time + maximum_age
        for row in ordered
    )
    plan_sha256 = canonical_sha256({
        "authorization_reference": authorization,
        "session_date": session_date,
        "reviewed_at": reviewed,
        "valid_until": valid_until,
        "policy_sha256": selected_policy.sha256,
        "source_sha256": source_sha256,
        "manifest_sha256": manifest.sha256,
        "contracts": tickers,
    })
    return BroadOptionsStreamPlan(
        authorization,
        session_date,
        reviewed,
        valid_until,
        selected_policy,
        ordered,
        manifest,
        source_sha256,
        plan_sha256,
    )


def broad_options_stream_plan_payload(
    plan: BroadOptionsStreamPlan,
) -> dict[str, object]:
    return {
        "schema": "advanced-options-v3-broad-stream-plan-v1",
        "authorization_reference": plan.authorization_reference,
        "session_date": plan.session_date.isoformat(),
        "reviewed_at_utc": plan.reviewed_at.isoformat(),
        "valid_until_utc": plan.valid_until.isoformat(),
        "policy": {
            "version": plan.policy.version,
            "underlyings": list(plan.policy.underlyings),
            "minimum_dte": plan.policy.minimum_dte,
            "maximum_dte": plan.policy.maximum_dte,
            "maximum_moneyness_fraction": str(
                plan.policy.maximum_moneyness_fraction
            ),
            "maximum_snapshot_age_seconds": (
                plan.policy.maximum_snapshot_age_seconds
            ),
            "maximum_option_quote_contracts": (
                plan.policy.maximum_option_quote_contracts
            ),
        },
        "policy_sha256": plan.policy.sha256,
        "source_sha256": plan.source_sha256,
        "manifest_sha256": plan.manifest.sha256,
        "plan_sha256": plan.plan_sha256,
        "contract_count": len(plan.contracts),
        "subscription_count": len(plan.manifest.members),
        "contracts": [
            p7_transport_candidate_payload(row)
            for row in plan.contracts
        ],
        "subscriptions": [
            {
                "asset_class": row.asset_class.value,
                "provider_symbol": row.provider_symbol,
                "underlying": row.underlying,
                "channel": row.channel.value,
                "catalog_version_id": row.catalog_version_id,
            }
            for row in plan.manifest.members
        ],
    }
