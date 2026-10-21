from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
import re
from typing import Literal

from options.analytics.contract_filters import filter_contract
from options.domain import (
    CatalogEligibility,
    ContractType,
    OptionContractCatalogEntry,
    OptionContractSnapshot,
)

from .contracts import (
    AssetClass,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from .option_discovery_scope import RealtimeOptionDiscoveryScope
from .preflight import DetectorFamily, ProviderTier


class ContractSelectionStatus(str, Enum):
    INCLUDED = "INCLUDED"
    EXCLUDED = "EXCLUDED"


class ManifestSelectionStatus(str, Enum):
    READY = "READY"
    BLOCKED = "BLOCKED"


OPTION_ACTIVITY_SELECTION_VERSION = "option-activity-selection-v1"
OPTION_ACTIVITY_SELECTION_MAXIMUM_AGE = timedelta(minutes=5)
_OPTION_CONTRACT_PATTERN = re.compile(
    r"^O:(?P<underlyer>[A-Z][A-Z0-9.-]{0,14})"
    r"(?P<expiration>\d{6})(?P<call_put>[CP])(?P<strike>\d{8})$"
)


def _activity_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _activity_sha256(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class OptionActivityContract:
    instrument_id: int
    contract_id: int
    catalog_version_id: int
    contract_ticker: str
    underlyer: str
    call_put: Literal["C", "P"]
    expiration_date: date
    strike: Decimal
    exercise_style: str
    multiplier: Decimal
    deliverables: tuple[str, ...]
    eligibility: str
    catalog_effective_from: datetime
    catalog_effective_to: datetime | None
    catalog_observed_at: datetime
    catalog_payload_sha256: str
    market_data_time: datetime
    source_sha256: str

    def __post_init__(self) -> None:
        for name in ("instrument_id", "contract_id", "catalog_version_id"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"option activity {name} must be positive")
        ticker = self.contract_ticker.strip().upper()
        underlyer = self.underlyer.strip().upper()
        call_put = self.call_put.strip().upper()
        match = _OPTION_CONTRACT_PATTERN.fullmatch(ticker)
        if (
            match is None
            or match.group("underlyer") != underlyer
            or datetime.strptime(match.group("expiration"), "%y%m%d").date()
            != self.expiration_date
            or match.group("call_put") != call_put
            or Decimal(match.group("strike")) / Decimal("1000") != self.strike
        ):
            raise ValueError("option activity contract ticker identity is invalid")
        if (
            call_put not in {"C", "P"}
            or self.exercise_style != "AMERICAN"
            or self.multiplier != Decimal("100")
            or self.deliverables
            or self.eligibility != "VALIDATED_ACTIVE"
        ):
            raise ValueError("option activity contract is adjusted or ineligible")
        effective_from = _activity_utc(
            self.catalog_effective_from,
            "catalog_effective_from",
        )
        effective_to = (
            None
            if self.catalog_effective_to is None
            else _activity_utc(
                self.catalog_effective_to,
                "catalog_effective_to",
            )
        )
        observed_at = _activity_utc(
            self.catalog_observed_at,
            "catalog_observed_at",
        )
        market_data_time = _activity_utc(
            self.market_data_time,
            "market_data_time",
        )
        if (
            effective_to is not None
            or observed_at < effective_from
            or market_data_time < observed_at
        ):
            raise ValueError("option activity contract source clocks are invalid")
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(self, "underlyer", underlyer)
        object.__setattr__(self, "call_put", call_put)
        object.__setattr__(self, "catalog_effective_from", effective_from)
        object.__setattr__(self, "catalog_effective_to", effective_to)
        object.__setattr__(self, "catalog_observed_at", observed_at)
        object.__setattr__(self, "market_data_time", market_data_time)
        object.__setattr__(
            self,
            "catalog_payload_sha256",
            _activity_sha256(
                self.catalog_payload_sha256,
                "catalog_payload_sha256",
            ),
        )
        object.__setattr__(
            self,
            "source_sha256",
            _activity_sha256(self.source_sha256, "source_sha256"),
        )


@dataclass(frozen=True, slots=True)
class OptionActivitySelectionRecord:
    authorization_reference: str
    reviewed_at: datetime
    valid_until: datetime
    policy_sha256: str
    source_sha256: str
    quote_ceiling: int
    contracts: tuple[OptionActivityContract, ...]
    source_clocks_sha256: str = ""
    selection_sha256: str = ""

    def __post_init__(self) -> None:
        authorization_reference = self.authorization_reference.strip()
        reviewed_at = _activity_utc(self.reviewed_at, "reviewed_at")
        valid_until = _activity_utc(self.valid_until, "valid_until")
        contracts = tuple(sorted(
            self.contracts,
            key=lambda row: (row.underlyer, row.contract_ticker),
        ))
        if not authorization_reference:
            raise ValueError(
                "option activity selection requires an authorization reference"
            )
        if (
            not contracts
            or any(not isinstance(row, OptionActivityContract) for row in contracts)
        ):
            raise ValueError("option activity selection requires typed contracts")
        for attribute in (
            "instrument_id",
            "contract_id",
            "catalog_version_id",
            "contract_ticker",
            "underlyer",
        ):
            if len({getattr(row, attribute) for row in contracts}) != len(contracts):
                raise ValueError(
                    f"option activity selection contains duplicate {attribute}"
                )
        if type(self.quote_ceiling) is not int or not (
            len(contracts) <= self.quote_ceiling <= 1_000
        ):
            raise ValueError("option activity selection quote ceiling is invalid")
        latest_source = max(row.market_data_time for row in contracts)
        earliest_expiry = min(
            row.market_data_time + OPTION_ACTIVITY_SELECTION_MAXIMUM_AGE
            for row in contracts
        )
        if (
            reviewed_at < latest_source
            or not reviewed_at < valid_until <= earliest_expiry
        ):
            raise ValueError("option activity selection source clocks are invalid")
        policy_sha256 = _activity_sha256(
            self.policy_sha256,
            "policy_sha256",
        )
        source_sha256 = _activity_sha256(
            self.source_sha256,
            "source_sha256",
        )
        source_clocks_sha256 = canonical_sha256({
            "reviewed_at": reviewed_at,
            "valid_until": valid_until,
            "contract_clocks": tuple(
                (
                    row.contract_ticker,
                    row.catalog_effective_from,
                    row.catalog_observed_at,
                    row.market_data_time,
                )
                for row in contracts
            ),
        })
        selection_sha256 = canonical_sha256({
            "contract_version": OPTION_ACTIVITY_SELECTION_VERSION,
            "authorization_reference": authorization_reference,
            "reviewed_at": reviewed_at,
            "valid_until": valid_until,
            "policy_sha256": policy_sha256,
            "source_sha256": source_sha256,
            "source_clocks_sha256": source_clocks_sha256,
            "quote_ceiling": self.quote_ceiling,
            "contracts": contracts,
        })
        if self.source_clocks_sha256 and self.source_clocks_sha256 != source_clocks_sha256:
            raise ValueError("option activity selection source clocks changed")
        if self.selection_sha256 and self.selection_sha256 != selection_sha256:
            raise ValueError("option activity selection digest changed")
        object.__setattr__(
            self,
            "authorization_reference",
            authorization_reference,
        )
        object.__setattr__(self, "reviewed_at", reviewed_at)
        object.__setattr__(self, "valid_until", valid_until)
        object.__setattr__(self, "policy_sha256", policy_sha256)
        object.__setattr__(self, "source_sha256", source_sha256)
        object.__setattr__(self, "contracts", contracts)
        object.__setattr__(
            self,
            "source_clocks_sha256",
            source_clocks_sha256,
        )
        object.__setattr__(self, "selection_sha256", selection_sha256)


def validate_option_activity_selection(
    selection: OptionActivitySelectionRecord,
    *,
    required_at: datetime,
    expected_underlyings: tuple[str, ...],
    expected_quote_ceiling: int,
) -> None:
    if not isinstance(selection, OptionActivitySelectionRecord):
        raise TypeError("option activity selection record is invalid")
    required = _activity_utc(required_at, "required_at")
    underlyings = tuple(sorted({
        value.strip().upper() for value in expected_underlyings if value.strip()
    }))
    if tuple(row.underlyer for row in selection.contracts) != underlyings:
        raise ValueError("option activity selection expands or changes scope")
    if selection.quote_ceiling != expected_quote_ceiling:
        raise ValueError("option activity selection quote ceiling changed")
    if not selection.reviewed_at <= required <= selection.valid_until:
        raise ValueError("option activity selection is stale or expired")
    if any(
        row.catalog_effective_from > required
        or row.expiration_date < required.date()
        for row in selection.contracts
    ):
        raise ValueError("option activity contract is invalid for the session")


def option_activity_selection_payload(
    selection: OptionActivitySelectionRecord,
) -> dict[str, object]:
    if not isinstance(selection, OptionActivitySelectionRecord):
        raise TypeError("option activity selection record is invalid")
    return {
        "contract_version": OPTION_ACTIVITY_SELECTION_VERSION,
        "authorization_reference": selection.authorization_reference,
        "reviewed_at": selection.reviewed_at.isoformat(),
        "valid_until": selection.valid_until.isoformat(),
        "policy_sha256": selection.policy_sha256,
        "source_sha256": selection.source_sha256,
        "source_clocks_sha256": selection.source_clocks_sha256,
        "quote_ceiling": selection.quote_ceiling,
        "contracts": [
            {
                "instrument_id": row.instrument_id,
                "contract_id": row.contract_id,
                "catalog_version_id": row.catalog_version_id,
                "contract_ticker": row.contract_ticker,
                "underlyer": row.underlyer,
                "call_put": row.call_put,
                "expiration_date": row.expiration_date.isoformat(),
                "strike": str(row.strike),
                "exercise_style": row.exercise_style,
                "multiplier": str(row.multiplier),
                "deliverables": list(row.deliverables),
                "eligibility": row.eligibility,
                "catalog_effective_from": row.catalog_effective_from.isoformat(),
                "catalog_effective_to": None,
                "catalog_observed_at": row.catalog_observed_at.isoformat(),
                "catalog_payload_sha256": row.catalog_payload_sha256,
                "market_data_time": row.market_data_time.isoformat(),
                "source_sha256": row.source_sha256,
            }
            for row in selection.contracts
        ],
        "selection_sha256": selection.selection_sha256,
    }


def option_activity_selection_from_payload(
    value: object,
    *,
    required_at: datetime,
    expected_underlyings: tuple[str, ...],
    expected_quote_ceiling: int,
) -> OptionActivitySelectionRecord:
    if not isinstance(value, dict) or set(value) != {
        "contract_version",
        "authorization_reference",
        "reviewed_at",
        "valid_until",
        "policy_sha256",
        "source_sha256",
        "source_clocks_sha256",
        "quote_ceiling",
        "contracts",
        "selection_sha256",
    }:
        raise ValueError("option activity selection fields are not exact")
    if (
        value["contract_version"] != OPTION_ACTIVITY_SELECTION_VERSION
        or not isinstance(value["contracts"], list)
    ):
        raise ValueError("option activity selection contract version is invalid")
    contract_fields = {
        "instrument_id",
        "contract_id",
        "catalog_version_id",
        "contract_ticker",
        "underlyer",
        "call_put",
        "expiration_date",
        "strike",
        "exercise_style",
        "multiplier",
        "deliverables",
        "eligibility",
        "catalog_effective_from",
        "catalog_effective_to",
        "catalog_observed_at",
        "catalog_payload_sha256",
        "market_data_time",
        "source_sha256",
    }
    contracts = []
    for row in value["contracts"]:
        if (
            not isinstance(row, dict)
            or set(row) != contract_fields
            or row["catalog_effective_to"] is not None
            or not isinstance(row["deliverables"], list)
        ):
            raise ValueError("option activity contract fields are not exact")
        contracts.append(OptionActivityContract(
            instrument_id=row["instrument_id"],
            contract_id=row["contract_id"],
            catalog_version_id=row["catalog_version_id"],
            contract_ticker=row["contract_ticker"],
            underlyer=row["underlyer"],
            call_put=row["call_put"],
            expiration_date=date.fromisoformat(str(row["expiration_date"])),
            strike=Decimal(str(row["strike"])),
            exercise_style=row["exercise_style"],
            multiplier=Decimal(str(row["multiplier"])),
            deliverables=tuple(row["deliverables"]),
            eligibility=row["eligibility"],
            catalog_effective_from=datetime.fromisoformat(
                str(row["catalog_effective_from"]).replace("Z", "+00:00")
            ),
            catalog_effective_to=None,
            catalog_observed_at=datetime.fromisoformat(
                str(row["catalog_observed_at"]).replace("Z", "+00:00")
            ),
            catalog_payload_sha256=row["catalog_payload_sha256"],
            market_data_time=datetime.fromisoformat(
                str(row["market_data_time"]).replace("Z", "+00:00")
            ),
            source_sha256=row["source_sha256"],
        ))
    selection = OptionActivitySelectionRecord(
        authorization_reference=value["authorization_reference"],
        reviewed_at=datetime.fromisoformat(
            str(value["reviewed_at"]).replace("Z", "+00:00")
        ),
        valid_until=datetime.fromisoformat(
            str(value["valid_until"]).replace("Z", "+00:00")
        ),
        policy_sha256=value["policy_sha256"],
        source_sha256=value["source_sha256"],
        quote_ceiling=value["quote_ceiling"],
        contracts=tuple(contracts),
        source_clocks_sha256=value["source_clocks_sha256"],
        selection_sha256=value["selection_sha256"],
    )
    validate_option_activity_selection(
        selection,
        required_at=required_at,
        expected_underlyings=expected_underlyings,
        expected_quote_ceiling=expected_quote_ceiling,
    )
    return selection


@dataclass(frozen=True, slots=True)
class BoundedOptionManifestPolicy:
    version: str
    target_dtes: tuple[int, ...]
    maximum_target_dte_deviation: int
    strikes_per_side_per_expiry: int
    minimum_dte: int
    maximum_dte: int
    strike_corridor_fraction: Decimal
    minimum_day_volume: int
    minimum_open_interest: int
    maximum_contracts_per_underlying: int
    maximum_option_quote_contracts: int
    reserved_option_quote_contracts: int
    required_detectors: tuple[DetectorFamily, ...]
    unavailable_detectors: tuple[DetectorFamily, ...] = ()
    liquidity_rule: Literal["OR", "AND"] = "OR"

    def __post_init__(self) -> None:
        version = self.version.strip()
        targets = tuple(sorted(self.target_dtes))
        required = tuple(sorted(set(self.required_detectors), key=lambda row: row.value))
        unavailable = tuple(
            sorted(set(self.unavailable_detectors), key=lambda row: row.value)
        )
        if not version:
            raise ValueError("option manifest policy version cannot be blank")
        if (
            not targets
            or len(targets) != len(set(targets))
            or any(type(value) is not int or value < 0 for value in targets)
        ):
            raise ValueError(
                "option manifest target DTEs must be unique "
                "nonnegative integers"
            )
        if not 0 <= self.minimum_dte <= min(targets):
            raise ValueError("option manifest minimum DTE excludes a target lane")
        if not max(targets) <= self.maximum_dte <= 365:
            raise ValueError("option manifest maximum DTE excludes a target lane")
        if (
            type(self.maximum_target_dte_deviation) is not int
            or self.maximum_target_dte_deviation < 0
        ):
            raise ValueError("option manifest target DTE deviation cannot be negative")
        if (
            type(self.strikes_per_side_per_expiry) is not int
            or self.strikes_per_side_per_expiry <= 0
        ):
            raise ValueError("option manifest strike depth must be positive")
        expected_depth = (
            len(targets) * 2 * self.strikes_per_side_per_expiry
        )
        if self.maximum_contracts_per_underlying != expected_depth:
            raise ValueError(
                "option manifest underlying depth must equal expiry/side/strike allocation"
            )
        if not Decimal("0") < self.strike_corridor_fraction < Decimal("1"):
            raise ValueError("option manifest strike corridor must be in (0, 1)")
        if self.minimum_day_volume < 0 or self.minimum_open_interest < 0:
            raise ValueError("option manifest liquidity floors cannot be negative")
        if self.liquidity_rule not in {"OR", "AND"}:
            raise ValueError("option manifest liquidity rule must be OR or AND")
        if not 1 <= self.maximum_option_quote_contracts <= 1_000:
            raise ValueError("option manifest quote ceiling must be in [1, 1000]")
        if not 0 <= self.reserved_option_quote_contracts < self.maximum_option_quote_contracts:
            raise ValueError("option manifest reserved slots are outside the quote ceiling")
        if not required or set(required) & set(unavailable):
            raise ValueError("option manifest detector availability is inconsistent")
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "target_dtes", targets)
        object.__setattr__(self, "required_detectors", required)
        object.__setattr__(self, "unavailable_detectors", unavailable)

    @property
    def primary_option_quote_contracts(self) -> int:
        return self.maximum_option_quote_contracts - self.reserved_option_quote_contracts

    @property
    def sha256(self) -> str:
        payload = {
            "version": self.version,
            "target_dtes": self.target_dtes,
            "maximum_target_dte_deviation": self.maximum_target_dte_deviation,
            "strikes_per_side_per_expiry": self.strikes_per_side_per_expiry,
            "minimum_dte": self.minimum_dte,
            "maximum_dte": self.maximum_dte,
            "strike_corridor_fraction": str(self.strike_corridor_fraction),
            "minimum_day_volume": self.minimum_day_volume,
            "minimum_open_interest": self.minimum_open_interest,
            "maximum_contracts_per_underlying": self.maximum_contracts_per_underlying,
            "maximum_option_quote_contracts": self.maximum_option_quote_contracts,
            "reserved_option_quote_contracts": self.reserved_option_quote_contracts,
            "required_detectors": self.required_detectors,
            "unavailable_detectors": self.unavailable_detectors,
        }
        if self.liquidity_rule != "OR":
            payload["liquidity_rule"] = self.liquidity_rule
        return canonical_sha256(payload)


@dataclass(frozen=True, slots=True)
class OptionManifestCandidate:
    catalog_version_id: int
    catalog: OptionContractCatalogEntry
    snapshot: OptionContractSnapshot
    discovery_evidence_sha256: str

    def __post_init__(self) -> None:
        if type(self.catalog_version_id) is not int or self.catalog_version_id <= 0:
            raise ValueError("option manifest catalog version must be positive")
        if len(self.discovery_evidence_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.discovery_evidence_sha256
        ):
            raise ValueError("option manifest discovery evidence SHA-256 is invalid")
        catalog = self.catalog
        snapshot = self.snapshot
        if (
            catalog.contract_id != snapshot.contract_id
            or catalog.contract_ticker != snapshot.contract_ticker
            or catalog.underlyer != snapshot.underlyer
            or catalog.contract_type is not snapshot.contract_type
            or catalog.expiration_date != snapshot.expiration_date
            or catalog.strike != snapshot.strike
            or catalog.exercise_style is not snapshot.exercise_style
            or catalog.shares_per_contract != snapshot.shares_per_contract
        ):
            raise ValueError("option manifest catalog/snapshot identity mismatch")


@dataclass(frozen=True, slots=True)
class ContractSelectionDisposition:
    contract_id: int
    catalog_version_id: int
    contract_ticker: str
    underlyer: str
    expiration_date: date
    calendar_dte: int
    contract_type: ContractType
    strike: Decimal
    day_volume: int | None
    open_interest: int | None
    status: ContractSelectionStatus
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.status is ContractSelectionStatus.INCLUDED and self.reason_codes:
            raise ValueError("included option contract cannot have exclusion reasons")
        if self.status is ContractSelectionStatus.EXCLUDED and not self.reason_codes:
            raise ValueError("excluded option contract requires reasons")


@dataclass(frozen=True, slots=True)
class OptionManifestCoverage:
    underlyer: str
    target_dte: int
    contract_type: ContractType
    expiration_date: date | None
    selected_count: int
    required_count: int
    status: str
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SelectedContractManifestPlan:
    scope: RealtimeOptionDiscoveryScope
    policy: BoundedOptionManifestPolicy
    session_date: date
    status: ManifestSelectionStatus
    manifest: SubscriptionManifest | None
    dispositions: tuple[ContractSelectionDisposition, ...]
    coverage: tuple[OptionManifestCoverage, ...]
    source_evidence_sha256: str
    selection_sha256: str
    reason_codes: tuple[str, ...]

    @property
    def selected_contract_count(self) -> int:
        return sum(
            row.status is ContractSelectionStatus.INCLUDED
            for row in self.dispositions
        )

    @property
    def policy_admitted_contract_ids(self) -> frozenset[int]:
        return frozenset(row.contract_id for row in self.dispositions
            if row.status is ContractSelectionStatus.INCLUDED
            or set(row.reason_codes).issubset({"SIDE_DEPTH_LIMIT", "EXPIRY_NOT_SELECTED"}))


def _candidate_order(candidate: OptionManifestCandidate) -> tuple:
    snapshot = candidate.snapshot
    volume = snapshot.day_volume if snapshot.day_volume is not None else -1
    open_interest = snapshot.open_interest if snapshot.open_interest is not None else -1
    return (
        abs(snapshot.strike - snapshot.spot),
        -volume,
        -open_interest,
        snapshot.strike,
        snapshot.contract_ticker,
    )


def _disposition(
    candidate: OptionManifestCandidate,
    status: ContractSelectionStatus,
    reasons: tuple[str, ...] = (),
) -> ContractSelectionDisposition:
    row = candidate.snapshot
    return ContractSelectionDisposition(
        contract_id=row.contract_id,
        catalog_version_id=candidate.catalog_version_id,
        contract_ticker=row.contract_ticker,
        underlyer=row.underlyer,
        expiration_date=row.expiration_date,
        calendar_dte=row.calendar_dte,
        contract_type=row.contract_type,
        strike=row.strike,
        day_volume=row.day_volume,
        open_interest=row.open_interest,
        status=status,
        reason_codes=reasons,
    )


def _source_evidence_sha256(
    candidates: tuple[OptionManifestCandidate, ...],
) -> str:
    return canonical_sha256(tuple(
        (
            row.catalog_version_id,
            row.catalog.contract_id,
            row.catalog.payload_sha256,
            row.snapshot.snapshot_id,
            row.snapshot.normalized_payload_sha256,
            row.discovery_evidence_sha256,
        )
        for row in candidates
    ))


def _disposition_payload(
    dispositions: tuple[ContractSelectionDisposition, ...],
) -> tuple[dict[str, object], ...]:
    return tuple({
        "contract_id": row.contract_id,
        "catalog_version_id": row.catalog_version_id,
        "contract_ticker": row.contract_ticker,
        "underlyer": row.underlyer,
        "expiration_date": row.expiration_date,
        "calendar_dte": row.calendar_dte,
        "contract_type": row.contract_type,
        "strike": str(row.strike),
        "day_volume": row.day_volume,
        "open_interest": row.open_interest,
        "status": row.status,
        "reason_codes": row.reason_codes,
    } for row in dispositions)


def _blocked_plan(
    scope: RealtimeOptionDiscoveryScope,
    policy: BoundedOptionManifestPolicy,
    session_date: date,
    candidates: tuple[OptionManifestCandidate, ...],
    dispositions: tuple[ContractSelectionDisposition, ...],
    coverage: tuple[OptionManifestCoverage, ...],
    *reasons: str,
) -> SelectedContractManifestPlan:
    source_sha256 = _source_evidence_sha256(candidates)
    reason_codes = tuple(dict.fromkeys(reasons))
    selection_sha256 = canonical_sha256({
        "scope": scope,
        "policy_sha256": policy.sha256,
        "session_date": session_date,
        "source_evidence_sha256": source_sha256,
        "dispositions": _disposition_payload(dispositions),
        "coverage": coverage,
        "status": ManifestSelectionStatus.BLOCKED,
        "reason_codes": reason_codes,
    })
    return SelectedContractManifestPlan(
        scope,
        policy,
        session_date,
        ManifestSelectionStatus.BLOCKED,
        None,
        dispositions,
        coverage,
        source_sha256,
        selection_sha256,
        reason_codes,
    )


def select_bounded_manifest_expiration(expiration_depths, policy, target_dte, selected_expirations):
    choices = []
    for expiration, (dte, calls, puts) in expiration_depths.items():
        if expiration in selected_expirations or min(calls, puts) < policy.strikes_per_side_per_expiry:
            continue
        deviation = abs(dte - target_dte)
        if deviation <= policy.maximum_target_dte_deviation:
            choices.append((deviation, dte, expiration))
    return min(choices)[2] if choices else None


def build_bounded_option_session_manifest(
    scope: RealtimeOptionDiscoveryScope,
    policy: BoundedOptionManifestPolicy,
    candidates: tuple[OptionManifestCandidate, ...],
    *,
    session_date: date,
) -> SelectedContractManifestPlan:
    ordered = tuple(sorted(
        candidates,
        key=lambda row: (
            row.snapshot.underlyer,
            row.snapshot.expiration_date,
            row.snapshot.contract_type.value,
            row.snapshot.strike,
            row.snapshot.contract_ticker,
        ),
    ))
    if len({row.catalog.contract_id for row in ordered}) != len(ordered):
        raise ValueError("option manifest candidates contain duplicate contracts")
    if any(
        row.catalog.underlyer not in scope.universe.option_underlyings
        for row in ordered
    ):
        raise ValueError("option manifest candidate is outside the reviewed universe")
    if (
        scope.provider_tier is ProviderTier.MASSIVE_DEVELOPER
        and (
            DetectorFamily.O2 not in policy.unavailable_detectors
            or not {
                DetectorFamily.S1,
                DetectorFamily.S2,
                DetectorFamily.O1,
                DetectorFamily.O3,
            }.issubset(policy.required_detectors)
        )
    ):
        raise ValueError("Developer manifest must preserve S1/S2/O1/O3 and disable O2")
    if (
        scope.provider_tier is ProviderTier.MASSIVE_ADVANCED
        and DetectorFamily.O2 in policy.unavailable_detectors
    ):
        raise ValueError("Advanced manifest cannot disable O2 by tier policy")
    required_primary = (
        len(scope.universe.option_underlyings)
        * policy.maximum_contracts_per_underlying
    )
    if required_primary > policy.primary_option_quote_contracts:
        dispositions = tuple(
            _disposition(
                row,
                ContractSelectionStatus.EXCLUDED,
                ("PRIMARY_SLOT_BUDGET_EXCEEDED",),
            )
            for row in ordered
        )
        return _blocked_plan(
            scope,
            policy,
            session_date,
            ordered,
            dispositions,
            (),
            "PRIMARY_SLOT_BUDGET_EXCEEDED",
        )

    eligible: dict[int, OptionManifestCandidate] = {}
    exclusion_reasons: dict[int, tuple[str, ...]] = {}
    for row in ordered:
        catalog = row.catalog
        snapshot = row.snapshot
        reasons = []
        if catalog.eligibility_status is not CatalogEligibility.VALIDATED_ACTIVE:
            reasons.extend(catalog.exclusion_reasons or ("CATALOG_NOT_ELIGIBLE",))
        else:
            filtered = filter_contract(
                contract_type=catalog.contract_type,
                exercise_style=catalog.exercise_style,
                shares_per_contract=catalog.shares_per_contract,
                has_additional_deliverables=False,
                expiration_date=catalog.expiration_date,
                expiration_cutoff=snapshot.expiration_cutoff,
                market_time=snapshot.market_data_time,
                strike=catalog.strike,
                spot=snapshot.spot,
                day_volume=snapshot.day_volume,
                open_interest=snapshot.open_interest,
                minimum_dte=policy.minimum_dte,
                maximum_dte=policy.maximum_dte,
                strike_corridor_fraction=policy.strike_corridor_fraction,
                minimum_day_volume=policy.minimum_day_volume,
                minimum_open_interest=policy.minimum_open_interest,
                liquidity_rule=policy.liquidity_rule,
            )
            reasons.extend(reason.value for reason in filtered.reasons)
            if filtered.calendar_dte != snapshot.calendar_dte:
                reasons.append("DTE_IDENTITY_MISMATCH")
        if reasons:
            exclusion_reasons[catalog.contract_id] = tuple(dict.fromkeys(reasons))
        else:
            eligible[catalog.contract_id] = row

    selected: dict[int, OptionManifestCandidate] = {}
    selected_expirations: dict[str, set[date]] = {
        ticker: set() for ticker in scope.universe.option_underlyings
    }
    coverage = []
    for underlyer in scope.universe.option_underlyings:
        underlying_rows = tuple(
            row for row in eligible.values() if row.snapshot.underlyer == underlyer
        )
        by_expiration: dict[date, tuple[OptionManifestCandidate, ...]] = {}
        for expiration in sorted({row.snapshot.expiration_date for row in underlying_rows}):
            by_expiration[expiration] = tuple(
                row for row in underlying_rows if row.snapshot.expiration_date == expiration
            )
        expiration_depths = {expiration: (rows[0].snapshot.calendar_dte,
            sum(row.snapshot.contract_type is ContractType.CALL for row in rows),
            sum(row.snapshot.contract_type is ContractType.PUT for row in rows))
            for expiration, rows in by_expiration.items()}
        for target_dte in policy.target_dtes:
            expiration = select_bounded_manifest_expiration(expiration_depths, policy,
                target_dte, selected_expirations[underlyer])
            if expiration is None:
                for side in (ContractType.CALL, ContractType.PUT):
                    coverage.append(OptionManifestCoverage(
                        underlyer,
                        target_dte,
                        side,
                        None,
                        0,
                        policy.strikes_per_side_per_expiry,
                        "UNAVAILABLE",
                        ("DTE_SIDE_DEPTH_UNAVAILABLE",),
                    ))
                continue
            rows = by_expiration[expiration]
            selected_expirations[underlyer].add(expiration)
            for side in (ContractType.CALL, ContractType.PUT):
                side_rows = tuple(sorted(
                    (row for row in rows if row.snapshot.contract_type is side),
                    key=_candidate_order,
                ))
                admitted = side_rows[: policy.strikes_per_side_per_expiry]
                selected.update((row.catalog.contract_id, row) for row in admitted)
                coverage.append(OptionManifestCoverage(
                    underlyer,
                    target_dte,
                    side,
                    expiration,
                    len(admitted),
                    policy.strikes_per_side_per_expiry,
                    "READY",
                    (),
                ))

    coverage_rows = tuple(sorted(
        coverage,
        key=lambda row: (row.underlyer, row.target_dte, row.contract_type.value),
    ))
    coverage_failed = any(row.status != "READY" for row in coverage_rows)
    dispositions = []
    for row in ordered:
        contract_id = row.catalog.contract_id
        if contract_id in selected:
            dispositions.append(_disposition(row, ContractSelectionStatus.INCLUDED))
        elif contract_id in exclusion_reasons:
            dispositions.append(_disposition(
                row,
                ContractSelectionStatus.EXCLUDED,
                exclusion_reasons[contract_id],
            ))
        else:
            reason = (
                "SIDE_DEPTH_LIMIT"
                if row.snapshot.expiration_date
                in selected_expirations[row.snapshot.underlyer]
                else "EXPIRY_NOT_SELECTED"
            )
            dispositions.append(_disposition(
                row,
                ContractSelectionStatus.EXCLUDED,
                (reason,),
            ))
    disposition_rows = tuple(dispositions)
    if coverage_failed:
        return _blocked_plan(
            scope,
            policy,
            session_date,
            ordered,
            disposition_rows,
            coverage_rows,
            "REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE",
        )

    option_members = tuple(
        SubscriptionMember(
            AssetClass.OPTION,
            row.catalog.contract_ticker,
            row.catalog.underlyer,
            channel,
            catalog_version_id=row.catalog_version_id,
        )
        for row in sorted(selected.values(), key=lambda candidate: (
            candidate.catalog.underlyer,
            candidate.catalog.expiration_date,
            candidate.catalog.contract_type.value,
            candidate.catalog.strike,
        ))
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    equity_members = tuple(
        SubscriptionMember(AssetClass.EQUITY, ticker, ticker, channel)
        for ticker in scope.universe.equity_tickers
        for channel in (
            MarketChannel.TRADE,
            MarketChannel.QUOTE,
            MarketChannel.MINUTE_AGGREGATE,
        )
    )
    manifest = SubscriptionManifest(
        version=f"bounded-option-session:{policy.version}",
        session_date=session_date,
        universe=scope.universe,
        capability_profile_sha256=scope.profile_sha256,
        allocation_policy_sha256=policy.sha256,
        members=equity_members + option_members,
        maximum_option_quote_contracts=policy.maximum_option_quote_contracts,
    )
    source_sha256 = _source_evidence_sha256(ordered)
    selection_sha256 = canonical_sha256({
        "scope": scope,
        "policy_sha256": policy.sha256,
        "session_date": session_date,
        "source_evidence_sha256": source_sha256,
        "dispositions": _disposition_payload(disposition_rows),
        "coverage": coverage_rows,
        "manifest_sha256": manifest.sha256,
        "status": ManifestSelectionStatus.READY,
    })
    return SelectedContractManifestPlan(
        scope,
        policy,
        session_date,
        ManifestSelectionStatus.READY,
        manifest,
        disposition_rows,
        coverage_rows,
        source_sha256,
        selection_sha256,
        (),
    )