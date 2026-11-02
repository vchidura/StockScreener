"""Prospective Options opening admission and split V3 authorities."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import re
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json

from .advanced_options_control_plane import (
    ADVANCED_OPTIONS_ACCOUNT_ALIAS,
    ADVANCED_OPTIONS_REST_LANES,
    AdvancedOptionsSocketBudget,
)
from .advanced_options_reference_state import (
    AdvancedOptionsReferenceState,
    ReferenceAvailability,
)
from .contracts import MarketChannel, canonical_sha256
from .option_pre_session_producer import OptionPreSessionProducerResult
from .option_session_manifest import (
    ContractSelectionStatus,
    ManifestSelectionStatus,
)
from .option_slot_set import SlotSetKind, SlotSetPurpose, SlotSetVersion
from .provider_budget import ProviderBudgetScope


PROSPECTIVE_OPENING_ADMISSION_VERSION = (
    "advanced-options-prospective-opening-admission-v1"
)
SESSION_ACQUISITION_AUTHORIZATION_VERSION = (
    "advanced-options-session-acquisition-authorization-v1"
)
WINDOW_CYCLE_AUTHORIZATION_VERSION = (
    "advanced-options-window-cycle-authorization-v1"
)
AUTHORITY_RECEIPT_VERSION = "advanced-options-authority-receipt-v1"
AUTHORITY_BUNDLE_VERSION = "advanced-options-authority-bundle-v1"
PROSPECTIVE_OPENING_UNDERLYINGS = ("QQQ", "SPY")
PROSPECTIVE_OPENING_CHANNELS = (
    MarketChannel.QUOTE,
    MarketChannel.TRADE,
)
_OPTION_UNDERLYING = re.compile(r"^O:([A-Z]{1,6})")


class OpeningAdmissionStatus(str, Enum):
    READY = "READY"
    UNAVAILABLE = "UNAVAILABLE"


class AdvancedOptionsAuthorityKind(str, Enum):
    SESSION_ACQUISITION = "SESSION_ACQUISITION"
    WINDOW_CYCLE = "WINDOW_CYCLE"


SESSION_EFFECT_KINDS = frozenset({
    "PROVIDER_REQUEST",
    "PROVIDER_SOCKET",
})
WINDOW_EFFECT_KINDS = frozenset({
    "TARGET_WINDOW_STAGE",
    "TARGET_WINDOW_WRITE",
})


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
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


def _text(value: str, name: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{name} must be 1 to {maximum} characters")
    return normalized


def _option_membership(ticker: str) -> tuple[str, str]:
    value = ticker.strip().upper()
    match = _OPTION_UNDERLYING.match(value)
    side = value[-9:-8]
    if match is None or side not in {"C", "P"}:
        raise ValueError("option contract ticker is invalid")
    return match.group(1), side


@dataclass(frozen=True, slots=True)
class ProspectiveOpeningAdmissionPolicy:
    version: str = PROSPECTIVE_OPENING_ADMISSION_VERSION
    underlyings: tuple[str, ...] = PROSPECTIVE_OPENING_UNDERLYINGS
    contracts_per_underlying: int = 1
    channels: tuple[MarketChannel, ...] = PROSPECTIVE_OPENING_CHANNELS
    ranking: tuple[str, ...] = (
        "ABSOLUTE_MONEYNESS",
        "OPEN_INTEREST_DESC",
        "DAY_VOLUME_DESC",
        "EXPIRATION_ASC",
        "CONTRACT_TYPE_ASC",
        "CONTRACT_TICKER_ASC",
    )

    def __post_init__(self) -> None:
        underlyings = tuple(
            _text(value, "prospective underlying", maximum=16).upper()
            for value in self.underlyings
        )
        if self.version != PROSPECTIVE_OPENING_ADMISSION_VERSION:
            raise ValueError("prospective opening admission version changed")
        if (
            not underlyings
            or len(underlyings) != len(set(underlyings))
            or tuple(sorted(underlyings)) != underlyings
        ):
            raise ValueError(
                "prospective opening underlyings are invalid"
            )
        if (
            type(self.contracts_per_underlying) is not int
            or self.contracts_per_underlying <= 0
        ):
            raise ValueError(
                "prospective opening contract count is invalid"
            )
        if self.channels != PROSPECTIVE_OPENING_CHANNELS:
            raise ValueError("prospective opening admission requires exact T/Q channels")
        object.__setattr__(self, "underlyings", underlyings)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


PROSPECTIVE_OPENING_ADMISSION_POLICY = ProspectiveOpeningAdmissionPolicy()


@dataclass(frozen=True, slots=True)
class OpeningOpenInterestEvidence:
    contract_id: int
    contract_ticker: str
    underlyer: str
    open_interest: int
    known_at: datetime
    member_sha256: str

    def __post_init__(self) -> None:
        ticker = _text(self.contract_ticker, "contract_ticker", maximum=80).upper()
        underlyer = _text(self.underlyer, "underlyer", maximum=16).upper()
        known = _utc(self.known_at, "known_at")
        if (
            type(self.contract_id) is not int
            or self.contract_id <= 0
            or not ticker.startswith(f"O:{underlyer}")
            or type(self.open_interest) is not int
            or self.open_interest < 0
        ):
            raise ValueError("opening OI member identity is invalid")
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(self, "underlyer", underlyer)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(
            self,
            "member_sha256",
            _sha256(self.member_sha256, "member_sha256"),
        )


@dataclass(frozen=True, slots=True)
class OpeningReferenceEvidence:
    state_id: UUID
    state_sha256: str
    reference_generation: int
    session_date: date
    known_at: datetime
    valid_until: datetime
    reference_policy_sha256: str
    contract_scope_sha256: str
    open_interest_sha256: str
    open_interest: tuple[OpeningOpenInterestEvidence, ...]

    def __post_init__(self) -> None:
        known = _utc(self.known_at, "known_at")
        valid = _utc(self.valid_until, "valid_until")
        members = tuple(sorted(self.open_interest, key=lambda row: row.contract_id))
        if (
            not isinstance(self.state_id, UUID)
            or self.state_id.int == 0
            or type(self.reference_generation) is not int
            or self.reference_generation <= 0
            or not isinstance(self.session_date, date)
            or known >= valid
            or len({row.contract_id for row in members}) != len(members)
            or any(row.known_at > known for row in members)
        ):
            raise ValueError("opening reference identity is invalid")
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "valid_until", valid)
        object.__setattr__(self, "open_interest", members)
        for name in (
            "state_sha256",
            "reference_policy_sha256",
            "contract_scope_sha256",
            "open_interest_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))

    @classmethod
    def from_reference_state(
        cls,
        state: AdvancedOptionsReferenceState,
    ) -> OpeningReferenceEvidence:
        if not isinstance(state, AdvancedOptionsReferenceState):
            raise TypeError("opening reference requires a typed reference state")
        available = tuple(
            OpeningOpenInterestEvidence(
                contract_id=row.contract_id,
                contract_ticker=row.contract_ticker,
                underlyer=row.underlyer,
                open_interest=row.open_interest,
                known_at=row.known_at,
                member_sha256=row.sha256,
            )
            for row in state.open_interest
            if row.availability is ReferenceAvailability.AVAILABLE
            and row.open_interest is not None
        )
        return cls(
            state_id=state.state_id,
            state_sha256=state.state_sha256,
            reference_generation=state.reference_generation,
            session_date=state.session_date,
            known_at=state.known_at,
            valid_until=state.valid_until,
            reference_policy_sha256=state.reference_policy_sha256,
            contract_scope_sha256=state.contract_scope_sha256,
            open_interest_sha256=state.open_interest_sha256,
            open_interest=available,
        )

    @property
    def sha256(self) -> str:
        return canonical_sha256(opening_reference_evidence_payload(self))


@dataclass(frozen=True, slots=True)
class ProspectiveOpeningContract:
    contract_id: int
    catalog_version_id: int
    contract_ticker: str
    underlyer: str
    expiration_date: date
    contract_type: str
    strike: Decimal
    spot: Decimal
    absolute_moneyness: Decimal
    day_volume: int
    open_interest: int | None
    catalog_payload_sha256: str
    snapshot_payload_sha256: str
    open_interest_member_sha256: str | None

    def __post_init__(self) -> None:
        ticker = _text(self.contract_ticker, "contract_ticker", maximum=80).upper()
        underlyer = _text(self.underlyer, "underlyer", maximum=16).upper()
        if (
            type(self.contract_id) is not int
            or self.contract_id <= 0
            or type(self.catalog_version_id) is not int
            or self.catalog_version_id <= 0
            or not ticker.startswith(f"O:{underlyer}")
            or self.contract_type not in {"CALL", "PUT"}
            or self.strike <= 0
            or self.spot <= 0
            or self.absolute_moneyness < 0
            or type(self.day_volume) is not int
            or self.day_volume < 0
            or (
                self.open_interest is not None
                and (
                    type(self.open_interest) is not int
                    or self.open_interest < 0
                )
            )
            or (
                self.open_interest is None
                and self.open_interest_member_sha256 is not None
            )
            or (
                self.open_interest is not None
                and self.open_interest_member_sha256 is None
            )
        ):
            raise ValueError("prospective opening contract identity is invalid")
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(self, "underlyer", underlyer)
        for name in (
            "catalog_payload_sha256",
            "snapshot_payload_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        if self.open_interest_member_sha256 is not None:
            object.__setattr__(
                self,
                "open_interest_member_sha256",
                _sha256(
                    self.open_interest_member_sha256,
                    "open_interest_member_sha256",
                ),
            )


@dataclass(frozen=True, slots=True)
class ProspectiveOpeningAdmission:
    admission_id: UUID
    admission_sha256: str
    status: OpeningAdmissionStatus
    session_date: date
    prepared_at: datetime
    capture_boundary: datetime
    policy_sha256: str
    pre_session_selection_id: UUID | None
    pre_session_selection_sha256: str | None
    source_evidence_sha256: str | None
    reference_evidence_sha256: str | None
    contracts: tuple[ProspectiveOpeningContract, ...]
    reason_codes: tuple[str, ...]
    opening_capture_ready: bool
    opening_coverage_claimed: bool = False
    accumulation_900_second_coverage_claimed: bool = False

    def __post_init__(self) -> None:
        prepared = _utc(self.prepared_at, "prepared_at")
        boundary = _utc(self.capture_boundary, "capture_boundary")
        contracts = tuple(sorted(
            self.contracts,
            key=lambda row: (
                row.underlyer,
                row.contract_type,
                row.expiration_date,
                row.strike,
                row.contract_ticker,
            ),
        ))
        reasons = tuple(dict.fromkeys(
            _text(value, "reason_code", maximum=96).upper()
            for value in self.reason_codes
        ))
        if (
            not isinstance(self.admission_id, UUID)
            or self.admission_id.int == 0
            or not isinstance(self.status, OpeningAdmissionStatus)
            or not isinstance(self.session_date, date)
            or prepared.date() != self.session_date
            or boundary.date() != self.session_date
            or prepared >= boundary
            or self.opening_coverage_claimed
            or self.accumulation_900_second_coverage_claimed
        ):
            raise ValueError("prospective opening admission clocks or claims are invalid")
        object.__setattr__(self, "prepared_at", prepared)
        object.__setattr__(self, "capture_boundary", boundary)
        object.__setattr__(self, "contracts", contracts)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(
            self,
            "policy_sha256",
            _sha256(self.policy_sha256, "policy_sha256"),
        )
        for name in (
            "pre_session_selection_sha256",
            "source_evidence_sha256",
            "reference_evidence_sha256",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _sha256(value, name))
        if self.status is OpeningAdmissionStatus.READY:
            if (
                not self.opening_capture_ready
                or reasons
                or self.pre_session_selection_id is None
                or self.pre_session_selection_sha256 is None
                or self.source_evidence_sha256 is None
                or self.reference_evidence_sha256 is None
                or not contracts
                or len({
                    row.contract_ticker for row in contracts
                }) != len(contracts)
            ):
                raise ValueError("ready opening admission is incomplete")
        elif self.opening_capture_ready or contracts or not reasons:
            raise ValueError("unavailable opening admission must be explicit")
        expected_sha256 = canonical_sha256(
            prospective_opening_admission_payload(self, include_identity=False)
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:prospective-opening-admission:{expected_sha256}",
        )
        if self.admission_sha256 != expected_sha256 or self.admission_id != expected_id:
            raise ValueError("opening admission ID or SHA does not match its inputs")


def opening_reference_evidence_payload(
    evidence: OpeningReferenceEvidence,
) -> dict[str, object]:
    return {
        "state_id": str(evidence.state_id),
        "state_sha256": evidence.state_sha256,
        "reference_generation": evidence.reference_generation,
        "session_date": evidence.session_date.isoformat(),
        "known_at": evidence.known_at.isoformat(),
        "valid_until": evidence.valid_until.isoformat(),
        "reference_policy_sha256": evidence.reference_policy_sha256,
        "contract_scope_sha256": evidence.contract_scope_sha256,
        "open_interest_sha256": evidence.open_interest_sha256,
        "open_interest": [
            {
                "contract_id": row.contract_id,
                "contract_ticker": row.contract_ticker,
                "underlyer": row.underlyer,
                "open_interest": row.open_interest,
                "known_at": row.known_at.isoformat(),
                "member_sha256": row.member_sha256,
            }
            for row in evidence.open_interest
        ],
    }


def prospective_opening_admission_payload(
    admission: ProspectiveOpeningAdmission,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": PROSPECTIVE_OPENING_ADMISSION_VERSION,
        "status": admission.status.value,
        "session_date": admission.session_date.isoformat(),
        "prepared_at": admission.prepared_at.isoformat(),
        "capture_boundary": admission.capture_boundary.isoformat(),
        "policy_sha256": admission.policy_sha256,
        "pre_session_selection_id": (
            None
            if admission.pre_session_selection_id is None
            else str(admission.pre_session_selection_id)
        ),
        "pre_session_selection_sha256": (
            admission.pre_session_selection_sha256
        ),
        "source_evidence_sha256": admission.source_evidence_sha256,
        "reference_evidence_sha256": admission.reference_evidence_sha256,
        "contracts": [
            {
                "contract_id": row.contract_id,
                "catalog_version_id": row.catalog_version_id,
                "contract_ticker": row.contract_ticker,
                "underlyer": row.underlyer,
                "expiration_date": row.expiration_date.isoformat(),
                "contract_type": row.contract_type,
                "strike": str(row.strike),
                "spot": str(row.spot),
                "absolute_moneyness": str(row.absolute_moneyness),
                "day_volume": row.day_volume,
                "open_interest": row.open_interest,
                "catalog_payload_sha256": row.catalog_payload_sha256,
                "snapshot_payload_sha256": row.snapshot_payload_sha256,
                "open_interest_member_sha256": (
                    row.open_interest_member_sha256
                ),
            }
            for row in admission.contracts
        ],
        "reason_codes": list(admission.reason_codes),
        "opening_capture_ready": admission.opening_capture_ready,
        "opening_coverage_claimed": admission.opening_coverage_claimed,
        "accumulation_900_second_coverage_claimed": (
            admission.accumulation_900_second_coverage_claimed
        ),
    }
    if include_identity:
        value.update({
            "admission_id": str(admission.admission_id),
            "admission_sha256": admission.admission_sha256,
        })
    return value


def prospective_opening_admission_from_payload(
    value: object,
) -> ProspectiveOpeningAdmission:
    if not isinstance(value, Mapping):
        raise TypeError("prospective opening admission payload is invalid")
    contracts_value = value.get("contracts")
    if (
        value.get("schema") != PROSPECTIVE_OPENING_ADMISSION_VERSION
        or not isinstance(contracts_value, list)
        or any(not isinstance(row, Mapping) for row in contracts_value)
    ):
        raise ValueError("prospective opening admission payload changed")
    contracts = tuple(
        ProspectiveOpeningContract(
            contract_id=int(row["contract_id"]),
            catalog_version_id=int(row["catalog_version_id"]),
            contract_ticker=str(row["contract_ticker"]),
            underlyer=str(row["underlyer"]),
            expiration_date=date.fromisoformat(
                str(row["expiration_date"])
            ),
            contract_type=str(row["contract_type"]),
            strike=Decimal(str(row["strike"])),
            spot=Decimal(str(row["spot"])),
            absolute_moneyness=Decimal(
                str(row["absolute_moneyness"])
            ),
            day_volume=int(row["day_volume"]),
            open_interest=(
                None
                if row.get("open_interest") is None
                else int(row["open_interest"])
            ),
            catalog_payload_sha256=str(
                row["catalog_payload_sha256"]
            ),
            snapshot_payload_sha256=str(
                row["snapshot_payload_sha256"]
            ),
            open_interest_member_sha256=(
                None
                if row.get("open_interest_member_sha256") is None
                else str(row["open_interest_member_sha256"])
            ),
        )
        for row in contracts_value
    )
    admission = _opening_admission(
        status=OpeningAdmissionStatus(str(value["status"])),
        session_date=date.fromisoformat(str(value["session_date"])),
        prepared_at=datetime.fromisoformat(str(value["prepared_at"])),
        capture_boundary=datetime.fromisoformat(
            str(value["capture_boundary"])
        ),
        pre_session_selection_id=(
            None
            if value.get("pre_session_selection_id") is None
            else UUID(str(value["pre_session_selection_id"]))
        ),
        pre_session_selection_sha256=value.get(
            "pre_session_selection_sha256"
        ),
        source_evidence_sha256=value.get("source_evidence_sha256"),
        reference_evidence_sha256=value.get(
            "reference_evidence_sha256"
        ),
        contracts=contracts,
        reason_codes=tuple(value.get("reason_codes") or ()),
        policy_sha256=str(value["policy_sha256"]),
    )
    if prospective_opening_admission_payload(admission) != value:
        raise ValueError("prospective opening admission payload changed")
    return admission


def _opening_admission(
    *,
    status: OpeningAdmissionStatus,
    session_date: date,
    prepared_at: datetime,
    capture_boundary: datetime,
    pre_session_selection_id: UUID | None,
    pre_session_selection_sha256: str | None,
    source_evidence_sha256: str | None,
    reference_evidence_sha256: str | None,
    contracts: tuple[ProspectiveOpeningContract, ...] = (),
    reason_codes: tuple[str, ...] = (),
    policy: ProspectiveOpeningAdmissionPolicy = (
        PROSPECTIVE_OPENING_ADMISSION_POLICY
    ),
    policy_sha256: str | None = None,
) -> ProspectiveOpeningAdmission:
    if not isinstance(policy, ProspectiveOpeningAdmissionPolicy):
        raise TypeError("prospective opening policy is invalid")
    unsigned = ProspectiveOpeningAdmission.__new__(ProspectiveOpeningAdmission)
    object.__setattr__(unsigned, "admission_id", UUID(int=1))
    object.__setattr__(unsigned, "admission_sha256", "0" * 64)
    object.__setattr__(unsigned, "status", status)
    object.__setattr__(unsigned, "session_date", session_date)
    object.__setattr__(unsigned, "prepared_at", _utc(prepared_at, "prepared_at"))
    object.__setattr__(
        unsigned,
        "capture_boundary",
        _utc(capture_boundary, "capture_boundary"),
    )
    object.__setattr__(
        unsigned,
        "policy_sha256",
        (
            policy.sha256
            if policy_sha256 is None
            else _sha256(policy_sha256, "policy_sha256")
        ),
    )
    object.__setattr__(
        unsigned,
        "pre_session_selection_id",
        pre_session_selection_id,
    )
    object.__setattr__(
        unsigned,
        "pre_session_selection_sha256",
        pre_session_selection_sha256,
    )
    object.__setattr__(
        unsigned,
        "source_evidence_sha256",
        source_evidence_sha256,
    )
    object.__setattr__(
        unsigned,
        "reference_evidence_sha256",
        reference_evidence_sha256,
    )
    object.__setattr__(unsigned, "contracts", contracts)
    object.__setattr__(unsigned, "reason_codes", reason_codes)
    object.__setattr__(
        unsigned,
        "opening_capture_ready",
        status is OpeningAdmissionStatus.READY,
    )
    object.__setattr__(unsigned, "opening_coverage_claimed", False)
    object.__setattr__(
        unsigned,
        "accumulation_900_second_coverage_claimed",
        False,
    )
    digest = canonical_sha256(
        prospective_opening_admission_payload(unsigned, include_identity=False)
    )
    identity = uuid5(
        NAMESPACE_URL,
        f"stockscreener:prospective-opening-admission:{digest}",
    )
    return ProspectiveOpeningAdmission(
        admission_id=identity,
        admission_sha256=digest,
        status=status,
        session_date=session_date,
        prepared_at=prepared_at,
        capture_boundary=capture_boundary,
        policy_sha256=(
            policy.sha256
            if policy_sha256 is None
            else _sha256(policy_sha256, "policy_sha256")
        ),
        pre_session_selection_id=pre_session_selection_id,
        pre_session_selection_sha256=pre_session_selection_sha256,
        source_evidence_sha256=source_evidence_sha256,
        reference_evidence_sha256=reference_evidence_sha256,
        contracts=contracts,
        reason_codes=reason_codes,
        opening_capture_ready=status is OpeningAdmissionStatus.READY,
    )


def unavailable_prospective_opening_admission(
    *,
    session_date: date,
    prepared_at: datetime,
    capture_boundary: datetime,
    reason_codes: tuple[str, ...],
    source_evidence_sha256: str | None = None,
    policy: ProspectiveOpeningAdmissionPolicy = (
        PROSPECTIVE_OPENING_ADMISSION_POLICY
    ),
) -> ProspectiveOpeningAdmission:
    return _opening_admission(
        status=OpeningAdmissionStatus.UNAVAILABLE,
        session_date=session_date,
        prepared_at=prepared_at,
        capture_boundary=capture_boundary,
        pre_session_selection_id=None,
        pre_session_selection_sha256=None,
        source_evidence_sha256=source_evidence_sha256,
        reference_evidence_sha256=None,
        reason_codes=reason_codes,
        policy=policy,
    )


def late_canary_opening_admission(
    *,
    session_date: date,
    prepared_at: datetime,
    capture_boundary: datetime,
    activity_selection_sha256: str,
) -> ProspectiveOpeningAdmission:
    return unavailable_prospective_opening_admission(
        session_date=session_date,
        prepared_at=prepared_at,
        capture_boundary=capture_boundary,
        source_evidence_sha256=_sha256(
            activity_selection_sha256,
            "activity_selection_sha256",
        ),
        reason_codes=(
            "POST_E_ACTIVITY_RANKED_LATE_CANARY",
            "OPENING_COVERAGE_UNAVAILABLE",
            "ACCUMULATION_900_SECOND_COVERAGE_UNAVAILABLE",
        ),
    )


def build_prospective_opening_admission(
    result: OptionPreSessionProducerResult | None,
    reference: OpeningReferenceEvidence | None,
    *,
    prepared_at: datetime,
    capture_boundary: datetime,
    policy: ProspectiveOpeningAdmissionPolicy = (
        PROSPECTIVE_OPENING_ADMISSION_POLICY
    ),
) -> ProspectiveOpeningAdmission:
    if not isinstance(policy, ProspectiveOpeningAdmissionPolicy):
        raise TypeError("prospective opening policy is invalid")
    prepared = _utc(prepared_at, "prepared_at")
    boundary = _utc(capture_boundary, "capture_boundary")
    if prepared >= boundary:
        raise ValueError("prospective admission must be prepared before capture")
    session_date = boundary.date()
    if result is None:
        return unavailable_prospective_opening_admission(
            session_date=session_date,
            prepared_at=prepared,
            capture_boundary=boundary,
            reason_codes=("PRE_SESSION_SELECTION_UNAVAILABLE",),
            policy=policy,
        )
    if result.plan.session_date != session_date:
        raise ValueError("pre-session selection belongs to another session")
    if result.plan.status is not ManifestSelectionStatus.READY:
        return unavailable_prospective_opening_admission(
            session_date=session_date,
            prepared_at=prepared,
            capture_boundary=boundary,
            source_evidence_sha256=result.plan.source_evidence_sha256,
            reason_codes=("PRE_SESSION_SELECTION_BLOCKED",),
            policy=policy,
        )
    if not result.catalog or not result.snapshots:
        return unavailable_prospective_opening_admission(
            session_date=session_date,
            prepared_at=prepared,
            capture_boundary=boundary,
            source_evidence_sha256=result.plan.source_evidence_sha256,
            reason_codes=("PRE_SESSION_RANKING_INPUTS_UNAVAILABLE",),
            policy=policy,
        )
    if reference is None:
        return unavailable_prospective_opening_admission(
            session_date=session_date,
            prepared_at=prepared,
            capture_boundary=boundary,
            source_evidence_sha256=result.plan.source_evidence_sha256,
            reason_codes=("PRE_OPEN_REFERENCE_UNAVAILABLE",),
            policy=policy,
        )
    if (
        reference.session_date != session_date
        or reference.known_at > prepared
        or reference.valid_until < boundary
    ):
        raise ValueError("pre-open reference clocks or session diverged")

    catalog = {row.contract_id: row for row in result.catalog}
    snapshots = {row.contract_id: row for row in result.snapshots}
    oi = {row.contract_id: row for row in reference.open_interest}
    included = tuple(
        row
        for row in result.plan.dispositions
        if row.status is ContractSelectionStatus.INCLUDED
        and row.underlyer in policy.underlyings
    )
    if {row.underlyer for row in included} != set(
        policy.underlyings
    ):
        return unavailable_prospective_opening_admission(
            session_date=session_date,
            prepared_at=prepared,
            capture_boundary=boundary,
            source_evidence_sha256=result.plan.source_evidence_sha256,
            reason_codes=(
                "CONFIGURED_PRE_SESSION_COVERAGE_UNAVAILABLE",
            ),
            policy=policy,
        )
    if any(
        row.contract_id not in catalog
        or row.contract_id not in snapshots
        or row.contract_id not in oi
        for row in included
    ):
        return unavailable_prospective_opening_admission(
            session_date=session_date,
            prepared_at=prepared,
            capture_boundary=boundary,
            source_evidence_sha256=result.plan.source_evidence_sha256,
            reason_codes=("EXACT_PRE_SESSION_RANKING_INPUT_UNAVAILABLE",),
            policy=policy,
        )

    candidates: dict[str, list[ProspectiveOpeningContract]] = {
        ticker: [] for ticker in policy.underlyings
    }
    for disposition in included:
        catalog_row = catalog[disposition.contract_id]
        snapshot = snapshots[disposition.contract_id]
        oi_row = oi[disposition.contract_id]
        if (
            snapshot.market_data_time > prepared
            or snapshot.first_observed_at > prepared
            or catalog_row.first_observed_at > prepared
            or (
                catalog_row.valid_to is not None
                and catalog_row.valid_to <= boundary
            )
        ):
            raise ValueError("pre-session catalog or ranking evidence is not causal")
        if (
            snapshot.day_volume is None
            or snapshot.open_interest is None
            or snapshot.open_interest != oi_row.open_interest
            or snapshot.contract_ticker != oi_row.contract_ticker
            or snapshot.underlyer != oi_row.underlyer
        ):
            return unavailable_prospective_opening_admission(
                session_date=session_date,
                prepared_at=prepared,
                capture_boundary=boundary,
                source_evidence_sha256=result.plan.source_evidence_sha256,
                reason_codes=("EXACT_OI_LIQUIDITY_IDENTITY_UNAVAILABLE",),
                policy=policy,
            )
        candidates[disposition.underlyer].append(
            ProspectiveOpeningContract(
                contract_id=disposition.contract_id,
                catalog_version_id=disposition.catalog_version_id,
                contract_ticker=disposition.contract_ticker,
                underlyer=disposition.underlyer,
                expiration_date=disposition.expiration_date,
                contract_type=snapshot.contract_type.value,
                strike=snapshot.strike,
                spot=snapshot.spot,
                absolute_moneyness=(
                    abs(snapshot.strike - snapshot.spot) / snapshot.spot
                ),
                day_volume=snapshot.day_volume,
                open_interest=snapshot.open_interest,
                catalog_payload_sha256=catalog_row.payload_sha256,
                snapshot_payload_sha256=(
                    snapshot.normalized_payload_sha256
                ),
                open_interest_member_sha256=oi_row.member_sha256,
            )
        )

    selected = []
    for underlyer in policy.underlyings:
        for contract_type in ("CALL", "PUT"):
            ranked = sorted(
                (
                    row
                    for row in candidates[underlyer]
                    if row.contract_type == contract_type
                ),
                key=lambda row: (
                    row.absolute_moneyness,
                    -row.open_interest,
                    -row.day_volume,
                    row.expiration_date,
                    row.contract_ticker,
                ),
            )
            if len(ranked) < policy.contracts_per_underlying:
                return unavailable_prospective_opening_admission(
                    session_date=session_date,
                    prepared_at=prepared,
                    capture_boundary=boundary,
                    source_evidence_sha256=(
                        result.plan.source_evidence_sha256
                    ),
                    reason_codes=(
                        "PROSPECTIVE_RANKING_RESULT_UNAVAILABLE",
                    ),
                    policy=policy,
                )
            selected.extend(
                ranked[:policy.contracts_per_underlying]
            )

    return _opening_admission(
        status=OpeningAdmissionStatus.READY,
        session_date=session_date,
        prepared_at=prepared,
        capture_boundary=boundary,
        pre_session_selection_id=result.selection_id,
        pre_session_selection_sha256=result.plan.selection_sha256,
        source_evidence_sha256=result.plan.source_evidence_sha256,
        reference_evidence_sha256=reference.sha256,
        contracts=tuple(selected),
        policy=policy,
    )


@dataclass(frozen=True, slots=True)
class OpeningSubscriptionReadiness:
    readiness_sha256: str
    admission_id: UUID
    admission_sha256: str
    cohort_id: UUID
    slot_set_version: int
    slot_set_member_sha256: str
    slot_set_policy_sha256: str
    slots: tuple[tuple[int, int, str, int, tuple[str, ...]], ...]
    journal_generation: UUID
    journal_identity_sha256: str
    ready_at: datetime
    capture_boundary: datetime

    def __post_init__(self) -> None:
        ready = _utc(self.ready_at, "ready_at")
        boundary = _utc(self.capture_boundary, "capture_boundary")
        slots = tuple(sorted(self.slots, key=lambda row: row[2]))
        try:
            memberships = tuple(
                _option_membership(row[2]) for row in slots
            )
        except ValueError:
            memberships = ()
        if (
            not isinstance(self.admission_id, UUID)
            or not isinstance(self.cohort_id, UUID)
            or not isinstance(self.journal_generation, UUID)
            or self.admission_id.int == 0
            or self.cohort_id.int == 0
            or self.journal_generation.int == 0
            or self.slot_set_version != 1
            or ready > boundary
            or not slots
            or len({row[2] for row in slots}) != len(slots)
            or any(row[4] != ("Q", "T") for row in slots)
            or not memberships
        ):
            raise ValueError("opening subscription readiness is incomplete")
        object.__setattr__(self, "ready_at", ready)
        object.__setattr__(self, "capture_boundary", boundary)
        object.__setattr__(self, "slots", slots)
        for name in (
            "admission_sha256",
            "slot_set_member_sha256",
            "slot_set_policy_sha256",
            "journal_identity_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        expected = canonical_sha256(
            opening_subscription_readiness_payload(
                self,
                include_identity=False,
            )
        )
        if self.readiness_sha256 != expected:
            raise ValueError("opening subscription readiness SHA changed")


def opening_subscription_readiness_payload(
    readiness: OpeningSubscriptionReadiness,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value = {
        "admission_id": str(readiness.admission_id),
        "admission_sha256": readiness.admission_sha256,
        "cohort_id": str(readiness.cohort_id),
        "slot_set_version": readiness.slot_set_version,
        "slot_set_member_sha256": readiness.slot_set_member_sha256,
        "slot_set_policy_sha256": readiness.slot_set_policy_sha256,
        "slots": [
            {
                "instrument_id": row[0],
                "contract_id": row[1],
                "contract_ticker": row[2],
                "catalog_version_id": row[3],
                "channels": list(row[4]),
            }
            for row in readiness.slots
        ],
        "journal_generation": str(readiness.journal_generation),
        "journal_identity_sha256": readiness.journal_identity_sha256,
        "ready_at": readiness.ready_at.isoformat(),
        "capture_boundary": readiness.capture_boundary.isoformat(),
    }
    if include_identity:
        value["readiness_sha256"] = readiness.readiness_sha256
    return value


def build_opening_subscription_readiness(
    admission: ProspectiveOpeningAdmission,
    slot_set: SlotSetVersion,
    *,
    instrument_id_by_contract_ticker: dict[str, int],
    journal_generation: UUID,
    journal_identity_sha256: str,
    ready_at: datetime,
) -> OpeningSubscriptionReadiness:
    if (
        admission.status is not OpeningAdmissionStatus.READY
        or not isinstance(slot_set, SlotSetVersion)
        or slot_set.kind is not SlotSetKind.PRE_SESSION
        or slot_set.version != 1
        or slot_set.effective_at > _utc(ready_at, "ready_at")
    ):
        raise ValueError("opening subscription inputs are not ready")
    admitted = {row.contract_ticker: row for row in admission.contracts}
    if set(instrument_id_by_contract_ticker) != set(admitted):
        raise ValueError("opening instrument identity does not match admission")
    expected = {
        (
            instrument_id_by_contract_ticker[ticker],
            channel,
            admitted[ticker].catalog_version_id,
        )
        for ticker in admitted
        for channel in PROSPECTIVE_OPENING_CHANNELS
    }
    observed = {
        (row.instrument_id, row.channel, row.catalog_version_id)
        for row in slot_set.members
        if row.purpose is SlotSetPurpose.PRIMARY
    }
    if (
        observed != expected
        or len(slot_set.members) != len(expected)
        or slot_set.contract_count != len(admitted)
    ):
        raise ValueError("opening T/Q slots do not exactly match admission")
    slots = tuple(
        (
            instrument_id_by_contract_ticker[ticker],
            admitted[ticker].contract_id,
            ticker,
            admitted[ticker].catalog_version_id,
            ("Q", "T"),
        )
        for ticker in sorted(admitted)
    )
    unsigned = OpeningSubscriptionReadiness.__new__(
        OpeningSubscriptionReadiness
    )
    for name, value in (
        ("readiness_sha256", "0" * 64),
        ("admission_id", admission.admission_id),
        ("admission_sha256", admission.admission_sha256),
        ("cohort_id", slot_set.cohort_id),
        ("slot_set_version", slot_set.version),
        ("slot_set_member_sha256", slot_set.member_sha256),
        ("slot_set_policy_sha256", slot_set.policy_sha256),
        ("slots", slots),
        ("journal_generation", journal_generation),
        ("journal_identity_sha256", journal_identity_sha256),
        ("ready_at", _utc(ready_at, "ready_at")),
        ("capture_boundary", admission.capture_boundary),
    ):
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        opening_subscription_readiness_payload(
            unsigned,
            include_identity=False,
        )
    )
    return OpeningSubscriptionReadiness(
        readiness_sha256=digest,
        admission_id=admission.admission_id,
        admission_sha256=admission.admission_sha256,
        cohort_id=slot_set.cohort_id,
        slot_set_version=slot_set.version,
        slot_set_member_sha256=slot_set.member_sha256,
        slot_set_policy_sha256=slot_set.policy_sha256,
        slots=slots,
        journal_generation=journal_generation,
        journal_identity_sha256=journal_identity_sha256,
        ready_at=ready_at,
        capture_boundary=admission.capture_boundary,
    )


@dataclass(frozen=True, slots=True)
class SessionAcquisitionAuthorization:
    authorization_id: UUID
    authorization_sha256: str
    authorization_reference: str
    cohort_id: UUID
    session_date: date
    prepared_at: datetime
    acquisition_start: datetime
    capture_boundary: datetime
    acquisition_stop: datetime
    admission_id: UUID
    admission_sha256: str
    readiness_sha256: str
    slot_set_member_sha256: str
    provider_budget_sha256: str
    socket_budget_sha256: str
    journal_generation: UUID
    journal_identity_sha256: str

    def __post_init__(self) -> None:
        prepared = _utc(self.prepared_at, "prepared_at")
        start = _utc(self.acquisition_start, "acquisition_start")
        boundary = _utc(self.capture_boundary, "capture_boundary")
        stop = _utc(self.acquisition_stop, "acquisition_stop")
        if (
            not isinstance(self.authorization_id, UUID)
            or not isinstance(self.cohort_id, UUID)
            or not isinstance(self.admission_id, UUID)
            or not isinstance(self.journal_generation, UUID)
            or min(
                self.authorization_id.int,
                self.cohort_id.int,
                self.admission_id.int,
                self.journal_generation.int,
            ) == 0
            or not isinstance(self.session_date, date)
            or prepared.date() != self.session_date
            or start.date() != self.session_date
            or boundary.date() != self.session_date
            or stop.date() != self.session_date
            or not prepared <= start <= boundary < stop
        ):
            raise ValueError("session acquisition authorization clocks are invalid")
        object.__setattr__(
            self,
            "authorization_reference",
            _text(
                self.authorization_reference,
                "authorization_reference",
            ),
        )
        object.__setattr__(self, "prepared_at", prepared)
        object.__setattr__(self, "acquisition_start", start)
        object.__setattr__(self, "capture_boundary", boundary)
        object.__setattr__(self, "acquisition_stop", stop)
        for name in (
            "admission_sha256",
            "readiness_sha256",
            "slot_set_member_sha256",
            "provider_budget_sha256",
            "socket_budget_sha256",
            "journal_identity_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        expected = canonical_sha256(
            session_acquisition_authorization_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:options-session-acquisition:{expected}",
        )
        if (
            self.authorization_sha256 != expected
            or self.authorization_id != expected_id
        ):
            raise ValueError("session acquisition authority identity changed")


def session_acquisition_authorization_payload(
    authority: SessionAcquisitionAuthorization,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": SESSION_ACQUISITION_AUTHORIZATION_VERSION,
        "authorization_reference": authority.authorization_reference,
        "cohort_id": str(authority.cohort_id),
        "session_date": authority.session_date.isoformat(),
        "prepared_at": authority.prepared_at.isoformat(),
        "acquisition_start": authority.acquisition_start.isoformat(),
        "capture_boundary": authority.capture_boundary.isoformat(),
        "acquisition_stop": authority.acquisition_stop.isoformat(),
        "admission_id": str(authority.admission_id),
        "admission_sha256": authority.admission_sha256,
        "readiness_sha256": authority.readiness_sha256,
        "slot_set_member_sha256": authority.slot_set_member_sha256,
        "provider_budget_sha256": authority.provider_budget_sha256,
        "socket_budget_sha256": authority.socket_budget_sha256,
        "journal_generation": str(authority.journal_generation),
        "journal_identity_sha256": authority.journal_identity_sha256,
        "authority": {
            "provider_request": True,
            "provider_socket": True,
            "options_cycle": False,
            "projection": False,
            "publication": False,
            "pointer": False,
            "dispatch": False,
            "delivery": False,
            "broker": False,
            "trading": False,
        },
        "consumption_point": "FIRST_PROVIDER_EFFECT",
    }
    if include_identity:
        value.update({
            "authorization_id": str(authority.authorization_id),
            "authorization_sha256": authority.authorization_sha256,
        })
    return value


def session_acquisition_authorization_from_payload(
    value: object,
) -> SessionAcquisitionAuthorization:
    if not isinstance(value, Mapping):
        raise TypeError("session acquisition authority payload is invalid")
    if value.get("schema") != SESSION_ACQUISITION_AUTHORIZATION_VERSION:
        raise ValueError("session acquisition authority schema changed")
    authority = SessionAcquisitionAuthorization(
        authorization_id=UUID(str(value["authorization_id"])),
        authorization_sha256=str(value["authorization_sha256"]),
        authorization_reference=str(value["authorization_reference"]),
        cohort_id=UUID(str(value["cohort_id"])),
        session_date=date.fromisoformat(str(value["session_date"])),
        prepared_at=datetime.fromisoformat(str(value["prepared_at"])),
        acquisition_start=datetime.fromisoformat(
            str(value["acquisition_start"])
        ),
        capture_boundary=datetime.fromisoformat(
            str(value["capture_boundary"])
        ),
        acquisition_stop=datetime.fromisoformat(
            str(value["acquisition_stop"])
        ),
        admission_id=UUID(str(value["admission_id"])),
        admission_sha256=str(value["admission_sha256"]),
        readiness_sha256=str(value["readiness_sha256"]),
        slot_set_member_sha256=str(value["slot_set_member_sha256"]),
        provider_budget_sha256=str(value["provider_budget_sha256"]),
        socket_budget_sha256=str(value["socket_budget_sha256"]),
        journal_generation=UUID(str(value["journal_generation"])),
        journal_identity_sha256=str(
            value["journal_identity_sha256"]
        ),
    )
    if session_acquisition_authorization_payload(authority) != value:
        raise ValueError(
            "session acquisition authority payload changed"
        )
    return authority


def build_session_acquisition_authorization(
    admission: ProspectiveOpeningAdmission,
    readiness: OpeningSubscriptionReadiness,
    provider_budget: ProviderBudgetScope,
    socket_budget: AdvancedOptionsSocketBudget,
    *,
    authorization_reference: str,
    prepared_at: datetime,
    acquisition_start: datetime,
    acquisition_stop: datetime,
) -> SessionAcquisitionAuthorization:
    prepared = _utc(prepared_at, "prepared_at")
    start = _utc(acquisition_start, "acquisition_start")
    stop = _utc(acquisition_stop, "acquisition_stop")
    reference = _text(
        authorization_reference,
        "authorization_reference",
    )
    if (
        admission.status is not OpeningAdmissionStatus.READY
        or readiness.admission_id != admission.admission_id
        or readiness.admission_sha256 != admission.admission_sha256
        or readiness.capture_boundary != admission.capture_boundary
        or readiness.ready_at > admission.capture_boundary
        or readiness.ready_at > prepared
        or not isinstance(provider_budget, ProviderBudgetScope)
        or not isinstance(socket_budget, AdvancedOptionsSocketBudget)
        or provider_budget.session_date != admission.session_date
        or provider_budget.account_alias
        != ADVANCED_OPTIONS_ACCOUNT_ALIAS
        or provider_budget.authorization_reference != reference
        or provider_budget.lanes != ADVANCED_OPTIONS_REST_LANES
        or provider_budget.authorized_until < stop
        or not admission.prepared_at <= prepared < admission.capture_boundary
        or not prepared <= start <= admission.capture_boundary < stop
    ):
        raise ValueError("session acquisition authority inputs diverged")
    values = {
        "authorization_reference": reference,
        "cohort_id": readiness.cohort_id,
        "session_date": admission.session_date,
        "prepared_at": prepared,
        "acquisition_start": start,
        "capture_boundary": admission.capture_boundary,
        "acquisition_stop": stop,
        "admission_id": admission.admission_id,
        "admission_sha256": admission.admission_sha256,
        "readiness_sha256": readiness.readiness_sha256,
        "slot_set_member_sha256": readiness.slot_set_member_sha256,
        "provider_budget_sha256": provider_budget.sha256,
        "socket_budget_sha256": socket_budget.sha256,
        "journal_generation": readiness.journal_generation,
        "journal_identity_sha256": readiness.journal_identity_sha256,
    }
    unsigned = SessionAcquisitionAuthorization.__new__(
        SessionAcquisitionAuthorization
    )
    object.__setattr__(unsigned, "authorization_id", UUID(int=1))
    object.__setattr__(unsigned, "authorization_sha256", "0" * 64)
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        session_acquisition_authorization_payload(
            unsigned,
            include_identity=False,
        )
    )
    return SessionAcquisitionAuthorization(
        authorization_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:options-session-acquisition:{digest}",
        ),
        authorization_sha256=digest,
        **values,
    )


@dataclass(frozen=True, slots=True)
class TargetWindowAgePolicy:
    version: str
    maximum_age_seconds: int

    def __post_init__(self) -> None:
        _text(self.version, "age policy version", maximum=96)
        if (
            type(self.maximum_age_seconds) is not int
            or self.maximum_age_seconds <= 0
        ):
            raise ValueError("target-window maximum age must be explicit and positive")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class WindowCycleAuthorization:
    authorization_id: UUID
    authorization_sha256: str
    authorization_reference: str
    session_authorization_id: UUID
    session_authorization_sha256: str
    target_window_id: UUID
    target_cohort_id: UUID
    prepared_at: datetime
    history_binding_sha256: str
    equity_binding_sha256: str
    equity_receipt_id: UUID
    equity_cutoff: datetime
    equity_receipt_created_at: datetime
    options_cutoff: datetime
    age_policy_version: str
    age_policy_sha256: str
    maximum_age_seconds: int
    evidence_sha256: str
    build_id: str
    publication_authorization_reference: str | None = None

    def __post_init__(self) -> None:
        prepared = _utc(self.prepared_at, "prepared_at")
        equity = _utc(self.equity_cutoff, "equity_cutoff")
        equity_created = _utc(
            self.equity_receipt_created_at,
            "equity_receipt_created_at",
        )
        options = _utc(self.options_cutoff, "options_cutoff")
        if (
            not all(
                isinstance(value, UUID)
                for value in (
                    self.authorization_id,
                    self.session_authorization_id,
                    self.target_window_id,
                    self.target_cohort_id,
                    self.equity_receipt_id,
                )
            )
            or min(
                self.authorization_id.int,
                self.session_authorization_id.int,
                self.target_window_id.int,
                self.target_cohort_id.int,
                self.equity_receipt_id.int,
            ) == 0
            or not equity <= equity_created <= prepared <= options
            or (options - equity).total_seconds() > self.maximum_age_seconds
            or type(self.maximum_age_seconds) is not int
            or self.maximum_age_seconds <= 0
        ):
            raise ValueError("window cycle authority H/E/O clocks are invalid")
        object.__setattr__(
            self,
            "authorization_reference",
            _text(
                self.authorization_reference,
                "authorization_reference",
            ),
        )
        object.__setattr__(self, "prepared_at", prepared)
        object.__setattr__(self, "equity_cutoff", equity)
        object.__setattr__(
            self,
            "equity_receipt_created_at",
            equity_created,
        )
        object.__setattr__(self, "options_cutoff", options)
        object.__setattr__(
            self,
            "age_policy_version",
            _text(
                self.age_policy_version,
                "age_policy_version",
                maximum=96,
            ),
        )
        object.__setattr__(
            self,
            "build_id",
            _text(self.build_id, "build_id", maximum=96),
        )
        if self.publication_authorization_reference is not None:
            object.__setattr__(
                self,
                "publication_authorization_reference",
                _text(
                    self.publication_authorization_reference,
                    "publication_authorization_reference",
                ),
            )
        for name in (
            "session_authorization_sha256",
            "history_binding_sha256",
            "equity_binding_sha256",
            "age_policy_sha256",
            "evidence_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        if self.age_policy_sha256 != canonical_sha256(
            TargetWindowAgePolicy(
                self.age_policy_version,
                self.maximum_age_seconds,
            )
        ):
            raise ValueError("window cycle age policy identity changed")
        expected = canonical_sha256(
            window_cycle_authorization_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:options-window-cycle:{expected}",
        )
        if (
            self.authorization_sha256 != expected
            or self.authorization_id != expected_id
        ):
            raise ValueError("window cycle authority identity changed")


def window_cycle_authorization_payload(
    authority: WindowCycleAuthorization,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": WINDOW_CYCLE_AUTHORIZATION_VERSION,
        "authorization_reference": authority.authorization_reference,
        "session_authorization_id": str(
            authority.session_authorization_id
        ),
        "session_authorization_sha256": (
            authority.session_authorization_sha256
        ),
        "target_window_id": str(authority.target_window_id),
        "target_cohort_id": str(authority.target_cohort_id),
        "prepared_at": authority.prepared_at.isoformat(),
        "history_binding_sha256": authority.history_binding_sha256,
        "equity_binding_sha256": authority.equity_binding_sha256,
        "equity_receipt_id": str(authority.equity_receipt_id),
        "equity_cutoff": authority.equity_cutoff.isoformat(),
        "equity_receipt_created_at": (
            authority.equity_receipt_created_at.isoformat()
        ),
        "options_cutoff": authority.options_cutoff.isoformat(),
        "age_policy": {
            "version": authority.age_policy_version,
            "sha256": authority.age_policy_sha256,
            "maximum_age_seconds": authority.maximum_age_seconds,
        },
        "evidence_sha256": authority.evidence_sha256,
        "build_id": authority.build_id,
        "publication_authorization_reference": (
            authority.publication_authorization_reference
        ),
        "authority": {
            "provider_request": False,
            "provider_socket": False,
            "target_window_write": True,
            "target_window_stage": True,
            "projection": True,
            "publication": False,
            "pointer": False,
            "dispatch": False,
            "delivery": False,
            "broker": False,
            "trading": False,
        },
        "consumption_point": "FIRST_TARGET_WINDOW_WRITE_OR_STAGE_EFFECT",
    }
    if include_identity:
        value.update({
            "authorization_id": str(authority.authorization_id),
            "authorization_sha256": authority.authorization_sha256,
        })
    return value


def window_cycle_authorization_from_payload(
    value: object,
) -> WindowCycleAuthorization:
    if not isinstance(value, Mapping):
        raise TypeError("window cycle authority payload is invalid")
    if value.get("schema") != WINDOW_CYCLE_AUTHORIZATION_VERSION:
        raise ValueError("window cycle authority schema changed")
    age_policy = value.get("age_policy")
    if not isinstance(age_policy, Mapping):
        raise ValueError("window cycle age policy is unavailable")
    authority = WindowCycleAuthorization(
        authorization_id=UUID(str(value["authorization_id"])),
        authorization_sha256=str(value["authorization_sha256"]),
        authorization_reference=str(value["authorization_reference"]),
        session_authorization_id=UUID(
            str(value["session_authorization_id"])
        ),
        session_authorization_sha256=str(
            value["session_authorization_sha256"]
        ),
        target_window_id=UUID(str(value["target_window_id"])),
        target_cohort_id=UUID(str(value["target_cohort_id"])),
        prepared_at=datetime.fromisoformat(str(value["prepared_at"])),
        history_binding_sha256=str(value["history_binding_sha256"]),
        equity_binding_sha256=str(value["equity_binding_sha256"]),
        equity_receipt_id=UUID(str(value["equity_receipt_id"])),
        equity_cutoff=datetime.fromisoformat(str(value["equity_cutoff"])),
        equity_receipt_created_at=datetime.fromisoformat(
            str(value["equity_receipt_created_at"])
        ),
        options_cutoff=datetime.fromisoformat(
            str(value["options_cutoff"])
        ),
        age_policy_version=str(age_policy["version"]),
        age_policy_sha256=str(age_policy["sha256"]),
        maximum_age_seconds=int(age_policy["maximum_age_seconds"]),
        evidence_sha256=str(value["evidence_sha256"]),
        build_id=str(value["build_id"]),
        publication_authorization_reference=(
            None
            if value.get("publication_authorization_reference") is None
            else str(value["publication_authorization_reference"])
        ),
    )
    if window_cycle_authorization_payload(authority) != value:
        raise ValueError("window cycle authority payload changed")
    return authority


def build_window_cycle_authorization(
    session_authority: SessionAcquisitionAuthorization,
    *,
    authorization_reference: str,
    target_window_id: UUID,
    target_cohort_id: UUID,
    prepared_at: datetime,
    history_binding_sha256: str,
    equity_binding_sha256: str,
    equity_receipt_id: UUID,
    equity_cutoff: datetime,
    equity_receipt_created_at: datetime,
    options_cutoff: datetime,
    age_policy: TargetWindowAgePolicy,
    evidence_sha256: str,
    build_id: str,
    publication_authorization_reference: str | None = None,
) -> WindowCycleAuthorization:
    if (
        not isinstance(session_authority, SessionAcquisitionAuthorization)
        or not isinstance(age_policy, TargetWindowAgePolicy)
        or target_cohort_id != session_authority.cohort_id
        or _utc(equity_cutoff, "equity_cutoff").date()
        != session_authority.session_date
        or _utc(options_cutoff, "options_cutoff").date()
        != session_authority.session_date
        or _utc(options_cutoff, "options_cutoff")
        > session_authority.acquisition_stop
    ):
        raise ValueError("window cycle authority session scope diverged")
    values = {
        "authorization_reference": _text(
            authorization_reference,
            "authorization_reference",
        ),
        "session_authorization_id": session_authority.authorization_id,
        "session_authorization_sha256": (
            session_authority.authorization_sha256
        ),
        "target_window_id": target_window_id,
        "target_cohort_id": target_cohort_id,
        "prepared_at": prepared_at,
        "history_binding_sha256": history_binding_sha256,
        "equity_binding_sha256": equity_binding_sha256,
        "equity_receipt_id": equity_receipt_id,
        "equity_cutoff": equity_cutoff,
        "equity_receipt_created_at": equity_receipt_created_at,
        "options_cutoff": options_cutoff,
        "age_policy_version": age_policy.version,
        "age_policy_sha256": age_policy.sha256,
        "maximum_age_seconds": age_policy.maximum_age_seconds,
        "evidence_sha256": evidence_sha256,
        "build_id": build_id,
        "publication_authorization_reference": (
            publication_authorization_reference
        ),
    }
    unsigned = WindowCycleAuthorization.__new__(
        WindowCycleAuthorization
    )
    object.__setattr__(unsigned, "authorization_id", UUID(int=1))
    object.__setattr__(unsigned, "authorization_sha256", "0" * 64)
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        window_cycle_authorization_payload(
            unsigned,
            include_identity=False,
        )
    )
    return WindowCycleAuthorization(
        authorization_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:options-window-cycle:{digest}",
        ),
        authorization_sha256=digest,
        **values,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsAuthorityBundle:
    session: SessionAcquisitionAuthorization
    admission: ProspectiveOpeningAdmission
    readiness: OpeningSubscriptionReadiness
    cycle: WindowCycleAuthorization | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.session, SessionAcquisitionAuthorization)
            or self.admission.admission_id != self.session.admission_id
            or self.readiness.readiness_sha256 != self.session.readiness_sha256
            or (
                self.cycle is not None
                and (
                    self.cycle.session_authorization_id
                    != self.session.authorization_id
                    or self.cycle.target_cohort_id != self.session.cohort_id
                )
            )
        ):
            raise ValueError("advanced Options authority bundle diverged")

    @property
    def sha256(self) -> str:
        return canonical_sha256(
            advanced_options_authority_bundle_payload(self)
        )


def advanced_options_authority_bundle_payload(
    bundle: AdvancedOptionsAuthorityBundle,
) -> dict[str, object]:
    return {
        "schema": AUTHORITY_BUNDLE_VERSION,
        "bundle_sha256_inputs": {
            "admission_sha256": bundle.admission.admission_sha256,
            "readiness_sha256": bundle.readiness.readiness_sha256,
            "session_authorization_sha256": (
                bundle.session.authorization_sha256
            ),
            "window_cycle_authorization_sha256": (
                None
                if bundle.cycle is None
                else bundle.cycle.authorization_sha256
            ),
        },
        "admission": prospective_opening_admission_payload(bundle.admission),
        "readiness": opening_subscription_readiness_payload(
            bundle.readiness
        ),
        "session_acquisition": session_acquisition_authorization_payload(
            bundle.session
        ),
        "window_cycle": (
            None
            if bundle.cycle is None
            else window_cycle_authorization_payload(bundle.cycle)
        ),
        "coverage": {
            "opening_capture_ready": (
                bundle.admission.opening_capture_ready
            ),
            "opening_coverage_claimed": False,
            "accumulation_900_second_coverage_claimed": False,
        },
    }


def persist_session_acquisition_authorization_cursor(
    cursor,
    authority: SessionAcquisitionAuthorization,
) -> bool:
    if not isinstance(authority, SessionAcquisitionAuthorization):
        raise TypeError("session acquisition authority is invalid")
    document = session_acquisition_authorization_payload(authority)
    cursor.execute(
        "INSERT INTO rt.advanced_options_session_acquisition_authorizations ("
        "authorization_id,cohort_id,session_date,authorization_reference,"
        "admission_id,admission_sha256,readiness_sha256,"
        "slot_set_member_sha256,provider_budget_sha256,socket_budget_sha256,"
        "journal_generation,journal_identity_sha256,prepared_at,"
        "acquisition_start,capture_boundary,acquisition_stop,"
        "authorization_sha256,authorization_document"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING",
        (
            str(authority.authorization_id),
            str(authority.cohort_id),
            authority.session_date,
            authority.authorization_reference,
            str(authority.admission_id),
            authority.admission_sha256,
            authority.readiness_sha256,
            authority.slot_set_member_sha256,
            authority.provider_budget_sha256,
            authority.socket_budget_sha256,
            str(authority.journal_generation),
            authority.journal_identity_sha256,
            authority.prepared_at,
            authority.acquisition_start,
            authority.capture_boundary,
            authority.acquisition_stop,
            authority.authorization_sha256,
            Json(document),
        ),
    )
    inserted = cursor.rowcount == 1
    cursor.execute(
        "SELECT authorization_sha256,authorization_document "
        "FROM rt.advanced_options_session_acquisition_authorizations "
        "WHERE authorization_reference=%s",
        (authority.authorization_reference,),
    )
    row = cursor.fetchone()
    if (
        row is None
        or str(row[0]) != authority.authorization_sha256
        or canonical_sha256(row[1]) != canonical_sha256(document)
    ):
        raise ValueError(
            "session acquisition authorization replay diverged"
        )
    return inserted


def persist_window_cycle_authorization_cursor(
    cursor,
    authority: WindowCycleAuthorization,
) -> bool:
    if not isinstance(authority, WindowCycleAuthorization):
        raise TypeError("window cycle authority is invalid")
    cursor.execute(
        "SELECT authorization_sha256,cohort_id::text,session_date,"
        "acquisition_stop "
        "FROM rt.advanced_options_session_acquisition_authorizations "
        "WHERE authorization_id=%s",
        (str(authority.session_authorization_id),),
    )
    session = cursor.fetchone()
    if (
        session is None
        or str(session[0]) != authority.session_authorization_sha256
        or str(session[1]) != str(authority.target_cohort_id)
        or session[2] != authority.options_cutoff.date()
        or session[3] < authority.options_cutoff
    ):
        raise ValueError("window cycle session authority changed")
    cursor.execute(
        "SELECT window_run.cohort_id::text,window_run.cutoff,"
        "service.session_date "
        "FROM rt.window_runs AS window_run "
        "JOIN rt.service_plans AS service USING(plan_id) "
        "WHERE window_run.window_id=%s",
        (str(authority.target_window_id),),
    )
    if cursor.fetchone() != (
        str(authority.target_cohort_id),
        authority.options_cutoff,
        authority.options_cutoff.date(),
    ):
        raise ValueError("window cycle target window changed")
    cursor.execute(
        "SELECT receipt.cutoff,receipt.created_at,receipt.receipt_kind,"
        "window_run.state "
        "FROM rt.cycle_receipts AS receipt "
        "JOIN rt.window_runs AS window_run USING(window_id) "
        "WHERE receipt.receipt_id=%s",
        (str(authority.equity_receipt_id),),
    )
    if cursor.fetchone() != (
        authority.equity_cutoff,
        authority.equity_receipt_created_at,
        "INPUT_READY",
        "COMPLETE",
    ):
        raise ValueError("window cycle Equity receipt changed")
    cursor.execute(
        "SELECT binding_sha256 "
        "FROM rt.cohort_history_bindings "
        "WHERE target_cohort_id=%s",
        (str(authority.target_cohort_id),),
    )
    history_binding = cursor.fetchone()
    if (
        history_binding is None
        or str(history_binding[0]) != authority.history_binding_sha256
    ):
        raise ValueError("window cycle history binding changed")
    cursor.execute(
        "SELECT binding_sha256,target_cohort_id::text,"
        "equity_receipt_id::text,equity_cutoff "
        "FROM rt.advanced_options_equity_input_bindings "
        "WHERE target_options_window_id=%s",
        (str(authority.target_window_id),),
    )
    equity_binding = cursor.fetchone()
    if equity_binding != (
        authority.equity_binding_sha256,
        str(authority.target_cohort_id),
        str(authority.equity_receipt_id),
        authority.equity_cutoff,
    ):
        raise ValueError("window cycle Equity binding changed")
    document = window_cycle_authorization_payload(authority)
    cursor.execute(
        "INSERT INTO rt.advanced_options_window_cycle_authorizations ("
        "authorization_id,session_authorization_id,target_window_id,"
        "target_cohort_id,authorization_reference,prepared_at,"
        "history_binding_sha256,equity_binding_sha256,equity_receipt_id,"
        "equity_cutoff,equity_receipt_created_at,options_cutoff,"
        "age_policy_version,age_policy_sha256,maximum_age_seconds,"
        "evidence_sha256,build_id,publication_authorization_reference,"
        "authorization_sha256,authorization_document"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING",
        (
            str(authority.authorization_id),
            str(authority.session_authorization_id),
            str(authority.target_window_id),
            str(authority.target_cohort_id),
            authority.authorization_reference,
            authority.prepared_at,
            authority.history_binding_sha256,
            authority.equity_binding_sha256,
            str(authority.equity_receipt_id),
            authority.equity_cutoff,
            authority.equity_receipt_created_at,
            authority.options_cutoff,
            authority.age_policy_version,
            authority.age_policy_sha256,
            authority.maximum_age_seconds,
            authority.evidence_sha256,
            authority.build_id,
            authority.publication_authorization_reference,
            authority.authorization_sha256,
            Json(document),
        ),
    )
    inserted = cursor.rowcount == 1
    cursor.execute(
        "SELECT authorization_sha256,authorization_document "
        "FROM rt.advanced_options_window_cycle_authorizations "
        "WHERE target_window_id=%s",
        (str(authority.target_window_id),),
    )
    row = cursor.fetchone()
    if (
        row is None
        or str(row[0]) != authority.authorization_sha256
        or canonical_sha256(row[1]) != canonical_sha256(document)
    ):
        raise ValueError("window cycle authorization replay diverged")
    return inserted


def load_window_cycle_authorization_cursor(
    cursor,
    *,
    target_window_id: UUID,
    session_authorization_id: UUID,
) -> WindowCycleAuthorization:
    if (
        not isinstance(target_window_id, UUID)
        or not isinstance(session_authorization_id, UUID)
        or target_window_id.int == 0
        or session_authorization_id.int == 0
    ):
        raise ValueError("window cycle authority lookup scope is invalid")
    cursor.execute(
        "SELECT authorization_id::text,authorization_sha256,"
        "authorization_document "
        "FROM rt.advanced_options_window_cycle_authorizations "
        "WHERE target_window_id=%s AND session_authorization_id=%s",
        (str(target_window_id), str(session_authorization_id)),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(
            "window cycle authority is not prepared for the exact scope"
        )
    authority = window_cycle_authorization_from_payload(row[2])
    if (
        str(authority.authorization_id) != str(row[0])
        or authority.authorization_sha256 != str(row[1])
        or authority.target_window_id != target_window_id
        or authority.session_authorization_id
        != session_authorization_id
        or canonical_sha256(row[2])
        != canonical_sha256(
            window_cycle_authorization_payload(authority)
        )
    ):
        raise ValueError("stored window cycle authority identity changed")
    return authority


@dataclass(frozen=True, slots=True)
class AdvancedOptionsAuthorityReceipt:
    receipt_id: UUID
    receipt_sha256: str
    authority_kind: AdvancedOptionsAuthorityKind
    authority_id: UUID
    authority_sha256: str
    effect_kind: str
    effect_identity_sha256: str
    consumed_at: datetime

    def __post_init__(self) -> None:
        consumed = _utc(self.consumed_at, "consumed_at")
        allowed = (
            SESSION_EFFECT_KINDS
            if self.authority_kind
            is AdvancedOptionsAuthorityKind.SESSION_ACQUISITION
            else WINDOW_EFFECT_KINDS
        )
        if (
            not isinstance(self.receipt_id, UUID)
            or not isinstance(self.authority_id, UUID)
            or self.receipt_id.int == 0
            or self.authority_id.int == 0
            or self.effect_kind not in allowed
        ):
            raise ValueError("advanced Options authority receipt is invalid")
        object.__setattr__(self, "consumed_at", consumed)
        object.__setattr__(
            self,
            "authority_sha256",
            _sha256(self.authority_sha256, "authority_sha256"),
        )
        object.__setattr__(
            self,
            "effect_identity_sha256",
            _sha256(
                self.effect_identity_sha256,
                "effect_identity_sha256",
            ),
        )
        expected = canonical_sha256(
            authority_receipt_payload(self, include_identity=False)
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:options-authority-receipt:"
                f"{self.authority_kind.value}:{self.authority_id}:{expected}"
            ),
        )
        if self.receipt_sha256 != expected or self.receipt_id != expected_id:
            raise ValueError("authority receipt identity changed")


def authority_receipt_payload(
    receipt: AdvancedOptionsAuthorityReceipt,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": AUTHORITY_RECEIPT_VERSION,
        "authority_kind": receipt.authority_kind.value,
        "authority_id": str(receipt.authority_id),
        "authority_sha256": receipt.authority_sha256,
        "effect_kind": receipt.effect_kind,
        "effect_identity_sha256": receipt.effect_identity_sha256,
        "consumed_at": receipt.consumed_at.isoformat(),
    }
    if include_identity:
        value.update({
            "receipt_id": str(receipt.receipt_id),
            "receipt_sha256": receipt.receipt_sha256,
        })
    return value


def authority_receipt_from_payload(
    value: object,
) -> AdvancedOptionsAuthorityReceipt:
    if (
        not isinstance(value, Mapping)
        or value.get("schema") != AUTHORITY_RECEIPT_VERSION
    ):
        raise ValueError("authority receipt payload is invalid")
    receipt = AdvancedOptionsAuthorityReceipt(
        receipt_id=UUID(str(value["receipt_id"])),
        receipt_sha256=str(value["receipt_sha256"]),
        authority_kind=AdvancedOptionsAuthorityKind(
            str(value["authority_kind"])
        ),
        authority_id=UUID(str(value["authority_id"])),
        authority_sha256=str(value["authority_sha256"]),
        effect_kind=str(value["effect_kind"]),
        effect_identity_sha256=str(
            value["effect_identity_sha256"]
        ),
        consumed_at=datetime.fromisoformat(str(value["consumed_at"])),
    )
    if authority_receipt_payload(receipt) != value:
        raise ValueError("authority receipt payload changed")
    return receipt


def build_authority_receipt(
    authority: SessionAcquisitionAuthorization | WindowCycleAuthorization,
    *,
    effect_kind: str,
    effect_identity_sha256: str,
    consumed_at: datetime,
) -> AdvancedOptionsAuthorityReceipt:
    consumed = _utc(consumed_at, "consumed_at")
    if isinstance(authority, SessionAcquisitionAuthorization):
        kind = AdvancedOptionsAuthorityKind.SESSION_ACQUISITION
        if not (
            authority.acquisition_start
            <= consumed
            <= authority.acquisition_stop
        ):
            raise ValueError("session receipt is outside its authority clocks")
    elif isinstance(authority, WindowCycleAuthorization):
        kind = AdvancedOptionsAuthorityKind.WINDOW_CYCLE
        if consumed < authority.prepared_at:
            raise ValueError("cycle receipt precedes cycle preparation")
    else:
        raise TypeError("authority receipt requires a typed authority")
    unsigned = AdvancedOptionsAuthorityReceipt.__new__(
        AdvancedOptionsAuthorityReceipt
    )
    for name, value in (
        ("receipt_id", UUID(int=1)),
        ("receipt_sha256", "0" * 64),
        ("authority_kind", kind),
        ("authority_id", authority.authorization_id),
        ("authority_sha256", authority.authorization_sha256),
        ("effect_kind", effect_kind),
        ("effect_identity_sha256", effect_identity_sha256),
        ("consumed_at", consumed),
    ):
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        authority_receipt_payload(unsigned, include_identity=False)
    )
    return AdvancedOptionsAuthorityReceipt(
        receipt_id=uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:options-authority-receipt:"
                f"{kind.value}:{authority.authorization_id}:{digest}"
            ),
        ),
        receipt_sha256=digest,
        authority_kind=kind,
        authority_id=authority.authorization_id,
        authority_sha256=authority.authorization_sha256,
        effect_kind=effect_kind,
        effect_identity_sha256=effect_identity_sha256,
        consumed_at=consumed,
    )


def consume_authority_cursor(
    cursor,
    authority: SessionAcquisitionAuthorization | WindowCycleAuthorization,
    *,
    effect_kind: str,
    effect_identity_sha256: str,
    consumed_at: datetime,
) -> AdvancedOptionsAuthorityReceipt:
    session_id = (
        str(authority.authorization_id)
        if isinstance(authority, SessionAcquisitionAuthorization)
        else None
    )
    cycle_id = (
        str(authority.authorization_id)
        if isinstance(authority, WindowCycleAuthorization)
        else None
    )
    cursor.execute(
        "SELECT receipt_document "
        "FROM rt.advanced_options_authority_receipts "
        "WHERE session_authorization_id=%s "
        "OR window_cycle_authorization_id=%s",
        (session_id, cycle_id),
    )
    existing_row = cursor.fetchone()
    if existing_row is not None:
        existing = authority_receipt_from_payload(existing_row[0])
        if (
            existing.authority_id != authority.authorization_id
            or existing.authority_sha256
            != authority.authorization_sha256
            or existing.effect_kind != effect_kind
            or existing.effect_identity_sha256
            != _sha256(
                effect_identity_sha256,
                "effect_identity_sha256",
            )
        ):
            raise ValueError(
                f"{existing.authority_kind.value} authority is already consumed"
            )
        return existing
    receipt = build_authority_receipt(
        authority,
        effect_kind=effect_kind,
        effect_identity_sha256=effect_identity_sha256,
        consumed_at=consumed_at,
    )
    document = authority_receipt_payload(receipt)
    authority_relation = (
        "rt.advanced_options_session_acquisition_authorizations"
        if isinstance(authority, SessionAcquisitionAuthorization)
        else "rt.advanced_options_window_cycle_authorizations"
    )
    cursor.execute(
        "SELECT authorization_sha256 FROM "
        + authority_relation
        + " WHERE authorization_id=%s",
        (str(authority.authorization_id),),
    )
    stored_authority = cursor.fetchone()
    if (
        stored_authority is None
        or str(stored_authority[0]) != authority.authorization_sha256
    ):
        raise ValueError("authority receipt source identity changed")
    cursor.execute(
        "INSERT INTO rt.advanced_options_authority_receipts ("
        "receipt_id,authority_kind,session_authorization_id,"
        "window_cycle_authorization_id,authority_sha256,effect_kind,"
        "effect_identity_sha256,consumed_at,receipt_sha256,receipt_document"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (
            str(receipt.receipt_id),
            receipt.authority_kind.value,
            session_id,
            cycle_id,
            receipt.authority_sha256,
            receipt.effect_kind,
            receipt.effect_identity_sha256,
            receipt.consumed_at,
            receipt.receipt_sha256,
            Json(document),
        ),
    )
    cursor.execute(
        "SELECT receipt_id::text,receipt_sha256,receipt_document "
        "FROM rt.advanced_options_authority_receipts "
        "WHERE session_authorization_id=%s "
        "OR window_cycle_authorization_id=%s",
        (session_id, cycle_id),
    )
    row = cursor.fetchone()
    if (
        row is None
        or str(row[0]) != str(receipt.receipt_id)
        or str(row[1]) != receipt.receipt_sha256
        or canonical_sha256(row[2]) != canonical_sha256(document)
    ):
        raise ValueError(
            f"{receipt.authority_kind.value} receipt replay diverged"
        )
    return receipt


def consume_session_acquisition_authority_cursor(
    cursor,
    authority: SessionAcquisitionAuthorization,
    *,
    effect_kind: str,
    effect_identity_sha256: str,
    consumed_at: datetime,
) -> AdvancedOptionsAuthorityReceipt:
    if effect_kind not in SESSION_EFFECT_KINDS:
        raise ValueError("session acquisition effect kind is invalid")
    return consume_authority_cursor(
        cursor,
        authority,
        effect_kind=effect_kind,
        effect_identity_sha256=effect_identity_sha256,
        consumed_at=consumed_at,
    )


def consume_window_cycle_authority_cursor(
    cursor,
    authority: WindowCycleAuthorization,
    *,
    effect_kind: str,
    effect_identity_sha256: str,
    consumed_at: datetime,
) -> AdvancedOptionsAuthorityReceipt:
    if effect_kind not in WINDOW_EFFECT_KINDS:
        raise ValueError("window cycle effect kind is invalid")
    cursor.execute(
        "SELECT receipt.authority_sha256 "
        "FROM rt.advanced_options_authority_receipts AS receipt "
        "JOIN rt.advanced_options_session_acquisition_authorizations "
        "AS authority ON authority.authorization_id="
        "receipt.session_authorization_id "
        "WHERE receipt.session_authorization_id=%s "
        "AND receipt.authority_kind='SESSION_ACQUISITION' "
        "AND authority.authorization_sha256=%s",
        (
            str(authority.session_authorization_id),
            authority.session_authorization_sha256,
        ),
    )
    session_receipt = cursor.fetchone()
    if (
        session_receipt is None
        or str(session_receipt[0])
        != authority.session_authorization_sha256
    ):
        raise ValueError(
            "window cycle requires consumed session acquisition authority"
        )
    return consume_authority_cursor(
        cursor,
        authority,
        effect_kind=effect_kind,
        effect_identity_sha256=effect_identity_sha256,
        consumed_at=consumed_at,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsWindowStageEffectAuthorizer:
    authority: WindowCycleAuthorization

    def __post_init__(self) -> None:
        if not isinstance(self.authority, WindowCycleAuthorization):
            raise TypeError("window stage authority must be typed")

    def __call__(
        self,
        cursor,
        claim,
        stage,
        version: str,
        consumed_at: datetime,
    ) -> AdvancedOptionsAuthorityReceipt:
        if (
            version != "stages_v5"
            or stage.stage_id != "OPTIONS_INPUT_GATE"
            or claim.window.window_id != self.authority.target_window_id
            or claim.window.cohort_id != self.authority.target_cohort_id
            or claim.window.cutoff != self.authority.options_cutoff
        ):
            raise ValueError(
                "window stage effect authority target diverged"
            )
        effect_identity_sha256 = canonical_sha256({
            "schema": "advanced-options-window-stage-effect-v1",
            "authorization_sha256": self.authority.authorization_sha256,
            "target_window_id": str(claim.window.window_id),
            "target_cohort_id": str(claim.window.cohort_id),
            "options_cutoff": claim.window.cutoff.isoformat(),
            "stage_id": stage.stage_id,
            "stage_version": version,
        })
        return consume_window_cycle_authority_cursor(
            cursor,
            self.authority,
            effect_kind="TARGET_WINDOW_STAGE",
            effect_identity_sha256=effect_identity_sha256,
            consumed_at=consumed_at,
        )


def advanced_options_authority_status_cursor(
    cursor,
    bundle: AdvancedOptionsAuthorityBundle,
) -> dict[str, object]:
    cycle_id = (
        None
        if bundle.cycle is None
        else str(bundle.cycle.authorization_id)
    )
    cursor.execute(
        "SELECT receipt_id::text,authority_kind,receipt_sha256 "
        "FROM rt.advanced_options_authority_receipts "
        "WHERE session_authorization_id=%s "
        "OR window_cycle_authorization_id=%s "
        "ORDER BY authority_kind",
        (str(bundle.session.authorization_id), cycle_id),
    )
    receipts = {
        str(row[1]): {
            "receipt_id": str(row[0]),
            "receipt_sha256": str(row[2]),
        }
        for row in cursor.fetchall()
    }
    session_receipt = receipts.get(
        AdvancedOptionsAuthorityKind.SESSION_ACQUISITION.value
    )
    cycle_receipt = receipts.get(
        AdvancedOptionsAuthorityKind.WINDOW_CYCLE.value
    )
    return {
        "schema": AUTHORITY_BUNDLE_VERSION,
        "bundle_sha256": bundle.sha256,
        "admission": {
            "admission_id": str(bundle.admission.admission_id),
            "admission_sha256": bundle.admission.admission_sha256,
            "status": bundle.admission.status.value,
            "opening_capture_ready": (
                bundle.admission.opening_capture_ready
            ),
            "opening_coverage_claimed": False,
            "accumulation_900_second_coverage_claimed": False,
        },
        "session_acquisition": {
            "authorization_id": str(bundle.session.authorization_id),
            "authorization_sha256": bundle.session.authorization_sha256,
            "state": (
                "CONSUMED"
                if session_receipt is not None
                else "PREPARED_UNCONSUMED"
            ),
            "receipt": session_receipt,
        },
        "window_cycle": (
            {
                "state": "NOT_PREPARED",
                "authorization_id": None,
                "authorization_sha256": None,
                "receipt": None,
            }
            if bundle.cycle is None
            else {
                "state": (
                    "CONSUMED"
                    if cycle_receipt is not None
                    else "PREPARED_UNCONSUMED"
                ),
                "authorization_id": str(bundle.cycle.authorization_id),
                "authorization_sha256": (
                    bundle.cycle.authorization_sha256
                ),
                "receipt": cycle_receipt,
            }
        ),
        "cleanup": {
            "immutable_session_authorization_id": str(
                bundle.session.authorization_id
            ),
            "immutable_window_cycle_authorization_id": cycle_id,
            "immutable_receipt_ids": sorted(
                row["receipt_id"] for row in receipts.values()
            ),
            "transient_resources": [],
        },
    }


def advanced_options_authority_preflight_cursor(
    cursor,
    bundle: AdvancedOptionsAuthorityBundle,
    *,
    checked_at: datetime,
) -> dict[str, object]:
    checked = _utc(checked_at, "checked_at")
    if not bundle.session.prepared_at <= checked <= bundle.session.acquisition_stop:
        raise ValueError("authority preflight is outside session clocks")
    status = advanced_options_authority_status_cursor(cursor, bundle)
    return {
        "mode": "READ_ONLY_PREFLIGHT",
        "status": "PASS_ADVANCED_OPTIONS_V3_AUTHORITY_PREFLIGHT",
        "checked_at": checked.isoformat(),
        "bundle_sha256": bundle.sha256,
        "session_acquisition": status["session_acquisition"],
        "window_cycle": status["window_cycle"],
        "provider_effects": 0,
        "socket_effects": 0,
        "target_window_writes": 0,
        "target_window_stage_effects": 0,
        "authority_receipts_created": 0,
    }
