"""Exact one-shot authority for Advanced Equity cohort-local publication."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_sha256
from .orchestration_repository import CycleReceiptSpec


ADVANCED_EQUITY_PUBLICATION_AUTHORITY_SCHEMA = (
    "advanced-equity-p11-publication-authority-v1"
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: object, name: str) -> str:
    digest = str(value)
    if len(digest) != 64 or any(
        character not in "0123456789abcdef"
        for character in digest
    ):
        raise ValueError(f"{name} must be a SHA-256")
    return digest


@dataclass(frozen=True, slots=True)
class AdvancedEquityPublicationAuthority:
    authority_id: UUID
    authority_sha256: str
    authorization_reference: str
    cohort_id: UUID
    session_date: date
    cutoff: datetime
    target_window_id: UUID
    window_key_suffix: str
    input_receipt_sha256: str
    portal_receipt_sha256: str
    expected_prior_release_id: UUID | None
    expected_prior_cutoff: datetime | None
    prepared_at: datetime
    expires_at: datetime
    maximum_pointer_advances: int = 1
    platform_pointer_authorized: bool = False
    alerts_authorized: bool = False
    strategies_authorized: bool = False

    def __post_init__(self) -> None:
        cutoff = _utc(self.cutoff, "cutoff")
        prepared = _utc(self.prepared_at, "prepared_at")
        expires = _utc(self.expires_at, "expires_at")
        prior_cutoff = (
            None
            if self.expected_prior_cutoff is None
            else _utc(
                self.expected_prior_cutoff,
                "expected_prior_cutoff",
            )
        )
        reference = self.authorization_reference.strip()
        if (
            not reference
            or not isinstance(self.authority_id, UUID)
            or not isinstance(self.cohort_id, UUID)
            or not isinstance(self.target_window_id, UUID)
            or min(
                self.authority_id.int,
                self.cohort_id.int,
                self.target_window_id.int,
            ) == 0
            or not isinstance(self.session_date, date)
            or cutoff.date() != self.session_date
            or not prepared >= cutoff
            or not expires > prepared
            or not self.window_key_suffix.startswith("-retry")
            or not self.window_key_suffix[len("-retry"):].isdigit()
            or (self.expected_prior_release_id is None)
            != (prior_cutoff is None)
            or self.maximum_pointer_advances != 1
            or self.platform_pointer_authorized
            or self.alerts_authorized
            or self.strategies_authorized
        ):
            raise ValueError(
                "Advanced Equity publication authority scope is invalid"
            )
        object.__setattr__(
            self,
            "authorization_reference",
            reference,
        )
        object.__setattr__(self, "cutoff", cutoff)
        object.__setattr__(self, "prepared_at", prepared)
        object.__setattr__(self, "expires_at", expires)
        object.__setattr__(
            self,
            "expected_prior_cutoff",
            prior_cutoff,
        )
        for name in (
            "authority_sha256",
            "input_receipt_sha256",
            "portal_receipt_sha256",
        ):
            object.__setattr__(
                self,
                name,
                _sha256(getattr(self, name), name),
            )
        expected_sha256 = canonical_sha256(
            advanced_equity_publication_authority_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:advanced-equity-publication:"
                f"{expected_sha256}"
            ),
        )
        if (
            self.authority_sha256 != expected_sha256
            or self.authority_id != expected_id
        ):
            raise ValueError(
                "Advanced Equity publication authority identity changed"
            )


def advanced_equity_publication_authority_payload(
    authority: AdvancedEquityPublicationAuthority,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": ADVANCED_EQUITY_PUBLICATION_AUTHORITY_SCHEMA,
        "authorization_reference": authority.authorization_reference,
        "cohort_id": str(authority.cohort_id),
        "session_date": authority.session_date.isoformat(),
        "cutoff": authority.cutoff.isoformat(),
        "target_window_id": str(authority.target_window_id),
        "window_key_suffix": authority.window_key_suffix,
        "input_receipt_sha256": authority.input_receipt_sha256,
        "portal_receipt_sha256": authority.portal_receipt_sha256,
        "expected_prior_release_id": (
            None
            if authority.expected_prior_release_id is None
            else str(authority.expected_prior_release_id)
        ),
        "expected_prior_cutoff": (
            None
            if authority.expected_prior_cutoff is None
            else authority.expected_prior_cutoff.isoformat()
        ),
        "prepared_at": authority.prepared_at.isoformat(),
        "expires_at": authority.expires_at.isoformat(),
        "maximum_pointer_advances": (
            authority.maximum_pointer_advances
        ),
        "platform_pointer_authorized": (
            authority.platform_pointer_authorized
        ),
        "alerts_authorized": authority.alerts_authorized,
        "strategies_authorized": authority.strategies_authorized,
    }
    if include_identity:
        value.update({
            "authority_id": str(authority.authority_id),
            "authority_sha256": authority.authority_sha256,
        })
    return value


def build_advanced_equity_publication_authority(
    *,
    authorization_reference: str,
    cohort_id: UUID,
    session_date: date,
    cutoff: datetime,
    target_window_id: UUID,
    window_key_suffix: str,
    input_receipt: CycleReceiptSpec,
    portal_receipt: CycleReceiptSpec,
    expected_prior_release_id: UUID | None,
    expected_prior_cutoff: datetime | None,
    prepared_at: datetime,
    expires_at: datetime,
) -> AdvancedEquityPublicationAuthority:
    unsigned = AdvancedEquityPublicationAuthority.__new__(
        AdvancedEquityPublicationAuthority
    )
    values = {
        "authority_id": UUID(int=1),
        "authority_sha256": "0" * 64,
        "authorization_reference": authorization_reference,
        "cohort_id": cohort_id,
        "session_date": session_date,
        "cutoff": cutoff,
        "target_window_id": target_window_id,
        "window_key_suffix": window_key_suffix,
        "input_receipt_sha256": canonical_sha256(input_receipt),
        "portal_receipt_sha256": canonical_sha256(portal_receipt),
        "expected_prior_release_id": expected_prior_release_id,
        "expected_prior_cutoff": expected_prior_cutoff,
        "prepared_at": prepared_at,
        "expires_at": expires_at,
        "maximum_pointer_advances": 1,
        "platform_pointer_authorized": False,
        "alerts_authorized": False,
        "strategies_authorized": False,
    }
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        advanced_equity_publication_authority_payload(
            unsigned,
            include_identity=False,
        )
    )
    values["authority_sha256"] = digest
    values["authority_id"] = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-equity-publication:{digest}",
    )
    return AdvancedEquityPublicationAuthority(**values)


def advanced_equity_publication_authority_from_payload(
    value: object,
) -> AdvancedEquityPublicationAuthority:
    if not isinstance(value, Mapping):
        raise TypeError(
            "Advanced Equity publication authority payload is invalid"
        )
    authority = AdvancedEquityPublicationAuthority(
        authority_id=UUID(str(value["authority_id"])),
        authority_sha256=str(value["authority_sha256"]),
        authorization_reference=str(
            value["authorization_reference"]
        ),
        cohort_id=UUID(str(value["cohort_id"])),
        session_date=date.fromisoformat(str(value["session_date"])),
        cutoff=datetime.fromisoformat(str(value["cutoff"])),
        target_window_id=UUID(str(value["target_window_id"])),
        window_key_suffix=str(value["window_key_suffix"]),
        input_receipt_sha256=str(value["input_receipt_sha256"]),
        portal_receipt_sha256=str(value["portal_receipt_sha256"]),
        expected_prior_release_id=(
            None
            if value.get("expected_prior_release_id") is None
            else UUID(str(value["expected_prior_release_id"]))
        ),
        expected_prior_cutoff=(
            None
            if value.get("expected_prior_cutoff") is None
            else datetime.fromisoformat(
                str(value["expected_prior_cutoff"])
            )
        ),
        prepared_at=datetime.fromisoformat(str(value["prepared_at"])),
        expires_at=datetime.fromisoformat(str(value["expires_at"])),
        maximum_pointer_advances=int(
            value["maximum_pointer_advances"]
        ),
        platform_pointer_authorized=bool(
            value["platform_pointer_authorized"]
        ),
        alerts_authorized=bool(value["alerts_authorized"]),
        strategies_authorized=bool(
            value["strategies_authorized"]
        ),
    )
    if advanced_equity_publication_authority_payload(authority) != value:
        raise ValueError(
            "Advanced Equity publication authority payload changed"
        )
    return authority


def validate_advanced_equity_publication_authority(
    authority: AdvancedEquityPublicationAuthority,
    *,
    cohort_id: UUID,
    session_date: date,
    cutoff: datetime,
    window_key_suffix: str,
    input_receipt: CycleReceiptSpec,
    portal_receipt: CycleReceiptSpec,
    prior_release_id: UUID | None,
    prior_cutoff: datetime | None,
    checked_at: datetime,
) -> None:
    checked = _utc(checked_at, "checked_at")
    window_key = (
        f"advanced-{_utc(cutoff, 'cutoff'):%Y%m%dT%H%M%SZ}"
        f"{window_key_suffix}"
    )
    expected_window_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:window:{cohort_id}:{window_key}",
    )
    if (
        authority.cohort_id != cohort_id
        or authority.session_date != session_date
        or authority.cutoff != _utc(cutoff, "cutoff")
        or authority.window_key_suffix != window_key_suffix
        or authority.target_window_id != expected_window_id
        or authority.input_receipt_sha256
        != canonical_sha256(input_receipt)
        or authority.portal_receipt_sha256
        != canonical_sha256(portal_receipt)
        or authority.expected_prior_release_id != prior_release_id
        or authority.expected_prior_cutoff != prior_cutoff
        or not authority.prepared_at <= checked <= authority.expires_at
    ):
        raise ValueError(
            "Advanced Equity publication authority does not match "
            "the durable publication scope"
        )
