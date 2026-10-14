from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import UniverseManifest, canonical_sha256


class ReferenceKind(str, Enum):
    SECURITY = "SECURITY"
    FUNDAMENTALS = "FUNDAMENTALS"
    OPTION_CATALOG = "OPTION_CATALOG"
    CORPORATE_ACTION_COVERAGE = "CORPORATE_ACTION_COVERAGE"
    OI_COVERAGE = "OI_COVERAGE"
    EARNINGS_COVERAGE = "EARNINGS_COVERAGE"
    FOMC_COVERAGE = "FOMC_COVERAGE"
    RATE_CURVE = "RATE_CURVE"


class SeedMemberStatus(str, Enum):
    READY = "READY"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class SeedStatus(str, Enum):
    READY = "READY"
    UNAVAILABLE = "UNAVAILABLE"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _hash(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class ReferenceSeedMember:
    kind: ReferenceKind
    key: str
    source_id: str
    payload_sha256: str
    status: SeedMemberStatus
    observed_at: datetime
    received_at: datetime
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        key = self.key.strip().upper()
        source_id = self.source_id.strip()
        if not key or not source_id:
            raise ValueError("seed member key and source_id cannot be blank")
        observed = _utc(self.observed_at, "observed_at")
        received = _utc(self.received_at, "received_at")
        if received < observed:
            raise ValueError("seed member receipt cannot precede observation")
        reasons = tuple(reason.strip() for reason in self.reason_codes)
        if any(not reason for reason in reasons):
            raise ValueError("seed member reason codes cannot be blank")
        if self.status is SeedMemberStatus.READY and reasons:
            raise ValueError("ready seed member cannot have reasons")
        if self.status is not SeedMemberStatus.READY and not reasons:
            raise ValueError("non-ready seed member requires a reason")
        if (
            self.status is SeedMemberStatus.NOT_APPLICABLE
            and self.kind is not ReferenceKind.EARNINGS_COVERAGE
        ):
            raise ValueError("only earnings coverage may be not applicable")
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "payload_sha256", _hash(self.payload_sha256, "payload_sha256"))
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(self, "reason_codes", reasons)

    @property
    def identity(self) -> tuple[ReferenceKind, str]:
        return self.kind, self.key


@dataclass(frozen=True, slots=True)
class ReferenceSeedManifest:
    seed_id: UUID
    seed_sha256: str
    universe_sha256: str
    session_date: date
    status: SeedStatus
    reason_codes: tuple[str, ...]
    members: tuple[ReferenceSeedMember, ...]
    source_generations: tuple[tuple[str, str], ...]
    created_at: datetime
    effective_at: datetime
    valid_until: datetime


def _required_members(universe: UniverseManifest) -> frozenset[tuple[ReferenceKind, str]]:
    required = set()
    for ticker in universe.equity_tickers:
        required.update(
            {
                (ReferenceKind.SECURITY, ticker),
                (ReferenceKind.CORPORATE_ACTION_COVERAGE, ticker),
                (ReferenceKind.EARNINGS_COVERAGE, ticker),
            }
        )
    for ticker in universe.option_underlyings:
        required.update(
            {
                (ReferenceKind.OPTION_CATALOG, ticker),
                (ReferenceKind.OI_COVERAGE, ticker),
            }
        )
    required.update(
        {
            (ReferenceKind.FOMC_COVERAGE, "GLOBAL"),
            (ReferenceKind.RATE_CURVE, "USD"),
        }
    )
    return frozenset(required)


def _optional_members(universe: UniverseManifest) -> frozenset[tuple[ReferenceKind, str]]:
    return frozenset(
        (ReferenceKind.FUNDAMENTALS, ticker)
        for ticker in universe.equity_tickers
    )


def build_reference_seed_manifest(
    universe: UniverseManifest,
    *,
    session_date: date,
    members: tuple[ReferenceSeedMember, ...],
    source_generations: tuple[tuple[str, str], ...],
    created_at: datetime,
    effective_at: datetime,
    valid_until: datetime,
) -> ReferenceSeedManifest:
    if not members:
        raise ValueError("reference seed requires members")
    created = _utc(created_at, "created_at")
    effective = _utc(effective_at, "effective_at")
    valid = _utc(valid_until, "valid_until")
    if not created <= effective < valid:
        raise ValueError("reference seed clocks must be increasing")
    identities = tuple(member.identity for member in members)
    if len(identities) != len(set(identities)):
        raise ValueError("reference seed member identities must be unique")
    required = _required_members(universe)
    optional = _optional_members(universe)
    extras = set(identities) - required - optional
    if extras:
        raise ValueError("reference seed contains unreviewed member identities")
    generations = tuple(sorted((name.strip(), _hash(digest, name)) for name, digest in source_generations))
    if not generations or any(not name for name, _ in generations):
        raise ValueError("reference seed requires named source generations")
    reasons = [
        f"MISSING_{kind.value}:{key}"
        for kind, key in sorted(required - set(identities), key=lambda item: (item[0].value, item[1]))
    ]
    for member in members:
        if member.status is SeedMemberStatus.UNAVAILABLE and member.identity in required:
            reasons.append(f"UNAVAILABLE_{member.kind.value}:{member.key}")
    reasons = sorted(set(reasons))
    status = SeedStatus.UNAVAILABLE if reasons else SeedStatus.READY
    ordered_members = tuple(sorted(members, key=lambda row: (row.kind.value, row.key)))
    identity = {
        "universe_sha256": universe.sha256,
        "session_date": session_date,
        "status": status,
        "reason_codes": tuple(reasons),
        "members": ordered_members,
        "source_generations": generations,
        "created_at": created,
        "effective_at": effective,
        "valid_until": valid,
    }
    digest = canonical_sha256(identity)
    return ReferenceSeedManifest(
        seed_id=uuid5(NAMESPACE_URL, f"realtime-reference-seed:{digest}"),
        seed_sha256=digest,
        universe_sha256=universe.sha256,
        session_date=session_date,
        status=status,
        reason_codes=tuple(reasons),
        members=ordered_members,
        source_generations=generations,
        created_at=created,
        effective_at=effective,
        valid_until=valid,
    )