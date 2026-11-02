from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from uuid import NAMESPACE_URL, UUID, uuid5

from options.calendar import OptionExchangeCalendar

from .contracts import EQUITY_VALIDATION_UNIVERSE, canonical_sha256


_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
DEFAULT_OPTION_UNDERLYINGS = ("QQQ", "SPY")


class DesiredMemberKind(str, Enum):
    EQUITY = "EQUITY"
    SECTOR_ETF = "SECTOR_ETF"
    OPTION_UNDERLYING = "OPTION_UNDERLYING"


class MemberPreparationStatus(str, Enum):
    WARMING = "WARMING"
    READY = "READY"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class DesiredMemberIntent:
    ticker: str
    kind: DesiredMemberKind
    enabled: bool = True
    priority: int = 1

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper()
        if not _SYMBOL_PATTERN.fullmatch(ticker):
            raise ValueError("desired member ticker has invalid syntax")
        if not isinstance(self.kind, DesiredMemberKind):
            raise TypeError("desired member kind is invalid")
        if type(self.enabled) is not bool:
            raise TypeError("desired member enabled state must be boolean")
        if type(self.priority) is not int or not 1 <= self.priority <= 32767:
            raise ValueError("desired member priority must be in [1, 32767]")
        object.__setattr__(self, "ticker", ticker)

    @property
    def identity(self) -> tuple[str, str]:
        return self.ticker, self.kind.value


@dataclass(frozen=True, slots=True)
class DesiredStateChange:
    source_cohort_id: UUID
    members: tuple[DesiredMemberIntent, ...]
    source_slot_version: int | None = None
    enabled: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.source_cohort_id, UUID) or self.source_cohort_id.int == 0:
            raise ValueError("desired state source cohort is invalid")
        members = tuple(sorted(self.members, key=lambda row: row.identity))
        if not members or any(not isinstance(row, DesiredMemberIntent) for row in members):
            raise ValueError("desired state requires typed members")
        identities = tuple(row.identity for row in members)
        if len(identities) != len(set(identities)):
            raise ValueError("desired state contains duplicate member roles")
        if len({row.ticker for row in members}) > 400:
            raise ValueError("desired state supports at most 400 exact names")
        if type(self.enabled) is not bool:
            raise TypeError("desired state enabled state must be boolean")
        if self.source_slot_version is not None and (
            type(self.source_slot_version) is not int
            or self.source_slot_version <= 0
        ):
            raise ValueError("desired state source slot version must be positive")
        base = {
            row.ticker
            for row in members
            if row.kind in {
                DesiredMemberKind.EQUITY,
                DesiredMemberKind.SECTOR_ETF,
            }
            and row.enabled
        }
        underlyings = {
            row.ticker
            for row in members
            if row.kind is DesiredMemberKind.OPTION_UNDERLYING
            and row.enabled
        }
        if not base:
            raise ValueError("desired state requires an enabled Equity or sector ETF")
        if not underlyings.issubset(base):
            raise ValueError(
                "enabled Options underlyings require an enabled Equity or sector ETF role"
            )
        if not underlyings and self.source_slot_version is not None:
            raise ValueError("Equity-only desired state cannot reference a slot set")
        object.__setattr__(self, "members", members)


@dataclass(frozen=True, slots=True)
class DesiredStateRevision:
    revision_id: UUID
    revision_sha256: str
    effective_session_date: date
    source_cohort_id: UUID
    source_slot_version: int | None
    selection_policy_reference: str
    selection_policy_sha256: str
    enabled: bool
    recorded_at: datetime
    members: tuple[DesiredMemberIntent, ...]
    security_ids: tuple[tuple[str, UUID], ...]


@dataclass(frozen=True, slots=True)
class DesiredMemberPreparation:
    revision_id: UUID
    security_id: UUID | None
    ticker: str
    kind: DesiredMemberKind
    enabled: bool
    status: MemberPreparationStatus
    reasons: tuple[str, ...]
    checked_at: datetime


@dataclass(frozen=True, slots=True)
class DesiredSessionSnapshot:
    revision_id: UUID
    session_date: date
    manifest_id: UUID
    cohort_id: UUID
    slot_set_version: int | None
    snapshot_sha256: str
    materialized_at: datetime


def default_advanced_realtime_desired_state(
    source_cohort_id: UUID,
    *,
    source_slot_version: int | None = 1,
) -> DesiredStateChange:
    members = [
        DesiredMemberIntent(ticker, DesiredMemberKind.EQUITY)
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    ]
    members.extend(
        DesiredMemberIntent(ticker, DesiredMemberKind.OPTION_UNDERLYING)
        for ticker in DEFAULT_OPTION_UNDERLYINGS
    )
    return DesiredStateChange(
        source_cohort_id=source_cohort_id,
        source_slot_version=source_slot_version,
        members=tuple(members),
    )


def effective_next_session(
    observed_at: datetime,
    *,
    calendar: OptionExchangeCalendar | None = None,
) -> date:
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("desired state observation must be timezone-aware")
    observed = observed_at.astimezone(timezone.utc)
    exchange = calendar or OptionExchangeCalendar("XNYS")
    candidate = exchange.session_on_or_after(observed.date())
    if candidate > observed.date():
        return candidate
    if observed < exchange.session_open(candidate):
        return candidate
    return exchange.next_session_date(candidate)


def build_desired_state_revision(
    change: DesiredStateChange,
    *,
    effective_session_date: date,
    recorded_at: datetime,
    security_ids: dict[str, UUID],
    selection_policy_reference: str,
    selection_policy_sha256: str,
) -> DesiredStateRevision:
    if not isinstance(change, DesiredStateChange):
        raise TypeError("desired state change is invalid")
    if not isinstance(effective_session_date, date):
        raise TypeError("desired state effective session must be a date")
    if recorded_at.tzinfo is None or recorded_at.utcoffset() is None:
        raise ValueError("desired state recorded_at must be timezone-aware")
    recorded = recorded_at.astimezone(timezone.utc)
    expected_tickers = {row.ticker for row in change.members}
    if not set(security_ids).issubset(expected_tickers) or any(
        not isinstance(value, UUID) or value.int == 0
        for value in security_ids.values()
    ):
        raise ValueError("desired state stored security identities are invalid")
    policy_reference = selection_policy_reference.strip()
    if not policy_reference:
        raise ValueError("desired state selection policy reference cannot be blank")
    if (
        len(selection_policy_sha256) != 64
        or any(character not in "0123456789abcdef"
               for character in selection_policy_sha256)
    ):
        raise ValueError("desired state selection policy must be a SHA-256 digest")
    identity = {
        "version": "advanced-realtime-desired-state-v1",
        "effective_session_date": effective_session_date,
        "source_cohort_id": change.source_cohort_id,
        "source_slot_version": change.source_slot_version,
        "selection_policy_reference": policy_reference,
        "selection_policy_sha256": selection_policy_sha256,
        "enabled": change.enabled,
        "members": tuple(
            (
                row.ticker,
                row.kind.value,
                row.priority,
                row.enabled,
                security_ids.get(row.ticker),
            )
            for row in change.members
        ),
    }
    revision_sha256 = canonical_sha256(identity)
    return DesiredStateRevision(
        revision_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-realtime-desired:{revision_sha256}",
        ),
        revision_sha256=revision_sha256,
        effective_session_date=effective_session_date,
        source_cohort_id=change.source_cohort_id,
        source_slot_version=change.source_slot_version,
        selection_policy_reference=policy_reference,
        selection_policy_sha256=selection_policy_sha256,
        enabled=change.enabled,
        recorded_at=recorded,
        members=change.members,
        security_ids=tuple(sorted(security_ids.items())),
    )


def desired_member_intent_id(
    revision_id: UUID,
    ticker: str,
    kind: DesiredMemberKind,
) -> UUID:
    if not isinstance(revision_id, UUID) or revision_id.int == 0:
        raise ValueError("desired member intent revision is invalid")
    if not isinstance(kind, DesiredMemberKind):
        raise TypeError("desired member intent kind is invalid")
    normalized = ticker.strip().upper()
    if not _SYMBOL_PATTERN.fullmatch(normalized):
        raise ValueError("desired member intent ticker is invalid")
    return uuid5(
        NAMESPACE_URL,
        "stockscreener:advanced-realtime-desired-intent:"
        f"{revision_id}:{normalized}:{kind.value}",
    )
