"""Option slot sets and the equity-only manifest identity of the V2 control plane.

V2 splits what v1 stored as one manifest. `rt.subscription_manifests` and `rt.subscription_members` describe
the equity universe and which members are option underlyings. Option contracts live in versioned slot sets
under a cohort (`rt.slot_set_versions`, `rt.slot_set_members`, `rt.slot_set_current`): a PRE_SESSION version
first, REFRESH versions later, one active pointer per cohort.

This module is the pure part: member and version objects, the deterministic `member_sha256`, the provider cap
(1,000 option quote contracts per connection) and the split of an in-memory `SubscriptionManifest` into the
equity-only manifest identity and its slot-set members. SQL lives in `option_slot_set_repository`.

Not built, by decision: the quote-slot admission and rotation policy (which contracts fill the RESERVE slots,
reserve churn, pinned and retained legs, promotion timing). `SlotSetVersion` stores whatever members a policy
produces and `policy_sha256` is an input that identifies that policy; the pre-session derivation below admits
only the selected PRIMARY contracts and leaves the reserve budget unfilled.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable, Mapping
from uuid import UUID

from .contracts import (
    AssetClass,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    canonical_sha256,
)

MAXIMUM_SLOT_SET_CONTRACTS = 1_000
SLOT_SET_MEMBER_HASH_VERSION = "slot-set-members-v1"
OPTION_UNIVERSE_MANIFEST_VERSION = "option-universe-manifest-v2"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SLOT_CHANNELS = frozenset({MarketChannel.TRADE, MarketChannel.QUOTE})


class SlotSetKind(str, Enum):
    PRE_SESSION = "PRE_SESSION"
    REFRESH = "REFRESH"


class SlotSetPurpose(str, Enum):
    PRIMARY = "PRIMARY"
    RESERVE = "RESERVE"
    RETAINED_ALERT = "RETAINED_ALERT"
    POSITION = "POSITION"


def _positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class SlotSetMember:
    instrument_id: int
    channel: MarketChannel
    purpose: SlotSetPurpose
    priority: int
    catalog_version_id: int

    def __post_init__(self) -> None:
        _positive_int(self.instrument_id, "slot set instrument_id")
        _positive_int(self.catalog_version_id, "slot set catalog_version_id")
        if not isinstance(self.channel, MarketChannel) or self.channel not in _SLOT_CHANNELS:
            raise ValueError("slot set member channel must be T or Q")
        if not isinstance(self.purpose, SlotSetPurpose):
            raise ValueError("slot set member purpose is invalid")
        if type(self.priority) is not int or not 1 <= self.priority <= 32767:
            raise ValueError("slot set member priority must be an integer in [1, 32767]")

    @property
    def sort_key(self) -> tuple[str, int]:
        return self.channel.value, self.instrument_id


def slot_set_member_sha256(members: Iterable[SlotSetMember]) -> str:
    """Hash of the members in primary-key order; the cohort and version are not part of it."""
    ordered = sorted(members, key=lambda member: member.sort_key)
    return canonical_sha256({
        "version": SLOT_SET_MEMBER_HASH_VERSION,
        "members": [
            (row.instrument_id, row.channel.value, row.purpose.value, row.priority, row.catalog_version_id)
            for row in ordered
        ],
    })


@dataclass(frozen=True, slots=True)
class SlotSetVersion:
    cohort_id: UUID
    version: int
    kind: SlotSetKind
    effective_at: datetime
    policy_sha256: str
    members: tuple[SlotSetMember, ...]
    member_sha256: str = field(init=False)
    primary_contract_count: int = field(init=False)
    reserve_contract_count: int = field(init=False)
    contract_count: int = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.cohort_id, UUID):
            raise ValueError("slot set cohort_id must be a UUID")
        _positive_int(self.version, "slot set version")
        if not isinstance(self.kind, SlotSetKind):
            raise ValueError("slot set kind is invalid")
        if self.kind is SlotSetKind.PRE_SESSION and self.version != 1:
            raise ValueError("the PRE_SESSION slot set is version 1")
        if self.kind is SlotSetKind.REFRESH and self.version < 2:
            raise ValueError("a REFRESH slot set follows the PRE_SESSION version")
        if self.effective_at.tzinfo is None or self.effective_at.utcoffset() is None:
            raise ValueError("slot set effective_at must be timezone-aware")
        if not isinstance(self.policy_sha256, str) or not _SHA256_PATTERN.fullmatch(self.policy_sha256):
            raise ValueError("slot set policy_sha256 must be a SHA-256 digest")
        members = tuple(self.members)
        if not members or any(not isinstance(row, SlotSetMember) for row in members):
            raise ValueError("slot set requires typed members")
        ordered = tuple(sorted(members, key=lambda member: member.sort_key))
        if len({row.sort_key for row in ordered}) != len(ordered):
            raise ValueError("slot set cannot contain a duplicate channel and instrument")
        purposes: dict[int, SlotSetPurpose] = {}
        for row in ordered:
            if purposes.setdefault(row.instrument_id, row.purpose) is not row.purpose:
                raise ValueError("a slot set contract has one purpose across its channels")
        if len(purposes) > MAXIMUM_SLOT_SET_CONTRACTS:
            raise ValueError(
                f"slot set exceeds the {MAXIMUM_SLOT_SET_CONTRACTS} contract provider cap"
            )
        object.__setattr__(self, "effective_at", self.effective_at.astimezone(timezone.utc))
        object.__setattr__(self, "members", ordered)
        object.__setattr__(self, "member_sha256", slot_set_member_sha256(ordered))
        object.__setattr__(self, "contract_count", len(purposes))
        object.__setattr__(self, "primary_contract_count", sum(
            purpose is SlotSetPurpose.PRIMARY for purpose in purposes.values()
        ))
        object.__setattr__(self, "reserve_contract_count", sum(
            purpose is SlotSetPurpose.RESERVE for purpose in purposes.values()
        ))


def equity_manifest_members(manifest: SubscriptionManifest) -> tuple[SubscriptionMember, ...]:
    return tuple(row for row in manifest.members if row.asset_class is AssetClass.EQUITY)


def option_manifest_members(manifest: SubscriptionManifest) -> tuple[SubscriptionMember, ...]:
    return tuple(row for row in manifest.members if row.asset_class is AssetClass.OPTION)


def option_universe_manifest_sha256(manifest: SubscriptionManifest) -> str:
    """Identity of the persisted manifest: the equity members, the universe and the allocation policy.

    Option contracts are deliberately outside it, so a refresh or another slot set never creates a manifest.
    """
    return canonical_sha256({
        "version": OPTION_UNIVERSE_MANIFEST_VERSION,
        "manifest_version": manifest.version,
        "session_date": manifest.session_date,
        "universe": manifest.universe,
        "capability_profile_sha256": manifest.capability_profile_sha256,
        "allocation_policy_sha256": manifest.allocation_policy_sha256,
        "members": sorted(
            equity_manifest_members(manifest),
            key=lambda row: (row.provider_symbol, row.channel.value),
        ),
    })


def slot_members_from_manifest(
    manifest: SubscriptionManifest,
    instrument_ids: Mapping[str, int],
) -> tuple[SlotSetMember, ...]:
    """Slot-set members for the option members of an in-memory manifest.

    `instrument_ids` maps each option contract ticker to its `rt.instruments` id; the catalog version comes from
    the manifest member, so the instrument plus catalog version is the contract identity.
    """
    options = option_manifest_members(manifest)
    if set(instrument_ids) != {row.provider_symbol for row in options}:
        raise ValueError("slot set instrument identities do not match the manifest option contracts")
    return tuple(
        SlotSetMember(
            instrument_ids[row.provider_symbol],
            row.channel,
            SlotSetPurpose(row.purpose.value),
            row.priority,
            row.catalog_version_id,
        )
        for row in options
    )


def pre_session_slot_set(
    manifest: SubscriptionManifest,
    *,
    cohort_id: UUID,
    instrument_ids: Mapping[str, int],
    effective_at: datetime,
    policy_sha256: str | None = None,
) -> SlotSetVersion:
    """Version 1 of a cohort: the selected PRIMARY contracts, reserve slots unfilled.

    `policy_sha256` defaults to the manifest allocation policy; pass the quote-slot policy hash once a policy
    that fills reserve slots exists.
    """
    return SlotSetVersion(
        cohort_id=cohort_id,
        version=1,
        kind=SlotSetKind.PRE_SESSION,
        effective_at=effective_at,
        policy_sha256=policy_sha256 or manifest.allocation_policy_sha256,
        members=slot_members_from_manifest(manifest, instrument_ids),
    )
