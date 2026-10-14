"""SQL for option slot sets (see `option_slot_set`): persist, verify, load and activate.

Rows are immutable. Persisting an identical slot set again is a no-op; a different one under the same
(cohort, version) raises. A version is persisted and re-read from the database first; only then does the
`rt.slot_set_current` pointer move, in the caller's transaction (make-before-break). The pointer never moves
backwards and the database has no guard for that, so these functions are the only supported writers.
All functions run on the caller's cursor and transaction (CONTROL role writes; reads need no more).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from .contracts import MarketChannel
from .option_slot_set import SlotSetKind, SlotSetMember, SlotSetPurpose, SlotSetVersion

_MEMBER_INSERT_CHUNK = 250


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class ActiveSlotSet:
    slot_set: SlotSetVersion
    activated_at: datetime


def _verify_member_identities(cursor, cohort_id: UUID, members: tuple[SlotSetMember, ...]) -> None:
    """The cohort must be a MARKET_OPTIONS cohort and every member an option contract of one of its underlyings.

    The foreign keys only prove that the instrument and the catalog version exist, not that they belong together.
    """
    cursor.execute(
        "SELECT manifest.manifest_kind, instrument.security_id::text "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
        "LEFT JOIN rt.subscription_members AS member "
        "ON member.manifest_id=manifest.manifest_id AND member.option_underlying "
        "LEFT JOIN rt.instruments AS instrument ON instrument.instrument_id=member.instrument_id "
        "WHERE cohort.cohort_id=%s",
        (str(cohort_id),),
    )
    cohort_rows = cursor.fetchall()
    if not cohort_rows:
        raise ValueError("slot set cohort is absent")
    if {row[0] for row in cohort_rows} != {"MARKET_OPTIONS"}:
        raise ValueError("slot set requires a MARKET_OPTIONS cohort")
    underlying_securities = {str(row[1]) for row in cohort_rows if row[1] is not None}
    if not underlying_securities:
        raise ValueError("slot set cohort has no option underlyings")
    cursor.execute(
        "SELECT instrument_id, asset_class, contract_id FROM rt.instruments "
        "WHERE instrument_id=ANY(%s)",
        (sorted({row.instrument_id for row in members}),),
    )
    instruments = {
        int(instrument_id): (str(asset_class), int(contract_id) if contract_id is not None else None)
        for instrument_id, asset_class, contract_id in cursor.fetchall()
    }
    cursor.execute(
        "SELECT version.version_id, version.contract_id, contract.underlying_security_id::text "
        "FROM ref.option_contract_versions AS version "
        "JOIN ref.option_contracts AS contract ON contract.contract_id=version.contract_id "
        "WHERE version.version_id=ANY(%s)",
        (sorted({row.catalog_version_id for row in members}),),
    )
    versions = {
        int(version_id): (int(contract_id), str(security_id))
        for version_id, contract_id, security_id in cursor.fetchall()
    }
    for row in members:
        instrument = instruments.get(row.instrument_id)
        version = versions.get(row.catalog_version_id)
        if instrument is None or version is None:
            raise ValueError("slot set member instrument or catalog version is absent")
        if instrument[0] != "OPTION" or instrument[1] != version[0]:
            raise ValueError("slot set member catalog version does not belong to its instrument")
        if version[1] not in underlying_securities:
            raise ValueError("slot set member underlying is outside the cohort option universe")


def load_slot_set_version_cursor(
    cursor,
    cohort_id: UUID,
    version: int,
    *,
    verify_identities: bool = True,
) -> SlotSetVersion | None:
    """Read one version back, recomputing its counts and member hash and checking member identities."""
    cursor.execute(
        "SELECT kind, effective_at, policy_sha256, member_sha256, primary_contract_count, "
        "reserve_contract_count FROM rt.slot_set_versions WHERE cohort_id=%s AND version=%s",
        (str(cohort_id), version),
    )
    header = cursor.fetchone()
    if header is None:
        return None
    kind, effective_at, policy_sha256, member_sha256, primary_count, reserve_count = header
    cursor.execute(
        "SELECT instrument_id, channel, purpose, priority, catalog_version_id "
        "FROM rt.slot_set_members WHERE cohort_id=%s AND version=%s",
        (str(cohort_id), version),
    )
    members = tuple(
        SlotSetMember(
            int(instrument_id), MarketChannel(channel), SlotSetPurpose(purpose),
            int(priority), int(catalog_version_id),
        )
        for instrument_id, channel, purpose, priority, catalog_version_id in cursor.fetchall()
    )
    loaded = SlotSetVersion(
        cohort_id=cohort_id,
        version=version,
        kind=SlotSetKind(kind),
        effective_at=_utc(effective_at, "stored slot set effective_at"),
        policy_sha256=str(policy_sha256),
        members=members,
    )
    if (
        loaded.member_sha256 != str(member_sha256)
        or loaded.primary_contract_count != int(primary_count)
        or loaded.reserve_contract_count != int(reserve_count)
    ):
        raise ValueError("stored slot set hash or counts do not match its members")
    if verify_identities:
        _verify_member_identities(cursor, cohort_id, loaded.members)
    return loaded


def persist_slot_set_version_cursor(cursor, slot_set: SlotSetVersion) -> SlotSetVersion:
    """Insert a version and its members once; return the verified stored version."""
    if not isinstance(slot_set, SlotSetVersion):
        raise TypeError("slot set persistence requires a validated SlotSetVersion")
    stored = load_slot_set_version_cursor(cursor, slot_set.cohort_id, slot_set.version)
    if stored is not None:
        if stored != slot_set:
            raise ValueError("persisted slot set conflicts with a divergent replay")
        return stored
    _verify_member_identities(cursor, slot_set.cohort_id, slot_set.members)
    cursor.execute(
        "SELECT COALESCE(max(version), 0) FROM rt.slot_set_versions WHERE cohort_id=%s",
        (str(slot_set.cohort_id),),
    )
    if slot_set.version != int(cursor.fetchone()[0]) + 1:
        raise ValueError("slot set versions must be contiguous")
    cursor.execute(
        "INSERT INTO rt.slot_set_versions ("
        "cohort_id, version, kind, effective_at, policy_sha256, member_sha256, "
        "primary_contract_count, reserve_contract_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (cohort_id, version) DO NOTHING",
        (
            str(slot_set.cohort_id), slot_set.version, slot_set.kind.value, slot_set.effective_at,
            slot_set.policy_sha256, slot_set.member_sha256, slot_set.primary_contract_count,
            slot_set.reserve_contract_count,
        ),
    )
    for start in range(0, len(slot_set.members), _MEMBER_INSERT_CHUNK):
        chunk = slot_set.members[start:start + _MEMBER_INSERT_CHUNK]
        parameters = []
        for row in chunk:
            parameters.extend((
                str(slot_set.cohort_id), slot_set.version, row.instrument_id, row.channel.value,
                row.purpose.value, row.priority, row.catalog_version_id,
            ))
        cursor.execute(
            "INSERT INTO rt.slot_set_members ("
            "cohort_id, version, instrument_id, channel, purpose, priority, catalog_version_id) VALUES "
            + ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(chunk))
            + " ON CONFLICT DO NOTHING",
            tuple(parameters),
        )
    reloaded = load_slot_set_version_cursor(cursor, slot_set.cohort_id, slot_set.version)
    if reloaded != slot_set:
        raise ValueError("persisted slot set is incomplete or conflicts with the retained rows")
    return reloaded


def _move_pointer(cursor, slot_set: SlotSetVersion, activated_at: datetime, *, allow_superseded: bool = False) -> bool:
    activated = _utc(activated_at, "activated_at")
    if activated < slot_set.effective_at:
        raise ValueError("slot set activation cannot precede its effective time")
    cursor.execute(
        "INSERT INTO rt.slot_set_current (cohort_id, version, activated_at) VALUES (%s, %s, %s) "
        "ON CONFLICT (cohort_id) DO UPDATE SET version=EXCLUDED.version, "
        "activated_at=EXCLUDED.activated_at, updated_at=clock_timestamp() "
        "WHERE rt.slot_set_current.version < EXCLUDED.version "
        "AND rt.slot_set_current.activated_at <= EXCLUDED.activated_at "
        "RETURNING version",
        (str(slot_set.cohort_id), slot_set.version, activated),
    )
    if cursor.fetchone() is not None:
        return True
    cursor.execute(
        "SELECT version, activated_at FROM rt.slot_set_current WHERE cohort_id=%s",
        (str(slot_set.cohort_id),),
    )
    current = cursor.fetchone()
    if current is None:
        raise ValueError("slot set activation was rejected")
    current_version, current_activated_at = int(current[0]), _utc(current[1], "stored activated_at")
    if current_version == slot_set.version and current_activated_at == activated:
        return False
    if allow_superseded and current_version > slot_set.version:
        return False
    if current_version >= slot_set.version:
        raise ValueError("slot set activation cannot move the pointer backwards or replace a version")
    raise ValueError("slot set activation precedes the current activation time")


def activate_slot_set_cursor(
    cursor,
    cohort_id: UUID,
    version: int,
    *,
    activated_at: datetime,
) -> bool:
    """Verify the persisted version, then move the cohort pointer to it. False when it was already current."""
    slot_set = load_slot_set_version_cursor(cursor, cohort_id, version)
    if slot_set is None:
        raise ValueError("slot set version is not persisted")
    return _move_pointer(cursor, slot_set, activated_at)


def persist_and_activate_slot_set_cursor(
    cursor,
    slot_set: SlotSetVersion,
    *,
    activated_at: datetime | None = None,
    allow_superseded: bool = False,
) -> bool:
    """Persist and verify the version, then activate it; one caller transaction, so no half-moved pointer.

    `allow_superseded` makes the replay of an already persisted, identical version a no-op once the pointer has
    moved past it (an idempotent retry of a pre-session selection after a refresh); it never moves the pointer.
    """
    persisted = persist_slot_set_version_cursor(cursor, slot_set)
    return _move_pointer(
        cursor, persisted, activated_at or persisted.effective_at, allow_superseded=allow_superseded,
    )


def load_active_slot_set_cursor(cursor, cohort_id: UUID) -> ActiveSlotSet | None:
    cursor.execute(
        "SELECT version, activated_at FROM rt.slot_set_current WHERE cohort_id=%s",
        (str(cohort_id),),
    )
    current = cursor.fetchone()
    if current is None:
        return None
    slot_set = load_slot_set_version_cursor(cursor, cohort_id, int(current[0]))
    if slot_set is None:
        raise ValueError("the active slot set version is not persisted")
    return ActiveSlotSet(slot_set, _utc(current[1], "stored activated_at"))


def persist_and_activate_slot_set(
    connection,
    slot_set: SlotSetVersion,
    *,
    activated_at: datetime | None = None,
) -> bool:
    try:
        with connection.cursor() as cursor:
            moved = persist_and_activate_slot_set_cursor(cursor, slot_set, activated_at=activated_at)
        connection.commit()
        return moved
    except Exception:
        connection.rollback()
        raise
