from __future__ import annotations

import json
from contextlib import nullcontext
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import execute_values
from research.screening import VERSION as SCREENING_VERSION

from .contracts import canonical_json, canonical_sha256
from .lineage_head import screening_manifest_limit
from .portal_projection import (
    RealtimeDetectorDataset,
    RealtimeManagementAssessment,
    RealtimePortalSnapshot,
    RealtimeScreeningGeneration,
)
from .preflight import ProviderTier


PORTAL_BATCH_PAGE_SIZE = 100

SNAPSHOT_CLASS_EPHEMERAL = "EPHEMERAL_PORTAL"
SNAPSHOT_CLASS_SESSION_CLOSE = "SESSION_CLOSE"
RELEASE_CLASS_INTRADAY = "INTRADAY"
RELEASE_CLASS_SESSION_CLOSE = "SESSION_CLOSE"
RETENTION_HOT = "HOT"
RETENTION_KEPT = "KEPT"
RETENTION_HOT_AND_KEPT = "HOT_AND_KEPT"
RELEASE_MANIFEST_VERSION = "realtime-portal-release-v1"
HOT_MANIFEST_LIMIT_BYTES = 16384
_EXCHANGE_TIME_ZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class PortalReleaseReceipt:
    """Cycle receipt identity that dispatches one portal release."""

    receipt_id: UUID
    cohort_id: UUID
    provider_tier: ProviderTier
    cutoff: datetime
    universe_sha256: str
    source_receipt_sha256: str | None = None
    source_generation: int | None = None
    desired_revision_id: UUID | None = None
    desired_snapshot_sha256: str | None = None
    membership_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.provider_tier, ProviderTier):
            raise TypeError("portal release provider tier is invalid")
        if self.cutoff.tzinfo is None or self.cutoff.utcoffset() is None:
            raise ValueError("portal release cutoff must be timezone-aware")
        if len(self.universe_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.universe_sha256
        ):
            raise ValueError("portal release universe hash must be SHA-256")
        source_receipt_sha256 = (
            self.universe_sha256
            if self.source_receipt_sha256 is None
            else self.source_receipt_sha256
        )
        for name, value in (
            ("source receipt", source_receipt_sha256),
            ("desired snapshot", self.desired_snapshot_sha256),
            ("membership", self.membership_sha256),
        ):
            if value is not None and (
                len(value) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in value
                )
            ):
                raise ValueError(
                    f"portal release {name} hash must be SHA-256"
                )
        if self.source_generation is not None and (
            type(self.source_generation) is not int
            or self.source_generation <= 0
        ):
            raise ValueError(
                "portal release source generation must be positive"
            )
        object.__setattr__(
            self,
            "source_receipt_sha256",
            source_receipt_sha256,
        )
        object.__setattr__(self, "cutoff", self.cutoff.astimezone(timezone.utc))


@dataclass(frozen=True, slots=True)
class PortalRelease:
    release_id: UUID
    cohort_id: UUID
    provider_tier: ProviderTier
    release_class: str
    receipt_id: UUID
    session_date: date
    cutoff: datetime
    universe_sha256: str
    manifest: Mapping[str, object]
    manifest_sha256: str
    pointer_swapped: bool
    publication_generation: int = 1
    desired_revision_id: UUID | None = None
    desired_snapshot_sha256: str | None = None
    membership_sha256: str | None = None
    publication_disposition: str = "CURRENT"
    deliverable: bool = True


@dataclass(frozen=True, slots=True)
class PortalReleasePointer:
    cohort_id: UUID
    provider_tier: ProviderTier
    release_id: UUID
    cutoff: datetime
    session_date: date
    publication_generation: int | None = None
    desired_snapshot_sha256: str | None = None
    membership_sha256: str | None = None


def portal_session_date(snapshot: RealtimePortalSnapshot, session_date: date | None = None) -> date:
    if session_date is not None:
        return session_date
    stated = snapshot.payload.get("session") if isinstance(snapshot.payload, Mapping) else None
    if isinstance(stated, str):
        try:
            return date.fromisoformat(stated[:10])
        except ValueError:
            pass
    return snapshot.generated_at.astimezone(_EXCHANGE_TIME_ZONE).date()


def session_close_snapshot(
    snapshot: RealtimePortalSnapshot,
    *,
    source_manifest: Mapping[str, object] | None = None,
    payload: Mapping[str, object] | None = None,
) -> RealtimePortalSnapshot:
    """The self-contained session-close variant: same content (or the supplied embedded
    content), distinct identity, because the catalog keeps one class per snapshot id."""
    manifest = snapshot.source_manifest if source_manifest is None else source_manifest
    body = snapshot.payload if payload is None else payload
    manifest_sha256 = canonical_sha256(manifest)
    payload_sha256 = canonical_sha256(body)
    return replace(
        snapshot,
        snapshot_id=uuid5(
            NAMESPACE_URL,
            ":".join((
                "realtime-portal-session-close", str(snapshot.cohort_id),
                snapshot.snapshot_type, snapshot.snapshot_key, manifest_sha256, payload_sha256,
            )),
        ),
        source_manifest=manifest,
        source_manifest_sha256=manifest_sha256,
        payload=body,
        payload_sha256=payload_sha256,
    )


def _stored_row(stored, names: Sequence[str]):
    if stored is None:
        return None
    if isinstance(stored, Mapping):
        stored = tuple(stored[name] for name in names)
    return tuple(str(value) if isinstance(value, UUID) else value for value in stored)


def load_portal_release_pointer_cursor(
    cursor,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    for_update: bool = False,
) -> PortalReleasePointer | None:
    if not isinstance(provider_tier, ProviderTier):
        raise TypeError("portal pointer provider tier is invalid")
    cursor.execute(
        "SELECT release_id::text,cutoff,session_date,"
        "publication_generation,desired_snapshot_sha256,"
        "membership_sha256 "
        "FROM rt.portal_release_current WHERE cohort_id=%s "
        "AND provider_tier=%s"
        + (" FOR UPDATE" if for_update else ""),
        (str(cohort_id), provider_tier.value),
    )
    stored = _stored_row(
        cursor.fetchone(),
        (
            "release_id",
            "cutoff",
            "session_date",
            "publication_generation",
            "desired_snapshot_sha256",
            "membership_sha256",
        ),
    )
    if stored is None:
        return None
    return PortalReleasePointer(
        cohort_id,
        provider_tier,
        UUID(str(stored[0])),
        stored[1],
        stored[2],
        None if stored[3] is None else int(stored[3]),
        stored[4],
        stored[5],
    )


def restore_portal_release_pointer_cursor(
    cursor,
    *,
    cohort_id: UUID,
    provider_tier: ProviderTier,
    expected_current_release_id: UUID,
    previous: PortalReleasePointer | None,
) -> None:
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"portal-release:{cohort_id}:{provider_tier.value}",),
    )
    current = load_portal_release_pointer_cursor(
        cursor,
        cohort_id=cohort_id,
        provider_tier=provider_tier,
        for_update=True,
    )
    if current is None or current.release_id != expected_current_release_id:
        raise ValueError("portal pointer changed before rollback")
    if previous is None:
        cursor.execute(
            "DELETE FROM rt.portal_release_current "
            "WHERE cohort_id=%s AND provider_tier=%s "
            "AND release_id=%s",
            (
                str(cohort_id),
                provider_tier.value,
                str(expected_current_release_id),
            ),
        )
    else:
        if (
            previous.cohort_id != cohort_id
            or previous.provider_tier is not provider_tier
        ):
            raise ValueError("prior portal pointer scope changed")
        cursor.execute(
            "UPDATE rt.portal_release_current SET "
            "release_id=%s,cutoff=%s,session_date=%s,"
            "publication_generation=%s,desired_snapshot_sha256=%s,"
            "membership_sha256=%s,"
            "updated_at=clock_timestamp() "
            "WHERE cohort_id=%s AND provider_tier=%s "
            "AND release_id=%s",
            (
                str(previous.release_id),
                previous.cutoff,
                previous.session_date,
                previous.publication_generation,
                previous.desired_snapshot_sha256,
                previous.membership_sha256,
                str(cohort_id),
                provider_tier.value,
                str(expected_current_release_id),
            ),
        )
    if cursor.rowcount != 1:
        raise ValueError("portal pointer rollback was not exact")


def _write_snapshot(
    cursor,
    snapshot: RealtimePortalSnapshot,
    snapshot_class: str,
    session_date: date,
) -> None:
    expected = (
        str(snapshot.snapshot_id), str(snapshot.cohort_id), snapshot.provider_tier.value,
        snapshot_class, snapshot.snapshot_type, snapshot.snapshot_key,
        snapshot.source_generation, snapshot.source_manifest_sha256, snapshot.payload_sha256,
    )
    manifest_json = canonical_json(snapshot.source_manifest)
    if snapshot_class == SNAPSHOT_CLASS_EPHEMERAL and len(
        json.dumps(json.loads(manifest_json), ensure_ascii=False, sort_keys=True).encode("utf-8")
    ) > HOT_MANIFEST_LIMIT_BYTES:
        raise ValueError("portal snapshot manifest exceeds the hot lineage-head limit")
    cursor.execute(
        "INSERT INTO rt.snapshot_catalog ("
        "snapshot_id, cohort_id, provider_tier, snapshot_class, snapshot_type, snapshot_key, "
        "session_date, source_generation, manifest_sha256, payload_sha256, generated_at, "
        "valid_until) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (snapshot_id) DO NOTHING",
        (
            str(snapshot.snapshot_id), str(snapshot.cohort_id), snapshot.provider_tier.value,
            snapshot_class, snapshot.snapshot_type, snapshot.snapshot_key, session_date,
            snapshot.source_generation, snapshot.source_manifest_sha256,
            snapshot.payload_sha256, snapshot.generated_at, snapshot.valid_until,
        ),
    )
    cursor.execute(
        "SELECT snapshot_id, cohort_id, provider_tier, snapshot_class, snapshot_type, "
        "snapshot_key, source_generation, manifest_sha256, payload_sha256, generated_at, "
        "session_date FROM rt.snapshot_catalog WHERE snapshot_id=%s",
        (str(snapshot.snapshot_id),),
    )
    stored = _stored_row(cursor.fetchone(), (
        "snapshot_id", "cohort_id", "provider_tier", "snapshot_class", "snapshot_type",
        "snapshot_key", "source_generation", "manifest_sha256", "payload_sha256",
        "generated_at", "session_date",
    ))
    if stored is None or stored[:9] != expected:
        raise ValueError("persisted portal snapshot identity conflict")
    stored_generated_at, stored_session_date = stored[9], stored[10]
    if snapshot_class == SNAPSHOT_CLASS_EPHEMERAL:
        cursor.execute(
            "INSERT INTO rt.portal_snapshot_hot ("
            "snapshot_id, generated_at, cohort_id, snapshot_type, snapshot_key, "
            "source_manifest, payload) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb) "
            "ON CONFLICT (snapshot_id, generated_at) DO NOTHING",
            (
                str(snapshot.snapshot_id), stored_generated_at, str(snapshot.cohort_id),
                snapshot.snapshot_type, snapshot.snapshot_key, manifest_json,
                canonical_json(snapshot.payload),
            ),
        )
    else:
        cursor.execute(
            "INSERT INTO rt.portal_snapshot_kept ("
            "snapshot_id, session_date, cohort_id, snapshot_type, snapshot_key, "
            "source_manifest, payload) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb) "
            "ON CONFLICT (snapshot_id, session_date) DO NOTHING",
            (
                str(snapshot.snapshot_id), stored_session_date, str(snapshot.cohort_id),
                snapshot.snapshot_type, snapshot.snapshot_key, manifest_json,
                canonical_json(snapshot.payload),
            ),
        )


def _persist_portal_snapshot(
    connection,
    snapshot: RealtimePortalSnapshot,
    *,
    repository_cursor,
    retention: str = RETENTION_HOT,
    session_date: date | None = None,
    kept_snapshot: RealtimePortalSnapshot | None = None,
) -> RealtimePortalSnapshot | None:
    if (connection is None) == (repository_cursor is None):
        raise ValueError("portal snapshot persistence requires one transaction owner")
    if retention not in {RETENTION_HOT, RETENTION_KEPT, RETENTION_HOT_AND_KEPT}:
        raise ValueError("portal snapshot retention is invalid")
    if kept_snapshot is not None and retention != RETENTION_HOT_AND_KEPT:
        raise ValueError("a kept variant requires hot and kept retention")
    session = portal_session_date(snapshot, session_date)
    kept = None
    if retention == RETENTION_HOT_AND_KEPT:
        kept = kept_snapshot or session_close_snapshot(snapshot)
        if kept.snapshot_id == snapshot.snapshot_id or (
            kept.cohort_id, kept.provider_tier, kept.snapshot_type, kept.snapshot_key,
            kept.source_generation, kept.generated_at,
        ) != (
            snapshot.cohort_id, snapshot.provider_tier, snapshot.snapshot_type,
            snapshot.snapshot_key, snapshot.source_generation, snapshot.generated_at,
        ):
            raise ValueError("portal session-close variant must keep the snapshot identity and clock")
    elif retention == RETENTION_KEPT:
        kept = snapshot
    try:
        with (
            connection.cursor()
            if connection is not None
            else nullcontext(repository_cursor)
        ) as cursor:
            if retention != RETENTION_KEPT:
                _write_snapshot(cursor, snapshot, SNAPSHOT_CLASS_EPHEMERAL, session)
            if kept is not None:
                _write_snapshot(cursor, kept, SNAPSHOT_CLASS_SESSION_CLOSE, session)
        if connection is not None:
            connection.commit()
    except Exception:
        if connection is not None:
            connection.rollback()
        raise
    return kept


def persist_portal_snapshot_cursor(
    cursor,
    snapshot: RealtimePortalSnapshot,
    *,
    retention: str = RETENTION_HOT,
    session_date: date | None = None,
    kept_snapshot: RealtimePortalSnapshot | None = None,
) -> RealtimePortalSnapshot | None:
    """Writes the snapshot body into its retention class; returns the kept variant, if any.

    Visibility is a separate step: a snapshot becomes current only through a release."""
    return _persist_portal_snapshot(
        None, snapshot, repository_cursor=cursor, retention=retention,
        session_date=session_date, kept_snapshot=kept_snapshot,
    )


def persist_portal_snapshot(
    connection,
    snapshot: RealtimePortalSnapshot,
    *,
    retention: str = RETENTION_HOT,
    session_date: date | None = None,
    kept_snapshot: RealtimePortalSnapshot | None = None,
) -> RealtimePortalSnapshot | None:
    return _persist_portal_snapshot(
        connection, snapshot, repository_cursor=None, retention=retention,
        session_date=session_date, kept_snapshot=kept_snapshot,
    )


_PUBLICATION_WORK_COLUMNS = (
    "work_id",
    "product_key",
    "cohort_id",
    "provider_tier",
    "source_receipt_id",
    "source_receipt_sha256",
    "release_class",
    "session_date",
    "cutoff",
    "publication_generation",
    "desired_revision_id",
    "desired_snapshot_sha256",
    "membership_sha256",
    "snapshot_ids",
    "kept_snapshot_ids",
    "payload_sha256",
    "work_document",
    "state",
    "disposition",
    "deliverable",
    "attempts",
    "next_attempt_at",
    "lease_owner",
    "lease_epoch",
    "lease_until",
    "failure_reason",
    "release_id",
    "release_cutoff",
    "completed_at",
    "created_at",
    "updated_at",
)


def _publication_work_row(row) -> dict[str, object] | None:
    if row is None:
        return None
    return (
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(_PUBLICATION_WORK_COLUMNS, row, strict=True))
    )


def _publication_identity(
    receipt: PortalReleaseReceipt,
    snapshots: Sequence[RealtimePortalSnapshot],
) -> tuple[int, UUID | None, str | None, str | None, bool]:
    generation = max(row.source_generation for row in snapshots)
    if (
        receipt.source_generation is not None
        and receipt.source_generation != generation
    ):
        raise ValueError(
            "portal publication generation changed from its source receipt"
        )

    def identity(name: str, receipt_value):
        values = {
            row.source_manifest.get(name)
            for row in snapshots
            if row.source_manifest.get(name) is not None
        }
        if receipt_value is not None:
            values.add(
                str(receipt_value)
                if isinstance(receipt_value, UUID)
                else receipt_value
            )
        if len(values) > 1:
            raise ValueError(
                f"portal publication {name} identity is ambiguous"
            )
        return next(iter(values), None)

    desired_revision = identity(
        "desired_revision_id",
        receipt.desired_revision_id,
    )
    desired_snapshot = identity(
        "desired_snapshot_sha256",
        receipt.desired_snapshot_sha256,
    )
    membership = identity(
        "membership_sha256",
        receipt.membership_sha256,
    )
    deliverable = all(
        bool(
            row.source_manifest.get(
                "deliverable",
                row.payload.get("deliverable", True),
            )
        )
        for row in snapshots
    )
    return (
        generation,
        None if desired_revision is None else UUID(str(desired_revision)),
        None if desired_snapshot is None else str(desired_snapshot),
        None if membership is None else str(membership),
        deliverable,
    )


def enqueue_portal_publication_work_cursor(
    cursor,
    receipt: PortalReleaseReceipt,
    snapshots: Sequence[RealtimePortalSnapshot],
    *,
    product_key: str,
    created_at: datetime,
    kept_snapshots: Sequence[RealtimePortalSnapshot] = (),
    expected_ticker_keys: Sequence[str] | None = None,
    release_class: str = RELEASE_CLASS_INTRADAY,
    session_date: date | None = None,
) -> Mapping[str, object]:
    published = tuple(snapshots)
    kept = tuple(kept_snapshots)
    if not published:
        raise ValueError("portal publication work requires snapshots")
    key = product_key.strip().upper()
    if not key or len(key) > 64:
        raise ValueError("portal publication product key is invalid")
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("portal publication work clock must be timezone-aware")
    created = created_at.astimezone(timezone.utc)
    if release_class not in {
        RELEASE_CLASS_INTRADAY,
        RELEASE_CLASS_SESSION_CLOSE,
    }:
        raise ValueError("portal publication work release class is invalid")
    if any(
        row.cohort_id != receipt.cohort_id
        or row.provider_tier is not receipt.provider_tier
        for row in (*published, *kept)
    ):
        raise ValueError(
            "portal publication work conflicts with its source receipt"
        )
    (
        generation,
        desired_revision_id,
        desired_snapshot_sha256,
        membership_sha256,
        deliverable,
    ) = _publication_identity(receipt, published)
    release_session = (
        session_date
        or receipt.cutoff.astimezone(_EXCHANGE_TIME_ZONE).date()
    )
    replacement_ticker_keys = (
        None
        if expected_ticker_keys is None
        else tuple(sorted(set(expected_ticker_keys)))
    )
    if replacement_ticker_keys is not None:
        if (
            not replacement_ticker_keys
            or len(replacement_ticker_keys) != len(expected_ticker_keys)
        ):
            raise ValueError(
                "portal publication replacement ticker membership is invalid"
            )
        actual_ticker_keys = {
            row.snapshot_key
            for row in published
            if row.snapshot_type == "TICKER_PAGE"
        }
        if actual_ticker_keys != set(replacement_ticker_keys):
            raise ValueError(
                "portal publication replacement ticker membership changed"
            )
    work_document = {
        "contract": "portal-publication-work-v1",
        "product_key": key,
        "cohort_id": str(receipt.cohort_id),
        "provider_tier": receipt.provider_tier.value,
        "source_receipt_id": str(receipt.receipt_id),
        "source_receipt_sha256": receipt.source_receipt_sha256,
        "universe_sha256": receipt.universe_sha256,
        "release_class": release_class,
        "session_date": release_session.isoformat(),
        "cutoff": receipt.cutoff.isoformat(),
        "publication_generation": generation,
        "desired_revision_id": (
            None
            if desired_revision_id is None
            else str(desired_revision_id)
        ),
        "desired_snapshot_sha256": desired_snapshot_sha256,
        "membership_sha256": membership_sha256,
        "deliverable": deliverable,
        "replacement_ticker_keys": (
            None
            if replacement_ticker_keys is None
            else list(replacement_ticker_keys)
        ),
        "snapshots": [{
            "snapshot_id": str(row.snapshot_id),
            "snapshot_type": row.snapshot_type,
            "snapshot_key": row.snapshot_key,
            "source_generation": row.source_generation,
            "source_manifest_sha256": row.source_manifest_sha256,
            "payload_sha256": row.payload_sha256,
            "generated_at": row.generated_at.isoformat(),
        } for row in published],
        "kept_snapshot_ids": [
            str(row.snapshot_id) for row in kept
        ],
    }
    payload_sha256 = canonical_sha256(work_document)
    work_id = uuid5(
        NAMESPACE_URL,
        (
            "portal-publication-work:"
            f"{key}:{receipt.receipt_id}:{release_class}"
        ),
    )
    cursor.execute(
        "INSERT INTO rt.portal_publication_work("
        "work_id,product_key,cohort_id,provider_tier,source_receipt_id,"
        "source_receipt_sha256,release_class,session_date,cutoff,"
        "publication_generation,desired_revision_id,"
        "desired_snapshot_sha256,membership_sha256,snapshot_ids,"
        "kept_snapshot_ids,payload_sha256,work_document,state,"
        "disposition,deliverable,next_attempt_at) VALUES("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::uuid[],"
        "%s::uuid[],%s,%s::jsonb,'PENDING','PENDING',%s,%s) "
        "ON CONFLICT (work_id) DO NOTHING",
        (
            str(work_id),
            key,
            str(receipt.cohort_id),
            receipt.provider_tier.value,
            str(receipt.receipt_id),
            receipt.source_receipt_sha256,
            release_class,
            release_session,
            receipt.cutoff,
            generation,
            (
                None
                if desired_revision_id is None
                else str(desired_revision_id)
            ),
            desired_snapshot_sha256,
            membership_sha256,
            [str(row.snapshot_id) for row in published],
            [str(row.snapshot_id) for row in kept],
            payload_sha256,
            canonical_json(work_document),
            deliverable,
            created,
        ),
    )
    cursor.execute(
        "SELECT " + ",".join(_PUBLICATION_WORK_COLUMNS) + " "
        "FROM rt.portal_publication_work "
        "WHERE product_key=%s AND source_receipt_id=%s "
        "AND release_class=%s",
        (key, str(receipt.receipt_id), release_class),
    )
    stored = _publication_work_row(cursor.fetchone())
    if (
        stored is None
        or str(stored["work_id"]) != str(work_id)
        or stored["payload_sha256"] != payload_sha256
        or stored["work_document"] != work_document
    ):
        raise ValueError(
            "portal publication work identity changed on replay"
        )
    return stored


def load_portal_publication_work_cursor(
    cursor,
    *,
    product_key: str,
    source_receipt_id: UUID,
    release_class: str = RELEASE_CLASS_INTRADAY,
) -> Mapping[str, object] | None:
    cursor.execute(
        "SELECT " + ",".join(_PUBLICATION_WORK_COLUMNS) + " "
        "FROM rt.portal_publication_work "
        "WHERE product_key=%s AND source_receipt_id=%s "
        "AND release_class=%s",
        (
            product_key.strip().upper(),
            str(source_receipt_id),
            release_class,
        ),
    )
    return _publication_work_row(cursor.fetchone())


def claim_portal_publication_work_cursor(
    cursor,
    *,
    work_id: UUID,
    owner_id: UUID,
    owner_epoch: int,
    claimed_at: datetime,
    lease_until: datetime,
) -> Mapping[str, object] | None:
    claimed = claimed_at.astimezone(timezone.utc)
    expiry = lease_until.astimezone(timezone.utc)
    if (
        claimed_at.tzinfo is None
        or claimed_at.utcoffset() is None
        or lease_until.tzinfo is None
        or lease_until.utcoffset() is None
        or type(owner_epoch) is not int
        or owner_epoch <= 0
        or expiry <= claimed
    ):
        raise ValueError("portal publication claim is invalid")
    cursor.execute(
        "UPDATE rt.portal_publication_work SET "
        "state='CLAIMED',disposition='PENDING',attempts=attempts+1,"
        "lease_owner=%s,lease_epoch=CASE "
        "WHEN state='CLAIMED' THEN GREATEST(lease_epoch+1,%s) "
        "ELSE %s END,lease_until=%s,"
        "failure_reason=NULL,updated_at=clock_timestamp() "
        "WHERE work_id=%s AND ("
        "(state IN ('PENDING','RETRY') AND next_attempt_at<=%s "
        "AND (lease_until IS NULL OR lease_until<=%s)) "
        "OR (state='CLAIMED' AND lease_until<=%s)) "
        "RETURNING " + ",".join(_PUBLICATION_WORK_COLUMNS),
        (
            str(owner_id),
            owner_epoch,
            owner_epoch,
            expiry,
            str(work_id),
            claimed,
            claimed,
            claimed,
        ),
    )
    return _publication_work_row(cursor.fetchone())


def load_portal_publication_snapshots_cursor(
    cursor,
    work: Mapping[str, object],
    *,
    kept: bool = False,
) -> tuple[RealtimePortalSnapshot, ...]:
    identity_key = "kept_snapshot_ids" if kept else "snapshot_ids"
    retained_ids = work[identity_key]
    if isinstance(retained_ids, str):
        body = retained_ids.strip()
        if not body.startswith("{") or not body.endswith("}"):
            raise ValueError(
                "portal publication snapshot array is invalid"
            )
        snapshot_ids = tuple(
            value.strip().strip('"')
            for value in body[1:-1].split(",")
            if value.strip()
        )
    else:
        snapshot_ids = tuple(str(value) for value in retained_ids)
    if not snapshot_ids:
        return ()
    cursor.execute(
        "SELECT catalog.snapshot_id::text,catalog.cohort_id::text,"
        "catalog.provider_tier,catalog.snapshot_type,catalog.snapshot_key,"
        "catalog.source_generation,"
        "catalog.manifest_sha256 AS source_manifest_sha256,"
        "COALESCE(hot.source_manifest,kept.source_manifest) "
        "AS source_manifest,"
        "catalog.payload_sha256,"
        "COALESCE(hot.payload,kept.payload) AS payload,"
        "catalog.generated_at,catalog.valid_until "
        "FROM rt.snapshot_catalog AS catalog "
        "LEFT JOIN rt.portal_snapshot_hot AS hot "
        "ON hot.snapshot_id=catalog.snapshot_id "
        "AND hot.generated_at=catalog.generated_at "
        "LEFT JOIN rt.portal_snapshot_kept AS kept "
        "ON kept.snapshot_id=catalog.snapshot_id "
        "AND kept.session_date=catalog.session_date "
        "WHERE catalog.snapshot_id=ANY(%s::uuid[])",
        (list(snapshot_ids),),
    )
    names = (
        "snapshot_id",
        "cohort_id",
        "provider_tier",
        "snapshot_type",
        "snapshot_key",
        "source_generation",
        "source_manifest_sha256",
        "source_manifest",
        "payload_sha256",
        "payload",
        "generated_at",
        "valid_until",
    )
    rows = {
        str(values["snapshot_id"]): values
        for values in (
            dict(row)
            if isinstance(row, Mapping)
            else dict(zip(names, row, strict=True))
            for row in cursor.fetchall()
        )
    }
    if set(rows) != set(snapshot_ids):
        raise ValueError(
            "portal publication durable snapshot body is unavailable"
        )
    result = []
    for snapshot_id in snapshot_ids:
        row = rows[snapshot_id]
        snapshot = RealtimePortalSnapshot(
            snapshot_id=UUID(snapshot_id),
            cohort_id=UUID(str(row["cohort_id"])),
            provider_tier=ProviderTier(row["provider_tier"]),
            snapshot_type=row["snapshot_type"],
            snapshot_key=row["snapshot_key"],
            source_generation=int(row["source_generation"]),
            source_manifest_sha256=row["source_manifest_sha256"],
            source_manifest=row["source_manifest"],
            payload_sha256=row["payload_sha256"],
            payload=row["payload"],
            generated_at=row["generated_at"],
            valid_until=row["valid_until"],
        )
        if (
            canonical_sha256(snapshot.source_manifest)
            != snapshot.source_manifest_sha256
            or canonical_sha256(snapshot.payload)
            != snapshot.payload_sha256
        ):
            raise ValueError(
                "portal publication durable snapshot identity changed"
            )
        result.append(snapshot)
    return tuple(result)


def _record_portal_publication_receipt_cursor(
    cursor,
    work: Mapping[str, object],
    *,
    owner_id: UUID,
    owner_epoch: int,
    outcome: str,
    recorded_at: datetime,
    failure_reason: str | None = None,
    retry_at: datetime | None = None,
    release: PortalRelease | None = None,
    previous_release_id: UUID | None = None,
    current_release_id: UUID | None = None,
) -> None:
    attempt = int(work["attempts"])
    receipt_id = uuid5(
        NAMESPACE_URL,
        f"portal-publication-receipt:{work['work_id']}:{attempt}",
    )
    cursor.execute(
        "INSERT INTO rt.portal_publication_receipts("
        "publication_receipt_id,work_id,attempt,owner_id,owner_epoch,"
        "payload_sha256,outcome,failure_reason,retry_at,release_id,"
        "release_cutoff,previous_release_id,current_release_id,recorded_at"
        ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (publication_receipt_id) DO NOTHING",
        (
            str(receipt_id),
            str(work["work_id"]),
            attempt,
            str(owner_id),
            owner_epoch,
            work["payload_sha256"],
            outcome,
            failure_reason,
            retry_at,
            None if release is None else str(release.release_id),
            None if release is None else release.cutoff,
            (
                None
                if previous_release_id is None
                else str(previous_release_id)
            ),
            (
                None
                if current_release_id is None
                else str(current_release_id)
            ),
            recorded_at,
        ),
    )
    cursor.execute(
        "SELECT payload_sha256,outcome,failure_reason,retry_at,"
        "release_id::text,release_cutoff,previous_release_id::text,"
        "current_release_id::text "
        "FROM rt.portal_publication_receipts "
        "WHERE publication_receipt_id=%s",
        (str(receipt_id),),
    )
    retained = _stored_row(
        cursor.fetchone(),
        (
            "payload_sha256",
            "outcome",
            "failure_reason",
            "retry_at",
            "release_id",
            "release_cutoff",
            "previous_release_id",
            "current_release_id",
        ),
    )
    expected = (
        work["payload_sha256"],
        outcome,
        failure_reason,
        retry_at,
        None if release is None else str(release.release_id),
        None if release is None else release.cutoff,
        None if previous_release_id is None else str(previous_release_id),
        None if current_release_id is None else str(current_release_id),
    )
    if retained != expected:
        raise ValueError(
            "portal publication receipt identity changed on replay"
        )


def complete_portal_publication_work_cursor(
    cursor,
    work: Mapping[str, object],
    release: PortalRelease,
    *,
    owner_id: UUID,
    owner_epoch: int,
    completed_at: datetime,
    previous_release_id: UUID | None,
    current_release_id: UUID | None,
) -> None:
    disposition = release.publication_disposition
    cursor.execute(
        "UPDATE rt.portal_publication_work SET "
        "state='COMPLETE',disposition=%s,deliverable=%s,"
        "lease_owner=NULL,lease_until=NULL,failure_reason=NULL,"
        "release_id=%s,release_cutoff=%s,completed_at=%s,"
        "updated_at=clock_timestamp() "
        "WHERE work_id=%s AND state='CLAIMED' "
        "AND lease_owner=%s AND lease_epoch=%s RETURNING work_id",
        (
            disposition,
            release.deliverable,
            str(release.release_id),
            release.cutoff,
            completed_at,
            str(work["work_id"]),
            str(owner_id),
            owner_epoch,
        ),
    )
    retained = cursor.fetchone()
    retained_id = (
        None
        if retained is None
        else retained.get("work_id")
        if isinstance(retained, Mapping)
        else retained[0]
    )
    if retained_id is None or str(retained_id) != str(work["work_id"]):
        raise ValueError(
            "portal publication completion lost its work lease"
        )
    _record_portal_publication_receipt_cursor(
        cursor,
        work,
        owner_id=owner_id,
        owner_epoch=owner_epoch,
        outcome=disposition,
        recorded_at=completed_at,
        release=release,
        previous_release_id=previous_release_id,
        current_release_id=current_release_id,
    )


def fail_portal_publication_work_cursor(
    cursor,
    work: Mapping[str, object],
    *,
    owner_id: UUID,
    owner_epoch: int,
    failed_at: datetime,
    next_attempt_at: datetime,
    failure_reason: str,
    retryable: bool = True,
) -> str:
    reason = failure_reason.strip().upper()
    if not reason or len(reason) > 160:
        raise ValueError(
            "portal publication failure reason must contain 1-160 characters"
        )
    state = "RETRY" if retryable else "TERMINAL"
    disposition = "FAILED_RETRY" if retryable else "FAILED_TERMINAL"
    cursor.execute(
        "UPDATE rt.portal_publication_work SET "
        "state=%s,disposition=%s,lease_owner=NULL,lease_until=NULL,"
        "failure_reason=%s,next_attempt_at=%s,"
        "completed_at=CASE WHEN %s THEN NULL ELSE %s END,"
        "updated_at=clock_timestamp() "
        "WHERE work_id=%s AND state='CLAIMED' "
        "AND lease_owner=%s AND lease_epoch=%s RETURNING work_id",
        (
            state,
            disposition,
            reason,
            next_attempt_at,
            retryable,
            failed_at,
            str(work["work_id"]),
            str(owner_id),
            owner_epoch,
        ),
    )
    retained = cursor.fetchone()
    retained_id = (
        None
        if retained is None
        else retained.get("work_id")
        if isinstance(retained, Mapping)
        else retained[0]
    )
    if retained_id is None or str(retained_id) != str(work["work_id"]):
        raise ValueError(
            "portal publication failure lost its work lease"
        )
    _record_portal_publication_receipt_cursor(
        cursor,
        work,
        owner_id=owner_id,
        owner_epoch=owner_epoch,
        outcome=disposition,
        recorded_at=failed_at,
        failure_reason=reason,
        retry_at=next_attempt_at if retryable else None,
    )
    return state


def _release_entry(snapshot: RealtimePortalSnapshot, stored) -> dict[str, object]:
    return {
        "snapshot_id": str(snapshot.snapshot_id),
        "snapshot_class": stored[1],
        "snapshot_type": snapshot.snapshot_type,
        "snapshot_key": snapshot.snapshot_key,
        "source_generation": snapshot.source_generation,
        "generated_at": stored[4].astimezone(timezone.utc).isoformat(),
    }


def _entry_rank(entry: Mapping[str, object]) -> tuple[int, datetime]:
    return (
        int(entry["source_generation"]),
        datetime.fromisoformat(str(entry["generated_at"])),
    )


def _publish_portal_release(
    cursor,
    receipt: PortalReleaseReceipt,
    snapshots: Sequence[RealtimePortalSnapshot],
    release_class: str,
    session_date: date | None,
    replacement_ticker_keys: Sequence[str] | None,
) -> PortalRelease:
    if release_class not in {RELEASE_CLASS_INTRADAY, RELEASE_CLASS_SESSION_CLOSE}:
        raise ValueError("portal release class is invalid")
    published = tuple(snapshots)
    if not published:
        raise ValueError("portal release requires at least one snapshot")
    identities = tuple((row.snapshot_type, row.snapshot_key) for row in published)
    if len(identities) != len(set(identities)):
        raise ValueError("portal release snapshot identities must be unique")
    if any(
        row.cohort_id != receipt.cohort_id or row.provider_tier is not receipt.provider_tier
        for row in published
    ):
        raise ValueError("portal release snapshots conflict with the receipt identity")
    if release_class == RELEASE_CLASS_SESSION_CLOSE and session_date is None:
        raise ValueError("a session-close release requires its session date")
    replacement_keys = (
        None
        if replacement_ticker_keys is None
        else tuple(sorted(set(replacement_ticker_keys)))
    )
    if replacement_keys is not None:
        if (
            not replacement_keys
            or len(replacement_keys) != len(replacement_ticker_keys)
            or {
                row.snapshot_key
                for row in published
                if row.snapshot_type == "TICKER_PAGE"
            }
            != set(replacement_keys)
        ):
            raise ValueError(
                "portal release replacement ticker membership changed"
            )
    release_session = session_date or receipt.cutoff.astimezone(_EXCHANGE_TIME_ZONE).date()
    (
        publication_generation,
        desired_revision_id,
        desired_snapshot_sha256,
        membership_sha256,
        deliverable,
    ) = _publication_identity(receipt, published)
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"portal-release:{receipt.cohort_id}:{receipt.provider_tier.value}",),
    )
    cursor.execute(
        "SELECT snapshot_id, snapshot_class, manifest_sha256, payload_sha256, generated_at "
        "FROM rt.snapshot_catalog WHERE snapshot_id=ANY(%s::uuid[])",
        ([str(row.snapshot_id) for row in published],),
    )
    catalog = {}
    for row in cursor.fetchall():
        values = _stored_row(row, (
            "snapshot_id", "snapshot_class", "manifest_sha256", "payload_sha256", "generated_at",
        ))
        catalog[values[0]] = values
    entries: dict[tuple[str, str], dict[str, object]] = {}
    for row in published:
        stored = catalog.get(str(row.snapshot_id))
        if (
            stored is None
            or stored[2] != row.source_manifest_sha256
            or stored[3] != row.payload_sha256
            or (release_class == RELEASE_CLASS_SESSION_CLOSE and stored[1] != SNAPSHOT_CLASS_SESSION_CLOSE)
        ):
            raise ValueError("portal release names a snapshot that is not persisted as published")
        entries[(row.snapshot_type, row.snapshot_key)] = _release_entry(row, stored)
    previous_release_id = None
    previous_order = None
    previous_manifest = None
    previous_receipt_id = None
    if release_class == RELEASE_CLASS_INTRADAY:
        cursor.execute(
            "SELECT pointer.release_id::text,pointer.session_date,"
            "pointer.cutoff,COALESCE(pointer.publication_generation,"
            "release.publication_generation,0) AS publication_generation,"
            "release.receipt_id::text,"
            "release.desired_snapshot_sha256,release.manifest "
            "FROM rt.portal_release_current AS pointer "
            "JOIN rt.portal_releases AS release ON release.release_id=pointer.release_id "
            "AND release.cutoff=pointer.cutoff WHERE pointer.cohort_id=%s "
            "AND pointer.provider_tier=%s FOR UPDATE OF pointer",
            (str(receipt.cohort_id), receipt.provider_tier.value),
        )
        previous = cursor.fetchone()
        if previous is not None:
            values = (
                tuple(previous[name] for name in (
                    "release_id",
                    "session_date",
                    "cutoff",
                    "publication_generation",
                    "receipt_id",
                    "desired_snapshot_sha256",
                    "manifest",
                ))
                if isinstance(previous, Mapping)
                else tuple(previous)
            )
            (
                previous_release_id,
                previous_session,
                previous_cutoff,
                previous_generation,
                previous_receipt_id,
                previous_desired_snapshot,
                previous_manifest,
            ) = values
            previous_order = (
                previous_session,
                previous_cutoff,
                int(previous_generation),
            )
            if (
                previous_session == release_session
                and previous_desired_snapshot is not None
                and desired_snapshot_sha256 is not None
                and previous_desired_snapshot
                != desired_snapshot_sha256
            ):
                raise ValueError(
                    "portal publication desired snapshot changed intraday"
                )
    incoming_order = (
        release_session,
        receipt.cutoff,
        publication_generation,
    )
    rank_backward = (
        previous_order is not None
        and incoming_order < previous_order
    )
    historical_noncurrent = (
        release_class == RELEASE_CLASS_INTRADAY
        and not deliverable
        and not rank_backward
    )
    current_candidate = not rank_backward and not historical_noncurrent
    if previous_manifest is not None and current_candidate:
        for entry in previous_manifest["snapshots"]:
            identity = (entry["snapshot_type"], entry["snapshot_key"])
            if (
                replacement_keys is not None
                and entry["snapshot_type"] == "TICKER_PAGE"
                and entry["snapshot_key"] not in replacement_keys
            ):
                continue
            incoming = entries.get(identity)
            if (
                incoming is not None
                and _entry_rank(entry) == _entry_rank(incoming)
                and entry["snapshot_id"] != incoming["snapshot_id"]
            ):
                raise ValueError(
                    "portal release snapshot identity diverged at one generation"
                )
            if incoming is None or _entry_rank(entry) > _entry_rank(incoming):
                entries[identity] = dict(entry)
        publication_generation = max(
            int(entry["source_generation"])
            for entry in entries.values()
        )
        incoming_order = (
            release_session,
            receipt.cutoff,
            publication_generation,
        )
    source_receipt_ids = {str(receipt.receipt_id)}
    if previous_manifest is not None and current_candidate:
        source_receipt_ids.update(
            str(value)
            for value in previous_manifest.get(
                "source_receipt_ids",
                (previous_receipt_id,),
            )
            if value is not None
        )
    canonical_receipt_id = min(source_receipt_ids)
    disposition = (
        "SESSION_CLOSE"
        if release_class == RELEASE_CLASS_SESSION_CLOSE
        else "HISTORICAL_STALE"
        if rank_backward
        else "HISTORICAL_NONCURRENT"
        if historical_noncurrent
        else "CURRENT"
    )
    if rank_backward or historical_noncurrent:
        deliverable = False
    manifest = {
        "version": RELEASE_MANIFEST_VERSION,
        "release_class": release_class,
        "session_date": release_session.isoformat(),
        "cutoff": receipt.cutoff.isoformat(),
        "publication_generation": publication_generation,
        "desired_revision_id": (
            None
            if desired_revision_id is None
            else str(desired_revision_id)
        ),
        "desired_snapshot_sha256": desired_snapshot_sha256,
        "membership_sha256": membership_sha256,
        "source_receipt_ids": sorted(source_receipt_ids),
        "deliverable": deliverable,
        "replacement_ticker_keys": (
            None if replacement_keys is None else list(replacement_keys)
        ),
        "snapshots": [entries[identity] for identity in sorted(entries)],
    }
    manifest_sha256 = canonical_sha256(manifest)
    release_id = uuid5(
        NAMESPACE_URL,
        ":".join((
            "realtime-portal-release",
            str(receipt.cohort_id),
            receipt.provider_tier.value,
            release_class,
            release_session.isoformat(),
            receipt.cutoff.isoformat(),
            str(publication_generation),
            manifest_sha256,
        )),
    )
    equivalent_current = (
        previous_release_id is not None
        and str(previous_release_id) == str(release_id)
    )
    if equivalent_current:
        disposition = "EQUIVALENT_CURRENT"
    stored_disposition = (
        "CURRENT" if equivalent_current else disposition
    )
    cursor.execute(
        "INSERT INTO rt.portal_releases ("
        "release_id, cohort_id, provider_tier, release_class, receipt_id, session_date, "
        "cutoff, universe_sha256, manifest, manifest_sha256,"
        "publication_generation,desired_revision_id,"
        "desired_snapshot_sha256,membership_sha256,"
        "publication_disposition,deliverable) VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (release_id, cutoff) DO NOTHING",
        (
            str(release_id), str(receipt.cohort_id), receipt.provider_tier.value, release_class,
            canonical_receipt_id, release_session, receipt.cutoff,
            receipt.universe_sha256, canonical_json(manifest), manifest_sha256,
            publication_generation,
            (
                None
                if desired_revision_id is None
                else str(desired_revision_id)
            ),
            desired_snapshot_sha256,
            membership_sha256,
            stored_disposition,
            deliverable,
        ),
    )
    cursor.execute(
        "SELECT release_id, cohort_id, provider_tier, release_class, receipt_id, "
        "session_date, cutoff, universe_sha256, manifest_sha256,"
        "publication_generation,desired_revision_id,"
        "desired_snapshot_sha256,membership_sha256,"
        "publication_disposition,deliverable "
        "FROM rt.portal_releases "
        "WHERE release_id=%s AND cutoff=%s",
        (str(release_id), receipt.cutoff),
    )
    stored_release = _stored_row(cursor.fetchone(), (
        "release_id", "cohort_id", "provider_tier", "release_class", "receipt_id",
        "session_date", "cutoff", "universe_sha256", "manifest_sha256",
        "publication_generation", "desired_revision_id",
        "desired_snapshot_sha256", "membership_sha256",
        "publication_disposition", "deliverable",
    ))
    if stored_release is None or stored_release[:6] != (
        str(release_id), str(receipt.cohort_id), receipt.provider_tier.value, release_class,
        canonical_receipt_id, release_session,
    ) or stored_release[6:] != (
        receipt.cutoff,
        receipt.universe_sha256,
        manifest_sha256,
        publication_generation,
        None if desired_revision_id is None else str(desired_revision_id),
        desired_snapshot_sha256,
        membership_sha256,
        stored_disposition,
        deliverable,
    ):
        raise ValueError("persisted portal release identity conflict")
    swapped = False
    if release_class == RELEASE_CLASS_INTRADAY and current_candidate:
        cursor.execute(
            "INSERT INTO rt.portal_release_current ("
            "cohort_id,provider_tier,release_id,cutoff,session_date,"
            "publication_generation,desired_snapshot_sha256,membership_sha256) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (cohort_id, provider_tier) DO UPDATE SET "
            "release_id=EXCLUDED.release_id, cutoff=EXCLUDED.cutoff, "
            "session_date=EXCLUDED.session_date,"
            "publication_generation=EXCLUDED.publication_generation,"
            "desired_snapshot_sha256=EXCLUDED.desired_snapshot_sha256,"
            "membership_sha256=EXCLUDED.membership_sha256,"
            "updated_at=clock_timestamp() "
            "WHERE (rt.portal_release_current.session_date,"
            "rt.portal_release_current.cutoff,"
            "COALESCE(rt.portal_release_current.publication_generation,0)) "
            "<= (EXCLUDED.session_date,EXCLUDED.cutoff,"
            "EXCLUDED.publication_generation) RETURNING release_id",
            (
                str(receipt.cohort_id), receipt.provider_tier.value, str(release_id),
                receipt.cutoff, release_session, publication_generation,
                desired_snapshot_sha256, membership_sha256,
            ),
        )
        current = cursor.fetchone()
        current_id = (
            None
            if current is None
            else current.get("release_id")
            if isinstance(current, Mapping)
            else current[0]
        )
        if current_id is None or str(current_id) != str(release_id):
            raise ValueError(
                "portal release pointer lost its serialized ordering"
            )
        swapped = True
    return PortalRelease(
        release_id=release_id, cohort_id=receipt.cohort_id, provider_tier=receipt.provider_tier,
        release_class=release_class, receipt_id=UUID(canonical_receipt_id), session_date=release_session,
        cutoff=receipt.cutoff, universe_sha256=receipt.universe_sha256, manifest=manifest,
        manifest_sha256=manifest_sha256, pointer_swapped=swapped,
        publication_generation=publication_generation,
        desired_revision_id=desired_revision_id,
        desired_snapshot_sha256=desired_snapshot_sha256,
        membership_sha256=membership_sha256,
        publication_disposition=disposition,
        deliverable=deliverable,
    )


def publish_portal_release_cursor(
    cursor,
    receipt: PortalReleaseReceipt,
    snapshots: Sequence[RealtimePortalSnapshot],
    *,
    release_class: str = RELEASE_CLASS_INTRADAY,
    session_date: date | None = None,
    replacement_ticker_keys: Sequence[str] | None = None,
) -> PortalRelease:
    """Names persisted snapshots in an immutable release and, for an intraday release,
    swaps the single current pointer in the caller's transaction.

    An intraday release carries forward the previous release's entries that this lane did
    not republish (per type and key, the higher source generation and clock wins), so
    independent publication lanes never drop each other from the current view."""
    return _publish_portal_release(
        cursor,
        receipt,
        snapshots,
        release_class,
        session_date,
        replacement_ticker_keys,
    )


def publish_portal_release(
    connection,
    receipt: PortalReleaseReceipt,
    snapshots: Sequence[RealtimePortalSnapshot],
    *,
    release_class: str = RELEASE_CLASS_INTRADAY,
    session_date: date | None = None,
    replacement_ticker_keys: Sequence[str] | None = None,
) -> PortalRelease:
    try:
        with connection.cursor() as cursor:
            release = _publish_portal_release(
                cursor,
                receipt,
                snapshots,
                release_class,
                session_date,
                replacement_ticker_keys,
            )
        connection.commit()
        return release
    except Exception:
        connection.rollback()
        raise


def load_portal_release_cursor(
    cursor,
    *,
    release_id: UUID,
    cutoff: datetime,
) -> PortalRelease:
    cursor.execute(
        "SELECT release_id::text,cohort_id::text,provider_tier,"
        "release_class,receipt_id::text,session_date,cutoff,"
        "universe_sha256,manifest,manifest_sha256,"
        "publication_generation,desired_revision_id::text,"
        "desired_snapshot_sha256,membership_sha256,"
        "publication_disposition,deliverable "
        "FROM rt.portal_releases WHERE release_id=%s AND cutoff=%s",
        (str(release_id), cutoff),
    )
    names = (
        "release_id",
        "cohort_id",
        "provider_tier",
        "release_class",
        "receipt_id",
        "session_date",
        "cutoff",
        "universe_sha256",
        "manifest",
        "manifest_sha256",
        "publication_generation",
        "desired_revision_id",
        "desired_snapshot_sha256",
        "membership_sha256",
        "publication_disposition",
        "deliverable",
    )
    row = cursor.fetchone()
    retained = (
        None
        if row is None
        else dict(row)
        if isinstance(row, Mapping)
        else dict(zip(names, row, strict=True))
    )
    if retained is None or (
        canonical_sha256(retained["manifest"])
        != retained["manifest_sha256"]
    ):
        raise ValueError("portal publication release is unavailable or changed")
    cursor.execute(
        "SELECT release_id::text FROM rt.portal_release_current "
        "WHERE cohort_id=%s AND provider_tier=%s",
        (retained["cohort_id"], retained["provider_tier"]),
    )
    current = cursor.fetchone()
    current_id = (
        None
        if current is None
        else current.get("release_id")
        if isinstance(current, Mapping)
        else current[0]
    )
    return PortalRelease(
        release_id=UUID(str(retained["release_id"])),
        cohort_id=UUID(str(retained["cohort_id"])),
        provider_tier=ProviderTier(retained["provider_tier"]),
        release_class=retained["release_class"],
        receipt_id=UUID(str(retained["receipt_id"])),
        session_date=retained["session_date"],
        cutoff=retained["cutoff"],
        universe_sha256=retained["universe_sha256"],
        manifest=retained["manifest"],
        manifest_sha256=retained["manifest_sha256"],
        pointer_swapped=(
            current_id is not None
            and str(current_id) == str(retained["release_id"])
        ),
        publication_generation=int(
            retained["publication_generation"] or 1
        ),
        desired_revision_id=(
            None
            if retained["desired_revision_id"] is None
            else UUID(str(retained["desired_revision_id"]))
        ),
        desired_snapshot_sha256=retained[
            "desired_snapshot_sha256"
        ],
        membership_sha256=retained["membership_sha256"],
        publication_disposition=retained[
            "publication_disposition"
        ],
        deliverable=bool(retained["deliverable"]),
    )


def _persist_screening_generation(
    connection,
    generation: RealtimeScreeningGeneration,
    *,
    repository_cursor,
    values_executor=execute_values,
) -> None:
    if (connection is None) == (repository_cursor is None):
        raise ValueError("screening persistence requires one transaction owner")
    expected_generation = (
        str(generation.generation_id),
        str(generation.cohort_id),
        generation.provider_tier.value,
        generation.session_date,
        generation.source_manifest_sha256,
        generation.expected_members,
        len(generation.rows),
        generation.status,
    )
    expected_rows = {
        str(row.security_id): (row.ticker, row.payload_sha256)
        for row in generation.rows
    }
    manifest_json = canonical_json(generation.manifest)
    limit = screening_manifest_limit(generation.expected_members)
    if len(manifest_json) > limit:
        # Defence in depth behind the producer's guard: rt.screening_generations is unbounded until a retention
        # policy is assigned, so a manifest that grows with bars instead of members is never written.
        raise ValueError(
            f"screening generation manifest is {len(manifest_json)} bytes; "
            f"a lineage-head manifest must stay within {limit}"
        )
    try:
        with (
            connection.cursor()
            if connection is not None
            else nullcontext(repository_cursor)
        ) as cursor:
            cursor.execute(
                "INSERT INTO rt.screening_generations ("
                "generation_id, cohort_id, provider_tier, session_date, "
                "source_manifest_sha256, expected_members, actual_members, status, "
                "generated_at, manifest) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s::jsonb) ON CONFLICT (generation_id) DO NOTHING",
                (*expected_generation, generation.generated_at, manifest_json),
            )
            if generation.rows:
                values_executor(
                    cursor,
                    "INSERT INTO rt.screening_rows ("
                    "generation_id, security_id, ticker, eligible, payload_sha256, payload"
                    ") VALUES %s ON CONFLICT (generation_id, security_id) DO NOTHING",
                    [
                        (
                            str(generation.generation_id), str(row.security_id), row.ticker,
                            row.eligible, row.payload_sha256, canonical_json(row.payload),
                        )
                        for row in generation.rows
                    ],
                    page_size=PORTAL_BATCH_PAGE_SIZE,
                )
            cursor.execute(
                "SELECT generation_id, cohort_id, provider_tier, session_date, "
                "source_manifest_sha256, expected_members, actual_members, status "
                "FROM rt.screening_generations WHERE generation_id=%s",
                (str(generation.generation_id),),
            )
            stored_generation = cursor.fetchone()
            if isinstance(stored_generation, Mapping):
                stored_generation = tuple(stored_generation[name] for name in (
                    "generation_id", "cohort_id", "provider_tier", "session_date",
                    "source_manifest_sha256", "expected_members", "actual_members", "status",
                ))
            normalized = tuple(
                str(value) if isinstance(value, UUID) else value
                for value in stored_generation
            ) if stored_generation is not None else None
            if normalized != expected_generation:
                raise ValueError("persisted screening generation identity conflict")
            cursor.execute(
                "SELECT security_id, ticker, payload_sha256 FROM rt.screening_rows "
                "WHERE generation_id=%s",
                (str(generation.generation_id),),
            )
            stored_rows = {
                str(row["security_id"] if isinstance(row, Mapping) else row[0]):
                (row["ticker"], row["payload_sha256"]) if isinstance(row, Mapping) else (row[1], row[2])
                for row in cursor.fetchall()
            }
            if stored_rows != expected_rows:
                raise ValueError("persisted screening row identity conflict")
            cursor.execute(
                "INSERT INTO rt.screening_current ("
                "cohort_id, generation_id, session_date, generated_at) VALUES ("
                "%s, %s, %s, %s) ON CONFLICT (cohort_id) DO UPDATE SET "
                "generation_id=EXCLUDED.generation_id, session_date=EXCLUDED.session_date, "
                "generated_at=EXCLUDED.generated_at, updated_at=clock_timestamp() "
                "WHERE (rt.screening_current.session_date < EXCLUDED.session_date "
                "OR (rt.screening_current.session_date=EXCLUDED.session_date "
                "AND rt.screening_current.generated_at < EXCLUDED.generated_at) "
                "OR (%s AND rt.screening_current.generated_at < EXCLUDED.generated_at "
                "AND EXISTS (SELECT 1 FROM rt.screening_generations AS legacy "
                "WHERE legacy.generation_id=rt.screening_current.generation_id "
                "AND legacy.manifest->>'version' IS DISTINCT FROM %s))) "
                "AND (%s OR NOT EXISTS (SELECT 1 FROM rt.screening_generations AS compatible "
                "WHERE compatible.generation_id=rt.screening_current.generation_id "
                "AND compatible.manifest->>'version'=%s))",
                (
                    str(generation.cohort_id), str(generation.generation_id),
                    generation.session_date, generation.generated_at,
                    generation.manifest.get("version") == SCREENING_VERSION,
                    SCREENING_VERSION,
                    generation.manifest.get("version") == SCREENING_VERSION,
                    SCREENING_VERSION,
                ),
            )
        if connection is not None:
            connection.commit()
    except Exception:
        if connection is not None:
            connection.rollback()
        raise


def persist_screening_generation_cursor(
    cursor,
    generation: RealtimeScreeningGeneration,
    *,
    values_executor=execute_values,
) -> None:
    _persist_screening_generation(
        None,
        generation,
        repository_cursor=cursor,
        values_executor=values_executor,
    )


def persist_screening_generation(
    connection,
    generation: RealtimeScreeningGeneration,
    *,
    values_executor=execute_values,
) -> None:
    _persist_screening_generation(
        connection,
        generation,
        repository_cursor=None,
        values_executor=values_executor,
    )


def _persist_detector_dataset(
    connection,
    dataset: RealtimeDetectorDataset,
    *,
    repository_cursor,
    values_executor=execute_values,
) -> None:
    if (connection is None) == (repository_cursor is None):
        raise ValueError("detector dataset persistence requires one transaction owner")
    expected_dataset = (
        str(dataset.dataset_id),
        str(dataset.cohort_id),
        dataset.provider_tier.value,
        dataset.dataset_key,
        dataset.status,
        dataset.source_manifest_sha256,
    )
    expected_evaluations = {
        str(row.evaluation_id): (str(row.event_id), row.payload_sha256)
        for row in dataset.evaluations
    }
    try:
        with (
            connection.cursor()
            if connection is not None
            else nullcontext(repository_cursor)
        ) as cursor:
            cursor.execute(
                "INSERT INTO rt.detector_datasets ("
                "dataset_id, cohort_id, provider_tier, dataset_key, session_date, status, "
                "detector_ids, source_manifest_sha256, started_at, completed_at, summary) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb) "
                "ON CONFLICT (dataset_id) DO NOTHING",
                (
                    str(dataset.dataset_id), str(dataset.cohort_id),
                    dataset.provider_tier.value, dataset.dataset_key,
                    dataset.session_date, dataset.status, list(dataset.detector_ids),
                    dataset.source_manifest_sha256, dataset.started_at,
                    dataset.completed_at, canonical_json(dataset.summary),
                ),
            )
            if dataset.evaluations:
                values_executor(
                    cursor,
                    "INSERT INTO rt.detector_evaluations ("
                    "evaluation_id, dataset_id, event_id, detector_id, underlyer, "
                    "selection_status, observed_at, payload_sha256, payload) VALUES %s "
                    "ON CONFLICT (evaluation_id) DO NOTHING",
                    [
                        (
                            str(row.evaluation_id), str(dataset.dataset_id),
                            str(row.event_id), row.detector_id, row.underlyer,
                            row.selection_status, row.observed_at, row.payload_sha256,
                            canonical_json(row.payload),
                        )
                        for row in dataset.evaluations
                    ],
                    page_size=PORTAL_BATCH_PAGE_SIZE,
                )
            cursor.execute(
                "SELECT dataset_id, cohort_id, provider_tier, dataset_key, status, "
                "source_manifest_sha256 FROM rt.detector_datasets WHERE dataset_id=%s",
                (str(dataset.dataset_id),),
            )
            stored_dataset = cursor.fetchone()
            normalized = tuple(
                str(value) if isinstance(value, UUID) else value for value in stored_dataset
            ) if stored_dataset is not None else None
            if normalized != expected_dataset:
                raise ValueError("persisted detector dataset identity conflict")
            cursor.execute(
                "SELECT evaluation_id, event_id, payload_sha256 "
                "FROM rt.detector_evaluations WHERE dataset_id=%s",
                (str(dataset.dataset_id),),
            )
            stored_evaluations = {
                str(row[0]): (str(row[1]), row[2]) for row in cursor.fetchall()
            }
            if stored_evaluations != expected_evaluations:
                raise ValueError("persisted detector evaluation identity conflict")
        if connection is not None:
            connection.commit()
    except Exception:
        if connection is not None:
            connection.rollback()
        raise


def persist_detector_dataset_cursor(
    cursor,
    dataset: RealtimeDetectorDataset,
    *,
    values_executor=execute_values,
) -> None:
    _persist_detector_dataset(
        None,
        dataset,
        repository_cursor=cursor,
        values_executor=values_executor,
    )


def persist_detector_dataset(
    connection,
    dataset: RealtimeDetectorDataset,
    *,
    values_executor=execute_values,
) -> None:
    _persist_detector_dataset(
        connection,
        dataset,
        repository_cursor=None,
        values_executor=values_executor,
    )


def persist_management_assessment(
    connection,
    assessment: RealtimeManagementAssessment,
) -> None:
    expected = (
        str(assessment.assessment_id),
        str(assessment.cohort_id),
        str(assessment.alert_id),
        assessment.assessed_at,
        assessment.status,
        assessment.payload_sha256,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rt.alert_management_assessments ("
                "assessment_id, cohort_id, alert_id, assessed_at, status, reason_codes, "
                "payload_sha256, payload) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb) "
                "ON CONFLICT (assessment_id) DO NOTHING",
                (
                    str(assessment.assessment_id), str(assessment.cohort_id),
                    str(assessment.alert_id), assessment.assessed_at, assessment.status,
                    list(assessment.reason_codes), assessment.payload_sha256,
                    canonical_json(assessment.payload),
                ),
            )
            cursor.execute(
                "SELECT assessment_id, cohort_id, alert_id, assessed_at, status, "
                "payload_sha256 FROM rt.alert_management_assessments "
                "WHERE assessment_id=%s",
                (str(assessment.assessment_id),),
            )
            stored = cursor.fetchone()
            normalized = tuple(
                str(value) if isinstance(value, UUID) else value for value in stored
            ) if stored is not None else None
            if normalized != expected:
                raise ValueError("persisted management assessment identity conflict")
            cursor.execute(
                "INSERT INTO rt.alert_management_current ("
                "cohort_id, alert_id, assessment_id, assessed_at) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (cohort_id, alert_id) DO UPDATE SET "
                "assessment_id=EXCLUDED.assessment_id, assessed_at=EXCLUDED.assessed_at, "
                "updated_at=clock_timestamp() "
                "WHERE rt.alert_management_current.assessed_at < EXCLUDED.assessed_at",
                (
                    str(assessment.cohort_id), str(assessment.alert_id),
                    str(assessment.assessment_id), assessment.assessed_at,
                ),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _claim_alert_outbox(
    connection,
    *,
    repository_cursor,
    cohort_id: UUID,
    owner_id: UUID,
    owner_epoch: int,
    claimed_at: datetime,
    lease_until: datetime,
    limit: int,
) -> tuple[dict[str, object], ...]:
    if (connection is None) == (repository_cursor is None):
        raise ValueError("outbox claim requires one transaction owner")
    if claimed_at.tzinfo is None or lease_until.tzinfo is None:
        raise ValueError("outbox claim clocks must be timezone-aware")
    claimed = claimed_at.astimezone(timezone.utc)
    expiry = lease_until.astimezone(timezone.utc)
    if (
        expiry <= claimed
        or type(limit) is not int
        or not 1 <= limit <= 200
    ):
        raise ValueError("outbox claim requires a future lease and limit 1-200")
    resource_key = f"portal-publisher:{cohort_id}"
    try:
        with (
            connection.cursor()
            if connection is not None
            else nullcontext(repository_cursor)
        ) as cursor:
            cursor.execute(
                "SELECT owner_id, owner_epoch, lease_until>%s FROM rt.worker_leases "
                "WHERE resource_key=%s FOR UPDATE",
                (claimed, resource_key),
            )
            lease = cursor.fetchone()
            if lease is None or str(lease[0]) != str(owner_id):
                raise ValueError("publisher lease owner does not match")
            if lease[1] != owner_epoch or not lease[2]:
                raise ValueError("publisher lease is stale or expired")
            cursor.execute(
                "SELECT cohort_id FROM rt.portal_cohort_current "
                "WHERE scope='PLATFORM' AND effective_at<=%s FOR UPDATE",
                (claimed,),
            )
            active = cursor.fetchone()
            if active is None or str(active[0]) != str(cohort_id):
                raise ValueError("publisher cohort is not the active platform cohort")
            cursor.execute(
                "SELECT owner_epoch, status, lease_until>%s FROM rt.readiness_current "
                "WHERE scope_type='COHORT' AND scope_id=%s AND capability='PUBLICATION' "
                "FOR UPDATE",
                (claimed, str(cohort_id)),
            )
            readiness = cursor.fetchone()
            if (
                readiness is None
                or readiness[0] != owner_epoch
                or readiness[1] != "READY"
                or not readiness[2]
            ):
                raise ValueError("publisher readiness is not active for the owner epoch")
            cursor.execute(
                "UPDATE rt.alert_outbox AS outbox SET lease_owner=NULL, lease_until=NULL, "
                "failure_code='ALERT_EXPIRED', abandoned_at=%s "
                "FROM rt.detector_events AS event WHERE outbox.event_id=event.event_id "
                "AND outbox.cohort_id=%s AND outbox.delivered_at IS NULL "
                "AND outbox.abandoned_at IS NULL AND event.valid_until IS NOT NULL "
                "AND event.valid_until<=%s",
                (claimed, str(cohort_id), claimed),
            )
            cursor.execute(
                "WITH pending AS (SELECT outbox.outbox_id FROM rt.alert_outbox AS outbox "
                "JOIN rt.detector_events AS event ON event.event_id=outbox.event_id "
                "WHERE outbox.cohort_id=%s AND outbox.delivered_at IS NULL "
                "AND outbox.abandoned_at IS NULL AND outbox.next_attempt_at<=%s "
                "AND (event.valid_until IS NULL OR event.valid_until>%s) "
                "AND (outbox.lease_until IS NULL OR outbox.lease_until<=%s) "
                "ORDER BY outbox.next_attempt_at, outbox.outbox_id "
                "FOR UPDATE OF outbox SKIP LOCKED LIMIT %s) UPDATE rt.alert_outbox AS outbox SET "
                "lease_owner=%s, lease_epoch=%s, lease_until=%s, attempts=outbox.attempts+1 "
                "FROM pending WHERE outbox.outbox_id=pending.outbox_id RETURNING "
                "outbox.outbox_id, outbox.event_id, outbox.destination, outbox.message_kind, "
                "outbox.payload_sha256, outbox.payload, outbox.attempts",
                (
                    str(cohort_id), claimed, claimed, claimed, limit, str(owner_id),
                    owner_epoch, expiry,
                ),
            )
            rows = tuple(
                dict(row) if isinstance(row, dict) else {
                    "outbox_id": row[0], "event_id": row[1],
                    "destination": row[2], "message_kind": row[3],
                    "payload_sha256": row[4], "payload": row[5], "attempts": row[6],
                }
                for row in cursor.fetchall()
            )
        if connection is not None:
            connection.commit()
        return rows
    except Exception:
        if connection is not None:
            connection.rollback()
        raise


def claim_alert_outbox_cursor(
    cursor,
    *,
    cohort_id: UUID,
    owner_id: UUID,
    owner_epoch: int,
    claimed_at: datetime,
    lease_until: datetime,
    limit: int,
) -> tuple[dict[str, object], ...]:
    return _claim_alert_outbox(
        None,
        repository_cursor=cursor,
        cohort_id=cohort_id,
        owner_id=owner_id,
        owner_epoch=owner_epoch,
        claimed_at=claimed_at,
        lease_until=lease_until,
        limit=limit,
    )


def claim_alert_outbox(
    connection,
    *,
    cohort_id: UUID,
    owner_id: UUID,
    owner_epoch: int,
    claimed_at: datetime,
    lease_until: datetime,
    limit: int,
) -> tuple[dict[str, object], ...]:
    return _claim_alert_outbox(
        connection,
        repository_cursor=None,
        cohort_id=cohort_id,
        owner_id=owner_id,
        owner_epoch=owner_epoch,
        claimed_at=claimed_at,
        lease_until=lease_until,
        limit=limit,
    )


def mark_alert_outbox_delivered_cursor(
    cursor,
    *,
    outbox_id: UUID,
    owner_id: UUID,
    owner_epoch: int,
    delivered_at: datetime,
) -> None:
    if delivered_at.tzinfo is None:
        raise ValueError("outbox delivery clock must be timezone-aware")
    delivered = delivered_at.astimezone(timezone.utc)
    cursor.execute(
        "UPDATE rt.alert_outbox SET delivered_at=%s, lease_owner=NULL, lease_until=NULL, "
        "failure_code=NULL WHERE outbox_id=%s AND delivered_at IS NULL "
        "AND abandoned_at IS NULL AND lease_owner=%s AND lease_epoch=%s "
        "AND lease_until>%s RETURNING outbox_id",
        (delivered, str(outbox_id), str(owner_id), owner_epoch, delivered),
    )
    row = cursor.fetchone()
    if row is None or str(row[0]) != str(outbox_id):
        raise ValueError("outbox delivery acknowledgement lost its lease fence")


def mark_alert_outbox_delivered(
    connection,
    *,
    outbox_id: UUID,
    owner_id: UUID,
    owner_epoch: int,
    delivered_at: datetime,
) -> None:
    try:
        with connection.cursor() as cursor:
            mark_alert_outbox_delivered_cursor(
                cursor,
                outbox_id=outbox_id,
                owner_id=owner_id,
                owner_epoch=owner_epoch,
                delivered_at=delivered_at,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def fail_alert_outbox_delivery_cursor(
    cursor,
    *,
    outbox_id: UUID,
    owner_id: UUID,
    owner_epoch: int,
    failed_at: datetime,
    next_attempt_at: datetime,
    failure_code: str,
    retryable: bool = True,
) -> str:
    if failed_at.tzinfo is None or next_attempt_at.tzinfo is None:
        raise ValueError("outbox failure clocks must be timezone-aware")
    failed = failed_at.astimezone(timezone.utc)
    retry_at = next_attempt_at.astimezone(timezone.utc)
    code = failure_code.strip().upper()
    if not code or len(code) > 64:
        raise ValueError("outbox failure code must contain 1-64 characters")
    if type(retryable) is not bool:
        raise TypeError("outbox retryable flag must be boolean")
    if retryable and retry_at <= failed:
        raise ValueError("outbox retry must be scheduled after the failure")
    cursor.execute(
        "SELECT lease_owner, lease_epoch, lease_until>%s "
        "FROM rt.alert_outbox WHERE outbox_id=%s AND delivered_at IS NULL "
        "AND abandoned_at IS NULL FOR UPDATE",
        (failed, str(outbox_id)),
    )
    lease = cursor.fetchone()
    if lease is None or str(lease[0]) != str(owner_id):
        raise ValueError("outbox failure lease owner does not match")
    if lease[1] != owner_epoch or not lease[2]:
        raise ValueError("outbox failure lease is stale or expired")
    abandoned = not retryable
    cursor.execute(
        "UPDATE rt.alert_outbox SET lease_owner=NULL, lease_until=NULL, failure_code=%s, "
        "next_attempt_at=CASE WHEN %s THEN next_attempt_at ELSE %s END, "
        "abandoned_at=CASE WHEN %s THEN %s ELSE NULL END "
        "WHERE outbox_id=%s AND delivered_at IS NULL AND abandoned_at IS NULL "
        "AND lease_owner=%s AND lease_epoch=%s RETURNING outbox_id",
        (
            code,
            abandoned,
            retry_at,
            abandoned,
            failed,
            str(outbox_id),
            str(owner_id),
            owner_epoch,
        ),
    )
    updated = cursor.fetchone()
    if updated is None or str(updated[0]) != str(outbox_id):
        raise ValueError("outbox failure transition lost its lease fence")
    return "ABANDONED" if abandoned else "PENDING"


def fail_alert_outbox_delivery(
    connection,
    *,
    outbox_id: UUID,
    owner_id: UUID,
    owner_epoch: int,
    failed_at: datetime,
    next_attempt_at: datetime,
    failure_code: str,
    retryable: bool = True,
) -> str:
    try:
        with connection.cursor() as cursor:
            status = fail_alert_outbox_delivery_cursor(
                cursor,
                outbox_id=outbox_id,
                owner_id=owner_id,
                owner_epoch=owner_epoch,
                failed_at=failed_at,
                next_attempt_at=next_attempt_at,
                failure_code=failure_code,
                retryable=retryable,
            )
        connection.commit()
        return status
    except Exception:
        connection.rollback()
        raise


def outbox_identity(event_id: UUID, destination: str, message_kind: str, payload) -> tuple[UUID, str]:
    payload_sha256 = canonical_sha256(payload)
    outbox_id = uuid5(
        NAMESPACE_URL,
        f"realtime-outbox:{event_id}:{destination}:{message_kind}:{payload_sha256}",
    )
    return outbox_id, payload_sha256