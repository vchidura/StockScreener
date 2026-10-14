from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Protocol
from uuid import UUID

from .contracts import CANARY_UNIVERSE
from .database_access import RealtimeDatabaseRole
from .portal_projection import RealtimePortalSnapshot
from .portal_projection_repository import (
    RELEASE_CLASS_SESSION_CLOSE,
    RETENTION_HOT_AND_KEPT,
    PortalReleaseReceipt,
    claim_alert_outbox_cursor,
    fail_alert_outbox_delivery_cursor,
    mark_alert_outbox_delivered_cursor,
    persist_portal_snapshot_cursor,
    portal_session_date,
    publish_portal_release_cursor,
)
from .preflight import ProviderTier
from .worker_database import RealtimeProcessDatabase


MAX_PORTAL_SNAPSHOT_BATCH = 100
# Per-ticker workspaces are hot only; a session-close release never embeds them.
PER_TICKER_SNAPSHOT_TYPES = frozenset({"TICKER_PAGE", "TICKER_WORKSPACE_V1"})


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


class PortalDeliveryStatus(str, Enum):
    DELIVERED = "DELIVERED"
    RETRY = "RETRY"
    ABANDONED = "ABANDONED"


class PortalPublisherVerdict(str, Enum):
    IDLE = "IDLE"
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True, slots=True)
class PortalOutboxMessage:
    outbox_id: UUID
    event_id: UUID
    destination: str
    message_kind: str
    payload_sha256: str
    payload: Mapping[str, object]
    attempts: int

    def __post_init__(self) -> None:
        if not self.destination.strip() or not self.message_kind.strip():
            raise ValueError("portal outbox destination and kind cannot be blank")
        if len(self.payload_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.payload_sha256
        ):
            raise ValueError("portal outbox payload hash must be SHA-256")
        if self.attempts <= 0:
            raise ValueError("portal outbox attempts must be positive")


@dataclass(frozen=True, slots=True)
class PortalDeliveryDisposition:
    status: PortalDeliveryStatus
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, PortalDeliveryStatus):
            raise TypeError("portal delivery status is invalid")
        code = self.failure_code.strip().upper() if self.failure_code else None
        if self.status is PortalDeliveryStatus.DELIVERED and code is not None:
            raise ValueError("delivered portal message cannot have a failure code")
        if self.status is not PortalDeliveryStatus.DELIVERED and (
            code is None or len(code) > 64
        ):
            raise ValueError("failed portal delivery requires a bounded failure code")
        object.__setattr__(self, "failure_code", code)


class PortalDeliverySink(Protocol):
    def deliver(self, message: PortalOutboxMessage) -> PortalDeliveryDisposition:
        ...


@dataclass(frozen=True, slots=True)
class PortalPublicationConfig:
    cohort_id: UUID
    provider_tier: ProviderTier
    owner_id: UUID
    owner_epoch: int
    lease_seconds: int = 30
    claim_limit: int = 100
    retry_delay_seconds: int = 30

    def __post_init__(self) -> None:
        if not isinstance(self.provider_tier, ProviderTier):
            raise TypeError("portal publication provider tier is invalid")
        if self.owner_epoch <= 0 or self.lease_seconds <= 0:
            raise ValueError("portal publication owner epoch and lease must be positive")
        if not 1 <= self.claim_limit <= 200:
            raise ValueError("portal publication claim limit must be in [1, 200]")
        if self.retry_delay_seconds <= 0:
            raise ValueError("portal publication retry delay must be positive")


@dataclass(frozen=True, slots=True)
class PortalPublicationBatch:
    snapshots: tuple[RealtimePortalSnapshot, ...] = ()
    receipt: PortalReleaseReceipt | None = None
    session_close: bool = False
    session_date: date | None = None

    def __post_init__(self) -> None:
        if len(self.snapshots) > MAX_PORTAL_SNAPSHOT_BATCH:
            raise ValueError("portal snapshot batch exceeds 100 rows")
        if self.receipt is not None and not isinstance(self.receipt, PortalReleaseReceipt):
            raise TypeError("portal publication receipt is invalid")
        if (self.session_close or self.session_date is not None) and self.receipt is None:
            raise ValueError("a session-close publication requires its cycle receipt")
        if self.receipt is not None and not self.snapshots:
            raise ValueError("a receipt publication requires snapshots")
        identities = tuple(
            (snapshot.snapshot_type, snapshot.snapshot_key)
            for snapshot in self.snapshots
        )
        if len(identities) != len(set(identities)):
            raise ValueError("portal snapshot batch identities must be unique")
        if self.snapshots:
            ticker_keys = {
                snapshot.snapshot_key
                for snapshot in self.snapshots
                if snapshot.snapshot_type == "TICKER_PAGE"
            }
            if ticker_keys != set(CANARY_UNIVERSE.equity_tickers):
                raise ValueError("portal publication requires exact canary ticker pages")


@dataclass(frozen=True, slots=True)
class PortalPublicationStatus:
    checked_at: datetime
    promoted_snapshots: int
    claimed: int
    delivered: int
    retried: int
    abandoned: int
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "checked_at", _utc(self.checked_at, "checked_at"))
        counters = (
            self.promoted_snapshots,
            self.claimed,
            self.delivered,
            self.retried,
            self.abandoned,
        )
        if any(type(value) is not int or value < 0 for value in counters):
            raise ValueError("portal publication counters must be nonnegative")
        if self.claimed != self.delivered + self.retried + self.abandoned:
            raise ValueError("portal publication delivery counters are unbalanced")

    @property
    def verdict(self) -> PortalPublisherVerdict:
        if self.retried or self.abandoned:
            return PortalPublisherVerdict.DEGRADED
        if self.promoted_snapshots or self.claimed:
            return PortalPublisherVerdict.COMPLETE
        return PortalPublisherVerdict.IDLE


def _message(row: Mapping[str, object]) -> PortalOutboxMessage:
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("portal outbox payload must be an object")
    return PortalOutboxMessage(
        outbox_id=UUID(str(row["outbox_id"])),
        event_id=UUID(str(row["event_id"])),
        destination=str(row["destination"]),
        message_kind=str(row["message_kind"]),
        payload_sha256=str(row["payload_sha256"]),
        payload=payload,
        attempts=int(row["attempts"]),
    )


def run_portal_publication_once(
    database: RealtimeProcessDatabase,
    config: PortalPublicationConfig,
    batch: PortalPublicationBatch,
    sink: PortalDeliverySink,
    *,
    checked_at: datetime,
    snapshot_repository: Callable = persist_portal_snapshot_cursor,
    claim_repository: Callable = claim_alert_outbox_cursor,
    delivered_repository: Callable = mark_alert_outbox_delivered_cursor,
    failed_repository: Callable = fail_alert_outbox_delivery_cursor,
    release_repository: Callable = publish_portal_release_cursor,
) -> PortalPublicationStatus:
    if not isinstance(database, RealtimeProcessDatabase):
        raise TypeError("portal publisher database is invalid")
    if database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION:
        raise ValueError("portal publisher requires the publication database role")
    if not isinstance(config, PortalPublicationConfig):
        raise TypeError("portal publication config is invalid")
    if not isinstance(batch, PortalPublicationBatch):
        raise TypeError("portal publication batch is invalid")
    checked = _utc(checked_at, "checked_at")
    if any(
        snapshot.cohort_id != config.cohort_id
        or snapshot.provider_tier is not config.provider_tier
        or snapshot.generated_at > checked
        for snapshot in batch.snapshots
    ) or (
        batch.receipt is not None
        and (
            batch.receipt.cohort_id != config.cohort_id
            or batch.receipt.provider_tier is not config.provider_tier
            or batch.receipt.cutoff > checked
        )
    ):
        raise ValueError("portal snapshot batch conflicts with publisher identity or clock")
    if batch.snapshots:
        close_session = None
        if batch.session_close:
            close_session = batch.session_date or portal_session_date(next(
                (row for row in batch.snapshots if row.snapshot_type not in PER_TICKER_SNAPSHOT_TYPES),
                batch.snapshots[0],
            ))
        with database.transaction() as cursor:
            kept = []
            for snapshot in batch.snapshots:
                if batch.session_close and snapshot.snapshot_type not in PER_TICKER_SNAPSHOT_TYPES:
                    kept.append(snapshot_repository(
                        cursor, snapshot, retention=RETENTION_HOT_AND_KEPT,
                        session_date=close_session,
                    ))
                else:
                    snapshot_repository(cursor, snapshot)
            if batch.receipt is not None:
                release_repository(cursor, batch.receipt, batch.snapshots)
                if kept:
                    release_repository(
                        cursor, batch.receipt, tuple(kept),
                        release_class=RELEASE_CLASS_SESSION_CLOSE,
                        session_date=close_session,
                    )
    with database.transaction() as cursor:
        claimed_rows = claim_repository(
            cursor,
            cohort_id=config.cohort_id,
            owner_id=config.owner_id,
            owner_epoch=config.owner_epoch,
            claimed_at=checked,
            lease_until=checked + timedelta(seconds=config.lease_seconds),
            limit=config.claim_limit,
        )
    delivered = retried = abandoned = 0
    reasons: list[str] = []
    for row in claimed_rows:
        message = _message(row)
        try:
            disposition = sink.deliver(message)
            if not isinstance(disposition, PortalDeliveryDisposition):
                raise TypeError("portal delivery sink returned an invalid disposition")
        except Exception:
            disposition = PortalDeliveryDisposition(
                PortalDeliveryStatus.RETRY,
                "PORTAL_SINK_EXCEPTION",
            )
        with database.transaction() as cursor:
            if disposition.status is PortalDeliveryStatus.DELIVERED:
                delivered_repository(
                    cursor,
                    outbox_id=message.outbox_id,
                    owner_id=config.owner_id,
                    owner_epoch=config.owner_epoch,
                    delivered_at=checked,
                )
                delivered += 1
            else:
                retryable = disposition.status is PortalDeliveryStatus.RETRY
                status = failed_repository(
                    cursor,
                    outbox_id=message.outbox_id,
                    owner_id=config.owner_id,
                    owner_epoch=config.owner_epoch,
                    failed_at=checked,
                    next_attempt_at=checked + timedelta(
                        seconds=config.retry_delay_seconds
                    ),
                    failure_code=disposition.failure_code,
                    retryable=retryable,
                )
                expected = "PENDING" if retryable else "ABANDONED"
                if status != expected:
                    raise ValueError("portal outbox failure transition returned wrong status")
                reasons.append(disposition.failure_code or "PORTAL_DELIVERY_FAILED")
                if retryable:
                    retried += 1
                else:
                    abandoned += 1
    return PortalPublicationStatus(
        checked_at=checked,
        promoted_snapshots=len(batch.snapshots),
        claimed=len(claimed_rows),
        delivered=delivered,
        retried=retried,
        abandoned=abandoned,
        reason_codes=tuple(dict.fromkeys(reasons)),
    )