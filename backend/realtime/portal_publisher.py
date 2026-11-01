from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Protocol
from uuid import UUID

from .database_access import RealtimeDatabaseRole
from .portal_projection import RealtimePortalSnapshot
from .portal_projection_repository import (
    RELEASE_CLASS_INTRADAY,
    RELEASE_CLASS_SESSION_CLOSE,
    RETENTION_HOT_AND_KEPT,
    PortalRelease,
    PortalReleasePointer,
    PortalReleaseReceipt,
    claim_portal_publication_work_cursor,
    claim_alert_outbox_cursor,
    complete_portal_publication_work_cursor,
    enqueue_portal_publication_work_cursor,
    fail_portal_publication_work_cursor,
    fail_alert_outbox_delivery_cursor,
    load_portal_publication_snapshots_cursor,
    load_portal_publication_work_cursor,
    load_portal_release_cursor,
    load_portal_release_pointer_cursor,
    mark_alert_outbox_delivered_cursor,
    persist_portal_snapshot_cursor,
    portal_session_date,
    publish_portal_release_cursor,
    restore_portal_release_pointer_cursor,
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


class PortalPublicationRetryable(RuntimeError):
    def __init__(
        self,
        work_id: UUID,
        retry_at: datetime,
        reason_code: str,
    ) -> None:
        super().__init__(reason_code)
        self.work_id = work_id
        self.retry_at = _utc(retry_at, "publication retry_at")
        self.reason_code = reason_code


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
    dispatch_authorized: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.provider_tier, ProviderTier):
            raise TypeError("portal publication provider tier is invalid")
        if self.owner_epoch <= 0 or self.lease_seconds <= 0:
            raise ValueError("portal publication owner epoch and lease must be positive")
        if not 1 <= self.claim_limit <= 200:
            raise ValueError("portal publication claim limit must be in [1, 200]")
        if self.retry_delay_seconds <= 0:
            raise ValueError("portal publication retry delay must be positive")
        if type(self.dispatch_authorized) is not bool:
            raise TypeError("portal publication dispatch authority is invalid")


@dataclass(frozen=True, slots=True)
class PortalPublicationBatch:
    snapshots: tuple[RealtimePortalSnapshot, ...] = ()
    receipt: PortalReleaseReceipt | None = None
    session_close: bool = False
    session_date: date | None = None
    product_key: str = "PORTAL"
    expected_ticker_keys: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if len(self.snapshots) > MAX_PORTAL_SNAPSHOT_BATCH:
            raise ValueError("portal snapshot batch exceeds 100 rows")
        if self.receipt is not None and not isinstance(self.receipt, PortalReleaseReceipt):
            raise TypeError("portal publication receipt is invalid")
        if (self.session_close or self.session_date is not None) and self.receipt is None:
            raise ValueError("a session-close publication requires its cycle receipt")
        if self.receipt is not None and not self.snapshots:
            raise ValueError("a receipt publication requires snapshots")
        key = self.product_key.strip().upper()
        if not key or len(key) > 64:
            raise ValueError("portal publication product key is invalid")
        object.__setattr__(self, "product_key", key)
        identities = tuple(
            (snapshot.snapshot_type, snapshot.snapshot_key)
            for snapshot in self.snapshots
        )
        if len(identities) != len(set(identities)):
            raise ValueError("portal snapshot batch identities must be unique")
        if self.expected_ticker_keys is not None:
            expected_ticker_keys = tuple(
                sorted(set(self.expected_ticker_keys))
            )
            if (
                not expected_ticker_keys
                or len(expected_ticker_keys)
                != len(self.expected_ticker_keys)
            ):
                raise ValueError(
                    "portal publication expected ticker membership is invalid"
                )
            ticker_keys = {
                snapshot.snapshot_key
                for snapshot in self.snapshots
                if snapshot.snapshot_type == "TICKER_PAGE"
            }
            if ticker_keys != set(expected_ticker_keys):
                raise ValueError(
                    "portal publication ticker pages changed from declared membership"
                )
            object.__setattr__(
                self,
                "expected_ticker_keys",
                expected_ticker_keys,
            )


@dataclass(frozen=True, slots=True)
class PortalPublicationStatus:
    checked_at: datetime
    promoted_snapshots: int
    claimed: int
    delivered: int
    retried: int
    abandoned: int
    reason_codes: tuple[str, ...]
    release: PortalRelease | None = None
    work_id: UUID | None = None
    retry_at: datetime | None = None

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
        if self.release is not None and not isinstance(
            self.release,
            PortalRelease,
        ):
            raise TypeError("portal publication release result is invalid")
        if self.retry_at is not None:
            object.__setattr__(
                self,
                "retry_at",
                _utc(self.retry_at, "publication retry_at"),
            )

    @property
    def verdict(self) -> PortalPublisherVerdict:
        if self.retried or self.abandoned or self.reason_codes:
            return PortalPublisherVerdict.DEGRADED
        if self.promoted_snapshots or self.claimed:
            return PortalPublisherVerdict.COMPLETE
        return PortalPublisherVerdict.IDLE


@dataclass(frozen=True, slots=True)
class CanonicalPortalPublicationResult:
    status: PortalPublicationStatus
    release: PortalRelease
    pointer: PortalReleasePointer | None

    def __post_init__(self) -> None:
        historical = self.release.publication_disposition in {
            "HISTORICAL_STALE",
            "HISTORICAL_NONCURRENT",
        }
        historical_reason = (
            "PORTAL_PUBLICATION_"
            + self.release.publication_disposition
        )
        if (
            self.status.release != self.release
            or (
                self.pointer is None
                and (
                    self.release.pointer_swapped
                    or not historical
                )
            )
            or (
                historical
                and (
                    historical_reason not in self.status.reason_codes
                    or self.status.verdict
                    is not PortalPublisherVerdict.DEGRADED
                )
            )
            or (
                self.pointer is not None
                and (
                    self.pointer.cohort_id != self.release.cohort_id
                    or self.pointer.provider_tier
                    is not self.release.provider_tier
                )
            )
            or (
                self.release.pointer_swapped
                and (
                    self.pointer is None
                    or
                    self.pointer.release_id != self.release.release_id
                    or self.pointer.cutoff != self.release.cutoff
                )
            )
            or (
                not self.release.pointer_swapped
                and self.pointer is not None
                and self.pointer.release_id == self.release.release_id
            )
        ):
            raise ValueError(
                "canonical portal publication result is inconsistent"
            )


class _ForbiddenPortalDeliverySink:
    def deliver(self, _message):
        raise RuntimeError("portal delivery is not authorized")


class CanonicalPortalPublisher:
    """The sole durable release/pointer publisher."""

    def __init__(
        self,
        database: RealtimeProcessDatabase,
        config: PortalPublicationConfig,
        *,
        sink: PortalDeliverySink | None = None,
        publication_runner: Callable | None = None,
        pointer_loader: Callable = load_portal_release_pointer_cursor,
        pointer_restorer: Callable = restore_portal_release_pointer_cursor,
    ) -> None:
        if not isinstance(database, RealtimeProcessDatabase):
            raise TypeError("canonical portal publisher database is invalid")
        if (
            not isinstance(config, PortalPublicationConfig)
            or config.dispatch_authorized
        ):
            raise ValueError(
                "canonical portal stage publisher must disable dispatch"
            )
        self._database = database
        self._config = config
        self._sink = sink or _ForbiddenPortalDeliverySink()
        self._publication_runner = (
            run_portal_publication_once
            if publication_runner is None
            else publication_runner
        )
        self._pointer_loader = pointer_loader
        self._pointer_restorer = pointer_restorer
        if any(
            not callable(value)
            for value in (
                self._publication_runner,
                self._pointer_loader,
                self._pointer_restorer,
            )
        ):
            raise TypeError("canonical portal publisher callback is invalid")

    def _pointer(self) -> PortalReleasePointer | None:
        with self._database.transaction(read_only=True) as cursor:
            return self._pointer_loader(
                cursor,
                cohort_id=self._config.cohort_id,
                provider_tier=self._config.provider_tier,
            )

    def publish(
        self,
        batch: PortalPublicationBatch,
        *,
        checked_at: datetime,
    ) -> CanonicalPortalPublicationResult:
        if batch.receipt is None:
            raise ValueError(
                "canonical portal publication requires a release receipt"
            )
        status = self._publication_runner(
            self._database,
            self._config,
            batch,
            self._sink,
            checked_at=checked_at,
        )
        if not isinstance(status, PortalPublicationStatus):
            raise TypeError(
                "canonical portal publisher returned an invalid status"
            )
        release = status.release
        if (
            release is None
            or batch.receipt.receipt_id
            not in {
                UUID(str(value))
                for value in release.manifest.get(
                    "source_receipt_ids",
                    (release.receipt_id,),
                )
            }
            or release.cohort_id != self._config.cohort_id
            or release.provider_tier is not self._config.provider_tier
            or release.cutoff != batch.receipt.cutoff
            or any((
                status.claimed,
                status.delivered,
                status.retried,
                status.abandoned,
            ))
        ):
            raise ValueError(
                "canonical portal publication result changed authority"
            )
        pointer = self._pointer()
        if (
            release.pointer_swapped
            and (
                pointer is None
                or pointer.release_id != release.release_id
            )
        ) or (
            not release.pointer_swapped
            and pointer is not None
            and pointer.release_id == release.release_id
        ):
            raise ValueError(
                "canonical portal publication did not retain its pointer outcome"
            )
        return CanonicalPortalPublicationResult(
            status,
            release,
            pointer,
        )

    def resume(
        self,
        *,
        source_receipt_id: UUID,
        checked_at: datetime,
        product_key: str = "PORTAL",
    ) -> CanonicalPortalPublicationResult | None:
        status = resume_portal_publication_once(
            self._database,
            self._config,
            product_key=product_key,
            source_receipt_id=source_receipt_id,
            checked_at=checked_at,
        )
        if status is None:
            return None
        if status.release is None:
            raise ValueError(
                "resumed portal publication has no release"
            )
        pointer = self._pointer()
        if status.release.pointer_swapped and pointer is None:
            raise ValueError(
                "resumed portal publication has no readable pointer"
            )
        return CanonicalPortalPublicationResult(
            status,
            status.release,
            pointer,
        )


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


def _completed_publication_status(
    database: RealtimeProcessDatabase,
    work: Mapping[str, object],
    *,
    checked_at: datetime,
) -> PortalPublicationStatus:
    if work["release_id"] is None or work["release_cutoff"] is None:
        raise ValueError(
            "completed portal publication work has no release identity"
        )
    with database.transaction(read_only=True) as cursor:
        release = load_portal_release_cursor(
            cursor,
            release_id=UUID(str(work["release_id"])),
            cutoff=work["release_cutoff"],
        )
    reasons = (
        ()
        if work["disposition"] in {
            "CURRENT",
            "EQUIVALENT_CURRENT",
        }
        else (f"PORTAL_PUBLICATION_{work['disposition']}",)
    )
    return PortalPublicationStatus(
        checked_at,
        0,
        0,
        0,
        0,
        0,
        reasons,
        release,
        UUID(str(work["work_id"])),
    )


def _run_portal_publication_work_once(
    database: RealtimeProcessDatabase,
    config: PortalPublicationConfig,
    work: Mapping[str, object],
    *,
    checked_at: datetime,
    release_repository: Callable,
) -> PortalPublicationStatus:
    checked = _utc(checked_at, "checked_at")
    if work["state"] == "COMPLETE":
        return _completed_publication_status(
            database,
            work,
            checked_at=checked,
        )
    if work["state"] == "TERMINAL":
        raise ValueError(
            "portal publication work is terminal: "
            + str(work["failure_reason"])
        )
    retry_at = work["next_attempt_at"]
    if retry_at > checked:
        raise PortalPublicationRetryable(
            UUID(str(work["work_id"])),
            retry_at,
            "PORTAL_PUBLICATION_RETRY_PENDING",
        )
    with database.transaction() as cursor:
        claimed = claim_portal_publication_work_cursor(
            cursor,
            work_id=UUID(str(work["work_id"])),
            owner_id=config.owner_id,
            owner_epoch=config.owner_epoch,
            claimed_at=checked,
            lease_until=checked + timedelta(
                seconds=config.lease_seconds
            ),
        )
    if claimed is None:
        with database.transaction(read_only=True) as cursor:
            retained = load_portal_publication_work_cursor(
                cursor,
                product_key=str(work["product_key"]),
                source_receipt_id=UUID(str(work["source_receipt_id"])),
                release_class=str(work["release_class"]),
            )
        if retained is not None and retained["state"] == "COMPLETE":
            return _completed_publication_status(
                database,
                retained,
                checked_at=checked,
            )
        retry = (
            checked + timedelta(seconds=config.retry_delay_seconds)
            if retained is None
            else retained["lease_until"]
            or retained["next_attempt_at"]
        )
        raise PortalPublicationRetryable(
            UUID(str(work["work_id"])),
            retry,
            "PORTAL_PUBLICATION_ALREADY_CLAIMED",
        )

    try:
        with database.transaction() as cursor:
            previous = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=config.cohort_id,
                provider_tier=config.provider_tier,
                for_update=True,
            )
            snapshots = load_portal_publication_snapshots_cursor(
                cursor,
                claimed,
            )
            document = claimed["work_document"]
            receipt = PortalReleaseReceipt(
                UUID(str(claimed["source_receipt_id"])),
                UUID(str(claimed["cohort_id"])),
                ProviderTier(str(claimed["provider_tier"])),
                claimed["cutoff"],
                str(document["universe_sha256"]),
                source_receipt_sha256=str(
                    claimed["source_receipt_sha256"]
                ),
                source_generation=int(
                    document["publication_generation"]
                ),
                desired_revision_id=(
                    None
                    if claimed["desired_revision_id"] is None
                    else UUID(str(claimed["desired_revision_id"]))
                ),
                desired_snapshot_sha256=claimed[
                    "desired_snapshot_sha256"
                ],
                membership_sha256=claimed["membership_sha256"],
            )
            release = release_repository(
                cursor,
                receipt,
                snapshots,
                release_class=str(claimed["release_class"]),
                session_date=claimed["session_date"],
                replacement_ticker_keys=document.get(
                    "replacement_ticker_keys"
                ),
            )
            if not isinstance(release, PortalRelease):
                raise TypeError(
                    "portal release repository returned an invalid result"
                )
            kept = load_portal_publication_snapshots_cursor(
                cursor,
                claimed,
                kept=True,
            )
            if kept:
                release_repository(
                    cursor,
                    receipt,
                    kept,
                    release_class=RELEASE_CLASS_SESSION_CLOSE,
                    session_date=claimed["session_date"],
                )
            current = load_portal_release_pointer_cursor(
                cursor,
                cohort_id=config.cohort_id,
                provider_tier=config.provider_tier,
                for_update=True,
            )
            complete_portal_publication_work_cursor(
                cursor,
                claimed,
                release,
                owner_id=config.owner_id,
                owner_epoch=int(claimed["lease_epoch"]),
                completed_at=checked,
                previous_release_id=(
                    None if previous is None else previous.release_id
                ),
                current_release_id=(
                    None if current is None else current.release_id
                ),
            )
    except Exception as error:
        retryable = not isinstance(error, (TypeError, ValueError))
        reason = (
            "PORTAL_PUBLICATION_"
            + type(error).__name__.upper()
        )[:160]
        next_attempt = checked + timedelta(
            seconds=config.retry_delay_seconds
        )
        with database.transaction() as cursor:
            fail_portal_publication_work_cursor(
                cursor,
                claimed,
                owner_id=config.owner_id,
                owner_epoch=int(claimed["lease_epoch"]),
                failed_at=checked,
                next_attempt_at=next_attempt,
                failure_reason=reason,
                retryable=retryable,
            )
        if retryable:
            raise PortalPublicationRetryable(
                UUID(str(claimed["work_id"])),
                next_attempt,
                reason,
            ) from error
        raise
    reasons = (
        ()
        if release.publication_disposition in {
            "CURRENT",
            "EQUIVALENT_CURRENT",
        }
        else (
            "PORTAL_PUBLICATION_"
            + release.publication_disposition,
        )
    )
    return PortalPublicationStatus(
        checked,
        0,
        0,
        0,
        0,
        0,
        reasons,
        release,
        UUID(str(claimed["work_id"])),
    )


def resume_portal_publication_once(
    database: RealtimeProcessDatabase,
    config: PortalPublicationConfig,
    *,
    product_key: str,
    source_receipt_id: UUID,
    checked_at: datetime,
    release_class: str = "INTRADAY",
    release_repository: Callable = publish_portal_release_cursor,
) -> PortalPublicationStatus | None:
    with database.transaction(read_only=True) as cursor:
        work = load_portal_publication_work_cursor(
            cursor,
            product_key=product_key,
            source_receipt_id=source_receipt_id,
            release_class=release_class,
        )
    if work is None:
        return None
    return _run_portal_publication_work_once(
        database,
        config,
        work,
        checked_at=checked_at,
        release_repository=release_repository,
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
    enqueue_repository: Callable = enqueue_portal_publication_work_cursor,
    publication_work_runner: Callable = _run_portal_publication_work_once,
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
    release = None
    publication_reasons: tuple[str, ...] = ()
    publication_work_id = None
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
                work = enqueue_repository(
                    cursor,
                    batch.receipt,
                    batch.snapshots,
                    product_key=batch.product_key,
                    created_at=checked,
                    kept_snapshots=tuple(kept),
                    expected_ticker_keys=batch.expected_ticker_keys,
                    release_class=RELEASE_CLASS_INTRADAY,
                    session_date=close_session,
                )
        if batch.receipt is not None:
            publication_status = publication_work_runner(
                database,
                config,
                work,
                checked_at=checked,
                release_repository=release_repository,
            )
            release = publication_status.release
            publication_reasons = publication_status.reason_codes
            publication_work_id = publication_status.work_id
    claimed_rows = ()
    if config.dispatch_authorized:
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
    reasons: list[str] = list(publication_reasons)
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
        release=release,
        work_id=publication_work_id,
    )