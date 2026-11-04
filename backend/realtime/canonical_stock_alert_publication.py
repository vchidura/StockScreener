"""Hands each retained canonical Stock Alert batch to the portal through C7.

A batch only exists after its window sealed INPUT_READY, so a window stage whose
visibility clock is the window release can never read it. The batch owner publishes
instead. The snapshot reads the receipt-clock inputs the batch was built from, while
the batch itself is read at its own recording clock. C7 work rows make this durable:
a retryable failure is resumed by the publication-retry resident.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from options.calendar import OptionExchangeCalendar

from .canonical_stock_alert_repository import (
    load_canonical_stock_alert_projection_cursor,
)
from .database_access_v2 import APP_ROLE
from .portal_projection_repository import PortalReleaseReceipt
from .portal_publisher import PortalPublicationRetryable
from .preflight import ProviderTier
from .runner_environment import StockAlertProjectionRequest
from .stock_alert_repository import (
    compute_realtime_stock_alerts,
    publish_realtime_stock_alerts,
)


_DUE_NAMES = (
    "window_id",
    "input_receipt_id",
    "cutoff",
    "receipt_input_sha256",
    "completed_at",
    "created_at",
    "reference_generation",
    "universe_sha256",
    "provider_tier",
    "receipt_created_at",
)


def load_due_canonical_stock_alert_publications_cursor(
    cursor,
    *,
    cohort_id: UUID,
    session_date: date,
    limit: int = 1,
) -> tuple[Mapping[str, object], ...]:
    """Newest first, so the live cutoff never waits behind a catch-up backlog; an
    older batch is then stored without moving the pointer backward."""
    cursor.execute(
        "SELECT batch.window_id::text,batch.input_receipt_id::text,batch.cutoff,"
        "batch.receipt_input_sha256,batch.completed_at,batch.created_at,"
        "receipt.reference_generation,receipt.universe_sha256,"
        "window_run.provider_tier,receipt.created_at AS receipt_created_at "
        "FROM rt.canonical_stock_alert_batches AS batch "
        "JOIN rt.cycle_receipts AS receipt "
        "ON receipt.receipt_id=batch.input_receipt_id "
        "JOIN rt.window_runs AS window_run "
        "ON window_run.window_id=batch.window_id "
        "JOIN rt.service_plans AS plan ON plan.plan_id=window_run.plan_id "
        "WHERE batch.cohort_id=%s AND plan.session_date=%s "
        "AND NOT EXISTS (SELECT 1 FROM rt.portal_publication_work AS work "
        "WHERE work.source_receipt_id=batch.input_receipt_id "
        "AND work.product_key='STOCK_ALERTS') "
        "ORDER BY batch.cutoff DESC LIMIT %s",
        (str(cohort_id), session_date, limit),
    )
    return tuple(
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(_DUE_NAMES, row, strict=True))
        for row in cursor.fetchall()
    )


def publish_due_canonical_stock_alerts(
    provider,
    *,
    compute,
    publication,
    cohort_id: UUID,
    session_date: date,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    compute_snapshot: Callable = compute_realtime_stock_alerts,
    publish_snapshot: Callable = publish_realtime_stock_alerts,
) -> tuple[Mapping[str, object], ...]:
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
        isolation_level="REPEATABLE READ",
    ) as unit:
        due = load_due_canonical_stock_alert_publications_cursor(
            unit.cursor,
            cohort_id=cohort_id,
            session_date=session_date,
        )
        retained = tuple(
            load_canonical_stock_alert_projection_cursor(
                unit.cursor,
                StockAlertProjectionRequest(
                    cohort_id=cohort_id,
                    window_id=UUID(str(row["window_id"])),
                    input_receipt_id=UUID(str(row["input_receipt_id"])),
                    cutoff=row["cutoff"],
                    input_sha256=str(row["receipt_input_sha256"]),
                    known_by=max(row["completed_at"], row["created_at"]),
                ),
            )
            for row in due
        )
    close = OptionExchangeCalendar("XNYS").session_close(session_date)
    reports = []
    for row, projection in zip(due, retained, strict=True):
        cutoff = row["cutoff"]
        generation = int(row["reference_generation"])
        receipt_id = UUID(str(row["input_receipt_id"]))
        snapshot = compute_snapshot(
            compute,
            cohort_id=cohort_id,
            session=session_date,
            cutoff=cutoff,
            known_by=row["receipt_created_at"],
            source_generation=generation,
            retained_projection=projection,
        )
        try:
            publish_snapshot(
                publication,
                snapshot,
                receipt=PortalReleaseReceipt(
                    receipt_id,
                    cohort_id,
                    ProviderTier(str(row["provider_tier"])),
                    cutoff,
                    str(row["universe_sha256"]),
                    source_receipt_sha256=str(row["receipt_input_sha256"]),
                    source_generation=generation,
                ),
                session_close=cutoff == close,
                owner_id=uuid5(
                    NAMESPACE_URL,
                    f"canonical-stock-alerts-publisher:{cohort_id}",
                ),
                owner_epoch=1,
                checked_at=max(clock(), cutoff),
            )
            status = "PUBLISHED"
        except PortalPublicationRetryable as error:
            status = f"DEFERRED:{error.reason_code}"
        reports.append({
            "input_receipt_id": str(receipt_id),
            "cutoff": cutoff.isoformat(),
            "status": status,
        })
    return tuple(reports)
