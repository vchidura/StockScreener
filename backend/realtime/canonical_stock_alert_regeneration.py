"""Regenerate a closed session's stock alerts under the current policy, beside its originals.

The user approved this on Nov 5 for Nov 2, 3 and 4, which ran before event and sector
context could read N/A, so every stored evaluation was UNAVAILABLE.

Each stored window is replayed in order through the same detector code, with the inputs
that window saw. Original batches, evaluations and alerts are immutable and stay as they
are. This adds detector events under the current detector version, and alert rows marked
"regenerated", with lifecycle REGENERATED so nothing treats them as live. Their published
time is when they were regenerated, not the session's. Nothing is delivered, and no release
or pointer moves. The Stock Alerts page shows them on their session's date with a note.

To undo: delete the rows where projection->>'regenerated'='true' from rt.alert_current.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from equity.model_detection.contracts import Disposition
from options.calendar import OptionExchangeCalendar

from .canonical_stock_alert_product import CanonicalStockAlertBatch
from .canonical_stock_alert_repository import (
    _persist_alerts_cursor,
    _persist_events_cursor,
    _persist_states_cursor,
    _persist_version_cursor,
    build_canonical_stock_alert_batch_cursor,
)
from .database_access_v2 import APP_ROLE
from .portal_identity import load_portal_identity_cursor
from .portal_projection import build_alert_projection

REGENERATED_LIFECYCLE = "REGENERATED"
_RECEIPT_NAMES = (
    "receipt_id",
    "window_id",
    "cohort_id",
    "cutoff",
    "reference_generation",
    "input_sha256",
    "created_at",
    "deadline",
)


class _PlanOnly(Exception):
    """Rolls a plan's transaction back after it has computed everything."""


def load_session_receipts_cursor(
    cursor,
    *,
    cohort_id: UUID,
    session_date: date,
) -> tuple[Mapping[str, object], ...]:
    """One receipt per cutoff, in order: the first attempt that materialized, with or without a batch.

    A later attempt can be a recovery run hours after the session. Its decision time would fail
    the validity gate, though the bars were the same, so it says nothing about the signal.
    """
    cursor.execute(
        "SELECT DISTINCT ON (receipt.cutoff) "
        "receipt.receipt_id::text,receipt.window_id::text,"
        "receipt.cohort_id::text,receipt.cutoff,"
        "receipt.reference_generation,receipt.input_sha256,"
        "receipt.created_at,window_run.deadline "
        "FROM rt.cycle_receipts AS receipt "
        "JOIN rt.window_runs AS window_run "
        "ON window_run.window_id=receipt.window_id "
        "JOIN rt.service_plans AS plan ON plan.plan_id=window_run.plan_id "
        "WHERE receipt.cohort_id=%s AND plan.session_date=%s "
        "AND receipt.receipt_kind='INPUT_READY' "
        "AND EXISTS (SELECT 1 FROM rt.stage_runs AS stage "
        "WHERE stage.window_id=receipt.window_id "
        "AND stage.stage_id='EQUITY_MATERIALIZE' "
        "AND stage.state='COMPLETE') "
        "ORDER BY receipt.cutoff,receipt.created_at",
        (str(cohort_id), session_date),
    )
    return tuple(
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(_RECEIPT_NAMES, row, strict=True))
        for row in cursor.fetchall()
    )


def regenerated_alert_count_cursor(cursor, *, cohort_id: UUID, session_date: date) -> int:
    cursor.execute(
        "SELECT count(*) AS total FROM rt.alert_current "
        "WHERE cohort_id=%s AND alert_kind='STOCK' "
        "AND projection->>'regenerated'='true' AND projection->>'session'=%s",
        (str(cohort_id), session_date.isoformat()),
    )
    row = cursor.fetchone()
    return int(row["total"] if isinstance(row, Mapping) else row[0])


def _publication_row(batch: CanonicalStockAlertBatch, regenerated_at: datetime) -> dict[str, object]:
    return {
        "run_id": str(uuid5(
            NAMESPACE_URL,
            f"canonical-stock-alert-regenerated:{batch.cohort_id}:{batch.window_id}",
        )),
        "strategy_instance_id": "canonical-equity-stock-alert-v1",
        "session": batch.session_date.isoformat(),
        "published_at": regenerated_at.isoformat(),
        "selected": sum(row.decision.disposition is Disposition.MATCH for row in batch.evaluations),
        "expected": len(batch.evaluations),
        "missing": sum(
            row.decision.disposition in {Disposition.UNAVAILABLE, Disposition.ERROR}
            for row in batch.evaluations
        ),
        "status": "COMPLETE",
        "membership_sha256": batch.membership.membership_sha256,
        "observation_cutoff": batch.cutoff.isoformat(),
        "late_catch_up": True,
        "deliverable": False,
        "regenerated": True,
    }


def label_regenerated_batch(
    batch: CanonicalStockAlertBatch,
    regenerated_at: datetime,
) -> CanonicalStockAlertBatch:
    """Keep only the matches, marked so that nothing can read them as live."""
    publication = _publication_row(batch, regenerated_at)
    evaluations = []
    for row in batch.evaluations:
        if row.alert is None or row.decision.disposition is not Disposition.MATCH:
            evaluations.append(replace(row, alert=None))
            continue
        projection = deepcopy(dict(row.alert.projection))
        projection.update({
            "regenerated": True,
            "regenerated_at": regenerated_at.isoformat(),
            "publication": publication,
            "run_id": publication["run_id"],
            "strategy_instance_id": publication["strategy_instance_id"],
            "session": publication["session"],
            "published_at": regenerated_at.isoformat(),
            "status": REGENERATED_LIFECYCLE,
            "lifecycle": REGENERATED_LIFECYCLE,
            "disposition": row.decision.disposition.value,
            "reason_codes": list(row.decision.reason_codes),
            "entry_price": None,
            "entry_at": None,
            "exit_price": None,
            "exit_at": None,
            "exit_due_at": None,
            "latest_price": None,
            "latest_price_at": None,
            "paper_return": None,
            "daily_context_at": None,
            "indicator_at": None,
            "indicator_interval": "5m",
            "membership_sha256": publication["membership_sha256"],
            "observation_cutoff": publication["observation_cutoff"],
            "late_catch_up": True,
            "deliverable": False,
            "delivery_permission": False,
            "publication_permission": False,
            "execution_permission": False,
        })
        evaluations.append(replace(
            row,
            alert=build_alert_projection(
                row.event,
                alert_kind=row.alert.alert_kind,
                lifecycle=REGENERATED_LIFECYCLE,
                projection_schema=row.alert.projection_schema,
                projection=projection,
                destinations=("PORTAL",),
            ),
        ))
    return replace(batch, evaluations=tuple(evaluations))


def regenerate_session(
    provider,
    *,
    session_date: date,
    execute: bool,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    """One transaction per session: it all lands or none of it does. A plan rolls back."""
    regenerated_at = clock()
    if OptionExchangeCalendar("XNYS").session_close(session_date) >= regenerated_at:
        raise ValueError("only a closed session can be regenerated")
    report: dict[str, object] = {"session": session_date.isoformat(), "mode": "EXECUTE" if execute else "PLAN"}
    try:
        with provider.transaction(APP_ROLE, dict_cursor=True) as unit:
            identity = load_portal_identity_cursor(unit.cursor, available_by=regenerated_at)
            cohort_id = identity.session_cohorts.get(session_date.isoformat())
            if cohort_id is None:
                raise ValueError("the session has no served cohort")
            report["cohort_id"] = str(cohort_id)
            existing = regenerated_alert_count_cursor(
                unit.cursor,
                cohort_id=cohort_id,
                session_date=session_date,
            )
            if existing:
                report.update(status="ALREADY_REGENERATED", alerts=existing)
                return report
            receipts = load_session_receipts_cursor(
                unit.cursor,
                cohort_id=cohort_id,
                session_date=session_date,
            )
            dispositions: Counter[str] = Counter()
            reasons: Counter[str] = Counter()
            alerts: Counter[str] = Counter()
            for receipt in receipts:
                batch = label_regenerated_batch(
                    build_canonical_stock_alert_batch_cursor(
                        unit.cursor,
                        receipt,
                        session_date=session_date,
                    ),
                    regenerated_at,
                )
                _persist_version_cursor(unit.cursor, batch)
                _persist_events_cursor(unit.cursor, batch)
                _persist_states_cursor(unit.cursor, batch)
                _persist_alerts_cursor(unit.cursor, batch)
                for row in batch.evaluations:
                    dispositions[row.decision.disposition.value] += 1
                    if row.decision.disposition in {Disposition.UNAVAILABLE, Disposition.ERROR}:
                        reasons.update(row.decision.reason_codes)
                    if row.alert is not None:
                        alerts[row.ticker] += 1
            report.update(
                status="REGENERATED" if execute else "PLANNED",
                windows=len(receipts),
                dispositions=dict(sorted(dispositions.items())),
                unavailable_reasons=dict(reasons.most_common()),
                alerts=sum(alerts.values()),
                alerts_by_ticker=dict(sorted(alerts.items())),
                provider_requests=0,
                delivery_performed=False,
                pointer_performed=False,
            )
            if not execute:
                raise _PlanOnly
    except _PlanOnly:
        pass
    return report
