"""Durable receipt-bound repository for canonical V3 Stock Alerts."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json, execute_values

from equity.model_detection.contracts import (
    Disposition,
    Phase,
    PriorState,
    canonical_value,
)

from .canonical_stock_alert_product import (
    CANONICAL_STOCK_ALERT_PRODUCT,
    CanonicalStockAlertBatch,
    canonical_stock_alert_membership,
    produce_canonical_stock_alert_batch,
)
from .contracts import canonical_sha256
from .runner_environment import StockAlertProjectionRequest
from .screening_repository import (
    load_realtime_canonical_stock_alert_input,
)


CANONICAL_STOCK_ALERT_RETAINED_CONTRACT = (
    "canonical-stock-alert-retained-projection-v1"
)


class CanonicalStockAlertBatchUnavailable(LookupError):
    pass


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _values(row, names):
    return tuple(row[name] for name in names) if isinstance(
        row,
        Mapping,
    ) else tuple(row)


def load_due_canonical_stock_alert_receipts_cursor(
    cursor,
    *,
    cohort_id: UUID,
    session_date: date,
    limit: int = 8,
) -> tuple[Mapping[str, object], ...]:
    if not 1 <= limit <= 32:
        raise ValueError("canonical Stock Alert claim limit is invalid")
    cursor.execute(
        "SELECT receipt.receipt_id::text,receipt.window_id::text,"
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
        "AND NOT EXISTS (SELECT 1 "
        "FROM rt.canonical_stock_alert_batches AS batch "
        "WHERE batch.input_receipt_id=receipt.receipt_id) "
        "ORDER BY receipt.cutoff,receipt.created_at LIMIT %s "
        "FOR UPDATE OF window_run SKIP LOCKED",
        (str(cohort_id), session_date, limit),
    )
    names = (
        "receipt_id",
        "window_id",
        "cohort_id",
        "cutoff",
        "reference_generation",
        "input_sha256",
        "created_at",
        "deadline",
    )
    return tuple(
        dict(row) if isinstance(row, Mapping)
        else dict(zip(names, row, strict=True))
        for row in cursor.fetchall()
    )


def _prior_states_cursor(
    cursor,
    *,
    detector_sha256: str,
    cohort_id: UUID,
) -> dict[str, PriorState]:
    cursor.execute(
        "SELECT state_payload FROM rt.detector_state "
        "WHERE detector_sha256=%s AND cohort_id=%s",
        (detector_sha256, str(cohort_id)),
    )
    values = {}
    for row in cursor.fetchall():
        payload = (
            row["state_payload"]
            if isinstance(row, Mapping)
            else row[0]
        )
        state = payload["state"]
        values[str(payload["security_id"])] = PriorState(
            state_key=str(state["state_key"]),
            revision=int(state["revision"]),
            phase=Phase(str(state["phase"])),
            trigger_identity=state.get("trigger_identity"),
            valid_until=(
                None
                if state.get("valid_until") is None
                else datetime.fromisoformat(str(state["valid_until"]))
            ),
            last_input_sha256=state.get("last_input_sha256"),
        )
    return values


def _membership_cursor(cursor, inputs):
    cursor.execute(
        "SELECT snapshot.revision_id::text,snapshot.snapshot_sha256 "
        "FROM rt.realtime_desired_session_snapshots AS snapshot "
        "WHERE snapshot.cohort_id=%s AND snapshot.session_date=%s "
        "ORDER BY snapshot.materialized_at,snapshot.revision_id",
        (str(inputs.cohort_id), inputs.session),
    )
    snapshots = tuple(cursor.fetchall())
    if not snapshots:
        return canonical_stock_alert_membership(inputs)
    if len(snapshots) != 1:
        raise ValueError(
            "canonical Stock Alert cohort has ambiguous desired snapshots"
        )
    revision_id, snapshot_sha256 = _values(
        snapshots[0],
        ("revision_id", "snapshot_sha256"),
    )
    cursor.execute(
        "SELECT intent.ticker,intent.member_kind,intent.enabled,"
        "readiness.status,readiness.reasons,"
        "binding.security_id::text,binding.instrument_id "
        "FROM rt.realtime_desired_member_intents AS intent "
        "JOIN rt.realtime_desired_intent_readiness AS readiness "
        "ON readiness.intent_id=intent.intent_id "
        "LEFT JOIN rt.realtime_desired_member_bindings AS binding "
        "ON binding.intent_id=intent.intent_id "
        "WHERE intent.revision_id=%s "
        "AND intent.member_kind IN ('EQUITY','SECTOR_ETF') "
        "ORDER BY intent.ticker,intent.member_kind",
        (str(revision_id),),
    )
    names = (
        "ticker",
        "member_kind",
        "enabled",
        "status",
        "reason_codes",
        "security_id",
        "instrument_id",
    )
    intended = tuple(
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(names, row, strict=True))
        for row in cursor.fetchall()
    )
    if not intended:
        raise ValueError(
            "canonical Stock Alert desired membership is unavailable"
        )
    return canonical_stock_alert_membership(
        inputs,
        desired_revision_id=UUID(str(revision_id)),
        desired_snapshot_sha256=str(snapshot_sha256),
        intended_members=intended,
    )


def build_canonical_stock_alert_batch_cursor(
    cursor,
    receipt: Mapping[str, object],
    *,
    session_date: date,
) -> CanonicalStockAlertBatch:
    cohort_id = UUID(str(receipt["cohort_id"]))
    cutoff = _utc(receipt["cutoff"], "receipt cutoff")
    known_by = _utc(receipt["created_at"], "receipt created_at")
    inputs = load_realtime_canonical_stock_alert_input(
        cursor,
        cohort_id=cohort_id,
        session=session_date,
        cutoff=cutoff,
        source_generation=int(receipt["reference_generation"]),
        known_by=known_by,
    )
    from .canonical_stock_alert_product import (
        canonical_stock_alert_version,
    )

    version = canonical_stock_alert_version()
    return produce_canonical_stock_alert_batch(
        inputs,
        window_id=UUID(str(receipt["window_id"])),
        input_receipt_id=UUID(str(receipt["receipt_id"])),
        receipt_input_sha256=str(receipt["input_sha256"]),
        membership=_membership_cursor(cursor, inputs),
        deadline=_utc(receipt["deadline"], "receipt deadline"),
        prior_states=_prior_states_cursor(
            cursor,
            detector_sha256=version.detector_sha256,
            cohort_id=cohort_id,
        ),
    )


def _persist_version_cursor(cursor, batch: CanonicalStockAlertBatch) -> None:
    version = batch.version
    cursor.execute(
        "INSERT INTO rt.detector_versions("
        "detector_sha256,detector_id,version,code_sha256,policy_sha256,"
        "valuation_sha256,requirements,output_kind) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (
            version.detector_sha256,
            version.detector_id,
            version.version,
            version.code_sha256,
            version.policy_sha256,
            version.valuation_sha256,
            Json(version.requirements),
            version.output_kind,
        ),
    )
    cursor.execute(
        "SELECT detector_id,version,code_sha256,policy_sha256,"
        "valuation_sha256 FROM rt.detector_versions "
        "WHERE detector_sha256=%s",
        (version.detector_sha256,),
    )
    retained = cursor.fetchone()
    expected = (
        version.detector_id,
        version.version,
        version.code_sha256,
        version.policy_sha256,
        version.valuation_sha256,
    )
    if retained is None or _values(
        retained,
        (
            "detector_id",
            "version",
            "code_sha256",
            "policy_sha256",
            "valuation_sha256",
        ),
    ) != expected:
        raise ValueError("canonical detector version identity changed")


def _persist_events_cursor(cursor, batch: CanonicalStockAlertBatch) -> None:
    events = tuple(row.event for row in batch.evaluations)
    wanted_bars = sorted({
        str(input_row.bar_revision_id)
        for event in events
        for input_row in event.inputs
        if input_row.bar_revision_id is not None
    })
    if wanted_bars:
        cursor.execute(
            "SELECT bar_revision_id::text FROM rt.bar_catalog "
            "WHERE bar_revision_id=ANY(%s::uuid[])",
            (wanted_bars,),
        )
        if {
            str(
                row["bar_revision_id"]
                if isinstance(row, Mapping)
                else row[0]
            )
            for row in cursor.fetchall()
        } != set(wanted_bars):
            raise ValueError(
                "canonical detector bar input is unavailable"
            )
    execute_values(
        cursor,
        "INSERT INTO rt.detector_events("
        "event_id,dedup_sha256,cohort_id,provider_tier,detector_sha256,"
        "instrument_id,trigger_identity,trigger_at,decision_at,valid_until,"
        "disposition,direction,reason_codes,readiness_epoch,evidence_sha256,"
        "payload_schema,payload) VALUES %s ON CONFLICT DO NOTHING",
        [
            (
                str(event.event_id),
                event.dedup_sha256,
                str(event.cohort_id),
                event.provider_tier,
                event.detector_sha256,
                event.instrument_id,
                event.trigger_identity,
                event.trigger_at,
                event.decision_at,
                event.valid_until,
                event.disposition,
                event.direction,
                list(event.reason_codes),
                event.readiness_epoch,
                event.evidence_sha256,
                event.payload_schema,
                Json(event.payload),
            )
            for event in events
        ],
    )
    inputs = [
        (
            str(event.event_id),
            row.input_role,
            row.input_key,
            row.source_kind,
            str(row.feature_snapshot_id)
            if row.feature_snapshot_id else None,
            str(row.bar_revision_id) if row.bar_revision_id else None,
            str(row.option_mark_evidence_id)
            if row.option_mark_evidence_id else None,
            str(row.source_record_id) if row.source_record_id else None,
            str(row.journal_generation)
            if row.journal_generation else None,
            str(row.stream_session_id) if row.stream_session_id else None,
            row.source_offset,
            str(row.source_flow_id) if row.source_flow_id else None,
            str(row.source_cohort_id)
            if row.source_cohort_id else None,
            row.input_sha256,
            row.available_at,
            row.received_at,
            Json(row.retained_input),
        )
        for event in events
        for row in event.inputs
    ]
    if inputs:
        execute_values(
            cursor,
            "INSERT INTO rt.detector_event_inputs("
            "event_id,input_role,input_key,source_kind,feature_snapshot_id,"
            "bar_revision_id,option_mark_evidence_id,source_record_id,"
            "journal_generation,stream_session_id,source_offset,"
            "source_flow_id,source_cohort_id,input_sha256,available_at,"
            "received_at,retained_input) VALUES %s ON CONFLICT DO NOTHING",
            inputs,
        )


def _persist_states_cursor(cursor, batch: CanonicalStockAlertBatch) -> None:
    states = tuple(
        row.state for row in batch.evaluations if row.state is not None
    )
    if not states:
        return
    execute_values(
        cursor,
        "INSERT INTO rt.detector_state("
        "detector_sha256,cohort_id,state_key,revision,state_schema,"
        "state_sha256,state_payload,updated_at) VALUES %s "
        "ON CONFLICT(detector_sha256,cohort_id,state_key) DO UPDATE SET "
        "revision=EXCLUDED.revision,state_schema=EXCLUDED.state_schema,"
        "state_sha256=EXCLUDED.state_sha256,"
        "state_payload=EXCLUDED.state_payload,"
        "updated_at=EXCLUDED.updated_at "
        "WHERE rt.detector_state.revision < EXCLUDED.revision "
        "AND rt.detector_state.updated_at < EXCLUDED.updated_at",
        [
            (
                state.detector_sha256,
                str(state.cohort_id),
                state.state_key,
                state.revision,
                state.state_schema,
                state.state_sha256,
                Json(state.state_payload),
                state.updated_at,
            )
            for state in states
        ],
    )


def _persist_alerts_cursor(cursor, batch: CanonicalStockAlertBatch) -> None:
    alerts = tuple(
        row.alert for row in batch.evaluations if row.alert is not None
    )
    if not alerts:
        return
    execute_values(
        cursor,
        "INSERT INTO rt.alert_current("
        "alert_id,cohort_id,event_id,alert_kind,lifecycle,as_of,"
        "valid_until,projection_schema,projection_sha256,projection) "
        "VALUES %s ON CONFLICT(alert_id) DO UPDATE SET "
        "lifecycle=EXCLUDED.lifecycle,as_of=EXCLUDED.as_of,"
        "valid_until=EXCLUDED.valid_until,"
        "projection_schema=EXCLUDED.projection_schema,"
        "projection_sha256=EXCLUDED.projection_sha256,"
        "projection=EXCLUDED.projection,updated_at=clock_timestamp() "
        "WHERE rt.alert_current.event_id=EXCLUDED.event_id "
        "AND rt.alert_current.as_of < EXCLUDED.as_of",
        [
            (
                str(alert.alert_id),
                str(alert.cohort_id),
                str(alert.event_id),
                alert.alert_kind,
                alert.lifecycle,
                alert.as_of,
                alert.valid_until,
                alert.projection_schema,
                alert.projection_sha256,
                Json(alert.projection),
            )
            for alert in alerts
        ],
    )


def persist_canonical_stock_alert_batch_cursor(
    cursor,
    batch: CanonicalStockAlertBatch,
) -> Mapping[str, object]:
    if not isinstance(batch, CanonicalStockAlertBatch):
        raise TypeError("canonical Stock Alert batch is invalid")
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"canonical-stock-alert:{batch.cohort_id}:{batch.window_id}",),
    )
    _persist_version_cursor(cursor, batch)
    _persist_events_cursor(cursor, batch)
    _persist_states_cursor(cursor, batch)
    _persist_alerts_cursor(cursor, batch)
    counts = {
        disposition.value: sum(
            row.decision.disposition is disposition
            for row in batch.evaluations
        )
        for disposition in Disposition
    }
    batch_id = uuid5(
        NAMESPACE_URL,
        (
            "canonical-stock-alert-batch:"
            f"{batch.cohort_id}:{batch.window_id}:"
            f"{batch.input_receipt_id}"
        ),
    )
    document = {
        "contract": "canonical-stock-alert-batch-v1",
        "product": CANONICAL_STOCK_ALERT_PRODUCT,
        "batch_id": str(batch_id),
        "batch_sha256": batch.batch_sha256,
        "cohort_id": str(batch.cohort_id),
        "window_id": str(batch.window_id),
        "input_receipt_id": str(batch.input_receipt_id),
        "session_date": batch.session_date.isoformat(),
        "cutoff": batch.cutoff.isoformat(),
        "receipt_input_sha256": batch.receipt_input_sha256,
        "detector_input_sha256": batch.detector_input_sha256,
        "membership": batch.membership.document,
        "detector_sha256": batch.version.detector_sha256,
        "counts": counts,
        "completed_at": batch.completed_at.isoformat(),
        "deadline": (
            None if batch.deadline is None else batch.deadline.isoformat()
        ),
        "late_catch_up": batch.is_late,
        "deliverable": not batch.is_late,
    }
    cursor.execute(
        "INSERT INTO rt.canonical_stock_alert_batches("
        "batch_id,cohort_id,window_id,input_receipt_id,cutoff,"
        "receipt_input_sha256,detector_input_sha256,detector_sha256,"
        "status,evaluation_count,match_count,no_match_count,"
        "unavailable_count,error_count,batch_sha256,batch_document,"
        "completed_at,desired_revision_id,desired_snapshot_sha256,"
        "membership_sha256,membership_document,observation_session,"
        "deadline,late_catch_up,deliverable) VALUES("
        "%s,%s,%s,%s,%s,%s,%s,%s,'COMPLETE',%s,%s,%s,%s,%s,%s,%s,%s,"
        "%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING",
        (
            str(batch_id),
            str(batch.cohort_id),
            str(batch.window_id),
            str(batch.input_receipt_id),
            batch.cutoff,
            batch.receipt_input_sha256,
            batch.detector_input_sha256,
            batch.version.detector_sha256,
            len(batch.evaluations),
            counts[Disposition.MATCH.value],
            counts[Disposition.NO_MATCH.value],
            counts[Disposition.UNAVAILABLE.value],
            counts[Disposition.ERROR.value],
            batch.batch_sha256,
            Json(document),
            batch.completed_at,
            (
                None
                if batch.membership.desired_revision_id is None
                else str(batch.membership.desired_revision_id)
            ),
            batch.membership.desired_snapshot_sha256,
            batch.membership.membership_sha256,
            Json(batch.membership.document),
            batch.session_date,
            batch.deadline,
            batch.is_late,
            not batch.is_late,
        ),
    )
    evaluations = []
    for row in batch.evaluations:
        evaluation_id = uuid5(
            NAMESPACE_URL,
            f"canonical-stock-alert-evaluation:{batch_id}:{row.event.event_id}",
        )
        payload = {
            "contract": "canonical-stock-alert-evaluation-v1",
            "evaluation_id": str(evaluation_id),
            "batch_id": str(batch_id),
            "event_id": str(row.event.event_id),
            "security_id": str(row.security_id),
            "instrument_id": row.instrument_id,
            "ticker": row.ticker,
            "input_sha256": row.input_sha256,
            "disposition": row.decision.disposition.value,
            "reason_codes": list(row.decision.reason_codes),
            "desired_revision_id": (
                None
                if batch.membership.desired_revision_id is None
                else str(batch.membership.desired_revision_id)
            ),
            "desired_snapshot_sha256": (
                batch.membership.desired_snapshot_sha256
            ),
            "membership_sha256": batch.membership.membership_sha256,
            "observation_session": batch.session_date.isoformat(),
            "observation_cutoff": batch.cutoff.isoformat(),
            "late_catch_up": batch.is_late,
            "deliverable": not batch.is_late,
            "decision": canonical_value(row.decision),
            "alert": (
                None
                if row.alert is None
                else dict(row.alert.projection)
            ),
        }
        evaluations.append((
            str(evaluation_id),
            str(batch_id),
            str(row.event.event_id),
            str(row.security_id),
            row.instrument_id,
            row.ticker,
            row.decision.disposition.value,
            list(row.decision.reason_codes),
            row.input_sha256,
            None if row.alert is None else str(row.alert.alert_id),
            canonical_sha256(payload),
            Json(payload),
            row.event.decision_at,
            (
                None
                if batch.membership.desired_revision_id is None
                else str(batch.membership.desired_revision_id)
            ),
            batch.membership.desired_snapshot_sha256,
            batch.membership.membership_sha256,
            batch.session_date,
            batch.cutoff,
            batch.is_late,
            not batch.is_late,
        ))
    execute_values(
        cursor,
        "INSERT INTO rt.canonical_stock_alert_evaluations("
        "evaluation_id,batch_id,event_id,security_id,instrument_id,ticker,"
        "disposition,reason_codes,input_sha256,alert_id,evaluation_sha256,"
        "evaluation_payload,observed_at,desired_revision_id,"
        "desired_snapshot_sha256,membership_sha256,observation_session,"
        "observation_cutoff,late_catch_up,deliverable) "
        "VALUES %s ON CONFLICT DO NOTHING",
        evaluations,
    )
    cursor.execute(
        "SELECT batch_sha256,evaluation_count FROM "
        "rt.canonical_stock_alert_batches WHERE batch_id=%s",
        (str(batch_id),),
    )
    retained = cursor.fetchone()
    retained_values = (
        None
        if retained is None
        else _values(retained, ("batch_sha256", "evaluation_count"))
    )
    if (
        retained_values is None
        or retained_values[0] != batch.batch_sha256
        or int(retained_values[1]) != len(batch.evaluations)
    ):
        raise ValueError("canonical Stock Alert batch identity changed")
    return document


def load_canonical_stock_alert_projection_cursor(
    cursor,
    request: StockAlertProjectionRequest,
) -> Mapping[str, object]:
    cursor.execute(
        "SELECT batch.* FROM rt.canonical_stock_alert_batches AS batch "
        "WHERE batch.window_id=%s AND batch.input_receipt_id=%s",
        (str(request.window_id), str(request.input_receipt_id)),
    )
    batch = cursor.fetchone()
    if batch is None:
        cursor.execute(
            "SELECT input_receipt_id FROM rt.canonical_stock_alert_batches "
            "WHERE window_id=%s",
            (str(request.window_id),),
        )
        if cursor.fetchone() is not None:
            raise ValueError(
                "canonical Stock Alert batch diverges from INPUT_READY"
            )
        raise CanonicalStockAlertBatchUnavailable(
            "canonical Stock Alert batch is unavailable"
        )
    if (
        str(batch["cohort_id"]) != str(request.cohort_id)
        or batch["cutoff"] != request.cutoff
        or batch["receipt_input_sha256"] != request.input_sha256
        or batch["completed_at"] > request.known_by
        or batch["created_at"] > request.known_by
    ):
        raise ValueError(
            "canonical Stock Alert batch scope or clock changed"
        )
    cursor.execute(
        "SELECT evaluation.* "
        "FROM rt.canonical_stock_alert_evaluations AS evaluation "
        "WHERE evaluation.batch_id=%s "
        "ORDER BY evaluation.ticker,evaluation.evaluation_id",
        (str(batch["batch_id"]),),
    )
    evaluations = tuple(dict(row) for row in cursor.fetchall())
    if (
        len(evaluations) != int(batch["evaluation_count"])
        or any(row["observed_at"] > request.known_by for row in evaluations)
    ):
        raise ValueError(
            "canonical Stock Alert evaluation set changed"
        )
    document = {
        "contract": CANONICAL_STOCK_ALERT_RETAINED_CONTRACT,
        "loaded_at": request.known_by.isoformat(),
        "batch": {
            "batch_id": str(batch["batch_id"]),
            "batch_sha256": batch["batch_sha256"],
            "cohort_id": str(batch["cohort_id"]),
            "window_id": str(batch["window_id"]),
            "input_receipt_id": str(batch["input_receipt_id"]),
            "cutoff": batch["cutoff"].isoformat(),
            "receipt_input_sha256": batch["receipt_input_sha256"],
            "detector_input_sha256": batch["detector_input_sha256"],
            "detector_sha256": batch["detector_sha256"],
            "session_date": batch["observation_session"].isoformat(),
            "desired_revision_id": (
                None
                if batch["desired_revision_id"] is None
                else str(batch["desired_revision_id"])
            ),
            "desired_snapshot_sha256": batch[
                "desired_snapshot_sha256"
            ],
            "membership_sha256": batch["membership_sha256"],
            "membership": batch["membership_document"],
            "deadline": (
                None
                if batch["deadline"] is None
                else batch["deadline"].isoformat()
            ),
            "late_catch_up": bool(batch["late_catch_up"]),
            "deliverable": bool(batch["deliverable"]),
            "completed_at": batch["completed_at"].isoformat(),
        },
        "evaluations": [
            {
                "evaluation_id": str(row["evaluation_id"]),
                "event_id": str(row["event_id"]),
                "security_id": str(row["security_id"]),
                "instrument_id": int(row["instrument_id"]),
                "ticker": row["ticker"],
                "disposition": row["disposition"],
                "reason_codes": list(row["reason_codes"]),
                "input_sha256": row["input_sha256"],
                "evaluation_sha256": row["evaluation_sha256"],
                "payload": row["evaluation_payload"],
                "alert": row["evaluation_payload"].get("alert"),
                "alert_id": (
                    None
                    if row["alert_id"] is None
                    else str(row["alert_id"])
                ),
                "observed_at": row["observed_at"].isoformat(),
                "desired_revision_id": (
                    None
                    if row["desired_revision_id"] is None
                    else str(row["desired_revision_id"])
                ),
                "desired_snapshot_sha256": row[
                    "desired_snapshot_sha256"
                ],
                "membership_sha256": row["membership_sha256"],
                "observation_session": row[
                    "observation_session"
                ].isoformat(),
                "observation_cutoff": row[
                    "observation_cutoff"
                ].isoformat(),
                "late_catch_up": bool(row["late_catch_up"]),
                "deliverable": bool(row["deliverable"]),
            }
            for row in evaluations
        ],
    }
    document["projection_sha256"] = canonical_sha256(document)
    return document


def canonical_stock_alert_health_cursor(
    cursor,
    *,
    cohort_id: UUID,
) -> Mapping[str, object]:
    cursor.execute(
        "SELECT to_regclass('rt.canonical_stock_alert_batches')"
    )
    schema_row = cursor.fetchone()
    schema_relation = (
        next(iter(schema_row.values()))
        if isinstance(schema_row, Mapping)
        else schema_row[0]
    )
    if schema_relation is None:
        return {"status": "BLOCKED", "reason": "SCHEMA_UNAVAILABLE"}
    cursor.execute(
        "SELECT count(*),max(completed_at) "
        "FROM rt.canonical_stock_alert_batches WHERE cohort_id=%s",
        (str(cohort_id),),
    )
    count_row = cursor.fetchone()
    count, completed_at = (
        (
            count_row["count"],
            count_row["max"],
        )
        if isinstance(count_row, Mapping)
        else count_row
    )
    cursor.execute(
        "SELECT desired_revision_id::text,desired_snapshot_sha256,"
        "membership_sha256,membership_document,observation_session,"
        "cutoff,late_catch_up,deliverable "
        "FROM rt.canonical_stock_alert_batches WHERE cohort_id=%s "
        "ORDER BY cutoff DESC,completed_at DESC,batch_id LIMIT 1",
        (str(cohort_id),),
    )
    batch_row = cursor.fetchone()
    batch = (
        None
        if batch_row is None
        else dict(batch_row)
        if isinstance(batch_row, Mapping)
        else dict(zip((
            "desired_revision_id",
            "desired_snapshot_sha256",
            "membership_sha256",
            "membership_document",
            "observation_session",
            "cutoff",
            "late_catch_up",
            "deliverable",
        ), batch_row, strict=True))
    )
    cursor.execute(
        "SELECT work_id::text,state,disposition,attempts,"
        "next_attempt_at,failure_reason,release_id::text,deliverable "
        "FROM rt.portal_publication_work WHERE cohort_id=%s "
        "AND product_key='STOCK_ALERTS' "
        "ORDER BY cutoff DESC,created_at DESC LIMIT 1",
        (str(cohort_id),),
    )
    work_row = cursor.fetchone()
    work = (
        None
        if work_row is None
        else dict(work_row)
        if isinstance(work_row, Mapping)
        else dict(zip((
            "work_id",
            "state",
            "disposition",
            "attempts",
            "next_attempt_at",
            "failure_reason",
            "release_id",
            "deliverable",
        ), work_row, strict=True))
    )
    membership = (
        None if batch is None else batch["membership_document"]
    )
    intended = (
        () if membership is None else membership["intended_members"]
    )
    effective = (
        () if membership is None else membership["effective_members"]
    )
    partial = (
        membership is not None
        and (
            any(
                row["enabled"] and row["status"] != "READY"
                for row in intended
            )
            or {
                (str(row["security_id"]), row["ticker"])
                for row in effective
            }
            != {
                (str(row["security_id"]), row["ticker"])
                for row in intended
                if row["enabled"]
                and row["status"] == "READY"
                and row["security_id"] is not None
            }
        )
    )
    degraded = bool(
        partial
        or (batch is not None and not batch["deliverable"])
        or (
            work is not None
            and work["state"] in {"RETRY", "TERMINAL"}
        )
        or (
            work is not None
            and work["disposition"] in {
                "HISTORICAL_STALE",
                "HISTORICAL_NONCURRENT",
            }
        )
    )
    return {
        "status": "DEGRADED" if degraded else "READY",
        "reason": (
            "PARTIAL_OR_NONCURRENT_PUBLICATION"
            if degraded
            else None
        ),
        "completed_batches": int(count),
        "latest_completed_at": (
            None if completed_at is None else completed_at.isoformat()
        ),
        "membership": (
            None
            if batch is None
            else {
                "desired_revision_id": batch["desired_revision_id"],
                "desired_snapshot_sha256": batch[
                    "desired_snapshot_sha256"
                ],
                "membership_sha256": batch["membership_sha256"],
                "observation_session": batch[
                    "observation_session"
                ].isoformat(),
                "cutoff": batch["cutoff"].isoformat(),
                "intended_members": intended,
                "effective_members": effective,
                "partial": partial,
                "late_catch_up": bool(batch["late_catch_up"]),
                "deliverable": bool(batch["deliverable"]),
            }
        ),
        "publication": (
            None
            if work is None
            else {
                "work_id": work["work_id"],
                "state": work["state"],
                "disposition": work["disposition"],
                "attempts": int(work["attempts"]),
                "next_attempt_at": work[
                    "next_attempt_at"
                ].isoformat(),
                "failure_reason": work["failure_reason"],
                "release_id": work["release_id"],
                "deliverable": bool(work["deliverable"]),
            }
        ),
    }
