"""Realtime-only Stock Alerts writer and retained, request-time read projection."""
from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections.abc import Mapping
from uuid import UUID

from research.stock_alert_review import build_stock_eod_review
from research.stock_alerts import alert_page, session_dates

from .contracts import canonical_sha256
from .database_access import RealtimeDatabaseRole
from .lineage_head import reference_set, require_member_set
from .portal_compatibility_repository import (
    SNAPSHOT_RELATION, _RealtimeCompatibilityRepository, _active_cohort, _utc, action_evidence_until,
    load_current_release,
)
from .portal_projection_repository import (
    PortalReleaseReceipt,
    persist_portal_snapshot_cursor,
    publish_portal_release_cursor,
)
from .portal_publisher import (
    PortalPublicationBatch,
    PortalPublicationConfig,
    resume_portal_publication_once,
    run_portal_publication_once,
)
from .screening_repository import (
    load_realtime_canonical_stock_alert_input,
    load_realtime_stock_alert_input,
)
from .reference_continuity import source_manifest_identity_cursor
from .stock_alert_projection import STOCK_ALERTS_VERSION, produce_realtime_stock_alerts
from .worker_database import RealtimeProcessDatabase


class StockAlertPublicationUnavailable(LookupError):
    pass


class StockAlertQueryRejected(ValueError):
    pass


class RetainedForwardState(ValueError):
    """The retained alert release carries forward state this producer cannot reproduce."""


def compute_realtime_stock_alerts(
    compute_database: RealtimeProcessDatabase,
    *,
    retained_projection=None,
    **scope,
):
    """STOCK_ALERTS compute half: read-only. Loads the screening input and produces the snapshot;
    nothing is persisted or published. Refuses to run over a retained release with forward state."""
    if not isinstance(compute_database, RealtimeProcessDatabase) or compute_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Realtime alert production requires the target-local COMPUTE role")
    with compute_database.transaction(
        read_only=True, dict_cursor=True, isolation_level="REPEATABLE READ",
    ) as cursor:
        release = load_current_release(cursor, scope["cohort_id"])
        entry = release.entry(STOCK_ALERTS_VERSION, "PLATFORM") if release is not None else None
        previous = None
        if entry is not None:
            cursor.execute(
                "SELECT snapshot.payload IS NOT NULL AS retained_body, "
                "snapshot.payload->'streams' <> '[]'::jsonb AS retained_forward "
                "FROM " + SNAPSHOT_RELATION + " WHERE snapshot.snapshot_id=%s",
                (str(entry["snapshot_id"]),),
            )
            previous = cursor.fetchone()
        if (
            retained_projection is None
            and previous is not None
            and previous["retained_body"]
            and previous["retained_forward"] is not False
        ):
            raise RetainedForwardState(
                "Forward checkpoint loader is unavailable; refusing to replace retained publications")
        # A canonical projection is bound to the exact input its batch was built from.
        canonical = (
            isinstance(retained_projection, Mapping)
            and retained_projection.get("contract")
            == "canonical-stock-alert-retained-projection-v1"
        )
        inputs = (
            load_realtime_canonical_stock_alert_input
            if canonical
            else load_realtime_stock_alert_input
        )(cursor, **scope)
        return produce_realtime_stock_alerts(
            inputs,
            retained_projection=retained_projection,
        )


def publish_realtime_stock_alerts(
    publication_database: RealtimeProcessDatabase,
    snapshot,
    *,
    receipt=None,
    session_close=False,
    owner_id: UUID | None = None,
    owner_epoch: int = 1,
    checked_at: datetime | None = None,
):
    """STOCK_ALERTS publication half: persist the snapshot and, with a receipt, publish the intraday
    release plus (at session close) the kept session-close release."""
    if not isinstance(publication_database, RealtimeProcessDatabase) or publication_database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION:
        raise ValueError("Realtime alert persistence requires the target-local PUBLICATION role")
    if receipt is None:
        raise ValueError(
            "Realtime alert publication requires its source receipt"
        )
    if not isinstance(receipt, PortalReleaseReceipt):
        raise TypeError("Realtime alert source receipt is invalid")
    publisher_owner = owner_id or receipt.receipt_id
    run_portal_publication_once(
        publication_database,
        PortalPublicationConfig(
            receipt.cohort_id,
            receipt.provider_tier,
            publisher_owner,
            owner_epoch,
            dispatch_authorized=False,
        ),
        PortalPublicationBatch(
            (snapshot,),
            receipt=receipt,
            session_close=session_close,
            product_key="STOCK_ALERTS",
        ),
        None,
        checked_at=(
            max(snapshot.generated_at, receipt.cutoff)
            if checked_at is None
            else checked_at
        ),
        snapshot_repository=persist_portal_snapshot_cursor,
        release_repository=publish_portal_release_cursor,
    )
    return snapshot


def resume_realtime_stock_alert_publication(
    publication_database: RealtimeProcessDatabase,
    receipt: PortalReleaseReceipt,
    *,
    owner_id: UUID,
    owner_epoch: int,
    checked_at: datetime,
):
    if not isinstance(publication_database, RealtimeProcessDatabase) or (
        publication_database.process_plan.role
        is not RealtimeDatabaseRole.PUBLICATION
    ):
        raise ValueError(
            "Realtime alert retry requires the target-local PUBLICATION role"
        )
    return resume_portal_publication_once(
        publication_database,
        PortalPublicationConfig(
            receipt.cohort_id,
            receipt.provider_tier,
            owner_id,
            owner_epoch,
            dispatch_authorized=False,
        ),
        product_key="STOCK_ALERTS",
        source_receipt_id=receipt.receipt_id,
        checked_at=checked_at,
    )


def materialize_realtime_stock_alerts(compute_database: RealtimeProcessDatabase,
                                      publication_database: RealtimeProcessDatabase, *, receipt=None,
                                      session_close=False, **scope):
    """Compute then publish in one call, each under its own capability role."""
    if not isinstance(compute_database, RealtimeProcessDatabase) or compute_database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Realtime alert production requires the target-local COMPUTE role")
    if not isinstance(publication_database, RealtimeProcessDatabase) or publication_database.process_plan.role is not RealtimeDatabaseRole.PUBLICATION:
        raise ValueError("Realtime alert persistence requires the target-local PUBLICATION role")
    snapshot = compute_realtime_stock_alerts(compute_database, **scope)
    return publish_realtime_stock_alerts(
        publication_database, snapshot, receipt=receipt, session_close=session_close,
    )


class RealtimeStockAlertViewRepository(_RealtimeCompatibilityRepository):
    def _snapshot(self, cutoff, cohort_id):
        with self._cursor() as cursor:
            self._begin(cursor)
            if cohort_id is None:
                active = _active_cohort(cursor, cutoff)
                if active is None:
                    raise StockAlertPublicationUnavailable("Realtime alert cohort is unavailable")
                cohort_id = UUID(str(active["cohort_id"]))
            release = self._release(cursor, cohort_id)
            entry = release.entry(STOCK_ALERTS_VERSION, "PLATFORM") if release is not None else None
            if entry is None:
                raise StockAlertPublicationUnavailable("Realtime Stock Alerts has no retained projection")
            cursor.execute(
                "SELECT snapshot.*, manifest.manifest_id AS cohort_manifest_id, "
                "manifest.manifest_sha256 AS cohort_manifest_sha256, "
                "history_binding.binding_sha256 AS history_binding_sha256 "
                "FROM " + SNAPSHOT_RELATION + " "
                "JOIN rt.cohorts AS cohort ON cohort.cohort_id=snapshot.cohort_id "
                "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
                "LEFT JOIN rt.cohort_history_bindings AS history_binding "
                "ON history_binding.target_cohort_id=cohort.cohort_id "
                "JOIN rt.screening_current AS screening_current ON screening_current.cohort_id=cohort.cohort_id "
                "JOIN rt.screening_generations AS screening ON screening.generation_id=screening_current.generation_id "
                "AND screening.cohort_id=screening_current.cohort_id AND screening.status='COMPLETE' "
                "WHERE snapshot.cohort_id=%s AND snapshot.snapshot_type=%s AND snapshot.snapshot_key=%s "
                "AND snapshot.generated_at<=%s AND cohort.created_at<=%s AND cohort.effective_at<=%s "
                "AND manifest.created_at<=%s AND screening.generated_at<=%s "
                "AND snapshot.provider_tier=screening.provider_tier AND snapshot.snapshot_id=%s",
                (str(cohort_id), STOCK_ALERTS_VERSION, "PLATFORM", cutoff, cutoff, cutoff, cutoff, cutoff,
                 str(entry["snapshot_id"])),
            )
            row = cursor.fetchone()
            if row is None or row["payload"] is None:
                raise StockAlertPublicationUnavailable("Realtime Stock Alerts has no retained projection")
            manifest, payload = row["source_manifest"], row["payload"]
            expected_source_manifest_sha256, _ = source_manifest_identity_cursor(
                cursor,
                manifest,
                manifest_id=row["cohort_manifest_id"],
                cohort_manifest_sha256=row["cohort_manifest_sha256"],
                history_binding_sha256=row["history_binding_sha256"],
                cutoff=row["generated_at"],
            )
            if (canonical_sha256(manifest) != row["source_manifest_sha256"]
                or canonical_sha256(payload) != row["payload_sha256"]
                or manifest["version"] != STOCK_ALERTS_VERSION or payload["version"] != STOCK_ALERTS_VERSION
                or str(manifest["cohort_id"]) != str(cohort_id)
                or manifest["source_manifest_sha256"] != expected_source_manifest_sha256
                or manifest["source_generation"] != row["source_generation"]
                or manifest["provider_tier"] != row["provider_tier"]
                or any(payload[key] is not False for key in (
                    "provider_fetch_performed", "publication_permission", "execution_permission",
                ))):
                raise ValueError("Stored realtime alert identity/checksum drift")
            references = payload["references"]
            require_member_set(manifest["reference_set"], reference_set(references), name="Stock Alerts")
            reference_ids = [
                str(reference["security_id"])
                for reference in references
            ]
            cursor.execute(
                "SELECT security_id, ticker, revision_id, payload_sha256 "
                "FROM ref.security_revisions "
                "WHERE security_id=ANY(%s::uuid[]) "
                "AND effective_from<=%s "
                "AND (effective_to IS NULL OR effective_to>%s) "
                "AND observed_at<=%s "
                "ORDER BY security_id,effective_from DESC,"
                "observed_at DESC,revision_id",
                (reference_ids, cutoff, cutoff, cutoff),
            )
            current_by_security = {}
            for current in cursor.fetchall():
                current_by_security.setdefault(
                    str(current["security_id"]),
                    [],
                ).append(current)
            for reference in references:
                current = current_by_security.get(
                    str(reference["security_id"]),
                    (),
                )
                if (
                    len(current) != 1
                    or current[0]["ticker"] != reference["ticker"]
                    or str(current[0]["revision_id"])
                    != str(reference["security_revision_id"])
                    or current[0]["payload_sha256"]
                    != reference["payload_sha256"]
                ):
                    raise StockAlertPublicationUnavailable("Realtime alert reference changed; republish required")
            ids = reference_ids
            until = action_evidence_until(row["generated_at"], cutoff)
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM ref.corporate_action_revisions "
                "WHERE security_id=ANY(%s::uuid[]) AND received_at>%s AND received_at<=%s) "
                "OR EXISTS (SELECT 1 FROM ref.corporate_action_coverage "
                "WHERE security_id=ANY(%s::uuid[]) AND received_at>%s AND received_at<=%s) AS action_drift",
                (ids, row["generated_at"], until, ids, row["generated_at"], until),
            )
            if cursor.fetchone()["action_drift"]:
                raise StockAlertPublicationUnavailable("Realtime alert action evidence changed; republish required")
            cursor.execute(
                "SELECT work_id::text,source_receipt_id::text,cutoff,"
                "publication_generation,desired_snapshot_sha256,"
                "membership_sha256,state,disposition,deliverable,"
                "attempts,next_attempt_at,failure_reason,release_id::text,"
                "completed_at FROM rt.portal_publication_work "
                "WHERE cohort_id=%s AND product_key='STOCK_ALERTS' "
                "ORDER BY cutoff DESC,created_at DESC LIMIT 1",
                (str(cohort_id),),
            )
            publication_work = cursor.fetchone()
        result = deepcopy(payload)
        result["current_disposition"] = "CURRENT"
        result["current_release"] = {
            "release_id": str(release.release_id),
            "cutoff": release.cutoff.isoformat(),
            "publication_generation": (
                release.publication_generation
            ),
            "desired_snapshot_sha256": (
                release.desired_snapshot_sha256
            ),
            "membership_sha256": release.membership_sha256,
            "deliverable": release.deliverable,
        }
        result["publication_status"] = (
            None
            if publication_work is None
            else {
                "work_id": str(publication_work["work_id"]),
                "source_receipt_id": str(
                    publication_work["source_receipt_id"]
                ),
                "cutoff": publication_work["cutoff"].isoformat(),
                "publication_generation": int(
                    publication_work["publication_generation"]
                ),
                "desired_snapshot_sha256": publication_work[
                    "desired_snapshot_sha256"
                ],
                "membership_sha256": publication_work[
                    "membership_sha256"
                ],
                "state": publication_work["state"],
                "disposition": publication_work["disposition"],
                "deliverable": bool(
                    publication_work["deliverable"]
                ),
                "attempts": int(publication_work["attempts"]),
                "next_attempt_at": publication_work[
                    "next_attempt_at"
                ].isoformat(),
                "failure_reason": publication_work["failure_reason"],
                "release_id": (
                    None
                    if publication_work["release_id"] is None
                    else str(publication_work["release_id"])
                ),
                "completed_at": (
                    None
                    if publication_work["completed_at"] is None
                    else publication_work[
                        "completed_at"
                    ].isoformat()
                ),
                "current": (
                    publication_work["release_id"] is not None
                    and str(publication_work["release_id"])
                    == str(release.release_id)
                ),
            }
        )
        if isinstance(result.get("shadow"), dict):
            result["shadow"]["current_disposition"] = "CURRENT"
            result["shadow"]["publication_status"] = result[
                "publication_status"
            ]
        return result, row["generated_at"]

    def alert_view(self, *, available_by, cohort_id=None, source="SHADOW", session_date=None,
                   combined=None, offset=0, limit=100, **filters):
        cutoff = _utc(available_by, "available_by")
        if source not in {"SHADOW", "REPLAY"} or offset < 0 or not 1 <= limit <= 200:
            raise StockAlertQueryRejected("Invalid realtime alert source/page bounds")
        payload, generated_at = self._snapshot(cutoff, cohort_id)
        if source == "REPLAY":
            raise StockAlertPublicationUnavailable(payload["missing_producers"]["replay"])
        snapshot = payload["shadow"]
        if combined is False and not payload.get("canonical_product"):
            snapshot = next((stream for stream in payload["streams"]
                             if stream["strategy_instance"]["stream"] == "intraday"), None)
            if snapshot is None:
                raise StockAlertPublicationUnavailable("Realtime intraday forward stream is unavailable")
        dates = session_dates(cutoff.astimezone(ZoneInfo("America/New_York")).date().isoformat())
        snapshot.update(sessions=dates, current_session=dates[-1])
        if cutoff - generated_at > timedelta(minutes=50):
            snapshot["warnings"].append("Realtime retained alert data stale; no request-time refresh performed")
            for stream in snapshot.get("strategy_streams", []):
                stream.update(status="STALE", projector_stale=True)
        try:
            page = alert_page(snapshot, session=str(session_date) if session_date else None,
                              offset=offset, limit=limit, **filters)
        except ValueError as error:
            raise StockAlertQueryRejected(str(error)) from error
        schedules = payload["schedule_streams"]
        for name in ("intraday", "swing"):
            if not any(stream["stream"] == name for stream in schedules):
                schedules.append(dict(stream=name, label=name.title(), status="UNAVAILABLE"))
        for schedule in schedules:
            if schedule.get("publication_deadline") and datetime.fromisoformat(schedule["publication_deadline"]) < cutoff:
                schedule["status"] = "OVERDUE"
        page["schedule_streams"] = schedules
        page.update(provider_fetch_performed=False, execution_permission=False,
                    missing_producers=payload["missing_producers"])
        return page

    def eod_review(self, *, available_by, cohort_id=None, session_date=None, **filters):
        cutoff = _utc(available_by, "available_by")
        payload, generated_at = self._snapshot(cutoff, cohort_id)
        if payload.get("canonical_product"):
            dates = session_dates(
                cutoff.astimezone(
                    ZoneInfo("America/New_York")
                ).date().isoformat()
            )
            session = str(session_date) if session_date else dates[-1]
            result = build_stock_eod_review(
                [],
                {},
                as_of=generated_at,
                session_date=session,
                sessions=dates,
                storage_ready=False,
                **filters,
            )
            result.update(
                missing_producers={
                    "outcomes": (
                        "Canonical Stock Alert outcomes are not implemented"
                    )
                },
                provider_fetch_performed=False,
                read_at=cutoff.isoformat(),
            )
            return result
        dates = session_dates(cutoff.astimezone(ZoneInfo("America/New_York")).date().isoformat())
        session = str(session_date) if session_date else dates[-1]
        if session not in dates:
            raise StockAlertQueryRejected("Session outside the retained 21-session navigation window")
        records = [record for record in payload["review_publications"] if record["payload"]["session"] == session]
        outcomes = {}
        for stream in payload["streams"]:
            instance = stream["strategy_instance"]["instance_id"]
            for row in stream["alerts"]:
                outcomes[(instance, row["original_alert_id"])] = row
        try:
            result = build_stock_eod_review(
                records, outcomes, as_of=generated_at, session_date=session, sessions=dates,
                storage_ready=bool(payload["streams"]), **filters,
            )
        except ValueError as error:
            raise StockAlertQueryRejected(str(error)) from error
        result.update(missing_producers=payload["missing_producers"], provider_fetch_performed=False,
                      read_at=cutoff.isoformat())
        return result
