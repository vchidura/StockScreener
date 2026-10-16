"""Closure-keyed cohort technical projection jobs at finalized 15m cutoffs."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from .contracts import canonical_sha256
from .equity_worker_service import (
    EQUITY_PORTAL_PROJECT_JOB,
    EquityWorkerConfig,
    PreparedEquityWorkerOutput,
    run_equity_worker_once,
)
from .lineage_head import inputs_sha256
from .materialization_jobs import (
    ClaimedMaterializationJob,
    build_source_closure_materialization_job,
    enqueue_materialization_job_cursor,
)
from .ticker_repository import compute_realtime_ticker_cycle
from .worker_database import RealtimeProcessDatabase


PORTAL_PROJECT_POLICY_VERSION = "v2-15m-cohort-technical-project-v1"
PORTAL_PROJECT_POLICY_SHA256 = canonical_sha256({
    "version": PORTAL_PROJECT_POLICY_VERSION,
    "trigger_interval": "15m",
    "projection": "V2_TICKER_SCREENING_EVIDENCE",
    "publication": "SEPARATE_ATOMIC_STAGE",
})
PORTAL_PROJECT_BUILD_ID = "v2-incremental-portal-project-v1"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("portal project clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _portal_closure_cursor(cursor, *, cohort_id, cutoff):
    cursor.execute(
        "SELECT manifest.stock_count,member.instrument_id,"
        "current.bar_revision_id FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.subscription_members AS member USING(manifest_id) "
        "JOIN rt.instruments AS instrument USING(instrument_id) "
        "LEFT JOIN rt.bar_current AS current "
        "ON current.cohort_id=cohort.cohort_id "
        "AND current.instrument_id=member.instrument_id "
        "AND current.interval='15m' AND current.bar_end=%s "
        "AND current.session_scope='RTH' "
        "AND current.adjustment_basis='RAW_UNADJUSTED' "
        "WHERE cohort.cohort_id=%s AND instrument.asset_class='EQUITY' "
        "AND member.channel='AM' ORDER BY member.instrument_id",
        (cutoff, str(cohort_id)),
    )
    rows = tuple(cursor.fetchall())
    if (
        not rows
        or len(rows) != int(rows[0][0])
        or any(row[2] is None for row in rows)
        or len({int(row[1]) for row in rows}) != len(rows)
    ):
        return None
    heads = tuple((int(row[1]), UUID(str(row[2]))) for row in rows)
    return {
        "heads": heads,
        "bundle_sha256": canonical_sha256({
            "cohort_id": cohort_id,
            "cutoff": cutoff,
            "interval": "15m",
            "heads": heads,
        }),
    }


def enqueue_portal_projection_job_cursor(
    cursor,
    *,
    cohort_id: UUID,
    cutoff: datetime,
    available_at: datetime,
) -> UUID | None:
    boundary = _utc(cutoff)
    available = _utc(available_at)
    closure = _portal_closure_cursor(
        cursor,
        cohort_id=cohort_id,
        cutoff=boundary,
    )
    if closure is None:
        return None
    first_instrument, first_revision = closure["heads"][0]
    job = build_source_closure_materialization_job(
        job_kind=EQUITY_PORTAL_PROJECT_JOB,
        cohort_id=cohort_id,
        instrument_id=first_instrument,
        interval="15m",
        source_revision_id=first_revision,
        bundle_sha256=closure["bundle_sha256"],
        policy_sha256=PORTAL_PROJECT_POLICY_SHA256,
        build_id=PORTAL_PROJECT_BUILD_ID,
        available_at=available,
    )
    enqueue_materialization_job_cursor(cursor, job)
    return job.job_id


def prepare_portal_projection_output(
    database: RealtimeProcessDatabase,
    claimed: ClaimedMaterializationJob,
) -> PreparedEquityWorkerOutput:
    job = claimed.job
    if (
        job.job_kind != EQUITY_PORTAL_PROJECT_JOB
        or job.interval != "15m"
        or job.source_revision_id is None
        or job.bundle_sha256 is None
        or job.policy_sha256 != PORTAL_PROJECT_POLICY_SHA256
        or job.build_id != PORTAL_PROJECT_BUILD_ID
    ):
        raise ValueError("claimed portal projection job contract is invalid")
    with database.transaction(read_only=True) as cursor:
        cursor.execute(
            "SELECT session_date,bar_end FROM rt.bar_catalog "
            "WHERE bar_revision_id=%s AND cohort_id=%s "
            "AND instrument_id=%s AND interval='15m'",
            (
                str(job.source_revision_id),
                str(job.cohort_id),
                job.instrument_id,
            ),
        )
        source = cursor.fetchone()
        if source is None:
            raise ValueError("portal projection source revision is unavailable")
        session_date, cutoff = source
        closure = _portal_closure_cursor(
            cursor,
            cohort_id=job.cohort_id,
            cutoff=cutoff,
        )
        cursor.execute(
            "SELECT count(*) FROM rt.seed_manifests "
            "WHERE status='READY' AND created_at<=%s",
            (cutoff,),
        )
        generation = int(cursor.fetchone()[0])
    if closure is None or closure["bundle_sha256"] != job.bundle_sha256:
        raise ValueError("portal projection source closure changed")
    if generation < 1:
        raise ValueError("portal projection reference generation is unavailable")
    cycle = compute_realtime_ticker_cycle(
        database,
        state=None,
        loader=None,
        cohort_id=job.cohort_id,
        session=session_date,
        cutoff=cutoff,
        known_by=job.available_at,
        source_generation=generation,
    )
    inputs_sha256(cycle.inputs)

    def persist_output(_cursor):
        return None

    return PreparedEquityWorkerOutput(
        input_sha256=job.bundle_sha256,
        output_count=len(cycle.projection.snapshots),
        persist_output=persist_output,
    )


def run_portal_projection_worker_once(
    database: RealtimeProcessDatabase,
    config: EquityWorkerConfig,
    *,
    checked_at: datetime,
):
    if config.job_kind != EQUITY_PORTAL_PROJECT_JOB:
        raise ValueError("portal worker requires the portal project job kind")

    return run_equity_worker_once(
        database,
        config,
        prepare=prepare_portal_projection_output,
        checked_at=checked_at,
    )
