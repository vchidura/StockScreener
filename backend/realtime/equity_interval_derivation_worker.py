"""Closure-keyed raw higher-interval derivation from current final 5m inputs."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .acquisition_ledger import (
    AcquisitionWatermark,
    WatermarkTerminalState,
    advance_acquisition_watermark_cursor,
)
from .contracts import canonical_sha256
from .equity_bar_adapter import RealtimeEquityBarFact
from .equity_bar_repository import persist_realtime_equity_bars_cursor
from .equity_interval_derivation import derive_realtime_interval_facts
from .equity_interval_finalizer import (
    _bar_revision,
    _load_finalizer_security_cursor,
)
from .equity_worker_service import (
    EQUITY_INTERVAL_DERIVE_JOB,
    EquityWorkerConfig,
    PreparedEquityWorkerOutput,
    run_equity_worker_once,
)
from .materialization_jobs import (
    ClaimedMaterializationJob,
    build_source_closure_materialization_job,
    enqueue_materialization_job_cursor,
)
from .preflight import ProviderTier
from .worker_database import RealtimeProcessDatabase


DERIVATION_TARGETS = ("15m", "30m", "1h", "1d")
DERIVATION_POLICY_VERSION = "v2-raw-5m-derivation-v1"
DERIVATION_POLICY_SHA256 = canonical_sha256({
    "version": DERIVATION_POLICY_VERSION,
    "source_interval": "5m",
    "targets": DERIVATION_TARGETS,
    "basis": "RAW_UNADJUSTED",
    "session_scope": "RTH",
})
DERIVATION_BUILD_ID = "v2-incremental-derivation-v1"
ADVANCED_DERIVATION_BUILD_ID = (
    "v2-advanced-interval-derivation-v2"
)
DERIVATION_OVERLAP_POLICY = "v2_derived_exact_source_closure_v1"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("derivation clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _load_current_five_minute_facts(
    cursor,
    *,
    cohort_id: UUID,
    instrument_id: int,
    session_date: date,
    checked_at: datetime,
):
    cursor.execute(
        "SELECT revision.* FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "AND revision.cohort_id=current.cohort_id "
        "AND revision.provider_tier=current.provider_tier "
        "AND revision.instrument_id=current.instrument_id "
        "AND revision.interval=current.interval "
        "WHERE current.cohort_id=%s AND current.instrument_id=%s "
        "AND current.interval='5m' AND current.session_scope='RTH' "
        "AND current.adjustment_basis='RAW_UNADJUSTED' "
        "AND revision.session_date=%s AND revision.is_final "
        "AND revision.observed_at<=%s AND revision.created_at<=%s "
        "AND (revision.replay_available_at IS NULL "
        "OR revision.replay_available_at<=%s) "
        "AND (revision.provider_published_at IS NULL "
        "OR revision.provider_published_at<=%s) "
        "ORDER BY revision.bar_start",
        (
            str(cohort_id),
            instrument_id,
            session_date,
            checked_at,
            checked_at,
            checked_at,
            checked_at,
        ),
    )
    return tuple(
        RealtimeEquityBarFact(
            cohort_id=UUID(str(row["cohort_id"])),
            provider_tier=ProviderTier(row["provider_tier"]),
            instrument_id=int(row["instrument_id"]),
            revision=_bar_revision(row),
        )
        for row in cursor.fetchall()
    )


def _output_fact(
    fact: RealtimeEquityBarFact,
    *,
    build_id: str,
) -> RealtimeEquityBarFact:
    if fact.provider_tier is not ProviderTier.MASSIVE_ADVANCED:
        return fact
    return replace(
        fact,
        revision=replace(
            fact.revision,
            bar_revision_id=uuid5(
                NAMESPACE_URL,
                "stockscreener:advanced-interval-derivation:"
                f"{build_id}:{fact.cohort_id}:"
                f"{fact.revision.bar_revision_id}",
            ),
        ),
    )


def enqueue_interval_derivation_jobs_cursor(
    cursor,
    *,
    cohort_id: UUID,
    instrument_ids: tuple[int, ...],
    session_date: date,
    checked_at: datetime,
    targets: tuple[str, ...] = DERIVATION_TARGETS,
    policy_sha256: str = DERIVATION_POLICY_SHA256,
    build_id: str = DERIVATION_BUILD_ID,
) -> tuple[UUID, ...]:
    checked = _utc(checked_at)
    if (
        not instrument_ids
        or len(set(instrument_ids)) != len(instrument_ids)
        or any(type(value) is not int or value <= 0 for value in instrument_ids)
        or not targets
        or set(targets) - set(DERIVATION_TARGETS)
    ):
        raise ValueError("derivation job scope is invalid")
    jobs = []
    for instrument_id in sorted(instrument_ids):
        sources = _load_current_five_minute_facts(
            cursor,
            cohort_id=cohort_id,
            instrument_id=instrument_id,
            session_date=session_date,
            checked_at=checked,
        )
        if not sources:
            continue
        effective_build_id = (
            ADVANCED_DERIVATION_BUILD_ID
            if sources[0].provider_tier
            is ProviderTier.MASSIVE_ADVANCED
            else build_id
        )
        security = _load_finalizer_security_cursor(
            cursor,
            instrument_id=instrument_id,
            window_end=max(row.revision.bar_end for row in sources),
            as_of=checked,
        )
        for target in targets:
            candidates = derive_realtime_interval_facts(
                security,
                sources,
                target_interval=target,
                observed_at=checked,
                ingestion_segment_id=uuid5(
                    NAMESPACE_URL,
                    f"v2-derivation-preview:{cohort_id}:{instrument_id}:{target}",
                ),
            )
            for candidate in candidates:
                source_ids = candidate.revision.source_bar_revision_ids
                if not source_ids:
                    raise ValueError("derived candidate lacks source lineage")
                bundle = canonical_sha256({
                    "target_interval": target,
                    "bar_start": candidate.revision.bar_start,
                    "bar_end": candidate.revision.bar_end,
                    "source_revision_ids": source_ids,
                })
                job = build_source_closure_materialization_job(
                    job_kind=EQUITY_INTERVAL_DERIVE_JOB,
                    cohort_id=cohort_id,
                    instrument_id=instrument_id,
                    interval=target,
                    source_revision_id=source_ids[0],
                    bundle_sha256=bundle,
                    policy_sha256=policy_sha256,
                    build_id=effective_build_id,
                    available_at=checked,
                )
                enqueue_materialization_job_cursor(cursor, job)
                jobs.append(job.job_id)
    return tuple(jobs)


def prepare_interval_derivation_output(
    database: RealtimeProcessDatabase,
    claimed: ClaimedMaterializationJob,
) -> PreparedEquityWorkerOutput:
    job = claimed.job
    if (
        job.job_kind != EQUITY_INTERVAL_DERIVE_JOB
        or job.interval not in DERIVATION_TARGETS
        or job.source_revision_id is None
        or job.bundle_sha256 is None
        or job.policy_sha256 != DERIVATION_POLICY_SHA256
        or job.build_id not in {
            DERIVATION_BUILD_ID,
            ADVANCED_DERIVATION_BUILD_ID,
        }
    ):
        raise ValueError("claimed derivation job contract is invalid")
    with database.transaction(read_only=True, dict_cursor=True) as cursor:
        cursor.execute(
            "SELECT session_date,bar_start FROM rt.bar_catalog "
            "WHERE bar_revision_id=%s AND cohort_id=%s "
            "AND instrument_id=%s AND interval='5m'",
            (
                str(job.source_revision_id),
                str(job.cohort_id),
                job.instrument_id,
            ),
        )
        source = cursor.fetchone()
        if source is None:
            raise ValueError("derivation job source revision is unavailable")
        sources = _load_current_five_minute_facts(
            cursor,
            cohort_id=job.cohort_id,
            instrument_id=job.instrument_id,
            session_date=source["session_date"],
            checked_at=job.available_at,
        )
        expected_build_id = (
            ADVANCED_DERIVATION_BUILD_ID
            if sources[0].provider_tier
            is ProviderTier.MASSIVE_ADVANCED
            else DERIVATION_BUILD_ID
        )
        if job.build_id != expected_build_id:
            raise ValueError(
                "derivation job provider/build identity changed"
            )
        security = _load_finalizer_security_cursor(
            cursor,
            instrument_id=job.instrument_id,
            window_end=max(row.revision.bar_end for row in sources),
            as_of=job.available_at,
        )
    candidates = derive_realtime_interval_facts(
        security,
        sources,
        target_interval=job.interval,
        observed_at=job.available_at,
        ingestion_segment_id=job.job_id,
    )
    derived = next(
        (
            row for row in candidates
            if row.revision.source_bar_revision_ids
            and row.revision.source_bar_revision_ids[0] == job.source_revision_id
        ),
        None,
    )
    if derived is None:
        raise ValueError("derivation job source window is no longer complete")
    derived = _output_fact(
        derived,
        build_id=job.build_id,
    )
    bundle = canonical_sha256({
        "target_interval": job.interval,
        "bar_start": derived.revision.bar_start,
        "bar_end": derived.revision.bar_end,
        "source_revision_ids": derived.revision.source_bar_revision_ids,
    })
    if bundle != job.bundle_sha256:
        raise ValueError("derivation source closure changed before execution")
    watermark = AcquisitionWatermark(
        cohort_id=job.cohort_id,
        instrument_id=job.instrument_id,
        interval=job.interval,
        source_kind="DERIVED",
        last_complete_bar_end=derived.revision.bar_end,
        last_examined_bar_end=derived.revision.bar_end,
        overlap_policy=DERIVATION_OVERLAP_POLICY,
        terminal_state=WatermarkTerminalState.COMPLETE,
    )

    def persist_output(cursor):
        persist_realtime_equity_bars_cursor(cursor, (derived,))
        advance_acquisition_watermark_cursor(cursor, watermark)

    return PreparedEquityWorkerOutput(
        input_sha256=job.bundle_sha256,
        output_count=1,
        persist_output=persist_output,
    )


def run_interval_derivation_worker_once(
    database: RealtimeProcessDatabase,
    config: EquityWorkerConfig,
    *,
    checked_at: datetime,
):
    if config.job_kind != EQUITY_INTERVAL_DERIVE_JOB:
        raise ValueError("derivation worker requires the derivation job kind")
    return run_equity_worker_once(
        database,
        config,
        prepare=prepare_interval_derivation_output,
        checked_at=checked_at,
    )
