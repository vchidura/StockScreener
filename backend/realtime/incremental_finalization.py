"""Five-minute micro-window coordination for incremental V2 acquisition."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars

from .contracts import canonical_sha256
from .database_access import RealtimeDatabaseRole
from .equity_interval_finalizer import (
    FINALIZER_POLICY_SHA256,
    FiveMinuteFinalizerInputPending,
    enqueue_five_minute_finalizer_jobs_cursor,
    run_five_minute_finalizer_once,
)
from .equity_worker_service import (
    EQUITY_INTERVAL_FINALIZE_JOB,
    EquityWorkerConfig,
)
from .materialization_jobs import evaluate_job_barrier_cursor
from .orchestration_repository import (
    PostgresOrchestrationRepository,
    WindowSpec,
)
from .stage_database_v2 import V2WorkerDatabase


INCREMENTAL_FINALIZER_BUILD_ID = "v2-incremental-finalizer-v1"
INCREMENTAL_FINALIZER_SCOPE = "v2-incremental-five-minute-v1"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("incremental finalizer clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _boundaries(session_date: date, through: datetime):
    calendar = exchange_calendars.get_calendar("XNYS")
    key = session_date.isoformat()
    opened = calendar.session_open(key).to_pydatetime().astimezone(timezone.utc)
    closed = calendar.session_close(key).to_pydatetime().astimezone(timezone.utc)
    end = min(_utc(through), closed)
    boundary = opened + timedelta(minutes=5)
    values = []
    while boundary <= end:
        values.append(boundary)
        boundary += timedelta(minutes=5)
    return tuple(values)


def finalize_incremental_five_minute_windows(
    provider,
    *,
    cohort_id: UUID,
    session_date: date,
    instrument_ids: tuple[int, ...],
    through: datetime,
    checked_at: datetime,
    authorization_reference: str,
    plan_deadline: datetime,
    provider_tier: str = "MASSIVE_DEVELOPER",
) -> tuple[dict, ...]:
    checked = _utc(checked_at)
    deadline = _utc(plan_deadline)
    if (
        not instrument_ids
        or len(set(instrument_ids)) != len(instrument_ids)
        or any(type(value) is not int or value <= 0 for value in instrument_ids)
        or not authorization_reference.strip()
        or provider_tier not in {"MASSIVE_DEVELOPER", "MASSIVE_ADVANCED"}
    ):
        raise ValueError("incremental finalizer scope is invalid")
    repository = PostgresOrchestrationRepository(provider)
    config_sha256 = canonical_sha256({
        "scope": INCREMENTAL_FINALIZER_SCOPE,
        "cohort_id": cohort_id,
        "session_date": session_date,
        "instrument_ids": instrument_ids,
        "policy_sha256": FINALIZER_POLICY_SHA256,
        "build_id": INCREMENTAL_FINALIZER_BUILD_ID,
        "authorization_reference": authorization_reference.strip(),
        "plan_deadline": deadline,
        "provider_tier": provider_tier,
    })
    plan_id = repository.create_plan(
        cohort_id,
        session_date,
        ("EQUITY_MATERIALIZE",),
        config_sha256=config_sha256,
        deadline=deadline,
        scope_reference=authorization_reference.strip(),
        catalog_version="stages_v2",
    )
    owner_id = uuid5(
        NAMESPACE_URL,
        f"v2-incremental-finalizer-owner:{cohort_id}:{session_date}",
    )
    compute = V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)
    reports = []
    for cutoff in _boundaries(session_date, through):
        key = f"inc5-{cutoff:%Y%m%dT%H%MZ}"
        spec = WindowSpec(
            cohort_id,
            provider_tier,
            key,
            cutoff + timedelta(minutes=15),
            cutoff,
            cutoff + timedelta(minutes=20),
        )
        record = repository.ensure_window(plan_id, spec)
        if record.state == "COMPLETE":
            reports.append({
                "cutoff": cutoff,
                "status": "ALREADY_COMPLETE",
                "bars": 0,
            })
            continue
        claim = repository.claim_window(
            record.window_id,
            owner_id,
            checked,
            lease_seconds=300,
        )
        if claim is None:
            reports.append({
                "cutoff": cutoff,
                "status": "NOT_CLAIMED",
                "bars": 0,
            })
            continue
        barrier_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:equity-5m-finalizer:{record.window_id}",
        )
        try:
            with compute.transaction() as cursor:
                enqueue_five_minute_finalizer_jobs_cursor(
                    cursor,
                    cohort_id=cohort_id,
                    window_id=record.window_id,
                    instrument_ids=instrument_ids,
                    checked_at=checked,
                    policy_sha256=FINALIZER_POLICY_SHA256,
                    build_id=INCREMENTAL_FINALIZER_BUILD_ID,
                    barrier_id=barrier_id,
                )
        except FiveMinuteFinalizerInputPending:
            repository.release_window(claim, checked)
            reports.append({
                "cutoff": cutoff,
                "status": "INPUT_PENDING",
                "bars": 0,
            })
            continue
        total_bars = 0
        failed = False
        reason_codes = []
        config = EquityWorkerConfig(
            cohort_id=cohort_id,
            job_kind=EQUITY_INTERVAL_FINALIZE_JOB,
            lease_owner=uuid5(
                NAMESPACE_URL,
                f"v2-incremental-finalizer-lease:{record.window_id}",
            ),
            claim_limit=min(100, len(instrument_ids)),
        )
        for _ in range((len(instrument_ids) + config.claim_limit - 1) // config.claim_limit + 1):
            status = run_five_minute_finalizer_once(
                compute,
                config,
                checked_at=checked,
            )
            total_bars += status.output_count
            if status.failed or status.blocked:
                failed = True
            reason_codes.extend(status.reason_codes)
            if status.claimed == 0:
                break
        with compute.transaction() as cursor:
            verdict = evaluate_job_barrier_cursor(cursor, barrier_id)
        complete = verdict.state == "COMPLETE" and not failed
        repository.finish_window(
            claim,
            "COMPLETE" if complete else "FAILED",
            checked,
        )
        reports.append({
            "cutoff": cutoff,
            "status": "COMPLETE" if complete else "FAILED",
            "bars": total_bars,
            "barrier_state": verdict.state,
            "reason_codes": tuple(dict.fromkeys(reason_codes)),
        })
    return tuple(reports)
