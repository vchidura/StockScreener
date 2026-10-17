from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID

from equity.derivation import (
    derive_advanced_sparse_minute_window,
    derive_minute_bars,
)
from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
    SecurityReferenceRevision,
)

from .acquisition_ledger import (
    AcquisitionWatermark,
    WatermarkTerminalState,
    advance_acquisition_watermark_cursor,
)
from .contracts import canonical_sha256
from .equity_bar_adapter import adapt_derived_equity_bar
from .equity_bar_repository import persist_realtime_equity_bars_cursor
from .equity_worker_service import (
    EQUITY_INTERVAL_FINALIZE_JOB,
    EquityWorkerConfig,
    EquityWorkerStatus,
    PreparedEquityWorkerOutput,
    run_equity_worker_once,
)
from .materialization_jobs import (
    BarrierMember,
    BarrierOutcome,
    ClaimedMaterializationJob,
    MaterializationJob,
    build_closure_materialization_job,
    enqueue_materialization_job_cursor,
    open_job_barrier_cursor,
)
from .preflight import ProviderTier
from .worker_database import RealtimeProcessDatabase


FINALIZER_BARRIER_KIND = "EQUITY_5M_FINALIZE"
FINALIZER_OVERLAP_POLICY = "XNYS_REALTIME_1M_TO_5M_V1"
FINALIZER_POLICY_SHA256 = canonical_sha256(
    "stages_v2-equity-1m-to-5m-finalizer-v1"
)
MAX_FINALIZER_ENQUEUE_MEMBERS = 500
_FINALIZER_INTERVAL = "5m"
_SOURCE_INTERVAL = "1m"
_SOURCE_DURATION = timedelta(minutes=1)
_TARGET_DURATION = timedelta(minutes=5)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


class FiveMinuteFinalizerOutcome(str, Enum):
    COMPLETE = "COMPLETE"
    GAP = "GAP"
    LATE = "LATE"
    NO_TRADE = "NO_TRADE"


class FiveMinuteFinalizerInputPending(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class FiveMinuteFinalizerWindow:
    cohort_id: UUID
    provider_tier: ProviderTier
    window_id: UUID
    window_start: datetime
    window_end: datetime
    deadline: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.provider_tier, ProviderTier):
            raise TypeError("finalizer provider tier is invalid")
        start = _utc(self.window_start, "window_start")
        end = _utc(self.window_end, "window_end")
        deadline = _utc(self.deadline, "deadline")
        if end - start != _TARGET_DURATION or deadline < end:
            raise ValueError("finalizer window must be one closed five-minute interval")
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)
        object.__setattr__(self, "deadline", deadline)


@dataclass(frozen=True, slots=True)
class FiveMinuteFinalizerInput:
    window: FiveMinuteFinalizerWindow
    instrument_id: int
    security: SecurityReferenceRevision
    source_bars: tuple[EquityBarRevision, ...]
    checked_at: datetime
    policy_sha256: str
    build_id: str
    outcome: FiveMinuteFinalizerOutcome
    gap_free_coverage: bool = False

    def __post_init__(self) -> None:
        if type(self.instrument_id) is not int or self.instrument_id <= 0:
            raise ValueError("finalizer instrument ID must be positive")
        if not isinstance(self.security, SecurityReferenceRevision):
            raise TypeError("finalizer security reference is invalid")
        rows = tuple(self.source_bars)
        if any(not isinstance(row, EquityBarRevision) for row in rows):
            raise TypeError("finalizer source bars are invalid")
        checked = _utc(self.checked_at, "checked_at")
        build_id = self.build_id.strip()
        if not 1 <= len(build_id) <= 96:
            raise ValueError("finalizer build ID must contain 1-96 characters")
        if not isinstance(self.outcome, FiveMinuteFinalizerOutcome):
            raise TypeError("finalizer outcome is invalid")
        if type(self.gap_free_coverage) is not bool:
            raise TypeError("finalizer coverage proof must be boolean")
        object.__setattr__(self, "source_bars", rows)
        object.__setattr__(self, "checked_at", checked)
        object.__setattr__(
            self, "policy_sha256", _sha256(self.policy_sha256, "policy_sha256")
        )
        object.__setattr__(self, "build_id", build_id)

    @property
    def input_sha256(self) -> str:
        return canonical_sha256(
            {
                "contract": "realtime-equity-1m-to-5m-finalizer-v1",
                "cohort_id": self.window.cohort_id,
                "provider_tier": self.window.provider_tier.value,
                "instrument_id": self.instrument_id,
                "window_id": self.window.window_id,
                "window_start": self.window.window_start,
                "window_end": self.window.window_end,
                "deadline": self.window.deadline,
                "security_revision_id": self.security.security_revision_id,
                "security_payload_sha256": self.security.payload_sha256,
                "policy_sha256": self.policy_sha256,
                "build_id": self.build_id,
                "outcome": self.outcome.value,
                "gap_free_coverage": self.gap_free_coverage,
                "source_bars": tuple(
                    (
                        row.bar_revision_id,
                        row.bar_start,
                        row.bar_end,
                        row.system_observed_at,
                        row.payload_sha256,
                    )
                    for row in self.source_bars
                ),
            }
        )


@dataclass(frozen=True, slots=True)
class EnqueuedFiveMinuteFinalizerJob:
    job: MaterializationJob
    input: FiveMinuteFinalizerInput


def _validate_source_bars(
    window: FiveMinuteFinalizerWindow,
    security: SecurityReferenceRevision,
    rows: tuple[EquityBarRevision, ...],
    checked_at: datetime,
) -> None:
    starts = [row.bar_start for row in rows]
    if len(starts) != len(set(starts)):
        raise ValueError("finalizer source bars contain duplicate windows")
    for row in rows:
        if (
            row.security_id != security.security_id
            or row.ticker != security.ticker
            or row.interval != _SOURCE_INTERVAL
            or not row.is_final
            or row.adjusted
            or row.session_scope is not BarSessionScope.RTH
            or row.bar_start < window.window_start
            or row.bar_end > window.window_end
            or row.system_observed_at > checked_at
        ):
            raise ValueError("finalizer source bar violates the raw final 1m contract")


def _has_complete_source_window(
    window: FiveMinuteFinalizerWindow,
    rows: tuple[EquityBarRevision, ...],
) -> bool:
    expected_starts = tuple(
        window.window_start + index * _SOURCE_DURATION for index in range(5)
    )
    by_start = {row.bar_start: row for row in rows}
    if tuple(sorted(by_start)) != expected_starts:
        return False
    ordered = tuple(by_start[start] for start in expected_starts)
    return (
        ordered[0].bar_start == window.window_start
        and ordered[-1].bar_end == window.window_end
        and all(
            row.bar_end - row.bar_start == _SOURCE_DURATION for row in ordered
        )
        and all(
            left.bar_end == right.bar_start
            for left, right in zip(ordered, ordered[1:])
        )
    )


def build_five_minute_finalizer_input(
    window: FiveMinuteFinalizerWindow,
    *,
    instrument_id: int,
    security: SecurityReferenceRevision,
    source_bars: Sequence[EquityBarRevision],
    checked_at: datetime,
    policy_sha256: str,
    build_id: str,
    gap_free_coverage: bool = False,
) -> FiveMinuteFinalizerInput:
    if not isinstance(window, FiveMinuteFinalizerWindow):
        raise TypeError("finalizer window is invalid")
    checked = _utc(checked_at, "checked_at")
    rows = tuple(sorted(source_bars, key=lambda row: row.bar_start))
    _validate_source_bars(window, security, rows, checked)
    complete_source = _has_complete_source_window(window, rows)
    if complete_source:
        outcome = (
            FiveMinuteFinalizerOutcome.LATE
            if checked > window.deadline
            else FiveMinuteFinalizerOutcome.COMPLETE
        )
    elif (
        window.provider_tier is ProviderTier.MASSIVE_ADVANCED
        and gap_free_coverage
    ):
        outcome = (
            FiveMinuteFinalizerOutcome.NO_TRADE
            if not rows
            else FiveMinuteFinalizerOutcome.LATE
            if checked > window.deadline
            else FiveMinuteFinalizerOutcome.COMPLETE
        )
    elif checked >= window.deadline:
        outcome = FiveMinuteFinalizerOutcome.GAP
    else:
        raise FiveMinuteFinalizerInputPending(
            f"instrument {instrument_id} finalizer input is still ambiguous"
        )
    return FiveMinuteFinalizerInput(
        window=window,
        instrument_id=instrument_id,
        security=security,
        source_bars=rows,
        checked_at=checked,
        policy_sha256=policy_sha256,
        build_id=build_id,
        outcome=outcome,
        gap_free_coverage=gap_free_coverage,
    )


def build_five_minute_finalizer_job(
    prepared_input: FiveMinuteFinalizerInput,
    *,
    policy_sha256: str,
    build_id: str,
) -> MaterializationJob:
    if not isinstance(prepared_input, FiveMinuteFinalizerInput):
        raise TypeError("finalizer input is invalid")
    policy = _sha256(policy_sha256, "policy_sha256")
    build = build_id.strip()
    if policy != prepared_input.policy_sha256 or build != prepared_input.build_id:
        raise ValueError("finalizer job policy or build does not match its input")
    return build_closure_materialization_job(
        job_kind=EQUITY_INTERVAL_FINALIZE_JOB,
        cohort_id=prepared_input.window.cohort_id,
        instrument_id=prepared_input.instrument_id,
        interval=_FINALIZER_INTERVAL,
        primary_window_id=prepared_input.window.window_id,
        policy_sha256=policy,
        build_id=build,
        available_at=prepared_input.checked_at,
        bundle_sha256=prepared_input.input_sha256,
    )


def _load_finalizer_window_cursor(
    cursor,
    *,
    cohort_id: UUID,
    window_id: UUID,
) -> FiveMinuteFinalizerWindow:
    cursor.execute(
        "SELECT cohort_id, provider_tier, cutoff, deadline "
        "FROM rt.window_runs WHERE window_id=%s",
        (str(window_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("finalizer window does not exist")
    values = (
        (
            row["cohort_id"],
            row["provider_tier"],
            row["cutoff"],
            row["deadline"],
        )
        if isinstance(row, Mapping)
        else row
    )
    stored_cohort, provider_tier, window_end, deadline = values
    if str(stored_cohort) != str(cohort_id):
        raise ValueError("finalizer window cohort does not match")
    end = _utc(window_end, "window_end")
    return FiveMinuteFinalizerWindow(
        cohort_id=cohort_id,
        provider_tier=ProviderTier(provider_tier),
        window_id=window_id,
        window_start=end - _TARGET_DURATION,
        window_end=end,
        deadline=deadline,
    )


def _load_finalizer_security_cursor(
    cursor,
    *,
    instrument_id: int,
    window_end: datetime,
    as_of: datetime,
) -> SecurityReferenceRevision:
    cursor.execute(
        "SELECT reference.revision_id, reference.security_id, reference.ticker, "
        "reference.active, reference.company_name, reference.security_type, "
        "reference.cik, reference.composite_figi, reference.share_class_figi, "
        "reference.exchange, reference.sic_code, reference.sic_description, "
        "reference.sector, reference.industry, reference.list_date, "
        "reference.delisted_date, reference.weighted_shares, reference.free_float, "
        "reference.free_float_percent, reference.market_cap, "
        "reference.effective_from, reference.observed_at, reference.payload_sha256 "
        "FROM rt.instruments AS instrument "
        "JOIN ref.security_revisions AS reference USING (security_id) "
        "WHERE instrument.instrument_id=%s AND instrument.asset_class='EQUITY' "
        "AND reference.effective_from<=%s "
        "AND (reference.effective_to IS NULL OR reference.effective_to>%s) "
        "AND reference.observed_at<=%s "
        "ORDER BY reference.effective_from DESC, reference.observed_at DESC, "
        "reference.revision_id DESC LIMIT 1",
        (instrument_id, window_end, window_end, as_of),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("finalizer security reference is unavailable")
    if isinstance(row, Mapping):
        values = tuple(row.values())
    else:
        values = tuple(row)
    return SecurityReferenceRevision(
        security_revision_id=UUID(str(values[0])),
        security_id=UUID(str(values[1])),
        ticker=str(values[2]),
        active=bool(values[3]),
        company_name=values[4],
        security_type=values[5],
        cik=values[6],
        composite_figi=values[7],
        share_class_figi=values[8],
        primary_exchange=values[9],
        sic_code=values[10],
        sic_description=values[11],
        sector=values[12],
        industry=values[13],
        list_date=values[14],
        delisted_date=values[15],
        weighted_shares=values[16],
        free_float=values[17],
        free_float_percent=values[18],
        market_cap=values[19],
        source="REALTIME_REFERENCE_V2",
        effective_from=values[20],
        observed_at=values[21],
        payload_sha256=str(values[22]),
        raw_payload_json="{}",
    )


_BAR_COLUMNS = (
    "bar_revision_id",
    "security_id",
    "ticker",
    "interval",
    "session_date",
    "session_scope",
    "adjustment_basis",
    "bar_start",
    "bar_end",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "vwap",
    "transaction_count",
    "source_kind",
    "availability_mode",
    "is_final",
    "observed_at",
    "replay_available_at",
    "provider_published_at",
    "ingestion_segment_id",
    "source_revision_ids",
    "supersedes_revision_id",
    "payload_sha256",
    "quality_codes",
)


def _bar_revision(row) -> EquityBarRevision:
    values = row if isinstance(row, Mapping) else dict(zip(_BAR_COLUMNS, row))
    return EquityBarRevision(
        bar_revision_id=UUID(str(values["bar_revision_id"])),
        security_id=UUID(str(values["security_id"])),
        ticker=str(values["ticker"]),
        interval=str(values["interval"]),
        session_date=values["session_date"],
        bar_start=values["bar_start"],
        bar_end=values["bar_end"],
        open_price=values["open_price"],
        high_price=values["high_price"],
        low_price=values["low_price"],
        close_price=values["close_price"],
        volume=values["volume"],
        vwap=values["vwap"],
        transaction_count=values["transaction_count"],
        source_kind=BarSourceKind(values["source_kind"]),
        availability_mode=BarAvailabilityMode(values["availability_mode"]),
        is_final=bool(values["is_final"]),
        system_observed_at=values["observed_at"],
        replay_available_at=values["replay_available_at"],
        adjusted=values["adjustment_basis"] == "ADJUSTED",
        payload_sha256=str(values["payload_sha256"]),
        quality_codes=tuple(values["quality_codes"] or ()),
        provider_published_at=values["provider_published_at"],
        ingestion_segment_id=(
            UUID(str(values["ingestion_segment_id"]))
            if values["ingestion_segment_id"] is not None
            else None
        ),
        source_bar_revision_ids=tuple(
            UUID(str(value)) for value in values["source_revision_ids"] or ()
        ),
        supersedes_bar_revision_id=(
            UUID(str(values["supersedes_revision_id"]))
            if values["supersedes_revision_id"] is not None
            else None
        ),
        session_scope=BarSessionScope(values["session_scope"]),
    )


def _load_finalizer_source_bars_cursor(
    cursor,
    *,
    window: FiveMinuteFinalizerWindow,
    instrument_id: int,
    security: SecurityReferenceRevision,
    as_of: datetime,
) -> tuple[EquityBarRevision, ...]:
    cursor.execute(
        "SELECT DISTINCT ON (revision.bar_start) "
        "revision.bar_revision_id, revision.security_id, revision.ticker, "
        "revision.interval, revision.session_date, revision.session_scope, "
        "revision.adjustment_basis, revision.bar_start, revision.bar_end, "
        "revision.open_price, revision.high_price, revision.low_price, "
        "revision.close_price, revision.volume, revision.vwap, "
        "revision.transaction_count, revision.source_kind, "
        "revision.availability_mode, revision.is_final, revision.observed_at, "
        "revision.replay_available_at, revision.provider_published_at, "
        "revision.ingestion_segment_id, revision.source_revision_ids, "
        "revision.supersedes_revision_id, revision.payload_sha256, "
        "revision.quality_codes "
        "FROM rt.bar_revisions AS revision "
        "WHERE revision.cohort_id=%s AND revision.provider_tier=%s "
        "AND revision.instrument_id=%s AND revision.security_id=%s "
        "AND revision.ticker=%s AND revision.interval='1m' "
        "AND revision.session_scope='RTH' "
        "AND revision.adjustment_basis='RAW_UNADJUSTED' "
        "AND revision.source_kind IN ('NATIVE_REST', 'REALTIME_STREAM', 'RECONCILED') "
        "AND revision.is_final AND revision.bar_start>=%s "
        "AND revision.bar_end<=%s AND revision.observed_at<=%s "
        "ORDER BY revision.bar_start, "
        "CASE revision.source_kind WHEN 'RECONCILED' THEN 3 "
        "WHEN 'NATIVE_REST' THEN 2 ELSE 1 END DESC, "
        "revision.observed_at DESC, revision.bar_revision_id DESC",
        (
            str(window.cohort_id),
            window.provider_tier.value,
            instrument_id,
            str(security.security_id),
            security.ticker,
            window.window_start,
            window.window_end,
            as_of,
        ),
    )
    return tuple(_bar_revision(row) for row in cursor.fetchall())


def _advanced_gap_free_coverage_cursor(
    cursor,
    *,
    window: FiveMinuteFinalizerWindow,
) -> bool:
    cursor.execute(
        "SELECT session.session_id,session.journal_generation,"
        "session.owner_epoch "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.stream_sessions AS session "
        "ON session.manifest_id=cohort.manifest_id "
        "WHERE cohort.cohort_id=%s AND session.feed='STOCKS' "
        "AND session.journal_generation IS NOT NULL "
        "AND session.status IN ('LIVE','STOPPING','STOPPED') "
        "AND session.subscribed_at<=%s "
        "AND (session.ended_at IS NULL OR session.ended_at>=%s) "
        "AND session.event_time_watermark>=%s "
        "ORDER BY session.owner_epoch DESC LIMIT 1",
        (
            str(window.cohort_id),
            window.window_start,
            window.window_end,
            window.window_end,
        ),
    )
    session = cursor.fetchone()
    if session is None:
        return False
    cursor.execute(
        "SELECT count(*) FROM rt.consumer_checkpoints AS checkpoint "
        "WHERE checkpoint.cohort_id=%s "
        "AND checkpoint.journal_generation=%s "
        "AND checkpoint.owner_epoch=%s "
        "AND (convert_from(checkpoint.state_payload,'UTF8')::jsonb"
        "->>'stream_session_id')::uuid=%s "
        "AND (convert_from(checkpoint.state_payload,'UTF8')::jsonb"
        "->>'event_time_watermark')::timestamptz>=%s",
        (
            str(window.cohort_id),
            str(session[1]),
            int(session[2]),
            str(session[0]),
            window.window_end,
        ),
    )
    if int(cursor.fetchone()[0]) <= 0:
        return False
    cursor.execute(
        "SELECT count(*) FROM rt.stream_gaps "
        "WHERE session_id=%s AND journal_generation=%s "
        "AND gap_start<%s AND COALESCE(gap_end,%s)>%s "
        "AND recovery_status NOT IN ('REPAIRED','WAIVED')",
        (
            str(session[0]),
            str(session[1]),
            window.window_end,
            window.window_end,
            window.window_start,
        ),
    )
    return int(cursor.fetchone()[0]) == 0


def _load_finalizer_member_input_cursor(
    cursor,
    *,
    window: FiveMinuteFinalizerWindow,
    instrument_id: int,
    checked_at: datetime,
    policy_sha256: str,
    build_id: str,
) -> FiveMinuteFinalizerInput:
    security = _load_finalizer_security_cursor(
        cursor,
        instrument_id=instrument_id,
        window_end=window.window_end,
        as_of=checked_at,
    )
    source_bars = _load_finalizer_source_bars_cursor(
        cursor,
        window=window,
        instrument_id=instrument_id,
        security=security,
        as_of=checked_at,
    )
    gap_free_coverage = (
        _advanced_gap_free_coverage_cursor(
            cursor,
            window=window,
        )
        if window.provider_tier is ProviderTier.MASSIVE_ADVANCED
        else False
    )
    return build_five_minute_finalizer_input(
        window,
        instrument_id=instrument_id,
        security=security,
        source_bars=source_bars,
        checked_at=checked_at,
        policy_sha256=policy_sha256,
        build_id=build_id,
        gap_free_coverage=gap_free_coverage,
    )


def enqueue_five_minute_finalizer_jobs_cursor(
    cursor,
    *,
    cohort_id: UUID,
    window_id: UUID,
    instrument_ids: Sequence[int],
    checked_at: datetime,
    policy_sha256: str,
    build_id: str,
    barrier_id: UUID | None,
    barrier_kind: str = FINALIZER_BARRIER_KIND,
) -> tuple[EnqueuedFiveMinuteFinalizerJob, ...]:
    instruments = tuple(instrument_ids)
    if (
        not 1 <= len(instruments) <= MAX_FINALIZER_ENQUEUE_MEMBERS
        or len(instruments) != len(set(instruments))
        or any(type(value) is not int or value <= 0 for value in instruments)
    ):
        raise ValueError("finalizer instruments must be 1-500 unique positive IDs")
    checked = _utc(checked_at, "checked_at")
    policy = _sha256(policy_sha256, "policy_sha256")
    build = build_id.strip()
    window = _load_finalizer_window_cursor(
        cursor, cohort_id=cohort_id, window_id=window_id
    )
    inputs = tuple(
        _load_finalizer_member_input_cursor(
            cursor,
            window=window,
            instrument_id=instrument_id,
            checked_at=checked,
            policy_sha256=policy,
            build_id=build,
        )
        for instrument_id in instruments
    )
    jobs = tuple(
        build_five_minute_finalizer_job(
            prepared, policy_sha256=policy, build_id=build
        )
        for prepared in inputs
    )
    for job in jobs:
        enqueue_materialization_job_cursor(cursor, job)
    result = tuple(
        EnqueuedFiveMinuteFinalizerJob(job, prepared)
        for job, prepared in zip(jobs, inputs)
    )
    if barrier_id is not None:
        if not isinstance(barrier_id, UUID):
            raise TypeError("finalizer barrier ID is invalid")
        universe_sha256 = canonical_sha256(
            {
                "contract": "realtime-equity-5m-finalizer-universe-v1",
                "cohort_id": cohort_id,
                "window_id": window_id,
                "instrument_ids": instruments,
            }
        )
        open_job_barrier_cursor(
            cursor,
            barrier_id=barrier_id,
            cohort_id=cohort_id,
            window_id=window_id,
            barrier_kind=barrier_kind,
            universe_sha256=universe_sha256,
            members=tuple(
                BarrierMember(
                    member_key=f"instrument:{row.job.instrument_id}:5m",
                    expected_input_sha256=row.input.input_sha256,
                    job_id=row.job.job_id,
                )
                for row in result
            ),
        )
    return result


def prepare_five_minute_finalizer_output(
    database: RealtimeProcessDatabase,
    claimed: ClaimedMaterializationJob,
) -> PreparedEquityWorkerOutput:
    if not isinstance(claimed, ClaimedMaterializationJob):
        raise TypeError("claimed finalizer job is invalid")
    job = claimed.job
    if (
        job.job_kind != EQUITY_INTERVAL_FINALIZE_JOB
        or job.interval != _FINALIZER_INTERVAL
        or job.primary_window_id is None
        or job.bundle_sha256 is None
        or job.policy_sha256 is None
        or job.build_id is None
    ):
        raise ValueError("claimed job does not satisfy the 1m-to-5m finalizer contract")
    with database.transaction(read_only=True) as cursor:
        window = _load_finalizer_window_cursor(
            cursor,
            cohort_id=job.cohort_id,
            window_id=job.primary_window_id,
        )
        prepared = _load_finalizer_member_input_cursor(
            cursor,
            window=window,
            instrument_id=job.instrument_id,
            checked_at=job.available_at,
            policy_sha256=job.policy_sha256,
            build_id=job.build_id,
        )
        cursor.execute(
            "SELECT source_kind FROM rt.bar_current "
            "WHERE cohort_id=%s AND instrument_id=%s AND interval='5m' "
            "AND bar_start=%s AND bar_end=%s AND session_scope='RTH' "
            "AND adjustment_basis='RAW_UNADJUSTED'",
            (
                str(job.cohort_id),
                job.instrument_id,
                window.window_start,
                window.window_end,
            ),
        )
        current_five = cursor.fetchone()
    if prepared.input_sha256 != job.bundle_sha256:
        raise ValueError("finalizer input no longer matches its durable job closure")

    native_complete = (
        current_five is not None
        and current_five[0] in {
            BarSourceKind.NATIVE_REST.value,
            BarSourceKind.RECONCILED.value,
        }
    )
    facts = ()
    if (
        prepared.outcome
        not in {
            FiveMinuteFinalizerOutcome.GAP,
            FiveMinuteFinalizerOutcome.NO_TRADE,
        }
        and not native_complete
    ):
        if (
            window.provider_tier is ProviderTier.MASSIVE_ADVANCED
            and prepared.gap_free_coverage
        ):
            derived = (
                derive_advanced_sparse_minute_window(
                    prepared.security,
                    prepared.source_bars,
                    window_start=window.window_start,
                    window_end=window.window_end,
                    observed_at=prepared.checked_at,
                    ingestion_segment_id=job.job_id,
                ),
            )
        else:
            derived = derive_minute_bars(
                prepared.security,
                prepared.source_bars,
                target_interval=_FINALIZER_INTERVAL,
                observed_at=prepared.checked_at,
                ingestion_segment_id=job.job_id,
            )
        if (
            len(derived) != 1
            or derived[0].bar_start != window.window_start
            or derived[0].bar_end != window.window_end
        ):
            raise ValueError("finalizer did not derive exactly its complete five-minute window")
        facts = (
            adapt_derived_equity_bar(
                derived[0],
                cohort_id=job.cohort_id,
                provider_tier=window.provider_tier,
                instrument_id=job.instrument_id,
            ),
        )

    terminal_state = (
        WatermarkTerminalState.GAP
        if prepared.outcome is FiveMinuteFinalizerOutcome.GAP
        else WatermarkTerminalState.COMPLETE
    )
    reason_codes = {
        FiveMinuteFinalizerOutcome.COMPLETE: (),
        FiveMinuteFinalizerOutcome.GAP: ("FINALIZER_GAP",),
        FiveMinuteFinalizerOutcome.LATE: ("FINALIZER_LATE",),
        FiveMinuteFinalizerOutcome.NO_TRADE: (
            "FINALIZER_NO_TRADE",
        ),
    }[prepared.outcome]
    watermark = AcquisitionWatermark(
        cohort_id=job.cohort_id,
        instrument_id=job.instrument_id,
        interval=_FINALIZER_INTERVAL,
        source_kind=(
            str(current_five[0])
            if native_complete
            else BarSourceKind.DERIVED.value
        ),
        last_complete_bar_end=(
            None
            if terminal_state is WatermarkTerminalState.GAP
            else window.window_end
        ),
        last_examined_bar_end=window.window_end,
        overlap_policy=FINALIZER_OVERLAP_POLICY,
        terminal_state=terminal_state,
        reason_codes=reason_codes,
    )

    def persist_output(cursor) -> None:
        if facts:
            persist_realtime_equity_bars_cursor(cursor, facts)
        advance_acquisition_watermark_cursor(cursor, watermark)

    return PreparedEquityWorkerOutput(
        input_sha256=prepared.input_sha256,
        output_count=len(facts),
        persist_output=persist_output,
        barrier_outcome=(
            BarrierOutcome.MISSING
            if prepared.outcome is FiveMinuteFinalizerOutcome.GAP
            else BarrierOutcome.COMPLETE
        ),
    )


def run_five_minute_finalizer_once(
    database: RealtimeProcessDatabase,
    config: EquityWorkerConfig,
    *,
    checked_at: datetime,
) -> EquityWorkerStatus:
    if config.job_kind != EQUITY_INTERVAL_FINALIZE_JOB:
        raise ValueError("five-minute finalizer requires EQUITY_INTERVAL_FINALIZE jobs")
    return run_equity_worker_once(
        database,
        config,
        prepare=prepare_five_minute_finalizer_output,
        checked_at=checked_at,
    )
