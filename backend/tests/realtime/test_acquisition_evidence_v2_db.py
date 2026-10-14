"""DB-backed acquisition, jobs and evidence checks on a disposable V2 database.

Opt in with STOCKS_V2_DB_TESTS=1 and run WITHOUT the offline I/O guard plugin. The database is
test_realtime_v2_p2; it is created from the sql_v2 baseline and always dropped. Credentials stay inside
tests/realtime/v2_database.py: nothing here receives, stores or prints them.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from psycopg2 import errors

from equity.domain import EquityBarRevision
from realtime.acquisition_handoff import (
    DeveloperEquityBarAcquisitionFact,
    build_developer_acquisition_batch,
    load_developer_acquisition_batch_cursor,
    persist_developer_acquisition_batch_cursor,
)
from realtime.acquisition_ledger import (
    AcquisitionWatermark,
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RequestCompleteness,
    WatermarkTerminalState,
    advance_acquisition_watermark_cursor,
    finish_provider_request_cursor,
    load_acquisition_watermark_cursor,
    load_raw_payload_cursor,
    mark_stale_provider_requests_ambiguous_cursor,
    persist_raw_payload_cursor,
    record_provider_request_cursor,
    require_complete_provider_request_cursor,
    reserve_provider_request_cursor,
)
from realtime.equity_bar_adapter import adapt_developer_equity_bar
from realtime.equity_bar_repository import persist_realtime_equity_bars_cursor
from realtime.equity_evidence_repository import (
    RealtimeEquityMaterializationWrite,
    RealtimeFeatureDefinition,
    persist_realtime_equity_materialization_batch_cursor,
    persist_realtime_equity_materialization_cursor,
    resolve_current_evidence_ref_cursor,
    resolve_evidence_ref_as_of_cursor,
    resolve_release_evidence_refs_cursor,
)
from realtime.equity_materialization_adapter import materialize_realtime_equity_evidence
from realtime.materialization_jobs import (
    BarrierMember,
    BarrierOutcome,
    build_closure_materialization_job,
    claim_materialization_jobs_cursor,
    complete_materialization_job_cursor,
    enqueue_materialization_job_cursor,
    evaluate_job_barrier_cursor,
    fail_materialization_job_cursor,
    open_job_barrier_cursor,
    record_job_barrier_outcome_cursor,
)
from tests.realtime.test_equity_bar_adapter import _developer_revision
from tests.realtime.test_equity_materialization_adapter import _security
from v2_database import disposable_v2_database, insert_cohort, insert_service_plan, requires_v2_database


pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_p2"
UTC = timezone.utc


class _Database:
    """Connection access that never exposes the pool or its settings through repr."""

    def __init__(self, pool) -> None:
        self._pool = pool

    def __repr__(self) -> str:
        return "<hidden>"

    @contextmanager
    def transaction(self):
        connection = self._pool.getconn()
        try:
            with connection.cursor() as cursor:
                yield cursor
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            self._pool.putconn(connection)


@pytest.fixture(scope="module")
def database():
    with disposable_v2_database(DATABASE) as pool:
        yield _Database(pool)


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _seed(cursor):
    """A cohort, a security and an equity instrument; returns (cohort_id, security_id, instrument_id)."""
    cohort = insert_cohort(cursor, _now())
    security = uuid4()
    cursor.execute(
        "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
        "VALUES (%s, 'STOCK', 'fixture', %s)", (str(security), _now()),
    )
    cursor.execute(
        "INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
        "RETURNING instrument_id", (str(security),),
    )
    return cohort, security, cursor.fetchone()[0]


def _revision(security_id: UUID, end: datetime, *, segment: UUID, observed_at: datetime | None = None,
              payload: str | None = None) -> EquityBarRevision:
    return replace(
        _developer_revision(),
        bar_revision_id=uuid4(),
        security_id=security_id,
        session_date=end.date(),
        bar_start=end - timedelta(minutes=1),
        bar_end=end,
        system_observed_at=observed_at or end + timedelta(seconds=5),
        payload_sha256=payload or uuid4().hex + uuid4().hex,
        ingestion_segment_id=segment,
    )


def _batch(cohort, security, instrument, *, count=3, end=None, observed_at=None, segment=None):
    end = end or (_now() - timedelta(minutes=30)).replace(second=0)
    segment = segment or uuid4()
    facts = tuple(
        DeveloperEquityBarAcquisitionFact(
            instrument,
            _revision(security, end + timedelta(minutes=index), segment=segment,
                      observed_at=observed_at and observed_at + timedelta(minutes=index)),
        )
        for index in range(count)
    )
    return build_developer_acquisition_batch(
        cohort_id=cohort, provider="massive_developer", provider_batch_id=segment,
        started_at=end, completed_at=max(fact.observed_at for fact in facts), facts=facts,
    )


def _scalar(cursor, statement, parameters=()):
    cursor.execute(statement, parameters)
    return cursor.fetchone()[0]


def test_acquisition_batch_and_facts_are_written_once_into_a_monthly_partition(database):
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        batch = _batch(cohort, security, instrument)
        persist_developer_acquisition_batch_cursor(cursor, batch)
    with database.transaction() as cursor:
        persist_developer_acquisition_batch_cursor(cursor, batch)
        assert _scalar(cursor, "SELECT count(*) FROM rt.acquisition_batches WHERE cohort_id=%s",
                       (str(cohort),)) == 1
        assert _scalar(cursor, "SELECT count(*) FROM rt.acquisition_facts WHERE acquisition_batch_id=%s",
                       (str(batch.acquisition_batch_id),)) == 3
        cursor.execute("SELECT DISTINCT tableoid::regclass::text FROM rt.acquisition_facts "
                       "WHERE acquisition_batch_id=%s", (str(batch.acquisition_batch_id),))
        partitions = [row[0] for row in cursor.fetchall()]
        assert len(partitions) == 1 and partitions[0].startswith("rt.acquisition_facts_p")
        assert load_developer_acquisition_batch_cursor(cursor, batch.acquisition_batch_id) == batch


def test_a_different_payload_for_the_same_provider_batch_is_a_conflict_and_writes_nothing(database):
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        first = _batch(cohort, security, instrument, count=2)
        persist_developer_acquisition_batch_cursor(cursor, first)
    conflicting = _batch(cohort, security, instrument, count=3, segment=first.provider_batch_id)
    assert conflicting.acquisition_batch_id != first.acquisition_batch_id
    with pytest.raises(ValueError, match="batch identity conflict"):
        with database.transaction() as cursor:
            persist_developer_acquisition_batch_cursor(cursor, conflicting)
    with database.transaction() as cursor:
        assert _scalar(cursor, "SELECT count(*) FROM rt.acquisition_batches WHERE cohort_id=%s",
                       (str(cohort),)) == 1
        assert _scalar(cursor, "SELECT count(*) FROM rt.acquisition_facts WHERE acquisition_batch_id=%s",
                       (str(conflicting.acquisition_batch_id),)) == 0


def test_writes_outside_precreated_partitions_are_rejected_and_roll_back(database):
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
    old = datetime(2020, 1, 15, 12, 0, tzinfo=UTC)
    stale = _batch(cohort, security, instrument, count=1, end=old, observed_at=old + timedelta(seconds=5))
    with pytest.raises(errors.CheckViolation):
        with database.transaction() as cursor:
            persist_developer_acquisition_batch_cursor(cursor, stale)
    with database.transaction() as cursor:
        assert _scalar(cursor, "SELECT count(*) FROM rt.acquisition_batches WHERE cohort_id=%s",
                       (str(cohort),)) == 0

    revision = _revision(security, old, segment=uuid4())
    fact = adapt_developer_equity_bar(revision, cohort_id=cohort, instrument_id=instrument)
    with pytest.raises(errors.CheckViolation):
        with database.transaction() as cursor:
            persist_realtime_equity_bars_cursor(cursor, (fact,))
    with database.transaction() as cursor:
        assert _scalar(cursor, "SELECT count(*) FROM rt.bar_catalog WHERE cohort_id=%s", (str(cohort),)) == 0

    with pytest.raises(errors.CheckViolation):
        with database.transaction() as cursor:
            persist_raw_payload_cursor(cursor, b"stale", arrived_at=_now() - timedelta(days=90))

    with database.transaction() as cursor:
        catalog = uuid4()
        cursor.execute(
            "INSERT INTO rt.evidence_catalog (cohort_id, evidence_id, evidence_key, provider_tier, "
            "evidence_type, security_id, instrument_id, market_time) "
            "VALUES (%s, %s, %s, 'MASSIVE_DEVELOPER', 'FEATURE_SNAPSHOT', %s, %s, %s)",
            (str(cohort), str(catalog), f"key:{catalog}", str(security), instrument, _now()),
        )
        cursor.execute("SAVEPOINT body")
        with pytest.raises(errors.CheckViolation):
            cursor.execute(
                "INSERT INTO rt.equity_evidence_dependencies (cohort_id, evidence_id, dependency_order, "
                "dependency_evidence_id, durable_at) VALUES (%s, %s, 0, %s, %s)",
                (str(cohort), str(catalog), str(uuid4()), old),
            )
        cursor.execute("ROLLBACK TO SAVEPOINT body")


def test_bars_use_a_catalog_then_a_monthly_body_and_replay_is_idempotent(database):
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        end = (_now() - timedelta(minutes=30)).replace(second=0)
        facts = tuple(
            adapt_developer_equity_bar(
                _revision(security, end + timedelta(minutes=index), segment=uuid4()),
                cohort_id=cohort, instrument_id=instrument,
            )
            for index in range(3)
        )
        persist_realtime_equity_bars_cursor(cursor, facts)
    with database.transaction() as cursor:
        persist_realtime_equity_bars_cursor(cursor, facts)
        assert _scalar(cursor, "SELECT count(*) FROM rt.bar_catalog WHERE cohort_id=%s", (str(cohort),)) == 3
        assert _scalar(cursor, "SELECT count(*) FROM rt.bar_revisions WHERE cohort_id=%s", (str(cohort),)) == 3
        assert _scalar(cursor, "SELECT count(*) FROM rt.bar_current WHERE cohort_id=%s", (str(cohort),)) == 3
        cursor.execute("SELECT DISTINCT tableoid::regclass::text FROM rt.bar_revisions WHERE cohort_id=%s",
                       (str(cohort),))
        assert all(row[0].startswith("rt.bar_revisions_p") for row in cursor.fetchall())
    conflicting = replace(facts[0], revision=replace(facts[0].revision, payload_sha256="f" * 64))
    with pytest.raises(ValueError, match="identity conflict"):
        with database.transaction() as cursor:
            persist_realtime_equity_bars_cursor(cursor, (conflicting,))


def _reservation(cohort, *, lane=ProviderRequestLane.OPTIONS_ALIGNMENT, reserved_at=None):
    return ProviderRequestReservation(
        request_id=uuid4(), account_alias="developer", lane=lane, endpoint_class="aggs_minute",
        request_sha256=uuid4().hex + uuid4().hex, reserved_at=reserved_at or _now(), cohort_id=cohort,
    )


def _completion(*, complete=True, **changes):
    values = dict(
        state=ProviderRequestState.COMPLETE, started_at=_now(), completed_at=_now() + timedelta(seconds=1),
        outcome=ProviderRequestOutcome.OK, http_status=200, response_bytes=2_048, row_count=390,
        pages_fetched=2, requested_from=_now() - timedelta(hours=7), requested_to=_now(),
        price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
        completeness=RequestCompleteness.COMPLETE if complete else RequestCompleteness.INCOMPLETE,
        failure_code=None if complete else "PAGINATION_NOT_EXHAUSTED",
    )
    values.update(changes)
    return ProviderRequestCompletion(**values)


def test_ledger_records_completeness_and_basis_and_rejects_partial_reads(database):
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        complete = _reservation(cohort)
        partial = _reservation(cohort)
        done = _completion()
        record_provider_request_cursor(cursor, complete, done)
        record_provider_request_cursor(cursor, partial, _completion(complete=False))
        record_provider_request_cursor(cursor, complete, done)
        cursor.execute(
            "SELECT price_basis, completeness, failure_code, pages_fetched FROM rt.provider_request_ledger "
            "WHERE request_id=%s", (str(partial.request_id),),
        )
        assert cursor.fetchone() == ("RAW_FINALIZED_MINUTE_CLOSE_V1", "INCOMPLETE",
                                     "PAGINATION_NOT_EXHAUSTED", 2)
        require_complete_provider_request_cursor(cursor, complete.request_id)
        with pytest.raises(ValueError, match="incomplete"):
            require_complete_provider_request_cursor(cursor, partial.request_id)
        with pytest.raises(ValueError, match="different result"):
            finish_provider_request_cursor(cursor, complete.request_id, _completion(row_count=1))
        with pytest.raises(ValueError, match="identity conflict"):
            reserve_provider_request_cursor(
                cursor, replace(complete, lane=ProviderRequestLane.OPTIONS_CHAIN),
            )

    with database.transaction() as cursor:
        stale = _reservation(cohort, reserved_at=_now() - timedelta(minutes=10))
        reserve_provider_request_cursor(cursor, stale)
        assert mark_stale_provider_requests_ambiguous_cursor(
            cursor, reserved_before=_now() - timedelta(minutes=5),
        ) >= 1
        with pytest.raises(ValueError, match="did not complete successfully"):
            require_complete_provider_request_cursor(cursor, stale.request_id)
        finish_provider_request_cursor(cursor, stale.request_id, _completion())
        require_complete_provider_request_cursor(cursor, stale.request_id)


def test_the_database_itself_rejects_an_incomplete_request_without_a_failure_code(database):
    with pytest.raises(errors.CheckViolation):
        with database.transaction() as cursor:
            cursor.execute(
                "INSERT INTO rt.provider_request_ledger (request_id, account_alias, lane, endpoint_class, "
                "request_sha256, reserved_at, completeness) "
                "VALUES (%s, 'developer', 'OPTIONS_ALIGNMENT', 'aggs_minute', %s, %s, 'INCOMPLETE')",
                (str(uuid4()), "a" * 64, _now()),
            )


def test_a_batch_links_to_its_ledger_request_and_the_watermark_only_advances(database):
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        reservation = _reservation(cohort, lane=ProviderRequestLane.EQUITY_HISTORY)
        record_provider_request_cursor(cursor, reservation, _completion(
            price_basis=PriceBasis.ADJUSTED_BAR_START_V1,
        ))
        plain = _batch(cohort, security, instrument)
        linked = build_developer_acquisition_batch(
            cohort_id=cohort, provider=plain.provider, provider_batch_id=plain.provider_batch_id,
            started_at=plain.started_at, completed_at=plain.completed_at, facts=plain.facts,
            request_id=reservation.request_id,
        )
        persist_developer_acquisition_batch_cursor(cursor, linked)
        assert load_developer_acquisition_batch_cursor(cursor, linked.acquisition_batch_id).request_id == \
            reservation.request_id

        end = max(fact.revision.bar_end for fact in linked.facts)

        def mark(bar_end, **changes):
            return AcquisitionWatermark(
                cohort_id=cohort, instrument_id=instrument, interval="1m", source_kind="NATIVE_REST",
                last_complete_bar_end=bar_end, overlap_policy="developer_rest_overlap_v1",
                request_id=reservation.request_id, **changes,
            )

        assert advance_acquisition_watermark_cursor(cursor, mark(end)) is True
        assert advance_acquisition_watermark_cursor(cursor, mark(end - timedelta(minutes=1))) is False
        assert advance_acquisition_watermark_cursor(cursor, mark(end)) is False
        loaded = load_acquisition_watermark_cursor(
            cursor, cohort_id=cohort, instrument_id=instrument, interval="1m", source_kind="NATIVE_REST",
        )
        assert loaded.last_complete_bar_end == end and loaded.request_id == reservation.request_id
        later = end + timedelta(minutes=1)
        gap = mark(later, terminal_state=WatermarkTerminalState.GAP, reason_codes=("MISSING_MINUTE",))
        assert advance_acquisition_watermark_cursor(cursor, gap) is True
        assert advance_acquisition_watermark_cursor(cursor, mark(later)) is True
        assert advance_acquisition_watermark_cursor(cursor, gap) is False
        assert load_acquisition_watermark_cursor(
            cursor, cohort_id=cohort, instrument_id=instrument, interval="1m", source_kind="NATIVE_REST",
        ).terminal_state is WatermarkTerminalState.COMPLETE


def test_raw_payloads_are_chunked_and_read_back_exactly(database):
    content = (b"0123456789abcdef" * 200_000)[:2_500_000]
    arrived = _now()
    with database.transaction() as cursor:
        payload_id = persist_raw_payload_cursor(cursor, content, arrived_at=arrived)
        assert persist_raw_payload_cursor(cursor, content, arrived_at=arrived) == payload_id
        assert _scalar(cursor, "SELECT count(*) FROM rt.raw_payload_chunks WHERE payload_id=%s",
                       (str(payload_id),)) == 3
    with database.transaction() as cursor:
        assert load_raw_payload_cursor(cursor, payload_id) == content


def _window_run(cursor, cohort) -> UUID:
    plan = insert_service_plan(cursor, cohort, ("REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE"), _now())
    window = uuid4()
    now = _now()
    cursor.execute(
        "INSERT INTO rt.window_runs (window_id, plan_id, cohort_id, provider_tier, window_key, release_at, "
        "cutoff, deadline) VALUES (%s, %s, %s, 'MASSIVE_DEVELOPER', %s, %s, %s, %s)",
        (str(window), str(plan), str(cohort), f"window:{window}", now, now - timedelta(minutes=1),
         now + timedelta(minutes=5)),
    )
    return window


def test_jobs_claim_complete_and_barrier_members_compare_exact_input_hashes(database):
    kind = "EQUITY_EVIDENCE"
    owner, other_owner = uuid4(), uuid4()
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        _, _, second_instrument = _seed(cursor)
        window = _window_run(cursor, cohort)
        confirmation = uuid4()
        jobs = tuple(
            build_closure_materialization_job(
                job_kind=kind, cohort_id=cohort, instrument_id=instrument_id, interval="30m",
                primary_window_id=uuid4(), confirmation_window_ids=(confirmation,),
                policy_sha256="c" * 64, build_id="build-1", available_at=_now() - timedelta(minutes=1),
            )
            for instrument_id in (instrument, second_instrument)
        )
        for job in jobs:
            assert enqueue_materialization_job_cursor(cursor, job) == job.job_id
            assert enqueue_materialization_job_cursor(cursor, job) == job.job_id
        assert _scalar(cursor, "SELECT count(*) FROM rt.materialization_jobs WHERE cohort_id=%s",
                       (str(cohort),)) == 2
        expected = {job.instrument_id: ("1" if job.instrument_id == instrument else "2") * 64 for job in jobs}
        barrier = uuid4()
        members = tuple(BarrierMember(f"instrument:{job.instrument_id}", expected[job.instrument_id], job.job_id)
                        for job in jobs)
        open_job_barrier_cursor(
            cursor, barrier_id=barrier, cohort_id=cohort, window_id=window, barrier_kind="EQUITY_UNIVERSE",
            universe_sha256="e" * 64, members=members,
        )
        open_job_barrier_cursor(
            cursor, barrier_id=barrier, cohort_id=cohort, window_id=window, barrier_kind="EQUITY_UNIVERSE",
            universe_sha256="e" * 64, members=members,
        )
        claimed = claim_materialization_jobs_cursor(
            cursor, job_kind=kind, lease_owner=owner, lease_seconds=60, maximum_attempts=3, limit=10,
            cohort_id=cohort,
        )
        assert {item.job for item in claimed} == set(jobs)
        assert all(item.lease_epoch == 1 and item.attempts == 1 for item in claimed)
        assert claim_materialization_jobs_cursor(
            cursor, job_kind=kind, lease_owner=other_owner, lease_seconds=60, maximum_attempts=3, limit=10,
            cohort_id=cohort,
        ) == ()
        by_instrument = {item.job.instrument_id: item for item in claimed}

        first = by_instrument[instrument]
        complete_materialization_job_cursor(
            cursor, first,
            persist_output=lambda active: record_job_barrier_outcome_cursor(
                active, barrier_id=barrier, member_key=f"instrument:{instrument}",
                outcome=BarrierOutcome.COMPLETE, outcome_input_sha256=expected[instrument],
            ),
        )
        verdict = evaluate_job_barrier_cursor(cursor, barrier)
        assert verdict.state == "OPEN" and verdict.open_members == (f"instrument:{second_instrument}",)

        second = by_instrument[second_instrument]
        complete_materialization_job_cursor(
            cursor, second,
            persist_output=lambda active: record_job_barrier_outcome_cursor(
                active, barrier_id=barrier, member_key=f"instrument:{second_instrument}",
                outcome=BarrierOutcome.COMPLETE, outcome_input_sha256="9" * 64,
            ),
        )
        verdict = evaluate_job_barrier_cursor(cursor, barrier)
        assert verdict.state == "FAILED" and verdict.failed_members == (f"instrument:{second_instrument}",)
        assert verdict.complete_members == (f"instrument:{instrument}",)
        assert _scalar(cursor, "SELECT state FROM rt.job_barriers WHERE barrier_id=%s", (str(barrier),)) == "FAILED"
        assert _scalar(cursor, "SELECT count(*) FROM rt.materialization_jobs WHERE cohort_id=%s "
                       "AND status='COMPLETE'", (str(cohort),)) == 2


def test_an_expired_lease_is_reclaimed_with_a_new_fence_and_a_stale_owner_cannot_finish(database):
    kind = "EQUITY_EVIDENCE"
    first_owner, second_owner = uuid4(), uuid4()
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        job = build_closure_materialization_job(
            job_kind=kind, cohort_id=cohort, instrument_id=instrument, interval="1h",
            primary_window_id=uuid4(), policy_sha256="c" * 64, build_id="build-1",
            available_at=_now() - timedelta(minutes=5),
        )
        enqueue_materialization_job_cursor(cursor, job)
        stale = claim_materialization_jobs_cursor(
            cursor, job_kind=kind, lease_owner=first_owner, lease_seconds=60, maximum_attempts=3, limit=1,
            cohort_id=cohort,
        )[0]
        cursor.execute("UPDATE rt.materialization_jobs SET lease_until=clock_timestamp() - interval '1 second' "
                       "WHERE job_id=%s", (str(job.job_id),))
        fresh = claim_materialization_jobs_cursor(
            cursor, job_kind=kind, lease_owner=second_owner, lease_seconds=60, maximum_attempts=3, limit=1,
            cohort_id=cohort,
        )[0]
        assert (fresh.lease_epoch, fresh.attempts) == (2, 2)
        cursor.execute("SAVEPOINT stale_owner")
        with pytest.raises(ValueError, match="lease owner does not match"):
            complete_materialization_job_cursor(cursor, stale, persist_output=lambda active: None)
        cursor.execute("ROLLBACK TO SAVEPOINT stale_owner")
        assert fail_materialization_job_cursor(
            cursor, fresh, failure_code="output_failed", maximum_attempts=3, retry_delay_seconds=0,
        ) == "PENDING"
        assert fail_materialization_job_cursor(
            cursor, claim_materialization_jobs_cursor(
                cursor, job_kind=kind, lease_owner=second_owner, lease_seconds=60, maximum_attempts=3,
                limit=1, cohort_id=cohort,
            )[0],
            failure_code="bad_input", maximum_attempts=3, retryable=False,
        ) == "BLOCKED"


def _evidence_result(cohort, security_id, instrument, *, offset=0):
    last_end = (_now() - timedelta(hours=1)).replace(minute=0, second=0)
    first_start = last_end - timedelta(minutes=30 * 60)
    facts = tuple(
        adapt_developer_equity_bar(
            replace(
                _developer_revision(),
                bar_revision_id=uuid4(), security_id=security_id, interval="30m",
                session_date=(first_start + timedelta(minutes=30 * index)).date(),
                bar_start=first_start + timedelta(minutes=30 * index),
                bar_end=first_start + timedelta(minutes=30 * (index + 1)),
                system_observed_at=first_start + timedelta(minutes=30 * (index + 1)),
                payload_sha256=f"{index + 1:064x}", ingestion_segment_id=uuid4(),
            ),
            cohort_id=cohort, instrument_id=instrument,
        )
        for index in range(60)
    )
    return materialize_realtime_equity_evidence(
        analysis_run_id=uuid4(), security=replace(_security(), security_id=security_id), bars=facts,
        observed_at=facts[-1].revision.bar_end + timedelta(seconds=offset),
    )


def _definition():
    return RealtimeFeatureDefinition(
        bundle_sha256="b" * 64, bundle_version="equity_features_v1", feature_schema_version="1.0",
        dependency_spec={"bars": {"interval": "1m", "final": True}}, policy_sha256="c" * 64,
        code_sha256="d" * 64,
    )


def test_evidence_lands_in_the_monthly_partition_of_its_durable_arrival_and_reads_back(database):
    definition = _definition()
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        result = _evidence_result(cohort, security, instrument)
        snapshot_id = persist_realtime_equity_materialization_cursor(cursor, result, definition, generation=7)
    expected_rows = len(result.materialization.evidence)
    with database.transaction() as cursor:
        assert _scalar(cursor, "SELECT count(*) FROM rt.evidence_catalog WHERE cohort_id=%s",
                       (str(cohort),)) == expected_rows
        cursor.execute(
            "SELECT e.tableoid::regclass::text, to_char(c.durable_at AT TIME ZONE 'UTC', 'YYYYMM') || '01', "
            "e.durable_at = c.durable_at FROM rt.equity_evidence e "
            "JOIN rt.evidence_catalog c ON c.cohort_id=e.cohort_id AND c.evidence_id=e.evidence_id "
            "WHERE e.cohort_id=%s", (str(cohort),),
        )
        rows = cursor.fetchall()
        assert len(rows) == expected_rows
        assert all(table == f"rt.equity_evidence_p{month}" and same for table, month, same in rows)
        cursor.execute("SELECT tableoid::regclass::text FROM rt.feature_snapshots WHERE snapshot_id=%s",
                       (str(snapshot_id),))
        assert cursor.fetchone()[0].startswith("rt.feature_snapshots_p")
        assert _scalar(cursor, "SELECT count(*) FROM rt.evidence_source_window_catalog WHERE cohort_id=%s",
                       (str(cohort),)) == 1
        assert _scalar(cursor, "SELECT count(*) FROM rt.equity_evidence_source_windows WHERE cohort_id=%s",
                       (str(cohort),)) == 1
        dependencies = _scalar(cursor, "SELECT count(*) FROM rt.equity_evidence_dependencies WHERE cohort_id=%s",
                               (str(cohort),))
        assert dependencies > 0
        assert _scalar(
            cursor,
            "SELECT count(*) FROM rt.equity_evidence_dependencies d JOIN rt.evidence_catalog c "
            "ON c.cohort_id=d.cohort_id AND c.evidence_id=d.evidence_id AND c.durable_at=d.durable_at "
            "WHERE d.cohort_id=%s", (str(cohort),),
        ) == dependencies

        feature = next(
            row for row in result.materialization.evidence if row.evidence_type.value == "FEATURE_SNAPSHOT"
        )
        current = resolve_current_evidence_ref_cursor(
            cursor, cohort_id=cohort, security_id=security, interval=result.interval,
            evidence_type=feature.evidence_type, model_variant=feature.source_version,
        )
        assert current is not None and current.evidence_id == feature.evidence_id
        assert current.payload_sha256 == feature.payload_sha256
        as_of = resolve_evidence_ref_as_of_cursor(
            cursor, cohort_id=cohort, security_id=security, interval=result.interval,
            evidence_type=feature.evidence_type, model_variant=feature.source_version,
            market_as_of=feature.market_time, known_by=feature.observed_at,
        )
        assert as_of == current
        released = resolve_release_evidence_refs_cursor(
            cursor, (current,), security_id=security, known_by=_now() + timedelta(minutes=1),
        )
        assert released == {feature.evidence_id: current}
        with pytest.raises(ValueError, match="missing or its immutable identity"):
            resolve_release_evidence_refs_cursor(
                cursor, (current,), security_id=security, known_by=feature.observed_at,
            )


def test_detector_input_soft_references_resolve_against_the_v2_catalogs(database):
    from types import SimpleNamespace

    from realtime.detector_evidence import RealtimeDetectorInput
    from realtime.detector_evidence_repository import _require_input_references

    definition = _definition()
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        result = _evidence_result(cohort, security, instrument)
        snapshot_id = persist_realtime_equity_materialization_cursor(cursor, result, definition, generation=7)
        end = (_now() - timedelta(minutes=30)).replace(second=0)
        bar = adapt_developer_equity_bar(
            _revision(security, end, segment=uuid4()), cohort_id=cohort, instrument_id=instrument,
        )
        persist_realtime_equity_bars_cursor(cursor, (bar,))

        def event(**reference):
            row = RealtimeDetectorInput(
                input_role="INPUT", input_key=str(uuid4()), source_kind="REFERENCE", input_sha256="d" * 64,
                available_at=_now(), received_at=_now(), retained_input={"value": 1}, **reference,
            )
            return SimpleNamespace(inputs=(row,))

        _require_input_references(cursor, (
            event(feature_snapshot_id=snapshot_id), event(bar_revision_id=bar.revision.bar_revision_id),
        ))
        for reference in ("feature_snapshot_id", "bar_revision_id", "option_mark_evidence_id"):
            with pytest.raises(ValueError, match=f"{reference} is not available"):
                _require_input_references(cursor, (event(**{reference: uuid4()}),))


def test_discovery_skips_batches_older_than_the_job_retention_unless_requested_by_id(database):
    from realtime.acquisition_consumer import (
        build_developer_acquisition_job,
        enqueue_pending_developer_acquisition_jobs,
    )
    from tests.realtime.test_acquisition_handoff import _compute_database

    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        recent = _batch(cohort, security, instrument, count=1)
        old = _batch(
            cohort, security, instrument, count=1,
            end=(_now() - timedelta(days=10)).replace(second=0),
        )
        persist_developer_acquisition_batch_cursor(cursor, recent)
        persist_developer_acquisition_batch_cursor(cursor, old)

        def discover(ids=None):
            fake_database, _, fake_cursor = _compute_database()
            enqueue_pending_developer_acquisition_jobs(
                fake_database, cohort_id=cohort, fact_kind=recent.fact_kind, limit=10,
                acquisition_batch_ids=ids,
            )
            statement, parameters = next(
                call.args for call in fake_cursor.execute.call_args_list
                if len(call.args) == 2 and "FROM rt.acquisition_batches" in call.args[0]
            )
            cursor.execute(statement, parameters)
            return [UUID(str(row[0])) for row in cursor.fetchall()]

        assert discover() == [recent.acquisition_batch_id]
        assert discover((old.acquisition_batch_id,)) == [old.acquisition_batch_id]
        enqueue_materialization_job_cursor(cursor, build_developer_acquisition_job(recent))
        assert discover() == []


def test_evidence_replay_adds_no_second_body_and_a_new_closure_gets_its_own_snapshot(database):
    definition = _definition()
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
        result = _evidence_result(cohort, security, instrument)
        persist_realtime_equity_materialization_cursor(cursor, result, definition, generation=7)
        before = _scalar(cursor, "SELECT count(*) FROM rt.equity_evidence WHERE cohort_id=%s", (str(cohort),))
        persist_realtime_equity_materialization_cursor(cursor, result, definition, generation=7)
        assert _scalar(cursor, "SELECT count(*) FROM rt.equity_evidence WHERE cohort_id=%s",
                       (str(cohort),)) == before
        receipt = persist_realtime_equity_materialization_batch_cursor(
            cursor, (RealtimeEquityMaterializationWrite(result, definition, 7),),
        )
        assert receipt.persisted_count == 0 and receipt.skipped_count == 1
        assert _scalar(cursor, "SELECT count(*) FROM rt.feature_snapshots WHERE cohort_id=%s",
                       (str(cohort),)) == 1

        other = _evidence_result(cohort, security, instrument, offset=1)
        receipt = persist_realtime_equity_materialization_batch_cursor(
            cursor, (RealtimeEquityMaterializationWrite(other, definition, 8, update_evidence_current=False),),
        )
        assert receipt.persisted_count == 1
        assert _scalar(cursor, "SELECT count(*) FROM rt.feature_snapshots WHERE cohort_id=%s",
                       (str(cohort),)) == 2
        assert _scalar(cursor, "SELECT generation FROM rt.feature_current WHERE cohort_id=%s",
                       (str(cohort),)) == 8
        assert _scalar(cursor, "SELECT count(DISTINCT durable_at) FROM rt.equity_evidence_source_windows "
                       "WHERE cohort_id=%s", (str(cohort),)) == 2
