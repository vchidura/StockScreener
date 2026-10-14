from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.materialization_jobs import (
    BarrierMember,
    BarrierOutcome,
    ClaimedMaterializationJob,
    MaterializationJob,
    build_closure_materialization_job,
    claim_materialization_jobs,
    complete_materialization_job,
    complete_materialization_job_cursor,
    enqueue_materialization_job,
    evaluate_job_barrier_cursor,
    fail_materialization_job,
    materialization_closure_sha256,
    open_job_barrier_cursor,
    record_job_barrier_outcome_cursor,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
JOB_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
COHORT = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
REVISION = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
OWNER = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
WINDOW = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
CONFIRMATION = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")


def _job():
    return MaterializationJob(
        JOB_ID,
        "a" * 64,
        "BAR_FEATURE",
        1,
        COHORT,
        REVISION,
        "b" * 64,
        NOW,
    )


def _closure_job(**changes):
    values = dict(
        job_kind="equity_evidence",
        cohort_id=COHORT,
        instrument_id=7,
        interval="30m",
        primary_window_id=WINDOW,
        confirmation_window_ids=(CONFIRMATION,),
        policy_sha256="c" * 64,
        build_id="build-1",
        available_at=NOW,
    )
    values.update(changes)
    return build_closure_materialization_job(**values)


def _stored(job):
    return (
        str(job.job_id),
        job.closure_sha256,
        job.job_kind,
        str(job.cohort_id),
        job.instrument_id,
        job.interval,
        str(job.source_revision_id) if job.source_revision_id else None,
        str(job.primary_window_id) if job.primary_window_id else None,
        [str(value) for value in job.confirmation_window_ids],
        job.bundle_sha256,
        job.policy_sha256,
        job.build_id,
    )


def _connection(*rows):
    cursor = MagicMock()
    cursor.fetchone.side_effect = rows
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


def test_enqueue_is_idempotent_only_for_exact_closure_identity():
    job = _job()
    connection, cursor = _connection(_stored(job))

    assert enqueue_materialization_job(connection, job) == job.job_id
    assert connection.commit.call_count == 1
    insert = cursor.execute.call_args_list[0].args[0]
    assert "closure_sha256" in insert and "dedup_sha256" not in insert
    assert "ON CONFLICT DO NOTHING" in insert
    assert "WHERE closure_sha256=%s" in cursor.execute.call_args_list[1].args[0]

    other = _stored(job)
    conflict, _ = _connection((str(UUID(int=0)), *other[1:]))
    with pytest.raises(ValueError, match="closure identity conflict"):
        enqueue_materialization_job(conflict, job)
    assert conflict.rollback.call_count == 1

    missing, _ = _connection(None)
    with pytest.raises(ValueError, match="closure identity conflict"):
        enqueue_materialization_job(missing, job)


def test_closure_job_binds_every_v2_closure_dimension():
    base = _closure_job()
    assert base == _closure_job()
    assert base.source_revision_id is None and base.primary_window_id == WINDOW
    assert base.job_id == _closure_job().job_id
    assert base.closure_sha256 == materialization_closure_sha256(
        job_kind="EQUITY_EVIDENCE", cohort_id=COHORT, instrument_id=7, interval="30m",
        primary_window_id=WINDOW, confirmation_window_ids=(CONFIRMATION,),
        policy_sha256="c" * 64, build_id="build-1",
    )
    changed = {
        "policy": _closure_job(policy_sha256="d" * 64),
        "build": _closure_job(build_id="build-2"),
        "confirmation": _closure_job(confirmation_window_ids=()),
        "primary": _closure_job(primary_window_id=CONFIRMATION),
        "interval": _closure_job(interval="1h"),
        "instrument": _closure_job(instrument_id=8),
    }
    assert len({base.closure_sha256, *(job.closure_sha256 for job in changed.values())}) == 7
    assert enqueue_values_roundtrip(base)


def enqueue_values_roundtrip(job):
    connection, cursor = _connection(_stored(job))
    enqueue_materialization_job(connection, job)
    parameters = cursor.execute.call_args_list[0].args[1]
    return (
        parameters[5] == "30m"
        and parameters[7] == str(WINDOW)
        and parameters[8] == [str(CONFIRMATION)]
        and parameters[10] == "c" * 64
        and parameters[11] == "build-1"
    )


def test_job_requires_a_source_revision_or_primary_window_like_the_check_constraint():
    with pytest.raises(ValueError, match="source revision or a primary window"):
        MaterializationJob(JOB_ID, "a" * 64, "X", 1, COHORT, None, None, NOW)
    with pytest.raises(ValueError, match="require a primary window"):
        MaterializationJob(
            JOB_ID, "a" * 64, "X", 1, COHORT, REVISION, None, NOW,
            confirmation_window_ids=(CONFIRMATION,),
        )
    with pytest.raises(ValueError, match="interval is unsupported"):
        MaterializationJob(JOB_ID, "a" * 64, "X", 1, COHORT, REVISION, None, NOW, interval="2h")


def test_claim_is_bounded_skip_locked_and_increments_fence():
    job = _job()
    lease_until = NOW + timedelta(minutes=1)
    closure = _closure_job()
    row = (
        job.job_id,
        job.closure_sha256,
        job.job_kind,
        job.instrument_id,
        job.cohort_id,
        job.source_revision_id,
        job.bundle_sha256,
        job.available_at,
        None,
        None,
        [],
        None,
        None,
        OWNER,
        2,
        lease_until,
        1,
    )
    closure_row = (
        closure.job_id, closure.closure_sha256, closure.job_kind, closure.instrument_id,
        closure.cohort_id, None, None, closure.available_at, closure.interval,
        closure.primary_window_id, list(closure.confirmation_window_ids),
        closure.policy_sha256, closure.build_id, OWNER, 1, lease_until, 1,
    )
    connection, cursor = _connection()
    cursor.fetchall.return_value = (row, closure_row)

    claimed = claim_materialization_jobs(
        connection,
        job_kind=job.job_kind,
        lease_owner=OWNER,
        lease_seconds=60,
        maximum_attempts=3,
        limit=10,
        cohort_id=COHORT,
        source_revision_ids=(REVISION,),
    )

    sql = cursor.execute.call_args.args[0]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "cohort_id=%s" in sql
    assert "source_revision_id=ANY" in sql
    assert "job.closure_sha256" in sql and "dedup_sha256" not in sql
    assert str(COHORT) in cursor.execute.call_args.args[1]
    assert [str(REVISION)] in cursor.execute.call_args.args[1]
    assert "LIMIT %s" in sql
    assert "attempts >= %s" in cursor.execute.call_args_list[0].args[0]
    assert claimed[0].lease_epoch == 2
    assert claimed[0].attempts == 1
    assert claimed[0].job == job
    assert claimed[1].job == closure
    assert connection.commit.call_count == 1


def test_output_and_job_completion_commit_atomically_under_lease_fence():
    job = _job()
    claimed = ClaimedMaterializationJob(
        job,
        OWNER,
        2,
        NOW + timedelta(minutes=1),
        1,
    )
    connection, cursor = _connection((OWNER, 2, True))
    cursor.rowcount = 1
    output_calls = []

    def persist_output(active_cursor):
        output_calls.append(active_cursor)
        active_cursor.execute("INSERT INTO rt.feature_current VALUES (...)" )

    complete_materialization_job(
        connection,
        claimed,
        persist_output=persist_output,
    )

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements.index("INSERT INTO rt.feature_current VALUES (...)") < next(
        index for index, statement in enumerate(statements)
        if statement.startswith("UPDATE rt.materialization_jobs")
    )
    assert output_calls == [cursor]
    assert connection.commit.call_count == 1


def test_cursor_level_completion_composes_output_without_owning_transaction():
    claimed = ClaimedMaterializationJob(
        _job(),
        OWNER,
        2,
        NOW + timedelta(minutes=1),
        1,
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (OWNER, 2, True)
    cursor.rowcount = 1
    output = MagicMock()

    complete_materialization_job_cursor(
        cursor,
        claimed,
        persist_output=output,
    )

    output.assert_called_once_with(cursor)
    assert cursor.execute.call_count == 2


def test_stale_job_lease_blocks_output_and_rolls_back():
    claimed = ClaimedMaterializationJob(
        _job(),
        OWNER,
        2,
        NOW + timedelta(minutes=1),
        1,
    )
    connection, _ = _connection((OWNER, 3, True))
    output = MagicMock()

    with pytest.raises(ValueError, match="stale or expired"):
        complete_materialization_job(connection, claimed, persist_output=output)

    output.assert_not_called()
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


@pytest.mark.parametrize(
    ("attempts", "retryable", "expected_status"),
    ((1, True, "PENDING"), (3, True, "FAILED"), (1, False, "BLOCKED")),
)
def test_failed_job_transitions_under_lease_fence(
    attempts,
    retryable,
    expected_status,
):
    claimed = ClaimedMaterializationJob(
        _job(),
        OWNER,
        2,
        NOW + timedelta(minutes=1),
        attempts,
    )
    connection, cursor = _connection(
        (OWNER, 2, True),
        (expected_status,),
    )

    status = fail_materialization_job(
        connection,
        claimed,
        failure_code="output_failed",
        maximum_attempts=3,
        retry_delay_seconds=30,
        retryable=retryable,
    )

    assert status == expected_status
    update = cursor.execute.call_args_list[-1]
    assert "lease_owner=NULL" in update.args[0]
    assert update.args[1][0] == expected_status
    assert update.args[1][1] == "OUTPUT_FAILED"
    assert connection.commit.call_count == 1


BARRIER = UUID("99999999-9999-9999-9999-999999999999")


def _barrier_members():
    return (
        BarrierMember("AAPL:30m", "1" * 64, JOB_ID),
        BarrierMember("MSFT:30m", "2" * 64, None),
    )


def test_barrier_open_is_exact_about_members_and_idempotent():
    members = _barrier_members()
    cursor = MagicMock()
    cursor.fetchone.return_value = (str(BARRIER), str(COHORT), "f" * 64, 2)
    cursor.fetchall.return_value = (
        ("AAPL:30m", "1" * 64, str(JOB_ID)),
        ("MSFT:30m", "2" * 64, None),
    )
    batches = []

    def executor(active_cursor, statement, rows, *, page_size):
        batches.append((statement, tuple(rows)))

    assert open_job_barrier_cursor(
        cursor, barrier_id=BARRIER, cohort_id=COHORT, window_id=WINDOW,
        barrier_kind="EQUITY_UNIVERSE", universe_sha256="f" * 64, members=members,
        values_executor=executor,
    ) == BARRIER
    assert "rt.job_barriers" in cursor.execute.call_args_list[0].args[0]
    assert "ON CONFLICT DO NOTHING" in batches[0][0]
    assert batches[0][1][0][1:3] == ("AAPL:30m", str(JOB_ID))

    cursor.fetchall.return_value = (("AAPL:30m", "1" * 64, str(JOB_ID)),)
    with pytest.raises(ValueError, match="members conflict"):
        open_job_barrier_cursor(
            cursor, barrier_id=BARRIER, cohort_id=COHORT, window_id=WINDOW,
            barrier_kind="EQUITY_UNIVERSE", universe_sha256="f" * 64, members=members,
            values_executor=executor,
        )
    cursor.fetchone.return_value = (str(BARRIER), str(COHORT), "0" * 64, 2)
    with pytest.raises(ValueError, match="barrier identity conflict"):
        open_job_barrier_cursor(
            cursor, barrier_id=BARRIER, cohort_id=COHORT, window_id=WINDOW,
            barrier_kind="EQUITY_UNIVERSE", universe_sha256="f" * 64, members=members,
            values_executor=executor,
        )
    with pytest.raises(ValueError, match="unique"):
        open_job_barrier_cursor(
            cursor, barrier_id=BARRIER, cohort_id=COHORT, window_id=WINDOW,
            barrier_kind="EQUITY_UNIVERSE", universe_sha256="f" * 64,
            members=(members[0], members[0]), values_executor=executor,
        )


def test_barrier_outcome_is_recorded_once_and_conflicts_are_rejected():
    cursor = MagicMock()
    cursor.rowcount = 1
    record_job_barrier_outcome_cursor(
        cursor, barrier_id=BARRIER, member_key="AAPL:30m",
        outcome=BarrierOutcome.COMPLETE, outcome_input_sha256="1" * 64,
    )
    assert "outcome IS NULL" in cursor.execute.call_args.args[0]
    with pytest.raises(ValueError, match="requires its input hash"):
        record_job_barrier_outcome_cursor(
            cursor, barrier_id=BARRIER, member_key="AAPL:30m", outcome=BarrierOutcome.COMPLETE,
        )
    cursor.rowcount = 0
    cursor.fetchone.return_value = ("COMPLETE", "1" * 64)
    record_job_barrier_outcome_cursor(
        cursor, barrier_id=BARRIER, member_key="AAPL:30m",
        outcome=BarrierOutcome.COMPLETE, outcome_input_sha256="1" * 64,
    )
    with pytest.raises(ValueError, match="outcome conflict"):
        record_job_barrier_outcome_cursor(
            cursor, barrier_id=BARRIER, member_key="AAPL:30m",
            outcome=BarrierOutcome.COMPLETE, outcome_input_sha256="2" * 64,
        )


def _evaluated(rows, *, state="OPEN", count=None):
    cursor = MagicMock()
    cursor.fetchone.return_value = (state, count if count is not None else len(rows))
    cursor.fetchall.return_value = rows
    cursor.rowcount = 1
    return cursor


@pytest.mark.parametrize(
    ("rows", "state"),
    (
        ((("A", "1" * 64, "COMPLETE", "1" * 64, "COMPLETE"),
          ("B", "2" * 64, "COMPLETE", "2" * 64, "COMPLETE")), "COMPLETE"),
        ((("A", "1" * 64, "COMPLETE", "1" * 64, "COMPLETE"),
          ("B", "2" * 64, None, None, "LEASED")), "OPEN"),
        ((("A", "1" * 64, "COMPLETE", "9" * 64, "COMPLETE"),), "FAILED"),
        ((("A", "1" * 64, None, None, "BLOCKED"),
          ("B", "2" * 64, None, None, "LEASED")), "FAILED"),
        ((("A", "1" * 64, None, None, "SUPERSEDED"),), "FAILED"),
    ),
)
def test_barrier_compares_exact_input_hashes_not_counts(rows, state):
    cursor = _evaluated(rows)

    verdict = evaluate_job_barrier_cursor(cursor, BARRIER)

    assert verdict.state == state
    closes = [
        call for call in cursor.execute.call_args_list
        if call.args[0].startswith("UPDATE rt.job_barriers")
    ]
    assert bool(closes) == (state != "OPEN")


def test_barrier_derives_terminal_failure_from_its_job_and_rejects_count_drift():
    cursor = _evaluated((("A", "1" * 64, None, None, "FAILED"),))
    verdict = evaluate_job_barrier_cursor(cursor, BARRIER)
    assert verdict.failed_members == ("A",)
    assert any(
        call.args[0].startswith("UPDATE rt.job_barrier_members") and call.args[1][0] == "FAILED"
        for call in cursor.execute.call_args_list
    )
    with pytest.raises(ValueError, match="expected universe"):
        evaluate_job_barrier_cursor(_evaluated((("A", "1" * 64, None, None, None),), count=2), BARRIER)