from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from realtime.advanced_options_resident_barrier import (
    ResidentCompletion,
    ResidentCompletionBarrierState,
    ResidentCompletionDivergence,
    ResidentCompletionMember,
    ResidentCompletionStatus,
    advanced_options_v3_session_status_cursor,
    evaluate_resident_completion_barrier_cursor,
    load_ready_resident_window_targets_cursor,
    record_resident_completion_cursor,
    resident_consumer_hold_cursor,
    resident_completion_stage_outcome,
)
from realtime.contracts import canonical_sha256


NOW = datetime(2026, 10, 23, 14, 30, tzinfo=timezone.utc)


class BarrierCursor:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.rowcount = 0
        self.statement = ""

    def execute(self, statement, _parameters):
        self.statement = statement
        if statement.startswith("INSERT"):
            self.rowcount = 1

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class HoldCursor:
    def __init__(self, rows=()):
        self.rows = tuple(rows)

    def execute(self, statement, parameters):
        assert (
            "advanced_options_window_cycle_authorizations"
            in statement
        )
        assert len(parameters) == 1

    def fetchall(self):
        return self.rows


class HoldReader:
    def __init__(self):
        self.cutoffs = []

    def offsets_at(self, cutoff):
        self.cutoffs.append(cutoff)
        return 12, 9


class SessionStatusCursor:
    def __init__(self, session_id, cohort_id, generation, windows):
        self.session_id = session_id
        self.cohort_id = cohort_id
        self.generation = generation
        self.windows = windows
        self.rows = []
        self.one = None
        self.statements = []

    def execute(self, statement, parameters):
        self.statements.append(statement)
        if "session_acquisition_authorizations" in statement:
            self.one = (
                str(self.cohort_id),
                NOW.date(),
                str(self.generation),
            )
        elif "FROM rt.advanced_journal_registry" in statement:
            self.one = ("DRAINED", 30, 30)
        elif "FROM rt.consumer_checkpoints" in statement:
            self.rows = [
                ("market-state-materializer-v1", 30),
                ("option_flow_stream_v1", 30),
            ]
        elif "FROM rt.advanced_options_window_cycle_authorizations" in statement:
            self.rows = [
                (
                    str(window_id),
                    f"W{index}",
                    NOW,
                    NOW,
                    "COMPLETE",
                )
                for index, window_id in enumerate(
                    self.windows,
                    start=1,
                )
            ]
        elif "FROM rt.advanced_options_resident_completions" in statement:
            window_id = parameters[0]
            offset = (
                self.windows.index(UUID(str(window_id))) + 1
            ) * 10
            self.rows = [
                (member.value, "COMPLETE", offset)
                for member in ResidentCompletionMember
            ]
        elif "FROM rt.stage_runs" in statement:
            self.rows = [
                ("OPTIONS_INPUT_GATE", "COMPLETE", 1),
                ("OPTIONS_CYCLE", "COMPLETE", 1),
                ("OPTIONS_PROJECTION", "COMPLETE", 1),
                ("OPTIONS_PUBLISH", "COMPLETE", 1),
            ]
        elif "FROM rt.cycle_receipts" in statement:
            self.rows = [
                ("OPTIONS_CYCLE",),
                ("OPTIONS_INPUT_READY",),
                ("OPTIONS_PORTAL_READY",),
            ]
        elif "SELECT count(*) FROM rt.portal_releases" in statement:
            self.one = (1,)
        elif "FROM rt.portal_release_current" in statement:
            self.one = (str(uuid4()), NOW)
        else:
            raise AssertionError(statement)

    def fetchone(self):
        return self.one

    def fetchall(self):
        return list(self.rows)


def _completion(
    member,
    *,
    window_id,
    cohort_id,
    generation,
    status=ResidentCompletionStatus.COMPLETE,
    reasons=(),
    ending_offset=10,
):
    evidence = {
        "status": status.value,
        "kind": member.value,
        "effects": {
            "dispatch": False,
            "delivery": False,
            "broker": False,
            "trading": False,
        },
    }
    if status is ResidentCompletionStatus.FAILED:
        evidence["reason_codes"] = list(reasons)
    elif member is ResidentCompletionMember.INGRESS:
        evidence.update(
            raw_end_offset=ending_offset,
            normalized_end_offset=ending_offset,
            connection_attempts=1,
        )
    elif member is ResidentCompletionMember.MARKET_STATE:
        evidence.update(
            normalized_end_offset=ending_offset,
            state_sha256="a" * 64,
            shard_map_sha256="b" * 64,
            shard_ids=[0],
        )
    else:
        evidence.update(
            raw_end_offset=ending_offset,
            normalized_end_offset=ending_offset,
            checkpoint_end_offset=ending_offset,
            owner_epoch=3,
            evidence_ref=f"flow:{ending_offset}",
            shard_map_sha256="c" * 64,
            shard_ids=[0],
        )
    return ResidentCompletion(
        target_window_id=window_id,
        target_cohort_id=cohort_id,
        member=member,
        journal_generation=generation,
        owner_epoch=3,
        ending_offset=ending_offset,
        evidence_sha256=canonical_sha256(evidence),
        evidence=evidence,
        status=status,
        reason_codes=reasons,
        completed_at=NOW,
    )


def _row(completion):
    return (
        completion.target_window_id,
        completion.target_cohort_id,
        completion.member.value,
        completion.journal_generation,
        completion.owner_epoch,
        completion.ending_offset,
        completion.evidence_sha256,
        dict(completion.evidence),
        completion.status.value,
        list(completion.reason_codes),
        completion.completed_at,
    )


def test_resident_barrier_incomplete_members_wait_without_terminal_block():
    window_id, cohort_id, generation = uuid4(), uuid4(), uuid4()
    ingress = _completion(
        ResidentCompletionMember.INGRESS,
        window_id=window_id,
        cohort_id=cohort_id,
        generation=generation,
    )

    verdict = evaluate_resident_completion_barrier_cursor(
        BarrierCursor([_row(ingress)]),
        target_window_id=window_id,
        target_cohort_id=cohort_id,
        journal_generation=generation,
    )

    assert verdict.state is ResidentCompletionBarrierState.WAITING_INPUT
    assert verdict.reason_codes == (
        "RESIDENT_ROLE_NOT_COMPLETE:MARKET_STATE",
        "RESIDENT_ROLE_NOT_COMPLETE:FLOW",
    )
    assert resident_completion_stage_outcome(verdict).state == "DEFERRED"


def test_resident_barrier_propagates_terminal_failure_immediately():
    window_id, cohort_id, generation = uuid4(), uuid4(), uuid4()
    failed = _completion(
        ResidentCompletionMember.MARKET_STATE,
        window_id=window_id,
        cohort_id=cohort_id,
        generation=generation,
        status=ResidentCompletionStatus.FAILED,
        reasons=("CHECKPOINT_GAP",),
    )

    verdict = evaluate_resident_completion_barrier_cursor(
        BarrierCursor([_row(failed)]),
        target_window_id=window_id,
        target_cohort_id=cohort_id,
        journal_generation=generation,
    )

    assert verdict.state is ResidentCompletionBarrierState.FAILED
    assert verdict.reason_codes == (
        "RESIDENT_MARKET_STATE_FAILED:CHECKPOINT_GAP",
    )
    assert resident_completion_stage_outcome(verdict).state == "FAILED"


def test_resident_completion_replay_and_barrier_identity_are_immutable():
    window_id, cohort_id, generation = uuid4(), uuid4(), uuid4()
    completion = _completion(
        ResidentCompletionMember.FLOW,
        window_id=window_id,
        cohort_id=cohort_id,
        generation=generation,
    )
    cursor = BarrierCursor([_row(completion)])

    assert record_resident_completion_cursor(cursor, completion)

    divergent = _completion(
        ResidentCompletionMember.FLOW,
        window_id=window_id,
        cohort_id=cohort_id,
        generation=uuid4(),
    )
    with pytest.raises(
        ResidentCompletionDivergence,
        match="target identity diverged",
    ):
        evaluate_resident_completion_barrier_cursor(
            BarrierCursor([_row(divergent)]),
            target_window_id=window_id,
            target_cohort_id=cohort_id,
            journal_generation=generation,
        )


def test_resident_barrier_completes_only_with_exact_three_members():
    window_id, cohort_id, generation = uuid4(), uuid4(), uuid4()
    members = tuple(
        _completion(
            member,
            window_id=window_id,
            cohort_id=cohort_id,
            generation=generation,
            ending_offset=10,
        )
        for member in ResidentCompletionMember
    )

    verdict = evaluate_resident_completion_barrier_cursor(
        BarrierCursor([_row(member) for member in members]),
        target_window_id=window_id,
        target_cohort_id=cohort_id,
        journal_generation=generation,
    )

    assert verdict.state is ResidentCompletionBarrierState.COMPLETE
    assert len(verdict.members) == 3
    assert verdict.evidence_sha256 is not None


def test_three_consecutive_window_barriers_keep_monotonic_offsets():
    cohort_id, generation = uuid4(), uuid4()
    windows = (uuid4(), uuid4(), uuid4())
    offsets = (10, 20, 30)
    verdicts = []

    for window_id, ending_offset in zip(
        windows,
        offsets,
        strict=True,
    ):
        members = tuple(
            _completion(
                member,
                window_id=window_id,
                cohort_id=cohort_id,
                generation=generation,
                ending_offset=ending_offset,
            )
            for member in ResidentCompletionMember
        )
        verdicts.append(evaluate_resident_completion_barrier_cursor(
            BarrierCursor([_row(member) for member in members]),
            target_window_id=window_id,
            target_cohort_id=cohort_id,
            journal_generation=generation,
        ))

    assert all(
        verdict.state is ResidentCompletionBarrierState.COMPLETE
        for verdict in verdicts
    )
    assert [
        verdict.members[0].ending_offset for verdict in verdicts
    ] == [10, 20, 30]


def test_session_status_is_derived_and_final_barrier_is_complete():
    session_id, cohort_id, generation = uuid4(), uuid4(), uuid4()
    windows = [uuid4(), uuid4(), uuid4()]
    cursor = SessionStatusCursor(
        session_id,
        cohort_id,
        generation,
        windows,
    )

    status = advanced_options_v3_session_status_cursor(
        cursor,
        session_authorization_id=session_id,
    )

    assert [row["target_window_id"] for row in status["windows"]] == [
        str(window_id) for window_id in windows
    ]
    assert [
        row["residents"][0]["ending_offset"]
        for row in status["windows"]
    ] == [10, 20, 30]
    assert all(
        row["publication_count"] == 1
        for row in status["windows"]
    )
    assert status["final_barrier"]["state"] == "COMPLETE"
    assert all(
        statement.lstrip().startswith("SELECT")
        for statement in cursor.statements
    )


def test_complete_resident_evidence_requires_downstream_offset_keys():
    window_id, cohort_id, generation = uuid4(), uuid4(), uuid4()
    evidence = {
        "status": "COMPLETE",
        "kind": "FLOW",
        "raw_end_offset": 10,
        "normalized_end_offset": 10,
        "owner_epoch": 3,
        "evidence_ref": "flow:10",
        "effects": {
            "dispatch": False,
            "delivery": False,
            "broker": False,
            "trading": False,
        },
    }

    with pytest.raises(
        ValueError,
        match="checkpoint_end_offset",
    ):
        ResidentCompletion(
            target_window_id=window_id,
            target_cohort_id=cohort_id,
            member=ResidentCompletionMember.FLOW,
            journal_generation=generation,
            owner_epoch=3,
            ending_offset=10,
            evidence_sha256=canonical_sha256(evidence),
            evidence=evidence,
            status=ResidentCompletionStatus.COMPLETE,
            reason_codes=(),
            completed_at=NOW,
        )


def test_stopped_stream_releases_later_resident_window():
    window_id, cohort_id, generation = uuid4(), uuid4(), uuid4()
    cursor = BarrierCursor(rows=(
        (str(window_id), str(cohort_id), str(generation), NOW),
    ))

    targets = load_ready_resident_window_targets_cursor(
        cursor,
        session_authorization_id=uuid4(),
        member=ResidentCompletionMember.INGRESS,
        checked_at=NOW,
    )

    assert targets[0].target_window_id == window_id
    assert "stream.event_time_watermark>cycle.options_cutoff" in (
        cursor.statement
    )
    assert "stream.status='STOPPED'" in cursor.statement
    assert "stream.ended_at<=cycle.options_cutoff" in cursor.statement


def test_targets_skip_windows_nothing_will_run_again():
    cursor = BarrierCursor(rows=())

    load_ready_resident_window_targets_cursor(
        cursor,
        session_authorization_id=uuid4(),
        member=ResidentCompletionMember.INGRESS,
        checked_at=NOW,
    )

    assert "window_run.state IN ('PENDING','RUNNING')" in cursor.statement
    assert "window_run.deadline>%s" in cursor.statement


def _cycle(state, *, minute=20, deadline_minutes=5):
    cutoff = NOW.replace(minute=minute)
    return (
        cutoff - timedelta(minutes=5),
        str(uuid4()),
        cutoff,
        cutoff + timedelta(minutes=deadline_minutes),
        state,
    )


def _hold(rows, checked_at):
    return resident_consumer_hold_cursor(
        HoldCursor(rows),
        session_authorization_id=uuid4(),
        reader=HoldReader(),
        checked_at=checked_at,
    )


def test_no_open_cycle_means_no_hold():
    assert _hold((), NOW) is None
    assert _hold(
        (_cycle("COMPLETE"), _cycle("FAILED", minute=25)),
        NOW.replace(minute=26),
    ) is None


def test_hold_is_at_the_earliest_open_cycle_barrier():
    first = _cycle("RUNNING")
    later = _cycle("PENDING", minute=25)
    reader = HoldReader()

    hold = resident_consumer_hold_cursor(
        HoldCursor((first, later)),
        session_authorization_id=uuid4(),
        reader=reader,
        checked_at=first[2] + timedelta(seconds=10),
    )

    assert hold is not None
    assert hold.target_window_id == UUID(first[1])
    assert hold.cutoff == first[2]
    assert hold.deadline == first[3]
    assert (hold.raw_offset, hold.normalized_offset) == (12, 9)
    assert reader.cutoffs == [first[2]]


def test_hold_moves_on_when_a_window_ends_or_its_deadline_passes():
    ended = _cycle("MISSED")
    stuck = _cycle("RUNNING", minute=25)
    upcoming = _cycle("PENDING", minute=30)
    rows = (ended, stuck, upcoming)

    hold = _hold(rows, stuck[2])
    assert hold.target_window_id == UUID(stuck[1])

    hold = _hold(rows, stuck[3])
    assert hold.target_window_id == UUID(upcoming[1])

    assert _hold(rows, upcoming[3]) is None
