import importlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest


script = importlib.import_module(
    "scripts.recover_realtime_v2_advanced_drained_journal"
)
UTC = timezone.utc
DRAINED_AT = datetime(2026, 11, 2, 20, 4, 1, tzinfo=UTC)
GENERATION = UUID(int=1)


def _snapshot(**changes):
    snapshot = {
        "registration": SimpleNamespace(
            status="DRAINED",
            journal_generation=GENERATION,
            journal_schema_version=1,
            bridge_committed_offset=1863,
        ),
        "drained_at": DRAINED_AT,
        "session_ended_at": datetime(2026, 11, 2, 21, 0, 28, tzinfo=UTC),
        "session_last_raw_offset": 2374,
        "session_last_event_offset": 2377,
        "plan_status": "COMPLETE",
        "checkpoints": (1863,),
        "canonical_offset": 1863,
        "live_stream_active": False,
    }
    snapshot.update(changes)
    return snapshot


def _state(**changes):
    values = {
        "generation": GENERATION,
        "schema_version": 1,
        "integrity_check": "ok",
        "quarantine_count": 0,
        "raw_committed_offset": 2374,
        "normalized_committed_raw_offset": 2374,
        "normalized_committed_event_offset": 2377,
    }
    values.update(changes)
    return SimpleNamespace(**values)


def test_accepts_the_november_second_false_drain():
    script.validate_false_drain(_snapshot(), _state())


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        (
            {"session_ended_at": DRAINED_AT + timedelta(seconds=30)},
            "did not outlive the seal",
        ),
        ({"session_ended_at": None}, "has not ended"),
        ({"live_stream_active": True}, "stream is live"),
        ({"plan_status": "ACTIVE"}, "plan is still active"),
        ({"checkpoints": (1900,)}, "moved away"),
        ({"checkpoints": ()}, "unavailable or ambiguous"),
        ({"checkpoints": (1863, 1900)}, "unavailable or ambiguous"),
        ({"canonical_offset": 1900}, "ahead of the checkpoint"),
    ),
)
def test_rejects_anything_short_of_a_proven_false_drain(changes, message):
    with pytest.raises(ValueError, match=message):
        script.validate_false_drain(_snapshot(**changes), _state())


def test_rejects_a_journal_not_in_the_drained_state():
    registration = SimpleNamespace(
        status="DRAINING",
        journal_generation=GENERATION,
        journal_schema_version=1,
        bridge_committed_offset=1863,
    )
    with pytest.raises(ValueError, match="not DRAINED"):
        script.validate_false_drain(
            _snapshot(registration=registration),
            _state(),
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"quarantine_count": 1}, "damaged or quarantined"),
        ({"integrity_check": "corrupt"}, "damaged or quarantined"),
        ({"generation": UUID(int=2)}, "identity changed"),
        ({"raw_committed_offset": 2300}, "disagree"),
        ({"normalized_committed_event_offset": 2300}, "disagree"),
        ({"normalized_committed_raw_offset": 2300}, "disagree"),
    ),
)
def test_rejects_a_journal_file_that_does_not_match(changes, message):
    with pytest.raises(ValueError, match=message):
        script.validate_false_drain(_snapshot(), _state(**changes))


def test_a_resumed_replay_may_start_past_the_drained_offset():
    snapshot = _snapshot(checkpoints=(2000,), canonical_offset=2000)

    script.validate_false_drain(snapshot, _state(), resuming=True)
    with pytest.raises(ValueError, match="moved away"):
        script.validate_false_drain(snapshot, _state())
