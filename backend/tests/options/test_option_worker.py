import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.worker import OptionMaterializationWorker, OptionWorkerSettings


UTC = timezone.utc


class FakeCalendar:
    def latest_delayed_slot(self, now, **kwargs):
        return datetime(2026, 8, 31, 14, 0, tzinfo=UTC)

    def session_for_slot(self, slot):
        return slot.date()


class FakePipeline:
    def __init__(self):
        self.calls = []
        self.due_retry_slot = None

    def latest_due_retry_cycle(self):
        return self.due_retry_slot

    def run_once(self, underlyers=None, *, as_of, cycle_time, progress_callback=None):
        self.calls.append((as_of, cycle_time))
        return SimpleNamespace(
            results=(SimpleNamespace(status="COMPLETE"),)
        )


def test_option_worker_processes_each_delayed_slot_once():
    pipeline = FakePipeline()
    now = datetime(2026, 8, 31, 14, 16, tzinfo=UTC)
    worker = OptionMaterializationWorker(
        pipeline,
        calendar=FakeCalendar(),
        settings=OptionWorkerSettings(),
        clock=lambda: now,
    )

    assert worker.poll_once() is not None
    assert worker.poll_once() is None
    assert pipeline.calls == [
        (now, datetime(2026, 8, 31, 14, 0, tzinfo=UTC))
    ]


def test_detector_attempt_reports_running_failed_and_recorded_without_fake_completion():
    states = []
    now = datetime(2026, 8, 31, 14, 16, tzinfo=UTC)
    class DetectorPipeline(FakePipeline):
        def run_once(self, *args, **kwargs):
            return SimpleNamespace(results=(SimpleNamespace(status="COMPLETE"),),
                detector_evaluation={"status": "FAILED", "reason": "DETECTOR_SOURCE_TIMEOUT"})
    worker = OptionMaterializationWorker(DetectorPipeline(), calendar=FakeCalendar(), clock=lambda: now,
        detector_status_callback=states.append)
    worker.poll_once()
    assert [row["status"] for row in states] == ["RUNNING", "FAILED"]
    assert states[-1]["reason"] == "DETECTOR_SOURCE_TIMEOUT" and states[-1]["run_id"] is None
    assert states[-1]["finished_at"] == now.isoformat()
    states.clear()
    worker.pipeline.run_once = lambda *args, **kwargs: SimpleNamespace(results=(),
        detector_evaluation={"status": "RECORDED", "run_id": "retained-run", "new_alerts": 0})
    worker._completed_slots.clear()
    worker.poll_once()
    assert [row["status"] for row in states] == ["RUNNING", "RECORDED"]
    assert states[-1]["run_id"] == "retained-run"


def test_detector_status_write_failure_does_not_change_pipeline_result(caplog):
    worker = OptionMaterializationWorker(FakePipeline(), calendar=FakeCalendar(),
        detector_status_callback=lambda payload: (_ for _ in ()).throw(OSError("unavailable")))
    assert worker.poll_once() is not None
    assert "retained results unchanged" in caplog.text


def test_detector_attempt_status_writer_is_scoped_and_bounded(tmp_path, monkeypatch):
    import json
    from scripts import run_option_worker as script

    monkeypatch.setattr(script, "BACKEND_DIR", tmp_path)
    config = SimpleNamespace(configuration_sha256="a" * 64)
    record = script._detector_status_callback(config, "dataset-one")
    now = datetime(2026, 8, 31, 14, 0, tzinfo=UTC)
    for number in range(100):
        stamp = (now + timedelta(minutes=number)).isoformat()
        record(dict(scheduled_cycle=stamp, started_at=stamp, status="FAILED", finished_at=stamp,
            reason="DETECTOR_SOURCE_TIMEOUT"))
    path = tmp_path / "backups/options-worker/detector-status.json"
    assert len(json.loads(path.read_text())["attempts"]) == 96
    record = script._detector_status_callback(config, "dataset-two")
    record(dict(scheduled_cycle=now.isoformat(), started_at=now.isoformat(), status="RUNNING"))
    result = json.loads(path.read_text())
    assert result["dataset_id"] == "dataset-two" and len(result["attempts"]) == 1


def test_option_worker_logs_stock_behavior_shadow_stop(caplog):
    class ShadowPipeline(FakePipeline):
        def run_once(self, underlyers=None, *, as_of, cycle_time, progress_callback=None):
            return SimpleNamespace(results=(SimpleNamespace(
                status="COMPLETE",
                reasons=("STOCK_BEHAVIOR_SHADOW_UNAVAILABLE_RATE_STOP",),
            ),))

    worker = OptionMaterializationWorker(ShadowPipeline(), calendar=FakeCalendar())

    with caplog.at_level(logging.INFO, logger="option-worker"):
        worker.poll_once()

    assert "stock_behavior_shadow=UNAVAILABLE_RATE_STOP:1" in caplog.text


def test_option_worker_logs_package_assessment_state(caplog):
    class PackagePipeline(FakePipeline):
        def run_once(self, underlyers=None, *, as_of, cycle_time, progress_callback=None):
            return SimpleNamespace(results=(SimpleNamespace(
                status="COMPLETE",
                reasons=("OPTION_PACKAGE_ASSESSMENT_COMPLETE",),
            ),))

    worker = OptionMaterializationWorker(PackagePipeline(), calendar=FakeCalendar())

    with caplog.at_level(logging.INFO, logger="option-worker"):
        worker.poll_once()

    assert "package_assessments=COMPLETE:1" in caplog.text


def test_option_worker_maintains_partitions_once_per_utc_month():
    maintained = []
    current = [datetime(2026, 8, 31, 14, 16, tzinfo=UTC)]
    worker = OptionMaterializationWorker(
        FakePipeline(),
        calendar=FakeCalendar(),
        partition_maintainer=maintained.append,
        clock=lambda: current[0],
    )

    worker.poll_once()
    worker.poll_once()
    current[0] = datetime(2026, 9, 1, 14, 16, tzinfo=UTC)
    worker.poll_once()

    assert maintained == [
        datetime(2026, 8, 31, 14, 16, tzinfo=UTC),
        datetime(2026, 9, 1, 14, 16, tzinfo=UTC),
    ]


def test_option_worker_matures_outcomes_after_materialization():
    calls = []

    class OutcomeService:
        def mature(self, *, available_by):
            calls.append(available_by)
            return SimpleNamespace(
                candidates=1, due_measurements=1,
                available_measurements=1, persisted=1, pending=0,
            )

    now = datetime(2026, 8, 31, 14, 16, tzinfo=UTC)
    worker = OptionMaterializationWorker(
        FakePipeline(), calendar=FakeCalendar(), outcome_service=OutcomeService(),
        clock=lambda: now,
    )

    assert worker.poll_once() is not None
    assert calls == [now]


def test_option_worker_runs_continuous_validation_after_materialization():
    calls = []
    now = datetime(2026, 8, 31, 14, 16, tzinfo=UTC)
    worker = OptionMaterializationWorker(
        FakePipeline(), calendar=FakeCalendar(), clock=lambda: now,
        validation_callback=lambda available_at: calls.append(available_at) or {
            "state": "RUNNING"
        },
    )

    assert worker.poll_once() is not None
    assert calls == [now]


def test_outcomes_and_validation_use_post_work_clocks():
    start = datetime(2026, 8, 31, 14, 16, tzinfo=UTC)
    now = [start]
    assessed = []
    validated = []

    class SlowPipeline(FakePipeline):
        def run_once(self, *args, **kwargs):
            now[0] += timedelta(minutes=2)
            return super().run_once(*args, **kwargs)

    class SlowOutcomes:
        def mature(self, *, available_by):
            assessed.append(available_by)
            now[0] += timedelta(minutes=1)
            return SimpleNamespace(candidates=0, due_measurements=0, available_measurements=0, persisted=0, pending=0)

    worker = OptionMaterializationWorker(
        SlowPipeline(), calendar=FakeCalendar(), clock=lambda: now[0],
        outcome_service=SlowOutcomes(), validation_callback=validated.append,
    )
    worker.poll_once()
    assert assessed == [start + timedelta(minutes=2)]
    assert validated == [start + timedelta(minutes=3)]


def test_option_worker_contains_continuous_validation_failure(caplog):
    worker = OptionMaterializationWorker(
        FakePipeline(), calendar=FakeCalendar(),
        validation_callback=lambda _available_at: (_ for _ in ()).throw(
            RuntimeError("validation failed")
        ),
    )

    with caplog.at_level(logging.ERROR, logger="option-worker"):
        assert worker.poll_once() is not None

    assert "materialization remains complete" in caplog.text


def test_option_worker_passes_configured_slot_policy_to_calendar():
    observed = {}

    class RecordingCalendar:
        def latest_delayed_slot(self, now, **kwargs):
            observed.update(kwargs)
            return None

        def session_for_slot(self, slot):
            return slot.date()

    settings = OptionWorkerSettings(
        poll_seconds=5,
        slot_seconds=600,
        provider_delay_seconds=700,
        publication_grace_seconds=45,
    )
    worker = OptionMaterializationWorker(
        FakePipeline(), calendar=RecordingCalendar(), settings=settings,
    )

    assert worker.poll_once() is None
    assert observed == {
        "interval": timedelta(seconds=600),
        "provider_delay": timedelta(seconds=700),
        "publication_grace": timedelta(seconds=45),
    }


def test_option_worker_retries_operational_failure_after_five_minutes():
    class RetryPipeline(FakePipeline):
        def run_once(
            self, underlyers=None, *, as_of, cycle_time, progress_callback=None,
        ):
            self.calls.append((as_of, cycle_time))
            retryable = len(self.calls) == 1
            return SimpleNamespace(results=(SimpleNamespace(
                status="FAILED" if retryable else "ALREADY_COMPLETED",
                retryable=retryable,
            ),))

    pipeline = RetryPipeline()
    current = [datetime(2026, 8, 31, 14, 16, tzinfo=UTC)]
    worker = OptionMaterializationWorker(
        pipeline,
        calendar=FakeCalendar(),
        settings=OptionWorkerSettings(),
        clock=lambda: current[0],
    )

    assert worker.poll_once() is not None
    current[0] += timedelta(minutes=4, seconds=59)
    assert worker.poll_once() is None
    current[0] += timedelta(seconds=1)
    assert worker.poll_once() is not None
    assert worker.poll_once() is None
    assert len(pipeline.calls) == 2


def test_option_worker_does_not_retry_terminal_quality_failure():
    class QualityFailurePipeline(FakePipeline):
        def run_once(
            self, underlyers=None, *, as_of, cycle_time, progress_callback=None,
        ):
            self.calls.append((as_of, cycle_time))
            return SimpleNamespace(results=(SimpleNamespace(
                status="FAILED", retryable=False,
            ),))

    pipeline = QualityFailurePipeline()
    worker = OptionMaterializationWorker(
        pipeline,
        calendar=FakeCalendar(),
        settings=OptionWorkerSettings(),
    )

    assert worker.poll_once() is not None
    assert worker.poll_once() is None
    assert len(pipeline.calls) == 1


def test_option_worker_passes_controlled_underlying_subset():
    observed = []

    class SubsetPipeline(FakePipeline):
        def run_once(
            self, underlyers=None, *, as_of, cycle_time, progress_callback=None,
        ):
            observed.append(underlyers)
            return super().run_once(
                underlyers,
                as_of=as_of,
                cycle_time=cycle_time,
                progress_callback=progress_callback,
            )

    worker = OptionMaterializationWorker(
        SubsetPipeline(),
        calendar=FakeCalendar(),
        underlyers=("SPY",),
    )

    assert worker.poll_once() is not None
    assert observed == [("SPY",)]


def test_option_worker_does_not_replay_superseded_durable_retry():
    class LatestCalendar(FakeCalendar):
        def latest_delayed_slot(self, now, **kwargs):
            return datetime(2026, 8, 31, 14, 15, tzinfo=UTC)

    pipeline = FakePipeline()
    pipeline.due_retry_slot = datetime(2026, 8, 31, 14, 0, tzinfo=UTC)
    worker = OptionMaterializationWorker(
        pipeline,
        calendar=LatestCalendar(),
    )

    assert worker.poll_once() is not None
    assert worker.poll_once() is None
    assert pipeline.due_retry_slot == datetime(2026, 8, 31, 14, 0, tzinfo=UTC)
    assert [cycle_time for _, cycle_time in pipeline.calls] == [
        datetime(2026, 8, 31, 14, 15, tzinfo=UTC),
    ]


def test_option_worker_advances_after_persistent_failure_without_replaying_gaps():
    from options.calendar import OptionExchangeCalendar

    class PersistentFailurePipeline(FakePipeline):
        def run_once(self, underlyers=None, *, as_of, cycle_time, progress_callback=None):
            self.calls.append((as_of, cycle_time))
            return SimpleNamespace(results=(SimpleNamespace(status="FAILED", retryable=True),))

    current = [datetime(2026, 9, 17, 14, 16, tzinfo=UTC)]
    pipeline = PersistentFailurePipeline()
    worker = OptionMaterializationWorker(pipeline, calendar=OptionExchangeCalendar(), clock=lambda: current[0])
    for hour in (14, 15, 16, 19, 20):
        current[0] = datetime(2026, 9, 17, hour, 16, tzinfo=UTC)
        assert worker.poll_once() is not None
    assert pipeline.due_retry_slot is None
    assert [slot for _, slot in pipeline.calls] == [
        datetime(2026, 9, 17, hour, 0, tzinfo=UTC) for hour in (14, 15, 16, 19, 20)
    ]
    assert worker.calendar.latest_delayed_slot(current[0]) == datetime(2026, 9, 17, 20, 0, tzinfo=UTC)


def test_option_worker_new_slot_bypasses_old_retry_cooldown():
    from options.calendar import OptionExchangeCalendar

    class FailurePipeline(FakePipeline):
        def run_once(self, underlyers=None, *, as_of, cycle_time, progress_callback=None):
            self.calls.append((as_of, cycle_time))
            return SimpleNamespace(results=(SimpleNamespace(status="FAILED", retryable=True),))

    current = [datetime(2026, 9, 17, 14, 29, 31, tzinfo=UTC)]
    pipeline = FailurePipeline()
    worker = OptionMaterializationWorker(pipeline, calendar=OptionExchangeCalendar(), clock=lambda: current[0])
    worker.poll_once()
    current[0] = datetime(2026, 9, 17, 14, 30, 30, tzinfo=UTC)
    worker.poll_once()
    assert [slot for _, slot in pipeline.calls] == [
        datetime(2026, 9, 17, 14, 0, tzinfo=UTC),
        datetime(2026, 9, 17, 14, 15, tzinfo=UTC),
    ]


def test_option_worker_outcome_failure_does_not_repeat_completed_ingestion():
    import pytest

    class FailingOutcomes:
        def mature(self, *, available_by):
            raise RuntimeError("outcome failure")

    pipeline = FakePipeline()
    worker = OptionMaterializationWorker(pipeline, calendar=FakeCalendar(), outcome_service=FailingOutcomes())
    with pytest.raises(RuntimeError, match="outcome failure"):
        worker.poll_once()
    assert worker.poll_once() is None
    assert len(pipeline.calls) == 1


def test_option_worker_logs_redact_messages_arguments_and_tracebacks():
    import logging
    from scripts.run_option_worker import RedactingFormatter

    formatter = RedactingFormatter("%(message)s", ("configured-secret",))
    try:
        raise ValueError("configured-secret apiKey=other-secret")
    except ValueError:
        record = logging.LogRecord(
            "option-worker", logging.ERROR, __file__, 1,
            "worker failed %s", ("configured-secret",), sys.exc_info(),
        )
    text = formatter.format(record)
    assert "configured-secret" not in text
    assert "other-secret" not in text
    assert "Traceback" in text
    assert "[REDACTED]" in text