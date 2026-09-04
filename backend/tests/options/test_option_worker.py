import sys
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


def test_option_worker_prioritizes_durable_same_session_retry():
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
    pipeline.due_retry_slot = None
    assert worker.poll_once() is not None
    assert [cycle_time for _, cycle_time in pipeline.calls] == [
        datetime(2026, 8, 31, 14, 0, tzinfo=UTC),
        datetime(2026, 8, 31, 14, 15, tzinfo=UTC),
    ]