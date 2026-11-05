"""STOCK_ALERTS and STRATEGY_PUBLISH runners (offline fakes; the DB variant is test_stage_runners_bind_v2_db)."""
from __future__ import annotations

import threading
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

import realtime.stage_runners_alerts as runners
from realtime.canonical_stock_alert_repository import (
    CanonicalStockAlertBatchUnavailable,
)
from realtime.database_access_v2 import WriteFenceRejected
from realtime.portal_publisher import PortalPublicationRetryable
from realtime.preflight import ProviderTier
from realtime.runner_environment import RunnerEnvironment
from realtime.stage_orchestrator import StageCancelled
from realtime.stock_alert_repository import RetainedForwardState
from tests.realtime.test_stage_runners_equity import NOW, SESSION, _context, _Database, _receipt_row

SNAPSHOT = SimpleNamespace(payload={"missing_producers": {"replay": "x", "forward": "y"}})
RETAINED_PROJECTION = {"contract": "test-retained-alert-projection"}


@pytest.fixture(autouse=True)
def _no_durable_publication_to_resume(monkeypatch):
    monkeypatch.setattr(
        runners,
        "resume_realtime_stock_alert_publication",
        lambda *args, **kwargs: None,
    )


def _alert_environment():
    return RunnerEnvironment(
        None,
        stock_alert_projection_loader=lambda _known_by: RETAINED_PROJECTION,
    )


def _ready_context(generation=3, **options):
    """A context whose database answers with one visibility-sealed receipt."""
    context = _context("STOCK_ALERTS", **options)
    receipt = {
        **_receipt_row(context),
        "reference_generation": generation,
        "known_by": context.window.cutoff + timedelta(minutes=1),
    }
    database = _Database([receipt])
    return replace(context, database=database), database


def test_alerts_runner_fails_without_the_equity_cycle_receipt_and_computes_nothing(monkeypatch):
    monkeypatch.setattr(runners, "compute_realtime_stock_alerts", lambda *a, **k: pytest.fail("no receipt"))
    context = replace(_context("STOCK_ALERTS"), database=_Database([None]))
    outcome = runners.stock_alerts_runner(_alert_environment())(context)
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.EQUITY_CYCLE_RECEIPT_MISSING,)


@pytest.mark.parametrize("catalog_version", ["stages_v2", "stages_v3"])
def test_split_catalog_alerts_require_input_ready(
    monkeypatch,
    catalog_version,
):
    monkeypatch.setattr(
        runners,
        "compute_realtime_stock_alerts",
        lambda *a, **k: pytest.fail("no receipt"),
    )
    context = _context("STOCK_ALERTS")
    context = replace(
        context,
        plan=SimpleNamespace(
            cohort_id=context.plan.cohort_id,
            session_date=SESSION,
            stage_catalog_version=catalog_version,
        ),
        database=_Database([None]),
    )
    outcome = runners.stock_alerts_runner(
        _alert_environment()
    )(context)
    assert outcome.reason_codes == (
        runners.INPUT_READY_RECEIPT_MISSING,
    )


def test_alerts_runner_uses_the_receipt_generation_without_requery(
    monkeypatch,
):
    seen = {}
    monkeypatch.setattr(
        runners,
        "compute_realtime_stock_alerts",
        lambda *args, **kwargs: seen.update(kwargs) or SNAPSHOT,
    )
    monkeypatch.setattr(
        runners,
        "publish_realtime_stock_alerts",
        lambda *args, **kwargs: None,
    )
    context, database = _ready_context(generation=7)

    outcome = runners.stock_alerts_runner(_alert_environment())(context)

    assert outcome.state == "COMPLETE"
    assert seen["source_generation"] == 7
    assert seen["known_by"] == context.window.cutoff + timedelta(minutes=1)
    assert len(database.cursor.statements) == 1


def test_alerts_runner_blocks_without_the_retained_results_source(monkeypatch):
    monkeypatch.setattr(
        runners,
        "compute_realtime_stock_alerts",
        lambda *a, **k: pytest.fail("no retained source"),
    )
    context, _ = _ready_context()

    outcome = runners.stock_alerts_runner(
        RunnerEnvironment(None)
    )(context)

    assert outcome.state == "BLOCKED_POLICY"
    assert outcome.reason_codes == (
        runners.STOCK_ALERTS_RETAINED_RESULTS_SOURCE_UNAVAILABLE,
    )


def test_alerts_runner_blocks_when_no_canonical_batch_is_bound_to_input_ready(monkeypatch):
    monkeypatch.setattr(
        runners,
        "compute_realtime_stock_alerts",
        lambda *a, **k: pytest.fail("unbound results cannot compute"),
    )
    context, _ = _ready_context()

    def unavailable(_request):
        raise CanonicalStockAlertBatchUnavailable("not bound")

    outcome = runners.stock_alerts_runner(
        RunnerEnvironment(
            None,
            stock_alert_projection_loader=unavailable,
        )
    )(context)

    assert outcome.state == "BLOCKED_POLICY"
    assert outcome.reason_codes == (
        runners.STOCK_ALERTS_RETAINED_RESULTS_SOURCE_UNAVAILABLE,
    )


def test_alerts_runner_computes_read_only_then_publishes_under_the_publication_role(monkeypatch):
    seen = {}

    def compute(database, **scope):
        seen["compute"] = (database.process_plan.role.value, scope)
        return SNAPSHOT

    def publish(database, snapshot, **options):
        seen["publish"] = (database.process_plan.role.value, snapshot, options)

    monkeypatch.setattr(runners, "compute_realtime_stock_alerts", compute)
    monkeypatch.setattr(runners, "publish_realtime_stock_alerts", publish)
    context, database = _ready_context(generation=3)
    outcome = runners.stock_alerts_runner(_alert_environment())(context)
    assert outcome.state == "COMPLETE" and outcome.receipt is None
    assert outcome.reason_codes == ("MISSING_PRODUCER:forward", "MISSING_PRODUCER:replay")
    role, scope = seen["compute"]
    assert role == "COMPUTE"
    assert scope == dict(cohort_id=context.plan.cohort_id, session=SESSION, cutoff=NOW,
                         known_by=NOW + timedelta(minutes=1), source_generation=3,
                         retained_projection=RETAINED_PROJECTION)
    role, snapshot, options = seen["publish"]
    assert (role, snapshot) == ("PUBLICATION", SNAPSHOT)
    assert options["receipt"].cohort_id == context.plan.cohort_id and options["receipt"].cutoff == NOW
    assert options["receipt"].provider_tier is ProviderTier.MASSIVE_DEVELOPER
    assert options["session_close"] is False


def test_alerts_runner_resumes_durable_publication_before_recomputing(monkeypatch):
    work_id = uuid4()
    monkeypatch.setattr(
        runners,
        "resume_realtime_stock_alert_publication",
        lambda *args, **kwargs: SimpleNamespace(
            reason_codes=("PORTAL_PUBLICATION_HISTORICAL_STALE",),
            work_id=work_id,
        ),
    )
    monkeypatch.setattr(
        runners,
        "compute_realtime_stock_alerts",
        lambda *args, **kwargs: pytest.fail(
            "durable publication retry must not recompute"
        ),
    )
    context, _ = _ready_context()

    outcome = runners.stock_alerts_runner(
        _alert_environment()
    )(context)

    assert outcome.state == "COMPLETE"
    assert outcome.reason_codes == (
        "PORTAL_PUBLICATION_HISTORICAL_STALE",
    )
    assert outcome.evidence_ref == str(work_id)


def test_alerts_runner_defers_until_durable_publication_retry(monkeypatch):
    retry_at = NOW + timedelta(seconds=30)
    monkeypatch.setattr(
        runners,
        "resume_realtime_stock_alert_publication",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            PortalPublicationRetryable(
                uuid4(),
                retry_at,
                "PORTAL_PUBLICATION_RETRY_PENDING",
            )
        ),
    )
    context, _ = _ready_context()

    outcome = runners.stock_alerts_runner(
        _alert_environment()
    )(context)

    assert outcome.state == "DEFERRED"
    assert outcome.reason_codes == (
        "PORTAL_PUBLICATION_RETRY_PENDING",
    )
    assert outcome.retry_not_before == retry_at


def test_a_snapshot_without_missing_producers_completes_clean(monkeypatch):
    monkeypatch.setattr(runners, "compute_realtime_stock_alerts", lambda *a, **k: SimpleNamespace(payload={}))
    monkeypatch.setattr(runners, "publish_realtime_stock_alerts", lambda *a, **k: None)
    context, _ = _ready_context()
    outcome = runners.stock_alerts_runner(_alert_environment())(context)
    assert outcome.state == "COMPLETE" and outcome.reason_codes == ()


def test_the_final_window_of_a_session_publishes_the_kept_release(monkeypatch):
    import exchange_calendars

    close = exchange_calendars.get_calendar("XNYS").session_close(str(SESSION)).to_pydatetime()
    published = []
    monkeypatch.setattr(runners, "compute_realtime_stock_alerts", lambda *a, **k: SNAPSHOT)
    monkeypatch.setattr(runners, "publish_realtime_stock_alerts",
                        lambda database, snapshot, **options: published.append(options))
    context = _context("STOCK_ALERTS", cutoff=close)
    context = replace(
        context,
        database=_Database([{
            **_receipt_row(context),
            "cutoff": close,
            "known_by": close + timedelta(minutes=1),
        }]),
    )
    runners.stock_alerts_runner(_alert_environment())(context)
    assert published[0]["session_close"] is True


def test_a_retained_forward_state_blocks_instead_of_replacing_the_release(monkeypatch):
    def refuse(*args, **kwargs):
        raise RetainedForwardState("refusing to replace retained publications")

    monkeypatch.setattr(runners, "compute_realtime_stock_alerts", refuse)
    monkeypatch.setattr(runners, "publish_realtime_stock_alerts", lambda *a, **k: pytest.fail("must not publish"))
    context, _ = _ready_context()
    outcome = runners.stock_alerts_runner(_alert_environment())(context)
    assert outcome.state == "BLOCKED_POLICY" and outcome.reason_codes == (runners.STOCK_ALERTS_RETAINED_FORWARD_STATE,)


def test_alerts_runner_stops_between_phases_when_the_stage_is_cancelled(monkeypatch):
    cancelled = threading.Event()
    published = []
    monkeypatch.setattr(runners, "publish_realtime_stock_alerts", lambda *a, **k: published.append(True))

    def compute(database, **scope):
        cancelled.set()  # the coordinator abandons the stage while the compute phase runs
        return SNAPSHOT

    monkeypatch.setattr(runners, "compute_realtime_stock_alerts", compute)
    context, _ = _ready_context(cancelled=cancelled)
    outcome = runners.stock_alerts_runner(_alert_environment())(context)
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,) and published == []

    monkeypatch.setattr(runners, "compute_realtime_stock_alerts", lambda *a, **k: pytest.fail("cancelled"))
    context, _ = _ready_context(cancelled=cancelled)  # already cancelled before the compute phase
    assert runners.stock_alerts_runner(_alert_environment())(context).reason_codes == (runners.STAGE_CANCELLED,)


@pytest.mark.parametrize("error", [StageCancelled("abandoned"), WriteFenceRejected("fenced")])
def test_a_rejected_publication_write_is_a_cancelled_failure_not_a_crash(monkeypatch, error):
    def publish(*args, **kwargs):
        raise error

    monkeypatch.setattr(runners, "compute_realtime_stock_alerts", lambda *a, **k: SNAPSHOT)
    monkeypatch.setattr(runners, "publish_realtime_stock_alerts", publish)
    context, _ = _ready_context()
    outcome = runners.stock_alerts_runner(_alert_environment())(context)
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)


def _strategy_context(row=True, **options):
    context = _context("STRATEGY_PUBLISH", **options)
    return replace(context, database=_Database([_receipt_row(context) if row else None]))


def test_strategy_runner_fails_without_the_equity_cycle_receipt(monkeypatch):
    monkeypatch.setattr(runners, "materialize_due_realtime_strategies", lambda *a, **k: pytest.fail("no receipt"))
    outcome = runners.strategy_publish_runner(RunnerEnvironment(None))(_strategy_context(row=False))
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.EQUITY_CYCLE_RECEIPT_MISSING,)


def test_v3_strategy_runner_requires_input_ready(monkeypatch):
    monkeypatch.setattr(
        runners,
        "materialize_due_realtime_strategies",
        lambda *a, **k: pytest.fail("no receipt"),
    )
    context = _strategy_context(row=False)
    context = replace(
        context,
        plan=SimpleNamespace(
            cohort_id=context.plan.cohort_id,
            session_date=SESSION,
            stage_catalog_version="stages_v3",
        ),
    )
    outcome = runners.strategy_publish_runner(
        RunnerEnvironment(None)
    )(context)
    assert outcome.reason_codes == (
        runners.INPUT_READY_RECEIPT_MISSING,
    )


def test_strategy_runner_publishes_one_interval_at_a_time_under_both_roles(monkeypatch):
    calls = []

    def materialize(compute, publication, **options):
        calls.append((compute.process_plan.role.value, publication.process_plan.role.value, options))
        interval = options["intervals"][0]
        status = "ALREADY_STORED" if interval == "1h" else "PUBLISHED"
        return (dict(interval=interval, market_time="t", status=status, snapshot_id=str(uuid4())),)

    monkeypatch.setattr(runners, "materialize_due_realtime_strategies", materialize)
    context = _strategy_context()
    outcome = runners.strategy_publish_runner(RunnerEnvironment(None))(context)
    assert [options["intervals"] for _, _, options in calls] == [("1d",), ("1h",)]
    assert {(compute, publication) for compute, publication, _ in calls} == {("COMPUTE", "PUBLICATION")}
    for _, _, options in calls:
        # Strategies reuse the exact input visibility seal.
        assert options["cohort_id"] == context.plan.cohort_id
        assert options["available_by"] == (
            context.window.cutoff + timedelta(minutes=1)
        )
        assert "known_by" not in options
        assert options["receipt"].cohort_id == context.plan.cohort_id and options["receipt"].cutoff == NOW
    # A retained cutoff (ALREADY_STORED) is not a failure and does not degrade the stage.
    assert outcome.state == "COMPLETE" and outcome.reason_codes == ()
    assert outcome.evidence_ref == "1d=PUBLISHED;1h=ALREADY_STORED"


def test_a_snapshot_whose_members_could_not_be_evaluated_degrades_the_stage(monkeypatch):
    def materialize(compute, publication, **options):
        interval = options["intervals"][0]
        analysis = ("INSUFFICIENT_HISTORY", "COMPLETE_EMPTY") if interval == "1d" else ("COMPLETE", "COMPLETE_EMPTY")
        return (dict(interval=interval, market_time="t", status="PUBLISHED", snapshot_id="s", analysis=analysis),)

    monkeypatch.setattr(runners, "materialize_due_realtime_strategies", materialize)
    outcome = runners.strategy_publish_runner(RunnerEnvironment(None))(_strategy_context())
    # Published and valid, but the daily members were not evaluated: visible, never a failure.
    assert outcome.state == "COMPLETE" and outcome.reason_codes == ("STRATEGY_1D_INSUFFICIENT_HISTORY",)
    assert outcome.evidence_ref == "1d=PUBLISHED;1h=PUBLISHED"


def test_strategy_runner_stops_between_intervals_when_cancelled(monkeypatch):
    cancelled, calls = threading.Event(), []

    def materialize(compute, publication, **options):
        calls.append(options["intervals"])
        cancelled.set()
        return (dict(interval=options["intervals"][0], market_time="t", status="PUBLISHED", snapshot_id="s"),)

    monkeypatch.setattr(runners, "materialize_due_realtime_strategies", materialize)
    outcome = runners.strategy_publish_runner(RunnerEnvironment(None))(_strategy_context(cancelled=cancelled))
    assert calls == [("1d",)]
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)


def test_one_failing_interval_degrades_the_stage_and_both_failing_fail_it(monkeypatch):
    def only_hourly_fails(compute, publication, **options):
        if options["intervals"] == ("1h",):
            raise ValueError("Strategy producer retained cutoff identity/hash drift")
        return (dict(interval="1d", market_time="t", status="PUBLISHED", snapshot_id="s"),)

    monkeypatch.setattr(runners, "materialize_due_realtime_strategies", only_hourly_fails)
    outcome = runners.strategy_publish_runner(RunnerEnvironment(None))(_strategy_context())
    assert outcome.state == "COMPLETE" and outcome.reason_codes == ("STRATEGY_1H_FAILED:ValueError",)
    assert outcome.evidence_ref == "1d=PUBLISHED"

    def always_fails(compute, publication, **options):
        raise LookupError("cohort unavailable")

    monkeypatch.setattr(runners, "materialize_due_realtime_strategies", always_fails)
    outcome = runners.strategy_publish_runner(RunnerEnvironment(None))(_strategy_context())
    assert outcome.state == "FAILED"
    assert outcome.reason_codes == ("STRATEGY_1D_FAILED:LookupError", "STRATEGY_1H_FAILED:LookupError")


@pytest.mark.parametrize("error", [StageCancelled("abandoned"), WriteFenceRejected("fenced")])
def test_a_rejected_strategy_write_is_a_cancelled_failure_and_skips_the_other_interval(monkeypatch, error):
    calls = []

    def materialize(compute, publication, **options):
        calls.append(options["intervals"])
        raise error

    monkeypatch.setattr(runners, "materialize_due_realtime_strategies", materialize)
    outcome = runners.strategy_publish_runner(RunnerEnvironment(None))(_strategy_context())
    assert calls == [("1d",)]
    assert outcome.state == "FAILED" and outcome.reason_codes == (runners.STAGE_CANCELLED,)


def test_the_alert_stages_are_registered_in_the_data_lane():
    from realtime.stage_graph import STAGES_V1
    from realtime.stage_runners_v2 import RUNNER_FACTORIES

    assert RUNNER_FACTORIES["STOCK_ALERTS"] is runners.stock_alerts_runner
    assert RUNNER_FACTORIES["STRATEGY_PUBLISH"] is runners.strategy_publish_runner
    assert STAGES_V1["STOCK_ALERTS"].lane == STAGES_V1["STRATEGY_PUBLISH"].lane == "data"
