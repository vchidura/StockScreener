import importlib
import json
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from realtime.advanced_ingress_controller import (
    HybridRepairIdentity,
    HybridStartupState,
    IngressFailureDisposition,
)
from realtime.advanced_ingress_repository import (
    AdvancedIngressAdmissionError,
    load_advanced_ingress_runtime_plan_cursor,
)


script = importlib.import_module(
    "scripts.run_realtime_v2_advanced_controller"
)


def _base():
    return [
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--authorization-id",
        "11111111-1111-1111-1111-111111111111",
        "--universe-target",
        "1",
        "--stream-sessions",
        "3",
        "--connection-attempts",
        "3",
        "--decoded-bytes",
        "10485760",
        "--rest-repair-requests",
        "0",
        "--rest-repair-decoded-bytes",
        "0",
        "--execute",
    ]


def test_controller_requires_explicit_execution_and_bounded_plan_budget():
    base = _base()

    args = script.parse_args(base)

    assert args.authorization_id
    assert args.budgets.stream_sessions == 3
    assert args.budgets.connection_attempts == 3
    with pytest.raises(SystemExit):
        script.parse_args(base[:-1])


def test_controller_requires_exact_rest_repair_allocation():
    base = _base()
    requests = base.index("--rest-repair-requests") + 1
    decoded = base.index("--rest-repair-decoded-bytes") + 1
    base[requests] = "2"
    base[decoded] = "4096"

    with pytest.raises(SystemExit):
        script.parse_args(base)

    args = script.parse_args([
        *base,
        "--rest-repair-response-byte-limit",
        "2048",
        "--rest-repair-expected-cost",
        "standing authorization repair allocation",
    ])

    assert args.budgets.rest_repair_requests == 2
    assert args.rest_repair_response_byte_limit == 2048


def test_controller_auto_catch_up_derives_target_budget():
    base = _base()
    base[base.index("--universe-target") + 1] = "9"
    args = script.parse_args([
        *base,
        "--catch-up-mode",
        "AUTO",
    ])

    assert args.catch_up_mode == "AUTO"
    assert args.budgets.rest_repair_requests == 18
    assert args.budgets.rest_repair_decoded_bytes == (
        18 * (1024 * 1024 + 1)
    )
    assert args.rest_repair_response_byte_limit == 1024 * 1024
    assert args.rest_repair_expected_cost == (
        "included Massive Advanced aggregate entitlement"
    )


def test_controller_reads_exact_plan_and_rest_allocation_from_environment(
    monkeypatch,
):
    base = _base()
    base[base.index("--rest-repair-requests") + 1] = "2"
    base[base.index("--rest-repair-decoded-bytes") + 1] = "4096"
    cohort = "22222222-2222-2222-2222-222222222222"
    monkeypatch.setenv(
        "REALTIME_V2_ADVANCED_PREFERRED_COHORT_ID",
        cohort,
    )
    monkeypatch.setenv(
        "REALTIME_V2_ADVANCED_COHORT_SELECTION_REFERENCE",
        "manifest-bound-cohort",
    )
    monkeypatch.setenv(
        "REALTIME_V2_ADVANCED_REST_REPAIR_RESPONSE_BYTE_LIMIT",
        "2048",
    )
    monkeypatch.setenv(
        "REALTIME_V2_ADVANCED_REST_REPAIR_EXPECTED_COST",
        "included entitlement",
    )

    args = script.parse_args(base)

    assert str(args.preferred_cohort_id) == cohort
    assert args.cohort_selection_reference == "manifest-bound-cohort"
    assert args.rest_repair_response_byte_limit == 2048
    assert args.rest_repair_expected_cost == "included entitlement"


def test_child_command_has_one_plan_scope_and_no_retry_tuning():
    args = script.parse_args(_base())

    command = script._child_command(
        args,
        args.authorization_id,
    )
    rendered = " ".join(command)

    assert "run_realtime_v2_advanced_equity_ingress.py" in rendered
    assert "--ingress-plan-id" in command
    assert "--execute" in command
    assert "--cohort-id" not in command
    assert "--connection-attempts" not in command
    assert "--debug-once" not in command
    assert "--env-file" not in command


def test_external_ingress_mode_is_explicit():
    args = script.parse_args([*_base(), "--external-ingress"])

    assert args.external_ingress is True


def test_active_external_ingress_does_not_exhaust_in_use_connection():
    active = SimpleNamespace(
        connection_attempt_count=1,
        maximum_connection_attempts=1,
        latest_stream_status="SUBSCRIBING",
    )
    stopped = SimpleNamespace(
        connection_attempt_count=1,
        maximum_connection_attempts=1,
        latest_stream_status="STOPPED",
    )

    assert not script._connection_budget_exhausted(
        active,
        external_ingress=True,
    )
    assert script._connection_budget_exhausted(
        active,
        external_ingress=False,
    )
    assert script._connection_budget_exhausted(
        stopped,
        external_ingress=True,
    )


def test_external_ingress_with_zero_rest_does_not_plan_hybrid_repair():
    hybrid = MagicMock()

    script._tick_hybrid_repair(
        hybrid,
        external_ingress=True,
        rest_repair_requests=0,
        rest_repair_decoded_bytes=0,
    )

    hybrid.tick.assert_not_called()
    script._tick_hybrid_repair(
        hybrid,
        external_ingress=True,
        rest_repair_requests=1,
        rest_repair_decoded_bytes=1,
    )
    hybrid.tick.assert_called_once_with()


def test_runtime_scope_reports_terminal_plan_status_before_scope_mismatch():
    cursor = MagicMock()
    row = [None] * 32
    row[17] = "BLOCKED"
    cursor.fetchone.return_value = tuple(row)

    with pytest.raises(
        AdvancedIngressAdmissionError,
        match=(
            "daily ingress plan status is not "
            "runtime-admissible: BLOCKED"
        ),
    ):
        load_advanced_ingress_runtime_plan_cursor(
            cursor,
            script.UUID(int=1),
        )

    cursor.execute.assert_called_once()


def test_controller_pins_the_prepared_cohort_identity():
    cohort = "22222222-2222-2222-2222-222222222222"
    args = script.parse_args([
        *_base(),
        "--preferred-cohort-id",
        cohort,
        "--cohort-selection-reference",
        "preflight-bound-cohort",
    ])

    assert str(args.preferred_cohort_id) == cohort
    assert args.cohort_selection_reference == "preflight-bound-cohort"
    with pytest.raises(SystemExit):
        script.parse_args([
            *_base(),
            "--preferred-cohort-id",
            cohort,
        ])


def test_controller_pins_canary_window_from_cli_or_container_environment(monkeypatch):
    started = "2026-10-19T13:30:00+00:00"
    ended = "2026-10-19T13:40:00+00:00"
    args = script.parse_args([
        *_base(), "--canary-start-at", started, "--canary-end-at", ended,
    ])
    assert args.canary_end_at - args.canary_start_at == timedelta(seconds=600)
    monkeypatch.setenv("REALTIME_V2_ADVANCED_CANARY_START_AT", started)
    monkeypatch.setenv("REALTIME_V2_ADVANCED_CANARY_END_AT", ended)
    restarted = script.parse_args(_base())
    assert restarted.canary_start_at == args.canary_start_at
    assert restarted.canary_end_at == args.canary_end_at
    assert restarted.budgets == args.budgets
    with pytest.raises(SystemExit):
        script.parse_args([*_base(), "--canary-end-at", "2026-10-19T13:40:01+00:00"])
    with pytest.raises(SystemExit):
        script.parse_args([*_base(), "--startup-backfill-sessions", "1"])
    monkeypatch.delenv("REALTIME_V2_ADVANCED_CANARY_END_AT")
    with pytest.raises(SystemExit):
        script.parse_args(_base())


def test_controller_child_runner_does_not_retry_a_finished_process():
    class Process:
        returncode = 2

        def poll(self):
            return self.returncode

        def communicate(self):
            return "", "failed"

    calls = []

    def factory(command, **_kwargs):
        calls.append(tuple(command))
        return Process()

    result = script._run_child(
        ["child"],
        stop=threading.Event(),
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        poll_seconds=0.01,
        shutdown_seconds=1,
        popen_factory=factory,
    )

    assert result[:2] == (2, False)
    assert calls == [("child",)]


def test_websocket_child_starts_before_hybrid_repair_tick():
    order = []

    class Process:
        returncode = 0

        def __init__(self):
            self.polls = 0

        def poll(self):
            self.polls += 1
            return None if self.polls == 1 else 0

        def communicate(self):
            return "", ""

    def factory(command, **_kwargs):
        order.append(("websocket", tuple(command)))
        return Process()

    result = script._run_child(
        ["stream"],
        stop=threading.Event(),
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        poll_seconds=0,
        shutdown_seconds=1,
        popen_factory=factory,
        tick=lambda: order.append(("repair-tick", ())),
    )

    assert result[:2] == (0, False)
    assert [item[0] for item in order] == ["websocket", "repair-tick"]


def test_hybrid_repair_command_reuses_fixed_cutoff_incremental_runner(tmp_path):
    args = script.parse_args([
        *_base()[:-1],
        "--rest-repair-requests",
        "2",
        "--rest-repair-decoded-bytes",
        "4096",
        "--rest-repair-response-byte-limit",
        "2048",
        "--rest-repair-expected-cost",
        "bounded",
        "--execute",
    ])
    runtime = SimpleNamespace(
        cohort_id=script.UUID(int=2),
        plan_id=script.UUID(int=3),
        session_date=datetime(2026, 10, 19).date(),
        authorized_end_at=datetime(
            2026, 10, 19, 20, 0, tzinfo=timezone.utc,
        ),
        authorization_reference="owner-approved",
        budgets=args.budgets,
    )
    identity = HybridRepairIdentity(
        script.UUID(int=4),
        "adv-hybrid-test",
        HybridStartupState.REPAIR_READY,
        datetime(2026, 10, 19, 13, 30, tzinfo=timezone.utc),
        datetime(2026, 10, 19, 13, 45, tzinfo=timezone.utc),
        datetime(2026, 10, 19, 13, 30, tzinfo=timezone.utc),
        datetime(2026, 10, 19, 13, 40, tzinfo=timezone.utc),
        datetime(2026, 10, 19, 13, 45, tzinfo=timezone.utc),
        "MASSIVE_STOCKS_AGGREGATES_1M",
        "advanced-real-time-closed-bar-v1",
        timedelta(0),
        timedelta(seconds=10),
        2,
    )

    command = script._incremental_repair_command(
        args,
        runtime,
        identity,
        tmp_path / "repair.json",
        SimpleNamespace(
            rest_provider_delay_seconds=0,
            rest_aggregate_endpoint_class=(
                "MASSIVE_STOCKS_AGGREGATES_1M"
            ),
            rest_eligibility_policy_version=(
                "advanced-real-time-closed-bar-v1"
            ),
        ),
        2,
    )

    assert "run_realtime_v2_incremental_equity.py" in " ".join(command)
    assert command[command.index("--cutoff") + 1].endswith("13:40:00+00:00")
    assert "--advanced-ingress-plan-id" in command
    assert command[command.index("--provider-delay-seconds") + 1] == "0"
    assert command[command.index("--repair-attempt") + 1] == "2"
    assert command.count("--once") == 1


def test_zero_repair_budget_blocks_hybrid_gap_without_byte_limit(
    monkeypatch,
    tmp_path,
):
    now = datetime(2026, 10, 22, 14, 10, tzinfo=timezone.utc)
    args = script.parse_args(_base())
    runtime = SimpleNamespace(
        cohort_id=script.UUID(int=2),
        plan_id=script.UUID(int=3),
        budgets=args.budgets,
    )

    class Transaction:
        def __enter__(self):
            return SimpleNamespace(cursor=MagicMock())

        def __exit__(self, *_args):
            return False

    provider = SimpleNamespace(
        transaction=lambda *_args, **_kwargs: Transaction()
    )
    monkeypatch.setattr(
        script,
        "_load_hybrid_progress_cursor",
        lambda *_args: (
            now - timedelta(minutes=1),
            None,
            (1,),
        ),
    )
    monkeypatch.setattr(
        script,
        "plan_hybrid_startup",
        lambda **_kwargs: SimpleNamespace(
            state=HybridStartupState.REPAIR_READY,
        ),
    )
    monkeypatch.setattr(
        script,
        "hybrid_repair_identity",
        lambda **_kwargs: SimpleNamespace(
            maximum_attempts=2,
            release_at=now,
        ),
    )
    monkeypatch.setattr(
        script,
        "_ensure_hybrid_window_cursor",
        lambda *_args, **_kwargs: pytest.fail(
            "zero repair budget created a repair window"
        ),
    )
    coordinator = script._HybridRepairCoordinator(
        provider,
        args,
        runtime,
        session_open=now - timedelta(minutes=40),
        deadline=now + timedelta(minutes=5),
        journal_root=tmp_path,
        lease_seconds=60,
        settings=SimpleNamespace(
            rest_provider_delay_seconds=0,
            rest_aggregate_endpoint_class="MASSIVE_STOCKS_AGGREGATES_1M",
            rest_eligibility_policy_version=(
                "advanced-real-time-closed-bar-v1"
            ),
            rest_publication_retry_grace_seconds=10,
            rest_maximum_attempts=2,
        ),
    )

    coordinator._plan_next(now)

    assert coordinator.terminal_outcome == "AUTHORIZATION_EXHAUSTED"
    assert coordinator.window is None


def test_hybrid_attempt_paths_and_claims_are_durable_and_bounded(tmp_path):
    repair_id = script.UUID(int=4)
    first = script._hybrid_report_path(tmp_path, repair_id, 1)
    second = script._hybrid_report_path(tmp_path, repair_id, 2)
    cursor = MagicMock()
    cursor.fetchone.return_value = (2,)

    claimed = script._claim_hybrid_window_cursor(
        cursor,
        repair_id,
        script.UUID(int=5),
        checked_at=datetime(2026, 10, 19, 13, 45, tzinfo=timezone.utc),
        lease_seconds=330,
        retry_grace=timedelta(seconds=10),
        maximum_attempts=2,
    )

    assert first != second
    assert "attempt-01" in first.name
    assert "attempt-02" in second.name
    assert claimed == 2
    statement = cursor.execute.call_args.args[0]
    assert "owner_epoch<%s" in statement


def test_coverage_pending_gets_one_durable_retry_then_blocks(tmp_path):
    now = datetime(2026, 10, 19, 13, 45, tzinfo=timezone.utc)
    identity = HybridRepairIdentity(
        script.UUID(int=4),
        "adv-hybrid-test",
        HybridStartupState.REPAIR_READY,
        now - timedelta(minutes=5),
        now,
        now - timedelta(minutes=5),
        now,
        now,
        "MASSIVE_STOCKS_AGGREGATES_1M",
        "advanced-real-time-closed-bar-v1",
        timedelta(0),
        timedelta(seconds=10),
        2,
    )
    cursor = MagicMock()

    class Transaction:
        def __enter__(self):
            return SimpleNamespace(cursor=cursor)

        def __exit__(self, *_args):
            return False

    provider = SimpleNamespace(
        transaction=lambda *_args, **_kwargs: Transaction()
    )
    coordinator = script._HybridRepairCoordinator(
        provider,
        SimpleNamespace(),
        SimpleNamespace(plan_id=script.UUID(int=3)),
        session_open=now - timedelta(minutes=15),
        deadline=now + timedelta(minutes=30),
        journal_root=tmp_path,
        lease_seconds=330,
        settings=SimpleNamespace(),
        utc_clock=lambda: now,
    )
    first_report = script._hybrid_report_path(
        tmp_path,
        identity.repair_id,
        1,
    )
    first_report.write_text(json.dumps({
        "status": "RETRYABLE_COVERAGE_PENDING",
    }), encoding="utf-8")
    coordinator.window = script._HybridRepairWindow(
        identity,
        "RUNNING",
        1,
        now,
        None,
        first_report,
    )
    cursor.fetchone.side_effect = [
        (1,),
        ("PENDING", 1, now, None),
    ]

    coordinator._handle_attempt_result(
        now,
        return_code=3,
        stderr="",
    )

    assert coordinator.window.state == "PENDING"
    assert coordinator.window.owner_epoch == 1
    assert coordinator.terminal_outcome is None
    assert now + identity.retry_grace == now + timedelta(seconds=10)

    second_report = script._hybrid_report_path(
        tmp_path,
        identity.repair_id,
        2,
    )
    second_report.write_text(json.dumps({
        "status": "RETRYABLE_COVERAGE_PENDING",
    }), encoding="utf-8")
    coordinator.window = script._HybridRepairWindow(
        identity,
        "RUNNING",
        2,
        now + timedelta(seconds=10),
        None,
        second_report,
    )

    coordinator._handle_attempt_result(
        now + timedelta(seconds=10),
        return_code=3,
        stderr="",
    )

    assert coordinator.terminal_outcome == "BLOCKED_GAP"
    assert any(
        "state=%s" in call.args[0]
        and call.args[1][0] == "RETAINED_GAP"
        for call in cursor.execute.call_args_list
    )


def test_reportless_child_exit_accepts_complete_durable_closure(
    tmp_path,
    monkeypatch,
):
    now = datetime(2026, 10, 27, 14, 50, tzinfo=timezone.utc)
    identity = HybridRepairIdentity(
        script.UUID(int=4),
        "adv-hybrid-test",
        HybridStartupState.REPAIR_READY,
        now - timedelta(minutes=5),
        now,
        now - timedelta(minutes=5),
        now,
        now,
        "MASSIVE_STOCKS_AGGREGATES_1M",
        "advanced-real-time-closed-bar-v1",
        timedelta(0),
        timedelta(seconds=10),
        2,
    )
    coordinator = script._HybridRepairCoordinator(
        SimpleNamespace(),
        SimpleNamespace(),
        SimpleNamespace(
            plan_id=script.UUID(int=3),
            cohort_id=script.UUID(int=2),
        ),
        session_open=now - timedelta(minutes=15),
        deadline=now + timedelta(minutes=30),
        journal_root=tmp_path,
        lease_seconds=330,
        settings=SimpleNamespace(),
        utc_clock=lambda: now,
    )
    coordinator.window = script._HybridRepairWindow(
        identity,
        "RUNNING",
        1,
        now,
        None,
        script._hybrid_report_path(
            tmp_path,
            identity.repair_id,
            1,
        ),
    )
    coordinator.instrument_ids = (1, 2)
    verify = MagicMock()
    monkeypatch.setattr(
        coordinator,
        "_durable_closure_complete",
        lambda: True,
    )
    monkeypatch.setattr(coordinator, "_verify_completed", verify)

    coordinator._handle_attempt_result(
        now,
        return_code=1,
        stderr="post-write wrapper failure",
    )

    verify.assert_called_once_with(now)
    assert coordinator.terminal_outcome is None
    assert not coordinator.window.report_path.exists()


def test_blocked_backfill_delays_only_before_live_warmup():
    warmup = datetime(2026, 10, 16, 13, 29, tzinfo=timezone.utc)
    blocked = SimpleNamespace(state="BACKFILL_BLOCKED", work=(object(),))

    assert script._backfill_delays_live(
        blocked,
        now=warmup - timedelta(seconds=1),
        warmup_at=warmup,
    )
    assert not script._backfill_delays_live(
        blocked,
        now=warmup,
        warmup_at=warmup,
    )


def test_safe_draining_journal_does_not_block_reconnect():
    draining = SimpleNamespace(
        registered=0,
        writing=0,
        draining=1,
        quarantined=0,
    )
    writing = SimpleNamespace(
        registered=0,
        writing=1,
        draining=0,
        quarantined=0,
    )

    assert not script._recovery_blocks_reconnect(draining)
    assert script._recovery_blocks_reconnect(writing)


def test_no_report_child_exit_uses_durable_state_and_exit_code():
    checked_at = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    durable = {
        "session_status": "FAILED",
        "failure_code": None,
        "started_at": checked_at - timedelta(seconds=5),
        "ended_at": checked_at,
        "journal_status": "DRAINING",
        "quarantine_reason": None,
    }

    failure = script._classify_child_result(
        return_code=9,
        intentional=False,
        report=None,
        durable=durable,
        stderr="child terminated before report",
        checked_at=checked_at,
    )

    assert failure.failure_code == "CHILD_EXIT_9"
    assert failure.disposition is IngressFailureDisposition.TRANSIENT
    assert failure.live_seconds == 5


def test_runtime_path_collision_allocates_a_new_path_attempt(tmp_path):
    plan_id = script.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    session_date = datetime(2026, 10, 16).date()
    _journal, report = script._derived_runtime_paths(
        tmp_path,
        plan_id=plan_id,
        session_date=session_date,
        connection_attempt=1,
    )
    report.touch()

    journal, replacement_report, path_attempt = (
        script._allocate_runtime_paths(
            tmp_path,
            plan_id=plan_id,
            session_date=session_date,
            connection_attempt=1,
        )
    )

    assert path_attempt == 2
    assert "-path-002" in journal.name
    assert replacement_report.name == journal.with_suffix(".json").name
