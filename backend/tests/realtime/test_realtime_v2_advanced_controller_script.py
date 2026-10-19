import importlib
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from realtime.advanced_ingress_controller import (
    HybridRepairIdentity,
    HybridStartupState,
    IngressFailureDisposition,
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
        "conservative-unverified-delay-v1",
        timedelta(minutes=15),
    )

    command = script._incremental_repair_command(
        args,
        runtime,
        identity,
        tmp_path / "repair.json",
        SimpleNamespace(
            rest_provider_delay_seconds=900,
            rest_aggregate_endpoint_class=(
                "MASSIVE_STOCKS_AGGREGATES_1M"
            ),
            rest_eligibility_policy_version=(
                "conservative-unverified-delay-v1"
            ),
        ),
    )

    assert "run_realtime_v2_incremental_equity.py" in " ".join(command)
    assert command[command.index("--cutoff") + 1].endswith("13:40:00+00:00")
    assert "--advanced-ingress-plan-id" in command
    assert command.count("--once") == 1


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
