import importlib
import threading
from datetime import datetime, timedelta, timezone

import pytest


script = importlib.import_module(
    "scripts.run_realtime_v2_advanced_controller"
)


def _base(env_file):
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
        "--env-file",
        str(env_file),
        "--execute",
    ]


def test_controller_requires_explicit_execution_and_bounded_plan_budget(
    tmp_path,
):
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    base = _base(env_file)

    args = script.parse_args(base)

    assert args.authorization_id
    assert args.budgets.stream_sessions == 3
    assert args.budgets.connection_attempts == 3
    with pytest.raises(SystemExit):
        script.parse_args(base[:-1])


def test_child_command_has_one_plan_scope_and_no_retry_tuning(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    args = script.parse_args(_base(env_file))

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
