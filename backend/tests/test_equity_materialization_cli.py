import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import run_equity_materialization as command


def test_status_only_does_not_require_worker_leadership():
    args = command.parser().parse_args(["--status"])

    assert command.requires_leadership(args) is False


def test_coverage_report_does_not_require_worker_leadership():
    args = command.parser().parse_args(["--coverage-report", "--interval", "30m"])

    assert command.requires_leadership(args) is False


@pytest.mark.parametrize(
    "operation",
    ("--apply-migration", "--recover-stale-runs", "--bars", "--analyze", "--once"),
)
def test_mutating_operations_require_worker_leadership(operation):
    args = command.parser().parse_args([operation])

    assert command.requires_leadership(args) is True


def test_main_rejects_mutation_when_worker_owns_leadership():
    @contextmanager
    def not_leader(_name):
        yield False

    args = command.parser().parse_args(["--bars"])
    with (
        patch.object(command, "parser") as parser_mock,
        patch.object(command, "try_advisory_leadership", not_leader),
        patch.object(command, "run") as run_mock,
    ):
        parser_mock.return_value.parse_args.return_value = args
        with pytest.raises(RuntimeError, match="mutating operation requires leadership"):
            command.main()

    run_mock.assert_not_called()


def test_shadow_requires_analyze_and_cannot_mix_with_replay():
    with patch.object(sys, "argv", ["run_equity_materialization.py", "--shadow"]):
        with pytest.raises(SystemExit):
            command.main()
    with patch.object(
        sys, "argv",
        ["run_equity_materialization.py", "--shadow", "--replay", "--analyze"],
    ):
        with pytest.raises(SystemExit):
            command.main()