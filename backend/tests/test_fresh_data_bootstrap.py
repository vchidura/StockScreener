import sys
from pathlib import Path
from unittest.mock import patch

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import bootstrap_fresh_data as command


def args(*extra: str):
    return command.parser().parse_args([
        "--confirm-database-name", "stocks_db_fresh",
        "--history-start", "2021-09-01",
        "--intraday-start", "2026-06-01",
        "--end", "2026-09-01",
        *extra,
    ])


def test_bootstrap_requires_exact_database_confirmation(monkeypatch):
    monkeypatch.setenv("DB_NAME", "stocks_db_fresh")
    candidate = args("--phase", "schema")
    candidate.confirm_database_name = "stocks_db"

    with pytest.raises(RuntimeError, match="exactly equal"):
        command.validate_args(candidate)


def test_all_phase_builds_ordered_portal_bootstrap_without_research(monkeypatch):
    monkeypatch.setenv("DB_NAME", "stocks_db_fresh")
    candidate = args("--phase", "all", "--include-fundamentals")
    command.validate_args(candidate)
    commands = command.build_commands(candidate)
    phases = [phase for phase, _ in commands]

    assert phases[0:3] == ["schema", "universe", "reference"]
    assert phases[-3:] == ["snapshots", "validate", "validate"]
    assert "research-inputs" not in phases
    assert sum(phase == "native-bars" for phase in phases) == 4
    assert sum(phase == "derive" for phase in phases) == 4
    assert sum(phase == "reference" for phase in phases) == 2
    assert phases.index("publish") < phases.index("current-signals")
    assert phases.index("current-signals") < phases.index("snapshots")
    assert sum(phase == "current-signals" for phase in phases) == 2


def test_all_phase_includes_optional_research_before_validation(monkeypatch):
    monkeypatch.setenv("DB_NAME", "stocks_db_fresh")
    candidate = args(
        "--phase", "all", "--include-research-inputs",
        "--research-sessions", "400",
    )
    command.validate_args(candidate)
    commands = command.build_commands(candidate)
    phases = [phase for phase, _ in commands]
    research_index = phases.index("research-inputs")

    assert phases[research_index + 1] == "validate"
    assert "--backfill-actions" in commands[research_index][1]
    assert "--backfill-bars" in commands[research_index][1]
    assert "400" in commands[research_index][1]


def test_plan_mode_never_executes_subprocess(monkeypatch, capsys):
    monkeypatch.setenv("DB_NAME", "stocks_db_fresh")
    with (
        patch.object(sys, "argv", [
            "bootstrap_fresh_data.py",
            "--confirm-database-name", "stocks_db_fresh",
            "--history-start", "2021-09-01",
            "--intraday-start", "2026-06-01",
            "--end", "2026-09-01",
            "--phase", "schema",
        ]),
        patch.object(command.subprocess, "run") as run,
    ):
        assert command.main() == 0

    run.assert_not_called()
    assert "No commands executed" in capsys.readouterr().out
