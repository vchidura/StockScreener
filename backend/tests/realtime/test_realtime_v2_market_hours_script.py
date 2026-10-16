from datetime import date, datetime, timezone

import pytest

from scripts import start_realtime_v2_market_hours as script


def test_regular_session_derives_full_day_scope():
    scope = script.market_day_scope(date(2026, 10, 16))

    assert scope is not None
    assert scope.first_cutoff == datetime(
        2026, 10, 16, 13, 45, tzinfo=timezone.utc
    )
    assert scope.window_count == 26
    assert scope.daily_request_ceiling == 468


def test_early_close_derives_shorter_exact_ceiling():
    scope = script.market_day_scope(date(2026, 11, 27))

    assert scope is not None
    assert scope.first_cutoff == datetime(
        2026, 11, 27, 14, 45, tzinfo=timezone.utc
    )
    assert scope.window_count == 14
    assert scope.daily_request_ceiling == 252


def test_non_session_is_a_clean_no_work_result(monkeypatch, capsys):
    delegated = []
    monkeypatch.setattr(script, "run_services", lambda command: delegated.append(command))

    assert script.main(["--session-date", "2026-10-17", "--execute"]) == 0

    assert not delegated
    report = capsys.readouterr().out
    assert "SKIPPED_NOT_XNYS_SESSION" in report
    assert '"provider_requests": 0' in report


def test_market_day_delegates_derived_continuous_scope(monkeypatch):
    delegated = []
    monkeypatch.setattr(
        script,
        "run_services",
        lambda command: delegated.append(command) or 0,
    )

    assert script.main(["--session-date", "2026-10-16", "--execute"]) == 0

    command = delegated[0]
    assert command[command.index("--first-cutoff") + 1] == (
        "2026-10-16T13:45:00+00:00"
    )
    assert command[command.index("--daily-request-ceiling") + 1] == "468"
    assert "--continuous-ingestion" in command
    assert "--execute" in command
    assert (
        command[command.index("--authorization-reference") + 1]
        == script.STANDING_AUTHORIZATION_REFERENCE
    )


def test_execute_rejects_blank_standing_authorization():
    with pytest.raises(SystemExit):
        script.parse_args([
            "--session-date",
            "2026-10-16",
            "--authorization-reference",
            " ",
            "--execute",
        ])
