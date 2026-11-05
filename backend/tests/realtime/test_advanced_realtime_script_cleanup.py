from types import SimpleNamespace

from scripts import run_advanced_options_v3_market_loop as market_loop
from scripts import run_advanced_realtime_session_scheduler as scheduler


def test_market_loop_cleanup_stops_only_owned_options_services(
    monkeypatch,
):
    environment = {"scope": "test"}
    calls = []

    def compose(actual_environment, *arguments, **kwargs):
        calls.append((actual_environment, arguments, kwargs))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(market_loop, "_compose", compose)
    monkeypatch.setattr(
        market_loop,
        "_docker_snapshot",
        lambda _environment: {},
    )

    cleanup = market_loop._cleanup(environment, {})

    assert calls == [
        (
            environment,
            (
                "stop",
                "--timeout",
                "60",
                *market_loop.OPTIONS_SERVICES,
            ),
            {"timeout": 120, "check": False},
        ),
        (
            environment,
            ("rm", "--force", *market_loop.OPTIONS_SERVICES),
            {"timeout": 120, "check": False},
        ),
    ]
    assert cleanup["stop"] == 0
    assert cleanup["remove"] == 0
    assert cleanup["run_owned_containers"] == []


def test_scheduler_interrupt_report_uses_unique_path(monkeypatch, tmp_path):
    monkeypatch.setattr(scheduler.time, "time_ns", lambda: 123456789)

    interrupted = scheduler._cleanup_report_path(
        tmp_path,
        {"status": "PRESERVED_FOR_RESUME"},
    )
    completed = scheduler._cleanup_report_path(
        tmp_path,
        {"status": "CLEAN"},
    )

    assert interrupted == (
        tmp_path / "COORDINATOR-INTERRUPTED-123456789.json"
    )
    assert completed == tmp_path / "CLEANUP.json"
