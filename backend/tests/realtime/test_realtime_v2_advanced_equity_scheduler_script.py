import importlib
import threading
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest


script = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_scheduler"
)
BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
    "--authorization-id",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "--cohort-id",
    "11111111-1111-1111-1111-111111111111",
    "--session-date",
    "2026-10-16",
    "--first-cutoff",
    "2026-10-16T13:35:00+00:00",
    "--finality-delay-seconds",
    "5",
    "--authorization-reference",
    "advanced-equity-v2-20261016",
    "--publication-mode",
    "projection-only",
    "--report-directory",
    "advanced-reports",
    "--continuous",
    "--execute",
]


def test_scheduler_builds_exact_remaining_session_windows(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    args = script.parse_args(BASE)

    assert len(args.windows) == 78
    assert args.windows[0].cutoff.minute == 35
    assert args.report_directory == (
        tmp_path / "advanced-reports"
    ).resolve()
    assert args.publication_mode == "projection-only"


def test_scheduler_rejects_non_boundary_and_unexecuted_modes():
    with pytest.raises(SystemExit):
        script.parse_args(BASE[:-1])
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE[:BASE.index("2026-10-16T13:35:00+00:00")],
            "2026-10-16T13:36:00+00:00",
            *BASE[BASE.index("2026-10-16T13:35:00+00:00") + 1:],
        ])


def test_scheduler_includes_last_authorized_cutoff_but_not_later_windows():
    started = datetime(2026, 10, 16, 13, 30, tzinfo=timezone.utc)
    ended = started + timedelta(minutes=10)
    windows = tuple(SimpleNamespace(cutoff=started + timedelta(minutes=minutes)) for minutes in (5, 10, 15))
    assert script._authorized_windows(
        windows, first_cutoff=windows[0].cutoff, started=started, ended=ended,
    ) == windows[:2]
    for cutoff in (started, ended + timedelta(minutes=5)):
        with pytest.raises(ValueError, match="outside"):
            script._authorized_windows(windows, first_cutoff=cutoff, started=started, ended=ended)


def test_scheduler_drains_final_cutoff_after_provider_window_ends(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    started = datetime(2026, 10, 16, 13, 30, 30, tzinfo=timezone.utc)
    ended = started + timedelta(minutes=10)
    cutoff = ended - timedelta(seconds=30)
    now = cutoff + timedelta(seconds=5)
    stop = threading.Event()
    plan_id = UUID(int=2)
    cohort_id = UUID("11111111-1111-1111-1111-111111111111")
    pool = MagicMock()

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(script, "datetime", Clock)
    monkeypatch.setattr(script.threading, "Event", lambda: stop)
    monkeypatch.setattr(script.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(script, "RealtimeV2PipelineConfig", SimpleNamespace(
        from_mapping=lambda *_args, **_kwargs: SimpleNamespace(
            advanced_ingress_policy_path=tmp_path, advanced_ingress_policy_version="test",
        ),
    ))
    settings = SimpleNamespace(
        drain_deadline_seconds=30, shutdown_deadline_seconds=30,
        scheduler_window_retry_initial_backoff_seconds=30,
        scheduler_window_retry_max_backoff_seconds=300, scheduler_window_retry_max_attempts=3,
    )
    monkeypatch.setattr(script, "load_advanced_ingress_runtime_catalog", lambda *_args, **_kwargs:
        SimpleNamespace(profile_for_sha=lambda _sha: SimpleNamespace(settings=settings)))
    provider = SimpleNamespace(transaction=lambda *_args, **_kwargs:
        nullcontext(SimpleNamespace(cursor=object())))
    monkeypatch.setattr(script, "_provider", lambda _config: (provider, pool))
    monkeypatch.setattr(script, "load_advanced_scheduler_scope_cursor", lambda *_args, **_kwargs: {
        "cohort_id": cohort_id, "plan_id": plan_id, "runtime_profile_sha256": "a" * 64,
        "authorized_start_at": started, "authorized_end_at": ended,
    })
    monkeypatch.setattr(script, "ensure_advanced_scheduler_session_cursor", lambda *_args, **_kwargs:
        SimpleNamespace(scheduler_session_id=plan_id))
    monkeypatch.setattr(script, "prioritize_live_over_blocked_backfill_cursor", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(script, "audit_completed_session_coverage_cursor", lambda *_args, **_kwargs:
        {"status": "COMPLETE"})
    transitions = []
    monkeypatch.setattr(script, "persist_advanced_scheduler_lifecycle_cursor",
        lambda *_args, **kwargs: transitions.append(kwargs))
    calls = []

    def recovery(_cursor, **kwargs):
        return SimpleNamespace(
            complete=kwargs["cutoff"] < cutoff or bool(calls),
            retry_not_before=started, next_attempt=0, input_receipt=None, portal_receipt=None,
        )

    monkeypatch.setattr(script, "discover_advanced_window_recovery_cursor", recovery)
    monkeypatch.setattr(script, "ensure_advanced_window_retry_cursor", lambda *_args, **_kwargs:
        SimpleNamespace(
            retry_id=plan_id, report_relative_path="last.json", attempt=0,
            reused_input_receipt=False, reused_portal_receipt=False,
        ))
    monkeypatch.setattr(script, "set_advanced_window_retry_status_cursor", lambda *_args, **_kwargs: None)

    def run_window(args):
        calls.append(args)
        stop.set()
        return 0

    monkeypatch.setattr(script, "run_window", run_window)

    assert script.main(BASE) == 0
    assert len(calls) == 1
    assert calls[0][calls[0].index("--cutoff") + 1] == cutoff.isoformat()
    assert any(row["status"] == "DRAINING" for row in transitions)
    pool.closeall.assert_called_once()
