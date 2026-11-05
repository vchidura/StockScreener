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
    "--runtime-mode",
    "legacy-diagnostic",
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


def test_continuous_scheduler_needs_no_daily_identity_arguments(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    args = script.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--continuous",
        "--execute",
    ])

    assert args.runtime_mode == "continuous"
    assert args.authorization_id is None
    assert args.cohort_id is None
    assert args.session_date is None
    assert args.first_cutoff is None
    assert args.authorization_reference is None
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


def test_continuous_scheduler_does_not_rescan_durable_completed_prefix():
    session_date = datetime(2026, 11, 2).date()
    windows = script.advanced_session_windows(
        session_date,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
        serving_lag_seconds=30,
    )
    planner = script.AdvancedEquityLifecyclePlanner(
        clock=lambda: datetime(2026, 11, 2, 17, 0, tzinfo=timezone.utc)
    )

    first = planner.plan(
        drain_seconds=30,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
        last_completed_cutoff=windows[29].cutoff,
    )
    second = planner.plan(
        drain_seconds=30,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
        last_completed_cutoff=windows[29].cutoff,
    )
    remaining = script._authorized_windows(
        windows,
        first_cutoff=first.first_due_cutoff,
        started=windows[0].cutoff - timedelta(minutes=5),
        ended=windows[-1].cutoff,
    )

    assert first.first_due_cutoff == windows[30].cutoff
    assert second.first_due_cutoff == windows[30].cutoff
    assert remaining == windows[30:]


def test_progress_check_due_prefers_retry_then_reconciliation_expiry():
    now = datetime(2026, 11, 2, 17, 0, tzinfo=timezone.utc)
    future = SimpleNamespace(
        cutoff=now + timedelta(minutes=5),
        available_after=now + timedelta(minutes=5, seconds=35),
    )

    assert script._next_progress_check(
        (future,),
        first_cutoff=future.cutoff,
        now=now,
        retry_not_before=now + timedelta(seconds=30),
        reconciliation_expires_at=now + timedelta(seconds=60),
    ) == now + timedelta(seconds=30)
    assert script._next_progress_check(
        (future,),
        first_cutoff=future.cutoff,
        now=now,
        retry_not_before=None,
        reconciliation_expires_at=now + timedelta(seconds=60),
    ) == now + timedelta(seconds=60)


def test_retry_budget_stops_before_next_live_slot_and_drain():
    now = datetime(2026, 11, 2, 17, 2, tzinfo=timezone.utc)
    args = SimpleNamespace(completion_budget_seconds=300)
    windows = (
        SimpleNamespace(available_after=now - timedelta(minutes=2)),
        SimpleNamespace(available_after=now + timedelta(minutes=3)),
        SimpleNamespace(available_after=now + timedelta(minutes=8)),
    )

    budget, cap = script._retry_completion_budget(
        args,
        windows,
        attempt=2,
        now=now,
        drain_deadline=now + timedelta(minutes=10),
    )
    assert (budget, cap) == (180, now + timedelta(minutes=3))

    budget, cap = script._retry_completion_budget(
        args,
        windows,
        attempt=2,
        now=now,
        drain_deadline=now + timedelta(seconds=75),
    )
    assert (budget, cap) == (75, now + timedelta(seconds=75))


def test_current_window_keeps_its_budget_while_retry_is_deferred_at_slot():
    now = datetime(2026, 11, 2, 17, 5, tzinfo=timezone.utc)
    args = SimpleNamespace(completion_budget_seconds=300)
    windows = (
        SimpleNamespace(available_after=now),
        SimpleNamespace(available_after=now + timedelta(minutes=5)),
    )

    assert script._retry_completion_budget(
        args,
        windows,
        attempt=0,
        now=now,
        drain_deadline=now + timedelta(minutes=10),
    )[0] == 300
    assert script._retry_completion_budget(
        args,
        windows,
        attempt=1,
        now=now + timedelta(minutes=4, seconds=59, milliseconds=500),
        drain_deadline=now + timedelta(minutes=10),
    )[0] == 0


@pytest.mark.parametrize(
    "completed_windows",
    (0, 30),
)
def test_continuous_progress_cache_waits_for_next_cutoff_availability(
    tmp_path,
    monkeypatch,
    completed_windows,
):
    monkeypatch.chdir(tmp_path)
    now = [
        datetime(2026, 11, 2, 14, 0, tzinfo=timezone.utc)
        if completed_windows == 0
        else datetime(2026, 11, 2, 17, 0, tzinfo=timezone.utc)
    ]
    windows = script.advanced_session_windows(
        now[0].date(),
        finality_delay_seconds=5,
        completion_budget_seconds=300,
        serving_lag_seconds=30,
    )
    last_completed = (
        None
        if completed_windows == 0
        else windows[completed_windows - 1].cutoff
    )

    class Stop:
        waits = 0

        def is_set(self):
            return self.waits >= 4

        def wait(self, _seconds):
            now[0] += timedelta(seconds=1)
            self.waits += 1
            return self.is_set()

    stop = Stop()
    monkeypatch.setattr(script.threading, "Event", lambda: stop)
    monkeypatch.setattr(script.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(
        script,
        "RealtimeV2PipelineConfig",
        SimpleNamespace(
            from_mapping=lambda *_args, **_kwargs: SimpleNamespace(
                advanced_ingress_policy_path=tmp_path,
                advanced_ingress_policy_version="test",
            ),
        ),
    )
    settings = SimpleNamespace(
        drain_deadline_seconds=30,
        shutdown_deadline_seconds=30,
        scheduler_window_retry_initial_backoff_seconds=30,
        scheduler_window_retry_max_backoff_seconds=300,
        scheduler_window_retry_max_attempts=3,
    )
    profile = SimpleNamespace(
        sha256="a" * 64,
        settings=settings,
    )
    monkeypatch.setattr(
        script,
        "load_advanced_ingress_runtime_catalog",
        lambda *_args, **_kwargs: SimpleNamespace(
            profile_for_target=lambda _target: profile,
            profile_for_sha=lambda _sha: profile,
        ),
    )
    provider = SimpleNamespace(
        transaction=lambda *_args, **_kwargs: nullcontext(
            SimpleNamespace(cursor=object())
        )
    )
    pool = MagicMock()
    monkeypatch.setattr(script, "_provider", lambda _config: (provider, pool))
    scope = {
        "authorization_id": UUID(int=1),
        "authorization_reference": "continuous-test",
        "cohort_id": UUID(int=2),
        "plan_id": UUID(int=3),
        "runtime_profile_sha256": profile.sha256,
        "authorized_start_at": windows[0].cutoff - timedelta(minutes=5),
        "authorized_end_at": windows[-1].cutoff,
    }
    scope_queries = []
    monkeypatch.setattr(
        script,
        "load_continuous_advanced_scheduler_scope_cursor",
        lambda *_args, **_kwargs: scope_queries.append(now[0]) or scope,
    )
    progress_queries = []
    monkeypatch.setattr(
        script,
        "load_advanced_scheduler_progress_cursor",
        lambda *_args, **_kwargs: (
            progress_queries.append(now[0])
            or SimpleNamespace(last_completed_cutoff=last_completed)
        ),
    )
    monkeypatch.setattr(
        script,
        "ensure_advanced_scheduler_session_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(
            scheduler_session_id=UUID(int=4)
        ),
    )
    monkeypatch.setattr(
        script,
        "prioritize_live_over_blocked_backfill_cursor",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        script,
        "audit_completed_session_coverage_cursor",
        lambda *_args, **_kwargs: {"status": "COMPLETE"},
    )
    monkeypatch.setattr(
        script,
        "_recover_completed_session_window",
        lambda *_args, **_kwargs: False,
    )

    assert script.main([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--poll-seconds",
        "1",
        "--scope-reconcile-seconds",
        "60",
        "--report-directory",
        str(tmp_path / "reports"),
        "--continuous",
        "--execute",
    ], clock=lambda: now[0]) == 0
    assert len(scope_queries) == 1
    assert len(progress_queries) == 1
    pool.closeall.assert_called_once()


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
    monkeypatch.setattr(
        script,
        "load_advanced_scheduler_progress_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(
            last_completed_cutoff=None,
        ),
    )
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


def test_a_past_session_that_cannot_be_read_does_not_stop_the_scheduler(monkeypatch, capsys):
    now = datetime(2026, 11, 5, 11, 45, tzinfo=timezone.utc)
    scope = dict(cohort_id=UUID(int=1), plan_id=UUID(int=2), runtime_profile_sha256="a" * 64)
    calendar = SimpleNamespace(
        session_open=lambda _session: datetime(2026, 11, 5, 14, 30, tzinfo=timezone.utc),
        previous_session_date=lambda _session: datetime(2026, 11, 4).date(),
    )
    provider = SimpleNamespace(transaction=lambda *_args, **_kwargs: nullcontext(SimpleNamespace(cursor=MagicMock())))
    monkeypatch.setattr(script, "load_continuous_advanced_scheduler_scope_cursor", lambda *_args, **_kwargs: scope)

    def unreadable(*_args, **_kwargs):
        raise ValueError("durable Advanced INPUT_READY lacks its visibility seal")

    monkeypatch.setattr(script, "plan_session_windows", unreadable)
    state = {}

    recovered = script._recover_completed_session_window(
        provider,
        SimpleNamespace(),
        SimpleNamespace(),
        calendar,
        lifecycle=SimpleNamespace(
            phase=script.AdvancedEquityLifecyclePhase.BEFORE_OPEN,
            target_session_date=datetime(2026, 11, 5).date(),
        ),
        now=now,
        state=state,
    )

    assert recovered is False
    assert state["finished"] == datetime(2026, 11, 4).date()
    assert "visibility seal" in capsys.readouterr().err
