import importlib
from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from options.calendar import OptionExchangeCalendar
from realtime.advanced_equity_lifecycle import AdvancedEquityLifecyclePhase
from realtime.equity_bar_adapter import MAXIMUM_HISTORY_BINDING_DEPTH
from realtime.equity_bar_repository import load_current_bar_groups_cursor


scheduler = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_scheduler"
)
UTC = timezone.utc
CALENDAR = OptionExchangeCalendar("XNYS")
SCOPE = {
    "plan_id": UUID(int=1),
    "cohort_id": UUID(int=2),
    "authorization_id": UUID(int=3),
    "authorization_reference": "approved",
    "runtime_profile_sha256": "a" * 64,
}
SESSION = date(2026, 11, 2)
TARGET = date(2026, 11, 3)
LATE_EVENING = datetime(2026, 11, 2, 22, 0, tzinfo=UTC)


def _provider():
    return SimpleNamespace(
        transaction=lambda *_args, **_kwargs: nullcontext(
            SimpleNamespace(cursor=MagicMock())
        )
    )


def _args(tmp_path):
    return SimpleNamespace(
        database="stocks_realtime_v2",
        confirm_database_name="stocks_realtime_v2",
        finality_delay_seconds=5,
        completion_budget_seconds=300,
        authorization_reference=None,
        report_directory=tmp_path,
        publication_mode=scheduler.CONTINUOUS_PUBLICATION,
    )


def _catalog(attempts=3):
    profile = SimpleNamespace(
        settings=SimpleNamespace(scheduler_window_retry_max_attempts=attempts)
    )
    return SimpleNamespace(profile_for_sha=lambda _sha: profile)


def _lifecycle(phase=AdvancedEquityLifecyclePhase.IDLE):
    return SimpleNamespace(phase=phase, target_session_date=TARGET)


def _recover(monkeypatch, tmp_path, rows, *, now=LATE_EVENING, state=None, **kwargs):
    monkeypatch.setattr(
        scheduler,
        "load_continuous_advanced_scheduler_scope_cursor",
        lambda *_args, **_kwargs: SCOPE,
    )
    monkeypatch.setattr(
        scheduler,
        "plan_session_windows",
        lambda *_args, **_kwargs: rows,
    )
    monkeypatch.setattr(
        scheduler,
        "discover_advanced_window_recovery_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(
            next_attempt=4,
            input_receipt=None,
            portal_receipt=None,
        ),
    )
    runner = MagicMock(return_value=0)
    state = {} if state is None else state
    ran = scheduler._recover_completed_session_window(
        _provider(),
        _args(tmp_path),
        _catalog(),
        CALENDAR,
        lifecycle=kwargs.get("lifecycle", _lifecycle()),
        now=now,
        state=state,
        window_runner=runner,
    )
    return ran, runner, state


def _row(cutoff, *, ready=True, attempt=4):
    return {
        "cutoff": cutoff,
        "next_attempt": attempt,
        "latest_state": "FAILED",
        "missing_minute_coordinates": 0,
        "ready": ready,
        "waiting_for": None if ready else "ADJUSTED_DAILY_BAR",
    }


def test_closing_window_runs_once_its_inputs_exist(monkeypatch, tmp_path):
    cutoff = datetime(2026, 11, 2, 21, 0, tzinfo=UTC)

    ran, runner, state = _recover(monkeypatch, tmp_path, [_row(cutoff)])

    assert ran is True
    arguments = runner.call_args.args[0]
    assert arguments[arguments.index("--cutoff") + 1] == cutoff.isoformat()
    assert "--window-key-suffix=-retry4" in arguments
    assert "--retry-release-now" in arguments
    assert "--continuous-publication" in arguments
    # B20 (Nov 6): one attempt ran before the missed 5m bars were derived and nothing tried again. The session is
    # finished only when the close is complete or the attempts are spent; retries are spaced.
    assert state["next_check_at"] == LATE_EVENING + scheduler.COMPLETED_SESSION_RETRY_SPACING
    assert state.get("finished") != SESSION

    ran_again = scheduler._recover_completed_session_window(
        _provider(),
        _args(tmp_path),
        _catalog(),
        CALENDAR,
        lifecycle=_lifecycle(),
        now=LATE_EVENING + timedelta(minutes=1),
        state=state,
        window_runner=runner,
    )
    assert ran_again is False
    runner.assert_called_once()


def test_a_failed_recovery_is_tried_again_after_the_spacing(monkeypatch, tmp_path):
    cutoff = datetime(2026, 11, 2, 21, 0, tzinfo=UTC)
    ran, runner, state = _recover(monkeypatch, tmp_path, [_row(cutoff)])
    assert ran is True
    monkeypatch.setattr(scheduler, "plan_session_windows", lambda *_a, **_k: [_row(cutoff, attempt=5)])
    monkeypatch.setattr(
        scheduler, "discover_advanced_window_recovery_cursor",
        lambda *_a, **_k: SimpleNamespace(next_attempt=5, input_receipt=None, portal_receipt=None),
    )
    again = scheduler._recover_completed_session_window(
        _provider(), _args(tmp_path), _catalog(), CALENDAR, lifecycle=_lifecycle(),
        now=LATE_EVENING + scheduler.COMPLETED_SESSION_RETRY_SPACING, state=state, window_runner=runner,
    )
    assert again is True and runner.call_count == 2
    assert "--window-key-suffix=-retry5" in runner.call_args.args[0]
    monkeypatch.setattr(scheduler, "plan_session_windows", lambda *_a, **_k: [])
    done = scheduler._recover_completed_session_window(
        _provider(), _args(tmp_path), _catalog(), CALENDAR, lifecycle=_lifecycle(),
        now=LATE_EVENING + 2 * scheduler.COMPLETED_SESSION_RETRY_SPACING, state=state, window_runner=runner,
    )
    assert done is False and state["finished"] == SESSION


def test_waiting_window_does_not_spend_an_attempt(monkeypatch, tmp_path):
    ran, runner, state = _recover(
        monkeypatch,
        tmp_path,
        [_row(datetime(2026, 11, 2, 21, 0, tzinfo=UTC), ready=False)],
    )

    assert ran is False
    runner.assert_not_called()
    assert state["next_check_at"] == LATE_EVENING + timedelta(seconds=60)


def test_attempts_stay_bounded(monkeypatch, tmp_path):
    ran, runner, _state = _recover(
        monkeypatch,
        tmp_path,
        [_row(datetime(2026, 11, 2, 21, 0, tzinfo=UTC), attempt=8)],
    )

    assert ran is False
    runner.assert_not_called()
    assert _state["finished"] == SESSION


def test_recovery_waits_until_ordinary_retries_are_exhausted(
    monkeypatch,
    tmp_path,
):
    ran, runner, _state = _recover(
        monkeypatch,
        tmp_path,
        [_row(
            datetime(2026, 11, 2, 21, 0, tzinfo=UTC),
            attempt=3,
        )],
    )

    assert ran is False
    runner.assert_not_called()


def test_only_the_completed_sessions_final_window_is_recovered(
    monkeypatch,
    tmp_path,
):
    ran, runner, _state = _recover(
        monkeypatch,
        tmp_path,
        [
            _row(datetime(2026, 11, 2, 20, 55, tzinfo=UTC)),
            _row(
                datetime(2026, 11, 2, 21, 0, tzinfo=UTC),
                ready=False,
            ),
        ],
    )

    assert ran is False
    runner.assert_not_called()


def test_a_complete_session_is_not_replanned(monkeypatch, tmp_path):
    ran, runner, state = _recover(monkeypatch, tmp_path, [])
    assert (ran, state["finished"]) == (False, SESSION)
    runner.assert_not_called()

    planner = MagicMock(side_effect=AssertionError("replanned"))
    monkeypatch.setattr(scheduler, "plan_session_windows", planner)
    assert scheduler._recover_completed_session_window(
        _provider(),
        _args(tmp_path),
        _catalog(),
        CALENDAR,
        lifecycle=_lifecycle(),
        now=LATE_EVENING + timedelta(hours=1),
        state=state,
    ) is False


@pytest.mark.parametrize(
    ("phase", "now"),
    (
        # Live work always wins.
        (AdvancedEquityLifecyclePhase.DRAINING, LATE_EVENING),
        # Too close to the next activation to start a window.
        (
            AdvancedEquityLifecyclePhase.BEFORE_OPEN,
            datetime(2026, 11, 3, 14, 5, 1, tzinfo=UTC),
        ),
    ),
)
def test_recovery_never_competes_with_the_next_session(
    monkeypatch,
    tmp_path,
    phase,
    now,
):
    ran, runner, state = _recover(
        monkeypatch,
        tmp_path,
        [_row(datetime(2026, 11, 2, 21, 0, tzinfo=UTC))],
        now=now,
        lifecycle=_lifecycle(phase),
    )

    assert ran is False
    runner.assert_not_called()
    assert state == {}


def test_history_chain_limit_allows_one_cohort_per_session_for_months():
    assert MAXIMUM_HISTORY_BINDING_DEPTH >= 60


def _scopes(count):
    before = datetime(2026, 11, 3, 14, 30, tzinfo=UTC)
    return tuple(
        (UUID(int=index + 10), "MASSIVE_ADVANCED", before)
        for index in range(count)
    )


def test_bar_groups_read_a_chain_past_the_old_four_session_limit():
    # 2026-11-05 would have been the fifth binding.
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    load_current_bar_groups_cursor(
        cursor,
        cohort_id=UUID(int=1),
        provider_tier="MASSIVE_ADVANCED",
        instrument_id=7,
        security_id=UUID(int=8),
        ticker="SPY",
        groups={"1d:ADJUSTED": (
            datetime(2026, 10, 1, tzinfo=UTC),
            datetime(2026, 11, 3, tzinfo=UTC),
        )},
        history_source_scopes=_scopes(5),
    )

    assert cursor.execute.called


def test_bar_groups_still_bound_the_chain():
    with pytest.raises(ValueError, match="depth is too large"):
        load_current_bar_groups_cursor(
            MagicMock(),
            cohort_id=UUID(int=1),
            provider_tier="MASSIVE_ADVANCED",
            instrument_id=7,
            security_id=UUID(int=8),
            ticker="SPY",
            groups={"1d:ADJUSTED": (
                datetime(2026, 10, 1, tzinfo=UTC),
                datetime(2026, 11, 3, tzinfo=UTC),
            )},
            history_source_scopes=_scopes(
                MAXIMUM_HISTORY_BINDING_DEPTH + 1
            ),
        )


def test_the_close_waits_for_every_five_minute_bar(monkeypatch):
    """B20: the raw daily bar is derived from the session's 5m bars; on Nov 6 the close read ready with a 5m hole."""
    from contextlib import contextmanager

    from realtime import advanced_session_recovery as recovery
    session = date(2026, 11, 6)
    windows = recovery.advanced_session_windows(
        session, finality_delay_seconds=recovery.ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
        completion_budget_seconds=recovery.ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS, serving_lag_seconds=30)
    final = windows[-1].cutoff
    hole = windows[2].cutoff

    class _Cursor:
        def execute(self, *_a):
            pass

        def fetchone(self):
            return (2,)  # both members have their adjusted daily bar

    class _Provider:
        @contextmanager
        def transaction(self, *_a, **_k):
            yield SimpleNamespace(cursor=_Cursor())

    monkeypatch.setattr(recovery, "load_stream_gap_repair_scope_cursor", lambda *_a: SimpleNamespace(
        members=(SimpleNamespace(instrument_id=1), SimpleNamespace(instrument_id=2))))
    monkeypatch.setattr(recovery, "discover_advanced_window_recovery_cursor", lambda *_a, cutoff, **_k: SimpleNamespace(
        complete=cutoff != final, next_attempt=4, latest_state="FAILED"))
    monkeypatch.setattr(recovery, "load_stored_minutes_cursor", lambda *_a, **_k: tuple(range(10)))
    bars = {1: [{"bar_end": window.cutoff} for window in windows],
            2: [{"bar_end": window.cutoff} for window in windows if window.cutoff != hole]}
    monkeypatch.setattr(recovery, "load_bar_range_cursor", lambda *_a, **_k: bars)
    scope = {"plan_id": UUID(int=1), "cohort_id": UUID(int=2)}
    (row,) = recovery.plan_session_windows(_Provider(), scope, session, checked_at=final + timedelta(hours=2))
    assert row["cutoff"] == final and row["ready"] is False and row["waiting_for"] == "MISSING_5M_BARS"
    bars[2].append({"bar_end": hole})
    (row,) = recovery.plan_session_windows(_Provider(), scope, session, checked_at=final + timedelta(hours=2))
    assert row["ready"] is True and row["waiting_for"] is None
