import importlib
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from options.calendar import OptionExchangeCalendar
from realtime.advanced_session_schedule import session_wake_window


wrapper = importlib.import_module("scripts.run_realtime_v2_container")
probe = importlib.import_module("scripts.check_realtime_v2_role_sleep")
UTC = timezone.utc
CALENDAR = OptionExchangeCalendar("XNYS")


@pytest.mark.parametrize(
    ("checked_at", "wake_at", "sleep_at"),
    (
        # Overnight before a normal session.
        (
            datetime(2026, 11, 3, 3, 0, tzinfo=UTC),
            datetime(2026, 11, 3, 14, 10, tzinfo=UTC),
            datetime(2026, 11, 3, 21, 45, tzinfo=UTC),
        ),
        # During the session.
        (
            datetime(2026, 11, 3, 16, 0, tzinfo=UTC),
            datetime(2026, 11, 3, 14, 10, tzinfo=UTC),
            datetime(2026, 11, 3, 21, 45, tzinfo=UTC),
        ),
        # Just after sleeping, the next session is the target.
        (
            datetime(2026, 11, 3, 21, 45, tzinfo=UTC),
            datetime(2026, 11, 4, 14, 10, tzinfo=UTC),
            datetime(2026, 11, 4, 21, 45, tzinfo=UTC),
        ),
        # Across a weekend.
        (
            datetime(2026, 11, 7, 12, 0, tzinfo=UTC),
            datetime(2026, 11, 9, 14, 10, tzinfo=UTC),
            datetime(2026, 11, 9, 21, 45, tzinfo=UTC),
        ),
        # Thanksgiving is skipped.
        (
            datetime(2026, 11, 25, 22, 0, tzinfo=UTC),
            datetime(2026, 11, 27, 14, 10, tzinfo=UTC),
            datetime(2026, 11, 27, 18, 45, tzinfo=UTC),
        ),
        # Before the DST change, the open is an hour earlier in UTC.
        (
            datetime(2026, 10, 30, 3, 0, tzinfo=UTC),
            datetime(2026, 10, 30, 13, 10, tzinfo=UTC),
            datetime(2026, 10, 30, 20, 45, tzinfo=UTC),
        ),
    ),
)
def test_wake_window_follows_the_exchange_calendar(
    checked_at,
    wake_at,
    sleep_at,
):
    assert session_wake_window(checked_at, CALENDAR) == (wake_at, sleep_at)


def test_wake_window_rejects_a_naive_clock():
    with pytest.raises(ValueError, match="timezone-aware"):
        session_wake_window(datetime(2026, 11, 3, 3, 0), CALENDAR)


def test_wake_precedes_the_activation_boundary():
    wake_at, _sleep_at = session_wake_window(
        datetime(2026, 11, 3, 3, 0, tzinfo=UTC),
        CALENDAR,
    )
    assert wake_at < CALENDAR.session_open(date(2026, 11, 3)) - timedelta(
        minutes=15
    )


def test_off_hours_start_sleeps_without_running_the_role(tmp_path):
    state = tmp_path / "sleep.json"
    now = datetime(2026, 11, 3, 3, 0, tzinfo=UTC)
    run_child_window = MagicMock(return_value=0)
    observed = []

    def wait(seconds):
        observed.append((seconds, probe.role_is_sleeping(state)))
        clock.append(datetime(2026, 11, 3, 14, 10, tzinfo=UTC))

    clock = [now]

    assert wrapper.run_session_bound_child(
        "bridge",
        ["python", "role"],
        {},
        utc_clock=lambda: clock[-1],
        calendar=CALENDAR,
        state_path=state,
        wait=wait,
        run_child_window=run_child_window,
    ) == 0

    assert observed == [(wrapper.SLEEP_POLL_SECONDS, True)]
    run_child_window.assert_called_once()
    assert run_child_window.call_args.kwargs["sleep_at"] == datetime(
        2026, 11, 3, 21, 45, tzinfo=UTC,
    )
    assert not state.exists()


def test_start_inside_the_window_runs_immediately(tmp_path):
    state = tmp_path / "sleep.json"
    wait = MagicMock(side_effect=AssertionError("must not sleep"))
    run_child_window = MagicMock(return_value=3)

    assert wrapper.run_session_bound_child(
        "ingress",
        ["python", "role"],
        {},
        utc_clock=lambda: datetime(2026, 11, 3, 16, 0, tzinfo=UTC),
        calendar=CALENDAR,
        state_path=state,
        wait=wait,
        run_child_window=run_child_window,
    ) == 3


def test_role_that_stops_for_sleep_returns_to_waiting(tmp_path):
    state = tmp_path / "sleep.json"
    clock = [datetime(2026, 11, 3, 21, 44, tzinfo=UTC)]
    results = [None, 0]

    def run_child_window(*_args, **_kwargs):
        clock.append(datetime(2026, 11, 3, 21, 46, tzinfo=UTC))
        return results.pop(0)

    def wait(_seconds):
        assert probe.role_is_sleeping(state)
        clock.append(datetime(2026, 11, 4, 14, 10, tzinfo=UTC))

    assert wrapper.run_session_bound_child(
        "bridge",
        ["python", "role"],
        {},
        utc_clock=lambda: clock[-1],
        calendar=CALENDAR,
        state_path=state,
        wait=wait,
        run_child_window=run_child_window,
    ) == 0
    assert results == []


def test_sleep_covers_only_roles_without_off_hours_work():
    assert wrapper.SESSION_BOUND_COMPONENTS <= set(
        wrapper.FENCED_ROLE_NAMES
    )
    for component in (
        "controller",
        "scheduler",
        "options-lifecycle",
        "preparation",
        "capability-refresh",
        "publication-retry",
    ):
        assert component not in wrapper.SESSION_BOUND_COMPONENTS


def test_sleeping_probe_reads_only_a_fresh_state_file(tmp_path):
    state = tmp_path / "sleep.json"
    assert not probe.role_is_sleeping(state)
    wrapper.write_sleep_state(
        state,
        wake_at=datetime(2026, 11, 3, 14, 10, tzinfo=UTC),
    )
    assert probe.role_is_sleeping(state)
    # A wrapper that died must not keep reporting healthy.
    stale = state.stat().st_mtime + probe.MAXIMUM_STATE_AGE_SECONDS + 1
    assert not probe.role_is_sleeping(state, now=lambda: stale)
    state.write_text("not json", encoding="utf-8")
    assert not probe.role_is_sleeping(state)


def test_health_probe_never_contends_for_the_role_lease(monkeypatch, tmp_path):
    secret = tmp_path / "dsn"
    secret.write_text("postgresql://x", encoding="utf-8")
    run_child = MagicMock(return_value=0)
    run_fenced = MagicMock(side_effect=AssertionError("probe took the lease"))
    monkeypatch.setattr(wrapper, "run_child", run_child)
    monkeypatch.setattr(wrapper, "run_fenced_child", run_fenced)
    monkeypatch.delenv("REALTIME_V2_APP_DSN", raising=False)

    assert wrapper.main([
        "--component",
        "bridge",
        "--app-dsn-file",
        str(secret),
        "--health-probe",
        "--",
        "python",
        "probe",
    ]) == 0

    run_child.assert_called_once()
    assert run_child.call_args.args[0] == ["python", "probe"]


def test_sleep_is_opt_in(monkeypatch, tmp_path):
    secret = tmp_path / "dsn"
    secret.write_text("postgresql://x", encoding="utf-8")
    run_fenced = MagicMock(return_value=0)
    gate = MagicMock(side_effect=AssertionError("sleep was not enabled"))
    monkeypatch.setattr(wrapper, "run_fenced_child", run_fenced)
    monkeypatch.setattr(wrapper, "run_session_bound_child", gate)
    monkeypatch.delenv("REALTIME_V2_APP_DSN", raising=False)
    monkeypatch.delenv(wrapper.OFF_HOURS_SLEEP_ENVIRONMENT, raising=False)

    assert wrapper.main([
        "--component",
        "bridge",
        "--app-dsn-file",
        str(secret),
        "--",
        "python",
        "role",
    ]) == 0
    run_fenced.assert_called_once()
