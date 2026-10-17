from datetime import date, datetime, timezone
from uuid import UUID

from realtime.session_windows import advanced_session_windows
from realtime.v2_service import window_specs_for_session


UTC = timezone.utc
SESSION = date(2026, 10, 16)


def test_advanced_windows_are_exact_five_minute_closures():
    windows = advanced_session_windows(
        SESSION,
        finality_delay_seconds=5,
        completion_budget_seconds=120,
        serving_lag_seconds=30,
    )

    assert len(windows) == 78
    assert windows[0].cutoff == datetime(
        2026,
        10,
        16,
        13,
        35,
        tzinfo=UTC,
    )
    assert windows[0].available_after == datetime(
        2026,
        10,
        16,
        13,
        35,
        5,
        tzinfo=UTC,
    )
    assert windows[-1].cutoff == datetime(
        2026,
        10,
        16,
        20,
        0,
        tzinfo=UTC,
    )


def test_v2_window_factory_requires_explicit_advanced_finality_delay():
    cohort = UUID(
        "11111111-1111-1111-1111-111111111111"
    )
    specs = window_specs_for_session(
        cohort,
        SESSION,
        "MASSIVE_ADVANCED",
        completion_budget_seconds=120,
        serving_lag_seconds=30,
        advanced_finality_delay_seconds=5,
    )

    assert len(specs) == 78
    assert all(spec.provider_tier == "MASSIVE_ADVANCED" for spec in specs)
