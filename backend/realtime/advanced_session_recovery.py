"""Which windows of a closed Advanced session can run now that it has closed."""
from __future__ import annotations

from datetime import datetime, timedelta

from .advanced_realtime_v3_runtime import (
    ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS,
    ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
)
from .advanced_scheduler import discover_advanced_window_recovery_cursor
from .advanced_stream_gap_repair import (
    load_stored_minutes_cursor,
    load_stream_gap_repair_scope_cursor,
)
from .database_access_v2 import APP_ROLE
from .equity_bar_repository import load_bar_range_cursor
from .session_windows import advanced_session_windows


def plan_session_windows(
    provider,
    scope,
    session_date,
    *,
    checked_at: datetime,
) -> list[dict[str, object]]:
    """Every incomplete cutoff, marked ready only if its inputs are stored now."""
    windows = advanced_session_windows(
        session_date,
        finality_delay_seconds=ADVANCED_REALTIME_V3_FINALITY_DELAY_SECONDS,
        completion_budget_seconds=(
            ADVANCED_REALTIME_V3_COMPLETION_BUDGET_SECONDS
        ),
        serving_lag_seconds=30,
    )
    rows = []
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        cursor = unit.cursor
        repair_scope = load_stream_gap_repair_scope_cursor(
            cursor,
            scope["plan_id"],
        )
        members = tuple(
            member.instrument_id for member in repair_scope.members
        )
        cursor.execute(
            "SELECT count(DISTINCT instrument_id) FROM rt.bar_revisions "
            "WHERE cohort_id=%s AND interval='1d' "
            "AND adjustment_basis='ADJUSTED' AND session_date=%s "
            "AND is_final",
            (str(scope["cohort_id"]), session_date),
        )
        daily_members = int(cursor.fetchone()[0])
        final_cutoff = windows[-1].cutoff
        # B20: the raw daily bar is derived from the session's 5m bars, so the close also waits for all of them.
        five_minute = load_bar_range_cursor(
            cursor, instrument_ids=members, interval="5m", basis="RAW_UNADJUSTED",
            start=windows[0].cutoff - timedelta(minutes=5), end=final_cutoff,
        )
        expected_five_minute = {window.cutoff for window in windows}
        missing_five_minute = sum(
            len(expected_five_minute - {row["bar_end"] for row in rows}) for rows in five_minute.values()
        ) + len(set(members) - set(five_minute))
        for window in windows:
            recovery = discover_advanced_window_recovery_cursor(
                cursor,
                cohort_id=scope["cohort_id"],
                cutoff=window.cutoff,
                checked_at=checked_at,
            )
            if recovery.complete:
                continue
            present = load_stored_minutes_cursor(
                cursor,
                cohort_id=scope["cohort_id"],
                instrument_ids=members,
                start=window.cutoff - timedelta(minutes=5),
                end=window.cutoff,
            )
            missing = len(members) * 5 - len(present)
            # At the close, screening treats the session as complete and
            # needs that session's own adjusted daily bar.
            needs_daily = (
                window.cutoff == final_cutoff
                and daily_members != len(members)
            )
            needs_five_minute = window.cutoff == final_cutoff and missing_five_minute > 0
            rows.append({
                "cutoff": window.cutoff,
                "next_attempt": recovery.next_attempt,
                "latest_state": recovery.latest_state,
                "missing_minute_coordinates": missing,
                "ready": missing == 0 and not needs_daily and not needs_five_minute,
                "waiting_for": (
                    "MISSING_1M_BARS"
                    if missing
                    else "ADJUSTED_DAILY_BAR"
                    if needs_daily
                    else "MISSING_5M_BARS"
                    if needs_five_minute
                    else None
                ),
            })
    return rows
