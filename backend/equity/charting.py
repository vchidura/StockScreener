"""Display-only bar composition from finalized canonical inputs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import exchange_calendars
import pandas as pd


TARGET_MINUTES = {"15m": 15, "30m": 30, "1h": 60}
SOURCE_DURATION = timedelta(minutes=5)


def fold_latest_session_bars(
    source_bars: Iterable[dict[str, Any]],
    *,
    target_interval: str,
    observed_at: datetime,
    calendar_name: str = "XNYS",
) -> tuple[dict[str, Any], ...]:
    """Compose latest-session target candles from contiguous finalized 5m bars."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    if target_interval not in (*TARGET_MINUTES, "1d"):
        raise ValueError(f"unsupported display interval: {target_interval}")

    ordered = sorted(source_bars, key=lambda row: row["bar_start"])
    if not ordered:
        return ()
    latest_source = ordered[-1]
    session = pd.Timestamp(latest_source["bar_start"].astimezone(timezone.utc).date())
    calendar = exchange_calendars.get_calendar(calendar_name)
    if not calendar.is_session(session):
        return ()
    session_open = calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)
    session_close = calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)

    session_rows = [
        row for row in ordered
        if session_open <= row["bar_start"] < session_close
    ]
    by_start = {row["bar_start"]: row for row in session_rows}
    if len(by_start) != len(session_rows):
        return ()
    latest_end = session_rows[-1]["bar_end"] if session_rows else session_open
    if latest_end > min(observed_at.astimezone(timezone.utc), session_close):
        return ()

    if target_interval == "1d":
        windows = ((session_open, session_close),)
    else:
        minutes = TARGET_MINUTES[target_interval]
        windows = []
        window_start = session_open
        while window_start < latest_end:
            window_end = min(
                window_start + timedelta(minutes=minutes), session_close
            )
            windows.append((window_start, window_end))
            window_start = window_end

    results = []
    for window_start, expected_end in windows:
        available_end = min(expected_end, latest_end)
        selected = _contiguous_rows(by_start, window_start, available_end)
        if not selected:
            continue
        results.append({
            "bar_start": window_start,
            "bar_end": available_end,
            "open_price": selected[0]["open_price"],
            "high_price": max(row["high_price"] for row in selected),
            "low_price": min(row["low_price"] for row in selected),
            "close_price": selected[-1]["close_price"],
            "volume": sum(row["volume"] for row in selected),
            "derived": True,
            "provisional": available_end < expected_end,
            "source_interval": "5m",
        })
    return tuple(results)


def _contiguous_rows(
    by_start: dict[datetime, dict[str, Any]],
    start: datetime,
    end: datetime,
) -> tuple[dict[str, Any], ...]:
    expected_starts = []
    cursor = start
    while cursor < end:
        expected_starts.append(cursor)
        cursor += SOURCE_DURATION
    if not expected_starts:
        return ()
    selected = tuple(by_start[value] for value in expected_starts if value in by_start)
    if len(selected) != len(expected_starts):
        return ()
    if selected[0]["bar_start"] != start or selected[-1]["bar_end"] != end:
        return ()
    if any(
        left["bar_end"] != right["bar_start"]
        for left, right in zip(selected, selected[1:])
    ):
        return ()
    return selected