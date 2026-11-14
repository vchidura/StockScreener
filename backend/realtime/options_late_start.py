"""R67 (Nov 13): late start for the Options resident consumers (market-state, its detector router, flow).

A consumer with no checkpoint starts at journal offset 0, the stream's first frame. That is right when it is bound at
the open, but on Nov 13 the first binding came at 15:13 (R63), 38 minutes into the stream, and replaying from 0 at V4
volume never caught up (R65). None of these consumers needs the session's history to serve the current window:
market-state keeps one current quote and trade (and up to 8 recent quotes) per contract; the detector router and flow
start WARMING (flow's session volume baselines reconcile from the chain snapshot, SNAPSHOT_RECONCILED).

With REALTIME_V3_OPTIONS_LATE_START=ON, a consumer that has NO checkpoint starts at the journal point two minutes before
now instead of 0, and the resident barrier skips windows cut before that point (they were never consumed; they stay
missed instead of being marked complete with later state). A consumer that has a checkpoint resumes from it as before.
Off (the default) changes nothing.
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone

from .journal import RawJournalReader

OPTIONS_LATE_START_SWITCH = "REALTIME_V3_OPTIONS_LATE_START"
OPTIONS_LATE_START_LOOKBACK = timedelta(seconds=120)
WARMING_LATE_START = "WARMING_LATE_START"


def options_late_start_enabled(environment: Mapping[str, str] | None = None) -> bool:
    values = environment if isinstance(environment, Mapping) else os.environ
    value = values.get(OPTIONS_LATE_START_SWITCH)
    if value is None and values is not os.environ:
        value = os.environ.get(OPTIONS_LATE_START_SWITCH)
    return str(value or "").strip().upper() == "ON"


OPTIONS_STALE_CHECKPOINT_AGE = timedelta(minutes=10)


def options_checkpoint_stale(path, committed_raw_offset: int, *, now: datetime | None = None) -> bool:
    """True when a stored checkpoint is more than OPTIONS_STALE_CHECKPOINT_AGE behind the journal (its raw offset is
    before the frame of a window cut that long ago): resuming it would replay that backlog, so a late start begins at
    the late point instead (Nov 13: flow's checkpoint was ~1 h behind at 16:21)."""
    at = (now or datetime.now(timezone.utc)) - OPTIONS_STALE_CHECKPOINT_AGE
    raw, _ = RawJournalReader(path).offsets_at(at)
    return int(raw) > 0 and int(committed_raw_offset) < int(raw)


def options_late_start_point(path, *, now: datetime | None = None) -> tuple[datetime, int, int]:
    """(cutoff, raw_offset, event_offset): where a window cut two minutes ago ends in this journal; offsets are 0 for an
    empty journal (then the late start is the normal start)."""
    at = (now or datetime.now(timezone.utc)) - OPTIONS_LATE_START_LOOKBACK
    raw, event = RawJournalReader(path).offsets_at(at)
    return at, int(raw), int(event)
