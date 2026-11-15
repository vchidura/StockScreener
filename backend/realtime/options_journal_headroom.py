"""Options journal free-space guard (R81 / G106 H3, Nov 15).

Equity ingress refuses to open a journal unless the journal disk keeps 5 GiB and 20% free
(`measure_journal_capacity`, the ingress profile's `minimum_journal_free_bytes` / `_percent`). Options acquisition had
no such check, so a long V4 capture (~12 GB a day at 216 contracts) could fill the shared journal volume and starve
Equity. This guard gives Options the same thresholds:

- before a session's journal is opened (and before every reconnect attempt) the acquisition measures the journal
  disk; below the floor it refuses with the label `JOURNAL_DISK_HEADROOM_LOW` (no journal, no provider socket, no
  crash: the process waits and checks again);
- while capturing it re-measures at most once a minute; below the floor the attempt ends and the session PAUSES (the
  same path as a stop before the session's end: the journal stays open and the next start continues it when space is
  back).

Settings (compose environment, same defaults as the Equity ingress profile):
  REALTIME_V3_OPTIONS_JOURNAL_GUARD             ON (default) | OFF
  REALTIME_V3_OPTIONS_JOURNAL_MIN_FREE_BYTES    5368709120 (5 GiB)
  REALTIME_V3_OPTIONS_JOURNAL_MIN_FREE_PERCENT  20
"""
from __future__ import annotations

import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping

from .advanced_journal_registry import JournalCapacityObservation, measure_journal_capacity

GUARD_ENV = "REALTIME_V3_OPTIONS_JOURNAL_GUARD"
MIN_FREE_BYTES_ENV = "REALTIME_V3_OPTIONS_JOURNAL_MIN_FREE_BYTES"
MIN_FREE_PERCENT_ENV = "REALTIME_V3_OPTIONS_JOURNAL_MIN_FREE_PERCENT"
DEFAULT_MIN_FREE_BYTES = 5_368_709_120  # = advanced_equity_ingress_v1 minimum_journal_free_bytes
DEFAULT_MIN_FREE_PERCENT = 20  # = advanced_equity_ingress_v1 minimum_journal_free_percent
DEFAULT_CHECK_INTERVAL_SECONDS = 60.0
HEADROOM_LOW = "JOURNAL_DISK_HEADROOM_LOW"
HEADROOM_UNAVAILABLE = "JOURNAL_DISK_USAGE_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class HeadroomVerdict:
    admitted: bool
    reason: str | None
    observation: JournalCapacityObservation | None

    def document(self) -> dict[str, object]:
        observed = self.observation
        return {
            "admitted": self.admitted,
            "reason": self.reason,
            "free_bytes": None if observed is None else observed.free_bytes,
            "total_bytes": None if observed is None else observed.total_bytes,
            "free_percent": None if observed is None else round(observed.free_percent, 3),
            "minimum_free_bytes": None if observed is None else observed.minimum_free_bytes,
            "minimum_free_percent": None if observed is None else observed.minimum_free_percent,
        }


@dataclass(slots=True)
class OptionsJournalHeadroomGuard:
    """Thresholds plus a throttled in-session watch. `enabled=False` admits everything (the OFF switch)."""

    enabled: bool = True
    minimum_free_bytes: int = DEFAULT_MIN_FREE_BYTES
    minimum_free_percent: int = DEFAULT_MIN_FREE_PERCENT
    check_interval_seconds: float = DEFAULT_CHECK_INTERVAL_SECONDS
    disk_usage: Callable = shutil.disk_usage
    monotonic: Callable[[], float] = time.monotonic
    _next_watch_at: float = field(default=0.0, repr=False)
    low: HeadroomVerdict | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.minimum_free_bytes) is not int
            or self.minimum_free_bytes <= 0
            or type(self.minimum_free_percent) is not int
            or not 1 <= self.minimum_free_percent <= 100
            or not self.check_interval_seconds > 0
        ):
            raise ValueError("Options journal headroom thresholds are invalid")

    def check(self, root: Path) -> HeadroomVerdict:
        """Fail closed: an unreadable disk is a refusal, as for Equity (its ValueError fails the ingress)."""
        if not self.enabled:
            return HeadroomVerdict(True, None, None)
        try:
            observation = measure_journal_capacity(
                Path(root),
                minimum_free_bytes=self.minimum_free_bytes,
                minimum_free_percent=self.minimum_free_percent,
                disk_usage=self.disk_usage,
            )
        except (OSError, ValueError):
            return HeadroomVerdict(False, HEADROOM_UNAVAILABLE, None)
        if observation.admitted:
            return HeadroomVerdict(True, None, observation)
        return HeadroomVerdict(False, HEADROOM_LOW, observation)

    def watch(self, root: Path) -> bool:
        """In-session check, at most once per interval: True once the disk is below the floor (latched).
        Fail open here: a measurement error mid-session never stops a running capture."""
        if not self.enabled:
            return False
        if self.low is not None:
            return True
        now = self.monotonic()
        if now < self._next_watch_at:
            return False
        self._next_watch_at = now + self.check_interval_seconds
        verdict = self.check(root)
        if not verdict.admitted and verdict.reason == HEADROOM_LOW:
            self.low = verdict
            return True
        return False


def options_journal_headroom_guard(environment: Mapping[str, str]) -> OptionsJournalHeadroomGuard:
    mode = str(environment.get(GUARD_ENV, "ON") or "ON").strip().upper()
    if mode not in {"ON", "OFF"}:
        raise ValueError(f"{GUARD_ENV} must be ON or OFF")

    def integer(name: str, default: int) -> int:
        raw = str(environment.get(name, "") or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError as error:
            raise ValueError(f"{name} must be an integer") from error

    return OptionsJournalHeadroomGuard(
        enabled=mode == "ON",
        minimum_free_bytes=integer(MIN_FREE_BYTES_ENV, DEFAULT_MIN_FREE_BYTES),
        minimum_free_percent=integer(MIN_FREE_PERCENT_ENV, DEFAULT_MIN_FREE_PERCENT),
    )
