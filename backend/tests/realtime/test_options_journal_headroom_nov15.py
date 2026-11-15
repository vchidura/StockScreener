"""Options journal free-space guard (R81, Nov 15): the Equity ingress floor (5 GiB and 20% free) for Options."""
from __future__ import annotations

import pytest

from realtime.options_journal_headroom import (
    DEFAULT_MIN_FREE_BYTES,
    DEFAULT_MIN_FREE_PERCENT,
    HEADROOM_LOW,
    HEADROOM_UNAVAILABLE,
    OptionsJournalHeadroomGuard,
    options_journal_headroom_guard,
)

GIB = 1024 ** 3


def _disk(total, free):
    return lambda _path: (total, total - free, free)


def test_defaults_equal_the_equity_ingress_profile():
    import json
    from pathlib import Path

    policy = json.loads(
        (Path(__file__).resolve().parents[2] / "realtime/policies/advanced_equity_ingress_v1.json").read_text()
    )
    text = json.dumps(policy)
    assert f'"minimum_journal_free_bytes": {DEFAULT_MIN_FREE_BYTES}' in text
    assert f'"minimum_journal_free_percent": {DEFAULT_MIN_FREE_PERCENT}' in text
    guard = options_journal_headroom_guard({})
    assert (guard.enabled, guard.minimum_free_bytes, guard.minimum_free_percent) == (
        True, 5 * GIB, 20,
    )


@pytest.mark.parametrize(
    ("total", "free", "admitted"),
    [
        (1000 * GIB, 822 * GIB, True),  # live Nov 15: 82% free
        (1000 * GIB, 200 * GIB, True),  # exactly 20%
        (1000 * GIB, 199 * GIB, False),  # below 20%
        (20 * GIB, 5 * GIB, True),  # exactly 5 GiB and 25%
        (20 * GIB, 5 * GIB - 1, False),  # below 5 GiB
    ],
)
def test_check_admits_only_above_both_floors(tmp_path, total, free, admitted):
    guard = OptionsJournalHeadroomGuard(disk_usage=_disk(total, free))
    verdict = guard.check(tmp_path)
    assert verdict.admitted is admitted
    assert verdict.reason == (None if admitted else HEADROOM_LOW)
    assert verdict.document()["free_bytes"] == free


def test_check_fails_closed_when_the_disk_cannot_be_measured(tmp_path):
    def broken(_path):
        raise OSError("statvfs failed")

    assert OptionsJournalHeadroomGuard(disk_usage=broken).check(tmp_path).reason == HEADROOM_UNAVAILABLE
    assert OptionsJournalHeadroomGuard().check(tmp_path / "missing").reason == HEADROOM_UNAVAILABLE


def test_off_switch_admits_everything(tmp_path):
    guard = options_journal_headroom_guard({"REALTIME_V3_OPTIONS_JOURNAL_GUARD": "off"})
    guard.disk_usage = _disk(100 * GIB, 0)
    assert guard.check(tmp_path).admitted
    assert guard.watch(tmp_path) is False


def test_environment_overrides_and_invalid_values():
    guard = options_journal_headroom_guard({
        "REALTIME_V3_OPTIONS_JOURNAL_MIN_FREE_BYTES": "1073741824",
        "REALTIME_V3_OPTIONS_JOURNAL_MIN_FREE_PERCENT": "10",
    })
    assert (guard.minimum_free_bytes, guard.minimum_free_percent) == (GIB, 10)
    with pytest.raises(ValueError, match="ON or OFF"):
        options_journal_headroom_guard({"REALTIME_V3_OPTIONS_JOURNAL_GUARD": "maybe"})
    with pytest.raises(ValueError, match="must be an integer"):
        options_journal_headroom_guard({"REALTIME_V3_OPTIONS_JOURNAL_MIN_FREE_BYTES": "5G"})
    with pytest.raises(ValueError, match="thresholds are invalid"):
        options_journal_headroom_guard({"REALTIME_V3_OPTIONS_JOURNAL_MIN_FREE_PERCENT": "0"})


def test_watch_is_throttled_latched_and_fails_open(tmp_path):
    clock = [0.0]
    free = [500 * GIB]
    calls = []

    def disk(_path):
        calls.append(1)
        if free[0] is None:
            raise OSError("statvfs failed")
        return (1000 * GIB, 1000 * GIB - free[0], free[0])

    guard = OptionsJournalHeadroomGuard(disk_usage=disk, monotonic=lambda: clock[0], check_interval_seconds=60)
    assert guard.watch(tmp_path) is False and len(calls) == 1
    free[0] = 1 * GIB
    clock[0] = 30.0
    assert guard.watch(tmp_path) is False and len(calls) == 1  # throttled
    free[0] = None
    clock[0] = 61.0
    assert guard.watch(tmp_path) is False and len(calls) == 2  # an error mid-session never stops capture
    free[0] = 1 * GIB
    clock[0] = 122.0
    assert guard.watch(tmp_path) is True and len(calls) == 3
    free[0] = 500 * GIB
    clock[0] = 500.0
    assert guard.watch(tmp_path) is True and len(calls) == 3  # latched for the attempt
    assert guard.low.reason == HEADROOM_LOW
