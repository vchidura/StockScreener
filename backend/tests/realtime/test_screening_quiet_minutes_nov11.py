"""CRWD empty minute (Nov 11): an intraday bar derived from 1m bars may skip a quiet minute (the provider has no bar
for it); the sources must still be ordered, non-overlapping and inside the window, and the aggregate must match."""
from datetime import timedelta
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.screening_projection import _validate_bars
from tests.realtime.test_screening_vertical_slice import screening_fixture


def _setup():
    inputs = screening_fixture()
    member = next(row for row in inputs.members if row["ticker"] == "AAPL")
    template = min((row for row in inputs.bars if row["ticker"] == "AAPL" and row["interval"] == "1d"),
                   key=lambda row: row["bar_start"])
    return inputs, member, template


def _minute(template, start, index, *, price=100.0, volume=10.0):
    begin = start + timedelta(minutes=index)
    return {
        **template, "bar_revision_id": uuid5(NAMESPACE_URL, f"quiet-minute:{begin.isoformat()}:{price}:{volume}"),
        "interval": "1m", "adjustment_basis": "RAW_UNADJUSTED", "bar_start": begin,
        "bar_end": begin + timedelta(minutes=1), "source_kind": "REALTIME_STREAM", "source_revision_ids": (),
        "open": price, "high": price + 1, "low": price - 1, "close": price + 0.5, "volume": volume,
    }


def _derived(template, start, minutes, sources, *, interval="5m", volume=None):
    return {
        **template, "bar_revision_id": uuid5(NAMESPACE_URL, f"quiet-derived:{start.isoformat()}:{interval}:{len(sources)}"),
        "interval": interval, "adjustment_basis": "RAW_UNADJUSTED", "bar_start": start,
        "bar_end": start + timedelta(minutes=minutes), "source_kind": "DERIVED",
        "source_revision_ids": tuple(row["bar_revision_id"] for row in sources),
        "open": sources[0]["open"], "high": max(row["high"] for row in sources),
        "low": min(row["low"] for row in sources), "close": sources[-1]["close"],
        "volume": sum(row["volume"] for row in sources) if volume is None else volume,
    }


@pytest.mark.parametrize("skipped", ((1,), (0,), (4,), (1, 3)))
def test_a_5m_bar_from_1m_bars_accepts_quiet_minutes(skipped):
    inputs, member, template = _setup()
    start = template["bar_start"]
    sources = [_minute(template, start, index, price=100 + index) for index in range(5) if index not in skipped]
    _validate_bars((*sources, _derived(template, start, 5, sources)), member, inputs)


def test_the_complete_5m_bar_still_passes_and_the_aggregate_is_still_checked():
    inputs, member, template = _setup()
    start = template["bar_start"]
    sources = [_minute(template, start, index, price=100 + index) for index in (0, 2, 3, 4)]
    _validate_bars((*sources, _derived(template, start, 5, sources)), member, inputs)
    complete = [_minute(template, start, index, price=100 + index) for index in range(5)]
    _validate_bars((*complete, _derived(template, start, 5, complete)), member, inputs)
    with pytest.raises(ValueError, match="aggregate drift"):
        _validate_bars((*sources, _derived(template, start, 5, sources, volume=999.0)), member, inputs)


def test_quiet_minutes_do_not_admit_overlap_or_sources_outside_the_window():
    inputs, member, template = _setup()
    start = template["bar_start"]
    outside = [_minute(template, start, index, price=100 + index) for index in (0, 2, 5)]
    with pytest.raises(ValueError, match="window/basis drift"):
        _validate_bars((*outside, _derived(template, start, 5, outside)), member, inputs)
    first = _minute(template, start, 1, price=101)
    overlapping = {**_minute(template, start, 1, price=102), "bar_start": first["bar_start"] + timedelta(seconds=30),
                   "bar_end": first["bar_end"] + timedelta(seconds=30)}
    sources = [_minute(template, start, 0), first, overlapping]
    with pytest.raises(ValueError, match="window/basis drift"):
        _validate_bars((*sources, _derived(template, start, 5, sources)), member, inputs)


def test_bars_derived_from_5m_bars_still_need_every_source():
    inputs, member, template = _setup()
    start = template["bar_start"]
    fives, minutes = [], []
    for index in (0, 2):  # the 5m bucket at +5 minutes is missing
        own = [_minute(template, start + timedelta(minutes=5 * index), i, price=100 + i) for i in range(5)]
        minutes.extend(own)
        fives.append(_derived(template, start + timedelta(minutes=5 * index), 5, own))
    _validate_bars((*minutes, *fives), member, inputs)
    with pytest.raises(ValueError, match="window/basis drift"):
        _validate_bars((*minutes, *fives, _derived(template, start, 15, fives, interval="15m")), member, inputs)
