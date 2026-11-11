"""R39 (Nov 11): Dashboard payloads free of -0.0 (jsonb stores it as 0.0 and the stored checksum then fails), and the
SESSION refresh switch (first window and close only; no hourly rebuild)."""
import importlib
import json
from datetime import datetime, timezone

import pytest

from realtime.contracts import canonical_json, canonical_sha256
from realtime.dashboard_projection import without_negative_zero

script = importlib.import_module("scripts.run_realtime_v2_advanced_equity_window")


def _jsonb_round_trip(value):
    # Postgres numeric has no negative zero: -0.0 comes back as 0.0; every other number keeps its value.
    def numeric(text):
        number = float(text)
        return number + 0.0
    return json.loads(canonical_json(value), parse_float=numeric)


def test_negative_zero_breaks_the_checksum_after_jsonb_and_the_normalizer_keeps_it():
    payload = dict(
        overview=[dict(ticker="AAPL", chg_pct=round(-0.001, 2), close=348.56, volume=21002074, flag=True)],
        scan=dict(fibonacci=dict(results=[dict(confirmed_legs=[dict(distance_pct=-0.0)])])),
        tuple_values=(-0.0, 1.5), text="-0.0", nothing=None,
    )
    assert canonical_sha256(_jsonb_round_trip(payload)) != canonical_sha256(payload)  # the Nov 10 close failure
    fixed = without_negative_zero(payload)
    assert canonical_sha256(_jsonb_round_trip(fixed)) == canonical_sha256(fixed)
    assert "-0.0" not in canonical_json(fixed).replace('"-0.0"', "")
    # Nothing else changes: values, ints, bools, strings and None keep their type and value.
    assert fixed["overview"][0] == dict(ticker="AAPL", chg_pct=0.0, close=348.56, volume=21002074, flag=True)
    assert isinstance(fixed["overview"][0]["volume"], int) and fixed["overview"][0]["flag"] is True
    assert fixed["tuple_values"] == (0.0, 1.5) and fixed["text"] == "-0.0" and fixed["nothing"] is None


def test_normalizer_leaves_a_payload_without_negative_zero_byte_identical():
    payload = dict(a=[1, 2.5, -3.25, dict(b=0.0, c="x")], d=-1e-07)
    assert canonical_json(without_negative_zero(payload)) == canonical_json(payload)


def _at(minute):
    return datetime(2026, 11, 11, 16, minute, tzinfo=timezone.utc)


@pytest.mark.parametrize("switch,minute,warm,due", [
    (None, 35, True, True),        # default HOURLY: unchanged rule
    ("HOURLY", 35, True, True),
    ("SESSION", 35, True, False),  # no hourly rebuild
    ("session", 35, True, False),
    ("SESSION", 40, True, False),
    ("SESSION", 35, False, True),  # first window of a process or cohort, retry or restart with a fresh pair
    ("SESSION", 40, False, True),
])
def test_session_refresh_keeps_first_window_and_drops_hourly(monkeypatch, switch, minute, warm, due):
    if switch is None:
        monkeypatch.delenv(script.DASHBOARD_REFRESH_SWITCH, raising=False)
    else:
        monkeypatch.setenv(script.DASHBOARD_REFRESH_SWITCH, switch)
    assert script.dashboard_due(_at(minute), warm=warm) is due
