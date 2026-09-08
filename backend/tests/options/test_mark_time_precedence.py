import sys
from datetime import datetime, timezone
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.normalizer import parse_polygon_snapshot
from options.domain import DataQualityFlag

UTC = timezone.utc
OBSERVED = datetime(2026, 9, 4, 20, 0, tzinfo=UTC)

# 2026-09-04 19:45:00Z and 20:00:00Z as nanosecond epochs.
TRADE_NS = int(datetime(2026, 9, 4, 19, 45, tzinfo=UTC).timestamp() * 1e9)
RELEASE_NS = int(datetime(2026, 9, 4, 20, 0, tzinfo=UTC).timestamp() * 1e9)


def _payload(*, trade_ns=TRADE_NS, release_ns=RELEASE_NS):
    payload = {
        "details": {"ticker": "O:SPY260918C00650000"},
        "day": {"close": 2.5, "vwap": 2.4, "volume": 120},
        "underlying_asset": {"price": 645.0},
        "open_interest": 900,
    }
    if release_ns is not None:
        payload["day"]["last_updated"] = release_ns
    if trade_ns is not None:
        payload["last_trade"] = {"sip_timestamp": trade_ns}
    return payload


def test_sip_trade_timestamp_is_preferred_over_the_release_stamp():
    # day.last_updated carries the entitlement delay; the trade time is the real
    # market time at which day.close was established.
    observation = parse_polygon_snapshot(_payload(), OBSERVED)
    assert observation.option_mark_time == datetime(2026, 9, 4, 19, 45, tzinfo=UTC)
    assert observation.mark_time_from_release_stamp is False


def test_release_stamp_is_used_only_when_no_trade_timestamp_exists():
    observation = parse_polygon_snapshot(_payload(trade_ns=None), OBSERVED)
    assert observation.option_mark_time == datetime(2026, 9, 4, 20, 0, tzinfo=UTC)
    assert observation.mark_time_from_release_stamp is True


def test_missing_last_trade_object_falls_back_cleanly():
    payload = _payload()
    payload.pop("last_trade")
    observation = parse_polygon_snapshot(payload, OBSERVED)
    assert observation.option_mark_time == datetime(2026, 9, 4, 20, 0, tzinfo=UTC)
    assert observation.mark_time_from_release_stamp is True


def test_no_timestamp_at_all_yields_none_without_the_flag():
    observation = parse_polygon_snapshot(
        _payload(trade_ns=None, release_ns=None), OBSERVED
    )
    assert observation.option_mark_time is None
    assert observation.mark_time_from_release_stamp is False


def test_release_stamp_flag_is_a_named_quality_flag():
    assert DataQualityFlag.MARK_TIME_FROM_RELEASE_STAMP.value == (
        "MARK_TIME_FROM_RELEASE_STAMP"
    )


def test_preference_removes_the_entitlement_delay_offset():
    observation = parse_polygon_snapshot(_payload(), OBSERVED)
    release = datetime(2026, 9, 4, 20, 0, tzinfo=UTC)
    offset = (release - observation.option_mark_time).total_seconds()
    assert offset == 900.0
