from datetime import datetime, timezone

import pytest

from realtime.equity_v2_window_source import EXPECTED_PROVIDER_REQUESTS, WINDOW_MINUTES


def test_v2_window_scope_is_one_exact_five_minute_slice():
    assert WINDOW_MINUTES == 5
    assert EXPECTED_PROVIDER_REQUESTS == 18


def test_v2_window_source_rejects_naive_cutoff():
    from realtime.equity_v2_window_source import PolygonEquityV2WindowSource

    with pytest.raises(ValueError, match="timezone-aware"):
        PolygonEquityV2WindowSource(object(), cutoff=datetime(2026, 10, 15, 16, 0))

    assert datetime(2026, 10, 15, 16, 0, tzinfo=timezone.utc).utcoffset() is not None
