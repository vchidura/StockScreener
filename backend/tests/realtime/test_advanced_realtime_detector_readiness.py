import importlib

import pytest

from realtime.advanced_equity_detector_shadow import (
    CONTRACT_VERSION,
    PRODUCTION_AUTHORITY,
)
from realtime.advanced_options_p7_shadow import (
    EXPECTED_P7_MIGRATIONS,
    _migration_bindings,
)
from realtime.advanced_equity_s1_s2_shadow import s1_s2_requirements


equity_window = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_window"
)
WINDOW_BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
    "--cohort-id",
    "11111111-1111-1111-1111-111111111111",
    "--session-date",
    "2026-10-20",
    "--cutoff",
    "2026-10-20T13:35:00+00:00",
    "--finality-delay-seconds",
    "5",
    "--authorization-reference",
    "advanced-detector-readiness-source-gate",
    "--report-path",
    "advanced-detector-readiness.json",
    "--execute",
]


def test_cross_lane_detector_readiness_is_bound_to_031_and_shadow_only():
    migrations = _migration_bindings()

    assert tuple(row["version"] for row in migrations[-7:]) == (
        EXPECTED_P7_MIGRATIONS
    )
    assert migrations[-1]["version"] == (
        "031_realtime_advanced_journal_recovery"
    )
    assert CONTRACT_VERSION == "advanced-equity-detector-shadow-v1"
    assert set(PRODUCTION_AUTHORITY) == {
        "provider_requests",
        "portal_publication",
        "outbox_creation",
        "outbox_dispatch",
        "platform_pointer",
        "external_delivery",
        "trade_execution",
    }
    assert not any(PRODUCTION_AUTHORITY.values())
    assert {
        detector: s1_s2_requirements(detector)["trigger_interval"]
        for detector in ("S1", "S2")
    } == {"S1": "5m", "S2": "5m"}


@pytest.mark.parametrize(
    "prohibited_flag",
    ("--include-alerts", "--include-strategies", "--publish"),
)
def test_detector_shadow_does_not_unlock_legacy_window_lanes(
    prohibited_flag,
):
    with pytest.raises(SystemExit):
        equity_window.parse_args([*WINDOW_BASE, prohibited_flag])
