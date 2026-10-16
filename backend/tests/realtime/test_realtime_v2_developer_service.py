import importlib

import pytest

script = importlib.import_module("scripts.run_realtime_v2_developer_service")
BASE = [
    "--database", "stocks_realtime_v2",
    "--confirm-database-name", "stocks_realtime_v2",
    "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
    "--session-date", "2026-10-15",
    "--first-cutoff", "2026-10-15T17:45:00+00:00",
    "--window-count", "3",
    "--maximum-provider-requests", "54",
    "--cache-root", "backend\\.realtime-canary\\service-cache",
    "--report-directory", "backend\\.realtime-canary\\service-reports",
    "--summary-report", "backend\\.realtime-canary\\service-summary.json",
]


def test_developer_service_requires_exact_bounded_scope():
    args = script.parse_args(BASE)
    assert len(args.cutoffs) == 3
    assert args.maximum_provider_requests == 54
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE[:BASE.index("--maximum-provider-requests") + 1],
            "55",
            *BASE[BASE.index("--maximum-provider-requests") + 2:],
        ])
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--execute"])
