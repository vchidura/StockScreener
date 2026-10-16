import importlib
from uuid import UUID

import pytest

script = importlib.import_module("scripts.run_realtime_v2_bulk_backfill")
BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
    "--cohort-id",
    "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
    "--session-date",
    "2026-10-14",
    "--cache-directory",
    "backend\\.realtime-canary\\test-v2-backfill-cache",
]


def test_bulk_backfill_script_is_plan_only_without_both_authorizations():
    args = script.parse_args(BASE)
    assert not args.execute
    assert args.cohort_id == UUID("d1baee6f-4ab2-543c-8a86-ce10f8952e69")
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--execute", "--authorization-reference", "database-approved"])
    assert script.parse_args([
        *BASE,
        "--execute",
        "--authorization-reference",
        "database-approved",
        "--provider-authorization-reference",
        "provider-approved",
    ]).execute


def test_bulk_backfill_script_refuses_scope_drift():
    drifted = list(BASE)
    drifted[7] = "2026-10-13"
    with pytest.raises(SystemExit):
        script.parse_args(drifted)
    with pytest.raises(ValueError):
        script.parse_args([
            "--database",
            "stocks_realtime_v1",
            "--confirm-database-name",
            "stocks_realtime_v1",
            *BASE[4:],
        ])
