import importlib

import pytest


script = importlib.import_module("scripts.run_realtime_v2_incremental_equity")
BASE = [
    "--database", "stocks_realtime_v2",
    "--confirm-database-name", "stocks_realtime_v2",
    "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
    "--session-date", "2026-10-16",
    "--response-byte-limit", "2097152",
    "--account-request-ceiling", "4212",
    "--account-decoded-byte-ceiling", "8833208436",
    "--authorized-until", "2026-10-17T00:00:00+00:00",
    "--authorization-reference", "standing-v1",
    "--provider-authorization-reference", "subscription",
    "--expected-cost", "no incremental cost",
]


def test_incremental_script_is_plan_only_by_default():
    args = script.parse_args(BASE)

    assert not args.execute_shadow
    assert args.intervals == ("1m", "5m")


def test_incremental_execution_requires_explicit_shadow_mode():
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--continuous"])
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--execute-shadow"])

    args = script.parse_args([
        *BASE,
        "--interval", "1m",
        "--continuous",
        "--execute-shadow",
    ])
    assert args.intervals == ("1m",)
    assert args.continuous and args.execute_shadow
