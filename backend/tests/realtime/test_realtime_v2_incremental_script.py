import importlib

import pytest

from realtime.acquisition_ledger import ProviderRequestLane


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
    assert args.provider_delay_seconds == 900


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


def test_advanced_incremental_requires_explicit_bound_eligibility_policy():
    advanced = [
        *BASE,
        "--advanced-ingress-plan-id",
        "11111111-1111-1111-1111-111111111111",
        "--interval",
        "1m",
        "--cutoff",
        "2026-10-16T14:00:00+00:00",
        "--once",
        "--execute-shadow",
    ]
    with pytest.raises(SystemExit):
        script.parse_args(advanced)

    args = script.parse_args([
        *advanced,
        "--provider-delay-seconds",
        "900",
        "--rest-endpoint-class",
        "MASSIVE_STOCKS_AGGREGATES_1M",
        "--eligibility-policy-version",
        "conservative-unverified-delay-v1",
    ])
    scope = script._scope(args)

    assert scope.account_alias == "MASSIVE_ADVANCED"
    assert scope.lanes == frozenset({ProviderRequestLane.EQUITY_HISTORY})
    assert args.provider_delay_seconds == 900
