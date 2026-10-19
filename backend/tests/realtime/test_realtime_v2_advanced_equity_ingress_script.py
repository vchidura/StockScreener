import importlib
import asyncio
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest


script = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_ingress"
)
BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
    "--ingress-plan-id",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "--debug-once",
    "--maximum-market-messages",
    "5",
    "--duration-seconds",
    "300",
    "--execute",
]


def test_ingress_cli_requires_only_plan_scope_and_execution():
    args = script.parse_args(BASE)

    assert args.ingress_plan_id == UUID(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    )
    assert args.maximum_market_messages == 5
    assert not hasattr(args, "cohort_id")
    assert not hasattr(args, "session_date")
    assert not hasattr(args, "authorization_reference")
    assert not hasattr(args, "decoded_byte_ceiling")


def test_ingress_cli_requires_explicit_execution_and_bounded_once():
    with pytest.raises(SystemExit):
        script.parse_args(BASE[:-1])
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE[:BASE.index("--maximum-market-messages")],
            "--execute",
        ])


def test_expired_persisted_deadline_never_starts_a_gateway_even_after_restart():
    ended = datetime(2026, 10, 19, 13, 40, tzinfo=timezone.utc)
    gateway = SimpleNamespace(run_session=MagicMock(side_effect=AssertionError("expired provider I/O")))
    for now in (ended, ended + timedelta(minutes=1)):
        assert asyncio.run(script._run_continuous(
            gateway, threading.Event(), 0.1, ended, utc_clock=lambda: now,
        )) is None
        assert asyncio.run(script._run_once(
            gateway, 10, 600, deadline=ended, utc_clock=lambda: now,
        )) is None
    gateway.run_session.assert_not_called()


def test_monotonic_deadline_stops_provider_when_wall_clock_stalls():
    started = datetime(2026, 10, 19, 13, 30, tzinfo=timezone.utc)
    stopped = []

    async def run():
        try:
            await asyncio.Event().wait()
        finally:
            stopped.append(True)

    result = asyncio.run(script._run_continuous(
        SimpleNamespace(run_session=run), threading.Event(), 1,
        started + timedelta(milliseconds=10), utc_clock=lambda: started,
    ))
    assert result is None
    assert stopped == [True]


def test_ingress_paths_are_plan_session_attempt_derived_and_non_overwriting(
    tmp_path,
):
    plan_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    journal, report = script._derived_runtime_paths(
        tmp_path,
        plan_id=plan_id,
        session_date=date(2026, 10, 16),
        connection_attempt=2,
    )

    assert journal.name == (
        "stocks-2026-10-16-aaaaaaaa-aaaa-aaaa-aaaa-"
        "aaaaaaaaaaaa-attempt-002.sqlite"
    )
    assert report.name == journal.with_suffix(".json").name
    report.touch()
    with pytest.raises(RuntimeError, match="already exists"):
        script._derived_runtime_paths(
            tmp_path,
            plan_id=plan_id,
            session_date=date(2026, 10, 16),
            connection_attempt=2,
        )


@pytest.mark.parametrize(("symbol", "bounded"), [("SPY", False), ("SPY", True), ("AAPL", True)])
def test_ingress_runtime_scope_loads_pinned_plan_and_exact_am_members(
    monkeypatch,
    symbol,
    bounded,
):
    catalog = script.load_advanced_ingress_runtime_catalog(
        Path(script.BACKEND_DIR)
        / "realtime"
        / "policies"
        / "advanced_equity_ingress_v1.json"
    )
    profile = catalog.profile_for_target(1)
    plan = SimpleNamespace(
        plan_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        cohort_id=UUID("11111111-1111-1111-1111-111111111111"),
        manifest_id=UUID("22222222-2222-2222-2222-222222222222"),
        session_date=date(2026, 10, 16),
        universe_target=1,
        runtime_profile_version=profile.version,
        runtime_profile_sha256=profile.sha256,
        runtime_policy_sha256=catalog.scale_policy.sha256,
        configuration_sha256=profile.configuration_sha256(
            catalog.scale_policy.sha256
        ),
        authorized_start_at=datetime(2026, 10, 16, 13, 30, tzinfo=timezone.utc),
        authorized_end_at=datetime(2026, 10, 16, 13, 40, tzinfo=timezone.utc)
        if bounded else datetime(2026, 10, 16, 20, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        script,
        "load_advanced_ingress_runtime_plan_cursor",
        lambda _cursor, _plan_id: plan,
    )
    provider = MagicMock()
    unit = provider.transaction.return_value.__enter__.return_value
    unit.cursor.fetchone.return_value = {
        "manifest_id": "22222222-2222-2222-2222-222222222222",
        "session_date": date(2026, 10, 16),
        "allocation_policy_sha256": "a" * 64,
        "stock_count": 1,
        "profile_sha256": "b" * 64,
        "provider_tier": "MASSIVE_ADVANCED",
        "configuration_sha256": plan.configuration_sha256,
    }
    unit.cursor.fetchall.return_value = [{
        "instrument_id": 1,
        "security_id": "33333333-3333-3333-3333-333333333333",
        "ticker": symbol,
    }]

    if bounded and symbol != "SPY":
        with pytest.raises(ValueError, match="exact SPY/AM"):
            script._load_runtime_scope(provider, plan.plan_id, catalog)
        return
    (
        loaded_plan,
        loaded_profile,
        manifest,
        admissions,
        symbols,
    ) = script._load_runtime_scope(
        provider,
        plan.plan_id,
        catalog,
    )

    assert loaded_plan is plan
    assert loaded_profile.sha256 == profile.sha256
    assert tuple(member.channel.value for member in manifest.members) == ("AM",)
    assert tuple(admissions) == ("SPY",)
    assert symbols == ("SPY",)
