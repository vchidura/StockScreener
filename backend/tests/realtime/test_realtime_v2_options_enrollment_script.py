from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from realtime.contracts import DEVELOPER_FORWARD_UNIVERSE
from scripts import run_realtime_v2_options_enrollment as script


def _args(report, underlyings, *, execute=False):
    now = datetime.now(timezone.utc)
    values = [
        "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2",
        "--session-date", now.date().isoformat(),
        "--deadline", (now + timedelta(minutes=4)).isoformat(),
        "--authorized-underlyings", *underlyings,
        "--authorization-reference", "test-authorization",
        "--report-path", str(report),
    ]
    if execute:
        values.append("--execute")
    return values


def test_authorized_universe_mismatch_blocks_before_environment_database_or_provider(
    tmp_path, monkeypatch
):
    report = tmp_path / "blocked.json"
    monkeypatch.setattr(
        script,
        "_configuration_environment",
        lambda: (_ for _ in ()).throw(AssertionError("environment must not load")),
    )
    monkeypatch.setattr(
        script,
        "_pool",
        lambda: (_ for _ in ()).throw(AssertionError("database must not open")),
    )
    underlyings = tuple(
        "META" if symbol == "TSLA" else symbol
        for symbol in DEVELOPER_FORWARD_UNIVERSE.option_underlyings
    )
    assert script.main(_args(report, underlyings, execute=True)) == 2
    stored = json.loads(report.read_text(encoding="utf-8"))
    assert stored["status"] == "BLOCKED"
    assert stored["reason_codes"] == ["AUTHORIZED_UNIVERSE_IDENTITY_MISMATCH"]
    assert stored["provider_requests"] == 0
    assert stored["response_bytes"] == 0
    assert stored["database_writes"] is False


def test_plan_retains_the_exact_canonical_scope_without_opening_runtime(tmp_path, monkeypatch):
    report = tmp_path / "plan.json"
    monkeypatch.setattr(
        script,
        "_pool",
        lambda: (_ for _ in ()).throw(AssertionError("plan must not open the database")),
    )
    assert script.main(
        _args(report, DEVELOPER_FORWARD_UNIVERSE.option_underlyings)
    ) == 0
    stored = json.loads(report.read_text(encoding="utf-8"))
    assert stored["status"] == "PLAN"
    assert tuple(stored["canonical_underlyings"]) == (
        DEVELOPER_FORWARD_UNIVERSE.option_underlyings
    )
