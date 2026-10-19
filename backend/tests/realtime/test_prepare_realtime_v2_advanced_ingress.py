import importlib
import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime.advanced_ingress_plan import (
    AdvancedIngressBudgets,
    build_advanced_ingress_plan,
    build_advanced_stream_authorization,
)


script = importlib.import_module(
    "scripts.prepare_realtime_v2_advanced_ingress"
)
UTC = timezone.utc


class _Calendar:
    def session_open(self, _session_date):
        return datetime(2026, 10, 16, 13, 30, tzinfo=UTC)

    def session_close(self, _session_date):
        return datetime(2026, 10, 16, 20, 0, tzinfo=UTC)


class _Provider:
    @contextmanager
    def transaction(self, role, *, read_only=False):
        assert role == "app"
        assert read_only
        yield SimpleNamespace(cursor=object())


class _Pool:
    def __init__(self):
        self.closed = False

    def closeall(self):
        self.closed = True


def _plan(*, target=9, **window):
    budgets = AdvancedIngressBudgets(2, 2, 10_000, 0, 0)
    authorization = build_advanced_stream_authorization(
        parent_authorization_id=None,
        allowed_universe_targets=(target,),
        valid_from=date(2026, 10, 16),
        valid_through=date(2026, 10, 16),
        maximum_budgets=budgets,
        runtime_policy_sha256="a" * 64,
        approval_source="operator",
        approval_reference="owner-ticket",
    )
    return build_advanced_ingress_plan(
        authorization=authorization,
        cohort_id=UUID("11111111-1111-1111-1111-111111111111"),
        manifest_id=UUID("22222222-2222-2222-2222-222222222222"),
        session_date=date(2026, 10, 16),
        universe_target=target,
        runtime_profile_version=f"advanced-equity-ingress-{target}-v1",
        runtime_profile_sha256="b" * 64,
        configuration_sha256="c" * 64,
        budgets=budgets,
        calendar=_Calendar(),
        **window,
    )


@pytest.mark.parametrize("bounded", [False, True])
def test_prepare_cli_emits_redacted_provider_free_plan(
    tmp_path,
    monkeypatch,
    bounded,
):
    env_file = tmp_path / ".env"
    policy = (
        script.BACKEND_DIR
        / "realtime"
        / "policies"
        / "advanced_equity_ingress_v1.json"
    )
    env_file.write_text(
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_PATH="
        f"{policy}\n"
        "REALTIME_V2_ADVANCED_INGRESS_POLICY_VERSION="
        "advanced-equity-ingress-v1\n",
        encoding="utf-8",
    )
    report_path = tmp_path / "plan.json"
    pool = _Pool()
    monkeypatch.setattr(
        script,
        "_app_provider",
        lambda _settings, *, database: (_Provider(), pool),
    )
    captured = []

    def resolve(_cursor, request, *, runtime_catalog):
        captured.append(request)
        return _plan(
            target=request.universe_target,
            canary_start_at=request.canary_start_at,
            canary_end_at=request.canary_end_at,
        )

    monkeypatch.setattr(script, "resolve_advanced_ingress_plan_cursor", resolve)
    monkeypatch.setattr(
        script.psycopg2,
        "connect",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("prepare must not use the admin/provider connection")
        ),
    )

    result = script.main([
        "prepare",
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--env-file",
        str(env_file),
        "--report-path",
        str(report_path),
        "--authorization-id",
        "33333333-3333-3333-3333-333333333333",
        "--session-date",
        "2026-10-16",
        "--universe-target",
        "9",
        "--stream-sessions",
        "2",
        "--connection-attempts",
        "2",
        "--decoded-bytes",
        "10000",
        "--rest-repair-requests",
        "0",
        "--rest-repair-decoded-bytes",
        "0",
        *([
            "--universe-target", "1",
            "--canary-start-at", "2026-10-16T13:30:00+00:00",
            "--canary-end-at", "2026-10-16T13:40:00+00:00",
        ] if bounded else []),
    ])

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert result == 0
    assert report["provider_requests"] == 0
    assert report["credentials_exposed"] is False
    assert report["manifest_id"] == (
        "22222222-2222-2222-2222-222222222222"
    )
    assert "approval_reference" not in report
    assert "dsn" not in json.dumps(report).lower()
    assert pool.closed
    if bounded:
        assert captured[0].canary_end_at.isoformat() == report["authorized_end_at"]
        assert report["authorized_end_at"] == "2026-10-16T13:40:00+00:00"


@pytest.mark.parametrize("target", [None, 1, 9])
def test_sync_profiles_uses_explicit_policy_without_editing_shared_environment(tmp_path, monkeypatch, target):
    env_file = tmp_path / ".env"
    env_file.write_text("UNCHANGED=value\n", encoding="utf-8")
    policy = script.BACKEND_DIR / "realtime" / "policies" / "advanced_equity_ingress_v1.json"
    report_path = tmp_path / "profiles.json"
    monkeypatch.setattr(script, "_admin_cursor", lambda *_args, **_kwargs: pytest.fail("PLAN must not connect"))
    args = [
        "sync-profiles", "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2", "--env-file", str(env_file),
        "--ingress-policy-path", str(policy),
        "--ingress-policy-version", "advanced-equity-ingress-v1",
        "--report-path", str(report_path),
    ]
    if target is not None:
        args.extend(("--universe-target", str(target)))
    assert script.main(args) == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["mode"] == "PLAN"
    assert report["provider_requests"] == 0
    assert all(row["status"] == "CANDIDATE" for row in report["profiles"])
    assert env_file.read_text(encoding="utf-8") == "UNCHANGED=value\n"
    if target is not None:
        assert [row["universe_target"] for row in report["profiles"]] == [target]
    else:
        assert [row["universe_target"] for row in report["profiles"]] == [1, 9, 25, 50, 100, 200, 400]


def test_sync_selected_profile_keeps_its_original_ladder_position(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    policy = script.BACKEND_DIR / "realtime" / "policies" / "advanced_equity_ingress_v1.json"
    seen = []

    @contextmanager
    def admin(*_args, **_kwargs):
        yield object()

    def register(_cursor, profile, **scope):
        seen.append((profile.universe_target, scope))
        return SimpleNamespace(status="CANDIDATE")

    monkeypatch.setattr(script, "_admin_cursor", admin)
    monkeypatch.setattr(script, "register_advanced_runtime_profile_cursor", register)
    assert script.main([
        "sync-profiles", "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2", "--env-file", str(env_file),
        "--ingress-policy-path", str(policy),
        "--ingress-policy-version", "advanced-equity-ingress-v1",
        "--universe-target", "9", "--report-path", str(tmp_path / "selected.json"), "--execute",
    ]) == 0
    assert len(seen) == 1
    assert seen[0][0] == 9
    assert seen[0][1]["ladder_position"] == 1
    assert seen[0][1]["previous_universe_target"] == 1
