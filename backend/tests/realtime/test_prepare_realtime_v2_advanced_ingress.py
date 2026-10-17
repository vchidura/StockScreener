import importlib
import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import UUID

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


def _plan():
    budgets = AdvancedIngressBudgets(2, 2, 10_000, 0, 0)
    authorization = build_advanced_stream_authorization(
        parent_authorization_id=None,
        allowed_universe_targets=(9,),
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
        universe_target=9,
        runtime_profile_version="advanced-equity-ingress-9-v1",
        runtime_profile_sha256="b" * 64,
        configuration_sha256="c" * 64,
        budgets=budgets,
        calendar=_Calendar(),
    )


def test_prepare_cli_emits_redacted_provider_free_plan(
    tmp_path,
    monkeypatch,
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
    monkeypatch.setattr(
        script,
        "resolve_advanced_ingress_plan_cursor",
        lambda _cursor, _request, *, runtime_catalog: _plan(),
    )
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
