from __future__ import annotations

import json
import re
import threading
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from realtime.retention_maintenance import HEALTH_BLOCKED, STATUS_READY
from scripts import run_realtime_v2_retention_service as script

UTC = timezone.utc
NOW = datetime(2026, 10, 17, 20, 0, tzinfo=UTC)


class Calendar:
    def latest_completed_session(self, checked_at):
        assert checked_at == NOW
        return date(2026, 10, 16)


def base(tmp_path: Path) -> list[str]:
    return [
        "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2",
        "--archive-root", str((tmp_path / "archive").resolve()),
        "--status-path", str((tmp_path / "status.json").resolve()),
        "--partition-relation", "rt.portal_snapshot_hot",
        "--partition-relation", "rt.option_flow_clusters",
        "--retention-relation", "rt.raw_payloads",
        "--archive-relation", "rt.option_flow_clusters",
        "--archive-kind", "OPTION_FLOW_CLUSTERS",
        "--archive-build-id", "retention-build-v1",
        "--archive-policy-id", "retention-policy-v1",
    ]


def execution(tmp_path: Path, *mode: str) -> list[str]:
    return [
        *base(tmp_path),
        *mode,
        "--execute",
        "--app-dsn-file", str((tmp_path / "app-dsn").resolve()),
        "--admin-dsn-file", str((tmp_path / "admin-dsn").resolve()),
        "--authorization-reference", "owner-approved-retention-runtime",
    ]


def check(tmp_path: Path) -> list[str]:
    return [
        *base(tmp_path),
        "--check",
        "--app-dsn-file", str((tmp_path / "app-dsn").resolve()),
        "--admin-dsn-file", str((tmp_path / "admin-dsn").resolve()),
        "--authorization-reference", "owner-approved-retention-preflight",
    ]


def test_repository_root_supports_flat_container_layout(tmp_path, monkeypatch):
    container_backend = tmp_path / "app"
    container_backend.mkdir()
    repository_root = script._repository_root(container_backend)
    monkeypatch.setattr(script, "REPOSITORY_ROOT", repository_root)

    external_status = (tmp_path / "retention" / "status.json").resolve()
    assert repository_root == container_backend.resolve()
    assert script._outside_repository(external_status, "status_path") == external_status
    with pytest.raises(ValueError, match="must be outside the repository"):
        script._outside_repository(container_backend / "status.json", "status_path")


def test_plan_is_default_and_never_reads_secrets_or_connects(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(script, "_now", lambda: NOW)
    monkeypatch.setattr(script, "OptionExchangeCalendar", Calendar)
    monkeypatch.setattr(
        script,
        "_read_secret",
        lambda *_: (_ for _ in ()).throw(AssertionError("secret read")),
    )
    monkeypatch.setattr(
        script.psycopg2,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("database connection")),
    )

    assert script.main(base(tmp_path)) == 0

    output = capsys.readouterr().out
    payload = json.loads(output[:output.rindex("}") + 1])
    assert payload["mode"] == "PLAN"
    assert not payload["database_changes"] and payload["provider_requests"] == 0
    assert payload["request"] == {
        "as_of_date": "2026-10-17",
        "as_of_session": "2026-10-16",
        "archive_session_dates": ["2026-10-16"],
        "partition_relations": [
            "rt.option_flow_clusters",
            "rt.portal_snapshot_hot",
        ],
        "retention_relations": ["rt.raw_payloads"],
        "archive_relations": ["rt.option_flow_clusters"],
        "archive_kinds": ["OPTION_FLOW_CLUSTERS"],
    }


def test_cli_requires_exact_production_scope_and_execute_gates(tmp_path):
    relative = base(tmp_path)
    relative[relative.index("--archive-root") + 1] = "relative/archive"
    with pytest.raises(SystemExit):
        script.parse_args(relative)
    missing_build_identity = base(tmp_path)
    missing_build_identity[missing_build_identity.index("--archive-build-id") + 1] = ""
    with pytest.raises(SystemExit):
        script.parse_args(missing_build_identity)
    with pytest.raises(SystemExit):
        script.parse_args([
            "--database", "test_realtime_v2_x",
            "--confirm-database-name", "test_realtime_v2_x",
            "--status-path", str((tmp_path / "status.json").resolve()),
            "--status",
        ])
    with pytest.raises(SystemExit):
        script.parse_args([*base(tmp_path), "--once"])
    with pytest.raises(SystemExit):
        script.parse_args([*base(tmp_path), "--execute"])
    with pytest.raises(SystemExit):
        script.parse_args([*base(tmp_path), "--check"])
    with pytest.raises(SystemExit):
        script.parse_args([
            *base(tmp_path),
            "--once",
            "--execute",
            "--authorization-reference", "approved",
            "--app-dsn-file", str((tmp_path / "same").resolve()),
            "--admin-dsn-file", str((tmp_path / "same").resolve()),
        ])


def test_status_mode_is_file_only_and_fails_closed_when_missing(tmp_path):
    args = script.parse_args([
        "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2",
        "--status-path", str((tmp_path / "missing.json").resolve()),
        "--status",
    ])
    payload, code = script._status(args, NOW)
    assert code == 1
    assert payload["state"] == HEALTH_BLOCKED
    assert payload["reason_codes"] == ["RETENTION_STATUS_MISSING"]
    assert not payload["database_access"] and payload["provider_requests"] == 0


def test_dsn_secret_must_explicitly_target_production(tmp_path):
    good = tmp_path / "good"
    bad = tmp_path / "bad"
    good.write_text("dbname=stocks_realtime_v2 user=bootstrap", encoding="utf-8")
    bad.write_text("dbname=postgres user=bootstrap", encoding="utf-8")
    assert "stocks_realtime_v2" in script._validated_dsn(
        good, "stocks_realtime_v2", "app_dsn"
    )
    with pytest.raises(RuntimeError, match="explicitly target"):
        script._validated_dsn(bad, "stocks_realtime_v2", "admin_dsn")


class Cursor:
    def __init__(self, identity):
        self.identity = identity
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))

    def fetchone(self):
        return self.identity


class Connection:
    def __init__(self, identity):
        self.cursor_instance = Cursor(identity)
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def test_role_connection_factory_verifies_database_role_and_security_phase():
    connection = Connection((
        "stocks_realtime_v2",
        "stocks_realtime_v2_admin",
        "AUDITED",
    ))
    calls = []

    def connect(*args, **kwargs):
        calls.append((args, kwargs))
        return connection

    factory = script.RoleConnectionFactory(
        "dbname=stocks_realtime_v2 user=bootstrap",
        "stocks_realtime_v2",
        "admin",
        "retention-test",
        connector=connect,
    )
    assert factory() is connection
    assert connection.autocommit is False
    assert calls[0][1]["application_name"] == "retention-test"
    assert len(connection.cursor_instance.statements) == 2


def test_role_connection_factory_fails_closed_on_identity_mismatch():
    connection = Connection((
        "stocks_realtime_v2",
        "stocks_realtime_v2_app",
        "APPLIED",
    ))
    factory = script.RoleConnectionFactory(
        "dbname=stocks_realtime_v2 user=bootstrap",
        "stocks_realtime_v2",
        "admin",
        "retention-test",
        connector=lambda *_args, **_kwargs: connection,
    )
    with pytest.raises(RuntimeError, match="identity/security"):
        factory()
    assert connection.closed


def test_once_runs_one_exact_cycle_and_emits_status(tmp_path):
    args = script.parse_args(execution(tmp_path, "--once"))
    calls = []

    class Cycle:
        def run(self, request, *, execute):
            calls.append((request, execute))
            return SimpleNamespace(
                state=STATUS_READY,
                as_dict=lambda: {"state": STATUS_READY},
            )

    output = []
    assert script.run_cycles(
        Cycle(),
        args,
        stop=threading.Event(),
        clock=lambda: NOW,
        calendar=Calendar(),
        emit=output.append,
    ) == 0
    [(request, execute)] = calls
    assert execute
    assert request.as_of_session == date(2026, 10, 16)
    assert request.partition_relations == (
        "rt.option_flow_clusters",
        "rt.portal_snapshot_hot",
    )
    assert request.retention_relations == ("rt.raw_payloads",)
    assert request.archive_relations == ("rt.option_flow_clusters",)
    assert request.archive_kinds == ("OPTION_FLOW_CLUSTERS",)
    assert json.loads(output[0]) == {"state": STATUS_READY}


def test_check_preflight_is_read_only_and_reports_exact_candidates(tmp_path, monkeypatch):
    args = script.parse_args(check(tmp_path))
    connections = []

    class Connection:
        def close(self):
            connections.append("closed")

    connect = lambda: Connection()  # noqa: E731

    @contextmanager
    def leadership(_):
        yield

    monkeypatch.setattr(script, "retention_leadership", leadership)
    monkeypatch.setattr(
        script,
        "_read_partition_state",
        lambda *_: (
            SimpleNamespace(as_dict=lambda: {
                "as_of_date": "2026-10-17",
                "would_create": 0,
                "partitions": [],
            }),
            (),
        ),
    )

    class Retention:
        ok = True
        all_alarms = ()
        would_drop = (SimpleNamespace(
            relation="rt.raw_payloads",
            partition="raw_payloads_p20260901",
        ),)
        catalogs = (SimpleNamespace(
            catalog="rt.snapshot_catalog",
            before=date(2026, 10, 15),
        ),)

        def run(self, **options):
            assert options == {
                "execute": False,
                "as_of_date": date(2026, 10, 17),
                "relations": ("rt.raw_payloads",),
            }
            return self

        def as_dict(self):
            return {"mode": "DRY_RUN", "would_drop": 1}

    class Archive:
        ok = True

        def plan_archived_partitions(self, **options):
            assert options == {
                "as_of_session": date(2026, 10, 16),
                "relations": ("rt.option_flow_clusters",),
                "health_date": date(2026, 10, 17),
            }
            return self

        def as_dict(self):
            return {"candidate_partitions": 1}

        def plan_session_archive(self, session_date, kinds):
            assert session_date == date(2026, 10, 16)
            assert kinds == ("OPTION_FLOW_CLUSTERS",)
            return SimpleNamespace(
                ok=True,
                as_dict=lambda: {"session_date": session_date.isoformat()},
            )

    class ImpactRepository:
        def partition_impact(self, partition):
            assert partition.partition == "raw_payloads_p20260901"
            return SimpleNamespace(
                as_dict=lambda: {
                    "partition": partition.partition,
                    "rows": 3,
                    "total_bytes": 8192,
                },
            )

        def catalog_prune_impact(self, catalog, rule, before):
            assert catalog == "rt.snapshot_catalog"
            assert before == datetime(2026, 10, 15, tzinfo=UTC)
            return SimpleNamespace(
                as_dict=lambda: {
                    "catalog": catalog,
                    "rows": 2,
                    "direct_cascades": [],
                },
            )

    payload, code = script.run_preflight(
        args,
        connect,
        connect,
        checked_at=NOW,
        calendar=Calendar(),
        retention=Retention(),
        archive=Archive(),
        impact_repository=ImpactRepository(),
    )
    assert code == 0 and payload["ok"]
    assert payload["ordinary_retention"]["would_drop"] == 1
    assert payload["archive_retention"]["candidate_partitions"] == 1
    assert payload["ordinary_impact"]["partitions"][0]["rows"] == 3
    assert payload["ordinary_impact"]["catalogs"][0]["rows"] == 2
    assert payload["session_archive"]["session_date"] == "2026-10-16"
    assert payload["partition_creation"]["would_create"] == 0
    assert not payload["database_changes"]
    assert payload["archive_writes"] == payload["provider_requests"] == 0
    assert len(connections) == 2


def test_retention_compose_service_is_isolated_disabled_and_fail_closed():
    root = Path(__file__).resolve().parents[3]
    text = (root / "docker-compose.realtime-v2.yml").read_text(encoding="utf-8")
    match = re.search(
        r"^  realtime-v2-retention:\n(?P<body>.*?)(?=^  [a-z][^\n]*:\n|\Z)",
        text,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match is not None
    block = match.group("body")
    assert 'profiles: ["realtime-v2-retention"]' in block
    assert "scripts/run_realtime_v2_retention_service.py" in block
    assert "realtime_v2_app_dsn" in block
    assert "realtime_v2_admin_dsn" in block
    assert "realtime_v2_provider_key" not in block
    assert "REALTIME_V2_RETENTION_AUTHORIZATION_REFERENCE:-}" in block
    assert "REALTIME_V2_RETENTION_ARCHIVE_BUILD_ID:-}" in block
    assert "retention-archive-v1" not in block
    assert "/var/lib/realtime-v2-retention" in block
    assert (
        "  realtime_v2_retention:\n"
        "    name: ${REALTIME_V2_RETENTION_VOLUME_NAME:-"
        "alpha-research-realtime-v2-retention-volume-name-required}\n"
        "    external: true"
        in text
    )
    assert "--continuous" in block and "--execute" in block
    assert block.count("--partition-relation") == 16
    assert "--status" in block and "healthcheck:" in block
    assert "env_file:" not in block
