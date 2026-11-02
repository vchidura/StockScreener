import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime.advanced_realtime_session_operations import (
    AdvancedSessionOperation,
    build_diagnostic_authorization,
    diagnostic_authorization_payload,
)
from scripts import run_realtime_v2_canonical_stock_alerts as cli


DATABASE = "stocks_realtime_v2"
COHORT = "11111111-2222-3333-4444-555555555555"


def _base():
    return [
        "--database",
        DATABASE,
        "--confirm-database-name",
        DATABASE,
        "--cohort-id",
        COHORT,
        "--session-date",
        "2026-10-29",
        "--operation",
        "PRIMARY_SERVING",
    ]


def test_canonical_stock_alert_resident_requires_explicit_modes():
    run = cli.parse_args([
        *_base(),
        "--continuous",
        "--execute",
    ])
    health = cli.parse_args([*_base(), "--health"])

    assert run.continuous is True and run.execute is True
    assert health.health is True
    assert run.operation is AdvancedSessionOperation.PRIMARY_SERVING
    with pytest.raises(SystemExit):
        cli.parse_args(_base())
    with pytest.raises(SystemExit):
        cli.parse_args([
            *_base(),
            "--health",
            "--execute",
        ])


def test_diagnostic_resident_requires_owned_non_serving_identity(
    tmp_path,
):
    authorization = build_diagnostic_authorization(
        authorization_reference="diagnostic-test",
        owner="test-owner",
        session_date=date(2026, 10, 29),
        cohort_id=UUID(COHORT),
        source_sha256="a" * 64,
        backend_image_id=f"sha256:{'b' * 64}",
        input_evidence_sha256="c" * 64,
    )
    path = tmp_path / "diagnostic.json"
    path.write_text(
        json.dumps(diagnostic_authorization_payload(authorization)),
        encoding="utf-8",
    )
    diagnostic = cli.parse_args([
        *_base()[:-2],
        "--operation",
        "DIAGNOSTIC_NON_SERVING",
        "--diagnostic-authorization",
        str(path),
        "--continuous",
        "--execute",
    ])

    assert diagnostic.diagnostic_identity == authorization
    with pytest.raises(SystemExit):
        cli.parse_args([
            *_base()[:-2],
            "--operation",
            "DIAGNOSTIC_NON_SERVING",
            "--continuous",
            "--execute",
        ])


def test_primary_health_waits_for_first_e(monkeypatch, capsys):
    class Transaction:
        def __enter__(self):
            return SimpleNamespace(cursor=object())

        def __exit__(self, *_args):
            return False

    provider = SimpleNamespace(
        transaction=lambda *_args, **_kwargs: Transaction()
    )
    monkeypatch.setattr(
        cli,
        "canonical_stock_alert_health_cursor",
        lambda *_args, **_kwargs: {
            "status": "READY",
            "reason": None,
            "completed_batches": 0,
            "latest_completed_at": None,
        },
    )
    args = cli.parse_args([*_base(), "--health"])

    assert cli._health(provider, args) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["status"] == "READY_WAITING_FOR_E"
    assert status["reason"] == "INPUT_READY_UNAVAILABLE"


def test_compose_owns_canonical_service_without_legacy_sources():
    compose = (
        Path(__file__).resolve().parents[3]
        / "docker-compose.realtime-v2.yml"
    ).read_text(encoding="utf-8")
    start = compose.index("  realtime-v2-canonical-stock-alerts:")
    end = compose.index(
        "\n  realtime-v3-options-lifecycle:",
        start,
    )
    block = compose[start:end]

    assert "run_realtime_v2_canonical_stock_alerts.py" in block
    assert "--health" in block
    assert block.count("PRIMARY_SERVING") == 2
    assert "labels: *downstream-labels" in block
    assert "restart: unless-stopped" in block
    assert "--dynamic-scope" in block
    assert "--cohort-id" not in block
    assert "--session-date" not in block
    assert "project_stock_alert_results.py" not in block
    assert "stock-ideas-forward" not in block
    assert "stock-ideas-swing" not in block
