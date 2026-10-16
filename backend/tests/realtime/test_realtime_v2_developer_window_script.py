import importlib
import json
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from realtime.orchestration_repository import CycleReceiptSpec

script = importlib.import_module("scripts.run_realtime_v2_developer_window")
BASE = [
    "--database", "stocks_realtime_v2",
    "--confirm-database-name", "stocks_realtime_v2",
    "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
    "--window-start", "2026-10-15T13:30:00+00:00",
    "--cutoff", "2026-10-15T16:00:00+00:00",
    "--cache-directory", "backend\\.realtime-canary\\developer-window-cache",
    "--report-path", "backend\\.realtime-canary\\developer-window-report.json",
]


def test_developer_window_requires_exact_authorized_execution():
    assert not script.parse_args(BASE).execute
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--execute"])
    assert script.parse_args([
        *BASE,
        "--execute",
        "--authorization-reference",
        "approved",
    ]).execute


def test_developer_window_rejects_unaligned_cutoff():
    drifted = list(BASE)
    drifted[9] = "2026-10-15T16:01:00+00:00"
    with pytest.raises(SystemExit):
        script.parse_args(drifted)


def test_portal_reprojection_is_provider_free_and_scope_gated():
    args = script.parse_args([
        *BASE,
        "--execute",
        "--authorization-reference",
        "approved-correction",
        "--portal-reprojection",
        "--window-key-suffix=-portal-correction-approved",
    ])

    assert args.portal_reprojection
    assert not args.reuse_current_input
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--portal-reprojection",
            "--window-key-suffix=-receipt-retry1",
        ])
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--portal-reprojection",
            "--reuse-current-input",
            "--window-key-suffix=-portal-correction-invalid",
        ])
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--portal-reprojection",
            "--window-key-suffix=-portal-correction-" + "x" * 40,
        ])


@pytest.mark.parametrize("has_receipt", (False, True))
def test_receipt_only_recovery_needs_no_provider_key_or_client(monkeypatch, tmp_path, has_receipt):
    args = list(BASE)
    args[args.index("--report-path") + 1] = str(tmp_path / "report.json")
    args += [
        "--execute", "--authorization-reference", "approved",
        "--reuse-current-input", "--window-key-suffix=-receipt-retry1",
    ]
    pool, provider = MagicMock(), MagicMock()
    cursor = provider.transaction.return_value.__enter__.return_value.cursor
    cursor.fetchone.side_effect = [
        ("COMPLETE",), {"receipts": 1, "generations": 1, "current_releases": 0},
    ]
    cursor.fetchall.return_value = [(str(index), "COMPLETE") for index in range(6)]
    monkeypatch.setenv("REALTIME_V2_APP_DSN", "fixture")
    monkeypatch.setenv("POLYGON_API_KEY", "")
    monkeypatch.setattr(script, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(script, "_pool", lambda _: pool)
    monkeypatch.setattr(script, "V2ConnectionProvider", lambda *args: provider)
    sealed_receipt = CycleReceiptSpec(
        "INPUT_READY",
        script.datetime.fromisoformat("2026-10-15T16:00:00+00:00"),
        7,
        "a" * 64,
        "b" * 64,
        script.EQUITY_WINDOW_BUILD_ID,
    )
    monkeypatch.setattr(script, "equity_window_recovery_cursor", lambda *args: {
        "has_input_receipt": has_receipt,
        "input_receipt": sealed_receipt if has_receipt else None,
        "input_known_by": (
            script.datetime.fromisoformat("2026-10-15T16:10:00+00:00")
            if has_receipt else None
        ),
    })
    monkeypatch.setattr(script, "_load_scope", lambda *args: ((), object()))
    monkeypatch.setattr(script, "RunnerEnvironment", lambda *args, **kwargs: SimpleNamespace(**kwargs))
    repository = MagicMock()
    repository.create_plan.return_value = uuid4()
    monkeypatch.setattr(script, "PostgresOrchestrationRepository", lambda *args: repository)
    runners = {
        stage: MagicMock() for stage in script.ENABLED_STAGES
    }
    monkeypatch.setattr(script, "build_runners", lambda *args, **kwargs: runners)
    monkeypatch.setattr(script, "StageOrchestrator", lambda *args, **kwargs: MagicMock())

    @dataclass
    class Outcome:
        status: str = "COMPLETE"
        executed: tuple = ()

    service = MagicMock()
    service.tick_resident_once.return_value = "COMPLETE"
    service.poll_windows.return_value = SimpleNamespace(window_errors=(), windows=(Outcome(),))
    monkeypatch.setattr(script, "V2Service", lambda *args: service)
    transport = MagicMock()
    monkeypatch.setattr(script, "RequestsPolygonHttpTransport", transport)
    if has_receipt:
        assert script.main(args) == 0
        report = json.loads((tmp_path / "report.json").read_text())
        assert report["provider_requests"] == report["provider_response_bytes"] == 0
        reused = runners["EQUITY_MATERIALIZE"](SimpleNamespace())
        assert reused.receipt == sealed_receipt
    else:
        with pytest.raises(ValueError, match="matching durable"):
            script.main(args)
    transport.assert_not_called()
    pool.closeall.assert_called_once()
