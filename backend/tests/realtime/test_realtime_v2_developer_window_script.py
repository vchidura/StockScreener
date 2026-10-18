import importlib
import json
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

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


def test_projection_only_continuity_omits_portal_publish():
    args = script.parse_args([
        *BASE,
        "--execute",
        "--authorization-reference",
        "approved-projection-only",
        "--portal-reprojection",
        "--projection-only",
        "--allow-late-identity-continuity",
        "--window-key-suffix=-portal-correction-continuity",
    ])

    assert args.projection_only
    assert args.allow_late_identity_continuity
    assert "PORTAL_PUBLISH" not in script.PROJECTION_ONLY_STAGES
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--projection-only"])


def test_publication_only_requires_an_exact_projection_source_and_recovery_suffix():
    source_window_id = uuid4()
    args = script.parse_args([
        *BASE,
        "--portal-reprojection",
        "--publication-only",
        "--publication-source-window-id",
        str(source_window_id),
        "--allow-late-identity-continuity",
        "--window-key-suffix=-portal-publish-recovery1",
    ])

    assert args.publication_only
    assert args.publication_source_window_id == source_window_id
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--portal-reprojection",
            "--publication-only",
            "--window-key-suffix=-portal-publish-recovery1",
        ])
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE,
            "--portal-reprojection",
            "--publication-only",
            "--publication-source-window-id",
            str(source_window_id),
            "--window-key-suffix=-portal-correction-invalid",
        ])


def test_publication_recovery_state_binds_exact_receipts_and_pointer_effects():
    cohort_id = UUID("d1baee6f-4ab2-543c-8a86-ce10f8952e69")
    source_window_id = uuid4()
    cutoff = script.datetime.fromisoformat(
        "2026-10-16T20:00:00+00:00"
    )
    provider = MagicMock()
    cursor = provider.transaction.return_value.__enter__.return_value.cursor
    cursor.fetchone.side_effect = [
        {
            "window_id": source_window_id,
            "window_key": cutoff.isoformat() + "-portal-correction-p8-retry2",
            "state": "COMPLETE",
            "provider_tier": "MASSIVE_DEVELOPER",
            "cutoff": cutoff,
            "deadline": cutoff + script.timedelta(minutes=10),
            "plan_id": uuid4(),
            "session_date": cutoff.date(),
            "stage_catalog_version": script.STAGE_CATALOG_VERSION_V2,
            "enabled_stages": list(script.PROJECTION_ONLY_STAGES),
        },
        {
            "release_id": uuid4(),
            "cutoff": cutoff - script.timedelta(days=1),
            "session_date": (cutoff - script.timedelta(days=1)).date(),
            "updated_at": cutoff,
        },
        {
            "scope": "PLATFORM",
            "cohort_id": cohort_id,
            "provider_tier": "MASSIVE_DEVELOPER",
            "activation_id": uuid4(),
            "effective_at": cutoff,
            "updated_at": cutoff,
        },
        {
            "snapshot_catalog": 30,
            "snapshot_hot": 20,
            "snapshot_kept": 10,
            "releases": 4,
        },
        {
            "total_requests": 50,
            "total_response_bytes": 2603113,
            "cohort_requests": 50,
            "cohort_response_bytes": 2603113,
        },
    ]
    cursor.fetchall.side_effect = [
        [
            {"stage_id": "EQUITY_MATERIALIZE", "state": "COMPLETE", "attempt": 1},
            {"stage_id": "EQUITY_PROJECTION", "state": "COMPLETE", "attempt": 1},
        ],
        [
            {
                "receipt_id": uuid4(),
                "receipt_kind": "INPUT_READY",
                "cohort_id": cohort_id,
                "cutoff": cutoff,
                "reference_generation": 4,
                "universe_sha256": "a" * 64,
                "input_sha256": "b" * 64,
                "build_id": script.EQUITY_WINDOW_BUILD_ID,
            },
            {
                "receipt_id": uuid4(),
                "receipt_kind": "PORTAL_READY",
                "cohort_id": cohort_id,
                "cutoff": cutoff,
                "reference_generation": 4,
                "universe_sha256": "a" * 64,
                "input_sha256": "c" * 64,
                "build_id": script.EQUITY_WINDOW_BUILD_ID,
            },
        ],
    ]

    state = script._publication_recovery_state(
        provider,
        cohort_id=cohort_id,
        cutoff=cutoff,
        source_window_id=source_window_id,
    )
    effects = script._publication_effects(
        state,
        cutoff=cutoff,
        session_close=True,
    )

    assert set(state["receipts"]) == {"INPUT_READY", "PORTAL_READY"}
    assert effects["release_classes"] == ["INTRADAY", "SESSION_CLOSE"]
    assert effects["platform_pointer"]["effect"] == "UNCHANGED"
    provider.transaction.assert_called_once_with(
        read_only=True,
        dict_cursor=True,
    )


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
