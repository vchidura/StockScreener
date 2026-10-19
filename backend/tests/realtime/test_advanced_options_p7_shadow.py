from __future__ import annotations

import hashlib
import importlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from realtime.advanced_options_p7_shadow import (
    APPROVAL_VERSION,
    EVIDENCE_FILES,
    OWNER_DECISION,
    approved_authorization,
    build_authorization,
    provider_free_preflight,
    validate_authorization,
)


UTC = timezone.utc
START = datetime(2026, 10, 20, 13, 35, tzinfo=UTC)
RUN_ID = UUID("77777777-7777-7777-7777-777777777777")
HEAD = "89b6c1b142b411cc57c519ce2de57543311a1b48"
SPY = "O:SPY261120C00500000"
QQQ = "O:QQQ261120C00500000"
BACKEND_DIR = Path(__file__).resolve().parents[2]
script = importlib.import_module("scripts.prepare_advanced_options_p7_shadow")


def _plan(tmp_path, **changes):
    values = {
        "run_id": RUN_ID,
        "authorization_reference": (
            "ADVANCED-OPTIONS-P7-SHADOW-20261020-01"
        ),
        "baseline_head": HEAD,
        "start_at": START,
        "controlled_reconnect_at": START + timedelta(minutes=5),
        "stop_at": START + timedelta(minutes=10),
        "spy_contract": SPY,
        "qqq_contract": QQQ,
        "backend_dir": BACKEND_DIR,
        "artifact_root": tmp_path,
    }
    values.update(changes)
    return build_authorization(**values)


def _approved_files(tmp_path, plan):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(plan, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    body = manifest.read_bytes()
    approval = tmp_path / "approval.json"
    approval.write_text(
        json.dumps(
            {
                "contract_version": APPROVAL_VERSION,
                "manifest_id": plan["manifest_id"],
                "manifest_sha256": hashlib.sha256(body).hexdigest(),
                "authorization_reference": plan[
                    "authorization_reference"
                ],
                "owner_decision": OWNER_DECISION,
                "conditional_live_shadow_authorized": True,
                "provider_websocket_connection_attempts_authorized": 2,
                "provider_rest_requests_authorized": 0,
                "production_effects_authorized": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return manifest, approval


def test_plan_freezes_exact_bounded_non_serving_scope(tmp_path):
    plan = _plan(tmp_path)

    assert plan["provider"]["endpoint"] == (
        "wss://socket.massive.com/options"
    )
    assert plan["provider"]["channels"] == ["T", "Q"]
    assert plan["provider"]["rest_requests_authorized"] == 0
    assert plan["scope"]["underlyings"] == ["SPY", "QQQ"]
    assert plan["scope"]["subscriptions"] == [
        f"T.{SPY}",
        f"Q.{SPY}",
        f"T.{QQQ}",
        f"Q.{QQQ}",
    ]
    assert plan["socket_ceiling"]["connection_attempts"] == 2
    assert plan["socket_ceiling"]["reconnects"] == 1
    assert plan["database"]["name"] == (
        "test_realtime_v2_options_p7_20261020"
    )
    assert plan["database"]["roles"] == [
        "test_rt_options_p7_20261020_admin",
        "test_rt_options_p7_20261020_app",
        "test_rt_options_p7_20261020_api",
    ]
    assert plan["evidence"]["files"] == list(EVIDENCE_FILES)
    assert not any(plan["production_authority"].values())
    assert plan["provider_requests_authorized"] == 0
    assert plan["database_connections_authorized"] == 0
    assert plan["database_ddl_authorized"] == 0
    assert plan["files_written_by_preflight"] == 0
    assert plan["live_execution_implemented"] is True
    assert plan == validate_authorization(plan, backend_dir=BACKEND_DIR)


@pytest.mark.parametrize(
    ("change", "message"),
    (
        (
            {"controlled_reconnect_at": START + timedelta(minutes=4)},
            "exactly five minutes",
        ),
        (
            {"stop_at": START + timedelta(minutes=11)},
            "exactly ten minutes",
        ),
        (
            {"spy_contract": "O:QQQ261120C00500000"},
            "explicit SPY",
        ),
        (
            {"qqq_contract": "O:QQQ*"},
            "explicit QQQ",
        ),
    ),
)
def test_plan_rejects_scope_or_window_broadening(tmp_path, change, message):
    with pytest.raises(ValueError, match=message):
        _plan(tmp_path, **change)


def test_exact_approval_binds_raw_manifest_and_owner_decision(tmp_path):
    plan = _plan(tmp_path)
    manifest, approval = _approved_files(tmp_path, plan)

    assert approved_authorization(
        manifest,
        approval,
        backend_dir=BACKEND_DIR,
    ) == plan

    content = json.loads(approval.read_text(encoding="utf-8"))
    content["provider_websocket_connection_attempts_authorized"] = 3
    approval.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(ValueError, match="exact conditional owner approval"):
        approved_authorization(
            manifest,
            approval,
            backend_dir=BACKEND_DIR,
        )


def test_manifest_mutation_or_extra_approval_field_fails(tmp_path):
    plan = _plan(tmp_path)
    manifest, approval = _approved_files(tmp_path, plan)
    changed = dict(plan)
    changed["provider_requests_authorized"] = 1
    manifest.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ValueError, match="exact conditional owner approval"):
        approved_authorization(
            manifest,
            approval,
            backend_dir=BACKEND_DIR,
        )

    manifest, approval = _approved_files(tmp_path, plan)
    content = json.loads(approval.read_text(encoding="utf-8"))
    content["archive_authorized"] = True
    approval.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(ValueError, match="fields are not exact"):
        approved_authorization(
            manifest,
            approval,
            backend_dir=BACKEND_DIR,
        )


def test_provider_free_preflight_has_no_effects_and_writes_nothing(tmp_path):
    plan = _plan(tmp_path)

    result = provider_free_preflight(
        plan,
        backend_dir=BACKEND_DIR,
        now=START - timedelta(hours=1),
        environ={},
    )

    assert result["status"] == "PASS_PROVIDER_FREE_PREFLIGHT"
    assert result["provider_requests"] == 0
    assert result["external_socket_connections"] == 0
    assert result["database_connections"] == 0
    assert result["database_ddl"] == 0
    assert result["files_written"] == 0
    assert result["live_execution_available"] is True
    assert not Path(plan["evidence"]["directory"]).exists()
    assert not any(
        result[name]
        for name in (
            "publication_performed",
            "release_performed",
            "pointer_advanced",
            "execution_performed",
            "delivery_performed",
            "repair_performed",
            "backfill_performed",
            "archive_retention_performed",
            "application_data_deleted",
        )
    )


def test_preflight_rejects_credentials_expired_window_and_existing_output(
    tmp_path,
):
    plan = _plan(tmp_path)
    with pytest.raises(RuntimeError, match="prohibited credentials"):
        provider_free_preflight(
            plan,
            backend_dir=BACKEND_DIR,
            now=START - timedelta(hours=1),
            environ={"POLYGON_API_KEY": "not-read"},
        )
    with pytest.raises(ValueError, match="no longer prospective"):
        provider_free_preflight(
            plan,
            backend_dir=BACKEND_DIR,
            now=START,
            environ={},
        )
    Path(plan["evidence"]["directory"]).mkdir()
    with pytest.raises(FileExistsError, match="must be new"):
        provider_free_preflight(
            plan,
            backend_dir=BACKEND_DIR,
            now=START - timedelta(hours=1),
            environ={},
        )


def test_plan_cli_prints_only_and_exposes_no_execute_action(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(
        script,
        "provider_free_preflight",
        lambda *_args, **_kwargs: pytest.fail("plan attempted preflight"),
    )
    assert script.main(
        [
            "plan",
            "--run-id",
            str(RUN_ID),
            "--authorization-reference",
            "ADVANCED-OPTIONS-P7-SHADOW-20261020-01",
            "--baseline-head",
            HEAD,
            "--start-utc",
            "2026-10-20T13:35:00Z",
            "--controlled-reconnect-utc",
            "2026-10-20T13:40:00Z",
            "--stop-utc",
            "2026-10-20T13:45:00Z",
            "--spy-contract",
            SPY,
            "--qqq-contract",
            QQQ,
            "--artifact-root",
            str(tmp_path),
        ]
    ) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "AWAITING_OWNER_APPROVAL"
    assert result["live_execution_implemented"] is True
    assert not Path(result["evidence"]["directory"]).exists()

    with pytest.raises(SystemExit):
        script.main(["--execute"])


def test_plan_cli_create_exclusively_persists_the_exact_manifest(
    tmp_path,
    capsys,
):
    manifest = tmp_path / "authorization.json"
    arguments = [
        "plan",
        "--run-id",
        str(RUN_ID),
        "--authorization-reference",
        "ADVANCED-OPTIONS-P7-SHADOW-20261020-01",
        "--baseline-head",
        HEAD,
        "--start-utc",
        "2026-10-20T13:35:00Z",
        "--controlled-reconnect-utc",
        "2026-10-20T13:40:00Z",
        "--stop-utc",
        "2026-10-20T13:45:00Z",
        "--spy-contract",
        SPY,
        "--qqq-contract",
        QQQ,
        "--artifact-root",
        str(tmp_path / "evidence"),
        "--manifest-path",
        str(manifest),
    ]

    assert script.main(arguments) == 0
    printed = json.loads(capsys.readouterr().out)
    assert json.loads(manifest.read_text(encoding="utf-8")) == printed

    with pytest.raises(FileExistsError):
        script.main(arguments)
