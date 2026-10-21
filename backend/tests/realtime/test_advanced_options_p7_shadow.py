from __future__ import annotations

import hashlib
import importlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from realtime.advanced_options_p7_shadow import (
    APPROVAL_VERSION,
    EVIDENCE_FILES,
    OWNER_DECISION,
    approved_authorization,
    build_authorization,
    build_live_authorization,
    provider_free_preflight,
    validate_authorization,
)
from realtime.advanced_options_p7_selection import (
    P7TransportCandidate,
    p7_transport_selection_payload,
    select_p7_transport_contracts,
)


UTC = timezone.utc
START = datetime(2026, 10, 20, 13, 35, tzinfo=UTC)
RUN_ID = UUID("77777777-7777-7777-7777-777777777777")
HEAD = "89b6c1b142b411cc57c519ce2de57543311a1b48"
SPY = "O:SPY261020C00100000"
QQQ = "O:QQQ261020P00100000"
BACKEND_DIR = Path(__file__).resolve().parents[2]
script = importlib.import_module("scripts.prepare_advanced_options_p7_shadow")
live_script = importlib.import_module(
    "scripts.prepare_advanced_options_p7_live_authorization"
)


def _selection(spy_contract, qqq_contract):
    candidates = tuple(
        P7TransportCandidate(
            contract_ticker=contract,
            underlyer=underlyer,
            expiration_date=START.date(),
            option_type=option_type,
            strike=Decimal("100"),
            spot=Decimal("100"),
            bid=Decimal("1.00"),
            ask=Decimal("1.10"),
            day_volume=500,
            open_interest=1_000,
            calendar_dte=0,
            market_data_time=START - timedelta(minutes=1),
            source_sha256=source,
        )
        for contract, underlyer, option_type, source in (
            (spy_contract, "SPY", "C", "1" * 64),
            (qqq_contract, "QQQ", "P", "2" * 64),
        )
    )
    return p7_transport_selection_payload(
        select_p7_transport_contracts(
            candidates,
            reviewed_at=START - timedelta(seconds=30),
        )
    )


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
        "contract_selection": None,
        "backend_dir": BACKEND_DIR,
        "artifact_root": tmp_path,
    }
    values.update(changes)
    if values["contract_selection"] is None:
        try:
            values["contract_selection"] = _selection(
                values["spy_contract"],
                values["qqq_contract"],
            )
        except ValueError:
            values["contract_selection"] = _selection(SPY, QQQ)
    return build_authorization(**values)


def _selection_file(tmp_path):
    path = tmp_path / "selection.json"
    path.write_text(
        json.dumps(_selection(SPY, QQQ), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


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
                "provider_websocket_connection_attempts_authorized": 6,
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
    assert plan["socket_ceiling"]["connection_attempts"] == 6
    assert plan["socket_ceiling"]["reconnects"] == 5
    assert plan["socket_ceiling"]["reconnect_backoff"] == {
        "initial_seconds": 5,
        "minimum_seconds": 5,
        "multiplier": 2,
        "maximum_seconds": 60,
        "jitter_ceiling_seconds": 60,
    }
    assert plan["database"]["name"] == (
        "test_realtime_v2_options_p7_20261020"
    )
    assert plan["database"]["roles"] == [
        "test_rt_options_p7_20261020_admin",
        "test_rt_options_p7_20261020_app",
        "test_rt_options_p7_20261020_api",
    ]
    assert [row["version"] for row in plan["migrations"][-7:]] == [
        "025_realtime_advanced_options_control_plane",
        "026_advanced_options_flow_evidence",
        "027_advanced_options_provider_references",
        "028_advanced_options_reference_state",
        "029_advanced_options_rolling_checkpoints",
        "030_advanced_options_flow_package_evidence",
        "031_realtime_advanced_journal_recovery",
    ]
    assert plan["evidence"]["files"] == list(EVIDENCE_FILES)
    assert any(
        row["path"] == "realtime/contracts.py"
        for row in plan["source_bindings"]
    )
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
    content["provider_websocket_connection_attempts_authorized"] = 7
    approval.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(ValueError, match="exact conditional owner approval"):
        approved_authorization(
            manifest,
            approval,
            backend_dir=BACKEND_DIR,
        )


def test_live_authorization_generator_binds_exact_approved_scope(tmp_path):
    plan = _plan(tmp_path)
    manifest, approval = _approved_files(tmp_path, plan)
    output = tmp_path / "live-authorization.json"

    assert live_script.main([
        "--manifest",
        str(manifest),
        "--approval",
        str(approval),
        "--output",
        str(output),
    ]) == 0

    authorization = json.loads(output.read_text(encoding="utf-8"))
    expected = build_live_authorization(
        plan,
        manifest_sha256=hashlib.sha256(
            manifest.read_bytes()
        ).hexdigest(),
        approval_sha256=hashlib.sha256(
            approval.read_bytes()
        ).hexdigest(),
        authorized_at=datetime.fromisoformat(
            authorization["authorized_at_utc"]
        ),
    )
    assert authorization == expected
    assert authorization["provider_scope"]["connection_attempts"] == 6
    assert authorization["provider_scope"]["lease_scope"] == "HOST_LOCAL"


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
    selection = _selection_file(tmp_path)
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
            "--contract-selection",
            str(selection),
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
    selection = _selection_file(tmp_path)
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
        "--contract-selection",
        str(selection),
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
