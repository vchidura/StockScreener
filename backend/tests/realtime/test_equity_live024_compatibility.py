import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from realtime.equity_live024_compatibility import (
    CONTRACT_ID,
    LIVE024_MIGRATION_VERSIONS,
    LIVE024_REGISTRY_SHA256,
    LIVE024_SCHEMA_SHA256,
    LIVE024_SECURITY_POLICY_SHA256,
    POST024_RELATIONS,
    contract_metadata,
    live024_acl_contract_inventory,
    live024_migrations,
    live024_migration_rows,
    live024_registry_contract,
    live024_schema_contract_inventory,
    load_live_metadata_capture,
    load_live_preflight_report,
    load_qualification_report,
)
from scripts import preflight_realtime_v2_advanced_canary as preflight
from scripts import provision_realtime_v2_advanced_equity_scope as provision
from scripts import qualify_realtime_v2_advanced_docker as qualification


BACKEND_DIR = Path(__file__).resolve().parents[2]


def _write_bound_report(path, report):
    path.write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _qualification_report():
    return {
        "status": "PASS_EQUITY_LIVE024_COMPATIBILITY",
        "contract_id": CONTRACT_ID,
        "manifest_id": "revised-live024-manifest",
        "schema_sha256": LIVE024_SCHEMA_SHA256,
        "migrations": list(live024_migration_rows()),
        "registry_sha256": LIVE024_REGISTRY_SHA256,
        "schema_inventory_sha256": "a" * 64,
        "acl_inventory_sha256": "b" * 64,
        "security_policy_sha256": "c" * 64,
        "operator_bundle_sha256": "d" * 64,
        "runtime_bundle_sha256": "e" * 64,
        "runtime_profile_sha256": "f" * 64,
        "runtime_policy_sha256": "1" * 64,
        "image_id": "sha256:" + "2" * 64,
        "provider_requests": 0,
        "live_database_writes": 0,
        "live_profile_activated": False,
        "publication_performed": False,
        "cleanup_verified": True,
        "counter_preservation_verified": True,
        "live_inventory_match": True,
        "live_metadata_report_sha256": "3" * 64,
        "live_schema_inventory_sha256": "a" * 64,
        "live_acl_inventory_sha256": "b" * 64,
        "receipt_equality": {"equal": True},
        "metadata_evidence": {"status": "PASS_REAL_STOCK_METADATA"},
        "phases": [{
            "negative_cases": {
                "runtime_ddl": {"rejected": True},
                "new_options_receipt": {"rejected": True},
                "counter_reset": {"rejected": True},
                "api_write_and_ddl_rejected": True,
            },
        }],
    }


def test_contract_pins_exact_000_through_024_and_historical_registry():
    migrations = live024_migrations()
    registry = live024_registry_contract()

    assert tuple(migration.version for migration in migrations) == (
        LIVE024_MIGRATION_VERSIONS
    )
    assert migrations[-1].sequence == 24
    assert registry.sha256 == LIVE024_REGISTRY_SHA256
    assert not POST024_RELATIONS & set(registry.relation_names)
    assert "rt.option_flow_events" in registry.relation_names
    assert "rt.advanced_ingress_plans" in registry.relation_names


def test_contract_binds_operator_and_runtime_source_separately():
    metadata = contract_metadata(BACKEND_DIR)

    assert metadata["contract_id"] == CONTRACT_ID
    assert metadata["schema_sha256"] == LIVE024_SCHEMA_SHA256
    assert metadata["registry_sha256"] == LIVE024_REGISTRY_SHA256
    assert (
        metadata["security_policy_sha256"]
        == LIVE024_SECURITY_POLICY_SHA256
    )
    assert (
        metadata["security_policy_document_sha256"]
        != metadata["security_policy_sha256"]
    )
    assert len(metadata["operator"]["sha256"]) == 64
    assert len(metadata["runtime"]["sha256"]) == 64
    assert metadata["operator"]["sha256"] != metadata["runtime"]["sha256"]


def test_qualification_report_requires_exact_hash_scope_and_negative_evidence(
    tmp_path,
):
    path = tmp_path / "qualification.json"
    report = _qualification_report()
    digest = _write_bound_report(path, report)

    loaded, observed = load_qualification_report(path, digest)
    assert loaded == report
    assert observed == digest

    report["schema_sha256"] = "9" * 64
    digest = _write_bound_report(path, report)
    with pytest.raises(ValueError, match="exact live-024"):
        load_qualification_report(path, digest)


def test_preflight_compatibility_binding_is_explicit_and_report_bound(tmp_path):
    assert preflight.compatibility_binding({"verified_bindings": {}}) is None
    path = tmp_path / "qualification.json"
    report = _qualification_report()
    digest = _write_bound_report(path, report)
    manifest = {
        "verified_bindings": {
            "equity_live024": {
                "contract_id": CONTRACT_ID,
                "qualification_report": str(path),
                "qualification_report_sha256": digest,
            },
        },
    }

    binding, loaded, observed = preflight.compatibility_binding(manifest)

    assert binding["contract_id"] == CONTRACT_ID
    assert loaded == report
    assert observed == digest


def test_provisioner_accepts_only_fresh_spy_live_preflight(tmp_path):
    now = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
    path = tmp_path / "preflight.json"
    report = {
        "status": (
            "SCHEMA_SECURITY_HISTORY_BRIDGE_COMPATIBLE_METADATA_GATES_REQUIRED"
        ),
        "contract_id": CONTRACT_ID,
        "manifest_id": "revised-live024-manifest",
        "database": "stocks_realtime_v2",
        "schema_sha256": LIVE024_SCHEMA_SHA256,
        "blockers": [],
        "provider_requests": 0,
        "live_database_writes": 0,
        "live_profile_activation_performed": False,
        "execution_eligible": False,
        "qualification_report_sha256": "a" * 64,
        "checked_at": (now - timedelta(minutes=1)).isoformat(),
        "arm_deadline": (now + timedelta(minutes=4)).isoformat(),
        "security_policy_sha256": "b" * 64,
        "schema_inventory_sha256": "e" * 64,
        "acl_inventory_sha256": "f" * 64,
        "runtime_profile_sha256": "c" * 64,
        "runtime_policy_sha256": "d" * 64,
        "history_check": {"status": "PASS"},
        "bridge_check": {"status": "PASS"},
        "pre_provisioning_admission": {"status": "PASS"},
        "post_bootstrap_prearm_verification": {"status": "PASS"},
        "bootstrap_eligible": False,
        "permitted_actions": [],
    }
    digest = _write_bound_report(path, report)
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(json.dumps({
        "owner_decision": "approve_equity_live024_scope_provisioning",
        "conditional_execution_authorized": True,
        "contract_id": CONTRACT_ID,
        "preflight_report_sha256": digest,
        "qualification_report_sha256": report[
            "qualification_report_sha256"
        ],
        "manifest_id": report["manifest_id"],
        "authorization_reference": "owner-live024-approval",
    }), encoding="utf-8")
    args = SimpleNamespace(
        live024_preflight_report=path,
        live024_preflight_report_sha256=digest,
        live024_preflight_approval=approval_path,
        symbols=("SPY",),
        authorization_reference="owner-live024-approval",
    )

    loaded, observed = provision._load_compatibility_preflight(args, now)

    assert loaded == report
    assert observed == digest
    args.symbols = ("AAPL",)
    with pytest.raises(ValueError, match="exact SPY"):
        provision._load_compatibility_preflight(args, now)
    args.symbols = ("SPY",)
    approval_path.write_text(json.dumps({
        "owner_decision": "denied",
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="exact scope-provisioning approval"):
        provision._load_compatibility_preflight(args, now)


def test_live_preflight_report_loader_rejects_success_shaped_fallback(tmp_path):
    path = tmp_path / "preflight.json"
    report = {
        "status": (
            "SCHEMA_SECURITY_HISTORY_BRIDGE_COMPATIBLE_METADATA_GATES_REQUIRED"
        ),
        "contract_id": CONTRACT_ID,
        "schema_sha256": LIVE024_SCHEMA_SHA256,
        "blockers": ["SKIPPED_ACL"],
        "provider_requests": 0,
        "live_database_writes": 0,
        "live_profile_activation_performed": False,
        "execution_eligible": False,
        "qualification_report_sha256": "a" * 64,
        "checked_at": "2026-10-19T12:00:00+00:00",
        "arm_deadline": "2026-10-19T12:05:00+00:00",
        "history_check": {"status": "BLOCKED"},
        "bridge_check": {"status": "PASS"},
    }
    digest = _write_bound_report(path, report)

    with pytest.raises(ValueError, match="read-only admission"):
        load_live_preflight_report(path, digest)


def test_live_preflight_loader_accepts_exact_history_bootstrap_admission(
    tmp_path,
):
    path = tmp_path / "preflight.json"
    report = {
        "status": "HISTORY_BOOTSTRAP_REQUIRED",
        "contract_id": CONTRACT_ID,
        "schema_sha256": LIVE024_SCHEMA_SHA256,
        "blockers": [],
        "provider_requests": 0,
        "live_database_writes": 0,
        "live_profile_activation_performed": False,
        "execution_eligible": False,
        "bootstrap_eligible": True,
        "permitted_actions": [
            "SEPARATELY_AUTHORIZED_HISTORY_BOOTSTRAP"
        ],
        "history_check": {
            "status": "HISTORY_BOOTSTRAP_REQUIRED",
            "bootstrap_required": True,
            "permitted_action": (
                "SEPARATELY_AUTHORIZED_HISTORY_BOOTSTRAP"
            ),
        },
        "bridge_check": {
            "status": "NOT_RUN",
            "reason_codes": ["HISTORY_BOOTSTRAP_REQUIRED"],
        },
        "pre_provisioning_admission": {
            "status": "HISTORY_BOOTSTRAP_REQUIRED",
            "bootstrap_required": True,
            "permitted_action": (
                "SEPARATELY_AUTHORIZED_HISTORY_BOOTSTRAP"
            ),
        },
        "post_bootstrap_prearm_verification": {
            "status": "NOT_RUN",
            "reason_codes": ["HISTORY_BOOTSTRAP_REQUIRED"],
        },
        "qualification_report_sha256": "a" * 64,
        "checked_at": "2026-10-19T12:00:00+00:00",
        "arm_deadline": "2026-10-19T12:05:00+00:00",
        "schema_inventory_sha256": "b" * 64,
        "acl_inventory_sha256": "c" * 64,
        "security_policy_sha256": "d" * 64,
        "runtime_profile_sha256": "e" * 64,
        "runtime_policy_sha256": "f" * 64,
    }
    digest = _write_bound_report(path, report)

    loaded, observed = load_live_preflight_report(path, digest)

    assert loaded == report
    assert observed == digest


def test_live_capture_normalizes_environment_specific_relations(tmp_path):
    schema_inventory = {
        "relations": [
            ["rt", "capability_profiles", "r"],
            ["rt", "bars_20261019", "r"],
            ["rt", "readiness_transitions_transition_id_seq", "S"],
        ],
        "columns": [
            ["rt", "capability_profiles", "profile_id"],
            ["rt", "bars_20261019", "instrument_id"],
        ],
        "constraints": [],
        "indexes": [],
        "functions": [],
        "triggers": [],
        "sha256": "raw",
    }
    acl_inventory = {
        "owners": [
            ["rt", "capability_profiles", "r", "admin"],
            ["rt", "bars_20261019", "r", "admin"],
            ["rt", "readiness_transitions_transition_id_seq", "S", "admin"],
        ],
        "database_owner": "admin",
        "schema_owners": [],
        "function_owners": [],
        "relation_acl": [],
        "roles": [],
        "database_privileges": [],
        "schema_privileges": [],
        "function_privileges": [],
        "role_memberships": [],
        "sha256": "raw",
    }
    report = {
        "status": "PASS_READ_ONLY_CAPTURE",
        "compatibility_gate": "CAPTURE_COMPATIBLE",
        "blockers": [],
        "contract_id": CONTRACT_ID,
        "schema_sha256": LIVE024_SCHEMA_SHA256,
        "registry_sha256": LIVE024_REGISTRY_SHA256,
        "expected_migrations": list(live024_migration_rows()),
        "live_migrations": list(live024_migration_rows()),
        "provider_requests": 0,
        "live_database_writes": 0,
        "live_ddl": 0,
        "live_profile_activation_performed": False,
        "execution_eligible": False,
        "compatibility_proven": False,
        "schema_inventory": schema_inventory,
        "acl_inventory": acl_inventory,
    }
    path = tmp_path / "capture.json"
    digest = _write_bound_report(path, report)

    _, _, identities = load_live_metadata_capture(path, digest)

    normalized_schema = live024_schema_contract_inventory(schema_inventory)
    normalized_acl = live024_acl_contract_inventory(acl_inventory)
    assert ["rt", "bars_20261019", "r"] not in normalized_schema["relations"]
    assert identities == {
        "schema_inventory_sha256": normalized_schema["sha256"],
        "acl_inventory_sha256": normalized_acl["sha256"],
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_sha256", "0" * 64),
        ("symbols", ["AAPL"]),
        ("universe_target", 9),
        ("maximum_connection_attempts", 4),
        ("maximum_decoded_bytes", 10 * 1024 * 1024 + 1),
        ("maximum_rest_repair_requests", 1),
        ("publication_authorized", True),
        ("maximum_workload_seconds_after_build", 601),
    ],
)
def test_qualification_plan_rejects_scope_or_deadline_expansion(field, value):
    plan = qualification.qualification_plan(
        qualification.UUID("11111111-1111-1111-1111-111111111111")
    )
    plan[field] = value

    with pytest.raises(ValueError, match="exact live-024 scope"):
        qualification.validate_qualification_plan(plan)
