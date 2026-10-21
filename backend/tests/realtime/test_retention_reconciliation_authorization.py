import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest

from realtime.retention_reconciliation_authorization import (
    POSTFLIGHT_OBSERVATION_SCHEMA,
    build_reconciliation_authorization,
    validate_postflight_observation,
    validate_reconciliation_authorization,
)
from scripts.audit_realtime_v2_retention_archive import (
    build_retention_compatibility_plan,
)
from scripts.prepare_realtime_v2_retention_reconciliation_authorization import (
    main as prepare_authorization,
)
from test_audit_realtime_v2_retention_archive import (
    _write_reconciliation_report,
)


UTC = timezone.utc
PREPARED = datetime(2026, 10, 21, 6, 37, 51, tzinfo=UTC)
START = datetime(2026, 10, 22, 16, 30, tzinfo=UTC)
STOP = datetime(2026, 10, 22, 16, 40, tzinfo=UTC)
NONCE = UUID("12345678-1234-4abc-8def-1234567890ab")
REFERENCE = "OPTIONS-O-WP5-RECONCILIATION-20261022-01"
ROOT = Path(__file__).resolve().parents[3]


def _plan(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    report_path, _ = _write_reconciliation_report(tmp_path)
    report_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
    plan = build_retention_compatibility_plan(
        report_path,
        expected_report_sha256=report_sha256,
        expected_build_id="retention-production-v1",
        expected_policy_id="retention-policy-v1",
    )
    return report_path, report_sha256, plan


def _package(tmp_path, **overrides):
    report_path, report_sha256, plan = _plan(tmp_path)
    arguments = {
        "repository_root": ROOT,
        "expected_report_sha256": report_sha256,
        "expected_plan_sha256": plan["frozen_plan_sha256"],
        "authorization_reference": REFERENCE,
        "execution_nonce": NONCE,
        "prepared_at": PREPARED,
        "window_start": START,
        "window_end": STOP,
    }
    arguments.update(overrides)
    package = build_reconciliation_authorization(plan, **arguments)
    return report_path, plan, package, arguments


def _validate(
    package,
    *,
    report_path,
    plan,
    now=PREPARED,
    prior=(),
    root=ROOT,
    source_files=None,
):
    arguments = {
        "current_plan": plan,
        "report_path": report_path,
        "repository_root": root,
        "expected_package_sha256": package["package_sha256"],
        "now": now,
        "prior_execution_records": prior,
    }
    if source_files is not None:
        arguments["source_files"] = source_files
    return validate_reconciliation_authorization(package, **arguments)


def _postflight_observation(package):
    contract = package["postflight_contract"]
    originals = []
    for index, row in enumerate(contract["expected_originals"], start=1):
        originals.append({
            "kind": row["kind"],
            "data_file": row["data_file"],
            "data_sha256_before": row["data_sha256"],
            "data_sha256_after": row["data_sha256"],
            "data_mtime_ns_before": index,
            "data_mtime_ns_after": index,
            "manifest_file": row["manifest_file"],
            "manifest_sha256_before": f"{index:x}" * 64,
            "manifest_sha256_after": f"{index:x}" * 64,
            "manifest_mtime_ns_before": index + 10,
            "manifest_mtime_ns_after": index + 10,
        })
    return {
        "schema": POSTFLIGHT_OBSERVATION_SCHEMA,
        "authorization_package_sha256": package["package_sha256"],
        "mode": "READ_ONLY",
        "observed_at_utc": STOP.isoformat(),
        "originals": originals,
        "revisions": contract["expected_revisions"],
        "database": contract["expected_database"],
        "filesystem": contract["expected_filesystem"],
        "services": contract["expected_services"],
        "external_effects": contract["expected_external_effects"],
        "cleanup": {
            "temporary_files": [],
            "temporary_databases": [],
            "temporary_roles": [],
        },
    }


def test_authorization_binds_exact_append_only_scope_and_postflight(tmp_path):
    report_path, plan, package, _ = _package(tmp_path)

    result = _validate(
        package,
        report_path=report_path,
        plan=plan,
    )

    scope = package["authorization_scope"]["scope"]
    assert result["status"] == "PASS_RECONCILIATION_AUTHORIZATION_PREFLIGHT"
    assert result["window_status"] == "PROSPECTIVE"
    assert scope["read_relations"] == [
        "rt.option_flow_cluster_members",
        "rt.option_flow_clusters",
        "rt.option_flow_event_profiles",
        "rt.option_flow_events",
    ]
    assert scope["read_partitions"] == [
        "rt.option_flow_cluster_members_p20261019",
        "rt.option_flow_clusters_p20261019",
        "rt.option_flow_event_profiles_p20261019",
        "rt.option_flow_events_p20261019",
    ]
    assert len(scope["files"]) == 8
    assert {
        row["access"] for row in scope["files"]
    } == {"READ_PRESERVE", "CREATE_NEW_EXCLUSIVE"}
    assert package["authority"] == {
        "append_only_reconciliation_authorized": True,
        "archive_file_create_authorized": 4,
        "session_archive_ledger_insert_rows_authorized": 2,
        "original_file_rewrite_authorized": False,
        "original_timestamp_rewrite_authorized": False,
        "relation_expansion_authorized": False,
        "file_expansion_authorized": False,
        "archive_delete_authorized": False,
        "partition_prune_authorized": False,
        "retention_start_authorized": False,
        "archive_service_start_authorized": False,
        "service_restart_authorized": False,
    }
    assert package["window"]["missed_window_policy"] == (
        "EXPIRE_WITHOUT_ROLLOVER_OR_REUSE"
    )
    assert not package["window"]["rollover_authorized"]
    assert not package["window"]["reuse_authorized"]
    assert package["postflight_contract"]["success_state"][
        "retention_resume_authorized"
    ] is False


def test_preparation_script_writes_only_new_provider_free_artifacts(
    tmp_path,
    capsys,
):
    report_path, report_sha256, plan = _plan(tmp_path)
    output = tmp_path / "authorization.json"
    postflight = tmp_path / "postflight.json"

    assert prepare_authorization([
        "--report", str(report_path),
        "--expected-report-sha256", report_sha256,
        "--expected-plan-sha256", plan["frozen_plan_sha256"],
        "--expected-build-id", "retention-production-v1",
        "--expected-policy-id", "retention-policy-v1",
        "--authorization-reference", REFERENCE,
        "--execution-nonce", str(NONCE),
        "--prepared-at", PREPARED.isoformat(),
        "--window-start", START.isoformat(),
        "--window-end", STOP.isoformat(),
        "--output", str(output),
        "--postflight-output", str(postflight),
    ]) == 0

    package = json.loads(output.read_text(encoding="utf-8"))
    assert json.loads(postflight.read_text(encoding="utf-8")) == package[
        "postflight_contract"
    ]
    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == (
        "RECONCILIATION_AUTHORIZATION_PREPARED_NOT_EXECUTED"
    )
    assert summary["provider_requests"] == 0
    assert summary["database_connections"] == 0
    assert summary["archive_writes"] == 0
    assert not summary["retention_started"]
    with pytest.raises(FileExistsError):
        prepare_authorization([
            "--report", str(report_path),
            "--expected-report-sha256", report_sha256,
            "--expected-plan-sha256", plan["frozen_plan_sha256"],
            "--expected-build-id", "retention-production-v1",
            "--expected-policy-id", "retention-policy-v1",
            "--authorization-reference", REFERENCE,
            "--execution-nonce", str(NONCE),
            "--prepared-at", PREPARED.isoformat(),
            "--window-start", START.isoformat(),
            "--window-end", STOP.isoformat(),
            "--output", str(output),
            "--postflight-output", str(postflight),
        ])


@pytest.mark.parametrize(
    "mutate",
    (
        lambda package: package["authorization_scope"]["scope"][
            "read_relations"
        ].append("rt.raw_payloads"),
        lambda package: package["authorization_scope"]["scope"]["files"].append({
            "path": "2026-10-19/unexpected.json",
            "access": "CREATE_NEW_EXCLUSIVE",
        }),
        lambda package: package["authorization_scope"]["revisions"][0][
            "preserved"
        ].update({"preserve_timestamps": False}),
        lambda package: package["authorization_scope"]["revisions"][0].update({
            "action": "REWRITE_PRIMARY",
        }),
        lambda package: package["authorization_scope"]["revisions"][0][
            "relations"
        ].remove("rt.option_flow_event_profiles"),
        lambda package: package["authority"].update({
            "archive_delete_authorized": True,
        }),
        lambda package: package["authority"].update({
            "partition_prune_authorized": True,
        }),
        lambda package: package["authority"].update({
            "retention_start_authorized": True,
        }),
        lambda package: package["authority"].update({
            "archive_service_start_authorized": True,
        }),
    ),
)
def test_validator_rejects_expanded_or_non_append_scope(tmp_path, mutate):
    report_path, plan, package, _ = _package(tmp_path)
    changed = copy.deepcopy(package)
    mutate(changed)

    with pytest.raises(ValueError):
        _validate(
            changed,
            report_path=report_path,
            plan=plan,
        )


def test_validator_rejects_report_plan_and_source_drift(tmp_path):
    report_path, plan, package, _ = _package(tmp_path)
    report_path.write_bytes(report_path.read_bytes() + b"\x00")
    with pytest.raises(ValueError, match="report drifted"):
        _validate(package, report_path=report_path, plan=plan)

    report_path, plan, package, _ = _package(tmp_path / "plan")
    changed_plan = copy.deepcopy(plan)
    changed_plan["reconciliation"][0]["action"] = "REWRITE_PRIMARY"
    with pytest.raises(ValueError):
        _validate(package, report_path=report_path, plan=changed_plan)

    source_root = tmp_path / "source"
    source_root.mkdir()
    source = source_root / "source.py"
    source.write_text("original\n", encoding="ascii")
    report_path, report_sha256, plan = _plan(tmp_path / "source-plan")
    source_package = build_reconciliation_authorization(
        plan,
        repository_root=source_root,
        source_files=("source.py",),
        expected_report_sha256=report_sha256,
        expected_plan_sha256=plan["frozen_plan_sha256"],
        authorization_reference=REFERENCE,
        execution_nonce=NONCE,
        prepared_at=PREPARED,
        window_start=START,
        window_end=STOP,
    )
    source.write_text("changed\n", encoding="ascii")
    with pytest.raises(ValueError, match="source or scope drifted"):
        _validate(
            source_package,
            report_path=report_path,
            plan=plan,
            root=source_root,
            source_files=("source.py",),
        )


def test_validator_rejects_expiry_reuse_and_ambiguous_execution(tmp_path):
    report_path, plan, package, _ = _package(tmp_path)

    with pytest.raises(ValueError, match="expired"):
        _validate(
            package,
            report_path=report_path,
            plan=plan,
            now=STOP,
        )
    with pytest.raises(ValueError, match="already used"):
        _validate(
            package,
            report_path=report_path,
            plan=plan,
            prior=({"execution_nonce": str(NONCE)},),
        )
    with pytest.raises(ValueError, match="ambiguous"):
        _validate(
            package,
            report_path=report_path,
            plan=plan,
            prior=({"execution_nonce": str(UUID(int=1))},),
        )
    with pytest.raises(ValueError, match="ambiguous"):
        _validate(
            package,
            report_path=report_path,
            plan=plan,
            prior=None,
        )


def test_exact_read_only_postflight_passes_and_leaves_retention_stopped(
    tmp_path,
):
    _, _, package, _ = _package(tmp_path)
    observation = _postflight_observation(package)

    result = validate_postflight_observation(package, observation)

    assert result["status"] == (
        "PASS_RECONCILIATION_POSTFLIGHT_RETENTION_STOPPED"
    )
    assert result["retention_stopped"]
    assert not result["retention_resume_authorized"]


@pytest.mark.parametrize(
    "mutate, match",
    (
        (
            lambda value: value["originals"][0].update({
                "manifest_mtime_ns_after": 999,
            }),
            "bytes or timestamps",
        ),
        (
            lambda value: value["revisions"][0]["relation_counts"].pop(
                "rt.option_flow_event_profiles"
            ),
            "companion coverage",
        ),
        (
            lambda value: value["revisions"][1].update({
                "manifest_sha256": "0" * 64,
            }),
            "checksum",
        ),
        (
            lambda value: value["database"].update({"deletes": 1}),
            "database rows",
        ),
        (
            lambda value: value["filesystem"]["unexpected_files"].append(
                "unexpected"
            ),
            "filesystem",
        ),
        (
            lambda value: value["services"].update({
                "retention_after": "RUNNING",
            }),
            "service state",
        ),
        (
            lambda value: value["cleanup"]["temporary_roles"].append(
                "leftover"
            ),
            "cleanup",
        ),
    ),
)
def test_postflight_rejects_drift_effects_and_incomplete_cleanup(
    tmp_path,
    mutate,
    match,
):
    _, _, package, _ = _package(tmp_path)
    observation = copy.deepcopy(_postflight_observation(package))
    mutate(observation)

    with pytest.raises(ValueError, match=match):
        validate_postflight_observation(package, observation)
