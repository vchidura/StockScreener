import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from realtime.retention_lifecycle_authorization import (
    OBSERVATION_SCHEMA,
    POST_STOP_SCHEMA,
    build_lifecycle_authorization,
    classify_revision,
    lifecycle_status,
    validate_lifecycle_authorization,
    validate_post_stop_snapshot,
)
from scripts.audit_realtime_v2_retention_lifecycle import (
    main as audit_lifecycle,
)
import scripts.prepare_realtime_v2_retention_lifecycle_authorization as prepare


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[3]
READINESS_REPORT = (
    ROOT
    / "backend/.realtime-canary/"
    "options-owp5-execution-readiness-20261021.json"
)
PREPARED = datetime(2026, 10, 21, 7, 14, 9, 353000, tzinfo=UTC)
START = datetime(2026, 10, 22, 15, 30, tzinfo=UTC)
STOP = datetime(2026, 10, 22, 15, 40, tzinfo=UTC)
NONCE = UUID("7e3c8a6d-4f21-4d9e-8b75-2c1a0f6e93d4")
REFERENCE = "OPTIONS-O-WP5-RETENTION-STOP-20261022-01"
CONTAINER_ID = "6f2424ce65b3132ae8e5ec766a503919f2adc493fb4907f68eeffff67f2601ef"
IMAGE_ID = (
    "sha256:a3426e8e6d049a7a84036e94e5c0d924274afc91beaaaef05d794cdc77957adb"
)


def _revision(kind, primary, companion):
    stem = (
        "option_flow_events"
        if kind == "OPTION_FLOW_EVENTS"
        else "option_flow_clusters"
    )
    return {
        "kind": kind,
        "build_id": "retention-production-v1",
        "expected_relations": [primary, companion],
        "source_counts": {primary: 0, companion: 0},
        "manifest_relation_counts": {primary: 0, companion: 0},
        "file_relation_counts": {primary: 0, companion: 0},
        "integrity_status": "PASS",
        "content_matches": True,
        "files": {
            "data_present": True,
            "manifest_present": True,
            "data_path": (
                f"2026-10-19/{stem}."
                "e3b0c44298fc1c14.f58e881ede46a814.jsonl.gz"
            ),
            "manifest_path": (
                f"2026-10-19/{stem}."
                "e3b0c44298fc1c14.f58e881ede46a814.manifest.json"
            ),
            "data_sha256": (
                "f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec"
            ),
            "manifest_sha256": "a" * 64,
        },
        "matching_ledger_rows": [],
    }


def _observation():
    command = [
        "python",
        "-B",
        "scripts/run_realtime_v2_retention_service.py",
        "--continuous",
        "--execute",
    ]
    inventory = [
        {
            "path": path,
            "sha256": sha256,
            "bytes": size,
            "mtime_epoch_seconds": mtime,
        }
        for path, sha256, size, mtime in (
            (
                "2026-10-19/option_flow_events.jsonl.gz",
                "f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec",
                20,
                1792440699,
            ),
            (
                "2026-10-19/option_flow_events.manifest.json",
                "12fed294e61facfe4134db0382469877b167c64a9761ec31c36f72592139a83a",
                660,
                1792440699,
            ),
            (
                "2026-10-19/option_flow_clusters.jsonl.gz",
                "f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec",
                20,
                1792440699,
            ),
            (
                "2026-10-19/option_flow_clusters.manifest.json",
                "cdf51ddcadcdeea141f691dd4ab89e0535a15cde96679b715f8f3707df98f6fa",
                666,
                1792440699,
            ),
            (
                "2026-10-19/option_flow_events.e3b0c44298fc1c14.f58e881ede46a814.jsonl.gz",
                "f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec",
                20,
                1792464543,
            ),
            (
                "2026-10-19/option_flow_events.e3b0c44298fc1c14.f58e881ede46a814.manifest.json",
                "e545b28f267998e4aa1db9661088c03af810718f87d844efefbaee3deb263adc",
                734,
                1792464543,
            ),
            (
                "2026-10-19/option_flow_clusters.e3b0c44298fc1c14.f58e881ede46a814.jsonl.gz",
                "f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec",
                20,
                1792464542,
            ),
            (
                "2026-10-19/option_flow_clusters.e3b0c44298fc1c14.f58e881ede46a814.manifest.json",
                "7d0b372c9163272e2a9abcd073d7f7da5eff76bf0f02c342870ec3fdeed1eb45",
                741,
                1792464543,
            ),
        )
    ]
    return {
        "schema": OBSERVATION_SCHEMA,
        "checked_at_utc": PREPARED.isoformat(),
        "matching_containers": [{
            "id": CONTAINER_ID,
            "name": "stock-screener-realtime-v2-retention",
            "service": "realtime-v2-retention",
            "image_id": IMAGE_ID,
            "image_created_at": "2026-10-20T02:27:51.599601247Z",
            "image_reference": (
                "alpha-research-realtime-v2-backend@sha256:"
                "a3426e8e6d049a7a84036e94e5c0d924274afc91beaaaef05d794cdc77957adb"
            ),
            "command": command,
            "command_sha256": prepare.canonical_sha256(command),
            "entrypoint": None,
            "started_at_utc": "2026-10-20T02:54:15.627943246Z",
            "status": "running",
            "running": True,
            "health": "healthy",
            "restart_policy": {
                "name": "unless-stopped",
                "maximum_retry_count": 0,
            },
            "compose_identity": {
                "config_hash": "db471db2",
                "container_number": "1",
                "image": "sha256:ea58",
                "oneoff": "False",
                "project": "alpha-research-realtime-v2",
                "service": "realtime-v2-retention",
                "version": "5.5.1",
            },
            "mounts": [{
                "type": "volume",
                "destination": "/var/lib/realtime-v2-retention",
                "read_write": True,
                "name": "alpha-research-realtime-v2-retention-production-v1",
            }],
            "live_source_hashes": {
                "/app/realtime/session_archive.py": "d" * 64,
            },
        }],
        "archive_inventory": inventory,
        "source_counts": {
            "rt.option_flow_events": 0,
            "rt.option_flow_event_profiles": 0,
            "rt.option_flow_clusters": 0,
            "rt.option_flow_cluster_members": 0,
        },
        "ledger_rows": [
            {
                "kind": "OPTION_FLOW_EVENTS",
                "sha256": (
                    "f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec"
                ),
                "build_id": "retention-production-v1",
                "row_count": 0,
                "uri": "file:///archive/option_flow_events.jsonl.gz",
                "verified_at": "2026-10-19T20:11:39+00:00",
            },
            {
                "kind": "OPTION_FLOW_CLUSTERS",
                "sha256": (
                    "f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec"
                ),
                "build_id": "retention-production-v1",
                "row_count": 0,
                "uri": "file:///archive/option_flow_clusters.jsonl.gz",
                "verified_at": "2026-10-19T20:11:39+00:00",
            },
        ],
        "revision_evidence": [
            _revision(
                "OPTION_FLOW_EVENTS",
                "rt.option_flow_events",
                "rt.option_flow_event_profiles",
            ),
            _revision(
                "OPTION_FLOW_CLUSTERS",
                "rt.option_flow_clusters",
                "rt.option_flow_cluster_members",
            ),
        ],
        "lock_or_partial_files": [],
        "package_target_files": [],
    }


def _package():
    observation = _observation()
    package = build_lifecycle_authorization(
        READINESS_REPORT,
        observation,
        repository_root=ROOT,
        authorization_reference=REFERENCE,
        lifecycle_nonce=NONCE,
        prepared_at=PREPARED,
        window_start=START,
        window_end=STOP,
    )
    return observation, package


@pytest.mark.parametrize(
    "mutation, expected",
    (
        (lambda row: None, "ORPHANED_FILES"),
        (
            lambda row: row["matching_ledger_rows"].append({
                "sha256": row["files"]["data_sha256"],
                "build_id": row["build_id"],
                "row_count": 0,
            }),
            "COMPLETE_VALID",
        ),
        (
            lambda row: row["files"].update({"manifest_present": False}),
            "PARTIAL_AMBIGUOUS",
        ),
        (
            lambda row: (
                row["files"].update({
                    "data_present": False,
                    "manifest_present": False,
                }),
                row["matching_ledger_rows"].append({
                    "sha256": row["files"]["data_sha256"],
                    "build_id": row["build_id"],
                    "row_count": 0,
                }),
            ),
            "LEDGER_ONLY",
        ),
        (
            lambda row: row["manifest_relation_counts"].pop(
                row["expected_relations"][1]
            ),
            "DIVERGENT",
        ),
    ),
)
def test_revision_classifier_covers_all_terminal_states(mutation, expected):
    row = _revision(
        "OPTION_FLOW_EVENTS",
        "rt.option_flow_events",
        "rt.option_flow_event_profiles",
    )
    mutation(row)
    assert classify_revision(row) == expected


def test_package_binds_exact_stop_only_authority_and_orphaned_files():
    observation, package = _package()

    result = validate_lifecycle_authorization(
        package,
        readiness_report_path=READINESS_REPORT,
        current_observation={
            **observation,
            "checked_at_utc": (
                PREPARED + timedelta(minutes=5)
            ).isoformat(),
        },
        repository_root=ROOT,
        expected_package_sha256=package["package_sha256"],
        now=PREPARED,
        prior_stop_records=(),
    )

    assert result["status"] == "PASS_STOP_ONLY_LIFECYCLE_PREFLIGHT"
    assert package["authority"] == {
        "matching_container_count": 1,
        "disable_automatic_restart": True,
        "stop_exact_container": True,
        "start_authorized": False,
        "restart_authorized": False,
        "resume_authorized": False,
        "container_remove_authorized": False,
        "reconciliation_authorized": False,
        "archive_write_authorized": False,
        "database_write_authorized": False,
        "delete_authorized": False,
        "prune_authorized": False,
        "evidence_rewrite_authorized": False,
    }
    assert package["post_stop_contract"][
        "expected_revision_classifications"
    ] == {
        "OPTION_FLOW_CLUSTERS": "ORPHANED_FILES",
        "OPTION_FLOW_EVENTS": "ORPHANED_FILES",
    }
    assert lifecycle_status(package, observation) == {
        "state": "READY_FOR_EXPLICIT_STOP_AUTHORIZATION",
    }


@pytest.mark.parametrize(
    "mutate",
    (
        lambda value: value["matching_containers"].append(
            copy.deepcopy(value["matching_containers"][0])
        ),
        lambda value: value["matching_containers"][0].update({
            "image_id": "sha256:" + "0" * 64,
        }),
        lambda value: value["matching_containers"][0].update({
            "command_sha256": "0" * 64,
        }),
        lambda value: value["matching_containers"][0].update({
            "started_at_utc": "2026-10-20T03:00:00Z",
        }),
        lambda value: value["matching_containers"][0].update({
            "health": "unhealthy",
        }),
        lambda value: value["matching_containers"][0]["restart_policy"].update({
            "name": "always",
        }),
        lambda value: value["archive_inventory"][0].update({
            "sha256": "0" * 64,
        }),
        lambda value: value["ledger_rows"].clear(),
    ),
)
def test_validator_rejects_service_file_and_ledger_drift(mutate):
    observation, package = _package()
    changed = copy.deepcopy(observation)
    mutate(changed)
    with pytest.raises(ValueError):
        validate_lifecycle_authorization(
            package,
            readiness_report_path=READINESS_REPORT,
            current_observation=changed,
            repository_root=ROOT,
            expected_package_sha256=package["package_sha256"],
            now=PREPARED,
            prior_stop_records=(),
        )


def test_validator_rejects_expanded_authority_expiry_reuse_and_ambiguity():
    observation, package = _package()
    changed = copy.deepcopy(package)
    changed["authority"]["restart_authorized"] = True
    with pytest.raises(ValueError, match="package hash changed"):
        validate_lifecycle_authorization(
            changed,
            readiness_report_path=READINESS_REPORT,
            current_observation=observation,
            repository_root=ROOT,
            expected_package_sha256=package["package_sha256"],
            now=PREPARED,
            prior_stop_records=(),
        )
    with pytest.raises(ValueError, match="expired"):
        validate_lifecycle_authorization(
            package,
            readiness_report_path=READINESS_REPORT,
            current_observation=observation,
            repository_root=ROOT,
            expected_package_sha256=package["package_sha256"],
            now=STOP,
            prior_stop_records=(),
        )
    with pytest.raises(ValueError, match="already used"):
        validate_lifecycle_authorization(
            package,
            readiness_report_path=READINESS_REPORT,
            current_observation=observation,
            repository_root=ROOT,
            expected_package_sha256=package["package_sha256"],
            now=PREPARED,
            prior_stop_records=({"lifecycle_nonce": str(NONCE)},),
        )
    with pytest.raises(ValueError, match="ambiguous"):
        validate_lifecycle_authorization(
            package,
            readiness_report_path=READINESS_REPORT,
            current_observation=observation,
            repository_root=ROOT,
            expected_package_sha256=package["package_sha256"],
            now=PREPARED,
            prior_stop_records=None,
        )


def test_status_and_post_stop_contract_are_read_only_and_exact():
    observation, package = _package()
    stopped = copy.deepcopy(observation)
    container = stopped["matching_containers"][0]
    container.update({
        "status": "exited",
        "running": False,
        "health": None,
        "restart_policy": {
            "name": "no",
            "maximum_retry_count": 0,
        },
    })
    assert lifecycle_status(package, stopped) == {
        "state": "STOP_CONSUMED_POST_SNAPSHOT_REQUIRED",
    }
    contract = package["post_stop_contract"]
    snapshot = {
        "schema": POST_STOP_SCHEMA,
        "lifecycle_package_sha256": package["package_sha256"],
        "captured_at_utc": START.isoformat(),
        "container": contract["expected_container"],
        "database_snapshot": contract["expected_database_snapshot"],
        "archive_inventory": contract["expected_archive_inventory"],
        "lock_or_partial_files": [],
        "revision_classifications": contract[
            "expected_revision_classifications"
        ],
        "effects": contract["forbidden_effects"],
    }
    result = validate_post_stop_snapshot(package, snapshot)
    assert result["retention_stopped"]
    assert not result["retention_resume_authorized"]
    changed = copy.deepcopy(snapshot)
    changed["database_snapshot"]["writes"] = 1
    with pytest.raises(ValueError, match="database"):
        validate_post_stop_snapshot(package, changed)


def test_prepare_and_plan_paths_use_fake_observation_only(
    tmp_path,
    monkeypatch,
    capsys,
):
    observation = _observation()
    monkeypatch.setattr(
        prepare,
        "capture_observation",
        lambda **_kwargs: observation,
    )
    output = tmp_path / "lifecycle.json"
    post_stop = tmp_path / "post-stop.json"
    assert prepare.main([
        "--readiness-report", str(READINESS_REPORT),
        "--authorization-reference", REFERENCE,
        "--lifecycle-nonce", str(NONCE),
        "--prepared-at", PREPARED.isoformat(),
        "--window-start", START.isoformat(),
        "--window-end", STOP.isoformat(),
        "--output", str(output),
        "--post-stop-output", str(post_stop),
    ]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == (
        "STOP_ONLY_LIFECYCLE_PACKAGE_PREPARED_NOT_EXECUTED"
    )
    assert summary["service_lifecycle_actions"] == 0
    package = json.loads(output.read_text(encoding="utf-8"))
    assert audit_lifecycle([
        "--package", str(output),
        "--post-stop-contract", str(post_stop),
        "--readiness-report", str(READINESS_REPORT),
        "--expected-package-sha256", package["package_sha256"],
        "--plan",
    ]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["mode"] == "PLAN"
    assert plan["service_lifecycle_actions"] == 0
    assert plan["production_writes"] == 0
