import gzip
import hashlib
import json
from datetime import date

import pytest

from realtime.session_archive import ARCHIVE_SCHEMA_VERSION
from realtime.session_archive_repository import (
    _EXTRACTS,
    _PARTITION_SESSION_DATE,
)
from scripts.audit_realtime_v2_retention_archive import (
    build_retention_compatibility_plan,
    inspect_manifest,
)


def _write_archive(tmp_path, *, manifest_counts, rows):
    data = tmp_path / "option_flow_events.jsonl.gz"
    content = b"\n".join(
        json.dumps(row, sort_keys=True).encode("ascii")
        for row in rows
    ) + b"\n"
    payload = gzip.compress(content, mtime=0)
    data.write_bytes(payload)
    manifest = tmp_path / "option_flow_events.manifest.json"
    manifest.write_text(
        json.dumps({
            "archive_kind": "OPTION_FLOW_EVENTS",
            "build_id": "retention-production-v1",
            "clocks": {},
            "content_sha256": hashlib.sha256(content).hexdigest(),
            "file": data.name,
            "horizon_close_date": None,
            "policy_id": "retention-policy-v1",
            "relation_counts": manifest_counts,
            "row_count": len(rows),
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "session_date": "2026-10-19",
            "sha256": hashlib.sha256(payload).hexdigest(),
            "uncompressed_bytes": len(content),
        }),
        encoding="ascii",
    )
    return manifest


def test_manifest_audit_separates_internal_and_source_count_mismatch(tmp_path):
    manifest = _write_archive(
        tmp_path,
        manifest_counts={"rt.option_flow_events": 0},
        rows=[{
            "relation": "rt.option_flow_events",
            "row": {"event_id": "one"},
        }],
    )

    result = inspect_manifest(
        manifest,
        kind="OPTION_FLOW_EVENTS",
        session_date=date(2026, 10, 19),
        current_counts={"rt.option_flow_events": 2},
    )

    assert result["integrity_status"] == "FAIL"
    assert result["verification_code"] == "RELATION_COUNT_MISMATCH"
    assert result["manifest_matches_file_counts"] is False
    assert result["current_source_matches_file_counts"] is False


def test_option_flow_archive_sources_cover_every_contract_relation():
    assert tuple(
        row.relation for row in _EXTRACTS["OPTION_FLOW_EVENTS"]
    ) == (
        "rt.option_flow_events",
        "rt.option_flow_event_profiles",
    )
    assert tuple(
        row.relation for row in _EXTRACTS["OPTION_FLOW_CLUSTERS"]
    ) == (
        "rt.option_flow_clusters",
        "rt.option_flow_cluster_members",
    )
    assert _PARTITION_SESSION_DATE[
        "rt.option_flow_event_profiles"
    ] == "x.session_date"
    assert _PARTITION_SESSION_DATE[
        "rt.option_flow_cluster_members"
    ] == "x.session_date"


def test_malformed_selected_manifest_is_failure_shaped(tmp_path):
    manifest = tmp_path / "malformed.manifest.json"
    manifest.write_text(
        json.dumps({"archive_kind": "OPTION_FLOW_EVENTS"}),
        encoding="ascii",
    )

    result = inspect_manifest(
        manifest,
        kind="OPTION_FLOW_EVENTS",
        session_date=date(2026, 10, 19),
        current_counts={
            "rt.option_flow_events": 0,
            "rt.option_flow_event_profiles": 0,
        },
    )

    assert result["integrity_status"] == "FAIL"
    assert result["verification_code"] == "ValueError"
    assert "file identity" in result["verification_detail"]


def _write_reconciliation_report(tmp_path):
    empty_content = b""
    empty_payload = gzip.compress(empty_content, mtime=0)
    empty_file_sha256 = hashlib.sha256(empty_payload).hexdigest()
    empty_content_sha256 = hashlib.sha256(empty_content).hexdigest()
    kinds = []
    for kind, relations in (
        (
            "OPTION_FLOW_EVENTS",
            ("rt.option_flow_events", "rt.option_flow_event_profiles"),
        ),
        (
            "OPTION_FLOW_CLUSTERS",
            ("rt.option_flow_clusters", "rt.option_flow_cluster_members"),
        ),
    ):
        name = kind.lower()
        counts = {relation: 0 for relation in relations}
        kinds.append({
            "kind": kind,
            "current_source_counts": counts,
            "exact_current_archive_count": 0,
            "ledger_records": [{
                "build_id": "retention-production-v1",
                "row_count": 0,
                "sha256": empty_file_sha256,
                "uri": f"file:///archive/{name}.jsonl.gz",
                "verified_at": "2026-10-19T20:11:39+00:00",
            }],
            "manifests": [{
                "build_id": "retention-production-v1",
                "content_sha256": empty_content_sha256,
                "current_source_counts": counts,
                "current_source_matches_file_counts": True,
                "file": f"{name}.jsonl.gz",
                "file_relation_counts": counts,
                "file_row_count": 0,
                "file_sha256": empty_file_sha256,
                "integrity_status": "FAIL",
                "manifest": f"{name}.manifest.json",
                "manifest_matches_file_counts": False,
                "manifest_relation_counts": {relations[0]: 0},
                "policy_id": "retention-policy-v1",
                "verification_code": "RELATION_COUNT_MISMATCH",
                "verification_detail": "per-relation counts differ from the manifest",
            }],
            "ready": False,
        })
    report = {
        "archive_deletes": 0,
        "archive_writes": 0,
        "database": "stocks_realtime_v2",
        "database_writes": 0,
        "kinds": kinds,
        "next_action": "REVIEW_NEW_CONTENT_ADDRESSED_ARCHIVE_REVISION",
        "provider_requests": 0,
        "safe_to_resume_retention": False,
        "session_date": "2026-10-19",
        "status": "RECONCILIATION_REQUIRED",
    }
    path = tmp_path / "retention-report.json"
    path.write_text(json.dumps(report), encoding="utf-16")
    return path, report


def test_provider_free_plan_freezes_append_only_companion_reconciliation(
    tmp_path,
):
    report_path, _ = _write_reconciliation_report(tmp_path)
    report_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
    report_stat = report_path.stat()

    plan = build_retention_compatibility_plan(
        report_path,
        expected_report_sha256=report_sha256,
        expected_build_id="retention-production-v1",
        expected_policy_id="retention-policy-v1",
    )

    assert plan["status"] == "PASS"
    assert plan["mode"] == "PLAN"
    assert len(plan["frozen_plan_sha256"]) == 64
    assert not plan["safe_to_resume_retention"]
    assert not any(plan["authorization"].values())
    assert {
        row["action"] for row in plan["reconciliation"]
    } == {"APPEND_CONTENT_ADDRESSED_REVISION"}
    assert all(
        row["preserve_original_bytes_and_timestamps"]
        for row in plan["reconciliation"]
    )
    assert all(
        row["target_manifest"]["partition_coverage"]["complete"]
        and row["target_manifest"]["source_identity"]["complete"]
        for row in plan["reconciliation"]
    )
    assert report_path.stat() == report_stat


@pytest.mark.parametrize(
    "mutation, match",
    (
        (
            lambda report: report.update(archive_writes=1),
            "prohibited effect",
        ),
        (
            lambda report: report["kinds"][0][
                "current_source_counts"
            ].update({"rt.option_flow_event_profiles": 1}),
            "zero-row state",
        ),
        (
            lambda report: report["kinds"][1]["manifests"][0].update(
                verification_code="HASH_MISMATCH",
            ),
            "companion omission",
        ),
    ),
)
def test_provider_free_plan_fails_closed_on_divergence(
    tmp_path,
    mutation,
    match,
):
    report_path, report = _write_reconciliation_report(tmp_path)
    mutation(report)
    report_path.write_text(json.dumps(report), encoding="utf-16")
    report_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match=match):
        build_retention_compatibility_plan(
            report_path,
            expected_report_sha256=report_sha256,
            expected_build_id="retention-production-v1",
            expected_policy_id="retention-policy-v1",
        )
