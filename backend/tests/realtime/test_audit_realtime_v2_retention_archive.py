import gzip
import hashlib
import json
from datetime import date

from realtime.session_archive import ARCHIVE_SCHEMA_VERSION
from realtime.session_archive_repository import (
    _EXTRACTS,
    _PARTITION_SESSION_DATE,
)
from scripts.audit_realtime_v2_retention_archive import inspect_manifest


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
