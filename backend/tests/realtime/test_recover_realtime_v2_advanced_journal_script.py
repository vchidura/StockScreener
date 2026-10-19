import hashlib
import importlib
import json
from types import SimpleNamespace

import pytest


script = importlib.import_module(
    "scripts.recover_realtime_v2_advanced_journal"
)


def test_recovery_cli_separates_plan_and_execute_evidence(tmp_path):
    root = tmp_path / "journals"
    root.mkdir()
    report = tmp_path / "plan.json"
    base = [
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--journal-root",
        str(root),
        "--plan-id",
        "22222222-2222-2222-2222-222222222222",
        "--report-path",
        str(report),
    ]
    digest = "a" * 64
    authorized_counts = [
        "--expected-raw-offset",
        "9",
        "--expected-normalized-events",
        "9",
        "--expected-persisted-bars",
        "9",
    ]

    planned = script.parse_args([
        *base,
        "--plan",
        "--expected-journal-sha256",
        digest,
        "--expected-wal-sha256",
        digest,
        *authorized_counts,
    ])

    assert planned.plan
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--plan"])
    with pytest.raises(SystemExit):
        script.parse_args([
            *base,
            "--execute",
            "--expected-journal-sha256",
            digest,
            "--expected-wal-sha256",
            digest,
            *authorized_counts,
        ])


def test_execute_plan_report_is_hash_and_identity_bound(tmp_path):
    root = tmp_path / "journals"
    root.mkdir()
    plan = tmp_path / "plan.json"
    payload = {
        "contract": script.CONTRACT,
        "mode": "PLAN",
        "status": "READY",
        "registration": {
            "plan_id": "22222222-2222-2222-2222-222222222222",
        },
    }
    plan.write_text(json.dumps(payload), encoding="utf-8")
    digest = hashlib.sha256(plan.read_bytes()).hexdigest()
    args = script.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--journal-root",
        str(root),
        "--plan-id",
        "22222222-2222-2222-2222-222222222222",
        "--report-path",
        str(tmp_path / "execute.json"),
        "--execute",
        "--plan-report",
        str(plan),
        "--plan-report-sha256",
        digest,
    ])

    assert script._load_plan_report(args) == payload
    args.plan_report_sha256 = "b" * 64
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        script._load_plan_report(args)


def test_zero_message_snapshot_gate_rejects_any_usage():
    registration = SimpleNamespace(
        status="QUARANTINED",
        relative_path="journal.sqlite",
        sealed_files_sha256=None,
        raw_committed_offset=0,
        normalized_committed_offset=0,
        bridge_committed_offset=0,
    )
    snapshot = {
        "registration": registration,
        "quarantine": {
            "reason": "MISSING_JOURNAL",
            "detail": "journal.sqlite",
        },
        "plan": {
            "status": "BLOCKED",
            "terminal_outcome": "BLOCKED_CONFIGURATION",
        },
        "session": {
            "status": "CONNECTING",
            "ended_at": None,
            "event_time_watermark": None,
            "received_frame_count": 0,
            "received_byte_count": 0,
            "admitted_market_message_count": 0,
            "admitted_market_byte_count": 0,
            "last_raw_offset": 0,
            "last_event_offset": 0,
        },
        "usage": {
            "stream_session_count": 1,
            "connection_attempt_count": 1,
            "decoded_byte_count": 0,
            "rest_repair_request_count": 0,
            "rest_repair_decoded_byte_count": 0,
        },
        "bar_revision_count": 0,
        "incomplete_journal_count": 0,
        "recovery_events": (),
    }

    script._validate_snapshot(snapshot)
    snapshot["session"]["received_byte_count"] = 1
    with pytest.raises(ValueError, match="zero-message"):
        script._validate_snapshot(snapshot)
