import hashlib
import importlib
import json

import pytest


script = importlib.import_module("scripts.preflight_realtime_v2_advanced_canary")


def files(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "manifest_id": "exact-owned-manifest", "database": "stocks_realtime_v2",
    }), encoding="utf-8")
    approval = tmp_path / "approval.json"
    approval.write_text(json.dumps({
        "manifest_id": "exact-owned-manifest",
        "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "owner_decision": "approve_bounded_live_manifest",
        "conditional_execution_authorized": True,
    }), encoding="utf-8")
    return manifest, approval


def test_preflight_plan_does_not_create_a_container_or_connect(tmp_path, monkeypatch, capsys):
    manifest, approval = files(tmp_path)
    monkeypatch.setattr(script, "preflight", lambda *_args: pytest.fail("PLAN cannot execute"))
    report = tmp_path / "report.json"
    assert script.main([
        "--manifest", str(manifest), "--approval", str(approval), "--report-path", str(report),
    ]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "PLAN"
    assert result["provider_requests"] == result["live_database_writes"] == 0
    assert not report.exists()


@pytest.mark.parametrize("field", [
    "manifest_id", "manifest_sha256", "owner_decision", "conditional_execution_authorized",
])
def test_exact_approval_cannot_be_substituted(tmp_path, field):
    manifest, approval = files(tmp_path)
    content = json.loads(approval.read_text(encoding="utf-8"))
    content[field] = False if field == "conditional_execution_authorized" else "different"
    approval.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(ValueError, match="exact conditional"):
        script.approved_manifest(manifest, approval)


def test_mutated_manifest_fails_even_if_id_is_unchanged(tmp_path):
    manifest, approval = files(tmp_path)
    manifest.write_text(manifest.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError):
        script.approved_manifest(manifest, approval)


def test_schema_differences_are_exact_and_do_not_ignore_newer_optional_migrations():
    expected = [
        {"version": "001", "checksum_sha256": "a"},
        {"version": "002", "checksum_sha256": "b"},
    ]
    assert script.migration_differences(expected, expected) == {
        "missing": [], "unexpected": [], "checksum_mismatch": [],
    }
    observed = [
        {"version": "001", "checksum_sha256": "changed"},
        {"version": "003", "checksum_sha256": "c"},
    ]
    assert script.migration_differences(expected, observed) == {
        "missing": ["002"], "unexpected": ["003"], "checksum_mismatch": ["001"],
    }
    with pytest.raises(ValueError, match="duplicate"):
        script.migration_differences(expected, [*observed, observed[0]])


def test_history_and_bridge_gates_fail_closed_on_missing_or_unresolved_state():
    history, bridge = script.evaluate_history_bridge(
        [{"security_id": "spy"}],
        [
            {"interval": "1m", "bar_count": 2},
            {"interval": "5m", "bar_count": 1},
        ],
        {
            "quarantined_journals": 0,
            "inflight_journals": 0,
            "bridge_offset_lag": 0,
            "unresolved_gaps": 0,
        },
    )
    assert history["status"] == bridge["status"] == "PASS"

    history, bridge = script.evaluate_history_bridge(
        [],
        [{"interval": "1m", "bar_count": 1}],
        {
            "quarantined_journals": 1,
            "inflight_journals": 1,
            "bridge_offset_lag": 2,
            "unresolved_gaps": 1,
        },
    )
    assert history["status"] == "HISTORY_BOOTSTRAP_REQUIRED"
    assert bridge["status"] == "NOT_RUN"
    assert history["bootstrap_required"] is True
    assert history["execution_eligible"] is False
    assert history["permitted_action"] == (
        "SEPARATELY_AUTHORIZED_HISTORY_BOOTSTRAP"
    )
    assert "SPY_REFERENCE_IDENTITY_UNAVAILABLE" in history["reason_codes"]
    assert "SPY_5M_HISTORY_UNAVAILABLE" in history["reason_codes"]
    assert bridge["reason_codes"] == ["HISTORY_BOOTSTRAP_REQUIRED"]


def test_preflight_never_overwrites_an_existing_report_or_image_log(tmp_path):
    manifest, approval = files(tmp_path)
    report = tmp_path / "report.json"
    log = report.with_suffix(".image.log")
    log.write_text("immutable", encoding="utf-8")
    with pytest.raises(SystemExit):
        script.main([
            "--manifest", str(manifest), "--approval", str(approval), "--report-path", str(report),
        ])
    assert log.read_text(encoding="utf-8") == "immutable"
