from __future__ import annotations

import json
import hashlib
from datetime import date
from types import SimpleNamespace

import pytest

from realtime.advanced_realtime_v3_local_preflight import (
    authorization_clock_plan,
    build_authorization_clock_report,
    validate_manifest_authorization_clocks,
)
from realtime.contracts import canonical_sha256
from scripts import validate_advanced_realtime_v3_local_preflight as cli
from scripts import qualify_advanced_options_v3_docker as docker_cli


SESSION_DATE = date(2026, 11, 2)
PRIOR_SESSION = date(2026, 10, 30)


def _authorization_command(
    command_id,
    *,
    target,
    valid_from,
    valid_through,
    parent=False,
):
    argv = [
        "python.exe",
        "-B",
        "prepare_realtime_v2_advanced_ingress.py",
        "authorize",
        "--universe-target",
        str(target),
        "--valid-from",
        valid_from.isoformat(),
        "--valid-through",
        valid_through.isoformat(),
    ]
    if parent:
        argv.extend((
            "--parent-authorization-id",
            "{binding:equity_root_authorization_id}",
        ))
    return {"id": command_id, "argv": argv}


def _manifest(*, parent_session, child_session):
    return {
        "schema": (
            "advanced-realtime-v3-preopen-coordinator-manifest-v1"
        ),
        "session_date": SESSION_DATE.isoformat(),
        "gates": [{
            "id": "history",
            "commands": [
                _authorization_command(
                    "authorize_equity_root",
                    target=1,
                    valid_from=parent_session,
                    valid_through=parent_session,
                ),
                _authorization_command(
                    "authorize_equity_target9",
                    target=9,
                    valid_from=child_session,
                    valid_through=child_session,
                    parent=True,
                ),
            ],
        }],
    }


def test_clock_plan_uses_prior_xnys_session_for_parent():
    assert authorization_clock_plan(SESSION_DATE) == {
        "parent_valid_from": "2026-10-30",
        "parent_valid_through": "2026-10-30",
        "child_valid_from": "2026-11-02",
        "child_valid_through": "2026-11-02",
    }


def test_same_session_parent_and_child_fail_local_preflight():
    with pytest.raises(
        ValueError,
        match="authorization promotion clocks are invalid",
    ):
        validate_manifest_authorization_clocks(
            _manifest(
                parent_session=SESSION_DATE,
                child_session=SESSION_DATE,
            )
        )


def test_next_session_parent_and_child_pass_local_preflight():
    report = validate_manifest_authorization_clocks(
        _manifest(
            parent_session=PRIOR_SESSION,
            child_session=SESSION_DATE,
        )
    )

    assert report["status"] == "PASS"
    assert report["checks"]["next_session_promotion"] is True
    semantic_sha256 = report.pop("semantic_sha256")
    assert canonical_sha256(report) == semantic_sha256


def test_cli_writes_zero_effect_clock_plan(tmp_path):
    output = tmp_path / "local-preflight.json"

    assert cli.main([
        "--session-date",
        SESSION_DATE.isoformat(),
        "--output",
        str(output),
        "--execute",
    ]) == 0

    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["status"] == "PASS"
    assert report["mode"] == "CLOCK_PLAN"
    assert report["effects"]["provider_requests"] == 0
    assert report["effects"]["production_database_writes"] == 0


def test_cli_binds_valid_manifest_file_hash(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(_manifest(
            parent_session=PRIOR_SESSION,
            child_session=SESSION_DATE,
        )),
        encoding="utf-8",
    )
    output = tmp_path / "manifest-preflight.json"

    result = cli.execute(cli.parse_args([
        "--session-date",
        SESSION_DATE.isoformat(),
        "--manifest",
        str(manifest),
        "--output",
        str(output),
        "--execute",
    ]))

    report = json.loads(output.read_text(encoding="utf-8"))
    assert result["status"] == "PASS"
    assert report["manifest_file_sha256"] == result[
        "manifest_file_sha256"
    ]
    semantic_sha256 = report.pop("semantic_sha256")
    assert canonical_sha256(report) == semantic_sha256


def test_docker_qualification_requires_bound_local_preflight(tmp_path):
    report = build_authorization_clock_report(SESSION_DATE)
    path = tmp_path / "local-preflight.json"
    body = (
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    path.write_bytes(body)

    binding = docker_cli._assert_local_preflight(SimpleNamespace(
        local_preflight_report=path,
        local_preflight_file_sha256=hashlib.sha256(body).hexdigest(),
        local_preflight_semantic_sha256=report["semantic_sha256"],
        session_date=SESSION_DATE,
    ))

    assert binding["mode"] == "CLOCK_PLAN"
    assert binding["semantic_sha256"] == report["semantic_sha256"]
