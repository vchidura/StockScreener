#!/usr/bin/env python3
"""Aggregate the complete local-first V3 validation matrix without side effects."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_sha256  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.advanced_realtime_session_scheduler import (  # noqa: E402
    build_advanced_session_windows,
)


WORKFLOW_SCHEMA = "advanced-realtime-v3-local-workflow-v1"
SOURCE_QUALIFICATION_SCHEMA = (
    "advanced-options-v3-provider-free-qualification-report-v1"
)


def _load(path):
    body = path.read_bytes()
    value = json.loads(body)
    if not isinstance(value, dict):
        raise TypeError(f"report must be an object: {path}")
    semantic = value.get("semantic_sha256")
    document = dict(value)
    document.pop("semantic_sha256", None)
    if not isinstance(semantic, str) or canonical_sha256(document) != semantic:
        raise ValueError(f"report semantic identity changed: {path}")
    return value, hashlib.sha256(body).hexdigest()


def _zero(value):
    return isinstance(value, dict) and all(item == 0 for item in value.values())


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow-manifest", type=Path)
    parser.add_argument("--output-directory", type=Path)
    parser.add_argument("--local-preflight", type=Path)
    parser.add_argument("--source-qualification", type=Path)
    parser.add_argument("--admission-replay", type=Path)
    parser.add_argument("--rest-cycles", type=Path)
    parser.add_argument("--websocket-capture", type=Path)
    parser.add_argument("--host-pipeline", type=Path)
    parser.add_argument("--docker-role-qualification", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.workflow_manifest is not None:
        args.workflow_manifest = args.workflow_manifest.resolve()
        args.output_directory = (
            None
            if args.output_directory is None
            else args.output_directory.resolve()
        )
        if (
            not args.execute
            or not args.workflow_manifest.is_file()
            or args.output_directory is None
            or args.output_directory.exists()
            or not args.output_directory.parent.is_dir()
            or any(
                getattr(args, name) is not None
                for name in (
                    "local_preflight",
                    "source_qualification",
                    "admission_replay",
                    "rest_cycles",
                    "websocket_capture",
                    "host_pipeline",
                    "docker_role_qualification",
                    "output",
                )
            )
        ):
            parser.error(
                "workflow mode requires one exact manifest, a new output "
                "directory and --execute"
            )
        return args
    if args.output_directory is not None:
        parser.error("--output-directory requires --workflow-manifest")
    for name in (
        "local_preflight",
        "source_qualification",
        "admission_replay",
        "rest_cycles",
        "websocket_capture",
        "host_pipeline",
        "docker_role_qualification",
        "output",
    ):
        value = getattr(args, name)
        if value is None:
            parser.error("aggregate mode requires every report and --output")
        setattr(args, name, value.resolve())
    if (
        not all(
            getattr(args, name).is_file()
            for name in (
                "local_preflight",
                "source_qualification",
                "admission_replay",
                "rest_cycles",
                "websocket_capture",
                "host_pipeline",
                "docker_role_qualification",
            )
        )
        or args.output.exists()
        or not args.output.parent.is_dir()
        or not args.execute
    ):
        parser.error("holistic validation requires exact reports and new output")
    return args


def execute(args):
    reports = {}
    sources = {}
    for name in (
        "local_preflight",
        "source_qualification",
        "admission_replay",
        "rest_cycles",
        "websocket_capture",
        "host_pipeline",
        "docker_role_qualification",
    ):
        path = getattr(args, name)
        reports[name], file_sha256 = _load(path)
        sources[name] = {
            "path": str(path),
            "file_sha256": file_sha256,
            "semantic_sha256": reports[name]["semantic_sha256"],
        }
    preflight = reports["local_preflight"]
    qualification = reports["source_qualification"]
    replay = reports["admission_replay"]
    rest = reports["rest_cycles"]
    websocket = reports["websocket_capture"]
    host = reports["host_pipeline"]
    docker = reports["docker_role_qualification"]
    session_date = date.fromisoformat(str(preflight.get("session_date")))
    calendar = OptionExchangeCalendar("XNYS")
    if calendar.session_on_or_after(session_date) != session_date:
        raise ValueError("holistic preflight session is not an XNYS session")
    scheduled_cycles = len(build_advanced_session_windows(
        session_date,
        calendar=calendar,
    ))
    previous_session = calendar.previous_session_date(session_date)
    residents = host.get("durable_database_barrier", {}).get(
        "residents",
        (),
    )
    windows = host.get("durable_database_barrier", {}).get(
        "windows",
        (),
    )
    contracts = [
        tuple(sorted(row.get("contracts", ())))
        for row in rest.get("cycles", ())
    ]
    source_contract = qualification.get("contract")
    source_cleanup = qualification.get("cleanup")
    source_checks = qualification.get("checks")
    source_matrix_pass = (
        qualification.get("verdict") == "PASS"
        and _zero(qualification.get("effects"))
        and (
            (
                source_contract == SOURCE_QUALIFICATION_SCHEMA
                and isinstance(source_cleanup, dict)
                and not any(source_cleanup.values())
                and len(
                    qualification.get("source", {}).get("files", ())
                ) > 0
            )
            or (
                isinstance(source_checks, dict)
                and all(
                    row.get("returncode") == 0
                    for row in source_checks.values()
                )
            )
        )
    )
    checks = {
        "local_clock_preflight": (
            preflight.get("status") == "PASS"
            and preflight.get("session_date") == session_date.isoformat()
            and preflight.get("authorization_clocks") == {
                "parent_valid_from": previous_session.isoformat(),
                "parent_valid_through": previous_session.isoformat(),
                "child_valid_from": session_date.isoformat(),
                "child_valid_through": session_date.isoformat(),
            }
            and _zero(preflight.get("effects"))
        ),
        "source_and_test_matrix": source_matrix_pass,
        "sealed_admission_replay": (
            replay.get("status") == "PASS"
            and replay.get("mode") == "PROVIDER_FREE_LOCAL_REPLAY"
            and len(replay.get("selection", {}).get("contracts", ())) == 4
            and _zero(replay.get("effects"))
        ),
        "retained_rest_cycles": (
            rest.get("status") == "PASS"
            and rest.get("provider_requests") == len(contracts)
            and 1 <= len(contracts) <= scheduled_cycles
            and len(set(contracts)) == 1
            and len(contracts[0]) == 4
            and _zero(rest.get("effects"))
        ),
        "bounded_websockets": (
            websocket.get("status") == "PASS"
            and websocket.get("effects", {}).get(
                "equity_websocket_sessions"
            )
            == 1
            and websocket.get("effects", {}).get(
                "options_websocket_sessions"
            )
            == 1
            and websocket.get("equity", {}).get("connection_attempts") == 1
            and websocket.get("options", {}).get("connection_attempts") == 1
            and websocket.get("equity", {}).get("accepted_messages") == 9
            and websocket.get("options", {}).get("accepted_messages") == 8
            and websocket.get("equity", {}).get("final_state") == "STOPPED"
            and websocket.get("options", {}).get("final_state") == "STOPPED"
        ),
        "equity_ingress": (
            host.get("equity_ingress", {}).get("last_raw_offset") == 9
            and host.get("equity_ingress", {}).get("last_event_offset") == 9
        ),
        "options_acquisition": (
            host.get("options_acquisition", {}).get("state") == "COMPLETE"
            and host.get("options_acquisition", {}).get(
                "connection_attempts"
            )
            == 1
            and host.get("options_acquisition", {}).get(
                "forbidden_effects"
            )
            and _zero(
                host.get("options_acquisition", {}).get(
                    "forbidden_effects"
                )
            )
        ),
        "retained_window_resident_barrier": (
            bool(windows)
            and len(residents) == len(windows) * 3
            and all(row.get("status") == "COMPLETE" for row in residents)
            and all(row.get("state") == "COMPLETE" for row in windows)
        ),
        "host_restart_rehydration": (
            host.get("host_restart", {}).get("rehydrated") is True
            and host.get("host_restart", {}).get("same_bundle") is True
        ),
        "actual_role_docker_lifecycle": (
            docker.get("verdict") == "PASS"
            and docker.get("entrypoint", {}).get(
                "rehearsal_process_used"
            ) is False
            and docker.get("scope", {}).get("execution_flag") == "--no-build"
            and docker.get("single_bundle_handoff", {}).get(
                "resident_offsets_monotonic"
            ) is True
            and _zero(docker.get("effects"))
            and not any(docker.get("cleanup", {}).values())
        ),
        "empty_cleanup": (
            host.get("cleanup") == {
                "database_exists": False,
                "remaining_roles": [],
                "owned_processes": [],
            }
        ),
        "zero_replay_production_effects": _zero(host.get("effects")),
    }
    report = {
        "schema": "advanced-realtime-v3-local-holistic-validation-v1",
        "verdict": "PASS" if all(checks.values()) else "BLOCKED",
        "checks": checks,
        "sources": sources,
        "coverage": {
            "scheduled_cycles": scheduled_cycles,
            "provider_backed_cycles": len(contracts),
            "equity_symbols": 9,
            "options_contracts": 4,
            "resident_completions": len(residents),
            "database_windows": len(windows),
        },
        "limitations": [
            (
                "Retained provider evidence covers "
                f"{len(contracts)} cycles, not all {scheduled_cycles}."
            ),
            "Actual-role Docker evidence is provider-free diagnostic evidence, not live qualification.",
            "No live segment, full-session, or multi-session soak evidence is claimed.",
            "No production authority is granted by this report.",
        ],
        "effects": {
            "provider_requests": 0,
            "provider_websocket_sessions": 0,
            "production_database_writes": 0,
            "production_service_actions": 0,
            "docker_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
        "evidence_classes": {
            "fixture_source_matrix": "PASS",
            "retained_provider_replay": "PASS",
            "actual_role_docker": "PASS",
            "bounded_live_segment": "UNVALIDATED",
            "full_live_session": "UNVALIDATED",
            "multi_session_soak": "UNVALIDATED",
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    encoded = (
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(encoded)
    return report, hashlib.sha256(encoded).hexdigest()


def _workflow_artifact(manifest_path, value, name):
    if not isinstance(value, dict):
        raise ValueError(f"workflow artifact is absent: {name}")
    path = Path(str(value.get("path", "")))
    if not path.is_absolute():
        path = manifest_path.parent / path
    path = path.resolve()
    expected = str(value.get("file_sha256", ""))
    if (
        not path.is_file()
        or len(expected) != 64
        or hashlib.sha256(path.read_bytes()).hexdigest() != expected
    ):
        raise ValueError(f"workflow artifact identity changed: {name}")
    return path


def _load_workflow_manifest(path):
    manifest, file_sha256 = _load(path)
    if manifest.get("schema") != WORKFLOW_SCHEMA:
        raise ValueError("local workflow schema changed")
    session_date = date.fromisoformat(str(manifest.get("session_date")))
    calendar = OptionExchangeCalendar("XNYS")
    if calendar.session_on_or_after(session_date) != session_date:
        raise ValueError("local workflow session is not an XNYS session")
    artifacts = {
        name: _workflow_artifact(path, manifest.get(name), name)
        for name in (
            "source_qualification",
            "admission_capture",
            "rest_cycles",
            "websocket_capture",
        )
    }
    source, _ = _load(artifacts["source_qualification"])
    if (
        source.get("verdict") != "PASS"
        or source.get("contract") != SOURCE_QUALIFICATION_SCHEMA
        or not isinstance(source.get("cleanup"), dict)
        or any(source["cleanup"].values())
        or not _zero(source.get("effects"))
    ):
        raise ValueError("workflow source qualification is not reusable")
    image_id = str(manifest.get("image_id", ""))
    if (
        not image_id.startswith("sha256:")
        or len(image_id) != 71
    ):
        raise ValueError("workflow image identity is invalid")
    return {
        "manifest": manifest,
        "manifest_file_sha256": file_sha256,
        "session_date": session_date,
        "artifacts": artifacts,
        "source": source,
        "image_id": image_id,
    }


def _write_workflow_json(path, value):
    encoded = (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _run_workflow_step(name, command, output_directory):
    environment = dict(os.environ)
    removed = []
    for key in tuple(environment):
        normalized = key.upper()
        if (
            "POLYGON" in normalized
            or "MASSIVE" in normalized
            or normalized.endswith("PROVIDER_KEY")
        ):
            removed.append(key)
            environment.pop(key, None)
    completed = subprocess.run(
        command,
        cwd=str(BACKEND_DIR.parent),
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    receipt = {
        "schema": "advanced-realtime-v3-local-workflow-step-v1",
        "name": name,
        "command": command,
        "returncode": completed.returncode,
        "provider_credentials_forwarded": False,
        "removed_provider_environment_names": sorted(removed),
        "stdout_sha256": hashlib.sha256(
            completed.stdout.encode("utf-8")
        ).hexdigest(),
        "stderr_sha256": hashlib.sha256(
            completed.stderr.encode("utf-8")
        ).hexdigest(),
    }
    receipt["semantic_sha256"] = canonical_sha256(receipt)
    _write_workflow_json(output_directory / f"STEP-{name}.json", receipt)
    (output_directory / f"STEP-{name}.stdout.log").write_text(
        completed.stdout,
        encoding="utf-8",
    )
    (output_directory / f"STEP-{name}.stderr.log").write_text(
        completed.stderr,
        encoding="utf-8",
    )
    if completed.returncode:
        raise RuntimeError(
            f"local workflow step failed: {name} "
            f"(returncode {completed.returncode})"
        )
    return receipt


def execute_workflow(args):
    workflow = _load_workflow_manifest(args.workflow_manifest)
    output = args.output_directory
    output.mkdir()
    session_date = workflow["session_date"].isoformat()
    artifacts = workflow["artifacts"]
    source = workflow["source"]
    python = str(Path(sys.executable).resolve())
    scripts = BACKEND_DIR / "scripts"
    local_preflight = output / "local-preflight.json"
    admission_replay = output / "admission-replay.json"
    host_directory = output / "host-pipeline"
    docker_directory = output / "docker-role-qualification"
    commands = (
        (
            "LOCAL-PREFLIGHT",
            [
                python,
                str(scripts / "validate_advanced_realtime_v3_local_preflight.py"),
                "--session-date", session_date,
                "--output", str(local_preflight),
                "--execute",
            ],
        ),
        (
            "ADMISSION-REPLAY",
            [
                python,
                str(scripts / "replay_advanced_options_v3_capture_local.py"),
                "--capture", str(artifacts["admission_capture"]),
                "--capture-file-sha256",
                hashlib.sha256(
                    artifacts["admission_capture"].read_bytes()
                ).hexdigest(),
                "--output", str(admission_replay),
                "--execute",
            ],
        ),
        (
            "HOST-PIPELINE",
            [
                python,
                str(scripts / "validate_advanced_realtime_v3_host_pipeline.py"),
                "--session-date", session_date,
                "--websocket-capture", str(artifacts["websocket_capture"]),
                "--websocket-file-sha256",
                hashlib.sha256(
                    artifacts["websocket_capture"].read_bytes()
                ).hexdigest(),
                "--rest-summary", str(artifacts["rest_cycles"]),
                "--rest-file-sha256",
                hashlib.sha256(
                    artifacts["rest_cycles"].read_bytes()
                ).hexdigest(),
                "--output-directory", str(host_directory),
                "--execute",
            ],
        ),
    )
    receipts = [
        _run_workflow_step(name, command, output)
        for name, command in commands
    ]
    local_preflight_report, local_preflight_file_sha256 = _load(
        local_preflight
    )
    source_file_sha256 = hashlib.sha256(
        artifacts["source_qualification"].read_bytes()
    ).hexdigest()
    docker_command = [
        python,
        str(scripts / "qualify_advanced_options_v3_docker.py"),
        "--image-id", workflow["image_id"],
        "--session-date", session_date,
        "--output-directory", str(docker_directory),
        "--qualification-semantic-sha256", source["semantic_sha256"],
        "--qualification-file-sha256", source_file_sha256,
        "--local-preflight-report", str(local_preflight),
        "--local-preflight-semantic-sha256",
        local_preflight_report["semantic_sha256"],
        "--local-preflight-file-sha256", local_preflight_file_sha256,
        "--execute",
    ]
    receipts.append(_run_workflow_step(
        "DOCKER-ROLE-QUALIFICATION",
        docker_command,
        output,
    ))
    docker_reports = tuple(docker_directory.glob(
        "advanced-options-v3-production-role-docker-qualification-*.json"
    ))
    host_report = host_directory / "host-pipeline-report.json"
    if len(docker_reports) != 1 or not host_report.is_file():
        raise RuntimeError("local workflow outputs are incomplete")
    aggregate_args = SimpleNamespace(
        local_preflight=local_preflight,
        source_qualification=artifacts["source_qualification"],
        admission_replay=admission_replay,
        rest_cycles=artifacts["rest_cycles"],
        websocket_capture=artifacts["websocket_capture"],
        host_pipeline=host_report,
        docker_role_qualification=docker_reports[0],
        output=output / "holistic-report.json",
    )
    report, file_sha256 = execute(aggregate_args)
    workflow_receipt = {
        "schema": "advanced-realtime-v3-local-workflow-receipt-v1",
        "status": "PASS" if report["verdict"] == "PASS" else "BLOCKED",
        "session_date": session_date,
        "workflow_manifest": str(args.workflow_manifest),
        "workflow_manifest_file_sha256": workflow[
            "manifest_file_sha256"
        ],
        "steps": [row["semantic_sha256"] for row in receipts],
        "holistic_report": str(aggregate_args.output),
        "holistic_semantic_sha256": report["semantic_sha256"],
        "holistic_file_sha256": file_sha256,
        "effects": report["effects"],
    }
    workflow_receipt["semantic_sha256"] = canonical_sha256(
        workflow_receipt
    )
    _write_workflow_json(output / "WORKFLOW.json", workflow_receipt)
    return report, file_sha256


def main(argv=None):
    args = parse_args(argv)
    report, file_sha256 = (
        execute_workflow(args)
        if args.workflow_manifest is not None
        else execute(args)
    )
    print(json.dumps({
        "verdict": report["verdict"],
        "semantic_sha256": report["semantic_sha256"],
        "file_sha256": file_sha256,
        "checks": report["checks"],
        "effects": report["effects"],
    }, indent=2, sort_keys=True))
    return 0 if report["verdict"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
