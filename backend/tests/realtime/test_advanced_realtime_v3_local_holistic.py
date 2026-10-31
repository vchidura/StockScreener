import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from realtime.contracts import canonical_sha256
from scripts import validate_advanced_realtime_v3_host_pipeline as host_pipeline
from scripts import validate_advanced_realtime_v3_local_holistic as holistic


def _write(path, value):
    value = dict(value)
    value["semantic_sha256"] = canonical_sha256(value)
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _artifact(path):
    return {
        "path": str(path),
        "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def test_historical_host_replay_uses_session_bound_logical_clock(monkeypatch):
    session_date = date(2026, 10, 30)
    logical_clock = host_pipeline._replay_clock(session_date)
    assert logical_clock == datetime(
        2026,
        10,
        30,
        13,
        10,
        tzinfo=timezone.utc,
    )

    observed = {}

    def run(binding, **kwargs):
        observed["binding"] = binding
        observed["clock"] = kwargs["clock"]()
        return {"state": "STOPPED"}

    monkeypatch.setattr(host_pipeline, "run_acquisition", run)
    binding = SimpleNamespace(
        authority=SimpleNamespace(session_date=session_date),
    )
    result = host_pipeline._run_replay_acquisition(
        binding,
        replay_clock=logical_clock,
        provider=object(),
    )

    assert result == {"state": "STOPPED"}
    assert observed == {"binding": binding, "clock": logical_clock}
    with pytest.raises(ValueError, match="outside acquisition session"):
        host_pipeline._run_replay_acquisition(
            binding,
            replay_clock=datetime(2026, 10, 31, tzinfo=timezone.utc),
        )


def _preflight():
    return {
        "schema": "advanced-realtime-v3-local-preflight-v1",
        "status": "PASS",
        "mode": "CLOCK_PLAN",
        "session_date": "2026-11-27",
        "authorization_clocks": {
            "parent_valid_from": "2026-11-25",
            "parent_valid_through": "2026-11-25",
            "child_valid_from": "2026-11-27",
            "child_valid_through": "2026-11-27",
        },
        "effects": {"provider_requests": 0},
    }


def _source():
    return {
        "contract": holistic.SOURCE_QUALIFICATION_SCHEMA,
        "verdict": "PASS",
        "source": {"files": [{"path": "backend/example.py"}]},
        "cleanup": {"databases": [], "roles": [], "files": []},
        "effects": {"provider_rest": 0, "provider_websocket": 0},
    }


def _replay():
    return {
        "status": "PASS",
        "mode": "PROVIDER_FREE_LOCAL_REPLAY",
        "selection": {"contracts": ["a", "b", "c", "d"]},
        "effects": {"provider_requests": 0},
    }


def _rest():
    contracts = ["a", "b", "c", "d"]
    return {
        "status": "PASS",
        "provider_requests": 2,
        "cycles": [
            {"contracts": contracts},
            {"contracts": contracts},
        ],
        "effects": {"production_database_writes": 0},
    }


def _websocket():
    return {
        "status": "PASS",
        "effects": {
            "equity_websocket_sessions": 1,
            "options_websocket_sessions": 1,
        },
        "equity": {
            "connection_attempts": 1,
            "accepted_messages": 9,
            "final_state": "STOPPED",
        },
        "options": {
            "connection_attempts": 1,
            "accepted_messages": 8,
            "final_state": "STOPPED",
        },
    }


def _host():
    residents = [
        {"status": "COMPLETE", "member": member, "window_id": window}
        for window in ("one", "two")
        for member in ("INGRESS", "MARKET_STATE", "FLOW")
    ]
    return {
        "equity_ingress": {"last_raw_offset": 9, "last_event_offset": 9},
        "options_acquisition": {
            "state": "COMPLETE",
            "connection_attempts": 1,
            "forbidden_effects": {"publication": 0},
        },
        "durable_database_barrier": {
            "residents": residents,
            "windows": [
                {"window_id": "one", "state": "COMPLETE"},
                {"window_id": "two", "state": "COMPLETE"},
            ],
        },
        "host_restart": {"rehydrated": True, "same_bundle": True},
        "cleanup": {
            "database_exists": False,
            "remaining_roles": [],
            "owned_processes": [],
        },
        "effects": {"provider_requests_during_replay": 0},
    }


def _docker():
    return {
        "verdict": "PASS",
        "entrypoint": {"rehearsal_process_used": False},
        "scope": {"execution_flag": "--no-build"},
        "single_bundle_handoff": {"resident_offsets_monotonic": True},
        "effects": {"provider_rest": 0, "provider_websocket": 0},
        "cleanup": {
            "containers": [],
            "database": False,
            "network": False,
            "roles": [],
            "secrets": False,
            "volumes": [],
        },
    }


def test_workflow_executes_provider_free_steps_and_dynamic_short_session(
    monkeypatch,
    tmp_path,
):
    source = _write(tmp_path / "source.json", _source())
    admission_capture = tmp_path / "admission-capture.json"
    admission_capture.write_text("{}", encoding="utf-8")
    rest = _write(tmp_path / "rest.json", _rest())
    websocket = _write(tmp_path / "websocket.json", _websocket())
    manifest = {
        "schema": holistic.WORKFLOW_SCHEMA,
        "session_date": "2026-11-27",
        "source_qualification": _artifact(source),
        "admission_capture": _artifact(admission_capture),
        "rest_cycles": _artifact(rest),
        "websocket_capture": _artifact(websocket),
        "image_id": "sha256:" + "a" * 64,
    }
    manifest_path = _write(tmp_path / "workflow.json", manifest)
    commands = []

    def run(command, **_kwargs):
        commands.append(command)
        script = Path(command[1]).name
        if script == "validate_advanced_realtime_v3_local_preflight.py":
            _write(Path(command[command.index("--output") + 1]), _preflight())
        elif script == "replay_advanced_options_v3_capture_local.py":
            _write(Path(command[command.index("--output") + 1]), _replay())
        elif script == "validate_advanced_realtime_v3_host_pipeline.py":
            directory = Path(command[command.index("--output-directory") + 1])
            directory.mkdir()
            _write(directory / "host-pipeline-report.json", _host())
        elif script == "qualify_advanced_options_v3_docker.py":
            directory = Path(command[command.index("--output-directory") + 1])
            directory.mkdir()
            _write(
                directory
                / "advanced-options-v3-production-role-docker-qualification-test.json",
                _docker(),
            )
        else:
            raise AssertionError(script)
        return SimpleNamespace(returncode=0, stdout="PASS", stderr="")

    monkeypatch.setattr(holistic.subprocess, "run", run)
    output = tmp_path / "output"
    report, _file_sha256 = holistic.execute_workflow(SimpleNamespace(
        workflow_manifest=manifest_path,
        output_directory=output,
    ))

    assert report["verdict"] == "PASS"
    assert report["coverage"]["scheduled_cycles"] == 42
    assert report["coverage"]["provider_backed_cycles"] == 2
    assert (output / "WORKFLOW.json").is_file()
    assert len(commands) == 4
    assert all(
        "capture_advanced_options_v3_exact_cycles.py" not in command
        and "capture_advanced_realtime_v3_websockets.py" not in command
        for command in commands
    )
