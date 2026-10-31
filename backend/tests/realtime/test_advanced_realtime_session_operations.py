from __future__ import annotations

import json
import re
import shutil
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime.advanced_realtime_session_operations import (
    ACQUISITION_SERVICES,
    DOWNSTREAM_SERVICES,
    AdvancedSessionOperation,
    DownstreamBuildSegment,
    build_diagnostic_authorization,
    build_downstream_transition,
    build_preopen_freeze_report,
    diagnostic_authorization_from_payload,
    diagnostic_authorization_payload,
    downstream_transition_from_payload,
    downstream_transition_payload,
    evaluate_session_evidence,
    expected_session_cutoffs,
    preopen_freeze_template,
)
from realtime.contracts import canonical_sha256
from scripts import assemble_advanced_realtime_v3_freeze as freeze_cli
from scripts import render_advanced_realtime_v3_freeze_inputs as freeze_render_cli
from scripts import run_advanced_realtime_session_scheduler as cli


SESSION_DATE = date(2026, 10, 29)
COHORT_ID = UUID("11111111-2222-3333-4444-555555555555")
OLD_IMAGE = f"sha256:{'1' * 64}"
NEW_IMAGE = f"sha256:{'2' * 64}"


def _complete_freeze(session_date=SESSION_DATE):
    value = preopen_freeze_template(session_date)
    value.update({
        "operator": "operator",
        "approver": "approver",
        "freeze_completed_at_utc": (
            f"{session_date.isoformat()}T13:00:00+00:00"
        ),
    })
    normalization = value["normalization"]
    normalization.update({
        "stored_active_profile_sha256": "a" * 64,
        "rendered_source_profile_sha256": "a" * 64,
        "normalizer_source_sha256": "b" * 64,
        "manifest_cohort_sha256": "c" * 64,
        "journal_generation": (
            "33333333-4444-5555-6666-777777777777"
        ),
    })
    normalization["evidence"] = {
        name: True
        for name in normalization["evidence"]
    }
    finalization = value["finalization"]
    finalization["policy_command_sha256"] = "d" * 64
    finalization["evidence"] = {
        name: True
        for name in finalization["evidence"]
    }
    budgets = value["provider_budgets"]
    budgets["equity"].update({
        "connection_attempts": 3,
        "websocket_decoded_byte_ceiling": 100_000_000,
        "catch_up_mode": "DISABLED",
        "repair_request_ceiling": 0,
        "repair_response_byte_ceiling": 0,
        "repair_total_decoded_byte_ceiling": 0,
        "scope_sha256": "",
        "authority_unconsumed": True,
    })
    budgets["options"].update({
        "connection_attempts": 3,
        "socket_message_ceiling": 1_000_000,
        "socket_decoded_byte_ceiling": 100_000_000,
        "response_byte_ceiling": 1_000_000,
        "total_rest_decoded_byte_ceiling": (
            finalization["cutoff_count"] * 1_000_000
        ),
        "scope_sha256": "",
    })
    budgets["evidence"] = {
        name: True
        for name in budgets["evidence"]
    }
    value["identities"] = {
        "acquisition_image_id": OLD_IMAGE,
        "equity_authority_scope_sha256": "3" * 64,
        "options_session_authority_sha256": "4" * 64,
        "rendered_compose_sha256": "5" * 64,
        "package_sha256": "6" * 64,
        "bundle_sha256": "7" * 64,
        "pre_open_audit_report_sha256": "8" * 64,
    }
    computed = build_preopen_freeze_report(value)[
        "computed_budget_scope_sha256"
    ]
    budgets["equity"]["scope_sha256"] = computed["equity"]
    budgets["options"]["scope_sha256"] = computed["options"]
    return value


def _segments():
    return (
        DownstreamBuildSegment(
            segment_id="original",
            source_sha256="a" * 64,
            backend_image_id=OLD_IMAGE,
            first_window_index=1,
            last_window_index=39,
        ),
        DownstreamBuildSegment(
            segment_id="candidate",
            source_sha256="b" * 64,
            backend_image_id=NEW_IMAGE,
            first_window_index=40,
            last_window_index=78,
        ),
    )


def _transition(
    cycle_sha256="c" * 64,
    acquisition_sha256="d" * 64,
    package_sha256="6" * 64,
    bundle_sha256="7" * 64,
):
    old, new = _segments()
    return build_downstream_transition(
        authorization_reference="transition-test",
        session_date=SESSION_DATE,
        completed_window_index=39,
        completed_cycle_report_sha256=cycle_sha256,
        preserved_acquisition_identity_sha256=acquisition_sha256,
        preserved_package_sha256=package_sha256,
        preserved_bundle_sha256=bundle_sha256,
        old_segment=old,
        new_segment=new,
        services=(
            "realtime-v2-canonical-stock-alerts",
            "realtime-v2-options-p8-window",
        ),
        tier_a_qualification_sha256="e" * 64,
        tier_b_qualification_sha256="f" * 64,
    )


def _cycle(index, segment_id):
    release_id = f"release-{index}"
    return {
        "status": "COMPLETE_PUBLISHED_PINNED",
        "window": {"index": index},
        "release_id": release_id,
        "read_pin_release_id": release_id,
        "stored_reader_count": 4,
        "stock_alert_evaluation_count": 9,
        "stock_alert_unavailable_count": 9,
        "stock_alert_reason_codes_exact": True,
        "e_o_seconds": 5,
        "downstream_build_segment_id": segment_id,
        "forbidden_effects": {
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
    }


def _segment_evidence(segment):
    return {
        "segment_id": segment.segment_id,
        "source_sha256": segment.source_sha256,
        "backend_image_id": segment.backend_image_id,
        "first_window_index": segment.first_window_index,
        "last_window_index": segment.last_window_index,
        "restart_rehydration_passed": True,
        "publication_passed": True,
        "read_pin_passed": True,
        "stored_readers_passed": True,
    }


def _session_evidence(segments, transitions):
    cycles = []
    for segment in segments:
        cycles.extend(
            _cycle(index, segment.segment_id)
            for index in range(
                segment.first_window_index,
                segment.last_window_index + 1,
            )
        )
    return {
        "schema": "advanced-realtime-v3-session-evidence-v1",
        "session_date": "2026-10-29",
        "acquisition": {
            "preopen_identity_sha256": "d" * 64,
            "close_identity_sha256": "d" * 64,
            "opening_capture_complete": True,
            "final_capture_complete": True,
            "cutoffs": list(expected_session_cutoffs(SESSION_DATE)),
            "forbidden_changes": 0,
        },
        "cycles": cycles,
        "build_segments": [
            _segment_evidence(segment)
            for segment in segments
        ],
        "transitions": transitions,
        "cleanup": {
            "containers": [],
            "stop_returncode": 0,
            "remove_returncode": 0,
        },
    }


def test_preopen_freeze_is_go_only_when_every_field_matches():
    report = build_preopen_freeze_report(_complete_freeze())

    assert report["decision"] == "GO"
    assert report["failed_fields"] == []
    assert report["provider_effects"] == 0
    assert report["production_effects"] == 0

    invalid = _complete_freeze()
    invalid["provider_budgets"]["options"][
        "total_rest_decoded_byte_ceiling"
    ] = 78_000_001
    invalid["operator"] = ""
    refused = build_preopen_freeze_report(invalid)

    assert refused["decision"] == "NO_GO"
    assert "operator" in refused["failed_fields"]
    assert (
        "provider_budgets.options.total_rest_decoded_byte_ceiling"
        in refused["failed_fields"]
    )


def test_operating_contracts_accept_another_xnys_session():
    session_date = date(2026, 10, 30)
    freeze = _complete_freeze(session_date)
    report = build_preopen_freeze_report(freeze)

    assert report["decision"] == "GO"
    assert report["session_date"] == session_date.isoformat()
    authorization = build_diagnostic_authorization(
        authorization_reference="diagnostic-20261030-test",
        owner="diagnostic-owner",
        session_date=session_date,
        cohort_id=COHORT_ID,
        source_sha256="a" * 64,
        backend_image_id=OLD_IMAGE,
        input_evidence_sha256="b" * 64,
    )
    assert authorization.session_date == session_date


def test_freeze_assembler_derives_scopes_and_writes_go(tmp_path, capsys):
    session_date = date(2026, 10, 30)
    complete = _complete_freeze(session_date)
    inputs = {
        "schema": freeze_cli.INPUT_SCHEMA,
        "session_date": session_date.isoformat(),
        "sections": {
            name: complete[name]
            for name in freeze_cli.SECTIONS
        },
    }
    inputs["sections"]["provider_budgets"]["equity"][
        "scope_sha256"
    ] = ""
    inputs["sections"]["provider_budgets"]["options"][
        "scope_sha256"
    ] = ""
    inputs["sections"]["identities"][
        "equity_authority_scope_sha256"
    ] = ""
    inputs_path = tmp_path / "freeze-inputs.json"
    record_path = tmp_path / "freeze-record.json"
    report_path = tmp_path / "freeze-report.json"
    inputs_path.write_text(json.dumps(inputs), encoding="utf-8")

    assert freeze_cli.main([
        "--session-date",
        session_date.isoformat(),
        "--operator",
        "operator",
        "--approver",
        "approver",
        "--freeze-completed-at",
        "2026-10-30T13:00:00+00:00",
        "--inputs",
        str(inputs_path),
        "--freeze-record",
        str(record_path),
        "--freeze-report",
        str(report_path),
        "--execute",
    ]) == 0

    summary = json.loads(capsys.readouterr().out)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert summary["decision"] == "GO"
    assert report["decision"] == "GO"
    assert report["failed_fields"] == []


def test_freeze_input_renderer_requires_exact_named_bindings(
    tmp_path,
    capsys,
):
    template = {
        "schema": freeze_cli.INPUT_SCHEMA,
        "session_date": "2026-10-30",
        "sections": {
            "normalization": {
                "stored_active_profile_sha256": "${profile_sha256}",
            },
        },
    }
    template_path = tmp_path / "template.json"
    output_path = tmp_path / "rendered.json"
    template_path.write_text(json.dumps(template), encoding="utf-8")

    assert freeze_render_cli.main([
        "--session-date",
        "2026-10-30",
        "--template",
        str(template_path),
        "--binding",
        f"profile_sha256={'a' * 64}",
        "--output",
        str(output_path),
        "--execute",
    ]) == 0

    summary = json.loads(capsys.readouterr().out)
    rendered = json.loads(output_path.read_text(encoding="utf-8"))
    assert summary["binding_count"] == 1
    assert (
        rendered["sections"]["normalization"][
            "stored_active_profile_sha256"
        ]
        == "a" * 64
    )


def test_diagnostic_identity_is_owned_and_effect_free():
    authorization = build_diagnostic_authorization(
        authorization_reference="diagnostic-test",
        owner="diagnostic-owner",
        session_date=SESSION_DATE,
        cohort_id=COHORT_ID,
        source_sha256="a" * 64,
        backend_image_id=OLD_IMAGE,
        input_evidence_sha256="b" * 64,
    )
    payload = diagnostic_authorization_payload(authorization)

    assert payload["operation"] == "DIAGNOSTIC_NON_SERVING"
    assert set(payload["effects"].values()) == {0}
    assert diagnostic_authorization_from_payload(payload) == authorization

    payload["effects"]["pointer"] = 1
    with pytest.raises(ValueError, match="contract changed"):
        diagnostic_authorization_from_payload(payload)


def test_transition_contract_rejects_acquisition_roles():
    transition = _transition()
    payload = downstream_transition_payload(transition)

    assert downstream_transition_from_payload(payload) == transition
    assert set(transition.services).issubset(DOWNSTREAM_SERVICES)
    assert set(transition.services).isdisjoint(ACQUISITION_SERVICES)

    with pytest.raises(ValueError, match="scope is invalid"):
        build_downstream_transition(
            authorization_reference="transition-test",
            session_date=SESSION_DATE,
            completed_window_index=39,
            completed_cycle_report_sha256="c" * 64,
            preserved_acquisition_identity_sha256="d" * 64,
            preserved_package_sha256="6" * 64,
            preserved_bundle_sha256="7" * 64,
            old_segment=_segments()[0],
            new_segment=_segments()[1],
            services=("realtime-v2-advanced-finalizer",),
            tier_a_qualification_sha256="e" * 64,
            tier_b_qualification_sha256="f" * 64,
        )


def test_status_reports_single_build_and_controlled_transition_verdicts():
    single = DownstreamBuildSegment(
        segment_id="single",
        source_sha256="a" * 64,
        backend_image_id=OLD_IMAGE,
        first_window_index=1,
        last_window_index=78,
    )
    single_status = evaluate_session_evidence(
        _session_evidence((single,), [])
    )

    assert single_status["verdicts"][
        "PASS_ACQUISITION_FULL_SESSION"
    ]["status"] == "PASS"
    assert single_status["verdicts"][
        "PASS_END_TO_END_SEGMENT"
    ]["status"] == "PASS"
    assert single_status["verdicts"][
        "PASS_FULL_SESSION_SINGLE_BUILD"
    ]["status"] == "PASS"
    assert single_status["verdicts"][
        "PASS_CONTROLLED_TRANSITION"
    ]["status"] == "NOT_QUALIFIED"

    old, new = _segments()
    transition_status = evaluate_session_evidence(
        _session_evidence(
            (old, new),
            [{
                "status": "COMPLETE",
                "acquisition_containers_preserved": True,
                "cycle_backdated": False,
                "old_segment_id": old.segment_id,
                "new_segment_id": new.segment_id,
            }],
        )
    )

    assert transition_status["verdicts"][
        "PASS_CONTROLLED_TRANSITION"
    ]["status"] == "PASS"
    assert transition_status["verdicts"][
        "PASS_FULL_SESSION_SINGLE_BUILD"
    ]["status"] == "NOT_QUALIFIED"


def test_transition_command_recreates_only_downstream_roles(
    tmp_path,
    monkeypatch,
):
    cycle = {
        "status": "COMPLETE_PUBLISHED_PINNED",
        "window": {"index": 39},
    }
    cycle["semantic_sha256"] = canonical_sha256(cycle)
    cycle_path = tmp_path / "cycle.json"
    cycle_path.write_text(json.dumps(cycle), encoding="utf-8")
    freeze = build_preopen_freeze_report(_complete_freeze())
    transition = _transition(
        cycle["semantic_sha256"],
        acquisition_sha256=freeze["freeze_sha256"],
    )
    freeze_path = tmp_path / "freeze.json"
    freeze_path.write_text(json.dumps(freeze), encoding="utf-8")
    package_path = tmp_path / "package.json"
    package_path.write_text(
        json.dumps({"package_sha256": "6" * 64}),
        encoding="utf-8",
    )
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(
        json.dumps({"bundle_sha256": "7" * 64}),
        encoding="utf-8",
    )
    calls = []
    acquisition_state = {
        service: {
            "container_id": f"{service}-container",
            "image_id": OLD_IMAGE,
            "state": "running",
        }
        for service in ACQUISITION_SERVICES
    }

    def state(_args, services, *, backend_image_id):
        if tuple(services) == ACQUISITION_SERVICES:
            return acquisition_state
        return {
            service: {
                "container_id": f"{service}-{backend_image_id[-4:]}",
                "image_id": backend_image_id,
                "state": "running",
            }
            for service in services
        }

    monkeypatch.setattr(cli, "_assert_local_image", lambda *_: None)
    monkeypatch.setattr(cli, "_container_state", state)
    monkeypatch.setattr(
        cli,
        "_compose_command",
        lambda _args, *values, **kwargs: calls.append(
            (values, kwargs)
        ),
    )
    output = tmp_path / "transition-report.json"
    args = SimpleNamespace(
        transition_contract=transition,
        freeze_report=freeze_path,
        completed_cycle_report=cycle_path,
        package=package_path,
        bundle=bundle_path,
        output=output,
    )

    assert cli._run_transition(args) == 0
    up = calls[0]
    assert up[0][:6] == (
        "up",
        "--detach",
        "--no-deps",
        "--no-build",
        "--force-recreate",
        "--wait",
    )
    assert set(up[0][6:]) == set(transition.services)
    assert set(up[0][6:]).isdisjoint(ACQUISITION_SERVICES)
    assert up[1]["backend_image_id"] == NEW_IMAGE
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["acquisition_containers_preserved"] is True
    assert report["operation"] == (
        AdvancedSessionOperation
        .CONTROLLED_DOWNSTREAM_TRANSITION.value
    )


def test_windows_session_task_is_catch_up_disabled_and_single_owner():
    script = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "register_advanced_realtime_v3_session_task.ps1"
    ).read_text(encoding="utf-8")

    assert "[string]$Mode = 'Plan'" in script
    assert "CatchUpEnabled = $false" in script
    assert "StartWhenAvailable = $false" in script
    assert "-MultipleInstances IgnoreNew" in script
    assert "no second coordinator was registered" in script
    assert "'--continuous'" in script
    assert "'--execute'" in script
    assert "[string]$AcquisitionBinding" in script
    assert "[string]$SecretDirectory" in script
    assert "'--acquisition-binding'" in script
    assert "$acquisitionBindingPath" in script
    assert "'--secret-directory'" in script
    assert "$secretDirectoryPath" in script
    assert "-PathType Container" in script
    assert "requires every frozen artifact" in script


def test_windows_session_task_plan_satisfies_scheduler_parser_contract(
    tmp_path,
    monkeypatch,
):
    powershell = shutil.which("powershell.exe") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    script = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "register_advanced_realtime_v3_session_task.ps1"
    )
    artifacts = {
        name: tmp_path / name
        for name in (
            "python.exe",
            "authorization.json",
            "freeze.json",
            "package.json",
            "bundle.json",
            "acquisition-binding.json",
            "runtime.env",
            "docker.exe",
        )
    }
    for path in artifacts.values():
        path.write_text("{}\n", encoding="utf-8")
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    output = tmp_path / "new-output"
    scheduled_at = datetime.now(timezone.utc) + timedelta(days=2)
    completed = subprocess.run(
        (
            powershell,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-Mode",
            "Plan",
            "-TaskName",
            "provider-free-parser-contract",
            "-ScheduledAt",
            scheduled_at.isoformat(),
            "-Authorization",
            str(artifacts["authorization.json"]),
            "-FreezeReport",
            str(artifacts["freeze.json"]),
            "-Package",
            str(artifacts["package.json"]),
            "-Bundle",
            str(artifacts["bundle.json"]),
            "-AcquisitionBinding",
            str(artifacts["acquisition-binding.json"]),
            "-SecretDirectory",
            str(secrets),
            "-OutputDirectory",
            str(output),
            "-EnvFile",
            str(artifacts["runtime.env"]),
            "-Python",
            str(artifacts["python.exe"]),
            "-DockerExecutable",
            str(artifacts["docker.exe"]),
        ),
        check=True,
        capture_output=True,
        text=True,
    )
    plan = json.loads(completed.stdout)
    assert plan["ReadyToInstall"] is True
    assert plan["MissingFiles"] == []
    assert plan["MissingDirectories"] == []
    tokens = [
        value.replace('\\"', '"')
        for value in re.findall(r'"((?:\\"|[^"])*)"', plan["Arguments"])
    ]
    assert tokens[:3] == ["-B", str(script.with_name(
        "run_advanced_realtime_session_scheduler.py"
    )), "run"]
    monkeypatch.setattr(cli, "_load_authority", lambda _path: object())
    parsed = cli.parse_args(tokens[2:])
    assert parsed.acquisition_binding == artifacts[
        "acquisition-binding.json"
    ].resolve()
    assert parsed.secret_directory == secrets.resolve()
