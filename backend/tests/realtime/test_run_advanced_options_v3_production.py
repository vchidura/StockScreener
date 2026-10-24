from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from realtime.advanced_options_control_plane import (
    advanced_options_history_binding_payload,
    build_advanced_options_history_binding,
)
from realtime.advanced_options_service import AdvancedOptionsProcessKind
from realtime.advanced_options_v3_history import (
    AdvancedOptionsHistoryBindingIdentityError,
)
from realtime.contracts import canonical_sha256
from scripts import (
    run_advanced_options_v3_market_loop as market_loop,
    run_advanced_options_v3_production as cli,
)


DATABASE = "stocks_realtime_v2"


def _database_args():
    return [
        "--database",
        DATABASE,
        "--confirm-database-name",
        DATABASE,
    ]


def _artifact_args(tmp_path):
    return [
        "--package",
        str(tmp_path / "PACKAGE.json"),
        "--bundle",
        str(tmp_path / "SESSION-BUNDLE.json"),
    ]


def _capture(monkeypatch, name, result):
    calls = []

    def invoke(*args):
        calls.append(args)
        return result

    monkeypatch.setattr(cli, name, invoke)
    return calls


def test_prepare_cli_dispatches_exact_production_command(
    monkeypatch,
    tmp_path,
    capsys,
):
    expected = {"status": "PREPARED"}
    calls = _capture(monkeypatch, "_prepare", expected)

    assert cli.main([
        "prepare",
        *_database_args(),
        "--package-source",
        str(tmp_path / "source.json"),
        "--production-binding",
        str(tmp_path / "binding.json"),
        "--live-observation",
        str(tmp_path / "observation.json"),
        "--output-directory",
        str(tmp_path / "new-output"),
        "--execute",
    ]) == 0

    assert len(calls) == 1
    assert calls[0][0].command == "prepare"
    assert json.loads(capsys.readouterr().out) == expected


def test_start_residents_cli_starts_only_resident_roles(
    monkeypatch,
    tmp_path,
    capsys,
):
    expected = {"status": "STARTED"}
    calls = _capture(monkeypatch, "_compose", expected)

    assert cli.main([
        "start-residents",
        *_artifact_args(tmp_path),
        "--docker-executable",
        str(tmp_path / "docker.exe"),
        "--execute",
    ]) == 0

    assert calls[0][1] == cli.RESIDENT_KINDS
    assert AdvancedOptionsProcessKind.WINDOW not in calls[0][1]
    assert json.loads(capsys.readouterr().out) == expected


def test_authorize_window_cli_preserves_exact_clocks_and_receipt(
    monkeypatch,
    tmp_path,
    capsys,
):
    expected = {"status": "AUTHORIZED"}
    calls = _capture(monkeypatch, "_authorize_window", expected)
    receipt_id = UUID("11111111-2222-3333-4444-555555555555")

    assert cli.main([
        "authorize-window",
        *_database_args(),
        *_artifact_args(tmp_path),
        "--authorization-reference",
        "owned-window",
        "--publication-authorization-reference",
        "owned-publication",
        "--window-key",
        "v3-qualification-1",
        "--options-cutoff",
        "2026-10-23T13:00:00+00:00",
        "--window-deadline",
        "2026-10-23T13:03:00+00:00",
        "--equity-receipt-id",
        str(receipt_id),
        "--execute",
    ]) == 0

    args = calls[0][0]
    assert args.equity_receipt_id == receipt_id
    assert args.options_cutoff.isoformat() == "2026-10-23T13:00:00+00:00"
    assert args.window_deadline.isoformat() == "2026-10-23T13:03:00+00:00"
    assert json.loads(capsys.readouterr().out) == expected


def test_run_window_cli_starts_only_window_role(
    monkeypatch,
    tmp_path,
    capsys,
):
    expected = {"status": "STARTED"}
    calls = _capture(monkeypatch, "_compose", expected)

    assert cli.main([
        "run-window",
        *_artifact_args(tmp_path),
        "--docker-executable",
        str(tmp_path / "docker.exe"),
        "--execute",
    ]) == 0

    assert calls[0][1] == (AdvancedOptionsProcessKind.WINDOW,)
    assert json.loads(capsys.readouterr().out) == expected


def test_status_cli_is_read_only_and_needs_no_execute(
    monkeypatch,
    tmp_path,
    capsys,
):
    expected = {"status": "AVAILABLE"}
    calls = _capture(monkeypatch, "_status", expected)

    assert cli.main([
        "status",
        *_database_args(),
        *_artifact_args(tmp_path),
    ]) == 0

    assert len(calls) == 1
    assert json.loads(capsys.readouterr().out) == expected


def test_docker_role_mode_uses_production_service_boundary(
    monkeypatch,
    tmp_path,
):
    calls = []
    monkeypatch.setattr(
        cli,
        "run_p8_docker_service",
        lambda *args: calls.append(args) or 0,
    )

    assert cli.main([
        "--docker-service-kind",
        "FLOW",
        "--package",
        str(tmp_path / "PACKAGE.json"),
        "--execution-bundle",
        str(tmp_path / "SESSION-BUNDLE.json"),
    ]) == 0

    assert calls == [(
        AdvancedOptionsProcessKind.FLOW,
        Path(tmp_path / "PACKAGE.json").resolve(),
        Path(tmp_path / "SESSION-BUNDLE.json").resolve(),
    )]


def test_prepare_rejects_runtime_contracts_outside_admission():
    cohort_id = UUID("11111111-2222-3333-4444-555555555555")
    journal_generation = UUID(
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    )
    selected = [
        SimpleNamespace(contract_ticker=ticker)
        for ticker in (
            "O:QQQ261023C00705000",
            "O:QQQ261023P00705000",
            "O:SPY261023C00671000",
            "O:SPY261023P00671000",
        )
    ]
    history_evidence = {
        "schema": "advanced-options-v3-history-binding-evidence-v1",
        "evidence": "fixture",
    }
    history_evidence["evidence_sha256"] = canonical_sha256(
        history_evidence
    )
    history = build_advanced_options_history_binding(
        target_cohort_id=cohort_id,
        source_cohort_id=UUID(
            "99999999-8888-7777-6666-555555555555"
        ),
        history_before=market_loop.datetime(
            2026,
            10,
            23,
            13,
            30,
            tzinfo=market_loop.UTC,
        ),
        source_manifest_sha256="e" * 64,
        authorization_reference="V3-H-TEST",
        evidence_sha256=history_evidence["evidence_sha256"],
    )
    binding = SimpleNamespace(
        history=history,
        enrollment=SimpleNamespace(
            activity_selection=SimpleNamespace(contracts=selected),
            equity_plan=SimpleNamespace(
                cohort_id=UUID(
                    "77777777-6666-5555-4444-333333333333"
                )
            ),
            control_plane=SimpleNamespace(
                cohort_id=cohort_id,
                source_flow=SimpleNamespace(
                    journal_generation=journal_generation
                ),
                slot_set=SimpleNamespace(
                    contract_count=4,
                    member_sha256="f" * 64,
                ),
            ),
        )
    )
    session = SimpleNamespace(
        cohort_id=cohort_id,
        journal_generation=journal_generation,
        slot_set_member_sha256="f" * 64,
        admission_sha256="a" * 64,
    )
    pre_session = {
        "semantic_sha256": "b" * 64,
        "file_sha256": "c" * 64,
        "admission_sha256": session.admission_sha256,
        "source_capture_semantic_sha256": "d" * 64,
    }

    cli._validate_pre_session_identity(
        {
            "v3_pre_session_authority": pre_session,
            "v3_admitted_contracts": [
                row.contract_ticker for row in selected
            ],
            "v3_history_binding": {
                "binding": advanced_options_history_binding_payload(
                    history
                ),
                "evidence": history_evidence,
            },
        },
        binding,
        session,
    )
    with pytest.raises(
        AdvancedOptionsHistoryBindingIdentityError,
        match="package identity is absent or divergent",
    ):
        cli._validate_pre_session_identity(
            {
                "v3_pre_session_authority": pre_session,
                "v3_admitted_contracts": [
                    row.contract_ticker for row in selected
                ],
            },
            binding,
            session,
        )
    with pytest.raises(
        ValueError,
        match="pre-open acquisition identity changed",
    ):
        cli._validate_pre_session_identity(
            {
                "v3_pre_session_authority": pre_session,
                "v3_admitted_contracts": [
                    *[
                        row.contract_ticker
                        for row in selected[:-1]
                    ],
                    "O:SPY261023P00672000",
                ],
                "v3_history_binding": {
                    "binding": (
                        advanced_options_history_binding_payload(
                            history
                        )
                    ),
                    "evidence": history_evidence,
                },
            },
            binding,
            session,
        )


def test_market_loop_cli_freezes_four_attempt_hard_stop(tmp_path):
    digest = "a" * 64
    args = market_loop.parse_args([
    "--database",
    DATABASE,
    "--confirm-database-name",
    DATABASE,
    "--session-date",
    "2026-10-23",
    "--hard-stop",
    "2026-10-23T20:00:00+00:00",
    "--equity-cutoff",
    "2026-10-23T18:15:00+00:00",
    "--equity-cohort-id",
    "11111111-2222-3333-4444-555555555555",
    "--image-id",
    "sha256:" + digest,
    "--docker-executable",
    str(tmp_path / "docker.exe"),
    "--qualification-report",
    str(tmp_path / "qualification.json"),
    "--qualification-semantic-sha256",
    digest,
    "--qualification-file-sha256",
    digest,
    "--docker-qualification-report",
    str(tmp_path / "docker-qualification.json"),
    "--docker-qualification-semantic-sha256",
    digest,
    "--docker-qualification-file-sha256",
    digest,
    "--pre-session-capture",
    str(tmp_path / "capture.json"),
    "--pre-session-capture-semantic-sha256",
    digest,
    "--pre-session-capture-file-sha256",
    digest,
    "--pre-session-authority",
    str(tmp_path / "pre-session-authority.json"),
    "--pre-session-authority-semantic-sha256",
    digest,
    "--pre-session-authority-file-sha256",
    digest,
    "--history-binding-report",
    str(tmp_path / "history-binding.json"),
    "--history-binding-semantic-sha256",
    digest,
    "--history-binding-file-sha256",
    digest,
    "--authorization-reference",
    "advanced-v3-20261023",
    "--package-nonce",
    "99999999-8888-7777-6666-555555555555",
    "--output-directory",
    str(tmp_path / "output"),
    "--maximum-window-attempts",
    "4",
    "--execute",
    ])

    assert args.maximum_window_attempts == 4
    assert args.hard_stop.isoformat() == "2026-10-23T20:00:00+00:00"
    assert args.equity_cutoff.isoformat() == "2026-10-23T18:15:00+00:00"


def test_market_loop_equity_lookup_uses_non_reserved_window_alias(
    monkeypatch,
):
    statements = []

    class Cursor:
        def execute(self, statement, _parameters):
            statements.append(statement)

        def fetchone(self):
            return None

    class Transaction:
        cursor = Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    provider = SimpleNamespace(
        transaction=lambda *_args, **_kwargs: Transaction()
    )
    monkeypatch.setattr(
        market_loop,
        "load_advanced_equity_control_plane_plan_cursor",
        lambda *_args: SimpleNamespace(),
    )
    monkeypatch.setattr(
        market_loop,
        "validate_active_target9_equity_source_cursor",
        lambda *_args: None,
    )

    cutoff = market_loop.datetime(
        2026,
        10,
        23,
        18,
        15,
        tzinfo=market_loop.UTC,
    )
    assert market_loop._frozen_equity_input(
        provider,
        UUID("11111111-2222-3333-4444-555555555555"),
        cutoff,
    ) is None
    assert "AS window_run" in statements[0]
    assert "AS window " not in statements[0]
    assert statements[0].endswith("receipt.cutoff=%s")


def test_market_loop_capture_binding_is_provider_free(
    monkeypatch,
):
    selection_capture = SimpleNamespace(request_count=2)
    capture_port = SimpleNamespace(
        production_binding_payload={"binding_sha256": "b" * 64},
        capture_from_selection=lambda package, **kwargs: (
            {"selection": "sealed"},
            SimpleNamespace(history="history"),
        ),
    )
    components = SimpleNamespace(
        capture=capture_port,
        close=lambda: None,
    )
    build_calls = []

    def build(**kwargs):
        build_calls.append(kwargs)
        return components

    monkeypatch.setattr(
        market_loop,
        "build_advanced_options_p8_production_components",
        build,
    )
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)

    observation, binding, production, selection = (
        market_loop._capture_binding(
            {"package_sha256": "a" * 64},
            market_loop.datetime(
                2026,
                10,
                23,
                18,
                45,
                tzinfo=market_loop.UTC,
            ),
            {"REALTIME_V2_APP_DSN": "postgresql://test"},
            selection_capture,
        )
    )

    assert observation == {"selection": "sealed"}
    assert binding.history == "history"
    assert production == {"binding_sha256": "b" * 64}
    assert selection is selection_capture
    assert build_calls[0]["provider_free_selection"] is True
    assert "POLYGON_API_KEY" not in market_loop.os.environ


def test_market_loop_rejects_sealed_capture_outside_frozen_equity_age(
    monkeypatch,
):
    reviewed = market_loop.datetime(
        2026,
        10,
        23,
        18,
        44,
        tzinfo=market_loop.UTC,
    )
    selection_capture = SimpleNamespace(
        selection=SimpleNamespace(
            reviewed_at=reviewed,
            valid_until=reviewed + market_loop.timedelta(minutes=5),
        ),
    )
    monkeypatch.setattr(
        market_loop,
        "p8_activity_selection_capture_from_sealed",
        lambda _capture: selection_capture,
    )
    args = SimpleNamespace(
        equity_cutoff=market_loop.datetime(
            2026,
            10,
            23,
            18,
            15,
            tzinfo=market_loop.UTC,
        ),
    )

    with pytest.raises(RuntimeError, match="maximum age by 1440 seconds"):
        market_loop._validate_sealed_capture_scope(
            {"contract": "sealed"},
            args,
            checked_at=reviewed,
        )


def test_market_loop_loads_separate_pre_session_authority(
    monkeypatch,
    tmp_path,
):
    cohort_id = UUID("11111111-2222-3333-4444-555555555555")
    nonce = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    hard_stop = market_loop.datetime(
        2026,
        10,
        23,
        20,
        tzinfo=market_loop.UTC,
    )
    admission = SimpleNamespace(
        admission_id=UUID(
            "22222222-3333-4444-5555-666666666666"
        ),
        admission_sha256="a" * 64,
        capture_boundary=market_loop.datetime(
            2026,
            10,
            23,
            13,
            30,
            tzinfo=market_loop.UTC,
        ),
        contracts=(
            SimpleNamespace(contract_ticker="O:QQQ-C"),
            SimpleNamespace(contract_ticker="O:QQQ-P"),
            SimpleNamespace(contract_ticker="O:SPY-C"),
            SimpleNamespace(contract_ticker="O:SPY-P"),
        ),
    )
    session = SimpleNamespace(
        admission_id=admission.admission_id,
        admission_sha256=admission.admission_sha256,
        cohort_id=cohort_id,
        authorization_reference="V3-SPLIT-TEST",
        acquisition_stop=hard_stop,
        session_date=market_loop.date(2026, 10, 23),
    )
    document = {
        "contract": "advanced-options-v3-pre-session-authority-v1",
        "session_date": "2026-10-23",
        "target_cohort_id": str(cohort_id),
        "package_nonce": str(nonce),
        "authorization_reference": "V3-SPLIT-TEST",
        "source_capture_semantic_sha256": "b" * 64,
        "admission": {"fixture": "admission"},
        "admitted_contracts": [
            row.contract_ticker for row in admission.contracts
        ],
        "session_acquisition": {"fixture": "session"},
    }
    document["semantic_sha256"] = canonical_sha256(document)
    path = tmp_path / "pre-session-authority.json"
    path.write_text(
        json.dumps(document, sort_keys=True),
        encoding="utf-8",
    )
    args = SimpleNamespace(
        pre_session_authority=path,
        pre_session_authority_semantic_sha256=(
            document["semantic_sha256"]
        ),
        pre_session_authority_file_sha256=market_loop._file_sha256(
            path
        ),
        session_date=market_loop.date(2026, 10, 23),
        package_nonce=nonce,
        authorization_reference="V3-SPLIT-TEST",
        hard_stop=hard_stop,
    )
    monkeypatch.setattr(
        market_loop,
        "prospective_opening_admission_from_payload",
        lambda _value: admission,
    )
    monkeypatch.setattr(
        market_loop,
        "session_acquisition_authorization_from_payload",
        lambda _value: session,
    )

    loaded, contracts, identity = (
        market_loop._load_pre_session_authority(
            args,
            cohort_id,
        )
    )

    assert loaded is session
    assert contracts == [
        "O:QQQ-C",
        "O:QQQ-P",
        "O:SPY-C",
        "O:SPY-P",
    ]
    assert identity["admission_sha256"] == "a" * 64
    assert identity["source_capture_semantic_sha256"] == "b" * 64
