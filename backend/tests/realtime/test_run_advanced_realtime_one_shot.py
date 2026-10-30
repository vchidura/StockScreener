from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


script = importlib.import_module(
    "scripts.run_advanced_realtime_one_shot"
)
ROOT = Path(__file__).parents[3]
PLAN = (
    ROOT
    / "backend"
    / ".realtime-canary"
    / "unattended-v3-one-shot-plan-20261027.json"
)


def _load():
    body = PLAN.read_bytes()
    args = SimpleNamespace(
        plan=PLAN,
        plan_file_sha256=hashlib.sha256(body).hexdigest(),
    )
    return script._load_plan(args)


def test_one_shot_plan_freezes_budgets_and_hard_stop():
    plan, clocks = _load()

    assert plan["reference"]["request_ceiling"] == 43
    assert plan["equity_product"]["enabled"] is True
    assert plan["equity"]["rest_request_ceiling"] == 18
    assert plan["options"]["rest_request_count"] == 2
    assert clocks["equity_cutoff_utc"].isoformat() == (
        "2026-10-27T14:00:00+00:00"
    )
    assert clocks["hard_stop_utc"].isoformat() == (
        "2026-10-27T15:00:00+00:00"
    )


def test_one_shot_reference_commands_are_exact_and_bounded(tmp_path):
    plan, _clocks = _load()

    steps = script._reference_commands(plan, tmp_path)
    rendered = tuple(
        " ".join(step["command"]) for step in steps
    )

    assert len(steps) == 6
    assert sum(
        "--maximum-requests 31" in command
        for command in rendered
    ) == 1
    assert sum(
        "--maximum-provider-requests 1" in command
        for command in rendered
    ) == 3
    assert sum(
        "--maximum-provider-requests 4" in command
        for command in rendered
    ) == 1
    assert sum(
        "--maximum-provider-requests 2" in command
        for command in rendered
    ) == 1
    assert "--ticker" not in rendered[1]
    assert "--raw" not in rendered[1]
    assert "--raw" in rendered[2]
    assert "--ticker" not in rendered[2]


def test_one_shot_plan_rejects_file_drift(tmp_path):
    value = json.loads(PLAN.read_text(encoding="utf-8"))
    value["equity"]["rest_request_ceiling"] = 19
    changed = tmp_path / "plan.json"
    changed.write_text(json.dumps(value), encoding="utf-8")
    args = SimpleNamespace(
        plan=changed,
        plan_file_sha256=hashlib.sha256(
            changed.read_bytes()
        ).hexdigest(),
    )

    with pytest.raises(ValueError, match="plan contract changed"):
        script._load_plan(args)


def test_host_environment_loads_pipeline_and_python_path(monkeypatch):
    monkeypatch.setattr(
        script,
        "dotenv_values",
        lambda _path: {"REALTIME_V2_APP_DSN": "test-dsn"},
    )
    monkeypatch.delenv("PYTHONPATH", raising=False)

    environment = script._host_environment()

    assert environment["REALTIME_V2_APP_DSN"] == "test-dsn"
    assert environment["REALTIME_V2_EQUITY_PIPELINE"] == "MASSIVE_ADVANCED"
    assert environment["REALTIME_V2_OPTIONS_PIPELINE"] == "DISABLED"
    assert environment["PYTHONPATH"] == str(script.BACKEND_DIR)


def test_reference_step_reuses_exact_completed_evidence(tmp_path):
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps({
            "status": "COMPLETE",
            "provider_requests": 1,
        }),
        encoding="utf-8",
    )

    result = script._run_reference_step(
        {
            "report": report,
            "provider_requests": 1,
            "command": ("must-not-run",),
        },
        output=tmp_path,
        hard_stop=script._utc(
            "2026-10-27T15:00:00+00:00",
            "hard_stop",
        ),
    )

    assert result == "REUSED"


def test_post_e_capture_requires_exact_a_contracts(
    monkeypatch,
    tmp_path,
):
    plan, clocks = _load()
    contracts = (
        "O:QQQ261028C00715000",
        "O:QQQ261028P00715000",
        "O:SPY261028C00766000",
        "O:SPY261028P00766000",
    )
    (tmp_path / "A-CAPTURE.json").write_text(
        json.dumps({
            "selected_snapshots": {
                ticker: {} for ticker in contracts
            },
        }),
        encoding="utf-8",
    )
    calls = []
    monkeypatch.setattr(
        script,
        "_run",
        lambda argv, **_kwargs: calls.append(tuple(argv)),
    )

    script._capture_l(plan, clocks, tmp_path)

    command = calls[0]
    required = [
        command[index + 1]
        for index, value in enumerate(command)
        if value == "--required-contract-ticker"
    ]
    assert tuple(sorted(required)) == contracts
