import importlib
import json

import pytest

from realtime.reference_repository import REVIEWED_REFERENCE_SCOPES

script = importlib.import_module(
    "scripts.refresh_realtime_v2_advanced_spy_reference"
)
BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
    "--session-date",
    "2026-10-16",
    "--authorization-reference",
    "advanced-equity-v2-20261016:spy-reference",
    "--report-path",
    "spy-reference.json",
]


def test_spy_reference_plan_is_exact_and_provider_free(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)

    assert script.main(BASE) == 0

    output = capsys.readouterr().out
    assert '"provider_requests": 0' in output
    assert '"SPY"' in output
    assert script.MAXIMUM_REQUESTS == 12
    assert "ADVANCED_SPY_REFERENCES" in REVIEWED_REFERENCE_SCOPES


def test_spy_reference_rejects_another_session():
    with pytest.raises(SystemExit):
        script.parse_args([
            *BASE[:BASE.index("2026-10-16")],
            "2026-10-15",
            *BASE[BASE.index("2026-10-16") + 1:],
        ])


def test_new_reference_session_requires_exact_explicit_authority(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    args = [value if value != "2026-10-16" else "2026-10-19" for value in BASE]
    with pytest.raises(SystemExit):
        script.parse_args(args)
    assert script.main([*args, "--authorized-session-date", "2026-10-19"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["session_date"] == report["authorized_session_date"] == "2026-10-19"
    assert report["authorization_reference"] == BASE[BASE.index("--authorization-reference") + 1]
    assert report["maximum_requests"] == 12
    assert report["maximum_total_bytes"] == 12_000_000
    assert report["maximum_response_bytes"] == 3_000_000
    assert report["provider_requests"] == report["database_writes"] == 0
    assert not (tmp_path / "spy-reference.json").exists()
    with pytest.raises(SystemExit):
        script.parse_args([*args, "--authorized-session-date", "2026-10-20"])


def test_explicit_reference_scope_does_not_allow_nontrading_dates():
    args = [value if value != "2026-10-16" else "2026-10-18" for value in BASE]
    with pytest.raises(SystemExit):
        script.parse_args([*args, "--authorized-session-date", "2026-10-18"])
