import importlib

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
