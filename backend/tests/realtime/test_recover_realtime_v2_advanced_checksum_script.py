import importlib

import pytest


script = importlib.import_module(
    "scripts.recover_realtime_v2_advanced_checksum"
)


def _base(tmp_path):
    root = tmp_path / "journals"
    root.mkdir()
    return [
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--journal-root",
        str(root),
        "--plan-id",
        "11111111-1111-1111-1111-111111111111",
        "--prior-gap-id",
        "22222222-2222-2222-2222-222222222222",
        "--prior-recovered-generation",
        "33333333-3333-3333-3333-333333333333",
        "--report-path",
        str(tmp_path / "report.json"),
    ]


def test_checksum_recovery_plan_requires_exact_positive_evidence(tmp_path):
    base = _base(tmp_path)
    args = script.parse_args([
        *base,
        "--plan",
        "--expected-files-sha256",
        "a" * 64,
        "--expected-offset",
        "9",
        "--expected-bars",
        "9",
    ])

    assert args.plan
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--plan"])
    with pytest.raises(SystemExit):
        script.parse_args([
            *base,
            "--plan",
            "--expected-files-sha256",
            "a" * 64,
            "--expected-offset",
            "0",
            "--expected-bars",
            "9",
        ])
