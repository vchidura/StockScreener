import importlib

import pytest

script = importlib.import_module("scripts.provision_realtime_v2_credentials")
BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
]


def test_v2_credentials_require_canonical_authorized_execution():
    assert not script.parse_args(BASE).execute
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--execute"])
    assert script.parse_args([
        *BASE,
        "--execute",
        "--authorization-reference",
        "approved",
    ]).execute
    with pytest.raises(ValueError):
        script.parse_args([
            "--database",
            "stocks_realtime_v1",
            "--confirm-database-name",
            "stocks_realtime_v1",
        ])
