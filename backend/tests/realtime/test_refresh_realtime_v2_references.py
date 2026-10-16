import importlib
from types import SimpleNamespace

import pytest

script = importlib.import_module("scripts.refresh_realtime_v2_references")
BASE = [
    "--database",
    "stocks_realtime_v2",
    "--confirm-database-name",
    "stocks_realtime_v2",
    "--session-date",
    "2026-10-15",
]


def test_reference_refresh_requires_canonical_exact_scope_and_authorization():
    assert not script.parse_args(BASE).execute
    with pytest.raises(SystemExit):
        script.parse_args([*BASE, "--execute"])
    assert script.parse_args([
        *BASE,
        "--execute",
        "--authorization-reference",
        "approved",
    ]).execute
    with pytest.raises(SystemExit):
        script.parse_args([*BASE[:-1], "2026-10-14"])


def test_reference_transport_has_global_ceiling_and_pacing():
    calls = []
    clock = iter((0.0, 0.25, 0.5))

    class Delegate:
        def get(self, url, params, headers):
            calls.append((url, params, headers))
            return SimpleNamespace(status_code=200, body=b"{}")

    sleeps = []
    transport = script.PacedAccountingReferenceTransport(
        Delegate(),
        maximum_requests=2,
        minimum_interval_seconds=0.5,
        monotonic=lambda: next(clock),
        sleep=sleeps.append,
    )

    transport.get("https://api.massive.com/a", {}, {})
    transport.get("https://api.massive.com/b", {}, {})

    assert len(calls) == 2 and sleeps == [0.25]
    with pytest.raises(RuntimeError, match="ceiling"):
        transport.get("https://api.massive.com/c", {}, {})
