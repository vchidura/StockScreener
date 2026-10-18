import importlib
from datetime import datetime, timedelta, timezone
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
    "--authorized-until",
    "2026-10-19T00:00:00+00:00",
    "--provider-authorization-reference",
    "provider-plan",
    "--expected-cost",
    "unapproved",
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
    assert script.parse_args([
        *BASE[:BASE.index("2026-10-15")],
        "2026-10-16",
        *BASE[BASE.index("2026-10-15") + 1:],
    ]).session_date.isoformat() == "2026-10-16"


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


def test_reference_transport_rechecks_authorization_after_pacing():
    delegate = SimpleNamespace(
        get=lambda *_args, **_kwargs: pytest.fail("provider call")
    )
    now = datetime(2026, 10, 18, tzinfo=timezone.utc)
    transport = script.PacedAccountingReferenceTransport(
        delegate,
        authorized_until=now,
        utc_clock=lambda: now + timedelta(seconds=1),
    )

    with pytest.raises(RuntimeError, match="authorization expired"):
        transport.get("https://api.massive.com/a", {}, {})
