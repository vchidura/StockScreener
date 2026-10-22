from __future__ import annotations

import importlib
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest


detectors = importlib.import_module(
    "scripts.run_realtime_v2_advanced_equity_detectors"
)
health = importlib.import_module(
    "scripts.report_realtime_v2_advanced_health"
)

UTC = timezone.utc
NOW = datetime(2026, 10, 21, 15, 0, tzinfo=UTC)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
GENERATION = UUID("22222222-2222-2222-2222-222222222222")
OWNER = UUID("33333333-3333-3333-3333-333333333333")


class _Provider:
    def __init__(self):
        self.cursor = MagicMock()

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield SimpleNamespace(cursor=self.cursor)


def test_detector_cli_requires_bounded_continuous_execution():
    args = detectors.parse_args([
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
        "--cohort-id",
        str(COHORT),
        "--session-date",
        "2026-10-21",
        "--continuous",
        "--execute",
    ])

    assert args.maximum_records == 100
    assert args.lease_seconds == 60
    with pytest.raises(SystemExit):
        detectors.parse_args([
            "--database",
            "stocks_realtime_v2",
            "--confirm-database-name",
            "stocks_realtime_v2",
            "--cohort-id",
            str(COHORT),
            "--session-date",
            "2026-10-21",
            "--execute",
        ])


def test_detector_code_identity_binds_exact_source_bytes(tmp_path):
    first = tmp_path / "first.py"
    second = tmp_path / "second.py"
    first.write_text("first\n", encoding="ascii")
    second.write_text("second\n", encoding="ascii")

    digest = detectors.detector_code_sha256((first, second))

    assert len(digest) == 64
    second.write_text("changed\n", encoding="ascii")
    assert detectors.detector_code_sha256((first, second)) != digest


def test_detector_cycle_fences_both_s1_s2_consumers(monkeypatch):
    provider = _Provider()
    scope = detectors.AdvancedEquityDetectorScope(
        GENERATION,
        "a" * 64,
        17,
    )
    claims = []
    persisted = []

    monkeypatch.setattr(
        detectors,
        "load_advanced_equity_detector_scopes_cursor",
        lambda *_args, **_kwargs: (scope,),
    )

    def claim(_cursor, **kwargs):
        claims.append(kwargs)
        return 4

    monkeypatch.setattr(
        detectors,
        "claim_advanced_equity_detector_lease_cursor",
        claim,
    )
    monkeypatch.setattr(
        detectors,
        "build_s1_s2_consumer_config",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    monkeypatch.setattr(
        detectors,
        "s1_s2_evaluator_cursor",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        detectors,
        "plan_advanced_equity_detector_slice_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(records=(object(),)),
    )
    monkeypatch.setattr(
        detectors,
        "persist_advanced_equity_detector_slice_cursor",
        lambda _cursor, planned: persisted.append(planned),
    )

    result = detectors.run_detector_cycle(
        provider,
        cohort_id=COHORT,
        session_date=date(2026, 10, 21),
        session_close=datetime(2026, 10, 21, 20, 0, tzinfo=UTC),
        owner_id=OWNER,
        code_sha256="b" * 64,
        maximum_records=25,
        lease_seconds=60,
        clock=lambda: NOW,
    )

    assert result == {
        "status": "PROCESSED",
        "checked_at_utc": NOW.isoformat(),
        "cohort_id": str(COHORT),
        "session_date": "2026-10-21",
        "journal_scopes": 1,
        "active_consumers": 2,
        "records": 2,
        "provider_requests": 0,
        "alert_writes": 0,
        "outbox_writes": 0,
        "publication_writes": 0,
    }
    assert len(claims) == len(persisted) == 2
    assert {row["resource_key"].rsplit(":", 1)[-1] for row in claims} == {
        "S1",
        "S2",
    }
    assert all(row["owner_id"] == OWNER for row in claims)


def test_detector_shutdown_releases_only_owned_detector_leases():
    provider = _Provider()
    provider.cursor.fetchall.return_value = (("first",), ("second",))

    assert detectors.release_detector_leases(
        provider,
        owner_id=OWNER,
    ) == 2
    statement, parameters = provider.cursor.execute.call_args.args
    assert statement.startswith("DELETE FROM rt.worker_leases")
    assert parameters == (
        str(OWNER),
        "advanced-equity-detector:%",
    )


def test_detector_health_requires_all_expected_fences():
    cursor = MagicMock()
    cursor.fetchone.side_effect = ((4,), (1,), (4,), (2,))

    state, detail = health._detector_health(cursor, COHORT)

    assert state == "READY"
    assert detail["required_leases"] == detail["active_leases"] == 4

    cursor.fetchone.side_effect = ((4,), (1,), (3,), (2,))
    assert health._detector_health(cursor, COHORT)[0] == "DEGRADED"
