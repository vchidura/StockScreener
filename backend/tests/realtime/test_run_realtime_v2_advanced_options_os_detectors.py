from __future__ import annotations

import importlib
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest


detectors = importlib.import_module(
    "scripts.run_realtime_v2_advanced_options_os_detectors"
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


def test_detector_scopes_exclude_journals_that_predate_history_binding():
    cursor = MagicMock()
    cursor.fetchall.return_value = ()

    assert detectors.load_advanced_options_detector_scopes_cursor(
        cursor,
        cohort_id=COHORT,
        session_date=date(2026, 10, 21),
    ) == ()

    statement, params = cursor.execute.call_args.args
    assert "rt.cohort_history_bindings" in statement
    assert "registry.created_at>=binding.created_at" in statement
    assert "count(DISTINCT history.session_date)" in statement
    assert "history.observed_at<=registry.created_at" in statement
    assert "history.created_at<=registry.created_at" in statement
    assert "history.session_date<plan.session_date" in statement
    assert params == (str(COHORT), date(2026, 10, 21))


def test_detector_cycle_fences_both_os1_os2_consumers(monkeypatch):
    provider = _Provider()
    scope = detectors.AdvancedOptionsDetectorScope(
        GENERATION,
        "a" * 64,
        17,
    )
    claims = []
    persisted = []

    monkeypatch.setattr(
        detectors,
        "load_advanced_options_detector_scopes_cursor",
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
        "build_os1_os2_consumer_config",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    monkeypatch.setattr(
        detectors,
        "os1_os2_evaluator_cursor",
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
        "OS1",
        "OS2",
    }
    assert all(row["owner_id"] == OWNER for row in claims)


def test_a_long_cycle_claims_each_lease_when_it_is_made(monkeypatch):
    """B22 (Nov 6): a restart with a backlog ran longer than the lease. Each claim was stamped with the cycle's start,
    so every later scope got a lease that had already expired and its write failed."""
    from datetime import timedelta

    provider = _Provider()
    scopes = tuple(
        detectors.AdvancedOptionsDetectorScope(GENERATION, "a" * 64, instrument)
        for instrument in (17, 18, 19)
    )
    ticks = iter(NOW + timedelta(seconds=45 * step) for step in range(100))
    claims = []
    monkeypatch.setattr(detectors, "load_advanced_options_detector_scopes_cursor", lambda *_a, **_k: scopes)
    monkeypatch.setattr(
        detectors, "claim_advanced_equity_detector_lease_cursor",
        lambda _cursor, **kwargs: claims.append(kwargs["checked_at"]) or 1,
    )
    monkeypatch.setattr(detectors, "build_os1_os2_consumer_config", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(detectors, "os1_os2_evaluator_cursor", lambda *_a, **_k: object())
    monkeypatch.setattr(
        detectors, "plan_advanced_equity_detector_slice_cursor", lambda *_a, **_k: SimpleNamespace(records=()),
    )
    monkeypatch.setattr(detectors, "persist_advanced_equity_detector_slice_cursor", lambda *_a: None)

    detectors.run_detector_cycle(
        provider, cohort_id=COHORT, session_date=date(2026, 10, 21),
        session_close=datetime(2026, 10, 21, 20, 0, tzinfo=UTC), owner_id=OWNER, code_sha256="b" * 64,
        maximum_records=25, lease_seconds=60, clock=lambda: next(ticks),
    )

    assert len(claims) == 6
    assert claims == sorted(claims) and len(set(claims)) == 6
    assert claims[-1] - claims[0] > timedelta(seconds=60)


def test_detector_scope_history_counts_the_instruments_head_bars():
    """G102 A4: the 15-session history check reads the head by instrument; the binding is identity, not a filter."""
    cursor = MagicMock()
    cursor.fetchall.return_value = ()
    detectors.load_advanced_options_detector_scopes_cursor(cursor, cohort_id=COHORT, session_date=date(2026, 10, 21))
    statement = cursor.execute.call_args.args[0]
    assert "FROM rt.bar_head AS history_current" in statement
    assert "history_current.instrument_id=instrument.instrument_id" in statement
    assert "rt.bar_current" not in statement and "binding.source_cohort_id" not in statement


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


def test_scopes_run_oldest_journal_first_and_wait_for_an_unfinished_one(monkeypatch):
    """Nov 6 had two journals (4006c6a7 to 14:37, then 34b1b4f1 after the reconnect). Ordered by generation id the
    later one ran first, and the earlier one's bars then moved the per-security state backward."""
    provider = _Provider()
    older = detectors.AdvancedOptionsDetectorScope(UUID(int=9), "a" * 64, 17)
    newer = detectors.AdvancedOptionsDetectorScope(UUID(int=1), "a" * 64, 17)
    other = detectors.AdvancedOptionsDetectorScope(UUID(int=1), "a" * 64, 18)
    planned_for = []
    monkeypatch.setattr(detectors, "load_advanced_options_detector_scopes_cursor", lambda *_a, **_k: (older, newer, other))
    monkeypatch.setattr(detectors, "claim_advanced_equity_detector_lease_cursor", lambda *_a, **_k: 1)
    monkeypatch.setattr(detectors, "build_os1_os2_consumer_config", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(detectors, "os1_os2_evaluator_cursor", lambda *_a, **_k: object())

    def plan(_cursor, config, **kwargs):
        planned_for.append((config.journal_generation, config.instrument_id, config.detector_id))
        full = config.journal_generation == older.journal_generation
        return SimpleNamespace(records=(object(),) * (kwargs["maximum_records"] if full else 1))

    monkeypatch.setattr(detectors, "plan_advanced_equity_detector_slice_cursor", plan)
    monkeypatch.setattr(detectors, "persist_advanced_equity_detector_slice_cursor", lambda *_a: None)
    detectors.run_detector_cycle(
        provider, cohort_id=COHORT, session_date=date(2026, 10, 21),
        session_close=datetime(2026, 10, 21, 20, 0, tzinfo=UTC), owner_id=OWNER, code_sha256="b" * 64,
        maximum_records=25, lease_seconds=60, clock=lambda: NOW,
    )
    # The older journal of instrument 17 filled its slice, so 17's newer journal waits; instrument 18 runs.
    assert [(generation.int, instrument) for generation, instrument, _ in planned_for] == [
        (9, 17), (9, 17), (1, 18), (1, 18)]


def test_scope_query_orders_journals_by_creation():
    cursor = MagicMock()
    cursor.fetchall.return_value = ()
    detectors.load_advanced_options_detector_scopes_cursor(cursor, cohort_id=COHORT, session_date=date(2026, 10, 21))
    assert "ORDER BY registry.created_at,registry.journal_generation" in cursor.execute.call_args.args[0]
