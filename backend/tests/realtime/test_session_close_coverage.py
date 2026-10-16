from __future__ import annotations

import inspect
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from realtime.session_close_coverage import (
    SessionCloseEligibilityPolicy,
    SessionCloseNotEligible,
    audit_session_close_cursor,
    five_minute_session,
    missing_five_minute_ranges,
)


UTC = timezone.utc


def test_regular_session_has_78_five_minute_members_per_instrument():
    session = five_minute_session(date(2026, 10, 15))

    assert session.opened_at == datetime(2026, 10, 15, 13, 30, tzinfo=UTC)
    assert session.closed_at == datetime(2026, 10, 15, 20, 0, tzinfo=UTC)
    assert session.expected_per_instrument == 78
    assert 9 * session.expected_per_instrument == 702
    assert session.bar_starts[0] == session.opened_at
    assert session.bar_starts[-1] + timedelta(minutes=5) == session.closed_at


def test_half_day_session_uses_actual_close_and_has_42_members_per_instrument():
    session = five_minute_session(date(2026, 11, 27))

    assert session.opened_at == datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
    assert session.closed_at == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    assert session.expected_per_instrument == 42
    assert 9 * session.expected_per_instrument == 378
    assert session.bar_starts[-1] == datetime(2026, 11, 27, 17, 55, tzinfo=UTC)


def test_non_session_is_rejected():
    with pytest.raises(ValueError, match="not an XNYS session"):
        five_minute_session(date(2026, 11, 28))


def test_provider_qualified_close_includes_delay_and_finalization_grace():
    session = five_minute_session(date(2026, 10, 15))
    policy = SessionCloseEligibilityPolicy(900, 30)

    assert policy.eligible_at(session) == datetime(
        2026, 10, 15, 20, 15, 30, tzinfo=UTC
    )
    with pytest.raises(SessionCloseNotEligible) as raised:
        audit_session_close_cursor(
            object(),
            cohort_id=uuid4(),
            provider_tier="MASSIVE_DEVELOPER",
            session_date=session.session_date,
            known_by=policy.eligible_at(session) - timedelta(microseconds=1),
            eligibility_policy=policy,
        )
    assert raised.value.eligible_at == policy.eligible_at(session)


def test_current_coverage_does_not_read_revision_bodies():
    cohort, manifest = uuid4(), uuid4()
    session = five_minute_session(date(2026, 10, 15))
    observed_at = session.closed_at + timedelta(minutes=20)
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [(
            str(manifest), "a" * 64, "b" * 64, session.opened_at,
            session.session_date, 1, 1, "AAPL",
        )],
        [
            (1, bar_start, str(uuid4()), observed_at, observed_at)
            for bar_start in session.bar_starts
        ],
    ]

    result = audit_session_close_cursor(
        cursor, cohort_id=cohort, provider_tier="MASSIVE_DEVELOPER",
        session_date=session.session_date, known_by=observed_at,
    )

    assert result.status == "COMPLETE"
    assert result.complete_members == 78
    assert cursor.execute.call_count == 2
    assert all(
        "rt.bar_revisions" not in call.args[0]
        for call in cursor.execute.call_args_list
    )


def test_historical_lookup_is_bounded_and_provider_free():
    cohort, manifest = uuid4(), uuid4()
    session = five_minute_session(date(2026, 10, 15))
    known_by = session.closed_at + timedelta(minutes=20)
    future = known_by + timedelta(minutes=1)
    revisions = [str(uuid4()) for _ in session.bar_starts]
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [(
            str(manifest), "a" * 64, "b" * 64, session.opened_at,
            session.session_date, 1, 1, "AAPL",
        )],
        [
            (1, bar_start, revision, future, future)
            if index == 0 else (1, bar_start, revision, known_by, known_by)
            for index, (bar_start, revision) in enumerate(zip(session.bar_starts, revisions))
        ],
        [(1, session.bar_starts[0], revisions[0])],
    ]

    result = audit_session_close_cursor(
        cursor, cohort_id=cohort, provider_tier="MASSIVE_DEVELOPER",
        session_date=session.session_date, known_by=known_by,
    )

    assert result.status == "COMPLETE"
    assert cursor.execute.call_count == 3
    statement, parameters = cursor.execute.call_args.args
    assert "rt.bar_catalog" in statement and "rt.bar_revisions" in statement
    assert parameters[:2] == ([1], [session.bar_starts[0]])
    assert all(
        call.args[0].lstrip().startswith("WITH")
        for call in cursor.execute.call_args_list
    )


def test_missing_members_coalesce_only_consecutive_ranges_per_instrument():
    start = datetime(2026, 10, 15, 13, 30, tzinfo=UTC)
    ranges = missing_five_minute_ranges((
        (2, "MSFT", start + timedelta(minutes=10)),
        (1, "AAPL", start),
        (1, "AAPL", start + timedelta(minutes=5)),
        (1, "AAPL", start + timedelta(minutes=20)),
        (2, "MSFT", start + timedelta(minutes=15)),
    ))

    assert [
        (row.ticker, row.requested_from, row.requested_to, row.missing_members)
        for row in ranges
    ] == [
        ("AAPL", start, start + timedelta(minutes=10), 2),
        (
            "AAPL",
            start + timedelta(minutes=20),
            start + timedelta(minutes=25),
            1,
        ),
        (
            "MSFT",
            start + timedelta(minutes=10),
            start + timedelta(minutes=20),
            2,
        ),
    ]


def test_close_audit_module_has_no_provider_or_network_client():
    import realtime.session_close_coverage as coverage

    source = inspect.getsource(coverage)
    for forbidden in (
        "import requests",
        "httpx",
        "websockets",
        "import socket",
        "Polygon",
        "provider_request_ledger",
    ):
        assert forbidden not in source


def test_close_audit_cli_is_read_only_and_uses_the_api_principal():
    from scripts import audit_realtime_v2_session_close as script

    source = inspect.getsource(script)
    assert "REALTIME_V2_API_DSN" in source
    assert "API_ROLE" in source
    assert "read_only=True" in source
    assert "REPEATABLE READ" in source
    assert "POLYGON_API_KEY" not in source
    assert "args.execute != args.persist" in source
    assert "013_realtime_session_close_pointer_order" in source


@pytest.mark.parametrize(
    ("option", "value"),
    (
        ("--provider-delay-seconds", "0"),
        ("--provider-delay-seconds", "899"),
        ("--finalization-grace-seconds", "0"),
        ("--finalization-grace-seconds", "29"),
    ),
)
def test_close_audit_cli_cannot_weaken_pinned_eligibility(option, value):
    from scripts import audit_realtime_v2_session_close as script

    with pytest.raises(SystemExit):
        script.parse_args([
            "--database", "stocks_realtime_v2",
            "--confirm-database-name", "stocks_realtime_v2",
            "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
            "--session-date", "2026-10-15",
            option, value,
        ])


def test_persistence_refuses_schema_without_pointer_ordering(monkeypatch):
    from scripts import audit_realtime_v2_session_close as script

    pool = MagicMock()
    provider = MagicMock()
    provider.transaction.return_value.__enter__.return_value.cursor.fetchone.return_value = (
        False,
    )
    audit = MagicMock()
    monkeypatch.setenv("REALTIME_V2_APP_DSN", "fixture")
    monkeypatch.setattr(script, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(script, "SimpleConnectionPool", lambda *args, **kwargs: pool)
    monkeypatch.setattr(script, "V2ConnectionProvider", lambda *args, **kwargs: provider)
    monkeypatch.setattr(script, "audit_session_close_cursor", audit)

    with pytest.raises(RuntimeError, match="013_realtime_session_close_pointer_order"):
        script.main([
            "--database", "stocks_realtime_v2",
            "--confirm-database-name", "stocks_realtime_v2",
            "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
            "--session-date", "2026-10-15",
            "--persist", "--execute",
        ])

    audit.assert_not_called()
    pool.closeall.assert_called_once()


def test_close_audit_cli_rejects_naive_known_by(tmp_path):
    from scripts import audit_realtime_v2_session_close as script

    with pytest.raises(SystemExit):
        script.parse_args([
            "--database", "stocks_realtime_v2",
            "--confirm-database-name", "stocks_realtime_v2",
            "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
            "--session-date", "2026-10-15",
            "--known-by", "2026-10-15T20:15:00",
            "--report-path", str(tmp_path / "audit.json"),
        ])


def test_close_audit_cli_requires_explicit_persist_execute_pair():
    from scripts import audit_realtime_v2_session_close as script

    base = [
        "--database", "stocks_realtime_v2",
        "--confirm-database-name", "stocks_realtime_v2",
        "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
        "--session-date", "2026-10-15",
    ]
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--persist"])
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--execute"])
    args = script.parse_args([*base, "--persist", "--execute"])
    assert args.persist and args.execute
