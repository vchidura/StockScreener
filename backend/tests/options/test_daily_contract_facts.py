from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from database import get_db_connection
from options.calendar import OptionExchangeCalendar
from options.repositories.daily_facts import (
    MARK_SOURCE_DAILY_AGGREGATE,
    OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT,
    READ_QUERIES,
    DailyMarkRecord,
    DailyOpenInterestRecord,
    OptionDailyFactRepository,
)


@pytest.fixture
def rolled_back_repository(rolled_back_connection):
    factory, connection = rolled_back_connection
    yield OptionDailyFactRepository(connection_factory=factory), connection


def _contract(cursor) -> tuple[int, str]:
    cursor.execute(
        "SELECT contract_id, underlying FROM option_contract_catalog ORDER BY contract_id LIMIT 1"
    )
    row = cursor.fetchone()
    if row is None:
        pytest.skip("no catalogued option contracts in this database")
    return row[0], row[1]


def test_open_interest_settlement_precedes_the_cycle_session():
    calendar = OptionExchangeCalendar()
    assert calendar.previous_session(date(2026, 9, 3)) == date(2026, 9, 2)


def test_open_interest_at_the_close_still_describes_the_prior_session():
    calendar = OptionExchangeCalendar()
    close_slot = datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc)
    observed_session = calendar.session_for_slot(close_slot)
    assert observed_session == date(2026, 9, 3)
    assert calendar.previous_session(observed_session) == date(2026, 9, 2)


def test_persist_open_interest_round_trips(rolled_back_repository):
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
    session = date(1999, 1, 4)
    observed = datetime(1999, 1, 5, 14, 0, tzinfo=timezone.utc)
    written = repository.persist_open_interest(
        [
            DailyOpenInterestRecord(
                contract_id=contract_id,
                settlement_session=session,
                underlying=underlying,
                open_interest=4321,
                observed_at=observed,
                observed_session=date(1999, 1, 5),
            )
        ]
    )
    assert written == 1
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT open_interest, open_interest_source, mark_close
            FROM option_daily_contract_facts
            WHERE contract_id = %s AND settlement_session = %s
            """,
            (contract_id, session),
        )
        row = cursor.fetchone()
    assert row == (4321, OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT, None)


def test_reobserving_a_settled_session_never_overwrites(rolled_back_repository):
    """A settled session's open interest cannot legitimately change."""
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
    session = date(1999, 1, 5)
    first = DailyOpenInterestRecord(
        contract_id=contract_id,
        settlement_session=session,
        underlying=underlying,
        open_interest=1000,
        observed_at=datetime(1999, 1, 6, 14, 0, tzinfo=timezone.utc),
        observed_session=date(1999, 1, 6),
    )
    repository.persist_open_interest([first])
    repository.persist_open_interest(
        [
            DailyOpenInterestRecord(
                contract_id=contract_id,
                settlement_session=session,
                underlying=underlying,
                open_interest=9999,
                observed_at=datetime(1999, 1, 6, 18, 0, tzinfo=timezone.utc),
                observed_session=date(1999, 1, 6),
            )
        ]
    )
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT open_interest, open_interest_revised_value,"
            " open_interest_revision_count FROM option_daily_contract_facts"
            " WHERE contract_id = %s AND settlement_session = %s",
            (contract_id, session),
        )
        assert cursor.fetchone() == (1000, 9999, 1)


def test_settlement_must_precede_observation_session(rolled_back_repository):
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
    session = date(1999, 1, 5)
    with pytest.raises(Exception):
        repository.persist_open_interest(
            [
                DailyOpenInterestRecord(
                    contract_id=contract_id,
                    settlement_session=session,
                    underlying=underlying,
                    open_interest=1000,
                    observed_at=datetime(1999, 1, 5, 22, 0, tzinfo=timezone.utc),
                    observed_session=session,
                )
            ]
        )
    connection.rollback()


def test_marks_and_open_interest_share_one_row(rolled_back_repository):
    """The two column groups arrive from different pipelines at different times."""
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
    session = date(1999, 1, 6)
    repository.persist_open_interest(
        [
            DailyOpenInterestRecord(
                contract_id=contract_id,
                settlement_session=session,
                underlying=underlying,
                open_interest=250,
                observed_at=datetime(1999, 1, 7, 14, 0, tzinfo=timezone.utc),
                observed_session=date(1999, 1, 7),
            )
        ]
    )
    repository.persist_marks(
        [
            DailyMarkRecord(
                contract_id=contract_id,
                settlement_session=session,
                underlying=underlying,
                close=Decimal("1.25"),
                high=Decimal("1.40"),
                low=Decimal("1.10"),
                volume=42,
                observed_at=datetime(1999, 1, 8, 14, 0, tzinfo=timezone.utc),
            )
        ]
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT open_interest, mark_close, mark_source, count(*) OVER ()
            FROM option_daily_contract_facts
            WHERE contract_id = %s AND settlement_session = %s
            """,
            (contract_id, session),
        )
        rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 250
    assert rows[0][1] == Decimal("1.25000000")
    assert rows[0][2] == MARK_SOURCE_DAILY_AGGREGATE


def test_marks_may_exist_without_open_interest(rolled_back_repository):
    """Backfilled sessions predate capture, so their open interest is NULL forever."""
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
    session = date(1999, 1, 7)
    repository.persist_marks(
        [
            DailyMarkRecord(
                contract_id=contract_id,
                settlement_session=session,
                underlying=underlying,
                close=Decimal("2.50"),
                observed_at=datetime(1999, 1, 8, 14, 0, tzinfo=timezone.utc),
            )
        ]
    )
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT open_interest, mark_close FROM option_daily_contract_facts"
            " WHERE contract_id = %s AND settlement_session = %s",
            (contract_id, session),
        )
        assert cursor.fetchone() == (None, Decimal("2.50000000"))


def test_an_empty_row_is_rejected(rolled_back_repository):
    _, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
        with pytest.raises(Exception):
            cursor.execute(
                """
                INSERT INTO option_daily_contract_facts
                    (contract_id, settlement_session, underlying)
                VALUES (%s, %s, %s)
                """,
                (contract_id, date(1999, 1, 8), underlying),
            )
    connection.rollback()


def test_open_interest_without_provenance_is_rejected(rolled_back_repository):
    _, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
        with pytest.raises(Exception):
            cursor.execute(
                """
                INSERT INTO option_daily_contract_facts
                    (contract_id, settlement_session, underlying, open_interest)
                VALUES (%s, %s, %s, %s)
                """,
                (contract_id, date(1999, 1, 9), underlying, 10),
            )
    connection.rollback()


def test_mark_volume_without_provenance_is_rejected(rolled_back_repository):
    """Volume is available from two sources with different finality.

    A value with no declared source cannot be told apart from the settled figure.
    """
    _, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
        with pytest.raises(Exception):
            cursor.execute(
                """
                INSERT INTO option_daily_contract_facts
                    (contract_id, settlement_session, underlying,
                     open_interest, open_interest_source, open_interest_observed_at,
                     mark_volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    contract_id,
                    date(1999, 1, 10),
                    underlying,
                    10,
                    OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT,
                    datetime(1999, 1, 11, 14, 0, tzinfo=timezone.utc),
                    5000,
                ),
            )
    connection.rollback()


def test_open_interest_change_reads_two_sessions(rolled_back_repository):
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
    prior = date(1999, 2, 1)
    current = date(1999, 2, 2)
    observed = datetime(1999, 2, 3, 14, 0, tzinfo=timezone.utc)
    repository.persist_open_interest(
        [
            DailyOpenInterestRecord(
                contract_id, prior, underlying, 500, observed, observed.date()
            ),
            DailyOpenInterestRecord(
                contract_id, current, underlying, 1750, observed, observed.date()
            ),
        ]
    )
    rows = repository.open_interest_change(underlying, current, prior, limit=10)
    match = [row for row in rows if row["contract_id"] == contract_id]
    assert len(match) == 1
    assert match[0]["open_interest_change"] == 1250


def test_open_interest_change_uses_latest_revision(rolled_back_repository):
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
    prior = date(1999, 3, 1)
    current = date(1999, 3, 2)
    observed_session = date(1999, 3, 3)
    first_observed = datetime(1999, 3, 3, 14, 0, tzinfo=timezone.utc)
    revised_observed = datetime(1999, 3, 3, 18, 0, tzinfo=timezone.utc)
    repository.persist_open_interest(
        [
            DailyOpenInterestRecord(
                contract_id, prior, underlying, 500, first_observed, observed_session
            ),
            DailyOpenInterestRecord(
                contract_id, current, underlying, 1000, first_observed, observed_session
            ),
        ]
    )
    repository.persist_open_interest(
        [
            DailyOpenInterestRecord(
                contract_id, current, underlying, 1250, revised_observed, observed_session
            )
        ]
    )
    rows = repository.open_interest_change(underlying, current, prior, limit=10)
    match = [row for row in rows if row["contract_id"] == contract_id]
    assert match[0]["open_interest"] == 1250
    assert match[0]["open_interest_change"] == 750
    assert match[0]["open_interest_revision_count"] == 1


def test_detail_read_reports_volume_finality(rolled_back_repository):
    """A settled aggregate volume outranks the running snapshot total."""
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        contract_id, underlying = _contract(cursor)
    # The detail read joins the catalog version valid at the settlement session, so the
    # sessions must fall after the contract was catalogued.
    prior = date(2026, 12, 1)
    current = date(2026, 12, 2)
    observed = datetime(2026, 12, 3, 14, 0, tzinfo=timezone.utc)
    repository.persist_open_interest(
        [
            DailyOpenInterestRecord(
                contract_id, prior, underlying, 500, observed, observed.date()
            ),
            DailyOpenInterestRecord(
                contract_id, current, underlying, 1750, observed, observed.date()
            ),
        ]
    )

    rows = repository.open_interest_change_detail(underlying, current, prior)
    match = [row for row in rows if row["contract_id"] == contract_id]
    assert len(match) == 1
    assert match[0]["session_volume"] is None
    assert match[0]["session_volume_is_final"] is False

    repository.persist_marks(
        [
            DailyMarkRecord(
                contract_id=contract_id,
                settlement_session=current,
                underlying=underlying,
                close=Decimal("1.00"),
                volume=8100,
                observed_at=observed,
            )
        ]
    )
    rows = repository.open_interest_change_detail(underlying, current, prior)
    match = [row for row in rows if row["contract_id"] == contract_id]
    assert match[0]["session_volume"] == 8100
    assert match[0]["session_volume_is_final"] is True


def test_every_catalogued_read_is_explainable(rolled_back_repository):
    repository, _ = rolled_back_repository
    assert set(READ_QUERIES) == {
        "session_coverage",
        "open_interest_change",
        "open_interest_change_detail",
        "latest_session_pair",
    }
    plan = repository.explain(
        "session_coverage", ("SPY", date(2026, 9, 1), date(2026, 9, 30))
    )
    assert plan
    detail = repository.explain(
        "open_interest_change_detail",
        ("SPY", date(2026, 9, 3), date(2026, 9, 1), "SPY", date(2026, 9, 3)),
    )
    assert detail


def test_explain_rejects_an_uncatalogued_query(rolled_back_repository):
    repository, _ = rolled_back_repository
    with pytest.raises(KeyError):
        repository.explain("persist_open_interest", ())


def test_table_is_classified_as_retained_provider_evidence():
    """A purge must never take the one input that cannot be re-acquired."""
    from scripts.purge_option_derived_layer import DERIVED_TABLES, RETAINED_TABLES

    assert "option_daily_contract_facts" in RETAINED_TABLES
    assert "option_daily_contract_facts" not in DERIVED_TABLES
