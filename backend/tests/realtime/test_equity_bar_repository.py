from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from equity.domain import BarSourceKind
from realtime.equity_bar_adapter import adapt_developer_equity_bar
from realtime.equity_bar_repository import (
    load_current_bar_groups_cursor,
    persist_realtime_equity_bars,
    persist_realtime_equity_bars_cursor,
)
from tests.realtime.test_equity_bar_adapter import COHORT, _developer_revision


def _values_executor(cursor, statement, rows, *, page_size):
    assert page_size == 100
    cursor.value_statements.append(statement)
    if "INSERT INTO rt.bar_catalog" in statement:
        cursor.recorded_catalog_rows = tuple(rows)
    else:
        cursor.recorded_revision_rows = tuple(rows)
        cursor.recorded_revision_statement = statement


def _batch_executor(cursor, statement, rows, *, page_size):
    assert page_size == 100
    cursor.recorded_current_rows = tuple(rows)
    cursor.recorded_current_statement = statement


def _fact(**changes):
    return adapt_developer_equity_bar(
        replace(_developer_revision(), **changes),
        cohort_id=COHORT,
        instrument_id=1,
    )


def _cursor(fact, *, catalog=True, body=True):
    revision = fact.revision
    cursor = MagicMock()
    cursor.value_statements = []
    catalog_row = (
        revision.bar_revision_id, fact.cohort_id, fact.provider_tier.value,
        fact.instrument_id, revision.interval, revision.bar_start, revision.bar_end,
        revision.system_observed_at, revision.session_date,
    )
    body_row = (
        revision.bar_revision_id, fact.cohort_id, fact.provider_tier.value,
        fact.instrument_id, revision.payload_sha256,
    )

    def fetchall():
        statement = cursor.execute.call_args.args[0]
        if "FROM rt.bar_catalog" in statement:
            return (catalog_row,) if catalog else ()
        if "FROM rt.bar_revisions" in statement:
            return (body_row,) if body else ()
        raise AssertionError(statement)

    cursor.fetchall.side_effect = fetchall
    return cursor


def test_bar_revision_and_current_pointer_commit_atomically_in_batches():
    fact = _fact()
    cursor = _cursor(fact)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_realtime_equity_bars(
        connection,
        (fact,),
        values_executor=_values_executor,
        batch_executor=_batch_executor,
    )

    # Catalog first (it owns the identity and the FK target), then the partitioned body.
    assert "INSERT INTO rt.bar_catalog" in cursor.value_statements[0]
    assert "INSERT INTO rt.bar_revisions" in cursor.value_statements[1]
    assert all("ON CONFLICT DO NOTHING" in statement for statement in cursor.value_statements)
    assert "INSERT INTO rt.bar_current" in cursor.recorded_current_statement
    assert "rt.bar_current.provider_tier=EXCLUDED.provider_tier" in cursor.recorded_current_statement
    assert "WHEN 'RECONCILED' THEN 4" in cursor.recorded_current_statement
    assert cursor.recorded_catalog_rows[0][7] == fact.revision.system_observed_at
    assert cursor.recorded_revision_rows[0][2] == "MASSIVE_DEVELOPER"
    # observed_at is the partition key and must equal the catalog's observed_at.
    assert cursor.recorded_revision_rows[0][22] == fact.revision.system_observed_at
    assert cursor.recorded_revision_rows[0][25] == str(
        fact.revision.ingestion_segment_id
    )
    assert cursor.recorded_revision_rows[0][26:29] == (None, None, None)
    body_select = next(
        call for call in cursor.execute.call_args_list if "FROM rt.bar_revisions" in call.args[0]
    )
    assert "observed_at BETWEEN %s AND %s" in body_select.args[0]
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


@pytest.mark.parametrize("missing", ("catalog", "body"))
def test_bar_identity_conflict_rolls_back_current_pointer(missing):
    fact = _fact()
    cursor = _cursor(fact, catalog=missing != "catalog", body=missing != "body")
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    current_executor = MagicMock()

    with pytest.raises(ValueError, match="identity conflict"):
        persist_realtime_equity_bars(
            connection,
            (fact,),
            values_executor=_values_executor,
            batch_executor=current_executor,
        )

    current_executor.assert_not_called()
    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_catalog_conflict_stops_before_the_partitioned_body_is_written():
    fact = _fact()
    cursor = _cursor(fact, catalog=False)

    with pytest.raises(ValueError, match="identity conflict"):
        persist_realtime_equity_bars_cursor(
            cursor, (fact,), values_executor=_values_executor, batch_executor=_batch_executor,
        )

    assert len(cursor.value_statements) == 1


def test_cursor_level_bar_persistence_composes_without_owning_transaction():
    fact = _fact()
    cursor = _cursor(fact)

    persist_realtime_equity_bars_cursor(
        cursor,
        (fact,),
        values_executor=_values_executor,
        batch_executor=_batch_executor,
    )

    assert "INSERT INTO rt.bar_revisions" in cursor.recorded_revision_statement
    assert "INSERT INTO rt.bar_current" in cursor.recorded_current_statement


def test_derived_bar_persists_typed_uuid_source_lineage():
    source_revision_id = uuid4()
    revision = replace(
        _developer_revision(),
        bar_revision_id=uuid4(),
        source_kind=BarSourceKind.DERIVED,
        source_bar_revision_ids=(source_revision_id,),
    )
    fact = adapt_developer_equity_bar(
        revision,
        cohort_id=COHORT,
        instrument_id=1,
    )
    cursor = _cursor(fact)

    persist_realtime_equity_bars_cursor(
        cursor,
        (fact,),
        values_executor=_values_executor,
        batch_executor=_batch_executor,
    )

    assert cursor.recorded_revision_rows[0][29] == [source_revision_id]


def _current_bar(interval="5m", basis="RAW_UNADJUSTED", **changes):
    start = datetime(2026, 10, 2, 13, 30, tzinfo=timezone.utc)
    return dict(
        interval=interval, adjustment_basis=basis, bar_revision_id=uuid4(), bar_start=start,
        bar_end=start + timedelta(minutes=5), open_price=Decimal("10.00000000"),
        high_price=Decimal("11.00000000"), low_price=Decimal("9.00000000"),
        close_price=Decimal("10.50000000"), volume=Decimal("1200.00000000"), payload_sha256="a" * 64,
        **changes,
    )


def test_current_bar_groups_read_one_ticker_through_the_current_pointer_per_lineage_group():
    five, daily = _current_bar(), _current_bar("1d", "ADJUSTED")
    cursor = MagicMock()
    cursor.fetchall.return_value = [five, daily]
    first, last = five["bar_start"], five["bar_end"]
    cohort, security = uuid4(), uuid4()

    groups = load_current_bar_groups_cursor(
        cursor, cohort_id=cohort, provider_tier="MASSIVE_DEVELOPER", instrument_id=7, security_id=security,
        ticker="AAPL", groups={"5m:RAW_UNADJUSTED": (first, last), "1d:ADJUSTED": (first, last),
                               "1mo:ADJUSTED": (first, last)},
    )

    assert groups == {"5m:RAW_UNADJUSTED": [five], "1d:ADJUSTED": [daily], "1mo:ADJUSTED": []}
    statement, parameters = cursor.execute.call_args.args
    assert "FROM rt.bar_current AS cur" in statement
    assert "revision.observed_at=cur.observed_at" in statement
    assert "revision.instrument_id=cur.instrument_id" in statement and "revision.interval=cur.interval" in statement
    assert "cur.session_scope='RTH'" in statement and "revision.is_final" in statement
    assert "ORDER BY cur.interval, cur.adjustment_basis, cur.bar_start" in statement
    assert statement.count("cur.bar_start>=%s AND cur.bar_end<=%s") == 3
    assert parameters[:5] == (str(cohort), "MASSIVE_DEVELOPER", 7, str(security), "AAPL")
    assert parameters[5:9] == ("5m", "RAW_UNADJUSTED", first, last)


def test_current_bar_groups_accept_tuple_rows_and_validate_the_group_key():
    bar = _current_bar()
    cursor = MagicMock()
    cursor.fetchall.return_value = [(
        bar["interval"], bar["adjustment_basis"], bar["bar_revision_id"], bar["bar_start"], bar["bar_end"],
        bar["open_price"], bar["high_price"], bar["low_price"], bar["close_price"], bar["volume"],
        bar["payload_sha256"],
    )]
    window = (bar["bar_start"], bar["bar_end"])
    groups = load_current_bar_groups_cursor(
        cursor, cohort_id=uuid4(), provider_tier="MASSIVE_DEVELOPER", instrument_id=1, security_id=uuid4(),
        ticker="AAPL", groups={"5m:RAW_UNADJUSTED": window},
    )
    assert groups["5m:RAW_UNADJUSTED"] == [bar]
    for key in ("5m", "7m:RAW_UNADJUSTED", "5m:SPLIT_ADJUSTED", "5m:RAW_UNADJUSTED'; --"):
        with pytest.raises(ValueError, match="interval:basis"):
            load_current_bar_groups_cursor(
                cursor, cohort_id=uuid4(), provider_tier="MASSIVE_DEVELOPER", instrument_id=1,
                security_id=uuid4(), ticker="AAPL", groups={key: window},
            )
    cursor.reset_mock()
    assert load_current_bar_groups_cursor(
        cursor, cohort_id=uuid4(), provider_tier="MASSIVE_DEVELOPER", instrument_id=1, security_id=uuid4(),
        ticker="AAPL", groups={},
    ) == {}
    cursor.execute.assert_not_called()