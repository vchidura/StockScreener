from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from realtime.local_mock_execution import (
    LocalMockEarningsStatus,
    simulate_local_mock_execution,
)
from realtime.local_mock_repository import persist_local_mock_execution
from tests.realtime.test_local_mock_execution import (
    ALERT,
    NOW,
    _advanced_mark,
    _earnings,
    _mock_policy,
)


def _result():
    return simulate_local_mock_execution(
        _advanced_mark(),
        alert_id=ALERT,
        requested_at=NOW + timedelta(milliseconds=300),
        earnings=_earnings(),
        policy=_mock_policy(),
    )


def _connection(*, conflict=False):
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    stored = {
        "request": None,
        "inputs": {},
        "disposition": None,
        "fills": {},
    }

    def execute(statement, parameters=None):
        if statement.startswith("INSERT INTO rt.local_mock_requests"):
            stored["request"] = parameters
        elif statement.startswith("SELECT alert_id"):
            request = stored["request"]
            identity = (*request[1:9], *request[10:14])
            cursor.fetchone.return_value = (
                ("00000000-0000-0000-0000-000000000000", *identity[1:])
                if conflict else identity
            )
        elif statement.startswith("INSERT INTO rt.local_mock_request_inputs"):
            stored["inputs"][parameters[1]] = parameters
        elif statement.startswith("SELECT input_index"):
            cursor.fetchall.return_value = [
                (row[1], row[2])
                for row in sorted(stored["inputs"].values(), key=lambda value: value[1])
            ]
        elif statement.startswith("INSERT INTO rt.local_mock_dispositions"):
            stored["disposition"] = parameters
        elif statement.startswith("SELECT request_id, status"):
            cursor.fetchone.return_value = stored["disposition"][1:]
        elif statement.startswith("INSERT INTO rt.local_mock_fills"):
            stored["fills"][parameters[2]] = parameters
        elif statement.startswith("SELECT fill_id"):
            cursor.fetchall.return_value = [
                row
                for row in sorted(stored["fills"].values(), key=lambda value: value[2])
            ]

    cursor.execute.side_effect = execute
    return connection, cursor, stored


def _batch_executor(cursor, statement, parameters, *, page_size):
    assert page_size == 8
    for row in parameters:
        cursor.execute(statement, row)


def test_local_mock_result_persists_request_inputs_disposition_and_fills_atomically():
    result = _result()
    connection, cursor, stored = _connection()

    disposition_id = persist_local_mock_execution(
        connection,
        result,
        batch_executor=_batch_executor,
    )

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    request_index = next(
        index for index, statement in enumerate(statements)
        if statement.startswith("INSERT INTO rt.local_mock_requests")
    )
    disposition_index = next(
        index for index, statement in enumerate(statements)
        if statement.startswith("INSERT INTO rt.local_mock_dispositions")
    )
    fill_index = next(
        index for index, statement in enumerate(statements)
        if statement.startswith("INSERT INTO rt.local_mock_fills")
    )
    assert request_index < disposition_index < fill_index
    assert disposition_id is not None
    assert len(stored["inputs"]) == len(result.source_evidence_ids) == 2
    assert len(stored["fills"]) == len(result.fills) == 2
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_local_mock_identity_conflict_rolls_back_without_partial_commit():
    connection, _, _ = _connection(conflict=True)

    with pytest.raises(ValueError, match="request identity conflict"):
        persist_local_mock_execution(
            connection,
            _result(),
            batch_executor=_batch_executor,
        )

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1


def test_local_mock_identical_retry_is_idempotent():
    result = _result()
    connection, _, _ = _connection()

    first = persist_local_mock_execution(
        connection,
        result,
        batch_executor=_batch_executor,
    )
    repeated = persist_local_mock_execution(
        connection,
        result,
        batch_executor=_batch_executor,
    )

    assert repeated == first
    assert connection.commit.call_count == 2
    assert connection.rollback.call_count == 0


def test_local_mock_unavailable_disposition_persists_without_fills():
    result = simulate_local_mock_execution(
        _advanced_mark(),
        alert_id=ALERT,
        requested_at=NOW + timedelta(milliseconds=300),
        earnings=_earnings(
            LocalMockEarningsStatus.UNAVAILABLE,
            ("EARNINGS_COVERAGE_UNAVAILABLE",),
        ),
        policy=_mock_policy(),
    )
    connection, _, stored = _connection()

    persist_local_mock_execution(
        connection,
        result,
        batch_executor=_batch_executor,
    )

    assert stored["disposition"][2] == "MOCK_UNAVAILABLE"
    assert stored["disposition"][3] == ["EARNINGS_COVERAGE_UNAVAILABLE"]
    assert stored["fills"] == {}
    assert connection.commit.call_count == 1