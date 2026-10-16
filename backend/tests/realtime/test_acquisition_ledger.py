from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.acquisition_ledger import (
    AcquisitionWatermark,
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RequestCompleteness,
    WatermarkTerminalState,
    advance_acquisition_watermark_cursor,
    finish_provider_request_cursor,
    load_acquisition_watermark_cursor,
    load_raw_payload_cursor,
    mark_stale_provider_requests_ambiguous_cursor,
    persist_raw_payload_cursor,
    raw_payload_id,
    record_provider_request_cursor,
    require_complete_provider_request_cursor,
    reserve_provider_request_cursor,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 12, 15, 0, tzinfo=UTC)
REQUEST = UUID("11111111-1111-1111-1111-111111111111")
COHORT = UUID("22222222-2222-2222-2222-222222222222")


def _reservation(**changes):
    values = dict(
        request_id=REQUEST, account_alias="developer", lane=ProviderRequestLane.OPTIONS_ALIGNMENT,
        endpoint_class="aggs_minute", request_sha256="a" * 64, reserved_at=NOW, cohort_id=COHORT,
    )
    values.update(changes)
    return ProviderRequestReservation(**values)


def _completion(**changes):
    values = dict(
        state=ProviderRequestState.COMPLETE, completed_at=NOW + timedelta(seconds=2),
        started_at=NOW + timedelta(seconds=1), outcome=ProviderRequestOutcome.OK, http_status=200,
        response_bytes=1_000, row_count=390, pages_fetched=2,
        requested_from=NOW - timedelta(hours=7), requested_to=NOW,
        price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
        completeness=RequestCompleteness.COMPLETE,
    )
    values.update(changes)
    return ProviderRequestCompletion(**values)


def test_ledger_rows_use_the_v2_completeness_and_price_basis_columns():
    cursor = MagicMock()
    cursor.rowcount = 1
    reservation, completion = _reservation(), _completion()
    cursor.fetchone.return_value = (
        str(REQUEST), "developer", "OPTIONS_ALIGNMENT", str(COHORT), None, None,
        "aggs_minute", "a" * 64, 1,
    )

    assert record_provider_request_cursor(cursor, reservation, completion) == REQUEST

    reserve = cursor.execute.call_args_list[0].args[0]
    assert reserve.startswith("INSERT INTO rt.provider_request_ledger") and "'RESERVED'" in reserve
    finish = next(c for c in cursor.execute.call_args_list if c.args[0].startswith("UPDATE rt.provider_request_ledger"))
    assert "price_basis=%s, completeness=%s" in finish.args[0]
    assert "state IN ('RESERVED', 'AMBIGUOUS')" in finish.args[0]
    assert "RAW_FINALIZED_MINUTE_CLOSE_V1" in finish.args[1]
    assert "COMPLETE" in finish.args[1]


def test_reservation_conflicts_are_rejected_and_finish_is_idempotent_only_when_identical():
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        str(REQUEST), "developer", "OPTIONS_CHAIN", str(COHORT), None, None,
        "aggs_minute", "a" * 64, 1, None, None, None, None, None, None,
    )
    with pytest.raises(ValueError, match="ledger identity conflict"):
        reserve_provider_request_cursor(cursor, _reservation())

    completion = _completion()
    cursor.rowcount = 0
    cursor.fetchone.return_value = (
        "COMPLETE", completion.started_at, completion.completed_at, 200, "OK", 1_000, 390, 2,
        completion.requested_from, completion.requested_to, "RAW_FINALIZED_MINUTE_CLOSE_V1",
        "COMPLETE", None, None,
    )
    finish_provider_request_cursor(cursor, REQUEST, completion)
    with pytest.raises(ValueError, match="different result"):
        finish_provider_request_cursor(cursor, REQUEST, _completion(row_count=1))
    cursor.fetchone.return_value = None
    with pytest.raises(ValueError, match="not reserved"):
        finish_provider_request_cursor(cursor, REQUEST, completion)


def test_coordinated_reservation_fields_are_all_or_none():
    with pytest.raises(ValueError, match="all present"):
        _reservation(budget_scope_sha256="b" * 64)
    coordinated = _reservation(
        budget_scope_sha256="b" * 64,
        budget_session_date=NOW.date(),
        authorization_reference="approved",
        reserved_response_bytes=2_001,
        account_request_ceiling=10,
        account_decoded_byte_ceiling=20_000,
    )
    assert coordinated.reserved_response_bytes == 2_001


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"state": ProviderRequestState.RESERVED}, "cannot leave the request RESERVED"),
        ({"outcome": None}, "requires completed_at and outcome"),
        ({"state": ProviderRequestState.FAILED, "outcome": ProviderRequestOutcome.TIMEOUT,
          "completeness": RequestCompleteness.NOT_APPLICABLE}, "requires a failure code"),
        ({"completeness": RequestCompleteness.INCOMPLETE}, "INCOMPLETE provider request requires"),
        ({"state": ProviderRequestState.FAILED, "outcome": ProviderRequestOutcome.HTTP_ERROR,
          "failure_code": "X", "completeness": RequestCompleteness.COMPLETE}, "only an OK COMPLETE"),
        ({"requested_to": NOW - timedelta(days=2)}, "window is inverted"),
        ({"started_at": NOW + timedelta(hours=1)}, "completes before it starts"),
        ({"row_count": -1}, "row_count"),
    ),
)
def test_completion_mirrors_the_ledger_check_constraints(changes, message):
    with pytest.raises(ValueError, match=message):
        _completion(**changes)


def test_an_incomplete_read_is_recorded_with_its_failure_code_and_rejected_by_consumers():
    incomplete = _completion(
        completeness=RequestCompleteness.INCOMPLETE, failure_code="PAGINATION_NOT_EXHAUSTED",
    )
    assert incomplete.completeness is RequestCompleteness.INCOMPLETE

    cursor = MagicMock()
    for row, message in (
        (None, "not recorded"),
        (("FAILED", "TIMEOUT", "NOT_APPLICABLE"), "did not complete"),
        (("AMBIGUOUS", None, "NOT_APPLICABLE"), "did not complete"),
        (("COMPLETE", "OK", "INCOMPLETE"), "incomplete"),
    ):
        cursor.fetchone.return_value = row
        with pytest.raises(ValueError, match=message):
            require_complete_provider_request_cursor(cursor, REQUEST)
    cursor.fetchone.return_value = ("COMPLETE", "OK", "NOT_APPLICABLE")
    require_complete_provider_request_cursor(cursor, REQUEST)


def test_stale_reservations_become_ambiguous():
    cursor = MagicMock()
    cursor.rowcount = 3
    assert mark_stale_provider_requests_ambiguous_cursor(cursor, reserved_before=NOW) == 3
    assert "SET state='AMBIGUOUS'" in cursor.execute.call_args.args[0]
    with pytest.raises(ValueError, match="timezone-aware"):
        mark_stale_provider_requests_ambiguous_cursor(cursor, reserved_before=datetime(2026, 1, 1))


def _watermark(**changes):
    values = dict(
        cohort_id=COHORT, instrument_id=7, interval="1m", source_kind="NATIVE_REST",
        last_complete_bar_end=NOW, overlap_policy="developer_rest_overlap_v1", request_id=REQUEST,
    )
    values.update(changes)
    return AcquisitionWatermark(**values)


def test_watermark_only_moves_forward_and_a_gap_needs_a_reason():
    cursor = MagicMock()
    cursor.fetchone.return_value = (NOW,)

    assert advance_acquisition_watermark_cursor(cursor, _watermark()) is True

    statement, parameters = cursor.execute.call_args.args
    assert "rt.acquisition_watermarks.last_examined_bar_end" in statement
    assert "EXCLUDED.last_complete_bar_end IS NULL" in statement
    assert "terminal_state = 'GAP'" in statement
    assert parameters[:4] == (str(COHORT), 7, "1m", "NATIVE_REST")
    cursor.fetchone.return_value = None
    assert advance_acquisition_watermark_cursor(cursor, _watermark()) is False
    with pytest.raises(ValueError, match="requires reason codes"):
        _watermark(terminal_state=WatermarkTerminalState.GAP)
    gap = _watermark(terminal_state=WatermarkTerminalState.NO_TRADE, reason_codes=("NO_TRADES_IN_MINUTE",))
    assert gap.reason_codes == ("NO_TRADES_IN_MINUTE",)
    unresolved = _watermark(
        last_complete_bar_end=None,
        last_examined_bar_end=NOW,
        terminal_state=WatermarkTerminalState.GAP,
        reason_codes=("MISSING_MINUTE",),
    )
    assert unresolved.last_complete_bar_end is None
    assert unresolved.last_examined_bar_end == NOW
    with pytest.raises(ValueError, match="examined boundary to be complete"):
        _watermark(
            last_complete_bar_end=NOW - timedelta(minutes=1),
            last_examined_bar_end=NOW,
        )
    with pytest.raises(ValueError, match="interval is unsupported"):
        _watermark(interval="2m")


def test_watermark_round_trips_from_its_row():
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        NOW,
        NOW,
        "developer_rest_overlap_v1",
        "COMPLETE",
        [],
        str(REQUEST),
    )
    loaded = load_acquisition_watermark_cursor(
        cursor, cohort_id=COHORT, instrument_id=7, interval="1m", source_kind="NATIVE_REST",
    )
    assert loaded == _watermark()
    cursor.fetchone.return_value = None
    assert load_acquisition_watermark_cursor(
        cursor, cohort_id=COHORT, instrument_id=7, interval="1m", source_kind="NATIVE_REST",
    ) is None


def test_raw_payload_is_chunked_content_addressed_and_keyed_by_arrival():
    content = b"x" * (1_048_576 + 5)
    cursor = MagicMock()
    chunks = []
    digest = __import__("hashlib").sha256(content).hexdigest()
    cursor.fetchone.return_value = (digest, len(content), 2)

    def executor(_cursor, statement, rows, *, page_size):
        assert "INSERT INTO rt.raw_payload_chunks" in statement and "ON CONFLICT DO NOTHING" in statement
        chunks.extend(rows)

    payload_id = persist_raw_payload_cursor(
        cursor, content, arrived_at=NOW, request_id=REQUEST, values_executor=executor,
    )

    assert payload_id == raw_payload_id(content)
    assert [len(row[3]) for row in chunks] == [1_048_576, 5]
    assert all(row[1] == NOW for row in chunks)
    header = cursor.execute.call_args_list[0]
    assert header.args[0].startswith("INSERT INTO rt.raw_payloads")
    assert header.args[1][1] == NOW and header.args[1][3:5] == (len(content), 2)
    cursor.fetchone.return_value = ("0" * 64, len(content), 2)
    with pytest.raises(ValueError, match="header conflict"):
        persist_raw_payload_cursor(cursor, content, arrived_at=NOW, values_executor=executor)
    with pytest.raises(ValueError, match="non-empty bytes"):
        persist_raw_payload_cursor(cursor, b"", arrived_at=NOW, values_executor=executor)


def test_raw_payload_load_verifies_chunks_and_hash():
    content = b"hello world"
    digest = __import__("hashlib").sha256(content).hexdigest()
    cursor = MagicMock()
    cursor.fetchone.return_value = (NOW, digest, len(content), 2)
    cursor.fetchall.return_value = ((0, memoryview(b"hello ")), (1, b"world"))

    assert load_raw_payload_cursor(cursor, raw_payload_id(content)) == content

    cursor.fetchall.return_value = ((0, b"hello "),)
    with pytest.raises(ValueError, match="chunks are incomplete"):
        load_raw_payload_cursor(cursor, raw_payload_id(content))
    cursor.fetchall.return_value = ((0, b"HELLO "), (1, b"world"))
    with pytest.raises(ValueError, match="does not match its header"):
        load_raw_payload_cursor(cursor, raw_payload_id(content))
    cursor.fetchone.return_value = None
    with pytest.raises(ValueError, match="not retained"):
        load_raw_payload_cursor(cursor, raw_payload_id(content))
