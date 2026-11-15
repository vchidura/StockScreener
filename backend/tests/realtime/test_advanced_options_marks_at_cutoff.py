"""V3-1.5 (Nov 15): the window's marks at the cutoff (REALTIME_V3_MARKS_AT_CUTOFF = OFF / RECORD / JOURNAL)."""
import hashlib
import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest

from realtime.advanced_options_cutoff_quotes import (
    MARKS_JOURNAL_UNAVAILABLE,
    QUOTE_COLUMNS,
    CutoffQuotesUnavailable,
    head_at_barrier,
    quote_current_row,
    read_cutoff_quotes,
)
from realtime.advanced_options_mark_producer import (
    MARKS_AT_CUTOFF_SETTING,
    AdvancedOptionsMarkStatus,
    MarksAtCutoffMode,
    PostgresAdvancedOptionsMarkProducer,
    assemble_advanced_options_marks_cursor,
    build_advanced_options_mark_producer_plan,
    compare_cutoff_marks,
    marks_at_cutoff_mode,
)
from realtime.database_access_v2 import V2ConnectionProvider
from realtime.journal import RawJournalReader, RawJournalWriter
from realtime.materialization import (
    MaterializerShardState,
    _materialize_partitioned_shard_batch,
)
from test_advanced_options_input import NOW
from test_advanced_options_mark_producer import _QuoteCursor, _plan, _quote_rows


UTC = timezone.utc


# ---------------------------------------------------------------- switch


def test_switch_defaults_off_and_names_itself_when_invalid():
    assert marks_at_cutoff_mode({}) is MarksAtCutoffMode.OFF
    assert marks_at_cutoff_mode({MARKS_AT_CUTOFF_SETTING: "  "}) is MarksAtCutoffMode.OFF
    assert marks_at_cutoff_mode({MARKS_AT_CUTOFF_SETTING: "record"}) is MarksAtCutoffMode.RECORD
    assert marks_at_cutoff_mode({MARKS_AT_CUTOFF_SETTING: "JOURNAL"}) is MarksAtCutoffMode.JOURNAL
    with pytest.raises(ValueError, match=MARKS_AT_CUTOFF_SETTING):
        marks_at_cutoff_mode({MARKS_AT_CUTOFF_SETTING: "ON"})


# ---------------------------------------------------------------- journal read

GENERATION = UUID("00000000-0000-4000-8000-0000000000a1")
REGISTERED_SESSION = UUID("00000000-0000-4000-8000-0000000000b1")
ATTEMPT_SESSION = uuid5(REGISTERED_SESSION, "connection-attempt:1")
BASE_MS = int(NOW.timestamp() * 1000) - 5_000


def _journal(tmp_path, events):
    """A real journal file (the writer's schema) holding the given normalized events, one frame each.
    events: (instrument_id, kind, source_timestamp_ms, sequence, bid_text) in offset order."""
    path = tmp_path / "options.sqlite"
    RawJournalWriter(path, generation=GENERATION).close()
    connection = sqlite3.connect(path)
    for number, (instrument_id, kind, stamp_ms, sequence, bid) in enumerate(events, start=1):
        payload = f"frame-{number}".encode()
        received = (NOW - timedelta(seconds=5) + timedelta(milliseconds=number * 10)).isoformat()
        connection.execute(
            "INSERT INTO raw_messages(journal_generation,stream_session_id,message_number,feed,received_at,"
            "received_monotonic_ns,byte_length,payload,payload_sha256,schema_version,committed_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,1,?)",
            (str(GENERATION), str(ATTEMPT_SESSION), number, "OPTIONS", received, number, len(payload), payload,
             hashlib.sha256(payload).hexdigest(), received),
        )
        event_at = (NOW.replace(microsecond=0) - timedelta(seconds=5)).astimezone(UTC)
        event_at = event_at + timedelta(milliseconds=stamp_ms - BASE_MS)
        connection.execute(
            "INSERT INTO normalized_events(raw_offset,event_index,normalizer_version,instrument_id,provider_symbol,"
            "underlying,asset_class,shard_id,event_kind,source_session,source_sequence,source_timestamp,"
            "source_timestamp_unit,event_at,price_text,size_text,bid_text,ask_text,bid_size,ask_size,exchange,"
            "conditions_json,quality,reason_codes_json,details_json,payload_sha256,committed_at) "
            "VALUES(?,0,'n1',?,?,'SPY','OPTION',0,?,?,?,?,'ms',?,?,?,?,?,?,?,?,'[]','GOOD','[]',?,?,?)",
            (number, instrument_id, f"O:SPY{instrument_id}", kind, NOW.date().isoformat(), sequence, stamp_ms,
             event_at.isoformat(),
             "1.25" if kind == "T" else None, "1" if kind == "T" else None,
             bid if kind == "Q" else None, "1.75" if kind == "Q" else None,
             10 if kind == "Q" else None, 12 if kind == "Q" else None, 301 if kind == "T" else None,
             json.dumps({"bid_exchange": 301, "ask_exchange": 302} if kind == "Q" else {}),
             hashlib.sha256(f"event-{number}".encode()).hexdigest(), received),
        )
    connection.commit()
    connection.close()
    return path


def _read(path, barrier, instruments=(7,), **kwargs):
    return read_cutoff_quotes(
        path,
        instrument_ids=instruments,
        barrier_offset=barrier,
        journal_generation=GENERATION,
        registered_stream_session_id=REGISTERED_SESSION,
        allowed_stream_session_ids=frozenset({REGISTERED_SESSION, ATTEMPT_SESSION}),
        normalizer_version="n1",
        **kwargs,
    )


def _materializer_head(path, barrier, instrument_id):
    records = tuple(
        record for record in RawJournalReader(path).read_normalized_after(0, limit=10_000)
        if record.event_offset <= barrier
    )
    state = _materialize_partitioned_shard_batch(
        MaterializerShardState(0, "a" * 64, 0),
        records,
        scanned_offset=barrier,
        maximum_quote_history_per_instrument=8,
    )
    heads = {quote.instrument_id: quote for quote in state.quotes}
    if instrument_id not in heads:
        return None
    return quote_current_row(heads[instrument_id], shard_id=0, registered_stream_session_id=REGISTERED_SESSION)


EVENTS = (
    (7, "Q", BASE_MS + 1_000, 10, "1.5"),     # 1
    (7, "T", BASE_MS + 1_100, 11, None),      # 2
    (8, "Q", BASE_MS + 1_200, 12, "2.1"),     # 3
    (7, "Q", BASE_MS + 2_000, 13, "1.55"),    # 4
    (7, "Q", BASE_MS + 2_500, 9, "1.40"),     # 5: late, older provider sequence: never the head
    (7, "Q", BASE_MS + 6_000, 14, "1.60"),    # 6: after the barrier
)


def test_journal_read_is_the_materializer_head_at_the_barrier(tmp_path):
    path = _journal(tmp_path, EVENTS)
    for barrier in (1, 3, 4, 5, 6):
        read = _read(path, barrier, instruments=(7, 8))
        for instrument_id in (7, 8):
            assert read.quotes.get(instrument_id) == _materializer_head(path, barrier, instrument_id), barrier
    assert _read(path, 5).quotes[7]["source_offset"] == 4
    assert _read(path, 6).quotes[7]["source_offset"] == 6


def test_journal_row_has_the_quote_current_shape_and_types(tmp_path):
    path = _journal(tmp_path, EVENTS)
    row = _read(path, 5).quotes[7]
    assert tuple(row) == QUOTE_COLUMNS
    assert row["bid"] == Decimal("1.55") and str(row["bid"]) == "1.55000000"   # numeric(20,8) as Postgres returns it
    assert row["session_id"] == str(REGISTERED_SESSION)                     # registered, not the attempt id
    assert row["journal_generation"] == str(GENERATION)
    assert row["event_time_raw"] == BASE_MS + 2_000 and row["event_time_unit"] == "ms"
    assert row["reason_codes"] == [] and row["quality"] == "GOOD" and row["shard_id"] == 0
    assert row["event_at"].tzinfo is not None and row["received_at"].tzinfo is not None


def test_journal_read_reports_contracts_without_a_quote(tmp_path):
    path = _journal(tmp_path, EVENTS)
    read = _read(path, 2, instruments=(7, 8, 99))
    assert set(read.quotes) == {7}
    assert read.instruments_without_quote == (8, 99)


def test_missing_or_foreign_journal_is_unavailable(tmp_path):
    with pytest.raises(CutoffQuotesUnavailable, match=MARKS_JOURNAL_UNAVAILABLE):
        _read(tmp_path / "absent.sqlite", 3)
    path = _journal(tmp_path, EVENTS)
    with pytest.raises(CutoffQuotesUnavailable, match="JOURNAL_GENERATION_MISMATCH"):
        read_cutoff_quotes(path, instrument_ids=(7,), barrier_offset=3, journal_generation=uuid4(),
                           registered_stream_session_id=REGISTERED_SESSION)
    with pytest.raises(CutoffQuotesUnavailable, match="JOURNAL_IDENTITY_MISMATCH"):
        read_cutoff_quotes(
            path, instrument_ids=(7,), barrier_offset=3, journal_generation=GENERATION,
            registered_stream_session_id=REGISTERED_SESSION, normalizer_version="other")


def test_head_fold_follows_the_provider_sequence(tmp_path):
    path = _journal(tmp_path, EVENTS)
    records = tuple(
        record for record in RawJournalReader(path).read_normalized_after(0)
        if record.instrument_id == 7 and record.event_kind.value == "Q" and record.event_offset <= 5
    )
    head, shard = head_at_barrier(reversed(records))
    assert head.source_offset == 4 and shard == 0


# ---------------------------------------------------------------- the mark rule with the journal read


def _with_barrier(plan, barrier):
    return build_advanced_options_mark_producer_plan(
        enrollment=plan.enrollment,
        reference_state=plan.reference_state,
        rolling_state=plan.rolling_state,
        source_flow=plan.source_flow,
        cutoff=plan.cutoff,
        producer_build_id=plan.producer_build_id,
        producer_build_sha256=plan.producer_build_sha256,
        valuation_policy=plan.valuation_policy,
        market_state_committed_offset=barrier,
    )


def _quotes_of(rows, **changes):
    return {
        int(row["instrument_id"]): {**{key: row[key] for key in QUOTE_COLUMNS}, **changes}
        for row in rows
    }


def test_off_is_todays_read_unchanged():
    plan = _with_barrier(_plan(), 100)
    rows = _quote_rows(plan, event_at=NOW - timedelta(milliseconds=200), received_at=NOW - timedelta(milliseconds=150))
    plain = assemble_advanced_options_marks_cursor(_QuoteCursor(rows), plan)
    observed = {}
    recorded = assemble_advanced_options_marks_cursor(_QuoteCursor(rows), plan, observed_quotes=observed)
    assert plain == recorded
    assert plain.marks and set(observed) == {row.instrument_id for row in plain.evidence}


def test_unchanged_contract_gives_the_identical_mark_from_the_journal():
    plan = _with_barrier(_plan(), 100)
    rows = _quote_rows(plan, event_at=NOW - timedelta(milliseconds=200), received_at=NOW - timedelta(milliseconds=150))
    today = assemble_advanced_options_marks_cursor(_QuoteCursor(rows), plan)
    journal = assemble_advanced_options_marks_cursor(_QuoteCursor(rows), plan, cutoff_quotes=_quotes_of(rows))
    assert journal.marks_sha256 == today.marks_sha256
    assert journal == today


def test_journal_marks_a_contract_that_quoted_after_the_barrier():
    plan = _with_barrier(_plan(), 100)
    after = _quote_rows(plan, event_at=NOW + timedelta(seconds=40), received_at=NOW + timedelta(seconds=40))
    after = tuple({**row, "source_offset": 150} for row in after)          # today's head, past the barrier
    at_cutoff = _quotes_of(
        after,
        source_offset=90,
        event_at=NOW - timedelta(milliseconds=300),
        received_at=NOW - timedelta(milliseconds=290),
        event_time_raw=int((NOW - timedelta(milliseconds=300)).timestamp() * 1000),
    )
    today = assemble_advanced_options_marks_cursor(_QuoteCursor(after), plan)
    journal = assemble_advanced_options_marks_cursor(_QuoteCursor(after), plan, cutoff_quotes=at_cutoff)
    assert not today.marks
    assert all("OPTION_NBBO_AHEAD_OF_ROLLING_CHECKPOINT" in row.reason_codes for row in today.evidence)
    assert journal.marks and {mark.source_offset for mark in journal.marks} == {90}
    document = compare_cutoff_marks(
        today=today, journal=journal,
        today_quotes={key: {"source_offset": 150} for key in at_cutoff},
        journal_quotes=at_cutoff, barrier_offset=100, cutoff=NOW, window_id="w1",
    )
    assert document["differ"] == len(at_cutoff) and document["same"] == 0
    assert document["classes"] == {"QUOTED_AFTER_BARRIER": len(at_cutoff)}
    assert document["unexpected"] == 0


def test_journal_ages_in_market_time():
    plan = _with_barrier(_plan(), 100)
    # Event 1.05 s before the cutoff, received 0.2 s before it: fresh by the receive clock (today), stale in market time.
    late = _quote_rows(plan, event_at=NOW - timedelta(milliseconds=1050), received_at=NOW - timedelta(milliseconds=200))
    today = assemble_advanced_options_marks_cursor(_QuoteCursor(late), plan)
    journal = assemble_advanced_options_marks_cursor(_QuoteCursor(late), plan, cutoff_quotes=_quotes_of(late))
    assert today.marks and not journal.marks
    observed = {int(row["instrument_id"]): {"source_offset": row["source_offset"]} for row in late}
    document = compare_cutoff_marks(today=today, journal=journal, today_quotes=observed, journal_quotes=observed,
                                    barrier_offset=100, cutoff=NOW)
    assert set(document["classes"]) == {"AGE_BASIS"} and document["unexpected"] == 0
    # A skewed host clock (event 3 s past its receive time) is noncausal today and fine in market time.
    skewed = _quote_rows(plan, event_at=NOW - timedelta(milliseconds=100), received_at=NOW - timedelta(seconds=3))
    assert not assemble_advanced_options_marks_cursor(_QuoteCursor(skewed), plan).marks
    marked = assemble_advanced_options_marks_cursor(_QuoteCursor(skewed), plan, cutoff_quotes=_quotes_of(skewed))
    assert marked.marks and all(mark.observed_time == mark.market_time for mark in marked.marks)


def test_contract_without_a_journal_quote_is_absent():
    plan = _with_barrier(_plan(), 100)
    rows = _quote_rows(plan, event_at=NOW - timedelta(milliseconds=200), received_at=NOW - timedelta(milliseconds=150))
    result = assemble_advanced_options_marks_cursor(_QuoteCursor(rows), plan, cutoff_quotes={})
    assert not result.marks
    assert all("OPTION_NBBO_ABSENT" in row.reason_codes for row in result.evidence)


def test_same_quote_with_a_different_outcome_is_unexpected():
    plan = _with_barrier(_plan(), 100)
    rows = _quote_rows(plan, event_at=NOW - timedelta(milliseconds=200), received_at=NOW - timedelta(milliseconds=150))
    today = assemble_advanced_options_marks_cursor(_QuoteCursor(rows), plan)
    crossed = _quotes_of(rows, quality="CROSSED")
    journal = assemble_advanced_options_marks_cursor(_QuoteCursor(rows), plan, cutoff_quotes=crossed)
    observed = {key: {"source_offset": 1} for key in crossed}
    document = compare_cutoff_marks(today=today, journal=journal, today_quotes=observed, journal_quotes=crossed,
                                    barrier_offset=100, cutoff=NOW)
    assert document["classes"] == {"RULE_DIFFERS": len(crossed)} and document["unexpected"] == len(crossed)
    other_head = _quotes_of(rows, source_offset=50)
    document = compare_cutoff_marks(today=today, journal=journal, today_quotes=observed, journal_quotes=other_head,
                                    barrier_offset=100, cutoff=NOW)
    assert document["classes"] == {"HEAD_DIFFERS": len(crossed)}


# ---------------------------------------------------------------- producer and window wiring


class _Cursor(_QuoteCursor):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _Provider(V2ConnectionProvider):
    def __init__(self, rows, fail=False):
        self.rows = rows
        self.fail = fail
        self.calls = []

    @contextmanager
    def transaction(self, *args, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("database down")
        yield SimpleNamespace(connection=SimpleNamespace(cursor=lambda **_: _Cursor(self.rows)), cursor=None)


def test_record_producer_publishes_todays_preflight():
    plan = _with_barrier(_plan(), 100)
    rows = _quote_rows(plan, event_at=NOW - timedelta(milliseconds=200), received_at=NOW - timedelta(milliseconds=150))
    plain = PostgresAdvancedOptionsMarkProducer(_Provider(rows), plan).preflight()
    recorder = PostgresAdvancedOptionsMarkProducer(_Provider(rows), plan, record_observed=True)
    assert recorder.preflight() == plain
    assert set(recorder.observed_quotes) == {row.instrument_id for row in plain.evidence}
    assert PostgresAdvancedOptionsMarkProducer(_Provider(rows), plan).observed_quotes is None


def test_window_journal_read_falls_back_and_record_never_fails(tmp_path, caplog):
    from realtime.advanced_options_p8_window import _record_cutoff_marks, _window_cutoff_quotes

    plan = _with_barrier(_plan(), 100)
    binding = SimpleNamespace(enrollment=plan.enrollment, window_id=uuid4())
    registration = SimpleNamespace(journal_generation=GENERATION, stream_session_id=REGISTERED_SESSION,
                                   normalizer_version="n1")
    environment = SimpleNamespace(options_flow_source=SimpleNamespace(
        path=tmp_path / "absent.sqlite", registration=registration,
        source_flow=SimpleNamespace(stream_session_ids=(REGISTERED_SESSION,)),
    ))
    read, status = _window_cutoff_quotes(environment, binding, 100)
    assert read is None and status.startswith(MARKS_JOURNAL_UNAVAILABLE)
    read, status = _window_cutoff_quotes(SimpleNamespace(), binding, 100)
    assert read is None and status.startswith(MARKS_JOURNAL_UNAVAILABLE)

    rows = _quote_rows(plan, event_at=NOW - timedelta(milliseconds=200), received_at=NOW - timedelta(milliseconds=150))
    today = assemble_advanced_options_marks_cursor(_QuoteCursor(rows), plan)
    caplog.set_level(logging.WARNING)
    document = _record_cutoff_marks(
        _Provider(rows, fail=True), binding, plan, today,
        today_quotes={}, cutoff_read=SimpleNamespace(quotes=_quotes_of(rows), read_ms=1.0),
        cutoff_status="OK", barrier_offset=100, cutoff=NOW,
    )
    assert document["journal_status"].startswith("JOURNAL_PREFLIGHT_FAILED")
    assert any("MARKS_AT_CUTOFF_RECORD" in record.getMessage() for record in caplog.records)
    unavailable = _record_cutoff_marks(
        _Provider(rows), binding, plan, today, today_quotes={}, cutoff_read=None,
        cutoff_status=f"{MARKS_JOURNAL_UNAVAILABLE}:JOURNAL_FILE_MISSING", barrier_offset=100, cutoff=NOW,
    )
    assert unavailable["journal"] is None and unavailable["journal_status"].startswith(MARKS_JOURNAL_UNAVAILABLE)
    assert today.status is AdvancedOptionsMarkStatus.READY


def test_a_read_over_its_time_budget_is_unavailable(tmp_path):
    path = _journal(tmp_path, EVENTS)
    with pytest.raises(CutoffQuotesUnavailable, match="READ_TIME_BUDGET_EXCEEDED"):
        _read(path, 5, instruments=range(1, 3000), time_budget_seconds=-1.0)
    assert set(_read(path, 5, instruments=range(1, 3000), time_budget_seconds=None).quotes) == {7, 8}
