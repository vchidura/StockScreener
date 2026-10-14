from __future__ import annotations

from datetime import date

import pytest

from option_flow_support import CALL, COHORT, OTHER, T0
from realtime.option_flow.volume import (
    BaselineKind,
    PrintStatus,
    VolumeBook,
    VolumeConfig,
)

SESSION = date(2026, 10, 12)


def _book(**overrides) -> VolumeBook:
    return VolumeBook(COHORT, VolumeConfig(**overrides) if overrides else None)


def _add(book, seq, size=10, time_ms=None, price=5.0, exchange=302, contract=CALL):
    return book.add_print(contract, SESSION, seq, T0 + seq if time_ms is None else time_ms, exchange, price, size)


def _cancel(book, seq, size=10, time_ms=None, price=5.0, exchange=302, contract=CALL):
    return book.cancel_print(contract, SESSION, seq, T0 + seq if time_ms is None else time_ms, exchange, price, size)


def test_every_print_is_accumulated_with_before_and_after():
    book = _book(initial_baseline=BaselineKind.SESSION_COMPLETE)
    first, second = _add(book, 1, size=10), _add(book, 2, size=25)
    assert (first.before, first.after, first.status) == (0, 10, PrintStatus.COUNTED)
    assert (second.before, second.after) == (10, 35)
    assert first.baseline is BaselineKind.SESSION_COMPLETE


def test_warming_baseline_is_the_default_for_a_mid_session_start():
    assert _add(_book(), 1).baseline is BaselineKind.WARMING


def test_duplicate_print_is_idempotent():
    book = _book()
    _add(book, 1, size=10)
    again = _add(book, 1, size=10)
    assert again.status is PrintStatus.DUPLICATE and again.after == 10
    assert book.get(CALL, SESSION).cumulative == 10


def test_conflicting_duplicate_keeps_the_first_and_makes_the_baseline_incomplete():
    book = _book(initial_baseline=BaselineKind.SESSION_COMPLETE)
    _add(book, 1, size=10)
    conflict = _add(book, 1, size=12)
    assert conflict.status is PrintStatus.CONFLICT and conflict.baseline is BaselineKind.INCOMPLETE
    assert book.get(CALL, SESSION).cumulative == 10
    assert book.get(CALL, SESSION).reason == "DUPLICATE_SEQUENCE_CONFLICT"


def test_out_of_order_print_is_counted_and_flagged_late():
    book = _book()
    _add(book, 5, size=10)
    late = _add(book, 3, size=7)
    assert late.late and late.status is PrintStatus.COUNTED
    assert book.get(CALL, SESSION).cumulative == 17 and book.get(CALL, SESSION).max_seq == 5


def test_contracts_are_accumulated_independently():
    book = _book()
    _add(book, 1, size=10)
    _add(book, 1, size=99, contract=OTHER)
    assert book.get(CALL, SESSION).cumulative == 10 and book.get(OTHER, SESSION).cumulative == 99


def test_cancel_removes_the_original_volume_and_supersedes_it():
    book = _book(initial_baseline=BaselineKind.SESSION_COMPLETE)
    original = _add(book, 1, size=40)
    _add(book, 2, size=10)
    outcome = _cancel(book, 3, size=40, time_ms=T0 + 3)
    assert outcome.matched is original.record and (outcome.before, outcome.after) == (50, 10)
    assert not original.record.active
    assert book.get(CALL, SESSION).baseline is BaselineKind.SESSION_COMPLETE


def test_cancel_is_idempotent_and_does_not_cancel_twice():
    book = _book()
    _add(book, 1, size=40)
    _cancel(book, 3, size=40)
    again = _cancel(book, 3, size=40)
    assert again.duplicate and book.get(CALL, SESSION).cumulative == 0
    second_cancel = _cancel(book, 4, size=40)
    assert second_cancel.matched is None and second_cancel.reason == "CANCEL_ORIGINAL_NOT_FOUND"


def test_cancel_matches_the_newest_live_print_that_agrees():
    book = _book()
    older = _add(book, 1, size=10)
    newer = _add(book, 2, size=10)
    assert _cancel(book, 3, size=10).matched is newer.record
    assert _cancel(book, 4, size=10).matched is older.record
    assert book.get(CALL, SESSION).cumulative == 0


def test_cancel_with_a_different_price_or_exchange_does_not_match():
    book = _book(initial_baseline=BaselineKind.SESSION_COMPLETE)
    _add(book, 1, size=10)
    assert _cancel(book, 2, size=10, price=5.5).matched is None
    assert book.get(CALL, SESSION).baseline is BaselineKind.INCOMPLETE


def test_correction_is_a_cancel_plus_replacement_and_nets_out():
    book = _book(initial_baseline=BaselineKind.SESSION_COMPLETE)
    _add(book, 1, size=40, price=5.0)
    _cancel(book, 2, size=40, price=5.0)
    replacement = _add(book, 3, size=40, price=5.1)
    assert replacement.after == 40 and book.get(CALL, SESSION).baseline is BaselineKind.SESSION_COMPLETE


def test_unmatched_cancel_on_a_warming_baseline_is_expected_and_keeps_it_warming():
    book = _book()
    outcome = _cancel(book, 1, size=10)
    assert outcome.matched is None and outcome.reason == "CANCEL_ORIGINAL_NOT_FOUND"
    assert book.get(CALL, SESSION).baseline is BaselineKind.WARMING


def test_gap_makes_every_baseline_incomplete_including_contracts_not_yet_seen():
    book = _book(initial_baseline=BaselineKind.SESSION_COMPLETE)
    _add(book, 1)
    assert book.mark_all_incomplete(SESSION, "STREAM_DISCONNECT", T0 + 1000) == 1
    assert book.get(CALL, SESSION).baseline is BaselineKind.INCOMPLETE
    unseen = _add(book, 1, contract=OTHER)
    assert unseen.baseline is BaselineKind.INCOMPLETE
    assert book.get(OTHER, SESSION).reason == "STREAM_DISCONNECT"


def test_a_scoped_gap_only_touches_the_named_contract():
    book = _book(initial_baseline=BaselineKind.SESSION_COMPLETE)
    _add(book, 1)
    _add(book, 1, contract=OTHER)
    book.mark_incomplete(CALL, SESSION, "BACKPRESSURE_BUFFER_FULL", T0 + 10)
    assert book.get(CALL, SESSION).baseline is BaselineKind.INCOMPLETE
    assert book.get(OTHER, SESSION).baseline is BaselineKind.SESSION_COMPLETE


def test_warmup_then_snapshot_reconcile_never_double_counts():
    book = _book()
    for seq in range(1, 6):
        _add(book, seq, size=10)
    assert book.get(CALL, SESSION).cumulative == 50
    # The snapshot already includes prints 1-3 (30 shares of ours plus 90 before we started).
    result = book.reconcile(CALL, SESSION, snapshot_volume=120, watermark_seq=3, snapshot_time_ms=T0 + 10)
    assert result.accepted and result.cumulative == 120 + 20
    state = book.get(CALL, SESSION)
    assert state.baseline is BaselineKind.SNAPSHOT_RECONCILED
    assert (state.snapshot_volume, state.snapshot_watermark) == (120, 3)
    late = _add(book, 2, size=10, time_ms=T0 + 99)
    assert late.status is PrintStatus.COVERED_BY_SNAPSHOT and late.after == late.before == 140
    redelivered = _add(book, 4, size=10)
    assert redelivered.status is PrintStatus.DUPLICATE
    fresh = _add(book, 6, size=10)
    assert (fresh.before, fresh.after, fresh.baseline) == (140, 150, BaselineKind.SNAPSHOT_RECONCILED)


def test_reconcile_with_a_cancel_after_the_watermark_of_a_print_inside_the_snapshot():
    book = _book()
    _add(book, 1, size=40)
    _add(book, 2, size=10)
    _cancel(book, 5, size=40, time_ms=T0 + 5)
    assert book.get(CALL, SESSION).cumulative == 10
    # The snapshot (through seq 2) includes the 40 that the later cancel removes.
    result = book.reconcile(CALL, SESSION, 100, 2, T0 + 6)
    assert result.accepted and result.cumulative == 60


def test_cancel_inside_the_snapshot_is_not_applied_twice():
    book = _book()
    _add(book, 1, size=40)
    book.reconcile(CALL, SESSION, 100, 5, T0 + 6)
    outcome = _cancel(book, 4, size=40, time_ms=T0 + 4)
    assert outcome.after == outcome.before == 100
    after_watermark = _cancel(book, 7, size=40, time_ms=T0 + 7)
    assert after_watermark.matched is None and after_watermark.reason == "CANCEL_ORIGINAL_NOT_FOUND"


def test_reconcile_recovers_an_incomplete_baseline_only_with_a_snapshot_after_the_gap():
    book = _book(initial_baseline=BaselineKind.SESSION_COMPLETE)
    _add(book, 1, size=10)
    book.mark_all_incomplete(SESSION, "STREAM_DISCONNECT", T0 + 1000)
    early = book.reconcile(CALL, SESSION, 500, 1, T0 + 500)
    assert not early.accepted and early.reason == "RECONCILE_SNAPSHOT_PREDATES_GAP"
    assert book.get(CALL, SESSION).baseline is BaselineKind.INCOMPLETE
    late = book.reconcile(CALL, SESSION, 500, 1, T0 + 2000)
    assert late.accepted and late.baseline is BaselineKind.SNAPSHOT_RECONCILED


def test_reconcile_is_rejected_when_history_cannot_prove_the_watermark():
    book = _book(history_per_contract=3)
    for seq in range(1, 8):
        _add(book, seq, size=10)
    result = book.reconcile(CALL, SESSION, 500, 2, T0 + 100)
    assert not result.accepted and result.reason == "RECONCILE_HISTORY_EVICTED"
    assert book.get(CALL, SESSION).cumulative == 70
    assert book.reconcile(CALL, SESSION, 500, 6, T0 + 100).accepted


def test_a_reconcile_cannot_move_the_watermark_backwards():
    book = _book()
    _add(book, 1)
    book.reconcile(CALL, SESSION, 100, 5, T0 + 5)
    older = book.reconcile(CALL, SESSION, 90, 3, T0 + 6)
    assert not older.accepted and older.reason == "RECONCILE_WATERMARK_OLDER_THAN_PREVIOUS"


def test_reconcile_rejects_an_unmatched_cancel_after_the_watermark():
    book = _book()
    _cancel(book, 9, size=10)
    result = book.reconcile(CALL, SESSION, 100, 5, T0 + 20)
    assert not result.accepted and result.reason == "RECONCILE_UNMATCHED_CANCEL"


def test_reconcile_validates_its_inputs():
    book = _book()
    with pytest.raises(ValueError):
        book.reconcile(CALL, SESSION, -1, 1, T0)
    with pytest.raises(ValueError):
        book.reconcile(CALL, SESSION, 1, -1, T0)


def test_a_print_older_than_the_retained_history_is_not_guessed_at():
    book = _book(history_per_contract=2, initial_baseline=BaselineKind.SESSION_COMPLETE)
    for seq in range(10, 15):
        _add(book, seq)
    beyond = _add(book, 3)
    assert beyond.status is PrintStatus.BEYOND_HISTORY and beyond.baseline is BaselineKind.INCOMPLETE
    assert book.get(CALL, SESSION).cumulative == 50


def test_history_is_bounded():
    book = _book(history_per_contract=5)
    for seq in range(100):
        _add(book, seq)
    state = book.get(CALL, SESSION)
    assert len(state.history) == len(state.order) == 5 and state.cumulative == 1000


def test_checkpoint_rows_carry_the_baseline_and_are_emitted_once_when_dirty():
    book = _book()
    _add(book, 1, size=10)
    _add(book, 1, size=5, contract=OTHER)
    rows = book.checkpoint_rows()
    assert [row.contract_ticker for row in rows] == sorted([CALL, OTHER])
    row = next(item for item in rows if item.contract_ticker == CALL)
    assert (row.cumulative_volume, row.last_source_seq, row.baseline_kind) == (10, 1, "WARMING")
    assert row.snapshot_volume is None and row.snapshot_watermark_seq is None and row.cohort_id == COHORT
    assert book.checkpoint_rows() == []
    book.reconcile(CALL, SESSION, 100, 1, T0 + 5)
    reconciled = book.checkpoint_rows()[0]
    assert (reconciled.baseline_kind, reconciled.snapshot_volume, reconciled.snapshot_watermark_seq) == (
        "SNAPSHOT_RECONCILED", 100, 1)
    assert len(book.checkpoint_rows(dirty_only=False)) == 2


def test_state_digest_is_identical_for_identical_inputs_and_differs_otherwise():
    first, second = _book(), _book()
    for book in (first, second):
        _add(book, 1)
        _add(book, 2, size=7)
    assert first.digest() == second.digest()
    _add(second, 3)
    assert first.digest() != second.digest()


def test_initial_baseline_is_restricted():
    with pytest.raises(ValueError):
        VolumeConfig(initial_baseline=BaselineKind.INCOMPLETE)
