"""CRWD empty minute (Nov 11): the ticker verifier checks a stream 5m bar with a no-trade minute (sparse policy, fewer
than five 1m sources) by re-deriving it the finalizer's way from the stored minutes of its window."""
from datetime import timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest

from equity.derivation import derive_advanced_sparse_minute_window
from realtime.ticker_projection import TickerProjectionState, _DerivationVerifier, _fact, _same_aggregate, _security
from tests.realtime.test_ticker_vertical_slice import ticker_fixture


def _window():
    inputs = ticker_fixture()
    member = next(row for row in inputs.members if row["ticker"] == "AAPL")
    rows = sorted((row for row in inputs.bars if row["ticker"] == "AAPL" and row["interval"] == "1m"),
                  key=lambda row: row["bar_start"])
    by_start = {row["bar_start"]: row for row in rows}
    for row in rows:
        start = row["bar_start"]
        if start.minute % 5 == 0 and all(start + timedelta(minutes=i) in by_start for i in range(5)):
            minutes = [_fact(by_start[start + timedelta(minutes=i)], inputs) for i in range(5)]
            verifier = _DerivationVerifier(TickerProjectionState(), _security(member), inputs, UUID(int=11))
            return inputs, member, verifier, start, minutes
    pytest.skip("fixture has no complete five-minute window of 1m bars")


def _sparse(member, inputs, start, kept):
    return SimpleNamespace(revision=derive_advanced_sparse_minute_window(
        _security(member), [fact.revision for fact in kept], window_start=start,
        window_end=start + timedelta(minutes=5), observed_at=inputs.cutoff, ingestion_segment_id=UUID(int=12)))


def test_a_sparse_5m_bar_matches_its_re_derivation_from_the_stored_minutes():
    inputs, member, verifier, start, minutes = _window()
    stored_minutes = [fact for index, fact in enumerate(minutes) if index != 1]  # no trade in the second minute
    stored = _sparse(member, inputs, start, stored_minutes)
    assert len(stored.revision.source_bar_revision_ids) == 4
    expected = verifier._sparse_minute_expectation(stored, stored_minutes)
    assert expected is not None and _same_aggregate(stored, expected)


def test_a_sparse_bar_that_left_out_a_stored_minute_does_not_match():
    inputs, member, verifier, start, minutes = _window()
    stored = _sparse(member, inputs, start, minutes[:3])  # the stored minutes include a fourth one it left out
    expected = verifier._sparse_minute_expectation(stored, minutes[:4])
    assert expected is not None and not _same_aggregate(stored, expected)


def test_a_full_five_source_bar_is_left_to_the_dense_derivation():
    inputs, member, verifier, start, minutes = _window()
    assert verifier._sparse_minute_expectation(_sparse(member, inputs, start, minutes), minutes) is None
