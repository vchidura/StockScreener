"""_RowsBySecurity replaces a per-ticker rescan of the cohort's rows; it must return exactly what the filter did."""
from __future__ import annotations

import random
from uuid import UUID, uuid4

from realtime.ticker_projection import _RowsBySecurity


def _rows(count, securities, *, with_none, seed):
    rng = random.Random(seed)
    pool = [*securities, *([None] if with_none else [])]
    return [{"row": index, "security_id": rng.choice(pool)} for index in range(count)]


def test_grouping_returns_the_same_rows_in_the_same_order_as_the_filter_it_replaces():
    securities = [uuid4() for _ in range(6)]
    for seed in range(20):
        rows = _rows(200, securities, with_none=False, seed=seed)
        grouped = _RowsBySecurity(rows)
        for security in securities:
            assert grouped.of(str(security)) == [row for row in rows if str(row["security_id"]) == str(security)]
        assert grouped.of(str(uuid4())) == []


def test_shared_rows_without_a_security_are_included_at_their_original_positions():
    securities = [uuid4() for _ in range(5)]
    for seed in range(20):
        rows = _rows(200, securities, with_none=True, seed=seed)
        grouped = _RowsBySecurity(rows, shared_none=True)
        for security in securities:
            expected = [row for row in rows if row.get("security_id") is None or str(row["security_id"]) == str(security)]
            assert grouped.of(str(security)) == expected
        # a ticker with no rows of its own still sees the shared ones
        assert grouped.of(str(uuid4())) == [row for row in rows if row["security_id"] is None]


def test_a_missing_security_is_ignored_unless_shared_and_ids_match_by_text():
    own = uuid4()
    rows = [{"row": 0, "security_id": None}, {"row": 1, "security_id": str(own)}, {"row": 2}]
    assert _RowsBySecurity(rows[:2]).of(str(own)) == [rows[1]]
    assert _RowsBySecurity(rows, shared_none=True).of(str(own)) == rows
    assert _RowsBySecurity([{"row": 3, "security_id": UUID(int=own.int)}]).of(str(own))
