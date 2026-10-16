"""Memory sharing of bar rows and facts: identical output, fewer objects (the 400-name memory budget)."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import realtime.screening_repository as repository
import realtime.ticker_projection as projection
from realtime.screening_repository import share_bar_values
from realtime.ticker_projection import TickerProjectionState, _price

COHORT, SECURITY = uuid4(), uuid4()
STAMP = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)


def _copy(text):
    """A distinct str object with the same value, as psycopg2 builds one per value per row."""
    return "".join(list(text))


def _row(name="AAPL", **changes):
    row = {
        "bar_revision_id": uuid4(), "cohort_id": UUID(int=COHORT.int), "provider_tier": _copy("MASSIVE_DEVELOPER"),
        "security_id": UUID(int=SECURITY.int), "instrument_id": 1000 + len(name), "ticker": _copy(name),
        "interval": _copy("1m"), "session_date": date(2026, 10, 12), "open": 1.5, "bar_start": STAMP,
        "journal_generation": None, "payload_sha256": uuid4().hex + uuid4().hex,
    }
    row.update(changes)
    return row


def test_repeated_values_become_one_shared_instance_and_unique_values_stay_distinct():
    rows = share_bar_values([_row() for _ in range(3)])
    for column in ("cohort_id", "provider_tier", "security_id", "ticker", "interval"):
        assert all(row[column] is rows[0][column] for row in rows), column
    assert len({id(row["bar_revision_id"]) for row in rows}) == 3
    assert len({id(row["payload_sha256"]) for row in rows}) == 3
    assert [row["ticker"] for row in rows] == ["AAPL"] * 3 and rows[0]["cohort_id"] == COHORT


def test_sharing_never_adds_a_column_or_replaces_a_missing_value_or_changes_equality():
    plain = [_row("MSFT") for _ in range(2)]
    before = [dict(row) for row in plain]
    shared = share_bar_values(plain)
    assert shared == before and [list(row) for row in shared] == [list(row) for row in before]
    assert all(row["journal_generation"] is None for row in shared)
    narrow = share_bar_values([{"bar_revision_id": uuid4(), "ticker": _copy("MSFT")}])
    assert set(narrow[0]) == {"bar_revision_id", "ticker"}


def test_a_pool_that_outgrows_its_limit_starts_over_without_touching_retained_rows(monkeypatch):
    monkeypatch.setattr(repository, "_SHARED_POOL_LIMIT", 2)
    kept = share_bar_values([_row("QQQ")])[0]
    for index in range(4):
        share_bar_values([_row(f"T{index}")])
    again = share_bar_values([_row("QQQ")])[0]
    assert kept["ticker"] == again["ticker"] == "QQQ"
    assert len(repository._shared_bar_pools["ticker"]) <= 3


def test_prices_share_one_decimal_per_distinct_float_and_keep_the_value_and_exponent_of_str():
    prices = {}
    first, second = _price(187.35, prices), _price(187.35, prices)
    assert first is second and first == Decimal("187.35") and str(first) == "187.35"
    assert _price(187.350000001, prices) is not first
    assert str(_price(5.0, prices)) == "5.0" and str(_price(5, prices)) == "5"  # a float and an int never share
    assert str(_price(Decimal("1.50"), prices)) == "1.50" and str(_price("12.340", prices)) == "12.340"


def test_zero_keeps_its_sign_and_is_never_pooled():
    prices = {}
    assert str(_price(0.0, prices)) == "0.0" and str(_price(-0.0, prices)) == "-0.0"
    assert prices == {}


def test_each_tickers_facts_get_their_own_price_pool(monkeypatch):
    seen = []
    monkeypatch.setattr(projection, "_fact", lambda row, inputs, prices=None: seen.append(prices) or object())
    inputs = SimpleNamespace(provider_tier=SimpleNamespace(value="MASSIVE_DEVELOPER"))
    state = TickerProjectionState()
    with state.cycle():
        state.facts([{"bar_revision_id": uuid4(), "payload_sha256": "a" * 64}], inputs)
        state.facts([{"bar_revision_id": uuid4(), "payload_sha256": "b" * 64}], inputs)
    assert len(seen) == 2 and seen[0] is not seen[1]
