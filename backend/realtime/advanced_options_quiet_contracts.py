"""Quiet-contract rule for per-window Options snapshots (Nov 9).

A snapshot used to expire five minutes after the OLDEST quote among its contracts, so one quiet contract (a same-day
contract far from the money whose quote had not changed for five minutes) made every attempt stale and skipped the
whole Options cycle: 15 cycles on Nov 9, 45 of 103 snapshots. Replayed against the rule below, all 103 pass, with at
most 4 of 72 contracts quiet and at least 90 s of validity left.

The rule, shared by the scheduler that captures and every service that reloads the selection:
- a contract is quiet when its quote expires within QUIET_CONTRACT_MARGIN of the review;
- the snapshot stays valid until the earliest expiry among the other (fresh) contracts, provided no more than a
  quarter of the contracts are quiet and every underlying/side pair keeps at least one fresh contract;
- otherwise the old rule applies (earliest expiry over all contracts), which the scheduler treats as stale.
A selection with no quiet contract therefore validates exactly as before.
"""
from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta

QUIET_CONTRACT_MARGIN = timedelta(seconds=60)


def maximum_quiet_contracts(contract_count: int) -> int:
    return max(1, contract_count // 4)


def _rows(rows: Iterable[tuple[str, str, datetime]], maximum_age: timedelta):
    return tuple((pair_key, side, market_time + maximum_age) for pair_key, side, market_time in rows)


def quiet_tolerant_valid_until(
    rows: Iterable[tuple[str, str, datetime]], *, reviewed_at: datetime, maximum_age: timedelta,
) -> datetime:
    """rows: (underlyer, side, market_data_time) per contract."""
    expiries = _rows(rows, maximum_age)
    if not expiries:
        raise ValueError("quiet-contract rule needs at least one contract")
    earliest = min(expiry for _, _, expiry in expiries)
    horizon = reviewed_at + QUIET_CONTRACT_MARGIN
    if earliest > horizon:
        return earliest
    fresh = tuple(row for row in expiries if row[2] > horizon)
    quiet_count = len(expiries) - len(fresh)
    if (
        not fresh
        or quiet_count > maximum_quiet_contracts(len(expiries))
        or {row[:2] for row in fresh} != {row[:2] for row in expiries}
    ):
        return earliest
    return min(expiry for _, _, expiry in fresh)


def quiet_minority_is_valid(
    rows: Iterable[tuple[str, str, datetime]], *, valid_until: datetime, maximum_age: timedelta,
) -> bool:
    """Reload check: the contracts expiring before valid_until are exactly the quiet ones, and they are a minority
    that leaves every underlying/side pair covered. With none quiet this is the old check."""
    expiries = _rows(rows, maximum_age)
    quiet = tuple(row for row in expiries if row[2] < valid_until)
    if not quiet:
        return True
    fresh = tuple(row for row in expiries if row[2] >= valid_until)
    return (
        bool(fresh)
        and len(quiet) <= maximum_quiet_contracts(len(expiries))
        and {row[:2] for row in fresh} == {row[:2] for row in expiries}
    )


def quiet_contract_count(
    rows: Iterable[tuple[str, str, datetime]], *, valid_until: datetime, maximum_age: timedelta,
) -> int:
    return sum(1 for _, _, expiry in _rows(rows, maximum_age) if expiry < valid_until)
