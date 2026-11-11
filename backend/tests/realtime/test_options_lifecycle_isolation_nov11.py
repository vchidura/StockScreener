"""Nov 11 (after the Nov 10 session): Options lifecycle and window isolation (G102 R16, R19, R31)."""
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from realtime.advanced_options_automatic_preparation import _isolated_activity_candidates
from realtime.advanced_options_p8_window import _baseline_incomplete_instruments, _threshold_evidence
from realtime.option_flow.volume import BaselineKind
from scripts import run_advanced_options_v3_lifecycle as lifecycle_script


def test_one_refused_activity_candidate_is_left_out_not_the_allocation(caplog):
    rows = [SimpleNamespace(contract_ticker=ticker) for ticker in ("O:AMZN1", "O:AMZN2", "O:NFLX1")]
    snapshots = {row.contract_ticker: {"ticker": row.contract_ticker} for row in rows}

    def build(row, raw, *, session_date, observed_at, flow_policy):
        if row.contract_ticker == "O:AMZN2":
            raise ValueError("activity/momentum candidate is invalid")
        return (row.contract_ticker, raw["ticker"], session_date)

    with caplog.at_level("WARNING"):
        kept = _isolated_activity_candidates(
            rows, snapshots, session_date=date(2026, 11, 11), observed_at=None, flow_policy=None, build=build,
        )
    assert [row[0] for row in kept] == ["O:AMZN1", "O:NFLX1"]
    assert "rejected=1" in caplog.text and "O:AMZN2" in caplog.text
    # Nothing refused: the same candidates, in order, and no warning.
    caplog.clear()
    with caplog.at_level("WARNING"):
        assert len(_isolated_activity_candidates(
            rows[:1], snapshots, session_date=date(2026, 11, 11), observed_at=None, flow_policy=None, build=build,
        )) == 1
    assert caplog.text == ""


class _Conflict(Exception):
    def __init__(self, pgcode):
        super().__init__(pgcode)
        self.pgcode = pgcode


class _Provider:
    def __init__(self, failures):
        self.failures = list(failures)
        self.calls = 0

    @contextmanager
    def transaction(self, role, isolation_level=None):
        self.calls += 1
        assert isolation_level == "SERIALIZABLE"
        if self.failures:
            raise self.failures.pop(0)
        yield SimpleNamespace(cursor="cursor")


def test_reconcile_retries_serialization_conflicts_in_process(monkeypatch):
    monkeypatch.setattr(lifecycle_script, "reconcile_advanced_options_lifecycle_cursor",
                        lambda cursor, *, target_session_date, checked_at: {"cursor": cursor, "session": target_session_date})
    waits = []
    provider = _Provider([_Conflict("40001"), _Conflict("40P01")])
    status = lifecycle_script._reconcile_with_retry(provider, target_session_date=date(2026, 11, 11), wait=waits.append)
    assert status == {"cursor": "cursor", "session": date(2026, 11, 11)}
    assert provider.calls == 3 and waits == [0.1, 0.2]


def test_reconcile_raises_other_errors_and_the_last_conflict(monkeypatch):
    monkeypatch.setattr(lifecycle_script, "reconcile_advanced_options_lifecycle_cursor",
                        lambda cursor, **_: {"ok": True})
    with pytest.raises(_Conflict):
        lifecycle_script._reconcile_with_retry(_Provider([_Conflict("23505")]), target_session_date=None,
                                               wait=lambda _: None)
    provider = _Provider([_Conflict("40001")] * lifecycle_script.RECONCILE_CONFLICT_ATTEMPTS)
    with pytest.raises(_Conflict):
        lifecycle_script._reconcile_with_retry(provider, target_session_date=None, wait=lambda _: None)
    assert provider.calls == lifecycle_script.RECONCILE_CONFLICT_ATTEMPTS


def _volume(ticker, kind):
    return SimpleNamespace(checkpoint=SimpleNamespace(contract_ticker=ticker), completeness=kind)


def test_packages_on_an_incomplete_volume_baseline_are_not_packageable():
    """R31: after the Nov 10 18:25-18:33 stream gap 64 of 72 baselines were INCOMPLETE and E:18:30 FAILED."""
    rolling = SimpleNamespace(contract_volumes=(
        _volume("O:SPY1", BaselineKind.INCOMPLETE),
        _volume("O:SPY2", BaselineKind.SNAPSHOT_RECONCILED),
        _volume("O:QQQ1", BaselineKind.WARMING),
        _volume("O:QQQ2", BaselineKind.SESSION_COMPLETE),
    ))
    contracts = (
        SimpleNamespace(contract_ticker="O:SPY1", instrument_id=11),
        SimpleNamespace(contract_ticker="O:SPY2", instrument_id=12),
        SimpleNamespace(contract_ticker="O:QQQ1", instrument_id=21),
        SimpleNamespace(contract_ticker="O:QQQ2", instrument_id=22),
    )
    # Only marked contracts are added (unmarked ones are already left out).
    assert _baseline_incomplete_instruments(rolling, contracts, {11, 12, 22}) == (11,)
    assert _baseline_incomplete_instruments(rolling, contracts, {11, 12, 21, 22}) == (11, 21)


def test_threshold_passes_only_on_a_complete_baseline():
    observed = SimpleNamespace(premium=Decimal("250"), side_class="ASK", side_confidence="HIGH",
                               event_time=datetime(2026, 11, 10, 18, 29, tzinfo=timezone.utc))
    assert _threshold_evidence(observed, 40, BaselineKind.SNAPSHOT_RECONCILED).passed
    assert not _threshold_evidence(observed, 40, BaselineKind.INCOMPLETE).passed
    assert not _threshold_evidence(None, 40, BaselineKind.SESSION_COMPLETE).passed
