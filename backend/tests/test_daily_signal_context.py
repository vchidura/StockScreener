import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import refresh_daily_signal_context as context


NOW = datetime(2026, 9, 12, 14, tzinfo=timezone.utc)
SESSION = date(2026, 9, 11)


def setup_refresh(monkeypatch, *, status="COMPLETE", existing=False):
    cursor = MagicMock()
    members = [f"TEST{index:03}" for index in range(60)]
    cursor.fetchone.side_effect = [
        {"acquired": True},
        {"publication_id": "publication", "status": status, "expected_members": 60, "selected_members": 60},
        {"newer": False},
    ]
    rows = [{"ticker": ticker} for ticker in members]
    cursor.fetchall.side_effect = [rows, rows,
        [{"ticker": ticker, "universe_size": 60} for ticker in members] if existing else [],
        rows if existing else [],
    ]
    transactions = []

    @contextmanager
    def cursor_context():
        try:
            yield cursor
            transactions.append("commit")
        except Exception:
            transactions.append("rollback")
            raise

    monkeypatch.setattr(context, "get_db_cursor", cursor_context)
    panel = pd.DataFrame({"date": [pd.Timestamp(SESSION)] * 60, "ticker": members})
    cross = panel.assign(universe_size=60)
    states = panel.rename(columns={"date": "trade_date"})
    load = MagicMock(return_value=panel)
    rank_compute = MagicMock(return_value=cross)
    discovery_compute = MagicMock(return_value=states)
    rank_persist = MagicMock(return_value=60)
    discovery_persist = MagicMock(return_value=60)
    monkeypatch.setattr(context, "load_daily_panel", load)
    monkeypatch.setattr(context.ranks, "compute_signal", rank_compute)
    monkeypatch.setattr(context.discovery, "compute", discovery_compute)
    monkeypatch.setattr(context.ranks, "persist", rank_persist)
    monkeypatch.setattr(context.discovery, "persist", discovery_persist)
    return cursor, transactions, load, rank_compute, discovery_compute, rank_persist, discovery_persist


def test_refresh_reuses_panel_and_commits_both_snapshots(monkeypatch):
    cursor, transactions, load, rank_compute, discovery_compute, rank_persist, discovery_persist = setup_refresh(monkeypatch)
    result = context.refresh_current_daily_signals(now=NOW)
    assert result["status"] == "PUBLISHED"
    load.assert_called_once()
    assert load.call_args.kwargs["available_by"] == NOW
    assert rank_compute.call_args.kwargs["panel"] is discovery_compute.call_args.kwargs["panel"]
    assert rank_persist.call_args.kwargs["cursor"] is cursor
    assert discovery_persist.call_args.kwargs == {"cursor": cursor, "retain_history": True}
    assert transactions == ["commit"]


def test_existing_pair_skips_load_compute_and_writes(monkeypatch):
    _, _, load, rank_compute, discovery_compute, rank_persist, discovery_persist = setup_refresh(monkeypatch, existing=True)
    assert context.refresh_current_daily_signals(now=NOW)["status"] == "ALREADY_PRESENT"
    for operation in (load, rank_compute, discovery_compute, rank_persist, discovery_persist):
        operation.assert_not_called()


@pytest.mark.parametrize("status", ["DEGRADED", "FAILED", "PENDING"])
def test_incomplete_publication_does_not_generate(monkeypatch, status):
    _, _, load, _, _, rank_persist, _ = setup_refresh(monkeypatch, status=status)
    assert context.refresh_current_daily_signals(now=NOW)["status"] == "WAITING_FOR_COMPLETE_DAILY_PUBLICATION"
    load.assert_not_called()
    rank_persist.assert_not_called()


def test_dry_run_does_not_persist(monkeypatch):
    _, _, _, _, _, rank_persist, discovery_persist = setup_refresh(monkeypatch)
    assert context.refresh_current_daily_signals(now=NOW, dry_run=True)["status"] == "DRY_RUN"
    rank_persist.assert_not_called()
    discovery_persist.assert_not_called()


def test_discovery_write_failure_rolls_back_rank_write(monkeypatch):
    _, transactions, _, _, _, rank_persist, discovery_persist = setup_refresh(monkeypatch)
    discovery_persist.side_effect = RuntimeError("write failed")
    with pytest.raises(RuntimeError, match="write failed"):
        context.refresh_current_daily_signals(now=NOW)
    rank_persist.assert_called_once()
    assert transactions == ["rollback"]


def test_older_fallback_date_is_rejected_before_writes(monkeypatch):
    _, _, _, _, discovery_compute, rank_persist, _ = setup_refresh(monkeypatch)
    discovery_compute.return_value["trade_date"] = pd.Timestamp("2026-09-10")
    with pytest.raises(ValueError, match="fell back"):
        context.refresh_current_daily_signals(now=NOW)
    rank_persist.assert_not_called()


def test_partial_existing_pair_is_not_rewritten(monkeypatch):
    cursor, _, load, _, _, _, _ = setup_refresh(monkeypatch, existing=True)
    original = list(cursor.fetchall.side_effect)
    original[-1] = []
    cursor.fetchall.side_effect = original
    with pytest.raises(ValueError, match="partial daily signal snapshots"):
        context.refresh_current_daily_signals(now=NOW)
    load.assert_not_called()


def test_refresher_rejects_historical_backfill(monkeypatch):
    cursor, _, load, _, _, _, _ = setup_refresh(monkeypatch)
    responses = list(cursor.fetchone.side_effect)
    responses[-1] = {"newer": True}
    cursor.fetchone.side_effect = responses
    with pytest.raises(ValueError, match="cannot backfill historical"):
        context.refresh_current_daily_signals(now=NOW)
    load.assert_not_called()


def test_missing_visible_current_bar_blocks_generation(monkeypatch):
    _, _, load, rank_compute, _, _, _ = setup_refresh(monkeypatch)
    load.return_value = load.return_value.iloc[:-1]
    with pytest.raises(ValueError, match="missing visible input bars"):
        context.refresh_current_daily_signals(now=NOW)
    rank_compute.assert_not_called()


def test_output_coverage_cannot_be_smaller_than_the_declared_floor(monkeypatch):
    _, _, _, rank_compute, _, rank_persist, _ = setup_refresh(monkeypatch)
    rank_compute.return_value = rank_compute.return_value.iloc[:49]
    with pytest.raises(ValueError, match="universe coverage"):
        context.refresh_current_daily_signals(now=NOW)
    rank_persist.assert_not_called()


def test_shared_panel_preserves_each_generators_existing_computation(monkeypatch):
    from research import features
    from scripts import generate_cross_sectional_signal, generate_market_discovery
    import numpy as np

    random = np.random.default_rng(7)
    dates = pd.bdate_range(end=SESSION, periods=380)
    panels = []
    for index in range(60):
        closes = 100 * np.exp(np.cumsum(random.normal(0.0003, 0.012, len(dates))))
        panels.append(pd.DataFrame({
            "ticker": f"TEST{index:03}", "date": dates, "close": closes,
            "open": closes * 0.999, "high": closes * 1.01, "low": closes * 0.99,
            "volume": random.integers(100000, 500000, len(dates)),
        }))
    panel = pd.concat(panels, ignore_index=True)
    monkeypatch.setattr(features, "load_daily_panel", lambda *args: panel)
    monkeypatch.setattr(features, "load_sector_map", lambda: {})
    from research import discovery_states
    monkeypatch.setattr(discovery_states, "load_sector_map", lambda: {})

    ordinary_ranks = generate_cross_sectional_signal.compute_signal(SESSION.isoformat())
    shared_ranks = generate_cross_sectional_signal.compute_signal(SESSION.isoformat(), panel=panel)
    pd.testing.assert_frame_equal(ordinary_ranks, shared_ranks)
    start = pd.Timestamp(SESSION) - pd.Timedelta(days=generate_market_discovery.CURRENT_LOOKBACK_CALENDAR_DAYS)
    ordinary_states = discovery_states.classify_discovery_states(panel.loc[panel["date"] >= start])
    shared_states = generate_market_discovery.compute(SESSION.isoformat(), panel=panel)
    pd.testing.assert_frame_equal(ordinary_states, shared_states)