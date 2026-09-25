import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import refresh_equity_portal_snapshots as worker
def test_worker_loads_backend_environment_before_database_import():
    source = Path(worker.__file__).read_text(encoding="utf-8")

    load_position = source.index('load_dotenv(BACKEND_DIR / ".env")')
    database_position = source.index("from database import")
    assert load_position < database_position




def test_snapshot_refresh_does_not_produce_legacy_context(monkeypatch):
    from scripts import refresh_daily_signal_context as legacy
    connection = MagicMock()
    manager = MagicMock()
    manager.__enter__.return_value = connection
    monkeypatch.setattr(worker, "get_db_connection", lambda: manager)
    refresh = MagicMock(side_effect=RuntimeError("legacy unavailable"))
    monkeypatch.setattr(legacy, "refresh_current_daily_signals", refresh)
    manifest = MagicMock(return_value={"source_generation": 1})
    monkeypatch.setattr(worker, "source_manifest", manifest)
    monkeypatch.setattr(worker, "get_selected_tickers", lambda _: ["AAPL"])
    monkeypatch.setattr(worker, "get_tickers_overview", lambda _: [])
    monkeypatch.setattr(worker, "bulk_load_dataframes", lambda *args: {})
    monkeypatch.setattr(worker, "analyze_market_regime", lambda *args: {})
    monkeypatch.setattr(worker, "latest_sector_performance", lambda *args: [])
    monkeypatch.setattr(worker, "sector_intelligence", lambda *args: {})
    monkeypatch.setattr(worker, "compute_default_scanner_snapshots", lambda *args: {})
    monkeypatch.setattr(worker, "_compute_streak_snapshots", lambda: {})
    publish = MagicMock(return_value=["snapshot"])
    monkeypatch.setattr(worker, "publish", publish)
    monkeypatch.setattr(worker, "current", lambda _: dict(is_fresh=True, read_latency_ms=1))
    assert worker.refresh_once()["published"] == ["snapshot"]
    publish.assert_called_once()
    refresh.assert_not_called()
    assert manifest.call_count == 2
    assert "pg_advisory_unlock" in connection.cursor.return_value.execute.call_args.args[0]


def test_one_shot_skips_refresh_when_current_snapshot_is_fresh(monkeypatch):
    monkeypatch.setattr(worker, "current", lambda _: {"is_fresh": True})
    with patch.object(worker, "refresh_once") as refresh:
        with patch.object(sys, "argv", ["worker"]):
            assert worker.main() == 0
    refresh.assert_not_called()


def test_one_shot_refreshes_missing_snapshot(monkeypatch):
    monkeypatch.setattr(worker, "current", lambda _: None)
    monkeypatch.setattr(worker, "refresh_once", lambda: {"status": "published"})
    with patch.object(sys, "argv", ["worker"]):
        assert worker.main() == 0


def test_continuous_worker_retries_source_generation_change(monkeypatch):
    monkeypatch.setattr(worker, "current", lambda _: None)
    refresh = MagicMock(side_effect=[
        worker.SourceGenerationChanged("source changed"),
        KeyboardInterrupt(),
    ])
    monkeypatch.setattr(worker, "refresh_once", refresh)
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)

    with patch.object(sys, "argv", ["worker", "--continuous"]):
        with pytest.raises(KeyboardInterrupt):
            worker.main()

    assert refresh.call_count == 2


def test_one_shot_surfaces_source_generation_change(monkeypatch):
    monkeypatch.setattr(worker, "current", lambda _: None)
    monkeypatch.setattr(
        worker,
        "refresh_once",
        lambda: (_ for _ in ()).throw(worker.SourceGenerationChanged("source changed")),
    )

    with patch.object(sys, "argv", ["worker"]):
        with pytest.raises(worker.SourceGenerationChanged):
            worker.main()