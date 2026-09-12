from datetime import datetime, timezone
from pathlib import Path

from scripts import run_market_event_worker


BACKEND_DIR = Path(__file__).resolve().parents[1]


def test_native_worker_launcher_includes_active_market_events():
    source = (BACKEND_DIR / "scripts" / "start_workers.ps1").read_text(
        encoding="utf-8"
    )

    assert 'ScriptName "run_market_event_worker.py"' in source


def test_active_company_tickers_covers_portal_and_ranked_universes(monkeypatch):
    monkeypatch.setattr(
        run_market_event_worker,
        "get_selected_tickers",
        lambda active_only: ["AAPL", "ARKK"],
    )

    class UniverseRepository:
        def get_latest_as_of(self, context):
            return {"universe_run_id": "run"}

        def member_tickers(self, universe_run_id):
            return frozenset({"MSFT", "SPY"})

    references = [
        type("Reference", (), {"ticker": "AAPL", "security_type": "CS"})(),
        type("Reference", (), {"ticker": "ARKK", "security_type": "ETF"})(),
        type("Reference", (), {"ticker": "MSFT", "security_type": "CS"})(),
        type("Reference", (), {"ticker": "SPY", "security_type": "ETF"})(),
    ]
    monkeypatch.setattr(
        run_market_event_worker,
        "EquityUniverseRepository",
        UniverseRepository,
    )
    monkeypatch.setattr(
        run_market_event_worker.EquityReferenceRepository,
        "list_securities_as_of",
        lambda self, tickers, context: references,
    )

    assert run_market_event_worker.active_company_tickers(
        datetime(2026, 9, 11, tzinfo=timezone.utc)
    ) == ("AAPL", "MSFT")