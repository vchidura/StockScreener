from datetime import datetime, timezone
from uuid import uuid4

from scripts import run_corporate_action_worker


OBSERVED_AT = datetime(2026, 9, 11, 18, 0, tzinfo=timezone.utc)


def test_active_security_ids_cover_portal_and_ranked_universes(monkeypatch):
    monkeypatch.setattr(
        run_corporate_action_worker,
        "get_selected_tickers",
        lambda active_only: ["AAPL", "ARKK"],
    )

    class UniverseRepository:
        def get_latest_as_of(self, context):
            return {"universe_run_id": "run"}

        def member_tickers(self, universe_run_id):
            return frozenset({"MSFT"})

    references = [
        type("Reference", (), {
            "ticker": ticker, "security_id": uuid4(), "active": active,
        })()
        for ticker, active in (("AAPL", True), ("ARKK", True), ("MSFT", False))
    ]
    monkeypatch.setattr(
        run_corporate_action_worker,
        "EquityUniverseRepository",
        UniverseRepository,
    )
    monkeypatch.setattr(
        run_corporate_action_worker.EquityReferenceRepository,
        "list_securities_as_of",
        lambda self, tickers, context: references,
    )

    assert set(run_corporate_action_worker.active_security_ids(OBSERVED_AT)) == {
        "AAPL", "ARKK",
    }


def test_refresh_persists_normalized_live_actions(monkeypatch):
    security_id = uuid4()
    monkeypatch.setattr(
        run_corporate_action_worker,
        "active_security_ids",
        lambda observed_at: {"AAPL": security_id},
    )

    class Client:
        def fetch_splits(self, start, end):
            return ({
                "id": "split-1", "ticker": "AAPL",
                "execution_date": "2026-10-01", "split_from": 1,
                "split_to": 2,
            },)

        def fetch_dividends(self, start, end):
            return ({
                "id": "dividend-1", "ticker": "AAPL",
                "ex_dividend_date": "2026-09-20", "cash_amount": 0.25,
            },)

    class Repository:
        def __init__(self):
            self.actions = ()

        def persist(self, actions):
            self.actions = tuple(actions)
            return len(actions)

        def persist_coverage(self, coverage, actions):
            self.coverage = tuple(coverage)
            return len(coverage)

    repository = Repository()
    result = run_corporate_action_worker.refresh_corporate_actions(
        observed_at=OBSERVED_AT,
        client=Client(),
        repository=repository,
    )

    assert result.universe_size == 1
    assert result.split_rows == 1
    assert result.dividend_rows == 1
    assert result.normalized_actions == 2
    assert result.inserted_actions == 2
    assert result.inserted_coverage == 2
    assert {action.action_type for action in repository.actions} == {
        "SPLIT", "DIVIDEND",
    }
    assert all(action.availability_mode.value == "LIVE_OBSERVED" for action in repository.actions)
    assert {row.action_type for row in repository.coverage} == {
        "SPLIT", "DIVIDEND",
    }
    assert all(
        row.availability_mode.value == "LIVE_OBSERVED"
        and row.replay_available_at is None
        for row in repository.coverage
    )


def test_reconstructed_coverage_is_replay_explicit():
    rows = run_corporate_action_worker.build_coverage(
        ("AAPL",),
        start=OBSERVED_AT.date(),
        end=OBSERVED_AT.date(),
        observed_at=OBSERVED_AT,
        availability_mode=run_corporate_action_worker.BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
    )

    assert all(
        row.availability_mode.value == "HISTORICAL_RECONSTRUCTED"
        and row.replay_available_at == OBSERVED_AT
        for row in rows
    )


def test_native_worker_launcher_includes_corporate_action_refresh():
    source = (
        run_corporate_action_worker.BACKEND_DIR / "scripts" / "start_workers.ps1"
    ).read_text(encoding="utf-8")

    assert 'ScriptName "run_corporate_action_worker.py"' in source