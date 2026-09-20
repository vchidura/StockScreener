from datetime import datetime, timezone
from dataclasses import replace
from uuid import uuid4
import pytest
from equity.polygon import sha256_json

from scripts import run_corporate_action_worker
from scripts import backfill_corporate_actions


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

        def persist_observation(self, coverage, actions):
            return self.persist(actions), self.persist_coverage(coverage, actions)

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
    assert all(row.response_action_count == 1 and row.security_id == security_id for row in repository.coverage)
    assert all(row.source_key.endswith(":response-v2") for row in repository.coverage)


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
    assert all(row.response_action_count is None for row in rows)


def test_response_bound_coverage_distinguishes_empty_and_changed_response():
    identity = uuid4()
    arguments = dict(start=OBSERVED_AT.date(), end=OBSERVED_AT.date(), observed_at=OBSERVED_AT)
    empty = run_corporate_action_worker.build_coverage({"AAPL": identity}, responses={"SPLIT": [], "DIVIDEND": []}, **arguments)
    assert all(row.response_action_count == 0 and row.response_sha256 == sha256_json([]) for row in empty)
    changed = run_corporate_action_worker.build_coverage(
        {"AAPL": identity}, responses={"SPLIT": [], "DIVIDEND": [{"ticker": "AAPL", "id": "raw"}]}, **arguments,
    )
    assert changed[1].coverage_id != empty[1].coverage_id
    with pytest.raises(ValueError, match="requires security, count and checksum"):
        replace(empty[0], security_id=None)
    with pytest.raises(ValueError, match="response count"):
        replace(empty[0], response_action_count=True)


def test_native_worker_launcher_includes_corporate_action_refresh():
    source = (
        run_corporate_action_worker.BACKEND_DIR / "scripts" / "start_workers.ps1"
    ).read_text(encoding="utf-8")

    assert 'ScriptName "run_corporate_action_worker.py"' in source


def test_targeted_backfill_persists_response_bound_split_observation_atomically(monkeypatch):
    identities = {ticker: uuid4() for ticker in ("AAPL", "IWM")}
    universe = type("Universe", (), {
        "get_latest_as_of": lambda self, context: {"universe_run_id": "run"},
        "member_tickers": lambda self, run_id: frozenset(identities),
    })()
    references = [type("Reference", (), {"ticker": ticker, "security_id": identity})()
                  for ticker, identity in identities.items()]
    monkeypatch.setattr(backfill_corporate_actions, "EquityUniverseRepository", lambda: universe)
    monkeypatch.setattr(backfill_corporate_actions.EquityReferenceRepository, "list_securities_as_of",
                        lambda self, tickers, context: tuple(references))
    client = type("Client", (), {
        "fetch_splits": lambda self, start, end: ({
            "id": "split", "ticker": "AAPL", "execution_date": "2026-08-10",
            "split_from": 1, "split_to": 2,
        },),
        "fetch_dividends": lambda self, start, end: pytest.fail("dividends not requested"),
    })()
    monkeypatch.setattr(backfill_corporate_actions, "PolygonEquityClient", lambda: client)
    repository = type("Repository", (), {})()
    repository.calls = []
    repository.persist_observation = lambda coverage, actions: (
        repository.calls.append((tuple(coverage), tuple(actions))) or (len(actions), len(coverage))
    )
    monkeypatch.setattr(backfill_corporate_actions, "EquityCorporateActionRepository", lambda: repository)
    monkeypatch.setenv("DB_NAME", "stocks")
    arguments = type("Arguments", (), {
        "start": "2026-08-04", "end": "2026-08-14", "dividends": False,
        "ticker": ["AAPL", "IWM"], "apply": True,
        "confirm_database_name": "stocks", "output": None,
    })()
    monkeypatch.setattr(backfill_corporate_actions, "parser",
                        lambda: type("Parser", (), {"parse_args": lambda self: arguments})())

    assert backfill_corporate_actions.main() == 0
    coverage, actions = repository.calls[0]
    assert len(coverage) == 2 and {row.action_type for row in coverage} == {"SPLIT"}
    assert all(row.availability_mode.value == "LIVE_OBSERVED" for row in coverage)
    assert all(row.response_action_count is not None and row.response_sha256 for row in coverage)
    assert len(actions) == 1 and actions[0].ticker == "AAPL"


def test_backfill_apply_requires_database_and_universe_confirmation(monkeypatch):
    monkeypatch.setenv("DB_NAME", "stocks")
    arguments = type("Arguments", (), {
        "start": "2026-08-04", "end": "2026-08-14", "dividends": False,
        "ticker": ["OTHER"], "apply": True,
        "confirm_database_name": "wrong", "output": None,
    })()
    monkeypatch.setattr(backfill_corporate_actions, "parser",
                        lambda: type("Parser", (), {"parse_args": lambda self: arguments})())
    with pytest.raises(SystemExit, match="database-name"):
        backfill_corporate_actions.main()