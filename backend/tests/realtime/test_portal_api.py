from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from realtime.contracts import CANARY_UNIVERSE, EQUITY_VALIDATION_UNIVERSE
from realtime.database_access import (
    RealtimeDatabaseRole,
    RealtimePoolBudget,
    RealtimeProcessDatabasePlan,
)
from realtime.portal_api import (
    RealtimePortalRepositories,
    build_realtime_portal_repositories,
    create_realtime_portal_app,
)
from realtime.portal_compatibility_repository import (
    RealtimeOptionAlertRepository,
    RealtimeStockAlertRepository,
    RealtimeTickerScreenerRepository,
)
from realtime.worker_database import RealtimeProcessDatabase


NOW = datetime(2026, 10, 5, 15, tzinfo=timezone.utc)


def _repositories(*, missing_ticker=None, equity_validation=False):
    ticker_screener = MagicMock()

    def ticker_page(ticker, *, available_by, cohort_id=None):
        assert available_by == NOW
        if ticker == missing_ticker:
            return None
        return {
            "payload": {"ticker": ticker, "setups": []},
            "provider_tier": "MASSIVE_DEVELOPER",
            "is_fresh": True,
            "is_serveable": True,
            "provider_fetch_performed": False,
        }

    ticker_screener.ticker_page.side_effect = ticker_page
    tickers = (
        EQUITY_VALIDATION_UNIVERSE.equity_tickers
        if equity_validation
        else CANARY_UNIVERSE.equity_tickers
    )
    ticker_screener.build_latest.return_value = {
        "available": True,
        "reason": None,
        "rows": [
            {"ticker": ticker, "eligible": True}
            for ticker in tickers
        ],
        "provider_tier": "MASSIVE_DEVELOPER",
        "provider_fetch_performed": False,
    }
    stock_alerts = MagicMock()
    stock_alerts.alert_view.return_value = {
        "available": True,
        "alerts": [],
        "source": "REALTIME",
        "provider_tier": "MASSIVE_DEVELOPER",
        "provider_fetch_performed": False,
    }
    option_alerts = MagicMock()
    option_alerts.option_alert_evaluations.return_value = {
        "available": True,
        "data_tier": "MASSIVE_DEVELOPER",
        "data": {
            "evaluations": [],
            "provider_fetch_performed": False,
        },
    }
    return RealtimePortalRepositories(
        ticker_screener,
        stock_alerts,
        option_alerts,
    )


def _app(*, missing_ticker=None):
    return create_realtime_portal_app(
        _repositories(missing_ticker=missing_ticker),
        clock=lambda: NOW,
    )


def _endpoint(app, path):
    return next(route.endpoint for route in app.routes if route.path == path)


def _database(role):
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("stocks_realtime_v1",),
        ("advanced_realtime_v1", "stocks_realtime_v1"),
    )
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = connection
    plan = RealtimeProcessDatabasePlan(
        "portal-api-canary",
        role,
        RealtimePoolBudget(1, 2),
    )
    database = RealtimeProcessDatabase.wire(
        plan,
        principal_pool=pool,
        role_name=f"stocks_realtime_{role.value.lower()}",
    )
    cursor.reset_mock()
    connection.reset_mock()
    pool.reset_mock()
    cursor.fetchone.side_effect = None
    connection.closed = False
    return database, cursor


@pytest.mark.parametrize("timestamp,enabled", [
    ("2026-10-09T13:29:59+00:00", False),
    ("2026-10-09T13:30:00+00:00", True),
    ("2026-10-09T20:00:00+00:00", False),
    ("2026-10-11T15:00:00+00:00", False),
    ("2026-11-26T15:00:00+00:00", False),
    ("2026-11-27T18:00:00+00:00", False),
])
def test_notification_window_is_calendar_only_without_repositories(timestamp, enabled):
    from fastapi.testclient import TestClient
    from equity.calendar import market_notification_window
    now = datetime.fromisoformat(timestamp)
    app = create_realtime_portal_app(RealtimePortalRepositories(None, None, None), clock=lambda: now)
    with TestClient(app) as client:
        response = client.get("/api/equity/notification-window")
    assert response.status_code == 200
    assert response.json() == market_notification_window(now)
    assert response.json()["notifications_enabled"] is enabled


def test_dedicated_canary_endpoint_requires_exact_eight_ticker_coverage():
    payload = _endpoint(_app(), "/api/realtime/canary")()

    assert payload["status"] == "READY"
    assert set(payload["ready_tickers"]) == set(CANARY_UNIVERSE.equity_tickers)
    assert payload["missing_tickers"] == []
    assert payload["database_target"] == "stocks_realtime_v1"
    assert payload["provider_fetch_performed"] is False
    assert payload["canonical_fallback"] is False


def test_canary_endpoint_blocks_partial_ticker_coverage():
    payload = _endpoint(
        _app(missing_ticker="AAPL"),
        "/api/realtime/canary",
    )()

    assert payload["status"] == "BLOCKED"
    assert payload["missing_tickers"] == ["AAPL"]
    assert "TICKER_PAGE_COVERAGE_INCOMPLETE" in payload["reason_codes"]


def test_equity_validation_endpoint_requires_nine_tickers_without_options():
    app = create_realtime_portal_app(
        _repositories(equity_validation=True),
        clock=lambda: NOW,
    )

    payload = _endpoint(app, "/api/realtime/equity-validation")()

    assert payload["status"] == "READY"
    assert set(payload["ready_tickers"]) == set(
        EQUITY_VALIDATION_UNIVERSE.equity_tickers
    )
    assert payload["option_alerts_available"] is False
    assert payload["option_alerts_status"] == "NOT_APPLICABLE_EQUITY_VALIDATION"


def test_dedicated_routes_reject_unreviewed_ticker_and_expose_no_fallback():
    app = _app()

    nflx = _endpoint(app, "/api/realtime/tickers/{ticker}")("NFLX")
    assert nflx["payload"]["ticker"] == "NFLX"
    with pytest.raises(HTTPException) as error:
        _endpoint(app, "/api/realtime/tickers/{ticker}")("META")
    assert error.value.status_code == 422
    health = _endpoint(app, "/api/realtime/health")()
    assert health == {
        "status": "READY",
        "database_target": "stocks_realtime_v1",
        "canonical_fallback": False,
        "provider_fetch_performed": False,
    }


def test_realtime_portal_module_has_no_canonical_composition_imports():
    source = Path(__file__).parents[2].joinpath(
        "realtime", "portal_api.py"
    ).read_text(encoding="utf-8")

    assert "from database" not in source
    assert "import database" not in source
    assert "from main" not in source
    assert "import main" not in source


def test_standalone_realtime_api_uses_reader_dsn_and_loopback_only():
    source = Path(__file__).parents[2].joinpath(
        "scripts", "run_realtime_portal_api.py"
    ).read_text(encoding="utf-8")

    assert 'os.getenv("REALTIME_API_DSN"' in source
    assert 'default=BACKEND_DIR / ".env"' in source
    assert 'load_dotenv(BACKEND_DIR / ".env", override=False)' in source
    assert 'load_dotenv(environment_file, override=True)' in source
    assert "build_api_reader_database_plan" in source
    assert "ThreadedConnectionPool" in source
    assert 'args.host != "127.0.0.1"' in source
    assert "from main" not in source
    assert "import main" not in source


@pytest.mark.parametrize("environment_name,arguments,expected_port", [
    ("backend/.env", [], 8003),
    (".env", ["--env-file", ".env"], 8003),
    (".env.realtime", ["--env-file", ".env.realtime", "--port", "8004"], 8004),
])
def test_standalone_api_loads_root_environment_before_defaults(monkeypatch, tmp_path, environment_name, arguments, expected_port):
    from scripts import run_realtime_portal_api

    dsn = "postgresql://stocks_realtime_api:fixture@127.0.0.1:5432/stocks_realtime_v1"
    environment_file = tmp_path / environment_name
    environment_file.parent.mkdir(parents=True, exist_ok=True)
    environment_file.write_text(
        f"REALTIME_API_DSN={dsn}\nREALTIME_API_HOST=127.0.0.1\nREALTIME_API_PORT=8003\n",
        encoding="utf-8",
    )
    unrelated_directory = tmp_path / "elsewhere"
    unrelated_directory.mkdir()
    monkeypatch.chdir(unrelated_directory)
    monkeypatch.setattr(run_realtime_portal_api, "BACKEND_DIR", tmp_path / "backend")
    monkeypatch.setattr(run_realtime_portal_api, "WORKSPACE_DIR", tmp_path)
    monkeypatch.setattr("sys.argv", ["run_realtime_portal_api.py", *arguments])
    monkeypatch.setenv("REALTIME_API_DSN", "dbname=canonical user=canonical")
    monkeypatch.setenv("REALTIME_API_HOST", "0.0.0.0")
    monkeypatch.setenv("REALTIME_API_PORT", "9000")
    principal_pool = MagicMock()
    pool_factory = MagicMock(return_value=principal_pool)
    wire = MagicMock(return_value=MagicMock())
    app = object()
    server = MagicMock()
    monkeypatch.setattr(run_realtime_portal_api.pool, "ThreadedConnectionPool", pool_factory)
    monkeypatch.setattr(run_realtime_portal_api.RealtimeProcessDatabase, "wire", wire)
    monkeypatch.setattr(run_realtime_portal_api, "build_realtime_portal_repositories", MagicMock())
    monkeypatch.setattr(run_realtime_portal_api, "create_realtime_portal_app", MagicMock(return_value=app))
    monkeypatch.setattr(run_realtime_portal_api.uvicorn, "run", server)

    assert run_realtime_portal_api.main() == 0
    pool_factory.assert_called_once_with(2, 20, dsn=dsn)
    assert wire.call_args.args[0].role is RealtimeDatabaseRole.READER
    assert wire.call_args.kwargs["role_name"] == "stocks_realtime_reader"
    server.assert_called_once_with(app, host="127.0.0.1", port=expected_port)
    principal_pool.closeall.assert_called_once()


@pytest.mark.parametrize("dsn", [
    "dbname=canonical user=stocks_realtime_api",
    "dbname=stocks_realtime_v1 user=stocks_realtime_runtime",
    "dbname=stocks_realtime_v1 user=stocks_realtime_api options='-c role=stocks_realtime_owner'",
    "unexpected-fixture-secret",
])
def test_standalone_api_rejects_target_or_principal_drift_before_connecting(monkeypatch, tmp_path, dsn):
    from scripts import run_realtime_portal_api

    (tmp_path / ".env").write_text(f"REALTIME_API_DSN=\"{dsn}\"\n", encoding="utf-8")
    monkeypatch.setattr(run_realtime_portal_api, "WORKSPACE_DIR", tmp_path)
    monkeypatch.setattr("sys.argv", ["run_realtime_portal_api.py", "--env-file", ".env", "--host", "127.0.0.1", "--port", "8002"])
    pool_factory = MagicMock()
    monkeypatch.setattr(run_realtime_portal_api.pool, "ThreadedConnectionPool", pool_factory)
    with pytest.raises(ValueError, match="realtime API DSN") as error:
        run_realtime_portal_api.main()
    assert dsn not in str(error.value)
    pool_factory.assert_not_called()


def test_standalone_api_does_not_fall_back_from_missing_selected_file(monkeypatch, tmp_path):
    from scripts import run_realtime_portal_api

    monkeypatch.setattr(run_realtime_portal_api, "WORKSPACE_DIR", tmp_path)
    monkeypatch.setattr("sys.argv", ["run_realtime_portal_api.py", "--env-file", ".env.realtime"])
    pool_factory = MagicMock()
    monkeypatch.setattr(run_realtime_portal_api.pool, "ThreadedConnectionPool", pool_factory)
    with pytest.raises(FileNotFoundError, match="environment file is missing"):
        run_realtime_portal_api.main()
    pool_factory.assert_not_called()


def test_standalone_api_never_uses_canonical_database_settings(monkeypatch, tmp_path):
    from scripts import run_realtime_portal_api

    (tmp_path / ".env").write_text("DB_NAME=canonical\nDB_USER=canonical\n", encoding="utf-8")
    monkeypatch.setattr(run_realtime_portal_api, "WORKSPACE_DIR", tmp_path)
    monkeypatch.setattr("sys.argv", ["run_realtime_portal_api.py", "--env-file", ".env", "--host", "127.0.0.1", "--port", "8002"])
    monkeypatch.delenv("REALTIME_API_DSN", raising=False)
    pool_factory = MagicMock()
    monkeypatch.setattr(run_realtime_portal_api.pool, "ThreadedConnectionPool", pool_factory)
    with pytest.raises(RuntimeError, match="requires REALTIME_API_DSN"):
        run_realtime_portal_api.main()
    pool_factory.assert_not_called()


def test_uvicorn_factory_uses_reader_role_and_closes_pool_on_shutdown(monkeypatch):
    import asyncio
    from scripts import run_realtime_portal_api

    principal_pool = MagicMock()
    pool_factory = MagicMock(return_value=principal_pool)
    wire = MagicMock(return_value=MagicMock())
    app = create_realtime_portal_app(_repositories())
    monkeypatch.setenv("REALTIME_API_DSN", "dbname=stocks_realtime_v1 user=stocks_realtime_api")
    monkeypatch.setattr(run_realtime_portal_api.pool, "ThreadedConnectionPool", pool_factory)
    monkeypatch.setattr(run_realtime_portal_api.RealtimeProcessDatabase, "wire", wire)
    monkeypatch.setattr(run_realtime_portal_api, "build_realtime_portal_repositories", MagicMock())
    monkeypatch.setattr(run_realtime_portal_api, "create_realtime_portal_app", MagicMock(return_value=app))

    assert run_realtime_portal_api.create_app() is app
    assert wire.call_args.args[0].role is RealtimeDatabaseRole.READER
    assert wire.call_args.kwargs["role_name"] == "stocks_realtime_reader"
    principal_pool.closeall.assert_not_called()

    async def shutdown():
        async with app.router.lifespan_context(app):
            principal_pool.closeall.assert_not_called()

    asyncio.run(shutdown())
    principal_pool.closeall.assert_called_once()


def test_uvicorn_factory_closes_pool_when_target_verification_fails(monkeypatch):
    from scripts import run_realtime_portal_api

    principal_pool = MagicMock()
    monkeypatch.setenv("REALTIME_API_DSN", "dbname=stocks_realtime_v1 user=stocks_realtime_api")
    monkeypatch.setattr(run_realtime_portal_api.pool, "ThreadedConnectionPool", MagicMock(return_value=principal_pool))
    monkeypatch.setattr(run_realtime_portal_api.RealtimeProcessDatabase, "wire", MagicMock(side_effect=ValueError("target verification failed")))
    with pytest.raises(ValueError, match="target verification failed"):
        run_realtime_portal_api.create_app()
    principal_pool.closeall.assert_called_once()


def test_repository_composition_requires_verified_reader_role():
    reader_database, cursor = _database(RealtimeDatabaseRole.READER)
    repositories = build_realtime_portal_repositories(
        reader_database
    )

    assert isinstance(
        repositories.ticker_screener,
        RealtimeTickerScreenerRepository,
    )
    assert isinstance(repositories.stock_alerts, RealtimeStockAlertRepository)
    assert isinstance(repositories.option_alerts, RealtimeOptionAlertRepository)
    with repositories.ticker_screener._cursor():
        pass
    assert cursor.execute.call_args_list[0].args[0] == (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    )
    compute_database, _ = _database(RealtimeDatabaseRole.COMPUTE)
    with pytest.raises(ValueError, match="requires the reader database role"):
        build_realtime_portal_repositories(
            compute_database
        )