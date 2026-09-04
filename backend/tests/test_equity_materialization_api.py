import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.api import (
    current_chart_bar_projection,
    current_equity_materialization,
    current_trade_setup_projection,
    expected_materialized_market_time,
    minimum_fresh_materialized_market_time,
    router,
)


def test_materialized_equity_router_exposes_read_only_reporting_surfaces():
    paths = {route.path for route in router.routes}

    assert paths == {
        "/api/equity/health",
        "/api/equity/current",
        "/api/equity/security/{ticker}",
        "/api/equity/context/{ticker}",
        "/api/equity/outcomes",
        "/api/equity/qualifications",
    }
    assert all("GET" in route.methods for route in router.routes)


def test_current_projection_api_has_stable_pagination_metadata():
    cursor = MagicMock()
    cursor.fetchone.return_value = {"count": 854}
    cursor.fetchall.return_value = [{"ticker": "AAPL"}, {"ticker": "MSFT"}]

    @contextmanager
    def get_cursor():
        yield cursor

    with patch("database.get_db_cursor", get_cursor):
        result = current_equity_materialization(
            ticker=None,
            interval="15m",
            projection_type=None,
            source_name=None,
            limit=2,
            offset=2,
        )

    assert result == {
        "count": 2,
        "total": 854,
        "limit": 2,
        "offset": 2,
        "has_more": True,
        "results": [{"ticker": "AAPL"}, {"ticker": "MSFT"}],
    }
    page_sql, page_parameters = cursor.execute.call_args_list[1].args
    assert "source_name" in page_sql
    assert "LIMIT %s OFFSET %s" in page_sql
    assert page_parameters == ["15m", 2, 2]


def test_current_trade_setup_projection_returns_exact_published_row():
    cursor = MagicMock()
    market_time = datetime(2026, 8, 28, 20, 0, tzinfo=timezone.utc)
    expected = {
        "payload": {"ticker": "AAPL", "interval": "30m"},
        "evidence_id": "evidence-id",
        "analysis_run_id": "run-id",
        "market_time": market_time,
        "observed_at": datetime(2026, 8, 28, 20, 1, tzinfo=timezone.utc),
        "published_at": datetime(2026, 8, 28, 20, 2, tzinfo=timezone.utc),
    }
    cursor.fetchone.return_value = expected

    @contextmanager
    def get_cursor():
        yield cursor

    with patch("database.get_db_cursor", get_cursor):
        result = current_trade_setup_projection(
            "aapl", "30m",
            now=datetime(2026, 8, 30, 20, 0, tzinfo=timezone.utc),
        )

    assert {key: result[key] for key in expected} == expected
    assert result["expected_market_time"] == market_time
    assert result["is_fresh"] is True
    assert result["staleness_seconds"] == 0
    assert result["read_latency_ms"] >= 0
    query, parameters = cursor.execute.call_args.args
    assert "projection_type = 'TRADE_SETUP'" in query
    assert "source_name = 'EQUITY_SETUP'" in query
    assert parameters == ("AAPL", "30m")


def test_current_trade_setup_projection_rejects_wrong_future_watermark():
    cursor = MagicMock()
    cursor.fetchone.return_value = {
        "payload": {},
        "evidence_id": "evidence-id",
        "analysis_run_id": "run-id",
        "market_time": datetime(2026, 8, 31, 14, 30, tzinfo=timezone.utc),
        "observed_at": datetime(2026, 8, 31, 14, 30, tzinfo=timezone.utc),
        "published_at": datetime(2026, 8, 31, 14, 30, tzinfo=timezone.utc),
    }

    @contextmanager
    def get_cursor():
        yield cursor

    with patch("database.get_db_cursor", get_cursor):
        result = current_trade_setup_projection(
            "AAPL", "30m",
            now=datetime(2026, 8, 31, 14, 0, tzinfo=timezone.utc),
            provider_delay_minutes=0,
        )

    assert result["expected_market_time"] == datetime(
        2026, 8, 31, 14, 0, tzinfo=timezone.utc
    )
    assert result["staleness_seconds"] == 0
    assert result["is_fresh"] is False


def test_expected_materialized_market_time_respects_provider_delay():
    result = expected_materialized_market_time(
        datetime(2026, 8, 31, 14, 2, tzinfo=timezone.utc),
        "5m",
        provider_delay_minutes=15,
    )

    assert result == datetime(2026, 8, 31, 13, 45, tzinfo=timezone.utc)


def test_minimum_fresh_market_time_includes_publication_grace():
    result = minimum_fresh_materialized_market_time(
        datetime(2026, 8, 31, 14, 17, tzinfo=timezone.utc),
        "5m",
        provider_delay_minutes=15,
        publication_grace_seconds=600,
    )

    assert result == datetime(2026, 8, 31, 13, 50, tzinfo=timezone.utc)


def test_current_chart_bars_follow_exact_feature_source_revision_order():
    cursor = MagicMock()
    market_time = datetime(2026, 8, 28, 20, 0, tzinfo=timezone.utc)
    cursor.fetchall.return_value = [
        {
            "evidence_id": "evidence-id",
            "analysis_run_id": "run-id",
            "market_time": market_time,
            "observed_at": market_time,
            "published_at": market_time,
            "ordinal": 399,
            "bar_revision_id": "bar-1",
            "bar_start": datetime(2026, 8, 28, 19, 30, tzinfo=timezone.utc),
            "bar_end": market_time,
            "open_price": 100,
            "high_price": 102,
            "low_price": 99,
            "close_price": 101,
            "volume": 1000,
        },
    ]

    @contextmanager
    def get_cursor():
        yield cursor

    with patch("database.get_db_cursor", get_cursor):
        result = current_chart_bar_projection(
            "aapl", "30m", limit=10,
            now=datetime(2026, 8, 30, 20, 0, tzinfo=timezone.utc),
        )

    assert result["is_fresh"] is True
    assert result["bars"][0]["bar_revision_id"] == "bar-1"
    query, parameters = cursor.execute.call_args.args
    assert "unnest(feature.source_revision_ids)" in query
    assert "ORDER BY source.ordinal" in query
    assert parameters == ("AAPL", "30m", 10)