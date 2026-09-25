import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from equity.stock_discovery import filter_stocks


def row(ticker, momentum, eligible=True, rank=None):
    return dict(ticker=ticker, momentum=momentum, eligible=eligible, exclusion="" if eligible else "LOW_LIQUIDITY",
                price=10, dollar_volume=30_000_000, relative_volume=1., change=.01, volatility=.2,
                sector="Technology", company_name=ticker, trend="UP", state="TRENDING", break_side=0,
                rank=rank, side="NEUTRAL")


def test_filters_preserve_full_universe_rank_and_unavailable_rows():
    rows = [row("FIRST", .8, rank=1), row("SECOND", .2, rank=2), row("BAD", 9., False), row("LAST", -.5, rank=3)]
    result = filter_stocks(rows, search="SECOND")
    assert result["rows"][0]["rank"] == 2
    assert result["eligible_count"] == 3 and result["universe_count"] == 4
    assert next(item for item in rows if item["ticker"] == "BAD")["rank"] is None
    assert filter_stocks(rows, sort="WEAKEST")["rows"][0]["ticker"] == "LAST"


def test_stock_features_require_contiguous_same_security_history():
    import pandas as pd
    import numpy as np
    from equity.stock_discovery import stock_features
    from datetime import date
    frame = pd.DataFrame(dict(session_date=pd.bdate_range("2025-01-01", periods=253).date,
                              ordinal=range(253), security_id=["one"] * 253, open=np.arange(253) + 100.,
                              high=np.arange(253) + 102., low=np.arange(253) + 99., close=np.arange(253) + 101.,
                              volume=[1e6] * 253, raw_close=np.arange(253) + 101., raw_volume=[1e6] * 253))
    session = frame.session_date.iloc[-1]
    result = stock_features(frame, session, "one")
    assert result["eligible"] and abs(result["momentum"] - (332 / 101 - 1)) < 1e-12
    assert (result["price"], result["trend"], result["state"], result["break_side"]) == (353., "UP", "TRENDING", 0)
    assert result["dollar_volume"] == 343_500_000. and result["relative_volume"] == 1.
    assert abs(result["change"] - (353 / 352 - 1)) < 1e-12
    assert stock_features(frame.iloc[:-1], session, "one")["exclusion"] == "MISSING_LATEST_SESSION"
    frame.loc[100, "security_id"] = "other"
    assert stock_features(frame, session, "one")["exclusion"] == "IDENTITY_CHANGED"
    frame.loc[100, "security_id"] = "one"
    frame.loc[100, "ordinal"] = 102
    assert stock_features(frame, session, "one")["exclusion"] == "MISSING_HISTORY_SESSION"


def test_screener_route_is_read_only_and_preserves_rank(monkeypatch):
    from contextlib import contextmanager
    from datetime import datetime, timezone
    from unittest.mock import MagicMock
    from fastapi import FastAPI
    from equity import stock_discovery_api as api

    cursor = MagicMock()
    @contextmanager
    def context():
        yield cursor
    monkeypatch.setattr(api, "get_db_cursor", context)
    rows = [row("FIRST", .9, rank=1), row("SECOND", .3, rank=2)]
    monkeypatch.setattr(api, "load_snapshot", lambda *args: ({"market_time": datetime(2026, 9, 11, 20, tzinfo=timezone.utc),
                                                            "observed_at": datetime(2026, 9, 11, 21, tzinfo=timezone.utc)}, rows))
    app = FastAPI()
    app.include_router(api.router)
    response = api.screener(search="SECOND", minimum_price=0, minimum_dollar_volume=0,
                            minimum_relative_volume=0, offset=0, limit=100)
    assert response["rows"][0]["rank"] == 2
    assert cursor.execute.call_args.args[0] == "SET TRANSACTION READ ONLY"
    parameters = app.openapi()["paths"]["/api/stocks/screener"]["get"]["parameters"]
    assert next(parameter for parameter in parameters if parameter["name"] == "limit")["schema"]["maximum"] == 200


def test_snapshot_reader_preserves_stored_payloads_without_writes():
    from datetime import datetime, timezone
    from unittest.mock import MagicMock
    from equity.stock_discovery_service import load_snapshot

    cursor = MagicMock()
    cursor.fetchone.return_value = {"market_time": None}
    assert load_snapshot(cursor) == (None, [])
    stamp = datetime(2026, 9, 11, 20, tzinfo=timezone.utc)
    payload = row("STORED", .5, rank=7)
    cursor.fetchone.return_value = {"market_time": stamp}
    cursor.fetchall.return_value = [{"payload": payload, "observed_at": stamp}]
    assert load_snapshot(cursor, stamp.date()) == ({"market_time": stamp, "observed_at": stamp}, [payload])
    assert all(call.args[0].lstrip().startswith("SELECT") for call in cursor.execute.call_args_list)