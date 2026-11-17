"""G102 R88 (Nov 16): the ticker page's market context was refused all session (503 'ahead of the provider-qualified
watermark: interval=1d'): the intraday cohort clock was checked against the daily watermark."""
from datetime import datetime, timezone

import pytest

from realtime.ticker_repository import RealtimeTickerWorkspaceRepository, TickerPublicationUnavailable

UTC = timezone.utc


def _payload(quote_source, quote_as_of, cohort_market_time, trade_date="2026-11-16"):
    return {
        "_retained_read_state": None,
        "_chart_source": {"provider_tier": "MASSIVE_ADVANCED", "instrument_id": 1},
        "quote": {"source": quote_source, "as_of": quote_as_of, "trade_date": trade_date},
        "context": {
            "schema": "stock_equity_context_v1", "status": "READY",
            "cohort": {"market_time": cohort_market_time, "status": "READY", "generation": 1},
            "rows": [{"daily": {"status": "READY"}, "frames": {
                "1h": {"status": "READY", "market_time": "2026-11-16T15:30:00+00:00", "trend": "UP", "momentum": 1.0,
                       "close": 10.0}}}],
        },
    }


def _read(monkeypatch, payload, read_at):
    repository = RealtimeTickerWorkspaceRepository(lambda: None)
    monkeypatch.setattr(repository, "_snapshot", lambda *args, **kwargs: payload)
    return repository.read("SPY", "context", available_by=read_at)


def test_an_intraday_context_is_read_with_the_quotes_own_interval(monkeypatch):
    # window E:16:05 (cutoff 16:05, the 5m bar ending 16:05), read at 16:07 on Nov 16
    payload = _payload("5m", "2026-11-16T16:05:00+00:00", "2026-11-16T16:05:00+00:00")
    result = _read(monkeypatch, payload, datetime(2026, 11, 16, 16, 7, 12, tzinfo=UTC))
    assert result["status"] == result["cohort"]["status"] == "READY"
    assert result["rows"][0]["daily"]["status"] == "READY"
    assert result["rows"][0]["frames"]["1h"]["status"] in {"READY", "STALE", "DELAYED"}


def test_a_close_context_still_reads_against_the_daily_watermark(monkeypatch):
    payload = _payload("daily", "2026-11-16T21:00:00+00:00", "2026-11-16T21:00:00+00:00")
    result = _read(monkeypatch, payload, datetime(2026, 11, 16, 23, 0, tzinfo=UTC))
    assert result["status"] == "READY"


def test_a_quote_ahead_of_its_watermark_is_still_refused(monkeypatch):
    # a 5m quote stamped after the read time's latest final 5m bar
    payload = _payload("5m", "2026-11-16T16:10:00+00:00", "2026-11-16T16:10:00+00:00")
    with pytest.raises(TickerPublicationUnavailable, match="interval=5m"):
        _read(monkeypatch, payload, datetime(2026, 11, 16, 16, 7, 12, tzinfo=UTC))
