"""G75 breadth: chain snapshot paging, parsing, storage and per-underlying failure isolation (no provider calls)."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime import advanced_options_breadth as breadth
from realtime import advanced_options_continuous_scheduler as scheduler

UTC = timezone.utc
SESSION = date(2026, 11, 9)
CUTOFF = datetime(2026, 11, 9, 14, 40, tzinfo=UTC)
NOW = datetime(2026, 11, 9, 14, 41, tzinfo=UTC)


def _contract(ticker, kind="call", strike=735.0, expiration="2026-11-20", *, volume=120, oi=40, trade=True):
    """The shape measured on Friday's SPY chain (Nov 7): `day` and `last_trade` are empty for untraded contracts."""
    return {
        "break_even_price": strike + 1.0,
        "day": {"volume": volume, "close": 1.1, "open": 1.0} if volume is not None else {},
        "details": {"contract_type": kind, "exercise_style": "american", "expiration_date": expiration,
                    "shares_per_contract": 100, "strike_price": strike, "ticker": ticker},
        "greeks": {"delta": 0.51, "gamma": 0.02, "theta": -0.3, "vega": 0.4},
        "implied_volatility": 0.18,
        "last_quote": {"ask": 1.2, "bid": 1.0, "midpoint": 1.1, "last_updated": 1_762_700_000_000_000_000},
        "last_trade": {"price": 1.15, "size": 3, "sip_timestamp": 1_762_699_000_000_000_000} if trade else {},
        "open_interest": oi,
        "underlying_asset": {"price": 735.44, "ticker": "SPY"},
    }


class _Response:
    def __init__(self, document, status=200):
        self.status_code = status
        self.body = json.dumps(document).encode()


class _Transport:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        return self.pages.pop(0)


def _fetch(transport, *, clock=lambda: NOW, maximum_pages=40, deadline=NOW + timedelta(minutes=2)):
    tokens, finished = [], []
    result = breadth.fetch_underlying_breadth(
        transport, underlying="SPY", spot=Decimal("735.44"), session_date=SESSION, deadline=deadline, clock=clock,
        before_request=lambda page, url: tokens.append((page, url)) or len(tokens),
        after_request=lambda token, **kwargs: finished.append((token, kwargs)), maximum_pages=maximum_pages,
    )
    return result, tokens, finished


def test_a_page_parses_traded_and_untraded_contracts_and_skips_unusable_rows():
    rows, skipped = breadth.parse_breadth_page({"results": [
        _contract("O:SPY251120C00735000"),
        _contract("O:SPY251120P00700000", kind="put", strike=700.0, volume=None, trade=False),
        {"details": {"ticker": "SPY", "contract_type": "call"}},
        {"details": {"ticker": "O:SPY1", "contract_type": "straddle", "strike_price": 1, "expiration_date": "2026-11-20"}},
    ]}, underlying="SPY")
    assert skipped == 2 and [row.contract_ticker for row in rows] == ["O:SPY251120C00735000", "O:SPY251120P00700000"]
    traded, untraded = rows
    assert traded.day_volume == 120 and traded.open_interest == 40 and traded.bid == Decimal("1.0")
    assert traded.expiration_date == date(2026, 11, 20) and traded.strike_price == Decimal("735.0")
    assert traded.last_trade_at.tzinfo is UTC and traded.delta == 0.51 and traded.underlying_price == Decimal("735.44")
    assert untraded.day_volume is None and untraded.last_trade_price is None and untraded.contract_type == "put"
    with pytest.raises(breadth.BreadthPageError):
        breadth.parse_breadth_page({"status": "OK"}, underlying="SPY")


def test_the_query_is_bounded_by_dte_and_moneyness_with_same_day_contracts():
    url, params = breadth.breadth_query("SPY", Decimal("735.44"), SESSION)
    assert url == "https://api.massive.com/v3/snapshot/options/SPY"
    assert params["expiration_date.gte"] == "2026-11-09" and params["expiration_date.lte"] == "2027-01-08"
    assert params["strike_price.gte"] == "588.35" and params["strike_price.lte"] == "882.53"
    assert params["limit"] == "250"


def test_pages_follow_next_url_and_every_request_is_reserved_and_finished():
    next_url = "https://api.massive.com/v3/snapshot/options/SPY?cursor=abc"
    transport = _Transport([
        _Response({"results": [_contract("O:SPY1C")], "next_url": next_url}),
        _Response({"results": [_contract("O:SPY2C"), _contract("O:SPY1C", volume=130)]}),
    ])
    result, tokens, finished = _fetch(transport)
    assert result.state == "COMPLETE" and result.pages == 2 and len(result.rows) == 2
    assert {row.contract_ticker: row.day_volume for row in result.rows} == {"O:SPY1C": 130, "O:SPY2C": 120}
    assert [call[0] for call in transport.calls][1] == next_url and transport.calls[1][1] == {}
    assert [page for page, _url in tokens] == [0, 1]
    assert [kwargs["rows"] for _token, kwargs in finished] == [1, 2]
    assert all(kwargs["error"] is None for _token, kwargs in finished)


def test_an_unapproved_next_url_or_an_http_error_stops_the_underlying():
    transport = _Transport([_Response({"results": [], "next_url": "https://evil.example/v3/snapshot/options/SPY"})])
    with pytest.raises(breadth.BreadthPageError):
        _fetch(transport)
    transport = _Transport([_Response({"error": "x"}, status=429)])
    with pytest.raises(breadth.BreadthPageError):
        _, _, finished = _fetch(transport)


def test_the_page_ceiling_and_the_deadline_end_a_capture_as_partial():
    looping = "https://api.massive.com/v3/snapshot/options/SPY?cursor="
    transport = _Transport([
        _Response({"results": [_contract(f"O:SPY{index}C")], "next_url": f"{looping}{index}"}) for index in range(5)
    ])
    result, _tokens, _finished = _fetch(transport, maximum_pages=3)
    assert (result.state, result.reason, result.pages, len(result.rows)) == ("PARTIAL", "PAGE_CEILING", 3, 3)
    late, _tokens, _finished = _fetch(_Transport([]), deadline=NOW)
    assert (late.state, late.reason, late.pages) == ("PARTIAL", "DEADLINE", 0)


def test_persist_upserts_rows_keeping_the_newer_observation_and_records_the_capture(monkeypatch):
    statements = []
    monkeypatch.setattr(breadth, "execute_values", lambda cursor, sql, rows, **_k: statements.append((sql, rows)))
    cursor = MagicMock()
    rows, _ = breadth.parse_breadth_page({"results": [_contract("O:SPY1C")]}, underlying="SPY")
    captured = breadth.UnderlyingBreadth("SPY", tuple(rows), 1, 0, 1000, "COMPLETE", None)
    breadth.persist_breadth_cursor(cursor, session_date=SESSION, equity_cutoff=CUTOFF, started_at=NOW,
                                   observed_at=NOW, captured=captured)
    (sql, values), = statements
    assert sql.startswith("INSERT INTO rt.option_breadth_current (session_date,contract_ticker,")
    assert "ON CONFLICT (session_date,contract_ticker) DO UPDATE SET underlying=EXCLUDED.underlying" in sql
    assert sql.endswith("WHERE rt.option_breadth_current.observed_at<=EXCLUDED.observed_at")
    assert values[0][:3] == (SESSION, "O:SPY1C", "SPY") and values[0][-2:] == (CUTOFF, NOW)
    capture_sql, params = cursor.execute.call_args.args
    assert capture_sql.startswith("INSERT INTO rt.option_breadth_captures") and params[6:9] == ("COMPLETE", 1, 1)


class _Provider:
    def __init__(self):
        self.cursor = MagicMock()

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield SimpleNamespace(cursor=self.cursor)


def test_one_underlying_failing_never_stops_the_others(monkeypatch):
    provider = _Provider()
    monkeypatch.setattr(breadth, "reserve_coordinated_provider_request_cursor", lambda *_a, **_k: None)
    monkeypatch.setattr(breadth, "finish_provider_request_cursor", lambda *_a, **_k: None)
    monkeypatch.setattr(breadth, "execute_values", lambda *_a, **_k: None)
    transport = _Transport([
        _Response({"error": "bad"}, status=500),                  # AAPL: HTTP error
        _Response({"results": [_contract("O:SPY1C")]}),          # SPY: fine
    ])
    summary = breadth.capture_options_breadth(
        provider, session_date=SESSION, underlyings=("AAPL", "NVDA", "SPY"),
        spots={"AAPL": Decimal("342.45"), "SPY": Decimal("735.44")}, equity_cutoff=CUTOFF, cohort_id=UUID(int=5),
        authorization_reference="AUTH", authorized_until=NOW + timedelta(hours=6), transport=transport,
        deadline=NOW + timedelta(minutes=2), clock=lambda: NOW,
    )
    assert summary["AAPL"]["state"] == "FAILED" and "HTTP 500" in summary["AAPL"]["reason"]
    assert summary["NVDA"] == {"state": "FAILED", "contracts": 0, "pages": 0, "seconds": 0.0,
                               "reason": "SPOT_UNAVAILABLE"}
    assert summary["SPY"]["state"] == "COMPLETE" and summary["SPY"]["contracts"] == 1
    captures = [call.args[1] for call in provider.cursor.execute.call_args_list
                if call.args[0].startswith("INSERT INTO rt.option_breadth_captures")]
    assert [row[2] for row in captures] == ["AAPL", "NVDA", "SPY"]
    assert not any("DELETE" in call.args[0] for call in provider.cursor.execute.call_args_list)


def test_the_scheduler_never_lets_breadth_decide_the_window(monkeypatch):
    acquisition = SimpleNamespace(authority=SimpleNamespace(
        session_date=SESSION, cohort_id=UUID(int=5), acquisition_stop=NOW + timedelta(hours=6)))
    monkeypatch.setattr(scheduler, "_breadth_spots", lambda *_a: (_ for _ in ()).throw(RuntimeError("db down")))
    result = scheduler._capture_breadth(
        _Provider(), package={"cohort": {"underlyings": ["SPY"]}, "authorization_reference": "AUTH"},
        acquisition=acquisition, equity_cutoff=CUTOFF, api_key="k", clock=lambda: NOW,
        transport_factory=lambda *a, **k: _Transport([]),
    )
    assert result == {"status": "FAILED", "reason": "RuntimeError: db down"}
    assert scheduler._capture_breadth(
        _Provider(), package={"cohort": {}}, acquisition=acquisition, equity_cutoff=CUTOFF, api_key="k",
        clock=lambda: NOW, transport_factory=None,
    ) == {"status": "NO_UNDERLYINGS"}


# ---- reads: the Options Screener ---------------------------------------------------------------------------------------

def _stored_breadth(ticker, *, kind="call", strike="760", volume=900, oi=100, bid="1.00", ask="1.20"):
    return {
        "session_date": SESSION, "contract_ticker": ticker, "underlying": "SPY", "contract_type": kind,
        "expiration_date": date(2026, 11, 20), "strike_price": Decimal(strike), "day_volume": volume,
        "open_interest": oi, "last_trade_price": Decimal("1.1"), "last_trade_at": NOW, "bid": Decimal(bid),
        "ask": Decimal(ask), "quote_at": NOW, "implied_volatility": 0.2, "delta": -0.3 if kind == "put" else 0.3,
        "gamma": 0.01, "theta": -0.2, "vega": 0.3, "underlying_price": Decimal("735.44"), "equity_cutoff": CUTOFF,
        "observed_at": NOW, "contract_id": None,
    }


def test_a_breadth_row_has_the_screeners_fields_flagged_as_provider_values():
    row = breadth.breadth_screener_row(_stored_breadth("O:SPY251120C00760000"))
    assert row["contract_type"] == "CALL" and row["calendar_dte"] == 11 and row["strike"] == "760"
    assert row["mark"] == "1.1" and row["volume_open_interest_ratio"] == "9" and row["day_volume"] == 900
    assert row["local_iv"] is None and row["absolute_delta"] is None and row["greeks_reason"] == "NOT_COMPUTED"
    assert (row["provider_iv"], row["provider_delta"], row["provider_gamma"]) == (0.2, 0.3, 0.01)
    assert Decimal(row["otm_fraction"]) > 0 and row["contract_id"] == 0
    assert row["coverage_source"] == "BREADTH" and "LOCAL_GREEKS_UNAVAILABLE:NOT_COMPUTED" in row["quality_flags"]
    put = breadth.breadth_screener_row(_stored_breadth("O:SPY251120P00700000", kind="put", strike="700", oi=0))
    assert put["volume_open_interest_ratio"] is None and Decimal(put["otm_fraction"]) > 0


def test_the_screener_merges_breadth_under_the_published_rows(monkeypatch):
    from options.discovery import ContractRange, EligibleChainQuery
    from realtime import advanced_options_projection as projection

    monkeypatch.setattr(projection, "_validate_page_identity", lambda *_a, **_k: None)
    published = breadth.breadth_screener_row(_stored_breadth("O:SPY251120C00740000", strike="740", volume=50))
    published.update(coverage_source=None, mark_source="QUOTE_MIDPOINT", quality_flags=[])
    publication = projection.AdvancedOptionsScreenerPagePublication({
        "provider_tier": "MASSIVE_ADVANCED", "scope_sha256": "s" * 64, "rows": [published],
        "coverage": [{"underlying": "SPY", "market_time": NOW.isoformat(), "observed_time": NOW.isoformat()}],
        "underlyers": ["SPY"], "filter_values": {}, "published_at": NOW.isoformat(), "policy_sha256": "p" * 64,
    })
    rows = [
        breadth.breadth_screener_row(_stored_breadth("O:SPY251120C00740000", strike="740", volume=5000)),
        breadth.breadth_screener_row(_stored_breadth("O:SPY251120C00760000")),
        breadth.breadth_screener_row(_stored_breadth("O:SPY251120C00800000", strike="800", volume=3)),
    ]
    result = publication.query(
        EligibleChainQuery(filters=(ContractRange(field="day_volume", minimum=10),)),
        cohort_id=UUID(int=1), provider_tier="MASSIVE_ADVANCED", scope_sha256="s" * 64,
        breadth_rows=rows, breadth_coverage={"SPY": {"breadth_state": "COMPLETE", "breadth_contracts": 3}},
    )
    data = result["data"]
    assert [(row["contract_ticker"], row["day_volume"]) for row in data["rows"]] == [
        ("O:SPY251120C00760000", 900), ("O:SPY251120C00740000", 50)]
    assert data["total"] == 2 and data["coverage"][0]["breadth_state"] == "COMPLETE"
    alone = publication.query(EligibleChainQuery(), cohort_id=UUID(int=1), provider_tier="MASSIVE_ADVANCED",
                              scope_sha256="s" * 64)
    assert alone["data"]["total"] == 1 and alone["data"]["coverage"][0]["breadth_state"] == "NOT_CAPTURED"


# ---- our Greeks for breadth rows -------------------------------------------------------------------------------------

def _valuation(expiration, rate="0.0403", dividend="0.012"):
    return {"ticker": "SPY", "expiration_date": expiration, "risk_free_rate": rate, "dividend_yield": dividend,
            "model_version": "black-scholes-merton-v1"}


def test_our_greeks_use_the_exact_or_the_nearest_stored_expiration_and_say_why_when_missing():
    from dataclasses import replace

    parsed, _ = breadth.parse_breadth_page({"results": [
        _contract("O:SPY251120C00735000", expiration="2026-11-20"),
        _contract("O:SPY251127C00735000", expiration="2026-11-27"),
    ]}, underlying="SPY")
    # An at-the-money SPY call 11 and 18 days out trades near $9-11 (the shared fixture's $1.10 has no IV).
    rows = [replace(parsed[0], bid=Decimal("9.00"), ask=Decimal("9.40")),
            replace(parsed[1], bid=Decimal("11.00"), ask=Decimal("11.40"))]
    one_sided = breadth.BreadthRow(**{**{name: getattr(rows[0], name) for name in rows[0].__slots__},
                                      "contract_ticker": "O:SPY251120C00740000", "ask": None})
    local = breadth.breadth_local_greeks(
        (*rows, one_sided), spot=Decimal("735.44"), valuation_inputs=(_valuation("2026-11-20"),), observed_at=NOW,
    )
    exact, nearest, missing = (local[ticker] for ticker in (
        "O:SPY251120C00735000", "O:SPY251127C00735000", "O:SPY251120C00740000"))
    assert exact["valuation_basis"] == "EXACT_EXPIRY" and nearest["valuation_basis"] == "NEAREST_EXPIRY"
    assert exact["local_iv"] and 0 < exact["local_delta"] < 1 and exact["greeks_reason"] is None
    assert exact["risk_free_rate"] == 0.0403 and exact["spot"] == Decimal("735.44")
    assert missing["local_iv"] is None and missing["greeks_reason"] == "QUOTE_UNAVAILABLE"
    none = breadth.breadth_local_greeks(rows, spot=Decimal("735.44"), valuation_inputs=(), observed_at=NOW)
    assert {item["greeks_reason"] for item in none.values()} == {"VALUATION_INPUT_UNAVAILABLE"}
    no_spot = breadth.breadth_local_greeks(rows, spot=None, valuation_inputs=(_valuation("2026-11-20"),),
                                           observed_at=NOW)
    assert {item["greeks_reason"] for item in no_spot.values()} == {"SPOT_UNAVAILABLE"}


def test_a_breadth_row_shows_ours_and_the_providers_side_by_side():
    stored = _stored_breadth("O:SPY251120C00760000")
    stored.update(local_iv=0.19, local_delta=0.31, local_gamma=0.012, local_theta_per_day=-0.2,
                  local_vega_per_vol_point=0.3, local_rho_per_rate_point=0.05, spot=Decimal("735.40"),
                  risk_free_rate=0.0403, dividend_yield=0.012, valuation_basis="NEAREST_EXPIRY", greeks_reason=None)
    row = breadth.breadth_screener_row(stored)
    assert (row["local_iv"], row["absolute_delta"], row["provider_iv"], row["provider_delta"]) == (0.19, 0.31, 0.2, 0.3)
    assert row["spot"] == "735.4" and row["model_version"] == "black-scholes-merton-v1" and row["greeks_reason"] is None
    assert "VALUATION_INPUT_NEAREST_EXPIRY" in row["quality_flags"]


def test_breadth_reads_work_with_the_apis_dict_cursor_and_with_tuple_cursors():
    """Nov 8 rehearsal: the API's dict cursor returned mappings; the read assumed tuples, failed inside its
    savepoint, and the Screener quietly served the published rows only."""
    stored = _stored_breadth("O:SPY251120C00760000")
    capture = {"underlying": "SPY", "state": "COMPLETE", "observed_at": NOW, "contracts": 1, "reason": None}

    class _Cursor:
        def __init__(self, mapping):
            self.mapping, self.calls = mapping, 0
            self.description = [(name,) for name in stored]

        def execute(self, *_args):
            self.calls += 1

        def fetchall(self):
            row = stored if self.calls == 1 else capture
            return [dict(row)] if self.mapping else [tuple(row.values())]

    for mapping in (True, False):
        rows, captures = breadth.load_breadth_screener_rows_cursor(
            _Cursor(mapping), session_date=SESSION, underlyings=("SPY",), available_by=NOW,
        )
        assert [row["contract_ticker"] for row in rows] == ["O:SPY251120C00760000"]
        assert captures["SPY"]["breadth_state"] == "COMPLETE" and captures["SPY"]["breadth_contracts"] == 1
