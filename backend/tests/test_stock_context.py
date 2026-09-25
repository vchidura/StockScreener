from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from equity.polygon import sha256_json
from equity.stock_context import build_context, sector_context


NOW = datetime(2026, 9, 21, 16, tzinfo=timezone.utc)
DAILY = datetime(2026, 9, 18, 20, tzinfo=timezone.utc)
EXPECTED = {"1d": DAILY, "1h": NOW - timedelta(minutes=30), "30m": NOW - timedelta(minutes=30)}


def inputs():
    payload = dict(expected_members=2, rank_population=2, generation="cohort", universe="tracked",
        source_publication_id="bars", session="2026-09-18", market_time=DAILY.isoformat(),
        source_cutoff=(DAILY + timedelta(minutes=20)).isoformat(), rows=[
            dict(security_id="one", ticker="AAA", reference_id="ref-one", eligible=True,
                 values=dict(momentum_percentile=1., momentum_12_1=.3, price=20., discovery_state="PULLBACK", discovery_trend="UP")),
            dict(security_id="two", ticker="BBB", reference_id="ref-two", eligible=True,
                 values=dict(momentum_percentile=0., momentum_12_1=-.2, price=15., discovery_state="BOUNCE", discovery_trend="DOWN"))])
    daily = dict(snapshot_id="snapshot", generated_at=DAILY + timedelta(minutes=25), payload=payload, payload_sha256=sha256_json(payload))
    setups = [dict(security_id="one", ticker="AAA", interval=interval, analysis_run_id="run-" + interval,
        evidence_id="evidence-" + interval, payload_sha256="a" * 64, market_time=EXPECTED[interval],
        observed_at=EXPECTED[interval] + timedelta(minutes=15), published_at=EXPECTED[interval] + timedelta(minutes=16),
        created_at=EXPECTED[interval] + timedelta(minutes=16), source_version="setup-v1", trend="Bullish Stack",
        momentum="Uptrend", close=21.) for interval in ("1h", "30m")]
    references = {"ref-one": dict(security_id="one", sector="Technology"), "ref-two": dict(security_id="two", sector="Technology")}
    return daily, setups, references


def test_context_preserves_published_ranks_and_independent_timeframes():
    daily, setups, refs = inputs()
    before = deepcopy(daily)
    context = build_context(daily, setups, refs, now=NOW, expected=EXPECTED)
    assert [row["daily"]["percentile"] for row in context["rows"]] == [1., 0.]
    assert context["rows"][0]["frames"]["30m"]["status"] == "READY"
    assert context["rows"][1]["frames"]["30m"]["status"] == "UNAVAILABLE"
    assert context["rows"][1]["daily"]["status"] == "READY"
    assert context["cohort"]["eligible_ranked"] == 2
    assert daily == before and context["execution_permission"] is False


@pytest.mark.parametrize("field", ["observed_at", "published_at", "created_at", "market_time"])
def test_future_component_cannot_become_usable_or_remove_daily_rank(field):
    daily, setups, refs = inputs()
    setups[0][field] = NOW + timedelta(seconds=1)
    context = build_context(daily, setups, refs, now=NOW, expected=EXPECTED)
    assert context["rows"][0]["frames"]["1h"]["status"] == "UNAVAILABLE"
    assert context["rows"][0]["frames"]["1h"]["trend"] is None
    assert context["rows"][0]["daily"]["percentile"] == 1.


def test_stale_hourly_does_not_stale_halfhour_or_daily():
    daily, setups, refs = inputs()
    setups[0]["market_time"] -= timedelta(hours=1)
    context = build_context(daily, setups, refs, now=NOW, expected=EXPECTED)
    assert context["rows"][0]["frames"]["1h"]["status"] == "STALE"
    assert context["rows"][0]["frames"]["30m"]["status"] == "READY"
    assert context["rows"][0]["daily"]["status"] == "READY"


def test_cohort_checksum_identity_and_population_fail_closed():
    daily, setups, refs = inputs()
    daily["payload"]["rows"][0]["values"]["momentum_percentile"] = .8
    with pytest.raises(ValueError, match="checksum"):
        build_context(daily, setups, refs, now=NOW, expected=EXPECTED)
    daily["payload_sha256"] = sha256_json(daily["payload"])
    daily["payload"]["rank_population"] = 1
    daily["payload_sha256"] = sha256_json(daily["payload"])
    with pytest.raises(ValueError, match="population"):
        build_context(daily, setups, refs, now=NOW, expected=EXPECTED)


def test_sector_context_uses_published_full_cohort_not_a_new_sector_rank():
    context = build_context(*inputs(), now=NOW, expected=EXPECTED)
    summary = sector_context(context, "Technology")
    assert (summary["members"], summary["ranked"], summary["leading"], summary["lagging"]) == (2, 2, 1, 1)
    assert summary["average_percentile"] == .5
    assert summary["state_mix"] == {"PULLBACK": 1, "BOUNCE": 1}


def test_no_daily_snapshot_does_not_disable_existing_hourly_context():
    _, setups, _ = inputs()
    context = build_context(None, setups, {}, now=NOW, expected=EXPECTED)
    assert context["cohort"] is None and context["status"] == "PARTIAL"
    assert context["rows"][0]["daily"]["status"] == "UNAVAILABLE"
    assert context["rows"][0]["frames"]["1h"]["status"] == "READY"


def test_context_endpoint_filters_after_full_rank_population(monkeypatch):
    from equity import stock_context as reader
    monkeypatch.setattr(reader, "load_stock_context", lambda: build_context(*inputs(), now=NOW, expected=EXPECTED))
    result = reader.stock_context("bbb")
    assert [row["ticker"] for row in result["rows"]] == ["BBB"]
    assert result["cohort"]["eligible_ranked"] == 2
    assert result["rows"][0]["daily"]["percentile"] == 0.


def test_context_route_registered_once():
    import main
    for path in ("/api/stocks/context", "/api/stocks/alert-view", "/api/stocks/alert-eod-review", "/api/stocks/market-conditions"):
        assert sum(route.path == path for route in main.app.routes) == 1


def test_sector_price_intelligence_has_no_legacy_or_optional_context_dependency(monkeypatch):
    from contextlib import contextmanager
    from datetime import date
    from unittest.mock import MagicMock
    from equity import sector_research
    cursor = MagicMock()
    cursor.fetchall.return_value = [dict(sector="Technology", count=2)]
    @contextmanager
    def database():
        yield cursor
    monkeypatch.setattr(sector_research, "get_db_cursor", database)
    monkeypatch.setattr(sector_research, "_sector_ticker_returns", lambda cursor, sessions: [
        dict(sector="Technology", ticker="AAA", return_pct=.1, trade_date=date(2026, 9, 21)),
        dict(sector="Technology", ticker="BBB", return_pct=-.02, trade_date=date(2026, 9, 21))])
    result = sector_research.sector_intelligence(1)
    sector = result["results"][0]
    assert sector["rotation"]["1"] == dict(average_return=.04, positive_breadth=.5, tickers=2, rank=1)
    assert sector["rotation_delta"] == 0
    assert sector["leaders"]["21"] == [dict(ticker="AAA", return_pct=.1)]
    assert sector["laggards"]["21"] == [dict(ticker="BBB", return_pct=-.02)]
    assert result["discovery_trade_date"] is None and result["cross_sectional_trade_date"] is None
    assert all("market_discovery_states" not in call.args[0] and "cross_sectional_signals" not in call.args[0]
               for call in cursor.execute.call_args_list)


def test_ticker_reuse_cannot_attach_a_prior_security_rank_to_current_behavior():
    daily, setups, refs = inputs()
    setups[0]["security_id"] = "different-security"
    context = build_context(daily, setups, refs, now=NOW, expected=EXPECTED)
    affected = [row for row in context["rows"] if row["ticker"] == "AAA"]
    assert len(affected) == 1 and affected[0]["daily"]["percentile"] is None
    assert affected[0]["daily"]["status"] == "UNAVAILABLE"
    assert all(frame["status"] == "UNAVAILABLE" for frame in affected[0]["frames"].values())
    assert next(row for row in context["rows"] if row["ticker"] == "BBB")["daily"]["status"] == "READY"


def test_context_reader_uses_only_bounded_read_only_queries(monkeypatch):
    from contextlib import contextmanager
    from equity import stock_context as reader
    daily, setups, refs = inputs()
    statements = []
    class Cursor:
        def execute(self, sql, parameters=None):
            statements.append(sql)
            self.sql = sql
        def fetchone(self):
            return daily
        def fetchall(self):
            if "equity_security_reference_revisions" in self.sql:
                return [dict(security_revision_id=key, **value) for key, value in refs.items()]
            return setups
    @contextmanager
    def database():
        yield Cursor()
    monkeypatch.setattr(reader, "get_db_cursor", database)
    monkeypatch.setattr(reader, "expected_materialized_market_time", lambda now, interval: EXPECTED[interval])
    assert reader.load_stock_context(now=NOW)["cohort"]["eligible_ranked"] == 2
    assert "REPEATABLE READ, READ ONLY" in statements[0]
    assert "statement_timeout" in statements[1]
    assert all(not any(verb in sql.upper() for verb in ("INSERT ", "UPDATE ", "DELETE ")) for sql in statements)


def test_signed_zero_audit_requires_exact_original_hash_without_mutating_payload():
    from scripts.audit_equity_simplification import signed_zero_roundtrip_matches
    payload = {"value": 0.0, "text": "0.0"}
    assert signed_zero_roundtrip_matches(payload, sha256_json({"value": -0.0, "text": "0.0"}))
    assert not signed_zero_roundtrip_matches(payload, "a" * 64)
    assert sha256_json(payload) == sha256_json({"value": 0.0, "text": "0.0"})


def test_numeric_diagnostics_distinguishes_zero_floats_without_unbounded_samples():
    from scripts.audit_equity_simplification import numeric_diagnostics
    payload = {"rows": [{"change": 0.0, "count": 0, "flag": False, "label": "0.0"} for _ in range(25)]}
    original = deepcopy(payload)
    report = numeric_diagnostics(payload)
    assert report["counts"] == {"float": 25, "float_zero": 25, "int": 25}
    assert report["zero_fields"] == {"change": 25}
    assert report["original_search_skipped"] is True
    assert len(report["zero_paths_sample"]) == 20 and payload == original


def test_signed_zero_recovers_repeated_ma_fields_without_changing_values():
    from scripts.audit_equity_simplification import signed_zero_roundtrip_report
    rows = [dict(ticker=f"STOCK{index}", date="2026-09-21", days_since_cross=0,
                 price_change_since_cross_pct=0.) for index in range(30)]
    rows.append(dict(ticker="ROUNDING", date="2026-09-21", days_since_cross=2,
                     price_change_since_cross_pct=-0.0))
    original = dict(results=rows, results_by_signal={"Recent Bearish": deepcopy(rows)})
    stored = deepcopy(original)
    stored["results"][-1]["price_change_since_cross_pct"] = 0.
    stored["results_by_signal"]["Recent Bearish"][-1]["price_change_since_cross_pct"] = 0.
    before = sha256_json(stored)
    result = signed_zero_roundtrip_report(stored, sha256_json(original))
    assert result["matched"] and result["attempts"] == 1 and result["independent_zero_groups"] == 1
    assert len(result["negative_zero_paths"]) == 2 and sha256_json(stored) == before
    assert not signed_zero_roundtrip_report(stored, "a" * 64)["matched"]


def test_signed_zero_streak_recovery_follows_daily_copies_and_keeps_nonzero_fields():
    from scripts.audit_equity_simplification import signed_zero_roundtrip_report
    original = {"results": [dict(ticker="AAA", ma_analysis=dict(
        daily_details={"2026-09-18": dict(days_since_cross=0, price_change_since_cross_pct=0., ma_spread_pct=.1),
                       "2026-09-21": dict(days_since_cross=1, price_change_since_cross_pct=-0.0, ma_spread_pct=-0.0)},
        price_changes=[0., -0.0], spreads=[.1, -0.0]))]}
    stored = json_roundtrip_positive_zero(original)
    before = sha256_json(stored)
    recovered = signed_zero_roundtrip_report(stored, sha256_json(original))
    assert recovered["matched"] and len(recovered["copied_zero_paths"]) == 2
    assert sha256_json(stored) == before
    stored["results"][0]["ma_analysis"]["spreads"][0] = .2
    assert not signed_zero_roundtrip_report(stored, sha256_json(original))["matched"]


def json_roundtrip_positive_zero(value):
    if isinstance(value, dict):
        return {key: json_roundtrip_positive_zero(child) for key, child in value.items()}
    if isinstance(value, list):
        return [json_roundtrip_positive_zero(child) for child in value]
    return 0.0 if isinstance(value, float) and value == 0.0 else value


def test_signed_zero_search_stops_at_declared_attempt_budget():
    from scripts.audit_equity_simplification import signed_zero_roundtrip_report
    report = signed_zero_roundtrip_report({"values": [0.] * 20}, "a" * 64, maximum_attempts=3)
    assert not report["matched"] and report["reason"] == "SEARCH_BOUND" and report["attempts"] == 3