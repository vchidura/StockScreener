from datetime import datetime, timezone
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import UUID

from fastapi.testclient import TestClient

from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.ticker_repository import RealtimeTickerWorkspaceRepository
from realtime.ticker_projection import produce_realtime_pattern_watch_publication, TICKER_WORKSPACE_VERSION
from realtime.preflight import ProviderTier
from realtime.portal_projection import build_portal_snapshot


CUTOFF = datetime(2026, 10, 2, 21, 0, tzinfo=timezone.utc)
COHORT_ID = UUID("d1baee6f-4ab2-543c-8a86-ce10f8952e69")


def _pattern(readiness, grade, edge, touches):
    return {
        "type": "ASCENDING_TRIANGLE", "name": "Ascending triangle",
        "status": "FORMING", "bias": "BULLISH", "grade": grade,
        "start_time": 1, "end_time": 2, "formation_bars": 20,
        "upper_touches": touches // 2, "lower_touches": touches - touches // 2,
        "contraction_pct": 30.0, "apex_bars_ahead": 5.0, "fit_error_atr": 0.2,
        "flagpole_atr": None, "invalidation_price": None, "readiness": readiness,
        "boundary_role": "resistance", "boundary_price": 101.0,
        "edge_distance_atr": edge, "edge_distance_pct": 0.5, "lines": [],
    }


def _payload(ticker, sector, pattern):
    return {
        "profile": {"security": {"sector": sector}},
        "patterns": {"1d": {
            "ticker": ticker, "interval": "1d", "last_close": 100.0,
            "patterns": [pattern], "computed_at": CUTOFF.isoformat(),
        }},
        "channels": {"1d": {"channel": None}},
        "chart_market_times": {"1d": datetime(2026, 10, 2, 20, 0, tzinfo=timezone.utc).isoformat()},
    }


def test_pattern_producer_sorts_and_reader_only_slices_stored_publication(monkeypatch):
    inputs = SimpleNamespace(cohort_id=COHORT_ID, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                             source_generation=1, source_manifest_sha256="a" * 64, cutoff=CUTOFF,
                             members=({"ticker": "AAPL"}, {"ticker": "MSFT"}))
    snapshots = []
    for ticker, readiness in (("AAPL", "FORMING"), ("MSFT", "AT_EDGE")):
        payload = _payload(ticker, "Technology", _pattern(readiness, "VALID_GEOMETRY", 0.2, 6))
        payload["pattern_evidence_refs"] = {"1d": [dict(cohort_id=str(COHORT_ID), evidence_id=str(COHORT_ID),
            evidence_type="FEATURE_SNAPSHOT", market_time=payload["chart_market_times"]["1d"],
            payload_sha256="a" * 64)]}
        snapshots.append(build_portal_snapshot(
            cohort_id=COHORT_ID, provider_tier=inputs.provider_tier, snapshot_type=TICKER_WORKSPACE_VERSION,
            snapshot_key=ticker, source_generation=1,
            source_manifest={"reference": {"ticker": ticker, "security_revision_id": f"revision-{ticker}"}},
            payload=payload, generated_at=CUTOFF,
        ))
    publication = produce_realtime_pattern_watch_publication(inputs, snapshots)
    assert [row["ticker"] for row in publication.payload["views"]["1d"]["results"]] == ["MSFT", "AAPL"]
    assert publication.source_manifest["reference_set"]["count"] == 2
    assert "references" not in publication.source_manifest and "workspaces" not in publication.source_manifest
    @contextmanager
    def cursor():
        yield object()
    repository = RealtimeTickerWorkspaceRepository(cursor)
    row = dict(snapshot_id=publication.snapshot_id, source_generation=1, generated_at=CUTOFF,
               source_manifest=publication.source_manifest,
               payload={**publication.payload, "references": []})
    monkeypatch.setattr(repository, "_begin", lambda cursor: None)
    monkeypatch.setattr(repository, "_summary_snapshot", lambda cursor, **kwargs: row)
    result = repository.scan_patterns(
        interval="1d", limit=1, available_by=CUTOFF, cohort_id=COHORT_ID,
    )

    assert result["scanned"] == 2
    assert result["matched_tickers"] == 2
    assert [row["ticker"] for row in result["results"]] == ["MSFT"]
    assert result["cross_frame"] is None
    assert result["provider_fetch_performed"] is False


class _FakeTickerWorkspace:
    def list_tickers(self, **kwargs):
        assert kwargs["cohort_id"] == COHORT_ID
        return ("AAPL", "MSFT")

    def scan_patterns(self, **kwargs):
        assert kwargs["cohort_id"] == COHORT_ID
        assert kwargs["interval"] in {"1d", "1mo"}
        return {
            "interval": kwargs["interval"], "scanned": 2, "matched_tickers": 0,
            "results": [], "cross_frame": None, "computed_at": CUTOFF.isoformat(),
            "provider_fetch_performed": False,
        }


def test_pattern_watch_compatibility_routes_preserve_original_contract():
    app = create_realtime_portal_app(
        RealtimePortalRepositories(None, None, None, ticker_workspace=_FakeTickerWorkspace()),
        clock=lambda: CUTOFF,
    )
    with TestClient(app) as client:
        tickers = client.get("/api/tickers", params={"cohort_id": str(COHORT_ID)})
        patterns = client.get(
            "/api/chart-patterns/scan",
            params={"cohort_id": str(COHORT_ID), "interval": "1d"},
        )

    assert tickers.status_code == 200
    assert tickers.json() == ["AAPL", "MSFT"]
    assert patterns.status_code == 200
    assert patterns.json()["results"] == []
    assert patterns.json()["scanned"] == 2


def test_pattern_watch_universe_accepts_monthly_and_reserves_five_minute_for_tickers():
    app = create_realtime_portal_app(
        RealtimePortalRepositories(None, None, None, ticker_workspace=_FakeTickerWorkspace()),
        clock=lambda: CUTOFF,
    )
    params = {"cohort_id": str(COHORT_ID)}
    with TestClient(app) as client:
        monthly = client.get("/api/chart-patterns/scan", params=dict(params, interval="1mo"))
        five_minute = client.get("/api/chart-patterns/scan", params=dict(params, interval="5m"))

    assert monthly.status_code == 200
    assert monthly.json()["interval"] == "1mo"
    assert five_minute.status_code == 422
