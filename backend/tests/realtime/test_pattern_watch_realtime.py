from datetime import date, datetime, timezone
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import UUID

from fastapi.testclient import TestClient

from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.ticker_repository import RealtimeTickerWorkspaceRepository
from realtime.ticker_projection import produce_realtime_pattern_watch_publication, TICKER_WORKSPACE_VERSION
from realtime.preflight import ProviderTier
from realtime.portal_projection import build_portal_snapshot
from realtime.contracts import canonical_json
import pytest
import realtime.ticker_repository as ticker_repository


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
                             identity_continuity_sha256=None,
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
        fifteen_minute = client.get("/api/chart-patterns/scan", params=dict(params, interval="15m"))

    assert monthly.status_code == 200
    assert monthly.json()["interval"] == "1mo"
    assert five_minute.status_code == 422
    assert fifteen_minute.status_code == 422


def test_legacy_cross_frame_reads_filter_retired_frames_without_mutating_saved_payload(monkeypatch):
    @contextmanager
    def cursor():
        yield object()

    reader = RealtimeTickerWorkspaceRepository(cursor)
    pattern = _pattern("FORMING", "VALID_GEOMETRY", 0.2, 6)
    legacy = dict(patterns={interval: {} for interval in ("5m", "15m", "1d", "1mo")},
        chart_market_times={"1d": CUTOFF.isoformat(), "1mo": CUTOFF.isoformat()},
        cross_patterns=dict(interval="all", scanned=1, matched_tickers=1,
            results=[dict(ticker="AAPL", interval=interval, pattern=pattern)
                     for interval in ("5m", "15m", "1d", "1mo")],
            missing_intervals=["5m", "15m"], cross_frame=None))
    before = canonical_json(legacy)
    monkeypatch.setattr(reader, "_snapshot", lambda *args, **kwargs: legacy)
    monkeypatch.setattr(ticker_repository, "_read_availability", lambda *args: {"status": "READY"})
    result = reader.read("AAPL", "cross_patterns", available_by=CUTOFF, cohort_id=COHORT_ID)
    assert {row["interval"] for row in result["results"]} == {"1d", "1mo"}
    assert [frame["interval"] for frame in result["cross_frame"]["frames"]] == ["1mo", "1d"]
    assert result["retired_intervals"] == ["5m", "15m"]
    assert canonical_json(legacy) == before
    for interval in ("5m", "15m"):
        with pytest.raises(ValueError, match="only 30m"):
            reader.read("AAPL", "patterns", available_by=CUTOFF, cohort_id=COHORT_ID, interval=interval)


def _closing_reader(monkeypatch):
    statements = []
    rows = {}
    for day in (1, 2):
        session = date(2026, 10, day)
        market_time = datetime(2026, 10, day, 20, tzinfo=timezone.utc)
        rows[str(day)] = dict(snapshot_id=UUID(int=day), session_date=session, snapshot_class="SESSION_CLOSE",
            generated_at=market_time, source_generation=day, payload={
                "views": {"1d": dict(status="COMPLETE", market_time=market_time.isoformat(),
                    evidence_storage="IMMUTABLE_EVIDENCE", results=[dict(ticker="AAPL", last_close=100 + day)],
                    scanned=1, matched_tickers=1, interval="1d", computed_at=market_time.isoformat())},
                "references": [{"ticker": "AAPL"}], "workspaces": [],
            })

    class Cursor:
        def execute(self, sql, params=()):
            statements.append(sql)

        def fetchall(self):
            return [dict(session_date=row["session_date"], snapshot_id=row["snapshot_id"], generated_at=row["generated_at"])
                    for row in rows.values()]

    @contextmanager
    def cursor():
        yield Cursor()

    repository = RealtimeTickerWorkspaceRepository(cursor)
    monkeypatch.setattr(repository, "_release", lambda cursor, cohort_id: SimpleNamespace(
        entry=lambda *args: {"snapshot_id": str(UUID(int=2)), "source_generation": 2,
                             "generated_at": CUTOFF.isoformat()}))

    def summary(cursor, **scope):
        assert scope["read_mode"] == "retained"
        return rows[str(UUID(str(scope["snapshot_id"])).int)]

    monkeypatch.setattr(repository, "_summary_snapshot", summary)
    prohibited = lambda *args, **kwargs: pytest.fail("Closing reads must not resolve hot evidence or run pattern detection")
    monkeypatch.setattr(ticker_repository, "resolve_release_evidence_refs_cursor", prohibited)
    monkeypatch.setattr(ticker_repository, "produce_realtime_pattern_watch_publication", prohibited)
    return repository, rows, statements


def test_saved_pattern_closes_list_dates_and_keep_latest_market_stamp_static(monkeypatch):
    repository, rows, statements = _closing_reader(monkeypatch)
    result = repository.pattern_closing_sessions(available_by=CUTOFF, cohort_id=COHORT_ID)
    assert result["sessions"] == ["2026-10-01", "2026-10-02"]
    assert result["data_as_of"] == "2026-10-02T20:00:00+00:00"
    assert result["provider_fetch_performed"] is False
    assert any("snapshot.snapshot_class='SESSION_CLOSE'" in statement and "LIMIT 21" in statement for statement in statements)
    assert all(statement.startswith(("SELECT", "SET")) for statement in statements)


def test_saved_pattern_date_reads_exact_frozen_rows_without_resolving_hot_workspaces(monkeypatch):
    repository, rows, statements = _closing_reader(monkeypatch)
    app = create_realtime_portal_app(RealtimePortalRepositories(None, None, None, ticker_workspace=repository),
                                     clock=lambda: CUTOFF)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), read_mode="session_close")
        latest = client.get("/api/chart-patterns/scan", params=params)
        assert latest.status_code == 200, latest.text
        assert latest.json()["publication"]["session"] == "2026-10-02"
        selected = client.get("/api/chart-patterns/scan", params=dict(params, session_date="2026-10-01"))
        assert selected.status_code == 200, selected.text
        assert selected.json()["results"][0]["last_close"] == 101
        assert selected.json()["publication"]["snapshot_id"] == str(UUID(int=1))
        assert selected.json()["publication"]["read_mode"] == "SESSION_CLOSE"
        assert client.get("/api/chart-patterns/scan", params=dict(params, session_date="2026-09-30")).status_code == 503
        for invalid in ("not-a-date", "2026-10-03", ""):
            assert client.get("/api/chart-patterns/scan", params=dict(params, session_date=invalid)).status_code == 422
        listed = client.get("/api/chart-patterns/sessions", params={"cohort_id": str(COHORT_ID)})
        assert listed.status_code == 200, listed.text
        assert listed.json()["sessions"] == ["2026-10-01", "2026-10-02"]
    assert all(statement.startswith(("SELECT", "SET")) for statement in statements)


def test_pattern_close_rejects_hot_or_wrong_date_pins(monkeypatch):
    repository, rows, _ = _closing_reader(monkeypatch)
    with pytest.raises(LookupError, match="selected session"):
        repository.scan_patterns(interval="1d", limit=1000, available_by=CUTOFF, cohort_id=COHORT_ID,
            snapshot_id=UUID(int=2), session_date="2026-10-01", read_mode="session_close")
    rows["1"]["snapshot_class"] = "EPHEMERAL_PORTAL"
    with pytest.raises(LookupError, match="selected session"):
        repository.scan_patterns(interval="1d", limit=1000, available_by=CUTOFF, cohort_id=COHORT_ID,
            snapshot_id=UUID(int=1), session_date="2026-10-01", read_mode="session_close")


def _all_ticker_closing_reader(monkeypatch):
    repository, rows, statements = _closing_reader(monkeypatch)
    for row in rows.values():
        session = str(row["session_date"])
        market_time = row["generated_at"].isoformat()
        row["source_manifest"] = {}
        row["payload_sha256"] = str(row["snapshot_id"])
        row["payload"] = dict(session=session, market_time=market_time, generated_at=market_time,
            overview=[dict(ticker="AAPL", date=session, close=100 + row["source_generation"])],
            setup=dict(interval="1d", count=1, market_time=market_time, computed_at=market_time,
                       results=[dict(ticker="AAPL", trend=f"day-{row['source_generation']}")]),
            workspaces=[], missing_producers={})
    return repository, rows, statements


def test_all_tickers_closing_rows_and_setups_follow_selected_date_without_current_fallback(monkeypatch):
    repository, rows, statements = _all_ticker_closing_reader(monkeypatch)
    app = create_realtime_portal_app(RealtimePortalRepositories(None, None, None, ticker_workspace=repository),
                                     clock=lambda: CUTOFF)
    with TestClient(app) as client:
        scope = dict(cohort_id=str(COHORT_ID), read_mode="session_close")
        metadata = client.get("/api/tickers/sessions", params={"cohort_id": str(COHORT_ID)})
        assert metadata.status_code == 200, metadata.text
        assert metadata.json()["sessions"] == ["2026-10-01", "2026-10-02"]
        assert metadata.json()["data_as_of"] == "2026-10-02T20:00:00+00:00"
        for session, close in (("2026-10-01", 101), ("2026-10-02", 102)):
            result = client.get("/api/tickers/workspace-publication", params=dict(scope, session_date=session))
            assert result.status_code == 200, result.text
            data = result.json()
            assert data["publication"]["session"] == session
            assert data["publication"]["read_mode"] == "SESSION_CLOSE"
            assert data["overview"][0] == dict(ticker="AAPL", date=session, close=close)
            assert data["setup"]["publication"] == data["publication"]
            assert data["setup"]["results"][0]["trend"] == f"day-{close - 100}"
        latest = client.get("/api/tickers/workspace-publication", params=scope)
        assert latest.status_code == 200, latest.text
        assert latest.json()["overview"][0]["close"] == 102
        assert client.get("/api/tickers/workspace-publication", params=dict(scope, session_date="2026-09-30")).status_code == 503
        for invalid in ("", "2026-10-03", "not-a-date"):
            assert client.get("/api/tickers/workspace-publication", params=dict(scope, session_date=invalid)).status_code == 422
    assert all(statement.startswith(("SELECT", "SET")) for statement in statements)


def test_all_tickers_closing_pin_rejects_wrong_date_or_hot_data(monkeypatch):
    repository, rows, _ = _all_ticker_closing_reader(monkeypatch)
    scope = dict(available_by=CUTOFF, cohort_id=COHORT_ID, read_mode="session_close")
    with pytest.raises(LookupError, match="selected session"):
        repository.workspace_publication(**scope, snapshot_id=UUID(int=2), session_date="2026-10-01")
    rows["1"]["snapshot_class"] = "EPHEMERAL_PORTAL"
    with pytest.raises(LookupError, match="selected session"):
        repository.workspace_publication(**scope, snapshot_id=UUID(int=1), session_date="2026-10-01")
