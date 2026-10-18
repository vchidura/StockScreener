from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from functools import lru_cache
from uuid import UUID

from fastapi.testclient import TestClient
import pytest

from realtime.contracts import canonical_sha256
from realtime.portal_projection_repository import persist_portal_snapshot_cursor
from realtime.ticker_projection import produce_realtime_pattern_watch_publication, produce_realtime_ticker_workspace_summary
from realtime.ticker_projection import _fact, _security, materialize_ticker_interval
from realtime.ticker_reconstruction import publication_source_cutoff, reconstruct_ticker_closing_cycle
from realtime.ticker_repository import TickerCycle, persist_realtime_ticker_cycle_cursor
from tests.realtime.test_ticker_vertical_slice import ticker_app, ticker_fixture, ticker_projection


def test_materialization_identity_distinguishes_source_provenance_even_when_bar_ids_match(monkeypatch):
    inputs = ticker_fixture()
    member = next(member for member in inputs.members if member["ticker"] == "AAPL")
    security = _security(member)
    bars = tuple(_fact(row, inputs) for row in inputs.bars
        if row["ticker"] == "AAPL" and row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED")[-2:]
    monkeypatch.setattr("realtime.ticker_projection.materialize_realtime_equity_evidence",
        lambda **kwargs: kwargs["analysis_run_id"])
    def identity(facts):
        return materialize_ticker_interval(inputs, security, facts, interval="1d", basis="ADJUSTED",
            policy_sha256="a" * 64)
    original = identity(bars)
    assert identity(bars) == original
    assert identity(tuple(replace(fact, cohort_id=UUID(int=998)) for fact in bars)) != original
    assert identity(tuple(replace(fact, revision=replace(fact.revision, payload_sha256="e" * 64)) for fact in bars)) != original


@lru_cache(maxsize=1)
def rebuilt_cycle():
    inputs, projection = ticker_fixture(), ticker_projection()
    cycle = TickerCycle(inputs, projection,
        produce_realtime_ticker_workspace_summary(inputs, projection.snapshots, projection.summary_charts),
        produce_realtime_pattern_watch_publication(inputs, projection.snapshots))
    sources = {snapshot.snapshot_key: dict(snapshot_id=str(snapshot.snapshot_id),
        payload_sha256=snapshot.payload_sha256, source_manifest_sha256=snapshot.source_manifest_sha256)
        for snapshot in projection.snapshots}
    return reconstruct_ticker_closing_cycle(cycle, published_at=inputs.cutoff + timedelta(days=2),
        source_publications=sources)


def test_reconstruction_keeps_market_inputs_but_records_the_real_publication_clock():
    original = ticker_projection()
    hashes = [canonical_sha256(snapshot.payload) for snapshot in original.snapshots]
    cycle = rebuilt_cycle()
    for old, new in zip(original.snapshots, cycle.projection.snapshots):
        assert old.snapshot_id != new.snapshot_id
        assert new.generated_at > old.generated_at
        assert new.payload["quote"] == old.payload["quote"]
        assert new.payload["chart_refs"] == old.payload["chart_refs"]
        assert new.source_manifest["bar_lineage"] == old.source_manifest["bar_lineage"]
        assert new.payload["session"] == str(cycle.inputs.session)
        assert new.payload["multi"]["as_of"] == new.generated_at.isoformat()
        assert new.payload["reconstruction"] == new.source_manifest["reconstruction"]
        for metric in new.payload["multi"]["setup_read_metrics"].values():
            assert metric["published_at"] == new.generated_at.isoformat()
        assert publication_source_cutoff(new.generated_at, new.payload["reconstruction"],
            session_date=cycle.inputs.session, market_time=new.payload["quote"]["as_of"]) == cycle.inputs.cutoff
    assert hashes == [canonical_sha256(snapshot.payload) for snapshot in original.snapshots]
    assert cycle.pattern_publication.payload["views"]["1mo"]["status"] == "COMPLETE"
    assert cycle.summary.payload["session"] == str(cycle.inputs.session)


@pytest.mark.parametrize("field,value", [
    ("kind", "CURRENT"),
    ("market_cutoff", "2026-10-02T19:00:00+00:00"),
    ("knowledge_cutoff", "2099-01-01T00:00:00+00:00"),
    ("published_at", "2026-10-01T00:00:00+00:00"),
    ("input_cutoff", "2026-10-02T19:00:00"),
    ("session", "2026-10-03"),
])
def test_reconstruction_rejects_changed_clocks_or_session_identity(field, value):
    snapshot = rebuilt_cycle().projection.snapshots[0]
    provenance = deepcopy(snapshot.payload["reconstruction"])
    provenance[field] = value
    with pytest.raises(ValueError):
        publication_source_cutoff(snapshot.generated_at, provenance,
            session_date=rebuilt_cycle().inputs.session, market_time=snapshot.payload["quote"]["as_of"])


def test_reconstructed_monthly_reads_validate_original_reference_time_without_changing_the_current_view():
    cycle = rebuilt_cycle()
    now = cycle.projection.snapshots[0].generated_at + timedelta(seconds=1)
    app, store = ticker_app(clock=lambda: now)
    original_current = deepcopy(store.current)
    for snapshot in cycle.projection.snapshots:
        persist_portal_snapshot_cursor(store, snapshot)
    store.reference_expired = True
    snapshot = next(snapshot for snapshot in cycle.projection.snapshots if snapshot.snapshot_key == "AAPL")
    params = dict(cohort_id=str(snapshot.cohort_id), snapshot_id=str(snapshot.snapshot_id), read_mode="session_close")
    with TestClient(app) as client:
        publication = client.get("/api/stock/AAPL/workspace-publication", params=params)
        assert publication.status_code == 200, publication.text
        assert publication.json()["reconstruction"]["kind"] == "RECONSTRUCTED_RESEARCH"
        assert publication.json()["session"] == str(cycle.inputs.session)
        monthly = client.get("/api/stock/AAPL/chart", params=dict(params, interval="1mo", period="5y"))
        assert monthly.status_code == 200, monthly.text
        assert len(monthly.json()) >= 50
        store.reference_drift = True
        assert client.get("/api/stock/AAPL/workspace-publication", params=params).status_code == 503
    assert store.current == original_current


def test_reconstructed_pattern_watch_is_a_complete_separate_closing_publication():
    cycle = rebuilt_cycle()
    app, store = ticker_app(clock=lambda: cycle.summary.generated_at + timedelta(seconds=1))
    current = deepcopy(store.current)
    _, kept = persist_realtime_ticker_cycle_cursor(store, cycle, session_close=True)
    publication = next(snapshot for snapshot in kept if snapshot.snapshot_type == cycle.pattern_publication.snapshot_type)
    with TestClient(app) as client:
        result = client.get("/api/chart-patterns/scan", params=dict(cohort_id=str(publication.cohort_id),
            snapshot_id=str(publication.snapshot_id), read_mode="session_close",
            session_date=str(cycle.inputs.session), interval="1mo"))
        assert result.status_code == 200, result.text
        assert result.json()["publication"]["session"] == str(cycle.inputs.session)
        assert result.json()["scanned"] == len(cycle.inputs.members)
        summary = next(snapshot for snapshot in kept if snapshot.snapshot_type == cycle.summary.snapshot_type)
        summary_result = client.get("/api/tickers/workspace-publication", params=dict(cohort_id=str(summary.cohort_id),
            snapshot_id=str(summary.snapshot_id), read_mode="session_close", session_date=str(cycle.inputs.session)))
        assert summary_result.status_code == 200, summary_result.text
        assert summary_result.json()["publication"]["reconstruction"]["kind"] == "RECONSTRUCTED_RESEARCH"
    assert store.current == current
