from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from unittest.mock import MagicMock
from uuid import UUID

from fastapi.testclient import TestClient
import pytest

from equity.ticker_contracts import setup_summary_row
from realtime.contracts import canonical_json, canonical_sha256
from realtime.dashboard_projection import produce_realtime_dashboard
from realtime.lineage_head import rows_identity
from tests.realtime.test_dashboard_vertical_slice import (
    COHORT_ID, OBSERVED_AT, dashboard_app, dashboard_fixture, dashboard_snapshot,
)


def test_original_daily_summary_is_exact_real_published_setup_evidence():
    snapshot = dashboard_snapshot()
    summary = snapshot.payload["setup_summary"]
    evidence = snapshot.payload["setup_evidence"]
    assert summary["source"] == "MATERIALIZED_CURRENT_PROJECTION"
    assert summary["count"] == 9
    assert [row["ticker"] for row in summary["results"]] == snapshot.payload["tickers"]
    assert len({row["analysis_run_id"] for row in evidence}) == 1
    for row, published in zip(summary["results"], evidence):
        assert row == setup_summary_row(published)
        assert published["payload_sha256"] == canonical_sha256(published["payload"])
        assert len(published["source_revision_ids"]) == 283
        assert len(published["confirmation_revision_ids"]) >= 50
        assert row["rsi"] is not None and row["adx"] is not None
        assert row["momentum"] is not None and row["trend"] is not None
        assert row["confirm_interval"] == "1h"
        assert isinstance(row["multi_tf_agree"], bool)
        assert len(row) == 29


def test_get_reads_pinned_original_contract_without_producing(monkeypatch):
    app, store = dashboard_app()
    import realtime.setup_summary_projection as producer
    prohibited = MagicMock(side_effect=AssertionError("GET must not materialize"))
    monkeypatch.setattr(producer, "materialize_realtime_equity_evidence", prohibited)
    before = len(store.statements)
    with TestClient(app) as client:
        params = {"cohort_id": str(COHORT_ID)}
        publication = client.get("/api/latest-price-date", params=params).json()["publication"]
        params.update(snapshot_id=publication["snapshot_id"], interval="1d")
        response = client.get("/api/tickers/setup-summary", params=params)
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload.pop("publication") == publication
        assert payload == json.loads(canonical_json(dashboard_snapshot().payload["setup_summary"]))
    prohibited.assert_not_called()
    assert all(sql.startswith(("SELECT", "SET")) for sql in store.statements[before:])


@pytest.mark.parametrize("query,status", [
    ({}, 422),
    ({"cohort_id": str(COHORT_ID), "interval": "1h"}, 422),
    ({"cohort_id": str(COHORT_ID), "interval": "1wk"}, 422),
    ({"cohort_id": str(COHORT_ID), "interval": "invalid"}, 422),
    ({"cohort_id": str(UUID(int=8))}, 503),
    ({"cohort_id": str(COHORT_ID), "snapshot_id": str(UUID(int=9))}, 503),
])
def test_unsupported_summary_reads_fail_explicitly(query, status):
    app, _ = dashboard_app()
    with TestClient(app) as client:
        assert client.get("/api/tickers/setup-summary", params=query).status_code == status


@pytest.mark.parametrize("fault", ["missing", "membership", "evidence_hash", "flattened", "clock", "run"])
def test_missing_or_drifted_summary_never_becomes_empty_success(fault):
    app, store = dashboard_app()
    row = next(iter(store.snapshots.values()))
    row["payload"] = deepcopy(row["payload"])
    row["source_manifest"] = deepcopy(row["source_manifest"])
    if fault == "missing":
        del row["payload"]["setup_summary"]
        del row["payload"]["setup_evidence"]
    elif fault == "membership":
        row["payload"]["setup_summary"]["results"].pop()
    elif fault == "evidence_hash":
        row["payload"]["setup_evidence"][0]["payload_sha256"] = "f" * 64
    elif fault == "flattened":
        row["payload"]["setup_summary"]["results"][0]["rsi"] += 1
    elif fault == "clock":
        row["payload"]["setup_evidence"][0]["observed_at"] = (OBSERVED_AT + timedelta(seconds=1)).isoformat()
    else:
        row["payload"]["setup_evidence"][0]["analysis_run_id"] = str(UUID(int=9))
    if fault in {"evidence_hash", "clock", "run"}:
        # Re-bind the manifest identity so the reader's own evidence checks are what reject the drift.
        row["source_manifest"]["setup_evidence_set"] = rows_identity(row["payload"]["setup_evidence"])
    row["payload_sha256"] = canonical_sha256(row["payload"])
    row["source_manifest_sha256"] = canonical_sha256(row["source_manifest"])
    with TestClient(app) as client:
        response = client.get("/api/tickers/setup-summary", params={"cohort_id": str(COHORT_ID)})
        assert response.status_code == 503, response.text
        if fault == "missing":
            assert client.get("/api/latest-price-date", params={"cohort_id": str(COHORT_ID)}).status_code == 200


@pytest.mark.parametrize("days,fresh,stale", [(1, True, 0), (3, False, 2), (10, None, None)])
def test_summary_freshness_preserves_original_setup_and_publication_clocks(days, fresh, stale):
    app, _ = dashboard_app(clock=lambda: OBSERVED_AT + timedelta(days=days))
    with TestClient(app) as client:
        response = client.get("/api/tickers/setup-summary", params={"cohort_id": str(COHORT_ID)})
        if fresh is None:
            assert response.status_code == 503
            return
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["is_fresh"] is fresh
        assert payload["staleness_sessions"] == stale
        assert payload["computed_at"] == OBSERVED_AT.isoformat()
        assert payload["publication"]["generated_at"] == OBSERVED_AT.isoformat()


def test_setup_and_overview_pin_survives_pointer_rotation():
    app, store = dashboard_app()
    original = dashboard_snapshot()
    store.publish(produce_realtime_dashboard(replace(
        dashboard_fixture(), source_generation=4, cutoff=OBSERVED_AT + timedelta(minutes=1),
    )))
    with TestClient(app) as client:
        params = {"cohort_id": str(COHORT_ID), "snapshot_id": str(original.snapshot_id)}
        summary = client.get("/api/tickers/setup-summary", params=params).json()
        overview = client.get("/api/tickers/overview", params=params).json()
        assert summary["publication"]["snapshot_id"] == str(original.snapshot_id)
        assert summary["computed_at"] == OBSERVED_AT.isoformat()
        assert overview == json.loads(canonical_json(original.payload["overview"]))


def test_incomplete_hourly_confirmation_is_unavailable_not_fabricated_agreement():
    inputs = dashboard_fixture()
    missing = next(row for row in inputs.bars if row["ticker"] == "AAPL" and row["interval"] == "1h"
                   and row["session_date"] == inputs.session)
    snapshot = produce_realtime_dashboard(replace(
        inputs, bars=tuple(row for row in inputs.bars if row is not missing),
    ))
    summary = next(row for row in snapshot.payload["setup_summary"]["results"] if row["ticker"] == "AAPL")
    assert summary["trend"] is not None and summary["momentum"] is not None
    assert summary["confirm_trend"] is None and summary["multi_tf_agree"] is None
    assert snapshot.payload["missing_setup_confirmations"] == {
        "AAPL": "EXACT_20_SESSION_1H_CONFIRMATION_UNAVAILABLE",
    }
    assert snapshot.source_manifest["missing_setup_confirmation_set"]["count"] == 1
