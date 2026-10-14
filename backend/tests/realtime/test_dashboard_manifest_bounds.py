"""The Dashboard manifest is a bounded lineage head; per-member lists live in the payload."""
from dataclasses import replace
from uuid import UUID

from fastapi.testclient import TestClient
import pytest

from realtime.contracts import canonical_json, canonical_sha256
from realtime.dashboard_projection import (
    DAILY_SCANNER_CONTRACTS, MEMBER_LISTS, member_set_identities, produce_realtime_dashboard,
)
from realtime.lineage_head import (
    BOUNDED_MANIFEST_BYTES, inputs_bar_lineage_sha256, inputs_sha256, reference_set, rows_identity,
)
from realtime.portal_projection_repository import (
    RETENTION_HOT_AND_KEPT, SNAPSHOT_CLASS_EPHEMERAL, SNAPSHOT_CLASS_SESSION_CLOSE,
    persist_portal_snapshot_cursor,
)
from tests.realtime.portal_v2_support import PortalV2Statements
from tests.realtime.test_dashboard_vertical_slice import (
    COHORT_ID, dashboard_app, dashboard_fixture, dashboard_snapshot,
)


def manifest_size(manifest):
    return len(canonical_json(manifest).encode())


def nested_lists(value):
    if isinstance(value, dict):
        for item in value.values():
            yield from nested_lists(item)
    elif isinstance(value, list):
        yield value
        for item in value:
            yield from nested_lists(item)


class PortalCursor(PortalV2Statements):
    """Persistence-only fake with the real hot manifest cap in force."""

    def __init__(self):
        self.init_portal()
        self.result = []

    def execute(self, sql, params=()):
        self.result = []
        assert self.portal_execute(sql, params), sql

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result


def test_manifest_is_within_the_lineage_head_bound():
    snapshot = dashboard_snapshot()
    assert manifest_size(snapshot.source_manifest) <= BOUNDED_MANIFEST_BYTES


def test_manifest_has_no_per_bar_or_per_member_list_and_carries_digests():
    inputs, snapshot = dashboard_fixture(), dashboard_snapshot()
    manifest, payload = snapshot.source_manifest, snapshot.payload
    for key in ("lineage", *MEMBER_LISTS):
        assert key not in manifest
    assert list(nested_lists(manifest)) == [list(DAILY_SCANNER_CONTRACTS.values())]
    assert manifest["input_sha256"] == inputs_sha256(inputs)
    assert manifest["bar_lineage_sha256"] == inputs_bar_lineage_sha256(inputs)
    assert manifest["screening_generation"] == payload["context"]["cohort"]["generation"]
    assert manifest["expected_members"] == len(payload["tickers"]) == 9


def test_payload_holds_the_member_lists_the_manifest_identifies():
    snapshot = dashboard_snapshot()
    manifest, payload = snapshot.source_manifest, snapshot.payload
    assert manifest["reference_set"] == reference_set(payload["references"])
    assert manifest["reference_set"]["count"] == 9
    assert payload["tickers"] == [row["ticker"] for row in payload["references"]] == sorted(payload["tickers"])
    assert manifest["ticker_set"] == rows_identity(payload["tickers"])
    assert manifest["setup_evidence_set"] == rows_identity(payload["setup_evidence"])
    assert [row["ticker"] for row in payload["setup_evidence"]] == payload["tickers"]
    assert manifest["missing_setup_confirmation_set"] == rows_identity(
        sorted(payload["missing_setup_confirmations"].items()))
    assert {key: manifest[key] for key in member_set_identities(payload)} == member_set_identities(payload)


def test_manifest_size_does_not_grow_with_the_member_count():
    inputs, full = dashboard_fixture(), dashboard_snapshot()
    smaller = produce_realtime_dashboard(replace(
        inputs, members=tuple(row for row in inputs.members if row["ticker"] != "QQQ"),
        bars=tuple(row for row in inputs.bars if row["ticker"] != "QQQ"),
    ))
    assert smaller.source_manifest["reference_set"]["count"] == 8
    assert set(smaller.source_manifest) == set(full.source_manifest)
    assert abs(manifest_size(smaller.source_manifest) - manifest_size(full.source_manifest)) < 64


def test_producer_rejects_a_manifest_beyond_the_bound(monkeypatch):
    import realtime.dashboard_projection as producer
    monkeypatch.setattr(producer, "member_set_identities", lambda lists: dict(blob="x" * BOUNDED_MANIFEST_BYTES))
    with pytest.raises(ValueError, match="lineage-head manifest"):
        produce_realtime_dashboard(dashboard_fixture())


def test_session_close_persists_hot_and_kept_under_the_hot_manifest_cap():
    snapshot = dashboard_snapshot()
    cursor = PortalCursor()
    kept = persist_portal_snapshot_cursor(cursor, snapshot, retention=RETENTION_HOT_AND_KEPT)
    assert kept.source_manifest == snapshot.source_manifest and kept.payload == snapshot.payload
    classes = {row["snapshot_class"]: row for row in cursor.snapshots.values()}
    assert set(classes) == {SNAPSHOT_CLASS_EPHEMERAL, SNAPSHOT_CLASS_SESSION_CLOSE}
    for row in classes.values():
        assert canonical_sha256(row["source_manifest"]) == row["source_manifest_sha256"]
        assert canonical_sha256(row["payload"]) == row["payload_sha256"]
        assert "references" not in row["source_manifest"] and len(row["payload"]["references"]) == 9


@pytest.mark.parametrize("fault", [
    None, "reference_revision", "reference_dropped", "evidence_lineage", "missing_confirmation",
    "tickers", "manifest_identity",
])
def test_reader_rejects_payload_lists_that_drift_from_the_manifest_identities(fault):
    app, store = dashboard_app()
    row = next(iter(store.snapshots.values()))
    payload, manifest = row["payload"], row["source_manifest"]
    if fault == "reference_revision":
        payload["references"][0]["security_revision_id"] = str(UUID(int=7))
    elif fault == "reference_dropped":
        payload["references"].pop()
    elif fault == "evidence_lineage":
        payload["setup_evidence"][0]["source_revision_ids"].pop()
    elif fault == "missing_confirmation":
        payload["missing_setup_confirmations"]["AAPL"] = "DAILY_INPUT_MISSING"
    elif fault == "tickers":
        payload["tickers"] = payload["tickers"][::-1]
    elif fault == "manifest_identity":
        manifest["reference_set"]["count"] += 1
    row["payload_sha256"] = canonical_sha256(payload)
    row["source_manifest_sha256"] = canonical_sha256(manifest)
    with TestClient(app) as client:
        response = client.get("/api/latest-price-date", params={"cohort_id": str(COHORT_ID)})
    assert response.status_code == (200 if fault is None else 503), response.text
