from dataclasses import replace
from functools import partial
import json

from fastapi.testclient import TestClient
import pytest

from realtime import stock_alert_projection as projection
from realtime.contracts import canonical_json, canonical_sha256
from realtime.lineage_head import (
    BOUNDED_MANIFEST_BYTES, inputs_bar_lineage_sha256, inputs_sha256, reference_set,
    require_bounded_manifest,
)
from research.stock_idea_forward import forward_config
from research.stock_idea_replay import derive_hours
from tests.realtime.test_equity_validation_history import COHORT_ID
from tests.realtime.test_stock_alert_vertical_slice import (
    alert_app, alert_fixture, alert_snapshot, retained_snapshot, timely_fixture,
)

MANIFEST_KEYS = {
    "version", "cohort_id", "provider_tier", "source_generation", "source_manifest_sha256",
    "input_sha256", "reference_set", "bar_lineage_sha256", "domain_hourly_lineage_sha256",
    "policy_sha256", "producer_sha256", "detector_sha256", "price_basis", "retained_stream_sha256",
}
REMOVED_KEYS = {"references", "source_revision_ids", "source_payload_sha256", "domain_hourly_lineage"}

CASES = {"plain": (alert_snapshot, alert_fixture), "retained": (retained_snapshot, timely_fixture)}


def manifest_size(snapshot):
    return len(canonical_json(snapshot.source_manifest).encode())


@pytest.mark.parametrize("case", CASES)
def test_manifest_is_bounded_with_digests_only(case):
    snapshot, fixture = (build() for build in CASES[case])
    manifest = snapshot.source_manifest
    assert manifest_size(snapshot) <= BOUNDED_MANIFEST_BYTES
    require_bounded_manifest(manifest, name="Stock Alerts")
    assert set(manifest) == MANIFEST_KEYS
    assert not REMOVED_KEYS & set(manifest)
    # Constant size: only scalars and the two-key reference_set identity, never a per-bar or per-member list.
    assert all(isinstance(value, (str, int)) for key, value in manifest.items() if key != "reference_set")
    assert set(manifest["reference_set"]) == {"count", "sha256"}
    text = canonical_json(manifest)
    assert not any(str(row["bar_revision_id"]) in text for row in fixture.bars)
    assert not any(str(row["security_revision_id"]) in text for row in fixture.members)


@pytest.mark.parametrize("case", CASES)
def test_manifest_digests_bind_inputs_and_derived_hourly_lineage(case):
    snapshot, fixture = (build() for build in CASES[case])
    manifest = snapshot.source_manifest
    assert manifest["input_sha256"] == inputs_sha256(fixture)
    assert manifest["bar_lineage_sha256"] == inputs_bar_lineage_sha256(fixture)
    bars, _ = projection.alert_domain_inputs(fixture)
    hourly = {row["revision_id"]: row["source_revision_ids"]
              for row in derive_hours(bars, forward_config(quality_version=2))}
    assert hourly
    assert manifest["domain_hourly_lineage_sha256"] == canonical_sha256(hourly)
    assert manifest["reference_set"] == reference_set(fixture.members)


def test_bar_payload_change_moves_the_input_and_bar_lineage_digests():
    inputs = alert_fixture()
    bars = list(inputs.bars)
    bars[0] = dict(bars[0], payload_sha256="f" * 64)
    changed = replace(inputs, bars=tuple(bars))
    assert inputs_sha256(changed) != inputs_sha256(inputs)
    assert inputs_bar_lineage_sha256(changed) != inputs_bar_lineage_sha256(inputs)


@pytest.mark.parametrize("case", CASES)
def test_references_live_in_the_payload_and_match_the_manifest_identity(case):
    snapshot, fixture = (build() for build in CASES[case])
    payload = json.loads(canonical_json(snapshot.payload))
    assert payload["references"] == json.loads(canonical_json(fixture.members))
    assert len(payload["references"]) == len(fixture.members) == snapshot.source_manifest["reference_set"]["count"]
    assert snapshot.source_manifest["reference_set"] == reference_set(payload["references"])


def test_producer_enforces_the_bounded_manifest_before_building_the_snapshot(monkeypatch):
    seen = []

    def recorder(manifest, **kwargs):
        seen.append((manifest, kwargs))
        return require_bounded_manifest(manifest, **kwargs)

    monkeypatch.setattr(projection, "evaluate_alert_inputs", lambda *args, **kwargs: [])
    monkeypatch.setattr(projection, "require_bounded_manifest", recorder)
    snapshot = projection.produce_realtime_stock_alerts(alert_fixture())
    assert [kwargs for _, kwargs in seen] == [dict(name="Stock Alerts")]
    assert seen[0][0] == snapshot.source_manifest
    monkeypatch.setattr(projection, "require_bounded_manifest", partial(require_bounded_manifest, limit=64))
    with pytest.raises(ValueError, match="must stay within 64"):
        projection.produce_realtime_stock_alerts(alert_fixture())


def rehash(row):
    row["payload_sha256"] = canonical_sha256(row["payload"])
    row["source_manifest_sha256"] = canonical_sha256(row["source_manifest"])


@pytest.mark.parametrize("defect", [
    "none", "reference-field", "dropped-reference", "extra-reference", "revision-swap",
    "missing-references", "manifest-count", "manifest-digest", "missing-identity",
])
def test_reader_binds_payload_references_to_the_manifest_member_set(defect):
    app, store = alert_app()
    row = next(iter(store.snapshots.values()))
    references = row["payload"]["references"]
    if defect == "reference-field":
        references[0]["payload_sha256"] = "f" * 64
    elif defect == "dropped-reference":
        references.pop()
    elif defect == "extra-reference":
        references.append(dict(references[0], ticker="ZZZZ"))
    elif defect == "revision-swap":
        references[0]["security_revision_id"] = references[1]["security_revision_id"]
    elif defect == "missing-references":
        del row["payload"]["references"]
    elif defect == "manifest-count":
        row["source_manifest"]["reference_set"]["count"] += 1
    elif defect == "manifest-digest":
        row["source_manifest"]["reference_set"]["sha256"] = "e" * 64
    elif defect == "missing-identity":
        del row["source_manifest"]["reference_set"]
    rehash(row)
    with TestClient(app) as client:
        response = client.get("/api/stocks/alert-view", params=dict(cohort_id=str(COHORT_ID)))
    assert response.status_code == (200 if defect == "none" else 503), response.text
