import json
from collections import defaultdict
from math import ceil
from time import perf_counter

from realtime.contracts import canonical_json
from realtime.lineage_head import BOUNDED_MANIFEST_BYTES
import realtime.ticker_projection as ticker_projection
from tests.realtime.test_ticker_vertical_slice import ticker_fixture


def _bytes(value) -> int:
    return len(canonical_json(value).encode("utf-8"))


def _percentile(values, percentile):
    ordered = sorted(values)
    return ordered[max(0, ceil(len(ordered) * percentile) - 1)]


PHASE_FUNCTIONS = (
    "produce_realtime_screening", "derive_realtime_interval_facts", "build_portal_snapshot",
    "canonical_sha256", "_fact", "_chart_point",
)


def _time_phase(monkeypatch, name, totals):
    original = getattr(ticker_projection, name)

    def timed(*args, **kwargs):
        started = perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            totals[name]["calls"] += 1
            totals[name]["seconds"] += perf_counter() - started

    monkeypatch.setattr(ticker_projection, name, timed)


def test_current_realtime_architecture_profile(monkeypatch, record_property):
    inputs = ticker_fixture()
    original = ticker_projection.materialize_realtime_equity_evidence
    calls = []

    def measured_materialization(**kwargs):
        started = perf_counter()
        result = original(**kwargs)
        calls.append({
            "interval": result.interval,
            "primary_bars": len(result.bar_revision_ids),
            "confirmation_bars": len(result.confirmation_bar_revision_ids),
            "evidence_rows": len(result.materialization.evidence),
            "seconds": perf_counter() - started,
        })
        return result

    monkeypatch.setattr(
        ticker_projection,
        "materialize_realtime_equity_evidence",
        measured_materialization,
    )
    phase_totals = {name: {"calls": 0, "seconds": 0.0} for name in PHASE_FUNCTIONS}
    for name in PHASE_FUNCTIONS:
        _time_phase(monkeypatch, name, phase_totals)
    started = perf_counter()
    projection = ticker_projection.project_realtime_ticker_workspace(inputs)
    projection_seconds = perf_counter() - started

    snapshots = projection.snapshots
    section_bytes = defaultdict(int)
    manifest_section_bytes = defaultdict(int)
    manifest_bar_bytes = defaultdict(int)
    manifest_bar_rows = defaultdict(int)
    evidence_index_bytes = 0
    payload_bytes = manifest_bytes = 0
    for snapshot in snapshots:
        payload_bytes += _bytes(snapshot.payload)
        manifest_bytes += _bytes(snapshot.source_manifest)
        for name, value in snapshot.payload.items():
            section_bytes[name] += _bytes(value)
        for name, value in snapshot.source_manifest.items():
            manifest_section_bytes[name] += _bytes(value)
        for key, group in snapshot.source_manifest["bar_lineage"]["groups"].items():
            manifest_bar_bytes[key] += _bytes(group)
            manifest_bar_rows[key] += group["count"]
        evidence_index_bytes += _bytes(snapshot.payload["evidence_index"])

    call_seconds = [row["seconds"] for row in calls]
    calls_by_interval = {
        interval: sum(row["interval"] == interval for row in calls)
        for interval in sorted({row["interval"] for row in calls})
    }
    timing_by_shape = {}
    for row in calls:
        key = f"{row['interval']}:{row['primary_bars']}:{row['confirmation_bars']}"
        timing_by_shape.setdefault(key, []).append(row["seconds"])
    timing_by_shape = {
        key: {
            "calls": len(values),
            "total_seconds": round(sum(values), 6),
            "average_seconds": round(sum(values) / len(values), 6),
            "maximum_seconds": round(max(values), 6),
        }
        for key, values in sorted(timing_by_shape.items())
    }
    ticker_count = len(inputs.members)
    profile = {
        "ticker_count": ticker_count,
        "retained_input_bar_rows": len(inputs.bars),
        "projection_seconds": round(projection_seconds, 6),
        "technical_calls": len(calls),
        "technical_calls_per_ticker": len(calls) / ticker_count,
        "calls_by_interval": calls_by_interval,
        "persisted_materializations": len(projection.materializations),
        "call_seconds": {
            "total": round(sum(call_seconds), 6),
            "average": round(sum(call_seconds) / len(call_seconds), 6),
            "p95": round(_percentile(call_seconds, 0.95), 6),
            "maximum": round(max(call_seconds), 6),
        },
        "timing_by_shape": timing_by_shape,
        "ticker_snapshot_payload_bytes": payload_bytes,
        "ticker_snapshot_manifest_bytes": manifest_bytes,
        "ticker_snapshot_total_bytes": payload_bytes + manifest_bytes,
        "payload_section_bytes": dict(sorted(section_bytes.items())),
        "manifest_section_bytes": dict(sorted(manifest_section_bytes.items())),
        "manifest_bar_bytes_by_interval": dict(sorted(manifest_bar_bytes.items())),
        "manifest_bar_rows_by_interval": dict(sorted(manifest_bar_rows.items())),
        "evidence_index_bytes": evidence_index_bytes,
        "phase_seconds": {
            name: {"calls": row["calls"], "seconds": round(row["seconds"], 6)}
            for name, row in phase_totals.items()
        },
        "linear_400_ticker_projection": {
            "technical_calls_per_portal_cycle": round(len(calls) / ticker_count * 400),
            "compute_seconds_single_process": round(projection_seconds / ticker_count * 400, 3),
            "snapshot_payload_bytes": round(payload_bytes / ticker_count * 400),
            "snapshot_manifest_bytes": round(manifest_bytes / ticker_count * 400),
            "snapshot_total_bytes": round(
                (payload_bytes + manifest_bytes) / ticker_count * 400
            ),
        },
    }
    record_property("realtime_architecture_profile", json.dumps(profile, sort_keys=True))

    assert ticker_count == 9
    assert len(calls) == ticker_count * 12
    assert calls_by_interval == {
        "5m": 9,
        "15m": 9,
        "30m": 18,
        "1h": 18,
        "1d": 18,
        "1wk": 18,
        "1mo": 18,
    }
    assert len(projection.materializations) == ticker_count * 5
    assert all(snapshot.payload["provider_fetch_performed"] is False for snapshot in snapshots)
    assert all(_bytes(snapshot.source_manifest) <= BOUNDED_MANIFEST_BYTES for snapshot in snapshots)
    # By reference: the stored workspace is about 60 KB, not the 1.76 MB it was with embedded bodies.
    assert all(_bytes(snapshot.payload) < 80_000 for snapshot in snapshots)
    assert {"charts", "evidence"}.isdisjoint(section_bytes)
    assert sum(row["count"] for snapshot in snapshots
               for row in snapshot.source_manifest["bar_lineage"]["groups"].values()) == len(inputs.bars)