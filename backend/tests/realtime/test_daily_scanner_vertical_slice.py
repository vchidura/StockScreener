from copy import deepcopy
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from functools import lru_cache
import json
from unittest.mock import MagicMock
from uuid import UUID

from fastapi.testclient import TestClient
import numpy as np
import pandas as pd
import pytest

from equity.portal_scanners import compute_default_scanner_snapshots, default_scanner_analysis
from realtime.contracts import canonical_json, canonical_sha256
from realtime.dashboard_projection import (
    DAILY_SCANNER_CONTRACTS, SCANNER_BASIS, dashboard_frames, produce_realtime_dashboard,
)
from tests.realtime.test_dashboard_vertical_slice import (
    COHORT_ID, OBSERVED_AT, dashboard_app, dashboard_fixture, dashboard_snapshot,
)


@lru_cache(maxsize=2)
def daily_scanner_fixture(no_match=False):
    inputs = dashboard_fixture()
    sessions = sorted({row["session_date"] for row in inputs.bars if row["interval"] == "1d"})
    ordinal = {session: index for index, session in enumerate(sessions)}
    slot_counts = {}
    for row in inputs.bars:
        if row["interval"] == "30m":
            identity = (row["ticker"], row["session_date"])
            slot_counts[identity] = slot_counts.get(identity, 0) + 1
    bars = []
    for original in inputs.bars:
        row = dict(original)
        if no_match or row["ticker"] in {"AAPL", "MSFT", "AMZN"}:
            index = ordinal[row["session_date"]]
            close = 100 + .12 * index + 3 * np.sin(index / 6 + .25)
            if no_match:
                close = 100.
            elif row["ticker"] == "AAPL":
                close = 100 if index < len(sessions) - 3 else 104 + 2 * (index - len(sessions) + 3)
            elif row["ticker"] == "AMZN":
                close = 300 - close
            volume = 3_000_000 if not no_match and row["ticker"] == "AAPL" and index == len(sessions) - 1 else 1_000_000
            if row["interval"] == "30m":
                volume /= slot_counts[(row["ticker"], row["session_date"])]
            row.update(open=close, high=close + 1, low=close - 1, close=close, volume=volume)
            row["payload_sha256"] = canonical_sha256(("synthetic-default-daily", row))
        bars.append(row)
    indexed = {str(row["bar_revision_id"]): row for row in bars}
    for row in sorted(bars, key=lambda row: row["bar_end"] - row["bar_start"]):
        if (no_match or row["ticker"] in {"AAPL", "MSFT", "AMZN"}) and row["source_kind"] == "DERIVED":
            sources = sorted((indexed[str(identity)] for identity in row["source_revision_ids"]),
                             key=lambda source: source["bar_start"])
            row.update(open=sources[0]["open"], high=max(source["high"] for source in sources),
                       low=min(source["low"] for source in sources), close=sources[-1]["close"],
                       volume=sum(source["volume"] for source in sources))
            row["payload_sha256"] = canonical_sha256(("synthetic-default-daily-derived", row))
    return replace(inputs, bars=tuple(bars))


@lru_cache(maxsize=1)
def daily_scanner_snapshot():
    return produce_realtime_dashboard(daily_scanner_fixture())


def test_one_real_producer_pass_retains_all_six_original_contracts(monkeypatch):
    import realtime.dashboard_projection as producer
    compute = MagicMock(wraps=compute_default_scanner_snapshots)
    monkeypatch.setattr(producer, "compute_default_scanner_snapshots", compute)
    snapshot = produce_realtime_dashboard(daily_scanner_fixture())
    compute.assert_called_once()
    frames, _ = dashboard_frames(daily_scanner_fixture())
    expected = compute_default_scanner_snapshots(sorted(frames), frames, {key: frames[key] for key in ("SPY", "QQQ")},
                                                observed_at=OBSERVED_AT)
    assert set(snapshot.payload["scanners"]) == set(DAILY_SCANNER_CONTRACTS.values())
    for key in DAILY_SCANNER_CONTRACTS.values():
        result = snapshot.payload["scanners"][key]
        assert canonical_json(result) == canonical_json(expected[key])
        assert result["total_scanned"] == 9
        assert result["total_signals"] == len(result["results"]) > 0, key
        assert result["interval"] == "1d"
    assert snapshot.payload["scanners"]["SCAN_MOMENTUM_1D"]["results"][0]["ticker"] == "MSFT"
    assert snapshot.payload["scanners"]["SCAN_BEARISH_1D"]["results"][0]["ticker"] == "AMZN"
    assert {name: snapshot.payload["scanners"][key]["total_signals"] for name, key in DAILY_SCANNER_CONTRACTS.items()} == {
        "gaps": 1, "fvg": 4, "ma-crossover": 9, "momentum-pullback": 1, "bearish-bounce": 2, "fibonacci": 4,
    }
    assert snapshot.payload["scanner_basis"] == snapshot.source_manifest["scanner_basis"] == SCANNER_BASIS
    assert snapshot.payload["scan"] == expected["SCAN_ALL_1D_5"]
    assert "fvg" not in snapshot.payload["scan"]


@pytest.mark.parametrize("bars", [0, 19, 210])
def test_analysis_distinguishes_missing_short_and_completed_no_match(bars):
    close = np.full(bars, 100.)
    frame = pd.DataFrame(dict(open=close, high=close + 1, low=close - 1, close=close,
                              volume=np.full(bars, 1_000_000)),
                         index=pd.bdate_range(end="2026-10-02", periods=bars))
    frames = {"EMPTY": frame} if bars else {}
    results = compute_default_scanner_snapshots(["EMPTY"], frames, {}, observed_at=OBSERVED_AT)
    frozen = canonical_json(results)
    analysis = default_scanner_analysis(["EMPTY"], frames, results)
    for key in DAILY_SCANNER_CONTRACTS.values():
        assert results[key]["total_scanned"] == 1
        matches = 1 if bars == 210 and key == "SCAN_MA_1D_9_21" else 0
        assert results[key]["total_signals"] == matches
        expected = ("UNAVAILABLE" if bars == 0 else "INSUFFICIENT_HISTORY" if bars == 19
                    else "COMPLETE" if matches else "COMPLETE_EMPTY")
        assert analysis[key]["status"] == expected
        assert analysis[key]["total_evaluated"] == (1 if bars == 210 else 0)
        assert analysis[key]["total_unavailable"] == (0 if bars == 210 else 1)
        assert analysis[key]["members"][0]["available_bars"] == bars
    assert canonical_json(results) == frozen

@pytest.mark.parametrize("bars", [20, 49, 50])
def test_default_fvg_coverage_matches_actual_lookback_warmup(bars):
    close = np.full(bars, 100.)
    frame = pd.DataFrame(dict(open=close, high=close + 1, low=close - 1, close=close,
                              volume=np.full(bars, 1_000_000)),
                         index=pd.bdate_range(end="2026-10-02", periods=bars))
    frames = {"EMPTY": frame}
    snapshots = compute_default_scanner_snapshots(["EMPTY"], frames, {}, observed_at=OBSERVED_AT)
    analysis = default_scanner_analysis(["EMPTY"], frames, snapshots)
    fvg = analysis["SCAN_FVG_1D_50"]
    assert fvg["status"] == ("COMPLETE_EMPTY" if bars == 50 else "INSUFFICIENT_HISTORY")
    assert fvg["total_evaluated"] == (1 if bars == 50 else 0)
    assert fvg["members"][0]["required_bars"] == 50
    assert fvg["members"][0]["reason"] == (None if bars == 50 else "DAILY_WARMUP_NOT_MET")
    assert snapshots["SCAN_FVG_1D_50"]["results"] == []
    assert analysis["SCAN_GAPS_1D"]["status"] == "COMPLETE_EMPTY"


@pytest.mark.parametrize("bars", [199, 200])
def test_hourly_strategy_engines_use_native_warmup_and_independent_contracts(bars, monkeypatch):
    import equity.portal_scanners as producer
    from screeners import calculate_fibonacci_swing_pct, scan_fibonacci, scan_gap_strategies
    frame = pd.DataFrame(dict(open=np.full(bars, 100.), high=np.full(bars, 101.), low=np.full(bars, 99.),
        close=np.full(bars, 100.), volume=np.full(bars, 100000)), index=pd.date_range("2026-08-01", periods=bars, freq="h"))
    gap = MagicMock(wraps=scan_gap_strategies)
    fib = MagicMock(wraps=scan_fibonacci)
    monkeypatch.setattr(producer, "scan_gap_strategies", gap)
    monkeypatch.setattr(producer, "scan_fibonacci", fib)
    snapshots = producer.compute_default_scanner_snapshots(["AAPL"], {"AAPL": frame}, {}, observed_at=OBSERVED_AT, interval="1h")
    analysis = producer.default_scanner_analysis(["AAPL"], {"AAPL": frame}, snapshots, interval="1h")
    assert set(snapshots) == set(producer.HOURLY_SCANNER_MINIMUM_BARS)
    assert all(row["interval"] == "1h" for row in snapshots.values())
    assert analysis["SCAN_MOMENTUM_1H"]["members"][0]["required_bars"] == 200
    assert analysis["SCAN_MOMENTUM_1H"]["total_evaluated"] == (1 if bars == 200 else 0)
    gap.assert_called_once_with("AAPL", frame, interval="1h")
    assert fib.call_args.args[2] == calculate_fibonacci_swing_pct(frame, "1h")
    assert snapshots["SCAN_FIBONACCI_1H_ATR"]["min_swing_pct"] is None


def test_historical_hourly_operator_scanner_hashes_survive_jsonb_signed_zero_roundtrip(monkeypatch):
    import math
    import realtime.strategy_projection as producer
    from equity.materialization import _json_safe
    compute = producer.compute_default_scanner_snapshots

    def signed_zero_scanners(*args, **kwargs):
        scanners = compute(*args, **kwargs)
        moving_average = scanners["SCAN_MA_1H_9_21"]
        moving_average["results"][0]["ma_spread_pct"] = -0.0
        return scanners

    monkeypatch.setattr(producer, "compute_default_scanner_snapshots", signed_zero_scanners)
    snapshot = producer.project_realtime_strategy_history(persistence_history_inputs(), interval="1h").snapshot
    assert canonical_sha256(_json_safe(snapshot.payload)) == snapshot.payload_sha256
    decoded_manifest = json.loads(canonical_json(snapshot.source_manifest))
    assert canonical_sha256(_json_safe(decoded_manifest)) == snapshot.source_manifest_sha256
    for item in snapshot.source_manifest["history"]:
        spread = item["payload"]["scanners"]["SCAN_MA_1H_9_21"]["results"][0]["ma_spread_pct"]
        assert spread == 0 and math.copysign(1, spread) == 1


def test_historical_hourly_operator_assigns_distinct_feature_generations_per_session():
    from realtime.strategy_projection import project_realtime_strategy_history
    from realtime.strategy_repository import _strategy_materialization_writes, _validate_strategy_snapshot
    projection = project_realtime_strategy_history(persistence_history_inputs(), interval="1h")
    writes = _strategy_materialization_writes(projection)
    expected = list(range(persistence_history_inputs().source_generation, persistence_history_inputs().source_generation + 5))
    assert [write.generation for write in writes] == expected
    assert projection.snapshot.source_generation == expected[-1]
    assert all(write.update_evidence_current is False for write in writes)
    identities = [(write.result.cohort_id, write.result.instrument_id, write.definition.bundle_sha256, write.generation)
                  for write in writes]
    assert len(set(identities)) == len(writes)
    _validate_strategy_snapshot(projection.snapshot.source_manifest, projection.snapshot.payload)


def test_full_exact_cohort_no_match_publication_is_not_missing_data():
    snapshot = produce_realtime_dashboard(daily_scanner_fixture(no_match=True))
    app, _ = dashboard_app(snapshot=snapshot)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(snapshot.snapshot_id))
        for contract in DAILY_SCANNER_CONTRACTS:
            response = client.get(f"/api/scan/{contract}", params=params)
            assert response.status_code == 200, response.text
            scanner = response.json()
            assert scanner["total_scanned"] == 9
            assert scanner["total_signals"] == (9 if contract == "ma-crossover" else 0)
            if contract != "ma-crossover":
                assert scanner["results"] == []


def test_independent_strategy_release_has_exact_full_window_receipt_parity():
    from equity.domain import EvidenceType
    from realtime.strategy_projection import project_realtime_daily_strategies, STRATEGY_VERSION
    inputs = daily_scanner_fixture()
    projection = strategy_projection()
    snapshot = projection.snapshot
    assert snapshot.snapshot_type == STRATEGY_VERSION
    assert snapshot.snapshot_id != daily_scanner_snapshot().snapshot_id
    assert snapshot.payload["scanners"] == daily_scanner_snapshot().payload["scanners"]
    assert len(projection.materializations) == 9
    for result in projection.materializations:
        ticker = result.materialization.evidence[0].ticker
        daily_ids = tuple(row["bar_revision_id"] for row in sorted(inputs.bars, key=lambda row: row["bar_start"])
                          if row["ticker"] == ticker and row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED")
        assert result.bar_revision_ids == daily_ids
        for row in result.materialization.evidence:
            if row.evidence_type is EvidenceType.SCANNER_RESULT and row.source_version == STRATEGY_VERSION:
                receipt = json.loads(row.payload_json)
                assert receipt == snapshot.source_manifest["receipts"][ticker][row.source_name]
                assert row.source_revision_ids == daily_ids
                assert receipt["results"] == json.loads(canonical_json([
                    item for item in snapshot.payload["scanners"][row.source_name]["results"] if item["ticker"] == ticker]))
                assert row.payload_sha256 == canonical_sha256(receipt)
                assert snapshot.source_manifest["evidence_refs"][ticker][row.source_name]["evidence_id"] == str(row.evidence_id)


def test_reconstructed_history_uses_five_truncated_sessions_and_actual_observation_clocks():
    import exchange_calendars
    from realtime.strategy_projection import project_realtime_strategy_history, HISTORY_VERSION
    inputs = persistence_history_inputs()
    projection = persistence_history_projection()
    snapshot = projection.snapshot
    assert snapshot.snapshot_type == HISTORY_VERSION
    assert snapshot.payload["interpretation"] == "RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS"
    assert len(snapshot.payload["evaluation_sessions"]) == 5
    indexed = {str(row["bar_revision_id"]): row for row in inputs.bars}
    calendar = exchange_calendars.get_calendar("XNYS")
    from realtime.strategy_repository import _validate_release
    for item in snapshot.source_manifest["history"]:
        manifest, payload = item["source_manifest"], item["payload"]
        _validate_release(manifest, payload)
        close = calendar.session_close(item["session"]).to_pydatetime()
        assert payload["generated_at"] == inputs.cutoff.isoformat()
        for ticker, window in manifest["source_windows"].items():
            assert all(indexed[identity]["bar_end"] <= close for identity in window["primary_bar_revision_ids"])
            assert len(window["primary_bar_revision_ids"]) >= 210
            assert payload["analysis"]["SCAN_MOMENTUM_1D"]["total_evaluated"] == len(inputs.members)
            assert payload["scanners"]["SCAN_MOMENTUM_1D"]["results"] == []
    assert all(evidence.observed_at == inputs.cutoff for result in projection.materializations
               for evidence in result.materialization.evidence)


@lru_cache(maxsize=1)
def persistence_history_inputs():
    inputs = daily_scanner_fixture(no_match=True)
    return replace(inputs, members=tuple(row for row in inputs.members if row["ticker"] == "AAPL"),
                   bars=tuple(row for row in inputs.bars if row["ticker"] == "AAPL"))


@lru_cache(maxsize=1)
def persistence_history_projection():
    from realtime.strategy_projection import project_realtime_strategy_history
    return project_realtime_strategy_history(persistence_history_inputs())


def test_rolling_strategy_reuses_prior_outcomes_without_reconstruction(monkeypatch):
    import realtime.strategy_projection as producer
    inputs = persistence_history_inputs()
    retained = deepcopy(persistence_history_projection().snapshot.source_manifest["history"][:-1])
    frozen = canonical_json(retained)
    compute = MagicMock(wraps=producer.project_realtime_daily_strategies)
    monkeypatch.setattr(producer, "project_realtime_daily_strategies", compute)
    monkeypatch.setattr(producer, "project_realtime_strategy_history", MagicMock(side_effect=AssertionError("No reconstruction")))
    projection = producer.project_realtime_rolling_strategies(inputs, retained_history=retained)
    compute.assert_called_once()
    assert projection.snapshot.snapshot_type == producer.ROLLING_VERSION
    assert canonical_json(projection.snapshot.source_manifest["history"][:-1]) == frozen
    assert canonical_json(retained) == frozen
    assert len(projection.materializations) == 1
    assert projection.snapshot.payload["observed_sessions"] == projection.snapshot.payload["evaluation_sessions"]
    assert list(projection.snapshot.payload["session_provenance"].values()) == [
        "RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS"] * 4 + ["STORED_EVALUATION"]
    missing = producer.project_realtime_rolling_strategies(inputs, retained_history=retained[:1])
    row = missing.snapshot.payload["persistence"]["rows"][0]
    assert row["evaluated"] == 2
    assert [cell["state"] for cell in row["sessions"]] == ["NO_MATCH", "UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE", "NO_MATCH"]
    from realtime.strategy_repository import _validate_strategy_snapshot
    _validate_strategy_snapshot(projection.snapshot.source_manifest, projection.snapshot.payload)
    _validate_strategy_snapshot(missing.snapshot.source_manifest, missing.snapshot.payload)


@pytest.mark.parametrize("interval", ["1d", "1h"])
@pytest.mark.parametrize("changed_identity", [False, True])
def test_rolling_strategy_compares_json_reference_ids_with_database_uuids(interval, changed_identity):
    import realtime.strategy_projection as producer
    inputs = persistence_history_inputs()
    prior = producer.project_realtime_strategy_history(inputs, interval=interval)
    retained = json.loads(canonical_json(prior.snapshot.source_manifest["history"][:-1]))
    frozen = canonical_json(retained)
    inputs = replace(inputs, members=tuple(
        dict(member, security_id=UUID(str(member["security_id"]))) for member in inputs.members))
    if changed_identity:
        retained[0]["source_manifest"]["references"][0]["security_id"] = str(UUID(int=99))
        with pytest.raises(ValueError):
            producer.project_realtime_rolling_strategies(inputs, retained_history=retained, interval=interval)
        return
    projection = producer.project_realtime_rolling_strategies(inputs, retained_history=retained, interval=interval)
    from realtime.strategy_repository import _validate_strategy_snapshot
    _validate_strategy_snapshot(projection.snapshot.source_manifest, projection.snapshot.payload)
    _validate_strategy_snapshot(
        json.loads(canonical_json(projection.snapshot.source_manifest)),
        json.loads(canonical_json(projection.snapshot.payload)))
    assert canonical_json(projection.snapshot.source_manifest["history"][:-1]) == frozen
    assert canonical_json(retained) == frozen
    assert projection.snapshot.payload["observed_sessions"] == projection.snapshot.payload["evaluation_sessions"]


@pytest.mark.parametrize("interval", ["1d", "1h"])
def test_strategy_history_assembly_preserves_latest_evaluation_and_original_clocks(interval, monkeypatch):
    import realtime.strategy_projection as producer
    from realtime.strategy_repository import _validate_strategy_snapshot
    inputs = persistence_history_inputs()
    prior = producer.project_realtime_strategy_history(inputs, interval=interval)
    latest = producer.project_realtime_daily_strategies(inputs, interval=interval)
    frozen = canonical_json(latest.snapshot)
    published_at = inputs.cutoff + timedelta(minutes=5)
    prohibited = MagicMock(side_effect=AssertionError("History assembly cannot recalculate October 16"))
    monkeypatch.setattr(producer, "project_realtime_daily_strategies", prohibited)
    snapshot = producer.assemble_realtime_strategy_history(latest.snapshot,
        retained_history=prior.snapshot.source_manifest["history"][:-1],
        published_at=published_at, source_generation=inputs.source_generation + 10)
    assert canonical_json(latest.snapshot) == frozen
    assert snapshot.payload["scanners"] == latest.snapshot.payload["scanners"]
    assert snapshot.payload["evaluation_generated_at"] == latest.snapshot.payload["generated_at"]
    assert snapshot.generated_at == published_at
    assert snapshot.source_manifest["history"][-1]["payload"] == latest.snapshot.payload
    assert all(row["evaluated"] == 5 for row in snapshot.payload["persistence"]["rows"])
    _validate_strategy_snapshot(snapshot.source_manifest, snapshot.payload)
    with pytest.raises(ValueError, match="clock/generation"):
        producer.assemble_realtime_strategy_history(latest.snapshot,
            published_at=inputs.cutoff - timedelta(seconds=1), source_generation=inputs.source_generation + 10)
    prohibited.assert_not_called()


def test_rolling_strategy_source_release_compares_database_json_with_native_reference_types():
    import realtime.strategy_projection as producer
    from realtime.strategy_repository import _validate_rolling_sources
    inputs = persistence_history_inputs()
    inputs = replace(inputs, members=tuple(
        dict(member, security_id=UUID(str(member["security_id"]))) for member in inputs.members))
    prior = producer.project_realtime_strategy_history(inputs)
    retained = deepcopy(prior.snapshot.source_manifest["history"][:-1])
    for entry in retained:
        entry["source_release"] = dict(snapshot_id=str(prior.snapshot.snapshot_id),
            source_generation=prior.snapshot.source_generation,
            source_manifest_sha256=prior.snapshot.source_manifest_sha256,
            payload_sha256=prior.snapshot.payload_sha256, generated_at=prior.snapshot.generated_at.isoformat())
    snapshot = producer.project_realtime_rolling_strategies(inputs, retained_history=retained).snapshot
    cursor = MagicMock()
    cursor.fetchone.return_value = dict(snapshot_id=prior.snapshot.snapshot_id, cohort_id=inputs.cohort_id,
        source_generation=prior.snapshot.source_generation,
        source_manifest=json.loads(canonical_json(prior.snapshot.source_manifest)),
        source_manifest_sha256=prior.snapshot.source_manifest_sha256,
        payload=json.loads(canonical_json(prior.snapshot.payload)), payload_sha256=prior.snapshot.payload_sha256,
        generated_at=prior.snapshot.generated_at)
    _validate_rolling_sources(cursor, snapshot.source_manifest, known_by=inputs.cutoff)
    changed = deepcopy(snapshot.source_manifest)
    changed["history"][0]["payload"]["scope_note"] += " tampered"
    with pytest.raises(ValueError, match="differs from immutable source release"):
        _validate_rolling_sources(cursor, changed, known_by=inputs.cutoff)


@pytest.mark.parametrize("interval", ["1d", "1h"])
def test_strategy_session_selection_uses_exact_stored_data_without_production(interval, monkeypatch):
    import realtime.strategy_projection as producer
    import realtime.strategy_repository as repository
    inputs = persistence_history_inputs()
    prior = producer.project_realtime_strategy_history(inputs, interval=interval)
    latest = producer.project_realtime_daily_strategies(inputs, interval=interval)
    published_at = inputs.cutoff + timedelta(minutes=5)
    retained = deepcopy(prior.snapshot.source_manifest["history"][:-1])
    for entry in retained:
        entry["source_release"] = dict(snapshot_id=str(prior.snapshot.snapshot_id),
            source_generation=prior.snapshot.source_generation,
            source_manifest_sha256=prior.snapshot.source_manifest_sha256,
            payload_sha256=prior.snapshot.payload_sha256, generated_at=prior.snapshot.generated_at.isoformat())
    snapshot = producer.assemble_realtime_strategy_history(latest.snapshot,
        retained_history=retained,
        published_at=published_at, source_generation=inputs.source_generation + 10)
    projection = producer.DailyStrategyProjection(snapshot, (*prior.materializations, *latest.materializations))
    app, store = strategy_app(projection, clock=lambda: published_at)
    _, prior_store = strategy_app(prior)
    store.snapshots.update(prior_store.snapshots)
    store.evidence.update(prior_store.evidence)
    store.dependencies.update(prior_store.dependencies)
    prohibited = MagicMock(side_effect=AssertionError("GET must remain stored-only"))
    monkeypatch.setattr(repository, "project_realtime_daily_strategies", prohibited)
    monkeypatch.setattr(repository, "project_realtime_strategy_history", prohibited)
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(snapshot.snapshot_id), interval=interval, read_mode="retained")
    before = len(store.statements)
    with TestClient(app) as client:
        listed = client.get("/api/strategies/daily", params=dict(params, view="sessions"))
        assert listed.status_code == 200, listed.text
        assert listed.json()["sessions"] == snapshot.payload["evaluation_sessions"]
        for entry in snapshot.source_manifest["history"]:
            session = entry["session"]
            for strategy in DAILY_SCANNER_CONTRACTS:
                response = client.get("/api/strategies/daily", params=dict(params, session_date=session, strategy=strategy))
                assert response.status_code == 200, response.text
                result = response.json()
                publication = result["publication"]
                contract = DAILY_SCANNER_CONTRACTS[strategy]
                if interval == "1h":
                    from equity.portal_scanners import HOURLY_SCANNER_CONTRACTS
                    contract = HOURLY_SCANNER_CONTRACTS[contract]
                assert publication["session"] == publication["evaluation_session"] == session
                assert publication["snapshot_id"] == str(snapshot.snapshot_id)
                assert publication["generated_at"] == published_at.isoformat()
                assert publication["evaluation_generated_at"] == entry["payload"]["generated_at"]
                assert result["data"]["results"] == json.loads(canonical_json(entry["payload"]["scanners"][contract]["results"]))
            persistence = client.get("/api/strategies/daily", params=dict(params, session_date=session, view="persistence"))
            assert persistence.status_code == 200, persistence.text
            assert persistence.json()["evaluation_sessions"][-1] == session
            assert persistence.json()["observed_sessions"][-1] == session
        assert client.get("/api/strategies/daily", params=dict(params, session_date="2026-09-18")).status_code == 503
        for invalid in ("2026-10-03", "not-a-date", ""):
            assert client.get("/api/strategies/daily", params=dict(params, session_date=invalid)).status_code == 422
    assert all(statement.startswith(("SELECT", "SET")) for statement in store.statements[before:])
    prohibited.assert_not_called()


def test_rolling_strategy_stored_read_validates_prior_evidence_and_exact_provenance(monkeypatch):
    import realtime.strategy_projection as producer
    import realtime.strategy_repository as repository
    prior = persistence_history_projection()
    retained = deepcopy(prior.snapshot.source_manifest["history"][:-1])
    for item in retained:
        item["source_release"] = dict(snapshot_id=str(prior.snapshot.snapshot_id), source_generation=prior.snapshot.source_generation,
            source_manifest_sha256=prior.snapshot.source_manifest_sha256, payload_sha256=prior.snapshot.payload_sha256,
            generated_at=prior.snapshot.generated_at.isoformat())
    projection = producer.project_realtime_rolling_strategies(persistence_history_inputs(), retained_history=retained)
    app, store = strategy_app(projection)
    _, prior_store = strategy_app(prior)
    prohibited = MagicMock(side_effect=AssertionError("GET must remain stored-only"))
    monkeypatch.setattr(repository, "project_realtime_rolling_strategies", prohibited)
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(projection.snapshot.snapshot_id))
    with TestClient(app) as client:
        assert client.get("/api/strategies/daily", params=dict(params, view="persistence")).status_code == 503
        store.snapshots.update(prior_store.snapshots)
        store.evidence.update(prior_store.evidence)
        store.dependencies.update(prior_store.dependencies)
        response = client.get("/api/strategies/daily", params=dict(params, view="persistence"))
        assert response.status_code == 200, response.text
        assert len(response.json()["observed_sessions"]) == 5
        assert list(response.json()["session_provenance"].values())[-1] == "STORED_EVALUATION"
        selected = client.get("/api/strategies/daily", params=dict(params, ticker="AAPL", evaluation_session=projection.snapshot.payload["session"]))
        assert selected.status_code == 200, selected.text
        assert selected.json()["interpretation"] == "STORED_EVALUATION"
    prohibited.assert_not_called()


def test_rolling_strategy_loader_is_bounded_read_only_and_preserves_outcome_clocks(monkeypatch):
    from realtime.strategy_repository import load_retained_strategy_history_cursor
    import realtime.strategy_repository as repository
    projection = persistence_history_projection()
    _, store = strategy_app(projection)
    cursor = MagicMock()
    stored = next(iter(store.snapshots.values()))
    cursor.fetchall.return_value = [stored]
    durable = MagicMock()
    monkeypatch.setattr(repository, "_validate_durable", durable)
    retained = load_retained_strategy_history_cursor(cursor, persistence_history_inputs())
    assert len(retained) == 4
    assert [item["payload"]["generated_at"] for item in retained] == [projection.snapshot.generated_at.isoformat()] * 4
    assert all(item["source_release"]["snapshot_id"] == str(projection.snapshot.snapshot_id) for item in retained)
    statement, params = cursor.execute.call_args.args
    assert statement.startswith("SELECT DISTINCT ON") and "LIMIT 15" in statement
    assert "snapshot.snapshot_class='SESSION_CLOSE'" in statement and "snapshot.session_date BETWEEN" in statement
    assert params[0] == str(COHORT_ID)
    assert durable.call_count == 4


def test_kept_rolling_strategy_release_reads_without_sources_or_evidence_and_still_verifies_hashes():
    import realtime.strategy_projection as producer
    from realtime.portal_projection_repository import RETENTION_KEPT
    prior = persistence_history_projection()
    retained = deepcopy(prior.snapshot.source_manifest["history"][:-1])
    for item in retained:
        item["source_release"] = dict(snapshot_id=str(prior.snapshot.snapshot_id), source_generation=prior.snapshot.source_generation,
            source_manifest_sha256=prior.snapshot.source_manifest_sha256, payload_sha256=prior.snapshot.payload_sha256,
            generated_at=prior.snapshot.generated_at.isoformat())
    projection = producer.project_realtime_rolling_strategies(persistence_history_inputs(), retained_history=retained)
    app, store = strategy_app(projection, retention=RETENTION_KEPT)
    snapshot_id = str(projection.snapshot.snapshot_id)
    assert store.snapshots[snapshot_id]["snapshot_class"] == "SESSION_CLOSE"
    store.evidence.clear()
    store.dependencies.clear()
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=snapshot_id, view="persistence")
    start = len(store.statements)
    with TestClient(app) as client:
        for mode in ("current", "retained"):
            response = client.get("/api/strategies/daily", params=dict(params, read_mode=mode))
            assert response.status_code == 200, response.text
            assert len(response.json()["observed_sessions"]) == 5
    assert not any("equity_evidence" in sql or "SELECT snapshot.snapshot_id, snapshot.cohort_id, snapshot.source_generation" in sql
                   for sql in store.statements[start:])
    app_tampered, store_tampered = strategy_app(projection, retention=RETENTION_KEPT)
    store_tampered.snapshots[snapshot_id]["payload"]["scope_note"] = "tampered"
    with TestClient(app_tampered) as client:
        assert client.get("/api/strategies/daily", params=params).status_code in {422, 503}


def test_rolling_strategy_due_producer_uses_exact_cutoffs_and_evidence_first_generations(monkeypatch):
    import realtime.strategy_repository as repository
    compute, publication = MagicMock(), MagicMock()
    cursor = MagicMock()
    compute.transaction.return_value.__enter__.return_value = cursor
    cursor.fetchone.side_effect = [dict(provider_tier="MASSIVE_ADVANCED"), None, dict(generation=8), None, dict(generation=9)]
    publish = MagicMock(return_value=MagicMock(snapshot_id=UUID(int=11)))
    monkeypatch.setattr(repository, "materialize_realtime_daily_strategies", publish)
    outcomes = repository.materialize_due_realtime_strategies(compute, publication, cohort_id=COHORT_ID, available_by=OBSERVED_AT)
    assert [item["interval"] for item in outcomes] == ["1d", "1h"]
    assert publish.call_count == 2
    assert [call.kwargs["source_generation"] for call in publish.call_args_list] == [8, 9]
    assert all(call.kwargs["rolling_history"] for call in publish.call_args_list)
    assert publish.call_args_list[0].kwargs["market_time"] is None
    assert publish.call_args_list[1].kwargs["market_time"] <= OBSERVED_AT
    assert all(call.kwargs["read_only"] for call in compute.transaction.call_args_list)
    assert any("rt.feature_snapshots" in call.args[0] for call in cursor.execute.call_args_list)


def test_rolling_strategy_due_producer_loads_under_the_two_clocks_of_its_window(monkeypatch):
    import realtime.strategy_repository as repository
    compute, publication, cursor = MagicMock(), MagicMock(), MagicMock()
    compute.transaction.return_value.__enter__.return_value = cursor
    cursor.fetchone.side_effect = [dict(provider_tier="MASSIVE_ADVANCED"), None, dict(generation=8), None, dict(generation=9)]
    publish = MagicMock(return_value=MagicMock(snapshot_id=UUID(int=11)))
    monkeypatch.setattr(repository, "materialize_realtime_daily_strategies", publish)
    deadline = OBSERVED_AT + timedelta(minutes=10)
    repository.materialize_due_realtime_strategies(compute, publication, cohort_id=COHORT_ID, available_by=OBSERVED_AT,
                                                   known_by=deadline)
    # The cutoff stays the market boundary; the recording clock the loader and the validators use is known_by.
    assert all(call.kwargs["cutoff"] == OBSERVED_AT and call.kwargs["known_by"] == deadline
               for call in publish.call_args_list)
    publish.reset_mock()
    cursor.fetchone.side_effect = [dict(provider_tier="MASSIVE_ADVANCED"), None, dict(generation=8), None, dict(generation=9)]
    repository.materialize_due_realtime_strategies(compute, publication, cohort_id=COHORT_ID, available_by=OBSERVED_AT)
    assert all("known_by" not in call.kwargs for call in publish.call_args_list)  # the single-clock default
    with pytest.raises(ValueError, match="known_by must not precede the cutoff"):
        repository.materialize_due_realtime_strategies(compute, publication, cohort_id=COHORT_ID,
                                                       available_by=OBSERVED_AT, known_by=OBSERVED_AT - timedelta(seconds=1))


def test_rolling_strategy_due_producer_validates_a_stored_cutoff_as_recorded_by_known_by(monkeypatch):
    import realtime.strategy_repository as repository
    import realtime.strategy_projection as producer
    snapshot = producer.project_realtime_rolling_strategies(persistence_history_inputs()).snapshot
    compute, publication, cursor = MagicMock(), MagicMock(), MagicMock()
    compute.transaction.return_value.__enter__.return_value = cursor
    cursor.fetchone.side_effect = [dict(provider_tier="MASSIVE_ADVANCED"), dict(snapshot_id=snapshot.snapshot_id,
        source_manifest=snapshot.source_manifest, payload=snapshot.payload,
        source_manifest_sha256=snapshot.source_manifest_sha256, payload_sha256=snapshot.payload_sha256)]
    rolling, durable = MagicMock(), MagicMock()
    monkeypatch.setattr(repository, "_validate_rolling_sources", rolling)
    monkeypatch.setattr(repository, "_validate_durable", durable)
    deadline = OBSERVED_AT + timedelta(minutes=10)
    (outcome,) = repository.materialize_due_realtime_strategies(
        compute, publication, cohort_id=COHORT_ID, available_by=OBSERVED_AT, intervals=("1d",), known_by=deadline)
    assert outcome["status"] == "ALREADY_STORED" and outcome["analysis"]
    assert rolling.call_args.kwargs["known_by"] == deadline
    assert all(call.kwargs["known_by"] == deadline for call in durable.call_args_list) and durable.call_count


def test_rolling_strategy_due_producer_skips_durable_existing_cutoff(monkeypatch):
    import realtime.strategy_repository as repository
    import realtime.strategy_projection as producer
    projection = producer.project_realtime_rolling_strategies(persistence_history_inputs())
    snapshot = projection.snapshot
    compute, publication, cursor = MagicMock(), MagicMock(), MagicMock()
    compute.transaction.return_value.__enter__.return_value = cursor
    cursor.fetchone.side_effect = [dict(provider_tier="MASSIVE_ADVANCED"), dict(snapshot_id=snapshot.snapshot_id,
        source_manifest=snapshot.source_manifest, payload=snapshot.payload,
        source_manifest_sha256=snapshot.source_manifest_sha256, payload_sha256=snapshot.payload_sha256)]
    monkeypatch.setattr(repository, "_validate_rolling_sources", MagicMock())
    monkeypatch.setattr(repository, "_validate_durable", MagicMock())
    publish = MagicMock(side_effect=AssertionError("Existing session must not rebuild"))
    monkeypatch.setattr(repository, "materialize_realtime_daily_strategies", publish)
    outcomes = repository.materialize_due_realtime_strategies(compute, publication, cohort_id=COHORT_ID,
        available_by=OBSERVED_AT, intervals=("1d",))
    assert outcomes[0]["status"] == "ALREADY_STORED"
    publish.assert_not_called()


def test_rolling_strategy_next_day_retains_yesterdays_actual_outcome(monkeypatch):
    import exchange_calendars
    import realtime.strategy_projection as producer
    inputs = persistence_history_inputs()
    calendar = exchange_calendars.get_calendar("XNYS")
    previous_session = calendar.previous_session(str(inputs.session)).date()
    close = calendar.session_close(str(previous_session)).to_pydatetime()
    yesterday_inputs = replace(inputs, session=previous_session, bars=tuple(row for row in inputs.bars if row["bar_end"] <= close))
    bootstrap = persistence_history_projection().snapshot.source_manifest["history"]
    yesterday = producer.project_realtime_rolling_strategies(yesterday_inputs,
        retained_history=[item for item in bootstrap if item["session"] < str(previous_session)])
    retained = deepcopy(yesterday.snapshot.source_manifest["history"])
    original = canonical_json(retained[-1])
    compute = MagicMock(wraps=producer.project_realtime_daily_strategies)
    monkeypatch.setattr(producer, "project_realtime_daily_strategies", compute)
    today = producer.project_realtime_rolling_strategies(inputs, retained_history=retained)
    compute.assert_called_once()
    assert canonical_json(today.snapshot.source_manifest["history"][-2]) == original
    assert today.snapshot.source_manifest["history"][-2]["source_manifest"]["version"] == producer.STRATEGY_VERSION
    assert today.snapshot.payload["session_provenance"][str(previous_session)] == "STORED_EVALUATION"


@pytest.mark.parametrize("field", ["cohort", "basis", "clock", "evidence"])
def test_rolling_strategy_rejects_incompatible_prior_outcomes(field):
    import realtime.strategy_projection as producer
    retained = deepcopy(persistence_history_projection().snapshot.source_manifest["history"][:-1])
    item = retained[0]
    if field == "cohort":
        item["source_manifest"]["cohort_id"] = str(UUID(int=99))
    elif field == "basis":
        item["source_manifest"]["scanner_basis"] = dict(item["source_manifest"]["scanner_basis"], price="OTHER")
    elif field == "clock":
        item["payload"]["generated_at"] = (OBSERVED_AT + timedelta(days=1)).isoformat()
    else:
        item["source_manifest"]["evidence_refs"]["AAPL"]["SCAN_GAPS_1D"]["payload_sha256"] = "b" * 64
    with pytest.raises(ValueError):
        producer.project_realtime_rolling_strategies(persistence_history_inputs(), retained_history=retained)


def test_rolling_strategy_production_writes_only_new_evidence_before_source_validation_and_release(monkeypatch):
    import realtime.strategy_repository as repository
    import realtime.strategy_projection as producer
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.worker_database import RealtimeProcessDatabase
    projection = producer.project_realtime_rolling_strategies(persistence_history_inputs(),
        retained_history=persistence_history_projection().snapshot.source_manifest["history"][:-1])
    compute, publication = object.__new__(RealtimeProcessDatabase), object.__new__(RealtimeProcessDatabase)
    compute._process_plan = MagicMock(role=RealtimeDatabaseRole.COMPUTE)
    publication._process_plan = MagicMock(role=RealtimeDatabaseRole.PUBLICATION)
    events = []

    @contextmanager
    def transaction(database, **options):
        yield "compute" if database is compute else "publication"
        events.append("COMPUTE_COMMIT" if database is compute else "PUBLICATION_COMMIT")

    def evidence(cursor, writes):
        assert cursor == "compute" and len(writes) == 1
        assert writes[0].update_evidence_current is False
        assert writes[0].definition.dependency_spec["version"] == producer.STRATEGY_VERSION
        events.append("NEW_EVIDENCE")

    monkeypatch.setattr(RealtimeProcessDatabase, "transaction", transaction)
    monkeypatch.setattr(repository, "load_realtime_screening_input", lambda cursor, **scope: persistence_history_inputs())
    monkeypatch.setattr(repository, "load_retained_strategy_history_cursor", lambda cursor, inputs, **scope: ())
    monkeypatch.setattr(repository, "project_realtime_rolling_strategies", lambda inputs, **scope: projection)
    monkeypatch.setattr(repository, "persist_realtime_equity_materialization_batch_cursor", evidence)
    monkeypatch.setattr(repository, "_validate_rolling_sources", lambda cursor, manifest, **scope: events.append("SOURCE_VALIDATION"))
    monkeypatch.setattr(repository, "_validate_durable", lambda cursor, manifest, **scope: events.append("DURABLE_VALIDATION"))
    monkeypatch.setattr(repository, "persist_portal_snapshot_cursor", lambda cursor, snapshot, **options: events.append("ROLLING_RELEASE"))
    result = repository.materialize_realtime_daily_strategies(compute, publication, rolling_history=True)
    assert result == projection.snapshot
    assert events.index("NEW_EVIDENCE") < events.index("SOURCE_VALIDATION") < events.index("ROLLING_RELEASE")
    assert "COMPUTE_COMMIT" in events[:events.index("SOURCE_VALIDATION")]


def test_strategy_publication_validates_durability_as_of_the_snapshot_clock_when_it_runs_before_it(monkeypatch):
    """A producer run at a window deadline (available_by = deadline) publishes a snapshot whose clock and evidence
    observed_at are the deadline, which is still in the future of the wall clock: the durability validation must
    not use the earlier wall clock as its known_by."""
    import realtime.strategy_repository as repository
    import realtime.strategy_projection as producer
    from datetime import datetime
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.worker_database import RealtimeProcessDatabase
    projection = producer.project_realtime_rolling_strategies(persistence_history_inputs(),
        retained_history=persistence_history_projection().snapshot.source_manifest["history"][:-1])
    generated_at = projection.snapshot.generated_at
    compute, publication = object.__new__(RealtimeProcessDatabase), object.__new__(RealtimeProcessDatabase)
    compute._process_plan = MagicMock(role=RealtimeDatabaseRole.COMPUTE)
    publication._process_plan = MagicMock(role=RealtimeDatabaseRole.PUBLICATION)
    known = []

    class WallClockBeforeTheSnapshot(datetime):
        @classmethod
        def now(cls, tz=None):
            return generated_at - timedelta(minutes=5)

    @contextmanager
    def transaction(database, **options):
        yield "compute" if database is compute else "publication"

    monkeypatch.setattr(RealtimeProcessDatabase, "transaction", transaction)
    monkeypatch.setattr(repository, "datetime", WallClockBeforeTheSnapshot)
    monkeypatch.setattr(repository, "load_realtime_screening_input", lambda cursor, **scope: persistence_history_inputs())
    monkeypatch.setattr(repository, "load_retained_strategy_history_cursor", lambda cursor, inputs, **scope: ())
    monkeypatch.setattr(repository, "project_realtime_rolling_strategies", lambda inputs, **scope: projection)
    monkeypatch.setattr(repository, "persist_realtime_equity_materialization_batch_cursor", lambda cursor, writes: None)
    monkeypatch.setattr(repository, "_validate_rolling_sources", lambda cursor, manifest, **scope: known.append(scope["known_by"]))
    monkeypatch.setattr(repository, "_validate_durable", lambda cursor, manifest, **scope: known.append(scope["known_by"]))
    monkeypatch.setattr(repository, "persist_portal_snapshot_cursor", lambda cursor, snapshot, **options: None)
    assert repository.materialize_realtime_daily_strategies(compute, publication, rolling_history=True) == projection.snapshot
    assert known and set(known) == {generated_at}


def test_hourly_strategy_projection_retains_raw_finalized_inputs_and_completed_daily_confirmation():
    from realtime.strategy_projection import project_realtime_daily_strategies
    from realtime.hourly_strategy_projection import HOURLY_VERSION, HOURLY_BASIS
    inputs = persistence_history_inputs()
    projection = project_realtime_daily_strategies(inputs, interval="1h")
    snapshot = projection.snapshot
    assert snapshot.snapshot_type == HOURLY_VERSION
    assert snapshot.payload["scanner_basis"] == HOURLY_BASIS
    assert snapshot.source_manifest["slot_minutes"] == 30
    indexed = {row["bar_revision_id"]: row for row in inputs.bars}
    for result in projection.materializations:
        assert result.interval == "1h"
        assert all(indexed[identity]["adjustment_basis"] == "RAW_UNADJUSTED" for identity in result.bar_revision_ids)
        assert all(indexed[identity]["interval"] == "1d" for identity in result.confirmation_bar_revision_ids)
        assert all(indexed[identity]["bar_end"] <= inputs.cutoff for identity in (*result.bar_revision_ids, *result.confirmation_bar_revision_ids))
    assert snapshot.payload["analysis"]["SCAN_MOMENTUM_1H"]["members"][0]["required_bars"] == 200
    assert snapshot.payload["scanners"]["SCAN_FIBONACCI_1H_ATR"]["swing_policy"] == HOURLY_BASIS["fibonacci"]
    from realtime.strategy_repository import _validate_strategy_snapshot
    _validate_strategy_snapshot(snapshot.source_manifest, snapshot.payload)
    app, store = strategy_app(projection)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(snapshot.snapshot_id), interval="1h")
        response = client.get("/api/strategies/daily", params=dict(params, view="evaluations"))
        assert response.status_code == 200, response.text
        assert response.json()["publication"]["interval"] == "1h"
        assert client.get("/api/strategies/daily", params=dict(params, interval="1d")).status_code == 503
        store.split_coverage.clear()
        assert client.get("/api/strategies/daily", params=params).status_code == 503


def test_hourly_strategy_early_close_delay_finality_and_five_session_horizon():
    import exchange_calendars
    from realtime.hourly_strategy_projection import session_hourly_slots, hourly_strategy_inputs, HOURLY_HISTORY_VERSION
    from realtime.strategy_projection import project_realtime_strategy_history
    from realtime.strategy_repository import _validate_strategy_snapshot
    from realtime.preflight import ProviderTier
    calendar = exchange_calendars.get_calendar("XNYS")
    slots = session_hourly_slots(calendar, "2026-11-27")
    assert len(slots) == 4
    assert (slots[-1][1] - slots[-1][0]).total_seconds() == 1800
    inputs = persistence_history_inputs()
    target = calendar.session_close(str(inputs.session)).to_pydatetime()
    delayed = replace(inputs, provider_tier=ProviderTier.MASSIVE_DEVELOPER, cutoff=target + timedelta(minutes=14))
    with pytest.raises(ValueError, match="provider-qualified"):
        hourly_strategy_inputs(delayed, target)
    bar = next(row for row in inputs.bars if row["interval"] == "1h")
    invalid = replace(inputs, bars=tuple(dict(row, is_final=False) if row is bar else row for row in inputs.bars))
    with pytest.raises(ValueError, match="availability drift"):
        hourly_strategy_inputs(invalid)
    history = project_realtime_strategy_history(inputs, interval="1h")
    assert history.snapshot.snapshot_type == HOURLY_HISTORY_VERSION
    _validate_strategy_snapshot(history.snapshot.source_manifest, history.snapshot.payload)
    assert len(history.snapshot.payload["persistence"]["sessions"]) == 5
    assert history.snapshot.payload["persistence"]["window_unit"] == "XNYS_TRADING_SESSIONS"
    assert all(len(result.bar_revision_ids) > 5 for result in history.materializations)
    without_coverage = replace(inputs, coverage=())
    missing = __import__("realtime.strategy_projection", fromlist=["project_realtime_daily_strategies"]).project_realtime_daily_strategies(without_coverage, interval="1h")
    assert missing.snapshot.payload["analysis"]["SCAN_MOMENTUM_1H"]["members"][0]["reason"] == "HOURLY_SPLIT_COVERAGE_UNAVAILABLE"


def test_strategy_offline_viewport_fixtures_are_serialized_stored_reads():
    import os
    from pathlib import Path
    from realtime.strategy_projection import project_realtime_daily_strategies, project_realtime_strategy_history, project_realtime_rolling_strategies
    target = os.environ.get("REALTIME_STRATEGY_VIEWPORT_FIXTURE")
    if not target:
        return
    inputs = daily_scanner_fixture()
    inputs = replace(inputs, members=tuple(row for row in inputs.members if row["ticker"] == "AAPL"),
                     bars=tuple(row for row in inputs.bars if row["ticker"] == "AAPL"))
    fixtures = {}
    histories = {}
    for name, interval, history in [("daily", "1d", False), ("history", "1d", True), ("hourly", "1h", False), ("hourly_history", "1h", True), ("rolling", "1d", True), ("hourly_rolling", "1h", True)]:
        if "rolling" in name:
            prior = histories[interval]
            retained = deepcopy(prior.snapshot.source_manifest["history"][:-1])
            for item in retained:
                item["source_release"] = dict(snapshot_id=str(prior.snapshot.snapshot_id), source_generation=prior.snapshot.source_generation,
                    source_manifest_sha256=prior.snapshot.source_manifest_sha256, payload_sha256=prior.snapshot.payload_sha256,
                    generated_at=prior.snapshot.generated_at.isoformat())
            projection = project_realtime_rolling_strategies(inputs, retained_history=retained, interval=interval)
        else:
            projection = (project_realtime_strategy_history(inputs, interval=interval) if history
                          else project_realtime_daily_strategies(inputs, interval=interval))
            if history:
                histories[interval] = projection
        app, store = strategy_app(projection)
        if "rolling" in name:
            _, prior_store = strategy_app(histories[interval])
            store.snapshots.update(prior_store.snapshots)
            store.evidence.update(prior_store.evidence)
            store.dependencies.update(prior_store.dependencies)
            store.split_coverage.update(prior_store.split_coverage)
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(projection.snapshot.snapshot_id), interval=interval)
        with TestClient(app) as client:
            def read(**selection):
                response = client.get("/api/strategies/daily", params=dict(params, **selection))
                assert response.status_code == 200, response.text
                return response.json()
            fixtures[name] = dict(latest=read(), evaluations=read(view="evaluations"), persistence=read(view="persistence"),
                research=read(ticker="AAPL"), strategies={strategy: read(strategy=strategy)
                    for strategy in DAILY_SCANNER_CONTRACTS if strategy != "fvg"})
            if history:
                fixtures[name]["research_session"] = read(ticker="AAPL", evaluation_session=projection.snapshot.payload["evaluation_sessions"][0])
    path = Path(target).resolve()
    assert path == Path(__file__).parents[2].joinpath(".realtime-canary", "strategy-offline-fixture.json").resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(dict(mode="OFFLINE_FIXTURE", releases=fixtures)), encoding="utf-8")
    browser_path = os.environ.get("REALTIME_STRATEGY_BROWSER_FIXTURE")
    if browser_path:
        public = Path(browser_path).resolve()
        assert public == Path(__file__).parents[3].joinpath("frontend", "public", "realtime-strategy-offline-fixture.json").resolve()
        public.parent.mkdir(parents=True, exist_ok=True)
        public.write_text(canonical_json(dict(mode="OFFLINE_FIXTURE", releases=fixtures)), encoding="utf-8")


@lru_cache(maxsize=2)
def strategy_projection(interval="1d"):
    from realtime.strategy_projection import project_realtime_daily_strategies
    inputs = daily_scanner_fixture() if interval == "1d" else persistence_history_inputs()
    return project_realtime_daily_strategies(inputs, interval=interval)


def test_persistence_history_distinguishes_unknown_from_miss_and_tracks_same_zone():
    from realtime.strategy_persistence import build_strategy_persistence, persistence_matches, _distance
    assert _distance(dict(gap_low=100, gap_high=102, last_close=104, fill_pct=75)) == round(2 / 104 * 100, 4)
    assert _distance(dict(fvg_low=100, fvg_high=102, last_close=101)) == 0
    assert _distance(dict(ma_spread_pct=-1.2)) == 1.2
    assert persistence_matches("SCAN_MA_1D_9_21", [{"signal": "Above"}]) == []
    assert persistence_matches("SCAN_MA_1D_9_21", [{"signal": "Recent Bullish Crossover"}])
    assert persistence_matches("SCAN_FIBONACCI_1D_5", [{"signal": "Between Levels"}]) == []
    sessions = [f"2026-09-{day:02}" for day in range(21, 26)]
    history = []
    for index, session in enumerate(sessions):
        if index == 2:
            continue
        rows = [{"ticker": "AAPL", "gap_date": "2026-09-18", "gap_low": 100, "gap_high": 102,
                 "gap_direction": "UP"}] if index in {0, 1, 4} else []
        history.append(dict(session=session, source_manifest={"evidence_refs": {}, "source_windows": {}},
            payload=dict(market_time=session + "T20:00:00Z", generated_at=OBSERVED_AT.isoformat(),
                scanners={contract: dict(results=rows if contract == "SCAN_GAPS_1D" else []) for contract in DAILY_SCANNER_CONTRACTS.values()},
                analysis={contract: dict(members=[dict(ticker="AAPL", status="COMPLETE", reason=None,
                    available_bars=284, required_bars=20)]) for contract in DAILY_SCANNER_CONTRACTS.values()})))
    row = build_strategy_persistence(history, ["AAPL"], sessions)["rows"][0]
    assert (row["matched"], row["evaluated"], row["coverage"]) == (3, 4, "4/5")
    assert [cell["state"] for cell in row["sessions"]] == ["MATCH", "MATCH", "UNAVAILABLE", "NO_MATCH", "MATCH"]
    assert [cell["transition"] for cell in row["sessions"]] == ["FIRST_OBSERVED", "CONTINUING", "UNKNOWN", "UNKNOWN", "REAPPEARED"]
    assert row["sessions"][0]["event_ids"] == row["sessions"][1]["event_ids"] == row["sessions"][4]["event_ids"]
    assert row["consecutive_run"] == 1


def test_persistence_history_legacy_pin_has_only_one_stored_session_and_no_fabricated_cells():
    app, store = strategy_app()
    snapshot = strategy_projection().snapshot
    before = len(store.statements)
    with TestClient(app) as client:
        response = client.get("/api/strategies/daily", params=dict(cohort_id=str(COHORT_ID),
            snapshot_id=str(snapshot.snapshot_id), view="persistence"))
    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data["evaluation_sessions"]) == 5
    assert data["observed_sessions"] == [snapshot.payload["session"]]
    assert data["persistence"] is None
    assert data["interpretation"] == "RETAINED_LATEST_EVALUATION_ONLY"
    assert all(statement.startswith(("SELECT", "SET")) for statement in store.statements[before:])


def test_persistence_history_release_is_evidence_backed_and_exact_session_research_is_stored_only(monkeypatch):
    import realtime.strategy_repository as repository
    projection = persistence_history_projection()
    app, store = strategy_app(projection)
    prohibited = MagicMock(side_effect=AssertionError("GET cannot reconstruct research"))
    monkeypatch.setattr(repository, "project_realtime_strategy_history", prohibited)
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(projection.snapshot.snapshot_id))
    with TestClient(app) as client:
        response = client.get("/api/strategies/daily", params=dict(params, view="persistence"))
        assert response.status_code == 200, response.text
        result = response.json()
        assert len(result["observed_sessions"]) == 5
        assert all(row["evaluated"] == 5 for row in result["persistence"]["rows"])
        assert all(row["matched"] == 0 for row in result["persistence"]["rows"])
        session = result["evaluation_sessions"][0]
        selected = client.get("/api/strategies/daily", params=dict(params, ticker="AAPL", evaluation_session=session))
        assert selected.status_code == 200, selected.text
        assert selected.json()["evaluation_session"] == session
        assert selected.json()["evaluation_market_time"].startswith(session)
        assert selected.json()["evidence_refs"]["SCAN_MA_1D_9_21"]["market_time"].startswith(session)
        assert client.get("/api/strategies/daily", params=dict(params, ticker="AAPL", evaluation_session="2020-01-01")).status_code == 422
        evidence = next(row for row in store.evidence.values() if row["source_name"] == "SCAN_GAPS_1D")
        assert evidence["payload"]["interpretation"] == "RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS"
        del store.evidence[str(evidence["evidence_id"])]
        assert client.get("/api/strategies/daily", params=dict(params, view="persistence")).status_code == 503
    prohibited.assert_not_called()


def strategy_app(projection=None, clock=lambda: OBSERVED_AT, retention=None):
    from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
    from realtime.strategy_repository import RealtimeDailyStrategyRepository
    from tests.realtime.test_dashboard_vertical_slice import DashboardStore
    from tests.realtime.portal_v2_support import uncapped_hot_manifest_for_kept_only_snapshots
    from tests.realtime.test_ticker_vertical_slice import EVIDENCE_RESOLUTION_PREFIXES

    class StrategyStore(DashboardStore):
        def publish(self, snapshot, **kwargs):
            with uncapped_hot_manifest_for_kept_only_snapshots():
                return super().publish(snapshot, **kwargs)

        def execute(self, sql, params=()):
            if sql.startswith("SELECT revision_id") and params[2] > projection.snapshot.generated_at:
                if self.reference_expired or self.reference_changed:
                    self.statements.append(sql)
                    self.result = [] if self.reference_expired else [dict(revision_id="new-revision", payload_sha256="0" * 64)]
                    return
            if sql.startswith("SELECT snapshot.snapshot_id, snapshot.cohort_id, snapshot.source_generation"):
                self.statements.append(sql)
                stored = self.snapshots.get(params[0])
                self.result = [stored] if stored and str(stored["cohort_id"]) == params[1] else []
                return
            if sql.startswith("SELECT snapshot.*"):
                if isinstance(params[1], list):
                    selected = next((row for row in self.snapshots.values()
                        if str(row["cohort_id"]) == params[0] and row["snapshot_type"] in params[1]
                        and (params[7] is None or str(row["snapshot_id"]) == params[7])), None)
                    params = (params[0], selected["snapshot_type"] if selected else params[1][0], *params[2:])
                params = (*params[:7], OBSERVED_AT, *params[7:])
                super().execute(sql, params)
                for row in self.result:
                    row["stock_count"] = projection.snapshot.source_manifest["expected_members"]
                return
            if sql.startswith("SELECT coverage_id,security_id,action_type"):
                self.statements.append(sql)
                self.result = [row for identity, row in self.split_coverage.items() if identity in params[0]]
                return
            if sql.startswith("SELECT action_id,security_id,ticker,action_type"):
                self.statements.append(sql)
                self.result = [row for row in self.actions.values() if str(row["action_id"]) in params[0]]
                return
            if sql.startswith("SELECT dependency.evidence_id,COUNT(*)"):
                self.statements.append(sql)
                self.result = [dict(evidence_id=identity, dependency_count=len(self.dependencies.get(identity, ())))
                               for identity in params[1]]
                return
            if sql.startswith("SELECT dependency.dependency_order"):
                self.statements.append(sql)
                self.result = [dict(dependency_order=ordinal, dependency_evidence_id=identity,
                    payload=self.evidence[identity]["payload"], payload_sha256=self.evidence[identity]["payload_sha256"],
                    materialization_input_sha256=self.evidence[identity]["materialization_input_sha256"])
                    for ordinal, identity in enumerate(self.dependencies.get(params[1], ())) if identity in self.evidence]
                return
            if sql.startswith("SELECT evidence.*") or sql.startswith(EVIDENCE_RESOLUTION_PREFIXES):
                self.statements.append(sql)
                self.result = [row for row in self.evidence.values() if str(row["security_id"]) == params[1]
                               and str(row["evidence_id"]) in params[2]]
                if sql.startswith(EVIDENCE_RESOLUTION_PREFIXES):
                    self.result = [{key: row[key] for key in ("cohort_id", "evidence_id", "evidence_type", "market_time", "payload_sha256")}
                                   for row in self.result]
                return
            super().execute(sql, params)

    store = StrategyStore()
    projection = projection or strategy_projection()
    store.reference_changed = False
    store.publish(projection.snapshot, **({"retention": retention} if retention else {}))
    store.evidence = {}
    store.actions = {}
    store.split_coverage = {row["coverage_id"]: row for refs in projection.snapshot.source_manifest.get("split_coverage_refs", {}).values() for row in refs}
    for ticker, context in projection.snapshot.source_manifest.get("action_context", {}).items():
        for row in context["action_refs"]:
            store.actions[row["action_id"]] = dict(row, ticker=ticker)
    store.dependencies = {}
    for result in projection.materializations:
        from realtime.equity_evidence_repository import _prepare_realtime_equity_materialization
        from realtime.strategy_repository import _strategy_definition
        prepared = _prepare_realtime_equity_materialization(result, _strategy_definition(result, projection.snapshot.source_manifest),
            generation=3, update_evidence_current=False)
        for dependency in prepared.dependency_rows:
            store.dependencies.setdefault(dependency[1], []).append(dependency[3])
        for row in result.materialization.evidence:
            store.evidence[str(row.evidence_id)] = dict(cohort_id=COHORT_ID, security_id=row.security_id,
                evidence_id=row.evidence_id, evidence_type=row.evidence_type.value, market_time=row.market_time,
                payload_sha256=row.payload_sha256, payload=json.loads(row.payload_json), ticker=row.ticker,
                interval=row.interval, source_name=row.source_name, source_version=row.source_version,
                security_revision_id=row.security_revision_id, observed_at=row.observed_at,
                provider_tier=result.provider_tier.value, instrument_id=result.instrument_id,
                window_security_id=row.security_id, window_instrument_id=result.instrument_id,
                window_provider_tier=result.provider_tier.value, window_interval=result.interval,
                window_market_time=prepared.feature.market_time, window_observed_at=prepared.feature.observed_at,
                latest_bar_revision_id=row.latest_bar_revision_id, analysis_run_id=row.analysis_run_id,
                source_window_role=prepared.source_roles[row.evidence_id], source_window_sha256=result.input_sha256,
                materialization_input_sha256=result.input_sha256,
                primary_bar_revision_ids=result.bar_revision_ids, confirmation_bar_revision_ids=result.confirmation_bar_revision_ids)
    app = create_realtime_portal_app(RealtimePortalRepositories(None, None, None,
        daily_strategies=RealtimeDailyStrategyRepository(store.cursor)), clock=clock)
    return app, store


@pytest.mark.parametrize("interval", ["1d", "1h"])
@pytest.mark.parametrize("defect", ["reference_expired", "reference_changed", "action_drift"])
def test_strategy_retained_browse_survives_current_reference_and_action_changes(defect, interval):
    projection = strategy_projection(interval)
    original = projection.snapshot
    app, store = strategy_app(projection, clock=lambda: original.generated_at + timedelta(days=10))
    setattr(store, defect, True)
    frozen = canonical_json(store.snapshots[str(original.snapshot_id)])
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(original.snapshot_id), interval=interval)
    with TestClient(app) as client:
        assert client.get("/api/strategies/daily", params=params).status_code == 503
        for selection in ({}, {"strategy": "gaps"}, {"view": "evaluations"}, {"view": "persistence"},
                          {"ticker": original.source_manifest["tickers"][0]}):
            response = client.get("/api/strategies/daily", params=dict(params, read_mode="retained", **selection))
            assert response.status_code == 200, response.text
            result = response.json()
            publication = result["publication"]
            assert publication["snapshot_id"] == str(original.snapshot_id)
            assert publication["payload_sha256"] == original.payload_sha256
            assert publication["market_time"] == original.payload["market_time"]
            assert publication["generated_at"] == original.payload["generated_at"]
            assert publication["read_mode"] == "RETAINED"
            assert publication["current_data_available"] is False
            assert publication["execution_permission"] is False
            assert publication["provider_fetch_performed"] is False
            if defect != "action_drift":
                assert publication["reference_status"] == "UNAVAILABLE"
            else:
                assert publication["action_status"] == "CHANGED"
            if selection.get("strategy") == "gaps":
                contract = "SCAN_GAPS_1D" if interval == "1d" else "SCAN_GAPS_1H"
                assert result["data"]["results"] == json.loads(canonical_json(original.payload["scanners"][contract]["results"]))
        assert client.get("/api/strategies/daily", params=params).status_code == 503
    assert canonical_json(store.snapshots[str(original.snapshot_id)]) == frozen


@pytest.mark.parametrize("defect", ["reference_drift", "payload", "evidence", "pin", "interval"])
def test_strategy_retained_browse_rejects_invalid_original_data_and_substitution(defect):
    original = strategy_projection().snapshot
    app, store = strategy_app()
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(original.snapshot_id), read_mode="retained")
    if defect == "reference_drift":
        store.reference_drift = True
    elif defect == "payload":
        store.snapshots[str(original.snapshot_id)]["payload_sha256"] = "0" * 64
    elif defect == "evidence":
        store.evidence.clear()
    elif defect == "pin":
        params["snapshot_id"] = str(UUID(int=99))
    else:
        params["interval"] = "1h"
    with TestClient(app) as client:
        for selection in ({"strategy": "gaps"}, {"view": "persistence"}, {"ticker": "MSFT"}):
            response = client.get("/api/strategies/daily", params=dict(params, **selection))
            assert response.status_code == 503, response.text


@pytest.mark.parametrize("interval", ["1d", "1h"])
def test_strategy_validation_cache_reuses_immutable_checks_but_refreshes_live_state(monkeypatch, interval):
    import realtime.strategy_repository as repository
    projection = strategy_projection(interval)
    clock = {"now": projection.snapshot.generated_at}
    app, store = strategy_app(projection, clock=lambda: clock["now"])
    validation = MagicMock(wraps=repository._validate_strategy_snapshot)
    durable = MagicMock(wraps=repository._validate_durable)
    monkeypatch.setattr(repository, "_validate_strategy_snapshot", validation)
    monkeypatch.setattr(repository, "_validate_durable", durable)
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(projection.snapshot.snapshot_id), interval=interval, read_mode="retained")
    with TestClient(app) as client:
        response = client.get("/api/strategies/daily", params=dict(params, strategy="gaps"))
        assert response.status_code == 200, response.text
        cold_calls = durable.call_count
        assert cold_calls > 0
        clock["now"] += timedelta(days=10)
        store.reference_expired = store.action_drift = True
        response = client.get("/api/strategies/daily", params=dict(params, view="persistence"))
        assert response.status_code == 200, response.text
        assert validation.call_count == 1
        assert durable.call_count == cold_calls
        publication = response.json()["publication"]
        assert publication["current_data_available"] is False
        assert publication["reference_status"] == "UNAVAILABLE"
        assert publication["action_status"] == "CHANGED"
        assert publication["read_at"] == clock["now"].isoformat()
        assert client.get("/api/strategies/daily", params=dict(params, read_mode="current")).status_code == 503


def test_strategy_validation_cache_never_reuses_a_later_visibility_cutoff(monkeypatch):
    import realtime.strategy_repository as repository
    original = strategy_projection().snapshot
    clock = {"now": original.generated_at + timedelta(days=2)}
    app, store = strategy_app(clock=lambda: clock["now"])
    durable = MagicMock(wraps=repository._validate_durable)
    monkeypatch.setattr(repository, "_validate_durable", durable)
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(original.snapshot_id), read_mode="retained", view="evaluations")
    with TestClient(app) as client:
        assert client.get("/api/strategies/daily", params=params).status_code == 200
        first_calls = durable.call_count
        assert first_calls > 0
        clock["now"] = original.generated_at
        assert client.get("/api/strategies/daily", params=params).status_code == 200
        assert durable.call_count == first_calls * 2
        store.snapshots[str(original.snapshot_id)]["payload_sha256"] = "0" * 64
        assert client.get("/api/strategies/daily", params=params).status_code == 503


def test_strategy_validation_cache_rejects_appended_dependency_membership():
    app, store = strategy_app()
    original = strategy_projection().snapshot
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(original.snapshot_id), read_mode="retained")
    with TestClient(app) as client:
        assert client.get("/api/strategies/daily", params=params).status_code == 200
        setup = original.source_manifest["setup_evidence"][0]
        extra = next(identity for identity in store.evidence if identity not in setup["dependency_evidence_ids"])
        store.dependencies[setup["evidence_id"]].append(extra)
        assert client.get("/api/strategies/daily", params=dict(params, view="persistence")).status_code == 503


def test_strategy_validation_cache_is_bounded_hash_scoped_and_does_not_cache_failure(monkeypatch):
    import realtime.strategy_repository as repository
    validation = MagicMock()
    monkeypatch.setattr(repository, "_validate_strategy_snapshot", validation)
    monkeypatch.setattr(repository, "_evaluation_manifests", lambda manifest: ())
    monkeypatch.setattr(repository, "STRATEGY_VALIDATION_CACHE_SIZE", 2)
    reader = repository.RealtimeDailyStrategyRepository(MagicMock())
    stored = dict(cohort_id=COHORT_ID, snapshot_id=UUID(int=1), source_manifest_sha256="a" * 64,
                  payload_sha256="b" * 64, source_manifest={"version": repository.STRATEGY_VERSION}, payload={})
    for identity in range(1, 4):
        reader._validate_immutable_release(None, dict(stored, snapshot_id=UUID(int=identity)), cutoff=OBSERVED_AT, read_mode="retained")
    assert len(reader._validated_releases) == 2
    assert validation.call_count == 3
    reader._validate_immutable_release(None, stored, cutoff=OBSERVED_AT, read_mode="retained")
    assert validation.call_count == 4
    changed = dict(stored, payload_sha256="c" * 64)
    validation.side_effect = ValueError("Invalid immutable evidence")
    with pytest.raises(ValueError, match="Invalid immutable evidence"):
        reader._validate_immutable_release(None, changed, cutoff=OBSERVED_AT, read_mode="retained")
    validation.side_effect = None
    reader._validate_immutable_release(None, changed, cutoff=OBSERVED_AT, read_mode="retained")
    assert validation.call_count == 6
    reader._validate_immutable_release(None, changed, cutoff=OBSERVED_AT, read_mode="retained")
    assert validation.call_count == 6


def test_strategy_validation_cache_single_flights_concurrent_reads(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    import realtime.strategy_repository as repository
    entered, release, second_started = Event(), Event(), Event()

    def validate(manifest, payload):
        entered.set()
        assert release.wait(5)

    validation = MagicMock(side_effect=validate)
    monkeypatch.setattr(repository, "_validate_strategy_snapshot", validation)
    monkeypatch.setattr(repository, "_evaluation_manifests", lambda manifest: ())
    reader = repository.RealtimeDailyStrategyRepository(MagicMock())
    stored = dict(cohort_id=COHORT_ID, snapshot_id=UUID(int=1), source_manifest_sha256="a" * 64,
                  payload_sha256="b" * 64, source_manifest={"version": repository.STRATEGY_VERSION}, payload={})

    def read(second=False):
        if second:
            second_started.set()
        reader._validate_immutable_release(None, stored, cutoff=OBSERVED_AT, read_mode="retained")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(read)
        try:
            assert entered.wait(5)
            second = executor.submit(read, True)
            assert second_started.wait(5)
        finally:
            release.set()
        first.result(timeout=5)
        second.result(timeout=5)
    assert validation.call_count == 1


def test_strategy_stored_reads_are_independent_exact_and_evidence_backed(monkeypatch):
    import realtime.strategy_repository as repository
    app, store = strategy_app()
    prohibited = MagicMock(side_effect=AssertionError("Stored GET must not recompute"))
    monkeypatch.setattr(repository, "project_realtime_daily_strategies", prohibited)
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(strategy_projection().snapshot.snapshot_id))
    before = len(store.statements)
    with TestClient(app) as client:
        assert client.get("/api/latest-price-date", params=params).status_code == 503
        for strategy in DAILY_SCANNER_CONTRACTS:
            response = client.get("/api/strategies/daily", params=dict(params, strategy=strategy))
            assert response.status_code == 200, response.text
            assert response.json()["data"]["analysis"]["total_evaluated"] == 9
        response = client.get("/api/strategies/daily", params=dict(params, ticker="MSFT"))
        assert response.status_code == 200, response.text
        assert response.json()["scans"]["momentum"][0]["ticker"] == "MSFT"
        assert len(response.json()["evidence_refs"]) == 6
        assert response.json()["publication"]["snapshot_id"] == params["snapshot_id"]
    prohibited.assert_not_called()
    assert all(sql.startswith(("SELECT", "SET")) for sql in store.statements[before:])


def test_persistence_evaluations_are_one_exact_stored_bundle_not_fabricated_five_day_history(monkeypatch):
    import realtime.strategy_repository as repository
    app, store = strategy_app()
    original = strategy_projection().snapshot
    before = len(store.statements)
    prohibited = MagicMock(side_effect=AssertionError("Persistence GET cannot compute or load inputs"))
    monkeypatch.setattr(repository, "project_realtime_daily_strategies", prohibited)
    monkeypatch.setattr(repository, "load_realtime_screening_input", prohibited)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(original.snapshot_id), view="evaluations")
        response = client.get("/api/strategies/daily", params=params)
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["publication"]["snapshot_id"] == str(original.snapshot_id)
        assert result["observed_sessions"] == [original.payload["session"]]
        assert result["recurrence_available"] is False
        assert result["members"] == original.source_manifest["tickers"]
        assert result["scanners"] == json.loads(canonical_json(original.payload["scanners"]))
        assert result["analysis"] == original.payload["analysis"]
        assert result["evidence_refs"] == original.source_manifest["evidence_refs"]
        for override in ({"view": "history"}, {"ticker": "AAPL"}, {"strategy": "gaps"}):
            assert client.get("/api/strategies/daily", params=dict(params, **override)).status_code == 422
    prohibited.assert_not_called()
    assert all(statement.startswith(("SELECT", "SET")) for statement in store.statements[before:])


@pytest.mark.parametrize("offset_hours", [-7, 0, 5.5])
def test_strategy_timestamp_guards_compare_instants_without_rewriting_publication_clocks(offset_hours):
    from datetime import timezone
    app, store = strategy_app()
    zone = timezone(timedelta(hours=offset_hours))
    stored = next(iter(store.snapshots.values()))
    stored["generated_at"] = stored["generated_at"].astimezone(zone)
    for row in store.evidence.values():
        for field in ("market_time", "observed_at", "window_market_time", "window_observed_at"):
            row[field] = row[field].astimezone(zone)
    original = strategy_projection().snapshot
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(original.snapshot_id), strategy="gaps")
        response = client.get("/api/strategies/daily", params=params)
        assert response.status_code == 200, response.text
        assert response.json()["publication"]["generated_at"] == original.payload["generated_at"]
        receipt = next(row for row in store.evidence.values() if row["source_name"] == "SCAN_GAPS_1D")
        receipt["observed_at"] += timedelta(seconds=1)
        assert client.get("/api/strategies/daily", params=params).status_code == 503


@pytest.mark.parametrize("read_mode", ["current", "retained"])
@pytest.mark.parametrize("fault", ["missing", "payload", "window", "model", "security", "coverage", "group"])
def test_strategy_release_rejects_durable_and_coverage_drift(fault, read_mode):
    app, store = strategy_app()
    row = next(row for row in store.evidence.values() if row["source_name"] == "SCAN_FVG_1D_50")
    if fault == "missing":
        del store.evidence[str(row["evidence_id"])]
    elif fault == "payload":
        row["payload"] = {}
    elif fault == "window":
        row["primary_bar_revision_ids"] = row["primary_bar_revision_ids"][-50:]
    elif fault == "model":
        row["source_version"] = "OTHER"
    elif fault == "security":
        row["security_revision_id"] = UUID(int=99)
    else:
        stored = next(iter(store.snapshots.values()))
        stored["payload"] = deepcopy(stored["payload"])
        if fault == "coverage":
            stored["payload"]["analysis"]["SCAN_FVG_1D_50"]["total_evaluated"] = 0
        else:
            stored["payload"]["scanners"]["SCAN_FVG_1D_50"]["results_by_type"] = {}
        stored["payload_sha256"] = canonical_sha256(stored["payload"])
    with TestClient(app) as client:
        assert client.get("/api/strategies/daily", params=dict(cohort_id=str(COHORT_ID), strategy="gaps", read_mode=read_mode)).status_code == 503


def test_real_short_and_missing_strategy_release_never_claims_completed_no_match():
    from realtime.strategy_projection import project_realtime_daily_strategies
    inputs = daily_scanner_fixture()
    sessions = sorted({row["session_date"] for row in inputs.bars if row["interval"] == "1d"})[-19:]
    inputs = replace(inputs, bars=tuple(row for row in inputs.bars if not (
        row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED" and
        (row["ticker"] == "MSFT" or row["ticker"] == "AAPL" and row["session_date"] not in sessions))))
    projection = project_realtime_daily_strategies(inputs)
    app, _ = strategy_app(projection)
    with TestClient(app) as client:
        response = client.get("/api/strategies/daily", params=dict(cohort_id=str(COHORT_ID), strategy="gaps"))
        assert response.status_code == 200, response.text
        for scanner in (response.json()["data"], response.json()["fvgData"]):
            assert scanner["total_scanned"] == 9
            assert scanner["analysis"]["status"] == "PARTIAL"
            assert scanner["analysis"]["total_evaluated"] == 7
            members = {row["ticker"]: row for row in scanner["analysis"]["members"]}
            assert members["AAPL"]["status"] == "INSUFFICIENT_HISTORY"
            assert members["MSFT"]["status"] == "UNAVAILABLE"
        for ticker, expected in (("AAPL", "INSUFFICIENT_HISTORY"), ("MSFT", "UNAVAILABLE")):
            result = client.get("/api/strategies/daily", params=dict(cohort_id=str(COHORT_ID), ticker=ticker,
                snapshot_id=str(projection.snapshot.snapshot_id)))
            assert result.status_code == 200, result.text
            assert {row["status"] for row in result.json()["analysis"].values()} == {expected}
            assert all(not rows for rows in result.json()["scans"].values())
            assert len(result.json()["evidence_refs"]) == (6 if ticker == "AAPL" else 0)


@pytest.mark.parametrize("action_type", ["DIVIDEND", "MERGER", "SPINOFF", "SYMBOL_CHANGE", "OTHER"])
def test_one_member_corporate_action_does_not_block_healthy_strategy_members(action_type):
    from datetime import date
    from realtime.strategy_projection import project_realtime_daily_strategies
    inputs = daily_scanner_fixture()
    reference = next(row for row in inputs.members if row["ticker"] == "AAPL")
    action = dict(inputs.actions[0], security_id=reference["security_id"], ticker="AAPL",
                  action_type=action_type, effective_date=date(2026, 9, 1), payload_sha256=canonical_sha256(action_type))
    projection = project_realtime_daily_strategies(replace(inputs, actions=(*inputs.actions, action)))
    app, _ = strategy_app(projection)
    assert len(projection.materializations) == 8
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(projection.snapshot.snapshot_id))
        for strategy, contract in DAILY_SCANNER_CONTRACTS.items():
            response = client.get("/api/strategies/daily", params=dict(params, strategy=strategy))
            assert response.status_code == 200, response.text
            scanner = response.json()["data"]
            assert scanner["total_scanned"] == 9
            assert scanner["analysis"]["status"] == "PARTIAL"
            assert scanner["analysis"]["total_evaluated"] == 8
            assert scanner["analysis"]["total_unavailable"] == 1
            member = next(row for row in scanner["analysis"]["members"] if row["ticker"] == "AAPL")
            assert member["status"] == "UNAVAILABLE"
            assert member["reason"] == "CORPORATE_ACTION_REVIEW"
            assert member["available_bars"] == 283
            assert scanner["results"] == json.loads(canonical_json([
                row for row in daily_scanner_snapshot().payload["scanners"][contract]["results"] if row["ticker"] != "AAPL"]))
        research = client.get("/api/strategies/daily", params=dict(params, ticker="AAPL")).json()
        assert research["setup"] is None
        assert research["evidence_refs"] == {}
        assert all(not rows for rows in research["scans"].values())
    assert projection.snapshot.source_manifest["unavailable_members"]["AAPL"]["action_refs"][0]["action_type"] == action_type


def test_all_action_review_members_remain_unavailable_not_completed_empty():
    from datetime import date
    from realtime.strategy_projection import project_realtime_daily_strategies
    inputs = daily_scanner_fixture()
    actions = tuple(dict(inputs.actions[0], action_id=UUID(int=300 + index), ticker=member["ticker"],
        security_id=member["security_id"], action_type="MERGER", effective_date=date(2026, 9, 1),
        payload_sha256=canonical_sha256(("synthetic-merger", member["ticker"])))
        for index, member in enumerate(inputs.members))
    projection = project_realtime_daily_strategies(replace(inputs, actions=(*inputs.actions, *actions)))
    assert projection.materializations == ()
    app, store = strategy_app(projection)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), strategy="gaps")
        response = client.get("/api/strategies/daily", params=params)
        assert response.status_code == 200, response.text
        for scanner in (response.json()["data"], response.json()["fvgData"]):
            assert scanner["total_scanned"] == 9
            assert scanner["analysis"]["status"] == "UNAVAILABLE"
            assert scanner["analysis"]["total_evaluated"] == 0
            assert scanner["analysis"]["total_unavailable"] == 9
            assert scanner["results"] == []
            assert {row["reason"] for row in scanner["analysis"]["members"]} == {"CORPORATE_ACTION_REVIEW"}
        del store.actions[str(actions[0]["action_id"])]
        assert client.get("/api/strategies/daily", params=params).status_code == 503


def test_strategy_action_policy_is_pinned_without_rehashing_legacy_definitions():
    from realtime.strategy_repository import _strategy_definition
    projection = strategy_projection()
    manifest = dict(projection.snapshot.source_manifest)
    definition = _strategy_definition(projection.materializations[0], manifest)
    assert definition.dependency_spec["action_policy"] == "DAILY_STRATEGY_ACTION_CONTEXT_V1"
    del manifest["action_policy"]
    legacy = _strategy_definition(projection.materializations[0], manifest)
    assert "action_policy" not in legacy.dependency_spec
    assert legacy.bundle_sha256 != definition.bundle_sha256


def test_verified_recurring_dividend_preserves_original_scanner_results_and_basis():
    from datetime import date
    from realtime.strategy_projection import project_realtime_daily_strategies
    inputs = daily_scanner_fixture()
    member = next(row for row in inputs.members if row["ticker"] == "AAPL")
    raw = dict(ticker="AAPL", distribution_type="recurring", currency="USD", cash_amount=.26, ex_dividend_date="2026-09-01")
    action = dict(inputs.actions[0], ticker="AAPL", security_id=member["security_id"], action_type="DIVIDEND",
        effective_date=date(2026, 9, 1), ex_date=date(2026, 9, 1), cash_amount="0.26", raw_payload=raw,
        payload_sha256=canonical_sha256(raw))
    projection = project_realtime_daily_strategies(replace(inputs, actions=(*inputs.actions, action)))
    assert len(projection.materializations) == 9
    assert projection.snapshot.payload["scanners"] == daily_scanner_snapshot().payload["scanners"]
    assert projection.snapshot.source_manifest["unavailable_members"] == {}
    context = projection.snapshot.source_manifest["action_context"]["AAPL"]
    assert context["action_refs"][0]["handling"] == "RECURRING_CASH_DIVIDEND_PRICE_SERIES_UNCHANGED"
    app, store = strategy_app(projection)
    store.actions[str(action["action_id"])] = action
    with TestClient(app) as client:
        response = client.get("/api/strategies/daily", params=dict(cohort_id=str(COHORT_ID), strategy="gaps"))
        assert response.status_code == 200, response.text
        assert response.json()["data"]["analysis"]["total_evaluated"] == 9
        store.actions[str(action["action_id"])] = dict(action, cash_amount="99")
        assert client.get("/api/strategies/daily", params=dict(cohort_id=str(COHORT_ID), strategy="gaps")).status_code == 503


@pytest.mark.parametrize("fault", ["special", "unknown", "missing", "currency", "amount", "hash", "ex_date"])
def test_unverified_dividend_is_not_silently_accepted(fault):
    from datetime import date
    from realtime.strategy_projection import strategy_action_context
    raw = dict(ticker="AAPL", distribution_type="recurring", currency="USD", cash_amount=.26, ex_dividend_date="2026-09-01")
    action = dict(action_id=UUID(int=90), security_id=UUID(int=91), ticker="AAPL", action_type="DIVIDEND",
        effective_date=date(2026, 9, 1), ex_date=date(2026, 9, 1), cash_amount="0.26", raw_payload=raw)
    if fault in {"special", "unknown"}:
        raw["distribution_type"] = fault
    elif fault == "missing":
        del raw["distribution_type"]
    elif fault == "currency":
        raw["currency"] = "EUR"
    elif fault == "amount":
        action["cash_amount"] = "NaN"
    elif fault == "ex_date":
        action["ex_date"] = date(2026, 9, 2)
    action["payload_sha256"] = "0" * 64 if fault == "hash" else canonical_sha256(raw)
    assert strategy_action_context(action)["handling"] == "CORPORATE_ACTION_REVIEW"


@pytest.mark.parametrize("fault", ["created", "published", "coverage", "scale", "gap", "membership"])
def test_independent_strategy_source_guards_preserve_research_input_integrity(fault):
    from datetime import date
    from realtime.strategy_projection import project_realtime_daily_strategies
    inputs = daily_scanner_fixture()
    bars = [dict(row) for row in inputs.bars]
    if fault in {"created", "published"}:
        bars[0]["created_at" if fault == "created" else "provider_published_at"] = inputs.cutoff + timedelta(seconds=1)
    elif fault == "coverage":
        inputs = replace(inputs, coverage=())
    elif fault == "scale":
        row = next(row for row in bars if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"
                   and row["session_date"] == inputs.session)
        row["close"] *= 1.01
        row["high"] = max(row["high"], row["close"])
    elif fault == "membership":
        inputs = replace(inputs, members=inputs.members[:-1])
    else:
        row = [row for row in bars if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"
               and row["ticker"] == "AAPL"][1]
        bars.remove(row)
    with pytest.raises(ValueError):
        project_realtime_daily_strategies(replace(inputs, bars=tuple(bars)))


@pytest.mark.parametrize("query_data,status", [
    ({}, 422), ({"strategy": "unknown"}, 422), ({"interval": "1h"}, 503), ({"interval": "15m"}, 422),
    ({"refresh": "true"}, 422), ({"ticker": "MSFT"}, 422),
    ({"read_mode": "unknown"}, 422),
    ({"snapshot_id": "invalid"}, 422), ({"snapshot_id": str(UUID(int=99))}, 503),
    ({"cohort_id": str(UUID(int=98))}, 503),
])
def test_independent_strategy_queries_never_substitute_scope_or_pin(query_data, status):
    app, _ = strategy_app()
    params = dict(cohort_id=str(COHORT_ID), **query_data) if "cohort_id" not in query_data else query_data
    if not query_data:
        params = {}
    with TestClient(app) as client:
        assert client.get("/api/strategies/daily", params=params).status_code == status


def test_independent_strategy_pin_survives_rotation_and_expiry_without_current_evidence_following():
    from realtime.portal_projection import build_portal_snapshot
    original = strategy_projection().snapshot
    app, store = strategy_app(clock=lambda: OBSERVED_AT + timedelta(days=10))
    frozen = canonical_json(store.snapshots[str(original.snapshot_id)])
    newer = build_portal_snapshot(cohort_id=original.cohort_id, provider_tier=original.provider_tier,
        snapshot_type=original.snapshot_type, snapshot_key=original.snapshot_key, source_generation=4,
        source_manifest=dict(original.source_manifest, source_generation=4), payload=original.payload, generated_at=original.generated_at)
    store.publish(newer)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), strategy="gaps", snapshot_id=str(original.snapshot_id))
        response = client.get("/api/strategies/daily", params=params)
        assert response.status_code == 200, response.text
        assert response.json()["publication"]["snapshot_id"] == str(original.snapshot_id)
        assert response.json()["publication"]["status"] == "EXPIRED"
        assert client.get("/api/strategies/daily", params=dict(cohort_id=str(COHORT_ID))).json()["publication"]["snapshot_id"] == str(newer.snapshot_id)
    assert canonical_json(store.snapshots[str(original.snapshot_id)]) == frozen
    assert not any("equity_evidence_current" in sql or "screening_current" in sql for sql in store.statements)


@pytest.mark.parametrize("failure", [None, "evidence", "durable", "input_drift"])
def test_strategy_production_commits_evidence_before_publication_and_never_moves_setup_pointers(monkeypatch, failure):
    import realtime.strategy_repository as repository
    from realtime.database_access import RealtimeDatabaseRole
    from realtime.worker_database import RealtimeProcessDatabase
    compute = object.__new__(RealtimeProcessDatabase)
    compute._process_plan = MagicMock(role=RealtimeDatabaseRole.COMPUTE)
    publication = object.__new__(RealtimeProcessDatabase)
    publication._process_plan = MagicMock(role=RealtimeDatabaseRole.PUBLICATION)
    events = []

    @contextmanager
    def transaction(database, **options):
        owner = "compute" if database is compute else "publication"
        events.append(("begin", owner))
        yield owner
        events.append(("commit", owner))

    def evidence(cursor, writes):
        assert cursor == "compute"
        assert len(writes) == 9
        assert all(write.update_evidence_current is False for write in writes)
        assert all(write.definition.dependency_spec["primary_window"] == "EXACT_RETAINED_DAILY" for write in writes)
        events.append(("evidence", cursor))
        if failure == "evidence":
            raise ValueError("Evidence write failed")

    def durable(cursor, manifest, **scope):
        assert ("commit", "compute") in events[events.index(("evidence", "compute")):]
        if failure == "durable":
            raise ValueError("Durable parity failed")

    monkeypatch.setattr(RealtimeProcessDatabase, "transaction", transaction)
    monkeypatch.setattr(repository, "load_realtime_screening_input", lambda cursor, **scope: daily_scanner_fixture())
    monkeypatch.setattr(repository, "project_realtime_daily_strategies", lambda inputs: strategy_projection())
    monkeypatch.setattr(repository, "persist_realtime_equity_materialization_batch_cursor", evidence)
    monkeypatch.setattr(repository, "_validate_durable", durable)
    monkeypatch.setattr(repository, "persist_portal_snapshot_cursor", lambda cursor, snapshot, **options: events.append(("portal", cursor)))
    progress = {}
    if failure:
        with pytest.raises(ValueError):
            repository.materialize_realtime_daily_strategies(compute, publication, cohort_id=COHORT_ID,
                expected_snapshot_id=UUID(int=99) if failure == "input_drift" else None, progress=progress)
        assert not any(event[0] == "portal" for event in events)
        if failure == "input_drift":
            assert not any(event[0] == "evidence" for event in events)
        assert progress.get("strategy_evidence_committed", False) is (failure == "durable")
    else:
        assert repository.materialize_realtime_daily_strategies(compute, publication, cohort_id=COHORT_ID, progress=progress) == strategy_projection().snapshot
        assert events.index(("evidence", "compute")) < events.index(("portal", "publication"))
        assert progress["strategy_evidence_committed"] is True
        assert progress["strategy_evidence_members"] == 9
    if failure in {None, "durable"}:
        assert progress["target_writes"] is True


@pytest.mark.parametrize("contract,key", DAILY_SCANNER_CONTRACTS.items())
def test_stored_routes_are_exact_pinned_producer_outputs_with_zero_computation(contract, key, monkeypatch):
    snapshot = daily_scanner_snapshot()
    app, store = dashboard_app(snapshot=snapshot)
    import realtime.dashboard_projection as producer
    import screeners
    prohibited = MagicMock(side_effect=AssertionError("Stored GET must not compute/fetch/write"))
    monkeypatch.setattr(producer, "compute_default_scanner_snapshots", prohibited)
    monkeypatch.setattr(screeners, "download_historical_data", prohibited)
    for name in ("scan_gap_strategies", "scan_fair_value_gaps", "scan_moving_average_crossover",
                 "scan_momentum_pullback", "scan_bearish_bounce", "scan_fibonacci"):
        monkeypatch.setattr(screeners, name, prohibited)
    before = len(store.statements)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(snapshot.snapshot_id))
        latest = client.get("/api/latest-price-date", params=params).json()["publication"]
        response = client.get(f"/api/scan/{contract}", params=params)
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload.pop("publication") == latest
        assert payload.pop("scanner_basis") == SCANNER_BASIS
        assert payload == json.loads(canonical_json(snapshot.payload["scanners"][key]))
    prohibited.assert_not_called()
    assert all(sql.startswith(("SELECT", "SET")) for sql in store.statements[before:])


@pytest.mark.parametrize("contract", DAILY_SCANNER_CONTRACTS)
@pytest.mark.parametrize("override,status", [
    ({"cohort_id": None}, 422), ({"snapshot_id": None}, 422), ({"interval": "1h"}, 422),
    ({"tickers": "AAPL"}, 422), ({"scan_date": "2026-10-02"}, 422), ({"sector": "Tech"}, 422),
    ({"snapshot_id": "invalid"}, 422), ({"snapshot_id": str(UUID(int=99))}, 503),
    ({"cohort_id": str(UUID(int=98))}, 503),
])
def test_individual_scanners_reject_unsupported_scope_without_substitution(contract, override, status):
    snapshot = daily_scanner_snapshot()
    app, store = dashboard_app(snapshot=snapshot)
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(snapshot.snapshot_id))
    params.update(override)
    params = {key: value for key, value in params.items() if value is not None}
    before = len(store.statements)
    with TestClient(app) as client:
        assert client.get(f"/api/scan/{contract}", params=params).status_code == status
    if status == 422:
        assert len(store.statements) == before


@pytest.mark.parametrize("contract,override", [
    ("fvg", {"lookback": 49}), ("fvg", {"lookback": "invalid"}),
    ("ma-crossover", {"short_period": 10}), ("ma-crossover", {"long_period": 50}),
    ("fibonacci", {"min_swing_pct": 3}), ("fibonacci", {"min_swing_pct": "nan"}),
])
def test_non_default_parameters_are_unavailable(contract, override):
    snapshot = daily_scanner_snapshot()
    app, _ = dashboard_app(snapshot=snapshot)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(snapshot.snapshot_id), **override)
        assert client.get(f"/api/scan/{contract}", params=params).status_code == 422


@pytest.mark.parametrize("fault", ["old", "fvg_missing", "basis", "count", "group", "clock", "cohort_row", "fvg_shape",
                                   "reference", "action", "hash"])
def test_missing_incompatible_or_expired_scanners_never_advance_half_a_gap_page(fault):
    app, store = dashboard_app(snapshot=daily_scanner_snapshot())
    stored = next(iter(store.snapshots.values()))
    stored["payload"] = deepcopy(stored["payload"])
    stored["source_manifest"] = deepcopy(stored["source_manifest"])
    payload = stored["payload"]
    if fault == "old":
        del payload["scanners"]
        del payload["scanner_basis"]
        del stored["source_manifest"]["scanner_contracts"]
        del stored["source_manifest"]["scanner_basis"]
    elif fault == "fvg_missing":
        del payload["scanners"]["SCAN_FVG_1D_50"]
    elif fault == "basis":
        payload["scanner_basis"]["volume"] = "RAW"
    elif fault in {"count", "group", "clock", "cohort_row", "fvg_shape"}:
        scanner = payload["scanners"]["SCAN_FVG_1D_50"]
        if fault == "count":
            scanner["total_signals"] += 1
        elif fault == "group":
            scanner["results_by_type"] = {}
        elif fault == "clock":
            scanner["scan_datetime"] = (OBSERVED_AT + timedelta(seconds=1)).isoformat()
        elif fault == "cohort_row":
            scanner["results"][0]["ticker"] = "OUTSIDE"
        else:
            del scanner["results"][0]["fvg_high"]
    elif fault == "reference":
        store.reference_drift = True
    elif fault == "action":
        store.action_drift = True
    stored["payload_sha256"] = "0" * 64 if fault == "hash" else canonical_sha256(payload)
    stored["source_manifest_sha256"] = canonical_sha256(stored["source_manifest"])
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(stored["snapshot_id"]))
        for contract in ("gaps", "fvg"):
            assert client.get(f"/api/scan/{contract}", params=params).status_code == 503
        if fault == "old":
            for route in ("/api/scan/all", "/api/tickers/overview", "/api/tickers/setup-summary"):
                assert client.get(route, params=params).status_code == 200


@pytest.mark.parametrize("delay", ["deadline", "provider"])
def test_delayed_scanners_remain_browsable_with_expired_publication_status(delay):
    app, store = dashboard_app(
        snapshot=daily_scanner_snapshot(),
        clock=lambda: OBSERVED_AT + timedelta(days=10 if delay == "provider" else 0),
    )
    stored = next(iter(store.snapshots.values()))
    if delay == "deadline":
        stored["valid_until"] = OBSERVED_AT
    params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(stored["snapshot_id"]))
    with TestClient(app) as client:
        for contract in ("gaps", "fvg"):
            response = client.get(f"/api/scan/{contract}", params=params)
            assert response.status_code == 200
            assert response.json()["publication"]["status"] == "EXPIRED"


def test_individual_pins_survive_pointer_rotation_without_rewriting_old_bundle():
    original = daily_scanner_snapshot()
    app, store = dashboard_app(snapshot=original, clock=lambda: OBSERVED_AT + timedelta(minutes=1))
    frozen = canonical_json(next(iter(store.snapshots.values()))["payload"])
    newer = produce_realtime_dashboard(replace(daily_scanner_fixture(), source_generation=4,
                                              cutoff=OBSERVED_AT + timedelta(minutes=1)))
    store.publish(newer)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(original.snapshot_id))
        for contract in DAILY_SCANNER_CONTRACTS:
            response = client.get(f"/api/scan/{contract}", params=params)
            assert response.status_code == 200, response.text
            assert response.json()["publication"]["snapshot_id"] == str(original.snapshot_id)
        latest = client.get("/api/latest-price-date", params={"cohort_id": str(COHORT_ID)})
        assert latest.json()["publication"]["snapshot_id"] == str(newer.snapshot_id)
    assert canonical_json(store.snapshots[str(original.snapshot_id)]["payload"]) == frozen


def test_daily_to_weekly_preserves_partial_current_week():
    frames, _ = dashboard_frames(daily_scanner_fixture())
    frame = frames["MSFT"].iloc[:-2]
    assert frame.index[-1].weekday() == 2
    snapshots = compute_default_scanner_snapshots(["MSFT"], {"MSFT": frame}, {}, observed_at=OBSERVED_AT)
    row = snapshots["SCAN_MA_1D_9_21"]["results"][0]
    weekly = frame.close.resample("W-FRI").last().dropna()
    assert weekly.index[-1] > frame.index[-1]
    assert weekly.iloc[-1] == frame.close.iloc[-1]
    assert row["weekly_short_ma"] == round(weekly.rolling(9).mean().iloc[-1], 2)
    assert row["weekly_long_ma"] == round(weekly.rolling(21).mean().iloc[-1], 2)


@pytest.mark.parametrize("missing", [None, "fvg", "scanner_manifest", "setup", "setup_evidence", "basis"])
def test_daily_ticker_capability_requires_complete_retained_contracts_without_repair(missing):
    app, store = dashboard_app(snapshot=daily_scanner_snapshot())
    stored = next(iter(store.snapshots.values()))
    stored["payload"] = deepcopy(stored["payload"])
    stored["source_manifest"] = deepcopy(stored["source_manifest"])
    if missing == "fvg":
        del stored["payload"]["scanners"]["SCAN_FVG_1D_50"]
    elif missing == "scanner_manifest":
        stored["source_manifest"]["scanner_contracts"].remove("SCAN_FVG_1D_50")
    elif missing == "setup":
        del stored["payload"]["setup_summary"]
    elif missing == "setup_evidence":
        del stored["payload"]["setup_evidence"]
    elif missing == "basis":
        stored["payload"]["scanner_basis"]["volume"] = "RAW"
    stored["payload_sha256"] = canonical_sha256(stored["payload"])
    stored["source_manifest_sha256"] = canonical_sha256(stored["source_manifest"])
    frozen = canonical_json((stored["payload"], stored["source_manifest"]))
    before = len(store.statements)
    with TestClient(app) as client:
        params = dict(cohort_id=str(COHORT_ID), snapshot_id=str(stored["snapshot_id"]))
        response = client.get("/api/latest-price-date", params=params)
        assert response.status_code == 200, response.text
        assert response.json()["publication"]["strategy_ticker_available"] is (missing is None)
        assert client.get("/api/scan/all", params=params).status_code == 200
    assert canonical_json((stored["payload"], stored["source_manifest"])) == frozen
    assert all(sql.startswith(("SELECT", "SET")) for sql in store.statements[before:])
