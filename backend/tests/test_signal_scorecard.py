def test_batch_report_retains_audited_universe_discrepancy(monkeypatch, tmp_path):
    import json
    from research import frozen_daily_study as launcher
    from scripts.report_equity_signal_scorecard import load_batch_scope

    issue = dict(session="2024-03-04", universe_run_id="audited-run", member_count_discrepancy={
        "declared_members": 1504, "stored_members": 1503,
    })
    merged = tmp_path / "merged.json"
    outcome = tmp_path / "outcomes.json"
    events = tmp_path / "events.jsonl"
    merged.write_text(json.dumps(dict(coverage_count=1, coverage_counts={"NO_SIGNAL": 1}, coverage_sha256="hash",
                                      universe_selection={"runs": [issue]})))
    outcome.write_text(json.dumps({"outcomes": {"policies": []}}))
    events.write_text("")
    (tmp_path / "plan.json").write_text(json.dumps(dict(contract="MANUAL_DAILY_ADAPTER_REPLAY_V2", runner_code_sha256={},
        groups=[dict(name="sample-1/composite-scanners-1d-v1", outcome_command=[str(outcome)], events=str(events), report=str(merged))])))
    monkeypatch.setattr(launcher, "verify_replays", lambda plan: None)
    monkeypatch.setattr(launcher, "completed", lambda *args: True)
    assert load_batch_scope(tmp_path)["coverage"][0]["universe_count_discrepancies"] == [issue]
def test_batch_cells_include_all_declared_adapters_even_without_events():
    from research.signal_scorecard import adapter_signal_cells
    from research.frozen_daily_study import CURRENT_ADAPTERS

    cells = adapter_signal_cells(CURRENT_ADAPTERS)
    keys = {(row["source_name"], row["source_version"], row["direction"], row["horizon_key"], row["return_mode"]) for row in cells}
    assert len(keys) == len(cells)
    assert len(adapter_signal_cells(["composite-scanners-1d-v1"])) == 84
    assert {"MA_CROSSOVER_9_21", "GAP_ENTRY_FILL", "PATTERN_FALLING_WEDGE_BOUNDARY_BREAK"} <= {row["source_name"] for row in cells}


def test_batch_reads_are_exact_subject_and_policy_scoped():
    from scripts.report_equity_signal_scorecard import read_batch_measurements
    from unittest.mock import MagicMock
    from datetime import datetime, timezone

    cursor = MagicMock()
    cursor.fetchall.side_effect = [[{"evidence_id": "one"}], [{"outcome_id": "result"}]]
    events, outcomes = read_batch_measurements(cursor, {"subjects": ["one"], "policy_ids": ["policy"]}, datetime.now(timezone.utc))
    assert events == [{"evidence_id": "one"}] and outcomes == [{"outcome_id": "result"}]
    assert "evidence.evidence_id=ANY" in cursor.execute.call_args_list[0].args[0]
    assert "outcome_policy_id=ANY" in cursor.execute.call_args_list[1].args[0]
    assert cursor.execute.call_args_list[1].args[1][:2] == (["policy"], ["one"])
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path


def test_retained_sample_reader_preserves_original_membership_and_hashes():
    from research.frozen_daily_study import ROOT, SAMPLE_FILES, file_sha256, load_samples
    samples, hashes = load_samples(ROOT / "backend/research/inputs")
    assert len(samples["1"]) == len(samples["2"]) == 300
    assert not set(samples["1"]) & set(samples["2"])
    assert hashes == {name: file_sha256(ROOT / "backend/research/inputs" / name) for name in SAMPLE_FILES.values()}


def test_retained_completion_reader_rejects_changed_commands_and_artifacts(tmp_path):
    import json
    from research.frozen_daily_study import completed, file_sha256
    report = tmp_path / "report.json"
    command = ["retired-replay"]
    assert not completed(command, report, (report,))
    report.write_text("{}")
    report.with_suffix(".complete.json").write_text(json.dumps({
        "command": command, "sha256": {str(report): file_sha256(report)},
    }))
    assert completed(command, report, (report,))
    with pytest.raises(ValueError, match="command differs"):
        completed(["other"], report, (report,))
    report.write_text('{"changed": true}')
    with pytest.raises(ValueError, match="artifact changed"):
        completed(command, report, (report,))


@pytest.mark.parametrize("filename,batch", [
    ("backend/scripts/run_daily_strategy_backtests.py", "legacy-daily-harness"),
    ("backend/equity/scanner_research.py", "stock-research-retirement"),
])
def test_retired_source_resolution_is_allowlisted_and_hash_checked(tmp_path, monkeypatch, filename, batch):
    import json
    from research.frozen_daily_study import file_sha256, source_path
    monkeypatch.delenv("STOCK_SCREENER_LEGACY_ARCHIVE_ROOT", raising=False)
    archive = tmp_path / "legacy" / batch
    saved = archive / "files" / filename
    saved.parent.mkdir(parents=True)
    saved.write_text("original")
    (archive / "manifest.json").write_text(json.dumps({"files": [{
        "original": filename, "archived": "files/" + filename, "sha256": file_sha256(saved),
    }]}))
    assert source_path(filename.replace("/", "\\"), root=tmp_path) == saved
    assert source_path("backend/research/strategy_v2.py", root=tmp_path) == tmp_path / "backend/research/strategy_v2.py"
    saved.write_text("changed")
    with pytest.raises(ValueError, match="archive checksum"):
        source_path(filename, root=tmp_path)


def test_external_archive_is_explicit_and_never_falls_back(tmp_path, monkeypatch):
    import json
    from research.frozen_daily_study import file_sha256, source_path
    root = tmp_path / "repository"
    external = tmp_path / "external-archive"
    filename = "backend/scripts/run_daily_strategy_backtests.py"
    batch = "legacy-daily-harness"
    saved = external / batch / "files" / filename
    saved.parent.mkdir(parents=True)
    saved.write_text("original")
    manifest = {"files": [{"original": filename, "archived": "files/" + filename,
                           "sha256": file_sha256(saved)}]}
    (external / batch / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.delenv("STOCK_SCREENER_LEGACY_ARCHIVE_ROOT", raising=False)
    assert not (root / "legacy").exists()
    assert source_path(filename, root=root, archive_root=external) == saved
    monkeypatch.setenv("STOCK_SCREENER_LEGACY_ARCHIVE_ROOT", str(external))
    assert source_path(filename, root=root) == saved
    with pytest.raises(ValueError, match="absolute path"):
        source_path(filename, root=root, archive_root=Path("relative-archive"))
    with pytest.raises(FileNotFoundError, match="cannot skip missing"):
        source_path(filename, root=root, archive_root=tmp_path / "missing")
    saved.write_text("tampered")
    with pytest.raises(ValueError, match="archive checksum"):
        source_path(filename, root=root)
    current = root / filename
    current.parent.mkdir(parents=True)
    current.write_text("current source is authoritative")
    assert source_path(filename, root=root) == current
    monkeypatch.delenv("STOCK_SCREENER_LEGACY_ARCHIVE_ROOT")
    current.unlink()
    with pytest.raises(FileNotFoundError, match="STOCK_SCREENER_LEGACY_ARCHIVE_ROOT"):
        source_path(filename, root=root)

from research.signal_scorecard import (
    registered_signal_cells, evidence_role_contract, coverage_status, outcome_usability,
    deduplicate_lifecycles, daily_statistics, build_daily_scorecard,
    policy_event_scope, matching_cell_policies,
)


def test_registered_grid_retains_all_daily_sides_modes_and_horizons():
    cells = registered_signal_cells()
    assert len(cells) == 312
    daily = [row for row in cells if row["interval"] == "1d"]
    assert len(daily) == 84
    assert {row["horizon_bars"] for row in daily} == {5, 10, 21}
    assert {row["direction"] for row in daily} == {-1, 1}
    assert {row["return_mode"] for row in daily} == {"DIRECTIONAL_HORIZON", "RECOMMENDATION_PLAN"}
    assert not any(row["source_name"] == "sma200_reclaim_rejection" and row["interval"] in ("30m", "1h") for row in cells)
    assert len({tuple(row.values()) for row in cells}) == len(cells)


def test_location_and_participation_are_not_independent_direction_votes():
    assert evidence_role_contract("SCANNER_RESULT", "LOCATION") == "CONDITIONAL_LOCATION_NOT_STANDALONE_DIRECTION"
    assert evidence_role_contract("PATTERN_OBSERVATION", "PARTICIPATION") == "INCREMENTAL_PARTICIPATION_NOT_DIRECTIONAL_VOTE"
    assert evidence_role_contract("TRADE_SETUP", "SETUP") == "COMPOSITE_SETUP_REQUIRES_OWN_QUALIFICATION"


def test_missing_coverage_is_neither_zero_return_nor_a_failure_of_signal():
    assert coverage_status(0, 0, 0) == "NO_EVENTS"
    assert coverage_status(10, 0, 0) == "NO_RETAINED_OUTCOMES"
    assert coverage_status(10, 10, 0) == "NO_USABLE_OUTCOMES"
    assert coverage_status(10, 10, 8) == "PARTIAL_OUTCOMES"
    assert coverage_status(10, 10, 10) == "COVERED_NOT_QUALIFIED"
    with pytest.raises(ValueError):
        coverage_status(10, 11, 5)


@pytest.fixture
def event_outcome():
    signal = datetime(2025, 1, 10, 21, tzinfo=timezone.utc)
    event = dict(evidence_id="event", ticker="TEST", security_id="security", source_name="breakout_expansion", source_version="1.0",
                 interval="1d", direction=1, origin="LIVE_OBSERVED", run_purpose="ORIGINAL", lifecycle_key="one", quality_state="RESEARCH_ONLY",
                 market_time=signal, observed_at=signal, quality_codes=[])
    policy = dict(outcome_policy_id="policy", benchmark_policy={"primary": "SECTOR"}, source_name="breakout_expansion", source_version="1.0",
                  interval="1d", evidence_type="SCANNER_RESULT", horizons={"5d": 5}, success_definition={}, policy_key="key", policy_sha256="hash",
                  effective_from=signal-timedelta(days=1), effective_to=None, cost_model={"round_trip_bps": 4})
    outcome = dict(subject_evidence_id="event", outcome_policy_id="policy", horizon_key="5d", is_stale=False, entry_status="ENTERED",
                   signal_time=signal, entry_time=signal+timedelta(days=3), exit_time=signal+timedelta(days=10),
                   outcome_available_at=signal+timedelta(days=10, minutes=15), created_at=signal+timedelta(days=10, minutes=20),
                   net_return=.01, net_alpha=.005, sector_net_alpha=.003, mae_pct=-2, mfe_pct=3, quality_codes=[])
    return event, outcome, policy, signal+timedelta(days=20)


def test_outcome_checks_identity_benchmark_and_causal_clocks(event_outcome):
    event, outcome, policy, cutoff = event_outcome
    assert outcome_usability(event, outcome, policy, cutoff) == "USABLE"
    assert outcome_usability(event, dict(outcome, sector_net_alpha=None), policy, cutoff) == "MISSING_RETURN_OR_PRIMARY_BENCHMARK"
    assert outcome_usability(event, dict(outcome, is_stale=True), policy, cutoff) == "STALE_OUTCOME"
    assert outcome_usability(event, dict(outcome, entry_time=event["observed_at"]), policy, cutoff) == "INVALID_CAUSAL_TIMING"
    assert outcome_usability(event, dict(outcome, outcome_policy_id="other"), policy, cutoff) == "IDENTITY_MISMATCH"
    assert outcome_usability(event, dict(outcome, entry_status="NO_LIQUID_BAR"), policy, cutoff) == "ENTRY_NO_LIQUID_BAR"


def test_lifecycle_dedup_never_pools_versions_or_origins(event_outcome):
    event = event_outcome[0]
    repeated = dict(event, evidence_id="repeat", market_time=event["market_time"]+timedelta(days=3), observed_at=event["observed_at"]+timedelta(days=3))
    selected, dropped = deduplicate_lifecycles([repeated, event, dict(event, evidence_id="new-version", source_version="2.0")])
    assert dropped == 1 and {row["evidence_id"] for row in selected} == {"event", "new-version"}


def test_daily_scorecard_retains_empty_cells_and_policy_revisions(event_outcome):
    event, outcome, policy, cutoff = event_outcome
    config = dict(later_start="2025-01-01", minimum_events=100, minimum_independent_periods=40)
    other = dict(policy, outcome_policy_id="second", cost_model={"round_trip_bps": 25})
    rows, _ = build_daily_scorecard([event], [outcome], [policy, other], {"SAMPLE_1": ["TEST"]}, config, cutoff)
    relevant = [row for row in rows if row["source_name"] == "breakout_expansion" and row["direction"] == 1 and row["horizon_key"] == "5d"
                and row["return_mode"] == "DIRECTIONAL_HORIZON" and row["origin"] == "LIVE_OBSERVED" and row["sample_scope"] == "SAMPLE_1"]
    assert len(relevant) == 2
    by_id = {row["outcome_policy_id"]: row for row in relevant}
    assert by_id["policy"]["usable_outcomes"] == 1 and by_id["second"]["usable_outcomes"] == 0
    assert by_id["policy"]["evidence_status"] == "INSUFFICIENT_EVIDENCE"
    assert all(row["individual_probability"] is None and row["quality_score"] is None for row in rows)
    assert {row["source_name"] for row in rows} == {row["source_name"] for row in registered_signal_cells()}
    assert by_id["policy"]["fdr_family_cells"] == 86


def test_missing_outcome_does_not_enter_measured_mean(event_outcome):
    event, outcome, policy, cutoff = event_outcome
    missing = dict(event, evidence_id="missing", ticker="OTHER", lifecycle_key="another", security_id="other")
    rows, _ = build_daily_scorecard([event, missing], [outcome], [policy], {},
                                   dict(later_start="2025-01-01", minimum_events=100, minimum_independent_periods=40), cutoff)
    row = next(row for row in rows if row["outcome_policy_id"] == "policy" and row["direction"] == 1 and row["origin"] == "LIVE_OBSERVED")
    assert row["data_status"] == "PARTIAL_OUTCOMES" and row["outcome_states"]["NO_RETAINED_OUTCOME"] == 1
    assert row["statistics"]["mean_net_return"] == .01


def test_replay_policy_link_retains_missing_denominator_without_backdating_live_policy(event_outcome):
    event, outcome, policy, cutoff = event_outcome
    historical = dict(event, origin="HISTORICAL_RECONSTRUCTED", run_purpose="RECONSTRUCTED_LEDGER")
    missing = dict(historical, evidence_id="missing", ticker="OTHER", security_id="other", lifecycle_key="another")
    policy = dict(policy, effective_from=cutoff)
    links = {("event", "policy", "5d"): outcome}
    selected, details = policy_event_scope([historical, missing], policy, "5d", links)
    assert selected == [historical, missing]
    assert details["outside_policy_window"] == details["retrospective_policy_events"] == 2
    assert policy_event_scope([event], policy, "5d", links)[0] == []
    assert policy_event_scope([historical, missing], policy, "10d", links)[0] == []
    rows, _ = build_daily_scorecard([historical, missing], [outcome], [policy], {},
                                   dict(later_start="2025-01-01", minimum_events=100, minimum_independent_periods=40), cutoff)
    row = next(row for row in rows if row["origin"] == "HISTORICAL_RECONSTRUCTED" and row["direction"] == 1 and row["outcome_policy_id"] == "policy")
    assert row["policy_eligible_events"] == 2 and row["usable_outcomes"] == 1
    assert row["outcome_states"]["NO_RETAINED_OUTCOME"] == 1 and row["retrospective_policy_events"] == 2
    assert row["publication_state"] == "REPORT_ONLY_NOT_PUBLISHED"


def test_registry_policy_links_match_mode_horizon_and_version_without_events(event_outcome):
    policy = event_outcome[2]
    plan = dict(policy, outcome_policy_id="plan", success_definition={"exit_model": "FIRST_STOP_TARGET_OR_HORIZON_CLOSE"})
    cell = dict(source_name=policy["source_name"], source_version=policy["source_version"], interval="1d",
                horizon_key="5d", horizon_bars=5, return_mode="DIRECTIONAL_HORIZON")
    assert matching_cell_policies(cell, [policy, plan]) == [policy]
    assert matching_cell_policies(dict(cell, return_mode="RECOMMENDATION_PLAN"), [policy, plan]) == [plan]
    assert matching_cell_policies(dict(cell, horizon_key="10d", horizon_bars=10), [policy, plan]) == []
    assert matching_cell_policies(dict(cell, source_version="other"), [policy, plan]) == []


def test_scorecard_runner_uses_readonly_snapshot_and_keeps_empty_registry(monkeypatch, tmp_path):
    import json
    from pathlib import Path
    from unittest.mock import MagicMock
    import scripts.report_equity_signal_scorecard as script

    inputs = Path(__file__).resolve().parents[2] / "backend/research/inputs"
    config = json.loads((inputs / "equity_signal_scorecard_config.json").read_text())
    for index, name in enumerate(config["sample_manifests"]):
        (tmp_path / name).write_text(json.dumps(dict(sampled_tickers=[f"S{index}_{number}" for number in range(300)])))
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    cursor = MagicMock()
    cursor.fetchone.return_value = {"read_only": "on", "cutoff": datetime.now(timezone.utc)}
    cursor.fetchall.return_value = []
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    monkeypatch.setattr(script, "get_db_cursor", lambda: manager)
    report = script.run_report(path)
    assert len(report["registered_coverage_matrix"]) == 312
    assert len(report["daily_scorecard"]) == 84 * 2 * 3
    assert report["production_mutations"] is False
    statements = [call.args[0].strip() for call in cursor.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert all(statement.startswith(("SELECT", "SET", "WITH")) for statement in statements)


def test_storage_recovery_requires_exact_identity_and_both_hashes():
    from types import SimpleNamespace
    from scripts.repair_equity_feature_storage import EVIDENCE_ID, verify_reconstruction
    from equity.polygon import sha256_json

    source_ids = ["source-one", "source-two"]
    payload_hash = "a" * 64
    key = "original-key:" + sha256_json(source_ids) + ":" + payload_hash
    recovered = SimpleNamespace(evidence_id=EVIDENCE_ID, evidence_key=key,
                                payload_sha256=payload_hash, source_revision_ids=source_ids)
    verify_reconstruction(recovered, key)
    for field, value in (("evidence_id", "different"), ("evidence_key", "different"),
                         ("payload_sha256", "b" * 64), ("source_revision_ids", ["source-two", "source-one"])):
        changed = SimpleNamespace(**(vars(recovered) | {field: value}))
        with pytest.raises(ValueError, match="exact evidence ID"):
            verify_reconstruction(changed, key)


def test_storage_probe_rolls_back_only_its_failed_read():
    from unittest.mock import MagicMock
    from scripts.audit_equity_evidence_storage import probe

    cursor = MagicMock()
    cursor.execute.side_effect = [None, ValueError("unreadable test value"), None, None]
    result, error = probe(cursor, "SELECT quality_codes FROM equity_evidence")
    assert result is None and error["type"] == "ValueError"
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements == ["SAVEPOINT storage_probe", "SELECT quality_codes FROM equity_evidence",
                          "ROLLBACK TO SAVEPOINT storage_probe", "RELEASE SAVEPOINT storage_probe"]