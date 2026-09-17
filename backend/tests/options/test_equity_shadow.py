from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from options.analytics.equity_shadow import (
    equity_alignment, horizon_exit, select_shadow_pair, measure_shadow_outcome, first_decision_cohorts, summarize_shadow_pairs,
)


def clock(day, hour=20):
    return datetime(2026, 9, day, hour, tzinfo=timezone.utc)


@pytest.fixture
def candidate():
    return dict(candidate_id="first", matrix_id="matrix", underlying="TEST", strategy_name="DIRECTIONAL_LONG_PREMIUM",
                structure_type="LONG_CALL", candidate_kind="SINGLE_CONTRACT", candidate_rank=1, capital_at_risk=100,
                market_data_time=clock(11, 16), observed_time=clock(11, 17), primary_evidence={"directional_thesis": "BULLISH"})


@pytest.fixture
def evidence():
    return dict(evidence_id="setup", ticker="TEST", interval_key="1d", evidence_type="TRADE_SETUP",
                availability_mode="LIVE_OBSERVED", market_time=clock(10), observed_at=clock(10, 21), created_at=clock(10, 21),
                quality_state="VALID", lifecycle_status="CONFIRMED", direction=1, model_version="setup-v1", valid_until=None)


def test_alignment_uses_only_actual_decision_visible_evidence(candidate, evidence):
    future = dict(evidence, evidence_id="future", direction=-1, observed_at=clock(12), created_at=clock(12))
    result = equity_alignment(candidate, [evidence, future])
    assert result["alignment"] == "ALIGNED" and result["equity_evidence_id"] == "setup"
    assert result["qualification"] == "UNQUALIFIED_RESEARCH_CONTEXT"
    assert equity_alignment(candidate, [future])["alignment"] == "UNAVAILABLE"
    assert equity_alignment(candidate, [dict(evidence, availability_mode="HISTORICAL_RECONSTRUCTED")])["alignment"] == "UNAVAILABLE"


def test_alignment_does_not_promote_stale_conflicted_or_detector_evidence(candidate, evidence):
    assert equity_alignment(candidate, [dict(evidence, market_time=clock(8))])["alignment"] == "UNAVAILABLE"
    assert equity_alignment(candidate, [dict(evidence, lifecycle_status="CONFLICTED")])["alignment"] == "CONFLICTED"
    assert equity_alignment(candidate, [dict(evidence, direction=-1)])["alignment"] == "OPPOSED"
    assert equity_alignment(dict(candidate, candidate_kind="RESEARCH_ONLY"), [evidence])["alignment"] == "NOT_DIRECTIONAL"
    assert equity_alignment(candidate, [evidence, dict(evidence, evidence_id="ambiguous")])["alignment"] == "UNAVAILABLE"
    assert equity_alignment(candidate, [dict(evidence, lifecycle_status="INVALIDATED")])["alignment"] == "UNAVAILABLE"
    assert equity_alignment(candidate, [dict(evidence, valid_until=candidate["market_data_time"])])["alignment"] == "UNAVAILABLE"
    assert equity_alignment(candidate, [dict(evidence, created_at=clock(12))])["alignment"] == "UNAVAILABLE"


def test_horizon_uses_trading_sessions_and_rejects_expiry_at_exit(candidate):
    assert horizon_exit(clock(4), 5).date().isoformat() == "2026-09-14"
    legs = {"first": [dict(expiration_date="2026-09-18")]}
    result = select_shadow_pair([candidate], legs, {"first": {"alignment": "ALIGNED"}}, 5)
    assert result["option_only_candidate_id"] is None
    assert result["exclusions"][0]["reason"] == "EXPIRATION_NOT_BEYOND_PLANNED_EXIT"
    with pytest.raises(ValueError, match="fixed"):
        horizon_exit(clock(11), 3)


def test_shadow_selection_preserves_rank_and_does_not_use_future_returns(candidate):
    second = dict(candidate, candidate_id="second", candidate_rank=2, future_return=-100)
    legs = {identity: [dict(expiration_date="2026-12-18")] for identity in ("first", "second")}
    annotations = {"first": {"alignment": "OPPOSED"}, "second": {"alignment": "ALIGNED"}}
    result = select_shadow_pair([candidate, second], legs, annotations, 10)
    assert result["option_only_candidate_id"] == "first" and result["equity_aligned_candidate_id"] == "second"
    second["future_return"] = 100
    assert select_shadow_pair([candidate, second], legs, annotations, 10) == result
    assert not result["execution_gates_overridden"]


@pytest.fixture
def marks(candidate):
    from options.outcomes import configured_valuation_policy
    policy = configured_valuation_policy()
    candidate = dict(candidate, candidate_id=str(uuid4()), capital_at_risk=200)
    leg = dict(contract_id=1, leg_index=0, side="BUY", ratio=1, multiplier=100, model_mark=2,
               expiration_date="2026-12-18", strike=100, contract_type="CALL", local_iv=.3,
               source_market_time=candidate["market_data_time"], entry_first_observed_at=candidate["observed_time"],
               valuation_policy_sha256=policy.policy_sha256, mark_source=policy.primary_model_mark_source.value)
    target = horizon_exit(candidate["market_data_time"], 5)
    snapshot = dict(contract_id=1, shares_per_contract=100, model_mark=3, expiration_date="2026-12-18", strike=100,
                    contract_type="CALL", local_iv=.25, snapshot_id=str(uuid4()), batch_id=str(uuid4()), market_data_time=target, scheduled_cycle=target,
                    first_observed_at=target + timedelta(minutes=15), created_at=target + timedelta(minutes=15),
                    mark_market_data_time=target, spot_market_data_time=target, revision=1,
                    valuation_policy_sha256=policy.policy_sha256, mark_source=policy.primary_model_mark_source.value)
    return candidate, [leg], [snapshot], policy, target


def test_option_shadow_uses_contract_prices_commission_and_maturity(marks):
    candidate, legs, snapshots, policy, target = marks
    assert measure_shadow_outcome(candidate, legs, snapshots, 5, candidate["observed_time"], policy)["status"] == "NOT_MATURE"
    result = measure_shadow_outcome(candidate, legs, snapshots, 5, target + timedelta(days=1), policy)
    assert result["gross_pnl"] == 100
    assert result["net_pnl"] == pytest.approx(100 - 2 * float(policy.commission_per_contract_per_side))
    assert result["net_return"] == pytest.approx(result["net_pnl"] / 200)
    assert result["holding_sessions"] == 5 and result["iv_changes"][0]["exit_iv"] == .25


@pytest.mark.parametrize("field,value", [("shares_per_contract", 150), ("strike", 101), ("valuation_policy_sha256", "f" * 64), ("model_mark", None)])
def test_shadow_outcomes_reject_changed_contract_or_policy(marks, field, value):
    candidate, legs, snapshots, policy, target = marks
    snapshots[0][field] = value
    assert measure_shadow_outcome(candidate, legs, snapshots, 5, target + timedelta(days=1), policy)["status"] == "COHERENT_EXIT_MARK_UNAVAILABLE"


def test_shadow_rejects_stale_prices_and_incoherent_multileg_batches(marks):
    candidate, legs, snapshots, policy, target = marks
    second_leg = dict(legs[0], contract_id=2, leg_index=1, side="SELL")
    second_mark = dict(snapshots[0], contract_id=2, snapshot_id=str(uuid4()), batch_id=str(uuid4()))
    assert measure_shadow_outcome(candidate, legs + [second_leg], snapshots + [second_mark], 5, target + timedelta(days=1), policy)["status"] == "COHERENT_EXIT_MARK_UNAVAILABLE"
    snapshots[0]["mark_market_data_time"] = target - timedelta(hours=2)
    assert measure_shadow_outcome(candidate, legs, snapshots, 5, target + timedelta(days=1), policy)["status"] == "COHERENT_EXIT_MARK_UNAVAILABLE"
    legs[0]["entry_first_observed_at"] = target
    assert measure_shadow_outcome(candidate, legs, snapshots, 5, target + timedelta(days=1), policy)["status"] == "ENTRY_PRICE_NOT_DECISION_VISIBLE"


def test_first_matrix_selection_ignores_later_outcomes(candidate):
    later = dict(candidate, candidate_id="later", matrix_id="later", observed_time=clock(11, 18), future_return=999)
    cohorts = first_decision_cohorts([later, candidate])
    assert len(cohorts) == 1 and cohorts[0][1] == [candidate]


def test_shadow_summary_separates_missing_outcomes_cash_abstentions_and_horizons():
    base = dict(strategy_name="test", underlying="TEST", structure_type="LONG_CALL", holding_sessions=5, entry_session="2026-09-11",
                option_only_candidate_id="first", option_only_contract_ids=[1])
    measured = dict(status="MEASURED_INDICATIVE", net_return=.1)
    missing = dict(status="COHERENT_EXIT_MARK_UNAVAILABLE", net_return=None)
    pairs = [base | dict(option_only_outcome=measured, equity_aligned_outcome=dict(status="ABSTAIN_CASH", net_return=0)),
             base | dict(option_only_outcome=missing, equity_aligned_outcome=measured),
             base | dict(holding_sessions=10, option_only_outcome=measured, equity_aligned_outcome=measured)]
    results = summarize_shadow_pairs(pairs)
    assert len(results) == 2
    short = results[0]
    assert short["option_only_measured"] == 1 and short["matched_measured"]["pairs"] == 0
    assert short["common_opportunity_with_cash_abstention"]["pairs"] == 1
    assert short["common_opportunity_with_cash_abstention"]["equal_entry_session_mean_difference"] == -.1
    assert short["repeated_package_occurrences"] == 1
    assert results[1]["matched_measured"]["equal_entry_session_mean_difference"] == 0


def test_coherent_debit_spread_payoff_and_future_exit_rows(marks):
    candidate, legs, snapshots, policy, target = marks
    candidate["capital_at_risk"] = 150
    short = dict(legs[0], contract_id=2, leg_index=1, side="SELL", model_mark=.5, strike=105)
    short_exit = dict(snapshots[0], contract_id=2, strike=105, model_mark=1, snapshot_id=str(uuid4()))
    result = measure_shadow_outcome(candidate, legs + [short], snapshots + [short_exit], 5, target + timedelta(days=1), policy)
    assert result["gross_pnl"] == 50
    assert result["estimated_commission"] == 4 * float(policy.commission_per_contract_per_side)
    short_exit["created_at"] = target + timedelta(days=2)
    assert measure_shadow_outcome(candidate, legs + [short], snapshots + [short_exit], 5, target + timedelta(days=1), policy)["status"] == "COHERENT_EXIT_MARK_UNAVAILABLE"


def test_close_scheduled_batch_allows_policy_age_but_not_another_cycle(marks):
    candidate, legs, snapshots, policy, target = marks
    source = target - timedelta(seconds=min(840, policy.maximum_source_age_seconds))
    snapshots[0].update(market_data_time=source, mark_market_data_time=source, spot_market_data_time=source)
    assert measure_shadow_outcome(candidate, legs, snapshots, 5, target + timedelta(days=1), policy)["status"] == "MEASURED_INDICATIVE"
    snapshots[0]["scheduled_cycle"] = target - timedelta(minutes=15)
    assert measure_shadow_outcome(candidate, legs, snapshots, 5, target + timedelta(days=1), policy)["status"] == "COHERENT_EXIT_MARK_UNAVAILABLE"


def test_readonly_report_handles_no_candidates_without_touching_workers(monkeypatch, tmp_path):
    import scripts.report_option_equity_shadow as script
    from unittest.mock import MagicMock
    from pathlib import Path

    cursor = MagicMock()
    cursor.fetchone.return_value = dict(read_only="on", cutoff=clock(12))
    cursor.fetchall.return_value = []
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    monkeypatch.setattr(script, "get_db_cursor", lambda: manager)
    config_path = Path(__file__).resolve().parents[3] / "backend/research/inputs/option_equity_shadow_config.json"
    output = tmp_path / "report.json"
    result = script.run_report(config_path, output)
    assert result["raw_candidate_rows"] == 0 and result["summaries"] == []
    assert result["production_mutations"] is False and result["equity_paper_tracker_modified"] is False
    queries = [call.args[0].strip() for call in cursor.execute.call_args_list]
    assert queries[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert all(query.startswith(("SET ", "SELECT ", "WITH ")) for query in queries)
    assert output.exists()


def test_report_retains_long_horizon_and_reduces_repeats_before_row_limit():
    import json
    from pathlib import Path
    from scripts.report_option_equity_shadow import SQL_CANDIDATES
    config = json.loads((Path(__file__).resolve().parents[3] / "backend/research/inputs/option_equity_shadow_config.json").read_text())
    target = horizon_exit(clock(11), 21)
    assert target - timedelta(days=config["lookback_calendar_days"]) < clock(11)
    assert target - timedelta(days=30) > clock(11)
    assert "FIRST_VALUE(matrix_id)" in SQL_CANDIDATES
    assert SQL_CANDIDATES.index("WHERE matrix_id=first_matrix_id") < SQL_CANDIDATES.index("LIMIT 50001")