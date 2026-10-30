from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.inventory_legacy_equity_b1_readiness import classify_lane_readiness


def baseline_gates():
    return {
        "dated_universe_membership": True,
        "adjusted_daily_features": True,
        "raw_30m_history": True,
        "full_hour_derivation": True,
        "benchmark_context": True,
        "action_coverage": True,
        "warmup_continuity": True,
        "prior_session_context": True,
        "multi_session_paths": True,
        "pit_sector_mapping": True,
        "sector_benchmark_daily_hourly": True,
        "earnings_coverage": True,
        "raw_5m_history": True,
        "prior_daily_atr": True,
        "opening_state": True,
        "volume_baselines": True,
        "aggregate_vwap": True,
        "survivorship_safe_universe": True,
        "split_dividend_consistency": True,
        "pit_fundamentals": True,
        "filing_availability": True,
        "sector_ranks": True,
        "long_mature_outcomes": True,
    }


def test_lane_classifier_marks_all_lanes_input_ready_when_all_gates_pass():
    result = classify_lane_readiness(baseline_gates())
    assert {lane["status"] for lane in result.values()} == {"INPUT_READY"}
    assert all(not lane["missing_gates"] for lane in result.values())


def test_lane_classifier_blocks_only_five_minute_lane_when_raw_5m_is_missing():
    gates = baseline_gates()
    gates["raw_5m_history"] = False
    gates["aggregate_vwap"] = False
    result = classify_lane_readiness(gates)
    assert result["d1_d2"]["status"] == "BLOCKED_INPUT_READINESS"
    assert "RAW_5M_HISTORY_GAP" in result["d1_d2"]["blockers"]
    assert "AGGREGATE_VWAP_GAP" in result["d1_d2"]["blockers"]
    assert result["controls_r2_b3_f2"]["status"] == "INPUT_READY"
    assert result["sw3_r_b_f"]["status"] == "INPUT_READY"
    assert result["sw1"]["status"] == "INPUT_READY"
    assert result["p1_p2"]["status"] == "INPUT_READY"


def test_lane_classifier_requires_sw1_sector_and_earnings_coverage():
    gates = baseline_gates()
    gates["pit_sector_mapping"] = False
    gates["earnings_coverage"] = False
    result = classify_lane_readiness(gates)
    assert result["sw1"]["status"] == "BLOCKED_INPUT_READINESS"
    assert result["sw1"]["missing_gates"] == ["pit_sector_mapping", "earnings_coverage"]
    assert result["sw1"]["blockers"] == ["PIT_SECTOR_MAPPING_GAP", "EARNINGS_EVENT_COVERAGE_GAP"]
