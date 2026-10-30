from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.execute_legacy_equity_backtest_plan import _phase_verdict


def test_phase_verdict_is_qualified_when_all_lanes_ready():
    readiness = {
        "controls_r2_b3_f2": {"status": "INPUT_READY"},
        "sw3_r_b_f": {"status": "INPUT_READY"},
    }
    assert _phase_verdict(readiness, ("controls_r2_b3_f2", "sw3_r_b_f")) == "QUALIFIED"


def test_phase_verdict_is_inconclusive_for_mixed_status():
    readiness = {
        "controls_r2_b3_f2": {"status": "INPUT_READY"},
        "sw3_r_b_f": {"status": "BLOCKED_INPUT_READINESS"},
    }
    assert _phase_verdict(readiness, ("controls_r2_b3_f2", "sw3_r_b_f")) == "INCONCLUSIVE"


def test_phase_verdict_is_blocked_when_no_lane_ready():
    readiness = {
        "controls_r2_b3_f2": {"status": "BLOCKED_INPUT_READINESS"},
        "sw3_r_b_f": {"status": "BLOCKED_INPUT_READINESS"},
    }
    assert _phase_verdict(readiness, ("controls_r2_b3_f2", "sw3_r_b_f")) == "BLOCKED_INPUT_READINESS"
