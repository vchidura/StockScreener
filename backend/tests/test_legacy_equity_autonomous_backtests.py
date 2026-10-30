from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.run_legacy_equity_autonomous_backtests import (
    _best_mode_window,
    _d1_d2_input_readiness,
    _study_status,
)


def test_best_mode_window_prefers_longer_session_coverage():
    inventory = {
        "mode_coverage_in_universe_window": [
            {
                "interval": "30m",
                "availability_mode": "HISTORICAL_RECONSTRUCTED",
                "sessions_in_universe_window": 10,
                "first_session_in_universe_window": "2026-08-01",
                "last_session_in_universe_window": "2026-08-15",
                "maximum_securities_per_session": 100,
            },
            {
                "interval": "30m",
                "availability_mode": "LIVE_OBSERVED",
                "sessions_in_universe_window": 20,
                "first_session_in_universe_window": "2026-08-01",
                "last_session_in_universe_window": "2026-08-31",
                "maximum_securities_per_session": 80,
            },
        ]
    }
    assert _best_mode_window(inventory, "30m") == ("LIVE_OBSERVED", "2026-08-01", "2026-08-31")


def test_study_status_reports_inconclusive_for_insufficient_blocks():
    evaluation = {"cells": [{"status": "INSUFFICIENT_INDEPENDENT_BLOCKS"}]}
    assert _study_status(evaluation) == "INCONCLUSIVE_INSUFFICIENT_BLOCKS"


def test_study_status_reports_qualified_when_all_cells_pass():
    evaluation = {"cells": [{"status": "PASS_RESEARCH_ONLY"}, {"status": "PASS_RESEARCH_ONLY"}]}
    assert _study_status(evaluation) == "QUALIFIED_RESEARCH_ONLY"


def test_d1_d2_input_readiness_ready_with_required_columns_and_no_nulls():
    inventory = {
        "d1_d2_required_inputs": {
            "columns": {"has_vwap": True, "has_transaction_count": True},
            "mode_null_coverage": [
                {
                    "availability_mode": "LIVE_OBSERVED",
                    "vwap_null_rows": 0,
                    "transaction_count_null_rows": 0,
                }
            ],
        }
    }
    readiness = _d1_d2_input_readiness(inventory, ("LIVE_OBSERVED", "2026-01-01", "2026-02-01"))
    assert readiness["status"] == "READY"
