import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from main import app
from options.api import (
    _performance_checkpoints,
    option_candidate_detail,
    option_candidates,
    option_data_quality,
    option_health,
    option_opportunities,
    option_performance,
    option_signals,
    option_universe,
)
from options.calendar import OptionExchangeCalendar


UTC = timezone.utc


def test_options_health_exposes_delayed_read_only_contract():
    payload = option_health().model_dump(mode="json")
    assert payload["data_tier"] == "15-MINUTE DELAYED RESEARCH DATA"
    assert payload["data"]["read_only"] is True
    assert payload["data"]["schema_ready"] is True
    assert payload["data"]["candidate_workbench"]["available"] is True
    assert payload["data"]["candidate_workbench"]["candidate_count"] >= 1


def test_options_universe_returns_configured_or_persisted_members():
    payload = option_universe().model_dump(mode="json")
    assert len(payload["data"]) == 13
    assert {row["ticker"] for row in payload["data"]} >= {"AAPL", "SPY", "IWM"}


def test_candidate_workbench_exposes_typed_delayed_research_contract():
    payload = option_candidates(limit=5, offset=0).model_dump(mode="json")
    assert payload["reason"] in {None, "NO_STRATEGY_RESULTS"}
    assert payload["data"]["title"] == "Weekly Research Candidates"
    assert payload["data"]["quote_liquidity"] == "NOT_AVAILABLE"
    assert payload["data"]["execution_mode"] == "READ_ONLY_RESEARCH"
    assert payload["data"]["limit"] == 5
    assert set(payload["data"]["status_counts"]) == {
        "selected",
        "suppressed",
        "rejected",
    }
    if payload["data"]["rows"]:
        detail = option_candidate_detail(
            UUID(payload["data"]["rows"][0]["candidate_id"])
        ).model_dump(mode="json")
        assert detail["available"] is True
        assert detail["data"]["execution_mode"] == "READ_ONLY_RESEARCH"


def test_research_findings_expose_distinct_source_contracts():
    payload = option_candidates(
        persona="MOMENTUM",
        status="SELECTED",
        limit=100,
        offset=0,
    ).model_dump(mode="json")
    findings = [
        row for row in payload["data"]["rows"]
        if row["candidate_kind"] == "RESEARCH_ONLY"
    ]

    assert findings
    assert all(row["source_contract_id"] for row in findings)
    assert all(row["source_contract_ticker"] for row in findings)
    assert len({row["source_contract_id"] for row in findings}) == len(findings)


def test_recommendation_api_exposes_persisted_signal_contract():
    payload = option_signals(limit=5, offset=0).model_dump(mode="json")

    assert payload["reason"] in {None, "NO_SIGNAL_EVENTS"}
    assert payload["data"]["limit"] == 5
    assert payload["data"]["offset"] == 0
    assert payload["data"]["execution_mode"] == "READ_ONLY_RESEARCH"
    assert set(payload["data"]["status_counts"]) == {
        "pending", "ready", "blocked", "expired",
    }
    for row in payload["data"]["rows"]:
        assert isinstance(row["legs"], list)
        assert row["expected_leg_count"] == len(row["legs"])


def test_performance_api_exposes_checkpoint_and_management_contract():
    payload = option_performance(
        cohort="OPPORTUNITY_BOARD", days=14, limit=5, offset=0
    ).model_dump(mode="json")

    assert payload["reason"] in {None, "NO_SIGNAL_PERFORMANCE"}
    assert payload["data"]["days"] == 14
    assert payload["data"]["cohort"] == "OPPORTUNITY_BOARD"
    assert payload["data"]["limit"] == 5
    assert payload["data"]["valuation_mode"] == "RESEARCH_DELAYED_PROXY"
    assert payload["data"]["materialization_owner"] == "OPTION_WORKER"
    assert payload["data"]["entry_basis"] == "FIRST_BOARD_OCCURRENCE"
    assert payload["data"]["navigation_revalues"] is False
    assert payload["data"]["current_mark_included"] is False
    assert isinstance(payload["data"]["measurement_summary"], list)
    for row in payload["data"]["rows"]:
        assert row["structure_type"]
        assert row["capital_at_risk"] is not None
        assert all(
            checkpoint["status"] in {"AVAILABLE", "PENDING", "NOT_DUE"}
            for checkpoint in row["checkpoints"]
        )
        assert {
            checkpoint["measurement_type"] for checkpoint in row["checkpoints"]
        } <= {"15MIN", "30MIN", "60MIN", "CLOSE", "NEXT_OPEN"}
        assert row["candidate_rank"] == 1


def test_performance_api_can_include_all_structured_signals():
    board = option_performance(
        cohort="OPPORTUNITY_BOARD", days=14, limit=200, offset=0
    ).model_dump(mode="json")
    all_signals = option_performance(
        cohort="ALL_SIGNALS", days=14, limit=200, offset=0
    ).model_dump(mode="json")

    assert board["data"]["total"] <= all_signals["data"]["total"]
    assert all_signals["data"]["cohort"] == "ALL_SIGNALS"
    assert all_signals["data"]["entry_basis"] == "ORIGINAL_SIGNAL_PACKAGE"


def test_performance_api_can_filter_historical_board_by_strategy():
    payload = option_performance(
        strategy="income_wheel", cohort="OPPORTUNITY_BOARD",
        days=14, limit=200, offset=0,
    ).model_dump(mode="json")

    assert all(
        row["strategy_name"] == "INCOME_WHEEL"
        for row in payload["data"]["rows"]
    )


def test_performance_checkpoints_distinguish_available_pending_and_not_due():
    market_time = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
    outcome = {"measurement_type": "15MIN", "net_pnl": "10.00"}
    row = {"market_data_time": market_time, "outcomes": [outcome]}

    checkpoints = _performance_checkpoints(
        row,
        generated_at=market_time + timedelta(minutes=31),
        exchange=OptionExchangeCalendar(),
    )

    statuses = {
        checkpoint["measurement_type"]: checkpoint["status"]
        for checkpoint in checkpoints
    }
    assert statuses["15MIN"] == "AVAILABLE"
    assert statuses["30MIN"] == "PENDING"
    assert statuses["60MIN"] == "NOT_DUE"
    assert checkpoints[0]["outcome"] == outcome
    assert "outcomes" not in row


def test_opportunity_board_separates_structures_from_research_detectors():
    payload = option_opportunities(per_strategy=1).model_dump(mode="json")

    assert payload["reason"] in {None, "NO_OPPORTUNITY_RESULTS"}
    assert payload["data"]["selection_basis"] == "BACKEND_STRATEGY_RANK"
    assert payload["data"]["execution_mode"] == "READ_ONLY_RESEARCH"
    assert payload["data"]["configured_underlyer_count"] == 13
    assert payload["data"]["covered_underlyer_count"] == len(
        payload["data"]["underlyers"]
    )
    assert all(
        row["candidate_kind"] != "RESEARCH_ONLY"
        for row in payload["data"]["structured"]
    )
    assert all(
        row["candidate_kind"] == "RESEARCH_ONLY"
        for row in payload["data"]["research_highlights"]
    )
    for row in (
        payload["data"]["structured"]
        + payload["data"]["research_highlights"]
    ):
        assert row["strategy_position"] == 1
        assert row["window_state"] in {"ACTIVE", "ELAPSED", "UNBOUNDED"}
        assert isinstance(row["legs"], list)


def test_data_quality_explains_retention_and_unknown_references():
    payload = option_data_quality(limit=5).model_dump(mode="json")

    assert payload["data"]["definitions"]["unknown_references"].startswith(
        "Provider option tickers absent"
    )
    assert len(payload["data"]["retention_criteria"]) == 6
    assert len(payload["data"]["model_eligibility_criteria"]) == 3
    assert payload["data"]["unknown_reference_gate"] == {
        "maximum_count": 20,
        "maximum_fraction": 0.01,
        "rule": "Reference drift fails above the lower of the count and fraction thresholds.",
    }
    for run in payload["data"]["runs"]:
        assert run["excluded_row_count"] == max(
            run["received_row_count"] - run["retained_row_count"],
            0,
        )
        assert 0 <= run["catalog_coverage_fraction"] <= 1
        assert 0 <= run["retention_fraction"] <= 1
        assert all(
            {"code", "label", "count"} <= set(reason)
            for reason in run["exclusion_breakdown"]
        )


def test_options_routes_are_registered_on_main_app():
    paths = {route.path for route in app.routes}
    assert {
        "/api/options/health",
        "/api/options/universe",
        "/api/options/chain/{underlyer}",
        "/api/options/analysis/{underlyer}",
        "/api/options/data-quality",
        "/api/options/opportunities",
        "/api/options/candidates",
        "/api/options/candidates/{candidate_id}",
        "/api/options/scenarios/{candidate_id}",
        "/api/options/signals",
        "/api/options/performance",
    } <= paths
