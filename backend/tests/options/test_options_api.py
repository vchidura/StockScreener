import sys
from pathlib import Path
from uuid import UUID

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from main import app
from options.api import (
    option_candidate_detail,
    option_candidates,
    option_health,
    option_universe,
)


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


def test_options_routes_are_registered_on_main_app():
    paths = {route.path for route in app.routes}
    assert {
        "/api/options/health",
        "/api/options/universe",
        "/api/options/chain/{underlyer}",
        "/api/options/analysis/{underlyer}",
        "/api/options/data-quality",
        "/api/options/candidates",
        "/api/options/candidates/{candidate_id}",
        "/api/options/scenarios/{candidate_id}",
        "/api/options/signals",
    } <= paths
