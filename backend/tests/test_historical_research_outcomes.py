import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from equity.historical_research import (
    GAP_PRIMARY_SOURCES,
    composite_scanner_outcome_policies,
    historical_event_evidence,
    primary_gap_outcome_policies,
)
from equity.historical_universe import HistoricalUniversePolicy
from equity.polygon import normalize_security_reference
from research.historical_signal_replay import HistoricalSignalEvent


UTC = timezone.utc


def event(
    *, source_name="GAP_BREAKAWAY_HOLD", direction=1,
    hypothesis="FORMATION_HOLD", last_close=105.0,
):
    signal_time = datetime(2026, 4, 9, 20, tzinfo=UTC)
    return HistoricalSignalEvent(
        event_id=uuid4(), source_name=source_name,
        source_version="gap_formation_v1", ticker="AAPL",
        signal_date=signal_time.date(), signal_time=signal_time,
        direction=direction, setup_anchor="AAPL:2026-04-09:UP",
        universe_run_id=uuid4(),
        universe_policy_version=HistoricalUniversePolicy().policy_version,
        source_bar_revision_ids=(uuid4(),),
        payload={
            "current_open": 104.0, "fill_target": 100.0,
            "hypothesis": hypothesis, "last_close": last_close,
            "qualification_eligible": True,
        },
    )


def security():
    return normalize_security_reference(
        {"active": True, "ticker": "AAPL", "type": "CS", "name": "Apple"},
        observed_at=datetime(2026, 8, 31, tzinfo=UTC),
        source_as_of_date=datetime(2026, 4, 9, tzinfo=UTC).date(),
    )


def test_historical_event_becomes_replay_only_durable_evidence() -> None:
    result = historical_event_evidence(event(), security())
    payload = json.loads(result.payload_json)

    assert result.analysis_run_id is None
    assert result.source_name == "GAP_BREAKAWAY_HOLD"
    assert result.observed_at == result.market_time
    assert result.source_revision_ids == (result.latest_bar_revision_id,)
    assert result.quality_codes == (
        "HISTORICAL_RECONSTRUCTED", "REPLAY_ONLY", "UNQUALIFIED_DIRECTION",
    )
    assert payload["stop_price"] == 100.0
    assert payload["target_price"] == 115.0
    assert payload["universe_run_id"]


def test_fade_risk_uses_formation_open_as_invalidation() -> None:
    result = historical_event_evidence(
        event(
            source_name="GAP_FADE_REVERSAL", direction=-1,
            hypothesis="FADE_REVERSAL", last_close=100.0,
        ),
        security(),
    )
    payload = json.loads(result.payload_json)

    assert payload["stop_price"] == 104.0
    assert payload["target_price"] == 92.0


def test_adapter_defined_bracket_is_preserved() -> None:
    source = event(
        source_name="GAP_ENTRY_FILL", direction=-1,
        hypothesis="GAP_ENTRY_FILL", last_close=102.5,
    )
    source = __import__("dataclasses").replace(
        source,
        payload={
            **source.payload,
            "stop_price": 103.5,
            "target_price": 101.0,
            "risk_basis": "GAP_NEAR_EDGE_TO_FAR_EDGE",
        },
    )

    payload = json.loads(historical_event_evidence(source, security()).payload_json)

    assert payload["stop_price"] == 103.5
    assert payload["target_price"] == 101.0
    assert payload["risk_basis"] == "GAP_NEAR_EDGE_TO_FAR_EDGE"


def test_primary_policies_form_eighteen_direction_horizon_cells() -> None:
    policies = primary_gap_outcome_policies(
        source_version="gap_formation_v1",
        effective_from=datetime(2026, 4, 9, tzinfo=UTC),
    )

    assert tuple(policy.source_name for policy in policies) == GAP_PRIMARY_SOURCES
    assert all(
        json.loads(policy.horizons_json) == {"5d": 5, "10d": 10, "21d": 21}
        for policy in policies
    )
    assert all(policy.policy_version == "directional_outcome_v3_sector" for policy in policies)
    assert all(json.loads(policy.benchmark_policy_json)["sector"] is True for policy in policies)
    assert all(
        json.loads(policy.benchmark_policy_json)["primary"] == "SECTOR"
        for policy in policies
    )
    assert len(policies) * 2 * 3 == 18


def test_primary_policies_accept_plugin_source_family() -> None:
    policies = primary_gap_outcome_policies(
        source_version="gap_entry_fill_v1",
        effective_from=datetime(2026, 4, 9, tzinfo=UTC),
        horizon_sessions=(1, 3, 5),
        source_names=("GAP_ENTRY_FILL",),
    )

    assert len(policies) == 1
    assert policies[0].source_name == "GAP_ENTRY_FILL"
    assert json.loads(policies[0].horizons_json) == {"1d": 1, "3d": 3, "5d": 5}


def test_composite_policies_cover_both_return_modes_and_supported_intervals() -> None:
    effective_from = datetime(2026, 8, 30, tzinfo=UTC)

    intraday = composite_scanner_outcome_policies(
        interval="30m", effective_from=effective_from
    )
    daily = composite_scanner_outcome_policies(
        interval="1d", effective_from=effective_from
    )

    assert len(intraday) == 12
    assert len(daily) == 14
    assert {
        policy.policy_key.removesuffix(":SECTOR_PRIMARY").rsplit(":", 1)[-1]
        for policy in daily
    } == {"SIGNED", "RECOMMENDATION_PLAN"}
    assert all(
        policy.policy_key.endswith(":SECTOR_PRIMARY") for policy in daily
    )
    assert all(
        json.loads(policy.horizons_json) == {"5d": 5, "10d": 10, "21d": 21}
        for policy in daily
    )
    assert not any(
        policy.source_name == "sma200_reclaim_rejection" for policy in intraday
    )


def test_composite_policies_reject_unsupported_interval() -> None:
    with __import__("pytest").raises(ValueError, match="unsupported"):
        composite_scanner_outcome_policies(
            interval="15m",
            effective_from=datetime(2026, 8, 30, tzinfo=UTC),
        )