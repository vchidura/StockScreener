import json
import sys
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.run_historical_signal_outcomes import (
    evaluate,
    load_events,
    parser,
    publication_metadata,
)
from equity.historical_research import historical_event_evidence_id
from research.historical_signal_replay import HistoricalSignalEvent
from datetime import datetime, timezone
from uuid import uuid4


def test_cli_requires_explicit_qualification_effective_time() -> None:
    args = parser().parse_args(["--all", "--events", "events.jsonl"])

    assert args.qualification_effective_from is None
    assert args.horizon_sessions == (5, 10, 21)
    assert args.source_version == "gap_formation_v1"


def test_event_loader_reports_invalid_line(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(json.dumps({"event_id": "bad"}) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="line 1"):
        load_events(path)


def test_publication_metadata_hashes_exact_event_file_and_universe(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text('{"event":"fixture"}\n', encoding="utf-8")
    signal_time = datetime(2026, 4, 9, 20, tzinfo=timezone.utc)
    universe_run_id = uuid4()
    event = HistoricalSignalEvent(
        event_id=uuid4(), source_name="MA_CROSSOVER_9_21",
        source_version="ma_crossover_9_21_v1", ticker="AAPL",
        signal_date=signal_time.date(), signal_time=signal_time, direction=1,
        setup_anchor="AAPL:cross", universe_run_id=universe_run_id,
        universe_policy_version="liquid_us_common_stocks_v2",
        source_bar_revision_ids=(uuid4(),),
        payload={"qualification_eligible": True},
    )

    result = publication_metadata(
        (event,), events_path=path,
        available_by=datetime(2026, 9, 1, tzinfo=timezone.utc),
        horizon_sessions=(5, 10, 21), round_trip_cost_bps=4.0,
    )

    assert result["event_count"] == 1
    assert result["eligible_event_count"] == 1
    assert result["source_names"] == ["MA_CROSSOVER_9_21"]
    assert result["universe_policy_versions"] == ["liquid_us_common_stocks_v2"]
    assert len(result["event_file_sha256"]) == 64
    assert len(result["universe_run_ids_sha256"]) == 64


def test_gap_evaluation_builds_directional_and_plan_policies(monkeypatch) -> None:
    signal_time = datetime(2026, 4, 9, 20, tzinfo=timezone.utc)
    event = HistoricalSignalEvent(
        event_id=uuid4(), source_name="GAP_ENTRY_FILL",
        source_version="gap_entry_fill_v2", ticker="AAPL",
        signal_date=signal_time.date(), signal_time=signal_time, direction=-1,
        setup_anchor="AAPL:gap", universe_run_id=uuid4(),
        universe_policy_version="liquid_us_common_stocks_v2",
        source_bar_revision_ids=(uuid4(),),
        payload={"qualification_eligible": True},
    )
    calls = []

    class Service:
        def __init__(self, client, **kwargs):
            pass

        def evaluate_directional_outcomes(self, policy, horizon_key, **kwargs):
            calls.append((policy, horizon_key, kwargs))
            return __import__("types").SimpleNamespace(
                policy_key=policy.policy_key, policy_version=policy.policy_version,
                horizon_key=horizon_key, due=0, persisted=0, pending=0,
            )

    monkeypatch.setattr(
        "scripts.run_historical_signal_outcomes.EquityMaterializationService",
        Service,
    )
    monkeypatch.setattr(
        "scripts.run_historical_signal_outcomes.maturity_cutoff",
        lambda count: datetime(2026, 8, 30, tzinfo=timezone.utc),
    )

    evaluate(
        (event,), horizon_sessions=(1, 3), round_trip_cost_bps=4.0,
        available_by=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert len(calls) == 4
    assert {
        policy.policy_key.removesuffix(":SECTOR_PRIMARY").rsplit(":", 1)[-1]
        for policy, _, _ in calls
    } == {"SIGNED", "RECOMMENDATION_PLAN"}
    assert all(
        json.loads(policy.benchmark_policy_json)["primary"] == "SECTOR"
        for policy, _, _ in calls
    )
    assert all(
        kwargs["subject_evidence_ids"] == (historical_event_evidence_id(event),)
        for _, _, kwargs in calls
    )


def test_composite_evaluation_supports_per_source_versions(monkeypatch) -> None:
    signal_time = datetime(2026, 4, 9, 20, tzinfo=timezone.utc)
    events = tuple(
        HistoricalSignalEvent(
            event_id=uuid4(), source_name=source_name,
            source_version=source_version, ticker="AAPL",
            signal_date=signal_time.date(), signal_time=signal_time,
            direction=1, setup_anchor=f"AAPL:{source_name}",
            universe_run_id=uuid4(),
            universe_policy_version="liquid_us_common_stocks_v2",
            source_bar_revision_ids=(uuid4(),),
            payload={
                "qualification_eligible": True,
                "outcome_modes": [
                    "DIRECTIONAL_HORIZON", "RECOMMENDATION_PLAN",
                ],
            },
        )
        for source_name, source_version in (
            ("breakout_expansion", "1.0"),
            ("level_retest_rejection", "1.2"),
        )
    )
    calls = []

    class Service:
        def __init__(self, client, **kwargs):
            pass

        def evaluate_directional_outcomes(self, policy, horizon_key, **kwargs):
            calls.append((policy, horizon_key, kwargs))
            return __import__("types").SimpleNamespace(
                policy_key=policy.policy_key,
                policy_version=policy.policy_version,
                horizon_key=horizon_key, due=0, persisted=0, pending=0,
            )

    monkeypatch.setattr(
        "scripts.run_historical_signal_outcomes.EquityMaterializationService",
        Service,
    )
    monkeypatch.setattr(
        "scripts.run_historical_signal_outcomes.maturity_cutoff",
        lambda count: datetime(2026, 8, 30, tzinfo=timezone.utc),
    )

    evaluate(
        events, horizon_sessions=(5, 10, 21), round_trip_cost_bps=4.0,
        available_by=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert len(calls) == 12
    assert {
        (policy.source_name, policy.source_version)
        for policy, _, _ in calls
    } == {
        ("breakout_expansion", "1.0"),
        ("level_retest_rejection", "1.2"),
    }
    assert {
        policy.policy_key.removesuffix(":SECTOR_PRIMARY").rsplit(":", 1)[-1]
        for policy, _, _ in calls
    } == {"SIGNED", "RECOMMENDATION_PLAN"}


def test_pattern_break_evaluation_is_directional_only(monkeypatch) -> None:
    signal_time = datetime(2026, 8, 28, 20, tzinfo=timezone.utc)
    events = tuple(
        HistoricalSignalEvent(
            event_id=uuid4(), source_name=source_name,
            source_version="forming_patterns_boundary_break_v1", ticker="AAPL",
            signal_date=signal_time.date(), signal_time=signal_time,
            direction=direction, setup_anchor=f"AAPL:{source_name}",
            universe_run_id=uuid4(),
            universe_policy_version="liquid_us_common_stocks_v2",
            source_bar_revision_ids=(uuid4(),),
            payload={
                "qualification_eligible": True,
                "outcome_modes": ["DIRECTIONAL_HORIZON"],
            },
        )
        for source_name, direction in (
            ("PATTERN_FALLING_WEDGE_BOUNDARY_BREAK", 1),
            ("PATTERN_RISING_WEDGE_BOUNDARY_BREAK", -1),
        )
    )
    calls = []

    class Service:
        def __init__(self, client, **kwargs):
            pass

        def evaluate_directional_outcomes(self, policy, horizon_key, **kwargs):
            calls.append((policy, horizon_key))
            return __import__("types").SimpleNamespace(
                policy_key=policy.policy_key,
                policy_version=policy.policy_version,
                horizon_key=horizon_key, due=0, persisted=0, pending=0,
            )

    monkeypatch.setattr(
        "scripts.run_historical_signal_outcomes.EquityMaterializationService",
        Service,
    )
    monkeypatch.setattr(
        "scripts.run_historical_signal_outcomes.maturity_cutoff",
        lambda count: datetime(2026, 8, 30, tzinfo=timezone.utc),
    )

    evaluate(
        events, horizon_sessions=(5, 10, 21), round_trip_cost_bps=4.0,
        available_by=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )

    assert len(calls) == 6
    assert all(":SIGNED:SECTOR_PRIMARY" in policy.policy_key for policy, _ in calls)
    assert not any("RECOMMENDATION_PLAN" in policy.policy_key for policy, _ in calls)