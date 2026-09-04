import sys
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import pytest

from research.historical_signal_replay import (
    GapFormationAdapter,
    GapFormationV2Adapter,
    HistoricalSignalEvent,
)
from scripts.merge_historical_signal_research import merge
from scripts.run_historical_signal_research import load_adapter, parser


def test_builtin_and_plugin_adapter_loading() -> None:
    builtin = load_adapter(parser().parse_args(["--signal", "gap-formation-v1"]))
    plugin = load_adapter(parser().parse_args([
        "--adapter", "research.historical_signal_replay:GAP_FORMATION_ADAPTER",
    ]))

    assert isinstance(builtin, GapFormationAdapter)
    assert isinstance(plugin, GapFormationAdapter)

    revised = load_adapter(parser().parse_args(["--signal", "gap-formation-v2"]))
    assert isinstance(revised, GapFormationV2Adapter)


def test_plugin_contract_rejects_non_adapter_objects() -> None:
    with pytest.raises(TypeError, match="missing source_name"):
        load_adapter(parser().parse_args([
            "--adapter", "research.historical_signal_replay:BUILTIN_ADAPTERS",
        ]))


def test_controlled_replay_accepts_ticker_subset() -> None:
    args = parser().parse_args([
        "--signal", "composite-scanners-1d-v1",
        "--tickers", "aapl,MSFT",
    ])

    assert args.tickers == "aapl,MSFT"


def test_shard_arguments_are_explicit_and_deterministic() -> None:
    args = parser().parse_args(["--shard-count", "6", "--shard-index", "3"])

    assert args.shard_count == 6
    assert args.shard_index == 3
    tickers = ["A", "B", "C", "D", "E", "F", "G", "H"]
    shards = [set(tickers[index::3]) for index in range(3)]
    assert set.union(*shards) == set(tickers)
    assert all(shards[left].isdisjoint(shards[right])
               for left in range(3) for right in range(left + 1, 3))


def test_shard_merge_requires_complete_set_and_sorts_events(tmp_path) -> None:
    report_paths = []
    event_paths = []
    events = []
    for index, ticker in enumerate(("MSFT", "AAPL")):
        signal_time = datetime(2026, 8, 28, 20 - index, tzinfo=timezone.utc)
        event = HistoricalSignalEvent(
            event_id=uuid4(), source_name="PATTERN_FALLING_WEDGE_BOUNDARY_BREAK",
            source_version="forming_patterns_boundary_break_v1", ticker=ticker,
            signal_date=signal_time.date(), signal_time=signal_time, direction=1,
            setup_anchor=f"{ticker}:pattern", universe_run_id=uuid4(),
            universe_policy_version="liquid_us_common_stocks_v2",
            source_bar_revision_ids=(uuid4(),),
            payload={"qualification_eligible": True},
        )
        events.append(event)
        report = {
            "source_name": "PATTERN_BOUNDARY_BREAK",
            "source_version": "forming_patterns_boundary_break_v1",
            "universe_policy_version": "liquid_us_common_stocks_v2",
            "first_session": "2026-08-25", "last_session": "2026-08-31",
            "sessions": 5, "shard_count": 2, "shard_index": index,
            "candidate_count": 1, "event_count": 1, "union_tickers": 1,
            "exclusion_counts": {"NOT_IN_UNIVERSE": index},
        }
        report_path = tmp_path / f"report-{index}.json"
        event_path = tmp_path / f"events-{index}.jsonl"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        event_path.write_text(
            json.dumps(event.as_dict()) + "\n", encoding="utf-8"
        )
        report_paths.append(report_path)
        event_paths.append(event_path)

    output = tmp_path / "merged.json"
    events_output = tmp_path / "merged.jsonl"
    result = merge(
        report_paths, event_paths, output=output, events_output=events_output
    )

    assert result["merged_shards"] == 2
    assert result["event_count"] == 2
    assert result["candidate_count"] == 2
    assert result["union_tickers"] == 2
    merged_events = [
        HistoricalSignalEvent.from_dict(json.loads(line))
        for line in events_output.read_text(encoding="utf-8").splitlines()
    ]
    assert [row.event_id for row in merged_events] == [
        row.event_id for row in sorted(
            events, key=lambda row: (row.signal_time, row.ticker, row.event_id.hex)
        )
    ]

    with pytest.raises(ValueError, match="every declared shard"):
        merge(
            report_paths[:1], event_paths[:1],
            output=output, events_output=events_output,
        )