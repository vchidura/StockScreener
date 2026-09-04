#!/usr/bin/env python3
"""Merge deterministic shards from the generic historical signal runner."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.historical_signal_replay import HistoricalSignalEvent


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--reports", type=Path, nargs="+", required=True)
    result.add_argument("--events", type=Path, nargs="+", required=True)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--events-output", type=Path, required=True)
    return result


def merge(
    report_paths: Sequence[Path],
    event_paths: Sequence[Path],
    *,
    output: Path,
    events_output: Path,
) -> dict[str, Any]:
    if len(report_paths) != len(event_paths):
        raise ValueError("report and event shard counts must match")
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in report_paths]
    if not reports:
        raise ValueError("at least one shard is required")
    shard_count = int(reports[0]["shard_count"])
    shard_indices = {int(report["shard_index"]) for report in reports}
    if len(reports) != shard_count or shard_indices != set(range(shard_count)):
        raise ValueError("reports must contain every declared shard exactly once")
    common_fields = (
        "source_name", "source_version", "universe_policy_version",
        "first_session", "last_session", "sessions",
    )
    for field in common_fields:
        values = {json.dumps(report[field], sort_keys=True) for report in reports}
        if len(values) != 1:
            raise ValueError(f"shard reports disagree on {field}")

    events_by_id = {}
    shard_event_counts = 0
    for report, path in zip(reports, event_paths):
        rows = [
            HistoricalSignalEvent.from_dict(json.loads(line))
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        expected = int(report["event_count"])
        if len(rows) != expected:
            raise ValueError(f"event shard count mismatch: {path}")
        shard_event_counts += expected
        for event in rows:
            if event.event_id in events_by_id:
                raise ValueError(f"duplicate event across shards: {event.event_id}")
            events_by_id[event.event_id] = event
    if len(events_by_id) != shard_event_counts:
        raise ValueError("merged event count does not equal shard totals")
    events = sorted(
        events_by_id.values(),
        key=lambda row: (row.signal_time, row.ticker, row.event_id.hex),
    )
    lane_counts = {}
    for event in events:
        lane_counts[event.source_name] = lane_counts.get(event.source_name, 0) + 1
    exclusion_keys = sorted({
        key for report in reports for key in report["exclusion_counts"]
    })
    report = {
        field: reports[0][field] for field in common_fields
    }
    report.update({
        "adapter_summary": {
            "direction_counts": {
                "LONG": sum(event.direction == 1 for event in events),
                "SHORT": sum(event.direction == -1 for event in events),
            },
            "lane_counts": dict(sorted(lane_counts.items())),
        },
        "candidate_count": sum(int(row["candidate_count"]) for row in reports),
        "event_count": len(events),
        "events_output": str(events_output),
        "exclusion_counts": {
            key: sum(int(row["exclusion_counts"].get(key, 0)) for row in reports)
            for key in exclusion_keys
        },
        "merged_shards": shard_count,
        "union_tickers": sum(int(row["union_tickers"]) for row in reports),
    })
    events_output.parent.mkdir(parents=True, exist_ok=True)
    events_output.write_text(
        "".join(
            json.dumps(event.as_dict(), sort_keys=True) + "\n"
            for event in events
        ),
        encoding="utf-8",
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return report


def main() -> int:
    args = parser().parse_args()
    merge(
        args.reports,
        args.events,
        output=args.output,
        events_output=args.events_output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
