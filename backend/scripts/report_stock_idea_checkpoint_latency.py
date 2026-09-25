#!/usr/bin/env python3
"""Summarize retained stock-idea checkpoint size and timing without writes."""
from __future__ import annotations

from contextlib import closing
import argparse
import json
from pathlib import Path
import sqlite3
import sys
from time import perf_counter
import zlib


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from research.stock_idea_forward import ForwardStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-encoding", action="store_true")
    args = parser.parse_args()
    path = BACKEND_DIR / "backups/equity-shadow/stock-ideas-forward-v2/forward.sqlite"
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        row = connection.execute("SELECT payload FROM forward_checkpoint WHERE singleton=1").fetchone()
        inputs = connection.execute("SELECT payload,length(payload) FROM forward_input_checkpoint WHERE singleton=1").fetchone()
        publications = connection.execute("SELECT COUNT(*),MAX(window_key) FROM forward_publications").fetchone()
    if row is None or inputs is None:
        raise ValueError("stock idea checkpoint is unavailable")
    decoded_inputs = ForwardStore.decode(inputs[0])
    state = ForwardStore.decode(row[0]) | decoded_inputs
    bars = state.get("bars", {})
    detectors = state.get("detectors", {})
    result = dict(version="stock_idea_checkpoint_latency_v1", mode="READ_ONLY",
        preparation_seconds=state.get("preparation_seconds"), next_boundary=state.get("next_boundary"),
        last_source_read=state.get("last_source_read"), members=len(state.get("members", ())),
        bar_series=len(bars), retained_bars=sum(len(rows) for rows in bars.values()),
        detector_states=len(detectors), pending_candidates=len(state.get("pending_candidates", {})),
        prepared_packets=len(state.get("prepared_packets", ())), positions=len(state.get("positions", {})),
        publications=publications[0], latest_publication=publications[1], input_checkpoint_bytes=inputs[1], source_writes=0)
    if args.benchmark_encoding:
        started = perf_counter()
        raw = json.dumps(decoded_inputs, sort_keys=True, default=str, allow_nan=False).encode()
        result["json_encode_seconds"] = round(perf_counter() - started, 4)
        result["input_json_bytes"] = len(raw)
        for level in (1, 6):
            started = perf_counter()
            compressed = zlib.compress(raw, level)
            result[f"zlib_{level}_seconds"] = round(perf_counter() - started, 4)
            result[f"zlib_{level}_bytes"] = len(compressed)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
