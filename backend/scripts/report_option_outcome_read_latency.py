#!/usr/bin/env python3
"""Benchmark legacy and bulk option outcome leg reads without writes."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from time import perf_counter

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from options.outcomes import configured_valuation_policy, measurement_checkpoints  # noqa: E402
from options.repositories.outcomes import OptionOutcomeRepository  # noqa: E402


def identity(legs):
    return tuple((str(row.contract_id), str(row.source_snapshot_id), str(row.source_batch_id),
        row.source_market_time.isoformat(), row.source_observed_time.isoformat(),
        str(row.entry_mark), str(row.exit_mark)) for row in legs)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=100)
    args = parser.parse_args()
    if not 1 <= args.requests <= 500:
        raise ValueError("requests must be between 1 and 500")
    now = datetime.now(timezone.utc)
    policy = configured_valuation_policy()
    repository = OptionOutcomeRepository()
    candidates = repository.list_pending_candidates(
        valuation_policy_sha256=policy.policy_sha256, available_by=now,
        limit=min(args.requests, 200))
    requests = []
    for candidate in candidates:
        completed = set(candidate["completed_measurements"] or ()) | set(candidate.get("unavailable_measurements") or ())
        for measurement_type, checkpoint in measurement_checkpoints(candidate["market_data_time"]).items():
            if measurement_type not in completed and checkpoint <= now:
                requests.append((candidate["candidate_id"], checkpoint, now))
                if len(requests) >= args.requests:
                    break
        if len(requests) >= args.requests:
            break

    started = perf_counter()
    legacy = {(candidate_id, checkpoint): repository.checkpoint_legs(candidate_id,
        checkpoint_time=checkpoint, available_by=available_by,
        valuation_policy_sha256=policy.policy_sha256)
        for candidate_id, checkpoint, available_by in requests}
    legacy_seconds = perf_counter() - started
    started = perf_counter()
    bulk = repository.checkpoint_legs_bulk(requests,
        valuation_policy_sha256=policy.policy_sha256)
    bulk_seconds = perf_counter() - started

    current_candidates = repository.list_current_candidates(
        valuation_policy_sha256=policy.policy_sha256, available_by=now,
        limit=args.requests)
    current_ids = [row["candidate_id"] for row in current_candidates]
    started = perf_counter()
    legacy_current = {candidate_id: repository.current_mark_legs(candidate_id,
        available_by=now, valuation_policy_sha256=policy.policy_sha256)
        for candidate_id in current_ids}
    legacy_current_seconds = perf_counter() - started
    started = perf_counter()
    bulk_current = repository.current_mark_legs_bulk(current_ids,
        available_by=now, valuation_policy_sha256=policy.policy_sha256)
    bulk_current_seconds = perf_counter() - started

    checkpoint_match = {key: identity(value) for key, value in legacy.items()} == {
        key: identity(value) for key, value in bulk.items()}
    current_match = {key: identity(value) for key, value in legacy_current.items()} == {
        key: identity(value) for key, value in bulk_current.items()}
    print(json.dumps({"version": "option_outcome_read_latency_v1", "mode": "READ_ONLY",
        "as_of": now, "checkpoint_requests": len(requests),
        "legacy_checkpoint_seconds": round(legacy_seconds, 4),
        "bulk_checkpoint_seconds": round(bulk_seconds, 4),
        "checkpoint_speedup": round(legacy_seconds / bulk_seconds, 3) if bulk_seconds else None,
        "checkpoint_identity_match": checkpoint_match,
        "current_candidates": len(current_ids),
        "legacy_current_seconds": round(legacy_current_seconds, 4),
        "bulk_current_seconds": round(bulk_current_seconds, 4),
        "current_speedup": round(legacy_current_seconds / bulk_current_seconds, 3) if bulk_current_seconds else None,
        "current_identity_match": current_match,
        "all_identity_matches": checkpoint_match and current_match,
        "source_writes": 0}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
