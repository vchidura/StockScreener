"""Offline, response-bound action coverage screen for the new detector study."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, timedelta
import gzip
import hashlib
import json
from pathlib import Path
import sys

import exchange_calendars
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from equity.historical_actions import validate_action_coverage
from research.equity_models.catalog import CATALOG, research_policy


def covers(windows, start, end):
    """Union inclusive dates; adjacent windows close a gap, actual gaps do not."""
    cursor = start
    for left, right in sorted(windows):
        if right < cursor:
            continue
        if left > cursor:
            return False
        cursor = max(cursor, right + timedelta(days=1))
        if cursor > end:
            return True
    return cursor > end


def screen(payload, start, end):
    calendar = exchange_calendars.get_calendar("XNYS")
    members = defaultdict(list)
    for row in payload["universe_members"]:
        members[row["universe_run_id"]].append(row)
    actions = {r["corporate_action_id"]: dict(r, effective_date=date.fromisoformat(r["effective_date"]))
               for r in payload["actions"]}
    links = defaultdict(list)
    for row in payload["action_coverage_members"]:
        links[row["coverage_id"]].append(actions[row["corporate_action_id"]])
    coverage = defaultdict(list)
    rejected = Counter()
    for original in payload["action_coverage"]:
        row = dict(original)
        row["window_start"], row["window_end"] = (date.fromisoformat(row[k]) for k in ("window_start", "window_end"))
        try:
            validate_action_coverage(row, links[row["coverage_id"]])
        except ValueError as exc:
            rejected[str(exc)] += 1
            continue
        row["received"] = max(pd.Timestamp(row[k]) for k in ("first_observed_at", "created_at"))
        coverage[(row["security_id"], row["action_type"])].append(row)
    rows = []
    for session in calendar.sessions_in_range(start, end):
        closing = calendar.session_close(session)
        prior_start = calendar.session_offset(session, -253).date()
        eligible = [r for r in payload["universe_runs"]
                    if max(pd.Timestamp(r[k]) for k in ("effective_from", "observed_at", "created_at")) <= closing]
        if not eligible:
            continue
        run = max(eligible, key=lambda r: (pd.Timestamp(r["effective_from"]), pd.Timestamp(r["observed_at"])))
        for member in members[run["universe_run_id"]]:
            security = member["security_id"]
            spans = [r for r in coverage[(security, "SPLIT")] if r["received"] <= closing]
            full = covers([(r["window_start"], r["window_end"]) for r in spans], prior_start, session.date())
            rows.append(dict(session=str(session.date()), security_id=security, ticker=member["ticker"],
                universe_run_id=run["universe_run_id"], warmup_start=str(prior_start),
                split_coverage_complete=full, available_response_count=len(spans),
                earliest_certified_start=str(min((r["window_start"] for r in spans), default="")),
                reason="PASS_COVERAGE_SCREEN" if full else "RAW_DAILY_WARMUP_SPLIT_COVERAGE_GAP"))
    return rows, dict(rejected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--start", default="2026-09-11")
    parser.add_argument("--end", default="2026-10-23")
    args = parser.parse_args()
    path = args.directory / "source-readiness-evidence.json.gz"
    compressed = path.read_bytes()
    manifest = json.loads((args.directory / "source-evidence-manifest.json").read_text())
    raw = gzip.decompress(compressed)
    if hashlib.sha256(compressed).hexdigest() != manifest["compressed_sha256"] or hashlib.sha256(raw).hexdigest() != manifest["raw_sha256"]:
        raise ValueError("source artifact checksum mismatch")
    payload = json.loads(raw)
    rows, rejected = screen(payload, args.start, args.end)
    if not rows:
        raise ValueError("no dated member sessions available for coverage screen")
    ready = sum(r["split_coverage_complete"] for r in rows)
    blockers = {}
    for model, policy in CATALOG.items():
        reasons = []
        if policy.interval != "event" and not ready:
            reasons.append("RAW_DAILY_WARMUP_SPLIT_COVERAGE_GAP")
        if policy.style == "POSITION":
            reasons.append("NO_POINT_IN_TIME_FUNDAMENTAL_REPORTS")
        if model == "SW2":
            reasons.append("NO_CANONICAL_ORIGINAL_EPS_CONSENSUS_HISTORY")
        if model in {"D1", "D2", "C1", "C2"}:
            reasons.append("NO_CANONICAL_STOCK_QUOTE_HISTORY")
        if model == "C2":
            reasons.append("NO_RETAINED_QUEUE_FILL_AND_LATENCY_EVIDENCE")
        blockers[model] = dict(status="BLOCKED_INPUT_READINESS" if reasons else "FURTHER_READINESS_CHECKS_REQUIRED",
                              reasons=reasons, policy_hash=research_policy(model, "none").policy_hash)
    summary = dict(schema="equity_detection_historical_readiness_v1", macro_policy="none",
                   start=args.start, end=args.end, source_transaction=payload["transaction"],
                   source_sha256=manifest["raw_sha256"], security_sessions=len(rows),
                   securities=len({r["security_id"] for r in rows}), sessions=len({r["session"] for r in rows}),
                   complete_warmup_coverage_security_sessions=ready,
                   rejected_action_coverage=rejected, models=blockers,
                   screening_clock="SESSION_CLOSE_OPTIMISTIC_UPPER_BOUND",
                   execution_permission=False, publication_permission=False,
                   performance_status="NOT_RUN_INPUT_READINESS_BLOCKED" if not ready else "REQUIRES_FURTHER_EVALUATION")
    from prepare_equity_detection_history import write_once
    write_once(args.directory / "historical-readiness-summary.json", summary)
    csv_path = args.directory / "security-session-readiness.csv"
    if csv_path.exists():
        raise ValueError("CSV already exists; choose a fresh output directory")
    with csv_path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({k: v for k, v in summary.items() if k not in {"models", "rejected_action_coverage"}}, indent=2))


if __name__ == "__main__":
    main()
