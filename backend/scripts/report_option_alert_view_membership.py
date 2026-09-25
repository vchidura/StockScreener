#!/usr/bin/env python3
"""Compare package-only Latest Run and Day History membership without writes."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from options.analytics.behavior_review import build_detector_alert_review  # noqa: E402
from options.repositories.alert_evaluations import OptionAlertEvaluationRepository  # noqa: E402
from options.repositories.alert_review_sources import OptionAlertReviewSourceRepository  # noqa: E402


def compact(review):
    def run_summary(run):
        if run is None:
            return None
        return {
            "run_id": run["run_id"],
            "scheduled_cycle": run["scheduled_cycle"],
            "published_at": run["published_at"],
            "covered_underlyings": run["covered_underlyings"],
            "expected_underlyings": run["expected_underlyings"],
            "selection_counts": run["selection_counts"],
        }

    return {
        "dataset_id": review["dataset_id"],
        "scope": review["scope"],
        "status": review["status"],
        "latest_run_id": review["latest_run_id"],
        "displayed_run_id": review["run"]["run_id"] if review["run"] else None,
        "latest_run": run_summary(review["latest_run"]),
        "included_runs": [run_summary(run) for run in review["runs"]],
        "total": review["total"],
        "new_alerts": review["new_alerts"],
        "repeat_hits": review["repeat_hits"],
        "rows": [{
            "run_id": row["run_id"],
            "scheduled_cycle": row["scheduled_cycle"],
            "detector_id": row["detector_id"],
            "underlyer": row["underlyer"],
            "strategy_name": row["strategy_name"],
            "candidate_rank": row["candidate_rank"],
            "repeat_count": row["repeat_count"],
        } for row in review["rows"]],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    as_of = datetime.now(timezone.utc)
    repository = OptionAlertEvaluationRepository()
    source_repository = OptionAlertReviewSourceRepository()
    reports = {
        scope.lower(): compact(build_detector_alert_review(
            dataset_id=args.dataset_id,
            as_of=as_of,
            scope=scope,
            session_date=args.session_date,
            limit=200,
            repository=repository,
            source_repository=source_repository,
        ))
        for scope in ("LATEST", "HISTORY")
    }
    print(json.dumps({
        "version": "option_alert_view_membership_v1",
        "mode": "READ_ONLY",
        "as_of": as_of,
        **reports,
        "source_writes": 0,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
