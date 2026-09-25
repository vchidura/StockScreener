#!/usr/bin/env python3
"""Mature option outcomes once after each finished detector cycle."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from equity.leadership import try_advisory_leadership  # noqa: E402
from options import load_option_runtime_configuration  # noqa: E402
from options.outcome_service import OptionOutcomeService  # noqa: E402
from options.repositories import OptionOutcomeRepository  # noqa: E402
from scripts.run_stock_alert_service import write_status  # noqa: E402


LOGGER = logging.getLogger("option-outcome-worker")
POLL_SECONDS = int(os.getenv("OPTION_OUTCOME_WORKER_POLL_SECONDS", "15"))
WORKER_LOCK_NAME = os.getenv(
    "OPTION_OUTCOME_WORKER_LOCK_NAME", "stock-screener:option-outcome-worker",
)
ROOT = BACKEND_DIR / "backups/options-outcome-worker"
DETECTOR_STATUS = BACKEND_DIR / "backups/options-worker/detector-status.json"


def read_finished_cycle(path: Path = DETECTOR_STATUS) -> dict | None:
    if not path.is_file():
        return None
    if path.stat().st_size > 262144:
        raise ValueError("detector status exceeds bound")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != "option_detector_operational_status_v1":
        raise ValueError("detector status identity mismatch")
    attempts = payload.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        return None
    latest = attempts[-1]
    if (not isinstance(latest, dict) or latest.get("status") == "RUNNING"
            or not latest.get("finished_at") or not latest.get("scheduled_cycle")):
        return None
    finished_at = datetime.fromisoformat(latest["finished_at"])
    scheduled_cycle = datetime.fromisoformat(latest["scheduled_cycle"])
    if finished_at.utcoffset() is None or scheduled_cycle.utcoffset() is None:
        raise ValueError("detector cycle clocks must be timezone-aware")
    return dict(dataset_id=payload.get("dataset_id"), scheduled_cycle=scheduled_cycle,
        finished_at=finished_at, status=latest["status"], run_id=latest.get("run_id"))


def read_checkpoint(path: Path = ROOT / "status.json") -> str | None:
    if not path.is_file():
        return None
    if path.stat().st_size > 262144:
        raise ValueError("outcome worker status exceeds bound")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != "option_outcome_worker_v1":
        raise ValueError("outcome worker status identity mismatch")
    return payload.get("completed_cycle")


def process_once(service, *, clock=lambda: datetime.now(timezone.utc),
                 detector_status: Path = DETECTOR_STATUS,
                 status_path: Path = ROOT / "status.json") -> dict:
    checked_at = clock().astimezone(timezone.utc)
    source = read_finished_cycle(detector_status)
    completed = read_checkpoint(status_path)
    if source is None:
        result = dict(version="option_outcome_worker_v1", state="WAITING_FOR_FINISHED_CYCLE",
            checked_at=checked_at.isoformat(), completed_cycle=completed)
        write_status(status_path, result)
        return result
    cycle_key = source["scheduled_cycle"].isoformat()
    if completed == cycle_key:
        result = dict(version="option_outcome_worker_v1", state="WAITING_FOR_NEW_CYCLE",
            checked_at=checked_at.isoformat(), completed_cycle=completed,
            source_dataset_id=source["dataset_id"], source_status=source["status"])
        write_status(status_path, result)
        return result
    started = time.perf_counter()
    outcomes = service.mature(available_by=checked_at, limit=1000)
    result = dict(version="option_outcome_worker_v1", state="COMPLETE",
        checked_at=checked_at.isoformat(), completed_cycle=cycle_key,
        source_dataset_id=source["dataset_id"], source_status=source["status"],
        source_run_id=source["run_id"], duration_seconds=round(time.perf_counter() - started, 3),
        candidates=outcomes.candidates, due=outcomes.due_measurements,
        available=outcomes.available_measurements, persisted=outcomes.persisted,
        pending=outcomes.pending, current_candidates=outcomes.current_candidates,
        current_persisted=outcomes.current_persisted,
        unavailable_measurements=outcomes.unavailable_measurements,
        unavailable_persisted=outcomes.unavailable_persisted)
    write_status(status_path, result)
    LOGGER.info("option outcomes cycle=%s duration_seconds=%s candidates=%s due=%s available=%s persisted=%s pending=%s",
        cycle_key, result["duration_seconds"], result["candidates"], result["due"],
        result["available"], result["persisted"], result["pending"])
    return result


def run_worker(service, *, once=False, wait=time.sleep) -> None:
    while True:
        try:
            process_once(service)
        except Exception:
            LOGGER.exception("option outcome cycle failed; committed outcomes remain idempotent")
            if once:
                raise
        if once:
            return
        wait(POLL_SECONDS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if POLL_SECONDS <= 0:
        raise ValueError("option outcome poll seconds must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    configuration = load_option_runtime_configuration(dict(os.environ), BACKEND_DIR)
    repository = OptionOutcomeRepository()
    service = OptionOutcomeService(repository, policy=configuration.valuation_policy,
        availability_evidence_enabled=configuration.settings.outcome_unavailable_evidence_enabled)
    with try_advisory_leadership(WORKER_LOCK_NAME) as is_leader:
        if not is_leader:
            LOGGER.error("another option outcome worker holds leadership")
            return 2
        run_worker(service, once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
