"""Operate the observation-only ridge/momentum/SPY research tracker."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import threading

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from research.paper_tracker import PaperLedger, check_current_inputs, freeze_manifest, observe, report_study, utc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=BACKEND_DIR / "backups" / "equity-paper" / "ridge_forward_v1")
    parser.add_argument("--source", type=Path, default=BACKEND_DIR.parent / "docs" / "equity_ridge_challenger_results.json")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--enroll", action="store_true")
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--watch", action="store_true")
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--check-inputs", action="store_true")
    args = parser.parse_args()
    ledger = PaperLedger(args.directory)
    if args.enroll:
        with ledger.locked():
            if ledger.read("manifest") is None:
                ledger.append("manifest", freeze_manifest(args.source, datetime.now(timezone.utc)))
        result = report_study(ledger)
    elif args.status:
        result = report_study(ledger)
    elif args.check_inputs:
        result = check_current_inputs(ledger)
    elif args.once:
        result = observe(ledger)
    else:
        if ledger.read("manifest") is None:
            raise ValueError("enroll the paper study before starting its watcher")
        previous = None
        stop = threading.Event()
        try:
            while not stop.is_set():
                try:
                    result = observe(ledger)
                    message = {key: value for key, value in result.items() if key != "observed_at"}
                except Exception as error:
                    message = dict(status="OBSERVER_ERROR", error_type=type(error).__name__, detail="Run --once for diagnostics; no orders were sent")
                encoded = json.dumps(message, sort_keys=True)
                if encoded != previous:
                    print(json.dumps(message, indent=2), flush=True)
                    previous = encoded
                manifest = ledger.read("manifest")
                if message.get("report", {}).get("completed_cohorts") == 6:
                    break
                if datetime.now(timezone.utc) > utc(manifest["cohorts"][-1]["exit_close"]) + timedelta(days=7):
                    print(json.dumps(dict(status="STOPPED_FOR_REVIEW", reason="Six-cohort observation calendar ended; incomplete data is not a pass")), flush=True)
                    break
                stop.wait(60)
        except KeyboardInterrupt:
            print("Paper observer stopped; immutable research records retained.", flush=True)
        return 0
    print(json.dumps(result, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())