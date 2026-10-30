"""Offline roadmap model replay. Never imports database or fetches provider data."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import gzip
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.equity_models.catalog import CATALOG, DEFAULT_MODELS
from research.equity_detection_backtest import run_detection_backtest, write_run


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list-models", action="store_true")
    parser.add_argument("--input", type=Path, help="frozen equity_research_dataset_v1 JSON or JSON.gz")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--models", nargs="+", choices=sorted(CATALOG), default=DEFAULT_MODELS)
    parser.add_argument("--securities", nargs="+")
    parser.add_argument("--start", help="timezone-aware evaluation start")
    parser.add_argument("--end", help="timezone-aware evaluation end")
    parser.add_argument("--as-of", help="timezone-aware measurement cutoff")
    parser.add_argument("--macro-policy", choices=("required", "none"), default="required",
                        help="explicit research override; changes policy and run identity")
    args = parser.parse_args(argv)
    if args.list_models:
        print(json.dumps([dict(asdict(p), policy_hash=p.policy_hash) for p in CATALOG.values()], indent=2))
        return 0
    if not args.input or not args.output:
        parser.error("--input and --output are required for replay")
    opener = gzip.open if args.input.suffix == ".gz" else open
    with opener(args.input, "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    result = run_detection_backtest(payload, models=args.models, securities=args.securities,
                                    start=args.start, end=args.end, as_of=args.as_of, macro_policy=args.macro_policy)
    write_run(result, args.output)
    print(json.dumps(dict(run_id=result["manifest"]["run_id"], evaluations=len(result["evaluations"]),
                          matches=sum(r["status"] == "MATCH" for r in result["evaluations"]),
                          output=str(args.output.resolve())), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
