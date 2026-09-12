#!/usr/bin/env python3
"""Backfill reconstructed Treasury curves needed by settlement-IV research."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from http_client import get_session  # noqa: E402
from options.model_inputs import TREASURY_CURVE_URL, parse_treasury_curve  # noqa: E402
from options.repositories.model_inputs import OptionModelInputRepository  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, default=2024)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.end_year < args.start_year:
        raise ValueError("end year must not precede start year")
    observed_at = datetime.now(timezone.utc)
    rows = []
    for year in range(args.start_year, args.end_year + 1):
        response = get_session().get(
            TREASURY_CURVE_URL.format(year=year), timeout=(5, 30)
        )
        response.raise_for_status()
        rows.extend(parse_treasury_curve(response.text, received_at=observed_at))
    inserted = OptionModelInputRepository().persist_rates(rows) if args.apply else 0
    print({
        "years": args.end_year - args.start_year + 1,
        "observations": len(rows),
        "inserted": inserted,
        "mode": "APPLY" if args.apply else "DRY_RUN",
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())