#!/usr/bin/env python3
"""Validate or persist a point-in-time option event-calendar document."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from options.config import load_option_runtime_configuration  # noqa: E402
from options.event_calendar import parse_event_calendar_document  # noqa: E402
from options.repositories import OptionMarketEventRepository  # noqa: E402


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("document", type=Path)
    result.add_argument(
        "--apply",
        action="store_true",
        help="Persist facts; the default validates and reports only.",
    )
    return result


def main() -> int:
    args = parser().parse_args()
    payload = json.loads(args.document.read_text(encoding="utf-8"))
    batch = parse_event_calendar_document(
        payload, received_at=datetime.now(timezone.utc)
    )
    configuration = load_option_runtime_configuration()
    result = {
        "status": "DRY_RUN",
        "source": batch.source,
        "observed_at": batch.observed_at,
        "coverage_count": len(batch.coverage),
        "event_count": len(batch.events),
        "configured_source": configuration.settings.event_calendar_provider,
        "active_for_runtime": (
            batch.source == configuration.settings.event_calendar_provider
        ),
    }
    if args.apply:
        persisted = OptionMarketEventRepository().persist_batch(
            batch.coverage, batch.events
        )
        result.update({"status": "APPLIED", **asdict(persisted)})
    print(json.dumps(result, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())