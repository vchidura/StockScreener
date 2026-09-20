#!/usr/bin/env python3
"""Run one assessment-only option dry-run directly for runtime diagnostics."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from options.api import (  # noqa: E402
    OptionAlertDryRunRequest,
    option_alert_publications_dry_run,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate_id", type=UUID)
    parser.add_argument("--strategy", required=True)
    arguments = parser.parse_args()
    request = OptionAlertDryRunRequest.model_validate({
        "policy_version": "wp4_stock_behavior_shadow_v1",
        "strategies": [{
            "strategy_name": arguments.strategy,
            "management_source": "ORIGINAL",
        }],
        "candidates": [{"candidate_id": str(arguments.candidate_id)}],
    })
    result = option_alert_publications_dry_run(request)
    print(json.dumps(result.model_dump(mode="json"), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())