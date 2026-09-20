#!/usr/bin/env python3
"""Validate non-executing option evidence configuration without database/provider access."""
from __future__ import annotations

import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_option_runtime_configuration  # noqa: E402
from options.outcome_contracts import PACKAGE_ASSESSMENT_POLICY  # noqa: E402


def main() -> int:
    try:
        configuration = load_option_runtime_configuration()
    except Exception as exc:
        print(
            json.dumps({"status": "INVALID", "error_type": type(exc).__name__,
                        "error": str(exc)}),
            file=sys.stderr,
        )
        return 2
    launch = configuration.stock_behavior_shadow_launch
    print(json.dumps({
        "status": "VALID",
        "start_read_only": configuration.settings.start_read_only,
        "stock_behavior_shadow_enabled": (
            configuration.settings.stock_behavior_shadow_enabled
        ),
        "stock_behavior_launch_sha256": launch.sha256 if launch else None,
        "package_assessments_enabled": (
            configuration.settings.package_assessments_enabled
        ),
        "package_assessment_policy_sha256": PACKAGE_ASSESSMENT_POLICY.sha256,
        "outcome_unavailable_evidence_enabled": (
            configuration.settings.outcome_unavailable_evidence_enabled
        ),
        "execution_permission": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())