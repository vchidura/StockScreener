#!/usr/bin/env python3
"""Dispatch the V2 equity supervisor selected by backend/.env."""
from __future__ import annotations

import sys
from pathlib import Path

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.equity_pipeline_config import (  # noqa: E402
    EquityPipeline,
    RealtimeV2PipelineConfig,
)


def selected_main(config: RealtimeV2PipelineConfig):
    if config.equity_pipeline is EquityPipeline.MASSIVE_ADVANCED:
        from scripts.run_realtime_v2_advanced_services import main
    else:
        from scripts.run_realtime_v2_services import main
    return main


def main(argv=None):
    config = RealtimeV2PipelineConfig.from_mapping(
        dotenv_values(BACKEND_DIR / ".env")
    )
    return selected_main(config)(argv)


if __name__ == "__main__":
    raise SystemExit(main())
