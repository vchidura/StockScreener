#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import logging
import os
import socket
import sys
from pathlib import Path
from uuid import uuid4


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env")

from options import build_option_startup_state, load_option_runtime_configuration
from options.startup import ensure_option_partitions
from options.data import build_data_engine
from options.orchestration import ManualOptionPipeline
from options.outcome_service import OptionOutcomeService
from options.repositories import OptionOutcomeRepository, OptionSchedulerLeadership
from options.strategy_orchestration import OptionStrategyPipeline
from options.worker import OptionMaterializationWorker


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delayed option materialization worker")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process the latest observable delayed slot and exit.",
    )
    parser.add_argument(
        "--underlyers",
        help="Optional comma-separated configured underlyers for a controlled run.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )
    configuration = load_option_runtime_configuration()
    build_option_startup_state(configuration)
    instance_id = uuid4()
    leadership = OptionSchedulerLeadership(
        instance_id=instance_id,
        configuration_sha256=configuration.configuration_sha256,
        policy_sha256=configuration.policy_sha256,
        process_id=os.getpid(),
        host_name=socket.gethostname(),
    )
    if not leadership.acquire():
        logging.getLogger("option-worker").error(
            "another option worker holds leadership"
        )
        return 2
    try:
        strategy_pipeline = OptionStrategyPipeline(configuration)
        outcome_repository = OptionOutcomeRepository()
        pipeline = ManualOptionPipeline(
            configuration,
            build_data_engine(configuration),
            strategy_pipeline=strategy_pipeline,
            outcome_repository=outcome_repository,
        )
        underlyers = None
        if args.underlyers:
            underlyers = tuple(
                value.strip().upper()
                for value in args.underlyers.split(",")
                if value.strip()
            )
        worker = OptionMaterializationWorker(
            pipeline,
            underlyers=underlyers,
            outcome_service=OptionOutcomeService(outcome_repository),
            partition_maintainer=ensure_option_partitions,
        )
        if args.once:
            result = worker.poll_once(leadership.heartbeat)
            payload = (
                asdict(result)
                if result is not None
                else {"status": "NO_OBSERVABLE_SLOT"}
            )
            print(json.dumps(payload, sort_keys=True, default=str))
        else:
            worker.run_forever(leadership)
    finally:
        leadership.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())