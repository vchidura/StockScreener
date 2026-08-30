import argparse
from dataclasses import asdict, is_dataclass
import json
import os
import socket
import sys
from pathlib import Path
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options import build_option_startup_state, load_option_runtime_configuration
from options.data import build_data_engine
from options.orchestration import ManualOptionPipeline
from options.repositories import OptionSchedulerLeadership
from options.strategy_orchestration import OptionStrategyPipeline
from database import get_db_cursor


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polygon Options Developer pipeline")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one controlled read-only ingestion/analysis cycle.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print sanitized durable pipeline counts and latest states.",
    )
    parser.add_argument(
        "--strategies-only",
        action="store_true",
        help="Evaluate the latest compatible durable matrices without provider calls.",
    )
    parser.add_argument(
        "--underlyers",
        help="Optional comma-separated configured underlyers for the manual cycle.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    configuration = load_option_runtime_configuration()
    startup = build_option_startup_state(configuration)
    if args.status:
        with get_db_cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(1) FROM option_contract_catalog) AS catalog_contracts,
                    (SELECT COUNT(1) FROM option_chain_snapshots) AS snapshots,
                    (SELECT COUNT(1) FROM option_analysis_runs) AS analysis_runs,
                    (SELECT COUNT(1) FROM option_ingestion_runs) AS ingestion_runs,
                    CASE WHEN to_regclass('option_strategy_candidates') IS NULL
                        THEN 0 ELSE (SELECT COUNT(1) FROM option_strategy_candidates)
                    END AS strategy_candidates,
                    CASE WHEN to_regclass('option_signal_events') IS NULL
                        THEN 0 ELSE (SELECT COUNT(1) FROM option_signal_events)
                    END AS signal_events
                """
            )
            counts = dict(cursor.fetchone())
            cursor.execute(
                """
                SELECT underlying, status, received_row_count, retained_row_count,
                       unknown_reference_count, completed_at
                FROM option_ingestion_runs
                ORDER BY started_at DESC
                LIMIT 20
                """
            )
            ingestion = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT stage, status, attempt_count, next_attempt_at,
                       lease_expires_at, last_error
                FROM option_work_items
                ORDER BY created_at DESC
                LIMIT 20
                """
            )
            work = [dict(row) for row in cursor.fetchall()]
        print(
            json.dumps(
                {"counts": counts, "ingestion": ingestion, "work": work},
                sort_keys=True,
                default=str,
            )
        )
        return
    if not args.once and not args.strategies_only:
        print(json.dumps(startup.metadata(), sort_keys=True, default=str))
        return

    underlyers = None
    if args.underlyers:
        underlyers = tuple(
            value.strip().upper()
            for value in args.underlyers.split(",")
            if value.strip()
        )
    instance_id = uuid4()
    leadership = OptionSchedulerLeadership(
        instance_id=instance_id,
        configuration_sha256=configuration.configuration_sha256,
        policy_sha256=configuration.policy_sha256,
        process_id=os.getpid(),
        host_name=socket.gethostname(),
    )
    with leadership:
        strategy_pipeline = OptionStrategyPipeline(configuration)
        if args.strategies_only:
            result = strategy_pipeline.run_latest(underlyers)
        else:
            result = ManualOptionPipeline(
                configuration,
                build_data_engine(configuration),
                strategy_pipeline=strategy_pipeline,
            ).run_once(underlyers)
    payload = (
        asdict(result)
        if is_dataclass(result)
        else [asdict(item) if is_dataclass(item) else item for item in result]
    )
    print(json.dumps(payload, sort_keys=True, default=str))


if __name__ == "__main__":
    main()