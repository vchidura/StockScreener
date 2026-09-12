#!/usr/bin/env python3
"""Publish the newest complete configured-universe option Board revision."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402
from options.repositories.board import (  # noqa: E402
    BOARD_SELECTOR_SHA256,
    BOARD_SELECTOR_VERSION,
    OptionBoardPublicationRepository,
)
from options.strategies.gates import GATE_LEDGER_VERSION  # noqa: E402


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--apply",
        action="store_true",
        help="Persist the publication; default is a read-only dry run.",
    )
    return result


def latest_complete_cycle(
    underlyers: tuple[str, ...],
    strategy_policy_sha256: str,
    configuration_sha256: str,
):
    with get_db_cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout = '30s'")
        cursor.execute(
            """
            SELECT run.scheduled_cycle,
                   COUNT(DISTINCT analysis.underlying) AS underlyings
            FROM option_ingestion_runs AS run
            JOIN option_analysis_runs AS analysis USING (batch_id)
            WHERE analysis.status = 'COMPLETE'
                            AND run.configuration_sha256 = %s
              AND analysis.underlying = ANY(%s)
              AND EXISTS (
                  SELECT 1
                  FROM option_strategy_candidates AS candidate
                  WHERE candidate.matrix_id = analysis.matrix_id
                    AND candidate.policy_sha256 = %s
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM option_strategy_candidates AS candidate
                  WHERE candidate.matrix_id = analysis.matrix_id
                    AND candidate.policy_sha256 = %s
                    AND (
                        SELECT COUNT(*)
                        FROM option_candidate_execution_gates AS gate
                        WHERE gate.candidate_id = candidate.candidate_id
                                                    AND gate.ledger_version = %s
                    ) <> 6
              )
            GROUP BY run.scheduled_cycle
            HAVING COUNT(DISTINCT analysis.underlying) = %s
            ORDER BY run.scheduled_cycle DESC
            LIMIT 1
            """,
            (
                configuration_sha256,
                list(underlyers),
                strategy_policy_sha256,
                strategy_policy_sha256,
                GATE_LEDGER_VERSION,
                len(underlyers),
            ),
        )
        row = cursor.fetchone()
    return row["scheduled_cycle"] if row else None


def main() -> int:
    args = parser().parse_args()
    configuration = load_option_runtime_configuration()
    cycle = latest_complete_cycle(
        configuration.settings.underlyers,
        configuration.strategy_policy_sha256,
        configuration.configuration_sha256,
    )
    if cycle is None:
        print(json.dumps({"status": "NO_COMPLETE_CONFIGURED_UNIVERSE_CYCLE"}))
        return 1
    payload = {
        "status": "READY",
        "scheduled_cycle": cycle,
        "selector_version": BOARD_SELECTOR_VERSION,
        "selector_sha256": BOARD_SELECTOR_SHA256,
        "underlyings": len(configuration.settings.underlyers),
    }
    if not args.apply:
        payload["status"] = "DRY_RUN"
        print(json.dumps(payload, default=str, indent=2), flush=True)
        return 0
    print(
        json.dumps(
            {**payload, "status": "APPLYING"}, default=str, indent=2
        ),
        flush=True,
    )
    published_at = datetime.now(timezone.utc)
    result = OptionBoardPublicationRepository().publish_complete_cycle(
        scheduled_cycle=cycle,
        as_of_session=OptionExchangeCalendar().session_for_slot(cycle),
        expected_underlyers=configuration.settings.underlyers,
        strategy_policy_sha256=configuration.strategy_policy_sha256,
        configuration_sha256=configuration.configuration_sha256,
        published_at=published_at,
    )
    payload["publication"] = asdict(result)
    payload["status"] = result.status
    print(json.dumps(payload, default=str, indent=2), flush=True)
    return 0 if result.publication_id is not None else 2


if __name__ == "__main__":
    raise SystemExit(main())