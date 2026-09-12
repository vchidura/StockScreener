#!/usr/bin/env python3
"""Requeue terminal option work matching one exact repaired error."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--error")
    parser.add_argument("--all-current-config", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    if not args.error and not args.all_current_config:
        raise RuntimeError("provide --error or --all-current-config")
    configuration = load_option_runtime_configuration()
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS count
            FROM option_work_items AS work
            JOIN option_ingestion_runs AS ingestion
              ON work.subject_id = ingestion.batch_id::text
            WHERE work.stage = 'NORMALIZE'
              AND work.status = 'TERMINAL_FAILED'
              AND (%s OR work.last_error = %s)
              AND ingestion.configuration_sha256 = %s
              AND ingestion.policy_sha256 = %s
            """,
            (
                args.all_current_config,
                args.error,
                configuration.configuration_sha256,
                configuration.policy_sha256,
            ),
        )
        count = cursor.fetchone()["count"]
        if args.apply:
            if args.confirm != "REQUEUE":
                raise RuntimeError("--apply requires --confirm REQUEUE")
            cursor.execute(
                """
                UPDATE option_work_items AS work
                SET status = 'RETRY', attempt_count = 0,
                    next_attempt_at = NOW(), lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = NOW()
                FROM option_ingestion_runs AS ingestion
                WHERE work.subject_id = ingestion.batch_id::text
                  AND work.stage = 'NORMALIZE'
                  AND work.status = 'TERMINAL_FAILED'
                  AND (%s OR work.last_error = %s)
                  AND ingestion.configuration_sha256 = %s
                  AND ingestion.policy_sha256 = %s
                """,
                (
                    args.all_current_config,
                    args.error,
                    configuration.configuration_sha256,
                    configuration.policy_sha256,
                ),
            )
    print({"matched": count, "status": "REQUEUED" if args.apply else "DRY_RUN"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())