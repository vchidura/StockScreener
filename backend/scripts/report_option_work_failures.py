#!/usr/bin/env python3
"""Report active-configuration option work status and sanitized errors."""
from __future__ import annotations

import json
import argparse
from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env", override=True)

from database import get_db_cursor  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=BACKEND_DIR.parent / "docs" / "option_work_failures.json",
    )
    args = parser.parse_args()
    configuration = load_option_runtime_configuration()
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT work.stage, work.status, work.last_error,
                   COUNT(*) AS count, MIN(work.next_attempt_at) AS next_attempt_at
            FROM option_work_items AS work
            LEFT JOIN option_ingestion_runs AS ingestion
              ON work.subject_id = ingestion.batch_id::text
            LEFT JOIN option_analysis_runs AS analysis
              ON work.subject_id = analysis.matrix_id::text
            LEFT JOIN option_ingestion_runs AS analysis_ingestion
              ON analysis_ingestion.batch_id = analysis.batch_id
            WHERE COALESCE(
                ingestion.configuration_sha256,
                analysis_ingestion.configuration_sha256
            ) = %s
            GROUP BY work.stage, work.status, work.last_error
            ORDER BY work.stage, work.status, work.last_error
            """,
            (configuration.configuration_sha256,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            """
            SELECT ingestion.batch_id, ingestion.underlying,
                   ingestion.received_row_count, ingestion.catalog_row_count,
                   ingestion.retained_row_count, ingestion.rejected_counts,
                   ingestion.unknown_reference_count,
                   ingestion.market_data_time, ingestion.completed_at
            FROM option_ingestion_runs AS ingestion
            WHERE ingestion.configuration_sha256 = %s
              AND ingestion.policy_sha256 = %s
            ORDER BY ingestion.started_at DESC, ingestion.underlying
            LIMIT 50
            """,
            (
                configuration.configuration_sha256,
                configuration.policy_sha256,
            ),
        )
        batches = [dict(row) for row in cursor.fetchall()]
    report = {
        "configuration_sha256": configuration.configuration_sha256,
        "groups": rows,
        "batches": batches,
    }
    rendered = json.dumps(report, default=str, indent=2)
    args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
