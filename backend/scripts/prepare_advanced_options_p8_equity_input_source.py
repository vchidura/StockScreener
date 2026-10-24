#!/usr/bin/env python3
"""Freeze one completed QQQ/SPY Equity window for a later P8 package."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_equity_control_plane import (  # noqa: E402
    load_advanced_equity_control_plane_plan_cursor,
    validate_active_target9_equity_source_cursor,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402
from realtime.relation_registry import APP_ROLE  # noqa: E402


SCHEMA = "advanced-options-p8-equity-input-source-v1"


def _args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equity-cohort-id", type=UUID, required=True)
    parser.add_argument("--equity-window-id", type=UUID, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _args()
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError("P8 Equity input source output must be new")
    if not output.parent.is_dir():
        raise FileNotFoundError(
            "P8 Equity input source output parent is absent"
        )
    load_dotenv(BACKEND_DIR / ".env", override=False)
    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=os.environ["REALTIME_V2_APP_DSN"],
        options="-c timezone=UTC",
    )
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=30_000,
            ),
        )
        provider.verify()
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            dict_cursor=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            plan = load_advanced_equity_control_plane_plan_cursor(
                unit.cursor,
                args.equity_cohort_id,
            )
            validate_active_target9_equity_source_cursor(
                unit.cursor,
                plan,
            )
            symbols = tuple(plan.universe.equity_tickers)
            unit.cursor.execute(
                "SELECT receipt.receipt_id::text,receipt.cutoff,"
                "receipt.reference_generation,receipt.universe_sha256,"
                "receipt.input_sha256,receipt.build_id,window_run.state,"
                "window_run.cohort_id::text "
                "FROM rt.window_runs AS window_run "
                "JOIN rt.cycle_receipts AS receipt "
                "ON receipt.window_id=window_run.window_id "
                "WHERE window_run.window_id=%s "
                "AND receipt.receipt_kind='INPUT_READY'",
                (str(args.equity_window_id),),
            )
            receipt = unit.cursor.fetchone()
    finally:
        pool.closeall()
    if (
        receipt is None
        or str(receipt["state"]) != "COMPLETE"
        or UUID(str(receipt["cohort_id"])) != args.equity_cohort_id
        or str(receipt["universe_sha256"]) != plan.universe.sha256
    ):
        raise ValueError(
            "P8 Equity input source receipt is unavailable"
        )
    report = {
        "source_cohort_id": str(plan.cohort_id),
        "source_cohort_sha256": plan.cohort_sha256,
        "source_manifest_id": str(plan.manifest_id),
        "source_manifest_sha256": plan.manifest.sha256,
        "source_profile_id": str(plan.profile_id),
        "source_profile_sha256": plan.profile_sha256,
        "equity_window_id": str(args.equity_window_id),
        "equity_receipt_id": str(receipt["receipt_id"]),
        "equity_cutoff_utc": receipt["cutoff"].isoformat(),
        "reference_generation": int(receipt["reference_generation"]),
        "universe_sha256": str(receipt["universe_sha256"]),
        "input_sha256": str(receipt["input_sha256"]),
        "build_id": str(receipt["build_id"]),
        "symbols": list(symbols),
    }
    semantic_sha256 = canonical_sha256(report)
    document = {
        "schema": SCHEMA,
        **report,
        "report_sha256": semantic_sha256,
    }
    output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": "P8_EQUITY_INPUT_SOURCE_FROZEN",
        "output": str(output),
        "semantic_sha256": semantic_sha256,
        "file_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "provider_requests": 0,
        "database_writes": 0,
        "symbols": list(symbols),
        "equity_cutoff_utc": receipt["cutoff"].isoformat(),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
