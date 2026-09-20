#!/usr/bin/env python3
"""Verify the current 13-name behavior cohort retries without inserting records."""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor
from equity.behavior_producer import produce_stock_behavior_snapshots
from equity.domain import DecisionWatermark
from equity.repositories import (
    EquityBarRepository,
    EquityCorporateActionRepository,
    EquityEvidenceRepository,
    EquityReferenceRepository,
)
from options.config import load_option_runtime_configuration


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--apply", action="store_true")
    result.add_argument("--confirm-existing-count", type=int)
    return result


def counts() -> dict[str, int]:
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '15s'")
        cursor.execute("""
            SELECT
                (SELECT COUNT(*) FROM equity_context_snapshots
                 WHERE context_kind = 'STOCK_BEHAVIOR') AS behavior_contexts,
                (SELECT COUNT(*) FROM equity_context_evidence AS link
                 JOIN equity_context_snapshots AS context
                   USING (equity_context_snapshot_id)
                 WHERE context.context_kind = 'STOCK_BEHAVIOR') AS behavior_links,
                (SELECT COUNT(*) FROM equity_evidence
                 WHERE source_name IN (
                     'STOCK_BEHAVIOR_ADJUSTED_DAILY', 'STOCK_BEHAVIOR_RAW_SOURCE'
                 )) AS behavior_source_evidence
        """)
        return {key: int(value) for key, value in dict(cursor.fetchone()).items()}


def main() -> int:
    arguments = parser().parse_args()
    if arguments.apply and arguments.confirm_existing_count != 13:
        raise RuntimeError("--apply requires --confirm-existing-count 13")
    checked_at = datetime.now(timezone.utc)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '15s'")
        cursor.execute("""
            SELECT market_time FROM equity_analysis_runs
            WHERE interval = '30m' AND run_purpose = 'ORIGINAL'
              AND status IN ('COMPLETE', 'DEGRADED') AND published_at IS NOT NULL
              AND published_at <= %s
            ORDER BY market_time DESC, published_at DESC LIMIT 1
        """, (checked_at,))
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError("no terminal 30m ORIGINAL analysis is available")
    watermark = DecisionWatermark(row["market_time"], checked_at)
    tickers = load_option_runtime_configuration().settings.underlyers
    reference_repository = EquityReferenceRepository()
    securities = tuple(filter(None, (
        reference_repository.get_security_as_of(ticker, watermark)
        for ticker in tickers
    )))
    if len(securities) != len(tickers):
        raise RuntimeError("the exact 13 security references are unavailable")
    evidence_repository = EquityEvidenceRepository()
    available = sum(
        evidence_repository.get_behavior_as_of(
            security.security_id, watermark, profile="OPTIONS_SWING_V1",
            definition_sha256=__import__("equity.behavior", fromlist=["DEFINITION_SHA256"]).DEFINITION_SHA256,
            policy_sha256=__import__("equity.behavior", fromlist=["OPTIONS_SWING_PROFILE"]).OPTIONS_SWING_PROFILE.sha256,
        ) is not None
        for security in securities
    )
    if available != 13:
        raise RuntimeError(f"expected 13 current behavior readers, found {available}")
    before = counts()
    result = None
    after = before
    if arguments.apply:
        result = produce_stock_behavior_snapshots(
            securities, watermark=watermark,
            evidence_repository=evidence_repository,
            bar_repository=EquityBarRepository(),
            corporate_action_repository=EquityCorporateActionRepository(),
            received_at=checked_at,
        )
        after = counts()
        if (
            result.inserted != 0 or result.existing != 13
            or result.skipped or result.failed or before != after
        ):
            raise RuntimeError("public behavior retry was not exactly idempotent")
    print(json.dumps({
        "operation": "VERIFY_STOCK_BEHAVIOR_RETRY",
        "apply_requested": arguments.apply,
        "checked_at": checked_at,
        "watermark": asdict(watermark),
        "operational_readers": available,
        "before": before,
        "result": asdict(result) if result is not None else None,
        "after": after,
        "counts_unchanged": before == after,
    }, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())