#!/usr/bin/env python3
"""Exercise the real stock behavior producer path without persisting any records."""
from __future__ import annotations

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

from equity.behavior_producer import produce_stock_behavior_snapshots
from database import get_db_cursor
from equity.domain import DecisionWatermark
from equity.repositories import (
    EquityBarRepository,
    EquityCorporateActionRepository,
    EquityEvidenceRepository,
    EquityReferenceRepository,
)
from options.config import load_option_runtime_configuration


def main() -> int:
    checked_at = datetime.now(timezone.utc)
    watermark = DecisionWatermark(checked_at, checked_at)
    tickers = load_option_runtime_configuration().settings.underlyers
    reference_repository = EquityReferenceRepository()
    securities = tuple(filter(None, (
        reference_repository.get_security_as_of(ticker, watermark)
        for ticker in tickers
    )))
    found = {security.ticker for security in securities}
    missing = tuple(ticker for ticker in tickers if ticker not in found)
    bar_repository = EquityBarRepository()
    adjusted_reads = bar_repository.read_behavior_adjusted_daily(
        tickers, watermark, limit_per_ticker=273,
    )
    adjusted_sessions = {
        read.ticker: tuple(bar.session_date for bar in read.bars)
        for read in adjusted_reads
    }
    spy_sessions = set(adjusted_sessions.get("SPY", ()))
    adjusted_windows = [{
        "ticker": ticker,
        "bar_count": len(sessions),
        "window_start": sessions[0] if sessions else None,
        "window_end": sessions[-1] if sessions else None,
        "missing_vs_spy": sorted(spy_sessions - set(sessions)),
        "extra_vs_spy": sorted(set(sessions) - spy_sessions),
    } for ticker, sessions in adjusted_sessions.items()]
    result = produce_stock_behavior_snapshots(
        securities,
        watermark=watermark,
        evidence_repository=EquityEvidenceRepository(),
        bar_repository=bar_repository,
        corporate_action_repository=EquityCorporateActionRepository(),
        persist=False,
    ) if securities else None
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '15s'")
        cursor.execute("""
            SELECT DISTINCT ON (ticker) ticker, security_id, session_date,
                   bar_end, system_observed_at, created_at
            FROM equity_bar_revisions
            WHERE ticker = ANY(%s::text[]) AND interval = '1d'
              AND session_scope = 'RTH' AND adjusted = TRUE AND is_final = TRUE
              AND quality_codes @> ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::text[]
            ORDER BY ticker, session_date DESC, created_at DESC, bar_revision_id
        """, (list(tickers),))
        adjusted_latest = [dict(row) for row in cursor.fetchall()]
        cursor.execute("""
            SELECT ingestion_segment_id, status, requested_from, requested_to,
                   market_watermark, record_count, gap_details, created_at, completed_at
            FROM equity_ingestion_segments
            WHERE dataset = 'EQUITY_BEHAVIOR_ADJUSTED_DAILY'
            ORDER BY created_at DESC LIMIT 10
        """)
        segments = [dict(row) for row in cursor.fetchall()]
    payload = {
        "operation": "STOCK_BEHAVIOR_PRODUCTION_READINESS",
        "database_mutations": False,
        "checked_at": checked_at,
        "configured_tickers": tickers,
        "missing_security_references": missing,
        "adjusted_windows": adjusted_windows,
        "adjusted_latest": adjusted_latest,
        "latest_adjusted_ingestion_segment": segments[0] if segments else None,
        "recent_adjusted_ingestion_segments": segments,
        "result": asdict(result) if result is not None else None,
        "status": (
            "READY" if result is not None and not missing
            and result.planned == len(tickers) and not result.skipped and not result.failed
            else "BLOCKED"
        ),
    }
    print(json.dumps(payload, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())