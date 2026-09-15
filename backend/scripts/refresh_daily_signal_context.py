"""Publish current daily ranks and discovery together without rewriting prior dates."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import sys
import time

import pandas as pd
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor
from equity.calendar import latest_expected_market_time
from research.features import load_daily_panel
from scripts import generate_cross_sectional_signal as ranks
from scripts import generate_market_discovery as discovery


LOCK_NAME = "stock-screener:daily-signal-context"


def _validate_output(frame, date_column, session, members):
    if frame.empty or frame["ticker"].duplicated().any():
        raise ValueError("daily signal output is empty or contains duplicate tickers")
    if set(pd.to_datetime(frame[date_column]).dt.date) != {session}:
        raise ValueError("daily signal output fell back to an older session")
    names = set(frame["ticker"])
    if not names.issubset(members) or len(names) < max(50, math.ceil(len(members) * 0.90)):
        raise ValueError("daily signal output has insufficient or foreign universe coverage")


def refresh_current_daily_signals(*, now=None, dry_run=False):
    started = time.perf_counter()
    observed_at = now or datetime.now(timezone.utc)
    market_time = latest_expected_market_time(
        observed_at - timedelta(minutes=int(os.getenv("EQUITY_PROVIDER_DELAY_MINUTES", "15"))),
        "1d",
    )
    session = market_time.date()
    result = {"session": session.isoformat(), "market_time": market_time.isoformat()}
    with get_db_cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS acquired", (LOCK_NAME,))
        if not cursor.fetchone()["acquired"]:
            return result | {"status": "BUSY"}
        cursor.execute(
            """
            SELECT publication_id, status, expected_members, selected_members
            FROM equity_bar_publications
            WHERE interval = '1d' AND market_time = %s AND observed_at <= %s
              AND session_scope = 'RTH' AND adjusted = FALSE
                            AND published_at IS NOT NULL AND published_at <= %s
            ORDER BY created_at DESC LIMIT 1
                        """, (market_time, observed_at, observed_at),
        )
        publication = cursor.fetchone()
        if (not publication or publication["status"] != "COMPLETE"
                or publication["selected_members"] != publication["expected_members"]):
            return result | {"status": "WAITING_FOR_COMPLETE_DAILY_PUBLICATION"}
        cursor.execute(
            "SELECT ticker FROM equity_bar_publication_members WHERE publication_id = %s AND status = 'SELECTED'",
            (publication["publication_id"],),
        )
        members = {row["ticker"] for row in cursor.fetchall()}
        cursor.execute("SELECT ticker FROM selected_tickers WHERE is_active = TRUE")
        selected = {row["ticker"] for row in cursor.fetchall()}
        if len(members) != publication["expected_members"] or members != selected:
            return result | {"status": "WAITING_FOR_CURRENT_UNIVERSE"}
        cursor.execute(
            "SELECT ticker, universe_size FROM cross_sectional_signals WHERE trade_date = %s AND model_version = %s",
            (session, ranks.MODEL_VERSION),
        )
        existing_ranks = cursor.fetchall()
        cursor.execute(
            "SELECT ticker FROM market_discovery_states WHERE trade_date = %s AND model_version = %s",
            (session, discovery.DISCOVERY_MODEL_VERSION),
        )
        existing_discovery = cursor.fetchall()
        if existing_ranks or existing_discovery:
            minimum = max(50, math.ceil(len(members) * 0.90))
            for rows in (existing_ranks, existing_discovery):
                if len(rows) < minimum or not {row["ticker"] for row in rows}.issubset(members):
                    raise ValueError("partial daily signal snapshots exist; refusing to rewrite them")
            if any(row["universe_size"] != len(existing_ranks) for row in existing_ranks):
                raise ValueError("stored rank universe_size is inconsistent")
            return result | {"status": "ALREADY_PRESENT", "rank_rows": len(existing_ranks),
                             "discovery_rows": len(existing_discovery)}
        cursor.execute(
            """
            SELECT EXISTS(SELECT 1 FROM cross_sectional_signals
                          WHERE model_version = %s AND trade_date > %s)
                OR EXISTS(SELECT 1 FROM market_discovery_states
                          WHERE model_version = %s AND trade_date > %s) AS newer
            """, (ranks.MODEL_VERSION, session, discovery.DISCOVERY_MODEL_VERSION, session),
        )
        if cursor.fetchone()["newer"]:
            raise ValueError("daily signal refresh cannot backfill historical predictions")
        start = (pd.Timestamp(session) - pd.Timedelta(days=1000)).date().isoformat()
        panel = load_daily_panel(start, session.isoformat(), sorted(members), available_by=observed_at)
        latest = panel[panel["date"] == pd.Timestamp(session)]
        _validate_output(latest, "date", session, members)
        if set(latest["ticker"]) != members:
            raise ValueError("published daily cohort has missing visible input bars")
        cross = ranks.compute_signal(session.isoformat(), panel=panel)
        states = discovery.compute(session.isoformat(), panel=panel)
        _validate_output(cross, "date", session, members)
        _validate_output(states, "trade_date", session, members)
        result |= {"publication_id": str(publication["publication_id"]),
                   "rank_rows": len(cross), "discovery_rows": len(states), "panel_rows": len(panel),
                   "compute_seconds": round(time.perf_counter() - started, 3)}
        if dry_run:
            return result | {"status": "DRY_RUN"}
        ranks.persist(cross, cursor=cursor)
        discovery.persist(states, cursor=cursor, retain_history=True)
        return result | {"status": "PUBLISHED"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args()
    report = refresh_current_daily_signals(dry_run=arguments.dry_run)
    print(json.dumps(report, indent=2))
    return 0 if report["status"] in ("PUBLISHED", "ALREADY_PRESENT", "DRY_RUN") else 1


if __name__ == "__main__":
    raise SystemExit(main())