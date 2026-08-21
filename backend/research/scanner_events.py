"""Optimized shadow capture and outcome evaluation for scanner events."""
from __future__ import annotations

import gc
import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from database import get_db_cursor, get_selected_tickers
from research.composite_scanners import build_all_scanner_events
from research.discovery_states import classify_discovery_history
from research.features import load_daily_panel
from research.scanner_calibration import walk_forward_calibration
from research.scanner_confidence import _benjamini_hochberg, _normal_p_value
from research.trend_pullback import load_hourly_panel

VALIDATION_STATUS = "UNVALIDATED_TIMING"
ROUND_TRIP_COST_BPS = 4.0
OUTCOME_ENTRY_MODEL = "next_bar_open_v2"
DAILY_PRICE_INTERVALS = {"1d", "1wk"}
HORIZONS = {"1d": (5, 10, 21), "1wk": (5, 10, 21), "1h": (7, 21, 35)}
LOOKBACK_DAYS = {"1d": 420, "1wk": 1800, "1h": 60}
BATCH_SIZE = 10
BACKFILL_BATCH_SIZE = 5
ACTIVE_DISCOVERY_STATES = {
    "CONTINUATION", "REVERSAL_WATCH", "EMERGING_REVERSAL",
    "REVERSAL_CONFIRMED", "CONFLICT", "LAGGARD",
}


def ensure_tables() -> None:
    with get_db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scanner_events (
                event_id BIGSERIAL PRIMARY KEY,
                event_key VARCHAR(160) NOT NULL UNIQUE,
                scanner_name VARCHAR(64) NOT NULL,
                scanner_version VARCHAR(32) NOT NULL,
                interval VARCHAR(8) NOT NULL,
                ticker VARCHAR(16) NOT NULL,
                signal_time TIMESTAMPTZ NOT NULL,
                last_seen_at TIMESTAMPTZ NOT NULL,
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                trade_date DATE NOT NULL,
                direction SMALLINT NOT NULL CHECK (direction IN (-1, 1)),
                trigger_type VARCHAR(48) NOT NULL,
                discovery_state VARCHAR(32),
                validation_status VARCHAR(32) NOT NULL DEFAULT 'UNVALIDATED_TIMING',
                entry_price DOUBLE PRECISION NOT NULL,
                atr_at_signal DOUBLE PRECISION,
                reference_level DOUBLE PRECISION,
                stop_price DOUBLE PRECISION,
                target_price DOUBLE PRECISION,
                risk_per_share DOUBLE PRECISION,
                round_trip_cost_bps REAL NOT NULL DEFAULT 4.0,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scanner_event_outcomes (
                outcome_id BIGSERIAL PRIMARY KEY,
                event_id BIGINT NOT NULL REFERENCES scanner_events(event_id) ON DELETE CASCADE,
                horizon_bars SMALLINT NOT NULL CHECK (horizon_bars > 0),
                bars_observed SMALLINT NOT NULL,
                exit_time TIMESTAMPTZ NOT NULL,
                exit_price DOUBLE PRECISION NOT NULL,
                raw_return DOUBLE PRECISION NOT NULL,
                signed_return DOUBLE PRECISION NOT NULL,
                net_signed_return DOUBLE PRECISION NOT NULL,
                benchmark_return DOUBLE PRECISION,
                alpha_return DOUBLE PRECISION,
                net_alpha_return DOUBLE PRECISION,
                mae_pct DOUBLE PRECISION,
                mfe_pct DOUBLE PRECISION,
                mae_r DOUBLE PRECISION,
                mfe_r DOUBLE PRECISION,
                stop_hit BOOLEAN NOT NULL DEFAULT FALSE,
                target_hit BOOLEAN NOT NULL DEFAULT FALSE,
                first_hit VARCHAR(16) NOT NULL DEFAULT 'NONE'
                          CHECK (first_hit IN ('STOP', 'TARGET', 'SAME_BAR', 'NONE')),
                entry_time TIMESTAMPTZ,
                entry_price DOUBLE PRECISION,
                entry_model VARCHAR(32) NOT NULL DEFAULT 'signal_close_v1',
                evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (event_id, horizon_bars)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scanner_events_time ON scanner_events (signal_time DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scanner_events_ticker ON scanner_events (ticker, signal_time DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scanner_events_scanner ON scanner_events (scanner_name, interval, signal_time DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scanner_events_state ON scanner_events (discovery_state, signal_time DESC)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_scanner_event_key ON scanner_events (event_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scanner_outcomes_event ON scanner_event_outcomes (event_id, horizon_bars)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_scanner_outcome_horizon ON scanner_event_outcomes (event_id, horizon_bars)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scanner_outcomes_evaluated ON scanner_event_outcomes (evaluated_at DESC)")
        cur.execute("ALTER TABLE scanner_event_outcomes ADD COLUMN IF NOT EXISTS entry_time TIMESTAMPTZ")
        cur.execute("ALTER TABLE scanner_event_outcomes ADD COLUMN IF NOT EXISTS entry_price DOUBLE PRECISION")
        cur.execute("ALTER TABLE scanner_event_outcomes ADD COLUMN IF NOT EXISTS entry_model VARCHAR(32) NOT NULL DEFAULT 'signal_close_v1'")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scanner_event_occurrences (
                occurrence_id BIGSERIAL PRIMARY KEY,
                event_id BIGINT NOT NULL REFERENCES scanner_events(event_id) ON DELETE CASCADE,
                signal_time TIMESTAMPTZ NOT NULL,
                trade_date DATE NOT NULL,
                trigger_type VARCHAR(48) NOT NULL,
                discovery_state VARCHAR(32),
                entry_price DOUBLE PRECISION NOT NULL,
                atr_at_signal DOUBLE PRECISION,
                reference_level DOUBLE PRECISION,
                stop_price DOUBLE PRECISION,
                target_price DOUBLE PRECISION,
                risk_per_share DOUBLE PRECISION,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (event_id, signal_time)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scanner_occurrences_time ON scanner_event_occurrences (signal_time DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scanner_occurrences_event ON scanner_event_occurrences (event_id, signal_time)")


def _event_key(scanner_name: str, scanner_version: str, interval: str,
               ticker: str, direction: int, setup_anchor: str) -> str:
    identity = "|".join([
        scanner_name, scanner_version, interval, ticker,
        str(direction), setup_anchor,
    ])
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return f"{scanner_name}:{interval}:{ticker}:{digest}"


def _signal_time(value, interval: str) -> datetime:
    timestamp = pd.Timestamp(value)
    if interval in DAILY_PRICE_INTERVALS:
        # Daily and completed weekly decisions are available after the 16:00 ET close.
        eastern = datetime.combine(
            timestamp.date(), time(16, 0), tzinfo=ZoneInfo("America/New_York")
        )
        return eastern.astimezone(timezone.utc)
    # Hourly loader returns naive Eastern wall-clock timestamps; Postgres receives UTC.
    return timestamp.tz_localize("America/New_York").tz_convert("UTC").to_pydatetime()


def _discovery_states(trade_date: date) -> dict[str, str]:
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT ticker, state FROM market_discovery_states
            WHERE trade_date = (
                SELECT MAX(trade_date) FROM market_discovery_states WHERE trade_date <= %s
            )
        """, (trade_date,))
        return {row["ticker"]: row["state"] for row in cur.fetchall()}


def _hourly_tickers() -> list[str]:
    """Only scan hourly bars for active discovery cohorts, never all 400 names."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT ticker FROM market_discovery_states
            WHERE trade_date = (SELECT MAX(trade_date) FROM market_discovery_states)
              AND state IN (
                  'CONTINUATION', 'REVERSAL_WATCH', 'EMERGING_REVERSAL',
                  'REVERSAL_CONFIRMED', 'CONFLICT', 'LAGGARD'
              )
            ORDER BY ticker
        """)
        return [row["ticker"] for row in cur.fetchall()]


def _canonical_capture_end(interval: str, selected: list[str], as_of: str | None) -> date:
    candidate = pd.Timestamp(as_of).date() if as_of else None
    with get_db_cursor() as cur:
        if interval in DAILY_PRICE_INTERVALS:
            if candidate is None:
                cur.execute("SELECT MAX(trade_date) AS d FROM market_discovery_states")
                row = cur.fetchone()
                if not row or not row["d"]:
                    raise ValueError("No complete market discovery snapshot is available")
                candidate = row["d"]
            if interval == "1d":
                return candidate
            if candidate.weekday() == 4:
                return candidate
            cur.execute(
                "SELECT MIN(datetime::date) AS d FROM stock_prices_daily "
                "WHERE datetime::date > %s",
                (candidate,),
            )
            next_session = cur.fetchone()["d"]
            if next_session and next_session.isocalendar()[:2] != candidate.isocalendar()[:2]:
                return candidate
            week_start = candidate - timedelta(days=candidate.weekday())
            cur.execute(
                "SELECT MAX(datetime::date) AS d FROM stock_prices_daily "
                "WHERE datetime::date < %s",
                (week_start,),
            )
            row = cur.fetchone()
            if not row or not row["d"]:
                raise ValueError("No completed weekly session is available")
            return row["d"]
        if candidate is not None:
            return candidate
        if interval == "1h":
            cur.execute("""
            SELECT (datetime AT TIME ZONE 'America/New_York')::date AS d,
                   COUNT(DISTINCT ticker) AS names
            FROM stock_prices_hourly
            WHERE ticker = ANY(%s)
            GROUP BY 1
            HAVING COUNT(DISTINCT ticker) >= CEIL(%s * 0.90)
            ORDER BY d DESC LIMIT 1
            """, (selected, len(selected)))
            row = cur.fetchone()
            if not row or not row["d"]:
                raise ValueError("No sufficiently complete hourly session is available")
            return row["d"]
    raise ValueError(f"Unsupported interval: {interval}")


def _resample_daily_panel(panel: pd.DataFrame, interval: str) -> pd.DataFrame:
    if panel.empty or interval == "1d":
        return panel
    if interval != "1wk":
        raise ValueError(f"Unsupported daily-derived interval: {interval}")
    work = panel.copy()
    work["period"] = pd.to_datetime(work["date"]).dt.to_period("W-FRI")
    return work.groupby(["ticker", "period"], observed=True, sort=False).agg(
        date=("date", "max"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index(drop=False).drop(columns="period").sort_values(
        ["ticker", "date"]
    ).reset_index(drop=True)


def _load_scanner_panel(interval: str, start: str, end: str,
                        tickers: list[str]) -> pd.DataFrame:
    if interval in DAILY_PRICE_INTERVALS:
        return _resample_daily_panel(
            load_daily_panel(start, end, tickers), interval
        )
    return load_hourly_panel(start, end, tickers)


def _composite_event_rows(events: pd.DataFrame, interval: str,
                          states: dict[str, str], capture_date: date,
                          allowed_states: set[str] | None = None) -> list[tuple]:
    if events.empty:
        return []
    current = events[pd.to_datetime(events["date"]).dt.date == capture_date]
    rows = []
    for _, event in current.iterrows():
        scanner_name = str(event["scanner_name"])
        scanner_version = str(event["scanner_version"])
        ticker = str(event["ticker"])
        if allowed_states is not None and states.get(ticker) not in allowed_states:
            continue
        direction = int(event["direction"])
        signal_time = _signal_time(event["date"], interval)
        entry = float(event["entry_price"])
        atr = _float_or_none(event["atr_at_signal"])
        reference = _float_or_none(event["reference_level"])
        stop = _float_or_none(event["stop_price"])
        target = _float_or_none(event["target_price"])
        risk = abs(entry - stop) if stop is not None else None
        setup_anchor = str(event["setup_anchor"])
        rows.append((
            _event_key(
                scanner_name, scanner_version, interval, ticker,
                direction, setup_anchor,
            ),
            scanner_name, scanner_version, interval, ticker, signal_time,
            signal_time, signal_time.astimezone(timezone.utc).date(), direction,
            str(event["trigger_type"]), states.get(ticker), VALIDATION_STATUS,
            entry, atr, reference, stop, target, risk, ROUND_TRIP_COST_BPS,
            str(event["metadata"]),
        ))
    return rows


def _capture_batch(panel: pd.DataFrame, interval: str, states: dict[str, str],
                   capture_date: date) -> list[tuple]:
    if panel.empty:
        return []
    events = build_all_scanner_events(panel, interval)
    return _composite_event_rows(events, interval, states, capture_date)


def _persist_event_rows(rows: list[tuple]) -> dict:
    """Upsert lifecycles and insert each observed timestamp exactly once."""
    if not rows:
        return {"matched": 0, "lifecycles": 0, "inserted": 0,
                "refreshed": 0, "occurrences_inserted": 0}
    rows_by_key: dict[str, list[tuple]] = {}
    for row in rows:
        rows_by_key.setdefault(row[0], []).append(row)
    lifecycle_rows = []
    for lifecycle in rows_by_key.values():
        first = list(min(lifecycle, key=lambda row: row[5]))
        first[6] = max(row[6] for row in lifecycle)
        lifecycle_rows.append(tuple(first))
    keys = list(rows_by_key)
    occurrence_rows_by_key = {(row[0], row[5]): row for row in rows}
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT event_key, event_id, signal_time
            FROM scanner_events WHERE event_key = ANY(%s)
        """, (keys,))
        existing_rows = {row["event_key"]: dict(row) for row in cur.fetchall()}
        existing = set(existing_rows)
        earlier_keys = {
            row[0] for row in lifecycle_rows
            if row[0] in existing_rows and row[5] < existing_rows[row[0]]["signal_time"]
        }
        cur.executemany("""
            INSERT INTO scanner_events (
                event_key, scanner_name, scanner_version, interval, ticker,
                signal_time, last_seen_at, trade_date, direction, trigger_type, discovery_state,
                validation_status, entry_price, atr_at_signal, reference_level,
                stop_price, target_price, risk_per_share, round_trip_cost_bps, metadata
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb
            ) ON CONFLICT (event_key) DO UPDATE SET
                signal_time = LEAST(scanner_events.signal_time, EXCLUDED.signal_time),
                last_seen_at = GREATEST(scanner_events.last_seen_at, EXCLUDED.last_seen_at),
                trade_date = LEAST(scanner_events.trade_date, EXCLUDED.trade_date),
                trigger_type = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.trigger_type ELSE scanner_events.trigger_type END,
                discovery_state = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.discovery_state ELSE scanner_events.discovery_state END,
                entry_price = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.entry_price ELSE scanner_events.entry_price END,
                atr_at_signal = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.atr_at_signal ELSE scanner_events.atr_at_signal END,
                reference_level = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.reference_level ELSE scanner_events.reference_level END,
                stop_price = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.stop_price ELSE scanner_events.stop_price END,
                target_price = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.target_price ELSE scanner_events.target_price END,
                risk_per_share = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.risk_per_share ELSE scanner_events.risk_per_share END,
                metadata = CASE WHEN EXCLUDED.signal_time <= scanner_events.signal_time
                    THEN EXCLUDED.metadata ELSE scanner_events.metadata END,
                occurrence_count = scanner_events.occurrence_count
        """, lifecycle_rows)
        cur.execute("SELECT event_key, event_id FROM scanner_events WHERE event_key = ANY(%s)", (keys,))
        event_ids = {row["event_key"]: row["event_id"] for row in cur.fetchall()}
        if earlier_keys:
            cur.execute("""
                DELETE FROM scanner_event_outcomes
                WHERE event_id = ANY(%s)
            """, ([event_ids[key] for key in earlier_keys],))
        occurrence_rows = [(
            event_ids[row[0]], row[5], row[7], row[9], row[10], row[12], row[13],
            row[14], row[15], row[16], row[17], row[19],
        ) for row in occurrence_rows_by_key.values()]
        occurrence_keys = [(row[0], row[1]) for row in occurrence_rows]
        cur.execute("""
            SELECT event_id, signal_time FROM scanner_event_occurrences
            WHERE event_id = ANY(%s)
        """, (list(event_ids.values()),))
        existing_occurrences = {
            (row["event_id"], row["signal_time"]) for row in cur.fetchall()
        }
        cur.executemany("""
            INSERT INTO scanner_event_occurrences (
                event_id, signal_time, trade_date, trigger_type, discovery_state,
                entry_price, atr_at_signal, reference_level, stop_price, target_price,
                risk_per_share, metadata
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            ON CONFLICT (event_id, signal_time) DO UPDATE SET
                discovery_state = EXCLUDED.discovery_state,
                trigger_type = EXCLUDED.trigger_type,
                entry_price = EXCLUDED.entry_price,
                atr_at_signal = EXCLUDED.atr_at_signal,
                reference_level = EXCLUDED.reference_level,
                stop_price = EXCLUDED.stop_price,
                target_price = EXCLUDED.target_price,
                risk_per_share = EXCLUDED.risk_per_share,
                metadata = EXCLUDED.metadata
        """, occurrence_rows)
        occurrences_inserted = len(set(occurrence_keys) - existing_occurrences)
        cur.execute("""
            UPDATE scanner_events e SET occurrence_count = counts.n
            FROM (
                SELECT event_id, COUNT(*)::integer AS n
                FROM scanner_event_occurrences
                WHERE event_id = ANY(%s)
                GROUP BY event_id
            ) counts
            WHERE e.event_id = counts.event_id
        """, (list(event_ids.values()),))
    return {
        "matched": len(occurrence_rows), "lifecycles": len(lifecycle_rows),
        "inserted": len(set(keys) - existing),
        "refreshed": len(set(keys) & existing),
        "occurrences_inserted": occurrences_inserted,
    }


def _refresh_existing_event_metadata(rows: list[tuple]) -> dict:
    """Update metadata only when a generated row matches a persisted event anchor."""
    if not rows:
        return {"matched": 0, "lifecycles": 0, "inserted": 0,
                "refreshed": 0, "occurrences_inserted": 0}
    generated = {(row[0], row[5]): row for row in rows}
    keys = sorted({key for key, _ in generated})
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT event_key, event_id, signal_time FROM scanner_events
            WHERE event_key = ANY(%s)
        """, (keys,))
        persisted = [dict(row) for row in cur.fetchall()]
        updates = []
        for event in persisted:
            row = generated.get((event["event_key"], event["signal_time"]))
            if row is not None:
                updates.append((row[19], event["event_id"], event["signal_time"]))
        cur.executemany("""
            UPDATE scanner_events SET metadata = %s::jsonb
            WHERE event_id = %s AND signal_time = %s
        """, updates)
        cur.executemany("""
            UPDATE scanner_event_occurrences SET metadata = %s::jsonb
            WHERE event_id = %s AND signal_time = %s
        """, updates)
    return {
        "matched": len(updates), "lifecycles": len(updates),
        "inserted": 0, "refreshed": len(updates),
        "occurrences_inserted": 0,
    }


def capture_events(interval: str, as_of: str | None = None,
                   tickers: list[str] | None = None) -> dict:
    """Capture latest-bar events only; repeated runs are idempotent."""
    if interval not in HORIZONS:
        raise ValueError(f"Unsupported interval: {interval}")
    ensure_tables()
    selected = tickers or (_hourly_tickers() if interval == "1h" else get_selected_tickers(True))
    if not selected:
        return {"scanned": 0, "matched": 0, "inserted": 0}

    end = _canonical_capture_end(interval, selected, as_of)
    start = end - pd.Timedelta(days=LOOKBACK_DAYS[interval])
    states = _discovery_states(end)
    all_rows: list[tuple] = []
    for offset in range(0, len(selected), BATCH_SIZE):
        batch = selected[offset:offset + BATCH_SIZE]
        if interval == "1d":
            panel = load_daily_panel(start.isoformat(), end.isoformat(), batch)
        elif interval == "1wk":
            panel = _load_scanner_panel(
                interval, start.isoformat(), end.isoformat(), batch
            )
        else:
            panel = load_hourly_panel(start.isoformat(), end.isoformat(), batch)
        all_rows.extend(_capture_batch(panel, interval, states, end))

    result = _persist_event_rows(all_rows)
    return {"scanned": len(selected), **result, "capture_date": str(end)}


def backfill_events(interval: str, sessions: int = 25,
                    tickers: list[str] | None = None) -> dict:
    """Replay the latest complete sessions using point-in-time detector output."""
    if interval not in HORIZONS:
        raise ValueError(f"Unsupported interval: {interval}")
    if sessions < 1:
        raise ValueError("sessions must be positive")
    ensure_tables()
    selected = tickers or (_hourly_tickers() if interval == "1h" else get_selected_tickers(True))
    if not selected:
        return {"scanned": 0, "sessions": 0, "matched": 0, "inserted": 0}
    end = _canonical_capture_end(interval, selected, None)
    with get_db_cursor() as cur:
        if interval == "1d":
            cur.execute("""
                SELECT datetime::date AS d
                FROM stock_prices_daily WHERE ticker = ANY(%s) AND datetime::date <= %s
                GROUP BY 1 HAVING COUNT(DISTINCT ticker) >= CEIL(%s * 0.90)
                ORDER BY d DESC LIMIT %s
            """, (selected, end, len(selected), sessions))
        elif interval == "1wk":
            cur.execute("""
                SELECT MAX(datetime::date) AS d
                FROM stock_prices_daily
                WHERE ticker = ANY(%s) AND datetime::date <= %s
                GROUP BY DATE_TRUNC('week', datetime)
                HAVING COUNT(DISTINCT ticker) >= CEIL(%s * 0.90)
                ORDER BY d DESC LIMIT %s
            """, (selected, end, len(selected), sessions))
        else:
            cur.execute("""
                SELECT (datetime AT TIME ZONE 'America/New_York')::date AS d
                FROM stock_prices_hourly WHERE ticker = ANY(%s)
                  AND (datetime AT TIME ZONE 'America/New_York')::date <= %s
                GROUP BY 1 HAVING COUNT(DISTINCT ticker) >= CEIL(%s * 0.90)
                ORDER BY d DESC LIMIT %s
            """, (selected, end, len(selected), sessions))
        session_dates = sorted(row["d"] for row in cur.fetchall())
    if not session_dates:
        return {"scanned": len(selected), "sessions": 0, "matched": 0, "inserted": 0}

    load_start = session_dates[0] - pd.Timedelta(days=LOOKBACK_DAYS[interval])
    states_by_date = {value: _discovery_states(value) for value in session_dates}
    totals = {"matched": 0, "lifecycles": 0, "inserted": 0,
              "refreshed": 0, "occurrences_inserted": 0}
    for offset in range(0, len(selected), BACKFILL_BATCH_SIZE):
        batch = selected[offset:offset + BACKFILL_BATCH_SIZE]
        panel = _load_scanner_panel(
            interval, load_start.isoformat(), end.isoformat(), batch
        )
        events = build_all_scanner_events(panel, interval)
        batch_rows: list[tuple] = []
        for session_date in session_dates:
            batch_rows.extend(_composite_event_rows(
                events, interval, states_by_date[session_date], session_date
            ))
        result = _persist_event_rows(batch_rows)
        for key in totals:
            totals[key] += result[key]
        del panel, events, batch_rows
        gc.collect()
    return {
        "scanned": len(selected), "sessions": len(session_dates),
        "first_session": str(session_dates[0]), "last_session": str(session_dates[-1]),
        **totals,
    }


def qualification_backfill(interval: str, start: str, end: str | None = None,
                           ticker_offset: int = 0,
                           ticker_limit: int | None = None,
                           scanner_names: set[str] | None = None,
                           batch_size: int = BACKFILL_BATCH_SIZE,
                           refresh_existing_only: bool = False) -> dict:
    """Replay a qualification window with point-in-time universe and discovery cohorts."""
    if interval not in HORIZONS:
        raise ValueError(f"Unsupported interval: {interval}")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    ensure_tables()
    start_date = pd.Timestamp(start).date()
    with get_db_cursor() as cur:
        cur.execute("SELECT MAX(datetime::date) AS d FROM stock_prices_daily")
        latest_daily = cur.fetchone()["d"]
    end_date = min(pd.Timestamp(end).date(), latest_daily) if end else latest_daily
    if start_date > end_date:
        raise ValueError("qualification start must be on or before end")

    with get_db_cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ticker FROM stock_prices_daily
            WHERE datetime::date <= %s ORDER BY ticker
        """, (end_date,))
        daily_universe = [row["ticker"] for row in cur.fetchall()]
        if interval == "1d":
            cur.execute("""
                SELECT datetime::date AS d
                FROM stock_prices_daily
                WHERE datetime::date BETWEEN %s AND %s
                GROUP BY 1 HAVING COUNT(DISTINCT ticker) >= %s
                ORDER BY d
            """, (start_date, end_date, max(50, int(len(daily_universe) * 0.90))))
        elif interval == "1wk":
            cur.execute("""
                SELECT MAX(datetime::date) AS d
                FROM stock_prices_daily
                WHERE datetime::date BETWEEN %s AND %s
                GROUP BY DATE_TRUNC('week', datetime)
                HAVING COUNT(DISTINCT ticker) >= %s
                ORDER BY d
            """, (start_date, end_date, max(50, int(len(daily_universe) * 0.90))))
        else:
            cur.execute("""
                SELECT (datetime AT TIME ZONE 'America/New_York')::date AS d
                FROM stock_prices_hourly
                WHERE ticker = ANY(%s)
                  AND (datetime AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
                GROUP BY 1 HAVING COUNT(DISTINCT ticker) >= %s
                ORDER BY d
            """, (
                daily_universe, start_date, end_date,
                max(50, int(len(daily_universe) * 0.90)),
            ))
        session_dates = [row["d"] for row in cur.fetchall()]
    if not session_dates:
        return {"scanned": 0, "sessions": 0, "matched": 0, "inserted": 0}

    with get_db_cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT trade_date) AS dates
            FROM market_discovery_states
            WHERE model_version = %s AND trade_date = ANY(%s)
        """, ("discovery-1.0-shadow", session_dates))
        persisted_dates = int(cur.fetchone()["dates"] or 0)
    if persisted_dates < len(session_dates):
        discovery_start = session_dates[0] - pd.Timedelta(days=800)
        daily_panel = load_daily_panel(
            discovery_start.isoformat(), session_dates[-1].isoformat(), daily_universe
        )
        historical_states = classify_discovery_history(daily_panel, session_dates)
        _persist_historical_discovery_states(historical_states)
        del daily_panel, historical_states
        gc.collect()
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT trade_date, ticker, state FROM market_discovery_states
            WHERE model_version = %s AND trade_date = ANY(%s)
        """, ("discovery-1.0-shadow", session_dates))
        state_rows = cur.fetchall()
    historical_states = pd.DataFrame([dict(row) for row in state_rows])
    states_by_date = {
        value: dict(zip(group["ticker"], group["state"]))
        for value, group in historical_states.groupby("trade_date", sort=False)
    }
    del historical_states
    gc.collect()

    existing_event_keys: set[str] | None = None
    if refresh_existing_only:
        clauses = ["interval = %s"]
        params: list = [interval]
        if scanner_names:
            clauses.append("scanner_name = ANY(%s)")
            params.append(sorted(scanner_names))
        with get_db_cursor() as cur:
            cur.execute(f"""
                SELECT event_key, ticker FROM scanner_events
                WHERE {' AND '.join(clauses)}
            """, params)
            existing_rows = cur.fetchall()
        existing_event_keys = {row["event_key"] for row in existing_rows}
        selected = sorted({row["ticker"] for row in existing_rows})
    elif interval == "1h":
        selected = sorted({
            ticker for states in states_by_date.values()
            for ticker, state in states.items() if state in ACTIVE_DISCOVERY_STATES
        })
    else:
        selected = daily_universe
    universe_size = len(selected)
    selected = selected[ticker_offset:]
    if ticker_limit is not None:
        selected = selected[:ticker_limit]
    load_start = session_dates[0] - pd.Timedelta(days=LOOKBACK_DAYS[interval])
    totals = {"matched": 0, "lifecycles": 0, "inserted": 0,
              "refreshed": 0, "occurrences_inserted": 0}
    for offset in range(0, len(selected), batch_size):
        batch = selected[offset:offset + batch_size]
        panel = _load_scanner_panel(
            interval, load_start.isoformat(), end_date.isoformat(), batch
        )
        events = build_all_scanner_events(panel, interval)
        if scanner_names:
            events = events[events["scanner_name"].isin(scanner_names)]
        batch_rows: list[tuple] = []
        for session_date in session_dates:
            batch_rows.extend(_composite_event_rows(
                events, interval, states_by_date.get(session_date, {}), session_date,
                ACTIVE_DISCOVERY_STATES
                if interval == "1h" and not refresh_existing_only else None,
            ))
        if existing_event_keys is not None:
            batch_rows = [
                row for row in batch_rows if row[0] in existing_event_keys
            ]
        result = (
            _refresh_existing_event_metadata(batch_rows)
            if refresh_existing_only else _persist_event_rows(batch_rows)
        )
        for key in totals:
            totals[key] += result[key]
        del panel, events, batch_rows
        gc.collect()
    return {
        "scanned": len(selected), "sessions": len(session_dates),
        "universe_size": universe_size, "ticker_offset": ticker_offset,
        "scanner_names": sorted(scanner_names) if scanner_names else "all",
        "refresh_existing_only": refresh_existing_only,
        "first_session": str(session_dates[0]), "last_session": str(session_dates[-1]),
        "universe_method": "point_in_time_data_availability",
        "discovery_method": "reconstructed_point_in_time",
        **totals,
    }


def _persist_historical_discovery_states(states: pd.DataFrame) -> int:
    """Persist reconstructed state labels needed by scanner qualification."""
    if states.empty:
        return 0
    sql = """
        INSERT INTO market_discovery_states (
            trade_date, ticker, model_version, state, validation_status,
            activity_percentile, echo_percentile, older_momentum_percentile,
            long_momentum_percentile, recent_21d_percentile, recent_21d_return,
            recent_5d_return, close_price, sma_20, sma_50,
            higher_swing_high, higher_swing_low, evidence
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb
        ) ON CONFLICT (trade_date, ticker, model_version) DO UPDATE SET
            state = EXCLUDED.state,
            validation_status = EXCLUDED.validation_status,
            evidence = EXCLUDED.evidence
    """
    inserted = 0
    for offset in range(0, len(states), 5000):
        rows = [(
            row["trade_date"], row["ticker"], row["model_version"], row["state"],
            row["validation_status"], _float_or_none(row["activity_percentile"]),
            _float_or_none(row["echo_percentile"]),
            _float_or_none(row["older_momentum_percentile"]),
            _float_or_none(row["long_momentum_percentile"]),
            _float_or_none(row["recent_21d_percentile"]),
            _float_or_none(row["recent_21d_return"]),
            _float_or_none(row["recent_5d_return"]), _float_or_none(row["close"]),
            _float_or_none(row["sma20"]), _float_or_none(row["sma50"]),
            bool(row["higher_swing_high"]) if pd.notna(row["higher_swing_high"]) else None,
            bool(row["higher_swing_low"]) if pd.notna(row["higher_swing_low"]) else None,
            row["evidence"],
        ) for _, row in states.iloc[offset:offset + 5000].iterrows()]
        with get_db_cursor() as cur:
            cur.executemany(sql, rows)
        inserted += len(rows)
        del rows
        gc.collect()
    return inserted


def _due_events(interval: str, limit: int = 5000) -> list[dict]:
    horizons = list(HORIZONS[interval])
    table = "stock_prices_daily" if interval in DAILY_PRICE_INTERVALS else "stock_prices_hourly"
    timestamp_expr = ("p.datetime AT TIME ZONE 'UTC'" if interval in DAILY_PRICE_INTERVALS
                      else "p.datetime")
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT e.event_id, e.ticker, e.interval, e.signal_time, e.direction,
                   e.entry_price, e.stop_price, e.target_price, e.risk_per_share,
                   e.round_trip_cost_bps, h.horizon_bars
            FROM scanner_events e
            CROSS JOIN UNNEST(%s::smallint[]) AS h(horizon_bars)
            LEFT JOIN scanner_event_outcomes o
              ON o.event_id = e.event_id AND o.horizon_bars = h.horizon_bars
            WHERE e.interval = %s AND o.outcome_id IS NULL
              AND (
                SELECT COUNT(*) FROM {table} p
                WHERE p.ticker = e.ticker AND {timestamp_expr} > e.signal_time
              ) >= h.horizon_bars
            ORDER BY e.signal_time, e.event_id, h.horizon_bars
            LIMIT %s
        """, (horizons, interval, limit))
        return [dict(row) for row in cur.fetchall()]


def _bars_for_event(event: dict) -> pd.DataFrame:
    interval = event["interval"]
    limit = int(event["horizon_bars"])
    table = "stock_prices_daily" if interval in DAILY_PRICE_INTERVALS else "stock_prices_hourly"
    timestamp_expr = ("datetime AT TIME ZONE 'UTC'"
                      if interval in DAILY_PRICE_INTERVALS else "datetime")
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT {timestamp_expr} AS bar_time, open_price AS open, high, low,
                   close_price AS close
            FROM {table}
            WHERE ticker = %s AND {timestamp_expr} > %s
            ORDER BY datetime LIMIT %s
        """, (event["ticker"], event["signal_time"], limit))
        bars = [dict(row) for row in cur.fetchall()]
    frame = pd.DataFrame(bars)
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _benchmark_return(interval: str, signal_time: datetime, horizon: int) -> float | None:
    table = "stock_prices_daily" if interval in DAILY_PRICE_INTERVALS else "stock_prices_hourly"
    timestamp_expr = ("datetime AT TIME ZONE 'UTC'"
                      if interval in DAILY_PRICE_INTERVALS else "datetime")
    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute(f"""
            WITH future AS (
                SELECT ticker, open_price, close_price,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY datetime) AS rn
                FROM {table} WHERE {timestamp_expr} > %s
            ), entry AS (
                SELECT ticker, open_price FROM future WHERE rn = 1
            ), exit AS (
                SELECT ticker, close_price FROM future WHERE rn = %s
            )
            SELECT AVG(exit.close_price / NULLIF(entry.open_price, 0) - 1)
            FROM entry JOIN exit USING (ticker)
        """, (signal_time, horizon))
        value = cur.fetchone()[0]
    return float(value) if value is not None else None


def _batched_benchmark_returns(interval: str, due: list[dict]) -> dict[tuple, float | None]:
    """Compute all due benchmark returns in one interval-wide window-function pass."""
    if not due:
        return {}
    signal_times = sorted({event["signal_time"] for event in due})
    benchmark_keys = (
        sorted({value.date() for value in signal_times})
        if interval in DAILY_PRICE_INTERVALS else signal_times
    )
    horizons = sorted({int(event["horizon_bars"]) for event in due})
    lead_columns = ",\n".join([
        "LEAD(open_price, 1) OVER "
        "(PARTITION BY ticker ORDER BY datetime) AS next_open",
        *[
            f"LEAD(close_price, {horizon}) OVER "
            f"(PARTITION BY ticker ORDER BY datetime) AS exit_{horizon}"
            for horizon in horizons
        ],
    ])
    return_columns = ",\n".join(
        f"AVG(exit_{horizon} / NULLIF(next_open, 0) - 1) AS return_{horizon}"
        for horizon in horizons
    )
    start = min(signal_times)
    end = max(signal_times) + pd.Timedelta(
        days=45 if interval in DAILY_PRICE_INTERVALS else 14
    )
    table = "stock_prices_daily" if interval in DAILY_PRICE_INTERVALS else "stock_prices_hourly"
    timestamp_expr = ("datetime AT TIME ZONE 'UTC'"
                      if interval in DAILY_PRICE_INTERVALS else "datetime")
    group_expr = (f"({timestamp_expr})::date"
                  if interval in DAILY_PRICE_INTERVALS else timestamp_expr)
    with get_db_cursor() as cur:
        cur.execute(f"""
            WITH bars AS (
                  SELECT {group_expr} AS signal_key, ticker,
                       {lead_columns}
                FROM {table}
                WHERE {timestamp_expr} BETWEEN %s AND %s
            )
            SELECT signal_key, {return_columns}
            FROM bars
            WHERE signal_key = ANY(%s)
            GROUP BY signal_key
        """, (start, end, benchmark_keys))
        rows = cur.fetchall()
    result: dict[tuple, float | None] = {}
    for row in rows:
        for horizon in horizons:
            value = row[f"return_{horizon}"]
            key_time = row["signal_key"] if interval == "1d" else row["signal_key"]
            if interval in DAILY_PRICE_INTERVALS:
                matching_signal = next(
                    value for value in signal_times if value.date() == key_time
                )
            else:
                matching_signal = key_time
            result[(interval, matching_signal, horizon)] = (
                float(value) if value is not None else None
            )
    return result


def _batched_forward_bars(interval: str, due: list[dict]) -> dict[str, pd.DataFrame]:
    """Load the replay evaluation window once per ticker and interval."""
    if not due:
        return {}
    tickers = sorted({event["ticker"] for event in due})
    start = min(event["signal_time"] for event in due)
    table = "stock_prices_daily" if interval in DAILY_PRICE_INTERVALS else "stock_prices_hourly"
    timestamp_expr = ("datetime AT TIME ZONE 'UTC'"
                      if interval in DAILY_PRICE_INTERVALS else "datetime")
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT ticker, {timestamp_expr} AS bar_time, open_price AS open,
                   high, low, close_price AS close
            FROM {table}
            WHERE ticker = ANY(%s) AND {timestamp_expr} > %s
            ORDER BY ticker, datetime
        """, (tickers, start))
        frame = pd.DataFrame([dict(row) for row in cur.fetchall()])
    if frame.empty:
        return {}
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["bar_time"] = pd.to_datetime(frame["bar_time"], utc=True)
    return {
        str(ticker): group.reset_index(drop=True)
        for ticker, group in frame.groupby("ticker", sort=False)
    }


def _first_hit(bars: pd.DataFrame, direction: int,
               stop: float | None, target: float | None) -> tuple[bool, bool, str]:
    stop_hit = target_hit = False
    first = "NONE"
    if stop is None or target is None:
        return stop_hit, target_hit, first
    for _, bar in bars.iterrows():
        bar_stop = bar["low"] <= stop if direction == 1 else bar["high"] >= stop
        bar_target = bar["high"] >= target if direction == 1 else bar["low"] <= target
        stop_hit |= bool(bar_stop)
        target_hit |= bool(bar_target)
        if first == "NONE" and (bar_stop or bar_target):
            first = "SAME_BAR" if bar_stop and bar_target else "STOP" if bar_stop else "TARGET"
    return stop_hit, target_hit, first


def evaluate_outcomes(interval: str, limit: int = 5000) -> dict:
    """Evaluate only due event/horizon pairs; previously stored outcomes are untouched."""
    ensure_tables()
    due = _due_events(interval, limit=limit)
    if not due:
        return {"due": 0, "inserted": 0}
    benchmark_cache = _batched_benchmark_returns(interval, due)
    batched_bars = _batched_forward_bars(interval, due)
    rows = []
    for event in due:
        ticker_bars = batched_bars.get(str(event["ticker"]))
        signal_time = pd.Timestamp(event["signal_time"])
        if ticker_bars is None:
            continue
        bars = ticker_bars[ticker_bars["bar_time"] > signal_time].head(
            int(event["horizon_bars"])
        )
        if len(bars) < event["horizon_bars"]:
            continue
        direction = int(event["direction"])
        entry = float(bars.iloc[0]["open"])
        if not np.isfinite(entry) or entry <= 0:
            continue
        raw_return = float(bars.iloc[-1]["close"] / entry - 1)
        signed = direction * raw_return
        cost = float(event["round_trip_cost_bps"] or 0) / 10_000.0
        key = (interval, event["signal_time"], int(event["horizon_bars"]))
        if key not in benchmark_cache or benchmark_cache[key] is None:
            benchmark_cache[key] = _benchmark_return(*key)
        benchmark = benchmark_cache[key]
        if benchmark is None or not np.isfinite(benchmark):
            continue
        signed_benchmark = direction * benchmark if benchmark is not None else None
        alpha = signed - signed_benchmark if signed_benchmark is not None else None
        net_signed = signed - cost
        net_alpha = alpha - cost if alpha is not None else None
        favorable = ((bars["high"].max() / entry - 1) if direction == 1
                     else (1 - bars["low"].min() / entry))
        adverse = ((bars["low"].min() / entry - 1) if direction == 1
                   else (1 - bars["high"].max() / entry))
        stop = _float_or_none(event["stop_price"])
        target = _float_or_none(event["target_price"])
        risk_pct = abs(entry - stop) / entry if stop is not None and entry else None
        mae_r = adverse / risk_pct if risk_pct else None
        mfe_r = favorable / risk_pct if risk_pct else None
        stop_hit, target_hit, first = _first_hit(
            bars, direction, stop, target
        )
        rows.append((
            event["event_id"], event["horizon_bars"], len(bars),
            bars.iloc[-1]["bar_time"], float(bars.iloc[-1]["close"]),
            raw_return, signed, net_signed, benchmark, alpha, net_alpha,
            adverse, favorable, mae_r, mfe_r, stop_hit, target_hit, first,
            bars.iloc[0]["bar_time"], entry, OUTCOME_ENTRY_MODEL,
        ))
    inserted = 0
    if rows:
        with get_db_cursor() as cur:
            cur.executemany("""
                INSERT INTO scanner_event_outcomes (
                    event_id, horizon_bars, bars_observed, exit_time, exit_price,
                    raw_return, signed_return, net_signed_return, benchmark_return,
                    alpha_return, net_alpha_return, mae_pct, mfe_pct, mae_r, mfe_r,
                    stop_hit, target_hit, first_hit, entry_time, entry_price, entry_model
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id, horizon_bars) DO NOTHING
            """, rows)
            inserted = cur.rowcount
    return {"due": len(due), "inserted": inserted}


def reset_stale_outcomes(interval: str) -> dict:
    """Delete derived outcomes computed under an older execution model."""
    if interval not in HORIZONS:
        raise ValueError(f"Unsupported interval: {interval}")
    ensure_tables()
    with get_db_cursor() as cur:
        cur.execute("""
            DELETE FROM scanner_event_outcomes o
            USING scanner_events e
            WHERE o.event_id = e.event_id AND e.interval = %s
                            AND (
                                    o.entry_model <> %s OR o.benchmark_return IS NULL
                                    OR o.entry_price IS NULL OR o.entry_price <= 0
                            )
        """, (interval, OUTCOME_ENTRY_MODEL))
        deleted = cur.rowcount
    return {"deleted": deleted, "entry_model": OUTCOME_ENTRY_MODEL}


def event_summary(interval: str | None = None, discovery_state: str | None = None,
                  min_independent_periods: int = 20) -> list[dict]:
    """Portfolio metrics using horizon-spaced signal timestamps as observations."""
    clauses = []
    params: list = []
    if interval:
        clauses.append("e.interval = %s")
        params.append(interval)
    if discovery_state:
        clauses.append("e.discovery_state = %s")
        params.append(discovery_state)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    with get_db_cursor() as cur:
        cur.execute(f"""
            WITH per_time AS (
                SELECT e.scanner_name, e.scanner_version, e.interval,
                       e.discovery_state, e.direction, o.horizon_bars,
                       e.signal_time, e.trade_date,
                       AVG(o.net_signed_return) AS net_return,
                       AVG(o.net_alpha_return) AS net_alpha,
                       AVG(o.mae_pct) AS mae_pct,
                       AVG(o.mfe_pct) AS mfe_pct,
                       AVG(o.mae_r) AS mae_r,
                       AVG(o.mfe_r) AS mfe_r,
                       AVG(CASE WHEN o.first_hit = 'STOP' THEN 1.0 ELSE 0.0 END) AS stop_first,
                       AVG(CASE WHEN o.first_hit = 'TARGET' THEN 1.0 ELSE 0.0 END) AS target_first,
                       COUNT(*) AS names
                FROM scanner_events e
                JOIN scanner_event_outcomes o ON o.event_id = e.event_id
                {where}
                GROUP BY e.scanner_name, e.scanner_version, e.interval,
                         e.discovery_state, e.direction, o.horizon_bars,
                         e.signal_time, e.trade_date
            )
            SELECT * FROM per_time
            ORDER BY interval, horizon_bars, discovery_state, direction, signal_time
        """, params)
        observations = pd.DataFrame([dict(row) for row in cur.fetchall()])
        if observations.empty:
            return []
        cur.execute("""
            SELECT DISTINCT datetime::date AS bar_date
            FROM stock_prices_daily ORDER BY bar_date
        """)
        daily_calendar = {row["bar_date"]: index for index, row in enumerate(cur.fetchall())}
        cur.execute("""
            SELECT DISTINCT datetime AS bar_time
            FROM stock_prices_hourly ORDER BY bar_time
        """)
        hourly_calendar = {row["bar_time"]: index for index, row in enumerate(cur.fetchall())}

    group_columns = [
        "scanner_name", "scanner_version", "interval", "discovery_state",
        "direction", "horizon_bars",
    ]
    results = []
    for key, group in observations.groupby(group_columns, dropna=False, sort=True):
        group = group.sort_values("signal_time")
        horizon = int(key[-1])
        selected_indices = []
        last_ordinal = -horizon
        for row_index, row in group.iterrows():
            if row["interval"] in DAILY_PRICE_INTERVALS:
                ordinal = daily_calendar.get(row["trade_date"])
            else:
                signal_time = pd.Timestamp(row["signal_time"])
                if signal_time.tzinfo is not None:
                    signal_time = signal_time.tz_convert("UTC").to_pydatetime()
                ordinal = hourly_calendar.get(signal_time)
            if ordinal is not None and ordinal - last_ordinal >= horizon:
                selected_indices.append(row_index)
                last_ordinal = ordinal
        sample = group.loc[selected_indices]
        periods = len(sample)
        if periods < min_independent_periods:
            continue
        alpha = pd.to_numeric(sample["net_alpha"], errors="coerce").dropna()
        alpha_t = (float(alpha.mean() / (alpha.std() / np.sqrt(len(alpha))))
                   if len(alpha) > 1 and alpha.std() else None)
        mean_alpha = float(alpha.mean()) if len(alpha) else None
        status = _promotion_status(periods, mean_alpha, alpha_t)
        results.append({
            **dict(zip(group_columns, key)),
            "independent_periods": periods,
            "events": int(pd.to_numeric(sample["names"]).sum()),
            "mean_net_return": _mean(sample["net_return"]),
            "mean_net_alpha": mean_alpha,
            "alpha_t_stat": alpha_t,
            "hit_rate": float((pd.to_numeric(sample["net_return"]) > 0).mean()),
            "mean_mae_pct": _mean(sample["mae_pct"]),
            "mean_mfe_pct": _mean(sample["mfe_pct"]),
            "mean_mae_r": _mean(sample["mae_r"]),
            "mean_mfe_r": _mean(sample["mfe_r"]),
            "stop_first_rate": _mean(sample["stop_first"]),
            "target_first_rate": _mean(sample["target_first"]),
            "promotion_status": status,
        })
    return results


def _review_priority(interval: str, direction: int,
                     discovery_state: str | None) -> tuple[str, list[str]]:
    if interval != "1h":
        return "UNRANKED", [
            "Review priority is not qualified for daily or weekly signals."
        ]

    state = discovery_state or "NEUTRAL"
    aligned_states = (
        {"CONTINUATION", "EMERGING_REVERSAL", "REVERSAL_CONFIRMED"}
        if direction == 1 else {"CONFLICT", "LAGGARD"}
    )
    opposed_states = (
        {"CONFLICT", "LAGGARD"}
        if direction == 1 else {
            "CONTINUATION", "REVERSAL_WATCH", "EMERGING_REVERSAL",
            "REVERSAL_CONFIRMED",
        }
    )
    if state in aligned_states:
        return "HIGHER", [
            f"Hourly {('long' if direction == 1 else 'short')} direction aligns "
            f"with the {state.lower().replace('_', ' ')} discovery state."
        ]
    if state in opposed_states:
        return "LOWER", [
            f"Hourly {('long' if direction == 1 else 'short')} direction opposes "
            f"the {state.lower().replace('_', ' ')} discovery state."
        ]
    return "STANDARD", [
        "Hourly direction has neutral or inconclusive discovery-state context."
    ]


def qualification_summary(interval: str) -> list[dict]:
    """Aggregate promotion report across states using horizon-spaced portfolios."""
    if interval not in HORIZONS:
        raise ValueError(f"Unsupported interval: {interval}")
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT e.scanner_name, e.scanner_version, e.interval, e.direction,
                   o.horizon_bars, e.signal_time, e.trade_date,
                   AVG(o.net_signed_return) AS net_return,
                   AVG(o.net_alpha_return) AS net_alpha,
                   AVG(o.mae_pct) AS mae_pct, AVG(o.mfe_pct) AS mfe_pct,
                   AVG(o.mae_r) AS mae_r, AVG(o.mfe_r) AS mfe_r,
                   AVG(CASE WHEN o.first_hit = 'STOP' THEN 1.0 ELSE 0.0 END) AS stop_first,
                   AVG(CASE WHEN o.first_hit = 'TARGET' THEN 1.0 ELSE 0.0 END) AS target_first,
                   COUNT(*) AS names
            FROM scanner_events e
            JOIN scanner_event_outcomes o ON o.event_id = e.event_id
            WHERE e.interval = %s
            GROUP BY e.scanner_name, e.scanner_version, e.interval, e.direction,
                     o.horizon_bars, e.signal_time, e.trade_date
            ORDER BY e.scanner_name, e.direction, o.horizon_bars, e.signal_time
        """, (interval,))
        observations = pd.DataFrame([dict(row) for row in cur.fetchall()])
        if observations.empty:
            return []
        if interval in DAILY_PRICE_INTERVALS:
            cur.execute("""
                SELECT DISTINCT datetime::date AS bar_key
                FROM stock_prices_daily ORDER BY bar_key
            """)
        else:
            cur.execute("""
                SELECT DISTINCT datetime AS bar_key
                FROM stock_prices_hourly ORDER BY bar_key
            """)
        calendar = {row["bar_key"]: index for index, row in enumerate(cur.fetchall())}

    group_columns = [
        "scanner_name", "scanner_version", "interval", "direction", "horizon_bars",
    ]
    results = []
    for key, group in observations.groupby(group_columns, sort=True):
        horizon = int(key[-1])
        selected_indices = []
        last_ordinal = -horizon
        for row_index, row in group.sort_values("signal_time").iterrows():
            if interval in DAILY_PRICE_INTERVALS:
                bar_key = row["trade_date"]
            else:
                bar_key = pd.Timestamp(row["signal_time"])
                if bar_key.tzinfo is not None:
                    bar_key = bar_key.tz_convert("UTC").to_pydatetime()
            ordinal = calendar.get(bar_key)
            if ordinal is not None and ordinal - last_ordinal >= horizon:
                selected_indices.append(row_index)
                last_ordinal = ordinal
        sample = group.loc[selected_indices].sort_values("signal_time")
        periods = len(sample)
        alpha = pd.to_numeric(sample["net_alpha"], errors="coerce").dropna()
        alpha_t = (
            float(alpha.mean() / (alpha.std(ddof=1) / np.sqrt(len(alpha))))
            if len(alpha) > 1 and alpha.std(ddof=1) > 0 else None
        )
        alpha_standard_error = (
            float(alpha.std(ddof=1) / np.sqrt(len(alpha)))
            if len(alpha) > 1 and alpha.std(ddof=1) > 0 else None
        )
        midpoint = max(1, periods // 2)
        early_alpha = _mean(sample.iloc[:midpoint]["net_alpha"])
        late_alpha = _mean(sample.iloc[midpoint:]["net_alpha"])
        mean_alpha = _mean(alpha)
        events = int(pd.to_numeric(group["names"]).sum())
        hit_rate = float((pd.to_numeric(sample["net_return"]) > 0).mean()) \
            if periods else None
        if periods and hit_rate is not None:
            z = 1.96
            denominator = 1 + z * z / periods
            center = (hit_rate + z * z / (2 * periods)) / denominator
            radius = z * np.sqrt(
                hit_rate * (1 - hit_rate) / periods
                + z * z / (4 * periods * periods)
            ) / denominator
            hit_rate_ci_low = float(max(0.0, center - radius))
            hit_rate_ci_high = float(min(1.0, center + radius))
        else:
            hit_rate_ci_low = hit_rate_ci_high = None
        qualified = (
            events >= 100 and periods >= 40
            and mean_alpha is not None and mean_alpha > 0
            and alpha_t is not None and alpha_t > 2
            and early_alpha is not None and early_alpha > 0
            and late_alpha is not None and late_alpha > 0
        )
        calibration = walk_forward_calibration(
            sample["net_return"], sample["net_alpha"]
        )
        results.append({
            "scanner_name": str(key[0]),
            "scanner_version": str(key[1]),
            "interval": str(key[2]),
            "direction": int(key[3]),
            "horizon_bars": int(key[4]),
            "events": events,
            "independent_periods": periods,
            "mean_net_return": _mean(sample["net_return"]),
            "mean_net_alpha": mean_alpha,
            "alpha_t_stat": alpha_t,
            "alpha_p_value": _normal_p_value(alpha_t),
            "alpha_fdr_q": None,
            "alpha_ci_low": mean_alpha - 1.96 * alpha_standard_error
                if mean_alpha is not None and alpha_standard_error is not None else None,
            "alpha_ci_high": mean_alpha + 1.96 * alpha_standard_error
                if mean_alpha is not None and alpha_standard_error is not None else None,
            "early_alpha": early_alpha,
            "late_alpha": late_alpha,
            "hit_rate": hit_rate,
            "hit_rate_ci_low": hit_rate_ci_low,
            "hit_rate_ci_high": hit_rate_ci_high,
            "mean_mae_pct": _mean(sample["mae_pct"]),
            "mean_mfe_pct": _mean(sample["mfe_pct"]),
            "stop_first_rate": _mean(sample["stop_first"]),
            "target_first_rate": _mean(sample["target_first"]),
            "qualification_status": "PRIMARY_PASS" if qualified else "NOT_QUALIFIED",
            "evidence_status": "MONITOR_ONLY" if qualified else "UNRANKED",
            "calibration_status": "NOT_ELIGIBLE",
            **calibration,
        })
    return results


def _apply_qualification_fdr(rows: list[dict]) -> list[dict]:
    """Correct the full primary scanner family and assign evidence states."""
    if not rows:
        return rows
    report = pd.DataFrame(rows)
    report["alpha_fdr_q"] = _benjamini_hochberg(report["alpha_p_value"])
    robust = (
        (report["qualification_status"] == "PRIMARY_PASS")
        & (report["alpha_fdr_q"] <= 0.05)
    )
    report["evidence_status"] = "UNRANKED"
    report.loc[
        report["qualification_status"] == "PRIMARY_PASS", "evidence_status"
    ] = "MONITOR_ONLY"
    report.loc[robust, "evidence_status"] = "ROBUST_PASS"
    report["calibration_status"] = "NOT_ELIGIBLE"
    enough_oos = pd.to_numeric(
        report.get("calibration_oos_periods"), errors="coerce"
    ) >= 100
    brier_pass = pd.to_numeric(
        report.get("brier_score"), errors="coerce"
    ) < 0.25
    calibration_pass = pd.to_numeric(
        report.get("expected_calibration_error"), errors="coerce"
    ) <= 0.05
    report.loc[robust, "calibration_status"] = "FAILED_DIAGNOSTICS"
    report.loc[
        robust & enough_oos & brier_pass & calibration_pass,
        "calibration_status",
    ] = "RESEARCH_CALIBRATED"
    unsupported = ~robust
    for column in (
        "calibrated_win_probability",
        "calibrated_win_probability_ci_low",
        "calibrated_win_probability_ci_high",
        "live_expected_alpha",
        "live_expected_alpha_ci_low",
        "live_expected_alpha_ci_high",
    ):
        report.loc[unsupported, column] = None
    return report.replace({np.nan: None}).to_dict(orient="records")


def qualification_report(interval: str | None = None) -> list[dict]:
    """Return globally FDR-corrected qualification rows, optionally filtered by interval."""
    rows = [
        row for value in HORIZONS
        for row in qualification_summary(value)
    ]
    corrected = _apply_qualification_fdr(rows)
    return [row for row in corrected if interval is None or row["interval"] == interval]


def _mean(values) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.mean()) if len(numeric) else None


def _promotion_status(periods: int, mean_alpha: float | None,
                      alpha_t: float | None) -> str:
    if periods < 20:
        return "COLLECTING"
    if periods < 100:
        return "INSUFFICIENT_SAMPLE"
    if mean_alpha is None or mean_alpha <= 0 or alpha_t is None or alpha_t < 2:
        return "FAILED"
    # Chronological/sector stability is a separate promotion gate.
    return "PROMISING"


def pending_outcome_counts() -> list[dict]:
    """Operational backlog by interval and horizon, including not-yet-due pairs."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT e.interval, h.horizon_bars,
                   COUNT(*) FILTER (WHERE o.outcome_id IS NULL) AS pending,
                   COUNT(*) FILTER (WHERE o.outcome_id IS NOT NULL) AS evaluated
            FROM scanner_events e
            CROSS JOIN LATERAL UNNEST(
                 CASE WHEN e.interval IN ('1d', '1wk')
                     THEN ARRAY[5,10,21]::smallint[]
                     ELSE ARRAY[7,21,35]::smallint[] END
            ) h(horizon_bars)
            LEFT JOIN scanner_event_outcomes o
              ON o.event_id = e.event_id AND o.horizon_bars = h.horizon_bars
            GROUP BY e.interval, h.horizon_bars
            ORDER BY e.interval, h.horizon_bars
        """)
        return [dict(row) for row in cur.fetchall()]


def recent_events(interval: str | None = None, limit: int = 100) -> list[dict]:
    """Latest captured event lifecycles with all completed outcomes."""
    clauses = []
    params: list = []
    if interval:
        clauses.append("e.interval = %s")
        params.append(interval)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(limit)
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT e.event_id, e.scanner_name, e.scanner_version, e.interval,
                   e.ticker, e.signal_time, e.last_seen_at, e.occurrence_count,
                   e.direction, e.trigger_type, e.discovery_state,
                   e.validation_status,
                                     CASE WHEN e.interval = '1d' THEN (
                       SELECT p.open_price FROM stock_prices_daily p
                       WHERE p.ticker = e.ticker AND p.datetime::date = e.trade_date
                       ORDER BY p.datetime LIMIT 1
                                     ) WHEN e.interval = '1wk' THEN (
                                             SELECT p.open_price FROM stock_prices_daily p
                                             WHERE p.ticker = e.ticker
                                                 AND DATE_TRUNC('week', p.datetime) = DATE_TRUNC('week', e.trade_date::timestamp)
                                             ORDER BY p.datetime LIMIT 1
                   ) ELSE (
                       SELECT p.open_price FROM stock_prices_hourly p
                       WHERE p.ticker = e.ticker AND p.datetime = e.signal_time
                       ORDER BY p.datetime LIMIT 1
                   ) END AS signal_open_price,
                   e.entry_price, e.atr_at_signal,
                   e.reference_level, e.stop_price, e.target_price,
                   e.risk_per_share, e.metadata,
                   COALESCE(jsonb_agg(
                       jsonb_build_object(
                           'horizon_bars', o.horizon_bars,
                           'entry_time', o.entry_time,
                           'entry_price', o.entry_price,
                           'entry_model', o.entry_model,
                           'exit_time', o.exit_time,
                           'net_signed_return', o.net_signed_return,
                           'net_alpha_return', o.net_alpha_return,
                           'mae_pct', o.mae_pct, 'mfe_pct', o.mfe_pct,
                           'mae_r', o.mae_r, 'mfe_r', o.mfe_r,
                           'first_hit', o.first_hit
                       ) ORDER BY o.horizon_bars
                   ) FILTER (WHERE o.outcome_id IS NOT NULL), '[]'::jsonb) AS outcomes
            FROM scanner_events e
            LEFT JOIN scanner_event_outcomes o ON o.event_id = e.event_id
            {where}
            GROUP BY e.event_id
            ORDER BY e.signal_time DESC
            LIMIT %s
        """, params)
        return [dict(row) for row in cur.fetchall()]


def latest_ticker_signals(interval: str | None = None, limit: int = 500) -> list[dict]:
    """Latest observed scanner occurrence for each ticker."""
    interval_clause = "AND e.interval = %s" if interval else ""
    params: list = [interval] if interval else []
    params.append(limit)
    with get_db_cursor() as cur:
        cur.execute(f"""
            WITH ranked AS (
                SELECT e.event_id, e.scanner_name, e.scanner_version, e.interval,
                       e.ticker, o.signal_time, o.trade_date,
                       e.direction, o.trigger_type, o.discovery_state,
                       e.validation_status, o.entry_price, o.atr_at_signal,
                       o.reference_level, o.stop_price, o.target_price,
                      o.risk_per_share, o.metadata, t.sector,
                      current_state.state AS current_discovery_state,
                      current_state.evidence ->> 'trend_state' AS trend_state,
                      current_state.evidence ->> 'extension_risk' AS extension_risk,
                      current_state.evidence ->> 'reversal_trigger' AS reversal_trigger,
                      current_state.evidence ->> 'position_guidance' AS position_guidance,
                       ROW_NUMBER() OVER (
                           PARTITION BY e.ticker
                           ORDER BY o.signal_time DESC, o.occurrence_id DESC
                       ) AS ticker_rank
                FROM scanner_event_occurrences o
                JOIN scanner_events e ON e.event_id = o.event_id
                                LEFT JOIN selected_tickers t ON t.ticker = e.ticker
                                LEFT JOIN LATERAL (
                                        SELECT d.state, d.evidence
                                        FROM market_discovery_states d
                                        WHERE d.ticker = e.ticker
                                        ORDER BY d.trade_date DESC
                                        LIMIT 1
                                ) current_state ON TRUE
                WHERE TRUE {interval_clause}
            )
            SELECT r.event_id, r.scanner_name, r.scanner_version, r.interval,
                   r.ticker, r.signal_time, r.direction, r.trigger_type,
                     r.discovery_state, r.current_discovery_state,
                     r.trend_state, r.extension_risk, r.reversal_trigger,
                     r.position_guidance, r.validation_status, r.sector,
                     CASE WHEN r.interval = '1d' THEN (
                       SELECT p.open_price FROM stock_prices_daily p
                       WHERE p.ticker = r.ticker AND p.datetime::date = r.trade_date
                       ORDER BY p.datetime LIMIT 1
                                     ) WHEN r.interval = '1wk' THEN (
                                             SELECT p.open_price FROM stock_prices_daily p
                                             WHERE p.ticker = r.ticker
                                                 AND DATE_TRUNC('week', p.datetime) = DATE_TRUNC('week', r.trade_date::timestamp)
                                             ORDER BY p.datetime LIMIT 1
                   ) ELSE (
                       SELECT p.open_price FROM stock_prices_hourly p
                       WHERE p.ticker = r.ticker AND p.datetime = r.signal_time
                       ORDER BY p.datetime LIMIT 1
                   ) END AS signal_open_price,
                   r.entry_price AS signal_close_price,
                   r.stop_price, r.target_price,
                   CASE WHEN r.interval IN ('1d', '1wk') THEN (
                       SELECT p.open_price FROM stock_prices_daily p
                       WHERE p.ticker = r.ticker AND p.datetime::date > r.trade_date
                       ORDER BY p.datetime LIMIT 1
                   ) ELSE (
                       SELECT p.open_price FROM stock_prices_hourly p
                       WHERE p.ticker = r.ticker AND p.datetime > r.signal_time
                       ORDER BY p.datetime LIMIT 1
                   ) END AS next_open_price,
                   CASE WHEN r.interval IN ('1d', '1wk') THEN (
                       SELECT p.datetime FROM stock_prices_daily p
                       WHERE p.ticker = r.ticker AND p.datetime::date > r.trade_date
                       ORDER BY p.datetime LIMIT 1
                   ) ELSE (
                       SELECT p.datetime FROM stock_prices_hourly p
                       WHERE p.ticker = r.ticker AND p.datetime > r.signal_time
                       ORDER BY p.datetime LIMIT 1
                   ) END AS next_open_time
            FROM ranked r
            WHERE r.ticker_rank = 1
            ORDER BY r.signal_time DESC, r.ticker
            LIMIT %s
        """, params)
        rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        tier, reasons = _review_priority(
            row["interval"], row["direction"], row["discovery_state"]
        )
        row["review_priority_tier"] = tier
        row["review_priority_reasons"] = reasons
    priority_order = {"HIGHER": 3, "STANDARD": 2, "LOWER": 1, "UNRANKED": 0}
    rows.sort(key=lambda row: row["ticker"])
    rows.sort(
        key=lambda row: priority_order[row["review_priority_tier"]], reverse=True
    )
    rows.sort(key=lambda row: row["signal_time"], reverse=True)
    return rows


def latest_sector_performance(sessions: int = 1) -> list[dict]:
    """Equal-weight sector returns over a supported trading-session horizon."""
    if sessions not in (1, 5, 10, 21):
        raise ValueError("Sector performance sessions must be 1, 5, 10, or 21")
    with get_db_cursor() as cur:
        cur.execute("""
            WITH latest_date AS (
                SELECT MAX(datetime::date) AS trade_date
                FROM stock_prices_daily
            ), current_bars AS (
                SELECT DISTINCT ON (p.ticker)
                       p.ticker, p.close_price, p.datetime::date AS trade_date
                FROM stock_prices_daily p
                CROSS JOIN latest_date d
                WHERE p.datetime::date = d.trade_date
                ORDER BY p.ticker, p.datetime DESC
            ), ticker_returns AS (
                SELECT c.ticker, t.sector, c.trade_date,
                       (c.close_price / NULLIF(previous.close_price, 0) - 1)::double precision AS return_pct
                FROM current_bars c
                JOIN selected_tickers t ON t.ticker = c.ticker AND t.is_active = TRUE
                JOIN LATERAL (
                    SELECT p.close_price
                    FROM stock_prices_daily p
                    WHERE p.ticker = c.ticker AND p.datetime::date < c.trade_date
                    ORDER BY p.datetime DESC
                    OFFSET %s
                    LIMIT 1
                ) previous ON TRUE
                WHERE t.sector IS NOT NULL AND BTRIM(t.sector) <> ''
            )
            SELECT sector, MAX(trade_date) AS trade_date,
                   COUNT(*)::integer AS tickers,
                   AVG(return_pct)::double precision AS average_return,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_pct)::double precision AS median_return,
                   COUNT(*) FILTER (WHERE return_pct > 0)::integer AS positive_tickers,
                   COUNT(*) FILTER (WHERE return_pct < 0)::integer AS negative_tickers,
                   (COUNT(*) FILTER (WHERE return_pct > 0)::double precision / NULLIF(COUNT(*), 0)) AS positive_breadth,
                   (ARRAY_AGG(ticker ORDER BY return_pct DESC))[1] AS best_ticker,
                   MAX(return_pct)::double precision AS best_return,
                   (ARRAY_AGG(ticker ORDER BY return_pct ASC))[1] AS worst_ticker,
                   MIN(return_pct)::double precision AS worst_return
            FROM ticker_returns
            WHERE return_pct IS NOT NULL
            GROUP BY sector
            ORDER BY average_return DESC, sector
        """, (sessions - 1,))
        return [dict(row) for row in cur.fetchall()]


def ticker_events(ticker: str, limit: int = 50) -> list[dict]:
    """Recent scanner setup lifecycles for one ticker."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT e.event_id, e.scanner_name, e.scanner_version, e.interval,
                   e.signal_time, e.last_seen_at, e.occurrence_count,
                   e.direction, e.trigger_type, e.discovery_state,
                   e.validation_status,
                                     CASE WHEN e.interval = '1d' THEN (
                       SELECT p.open_price FROM stock_prices_daily p
                       WHERE p.ticker = e.ticker AND p.datetime::date = e.trade_date
                       ORDER BY p.datetime LIMIT 1
                                     ) WHEN e.interval = '1wk' THEN (
                                             SELECT p.open_price FROM stock_prices_daily p
                                             WHERE p.ticker = e.ticker
                                                 AND DATE_TRUNC('week', p.datetime) = DATE_TRUNC('week', e.trade_date::timestamp)
                                             ORDER BY p.datetime LIMIT 1
                   ) ELSE (
                       SELECT p.open_price FROM stock_prices_hourly p
                       WHERE p.ticker = e.ticker AND p.datetime = e.signal_time
                       ORDER BY p.datetime LIMIT 1
                   ) END AS signal_open_price,
                   e.entry_price, e.atr_at_signal,
                   e.reference_level, e.stop_price, e.target_price,
                   e.risk_per_share, e.metadata,
                   COALESCE(jsonb_agg(
                       jsonb_build_object(
                           'horizon_bars', o.horizon_bars,
                           'entry_time', o.entry_time,
                           'entry_price', o.entry_price,
                           'entry_model', o.entry_model,
                           'exit_time', o.exit_time,
                           'exit_price', o.exit_price,
                           'net_signed_return', o.net_signed_return,
                           'net_alpha_return', o.net_alpha_return,
                           'mae_pct', o.mae_pct, 'mfe_pct', o.mfe_pct,
                           'mae_r', o.mae_r, 'mfe_r', o.mfe_r,
                           'first_hit', o.first_hit
                       ) ORDER BY o.horizon_bars
                   ) FILTER (WHERE o.outcome_id IS NOT NULL), '[]'::jsonb) AS outcomes
            FROM scanner_events e
            LEFT JOIN scanner_event_outcomes o ON o.event_id = e.event_id
            WHERE e.ticker = %s
            GROUP BY e.event_id
            ORDER BY e.signal_time DESC
            LIMIT %s
        """, (ticker.upper(), limit))
        return [dict(row) for row in cur.fetchall()]


def _float_or_none(value):
    return float(value) if pd.notna(value) else None
