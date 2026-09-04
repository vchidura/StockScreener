"""Canonical sector performance and intelligence reads."""
from __future__ import annotations

from database import get_db_cursor


SECTOR_ROTATION_SESSIONS = (1, 5, 10, 21, 63)
LEADER_LAGGARD_SESSIONS = (1, 2, 3, 5, 10, 21, 63)


def latest_sector_performance(sessions: int = 1) -> list[dict]:
    if sessions not in (1, 5, 10, 21):
        raise ValueError("Sector performance sessions must be 1, 5, 10, or 21")
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH latest_date AS (
                SELECT MAX(datetime::DATE) AS trade_date
                FROM equity_canonical_daily_bars
            ), current_bars AS (
                SELECT DISTINCT ON (price.ticker)
                       price.ticker, price.close_price,
                       price.datetime::DATE AS trade_date
                FROM equity_canonical_daily_bars AS price
                CROSS JOIN latest_date
                WHERE price.datetime::DATE = latest_date.trade_date
                ORDER BY price.ticker, price.datetime DESC
            ), ticker_returns AS (
                SELECT current.ticker, selected.sector, current.trade_date,
                       (current.close_price / NULLIF(previous.close_price, 0) - 1)
                         ::DOUBLE PRECISION AS return_pct
                FROM current_bars AS current
                JOIN selected_tickers AS selected
                  ON selected.ticker = current.ticker
                 AND selected.is_active = TRUE
                JOIN LATERAL (
                    SELECT price.close_price
                    FROM equity_canonical_daily_bars AS price
                    WHERE price.ticker = current.ticker
                      AND price.datetime::DATE < current.trade_date
                    ORDER BY price.datetime DESC
                    OFFSET %s LIMIT 1
                ) AS previous ON TRUE
                WHERE selected.sector IS NOT NULL
                  AND BTRIM(selected.sector) <> ''
                  AND selected.sector <> 'ETF'
            )
            SELECT sector, MAX(trade_date) AS trade_date,
                   COUNT(*)::INTEGER AS tickers,
                   AVG(return_pct)::DOUBLE PRECISION AS average_return,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_pct)
                     ::DOUBLE PRECISION AS median_return,
                   COUNT(*) FILTER (WHERE return_pct > 0)::INTEGER
                     AS positive_tickers,
                   COUNT(*) FILTER (WHERE return_pct < 0)::INTEGER
                     AS negative_tickers,
                   COUNT(*) FILTER (WHERE return_pct > 0)::DOUBLE PRECISION
                     / NULLIF(COUNT(*), 0) AS positive_breadth,
                   (ARRAY_AGG(ticker ORDER BY return_pct DESC))[1] AS best_ticker,
                   MAX(return_pct)::DOUBLE PRECISION AS best_return,
                   (ARRAY_AGG(ticker ORDER BY return_pct))[1] AS worst_ticker,
                   MIN(return_pct)::DOUBLE PRECISION AS worst_return
            FROM ticker_returns
            WHERE return_pct IS NOT NULL
            GROUP BY sector
            ORDER BY average_return DESC, sector
            """,
            (sessions - 1,),
        )
        return [dict(row) for row in cursor.fetchall()]


def sector_intelligence(leader_limit: int = 5) -> dict:
    if leader_limit <= 0:
        raise ValueError("leader_limit must be positive")
    with get_db_cursor() as cursor:
        sessions = sorted(
            set(SECTOR_ROTATION_SESSIONS) | set(LEADER_LAGGARD_SESSIONS)
        )
        returns_by_session = {
            value: _sector_ticker_returns(cursor, value) for value in sessions
        }
        cursor.execute(
            """
            SELECT sector, COUNT(*)::INTEGER AS count
            FROM selected_tickers
            WHERE is_active = TRUE
              AND sector IS NOT NULL AND BTRIM(sector) <> '' AND sector <> 'ETF'
            GROUP BY sector
            """
        )
        sector_universe = {
            row["sector"]: row["count"] for row in cursor.fetchall()
        }
        cursor.execute(
            """
            SELECT trade_date FROM market_discovery_states
            ORDER BY trade_date DESC LIMIT 1
            """
        )
        discovery_row = cursor.fetchone()
        discovery_mix: dict[str, dict[str, int]] = {}
        if discovery_row:
            cursor.execute(
                """
                SELECT selected.sector, discovery.state,
                       COUNT(*)::INTEGER AS count
                FROM market_discovery_states AS discovery
                JOIN selected_tickers AS selected
                  ON selected.ticker = discovery.ticker
                 AND selected.is_active = TRUE
                WHERE discovery.trade_date = %s
                  AND selected.sector IS NOT NULL
                  AND BTRIM(selected.sector) <> ''
                  AND selected.sector <> 'ETF'
                GROUP BY selected.sector, discovery.state
                """,
                (discovery_row["trade_date"],),
            )
            for row in cursor.fetchall():
                discovery_mix.setdefault(row["sector"], {})[
                    row["state"]
                ] = row["count"]
        cursor.execute("SELECT MAX(trade_date) AS d FROM cross_sectional_signals")
        cross_sectional_row = cursor.fetchone()
        skew_by_sector: dict[str, dict] = {}
        names_by_sector: dict[str, list[dict]] = {}
        if cross_sectional_row and cross_sectional_row["d"]:
            cursor.execute(
                """
                SELECT selected.sector,
                       COUNT(*) FILTER (WHERE signal.side = 'LONG')::INTEGER
                         AS long_skew,
                       COUNT(*) FILTER (WHERE signal.side = 'SHORT')::INTEGER
                         AS short_skew,
                       AVG(signal.percentile)::DOUBLE PRECISION
                         AS average_percentile,
                       COUNT(*)::INTEGER AS covered
                FROM cross_sectional_signals AS signal
                JOIN selected_tickers AS selected
                  ON selected.ticker = signal.ticker
                 AND selected.is_active = TRUE
                WHERE signal.trade_date = %s
                  AND selected.sector IS NOT NULL
                  AND BTRIM(selected.sector) <> ''
                  AND selected.sector <> 'ETF'
                GROUP BY selected.sector
                """,
                (cross_sectional_row["d"],),
            )
            for row in cursor.fetchall():
                skew_by_sector[row["sector"]] = {
                    "long_skew": row["long_skew"],
                    "short_skew": row["short_skew"],
                    "average_percentile": row["average_percentile"],
                    "covered": row["covered"],
                }
            cursor.execute(
                """
                SELECT selected.sector, signal.ticker, signal.side,
                       signal.percentile
                FROM cross_sectional_signals AS signal
                JOIN selected_tickers AS selected
                  ON selected.ticker = signal.ticker
                 AND selected.is_active = TRUE
                WHERE signal.trade_date = %s
                  AND signal.side IN ('LONG', 'SHORT')
                  AND selected.sector IS NOT NULL
                  AND BTRIM(selected.sector) <> ''
                  AND selected.sector <> 'ETF'
                ORDER BY selected.sector,
                  CASE WHEN signal.side = 'LONG'
                       THEN signal.percentile ELSE -signal.percentile END DESC
                """,
                (cross_sectional_row["d"],),
            )
            for row in cursor.fetchall():
                names_by_sector.setdefault(row["sector"], []).append(dict(row))

    for sector, skew in skew_by_sector.items():
        names = names_by_sector.get(sector, [])
        skew["net_tilt"] = (
            (skew["long_skew"] - skew["short_skew"]) / skew["covered"]
            if skew["covered"] else None
        )
        skew["long_names"] = [
            row["ticker"] for row in names if row["side"] == "LONG"
        ][:leader_limit]
        skew["short_names"] = [
            row["ticker"] for row in names if row["side"] == "SHORT"
        ][:leader_limit]

    aggregates: dict[int, dict[str, dict]] = {}
    leaders: dict[str, dict[str, list[dict]]] = {}
    laggards: dict[str, dict[str, list[dict]]] = {}
    for sessions, rows in returns_by_session.items():
        by_sector: dict[str, list[dict]] = {}
        for row in rows:
            by_sector.setdefault(row["sector"], []).append(row)
        aggregate = {}
        for sector, sector_rows in by_sector.items():
            values = [row["return_pct"] for row in sector_rows]
            aggregate[sector] = {
                "average_return": sum(values) / len(values),
                "positive_breadth": sum(value > 0 for value in values) / len(values),
                "tickers": len(values),
            }
            ordered = sorted(
                sector_rows, key=lambda row: row["return_pct"], reverse=True
            )
            leaders.setdefault(sector, {})[str(sessions)] = [
                {"ticker": row["ticker"], "return_pct": row["return_pct"]}
                for row in ordered[:leader_limit]
            ]
            laggards.setdefault(sector, {})[str(sessions)] = [
                {"ticker": row["ticker"], "return_pct": row["return_pct"]}
                for row in ordered[-leader_limit:][::-1]
            ]
        ranked = sorted(
            aggregate.items(),
            key=lambda item: item[1]["average_return"],
            reverse=True,
        )
        for rank, (sector, _) in enumerate(ranked, 1):
            aggregate[sector]["rank"] = rank
        aggregates[sessions] = aggregate

    all_sectors = sorted(set().union(
        *(value.keys() for value in aggregates.values()),
        discovery_mix.keys(), skew_by_sector.keys(),
    ))
    fast = min(SECTOR_ROTATION_SESSIONS)
    slow = max(SECTOR_ROTATION_SESSIONS)
    results = []
    for sector in all_sectors:
        rotation = {
            str(value): aggregates.get(value, {}).get(sector)
            for value in SECTOR_ROTATION_SESSIONS
        }
        fast_rank = (aggregates.get(fast, {}).get(sector) or {}).get("rank")
        slow_rank = (aggregates.get(slow, {}).get(sector) or {}).get("rank")
        results.append({
            "sector": sector,
            "rotation": rotation,
            "rotation_delta": (
                slow_rank - fast_rank
                if fast_rank is not None and slow_rank is not None else None
            ),
            "discovery_mix": discovery_mix.get(sector, {}),
            "discovery_universe": sector_universe.get(sector, 0),
            "cross_sectional_skew": skew_by_sector.get(sector),
            "leaders": leaders.get(sector, {}),
            "laggards": laggards.get(sector, {}),
        })
    results.sort(
        key=lambda row: (
            row["rotation"].get(str(fast)) or {}
        ).get("average_return", 0),
        reverse=True,
    )
    one_session = returns_by_session.get(1, [])
    return {
        "trade_date": (
            one_session[0]["trade_date"].isoformat() if one_session else None
        ),
        "discovery_trade_date": (
            discovery_row["trade_date"].isoformat() if discovery_row else None
        ),
        "cross_sectional_trade_date": (
            cross_sectional_row["d"].isoformat()
            if cross_sectional_row and cross_sectional_row["d"] else None
        ),
        "sessions": list(SECTOR_ROTATION_SESSIONS),
        "leader_sessions": list(LEADER_LAGGARD_SESSIONS),
        "results": results,
    }


def _sector_ticker_returns(cursor, sessions: int) -> list[dict]:
    cursor.execute(
        """
        WITH latest_date AS (
            SELECT MAX(datetime::DATE) AS trade_date
            FROM equity_canonical_daily_bars
        ), current_bars AS (
            SELECT DISTINCT ON (price.ticker)
                   price.ticker, price.close_price,
                   price.datetime::DATE AS trade_date
            FROM equity_canonical_daily_bars AS price
            CROSS JOIN latest_date
            WHERE price.datetime::DATE = latest_date.trade_date
            ORDER BY price.ticker, price.datetime DESC
        )
        SELECT current.ticker, selected.sector, current.trade_date,
               (current.close_price / NULLIF(previous.close_price, 0) - 1)
                 ::DOUBLE PRECISION AS return_pct
        FROM current_bars AS current
        JOIN selected_tickers AS selected
          ON selected.ticker = current.ticker AND selected.is_active = TRUE
        JOIN LATERAL (
            SELECT price.close_price
            FROM equity_canonical_daily_bars AS price
            WHERE price.ticker = current.ticker
              AND price.datetime::DATE < current.trade_date
            ORDER BY price.datetime DESC
            OFFSET %s LIMIT 1
        ) AS previous ON TRUE
        WHERE selected.sector IS NOT NULL
          AND BTRIM(selected.sector) <> ''
          AND selected.sector <> 'ETF'
        """,
        (sessions - 1,),
    )
    return [
        dict(row) for row in cursor.fetchall() if row["return_pct"] is not None
    ]
