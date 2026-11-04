"""Compatibility views derived only from a validated retained workspace summary."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone


def _return(row) -> float | None:
    value = row.get("chg_pct")
    return None if value is None else float(value) / 100


def _fact(value, publication, *, status="READY", reasons=()):
    return {
        "status": status,
        "value": value,
        "reason_codes": list(reasons),
        "available_at": publication["generated_at"],
        "market_time": publication["market_time"],
        "session": publication["session"],
        "lineage_ref": publication["snapshot_id"],
    }


def _prices(row):
    change = _return(row)
    close = float(row["close"])
    sma50 = row.get("sma_50")
    return {
        "close": close,
        "direction": (
            "ABOVE_50_SMA"
            if sma50 is not None and close >= float(sma50)
            else "BELOW_50_SMA"
            if sma50 is not None
            else "UNAVAILABLE"
        ),
        "moving_average_basis": "SMA50",
        "sma50": None if sma50 is None else float(sma50),
        "sma50_change10": None,
        "ema50": None,
        "ema50_change10": None,
        "return1": change,
        "return5": None,
        "return20": None,
        "volume": row.get("volume"),
        "range252_low": row.get("low_52w"),
        "range252_high": row.get("high_52w"),
    }


def _rotation(value, benchmark):
    relative = None if value is None or benchmark is None else value - benchmark
    return {
        "state": (
            "LEADING"
            if relative is not None and relative > 0
            else "LAGGING"
            if relative is not None and relative < 0
            else "NEUTRAL"
        ),
        "relative1": relative,
        "relative5": None,
        "relative20": None,
        "relative_change5": None,
        "benchmark_return5": None,
        "absolute_return5": None,
        "absolute_return20": None,
        "emerging_leadership": False,
    }


def _sector_rows(overview, publication):
    benchmark = _return(next((row for row in overview if row["ticker"] == "SPY"), {}))
    grouped = defaultdict(list)
    for row in overview:
        sector = row.get("sector")
        if sector and sector != "ETF":
            grouped[sector].append(row)
    rows = []
    for sector, members in sorted(grouped.items()):
        returns = [value for row in members if (value := _return(row)) is not None]
        average = sum(returns) / len(returns) if returns else None
        representative = sorted(members, key=lambda row: row["ticker"])[0]
        rows.append(
            {
                "ticker": representative["ticker"],
                "sector": sector,
                "context": _fact(_prices(representative), publication),
                "rotation": {
                    **_fact(_rotation(average, benchmark), publication),
                    "persistence": {
                        "status": "UNAVAILABLE",
                        "observations": 1,
                        "confirmed_state": None,
                    },
                },
                "history": [],
                "_members": members,
                "_average": average,
            }
        )
    return rows


def retained_market_conditions(summary):
    publication = summary["publication"]
    overview = summary["overview"]
    benchmark_rows = {
        row["ticker"]: row for row in overview if row["ticker"] in {"SPY", "QQQ"}
    }
    sectors = _sector_rows(overview, publication)
    sector_by_name = {row["sector"]: row for row in sectors}
    spy_return = _return(benchmark_rows.get("SPY", {}))
    stocks = []
    ready_states = 0
    for row in overview:
        sector = row.get("sector")
        if not sector or sector == "ETF":
            continue
        sector_row = sector_by_name.get(sector)
        value = _return(row)
        sector_return = None if sector_row is None else sector_row["_average"]
        relative = _rotation(value, sector_return)
        labels = []
        if relative["relative1"] is not None:
            labels.append("OUTPERFORMING" if relative["relative1"] > 0 else "UNDERPERFORMING")
        stocks.append(
            {
                "security_id": row["ticker"],
                "security_type": "CS",
                "ticker": row["ticker"],
                "sector": sector,
                "proxy": None if sector_row is None else sector_row["ticker"],
                "context": _fact(_prices(row), publication),
                "relative": _fact(relative, publication),
                "divergence": _fact(
                    {
                        "labels": labels,
                        "stock_return": value,
                        "sector_return": sector_return,
                    },
                    publication,
                ),
            }
        )
        ready_states += 1
    clean_sectors = [
        {key: value for key, value in row.items() if not key.startswith("_")}
        for row in sectors
    ]
    market_direction = (
        "POSITIVE"
        if spy_return is not None and spy_return > 0
        else "NEGATIVE"
        if spy_return is not None and spy_return < 0
        else "NEUTRAL"
    )
    expected_session = publication["expected_market_time"][:10]
    return {
        "status": publication["status"],
        "price_basis": publication["price_basis"],
        "session": publication["session"],
        "as_of": publication["market_time"],
        "capture_age_seconds": max(
            0,
            int(
                (
                    datetime.fromisoformat(publication["read_at"]).astimezone(timezone.utc)
                    - datetime.fromisoformat(publication["generated_at"]).astimezone(timezone.utc)
                ).total_seconds()
            ),
        ),
        "stale": publication["status"] != "READY",
        "expected_session": expected_session,
        "market": _fact({"direction": market_direction}, publication),
        "benchmarks": {
            ticker: _fact(_prices(row), publication)
            for ticker, row in benchmark_rows.items()
        },
        "sectors": clean_sectors,
        "stocks": stocks,
        "coverage": {
            "expected_sectors": len(sectors),
            "ready_sectors": len(sectors),
            "expected_stocks": len(stocks),
            "stock_states": {"READY": ready_states},
        },
        "warnings": [
            "Showing the exact retained realtime cohort; full-market and macro producers are unavailable.",
            "Only the retained one-session sector comparison is available.",
        ],
        "snapshot_sha256": publication["payload_sha256"],
        "refresh_mode": "STORED_RETAINED_RELEASE",
        "provider_fetch_performed": False,
    }


def retained_sector_intelligence(summary, leader_limit=5):
    publication = summary["publication"]
    sectors = _sector_rows(summary["overview"], publication)
    ranked = sorted(
        sectors,
        key=lambda row: (
            row["_average"] is not None,
            row["_average"] if row["_average"] is not None else float("-inf"),
        ),
        reverse=True,
    )
    results = []
    for rank, row in enumerate(ranked, 1):
        members = sorted(
            (
                {"ticker": member["ticker"], "return_pct": _return(member)}
                for member in row["_members"]
                if _return(member) is not None
            ),
            key=lambda member: member["return_pct"],
            reverse=True,
        )
        returns = [member["return_pct"] for member in members]
        one = {
            "average_return": row["_average"],
            "positive_breadth": (
                sum(value > 0 for value in returns) / len(returns)
                if returns
                else None
            ),
            "tickers": len(members),
            "rank": rank,
        }
        results.append(
            {
                "sector": row["sector"],
                "rotation": {
                    "1": one,
                    "5": None,
                    "10": None,
                    "21": None,
                    "63": None,
                },
                "rotation_delta": None,
                "discovery_mix": {},
                "discovery_universe": len(members),
                "cross_sectional_skew": None,
                "leaders": {"1": members[:leader_limit]},
                "laggards": {"1": list(reversed(members[-leader_limit:]))},
            }
        )
    return {
        "trade_date": publication["session"],
        "discovery_trade_date": None,
        "cross_sectional_trade_date": None,
        "legacy_context_status": "RETIRED_USE_RETAINED_REALTIME_COHORT",
        "sessions": [1, 5, 10, 21, 63],
        "leader_sessions": [1, 2, 3, 5, 10, 21, 63],
        "results": results,
        "status": publication["status"],
        "as_of": publication["market_time"],
        "snapshot_id": publication["snapshot_id"],
        "provider_fetch_performed": False,
    }


def retained_market_regime(summary):
    publication = summary["publication"]
    return {
        "regime": "Unknown",
        "description": (
            "The retained realtime cohort is available, but the full-market regime "
            "producer is not part of this release."
        ),
        "caution_buy": False,
        "caution_sell": False,
        "divergence": None,
        "spy": None,
        "qqq": None,
        "status": publication["status"],
        "as_of": publication["market_time"],
        "snapshot_id": publication["snapshot_id"],
        "provider_fetch_performed": False,
    }
