"""Shared Screening features and retained legacy snapshot filtering."""

from __future__ import annotations

from collections import Counter

from equity.daily_state import stock_features as stock_features

VERSION = "stock_discovery_v1"

SNAPSHOT_SOURCE = "STOCK_DISCOVERY"

ALERT_SOURCE = "STOCK_DISCOVERY_ALERT"

MARK_SOURCE = "STOCK_DISCOVERY_MARK"

NOTIONAL = 1000.0

COST_BPS = 10.0

def filter_stocks(rows, *, search="", sector=None, side=None, state=None, eligible_only=True,
                  minimum_price=0, minimum_dollar_volume=0, minimum_relative_volume=0,
                  sort="RANK", offset=0, limit=100):
    sectors = sorted({row.get("sector") for row in rows if row.get("sector")})
    selected = [row for row in rows if (not eligible_only or row["eligible"])
                and (not search or search.upper() in (row["ticker"] + " " + (row.get("company_name") or "")).upper())
                and (not sector or row.get("sector") == sector) and (not side or row["side"] == side)
                and (not state or row["state"] == state)
                and (row["price"] or 0) >= minimum_price and (row["dollar_volume"] or 0) >= minimum_dollar_volume
                and (row["relative_volume"] or 0) >= minimum_relative_volume]
    field = {"RANK": "rank", "WEAKEST": "momentum", "CHANGE": "change", "VOLUME": "relative_volume", "LIQUIDITY": "dollar_volume", "VOLATILITY": "volatility"}[sort]
    descending = sort not in ("RANK", "WEAKEST")
    selected.sort(key=lambda row: (row.get(field) is None, (-row[field] if descending else row[field]) if row.get(field) is not None else 0, row["ticker"]))
    return dict(rows=selected[offset:offset + limit], total=len(selected), offset=offset, limit=limit, sectors=sectors,
                eligible_count=sum(row["eligible"] for row in rows), universe_count=len(rows),
                exclusion_counts=dict(Counter(row["exclusion"] for row in rows if not row["eligible"])))
