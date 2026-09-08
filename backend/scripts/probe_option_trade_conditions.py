#!/usr/bin/env python3
"""Compare today's observed option trade conditions with provider definitions."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402


def main() -> int:
    configuration = load_option_runtime_configuration()
    api_key = configuration.settings.polygon_api_key.get_secret_value()
    response = requests.get(
        "https://api.polygon.io/v3/reference/conditions",
        params={
            "asset_class": "options",
            "data_type": "trade",
            "limit": 1000,
            "apiKey": api_key,
        },
        timeout=30,
    )
    response.raise_for_status()
    definitions = {
        int(row["id"]): row for row in response.json().get("results", [])
    }
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT condition_code, correction, classification_status,
                   COUNT(*) AS trades, SUM(notional) AS notional
            FROM option_trade_events
            LEFT JOIN LATERAL unnest(conditions) condition_code ON TRUE
            WHERE (sip_timestamp AT TIME ZONE 'America/New_York')::date =
                  (NOW() AT TIME ZONE 'America/New_York')::date
            GROUP BY condition_code, correction, classification_status
            ORDER BY trades DESC, condition_code NULLS FIRST
            """
        )
        observed = [dict(row) for row in cursor.fetchall()]
    report = []
    for item in observed:
        definition = definitions.get(item["condition_code"])
        report.append({
            **item,
            "definition": None if definition is None else {
                "name": definition.get("name"),
                "abbreviation": definition.get("abbreviation"),
                "type": definition.get("type"),
                "legacy": definition.get("legacy"),
                "update_rules": definition.get("update_rules"),
            },
        })
    print(json.dumps({
        "definition_count": len(definitions),
        "definitions": [
            {
                "id": condition_id,
                "name": row.get("name"),
                "abbreviation": row.get("abbreviation"),
                "legacy": row.get("legacy"),
                "updates_volume": (
                    (row.get("update_rules") or {})
                    .get("consolidated", {})
                    .get("updates_volume")
                ),
            }
            for condition_id, row in sorted(definitions.items())
        ],
        "observed": report,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
