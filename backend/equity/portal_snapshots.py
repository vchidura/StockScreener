"""Persistence and freshness reads for expensive equity portal snapshots."""
from __future__ import annotations

import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Mapping
from uuid import NAMESPACE_URL, uuid5

from psycopg2.extras import Json

from database import get_db_cursor
from .polygon import sha256_json


SNAPSHOT_TYPES = frozenset((
    "TICKER_OVERVIEW",
    "MARKET_REGIME",
    "SECTOR_PERFORMANCE_1",
    "SECTOR_PERFORMANCE_5",
    "SECTOR_PERFORMANCE_10",
    "SECTOR_PERFORMANCE_21",
    "SECTOR_INTELLIGENCE",
    "SCAN_GAPS_1D",
    "SCAN_FVG_1D_50",
    "SCAN_MA_1D_9_21",
    "SCAN_MOMENTUM_1D",
    "SCAN_BEARISH_1D",
    "SCAN_FIBONACCI_1D_5",
    "SCAN_ALL_1D_5",
    "STREAK_GAPS_5",
    "STREAK_MA_5",
    "STREAK_MOMENTUM_5",
    "STREAK_BEARISH_5",
    "STREAK_FIBONACCI_5",
    "STREAK_SUMMARY_3_5",
))


def source_manifest() -> dict[str, Any]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT generation, changed_at
            FROM equity_portal_source_state
            WHERE singleton = TRUE
            """
        )
        source = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT interval, COUNT(DISTINCT ticker) AS members, MAX(bar_end) AS market_time
            FROM equity_current_bar_projection
            WHERE interval IN ('1d', '1wk')
            GROUP BY interval
            ORDER BY interval
            """
        )
        bars = [dict(row) for row in cursor.fetchall()]
    return {"source_generation": int(source["generation"]), "bars": bars}


def publish(payloads: Mapping[str, Any], manifest: Mapping[str, Any]) -> list[str]:
    unknown = set(payloads) - SNAPSHOT_TYPES
    if unknown:
        raise ValueError(f"unsupported equity portal snapshots: {sorted(unknown)}")
    generated_at = datetime.now(timezone.utc)
    manifest_payload = _json_value(dict(manifest))
    manifest_sha256 = sha256_json(manifest_payload)
    generation = int(manifest_payload["source_generation"])
    published = []
    with get_db_cursor() as cursor:
        for snapshot_type, payload in payloads.items():
            payload = _json_value(payload)
            payload_sha256 = sha256_json(payload)
            snapshot_id = uuid5(
                NAMESPACE_URL,
                f"equity-portal:{snapshot_type}:{manifest_sha256}:{payload_sha256}",
            )
            cursor.execute(
                """
                INSERT INTO equity_portal_snapshots (
                    snapshot_id, snapshot_type, source_generation, source_manifest,
                    source_manifest_sha256, payload, payload_sha256, generated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (snapshot_type, source_manifest_sha256, payload_sha256)
                DO NOTHING
                """,
                (
                    snapshot_id, snapshot_type, generation, Json(manifest_payload),
                    manifest_sha256, Json(payload), payload_sha256, generated_at,
                ),
            )
            cursor.execute(
                """
                INSERT INTO equity_portal_current_projections (
                    snapshot_type, snapshot_id, published_at
                ) VALUES (%s,%s,%s)
                ON CONFLICT (snapshot_type) DO UPDATE SET
                    snapshot_id = EXCLUDED.snapshot_id,
                    published_at = EXCLUDED.published_at
                WHERE equity_portal_current_projections.published_at <= EXCLUDED.published_at
                """,
                (snapshot_type, snapshot_id, generated_at),
            )
            published.append(str(snapshot_id))
    return published


def current(snapshot_type: str) -> dict[str, Any] | None:
    if snapshot_type not in SNAPSHOT_TYPES:
        raise ValueError(f"unsupported equity portal snapshot: {snapshot_type}")
    started = time.perf_counter()
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT snapshot.snapshot_id, snapshot.payload, snapshot.generated_at,
                   projection.published_at,
                   snapshot.source_generation = source.generation AS is_fresh
            FROM equity_portal_current_projections projection
            JOIN equity_portal_snapshots snapshot
              ON snapshot.snapshot_id = projection.snapshot_id
            CROSS JOIN equity_portal_source_state source
            WHERE projection.snapshot_type = %s
              AND source.singleton = TRUE
            """,
            (snapshot_type,),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    result = dict(row)
    result["read_latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value