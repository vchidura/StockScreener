"""Bounded readers of published screening facts; never prepare source data."""
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import os
from uuid import UUID

import exchange_calendars
from fastapi import APIRouter, HTTPException

from database import get_db_cursor
from equity.calendar import latest_expected_market_time
from research.screening import FIELDS, PATTERNS, VERSION, STATE_FIELDS, SHORT_MOMENTUM_FIELDS, GAP_VERSION, GAP_CATALOG, supported_hourly_contract, GapPredicate, StrictModel, Query, evaluate_gaps, query_generation
from pydantic import Field


router = APIRouter(prefix="/api/stocks/screening", tags=["stock-screening"])
SNAPSHOT_TYPE = "SCREENING_DAILY_V1"
DEFERRED = {
    "sector": "Dated source coverage not yet verified",
    "industry": "Dated source coverage not yet verified",
    "market_cap": "Dated source coverage not yet verified",
    "rsi": "Indicator contract not yet inventoried",
    "adx": "Indicator contract not yet inventoried",
    "rs63": "Same-universe benchmark source not yet verified",
    "atr": "Indicator contract not yet inventoried",
    "active_setups": "Retained lifecycle and invalidation coverage not yet verified",
    "persistence": "Requires five expected compatible daily screening publications",
}


def publication_index():
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='2s'")
        cursor.execute("""SELECT snapshot_id,source_manifest,generated_at FROM equity_portal_snapshots
            WHERE snapshot_type=%s ORDER BY source_manifest->>'session' DESC,generated_at DESC,snapshot_id
            LIMIT 120""", (SNAPSHOT_TYPE,))
        return [dict(row) for row in cursor.fetchall()]


@lru_cache(maxsize=4)
def load_generation(snapshot_id):
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='2s'")
        cursor.execute("""SELECT payload,generated_at FROM equity_portal_snapshots
            WHERE snapshot_id=%s AND snapshot_type=%s""", (UUID(snapshot_id), SNAPSHOT_TYPE))
        record = cursor.fetchone()
        if record is None:
            raise HTTPException(404, "Requested screening publication is unavailable")
        payload = record["payload"]
        if len(payload.get("rows", [])) > 1000:
            raise HTTPException(503, "Publication exceeds bounded reader capacity")
        return dict(payload, observed_at=record["generated_at"].isoformat())


@router.get("/catalog")
def catalog():
    publications = publication_index()
    published_fields = set(publications[0]["source_manifest"].get("field_coverage", {})) if publications else set()
    fields = {name: spec for name, spec in FIELDS.items() if name not in STATE_FIELDS | SHORT_MOMENTUM_FIELDS or name in published_fields}
    gaps = GAP_CATALOG if publications and publications[0]["source_manifest"].get("gap_contract") == GAP_CATALOG else None
    published_hourly = publications[0]["source_manifest"].get("hourly_contract") if publications else None
    hourly = published_hourly if supported_hourly_contract(published_hourly) else None
    return dict(version=VERSION, fields=fields, patterns=PATTERNS, gaps=gaps, hourly=hourly, deferred=DEFERRED,
                publications=[dict(generation=row["source_manifest"]["generation"],
                                   session=row["source_manifest"]["session"], observed_at=row["generated_at"],
                                   fields=list(row["source_manifest"].get("field_coverage", {})))
                              for row in publications],
                status="READY" if publications else "AWAITING_FIRST_PUBLICATION")


@router.post("/query")
def query(request: Query):
    publications = publication_index()
    selected = next((row for row in publications if request.generation is None or row["source_manifest"]["generation"] == request.generation), None)
    if selected is None and request.generation:
        with get_db_cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout='2s'")
            cursor.execute("""SELECT snapshot_id FROM equity_portal_snapshots
                WHERE snapshot_type=%s AND source_manifest->>'generation'=%s LIMIT 1""", (SNAPSHOT_TYPE, request.generation))
            selected = cursor.fetchone()
    if selected is None:
        raise HTTPException(404, "Requested screening publication is unavailable" if request.generation else "Awaiting first daily screening publication")
    generation = load_generation(str(selected["snapshot_id"]))
    previous = previous_generation(generation, publications)
    try:
        result = query_generation(generation, request, previous)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    expected = latest_expected_market_time(datetime.now(timezone.utc), "1d")
    result["stale"] = datetime.fromisoformat(generation["market_time"]) < expected
    hourly = generation.get("hourly_source")
    if hourly:
        delay = int(os.getenv("EQUITY_PROVIDER_DELAY_MINUTES", "15"))
        expected_hourly = latest_expected_market_time(datetime.now(timezone.utc) - timedelta(minutes=delay), "1h")
        result["hourly_stale"] = not hourly.get("market_time") or datetime.fromisoformat(hourly["market_time"]) < expected_hourly
        result["hourly_expected_market_time"] = expected_hourly.isoformat()
        result["hourly_provider_delay_minutes"] = delay
    return result


def previous_generation(generation, publications):
    expected_session = generation.get("previous_expected_session")
    if not expected_session:
        return None
    calendar = exchange_calendars.get_calendar("XNYS")
    if str(calendar.previous_session(generation["session"]).date()) != expected_session:
        return None
    selected = next((row for row in publications if row["source_manifest"]["session"] == expected_session), None)
    if selected is None:
        with get_db_cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout='2s'")
            cursor.execute("""SELECT snapshot_id FROM equity_portal_snapshots
                WHERE snapshot_type=%s AND source_manifest->>'session'=%s
                ORDER BY generated_at DESC,snapshot_id LIMIT 1""", (SNAPSHOT_TYPE, expected_session))
            selected = cursor.fetchone()
    return load_generation(str(selected["snapshot_id"])) if selected else None


class SnapshotRowRequest(StrictModel):
    generation: str = Field(min_length=64, max_length=64)
    security_id: str = Field(min_length=1, max_length=64)


class GapDetailRequest(SnapshotRowRequest):
    gap: GapPredicate | None = None


def snapshot_row(request: SnapshotRowRequest):
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='2s'")
        cursor.execute("""SELECT snapshot_id FROM equity_portal_snapshots
            WHERE snapshot_type=%s AND source_manifest->>'generation'=%s LIMIT 1""", (SNAPSHOT_TYPE, request.generation))
        record = cursor.fetchone()
    if record is None:
        raise HTTPException(404, "Requested screening publication is unavailable")
    generation = load_generation(str(record["snapshot_id"]))
    row = next((row for row in generation["rows"] if row["security_id"] == request.security_id), None)
    if row is None:
        raise HTTPException(404, "Requested security is not in this publication")
    return generation, row


@router.post("/gap-details")
def gap_details(request: GapDetailRequest):
    generation, row = snapshot_row(request)
    if generation.get("gap_contract", {}).get("version") != GAP_VERSION:
        raise HTTPException(422, "Daily gap context unavailable in this publication")
    context = row["gaps"]
    state, episodes, reason = evaluate_gaps(context, request.gap or GapPredicate())
    return dict(generation=request.generation, security_id=request.security_id, session=generation["session"],
                source_cutoff=generation["source_cutoff"], status=context["status"], match_state=state,
                reason=reason, episode_count=len(context["episodes"]), matching_episodes=episodes,
                contract=generation["gap_contract"])


@router.post("/hourly-details")
def hourly_details(request: SnapshotRowRequest):
    generation, row = snapshot_row(request)
    if not supported_hourly_contract(generation.get("hourly_contract")):
        raise HTTPException(422, "Hourly context unavailable in this publication")
    return dict(generation=request.generation, security_id=request.security_id, source=generation["hourly_source"],
        values=row["hourly"]["values"], missing=row["hourly"]["missing"],
        lineage=generation["hourly_lineage"][request.security_id])