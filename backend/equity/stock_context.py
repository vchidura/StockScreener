"""Read-only equity context over existing immutable cohorts and published setups."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import math

from fastapi import APIRouter, HTTPException, Query
from psycopg2 import Error as DatabaseError

from database import get_db_cursor
from equity.api import expected_materialized_market_time
from equity.polygon import sha256_json


VERSION = "stock_equity_context_v1"
router = APIRouter(prefix="/api/stocks/context", tags=["stock-context"])
INTERVALS = ("1d", "1h", "30m")


def stamp(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("context clocks must be timezone aware")
    return result


def number(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def build_context(daily, setups, references, *, now, expected):
    now = stamp(now)
    payload = daily["payload"] if daily else None
    rows = payload.get("rows", []) if payload else []
    if len(rows) > 1000 or len({row["security_id"] for row in rows}) != len(rows):
        raise ValueError("daily cohort is duplicated or exceeds its bound")
    if payload and (payload.get("expected_members") != len(rows)
                    or stamp(daily["generated_at"]) > now or stamp(payload["source_cutoff"]) > now
                    or stamp(payload["market_time"]) > expected["1d"]):
        raise ValueError("daily cohort membership or source clocks are invalid")
    if payload and sha256_json(payload) != daily["payload_sha256"]:
        raise ValueError("daily cohort checksum mismatch")
    cohort = None
    if payload:
        ranked = sum(row.get("eligible", False) and number(row["values"].get("momentum_percentile")) is not None for row in rows)
        if ranked != payload.get("rank_population"):
            raise ValueError("rank population differs from retained publication")
        cohort = dict(snapshot_id=str(daily["snapshot_id"]), generation=payload["generation"],
            payload_sha256=daily["payload_sha256"], source_publication_id=payload["source_publication_id"],
            universe=payload["universe"], expected_members=len(rows), eligible_ranked=ranked,
            session=payload["session"], market_time=payload["market_time"], source_cutoff=payload["source_cutoff"],
            available_at=stamp(daily["generated_at"]).isoformat(), ranking="RAW_12_MINUS_1_MOMENTUM_PERCENTILE",
            status="READY" if stamp(payload["market_time"]) == expected["1d"] else "STALE")
    grouped = {interval: [row for row in setups if row["interval"] == interval] for interval in INTERVALS}
    coherent = {interval: len({(str(row["analysis_run_id"]), stamp(row["market_time"])) for row in values}) <= 1
                for interval, values in grouped.items()}
    by_identity = {}
    for source in setups:
        identity = str(source["security_id"])
        key = (identity, source["interval"])
        if key in by_identity:
            raise ValueError("duplicate setup identity")
        by_identity[key] = source
    securities = {row["security_id"]: row for row in rows}
    daily_identities = {row["ticker"]: row["security_id"] for row in rows}
    identity_conflicts = {source["ticker"] for source in setups if source["ticker"] in daily_identities
                          and str(source["security_id"]) != daily_identities[source["ticker"]]}
    for source in setups:
        if source["ticker"] in identity_conflicts:
            continue
        securities.setdefault(str(source["security_id"]), dict(security_id=str(source["security_id"]), ticker=source["ticker"], values={}))
    output = []
    for identity, row in securities.items():
        values = row["values"]
        percentile = number(values.get("momentum_percentile")) if row.get("eligible") else None
        daily_state = dict(status=cohort["status"] if cohort and percentile is not None else "UNAVAILABLE",
            percentile=percentile, momentum=number(values.get("momentum_12_1")), price=number(values.get("price")),
            trend=values.get("discovery_trend"), state=values.get("discovery_state"),
            missing=row.get("missing", {}), reference_id=row.get("reference_id"))
        if row["ticker"] in identity_conflicts:
            daily_state.update(status="UNAVAILABLE", percentile=None, momentum=None, price=None, trend=None, state=None,
                               missing={"identity": "DAILY_INTRADAY_IDENTITY_MISMATCH"})
        frames = {}
        for interval in INTERVALS:
            source = by_identity.get((identity, interval))
            if row["ticker"] in identity_conflicts:
                frames[interval] = dict(status="UNAVAILABLE", reason="DAILY_INTRADAY_IDENTITY_MISMATCH")
                continue
            if source is None:
                frames[interval] = dict(status="UNAVAILABLE", reason="SETUP_NOT_PUBLISHED")
                continue
            status, reason = "READY", None
            clocks = [stamp(source[key]) for key in ("market_time", "observed_at", "published_at", "created_at")]
            if source["ticker"] != row["ticker"]:
                status, reason = "UNAVAILABLE", "SECURITY_TICKER_MISMATCH"
            elif max(clocks) > now or clocks[0] > expected[interval]:
                status, reason = "UNAVAILABLE", "SOURCE_NOT_YET_AVAILABLE"
            elif not coherent[interval]:
                status, reason = "UNAVAILABLE", "MIXED_ANALYSIS_COHORT"
            elif clocks[0] < expected[interval]:
                status, reason = "STALE", "PRIOR_COMPLETED_INTERVAL"
            frames[interval] = dict(status=status, reason=reason, evidence_id=str(source["evidence_id"]),
                payload_sha256=source["payload_sha256"], analysis_run_id=str(source["analysis_run_id"]),
                market_time=clocks[0].isoformat(), available_at=max(clocks[1:]).isoformat(),
                source_version=source["source_version"], price_basis="RAW_UNADJUSTED_SETUP",
                trend=source.get("trend") if status != "UNAVAILABLE" else None,
                momentum=source.get("momentum") if status != "UNAVAILABLE" else None,
                close=number(source.get("close")) if status != "UNAVAILABLE" else None)
        reference = references.get(str(row.get("reference_id")))
        sector = reference.get("sector") if reference and str(reference["security_id"]) == identity else None
        output.append(dict(security_id=identity, ticker=row["ticker"], company_name=row.get("company_name"),
            sector=sector, daily=daily_state, frames=frames))
    return dict(schema=VERSION, as_of=now.isoformat(), cohort=cohort, rows=sorted(output, key=lambda row: row["ticker"]),
                status="READY" if cohort and cohort["status"] == "READY" else "PARTIAL" if output else "UNAVAILABLE",
                execution_permission=False)


def load_stock_context(*, now=None):
    now = now or datetime.now(timezone.utc)
    expected = {interval: expected_materialized_market_time(now, interval) for interval in INTERVALS}
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='5s'")
        cursor.execute("""SELECT snapshot_id,payload_sha256,generated_at,payload
            FROM equity_portal_snapshots WHERE snapshot_type='SCREENING_DAILY_V1'
              AND NOT (source_manifest ? 'hourly_refresh_policy') AND generated_at<=%s
              AND (source_manifest->>'market_time')::timestamptz<=%s
              AND octet_length(payload::text)<=20000000
            ORDER BY source_manifest->>'session' DESC,generated_at DESC,snapshot_id LIMIT 1""", (now, expected["1d"]))
        record = cursor.fetchone()
        daily = dict(record) if record else None
        references = {}
        if daily:
            ids = [row["reference_id"] for row in daily["payload"]["rows"] if row.get("reference_id")]
            cursor.execute("""SELECT security_revision_id::text,security_id::text,sector
                FROM equity_security_reference_revisions WHERE security_revision_id=ANY(%s::uuid[])
                  AND observed_at<=%s AND created_at<=%s""", (ids, daily["payload"]["source_cutoff"], daily["payload"]["source_cutoff"]))
            references = {row["security_revision_id"]: dict(row) for row in cursor.fetchall()}
        cursor.execute("""SELECT evidence.security_id::text,projection.ticker,projection.interval_key AS interval,
                projection.evidence_id::text,projection.analysis_run_id::text,projection.market_time,
                projection.observed_at,projection.published_at,evidence.created_at,evidence.source_version,
                evidence.payload_sha256,evidence.payload->'ema_alignment'->>'primary' AS trend,
                evidence.payload->'momentum'->>'state' AS momentum,
                (evidence.payload->>'last_close')::float8 AS close
            FROM equity_current_projection projection JOIN equity_evidence evidence USING(evidence_id)
            WHERE projection.projection_type='TRADE_SETUP' AND projection.source_name='EQUITY_SETUP'
              AND projection.interval_key=ANY(%s::text[]) AND evidence.ticker=projection.ticker
              AND evidence.interval=projection.interval_key AND evidence.analysis_run_id=projection.analysis_run_id
            ORDER BY projection.interval_key,projection.ticker LIMIT 3001""", (list(INTERVALS),))
        setups = [dict(row) for row in cursor.fetchall()]
        if len(setups) > 3000:
            raise ValueError("context setup read exceeds declared bound")
    return build_context(daily, setups, references, now=now, expected=expected)


def sector_context(context, sector):
    rows = [row for row in context["rows"] if row["sector"] == sector]
    ranked = [row for row in rows if row["daily"]["percentile"] is not None]
    return dict(cohort=context["cohort"], members=len(rows), ranked=len(ranked), unknown=len(rows) - len(ranked),
        state_mix=dict(Counter(row["daily"]["state"] or "UNAVAILABLE" for row in rows)),
        leading=sum(row["daily"]["percentile"] >= .9 for row in ranked),
        lagging=sum(row["daily"]["percentile"] <= .1 for row in ranked),
        average_percentile=sum(row["daily"]["percentile"] for row in ranked) / len(ranked) if ranked else None)


@router.get("")
def stock_context(ticker: str | None = Query(None, min_length=1, max_length=32, pattern=r"^[A-Za-z0-9.\-]+$")):
    try:
        context = load_stock_context()
        if ticker:
            context["rows"] = [row for row in context["rows"] if row["ticker"] == ticker.upper()]
        return context
    except (ValueError, KeyError, TypeError, DatabaseError) as error:
        raise HTTPException(503, "Published equity context is unavailable or incompatible") from error


@router.get("/bridge/{ticker}")
def stock_bridge_context(ticker: str):
    from equity.behavior import DEFINITION_V1_SHA256, OPTIONS_SWING_PROFILE
    from equity.behavior_context import attach_daily_rank
    from equity.domain import DecisionWatermark
    from equity.repositories import EquityEvidenceRepository, EquityReferenceRepository
    if not ticker or len(ticker) > 32 or any(not (char.isalnum() or char in ".-") for char in ticker):
        raise HTTPException(422, "Invalid ticker")
    now = datetime.now(timezone.utc)
    decision = DecisionWatermark(now, now)
    try:
        security = EquityReferenceRepository().get_security_as_of(ticker.upper(), decision)
        snapshot = EquityEvidenceRepository().get_behavior_as_of(security.security_id, decision,
            profile=OPTIONS_SWING_PROFILE.name, definition_sha256=DEFINITION_V1_SHA256,
            policy_sha256=OPTIONS_SWING_PROFILE.sha256) if security else None
        if snapshot is None:
            return dict(status="UNAVAILABLE", reason="COMPATIBLE_BEHAVIOR_SNAPSHOT_UNAVAILABLE", execution_permission=False)
        attachment = attach_daily_rank(snapshot, load_stock_context(now=now), decision)
        return dict(status="AVAILABLE", behavior=snapshot.model_dump(mode="json"),
                    attachment=attachment.model_dump(mode="json"), attachment_sha256=attachment.sha256,
                    execution_permission=False)
    except (ValueError, KeyError, TypeError, DatabaseError) as error:
        raise HTTPException(503, "Published bridge context is unavailable or incompatible") from error