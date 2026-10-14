#!/usr/bin/env python3
"""Read-only V2 audit of the retained ticker history inputs of one cohort and session.

Checks, from one REPEATABLE READ read-only transaction with a statement timeout: reference identity of the
cohort's equity members, evidence source-window closures and their bar lineage for the session, the current
evidence pointers against their immutable rows, the retained current bars per ticker/interval/basis, the
current portal release and the current screening generation. It also checks that the current ticker workspaces,
which are stored by reference (`chart_refs`, `evidence_index`), still resolve: every pinned evidence row exists with
its type and payload hash, and every chart group's published lineage head (count and chain digest) equals the bars
`rt.bar_current` holds now. No provider request and no write. Targets
stocks_realtime_v2 or an explicit test_realtime_v2_ database (--allow-disposable).

The v1 checks that read embedded v1 portal payloads (pattern watch parity, AAPL technical summary, snapshot
selection predicates) are not ported: V2 hot manifests are lineage heads and payload parity belongs to the
PORTAL_PUBLISH stage tests.
"""
from __future__ import annotations

import argparse
import json
import sys
from contextlib import closing
from datetime import date
from pathlib import Path
from uuid import UUID

BACKEND_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import validate_target_database_v2  # noqa: E402
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import (  # noqa: E402
    admin_settings, connect, query_rows, read_only_transaction, require_new_workspace_report,
)

# The cohort's equity members with the latest security revision visible now (its ticker).
_COHORT_MEMBERS = (
    "WITH members AS (SELECT DISTINCT instrument.instrument_id,instrument.security_id "
    "FROM rt.cohorts AS cohort JOIN rt.subscription_members AS member ON member.manifest_id=cohort.manifest_id "
    "JOIN rt.instruments AS instrument ON instrument.instrument_id=member.instrument_id "
    "WHERE cohort.cohort_id=%s AND member.channel='AM' AND instrument.asset_class='EQUITY'), "
    "tickers AS (SELECT DISTINCT ON (members.security_id) members.instrument_id,members.security_id,revision.ticker "
    "FROM members JOIN ref.security_revisions AS revision ON revision.security_id=members.security_id "
    "AND revision.effective_from<=current_timestamp "
    "AND (revision.effective_to IS NULL OR revision.effective_to>current_timestamp) "
    "AND revision.observed_at<=current_timestamp "
    "ORDER BY members.security_id,revision.effective_from DESC) "
)
# Source windows closed on the audited session (a day of slack on each side keeps the scan on the market_time index).
_SESSION_WINDOWS = (
    "windows AS (SELECT cohort_id,source_window_id,interval,market_time,observed_at,"
    "primary_bar_revision_ids,confirmation_bar_revision_ids FROM rt.equity_evidence_source_windows "
    "WHERE cohort_id=%s AND market_time >= (%s::date - 1) AND market_time < (%s::date + 2) "
    "AND (market_time AT TIME ZONE 'America/New_York')::date=%s) "
)


# The ticker workspaces of the cohort's current release (a workspace is stored by reference: `chart_refs` name the
# lineage-head groups of its charts and `evidence_index` pins its evidence rows).
_CURRENT_WORKSPACES = (
    "current_workspaces AS (SELECT hot.snapshot_id,hot.snapshot_key AS ticker,hot.payload,hot.source_manifest "
    "FROM rt.portal_release_current AS pointer "
    "JOIN rt.portal_releases AS release ON release.release_id=pointer.release_id AND release.cutoff=pointer.cutoff "
    "CROSS JOIN LATERAL jsonb_array_elements(release.manifest->'snapshots') AS entry(value) "
    "JOIN rt.portal_snapshot_hot AS hot ON hot.snapshot_id=(entry.value->>'snapshot_id')::uuid "
    "AND hot.cohort_id=pointer.cohort_id "
    "WHERE pointer.cohort_id=%s AND entry.value->>'snapshot_type'='TICKER_WORKSPACE_V1') "
)
# Evidence pinned by the index that no longer resolves, or resolves to another type or payload hash.
_EVIDENCE_REFERENCES = (
    "WITH " + _CURRENT_WORKSPACES + ", refs AS (SELECT workspace.ticker,ref.value->>'evidence_id' AS evidence_id,"
    "ref.value->>'evidence_type' AS evidence_type,ref.value->>'payload_sha256' AS payload_sha256 "
    "FROM current_workspaces AS workspace "
    "CROSS JOIN LATERAL jsonb_each(COALESCE(workspace.payload->'evidence_index','{}'::jsonb)) AS grouped(key,value) "
    "CROSS JOIN LATERAL jsonb_array_elements(grouped.value) AS ref(value)) "
    "SELECT refs.ticker,COUNT(*) AS referenced,COUNT(*) FILTER (WHERE evidence.evidence_id IS NULL) AS missing,"
    "COUNT(*) FILTER (WHERE evidence.evidence_id IS NOT NULL AND (evidence.evidence_type<>refs.evidence_type "
    "OR evidence.payload_sha256<>refs.payload_sha256)) AS mismatched "
    "FROM refs LEFT JOIN rt.evidence_catalog AS catalog ON catalog.cohort_id=%s "
    "AND catalog.evidence_id=refs.evidence_id::uuid "
    "LEFT JOIN rt.equity_evidence AS evidence ON evidence.cohort_id=catalog.cohort_id "
    "AND evidence.evidence_id=catalog.evidence_id AND evidence.durable_at=catalog.durable_at "
    "GROUP BY refs.ticker ORDER BY refs.ticker"
)
# Each chart group's published lineage head against the bars rt.bar_current points at now: the count and the
# ordered chain digest over (revision id, payload hash), computed in the database exactly as the reader does.
_CHART_LINEAGE = (
    "WITH " + _CURRENT_WORKSPACES + ", chart_groups AS (SELECT workspace.ticker,chart.key AS chart_interval,"
    "chart.value #>> '{}' AS group_key,"
    "workspace.source_manifest->'bar_lineage'->'groups'->(chart.value #>> '{}') AS head,"
    "(workspace.source_manifest->'reference'->>'instrument_id')::bigint AS instrument_id "
    "FROM current_workspaces AS workspace "
    "CROSS JOIN LATERAL jsonb_each(COALESCE(workspace.payload->'chart_refs','{}'::jsonb)) AS chart(key,value)) "
    "SELECT chart_groups.ticker,chart_groups.chart_interval,(chart_groups.head->>'count')::integer AS published_count,"
    "chart_groups.head->>'chain_sha256' AS published_chain,bars.current_count,bars.current_chain "
    "FROM chart_groups CROSS JOIN LATERAL (SELECT COUNT(*) AS current_count,"
    "encode(sha256(convert_to(COALESCE(string_agg(cur.bar_revision_id::text||':'||revision.payload_sha256||';',"
    "'' ORDER BY cur.bar_start),''),'UTF8')),'hex') AS current_chain "
    "FROM rt.bar_current AS cur JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=cur.bar_revision_id "
    "AND revision.observed_at=cur.observed_at AND revision.cohort_id=cur.cohort_id "
    "AND revision.instrument_id=cur.instrument_id AND revision.interval=cur.interval AND revision.is_final "
    "WHERE cur.cohort_id=%s AND cur.instrument_id=chart_groups.instrument_id AND cur.session_scope='RTH' "
    "AND cur.interval=split_part(chart_groups.group_key,':',1) "
    "AND cur.adjustment_basis=split_part(chart_groups.group_key,':',2) "
    "AND cur.bar_start>=(chart_groups.head->>'first_bar_start')::timestamptz "
    "AND cur.bar_end<=(chart_groups.head->>'last_bar_end')::timestamptz) AS bars "
    "ORDER BY chart_groups.ticker,chart_groups.chart_interval"
)


def by_reference_integrity(cursor, cohort: str) -> dict:
    """Do the current workspaces' references still resolve? Evidence ids must exist with the pinned type and
    payload hash; each chart group's count and chain digest must equal its published lineage head (a bar
    revised or retired since publication shows up here as drift, and the reader refuses that chart)."""
    evidence = query_rows(cursor, _EVIDENCE_REFERENCES, (cohort, cohort))
    groups = query_rows(cursor, _CHART_LINEAGE, (cohort, cohort))
    drifted = [
        {"ticker": row["ticker"], "interval": row["chart_interval"], "published_count": row["published_count"],
         "current_count": row["current_count"]}
        for row in groups
        if row["current_count"] != row["published_count"] or row["current_chain"] != row["published_chain"]
    ]
    return {
        "workspaces": len({row["ticker"] for row in (*evidence, *groups)}),
        "evidence_referenced": sum(row["referenced"] for row in evidence),
        "evidence_missing": sum(row["missing"] for row in evidence),
        "evidence_mismatched": sum(row["mismatched"] for row in evidence),
        "evidence_by_ticker": evidence,
        "chart_groups": len(groups),
        "chart_groups_drifted": drifted,
    }


def _scale(rows) -> list[dict]:
    daily = {}
    for row in rows:
        if row["interval"] == "1d":
            daily.setdefault(row["ticker"], {})[row["adjustment_basis"]] = row["latest_close"]
    scale = []
    for ticker, values in sorted(daily.items()):
        adjusted = values.get("ADJUSTED")
        raw = values.get("RAW_UNADJUSTED")
        scale.append({
            "ticker": ticker,
            "adjusted_close": adjusted,
            "raw_close": raw,
            "absolute_difference": None if adjusted is None or raw is None else adjusted - raw,
            "relative_difference": None if adjusted in {None, 0} or raw is None else raw / adjusted - 1,
        })
    return scale


def build_report(cursor, control, *, cohort_id: UUID, session_date: date):
    cohort = str(cohort_id)
    reference_membership = query_rows(cursor, """
        SELECT instrument.instrument_id,instrument.security_id::text,
            COUNT(reference.revision_id) AS visible_revision_count,
            ARRAY_AGG(DISTINCT reference.ticker) FILTER (WHERE reference.revision_id IS NOT NULL) AS tickers,
            MIN(reference.effective_from) AS first_effective_at,
            MAX(reference.observed_at) AS latest_observed_at
        FROM rt.cohorts AS cohort
        JOIN rt.subscription_members AS member ON member.manifest_id=cohort.manifest_id
        JOIN rt.instruments AS instrument ON instrument.instrument_id=member.instrument_id
        LEFT JOIN ref.security_revisions AS reference ON reference.security_id=instrument.security_id
            AND reference.effective_from<=current_timestamp
            AND (reference.effective_to IS NULL OR reference.effective_to>current_timestamp)
            AND reference.observed_at<=current_timestamp
        WHERE cohort.cohort_id=%s AND member.channel='AM' AND instrument.asset_class='EQUITY'
        GROUP BY instrument.instrument_id,instrument.security_id ORDER BY instrument.instrument_id
    """, (cohort,))
    reference_validity = query_rows(cursor, """
        SELECT reference.security_id::text,reference.ticker,COUNT(*) AS retained_revisions,
            MIN(reference.effective_from) AS first_effective_at,
            MAX(reference.effective_from) AS latest_effective_at,
            MAX(reference.effective_to) AS latest_effective_to,
            BOOL_OR(reference.effective_to IS NULL) AS has_open_ended_revision,
            MAX(reference.observed_at) AS latest_observed_at,current_timestamp AS checked_at
        FROM ref.security_revisions AS reference WHERE reference.security_id IN (
            SELECT instrument.security_id FROM rt.cohorts AS cohort
            JOIN rt.subscription_members AS member ON member.manifest_id=cohort.manifest_id
            JOIN rt.instruments AS instrument ON instrument.instrument_id=member.instrument_id
            WHERE cohort.cohort_id=%s AND member.channel='AM' AND instrument.asset_class='EQUITY')
        GROUP BY reference.security_id,reference.ticker ORDER BY reference.ticker
    """, (cohort,))
    window = (cohort, session_date, session_date, session_date)
    historical_evidence_counts = query_rows(cursor, "WITH " + _SESSION_WINDOWS + """
        SELECT windows.interval,COUNT(DISTINCT windows.source_window_id) AS closures,
            COUNT(evidence.evidence_id) AS evidence_rows
        FROM windows LEFT JOIN rt.equity_evidence AS evidence ON evidence.cohort_id=windows.cohort_id
            AND evidence.source_window_id=windows.source_window_id
        GROUP BY windows.interval ORDER BY windows.interval
    """, window)
    cursor.execute("WITH " + _SESSION_WINDOWS + """
        SELECT COUNT(*) AS checked_sources,
            COUNT(*) FILTER (WHERE bar.bar_revision_id IS NULL) AS missing_sources,
            COUNT(*) FILTER (WHERE bar.bar_end>windows.market_time) AS future_market_sources,
            COUNT(*) FILTER (WHERE bar.observed_at>windows.observed_at) AS future_observation_sources
        FROM windows
        CROSS JOIN LATERAL unnest(windows.primary_bar_revision_ids||windows.confirmation_bar_revision_ids) AS source(id)
        LEFT JOIN rt.bar_catalog AS bar ON bar.bar_revision_id=source.id AND bar.cohort_id=windows.cohort_id
    """, window)
    historical_lineage = dict(cursor.fetchone())
    cursor.execute("""
        SELECT COUNT(*) AS pointer_count,
            COUNT(*) FILTER (WHERE evidence.evidence_id IS NULL
                OR pointer.payload_sha256<>evidence.payload_sha256 OR pointer.market_time<>evidence.market_time
                OR pointer.observed_at<>evidence.observed_at OR pointer.interval IS DISTINCT FROM evidence.interval
                OR pointer.model_variant<>evidence.source_version) AS mismatches
        FROM rt.equity_evidence_current AS pointer
        LEFT JOIN rt.evidence_catalog AS catalog ON catalog.cohort_id=pointer.cohort_id
            AND catalog.evidence_id=pointer.evidence_id
        LEFT JOIN rt.equity_evidence AS evidence ON evidence.cohort_id=catalog.cohort_id
            AND evidence.evidence_id=catalog.evidence_id AND evidence.durable_at=catalog.durable_at
        WHERE pointer.cohort_id=%s
    """, (cohort,))
    evidence_pointers = dict(cursor.fetchone())
    current_releases = query_rows(cursor, """
        SELECT pointer.provider_tier,pointer.release_id::text,release.release_class,pointer.session_date,
            pointer.cutoff,release.manifest_sha256,pointer.updated_at,release.release_id IS NOT NULL AS release_present
        FROM rt.portal_release_current AS pointer
        LEFT JOIN rt.portal_releases AS release ON release.release_id=pointer.release_id AND release.cutoff=pointer.cutoff
        WHERE pointer.cohort_id=%s ORDER BY pointer.provider_tier
    """, (cohort,))
    current_screening = query_rows(cursor, """
        SELECT generation.generation_id::text,generation.status,generation.expected_members,generation.actual_members,
            generation.session_date,generation.generated_at,generation.provider_tier
        FROM rt.screening_current AS pointer
        JOIN rt.screening_generations AS generation ON generation.generation_id=pointer.generation_id
            AND generation.cohort_id=pointer.cohort_id
        WHERE pointer.cohort_id=%s
    """, (cohort,))
    latest_sessions = query_rows(cursor, _COHORT_MEMBERS + """
        SELECT tickers.ticker,bar.interval,bar.adjustment_basis,MAX(catalog.session_date) AS latest_session_date,
            MAX(bar.bar_end) AS latest_bar_end,COUNT(*) AS retained_current_bar_count
        FROM tickers JOIN rt.bar_current AS bar ON bar.cohort_id=%s AND bar.instrument_id=tickers.instrument_id
        JOIN rt.bar_catalog AS catalog ON catalog.bar_revision_id=bar.bar_revision_id AND catalog.cohort_id=bar.cohort_id
        GROUP BY tickers.ticker,bar.interval,bar.adjustment_basis
        ORDER BY tickers.ticker,bar.interval,bar.adjustment_basis
    """, (cohort, cohort))
    interval_counts = query_rows(cursor, _COHORT_MEMBERS + """
        SELECT tickers.ticker,bar.interval,bar.adjustment_basis,COUNT(*) AS count,
            (ARRAY_AGG(revision.close_price::float8 ORDER BY bar.bar_end DESC))[1] AS latest_close,
            MAX(bar.bar_end) AS latest_bar_end,ARRAY_AGG(DISTINCT bar.source_kind ORDER BY bar.source_kind) AS sources
        FROM tickers JOIN rt.bar_current AS bar ON bar.cohort_id=%s AND bar.instrument_id=tickers.instrument_id
        JOIN rt.bar_catalog AS catalog ON catalog.bar_revision_id=bar.bar_revision_id AND catalog.cohort_id=bar.cohort_id
            AND catalog.session_date=%s
        JOIN rt.bar_revisions AS revision ON revision.bar_revision_id=bar.bar_revision_id
            AND revision.observed_at=bar.observed_at
        GROUP BY tickers.ticker,bar.interval,bar.adjustment_basis
        ORDER BY tickers.ticker,bar.interval,bar.adjustment_basis
    """, (cohort, cohort, session_date))
    reference_integrity = by_reference_integrity(cursor, cohort)
    return {
        "database": control["database"],
        "lineage": control["lineage"],
        "cohort_id": cohort,
        "session_date": session_date.isoformat(),
        "checked_at": control["checked_at"].isoformat(),
        "transaction_read_only": control["read_only"],
        "reference_membership": reference_membership,
        "reference_validity": reference_validity,
        "historical_evidence_counts": historical_evidence_counts,
        "historical_lineage": historical_lineage,
        "evidence_pointers": evidence_pointers,
        "current_releases": current_releases,
        "current_screening": current_screening,
        "latest_retained_sessions": latest_sessions,
        "interval_counts": interval_counts,
        "daily_scale": _scale(interval_counts),
        "by_reference_integrity": reference_integrity,
        "database_writes": 0,
        "provider_requests": 0,
        "values_exposed": False,
    }


def run_audit(connection, *, database: str, cohort_id: UUID, session_date: date):
    with read_only_transaction(connection, database) as (cursor, control):
        return build_report(cursor, control, cohort_id=cohort_id, session_date=session_date)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Read-only V2 audit of retained ticker history inputs.")
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--allow-disposable", action="store_true")
    parser.add_argument("--cohort-id", required=True, type=UUID)
    parser.add_argument("--session-date", required=True, type=date.fromisoformat)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--report-path", type=Path)
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name, allow_disposable=args.allow_disposable)
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    report_path = require_new_workspace_report(args.report_path, WORKSPACE_DIR) if args.report_path else None
    settings = admin_settings(args.env_file)
    with closing(connect(args.database, settings, application_name="audit_realtime_ticker_history")) as connection:
        report = run_audit(connection, database=args.database, cohort_id=args.cohort_id,
                           session_date=args.session_date)
    report = json.loads(json.dumps(report, default=str))
    if report_path is not None:
        write_report(report_path, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
