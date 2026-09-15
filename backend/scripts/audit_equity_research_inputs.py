"""Audit retained daily research inputs without fetching, training or database writes."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, timedelta
import json
from pathlib import Path
import sys
from uuid import NAMESPACE_URL, uuid5

import exchange_calendars
import pandas as pd
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor
from equity.historical_universe import (
    HistoricalUniversePolicy, grouped_daily_rows, historical_universe_run_id, select_historical_members,
)
from equity.historical_prices import read_historical_prices, reread_historical_prices
from equity.historical_actions import read_historical_actions, reread_historical_actions
from equity.polygon import normalize_security_reference, sha256_json
from research.gics_sectors import BROAD_MARKET_ETF, ALTERNATE_MARKET_ETF, SECTOR_BENCHMARK_ETF


BENCHMARKS = tuple(sorted({BROAD_MARKET_ETF, ALTERNATE_MARKET_ETF, *SECTOR_BENCHMARK_ETF.values()}))


def summarize_universes(rows, expected_policy_hash):
    if not rows:
        return {"runs": 0, "blockers": ["NO_HISTORICAL_UNIVERSES"]}
    dates = [row["session_date"] for row in rows]
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = {value.date() for value in calendar.sessions_in_range(min(dates), max(dates))}
    observed = set(dates)
    checks = {
        "DUPLICATE_UNIVERSE_SESSION": len(dates) - len(observed),
        "MISSING_UNIVERSE_SESSION": len(sessions - observed),
        "NON_SESSION_UNIVERSE": len(observed - sessions),
        "UNIVERSE_POLICY_MISMATCH": sum(row["policy_sha256"].strip() != expected_policy_hash for row in rows),
        "INCOMPLETE_UNIVERSE": sum(row["status"] != "COMPLETE" for row in rows),
        "UNIVERSE_MEMBER_COUNT_MISMATCH": sum(row["members"] != row["admitted_members"] for row in rows),
        "UNIVERSE_AVAILABILITY_INVALID": sum(not row["availability_valid"] for row in rows),
    }
    return {
        "runs": len(rows), "sessions": len(observed),
        "first_session": min(dates).isoformat(), "last_session": max(dates).isoformat(),
        "memberships": sum(row["members"] for row in rows),
        "minimum_members": min(row["members"] for row in rows),
        "maximum_members": max(row["members"] for row in rows),
        "policy_hashes": sorted({row["policy_sha256"].strip() for row in rows}),
        "missing_session_examples": [value.isoformat() for value in sorted(sessions - observed)[:20]],
        "member_count_mismatch_examples": [
            {"session": row["session_date"].isoformat(), "declared": row["admitted_members"],
             "stored": row["members"]}
            for row in rows if row["members"] != row["admitted_members"]
        ][:20],
        "checks": checks, "blockers": [name for name, count in checks.items() if count],
    }


def audit_price_coverage(cursor, policy_version, adjusted, first_session, last_session, data_end):
    calendar = exchange_calendars.get_calendar("XNYS")
    first = pd.Timestamp(first_session)
    warmup_start = calendar.sessions_window(first, -253)[0].date()
    sessions = [value.date() for value in calendar.sessions_in_range(warmup_start, data_end)]
    cursor.execute(
        """
        WITH sessions AS (
            SELECT session_date, ordinal FROM unnest(%s::DATE[]) WITH ORDINALITY AS calendar(session_date, ordinal)
        ), bars AS (
            SELECT DISTINCT ON (bar.ticker, bar.session_date)
                bar.ticker, bar.session_date, bar.security_id, sessions.ordinal,
                bar.bar_end, bar.replay_available_at
            FROM equity_bar_revisions AS bar JOIN sessions USING (session_date)
            WHERE bar.availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND bar.interval = '1d' AND bar.session_scope = 'RTH' AND bar.is_final
              AND bar.adjusted = %s AND bar.quality_codes @>
                  ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
            ORDER BY bar.ticker, bar.session_date, bar.replay_available_at DESC, bar.created_at DESC
        ), paths AS (
            SELECT *, lag(ordinal, 252) OVER series = ordinal - 252 AS warmup_252,
                lead(ordinal, 5) OVER series = ordinal + 5 AS path_5,
                lead(ordinal, 10) OVER series = ordinal + 10 AS path_10,
                lead(ordinal, 21) OVER series = ordinal + 21 AS path_21
            FROM bars WINDOW series AS (PARTITION BY ticker, security_id ORDER BY session_date)
        )
        SELECT run.effective_from::DATE AS session_date,
            COUNT(*) AS memberships,
            COUNT(*) FILTER (WHERE paths.ticker IS NOT NULL) AS with_signal_bar,
            COUNT(*) FILTER (WHERE paths.ticker IS NOT NULL AND paths.security_id <> member.security_id) AS security_identity_mismatch,
            COUNT(*) FILTER (WHERE paths.replay_available_at IS NULL OR paths.replay_available_at <> paths.bar_end) AS missing_or_invalid_signal_availability,
            COUNT(*) FILTER (WHERE paths.warmup_252) AS with_contiguous_warmup_252,
            COUNT(*) FILTER (WHERE paths.path_5) AS with_contiguous_forward_5,
            COUNT(*) FILTER (WHERE paths.path_10) AS with_contiguous_forward_10,
            COUNT(*) FILTER (WHERE paths.path_21) AS with_contiguous_forward_21,
            COUNT(*) FILTER (WHERE paths.warmup_252 AND paths.path_21 AND paths.security_id = member.security_id) AS feature_and_forward_21
        FROM equity_original_universe_runs AS run
        JOIN equity_universe_members AS member USING (universe_run_id)
        LEFT JOIN paths ON paths.ticker = member.ticker AND paths.session_date = run.effective_from::DATE
        WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED' AND run.policy_version = %s
        GROUP BY run.effective_from::DATE ORDER BY session_date
        """, (sessions, adjusted, policy_version),
    )
    coverage = [dict(row) for row in cursor.fetchall()]
    summary = summarize_price_coverage(coverage, sessions, date.fromisoformat(last_session))
    cursor.execute(
        """
        SELECT ticker, COUNT(DISTINCT session_date) AS sessions,
            MIN(session_date) AS first_session, MAX(session_date) AS last_session,
            COUNT(DISTINCT session_date) FILTER (WHERE session_date >= %s) AS study_sessions,
            COUNT(*) - COUNT(DISTINCT session_date) AS additional_revisions
        FROM equity_bar_revisions
        WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED' AND interval = '1d'
          AND session_scope = 'RTH' AND is_final AND adjusted = %s
          AND quality_codes @> ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
          AND ticker = ANY(%s) AND session_date BETWEEN %s AND %s
        GROUP BY ticker ORDER BY ticker
        """, (first_session, adjusted, list(BENCHMARKS), warmup_start, data_end),
    )
    benchmarks = [dict(row) for row in cursor.fetchall()]
    expected_study_sessions = sum(value >= first.date() for value in sessions)
    missing = set(BENCHMARKS) - {row["ticker"] for row in benchmarks}
    summary["benchmarks"] = {
        "rows": benchmarks, "expected_study_sessions": expected_study_sessions,
        "missing_or_incomplete": sorted(missing | {
            row["ticker"] for row in benchmarks if row["study_sessions"] != expected_study_sessions
        }),
    }
    summary["adjusted"] = adjusted
    summary["warmup_start_required"] = warmup_start.isoformat()
    cursor.execute(
                """
                SELECT member.ticker, member.security_id AS membership_security_id,
                        price.security_id AS bar_security_id,
                        original.company_name AS membership_company,
                        original.cik AS membership_cik, original.composite_figi AS membership_figi,
                        original.share_class_figi AS membership_share_class_figi,
                        current_ref.company_name AS bar_company, current_ref.cik AS bar_cik,
                        current_ref.composite_figi AS bar_figi,
                        current_ref.share_class_figi AS bar_share_class_figi
                FROM equity_original_universe_runs AS run JOIN equity_universe_members AS member USING (universe_run_id)
                JOIN LATERAL (
                        SELECT security_id FROM equity_bar_revisions AS bar
                        WHERE bar.ticker = member.ticker AND bar.session_date = run.effective_from::DATE
                            AND bar.interval = '1d' AND bar.session_scope = 'RTH' AND bar.is_final
                            AND bar.adjusted = %s AND bar.availability_mode = 'HISTORICAL_RECONSTRUCTED'
                            AND bar.quality_codes @> ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
                        ORDER BY bar.replay_available_at DESC, bar.created_at DESC LIMIT 1
                ) AS price ON price.security_id <> member.security_id
                LEFT JOIN equity_security_reference_revisions AS original
                    ON original.security_revision_id = member.security_revision_id
                LEFT JOIN LATERAL (
                        SELECT company_name, cik, composite_figi, share_class_figi
                        FROM equity_security_reference_revisions AS reference
                        WHERE reference.security_id = price.security_id AND reference.ticker = member.ticker
                        ORDER BY effective_from DESC, observed_at DESC LIMIT 1
                ) AS current_ref ON TRUE
                WHERE run.policy_version = %s AND run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
                    AND run.effective_from::DATE = %s
                ORDER BY member.ticker LIMIT 12
                """, (adjusted, policy_version, first_session),
    )
    summary["first_session_identity_mismatch_examples"] = [dict(row) for row in cursor.fetchall()]
    return summary


def summarize_price_coverage(rows, sessions, universe_end):
    ordinals = {session: index for index, session in enumerate(sessions)}
    last = len(sessions) - 1
    metrics = [key for key in rows[0] if key != "session_date"] if rows else []
    totals = {key: sum(row[key] for row in rows) for key in metrics}
    maturity = {}
    for horizon in (5, 10, 21):
        mature = [row for row in rows if ordinals[row["session_date"]] + horizon <= last]
        possible = sum(row["memberships"] for row in mature)
        present = sum(row[f"with_contiguous_forward_{horizon}"] for row in mature)
        maturity[str(horizon)] = {"mature_memberships": possible,
                                  "missing_forward_paths": possible - present,
                                  "immature_memberships": totals.get("memberships", 0) - possible}
    return {"totals": totals, "maturity": maturity,
            "first_session_coverage": rows[0] if rows else None,
            "last_session_coverage": rows[-1] if rows else None,
            "first_session_90pct_warmup": next((row["session_date"].isoformat() for row in rows
                if row["with_contiguous_warmup_252"] >= row["memberships"] * 0.90), None),
            "universe_end": universe_end.isoformat()}


def audit_reference_actions(cursor, policy_version, first_session, data_end):
    cursor.execute(
        """
        WITH members AS (
            SELECT DISTINCT member.security_id, member.ticker,
                MIN(run.effective_from) AS first_membership, MAX(run.effective_from) AS last_membership
            FROM equity_original_universe_runs AS run JOIN equity_universe_members AS member USING (universe_run_id)
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED' AND run.policy_version = %s
            GROUP BY member.security_id, member.ticker
        )
        SELECT COUNT(*) AS union_identities,
            COUNT(*) FILTER (WHERE sector.security_revision_id IS NOT NULL) AS dated_effective_sector,
            COUNT(*) FILTER (WHERE sector.source_as_of_date IS NOT NULL) AS sector_with_source_as_of_date,
            COUNT(*) FILTER (WHERE latest.active = FALSE OR latest.delisted_date IS NOT NULL) AS inactive_or_delisted_references,
            COUNT(*) FILTER (WHERE latest.security_revision_id IS NULL) AS missing_reference,
            COUNT(*) FILTER (WHERE NOT EXISTS (
                SELECT 1 FROM equity_bar_revisions AS bar WHERE bar.security_id = members.security_id
                  AND bar.ticker = members.ticker AND bar.availability_mode = 'HISTORICAL_RECONSTRUCTED'
                  AND bar.interval = '1d' AND bar.is_final
            )) AS identities_without_any_historical_daily_bar
        FROM members
        LEFT JOIN LATERAL (
            SELECT security_revision_id, source_as_of_date
            FROM equity_security_reference_revisions AS reference
            WHERE reference.security_id = members.security_id AND reference.ticker = members.ticker
              AND reference.effective_from <= members.first_membership AND reference.sector IS NOT NULL
            ORDER BY reference.effective_from DESC, reference.observed_at DESC LIMIT 1
        ) AS sector ON TRUE
        LEFT JOIN LATERAL (
            SELECT security_revision_id, active, delisted_date
            FROM equity_security_reference_revisions AS reference
            WHERE reference.security_id = members.security_id AND reference.ticker = members.ticker
            ORDER BY reference.effective_from DESC, reference.observed_at DESC LIMIT 1
        ) AS latest ON TRUE
        """, (policy_version,),
    )
    references = dict(cursor.fetchone())
    cursor.execute(
        """
        SELECT action_type, COUNT(*) AS actions, COUNT(DISTINCT ticker) AS tickers,
            MIN(effective_date) AS first_date, MAX(effective_date) AS last_date,
            COUNT(*) FILTER (WHERE replay_available_at IS NULL) AS missing_replay_time
        FROM equity_corporate_actions
        WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED' AND effective_date <= %s
        GROUP BY action_type ORDER BY action_type
        """, (data_end,),
    )
    actions = [dict(row) for row in cursor.fetchall()]
    cursor.execute(
        """
        SELECT availability_mode, action_type, COUNT(*) AS windows,
            MIN(window_start) AS first_date, MAX(window_end) AS last_date
        FROM equity_corporate_action_coverage WHERE window_start <= %s AND window_end >= %s
        GROUP BY availability_mode, action_type ORDER BY availability_mode, action_type
        """, (data_end, first_session),
    )
    coverage = [dict(row) for row in cursor.fetchall()]
    return {"references": references, "historical_action_inventory": actions,
            "action_coverage_windows_overlapping_study": coverage,
            "caveats": ["Dated effective sector is not independently verified provider-as-known history.",
                        "Recorded actions do not prove complete negative coverage or delisting settlement returns.",
                        "Split-adjusted prices are not dividend-inclusive total returns."]}


def audit_inputs(policy_version="liquid_us_common_stocks_v2"):
    expected_hash = HistoricalUniversePolicy(policy_version=policy_version).policy_sha256
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '180s'")
        cursor.execute("SELECT current_setting('transaction_read_only') AS read_only, transaction_timestamp() AS cutoff")
        transaction = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT run.universe_run_id, run.effective_from::DATE AS session_date,
                   run.policy_sha256, run.status, run.admitted_members,
                   COUNT(member.ticker)::INTEGER AS members,
                   (run.replay_available_at IS NOT NULL
                    AND run.replay_available_at >= run.effective_from
                    AND run.replay_available_at <= run.observed_at
                    AND run.source_request_sha256 IS NOT NULL) AS availability_valid
            FROM equity_original_universe_runs AS run
            LEFT JOIN equity_universe_members AS member USING (universe_run_id)
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND run.policy_version = %s
            GROUP BY run.universe_run_id, run.effective_from, run.policy_sha256,
                     run.status, run.admitted_members, run.replay_available_at,
                     run.observed_at, run.source_request_sha256
            ORDER BY session_date, run.universe_run_id
            """, (policy_version,),
        )
        universes = summarize_universes([dict(row) for row in cursor.fetchall()], expected_hash)
        cursor.execute(
            """
            SELECT adjusted, COUNT(*) AS revisions,
                   COUNT(DISTINCT (ticker, session_date)) AS ticker_sessions,
                   COUNT(DISTINCT ticker) AS tickers,
                   MIN(session_date) AS first_session, MAX(session_date) AS last_session,
                   COUNT(*) FILTER (WHERE replay_available_at IS NULL
                     OR replay_available_at <> bar_end) AS invalid_replay_availability,
                   COUNT(*) FILTER (WHERE NOT quality_codes @>
                     ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]) AS non_exact_case_revisions
            FROM equity_bar_revisions
            WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND interval = '1d' AND session_scope = 'RTH' AND is_final = TRUE
            GROUP BY adjusted ORDER BY adjusted
            """,
        )
        lineages = [dict(row) for row in cursor.fetchall()]
        price_coverage = []
        reference_actions = {}
        if universes["runs"] and lineages:
            for lineage in lineages:
                price_coverage.append(audit_price_coverage(
                    cursor, policy_version, lineage["adjusted"], universes["first_session"],
                    universes["last_session"], lineage["last_session"],
                ))
            reference_actions = audit_reference_actions(
                cursor, policy_version, universes["first_session"],
                max(row["last_session"] for row in lineages),
            )
    return {
        "audit_version": "equity_research_input_audit_v1", "policy_version": policy_version,
        "expected_policy_sha256": expected_hash, "transaction": transaction,
        "universe": universes, "bar_lineages": lineages,
        "price_coverage": price_coverage, "reference_actions": reference_actions,
        "study_settings_status": "NOT_FROZEN", "readiness": "NOT_CERTIFIED",
        "unverified": ["DATED_SECTOR_PROVENANCE", "DIVIDEND_INCLUSIVE_OUTCOME_IMPLEMENTATION",
                   "COMPLETE_ACTION_AND_DELISTING_SETTLEMENT_COVERAGE",
                   "HISTORICAL_REPLAY_VS_LIVE_AVAILABILITY_EQUIVALENCE"],
    }


def classify_identity_pair(row):
    def cik(value):
        text = str(value or "").strip()
        return text.lstrip("0") or None if text.isdigit() else None

    original_cik, bar_cik = cik(row.get("membership_cik")), cik(row.get("bar_cik"))
    original_class = str(row.get("membership_share_class_figi") or "").strip()
    bar_class = str(row.get("bar_share_class_figi") or "").strip()
    original_type, bar_type = row.get("membership_security_type"), row.get("bar_security_type")
    if (original_cik and bar_cik and original_cik != bar_cik) or (
        original_class and bar_class and original_class != bar_class
    ) or (original_type and bar_type and original_type != bar_type):
        return "ISSUER_OR_SHARE_CLASS_CONFLICT"
    if original_cik and original_cik == bar_cik and original_class and original_class == bar_class \
            and original_type == bar_type == "CS":
        return "IDENTIFIER_DRIFT_CANDIDATE"
    return "INSUFFICIENT_IDENTITY_EVIDENCE"


def replay_symbol_change_candidate(reference_rows, daily_history, *, signal_date, prior_sessions, policy, transition):
    change = transition["symbol_change"]
    if signal_date.isoformat() != change["effective_session"]:
        raise ValueError("symbol-change candidate is restricted to its exact effective session")
    if not transition.get("transition_version") or not transition.get("sources"):
        raise ValueError("symbol-change candidate requires versioned source evidence")
    previous, current = change["previous_ticker"], change["new_ticker"]
    if previous == current:
        raise ValueError("symbol change must have distinct tickers")
    merger = transition["merger"]
    if (merger["successor_cik"] != change["cik"] or merger["predecessor_ticker"] != current
            or date.fromisoformat(merger["effective_date"]) > signal_date):
        raise ValueError("merger and symbol-change evidence are inconsistent")
    selected = {ticker: [row for row in reference_rows if row.get("ticker") == ticker]
                for ticker in (previous, current)}
    if any(len(rows) != 1 for rows in selected.values()):
        raise ValueError("symbol-change source must contain each ticker exactly once")
    old_reference, new_reference = selected[previous][0], selected[current][0]
    for reference in (old_reference, new_reference):
        if reference.get("type") != "CS" or not reference.get("active"):
            raise ValueError("symbol-change candidate requires active common-stock references")
        for field in ("composite_figi", "share_class_figi"):
            if not change.get(field) or reference.get(field) != change[field]:
                raise ValueError("symbol-change source identifiers do not match reviewed evidence")
    if old_reference.get("cik") != change["cik"] or new_reference.get("cik") not in (
        change["cik"], transition["merger"]["predecessor_cik"],
    ):
        raise ValueError("symbol-change issuer does not match reviewed evidence")
    if any(session >= signal_date for session in prior_sessions):
        raise ValueError("symbol-change eligibility must use preceding sessions")
    calendar = exchange_calendars.get_calendar("XNYS")
    expected_sessions = [value.date() for value in calendar.sessions_window(
        calendar.previous_session(pd.Timestamp(signal_date)), -policy.lookback_sessions,
    )]
    if sorted(prior_sessions) != expected_sessions:
        raise ValueError("symbol-change candidate requires the complete preceding XNYS window")
    corrected_reference = dict(old_reference, ticker=current)
    corrected_references = [dict(row) for row in reference_rows if row.get("ticker") not in (previous, current)]
    corrected_references.append(corrected_reference)
    history = {}
    for session in prior_sessions:
        history[session] = {ticker: dict(values) for ticker, values in daily_history.get(session, {}).items()}
        history[session].pop(current, None)
        if previous in history[session]:
            history[session][current] = dict(history[session][previous])
        history[session].pop(previous, None)
    selection = select_historical_members(
        corrected_references, history, signal_date=signal_date, prior_sessions=prior_sessions, policy=policy,
    )
    members = [{"ticker": member.ticker, "rank": rank,
                "latest_price": str(member.latest_price), "median_dollar_volume": str(member.median_dollar_volume),
                "observed_sessions": member.observed_sessions}
               for rank, member in enumerate(selection.members, start=1)]
    inputs = {"transition_sha256": sha256_json(transition), "source_reference_sha256": sha256_json(reference_rows),
              "source_history_sha256": sha256_json({session.isoformat(): daily_history.get(session, {}) for session in prior_sessions}),
              "policy_sha256": policy.policy_sha256, "session": signal_date.isoformat()}
    digest = sha256_json({"version": "symbol_change_candidate_v1", "inputs": inputs, "members": members})
    return {
        "candidate_version": "symbol_change_candidate_v1", "candidate_sha256": digest,
        "proposed_universe_run_id": str(uuid5(NAMESPACE_URL, f"historical-universe-correction:{digest}")),
        "inputs": inputs, "members": len(members), "member_manifest_sha256": sha256_json(members),
        "successor_member": next((row for row in members if row["ticker"] == current), None),
        "removed_reference_tickers": [previous], "corrected_reference": corrected_reference,
        "history_source_ticker": previous, "original_inputs_preserved": True,
        "candidate_status": "REVIEW_ONLY", "repair_authorized": False,
    }


def review_membership_source(row, cache_dir, transition=None):
    row = dict(row)
    tickers = row.pop("stored_tickers")
    ranks = row.pop("stored_ranks")
    result = dict(row)
    result["missing_member_ranks"] = sorted(set(range(1, row["admitted_members"] + 1)) - set(ranks))
    result["stored_members_reproduce_run_id"] = historical_universe_run_id(
        signal_date=row["session_date"], policy_sha256=row["policy_sha256"],
        source_request_sha256=row["source_request_sha256"], member_tickers=tickers,
    ) == row["universe_run_id"]
    path = cache_dir / f"tickers-CS_{row['session_date'].isoformat()}.json"
    if not path.is_file():
        return result | {"source_cache_status": "NOT_RETAINED", "repair_authorized": False}
    document = json.loads(path.read_text(encoding="utf-8"))
    payload = document.get("payload")
    if not isinstance(payload, list) or sha256_json(payload) != document.get("sha256"):
        return result | {"source_cache_status": "CHECKSUM_INVALID", "repair_authorized": False}
    if sha256_json(payload) != row["source_request_sha256"]:
        return result | {"source_cache_status": "REQUEST_HASH_MISMATCH", "repair_authorized": False}
    identities = {}
    for reference in payload:
        revision = normalize_security_reference(
            reference, observed_at=row["observed_at"], source_as_of_date=row["session_date"],
        )
        identities.setdefault(str(revision.security_id), set()).add(revision.ticker)
    collisions = [
        {"security_id": identity, "source_tickers": sorted(names),
         "stored_tickers": sorted(names.intersection(tickers))}
        for identity, names in sorted(identities.items())
        if len(names) > 1 and names.intersection(tickers)
    ]
    result |= {"source_cache_status": "VERIFIED", "reference_identity_collisions": collisions,
               "repair_authorized": False}
    policy = HistoricalUniversePolicy()
    if policy.policy_sha256 != row["policy_sha256"]:
        return result | {"eligibility_replay_status": "UNSUPPORTED_POLICY"}
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = [value.date() for value in calendar.sessions_in_range(
        pd.Timestamp(row["session_date"]) - pd.Timedelta(days=60),
        pd.Timestamp(row["session_date"]),
    )][:-1][-policy.lookback_sessions:]
    history = {}
    for session in sessions:
        grouped_path = cache_dir / f"grouped-unadjusted_{session.isoformat()}.json"
        if not grouped_path.is_file():
            return result | {"eligibility_replay_status": "GROUPED_CACHE_NOT_RETAINED"}
        grouped = json.loads(grouped_path.read_text(encoding="utf-8"))
        daily = grouped.get("payload")
        if not isinstance(daily, list) or sha256_json(daily) != grouped.get("sha256"):
            return result | {"eligibility_replay_status": "GROUPED_CHECKSUM_INVALID"}
        history[session] = grouped_daily_rows(daily)
    replay = select_historical_members(
        payload, history, signal_date=row["session_date"], prior_sessions=sessions, policy=policy,
    )
    replay_tickers = [member.ticker for member in replay.members]
    reproduced_id = historical_universe_run_id(
        signal_date=row["session_date"], policy_sha256=row["policy_sha256"],
        source_request_sha256=row["source_request_sha256"], member_tickers=replay_tickers,
    )
    result |= {
        "eligibility_replay_status": "REPRODUCED" if reproduced_id == row["universe_run_id"] else "RUN_ID_MISMATCH",
        "replayed_members": len(replay.members),
        "missing_from_storage": [
            {"ticker": member.ticker, "member_rank": rank}
            for rank, member in enumerate(replay.members, start=1) if member.ticker not in tickers
        ],
        "extra_in_storage": sorted(set(tickers) - set(replay_tickers)),
    }
    if transition is not None and reproduced_id == row["universe_run_id"]:
        candidate = replay_symbol_change_candidate(
            payload, history, signal_date=row["session_date"], prior_sessions=sessions, policy=policy, transition=transition,
        )
        result["correction_candidate"] = candidate | {"original_universe_run_id": str(row["universe_run_id"])}
    return result


def review_identities(policy_version, cache_dir):
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '180s'")
        cursor.execute("SELECT transaction_timestamp() AS cutoff, current_setting('transaction_read_only') AS read_only")
        transaction = dict(cursor.fetchone())
        cursor.execute(
            """
            WITH members AS MATERIALIZED (
                SELECT member.ticker, member.security_id, member.security_revision_id,
                       run.effective_from::DATE AS session_date
                FROM equity_original_universe_runs AS run JOIN equity_universe_members AS member USING (universe_run_id)
                WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED' AND run.policy_version = %s
            ), bars AS MATERIALIZED (
                SELECT DISTINCT ON (ticker, session_date) ticker, session_date, security_id
                FROM equity_bar_revisions
                WHERE availability_mode = 'HISTORICAL_RECONSTRUCTED' AND interval = '1d'
                  AND session_scope = 'RTH' AND adjusted = TRUE AND is_final
                  AND quality_codes @> ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::TEXT[]
                  AND session_date BETWEEN (SELECT MIN(session_date) FROM members)
                                       AND (SELECT MAX(session_date) FROM members)
                ORDER BY ticker, session_date, replay_available_at DESC, created_at DESC
            ), mismatches AS MATERIALIZED (
                SELECT member.*, bars.security_id AS bar_security_id
                FROM members AS member JOIN bars USING (ticker, session_date)
                WHERE member.security_id <> bars.security_id
            ), pairs AS MATERIALIZED (
                SELECT mismatch.ticker, mismatch.security_id AS membership_security_id,
                       mismatch.bar_security_id, reference.cik AS membership_cik,
                       reference.share_class_figi AS membership_share_class_figi,
                       reference.security_type AS membership_security_type,
                       reference.company_name AS membership_company,
                       MIN(mismatch.session_date) AS first_session,
                       MAX(mismatch.session_date) AS last_session,
                       COUNT(*) AS affected_member_dates
                FROM mismatches AS mismatch
                LEFT JOIN equity_security_reference_revisions AS reference USING (security_revision_id)
                GROUP BY mismatch.ticker, mismatch.security_id, mismatch.bar_security_id,
                         reference.cik, reference.share_class_figi, reference.security_type, reference.company_name
            )
            SELECT pairs.*, bar_reference.cik AS bar_cik,
                   bar_reference.share_class_figi AS bar_share_class_figi,
                   bar_reference.security_type AS bar_security_type,
                   bar_reference.company_name AS bar_company,
                   bar_reference.effective_from AS bar_reference_effective_from
            FROM pairs LEFT JOIN LATERAL (
                SELECT cik, share_class_figi, security_type, company_name, effective_from
                FROM equity_security_reference_revisions AS reference
                WHERE reference.security_id = pairs.bar_security_id AND reference.ticker = pairs.ticker
                ORDER BY effective_from DESC, observed_at DESC LIMIT 1
            ) AS bar_reference ON TRUE
            ORDER BY pairs.ticker, pairs.membership_security_id, pairs.bar_security_id,
                     pairs.first_session, pairs.membership_company
            """, (policy_version,),
        )
        pairs = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            """
            SELECT run.universe_run_id, run.effective_from::DATE AS session_date,
                   run.admitted_members, run.policy_sha256, run.source_request_sha256, run.observed_at,
                   COUNT(member.ticker) AS stored_members,
                   COALESCE(array_agg(member.ticker ORDER BY member.ticker)
                       FILTER (WHERE member.ticker IS NOT NULL), ARRAY[]::TEXT[]) AS stored_tickers,
                   COALESCE(array_agg(member.member_rank ORDER BY member.member_rank)
                       FILTER (WHERE member.member_rank IS NOT NULL), ARRAY[]::INTEGER[]) AS stored_ranks
            FROM equity_original_universe_runs AS run LEFT JOIN equity_universe_members AS member USING (universe_run_id)
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED' AND run.policy_version = %s
            GROUP BY run.universe_run_id, run.effective_from, run.admitted_members,
                     run.policy_sha256, run.source_request_sha256, run.observed_at
            HAVING COUNT(member.ticker) <> run.admitted_members
            ORDER BY session_date
            """, (policy_version,),
        )
        memberships = [review_membership_source(dict(row), cache_dir) for row in cursor.fetchall()]
    for row in pairs:
        row["classification"] = classify_identity_pair(row)
        row["repair_authorized"] = False
    counts = Counter(row["classification"] for row in pairs)
    affected = Counter()
    for row in pairs:
        affected[row["classification"]] += row["affected_member_dates"]
    report = {"review_version": "historical_identity_review_v1", "policy_version": policy_version,
              "transaction": transaction, "pair_groups": len(pairs),
              "classification_counts": dict(counts), "affected_member_dates": dict(affected),
              "identity_pairs": pairs, "membership_discrepancies": memberships,
              "readiness": "NOT_CERTIFIED", "repair_status": "REVIEW_ONLY",
              "limitations": ["Latest reference metadata is diagnostic, not proof of historical continuity.",
                              "Matching CIK and share-class FIGI is a review candidate, not automatic relink permission.",
                              "Date bounds do not authorize filling intervening dates or crossing issuer transitions."]}
    report["review_sha256"] = sha256_json(json.loads(json.dumps(report, default=str)))
    return report


def review_transition_prices(transition, membership, cache_dir):
    candidate = membership.get("correction_candidate")
    if candidate is None:
        return {"status": "BLOCKED", "reason": "ORIGINAL_INPUT_REPLAY_NOT_VERIFIED"}
    change = transition["symbol_change"]
    session = date.fromisoformat(change["effective_session"])
    calendar = exchange_calendars.get_calendar("XNYS")
    prior_sessions = [value.date() for value in calendar.sessions_window(calendar.previous_session(pd.Timestamp(session)), -20)]
    identity_map = {}
    reference_hashes = {}
    try:
        successor = normalize_security_reference(
            candidate["corrected_reference"], observed_at=membership["observed_at"], source_as_of_date=session,
        ).security_id
        for current in prior_sessions + [session]:
            path = cache_dir / f"tickers-CS_{current.isoformat()}.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            payload = document.get("payload")
            if not isinstance(payload, list) or sha256_json(payload) != document.get("sha256"):
                raise ValueError(f"invalid dated reference checksum: {current}")
            reference_hashes[current.isoformat()] = document["sha256"]
            ticker = change["previous_ticker"] if current < session else change["new_ticker"]
            matches = [row for row in payload if row.get("ticker") == ticker]
            if len(matches) != 1:
                raise ValueError(f"missing or ambiguous dated reference: {current} {ticker}")
            reference = matches[0]
            if current == session:
                if document["sha256"] != candidate["inputs"]["source_reference_sha256"]:
                    raise ValueError("transition reference no longer matches reviewed candidate")
                reference = candidate["corrected_reference"]
            if reference.get("type") != "CS" or any(reference.get(field) != change[field]
                for field in ("cik", "composite_figi", "share_class_figi")):
                raise ValueError(f"dated issuer/share-class continuity unresolved: {current} {ticker}")
            identity = normalize_security_reference(reference, observed_at=membership["observed_at"], source_as_of_date=current).security_id
            if identity != successor:
                raise ValueError(f"dated security identity drift requires reviewed correction: {current}")
            identity_map[(ticker, current)] = identity
    except (ValueError, OSError) as error:
        return {"status": "BLOCKED", "reason": "DATED_REFERENCE_UNRESOLVED", "detail": str(error)}
    evidence = {"transition_sha256": sha256_json(transition), "candidate_sha256": candidate["candidate_sha256"],
                "reference_payload_hashes": reference_hashes}
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '180s'")
        cursor.execute("SELECT transaction_timestamp() AS cutoff, current_setting('transaction_read_only') AS read_only")
        transaction = dict(cursor.fetchone())
        try:
            result = read_historical_prices(
                cursor, identity_by_ticker_session=identity_map, adjusted=True, source_cutoff=transaction["cutoff"],
                identity_evidence_sha256=sha256_json(evidence),
            )
            reread_historical_prices(cursor, result.manifest)
        except ValueError as error:
            return {"status": "BLOCKED", "reason": "PRICE_LINEAGE_UNRESOLVED", "detail": str(error),
                    "transaction": transaction, "identity_evidence": evidence}
    frame = result.frames_by_security[successor]
    return {
        "status": "IDENTITY_ALIGNED_SLICE", "transaction": transaction, "identity_evidence": evidence,
        "price_manifest": result.manifest, "pinned_reread_matches": True, "sessions": len(frame),
        "security_id": str(successor), "source_ticker_counts": dict(Counter(frame["source_ticker"])),
        "last_warmup_close": str(frame.loc[prior_sessions[-1], "close"]),
        "signal_session_close": str(frame.loc[session, "close"]),
        "readiness": "NOT_CERTIFIED", "repair_authorized": False,
        "limitations": ["Identity alignment does not independently certify provider prices or as-known history.",
                        "This slice contains no old-DOC merger consideration or forward return path.",
                        "No strategy, outcome, universe correction or price revision was published."],
    }


def review_transition_actions(transition, price_review):
    if price_review.get("status") != "IDENTITY_ALIGNED_SLICE":
        return {"status": "BLOCKED", "reason": "PRICE_IDENTITY_REVIEW_REQUIRED"}
    manifest = price_review["price_manifest"]
    change = transition["symbol_change"]
    effective = date.fromisoformat(change["effective_session"])
    first = min(date.fromisoformat(row["session"]) for row in manifest["revisions"])
    scopes = [dict(ticker=change["previous_ticker"], security_id=price_review["security_id"],
                   window_start=first, window_end=effective - timedelta(days=1)),
              dict(ticker=change["new_ticker"], security_id=price_review["security_id"],
                   window_start=effective, window_end=effective)]
    results = []
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '180s'")
        cursor.execute("SELECT transaction_timestamp() AS cutoff, current_setting('transaction_read_only') AS read_only")
        transaction = dict(cursor.fetchone())
        for scope in scopes:
            for kind in ("SPLIT", "DIVIDEND"):
                try:
                    actions = read_historical_actions(cursor, scopes=[scope], action_types=[kind],
                                                       source_cutoff=transaction["cutoff"],
                                                       identity_evidence_sha256=manifest["identity_evidence_sha256"])
                    reread_historical_actions(cursor, actions.manifest)
                    results.append({"scope": scope, "action_type": kind, "status": "RESPONSE_BOUND",
                                    "manifest": actions.manifest, "pinned_reread_matches": True})
                except ValueError as error:
                    results.append({"scope": scope, "action_type": kind, "status": "BLOCKED", "detail": str(error)})
    return {"status": "BLOCKED", "transaction": transaction, "scope_results": results,
            "unsupported_completeness": ["SYMBOL_CHANGE", "MERGER", "SPINOFF", "OTHER"],
            "known_transition_sha256": sha256_json(transition), "readiness": "NOT_CERTIFIED", "repair_authorized": False,
            "limitations": ["Known SEC transition facts are positive evidence, not a complete event inventory.",
                            "SPLIT/DIVIDEND response coverage does not certify merger consideration or total returns.",
                            "Calendar-day identity between observed trading sessions is a reviewed transition scope, not a fabricated price."]}


def review_transition(transition, cache_dir, policy_version="liquid_us_common_stocks_v2", *, include_prices=False, include_actions=False):
    session = date.fromisoformat(transition["symbol_change"]["effective_session"])
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '180s'")
        cursor.execute("SELECT transaction_timestamp() AS cutoff, current_setting('transaction_read_only') AS read_only")
        transaction = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT run.universe_run_id, run.effective_from::DATE AS session_date,
                   run.admitted_members, run.policy_sha256, run.source_request_sha256, run.observed_at,
                   COUNT(member.ticker) AS stored_members,
                   COALESCE(array_agg(member.ticker ORDER BY member.ticker)
                       FILTER (WHERE member.ticker IS NOT NULL), ARRAY[]::TEXT[]) AS stored_tickers,
                   COALESCE(array_agg(member.member_rank ORDER BY member.member_rank)
                       FILTER (WHERE member.member_rank IS NOT NULL), ARRAY[]::INTEGER[]) AS stored_ranks
            FROM equity_original_universe_runs AS run LEFT JOIN equity_universe_members AS member USING (universe_run_id)
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED' AND run.policy_version = %s
              AND run.effective_from::DATE = %s
            GROUP BY run.universe_run_id, run.effective_from, run.admitted_members,
                     run.policy_sha256, run.source_request_sha256, run.observed_at
            ORDER BY run.observed_at
            """, (policy_version, session),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    if len(rows) != 1:
        raise ValueError("transition review requires exactly one original universe run")
    membership = review_membership_source(rows[0], cache_dir, transition=transition)
    report = {
        "review_version": "historical_transition_review_v1", "transaction": transaction,
        "transition": transition, "membership": membership,
        "repair_status": "PROPOSAL_ONLY", "readiness": "NOT_CERTIFIED", "repair_authorized": False,
        "blockers": ["UNIVERSE_REVISION_PUBLICATION_REQUIRED", "DATED_BAR_LINEAGE_REPAIR_REQUIRED",
                     "MERGER_CONSIDERATION_AND_DIVIDEND_RETURN_PATH_REQUIRED"],
    }
    if "correction_candidate" not in membership:
        report["blockers"].append("ORIGINAL_INPUT_REPLAY_NOT_VERIFIED")
    if include_prices or include_actions:
        report["price_review"] = review_transition_prices(transition, membership, cache_dir)
    if include_actions:
        report["action_review"] = review_transition_actions(transition, report["price_review"])
    report["review_sha256"] = sha256_json(json.loads(json.dumps(report, default=str)))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy-version", default="liquid_us_common_stocks_v2")
    parser.add_argument("--output", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--identity-review", action="store_true")
    mode.add_argument("--transition-manifest", type=Path)
    parser.add_argument("--transition-prices", action="store_true", help="Read and pin the transition's 20-session warm-up plus effective-session prices")
    parser.add_argument("--transition-actions", action="store_true", help="Also audit response-bound action coverage for the transition price slice")
    parser.add_argument("--cache-dir", type=Path, default=BACKEND_DIR / ".cache" / "historical-signal-research")
    arguments = parser.parse_args()
    if (arguments.transition_prices or arguments.transition_actions) and not arguments.transition_manifest:
        parser.error("transition price/action review requires --transition-manifest")
    if arguments.transition_manifest:
        transition = json.loads(arguments.transition_manifest.read_text(encoding="utf-8"))
        report = review_transition(transition, arguments.cache_dir, arguments.policy_version,
                       include_prices=arguments.transition_prices, include_actions=arguments.transition_actions)
    else:
        report = (review_identities(arguments.policy_version, arguments.cache_dir)
                  if arguments.identity_review else audit_inputs(arguments.policy_version))
    serialized = json.dumps(report, indent=2, default=str, allow_nan=False)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())