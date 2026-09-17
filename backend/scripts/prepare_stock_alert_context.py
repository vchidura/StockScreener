"""Build bounded, read-only stock-alert context readiness and annotation artifacts."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import zlib

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from research.gics_sectors import SECTOR_BENCHMARK_ETF, sector_for_sic
from research.stock_alert_context import (VERSION, READINESS_VERSION, comparable_iv_context, daily_price_context,
    dated_reference, event_context, financial_context, market_context, native_window_context, observation, sector_context, utc)
from research.stock_idea_engine import digest, read_candidate
from research.stock_idea_forward import alert_selection_cohort, forward_config
from research.stock_idea_replay import execution_times


def read_enrollment(path, session):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        captured = datetime.now(timezone.utc)
        checkpoint = connection.execute("SELECT payload FROM forward_checkpoint").fetchone()[0]
        policy = json.loads(connection.execute("SELECT payload FROM forward_manifest").fetchone()[0])
        state = json.loads(zlib.decompress(checkpoint))
        publications = [json.loads(zlib.decompress(row[0])) for row in connection.execute("SELECT payload FROM forward_publications ORDER BY window_key")]
    sessions = {session} if isinstance(session, str) else set(session)
    publications = [row for row in publications if row["session"] in sessions]
    if not 1 <= len(sessions) <= 2 or not 0 < len(state["members"]) <= 1000 or len(publications) > 30 * len(sessions):
        raise ValueError("context audit exceeds its declared one-session/cohort bounds")
    return state, policy, publications, dict(captured_at=captured.isoformat(), checkpoint_sha256=hashlib.sha256(checkpoint).hexdigest(),
        publications_sha256=digest(publications), enrolled_at=state["enrolled_at"])


def capture_sector_option_activity(cursor, proxies, cutoff):
    from psycopg2 import Error as DatabaseError
    from options.config import load_option_runtime_configuration
    configuration = load_option_runtime_configuration()
    cursor.execute("SAVEPOINT sector_option_activity")
    try:
        cursor.execute("""WITH latest AS (
            SELECT requested.underlying,matrix.* FROM unnest(%s::text[]) AS requested(underlying)
            CROSS JOIN LATERAL (
                SELECT analysis.matrix_id::text,analysis.batch_id::text,analysis.market_time,
                    analysis.observed_time,analysis.created_at,analysis.completed_at,ingestion.retained_row_count AS retained_contracts
                FROM option_analysis_runs analysis JOIN option_ingestion_runs ingestion USING(batch_id)
                WHERE analysis.underlying=requested.underlying AND analysis.status='COMPLETE' AND ingestion.status='COMPLETE'
                  AND analysis.policy_sha256=%s AND ingestion.configuration_sha256=%s
                  AND analysis.market_time>=%s AND analysis.market_time<=%s AND analysis.observed_time<=%s
                  AND analysis.created_at<=%s AND analysis.completed_at<=%s
                  AND ingestion.first_observed_at<=%s AND ingestion.completed_at<=%s
                ORDER BY analysis.market_time DESC,analysis.completed_at DESC,analysis.matrix_id LIMIT 1
            ) matrix
        ) SELECT latest.*,activity.* FROM latest CROSS JOIN LATERAL (
            SELECT count(*) AS contracts,count(*) FILTER(WHERE valid) AS valid_contracts,
                COALESCE(sum(day_volume) FILTER(WHERE valid AND contract_type='CALL'),0)::float8 AS call_volume,
                COALESCE(sum(day_volume) FILTER(WHERE valid AND contract_type='PUT'),0)::float8 AS put_volume,
                COALESCE(sum(day_volume*mark*shares_per_contract) FILTER(WHERE valid AND contract_type='CALL'),0)::float8 AS call_premium,
                COALESCE(sum(day_volume*mark*shares_per_contract) FILTER(WHERE valid AND contract_type='PUT'),0)::float8 AS put_premium,
                greatest(latest.observed_time,max(first_observed_at)) AS observed_at,
                greatest(latest.created_at,max(created_at),max(updated_at)) AS created_at,
                greatest(latest.completed_at,latest.observed_time,latest.created_at,max(first_observed_at),max(created_at),max(updated_at)) AS available_at
            FROM (SELECT snapshot.*,COALESCE(model_mark,display_mark) AS mark,
                (contract_type IN ('CALL','PUT') AND day_volume>=0 AND day_volume<'Infinity'::float8
                    AND COALESCE(model_mark,display_mark)>=0 AND COALESCE(model_mark,display_mark)<'Infinity'::float8
                    AND shares_per_contract>0 AND shares_per_contract<'Infinity'::float8) AS valid
                FROM option_chain_snapshots snapshot WHERE batch_id=latest.batch_id::uuid
                  AND first_observed_at<=%s AND created_at<=%s AND updated_at<=%s AND market_data_time<=%s) snapshots
        ) activity ORDER BY latest.underlying""", (proxies, configuration.policy_sha256, configuration.configuration_sha256,
            cutoff - timedelta(days=7), cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff))
        return [dict(row) for row in cursor.fetchall()], None
    except DatabaseError:
        cursor.execute("ROLLBACK TO SAVEPOINT sector_option_activity")
        return [], "OPTION_ACTIVITY_READ_UNAVAILABLE"
    finally:
        cursor.execute("RELEASE SAVEPOINT sector_option_activity")


def capture_financial_reports(cursor, members, cutoffs):
    cursor.execute("""SELECT DISTINCT report.* FROM unnest(%s::uuid[]) AS member(security_id)
        CROSS JOIN unnest(%s::timestamptz[]) AS point(cutoff)
        CROSS JOIN unnest(ARRAY['quarterly','annual','trailing_twelve_months']) AS frame(timeframe)
        CROSS JOIN LATERAL (
            SELECT fundamental_report_id::text,security_id::text,timeframe,period_end,filing_date,
                availability_time,observed_at,created_at,source,accession_number,quality_codes,
                revenue,operating_income,net_income,diluted_eps,cash_and_equivalents,current_debt,
                long_term_debt,operating_cash_flow,capital_expenditures,free_cash_flow
            FROM equity_fundamental_reports WHERE security_id=member.security_id AND timeframe=frame.timeframe
                AND period_end<=point.cutoff::date AND filing_date<=point.cutoff::date
                AND availability_time<=point.cutoff AND observed_at<=point.cutoff AND created_at<=point.cutoff
            ORDER BY period_end DESC,availability_time DESC,observed_at DESC,created_at DESC,fundamental_report_id DESC
            LIMIT 1) report""", (sorted({row["security_id"] for row in members}), sorted(set(cutoffs))))
    reports = [dict(row) for row in cursor.fetchall()]
    if len(reports) > len(members) * len(cutoffs) * 3:
        raise ValueError("financial capture exceeds declared cohort/cutoff bounds")
    return reports


def capture(cursor, members, publications, audit_cutoff, configuration, *, market_only=False, company_only=False):
    import exchange_calendars
    calendar = exchange_calendars.get_calendar("XNYS")
    earliest = min((utc(row["input_deadline"]) for row in publications), default=audit_cutoff)
    earliest_session = calendar.date_to_session(earliest.date(), direction="previous")
    start = calendar.session_offset(earliest_session, -260).date()
    end = audit_cutoff.date()
    names = sorted({row["ticker"] for row in members} | {"SPY", "QQQ"} | set(SECTOR_BENCHMARK_ETF.values()))
    checkpoints = sorted({utc(row["input_deadline"]) for row in publications if row["selected"] or row.get("candidates")} | {audit_cutoff})
    result = dict(bars=[], references=[], actions=[], events=[], event_coverage=[], iv=[], matrices=[], activity=[], prints=[], action_coverage=[])
    cursor.execute("SELECT transaction_timestamp() AS transaction_at, current_setting('transaction_read_only') AS read_only, current_setting('transaction_isolation') AS isolation")
    result["transaction"] = dict(cursor.fetchone())
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND (table_name ILIKE '%vix%' OR table_name ILIKE '%borrow%' OR table_name ILIKE '%halt%' OR table_name ILIKE '%quote%' OR table_name ILIKE '%daily_contract%')")
    result["capability_tables"] = [row["table_name"] for row in cursor.fetchall()]
    for offset in range(0, len(names), 25):
        batch = names[offset:offset + 25]
        cursor.execute("""SELECT DISTINCT reference.* FROM unnest(%s::text[]) AS member(ticker)
            CROSS JOIN unnest(%s::timestamptz[]) AS point(cutoff)
            CROSS JOIN LATERAL (SELECT ticker,security_id::text,security_revision_id::text AS revision_id,security_type,
                active,sic_code,effective_from,observed_at,created_at,source_as_of_date,source
                FROM equity_security_reference_revisions WHERE ticker=member.ticker AND effective_from<=point.cutoff
                  AND observed_at<=point.cutoff AND created_at<=point.cutoff
                ORDER BY effective_from DESC,observed_at DESC,created_at DESC,security_revision_id LIMIT 1) reference""",
            (batch, checkpoints))
        rows = [dict(row) for row in cursor.fetchall()]
        if len(rows) > 20000:
            raise ValueError("dated reference batch exceeds read limit")
        result["references"].extend(rows)
        cursor.execute("""SELECT ticker,security_id::text,interval,session_date::text AS session,bar_start,bar_end,
            bar_revision_id::text AS revision_id,open_price::float8 AS open,high_price::float8 AS high,
            low_price::float8 AS low,close_price::float8 AS close,volume::float8,source_kind,system_observed_at,created_at
            FROM equity_bar_revisions WHERE ticker=ANY(%s::text[]) AND interval='1d' AND session_scope='RTH'
              AND NOT adjusted AND is_final AND session_date>=%s AND session_date<=%s
              AND system_observed_at<=%s AND created_at<=%s ORDER BY ticker,session_date,created_at LIMIT 30001""",
            (batch, start, end, audit_cutoff, audit_cutoff))
        rows = [dict(row) for row in cursor.fetchall()]
        if len(rows) > 30000:
            raise ValueError("daily price batch exceeds read limit")
        result["bars"].extend(rows)
    if not market_only:
        cursor.execute("""SELECT ticker,security_id::text,bar_end,open_price::float8 AS open,close_price::float8 AS close,
        volume::float8,bar_revision_id::text AS revision_id,system_observed_at,created_at FROM equity_bar_revisions
        WHERE ticker=ANY(%s::text[]) AND interval='30m' AND session_scope='RTH' AND NOT adjusted AND is_final
          AND session_date>=%s AND session_date<=%s AND system_observed_at<=%s AND created_at<=%s
        ORDER BY bar_end,created_at LIMIT 40001""", (names, earliest.date(), end, audit_cutoff, audit_cutoff))
        result["native_bars"] = [dict(row) for row in cursor.fetchall()]
        if len(result["native_bars"]) > 40000:
            raise ValueError("native window audit exceeds bounded session scope")
    cursor.execute("""SELECT corporate_action_id::text AS revision_id,security_id::text,ticker,action_type,effective_date,
        first_observed_at,created_at,split_from::float8,split_to::float8,source,payload_sha256 FROM equity_corporate_actions WHERE ticker=ANY(%s::text[]) AND effective_date>=%s
          AND effective_date<=%s AND first_observed_at<=%s AND created_at<=%s ORDER BY effective_date""",
        (names, start, end + timedelta(days=45), audit_cutoff, audit_cutoff))
    result["actions"] = [dict(row) for row in cursor.fetchall()]
    if market_only:
        from equity.api import expected_materialized_market_time
        boundary = expected_materialized_market_time(audit_cutoff, "30m")
        volume_start = calendar.session_offset(calendar.date_to_session(boundary.date()), -40).date()
        proxies = sorted({"SPY", "QQQ"} | set(SECTOR_BENCHMARK_ETF.values()))
        cursor.execute("""SELECT ticker,security_id::text,session_date::text AS session,bar_start,bar_end,
            volume::float8,source_kind,bar_revision_id::text AS revision_id,system_observed_at,created_at
            FROM equity_bar_revisions WHERE ticker=ANY(%s::text[]) AND interval='30m' AND session_scope='RTH'
              AND NOT adjusted AND is_final AND session_date>=%s AND bar_end<=%s
              AND system_observed_at<=%s AND created_at<=%s ORDER BY ticker,bar_end,created_at LIMIT 20001""",
            (proxies, volume_start, boundary, audit_cutoff, audit_cutoff))
        result["volume_bars"] = [dict(row) for row in cursor.fetchall()]
        if len(result["volume_bars"]) > 20000:
            raise ValueError("proxy volume capture exceeds bounded history")
        result["volume_boundary"] = boundary.isoformat()
        result["sector_option_activity"], result["sector_option_error"] = capture_sector_option_activity(cursor, proxies, audit_cutoff)
        return result
    cursor.execute("""SELECT coverage_id::text AS revision_id,ticker,action_type,window_start,window_end,first_observed_at,created_at,
        availability_mode FROM equity_corporate_action_coverage WHERE ticker=ANY(%s::text[]) AND window_end>=%s
          AND first_observed_at>=%s AND first_observed_at<=%s AND created_at<=%s""",
        (names, earliest.date(), earliest - timedelta(days=2), audit_cutoff, audit_cutoff))
    result["action_coverage"] = [dict(row) for row in cursor.fetchall()]
    cursor.execute("""SELECT market_event_id::text AS revision_id,event_type,affected_underlying,scheduled_time,source,source_key,
        confidence,status,announcement_time,first_observed_at,created_at FROM option_market_events
        WHERE (affected_underlying=ANY(%s::text[]) OR affected_underlying IS NULL)
          AND first_observed_at<=%s AND created_at<=%s ORDER BY first_observed_at LIMIT 50001""",
        (names, audit_cutoff, audit_cutoff))
    result["events"] = [dict(row) for row in cursor.fetchall()]
    if len(result["events"]) > 50000:
        raise ValueError("event revision scope exceeds read limit")
    cursor.execute("""SELECT coverage_id::text AS revision_id,event_type,affected_underlying,source,window_start,window_end,
        first_observed_at,created_at FROM option_event_calendar_coverage
        WHERE (affected_underlying=ANY(%s::text[]) OR affected_underlying IS NULL)
          AND first_observed_at>=%s AND first_observed_at<=%s AND created_at<=%s AND window_end>=%s""",
        (names, earliest - timedelta(seconds=configuration.settings.event_calendar_max_age_seconds), audit_cutoff, audit_cutoff, earliest))
    result["event_coverage"] = [dict(row) for row in cursor.fetchall()]
    if company_only:
        result["fundamentals"] = capture_financial_reports(cursor, members, checkpoints)
        cursor.execute("""SELECT COUNT(*) AS retained_reports,COUNT(DISTINCT security_id) AS retained_securities,
            COUNT(*) FILTER(WHERE filing_date IS NULL) AS missing_filing_date
            FROM equity_fundamental_reports WHERE security_id=ANY(%s::uuid[])
                AND observed_at<=%s AND created_at<=%s""",
            ([row["security_id"] for row in members], audit_cutoff, audit_cutoff))
        result["financial_inventory"] = dict(cursor.fetchone())
        return result
    underlyings = list(configuration.settings.underlyers)
    cursor.execute("""SELECT context.iv_context_id::text AS revision_id,context.underlying,context.expiration_bucket,
        context.current_comparable_iv,context.sample_count,context.coverage_fraction,context.range_position_rank,context.empirical_percentile,
        context.first_observed_time,context.created_at,context.history_availability_mode,context.null_reason_codes,analysis.market_time,
        analysis.completed_at AS analysis_completed_at FROM option_iv_context_snapshots context
        JOIN option_analysis_runs analysis USING(matrix_id) JOIN option_ingestion_runs ingestion USING(batch_id)
        WHERE context.underlying=ANY(%s::text[]) AND context.settlement_valuation_policy_sha256=%s
          AND context.calculation_version=%s AND analysis.policy_sha256=%s AND ingestion.configuration_sha256=%s
          AND analysis.status='COMPLETE' AND context.first_observed_time<=%s AND context.created_at<=%s
          AND analysis.completed_at<=%s AND analysis.market_time>=%s""",
        (underlyings, configuration.settlement_valuation_policy_sha256, configuration.settlement_valuation_policy.iv_context_calculation_version,
         configuration.policy_sha256, configuration.configuration_sha256, audit_cutoff, audit_cutoff, audit_cutoff, earliest - timedelta(days=7)))
    result["iv"] = [dict(row) for row in cursor.fetchall()]
    cursor.execute("""SELECT analysis.matrix_id::text,analysis.batch_id::text,analysis.underlying,analysis.market_time,
        analysis.observed_time,analysis.completed_at,analysis.created_at FROM option_analysis_runs analysis
        JOIN option_ingestion_runs ingestion USING(batch_id) WHERE analysis.underlying=ANY(%s::text[])
          AND analysis.status='COMPLETE' AND analysis.policy_sha256=%s AND ingestion.configuration_sha256=%s
          AND analysis.completed_at<=%s AND analysis.created_at<=%s AND analysis.market_time>=%s""",
        (underlyings, configuration.policy_sha256, configuration.configuration_sha256, audit_cutoff, audit_cutoff, earliest - timedelta(days=7)))
    result["matrices"] = [dict(row) for row in cursor.fetchall()]
    matrix_cutoffs = set()
    for cutoff in checkpoints:
        for underlying in underlyings:
            rows = [row for row in result["matrices"] if row["underlying"] == underlying
                and max(utc(row["completed_at"]), utc(row["created_at"]), utc(row["observed_time"])) <= cutoff]
            latest = max(rows, key=lambda row: (utc(row["market_time"]), utc(row["completed_at"])), default=None)
            if latest:
                matrix_cutoffs.add((latest["matrix_id"], latest["batch_id"], underlying, cutoff))
    for matrix_id, batch_id, underlying, cutoff in sorted(matrix_cutoffs):
        cursor.execute("""SELECT count(*) AS contracts,count(*) FILTER(WHERE bid IS NOT NULL AND ask IS NOT NULL AND bid>0 AND ask>=bid) AS quoted_contracts,
            count(*) FILTER(WHERE day_volume IS NOT NULL) AS volume_contracts,count(*) FILTER(WHERE open_interest IS NOT NULL) AS oi_contracts,
            count(*) FILTER(WHERE contract_type='PUT') AS puts,count(*) FILTER(WHERE contract_type='CALL') AS calls,
            max(market_data_time) AS market_time,max(first_observed_at) AS observed_at,max(created_at) AS created_at
            FROM option_chain_snapshots WHERE batch_id=%s AND first_observed_at<=%s AND created_at<=%s AND updated_at<=%s""",
            (batch_id, cutoff, cutoff, cutoff))
        result["activity"].append(dict(cursor.fetchone(), matrix_id=matrix_id, underlying=underlying, cutoff=cutoff))
    cursor.execute("""SELECT underlying,count(*) AS prints,count(DISTINCT contract_id) AS contracts,
        count(*) FILTER(WHERE classification_status='INCLUDED') AS included_prints,
        count(*) FILTER(WHERE classification_status='PENDING') AS pending_prints,
        min(sip_timestamp) AS first_market_time,max(sip_timestamp) AS last_market_time,max(first_observed_at) AS observed_at
        FROM option_trade_events WHERE underlying=ANY(%s::text[]) AND sip_timestamp>=%s AND sip_timestamp<=%s
          AND first_observed_at<=%s AND created_at<=%s GROUP BY underlying""",
        (underlyings, earliest.replace(hour=0, minute=0, second=0, microsecond=0), audit_cutoff, audit_cutoff, audit_cutoff))
    result["prints"] = [dict(row) for row in cursor.fetchall()]
    if "option_daily_contract_mark_revisions" in result["capability_tables"]:
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='option_daily_contract_mark_revisions'")
        columns = {row["column_name"] for row in cursor.fetchall()}
        result["raw_iv_inventory_clock_columns"] = sorted(columns & {"first_observed_at", "first_observed_time", "observed_at", "created_at"})
        cursor.execute("""SELECT underlying,count(*) AS marks,count(DISTINCT settlement_session) AS sessions,
            min(settlement_session) AS first_session,max(settlement_session) AS last_session
            FROM option_daily_contract_mark_revisions WHERE underlying=ANY(%s::text[]) AND NOT mark_adjusted
              AND mark_source=%s AND valuation_policy_sha256=%s AND settlement_session>=%s AND settlement_session<=%s
            GROUP BY underlying""", (underlyings, configuration.settlement_valuation_policy.mark_source,
                configuration.settlement_valuation_policy_sha256, start, end))
        result["raw_iv_inventory"] = [dict(row) for row in cursor.fetchall()]
    cursor.execute("""SELECT source,count(*) AS observations,count(DISTINCT rate_date) AS rate_dates,min(rate_date) AS first_date,
        max(rate_date) AS last_date,max(first_observed_at) AS observed_at,max(created_at) AS created_at
        FROM option_risk_free_rate_observations WHERE rate_date>=%s AND rate_date<=%s
          AND first_observed_at<=%s AND created_at<=%s GROUP BY source""", (start, end, audit_cutoff, audit_cutoff))
    result["rates_inventory"] = [dict(row) for row in cursor.fetchall()]
    return result


def build_manifest(state, policy, publications, facts, audit_cutoff, configuration, *, include_current=True, include_candidates=False):
    import exchange_calendars
    calendar = exchange_calendars.get_calendar("XNYS")
    by_ticker, by_security = defaultdict(list), defaultdict(list)
    for row in facts["bars"]:
        by_ticker[row["ticker"]].append(row)
        by_security[row["security_id"]].append(row)
    references = defaultdict(list)
    for row in facts["references"]:
        references[row["security_id"]].append(row)
    current_session = calendar.date_to_session(audit_cutoff.date(), direction="previous")
    if calendar.session_close(current_session).to_pydatetime() > audit_cutoff:
        current_session = calendar.previous_session(current_session)
    points = [dict(key="CURRENT_READINESS", cutoff=audit_cutoff, prior_session=str(current_session.date()), plans=[])] if include_current else []
    for publication in publications:
        plan_ids = list(publication["candidates"]) if include_candidates else publication["selected"]
        if plan_ids:
            session = calendar.date_to_session(publication["session"])
            points.append(dict(key=publication["window_key"], cutoff=utc(publication["input_deadline"]),
                prior_session=str(calendar.previous_session(session).date()), publication=publication,
                plans=[read_candidate(publication["candidates"][key]) for key in plan_ids]))
    contexts, annotations = {}, []
    cache = {}
    for point in points:
        cutoff, prior = point["cutoff"], point["prior_session"]
        def price_context(ticker, security_id=None):
            if security_id is None:
                refs = [row for row in facts["references"] if row["ticker"] == ticker and row["active"]
                    and max(utc(row["effective_from"]), utc(row["observed_at"]), utc(row["created_at"])) <= cutoff]
                latest = max(refs, key=lambda row: utc(row["effective_from"]), default=None)
                security_id = latest["security_id"] if latest else None
            if not security_id:
                return observation("UNAVAILABLE", "BENCHMARK_IDENTITY_UNAVAILABLE")
            key = (security_id, prior, cutoff)
            if key not in cache:
                cache[key] = daily_price_context(by_security[security_id], security_id, prior, cutoff, facts["actions"])
            return cache[key]
        spy, qqq = price_context("SPY"), price_context("QQQ")
        boundary = utc(point["publication"].get("market_time", point["publication"]["window_key"])) if point.get("publication") else (
            audit_cutoff - timedelta(minutes=15)).replace(minute=((audit_cutoff - timedelta(minutes=15)).minute // 30) * 30, second=0, microsecond=0)
        bundle = dict(spy=spy, qqq=qqq, market=market_context(spy, qqq), cutoff=cutoff, prior_session=prior, members=[])
        targets = [dict(security_id=plan.security_id, ticker=plan.ticker, plan=plan) for plan in point["plans"]] or state["members"]
        for member in targets:
            identity, ticker = member["security_id"], member["ticker"]
            reference = dated_reference(references[identity], identity, ticker, cutoff)
            sector = sector_for_sic(reference.get("sic_code")) if reference and reference["security_type"] == "CS" else None
            proxy = SECTOR_BENCHMARK_ETF.get(sector)
            stock = price_context(ticker, identity)
            factors = dict(stock_daily=stock, sector=sector_context(reference, stock, price_context(proxy) if proxy else None, spy),
                native30m=native_window_context(facts.get("native_bars", []), identity, boundary, cutoff),
                vix=observation("NOT_COVERED", "NO_RETAINED_APPROVED_VIX_OBSERVATIONS"),
                halts=observation("NOT_COVERED", "HALT_COVERAGE_NOT_PROVEN"),
                short_borrow=observation("NOT_COVERED", "BORROW_LOCATE_COVERAGE_NOT_PROVEN"),
                macro_other=observation("NOT_COVERED", "CPI_JOBS_CALENDAR_NOT_RETAINED"),
                flow=observation("NOT_COVERED", "CAUSAL_CLASSIFIED_FLOW_POPULATION_NOT_PROVEN"))
            if "fundamentals" in facts:
                factors["financials"] = financial_context(facts["fundamentals"], identity, reference, cutoff)
            plan = member.get("plan")
            if plan and plan.model == "discovery":
                start, end = None, None
            elif plan:
                start, end = execution_times(plan, utc(point["publication"]["actual_publication_at"]), policy)
            else:
                start, end = cutoff, cutoff + timedelta(days=1)
            for event_type, field in (("EARNINGS", "earnings"), ("FED_RATE_DECISION", "fomc")):
                factors[field] = (observation("NOT_APPLICABLE", "NOT_A_COMMON_STOCK") if event_type == "EARNINGS" and reference and reference["security_type"] != "CS"
                    else event_context(ticker, event_type, start, end, cutoff, facts["events"], facts["event_coverage"],
                        max_age_seconds=configuration.settings.event_calendar_max_age_seconds))
            known_actions = [row for row in facts["actions"] if row["security_id"] == identity and start and end
                and utc(start).date() <= row["effective_date"] <= utc(end).date()
                and max(utc(row["first_observed_at"]), utc(row["created_at"])) <= cutoff]
            factors["actions"] = observation("UNAVAILABLE", "ALL_ACTION_TYPE_COVERAGE_NOT_CERTIFIED")
            factors["actions"].update(value=dict(known_events=[dict(type=row["action_type"], effective_date=row["effective_date"]) for row in known_actions]),
                source_revision_ids=[row["revision_id"] for row in known_actions])
            options_stock = ticker in configuration.settings.underlyers and reference and reference["security_type"] == "CS"
            for bucket in ("7D", "21D", "45D"):
                factors["iv_" + bucket] = (comparable_iv_context(facts["iv"], ticker, bucket, cutoff, prior, configuration.settlement_valuation_policy)
                    if options_stock else observation("NOT_COVERED", "OUTSIDE_OPTIONS_COVERED_STOCK_COHORT"))
            activity = next((row for row in facts["activity"] if row["underlying"] == ticker and row["cutoff"] == cutoff), None)
            factors["option_activity"] = observation("NOT_COVERED", "NO_TIMELY_OPTION_MATRIX")
            factors["quotes"] = observation("NOT_COVERED", "NO_CONTEMPORANEOUS_NBBO")
            factors["oi"] = observation("UNAVAILABLE", "OI_SETTLEMENT_KNOWLEDGE_DATE_NOT_PROVEN")
            if options_stock and activity and activity["contracts"]:
                factors["option_activity"].update(status="READY", reason_codes=["SCOPED_ACTIVITY_NOT_DIRECTION_OR_LIQUIDITY"],
                    value={key: activity[key] for key in ("contracts", "volume_contracts", "oi_contracts", "puts", "calls")},
                    source_revision_ids=[activity["matrix_id"]], market_time=activity["market_time"], observed_at=activity["observed_at"],
                    created_at=activity["created_at"], available_at=max(utc(activity["observed_at"]), utc(activity["created_at"])).isoformat())
                if utc(activity["market_time"]).date().isoformat() < prior:
                    factors["option_activity"].update(status="STALE", reason_codes=["OPTION_MATRIX_STALE"])
                factors["quotes"]["retained_quoted_contracts"] = activity["quoted_contracts"]
            row = dict(security_id=identity, ticker=ticker, reference_id=reference["revision_id"] if reference else None,
                security_type=reference["security_type"] if reference else None, options_covered_stock=bool(options_stock), factors=factors)
            bundle["members"].append(row)
            if plan:
                annotations.append(dict(alert_id=plan.episode_id, source_id=point["publication"]["policy_version"],
                    run_id=point["key"], input_cutoff=cutoff, publication_at=point["publication"]["actual_publication_at"],
                    capture_mode="RECONSTRUCTED_FROM_RETAINED_ASOF_INPUTS", market_ref=point["key"], **row))
        contexts[point["key"]] = bundle
    summaries = {key: dict(members=len(bundle["members"]), market=bundle["market"]["status"],
        factors={factor: dict(Counter(row["factors"][factor]["status"] for row in bundle["members"])) for factor in bundle["members"][0]["factors"]})
        for key, bundle in contexts.items()}
    return dict(contexts=contexts, annotations=annotations, summary=summaries)


def verify_artifacts(path):
    manifest = json.loads(path.read_text(encoding="utf-8"))
    annotations = json.loads(path.with_suffix(".annotations.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == READINESS_VERSION and annotations["schema_version"] == VERSION
    assert annotations["readiness_manifest_sha256"] == digest(manifest)
    assert manifest["database_snapshot"]["read_only"] == "on"
    assert manifest["database_snapshot"]["isolation"] == "repeatable read"
    assert manifest["original_publications_unchanged"] and annotations["annotations_only"]
    keys = [(row["source_id"], row["run_id"], row["alert_id"]) for row in annotations["rows"]]
    assert len(keys) == len(set(keys))
    expected_contexts = {key: {field: bundle[field] for field in ("market", "spy", "qqq", "cutoff", "prior_session")}
        for key, bundle in manifest["observations"].items()}
    assert annotations["publication_contexts"] == expected_contexts, "publication context differs from manifest"
    for row in annotations["rows"]:
        bundle = manifest["observations"][row["run_id"]]
        assert row["market_ref"] == row["run_id"] and utc(row["input_cutoff"]) == utc(bundle["cutoff"])
        assert any(all(row.get(key) == value for key, value in member.items()) for member in bundle["members"]), "annotation facts differ from manifest"
    checked = 0
    for bundle in manifest["observations"].values():
        cutoff = utc(bundle["cutoff"])
        facts = [bundle[field] for field in ("spy", "qqq", "market")]
        facts += [factor for member in bundle["members"] for factor in member["factors"].values()]
        for factor in facts:
            for field in ("observed_at", "created_at", "available_at"):
                assert factor[field] is None or utc(factor[field]) <= cutoff, field
            if factor["status"] == "READY":
                assert factor["value"] is not None and factor["source_revision_ids"]
            checked += 1
    assert all(row["capture_mode"] == "RECONSTRUCTED_FROM_RETAINED_ASOF_INPUTS" for row in annotations["rows"])
    print(json.dumps(dict(status="VERIFIED", fact_records=checked, annotation_rows=len(keys),
        hashes_match=True, causal_timestamps=True, original_publications_unchanged=True), indent=2), flush=True)
    return manifest, annotations


def freeze_alert_context(args):
    from research.stock_alert_annotations import freeze_publication_context
    manifest, annotations = verify_artifacts(args.output)
    state, policy, publications, _ = read_enrollment(args.state_dir / "forward.sqlite", args.session.isoformat())
    if (manifest["session"] != args.session.isoformat() or manifest["universe_sha256"] != digest(state["members"])
            or manifest["baseline_policy_sha256"] != digest(policy) or not manifest.get("company_context_only")):
        raise ValueError("annotation source ledger does not match retained manifest")
    by_run = {row["window_key"]: row for row in publications}
    grouped = defaultdict(list)
    for annotation in annotations["rows"]:
        grouped[annotation["run_id"]].append(annotation)
    for run_id in grouped:
        if run_id not in by_run or manifest.get("publication_hashes", {}).get(run_id) != digest(by_run[run_id]):
            raise ValueError("original publication hash does not match annotation manifest")
    counts = Counter()
    for run_id, rows in grouped.items():
        counts[freeze_publication_context(args.state_dir / "alert-context", by_run[run_id], rows,
            annotations["publication_contexts"][run_id], assembled_at=datetime.now(timezone.utc).isoformat(),
            manifest_sha256=digest(manifest))] += 1
    print(json.dumps(dict(status="CONTEXT_FREEZE_COMPLETE", annotation_rows=len(annotations["rows"]),
        publications=dict(counts), ledger_writes=False, provider_requests=False), indent=2), flush=True)


def write_rotation_snapshot(snapshot, args):
    from research.stock_rotation import compact_rotation_lineage, verify_rotation_snapshot
    snapshot = compact_rotation_lineage(snapshot)
    snapshot["projection_code_sha256"] = hashlib.sha256((BACKEND / "research/stock_rotation.py").read_bytes()).hexdigest()
    snapshot["projected_at"] = datetime.now(timezone.utc).isoformat()
    snapshot["snapshot_sha256"] = digest({key: value for key, value in snapshot.items() if key != "snapshot_sha256"})
    verified = verify_rotation_snapshot(snapshot)
    payload = json.dumps(snapshot, indent=2, allow_nan=False) + "\n"
    if len(payload.encode("utf-8")) > 20_000_000:
        raise ValueError("rotation payload exceeds the bounded reader; previous view retained")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        stream.write(payload)
    if args.publish_rotation_view:
        path = BACKEND / "backups/stock-rotation/latest.json"
        if path.exists() and utc(json.loads(path.read_text(encoding="utf-8"))["as_of"]) > utc(snapshot["as_of"]):
            raise ValueError("newer rotation view exists; refusing to roll it back")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix("." + snapshot["snapshot_sha256"] + ".tmp")
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                stream.write(payload)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    print(json.dumps(dict(verified, output=str(args.output), view_published=args.publish_rotation_view,
        bytes=len(payload.encode("utf-8")), session=snapshot["session"], as_of=snapshot["as_of"],
        input_counts=snapshot["input_counts"], elapsed_seconds=snapshot["elapsed_seconds"],
        split_validation=snapshot.get("split_validation"),
        additional_context={key: dict(status=value["status"], value=value["value"], reasons=value["reason_codes"],
            coverage=[value.get("timely_observations"), value.get("expected_observations")]) for key, value in snapshot.get("additional_context", {}).items()}), indent=2), flush=True)


def load_etf_creation_records(path):
    if not path.exists():
        return [], "DATED_NAV_AND_SHARES_OUTSTANDING_REQUIRED"
    if path.stat().st_size > 1_000_000:
        raise ValueError("ETF creation records exceed read bound")
    artifact = json.loads(path.read_text(encoding="utf-8"))
    if artifact.get("schema") != "etf_nav_share_records_v1" or artifact.get("usage_approved") is not True:
        return [], "ETF_NAV_SHARE_SOURCE_APPROVAL_REQUIRED"
    records = artifact["records"]
    if artifact.get("records_sha256") != digest(records) or len(records) > 1000:
        raise ValueError("ETF record hash or count invalid")
    fields = {"ticker", "security_id", "session", "market_time", "observed_at", "created_at", "revision_id", "source", "currency",
        "shares_outstanding", "nav", "nav_basis", "split_factor_from_previous", "split_review", "split_evidence_id"}
    if any(set(row) != fields for row in records) or len({row["revision_id"] for row in records}) != len(records):
        raise ValueError("ETF record schema or revision identity invalid")
    for row in records:
        for field in ("market_time", "observed_at", "created_at"):
            utc(row[field])
    return records, None


def load_context_split_reviews(path, cutoff):
    if path.stat().st_size > 100_000:
        raise ValueError("split review exceeds bounded size")
    record = json.loads(path.read_text(encoding="utf-8"))
    reviews = record.get("reviews")
    if (record.get("schema") != "stock_sector_split_review_v1" or not isinstance(reviews, list) or len(reviews) != 4
            or record.get("scope") != "MARKET_CONDITIONS_CONTEXT_ONLY" or record.get("live_gate_enabled") is not False
            or record.get("reviews_sha256") != digest(reviews)
            or {row["ticker"] for row in reviews} != {"XLY", "XLE", "XLU", "XLB"}
            or any(row["status"] != "REVIEWED" or utc(row["reviewed_at"]) > cutoff for row in reviews)):
        raise ValueError("four-proxy split review is invalid or not available")
    return reviews


def prepare_rotation(args, state, policy, publications, retained):
    from dotenv import load_dotenv
    from research.stock_rotation import build_rotation_snapshot, split_basis_comparison
    load_dotenv(BACKEND / ".env")
    from database import get_db_cursor
    from equity.market_context_source import collect_fred_context
    started = time.monotonic()
    macro = collect_fred_context(BACKEND / "backups/stock-rotation/fred", fetch=getattr(args, "fetch_macro", False))
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cutoff = datetime.now(timezone.utc)
        facts = capture(cursor, state["members"], publications, cutoff, None, market_only=True)
    facts["macro_context"] = macro
    if getattr(args, "split_review", None):
        facts["split_reviews"] = load_context_split_reviews(args.split_review, cutoff)
    facts["etf_records"], facts["etf_records_error"] = load_etf_creation_records(BACKEND / "backups/stock-rotation/etf-nav-shares.json")
    original_facts_hash = digest(facts)
    snapshot = build_rotation_snapshot(state["members"], facts, cutoff)
    if "split_reviews" in facts:
        raw = build_rotation_snapshot(state["members"], {key: value for key, value in facts.items() if key != "split_reviews"}, cutoff)
        snapshot["split_validation"] = split_basis_comparison(raw, snapshot)
    if digest(facts) != original_facts_hash:
        raise ValueError("context calculation mutated source facts")
    if getattr(args, "continuous", False):
        snapshot["refresh_mode"] = "CONTINUOUS_SOURCE_WINDOW_REFRESH"
    _, _, _, after = read_enrollment(args.state_dir / "forward.sqlite", args.session.isoformat())
    snapshot.update(database_snapshot=facts["transaction"], forward_snapshot=retained,
        generated_at=datetime.now(timezone.utc).isoformat(), universe_sha256=digest(state["members"]),
        baseline_policy_sha256=digest(policy), source_facts_sha256=digest(facts),
        input_counts={key: len(facts[key]) for key in ("bars", "references", "actions", "volume_bars")},
        original_publications_unchanged=after["publications_sha256"] == retained["publications_sha256"],
        code_hashes={path: hashlib.sha256((BACKEND / path).read_bytes()).hexdigest() for path in
            ("research/stock_rotation.py", "research/stock_alert_context.py", "research/gics_sectors.py", "equity/market_context_source.py", "scripts/prepare_stock_alert_context.py")},
        elapsed_seconds=round(time.monotonic() - started, 3))
    snapshot = json.loads(json.dumps(snapshot, default=str, allow_nan=False))
    snapshot["snapshot_sha256"] = digest(snapshot)
    write_rotation_snapshot(snapshot, args)


def monitor_rotation(args):
    from threading import Event
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    from database import get_db_connection
    from equity.api import expected_materialized_market_time
    from equity.market_context_source import source_configuration
    from research.stock_rotation import rotation_refresh_due, verify_rotation_snapshot
    lock_name = "stock-screener:market-context-refresh-v1"
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS acquired", (lock_name,))
            if not cursor.fetchone()[0]:
                raise ValueError("market context refresh already owns leadership")
        connection.commit()
        try:
            while True:
                load_dotenv(BACKEND / ".env")
                now = datetime.now(timezone.utc)
                path = BACKEND / "backups/stock-rotation/latest.json"
                snapshot = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
                if snapshot:
                    verify_rotation_snapshot(snapshot)
                expected = expected_materialized_market_time(now, "1d").date().isoformat()
                boundary = expected_materialized_market_time(now, "30m")
                configuration = source_configuration()
                review_hash = digest(load_context_split_reviews(args.split_review, now)) if args.split_review else None
                if rotation_refresh_due(snapshot, expected, boundary, now, macro_configured=configuration["api_key_configured"],
                    macro_states=configuration["series"], split_review_sha256=review_hash):
                    attempt = argparse.Namespace(**vars(args))
                    attempt.session = now.date()
                    attempt.output = BACKEND / "backups/stock-rotation/captures" / (now.strftime("%Y%m%dT%H%M%S%fZ") + ".json")
                    try:
                        state, policy, publications, retained = read_enrollment(attempt.state_dir / "forward.sqlite", attempt.session.isoformat())
                        prepare_rotation(attempt, state, policy, publications, retained)
                    except Exception as error:
                        print(json.dumps(dict(status="REFRESH_FAILED", error_type=type(error).__name__, previous_view_retained=True)), flush=True)
                else:
                    print(json.dumps(dict(status="WAITING_FOR_NEXT_SOURCE_WINDOW", checked_at=now.isoformat(), next_check_seconds=300)), flush=True)
                Event().wait(300)
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_name,))
            connection.commit()


def capture_bond_study(cursor, session, cutoff):
    import exchange_calendars
    from research.stock_rotation import BOND_STUDY_VERSION
    calendar = exchange_calendars.get_calendar("XNYS")
    ending = calendar.date_to_session(session)
    if calendar.session_close(ending) > cutoff:
        raise ValueError("bond study must end at a completed session")
    sessions = [str(item.date()) for item in calendar.sessions_in_range(calendar.session_offset(ending, -252), ending)]
    cursor.execute("SELECT transaction_timestamp() AS transaction_at, current_setting('transaction_read_only') AS read_only, current_setting('transaction_isolation') AS isolation")
    result = dict(schema=BOND_STUDY_VERSION, session=str(ending.date()), cutoff=cutoff.isoformat(), sessions=sessions,
        price_basis="RAW_ACTION_GATED", oas_units="PERCENT", prices={}, references=[], actions=[], database_snapshot=dict(cursor.fetchone()))
    for ticker in ("HYG", "LQD"):
        cursor.execute("""SELECT ticker,security_id::text,security_revision_id::text AS revision_id,security_type,
            active,sic_code,effective_from,observed_at,created_at FROM equity_security_reference_revisions
            WHERE ticker=%s AND effective_from<=%s AND observed_at<=%s AND created_at<=%s
            ORDER BY effective_from DESC,observed_at DESC,created_at DESC,security_revision_id LIMIT 1""", (ticker, cutoff, cutoff, cutoff))
        reference = cursor.fetchone()
        if not reference:
            result["prices"][ticker] = dict(security_id="", bars=[])
            continue
        reference = dict(reference)
        result["references"].append(reference)
        cursor.execute("""SELECT ticker,security_id::text,session_date::text AS session,bar_start,bar_end,
            bar_revision_id::text AS revision_id,open_price::float8 AS open,high_price::float8 AS high,
            low_price::float8 AS low,close_price::float8 AS close,volume::float8,source_kind,system_observed_at,created_at
            FROM equity_bar_revisions WHERE ticker=%s AND security_id=%s::uuid AND interval='1d' AND session_scope='RTH'
              AND NOT adjusted AND is_final AND session_date>=%s AND session_date<=%s
              AND system_observed_at<=%s AND created_at<=%s ORDER BY session_date,created_at LIMIT 1001""",
            (ticker, reference["security_id"], sessions[0], sessions[-1], cutoff, cutoff))
        bars = [dict(row) for row in cursor.fetchall()]
        if len(bars) > 1000:
            raise ValueError("bond study daily revisions exceed bound")
        result["prices"][ticker] = dict(security_id=reference["security_id"], bars=bars)
    identities = [row["security_id"] for row in result["references"]]
    cursor.execute("""SELECT corporate_action_id::text AS revision_id,security_id::text,ticker,action_type,
        effective_date,first_observed_at,created_at FROM equity_corporate_actions
        WHERE security_id=ANY(%s::uuid[]) AND effective_date>=%s AND effective_date<=%s
          AND first_observed_at<=%s AND created_at<=%s ORDER BY effective_date LIMIT 501""",
        (identities, sessions[0], sessions[-1], cutoff, cutoff))
    result["actions"] = [dict(row) for row in cursor.fetchall()]
    if len(result["actions"]) > 500:
        raise ValueError("bond study actions exceed bound")
    return result


def run_bond_study(args):
    from research.stock_rotation import compare_bond_proxy_with_oas, verify_bond_comparison, bond_comparison_view
    if args.verify:
        artifact = json.loads(args.output.read_text(encoding="utf-8"))
        print(json.dumps(verify_bond_comparison(artifact["inputs"], artifact["report"]), indent=2), flush=True)
        return
    if args.output.exists():
        raise ValueError("bond study output already exists; choose a new path")
    if args.bond_from:
        prior = json.loads(args.bond_from.read_text(encoding="utf-8"))
        inputs = prior["inputs"]
        verify_bond_comparison(inputs, prior["report"])
    else:
        from dotenv import load_dotenv
        load_dotenv(BACKEND / ".env")
        from database import get_db_cursor
        from equity.market_context_source import retained_oas_reference
        record, status = retained_oas_reference(args.oas_reference or BACKEND / "backups/stock-rotation/fred/BAMLH0A0HYM2.latest.json")
        with get_db_cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout='30s'")
            cutoff = datetime.now(timezone.utc)
            inputs = capture_bond_study(cursor, args.session, cutoff)
        inputs.update(oas_record=record, reference_status=status, history_mode="RECONSTRUCTED_AT_CAPTURE_NOT_ORIGINAL_DECISION_CONTEXT",
            code_hashes={name: hashlib.sha256((BACKEND / name).read_bytes()).hexdigest() for name in
                ("research/stock_rotation.py", "research/stock_alert_context.py", "equity/market_context_source.py", "scripts/prepare_stock_alert_context.py")})
        inputs = json.loads(json.dumps(inputs, default=str, allow_nan=False))
        inputs["input_sha256"] = digest(inputs)
    report = compare_bond_proxy_with_oas(inputs)
    report["report_sha256"] = digest(report)
    verified = verify_bond_comparison(inputs, report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(dict(inputs=inputs, report=report), stream, indent=2, allow_nan=False)
        stream.write("\n")
    if args.publish_bond_comparison:
        view = bond_comparison_view(inputs, report)
        target = BACKEND / "backups/stock-rotation/bond-comparison.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and utc(json.loads(target.read_text(encoding="utf-8"))["cutoff"]) > utc(inputs["cutoff"]):
            raise ValueError("newer bond comparison exists; refusing rollback")
        temporary = target.with_suffix("." + view["view_sha256"] + ".tmp")
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                json.dump(view, stream, indent=2, allow_nan=False)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    print(json.dumps(dict(verified, output=str(args.output), period=[report["period_start"], report["period_end"]],
        reference_status=report["reference"]["status"], current_proxy=report["current_proxy"]["value"],
        price_states={ticker: len(source["bars"]) for ticker, source in inputs["prices"].items()},
        summary=report["summary"], view_published=args.publish_bond_comparison), indent=2), flush=True)


def collect_financial_pilot(securities, client, repository, retain_receipt):
    from dataclasses import replace
    from equity.polygon import canonical_json, normalize_fundamental_reports
    results = []
    for security in securities:
        response = client.fetch_bounded_financials(security.ticker)
        retain_receipt(security, response)
        if response["status"] != "RECEIVED":
            return dict(status="PROVIDER_UNAVAILABLE", results=results, http_status=response["http_status"],
                failed_ticker=security.ticker, failed_statement=response["failed_statement"])
        statements = {}
        for name, receipt in response["receipts"].items():
            rows = receipt["rows"]
            for row in rows:
                if (security.ticker not in row.get("tickers", []) or not security.cik or
                        str(row.get("cik", "")).zfill(10) != str(security.cik).zfill(10) or
                        row.get("timeframe") != "quarterly" or not row.get("filing_date") or not row.get("period_end")):
                    raise ValueError("financial response identity or period is unverified")
            statements[name] = rows
        reports = normalize_fundamental_reports(security, income_rows=statements["income-statements"],
            balance_rows=statements["balance-sheets"], cash_flow_rows=statements["cash-flow-statements"],
            filing_rows=[], observed_at=utc(response["received_at"]))
        bounded = []
        for report in reports:
            metrics = json.loads(report.metrics_json)
            metrics["free_cash_flow"] = None
            bounded.append(replace(report, metrics_json=canonical_json(metrics),
                quality_codes=tuple(sorted(set(report.quality_codes) | {"STATEMENT_UNITS_NOT_VERIFIED", "CASH_FLOW_PERIOD_BASIS_NOT_VERIFIED", "CASH_FLOW_SIGN_NOT_VERIFIED"}))))
        written = repository.persist_fundamental_reports(bounded)
        results.append(dict(ticker=security.ticker, security_id=str(security.security_id), reports_written=written,
            received_at=response["received_at"], truncated=any(row["truncated"] for row in response["receipts"].values())))
    return dict(status="PILOT_COMPLETE", results=results)


def run_fundamentals_pilot(args):
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    import requests
    from database import get_db_cursor
    from equity.domain import DecisionWatermark
    from equity.polygon import PolygonEquityClient
    from equity.repositories import EquityReferenceRepository
    from research.stock_idea_forward import publish_view
    if args.output.exists():
        raise ValueError("financial pilot output already exists; no repeated acquisition")
    now = datetime.now(timezone.utc)
    state, _, _, retained = read_enrollment(args.state_dir / "forward.sqlite", now.date().isoformat())
    members, cohort = alert_selection_cohort(state, utc(state["next_boundary"]))
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='10s'")
        cursor.execute("""SELECT member.ticker,reference.security_id::text FROM
            unnest(%s::text[],%s::uuid[]) AS member(ticker,security_id)
            CROSS JOIN LATERAL (SELECT security_id,security_type,active FROM equity_security_reference_revisions
                WHERE ticker=member.ticker AND effective_from<=%s AND observed_at<=%s AND created_at<=%s
                ORDER BY effective_from DESC,observed_at DESC,created_at DESC,security_revision_id LIMIT 1) reference
            WHERE reference.security_id=member.security_id AND reference.security_type='CS' AND reference.active
            ORDER BY member.ticker LIMIT 5""", ([row["ticker"] for row in members], [row["security_id"] for row in members], now, now, now))
        selected = [dict(row) for row in cursor.fetchall()]
    repository = EquityReferenceRepository()
    securities = [repository.get_security_as_of(row["ticker"], DecisionWatermark(market_time=now, observed_time=now)) for row in selected]
    if not securities or any(security is None or str(security.security_id) != row["security_id"] for security, row in zip(securities, selected)):
        raise ValueError("financial pilot cohort identity is unavailable")
    report = dict(schema="stock_alert_financial_pilot_v1", started_at=now.isoformat(), cohort=selected,
        enrollment_sha256=digest(state["members"]), selection_cohort=cohort, forward_snapshot=retained,
        maximum_requests=len(securities) * 3, maximum_rows_per_response=2, maximum_bytes_per_response=1_000_000,
        retries=False, pagination=False, status="STARTED", receipts=[])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, default=str, allow_nan=False)
    def retain_receipt(security, response):
        receipt_path = args.output.parent / (args.output.stem + "." + security.ticker + ".receipt.json")
        with receipt_path.open("x", encoding="utf-8") as handle:
            json.dump(dict(security_id=str(security.security_id), ticker=security.ticker, response=response), handle, default=str, allow_nan=False)
        report["receipts"].append(dict(path=receipt_path.name, sha256=hashlib.sha256(receipt_path.read_bytes()).hexdigest()))
        publish_view(args.output, report)
    try:
        with requests.Session() as session:
            result = collect_financial_pilot(securities, PolygonEquityClient(session=session, timeout_seconds=20), repository, retain_receipt)
        report.update(result)
    except Exception as error:
        report.update(status="PILOT_FAILED", error_type=type(error).__name__)
    report["completed_at"] = datetime.now(timezone.utc).isoformat()
    publish_view(args.output, report)
    print(json.dumps({key: value for key, value in report.items() if key not in ("receipts", "forward_snapshot", "selection_cohort")}, indent=2), flush=True)


def review_sector_actions(cursor, cutoff):
    names = ["XLY", "XLE", "XLU", "XLB"]
    cursor.execute("""SELECT action.corporate_action_id::text AS revision_id,action.security_id::text,action.ticker,
        action.action_type,action.effective_date,action.split_from,action.split_to,action.source,
        action.first_observed_at,action.created_at,action.availability_mode,action.payload_sha256,
        before.close_price::float8 AS prior_close,after.open_price::float8 AS next_open,
        before.bar_revision_id::text AS prior_bar_revision,after.bar_revision_id::text AS next_bar_revision
        FROM equity_corporate_actions action
        LEFT JOIN LATERAL (SELECT close_price,bar_revision_id FROM equity_bar_revisions
            WHERE security_id=action.security_id AND ticker=action.ticker AND interval='1d' AND session_scope='RTH'
                AND NOT adjusted AND is_final AND session_date<action.effective_date AND session_date>=action.effective_date-7
                AND system_observed_at<=%s AND created_at<=%s
            ORDER BY session_date DESC,CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1 ELSE 2 END,
                system_observed_at DESC,created_at DESC,bar_revision_id LIMIT 1) before ON TRUE
        LEFT JOIN LATERAL (SELECT open_price,bar_revision_id FROM equity_bar_revisions
            WHERE security_id=action.security_id AND ticker=action.ticker AND interval='1d' AND session_scope='RTH'
                AND NOT adjusted AND is_final AND session_date>=action.effective_date AND session_date<=action.effective_date+7
                AND bar_end<=%s AND system_observed_at<=%s AND created_at<=%s
            ORDER BY session_date,CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1 ELSE 2 END,
                system_observed_at DESC,created_at DESC,bar_revision_id LIMIT 1) after ON TRUE
        WHERE action.ticker=ANY(%s::text[]) AND action.effective_date>=%s AND action.effective_date<=%s
            AND action.action_type IN ('SPLIT','MERGER','SPINOFF','SYMBOL_CHANGE')
            AND action.first_observed_at<=%s AND action.created_at<=%s
        ORDER BY action.ticker,action.effective_date,action.created_at LIMIT 101""",
        (cutoff, cutoff, cutoff, cutoff, cutoff, names, cutoff.date() - timedelta(days=550), cutoff.date(), cutoff, cutoff))
    rows = [dict(row) for row in cursor.fetchall()]
    if len(rows) > 100:
        raise ValueError("proxy action review exceeds declared bound")
    for row in rows:
        before, after = row["prior_close"], row["next_open"]
        split_from, split_to = row["split_from"], row["split_to"]
        row["raw_price_ratio"] = after / before if before and after else None
        row["split_expected_ratio"] = float(split_from / split_to) if split_from and split_to and split_from > 0 and split_to > 0 else None
    return dict(status="REVIEW_ONLY_NO_ADJUSTMENT_APPROVED", checked_at=cutoff.isoformat(), tickers=names, actions=rows,
        provider_requests=False, source_writes=False, limitation="Price ratios are diagnostics, not independent split or continuity certification")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=date.fromisoformat)
    parser.add_argument("--split-review", type=Path, help="Explicit four-proxy reviewed split manifest for rotation only; never changes source bars")
    parser.add_argument("--review-sector-actions", action="store_true", help="Read the four blocked ETF action terms and adjacent raw bars; no repairs or providers")
    parser.add_argument("--fundamentals-pilot", action="store_true", help="Acquire at most two quarterly rows per statement for five enrolled stocks; stop on denial; no pagination/retries")
    parser.add_argument("--state-dir", type=Path, default=BACKEND / "backups/equity-shadow/stock-ideas-forward-v1")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check-sources", action="store_true", help="Report credential presence and approvals only, without requests or database imports")
    parser.add_argument("--fetch-macro", action="store_true", help="Fetch the two configured daily FRED series before a rotation capture")
    parser.add_argument("--continuous", action="store_true", help="Refresh only the separate rotation view when source windows advance; checks every five minutes")
    parser.add_argument("--verify", action="store_true", help="Verify existing artifacts offline; no database reads or writes")
    parser.add_argument("--rotation-only", action="store_true", help="Capture daily rotation only; no options or event reads")
    parser.add_argument("--company-context", action="store_true", help="Capture retained prices, earnings and filed financials only; no options inventory or provider requests")
    parser.add_argument("--freeze-alert-context", action="store_true", help="Verify an existing company-context output and append immutable sidecars; no capture or provider calls")
    parser.add_argument("--publish-rotation-view", action="store_true", help="Atomically publish the separate latest rotation view")
    parser.add_argument("--rotation-from", type=Path, help="Compact an existing verified rotation artifact offline; no database reads")
    parser.add_argument("--bond-study", action="store_true", help="Capture HYG/LQD history and compare with approved retained OAS; no provider calls")
    parser.add_argument("--bond-from", type=Path, help="Recompute an existing frozen bond study offline into a new artifact")
    parser.add_argument("--oas-reference", type=Path, help="Exact retained FRED credit receipt for the bond study; approval remains required")
    parser.add_argument("--publish-bond-comparison", action="store_true", help="Publish only the separate verified bond comparison summary")
    args = parser.parse_args()
    if args.split_review and (not args.rotation_only or args.verify or args.rotation_from):
        parser.error("Split review requires a new rotation capture or its continuous worker")
    if args.review_sector_actions:
        if any((args.fundamentals_pilot, args.company_context, args.freeze_alert_context, args.verify, args.rotation_only,
                args.continuous, args.fetch_macro, args.rotation_from, args.publish_rotation_view, args.bond_study,
                args.bond_from, args.oas_reference, args.publish_bond_comparison, args.check_sources)):
            parser.error("Proxy action review cannot be combined with capture or acquisition")
        if args.output and args.output.exists():
            parser.error("Use a new review output path")
        from dotenv import load_dotenv
        load_dotenv(BACKEND / ".env")
        from database import get_db_cursor
        with get_db_cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout='20s'")
            report = review_sector_actions(cursor, datetime.now(timezone.utc))
        if args.output:
            with args.output.open("x", encoding="utf-8") as handle:
                json.dump(report, handle, indent=2, default=str, allow_nan=False)
        print(json.dumps(report, indent=2, default=str, allow_nan=False), flush=True)
        return
    if args.fundamentals_pilot:
        if not args.output or any((args.company_context, args.freeze_alert_context, args.verify, args.rotation_only,
                args.continuous, args.fetch_macro, args.rotation_from, args.publish_rotation_view, args.bond_study,
                args.bond_from, args.oas_reference, args.publish_bond_comparison, args.check_sources)):
            parser.error("Financial pilot needs a new output and cannot be combined with other modes")
        run_fundamentals_pilot(args)
        return
    if args.freeze_alert_context and (not args.company_context or args.verify):
        parser.error("Freezing requires company-context and cannot be combined with verify")
    if args.company_context and any((args.rotation_only, args.continuous, args.fetch_macro, args.rotation_from,
            args.publish_rotation_view, args.bond_study, args.bond_from, args.oas_reference, args.publish_bond_comparison, args.check_sources)):
        parser.error("Company context cannot be combined with rotation, bond, acquisition or continuous modes")
    if args.bond_study:
        if not args.output or (not args.session and not args.bond_from and not args.verify) or any((args.rotation_only, args.continuous, args.fetch_macro, args.rotation_from, args.publish_rotation_view, args.check_sources)):
            parser.error("Bond study requires output and session/from/verify, without rotation or acquisition modes")
        if (args.verify and (args.publish_bond_comparison or args.bond_from or args.oas_reference)) or (args.bond_from and args.oas_reference):
            parser.error("Offline bond study cannot change reference inputs or publish while verifying")
        run_bond_study(args)
        return
    if args.bond_from or args.oas_reference or args.publish_bond_comparison:
        parser.error("Bond study options require --bond-study")
    if args.check_sources:
        from dotenv import load_dotenv
        from equity.market_context_source import source_configuration
        load_dotenv(BACKEND / ".env")
        print(json.dumps(source_configuration(), indent=2), flush=True)
        return
    if args.continuous:
        if not args.rotation_only or not args.publish_rotation_view or args.verify or args.rotation_from or args.output:
            parser.error("Continuous mode requires rotation-only and publish-rotation-view, without verify/from/output")
        monitor_rotation(args)
        return
    if args.session is None or args.output is None:
        parser.error("--session and --output are required for capture or verification")
    if args.fetch_macro and (not args.rotation_only or args.verify or args.rotation_from):
        parser.error("Macro acquisition requires a new rotation-only capture")
    if args.publish_rotation_view and (not args.rotation_only or args.verify):
        parser.error("Publishing requires a new rotation-only capture")
    if args.rotation_from and (not args.rotation_only or args.verify):
        parser.error("Offline rotation projection requires rotation-only mode without --verify")
    if args.verify:
        if args.rotation_only:
            from research.stock_rotation import verify_rotation_snapshot
            print(json.dumps(verify_rotation_snapshot(json.loads(args.output.read_text(encoding="utf-8"))), indent=2), flush=True)
        else:
            verify_artifacts(args.output)
        return
    if args.freeze_alert_context:
        freeze_alert_context(args)
        return
    annotations_path = args.output.with_suffix(".annotations.json")
    if args.output.exists() or annotations_path.exists():
        parser.error("Use new output paths; existing evidence is never overwritten")
    if args.rotation_from:
        write_rotation_snapshot(json.loads(args.rotation_from.read_text(encoding="utf-8")), args)
        return
    state, policy, publications, retained = read_enrollment(args.state_dir / "forward.sqlite", args.session.isoformat())
    if args.rotation_only:
        prepare_rotation(args, state, policy, publications, retained)
        return
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    from database import get_db_cursor
    from options.config import load_option_runtime_configuration
    configuration = load_option_runtime_configuration()
    started = time.monotonic()
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cutoff = datetime.now(timezone.utc)
        facts = capture(cursor, state["members"], publications, cutoff, configuration, company_only=args.company_context)
    result = build_manifest(state, policy, publications, facts, cutoff, configuration)
    selection_members, selection_cohort = alert_selection_cohort(state, utc(state["next_boundary"]))
    hashes = {path: hashlib.sha256((BACKEND / path).read_bytes()).hexdigest() for path in
        ("research/stock_alert_context.py", "research/gics_sectors.py", "scripts/prepare_stock_alert_context.py")}
    manifest = dict(schema_version=READINESS_VERSION, generated_at=datetime.now(timezone.utc).isoformat(), audit_cutoff=cutoff.isoformat(),
        database_snapshot=facts["transaction"], forward_snapshot=retained, session=args.session.isoformat(),
        universe=state["members"], universe_sha256=digest(state["members"]), baseline_policy_sha256=digest(policy),
        alert_selection_members=len(selection_members), alert_selection_cohort=selection_cohort,
        publication_hashes={row["window_key"]: digest(row) for row in publications},
        quality_policy_sha256=digest(forward_config(quality_version=2)), code_hashes=hashes,
        options_configuration_sha256=configuration.configuration_sha256, settlement_policy_sha256=configuration.settlement_valuation_policy_sha256,
        options_covered_stocks=[dict(security_id=row["security_id"], ticker=row["ticker"]) for row in result["contexts"]["CURRENT_READINESS"]["members"] if row["options_covered_stock"]],
        required_iv_samples=configuration.settlement_valuation_policy.minimum_iv_sample_sessions,
        required_iv_coverage=configuration.settlement_valuation_policy.minimum_iv_coverage_fraction,
        required_iv_lookback=configuration.settlement_valuation_policy.iv_lookback_sessions,
        raw_iv_inventory=facts.get("raw_iv_inventory", []), comparable_iv_contexts=len(facts["iv"]),
        raw_iv_inventory_clock_columns=facts.get("raw_iv_inventory_clock_columns", []),
        raw_iv_inventory_role="CURRENT_INVENTORY_ONLY_NOT_HISTORICAL_READINESS", rates_inventory=facts.get("rates_inventory", []),
        financial_reports=len(facts.get("fundamentals", [])), company_context_only=args.company_context,
        financial_inventory=facts.get("financial_inventory"),
        action_coverage_inventory=facts["action_coverage"],
        flow_inventory=facts["prints"], capability_tables=facts["capability_tables"], usage_rights="NOT_VERIFIED_NO_NEW_ACQUISITION",
        observations=result["contexts"], summary=result["summary"], scope="CURRENT_COHORT_AND_RETAINED_SELECTED_DECISIONS_ONLY",
        limitations=["Prior inspected session is development evidence, not held-out validation", "Raw action-gated prices; no automatic continuity repair",
            "Flow classification history and OI settlement timestamps are not certified", "No provider, activation, probability or selection changes"],
        elapsed_seconds=round(time.monotonic() - started, 3))
    annotations = dict(schema_version=VERSION, generated_at=manifest["generated_at"], readiness_manifest_sha256=digest(manifest),
        universe_sha256=manifest["universe_sha256"], annotations_only=True,
        publication_contexts={key: {field: bundle[field] for field in ("market", "spy", "qqq", "cutoff", "prior_session")} for key, bundle in result["contexts"].items()},
        rows=result["annotations"])
    _, _, _, after = read_enrollment(args.state_dir / "forward.sqlite", args.session.isoformat())
    manifest["original_publications_unchanged"] = after["publications_sha256"] == retained["publications_sha256"]
    if not manifest["original_publications_unchanged"]:
        raise ValueError("source publications advanced during capture; no mixed artifact published")
    annotations["readiness_manifest_sha256"] = digest(manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    annotations_path.write_text(json.dumps(annotations, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(dict(readiness_manifest=str(args.output), annotations=str(annotations_path),
        original_publications_unchanged=True, annotations_count=len(result["annotations"]),
        alert_selection_members=len(selection_members), financial_inventory=facts.get("financial_inventory"),
        current_readiness=result["summary"]["CURRENT_READINESS"], comparable_iv_contexts=len(facts["iv"]),
        options_covered_stocks=[row["ticker"] for row in manifest["options_covered_stocks"]], elapsed_seconds=manifest["elapsed_seconds"]), indent=2), flush=True)


if __name__ == "__main__":
    main()