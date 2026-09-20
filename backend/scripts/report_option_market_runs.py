from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_connection
from options.calendar import OptionExchangeCalendar
from options.config import load_option_runtime_configuration
from options.worker import OptionWorkerSettings


def session_validation(cursor, configuration, opened, due, settings) -> dict[str, object]:
    if due is None or due <= opened:
        return {"status": "NO_OBSERVABLE_SLOT", "expected_member_slots": 0}
    members = list(configuration.settings.underlyers)
    cursor.execute(
        """
        WITH scheduled AS MATERIALIZED (
            SELECT DISTINCT ON (underlying, scheduled_cycle) * FROM option_ingestion_runs
            WHERE underlying = ANY(%s) AND configuration_sha256 = %s AND policy_sha256 = %s
              AND scheduled_cycle > %s AND scheduled_cycle <= %s
              AND mod(extract(epoch FROM (scheduled_cycle - %s::timestamptz)), %s) = 0
            ORDER BY underlying, scheduled_cycle, started_at DESC
        ), grid AS (
            SELECT member.underlying, slot FROM unnest(%s::text[]) member(underlying)
            CROSS JOIN generate_series(%s::timestamptz + %s * interval '1 second', %s::timestamptz, %s * interval '1 second') AS slot
        )
        SELECT grid.slot, count(*) AS expected,
               count(scheduled.batch_id) AS ingestion_present,
               count(*) FILTER (WHERE scheduled.status = 'COMPLETE') AS ingestion_complete,
               count(*) FILTER (WHERE scheduled.status = 'FAILED') AS ingestion_failed,
               count(*) FILTER (WHERE scheduled.status = 'COMPLETE' AND analysis.status = 'COMPLETE') AS analysis_complete,
               COALESCE(array_agg(grid.underlying ORDER BY grid.underlying) FILTER (WHERE scheduled.batch_id IS NULL), ARRAY[]::text[]) AS missing_underlyings,
               COALESCE(array_agg(grid.underlying ORDER BY grid.underlying) FILTER (WHERE scheduled.batch_id IS NOT NULL AND (scheduled.status <> 'COMPLETE' OR analysis.status IS DISTINCT FROM 'COMPLETE')), ARRAY[]::text[]) AS incomplete_underlyings
        FROM grid LEFT JOIN scheduled ON scheduled.underlying = grid.underlying AND scheduled.scheduled_cycle = grid.slot
        LEFT JOIN LATERAL (SELECT status FROM option_analysis_runs WHERE batch_id = scheduled.batch_id AND policy_sha256 = %s ORDER BY observed_time DESC LIMIT 1) AS analysis ON TRUE
        GROUP BY grid.slot ORDER BY grid.slot
        """,
        (members, configuration.configuration_sha256, configuration.policy_sha256, opened, due, opened, settings.slot_seconds,
         members, opened, settings.slot_seconds, due, settings.slot_seconds, configuration.policy_sha256),
    )
    slots = [dict(row) for row in cursor.fetchall()]
    cursor.execute(
        """
        WITH batches AS MATERIALIZED (
            SELECT batch_id, underlying, retained_row_count FROM option_ingestion_runs
            WHERE underlying = ANY(%s) AND configuration_sha256 = %s AND policy_sha256 = %s
              AND scheduled_cycle > %s AND scheduled_cycle <= %s
              AND mod(extract(epoch FROM (scheduled_cycle - %s::timestamptz)), %s) = 0
        )
        SELECT batches.underlying, count(snapshot.snapshot_id) AS snapshots,
               count(snapshot.model_mark) AS model_marks,
               count(*) FILTER (WHERE snapshot.iv_converged) AS iv_converged,
               count(*) FILTER (WHERE snapshot.snapshot_id IS NOT NULL AND snapshot.model_mark IS NULL) AS diagnostic_only,
               count(*) FILTER (WHERE snapshot.snapshot_id IS NOT NULL AND (catalog.contract_id IS NULL OR version.contract_id IS NULL)) AS missing_contract_reference,
               count(*) FILTER (WHERE catalog.contract_id IS NOT NULL AND
                    (snapshot.underlying IS DISTINCT FROM catalog.underlying OR snapshot.contract_ticker IS DISTINCT FROM catalog.contract_ticker
                     OR snapshot.contract_type IS DISTINCT FROM version.contract_type OR snapshot.strike IS DISTINCT FROM version.strike
                     OR snapshot.expiration_date IS DISTINCT FROM version.expiration_date
                     OR snapshot.shares_per_contract IS DISTINCT FROM version.shares_per_contract)) AS identity_mismatch,
               count(*) FILTER (WHERE snapshot.strike <= 0 OR snapshot.spot <= 0 OR snapshot.model_mark <= 0
                    OR snapshot.day_volume < 0 OR snapshot.open_interest < 0
                    OR snapshot.spot::text IN ('NaN','Infinity','-Infinity') OR snapshot.model_mark::text IN ('NaN','Infinity','-Infinity')) AS invalid_numeric,
               count(*) FILTER (WHERE snapshot.first_observed_at < GREATEST(snapshot.market_data_time, snapshot.spot_market_data_time, snapshot.mark_market_data_time)
                    OR snapshot.revised_observed_at < snapshot.first_observed_at OR snapshot.expiration_cutoff <= snapshot.market_data_time
                    OR snapshot.calendar_dte <> snapshot.expiration_date - (snapshot.market_data_time AT TIME ZONE 'America/New_York')::date) AS invalid_timing,
               count(*) FILTER (WHERE snapshot.model_mark IS NOT NULL AND
                    (snapshot.valuation_policy_version IS NULL OR snapshot.valuation_policy_sha256 IS DISTINCT FROM %s
                     OR NOT(snapshot.mark_source = ANY(%s)))) AS invalid_valuation_provenance,
               count(*) FILTER (WHERE snapshot.model_mark IS NOT NULL AND
                    (extract(epoch FROM (snapshot.first_observed_at - snapshot.mark_market_data_time)) > %s
                     OR abs(extract(epoch FROM (snapshot.spot_market_data_time - snapshot.mark_market_data_time))) > %s)) AS model_mark_age_or_skew,
               count(*) FILTER (WHERE snapshot.iv_converged AND
                    (snapshot.local_iv IS NULL OR snapshot.local_iv <= 0 OR snapshot.local_delta IS NULL OR snapshot.local_gamma IS NULL
                     OR snapshot.local_theta_per_day IS NULL OR snapshot.local_vega_per_vol_point IS NULL OR snapshot.local_rho_per_rate_point IS NULL)) AS converged_missing_greeks,
               min(snapshot.market_data_time) AS first_market_time, max(snapshot.market_data_time) AS last_market_time,
               max(snapshot.first_observed_at) AS last_observed_at
        FROM batches LEFT JOIN option_chain_snapshots AS snapshot USING (batch_id)
        LEFT JOIN option_contract_catalog AS catalog USING (contract_id)
                LEFT JOIN LATERAL (
                        SELECT * FROM option_contract_catalog_versions AS version
                        WHERE version.contract_id = snapshot.contract_id
                            AND version.valid_from <= snapshot.market_data_time
                            AND (version.valid_to IS NULL OR version.valid_to > snapshot.market_data_time)
                            AND version.first_observed_at <= snapshot.first_observed_at
                            AND COALESCE(version.revised_observed_at, version.first_observed_at) <= snapshot.first_observed_at
                        ORDER BY version.valid_from DESC, version.first_observed_at DESC LIMIT 1
                ) AS version ON TRUE
        GROUP BY batches.underlying ORDER BY batches.underlying
        """,
        (members, configuration.configuration_sha256, configuration.policy_sha256, opened, due, opened, settings.slot_seconds,
         configuration.valuation_policy_sha256, [item.value for item in configuration.valuation_policy.allowed_entry_mark_sources],
         configuration.valuation_policy.maximum_source_age_seconds, configuration.valuation_policy.maximum_option_spot_skew_seconds),
    )
    snapshot_checks = [dict(row) for row in cursor.fetchall()]
    cursor.execute(
        """
        SELECT flag, count(*) AS occurrences FROM option_chain_snapshots AS snapshot
        JOIN option_ingestion_runs AS ingestion USING (batch_id)
        CROSS JOIN LATERAL unnest(snapshot.quality_flags) AS flag
        WHERE ingestion.configuration_sha256 = %s AND ingestion.policy_sha256 = %s
          AND ingestion.scheduled_cycle > %s AND ingestion.scheduled_cycle <= %s
          AND mod(extract(epoch FROM (ingestion.scheduled_cycle - %s::timestamptz)), %s) = 0
        GROUP BY flag ORDER BY flag
        """, (configuration.configuration_sha256, configuration.policy_sha256, opened, due, opened, settings.slot_seconds),
    )
    flags = [dict(row) for row in cursor.fetchall()]
    cursor.execute(
        """
        WITH candidates AS MATERIALIZED (
            SELECT candidate.* FROM option_strategy_candidates AS candidate
            JOIN option_analysis_runs AS analysis USING (matrix_id)
            JOIN option_ingestion_runs AS ingestion USING (batch_id)
            WHERE ingestion.configuration_sha256 = %s AND ingestion.policy_sha256 = %s AND candidate.policy_sha256 = %s
              AND ingestion.scheduled_cycle > %s AND ingestion.scheduled_cycle <= %s
              AND mod(extract(epoch FROM (ingestion.scheduled_cycle - %s::timestamptz)), %s) = 0
        )
        SELECT count(*) AS candidates, count(*) FILTER (WHERE candidate.status = 'SELECTED') AS selected,
               count(*) FILTER (WHERE candidate.status = 'SUPPRESSED') AS suppressed,
               count(*) FILTER (WHERE candidate.execution_eligibility IS NOT NULL) AS execution_eligible,
               count(*) FILTER (WHERE candidate.observed_time < candidate.market_data_time) AS invalid_candidate_time,
               count(*) FILTER (WHERE candidate.candidate_kind = 'RESEARCH_ONLY' AND EXISTS(SELECT 1 FROM option_candidate_legs WHERE candidate_id = candidate.candidate_id)) AS observation_with_legs,
               count(*) FILTER (WHERE (SELECT count(*) FROM option_candidate_execution_gates WHERE candidate_id = candidate.candidate_id AND ledger_version = 'gate_ledger_v2') <> 6) AS gate_ledger_mismatch,
               count(*) FILTER (WHERE EXISTS(
                   SELECT 1 FROM option_candidate_legs AS leg LEFT JOIN option_chain_snapshots AS snapshot ON snapshot.snapshot_id = leg.snapshot_id
                   WHERE leg.candidate_id = candidate.candidate_id AND
                    (snapshot.snapshot_id IS NULL OR leg.contract_id IS DISTINCT FROM snapshot.contract_id
                     OR leg.contract_ticker IS DISTINCT FROM snapshot.contract_ticker OR leg.strike IS DISTINCT FROM snapshot.strike
                     OR leg.expiration_date IS DISTINCT FROM snapshot.expiration_date OR leg.contract_type IS DISTINCT FROM snapshot.contract_type
                     OR leg.multiplier IS DISTINCT FROM snapshot.shares_per_contract OR leg.model_mark IS DISTINCT FROM snapshot.model_mark
                     OR leg.valuation_policy_sha256 IS DISTINCT FROM snapshot.valuation_policy_sha256
                     OR leg.source_market_time > candidate.market_data_time OR snapshot.first_observed_at > candidate.observed_time)
               )) AS invalid_leg_lineage
        FROM candidates AS candidate
        """,
        (configuration.configuration_sha256, configuration.policy_sha256, configuration.strategy_policy_sha256, opened, due, opened, settings.slot_seconds),
    )
    candidate_checks = dict(cursor.fetchone())
    cursor.execute(
        """
        SELECT count(*) AS active_queries,
               COALESCE(jsonb_agg(jsonb_build_object('pid', pid, 'state', state, 'wait_type', wait_event_type,
                 'wait_event', wait_event, 'query_age_seconds', extract(epoch FROM (clock_timestamp() - query_start)),
                 'blocked_by', pg_blocking_pids(pid),
                 'query_family', CASE WHEN query ILIKE '%%option_signal%%' THEN 'OPTION_OUTCOME'
                                      WHEN query ILIKE '%%option_%%' THEN 'OPTION_OTHER' ELSE 'OTHER' END)), '[]'::jsonb) AS queries
        FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()
          AND state <> 'idle' AND query ILIKE '%%option_%%'
        """
    )
    database_activity = dict(cursor.fetchone())
    numeric_violations = sum(row[key] for row in snapshot_checks for key in (
        'missing_contract_reference', 'identity_mismatch', 'invalid_numeric', 'invalid_timing',
        'invalid_valuation_provenance', 'model_mark_age_or_skew', 'converged_missing_greeks'))
    candidate_violations = sum(candidate_checks[key] for key in ('invalid_candidate_time', 'observation_with_legs', 'gate_ledger_mismatch', 'invalid_leg_lineage'))
    return {
        "status": "COMPLETE" if all(row["analysis_complete"] == row["expected"] for row in slots) else "INCOMPLETE",
        "expected_member_slots": sum(row["expected"] for row in slots),
        "present_member_slots": sum(row["ingestion_present"] for row in slots),
        "complete_member_slots": sum(row["analysis_complete"] for row in slots),
        "missing_member_slots": sum(row["expected"] - row["ingestion_present"] for row in slots),
        "snapshot_totals": {key: sum(row[key] for row in snapshot_checks) for key in
                    ('snapshots', 'model_marks', 'iv_converged', 'diagnostic_only', 'missing_contract_reference',
                     'identity_mismatch', 'invalid_numeric', 'invalid_timing', 'invalid_valuation_provenance',
                     'model_mark_age_or_skew', 'converged_missing_greeks')},
        "slots": slots, "snapshot_checks": snapshot_checks, "quality_flag_occurrences": flags,
        "candidate_checks": candidate_checks, "database_activity": database_activity,
        "retained_integrity_verdict": "REVIEW_REQUIRED" if numeric_violations + candidate_violations else "PASS_CHECKED_INVARIANTS" if any(row["snapshots"] for row in snapshot_checks) else "NO_RETAINED_SNAPSHOTS",
        "provider_price_crosscheck_performed": False,
    }


def report(*, validate_session: bool = False) -> dict[str, object]:
    configuration = load_option_runtime_configuration()
    settings = OptionWorkerSettings.from_environment()
    with get_db_connection() as connection:
        connection.rollback()
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout = '20s'")
                cursor.execute("SELECT clock_timestamp() AS checked_at")
                now = cursor.fetchone()["checked_at"]
                exchange = exchange_calendars.get_calendar("XNYS")
                today = pd.Timestamp(now.astimezone(ZoneInfo("America/New_York")).date())
                session = exchange.date_to_session(today, direction="previous")
                opened = exchange.session_open(session).to_pydatetime().astimezone(timezone.utc)
                closed = exchange.session_close(session).to_pydatetime().astimezone(timezone.utc)
                is_open = opened <= now < closed
                if now < opened:
                    next_open = opened
                else:
                    next_open = exchange.session_open(exchange.next_session(session)).to_pydatetime().astimezone(timezone.utc)
                due = OptionExchangeCalendar().latest_delayed_slot(
                    now, interval=timedelta(seconds=settings.slot_seconds),
                    provider_delay=timedelta(seconds=settings.provider_delay_seconds),
                    publication_grace=timedelta(seconds=settings.publication_grace_seconds),
                )
                cursor.execute(
                    """
                    SELECT version FROM public.schema_migrations
                    WHERE version = '043_option_alert_publications'
                    """
                )
                registered = cursor.fetchone() is not None
                cursor.execute("SELECT to_regclass('public.option_alert_plans') IS NOT NULL AND to_regclass('public.option_alert_publication_events') IS NOT NULL AS ready")
                schema_ready = cursor.fetchone()["ready"]
                schema = {"registered": registered, "tables_present": schema_ready}
                if schema_ready:
                    cursor.execute("SELECT (SELECT COUNT(*) FROM public.option_alert_plans) AS plans, (SELECT COUNT(*) FROM public.option_alert_publication_events) AS events")
                    schema.update(dict(cursor.fetchone()))
                    cursor.execute(
                        """
                        SELECT c.relname, t.tgname, t.tgenabled
                        FROM pg_trigger AS t JOIN pg_class AS c ON c.oid = t.tgrelid
                        JOIN pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public' AND c.relname IN ('option_alert_plans', 'option_alert_publication_events')
                          AND NOT t.tgisinternal ORDER BY c.relname, t.tgname
                        """
                    )
                    schema["triggers"] = [dict(row) for row in cursor.fetchall()]
                cursor.execute("SELECT COUNT(*) AS remaining FROM pg_namespace WHERE nspname LIKE 'option_alert_test_%'")
                schema["remaining_test_schemas"] = cursor.fetchone()["remaining"]
                cursor.execute(
                    """
                    SELECT instance_id, status, last_heartbeat_at, configuration_sha256
                    FROM option_scheduler_instances ORDER BY last_heartbeat_at DESC LIMIT 1
                    """
                )
                leader = cursor.fetchone()
                if leader:
                    leader = dict(leader)
                    leader["heartbeat_age_seconds"] = round((now - leader["last_heartbeat_at"]).total_seconds(), 1)
                    leader["active_configuration"] = leader["configuration_sha256"] == configuration.configuration_sha256
                cursor.execute(
                    """
                    SELECT member.underlying, run.batch_id, run.scheduled_cycle, run.status,
                           run.started_at, run.completed_at, run.market_data_time,
                           run.received_row_count, run.retained_row_count, run.unknown_reference_count,
                           run.error_category, run.failure_reason,
                           analysis.matrix_id, analysis.status AS analysis_status,
                           analysis.market_time AS analysis_market_time, analysis.observed_time AS analysis_observed_at,
                           completed.scheduled_cycle AS latest_complete_cycle,
                           candidates.total AS candidates, candidates.selected, candidates.suppressed
                    FROM unnest(%s::text[]) AS member(underlying)
                    LEFT JOIN LATERAL (
                        SELECT * FROM option_ingestion_runs
                        WHERE underlying = member.underlying AND configuration_sha256 = %s AND policy_sha256 = %s
                                                    AND mod(extract(epoch FROM (scheduled_cycle - %s::timestamptz)), %s) = 0
                        ORDER BY scheduled_cycle DESC, started_at DESC LIMIT 1
                    ) AS run ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT * FROM option_analysis_runs WHERE batch_id = run.batch_id AND policy_sha256 = %s
                        ORDER BY observed_time DESC LIMIT 1
                    ) AS analysis ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT ingestion.scheduled_cycle FROM option_ingestion_runs AS ingestion
                        JOIN option_analysis_runs AS completed_analysis USING (batch_id)
                        WHERE ingestion.underlying = member.underlying AND ingestion.configuration_sha256 = %s
                          AND ingestion.policy_sha256 = %s AND completed_analysis.policy_sha256 = %s
                          AND ingestion.status = 'COMPLETE' AND completed_analysis.status = 'COMPLETE'
                        ORDER BY ingestion.scheduled_cycle DESC LIMIT 1
                    ) AS completed ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 'SELECTED') AS selected,
                               COUNT(*) FILTER (WHERE status = 'SUPPRESSED') AS suppressed
                        FROM option_strategy_candidates WHERE matrix_id = analysis.matrix_id AND policy_sha256 = %s
                    ) AS candidates ON TRUE
                    ORDER BY member.underlying
                    """,
                    (list(configuration.settings.underlyers), configuration.configuration_sha256, configuration.policy_sha256, opened, settings.slot_seconds,
                     configuration.policy_sha256, configuration.configuration_sha256, configuration.policy_sha256,
                     configuration.policy_sha256, configuration.strategy_policy_sha256),
                )
                runs = [dict(row) for row in cursor.fetchall()]
                for row in runs:
                    row["due_cycle_complete"] = due is not None and row["latest_complete_cycle"] == due
                    row["completed_slot_lag_seconds"] = (due - row["latest_complete_cycle"]).total_seconds() if due is not None and row["latest_complete_cycle"] else None
                cursor.execute(
                    """
                    SELECT stage, status, COUNT(*) AS items
                    FROM option_work_items
                    WHERE status IN ('PENDING', 'RETRY', 'CLAIMED')
                    GROUP BY stage, status ORDER BY stage, status
                    """
                )
                work = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT work.work_id, work.stage, work.status, work.attempt_count, work.maximum_attempts,
                           work.next_attempt_at, work.updated_at, ingestion.underlying, ingestion.scheduled_cycle,
                           work.next_attempt_at <= NOW() AND work.attempt_count < work.maximum_attempts AS retry_due,
                           CASE WHEN work.last_error ILIKE '%%transport%%' THEN 'PROVIDER_TRANSPORT'
                                WHEN work.last_error ILIKE '%%expired%%' THEN 'EXPIRED_INPUT_OR_LEASE'
                                WHEN work.last_error ILIKE '%%source%%age%%' THEN 'SOURCE_AGE'
                                ELSE 'OTHER_RETAINED_ERROR' END AS error_class
                    FROM option_work_items AS work
                    LEFT JOIN option_analysis_runs AS analysis ON work.stage = 'STRATEGY' AND work.subject_id = analysis.matrix_id::text
                    JOIN option_ingestion_runs AS ingestion ON
                        (work.stage = 'NORMALIZE' AND work.subject_id = ingestion.batch_id::text)
                        OR (work.stage = 'STRATEGY' AND analysis.batch_id = ingestion.batch_id)
                    WHERE work.status = 'RETRY' AND work.stage IN ('NORMALIZE','STRATEGY')
                      AND ingestion.configuration_sha256 = %s AND ingestion.policy_sha256 = %s
                    ORDER BY ingestion.scheduled_cycle DESC, work.stage, ingestion.underlying LIMIT 30
                    """, (configuration.configuration_sha256, configuration.policy_sha256),
                )
                retries = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT publication_id, scheduled_cycle, published_at, covered_underlying_count, expected_underlying_count
                    FROM option_board_publications WHERE configuration_sha256 = %s AND strategy_policy_sha256 = %s AND status = 'COMPLETE'
                    ORDER BY scheduled_cycle DESC, published_at DESC LIMIT 1
                    """, (configuration.configuration_sha256, configuration.strategy_policy_sha256),
                )
                board = cursor.fetchone()
                covered = sum(row["due_cycle_complete"] for row in runs)
                session_checks = session_validation(cursor, configuration, opened, due, settings) if validate_session else None
                return {
                    "checked_at": now, "checked_at_et": now.astimezone(ZoneInfo("America/New_York")).isoformat(),
                    "market_open": is_open, "session": session.date().isoformat(), "session_open": opened, "session_close": closed,
                    "next_session_open": next_open,
                    "next_first_observable_slot_check": next_open + timedelta(seconds=settings.slot_seconds + settings.provider_delay_seconds + settings.publication_grace_seconds),
                    "expected_delayed_slot": due, "provider_delay_seconds": settings.provider_delay_seconds,
                    "publication_grace_seconds": settings.publication_grace_seconds,
                    "market_hours_verdict": "OUTSIDE_MARKET_HOURS" if not is_open else "NO_SLOT_DUE" if due is None else "CURRENT" if covered == len(runs) else "INCOMPLETE_OR_STALE",
                    "data_readiness_verdict": "NO_SLOT_DUE" if due is None else "CURRENT" if covered == len(runs) else "INCOMPLETE_OR_STALE",
                    "due_complete_underlyings": covered, "configured_underlyings": len(runs),
                    "configuration_sha256": configuration.configuration_sha256, "market_policy_sha256": configuration.policy_sha256,
                    "strategy_policy_sha256": configuration.strategy_policy_sha256,
                    "publication_schema": schema, "leader": leader, "latest_runs": runs, "pending_work": work,
                    "retained_retries": retries,
                    "latest_board": dict(board) if board else None,
                    "new_alert_publisher_activated_by_this_check": False, "data_mutated": False,
                    "session_validation": session_checks,
                    "observable_session_slots_complete": bool(session_checks and session_checks["status"] == "COMPLETE"),
                    "historical_all_slots_verified": False,
                }
        finally:
            connection.rollback()
            connection.set_session(readonly=False, isolation_level="READ COMMITTED")


def main():
    parser = argparse.ArgumentParser(description="Read-only latest option run and publication-schema audit; no workers or provider calls.")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-market-open", action="store_true")
    parser.add_argument("--validate-session", action="store_true", help="Check all currently observable slots and retained contract/candidate invariants for the current session, including after close.")
    args = parser.parse_args()
    payload = report(validate_session=True) if args.validate_session else report()
    serialized = json.dumps(payload, default=str, indent=2, allow_nan=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized, flush=True)
    if args.require_market_open:
        if not payload["market_open"]:
            return 2
        if payload["market_hours_verdict"] != "CURRENT":
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())