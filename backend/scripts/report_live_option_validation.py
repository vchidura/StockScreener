#!/usr/bin/env python3
"""Validate the newest completed option slot across every production boundary."""
from __future__ import annotations

import json
import sys
from datetime import timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402


SQL_LATEST_SLOT = """
SELECT MAX(run.scheduled_cycle) AS scheduled_cycle
FROM option_ingestion_runs run
JOIN option_analysis_runs analysis USING (batch_id)
WHERE analysis.status = 'COMPLETE'
"""

SQL_ANALYSIS = """
SELECT analysis.status, COUNT(*) AS matrices,
       COUNT(DISTINCT analysis.underlying) AS underlyings,
       SUM(analysis.received_contract_count) AS received,
       SUM(analysis.eligible_contract_count) AS eligible,
       SUM(analysis.iv_attempt_count) AS iv_attempted,
       SUM(analysis.iv_converged_count) AS iv_converged,
       MIN(analysis.iv_convergence_fraction) AS minimum_iv_convergence,
       MAX(analysis.observed_time - run.scheduled_cycle) AS maximum_execution_lag
FROM option_analysis_runs analysis
JOIN option_ingestion_runs run USING (batch_id)
WHERE run.scheduled_cycle = %s
GROUP BY analysis.status
ORDER BY analysis.status
"""

SQL_SNAPSHOTS = """
SELECT COUNT(*) AS snapshots,
       COUNT(*) FILTER (WHERE snapshot.model_mark IS NOT NULL) AS model_marks,
       COUNT(*) FILTER (WHERE snapshot.local_iv IS NOT NULL) AS local_ivs,
       COUNT(*) FILTER (WHERE snapshot.mark_market_data_time IS NOT NULL
                         AND snapshot.spot_market_data_time IS NOT NULL
                         AND abs(extract(epoch FROM (
                             snapshot.mark_market_data_time
                             - snapshot.spot_market_data_time
                         ))) <= 60) AS aligned_within_60s,
       COUNT(*) FILTER (WHERE 'MARK_TIME_FROM_RELEASE_STAMP' = ANY(snapshot.quality_flags))
           AS release_stamp_fallbacks,
       percentile_cont(0.5) WITHIN GROUP (
           ORDER BY abs(extract(epoch FROM (
               snapshot.mark_market_data_time - snapshot.spot_market_data_time
           )))
       ) FILTER (
           WHERE snapshot.mark_market_data_time IS NOT NULL
             AND snapshot.spot_market_data_time IS NOT NULL
       ) AS median_absolute_skew_seconds
FROM option_chain_snapshots snapshot
JOIN option_ingestion_runs run USING (batch_id)
WHERE run.scheduled_cycle = %s
"""

SQL_MARKS_BY_VOLUME = """
SELECT CASE
           WHEN snapshot.day_volume IS NULL THEN 'NULL'
           WHEN snapshot.day_volume < 100 THEN '1-99'
           WHEN snapshot.day_volume < 1000 THEN '100-999'
           ELSE '1000+'
       END AS volume_bucket,
       COUNT(*) AS snapshots,
       COUNT(snapshot.model_mark) AS model_marks
FROM option_chain_snapshots snapshot
JOIN option_ingestion_runs run USING (batch_id)
WHERE run.scheduled_cycle = %s
GROUP BY 1
ORDER BY 1
"""

SQL_OI = """
SELECT settlement_session,
       open_interest_observed_session,
       COUNT(*) AS contracts,
       COUNT(DISTINCT underlying) AS underlyings,
       SUM(open_interest_revision_count) AS revisions
FROM option_daily_contract_facts
WHERE open_interest_observed_session = %s
  AND open_interest IS NOT NULL
GROUP BY settlement_session, open_interest_observed_session
ORDER BY settlement_session
"""

SQL_TRADES = """
SELECT COUNT(*) AS trades,
    COUNT(*) FILTER (WHERE classification_status = 'INCLUDED') AS included_trades,
    COUNT(*) FILTER (WHERE classification_status <> 'INCLUDED') AS excluded_trades,
       COUNT(DISTINCT underlying) AS underlyings,
       MIN(sip_timestamp) AS first_trade,
       MAX(sip_timestamp) AS last_trade,
       SUM(notional) AS notional
FROM option_trade_events
WHERE (sip_timestamp AT TIME ZONE 'America/New_York')::date = %s
"""

SQL_GAMMA = """
SELECT profile.scope, COUNT(*) AS profiles,
       COUNT(DISTINCT profile.underlying) AS underlyings,
       COUNT(*) FILTER (WHERE profile.coverage_fraction >= 0.95) AS high_coverage,
       COUNT(*) FILTER (WHERE profile.flip_spot IS NOT NULL) AS with_flip
FROM option_gamma_profiles profile
JOIN option_analysis_runs analysis USING (matrix_id)
JOIN option_ingestion_runs run ON run.batch_id = analysis.batch_id
WHERE run.scheduled_cycle = %s
GROUP BY profile.scope
ORDER BY profile.scope
"""

SQL_CANDIDATES = """
SELECT candidate.strategy_name, candidate.status, candidate.candidate_kind,
       COUNT(*) AS candidates
FROM option_strategy_candidates candidate
JOIN option_analysis_runs analysis USING (matrix_id)
JOIN option_ingestion_runs run ON run.batch_id = analysis.batch_id
WHERE run.scheduled_cycle = %s
GROUP BY candidate.strategy_name, candidate.status, candidate.candidate_kind
ORDER BY candidate.strategy_name, candidate.status
"""

SQL_GATES = """
SELECT COUNT(DISTINCT candidate.candidate_id) AS candidates,
       COUNT(gate.*) AS gate_rows,
       COUNT(DISTINCT gate.gate_name) AS gate_names,
       COUNT(*) FILTER (WHERE gate.verdict = 'PASS') AS pass,
       COUNT(*) FILTER (WHERE gate.verdict = 'FAIL') AS fail,
       COUNT(*) FILTER (WHERE gate.verdict = 'UNAVAILABLE') AS unavailable,
       COUNT(DISTINCT candidate.candidate_id) FILTER (
           WHERE gate.candidate_id IS NULL
       ) AS candidates_without_gates
FROM option_strategy_candidates candidate
JOIN option_analysis_runs analysis USING (matrix_id)
JOIN option_ingestion_runs run ON run.batch_id = analysis.batch_id
LEFT JOIN option_candidate_execution_gates gate
  ON gate.candidate_id = candidate.candidate_id
WHERE run.scheduled_cycle = %s
"""

SQL_GATE_BREAKDOWN = """
SELECT gate.gate_name, gate.verdict, COUNT(*) AS candidates
FROM option_candidate_execution_gates gate
JOIN option_strategy_candidates candidate USING (candidate_id)
JOIN option_analysis_runs analysis USING (matrix_id)
JOIN option_ingestion_runs run ON run.batch_id = analysis.batch_id
WHERE run.scheduled_cycle = %s
GROUP BY gate.gate_name, gate.verdict
ORDER BY gate.gate_name, gate.verdict
"""

SQL_GATE_REASONS = """
SELECT gate.gate_name, gate.verdict, reason, COUNT(*) AS candidates
FROM option_candidate_execution_gates gate
JOIN option_strategy_candidates candidate USING (candidate_id)
JOIN option_analysis_runs analysis USING (matrix_id)
JOIN option_ingestion_runs run ON run.batch_id = analysis.batch_id
CROSS JOIN LATERAL unnest(gate.reason_codes) reason
WHERE run.scheduled_cycle = %s
GROUP BY gate.gate_name, gate.verdict, reason
ORDER BY gate.gate_name, candidates DESC, reason
"""

SQL_SUPPRESSIONS = """
SELECT candidate.strategy_name, reason, COUNT(*) AS candidates
FROM option_strategy_candidates candidate
JOIN option_analysis_runs analysis USING (matrix_id)
JOIN option_ingestion_runs run ON run.batch_id = analysis.batch_id
CROSS JOIN LATERAL unnest(candidate.reason_codes) reason
WHERE run.scheduled_cycle = %s
    AND candidate.status = 'SUPPRESSED'
GROUP BY candidate.strategy_name, reason
ORDER BY candidate.strategy_name, candidates DESC, reason
"""

SQL_SIGNALS_OUTCOMES = """
SELECT
    COUNT(DISTINCT signal.event_id) AS signals,
    COUNT(DISTINCT outcome.outcome_id) AS decay_outcomes,
    COUNT(DISTINCT current_mark.candidate_id) AS current_marks
FROM option_strategy_candidates candidate
JOIN option_analysis_runs analysis USING (matrix_id)
JOIN option_ingestion_runs run ON run.batch_id = analysis.batch_id
LEFT JOIN option_signal_events signal
  ON signal.source_candidate_id = candidate.candidate_id
LEFT JOIN option_signal_decay_outcomes outcome
  ON outcome.candidate_id = candidate.candidate_id
LEFT JOIN option_signal_current_marks current_mark
  ON current_mark.candidate_id = candidate.candidate_id
WHERE run.scheduled_cycle = %s
"""

SQL_SESSION_MEASUREMENTS = """
SELECT
    (SELECT COUNT(*) FROM option_signal_decay_outcomes outcome
     WHERE (outcome.market_time AT TIME ZONE 'America/New_York')::date = %s)
        AS decay_outcomes,
    (SELECT COUNT(*) FROM option_signal_current_marks current_mark
     WHERE (current_mark.market_time AT TIME ZONE 'America/New_York')::date = %s)
        AS current_marks,
    (SELECT COUNT(*) FROM option_signal_current_marks current_mark
     JOIN option_strategy_candidates candidate USING (candidate_id)
     WHERE (current_mark.market_time AT TIME ZONE 'America/New_York')::date = %s
       AND current_mark.market_time <= candidate.market_data_time)
        AS noncausal_current_marks
"""

SQL_WORK = """
SELECT work.stage, work.status, COUNT(*) AS items,
             work.attempt_count, work.last_error
FROM option_work_items work
JOIN option_analysis_runs analysis
    ON work.subject_id = analysis.matrix_id::text
JOIN option_ingestion_runs run ON run.batch_id = analysis.batch_id
WHERE run.scheduled_cycle = %s
GROUP BY work.stage, work.status, work.attempt_count, work.last_error
ORDER BY work.stage, work.status, work.last_error
"""

SQL_PACKAGE_COHERENCE = """
WITH latest_batches AS (
    SELECT run.underlying, run.batch_id
    FROM option_ingestion_runs run
    JOIN option_analysis_runs analysis USING (batch_id)
    WHERE run.scheduled_cycle = %s
      AND analysis.status = 'COMPLETE'
), prior_candidates AS (
    SELECT candidate.candidate_id, candidate.underlying,
        candidate.candidate_kind, COUNT(leg.*) AS leg_count
    FROM option_strategy_candidates candidate
    JOIN option_candidate_legs leg USING (candidate_id)
    WHERE candidate.status = 'SELECTED'
      AND candidate.candidate_kind IN ('SINGLE_CONTRACT', 'MULTI_LEG')
      AND candidate.market_data_time < %s
    GROUP BY candidate.candidate_id, candidate.underlying,
          candidate.candidate_kind
), matched AS (
    SELECT candidate.candidate_id, candidate.candidate_kind,
        candidate.leg_count,
        COUNT(DISTINCT snapshot.contract_id) AS matched_legs
    FROM prior_candidates candidate
    JOIN latest_batches batch USING (underlying)
    LEFT JOIN option_candidate_legs leg USING (candidate_id)
    LEFT JOIN option_chain_snapshots snapshot
      ON snapshot.batch_id = batch.batch_id
     AND snapshot.contract_id = leg.contract_id
     AND snapshot.model_mark IS NOT NULL
    GROUP BY candidate.candidate_id, candidate.candidate_kind,
          candidate.leg_count
)
SELECT candidate_kind, leg_count,
    COUNT(*) AS candidates,
    COUNT(*) FILTER (WHERE matched_legs = leg_count) AS coherent,
    COUNT(*) FILTER (WHERE matched_legs = 0) AS no_legs,
    COUNT(*) FILTER (
        WHERE matched_legs > 0 AND matched_legs < leg_count
    ) AS partial
FROM matched
GROUP BY candidate_kind, leg_count
ORDER BY candidate_kind, leg_count
"""

SQL_TRADE_BATCH_HEALTH = """
WITH cycle_window AS (
        SELECT MIN(run.started_at) AS window_start,
                     MAX(analysis.completed_at) AS window_end
        FROM option_analysis_runs analysis
        JOIN option_ingestion_runs run USING (batch_id)
        WHERE run.scheduled_cycle = %s
)
SELECT run.status, COUNT(DISTINCT run.batch_id) AS batches,
             COUNT(DISTINCT page.redacted_request->>'path') AS contracts,
             SUM(page.row_count) AS rows,
             COUNT(*) FILTER (WHERE page.validation_status = 'INVALID') AS invalid_pages
FROM option_ingestion_runs run
JOIN option_raw_batch_pages page USING (batch_id)
CROSS JOIN cycle_window cycle
WHERE page.redacted_request->>'path' LIKE '/v3/trades/%%'
    AND run.started_at >= cycle.window_start
    AND run.started_at <= cycle.window_end
GROUP BY run.status
ORDER BY run.status
"""


def _one(cursor, sql: str, params: tuple[Any, ...]) -> dict[str, Any]:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return dict(row) if row else {}


def _many(cursor, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cursor.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def main() -> int:
    configuration = load_option_runtime_configuration()
    calendar = OptionExchangeCalendar()
    with get_db_cursor() as cursor:
        cursor.execute(SQL_LATEST_SLOT)
        row = cursor.fetchone()
        slot = row["scheduled_cycle"] if row else None
        if slot is None:
            print(json.dumps({"status": "NO_COMPLETE_OPTION_SLOT"}))
            return 1
        observed_session = calendar.session_for_slot(slot.astimezone(timezone.utc))
        expected_settlement = calendar.previous_session(observed_session)
        analysis = _many(cursor, SQL_ANALYSIS, (slot,))
        snapshots = _one(cursor, SQL_SNAPSHOTS, (slot,))
        marks_by_volume = _many(cursor, SQL_MARKS_BY_VOLUME, (slot,))
        open_interest = _many(cursor, SQL_OI, (observed_session,))
        trades = _one(cursor, SQL_TRADES, (observed_session,))
        gamma = _many(cursor, SQL_GAMMA, (slot,))
        candidates = _many(cursor, SQL_CANDIDATES, (slot,))
        gates = _one(cursor, SQL_GATES, (slot,))
        gate_breakdown = _many(cursor, SQL_GATE_BREAKDOWN, (slot,))
        gate_reasons = _many(cursor, SQL_GATE_REASONS, (slot,))
        suppressions = _many(cursor, SQL_SUPPRESSIONS, (slot,))
        signals_outcomes = _one(cursor, SQL_SIGNALS_OUTCOMES, (slot,))
        session_measurements = _one(
            cursor,
            SQL_SESSION_MEASUREMENTS,
            (observed_session, observed_session, observed_session),
        )
        work = _many(cursor, SQL_WORK, (slot,))
        package_coherence = _many(
            cursor, SQL_PACKAGE_COHERENCE, (slot, slot)
        )
        trade_batch_health = _many(cursor, SQL_TRADE_BATCH_HEALTH, (slot,))

    expected_underlyers = len(configuration.settings.underlyers)
    complete_matrices = sum(
        int(item["matrices"]) for item in analysis if item["status"] == "COMPLETE"
    )
    snapshot_count = int(snapshots.get("snapshots") or 0)
    gate_candidates = int(gates.get("candidates") or 0)
    gate_rows = int(gates.get("gate_rows") or 0)
    trade_batches_complete = (
        bool(trade_batch_health)
        and all(item["status"] == "COMPLETE" for item in trade_batch_health)
        and sum(int(item["invalid_pages"] or 0) for item in trade_batch_health) == 0
        and sum(int(item["batches"] or 0) for item in trade_batch_health)
            == sum(int(item["contracts"] or 0) for item in trade_batch_health)
    )
    oi_valid = (
        len(open_interest) == 1
        and open_interest[0]["settlement_session"] == expected_settlement
        and open_interest[0]["open_interest_observed_session"] == observed_session
        and int(open_interest[0]["underlyings"]) == expected_underlyers
    )
    checks = {
        "all_underlyers_analyzed": complete_matrices == expected_underlyers,
        "snapshots_present": snapshot_count > 0,
        "model_mark_coverage_at_least_95pct": (
            int(snapshots.get("model_marks") or 0) / snapshot_count >= 0.95
            if snapshot_count else False
        ),
        "all_persisted_marks_aligned_within_60s": (
            int(snapshots.get("aligned_within_60s") or 0) == snapshot_count
        ),
        "no_release_stamp_fallbacks": int(
            snapshots.get("release_stamp_fallbacks") or 0
        ) == 0,
        "open_interest_on_prior_settlement": oi_valid,
        "trades_captured": int(trades.get("trades") or 0) > 0,
        "strategy_readable_trades_present": int(
            trades.get("included_trades") or 0
        ) > 0,
        "trade_batches_complete_and_distinct": trade_batches_complete,
        "four_gamma_scopes_per_matrix": (
            len(gamma) == 4
            and all(int(item["profiles"]) == complete_matrices for item in gamma)
        ),
        "candidates_present": gate_candidates > 0,
        "six_gates_per_candidate": gate_rows == gate_candidates * 6,
        "all_gate_names_present": int(gates.get("gate_names") or 0) == 6,
        "no_candidate_missing_gates": int(
            gates.get("candidates_without_gates") or 0
        ) == 0,
        "session_decay_outcomes_present": int(
            session_measurements.get("decay_outcomes") or 0
        ) > 0,
        "session_current_marks_present": int(
            session_measurements.get("current_marks") or 0
        ) > 0,
        "no_noncausal_session_current_marks": int(
            session_measurements.get("noncausal_current_marks") or 0
        ) == 0,
    }
    report = {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "slot": slot,
        "observed_session": observed_session,
        "expected_open_interest_settlement": expected_settlement,
        "expected_underlyers": expected_underlyers,
        "checks": checks,
        "analysis": analysis,
        "snapshots": snapshots,
        "marks_by_volume": marks_by_volume,
        "open_interest": open_interest,
        "trades": trades,
        "gamma": gamma,
        "candidates": candidates,
        "gates": gates,
        "gate_breakdown": gate_breakdown,
        "gate_reasons": gate_reasons,
        "suppressions": suppressions,
        "signals_and_outcomes": signals_outcomes,
        "session_measurements": session_measurements,
        "work": work,
        "prior_candidate_package_coherence": package_coherence,
        "trade_batch_health": trade_batch_health,
    }
    print(json.dumps(report, indent=2, default=str, sort_keys=True, allow_nan=False))
    return 0 if all(checks.values()) else 2


if __name__ == "__main__":
    raise SystemExit(main())
