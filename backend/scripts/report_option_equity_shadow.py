"""Read-only equity-context / option-contract alert shadow comparison."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
from uuid import UUID

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor
from options.config import load_option_runtime_configuration
from options.analytics.equity_shadow import (
    DIRECTIONAL_STRATEGIES, HOLDING_SESSIONS, equity_alignment, first_decision_cohorts,
    measure_shadow_outcome, select_shadow_pair, summarize_shadow_pairs,
)


SQL_CANDIDATES = """
        WITH visible AS (
            SELECT candidate.*, context.trend_state AS original_option_trend,
        context.earnings_blackout_state,context.fed_blackout_state,context.quote_spread_state,
        context.equity_context_snapshot_id::text AS original_equity_context_id
            FROM option_strategy_candidates AS candidate
            LEFT JOIN option_context_snapshots AS context USING(context_snapshot_id)
            WHERE candidate.status='SELECTED' AND candidate.policy_sha256=%s
                AND candidate.market_data_time >= %s AND candidate.market_data_time <= %s
                AND candidate.observed_time<=%s AND candidate.created_at<=%s
        ), first_matrices AS (
            SELECT visible.*,COUNT(*) OVER () AS source_candidate_rows,
                FIRST_VALUE(matrix_id) OVER (
                    PARTITION BY underlying,strategy_name,structure_type,(market_data_time AT TIME ZONE 'America/New_York')::date
                    ORDER BY observed_time,market_data_time,matrix_id
                ) AS first_matrix_id
            FROM visible
        )
        SELECT * FROM first_matrices WHERE matrix_id=first_matrix_id
        ORDER BY market_data_time,observed_time,candidate_id LIMIT 50001
"""
SQL_EVIDENCE = """
    SELECT evidence.evidence_id::text,evidence.ticker,evidence.security_id::text,
        evidence.interval AS interval_key,evidence.evidence_type,evidence.evidence_role,evidence.direction,
        evidence.lifecycle_status,evidence.quality_state,evidence.quality_codes,evidence.source_version AS model_version,
        evidence.market_time,evidence.observed_at,evidence.created_at,evidence.valid_until,
        evidence.qualification_revision_id::text,evidence.payload,evidence.payload_sha256,
        bar.availability_mode
    FROM equity_evidence AS evidence
    JOIN equity_bar_revisions AS bar ON bar.bar_revision_id=evidence.latest_bar_revision_id
    WHERE evidence.ticker=ANY(%s::text[]) AND evidence.interval='1d' AND evidence.evidence_type='TRADE_SETUP'
      AND evidence.market_time >= %s AND evidence.market_time<=%s
      AND evidence.observed_at<=%s AND evidence.created_at<=%s
      AND bar.availability_mode='LIVE_OBSERVED'
    ORDER BY evidence.ticker,evidence.market_time,evidence.observed_at,evidence.created_at,evidence.evidence_id
"""
SQL_LEGS = """
    SELECT leg.*, entry.first_observed_at AS entry_first_observed_at
    FROM option_candidate_legs AS leg
    LEFT JOIN option_chain_snapshots AS entry ON entry.snapshot_id=leg.snapshot_id
    WHERE leg.candidate_id=ANY(%s::uuid[]) ORDER BY leg.candidate_id,leg.leg_index
"""
SQL_EXIT = """
        SELECT snapshot.*,run.scheduled_cycle
        FROM option_chain_snapshots AS snapshot JOIN option_ingestion_runs AS run USING(batch_id)
        WHERE snapshot.contract_id=ANY(%s::bigint[]) AND run.scheduled_cycle=ANY(%s::timestamptz[])
            AND snapshot.valuation_policy_sha256=%s AND snapshot.first_observed_at<=%s AND snapshot.created_at<=%s
            AND (snapshot.revised_observed_at IS NULL OR snapshot.revised_observed_at<=%s)
        ORDER BY run.scheduled_cycle,snapshot.batch_id,snapshot.contract_id,snapshot.first_observed_at,snapshot.revision
"""


def serializable(value):
    if isinstance(value, dict):
        return {str(key): serializable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serializable(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def digest(value):
    return hashlib.sha256(json.dumps(serializable(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def run_report(config_path, output_path=None):
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if config["holding_sessions"] != list(HOLDING_SESSIONS) or set(config["directional_strategies"]) != DIRECTIONAL_STRATEGIES \
            or config["lookback_calendar_days"] != 60:
        raise ValueError("this report implements only the frozen 60-day, 5/10/21-session shadow contract")
    runtime = load_option_runtime_configuration()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=config["lookback_calendar_days"])
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '120s'")
        cursor.execute("SELECT current_setting('transaction_read_only') AS read_only,transaction_timestamp() AS cutoff")
        transaction = dict(cursor.fetchone())
        cursor.execute(SQL_CANDIDATES, (runtime.strategy_policy_sha256, start, now, now, now))
        candidates = [dict(row) for row in cursor.fetchall()]
        if len(candidates) > 50000:
            raise ValueError("shadow candidate bound exceeded; do not silently truncate the study")
        raw_candidate_count = int(candidates[0]["source_candidate_rows"]) if candidates else 0
        cohorts = first_decision_cohorts(candidates)
        retained = [row for _, rows in cohorts for row in rows]
        identities = [str(row["candidate_id"]) for row in retained]
        cursor.execute(SQL_LEGS, (identities,))
        legs = [dict(row) for row in cursor.fetchall()]
        if len({(row["candidate_id"], row["leg_index"]) for row in legs}) != len(legs):
            raise ValueError("ambiguous entry snapshot or ordered leg identity")
        legs_by_candidate = defaultdict(list)
        for leg in legs:
            legs_by_candidate[str(leg["candidate_id"])].append(leg)
        cursor.execute(SQL_EVIDENCE, (sorted({row["underlying"] for row in retained}), start - timedelta(days=7), now, now, now))
        evidence = [dict(row) for row in cursor.fetchall()]
        if len({row["evidence_id"] for row in evidence}) != len(evidence):
            raise ValueError("ambiguous source evidence linkage")
        cursor.execute("""
            SELECT candidate_id::text,ledger_version,gate_name,verdict,blocking,reason_codes,evidence,evaluated_at
            FROM option_candidate_execution_gates WHERE candidate_id=ANY(%s::uuid[]) ORDER BY candidate_id,ledger_version,gate_name
        """, (identities,))
        gates = [dict(row) for row in cursor.fetchall()]
        annotations = {str(row["candidate_id"]): equity_alignment(row, evidence) for row in retained}
        pairs = []
        for (session, underlyer, strategy, structure), rows in cohorts:
            if strategy not in DIRECTIONAL_STRATEGIES or rows[0]["candidate_kind"] == "RESEARCH_ONLY":
                continue
            for horizon in HOLDING_SESSIONS:
                pair = select_shadow_pair(rows, legs_by_candidate, annotations, horizon)
                pair.update(entry_session=session, underlying=underlyer, strategy_name=strategy, structure_type=structure,
                            matrix_id=str(rows[0]["matrix_id"]), option_only_contract_ids=[leg["contract_id"] for leg in legs_by_candidate.get(pair["option_only_candidate_id"], [])])
                pair["equity_alignment"] = annotations.get(pair["option_only_candidate_id"], {}).get("alignment", "NO_ELIGIBLE_PACKAGE")
                pairs.append(pair)
        chosen_ids = {pair[field] for pair in pairs for field in ("option_only_candidate_id", "equity_aligned_candidate_id") if pair[field]}
        targets = sorted({pair["planned_exit"] for pair in pairs if pair["option_only_candidate_id"] and datetime.fromisoformat(pair["planned_exit"]) <= now})
        contract_ids = sorted({leg["contract_id"] for identity in chosen_ids for leg in legs_by_candidate[identity]})
        snapshots = []
        if targets and contract_ids:
            cursor.execute(SQL_EXIT, (contract_ids, targets, runtime.valuation_policy.policy_sha256, now, now, now))
            snapshots = [dict(row) for row in cursor.fetchall()]
    by_id = {str(row["candidate_id"]): row for row in retained}
    measurements = {}
    for pair in pairs:
        for arm in ("option_only", "equity_aligned"):
            identity = pair[f"{arm}_candidate_id"]
            if identity is None:
                pair[f"{arm}_outcome"] = dict(status="ABSTAIN_CASH" if arm == "equity_aligned" and pair["option_only_candidate_id"] else "NO_ELIGIBLE_PACKAGE",
                                             net_return=0.0 if arm == "equity_aligned" and pair["option_only_candidate_id"] else None)
                continue
            key = (identity, pair["holding_sessions"])
            if key not in measurements:
                measurements[key] = measure_shadow_outcome(by_id[identity], legs_by_candidate[identity], snapshots,
                                                           pair["holding_sessions"], now, runtime.valuation_policy)
            pair[f"{arm}_outcome"] = measurements[key]
    gate_map = defaultdict(list)
    for gate in gates:
        gate_map[gate["candidate_id"]].append(gate)
    alert_context = []
    for candidate in retained:
        identity = str(candidate["candidate_id"])
        alert_context.append(dict(candidate_id=identity, underlying=candidate["underlying"], strategy_name=candidate["strategy_name"],
                                  candidate_kind=candidate["candidate_kind"], structure_type=candidate["structure_type"],
                                  market_data_time=candidate["market_data_time"], observed_time=candidate["observed_time"],
                                  candidate_recorded_at=candidate["created_at"],
                                  record_mode="RETROSPECTIVE_ASOF_JOIN_NOT_PROSPECTIVE_ALERT_ISSUANCE",
                                  original_rank=candidate["candidate_rank"], original_execution_eligibility=candidate["execution_eligibility"],
                                  original_reasons=candidate["reason_codes"], original_gates=gate_map[identity],
                                  equity_context=annotations[identity], ordered_legs=legs_by_candidate[identity],
                                  option_economics=candidate["primary_evidence"], breakevens=candidate["breakevens"],
                                  maximum_loss=candidate["maximum_loss"], maximum_profit=candidate["maximum_profit"],
                                  near_term_event_state=dict(earnings=candidate["earnings_blackout_state"], fed=candidate["fed_blackout_state"]),
                                  full_horizon_event_coverage="UNVERIFIED_NOT_AN_EXECUTION_RECOMMENDATION",
                                  original_equity_context_id=candidate["original_equity_context_id"]))
    summary = summarize_shadow_pairs(pairs)
    report = dict(config=config, config_sha256=digest(config), generated_at=now.isoformat(), transaction=transaction,
                  runtime_strategy_policy_sha256=runtime.strategy_policy_sha256, valuation_policy=runtime.valuation_policy.model_dump(mode="json"),
                  valuation_policy_sha256=runtime.valuation_policy.policy_sha256, raw_candidate_rows=raw_candidate_count,
                  retained_candidates=len(retained), discarded_repeat_snapshot_rows=raw_candidate_count - len(retained),
                  raw_detector_alerts_retained=sum(row["candidate_kind"] == "RESEARCH_ONLY" for row in retained),
                  alignment_counts=dict(Counter(row["alignment"] for row in annotations.values())),
                  measurement_status_counts=dict(Counter(row["status"] for row in measurements.values())),
                  cohort_pairs=pairs, summaries=summary, alert_context=alert_context, equity_evidence=evidence,
                  input_hashes=dict(candidates=digest(retained), legs=digest(legs), equity_evidence=digest(evidence), gates=digest(gates), snapshots=digest(snapshots)),
                  implementation_sha256=digest({path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in
                                                (Path(__file__), BACKEND_DIR / "options" / "analytics" / "equity_shadow.py")}),
                  measured_exit_snapshots=snapshots, production_mutations=False, equity_paper_tracker_modified=False,
                  classification="SHADOW_EQUITY_OPTION_CONTEXT_NOT_CERTIFIED",
                  limitations=["Persisted candidates may already contain equity filters; this is not a detection-engine counterfactual.",
                               "Daily setup direction is not validated separately at each horizon and is not the experimental ridge model.",
                               "Shared market dates, overlapping holdings and recurring contracts are not independent trials.",
                               "Missing outcomes are not zero returns; only an explicit shadow abstention is modelled as cash.",
                               "No portfolio drawdown, calibrated probabilities, real fills or execution readiness are inferred."])
    report = serializable(report)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=BACKEND_DIR / "research/inputs/option_equity_shadow_config.json")
    parser.add_argument("--output", type=Path, default=BACKEND_DIR.parent / "docs" / "option_equity_shadow_results.json")
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args()
    report = run_report(args.config, None if args.no_write else args.output)
    print(json.dumps({key: report[key] for key in ("config_sha256", "raw_candidate_rows", "retained_candidates", "raw_detector_alerts_retained",
                                                 "alignment_counts", "measurement_status_counts", "summaries")}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())