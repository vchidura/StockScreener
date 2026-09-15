"""Inventory retained equity signals and evaluate daily evidence without publication."""
import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
from uuid import UUID

from dotenv import load_dotenv
import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor
from research.composite_scanners import SCANNER_VERSIONS
from research.features import FEATURE_COLUMNS
from research.signal_scorecard import registered_signal_cells, evidence_role_contract, build_daily_scorecard, matching_cell_policies, adapter_signal_cells


ORIGIN_SQL = """CASE WHEN 'HISTORICAL_RECONSTRUCTED'=ANY(evidence.quality_codes)
    THEN 'HISTORICAL_RECONSTRUCTED' ELSE COALESCE(bar.availability_mode,'UNKNOWN') END"""
PURPOSE_SQL = """COALESCE(analysis.run_purpose,CASE WHEN 'HISTORICAL_RECONSTRUCTED'=ANY(evidence.quality_codes)
    THEN 'RECONSTRUCTED_LEDGER' ELSE 'UNLINKED_LEDGER' END)"""
JOINS_SQL = """FROM equity_evidence AS evidence
    LEFT JOIN equity_bar_revisions AS bar ON bar.bar_revision_id=evidence.latest_bar_revision_id
    LEFT JOIN equity_analysis_runs AS analysis ON analysis.analysis_run_id=evidence.analysis_run_id"""
SQL_INVENTORY = f"""
    WITH classified AS MATERIALIZED (
        SELECT evidence.*,{ORIGIN_SQL} AS origin,{PURPOSE_SQL} AS data_run_purpose
        {JOINS_SQL} WHERE evidence.observed_at<=%s AND evidence.created_at<=%s
    )
    SELECT evidence.source_name,evidence.source_version,evidence.evidence_type,evidence.evidence_role,evidence.interval,
        evidence.direction,evidence.origin,evidence.data_run_purpose AS run_purpose,
        COUNT(*) AS evidence_records,COUNT(DISTINCT evidence.ticker) AS distinct_tickers,
        COUNT(DISTINCT evidence.market_time::date) AS market_dates,
        COUNT(DISTINCT (evidence.security_id,COALESCE(evidence.lifecycle_key,evidence.evidence_id::text))) AS lifecycle_subjects,
        MIN(evidence.market_time) AS first_market_time,MAX(evidence.market_time) AS last_market_time,
        MAX(evidence.observed_at) AS last_observed_at,
        COUNT(*) FILTER (WHERE evidence.quality_state IN ('FAILED','STALE')) AS bad_quality_records,
        COUNT(*) FILTER (WHERE evidence.qualification_revision_id IS NOT NULL) AS qualification_linked_records
    FROM classified AS evidence
    GROUP BY evidence.source_name,evidence.source_version,evidence.evidence_type,evidence.evidence_role,evidence.interval,
        evidence.direction,evidence.origin,evidence.data_run_purpose
    ORDER BY evidence.source_name,evidence.source_version,evidence.interval,evidence.direction,origin,run_purpose
"""
SQL_EVENTS = f"""
    SELECT evidence.evidence_id::text,evidence.source_name,evidence.source_version,evidence.evidence_type,evidence.evidence_role,
        evidence.ticker,evidence.security_id::text,evidence.interval,evidence.direction,evidence.lifecycle_key,
        evidence.market_time,evidence.observed_at,evidence.created_at,evidence.quality_state,evidence.quality_codes,
        evidence.payload_sha256,evidence.latest_bar_revision_id::text,{ORIGIN_SQL} AS origin,{PURPOSE_SQL} AS run_purpose
    {JOINS_SQL}
    WHERE evidence.interval='1d' AND evidence.evidence_type='SCANNER_RESULT' AND evidence.direction IN (-1,1)
      AND evidence.source_name=ANY(%s::text[]) AND evidence.observed_at<=%s AND evidence.created_at<=%s
    ORDER BY evidence.market_time,evidence.observed_at,evidence.evidence_id LIMIT 250001
"""
SQL_OUTCOMES = """
    SELECT DISTINCT ON(subject_evidence_id,outcome_policy_id,horizon_key)
        outcome_id::text,subject_evidence_id::text,outcome_policy_id::text,horizon_key,outcome_revision,
        entry_status,signal_time,entry_time,exit_time,net_return,net_alpha,sector_net_alpha,mae_pct,mfe_pct,
        stop_hit,target_hit,first_hit,outcome_category,is_stale,outcome_available_at,created_at,quality_codes
    FROM equity_research_outcomes
    WHERE subject_evidence_id=ANY(%s::uuid[]) AND outcome_available_at<=%s AND created_at<=%s
    ORDER BY subject_evidence_id,outcome_policy_id,horizon_key,outcome_revision DESC,created_at DESC,outcome_id
    LIMIT 1500001
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


def load_batch_scope(batch_dir):
    from research.frozen_daily_study import verify_replays, completed, file_sha256, source_path
    from scripts.run_historical_signal_outcomes import load_events
    from equity.historical_research import historical_event_evidence_id

    plan = json.loads((batch_dir / "plan.json").read_text(encoding="utf-8"))
    if plan["contract"] != "MANUAL_DAILY_ADAPTER_REPLAY_V2":
        raise ValueError("batch report requires the strict replay contract")
    verify_replays(plan)
    for filename, expected in plan["runner_code_sha256"].items():
        if file_sha256(source_path(filename)) != expected:
            raise ValueError("batch implementation differs from the frozen plan")
    subjects, policy_ids, coverage = set(), set(), []
    for group in plan["groups"]:
        command = group["outcome_command"]
        outcome_path = Path(command[-1])
        if not completed(command, outcome_path, (outcome_path,)):
            raise ValueError("finish the batch outcome stage before reporting")
        outcome_report = json.loads(outcome_path.read_text(encoding="utf-8"))
        policy_ids.update(row["outcome_policy_id"] for row in outcome_report.get("outcomes", {}).get("policies", []))
        subjects.update(str(historical_event_evidence_id(event)) for event in load_events(Path(group["events"]))
                        if event.payload.get("qualification_eligible"))
        replay_report = json.loads(Path(group["report"]).read_text(encoding="utf-8"))
        coverage.append(dict(group=group["name"], records=replay_report["coverage_count"],
                             states=replay_report["coverage_counts"], coverage_sha256=replay_report["coverage_sha256"],
                             universe_count_discrepancies=[row for row in replay_report["universe_selection"]["runs"]
                                                           if row.get("member_count_discrepancy")]))
    return dict(plan=plan, subjects=sorted(subjects), policy_ids=sorted(policy_ids), coverage=coverage,
                cells=adapter_signal_cells(group["name"].split("/")[-1] for group in plan["groups"]))


def read_batch_measurements(cursor, scope, now):
    events, outcomes = [], []
    event_sql = SQL_EVENTS.replace("evidence.source_name=ANY(%s::text[])", "evidence.evidence_id=ANY(%s::uuid[])").replace(" LIMIT 250001", "")
    outcome_sql = SQL_OUTCOMES.replace("WHERE subject_evidence_id", "WHERE outcome_policy_id=ANY(%s::uuid[]) AND subject_evidence_id").replace("LIMIT 1500001", "")
    for offset in range(0, len(scope["subjects"]), 5000):
        subjects = scope["subjects"][offset:offset + 5000]
        cursor.execute(event_sql, (subjects, now, now))
        chunk = [dict(row) for row in cursor.fetchall()]
        if {row["evidence_id"] for row in chunk} != set(subjects):
            raise ValueError("batch evidence is missing or does not match the requested subjects")
        events.extend(chunk)
        cursor.execute(outcome_sql, (scope["policy_ids"], subjects, now, now))
        outcomes.extend(dict(row) for row in cursor.fetchall())
    return events, outcomes


def batch_inventory(events):
    grouped = {}
    fields = ("source_name", "source_version", "evidence_type", "evidence_role", "interval", "direction", "origin", "run_purpose")
    for event in events:
        grouped.setdefault(tuple(event[field] for field in fields), []).append(event)
    return [dict(zip(fields, key), evidence_records=len(rows), distinct_tickers=len({row["ticker"] for row in rows}),
                 market_dates=len({row["market_time"] for row in rows})) for key, rows in sorted(grouped.items())]


def run_report(config_path, output_path=None, *, batch_dir=None):
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if config["daily_horizons"] != [5, 10, 21] or config["minimum_events"] != 100 or config["minimum_independent_periods"] != 40:
        raise ValueError("signal inventory uses the declared daily horizons and existing research thresholds")
    samples, sample_provenance = {}, []
    for index, filename in enumerate(config["sample_manifests"], 1):
        path = config_path.parent / filename
        document = json.loads(path.read_text(encoding="utf-8"))
        if len(set(document["sampled_tickers"])) != 300:
            raise ValueError("scorecard sample manifest must preserve 300 unique tickers")
        samples[f"SAMPLE_{index}"] = document["sampled_tickers"]
        sample_provenance.append(dict(file=filename, sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    if set(samples) != {"SAMPLE_1", "SAMPLE_2"} or set(samples["SAMPLE_1"]).intersection(samples["SAMPLE_2"]):
        raise ValueError("scorecard requires the two original disjoint samples")
    scope = load_batch_scope(batch_dir) if batch_dir is not None else None
    if scope:
        if any(scope["plan"]["sample_file_sha256"].get(row["file"]) != row["sha256"] for row in sample_provenance):
            raise ValueError("scorecard sample manifests differ from the frozen batch")
        if scope["plan"]["sample"] != "both":
            samples = {f"SAMPLE_{scope['plan']['sample']}": samples[f"SAMPLE_{scope['plan']['sample']}"]}
    now = datetime.now(timezone.utc)
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '120s'")
        cursor.execute("SELECT current_setting('transaction_read_only') AS read_only,transaction_timestamp() AS cutoff")
        transaction = dict(cursor.fetchone())
        if scope:
            events, outcomes = read_batch_measurements(cursor, scope, now)
            inventory = batch_inventory(events)
            cursor.execute("SELECT * FROM equity_outcome_policies WHERE outcome_policy_id=ANY(%s::uuid[]) AND created_at<=%s ORDER BY outcome_policy_id", (scope["policy_ids"], now))
        else:
            cursor.execute(SQL_INVENTORY, (now, now))
            inventory = [dict(row) for row in cursor.fetchall()]
            cursor.execute("SELECT * FROM equity_outcome_policies WHERE created_at<=%s ORDER BY source_name,source_version,interval,outcome_policy_id", (now,))
        policies = [dict(row) for row in cursor.fetchall()]
        if not scope:
            cursor.execute(SQL_EVENTS, (list(SCANNER_VERSIONS), now, now))
            events = [dict(row) for row in cursor.fetchall()]
            if len(events) > 250000:
                raise ValueError("daily event safety bound exceeded; no truncated scorecard will be produced")
            events = [row for row in events if SCANNER_VERSIONS[row["source_name"]] == row["source_version"]]
            cursor.execute(SQL_OUTCOMES, ([row["evidence_id"] for row in events], now, now))
            outcomes = [dict(row) for row in cursor.fetchall()]
            if len(outcomes) > 1500000:
                raise ValueError("daily outcome safety bound exceeded; no truncated scorecard will be produced")
        cursor.execute("""
            SELECT DISTINCT ON(source_name,source_version,interval,direction,horizon_key,outcome_policy_key,metrics->>'outcome_policy_id')
                qualification_revision_id::text,source_name,source_version,interval,direction,horizon_key,outcome_policy_key,
                qualification_state,effective_from,created_at,sample_size,independent_periods,
                metrics->>'outcome_policy_id' AS outcome_policy_id, metrics->>'qualification_metrics_version' AS metrics_version,
                metrics->>'probability_target' AS probability_target,alpha_fdr_q,mean_net_alpha
            FROM equity_qualification_revisions
            WHERE effective_from<=%s AND created_at<=%s AND (effective_to IS NULL OR effective_to>%s)
              AND metrics->>'research_scope'='EQUITY_SIGNAL'
            ORDER BY source_name,source_version,interval,direction,horizon_key,outcome_policy_key,metrics->>'outcome_policy_id',effective_from DESC,created_at DESC
        """, (now, now, now))
        qualifications = [dict(row) for row in cursor.fetchall()]
        if scope:
            qualifications = []
    for row in inventory:
        matching = [policy for policy in policies if policy["evidence_type"] == row["evidence_type"]
                    and all(policy[field] == row[field] for field in ("source_name", "source_version", "interval"))]
        row["outcome_policy_ids"] = [str(policy["outcome_policy_id"]) for policy in matching]
        row["evaluation_contract"] = evidence_role_contract(row["evidence_type"], row["evidence_role"])
        row["coverage_state"] = "POLICY_DEFINED_MEASUREMENT_REQUIRES_REVIEW" if matching else "NO_STANDALONE_OUTCOME_POLICY"
        if row["source_name"] in SCANNER_VERSIONS:
            row["detector_version_state"] = "CURRENT" if row["source_version"] == SCANNER_VERSIONS[row["source_name"]] else "OTHER_VERSION_NOT_POOLED"
    grid = []
    declared_cells = scope["cells"] if scope else registered_signal_cells()
    for cell in declared_cells:
        groups = [row for row in inventory if all(row[field] == cell[field] for field in ("source_name", "source_version", "interval", "direction"))]
        grid.append(dict(cell, retained_evidence_records=sum(row["evidence_records"] for row in groups),
                         policy_ids=sorted(str(policy["outcome_policy_id"]) for policy in matching_cell_policies(cell, policies)),
                         origins=sorted({row["origin"] for row in groups}),
                         state="EVIDENCE_RETAINED" if groups else "NO_RETAINED_EVENTS"))
    print(f"Reading scorecard inputs: {len(events)} daily records, {len(outcomes)} latest outcomes", flush=True)
    scorecard, duplicate_count = build_daily_scorecard(events, outcomes, policies, samples, config, now,
        **(dict(cells=declared_cells, origin_groups={("HISTORICAL_RECONSTRUCTED", "RECONSTRUCTED_LEDGER")},
                aggregate_scope="ALL_BATCH_TICKERS") if scope else {}))
    report = dict(study_id=config["study_id"], config=config, config_sha256=digest(config), generated_at=now.isoformat(), transaction=transaction,
                  inventory=inventory, registered_coverage_matrix=grid, daily_scorecard=scorecard,
                  existing_published_qualifications=qualifications, samples=sample_provenance,
                  bundled_feature_columns=FEATURE_COLUMNS, input_counts=dict(inventory_groups=len(inventory),
                    registered_cells=len(grid), daily_evidence_records=len(events), latest_outcomes=len(outcomes),
                    repeated_lifecycle_records_removed=duplicate_count, scorecard_cells=len(scorecard)),
                  data_status_counts=dict(Counter(row["data_status"] for row in scorecard)),
                  evidence_status_counts=dict(Counter(row["evidence_status"] for row in scorecard)),
                  input_hashes=dict(events=digest(events), outcomes=digest(outcomes), policies=digest(policies), qualifications=digest(qualifications)),
                  exact_input_manifest=dict(events=[dict(evidence_id=row["evidence_id"], payload_sha256=row["payload_sha256"]) for row in events],
                                           outcomes=[dict(outcome_id=row["outcome_id"], outcome_revision=row["outcome_revision"]) for row in outcomes]),
                  policies=policies, implementation_sha256=digest({path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in (Path(__file__), BACKEND_DIR / "research" / "signal_scorecard.py")}),
                  classification="RETAINED_LEDGER_EVIDENCE_AUDIT_NOT_NEW_REPLAY_OR_LIVE_QUALIFICATION",
                  production_mutations=False, models_fitted=False, paper_tracker_modified=False)
    if scope:
        report.update(classification="EXACT_BATCH_EVIDENCE_REPORT_NOT_LIVE_QUALIFICATION",
                      batch_plan_sha256=digest(scope["plan"]), source_cutoff=scope["plan"]["source_cutoff"],
                      batch_eligibility_coverage=scope["coverage"], batch_outcome_policy_ids=scope["policy_ids"])
    report = serializable(report)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        table_path = output_path.with_suffix(".csv")
        flat = [{key: value for key, value in row.items() if not isinstance(value, (dict, list))}
            | {f"stat_{key}": value for key, value in row["statistics"].items()}
            | {f"{period.lower()}_{key}": value for period, statistics in row["periods"].items() for key, value in statistics.items()}
            | {f"outcomes_{key}": row["outcome_states"].get(key, 0) for key in sorted({state for item in scorecard for state in item["outcome_states"]})}
            for row in scorecard]
        pd.DataFrame(flat).to_csv(table_path, index=False)
        report["scorecard_csv_sha256"] = hashlib.sha256(table_path.read_bytes()).hexdigest()
        output_path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=BACKEND_DIR.parent / "docs" / "equity_signal_scorecard_config.json")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--batch-dir", type=Path)
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args()
    output = args.output or (args.batch_dir / "scorecard.json" if args.batch_dir else BACKEND_DIR.parent / "docs" / "equity_signal_scorecard_results.json")
    report = run_report(args.config, None if args.no_write else output, batch_dir=args.batch_dir)
    print(json.dumps({key: report[key] for key in ("config_sha256", "input_counts", "data_status_counts", "evidence_status_counts")}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())