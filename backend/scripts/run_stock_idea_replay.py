"""Plan, freeze existing inputs, validate, or replay the covered-universe pilot."""
from __future__ import annotations

import argparse
from datetime import timedelta
import json
from pathlib import Path
import sys

BACKEND = Path(__file__).resolve().parents[1]
ROOT = BACKEND.parent
sys.path.insert(0, str(BACKEND))

from research.stock_idea_engine import digest
from research.stock_idea_replay import derive_hours, publication_windows, run_replay, session_windows, utc, validate_inputs, write_once


def freeze_inputs(config, cutoff, requested_cohort=None, evaluation_plan=None):
    from collections import defaultdict
    from dotenv import load_dotenv
    import exchange_calendars

    load_dotenv(BACKEND / ".env")
    from database import get_db_cursor

    cutoff = utc(cutoff)
    if requested_cohort is not None and (not requested_cohort or len(requested_cohort) != len(set(requested_cohort))):
        raise ValueError("an explicit cohort must be nonempty and contain unique security IDs")
    if cutoff < session_windows(config["end"])[-1][1]:
        raise ValueError("the pilot source cutoff must follow its last decision boundary")
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cursor.execute("""
            WITH runs AS (
                SELECT DISTINCT ON(effective_from::date) universe_run_id,effective_from,observed_at,created_at
                FROM equity_original_universe_runs
                WHERE policy_version='liquid_us_common_stocks_v2'
                  AND availability_mode='HISTORICAL_RECONSTRUCTED'
                  AND effective_from::date BETWEEN %s::date AND %s::date
                  AND observed_at<=%s AND created_at<=%s
                ORDER BY effective_from::date,created_at DESC,universe_run_id
            ) SELECT runs.effective_from::date::text AS session,member.security_id::text,member.ticker,
                runs.universe_run_id::text,runs.observed_at,runs.created_at
              FROM runs JOIN equity_universe_members member USING(universe_run_id)
              ORDER BY session,security_id
        """, (config["intraday_warmup_start"], config["end"], cutoff, cutoff))
        members = [dict(row) for row in cursor.fetchall()]
        print(f"Pinned {len(members):,} dated memberships", file=sys.stderr, flush=True)
        initial = {member["security_id"]: member["ticker"] for member in members if member["session"] == config["start"]}
        if not initial:
            raise ValueError("no pinned dated universe for the pilot's first session")
        if requested_cohort is not None and not set(requested_cohort) <= set(initial):
            raise ValueError("requested cohort contains identities outside the first dated universe")
        intraday_names = sorted({initial[security] for security in (requested_cohort or initial)})
        daily_names = sorted({member["ticker"] for member in members} | {"SPY"})

        def read_bars(names, interval, start, adjusted):
            records = []
            for offset in range(0, len(names), 40):
                cursor.execute("""
                    SELECT DISTINCT ON(ticker,bar_start) ticker,security_id::text,interval,
                        session_date::text AS session,bar_start,bar_end,bar_revision_id::text AS revision_id,
                        open_price::float8 AS open,high_price::float8 AS high,low_price::float8 AS low,
                        close_price::float8 AS close,volume::float8,system_observed_at,created_at,
                        availability_mode,source_kind
                    FROM equity_bar_revisions
                    WHERE ticker=ANY(%s::text[]) AND interval=%s AND session_scope='RTH'
                      AND adjusted=%s AND is_final AND session_date BETWEEN %s::date AND %s::date
                                            AND bar_start>=%s::date AT TIME ZONE 'UTC'
                                            AND bar_start<(%s::date + INTERVAL '1 day') AT TIME ZONE 'UTC'
                      AND system_observed_at<=%s AND created_at<=%s AND bar_end<=%s
                      AND (NOT %s OR (availability_mode='HISTORICAL_RECONSTRUCTED'
                           AND quality_codes @> ARRAY['GROUPED_DAILY_EXACT_TICKER_V2']::text[]))
                    ORDER BY ticker,bar_start,
                        CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1 ELSE 2 END,
                        system_observed_at DESC,created_at DESC,bar_revision_id
                """, (names[offset:offset + 40], interval, adjusted, start, config["exit_end"], start, config["exit_end"], cutoff, cutoff, cutoff, adjusted))
                records.extend(dict(row) for row in cursor.fetchall())
                if offset % 200 == 0 or offset + 40 >= len(names):
                    print(f"Pinned {interval} adjusted={adjusted}: {min(offset + 40, len(names))}/{len(names)} tickers, {len(records):,} rows",
                          file=sys.stderr, flush=True)
            return json.loads(json.dumps(records, default=str))

        native = read_bars(intraday_names, "30m", config["intraday_warmup_start"], False)
        opening = session_windows(config["start"])[0][0]
        hours = derive_hours([bar for bar in native if utc(bar["bar_end"]) < opening], config)
        by_security = defaultdict(set)
        for bar in hours:
            by_security[bar["security_id"]].add(utc(bar["bar_start"]))
        calendar = exchange_calendars.get_calendar("XNYS")
        prior = calendar.previous_session(config["start"])
        expected = [start for session in calendar.sessions_in_range(config["intraday_warmup_start"], prior)
                    for start, _ in session_windows(str(session.date()), "1h")][-config["feature_warmup_bars"]:]
        cohort = sorted(security for security in (requested_cohort or initial) if set(expected) <= by_security[security])
        if not cohort:
            raise ValueError("no dated members have the required contiguous pre-pilot hourly warm-up")
        print(f"Frozen pre-pilot covered cohort: {len(cohort)}/{len(initial)} dated members", file=sys.stderr, flush=True)
        native = [bar for bar in native if bar["security_id"] in cohort]
        daily = read_bars(daily_names, "1d", config["warmup_start"], True)
        raw_daily = read_bars(sorted({initial[security] for security in cohort}), "1d", config["warmup_start"], False)
        raw_by_key = {(bar["security_id"], bar["session"]): bar for bar in raw_daily}
        for bar in daily:
            raw = raw_by_key.get((bar["security_id"], bar["session"]))
            bar["price_basis"] = "SPLIT_ADJUSTED"
            bar["execution_scale"] = raw["close"] / bar["close"] if raw is not None and bar["close"] > 0 else None
            bar["execution_scale_revision_ids"] = [raw["revision_id"]] if raw else []
            if raw:
                bar["execution_scale_observed_at"] = raw["system_observed_at"]
                bar["execution_scale_created_at"] = raw["created_at"]
        actions = []
        for offset in range(0, len(daily_names), 40):
            cursor.execute("""
                SELECT ticker,security_id::text,action_type,effective_date::text,first_observed_at,created_at
                FROM equity_corporate_actions WHERE ticker=ANY(%s::text[])
                  AND effective_date BETWEEN %s::date AND %s::date
                  AND first_observed_at<=%s AND created_at<=%s
                ORDER BY ticker,effective_date,action_type,first_observed_at,created_at
            """, (daily_names[offset:offset + 40], config["warmup_start"], config["exit_end"], cutoff, cutoff))
            actions.extend(dict(row) for row in cursor.fetchall())
    bars = sorted(native + daily, key=lambda bar: (bar["security_id"], bar["interval"], bar["bar_start"]))
    members = json.loads(json.dumps(members, default=str))
    actions = json.loads(json.dumps(actions, default=str))
    result = dict(schema_version=1, config_sha256=digest(config), source_cutoff=cutoff.isoformat(),
        covered_security_ids=cohort, cohort_rule="FIRST_DATED_UNIVERSE_AND_200_CONTIGUOUS_PRE_PILOT_DERIVED_HOURS",
        cohort_exclusions=sorted(set(initial) - set(cohort)), memberships=members, memberships_sha256=digest(members),
        bars=bars, bars_sha256=digest(bars), actions=actions, actions_sha256=digest(actions),
        action_coverage="KNOWN_ACTIONS_ONLY_NOT_CERTIFIED", extraction="READ_ONLY_EXISTING_DATA_NO_DOWNLOADS")
    if evaluation_plan is not None:
        result.update(study_id=evaluation_plan["study_id"], evaluation_plan_sha256=digest(evaluation_plan))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--freeze-inputs", action="store_true")
    mode.add_argument("--validate", action="store_true")
    mode.add_argument("--rank-readiness", action="store_true")
    mode.add_argument("--replay", action="store_true")
    parser.add_argument("--config", type=Path, default=ROOT / "backend/research/inputs/stock_idea_pilot_config.json")
    parser.add_argument("--evaluation-plan", type=Path, default=ROOT / "backend/research/inputs/stock_idea_evaluation_plan.json")
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--source-cutoff")
    parser.add_argument("--cohort", type=Path, help="Optional frozen JSON array of security IDs; never ticker-only identities")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--validation-report", type=Path)
    args = parser.parse_args(argv)
    config = json.loads(args.config.read_text(encoding="utf-8"))
    evaluation_plan = json.loads(args.evaluation_plan.read_text(encoding="utf-8"))
    from research.stock_idea_evaluation import validate_plan
    validate_plan(evaluation_plan, config)
    if not any((args.freeze_inputs, args.validate, args.rank_readiness, args.replay)):
        result = dict(mode="PLAN_ONLY", config_sha256=digest(config), classification=config["classification"],
            study_id=evaluation_plan["study_id"], evaluation_plan_sha256=digest(evaluation_plan),
            primary_family_size=evaluation_plan["primary_family_size"], inherited_qualification_ids=[],
            decision_start=config["start"], decision_end=config["end"], windows=len(publication_windows(config)),
            warmup_start=config["warmup_start"], intraday_warmup_start=config["intraday_warmup_start"],
            exit_coverage_end=config["exit_end"], source_cutoff_required=True,
            fixed_execution_interval=config["execution_interval"], writes_database=False, downloads=False,
            next_step="Freeze existing inputs at an explicit source cutoff, validate, then replay to a new directory")
    elif args.inputs is None:
        parser.error("--inputs is required for freeze, validation, and replay")
    elif args.freeze_inputs:
        if args.inputs.exists():
            parser.error("refusing to overwrite an existing frozen input file")
        if not args.source_cutoff:
            parser.error("--freeze-inputs requires --source-cutoff")
        cohort = json.loads(args.cohort.read_text(encoding="utf-8")) if args.cohort else None
        bundle = freeze_inputs(config, args.source_cutoff, cohort, evaluation_plan)
        args.inputs.parent.mkdir(parents=True, exist_ok=True)
        write_once(args.inputs, bundle)
        result = validate_inputs(bundle, config)
    else:
        bundle = json.loads(args.inputs.read_text(encoding="utf-8"))
        if args.validate:
            result = validate_inputs(bundle, config)
        elif args.rank_readiness:
            from collections import Counter
            from research.stock_idea_models import feature_frames, daily_contexts
            validation = validate_inputs(bundle, config)
            if validation["errors"]:
                raise ValueError("; ".join(validation["errors"]))
            print("Computing pinned daily features and ranks; no detection or returns", file=sys.stderr, flush=True)
            daily_bundle = dict(bundle, bars=[bar for bar in bundle["bars"] if bar["interval"] == "1d"])
            frames = feature_frames(daily_bundle, config)
            contexts, coverage = daily_contexts(frames, bundle["memberships"], config)
            cohort = set(bundle["covered_security_ids"])
            counts = Counter(session for security, session in contexts if security in cohort)
            result = dict(status="READINESS_ONLY", study_id=evaluation_plan["study_id"], returns_evaluated=False,
                source_cutoff=bundle["source_cutoff"], inputs_sha256=digest(bundle), daily_frames=len(frames),
                covered_security_count=len(cohort), rows=[dict(row, covered_with_context=counts[row["session"]]) for row in coverage])
            report = args.validation_report or args.inputs.with_suffix(".rank-readiness.json")
            report.parent.mkdir(parents=True, exist_ok=True)
            write_once(report, result)
        else:
            if args.output is None or args.workers < 1:
                parser.error("--replay requires a new --output directory and positive --workers")
            result = run_replay(bundle, config, args.output, args.workers, evaluation_plan)
    if args.freeze_inputs or args.validate:
        report = args.validation_report or args.inputs.with_suffix(".validation.json")
        report.parent.mkdir(parents=True, exist_ok=True)
        write_once(report, result)
        printed = {key: value for key, value in result.items() if key not in ("coverage", "exclusions")}
        printed.update(validation_report=str(report), exclusion_count=len(result["exclusions"]),
            covered_security_count=len(bundle["covered_security_ids"]),
            coverage=[{key: value for key, value in row.items() if key != "excluded_population"} for row in result["coverage"]])
    elif args.rank_readiness:
        printed = dict(result, rows=[{key: value for key, value in row.items() if key != "missing_or_ineligible"} for row in result["rows"]])
    elif args.replay:
        printed = {key: value for key, value in result.items() if key not in ("cells", "paired_selection_lift")}
    else:
        printed = result
    print(json.dumps(printed, indent=2, default=str, allow_nan=False))
    return 2 if result.get("status") == "BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())