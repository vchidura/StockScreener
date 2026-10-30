"""Two-track, shard-based autonomous recovery runner for legacy equity backtests."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

BACKEND_DIR = Path(__file__).resolve().parents[1]
ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

from research.legacy_backtest_variants import lane_variants
from research.stock_idea_engine import PublicationLedger, digest
from research.stock_idea_evaluation import evaluate_publications, validate_plan
from research.stock_idea_models import daily_contexts, daily_observations, feature_frames, higher_context_observations, intraday_observations
from research.stock_idea_replay import replay_observations, summarize, utc, validate_inputs, write_once
from scripts.run_legacy_equity_autonomous_backtests import (
    _best_mode_window,
    _build_config_for_window,
    _clip_window,
    _d1_d2_input_readiness,
    _read_only_guard,
    _sanitize_bundle,
    _study_status,
    inventory_modes,
)
from scripts.run_stock_idea_replay import freeze_inputs


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, default=str, allow_nan=False) + "\n").encode("utf-8")


def _write_json_gz(path: Path, value: Any) -> dict[str, Any]:
    payload = _json_bytes(value)
    compressed = gzip.compress(payload)
    if path.exists():
        if path.read_bytes() != compressed:
            raise ValueError(f"refusing to overwrite a different artifact: {path}")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as handle:
            handle.write(compressed)
    return {
        "path": str(path),
        "sha256_gzip": _sha256_bytes(compressed),
        "sha256_json": _sha256_bytes(payload),
        "json_bytes": len(payload),
        "gzip_bytes": len(compressed),
    }


def _write_progress(path: Path, value: Any) -> None:
    content = json.dumps(value, sort_keys=True, indent=2, default=str, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def _read_json_gz(path: Path) -> Any:
    return json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))


def _split_shards(values: list[str], size: int) -> list[list[str]]:
    return [values[i:i + size] for i in range(0, len(values), size)]


def _count_observation_candidates(observations: list[dict[str, Any]]) -> int:
    return sum(len(item.get("candidates", [])) for item in observations)


def _load_start_cohort_ids(start_session: str, limit: int | None = None) -> list[str]:
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        _read_only_guard(cursor, "stocks_db_fresh")
        cursor.execute(
            """
            WITH picked AS (
              SELECT DISTINCT ON (effective_from::date) universe_run_id
              FROM equity_original_universe_runs
              WHERE policy_version='liquid_us_common_stocks_v2'
                AND availability_mode='HISTORICAL_RECONSTRUCTED'
                AND effective_from::date=%s::date
              ORDER BY effective_from::date, created_at DESC, universe_run_id
            )
            SELECT security_id::text AS security_id
            FROM equity_universe_members
            WHERE universe_run_id=(SELECT universe_run_id FROM picked)
            ORDER BY security_id
            """,
            (start_session,),
        )
        rows = [row["security_id"] for row in cursor.fetchall()]
    return rows if limit is None else rows[:limit]


def _load_intraday_covered_start_cohort_ids(
    *,
    start_session: str,
    intraday_warmup_start: str,
    end_session: str,
    minimum_half_hours: int,
    limit: int | None = None,
) -> list[str]:
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        _read_only_guard(cursor, "stocks_db_fresh")
        cursor.execute(
            """
            WITH picked AS (
              SELECT DISTINCT ON (effective_from::date) universe_run_id
              FROM equity_original_universe_runs
              WHERE policy_version='liquid_us_common_stocks_v2'
                AND availability_mode='HISTORICAL_RECONSTRUCTED'
                AND effective_from::date=%s::date
              ORDER BY effective_from::date, created_at DESC, universe_run_id
            ), members AS (
              SELECT member.security_id, member.ticker
              FROM equity_universe_members AS member
              WHERE member.universe_run_id=(SELECT universe_run_id FROM picked)
            )
            SELECT member.security_id::text AS security_id
            FROM members AS member
            JOIN equity_bar_revisions AS bar ON bar.ticker=member.ticker
            WHERE bar.interval='30m'
              AND bar.session_scope='RTH'
              AND NOT bar.adjusted
              AND bar.is_final
              AND bar.session_date BETWEEN %s::date AND %s::date
            GROUP BY member.security_id
            HAVING COUNT(DISTINCT bar.bar_start) >= %s
            ORDER BY member.security_id
            """,
            (start_session, intraday_warmup_start, end_session, minimum_half_hours),
        )
        rows = [row["security_id"] for row in cursor.fetchall()]
    return rows if limit is None else rows[:limit]


def _build_observations_from_frames(
    *,
    frames: dict[tuple[str, str], Any],
    contexts: dict[tuple[str, str], Any],
    covered_cohort: set[str],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    jobs = [
        (key, frame.loc[frame.session.le(config["end"])])
        for key, frame in sorted(frames.items())
        if key[0] in covered_cohort
    ]

    def detect(job):
        (_, interval), frame = job
        if interval == "1d":
            result = daily_observations(frame, contexts, config) + higher_context_observations(frame, config)
        else:
            result = intraday_observations(frame, interval, contexts, config)
        return [row for row in result if config["start"] <= row["market_time"].date().isoformat() <= config["end"]]

    all_rows = [row for job in jobs for row in detect(job)]
    return sorted(all_rows, key=lambda item: (item["market_time"], item["security_id"], item["interval"]))


def _apply_track_policy(bundle: dict[str, Any], config: dict[str, Any], track: str) -> dict[str, Any]:
    adjusted = json.loads(json.dumps(bundle))
    if track == "AS_OBSERVED":
        adjusted["track_policy"] = {"track": track, "policy_version": "as_observed_v1"}
        adjusted["bars_sha256"] = digest(adjusted["bars"])
        return adjusted
    if track != "RETROSPECTIVE_RECONSTRUCTED_RESEARCH":
        raise ValueError(f"unsupported track: {track}")
    for bar in adjusted["bars"]:
        visible_at = (utc(bar["bar_end"]) + timedelta(minutes=config["provider_delay_minutes"])).isoformat()
        bar["visible_at"] = visible_at
        if bar.get("execution_scale_observed_at") is not None:
            bar["execution_scale_observed_at"] = visible_at
            bar["execution_scale_created_at"] = visible_at
    adjusted["track_policy"] = {
        "track": track,
        "policy_version": "retrospective_reconstructed_research_v1",
        "limitations": [
            "RETROSPECTIVE_RESEARCH_ONLY",
            "NOT_LIVE_OBSERVED_PARITY_EVIDENCE",
            "ORIGINAL_SOURCE_INGESTION_CLOCKS_RETAINED_AS_METADATA",
        ],
    }
    adjusted["bars_sha256"] = digest(adjusted["bars"])
    return adjusted


def _run_variant_on_bundle(
    *,
    bundle: dict[str, Any],
    frames: dict[tuple[str, str], Any],
    contexts: dict[tuple[str, str], Any],
    rank_coverage: dict[str, Any],
    variant_config: dict[str, Any],
    validation_config: dict[str, Any],
    evaluation_plan: dict[str, Any],
    shard_dir: Path,
    track: str,
) -> dict[str, Any]:
    shard_dir.mkdir(parents=True, exist_ok=True)
    study_bundle = json.loads(json.dumps(bundle))
    study_bundle["config_sha256"] = digest(validation_config)
    validation = validate_inputs(study_bundle, validation_config)
    if validation["errors"]:
        raise RuntimeError(f"invalid shard bundle: {validation['errors']}")
    observations = _build_observations_from_frames(
        frames=frames,
        contexts=contexts,
        covered_cohort=set(study_bundle["covered_security_ids"]),
        config=variant_config,
    )
    ledger = PublicationLedger(shard_dir / "replay.sqlite")
    publications, outcomes = replay_observations(study_bundle, variant_config, observations, ledger)
    evaluation = evaluate_publications(publications, variant_config, evaluation_plan, study_bundle["source_cutoff"])
    summary = summarize(publications, outcomes, variant_config, evaluation_plan)
    summary["status"] = _study_status(evaluation)
    summary["track"] = track
    write_once(shard_dir / "summary.json", summary)
    write_once(shard_dir / "evaluation.json", evaluation)
    write_once(shard_dir / "daily_rank_coverage.json", rank_coverage)
    pub_art = _write_json_gz(shard_dir / "publications.json.gz", publications)
    out_art = _write_json_gz(shard_dir / "candidate_outcomes.json.gz", outcomes)
    compact = {
        "track": track,
        "validation_config_sha256": digest(validation_config),
        "variant_config_sha256": digest(variant_config),
        "candidate_count": _count_observation_candidates(observations),
        "observation_count": len(observations),
        "publication_count": len(publications),
        "selected_total": sum(len(pub.get("selected", [])) for pub in publications),
        "outcome_count": len(outcomes),
        "status": summary["status"],
        "publication_artifact": pub_art,
        "outcome_artifact": out_art,
    }
    write_once(shard_dir / "manifest.json", compact)
    return compact


def run(
    *,
    artifact_root: Path,
    supersedes: str,
    env_file: Path,
    expected_database: str,
    workers: int,
    max_sessions: int,
    shard_size: int,
    shard_start: int,
    shard_limit: int | None,
    variant_keys: set[str] | None,
    eligible_limit: int | None,
) -> dict[str, Any]:
    workers = max(1, min(int(workers), 4))
    shard_size = max(1, min(int(shard_size), 100))
    load_dotenv(env_file)
    artifact_root.mkdir(parents=True, exist_ok=True)

    base_config = json.loads((ROOT / "backend/research/inputs/stock_idea_pilot_config.json").read_text(encoding="utf-8"))
    evaluation_plan = json.loads((ROOT / "backend/research/inputs/stock_idea_evaluation_plan.json").read_text(encoding="utf-8"))
    validate_plan(evaluation_plan, base_config)

    inventory = inventory_modes(expected_database=expected_database)
    _write_progress(artifact_root / "availability-mode-inventory.json", inventory)
    mode_window_30m = _best_mode_window(inventory, "30m")
    mode_window_5m = _best_mode_window(inventory, "5m")
    if mode_window_30m is None:
        raise RuntimeError("no 30m retained bars found across any availability mode")
    d1_d2_readiness = _d1_d2_input_readiness(inventory, mode_window_5m)

    _, start_30m, end_30m = mode_window_30m
    start_30m, end_30m = _clip_window(start_30m, end_30m, max_sessions, base_config["calendar"])
    control_config = _build_config_for_window(base_config, start_30m, end_30m)
    source_cutoff = inventory["transaction"]["cutoff"]
    eligible_ids = _load_intraday_covered_start_cohort_ids(
        start_session=control_config["start"],
        intraday_warmup_start=control_config["intraday_warmup_start"],
        end_session=control_config["end"],
        minimum_half_hours=control_config["feature_warmup_bars"] * 2,
        limit=eligible_limit,
    )
    if not eligible_ids:
        raise RuntimeError("no start-universe securities satisfy the retained 30m warmup prefilter")
    _write_progress(
        artifact_root / "eligible-cohort.json",
        {
            "policy_version": "legacy_intraday_30m_warmup_prefilter_v1",
            "start_session": control_config["start"],
            "intraday_warmup_start": control_config["intraday_warmup_start"],
            "end_session": control_config["end"],
            "minimum_half_hours": control_config["feature_warmup_bars"] * 2,
            "eligible_security_ids": eligible_ids,
            "eligible_count": len(eligible_ids),
            "limit": eligible_limit,
        },
    )

    track_a_root = artifact_root / "A"
    track_b_root = artifact_root / "B"
    track_a_root.mkdir(parents=True, exist_ok=True)
    track_b_root.mkdir(parents=True, exist_ok=True)
    _write_progress(track_a_root / "ingestion-clock-audit.json", {"mode_rows_30m": [row for row in inventory["mode_rows"] if row["interval"] == "30m"]})

    diag_a_path = track_a_root / "control-slice-diagnostic.json"
    diag_b_path = track_b_root / "control-slice-diagnostic.json"
    if diag_a_path.exists() and diag_b_path.exists():
        diag_a = json.loads(diag_a_path.read_text(encoding="utf-8"))
        diag_b = json.loads(diag_b_path.read_text(encoding="utf-8"))
    else:
        diag_start, diag_end = _clip_window(start_30m, end_30m, 15, base_config["calendar"])
        diag_config = _build_config_for_window(base_config, diag_start, diag_end)
        diagnostic_eligible = _load_intraday_covered_start_cohort_ids(
            start_session=diag_config["start"],
            intraday_warmup_start=diag_config["intraday_warmup_start"],
            end_session=diag_config["end"],
            minimum_half_hours=diag_config["feature_warmup_bars"] * 2,
            limit=min(shard_size, 40),
        )
        diag_ids = diagnostic_eligible
        diag_bundle = freeze_inputs(
            diag_config,
            source_cutoff,
            requested_cohort=diag_ids,
            evaluation_plan=evaluation_plan,
            include_all_daily_members=False,
            allow_any_adjusted_daily_mode=False,
            canonicalize_security_ids=True,
        )
        diag_bundle, diag_sanitized = _sanitize_bundle(diag_bundle)

        def run_diagnostic(track: str, out_dir: Path) -> dict[str, Any]:
            tracked = _apply_track_policy(diag_bundle, diag_config, track)
            frames = feature_frames(tracked, diag_config)
            contexts, rank_coverage = daily_contexts(frames, tracked["memberships"], diag_config)
            observations = _build_observations_from_frames(
                frames=frames,
                contexts=contexts,
                covered_cohort=set(tracked["covered_security_ids"]),
                config=diag_config,
            )
            report = {
                "track": track,
                "generated_at": _now(),
                "window": {"start": diag_config["start"], "end": diag_config["end"]},
                "cohort_requested": len(diag_ids),
                "cohort_covered": len(tracked["covered_security_ids"]),
                "observation_count": len(observations),
                "candidate_count": _count_observation_candidates(observations),
                "sanitation": diag_sanitized,
                "rank_coverage_rows": (
                    len(rank_coverage.get("coverage", []))
                    if isinstance(rank_coverage, dict)
                    else len(rank_coverage)
                ),
            }
            _write_progress(out_dir / "control-slice-diagnostic.json", report)
            return report

        diag_a = run_diagnostic("AS_OBSERVED", track_a_root)
        diag_b = run_diagnostic("RETROSPECTIVE_RECONSTRUCTED_RESEARCH", track_b_root)
    if diag_b["candidate_count"] <= 0:
        raise RuntimeError("retrospective control-slice still produced zero candidates")

    _write_progress(track_a_root / "final-track-report.json", {
        "track": "AS_OBSERVED",
        "generated_at": _now(),
        "supersedes_run": supersedes,
        "window": {"start": control_config["start"], "end": control_config["end"]},
        "diagnostic": diag_a,
        "limitations": ["INGESTION_CLOCK_BOUND", "OPERATIONAL_EVIDENCE_ONLY"],
    })

    shards = _split_shards(eligible_ids, shard_size)
    variants = lane_variants(control_config)
    variant_specs = [
        {
            "key": f"v{index:02d}",
            "lane": lane,
            "variant_name": variant_name,
            "variant_config": variant_config,
            "variant_id": variant_config.get("variant_policy_version", variant_name),
        }
        for index, (lane, variant_name, variant_config) in enumerate(variants, 1)
    ]
    if variant_keys is not None:
        variant_specs = [spec for spec in variant_specs if spec["key"] in variant_keys]
        if not variant_specs:
            raise RuntimeError("no variants selected after applying --variant-key filter")
    studies_root = track_b_root / "s"
    aggregates_root = track_b_root / "a"
    shard_index = []

    shard_start = max(1, int(shard_start))
    if shard_limit is None:
        selected_shards = list(enumerate(shards, 1))[shard_start - 1:]
    else:
        shard_limit = max(1, int(shard_limit))
        selected_shards = list(enumerate(shards, 1))[shard_start - 1:shard_start - 1 + shard_limit]

    for shard_number, requested_ids in selected_shards:
        bundle = freeze_inputs(
            control_config,
            source_cutoff,
            requested_cohort=requested_ids,
            evaluation_plan=evaluation_plan,
            include_all_daily_members=False,
            allow_any_adjusted_daily_mode=False,
            canonicalize_security_ids=True,
        )
        bundle, sanitation = _sanitize_bundle(bundle)
        bundle = _apply_track_policy(bundle, control_config, "RETROSPECTIVE_RECONSTRUCTED_RESEARCH")
        if not bundle["covered_security_ids"]:
            shard_index.append({"shard": shard_number, "requested_ids": len(requested_ids), "covered_ids": 0, "status": "EMPTY_COHORT_AFTER_WARMUP"})
            continue
        frames = feature_frames(bundle, control_config)
        contexts, rank_coverage = daily_contexts(frames, bundle["memberships"], control_config)
        shard_row = {
            "shard": shard_number,
            "requested_ids": len(requested_ids),
            "covered_ids": len(bundle["covered_security_ids"]),
            "status": "COMPLETED",
            "sanitation": sanitation,
            "variants": {},
        }
        for spec in variant_specs:
            shard_dir = studies_root / spec["key"] / f"shard-{shard_number:03d}"
            if (shard_dir / "manifest.json").exists():
                shard_row["variants"][f"{spec['lane']}:{spec['variant_name']}:{spec['variant_id']}"] = json.loads(
                    (shard_dir / "manifest.json").read_text(encoding="utf-8")
                )
                continue
            shard_row["variants"][f"{spec['lane']}:{spec['variant_name']}:{spec['variant_id']}"] = _run_variant_on_bundle(
                bundle=bundle,
                frames=frames,
                contexts=contexts,
                rank_coverage=rank_coverage,
                variant_config=spec["variant_config"],
                validation_config=control_config,
                evaluation_plan=evaluation_plan,
                shard_dir=shard_dir,
                track="RETROSPECTIVE_RECONSTRUCTED_RESEARCH",
            )
        shard_index.append(shard_row)
        _write_progress(track_b_root / "shard-progress.json", shard_index)

    _write_progress(track_b_root / "shard-index.json", shard_index)

    study_results = []
    for spec in variant_specs:
        variant_root = studies_root / spec["key"]
        publications = []
        outcomes = {}
        for file in sorted(variant_root.glob("shard-*/publications.json.gz")):
            publications.extend(_read_json_gz(file))
        for file in sorted(variant_root.glob("shard-*/candidate_outcomes.json.gz")):
            outcomes.update(_read_json_gz(file))
        if not publications:
            continue
        evaluation = evaluate_publications(publications, spec["variant_config"], evaluation_plan, source_cutoff)
        summary = summarize(publications, outcomes, spec["variant_config"], evaluation_plan)
        summary["status"] = _study_status(evaluation)
        summary["track"] = "RETROSPECTIVE_RECONSTRUCTED_RESEARCH"
        agg_dir = aggregates_root / spec["key"]
        agg_dir.mkdir(parents=True, exist_ok=True)
        write_once(agg_dir / "evaluation.json", evaluation)
        write_once(agg_dir / "summary.json", summary)
        write_once(agg_dir / "manifest.json", {
            "publication_count": len(publications),
            "outcome_count": len(outcomes),
            "selected_total": sum(len(pub.get("selected", [])) for pub in publications),
            "status": summary["status"],
        })
        study_results.append({
            "lane": spec["lane"],
            "variant": spec["variant_name"],
            "variant_policy_version": spec["variant_id"],
            "status": summary["status"],
            "publication_count": len(publications),
            "selected_total": sum(len(pub.get("selected", [])) for pub in publications),
            "outcome_count": len(outcomes),
            "variant_key": spec["key"],
        })

    blocked_lanes = [
        {"lane": "sw1", "status": "BLOCKED_INPUT_READINESS", "reason": "EARNINGS_EVENT_COVERAGE_ABSENT"},
        {"lane": "p1_p2", "status": "BLOCKED_INPUT_READINESS", "reason": "PIT_FUNDAMENTALS_ABSENT"},
    ]
    if d1_d2_readiness["status"] == "BLOCKED_INPUT_READINESS":
        blocked_lanes.append({"lane": "d1_d2", **d1_d2_readiness})

    _write_progress(track_b_root / "final-track-report.json", {
        "track": "RETROSPECTIVE_RECONSTRUCTED_RESEARCH",
        "track_policy_version": "retrospective_reconstructed_research_v1",
        "generated_at": _now(),
        "supersedes_run": supersedes,
        "window": {"start": control_config["start"], "end": control_config["end"]},
        "diagnostic": diag_b,
        "study_results": study_results,
        "blocked_lanes": blocked_lanes,
        "d1_d2_input_readiness": d1_d2_readiness,
        "shard_count": len(shards),
        "processed_shards_in_this_run": [number for number, _ in selected_shards],
        "limitations": [
            "RETROSPECTIVE_RESEARCH_ONLY",
            "NOT_LIVE_OBSERVED_PARITY_EVIDENCE",
            "NO_DB_ROW_MUTATION_OR_RELABELING",
        ],
    })

    run_index = {
        "run_version": "legacy_autonomous_recovery_run_v2",
        "generated_at": _now(),
        "artifact_root": str(artifact_root),
        "track_reports": {
            "AS_OBSERVED": str(track_a_root / "final-track-report.json"),
            "RETROSPECTIVE_RECONSTRUCTED_RESEARCH": str(track_b_root / "final-track-report.json"),
        },
    }
    _write_progress(artifact_root / "run-index.json", run_index)
    return run_index


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--supersedes-run", required=True)
    parser.add_argument("--env-file", type=Path, default=Path(r"C:\Users\vachi\source\repo\AlphaResearchPro\backend\.env"))
    parser.add_argument("--expected-database", default="stocks_db_fresh")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--max-sessions", type=int, default=45)
    parser.add_argument("--shard-size", type=int, default=80)
    parser.add_argument("--shard-start", type=int, default=1)
    parser.add_argument("--shard-limit", type=int, default=None)
    parser.add_argument("--variant-key", action="append", dest="variant_keys", help="Repeatable vNN selector, e.g. --variant-key v01")
    parser.add_argument(
        "--eligible-limit",
        type=int,
        default=None,
        help="Optional bounded eligible-cohort limit for a one-shard correctness run",
    )
    args = parser.parse_args()
    selected_variant_keys = None if not args.variant_keys else {item.strip() for item in args.variant_keys if item.strip()}
    report = run(
        artifact_root=args.artifact_root,
        supersedes=args.supersedes_run,
        env_file=args.env_file,
        expected_database=args.expected_database,
        workers=args.workers,
        max_sessions=args.max_sessions,
        shard_size=args.shard_size,
        shard_start=args.shard_start,
        shard_limit=args.shard_limit,
        variant_keys=selected_variant_keys,
        eligible_limit=args.eligible_limit,
    )
    print(json.dumps(report, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
