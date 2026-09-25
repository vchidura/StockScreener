#!/usr/bin/env python3
"""Report current retained stock-behavior metric and source-contract coverage."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sys

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.behavior_coverage import (
    summarize_adjusted_bar_inventory, summarize_coverage,
    summarize_hourly_split_continuity,
)
from equity.behavior_sources import (
    BEHAVIOR_SOURCE_SELECTION_POLICY, OPTIONS_SWING_HYBRID_SOURCE_POLICY,
    source_tail_bars_by_interval,
)
from equity.domain import DecisionWatermark
from equity.repositories import EquityBarRepository, EquityCorporateActionRepository, EquityEvidenceRepository
from options.config import load_option_runtime_configuration


def summarize_stock_setup_source(path, *, as_of, maximum_publications=12):
    from collections import Counter
    from contextlib import closing
    import sqlite3
    import zlib

    from equity.stock_alert_results import _decode_original_setup_payload, utc

    path = path.resolve()
    if not path.is_file() or not 1 <= maximum_publications <= 48 or as_of.utcoffset() is None:
        raise ValueError("stock setup source review requires a bounded existing ledger and aware cutoff")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        rows = connection.execute("""SELECT window_key,CASE WHEN length(payload)<=4194304 THEN payload END
            FROM forward_publications ORDER BY window_key DESC LIMIT ?""", (maximum_publications,)).fetchall()
    if any(payload is None for _, payload in rows):
        raise ValueError("stock setup source review exceeds publication or payload bound")
    publications = []
    totals = Counter()
    statuses = Counter()
    for window_key, payload in rows:
        try:
            publication = _decode_original_setup_payload(payload)
        except (json.JSONDecodeError, zlib.error, ValueError, TypeError) as error:
            raise ValueError("stock setup source review found an invalid publication") from error
        candidates = Counter(row.get("model", "UNKNOWN") for row in publication.get("candidates", {}).values())
        missing_members = set(publication.get("missing_members", ()))
        for row in publication.get("candidates", {}).values():
            model = row.get("model", "UNKNOWN")
            remaining_seconds = (utc(row["expires_at"]) - utc(publication["actual_publication_at"])).total_seconds()
            statuses[(model, "ACTIVE_AT_PUBLICATION" if remaining_seconds > 0 else "EXPIRED_AT_PUBLICATION")] += 1
            for seconds in (300, 900, 1200, 1800):
                if remaining_seconds >= seconds:
                    statuses[(model, f"REMAINING_GE_{seconds}_SECONDS")] += 1
            statuses[(model, "MISSING_MEMBER" if row.get("security_id") in missing_members else "AVAILABLE_MEMBER")] += 1
        active = Counter(row.get("model", "UNKNOWN") for row in publication.get("candidates", {}).values()
            if utc(row["expires_at"]) > as_of)
        active_at_publication = Counter(row.get("model", "UNKNOWN") for row in publication.get("candidates", {}).values()
            if utc(row["expires_at"]) > utc(publication["actual_publication_at"]))
        by_ticker_model = Counter((row.get("ticker", "UNKNOWN"), row.get("model", "UNKNOWN"))
            for row in publication.get("candidates", {}).values())
        expirations = sorted(utc(row["expires_at"]) for row in publication.get("candidates", {}).values())
        dispositions = Counter((row.get("model", "UNKNOWN"), row.get("selection", "UNKNOWN"), row.get("reason"))
            for row in publication.get("dispositions", ()))
        totals.update(candidates)
        publications.append(dict(window_key=window_key, market_time=publication.get("market_time", window_key),
            actual_publication_at=publication.get("actual_publication_at"), coverage=publication.get("coverage"),
            retry_of=publication.get("retry_of"), expected_members=len(publication.get("expected_members", ())),
            missing_members=sorted(publication.get("missing_members", ())), candidates=dict(candidates),
            candidate_ticker_models={f"{ticker}:{model}": count for (ticker, model), count in sorted(by_ticker_model.items())},
            earliest_expiry=expirations[0] if expirations else None, latest_expiry=expirations[-1] if expirations else None,
            active_at_publication=dict(active_at_publication), active_at_review=dict(active),
            selected=len(publication.get("selected", ())),
            dispositions={f"{model}:{selection}:{reason or 'NONE'}": count
                for (model, selection, reason), count in sorted(dispositions.items(), key=lambda item: str(item[0]))}))
    return dict(version="stock_setup_source_review_v1", as_of=as_of, ledger=str(path),
        publications=publications, candidate_totals=dict(totals),
        candidate_status_totals={f"{model}:{status}": count for (model, status), count in sorted(statuses.items())},
        source_writes=0,
        publication_permission=False, execution_permission=False)


def summarize_o1_indicator_observations(records, *, start_date, end_date, as_of):
    from collections import Counter, defaultdict

    def rate(numerator, denominator):
        return round(numerator / denominator, 6) if denominator else None

    baselines = Counter()
    underlyers = Counter()
    directions = Counter()
    sessions = Counter()
    policies = Counter()
    outcomes = Counter()
    metric_statuses = defaultdict(Counter)
    metric_reasons = defaultdict(Counter)
    challenger_verdicts = defaultdict(Counter)
    challenger_reasons = defaultdict(Counter)
    episode_keys = set()
    for record in records:
        observation = record.observation
        baselines[observation.baseline_disposition] += 1
        underlyers[observation.underlyer] += 1
        directions["BULLISH" if observation.direction == 1 else "BEARISH"] += 1
        sessions[observation.scheduled_cycle.date().isoformat()] += 1
        policies[observation.policy.sha256] += 1
        outcomes[observation.outcome_status] += 1
        episode_keys.add((record.dataset_id, record.recurrence_sha256))
        for measurement in observation.measurements:
            metric_statuses[measurement.metric_id][measurement.status] += 1
            metric_reasons[measurement.metric_id].update(measurement.reason_codes)
        for challenger in observation.challengers:
            challenger_verdicts[challenger.challenger_id][challenger.verdict] += 1
            challenger_reasons[challenger.challenger_id].update(challenger.reason_codes)
    total = len(records)
    metrics = {}
    for metric_id in sorted(metric_statuses):
        counts = metric_statuses[metric_id]
        metrics[metric_id] = dict(ready=counts["READY"], unavailable=counts["UNAVAILABLE"],
            ready_rate=rate(counts["READY"], sum(counts.values())), unavailable_reasons=dict(metric_reasons[metric_id]))
    challengers = {}
    for challenger_id in sorted(challenger_verdicts):
        counts = challenger_verdicts[challenger_id]
        available = counts["PASS"] + counts["FAIL"]
        challengers[challenger_id] = dict(passed=counts["PASS"], failed=counts["FAIL"],
            unavailable=counts["UNAVAILABLE"], pass_rate_when_available=rate(counts["PASS"], available),
            reason_counts=dict(challenger_reasons[challenger_id]))
    top_underlyer = underlyers.most_common(1)[0] if underlyers else None
    top_session = sessions.most_common(1)[0] if sessions else None
    return dict(version="option_o1_indicator_period_review_v1",
        status="NO_OBSERVATIONS" if not records else "SHADOW_EVIDENCE_ONLY",
        start_date=start_date, end_date=end_date, as_of=as_of,
        observations=total, distinct_dataset_episodes=len(episode_keys),
        duplicate_dataset_episode_rows=total - len(episode_keys), datasets=sorted({row.dataset_id for row in records}),
        baseline_dispositions=dict(baselines), metric_availability=metrics, challenger_verdicts=challengers,
        concentration=dict(underlyers=dict(underlyers), directions=dict(directions), sessions=dict(sessions),
            largest_underlyer_share=rate(top_underlyer[1], total) if top_underlyer else None,
            largest_session_share=rate(top_session[1], total) if top_session else None),
        policy_sha256s=dict(policies), outcome_statuses=dict(outcomes),
        inference=dict(independent_samples=False, correlated_observations=True,
            outcome_comparison_ready=False, admission_changed=False, execution_changed=False),
        limitations=["CONTRACT_EPISODES_WITHIN_A_SESSION_AND_UNDERLYER_ARE_CORRELATED",
            "CHALLENGER_VERDICTS_ARE_DESCRIPTIVE_NOT_PERFORMANCE_ESTIMATES",
            "OUTCOMES_ARE_NOT_YET_MEASURED", "NO_ADMISSION_PACKAGE_OR_EXECUTION_EFFECT"],
        source_writes=0, publication_permission=False, execution_permission=False)


def summarize_stock_setup_indicator_observations(records, *, start_date, end_date, as_of):
    from collections import Counter, defaultdict

    def rate(numerator, denominator):
        return round(numerator / denominator, 6) if denominator else None

    by_model = {}
    for model in ("S1", "S2"):
        scoped = [row for row in records if row.detector_id == model]
        baselines = Counter(row.observation.baseline_disposition for row in scoped)
        baseline_reasons = Counter(reason for row in scoped for reason in row.observation.baseline_reasons)
        source_statuses = Counter(row.observation.setup_source.source_status for row in scoped)
        setup_dispositions = Counter(
            f"{row.observation.setup_source.setup_selection}:{row.observation.setup_source.setup_reason or 'NONE'}"
            for row in scoped)
        underlyers = Counter(row.observation.underlyer for row in scoped)
        directions = Counter("BULLISH" if row.observation.direction == 1 else "BEARISH" for row in scoped)
        metrics = defaultdict(Counter)
        metric_reasons = defaultdict(Counter)
        challengers = defaultdict(Counter)
        challenger_reasons = defaultdict(Counter)
        for record in scoped:
            for measurement in record.observation.measurements:
                metrics[measurement.metric_id][measurement.status] += 1
                metric_reasons[measurement.metric_id].update(measurement.reason_codes)
            for challenger in record.observation.challengers:
                challengers[challenger.challenger_id][challenger.verdict] += 1
                challenger_reasons[challenger.challenger_id].update(challenger.reason_codes)
        metric_summary = {metric_id: dict(ready=counts["READY"], unavailable=counts["UNAVAILABLE"],
            ready_rate=rate(counts["READY"], sum(counts.values())), unavailable_reasons=dict(metric_reasons[metric_id]))
            for metric_id, counts in sorted(metrics.items())}
        challenger_summary = {}
        for challenger_id, counts in sorted(challengers.items()):
            available = counts["PASS"] + counts["FAIL"]
            challenger_summary[challenger_id] = dict(passed=counts["PASS"], failed=counts["FAIL"],
                unavailable=counts["UNAVAILABLE"], pass_rate_when_available=rate(counts["PASS"], available),
                reason_counts=dict(challenger_reasons[challenger_id]))
        by_model[model] = dict(observations=len(scoped), baseline_dispositions=dict(baselines),
            baseline_reasons=dict(baseline_reasons), source_statuses=dict(source_statuses),
            setup_dispositions=dict(setup_dispositions),
            expired_before_option_decision=baseline_reasons["STOCK_EPISODE_EXPIRED_BEFORE_OPTION_DECISION"],
            structurally_blocked=baseline_reasons["STOCK_EPISODE_STRUCTURALLY_BLOCKED"],
            confirmed_with_option_activity=baselines["CONFIRMED"], unmatched_option_activity=baselines["UNMATCHED"],
            metric_availability=metric_summary, challenger_verdicts=challenger_summary,
            concentration=dict(underlyers=dict(underlyers), directions=dict(directions)))
    episode_keys = {(row.dataset_id, row.detector_id, row.recurrence_sha256) for row in records}
    return dict(version="option_stock_setup_indicator_period_review_v1",
        status="NO_OBSERVATIONS" if not records else "SHADOW_EVIDENCE_ONLY",
        start_date=start_date, end_date=end_date, as_of=as_of, observations=len(records),
        distinct_dataset_model_episodes=len(episode_keys), duplicate_dataset_model_episode_rows=len(records) - len(episode_keys),
        datasets=sorted({row.dataset_id for row in records}), models=by_model,
        inference=dict(independent_samples=False, correlated_observations=True,
            timing_loss_is_separate_cohort=True, outcome_comparison_ready=False,
            admission_changed=False, execution_changed=False),
        limitations=["STOCK_EPISODES_WITHIN_A_SESSION_AND_UNDERLYER_ARE_CORRELATED",
            "EXPIRED_EPISODES_MEASURE_PIPELINE_TIMING_NOT_OPTION_SIGNAL_FAILURE",
            "CHALLENGER_VERDICTS_ARE_DESCRIPTIVE_NOT_PERFORMANCE_ESTIMATES",
            "OUTCOMES_ARE_NOT_YET_MEASURED", "NO_ADMISSION_PACKAGE_OR_EXECUTION_EFFECT"],
        source_writes=0, publication_permission=False, execution_permission=False)


def o1_confirmation_diagnostic(sources, repository, *, decision_at):
    from collections import Counter
    from zoneinfo import ZoneInfo

    from options.detector_launch import aligned_stock_windows
    from options.dual_origin import assess_options_intraday
    from options.intraday_participation import bind_intraday_components

    if decision_at.utcoffset() is None or len(sources["activity"]) > 20000:
        raise ValueError("O1 confirmation diagnostic requires bounded causal inputs")
    cache = {}
    dispositions, reasons, source_sessions = Counter(), Counter(), Counter()
    examples = []
    detected = [row for row in sources["activity"] if row["finding"].disposition == "DETECTED"]
    for row in detected:
        source = row["source"]
        window = aligned_stock_windows({source.underlyer: source.market_time})[source.underlyer][1]
        key = (source.security_id, window)
        if key not in cache:
            carrier = repository.intraday_confirmation_source(
                security_id=source.security_id,
                market_cutoff=window or source.market_time,
                as_of=decision_at,
            )
            cache[key] = bind_intraday_components(carrier, received_at=decision_at) if carrier else None
        stock = cache[key]
        decision = assess_options_intraday(
            source,
            stock,
            direction=1 if source.contract_type == "CALL" else -1,
            market_cutoff=max(row["lineage"].scheduled_cycle, row["lineage"].market_time),
            decision_at=decision_at,
        )
        dispositions[decision.disposition] += 1
        reasons.update(decision.reasons)
        trend = next((component for component in stock.components if component.key == "TREND.30m"), None) if stock else None
        trend_dates = {
            origin.market_time.astimezone(ZoneInfo("America/New_York")).date()
            for origin in trend.sources
        } if trend else set()
        if not trend_dates:
            basis = "NO_30M_SOURCE"
        elif trend_dates == {source.volume_session}:
            basis = "SAME_SESSION_30M"
        elif max(trend_dates) < source.volume_session:
            basis = "PRIOR_SESSION_30M"
        else:
            basis = "MIXED_OR_FUTURE_30M"
        source_sessions[basis] += 1
        if len(examples) < 8:
            examples.append(dict(
                underlyer=source.underlyer,
                contract_id=source.contract_id,
                option_market_time=source.market_time,
                completed_window=window,
                disposition=decision.disposition,
                reasons=decision.reasons,
                source_session_basis=basis,
                trend_market_times=sorted(
                    origin.market_time for origin in trend.sources
                ) if trend else [],
            ))
    return dict(
        detected_activity=len(detected),
        confirmation_dispositions=dict(sorted(dispositions.items())),
        confirmation_reasons=dict(sorted(reasons.items())),
        source_session_basis=dict(sorted(source_sessions.items())),
        examples=examples,
        publication_permission=False,
        execution_permission=False,
    )


def o1_package_dry_run(runtime, launch, recorded, retained, package_sources):
    from collections import Counter
    from types import SimpleNamespace

    from equity.repositories import EquityCorporateActionRepository
    from equity.stock_alert_results import read_direct_stock_setup_shadow_windows, read_direct_stock_setup_windows
    from options.detector_collection import DetectorCycleCollector, DetectorCycleInputs, ProductionDetectorSourceReader
    from options.detector_launch import aligned_stock_windows, LatestCompletedDetectorForwardLaunch
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    if not isinstance(launch, LatestCompletedDetectorForwardLaunch):
        raise ValueError("manual O1 v3 validation requires the latest-completed launch contract")
    cutoff = recorded.selected_at
    source_repository = OptionStockBehaviorAssessmentRepository()
    repository = SimpleNamespace(
        detector_package_sources=lambda **_: package_sources,
        intraday_confirmation_source=source_repository.intraday_confirmation_source,
        detector_technical_sources=source_repository.detector_technical_sources,
    )

    def setups(*, market_cutoff, as_of, market_cutoffs=None):
        return read_direct_stock_setup_windows(
            (BACKEND_DIR / launch.stock_ledger).resolve(),
            policies=(launch.acceptance_source, launch.resumption_source),
            windows=aligned_stock_windows(market_cutoffs),
            as_of=as_of,
            clock=lambda: cutoff,
        )

    def setup_shadows(*, market_cutoff, as_of, market_cutoffs=None):
        return read_direct_stock_setup_shadow_windows(
            (BACKEND_DIR / launch.stock_ledger).resolve(),
            policies=(launch.acceptance_source, launch.resumption_source),
            windows=aligned_stock_windows(market_cutoffs),
            as_of=as_of,
            clock=lambda: cutoff,
        )

    clock = lambda: cutoff
    production_sources = ProductionDetectorSourceReader(
        SimpleNamespace(read=lambda **_: retained),
        repository,
        EquityCorporateActionRepository(),
        setup_reader=setups,
        setup_shadow_reader=setup_shadows,
        aligned_setup_windows=True,
        intraday_confirmation=True,
        clock=clock,
    )

    setup_observations = []
    def sources(**arguments):
        inputs = production_sources(**arguments)
        setup_observations.extend(inputs.stock_setup_observations)
        return DetectorCycleInputs(
            matrices=inputs.matrices,
            package_inputs=tuple(row for row in inputs.package_inputs
                if row["decision"].detector_id == "O1"),
            rejections=inputs.rejections,
            reject_invalid_packages=inputs.reject_invalid_packages,
        )

    collector = DetectorCycleCollector(
        sources,
        SimpleNamespace(prior_selected=lambda **_: {}),
        clock=clock,
    )
    run, records = collector(
        configuration=runtime,
        dataset_id=f"{launch.dataset_id}-manual-validation",
        scheduled_cycle=recorded.scheduled_cycle,
        completed_matrices=dict(recorded.source_matrices),
        started_at=cutoff,
    )
    rows = []
    for record in records:
        if record.detector_id != "O1":
            continue
        package = record.package
        rows.append(dict(
            candidate_id=str(package.candidate_id),
            underlyer=package.underlyer,
            direction=package.direction,
            candidate_rank=package.candidate_rank,
            selection_status=record.selection_status,
            selection_reason=record.selection_reason,
            decision_at=package.decision_at,
            entry_deadline=package.entry_deadline,
            exit_deadline=package.exit_deadline,
            event_horizon_status=package.event_horizon_status,
        ))
    return dict(
        status="MANUAL_READ_ONLY_NOT_PERSISTED",
        dataset_id=run.dataset_id,
        scheduled_cycle=run.scheduled_cycle,
        selected_at=run.selected_at,
        o1_records=rows,
        o1_record_count=len(rows),
        selection_counts=dict(run.selection_counts),
        rejections=dict(run.rejections),
        stock_setup_shadow=dict(total=len(setup_observations),
            by_model=dict(Counter(row.detector_id for row in setup_observations)),
            baseline_dispositions={f"{model}:{disposition}": count for (model, disposition), count in
                Counter((row.detector_id, row.baseline_disposition) for row in setup_observations).items()},
            baseline_reasons=dict(Counter(reason for row in setup_observations for reason in row.baseline_reasons)),
            underlyers=sorted({row.underlyer for row in setup_observations})),
        source_matrices_match=dict(run.source_matrices) == dict(recorded.source_matrices),
        publication_permission=False,
        execution_permission=False,
    )


def package_binding_diagnostic(sources, packages, *, valuation_policy=None):
    from collections import Counter, defaultdict
    from options.detector_collection import retained_contract_reference

    activity = {row["snapshot"].snapshot_id: row for row in sources["activity"]}
    legs, references = defaultdict(list), defaultdict(list)
    for row in packages["legs"]:
        legs[row["candidate_id"]].append(row)
    for row in packages["references"]:
        references[row["contract_id"]].append(row)
    counts, examples, reference_examples = Counter(), [], []
    for candidate in packages["candidates"]:
        candidate_legs = legs[candidate["candidate_id"]]
        long_leg = next((leg for leg in candidate_legs if leg["side"] == "BUY"), None)
        matched = activity.get(long_leg["snapshot_id"]) if long_leg else None
        if matched is None or matched["finding"].disposition != "DETECTED":
            counts["MATCHED_PARTICIPATION_UNAVAILABLE"] += 1
            continue
        reasons = set()
        for leg in candidate_legs:
            source = activity.get(leg["snapshot_id"])
            if source is None:
                reasons.add("LEG_SNAPSHOT_UNAVAILABLE")
                continue
            reference = next((row for row in references[leg["contract_id"]]
                if row.get("candidate_id", candidate["candidate_id"]) == candidate["candidate_id"]
                and row["valid_from"] <= candidate["market_data_time"]
                and (row["valid_to"] is None or row["valid_to"] > candidate["market_data_time"])
                and max(row["first_observed_at"], row["revised_observed_at"] or row["first_observed_at"]) <= candidate["observed_time"]), None)
            if reference is None:
                reasons.add("CAUSAL_REFERENCE_UNAVAILABLE")
            else:
                try:
                    retained_contract_reference(reference)
                except (ValueError, KeyError) as error:
                    reasons.add("REFERENCE_DECODE_" + type(error).__name__ + ":" + str(error)[:160])
                    if len(reference_examples) < 3:
                        reference_examples.append({key: reference.get(key) for key in ("contract_ticker", "eligibility_status", "shares_per_contract",
                            "additional_underlyings", "adjustment_metadata", "correction")})
            snapshot = source["snapshot"]
            if not any(bar["security_id"] == matched["security"].security_id and bar["bar_end"] == snapshot.spot_market_data_time
                    and bar["close_price"] == snapshot.spot for bar in packages["raw_bars"]):
                reasons.add("EXACT_RAW_SPOT_UNAVAILABLE")
        counts.update(reasons or {"EXACT_PACKAGE_SOURCES_AVAILABLE"})
        if reasons and len(examples) < 5:
            examples.append(dict(candidate_id=str(candidate["candidate_id"]), underlying=candidate["underlying"], reasons=sorted(reasons)))
    surface, flags, clock_gaps = Counter(), Counter(), []
    for row in sources["activity"]:
        snapshot = row["snapshot"]
        if snapshot.spot_market_data_time != snapshot.market_data_time and len(clock_gaps) < 3:
            clock_gaps.append(dict(option_time=snapshot.market_data_time, spot_time=snapshot.spot_market_data_time,
                absolute_difference_seconds=abs((snapshot.spot_market_data_time - snapshot.market_data_time).total_seconds())))
        flags.update(snapshot.quality_flags)
        skew = (snapshot.market_data_time - snapshot.spot_market_data_time).total_seconds()
        for reason, failed in (("QUALITY_FLAGS_PRESENT", bool(snapshot.quality_flags)),
                ("IV_UNAVAILABLE", not snapshot.iv_converged or snapshot.local_iv is None),
                ("REVISED_SNAPSHOT", snapshot.revised_observed_at is not None),
                ("MARK_CLOCK_MISMATCH", snapshot.mark_market_data_time != snapshot.market_data_time),
            ("SPOT_CLOCK_MISMATCH" if valuation_policy is None else "SPOT_CLOCK_OUTSIDE_VALUATION_POLICY",
                skew != 0 if valuation_policy is None else not 0 <= skew <= valuation_policy.maximum_option_spot_skew_seconds),
            ("VALUATION_POLICY_MISMATCH", valuation_policy is not None and snapshot.valuation_policy_sha256 != valuation_policy.policy_sha256)):
            if failed:
                surface[reason] += 1
    return dict(package_reasons=dict(counts), package_examples=examples,
        reference_examples=reference_examples, surface_clock_examples=clock_gaps,
        surface_point_reasons=dict(surface), surface_quality_flags=dict(flags), counts_may_overlap=True)


def surface_observation_sample(completed, *, limit=5):
    from options.analytics.alert_selection import SurfaceEvaluationEvidence

    if type(limit) is not int or not 1 <= limit <= 5 or len(completed) > 32:
        raise ValueError("surface review requires at most five examples and 32 completed runs")
    observations = []
    for run, records in sorted(completed, key=lambda item: (item[0].scheduled_cycle, str(item[0].run_id))):
        for record in records:
            if isinstance(record, SurfaceEvaluationEvidence):
                observations.append((run, record))
    if len({run.dataset_id for run, _ in completed}) > 1:
        raise ValueError("surface review cannot mix detector datasets")
    detected = [(run, record, finding) for run, record in observations for finding in record.observation.findings]
    first_run = detected[0][0] if detected else None
    initial = [(run, record, finding) for run, record, finding in detected if run.run_id == first_run.run_id]
    initial.sort(key=lambda item: (-abs(item[2].robust_z), item[1].observation.underlyer,
        item[1].observation.expiration_date, item[1].observation.contract_type, item[2].contract_id))
    selected, names = [], set()
    for run, record, finding in initial:
        observation = record.observation
        if observation.underlyer in names or len(selected) == limit:
            continue
        names.add(observation.underlyer)
        later = []
        for subsequent_run, subsequent_record in observations:
            subsequent = subsequent_record.observation
            if (subsequent_run.scheduled_cycle <= run.scheduled_cycle
                    or subsequent.security_id != observation.security_id
                    or subsequent.expiration_date != observation.expiration_date
                    or subsequent.contract_type != observation.contract_type
                    or subsequent.policy_sha256 != observation.policy_sha256):
                continue
            repeated = next((row for row in subsequent.findings if row.contract_id == finding.contract_id), None)
            later.append(dict(run_id=subsequent_run.run_id, evaluation_id=subsequent_record.evaluation_id,
                decision_at=subsequent.decision_at, disposition=subsequent.finding_disposition,
                status="REDETECTED" if repeated else "NOT_REDETECTED_IN_RETAINED_COHORT",
                after_original_validity=subsequent.decision_at >= observation.valid_until,
                residual_iv_points=repeated.residual * 100 if repeated else None,
                robust_z=repeated.robust_z if repeated else None,
                same_residual_sign=(repeated.residual * finding.residual > 0) if repeated else None))
        selected.append(dict(underlying=observation.underlyer, contract_id=finding.contract_id,
            expiration=observation.expiration_date, contract_type=observation.contract_type, strike=finding.strike,
            interpretation="RELATIVELY_RICH_IV" if finding.residual > 0 else "RELATIVELY_CHEAP_IV",
            run_id=run.run_id, evaluation_id=record.evaluation_id, evaluation_sha256=record.sha256,
            episode_id=finding.episode_id, snapshot_id=finding.snapshot_id,
            source_sha256=observation.source_sha256, policy_sha256=observation.policy_sha256,
            scheduled_cycle=run.scheduled_cycle, market_cutoff=observation.market_cutoff,
            decision_at=observation.decision_at, published_at=run.selected_at, valid_until=observation.valid_until,
            remaining_seconds_at_publication=(observation.valid_until - run.selected_at).total_seconds(),
            local_iv_percent=finding.local_iv * 100, fitted_iv_percent=finding.fitted_iv * 100,
            residual_iv_points=finding.residual * 100, robust_z=finding.robust_z,
            input_strikes=observation.input_count, residual_mad=observation.residual_mad,
            provisional_nine_strikes_z3=observation.input_count >= 9 and abs(finding.robust_z) >= 3,
            stock_context=observation.stock_context_status, stock_metrics=dict(observation.stock_metrics),
            event_context=observation.event_context_status, reasons=observation.reasons,
            followup_observations=later, outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES"))
    return dict(version="o2_retained_observation_sample_v1", status="EXPLORATORY_RETAINED_SAMPLE",
        selection_rule="EARLIEST_DETECTED_RUN_ABS_Z_DESC_ONE_PER_UNDERLYING_NO_FUTURE_OUTCOME_SELECTION",
        completed_runs=len(completed), observation_records=len(observations),
        detected_observation_records=sum(bool(record.observation.findings) for _, record in observations),
        detected_contract_occurrences=len(detected),
        distinct_detected_episodes=len({finding.episode_id for _, _, finding in detected}),
        sample_run_id=first_run.run_id if first_run else None, selected=selected,
        limitations=["POST_HOC_DESCRIPTIVE_SAMPLE_NOT_PROSPECTIVE_PERFORMANCE",
            "PROVISIONAL_STRENGTH_CHECK_NOT_CALIBRATED_OR_ACTIVATED",
            "NON_REDETECTION_DOES_NOT_PROVE_RESOLUTION_OR_CONTRACT_COVERAGE",
            "REDETECTION_AFTER_ORIGINAL_EXPIRY_DOES_NOT_EXTEND_VALIDITY",
            "NO_EXECUTABLE_PRICE_EDGE_FILL_OR_PROFITABILITY_ASSESSMENT"],
        publication_permission=False, execution_permission=False)


def retained_surface_observation_sample(session_date, *, limit=5):
    from zoneinfo import ZoneInfo
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository
    from options.repositories.alert_review_sources import configured_detector_dataset

    cutoff = datetime.now(timezone.utc)
    dataset = configured_detector_dataset()
    repository = OptionAlertEvaluationRepository()
    runs = [run for run in repository.completed_runs(dataset_id=dataset, as_of=cutoff)
            if run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() == session_date]
    if len(runs) > 32:
        raise ValueError("surface session review exceeds 32-run bound")
    completed = []
    for run in runs:
        retained = repository.completed_run(dataset_id=dataset, scheduled_cycle=run.scheduled_cycle, as_of=cutoff)
        if retained is None or retained[0].sha256 != run.sha256:
            raise ValueError("surface review lost its exact immutable run")
        completed.append(retained)
    return dict(surface_observation_sample(completed, limit=limit), as_of=cutoff, dataset_id=dataset,
        session_date=session_date, evidence_basis="ORIGINAL_HASH_VERIFIED_COMPLETED_RUNS", source_writes=0)


def read_surface_checkpoint_rows(cursor, *, run, underlying, expiration, contract_type, checkpoint_at, matrix_id=None):
    from datetime import timedelta

    cursor.execute("""SELECT analysis.matrix_id,analysis.batch_id,analysis.market_time,analysis.observed_time,
            analysis.completed_at,ingestion.scheduled_cycle
        FROM option_analysis_runs AS analysis JOIN option_ingestion_runs AS ingestion USING(batch_id)
        WHERE analysis.underlying=%s AND ingestion.configuration_sha256=%s
          AND analysis.policy_sha256=%s AND ingestion.policy_sha256=%s
          AND analysis.status='COMPLETE' AND ingestion.status='COMPLETE'
          AND analysis.completed_at<=%s AND ingestion.completed_at<=%s
          AND analysis.observed_time<=%s AND analysis.created_at<=%s AND analysis.market_time<=%s
          AND ingestion.scheduled_cycle>=%s AND ingestion.scheduled_cycle<=%s
          AND (%s::uuid IS NULL OR analysis.matrix_id=%s::uuid)
        ORDER BY ingestion.scheduled_cycle DESC,analysis.completed_at DESC,analysis.matrix_id LIMIT 1""",
        (underlying, run.configuration_sha256, run.market_policy_sha256, run.market_policy_sha256,
         checkpoint_at, checkpoint_at, checkpoint_at, checkpoint_at, checkpoint_at,
         run.scheduled_cycle.replace(hour=0, minute=0, second=0, microsecond=0),
         min(checkpoint_at, run.scheduled_cycle.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)),
         matrix_id, matrix_id))
    matrix = cursor.fetchone()
    if matrix is None:
        return None, ()
    cursor.execute("""SELECT * FROM option_chain_snapshots WHERE batch_id=%s
        AND underlying=%s AND expiration_date=%s AND contract_type=%s
        AND market_data_time<=%s AND first_observed_at<=%s AND first_observed_at<=%s AND created_at<=%s
        ORDER BY contract_id,snapshot_id LIMIT 5001""",
        (matrix["batch_id"], underlying, expiration, contract_type, matrix["market_time"],
         matrix["observed_time"], checkpoint_at, checkpoint_at))
    rows = tuple(dict(row) for row in cursor.fetchall())
    if len(rows) > 5000:
        raise ValueError("surface checkpoint source exceeds 5000-row bound")
    return dict(matrix), rows


def verified_surface_baseline(*, observation, run, matrix, rows, snapshot_id, valuation_policy):
    from options.analytics.surface_followup import coherent_surface_peers
    from options.repositories.snapshots import _snapshot
    from options.surface_detection import CausalSurfacePoint, CausalSurfaceSource

    snapshots = tuple(_snapshot(row) for row in rows)
    target = next((row for row in snapshots if str(row.snapshot_id) == str(snapshot_id)), None)
    if target is None or matrix is None:
        raise ValueError("original surface snapshot or matrix is unavailable")
    peers = coherent_surface_peers(target, snapshots, valuation_policy)
    created = {row["snapshot_id"]: row["created_at"] for row in rows}
    source = CausalSurfaceSource(security_id=observation.security_id, underlyer=observation.underlyer,
        matrix_id=matrix["matrix_id"], scheduled_cycle=observation.scheduled_cycle, batch_id=matrix["batch_id"],
        configuration_sha256=run.configuration_sha256, market_policy_sha256=run.market_policy_sha256,
        valuation_policy_sha256=target.valuation_policy_sha256, contract_type=target.contract_type.value,
        expiration_date=target.expiration_date, expiration_cutoff=target.expiration_cutoff, spot=target.spot,
        market_time=max(row.market_data_time for row in peers), observed_at=max(row.first_observed_at for row in peers),
        recorded_at=max(created[row.snapshot_id] for row in peers), received_at=observation.decision_at,
        spot_market_time=target.spot_market_data_time, valuation_policy=valuation_policy, model_version=target.model_version,
        points=tuple(CausalSurfacePoint(contract_id=row.contract_id, contract_ticker=row.contract_ticker,
            snapshot_id=row.snapshot_id, snapshot_sha256=row.normalized_payload_sha256, strike=row.strike,
            local_iv=row.local_iv, market_time=row.market_data_time, observed_at=row.first_observed_at) for row in peers))
    if source.sha256 != observation.source_sha256:
        raise ValueError("original surface source hash cannot be reproduced")
    return peers


def validate_surface_sample_seed(sample, run, records):
    if (sample.get("version") != "o2_retained_observation_sample_v1"
            or sample.get("evidence_basis") != "ORIGINAL_HASH_VERIFIED_COMPLETED_RUNS"
            or not 1 <= len(sample.get("selected", ())) <= 5
            or sample.get("dataset_id") != run.dataset_id or sample.get("sample_run_id") != str(run.run_id)):
        raise ValueError("surface follow-up requires the original bounded sample artifact")
    expected = surface_observation_sample(((run, records),), limit=len(sample["selected"]))["selected"]
    expected = json.loads(json.dumps(expected, default=str, allow_nan=False))
    if len(expected) != len(sample["selected"]):
        raise ValueError("surface sample membership differs from original run")
    for retained, supplied in zip(expected, sample["selected"]):
        if any(supplied.get(key) != value for key, value in retained.items() if key != "followup_observations"):
            raise ValueError("surface sample identity, measurements or clocks were changed")
    return expected


def retained_surface_checkpoint_followup(sample_path):
    import hashlib
    from collections import Counter
    from datetime import timedelta
    from options.analytics.surface_followup import FOLLOWUP_POLICY, measure_surface_checkpoint
    from options.calendar import OptionExchangeCalendar
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository
    from options.repositories.snapshots import _snapshot

    if sample_path.stat().st_size > 1048576:
        raise ValueError("surface sample artifact exceeds one MiB")
    payload = sample_path.read_bytes()
    sample = json.loads(payload)
    if not isinstance(sample, dict) or not sample.get("selected"):
        raise ValueError("surface sample artifact requires selected observations")
    cutoff = datetime.now(timezone.utc)
    repository = OptionAlertEvaluationRepository()
    cycle = datetime.fromisoformat(sample["selected"][0]["scheduled_cycle"])
    retained = repository.completed_run(dataset_id=sample["dataset_id"], scheduled_cycle=cycle, as_of=cutoff)
    if retained is None:
        raise ValueError("original completed surface run is unavailable")
    run, records = retained
    selected = validate_surface_sample_seed(sample, run, records)
    by_id = {str(record.evaluation_id): record for record in records}
    valuation_policy = load_option_runtime_configuration().valuation_policy
    closing = OptionExchangeCalendar().session_close(date.fromisoformat(sample["session_date"]))
    reviewed = []
    with repository._cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '5s'")
        for member in selected:
            observation = by_id[member["evaluation_id"]].observation
            matrix, rows = read_surface_checkpoint_rows(cursor, run=run, underlying=observation.underlyer,
                expiration=observation.expiration_date, contract_type=observation.contract_type,
                checkpoint_at=observation.decision_at, matrix_id=observation.matrix_id)
            baseline = verified_surface_baseline(observation=observation, run=run, matrix=matrix, rows=rows,
                snapshot_id=member["snapshot_id"], valuation_policy=valuation_policy)
            checkpoints = []
            for minutes in FOLLOWUP_POLICY["offset_minutes"]:
                checkpoint = run.selected_at + timedelta(minutes=minutes)
                measurement = dict(offset_minutes=minutes, checkpoint_at=checkpoint,
                    after_original_validity=checkpoint >= observation.valid_until,
                    cohort_fit=None, fixed_peer_fit=None, absolute_residual_change_iv_points=None)
                if checkpoint > cutoff:
                    measurement.update(status="PENDING", reason="CHECKPOINT_NOT_DUE")
                elif checkpoint >= closing:
                    measurement.update(status="UNAVAILABLE", reason="CHECKPOINT_OUTSIDE_SOURCE_SESSION")
                else:
                    matrix, rows = read_surface_checkpoint_rows(cursor, run=run, underlying=observation.underlyer,
                        expiration=observation.expiration_date, contract_type=observation.contract_type,
                        checkpoint_at=checkpoint)
                    if matrix is None:
                        measurement.update(status="UNAVAILABLE", reason="NO_COMPLETED_MATRIX_AT_CHECKPOINT")
                    else:
                        measurement.update(measure_surface_checkpoint(baseline_peers=baseline,
                            contract_id=member["contract_id"], snapshots=tuple(_snapshot(row) for row in rows),
                            checkpoint_at=checkpoint, valuation_policy=valuation_policy), matrix=matrix)
                checkpoints.append(measurement)
            reviewed.append(dict(underlying=member["underlying"], contract_id=member["contract_id"],
                contract_type=member["contract_type"], strike=member["strike"], expiration=member["expiration"],
                original_evaluation_id=member["evaluation_id"], original_evaluation_sha256=member["evaluation_sha256"],
                original_source_sha256=observation.source_sha256, baseline_source_hash_verified=True,
                original_valid_until=observation.valid_until, original_residual_iv_points=member["residual_iv_points"],
                original_robust_z=member["robust_z"], checkpoints=checkpoints))
    return dict(version="o2_retained_checkpoint_report_v1", as_of=cutoff, dataset_id=run.dataset_id,
        sample_sha256=hashlib.sha256(payload).hexdigest(), policy=FOLLOWUP_POLICY,
        policy_sha256=hashlib.sha256(json.dumps(FOLLOWUP_POLICY, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest(),
        status="POST_HOC_RETAINED_CHECKPOINT_REVIEW", selected=reviewed,
        checkpoint_status_counts=dict(Counter(row["status"] for member in reviewed for row in member["checkpoints"])),
        checkpoint_reason_counts=dict(Counter(row["reason"] for member in reviewed for row in member["checkpoints"] if row["reason"])),
        limitations=["FIXED_CHECKPOINTS_USE_ONLY_RETAINED_RECEIPTS_AVAILABLE_BY_THAT_TIME",
            "LATEST_COMPLETED_MATRIX_IS_PER_UNDERLYING_NOT_A_NEW_ALERT_PUBLICATION",
            "TARGET_COHORT_MAY_DIFFER_FROM_PRODUCTION_LARGEST_COHORT",
            "COHORT_ONLY_RESIDUAL_MOVEMENT_IS_NOT_FIXED_PEER_CONVERGENCE",
            "ORIGINAL_SIGNAL_EXPIRY_IS_NOT_EXTENDED", "MODEL_MARKS_ARE_NOT_FILLS_OR_RETURNS",
            "NO_PROVIDER_FETCH_NO_WORKER_OR_POLICY_ACTIVATION"], source_writes=0,
        publication_permission=False, execution_permission=False)


def technical_source_diagnostic(runtime):
    from contextlib import contextmanager
    from time import perf_counter
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    report = dict(as_of=datetime.now(timezone.utc), mode="READ_ONLY_QUERY_DIAGNOSTIC", queries=[])
    def plan_nodes(plan):
        result = [{key: plan[key] for key in ("Node Type", "Relation Name", "Index Name", "Plan Rows", "Total Cost") if key in plan}]
        for child in plan.get("Plans", []):
            result.extend(plan_nodes(child))
        return result
    class DiagnosticCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def execute(self, query, parameters=None):
            if not query.lstrip().startswith("SELECT"):
                return self.cursor.execute(query, parameters)
            measured = dict(number=len(report["queries"]) + 1)
            report["queries"].append(measured)
            if "equity_bar_revisions" in query:
                measured["bar_ids"] = len(parameters[0])
                self.cursor.execute("EXPLAIN (FORMAT JSON) " + query, parameters)
                plan = self.cursor.fetchone()["QUERY PLAN"][0]["Plan"]
                measured["plan"] = plan_nodes(plan)
            started = perf_counter()
            try:
                return self.cursor.execute(query, parameters)
            finally:
                measured["seconds"] = round(perf_counter() - started, 4)

        def fetchall(self):
            rows = self.cursor.fetchall()
            report["queries"][-1]["rows"] = len(rows)
            return rows

    class DiagnosticRepository(OptionStockBehaviorAssessmentRepository):
        @contextmanager
        def _cursor(self):
            with super()._cursor() as cursor:
                yield DiagnosticCursor(cursor)

        @staticmethod
        def _technical_bars(cursor, ids, as_of):
            started = perf_counter()
            bars = OptionStockBehaviorAssessmentRepository._technical_bars(cursor, ids, as_of)
            report["guarded_bar_seconds"] = round(perf_counter() - started, 4)
            cursor.execute(
                "SELECT * FROM equity_bar_revisions WHERE bar_revision_id=ANY(%s::uuid[]) "
                "AND NOT adjusted AND is_final AND session_scope='RTH' AND availability_mode='LIVE_OBSERVED' "
                "AND quality_codes=ARRAY[]::text[] AND system_observed_at<=%s AND created_at<=%s ORDER BY bar_start",
                (ids, as_of, as_of))
            original = cursor.fetchall()
            report["same_snapshot_exact_row_parity"] = {row["bar_revision_id"]: row for row in bars} == {
                row["bar_revision_id"]: dict(row) for row in original}
            if not report["same_snapshot_exact_row_parity"]:
                raise ValueError("guarded bar lookup differs from original query")
            return bars

    try:
        sources = DiagnosticRepository().detector_technical_sources(underlyers=runtime.settings.underlyers,
            market_cutoff=report["as_of"], as_of=report["as_of"])
        report.update(status="READ_COMPLETE", counts={name: len(rows) for name, rows in sources.items()})
    except Exception as error:
        report.update(status="READ_FAILED", error=type(error).__name__)
    bar_queries = [row for row in report["queries"] if "bar_ids" in row]
    if len(bar_queries) > 2:
        report["bar_queries"] = dict(attempt_count=len(bar_queries) - 1,
            first_attempt=bar_queries[0], original_comparison=bar_queries[-1])
        report["queries"] = [row for row in report["queries"] if "bar_ids" not in row]
    return report


def review_detector_launch(path, runtime):
    import psutil
    from options.detector_launch import decode_detector_forward_launch, validate_detector_forward_launch
    from options.repositories.alert_review_sources import configured_detector_launch

    previous = configured_detector_launch()
    replacement = decode_detector_forward_launch(path.read_text(encoding="utf-8"))
    validate_detector_forward_launch(replacement, configuration=runtime, backend_dir=BACKEND_DIR)
    before, after = previous.model_dump(), replacement.model_dump()
    processes, wrappers = [], []
    for process in psutil.process_iter(["pid", "ppid", "name", "cmdline", "exe", "create_time"]):
        info = process.info
        arguments = info["cmdline"] or []
        text = " ".join(arguments)
        if info["name"] == "powershell.exe" and "changedRuntime" in text:
            wrappers.append(info["pid"])
        if info["name"] != "python.exe" or info["pid"] == __import__("os").getpid():
            continue
        if str(BACKEND_DIR).lower() not in (text + (info["exe"] or "")).lower() and not ("uvicorn" in arguments and "8001" in arguments):
            continue
        script = next((Path(value).name for value in arguments if value.endswith(".py")), "uvicorn" if "uvicorn" in arguments else "python")
        processes.append(dict(pid=info["pid"], parent=info["ppid"], created=info["create_time"], script=script))
    return dict(checked_at=datetime.now(timezone.utc), dataset=replacement.dataset_id, sha256=replacement.sha256,
        changed_fields=[key for key in after if after[key] != before.get(key)],
        changed_runtime=[name for name, value in replacement.runtime_sources if dict(previous.runtime_sources).get(name) != value],
        comparison_jobs=wrappers, processes=processes)


def parse_args():
    from uuid import UUID
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Optional JSON report path; omitted means no file write.")
    parser.add_argument("--surface-observations", type=date.fromisoformat, metavar="YYYY-MM-DD",
        help="Sample original hash-verified O2 observations for one session; no collection or publication.")
    parser.add_argument("--surface-followup", type=Path,
        help="Measure fixed checkpoints for an existing O2 sample artifact using retained snapshots only.")
    parser.add_argument("--sample-limit", type=int, choices=range(1, 6), default=5, metavar="1..5")
    parser.add_argument("--setup-publications", action="store_true", help="Inspect bounded retained setup publication evidence only.")
    parser.add_argument("--dual-origin-readiness", action="store_true", help="Inspect current O1 activity and S1 setup inventory without writes or detector activation.")
    parser.add_argument("--detector-sources", action="store_true", help="Read and bind the latest complete retained detector cycle; diagnostic only, no writes.")
    parser.add_argument("--detector-run-id", type=UUID, help="With --detector-sources, inspect one recorded run at its original selection cutoff; never replay or rewrite it.")
    parser.add_argument("--compact", action="store_true", help="With --detector-sources, emit only the O1 confirmation, package dry-run and recorded rejection summary.")
    parser.add_argument("--technical-replay-preflight", type=date.fromisoformat, metavar="YYYY-MM-DD", help="Read-only strict-as-of technical replay gate for one session; optional new audit file only.")
    parser.add_argument("--detector-schema", action="store_true", help="Read-only migration-053/054/055 preflight/postflight; no output file or source scan.")
    parser.add_argument("--o1-indicator-review", nargs=2, type=date.fromisoformat, metavar=("START", "END"),
        help="Summarize retained prospective O1 shadow indicators for an inclusive period of at most 32 dates.")
    parser.add_argument("--detector-dataset-id", help="Limit an indicator review to one exact detector dataset.")
    parser.add_argument("--stock-setup-source-review", action="store_true",
        help="Summarize the pinned S1/S2 stock setup ledger without changing or replaying it.")
    parser.add_argument("--stock-setup-indicator-review", nargs=2, type=date.fromisoformat, metavar=("START", "END"),
        help="Summarize retained prospective S1/S2 mixed indicators for an inclusive period of at most 32 dates.")
    parser.add_argument("--technical-source-diagnostic", action="store_true", help="Read-only source query timings and bar lookup plan under the unchanged five-second limit.")
    parser.add_argument("--review-detector-launch", type=Path, help="Read-only comparison of a prepared replacement manifest and current process identities.")
    parser.add_argument("--detector-strategy-policy-file", choices=(
        "options/policies/strategy_technical_forward_v1.json",
        "options/policies/strategy_technical_forward_v2.json",
    ), help="With --review-detector-launch, validate against this immutable replacement strategy policy.")
    parser.add_argument("--publication-limit", type=int, default=12, choices=range(1, 49), metavar="1..48")
    parser.add_argument("--original-setup-ledger", type=Path, help="Compare exact retained publication keys against this SQLite ledger in query-only mode.")
    args = parser.parse_args()
    if args.surface_followup and args.surface_observations:
        parser.error("--surface-followup cannot select a new sample")
    if (args.surface_observations or args.surface_followup) and (args.review_detector_launch or args.technical_source_diagnostic
            or args.detector_sources or args.detector_run_id or args.detector_schema or args.technical_replay_preflight
            or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("surface review cannot be combined with other modes")
    if args.sample_limit != 5 and not args.surface_observations:
        parser.error("--sample-limit requires --surface-observations")
    if args.review_detector_launch and (args.output or args.technical_source_diagnostic or args.detector_sources or args.detector_run_id
            or args.detector_schema or args.technical_replay_preflight or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("--review-detector-launch cannot be combined with other modes")
    if args.detector_strategy_policy_file and not args.review_detector_launch:
        parser.error("--detector-strategy-policy-file requires --review-detector-launch")
    if args.technical_source_diagnostic and (args.output or args.detector_sources or args.detector_run_id or args.detector_schema
            or args.technical_replay_preflight or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("--technical-source-diagnostic cannot be combined with other modes or output writes")
    if args.detector_run_id and not args.detector_sources:
        parser.error("--detector-run-id requires --detector-sources")
    if args.compact and not args.detector_sources:
        parser.error("--compact requires --detector-sources")
    if args.technical_replay_preflight and (args.detector_sources or args.detector_schema or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("--technical-replay-preflight cannot be combined with other reports")
    if args.detector_sources and ((args.output and not args.compact) or args.detector_schema or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("--detector-sources cannot be combined with other reports or output writes")
    if args.detector_schema and (args.output or args.dual_origin_readiness or args.setup_publications or args.original_setup_ledger):
        parser.error("--detector-schema cannot be combined with source reports or output writes")
    if args.dual_origin_readiness and (args.output or args.setup_publications or args.original_setup_ledger):
        parser.error("--dual-origin-readiness is read-only and cannot be combined with output or setup options")
    if args.original_setup_ledger and not args.setup_publications:
        parser.error("--original-setup-ledger requires --setup-publications")
    if args.o1_indicator_review and any((args.surface_observations, args.surface_followup, args.review_detector_launch,
            args.technical_source_diagnostic, args.detector_sources, args.detector_run_id, args.detector_schema,
            args.technical_replay_preflight, args.dual_origin_readiness, args.setup_publications,
            args.original_setup_ledger, args.compact)):
        parser.error("--o1-indicator-review cannot be combined with another report mode")
    if args.stock_setup_source_review and any((args.surface_observations, args.surface_followup, args.review_detector_launch,
            args.technical_source_diagnostic, args.detector_sources, args.detector_run_id, args.detector_schema,
            args.technical_replay_preflight, args.dual_origin_readiness, args.setup_publications,
            args.original_setup_ledger, args.compact, args.o1_indicator_review, args.detector_dataset_id, args.output)):
        parser.error("--stock-setup-source-review is read-only and cannot be combined with another report mode or output")
    if args.stock_setup_indicator_review and any((args.surface_observations, args.surface_followup, args.review_detector_launch,
            args.technical_source_diagnostic, args.detector_sources, args.detector_run_id, args.detector_schema,
            args.technical_replay_preflight, args.dual_origin_readiness, args.setup_publications,
            args.original_setup_ledger, args.compact, args.o1_indicator_review, args.stock_setup_source_review)):
        parser.error("--stock-setup-indicator-review cannot be combined with another report mode")
    if args.detector_dataset_id and not (args.o1_indicator_review or args.stock_setup_indicator_review):
        parser.error("--detector-dataset-id requires an indicator review")
    return args


def main() -> int:
    args = parse_args()
    if args.stock_setup_indicator_review:
        from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

        start_date, end_date = args.stock_setup_indicator_review
        cutoff = datetime.now(timezone.utc)
        records = OptionAlertEvaluationRepository().stock_setup_indicator_observations(
            start_date=start_date, end_date=end_date, as_of=cutoff, dataset_id=args.detector_dataset_id)
        report = summarize_stock_setup_indicator_observations(records,
            start_date=start_date, end_date=end_date, as_of=cutoff)
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str, allow_nan=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as destination:
                destination.write(rendered + "\n")
        print(rendered)
        return 0
    if args.stock_setup_source_review:
        from options.repositories.alert_review_sources import configured_detector_launch

        launch = configured_detector_launch()
        report = summarize_stock_setup_source((BACKEND_DIR / launch.stock_ledger),
            as_of=datetime.now(timezone.utc))
        print(json.dumps(report, indent=2, sort_keys=True, default=str, allow_nan=False))
        return 0
    if args.o1_indicator_review:
        from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

        start_date, end_date = args.o1_indicator_review
        cutoff = datetime.now(timezone.utc)
        records = OptionAlertEvaluationRepository().o1_indicator_observations(
            start_date=start_date, end_date=end_date, as_of=cutoff, dataset_id=args.detector_dataset_id)
        report = summarize_o1_indicator_observations(records,
            start_date=start_date, end_date=end_date, as_of=cutoff)
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str, allow_nan=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as destination:
                destination.write(rendered + "\n")
        print(rendered)
        return 0
    if args.surface_followup:
        report = retained_surface_checkpoint_followup(args.surface_followup)
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str, allow_nan=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as destination:
                destination.write(rendered + "\n")
        summary = {key: value for key, value in report.items() if key != "selected"}
        summary["selected"] = [dict(underlying=member["underlying"], contract_id=member["contract_id"],
            baseline_source_hash_verified=member["baseline_source_hash_verified"],
            checkpoints=[dict(offset_minutes=row["offset_minutes"], status=row["status"], reason=row["reason"],
                residual_iv_points=(row["cohort_fit"] or {}).get("residual_iv_points"),
                fixed_peer_residual_iv_points=(row["fixed_peer_fit"] or {}).get("residual_iv_points"),
                available_original_peer_count=row.get("available_original_peer_count"),
                absolute_residual_change_iv_points=row["absolute_residual_change_iv_points"])
                for row in member["checkpoints"]]) for member in report["selected"]]
        print(json.dumps(summary, indent=2, sort_keys=True, default=str, allow_nan=False))
        return 0
    if args.surface_observations:
        report = retained_surface_observation_sample(args.surface_observations, limit=args.sample_limit)
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str, allow_nan=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as destination:
                destination.write(rendered + "\n")
        print(rendered)
        return 0
    if args.detector_schema:
        import hashlib
        from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

        report = OptionAlertEvaluationRepository().schema_readiness()
        report["migration_sha256"] = hashlib.sha256((BACKEND_DIR / "migrations/053_option_detector_evaluations.sql").read_bytes()).hexdigest()
        report["o1_migration_sha256"] = hashlib.sha256((BACKEND_DIR / "migrations/054_option_o1_indicator_observations.sql").read_bytes()).hexdigest()
        report["stock_setup_migration_sha256"] = hashlib.sha256((BACKEND_DIR / "migrations/055_option_stock_setup_indicator_observations.sql").read_bytes()).hexdigest()
        print(json.dumps(report, sort_keys=True, indent=2, default=str))
        return 0
    runtime = load_option_runtime_configuration(
        dict(os.environ, OPTION_STRATEGY_POLICY_FILE=args.detector_strategy_policy_file),
        BACKEND_DIR,
    ) if args.detector_strategy_policy_file else load_option_runtime_configuration()
    if args.review_detector_launch:
        print(json.dumps(review_detector_launch(args.review_detector_launch, runtime), indent=2, default=str))
        return 0
    if args.technical_source_diagnostic:
        report = technical_source_diagnostic(runtime)
        print(json.dumps(report, indent=2, default=str))
        return 0 if report["status"] == "READ_COMPLETE" else 1
    if args.technical_replay_preflight:
        import hashlib
        from options.alert_plans import TECHNICAL_EXIT_POLICY
        from options.dual_origin import TECHNICAL_QUALIFICATION_POLICY
        from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

        cutoff = datetime.now(timezone.utc)
        inputs = OptionStockBehaviorAssessmentRepository().technical_replay_preflight(configuration=runtime,
            session_date=args.technical_replay_preflight, as_of=cutoff)
        expected = len(runtime.settings.underlyers)
        complete = [row for row in inputs["rows"] if row["matrix_count"] == expected and row["covered"] == expected]
        report = dict(version="option_technical_replay_preflight_v1", status="BLOCKED_NOT_REPLAYED",
            replay_dataset_id=f"options-technical-strict-{args.technical_replay_preflight}-v1",
            session_date=args.technical_replay_preflight.isoformat(), as_of=cutoff.isoformat(),
            evidence_mode="STRICT_HISTORICAL_AS_OF", underlyers=runtime.settings.underlyers,
            configuration_sha256=runtime.configuration_sha256, management_policy_sha256=TECHNICAL_EXIT_POLICY.sha256,
            qualification_policy_sha256=TECHNICAL_QUALIFICATION_POLICY.sha256,
            complete_cycles=len({row["scheduled_cycle"] for row in complete}),
            incomplete_cycles=len({row["scheduled_cycle"] for row in inputs["rows"] if row not in complete}),
            matrices=len(complete), directional_candidates=sum(row["candidate_count"] for row in complete),
            timely_directional_candidates=sum(row["timely_candidates"] for row in complete),
            matrices_with_stock_behavior=sum(row["has_stock_behavior"] for row in complete),
            matrices_with_potential_technical_evidence=sum(row["has_potential_technical_evidence"] for row in complete),
            publications=inputs["publications"], rows=complete,
            remaining_gates=["EXACT_TECHNICAL_SOURCE_POLICY_AND_RAW_PRICE_BINDING", "S1_S2_HISTORICAL_DIRECT_RECEIPTS",
                "PRODUCTION_PACKAGE_SOURCE_ASSEMBLY", "SEPARATE_REPLAY_EVIDENCE_ADMISSION"],
            replay_rows_written=0, forward_activated=False, execution_permission=False)
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str, allow_nan=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as destination:
                destination.write(rendered + "\n")
        print(json.dumps({**{key: value for key, value in report.items() if key != "rows"},
            "report_sha256": hashlib.sha256(rendered.encode("utf-8")).hexdigest()}, indent=2, sort_keys=True, default=str))
        return 0
    if args.detector_sources:
        from collections import Counter, defaultdict
        from equity.repositories import EquityReferenceRepository
        from options.detector_collection import RetainedDetectorSourceReader
        from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository
        from options.surface_detection import assess_surface_first

        cutoff = datetime.now(timezone.utc)
        repository = OptionStockBehaviorAssessmentRepository()
        cycles = defaultdict(dict)
        recorded = None
        if args.detector_run_id:
            from options.repositories.alert_evaluations import OptionAlertEvaluationRepository
            from options.repositories.alert_review_sources import OptionAlertReviewSourceRepository, configured_detector_launch

            evaluation_repository = OptionAlertEvaluationRepository()
            datasets = OptionAlertReviewSourceRepository().dataset_index(as_of=cutoff)["datasets"]
            recorded = next((run for dataset in datasets for run in evaluation_repository.completed_runs(
                dataset_id=dataset, as_of=cutoff) if run.run_id == args.detector_run_id), None)
            if recorded is None or recorded.configuration_sha256 != runtime.configuration_sha256:
                raise ValueError("recorded detector run unavailable in the current configuration/dataset")
            cutoff = recorded.selected_at
            cycles[recorded.scheduled_cycle] = dict(recorded.source_matrices)
        else:
            for row in repository.completed_matrices(configuration=runtime, as_of=cutoff):
                cycles[row["scheduled_cycle"]][row["underlying"]] = row["matrix_id"]
        complete = [cycle for cycle, matrices in cycles.items() if set(matrices) == set(runtime.settings.underlyers)]
        report = dict(status="NO_COMPLETE_RETAINED_CYCLE", as_of=cutoff, publication_permission=False,
            execution_permission=False, source_policy_approved=False)
        if complete:
            cycle = max(complete)
            sources = RetainedDetectorSourceReader(repository, EquityReferenceRepository(), EquityEvidenceRepository(), clock=lambda: cutoff).read(
                configuration=runtime, scheduled_cycle=cycle, completed_matrices=cycles[cycle], as_of=cutoff)
            packages = repository.detector_package_sources(configuration=runtime,
                candidate_ids=tuple(row["candidate_id"] for row in sources["candidates"]), as_of=cutoff)
            observations = [assess_surface_first(**values) for values in sources["surface_inputs"]]
            report.update(status="DIAGNOSTIC_NOT_APPROVED", scheduled_cycle=cycle, received_at=sources["received_at"],
                matrices=len(sources["matrices"]), retained_candidates=len(sources["candidates"]),
                package_source_counts={key: len(values) for key, values in packages.items()},
                binding_diagnostic=package_binding_diagnostic(sources, packages, valuation_policy=runtime.valuation_policy),
                o1_confirmation=o1_confirmation_diagnostic(sources, repository, decision_at=cutoff),
                o1_package_dry_run=o1_package_dry_run(
                    runtime, configured_detector_launch(), recorded, sources, packages,
                ) if recorded else None,
                recorded_rejections=dict(recorded.rejections) if recorded else None,
                inspection_basis="CURRENT_RETAINED_ROWS_AT_ORIGINAL_CUTOFF_NOT_A_REPLAY" if recorded else "CURRENT_RETAINED_ROWS",
                activity_dispositions=dict(Counter(row["finding"].disposition for row in sources["activity"])),
                activity_reasons=dict(Counter(reason for row in sources["activity"] for reason in row["finding"].reasons)),
                surface_dispositions=dict(Counter(row.finding_disposition for row in observations)),
                source_exclusions=sources["rejections"],
                pending=["NO_PERSISTENCE_OR_PUBLICATION_BY_DIAGNOSTIC"] if recorded else
                    ["PLAN_QUALIFICATION_NOT_RUN_BY_DIAGNOSTIC", "NO_SELECTION_OR_PUBLICATION_BY_DIAGNOSTIC"])
        if args.compact:
            report = {key: report.get(key) for key in (
                "status", "as_of", "scheduled_cycle", "o1_confirmation",
                "o1_package_dry_run", "recorded_rejections",
                "publication_permission", "execution_permission",
            )}
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as destination:
                destination.write(rendered + "\n")
        print(rendered)
        return 0
    if args.dual_origin_readiness:
        from equity.stock_alert_results import read_setup_publication_inventory
        from equity.behavior_setup import summarize_setup_publications
        from options.repositories.daily_facts import OptionDailyFactRepository

        cutoff = datetime.now(timezone.utc)
        activity = OptionDailyFactRepository().participation_inventory(configuration=runtime, as_of=cutoff)
        rows, received_at = read_setup_publication_inventory(limit=args.publication_limit)
        setup = summarize_setup_publications(rows, runtime.settings.underlyers, received_at)
        report = dict(status="DIAGNOSTIC_NOT_APPROVED", as_of=cutoff, received_at=received_at,
            universe=runtime.settings.underlyers, activity_matrices=activity,
            setup=dict(status=setup["status"], counts=setup["counts"],
                distinct_hourly_acceptance_episodes=setup["distinct_hourly_acceptance_episodes"],
                missing_hourly_acceptance_tickers=setup["missing_hourly_acceptance_tickers"],
                excluded_record_count=len(setup["excluded_records"])),
            unverified=["OPTION_STOCK_SECURITY_AND_PRICE_BASIS_BINDING", "FRESH_CROSS_MARKET_RECEIPT",
                "TRUSTED_S1_SOURCE_POLICY_APPROVAL", "DUAL_ORIGIN_WRITER_INTEGRATION"],
            source_policy_approved=False, publication_permission=False, execution_permission=False)
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return 0
    if args.setup_publications:
        from equity.stock_alert_results import read_setup_publication_inventory
        from equity.behavior_setup import summarize_setup_publications

        rows, received_at = read_setup_publication_inventory(limit=args.publication_limit)
        report = summarize_setup_publications(rows, runtime.settings.underlyers, received_at)
        if args.original_setup_ledger and rows:
            from equity.stock_alert_results import read_original_setup_publications
            from equity.behavior_setup import compare_original_setup_publication

            original_policy, originals = read_original_setup_publications(
                args.original_setup_ledger, [row["record_id"] for row in rows],
            )
            report["original_source_comparison"] = [compare_original_setup_publication(
                row, originals.get(row["record_id"]), original_policy,
            ) for row in rows]
        rendered = json.dumps(report, indent=2, sort_keys=True)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        return 0
    cutoff = datetime.now(timezone.utc)
    watermark = DecisionWatermark(cutoff, cutoff)
    tickers = runtime.settings.underlyers
    intervals = ("1d", "1h", "30m")
    reads = EquityEvidenceRepository().read_behavior_feature_sources(
        tickers, watermark, intervals=intervals,
        maximum_source_bars=max(source_tail_bars_by_interval().values()),
        source_bars_by_interval=source_tail_bars_by_interval(),
    )
    selected_bars = [bar for read in reads for bar in read.bars]
    coverage_rows, actions_by_coverage = ((), {})
    if selected_bars:
        coverage_rows, actions_by_coverage = EquityCorporateActionRepository().read_behavior_split_coverage(
            tickers, watermark,
            window_start=min(bar.session_date for bar in selected_bars),
            window_end=max(bar.session_date for bar in selected_bars),
        )
    received_at = datetime.now(timezone.utc)
    report = summarize_coverage(
        reads, tickers, intervals, watermark, received_at,
        coverage_rows, actions_by_coverage,
    )
    bar_repository = EquityBarRepository()
    adjusted_daily_reads = bar_repository.read_behavior_adjusted_daily(
        tickers, watermark, limit_per_ticker=273,
    )
    raw_grouped_daily_reads = bar_repository.read_behavior_grouped_daily(
        tickers, watermark, adjusted=False, limit_per_ticker=273,
    )
    adjusted_by_interval = {
        "1d": {row.ticker: row.bars for row in adjusted_daily_reads},
        **{interval: bar_repository.list_final_for_tickers_as_of(
            tickers, interval, watermark, limit_per_ticker=limit, adjusted=True,
        ) for interval, limit in source_tail_bars_by_interval().items() if interval != "1d"},
    }
    report["adjusted_bar_inventory"] = summarize_adjusted_bar_inventory(
        reads, adjusted_by_interval, tickers,
        {row.ticker: row.bar_created_ats for row in adjusted_daily_reads}, received_at,
    )
    linked_actions = [action for actions in actions_by_coverage.values() for action in actions]
    report["linked_split_action_count"] = len(linked_actions)
    report["linked_split_action_tickers"] = sorted({action["ticker"] for action in linked_actions})
    report["hybrid_source_policy_sha256"] = OPTIONS_SWING_HYBRID_SOURCE_POLICY.sha256
    report["hourly_split_continuity"] = summarize_hourly_split_continuity(
        reads, raw_grouped_daily_reads, adjusted_daily_reads,
        coverage_rows, actions_by_coverage,
        tickers, received_at,
    )
    report["raw_grouped_daily_tickers"] = sorted(row.ticker for row in raw_grouped_daily_reads)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "STOCK_BEHAVIOR_COVERAGE "
        f"received_at={report['received_at']} "
        f"source_policy={BEHAVIOR_SOURCE_SELECTION_POLICY.sha256} "
        f"hybrid_policy={OPTIONS_SWING_HYBRID_SOURCE_POLICY.sha256} "
        f"expected={report['expected_rows']} observed={report['observed_rows']} "
        f"math={json.dumps(report['mathematical_status_counts'], sort_keys=True)} "
        f"contract={json.dumps(report['contract_status_counts'], sort_keys=True)}"
    )
    print("BLOCKERS " + json.dumps(report["blocker_counts"], sort_keys=True))
    print(
        "SPLIT_COVERAGE "
        f"rows={report['split_coverage_rows']} "
        f"response_bound={report['response_bound_split_coverage_rows']} "
        f"tickers={len(report['response_bound_split_coverage_tickers'])} "
        f"window={report['split_coverage_window_start']}..{report['split_coverage_window_end']} "
        f"bar_modes={json.dumps(report['feature_bar_mode_counts'], sort_keys=True)} "
        f"coverage_modes={json.dumps(report['response_bound_split_coverage_mode_counts'], sort_keys=True)}"
    )
    print(
        "INTERVALS "
        f"contract={json.dumps(report['contract_status_counts_by_interval'], sort_keys=True)} "
        f"windows={json.dumps(report['source_windows_by_interval'], sort_keys=True)} "
        f"blockers={json.dumps(report['blocker_counts_by_interval'], sort_keys=True)}"
    )
    print(
        "ADJUSTED_INVENTORY "
        f"bars={json.dumps(report['adjusted_bar_inventory']['bar_counts_by_interval'], sort_keys=True)} "
        f"math_ready_names={json.dumps(report['adjusted_bar_inventory']['mathematical_history_ready_counts_by_interval'], sort_keys=True)} "
        f"derived_candidates={json.dumps(report['adjusted_bar_inventory']['derived_evidence_candidate_counts_by_interval'], sort_keys=True)} "
        f"ready_tickers={json.dumps(report['adjusted_bar_inventory']['mathematical_history_ready_tickers_by_interval'], sort_keys=True)} "
        f"unready_tickers={json.dumps(report['adjusted_bar_inventory']['mathematical_history_unready_tickers_by_interval'], sort_keys=True)} "
        f"prospective_ready={report['adjusted_bar_inventory']['prospective_behavior_contract_ready']} "
        f"blockers={json.dumps(report['adjusted_bar_inventory']['blocker_counts'], sort_keys=True)} "
        f"linked_split_actions={report['linked_split_action_count']} "
        f"split_tickers={json.dumps(report['linked_split_action_tickers'])}"
    )
    print(
        "HOURLY_SPLIT_CONTINUITY "
        f"status={json.dumps(report['hourly_split_continuity']['status_counts'], sort_keys=True)} "
        f"candidates={json.dumps(report['hourly_split_continuity']['candidate_tickers'])} "
        f"unavailable={json.dumps(report['hourly_split_continuity']['unavailable_tickers'])} "
        f"blockers={json.dumps(report['hourly_split_continuity']['blocker_counts'], sort_keys=True)}"
    )
    print(f"RAW_GROUPED_DAILY tickers={json.dumps(report['raw_grouped_daily_tickers'])}")
    print(
        "HOURLY_CONTINUITY_EXCEPTIONS " + json.dumps([
            row for row in report["hourly_split_continuity"]["rows"]
            if row["status"] == "UNAVAILABLE"
        ], sort_keys=True)
    )
    if args.output is not None:
        print(f"REPORT_WRITTEN path={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())