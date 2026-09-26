"""Read-only v1 behavior selection and descriptive outcome review."""
from collections import Counter, defaultdict
from datetime import datetime
from math import isfinite
from statistics import fmean

from equity.behavior import DEFINITION_V1_SHA256, OPTIONS_SWING_PROFILE
from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY, StockBehaviorGateAssessment
from options.outcome_contracts import OptionOutcomeAvailabilityPolicy
from options.outcomes import measurement_checkpoints


REVIEW_VERSION = "option_behavior_review_v1"
TARGETS = {(row.strategy_name, row.structure_type) for row in STOCK_BEHAVIOR_GATE_POLICY.targets}
MEASUREMENTS = ("60MIN", "CLOSE", "NEXT_OPEN")
REQUIRED_GATES = {"PROFILE_DATA_READY", "DIRECTIONAL_THESIS_STRUCTURE", "UNDERLYING_LIQUIDITY_EVIDENCE",
    *(f"TREND_SLOPE_{interval}" for interval in STOCK_BEHAVIOR_GATE_POLICY.required_trend_intervals)}
PARTICIPATION_BUCKETS = (
    ("QUIET_LT_0_75X", None, .75),
    ("NORMAL_0_75_TO_1_25X", .75, 1.25),
    ("ELEVATED_1_25_TO_2X", 1.25, 2.),
    ("SURGE_GE_2X", 2., None),
)


def _complete_package(row, applicable, as_of):
    expected_legs = {"LONG_CALL": 1, "LONG_PUT": 1, "CASH_SECURED_PUT": 1, "CALL_DEBIT_VERTICAL": 2,
        "PUT_DEBIT_VERTICAL": 2, "CALL_CREDIT_VERTICAL": 2, "PUT_CREDIT_VERTICAL": 2,
        "IRON_CONDOR": 4, "CALL_BUTTERFLY": 3, "PUT_BUTTERFLY": 3}
    try:
        legs = row.get("legs") or ()
        return bool(legs) and len(legs) == expected_legs.get(row["structure_type"]) and all(
            all(isfinite(float(leg[field])) and float(leg[field]) > 0 for field in ("model_mark", "ratio", "multiplier"))
            and (leg["source_market_time"] if isinstance(leg["source_market_time"], datetime)
                 else datetime.fromisoformat(leg["source_market_time"])) <= as_of for leg in legs
        )
    except (ValueError, TypeError, KeyError, OverflowError):
        return False


def annotate_candidates(candidates, assessments, as_of):
    rows = []
    for original in candidates:
        row = dict(original)
        reasons, disposition, decision, recorded, metrics, payload_hash = [], "UNAVAILABLE", None, None, {}, None
        applicable = (row["strategy_name"], row["structure_type"]) in TARGETS
        evidence = assessments.get(str(row["candidate_id"]))
        if not applicable:
            disposition, reasons = "NOT_APPLICABLE", ["DETECTOR_PROFILE_NOT_REGISTERED"]
        elif evidence is None:
            reasons = ["STOCK_ASSESSMENT_NOT_RECORDED"]
        else:
            try:
                assessment = StockBehaviorGateAssessment.model_validate_json(evidence["payload_text"])
                identity = {
                    "candidate_id": row["candidate_id"], "candidate_identity_sha256": row["candidate_identity"],
                    "matrix_id": row["matrix_id"], "underlyer": row["underlying"],
                    "strategy_name": row["strategy_name"], "structure_type": row["structure_type"],
                    "option_strategy_version": row["strategy_version"], "option_strategy_policy_sha256": row["policy_sha256"],
                    "option_market_time": row["market_data_time"], "option_observed_at": row["observed_time"],
                    "option_configuration_sha256": row["configuration_sha256"],
                    "option_market_policy_sha256": row["market_policy_sha256"],
                    "option_analysis_policy_sha256": row["analysis_policy_sha256"],
                    "stock_market_cutoff": row["scheduled_cycle"],
                    "detector_policy_version": STOCK_BEHAVIOR_GATE_POLICY.version,
                    "detector_policy_sha256": STOCK_BEHAVIOR_GATE_POLICY.sha256,
                    "entry_deadline": row["valid_until"], "decision_at": row["observed_time"],
                }
                if (any(getattr(assessment, key) != value for key, value in identity.items())
                        or assessment.sha256 != evidence["payload_sha256"]
                        or assessment.canonical_json() != evidence["payload_text"]
                        or not assessment.decision_at <= evidence["recorded_at"] <= as_of):
                    raise ValueError("assessment identity/hash/receipt mismatch")
                required = [gate for gate in assessment.gates if gate.requirement == "REQUIRED"]
                if assessment.stock_snapshot_id is not None and (
                    assessment.stock_profile != STOCK_BEHAVIOR_GATE_POLICY.profile
                    or assessment.stock_definition_sha256 != DEFINITION_V1_SHA256
                    or assessment.stock_policy_sha256 != OPTIONS_SWING_PROFILE.sha256
                ):
                    raise ValueError("unsupported stock profile identity")
                if assessment.disposition == "ELIGIBLE_RESEARCH" and (
                    not assessment.applicable or assessment.stock_snapshot_id is None
                    or len(required) != len(REQUIRED_GATES) or {gate.gate_id for gate in required} != REQUIRED_GATES
                    or any(gate.verdict != "PASS" for gate in required)
                ):
                    raise ValueError("eligible assessment requires all six v1 gates")
                disposition, decision, recorded = assessment.disposition, assessment.decision_at, evidence["recorded_at"]
                payload_hash = assessment.sha256
                reasons = sorted({reason for gate in required for reason in gate.reason_codes})
                metrics = {gate.gate_id: gate.actual_float for gate in assessment.gates
                    if gate.actual_float is not None and isfinite(gate.actual_float)}
            except (ValueError, TypeError, KeyError):
                disposition, reasons = "UNAVAILABLE", ["STOCK_ASSESSMENT_INVALID"]
        complete = _complete_package(row, applicable, as_of)
        if not complete:
            reasons = [*reasons, "OPTION_PACKAGE_INCOMPLETE"]
        timely = bool(recorded and row["valid_until"] and recorded < row["valid_until"])
        eligible = row["status"] == "SELECTED" and disposition == "ELIGIBLE_RESEARCH" and complete
        row["behavior"] = dict(disposition=disposition, reasons=reasons, applicable=applicable,
            decision_at=decision, recorded_at=recorded, payload_sha256=payload_hash,
            metrics=metrics, complete_package=complete, timely_at_recording=timely,
            entry_open_now=bool(row["valid_until"] and as_of < row["valid_until"]),
            eligible=eligible, shortlist=False, selection_reason=None)
        rows.append(row)
    groups = defaultdict(list)
    for row in rows:
        behavior = row["behavior"]
        if behavior["eligible"] and behavior["timely_at_recording"]:
            groups[(row["underlying"], row["strategy_name"], row["structure_type"], row["matrix_id"])].append(row)
    for group in groups.values():
        ordered = sorted(group, key=lambda row: (row["candidate_rank"], str(row["candidate_id"])))
        for index, row in enumerate(ordered):
            row["behavior"].update(shortlist=index == 0,
                selection_reason="MODEL_RANK_REPRESENTATIVE" if index == 0 else "LOWER_RANK_SAME_MODEL_DIRECTION")
    return rows


def selection_funnel(rows):
    behavior = [row["behavior"] for row in rows]
    return dict(candidates=len(rows), selected=sum(row["status"] == "SELECTED" for row in rows),
        applicable=sum(item["applicable"] for item in behavior),
        dispositions=dict(Counter(item["disposition"] for item in behavior)),
        eligible=sum(item["eligible"] for item in behavior),
        timely=sum(item["eligible"] and item["timely_at_recording"] for item in behavior),
        shortlist=sum(item["shortlist"] for item in behavior),
        entry_open_now=sum(item["shortlist"] and item["entry_open_now"] for item in behavior),
        reasons=dict(Counter(reason for item in behavior for reason in item["reasons"])))


def daily_scorecard(rows, outcomes, unavailable, as_of, *, persisted_alerts=False):
    outcome_by_key = {(str(row["candidate_id"]), row["measurement_type"]): row for row in outcomes}
    missing_by_key = {(str(row["candidate_id"]), row["measurement_type"]) for row in unavailable}
    cohorts = {}
    for row in sorted(rows, key=lambda item: (item["market_data_time"], item["observed_time"], item["candidate_rank"], str(item["candidate_id"]))):
        if persisted_alerts:
            cohorts.setdefault(str(row["candidate_id"]), row)
            continue
        if row["status"] != "SELECTED" or not row["behavior"]["applicable"] or not row["behavior"]["complete_package"]:
            continue
        if row["behavior"]["disposition"] not in ("ELIGIBLE_RESEARCH", "BLOCKED"):
            continue
        key = (row["underlying"], row["strategy_name"], row["structure_type"])
        cohorts.setdefault(key, row)
    def outcome_state(row, horizon):
        key = (str(row["candidate_id"]), horizon)
        checkpoint = measurement_checkpoints(row["market_data_time"]).get(horizon)
        outcome = outcome_by_key.get(key)
        state = ("NOT_APPLICABLE" if checkpoint is None else "NOT_DUE" if checkpoint > as_of
            else "MEASURED" if outcome and outcome.get("net_return") is not None
            else "UNAVAILABLE" if key in missing_by_key
            else "PENDING" if as_of < availability_policy.deadline(checkpoint) else "MISSING_AFTER_DEADLINE")
        return state, outcome

    cells = []
    cohort_rows = {}
    availability_policy = OptionOutcomeAvailabilityPolicy()
    arms = ("PERSISTED_BASELINE_ALERTS",) if persisted_alerts else ("ASSESSMENT_COVERED_CANDIDATE_BASELINE", "BEHAVIOR_V1_PASSED", "BEHAVIOR_V1_TIMELY")
    for arm in arms:
        grouped = defaultdict(list)
        for row in cohorts.values():
            if arm == "BEHAVIOR_V1_PASSED" and not row["behavior"]["eligible"]:
                continue
            if arm == "BEHAVIOR_V1_TIMELY" and not (row["behavior"]["eligible"] and row["behavior"]["timely_at_recording"]):
                continue
            for horizon in MEASUREMENTS:
                state, outcome = outcome_state(row, horizon)
                if arm in ("ASSESSMENT_COVERED_CANDIDATE_BASELINE", "PERSISTED_BASELINE_ALERTS"):
                    detail = cohort_rows.setdefault(str(row["candidate_id"]), {
                        field: row[field] for field in ("candidate_id", "candidate_identity", "underlying", "strategy_name",
                            "structure_type", "candidate_rank", "market_data_time", "observed_time")})
                    detail.update(behavior=row["behavior"])
                    detail.setdefault("outcomes", {})[horizon] = dict(state=state,
                        net_return=float(outcome["net_return"]) if state == "MEASURED" else None,
                        net_pnl=outcome.get("net_pnl") if state == "MEASURED" else None,
                        estimated_cost=outcome.get("estimated_cost") if state == "MEASURED" else None)
                grouped[(row["strategy_name"], row["structure_type"], horizon)].append((row, state, outcome))
        keys = {(row["strategy_name"], row["structure_type"], horizon) for row in cohorts.values() for horizon in MEASUREMENTS}
        for strategy, structure, horizon in sorted(keys):
            values = grouped[(strategy, structure, horizon)]
            measured = [float(outcome["net_return"]) for _, state, outcome in values if state == "MEASURED"]
            cells.append(dict(arm=arm, strategy=strategy, structure=structure, horizon=horizon,
                cohorts=len(values), states=dict(Counter(state for _, state, _ in values)), measured=len(measured),
                outcome_coverage=len(measured) / len(values) if values else None, mean_net_return=fmean(measured) if measured else None,
                positive_mark_fraction=sum(value > 0 for value in measured) / len(measured) if measured else None,
                minimum_net_return=min(measured) if measured else None, maximum_net_return=max(measured) if measured else None,
                distinct_underlyings=len({row["underlying"] for row, _, _ in values}),
                verdict="DESCRIPTIVE_ONLY" if measured else "INCONCLUSIVE", probability=None))
    participation_groups = defaultdict(list)
    participation_missing = 0
    for row in cohorts.values():
        value = row["behavior"]["metrics"].get("PARTICIPATION_EVIDENCE")
        if value is None or not isfinite(value):
            participation_missing += 1
            continue
        bucket = next(name for name, lower, upper in PARTICIPATION_BUCKETS
            if (lower is None or value >= lower) and (upper is None or value < upper))
        for horizon in MEASUREMENTS:
            state, outcome = outcome_state(row, horizon)
            participation_groups[(bucket, row["strategy_name"], row["structure_type"], horizon)].append((state, outcome))
    participation_cells = []
    for (bucket, strategy, structure, horizon), values in sorted(participation_groups.items()):
        measured = [float(outcome["net_return"]) for state, outcome in values if state == "MEASURED"]
        participation_cells.append(dict(bucket=bucket, strategy=strategy, structure=structure, horizon=horizon,
            cohorts=len(values), states=dict(Counter(state for state, _ in values)), measured=len(measured),
            outcome_coverage=len(measured) / len(values), mean_net_return=fmean(measured) if measured else None,
            positive_mark_fraction=sum(value > 0 for value in measured) / len(measured) if measured else None,
            verdict="DESCRIPTIVE_ONLY" if measured else "INCONCLUSIVE"))
    factors = defaultdict(list)
    for row in rows:
        for metric, value in row["behavior"]["metrics"].items():
            factors[metric].append(value)
    return dict(funnel=selection_funnel(rows), cohort_count=len(cohorts), cohort_rows=list(cohort_rows.values()), cells=cells,
        cohort_rule="PERSISTED_FIRST_ALERT_ONLY_REPEATS_EXCLUDED" if persisted_alerts else "FIRST_ASSESSMENT_COVERED_SELECTED_PER_SESSION_UNDERLYING_STRATEGY_STRUCTURE_BEFORE_OUTCOMES",
        factor_basis="CANDIDATE_OCCURRENCES_NOT_INDEPENDENT_SAMPLES",
        factors={key: dict(count=len(values), minimum=min(values), mean=fmean(values), maximum=max(values)) for key, values in sorted(factors.items())},
        participation_analysis=dict(schema_version="option_daily_rvol_challenger_v1",
            metric_id="daily_rvol20", gate_id="PARTICIPATION_EVIDENCE",
            basis="LATEST_COMPLETED_DAILY_VOLUME_OVER_PRIOR_20_SESSION_MEAN",
            timing="PRIOR_COMPLETED_SESSION_FOR_INTRADAY_CANDIDATES", selection_effect=False,
            missing_cohorts=participation_missing,
            buckets=[dict(name=name, minimum=lower, maximum_exclusive=upper) for name, lower, upper in PARTICIPATION_BUCKETS],
            cells=participation_cells),
        by_strategy={name: selection_funnel([row for row in rows if row["strategy_name"] == name]) for name in sorted({row["strategy_name"] for row in rows})},
        outcome_basis="INDICATIVE_OPTION_MARKS_NET_COMMISSION_NO_SLIPPAGE", probability=None,
        calibration_status="NOT_ATTEMPTED", threshold_changes=False, execution_permission=False)


def completed_run_selection(matrices, expected_underlyers, *, as_of, session_date=None):
    from zoneinfo import ZoneInfo

    expected = set(expected_underlyers)
    if not expected or len(expected) != len(expected_underlyers) or as_of.tzinfo is None:
        raise ValueError("completed runs require a distinct universe and aware cutoff")
    cycles = defaultdict(dict)
    for row in matrices:
        if row["underlying"] not in expected or row["completed_at"] > as_of or row["scheduled_cycle"] > as_of:
            continue
        group = cycles[row["scheduled_cycle"]]
        prior = group.get(row["underlying"])
        if prior is None or (row["completed_at"], str(row["matrix_id"])) > (prior["completed_at"], str(prior["matrix_id"])):
            group[row["underlying"]] = row
    runs = []
    for cycle, members in sorted(cycles.items()):
        if set(members) != expected:
            continue
        runs.append(dict(run_id=cycle.isoformat(), scheduled_cycle=cycle,
            session_date=cycle.astimezone(ZoneInfo("America/New_York")).date().isoformat(),
            completed_at=max(row["completed_at"] for row in members.values()),
            matrix_ids=[members[symbol]["matrix_id"] for symbol in sorted(expected)],
            covered_underlyings=len(members), expected_underlyings=len(expected)))
    active = runs[-1] if runs else None
    newer_partial = [cycle for cycle, members in cycles.items() if set(members) != expected
        and (active is None or cycle > active["scheduled_cycle"])]
    pending_cycle = max(newer_partial) if newer_partial else None
    pending = dict(scheduled_cycle=pending_cycle, covered_underlyings=len(cycles[pending_cycle]),
        expected_underlyings=len(expected), missing_underlyings=sorted(expected - set(cycles[pending_cycle]))) if pending_cycle else None
    return _partition_runs(runs, session_date=session_date, pending=pending)


def _partition_runs(runs, *, session_date, pending=None):
    active = runs[-1] if runs else None
    selected_date = session_date.isoformat() if session_date else active["session_date"] if active else None
    day_runs = [run for run in runs if run["session_date"] == selected_date]
    latest = day_runs[-1] if day_runs else None
    history = [run for run in day_runs if active is None or run["run_id"] != active["run_id"]]
    return dict(session_date=selected_date, sessions=sorted({run["session_date"] for run in runs}),
        active_run=active, latest_run=latest, history_runs=history, runs=day_runs, newer_partial_run=pending,
        withheld_run=active if active and active["session_date"] == selected_date else None)


def _detector_run_summary(run):
    return dict(run_id=str(run.run_id), scheduled_cycle=run.scheduled_cycle.isoformat(),
        selected_at=run.selected_at.isoformat(), published_at=run.selected_at.isoformat(),
        market_time=run.market_time.isoformat(), observed_time=run.observed_time.isoformat(),
        expected_underlyings=len(run.expected_underlyers), covered_underlyings=len(run.source_matrices),
        selection_counts=dict(run.selection_counts), rejections=dict(run.rejections))


def _detector_strategy(record):
    import json
    from options.analytics.alert_selection import (
        O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence,
    )

    if isinstance(record, O3CreditEvaluationEvidence):
        return "SPREAD_RANGE_LOCATOR"
    if isinstance(record, (O1IndicatorEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence)):
        return None
    return json.loads(record.plan_payload_text).get("management_policy", {}).get("strategy_name")


def _detector_triggered_at(record):
    import json
    from options.analytics.alert_selection import (
        O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence,
    )

    if isinstance(record, StockSetupIndicatorEvaluationEvidence):
        return record.observation.setup_source.candidate.trigger_at
    if isinstance(record, (O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, SurfaceEvaluationEvidence)):
        return None
    plan = json.loads(record.plan_payload_text)
    if record.detector_id == "O1":
        value = plan.get("source_market_time")
    else:
        evidence = plan.get("management_policy", {}).get("technical_exit", {}).get("evidence", {})
        value = evidence.get("market_time") if evidence.get("source_kind") == "STOCK_SETUP" else None
    if not isinstance(value, str):
        return None
    try:
        triggered_at = datetime.fromisoformat(value)
    except ValueError:
        return None
    return triggered_at if triggered_at.utcoffset() is not None and triggered_at <= record.package.decision_at else None


def _sort_detector_records(records, sort_by, sort_order):
    import json
    from decimal import Decimal
    from options.analytics.alert_selection import (
        O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence,
    )

    if sort_by not in {"triggered_at", "run", "underlyer", "detector", "category", "strategy", "rank", "entry_limit"} or sort_order not in {"asc", "desc"}:
        raise ValueError("invalid detector sort")
    def value(record):
        observation = isinstance(record, (O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence))
        source = record.observation if observation else record.package
        if sort_by == "triggered_at": return _detector_triggered_at(record)
        if sort_by == "run": return record.scheduled_cycle
        if sort_by == "underlyer": return source.underlyer
        if sort_by == "detector": return record.detector_id
        if sort_by == "category": return ("NEUTRAL_VOL" if record.detector_id == "O2" else "DEFINED_RISK_INCOME" if record.detector_id == "O3" else "MOMENTUM") if observation else source.primary_category
        if sort_by == "strategy": return _detector_strategy(record)
        if sort_by == "rank": return None if observation else source.candidate_rank
        return None if observation else Decimal(json.loads(record.plan_payload_text)["entry_limit"])
    ordered = sorted(records, key=lambda record: str(record.evaluation_id))
    values = [(record, value(record)) for record in ordered]
    present = [(record, key) for record, key in values if key is not None]
    missing = [record for record, key in values if key is None]
    return [record for record, _ in sorted(present, key=lambda item: item[1], reverse=sort_order == "desc")] + missing


def build_detector_evaluation_review(*, as_of, session_date=None, dataset_id=None, detector=None,
                                    selection_status=None, limit=50, offset=0, repository=None,
                                    underlyer=None, sort_by="run", sort_order="asc"):
    import json
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository

    from options.analytics.alert_selection import (
        O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence,
    )

    if not 1 <= limit <= 200 or offset < 0 or detector not in (None, "O1", "O2", "O3", "S1", "S2") or selection_status not in (None, "SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION"):
        raise ValueError("invalid detector evaluation review filters")
    inputs = (repository or OptionAlertEvaluationRepository()).review_inputs(
        as_of=as_of, session_date=session_date, dataset_id=dataset_id)
    records = inputs["records"]
    groups = defaultdict(list)
    exposures = defaultdict(set)
    filtered = []
    scoped = []
    for record in records:
        if detector is not None and record.detector_id != detector:
            continue
        observation_record = isinstance(record, (
            O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence,
        ))
        symbol = record.observation.underlyer if observation_record else record.package.underlyer
        if underlyer and symbol != underlyer.strip().upper():
            continue
        scoped.append(record)
        groups[(record.detector_id, record.selection_status, record.selection_reason)].append(record)
        if not observation_record:
            exposures[record.package.exposure_sha256].add(record.detector_id)
        if selection_status is None or record.selection_status == selection_status:
            filtered.append(record)
    rows = []
    filtered = _sort_detector_records(filtered, sort_by, sort_order)
    for record in filtered[offset:offset + limit]:
        if isinstance(record, O1IndicatorEvaluationEvidence):
            observation = record.observation
            rows.append(dict(evaluation_id=str(record.evaluation_id), candidate_id=None, run_id=str(record.run_id),
                scheduled_cycle=record.scheduled_cycle.isoformat(), detector_id="O1", origin="OPTIONS_FIRST",
                underlyer=observation.underlyer, direction=observation.direction, category="MOMENTUM",
                strategy_name=None, candidate_rank=None, selection_status="OBSERVATION",
                selection_reason="INDICATOR_SHADOW_ONLY", plan_sha256=None,
                selected_at=record.selected_at.isoformat(), entry_deadline=None, exit_deadline=None,
                entry_limit=None, outcome_status="NOT_YET_MEASURED_SHADOW", net_return=None,
                shared_model_exposure=False, event_horizon_status=None,
                observation=observation.model_dump(mode="json")))
            continue
        if isinstance(record, O3CreditEvaluationEvidence):
            observation = record.observation
            rows.append(dict(evaluation_id=str(record.evaluation_id), candidate_id=str(record.candidate_id), run_id=str(record.run_id),
                scheduled_cycle=record.scheduled_cycle.isoformat(), detector_id="O3", origin="OPTIONS_FIRST",
                underlyer=observation.underlyer, direction=observation.direction, category="DEFINED_RISK_INCOME",
                strategy_name="SPREAD_RANGE_LOCATOR", candidate_rank=observation.candidate_rank, selection_status="SELECTED",
                selection_reason="O3_INDICATIVE_ADMISSION", plan_sha256=None,
                selected_at=record.selected_at.isoformat(), entry_deadline=None, exit_deadline=None,
                entry_limit=None, outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", net_return=None,
                shared_model_exposure=False, event_horizon_status=None,
                observation=observation.model_dump(mode="json")))
            continue
        if isinstance(record, StockSetupIndicatorEvaluationEvidence):
            observation = record.observation
            rows.append(dict(evaluation_id=str(record.evaluation_id), candidate_id=None, run_id=str(record.run_id),
                scheduled_cycle=record.scheduled_cycle.isoformat(), detector_id=record.detector_id, origin="STOCK_FIRST",
                underlyer=observation.underlyer, direction=observation.direction, category="MOMENTUM",
                strategy_name=None, candidate_rank=None, selection_status="OBSERVATION",
                selection_reason="MIXED_INDICATOR_SHADOW_ONLY", plan_sha256=None,
                selected_at=record.selected_at.isoformat(), entry_deadline=None, exit_deadline=None,
                entry_limit=None, outcome_status="NOT_YET_MEASURED_SHADOW", net_return=None,
                shared_model_exposure=False, event_horizon_status=None,
                observation=observation.model_dump(mode="json")))
            continue
        if isinstance(record, SurfaceEvaluationEvidence):
            observation = record.observation
            rows.append(dict(evaluation_id=str(record.evaluation_id), candidate_id=None, run_id=str(record.run_id),
                scheduled_cycle=record.scheduled_cycle.isoformat(), detector_id="O2", origin="OPTIONS_FIRST",
                underlyer=observation.underlyer, direction=None, category="NEUTRAL_VOL", strategy_name=None, candidate_rank=None,
                selection_status="OBSERVATION", selection_reason="OBSERVATION_ONLY", plan_sha256=None,
                selected_at=record.selected_at.isoformat(), entry_deadline=None, exit_deadline=None, entry_limit=None,
                outcome_status="NOT_APPLICABLE_OBSERVATION", net_return=None, shared_model_exposure=False,
                event_horizon_status=observation.event_context_status,
                observation=dict(finding_disposition=observation.finding_disposition,
                    stock_context_status=observation.stock_context_status, reasons=observation.reasons,
                    findings=[row.model_dump(mode="json") for row in observation.findings])))
            continue
        package = record.package
        plan = json.loads(record.plan_payload_text)
        rows.append(dict(evaluation_id=str(record.evaluation_id), candidate_id=str(package.candidate_id),
            run_id=str(record.run_id), scheduled_cycle=package.scheduled_cycle.isoformat(),
            detector_id=package.detector_id, origin="OPTIONS_FIRST" if package.detector_id == "O1" else "STOCK_FIRST",
            underlyer=package.underlyer, direction=package.direction, category=package.primary_category,
            strategy_name=_detector_strategy(record),
            candidate_rank=package.candidate_rank, selection_status=record.selection_status,
            selection_reason=record.selection_reason, plan_sha256=package.plan_sha256,
            selected_at=record.selected_at.isoformat(), entry_deadline=package.entry_deadline.isoformat(),
            exit_deadline=package.exit_deadline.isoformat(), entry_limit=plan["entry_limit"],
            outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", net_return=None,
            event_horizon_status=package.event_horizon_status,
            shared_model_exposure=len(exposures[package.exposure_sha256]) > 1))
    cells = []
    for (model, status, reason), values in sorted(groups.items()):
        identities = {record.recurrence_sha256 for record in values}
        cells.append(dict(detector_id=model, selection_status=status, selection_reason=reason,
            occurrences=len(values), distinct_packages=len(identities),
            repeated_package_occurrences=len(values) - len(identities),
            measured=None, positive_rate=None, mean_net_return=None,
            outcome_status="NOT_APPLICABLE_OBSERVATION" if model == "O2" else
                "NOT_YET_MEASURED_SHADOW" if all(isinstance(row, (
                    O1IndicatorEvaluationEvidence, StockSetupIndicatorEvaluationEvidence)) for row in values)
                else "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES"))
    models = []
    for model in ("O1", "O2", "O3", "S1", "S2"):
        model_records = [record for record in scoped if record.detector_id == model]
        models.append(dict(detector_id=model, evaluated=len(model_records),
            detected=sum(record.observation.finding_disposition == "DETECTED" if isinstance(record, SurfaceEvaluationEvidence)
                else True for record in model_records),
            selected=sum(record.selection_status == "SELECTED" for record in model_records),
            not_selected=sum(record.selection_status == "NOT_SELECTED" for record in model_records),
            repeats=sum(record.selection_status == "REPEAT" for record in model_records),
            observations=sum(record.selection_status == "OBSERVATION" for record in model_records),
            measured=None, positive_rate=None, mean_net_return=None,
            outcome_status="NOT_APPLICABLE_OBSERVATION" if model == "O2" else
                "NOT_YET_MEASURED_SHADOW" if model_records and all(isinstance(row, (
                    O1IndicatorEvaluationEvidence, StockSetupIndicatorEvaluationEvidence)) for row in model_records)
                else "NOT_BOUND_TO_PROSPECTIVE_OUTCOMES"))
    return dict(version="option_detector_evaluation_review_v1", as_of=as_of.isoformat(),
        storage_ready=inputs["ready"], datasets=inputs["datasets"], dataset_id=inputs["dataset_id"],
        sessions=[day.isoformat() for day in inputs["sessions"]],
        session_date=inputs["session_date"].isoformat() if inputs["session_date"] else None,
        completed_runs=[_detector_run_summary(run) for run in inputs.get("runs", ())],
        rows=rows, total=len(filtered), cells=cells, models=models, limit=limit, offset=offset,
        maximum_new_alerts=20, cohort_basis="FROZEN_SELECTION_BEFORE_OUTCOMES",
        outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", execution_permission=False)


def build_detector_alert_review(*, dataset_id, as_of, scope="LATEST", session_date=None,
                               detector=None, limit=50, offset=0, repository=None,
                               underlyer=None, sort_by="run", sort_order="asc", source_repository=None,
                               mark_repository=None, valuation_policy=None, history_dataset_ids=()):
    import json
    from psycopg2 import Error as DatabaseError
    from zoneinfo import ZoneInfo
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository
    from options.analytics.alert_selection import (
        O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence,
    )

    history_dataset_ids = tuple(dict.fromkeys(history_dataset_ids or ()))
    if (not dataset_id or len(dataset_id) > 80 or as_of.utcoffset() is None
            or scope not in ("LATEST", "HISTORY") or detector not in (None, "O1", "O2", "O3", "S1", "S2")
            or not 1 <= limit <= 200 or offset < 0 or len(history_dataset_ids) > 20
            or any(not value or len(value) > 80 for value in history_dataset_ids)):
        raise ValueError("invalid detector alert scope")
    reader = repository or OptionAlertEvaluationRepository()
    _sort_detector_records((), sort_by, sort_order)
    dataset_ids = history_dataset_ids if scope == "HISTORY" and history_dataset_ids else (dataset_id,)
    runs_by_cycle = {}
    for source_dataset_id in dataset_ids:
        for run in reader.completed_runs(dataset_id=source_dataset_id, as_of=as_of):
            existing = runs_by_cycle.get(run.scheduled_cycle)
            if existing is None or (run.selected_at, run.dataset_id) > (existing.selected_at, existing.dataset_id):
                runs_by_cycle[run.scheduled_cycle] = run
    runs = tuple(sorted(runs_by_cycle.values(), key=lambda run: run.scheduled_cycle))
    sessions = sorted({run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date().isoformat() for run in runs})
    candidates = tuple(run for run in runs if session_date is None
        or run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() == session_date)
    latest = max(candidates, key=lambda run: run.scheduled_cycle) if candidates else None
    latest_date = latest.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() if latest else None
    selected_date = session_date or latest_date
    if latest is None:
        return dict(version="option_detector_alert_review_v2", dataset_id=dataset_id, scope=scope,
            dataset_ids=list(dataset_ids),
            status="NO_COMPLETE_RUN", as_of=as_of.isoformat(), latest_run_id=None, run=None, runs=[], latest_run=None,
            session_date=selected_date.isoformat() if selected_date else None,
            sessions=sessions, rows=[], total=0, new_alerts=0, repeat_hits=0, maximum_new_alerts=20,
            outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", execution_permission=False)
    if scope == "LATEST":
        completed = reader.completed_run(dataset_id=dataset_id, scheduled_cycle=latest.scheduled_cycle, as_of=as_of)
        if completed is None or completed[0] != latest:
            raise ValueError("latest completed detector run cannot be reconciled")
        active_runs, records = (latest,), completed[1]
    else:
        active_runs = tuple(run for run in candidates if run.run_id != latest.run_id)
        active_ids = {run.run_id for run in active_runs}
        records = []
        for source_dataset_id in dataset_ids:
            inputs = reader.review_inputs(dataset_id=source_dataset_id, session_date=selected_date, as_of=as_of)
            records.extend(row for row in inputs["records"] if row.run_id in active_ids)
        records = tuple(records)
    observation_types = (O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence, SurfaceEvaluationEvidence)
    visible = tuple(row for row in records if (row.selection_status == "SELECTED"
        or isinstance(row, (O1IndicatorEvaluationEvidence, O3CreditEvaluationEvidence, StockSetupIndicatorEvaluationEvidence))
        or isinstance(row, SurfaceEvaluationEvidence) and row.observation.finding_disposition == "DETECTED")
        and (detector is None or row.detector_id == detector)
        and (not underlyer or (row.observation.underlyer if isinstance(row, observation_types)
            else row.package.underlyer) == underlyer.strip().upper()))
    observations = tuple(row for row in visible if isinstance(row, observation_types))
    selected = _sort_detector_records(
        (row for row in visible if not isinstance(row, observation_types)), sort_by, sort_order)
    if scope == "LATEST" and len(selected) > 20:
        raise ValueError("latest detector run exceeds maximum new alerts")
    displayed = _sort_detector_records((*selected, *(row for row in observations if isinstance(row, O3CreditEvaluationEvidence))), sort_by, sort_order)
    page = displayed[offset:offset + limit]
    package_page = tuple(row for row in page if not isinstance(row, O3CreditEvaluationEvidence))
    hits = {}
    if package_page:
        records_by_dataset = defaultdict(list)
        for record in package_page:
            records_by_dataset[record.dataset_id].append(record)
        for source_dataset_id, source_records in records_by_dataset.items():
            hits.update(reader.repeat_counts(dataset_id=source_dataset_id, records=tuple(source_records), as_of=as_of))
    originals = source_repository.original_packages(package_page, as_of=as_of) if source_repository is not None and package_page else {}
    marks, mark_reason = {}, None
    if scope == "HISTORY" and page and valuation_policy is not None:
        from options.repositories.outcomes import OptionOutcomeRepository

        try:
            mark_inputs = (mark_repository or OptionOutcomeRepository()).retained_plan_marks(
                [record.candidate_id for record in page], available_by=as_of,
                valuation_policy_sha256=valuation_policy.policy_sha256)
            marks = mark_inputs["rows"]
            if not mark_inputs["ready"]:
                mark_reason = "CURRENT_MARK_STORAGE_UNAVAILABLE"
        except (ValueError, DatabaseError):
            mark_reason = "CURRENT_MARK_READ_UNAVAILABLE"
    rows = []
    for record in page:
        if isinstance(record, O3CreditEvaluationEvidence):
            from options.credit_detection import O3_CREDIT_POLICY

            observation = record.observation
            contract_type = "PUT" if observation.structure_type == "PUT_CREDIT_VERTICAL" else "CALL"
            expiration = observation.legs[0].expiration_date
            original = dict(status="AVAILABLE", basis="ORIGINAL_CANDIDATE_SNAPSHOTS",
                structure_type=observation.structure_type, expiration_date=expiration.isoformat(),
                calendar_dte=(expiration - observation.decision_at.date()).days,
                market_data_time=max(leg.source_market_time for leg in observation.legs).isoformat(),
                observed_time=observation.decision_at.isoformat(), net_premium=str(observation.net_credit),
                capital_at_risk=str(observation.maximum_loss), maximum_loss=str(observation.maximum_loss),
                maximum_profit=str(observation.maximum_profit), breakevens=[str(observation.breakeven)],
                legs=[dict(leg_index=leg.leg_index, snapshot_id=str(leg.snapshot_id), contract_id=leg.contract_id,
                    contract_ticker=leg.contract_ticker, side=leg.side, ratio=leg.ratio, multiplier=leg.multiplier,
                    expiration_date=leg.expiration_date.isoformat(), strike=str(leg.strike), contract_type=contract_type,
                    model_mark=str(leg.model_mark), local_iv=leg.local_iv, local_delta=leg.local_delta,
                    local_gamma=leg.local_gamma, local_theta_per_day=leg.local_theta_per_day,
                    local_vega_per_vol_point=leg.local_vega_per_vol_point,
                    local_rho_per_rate_point=leg.local_rho_per_rate_point, spot=str(leg.spot),
                    day_volume=leg.day_volume, open_interest=leg.open_interest,
                    source_market_time=leg.source_market_time.isoformat(), mark_source=leg.mark_source,
                    valuation_policy_version=leg.valuation_policy_version,
                    valuation_policy_sha256=leg.valuation_policy_sha256,
                    quality_flags=[], quote_bid=str(leg.bid) if leg.bid is not None else None,
                    quote_ask=str(leg.ask) if leg.ask is not None else None,
                    quote_midpoint=None, quote_spread_midpoint=None) for leg in observation.legs])
            current_mark = None
            if scope == "HISTORY" and valuation_policy is not None:
                from options.outcomes import review_retained_candidate_mark

                current_mark = review_retained_candidate_mark(dict(candidate_id=record.candidate_id,
                    candidate_identity=observation.candidate_identity_sha256, matrix_id=record.matrix_id,
                    market_data_time=original["market_data_time"], net_premium=original["net_premium"],
                    capital_at_risk=original["capital_at_risk"], legs=original["legs"]),
                    marks.get(str(record.candidate_id)), checked_at=as_of, policy=valuation_policy)
                if mark_reason:
                    current_mark["reason"] = mark_reason
            rows.append(dict(evaluation_id=str(record.evaluation_id), candidate_id=str(record.candidate_id),
                matrix_id=str(record.matrix_id), run_id=str(record.run_id), scheduled_cycle=record.scheduled_cycle.isoformat(),
                triggered_at=observation.decision_at.isoformat(), detector_id="O3", origin="OPTIONS_FIRST",
                underlyer=observation.underlyer, direction=observation.direction, category="DEFINED_RISK_INCOME",
                strategy_name="SPREAD_RANGE_LOCATOR", candidate_rank=observation.candidate_rank,
                first_selected_at=record.selected_at.isoformat(), hit_count=1, repeat_count=0,
                last_seen_at=record.selected_at.isoformat(), plan_sha256=None,
                entry_limit=str(observation.net_credit), entry_deadline=observation.valid_until.isoformat(),
                exit_deadline=None, management_policy=dict(policy_version=O3_CREDIT_POLICY["version"],
                    strategy_name="SPREAD_RANGE_LOCATOR", basis="ENTRY_CREDIT",
                    stop_loss_multiple=O3_CREDIT_POLICY["stop_loss_multiple"],
                    take_profit_fraction=O3_CREDIT_POLICY["take_profit_fraction"],
                    exit_dte=O3_CREDIT_POLICY["minimum_exit_dte"]), event_horizon_status=None,
                original_package=original, current_mark=current_mark, observation=observation.model_dump(mode="json"),
                outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", net_return=None, fill=None))
            continue
        package = record.package
        plan = json.loads(record.plan_payload_text)
        repeat = hits[record.evaluation_id]
        triggered_at = _detector_triggered_at(record)
        original = originals.get(str(record.candidate_id), dict(status="UNAVAILABLE", legs=[]))
        current_mark = None
        if scope == "HISTORY" and valuation_policy is not None and original.get("status") == "AVAILABLE":
            from options.outcomes import review_retained_candidate_mark

            current_mark = review_retained_candidate_mark(dict(
                candidate_id=record.candidate_id,
                candidate_identity=package.candidate_identity_sha256,
                matrix_id=package.matrix_id,
                market_data_time=original["market_data_time"],
                net_premium=original["net_premium"],
                capital_at_risk=original["capital_at_risk"],
                legs=original["legs"],
            ), marks.get(str(record.candidate_id)), checked_at=as_of, policy=valuation_policy)
            if mark_reason:
                current_mark["reason"] = mark_reason
        rows.append(dict(evaluation_id=str(record.evaluation_id), candidate_id=str(record.candidate_id), matrix_id=str(package.matrix_id),
            run_id=str(record.run_id), scheduled_cycle=record.scheduled_cycle.isoformat(),
            triggered_at=triggered_at.isoformat() if triggered_at else None,
            detector_id=record.detector_id, origin="OPTIONS_FIRST" if record.detector_id == "O1" else "STOCK_FIRST",
            underlyer=package.underlyer, direction=package.direction, category=package.primary_category,
            strategy_name=_detector_strategy(record),
            candidate_rank=package.candidate_rank, first_selected_at=record.selected_at.isoformat(),
            hit_count=1 + repeat["repeats"], repeat_count=repeat["repeats"],
            last_seen_at=(repeat["last_seen_at"] or record.selected_at).isoformat(),
            plan_sha256=package.plan_sha256, entry_limit=plan["entry_limit"],
            entry_deadline=package.entry_deadline.isoformat(), exit_deadline=package.exit_deadline.isoformat(),
            management_policy=plan["management_policy"], event_horizon_status=package.event_horizon_status,
            original_package=original, current_mark=current_mark,
            outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", net_return=None, fill=None))
    return dict(version="option_detector_alert_review_v2", dataset_id=dataset_id, scope=scope,
        dataset_ids=list(dataset_ids),
        status="COMPLETE", as_of=as_of.isoformat(), latest_run_id=str(latest.run_id),
        run=_detector_run_summary(latest) if scope == "LATEST" else None,
        latest_run=_detector_run_summary(latest), runs=[_detector_run_summary(run) for run in active_runs],
        session_date=selected_date.isoformat(), sessions=sessions,
        rows=rows, total=len(displayed), limit=limit, offset=offset,
        detected_observations=len(observations),
        new_alerts=sum(dict(run.selection_counts)["SELECTED"] for run in active_runs),
        repeat_hits=sum(dict(run.selection_counts)["REPEAT"] for run in active_runs), maximum_new_alerts=20,
        outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", execution_permission=False)


def build_behavior_review(configuration, *, session_date=None, as_of, view="SHORTLIST", limit=50, offset=0,
                          underlyer=None, strategy=None, minimum_dte=None, maximum_dte=None, repository=None,
                          scope="LATEST", mark_repository=None, alert_repository=None):
    from time import perf_counter
    from zoneinfo import ZoneInfo
    from options.calendar import OptionExchangeCalendar
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository

    if view not in ("SHORTLIST", "ELIGIBLE", "ALL", "DAILY") or scope not in ("LATEST", "CURRENT", "HISTORY") or not 1 <= limit <= 200 or offset < 0:
        raise ValueError("invalid bounded research view")
    if as_of.tzinfo is None:
        raise ValueError("research review requires an aware receipt cutoff")
    calendar = OptionExchangeCalendar()
    today = as_of.astimezone(ZoneInfo("America/New_York")).date()
    latest_completed = calendar.latest_completed_session(as_of)
    started = perf_counter()
    reader = repository or OptionStockBehaviorAssessmentRepository()
    run_selection, matrix_ids = None, None
    baseline = scope in ("CURRENT", "HISTORY") and getattr(configuration.settings, "baseline_alerts_enabled", False)
    effective_from = getattr(getattr(configuration, "settings", None), "baseline_alerts_effective_from", None)
    if baseline and effective_from is not None and as_of < effective_from:
        baseline = False
    if baseline and session_date is not None and effective_from is not None:
        baseline = session_date >= effective_from.astimezone(ZoneInfo("America/New_York")).date()
    members, candidate_ids = {}, None
    if scope in ("CURRENT", "HISTORY"):
        from datetime import date

        if baseline:
            from options.repositories.board import OptionBoardPublicationRepository

            alert_reader = alert_repository or OptionBoardPublicationRepository()
            stored_runs = alert_reader.baseline_runs(configuration=configuration, as_of=as_of)
            runs = [dict(run_id=str(run["publication_id"]), publication_id=run["publication_id"],
                scheduled_cycle=run["scheduled_cycle"], session_date=run["as_of_session"].isoformat(),
                completed_at=run["published_at"], matrix_ids=run["source_matrix_ids"],
                expected_underlyings=run["expected_underlying_count"], covered_underlyings=run["covered_underlying_count"],
                new_alerts=run["new_alerts"], repeat_hits=run["repeat_hits"], rejections=run["rejections"], by_model=run["by_model"])
                for run in stored_runs]
            run_selection = _partition_runs(runs, session_date=session_date)
            if effective_from is not None:
                legacy = completed_run_selection(reader.completed_matrices(configuration=configuration, as_of=as_of),
                    configuration.settings.underlyers, as_of=as_of)
                cutover_date = effective_from.astimezone(ZoneInfo("America/New_York")).date().isoformat()
                run_selection["sessions"] = sorted(set(run_selection["sessions"]) | {
                    day for day in legacy["sessions"] if day < cutover_date})
        else:
            run_selection = completed_run_selection(reader.completed_matrices(configuration=configuration, as_of=as_of),
                configuration.settings.underlyers, as_of=as_of, session_date=session_date)
        session_date = date.fromisoformat(run_selection["session_date"]) if run_selection["session_date"] else latest_completed
        selected_runs = (run_selection["runs"] if view == "DAILY" else run_selection["history_runs"] if scope == "HISTORY"
            else [run_selection["latest_run"]] if run_selection["latest_run"] else [])
        matrix_ids = [matrix_id for run in selected_runs for matrix_id in run["matrix_ids"]]
        if baseline:
            members = alert_reader.baseline_members([run["publication_id"] for run in selected_runs], configuration=configuration, as_of=as_of)
            candidate_ids = list(members)
    elif session_date is None:
        try:
            calendar.session_close(today)
            session_date = today
        except ValueError:
            session_date = latest_completed
    session_close = calendar.session_close(session_date)
    if matrix_ids == [] or candidate_ids == []:
        inputs = dict(candidates=[], assessments={}, outcomes=[], unavailable=[], schema={})
    else:
        inputs = reader.research_inputs(
            configuration=configuration, session_date=session_date, as_of=as_of, daily=view == "DAILY", history=scope == "HISTORY", matrix_ids=matrix_ids,
            **({"candidate_ids": candidate_ids} if baseline else {}))
    rows = annotate_candidates(inputs["candidates"], inputs["assessments"], as_of)
    from options.strategies.domain import StructureType
    from options.strategies.registry import discovery_categories

    for row in rows:
        row["category_ids"] = list(discovery_categories(StructureType(row["structure_type"])))
    if baseline:
        from options.analytics.alert_selection import BASELINE_MODELS, BASELINE_SELECTOR_SHA256, BASELINE_SELECTOR_VERSION, baseline_package_identity

        if {str(row["candidate_id"]) for row in rows} != set(members):
            raise ValueError("persisted alert members could not be resolved exactly")
        for row in rows:
            member = members[str(row["candidate_id"])]
            if (baseline_package_identity(row) != member["selection_evidence"]["alert_identity"]
                    or not row["behavior"]["complete_package"]
                    or not row["observed_time"] <= member["published_at"] < row["valid_until"]):
                raise ValueError("persisted alert package identity or original deadline mismatch")
            row.update(alert=member, hit_count=member["hit_count"], display_name=BASELINE_MODELS[row["strategy_name"]])
    rows = [row for row in rows if (not underlyer or row["underlying"] == underlyer.upper())
            and (not strategy or row["strategy_name"] == strategy)
            and (minimum_dte is None or row["calendar_dte"] is not None and row["calendar_dte"] >= minimum_dte)
            and (maximum_dte is None or row["calendar_dte"] is not None and row["calendar_dte"] <= maximum_dte)]
    common = dict(version=REVIEW_VERSION, session_date=session_date.isoformat(), as_of=as_of.isoformat(),
        policy_version=STOCK_BEHAVIOR_GATE_POLICY.version, policy_sha256=STOCK_BEHAVIOR_GATE_POLICY.sha256,
        strategy_policy_sha256=configuration.strategy_policy_sha256, valuation_policy_sha256=configuration.valuation_policy_sha256,
        session_close=session_close.isoformat() if session_close else None,
        session_state="NON_TRADING_DAY" if session_close is None else "CLOSED" if as_of >= session_close else "IN_PROGRESS",
        scope=scope, latest_completed_session=latest_completed.isoformat(),
        history_basis="RETAINED_CANDIDATE_DETECTIONS_NOT_PUBLICATIONS",
        selection_basis=("COMPLETE_CYCLES_IN_SESSION" if view == "DAILY" else "EARLIER_COMPLETE_CYCLES_EXCLUDING_ACTIVE_RUN" if scope == "HISTORY" else "LATEST_COMPLETE_CONFIGURED_CYCLE")
            if run_selection is not None else "ALL_SELECTED_SESSION_MATRICES" if view == "DAILY" else "LATEST_COMPLETE_MATRIX_PER_UNDERLYING",
        schema=inputs["schema"], assessment_only=True, execution_permission=False, probability=None)
    if run_selection is not None:
        def summary(run):
            return {key: value for key, value in run.items() if key != "matrix_ids"} if run else None

        common.update(sessions=run_selection["sessions"], run=summary(run_selection["latest_run"]),
            active_run=summary(run_selection["active_run"]), withheld_run=summary(run_selection["withheld_run"]),
            newer_partial_run=run_selection["newer_partial_run"],
            runs=[summary(run) for run in selected_runs], configuration_sha256=configuration.configuration_sha256,
            run_scope="COMPLETED_CONFIGURED_UNIVERSE_SCHEDULED_CYCLE", default_selection="MODEL_SELECTED_COMPLETE_PACKAGES")
    if baseline:
        common.update(history_basis="PERSISTED_BASELINE_ALERT_MEMBERS", selection_basis="WORKER_PERSISTED_FIRST_ALERTS",
            default_selection="PERSISTED_BASELINE_ALERT_MEMBERS", selector_version=BASELINE_SELECTOR_VERSION,
            selector_sha256=BASELINE_SELECTOR_SHA256, models=BASELINE_MODELS,
            run_funnel=dict(new_alerts=sum(run["new_alerts"] for run in selected_runs),
                repeat_hits=sum(run["repeat_hits"] for run in selected_runs)))
    if view == "DAILY":
        result = {**common, **daily_scorecard(rows, inputs["outcomes"], inputs["unavailable"], as_of, persisted_alerts=baseline)}
    else:
        filtered = [row for row in rows if view == "ALL" or row["behavior"]["eligible" if view == "ELIGIBLE" else "shortlist"]]
        if run_selection is not None:
            filtered = [row for row in filtered if row["status"] == "SELECTED" and row["behavior"]["complete_package"]]
        if scope == "HISTORY":
            filtered.sort(key=lambda row: (-row["market_data_time"].timestamp(), row["underlying"], row["strategy_name"], row["candidate_rank"], str(row["candidate_id"])))
        else:
            filtered.sort(key=lambda row: (row["underlying"], row["strategy_name"], row["candidate_rank"], str(row["candidate_id"])))
        result = {**common, "funnel": selection_funnel(rows), "rows": filtered[offset:offset + limit], "total": len(filtered),
            "limit": limit, "offset": offset, "view": view,
            "shortlist_rule": "WORKER_PERSISTED_FIRST_ALERTS" if baseline else "FIRST_MODEL_RANK_PER_MATRIX_UNDERLYING_STRATEGY_STRUCTURE",
            "exposure_note": "Strategy representatives may share underlying exposure; not portfolio approval."}
        if scope == "HISTORY" and result["rows"]:
            from psycopg2 import Error as DatabaseError
            from options.outcomes import review_retained_candidate_mark
            from options.repositories.outcomes import OptionOutcomeRepository

            marks, mark_reason = {}, None
            ids = [row["candidate_id"] for row in result["rows"]]
            try:
                activity = reader.page_leg_activity(ids)
            except (ValueError, DatabaseError):
                activity = {}
            for row in result["rows"]:
                row["legs"] = [{**leg, **activity.get((str(row["candidate_id"]), leg.get("leg_index")), {})} for leg in row["legs"]]
            try:
                mark_inputs = (mark_repository or OptionOutcomeRepository()).retained_plan_marks(
                    ids, available_by=as_of,
                    valuation_policy_sha256=configuration.valuation_policy_sha256)
                marks = mark_inputs["rows"]
                if not mark_inputs["ready"]:
                    mark_reason = "CURRENT_MARK_STORAGE_UNAVAILABLE"
            except (ValueError, DatabaseError):
                mark_reason = "CURRENT_MARK_READ_UNAVAILABLE"
            for row in result["rows"]:
                row["current_mark"] = review_retained_candidate_mark(row, marks.get(str(row["candidate_id"])),
                    checked_at=as_of, policy=configuration.valuation_policy)
                if mark_reason:
                    row["current_mark"]["reason"] = mark_reason
    result["elapsed_seconds"] = round(perf_counter() - started, 4)
    return result