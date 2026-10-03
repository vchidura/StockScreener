"""Read-only v1 behavior selection and descriptive outcome review."""
from collections import Counter, defaultdict
from datetime import date, datetime
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


def _detector_run_summary(run):
    return dict(run_id=str(run.run_id), scheduled_cycle=run.scheduled_cycle.isoformat(),
        selected_at=run.selected_at.isoformat(), published_at=run.selected_at.isoformat(),
        market_time=run.market_time.isoformat(), observed_time=run.observed_time.isoformat(),
        expected_underlyings=len(run.expected_underlyers), covered_underlyings=len(run.source_matrices),
        coverage_status=run.coverage_status,
        unavailable_underlyers=dict(run.unavailable_underlyers) if run.partial_coverage else {},
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
        value = evidence.get("market_time") if evidence.get("source_kind") in (
            "STOCK_SETUP", "CANONICAL_STOCK_SIGNAL") else None
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
                               mark_repository=None, valuation_policy=None, history_dataset_ids=(), attempts=()):
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
    display_records_supported = hasattr(source_repository, "alert_display_records")
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
    valid_attempts = []
    for attempt in attempts:
        cycle = datetime.fromisoformat(attempt["scheduled_cycle"])
        started = datetime.fromisoformat(attempt["started_at"])
        if (cycle.utcoffset() is None or started.utcoffset() is None or not cycle <= started <= as_of
                or attempt["status"] not in ("RUNNING", "FAILED", "INCOMPLETE", "UNVERIFIED")):
            raise ValueError("detector attempt identity or clocks mismatch")
        if session_date is None or cycle.astimezone(ZoneInfo("America/New_York")).date() == session_date:
            valid_attempts.append((cycle, attempt))
    latest_attempt = max(valid_attempts, key=lambda item: item[0]) if valid_attempts else None
    newer_attempt = latest_attempt is not None and (latest is None or latest_attempt[0] > latest.scheduled_cycle)
    if newer_attempt:
        selected_date = latest_attempt[0].astimezone(ZoneInfo("America/New_York")).date()
    if latest is None or scope == "LATEST" and newer_attempt:
        return dict(version="option_detector_alert_review_v2", dataset_id=dataset_id, scope=scope,
            dataset_ids=list(dataset_ids),
            status=latest_attempt[1]["status"] if newer_attempt else "NO_COMPLETE_RUN",
            latest_attempt=latest_attempt[1] if newer_attempt else None,
            as_of=as_of.isoformat(), latest_run_id=str(latest.run_id) if latest else None, run=None, runs=[],
            latest_run=_detector_run_summary(latest) if latest else None,
            session_date=selected_date.isoformat() if selected_date else None,
            sessions=sessions, rows=[], total=0, new_alerts=0, repeat_hits=0, maximum_new_alerts=20,
            outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", execution_permission=False)
    display_observation_count = None
    if scope == "LATEST":
        if display_records_supported:
            inputs = source_repository.alert_display_records(dataset_id=dataset_id, session_date=selected_date,
                runs=(latest,), as_of=as_of, detector=detector, underlyer=underlyer)
            active_runs, records = (latest,), inputs["records"]
            display_observation_count = inputs["detected_observations"]
        else:
            completed = reader.completed_run(dataset_id=dataset_id, scheduled_cycle=latest.scheduled_cycle, as_of=as_of)
            if completed is None or completed[0] != latest:
                raise ValueError("latest completed detector run cannot be reconciled")
            active_runs, records = (latest,), completed[1]
    else:
        active_runs = tuple(run for run in candidates if newer_attempt or run.run_id != latest.run_id)
        active_ids = {run.run_id for run in active_runs}
        records = []
        history_observation_count = 0
        optimized_history = True
        for source_dataset_id in dataset_ids:
            source_runs = tuple(run for run in active_runs if run.dataset_id == source_dataset_id)
            if not source_runs:
                continue
            if display_records_supported:
                inputs = source_repository.alert_display_records(dataset_id=source_dataset_id, session_date=selected_date,
                    runs=source_runs, as_of=as_of, detector=detector, underlyer=underlyer)
                records.extend(inputs["records"])
                history_observation_count += inputs["detected_observations"]
            else:
                optimized_history = False
                inputs = reader.review_inputs(dataset_id=source_dataset_id, session_date=selected_date, as_of=as_of)
                records.extend(row for row in inputs["records"] if row.run_id in active_ids)
        records = tuple(records)
        if optimized_history:
            display_observation_count = history_observation_count
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
    marks, mark_reason, management_status_ready = {}, None, False
    if page and valuation_policy is not None:
        from options.repositories.outcomes import OptionOutcomeRepository

        try:
            mark_inputs = (mark_repository or OptionOutcomeRepository()).retained_plan_marks(
                [record.candidate_id for record in page], available_by=as_of,
                valuation_policy_sha256=valuation_policy.policy_sha256)
            marks = mark_inputs["rows"]
            management_status_ready = bool(mark_inputs.get("management_status_ready"))
            if not mark_inputs["ready"]:
                mark_reason = "CURRENT_MARK_STORAGE_UNAVAILABLE"
        except (ValueError, DatabaseError):
            mark_reason = "CURRENT_MARK_READ_UNAVAILABLE"
    rows = []
    current_session_date = as_of.astimezone(ZoneInfo("America/New_York")).date()
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
            if valuation_policy is not None:
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
                management_status="NOT_APPLICABLE",
                outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", net_return=None, fill=None))
            continue
        package = record.package
        plan = json.loads(record.plan_payload_text)
        repeat = hits[record.evaluation_id]
        stock_confirmation = plan.get("stock_confirmation") or {}
        confirmation_timeframes = sorted(set((repeat.get("confirmation_timeframes") or ()))
            | ({stock_confirmation["interval"]} if stock_confirmation.get("schema_version") == "option_canonical_stock_signal_v1" else set()),
            key=lambda value: {"5m": 0, "15m": 1, "30m": 2, "1h": 3}.get(value, 99))
        triggered_at = _detector_triggered_at(record)
        original = originals.get(str(record.candidate_id), dict(status="UNAVAILABLE", legs=[]))
        current_mark = None
        if valuation_policy is not None and original.get("status") == "AVAILABLE":
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
        management_status = marks.get(str(record.candidate_id), {}).get("management_status")
        if management_status is None:
            expiration_date = original.get("expiration_date")
            if isinstance(expiration_date, str):
                try:
                    expiration_date = date.fromisoformat(expiration_date)
                except ValueError:
                    expiration_date = None
            elif isinstance(expiration_date, datetime):
                expiration_date = expiration_date.date()
            management_status = ("EXPIRED" if isinstance(expiration_date, date) and expiration_date < current_session_date
                else "MONITORING" if management_status_ready else "UNAVAILABLE")
        rows.append(dict(evaluation_id=str(record.evaluation_id), candidate_id=str(record.candidate_id), matrix_id=str(package.matrix_id),
            run_id=str(record.run_id), scheduled_cycle=record.scheduled_cycle.isoformat(),
            triggered_at=triggered_at.isoformat() if triggered_at else None,
            detector_id=record.detector_id, origin="OPTIONS_FIRST" if record.detector_id == "O1" else "STOCK_FIRST",
            underlyer=package.underlyer, direction=package.direction, category=package.primary_category,
            strategy_name=_detector_strategy(record),
            candidate_rank=package.candidate_rank, first_selected_at=record.selected_at.isoformat(),
            hit_count=1 + repeat["repeats"], repeat_count=repeat["repeats"],
            confirmation_timeframes=confirmation_timeframes,
            last_seen_at=(repeat["last_seen_at"] or record.selected_at).isoformat(),
            plan_sha256=package.plan_sha256, entry_limit=plan["entry_limit"],
            entry_deadline=package.entry_deadline.isoformat(), exit_deadline=package.exit_deadline.isoformat(),
            management_policy=plan["management_policy"], event_horizon_status=package.event_horizon_status,
            management_status=management_status,
            original_package=original, current_mark=current_mark,
            outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", net_return=None, fill=None))
    return dict(version="option_detector_alert_review_v2", dataset_id=dataset_id, scope=scope,
        dataset_ids=list(dataset_ids),
        status="PARTIAL" if scope == "LATEST" and latest.coverage_status == "PARTIAL" else "COMPLETE",
        latest_attempt=latest_attempt[1] if newer_attempt else None,
        as_of=as_of.isoformat(), latest_run_id=str(latest.run_id),
        run=_detector_run_summary(latest) if scope == "LATEST" else None,
        latest_run=_detector_run_summary(latest), runs=[_detector_run_summary(run) for run in active_runs],
        session_date=selected_date.isoformat(), sessions=sessions,
        rows=rows, total=len(displayed), limit=limit, offset=offset,
        detected_observations=(display_observation_count if display_observation_count is not None else len(observations)),
        new_alerts=sum(dict(run.selection_counts)["SELECTED"] for run in active_runs),
        repeat_hits=sum(dict(run.selection_counts)["REPEAT"] for run in active_runs), maximum_new_alerts=20,
        outcome_status="NOT_BOUND_TO_PROSPECTIVE_OUTCOMES", execution_permission=False)
