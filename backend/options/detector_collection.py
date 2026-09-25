from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from typing import Callable, Mapping

from options.analytics.alert_selection import (
    build_detector_run, build_o1_indicator_evidence, build_selection_evidence,
    build_stock_setup_indicator_evidence, build_surface_evidence,
    select_dual_origin_packages,
)
from options.dual_origin import bind_activity_source, detect_option_participation, qualify_dual_origin_package
from options.surface_detection import assess_surface_first, bind_surface_source, surface_snapshot_eligible


@dataclass(frozen=True)
class DetectorCycleInputs:
    matrices: tuple[Mapping, ...]
    package_inputs: tuple[Mapping, ...] = ()
    o1_observations: tuple[object, ...] = ()
    stock_setup_observations: tuple[object, ...] = ()
    surface_inputs: tuple[Mapping, ...] = ()
    rejections: tuple[tuple[str, int], ...] = ()
    reject_invalid_packages: bool = False


class RetainedDetectorSourceReader:
    def __init__(self, cycle_repository, reference_repository, evidence_repository, *, clock=None):
        self.cycle_repository = cycle_repository
        self.reference_repository = reference_repository
        self.evidence_repository = evidence_repository
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def read(self, *, configuration, scheduled_cycle, completed_matrices, as_of):
        from equity.behavior import DEFINITION_V1_SHA256, OPTIONS_SWING_PROFILE
        from equity.domain import DecisionWatermark
        from options.repositories.daily_facts import DailyOpenInterestRecord
        from options.repositories.snapshots import _snapshot
        from options.repositories.stock_behavior_assessments import OptionMatrixAssessmentLineage

        retained = self.cycle_repository.detector_cycle_sources(configuration=configuration,
            scheduled_cycle=scheduled_cycle, completed_matrices=completed_matrices, as_of=as_of)
        by_batch = {row["batch_id"]: row for row in retained["matrices"]}
        facts = defaultdict(list)
        for row in retained["open_interest"]:
            facts[row["contract_id"]].append(row)
        stocks, securities = {}, {}
        for symbol in sorted(completed_matrices):
            matrix = next(row for row in retained["matrices"] if row["underlying"] == symbol)
            watermark = DecisionWatermark(max(scheduled_cycle, matrix["market_time"]), as_of)
            security = self.reference_repository.get_security_as_of(symbol, watermark)
            securities[symbol] = security
            stocks[symbol] = self.evidence_repository.get_behavior_as_of(security.security_id, watermark,
                profile=OPTIONS_SWING_PROFILE.name, definition_sha256=DEFINITION_V1_SHA256,
                policy_sha256=OPTIONS_SWING_PROFILE.sha256) if security is not None else None
        received_at = self.clock()
        if received_at.utcoffset() is None or received_at < as_of:
            raise ValueError("detector source read receipt precedes query cutoff")
        activity, groups, excluded = [], defaultdict(list), Counter()
        for row in retained["snapshots"]:
            snapshot = _snapshot(row)
            matrix = by_batch.get(snapshot.batch_id)
            if matrix is None or matrix["underlying"] != snapshot.underlyer:
                raise ValueError("retained snapshot is outside the exact matrix scope")
            security = securities[snapshot.underlyer]
            if security is None:
                excluded["STOCK_SECURITY_IDENTITY_UNAVAILABLE"] += 1
                continue
            lineage = OptionMatrixAssessmentLineage(**{key: matrix[key]
                for key in OptionMatrixAssessmentLineage.__dataclass_fields__})
            matches = [fact for fact in facts[snapshot.contract_id]
                if fact["underlying"] == snapshot.underlyer and fact["open_interest"] == snapshot.open_interest
                and fact["open_interest_observed_at"] <= snapshot.first_observed_at
                and max(fact["created_at"], fact["updated_at"]) <= as_of
                and not fact.get("open_interest_revision_count")
                and fact.get("open_interest_source") == "PROVIDER_CHAIN_SNAPSHOT"]
            fact = matches[0] if len(matches) == 1 else None
            oi = DailyOpenInterestRecord(contract_id=fact["contract_id"], underlying=fact["underlying"],
                settlement_session=fact["settlement_session"], open_interest=fact["open_interest"],
                observed_at=fact["open_interest_observed_at"], observed_session=fact["open_interest_observed_session"],
                batch_id=fact["open_interest_batch_id"]) if fact is not None else None
            try:
                source = bind_activity_source(snapshot=snapshot, open_interest_record=oi, lineage=lineage,
                    security=security, recorded_at=max(row["created_at"], matrix["created_at"],
                        fact["updated_at"] if fact else row["created_at"]), received_at=received_at)
                finding = detect_option_participation(source, market_cutoff=max(scheduled_cycle, lineage.market_time), decision_at=received_at)
            except ValueError:
                excluded["ACTIVITY_SOURCE_INVALID"] += 1
            else:
                activity.append(dict(source=source, finding=finding, snapshot=snapshot, lineage=lineage,
                    security=security, stock=stocks[snapshot.underlyer]))
            if not surface_snapshot_eligible(snapshot, valuation_policy=configuration.valuation_policy):
                excluded["SURFACE_POINT_INELIGIBLE"] += 1
            else:
                groups[(snapshot.batch_id, snapshot.expiration_date, snapshot.contract_type)].append((snapshot, row["created_at"], lineage))
        surfaces = []
        for values in groups.values():
            cohorts = defaultdict(list)
            for value in values:
                snapshot = value[0]
                cohorts[(snapshot.spot, snapshot.spot_market_data_time,
                    snapshot.valuation_policy_sha256, snapshot.model_version)].append(value)
            ordered = sorted(cohorts.items(), key=lambda item: (-len(item[1]),
                -max(value[0].market_data_time for value in item[1]).timestamp(), str(item[0])))
            values = ordered[0][1]
            excluded["SURFACE_OTHER_COHERENT_COHORT"] += sum(len(group) for _, group in ordered[1:])
            snapshots = tuple(value[0] for value in values)
            try:
                source = bind_surface_source(snapshots=snapshots, lineage=values[0][2],
                    security=securities[snapshots[0].underlyer],
                    recorded_at=max(value[1] for value in values), received_at=received_at,
                    valuation_policy=configuration.valuation_policy)
            except ValueError:
                excluded["SURFACE_SOURCE_INVALID"] += 1
                continue
            surfaces.append(dict(source=source, stock=stocks[snapshots[0].underlyer],
                market_cutoff=max(scheduled_cycle, values[0][2].market_time), decision_at=received_at))
        return dict(matrices=retained["matrices"], candidates=retained["candidates"],
            activity=tuple(activity), surface_inputs=tuple(surfaces), received_at=received_at,
            rejections=dict(sorted(excluded.items())))


def retained_contract_reference(row):
    from options.domain import AssetType, CatalogEligibility, OptionContractReference, validate_standard_contract

    if (row["eligibility_status"] != "VALIDATED_ACTIVE" or row["additional_underlyings"]
            or not isinstance(row["adjustment_metadata"], dict)
            or set(row["adjustment_metadata"]) - {"cfi", "correction"}):
        raise ValueError("technical packages require explicitly validated standard references")
    reference = OptionContractReference(contract_ticker=row["contract_ticker"], underlyer=row["underlying"],
        asset_type=AssetType(row["asset_type"]), provider=row["provider"], provider_version=row["provider_version"],
        provider_contract_type=row["provider_contract_type"], expiration_date=row["expiration_date"], strike=row["strike"],
        provider_exercise_style=row["provider_exercise_style"], shares_per_contract=row["shares_per_contract"],
        primary_exchange=row["primary_exchange"], correction=row["correction"], additional_underlyings_json="[]",
        adjustment_metadata_json=json.dumps(row["adjustment_metadata"], sort_keys=True, separators=(",", ":")),
        changes_deliverables=bool(row.get("changes_deliverables", False)), valid_from=row["valid_from"], valid_to=row["valid_to"],
        first_observed_at=row["first_observed_at"], revised_observed_at=row["revised_observed_at"],
        refreshed_at=row["refreshed_at"], payload_sha256=row["payload_sha256"])
    if validate_standard_contract(reference).eligibility_status != CatalogEligibility.VALIDATED_ACTIVE:
        raise ValueError("technical packages require explicitly validated standard references")
    return reference


def bind_structural_technical_source(row, bars, *, security, direction, spot, received_at):
    from options.alert_plans import TechnicalExitEvidence, TechnicalLevel, _canonical
    from equity.materialization import SETUP_VERSION
    from equity.polygon import sha256_json

    payload = row["payload"]
    if (row["source_version"] != SETUP_VERSION or row["security_id"] != security.security_id
            or row["direction"] != direction or sha256_json(payload) != row["payload_sha256"]
            or row["observed_at"] > received_at or row["created_at"] > received_at):
        raise ValueError("structural technical source identity or checksum mismatch")
    selected_bars = [bars[identity] for identity in row["feature_bar_ids"] if identity in bars]
    if (not selected_bars or len(selected_bars) != len(row["feature_bar_ids"])
            or any(bar["security_id"] != security.security_id or bar["ticker"] != security.ticker
                or bar["adjusted"] or bar["bar_end"] > row["market_time"] for bar in selected_bars)):
        raise ValueError("structural technical source lacks exact raw feature history")
    structural = {"Price Action", "Gap", "FVG", "Pattern", "Volume Pivot"}
    stops = [level for level in payload["stops"] if level.get("source") in structural
        and direction * (spot - Decimal(str(level["price"]))) > 0]
    targets = [level for level in payload["targets"] if level.get("source") in structural
        and direction * (Decimal(str(level["price"])) - spot) > 0]
    if not stops or not targets:
        raise ValueError("structural stop or target unavailable; no ATR-only substitute")
    stop = min(stops, key=lambda level: direction * (spot - Decimal(str(level["price"]))))
    levels = [TechnicalLevel(level_id="structural-stop", role="INVALIDATION", price=Decimal(str(stop["price"])))]
    levels.extend(TechnicalLevel(level_id=f"structural-target-{index}", role="OPPOSING_STRUCTURE", price=Decimal(str(level["price"])))
        for index, level in enumerate(targets))
    fib = payload.get("strategy_results", {}).get("fibonacci") or {}
    levels.extend(TechnicalLevel(level_id=f"fib-{index}", role="FIBONACCI", price=Decimal(str(level["price"])))
        for index, level in enumerate(fib.get("levels", ())) if level.get("price") is not None and Decimal(str(level["price"])) > 0)
    policy_hash = hashlib.sha256(_canonical(dict(version="retained_raw_structural_levels_v1", source_version=SETUP_VERSION,
        role="STRUCTURAL_CONTEXT_NOT_QUALIFIED_DIRECTION", levels="NEAREST_PRICE_ACTION_GAP_FVG_PATTERN_VOLUME_PIVOT")).encode("ascii")).hexdigest()
    return TechnicalExitEvidence(security_id=security.security_id, underlyer=security.ticker, direction=direction,
        source_kind="STRUCTURE_SNAPSHOT", source_policy_sha256=policy_hash, source_payload_sha256=row["payload_sha256"],
        source_revision_ids=tuple(row["feature_bar_ids"]), interval=row["interval"], market_time=row["market_time"],
        available_at=max(row["observed_at"], row["created_at"]), received_at=received_at, valid_until=row["valid_until"],
        atr=Decimal(str(payload["technicals"]["atr"])), levels=tuple(levels))


class ProductionDetectorSourceReader:
    def __init__(self, retained_reader, repository, corporate_action_repository, *, setup_reader, clock=None,
                 setup_shadow_reader=None, aligned_setup_windows=False, intraday_confirmation=False,
                 setup_wait_enabled=False):
        self.retained_reader = retained_reader
        self.repository = repository
        self.corporate_action_repository = corporate_action_repository
        self.setup_reader = setup_reader
        self.setup_shadow_reader = setup_shadow_reader
        self.aligned_setup_windows = aligned_setup_windows
        self.intraday_confirmation = intraday_confirmation
        self.setup_wait_enabled = setup_wait_enabled
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def __call__(self, *, configuration, dataset_id, scheduled_cycle, completed_matrices, started_at,
                 progress_callback=None):
        from options.alert_qualification import retained_candidate
        from options.alert_plans import TechnicalExitEvidence, TechnicalLevel
        from options.dual_origin import assess_options_first, assess_stock_first, StockFirstPolicy, StockResumptionPolicy
        from options.outcome_contracts import assess_option_package
        from options.calendar import OptionExchangeCalendar
        from equity.repositories import _bar_from_row

        retained = self.retained_reader.read(configuration=configuration, scheduled_cycle=scheduled_cycle,
            completed_matrices=completed_matrices, as_of=started_at)
        cutoff = self.clock()
        package_sources = self.repository.detector_package_sources(configuration=configuration,
            candidate_ids=tuple(row["candidate_id"] for row in retained["candidates"]), as_of=cutoff)
        activity = {row["snapshot"].snapshot_id: row for row in retained["activity"]}
        assessor = assess_options_first
        intraday_stocks = {}
        o1_observations = []
        if self.intraday_confirmation:
            from options.dual_origin import assess_options_intraday
            from options.intraday_participation import bind_intraday_components, build_o1_indicator_observation
            from options.detector_launch import aligned_stock_windows

            assessor = assess_options_intraday
            for matched in retained["activity"]:
                if matched["finding"].disposition != "DETECTED":
                    continue
                source = matched["source"]
                window = aligned_stock_windows({source.underlyer: source.market_time})[source.underlyer][1]
                key = (source.security_id, window)
                if key not in intraday_stocks:
                    carrier = self.repository.intraday_confirmation_source(security_id=source.security_id,
                        market_cutoff=window or source.market_time, as_of=cutoff)
                    intraday_stocks[key] = bind_intraday_components(carrier, received_at=self.clock()) if carrier else None
                matched["intraday_stock"] = intraday_stocks[key]
            cutoff = self.clock()
            for matched in retained["activity"]:
                if matched["finding"].disposition != "DETECTED":
                    continue
                direction = 1 if matched["snapshot"].contract_type.value == "CALL" else -1
                decision = assessor(matched["source"], matched.get("intraday_stock"), direction=direction,
                    market_cutoff=max(scheduled_cycle, matched["lineage"].market_time), decision_at=cutoff)
                if decision.activity is not None and decision.activity.disposition == "DETECTED":
                    o1_observations.append(build_o1_indicator_observation(matched["source"], decision,
                        matched.get("intraday_stock"), scheduled_cycle=scheduled_cycle))
        technical_underlyers = set()
        for leg in package_sources["legs"]:
            matched = activity.get(leg["snapshot_id"])
            if leg["side"] != "BUY" or matched is None or matched["finding"].disposition != "DETECTED":
                continue
            decision = assessor(matched["source"], matched.get("intraday_stock") if self.intraday_confirmation else matched["stock"],
                direction=1 if matched["snapshot"].contract_type.value == "CALL" else -1,
                market_cutoff=max(scheduled_cycle, matched["lineage"].market_time), decision_at=cutoff)
            if decision.disposition == "CONFIRMED":
                technical_underlyers.add(matched["snapshot"].underlyer)
        technical_sources = self.repository.detector_technical_sources(underlyers=configuration.settings.underlyers,
            market_cutoff=max(scheduled_cycle, *(row["market_time"] for row in retained["matrices"])), as_of=cutoff,
            technical_underlyers=tuple(sorted(technical_underlyers)))
        setup_arguments = dict(market_cutoff=max(scheduled_cycle, *(row["market_time"] for row in retained["matrices"])), as_of=cutoff)
        setup_rejections = {}
        if self.aligned_setup_windows:
            setup_arguments["market_cutoffs"] = {row["underlying"]: max(scheduled_cycle, row["market_time"]) for row in retained["matrices"]}
            if self.setup_wait_enabled:
                setup_arguments["wait_deadline"] = max((row["finding"].valid_until for row in retained["activity"]
                    if row["finding"].disposition == "DETECTED"), default=cutoff)
                setup_arguments["progress_callback"] = progress_callback
            setups, setup_rejections = self.setup_reader(**setup_arguments)
        else:
            setups = self.setup_reader(**setup_arguments)
        shadow_arguments = {key: value for key, value in setup_arguments.items()
            if key not in {"wait_deadline", "progress_callback"}}
        if self.setup_wait_enabled:
            shadow_arguments["as_of"] = self.clock()
            if shadow_arguments["as_of"] < cutoff:
                raise ValueError("stock setup shadow receipt moved backwards after wait")
        setup_shadows, shadow_rejections = (self.setup_shadow_reader(**shadow_arguments)
            if self.setup_shadow_reader is not None else ((), {}))
        source_bars = {row["bar_revision_id"]: row for row in technical_sources["bars"]}
        coverage_cache = {}
        for technical_source in technical_sources["sources"]:
            history = [source_bars[identity] for identity in technical_source["feature_bar_ids"] if identity in source_bars]
            if not history or len(history) != len(technical_source["feature_bar_ids"]):
                continue
            first_day = min(bar["session_date"] for bar in history)
            key = (technical_source["ticker"], first_day, technical_source["market_time"].date())
            if key not in coverage_cache:
                coverage_cache[key] = self.corporate_action_repository.latest_covered_actions(key[0], "SPLIT",
                    window_start=key[1], window_end=key[2], observed_at=cutoff, maximum_age=timedelta(hours=24))
        received_at = self.clock()
        if received_at < cutoff:
            raise ValueError("package source receipt moved backwards")
        legs, references = defaultdict(list), defaultdict(list)
        for row in package_sources["legs"]:
            legs[row["candidate_id"]].append(row)
        for row in package_sources["references"]:
            references[row["contract_id"]].append(row)
        raw_bars = package_sources["raw_bars"]
        rejected = Counter(retained["rejections"])
        rejected.update(setup_rejections)
        rejected.update({key: value for key, value in shadow_rejections.items() if "SHADOW_SOURCE_UNAVAILABLE" in key})
        if technical_sources.get("source_error"):
            rejected[technical_sources["source_error"]] += 1
        packages = []
        stock_setup_observations = []
        if setup_shadows:
            from options.stock_setup_binding import build_stock_setup_indicator_observation

            detected_activity = tuple(row["source"] for row in retained["activity"]
                if row["finding"].disposition == "DETECTED")
            matrices_by_underlyer = {row["underlying"]: row["matrix_id"] for row in retained["matrices"]}
            for setup_shadow in setup_shadows:
                try:
                    stock_setup_observations.append(build_stock_setup_indicator_observation(setup_shadow,
                        detected_activity, scheduled_cycle=scheduled_cycle,
                        matrix_id=matrices_by_underlyer[setup_shadow.candidate.ticker], decision_at=received_at))
                except (KeyError, ValueError):
                    rejected[setup_shadow.detector_id + "_SHADOW_OBSERVATION_INVALID"] += 1
        calendar = OptionExchangeCalendar()
        for row in package_sources["candidates"]:
            candidate = retained_candidate(row, legs[row["candidate_id"]])
            long_leg = next((leg for leg in candidate.legs if leg.side.value == "BUY"), None)
            matched = activity.get(long_leg.snapshot_id) if long_leg else None
            if matched is None or matched["finding"].disposition != "DETECTED":
                rejected["MATCHED_PARTICIPATION_UNAVAILABLE"] += 1
                continue
            direction = 1 if long_leg.contract_type.value == "CALL" else -1
            try:
                snapshots = tuple(activity[leg.snapshot_id]["snapshot"] for leg in candidate.legs)
                refs = tuple(retained_contract_reference(next(ref for ref in references[leg.contract_id]
                    if ref.get("candidate_id", candidate.candidate_id) == candidate.candidate_id
                    and ref["valid_from"] <= candidate.market_data_time
                    and (ref["valid_to"] is None or ref["valid_to"] > candidate.market_data_time)
                    and max(ref["first_observed_at"], ref["revised_observed_at"] or ref["first_observed_at"]) <= candidate.observed_time)) for leg in candidate.legs)
                bars = tuple(next(bar for bar in raw_bars if bar["security_id"] == matched["security"].security_id
                    and bar["bar_end"] == snapshot.spot_market_data_time and bar["close_price"] == snapshot.spot) for snapshot in snapshots)
            except (KeyError, StopIteration, ValueError):
                rejected["EXACT_PACKAGE_REFERENCE_OR_RAW_SPOT_UNAVAILABLE"] += 1
                continue
            stock = matched.get("intraday_stock") if self.intraday_confirmation else matched["stock"]
            common = dict(source=matched["source"], candidate=candidate, stock=stock, security=matched["security"],
                lineage=matched["lineage"], snapshots=snapshots, references=refs,
                raw_bars=tuple(_bar_from_row(bar) for bar in bars), raw_bar_created_ats=tuple(bar["created_at"] for bar in bars),
                source_received_at=received_at, planned_entry_at=received_at + timedelta(seconds=1),
                entry_limit=-candidate.net_premium, valuation_policy=configuration.valuation_policy,
                package_assessment=assess_option_package(candidate, valuation_policy_sha256=configuration.valuation_policy_sha256),
                event_detail=dict(holding_event_evidence=technical_sources["events"],
                    event_coverage_evidence=technical_sources["coverage"], holding_event_coverage=technical_sources["coverage"]))
            decisions = []
            stock_decision = assessor(matched["source"], stock, direction=direction,
                market_cutoff=max(scheduled_cycle, matched["lineage"].market_time), decision_at=received_at)
            if stock_decision.disposition == "CONFIRMED":
                candidates = sorted((source for source in technical_sources["sources"] if source["ticker"] == candidate.underlyer
                    and source["direction"] == direction and source["market_time"] <= stock_decision.market_cutoff), key=lambda source: source["market_time"], reverse=True)
                for source in candidates[:1]:
                    try:
                        technical = bind_structural_technical_source(source, source_bars, security=matched["security"],
                            direction=direction, spot=long_leg.spot, received_at=received_at)
                        first_day = min(source_bars[identity]["session_date"] for identity in source["feature_bar_ids"])
                        key = (candidate.underlyer, first_day, source["market_time"].date())
                        coverage, actions = coverage_cache[key]
                        if coverage is None or actions or coverage.get("created_at", cutoff) > cutoff:
                            raise ValueError("raw technical history split coverage unavailable or crossed")
                        decisions.append((stock_decision, technical, {}))
                    except ValueError:
                        rejected["O1_TECHNICAL_SOURCE_UNAVAILABLE"] += 1
                if not candidates:
                    rejected["O1_TECHNICAL_SOURCE_UNAVAILABLE"] += 1
            else:
                rejected["O1_STOCK_CONFIRMATION_UNAVAILABLE"] += 1
                if self.intraday_confirmation:
                    rejected.update("O1_" + reason for reason in stock_decision.reasons)
            for setup in setups:
                if setup.candidate.ticker != candidate.underlyer or setup.candidate.direction != direction:
                    continue
                policy_type = StockResumptionPolicy if setup.expected_model == "resumption" else StockFirstPolicy
                policy = policy_type(source_policy_sha256=setup.source_policy.sha256)
                decision = assess_stock_first(setup, matched["source"], policy=policy, trusted_source_policy=setup.source_policy,
                    market_cutoff=max(scheduled_cycle, matched["lineage"].market_time), decision_at=received_at)
                if decision.disposition != "CONFIRMED":
                    rejected[decision.detector_id + "_CONFIRMATION_UNAVAILABLE"] += 1
                    continue
                technical = TechnicalExitEvidence(security_id=matched["security"].security_id, underlyer=candidate.underlyer,
                    direction=direction, source_kind="STOCK_SETUP", source_policy_sha256=setup.source_policy.sha256,
                    source_payload_sha256=setup.sha256, source_revision_ids=setup.candidate.revision_ids, interval="1h",
                    market_time=setup.source.market_time, available_at=setup.source.observed_at,
                    received_at=setup.source.received_at, valid_until=setup.source.valid_until,
                    atr=Decimal(str(setup.candidate.activation_atr)), levels=(
                        TechnicalLevel(level_id="original-stop", role="INVALIDATION", price=Decimal(str(setup.candidate.stop))),
                        TechnicalLevel(level_id="original-target", role="OPPOSING_STRUCTURE", price=Decimal(str(setup.candidate.target)))))
                decisions.append((decision, technical, dict(setup=setup, trusted_source_policy=setup.source_policy, stock_first_policy=policy)))
            for decision, technical, extra in decisions:
                deadline = min(candidate.valid_until, decision.valid_until, technical.valid_until) - timedelta(seconds=1)
                exit_at = min(received_at + timedelta(hours=2), calendar.session_close(candidate.market_data_time.date()))
                if not common["planned_entry_at"] <= deadline < exit_at:
                    rejected["ORIGINAL_ENTRY_WINDOW_UNAVAILABLE"] += 1
                    continue
                events = [event for event in technical_sources["events"]
                    if event["affected_underlying"] in (None, candidate.underlyer) and event["status"] != "CANCELED"
                    and received_at <= event["scheduled_time"] <= exit_at]
                if events:
                    rejected["KNOWN_EVENT_IN_HOLDING_WINDOW"] += 1
                    continue
                packages.append(dict(common, **extra, decision=decision, entry_deadline=deadline, exit_deadline=exit_at,
                    technical_evidence=technical, technical_source_policy_sha256=technical.source_policy_sha256))
        return DetectorCycleInputs(matrices=retained["matrices"], package_inputs=tuple(packages),
            o1_observations=tuple(o1_observations),
            stock_setup_observations=tuple(stock_setup_observations),
            surface_inputs=retained["surface_inputs"], rejections=tuple(sorted(rejected.items())), reject_invalid_packages=True)


class DetectorCycleCollector:
    def __init__(self, source_reader: Callable, evaluation_repository, *, clock=None):
        self.source_reader = source_reader
        self.evaluation_repository = evaluation_repository
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def __call__(self, *, configuration, dataset_id, scheduled_cycle, completed_matrices, started_at,
                 progress_callback=None):
        if (started_at.utcoffset() is None or scheduled_cycle.utcoffset() is None
                or scheduled_cycle > started_at
                or set(completed_matrices) != set(configuration.settings.underlyers)):
            raise ValueError("detector collection requires a complete causal configured cycle")
        inputs = self.source_reader(configuration=configuration, dataset_id=dataset_id,
            scheduled_cycle=scheduled_cycle, completed_matrices=completed_matrices, started_at=started_at,
            progress_callback=progress_callback)
        if not isinstance(inputs, DetectorCycleInputs):
            raise ValueError("detector source reader must return bounded cycle inputs")
        if (len(inputs.package_inputs) + len(inputs.o1_observations) + len(inputs.stock_setup_observations)
            + len(inputs.surface_inputs) > 5000
                or len(inputs.matrices) != len(completed_matrices)
                or {row["underlying"]: row["matrix_id"] for row in inputs.matrices} != completed_matrices):
            raise ValueError("detector collection input bound or source matrix scope mismatch")
        rejected = Counter()
        for reason, count in inputs.rejections:
            if not reason or type(count) is not int or count < 0:
                raise ValueError("invalid detector source exclusion count")
            rejected[reason] += count
        packages, plans, observations = [], {}, []
        for source in inputs.package_inputs:
            try:
                package, plan = qualify_dual_origin_package(**source)
            except ValueError:
                if not inputs.reject_invalid_packages:
                    raise
                rejected["PACKAGE_QUALIFICATION_REJECTED"] += 1
                continue
            if package.decision_at < started_at:
                raise ValueError("detector package decision predates current collection")
            packages.append(package)
            plans[plan.sha256] = plan
        for source in inputs.surface_inputs:
            observation = assess_surface_first(**source)
            if observation.decision_at < started_at:
                raise ValueError("surface decision predates current collection")
            observations.append(observation)
        selected_at = self.clock()
        if selected_at.utcoffset() is None or selected_at < started_at:
            raise ValueError("detector selection clock precedes collection")
        prior = self.evaluation_repository.prior_selected(dataset_id=dataset_id,
            scheduled_cycle=scheduled_cycle, as_of=selected_at)
        prior_o1 = (self.evaluation_repository.prior_o1_observed(dataset_id=dataset_id,
            scheduled_cycle=scheduled_cycle, as_of=selected_at)
            if hasattr(self.evaluation_repository, "prior_o1_observed") else set())
        new_o1 = tuple(row for row in inputs.o1_observations if row.recurrence_sha256 not in prior_o1)
        repeat_o1 = len(inputs.o1_observations) - len(new_o1)
        if repeat_o1:
            rejected["O1_REPEAT_OBSERVATION"] += repeat_o1
        prior_setups = (self.evaluation_repository.prior_stock_setup_observed(dataset_id=dataset_id,
            scheduled_cycle=scheduled_cycle, as_of=selected_at)
            if hasattr(self.evaluation_repository, "prior_stock_setup_observed") else set())
        new_setups = tuple(row for row in inputs.stock_setup_observations
            if row.recurrence_sha256 not in prior_setups)
        repeat_setups = len(inputs.stock_setup_observations) - len(new_setups)
        if repeat_setups:
            rejected["STOCK_SETUP_REPEAT_OBSERVATION"] += repeat_setups
        selection = select_dual_origin_packages(packages, prior, decision_at=selected_at,
            scheduled_cycle=scheduled_cycle, expected_underlyers=configuration.settings.underlyers,
            completed_matrices=completed_matrices)
        records = (*build_selection_evidence(selection, plans, dataset_id=dataset_id, selected_at=selected_at),
            *build_o1_indicator_evidence(new_o1, dataset_id=dataset_id, selected_at=selected_at),
            *build_stock_setup_indicator_evidence(new_setups, dataset_id=dataset_id, selected_at=selected_at),
            *build_surface_evidence(observations, dataset_id=dataset_id, selected_at=selected_at))
        rejected.update(selection["evidence"]["rejections"])
        run = build_detector_run(configuration=configuration, dataset_id=dataset_id,
            scheduled_cycle=scheduled_cycle, selected_at=selected_at, matrices=inputs.matrices,
            records=records, rejections=rejected)
        return run, records