from __future__ import annotations

import hashlib
from collections import Counter, defaultdict, deque
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import ClassVar, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, Name, Sha256
from options.dual_origin import QualifiedDualOriginPackage, QualifiedResumptionPackage, QualifiedTechnicalPackage, load_qualified_package
from options.surface_detection import SurfaceDecision

from options.strategies.domain import StructureType, canonical_json, signal_identity_sha256
from options.strategies.registry import REGISTRY_BY_NAME


BASELINE_MODELS = {
    "INCOME_WHEEL": "Cash-secured put baseline",
    "SPREAD_RANGE_LOCATOR": "OI structure baseline",
    "DIRECTIONAL_LONG_PREMIUM": "Implied-move long premium baseline",
    "DIRECTIONAL_DEBIT_SPREAD": "Implied-move debit spread baseline",
}
STRUCTURE_LANES = {
    "CASH_SECURED_PUT": (1, "BULLISH"),
    "LONG_CALL": (1, "BULLISH"), "LONG_PUT": (1, "BEARISH"),
    "CALL_DEBIT_VERTICAL": (2, "BULLISH"), "PUT_DEBIT_VERTICAL": (2, "BEARISH"),
    "PUT_CREDIT_VERTICAL": (2, "BULLISH"), "CALL_CREDIT_VERTICAL": (2, "BEARISH"),
    "IRON_CONDOR": (4, "RANGE"),
    "CALL_BUTTERFLY": (3, "RANGE"), "PUT_BUTTERFLY": (3, "RANGE"),
}
BASELINE_SELECTOR_VERSION = "option_alert_baseline_selector_v1"
BASELINE_SELECTOR_POLICY = {
    "version": BASELINE_SELECTOR_VERSION,
    "models": BASELINE_MODELS,
    "structure_lanes": STRUCTURE_LANES,
    "maximum_new_alerts": 50,
    "maximum_expiry_days": 60,
    "lane": ["strategy_name", "underlying", "direction"],
    "allocation": "MODEL_ROUND_ROBIN_RANK_UNDERLYING_CANDIDATE_ID",
    "repeat": "MODEL_VERSION_POLICY_EXACT_SORTED_LEGS_UNTIL_FIRST_LEG_EXPIRY",
    "repeat_observation": "ONCE_PER_COMPLETE_RUN_ALL_QUALIFYING_PREVIOUS_ALERTS",
    "admission": "SELECTED_COMPLETE_CAUSAL_PACKAGE_KIND_CONTEXT_DIRECTION_PASS_BEFORE_ORIGINAL_DEADLINE",
    "required_gate_ledger": "gate_ledger_v2",
    "non_executable_gates": ["QUOTE_LIQUIDITY", "RISK_ENGINE", "READ_ONLY_MODE"],
    "execution_allowed": False,
}
BASELINE_SELECTOR_SHA256 = hashlib.sha256(
    canonical_json(BASELINE_SELECTOR_POLICY).encode("ascii")
).hexdigest()


def _time(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("alert clocks must be timezone-aware")
    return result


def baseline_package_identity(row):
    legs = tuple(sorted(
        (int(leg["contract_id"]), leg["side"], int(leg["ratio"]), int(leg["multiplier"]))
        for leg in row["legs"]
    ))
    return signal_identity_sha256(
        row["underlying"], row["strategy_name"], row["strategy_version"],
        row["policy_sha256"], StructureType(row["structure_type"]), legs,
    )


def _rejection(row, decision_at):
    if row.get("strategy_name") not in BASELINE_MODELS:
        return "MODEL_NOT_ENABLED"
    if row.get("status") != "SELECTED":
        return "NOT_SELECTED"
    if not row.get("gate_ledger_complete"):
        return "GATE_LEDGER_INCOMPLETE"
    if not row.get("baseline_gates_pass"):
        return "BASELINE_GATES_NOT_PASSED"
    try:
        if StructureType(row["structure_type"]) not in REGISTRY_BY_NAME[row["strategy_name"]].allowed_structure_types:
            return "MODEL_STRUCTURE_MISMATCH"
        if not _time(row["market_data_time"]) <= _time(row["observed_time"]) <= decision_at:
            return "NONCAUSAL_CANDIDATE"
        if decision_at >= _time(row["valid_until"]):
            return "ENTRY_DEADLINE_ELAPSED"
        expires_at = _time(row["expires_at"])
        if not decision_at < expires_at or (expires_at - decision_at).total_seconds() > 60 * 86400:
            return "EXPIRY_OUTSIDE_POLICY"
        expected_count, _ = STRUCTURE_LANES[row["structure_type"]]
        legs = row["legs"]
        if len(legs) != expected_count or len({leg["contract_id"] for leg in legs}) != len(legs):
            return "PACKAGE_INCOMPLETE"
        premium = Decimal(0)
        for leg in legs:
            if leg["side"] not in ("BUY", "SELL"):
                return "PACKAGE_INVALID"
            for field in ("contract_id", "ratio", "multiplier"):
                value = Decimal(str(leg[field]))
                if not value.is_finite() or value <= 0 or value != value.to_integral_value():
                    return "PACKAGE_INVALID"
            mark = Decimal(str(leg["model_mark"]))
            if not mark.is_finite() or mark <= 0:
                return "PACKAGE_INVALID"
            if _time(leg["source_market_time"]) > _time(row["observed_time"]):
                return "NONCAUSAL_LEG"
            premium += (1 if leg["side"] == "SELL" else -1) * mark * int(leg["ratio"]) * int(leg["multiplier"])
        retained_premium = Decimal(str(row["net_premium"]))
        if not retained_premium.is_finite() or abs(premium - retained_premium) > Decimal("0.00000002"):
            return "PACKAGE_PREMIUM_MISMATCH"
        rank = Decimal(str(row["candidate_rank"]))
        if not rank.is_finite() or rank < 1 or rank != rank.to_integral_value():
            return "RANK_INVALID"
        baseline_package_identity(row)
    except (KeyError, ValueError, TypeError, InvalidOperation, OverflowError):
        return "PACKAGE_INVALID"
    return None


def select_baseline_alerts(rows, prior_alerts, *, decision_at):
    decision_at = _time(decision_at)
    rejected = Counter()
    by_model = defaultdict(Counter)
    qualified = []
    for original in rows:
        model = original.get("strategy_name", "UNKNOWN")
        by_model[model]["input"] += 1
        reason = _rejection(original, decision_at)
        if reason:
            rejected[reason] += 1
            by_model[model]["quality_rejected"] += 1
            continue
        row = dict(original)
        row["alert_identity"] = baseline_package_identity(row)
        row["direction"] = STRUCTURE_LANES[row["structure_type"]][1]
        qualified.append(row)
    qualified.sort(key=lambda row: (int(row["candidate_rank"]), row["underlying"], str(row["candidate_id"])))
    seen, lanes, observations = set(), set(), []
    queues = defaultdict(deque)
    for row in qualified:
        identity = row["alert_identity"]
        if identity in seen:
            rejected["DUPLICATE_PACKAGE_IN_RUN"] += 1
            by_model[row["strategy_name"]]["duplicate_packages"] += 1
            continue
        seen.add(identity)
        prior = prior_alerts.get(identity)
        if prior is not None:
            if decision_at < _time(prior["expires_at"]):
                observations.append({
                    "alert_identity": identity,
                    "first_candidate_id": str(prior["candidate_id"]),
                    "candidate_id": str(row["candidate_id"]),
                })
                by_model[row["strategy_name"]]["repeats"] += 1
            else:
                rejected["PRIOR_ALERT_EXPIRED"] += 1
            continue
        lane = (row["strategy_name"], row["underlying"], row["direction"])
        if lane in lanes:
            rejected["LOWER_RANK_SAME_LANE"] += 1
            by_model[row["strategy_name"]]["lane_excluded"] += 1
            continue
        lanes.add(lane)
        queues[row["strategy_name"]].append(row)
    members = []
    while any(queues.values()) and len(members) < BASELINE_SELECTOR_POLICY["maximum_new_alerts"]:
        for model in sorted(queues):
            if queues[model] and len(members) < BASELINE_SELECTOR_POLICY["maximum_new_alerts"]:
                members.append(queues[model].popleft())
                by_model[model]["new_alerts"] += 1
    rejected["RUN_CAP"] += sum(len(queue) for queue in queues.values())
    for model, queue in queues.items():
        by_model[model]["cap_excluded"] = len(queue)
    positions = Counter()
    for row in members:
        lane = (row["underlying"], row["strategy_name"], row["candidate_kind"])
        positions[lane] += 1
        row["board_position"] = positions[lane]
    return {
        "members": members,
        "observations": observations,
        "evidence": {
            "input_candidates": len(rows), "new_alerts": len(members),
            "repeat_hits": len(observations), "rejections": dict(sorted(rejected.items())),
            "by_model": {model: dict(counts) for model, counts in sorted(by_model.items())},
            "execution_allowed": False,
        },
    }


DUAL_ORIGIN_SELECTOR_POLICY_V1 = {
    "version": "dual_origin_alert_selector_v1",
    "maximum_new_alerts": 50,
    "maximum_inputs": 5000,
    "maximum_prior_alerts": 100000,
    "lane": ["detector_id", "underlyer", "direction"],
    "allocation": "DETECTOR_ROUND_ROBIN_RANK_UNDERLYING_CANDIDATE_ID",
    "membership": "COMPLETE_CONFIGURED_CYCLE_QUALIFIED_INDICATIVE_ONLY",
    "matrix_scope": "EXACT_MATRIX_PER_UNDERLYING_SINGLE_SCHEDULED_CYCLE",
    "repeat": "EXACT_DETECTOR_CONFIRMATION_MANAGEMENT_PACKAGE_UNTIL_EXPIRY",
    "evidence_mode": "PROSPECTIVE_RECEIPT",
    "execution_allowed": False,
}
DUAL_ORIGIN_SELECTOR_V1_SHA256 = hashlib.sha256(canonical_json(DUAL_ORIGIN_SELECTOR_POLICY_V1).encode("ascii")).hexdigest()
DUAL_ORIGIN_SELECTOR_POLICY_V2 = {
    **DUAL_ORIGIN_SELECTOR_POLICY_V1,
    "version": "dual_origin_alert_selector_v2",
    "maximum_new_alerts": 20,
    "evaluation_pool": "ALL_DISTINCT_QUALIFIED_PACKAGES_AT_SELECTION_TIME",
    "overflow": "RETAIN_RUN_CAP_AND_LANE_EXCLUSIONS_NOT_ALERT_HITS",
}
DUAL_ORIGIN_SELECTOR_V2_SHA256 = hashlib.sha256(canonical_json(DUAL_ORIGIN_SELECTOR_POLICY_V2).encode("ascii")).hexdigest()
DUAL_ORIGIN_SELECTOR_POLICY = {
    **DUAL_ORIGIN_SELECTOR_POLICY_V2,
    "version": "dual_origin_alert_selector_v3",
    "package_detectors": ["O1", "S1", "S2"],
    "observation_only_detectors": ["O2"],
}
DUAL_ORIGIN_SELECTOR_SHA256 = hashlib.sha256(canonical_json(DUAL_ORIGIN_SELECTOR_POLICY).encode("ascii")).hexdigest()


def select_dual_origin_packages(packages, prior_alerts, *, decision_at, scheduled_cycle, expected_underlyers, completed_matrices):
    from options.dual_origin import QualifiedDualOriginPackage

    decision_at = _time(decision_at)
    scheduled_cycle = _time(scheduled_cycle)
    expected, completed = tuple(expected_underlyers), tuple(completed_matrices)
    if (not 1 <= len(expected) <= 13 or len(expected) != len(set(expected))
            or len(set(completed_matrices.values())) != len(completed_matrices) or scheduled_cycle > decision_at
            or len(packages) > DUAL_ORIGIN_SELECTOR_POLICY["maximum_inputs"]
            or len(prior_alerts) > DUAL_ORIGIN_SELECTOR_POLICY["maximum_prior_alerts"]):
        raise ValueError("dual-origin selection requires distinct universe and bounded inputs")
    if set(completed) - set(expected):
        raise ValueError("completed universe contains unconfigured names")
    if set(completed) != set(expected):
        return dict(status="INCOMPLETE_UNIVERSE", members=[], observations=[], evaluation_rows=[],
            missing_underlyers=sorted(set(expected) - set(completed)), selector_sha256=DUAL_ORIGIN_SELECTOR_SHA256)
    rows = []
    for package in packages:
        if not isinstance(package, QualifiedDualOriginPackage):
            raise ValueError("only fully qualified dual-origin packages may enter alert selection")
        row = load_qualified_package(package)
        if row.underlyer not in expected:
            raise ValueError("qualified package is outside the configured universe")
        if row.matrix_id != completed_matrices[row.underlyer] or row.scheduled_cycle != scheduled_cycle:
            raise ValueError("qualified package does not belong to the exact completed cycle matrices")
        rows.append(row)
    rejected, seen, lanes = Counter(), set(), set()
    queues, by_model = defaultdict(deque), defaultdict(Counter)
    observations = []
    evaluation_rows = []
    for row in sorted(rows, key=lambda value: (value.candidate_rank, value.underlyer, str(value.candidate_id), value.sha256)):
        model = row.detector_id
        by_model[model]["input"] += 1
        if not row.decision_at <= decision_at < row.entry_deadline:
            rejected["ORIGINAL_ENTRY_DEADLINE_OR_DECISION_CLOCK"] += 1
            by_model[model]["rejected"] += 1
            continue
        identity = row.recurrence_sha256
        if identity in seen:
            rejected["DUPLICATE_PACKAGE_IN_RUN"] += 1
            by_model[model]["duplicates"] += 1
            continue
        seen.add(identity)
        prior = prior_alerts.get(identity)
        if prior is not None:
            if not isinstance(prior, QualifiedDualOriginPackage):
                raise ValueError("prior alert identity must resolve an original qualified package")
            prior = load_qualified_package(prior)
            if (prior.recurrence_sha256 != identity or prior.detector_id != row.detector_id
                    or prior.exposure_sha256 != row.exposure_sha256 or prior.expires_at != row.expires_at
                    or prior.decision_at >= row.decision_at):
                raise ValueError("prior alert identity, expiry or chronology mismatch")
            if decision_at < prior.expires_at:
                observations.append(dict(recurrence_sha256=identity, detector_id=model,
                    first_candidate_id=str(prior.candidate_id), candidate_id=str(row.candidate_id)))
                evaluation_rows.append(dict(package=row, selection_status="REPEAT", selection_reason="PRIOR_ALERT",
                    first_candidate_id=str(prior.candidate_id)))
                by_model[model]["repeat_hits"] += 1
            else:
                rejected["PRIOR_ALERT_EXPIRED"] += 1
            continue
        lane = (model, row.underlyer, row.direction)
        if lane in lanes:
            rejected["LOWER_RANK_SAME_DETECTOR_UNDERLYING_DIRECTION"] += 1
            by_model[model]["lane_excluded"] += 1
            evaluation_rows.append(dict(package=row, selection_status="NOT_SELECTED",
                selection_reason="LOWER_RANK_SAME_DETECTOR_UNDERLYING_DIRECTION", first_candidate_id=None))
            continue
        lanes.add(lane)
        queues[model].append(row)
    members = []
    while any(queues.values()) and len(members) < DUAL_ORIGIN_SELECTOR_POLICY["maximum_new_alerts"]:
        for model in sorted(queues):
            if queues[model] and len(members) < DUAL_ORIGIN_SELECTOR_POLICY["maximum_new_alerts"]:
                member = queues[model].popleft()
                members.append(member)
                evaluation_rows.append(dict(package=member, selection_status="SELECTED",
                    selection_reason="WITHIN_RUN_BUDGET", first_candidate_id=None))
                by_model[model]["new_alerts"] += 1
    for model, queue in queues.items():
        by_model[model]["cap_excluded"] = len(queue)
        rejected["RUN_CAP"] += len(queue)
        evaluation_rows.extend(dict(package=row, selection_status="NOT_SELECTED",
            selection_reason="RUN_CAP", first_candidate_id=None) for row in queue)
    evaluation_rows.sort(key=lambda record: (record["package"].detector_id, record["package"].underlyer,
        record["package"].candidate_rank, str(record["package"].candidate_id), record["package"].sha256))
    return dict(status="SELECTED_IN_MEMORY", members=members, observations=observations, evaluation_rows=evaluation_rows,
        selector_sha256=DUAL_ORIGIN_SELECTOR_SHA256,
        evidence=dict(input_packages=len(rows), new_alerts=len(members), repeat_hits=len(observations),
            evaluation_packages=len(evaluation_rows),
            qualified_not_selected=sum(row["selection_status"] == "NOT_SELECTED" for row in evaluation_rows),
            by_model={model: dict(counts) for model, counts in sorted(by_model.items())},
            rejections=dict(sorted(rejected.items())), execution_allowed=False))


class DetectorSelectionEvidence(Contract):
    schema_version: Literal["option_detector_selection_evidence_v1"] = "option_detector_selection_evidence_v1"
    dataset_id: Name
    run_id: UUID
    selector_sha256: Sha256 = DUAL_ORIGIN_SELECTOR_SHA256
    selected_at: AwareDatetime
    package: QualifiedDualOriginPackage | QualifiedResumptionPackage | QualifiedTechnicalPackage
    selection_status: Literal["SELECTED", "NOT_SELECTED", "REPEAT"]
    selection_reason: Literal["WITHIN_RUN_BUDGET", "RUN_CAP", "LOWER_RANK_SAME_DETECTOR_UNDERLYING_DIRECTION", "PRIOR_ALERT"]
    first_candidate_id: UUID | None = None
    plan_payload_text: str = Field(max_length=32768)
    evidence_mode: Literal["PROSPECTIVE_RECEIPT"] = "PROSPECTIVE_RECEIPT"
    publication_permission: Literal[False] = False

    @property
    def scheduled_cycle(self):
        return self.package.scheduled_cycle

    @property
    def detector_id(self):
        return self.package.detector_id

    @property
    def candidate_id(self):
        return self.package.candidate_id

    @property
    def matrix_id(self):
        return self.package.matrix_id

    @property
    def recurrence_sha256(self):
        return self.package.recurrence_sha256

    @property
    def evaluation_id(self):
        return uuid5(NAMESPACE_URL, f"option-detector-evaluation:{self.run_id}:{self.package.detector_id}:{self.package.recurrence_sha256}")

    @model_validator(mode="after")
    def validate_evidence(self):
        import json

        package = self.package
        expected_run = uuid5(NAMESPACE_URL, f"option-detector-evaluation-run:{self.dataset_id}:{package.scheduled_cycle.isoformat()}")
        reasons = {"SELECTED": {"WITHIN_RUN_BUDGET"}, "NOT_SELECTED": {"RUN_CAP", "LOWER_RANK_SAME_DETECTOR_UNDERLYING_DIRECTION"}, "REPEAT": {"PRIOR_ALERT"}}
        if (self.run_id != expected_run or self.selector_sha256 not in (DUAL_ORIGIN_SELECTOR_V2_SHA256, DUAL_ORIGIN_SELECTOR_SHA256)
                or self.selection_reason not in reasons[self.selection_status]
                or (self.selection_status == "REPEAT") != (self.first_candidate_id is not None)
                or not package.decision_at <= self.selected_at < package.entry_deadline):
            raise ValueError("selection evidence identity, reason or clock mismatch")
        if self.selector_sha256 == DUAL_ORIGIN_SELECTOR_V2_SHA256 and (package.detector_id == "S2" or isinstance(package, QualifiedTechnicalPackage)):
            raise ValueError("selector v2 did not admit S2 or technical packages")
        plan = json.loads(self.plan_payload_text)
        if (canonical_json(plan) != self.plan_payload_text
                or hashlib.sha256(self.plan_payload_text.encode("ascii")).hexdigest() != package.plan_sha256):
            raise ValueError("selection plan must retain its canonical original hash")
        expected = dict(version="dual_origin_indicative_plan_v2" if isinstance(package, QualifiedTechnicalPackage) else "dual_origin_indicative_plan_v1", candidate_id=str(package.candidate_id),
            candidate_identity_sha256=package.candidate_identity_sha256, matrix_id=str(package.matrix_id),
            detector_id=package.detector_id, decision_sha256=package.decision_sha256,
            handoff_sha256=package.handoff_sha256, source_basis_sha256=package.source_basis_sha256)
        if any(plan.get(key) != value for key, value in expected.items()):
            raise ValueError("selection plan does not match qualified package")
        for key in ("decision_at", "entry_deadline", "exit_deadline"):
            if _time(plan[key]) != getattr(package, key):
                raise ValueError("selection plan original clocks mismatch")
        if isinstance(package, QualifiedTechnicalPackage):
            from options.alert_plans import TECHNICAL_EXIT_POLICY, TechnicalExitEvidence, technical_exit_terms
            from options.calendar import OptionExchangeCalendar

            management = plan.get("management_policy", {})
            technical = management.get("technical_exit", {})
            evidence = TechnicalExitEvidence.model_validate(technical.get("evidence", {}))
            expected_technical = technical_exit_terms(evidence=evidence,
                trusted_source_policy_sha256=evidence.source_policy_sha256, security_id=evidence.security_id,
                underlyer=package.underlyer, direction=package.direction, stock_entry=Decimal(technical["underlying_entry"]),
                entry_debit=Decimal(plan["entry_limit"]), decision_at=package.decision_at, exit_deadline=package.exit_deadline,
                session_close=OptionExchangeCalendar().session_close(package.scheduled_cycle.date()))
            if (any(technical.get(key) != value for key, value in expected_technical.items())
                    or plan.get("qualification_policy_sha256") != package.qualification_policy_sha256
                    or plan.get("management_policy_version") != TECHNICAL_EXIT_POLICY.version
                    or Decimal(str(management.get("stop_loss_fraction"))) != Decimal(TECHNICAL_EXIT_POLICY.hard_stop_fraction)
                    or Decimal(str(management.get("take_profit_fraction"))) != Decimal(TECHNICAL_EXIT_POLICY.hard_profit_fraction)
                    or management.get("maximum_hold_seconds") != TECHNICAL_EXIT_POLICY.maximum_hold_seconds):
                raise ValueError("technical selection plan does not preserve its frozen policy and levels")
        if len(self.canonical_json().encode("ascii")) > 65536:
            raise ValueError("selection evidence payload exceeds bound")
        return self


def build_selection_evidence(selection, plans, *, dataset_id, selected_at):
    if selection.get("status") != "SELECTED_IN_MEMORY" or selection.get("selector_sha256") != DUAL_ORIGIN_SELECTOR_SHA256:
        raise ValueError("only a completed versioned selection can be retained")
    records = []
    for row in selection["evaluation_rows"]:
        package = row["package"]
        plan = plans.get(package.plan_sha256)
        if plan is None:
            raise ValueError("every evaluated alternative requires its original frozen plan")
        records.append(DetectorSelectionEvidence(dataset_id=dataset_id,
            run_id=uuid5(NAMESPACE_URL, f"option-detector-evaluation-run:{dataset_id}:{package.scheduled_cycle.isoformat()}"),
            selected_at=selected_at, package=package, selection_status=row["selection_status"],
            selection_reason=row["selection_reason"], first_candidate_id=row["first_candidate_id"],
            plan_payload_text=plan.payload_json))
    if (len({row.run_id for row in records}) > 1
            or len({row.evaluation_id for row in records}) != len(records)
            or {row.package.sha256 for row in records if row.selection_status == "SELECTED"}
                != {row.sha256 for row in selection["members"]}
            or len(records) != selection["evidence"]["evaluation_packages"]
            or sum(row.selection_status == "NOT_SELECTED" for row in records) != selection["evidence"]["qualified_not_selected"]
            or sum(row.selection_status == "REPEAT" for row in records) != len(selection["observations"])
            or len(selection["members"]) > DUAL_ORIGIN_SELECTOR_POLICY["maximum_new_alerts"]):
        raise ValueError("selection evaluation membership does not reconcile")
    return tuple(records)


class SurfaceEvaluationEvidence(Contract):
    schema_version: Literal["option_surface_evaluation_evidence_v1"] = "option_surface_evaluation_evidence_v1"
    dataset_id: Name
    run_id: UUID
    selector_sha256: Sha256 = DUAL_ORIGIN_SELECTOR_SHA256
    selected_at: AwareDatetime
    observation: SurfaceDecision
    recurrence_sha256: Sha256
    selection_status: Literal["OBSERVATION"] = "OBSERVATION"
    selection_reason: Literal["OBSERVATION_ONLY"] = "OBSERVATION_ONLY"
    evidence_mode: Literal["PROSPECTIVE_RECEIPT"] = "PROSPECTIVE_RECEIPT"
    publication_permission: Literal[False] = False

    @property
    def scheduled_cycle(self):
        return self.observation.scheduled_cycle

    @property
    def detector_id(self):
        return "O2"

    @property
    def candidate_id(self):
        return None

    @property
    def matrix_id(self):
        return self.observation.matrix_id

    @property
    def evaluation_id(self):
        return uuid5(NAMESPACE_URL, f"option-detector-evaluation:{self.run_id}:O2:{self.recurrence_sha256}")

    @model_validator(mode="after")
    def validate_evidence(self):
        expected = uuid5(NAMESPACE_URL, f"option-detector-evaluation-run:{self.dataset_id}:{self.scheduled_cycle.isoformat()}")
        if (self.run_id != expected or self.selector_sha256 != DUAL_ORIGIN_SELECTOR_SHA256
            or self.recurrence_sha256 != _surface_identity(self.observation)
                or self.observation.decision_at > self.selected_at or len(self.canonical_json().encode("ascii")) > 65536):
            raise ValueError("surface evaluation identity, clock or payload bound mismatch")
        return self


def _surface_identity(observation):
    return hashlib.sha256(canonical_json(dict(policy=observation.policy_sha256,
        security=str(observation.security_id), expiration=observation.expiration_date.isoformat(),
        contract_type=observation.contract_type)).encode("ascii")).hexdigest()


def build_surface_evidence(observations, *, dataset_id, selected_at):
    if len(observations) > 5000:
        raise ValueError("surface evaluation exceeds input bound")
    records = tuple(SurfaceEvaluationEvidence(dataset_id=dataset_id, selected_at=selected_at,
        run_id=uuid5(NAMESPACE_URL, f"option-detector-evaluation-run:{dataset_id}:{row.scheduled_cycle.isoformat()}"),
        recurrence_sha256=_surface_identity(row),
        observation=SurfaceDecision.model_validate_json(row.canonical_json())) for row in observations)
    if len({row.evaluation_id for row in records}) != len(records):
        raise ValueError("surface group must be evaluated once per run")
    return records


def load_evaluation_evidence(payload_text):
    import json

    payload = json.loads(payload_text)
    contracts = {"option_detector_selection_evidence_v1": DetectorSelectionEvidence,
        "option_surface_evaluation_evidence_v1": SurfaceEvaluationEvidence}
    contract = contracts.get(payload.get("schema_version"))
    if contract is None:
        raise ValueError("unsupported detector evaluation schema")
    return contract.model_validate_json(payload_text)


class DetectorRunEvidence(Contract):
    actual_matrix_watermarks: ClassVar[bool] = False
    schema_version: Literal["option_detector_run_v1"] = "option_detector_run_v1"
    dataset_id: Name
    scheduled_cycle: AwareDatetime
    selected_at: AwareDatetime
    configuration_sha256: Sha256
    strategy_policy_sha256: Sha256
    market_policy_sha256: Sha256
    strategy_version: Name
    selector_sha256: Sha256 = DUAL_ORIGIN_SELECTOR_SHA256
    expected_underlyers: tuple[Name, ...] = Field(min_length=1, max_length=13)
    source_matrices: tuple[tuple[Name, UUID], ...] = Field(min_length=1, max_length=13)
    market_time: AwareDatetime
    observed_time: AwareDatetime
    record_sha256s: tuple[tuple[UUID, Sha256], ...] = Field(max_length=5000)
    selection_counts: tuple[tuple[Literal["SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION"], int], ...]
    rejections: tuple[tuple[Name, int], ...] = ()
    evidence_mode: Literal["PROSPECTIVE_RECEIPT"] = "PROSPECTIVE_RECEIPT"
    execution_permission: Literal[False] = False

    @property
    def run_id(self):
        return uuid5(NAMESPACE_URL, f"option-detector-evaluation-run:{self.dataset_id}:{self.scheduled_cycle.isoformat()}")

    @property
    def scope_sha256(self):
        return hashlib.sha256(canonical_json(dict(version="option_detector_run_scope_v2" if self.actual_matrix_watermarks else "option_detector_run_scope_v1",
            dataset_id=self.dataset_id, selector_sha256=self.selector_sha256,
            configuration_sha256=self.configuration_sha256, strategy_policy_sha256=self.strategy_policy_sha256,
            market_policy_sha256=self.market_policy_sha256, strategy_version=self.strategy_version,
            underlyers=self.expected_underlyers)).encode("ascii")).hexdigest()

    @model_validator(mode="after")
    def validate_run(self):
        if self.selector_sha256 != DUAL_ORIGIN_SELECTOR_SHA256:
            raise ValueError("unsupported detector run selector")
        if tuple(sorted(set(self.expected_underlyers))) != self.expected_underlyers:
            raise ValueError("detector run universe must be distinct and sorted")
        if (tuple(name for name, _ in self.source_matrices) != self.expected_underlyers
                or len({matrix for _, matrix in self.source_matrices}) != len(self.source_matrices)):
            raise ValueError("detector run requires exact complete source matrices")
        if (self.scheduled_cycle > self.selected_at or not self.market_time <= self.observed_time <= self.selected_at
            or not self.actual_matrix_watermarks and self.market_time > self.scheduled_cycle):
            raise ValueError("detector run clocks must be causal")
        if (tuple(sorted(self.record_sha256s, key=lambda item: str(item[0]))) != self.record_sha256s
                or len({identity for identity, _ in self.record_sha256s}) != len(self.record_sha256s)):
            raise ValueError("detector run record identities must be sorted and unique")
        counts = dict(self.selection_counts)
        if (tuple(sorted(self.selection_counts)) != self.selection_counts or len(counts) != len(self.selection_counts)
                or set(counts) != {"SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION"}
                or any(type(value) is not int or value < 0 for value in counts.values())
                or counts["SELECTED"] > 20 or sum(counts.values()) != len(self.record_sha256s)):
            raise ValueError("detector run counts must reconcile with the global alert budget")
        if any(type(count) is not int or count < 0 for _, count in self.rejections):
            raise ValueError("detector run exclusions must be nonnegative")
        return self


class ForwardDetectorRunEvidence(DetectorRunEvidence):
    actual_matrix_watermarks: ClassVar[bool] = True
    schema_version: Literal["option_detector_run_v2"] = "option_detector_run_v2"
    watermark_basis: Literal["ACTUAL_RETAINED_MATRIX_WATERMARKS"] = "ACTUAL_RETAINED_MATRIX_WATERMARKS"


def load_detector_run(payload_text):
    import json

    contract = {"option_detector_run_v1": DetectorRunEvidence,
        "option_detector_run_v2": ForwardDetectorRunEvidence}.get(json.loads(payload_text).get("schema_version"))
    if contract is None:
        raise ValueError("unsupported detector run schema")
    return contract.model_validate_json(payload_text)


def build_detector_run(*, configuration, dataset_id, scheduled_cycle, selected_at, matrices, records, rejections):
    from collections import Counter

    counts = Counter(record.selection_status for record in records)
    contract = ForwardDetectorRunEvidence if getattr(configuration.strategy_policy, "forward_admission", None) is not None else DetectorRunEvidence
    run = contract(dataset_id=dataset_id, scheduled_cycle=scheduled_cycle, selected_at=selected_at,
        configuration_sha256=configuration.configuration_sha256, strategy_policy_sha256=configuration.strategy_policy_sha256,
        market_policy_sha256=configuration.policy_sha256, strategy_version=configuration.strategy_policy.strategy_version,
        expected_underlyers=tuple(sorted(configuration.settings.underlyers)),
        source_matrices=tuple(sorted((row["underlying"], row["matrix_id"]) for row in matrices)),
        market_time=max(row["market_time"] for row in matrices), observed_time=max(row["observed_time"] for row in matrices),
        record_sha256s=tuple(sorted(((row.evaluation_id, row.sha256) for row in records), key=lambda item: str(item[0]))),
        selection_counts=tuple(sorted((status, counts[status]) for status in ("SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION"))),
        rejections=tuple(sorted(rejections.items())))
    source_map = dict(run.source_matrices)
    for record in records:
        symbol = record.observation.underlyer if isinstance(record, SurfaceEvaluationEvidence) else record.package.underlyer
        if (record.dataset_id != run.dataset_id or record.run_id != run.run_id
                or record.selected_at != run.selected_at or record.selector_sha256 != run.selector_sha256
                or source_map.get(symbol) != record.matrix_id):
            raise ValueError("detector run record is not bound to its exact scope and source")
    return run