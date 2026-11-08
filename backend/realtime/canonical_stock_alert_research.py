"""Live Stock Alerts from the research equity rules (docs/EQUITY_MODEL_LAB_DESIGN.md section 6).

One batch per window receipt, as the placeholder breakout produced, through the same detector events, states,
alert rows and publication. Each rule in `research_rule_input.RESEARCH_RULES` is evaluated for each live stock on
every completed trigger bar of its interval, in order, with its research state kept per rule and stock. A member with
no rule due in a window (or an ETF, which serves as a benchmark) gets one NO_MATCH evaluation, because publication
requires every member to be evaluated. Only a MATCH becomes an alert row; UNAVAILABLE results stay evaluations.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import math
from pathlib import Path
from typing import Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

import exchange_calendars
import pandas as pd

from equity.model_detection.contracts import (
    Decision,
    Direction,
    Disposition,
    FeatureSnapshot,
    GateStatus,
    GateValue,
    ModelIdentity,
    Phase,
    Readiness,
    StateDecision,
    SuggestedGeometry,
    Transition,
    canonical_sha256 as model_sha256,
    canonical_value,
)
from research.equity_models.catalog import CATALOG

from . import research_rule_input as rri
from .canonical_stock_alert_product import (
    CANONICAL_STOCK_ALERT_PRODUCT,
    CANONICAL_STOCK_ALERT_SCHEMA,
    CANONICAL_STOCK_EVALUATION_SCHEMA,
    CanonicalStockAlertBatch,
    CanonicalStockAlertEvaluation,
    CanonicalStockAlertMembership,
    _product_members,
)
from .contracts import canonical_sha256
from .detector_adapters import StableDetectorAdapterResult
from .detector_evidence import (
    RealtimeDetectorInput,
    RealtimeDetectorState,
    RealtimeDetectorVersion,
    build_detector_event,
    build_detector_version,
)
from .lineage_head import inputs_sha256
from .portal_projection import build_alert_projection
from .screening_projection import RealtimeScreeningInput

UTC = timezone.utc
RESEARCH_STATE_SCHEMA = "research_rule_state_v1"
RESEARCH_CONTEXT_CONTRACT = "research-rule-context-v1"
RESEARCH_VERSION = "3-research"
_SOURCE_ROOT = Path(__file__).resolve().parents[1]
_CODE_PATHS = (
    _SOURCE_ROOT / "research/equity_models/catalog.py",
    _SOURCE_ROOT / "research/equity_models/data.py",
    _SOURCE_ROOT / "research/equity_models/detectors.py",
    _SOURCE_ROOT / "research/gics_sectors.py",
    _SOURCE_ROOT / "realtime/research_rule_input.py",
    Path(__file__),
)
_CALENDAR = exchange_calendars.get_calendar("XNYS")


def research_code_sha256(paths=_CODE_PATHS) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(Path(path).name.encode("ascii") + b"\0" + Path(path).read_bytes() + b"\0")
    return digest.hexdigest()


def rule_set_sha256() -> str:
    return canonical_sha256({
        "rules": [[rule, rri.rule_policy(rule).policy_hash] for rule in rri.RESEARCH_RULES],
        "macro_policy": rri.RESEARCH_MACRO_POLICY,
        "earnings_policy": rri.RESEARCH_EARNINGS_POLICY,
        "trigger_intervals": list(rri.TRIGGER_INTERVALS),
    })


def research_rule_version() -> RealtimeDetectorVersion:
    return build_detector_version(
        detector_id=CANONICAL_STOCK_ALERT_PRODUCT,
        version=RESEARCH_VERSION,
        code_sha256=research_code_sha256(),
        policy_sha256=rule_set_sha256(),
        requirements={
            "receipt": "INPUT_READY",
            "rules": list(rri.RESEARCH_RULES),
            "bars": {"30m": "RAW_UNADJUSTED", "1h": "RAW_UNADJUSTED full RTH hours", "1d": "ADJUSTED"},
            "history_sessions": dict(rri.HISTORY_SESSIONS),
            "macro_policy": rri.RESEARCH_MACRO_POLICY,
            "earnings_policy": rri.RESEARCH_EARNINGS_POLICY,
            "missing_context": "UNAVAILABLE",
            "publication": "MATCH_ONLY",
            "execution_permission": False,
        },
        output_kind="ALERT",
    )


@dataclass(frozen=True, slots=True)
class ResearchPriorState:
    revision: int
    research_state: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class ResearchInputs:
    """What the repository loads for one receipt: the research input and the members it covers."""
    data: rri.CachedResearchData
    members: tuple[rri.ResearchMember, ...]
    bar_sha256: Mapping[str, str]        # bar_revision_id -> payload_sha256
    summary: Mapping[str, object]


def _gate_value(value):
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return Decimal(str(round(float(value), 10)))
    return None


def _gates(result) -> tuple[GateValue, ...]:
    gates = []
    for gate in result.get("gates") or ():
        status = GateStatus(str(gate["status"]))
        gates.append(GateValue(str(gate["name"]), _gate_value(gate.get("value")),
                               _gate_value(gate.get("threshold")), status))
    return tuple(gates)


def entry_valid_until(policy, decided_at: datetime, session: str) -> datetime:
    """Swing: the next session's close. Intraday: the next confirmation boundary, never past today's close."""
    if policy.style == "SWING":
        following = _CALENDAR.session_offset(pd.Timestamp(session), 1)
        return _CALENDAR.session_close(following).to_pydatetime().astimezone(UTC)
    close = _CALENDAR.session_close(pd.Timestamp(session)).to_pydatetime().astimezone(UTC)
    step = {"30m": 1800, "1h": 3600}[policy.interval]
    return min(decided_at + timedelta(seconds=step), close)


def _decision(rule: str, result: Mapping, *, input_sha: str, key: str, revision: int,
              decided_at: datetime, session: str, error: str | None = None) -> Decision:
    policy = rri.rule_policy(rule)
    model = ModelIdentity(rule, policy.version, policy.style, policy.origin, policy.policy_hash)
    status = "ERROR" if error else str(result["status"])
    reasons = (error,) if error else tuple(str(reason) for reason in result.get("reasons") or ())
    readiness = (Readiness.ERROR if status == "ERROR" else Readiness.UNAVAILABLE if status == "UNAVAILABLE"
                 else Readiness.READY)
    features = FeatureSnapshot(policy.feature_version, input_sha, {}, readiness,
                               reasons if readiness is not Readiness.READY else ())
    state = StateDecision(key, revision, Phase.IDLE, Transition.HOLD, None, None, input_sha)
    if status == "MATCH":
        signal = result["signal"]
        direction = Direction(int(signal["direction"]))
        geometry = SuggestedGeometry(Decimal(str(signal["reference"])), Decimal(str(signal["stop"])),
                                     Decimal(str(signal["target"])), direction, decided_at,
                                     entry_valid_until(policy, decided_at, session))
        trigger = model_sha256({"schema": "research_rule_trigger_v1", "rule": rule,
                                "episode_id": signal["episode_id"]})
        return Decision(model, input_sha, features, Disposition.MATCH, _gates(result), direction, trigger, state,
                        geometry, ())
    disposition = {"NO_MATCH": Disposition.NO_MATCH, "UNAVAILABLE": Disposition.UNAVAILABLE}.get(
        status, Disposition.ERROR)
    gates = tuple(gate for gate in _gates(result)) if not error else ()
    return Decision(model, input_sha, features, disposition, gates, None, None, state, None, reasons)


def _context(member: rri.ResearchMember, data: rri.CachedResearchData, session: str, known_by: datetime):
    facts = sorted(
        ({"kind": row["kind"], "values": row["values"], "market_at": row["market_at"].isoformat()}
         for row in data.facts if row["security_id"] == member.security_id
         and str(row["market_at"].tz_convert("America/New_York").date()) < session <= str(
             row["valid_until"].tz_convert("America/New_York").date())),
        key=lambda item: item["kind"])
    document = {
        "contract": RESEARCH_CONTEXT_CONTRACT, "security_id": member.security_id, "ticker": member.ticker,
        "session": session, "sector_ticker": member.sector_ticker, "is_stock": member.is_stock,
        "facts": facts, "dataset_sha256": data.input_hash, "rule_set_sha256": rule_set_sha256(),
    }
    sha = canonical_sha256(document)
    source_id = str(uuid5(NAMESPACE_URL, f"research-rule-context:{sha}"))
    available = min(known_by, max((row["market_at"] for row in data.facts
                                   if row["security_id"] == member.security_id),
                                  default=pd.Timestamp(known_by)).to_pydatetime())
    return RealtimeDetectorInput(
        input_role="EQUITY_CONTEXT", input_key=source_id, source_kind="REFERENCE", input_sha256=sha,
        available_at=available, received_at=available,
        retained_input={"source": {"source_id": source_id, "payload_sha256": sha}, "document": document},
        source_record_id=UUID(source_id),
    ), source_id, sha


def _bar_input(row: Mapping, bar_sha256: Mapping[str, str]) -> RealtimeDetectorInput:
    return RealtimeDetectorInput(
        input_role="COMPLETED_BAR", input_key=str(row["source_id"]), source_kind="BAR_REVISION",
        input_sha256=bar_sha256[str(row["source_id"])],
        available_at=row["available_at"].to_pydatetime(), received_at=row["received_at"].to_pydatetime(),
        retained_input={"interval": row["interval"], "start": row["start"].isoformat(),
                        "market_at": row["market_at"].isoformat(), "close": row["close"]},
        bar_revision_id=UUID(str(row["source_id"])),
    )


def _alert(event, member: rri.ResearchMember, rule: str, decision: Decision, *, deliverable: bool,
           company_name, also_matched=(), session: str | None = None):
    """A MATCH is an alert; UNAVAILABLE and ERROR are non-actionable observation rows with their reason (the
    evaluations table requires a row for every result except NO_MATCH, as the placeholder rule did)."""
    if decision.disposition is Disposition.NO_MATCH:
        return None
    matched = decision.disposition is Disposition.MATCH and decision.geometry is not None
    policy = rri.rule_policy(rule) if rule in CATALOG else None
    geometry = decision.geometry if matched else None
    trigger = None if geometry is None else float(geometry.trigger)
    stop = None if geometry is None else float(geometry.stop)
    target = None if geometry is None else float(geometry.target)
    risk = None if trigger is None else abs(trigger - stop)
    lifecycle = "OPEN" if matched else "UNAVAILABLE"
    projection = {
        "contract": CANONICAL_STOCK_ALERT_SCHEMA, "security_id": member.security_id, "ticker": member.ticker,
        "company_name": company_name, "model": rule,
        "rule_family": None if policy is None else policy.family,
        "rule_variant": None if policy is None else policy.variant,
        "interval": None if policy is None else policy.interval,
        "trade_style": None if policy is None else policy.style,
        "setup_origin": None if policy is None else policy.origin,
        "suggested_hold": None if policy is None else f"{policy.horizon} {policy.horizon_unit}",
        "also_matched": list(also_matched),
        # Nov 8 (user): repeated matches of one stock, side and style in a session fold into the first alert,
        # with hits and the later matches as details (research.stock_alerts.collapse_alert_rows).
        "episode_group": (None if not matched or policy is None or session is None
                          else f"{session}:{member.security_id}:{int(decision.direction)}:{policy.style}"),
        "direction": 0 if decision.direction is None else int(decision.direction),
        "triggered_at": event.trigger_at.isoformat(), "published_at": event.decision_at.isoformat(),
        "entry_valid_until": None if geometry is None else geometry.valid_until.isoformat(),
        "trigger_price": trigger, "stop": stop, "target": target,
        "risk_pct": risk / trigger if trigger and risk is not None else None,
        "reward_risk": abs(target - trigger) / risk if risk else None,
        "status": lifecycle, "lifecycle": lifecycle, "lane": "TRADE" if matched else "OBSERVATION",
        "policy_version": decision.model.version, "policy_sha256": decision.model.policy_sha256,
        "feature_sha256": decision.features.feature_sha256, "decision_sha256": decision.decision_sha256,
        "state_key": decision.state.state_key, "state_revision": decision.state.revision,
        "reason_codes": list(decision.reason_codes), "gates": canonical_value(decision.gates),
        "reason": None if matched else ", ".join(decision.reason_codes),
        "indicators": {}, "indicator_status": "AVAILABLE" if matched else "UNAVAILABLE",
        "context": {"status": "AVAILABLE" if matched else "UNAVAILABLE",
                    "reason_codes": list(decision.reason_codes)},
        "warnings": (["Research rule under observation: not validated; not trading advice"] if matched
                     else [f"Research rule {rule} could not evaluate; no actionable alert"]),
        "non_actionable": not matched, "deliverable": deliverable and matched,
        "delivery_permission": False, "publication_permission": False, "execution_permission": False,
    }
    return build_alert_projection(event, alert_kind="STOCK", lifecycle=lifecycle,
                                  projection_schema=CANONICAL_STOCK_ALERT_SCHEMA, projection=projection,
                                  destinations=("PORTAL",))


def member_state_key(security_id: str) -> str:
    return model_sha256({"schema": RESEARCH_STATE_SCHEMA, "security_id": security_id})


def produce_research_rule_batch(
    inputs: RealtimeScreeningInput,
    research: ResearchInputs,
    *,
    window_id: UUID,
    input_receipt_id: UUID,
    receipt_input_sha256: str,
    membership: CanonicalStockAlertMembership,
    prior_states: Mapping[str, ResearchPriorState],
    deadline: datetime | None,
    detector_input_sha256: str,
    version: RealtimeDetectorVersion | None = None,
) -> CanonicalStockAlertBatch:
    """One evaluation per member per window (the evaluations table allows one per security and batch): every rule due
    for the member is evaluated, in rule order and bar order, and summarized in that one evaluation."""
    version = version or research_rule_version()
    deadline = None if deadline is None else deadline.astimezone(UTC)
    is_late = deadline is not None and inputs.recorded_by > deadline
    known_by = pd.Timestamp(inputs.recorded_by)
    session = inputs.session.isoformat()
    session_open = pd.Timestamp(_CALENDAR.session_open(pd.Timestamp(inputs.session)))
    members = {member.security_id: member for member in research.members}
    evaluations: list[CanonicalStockAlertEvaluation] = []
    for row in _product_members(inputs, membership):
        member = members[str(row["security_id"])]
        key = member_state_key(member.security_id)
        prior = prior_states.get(key)
        prior_rules = (prior.research_state.get("rules") if prior else None) or {}
        rules_state = {rule: dict(value) for rule, value in prior_rules.items()}
        results: list[tuple[str, Mapping, pd.Timestamp, str | None]] = []
        if member.is_stock:
            for rule in rri.RESEARCH_RULES:
                entry = rules_state.get(rule) or {}
                state = entry.get("research_state")
                # Only today's bars: the cohort's state starts at the session open; history is warmup only.
                after = (pd.Timestamp(entry["processed_until"]) if entry.get("processed_until")
                         else session_open - pd.Timedelta(microseconds=1))
                for at, bar_session in rri.due_bar_times(research.data, member, CATALOG[rule].interval,
                                                         after=after, until=known_by):
                    entry["processed_until"] = at.isoformat()
                    if bar_session != session:
                        continue
                    try:
                        result, error = rri.evaluate_rule(research.data, member, rule, at, bar_session, state), None
                    except Exception as failure:  # noqa: BLE001 - one rule never blocks another
                        result = {"status": "ERROR", "reasons": [], "gates": [], "input_ids": [], "state": state}
                        error = f"RESEARCH_RULE_ERROR:{type(failure).__name__}"
                    if error is None and result.get("state") is not None:
                        state = result["state"]
                    results.append((rule, result, at, error))
                entry["research_state"] = state
                rules_state[rule] = entry
        changed = bool(results) or rules_state != {rule: dict(value) for rule, value in prior_rules.items()}
        evaluations.append(_member_evaluation(
            inputs, research, member, row, results, rules_state, key=key,
            revision=(prior.revision if prior else 0) + (1 if changed else 0),
            context=_context(member, research.data, session, inputs.recorded_by), version=version,
            window_id=window_id, input_receipt_id=input_receipt_id, receipt_input_sha256=receipt_input_sha256,
            membership=membership, is_late=is_late, changed=changed))
    return CanonicalStockAlertBatch(
        cohort_id=inputs.cohort_id, window_id=window_id, input_receipt_id=input_receipt_id,
        session_date=inputs.session, cutoff=inputs.cutoff, receipt_input_sha256=receipt_input_sha256,
        detector_input_sha256=detector_input_sha256, membership=membership, version=version,
        evaluations=tuple(evaluations), completed_at=inputs.recorded_by, deadline=deadline, is_late=is_late,
    )


def _payload(inputs, member, rule, decision, *, window_id, input_receipt_id, receipt_input_sha256, membership,
             context_id, context_sha, is_late, research_document):
    return {
        "contract": CANONICAL_STOCK_EVALUATION_SCHEMA, "product": CANONICAL_STOCK_ALERT_PRODUCT,
        "rules_version": RESEARCH_VERSION, "rule": rule,
        "window_id": str(window_id), "input_receipt_id": str(input_receipt_id),
        "receipt_input_sha256": receipt_input_sha256, "screening_input_sha256": inputs_sha256(inputs),
        "desired_revision_id": (None if membership.desired_revision_id is None
                                else str(membership.desired_revision_id)),
        "desired_snapshot_sha256": membership.desired_snapshot_sha256,
        "membership_sha256": membership.membership_sha256,
        "security_id": member.security_id, "ticker": member.ticker,
        "context_source_record_id": context_id, "context_source_sha256": context_sha,
        "decision": canonical_value(decision), "research": research_document,
        "context_reason_codes": [],
        "observation_session": inputs.session.isoformat(), "observation_cutoff": inputs.cutoff.isoformat(),
        "completed_at": inputs.recorded_by.isoformat(), "late_catch_up": is_late, "deliverable": not is_late,
        "publication_permission": False, "execution_permission": False,
    }


def _trigger_bar(research, member, interval, decided_at):
    return max((bar for bar in research.data.bars_for(member.security_id, interval)
                if bar["received_at"] <= decided_at), key=lambda bar: bar["market_at"])


def _rule_summary(rule, result, at, error):
    item = {"rule": rule, "decision_at": at.isoformat(), "status": "ERROR" if error else result.get("status"),
            "reasons": [error] if error else list(result.get("reasons") or ())}
    if item["status"] == "MATCH":
        policy = rri.rule_policy(rule)
        signal = result["signal"]
        item.update(direction=signal["direction"], reference=signal["reference"], stop=signal["stop"],
                    target=signal["target"], episode_id=signal["episode_id"], style=policy.style,
                    interval=policy.interval, suggested_hold=f"{policy.horizon} {policy.horizon_unit}",
                    gates=result.get("gates") or [])
    return item


def _member_evaluation(inputs, research, member, row, results, rules_state, *, key, revision, context, version,
                       window_id, input_receipt_id, receipt_input_sha256, membership, is_late, changed):
    context_input, context_id, context_sha = context
    session = inputs.session.isoformat()
    rule_set = rule_set_sha256()
    summaries = [_rule_summary(rule, result, at, error) for rule, result, at, error in results]
    matches = sorted((item for item in results if item[3] is None and item[1].get("status") == "MATCH"),
                     key=lambda item: (item[2], rri.RESEARCH_RULES.index(item[0])))
    statuses = [("ERROR" if error else result.get("status")) for _rule, result, _at, error in results]
    input_sha = model_sha256({"schema": "research_rule_member_input_v1", "security_id": member.security_id,
                              "cutoff": inputs.cutoff.isoformat(), "dataset_sha256": research.data.input_hash,
                              "rules": [[item["rule"], item["decision_at"], item["status"]] for item in summaries]})
    if matches:
        rule, result, at, _error = matches[0]
        decided_at = at.to_pydatetime()
        decision = _decision(rule, result, input_sha=input_sha, key=key, revision=revision, decided_at=decided_at,
                             session=session)
        trigger_bar = _trigger_bar(research, member, CATALOG[rule].interval, at)
        primary = rule
    else:
        unavailable = sum(status == "UNAVAILABLE" for status in statuses)
        errors = sum(status == "ERROR" for status in statuses)
        if results and errors == len(statuses):
            disposition, readiness = Disposition.ERROR, Readiness.ERROR
            reasons = (f"RULE_ERRORS:{errors}",)
        elif results and unavailable + errors == len(statuses):
            disposition, readiness = Disposition.UNAVAILABLE, Readiness.UNAVAILABLE
            reasons = tuple(sorted({reason for item in summaries for reason in item["reasons"]}))[:6] or (
                "RULES_UNAVAILABLE",)
        elif results:
            disposition, readiness = Disposition.NO_MATCH, Readiness.READY
            reasons = (f"RULES_EVALUATED:{len(statuses)}", f"RULES_UNAVAILABLE:{unavailable}",
                       f"RULES_ERROR:{errors}")
        else:
            disposition, readiness = Disposition.NO_MATCH, Readiness.READY
            reasons = ("NO_RULE_DUE" if member.is_stock else "BENCHMARK_ONLY",)
        model = ModelIdentity("research-rule-set", RESEARCH_VERSION, "MIXED", "WINDOW", rule_set)
        features = FeatureSnapshot("research-rule-set", input_sha, {}, readiness,
                                   reasons if readiness is not Readiness.READY else ())
        decision = Decision(model, input_sha, features, disposition, (), None, None,
                            StateDecision(key, revision, Phase.IDLE, Transition.HOLD, None, None, input_sha), None,
                            reasons)
        if results:
            latest = max(results, key=lambda item: item[2])
            decided_at = latest[2].to_pydatetime()
            trigger_bar = _trigger_bar(research, member, CATALOG[latest[0]].interval, latest[2])
        else:
            decided_at, trigger_bar = inputs.recorded_by, None
        primary = "research-rule-set"
    adapter = StableDetectorAdapterResult(
        adapter_id=CANONICAL_STOCK_ALERT_PRODUCT, cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
        input_sha256=input_sha,
        status="UNAVAILABLE" if decision.disposition in {Disposition.UNAVAILABLE, Disposition.ERROR} else "EXECUTED",
        output=decision, reason_codes=decision.reason_codes, publication_permission=False, execution_permission=False)
    bars = {}
    for rule, _result, at, _error in results:
        bar = _trigger_bar(research, member, CATALOG[rule].interval, at)
        bars[str(bar["source_id"])] = bar
    also = [_rule_summary(rule, result, at, None) for rule, result, at, _e in matches[1:]]
    event = build_detector_event(
        adapter, version, instrument_id=int(member.instrument_id),
        trigger_identity=decision.trigger_identity or model_sha256({
            "kind": "research-rule-member", "state_key": key, "revision": revision, "input": input_sha}),
        trigger_at=inputs.cutoff if trigger_bar is None else trigger_bar["market_at"].to_pydatetime(),
        decision_at=decided_at, valid_until=decision.valid_until, disposition=decision.disposition.value,
        direction=None if decision.direction is None else int(decision.direction),
        reason_codes=decision.reason_codes, readiness_epoch=1, payload_schema=CANONICAL_STOCK_EVALUATION_SCHEMA,
        payload=_payload(inputs, member, primary, decision, window_id=window_id, input_receipt_id=input_receipt_id,
                         receipt_input_sha256=receipt_input_sha256, membership=membership, context_id=context_id,
                         context_sha=context_sha, is_late=is_late,
                         research_document={"rules": summaries, "also_matched": also}),
        inputs=(*(_bar_input(bar, research.bar_sha256) for bar in bars.values()), context_input),
    )
    state = None
    if changed and rules_state:
        payload = {"security_id": member.security_id, "ticker": member.ticker, "revision": revision,
                   "rules": rules_state}
        state = RealtimeDetectorState(
            detector_sha256=version.detector_sha256, cohort_id=inputs.cohort_id, state_key=key, revision=revision,
            state_schema=RESEARCH_STATE_SCHEMA, state_sha256=canonical_sha256(payload), state_payload=payload,
            updated_at=inputs.cutoff)
    return CanonicalStockAlertEvaluation(
        security_id=UUID(member.security_id), instrument_id=int(member.instrument_id), ticker=member.ticker,
        input_sha256=input_sha, decision=decision, event=event, state=state,
        alert=_alert(event, member, primary, decision, deliverable=not is_late, company_name=row.get("company_name"),
                     also_matched=also, session=session))


def research_members(product_rows: Sequence[Mapping[str, object]]) -> tuple[rri.ResearchMember, ...]:
    """Live members with their sector benchmark when that ETF is itself a member (the live product's mapping)."""
    from research.gics_sectors import SECTOR_BENCHMARK_ETF, resolve_benchmark_ticker, sector_benchmark_ticker

    tickers = {str(row["ticker"]) for row in product_rows}
    members = []
    for row in product_rows:
        stock = row.get("security_type") == "CS"
        sector = None
        if stock and row.get("sector") in SECTOR_BENCHMARK_ETF:
            candidate = resolve_benchmark_ticker(str(row["ticker"]), sector_benchmark_ticker(str(row["sector"])))
            sector = candidate if candidate in tickers else None
        members.append(rri.ResearchMember(str(row["security_id"]), int(row["instrument_id"]), str(row["ticker"]),
                                          row.get("security_type"), sector, stock))
    return tuple(members)


STOCK_ALERT_RULES_ENV = "REALTIME_V2_STOCK_ALERT_RULES"


def stock_alert_rules() -> str:
    """RESEARCH (default in this image): the research rules; PLACEHOLDER: the Nov 5 breakout reference rule."""
    import os

    value = os.environ.get(STOCK_ALERT_RULES_ENV, "RESEARCH").strip().upper()
    if value not in {"RESEARCH", "PLACEHOLDER"}:
        raise ValueError(f"{STOCK_ALERT_RULES_ENV} must be RESEARCH or PLACEHOLDER")
    return value


def _rows(cursor, names):
    return [dict(row) if isinstance(row, Mapping) else dict(zip(names, row)) for row in cursor.fetchall()]


def load_research_inputs_cursor(cursor, inputs: RealtimeScreeningInput,
                                product_rows: Sequence[Mapping[str, object]]) -> ResearchInputs:
    from .equity_bar_repository import load_bar_range_cursor

    members = research_members(product_rows)
    ids = [member.instrument_id for member in members]
    securities = [member.security_id for member in members]
    known_by = inputs.recorded_by
    bars, bar_sha256 = {}, {}
    for interval, basis in (("30m", "RAW_UNADJUSTED"), ("1h", "RAW_UNADJUSTED"), ("1d", "ADJUSTED")):
        loaded = load_bar_range_cursor(
            cursor, instrument_ids=ids, interval=interval, basis=basis,
            start=rri.history_start(_CALENDAR, inputs.session, interval),
            end=inputs.cutoff + timedelta(seconds=1), as_of=known_by)
        bars[interval] = loaded
        for rows in loaded.values():
            for row in rows:
                bar_sha256[str(row["bar_revision_id"])] = str(row["payload_sha256"])
    cursor.execute(
        "SELECT security_id::text,action_type,ex_date,effective_date FROM ref.corporate_action_revisions "
        "WHERE security_id=ANY(%s::uuid[]) AND received_at<=%s", (securities, known_by))
    splits = rri.split_segments(_rows(cursor, ("security_id", "action_type", "ex_date", "effective_date")))
    cursor.execute(
        "SELECT security_id::text,action_type,max(window_end) AS covered_through "
        "FROM ref.corporate_action_coverage WHERE security_id=ANY(%s::uuid[]) AND received_at<=%s "
        "GROUP BY 1,2", (securities, known_by))
    coverage: dict[str, dict[str, object]] = {}
    for row in _rows(cursor, ("security_id", "action_type", "covered_through")):
        coverage.setdefault(row["security_id"], {})[row["action_type"]] = row["covered_through"]
    covered = {security: all(coverage.get(security, {}).get(kind) is not None
                             and coverage[security][kind] >= inputs.session for kind in ("SPLIT", "DIVIDEND"))
               for security in securities}
    payload, summary = rri.build_research_payload(members, bars, session=inputs.session, as_of=known_by,
                                                  splits=splits, covered=covered, cohort_id=str(inputs.cohort_id))
    return ResearchInputs(rri.CachedResearchData(payload), members, bar_sha256,
                          {**summary, "actions_covered": sum(covered.values())})


def research_prior_states_cursor(cursor, *, detector_sha256: str, cohort_id: UUID) -> dict[str, ResearchPriorState]:
    cursor.execute(
        "SELECT state_key,revision,state_payload FROM rt.detector_state WHERE detector_sha256=%s AND cohort_id=%s",
        (detector_sha256, str(cohort_id)))
    return {str(row["state_key"]): ResearchPriorState(int(row["revision"]), dict(row["state_payload"] or {}))
            for row in _rows(cursor, ("state_key", "revision", "state_payload"))}


def build_research_rule_batch_cursor(cursor, receipt: Mapping[str, object], *, context_inputs: RealtimeScreeningInput,
                                     membership: CanonicalStockAlertMembership) -> CanonicalStockAlertBatch:
    version = research_rule_version()
    product_rows = _product_members(context_inputs, membership)
    research = load_research_inputs_cursor(cursor, context_inputs, product_rows)
    deadline = receipt.get("deadline")
    return produce_research_rule_batch(
        context_inputs, research,
        window_id=UUID(str(receipt["window_id"])), input_receipt_id=UUID(str(receipt["receipt_id"])),
        receipt_input_sha256=str(receipt["input_sha256"]), membership=membership,
        prior_states=research_prior_states_cursor(cursor, detector_sha256=version.detector_sha256,
                                                  cohort_id=context_inputs.cohort_id),
        deadline=None if deadline is None else deadline.astimezone(UTC),
        detector_input_sha256=inputs_sha256(context_inputs), version=version)
