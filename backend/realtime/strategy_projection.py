"""Independent default daily strategy releases over exact retained research inputs."""
from dataclasses import dataclass, replace
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path

import exchange_calendars
import numpy as np

from equity.domain import EvidenceRole, EvidenceType, LifecycleStatus, QualityState
from equity.materialization import _evidence, _json_safe
from equity.portal_scanners import compute_default_scanner_snapshots, default_scanner_analysis, HOURLY_SCANNER_CONTRACTS

from .contracts import canonical_sha256
from .dashboard_projection import DAILY_SCANNER_CONTRACTS, SCANNER_BASIS, dashboard_frames, _overview
from .portal_projection import build_portal_snapshot
from .preflight import ProviderTier
from .screening_projection import SCREENING_BASIS, _validate_bars
from .setup_summary_projection import produce_daily_setup_summary, produce_hourly_setup_summary
from .strategy_persistence import build_strategy_persistence
from .ticker_projection import _security


STRATEGY_VERSION = "DAILY_STRATEGIES_V1"
STRATEGY_KEY = "COHORT"
HISTORY_VERSION = "DAILY_STRATEGY_HISTORY_V1"
RECONSTRUCTED_VERSION = "DAILY_RECONSTRUCTED_EVALUATION_V1"
ROLLING_VERSION = "DAILY_STRATEGY_ROLLING_V1"
HOURLY_ROLLING_VERSION = "HOURLY_STRATEGY_ROLLING_V1"


@dataclass(frozen=True)
class DailyStrategyProjection:
    snapshot: object
    materializations: tuple


def strategy_action_context(row):
    raw = row.get("raw_payload")
    raw = raw if isinstance(raw, dict) else {}
    amount = row.get("cash_amount")
    verified = False
    try:
        cash = Decimal(str(amount))
        verified = (row["action_type"] == "DIVIDEND" and canonical_sha256(raw) == row["payload_sha256"]
            and raw.get("ticker") == row["ticker"] and raw.get("distribution_type") == "recurring"
            and raw.get("currency") == "USD" and cash.is_finite() and cash > 0
            and Decimal(str(raw.get("cash_amount"))) == cash
            and raw.get("ex_dividend_date") == str(row["effective_date"])
            and str(row.get("ex_date")) == str(row["effective_date"]))
    except (InvalidOperation, ValueError):
        verified = False
    return dict(action_id=str(row["action_id"]), security_id=str(row["security_id"]),
        action_type=row["action_type"], effective_date=str(row["effective_date"]), payload_sha256=row["payload_sha256"],
        cash_amount=str(amount) if amount is not None else None, currency=raw.get("currency"),
        distribution_type=raw.get("distribution_type"),
        handling="RECURRING_CASH_DIVIDEND_PRICE_SERIES_UNCHANGED" if verified else "CORPORATE_ACTION_REVIEW")


def strategy_reference_identity(reference):
    return (reference["ticker"], str(reference["security_id"]), reference["instrument_id"], reference["security_type"])


def _validate_strategy_inputs(inputs):
    if (len(inputs.source_manifest_sha256) != 64
            or any(value not in "0123456789abcdef" for value in inputs.source_manifest_sha256)
            or type(inputs.source_generation) is not int or inputs.source_generation <= 0):
        raise ValueError("Strategy source manifest/generation is invalid")
    calendar = exchange_calendars.get_calendar("XNYS")
    market_time = calendar.session_close(str(inputs.session)).to_pydatetime()
    if market_time > inputs.cutoff:
        raise ValueError("Strategy session is not completed at cutoff")
    members = inputs.members
    if (not 0 < len(members) <= 1000 or len({row["ticker"] for row in members}) != len(members)
            or len({str(row["security_id"]) for row in members}) != len(members)
            or set(row["ticker"] for row in inputs.bars) - {row["ticker"] for row in members}):
        raise ValueError("Strategy exact unique cohort membership is required")
    delay = timedelta(minutes=15 if inputs.provider_tier is ProviderTier.MASSIVE_DEVELOPER else 0)
    unavailable_members = {}
    action_context = {}
    for member in members:
        if (not member.get("security_revision_id") or member.get("security_type") not in {"CS", "ETF", "ETV"}
                or member["observed_at"] > inputs.cutoff):
            raise ValueError("Strategy reference identity/availability drift")
        own = [row for row in inputs.bars if row["ticker"] == member["ticker"]
               and row["interval"] not in {"1wk", "1mo"}]
        _validate_bars(own, member, inputs)
        for row in own:
            clocks = [row["bar_end"] + delay, row["observed_at"], row["created_at"]]
            clocks.extend(row[key] for key in ("replay_available_at", "provider_published_at")
                          if row.get(key) is not None)
            if any(clock.tzinfo is None or clock.utcoffset() is None for clock in clocks):
                raise ValueError("Strategy source clocks must be timezone-aware")
            if max(clocks) > inputs.cutoff:
                raise ValueError("Strategy source availability is beyond cutoff")
        daily = sorted((row for row in own if row["interval"] == "1d"
                        and row["adjustment_basis"] == "ADJUSTED"), key=lambda row: row["session_date"])
        actions = [row for row in inputs.actions if str(row["security_id"]) == str(member["security_id"])]
        if any(row["ticker"] != member["ticker"] or row["received_at"] > inputs.cutoff for row in actions):
            raise ValueError("Strategy action identity/availability drift")
        if not daily:
            continue
        sessions = [stamp.date() for stamp in calendar.sessions_in_range(str(daily[0]["session_date"]), str(inputs.session))]
        if [row["session_date"] for row in daily] != sessions:
            raise ValueError("Strategy retained daily window is incomplete or ambiguous")
        review_actions = [row for row in actions if daily[0]["session_date"] < row["effective_date"] <= inputs.session
                          and row["action_type"] != "SPLIT"]
        if review_actions:
            refs = [strategy_action_context(row) for row in review_actions]
            reason = "CORPORATE_ACTION_REVIEW" if any(row["handling"] == "CORPORATE_ACTION_REVIEW" for row in refs) else None
            context = dict(reason=reason, available_bars=len(daily), action_refs=refs)
            action_context[member["ticker"]] = context
            if reason:
                unavailable_members[member["ticker"]] = context
        coverage = [row for row in inputs.coverage if str(row["security_id"]) == str(member["security_id"])
                    and row["action_type"] == "SPLIT" and row["window_start"] <= sessions[0]
                    and row["window_end"] >= inputs.session and row["received_at"] <= inputs.cutoff
                    and row["response_action_count"] == sum(action["action_type"] == "SPLIT"
                        and row["window_start"] <= action["effective_date"] <= row["window_end"] for action in actions)
                    and all(action["received_at"] <= row["received_at"] for action in actions
                            if action["action_type"] == "SPLIT" and row["window_start"] <= action["effective_date"] <= row["window_end"])]
        if not coverage:
            raise ValueError("Strategy split/action coverage is unavailable or inconsistent")
        splits = [row for row in actions if row["action_type"] == "SPLIT"]
        if any(not np.isfinite([float(row["split_from"]), float(row["split_to"])]).all()
               or float(row["split_from"]) <= 0 or float(row["split_to"]) <= 0 for row in splits):
            raise ValueError("Strategy split evidence is invalid")
        for previous, current in zip(daily, daily[1:]):
            change = float(current["close"]) / float(previous["close"]) - 1
            if current["session_date"] in {row["effective_date"] for row in splits} and (change <= -.38 or change >= .60):
                raise ValueError("Strategy adjusted split/action drift")
        raw = [row for row in own if row["interval"] == "1d" and row["adjustment_basis"] == "RAW_UNADJUSTED"
               and row["session_date"] == inputs.session]
        if len(raw) != 1 or abs(float(raw[0]["close"]) / float(daily[-1]["close"]) - 1) > SCREENING_BASIS["current_scale_max_relative_difference"]:
            raise ValueError("Strategy current adjusted/raw scale drift")
    return market_time, unavailable_members, action_context


def project_realtime_daily_strategies(inputs, *, reconstructed=False, interval="1d", market_time=None):
    if interval not in {"1d", "1h"}:
        raise ValueError("Strategy production supports only 1d/1h")
    if interval == "1h":
        from .hourly_strategy_projection import hourly_strategy_inputs, HOURLY_VERSION, HOURLY_RECONSTRUCTED_VERSION, HOURLY_BASIS
        version = HOURLY_RECONSTRUCTED_VERSION if reconstructed else HOURLY_VERSION
        market_time, unavailable_members, action_context, frames, raw = hourly_strategy_inputs(inputs, market_time)
        basis = HOURLY_BASIS
        contracts = {name: HOURLY_SCANNER_CONTRACTS[contract] for name, contract in DAILY_SCANNER_CONTRACTS.items()}
    else:
        version = RECONSTRUCTED_VERSION if reconstructed else STRATEGY_VERSION
        market_time, unavailable_members, action_context = _validate_strategy_inputs(inputs)
        frames, raw = dashboard_frames(inputs)
        basis, contracts = SCANNER_BASIS, DAILY_SCANNER_CONTRACTS
    tickers = sorted(frames)
    eligible_frames = {ticker: frame for ticker, frame in frames.items() if ticker not in unavailable_members}
    scanners = compute_default_scanner_snapshots(tickers, eligible_frames,
        {ticker: eligible_frames[ticker] for ticker in ("SPY", "QQQ") if ticker in eligible_frames}, observed_at=inputs.cutoff,
        **({"interval": interval} if interval == "1h" else {}))
    scanners = {key: _json_safe(scanners[key]) for key in contracts.values()}
    analysis = default_scanner_analysis(tickers, frames, scanners, unavailable_members=unavailable_members,
                                       action_context=action_context, interval=interval)
    materializations = []
    setup_producer = produce_hourly_setup_summary if interval == "1h" else produce_daily_setup_summary
    setup_rows, setup_evidence, missing = setup_producer(inputs, materializations=materializations,
        excluded_tickers=unavailable_members, **({"market_time": market_time} if interval == "1h" else {}))
    references = {row["ticker"]: row for row in inputs.members}
    evidence_refs = {ticker: {} for ticker in tickers}
    receipts = {ticker: {} for ticker in tickers}
    results = []
    for result in materializations:
        ticker = result.materialization.evidence[0].ticker
        feature = result.materialization.evidence[0]
        extra = []
        for contract in scanners:
            member = next(row for row in analysis[contract]["members"] if row["ticker"] == ticker)
            receipt = dict(contract=contract, analysis=member, scanner_basis=basis,
                           input_sha256=result.input_sha256,
                           results=[row for row in scanners[contract]["results"] if row["ticker"] == ticker])
            if reconstructed:
                receipt.update(interpretation="RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS",
                    reconstruction_at=inputs.cutoff.isoformat())
            evidence = _evidence(
                analysis_run_id=feature.analysis_run_id, security=_security(references[ticker]), interval=interval,
                evidence_type=EvidenceType.SCANNER_RESULT, evidence_role=EvidenceRole.LOCATION,
                lifecycle_key=f"{'daily' if interval == '1d' else 'hourly'}-strategy:{contract}:{ticker}", lifecycle_status=LifecycleStatus.SNAPSHOT,
                direction=None, strength=None, market_time=market_time, observed_at=inputs.cutoff,
                source_name=contract, source_version=version,
                latest_bar_revision_id=result.bar_revision_ids[-1], source_revision_ids=result.bar_revision_ids,
                fundamental_report_ids=(), quality_state=QualityState.RESEARCH_ONLY,
                quality_codes=("RESEARCH_ONLY",), payload=receipt,
            )
            extra.append(evidence)
            evidence_refs[ticker][contract] = dict(cohort_id=str(inputs.cohort_id), evidence_id=str(evidence.evidence_id),
                evidence_type=evidence.evidence_type.value, market_time=evidence.market_time.isoformat(),
                payload_sha256=evidence.payload_sha256)
            receipts[ticker][contract] = json.loads(evidence.payload_json)
        results.append(replace(result, materialization=replace(result.materialization,
                              evidence=(*result.materialization.evidence, *extra))))
    manifest = dict(version=version, cohort_id=str(inputs.cohort_id), provider_tier=inputs.provider_tier.value,
        source_generation=inputs.source_generation, source_manifest_sha256=inputs.source_manifest_sha256,
        session=str(inputs.session), market_time=market_time.isoformat(), generated_at=inputs.cutoff.isoformat(),
        expected_members=len(tickers), tickers=tickers, references=[dict(references[ticker]) for ticker in tickers],
        input_sha256=canonical_sha256(inputs), scanner_basis=basis,
        scanner_contracts=list(scanners), evidence_refs=evidence_refs, receipts=receipts,
        unavailable_members=unavailable_members,
        action_context=action_context, action_policy="DAILY_STRATEGY_ACTION_CONTEXT_V1",
        setup_evidence=setup_evidence, missing_setup_confirmations=missing,
        source_windows={result.materialization.evidence[0].ticker: dict(input_sha256=result.input_sha256,
            primary_bar_revision_ids=[str(value) for value in result.bar_revision_ids],
            confirmation_bar_revision_ids=[str(value) for value in result.confirmation_bar_revision_ids]) for result in results},
        code_sha256={str(path.name): canonical_sha256(path.read_text(encoding="utf-8")) for path in
            (Path(__file__), Path(__file__).parents[1] / "equity" / "portal_scanners.py",
             Path(__file__).parents[1] / "screeners.py", Path(__file__).with_name("setup_summary_projection.py"),
             Path(__file__).parents[1] / "equity" / "materialization.py",
             Path(__file__).parents[1] / "equity" / "technicals.py",
             Path(__file__).parents[1] / "equity" / "setup_composition.py")})
    payload = dict(version=version, cohort_id=str(inputs.cohort_id), session=str(inputs.session),
        market_time=market_time.isoformat(), generated_at=inputs.cutoff.isoformat(),
        scope_note=f"Selected realtime cohort: {len(tickers)} tickers; not full-market or sector coverage.",
        price_basis="PROVIDER_SPLIT_ADJUSTED_HISTORY_PINNED_CURRENT_RAW_QUOTE", missing_producers={},
        scanner_basis=basis, scanners=scanners, analysis=analysis,
        overview=[_json_safe(_overview(ticker, frames[ticker], raw[ticker])) for ticker in tickers
              if ticker not in unavailable_members and len(frames[ticker]) >= 2 and raw[ticker]],
        setup_summary=dict(interval=interval, source="MATERIALIZED_CURRENT_PROJECTION", computed_at=inputs.cutoff.isoformat(),
            market_time=market_time.isoformat(), count=len(setup_rows), results=setup_rows),
        provider_fetch_performed=False, execution_permission=False)
    if interval == "1h":
        from .hourly_strategy_projection import hourly_split_coverage, split_coverage_ref, session_hourly_slots
        slot_start = next(start for start, end in session_hourly_slots(exchange_calendars.get_calendar("XNYS"), inputs.session) if end == market_time)
        manifest.update(interval="1h", slot_minutes=int((market_time - slot_start).total_seconds() / 60))
        manifest.update(action_policy="HOURLY_STRATEGY_ACTION_CONTEXT_V1", split_coverage_refs={ticker: [split_coverage_ref(row)
            for row in hourly_split_coverage(inputs, references[ticker], raw[ticker][0]["session_date"])] for ticker in tickers if raw[ticker]})
        payload.update(interval="1h", price_basis="RAW_UNADJUSTED_RTH_ACTION_GATED_OHLC")
        manifest["code_sha256"]["hourly_strategy_projection.py"] = canonical_sha256(Path(__file__).with_name("hourly_strategy_projection.py").read_text(encoding="utf-8"))
    snapshot = build_portal_snapshot(cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
        snapshot_type=version, snapshot_key=STRATEGY_KEY, source_generation=inputs.source_generation,
        source_manifest=manifest, payload=payload, generated_at=inputs.cutoff)
    return DailyStrategyProjection(snapshot, tuple(results))


def project_realtime_strategy_history(inputs, *, interval="1d", market_time=None):
    calendar = exchange_calendars.get_calendar("XNYS")
    if interval == "1h" and market_time is None:
        from .hourly_strategy_projection import hourly_strategy_inputs
        market_time = hourly_strategy_inputs(inputs)[0]
    sessions = calendar.sessions_in_range(calendar.session_offset(str(inputs.session), -4), str(inputs.session))
    projections = []
    for ordinal, session in enumerate(sessions):
        session_cutoff = calendar.session_close(session).to_pydatetime()
        if interval == "1h" and session.date() == inputs.session and market_time is not None:
            session_cutoff = market_time
        truncated = replace(inputs, session=session.date(), source_generation=inputs.source_generation + ordinal,
            bars=tuple(row for row in inputs.bars if row["bar_end"] <= session_cutoff))
        unavailable = set()
        for member in inputs.members:
            ticker = member["ticker"]
            daily = sorted(row["session_date"] for row in truncated.bars if row["ticker"] == ticker
                           and row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED")
            raw = [row for row in truncated.bars if row["ticker"] == ticker and row["interval"] == "1d"
                   and row["adjustment_basis"] == "RAW_UNADJUSTED" and row["session_date"] == session.date()]
            expected = [stamp.date() for stamp in calendar.sessions_in_range(str(daily[0]), session)] if daily else []
            if interval == "1d" and (not daily or daily != expected or not raw):
                unavailable.add(ticker)
        truncated = replace(truncated, bars=tuple(row for row in truncated.bars if row["ticker"] not in unavailable))
        projections.append(project_realtime_daily_strategies(truncated, reconstructed=True, interval=interval,
            market_time=session_cutoff if interval == "1h" else None))
    latest = projections[-1].snapshot
    history = [dict(session=projection.snapshot.payload["session"],
        source_manifest=projection.snapshot.source_manifest, payload=projection.snapshot.payload)
        for projection in projections]
    if interval == "1h":
        from .hourly_strategy_projection import HOURLY_HISTORY_VERSION
    version = HISTORY_VERSION if interval == "1d" else HOURLY_HISTORY_VERSION
    manifest = dict(latest.source_manifest, version=version,
        interpretation="RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS",
        reconstruction_at=inputs.cutoff.isoformat(), evaluation_sessions=[str(session.date()) for session in sessions],
        history=history)
    manifest["code_sha256"]["strategy_persistence.py"] = canonical_sha256(Path(__file__).with_name("strategy_persistence.py").read_text(encoding="utf-8"))
    payload = dict(latest.payload, version=version,
        interpretation=manifest["interpretation"], reconstruction_at=manifest["reconstruction_at"],
        evaluation_sessions=manifest["evaluation_sessions"],
        persistence=build_strategy_persistence(history, manifest["tickers"], manifest["evaluation_sessions"], interval=interval))
    snapshot = build_portal_snapshot(cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
        snapshot_type=version, snapshot_key=STRATEGY_KEY, source_generation=latest.source_generation,
        source_manifest=manifest, payload=payload, generated_at=inputs.cutoff)
    return DailyStrategyProjection(snapshot, tuple(result for projection in projections for result in projection.materializations))


def project_realtime_rolling_strategies(inputs, *, retained_history=(), interval="1d", market_time=None):
    latest = project_realtime_daily_strategies(inputs, interval=interval, market_time=market_time)
    snapshot = assemble_realtime_strategy_history(latest.snapshot, retained_history=retained_history,
        published_at=inputs.cutoff, source_generation=inputs.source_generation)
    return DailyStrategyProjection(snapshot, latest.materializations)


def assemble_realtime_strategy_history(source, *, retained_history=(), published_at, source_generation):
    from .strategy_repository import _validate_release
    _validate_release(source.source_manifest, source.payload)
    if (published_at.tzinfo is None or published_at.utcoffset() is None
            or published_at < source.generated_at or type(source_generation) is not int or source_generation < 1):
        raise ValueError("Strategy history publication clock/generation is invalid")
    if len(retained_history) > 5:
        raise ValueError("Rolling strategy history exceeds five session outcomes")
    interval = source.source_manifest.get("interval", "1d")
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = [str(session.date()) for session in calendar.sessions_in_range(
        calendar.session_offset(source.payload["session"], -4), source.payload["session"])]
    previous = {}
    for item in retained_history:
        manifest, payload = item["source_manifest"], item["payload"]
        _validate_release(manifest, payload)
        if (item["session"] != manifest["session"] or item["session"] != payload["session"]
                or item["session"] > sessions[-1] or item["session"] in previous
                or manifest["cohort_id"] != source.source_manifest["cohort_id"]
                or manifest["provider_tier"] != source.source_manifest["provider_tier"]
                or manifest["source_manifest_sha256"] != source.source_manifest["source_manifest_sha256"]
                or manifest.get("interval", "1d") != interval
                or manifest["tickers"] != source.source_manifest["tickers"]
                or manifest["scanner_basis"] != source.source_manifest["scanner_basis"]
                or manifest["scanner_contracts"] != source.source_manifest["scanner_contracts"]
                or datetime.fromisoformat(payload["generated_at"]) > published_at
                or datetime.fromisoformat(payload["market_time"]) > datetime.fromisoformat(source.payload["market_time"])
                or any(manifest["code_sha256"].get(name) != source.source_manifest["code_sha256"].get(name)
                    for name in source.source_manifest["code_sha256"] if name not in {"strategy_projection.py", "strategy_persistence.py"})
                or any(strategy_reference_identity(reference) != strategy_reference_identity(current)
                    for reference, current in zip(manifest["references"], source.source_manifest["references"]))):
            raise ValueError("Rolling strategy retained session identity/basis/model/clock drift")
        previous[item["session"]] = item
    history = [deepcopy(previous[session]) for session in sessions[:-1] if session in previous]
    history.append(dict(session=source.payload["session"], source_manifest=deepcopy(source.source_manifest),
        payload=deepcopy(source.payload)))
    version = ROLLING_VERSION if interval == "1d" else HOURLY_ROLLING_VERSION
    provenance = {item["session"]: "RECONSTRUCTED_RESEARCH_NOT_ORIGINAL_ON_TIME_DECISIONS"
                  if "RECONSTRUCTED" in item["source_manifest"]["version"] else "STORED_EVALUATION"
                  for item in history}
    manifest = dict(source.source_manifest, version=version, interpretation="STORED_SESSION_OUTCOMES_WITH_EXPLICIT_PROVENANCE",
        source_generation=source_generation, generated_at=published_at.isoformat(),
        evaluation_generated_at=source.payload["generated_at"],
        evaluation_sessions=sessions, observed_sessions=[item["session"] for item in history], session_provenance=provenance,
        history=history)
    manifest["code_sha256"] = dict(manifest["code_sha256"])
    manifest["code_sha256"]["strategy_persistence.py"] = canonical_sha256(
        Path(__file__).with_name("strategy_persistence.py").read_text(encoding="utf-8"))
    payload = dict(source.payload, version=version, interpretation=manifest["interpretation"],
        generated_at=manifest["generated_at"], evaluation_generated_at=manifest["evaluation_generated_at"],
        evaluation_sessions=sessions, observed_sessions=manifest["observed_sessions"], session_provenance=provenance,
        persistence=build_strategy_persistence(history, manifest["tickers"], sessions, interval=interval))
    return build_portal_snapshot(cohort_id=source.cohort_id, provider_tier=source.provider_tier,
        snapshot_type=version, snapshot_key=STRATEGY_KEY, source_generation=source_generation,
        source_manifest=manifest, payload=payload, generated_at=published_at)