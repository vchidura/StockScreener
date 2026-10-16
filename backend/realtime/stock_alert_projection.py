"""Provider-free Stock Alerts projection; never invents forward enrollment or runs."""
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import timedelta
import math
from pathlib import Path
from typing import Mapping
from uuid import UUID

import exchange_calendars

from research.stock_alert_results import combine_snapshots, namespace_snapshot, strategy_instance
from research.stock_alerts import empty_snapshot, enrich_replay, replay_snapshot, session_dates
from research.stock_idea_forward import (
    READINESS_POLICY, forward_config, readiness_deadline, update_positions, window_clock,
)
from research.stock_idea_models import (
    daily_observations, feature_frames, higher_context_observations, intraday_observations,
)
from research.stock_idea_replay import available_at, derive_hours, session_windows, utc

from .contracts import canonical_sha256
from .equity_portal_history import build_realtime_equity_portal_history_plan
from .lineage_head import inputs_bar_lineage_sha256, inputs_sha256, reference_set, require_bounded_manifest
from .portal_projection import build_portal_snapshot
from .screening_projection import RealtimeScreeningInput


STOCK_ALERTS_VERSION = "STOCK_ALERTS_V1"
MISSING_PRODUCERS = {
    "forward": "REALTIME_FORWARD_ENROLLMENT_AND_PUBLICATION_CHECKPOINT_UNAVAILABLE",
    "replay": "REALTIME_RETAINED_REPLAY_COHORT_UNAVAILABLE",
    "context": "REALTIME_PUBLICATION_ROTATION_AND_EVENT_CONTEXT_UNAVAILABLE",
}


@dataclass(frozen=True)
class RetainedAlertStream:
    cohort_id: UUID
    source_manifest_sha256: str
    stream: str
    policy: Mapping
    state: Mapping
    publications: tuple[Mapping, ...]


def alert_source_inputs(inputs: RealtimeScreeningInput) -> RealtimeScreeningInput:
    return replace(inputs, bars=tuple(
        row for row in inputs.bars
        if (
            row["interval"] == "30m" and row["adjustment_basis"] == "RAW_UNADJUSTED"
            or row["interval"] == "1d" and row["adjustment_basis"] in {"ADJUSTED", "RAW_UNADJUSTED"}
        )
    ))


def _validate_alert_sources(inputs: RealtimeScreeningInput, plan, bars_by_ticker) -> None:
    members = inputs.members
    if (
        not 0 < len(members) <= 1000
        or len({str(member["security_id"]) for member in members}) != len(members)
        or len({member["ticker"] for member in members}) != len(members)
    ):
        raise ValueError("Full unique alert manifest membership is required")
    if set(bars_by_ticker) - {member["ticker"] for member in members}:
        raise ValueError("Alert bars extend beyond the exact manifest")
    coverage_by_security, actions_by_security = {}, {}
    for row in inputs.coverage:
        coverage_by_security.setdefault(str(row["security_id"]), []).append(row)
    for row in inputs.actions:
        actions_by_security.setdefault(str(row["security_id"]), []).append(row)
    adjusted_sessions = tuple(plan.adjusted_sessions)
    raw_sessions = tuple(plan.raw_sessions)
    recorded_by = inputs.recorded_by
    for member in members:
        identity, ticker = str(member["security_id"]), member["ticker"]
        if not member.get("security_revision_id") or member.get("security_type") not in {"CS", "ETF", "ETV"}:
            raise ValueError(f"Alert reference identity/type unavailable: {ticker}")
        if member["observed_at"] > inputs.cutoff:
            raise ValueError(f"Alert reference is not available: {ticker}")
        own = bars_by_ticker.get(ticker, ())
        for row in own:
            if (
                str(row["cohort_id"]) != str(inputs.cohort_id)
                or row["provider_tier"] != inputs.provider_tier.value
                or str(row["security_id"]) != identity
                or row["instrument_id"] != member["instrument_id"]
                or row["ticker"] != ticker
                or row["session_scope"] != "RTH"
                or not row["is_final"]
                or row["bar_end"] > inputs.cutoff
                or row["observed_at"] > recorded_by
                or row["created_at"] > recorded_by
                or row.get("replay_available_at") is not None and row["replay_available_at"] > recorded_by
                or row.get("provider_published_at") is not None and row["provider_published_at"] > recorded_by
            ):
                raise ValueError(f"Alert source identity/availability drift: {ticker}")
            prices = tuple(float(row[name]) for name in ("open", "high", "low", "close", "volume"))
            if (
                not all(math.isfinite(value) for value in prices)
                or min(prices[:4]) <= 0
                or prices[4] < 0
                or prices[1] < max(prices[0], prices[2], prices[3])
                or prices[2] > min(prices[0], prices[1], prices[3])
            ):
                raise ValueError(f"Invalid alert bar: {ticker}")
            if row["source_kind"] in {"DERIVED", "RECONCILED"} and not row["source_revision_ids"]:
                raise ValueError("Alert derived source lineage is missing")
        adjusted = sorted(
            (row for row in own if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"),
            key=lambda row: row["session_date"],
        )
        raw = sorted(
            (row for row in own if row["interval"] == "1d" and row["adjustment_basis"] == "RAW_UNADJUSTED"),
            key=lambda row: row["session_date"],
        )
        if tuple(row["session_date"] for row in adjusted) != adjusted_sessions:
            raise ValueError(f"Alert adjusted history is incomplete: {ticker}")
        if tuple(row["session_date"] for row in raw) != raw_sessions:
            raise ValueError(f"Alert raw daily history is incomplete: {ticker}")
        actions = actions_by_security.get(identity, ())
        if any(row["ticker"] != ticker or row["received_at"] > inputs.cutoff for row in actions):
            raise ValueError(f"Alert action evidence drift: {ticker}")
        if any(
            row["action_type"] == "SPLIT"
            and (
                not math.isfinite(float(row["split_from"]))
                or not math.isfinite(float(row["split_to"]))
                or float(row["split_from"]) <= 0
                or float(row["split_to"]) <= 0
            )
            for row in actions
        ):
            raise ValueError(f"Invalid alert split evidence: {ticker}")
        coverage = [
            row for row in coverage_by_security.get(identity, ())
            if (
                row["action_type"] == "SPLIT"
                and row["window_start"] <= adjusted_sessions[0]
                and row["window_end"] >= inputs.session
                and row["received_at"] <= inputs.cutoff
            )
        ]
        coverage = [
            row for row in coverage
            if (
                row["response_action_count"] == sum(
                    action["action_type"] == "SPLIT"
                    and row["window_start"] <= action["effective_date"] <= row["window_end"]
                    for action in actions
                )
                and all(
                    action["received_at"] <= row["received_at"]
                    for action in actions
                    if action["action_type"] == "SPLIT"
                    and row["window_start"] <= action["effective_date"] <= row["window_end"]
                )
            )
        ]
        if not coverage:
            raise ValueError(f"Alert split/action coverage drift: {ticker}")


def alert_domain_inputs(inputs: RealtimeScreeningInput):
    inputs = alert_source_inputs(inputs)
    policy = forward_config(quality_version=2)
    # created_at and the availability clocks (observed_at, replay/published and bar_end plus the provider
    # delay) are recording clocks: they are checked against `recorded_by` (known_by, else the cutoff);
    # bar_end itself stays a market time, checked against the cutoff.
    recorded_by = inputs.recorded_by
    for row in inputs.bars:
        created = row["created_at"]
        if created.tzinfo is None or created.utcoffset() is None or created > recorded_by:
            raise ValueError("Alert source creation clock is beyond cutoff")
    def source_clock(row):
        return max(row["bar_end"] + timedelta(minutes=15), row["created_at"], row["observed_at"],
                   *(row[key] for key in ("replay_available_at", "provider_published_at") if row.get(key) is not None))
    for row in inputs.bars:
        effective_available_at = source_clock(row)
        if effective_available_at > recorded_by or row["bar_end"] > inputs.cutoff:
            raise ValueError("Alert source availability is beyond cutoff")
    intraday_plan = build_realtime_equity_portal_history_plan(
        inputs.session,
        equity_ticker_count=len(inputs.members),
    )
    calendar = exchange_calendars.get_calendar("XNYS")
    session_close = calendar.session_close(str(inputs.session)).to_pydatetime()
    daily_plan = intraday_plan
    if inputs.cutoff < session_close:
        daily_plan = build_realtime_equity_portal_history_plan(
            calendar.previous_session(str(inputs.session)).date(),
            equity_ticker_count=len(inputs.members),
        )
    bars_by_ticker = {}
    for row in inputs.bars:
        bars_by_ticker.setdefault(row["ticker"], []).append(row)
    _validate_alert_sources(inputs, daily_plan, bars_by_ticker)
    bars = []
    for member in inputs.members:
        identity, ticker = str(member["security_id"]), member["ticker"]
        own = bars_by_ticker.get(ticker, ())
        raw = {row["session_date"]: row for row in own
               if row["interval"] == "1d" and row["adjustment_basis"] == "RAW_UNADJUSTED"}
        native = sorted((row for row in own if row["interval"] == "30m"
                         and row["adjustment_basis"] == "RAW_UNADJUSTED"), key=lambda row: row["bar_start"])
        expected = [
            window
            for session in intraday_plan.raw_sessions
            for window in session_windows(str(session))
            if window[1] <= inputs.cutoff
        ]
        if [(row["bar_start"], row["bar_end"]) for row in native] != expected:
            raise ValueError(f"Alert completed 30m history is incomplete: {ticker}")
        daily = sorted((row for row in own if row["interval"] == "1d"
                        and row["adjustment_basis"] == "ADJUSTED"), key=lambda row: row["bar_start"])
        for row in [*daily, *native]:
            clocks = [row["observed_at"], row["created_at"], row["bar_end"]]
            clocks.extend(row[key] for key in ("replay_available_at", "provider_published_at") if row.get(key) is not None)
            if any(stamp.tzinfo is None or stamp.utcoffset() is None for stamp in clocks):
                raise ValueError("Alert source clocks must be timezone-aware")
            if (max(clocks[:2] + clocks[3:]) > recorded_by or row["bar_end"] > inputs.cutoff
                    or row["observed_at"] < row["bar_end"]):
                raise ValueError("Alert source availability clock drift")
            bar = dict(
                security_id=identity, ticker=ticker, interval=row["interval"],
                session=str(row["session_date"]), bar_start=row["bar_start"].isoformat(),
                bar_end=row["bar_end"].isoformat(), revision_id=str(row["bar_revision_id"]),
                source_revision_ids=[str(value) for value in row["source_revision_ids"]],
                system_observed_at=row["observed_at"].isoformat(),
                created_at=row["created_at"].isoformat(),
                visible_at=max(*clocks, row["bar_end"] + timedelta(minutes=15)).isoformat(),
                **{key: float(row[key]) for key in ("open", "high", "low", "close", "volume")},
            )
            if row["interval"] == "1d":
                nominal = raw.get(row["session_date"])
                bar.update(price_basis="SPLIT_ADJUSTED", execution_scale=None,
                           execution_scale_revision_ids=[], execution_scale_observed_at=None,
                           execution_scale_created_at=None)
                if nominal is not None:
                    scale_clocks = [nominal["observed_at"], nominal["created_at"]]
                    scale_clocks.extend(nominal[key] for key in ("replay_available_at", "provider_published_at")
                                        if nominal.get(key) is not None)
                    if max(scale_clocks) > max(
                        nominal["observed_at"], nominal["created_at"], nominal["bar_end"] + timedelta(minutes=15),
                    ):
                        raise ValueError("Raw execution scale availability cannot be represented by the owning policy")
                    bar.update(
                        execution_scale=float(nominal["close"]) / float(row["close"]),
                        execution_scale_revision_ids=[str(nominal["bar_revision_id"])],
                        execution_scale_observed_at=nominal["observed_at"].isoformat(),
                        execution_scale_created_at=nominal["created_at"].isoformat(),
                    )
            bars.append(bar)
            if row["interval"] == "30m" and utc(bar["visible_at"]) > available_at(bar, policy):
                raise ValueError("Raw execution availability cannot be represented by the owning policy")
    actions = [dict(
        security_id=str(row["security_id"]), action_type=row["action_type"],
        effective_date=str(row["effective_date"]), revision_id=str(row["action_id"]),
        observed_at=row["received_at"].isoformat(),
        split_from=float(row["split_from"]) if row.get("split_from") is not None else None,
        split_to=float(row["split_to"]) if row.get("split_to") is not None else None,
    ) for row in inputs.actions]
    return bars, actions


def evaluate_alert_inputs(inputs, bars, actions, policy):
    """Execute the owning detectors with actual clocks, including unavailable packets."""
    frames = feature_frames(dict(bars=bars, actions=actions), policy)
    from research.stock_idea_forward import daily_rank_context
    members = [dict(security_id=str(row["security_id"]), ticker=row["ticker"]) for row in inputs.members]
    contexts = {}
    daily = {key: frame for key, frame in frames.items() if key[1] == "1d"}
    for session in session_dates(str(inputs.session)):
        closing = session_windows(session, "1d")[0][1]
        contexts.update(daily_rank_context(daily, members, closing + timedelta(minutes=17)))
    packets = []
    for (_, interval), frame in frames.items():
        if interval == "1d":
            packets.extend(daily_observations(frame, contexts, policy))
            packets.extend(higher_context_observations(frame, policy))
        else:
            # Use the existing 200-bar warmup plus a bounded detector window.
            packets.extend(intraday_observations(frame.tail(500).reset_index(drop=True), interval, contexts, policy))
    return packets


def _retained_snapshot(inputs, retained, bars, actions):
    if retained.stream not in {"intraday", "swing"}:
        raise ValueError("Unknown realtime alert stream")
    if retained.cohort_id != inputs.cohort_id or retained.source_manifest_sha256 != inputs.source_manifest_sha256:
        raise ValueError("Retained alert stream belongs to a different realtime cohort")
    policy = forward_config(quality_version=2, swing=retained.stream == "swing")
    if dict(retained.policy) != policy:
        raise ValueError("Retained realtime alert policy does not match the owning worker")
    state = deepcopy(dict(retained.state))
    members = [dict(security_id=str(row["security_id"]), ticker=row["ticker"]) for row in inputs.members]
    if state["members"] != members or utc(state["enrolled_at"]) > inputs.cutoff:
        raise ValueError("Retained alert enrollment identity/clock drift")
    if any(member["observed_at"] > utc(state["enrolled_at"]) for member in inputs.members):
        raise ValueError("Retained alert reference was unavailable at enrollment")
    indexed = {bar["revision_id"]: bar for bar in bars}
    domain_hours = derive_hours(bars, policy)
    revision_ids = {str(row["bar_revision_id"]) for row in inputs.bars}
    revision_ids.update(row["revision_id"] for row in domain_hours)
    availability = {
        str(row["bar_revision_id"]): max(
            row["bar_end"] + timedelta(minutes=15), row["observed_at"], row["created_at"],
            *(row[key] for key in ("replay_available_at", "provider_published_at") if row.get(key) is not None),
        ) for row in inputs.bars
    }
    availability.update((row["revision_id"], max(availability[source] for source in row["source_revision_ids"]))
                        for row in domain_hours)
    for rows in state.get("bars", {}).values():
        for bar in rows.values():
            if bar["revision_id"] not in indexed or bar != indexed[bar["revision_id"]]:
                raise ValueError("Retained alert checkpoint bar identity/clock drift")
    publications = deepcopy(list(retained.publications))
    if len({row["window_key"] for row in publications}) != len(publications):
        raise ValueError("Retained alert publication windows are duplicated")
    for publication in publications:
        if (publication["source"] != "SHADOW" or publication["arm"] != "PRIORITY"
            or publication["policy_version"] != policy["policy_version"]
            or utc(publication["window_key"]) <= utc(state["enrolled_at"])
            or not utc(publication["window_key"]) <= utc(publication["deadline"])
            <= utc(publication["actual_publication_at"]) <= inputs.cutoff
            or set(publication["revision_ids"]) - revision_ids):
            raise ValueError("Retained alert publication identity/clock drift")
        if set(publication["selected"]) - publication["candidates"].keys():
            raise ValueError("Retained alert selected candidate evidence is missing")
        if set(publication["expected_members"]) != {member["security_id"] for member in members}:
            raise ValueError("Retained alert publication enrollment drift")
        input_deadline = utc(publication["input_deadline"])
        if input_deadline > utc(publication["actual_publication_at"]) or any(
            availability[revision] > input_deadline for revision in publication["revision_ids"]
        ):
            raise ValueError("Retained publication used unavailable input evidence")
        for candidate in publication["candidates"].values():
            if (candidate["security_id"], candidate["ticker"]) not in {
                (member["security_id"], member["ticker"]) for member in members
            } or set(candidate["revision_ids"]) - revision_ids:
                raise ValueError("Retained alert candidate input identity drift")
            if (utc(candidate["trigger_at"]) > utc(publication["window_key"])
                or utc(candidate["available_at"]) > input_deadline
                or any(availability[revision] > input_deadline for revision in candidate["revision_ids"])):
                raise ValueError("Retained candidate used unavailable input evidence")
        for episode_id in publication["selected"]:
            if (publication["candidates"][episode_id]["model"] != "discovery"
                and episode_id not in state.get("positions", {})):
                raise ValueError("Retained selected alert lifecycle evidence is unavailable")
    state["bars"] = {}
    for bar in bars:
        state["bars"].setdefault(bar["security_id"] + "|" + bar["interval"], {})[bar["bar_start"]] = bar
    state["actions"] = actions
    state = update_positions(state, policy, inputs.cutoff)
    for publication in publications:
        publication["outcomes"] = {key: state.get("positions", {})[key] for key in publication["selected"]
                                   if key in state.get("positions", {})}
    snapshot = replay_snapshot(publications, dict(policy, end=str(inputs.session)),
                               inputs.cutoff.isoformat(), policy["policy_version"])
    snapshot.update(source="SHADOW", source_id=policy["policy_version"],
                    source_label="Realtime-owned forward shadow / paper only",
                    current_session=str(inputs.session),
                    warnings=["Forward paper only; no brokerage orders",
                              MISSING_PRODUCERS["context"]])
    boundary = state.get("next_boundary")
    if boundary is not None:
        cutoff, deadline = window_clock(utc(boundary))
        if state.get("dispatch_policy") == READINESS_POLICY:
            cutoff = utc(boundary) + timedelta(minutes=policy["provider_delay_minutes"])
            deadline = readiness_deadline(utc(boundary))
        if utc(boundary) <= max((utc(row["window_key"]) for row in publications), default=utc(state["enrolled_at"])):
            raise ValueError("Retained alert next boundary regressed")
        snapshot.update(publication_window_start=cutoff.isoformat(), publication_deadline=deadline.isoformat())
    snapshot = enrich_replay(snapshot, dict(bars=bars, actions=actions), policy)
    for row in snapshot["alerts"]:
        row["context"] = dict(status="UNAVAILABLE", reason=MISSING_PRODUCERS["context"], factors={})
        row["company_name"] = next(member["company_name"] for member in inputs.members
                                   if str(member["security_id"]) == row["security_id"])
    instance = strategy_instance(policy, state, retained.stream)
    records = [dict(instance_id=instance["instance_id"], stream=retained.stream, label=instance["label"],
                    record_id=row["window_key"], payload=row) for row in publications]
    schedule = dict(stream=retained.stream, label=instance["label"], status="UNAVAILABLE")
    if boundary is not None:
        schedule.update(status="RETAINED", publication_window_start=snapshot["publication_window_start"],
                        publication_deadline=snapshot["publication_deadline"], source_as_of=inputs.cutoff.isoformat())
    return namespace_snapshot(instance, snapshot), instance, records, schedule


def produce_realtime_stock_alerts(inputs: RealtimeScreeningInput, *, streams=()):
    inputs = alert_source_inputs(inputs)
    bars, actions = alert_domain_inputs(inputs)
    policy = forward_config(quality_version=2)
    packets = evaluate_alert_inputs(inputs, bars, actions, policy)
    if len({stream.stream for stream in streams}) != len(streams):
        raise ValueError("Realtime alert streams must remain independent and unique")
    snapshots, instances, records, schedules = [], [], [], []
    for stream in streams:
        snapshot, instance, publications, schedule = _retained_snapshot(inputs, stream, bars, actions)
        snapshots.append(snapshot)
        instances.append(instance)
        records.extend(publications)
        schedules.append(schedule)
    if snapshots:
        shadow = combine_snapshots(snapshots, instances)
        shadow["current_session"] = str(inputs.session)
    else:
        shadow = empty_snapshot("SHADOW")
        shadow.update(status="UNAVAILABLE", as_of=inputs.cutoff.isoformat(),
                      sessions=session_dates(str(inputs.session)), current_session=str(inputs.session),
                      warnings=[MISSING_PRODUCERS["forward"], MISSING_PRODUCERS["context"]])
    observations = []
    packets_by_member_interval = {}
    for row in packets:
        packets_by_member_interval.setdefault((row["security_id"], row["interval"]), []).append(row)
    for member in inputs.members:
        for interval in ("30m", "1h", "1d"):
            own = packets_by_member_interval.get((str(member["security_id"]), interval), ())
            latest = max(own, key=lambda row: row["market_time"], default=None)
            ready = latest is not None and latest["ready"] and latest["available_at"] <= inputs.cutoff
            observations.append(dict(
                ticker=member["ticker"], security_id=str(member["security_id"]), interval=interval,
                status="EVALUATED" if ready else "UNAVAILABLE",
                candidates=len(latest["candidates"]) if latest else 0, selected=False,
                reason_codes=[] if ready else ["ALERT_DATED_CONTEXT_OR_TIMELY_INPUT_UNAVAILABLE"],
                market_time=latest["market_time"].isoformat() if latest else None,
                available_at=latest["available_at"].isoformat() if latest else None,
            ))
    payload = dict(
        version=STOCK_ALERTS_VERSION, references=list(inputs.members), shadow=shadow, streams=snapshots,
        review_publications=records, observations=observations, schedule_streams=schedules,
        missing_producers={key: value for key, value in MISSING_PRODUCERS.items()
                           if key != "forward" or not streams},
        provider_fetch_performed=False, publication_permission=False, execution_permission=False,
    )
    hourly_lineage = {row["revision_id"]: row["source_revision_ids"] for row in derive_hours(bars, policy)}
    # Lineage-head manifest: constant-size identity and digests only. The member list is in the payload
    # under `references`; the reader recomputes `reference_set` from it.
    manifest = dict(
        version=STOCK_ALERTS_VERSION, cohort_id=str(inputs.cohort_id),
        provider_tier=inputs.provider_tier.value, source_generation=inputs.source_generation,
        source_manifest_sha256=inputs.source_manifest_sha256,
        input_sha256=inputs_sha256(inputs), reference_set=reference_set(inputs.members),
        bar_lineage_sha256=inputs_bar_lineage_sha256(inputs),
        domain_hourly_lineage_sha256=canonical_sha256(hourly_lineage),
        policy_sha256=canonical_sha256(policy),
        producer_sha256=canonical_sha256(Path(__file__).read_text(encoding="utf-8")),
        detector_sha256=policy["model_source_sha256"],
        price_basis="ADJUSTED_DAILY_FEATURES_PINNED_RAW_SCALE_AND_RAW_30M_EXECUTION",
        retained_stream_sha256=canonical_sha256(streams),
    )
    require_bounded_manifest(manifest, name="Stock Alerts")
    return build_portal_snapshot(
        cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier,
        snapshot_type=STOCK_ALERTS_VERSION, snapshot_key="PLATFORM",
        source_generation=inputs.source_generation, source_manifest=manifest,
        payload=payload, generated_at=inputs.cutoff,
    )
