"""Pinned-input replay support for the isolated stock idea engine."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import json
import math
from pathlib import Path
import sqlite3
import sys

import exchange_calendars
import pandas as pd

from research.stock_idea_engine import MODELS, PublicationLedger, candidate_record, decide_publication, digest, entry_gate, read_candidate


def utc(value):
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("all source clocks must be timezone aware")
    return stamp.astimezone(timezone.utc)


@lru_cache(maxsize=2048)
def session_windows(session, interval="30m"):
    calendar = exchange_calendars.get_calendar("XNYS")
    session = pd.Timestamp(session)
    opening = calendar.session_open(session).to_pydatetime()
    closing = calendar.session_close(session).to_pydatetime()
    if interval == "1d":
        return ((opening, closing),)
    if interval not in ("30m", "1h"):
        raise ValueError("unsupported execution/trigger interval")
    duration = timedelta(minutes=30 if interval == "30m" else 60)
    windows = []
    while opening < closing:
        ending = min(opening + duration, closing)
        windows.append((opening, ending))
        opening = ending
    return tuple(windows)


def publication_windows(config):
    calendar = exchange_calendars.get_calendar(config["calendar"])
    return tuple((str(session.date()), end, end + timedelta(minutes=config["publication_delay_minutes"]))
                 for session in calendar.sessions_in_range(config["start"], config["end"])
                 for _, end in session_windows(str(session.date())))


def available_at(bar, config):
    end = utc(bar["bar_end"])
    simulated = end + timedelta(minutes=config["provider_delay_minutes"])
    if end >= utc(config["retained_live_from"]):
        return max(simulated, utc(bar["system_observed_at"]), utc(bar["created_at"]))
    return simulated


def valid_bar(bar):
    values = [bar[name] for name in ("open", "high", "low", "close", "volume")]
    return (all(math.isfinite(value) for value in values) and min(values[:4]) > 0 and values[4] >= 0
            and bar["high"] >= max(bar["open"], bar["close"], bar["low"])
            and bar["low"] <= min(bar["open"], bar["close"], bar["high"]))


def validate_inputs(bundle, config):
    if bundle["config_sha256"] != digest(config):
        raise ValueError("input manifest does not match the frozen configuration")
    if (config["execution_interval"] != "30m" or config["cap_per_model"] != 3
            or config["production_publication"] or config["data_mutation"]
            or config["refill_after_display_dedup"]):
        raise ValueError("unsupported v1 execution or safety contract")
    cutoff = utc(bundle["source_cutoff"])
    errors, exclusions, seen = [], [], set()
    frozen = dict(provider_delay_minutes=15, publication_delay_minutes=17, feature_warmup_bars=200,
                  ema_adjust=False, atr_alpha=1 / 14, atr_shift=1, stop_buffer_atr=.1, minimum_room_risk=1)
    if any(config.get(name) != value for name, value in frozen.items()):
        errors.append("unsupported timing/indicator contract; v1 parameters are frozen")
    model_contracts = {
        "resumption": dict(rs_percentile_threshold=.7, range_bars=20, retracement_atr=[1, 3], trigger_age_bars=[2, 10], time_cap_minutes=240, daily_horizon_sessions=21),
        "acceptance": dict(range_bars=10, range_max_atr=4, true_range_ratio_5_20_max=.75, break_atr=.15, acceptance_bars=1, chase_atr=1, time_cap_minutes=120, daily_horizon_sessions=10),
        "failure": dict(range_bars=20, extension_atr=.5, reentry_atr=.1, deadline_bars=3, time_cap_minutes=60, daily_horizon_sessions=5),
        "discovery": dict(lookback_sessions=252, skip_sessions=21, tail_fraction=.1, entries=False)}
    for model, contract in model_contracts.items():
        if any(config["models"][model].get(name) != value for name, value in contract.items()):
            errors.append(f"unsupported {model} contract; implement a new adapter version before changing it")
    members = bundle["memberships"]
    cohort = set(bundle["covered_security_ids"])
    if not cohort or len(cohort) != len(bundle["covered_security_ids"]):
        errors.append("covered cohort must be nonempty and unique")
    if not cohort <= {member["security_id"] for member in members if member["session"] == config["start"]}:
        errors.append("covered cohort must be drawn from the first pinned dated universe")
    dates = {session for session, _, _ in publication_windows(config)}
    member_keys = set()
    for member in members:
        key = (member["session"], member["security_id"])
        if key in member_keys:
            errors.append(f"duplicate dated membership: {key}")
        member_keys.add(key)
        if utc(member["observed_at"]) > cutoff or utc(member["created_at"]) > cutoff or not member["universe_run_id"]:
            errors.append(f"unpinned membership: {key}")
    if not dates <= {member["session"] for member in members}:
        errors.append("missing expected dated universes")
    by_security = defaultdict(list)
    valid_windows = defaultdict(set)
    visible_windows = defaultdict(set)
    for bar in bundle["bars"]:
        key = (bar["security_id"], bar["interval"], bar["bar_start"])
        if key in seen:
            errors.append(f"duplicate pinned bar: {key}")
        seen.add(key)
        if max(utc(bar["system_observed_at"]), utc(bar["created_at"]), utc(bar["bar_end"])) > cutoff:
            errors.append(f"revision beyond source cutoff: {bar['revision_id']}")
        if utc(bar["system_observed_at"]) < utc(bar["bar_end"]):
            errors.append(f"final bar observed before completion: {bar['revision_id']}")
        if bar.get("execution_scale") is not None:
            if (not bar.get("execution_scale_revision_ids")
                    or max(utc(bar["execution_scale_observed_at"]), utc(bar["execution_scale_created_at"])) > cutoff):
                errors.append(f"unpinned daily execution scale: {bar['revision_id']}")
        if not bar["revision_id"] or bar["interval"] not in ("30m", "1d"):
            errors.append("replay accepts pinned native 30m and daily bars only")
            continue
        session = bar["session"]
        try:
            clock_valid = (utc(bar["bar_start"]), utc(bar["bar_end"])) in session_windows(session, bar["interval"])
        except ValueError:
            clock_valid = False
        if not clock_valid or not valid_bar(bar):
            exclusions.append(dict(security_id=bar["security_id"], revision_id=bar["revision_id"], reason="INVALID_OHLC_OR_CLOCK"))
        elif bar["interval"] == "30m":
            key = (bar["security_id"], session)
            valid_windows[key].add(utc(bar["bar_start"]))
            if available_at(bar, config) <= utc(bar["bar_end"]) + timedelta(minutes=config["publication_delay_minutes"]):
                visible_windows[key].add(utc(bar["bar_start"]))
        by_security[(bar["security_id"], bar["interval"])].append(bar)
    coverage = []
    for session in sorted(dates):
        expected = {member["security_id"] for member in members if member["session"] == session}
        covered = expected & cohort
        expected_windows = {start for start, _ in session_windows(session)}
        complete = sum(valid_windows[(security, session)] == expected_windows for security in covered)
        deadline_complete = sum(visible_windows[(security, session)] == expected_windows for security in covered)
        coverage.append(dict(session=session, dated_members=len(expected), expected_covered=len(covered),
            complete_covered=complete, deadline_complete_covered=deadline_complete,
            expected_security_windows=len(covered) * len(expected_windows),
            stored_valid_security_windows=sum(len(valid_windows[(security, session)]) for security in covered),
            deadline_visible_security_windows=sum(len(visible_windows[(security, session)]) for security in covered),
            excluded_population=sorted(expected - cohort)))
    if digest(bundle["bars"]) != bundle["bars_sha256"]:
        errors.append("pinned bar checksum mismatch")
    if digest(members) != bundle["memberships_sha256"]:
        errors.append("membership checksum mismatch")
    if digest(bundle["actions"]) != bundle["actions_sha256"]:
        errors.append("action checksum mismatch")
    for action in bundle["actions"]:
        if max(utc(action["first_observed_at"]), utc(action["created_at"])) > cutoff:
            errors.append("corporate action revision beyond cutoff")
    expected_total = sum(row["expected_security_windows"] for row in coverage)
    visible_total = sum(row["deadline_visible_security_windows"] for row in coverage)
    return dict(status="BLOCKED" if errors else "VALIDATED", errors=errors, exclusions=exclusions,
                stored_bar_count=len(bundle["bars"]), dated_membership_count=len(members), action_count=len(bundle["actions"]),
                expected_security_windows=expected_total, deadline_visible_security_windows=visible_total,
                deadline_visible_fraction=visible_total / expected_total if expected_total else None,
                coverage=coverage, windows=len(publication_windows(config)), source_cutoff=bundle["source_cutoff"],
                classification=config["classification"], action_coverage=bundle["action_coverage"],
                limitation="Covered population only; reconstructed timing before retained_live_from; no bid/ask or borrow validation")


def derive_hours(bars, config):
    grouped = defaultdict(dict)
    for bar in bars:
        if bar["interval"] == "30m":
            grouped[(bar["security_id"], bar["session"])][utc(bar["bar_start"])] = bar
    result = []
    for (_, session), by_start in sorted(grouped.items()):
        for start, end in session_windows(session, "1h"):
            windows = [(opening, closing) for opening, closing in session_windows(session) if start <= opening < end]
            sources = [by_start.get(opening) for opening, _ in windows]
            if not sources or any(bar is None or not valid_bar(bar) or utc(bar["bar_end"]) != closing
                                  for bar, (_, closing) in zip(sources, windows)):
                continue
            ids = [bar["revision_id"] for bar in sources]
            result.append(dict(sources[0], interval="1h", bar_start=start.isoformat(), bar_end=end.isoformat(),
                open=sources[0]["open"], high=max(bar["high"] for bar in sources),
                low=min(bar["low"] for bar in sources), close=sources[-1]["close"],
                volume=sum(bar["volume"] for bar in sources), revision_id=digest([config["hourly_derivation"], ids]),
                source_revision_ids=ids, visible_at=max(available_at(bar, config) for bar in sources).isoformat(),
                system_observed_at=max(utc(bar["system_observed_at"]) for bar in sources).isoformat(),
                created_at=max(utc(bar["created_at"]) for bar in sources).isoformat()))
    return result


def execution_times(candidate, publication, config):
    calendar = exchange_calendars.get_calendar("XNYS")
    session = str(candidate.trigger_at.date())
    if candidate.interval == "1d":
        entry_session = str(calendar.next_session(session).date())
        opening = session_windows(entry_session)[0][0]
        horizon = config["models"][candidate.model]["daily_horizon_sessions"]
        ending = calendar.session_close(calendar.session_offset(session, horizon)).to_pydatetime()
        return (opening if opening > publication else None), ending
    windows = session_windows(session)
    opening = next((start for start, _ in windows if start > publication), None)
    if opening is None:
        return None, windows[-1][1]
    cap = opening + timedelta(minutes=config["models"][candidate.model]["time_cap_minutes"])
    ending = next((end for _, end in windows if end >= cap), windows[-1][1])
    return opening, ending


def mark_position(position, bars, actions, now, config):
    if position["state"] in ("CLOSED", "NO_FILL"):
        return position
    position = json.loads(json.dumps(position))
    candidate = read_candidate(position["candidate"])
    if candidate.health not in ("READY", "STALE"):
        return position | dict(state="UNRESOLVED", reason=candidate.health)
    publication = utc(position["publication_at"])
    opening, ending = execution_times(candidate, publication, config)
    position.update(expected_entry_at=opening.isoformat() if opening else None, planned_exit_at=ending.isoformat())
    if opening is None:
        return position | dict(state="NO_FILL", reason="SESSION_CLOSED_BEFORE_ENTRY", gross=0., cost_applies=False)
    calendar = exchange_calendars.get_calendar("XNYS")
    expected = [(start, end) for session in calendar.sessions_in_range(opening.date(), ending.date())
                for start, end in session_windows(str(session.date())) if start >= opening and end <= ending]
    indexed = {utc(bar["bar_start"]): bar for bar in bars if bar["interval"] == "30m"
               and bar["security_id"] == candidate.security_id and available_at(bar, config) <= now}
    path_ids = []
    for start, end in expected:
        if end + timedelta(minutes=config["provider_delay_minutes"]) > now:
            break
        bar = indexed.get(start)
        if bar is None or utc(bar["bar_end"]) != end or not valid_bar(bar):
            return position | dict(state="UNRESOLVED", reason="MISSING_OR_INVALID_EXECUTION_BAR", missing_at=start.isoformat())
        if any(action["security_id"] == candidate.security_id and opening.date() <= date.fromisoformat(action["effective_date"]) <= end.date()
               and action["action_type"] in ("SPLIT", "MERGER", "SYMBOL_CHANGE", "SPINOFF") for action in actions):
            return position | dict(state="UNRESOLVED", reason="CORPORATE_ACTION_UNRESOLVED")
        path_ids.append(bar["revision_id"])
        if start == opening:
            if bar["volume"] == 0:
                return position | dict(state="NO_FILL", reason="ZERO_VOLUME", gross=0., cost_applies=False,
                                       path_revision_ids=path_ids)
            problem = entry_gate(candidate, bar["open"])
            if problem:
                return position | dict(state="NO_FILL", reason=problem, gross=0., cost_applies=False,
                                       path_revision_ids=path_ids)
            position.update(state="OPEN", entry_price=bar["open"], entry_at=start.isoformat(), cost_applies=True)
        elif bar["volume"] == 0:
            return position | dict(state="UNRESOLVED", reason="NON_TRADING_EXECUTION_PATH")
        direction = candidate.direction
        exit_price, reason = None, None
        if direction * (bar["open"] - candidate.stop) <= 0:
            exit_price, reason = bar["open"], "STOP_GAP"
        elif direction * (bar["open"] - candidate.target) >= 0:
            exit_price, reason = candidate.target, "TARGET_GAP_CONSERVATIVE"
        else:
            stopped = bar["low"] <= candidate.stop if direction == 1 else bar["high"] >= candidate.stop
            targeted = bar["high"] >= candidate.target if direction == 1 else bar["low"] <= candidate.target
            if stopped:
                exit_price, reason = candidate.stop, "STOP_FIRST_AMBIGUOUS" if targeted else "STOP"
            elif targeted:
                exit_price, reason = candidate.target, "TARGET"
            elif end == ending:
                exit_price, reason = bar["close"], "TIME_OR_SESSION_CLOSE"
        position.update(mark_price=bar["close"], mark_at=end.isoformat(), path_revision_ids=path_ids)
        if exit_price is not None:
            gross = direction * (exit_price / position["entry_price"] - 1)
            return position | dict(state="CLOSED", exit_price=exit_price, exit_at=end.isoformat(),
                exit_time_precision="30M_BAR_BOUNDARY_NOT_INTRABAR_SEQUENCE", reason=reason, gross=gross,
                net_by_cost_bps={str(cost): gross - cost / 10000 for cost in config["cost_bps"]})
    return position


def build_observations(bundle, config, workers=1):
    from concurrent.futures import ThreadPoolExecutor
    from research.stock_idea_models import daily_contexts, daily_observations, feature_frames, higher_context_observations, intraday_observations
    print("Preparing frozen feature frames", file=sys.stderr, flush=True)
    frames = feature_frames(bundle, config)
    contexts, rank_coverage = daily_contexts(frames, bundle["memberships"], config)
    cohort = set(bundle["covered_security_ids"])
    jobs = [(key, frame.loc[frame.session.le(config["end"])]) for key, frame in sorted(frames.items()) if key[0] in cohort]

    def detect_job(job):
        (_, interval), frame = job
        if interval == "1d":
            result = daily_observations(frame, contexts, config) + higher_context_observations(frame, config)
        else:
            result = intraday_observations(frame, interval, contexts, config)
        return [observation for observation in result if config["start"] <= observation["market_time"].date().isoformat() <= config["end"]]

    def collect(iterator):
        results = []
        for index, result in enumerate(iterator, 1):
            results.append(result)
            if index % 50 == 0 or index == len(jobs):
                print(f"Detected {index}/{len(jobs)} frozen security/interval histories", file=sys.stderr, flush=True)
        return results

    if workers == 1:
        results = collect(map(detect_job, jobs))
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = collect(executor.map(detect_job, jobs))
    observations = sorted((item for result in results for item in result),
                          key=lambda item: (item["market_time"], item["security_id"], item["interval"]))
    return observations, rank_coverage


def replay_observations(bundle, config, observations, ledger, *, now_override=None):
    by_boundary = defaultdict(list)
    for observation in observations:
        by_boundary[observation["market_time"]].append(observation)
    dated = defaultdict(set)
    for member in bundle["memberships"]:
        if member["security_id"] in bundle["covered_security_ids"]:
            dated[member["session"]].add(member["security_id"])
    policy_hash = digest(config)
    variants = [("PRIORITY", "PRIORITY", None), ("ALL", "ALL", None), ("MOMENTUM", "MOMENTUM", None)]
    variants.extend((f"RANDOM_{seed}", "RANDOM", seed) for seed in config["random_seeds"])
    pending, outcomes, publications = {}, {}, []
    bars = [bar for bar in bundle["bars"] if bar["interval"] == "30m"]
    bars_by_security = defaultdict(list)
    for bar in bars:
        bars_by_security[bar["security_id"]].append(bar)
    windows = publication_windows(config)
    for window_index, (session, boundary, deadline) in enumerate(windows, 1):
        packets = sorted(by_boundary[boundary], key=lambda item: (item["security_id"], item["interval"]))
        visible = [packet for packet in packets if packet["available_at"] <= deadline]
        updates, revisions, lifecycles = [], [], {}
        available = {packet["security_id"] for packet in visible if packet["interval"] == "30m" and packet["ready"]}
        for packet in visible:
            updates.extend(packet["updates"])
            revisions.extend(packet["revision_ids"])
            if packet.get("lifecycles"):
                lifecycles[f"{packet['security_id']}:{packet['interval']}"] = packet["lifecycles"]
            for candidate in packet["candidates"]:
                if candidate.trigger_at.date().isoformat() < config["start"]:
                    continue
                pending[candidate.episode_id] = candidate
                if candidate.model != "discovery":
                    outcomes.setdefault(candidate.episode_id, dict(state="PENDING", candidate=candidate_record(candidate),
                                                                    publication_at=deadline.isoformat()))
        terminal_ids = {update["episode_id"] for update in updates if update["kind"] in ("INVALIDATED", "EXPIRED", "DATA_RISK")}
        for episode_id in terminal_ids:
            pending.pop(episode_id, None)
        with sqlite3.connect(ledger.path) as connection:
            lifecycles = ledger.catalog_state(connection, {"lifecycles": lifecycles})["lifecycles"]
        candidates = []
        for candidate in pending.values():
            missing = candidate.interval == "30m" and candidate.security_id not in available
            if candidate.interval == "1h" and any(end == boundary for _, end in session_windows(session, "1h")):
                missing = not any(packet["security_id"] == candidate.security_id and packet["interval"] == "1h" and packet["ready"] for packet in visible)
            candidates.append(replace(candidate, health="STALE" if missing else candidate.health))
        sizes = None
        for variant, arm, seed in variants:
            inputs = dict(candidates=[candidate_record(candidate) for candidate in sorted(candidates, key=lambda item: item.episode_id)],
                          expected_members=sorted(dated[session]), revision_ids=sorted(set(revisions)), updates=sorted(updates, key=digest))

            def decide(state):
                for episode_id, position in state.get("positions", {}).items():
                    if position["state"] in ("CLOSED", "NO_FILL"):
                        continue
                    state["positions"][episode_id] = mark_position(position, bars_by_security[position["candidate"]["security_id"]],
                                                                  bundle["actions"], deadline, config)
                state.setdefault("lifecycles", {}).update(lifecycles)
                return decide_publication(state, candidates, deadline=deadline, now=now_override or deadline,
                    expected_members=dated[session], available_members=available, window_key=boundary.isoformat(),
                    policy_hash=policy_hash, revision_ids=revisions, updates=updates, arm=arm, seed=seed,
                    max_active_positions=config["max_active_positions_per_arm"], trade_sizes=sizes if arm == "RANDOM" else None)

            publication = ledger.commit(state_key=variant, window_key=boundary.isoformat(), policy_hash=policy_hash,
                                        inputs=inputs, decide=decide)
            publications.append(dict(publication, arm=variant, session=session))
            if arm == "PRIORITY":
                sizes = {model: sum(row["model"] == model and row["selection"] == "SELECTED"
                                    for row in publication["dispositions"]) for model in MODELS}
        pending = {episode_id: candidate for episode_id, candidate in pending.items() if candidate.expires_at > deadline}
        if window_index % 13 == 0 or window_index == len(windows):
            print(f"Published {window_index}/{len(windows)} windows through {session}", file=sys.stderr, flush=True)
    final_time = min(utc(bundle["source_cutoff"]), session_windows(config["exit_end"])[-1][1] + timedelta(minutes=15))
    final_outcomes = {episode_id: mark_position(position, bars_by_security[position["candidate"]["security_id"]],
                                               bundle["actions"], final_time, config) for episode_id, position in outcomes.items()}
    for publication in publications:
        publication["outcomes"] = {episode_id: mark_position(
            dict(outcomes[episode_id], publication_at=publication["deadline"]),
            bars_by_security[outcomes[episode_id]["candidate"]["security_id"]], bundle["actions"], final_time, config)
            for episode_id in publication["selected"] if episode_id in outcomes}
    return publications, final_outcomes


def summarize(publications, outcomes, config, evaluation_plan=None):
    import numpy as np
    from collections import Counter
    from research.stock_idea_evaluation import block_statistics
    if evaluation_plan is None:
        evaluation_plan = json.loads((Path(__file__).resolve().parents[2] / "docs/stock_idea_evaluation_plan.json").read_text(encoding="utf-8"))
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = [str(session.date()) for session in calendar.sessions_in_range(config["start"], config["end"])]
    blocks = [sessions[index:index + config["bootstrap_block_sessions"]]
              for index in range(0, len(sessions), config["bootstrap_block_sessions"])]
    cells = []
    arms = sorted({publication["arm"] for publication in publications})
    for arm in arms:
        arm_publications = [publication for publication in publications if publication["arm"] == arm]
        for model in ("resumption", "acceptance", "failure"):
            for interval in ("30m", "1h", "1d"):
                for direction in (1, -1):
                    selected = [(publication, row["episode_id"]) for publication in arm_publications
                                for row in publication["dispositions"] if row["selection"] == "SELECTED"
                                and row["model"] == model and row["interval"] == interval and row["direction"] == direction]
                    complete = [(publication, publication["outcomes"][episode_id]) for publication, episode_id in selected
                                if episode_id in publication["outcomes"] and publication["outcomes"][episode_id]["state"] in ("CLOSED", "NO_FILL")]
                    for cost in config["cost_bps"]:
                        values = [position["gross"] - cost / 10000 * position["cost_applies"] for _, position in complete]
                        block_size = evaluation_plan["block_sessions"]["daily"][model] if interval == "1d" else evaluation_plan["block_sessions"]["intraday"]
                        statistics = block_statistics([(publication["session"], value) for (publication, _), value in zip(complete, values)],
                                                      sessions, block_size, evaluation_plan)
                        cells.append(dict(arm=arm, model=model, interval=interval, direction=direction, cost_bps=cost,
                            selected=len(selected), completed=len(complete), mean_net=float(np.mean(values)) if values else None,
                            block_interval95=statistics["interval95"], populated_blocks=statistics["populated_blocks"],
                            block_sessions=block_size, inference_status=statistics["reason"], role="DESCRIPTIVE_NOT_PRIMARY_QUALIFICATION"))
    paired = []
    baseline = {publication["window_key"]: publication for publication in publications if publication["arm"] == "ALL"}
    for arm in (name for name in arms if name != "ALL"):
        for model in ("resumption", "acceptance", "failure"):
            for cost in config["cost_bps"]:
                lifts, incomplete = [], 0
                for publication in (item for item in publications if item["arm"] == arm):
                    comparison = baseline[publication["window_key"]]
                    sides = [[position for position in side["outcomes"].values() if position["candidate"]["model"] == model]
                             for side in (publication, comparison)]
                    if not all(sides):
                        continue
                    if any(position["state"] not in ("CLOSED", "NO_FILL") for side in sides for position in side):
                        incomplete += 1
                        continue
                    means = [float(np.mean([position["gross"] - cost / 10000 * position["cost_applies"] for position in side])) for side in sides]
                    lifts.append((publication["session"], means[0] - means[1]))
                block_size = evaluation_plan["block_sessions"]["pooled"][model]
                statistics = block_statistics(lifts, sessions, block_size, evaluation_plan)
                paired.append(dict(arm=arm, baseline="ALL", model=model, cost_bps=cost, paired_windows=len(lifts),
                    incomplete_windows=incomplete, all_decision_windows=len(baseline),
                    mean_window_selection_lift=float(np.mean([lift for _, lift in lifts])) if lifts else None,
                    block_interval95=statistics["interval95"], block_sessions=block_size,
                    role="DESCRIPTIVE_ALL_CANDIDATE_COMPARISON"))
    random_distribution = []
    for model in ("resumption", "acceptance", "failure"):
        for cost in config["cost_bps"]:
            values = [item["mean_window_selection_lift"] for item in paired if item["arm"].startswith("RANDOM_")
                      and item["model"] == model and item["cost_bps"] == cost and item["mean_window_selection_lift"] is not None]
            random_distribution.append(dict(model=model, cost_bps=cost, declared_seeds=config["random_seeds"],
                available_seeds=len(values), mean_lift=float(np.mean(values)) if values else None,
                median_lift=float(np.median(values)) if values else None,
                minimum_lift=min(values) if values else None, maximum_lift=max(values) if values else None))
    return dict(classification=config["classification"], sessions=len(sessions), calendar_blocks=blocks,
        windows_per_arm=len(publication_windows(config)), examined_cells=len(cells) + len(paired), cells=cells,
        paired_selection_lift=paired, random_seed_distribution=random_distribution,
        opportunity_states=dict(Counter(position["state"] for position in outcomes.values())),
        notification_counts={arm: sum(len(publication["display"]) for publication in publications if publication["arm"] == arm) for arm in arms},
        inference="Development diagnostic; few calendar blocks; intervals are descriptive, not multiplicity-adjusted qualification",
        accounting="Per-opportunity only; no combined portfolio CAGR/drawdown; no quote, borrow, or full-market validation")


def write_once(path, value):
    content = json.dumps(value, sort_keys=True, indent=2, default=str, allow_nan=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise ValueError(f"refusing to overwrite a different artifact: {path}")
    else:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(content)


def run_replay(bundle, config, output, workers=1, evaluation_plan=None):
    from research.stock_idea_evaluation import evaluate_publications, validate_plan
    if evaluation_plan is None:
        evaluation_plan = json.loads((Path(__file__).resolve().parents[2] / "docs/stock_idea_evaluation_plan.json").read_text(encoding="utf-8"))
    validate_plan(evaluation_plan, config)
    if bundle.get("evaluation_plan_sha256", digest(evaluation_plan)) != digest(evaluation_plan):
        raise ValueError("frozen inputs are bound to a different evaluation protocol")
    validation = validate_inputs(bundle, config)
    if validation["errors"]:
        raise ValueError("; ".join(validation["errors"]))
    output = Path(output)
    if output.exists() and any(output.iterdir()) and not (output / "manifest.json").exists():
        raise ValueError("refusing to add replay artifacts to an unrelated nonempty directory")
    output.mkdir(parents=True, exist_ok=True)
    source_root = Path(__file__).resolve().parents[1]
    source_hashes = {name: __import__("hashlib").sha256((source_root / name).read_bytes()).hexdigest()
                     for name in ("research/stock_idea_engine.py", "research/stock_idea_models.py", "research/stock_idea_replay.py", "research/stock_idea_evaluation.py", "research/strategy_v2.py", "scripts/run_stock_idea_replay.py")}
    write_once(output / "manifest.json", dict(config=config, evaluation_plan=evaluation_plan,
        evaluation_plan_sha256=digest(evaluation_plan), study_id=evaluation_plan["study_id"], inputs_sha256=digest(bundle), source_hashes=source_hashes))
    write_once(output / "validation.json", validation)
    observations, rank_coverage = build_observations(bundle, config, workers)
    ledger = PublicationLedger(output / "replay.sqlite")
    publications, outcomes = replay_observations(bundle, config, observations, ledger)
    write_once(output / "daily_rank_coverage.json", rank_coverage)
    write_once(output / "publications.json", publications)
    write_once(output / "candidate_outcomes.json", outcomes)
    evaluation = evaluate_publications(publications, config, evaluation_plan, bundle["source_cutoff"])
    write_once(output / "evaluation.json", evaluation)
    summary = summarize(publications, outcomes, config, evaluation_plan)
    summary.update(study_id=evaluation_plan["study_id"], evaluation_plan_sha256=digest(evaluation_plan),
        primary_family_size=evaluation["primary_family_size"], publication_audit=evaluation["publication_audit"],
        model_decisions=[{key: cell[key] for key in ("model", "interval", "direction", "status")} for cell in evaluation["cells"]],
        inherited_qualification_ids=[])
    summary.update(source_cutoff=bundle["source_cutoff"], action_coverage=bundle["action_coverage"])
    write_once(output / "summary.json", summary)
    return summary