"""Durable forward-shadow publication using the shared stock idea decisions."""
from __future__ import annotations

from collections import defaultdict
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import tempfile
import zlib

import exchange_calendars
import numpy as np
import pandas as pd

from research.stock_idea_engine import candidate_record, decide_publication, digest, entry_gate, read_candidate
from research.stock_idea_replay import available_at, derive_hours, execution_times, session_windows, utc, valid_bar


FORWARD_VERSION = "stock_ideas_forward_shadow_v1"
QUALITY_VERSION = "stock_ideas_forward_quality_v2"
DATA_DELAY_MINUTES = 17
DISPATCH_GRACE_SECONDS = 5
READINESS_POLICY = dict(version="stock_ideas_source_ready_v2", intraday_wait_minutes=30,
    closing_wait_minutes=60, dispatch_grace_seconds=5, source_gate="COMPLETE_ENROLLED_30M_AND_CLOSING_DAILY")


def runtime_sources():
    backend = Path(__file__).resolve().parents[1]
    paths = ["research/stock_idea_engine.py", "research/stock_idea_models.py", "research/stock_idea_replay.py",
             "research/stock_idea_forward.py", "equity/stock_idea_forward_source.py", "scripts/run_stock_idea_worker.py"]
    return {path: hashlib.sha256((backend / path).read_bytes()).hexdigest() for path in paths}


def forward_config(*, quality_version=1):
    root = Path(__file__).resolve().parents[2]
    config = json.loads((root / "docs/stock_idea_pilot_config.json").read_text(encoding="utf-8"))
    config = dict(config, policy_version=FORWARD_VERSION, classification="FORWARD_SHADOW_UNQUALIFIED",
        availability="ACTUAL_OBSERVED_AND_CREATED", retained_live_from="1900-01-01T00:00:00+00:00",
        dispatch_grace_seconds=DISPATCH_GRACE_SECONDS, universe="ENROLLED_TRACKED_DAILY_PUBLICATION",
        production_publication=False, data_mutation=False,
        model_source_sha256=__import__("hashlib").sha256((root / "backend/research/stock_idea_models.py").read_bytes()).hexdigest())
    if quality_version not in (1, 2):
        raise ValueError("unsupported forward quality policy")
    if quality_version == 2:
        from research.stock_idea_models import ACCEPTANCE_CONFIRMATION_V2
        config.update(policy_version=QUALITY_VERSION, execution_quality="CURRENT_NATIVE_PRICE_AND_ENTRY_SLOT_V2")
        config["models"]["acceptance"].update(intraday_version="range_breakout_acceptance_intraday_v2",
            confirmation_policy=dict(ACCEPTANCE_CONFIRMATION_V2))
    return config


def decision_candidate(candidate, state, *, boundary, cutoff, actual_time, config):
    evidence = dict(status="UNAVAILABLE", price=None, market_time=None, revision_id=None,
        trigger_price=candidate.price, reward_risk=None, risk_pct=None)
    if candidate.model == "discovery":
        return candidate, evidence | dict(status="WATCH_ONLY")
    opening, ending = execution_times(candidate, actual_time, config)
    evidence.update(expected_entry_at=opening.isoformat() if opening else None, planned_exit_at=ending.isoformat())
    if opening is None:
        return replace(candidate, selection_block=candidate.selection_block or "NO_REMAINING_ENTRY_SLOT"), evidence | dict(status="NO_REMAINING_ENTRY_SLOT")
    bars = state.get("bars", {}).get(candidate.security_id + "|30m", {}).values()
    eligible = [bar for bar in bars if bar["security_id"] == candidate.security_id and bar["ticker"] == candidate.ticker
        and bar["interval"] == "30m" and utc(bar["bar_end"]) <= boundary
        and available_at(bar, config) <= cutoff and utc(bar["bar_end"]) + timedelta(minutes=config["provider_delay_minutes"]) <= cutoff]
    bar = max(eligible, key=lambda item: (utc(item["bar_end"]), utc(item["created_at"]), item["revision_id"]), default=None)
    if not bar or utc(bar["bar_end"]) != boundary or utc(bar["bar_end"]) < candidate.trigger_at:
        return replace(candidate, selection_block=candidate.selection_block or "DECISION_PRICE_UNAVAILABLE"), evidence
    if not valid_bar(bar) or bar["volume"] <= 0 or (utc(bar["bar_start"]), utc(bar["bar_end"])) not in session_windows(bar["session"]):
        return replace(candidate, selection_block=candidate.selection_block or "INVALID_DECISION_BAR"), evidence | dict(status="INVALID_DECISION_BAR")
    price = float(bar["close"])
    reason = entry_gate(candidate, price)
    risk = candidate.direction * (price - candidate.stop)
    room = candidate.direction * (candidate.target - price)
    evidence.update(status=reason or "READY", price=price, market_time=bar["bar_end"], revision_id=bar["revision_id"],
        observed_at=bar["system_observed_at"], created_at=bar["created_at"],
        reward_risk=room / risk if risk > 0 and room > 0 else None, risk_pct=risk / price if risk > 0 else None)
    if reason:
        return replace(candidate, selection_block=candidate.selection_block or reason), evidence
    proxy = replace(candidate, price=price, room_risk=room / risk,
        extension=abs(price - candidate.reference) / candidate.activation_atr if candidate.model != "resumption" else candidate.extension)
    return proxy, evidence


def next_boundary(after):
    calendar = exchange_calendars.get_calendar("XNYS")
    session = calendar.date_to_session(after.date(), direction="next")
    while True:
        for _, ending in session_windows(str(session.date())):
            if ending > after:
                return ending
        session = calendar.next_session(session)


def window_clock(boundary):
    cutoff = boundary + timedelta(minutes=DATA_DELAY_MINUTES)
    return cutoff, cutoff + timedelta(seconds=DISPATCH_GRACE_SECONDS)


def readiness_deadline(boundary):
    closing = session_windows(str(boundary.date()))[-1][1]
    minutes = READINESS_POLICY["closing_wait_minutes" if boundary == closing else "intraday_wait_minutes"]
    return boundary + timedelta(minutes=minutes) - timedelta(seconds=DISPATCH_GRACE_SECONDS)


def forward_decision(state, packets, *, boundary, actual_time, members, policy_hash, config, readiness=None):
    cutoff, latest_dispatch = window_clock(boundary)
    if readiness is not None:
        if state.get("dispatch_policy") != READINESS_POLICY:
            raise ValueError("source-ready dispatch must be explicitly enabled")
        cutoff = utc(readiness["input_cutoff"])
        if cutoff < boundary + timedelta(minutes=config["provider_delay_minutes"]):
            raise ValueError("source-ready cutoff precedes provider availability")
        latest_dispatch = readiness_deadline(boundary)
        policy_hash = digest(dict(base_policy_hash=policy_hash, dispatch_policy=READINESS_POLICY))
    if actual_time < cutoff:
        raise ValueError("cannot publish before the scheduled boundary plus processing delay")
    state = dict(state)
    enrollment = utc(state["enrolled_at"])
    if boundary <= enrollment:
        raise ValueError("pre-enrollment windows cannot become forward publications")
    expected = {member["security_id"] for member in members}
    quarantined = state.get("identity_breaks", {})
    pending = {key: read_candidate(value) for key, value in state.get("pending_candidates", {}).items()}
    updates, revision_ids, ready = [], set(), set()
    for packet in sorted(packets, key=lambda item: (item["security_id"], item["interval"])):
        if packet["market_time"] != boundary or packet["available_at"] > cutoff:
            continue
        revision_ids.update(packet["revision_ids"])
        updates.extend(packet["updates"])
        if packet["interval"] == "30m" and packet["ready"] and packet["security_id"] not in quarantined:
            ready.add(packet["security_id"])
        for candidate in packet["candidates"]:
            if candidate.trigger_at > enrollment and candidate.available_at <= cutoff:
                pending[candidate.episode_id] = candidate
    invalidated = {update["episode_id"] for update in updates
                   if update.get("episode_id") and update["kind"] in ("INVALIDATED", "EXPIRED", "DATA_RISK")}
    for episode_id in invalidated:
        pending.pop(episode_id, None)
    candidates = [replace(candidate, health="STALE") if candidate.security_id in quarantined
                  or candidate.interval != "1d" and candidate.security_id not in ready
                  else candidate for candidate in pending.values()]
    original_candidates = {candidate.episode_id: candidate for candidate in candidates}
    decision_prices = {}
    if config.get("execution_quality") == "CURRENT_NATIVE_PRICE_AND_ENTRY_SLOT_V2":
        checked = [decision_candidate(candidate, state, boundary=boundary, cutoff=cutoff, actual_time=actual_time, config=config)
                   for candidate in candidates]
        candidates = [candidate for candidate, _ in checked]
        decision_prices = {candidate.episode_id: evidence for candidate, evidence in checked}
        revision_ids.update(item["revision_id"] for item in decision_prices.values() if item["revision_id"])
    missed = actual_time > latest_dispatch
    engine_deadline = cutoff if missed else actual_time
    engine_state = {key: state[key] for key in ("positions", "selected", "missed", "risk_updates") if key in state}
    result_state, publication, outbox = decide_publication(engine_state, candidates, deadline=engine_deadline,
        now=actual_time, expected_members=expected, available_members=ready, window_key=boundary.isoformat(),
        policy_hash=policy_hash, revision_ids=revision_ids, updates=updates,
        max_active_positions=config["max_active_positions_per_arm"])
    result_state = state | result_state
    for episode_id in publication["selected"]:
        if episode_id in result_state.get("positions", {}):
            result_state["positions"][episode_id]["candidate"] = candidate_record(original_candidates[episode_id])
    publication.update(input_deadline=cutoff.isoformat(), scheduled_publication_at=cutoff.isoformat(),
        latest_dispatch_at=latest_dispatch.isoformat(), actual_publication_at=actual_time.isoformat(),
        source="SHADOW", arm="PRIORITY", session=str(boundary.date()),
        policy_version=config["policy_version"], universe_manifest_sha256=digest(members),
        runtime_sources=state.get("runtime_sources", {}),
        candidates={key: candidate_record(candidate) for key, candidate in original_candidates.items()})
    if decision_prices:
        publication["decision_prices"] = decision_prices
    if readiness is not None:
        publication.update(dispatch_policy=READINESS_POLICY, source_readiness=readiness,
            scheduled_publication_at=None, earliest_publication_at=(boundary + timedelta(minutes=config["provider_delay_minutes"])).isoformat())
    result_state["pending_candidates"] = {key: candidate_record(candidate) for key, candidate in pending.items()
                                           if candidate.expires_at > actual_time and key not in invalidated}
    result_state["last_boundary"] = boundary.isoformat()
    return result_state, publication, outbox


class ForwardPublicationLate(ValueError):
    pass


class ForwardStore:
    INPUT_KEYS = ("bars", "detectors", "contexts")

    def __init__(self, path, policy):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.policy_hash = digest(policy)
        self.saved_inputs = None
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS forward_manifest (singleton INTEGER PRIMARY KEY CHECK(singleton=1), policy_hash TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS forward_checkpoint (singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS forward_input_checkpoint (singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS forward_publications (window_key TEXT PRIMARY KEY, payload BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS forward_outbox (notification_id TEXT PRIMARY KEY, window_key TEXT NOT NULL, payload TEXT NOT NULL);
            """)
            row = connection.execute("SELECT policy_hash FROM forward_manifest WHERE singleton=1").fetchone()
            if row and row[0] != self.policy_hash:
                raise ValueError("forward policy changed; preserve the old store and enroll a new policy")
            connection.execute("INSERT OR IGNORE INTO forward_manifest VALUES (1,?,?)", (self.policy_hash, json.dumps(policy, sort_keys=True)))

    @staticmethod
    def encode(value):
        return zlib.compress(json.dumps(value, sort_keys=True, default=str, allow_nan=False).encode())

    @staticmethod
    def decode(value):
        return json.loads(zlib.decompress(value))

    def load(self):
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("BEGIN")
            row = connection.execute("SELECT payload FROM forward_checkpoint WHERE singleton=1").fetchone()
            if not row:
                return None
            state = self.decode(row[0])
            if state.pop("external_inputs", False):
                inputs = connection.execute("SELECT payload FROM forward_input_checkpoint WHERE singleton=1").fetchone()
                if not inputs:
                    raise ValueError("forward checkpoint is missing its retained inputs")
                state.update(self.decode(inputs[0]))
                self.saved_inputs = {key: state.get(key) for key in self.INPUT_KEYS}
        return state

    def save(self, state, publication=None, outbox=(), *, clock=None):
        inputs = {key: state[key] for key in self.INPUT_KEYS if key in state}
        inputs_changed = self.saved_inputs is None or any(state.get(key) is not self.saved_inputs.get(key) for key in self.INPUT_KEYS)
        encoded_inputs = self.encode(inputs) if inputs_changed else None
        checkpoint = {key: value for key, value in state.items() if key not in self.INPUT_KEYS}
        checkpoint["external_inputs"] = True
        deadline = utc(publication["latest_dispatch_at"]) if clock and publication and publication["coverage"] != "MISSED_PUBLICATION" else None
        with closing(sqlite3.connect(self.path, timeout=30)) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            if publication:
                existing = connection.execute("SELECT payload FROM forward_publications WHERE window_key=?", (publication["window_key"],)).fetchone()
                if existing:
                    return self.decode(existing[0])
                if deadline:
                    dispatched_at = clock()
                    if dispatched_at > deadline:
                        raise ForwardPublicationLate("publication preparation exceeded the dispatch deadline")
                    publication.update(actual_publication_at=dispatched_at.isoformat(), deadline=dispatched_at.isoformat())
                    for episode_id in publication["selected"]:
                        if episode_id in state.get("positions", {}):
                            state["positions"][episode_id]["publication_at"] = dispatched_at.isoformat()
                connection.execute("INSERT INTO forward_publications VALUES (?,?)", (publication["window_key"], self.encode(publication)))
                for item in outbox:
                    connection.execute("INSERT OR IGNORE INTO forward_outbox VALUES (?,?,?)",
                        (digest([publication["window_key"], item]), publication["window_key"], json.dumps(item, default=str, sort_keys=True)))
            if inputs_changed:
                connection.execute("INSERT INTO forward_input_checkpoint VALUES (1,?) ON CONFLICT(singleton) DO UPDATE SET payload=excluded.payload", (encoded_inputs,))
            connection.execute("INSERT INTO forward_checkpoint VALUES (1,?) ON CONFLICT(singleton) DO UPDATE SET payload=excluded.payload", (self.encode(checkpoint),))
            if deadline and clock() > deadline:
                raise ForwardPublicationLate("publication writes exceeded the dispatch deadline; rolled back")
        self.saved_inputs = {key: state.get(key) for key in self.INPUT_KEYS}
        return publication

    def publications(self):
        with closing(sqlite3.connect(self.path)) as connection:
            return [self.decode(row[0]) for row in connection.execute("SELECT payload FROM forward_publications ORDER BY window_key")]


@lru_cache(maxsize=6)
def ordinal_map(interval, end_session):
    calendar = exchange_calendars.get_calendar("XNYS")
    sessions = calendar.sessions_in_range("2021-01-01", end_session)
    if interval == "1d":
        return {str(session.date()): index for index, session in enumerate(sessions)}
    return {opening: ordinal for ordinal, opening in enumerate(opening for session in sessions
            for opening, _ in session_windows(str(session.date()), interval))}


def build_frame(bars, config, *, daily=False, actions=()):
    from research.stock_idea_models import feature_frames
    if not bars:
        return pd.DataFrame()
    if daily:
        adjusted = []
        split_terms = {(action["effective_date"], action.get("split_from"), action.get("split_to")) for action in actions
                       if action["security_id"] == bars[0]["security_id"] and action["action_type"] == "SPLIT"}
        for original in bars:
            bar = dict(original)
            factor = 1.
            for effective_date, split_from, split_to in sorted(split_terms):
                if original["session"] < effective_date:
                    if split_from is None or split_to is None or min(split_from, split_to) <= 0:
                        factor = None
                        break
                    factor *= split_from / split_to
            if factor is None or not math.isfinite(factor) or factor <= 0:
                continue
            for field in ("open", "high", "low", "close"):
                bar[field] = original[field] * factor
            bar.update(volume=original["volume"] / factor, price_basis="SPLIT_ADJUSTED", execution_scale=1 / factor,
                execution_scale_revision_ids=[original["revision_id"]], execution_scale_observed_at=original["system_observed_at"],
                execution_scale_created_at=original["created_at"])
            adjusted.append(bar)
        bars = adjusted
        if not bars:
            return pd.DataFrame()
    frame = feature_frames(dict(bars=bars, actions=actions), config, derive_hourly=False).get((bars[0]["security_id"], bars[0]["interval"]), pd.DataFrame())
    if frame.empty:
        return frame
    ordinals = ordinal_map(bars[0]["interval"], frame.session.iloc[-1])
    if daily:
        frame["ordinal"] = frame.session.map(ordinals)
    else:
        frame["ordinal"] = frame.bar_start.map(ordinals)
    return frame


def daily_rank_context(frames, members, deadline):
    cohort = {member["security_id"] for member in members}
    values, spy_returns = [], {}
    for (security, interval), frame in frames.items():
        if interval != "1d" or frame.empty:
            continue
        visible = frame.loc[frame.bar_end.lt(deadline) & frame.visible_at.le(deadline)]
        if visible.empty:
            continue
        row = visible.iloc[-1]
        if row.ticker == "SPY" and row.ready and pd.notna(row.return63):
            spy_returns[row.session] = float(row.return63)
        if (security in cohort and row.ready and row.warmup >= 253 and row.close >= 5
                and row.liquidity >= 20_000_000 and pd.notna(row.momentum) and pd.notna(row.return63)):
            values.append(row.to_dict())
    if not values:
        return {}
    latest = max(value["session"] for value in values)
    spy_return = spy_returns.get(latest)
    values = [value for value in values if value["session"] == latest]
    pool = pd.DataFrame(values).sort_values(["momentum", "security_id"], ascending=[False, True])
    pool["rs_percentile"] = pool.return63.rank(method="average", pct=True)
    ids = sorted(pool.revision_id.tolist())
    result = {}
    for rank, (_, row) in enumerate(pool.iterrows(), 1):
        value = row.to_dict()
        value.update(rs63=float(row.return63) - spy_return if spy_return is not None else None,
                     momentum_rank=rank, rank_count=len(pool), rank_revision_ids=ids, expected_rank_members=len(members))
        result[(row.security_id, row.session)] = value
    return result


def serialize_contexts(contexts):
    return {f"{security}|{session}": {key: value for key, value in row.items() if key in (
        "rs63", "rs_percentile", "close", "ema50", "ema50_prior10", "liquidity", "momentum", "visible_at",
        "rank_revision_ids", "rank_count", "momentum_rank", "expected_rank_members")}
        for (security, session), row in contexts.items()}


def restore_contexts(values):
    contexts = {}
    for key, row in values.items():
        security, session = key.split("|", 1)
        contexts[(security, session)] = dict(row, rs63=row["rs63"] if row["rs63"] is not None else float("nan"), visible_at=pd.Timestamp(row["visible_at"]))
    return contexts


def advance_detectors(state, batch, config, cutoff, *, bootstrap=False, frame_cache=None):
    from research.stock_idea_models import intraday_observations, daily_observations, higher_context_observations
    state = deepcopy(state)
    inputs = state.setdefault("bars", {})
    identities = {member["ticker"]: member["security_id"] for member in state["members"]}
    corrections = []
    for bar in batch["bars"]:
        if available_at(bar, config) > cutoff or not valid_bar(bar):
            continue
        if bar["ticker"] in identities and bar["security_id"] != identities[bar["ticker"]]:
            state.setdefault("identity_breaks", {})[identities[bar["ticker"]]] = "IDENTITY_CHANGED"
            continue
        key = f"{bar['security_id']}|{bar['interval']}"
        retained = inputs.setdefault(key, {})
        old = retained.get(bar["bar_start"])
        if old and old["revision_id"] != bar["revision_id"] and not bootstrap:
            corrections.append(dict(kind="LATE_INPUT_CORRECTION", security_id=bar["security_id"],
                                    original_revision_id=old["revision_id"], revision_id=bar["revision_id"]))
            state.setdefault("identity_breaks", {})[bar["security_id"]] = "INPUT_CORRECTION_REVIEW"
            continue
        retained[bar["bar_start"]] = bar
    state["actions"] = batch["actions"]
    grouped = {tuple(key.split("|", 1)): sorted(rows.values(), key=lambda bar: bar["bar_start"]) for key, rows in inputs.items()}
    cache = frame_cache if frame_cache is not None else {}

    def cached_frame(key, bars):
        if not bars:
            return pd.DataFrame()
        actions = [action for action in batch["actions"] if action["security_id"] == key[0]]
        signature = digest([[bar["revision_id"] for bar in bars], actions])
        if key not in cache or cache[key][0] != signature:
            cache[key] = (signature, build_frame(bars, config, daily=key[1] == "1d", actions=actions))
        return cache[key][1]

    frames = {key: cached_frame(key, bars) for key, bars in grouped.items() if bars}
    new_context = daily_rank_context({key: frame for key, frame in frames.items()
        if key[0] not in state.get("identity_breaks", {})}, state["members"], cutoff)
    stored_context = state.setdefault("contexts", {})
    for key, row in serialize_contexts(new_context).items():
        stored_context[key] = row
    contexts = restore_contexts(stored_context)
    packets = []
    detectors = state.setdefault("detectors", {})
    for member in state["members"]:
        security = member["security_id"]
        native = grouped.get((security, "30m"), [])
        hours = derive_hours(native, config)
        frames[(security, "1h")] = cached_frame((security, "1h"), hours)
        for interval in ("30m", "1h"):
            frame = frames.get((security, interval))
            if frame is None or frame.empty:
                continue
            key = f"{security}|{interval}"
            saved = detectors.get(key, {})
            initial = {(model, int(direction)): value for name, value in saved.get("states", {}).items() for model, direction in [name.split(":")]}
            sink = {}
            observations = intraday_observations(frame, interval, contexts, config, initial_state=initial,
                after_ordinal=saved.get("ordinal"), state_sink=sink)
            if security in state.get("identity_breaks", {}):
                for packet in observations:
                    packet.update(ready=False, candidates=[])
            for packet in observations:
                if packet["market_time"] > utc(state["enrolled_at"]):
                    packets.append(packet)
            detectors[key] = dict(ordinal=int(frame.ordinal.iloc[-1]), states={f"{model}:{direction}": value for (model, direction), value in sink.items()})
        daily = frames.get((security, "1d"))
        if daily is not None and not daily.empty:
            latest = daily.bar_end.iloc[-1].isoformat()
            if state.setdefault("daily_processed", {}).get(security) != latest:
                observations = daily_observations(daily, contexts, config) + higher_context_observations(daily, config)
                if security in state.get("identity_breaks", {}):
                    for packet in observations:
                        packet.update(ready=False, candidates=[])
                packets.extend(packet for packet in observations if packet["market_time"] > utc(state["enrolled_at"])
                               and packet["market_time"].isoformat() > state["daily_processed"].get(security, ""))
                state["daily_processed"][security] = latest
    if corrections:
        for packet in packets:
            packet["updates"].extend(item for item in corrections if item["security_id"] == packet["security_id"])
    state["last_source_read"] = cutoff.isoformat()
    state["corrections"] = list({digest(item): item for item in state.get("corrections", []) + corrections}.values())
    return state, packets


def packet_record(packet):
    return dict(security_id=packet["security_id"], interval=packet["interval"], market_time=str(packet["market_time"]),
        available_at=str(packet["available_at"]), ready=packet["ready"], revision_ids=packet["revision_ids"],
        updates=packet["updates"], candidates=[candidate_record(candidate) for candidate in packet["candidates"]])


def restore_packet(packet):
    return dict(packet, market_time=utc(packet["market_time"]), available_at=utc(packet["available_at"]),
                candidates=[read_candidate(record) for record in packet["candidates"]])


def update_positions(state, config, now):
    from research.stock_idea_replay import mark_position
    for episode_id, position in state.get("positions", {}).items():
        if position["state"] in ("CLOSED", "NO_FILL"):
            continue
        security = position["candidate"]["security_id"]
        if security in state.get("identity_breaks", {}):
            position.update(state="UNRESOLVED", reason=state["identity_breaks"][security])
            continue
        bars = list(state["bars"].get(f"{security}|30m", {}).values())
        state["positions"][episode_id] = mark_position(position, bars, state.get("actions", []), now, config)
    return state


def shadow_snapshot(state, publications, config, now):
    from research.stock_alerts import replay_snapshot, session_dates
    projected = []
    for publication in publications:
        item = dict(publication, outcomes={key: state.get("positions", {})[key] for key in publication["selected"] if key in state.get("positions", {})})
        projected.append(item)
    last_session = publications[-1]["session"] if publications else str(next_boundary(now).date())
    snapshot = replay_snapshot(projected, dict(config, end=last_session), now.isoformat(), config["policy_version"])
    snapshot.update(source="SHADOW", source_label="Multi-model forward shadow / tracked universe", source_id=config["policy_version"],
        as_of=now.isoformat(), status="READY" if publications else "WAITING_FOR_PUBLICATION",
        sessions=session_dates(last_session), warnings=["Unqualified forward shadow; no brokerage orders", "Known action coverage only"],
        enrolled_at=state["enrolled_at"], hit_coverage="Retained forward candidate windows since enrollment",
        next_publication_at=window_clock(utc(state["next_boundary"]))[0].isoformat())
    names = {member["security_id"]: member["ticker"] for member in state["members"]}
    by_run = {publication["window_key"]: publication for publication in publications}
    for row in snapshot["alerts"]:
        decision = by_run[row["run_id"]].get("decision_prices", {}).get(row["alert_id"])
        if decision:
            row["decision_price_evidence"] = dict(decision)
        security = row["security_id"]
        native = sorted(state["bars"].get(f"{security}|30m", {}).values(), key=lambda bar: bar["bar_end"])
        if not row["ticker"]:
            row["ticker"] = names.get(security, "")
        if native:
            mark = native[-1]
            row.update(latest_price=mark["close"], latest_price_at=mark["bar_end"])
            if now - utc(mark["bar_end"]) > timedelta(minutes=50):
                row["warnings"].append("Latest stored price may be stale; inspect its timestamp")
        row["warnings"] = [warning for warning in row["warnings"] if warning != "Paper only"] + ["Forward paper only"]
    snapshot["worker"] = dict(checked_at=now.isoformat(), next_boundary=state["next_boundary"],
        enrolled_members=len(state["members"]), identity_breaks=state.get("identity_breaks", {}),
        last_source_read=state.get("last_source_read"))
    if state.get("dispatch_policy") == READINESS_POLICY:
        boundary = utc(state.get("retry_boundary", state["next_boundary"]))
        snapshot.update(publication_mode="SOURCE_READINESS", next_publication_at=None,
            publication_window_start=(boundary + timedelta(minutes=config["provider_delay_minutes"])).isoformat(),
            publication_deadline=readiness_deadline(boundary).isoformat())
        snapshot["worker"]["dispatch_policy"] = READINESS_POLICY
    for record, publication in zip(snapshot["publications"], publications):
        if publication.get("retry_of"):
            record.update(trigger_at=publication["market_time"], retry_of=publication["retry_of"])
            snapshot["warnings"].append("A source-ready retry preserves the earlier incomplete run")
    return snapshot


def publish_view(path, snapshot):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False) as handle:
            temporary = handle.name
            json.dump(snapshot, handle, sort_keys=True, default=str, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary and Path(temporary).exists():
            Path(temporary).unlink()