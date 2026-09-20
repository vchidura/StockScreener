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
COHORT_QUARANTINE_VERSION = "stock_alert_prospective_identity_quarantine_v1"


def runtime_sources():
    backend = Path(__file__).resolve().parents[1]
    paths = ["research/stock_idea_engine.py", "research/stock_idea_models.py", "research/stock_idea_replay.py",
             "research/stock_idea_forward.py", "equity/stock_idea_forward_source.py", "scripts/run_stock_idea_worker.py"]
    return {path: hashlib.sha256((backend / path).read_bytes()).hexdigest() for path in paths}


def forward_config(*, quality_version=1, swing=False):
    root = Path(__file__).resolve().parents[2]
    config = json.loads((root / "backend/research/inputs/stock_idea_pilot_config.json").read_text(encoding="utf-8"))
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
    if swing:
        if quality_version != 2:
            raise ValueError("swing policy requires quality version 2")
        from research.stock_idea_swing import HORIZONS, SWING_POLICY, SWING_VERSION
        config.update(policy_version=SWING_VERSION, holding_policy=SWING_POLICY,
            swing_source_sha256=hashlib.sha256((root / "backend/research/stock_idea_swing.py").read_bytes()).hexdigest(),
            confirmation_window="NEXT_SESSION_ONLY", swing_bracket="FROZEN_DAILY_STOP_TARGET",
            preconfirmation_path="COMPLETE_NATIVE_30M_NO_BRACKET_TOUCH", holding_count="ENTRY_SESSION_IS_SESSION_ONE",
            daily_entry="NEXT_NATIVE_OPEN_AFTER_INTRADAY_CONFIRMATION",
            candidate_lifetime="NEXT_SESSION_SETUP_WITH_FROZEN_CONFIRMATION_EXPIRY")
        for model in HORIZONS:
            config["models"][model].pop("time_cap_minutes", None)
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


def alert_selection_cohort(state, boundary):
    members = state["members"]
    enrolled_hash = digest(members)
    parent, excluded, active = enrolled_hash, {}, None
    prior_activation = utc(state["enrolled_at"])
    for record in state.get("alert_cohort_history", []):
        body = {key: value for key, value in record.items() if key != "generation_id"}
        if record.get("version") != COHORT_QUARANTINE_VERSION or record.get("generation_id") != digest(body):
            raise ValueError("invalid alert cohort revision")
        activated = utc(record["activated_at"])
        if record["enrolled_universe_sha256"] != enrolled_hash or record["parent_generation"] != parent or activated < prior_activation:
            raise ValueError("alert cohort enrollment or history mismatch")
        if utc(record["effective_boundary"]) != next_boundary(activated):
            raise ValueError("alert cohort exclusion must start at a future boundary")
        member = record["excluded_member"]
        identity = member["security_id"]
        if member not in members or identity in excluded or record["reason"] != "IDENTITY_CHANGED":
            raise ValueError("invalid alert identity quarantine member")
        excluded[identity] = member
        remaining = [item for item in members if item["security_id"] not in excluded]
        if not remaining or record["selection_universe_sha256"] != digest(remaining):
            raise ValueError("alert selection cohort does not reconcile")
        if utc(record["effective_boundary"]) <= utc(boundary):
            active = dict(record, excluded_members=list(excluded.values()), eligible_members=len(remaining))
        parent, prior_activation = record["generation_id"], activated
    if active is None:
        return members, None
    excluded_ids = {member["security_id"] for member in active["excluded_members"]}
    return [member for member in members if member["security_id"] not in excluded_ids], active


def quarantine_alert_member(state, security_id, ticker, activated_at):
    activated_at = utc(activated_at)
    if state.get("dispatch_policy") != READINESS_POLICY:
        raise ValueError("identity quarantine requires source-ready dispatch")
    members, _ = alert_selection_cohort(state, next_boundary(activated_at))
    enrolled = next((member for member in state["members"] if member["security_id"] == security_id and member["ticker"] == ticker), None)
    if enrolled is None or state.get("identity_breaks", {}).get(security_id) != "IDENTITY_CHANGED":
        raise ValueError("quarantine requires the exact enrolled identity and recorded identity break")
    existing = next((record for record in state.get("alert_cohort_history", []) if record["excluded_member"]["security_id"] == security_id), None)
    if existing:
        return state, existing
    if activated_at <= utc(state["enrolled_at"]) or (state.get("last_boundary") and activated_at <= utc(state["last_boundary"])):
        raise ValueError("quarantine activation cannot predate enrollment or processed boundaries")
    if len(members) <= 1:
        raise ValueError("cannot quarantine the entire alert cohort")
    history = state.get("alert_cohort_history", [])
    record = dict(version=COHORT_QUARANTINE_VERSION, activated_at=activated_at.isoformat(),
        effective_boundary=next_boundary(activated_at).isoformat(), excluded_member=dict(enrolled), reason="IDENTITY_CHANGED",
        authorization="EXPLICIT_OPERATOR_QUARANTINE", enrolled_universe_sha256=digest(state["members"]),
        parent_generation=history[-1]["generation_id"] if history else digest(state["members"]),
        selection_universe_sha256=digest([member for member in members if member["security_id"] != security_id]))
    record["generation_id"] = digest(record)
    changed = dict(state, alert_cohort_history=history + [record])
    alert_selection_cohort(changed, next_boundary(activated_at))
    return changed, record


def active_correction_recoveries(state, boundary, known_at):
    recovered = {}
    for record in state.get("correction_recovery_history", []):
        body = {key: value for key, value in record.items() if key != "generation_id"}
        if record.get("version") != "stock_alert_correction_recovery_v1" or digest(body) != record.get("generation_id"):
            raise ValueError("invalid correction recovery record")
        if record["enrollment_sha256"] != digest(state["members"]):
            raise ValueError("correction recovery enrollment mismatch")
        if utc(record["effective_boundary"]) != next_boundary(utc(record["activated_at"])):
            raise ValueError("correction recovery must start at a future boundary")
        if utc(record["activated_at"]) > known_at or utc(record["effective_boundary"]) > boundary:
            continue
        pairs = {tuple(pair[key] for key in ("security_id", "original_revision_id", "revision_id")) for pair in record["pairs"]}
        for security in {pair[0] for pair in pairs}:
            pending = {tuple(pair[key] for key in ("security_id", "original_revision_id", "revision_id"))
                for pair in state.get("corrections", []) if pair["security_id"] == security}
            if pending and pending <= pairs and state.get("identity_breaks", {}).get(security) == "INPUT_CORRECTION_REVIEW":
                recovered[security] = record
    return recovered


def correction_inventory_hash(state):
    return digest(sorted(state.get("corrections", []), key=lambda pair: (pair["security_id"], pair["original_revision_id"], pair["revision_id"])))


def correction_recovery_record(state, revised_bars, config, activated_at):
    members, _ = alert_selection_cohort(state, next_boundary(activated_at))
    active = {member["security_id"]: member["ticker"] for member in members}
    recovered = active_correction_recoveries(state, next_boundary(activated_at), activated_at)
    pairs = [pair for pair in state.get("corrections", []) if pair["security_id"] in active and pair["security_id"] not in recovered]
    if not pairs or len(pairs) > 1000:
        raise ValueError("correction recovery requires 1-1000 unresolved reviewed pairs")
    selected = {bar["revision_id"]: bar for bar in revised_bars}
    originals = {bar["revision_id"]: bar for rows in state.get("bars", {}).values() for bar in rows.values()}
    accepted, slots = {}, set()
    for pair in pairs:
        security = pair["security_id"]
        original, revised = originals.get(pair["original_revision_id"]), selected.get(pair["revision_id"])
        if state.get("identity_breaks", {}).get(security) != "INPUT_CORRECTION_REVIEW" or not original or not revised:
            raise ValueError("every correction needs exact retained and current source revisions")
        fields = ("security_id", "ticker", "interval", "session")
        if (any(original[field] != revised[field] for field in fields) or revised["ticker"] != active[security]
                or revised["interval"] != "30m" or any(utc(original[field]) != utc(revised[field]) for field in ("bar_start", "bar_end"))
                or not valid_bar(revised) or available_at(revised, config) > activated_at
                or utc(revised["bar_start"]) < activated_at - timedelta(days=7)
                or (utc(revised["bar_start"]), utc(revised["bar_end"])) not in session_windows(revised["session"])):
            raise ValueError("correction changes identity/clock or violates bounded causal input rules")
        slot = (security, revised["bar_start"])
        if slot in slots:
            raise ValueError("multiple correction revisions for one slot require separate review")
        slots.add(slot)
        accepted[revised["revision_id"]] = dict(revised)
    record = dict(version="stock_alert_correction_recovery_v1", activated_at=activated_at.isoformat(),
        effective_boundary=next_boundary(activated_at).isoformat(), authorization="EXPLICIT_OPERATOR_CORRECTION_RECOVERY",
        pairs=sorted(pairs, key=lambda pair: (pair["security_id"], pair["original_revision_id"], pair["revision_id"])),
        revised_bars=[accepted[key] for key in sorted(accepted)], enrollment_sha256=digest(state["members"]))
    record["generation_id"] = digest(record)
    return record


def corrected_detector_inputs(state, recoveries):
    inputs = dict(state.get("bars", {}))
    for security, record in recoveries.items():
        key = security + "|30m"
        retained = dict(inputs.get(key, {}))
        replacements = {pair["original_revision_id"]: pair["revision_id"] for pair in record["pairs"] if pair["security_id"] == security}
        revised = {bar["revision_id"]: bar for bar in record["revised_bars"]}
        for start, bar in retained.items():
            if bar["revision_id"] in replacements:
                retained[start] = revised[replacements[bar["revision_id"]]]
        inputs[key] = retained
    return inputs


def forward_decision(state, packets, *, boundary, actual_time, members, policy_hash, config, readiness=None):
    cutoff, latest_dispatch = window_clock(boundary)
    cohort = None
    if state.get("alert_cohort_history"):
        members, cohort = alert_selection_cohort(state, boundary)
        if cohort:
            if utc(cohort["activated_at"]) > actual_time:
                raise ValueError("alert cohort was not active at publication time")
            policy_hash = digest(dict(base_policy_hash=policy_hash, alert_cohort_generation=cohort["generation_id"]))
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
    recoveries = active_correction_recoveries(state, boundary, cutoff)
    quarantined = {security: reason for security, reason in state.get("identity_breaks", {}).items() if security not in recoveries}
    if recoveries:
        policy_hash = digest(dict(base_policy_hash=policy_hash,
            correction_recoveries=sorted({record["generation_id"] for record in recoveries.values()})))
    pending = {key: read_candidate(value) for key, value in state.get("pending_candidates", {}).items()}
    updates, revision_ids, ready = [], set(), set()
    for packet in sorted(packets, key=lambda item: (item["security_id"], item["interval"])):
        if packet["market_time"] != boundary or packet["available_at"] > cutoff:
            continue
        recovery_boundary = state.get("detector_recovery_boundaries", {}).get(packet["security_id"])
        if packet["interval"] in ("30m", "1h") and recovery_boundary and boundary < utc(recovery_boundary):
            continue
        revision_ids.update(packet["revision_ids"])
        updates.extend(packet["updates"])
        if packet["interval"] == "30m" and packet["ready"] and packet["security_id"] not in quarantined and packet["security_id"] in expected:
            ready.add(packet["security_id"])
        for candidate in packet["candidates"]:
            if candidate.trigger_at > enrollment and candidate.available_at <= cutoff:
                pending[candidate.episode_id] = candidate
    invalidated = {update["episode_id"] for update in updates
                   if update.get("episode_id") and update["kind"] in ("INVALIDATED", "EXPIRED", "DATA_RISK")}
    for episode_id in invalidated:
        pending.pop(episode_id, None)
    candidates = [replace(candidate, health="STALE", selection_block="OUTSIDE_ACTIVE_ALERT_COHORT") if candidate.security_id not in expected
                  else replace(candidate, health="STALE") if candidate.security_id in quarantined
                  or candidate.interval != "1d" and candidate.security_id not in ready
                  else candidate for candidate in pending.values()]
    swing_state, swing_evidence, swing_diagnostics = None, {}, []
    decision_state = dict(state, bars=corrected_detector_inputs(state, recoveries)) if recoveries else state
    if config.get("holding_policy") == "DAILY_SETUP_NEXT_SESSION_CONFIRMATION_V1":
        from research.stock_idea_swing import swing_candidates
        swing_state, candidates, swing_evidence, swing_diagnostics = swing_candidates(decision_state, candidates,
            boundary=boundary, cutoff=cutoff, actual_time=actual_time, ready=ready, invalidated=invalidated,
            may_seed=actual_time <= latest_dispatch, config=config)
    original_candidates = {candidate.episode_id: candidate for candidate in candidates}
    decision_prices = {}
    if config.get("execution_quality") == "CURRENT_NATIVE_PRICE_AND_ENTRY_SLOT_V2":
        checked = [decision_candidate(candidate, decision_state, boundary=boundary, cutoff=cutoff, actual_time=actual_time, config=config)
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
    if swing_state is not None:
        result_state["swing_setup_book"] = swing_state
        publication.update(swing_evidence=swing_evidence, swing_diagnostics=swing_diagnostics)
    for episode_id in publication["selected"]:
        if episode_id in result_state.get("positions", {}):
            result_state["positions"][episode_id]["candidate"] = candidate_record(original_candidates[episode_id])
            if episode_id in swing_evidence:
                result_state["positions"][episode_id]["swing_evidence"] = swing_evidence[episode_id]
            security = original_candidates[episode_id].security_id
            if security in recoveries:
                result_state["positions"][episode_id]["correction_recovery_generation"] = recoveries[security]["generation_id"]
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
    if cohort:
        publication["alert_selection_cohort"] = cohort
    if recoveries:
        publication["correction_recovery"] = [dict(generation_id=generation, effective_boundary=record["effective_boundary"],
            activated_at=record["activated_at"]) for generation, record in sorted(
                {record["generation_id"]: record for record in recoveries.values()}.items())]
    if state.get("input_reconciliation_history"):
        publication["input_reconciliation"] = [dict(version=record["version"], observed_at=record["observed_at"],
            effective_boundary=record["effective_boundary"], revision_sha256=digest(record["inserted_revision_ids"]))
            for record in state["input_reconciliation_history"] if utc(record["effective_boundary"]) <= boundary]
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

    def activate_identity_quarantine(self, security_id, ticker, activated_at):
        state = self.load()
        if state is None:
            raise ValueError("identity quarantine requires an existing enrollment")
        changed, record = quarantine_alert_member(state, security_id, ticker, activated_at)
        if changed is state:
            return state, dict(status="QUARANTINE_ALREADY_RECORDED", revision=record)
        backup_path = self.path.parent / "quarantine-backups" / record["generation_id"] / "forward.sqlite"
        backup_path.parent.mkdir(parents=True, exist_ok=False)
        with closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)) as source:
            source.execute("PRAGMA query_only=ON")
            with closing(sqlite3.connect(backup_path)) as target:
                source.backup(target)
        preservation = dict(generation_id=record["generation_id"], backup=str(backup_path.relative_to(self.path.parent)).replace("\\", "/"),
            backup_sha256=hashlib.sha256(backup_path.read_bytes()).hexdigest(),
            enrolled_members_sha256=digest(state["members"]), positions_sha256=digest(state.get("positions", {})),
            original_checkpoint_sha256=digest(state))
        changed["alert_quarantine_preservation"] = state.get("alert_quarantine_preservation", []) + [preservation]
        self.save(changed)
        with closing(sqlite3.connect(backup_path.resolve().as_uri() + "?mode=ro", uri=True)) as before, closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)) as after:
            for table in ("forward_manifest", "forward_input_checkpoint", "forward_publications", "forward_outbox"):
                if before.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() != after.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall():
                    raise ValueError("quarantine preservation check failed: " + table)
        restored = self.load()
        unchanged = {key: value for key, value in state.items() if key not in ("alert_cohort_history", "alert_quarantine_preservation")}
        if any(restored.get(key) != value for key, value in unchanged.items()) or restored != changed:
            raise ValueError("quarantine changed unrelated checkpoint state")
        return restored, dict(status="QUARANTINE_ACTIVATED", revision=record, preservation=preservation,
            preserved_tables=["forward_manifest", "forward_input_checkpoint", "forward_publications", "forward_outbox"],
            original_enrollment_unchanged=True, positions_unchanged=True, next_boundary_unchanged=True)

    def reconcile_retained_history(self, batch, config, observed_at, *, clock):
        state = self.load()
        if state is None:
            raise ValueError("history reconciliation requires an existing enrollment")
        members, _ = alert_selection_cohort(state, next_boundary(observed_at))
        active = {member["security_id"]: member["ticker"] for member in members}
        missing = []
        for bar in batch["bars"]:
            if bar["interval"] != "30m" or active.get(bar["security_id"]) != bar["ticker"]:
                continue
            retained = state.get("bars", {}).get(bar["security_id"] + "|30m", {})
            if (retained and bar["bar_start"] not in retained and valid_bar(bar) and available_at(bar, config) <= observed_at
                    and utc(bar["bar_start"]) >= observed_at - timedelta(days=7)
                    and utc(bar["bar_end"]) <= max(utc(row["bar_end"]) for row in retained.values())):
                missing.append(bar)
        if not missing:
            return state, dict(status="NO_MISSING_RETAINED_HISTORY", checked_at=observed_at.isoformat())
        generation = digest(dict(observed_at=observed_at.isoformat(), revision_ids=sorted(bar["revision_id"] for bar in missing)))
        backup_path = self.path.parent / "history-backups" / generation / "forward.sqlite"
        backup_path.parent.mkdir(parents=True, exist_ok=False)
        with closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)) as source:
            source.execute("PRAGMA query_only=ON")
            with closing(sqlite3.connect(backup_path)) as target:
                source.backup(target)
        cache = {}
        rebuilt, _ = advance_detectors(state, dict(bars=missing, actions=state.get("actions", [])), config, observed_at, frame_cache=cache)
        allowed = ("bars", "detectors", "pending_candidates", "prepared_packets", "detector_recovery_boundaries", "input_reconciliation_history")
        changed = state | {key: rebuilt[key] for key in allowed}
        affected = {bar["security_id"] for bar in missing}
        effective = next_boundary(clock()).isoformat()
        changed["detector_recovery_boundaries"].update({security: effective for security in affected})
        record = changed["input_reconciliation_history"][-1]
        record.update(effective_boundary=effective, backup=str(backup_path.relative_to(self.path.parent)).replace("\\", "/"),
            backup_sha256=hashlib.sha256(backup_path.read_bytes()).hexdigest(), authorization="EXPLICIT_OPERATOR_RECONCILIATION")
        for key, rows in state.get("bars", {}).items():
            if any(changed["bars"][key].get(start) != bar for start, bar in rows.items()):
                raise ValueError("history reconciliation changed an existing bar")
        ready = {}
        for interval in ("30m", "1h"):
            ready[interval] = sum(bool(cache[(security, interval)][1].ready.iloc[-1]) for security in active
                if (security, interval) in cache and not cache[(security, interval)][1].empty)
        self.save(changed)
        with closing(sqlite3.connect(backup_path.resolve().as_uri() + "?mode=ro", uri=True)) as before, closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)) as after:
            for table in ("forward_manifest", "forward_publications", "forward_outbox"):
                if before.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() != after.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall():
                    raise ValueError("history preservation failed: " + table)
        restored = self.load()
        if restored != self.decode(self.encode(changed)):
            raise ValueError("reconciled checkpoint roundtrip failed")
        report = dict(status="HISTORY_RECONCILED", generation_id=generation, observed_at=observed_at.isoformat(),
            effective_boundary=effective, inserted_bars=len(missing), affected_members=len(affected), feature_ready=ready,
            backup=record["backup"], backup_sha256=record["backup_sha256"],
            preserved_tables=["forward_manifest", "forward_publications", "forward_outbox"],
            existing_bars_unchanged=True, positions_unchanged=True, enrollment_unchanged=True, quarantine_unchanged=True,
            next_boundary_unchanged=True, unchanged_checkpoint_fields=sorted(set(state) - set(allowed)))
        (backup_path.parent / "reconciliation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        return restored, report

    def recover_corrections(self, batch, config, observed_at, expected_hash, *, clock):
        state = self.load()
        if state is None or correction_inventory_hash(state) != expected_hash:
            raise ValueError("reviewed correction inventory changed; audit before recovery")
        members, _ = alert_selection_cohort(state, next_boundary(observed_at))
        active = {member["security_id"] for member in members}
        recovered = active_correction_recoveries(state, next_boundary(observed_at), observed_at)
        affected = {pair["security_id"] for pair in state.get("corrections", []) if pair["security_id"] in active} - set(recovered)
        if not affected:
            return state, dict(status="CORRECTIONS_ALREADY_RECOVERED", inventory_sha256=expected_hash)
        record = correction_recovery_record(state, batch["bars"], config, observed_at)
        backup_path = self.path.parent / "correction-backups" / record["generation_id"] / "forward.sqlite"
        backup_path.parent.mkdir(parents=True, exist_ok=False)
        with closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)) as source:
            source.execute("PRAGMA query_only=ON")
            with closing(sqlite3.connect(backup_path)) as target:
                source.backup(target)
        changed = deepcopy(state)
        changed["pending_candidates"] = {key: value for key, value in changed.get("pending_candidates", {}).items() if value["security_id"] not in affected}
        changed["prepared_packets"] = [packet for packet in changed.get("prepared_packets", []) if packet["security_id"] not in affected]
        for security in affected:
            for interval in ("30m", "1h"):
                detector = changed.get("detectors", {}).get(security + "|" + interval)
                if detector is not None:
                    detector["states"] = {}
        activated = clock()
        if activated < observed_at:
            raise ValueError("correction activation cannot predate source capture")
        record.update(source_cutoff=observed_at.isoformat(), activated_at=activated.isoformat(),
            effective_boundary=next_boundary(activated).isoformat(), inventory_sha256=expected_hash,
            backup=str(backup_path.relative_to(self.path.parent)).replace("\\", "/"),
            backup_sha256=hashlib.sha256(backup_path.read_bytes()).hexdigest())
        record["generation_id"] = digest({key: value for key, value in record.items() if key != "generation_id"})
        changed.setdefault("correction_recovery_history", []).append(record)
        changed.setdefault("detector_recovery_boundaries", {}).update({security: record["effective_boundary"] for security in affected})
        if not affected <= set(active_correction_recoveries(changed, utc(record["effective_boundary"]), activated)):
            raise ValueError("not every reviewed member is prospectively recoverable")
        allowed = {"detectors", "pending_candidates", "prepared_packets", "detector_recovery_boundaries", "correction_recovery_history"}
        if {key: value for key, value in changed.items() if key not in allowed} != {key: value for key, value in state.items() if key not in allowed}:
            raise ValueError("correction recovery changed protected state")
        self.save(changed)
        with closing(sqlite3.connect(backup_path.resolve().as_uri() + "?mode=ro", uri=True)) as before, closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)) as after:
            for table in ("forward_manifest", "forward_publications", "forward_outbox"):
                if before.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() != after.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall():
                    raise ValueError("correction preservation failed: " + table)
        restored = self.load()
        if restored != self.decode(self.encode(changed)):
            raise ValueError("correction recovery checkpoint roundtrip failed")
        report = dict(status="CORRECTIONS_RECOVERED_PROSPECTIVELY", generation_id=record["generation_id"],
            activated_at=record["activated_at"], effective_boundary=record["effective_boundary"],
            reviewed_pairs=len(record["pairs"]), recovered_members=len(affected), inventory_sha256=expected_hash,
            backup=record["backup"], backup_sha256=record["backup_sha256"], original_bars_unchanged=True,
            original_corrections_unchanged=True, positions_unchanged=True, previous_publications_unchanged=True,
            outbox_unchanged=True, enrollment_and_quarantine_unchanged=True, next_boundary_unchanged=True)
        (backup_path.parent / "recovery.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        return restored, report


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
    prior_latest = {key: max(utc(bar["bar_end"]) for bar in rows.values()) for key, rows in inputs.items() if rows}
    inserted_history = {}
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
            if state.setdefault("identity_breaks", {}).get(bar["security_id"]) != "IDENTITY_CHANGED":
                state["identity_breaks"][bar["security_id"]] = "INPUT_CORRECTION_REVIEW"
            continue
        if not bootstrap and not old and bar["interval"] == "30m" and key in prior_latest and utc(bar["bar_end"]) <= prior_latest[key]:
            inserted_history.setdefault(bar["security_id"], []).append(bar["revision_id"])
        retained[bar["bar_start"]] = bar
    if inserted_history:
        effective = next_boundary(cutoff).isoformat()
        state.setdefault("detector_recovery_boundaries", {}).update({security: effective for security in inserted_history})
        state.setdefault("input_reconciliation_history", []).append(dict(version="stock_alert_late_history_v1",
            observed_at=cutoff.isoformat(), effective_boundary=effective,
            inserted_revision_ids={security: sorted(revisions) for security, revisions in sorted(inserted_history.items())}))
        state["pending_candidates"] = {key: value for key, value in state.get("pending_candidates", {}).items()
            if value["security_id"] not in inserted_history or value["interval"] == "1d"}
        state["prepared_packets"] = [packet for packet in state.get("prepared_packets", [])
            if packet["security_id"] not in inserted_history or packet["interval"] == "1d"]
    state["actions"] = batch["actions"]
    state["corrections"] = list({digest(item): item for item in state.get("corrections", []) + corrections}.values())
    recoveries = active_correction_recoveries(state, cutoff, cutoff)
    detector_inputs = corrected_detector_inputs(state, recoveries) if recoveries else inputs
    detector_blocks = {security: reason for security, reason in state.get("identity_breaks", {}).items() if security not in recoveries}
    grouped = {tuple(key.split("|", 1)): sorted(rows.values(), key=lambda bar: bar["bar_start"]) for key, rows in detector_inputs.items()}
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
        if key[0] not in detector_blocks}, state["members"], cutoff)
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
            saved = {} if security in inserted_history else detectors.get(key, {})
            initial = {(model, int(direction)): value for name, value in saved.get("states", {}).items() for model, direction in [name.split(":")]}
            after_ordinal = saved.get("ordinal")
            effective = state.get("detector_recovery_boundaries", {}).get(security)
            if effective:
                suppressed = frame.loc[frame.bar_end.lt(utc(effective)), "ordinal"]
                if not suppressed.empty:
                    after_ordinal = max(after_ordinal if after_ordinal is not None else -1, int(suppressed.max()))
            sink = {}
            observations = intraday_observations(frame, interval, contexts, config, initial_state=initial,
                after_ordinal=after_ordinal, state_sink=sink)
            if security in detector_blocks:
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
                if security in detector_blocks:
                    for packet in observations:
                        packet.update(ready=False, candidates=[])
                packets.extend(packet for packet in observations if packet["market_time"] > utc(state["enrolled_at"])
                               and packet["market_time"].isoformat() > state["daily_processed"].get(security, ""))
                state["daily_processed"][security] = latest
    if corrections:
        for packet in packets:
            packet["updates"].extend(item for item in corrections if item["security_id"] == packet["security_id"])
    state["last_source_read"] = cutoff.isoformat()
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
    recoveries = active_correction_recoveries(state, now, now)
    corrected = corrected_detector_inputs(state, recoveries) if recoveries else state.get("bars", {})
    for episode_id, position in state.get("positions", {}).items():
        if position["state"] in ("CLOSED", "NO_FILL"):
            continue
        security = position["candidate"]["security_id"]
        recovered_position = security in recoveries and position.get("correction_recovery_generation") == recoveries[security]["generation_id"]
        if security in state.get("identity_breaks", {}) and not recovered_position:
            position.update(state="UNRESOLVED", reason=state["identity_breaks"][security])
            continue
        bars = list((corrected if recovered_position else state["bars"]).get(f"{security}|30m", {}).values())
        state["positions"][episode_id] = mark_position(position, bars, state.get("actions", []), now, config)
    return state


def shadow_snapshot(state, publications, config, now):
    from research.stock_alerts import enrich_replay, replay_snapshot, session_dates
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
    if config.get("holding_policy") == "DAILY_SETUP_NEXT_SESSION_CONFIRMATION_V1":
        snapshot["source_label"] = "Daily-owned swing shadow / intraday-confirmed entries"
        snapshot["worker"]["pending_swing_setups"] = sum(not saved.get("candidate") for saved in state.get("swing_setup_book", {}).values())
    if state.get("correction_recovery_history"):
        recovery = state["correction_recovery_history"][-1]
        snapshot["worker"]["correction_recovery"] = dict(generation_id=recovery["generation_id"],
            effective_boundary=recovery["effective_boundary"], activated_at=recovery["activated_at"],
            reviewed_members=len({pair["security_id"] for pair in recovery["pairs"]}),
            active_members=len(active_correction_recoveries(state, now, now)))
    if state.get("alert_cohort_history"):
        members, cohort = alert_selection_cohort(state, utc(state.get("retry_boundary", state["next_boundary"])))
        snapshot["worker"].update(selection_members=len(members), alert_selection_cohort=cohort,
            alert_cohort_history=state["alert_cohort_history"])
        latest = state["alert_cohort_history"][-1]
        names = ", ".join(record["excluded_member"]["ticker"] for record in state["alert_cohort_history"])
        snapshot["warnings"].append(f"Alert-only identity quarantine: {names}; latest effective boundary {latest['effective_boundary']}. Original enrollment and positions retained.")
    if state.get("dispatch_policy") == READINESS_POLICY:
        boundary = utc(state.get("retry_boundary", state["next_boundary"]))
        snapshot.update(publication_mode="SOURCE_READINESS", next_publication_at=None,
            publication_window_start=(boundary + timedelta(minutes=config["provider_delay_minutes"])).isoformat(),
            publication_deadline=readiness_deadline(boundary).isoformat())
        snapshot["worker"]["dispatch_policy"] = READINESS_POLICY
    for record, publication in zip(snapshot["publications"], publications):
        if publication.get("correction_recovery"):
            record["correction_recovery"] = publication["correction_recovery"]
        if publication.get("alert_selection_cohort"):
            record["alert_selection_cohort"] = publication["alert_selection_cohort"]
        if publication.get("retry_of"):
            record.update(trigger_at=publication["market_time"], retry_of=publication["retry_of"])
            snapshot["warnings"].append("A source-ready retry preserves the earlier incomplete run")
    alert_members = {row["security_id"] for row in snapshot["alerts"]}
    if alert_members:
        bars = [bar for key, retained in state.get("bars", {}).items() if key.rsplit("|", 1)[0] in alert_members
                for bar in retained.values()]
        actions = [action for action in state.get("actions", []) if action["security_id"] in alert_members]
        snapshot = enrich_replay(snapshot, dict(bars=bars, actions=actions), config)
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