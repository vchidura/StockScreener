"""Pure, clock-injected stock idea decisions; no feed or database dependencies."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import hashlib
import json
import math
import sqlite3
import zlib


VERSION = "stock_ideas_v1"
MODELS = ("resumption", "acceptance", "failure", "discovery")
ARMS = ("ALL", "PRIORITY", "MOMENTUM", "RANDOM")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     default=str, allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class Candidate:
    security_id: str
    ticker: str
    model: str
    interval: str
    direction: int
    horizon: str
    anchor: str
    trigger_at: datetime
    expires_at: datetime
    available_at: datetime
    reference: float
    stop: float
    target: float
    activation_atr: float
    price: float
    rs_rank: float
    extension: float
    room_risk: float
    liquidity: float
    momentum: float
    revision_ids: tuple[str, ...] = ()
    policy_version: str = VERSION
    generation: int = 0
    health: str = "READY"
    selection_block: str | None = None

    @property
    def episode_id(self):
        return digest([self.policy_version, self.security_id, self.model, self.interval,
                       self.direction, self.horizon, self.anchor, self.generation])

    def __post_init__(self):
        if self.model not in MODELS or self.direction not in (-1, 1):
            raise ValueError("invalid model or direction")
        if self.interval not in ("30m", "1h", "1d"):
            raise ValueError("weekly/monthly observations are context, not candidates")
        for value in (self.trigger_at, self.expires_at, self.available_at):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("candidate clocks must be timezone aware")


def entry_gate(candidate, price):
    if not all(math.isfinite(value) for value in (price, candidate.stop, candidate.target,
                                                  candidate.activation_atr)) or price <= 0:
        return "INVALID_ENTRY"
    if candidate.model == "discovery":
        return None
    risk = candidate.direction * (price - candidate.stop)
    room = candidate.direction * (candidate.target - price)
    if min(risk, room) <= 0:
        return "ENTRY_OUTSIDE_BRACKET"
    if room < risk:
        return "INSUFFICIENT_TARGET_ROOM"
    if candidate.model == "acceptance":
        chase = candidate.direction * (price - candidate.reference)
        if not 0 <= chase <= candidate.activation_atr:
            return "ENTRY_CHASE_OR_BOUNDARY_FAILED"
    return None


def priority(candidate, arm, seed, window_key):
    tie = (candidate.security_id, candidate.episode_id)
    if arm == "RANDOM":
        return (digest([seed, window_key, candidate.episode_id]),) + tie
    if arm == "MOMENTUM" or candidate.model == "discovery":
        return (-candidate.direction * candidate.momentum, -candidate.liquidity) + tie
    if candidate.model == "resumption":
        strength = candidate.rs_rank if candidate.direction == 1 else 1 - candidate.rs_rank
        return (-strength, candidate.extension, -candidate.liquidity) + tie
    if candidate.model == "acceptance":
        return (candidate.extension, -candidate.room_risk, -candidate.liquidity) + tie
    return (-candidate.room_risk, candidate.extension, -candidate.liquidity) + tie


def select_candidates(candidates, *, deadline, expected_members, window_key,
                      already_selected=(), arm="PRIORITY", seed=1729, cap=3,
                      active_positions=0, max_active_positions=1000, trade_sizes=None):
    if arm not in ARMS or cap != 3:
        raise ValueError("v1 requires a declared arm and cap three")
    unique = {}
    for candidate in candidates:
        previous = unique.setdefault(candidate.episode_id, candidate)
        if previous != candidate:
            raise ValueError("conflicting revisions for one candidate must be resolved before selection")
    ordered = sorted(unique.values(), key=lambda candidate: candidate.episode_id)
    dispositions, eligible = {}, []
    for candidate in ordered:
        reason = None
        if candidate.security_id not in expected_members:
            reason = "OUTSIDE_FROZEN_UNIVERSE"
        elif candidate.health != "READY":
            reason = candidate.health
        elif candidate.selection_block:
            reason = candidate.selection_block
        elif candidate.available_at > deadline or candidate.trigger_at > deadline:
            reason = "NOT_AVAILABLE"
        elif deadline > candidate.expires_at:
            reason = "EXPIRED"
        elif candidate.episode_id in already_selected:
            reason = "CONTINUING_EPISODE"
        elif not all(math.isfinite(value) for value in (candidate.rs_rank, candidate.extension,
                     candidate.room_risk, candidate.liquidity, candidate.momentum)):
            reason = "UNAVAILABLE_RANK_INPUT"
        else:
            reason = entry_gate(candidate, candidate.price)
        dispositions[candidate.episode_id] = dict(episode_id=candidate.episode_id,
            security_id=candidate.security_id, model=candidate.model, interval=candidate.interval,
            direction=candidate.direction, selection="SUPPRESSED" if reason else "ELIGIBLE",
            reason=reason, conflicts=[], context=[])
        if reason is None:
            eligible.append(candidate)
    for candidate in eligible:
        if candidate.model == "discovery":
            continue
        opposed = [other for other in eligible if other.security_id == candidate.security_id
                   and other.direction != candidate.direction and other.model != "discovery"
                   and other.model != candidate.model]
        conflicts = sorted(other.episode_id for other in opposed if other.horizon == candidate.horizon)
        countertrend = sorted(other.episode_id for other in opposed if other.horizon != candidate.horizon)
        dispositions[candidate.episode_id].update(conflicts=conflicts, context=countertrend)
        if conflicts:
            dispositions[candidate.episode_id].update(selection="SUPPRESSED", reason="OPPOSING_FRESH_MODEL")
    selected = []
    slots = max(0, max_active_positions - active_positions)
    for model in MODELS:
        pool = sorted((candidate for candidate in eligible if candidate.model == model
                       and dispositions[candidate.episode_id]["selection"] == "ELIGIBLE"),
                      key=lambda candidate: priority(candidate, arm, seed, window_key))
        limit = len(pool) if arm == "ALL" else cap
        if trade_sizes is not None:
            limit = min(limit, trade_sizes.get(model, 0))
        stocks, count = set(), 0
        for candidate in pool:
            reason = None
            if arm != "ALL" and candidate.security_id in stocks:
                reason = "MODEL_STOCK_DUPLICATE"
            elif count >= limit:
                reason = "MODEL_QUOTA"
            elif model != "discovery" and slots == 0:
                reason = "ACTIVE_POSITION_CAP"
            if reason:
                dispositions[candidate.episode_id].update(selection="SUPPRESSED", reason=reason)
                continue
            dispositions[candidate.episode_id]["selection"] = "SELECTED"
            selected.append(candidate)
            stocks.add(candidate.security_id)
            count += 1
            if model != "discovery":
                slots -= 1
    display = {}
    for candidate in selected:
        lane = "WATCH" if candidate.model == "discovery" else "TRADE"
        key = (lane, candidate.security_id, candidate.direction)
        item = display.setdefault(key, dict(lane=lane, security_id=candidate.security_id,
            direction=candidate.direction, models=[], episode_ids=[]))
        item["models"].append(candidate.model)
        item["episode_ids"].append(candidate.episode_id)
    return dict(selected=[candidate.episode_id for candidate in selected],
                dispositions=list(dispositions.values()),
                display=[display[key] for key in sorted(display)])


def candidate_record(candidate):
    record = dict(vars(candidate))
    if any(isinstance(value, float) and not math.isfinite(value) for value in record.values()):
        raise ValueError("candidate records cannot contain nonfinite numbers")
    for name in ("trigger_at", "expires_at", "available_at"):
        record[name] = str(record[name])
    return record


def read_candidate(record):
    values = dict(record)
    for name in ("trigger_at", "expires_at", "available_at"):
        values[name] = datetime.fromisoformat(values[name])
    values["revision_ids"] = tuple(values["revision_ids"])
    return Candidate(**values)


def reset_condition(model, direction, close, reference, activation_atr, range_low, range_high):
    if model == "resumption":
        return direction * (reference - close) < .50 * activation_atr
    return range_low <= close <= range_high


def advance_episode(previous, candidate, *, ordinal, close, range_low, range_high,
                    triggered=False, invalidated=False, reset_valid=True,
                    anchor_transition=False, fresh_retracement=False):
    state = dict(previous, candidate=dict(previous["candidate"])) if previous else None
    updates = []
    observation_time = candidate.trigger_at.isoformat()
    if state is not None and ordinal <= state["last_ordinal"]:
        return state, updates
    if state is not None:
        contiguous = ordinal == state["last_ordinal"] + 1
        state["last_ordinal"] = ordinal
        if not reset_valid or not contiguous or candidate.health != "READY":
            changed = state["setup"] != "INVALIDATED" or state["health"] == "READY"
            state.update(setup="INVALIDATED", health=candidate.health if candidate.health != "READY" else "UNAVAILABLE",
                         reset_count=0, reset_ordinal=None)
            if changed:
                updates.append(dict(kind="DATA_RISK", episode_id=state["candidate"]["episode_id"], at=observation_time))
            return state, updates
        state.update(last_seen=observation_time, observations=state["observations"] + 1, health="READY")
        original = read_candidate({key: value for key, value in state["candidate"].items() if key != "episode_id"})
        if invalidated or (state["setup"] == "TRIGGERED" and candidate.trigger_at > original.expires_at):
            if state["setup"] not in ("INVALIDATED", "EXPIRED"):
                state.update(setup="INVALIDATED" if invalidated else "EXPIRED", reset_count=0, reset_ordinal=None)
                updates.append(dict(kind=state["setup"], episode_id=original.episode_id, at=observation_time))
                return state, updates
        if state["setup"] in ("INVALIDATED", "EXPIRED"):
            reset = reset_condition(original.model, original.direction, close, original.reference,
                                    original.activation_atr, state["range_low"], state["range_high"])
            if state["reset_count"] < 2:
                state["reset_count"] = state["reset_count"] + 1 if reset else 0
                if state["reset_count"] == 2:
                    state["reset_ordinal"] = ordinal
                return state, updates
            if not triggered or ordinal <= state["reset_ordinal"]:
                return state, updates
            if candidate.anchor != original.anchor and not anchor_transition:
                return state, updates
            if original.model == "resumption" and not fresh_retracement:
                return state, updates
            candidate = replace(candidate, generation=original.generation + 1)
            updates.append(dict(kind="REARM", episode_id=original.episode_id, at=observation_time,
                                anchor_transition=anchor_transition, next_anchor=candidate.anchor))
            state = None
        elif state["setup"] == "TRIGGERED":
            refreshed = replace(candidate, anchor=original.anchor,
                generation=original.generation, trigger_at=original.trigger_at, expires_at=original.expires_at,
                reference=original.reference, stop=original.stop, target=original.target,
                activation_atr=original.activation_atr)
            state["candidate"] = candidate_record(refreshed) | {"episode_id": original.episode_id}
            return state, updates
        elif state["setup"] == "WATCH":
            if candidate.anchor != original.anchor and not anchor_transition:
                return state, updates
            if triggered:
                state["setup"] = "TRIGGERED"
                state["candidate"] = candidate_record(candidate) | {"episode_id": candidate.episode_id}
            return state, updates
    if state is None and reset_valid and candidate.health == "READY":
        state = dict(setup="TRIGGERED" if triggered else "WATCH", health="READY",
                     candidate=candidate_record(candidate) | {"episode_id": candidate.episode_id},
                     range_low=range_low, range_high=range_high, reset_count=0, reset_ordinal=None,
                     last_ordinal=ordinal, last_seen=observation_time, observations=1)
    return state, updates


def decide_publication(state, candidates, *, deadline, now, expected_members, available_members,
                       window_key, policy_hash, revision_ids=(), updates=(), arm="PRIORITY",
                       seed=1729, max_active_positions=1000, trade_sizes=None):
    if now < deadline:
        raise ValueError("publication cannot commit before its decision deadline")
    state = deepcopy(state)
    selected_before = set(state.get("selected", []))
    missed_before = set(state.get("missed", []))
    positions = state.setdefault("positions", {})
    active = sum(position["state"] in ("PENDING", "OPEN", "UNRESOLVED") for position in positions.values())
    risk_updates = list(updates)
    opposed_ids = set()
    for candidate in candidates:
        for episode_id, position in positions.items():
            owner = position["candidate"]
            if (position["state"] in ("PENDING", "OPEN", "UNRESOLVED") and candidate.model != "discovery"
                    and owner["security_id"] == candidate.security_id and owner["horizon"] == candidate.horizon
                    and owner["direction"] != candidate.direction):
                opposed_ids.add(candidate.episode_id)
                risk_updates.append(dict(kind="OPEN_POSITION_OPPOSITION", episode_id=episode_id,
                                         opposing_episode_id=candidate.episode_id))
    decision_candidates = [replace(candidate, selection_block="OPEN_POSITION_OPPOSITION")
                           if candidate.episode_id in opposed_ids else candidate for candidate in candidates]
    result = select_candidates(decision_candidates, deadline=deadline, expected_members=set(expected_members),
        window_key=window_key, already_selected=selected_before | missed_before, arm=arm, seed=seed,
        active_positions=active, max_active_positions=max_active_positions, trade_sizes=trade_sizes)
    for disposition in result["dispositions"]:
        if disposition["episode_id"] in missed_before:
            disposition.update(selection="SUPPRESSED", reason="MISSED_PUBLICATION")
    missed = now > deadline
    if missed:
        state["missed"] = sorted(missed_before | {row["episode_id"] for row in result["dispositions"]
                                                 if row["reason"] not in ("NOT_AVAILABLE", "EXPIRED", "CONTINUING_EPISODE")})
        for disposition in result["dispositions"]:
            if disposition["selection"] == "SELECTED":
                disposition.update(selection="SUPPRESSED", reason="MISSED_PUBLICATION")
        result.update(selected=[], display=[])
    candidate_by_id = {candidate.episode_id: candidate for candidate in candidates}
    for episode_id in result["selected"]:
        candidate = candidate_by_id[episode_id]
        if candidate.model != "discovery":
            positions.setdefault(episode_id, dict(state="PENDING", candidate=candidate_record(candidate),
                                                  publication_at=deadline.isoformat(), marks=[]))
    state["selected"] = sorted(selected_before | set(result["selected"]))
    publication = result | dict(window_key=window_key, policy_hash=policy_hash, deadline=deadline.isoformat(),
        decision_at=now.isoformat(), expected_members=sorted(set(expected_members)),
        missing_members=sorted(set(expected_members) - set(available_members)),
        revision_ids=sorted(set(revision_ids)), coverage="MISSED_PUBLICATION" if missed else "PUBLISHED",
        updates=sorted({digest(update): update for update in risk_updates}.values(), key=digest))
    outbox = [dict(kind="NEW_IDEA", payload=item) for item in result["display"]]
    prior_updates = set(state.get("risk_updates", []))
    outbox.extend(dict(kind="RISK_UPDATE", payload=item) for item in publication["updates"] if digest(item) not in prior_updates)
    state["risk_updates"] = sorted(prior_updates | {digest(item) for item in publication["updates"]})
    return state, publication, outbox


class PublicationLedger:
    """Local replay/shadow store. A caller must explicitly supply its own path."""

    def __init__(self, path):
        self.path = str(path)
        with sqlite3.connect(self.path) as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS publications (
                    publication_key TEXT PRIMARY KEY, input_hash TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS engine_state (
                    state_key TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS notification_outbox (
                    notification_id TEXT PRIMARY KEY, publication_key TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS audit_updates (
                    audit_id TEXT PRIMARY KEY, publication_key TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS candidate_catalog (
                    candidate_key TEXT PRIMARY KEY, payload BLOB NOT NULL);
            """)

    @staticmethod
    def catalog_state(connection, state):
        def catalog(record):
            if "catalog_ref" in record:
                return record
            payload = json.dumps(record, sort_keys=True, allow_nan=False, default=str).encode()
            key = hashlib.sha256(payload).hexdigest()
            if not connection.execute("SELECT 1 FROM candidate_catalog WHERE candidate_key=?", (key,)).fetchone():
                connection.execute("INSERT INTO candidate_catalog VALUES (?,?)", (key, zlib.compress(payload)))
            return dict(catalog_ref=key)

        result = dict(state)
        result["positions"] = {episode_id: dict(position, candidate=catalog(position["candidate"]))
                               for episode_id, position in state.get("positions", {}).items()}
        result["lifecycles"] = {key: {model: dict(life, candidate=catalog(life["candidate"])) if life else None
                                      for model, life in group.items()} for key, group in state.get("lifecycles", {}).items()}
        return result

    @staticmethod
    def restore_state(connection, state, *, include_lifecycles=False):
        def restore(record):
            if "catalog_ref" not in record:
                return record
            row = connection.execute("SELECT payload FROM candidate_catalog WHERE candidate_key=?", (record["catalog_ref"],)).fetchone()
            if row is None:
                raise ValueError("missing candidate catalog reference")
            return json.loads(zlib.decompress(row[0]))

        result = dict(state)
        result["positions"] = {episode_id: dict(position, candidate=restore(position["candidate"]))
                               if position["state"] not in ("CLOSED", "NO_FILL") else position
                               for episode_id, position in state.get("positions", {}).items()}
        if include_lifecycles:
            result["lifecycles"] = {key: {model: dict(life, candidate=restore(life["candidate"])) if life else None
                                          for model, life in group.items()} for key, group in state.get("lifecycles", {}).items()}
        return result

    def commit(self, *, state_key, window_key, policy_hash, inputs, decide):
        publication_key = digest([state_key, window_key, policy_hash])
        input_hash = digest(inputs)
        with sqlite3.connect(self.path, timeout=30) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute("SELECT input_hash,payload FROM publications WHERE publication_key=?",
                                          (publication_key,)).fetchone()
            if existing:
                if existing[0] != input_hash:
                    update = dict(kind="LATE_INPUT_CORRECTION", original_input_hash=existing[0], input_hash=input_hash)
                    connection.execute("INSERT OR IGNORE INTO audit_updates VALUES (?,?,?)",
                                       (digest([publication_key, update]), publication_key, json.dumps(update)))
                return json.loads(existing[1])
            saved = connection.execute("SELECT payload FROM engine_state WHERE state_key=?", (state_key,)).fetchone()
            state, publication, outbox = decide(self.restore_state(connection, json.loads(saved[0])) if saved else {})
            encode = lambda value: json.dumps(value, sort_keys=True, allow_nan=False, default=str)
            connection.execute("INSERT INTO publications VALUES (?,?,?)",
                               (publication_key, input_hash, encode(publication)))
            connection.execute("INSERT INTO engine_state VALUES (?,?) ON CONFLICT(state_key) DO UPDATE SET payload=excluded.payload",
                               (state_key, encode(self.catalog_state(connection, state))))
            for notification in outbox:
                connection.execute("INSERT INTO notification_outbox VALUES (?,?,?)",
                                   (digest([publication_key, notification]), publication_key, encode(notification)))
            return publication