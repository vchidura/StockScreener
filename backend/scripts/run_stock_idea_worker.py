"""Run the actual-observation, multi-model forward SHADOW stock alert worker."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import sys
import sqlite3
import threading
import time

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from research.stock_idea_engine import digest
from research.stock_idea_forward import (ForwardPublicationLate, ForwardStore, READINESS_POLICY, advance_detectors, forward_config, forward_decision,
    alert_selection_cohort, next_boundary, packet_record, publish_view, readiness_deadline, restore_packet, runtime_sources, shadow_snapshot, update_positions, window_clock)
from research.stock_idea_replay import session_windows, utc

DEFAULT_ROOT = BACKEND / "backups/equity-shadow/stock-ideas-forward-v1"
QUALITY_ROOT = BACKEND / "backups/equity-shadow/stock-ideas-forward-v2"
FRAME_CACHE = {}


def validate_view_policy(path, policy):
    if path.is_file():
        saved = json.loads(path.read_text(encoding="utf-8"))
        if saved.get("source") != "SHADOW" or saved.get("source_id") != policy["policy_version"]:
            raise ValueError("view belongs to a different policy; choose a separate shadow view path")


def inspect_source_readiness(path):
    from equity.api import expected_materialized_market_time
    from equity.stock_idea_forward_source import forward_input_readiness
    now = datetime.now(timezone.utc)
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("PRAGMA query_only=ON")
        row = connection.execute("SELECT payload FROM forward_checkpoint WHERE singleton=1").fetchone()
        if not row:
            return dict(status="NOT_ENROLLED")
        state = ForwardStore.decode(row[0])
    pending = utc(state.get("retry_boundary", state["next_boundary"]))
    latest_source_boundary = expected_materialized_market_time(now, "30m")
    prospective, cohort = alert_selection_cohort(state, next_boundary(now))
    pending_members, pending_cohort = alert_selection_cohort(state, pending)
    result = dict(checked_at=now.isoformat(), enrolled_members=len(state["members"]), pending_boundary=pending.isoformat(),
        pending_deadline=readiness_deadline(pending).isoformat(), pending_members=len(pending_members),
        pending_cohort=pending_cohort, future_members=len(prospective), future_cohort=cohort,
        identity_breaks=state.get("identity_breaks", {}))
    result["pending_source_ready"] = forward_input_readiness(pending_members, pending, now,
        include_daily=pending == session_windows(str(pending.date()))[-1][1]) is not None
    result["prospective_source_probe"] = dict(boundary=latest_source_boundary.isoformat(),
        ready=forward_input_readiness(prospective, latest_source_boundary, now) is not None,
        scope="RETAINED_SOURCE_MEMBERSHIP_ONLY_NOT_FUTURE_PUBLICATION_GUARANTEE")
    return result


def readiness_cycle(store, state, config, view_path, *, clock, read_inputs, read_readiness=None):
    from equity.stock_idea_forward_source import forward_input_readiness
    read_readiness = read_readiness or forward_input_readiness
    now = clock()
    retry = state.get("retry_boundary")
    boundary = utc(retry or state["next_boundary"])
    deadline = readiness_deadline(boundary)
    include_daily = boundary == session_windows(str(boundary.date()))[-1][1]
    if now <= deadline and now - utc(state.get("last_readiness_check", "1900-01-01T00:00:00Z")) < timedelta(seconds=10):
        return state, dict(status="WAITING_FOR_SOURCE_PUBLICATION", boundary=boundary.isoformat(), deadline=deadline.isoformat())
    state["last_readiness_check"] = now.isoformat()
    selection_members, cohort = alert_selection_cohort(state, boundary)
    readiness = read_readiness(selection_members, boundary, now, include_daily=include_daily) if now <= deadline else None
    if not readiness and now <= deadline:
        return state, dict(status="WAITING_FOR_SOURCE_PUBLICATION", boundary=boundary.isoformat(), deadline=deadline.isoformat())
    if readiness:
        cutoff = clock()
        batch = read_inputs(state["members"], cutoff, include_daily=include_daily,
            after=min(utc(state["last_source_read"]), boundary))
        state, packets = advance_detectors(state, batch, config, cutoff, frame_cache=FRAME_CACHE)
        prepared = {f"{packet['security_id']}:{packet['interval']}:{packet['market_time']}": packet
            for packet in state.get("prepared_packets", [])}
        prepared.update({f"{packet['security_id']}:{packet['interval']}:{packet['market_time']}": packet_record(packet) for packet in packets})
        state["prepared_packets"] = list(prepared.values())
        state = update_positions(state, config, cutoff)
        if include_daily:
            state["last_daily_read"] = cutoff.isoformat()
        readiness["input_cutoff"] = cutoff.isoformat()
        if cohort:
            readiness["alert_cohort_generation"] = cohort["generation_id"]
        store.save(state)
    else:
        readiness = dict(input_cutoff=deadline.isoformat(), publications=[], reason="SOURCE_NOT_READY_BEFORE_DEADLINE")
    packets = [restore_packet(packet) for packet in state.get("prepared_packets", [])]
    before_publication = state
    state, publication, outbox = forward_decision(state, packets, boundary=boundary, actual_time=clock(),
        members=state["members"], policy_hash=store.policy_hash, config=config, readiness=readiness)
    def finish(current, record):
        if retry:
            record.update(window_key=boundary.isoformat() + ":source-ready-retry", retry_of=boundary.isoformat(), market_time=boundary.isoformat())
            current.pop("retry_boundary", None)
        else:
            current["next_boundary"] = next_boundary(boundary).isoformat()
        current["prepared_packets"] = [packet for packet in current.get("prepared_packets", []) if utc(packet["market_time"]) > boundary]
    finish(state, publication)
    try:
        store.save(state, publication, outbox, clock=clock)
    except ForwardPublicationLate:
        state, publication, outbox = forward_decision(before_publication, packets, boundary=boundary, actual_time=clock(),
            members=before_publication["members"], policy_hash=store.policy_hash, config=config, readiness=readiness)
        finish(state, publication)
        store.save(state, publication, outbox)
    publish_view(view_path, shadow_snapshot(state, store.publications(), config, clock()))
    return state, dict(status=publication["coverage"], boundary=boundary.isoformat(), retry_of=retry,
        published_at=publication["actual_publication_at"], candidates=len(publication["dispositions"]),
        selected=len(publication["selected"]), missing_members=len(publication["missing_members"]), dispatch_policy=READINESS_POLICY["version"],
        selection_members=len(selection_members), cohort_generation=cohort["generation_id"] if cohort else None)


def cycle(store, state, config, view_path, *, clock=lambda: datetime.now(timezone.utc), read_inputs=None):
    from equity.stock_idea_forward_source import read_forward_inputs
    read_inputs = read_inputs or read_forward_inputs
    now = clock()
    boundary = utc(state["next_boundary"])
    if state.get("dispatch_policy") == READINESS_POLICY and (state.get("retry_boundary") or now >= boundary + timedelta(minutes=config["provider_delay_minutes"])):
        return readiness_cycle(store, state, config, view_path, clock=clock, read_inputs=read_inputs)
    cutoff, last_dispatch = window_clock(boundary)
    if now < boundary + timedelta(minutes=config["provider_delay_minutes"]):
        active = any(position["state"] not in ("CLOSED", "NO_FILL") for position in state.get("positions", {}).values())
        last_daily_read = utc(state.get("last_daily_read", state["last_source_read"]))
        daily_due = now - last_daily_read >= timedelta(minutes=5)
        if daily_due or active and now - utc(state["last_source_read"]) >= timedelta(seconds=60):
            if (cutoff - now).total_seconds() <= max(30., state.get("preparation_seconds", 0.)) + 3.:
                return state, dict(status="WAITING_FOR_PUBLICATION", next_publication_at=cutoff.isoformat())
            batch = read_inputs(state["members"], now, include_daily=daily_due,
                after=min(utc(state["last_source_read"]), last_daily_read) if daily_due else utc(state["last_source_read"]))
            state, packets = advance_detectors(state, batch, config, now, frame_cache=FRAME_CACHE)
            if daily_due:
                state["last_daily_read"] = now.isoformat()
            state["prepared_packets"] = state.get("prepared_packets", []) + [packet_record(packet) for packet in packets]
            state = update_positions(state, config, now)
            store.save(state)
            publish_view(view_path, shadow_snapshot(state, store.publications(), config, now))
            return state, dict(status="CONTEXT_REFRESHED" if daily_due else "PAPER_MARKS_REFRESHED",
                positions=len(state.get("positions", {})), next_publication_at=cutoff.isoformat())
        return state, dict(status="WAITING_FOR_FINAL_BARS", next_publication_at=cutoff.isoformat())
    if now > last_dispatch:
        packets = [restore_packet(packet) for packet in state.pop("prepared_packets", [])]
        state, publication, outbox = forward_decision(state, packets, boundary=boundary, actual_time=now,
            members=state["members"], policy_hash=store.policy_hash, config=config)
        state["next_boundary"] = next_boundary(boundary).isoformat()
        store.save(state, publication, outbox)
        publish_view(view_path, shadow_snapshot(state, store.publications(), config, now))
        return state, dict(status="MISSED_PUBLICATION", boundary=boundary.isoformat(), selected=0)
    if now < cutoff:
        last_read = utc(state["last_source_read"])
        remaining = (cutoff - now).total_seconds()
        preparation_budget = max(10., state.get("preparation_seconds", 0.)) + 3.
        if (now - last_read).total_seconds() < 10 or remaining <= preparation_budget:
            return state, dict(status="WAITING_FOR_PUBLICATION", next_publication_at=cutoff.isoformat())
        started = time.monotonic()
        include_daily = boundary == session_windows(str(boundary.date()))[-1][1]
        batch = read_inputs(state["members"], now, include_daily=include_daily,
                            after=last_read)
        state, packets = advance_detectors(state, batch, config, now, frame_cache=FRAME_CACHE)
        if include_daily:
            state["last_daily_read"] = now.isoformat()
        prepared = {f"{packet['security_id']}:{packet['interval']}:{packet['market_time']}": packet
                    for packet in state.get("prepared_packets", [])}
        prepared.update({f"{packet['security_id']}:{packet['interval']}:{packet['market_time']}": packet_record(packet) for packet in packets})
        state["prepared_packets"] = list(prepared.values())
        state = update_positions(state, config, now)
        state["preparation_seconds"] = max(state.get("preparation_seconds", 0.), round(time.monotonic() - started, 3))
        store.save(state)
        return state, dict(status="INPUTS_PREPARED", boundary=boundary.isoformat(), packets=len(packets), prepare_seconds=state["preparation_seconds"])
    packets = [restore_packet(packet) for packet in state.pop("prepared_packets", [])]
    before_publication = state
    state, publication, outbox = forward_decision(state, packets, boundary=boundary, actual_time=now,
        members=state["members"], policy_hash=store.policy_hash, config=config)
    state["next_boundary"] = next_boundary(boundary).isoformat()
    try:
        store.save(state, publication, outbox, clock=clock)
    except ForwardPublicationLate:
        state, publication, outbox = forward_decision(before_publication, packets, boundary=boundary, actual_time=clock(),
            members=before_publication["members"], policy_hash=store.policy_hash, config=config)
        state["next_boundary"] = next_boundary(boundary).isoformat()
        store.save(state, publication, outbox)
    publish_view(view_path, shadow_snapshot(state, store.publications(), config, clock()))
    return state, dict(status=publication["coverage"], boundary=boundary.isoformat(),
        published_at=publication["actual_publication_at"], candidates=len(publication["dispositions"]),
        selected=len(publication["selected"]), missing_members=len(publication["missing_members"]))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--check-readiness", action="store_true", help="Inspect cohort and source readiness without checkpoint writes")
    parser.add_argument("--reconcile-history", action="store_true", help="Back up and reconcile missing retained 30m history for future detectors only")
    parser.add_argument("--recover-corrections", action="store_true", help="Activate reviewed canonical corrections for future detectors, preserving original bars and positions")
    parser.add_argument("--expected-correction-hash", help="Required reviewed inventory SHA256 for --recover-corrections")
    parser.add_argument("--quarantine-security-id", help="Explicitly quarantine an enrolled identity from future alert selection only")
    parser.add_argument("--quarantine-ticker", help="Exact enrolled ticker paired with the quarantined security ID")
    parser.add_argument("--enable-source-readiness", action="store_true")
    parser.add_argument("--retry-window", help="Append a current-time retry of a retained empty, incomplete window")
    parser.add_argument("--quality-version", type=int, choices=(1, 2), default=2)
    parser.add_argument("--state-dir", type=Path)
    args = parser.parse_args(argv)
    quarantine = bool(args.quarantine_security_id or args.quarantine_ticker)
    if args.recover_corrections != bool(args.expected_correction_hash):
        parser.error("Correction recovery requires its reviewed inventory hash and no standalone hash")
    if args.recover_corrections and (quarantine or args.reconcile_history or args.once or args.status or args.plan or args.check_readiness or args.retry_window or args.enable_source_readiness):
        parser.error("Correction recovery cannot be combined with other operation modes")
    if args.reconcile_history and (quarantine or args.once or args.status or args.plan or args.check_readiness or args.retry_window or args.enable_source_readiness):
        parser.error("History reconciliation cannot be combined with other operation modes")
    if quarantine and (not args.quarantine_security_id or not args.quarantine_ticker or args.once or args.status or args.plan or args.check_readiness or args.retry_window or args.enable_source_readiness):
        parser.error("Quarantine requires exact security ID and ticker, and cannot be combined with other operation modes")
    if args.check_readiness and (args.once or args.status or args.plan or args.retry_window or args.enable_source_readiness):
        parser.error("Readiness inspection cannot be combined with writer modes")
    args.state_dir = args.state_dir or (QUALITY_ROOT if args.quality_version == 2 else DEFAULT_ROOT)
    view_path = Path(os.getenv("STOCK_ALERT_SHADOW_VIEW") or args.state_dir / "alerts-view.json")
    if args.plan:
        print(json.dumps(dict(mode="PLAN_ONLY", state_dir=str(args.state_dir), view=str(view_path),
            policy=forward_config(quality_version=args.quality_version), brokerage_orders=False, replay=False), indent=2))
        return 0
    if args.status:
        if not view_path.is_file():
            print(json.dumps(dict(status="NOT_ENROLLED", path=str(view_path))))
        else:
            view = json.loads(view_path.read_text(encoding="utf-8"))
            print(json.dumps({key: view.get(key) for key in ("status", "source", "source_id", "as_of", "sessions", "worker", "next_publication_at")}, indent=2))
        return 0
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    view_path = Path(os.getenv("STOCK_ALERT_SHADOW_VIEW") or args.state_dir / "alerts-view.json")
    from database import get_db_connection
    from equity.stock_idea_forward_source import enrolled_members, read_forward_inputs
    if args.check_readiness:
        print(json.dumps(inspect_source_readiness(args.state_dir / "forward.sqlite"), indent=2), flush=True)
        return 0
    config = forward_config(quality_version=args.quality_version)
    validate_view_policy(view_path, config)
    store = ForwardStore(args.state_dir / "forward.sqlite", config)
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext('stock-idea-forward-shadow-worker'))")
            acquired = cursor.fetchone()[0]
            connection.commit()
            if not acquired:
                print(json.dumps(dict(status="ALREADY_RUNNING")))
                return 0
        try:
            state = store.load()
            if args.recover_corrections:
                if state is None:
                    raise ValueError("correction recovery requires an enrolled checkpoint")
                now = datetime.now(timezone.utc)
                members, _ = alert_selection_cohort(state, next_boundary(now))
                batch = read_forward_inputs(members, now, after=now)
                state, result = store.recover_corrections(batch, config, now, args.expected_correction_hash,
                    clock=lambda: datetime.now(timezone.utc))
                print(json.dumps(result, indent=2), flush=True)
                return 0
            if args.reconcile_history:
                if state is None:
                    raise ValueError("history reconciliation requires an enrolled checkpoint")
                now = datetime.now(timezone.utc)
                members, _ = alert_selection_cohort(state, next_boundary(now))
                batch = read_forward_inputs(members, now, after=now)
                state, result = store.reconcile_retained_history(batch, config, now, clock=lambda: datetime.now(timezone.utc))
                print(json.dumps(result, indent=2), flush=True)
                return 0
            if quarantine:
                state, result = store.activate_identity_quarantine(args.quarantine_security_id, args.quarantine_ticker, datetime.now(timezone.utc))
                publish_view(view_path, shadow_snapshot(state, store.publications(), config, datetime.now(timezone.utc)))
                print(json.dumps(result, indent=2), flush=True)
                return 0
            if args.retry_window:
                if state is None:
                    raise ValueError("retry requires an enrolled checkpoint")
                boundary = utc(args.retry_window)
                publications = store.publications()
                original = next((row for row in publications if row["window_key"] == boundary.isoformat()), None)
                if not original or original["selected"] or not original["missing_members"]:
                    raise ValueError("retry requires an existing empty run with incomplete inputs")
                if any(row.get("retry_of") == boundary.isoformat() for row in publications):
                    raise ValueError("this window already has a retained retry")
                if boundary >= utc(state["next_boundary"]):
                    raise ValueError("retry cannot replace the next scheduled window")
                state["retry_boundary"] = boundary.isoformat()
            if args.enable_source_readiness:
                if state is None:
                    raise ValueError("enable source readiness after initial enrollment")
                if state.get("dispatch_policy") != READINESS_POLICY:
                    state.setdefault("dispatch_policy_history", []).append(dict(policy=READINESS_POLICY,
                        activated_at=datetime.now(timezone.utc).isoformat(), reason="USER_APPROVED_SOURCE_READINESS_FIX"))
                state["dispatch_policy"] = READINESS_POLICY
            if args.retry_window and state.get("dispatch_policy") != READINESS_POLICY:
                raise ValueError("retry requires source-readiness dispatch")
            if state is not None and state.get("dispatch_policy") is not None and state["dispatch_policy"] != READINESS_POLICY:
                raise ValueError("unsupported retained dispatch policy; explicit policy activation is required")
            sources = runtime_sources()
            runtime_at = datetime.now(timezone.utc).isoformat()
            if state is not None:
                if state.get("runtime_sources") != sources:
                    state.setdefault("runtime_history", []).append(dict(started_at=runtime_at, sources=sources))
                state["runtime_sources"] = sources
            if state is None:
                now = datetime.now(timezone.utc)
                members, cohort = enrolled_members(now)
                state = dict(enrolled_at=now.isoformat(), members=members, cohort=cohort, next_boundary=next_boundary(now).isoformat(),
                    runtime_sources=sources, runtime_history=[dict(started_at=runtime_at, sources=sources)],
                    dispatch_policy=READINESS_POLICY,
                    dispatch_policy_history=[dict(policy=READINESS_POLICY, activated_at=now.isoformat(), reason="NEW_ENROLLMENT")])
                print(json.dumps(dict(status="WARMING_UP", members=len(members), enrolled_at=state["enrolled_at"])), flush=True)
                batch = read_forward_inputs(members, now, bootstrap=True)
                state, _ = advance_detectors(state, batch, config, now, bootstrap=True, frame_cache=FRAME_CACHE)
                state["last_daily_read"] = now.isoformat()
                store.save(state)
                publish_view(view_path, shadow_snapshot(state, [], config, datetime.now(timezone.utc)))
                print(json.dumps(dict(status="ENROLLED", members=len(members), next_boundary=state["next_boundary"])), flush=True)
            else:
                print(json.dumps(dict(status="RESTORING_FEATURE_CACHE", members=len(state["members"]))), flush=True)
                state, _ = advance_detectors(state, dict(bars=[], actions=state.get("actions", [])), config,
                    utc(state["last_source_read"]), frame_cache=FRAME_CACHE)
                store.save(state)
                publish_view(view_path, shadow_snapshot(state, store.publications(), config, datetime.now(timezone.utc)))
                print(json.dumps(dict(status="RESUMED", members=len(state["members"]), next_boundary=state["next_boundary"])), flush=True)
            while True:
                try:
                    state, result = cycle(store, state, config, view_path)
                except Exception:
                    logging.exception("Forward shadow cycle failed; committed publications are preserved")
                    if args.once:
                        raise
                    state = store.load()
                    threading.Event().wait(10)
                    continue
                result["checked_at"] = datetime.now(timezone.utc).isoformat()
                if result["status"] not in ("WAITING_FOR_FINAL_BARS", "WAITING_FOR_PUBLICATION", "WAITING_FOR_SOURCE_PUBLICATION") or args.once:
                    print(json.dumps(result), flush=True)
                if args.once:
                    return 0
                threading.Event().wait(1)
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(hashtext('stock-idea-forward-shadow-worker'))")
                connection.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())