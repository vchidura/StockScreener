"""Provider-free, prospective annotation producer, separate from alert decisions."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from threading import Event
from zoneinfo import ZoneInfo

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from research.stock_alert_annotations import context_path, freeze_event_shadow, freeze_publication_context, read_context, read_event_shadow
from research.stock_alert_context import READINESS_VERSION, VERSION, observation, shadow_event_decision, utc
from research.stock_idea_engine import digest
from research.stock_idea_forward import publish_view
from scripts.prepare_stock_alert_context import build_manifest, capture, read_enrollment, verify_artifacts


WORKER_VERSION = "stock_alert_context_worker_v1"
CHECK_SECONDS = 60


def ledger_identity(state, policy):
    return dict(enrolled_at=state["enrolled_at"], universe_sha256=digest(state["members"]), policy_sha256=digest(policy))


def pending_publications(publications, activation, directory):
    pending = []
    for publication in publications:
        if not publication["selected"] or utc(publication["actual_publication_at"]) < utc(activation["activated_at"]):
            continue
        path = context_path(directory, publication["policy_version"], publication["window_key"])
        if path.exists():
            if read_context(path)["source_publication_sha256"] != digest(publication):
                raise ValueError("saved annotation publication changed")
        else:
            pending.append(publication)
    return sorted(pending, key=lambda row: (utc(row["actual_publication_at"]), row["window_key"]))


def build_publication_evidence(state, policy, publication, now, *, include_candidates=False):
    from database import get_db_cursor
    from options.config import load_option_runtime_configuration
    configuration = load_option_runtime_configuration()
    plan_ids = list(publication["candidates"]) if include_candidates else publication["selected"]
    if len(plan_ids) > 1000:
        raise ValueError("retained candidate context exceeds declared bound")
    selected = {publication["candidates"][key]["security_id"] for key in plan_ids}
    members = [row for row in state["members"] if row["security_id"] in selected]
    if len(members) != len(selected):
        raise ValueError("selected context securities outside original enrollment")
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='20s'")
        facts = capture(cursor, members, [publication], now, configuration, company_only=True)
    result = build_manifest(state, policy, [publication], facts, now, configuration, include_current=False, include_candidates=include_candidates)
    from research.stock_rotation import archived_rotation_context, shared_alert_context
    for bundle in result["contexts"].values():
        archived = archived_rotation_context(BACKEND / "backups/stock-rotation", bundle["cutoff"], bundle["prior_session"], digest(state["members"]))
        for member in bundle["members"]:
            member["factors"].update(shared_alert_context(archived, member, bundle["cutoff"], publication.get("market_time", publication["window_key"])))
            if include_candidates:
                verdict = observation("DIAGNOSTIC")
                verdict.update(value=shadow_event_decision(member["factors"]), source_revision_ids=sorted({revision
                    for name in ("earnings", "fomc") for revision in member["factors"][name]["source_revision_ids"]}))
                member["factors"]["event_shadow"] = verdict
    manifest = dict(schema_version=READINESS_VERSION, generated_at=datetime.now(timezone.utc).isoformat(),
        database_snapshot=facts["transaction"], session=publication["session"], original_publications_unchanged=True,
        universe_sha256=digest(state["members"]), baseline_policy_sha256=digest(policy), company_context_only=True,
        publication_hashes={publication["window_key"]: digest(publication)}, observations=result["contexts"],
        selected_securities=members, financial_inventory=facts.get("financial_inventory"),
        scope="ALL_RETAINED_CANDIDATE_CONTEXT" if include_candidates else "PROSPECTIVE_SELECTED_ALERT_ANNOTATION_ONLY", provider_requests=False,
        code_hashes={name: hashlib.sha256((BACKEND / name).read_bytes()).hexdigest() for name in
            ("scripts/run_stock_alert_context_worker.py", "scripts/prepare_stock_alert_context.py", "research/stock_alert_context.py", "research/stock_alert_annotations.py")})
    annotations = dict(schema_version=VERSION, generated_at=manifest["generated_at"], readiness_manifest_sha256=digest(manifest),
        annotations_only=True, publication_contexts={key: {field: bundle[field] for field in ("market", "spy", "qqq", "cutoff", "prior_session")}
            for key, bundle in result["contexts"].items()}, rows=result["annotations"])
    return manifest, annotations


def pending_event_publications(publications, activation, directory):
    pending = []
    for publication in publications:
        if (not publication.get("candidates") or publication.get("coverage", "PUBLISHED") != "PUBLISHED"
                or utc(publication["actual_publication_at"]) < utc(activation["activated_at"])):
            continue
        path = context_path(directory, publication["policy_version"], publication["window_key"])
        if path.exists():
            if read_event_shadow(path)["source_publication_sha256"] != digest(publication):
                raise ValueError("shadow source publication changed")
        else:
            pending.append(publication)
    return pending


def produce_cycle(state_dir, activation, now, *, builder=build_publication_evidence, shadow_activation=None):
    import exchange_calendars
    calendar = exchange_calendars.get_calendar("XNYS")
    current = calendar.date_to_session(now.astimezone(ZoneInfo("America/New_York")).date(), direction="previous")
    sessions = [str(current.date()), str(calendar.previous_session(current).date())]
    state, policy, publications, _ = read_enrollment(state_dir / "forward.sqlite", sessions)
    if ledger_identity(state, policy) != activation["ledger_identity"]:
        raise ValueError("alert ledger enrollment or policy changed; explicit reactivation required")
    directory = state_dir / "alert-context"
    pending = pending_publications(publications, activation, directory)
    if shadow_activation:
        if shadow_activation["ledger_identity"] != activation["ledger_identity"]:
            raise ValueError("event shadow activation ledger mismatch")
        shadow_pending = pending_event_publications(publications, shadow_activation, state_dir / "event-shadow")
        pending = sorted({row["window_key"]: row for row in pending + shadow_pending}.values(), key=lambda row: utc(row["actual_publication_at"]))
    status = dict(version=WORKER_VERSION, checked_at=now.isoformat(), activated_at=activation["activated_at"],
        status="WAITING_FOR_SELECTED_PUBLICATION", pending_publications=len(pending),
        observed_publications=len(publications), provider_requests=False, ledger_writes=False,
        shadow_events_enabled=bool(shadow_activation), live_gate_enabled=False,
        shadow_activated_at=shadow_activation["activated_at"] if shadow_activation else None)
    if not pending:
        return status
    publication = pending[0]
    include_candidates = bool(shadow_activation and utc(publication["actual_publication_at"]) >= utc(shadow_activation["activated_at"]))
    manifest, annotations = (builder(state, policy, publication, now, include_candidates=True) if include_candidates
        else builder(state, policy, publication, now))
    _, _, after, _ = read_enrollment(state_dir / "forward.sqlite", sessions)
    original = next((row for row in after if row["window_key"] == publication["window_key"]), None)
    if original is None or digest(original) != digest(publication):
        raise ValueError("original publication changed during annotation capture")
    evidence_dir = state_dir / "context-evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    path = evidence_dir / (now.strftime("%Y%m%dT%H%M%S%f") + ".json")
    for output, value in ((path, manifest), (path.with_suffix(".annotations.json"), annotations)):
        with output.open("x", encoding="utf-8") as handle:
            json.dump(value, handle, default=str, allow_nan=False)
    _, saved = verify_artifacts(path)
    selected_rows = [row for row in saved["rows"] if row["alert_id"] in publication["selected"]]
    result = freeze_publication_context(directory, publication, selected_rows,
        saved["publication_contexts"][publication["window_key"]], assembled_at=datetime.now(timezone.utc).isoformat(),
        manifest_sha256=saved["readiness_manifest_sha256"])
    if include_candidates:
        status["event_shadow_result"] = freeze_event_shadow(state_dir / "event-shadow", publication, saved["rows"],
            assembled_at=datetime.now(timezone.utc).isoformat(), manifest_sha256=saved["readiness_manifest_sha256"])
    status.update(status="CONTEXT_" + result, run_id=publication["window_key"], alerts=len(selected_rows), candidates=len(saved["rows"]),
        evidence_file=path.name, completed_at=datetime.now(timezone.utc).isoformat())
    return status


def review_session(state_dir, session, *, builder=build_publication_evidence):
    now = datetime.now(timezone.utc)
    state, policy, publications, _ = read_enrollment(state_dir / "forward.sqlite", session)
    baseline_hash = digest(publications)
    rows = []
    for publication in publications:
        if not publication.get("candidates") or publication.get("coverage", "PUBLISHED") != "PUBLISHED":
            continue
        manifest, annotations = builder(state, policy, publication, now, include_candidates=True)
        dispositions = {row["episode_id"]: row for row in publication.get("dispositions", [])}
        for annotation in annotations["rows"]:
            original = dispositions.get(annotation["alert_id"], {})
            factors = annotation["factors"]
            rows.append(dict(alert_id=annotation["alert_id"], run_id=publication["window_key"], ticker=annotation["ticker"],
                input_cutoff=publication["input_deadline"], publication_at=publication["actual_publication_at"],
                baseline_selected=annotation["alert_id"] in publication["selected"], baseline_reason=original.get("reason"),
                shadow=shadow_event_decision(factors), factors=factors,
                original_publication_sha256=digest(publication), manifest_sha256=digest(manifest)))
    _, _, after, _ = read_enrollment(state_dir / "forward.sqlite", session)
    if digest(after) != baseline_hash:
        raise ValueError("publication set changed during bounded review")
    return dict(schema="stock_alert_context_progression_review_v1", session=session, captured_at=now.isoformat(),
        original_publications_sha256=baseline_hash, original_publications_unchanged=True, live_gate_enabled=False,
        mode="RETROSPECTIVE_ENGINEERING_CHECK_NOT_HELD_OUT_EVALUATION", rows=rows,
        summary=dict(candidate_occurrences=len(rows), unique_candidates=len({row["alert_id"] for row in rows}),
            baseline_selected=sum(row["baseline_selected"] for row in rows),
            shadow_decisions=dict(Counter(row["shadow"]["disposition"] for row in rows)),
            archived_context_present=sum(bool(row["factors"]["spy"].get("source_snapshot_sha256")) for row in rows)),
        provider_requests=False, ledger_writes=False, old_annotations_rewritten=False,
        limitations=["Repeated/expired candidates remain separately identified", "No quota refill or return improvement simulation",
            "Post-publication reconstruction does not validate preselection latency", "Source gaps are not neutral or favorable"])


def main():
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--shadow-events", action="store_true", help="Prospectively record all-candidate event diagnostics without enabling any live gate")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--review-session", type=date.fromisoformat, help="Read-only retrospective candidate diagnostic; never writes live context or shadow records")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    reader = Path(os.getenv("STOCK_ALERT_SHADOW_VIEW") or BACKEND / "backups/equity-shadow/stock-ideas-forward-v2/alerts-view.json")
    state_dir = (args.state_dir or reader.parent).resolve()
    if state_dir != reader.parent.resolve():
        parser.error("Annotation state directory must match configured shadow reader")
    if args.review_session:
        if args.shadow_events or not args.output or args.output.exists():
            parser.error("Review needs a new output path and cannot activate event shadows")
        report = review_session(state_dir, args.review_session.isoformat())
        with args.output.open("x", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, default=str, allow_nan=False)
        print(json.dumps({key: value for key, value in report.items() if key != "rows"}, indent=2), flush=True)
        return
    if args.output:
        parser.error("Output is only valid for a retrospective review")
    status_path = state_dir / "alert-context/worker-status.json"
    if args.plan:
        print(json.dumps(dict(version=WORKER_VERSION, state_dir=str(state_dir), check_seconds=CHECK_SECONDS,
            maximum_publications_per_cycle=1, provider_requests=False, ledger_writes=False,
            shadow_events=args.shadow_events, live_gate_enabled=False), indent=2), flush=True)
        return
    if args.status:
        print(status_path.read_text(encoding="utf-8") if status_path.exists() else '{"status":"NOT_STARTED"}', flush=True)
        return
    from database import get_db_connection
    lock_name = "stock-alert-context:" + digest(str(state_dir))[:24]
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock_name,))
            locked = cursor.fetchone()[0]
        connection.commit()
        if not locked:
            print('{"status":"ALREADY_RUNNING"}', flush=True)
            return
        try:
            now = datetime.now(timezone.utc)
            state, policy, _, _ = read_enrollment(state_dir / "forward.sqlite", now.astimezone(ZoneInfo("America/New_York")).date().isoformat())
            activation_path = state_dir / "alert-context/worker-activation.json"
            activation_path.parent.mkdir(parents=True, exist_ok=True)
            if activation_path.exists():
                activation = json.loads(activation_path.read_text(encoding="utf-8"))
            else:
                activation = dict(version=WORKER_VERSION, activated_at=now.isoformat(), ledger_identity=ledger_identity(state, policy))
                with activation_path.open("x", encoding="utf-8") as handle:
                    json.dump(activation, handle, allow_nan=False)
            if activation.get("version") != WORKER_VERSION or activation["ledger_identity"] != ledger_identity(state, policy):
                raise ValueError("annotation activation does not match ledger")
            shadow_activation = None
            if args.shadow_events:
                shadow_path = state_dir / "event-shadow/activation.json"
                if shadow_path.exists():
                    shadow_activation = json.loads(shadow_path.read_text(encoding="utf-8"))
                else:
                    shadow_activation = dict(version="shadow_holding_event_v1", activated_at=now.isoformat(),
                        ledger_identity=ledger_identity(state, policy), live_gate_enabled=False)
                    shadow_path.parent.mkdir(parents=True, exist_ok=True)
                    with shadow_path.open("x", encoding="utf-8") as handle:
                        json.dump(shadow_activation, handle, allow_nan=False)
                if (shadow_activation.get("version") != "shadow_holding_event_v1" or shadow_activation.get("live_gate_enabled") is not False
                        or shadow_activation["ledger_identity"] != activation["ledger_identity"]):
                    raise ValueError("shadow activation does not match ledger")
            print(json.dumps(dict(status="ANNOTATOR_RESUMED", activated_at=activation["activated_at"], state_dir=str(state_dir))), flush=True)
            while True:
                try:
                    status = produce_cycle(state_dir, activation, datetime.now(timezone.utc), shadow_activation=shadow_activation)
                except Exception as error:
                    status = dict(status="ANNOTATION_FAILED", checked_at=datetime.now(timezone.utc).isoformat(),
                        error_type=type(error).__name__, provider_requests=False, ledger_writes=False)
                publish_view(status_path, status)
                print(json.dumps(status), flush=True)
                if args.once:
                    return
                Event().wait(CHECK_SECONDS)
        finally:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_name,))
            connection.commit()


if __name__ == "__main__":
    main()