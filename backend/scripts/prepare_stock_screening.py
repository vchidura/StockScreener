"""Prepare bounded retained daily screening facts; historical writes never promote current."""
import argparse
from collections import Counter
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
import time

from dotenv import load_dotenv
import exchange_calendars

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.screening_projection import build_latest, SNAPSHOT_TYPE
from equity.portal_snapshots import publish
from database import get_db_cursor
from research.screening import FIELDS, PATTERNS, FIELD_SET_VERSION, VERSION, Query, digest, query_generation


def preparation_sessions(session=None, start=None, end=None):
    if session is not None and (start is not None or end is not None):
        raise ValueError("Choose one session or a bounded session range")
    if (start is None) != (end is None):
        raise ValueError("Both start-session and end-session are required")
    if session is None and start is None:
        return [None]
    first, last = (session, session) if session else (start, end)
    calendar = exchange_calendars.get_calendar("XNYS")
    if first > last or not calendar.is_session(str(first)) or not calendar.is_session(str(last)):
        raise ValueError("Date bounds must be ordered exchange sessions")
    sessions = [stamp.date() for stamp in calendar.sessions_in_range(first, last)]
    if len(sessions) > 6:
        raise ValueError("Retained-data preparation is limited to six sessions per invocation")
    return sessions


def existing_generations(session):
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='5s'")
        cursor.execute("""SELECT snapshot_id,source_manifest FROM equity_portal_snapshots
            WHERE snapshot_type=%s AND source_manifest->>'session'=%s
              AND source_manifest->>'version'=%s AND source_manifest->>'field_set_version'=%s
              AND source_manifest->>'contract_hash'=%s ORDER BY generated_at DESC""",
            (SNAPSHOT_TYPE, str(session), VERSION, FIELD_SET_VERSION, digest(dict(fields=FIELDS, patterns=PATTERNS))))
        return [dict(snapshot_id=str(row["snapshot_id"]), generation=row["source_manifest"]["generation"]) for row in cursor.fetchall()]


def preservation_state():
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SELECT snapshot_id,payload_sha256,source_manifest_sha256,generated_at FROM equity_portal_snapshots")
        snapshots = {str(row["snapshot_id"]): (row["payload_sha256"], row["source_manifest_sha256"], row["generated_at"]) for row in cursor.fetchall()}
        cursor.execute("SELECT snapshot_type,snapshot_id,published_at FROM equity_portal_current_projections ORDER BY snapshot_type")
        pointers = [(row["snapshot_type"], str(row["snapshot_id"]), row["published_at"]) for row in cursor.fetchall()]
        cursor.execute("SELECT generation,changed_at FROM equity_portal_source_state WHERE singleton=TRUE")
        source = dict(cursor.fetchone())
    return snapshots, pointers, source


def approve_partial_source(payload, expected_unavailable, expected_selected, approved_at):
    unavailable = payload["source_unavailable_members"]
    if (payload["source_publication_status"] != "DEGRADED" or payload["source_selected_members"] != expected_selected
            or expected_selected <= 0 or expected_selected + len(unavailable) != payload["expected_members"]
            or sorted(member["ticker"] for member in unavailable) != sorted(expected_unavailable)
            or len(set(expected_unavailable)) != len(expected_unavailable) or not unavailable):
        raise ValueError("Partial publication differs from the explicitly approved coverage")
    missing_ids = {member["security_id"] for member in unavailable}
    retained = [row for row in payload["rows"] if row["security_id"] in missing_ids]
    if len(retained) != len(unavailable) or any(row["eligible"] or row["source_bar_id"] is not None or row["values"].get("price") is not None for row in retained):
        raise ValueError("Unavailable source members must remain unknown and unselected")
    approval = dict(version="screening_partial_source_approval_v1", approved_at=approved_at.isoformat(),
        session=payload["session"], source_publication_id=payload["source_publication_id"],
        selected_members=expected_selected, expected_members=payload["expected_members"],
        unavailable_members=unavailable, scope="EXACT_SESSION_AND_SOURCE_PUBLICATION",
        original_generation=payload["generation"])
    approval["approval_sha256"] = digest(approval)
    payload = dict(payload, partial_source_approval=approval)
    payload["generation"] = digest({key: value for key, value in payload.items() if key != "generation"})
    return payload


def prepare_one(session, *, publish_result=False, allow_degraded=False, approve_partial=False, expected_unavailable=(), expected_selected=None):
    started = time.perf_counter()
    payload = build_latest(session, allow_degraded=allow_degraded)
    if approve_partial:
        if session is None or not allow_degraded:
            raise ValueError("Partial current approval requires one explicit session and allow-degraded")
        payload = approve_partial_source(payload, expected_unavailable, expected_selected, datetime.now(timezone.utc))
    prepared_seconds = time.perf_counter() - started
    manifest = {name: value for name, value in payload.items() if name not in {"rows", "lineage", "hourly_lineage"}}
    manifest["source_generation"] = 0
    identifiers = publish({SNAPSHOT_TYPE: payload}, manifest, promote_current=session is None or approve_partial) if publish_result else []
    durations = []
    for _ in range(40):
        started = time.perf_counter()
        response = query_generation(payload, Query())
        durations.append((time.perf_counter() - started) * 1000)
    return dict(status="PUBLISHED" if publish_result else "MEASURED_NO_WRITES", snapshot_ids=identifiers,
        generation=payload["generation"], session=payload["session"], source_cutoff=payload["source_cutoff"],
        source_publication_id=payload["source_publication_id"], source_status=payload["source_publication_status"],
        source_unavailable_members=payload["source_unavailable_members"], promoted_current=publish_result and (session is None or approve_partial),
        partial_source_approval=payload.get("partial_source_approval"),
        expected_members=payload["expected_members"], eligible=sum(row["eligible"] for row in payload["rows"]),
        field_coverage=payload["field_coverage"], pattern_coverage=payload["pattern_coverage"],
        gap_coverage=payload.get("gap_coverage"),
        hourly_source=payload.get("hourly_source"), hourly_coverage=payload.get("hourly_coverage"),
        gap_unavailable_reasons=dict(Counter(row.get("gaps", {}).get("reason", "GAP_CONTEXT_UNAVAILABLE") for row in payload["rows"] if row.get("gaps", {}).get("status") != "READY")),
        missing_reasons={field: dict(Counter(row["missing"][field] for row in payload["rows"] if field in row["missing"])) for field in FIELDS if any(field in row["missing"] for row in payload["rows"])},
        unavailable_basic_rows=[dict(ticker=row["ticker"], quality=row["quality"]) for row in payload["rows"] if not row["eligible"]],
        preparation_seconds=round(prepared_seconds, 3), payload_bytes=len(json.dumps(payload).encode()),
        default_response_bytes=len(json.dumps(response).encode()), pure_query_p95_ms=round(sorted(durations)[37], 3))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--session", type=date.fromisoformat)
    parser.add_argument("--start-session", type=date.fromisoformat)
    parser.add_argument("--end-session", type=date.fromisoformat)
    parser.add_argument("--missing-only", action="store_true")
    parser.add_argument("--allow-degraded", action="store_true")
    parser.add_argument("--approve-partial-current", action="store_true")
    parser.add_argument("--expected-unavailable", nargs="+", default=[])
    parser.add_argument("--expected-selected", type=int)
    args = parser.parse_args(argv)
    sessions = preparation_sessions(args.session, args.start_session, args.end_session)
    if args.approve_partial_current and (args.session is None or not args.allow_degraded or not args.expected_unavailable or args.expected_selected is None or args.missing_only):
        parser.error("Partial approval requires --session, --allow-degraded, --expected-unavailable and --expected-selected; no missing-only")
    if not args.approve_partial_current and (args.expected_unavailable or args.expected_selected is not None):
        parser.error("Expected partial coverage requires --approve-partial-current")
    if sessions == [None] and (args.missing_only or args.allow_degraded):
        parser.error("missing-only and allow-degraded require explicit date bounds")
    before = preservation_state() if args.publish and sessions != [None] else None
    for session in sessions:
        existing = existing_generations(session) if args.missing_only else []
        if existing:
            print(json.dumps(dict(status="SKIPPED_EXISTING", session=str(session), snapshots=existing)), flush=True)
            continue
        print(json.dumps(prepare_one(session, publish_result=args.publish, allow_degraded=args.allow_degraded,
            approve_partial=args.approve_partial_current, expected_unavailable=args.expected_unavailable,
            expected_selected=args.expected_selected), indent=2), flush=True)
    if before is not None:
        after = preservation_state()
        if not all(after[0].get(key) == value for key, value in before[0].items()) or (not args.approve_partial_current and before[1:] != after[1:]):
            raise RuntimeError("Existing snapshot, pointer or source-generation preservation check failed")
        print(json.dumps(dict(status="PRESERVATION_PASS", prior_snapshots=len(before[0]), new_snapshots=len(after[0]) - len(before[0]),
            prior_snapshots_unchanged=True, current_pointers_unchanged=before[1] == after[1], source_generation_unchanged=before[2] == after[2],
            approved_current_publication=args.approve_partial_current)), flush=True)


if __name__ == "__main__":
    main()