"""Capture source originals and bounded read-only equity simplification evidence."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from itertools import combinations
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import zlib


ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"


def fingerprint(path):
    content = path.read_bytes()
    return dict(bytes=len(content), sha256=hashlib.sha256(content).hexdigest())


def capture_sources(destination):
    destination.mkdir(parents=True, exist_ok=False)
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "backend", "frontend", "README.md", "docker-compose.yml"],
        cwd=ROOT,
    ).decode("utf-8").split("\0")
    records = []
    for name in names:
        if not name or not (ROOT / name).is_file():
            continue
        if Path(name).name == ".env" or name.startswith("backend/backups/"):
            raise ValueError("source archive cannot include runtime secrets or data")
        source, target = ROOT / name, destination / "files" / name
        expected = fingerprint(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        if fingerprint(target) != expected or fingerprint(source) != expected:
            raise ValueError("source changed during archive capture")
        records.append(dict(original=name, archived="files/" + name, **expected))
    manifest = dict(captured_at=datetime.now(timezone.utc).isoformat(), files=records)
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return dict(path=str(destination.relative_to(ROOT)), files=len(records),
                bytes=sum(record["bytes"] for record in records), manifest=fingerprint(destination / "manifest.json"))


def retained_alerts():
    reports = {}
    for name in ("stock-ideas-forward-v1", "stock-ideas-forward-v2", "stock-ideas-swing-v1"):
        directory = BACKEND / "backups/equity-shadow" / name
        path = directory / "forward.sqlite"
        if not path.exists():
            reports[name] = dict(status="ABSENT")
            continue
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            rows = connection.execute("SELECT window_key,payload FROM forward_publications ORDER BY window_key LIMIT 10001").fetchall()
            if len(rows) > 10000:
                raise ValueError("publication baseline exceeds declared bound")
            checkpoint = connection.execute("SELECT payload FROM forward_checkpoint WHERE singleton=1").fetchone()
            state = json.loads(zlib.decompress(checkpoint[0])) if checkpoint else {}
            outbox = connection.execute("SELECT notification_id FROM forward_outbox ORDER BY notification_id").fetchall()
            reports[name] = dict(status="CAPTURED", publications={key: hashlib.sha256(payload).hexdigest() for key, payload in rows},
                active_positions={key: value for key, value in state.get("positions", {}).items()
                                  if value.get("state") in ("PENDING", "OPEN", "UNRESOLVED")},
                notification_ids=[row[0] for row in outbox], enrolled_at=state.get("enrolled_at"),
                database_bytes=path.stat().st_size)
        view = directory / "alerts-view.json"
        if view.exists():
            reports[name]["view_at_capture"] = fingerprint(view)
    return reports


def database_inventory():
    sys.path.insert(0, str(BACKEND))
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    from database import get_db_cursor
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='15s'")
        cursor.execute("SET LOCAL lock_timeout='2s'")
        cursor.execute("SELECT current_setting('transaction_read_only') AS read_only, transaction_timestamp() AS captured_at")
        snapshot = dict(cursor.fetchone())
        cursor.execute("""SELECT relation.relname AS relation, relation.reltuples::bigint AS estimated_rows,
            pg_total_relation_size(relation.oid) AS total_bytes, pg_indexes_size(relation.oid) AS index_bytes
            FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            WHERE namespace.nspname='public' AND relation.relkind IN ('r','m')
              AND (relation.relname LIKE 'equity_%' OR relation.relname LIKE 'stock_alert_%'
                   OR relation.relname IN ('cross_sectional_signals','market_discovery_states'))
            ORDER BY total_bytes DESC LIMIT 100""")
        snapshot["relations"] = [dict(row) for row in cursor.fetchall()]
        cursor.execute("""SELECT snapshot.snapshot_id::text,snapshot.snapshot_type,snapshot.payload_sha256,
            snapshot.generated_at FROM equity_portal_current_projections current
            JOIN equity_portal_snapshots snapshot USING(snapshot_id) ORDER BY snapshot.snapshot_type""")
        snapshot["current_projections"] = [dict(row) for row in cursor.fetchall()]
        cursor.execute("""SELECT snapshot_id::text,payload_sha256,source_manifest,generated_at
            FROM equity_portal_snapshots WHERE snapshot_type='SCREENING_DAILY_V1'
            ORDER BY generated_at DESC LIMIT 3""")
        snapshot["screening_generations"] = [dict(row) for row in cursor.fetchall()]
    return snapshot


def verify_baseline(path, current_alerts):
    baseline = json.loads(path.read_text(encoding="utf-8"))
    archive = ROOT / baseline["source_archive"]["path"]
    manifest_path = archive / "manifest.json"
    if fingerprint(manifest_path) != baseline["source_archive"]["manifest"]:
        raise ValueError("source archive manifest changed")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    protected = 0
    for record in manifest["files"]:
        expected = {name: record[name] for name in ("bytes", "sha256")}
        if fingerprint(archive / record["archived"]) != expected:
            raise ValueError("archived source changed: " + record["original"])
        if record["original"].startswith("backend/research/inputs/"):
            if fingerprint(ROOT / record["original"]) != expected:
                raise ValueError("frozen input changed: " + record["original"])
            protected += 1
    old = ast.parse((archive / "files/backend/equity/stock_discovery.py").read_text(encoding="utf-8"))
    new = ast.parse((BACKEND / "equity/daily_state.py").read_text(encoding="utf-8"))
    original_function = next(node for node in old.body if isinstance(node, ast.FunctionDef) and node.name == "stock_features")
    extracted_function = next(node for node in new.body if isinstance(node, ast.FunctionDef) and node.name == "stock_features")
    if ast.dump(original_function) != ast.dump(extracted_function):
        raise ValueError("daily-state function differs from preserved original")
    publications = 0
    changed_positions = {}
    for name, original in baseline["alerts"].items():
        current = current_alerts[name]
        for key, checksum in original.get("publications", {}).items():
            if current.get("publications", {}).get(key) != checksum:
                raise ValueError("retained alert publication changed")
            publications += 1
        if not set(original.get("notification_ids", [])) <= set(current.get("notification_ids", [])):
            raise ValueError("retained notification identity lost")
        changed_positions[name] = [key for key, value in original.get("active_positions", {}).items()
                                   if current.get("active_positions", {}).get(key) != value]
    from database import get_db_cursor
    from equity.polygon import sha256_json
    snapshots = {row["snapshot_id"]: row["payload_sha256"] for key in ("current_projections", "screening_generations")
                 for row in baseline.get("database", {}).get(key, [])}
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='15s'")
        cursor.execute("SELECT snapshot_id::text,snapshot_type,generated_at,payload,payload_sha256 FROM equity_portal_snapshots WHERE snapshot_id=ANY(%s::uuid[])", (list(snapshots),))
        records = [dict(row) for row in cursor.fetchall()]
        retained = {row["snapshot_id"]: sha256_json(row["payload"]) for row in records}
    if set(retained) != set(snapshots) or any(row["payload_sha256"] != snapshots[row["snapshot_id"]] for row in records):
        raise ValueError("retained snapshot identity or stored checksum changed")
    discrepancies = []
    signed_zero_matches = []
    signed_zero_details = []
    for row in records:
        if retained[row["snapshot_id"]] == snapshots[row["snapshot_id"]]:
            continue
        recovery = signed_zero_roundtrip_report(row["payload"], row["payload_sha256"])
        if recovery["matched"]:
            signed_zero_matches.append(row["snapshot_id"])
            signed_zero_details.append(dict(snapshot_id=row["snapshot_id"], snapshot_type=row["snapshot_type"], **recovery))
        else:
            discrepancies.append(dict(snapshot_id=row["snapshot_id"], snapshot_type=row["snapshot_type"],
                generated_at=row["generated_at"], stored_matches_baseline=True,
                stored=row["payload_sha256"], recomputed=retained[row["snapshot_id"]],
                numeric_diagnostics=numeric_diagnostics(row["payload"]), signed_zero_search=recovery))
    return dict(status="PRESERVATION_VERIFIED" if not discrepancies else "PAYLOAD_VERIFICATION_INCOMPLETE",
                archived_sources=len(manifest["files"]), frozen_inputs=protected,
                daily_state_ast_identical=True, alert_publications=publications, immutable_snapshots=len(snapshots),
                changed_active_position_ids=changed_positions, signed_zero_roundtrip_matches=signed_zero_matches,
                signed_zero_details=signed_zero_details,
                payload_checksum_discrepancies=discrepancies)


def signed_zero_roundtrip_matches(payload, expected):
    return signed_zero_roundtrip_report(payload, expected)["matched"]


def signed_zero_roundtrip_report(payload, expected, *, maximum_attempts=4096):
    from equity.polygon import sha256_json
    candidate = deepcopy(payload)
    groups, copies = {}, []

    def visit(value, path=(), ticker=None, session=None):
        if isinstance(value, dict):
            ticker, session = value.get("ticker", ticker), value.get("date", session)
            details = value.get("daily_details")
            copied_keys = set()
            if isinstance(details, dict) and details:
                for array_name, field in (("price_changes", "price_change_since_cross_pct"),
                                          ("spreads", "ma_spread_pct"), ("weekly_spreads", "weekly_spread_pct")):
                    source_paths = [(*path, "daily_details", day, field) for day in sorted(details)
                                    if details[day].get(field) is not None]
                    if isinstance(value.get(array_name), list) and len(value[array_name]) == len(source_paths):
                        copies.extend(((*path, array_name, index), source) for index, source in enumerate(source_paths))
                        copied_keys.add(array_name)
                if "weekly_spread_pct" in value:
                    copies.append(((*path, "weekly_spread_pct"), (*path, "daily_details", max(details), "weekly_spread_pct")))
                    copied_keys.add("weekly_spread_pct")
            for key, child in value.items():
                if key in copied_keys:
                    continue
                if key == "daily_details" and isinstance(child, dict):
                    for day, detail in child.items():
                        visit(detail, (*path, key, day), ticker, day)
                elif isinstance(child, float) and child == 0.:
                    if key == "price_change_since_cross_pct" and value.get("days_since_cross") == 0:
                        continue
                    identity = (ticker, session, key, sha256_json(value)) if ticker and session else (*path, key)
                    groups.setdefault(identity, []).append((*path, key))
                else:
                    visit(child, (*path, key), ticker, session)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                if isinstance(child, float) and child == 0.:
                    groups[(*path, index)] = [(*path, index)]
                else:
                    visit(child, (*path, index), ticker, session)

    def get(path):
        value = candidate
        for key in path:
            value = value[key]
        return value

    def assign(path, value):
        parent = candidate
        for key in path[:-1]:
            parent = parent[key]
        parent[path[-1]] = value

    visit(candidate)
    choices = list(groups.values())
    report = dict(matched=False, independent_zero_groups=len(choices), attempts=0,
                  maximum_attempts=maximum_attempts, reason="NO_EXACT_MATCH")
    if not choices or len(choices) > 256:
        return report | dict(reason="NO_CANDIDATES" if not choices else "CANDIDATE_BOUND")
    for size in range(1, len(choices) + 1):
        for selected in combinations(range(len(choices)), size):
            if report["attempts"] >= maximum_attempts:
                return report | dict(reason="SEARCH_BOUND")
            for index in selected:
                for path in choices[index]:
                    assign(path, -0.0)
            changed_copies = []
            for target, source in copies:
                target_value, source_value = get(target), get(source)
                if isinstance(target_value, float) and target_value == 0. and isinstance(source_value, float) and source_value == 0.:
                    changed_copies.append((target, target_value))
                    assign(target, source_value)
            report["attempts"] += 1
            if sha256_json(candidate) == expected:
                return report | dict(matched=True, reason="EXACT_ORIGINAL_SHA256",
                    negative_zero_paths=[list(path) for index in selected for path in choices[index]],
                    copied_zero_paths=[list(path) for path, _ in changed_copies
                                       if str(get(path)) == "-0.0"])
            for index in selected:
                for path in choices[index]:
                    assign(path, 0.0)
            for target, original in changed_copies:
                assign(target, original)
    return report


def numeric_diagnostics(payload):
    counts, zero_fields, samples = Counter(), Counter(), []
    def visit(value, path=()):
        if isinstance(value, dict):
            for key, child in value.items():
                visit(child, (*path, key))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, (*path, index))
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            counts[type(value).__name__] += 1
            if isinstance(value, float) and value == 0.:
                counts["float_zero"] += 1
                zero_fields[str(path[-1]) if path else "root"] += 1
                if len(samples) < 20:
                    samples.append(list(path))
    visit(payload)
    return dict(counts=dict(counts), zero_fields=dict(zero_fields), zero_paths_sample=samples,
                original_search_skipped=counts["float_zero"] > 10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-sources", type=Path)
    parser.add_argument("--database", action="store_true")
    parser.add_argument("--context", action="store_true")
    parser.add_argument("--verify-baseline", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("audit output already exists; do not overwrite a baseline")
    report = dict(captured_at=datetime.now(timezone.utc).isoformat(), database_mutated=False)
    if args.capture_sources:
        report["source_archive"] = capture_sources(args.capture_sources.resolve())
    report["alerts"] = retained_alerts()
    if args.database:
        report["database"] = database_inventory()
    if args.context:
        sys.path.insert(0, str(BACKEND))
        from dotenv import load_dotenv
        load_dotenv(BACKEND / ".env")
        from equity.stock_context import load_stock_context
        from time import perf_counter
        started = perf_counter()
        context = load_stock_context()
        report["context"] = dict(read_seconds=round(perf_counter() - started, 3),
            status=context["status"], cohort=context["cohort"], rows=len(context["rows"]),
            ready={interval: sum(row["frames"][interval]["status"] == "READY" for row in context["rows"])
                   for interval in ("1d", "1h", "30m")},
            with_daily_rank=sum(row["daily"]["percentile"] is not None for row in context["rows"]),
            sample=context["rows"][:3])
    if args.verify_baseline:
        sys.path.insert(0, str(BACKEND))
        from dotenv import load_dotenv
        load_dotenv(BACKEND / ".env")
        report["preservation"] = verify_baseline(args.verify_baseline, report["alerts"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(report, output, indent=2, default=str, allow_nan=False)
        output.write("\n")
    print(json.dumps(dict(output=str(args.output), source_archive=report.get("source_archive"),
        database_read_only=report.get("database", {}).get("read_only"),
        context=report.get("context"),
        preservation=report.get("preservation"),
        largest_relations=report.get("database", {}).get("relations", [])[:8],
        alerts={name: dict(status=record["status"], publications=len(record.get("publications", {})),
                          active_positions=len(record.get("active_positions", {}))) for name, record in report["alerts"].items()}), indent=2, default=str))


if __name__ == "__main__":
    main()