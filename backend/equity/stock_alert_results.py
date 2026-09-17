"""Shared result persistence only; never updates a strategy's source ledger."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib

from psycopg2.extras import Json

from database import get_db_cursor
from research.stock_alert_annotations import binding, context_path, read_context, validate_context
from research.stock_alert_results import (VERSION, combine_snapshots, namespace_snapshot, plan_record,
    strategy_instance, validate_result_transition)
from research.stock_idea_engine import digest
from research.stock_idea_replay import utc


def capture_result_source(directory, stream, now):
    directory = Path(directory)
    view_path = directory / "alerts-view.json"
    if view_path.stat().st_size > 20_000_000:
        raise ValueError("source alert view exceeds read bound")
    raw = view_path.read_bytes()
    snapshot = json.loads(raw)
    if utc(snapshot["as_of"]) > now:
        raise ValueError("source snapshot is in the future")
    with closing(sqlite3.connect((directory / "forward.sqlite").resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        policy = json.loads(connection.execute("SELECT payload FROM forward_manifest WHERE singleton=1").fetchone()[0])
        state = json.loads(zlib.decompress(connection.execute("SELECT payload FROM forward_checkpoint WHERE singleton=1").fetchone()[0]))
        publications = [json.loads(zlib.decompress(row[0])) for row in connection.execute("SELECT payload FROM forward_publications ORDER BY window_key LIMIT 10001")]
    if len(publications) > 10000 or len(snapshot.get("alerts", [])) > 20000:
        raise ValueError("source results exceed declared count bound")
    expected_policy = "stock_ideas_forward_swing_v1" if stream == "swing" else "stock_ideas_forward_quality_v2"
    if policy["policy_version"] != expected_policy or snapshot["source_id"] != expected_policy:
        raise ValueError("configured stream policy mismatch")
    instance = strategy_instance(policy, state, stream)
    if snapshot.get("enrolled_at") != state["enrolled_at"]:
        raise ValueError("source view enrollment mismatch")
    by_run = {publication["window_key"]: publication for publication in publications}
    for row in snapshot["alerts"]:
        publication = by_run[row["run_id"]]
        position = state["positions"][row["alert_id"]]
        candidate = position["candidate"]
        if row["alert_id"] not in publication["selected"] or any(row[field] != candidate[field] for field in ("security_id", "ticker", "model", "interval", "direction", "stop", "target", "policy_version")):
            raise ValueError("view plan does not match retained ledger")
        if (row["trigger_price"] != candidate["price"] or utc(row["triggered_at"]) != utc(candidate["trigger_at"])
                or utc(row["published_at"]) != utc(publication["actual_publication_at"])):
            raise ValueError("view plan clocks or trigger price changed")
    bundles = {}
    for run in snapshot["publications"]:
        path = context_path(directory / "alert-context", snapshot["source_id"], run["run_id"])
        if path.exists():
            bundle = read_context(path)
            if (bundle["source_id"] != snapshot["source_id"] or bundle["run_id"] != run["run_id"]
                    or bundle["source_publication_sha256"] != digest(by_run[run["run_id"]])):
                raise ValueError("context evidence does not match source publication")
            bundles[run["run_id"]] = bundle
    return dict(instance=instance | dict(policy=policy, enrollment=state["members"]), snapshot=snapshot,
        publications={key: value for key, value in by_run.items() if key in {run["run_id"] for run in snapshot["publications"]}},
        contexts=bundles, source_sha256=hashlib.sha256(raw).hexdigest())


def retain_record(cursor, instance_id, kind, record_id, payload):
    checksum = digest(payload)
    cursor.execute("SELECT payload_sha256 FROM stock_alert_result_records WHERE instance_id=%s AND kind=%s AND record_id=%s",
        (instance_id, kind, record_id))
    previous = cursor.fetchone()
    if previous:
        if previous["payload_sha256"] != checksum:
            raise ValueError("immutable shared result differs from retained source")
        return
    cursor.execute("INSERT INTO stock_alert_result_records(instance_id,kind,record_id,payload_sha256,payload) VALUES(%s,%s,%s,%s,%s)",
        (instance_id, kind, record_id, checksum, Json(payload)))


def persist_capture(capture, now):
    instance, snapshot = capture["instance"], capture["snapshot"]
    identity = instance["instance_id"]
    with get_db_cursor() as cursor:
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (VERSION + ":" + instance["stream"],))
        cursor.execute("SELECT source_as_of FROM stock_alert_result_streams WHERE stream=%s", (instance["stream"],))
        previous_stream = cursor.fetchone()
        if previous_stream and previous_stream["source_as_of"] and previous_stream["source_as_of"] > utc(snapshot["as_of"]):
            raise ValueError("source view moved backwards")
        cursor.execute("INSERT INTO stock_alert_result_instances(instance_id,payload) VALUES(%s,%s) ON CONFLICT DO NOTHING", (identity, Json(instance)))
        cursor.execute("SELECT payload FROM stock_alert_result_instances WHERE instance_id=%s", (identity,))
        if cursor.fetchone()["payload"] != instance:
            raise ValueError("strategy instance identity changed")
        cursor.execute("SELECT record_id FROM stock_alert_result_records WHERE instance_id=%s AND kind='plan'", (identity,))
        if {row["record_id"] for row in cursor.fetchall()} - {row["alert_id"] for row in snapshot["alerts"]}:
            raise ValueError("source view lost retained alert plans")
        for row in snapshot["publications"]:
            retain_record(cursor, identity, "publication", row["run_id"], row)
        for key, publication in capture["publications"].items():
            retain_record(cursor, identity, "publication_evidence", key, publication)
        for row in snapshot["observations"]:
            retain_record(cursor, identity, "observation", digest(row), row)
        for key, bundle in capture["contexts"].items():
            retain_record(cursor, identity, "context", key, bundle)
        for row in snapshot["alerts"]:
            retain_record(cursor, identity, "plan", row["alert_id"], plan_record(row))
            cursor.execute("SELECT payload FROM stock_alert_result_revisions WHERE instance_id=%s AND alert_id=%s ORDER BY source_as_of DESC,imported_at DESC LIMIT 1", (identity, row["alert_id"]))
            previous = cursor.fetchone()
            if previous:
                validate_result_transition(previous["payload"], row)
            cursor.execute("INSERT INTO stock_alert_result_revisions(instance_id,alert_id,source_as_of,payload_sha256,payload) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (identity, row["alert_id"], snapshot["as_of"], digest(row), Json(row)))
        metadata = {key: value for key, value in snapshot.items() if key not in ("alerts", "publications", "observations")}
        cursor.execute("""INSERT INTO stock_alert_result_streams(stream,instance_id,source_as_of,source_sha256,metadata,checked_at,imported_at,error)
            VALUES(%s,%s,%s,%s,%s,%s,%s,NULL) ON CONFLICT(stream) DO UPDATE SET instance_id=excluded.instance_id,
            source_as_of=excluded.source_as_of,source_sha256=excluded.source_sha256,metadata=excluded.metadata,
            checked_at=excluded.checked_at,imported_at=excluded.imported_at,error=NULL""",
            (instance["stream"], identity, snapshot["as_of"], capture["source_sha256"], Json(metadata), now, now))
    return dict(stream=instance["stream"], instance_id=identity, alerts=len(snapshot["alerts"]), publications=len(snapshot["publications"]),
        contexts=len(capture["contexts"]), source_as_of=snapshot["as_of"], status="IMPORTED")


def record_stream_error(stream, now, error):
    with get_db_cursor() as cursor:
        cursor.execute("""INSERT INTO stock_alert_result_streams(stream,checked_at,error) VALUES(%s,%s,%s)
            ON CONFLICT(stream) DO UPDATE SET checked_at=excluded.checked_at,error=excluded.error""", (stream, now, error))


def load_shared_results(now=None):
    now = now or datetime.now(timezone.utc)
    snapshots, streams = [], []
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='10s'")
        cursor.execute("SELECT * FROM stock_alert_result_streams ORDER BY stream")
        stored = {row["stream"]: row for row in cursor.fetchall()}
        for stream in ("intraday", "swing"):
            record = stored.get(stream)
            if not record or not record["instance_id"]:
                streams.append(dict(stream=stream, label=stream.title(), error=(record or {}).get("error") or "NOT_IMPORTED", status="UNAVAILABLE"))
                continue
            cursor.execute("SELECT payload FROM stock_alert_result_instances WHERE instance_id=%s", (record["instance_id"],))
            instance = cursor.fetchone()["payload"]
            cursor.execute("SELECT kind,payload,payload_sha256 FROM stock_alert_result_records WHERE instance_id=%s AND kind IN ('publication','observation') LIMIT 100001", (record["instance_id"],))
            rows = cursor.fetchall()
            if len(rows) > 100000 or any(digest(row["payload"]) != row["payload_sha256"] for row in rows):
                raise ValueError("shared result read bound or checksum failed")
            cursor.execute("""SELECT DISTINCT ON(alert_id) payload,payload_sha256 FROM stock_alert_result_revisions
                WHERE instance_id=%s ORDER BY alert_id,source_as_of DESC,imported_at DESC LIMIT 20001""", (record["instance_id"],))
            alerts = cursor.fetchall()
            if len(alerts) > 20000 or any(digest(row["payload"]) != row["payload_sha256"] for row in alerts):
                raise ValueError("shared alert read bound or checksum failed")
            snapshot = dict(record["metadata"], publications=[row["payload"] for row in rows if row["kind"] == "publication"],
                observations=[row["payload"] for row in rows if row["kind"] == "observation"], alerts=[row["payload"] for row in alerts])
            snapshots.append(namespace_snapshot(instance, snapshot))
            age = (now - record["checked_at"]).total_seconds()
            streams.append(dict(stream=stream, label=instance["label"], instance_id=instance["instance_id"],
                source_id=instance["policy_version"], as_of=record["source_as_of"].isoformat(),
                source_age_seconds=max(0, (now - record["source_as_of"]).total_seconds()),
                checked_at=record["checked_at"].isoformat(), imported_at=record["imported_at"].isoformat(),
                status="UNAVAILABLE" if record["error"] else "STALE" if age > 180 else snapshot["status"],
                error=record["error"], projector_stale=age > 180, worker=snapshot.get("worker"),
                publication_window_start=snapshot.get("publication_window_start"), publication_deadline=snapshot.get("publication_deadline")))
    return combine_snapshots(snapshots, streams)


def attach_shared_context(page):
    rows = []
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='10s'")
        cache, budget = {}, 10_000_000
        for row in page["rows"]:
            key = (row["strategy_instance_id"], row["original_run_id"])
            if key not in cache:
                cursor.execute("SELECT payload FROM stock_alert_result_records WHERE instance_id=%s AND kind='context' AND record_id=%s", key)
                record = cursor.fetchone()
                bundle = record["payload"] if record else None
                if bundle:
                    budget -= len(json.dumps(bundle).encode())
                    try:
                        if budget < 0:
                            raise ValueError("context budget exceeded")
                        validate_context(bundle)
                    except (ValueError, KeyError, TypeError):
                        bundle = None
                cache[key] = bundle
            original = row | dict(alert_id=row["original_alert_id"], run_id=row["original_run_id"])
            context = dict(status="UNAVAILABLE", reason="NO_SAVED_PUBLICATION_CONTEXT", factors={})
            bundle = cache[key]
            if bundle:
                saved = bundle["rows"].get(row["original_alert_id"])
                if bundle["source_id"] == row["original_source_id"] and saved and saved["binding"] == binding(original):
                    context = dict(status="AVAILABLE", reason=None, factors=saved["factors"], **{field: bundle[field] for field in
                        ("input_cutoff", "publication_at", "assembled_at", "capture_mode", "bundle_sha256", "source_publication_sha256")})
                else:
                    context["reason"] = "CONTEXT_PLAN_BINDING_MISMATCH"
            rows.append(dict(row, context=context))
    return dict(page, rows=rows)