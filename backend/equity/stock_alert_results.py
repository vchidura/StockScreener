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


def capture_result_source(directory, stream, now=None):
    directory = Path(directory)
    view_path = directory / "alerts-view.json"
    if view_path.stat().st_size > 20_000_000:
        raise ValueError("source alert view exceeds read bound")
    raw = view_path.read_bytes()
    now = now or datetime.now(timezone.utc)
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
        if row["alert_id"] not in publication["selected"]:
            raise ValueError("view plan does not match retained ledger")
        if row["lane"] == "WATCH":
            candidate = publication.get("candidates", {}).get(row["alert_id"])
            dispositions = [item for item in publication["dispositions"] if item["episode_id"] == row["alert_id"]]
            if (not candidate or len(dispositions) != 1 or dispositions[0]["selection"] != "SELECTED"
                    or row["model"] != "discovery" or row["interval"] != "1d" or row["status"] != "WATCH"
                    or row["hold"] != "Watch only" or row["policy_version"] != policy["policy_version"]
                    or any(row[field] != candidate[field] for field in ("security_id", "ticker", "model", "interval", "direction"))
                    or any(row[field] != dispositions[0][field] for field in ("security_id", "model", "interval", "direction"))
                    or any(row.get(field) is not None for field in ("trigger_price", "stop", "target", "risk_pct", "reward_risk",
                        "entry_price", "entry_at", "entry_risk", "exit_due_at", "exit_price", "exit_at", "paper_return", "mark_price", "mark_at"))):
                raise ValueError("view watch does not match retained ledger")
            if (utc(row["triggered_at"]) != utc(publication.get("market_time", publication["window_key"]))
                    or utc(row["published_at"]) != utc(publication["actual_publication_at"])):
                raise ValueError("view watch clocks changed")
            continue
        position = state.get("positions", {}).get(row["alert_id"])
        if row["lane"] != "TRADE" or row["model"] == "discovery" or position is None:
            raise ValueError("view trade has no retained position")
        candidate = position["candidate"]
        if any(row[field] != candidate[field] for field in ("security_id", "ticker", "model", "interval", "direction", "stop", "target", "policy_version")):
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
        contexts=bundles, source_sha256=hashlib.sha256(raw).hexdigest(), captured_at=now.isoformat())


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


def load_stock_eod_review(*, as_of=None, session_date=None, search="", model=None,
                          selection_status=None, direction=None, trade_type=None,
                          offset=0, limit=100, cursor_factory=get_db_cursor):
    from research.stock_alert_review import build_stock_eod_review

    as_of = as_of or datetime.now(timezone.utc)
    if as_of.utcoffset() is None:
        raise ValueError("stock EOD review requires an aware cutoff")
    with cursor_factory() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='10s'")
        cursor.execute("""SELECT to_regclass('public.stock_alert_result_records') IS NOT NULL
            AND to_regclass('public.stock_alert_result_instances') IS NOT NULL
            AND to_regclass('public.stock_alert_result_streams') IS NOT NULL
            AND to_regclass('public.stock_alert_result_revisions') IS NOT NULL AS ready""")
        if not cursor.fetchone()["ready"]:
            return build_stock_eod_review([], {}, as_of=as_of, session_date=None, sessions=[], storage_ready=False,
                search=search, model=model, selection_status=selection_status, direction=direction,
                trade_type=trade_type, offset=offset, limit=limit)
        cursor.execute("""SELECT DISTINCT record.payload->>'session' AS session
            FROM stock_alert_result_streams AS stream
            JOIN stock_alert_result_records AS record ON record.instance_id=stream.instance_id
            WHERE record.kind='publication_evidence' AND record.imported_at<=%s
              AND record.payload ? 'session'
            ORDER BY session DESC LIMIT 21""", (as_of,))
        sessions = sorted(row["session"] for row in cursor.fetchall())
        selected = str(session_date) if session_date else (sessions[-1] if sessions else None)
        if selected and selected not in sessions:
            raise ValueError("session outside the retained stock evaluation window")
        publication_rows = []
        if selected:
            cursor.execute("""SELECT stream.stream,stream.instance_id,instance.payload AS instance,
                    record.record_id,record.payload_sha256,record.payload
                FROM stock_alert_result_streams AS stream
                JOIN stock_alert_result_instances AS instance USING(instance_id)
                JOIN stock_alert_result_records AS record ON record.instance_id=stream.instance_id
                WHERE record.kind='publication_evidence' AND record.imported_at<=%s
                  AND record.payload->>'session'=%s
                  AND octet_length(record.payload::text)<=4194304
                  AND octet_length(instance.payload::text)<=4194304
                ORDER BY stream.stream,record.record_id LIMIT 1001""", (as_of, selected))
            publication_rows = [dict(row) for row in cursor.fetchall()]
        if len(publication_rows) > 1000:
            raise ValueError("stock EOD review exceeds publication bound")
        publications = []
        for row in publication_rows:
            if (not isinstance(row["payload_sha256"], str) or len(row["payload_sha256"]) != 64
                    or any(character not in "0123456789abcdef" for character in row["payload_sha256"])):
                raise ValueError("stock EOD publication checksum is invalid")
            publications.append(dict(instance_id=row["instance_id"], stream=row["stream"],
                label=row["instance"].get("label", row["stream"].title()), record_id=row["record_id"], payload=row["payload"]))
        candidate_ids = sorted({episode_id for record in publications for episode_id in record["payload"].get("candidates", {})})
        outcomes = {}
        if candidate_ids:
            instance_ids = sorted({record["instance_id"] for record in publications})
            cursor.execute("""SELECT DISTINCT ON(instance_id,alert_id) instance_id,alert_id,payload,payload_sha256
                FROM stock_alert_result_revisions
                WHERE instance_id=ANY(%s) AND alert_id=ANY(%s) AND imported_at<=%s
                  AND octet_length(payload::text)<=4194304
                ORDER BY instance_id,alert_id,source_as_of DESC,imported_at DESC LIMIT 10001""",
                (instance_ids, candidate_ids, as_of))
            outcome_rows = [dict(row) for row in cursor.fetchall()]
            if len(outcome_rows) > 10000 or any(digest(row["payload"]) != row["payload_sha256"] for row in outcome_rows):
                raise ValueError("stock EOD outcome read bound or checksum mismatch")
            outcomes = {(row["instance_id"], row["alert_id"]): row["payload"] for row in outcome_rows}
    return build_stock_eod_review(publications, outcomes, as_of=as_of, session_date=selected,
        sessions=sessions, storage_ready=True, search=search, model=model, selection_status=selection_status,
        direction=direction, trade_type=trade_type, offset=offset, limit=limit)


def read_published_stock_setup(*, policy, record_id, payload_sha256, episode_id,
                              security_id, ticker, market_cutoff, observed_cutoff,
                              cursor_factory=get_db_cursor, clock=None):
    from equity.behavior_setup import PublishedSetupSourcePolicy, bind_published_stock_setup

    policy = PublishedSetupSourcePolicy.model_validate(policy)
    if market_cutoff.tzinfo is None or observed_cutoff.tzinfo is None or market_cutoff > observed_cutoff:
        raise ValueError("setup reader requires causal aware cutoffs")
    with cursor_factory() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='5s'")
        cursor.execute("""SELECT to_regclass('public.stock_alert_result_records') IS NOT NULL
            AND to_regclass('public.stock_alert_result_instances') IS NOT NULL AS ready""")
        if not cursor.fetchone()["ready"]:
            return None
        cursor.execute("""
            SELECT CASE WHEN octet_length(instance.payload::text) <= 4194304
                        THEN instance.payload END AS instance,
                   CASE WHEN octet_length(record.payload::text) <= 4194304
                        THEN record.payload END AS publication,
                   record.payload_sha256, record.imported_at
            FROM stock_alert_result_records AS record
            JOIN stock_alert_result_instances AS instance USING(instance_id)
            WHERE record.instance_id=%s AND record.kind='publication_evidence'
              AND record.record_id=%s AND record.payload_sha256=%s
              AND record.imported_at<=%s AND instance.created_at<=%s
            LIMIT 1
        """, (policy.instance_id, record_id, payload_sha256, observed_cutoff, observed_cutoff))
        row = cursor.fetchone()
    received_at = clock() if clock else datetime.now(timezone.utc)
    if received_at < observed_cutoff:
        raise ValueError("setup observer receipt precedes query cutoff")
    if row is None:
        return None
    if row["instance"] is None or row["publication"] is None:
        raise ValueError("retained setup payload exceeds read bound")
    if utc(row["publication"].get("market_time", record_id)) > market_cutoff:
        raise ValueError("setup publication exceeds market cutoff")
    if utc(row["publication"]["actual_publication_at"]) > observed_cutoff:
        raise ValueError("setup publication exceeds observation cutoff")
    return bind_published_stock_setup(instance=row["instance"], publication=row["publication"],
        record_id=record_id, payload_sha256=row["payload_sha256"], imported_at=row["imported_at"],
        received_at=received_at, episode_id=episode_id, security_id=security_id, ticker=ticker, policy=policy)


def read_setup_publication_inventory(*, limit=12, cursor_factory=get_db_cursor, clock=None):
    if not 1 <= limit <= 48:
        raise ValueError("setup inventory requires 1..48 publications")
    cutoff = clock() if clock else datetime.now(timezone.utc)
    with cursor_factory() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='5s'")
        cursor.execute("""SELECT to_regclass('public.stock_alert_result_records') IS NOT NULL
            AND to_regclass('public.stock_alert_result_instances') IS NOT NULL
            AND to_regclass('public.stock_alert_result_streams') IS NOT NULL AS ready""")
        if not cursor.fetchone()["ready"]:
            return (), cutoff
        cursor.execute("""
            SELECT record.record_id, record.payload_sha256, record.imported_at,
                   CASE WHEN octet_length(record.payload::text) <= 4194304
                        THEN record.payload END AS publication,
                   instance.payload AS instance
            FROM stock_alert_result_streams AS stream
            JOIN stock_alert_result_instances AS instance USING(instance_id)
            JOIN LATERAL (
                SELECT record_id, payload_sha256, imported_at, payload
                FROM stock_alert_result_records
                WHERE instance_id=stream.instance_id AND kind='publication_evidence'
                  AND imported_at<=%s
                ORDER BY record_id DESC LIMIT %s
            ) AS record ON TRUE
            WHERE stream.stream='intraday' AND instance.created_at<=%s
              AND octet_length(instance.payload::text) <= 4194304
            ORDER BY record.record_id DESC
        """, (cutoff, limit, cutoff))
        rows = tuple(dict(row) for row in cursor.fetchall())
    received_at = clock() if clock else datetime.now(timezone.utc)
    if received_at < cutoff or len(rows) > limit:
        raise ValueError("setup inventory read bound/receipt mismatch")
    return rows, received_at


def read_original_setup_publications(path, record_ids):
    record_ids = tuple(dict.fromkeys(record_ids))
    if not 1 <= len(record_ids) <= 48:
        raise ValueError("original setup comparison requires 1..48 exact keys")
    path = Path(path).resolve()
    if not path.is_file():
        raise ValueError("original setup ledger does not exist")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        manifest = connection.execute("""SELECT policy_hash,
            CASE WHEN length(payload)<=4194304 THEN payload END
            FROM forward_manifest WHERE singleton=1""").fetchone()
        if manifest is None or manifest[1] is None or digest(json.loads(manifest[1])) != manifest[0]:
            raise ValueError("original setup manifest checksum/bound mismatch")
        publications = {}
        for record_id in record_ids:
            row = connection.execute("""SELECT CASE WHEN length(payload)<=4194304 THEN payload END
                FROM forward_publications WHERE window_key=?""", (record_id,)).fetchone()
            if row is None:
                continue
            publication = _decode_original_setup_payload(row[0])
            if publication.get("window_key") != record_id:
                raise ValueError("original setup publication key mismatch")
            publications[record_id] = publication
    return manifest[0], publications


def _decode_original_setup_payload(blob):
    if blob is None or len(blob) > 4194304:
        raise ValueError("original setup compressed payload exceeds bound")
    decoder = zlib.decompressobj()
    raw = decoder.decompress(blob, 16777217)
    if len(raw) > 16777216 or not decoder.eof or decoder.unused_data:
        raise ValueError("original setup decompressed payload exceeds bound or is invalid")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("original setup payload must be an object")
    return payload


def read_direct_stock_setup(path, *, policy, record_id, payload_sha256, episode_id,
                            security_id, ticker, market_cutoff, clock=None):
    from equity.behavior_setup import bind_direct_stock_setup, resolve_direct_setup_policy

    policy = resolve_direct_setup_policy(policy)
    started_at = clock() if clock else datetime.now(timezone.utc)
    if market_cutoff.tzinfo is None or started_at.tzinfo is None or market_cutoff > started_at:
        raise ValueError("direct setup requires a causal market cutoff")
    path = Path(path).resolve()
    if not path.is_file():
        return None
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        manifest = connection.execute("""SELECT policy_hash,
            CASE WHEN length(payload)<=4194304 THEN payload END
            FROM forward_manifest WHERE singleton=1""").fetchone()
        if manifest is None or manifest[1] is None or manifest[0] != policy.instance_policy_sha256:
            raise ValueError("direct setup manifest identity/bound mismatch")
        config = json.loads(manifest[1])
        if digest(config) != manifest[0]:
            raise ValueError("direct setup manifest checksum mismatch")
        row = connection.execute("""SELECT CASE WHEN length(payload)<=4194304 THEN payload END
            FROM forward_publications WHERE window_key=?""", (record_id,)).fetchone()
        if row is None:
            return None
        publication = _decode_original_setup_payload(row[0])
        checkpoint = connection.execute("""SELECT CASE WHEN length(payload)<=4194304 THEN payload END
            FROM forward_checkpoint WHERE singleton=1""").fetchone()
        if checkpoint is None:
            raise ValueError("direct setup enrollment checkpoint unavailable")
        state = _decode_original_setup_payload(checkpoint[0])
        instance = strategy_instance(config, state, "intraday") | {"policy": config, "enrollment": state["members"]}
    received_at = clock() if clock else datetime.now(timezone.utc)
    if received_at < started_at:
        raise ValueError("direct setup receipt moved backwards")
    if utc(publication.get("market_time", record_id)) > market_cutoff:
        raise ValueError("direct setup publication exceeds market cutoff")
    if utc(publication["actual_publication_at"]) > started_at:
        raise ValueError("direct setup publication was not available at read cutoff")
    return bind_direct_stock_setup(instance=instance, publication=publication, record_id=record_id,
        payload_sha256=payload_sha256, received_at=received_at, episode_id=episode_id,
        security_id=security_id, ticker=ticker, policy=policy)


def read_direct_stock_setups(path, *, policies, underlyers, market_cutoff, as_of, clock=None):
    return _read_direct_stock_setup_batch(path, policies=policies, underlyers=underlyers,
        market_cutoff=market_cutoff, as_of=as_of, clock=clock)[0]


def read_direct_stock_setup_windows(path, *, policies, windows, as_of, clock=None):
    from collections import Counter

    if not 1 <= len(windows) <= 13:
        raise ValueError("aligned stock setup read requires 1..13 underlyings")
    for cutoff, boundary in windows.values():
        if (cutoff.utcoffset() is None or cutoff > as_of
                or boundary is not None and (boundary.utcoffset() is None or boundary > cutoff)):
            raise ValueError("aligned stock setup window must be causal")
    try:
        return _read_direct_stock_setup_batch(path, policies=policies, underlyers=tuple(windows),
            market_cutoff=max(cutoff for cutoff, _ in windows.values()), as_of=as_of,
            clock=clock, windows=windows)
    except (OSError, sqlite3.Error, zlib.error, ValueError, KeyError, TypeError):
        return (), Counter({"S1_STOCK_SOURCE_UNAVAILABLE": len(windows), "S2_STOCK_SOURCE_UNAVAILABLE": len(windows)})


def read_direct_stock_setup_shadow_windows(path, *, policies, windows, as_of, clock=None):
    from collections import Counter

    if not 1 <= len(windows) <= 13:
        raise ValueError("aligned stock setup shadow read requires 1..13 underlyings")
    for cutoff, boundary in windows.values():
        if (cutoff.utcoffset() is None or cutoff > as_of
                or boundary is not None and (boundary.utcoffset() is None or boundary > cutoff)):
            raise ValueError("aligned stock setup shadow window must be causal")
    try:
        _, rejections, shadows = _read_direct_stock_setup_batch(path, policies=policies, underlyers=tuple(windows),
            market_cutoff=max(cutoff for cutoff, _ in windows.values()), as_of=as_of,
            clock=clock, windows=windows, include_shadow=True)
        return shadows, rejections
    except (OSError, sqlite3.Error, zlib.error, ValueError, KeyError, TypeError):
        return (), Counter({"S1_SHADOW_SOURCE_UNAVAILABLE": len(windows), "S2_SHADOW_SOURCE_UNAVAILABLE": len(windows)})


def _read_direct_stock_setup_batch(path, *, policies, underlyers, market_cutoff, as_of, clock=None, windows=None,
                                   include_shadow=False):
    from collections import Counter
    from uuid import UUID
    from equity.behavior_setup import bind_direct_stock_setup, resolve_direct_setup_policy

    policies = tuple(resolve_direct_setup_policy(policy) for policy in policies)
    if (not policies or len(policies) > 2 or len({policy.instance_id for policy in policies}) != 1
            or not 1 <= len(underlyers) <= 13 or market_cutoff.utcoffset() is None
            or as_of.utcoffset() is None or market_cutoff > as_of):
        raise ValueError("direct setup batch requires one pinned source and causal bounded scope")
    path = Path(path).resolve()
    if not path.is_file():
        raise ValueError("pinned direct setup ledger is missing")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        manifest = connection.execute("SELECT policy_hash,CASE WHEN length(payload)<=4194304 THEN payload END FROM forward_manifest WHERE singleton=1").fetchone()
        if manifest is None or manifest[1] is None or manifest[0] != policies[0].instance_policy_sha256:
            raise ValueError("pinned setup manifest identity or bound mismatch")
        config = json.loads(manifest[1])
        if digest(config) != manifest[0]:
            raise ValueError("pinned setup manifest checksum mismatch")
        checkpoint = connection.execute("SELECT CASE WHEN length(payload)<=4194304 THEN payload END FROM forward_checkpoint WHERE singleton=1").fetchone()
        if checkpoint is None:
            raise ValueError("pinned setup checkpoint unavailable")
        state = _decode_original_setup_payload(checkpoint[0])
        instance = strategy_instance(config, state, "intraday") | {"policy": config, "enrollment": state["members"]}
        if any(instance["instance_id"] != policy.instance_id or instance["policy_hash"] != policy.instance_policy_sha256 for policy in policies):
            raise ValueError("pinned setup instance changed")
        rows = connection.execute("SELECT window_key,CASE WHEN length(payload)<=4194304 THEN payload END FROM forward_publications ORDER BY window_key DESC LIMIT 12").fetchall()
        publications, size = [], 0
        for record_id, blob in rows:
            publication = _decode_original_setup_payload(blob)
            size += len(json.dumps(publication, allow_nan=False).encode("utf-8"))
            if size > 67108864:
                raise ValueError("direct setup batch exceeds payload budget")
            publications.append((record_id, publication))
    received_at = clock() if clock else datetime.now(timezone.utc)
    if received_at < as_of:
        raise ValueError("direct setup batch receipt moved backwards")
    rejections = Counter()
    if windows is not None:
        selected = []
        securities = {member["ticker"]: member["security_id"] for member in instance["enrollment"] if "ticker" in member}
        for ticker, (cutoff, boundary) in windows.items():
            eligible = [(record_id, publication) for record_id, publication in publications
                if boundary is not None and utc(publication.get("market_time", record_id)) == boundary
                and utc(publication["actual_publication_at"]) <= as_of]
            latest = max(eligible, key=lambda item: utc(item[1]["actual_publication_at"]), default=None)
            reason = None
            if boundary is None:
                reason = "STOCK_WINDOW_NOT_YET_CLOSED"
            elif latest is None:
                reason = "STOCK_WINDOW_NOT_AVAILABLE"
            elif latest[1].get("coverage") != "PUBLISHED":
                reason = "STOCK_WINDOW_NOT_PUBLISHED"
            elif any(latest[1].get("runtime_sources") != dict(policy.runtime_sources)
                    or latest[1].get("policy_hash") != policy.publication_policy_sha256 for policy in policies):
                reason = "STOCK_WINDOW_POLICY_OR_RUNTIME_MISMATCH"
            elif (securities.get(ticker) not in latest[1].get("expected_members", ())
                    or securities.get(ticker) in latest[1].get("missing_members", ())):
                reason = "STOCK_MEMBER_UNAVAILABLE"
            if reason:
                for model in ("S1", "S2"):
                    rejections[f"{model}_{reason}"] += 1
                continue
            selected.append((*latest, ticker))
        publications = selected
    else:
        publications = [(record_id, publication, None) for record_id, publication in publications]
    results, seen, shadows, shadow_seen = [], set(), [], set()
    for record_id, publication, ticker in publications:
        if utc(publication.get("market_time", record_id)) > market_cutoff or utc(publication["actual_publication_at"]) > as_of:
            continue
        matched_models = set()
        for episode_id, candidate in publication.get("candidates", {}).items():
            if (candidate["ticker"] not in underlyers or candidate["interval"] != "1h"
                    or ticker is not None and candidate["ticker"] != ticker
                    or episode_id in seen):
                continue
            policy = next((policy for policy in policies if policy.detector_version == candidate["policy_version"]), None)
            if policy is None:
                continue
            if include_shadow and episode_id not in shadow_seen:
                from options.stock_setup_binding import build_stock_setup_shadow_source

                try:
                    dispositions = [row for row in publication.get("dispositions", ()) if row.get("episode_id") == episode_id]
                    if len(dispositions) != 1:
                        raise ValueError("stock setup shadow source requires one retained disposition")
                    disposition = dispositions[0]
                    shadows.append(build_stock_setup_shadow_source(candidate_payload=candidate, episode_id=episode_id,
                        policy=policy, publication_payload_sha256=digest(publication),
                        publication_market_time=utc(publication.get("market_time", record_id)),
                        published_at=utc(publication["actual_publication_at"]), received_at=received_at,
                        setup_selection=disposition["selection"], setup_reason=disposition.get("reason")))
                    shadow_seen.add(episode_id)
                    if len(shadows) > 5000:
                        raise ValueError("direct setup shadow batch exceeds episode bound")
                except ValueError:
                    pass
            if utc(candidate["expires_at"]) <= received_at:
                continue
            try:
                evidence = bind_direct_stock_setup(instance=instance, publication=publication, record_id=record_id,
                    payload_sha256=digest(publication), received_at=received_at, episode_id=episode_id,
                    security_id=UUID(candidate["security_id"]), ticker=candidate["ticker"], policy=policy)
            except ValueError:
                continue
            results.append(evidence)
            seen.add(episode_id)
            matched_models.add("S2" if evidence.expected_model == "resumption" else "S1")
            if len(results) > 5000:
                raise ValueError("direct setup batch exceeds episode bound")
        if ticker is not None:
            for model in {"S1", "S2"} - matched_models:
                rejections[f"{model}_NO_ACTIVE_EPISODE_IN_STOCK_WINDOW"] += 1
    if include_shadow:
        return tuple(results), rejections, tuple(shadows)
    return tuple(results), rejections


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