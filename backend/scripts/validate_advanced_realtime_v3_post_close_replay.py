#!/usr/bin/env python3
"""Validate 45 sealed V3 cycles against the bounded post-close history export."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import sql


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
TEST_SUPPORT = BACKEND_DIR / "tests" / "realtime"
for path in (BACKEND_DIR, TEST_SUPPORT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from realtime.advanced_options_acquisition_runtime import (  # noqa: E402
    CONTROL_MESSAGES_PER_ATTEMPT,
    acquisition_binding_payload,
    run_acquisition,
)
from realtime.advanced_equity_ingress import (  # noqa: E402
    AdvancedEquityJournalPump,
)
from realtime.advanced_equity_journal_bridge import (  # noqa: E402
    AdvancedEquityJournalBridgeConfig,
    process_advanced_equity_journal_once,
)
from realtime.advanced_realtime_session_operations import (  # noqa: E402
    build_diagnostic_authorization,
    diagnostic_authorization_payload,
)
from realtime.buffer import BufferLimits, RawMessageBuffer  # noqa: E402
from realtime.contracts import (  # noqa: E402
    AssetClass,
    RawMessageEnvelope,
    canonical_sha256,
)
from realtime.database_access import RealtimeDatabaseRole  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.journal import RawJournalReader  # noqa: E402
from realtime.normalizer import (  # noqa: E402
    NORMALIZER_VERSION,
    InstrumentAdmission,
)
from realtime.security_v2 import (  # noqa: E402
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_database_v2,
)
from realtime.stage_database_v2 import V2WorkerDatabase  # noqa: E402
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5  # noqa: E402
from scripts import qualify_advanced_options_v3_docker as qualification  # noqa: E402
from scripts.audit_realtime_v2_target9_scanner_warmup import (  # noqa: E402
    build_report as build_warmup_report,
)
from scripts.export_advanced_realtime_v3_post_close_history import (  # noqa: E402
    _apply_options_supplement,
    _load_relation,
)
from scripts.validate_advanced_realtime_v3_host_pipeline import (  # noqa: E402
    ArtifactConnection,
    _processes,
    _role_command,
    _safe_environment,
    _start_process,
    _stop_processes,
)
from scripts.validate_realtime_v2_schema import _connect  # noqa: E402
from v2_database import disposable_v2_database  # noqa: E402


UTC = timezone.utc


class ThrottledArtifactConnection(ArtifactConnection):
    def __init__(
        self,
        frames,
        delay_seconds=0.025,
        yield_every_frames=16,
    ):
        super().__init__(frames)
        self.delay_seconds = delay_seconds
        self.yield_every_frames = yield_every_frames
        self.received = 0

    async def recv(self, *, decode=None):
        market_frames = self.received - 2
        if (
            market_frames > 0
            and market_frames % self.yield_every_frames == 0
        ):
            await __import__("asyncio").sleep(self.delay_seconds)
        self.received += 1
        return await super().recv(decode=decode)


def _read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return value


def _write(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _journal_frames(path: Path, cutoff: int):
    reader = RawJournalReader(path)
    recovery = reader.recovery_state(NORMALIZER_VERSION)
    if (
        recovery.integrity_check != "ok"
        or recovery.quarantine_count != 0
        or recovery.raw_committed_offset < cutoff
    ):
        raise ValueError("sealed Options journal is not replayable")
    rows = []
    offset = 0
    while offset < cutoff:
        batch = reader.read_after(
            offset,
            limit=min(10_000, cutoff - offset),
        )
        if not batch:
            break
        rows.extend(row.payload for row in batch)
        offset = batch[-1].raw_offset
    if len(rows) != cutoff:
        raise ValueError("sealed Options replay cutoff changed")
    first = reader.read_after(0, limit=1)[0]
    return (
        rows,
        recovery,
        first.stream_session_id,
        sum(map(len, rows)),
    )


def _equity_replay_journal(
    source_path,
    output_path,
    cutoff,
    members,
):
    source = RawJournalReader(source_path)
    recovery = source.recovery_state(NORMALIZER_VERSION)
    if (
        recovery.integrity_check != "ok"
        or recovery.quarantine_count != 0
        or recovery.raw_committed_offset < cutoff
        or recovery.normalized_committed_event_offset < cutoff
    ):
        raise ValueError("sealed Equity journal is not replayable")
    by_ticker = {
        str(row["ticker"]): int(row["instrument_id"])
        for row in members
    }
    admissions = {
        ticker: InstrumentAdmission(
            instrument_id=instrument_id,
            provider_symbol=ticker,
            underlying=ticker,
            asset_class=AssetClass.EQUITY,
            shard_id=0,
        )
        for ticker, instrument_id in by_ticker.items()
    }
    rows = []
    offset = 0
    while offset < cutoff:
        batch = source.read_after(
            offset,
            limit=min(10_000, cutoff - offset),
        )
        if not batch:
            break
        rows.extend(batch)
        offset = batch[-1].raw_offset
    if len(rows) != cutoff:
        raise ValueError("sealed Equity replay cutoff changed")
    buffer = RawMessageBuffer(
        BufferLimits(
            maximum_events=cutoff,
            maximum_bytes=64 * 1024 * 1024,
            maximum_age_ns=60_000_000_000,
        ),
        monotonic_ns=time.perf_counter_ns,
    )
    for row in rows:
        buffer.put_nowait(RawMessageEnvelope(
            stream_session_id=row.stream_session_id,
            message_number=row.message_number,
            feed=row.feed,
            received_at=row.received_at,
            received_monotonic_ns=time.perf_counter_ns(),
            payload=row.payload,
            schema_version=row.schema_version,
        ))
    pump = AdvancedEquityJournalPump(
        buffer,
        output_path,
        recovery.generation,
        admissions,
        maximum_batch_messages=500,
        maximum_batch_bytes=64 * 1024 * 1024,
    )
    pump.start()
    pump.request_stop()
    progress = pump.join(60)
    if (
        progress.last_raw_offset != cutoff
        or progress.last_event_offset != cutoff
    ):
        raise RuntimeError("derived Equity replay journal is incomplete")
    return {
        "path": str(output_path),
        "file_sha256": _sha256(output_path),
        "source_file_sha256": _sha256(source_path),
        "journal_generation": str(recovery.generation),
        "stream_session_id": str(rows[0].stream_session_id),
        "first_received_at_utc": rows[0].received_at.isoformat(),
        "last_received_at_utc": rows[-1].received_at.isoformat(),
        "raw_offset": progress.last_raw_offset,
        "event_offset": progress.last_event_offset,
        "excluded_source_tail_raw_count": (
            recovery.raw_committed_offset - cutoff
        ),
    }


def _target_columns(cursor, relation):
    schema, table = relation.split(".", 1)
    cursor.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s "
        "ORDER BY ordinal_position",
        (schema, table),
    )
    columns = [row[0] for row in cursor.fetchall()]
    if not columns:
        raise RuntimeError(f"history target relation is missing: {relation}")
    return columns


def _load_history(cursor, export_report, export_root, supplement, as_of):
    cursor.execute(
        "SELECT rt.ensure_partitions(%s,%s)",
        ("rt.bar_revisions", as_of.date()),
    )
    cursor.execute(
        "SELECT rt.ensure_partitions(%s,%s)",
        ("rt.portal_releases", as_of.date()),
    )
    loaded = []
    for row in export_report["exports"]:
        relation = row["relation"]
        path = export_root / f"{relation.replace('.', '-')}.csv"
        if not path.is_file() or _sha256(path) != row["file_sha256"]:
            raise ValueError(f"history export file identity changed: {relation}")
        _load_relation(cursor, {
            "relation": relation,
            "columns": _target_columns(cursor, relation),
            "file": str(path),
        })
        loaded.append({
            "relation": relation,
            "row_count": int(row["row_count"]),
            "file_sha256": row["file_sha256"],
        })
    _apply_options_supplement(cursor, supplement)
    return loaded


def _bind_target_history(
    cursor,
    seed,
    history,
    authorization_reference,
    equity_replay,
):
    cursor.execute(
        "INSERT INTO rt.subscription_members ("
        "manifest_id,instrument_id,channel,priority,purpose,"
        "option_underlying"
        ") SELECT %s,member.instrument_id,member.channel,"
        "member.priority,member.purpose,member.option_underlying "
        "FROM rt.subscription_members AS member "
        "JOIN rt.cohorts AS cohort USING(manifest_id) "
        "WHERE cohort.cohort_id=%s AND member.channel='AM'",
        (seed["manifest_id"], history["history_cohort_id"]),
    )
    continuity = []
    observed_at = datetime.fromisoformat(
        equity_replay["first_received_at_utc"]
    ).astimezone(UTC)
    for member in history["members"]:
        cursor.execute(
            "SELECT * FROM ref.security_revisions "
            "WHERE security_id=%s ORDER BY observed_at DESC,"
            "revision_id DESC LIMIT 1",
            (member["security_id"],),
        )
        source = cursor.fetchone()
        columns = tuple(
            column.name for column in cursor.description
        )
        if source is None:
            raise RuntimeError(
                "security continuity source revision is unavailable"
            )
        values = dict(zip(columns, source))
        effective_from = (
            values["effective_to"]
            if values["effective_to"] is not None
            else observed_at
        )
        revision_id = uuid5(
            NAMESPACE_URL,
            (
                "post-close-security-continuity:"
                f"{values['security_id']}:{effective_from.isoformat()}"
            ),
        )
        provenance = {
            "classification": "DISPOSABLE_REPLAY_CONTINUITY",
            "source_revision_id": str(values["revision_id"]),
            "security_id": str(values["security_id"]),
            "ticker": values["ticker"],
            "effective_from": effective_from,
            "observed_at": observed_at,
            "authorization_reference": authorization_reference,
        }
        values.update({
            "revision_id": revision_id,
            "effective_from": effective_from,
            "effective_to": None,
            "observed_at": observed_at,
            "payload_sha256": canonical_sha256(provenance),
            "imported_at": datetime.now(UTC),
        })
        statement = sql.SQL(
            "INSERT INTO ref.security_revisions ({}) VALUES ({})"
        ).format(
            sql.SQL(",").join(
                sql.Identifier(name) for name in columns
            ),
            sql.SQL(",").join(
                sql.Placeholder() for _ in columns
            ),
        )
        cursor.execute(
            statement,
            tuple(values[name] for name in columns),
        )
        continuity.append({
            **provenance,
            "revision_id": str(revision_id),
            "payload_sha256": values["payload_sha256"],
        })
    cursor.execute(
        "INSERT INTO rt.stream_sessions ("
        "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
        "started_at,authenticated_at,subscribed_at,ended_at,status"
        ") VALUES (%s,'massive','STOCKS',%s,%s,1,%s,%s,%s,%s,'STOPPED')",
        (
            equity_replay["stream_session_id"],
            seed["manifest_id"],
            str(uuid5(
                NAMESPACE_URL,
                (
                    "post-close-equity-owner:"
                    f"{equity_replay['journal_generation']}"
                ),
            )),
            equity_replay["first_received_at_utc"],
            equity_replay["first_received_at_utc"],
            equity_replay["first_received_at_utc"],
            equity_replay["last_received_at_utc"],
        ),
    )
    history_before = datetime.fromisoformat(
        history["as_of_utc"]
    ).astimezone(UTC)
    binding = {
        "target_cohort_id": seed["cohort_id"],
        "source_cohort_id": history["history_cohort_id"],
        "source_provider_tier": (
            history["history_binding"]["source_provider_tier"]
        ),
        "history_before": history_before,
        "source_manifest_sha256": (
            history["history_binding"]["source_manifest_sha256"]
        ),
        "authorization_reference": authorization_reference,
    }
    binding_sha256 = canonical_sha256(binding)
    cursor.execute(
        "INSERT INTO rt.cohort_history_bindings ("
        "target_cohort_id,source_cohort_id,source_provider_tier,"
        "history_before,source_manifest_sha256,"
        "authorization_reference,binding_sha256"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (
            binding["target_cohort_id"],
            binding["source_cohort_id"],
            binding["source_provider_tier"],
            binding["history_before"],
            binding["source_manifest_sha256"],
            binding["authorization_reference"],
            binding_sha256,
        ),
    )
    return {
        **binding,
        "binding_sha256": binding_sha256,
        "security_continuity": continuity,
    }


def _rollback_probe(connection, seed, package):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT universe_sha256 FROM rt.subscription_manifests "
            "WHERE manifest_id=%s",
            (seed["manifest_id"],),
        )
        universe_sha256 = cursor.fetchone()[0]
        cursor.execute(
            "SELECT receipt_id,cutoff FROM rt.cycle_receipts "
            "WHERE cohort_id=%s ORDER BY cutoff DESC LIMIT 1",
            (seed["cohort_id"],),
        )
        receipt = cursor.fetchone()
        synthetic_receipt = receipt is None
        if receipt is None:
            cursor.execute(
                "SELECT window_id,cutoff FROM rt.window_runs "
                "WHERE plan_id=%s ORDER BY cutoff DESC LIMIT 1",
                (seed["plan_id"],),
            )
            window = cursor.fetchone()
            if window is None:
                raise RuntimeError(
                    "rollback probe has no disposable window"
                )
            receipt_id = uuid4()
            cursor.execute(
                "INSERT INTO rt.cycle_receipts ("
                "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
                "reference_generation,universe_sha256,input_sha256,"
                "build_id"
                ") VALUES (%s,%s,%s,'PORTAL_READY',%s,1,%s,%s,%s)",
                (
                    str(receipt_id),
                    str(window[0]),
                    seed["cohort_id"],
                    window[1],
                    universe_sha256,
                    package["package_sha256"],
                    "post-close-disposable-rollback-probe-v1",
                ),
            )
            receipt = (receipt_id, window[1])
        release_id = uuid4()
        manifest = {
            "classification": "DISPOSABLE_ROLLBACK_PROBE",
            "package_sha256": package["package_sha256"],
        }
        manifest_sha256 = canonical_sha256(manifest)
        cursor.execute(
            "INSERT INTO rt.portal_releases ("
            "release_id,cohort_id,provider_tier,release_class,"
            "receipt_id,session_date,cutoff,universe_sha256,"
            "manifest,manifest_sha256"
            ") VALUES (%s,%s,'MASSIVE_ADVANCED','INTRADAY',"
            "%s,%s,%s,%s,%s::jsonb,%s)",
            (
                str(release_id),
                seed["cohort_id"],
                str(receipt[0]),
                package["window"]["session_date"],
                receipt[1],
                universe_sha256,
                json.dumps(manifest, sort_keys=True),
                manifest_sha256,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.portal_release_current ("
            "cohort_id,provider_tier,release_id,cutoff,session_date"
            ") VALUES (%s,'MASSIVE_ADVANCED',%s,%s,%s)",
            (
                seed["cohort_id"],
                str(release_id),
                receipt[1],
                package["window"]["session_date"],
            ),
        )
        cursor.execute(
            "INSERT INTO rt.advanced_options_read_pins ("
            "cohort_id,release_id,release_cutoff,package_sha256,"
            "enabled,enabled_at"
            ") VALUES (%s,%s,%s,%s,true,clock_timestamp())",
            (
                seed["cohort_id"],
                str(release_id),
                receipt[1],
                package["package_sha256"],
            ),
        )
        cursor.execute(
            "SELECT COUNT(*) FROM rt.advanced_options_read_pins "
            "WHERE cohort_id=%s AND enabled",
            (seed["cohort_id"],),
        )
        visible_before_rollback = int(cursor.fetchone()[0])
    connection.rollback()
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM rt.portal_releases "
            "WHERE release_id=%s",
            (str(release_id),),
        )
        release_after = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT COUNT(*) FROM rt.advanced_options_read_pins "
            "WHERE cohort_id=%s",
            (seed["cohort_id"],),
        )
        pin_after = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT COUNT(*) FROM rt.cycle_receipts "
            "WHERE receipt_id=%s",
            (str(receipt[0]),),
        )
        receipt_after = int(cursor.fetchone()[0])
    connection.rollback()
    return {
        "status": (
            "PASS"
            if visible_before_rollback == 1
            and release_after == 0
            and pin_after == 0
            and (not synthetic_receipt or receipt_after == 0)
            else "FAIL"
        ),
        "synthetic_disposable_receipt": synthetic_receipt,
        "visible_before_rollback": visible_before_rollback,
        "release_rows_after_rollback": release_after,
        "pin_rows_after_rollback": pin_after,
        "receipt_rows_after_rollback": receipt_after,
    }


def _seed_canonical_receipts(connection, seed, manifest):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT universe_sha256 FROM rt.subscription_manifests "
                "WHERE manifest_id=%s",
                (seed["manifest_id"],),
            )
            universe_sha256 = cursor.fetchone()[0]
            inserted = 0
            for window_id, cycle in zip(
                seed["window_ids"],
                manifest["cycles"]["cycles"],
                strict=True,
            ):
                receipt_id = uuid5(
                    NAMESPACE_URL,
                    f"post-close-input-ready:{window_id}",
                )
                stage_run_id = uuid5(
                    NAMESPACE_URL,
                    f"post-close-equity-materialize:{window_id}",
                )
                cutoff = datetime.fromisoformat(
                    cycle["boundary_utc"]
                ).astimezone(UTC)
                cursor.execute(
                    "INSERT INTO rt.cycle_receipts ("
                    "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
                    "reference_generation,universe_sha256,input_sha256,"
                    "build_id"
                    ") VALUES (%s,%s,%s,'INPUT_READY',%s,1,%s,%s,%s)",
                    (
                        str(receipt_id),
                        window_id,
                        seed["cohort_id"],
                        cutoff,
                        universe_sha256,
                        cycle["file_sha256"],
                        "post-close-equity-materialize-v1",
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.stage_runs ("
                    "stage_run_id,window_id,stage_id,stage_version,"
                    "attempt,state,receipt_id,started_at,finished_at"
                    ") VALUES (%s,%s,'EQUITY_MATERIALIZE',%s,1,"
                    "'COMPLETE',%s,clock_timestamp(),clock_timestamp()) "
                    "ON CONFLICT (window_id,stage_id,attempt) DO UPDATE "
                    "SET state='COMPLETE',receipt_id=excluded.receipt_id,"
                    "started_at=excluded.started_at,"
                    "finished_at=excluded.finished_at,"
                    "reason_codes=ARRAY[]::text[]",
                    (
                        str(stage_run_id),
                        window_id,
                        STAGE_CATALOG_VERSION_V5,
                        str(receipt_id),
                    ),
                )
                inserted += 1
    return inserted


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-manifest", type=Path, required=True)
    parser.add_argument("--history-source", type=Path, required=True)
    parser.add_argument("--history-export-report", type=Path, required=True)
    parser.add_argument("--options-reference", type=Path, required=True)
    parser.add_argument("--equity-journal", type=Path, required=True)
    parser.add_argument("--options-journal", type=Path, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in (
        "replay_manifest",
        "history_source",
        "history_export_report",
        "options_reference",
        "equity_journal",
        "options_journal",
        "output_directory",
    ):
        setattr(args, name, getattr(args, name).resolve())
    if (
        any(
            not getattr(args, name).is_file()
            for name in (
                "replay_manifest",
                "history_source",
                "history_export_report",
                "options_reference",
                "equity_journal",
                "options_journal",
            )
        )
        or args.output_directory.exists()
        or not args.output_directory.parent.is_dir()
        or not args.authorization_reference.strip()
        or not args.execute
    ):
        parser.error("post-close validation scope is invalid")
    return args


def execute(args):
    manifest = _read(args.replay_manifest)
    history = _read(args.history_source)
    export_report = _read(args.history_export_report)
    supplement = _read(args.options_reference)
    if (
        manifest.get("status") != "READY_FOR_READ_ONLY_HISTORY_EXPORT"
        or manifest.get("classification") != "FAILED_SEGMENT_REPLAY_ONLY"
        or manifest.get("cycles", {}).get("count") != 45
        or history.get("status") != "PASS"
        or export_report.get("status") != "PASS"
        or export_report.get("target", {}).get(
            "database_exists_after_cleanup"
        ) is not False
        or supplement.get("status") != "PASS"
    ):
        raise ValueError("post-close validation inputs are invalid")
    if (
        _sha256(args.equity_journal)
        != manifest["journals"]["equity"]["file_sha256"]
        or _sha256(args.options_journal)
        != manifest["journals"]["options"]["file_sha256"]
    ):
        raise ValueError("sealed journal file identity changed")
    cutoffs = tuple(
        datetime.fromisoformat(row["boundary_utc"]).astimezone(UTC)
        for row in manifest["cycles"]["cycles"]
    )
    option_cutoff = int(
        manifest["journals"]["options"]["replay_cutoff"]["raw_offset"]
    )
    frames, option_recovery, option_stream_session_id, option_bytes = (
        _journal_frames(args.options_journal, option_cutoff)
    )
    output = args.output_directory
    output.mkdir()
    equity_cutoff = int(
        manifest["journals"]["equity"]["replay_cutoff"]["raw_offset"]
    )
    equity_replay = _equity_replay_journal(
        args.equity_journal,
        output / "equity-replay.sqlite",
        equity_cutoff,
        history["members"],
    )
    suffix = uuid4().hex[:12]
    database = f"test_realtime_v2_v3postrun_{suffix}"
    roles = build_role_names_v2(f"test_rt_v3post_{suffix}")
    settings = qualification._settings()
    login = roles.prefix + "_worker"
    login_owned = False
    roles_owned = False
    worker_pool = None
    processes = {}
    handles = []
    durable = None
    restart_evidence = None
    acquisition_report = None
    readers = None
    rollback = None
    history_binding = None
    loaded = None
    started_at = datetime.now(UTC)
    try:
        with disposable_v2_database(database) as admin_pool:
            connection = admin_pool.getconn()
            try:
                with connection:
                    with connection.cursor() as cursor:
                        loaded = _load_history(
                            cursor,
                            export_report,
                            args.history_export_report.parent,
                            supplement,
                            datetime.fromisoformat(history["as_of_utc"]),
                        )
                seed = qualification._seed(
                    admin_pool,
                    session_date=datetime.fromisoformat(
                        manifest["session_date"]
                    ).date(),
                    suffix=suffix,
                    window_cutoffs=cutoffs,
                    validation_duration_minutes=60,
                    identity_time=(
                        cutoffs[0] - timedelta(minutes=5)
                    ),
                )
                seed["journal_generation"] = str(
                    option_recovery.generation
                )
                with connection:
                    with connection.cursor() as cursor:
                        history_binding = _bind_target_history(
                            cursor,
                            seed,
                            history,
                            args.authorization_reference.strip(),
                            equity_replay,
                        )
                _, security = harden_database_v2(connection, roles)
                roles_owned = True
                if any(
                    not verdict.startswith("PASS")
                    for _, verdict in security
                ):
                    raise RuntimeError(
                        "post-close disposable security audit failed"
                    )
            finally:
                admin_pool.putconn(connection)
            login, host_dsn = qualification._worker_login(
                settings,
                database,
                roles,
            )
            login_owned = True
            worker_pool = ThreadedConnectionPool(1, 10, dsn=host_dsn)
            provider = V2ConnectionProvider(
                {APP_ROLE: worker_pool},
                V2AccessConfig(
                    security_mode="hardened",
                    role_prefix=roles.prefix,
                    allow_disposable=True,
                ),
            )
            provider.verify()
            equity_database = V2WorkerDatabase(
                provider,
                RealtimeDatabaseRole.COMPUTE,
            )
            equity_config = AdvancedEquityJournalBridgeConfig(
                cohort_id=UUID(seed["cohort_id"]),
                journal_generation=UUID(
                    equity_replay["journal_generation"]
                ),
                stream_session_id=UUID(
                    equity_replay["stream_session_id"]
                ),
                owner_epoch=1,
                normalizer_version=NORMALIZER_VERSION,
                shard_ids=(0,),
                batch_limit=500,
            )
            equity_statuses = []
            while (
                not equity_statuses
                or equity_statuses[-1].committed_offset < equity_cutoff
            ):
                status = process_advanced_equity_journal_once(
                    equity_database,
                    RawJournalReader(equity_replay["path"]),
                    equity_config,
                    checked_at=datetime.now(UTC),
                )
                equity_statuses.append(status)
                if (
                    status.committed_offset < equity_cutoff
                    and status.scanned_records == 0
                ):
                    raise RuntimeError(
                        "Equity journal replay made no progress"
                    )
            equity_ingestion = {
                **equity_replay,
                "batches": len(equity_statuses),
                "committed_offset": (
                    equity_statuses[-1].committed_offset
                ),
                "persisted_bars": sum(
                    row.persisted_bars for row in equity_statuses
                ),
            }
            source_identity = canonical_sha256({
                "replay_manifest": manifest["semantic_sha256"],
                "history_export": export_report["semantic_sha256"],
                "options_reference": supplement["semantic_sha256"],
            })
            runtime_identity = "sha256:" + hashlib.sha256(
                Path(sys.executable).read_bytes()
            ).hexdigest()
            helper_args = SimpleNamespace(
                session_date=datetime.fromisoformat(
                    manifest["session_date"]
                ).date(),
                qualification_semantic_sha256=source_identity,
                qualification_file_sha256=canonical_sha256({
                    "replay_manifest": _sha256(args.replay_manifest),
                    "history_export": _sha256(args.history_export_report),
                }),
                image_id=runtime_identity,
                flow_release_delay_seconds=0,
                package_window_minutes=60,
            )
            contracts = tuple(manifest["cycles"]["contracts"])
            acquisition_replay_clock = cutoffs[0]
            binding = qualification._qualification_acquisition_binding(
                seed,
                helper_args,
                contract_tickers=contracts,
                now=acquisition_replay_clock,
                connection_attempt_ceiling=1,
                decoded_message_ceiling=(
                    option_cutoff + CONTROL_MESSAGES_PER_ATTEMPT
                ),
                decoded_byte_ceiling=64 * 1024 * 1024,
            )
            package, bundle = qualification._qualification_artifacts(
                seed,
                helper_args,
                binding,
            )
            qualification._seed_service_leases(admin_pool, bundle)
            package_path = output / "package.json"
            bundle_path = output / "execution-bundle.json"
            binding_path = output / "acquisition-binding.json"
            downstream = output / "downstream-bindings"
            status_dir = output / "p8-status"
            journal_root = output / "journals"
            downstream.mkdir()
            status_dir.mkdir()
            _write(package_path, package)
            _write(bundle_path, bundle)
            _write(binding_path, acquisition_binding_payload(binding))
            try:
                acquisition_report = run_acquisition(
                    binding,
                    provider=provider,
                    api_key="sealed-artifact-no-provider",
                    journal_root=journal_root,
                    status_path=output / "options-acquisition-status.json",
                    lock_path=output / "options.lock",
                    stop_event=__import__("threading").Event(),
                    connection_factory=(
                        lambda _url: ThrottledArtifactConnection(frames)
                    ),
                    clock=lambda: acquisition_replay_clock,
                    diagnostic=False,
                )
            except Exception as error:
                raise RuntimeError(
                    "sealed Options acquisition replay failed"
                ) from error
            if (
                acquisition_report["last_raw_offset"] != option_cutoff
                or acquisition_report["last_event_offset"]
                != manifest["journals"]["options"]["replay_cutoff"][
                    "event_offset"
                ]
            ):
                raise RuntimeError(
                    "sealed Options journal replay offsets changed"
                )
            diagnostic = build_diagnostic_authorization(
                authorization_reference=(
                    f"post-close-replay:{suffix}"
                ),
                owner=f"stockscreener-v3post-{suffix}",
                session_date=helper_args.session_date,
                cohort_id=UUID(seed["cohort_id"]),
                source_sha256=source_identity,
                backend_image_id=runtime_identity,
                input_evidence_sha256=manifest["semantic_sha256"],
            )
            diagnostic_path = output / "diagnostic.json"
            _write(
                diagnostic_path,
                diagnostic_authorization_payload(diagnostic),
            )
            environment = {
                **os.environ,
                "REALTIME_V2_APP_DSN": host_dsn,
                "REALTIME_V2_ROLE_PREFIX": roles.prefix,
                "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
                "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
                "REALTIME_V3_ACQUISITION_SPLIT": "ENABLED",
                "REALTIME_V3_OPTIONS_ACQUISITION_BINDING_PATH": str(
                    binding_path
                ),
                "REALTIME_V3_OPTIONS_ACQUISITION_STATUS_PATH": str(
                    output / "options-acquisition-status.json"
                ),
                "REALTIME_V3_OPTIONS_DOWNSTREAM_BINDING_DIR": str(
                    downstream
                ),
                "REALTIME_V2_ADVANCED_JOURNAL_DIR": str(journal_root),
                "REALTIME_V2_P8_STATUS_DIR": str(status_dir),
                "REALTIME_V2_ADVANCED_COHORT_ID": seed["cohort_id"],
                "REALTIME_V2_ADVANCED_SESSION_DATE": (
                    helper_args.session_date.isoformat()
                ),
                "REALTIME_V3_DIAGNOSTIC_AUTHORIZATION_ID": str(
                    diagnostic.authorization_id
                ),
            }
            processes, handles = _processes(
                output,
                environment,
                package_path,
                bundle_path,
                seed,
                diagnostic_path,
            )
            qualification._attach_qualification_downstream_bindings(
                downstream,
                bundle,
                seed,
                helper_args,
                equity_source_sha256=(
                    manifest["journals"]["equity"]["file_sha256"]
                ),
                cycle_cutoffs=cutoffs,
                options_activity_sha256s=[
                    row["file_sha256"]
                    for row in manifest["cycles"]["cycles"]
                ],
            )
            deadline = time.monotonic() + 600
            last_error = None
            while time.monotonic() < deadline:
                failed = {
                    name: process.returncode
                    for name, process in processes.items()
                    if process.poll() not in (None, 0)
                }
                if failed:
                    raise RuntimeError(
                        f"post-close resident process failed: {failed}"
                    )
                try:
                    durable = qualification._durable_evidence(
                        admin_pool,
                        seed,
                    )
                    break
                except RuntimeError as error:
                    last_error = error
                    time.sleep(1)
            if durable is None:
                raise RuntimeError(
                    "post-close resident barrier did not complete"
                ) from last_error
            connection = admin_pool.getconn()
            try:
                canonical_receipts = _seed_canonical_receipts(
                    connection,
                    seed,
                    manifest,
                )
            finally:
                admin_pool.putconn(connection)
            canonical_deadline = time.monotonic() + 120
            canonical_batches = 0
            while time.monotonic() < canonical_deadline:
                process = processes["CANONICAL_STOCK_ALERTS"]
                if process.poll() not in (None, 0):
                    raise RuntimeError(
                        "canonical Stock Alerts process failed"
                    )
                connection = admin_pool.getconn()
                try:
                    with connection:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                "SELECT COUNT(*) FROM "
                                "rt.canonical_stock_alert_batches "
                                "WHERE cohort_id=%s",
                                (seed["cohort_id"],),
                            )
                            canonical_batches = int(
                                cursor.fetchone()[0]
                            )
                finally:
                    admin_pool.putconn(connection)
                if canonical_batches == len(seed["window_ids"]):
                    break
                time.sleep(1)
            if canonical_batches != len(seed["window_ids"]):
                raise RuntimeError(
                    "canonical Stock Alerts did not persist every cycle"
                )
            window_status_path = status_dir / "window.json"
            before_restart = json.loads(
                window_status_path.read_text(encoding="utf-8")
            )
            window_process = processes["WINDOW"]
            if window_process.poll() is None:
                window_process.terminate()
                window_process.wait(timeout=15)
            process, opened = _start_process(
                "WINDOW",
                _role_command(
                    __import__(
                        "realtime.advanced_options_service",
                        fromlist=["AdvancedOptionsProcessKind"],
                    ).AdvancedOptionsProcessKind.WINDOW,
                    package_path,
                    bundle_path,
                ),
                output,
                _safe_environment(environment),
                suffix="-restart",
            )
            processes["WINDOW"] = process
            handles.extend(opened)
            restart_deadline = time.monotonic() + 90
            after_restart = None
            while time.monotonic() < restart_deadline:
                if window_status_path.is_file():
                    candidate = json.loads(
                        window_status_path.read_text(encoding="utf-8")
                    )
                    qualification_evidence = (
                        candidate.get("role_evidence") or {}
                    ).get("qualification") or {}
                    if qualification_evidence.get("rehydrated") is True:
                        after_restart = candidate
                        break
                if process.poll() not in (None, 0):
                    raise RuntimeError(
                        "restarted WINDOW process failed"
                    )
                time.sleep(0.5)
            if after_restart is None:
                raise RuntimeError(
                    "post-close WINDOW restart did not rehydrate"
                )
            restart_evidence = {
                "before_state": before_restart.get("state"),
                "after_state": after_restart.get("state"),
                "rehydrated": True,
                "same_bundle": (
                    before_restart.get("bundle_sha256")
                    == after_restart.get("bundle_sha256")
                ),
            }
            _stop_processes(processes, handles)
            processes = {}
            handles = []
            connection = admin_pool.getconn()
            try:
                with connection.cursor() as cursor:
                    warmup = build_warmup_report(
                        cursor,
                        {
                            "database": database,
                            "read_only": False,
                            "isolation": "READ COMMITTED",
                            "checked_at": datetime.now(UTC),
                        },
                        cohort_id=UUID(history["history_cohort_id"]),
                        through_session=datetime.fromisoformat(
                            history["history_valid_through"]
                        ).date(),
                    )
                    cursor.execute(
                        "SELECT COUNT(*) FROM rt.window_runs "
                        "WHERE plan_id=%s AND state='COMPLETE'",
                        (seed["plan_id"],),
                    )
                    completed_windows = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT COUNT(*) FROM "
                        "rt.advanced_options_resident_completions "
                        "WHERE target_cohort_id=%s AND status='COMPLETE'",
                        (seed["cohort_id"],),
                    )
                    resident_completions = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT COUNT(*) FROM rt.stock_alert_result_instances"
                    )
                    stock_alert_instances = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT COUNT(*) FROM "
                        "rt.canonical_stock_alert_batches "
                        "WHERE cohort_id=%s",
                        (seed["cohort_id"],),
                    )
                    canonical_batches = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT COUNT(*) FROM "
                        "rt.canonical_stock_alert_evaluations AS evaluation "
                        "JOIN rt.canonical_stock_alert_batches AS batch "
                        "USING(batch_id) WHERE batch.cohort_id=%s",
                        (seed["cohort_id"],),
                    )
                    canonical_evaluations = int(cursor.fetchone()[0])
                connection.rollback()
                rollback = _rollback_probe(connection, seed, package)
                readers = {
                    "warmup_verdict": warmup["verdict"],
                    "completed_windows": completed_windows,
                    "resident_completions": resident_completions,
                    "stock_alert_result_instances": stock_alert_instances,
                    "canonical_batches": canonical_batches,
                    "canonical_evaluations": canonical_evaluations,
                    "canonical_input_receipts": canonical_receipts,
                    "queryable": True,
                }
            finally:
                admin_pool.putconn(connection)
            worker_pool.closeall()
            worker_pool = None
    finally:
        if processes:
            _stop_processes(processes, handles)
        if worker_pool is not None:
            worker_pool.closeall()
        if login_owned:
            qualification._drop_login(settings, login)
        if roles_owned:
            with closing(_connect(settings, "postgres")) as connection:
                drop_disposable_roles_v2(connection, roles)
                connection.commit()
    with closing(_connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)",
                (database,),
            )
            database_exists = bool(cursor.fetchone()[0])
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname LIKE %s",
                (roles.prefix + "%",),
            )
            remaining_roles = [row[0] for row in cursor.fetchall()]
    passed = (
        durable is not None
        and len(durable["windows"]) == 45
        and len(durable["residents"]) == 135
        and restart_evidence is not None
        and restart_evidence["rehydrated"] is True
        and restart_evidence["same_bundle"] is True
        and readers is not None
        and readers["warmup_verdict"] == "PASS"
        and readers["completed_windows"] == 45
        and readers["resident_completions"] == 135
        and readers["canonical_input_receipts"] == 45
        and readers["canonical_batches"] == 45
        and readers["canonical_evaluations"] == 405
        and equity_ingestion["committed_offset"] == equity_cutoff
        and equity_ingestion["persisted_bars"] > 0
        and len(history_binding["security_continuity"]) == 9
        and rollback is not None
        and rollback["status"] == "PASS"
        and not database_exists
        and not remaining_roles
    )
    report = {
        "schema": "advanced-realtime-v3-post-close-validation-v1",
        "status": "PASS" if passed else "FAIL",
        "classification": "FAILED_SEGMENT_TECHNICAL_VALIDATION",
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "source": {
            "replay_manifest_semantic_sha256": manifest["semantic_sha256"],
            "history_export_semantic_sha256": (
                export_report["semantic_sha256"]
            ),
            "options_reference_semantic_sha256": supplement["semantic_sha256"],
            "equity_journal_file_sha256": _sha256(args.equity_journal),
            "options_journal_file_sha256": _sha256(args.options_journal),
            "completed_cycles": 45,
            "equity_replay_cutoff": (
                manifest["journals"]["equity"]["replay_cutoff"]
            ),
            "options_replay_cutoff": (
                manifest["journals"]["options"]["replay_cutoff"]
            ),
            "options_payload_bytes_replayed": option_bytes,
            "options_stream_session_id": str(option_stream_session_id),
        },
        "history": {
            "loaded_relations": loaded,
            "history_binding": history_binding,
        },
        "equity_ingestion": equity_ingestion,
        "options_acquisition": acquisition_report,
        "durable_database_barrier": durable,
        "readers": readers,
        "rollback": rollback,
        "restart_rehydration": restart_evidence,
        "cleanup": {
            "database_exists": database_exists,
            "remaining_roles": remaining_roles,
            "owned_processes": [],
        },
        "limitations": [
            "The provider-free resident boundary validates 45 durable cycle "
            "bindings and stage behavior; it is not a production publication.",
        ],
        "effects": {
            "provider_requests": 0,
            "provider_connections": 0,
            "production_database_reads": 0,
            "production_database_writes": 0,
            "disposable_database_writes": 1,
            "docker_actions": 0,
            "production_service_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
    }
    report["semantic_sha256"] = canonical_sha256(report)
    _write(output / "report.json", report)
    return report


def main(argv=None):
    args = parse_args(argv)
    report = execute(args)
    print(json.dumps({
        "status": report["status"],
        "completed_cycles": report["source"]["completed_cycles"],
        "completed_windows": (
            None
            if report["readers"] is None
            else report["readers"]["completed_windows"]
        ),
        "resident_completions": (
            None
            if report["readers"] is None
            else report["readers"]["resident_completions"]
        ),
        "rollback": report["rollback"],
        "restart_rehydration": report["restart_rehydration"],
        "cleanup": report["cleanup"],
        "semantic_sha256": report["semantic_sha256"],
        "output": str(args.output_directory / "report.json"),
    }, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
