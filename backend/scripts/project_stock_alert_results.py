"""Project independent forward results into PostgreSQL; never alter source ledgers."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from threading import Event
from uuid import UUID

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--publish-reader", action="store_true")
    parser.add_argument("--check-storage", action="store_true", help="Read reconciliation and test immutable-table guards in rolled-back transactions")
    args = parser.parse_args()
    if args.check_storage and (args.publish_reader or args.continuous or args.verify):
        parser.error("Storage checks are separate from imports and cutover")
    if args.publish_reader and (not args.verify or args.continuous):
        parser.error("Reader cutover requires a successful one-shot verification")
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
    from realtime.database_access_v2 import (
        APP_ROLE,
        V2AccessConfig,
        V2ConnectionProvider,
    )
    from realtime.equity_pipeline_config import (
        RealtimeV2PipelineConfig,
        RealtimeV2Process,
    )
    from realtime.migrations import EXPECTED_V2_DATABASE_NAME

    config = RealtimeV2PipelineConfig.from_mapping(
        os.environ,
        process=RealtimeV2Process.APP_WORKER,
    )
    pool = ThreadedConnectionPool(1, 2, dsn=config.app_dsn)
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
            statement_timeout_ms=30_000,
        ),
    )
    provider.verify()

    @contextmanager
    def get_db_cursor():
        with provider.transaction(
            APP_ROLE,
            dict_cursor=True,
        ) as unit:
            unit.cursor.execute(
                "SET LOCAL search_path TO rt,ref,pg_catalog"
            )
            yield unit.cursor

    from equity.stock_alert_results import (
        _load_shared_results_cursor,
        capture_result_source,
        persist_stock_alert_input_binding_cursor,
        persist_capture_cursor,
        record_stream_error,
    )
    from realtime.runner_environment import StockAlertProjectionRequest
    from research.stock_alert_results import (
        combine_snapshots,
        namespace_snapshot,
    )
    from research.stock_idea_engine import digest
    from research.stock_idea_replay import utc
    if args.check_storage:
        class RollbackStorageCheck(Exception):
            pass

        try:
            with provider.transaction(
                APP_ROLE,
                dict_cursor=True,
            ) as unit:
                cursor = unit.cursor
                for table in (
                    "stock_alert_result_instances",
                    "stock_alert_result_records",
                    "stock_alert_result_revisions",
                ):
                    cursor.execute("SAVEPOINT immutable_check")
                    try:
                        cursor.execute(
                            "UPDATE "
                            + table
                            + " SET instance_id=instance_id "
                            "WHERE ctid=(SELECT ctid FROM "
                            + table
                            + " LIMIT 1)"
                        )
                    except psycopg2.errors.RaiseException:
                        cursor.execute(
                            "ROLLBACK TO SAVEPOINT immutable_check"
                        )
                    else:
                        raise ValueError(
                            "immutable result table allowed an update"
                        )
                cursor.execute(
                    "SELECT stream,metadata "
                    "FROM stock_alert_result_streams "
                    "ORDER BY stream"
                )
                retained = cursor.fetchall()
                cursor.execute("SAVEPOINT failure_check")
                cursor.execute(
                    "UPDATE stock_alert_result_streams "
                    "SET error='CHECK_ONLY' WHERE stream='swing'"
                )
                cursor.execute(
                    "SELECT stream,metadata "
                    "FROM stock_alert_result_streams "
                    "ORDER BY stream"
                )
                if cursor.fetchall() != retained:
                    raise ValueError(
                        "failure status removed retained result metadata"
                    )
                cursor.execute(
                    "ROLLBACK TO SAVEPOINT failure_check"
                )
                raise RollbackStorageCheck
        except RollbackStorageCheck:
            print(json.dumps(dict(
                status="STORAGE_GUARDS_VERIFIED",
                immutable_tables=3,
                failure_metadata_preserved=True,
                mutations_committed=False,
            )), flush=True)
        finally:
            pool.closeall()
        return
    directories = dict(intraday=Path(os.getenv("STOCK_ALERT_SHADOW_VIEW") or BACKEND / "backups/equity-shadow/stock-ideas-forward-v2/alerts-view.json").parent,
        swing=BACKEND / "backups/equity-shadow/stock-ideas-swing-v1")
    try:
        while True:
            now, reports, captures = datetime.now(timezone.utc), [], []
            for stream, directory in directories.items():
                try:
                    captured = capture_result_source(directory, stream)
                    captures.append(captured)
                except Exception as error:
                    record_stream_error(
                        stream,
                        datetime.now(timezone.utc),
                        type(error).__name__,
                        cursor_factory=get_db_cursor,
                    )
                    reports.append(dict(stream=stream, status="IMPORT_FAILED", error=type(error).__name__, detail=str(error) if isinstance(error, ValueError) else None))
            if len(captures) != len(directories):
                message = (
                    "not all result streams captured; "
                    "no retained stream import was committed"
                )
                if not args.continuous:
                    raise ValueError(message)
                from research.stock_idea_forward import publish_view
                status = dict(
                    checked_at=now.isoformat(),
                    streams=reports,
                    source_writes=False,
                    status="WAITING_FOR_V3_RESULT_SOURCES",
                )
                publish_view(
                    BACKEND
                    / "backups/stock-alert-results/projector-status.json",
                    status,
                )
                print(json.dumps(status, indent=2), flush=True)
                Event().wait(60)
                continue
            now = datetime.now(timezone.utc)
            with provider.transaction(
                APP_ROLE,
                isolation_level="REPEATABLE READ",
                dict_cursor=True,
            ) as unit:
                cursor = unit.cursor
                cursor.execute(
                    "SET LOCAL search_path TO rt,ref,pg_catalog"
                )
                cursor.execute(
                    "SELECT pg_try_advisory_xact_lock("
                    "hashtext('stock-alert-results-projector-v1')) "
                    "AS acquired"
                )
                if not cursor.fetchone()["acquired"]:
                    raise ValueError(
                        "results projector already running"
                    )
                for capture in captures:
                    reports.append(persist_capture_cursor(
                        cursor,
                        capture,
                        utc(capture["captured_at"]),
                    ))
                now = datetime.now(timezone.utc)
                declarations = tuple(
                    capture["snapshot"].get("v3_input_binding")
                    for capture in captures
                )
                if any(value is not None for value in declarations):
                    if (
                        len(declarations) != 2
                        or declarations[0] is None
                        or declarations[0] != declarations[1]
                    ):
                        raise ValueError(
                            "Stock Alert streams declare different V3 inputs"
                        )
                    declaration = declarations[0]
                    if not isinstance(declaration, dict):
                        raise TypeError(
                            "Stock Alert V3 input declaration must be an object"
                        )
                    request = StockAlertProjectionRequest(
                        cohort_id=UUID(str(declaration["cohort_id"])),
                        window_id=UUID(str(declaration["window_id"])),
                        input_receipt_id=UUID(
                            str(declaration["input_receipt_id"])
                        ),
                        cutoff=utc(declaration["cutoff"]),
                        input_sha256=str(declaration["input_sha256"]),
                        known_by=now,
                    )
                    reports.append(
                        {
                            "status": "INPUT_READY_BOUND",
                            **persist_stock_alert_input_binding_cursor(
                                cursor,
                                binding_id=UUID(
                                    str(declaration["binding_id"])
                                ),
                                request=request,
                                detector_input_sha256=str(
                                    declaration["detector_input_sha256"]
                                ),
                            ),
                        }
                    )
                snapshots, streams, _, _ = (
                    _load_shared_results_cursor(cursor, now)
                )
                first = combine_snapshots(snapshots, streams)
                if args.verify:
                    for capture in captures:
                        persist_capture_cursor(cursor, capture, now)
                    now = datetime.now(timezone.utc)
                    snapshots, streams, _, _ = (
                        _load_shared_results_cursor(cursor, now)
                    )
                    after = combine_snapshots(snapshots, streams)
                    for field in (
                        "alerts",
                        "publications",
                        "observations",
                    ):
                        if digest(first[field]) != digest(after[field]):
                            raise ValueError(
                                "repeat import changed shared results"
                            )
                    reconciled = {}
                    for field in (
                        "alerts",
                        "publications",
                        "observations",
                    ):
                        expected = [
                            row
                            for capture in captures
                            for row in namespace_snapshot(
                                capture["instance"],
                                capture["snapshot"],
                            )[field]
                        ]
                        actual = {
                            digest(row)
                            for row in after[field]
                        }
                        if any(
                            digest(row) not in actual
                            for row in expected
                        ):
                            raise ValueError(
                                "source and shared " + field + " differ"
                            )
                        reconciled[field] = len(expected)
                    reports.append(dict(
                        status="RECONCILED",
                        counts=reconciled,
                        repeat_import_unchanged=True,
                        source_writes=False,
                    ))
            if args.verify and args.publish_reader:
                from research.stock_idea_forward import publish_view
                publish_view(BACKEND / "backups/stock-alert-results/reader.json", dict(version="stock_alert_results_v1", activated_at=now.isoformat()))
                reports.append(dict(status="COMBINED_READER_ACTIVATED"))
            from research.stock_idea_forward import publish_view
            status = dict(checked_at=now.isoformat(), streams=reports, source_writes=False)
            publish_view(BACKEND / "backups/stock-alert-results/projector-status.json", status)
            print(json.dumps(status, indent=2), flush=True)
            if not args.continuous:
                if any(report["status"] == "IMPORT_FAILED" for report in reports):
                    raise ValueError("result projection incomplete; sources preserved")
                return
            Event().wait(60)
    finally:
        pool.closeall()


if __name__ == "__main__":
    main()
