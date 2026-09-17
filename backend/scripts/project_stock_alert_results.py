"""Project independent forward results into PostgreSQL; never alter source ledgers."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from threading import Event

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
    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")
    from database import get_db_cursor, get_db_connection
    from equity.stock_alert_results import capture_result_source, persist_capture, record_stream_error, load_shared_results
    from research.stock_alert_results import namespace_snapshot
    from research.stock_idea_engine import digest
    if args.check_storage:
        import psycopg2
        with get_db_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout='10s'")
                    for table in ("stock_alert_result_instances", "stock_alert_result_records", "stock_alert_result_revisions"):
                        cursor.execute("SAVEPOINT immutable_check")
                        try:
                            cursor.execute("UPDATE " + table + " SET instance_id=instance_id WHERE ctid=(SELECT ctid FROM " + table + " LIMIT 1)")
                        except psycopg2.errors.RaiseException:
                            cursor.execute("ROLLBACK TO SAVEPOINT immutable_check")
                        else:
                            raise ValueError("immutable result table allowed an update")
                    cursor.execute("SELECT stream,metadata FROM stock_alert_result_streams ORDER BY stream")
                    retained = cursor.fetchall()
                    cursor.execute("SAVEPOINT failure_check")
                    cursor.execute("UPDATE stock_alert_result_streams SET error='CHECK_ONLY' WHERE stream='swing'")
                    cursor.execute("SELECT stream,metadata FROM stock_alert_result_streams ORDER BY stream")
                    if cursor.fetchall() != retained:
                        raise ValueError("failure status removed retained result metadata")
                    cursor.execute("ROLLBACK TO SAVEPOINT failure_check")
                print(json.dumps(dict(status="STORAGE_GUARDS_VERIFIED", immutable_tables=3, failure_metadata_preserved=True,
                    mutations_committed=False)), flush=True)
            finally:
                connection.rollback()
        return
    directories = dict(intraday=Path(os.getenv("STOCK_ALERT_SHADOW_VIEW") or BACKEND / "backups/equity-shadow/stock-ideas-forward-v2/alerts-view.json").parent,
        swing=BACKEND / "backups/equity-shadow/stock-ideas-swing-v1")
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext('stock-alert-results-projector-v1'))")
            if not cursor.fetchone()[0]:
                raise ValueError("results projector already running")
        connection.commit()
        try:
            while True:
                now, reports, captures = datetime.now(timezone.utc), [], []
                for stream, directory in directories.items():
                    try:
                        captured = capture_result_source(directory, stream, now)
                        reports.append(persist_capture(captured, now))
                        captures.append(captured)
                    except Exception as error:
                        record_stream_error(stream, now, type(error).__name__)
                        reports.append(dict(stream=stream, status="IMPORT_FAILED", error=type(error).__name__, detail=str(error) if isinstance(error, ValueError) else None))
                if args.verify:
                    if len(captures) != len(directories):
                        raise ValueError("not all result streams imported")
                    before = load_shared_results(now)
                    for capture in captures:
                        persist_capture(capture, now)
                    after = load_shared_results(now)
                    for field in ("alerts", "publications", "observations"):
                        if digest(before[field]) != digest(after[field]):
                            raise ValueError("repeat import changed shared results")
                    reconciled = {}
                    for field in ("alerts", "publications", "observations"):
                        expected = [row for capture in captures for row in namespace_snapshot(capture["instance"], capture["snapshot"])[field]]
                        actual = {digest(row) for row in after[field]}
                        if any(digest(row) not in actual for row in expected):
                            raise ValueError("source and shared " + field + " differ")
                        reconciled[field] = len(expected)
                    reports.append(dict(status="RECONCILED", counts=reconciled, repeat_import_unchanged=True, source_writes=False))
                    if args.publish_reader:
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
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(hashtext('stock-alert-results-projector-v1'))")
            connection.commit()


if __name__ == "__main__":
    main()