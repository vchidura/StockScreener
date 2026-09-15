"""Locate unreadable evidence arrays without modifying the database."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from dotenv import load_dotenv
from psycopg2 import sql

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor


def probe(cursor, query, parameters=()):
    cursor.execute("SAVEPOINT storage_probe")
    try:
        cursor.execute(query, parameters)
        result = [dict(row) for row in cursor.fetchall()]
        error = None
    except Exception as failure:
        cursor.execute("ROLLBACK TO SAVEPOINT storage_probe")
        result = None
        error = dict(type=type(failure).__name__, sqlstate=getattr(failure, "pgcode", None), detail=str(failure).strip()[:500])
    cursor.execute("RELEASE SAVEPOINT storage_probe")
    return result, error


def audit(output_path):
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='60s'")
        cursor.execute("SET LOCAL jit=off")
        cursor.execute("SET LOCAL max_parallel_workers_per_gather=0")
        cursor.execute("""SELECT version(),pg_relation_size('equity_evidence') AS bytes,
            current_setting('block_size')::int AS block_size,current_setting('data_checksums') AS checksums,
            current_setting('transaction_read_only') AS read_only,transaction_timestamp() AS cutoff,
            pg_relation_filepath('equity_evidence') AS relation_path""")
        database = dict(cursor.fetchone())
        cursor.execute("SELECT count(*) AS records FROM equity_evidence")
        count = cursor.fetchone()["records"]
        pending = [(0, database["bytes"] // database["block_size"])]
        pages, probes = [], 0
        while pending:
            start, end = pending.pop()
            _, error = probe(cursor, """WITH selected AS MATERIALIZED (
                SELECT quality_codes FROM equity_evidence WHERE ctid >= %s::tid AND ctid < %s::tid
                ) SELECT sum(octet_length(quality_codes::text)) AS decoded_bytes FROM selected""",
                (f"({start},0)", f"({end},0)"))
            probes += 1
            if error is None:
                continue
            if error["sqlstate"] == "57014":
                raise RuntimeError("storage audit query timed out; do not classify a timeout as row damage")
            if end - start == 1:
                pages.append(start)
                print(f"Unreadable quality-code page: {start}", flush=True)
                if len(pages) > 32:
                    raise RuntimeError("storage audit exceeds 32 affected pages; stop for database recovery review")
            else:
                middle = (start + end) // 2
                pending.extend([(middle, end), (start, middle)])
        cursor.execute("SELECT attname FROM pg_attribute WHERE attrelid='equity_evidence'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")
        columns = [row["attname"] for row in cursor.fetchall()]
        damaged = []
        for page in pages:
            cursor.execute("SELECT ctid::text AS location,evidence_id::text FROM equity_evidence WHERE ctid >= %s::tid AND ctid < %s::tid",
                           (f"({page},0)", f"({page + 1},0)"))
            subjects = [dict(row) for row in cursor.fetchall()]
            for subject in subjects:
                _, error = probe(cursor, "SELECT quality_codes::text AS value FROM equity_evidence WHERE ctid=%s::tid", (subject["location"],))
                if error is None:
                    continue
                subject["quality_error"] = error
                subject["fields"] = {}
                for column in columns:
                    result, field_error = probe(cursor, sql.SQL("SELECT {field}::text AS value FROM equity_evidence WHERE ctid=%s::tid").format(field=sql.Identifier(column)), (subject["location"],))
                    subject["fields"][column] = dict(error=field_error) if field_error else dict(value=result[0]["value"])
                damaged.append(subject)
        report = dict(audited_at=datetime.now(timezone.utc).isoformat(), database=database, records=count,
                      probes=probes, affected_pages=pages, affected_rows=damaged, database_mutated=False)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str, ensure_ascii=True) + "\n", encoding="utf-8")
    print(json.dumps(dict(output=str(output_path), records=count, affected_pages=pages,
                         affected_rows=[dict(evidence_id=row["evidence_id"], location=row["location"],
                                             unreadable_fields=[name for name, field in row["fields"].items() if "error" in field]) for row in damaged]), indent=2), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=BACKEND_DIR / "backups" / "equity_evidence_storage_audit.json")
    audit(parser.parse_args().output)


if __name__ == "__main__":
    main()