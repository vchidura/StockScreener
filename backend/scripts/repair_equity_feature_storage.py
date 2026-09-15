"""Recover one audited PNC feature row from hash-matched canonical inputs."""
import argparse
from dataclasses import asdict
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from pathlib import Path
import sys
from uuid import UUID

from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import Json

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor
from equity.domain import EvidenceRole, EvidenceType, LifecycleStatus, QualityState
from equity.materialization import _evidence, _feature_payload, _direction_value, bars_to_frame
from equity.polygon import sha256_json
from equity.repositories import _bar_from_row, _security_from_row


EVIDENCE_ID = "92460ab7-aefd-59e3-906d-9cba058eb118"
RUN_ID = "b06ebac5-89b2-5315-a10c-52343cd4381a"


def json_value(value):
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def verify_reconstruction(recovered, intact_key):
    source_hash, payload_hash = intact_key.rsplit(":", 2)[-2:]
    if str(recovered.evidence_id) != EVIDENCE_ID or recovered.evidence_key != intact_key \
            or recovered.payload_sha256 != payload_hash \
            or sha256_json([str(identity) for identity in recovered.source_revision_ids]) != source_hash:
        raise ValueError("reconstruction does not reproduce the exact evidence ID, key, source hash and payload hash")


def repair(*, apply=False, directory):
    audit_path = directory / "equity_evidence_storage_audit.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    subjects = audit["affected_rows"]
    if len(subjects) != 1 or subjects[0]["evidence_id"] != EVIDENCE_ID or audit["affected_pages"] != [78960]:
        raise ValueError("repair is restricted to the single audited PNC feature row")
    intact_key = subjects[0]["fields"]["evidence_key"]["value"]
    with get_db_cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" + ("" if apply else ", READ ONLY"))
        cursor.execute("SET LOCAL statement_timeout='60s'")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        if apply:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext('equity-pnc-feature-storage-repair'))")
        cursor.execute("SELECT evidence_id::text,evidence_key,ctid::text AS location FROM equity_evidence WHERE evidence_id=%s::uuid" + (" FOR UPDATE" if apply else ""), (EVIDENCE_ID,))
        current = dict(cursor.fetchone())
        if current["evidence_key"] != intact_key or current["location"] != subjects[0]["location"]:
            raise ValueError("audited row changed or moved; refusing to overwrite it")
        cursor.execute("SELECT * FROM equity_analysis_runs WHERE analysis_run_id=%s::uuid", (RUN_ID,))
        run = dict(cursor.fetchone())
        cursor.execute("SELECT * FROM equity_analysis_members WHERE analysis_run_id=%s::uuid AND ticker='PNC'", (RUN_ID,))
        member = dict(cursor.fetchone())
        cursor.execute("""SELECT * FROM equity_security_reference_revisions WHERE ticker='PNC'
            AND effective_from<=%s AND observed_at<=%s ORDER BY effective_from DESC,observed_at DESC,created_at DESC LIMIT 1""",
            (run["market_time"], run["observed_at"]))
        security = _security_from_row(dict(cursor.fetchone()))
        cursor.execute("""WITH visible AS (
            SELECT DISTINCT ON(bar_start) * FROM equity_bar_revisions
            WHERE ticker='PNC' AND interval='5m' AND session_scope='RTH' AND adjusted=FALSE AND is_final
              AND bar_end<=%s AND COALESCE(replay_available_at,system_observed_at)<=%s AND created_at<=%s
            ORDER BY bar_start,CASE WHEN source_kind='RECONCILED' THEN 0 WHEN source_kind='NATIVE_REST' THEN 1
                WHEN source_kind='REALTIME_STREAM' THEN 2 WHEN source_kind='DERIVED' THEN 3 ELSE 4 END,
                CASE WHEN availability_mode='LIVE_OBSERVED' THEN 0 ELSE 1 END,
                COALESCE(replay_available_at,system_observed_at) DESC,created_at DESC
            ) SELECT * FROM visible ORDER BY bar_start DESC LIMIT %s""",
            (run["market_time"], run["observed_at"], run["completed_at"], member["source_bar_count"]))
        bars = tuple(reversed([_bar_from_row(dict(row)) for row in cursor.fetchall()]))
        if len(bars) != 400 or bars[-1].bar_revision_id != member["latest_bar_revision_id"] or security.security_id != member["security_id"]:
            raise ValueError("source window or security differs from original run membership")
        cursor.execute("""SELECT fundamental_report_id FROM equity_fundamental_reports WHERE security_id=%s
            AND availability_time<=%s AND observed_at<=%s
            ORDER BY availability_time DESC,observed_at DESC,period_end DESC LIMIT 8""",
            (security.security_id, run["market_time"], run["observed_at"]))
        reports = tuple(row["fundamental_report_id"] for row in cursor.fetchall())
        payload, direction, quality = _feature_payload(bars_to_frame(bars), {"free_float": security.free_float})
        recovered = _evidence(analysis_run_id=UUID(RUN_ID), security=security, interval="5m",
            evidence_type=EvidenceType.FEATURE_SNAPSHOT, evidence_role=EvidenceRole.REGIME,
            lifecycle_key="feature:PNC:5m", lifecycle_status=LifecycleStatus.SNAPSHOT,
            direction=_direction_value(direction), strength=None, market_time=run["market_time"], observed_at=run["observed_at"],
            source_name="EQUITY_FEATURES", source_version="equity_features_v1", latest_bar_revision_id=bars[-1].bar_revision_id,
            source_revision_ids=tuple(row.bar_revision_id for row in bars), fundamental_report_ids=reports,
            quality_state=QualityState.COMPLETE if not quality else QualityState.DEGRADED, quality_codes=quality, payload=payload)
        verify_reconstruction(recovered, intact_key)
        prepared = asdict(recovered)
        prepared["payload"] = json.loads(prepared.pop("payload_json"))
        prepared["source_window_sha256"] = None
        prepared["supersedes_evidence_id"] = None
        cursor.execute("SELECT transaction_timestamp() AS repaired_at")
        repaired_at = cursor.fetchone()["repaired_at"]
        prepared["created_at"] = repaired_at
        audit_record = dict(evidence_id=EVIDENCE_ID, original_location=current["location"], apply_requested=apply,
                            original_audit_sha256=hashlib.sha256(audit_path.read_bytes()).hexdigest(),
                            exact_identity_and_hashes_match=True, recovered=json_value(prepared), original_run=json_value(run),
                            created_at_policy="Original physical insertion timestamp unreadable; use actual repair transaction time, never backdate",
                            applied=False)
        directory.mkdir(parents=True, exist_ok=True)
        if apply:
            page_path = directory / "equity_evidence_page_78960_before_repair.bin"
            cursor.execute("SELECT pg_read_binary_file(pg_relation_filepath('equity_evidence'),%s,%s) AS page", (78960 * 8192, 8192))
            page = bytes(cursor.fetchone()["page"])
            if len(page) != 8192:
                raise ValueError("raw page backup length mismatch")
            with page_path.open("xb") as handle:
                handle.write(page)
            audit_record["raw_page_sha256"] = hashlib.sha256(page).hexdigest()
        record_path = directory / ("equity_feature_repair_applied.json" if apply else "equity_feature_repair_dry_run.json")
        with record_path.open("x", encoding="utf-8") as handle:
            json.dump(audit_record, handle, indent=2, allow_nan=False)
            handle.write("\n")
        if apply:
            values = []
            for key, value in prepared.items():
                values.append(Json(value) if key == "payload" else value.value if isinstance(value, Enum)
                              else list(value) if isinstance(value, tuple) else value)
            assignments = sql.SQL(",").join(sql.SQL("{}=%s").format(sql.Identifier(key)) for key in prepared)
            cursor.execute(sql.SQL("UPDATE equity_evidence SET {} WHERE evidence_id=%s::uuid AND evidence_key=%s AND ctid=%s::tid RETURNING evidence_id").format(assignments),
                           (*values, EVIDENCE_ID, intact_key, current["location"]))
            if len(cursor.fetchall()) != 1:
                raise ValueError("repair did not update exactly one audited row")
            cursor.execute("SELECT * FROM equity_evidence WHERE evidence_id=%s::uuid", (EVIDENCE_ID,))
            actual = dict(cursor.fetchone())
            for key, value in prepared.items():
                if json_value(actual[key]) != json_value(value):
                    raise ValueError(f"repaired field failed exact readback: {key}")
            cursor.execute("SELECT sum(octet_length(quality_codes::text)) AS total FROM equity_evidence")
            audit_record["post_repair_array_decode_bytes"] = cursor.fetchone()["total"]
            audit_record["applied"] = True
    if apply:
        record_path.write_text(json.dumps(audit_record, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(dict(evidence_id=EVIDENCE_ID, applied=apply, source_bars=len(bars),
                         payload_sha256=recovered.payload_sha256, source_hash=intact_key.rsplit(":", 2)[-2],
                         security_id=str(security.security_id), report=str(record_path)), indent=2), flush=True)
    return audit_record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--directory", type=Path, default=BACKEND_DIR / "backups")
    args = parser.parse_args()
    repair(apply=args.apply, directory=args.directory)


if __name__ == "__main__":
    main()