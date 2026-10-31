#!/usr/bin/env python3
"""Export and disposable-load the bounded V3 post-close history baseline."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from psycopg2 import sql


BACKEND_DIR = Path(__file__).resolve().parent.parent
TEST_SUPPORT = BACKEND_DIR / "tests" / "realtime"
for path in (BACKEND_DIR, TEST_SUPPORT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import APP_ROLE  # noqa: E402
from scripts import qualify_advanced_options_v3_docker as qualification  # noqa: E402
from scripts.audit_realtime_v2_target9_scanner_warmup import (  # noqa: E402
    build_report as build_warmup_report,
)
from scripts.inspect_realtime_v2_advanced_equity_membership import (  # noqa: E402
    _provider,
)
from scripts.validate_realtime_v2_schema import _connect  # noqa: E402
from v2_database import disposable_v2_database  # noqa: E402


UTC = timezone.utc


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


def _bar_queries():
    roots = (
        "WITH roots AS ("
        "SELECT current.*,revision.session_date "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "WHERE current.cohort_id=%s "
        "AND current.instrument_id=ANY(%s) "
        "AND revision.session_date<=%s"
        ") "
    )
    return {
        "rt.bar_catalog": (
            roots
            + "SELECT catalog.* FROM rt.bar_catalog AS catalog "
            "JOIN roots USING(bar_revision_id) "
            "ORDER BY catalog.bar_revision_id"
        ),
        "rt.bar_revisions": (
            roots
            + "SELECT revision.* FROM rt.bar_revisions AS revision "
            "JOIN roots ON roots.bar_revision_id=revision.bar_revision_id "
            "AND roots.observed_at=revision.observed_at "
            "ORDER BY revision.bar_revision_id,revision.observed_at"
        ),
        "rt.bar_current": (
            roots
            + "SELECT cohort_id,provider_tier,instrument_id,interval,"
            "bar_start,session_scope,adjustment_basis,source_kind,"
            "bar_revision_id,bar_end,observed_at,updated_at "
            "FROM roots ORDER BY instrument_id,interval,bar_start,"
            "session_scope,adjustment_basis"
        ),
    }


def _specs(history):
    security_ids = [
        row["security_id"] for row in history["members"]
    ]
    instrument_ids = [
        int(row["instrument_id"]) for row in history["members"]
    ]
    cohort_id = history["history_cohort_id"]
    history_through = history["history_valid_through"]
    as_of = history["as_of_utc"]
    binding = history["history_binding"]
    manifest_sha256 = binding["source_manifest_sha256"]
    common = {
        "security_ids": security_ids,
        "instrument_ids": instrument_ids,
        "cohort_id": cohort_id,
        "history_through": history_through,
        "as_of": as_of,
        "manifest_sha256": manifest_sha256,
    }
    specs = [
        (
            "ref.securities",
            "SELECT security.* FROM ref.securities AS security "
            "WHERE security.security_id=ANY(%s::uuid[]) "
            "ORDER BY security.security_id",
            (security_ids,),
        ),
        (
            "ref.security_revisions",
            "SELECT revision.* FROM ref.security_revisions AS revision "
            "WHERE revision.security_id=ANY(%s::uuid[]) "
            "AND revision.observed_at<=%s "
            "ORDER BY revision.observed_at,revision.revision_id",
            (security_ids, as_of),
        ),
        (
            "ref.corporate_action_revisions",
            "SELECT revision.* "
            "FROM ref.corporate_action_revisions AS revision "
            "WHERE revision.security_id=ANY(%s::uuid[]) "
            "AND revision.observed_at<=%s "
            "ORDER BY revision.observed_at,revision.action_id",
            (security_ids, as_of),
        ),
        (
            "ref.corporate_action_coverage",
            "SELECT coverage.* FROM ref.corporate_action_coverage AS coverage "
            "WHERE coverage.security_id=ANY(%s::uuid[]) "
            "AND coverage.observed_at<=%s "
            "ORDER BY coverage.observed_at,coverage.coverage_id",
            (security_ids, as_of),
        ),
        (
            "ref.risk_free_rate_observations",
            "SELECT rate.* FROM ref.risk_free_rate_observations AS rate "
            "WHERE rate.rate_date<=%s "
            "ORDER BY rate.rate_date,rate.tenor_days,rate.observation_id",
            (history_through,),
        ),
        (
            "rt.instruments",
            "SELECT instrument.* FROM rt.instruments AS instrument "
            "WHERE instrument.instrument_id=ANY(%s) "
            "ORDER BY instrument.instrument_id",
            (instrument_ids,),
        ),
        (
            "rt.capability_profiles",
            "SELECT profile.* FROM rt.capability_profiles AS profile "
            "JOIN rt.subscription_manifests AS manifest USING(profile_id) "
            "WHERE manifest.manifest_sha256=%s "
            "ORDER BY profile.profile_id",
            (manifest_sha256,),
        ),
        (
            "rt.subscription_manifests",
            "SELECT manifest.* FROM rt.subscription_manifests AS manifest "
            "WHERE manifest.manifest_sha256=%s "
            "ORDER BY manifest.manifest_id",
            (manifest_sha256,),
        ),
        (
            "rt.subscription_members",
            "SELECT member.* FROM rt.subscription_members AS member "
            "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
            "WHERE manifest.manifest_sha256=%s "
            "ORDER BY member.manifest_id,member.channel,"
            "member.instrument_id",
            (manifest_sha256,),
        ),
        (
            "rt.cohorts",
            "SELECT cohort.* FROM rt.cohorts AS cohort "
            "WHERE cohort.cohort_id=%s "
            "ORDER BY cohort.cohort_id",
            (cohort_id,),
        ),
    ]
    bar_parameters = (
        cohort_id,
        instrument_ids,
        history_through,
    )
    for relation, query in _bar_queries().items():
        specs.append((relation, query, bar_parameters))
    return common, specs


def _render(cursor, query, parameters):
    return cursor.mogrify(query, parameters).decode("utf-8")


def _export_relation(cursor, output: Path, relation, query, parameters):
    rendered = _render(cursor, query, parameters)
    cursor.execute(f"SELECT * FROM ({rendered}) AS exported LIMIT 0")
    columns = tuple(column.name for column in cursor.description)
    cursor.execute(f"SELECT COUNT(*) FROM ({rendered}) AS exported")
    row_count = int(cursor.fetchone()[0])
    path = output / f"{relation.replace('.', '-')}.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        cursor.copy_expert(
            f"COPY ({rendered}) TO STDOUT WITH (FORMAT CSV)",
            stream,
        )
        stream.flush()
        os.fsync(stream.fileno())
    return {
        "relation": relation,
        "columns": list(columns),
        "row_count": row_count,
        "file": str(path),
        "file_bytes": path.stat().st_size,
        "file_sha256": _sha256(path),
        "query": query,
        "parameters": list(parameters),
    }


def _load_relation(cursor, exported):
    schema, table = exported["relation"].split(".", 1)
    columns = sql.SQL(",").join(
        sql.Identifier(name) for name in exported["columns"]
    )
    statement = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV)"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        columns,
    )
    with Path(exported["file"]).open(
        "r",
        encoding="utf-8",
        newline="",
    ) as stream:
        cursor.copy_expert(statement.as_string(cursor), stream)


def _apply_options_supplement(cursor, supplement):
    for row in supplement["rows"]:
        contract = row["contract"]
        version = row["version"]
        instrument = row["instrument"]
        oi = row["oi_baseline"]
        cursor.execute(
            "INSERT INTO ref.option_contracts ("
            "contract_id,underlying_security_id,provider,contract_ticker,"
            "first_observed_at,source_origin"
            ") VALUES (%s,%s,%s,%s,%s,%s)",
            tuple(contract[name] for name in (
                "contract_id",
                "underlying_security_id",
                "provider",
                "contract_ticker",
                "first_observed_at",
                "source_origin",
            )),
        )
        cursor.execute(
            "INSERT INTO ref.option_contract_versions ("
            "version_id,contract_id,call_put,expiration,strike,"
            "exercise_style,multiplier,deliverables,eligibility,"
            "effective_from,effective_to,observed_at,payload_sha256,"
            "imported_at"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s)",
            (
                version["version_id"],
                version["contract_id"],
                version["call_put"],
                version["expiration"],
                version["strike"],
                version["exercise_style"],
                version["multiplier"],
                json.dumps(version["deliverables"]),
                version["eligibility"],
                version["effective_from"],
                version["effective_to"],
                version["observed_at"],
                version["payload_sha256"],
                version["imported_at"],
            ),
        )
        cursor.execute(
            "INSERT INTO rt.instruments ("
            "instrument_id,asset_class,contract_id,created_at"
            ") VALUES (%s,%s,%s,%s)",
            tuple(instrument[name] for name in (
                "instrument_id",
                "asset_class",
                "contract_id",
                "created_at",
            )),
        )
        cursor.execute(
            "INSERT INTO ref.oi_baselines ("
            "baseline_id,contract_id,settlement_session,open_interest,"
            "source,date_basis,observed_session,observed_at,received_at,"
            "payload_sha256,supersedes_baseline_id"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            tuple(oi[name] for name in (
                "baseline_id",
                "contract_id",
                "settlement_session",
                "open_interest",
                "source",
                "date_basis",
                "observed_session",
                "observed_at",
                "received_at",
                "payload_sha256",
                "supersedes_baseline_id",
            )),
        )
    cursor.execute(
        "SELECT setval('rt.instruments_instrument_id_seq',"
        "(SELECT max(instrument_id) FROM rt.instruments),true)"
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-source", type=Path, required=True)
    parser.add_argument("--options-reference", type=Path, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in ("history_source", "options_reference", "output_directory"):
        setattr(args, name, getattr(args, name).resolve())
    if (
        not args.history_source.is_file()
        or not args.options_reference.is_file()
        or args.output_directory.exists()
        or not args.output_directory.parent.is_dir()
        or not args.authorization_reference.strip()
        or not args.execute
    ):
        parser.error("post-close history export scope is invalid")
    return args


def execute(args):
    history = _read(args.history_source)
    supplement = _read(args.options_reference)
    if (
        history.get("status") != "PASS"
        or history.get("options_reference", {}).get("mode")
        != "SEALED_ARTIFACT_DISPOSABLE_ONLY"
        or supplement.get("status") != "PASS"
        or history["options_reference"]["supplement"]["semantic_sha256"]
        != supplement["semantic_sha256"]
    ):
        raise ValueError("post-close history export inputs are invalid")
    output = args.output_directory
    output.mkdir()
    common, specs = _specs(history)
    provider, source_pool = _provider()
    exports = []
    try:
        with provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            unit.cursor.execute("SET LOCAL statement_timeout = '120s'")
            for relation, query, parameters in specs:
                exports.append(_export_relation(
                    unit.cursor,
                    output,
                    relation,
                    query,
                    parameters,
                ))
    finally:
        source_pool.closeall()

    database = f"test_realtime_v2_v3post_{uuid4().hex[:12]}"
    parity = []
    cleanup_database_exists = None
    try:
        with disposable_v2_database(database) as admin_pool:
            connection = admin_pool.getconn()
            try:
                with connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT rt.ensure_partitions(%s,%s)",
                            ("rt.bar_revisions", history["as_of_utc"][:10]),
                        )
                        for exported in exports:
                            _load_relation(cursor, exported)
                        _apply_options_supplement(cursor, supplement)
                        cursor.execute(
                            "SELECT COUNT(*) FROM rt.bar_revisions AS child "
                            "CROSS JOIN LATERAL "
                            "UNNEST(child.source_revision_ids) AS source_id "
                            "LEFT JOIN rt.bar_catalog AS parent "
                            "ON parent.bar_revision_id=source_id "
                            "WHERE parent.bar_revision_id IS NULL"
                        )
                        missing_lineage = int(cursor.fetchone()[0])
                        cursor.execute(
                            "SELECT COUNT(*) FROM ref.option_contracts "
                            "WHERE contract_ticker=ANY(%s)",
                            ([
                                row["contract"]["contract_ticker"]
                                for row in supplement["rows"]
                            ],),
                        )
                        option_contract_count = int(cursor.fetchone()[0])
                        warmup = build_warmup_report(
                            cursor,
                            {
                                "database": database,
                                "read_only": False,
                                "isolation": "READ COMMITTED",
                                "checked_at": datetime.now(UTC),
                            },
                            cohort_id=__import__("uuid").UUID(
                                common["cohort_id"]
                            ),
                            through_session=datetime.fromisoformat(
                                common["history_through"]
                            ).date(),
                        )
                for exported in exports:
                    relation, query, parameters = next(
                        item for item in specs
                        if item[0] == exported["relation"]
                    )
                    with connection.cursor() as cursor:
                        rendered = _render(cursor, query, parameters)
                        temporary = output / (
                            f".verify-{relation.replace('.', '-')}.csv"
                        )
                        with temporary.open(
                            "w",
                            encoding="utf-8",
                            newline="",
                        ) as stream:
                            cursor.copy_expert(
                                f"COPY ({rendered}) TO STDOUT "
                                "WITH (FORMAT CSV)",
                                stream,
                            )
                        target_sha256 = _sha256(temporary)
                        temporary.unlink()
                    if target_sha256 != exported["file_sha256"]:
                        raise RuntimeError(
                            f"history export parity failed: {relation}"
                        )
                    parity.append({
                        "relation": relation,
                        "row_count": exported["row_count"],
                        "file_sha256": exported["file_sha256"],
                    })
            finally:
                admin_pool.putconn(connection)
        settings = qualification._settings()
        with closing(_connect(settings, "postgres")) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s)",
                    (database,),
                )
                cleanup_database_exists = bool(cursor.fetchone()[0])
    finally:
        if cleanup_database_exists is None:
            cleanup_database_exists = True

    passed = (
        warmup["verdict"] == "PASS"
        and missing_lineage == 0
        and option_contract_count == 4
        and cleanup_database_exists is False
    )
    report = {
        "schema": "advanced-realtime-v3-post-close-history-export-v1",
        "status": "PASS" if passed else "FAIL",
        "authorization_reference": args.authorization_reference.strip(),
        "source": {
            "database": history["database"],
            "transaction_read_only": True,
            "isolation": "REPEATABLE READ",
            "history_source_semantic_sha256": history["semantic_sha256"],
        },
        "target": {
            "database": database,
            "owned_disposable": True,
            "database_exists_after_cleanup": cleanup_database_exists,
        },
        "exports": parity,
        "relation_count": len(parity),
        "row_count": sum(row["row_count"] for row in parity),
        "warmup": warmup,
        "missing_lineage_rows": missing_lineage,
        "sealed_option_contract_count": option_contract_count,
        "options_reference_semantic_sha256": supplement["semantic_sha256"],
        "effects": {
            "provider_requests": 0,
            "production_database_reads": 1,
            "production_database_writes": 0,
            "disposable_database_writes": 1,
            "docker_actions": 0,
            "service_actions": 0,
            "publication": 0,
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
        "relation_count": report["relation_count"],
        "row_count": report["row_count"],
        "missing_lineage_rows": report["missing_lineage_rows"],
        "sealed_option_contract_count": (
            report["sealed_option_contract_count"]
        ),
        "database_exists_after_cleanup": (
            report["target"]["database_exists_after_cleanup"]
        ),
        "semantic_sha256": report["semantic_sha256"],
        "output": str(args.output_directory / "report.json"),
    }, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
