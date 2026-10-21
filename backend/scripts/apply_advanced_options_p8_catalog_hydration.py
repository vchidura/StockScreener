#!/usr/bin/env python3
"""Apply one exact, provider-free P8 option catalog hydration manifest."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import AssetType, OptionContractReference  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402
from realtime.option_pre_session_runtime import (  # noqa: E402
    _admitted_catalog_entry,
)
from realtime.relation_registry import APP_ROLE  # noqa: E402


SCHEMA = "advanced-options-p8-catalog-hydration-candidates-v1"
SOURCE_ORIGIN = "PRE_SESSION_DISCOVERY"


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--expected-manifest-sha256", required=True)
    parser.add_argument("--expected-file-sha256", required=True)
    parser.add_argument("--expected-contract-inserts", type=int, required=True)
    parser.add_argument("--expected-version-inserts", type=int, required=True)
    parser.add_argument("--expected-instrument-inserts", type=int, required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def load_manifest(
    path: Path,
    *,
    expected_manifest_sha256: str,
    expected_file_sha256: str,
) -> dict[str, object]:
    raw = path.resolve().read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_file_sha256:
        raise ValueError("P8 hydration manifest file identity changed")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise TypeError("P8 hydration manifest must be an object")
    unsigned = dict(value)
    manifest_sha256 = unsigned.pop("manifest_sha256", None)
    if (
        value.get("schema") != SCHEMA
        or manifest_sha256 != expected_manifest_sha256
        or _canonical_sha256(unsigned) != expected_manifest_sha256
        or value.get("provider_requests") != 2
        or value.get("pagination_requests") != 0
        or value.get("database_writes") != 0
        or value.get("session_date") != "2026-10-21"
    ):
        raise ValueError("P8 hydration manifest contract changed")
    responses = value.get("responses")
    if (
        not isinstance(responses, list)
        or [row.get("underlyer") for row in responses]
        != ["QQQ", "SPY"]
    ):
        raise ValueError("P8 hydration manifest scope changed")
    return value


def _entries(
    manifest: dict[str, object],
    *,
    expected_candidate_count: int = 298,
):
    captured_at = datetime.fromisoformat(str(manifest["captured_at_utc"]))
    entries = []
    for response in manifest["responses"]:
        for candidate in response["candidates"]:
            payload = candidate["provider_payload"]
            additional = payload.get("additional_underlyings") or []
            correction = payload.get("correction")
            reference = OptionContractReference(
                contract_ticker=str(candidate["ticker"]),
                underlyer=str(candidate["underlying_ticker"]),
                asset_type=AssetType.ETF,
                provider="massive",
                provider_version="v3-reference-options-contracts",
                provider_contract_type=str(candidate["contract_type"]),
                expiration_date=date.fromisoformat(
                    str(candidate["expiration_date"])
                ),
                strike=Decimal(str(candidate["strike_price"])),
                provider_exercise_style=str(candidate["exercise_style"]),
                shares_per_contract=int(candidate["shares_per_contract"]),
                primary_exchange=candidate.get("primary_exchange"),
                correction=None if correction is None else str(correction),
                additional_underlyings_json=json.dumps(
                    additional,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                adjustment_metadata_json=json.dumps(
                    {
                        key: payload[key]
                        for key in ("cfi", "correction")
                        if payload.get(key) is not None
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                changes_deliverables=bool(additional),
                valid_from=captured_at,
                valid_to=None,
                first_observed_at=captured_at,
                revised_observed_at=None,
                refreshed_at=captured_at,
                payload_sha256=str(candidate["provider_payload_sha256"]),
            )
            entry = _admitted_catalog_entry(reference)
            entries.append((
                entry,
                str(candidate["underlying_security_id"]),
            ))
    tickers = [entry.catalog.contract_ticker for entry, _ in entries]
    if (
        len(tickers) != expected_candidate_count
        or len(tickers) != len(set(tickers))
    ):
        raise ValueError("P8 hydration candidate membership changed")
    return tuple(entries)


def _current_rows(cursor, tickers):
    cursor.execute(
        "SELECT contract.contract_ticker,contract.contract_id,"
        "contract.underlying_security_id::text,contract.provider,"
        "version.version_id,version.call_put,version.expiration,"
        "version.strike,version.exercise_style,version.multiplier,"
        "version.eligibility,version.payload_sha256,"
        "instrument.instrument_id "
        "FROM ref.option_contracts AS contract "
        "LEFT JOIN ref.option_contract_versions AS version "
        "ON version.contract_id=contract.contract_id "
        "AND version.effective_to IS NULL "
        "LEFT JOIN rt.instruments AS instrument "
        "ON instrument.contract_id=contract.contract_id "
        "WHERE contract.contract_ticker=ANY(%s) "
        "ORDER BY contract.contract_ticker,version.observed_at DESC",
        (list(tickers),),
    )
    rows = defaultdict(list)
    for row in cursor.fetchall():
        rows[str(row["contract_ticker"])].append(row)
    return rows


def _diff(entries, rows):
    conflicts = []
    contract_inserts = 0
    version_inserts = 0
    instrument_inserts = 0
    for entry, security_id in entries:
        catalog = entry.catalog
        current = rows.get(catalog.contract_ticker, [])
        contract_ids = {
            int(row["contract_id"]) for row in current
        }
        if len(contract_ids) > 1 or any(
            str(row["underlying_security_id"]) != security_id
            or str(row["provider"]).lower() not in {"massive", "polygon"}
            for row in current
        ):
            conflicts.append(catalog.contract_ticker)
            continue
        if not current:
            contract_inserts += 1
        if not any(
            row["version_id"] is not None
            and str(row["call_put"]) == catalog.contract_type.value[0]
            and row["expiration"] == catalog.expiration_date
            and Decimal(row["strike"]) == catalog.strike
            and str(row["exercise_style"]) == catalog.exercise_style.value
            and Decimal(row["multiplier"])
            == Decimal(catalog.shares_per_contract)
            and str(row["eligibility"]) == "VALIDATED_ACTIVE"
            for row in current
        ):
            version_inserts += 1
        if not any(row["instrument_id"] is not None for row in current):
            instrument_inserts += 1
    return {
        "conflicts": sorted(set(conflicts)),
        "contract_inserts": contract_inserts,
        "version_inserts": version_inserts,
        "instrument_inserts": instrument_inserts,
    }


def apply_manifest_cursor(
    cursor,
    manifest: dict[str, object],
    *,
    expected_contract_inserts: int,
    expected_version_inserts: int,
    expected_instrument_inserts: int,
    expected_candidate_count: int = 298,
) -> dict[str, int]:
    entries = _entries(
        manifest,
        expected_candidate_count=expected_candidate_count,
    )
    tickers = tuple(
        entry.catalog.contract_ticker for entry, _ in entries
    )
    before = _diff(entries, _current_rows(cursor, tickers))
    expected = {
        "contract_inserts": expected_contract_inserts,
        "version_inserts": expected_version_inserts,
        "instrument_inserts": expected_instrument_inserts,
    }
    if before["conflicts"] or {
        key: before[key] for key in expected
    } != expected:
        raise ValueError(
            "P8 hydration production diff changed: "
            + ",".join(before["conflicts"])
        )

    applied = {
        "contract_inserts": 0,
        "version_inserts": 0,
        "instrument_inserts": 0,
    }
    for entry, security_id in entries:
        catalog = entry.catalog
        cursor.execute(
            "SELECT contract_id,underlying_security_id::text,provider "
            "FROM ref.option_contracts "
            "WHERE contract_ticker=%s "
            "AND provider IN ('massive','polygon') "
            "ORDER BY contract_id FOR SHARE",
            (catalog.contract_ticker,),
        )
        contracts = cursor.fetchall()
        if len(contracts) > 1:
            raise ValueError("P8 hydration contract identity is ambiguous")
        if contracts:
            contract_id = int(contracts[0]["contract_id"])
            if (
                str(contracts[0]["underlying_security_id"]) != security_id
                or str(contracts[0]["provider"]).lower()
                not in {"massive", "polygon"}
            ):
                raise ValueError("P8 hydration contract identity changed")
        else:
            contract_id = catalog.contract_id
            cursor.execute(
                "INSERT INTO ref.option_contracts ("
                "contract_id,underlying_security_id,provider,"
                "contract_ticker,first_observed_at,source_origin) "
                "VALUES (%s,%s,'massive',%s,%s,%s)",
                (
                    contract_id,
                    security_id,
                    catalog.contract_ticker,
                    catalog.first_observed_at,
                    SOURCE_ORIGIN,
                ),
            )
            applied["contract_inserts"] += 1

        cursor.execute(
            "SELECT version_id FROM ref.option_contract_versions "
            "WHERE contract_id=%s AND effective_to IS NULL "
            "AND call_put=%s AND expiration=%s AND strike=%s "
            "AND exercise_style=%s AND multiplier=%s "
            "AND eligibility='VALIDATED_ACTIVE'",
            (
                contract_id,
                catalog.contract_type.value[0],
                catalog.expiration_date,
                catalog.strike,
                catalog.exercise_style.value,
                catalog.shares_per_contract,
            ),
        )
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO ref.option_contract_versions ("
                "version_id,contract_id,call_put,expiration,strike,"
                "exercise_style,multiplier,deliverables,eligibility,"
                "effective_from,effective_to,observed_at,payload_sha256) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,'[]'::jsonb,"
                "'VALIDATED_ACTIVE',%s,NULL,%s,%s)",
                (
                    entry.catalog_version_id,
                    contract_id,
                    catalog.contract_type.value[0],
                    catalog.expiration_date,
                    catalog.strike,
                    catalog.exercise_style.value,
                    catalog.shares_per_contract,
                    catalog.valid_from,
                    catalog.revised_observed_at
                    or catalog.first_observed_at,
                    catalog.payload_sha256,
                ),
            )
            applied["version_inserts"] += 1

        cursor.execute(
            "SELECT instrument_id FROM rt.instruments "
            "WHERE contract_id=%s",
            (contract_id,),
        )
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO rt.instruments (asset_class,contract_id) "
                "VALUES ('OPTION',%s)",
                (contract_id,),
            )
            applied["instrument_inserts"] += 1

    after = _diff(entries, _current_rows(cursor, tickers))
    if (
        applied != expected
        or after["conflicts"]
        or any(after[key] != 0 for key in expected)
    ):
        raise RuntimeError("P8 hydration postcondition failed")
    return applied


def main(argv=None) -> int:
    args = _parse_args(argv)
    if args.confirm_database_name != EXPECTED_V2_DATABASE_NAME:
        raise ValueError("P8 hydration database confirmation changed")
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError("P8 hydration report output must be new")
    manifest = load_manifest(
        args.manifest,
        expected_manifest_sha256=args.expected_manifest_sha256,
        expected_file_sha256=args.expected_file_sha256,
    )
    if not args.apply:
        print(json.dumps({
            "status": "P8_CATALOG_HYDRATION_PLAN_VALID",
            "manifest_sha256": args.expected_manifest_sha256,
            "database_writes": 0,
            "provider_requests": 0,
        }, sort_keys=True))
        return 0

    pool = ThreadedConnectionPool(
        1,
        2,
        dsn=os.environ["REALTIME_V2_APP_DSN"],
        options="-c timezone=UTC",
    )
    try:
        provider = V2ConnectionProvider(
            {APP_ROLE: pool},
            V2AccessConfig(
                security_mode="hardened",
                role_prefix=EXPECTED_V2_DATABASE_NAME,
                statement_timeout_ms=300_000,
            ),
        )
        provider.verify()
        with provider.transaction(
            APP_ROLE,
            isolation_level="SERIALIZABLE",
            dict_cursor=True,
        ) as unit:
            applied = apply_manifest_cursor(
                unit.cursor,
                manifest,
                expected_contract_inserts=args.expected_contract_inserts,
                expected_version_inserts=args.expected_version_inserts,
                expected_instrument_inserts=args.expected_instrument_inserts,
            )
    finally:
        pool.closeall()

    report = {
        "schema": "advanced-options-p8-catalog-hydration-application-v1",
        "applied_at_utc": datetime.now().astimezone().isoformat(),
        "manifest_sha256": args.expected_manifest_sha256,
        "provider_requests": 0,
        "database_writes": sum(applied.values()),
        **applied,
    }
    report["report_sha256"] = _canonical_sha256(report)
    output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": "PASS_P8_CATALOG_HYDRATION_APPLIED",
        "output": str(output),
        **report,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
