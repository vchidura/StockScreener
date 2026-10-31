#!/usr/bin/env python3
"""Derive disposable-only exact-four catalog and prior OI from a sealed cycle."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_sha256  # noqa: E402


UTC = timezone.utc


def _read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return value


def _write(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _snapshot_at(value: dict) -> datetime:
    quote = value.get("last_quote")
    if not isinstance(quote, dict):
        raise ValueError("sealed option snapshot has no quote")
    nanoseconds = int(quote["last_updated"])
    return datetime.fromtimestamp(
        nanoseconds / 1_000_000_000,
        tz=UTC,
    )


def build_supplement(
    cycle: dict,
    history: dict,
    manifest: dict,
) -> dict:
    semantic = cycle.get("semantic_sha256")
    if (
        cycle.get("schema")
        != "advanced-realtime-v3-host-segment-cycle-v1"
        or cycle.get("index") != 1
        or semantic != canonical_sha256({
            key: value
            for key, value in cycle.items()
            if key != "semantic_sha256"
        })
        or history.get("warmup", {}).get("verdict") != "PASS"
        or manifest.get("status")
        != "READY_FOR_READ_ONLY_HISTORY_EXPORT"
    ):
        raise ValueError("sealed options reference inputs are invalid")
    contracts = tuple(cycle.get("contracts", ()))
    snapshots = cycle.get("selected_snapshots")
    members = {
        str(row["ticker"]): str(row["security_id"])
        for row in history.get("members", ())
    }
    if (
        len(contracts) != 4
        or not isinstance(snapshots, dict)
        or set(snapshots) != set(contracts)
        or not {"QQQ", "SPY"}.issubset(members)
    ):
        raise ValueError("sealed options reference membership is invalid")
    session_date = date.fromisoformat(str(manifest["session_date"]))
    settlement_session = date.fromisoformat(
        str(manifest["history_valid_through"])
    )
    completed_at = datetime.fromisoformat(
        str(cycle["completed_at_utc"]).replace("Z", "+00:00")
    ).astimezone(UTC)
    rows = []
    for index, ticker in enumerate(sorted(contracts), start=1):
        snapshot = snapshots[ticker]
        details = snapshot.get("details")
        underlying = snapshot.get("underlying_asset")
        if not isinstance(details, dict) or not isinstance(underlying, dict):
            raise ValueError("sealed option snapshot identity is incomplete")
        underlyer = str(underlying.get("ticker", "")).upper()
        call_put = {
            "call": "C",
            "put": "P",
        }.get(str(details.get("contract_type", "")).lower())
        expiration = date.fromisoformat(str(details["expiration_date"]))
        observed_at = _snapshot_at(snapshot)
        open_interest = int(snapshot["open_interest"])
        payload_sha256 = canonical_sha256(snapshot)
        if (
            underlyer not in {"QQQ", "SPY"}
            or call_put not in {"C", "P"}
            or expiration != session_date
            or open_interest < 0
            or observed_at > completed_at + timedelta(seconds=5)
        ):
            raise ValueError("sealed option snapshot values are invalid")
        recorded_at = max(observed_at, completed_at)
        contract_id = 8_300_000 + index
        rows.append({
            "contract": {
                "contract_id": contract_id,
                "underlying_security_id": members[underlyer],
                "provider": "massive",
                "contract_ticker": ticker,
                "first_observed_at": observed_at.isoformat(),
                "source_origin": "SEALED_POST_CLOSE_REPLAY",
            },
            "version": {
                "version_id": 8_400_000 + index,
                "contract_id": contract_id,
                "call_put": call_put,
                "expiration": expiration.isoformat(),
                "strike": str(details["strike_price"]),
                "exercise_style": str(
                    details.get("exercise_style", "american")
                ).upper(),
                "multiplier": str(
                    details.get("shares_per_contract", 100)
                ),
                "deliverables": [],
                "eligibility": "VALIDATED_ACTIVE",
                "effective_from": observed_at.isoformat(),
                "effective_to": None,
                "observed_at": observed_at.isoformat(),
                "payload_sha256": payload_sha256,
                "imported_at": recorded_at.isoformat(),
            },
            "instrument": {
                "instrument_id": 8_500_000 + index,
                "asset_class": "OPTION",
                "contract_id": contract_id,
                "created_at": recorded_at.isoformat(),
            },
            "oi_baseline": {
                "baseline_id": str(uuid5(
                    NAMESPACE_URL,
                    f"post-close-oi:{ticker}:{settlement_session}",
                )),
                "contract_id": contract_id,
                "settlement_session": settlement_session.isoformat(),
                "open_interest": open_interest,
                "source": "massive_sealed_snapshot",
                "date_basis": "SAME_DAY_PRIOR_OPEN_INTEREST",
                "observed_session": session_date.isoformat(),
                "observed_at": observed_at.isoformat(),
                "received_at": recorded_at.isoformat(),
                "payload_sha256": payload_sha256,
                "supersedes_baseline_id": None,
            },
            "source_snapshot_sha256": payload_sha256,
        })
    result = {
        "schema": "advanced-options-v3-post-close-reference-v1",
        "status": "PASS",
        "classification": "SEALED_ARTIFACT_DISPOSABLE_ONLY",
        "session_date": session_date.isoformat(),
        "history_valid_through": settlement_session.isoformat(),
        "replay_manifest_semantic_sha256": manifest["semantic_sha256"],
        "history_source_semantic_sha256": history["semantic_sha256"],
        "cycle_semantic_sha256": semantic,
        "contract_count": len(rows),
        "oi_baseline_count": len(rows),
        "rows": rows,
        "effects": {
            "provider_requests": 0,
            "production_database_reads": 0,
            "production_database_writes": 0,
            "disposable_database_writes": 0,
        },
    }
    result["semantic_sha256"] = canonical_sha256(result)
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycle", type=Path, required=True)
    parser.add_argument("--history-source", type=Path, required=True)
    parser.add_argument("--replay-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    for name in (
        "cycle",
        "history_source",
        "replay_manifest",
        "output",
    ):
        setattr(args, name, getattr(args, name).resolve())
    if (
        not args.cycle.is_file()
        or not args.history_source.is_file()
        or not args.replay_manifest.is_file()
        or args.output.exists()
        or not args.output.parent.is_dir()
    ):
        parser.error("post-close options reference scope is invalid")
    return args


def main(argv=None):
    args = parse_args(argv)
    result = build_supplement(
        _read(args.cycle),
        _read(args.history_source),
        _read(args.replay_manifest),
    )
    _write(args.output, result)
    print(json.dumps({
        "status": result["status"],
        "contract_count": result["contract_count"],
        "oi_baseline_count": result["oi_baseline_count"],
        "semantic_sha256": result["semantic_sha256"],
        "file_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        "output": str(args.output),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
