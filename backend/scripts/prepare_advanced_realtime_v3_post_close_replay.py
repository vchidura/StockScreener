#!/usr/bin/env python3
"""Seal failed-segment evidence for read-only post-close disposable replay."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.journal import RawJournalReader  # noqa: E402
from realtime.normalizer import NORMALIZER_VERSION  # noqa: E402


UTC = timezone.utc
ZERO_EFFECTS = {
    "database_writes",
    "docker_actions",
    "service_actions",
    "publication",
    "pointer_changes",
    "read_pin_changes",
    "dispatch",
    "delivery",
    "broker",
    "trading",
}


def _read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return value


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _terminal(
    value: dict,
    *,
    session_date: date,
    expected_cycles: int,
) -> dict:
    effects = value.get("production_effects")
    durable = value.get("durable_evidence")
    closure = value.get("closure")
    if (
        value.get("schema")
        != "advanced-realtime-v3-host-market-segment-terminal-failure-v1"
        or value.get("status") != "FAILED_CLOSED"
        or not isinstance(effects, dict)
        or any(effects.get(name) != 0 for name in ZERO_EFFECTS)
        or not isinstance(durable, dict)
        or durable.get("cycle_file_count") != expected_cycles
        or durable.get("summary_exists") is not False
        or not isinstance(closure, dict)
        or closure.get("owned_process_count") != 0
        or closure.get("retry_authorized") is not False
        or closure.get("restart_authorized") is not False
        or closure.get("post_close_replay_eligible_cycles")
        != expected_cycles
    ):
        raise ValueError("terminal failure receipt is not replay eligible")
    last_boundary = datetime.fromisoformat(
        str(value["last_completed_boundary_utc"]).replace("Z", "+00:00")
    ).astimezone(UTC)
    failed_boundary = datetime.fromisoformat(
        str(value["failed_boundary_utc"]).replace("Z", "+00:00")
    ).astimezone(UTC)
    observed_at = datetime.fromisoformat(
        str(value["observed_at_utc"]).replace("Z", "+00:00")
    ).astimezone(UTC)
    if (
        last_boundary.date() != session_date
        or failed_boundary != last_boundary + timedelta(minutes=5)
        or observed_at < failed_boundary
    ):
        raise ValueError("terminal failure boundaries are invalid")
    return {
        "status": value["status"],
        "observed_at_utc": observed_at.isoformat(),
        "last_completed_boundary_utc": last_boundary.isoformat(),
        "failed_boundary_utc": failed_boundary.isoformat(),
        "completed_cycles": expected_cycles,
        "blockers": value.get("blockers"),
        "durable_evidence": durable,
    }


def _cycles(
    directory: Path,
    *,
    session_date: date,
    expected_cycles: int,
) -> dict:
    paths = tuple(sorted(directory.glob("cycle-*.json")))
    if len(paths) != expected_cycles:
        raise ValueError("sealed cycle file count changed")
    rows = []
    prior_boundary = None
    contracts = None
    total_decoded_bytes = 0
    for expected_index, path in enumerate(paths, start=1):
        value = _read(path)
        semantic_sha256 = value.get("semantic_sha256")
        semantic_payload = {
            key: item
            for key, item in value.items()
            if key != "semantic_sha256"
        }
        boundary = datetime.fromisoformat(
            str(value["boundary_utc"]).replace("Z", "+00:00")
        ).astimezone(UTC)
        current_contracts = tuple(value.get("contracts", ()))
        if (
            path.name != f"cycle-{expected_index:03d}.json"
            or value.get("schema")
            != "advanced-realtime-v3-host-segment-cycle-v1"
            or value.get("index") != expected_index
            or value.get("request_count") != 1
            or boundary.date() != session_date
            or (
                prior_boundary is not None
                and boundary != prior_boundary + timedelta(minutes=5)
            )
            or len(current_contracts) != 4
            or (
                contracts is not None
                and current_contracts != contracts
            )
            or semantic_sha256 != canonical_sha256(semantic_payload)
        ):
            raise ValueError(f"sealed cycle is invalid: {path}")
        contracts = current_contracts
        prior_boundary = boundary
        total_decoded_bytes += int(value["decoded_bytes"])
        rows.append({
            "index": expected_index,
            "boundary_utc": boundary.isoformat(),
            "file": str(path),
            "file_sha256": _file_sha256(path),
            "semantic_sha256": semantic_sha256,
            "decoded_bytes": int(value["decoded_bytes"]),
        })
    return {
        "count": len(rows),
        "first_boundary_utc": rows[0]["boundary_utc"],
        "last_boundary_utc": rows[-1]["boundary_utc"],
        "contracts": list(contracts or ()),
        "total_decoded_bytes": total_decoded_bytes,
        "cycles_sha256": canonical_sha256(rows),
        "cycles": rows,
    }


def _journal(
    path: Path,
    expected: dict,
    prefix: str,
    *,
    last_completed_boundary: datetime,
    observed_at: datetime,
) -> dict:
    reader = RawJournalReader(path)
    recovery = reader.recovery_state(NORMALIZER_VERSION)
    expected_raw = int(expected[f"{prefix}_raw_offset"])
    expected_event = int(expected[f"{prefix}_event_offset"])
    actual = {
        "integrity_check": recovery.integrity_check,
        "quarantine_count": recovery.quarantine_count,
        "raw_committed_offset": recovery.raw_committed_offset,
        "normalized_committed_raw_offset": (
            recovery.normalized_committed_raw_offset
        ),
        "normalized_committed_event_offset": (
            recovery.normalized_committed_event_offset
        ),
    }
    if (
        actual["integrity_check"] != "ok"
        or actual["quarantine_count"] != 0
        or actual["raw_committed_offset"] < expected_raw
        or actual["normalized_committed_raw_offset"]
        != actual["raw_committed_offset"]
        or actual["normalized_committed_event_offset"] < expected_event
    ):
        raise ValueError(
            f"sealed {prefix} journal identity changed: expected "
            f"raw/event {expected_raw}/{expected_event}, actual {actual}"
        )
    raw_tail = reader.read_after(expected_raw, limit=10_000)
    event_tail = reader.read_normalized_after(expected_event, limit=10_000)
    if (
        len(raw_tail) != actual["raw_committed_offset"] - expected_raw
        or len(event_tail)
        != actual["normalized_committed_event_offset"] - expected_event
        or any(
            not last_completed_boundary
            <= row.received_at
            <= observed_at
            for row in raw_tail
        )
        or any(
            not last_completed_boundary
            <= row.received_at
            <= observed_at
            for row in event_tail
        )
    ):
        raise ValueError(f"sealed {prefix} journal tail is not bounded")
    tail = {
        "excluded_from_completed_cycle_replay": bool(raw_tail or event_tail),
        "raw_count": len(raw_tail),
        "event_count": len(event_tail),
        "first_received_at_utc": (
            None if not raw_tail else raw_tail[0].received_at.isoformat()
        ),
        "last_received_at_utc": (
            None if not raw_tail else raw_tail[-1].received_at.isoformat()
        ),
        "raw_payload_sha256": canonical_sha256(tuple(
            (row.raw_offset, row.payload_sha256)
            for row in raw_tail
        )),
        "normalized_events_sha256": canonical_sha256(tuple(
            (
                row.event_offset,
                row.raw_offset,
                row.provider_symbol,
                row.event_kind.value,
                row.payload_sha256,
            )
            for row in event_tail
        )),
    }
    return {
        "path": str(path),
        "file_sha256": _file_sha256(path),
        "file_bytes": path.stat().st_size,
        "journal_generation": str(recovery.generation),
        "schema_version": recovery.schema_version,
        "raw_committed_offset": recovery.raw_committed_offset,
        "normalized_committed_raw_offset": (
            recovery.normalized_committed_raw_offset
        ),
        "normalized_committed_event_offset": (
            recovery.normalized_committed_event_offset
        ),
        "quarantine_count": recovery.quarantine_count,
        "integrity_check": recovery.integrity_check,
        "replay_cutoff": {
            "raw_offset": expected_raw,
            "event_offset": expected_event,
        },
        "post_cycle_tail": tail,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--history-valid-through",
        type=date.fromisoformat,
        required=True,
    )
    parser.add_argument("--segment-state", type=Path, required=True)
    parser.add_argument("--terminal-failure", type=Path, required=True)
    parser.add_argument("--cycles-directory", type=Path, required=True)
    parser.add_argument("--equity-journal", type=Path, required=True)
    parser.add_argument("--options-journal", type=Path, required=True)
    parser.add_argument("--broader-chain-summary", type=Path, required=True)
    parser.add_argument("--expected-cycles", type=int, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in (
        "segment_state",
        "terminal_failure",
        "cycles_directory",
        "equity_journal",
        "options_journal",
        "broader_chain_summary",
        "output_directory",
    ):
        setattr(args, name, getattr(args, name).resolve())
    if (
        args.expected_cycles != 45
        or args.history_valid_through >= args.session_date
        or any(
            not getattr(args, name).is_file()
            for name in (
                "segment_state",
                "terminal_failure",
                "equity_journal",
                "options_journal",
                "broader_chain_summary",
            )
        )
        or not args.cycles_directory.is_dir()
        or args.output_directory.exists()
        or not args.output_directory.parent.is_dir()
        or not args.authorization_reference.strip()
        or not args.execute
    ):
        parser.error("post-close replay preparation scope is invalid")
    return args


def execute(args):
    output = args.output_directory
    output.mkdir()
    terminal_value = _read(args.terminal_failure)
    terminal = _terminal(
        terminal_value,
        session_date=args.session_date,
        expected_cycles=args.expected_cycles,
    )
    cycles = _cycles(
        args.cycles_directory,
        session_date=args.session_date,
        expected_cycles=args.expected_cycles,
    )
    if cycles["last_boundary_utc"] != terminal["last_completed_boundary_utc"]:
        raise ValueError("cycle tail does not match terminal failure receipt")
    durable = terminal["durable_evidence"]
    last_completed_boundary = datetime.fromisoformat(
        terminal["last_completed_boundary_utc"]
    ).astimezone(UTC)
    observed_at = datetime.fromisoformat(
        terminal["observed_at_utc"]
    ).astimezone(UTC)
    equity = _journal(
        args.equity_journal,
        durable,
        "equity",
        last_completed_boundary=last_completed_boundary,
        observed_at=observed_at,
    )
    options = _journal(
        args.options_journal,
        durable,
        "options",
        last_completed_boundary=last_completed_boundary,
        observed_at=observed_at,
    )
    broader = _read(args.broader_chain_summary)
    broader_effects = broader.get("effects")
    if (
        broader.get("schema")
        != "advanced-options-v3-broader-chain-capture-v1"
        or broader.get("status") != "PASS"
        or broader.get("session_date") != args.session_date.isoformat()
        or broader.get("provider_requests") != 2
        or not isinstance(broader_effects, dict)
        or any(
            broader_effects.get(name) != 0
            for name in (
                "database_reads",
                "database_writes",
                "docker_actions",
                "production_service_actions",
                "publication",
                "pointer_changes",
                "read_pin_changes",
            )
        )
    ):
        raise ValueError("broader-chain evidence is invalid")
    stale_state = _read(args.segment_state)
    manifest = {
        "schema": "advanced-realtime-v3-post-close-replay-manifest-v1",
        "status": "READY_FOR_READ_ONLY_HISTORY_EXPORT",
        "classification": "FAILED_SEGMENT_REPLAY_ONLY",
        "authorization_reference": args.authorization_reference.strip(),
        "session_date": args.session_date.isoformat(),
        "history_valid_through": args.history_valid_through.isoformat(),
        "historical_as_of_utc": cycles["first_boundary_utc"],
        "terminal_failure": {
            **terminal,
            "path": str(args.terminal_failure),
            "file_sha256": _file_sha256(args.terminal_failure),
        },
        "superseded_segment_state": {
            "path": str(args.segment_state),
            "file_sha256": _file_sha256(args.segment_state),
            "reported_status": stale_state.get("status"),
            "superseded_by_terminal_failure": True,
        },
        "cycles": cycles,
        "journals": {
            "equity": equity,
            "options": options,
        },
        "broader_chain": {
            "path": str(args.broader_chain_summary),
            "file_sha256": _file_sha256(args.broader_chain_summary),
            "semantic_sha256": broader["semantic_sha256"],
            "eligible_contracts": sum(
                int(row["eligible_row_count"])
                for row in broader["captures"]
            ),
        },
        "history_export": {
            "production_source_mode": "READ_ONLY_REPEATABLE_READ",
            "target": "OWNED_DISPOSABLE_V2_ONLY",
            "required_scope": [
                "security_reference_identities",
                "adjusted_daily_bars",
                "native_1m_5m_30m_bars",
                "derived_15m_30m_1h_1d_1wk_1mo_bars",
                "bar_lineage",
                "treasury_rates",
                "corporate_actions",
                "option_catalog",
                "prior_open_interest",
            ],
            "exact_row_counts_and_hashes_required": True,
        },
        "required_validation": [
            "equity_ingestion",
            "options_acquisition",
            "INGRESS",
            "MARKET_STATE",
            "FLOW",
            "WINDOW",
            "canonical_stock_alerts",
            "stored_readers",
            "disposable_publication_read_pin",
            "rollback",
            "restart_rehydration",
            "empty_cleanup",
        ],
        "effects": {
            "provider_requests": 0,
            "production_database_reads": 0,
            "production_database_writes": 0,
            "disposable_database_writes": 0,
            "docker_actions": 0,
            "service_actions": 0,
            "publication": 0,
            "pointer_changes": 0,
            "read_pin_changes": 0,
            "dispatch": 0,
            "delivery": 0,
            "broker": 0,
            "trading": 0,
        },
    }
    manifest["semantic_sha256"] = canonical_sha256(manifest)
    _write(output / "manifest.json", manifest)
    receipt = {
        "schema": "advanced-realtime-v3-post-close-replay-preparation-v1",
        "status": manifest["status"],
        "manifest": str(output / "manifest.json"),
        "manifest_file_sha256": _file_sha256(output / "manifest.json"),
        "manifest_semantic_sha256": manifest["semantic_sha256"],
        "completed_cycles": cycles["count"],
        "equity_offsets": [
            equity["raw_committed_offset"],
            equity["normalized_committed_event_offset"],
        ],
        "options_offsets": [
            options["raw_committed_offset"],
            options["normalized_committed_event_offset"],
        ],
        "effects": manifest["effects"],
    }
    _write(output / "receipt.json", receipt)
    return receipt


def main(argv=None):
    args = parse_args(argv)
    receipt = execute(args)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
