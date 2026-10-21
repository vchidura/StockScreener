#!/usr/bin/env python3
"""Select fresh activity-ranked contracts for one bounded P7 diagnostic."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p7_selection import (  # noqa: E402
    p7_transport_selection_payload,
    p7_transport_candidate_from_payload,
    select_p7_transport_contracts,
)


UTC = timezone.utc


def _timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "timestamp must include an explicit UTC offset"
        )
    return result.astimezone(UTC)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--candidates", type=Path, required=True)
    value.add_argument("--reviewed-at", type=_timestamp, required=True)
    value.add_argument("--output", type=Path, required=True)
    return value


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    candidates_path = args.candidates.resolve()
    output = args.output.resolve()
    payload = json.loads(candidates_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise TypeError("P7 transport candidates must be a JSON array")
    candidates = tuple(
        p7_transport_candidate_from_payload(row)
        for row in payload
    )
    selection = select_p7_transport_contracts(
        candidates,
        reviewed_at=args.reviewed_at,
    )
    if output.exists():
        raise FileExistsError("P7 transport selection output must be new")
    if not output.parent.is_dir():
        raise FileNotFoundError(
            "P7 transport selection output parent is absent"
        )
    result = p7_transport_selection_payload(selection)
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": selection.status.value,
        "contracts": list(selection.contracts),
        "valid_until": (
            None
            if selection.valid_until is None
            else selection.valid_until.isoformat()
        ),
        "selection_sha256": selection.selection_sha256,
        "provider_requests": 0,
        "external_socket_connections": 0,
        "database_connections": 0,
    }, indent=2, sort_keys=True))
    return 0 if selection.status.value == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
