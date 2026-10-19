#!/usr/bin/env python3
"""Build or validate a provider-free Advanced Options P7 shadow authorization."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p7_shadow import (  # noqa: E402
    approved_authorization,
    build_authorization,
    provider_free_preflight,
)


UTC = timezone.utc


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise argparse.ArgumentTypeError(
            "timestamp must be ISO-8601 with an explicit UTC offset"
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "timestamp must include an explicit UTC offset"
        )
    return parsed.astimezone(UTC)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    actions = root.add_subparsers(dest="action", required=True)
    plan = actions.add_parser("plan")
    plan.add_argument("--run-id", type=UUID, required=True)
    plan.add_argument("--authorization-reference", required=True)
    plan.add_argument("--baseline-head", required=True)
    plan.add_argument("--start-utc", type=_timestamp, required=True)
    plan.add_argument(
        "--controlled-reconnect-utc",
        type=_timestamp,
        required=True,
    )
    plan.add_argument("--stop-utc", type=_timestamp, required=True)
    plan.add_argument("--spy-contract", required=True)
    plan.add_argument("--qqq-contract", required=True)
    plan.add_argument(
        "--artifact-root",
        type=Path,
        default=BACKEND_DIR / "tests" / ".artifacts",
    )
    plan.add_argument("--manifest-path", type=Path)
    preflight = actions.add_parser("preflight")
    preflight.add_argument("--manifest", type=Path, required=True)
    preflight.add_argument("--approval", type=Path, required=True)
    return root


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    if args.action == "plan":
        result = build_authorization(
            run_id=args.run_id,
            authorization_reference=args.authorization_reference,
            baseline_head=args.baseline_head,
            start_at=args.start_utc,
            controlled_reconnect_at=args.controlled_reconnect_utc,
            stop_at=args.stop_utc,
            spy_contract=args.spy_contract,
            qqq_contract=args.qqq_contract,
            backend_dir=BACKEND_DIR,
            artifact_root=args.artifact_root,
        )
        if args.manifest_path is not None:
            manifest_path = args.manifest_path.resolve()
            if not manifest_path.parent.is_dir():
                raise FileNotFoundError(
                    "P7 manifest parent directory does not exist"
                )
            with manifest_path.open(
                "x",
                encoding="utf-8",
                newline="\n",
            ) as handle:
                handle.write(
                    json.dumps(result, indent=2, sort_keys=True) + "\n"
                )
    else:
        manifest = approved_authorization(
            args.manifest.resolve(),
            args.approval.resolve(),
            backend_dir=BACKEND_DIR,
        )
        result = provider_free_preflight(
            manifest,
            backend_dir=BACKEND_DIR,
            now=datetime.now(UTC),
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
