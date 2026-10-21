#!/usr/bin/env python3
"""Provider-free PLAN, preflight and status for one inert exact P8 canary package."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_p8_canary_authorization import (  # noqa: E402
    p8_canary_plan,
    p8_canary_status,
    provider_free_p8_canary_preflight,
    validate_p8_canary_authorization,
    validate_p8_live_observation,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--qualification-report", type=Path, required=True)
    parser.add_argument(
        "--expected-qualification-semantic-sha256",
        required=True,
    )
    parser.add_argument(
        "--expected-qualification-file-sha256",
        required=True,
    )
    parser.add_argument("--expected-package-sha256", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--prior-run-records", type=Path)
    parser.add_argument("--live-observation", type=Path)
    parser.add_argument("--checked-at")
    parser.add_argument("--output", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--status", action="store_true")
    return parser.parse_args(argv)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write_output(path: Path | None, payload: object) -> None:
    if path is None:
        return
    target = path.resolve()
    if target.exists():
        raise FileExistsError("P8 audit output must be new")
    if not target.parent.is_dir():
        raise FileNotFoundError("P8 audit output parent is absent")
    with target.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def main(argv=None) -> int:
    args = parse_args(argv)
    package = _json(args.package.resolve())
    validate_p8_canary_authorization(
        package,
        qualification_report_path=args.qualification_report.resolve(),
        repository_root=REPOSITORY_ROOT,
        current_source_commit=args.source_commit,
        expected_package_sha256=args.expected_package_sha256,
        expected_qualification_semantic_sha256=(
            args.expected_qualification_semantic_sha256
        ),
        expected_qualification_file_sha256=(
            args.expected_qualification_file_sha256
        ),
    )
    if not any((args.preflight, args.status)):
        args.plan = True
    checked_at = (
        datetime.fromisoformat(args.checked_at.replace("Z", "+00:00"))
        if args.checked_at
        else datetime.now(UTC)
    )
    prior = (
        None
        if args.prior_run_records is None
        else _json(args.prior_run_records.resolve())
    )
    if args.plan:
        payload = p8_canary_plan(package)
    elif args.status:
        payload = p8_canary_status(
            package,
            now=checked_at,
            prior_run_records=prior,
        )
    elif args.live_observation is not None:
        payload = validate_p8_live_observation(
            package,
            _json(args.live_observation.resolve()),
            repository_root=REPOSITORY_ROOT,
            now=checked_at,
            prior_run_records=prior,
        )
    else:
        payload = provider_free_p8_canary_preflight(
            package,
            repository_root=REPOSITORY_ROOT,
            now=checked_at,
            prior_run_records=prior,
        )
    _write_output(args.output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
