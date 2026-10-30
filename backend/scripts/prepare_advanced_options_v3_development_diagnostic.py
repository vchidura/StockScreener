#!/usr/bin/env python3
"""Bind fixture E and a passed live Options journal into an inert plan."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_options_development_fixture import (  # noqa: E402
    validate_development_fixture_e,
)
from realtime.advanced_options_development_plan import (  # noqa: E402
    build_development_diagnostic_plan,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture-e", type=Path, required=True)
    parser.add_argument("--fixture-e-semantic-sha256", required=True)
    parser.add_argument("--fixture-e-file-sha256", required=True)
    parser.add_argument("--live-options-report", type=Path, required=True)
    parser.add_argument("--live-options-report-semantic-sha256", required=True)
    parser.add_argument("--live-options-report-file-sha256", required=True)
    parser.add_argument("--live-options-journal", type=Path, required=True)
    parser.add_argument("--live-options-journal-file-sha256", required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args(argv)
    args.fixture_e = args.fixture_e.resolve()
    args.live_options_report = args.live_options_report.resolve()
    args.live_options_journal = args.live_options_journal.resolve()
    args.output_directory = args.output_directory.resolve()
    if (
        not args.fixture_e.is_file()
        or not args.live_options_report.is_file()
        or not args.live_options_journal.is_file()
        or args.output_directory.exists()
    ):
        parser.error("development diagnostic inputs or output changed")
    return args


def _load(path, semantic_sha256, file_sha256, *, fixture=False):
    body = path.read_bytes()
    value = json.loads(body)
    if (
        hashlib.sha256(body).hexdigest() != file_sha256
        or value.get("semantic_sha256") != semantic_sha256
    ):
        raise ValueError(f"{path.name} identity changed")
    if fixture:
        validate_development_fixture_e(
            value,
            environment={
                "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
                "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
            },
        )
    return value


def main(argv=None) -> int:
    args = parse_args(argv)
    fixture = _load(
        args.fixture_e,
        args.fixture_e_semantic_sha256,
        args.fixture_e_file_sha256,
        fixture=True,
    )
    live_report = _load(
        args.live_options_report,
        args.live_options_report_semantic_sha256,
        args.live_options_report_file_sha256,
    )
    journal_body = args.live_options_journal.read_bytes()
    if (
        hashlib.sha256(journal_body).hexdigest()
        != args.live_options_journal_file_sha256
    ):
        raise ValueError("live Options journal identity changed")
    args.output_directory.mkdir()
    bound_journal = (
        args.output_directory / "bound-live-options-journal.sqlite"
    )
    shutil.copyfile(args.live_options_journal, bound_journal)
    bound_sha256 = hashlib.sha256(bound_journal.read_bytes()).hexdigest()
    if bound_sha256 != args.live_options_journal_file_sha256:
        raise RuntimeError("bound live Options journal copy changed")
    environment = {
        **os.environ,
        "REALTIME_V3_VALIDATION_MODE": "DEVELOPMENT_DIAGNOSTIC",
        "REALTIME_V3_PROVIDER_FREE_QUALIFICATION": "ENABLED",
    }
    plan = build_development_diagnostic_plan(
        fixture_e=fixture,
        fixture_e_file_sha256=args.fixture_e_file_sha256,
        live_options_report=live_report,
        live_options_report_file_sha256=(
            args.live_options_report_file_sha256
        ),
        bound_journal_path=bound_journal,
        bound_journal_sha256=bound_sha256,
        authorization_reference=args.authorization_reference,
        environment=environment,
    )
    plan_path = args.output_directory / "PLAN.json"
    encoded = (
        json.dumps(plan, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with plan_path.open("xb") as stream:
        stream.write(encoded)
    print(json.dumps({
        "status": "PREPARED_DEVELOPMENT_DIAGNOSTIC_PLAN",
        "plan_sha256": plan["plan_sha256"],
        "semantic_sha256": plan["semantic_sha256"],
        "file_sha256": hashlib.sha256(encoded).hexdigest(),
        "plan": str(plan_path),
        "bound_journal": str(bound_journal),
        "database_writes": 0,
        "service_actions": 0,
        "provider_requests": 0,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
