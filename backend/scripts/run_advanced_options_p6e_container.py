#!/usr/bin/env python3
"""Run the provider-free P6E source/fault/process suite inside one bounded container."""
from __future__ import annotations

import argparse
import contextlib
import io
import importlib.util
import json
import os
import re
import sys
import time
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.realtime_script_support import write_report  # noqa: E402


PROHIBITED_ENV = (
    "POLYGON_API_KEY",
    "MASSIVE_API_KEY",
    "DATABASE_URL",
    "REALTIME_V2_APP_DSN",
    "REALTIME_V2_API_DSN",
)
TESTS = (
    "tests/realtime/test_advanced_gateway.py",
    "tests/realtime/test_buffer.py",
    "tests/realtime/test_advanced_options_ingress.py",
    "tests/realtime/test_advanced_options_p6e_process_sharding.py",
)


def validate_scope(manifest):
    if (
        manifest.get("contract_version")
        != "advanced-options-p6e-docker-v1"
        or manifest.get("provider_requests_authorized") != 0
        or manifest.get("publication_authorized") is not False
        or manifest.get("archive_retention_authorized") is not False
        or manifest.get("execution_authorized") is not False
        or manifest.get("delivery_authorized") is not False
        or tuple(manifest.get("tests", ())) != TESTS
    ):
        raise ValueError("P6E container scope changed")
    present = tuple(name for name in PROHIBITED_ENV if os.getenv(name))
    if present:
        raise RuntimeError(
            "unowned provider/database environment is present: "
            + ",".join(present)
        )


def main(argv=None) -> int:
    import resource

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    validate_scope(manifest)
    guard_path = BACKEND_DIR / "tests" / "realtime" / "offline_io_guard.py"
    spec = importlib.util.spec_from_file_location(
        "p6e_offline_io_guard",
        guard_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load P6E offline I/O guard")
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    class _GuardConfig:
        @staticmethod
        def getoption(name):
            if name != "--realtime-no-network":
                raise ValueError(f"unexpected guard option: {name}")
            return False

    guard.pytest_configure(_GuardConfig())
    import pytest

    capture = io.StringIO()
    started = time.perf_counter()
    with contextlib.redirect_stdout(capture), contextlib.redirect_stderr(capture):
        code = pytest.main([
            *TESTS,
            "-q",
            "--tb=short",
            "-p",
            "no:cacheprovider",
        ])
    elapsed = time.perf_counter() - started
    output = capture.getvalue()
    (args.output / "pytest.log").write_text(output, encoding="utf-8")
    match = re.search(
        r"(?P<passed>\d+) passed(?:, (?P<skipped>\d+) skipped)?",
        output,
    )
    report = {
        "status": "PASS_PROVIDER_FREE" if code == 0 else "FAIL",
        "contract_version": manifest["contract_version"],
        "run_id": manifest["run_id"],
        "tests": list(TESTS),
        "pytest_exit_code": int(code),
        "passed": 0 if match is None else int(match.group("passed")),
        "skipped": (
            0
            if match is None or match.group("skipped") is None
            else int(match.group("skipped"))
        ),
        "elapsed_seconds": elapsed,
        "process_peak_rss_bytes": (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        ),
        "provider_requests": 0,
        "publication_performed": False,
        "archive_retention_performed": False,
        "execution_performed": False,
        "delivery_performed": False,
        "external_network_authorized": False,
    }
    write_report(args.output / "container.json", report)
    print(json.dumps({
        "status": report["status"],
        "passed": report["passed"],
        "seconds": report["elapsed_seconds"],
    }, sort_keys=True))
    return int(code)


if __name__ == "__main__":
    raise SystemExit(main())
