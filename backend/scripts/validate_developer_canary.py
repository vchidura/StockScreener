#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
ROOT = BACKEND_DIR.parent
REPORT_ROOT = BACKEND_DIR / "backups" / "realtime" / "developer-validation"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import DataEngine, load_option_runtime_configuration
from options.data.polygon_http import RequestsPolygonHttpTransport
from realtime.developer_validation import (
    DEVELOPER_CANARY_TICKERS,
    DeveloperCanaryPolicy,
    DeveloperCanaryValidator,
    assess_sanitized_developer_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the reviewed realtime canary under Polygon Developer."
    )
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("--execute", action="store_true")
    operations.add_argument("--check-config", action="store_true")
    operations.add_argument(
        "--assess-report",
        help="Assess one sanitized report filename under the current Developer policy.",
    )
    parser.add_argument(
        "--ticker",
        action="append",
        choices=DEVELOPER_CANARY_TICKERS,
        help="Restrict validation to reviewed canary symbols; repeat for multiple.",
    )
    return parser.parse_args()


def _configuration():
    load_dotenv(BACKEND_DIR / ".env", override=False)
    environ = dict(os.environ)
    environ.pop("OPTION_STOCK_BEHAVIOR_SHADOW_LAUNCH_FILE", None)
    environ.update(
        {
            "OPTION_DATA_ENGINE": "polygon_developer",
            "OPTION_FIXED_STOCK_UNDERLYERS": "AAPL,MSFT,NVDA,AMZN,TSLA",
            "OPTION_FIXED_ETF_UNDERLYERS": "SPY,QQQ,IWM",
            "OPTION_STOCK_UNIVERSE_SIZE": "5",
            "OPTION_ETF_UNIVERSE_SIZE": "3",
            "OPTION_START_READ_ONLY": "true",
            "OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED": "false",
        }
    )
    configuration = load_option_runtime_configuration(environ, BACKEND_DIR)
    if configuration.settings.data_engine is not DataEngine.POLYGON_DEVELOPER:
        raise RuntimeError("Developer canary requires polygon_developer")
    if not configuration.settings.start_read_only:
        raise RuntimeError("Developer canary requires read-only startup")
    if set(configuration.settings.underlyers) != set(DEVELOPER_CANARY_TICKERS):
        raise RuntimeError("Developer canary universe does not match the reviewed symbols")
    return configuration


def _write_report(payload: dict[str, object], checked_at: datetime) -> Path:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    destination = REPORT_ROOT / (
        "developer-canary-" + checked_at.strftime("%Y%m%dT%H%M%SZ") + ".json"
    )
    if destination.exists():
        raise RuntimeError("Developer canary report already exists")
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=REPORT_ROOT,
        prefix="developer-canary-",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, destination)
    return destination


def main() -> int:
    args = parse_args()
    selected = tuple(dict.fromkeys(args.ticker or DEVELOPER_CANARY_TICKERS))
    policy = DeveloperCanaryPolicy(
        tickers=selected,
        maximum_requests=len(selected) * 11,
    )
    if args.assess_report:
        report_name = Path(args.assess_report).name
        if report_name != args.assess_report or not report_name.startswith(
            "developer-canary-"
        ) or not report_name.endswith("Z.json"):
            raise ValueError("assess-report must be one Developer canary report filename")
        source = REPORT_ROOT / report_name
        if not source.is_file() or source.stat().st_size > 2 * 1024 * 1024:
            raise ValueError("Developer canary report is missing or exceeds the bound")
        raw = source.read_bytes()
        payload = json.loads(raw)
        assessment = assess_sanitized_developer_report(
            payload,
            source_report_sha256=hashlib.sha256(raw).hexdigest(),
            policy=policy,
        )
        checked_at = datetime.now(timezone.utc)
        assessment_payload = asdict(assessment)
        assessment_payload["assessed_at"] = checked_at.isoformat()
        destination = _write_assessment(
            assessment_payload,
            checked_at,
        )
        print(
            json.dumps(
                {
                    **assessment_payload,
                    "report": str(destination.relative_to(ROOT)).replace("\\", "/"),
                    "provider_requests": False,
                },
                indent=2,
            )
        )
        return 0 if assessment.status == "PORTABLE_READY" else 2
    if not args.execute and not args.check_config:
        print(
            json.dumps(
                {
                    "mode": "PLAN",
                    "tickers": policy.tickers,
                    "maximum_requests": policy.maximum_requests,
                    "snapshot_contracts_per_type": policy.snapshot_contracts_per_type,
                    "trade_contracts_per_type": policy.trade_contracts_per_type,
                    "o2_enabled": policy.o2_enabled,
                    "database_writes": False,
                    "raw_payloads_retained": False,
                },
                indent=2,
            )
        )
        print("No provider requests made. Add --execute after explicit authorization.")
        return 0
    configuration = _configuration()
    if args.check_config:
        print(
            json.dumps(
                {
                    "status": "CONFIGURATION_READY",
                    "configuration_sha256": configuration.configuration_sha256,
                    "tickers": configuration.settings.underlyers,
                    "data_engine": configuration.settings.data_engine.value,
                    "read_only": configuration.settings.start_read_only,
                    "provider_requests": False,
                },
                indent=2,
            )
        )
        return 0
    checked_at = datetime.now(timezone.utc)
    transport = RequestsPolygonHttpTransport(
        configuration.settings.polygon_api_key.get_secret_value(),
        timeout_seconds=configuration.policy.provider_requests.request_timeout_seconds,
    )
    report = DeveloperCanaryValidator(
        transport,
        configuration_sha256=configuration.configuration_sha256,
        policy=policy,
    ).validate(checked_at=checked_at)
    payload = asdict(report)
    payload["checked_at"] = report.checked_at.isoformat()
    payload["ticker_results"] = [
        {**asdict(row), "expiration": row.expiration.isoformat()}
        for row in report.ticker_results
    ]
    report_path = _write_report(payload, checked_at)
    print(
        json.dumps(
            {
                "status": report.status,
                "checked_at": report.checked_at.isoformat(),
                "tickers": [row.ticker for row in report.ticker_results],
                "request_count": report.request_count,
                "response_bytes": report.response_bytes,
                "advanced_unavailable": report.advanced_unavailable,
                "developer_disabled": report.developer_disabled,
                "report": str(report_path.relative_to(ROOT)).replace("\\", "/"),
                "database_writes": report.database_writes,
                "raw_payloads_retained": report.raw_payloads_retained,
            },
            indent=2,
        )
    )
    return 0 if report.status in {"PORTABLE_READY", "PORTABLE_DEGRADED"} else 2


def _write_assessment(payload: dict[str, object], checked_at: datetime) -> Path:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    destination = REPORT_ROOT / (
        "developer-assessment-" + checked_at.strftime("%Y%m%dT%H%M%SZ") + ".json"
    )
    if destination.exists():
        raise RuntimeError("Developer assessment report already exists")
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=REPORT_ROOT,
        prefix="developer-assessment-",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, destination)
    return destination


if __name__ == "__main__":
    raise SystemExit(main())