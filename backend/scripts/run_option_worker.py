#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import socket
import sys
from pathlib import Path
from uuid import uuid4


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import dotenv_values, load_dotenv

load_dotenv(BACKEND_DIR / ".env")

REVIEWED_OPTION_PINS = (
    "OPTION_STRATEGY_POLICY_FILE",
    "OPTION_TECHNICAL_FORWARD_LAUNCH_FILE",
    "OPTION_TECHNICAL_FORWARD_LAUNCH_SHA256",
)


def _load_reviewed_option_pins(path, environment=None):
    environment = os.environ if environment is None else environment
    pinned = dotenv_values(path)
    for key in REVIEWED_OPTION_PINS:
        value = pinned.get(key)
        if value is not None:
            environment[key] = value
    return environment


_load_reviewed_option_pins(BACKEND_DIR / ".env")

from options import build_option_startup_state, load_option_runtime_configuration
from options.startup import ensure_option_partitions
from options.data import build_data_engine
from options.orchestration import ManualOptionPipeline
from options.repositories import OptionSchedulerLeadership
from options.strategy_orchestration import OptionStrategyPipeline
from options.worker import OptionMaterializationWorker


def _detector_storage_ready(schema):
    return bool(
        schema.get("registered") and schema.get("runtime_select") and schema.get("runtime_insert")
        and schema.get("o1_table_present") and schema.get("o1_registered")
        and schema.get("o1_runtime_select") and schema.get("o1_runtime_insert")
        and schema.get("o3_table_present") and schema.get("o3_registered") and schema.get("o3_admission_registered")
        and schema.get("o3_runtime_select") and schema.get("o3_runtime_insert")
        and schema.get("stock_setup_table_present") and schema.get("stock_setup_registered")
        and schema.get("stock_setup_runtime_select") and schema.get("stock_setup_runtime_insert")
        and schema.get("unvalidated_constraints") == 0
        and len(schema.get("triggers", ())) == 3
        and all(row["enabled"] == "O" for row in schema["triggers"])
        and len(schema.get("o1_triggers", ())) == 3
        and all(row["enabled"] == "O" for row in schema["o1_triggers"])
        and len(schema.get("o3_triggers", ())) == 3
        and all(row["enabled"] == "O" for row in schema["o3_triggers"])
        and len(schema.get("stock_setup_triggers", ())) == 3
        and all(row["enabled"] == "O" for row in schema["stock_setup_triggers"])
        and len(schema.get("indexes", ())) == 4
        and all(row["valid"] and row["ready"] for row in schema["indexes"])
    )


def _detector_pipeline_arguments(configuration):
    path = os.getenv("OPTION_TECHNICAL_FORWARD_LAUNCH_FILE")
    expected = os.getenv("OPTION_TECHNICAL_FORWARD_LAUNCH_SHA256")
    if not path and not expected:
        return {}
    if not path or not expected:
        raise ValueError("technical forward launch requires both manifest path and reviewed checksum")
    from options.detector_launch import load_detector_forward_launch, build_detector_collector
    from options.repositories.alert_evaluations import OptionAlertEvaluationRepository
    from options.repositories.stock_behavior_assessments import OptionStockBehaviorAssessmentRepository
    from datetime import datetime, timezone

    launch = load_detector_forward_launch(path, expected_sha256=expected, configuration=configuration, backend_dir=BACKEND_DIR)
    schema = OptionAlertEvaluationRepository().schema_readiness()
    if not _detector_storage_ready(schema):
        raise ValueError("technical forward storage guards or permissions are not ready")
    sources = OptionStockBehaviorAssessmentRepository()
    cutoff = datetime.now(timezone.utc)
    sources.detector_package_sources(configuration=configuration, candidate_ids=(), as_of=cutoff)
    sources.detector_technical_sources(underlyers=configuration.settings.underlyers, market_cutoff=cutoff, as_of=cutoff)
    return dict(detector_cycle_hook=build_detector_collector(launch, configuration=configuration, backend_dir=BACKEND_DIR),
        detector_dataset_id=launch.dataset_id, detector_effective_from=launch.effective_from)


def _continuous_validation_callback(configuration, evidence_runtime=None):
    launch = configuration.stock_behavior_shadow_launch
    if launch is None and not (
        configuration.settings.package_assessments_enabled
        or configuration.settings.outcome_unavailable_evidence_enabled
    ):
        return None

    def validate(_available_at):
        result = {}
        if launch is not None:
            from report_option_stock_behavior_assessments import (
                build_report as stock_behavior_report,
            )

            report = stock_behavior_report(24, launch)
            if evidence_runtime is not None:
                runtime = evidence_runtime.summary()
                report["worker_runtime_evidence"] = runtime
                report.setdefault("realtime_evidence_validations", {})["WORKER_MEMORY_OVERHEAD"] = {
                    "state": runtime["state"], "reason": runtime["acceptance"],
                    "sample_count": runtime["memory_sample_count"],
                }
            _write_validation_artifact(
                BACKEND_DIR / launch.artifact_destination, report,
            )
            launch_report = report.get("launch") or {}
            result["stock_behavior"] = {
                "launch_id": launch.launch_id,
                "launch_sha256": launch.sha256,
                "state": launch_report.get("state"),
                "assessment_count": launch_report.get("assessment_count", 0),
                "stop_reasons": launch_report.get("stop_reasons", ()),
                "validation_warnings": launch_report.get(
                    "validation_warnings", ()
                ),
            }
        if (
            configuration.settings.package_assessments_enabled
            or configuration.settings.outcome_unavailable_evidence_enabled
        ):
            from report_option_package_assessments import (
                build_report as package_report,
            )

            report = package_report(24)
            if evidence_runtime is not None:
                report["worker_runtime_evidence"] = evidence_runtime.summary()
            _write_validation_artifact(
                BACKEND_DIR / "backups/options-package-assessments/status.json",
                report,
            )
            result["package_outcomes"] = {
                "storage_state": report.get("storage_state"),
                "framework_storage_ready": report.get("framework_storage_ready"),
                "assessment_count": report.get("assessment_count", 0),
                "outcome_unavailable_count": report.get(
                    "outcome_unavailable_count", 0
                ),
                "policy_usage": report.get("policy_usage"),
            }
        return result

    return validate


def _detector_status_callback(configuration, dataset_id):
    path = BACKEND_DIR / "backups/options-worker/detector-status.json"
    attempts = []
    if path.is_file() and path.stat().st_size <= 131072:
        try:
            previous = json.loads(path.read_text(encoding="utf-8"))
            if (previous.get("dataset_id") == dataset_id
                    and previous.get("configuration_sha256") == configuration.configuration_sha256):
                attempts = previous.get("attempts", [])[-95:]
        except (OSError, ValueError, TypeError):
            attempts = []

    def record(attempt):
        nonlocal attempts
        attempts = [row for row in attempts if row["scheduled_cycle"] != attempt["scheduled_cycle"]]
        attempts = sorted([*attempts, attempt], key=lambda row: row["scheduled_cycle"])[-96:]
        _write_validation_artifact(path, dict(version="option_detector_operational_status_v1", dataset_id=dataset_id,
            configuration_sha256=configuration.configuration_sha256, attempts=attempts,
            checked_at=attempt.get("finished_at", attempt["started_at"])))
    return record


def _write_validation_artifact(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


class RedactingFormatter(logging.Formatter):
    def __init__(self, fmt: str, secrets: tuple[str, ...]) -> None:
        super().__init__(fmt)
        self.secrets = sorted((value for value in secrets if len(value) >= 4), key=len, reverse=True)

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        return re.sub(r"(?i)((?:api[_-]?key|token|password|authorization)\s*[=:]\s*)[^\s&]+", r"\1[REDACTED]", text)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delayed option materialization worker")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process the latest observable delayed slot and exit.",
    )
    parser.add_argument("--check-technical-launch", action="store_true", help="Read-only launch/configuration/schema checks; no worker lock, provider requests or writes.")
    parser.add_argument(
        "--underlyers",
        help="Optional comma-separated configured underlyers for a controlled run.",
    )
    parser.add_argument(
        "--log-file", type=Path,
        default=BACKEND_DIR / "backups" / "options-worker" / "worker.log",
        help="Rotating redacted worker log (2 MB per file, three backups).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if getattr(args, "check_technical_launch", False):
        configuration = load_option_runtime_configuration()
        arguments = _detector_pipeline_arguments(configuration)
        print(json.dumps(dict(status="READY_FOR_NEXT_SESSION" if arguments else "DISABLED",
            dataset_id=arguments.get("detector_dataset_id"), effective_from=arguments.get("detector_effective_from"),
            strategy_version=configuration.strategy_policy.strategy_version,
            valuation_policy_version=configuration.valuation_policy.policy_version,
            freshness_seconds=configuration.valuation_policy.maximum_source_age_seconds,
            maximum_new_alerts=20, current_source_freshness="CHECKED_AT_EACH_MARKET_RUN", execution_permission=False), default=str))
        return 0
    args.log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = RedactingFormatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        tuple(value for key, value in os.environ.items()
              if re.search(r"key|token|secret|password|credential", key, re.I)),
    )
    handlers = [logging.StreamHandler(), RotatingFileHandler(
        args.log_file, maxBytes=2_000_000, backupCount=3, encoding="utf-8",
    )]
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(
        level=logging.INFO,
        handlers=handlers,
        force=True,
    )
    configuration = load_option_runtime_configuration()
    build_option_startup_state(configuration)
    detector_arguments = _detector_pipeline_arguments(configuration)
    instance_id = uuid4()
    leadership = OptionSchedulerLeadership(
        instance_id=instance_id,
        configuration_sha256=configuration.configuration_sha256,
        policy_sha256=configuration.policy_sha256,
        process_id=os.getpid(),
        host_name=socket.gethostname(),
    )
    if not leadership.acquire():
        logging.getLogger("option-worker").error(
            "another option worker holds leadership"
        )
        return 2
    try:
        strategy_pipeline = OptionStrategyPipeline(configuration)
        pipeline = ManualOptionPipeline(
            configuration,
            build_data_engine(configuration),
            strategy_pipeline=strategy_pipeline,
            **detector_arguments,
        )
        underlyers = None
        if args.underlyers:
            underlyers = tuple(
                value.strip().upper()
                for value in args.underlyers.split(",")
                if value.strip()
            )
        worker = OptionMaterializationWorker(
            pipeline,
            underlyers=underlyers,
            partition_maintainer=ensure_option_partitions,
            validation_callback=_continuous_validation_callback(configuration, strategy_pipeline.evidence_runtime),
            effective_from=detector_arguments.get("detector_effective_from"),
            detector_status_callback=_detector_status_callback(configuration, detector_arguments["detector_dataset_id"])
                if detector_arguments else None,
        )
        if detector_arguments:
            logging.getLogger("option-worker").info("TECHNICAL_FORWARD_ARMED dataset=%s effective_from=%s maximum_new_alerts=20 execution=False",
                detector_arguments["detector_dataset_id"], detector_arguments["detector_effective_from"].isoformat())
        if args.once:
            result = worker.poll_once(leadership.heartbeat)
            payload = (
                asdict(result)
                if result is not None
                else {"status": "NO_OBSERVABLE_SLOT"}
            )
            print(json.dumps(payload, sort_keys=True, default=str))
        else:
            worker.run_forever(leadership)
    finally:
        leadership.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())