#!/usr/bin/env python3
"""Independently verify a frozen WP7 package-evaluation artifact."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics.package_evaluation import (  # noqa: E402
    OptionPackageEvaluationManifest,
)


def _arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    return parser.parse_args()


def _sha256_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def verify_report(manifest_path: Path, report_path: Path | None = None) -> dict:
    manifest = OptionPackageEvaluationManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    approved_path = (BACKEND_DIR / manifest.artifact_destination).resolve()
    if report_path is None:
        report_path = approved_path
    else:
        report_path = report_path.resolve()
    if report_path != approved_path:
        raise ValueError("report path differs from frozen artifact destination")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("evaluation report must be a JSON object")
    if report.get("manifest") != manifest.model_dump(mode="json"):
        raise ValueError("embedded manifest differs from frozen manifest")
    if report.get("manifest_sha256") != manifest.sha256:
        raise ValueError("manifest hash differs from frozen manifest")
    claimed_report_sha256 = report.get("report_sha256")
    report_without_hash = dict(report)
    report_without_hash.pop("report_sha256", None)
    resolved_report_sha256 = _sha256_json(report_without_hash)
    if claimed_report_sha256 != resolved_report_sha256:
        raise ValueError("report hash does not match artifact content")
    required_values = {
        "schema_version": "option_package_evaluation_report_v1",
        "study_id": manifest.study_id,
        "transaction_read_only": True,
        "calibration_status": "NOT_ATTEMPTED",
        "probability": None,
        "research_only": True,
        "execution_permission": False,
    }
    for field, expected in required_values.items():
        if report.get(field) != expected:
            raise ValueError(f"report {field} violates frozen research boundary")
    return {
        "status": "VERIFIED",
        "study_id": manifest.study_id,
        "manifest_sha256": manifest.sha256,
        "report_sha256": resolved_report_sha256,
        "artifact": manifest.artifact_destination,
        "source_counts": report.get("source_counts", {}),
        "probability": None,
        "execution_permission": False,
    }


def main() -> int:
    args = _arguments()
    result = verify_report(
        args.manifest.resolve(), args.report.resolve() if args.report else None,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())