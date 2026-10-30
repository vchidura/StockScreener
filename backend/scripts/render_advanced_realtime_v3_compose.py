#!/usr/bin/env python3
"""Render and redact-check the Advanced Realtime V3 Compose configuration."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

from dotenv import dotenv_values


BACKEND_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = BACKEND_DIR.parent
TARGET_SERVICES = (
    "realtime-v3-options-acquisition",
    "realtime-v2-options-p8-ingress",
    "realtime-v2-options-p8-market-state",
    "realtime-v2-options-p8-flow",
    "realtime-v2-options-p8-window",
    "realtime-v2-canonical-stock-alerts",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _secret_values(directory: Path) -> tuple[str, ...]:
    values = []
    for path in sorted(directory.iterdir()):
        if path.is_file():
            value = path.read_text(encoding="utf-8").strip()
            if value:
                values.append(value)
    if not values:
        raise ValueError("secret directory contains no values")
    return tuple(values)


def _validate_rendered(
    rendered: dict,
    image_id: str,
    secret_values: tuple[str, ...],
) -> None:
    services = rendered.get("services")
    if not isinstance(services, dict):
        raise ValueError("rendered Compose services are absent")
    missing = [
        name for name in TARGET_SERVICES
        if name not in services
    ]
    divergent = [
        name for name in TARGET_SERVICES
        if name in services
        and services[name].get("image") != image_id
    ]
    encoded = json.dumps(rendered, sort_keys=True)
    if missing or divergent:
        raise ValueError(
            f"rendered Compose service identity changed: "
            f"missing={missing} divergent={divergent}"
        )
    if any(secret in encoded for secret in secret_values):
        raise ValueError("rendered Compose configuration exposed a secret")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-executable", default="docker.exe")
    parser.add_argument(
        "--compose-file",
        type=Path,
        default=REPOSITORY_ROOT / "docker-compose.realtime-v2.yml",
    )
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--acquisition-binding", type=Path, required=True)
    parser.add_argument("--secret-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    for name in (
        "compose_file",
        "env_file",
        "package",
        "bundle",
        "acquisition_binding",
        "secret_directory",
        "output",
        "report",
    ):
        setattr(args, name, getattr(args, name).resolve())
    if (
        not args.image_id.startswith("sha256:")
        or len(args.image_id) != 71
        or not all(
            path.is_file()
            for path in (
                args.compose_file,
                args.env_file,
                args.package,
                args.bundle,
                args.acquisition_binding,
            )
        )
        or not args.secret_directory.is_dir()
        or args.output.exists()
        or args.report.exists()
        or args.output.parent != args.report.parent
        or not args.output.parent.is_dir()
        or not args.execute
    ):
        parser.error(
            "Compose rendering requires exact files, immutable image, new "
            "colocated outputs and --execute"
        )
    return args


def execute(args) -> dict:
    environment = {
        **{
            name: value
            for name, value in dotenv_values(args.env_file).items()
            if value is not None
        },
        **os.environ,
        "REALTIME_V2_BACKEND_IMAGE": args.image_id,
        "REALTIME_V2_P8_PACKAGE_PATH": str(args.package),
        "REALTIME_V2_P8_BUNDLE_PATH": str(args.bundle),
        "REALTIME_V3_OPTIONS_ACQUISITION_BINDING_PATH": (
            str(args.acquisition_binding)
        ),
        "REALTIME_V2_DOCKER_SECRET_DIR": str(args.secret_directory),
    }
    completed = subprocess.run(
        [
            args.docker_executable,
            "compose",
            "--file",
            str(args.compose_file),
            "--profile",
            "realtime-v3-options",
            "config",
            "--format",
            "json",
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )
    if completed.returncode:
        raise RuntimeError(
            f"Compose rendering failed with {completed.returncode}: "
            f"{completed.stderr[-2000:]}"
        )
    try:
        rendered = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Compose rendering did not return JSON") from exc
    secrets = _secret_values(args.secret_directory)
    _validate_rendered(rendered, args.image_id, secrets)
    encoded = (
        json.dumps(rendered, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with args.output.open("xb") as stream:
        stream.write(encoded)
    report = {
        "schema": "advanced-realtime-v3-compose-render-report-v1",
        "status": "COMPOSE_RENDERED_REDACTED",
        "image_id": args.image_id,
        "rendered_compose_sha256": hashlib.sha256(encoded).hexdigest(),
        "package_file_sha256": _sha256(args.package),
        "bundle_file_sha256": _sha256(args.bundle),
        "acquisition_binding_file_sha256": _sha256(
            args.acquisition_binding
        ),
        "target_services": list(TARGET_SERVICES),
        "secret_values_checked": len(secrets),
        "provider_effects": 0,
        "production_effects": 0,
        "service_actions": 0,
    }
    report_bytes = (
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    with args.report.open("xb") as stream:
        stream.write(report_bytes)
    return {
        **report,
        "output": str(args.output),
        "report": str(args.report),
        "report_file_sha256": hashlib.sha256(
            report_bytes
        ).hexdigest(),
    }


def main(argv=None) -> int:
    result = execute(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
