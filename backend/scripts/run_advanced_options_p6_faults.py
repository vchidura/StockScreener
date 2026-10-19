#!/usr/bin/env python3
"""Inject the approved P6 container faults and collect Docker resource samples."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path


PROJECT = "stockscreener-options-p6-20261018"
CONTAINERS = {
    "replay": f"{PROJECT}-replay",
    "consumer": f"{PROJECT}-consumer",
    "orchestrator": f"{PROJECT}-orchestrator",
    "reader": f"{PROJECT}-reader",
    "metrics": f"{PROJECT}-metrics",
}


def _run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        args,
        check=check,
        capture_output=True,
        text=True,
    )


def _compose(compose_file: Path, *args: str) -> subprocess.CompletedProcess:
    return _run(
        "docker",
        "compose",
        "-p",
        PROJECT,
        "-f",
        str(compose_file),
        *args,
    )


def _stats() -> list[dict[str, object]]:
    result = _run(
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{json .}}",
        *CONTAINERS.values(),
        check=False,
    )
    rows = []
    for line in result.stdout.splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _running(name: str) -> bool:
    result = _run(
        "docker",
        "inspect",
        "-f",
        "{{.State.Running}}",
        name,
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-scale", type=float, default=1.0)
    args = parser.parse_args(argv)
    if not 0 < args.duration_scale <= 1:
        parser.error("--duration-scale must be in (0, 1]")
    args.compose_file = args.compose_file.resolve()
    started = time.monotonic()
    samples = []
    faults = []
    schedule = [
        (600, "consumer-kill"),
        (1200, "reader-network"),
        (1800, "orchestrator-kill"),
    ]
    completed = set()
    deadline = started + 3000 * args.duration_scale + 300
    while time.monotonic() < deadline:
        elapsed_logical = (time.monotonic() - started) / args.duration_scale
        samples.append({
            "elapsed_logical_seconds": round(elapsed_logical, 3),
            "containers": _stats(),
        })
        for at, fault in schedule:
            if fault in completed or elapsed_logical < at:
                continue
            if fault == "consumer-kill":
                _compose(args.compose_file, "kill", "-s", "SIGKILL", "p6-consumer")
                _compose(args.compose_file, "start", "p6-consumer")
            elif fault == "orchestrator-kill":
                _compose(
                    args.compose_file,
                    "kill",
                    "-s",
                    "SIGKILL",
                    "p6-orchestrator",
                )
                _compose(args.compose_file, "start", "p6-orchestrator")
            else:
                network = f"{PROJECT}-internal"
                _run(
                    "docker",
                    "network",
                    "disconnect",
                    network,
                    CONTAINERS["reader"],
                )
                time.sleep(30 * args.duration_scale)
                _run(
                    "docker",
                    "network",
                    "connect",
                    network,
                    CONTAINERS["reader"],
                )
            faults.append({
                "fault": fault,
                "elapsed_logical_seconds": round(elapsed_logical, 3),
            })
            completed.add(fault)
        if not _running(CONTAINERS["metrics"]):
            result = _run(
                "docker",
                "inspect",
                "-f",
                "{{.State.ExitCode}}",
                CONTAINERS["metrics"],
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip() == "0":
                break
        time.sleep(min(15, max(0.1, 15 * args.duration_scale)))
    else:
        raise RuntimeError("P6 fault controller timed out")
    missing = sorted({fault for _, fault in schedule} - completed)
    if missing:
        raise RuntimeError(f"P6 faults were not injected: {missing}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {"faults": faults, "resource_samples": samples},
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
