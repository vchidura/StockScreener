#!/usr/bin/env python3
"""Build and run the isolated provider-free P6E Docker source/process qualifier."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from uuid import UUID, uuid4


BACKEND_DIR = Path(__file__).resolve().parent.parent
TESTS = (
    "tests/realtime/test_advanced_gateway.py",
    "tests/realtime/test_buffer.py",
    "tests/realtime/test_advanced_options_ingress.py",
    "tests/realtime/test_advanced_options_p6e_process_sharding.py",
)
CPUS = 2
MEMORY_BYTES = 2 * 1024**3
MAXIMUM_SECONDS = 300


def qualification_plan(run_id: UUID) -> dict[str, object]:
    suffix = run_id.hex
    return {
        "contract_version": "advanced-options-p6e-docker-v1",
        "run_id": str(run_id),
        "image": f"alpha-research-options-p6e:{suffix}",
        "container": f"stockscreener-options-p6e-{suffix}",
        "cpus": CPUS,
        "memory_bytes": MEMORY_BYTES,
        "maximum_seconds": MAXIMUM_SECONDS,
        "tests": list(TESTS),
        "provider_requests_authorized": 0,
        "publication_authorized": False,
        "archive_retention_authorized": False,
        "execution_authorized": False,
        "delivery_authorized": False,
    }


def _run(*arguments, timeout=120, check=True):
    result = subprocess.run(
        ["docker", *map(str, arguments)],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode:
        raise RuntimeError(
            f"Docker {arguments[0]} failed ({result.returncode}): "
            f"{result.stderr[-1500:]}"
        )
    return result


def _inspect(kind, name):
    result = _run(kind, "inspect", name, timeout=15, check=False)
    if result.returncode:
        if "no such" in result.stderr.lower():
            return None
        raise RuntimeError(f"cannot inspect owned {kind}: {result.stderr[-500:]}")
    return json.loads(result.stdout)[0]


def _running_containers():
    result = _run(
        "ps",
        "--format",
        "{{.ID}}|{{.Names}}|{{.Image}}",
        timeout=30,
    )
    return tuple(sorted(
        line for line in result.stdout.splitlines() if line.strip()
    ))


def _remove_owned_container(plan):
    info = _inspect("container", plan["container"])
    if info is None:
        return
    labels = info["Config"].get("Labels") or {}
    if labels.get("qualification.run_id") != plan["run_id"]:
        raise RuntimeError("refusing to remove an unowned P6E container")
    if info["State"]["Running"]:
        _run("kill", info["Id"], timeout=30)
    _run("rm", info["Id"], timeout=30)


def _remove_owned_image(plan):
    info = _inspect("image", plan["image"])
    if info is None:
        return
    _run("image", "rm", info["Id"], timeout=60)


def _container_arguments(plan, manifest, output):
    return (
        "create",
        "--name",
        plan["container"],
        "--label",
        f"qualification.run_id={plan['run_id']}",
        "--cpus",
        str(plan["cpus"]),
        "--memory",
        str(plan["memory_bytes"]),
        "--memory-swap",
        str(plan["memory_bytes"]),
        "--pids-limit",
        "128",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--tmpfs",
        "/tmp:rw,nosuid,size=268435456",
        "--env",
        "PYTHONDONTWRITEBYTECODE=1",
        "--env",
        "OPENBLAS_NUM_THREADS=2",
        "--env",
        "OMP_NUM_THREADS=2",
        "--mount",
        f"type=bind,source={manifest},target=/qualification.json,readonly",
        "--mount",
        f"type=bind,source={output},target=/qualification-output",
        plan["image"],
        "python",
        "-B",
        "scripts/run_advanced_options_p6e_container.py",
        "--manifest",
        "/qualification.json",
        "--output",
        "/qualification-output",
    )


def execute(plan, output):
    output.mkdir(parents=True, exist_ok=False)
    manifest = output / "manifest.json"
    manifest.write_text(
        json.dumps(plan, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    before = _running_containers()
    if _inspect("container", plan["container"]) is not None:
        raise RuntimeError("P6E container already exists")
    if _inspect("image", plan["image"]) is not None:
        raise RuntimeError("P6E image tag already exists")
    started = time.perf_counter()
    container_id = None
    image_id = None
    worker = None
    error = None
    try:
        build_log = output / "build.log"
        with build_log.open("w", encoding="utf-8") as log:
            build = subprocess.run(
                [
                    "docker",
                    "build",
                    "-f",
                    str(BACKEND_DIR / "Dockerfile.p6e"),
                    "-t",
                    plan["image"],
                    str(BACKEND_DIR),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
                timeout=1200,
            )
        if build.returncode:
            raise RuntimeError(
                f"P6E image build failed ({build.returncode})"
            )
        image_id = _run(
            "image",
            "inspect",
            plan["image"],
            "--format",
            "{{.Id}}",
        ).stdout.strip()
        created = _run(*_container_arguments(plan, manifest, output))
        container_id = created.stdout.strip()
        _run("start", container_id)
        deadline = time.monotonic() + plan["maximum_seconds"]
        while True:
            info = _inspect("container", plan["container"])
            if info is None:
                raise RuntimeError("P6E container vanished")
            if not info["State"]["Running"]:
                break
            if time.monotonic() >= deadline:
                _run("kill", container_id, timeout=30)
                raise TimeoutError("P6E container exceeded its deadline")
            time.sleep(0.25)
        logs = _run("logs", container_id, timeout=30)
        (output / "container.log").write_text(
            logs.stdout + logs.stderr,
            encoding="utf-8",
        )
        if info["State"]["ExitCode"] != 0 or info["State"]["OOMKilled"]:
            raise RuntimeError(
                "P6E container failed: "
                f"exit={info['State']['ExitCode']}, "
                f"oom={info['State']['OOMKilled']}"
            )
        worker = json.loads(
            (output / "container.json").read_text(encoding="utf-8")
        )
        if worker["status"] != "PASS_PROVIDER_FREE":
            raise RuntimeError("P6E worker report did not pass")
    except BaseException as failure:
        error = f"{type(failure).__name__}:{failure}"
    finally:
        _remove_owned_container(plan)
        _remove_owned_image(plan)
    after = _running_containers()
    cleanup_verified = (
        _inspect("container", plan["container"]) is None
        and _inspect("image", plan["image"]) is None
    )
    report = {
        **plan,
        "status": (
            "PASS_PROVIDER_FREE"
            if error is None and cleanup_verified and before == after
            else "FAIL"
        ),
        "image_id": image_id,
        "container_id": container_id,
        "worker": worker,
        "elapsed_seconds": time.perf_counter() - started,
        "cleanup_verified": cleanup_verified,
        "running_container_set_unchanged": before == after,
        "provider_requests": 0,
        "publication_performed": False,
        "archive_retention_performed": False,
        "execution_performed": False,
        "delivery_performed": False,
        "error": error,
    }
    (output / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps({
        "status": report["status"],
        "cleanup_verified": report["cleanup_verified"],
        "error": report["error"],
    }, sort_keys=True))
    return 0 if report["status"] == "PASS_PROVIDER_FREE" else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--run-id", type=UUID)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    plan = qualification_plan(args.run_id or uuid4())
    if not args.execute:
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0
    return execute(plan, args.output_directory.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
