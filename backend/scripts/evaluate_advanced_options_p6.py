#!/usr/bin/env python3
"""Merge P6 workload/resource evidence and emit the frozen PASS/FAIL verdict."""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from contextlib import closing
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import provision_realtime_v2_database as provision  # noqa: E402


DATABASE = "test_realtime_v2_options_p6"
FAULTS = {"consumer-kill", "reader-network", "orchestrator-kill"}
CPU_QUOTAS = {
    "stockscreener-options-p6-20261018-replay": 1.0,
    "stockscreener-options-p6-20261018-consumer": 2.0,
    "stockscreener-options-p6-20261018-orchestrator": 2.0,
    "stockscreener-options-p6-20261018-reader": 1.0,
    "stockscreener-options-p6-20261018-metrics": 0.5,
}
BYTE_UNITS = {
    "B": 1,
    "KB": 1_000,
    "KIB": 1_024,
    "MB": 1_000_000,
    "MIB": 1_048_576,
    "GB": 1_000_000_000,
    "GIB": 1_073_741_824,
}


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return math.inf
    ordered = sorted(values)
    index = min(
        len(ordered) - 1,
        max(0, math.ceil(len(ordered) * percentile) - 1),
    )
    return ordered[index]


def _percent(value: str) -> float:
    return float(value.strip().rstrip("%"))


def _bytes(value: str) -> float:
    match = re.fullmatch(r"\s*([0-9.]+)\s*([A-Za-z]+)\s*", value)
    if match is None:
        raise ValueError(f"unsupported Docker size: {value}")
    unit = match.group(2).upper()
    if unit not in BYTE_UNITS:
        raise ValueError(f"unsupported Docker size unit: {unit}")
    return float(match.group(1)) * BYTE_UNITS[unit]


def _resource_summary(resource: dict[str, object]) -> dict[str, object]:
    by_container: dict[str, dict[str, list[float]]] = {}
    for sample in resource["resource_samples"]:
        for row in sample["containers"]:
            name = row.get("Name") or row.get("Container")
            if name not in CPU_QUOTAS:
                continue
            values = by_container.setdefault(
                name,
                {"cpu_percent": [], "memory_percent": [], "memory_bytes": []},
            )
            values["cpu_percent"].append(_percent(row["CPUPerc"]))
            values["memory_percent"].append(_percent(row["MemPerc"]))
            values["memory_bytes"].append(
                _bytes(str(row["MemUsage"]).split("/", 1)[0])
            )
    return {
        name: {
            "samples": len(values["cpu_percent"]),
            "cpu_average_percent": (
                sum(values["cpu_percent"]) / len(values["cpu_percent"])
            ),
            "cpu_p99_percent": _percentile(values["cpu_percent"], 0.99),
            "memory_p99_percent": _percentile(
                values["memory_percent"],
                0.99,
            ),
            "memory_max_bytes": max(values["memory_bytes"]),
            "cpu_quota": CPU_QUOTAS[name],
        }
        for name, values in by_container.items()
        if values["cpu_percent"]
    }


def _database_bytes() -> int:
    settings = provision._settings()
    with closing(provision._connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_database_size(%s)",
                (DATABASE,),
            )
            return int(cursor.fetchone()[0])


def evaluate(
    workload: dict[str, object],
    resource: dict[str, object],
) -> dict[str, object]:
    resources = _resource_summary(resource)
    faults = {row["fault"] for row in resource["faults"]}
    events = workload["events"]
    storage = workload["storage"]
    no_effects = workload["orchestrator"]["forbidden_effect_counts"]
    checks = {
        "full_duration_scale": workload["duration_scale"] == 1,
        "logical_observation_at_least_2400_seconds": (
            workload["logical_input_duration_seconds"] >= 2390
        ),
        "weighted_rate_at_least_990_per_second": (
            workload["accepted_per_second"] >= 990
        ),
        "p95_lag_at_most_5000_ms": workload["lag_ms"]["p95"] <= 5000,
        "p99_lag_at_most_10000_ms": workload["lag_ms"]["p99"] <= 10000,
        "queue_depth_at_most_100000": (
            workload["queue"]["maximum_depth"] <= 100_000
        ),
        "drain_at_most_120_seconds": workload["drain_seconds"] <= 120,
        "duplicates_exact": events["duplicates"] == 100,
        "out_of_order_exact": events["out_of_order"] == 100,
        "generation_mismatch_exact": events["generation_mismatch"] == 1,
        "cycles_exercised": workload["cycles"] > 0,
        "o2_remains_unavailable": (
            workload["o2_reason"] == "O2_MODEL_NOT_ACCEPTED"
        ),
        "reader_network_fault_observed": workload["reader"]["failures"] > 0,
        "all_faults_injected": faults == FAULTS,
        "journal_at_most_6_gib": (
            storage["journal_bytes"] <= 6 * 1_073_741_824
        ),
        "reports_at_most_100_mib": (
            storage["report_bytes_before_final_report"] <= 100 * 1_048_576
        ),
        "disk_free_at_least_10_gib": (
            min(
                storage["journal_free_bytes"],
                storage["report_free_bytes"],
            )
            >= 10 * 1_073_741_824
        ),
        "disk_free_at_least_30_percent": (
            min(
                storage["journal_free_percent"],
                storage["report_free_percent"],
            )
            >= 30
        ),
        "database_at_most_4_gib": _database_bytes() <= 4 * 1_073_741_824,
        "forbidden_effect_counts_zero": not any(no_effects.values()),
        "provider_calls_zero": workload["provider_calls"] == 0,
        "publication_not_authorized": not workload["publication_authorized"],
        "archive_not_authorized": not workload[
            "archive_execution_authorized"
        ],
        "retention_not_authorized": not workload[
            "retention_execution_authorized"
        ],
        "execution_not_authorized": not workload["execution_authorized"],
        "delivery_not_authorized": not workload["delivery_authorized"],
        "all_resource_services_sampled": set(resources) == set(CPU_QUOTAS),
        "cpu_average_headroom": all(
            row["cpu_average_percent"] <= row["cpu_quota"] * 70
            for row in resources.values()
        ),
        "cpu_p99_headroom": all(
            row["cpu_p99_percent"] <= row["cpu_quota"] * 90
            for row in resources.values()
        ),
        "memory_headroom": all(
            row["memory_p99_percent"] <= 70
            for row in resources.values()
        ),
    }
    failures = sorted(name for name, passed in checks.items() if not passed)
    return {
        "status": "PASS" if not failures else "FAIL",
        "checks": checks,
        "failures": failures,
        "resources": resources,
        "workload": workload,
        "faults": resource["faults"],
        "database_bytes": _database_bytes(),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload-report", type=Path, required=True)
    parser.add_argument("--resource-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    workload = json.loads(args.workload_report.read_text(encoding="utf-8"))
    resource = json.loads(args.resource_report.read_text(encoding="utf-8"))
    report = evaluate(workload, resource)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps({
        "status": report["status"],
        "failure_count": len(report["failures"]),
        "failures": report["failures"],
    }, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
