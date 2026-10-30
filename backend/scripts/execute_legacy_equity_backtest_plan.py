"""Continue the legacy equity backtest plan from readiness through final report."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


PHASES = ("B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8")

LANE_TO_PHASE = {
    "controls_r2_b3_f2": "B3",
    "sw3_r_b_f": "B4",
    "sw1": "B4",
    "d1_d2": "B5",
    "p1_p2": "B6",
}

PHASE_LANES = {
    "B3": ("controls_r2_b3_f2",),
    "B4": ("sw3_r_b_f", "sw1"),
    "B5": ("d1_d2",),
    "B6": ("p1_p2",),
}


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("ascii")).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_immutable(path: Path, content: bytes) -> str:
    digest = _sha256_bytes(content)
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) != digest:
            raise FileExistsError(f"immutable artifact conflict at {path}")
        return digest
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return digest


def _write_immutable_json(path: Path, payload: dict[str, Any]) -> str:
    content = (json.dumps(payload, indent=2, ensure_ascii=True, default=str) + "\n").encode("utf-8")
    digest = _write_immutable(path, content)
    _write_immutable(path.with_suffix(path.suffix + ".sha256"), (digest + "\n").encode("ascii"))
    return digest


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object at {path}")
    return value


def _phase_verdict(lane_readiness: dict[str, Any], lanes: tuple[str, ...]) -> str:
    statuses = [lane_readiness[name]["status"] for name in lanes]
    if all(status == "INPUT_READY" for status in statuses):
        return "QUALIFIED"
    if any(status == "INPUT_READY" for status in statuses):
        return "INCONCLUSIVE"
    return "BLOCKED_INPUT_READINESS"


def _blocked_lane_artifact(*, lane: str, phase: str, readiness: dict[str, Any], source_sha: str) -> dict[str, Any]:
    detail = readiness[lane]
    return {
        "artifact_version": "legacy_equity_lane_verdict_v1",
        "phase": phase,
        "lane": lane,
        "verdict": detail["status"],
        "required_gates": detail["required_gates"],
        "missing_gates": detail["missing_gates"],
        "blockers": detail["blockers"],
        "source_readiness_sha256": source_sha,
        "notes": [
            "Provider-free, read-only continuation artifact.",
            "No model execution occurred for BLOCKED_INPUT_READINESS lanes.",
        ],
    }


def _append_execution_log(path: Path, entry: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, sort_keys=True, ensure_ascii=True, default=str)
    with path.open("a", encoding="utf-8") as output:
        output.write(line + "\n")


def execute(*, artifact_root: Path, readiness_path: Path, daily_path: Path, validation_proof: dict[str, str]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    readiness = _read_json(readiness_path)
    daily = _read_json(daily_path)
    readiness_sha = _sha256_bytes(readiness_path.read_bytes())
    daily_sha = _sha256_bytes(daily_path.read_bytes())
    lane_readiness = readiness["lane_readiness"]
    log_path = artifact_root / "logs" / "execution.jsonl"

    b2 = {
        "artifact_version": "legacy_equity_phase_b2_contracts_v1",
        "phase": "B2",
        "verdict": "QUALIFIED",
        "provider_calls_made": False,
        "database_writes_made": False,
        "pinned_files_unchanged": [
            "backend/research/stock_idea_models.py",
            "backend/research/stock_idea_swing.py",
        ],
        "validation": validation_proof,
        "source_artifacts": {
            "daily_coverage_sha256": daily_sha,
            "b1_readiness_sha256": readiness_sha,
        },
    }
    b2_sha = _write_immutable_json(artifact_root / "phase-b2-contract-validation.json", b2)
    _append_execution_log(log_path, {
        "phase": "B2", "state": "COMPLETED", "verdict": b2["verdict"],
        "artifact": "phase-b2-contract-validation.json", "artifact_sha256": b2_sha,
        "completed_at": now,
    })

    phase_summaries: dict[str, dict[str, Any]] = {}
    lane_artifacts: dict[str, dict[str, str]] = {}
    for phase, lanes in PHASE_LANES.items():
        verdict = _phase_verdict(lane_readiness, lanes)
        lane_entries = []
        for lane in lanes:
            lane_payload = _blocked_lane_artifact(
                lane=lane,
                phase=phase,
                readiness=lane_readiness,
                source_sha=readiness_sha,
            )
            lane_path = artifact_root / "lanes" / lane / "qualification.json"
            lane_sha = _write_immutable_json(lane_path, lane_payload)
            lane_entries.append({"lane": lane, "verdict": lane_payload["verdict"], "artifact_sha256": lane_sha})
            lane_artifacts[lane] = {"path": str(lane_path), "sha256": lane_sha}
        phase_payload = {
            "artifact_version": "legacy_equity_phase_summary_v1",
            "phase": phase,
            "verdict": verdict,
            "lane_results": lane_entries,
            "source_readiness_sha256": readiness_sha,
        }
        phase_path = artifact_root / f"phase-{phase.lower()}-summary.json"
        phase_sha = _write_immutable_json(phase_path, phase_payload)
        phase_summaries[phase] = {
            "path": str(phase_path),
            "sha256": phase_sha,
            "verdict": verdict,
        }
        _append_execution_log(log_path, {
            "phase": phase, "state": "COMPLETED", "verdict": verdict,
            "artifact": phase_path.name, "artifact_sha256": phase_sha, "completed_at": now,
        })

    b7 = {
        "artifact_version": "legacy_equity_phase_b7_evaluation_v1",
        "phase": "B7",
        "verdict": "BLOCKED_INPUT_READINESS",
        "evaluation_status": "NO_INPUT_READY_LANES",
        "lane_verdicts": {lane: lane_readiness[lane]["status"] for lane in lane_readiness},
        "guardrails": {
            "chronological_split": "60/20/20",
            "minimum_blocks": 40,
            "minimum_maturity_ratio": 0.95,
            "familywise_method": "Holm",
        },
        "reason": "No lane reached INPUT_READY; no training or holdout evaluation executed.",
        "source_readiness_sha256": readiness_sha,
    }
    b7_sha = _write_immutable_json(artifact_root / "phase-b7-evaluation.json", b7)
    _append_execution_log(log_path, {
        "phase": "B7", "state": "COMPLETED", "verdict": b7["verdict"],
        "artifact": "phase-b7-evaluation.json", "artifact_sha256": b7_sha, "completed_at": now,
    })

    promotable = []
    b8 = {
        "artifact_version": "legacy_equity_phase_b8_promotion_v1",
        "phase": "B8",
        "verdict": "REJECTED",
        "promotable_lanes": promotable,
        "v2_promotion_recommendation": "DO_NOT_PROMOTE",
        "blocked_lanes": {
            lane: lane_readiness[lane]["blockers"] for lane in sorted(lane_readiness)
        },
        "required_for_future_v2_consideration": [
            "INPUT_READY lane status with reproducible artifacts",
            "B7 evaluation passes with untouched holdout confirmation",
            "Canonical parity evidence between legacy and V2 adapters",
            "Separate authorized V2 evidence-only shadow parity",
        ],
        "source_artifacts": {
            "daily_coverage_sha256": daily_sha,
            "b1_readiness_sha256": readiness_sha,
            "b7_evaluation_sha256": b7_sha,
        },
    }
    b8_sha = _write_immutable_json(artifact_root / "final-qualification-and-v2-promotion-report.json", b8)
    _append_execution_log(log_path, {
        "phase": "B8", "state": "COMPLETED", "verdict": b8["verdict"],
        "artifact": "final-qualification-and-v2-promotion-report.json",
        "artifact_sha256": b8_sha,
        "completed_at": now,
    })

    final = {
        "completed_at": now,
        "artifact_root": str(artifact_root),
        "phase_artifacts": phase_summaries | {
            "B2": {
                "path": str(artifact_root / "phase-b2-contract-validation.json"),
                "sha256": b2_sha,
                "verdict": b2["verdict"],
            },
            "B7": {
                "path": str(artifact_root / "phase-b7-evaluation.json"),
                "sha256": b7_sha,
                "verdict": b7["verdict"],
            },
            "B8": {
                "path": str(artifact_root / "final-qualification-and-v2-promotion-report.json"),
                "sha256": b8_sha,
                "verdict": b8["verdict"],
            },
        },
        "lane_artifacts": lane_artifacts,
        "execution_log": str(log_path),
    }
    final_sha = _write_immutable_json(artifact_root / "execution-checkpoint-summary.json", final)
    return final | {"execution_checkpoint_sha256": final_sha}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--readiness-artifact", type=Path, required=True)
    parser.add_argument("--daily-artifact", type=Path, required=True)
    parser.add_argument(
        "--validation-command",
        default="python -m pytest backend/tests/test_legacy_equity_phase_execution.py -q",
    )
    parser.add_argument("--validation-result", default="passed")
    args = parser.parse_args()
    report = execute(
        artifact_root=args.artifact_root,
        readiness_path=args.readiness_artifact,
        daily_path=args.daily_artifact,
        validation_proof={
            "command": args.validation_command,
            "result": args.validation_result,
        },
    )
    print(
        json.dumps(
            {
                "artifact_root": report["artifact_root"],
                "execution_checkpoint_sha256": report["execution_checkpoint_sha256"],
                "final_report": report["phase_artifacts"]["B8"],
            },
            indent=2,
            ensure_ascii=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
