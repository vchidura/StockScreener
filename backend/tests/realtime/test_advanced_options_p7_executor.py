from __future__ import annotations

import hashlib
import importlib
import json
import time
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from realtime.advanced_options_ingress import AdvancedOptionsJournalPump
from realtime.advanced_options_p7_executor import execute_shadow
from realtime.advanced_options_p7_executor import (
    run_disposable_database_phase,
)
from realtime.advanced_options_p7_shadow import build_authorization
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import AssetClass, MarketFeed, RawMessageEnvelope
from realtime.normalizer import InstrumentAdmission
from v2_database import requires_v2_database


UTC = timezone.utc
START = datetime(2026, 10, 20, 13, 35, tzinfo=UTC)
RUN_ID = UUID("88888888-8888-8888-8888-888888888888")
HEAD = "89b6c1b142b411cc57c519ce2de57543311a1b48"
BACKEND_DIR = Path(__file__).resolve().parents[2]
script = importlib.import_module("scripts.run_advanced_options_p7_shadow")


def _plan(tmp_path):
    return build_authorization(
        run_id=RUN_ID,
        authorization_reference=(
            "ADVANCED-OPTIONS-P7-SHADOW-20261020-TEST"
        ),
        baseline_head=HEAD,
        start_at=START,
        controlled_reconnect_at=START + timedelta(minutes=5),
        stop_at=START + timedelta(minutes=10),
        spy_contract="O:SPY261120C00500000",
        qqq_contract="O:QQQ261120C00500000",
        backend_dir=BACKEND_DIR,
        artifact_root=tmp_path,
    )


def _capture(plan, _api_key):
    temporary = Path(plan["cleanup"]["temporary_directory"])
    temporary.mkdir(parents=True)
    Path(plan["cleanup"]["journal_files"][0]).write_bytes(b"fixture")
    return {
        "status": "PASS_CAPTURE",
        "blockers": [],
        "journal_generation": str(RUN_ID),
        "journal_root": str(temporary),
        "journal_path": plan["cleanup"]["journal_files"][0],
        "session_ids": [str(RUN_ID)],
        "sessions": [
            {
                "attempt": 1,
                "stream_session_id": str(RUN_ID),
                "accepted_frames": 2,
                "received_frames": 4,
                "received_bytes": 1024,
                "termination": "DEADLINE",
            }
        ],
        "telemetry": [
            {
                "last_raw_offset": 2,
                "last_event_offset": 4,
            }
        ],
        "received_frames": 4,
        "received_bytes": 1024,
        "raw_frames": 2,
        "normalized_messages": 4,
        "event_counts": {"Q": 2, "T": 2},
        "condition_counts": {"209": 2},
        "exchange_counts": {"302": 2},
        "symbol_event_counts": {
            "O:SPY261120C00500000|Q": 1,
            "O:SPY261120C00500000|T": 1,
            "O:QQQ261120C00500000|Q": 1,
            "O:QQQ261120C00500000|T": 1,
        },
        "progress": {
            "last_raw_offset": 2,
            "last_event_offset": 4,
        },
        "buffer": {
            "admitted_events": 0,
            "full_count": 0,
        },
    }


def _database(_plan, _capture_report):
    return {
        "status": "PASS_DATABASE",
        "events": 2,
        "clusters": 1,
        "volume_rows": 2,
        "checkpoint_rows": 9,
        "replay_identical": True,
        "cleanup_verified": True,
        "database_absent": True,
        "roles_absent": True,
    }


def test_executor_writes_all_evidence_and_cleans_owned_temporary_data(
    tmp_path,
):
    plan = _plan(tmp_path)

    report = execute_shadow(
        plan,
        manifest_sha256="a" * 64,
        approval_sha256="b" * 64,
        api_key="test-secret",
        now=START + timedelta(seconds=1),
        capture=_capture,
        database=_database,
    )

    evidence = Path(plan["evidence"]["directory"])
    assert report["status"] == "PASS_LIVE_SHADOW"
    assert sorted(path.name for path in evidence.iterdir()) == sorted(
        plan["evidence"]["files"]
    )
    assert not Path(plan["cleanup"]["temporary_directory"]).exists()
    assert json.loads(
        (evidence / "no-effects.json").read_text(encoding="utf-8")
    )["provider_rest_requests"] == 0
    assert json.loads(
        (evidence / "report.json").read_text(encoding="utf-8")
    )["status"] == "PASS_LIVE_SHADOW"


def test_executor_failure_is_failure_shaped_and_still_cleans(tmp_path):
    plan = _plan(tmp_path)

    def failed_database(_plan, _capture_report):
        raise RuntimeError("database fixture failed")

    report = execute_shadow(
        plan,
        manifest_sha256="a" * 64,
        approval_sha256="b" * 64,
        api_key="test-secret",
        now=START + timedelta(seconds=1),
        capture=_capture,
        database=failed_database,
    )

    evidence = Path(plan["evidence"]["directory"])
    assert report["status"] == "FAIL"
    assert report["error"] == "RuntimeError:database fixture failed"
    assert not Path(plan["cleanup"]["temporary_directory"]).exists()
    assert json.loads(
        (evidence / "database-audit.json").read_text(encoding="utf-8")
    ) == {"status": "NOT_COMPLETED"}


def test_live_cli_plan_validates_approval_without_loading_credentials(
    tmp_path,
    monkeypatch,
    capsys,
):
    plan = _plan(tmp_path)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    approval = tmp_path / "approval.json"
    approval.write_text(
        json.dumps(
            {
                "contract_version": (
                    "advanced-options-p7-shadow-approval-v1"
                ),
                "manifest_id": plan["manifest_id"],
                "manifest_sha256": hashlib.sha256(
                    manifest.read_bytes()
                ).hexdigest(),
                "authorization_reference": plan[
                    "authorization_reference"
                ],
                "owner_decision": (
                    "approve_bounded_advanced_options_p7_shadow"
                ),
                "conditional_live_shadow_authorized": True,
                "provider_websocket_connection_attempts_authorized": 2,
                "provider_rest_requests_authorized": 0,
                "production_effects_authorized": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        script,
        "_api_key",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("PLAN loaded a credential")
        ),
    )

    assert script.main(
        [
            "--manifest",
            str(manifest),
            "--approval",
            str(approval),
        ]
    ) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "PLAN"
    assert result["provider_requests"] == 0
    assert result["external_socket_connections"] == 0
    assert result["database_connections"] == 0
    assert result["database_ddl"] == 0
    assert result["files_written"] == 0


@requires_v2_database
def test_real_executor_database_phase_replays_and_cleans(tmp_path):
    plan = deepcopy(_plan(tmp_path))
    plan["database"] = {
        **plan["database"],
        "name": "test_realtime_v2_options_p7r_20261019",
        "role_prefix": "test_rt_options_p7r_20261019",
        "roles": [
            "test_rt_options_p7r_20261019_admin",
            "test_rt_options_p7r_20261019_app",
            "test_rt_options_p7r_20261019_api",
        ],
    }
    generation = RUN_ID
    session_ids = (
        UUID("99999999-9999-9999-9999-999999999999"),
        UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
    )
    journal_root = tmp_path / "synthetic-journal"
    journal_path = journal_root / "options.sqlite"
    buffer = RawMessageBuffer(
        BufferLimits(2048, 32 * 1024 * 1024, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )
    admissions = {
        "O:SPY261120C00500000": InstrumentAdmission(
            1001,
            "O:SPY261120C00500000",
            "SPY",
            AssetClass.OPTION,
            0,
        ),
        "O:QQQ261120C00500000": InstrumentAdmission(
            1002,
            "O:QQQ261120C00500000",
            "QQQ",
            AssetClass.OPTION,
            0,
        ),
    }
    pump = AdvancedOptionsJournalPump(
        buffer,
        journal_path,
        generation,
        admissions,
        maximum_batch_messages=250,
        maximum_batch_bytes=4 * 1024 * 1024,
    )
    base_ms = int(START.timestamp() * 1000)
    payload_bytes = 0
    pump.start()
    for index in range(50):
        rows = []
        for contract_index, contract in enumerate(admissions):
            price = 5.0 + contract_index
            rows.extend(
                (
                    {
                        "ev": "Q",
                        "sym": contract,
                        "bx": 301,
                        "ax": 302,
                        "bp": price - 0.1,
                        "ap": price,
                        "bs": 10,
                        "as": 12,
                        "t": base_ms + index - 1,
                        "q": index + 1,
                    },
                    {
                        "ev": "T",
                        "sym": contract,
                        "x": 302 + (index + contract_index) % 3,
                        "p": price,
                        "s": 100,
                        "c": [209],
                        "t": base_ms + index,
                        "q": index + 1,
                    },
                )
            )
        payload = json.dumps(rows, separators=(",", ":")).encode("ascii")
        payload_bytes += len(payload)
        buffer.put_nowait(
            RawMessageEnvelope(
                session_ids[0],
                index,
                MarketFeed.OPTIONS,
                START,
                time.monotonic_ns(),
                payload,
            )
        )
    pump.request_stop()
    progress = pump.join(30)
    capture = {
        "journal_generation": str(generation),
        "journal_root": str(journal_root),
        "journal_path": str(journal_path),
        "session_ids": [str(session_ids[0])],
        "sessions": [
            {
                "attempt": 1,
                "stream_session_id": str(session_ids[0]),
                "started_at": START.isoformat(),
                "ended_at": (START + timedelta(minutes=5)).isoformat(),
                "received_frames": 50,
                "received_bytes": payload_bytes,
                "accepted_frames": 50,
                "raw_offset_end": 50,
            },
            {
                "attempt": 2,
                "stream_session_id": str(session_ids[0]),
                "started_at": (START + timedelta(minutes=5)).isoformat(),
                "ended_at": (START + timedelta(minutes=10)).isoformat(),
                "received_frames": 0,
                "received_bytes": 0,
                "accepted_frames": 0,
                "raw_offset_end": 50,
            },
        ],
        "progress": asdict(progress),
    }

    report = run_disposable_database_phase(plan, capture)

    assert report["status"] == "PASS_DATABASE"
    assert report["events"] > 0
    assert report["clusters"] > 0
    assert report["volume_rows"] == 2
    assert report["checkpoint_rows"] == 9
    assert report["replay_identical"]
    assert report["cleanup_verified"]
    assert report["database_absent"]
    assert report["roles_absent"]
