from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import time
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from random import Random
from types import SimpleNamespace
from uuid import UUID

import pytest

import realtime.advanced_options_p7_executor as p7_executor
from realtime.advanced_options_ingress import AdvancedOptionsJournalPump
from realtime.advanced_options_p7_executor import (
    _read_bounded_journal,
    _reconnect_backoff_seconds,
    _subscription_manifest,
    _wait_for_reconnect_backoff,
    execute_shadow,
)
from realtime.advanced_options_p7_executor import (
    run_disposable_database_phase,
)
from realtime.advanced_options_p7_shadow import (
    APPROVAL_VERSION,
    OWNER_DECISION,
    build_authorization,
)
from realtime.advanced_options_p7_selection import (
    P7TransportCandidate,
    p7_transport_selection_payload,
    select_p7_transport_contracts,
)
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
    spy = "O:SPY261020C00100000"
    qqq = "O:QQQ261020P00100000"
    candidates = (
        P7TransportCandidate(
            spy,
            "SPY",
            START.date(),
            "C",
            Decimal("100"),
            Decimal("100"),
            Decimal("1.00"),
            Decimal("1.10"),
            500,
            1_000,
            0,
            START - timedelta(minutes=1),
            "1" * 64,
        ),
        P7TransportCandidate(
            qqq,
            "QQQ",
            START.date(),
            "P",
            Decimal("100"),
            Decimal("100"),
            Decimal("1.00"),
            Decimal("1.10"),
            600,
            1_200,
            0,
            START - timedelta(minutes=1),
            "2" * 64,
        ),
    )
    return build_authorization(
        run_id=RUN_ID,
        authorization_reference=(
            "ADVANCED-OPTIONS-P7-SHADOW-20261020-TEST"
        ),
        baseline_head=HEAD,
        start_at=START,
        controlled_reconnect_at=START + timedelta(minutes=5),
        stop_at=START + timedelta(minutes=10),
        spy_contract=spy,
        qqq_contract=qqq,
        contract_selection=p7_transport_selection_payload(
            select_p7_transport_contracts(
                candidates,
                reviewed_at=START - timedelta(seconds=30),
            )
        ),
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
                "error_detail": None,
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
            "O:SPY261020C00100000|Q": 1,
            "O:SPY261020C00100000|T": 1,
            "O:QQQ261020P00100000|Q": 1,
            "O:QQQ261020P00100000|T": 1,
        },
        "provider_status_counts": {},
        "provider_messages": [],
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


def test_p7_subscription_manifest_is_options_only(tmp_path):
    plan = _plan(tmp_path)

    manifest = _subscription_manifest(plan)

    assert manifest.universe.equity_tickers == ("QQQ", "SPY")
    assert manifest.universe.option_underlyings == ("QQQ", "SPY")
    assert {
        (member.underlying, member.channel.value)
        for member in manifest.members
    } == {
        ("SPY", "T"),
        ("SPY", "Q"),
        ("QQQ", "T"),
        ("QQQ", "Q"),
    }


def test_p7_journal_reads_chunk_at_the_reader_limit():
    class Reader:
        def __init__(self, count, offset_name):
            self.rows = tuple(
                SimpleNamespace(**{offset_name: index})
                for index in range(1, count + 1)
            )
            self.limits = []
            self.offset_name = offset_name

        def _read(self, offset, *, limit):
            assert 1 <= limit <= 10_000
            self.limits.append(limit)
            return tuple(
                row for row in self.rows
                if getattr(row, self.offset_name) > offset
            )[:limit]

        def read_after(self, offset, *, limit):
            return self._read(offset, limit=limit)

        def read_normalized_after(self, offset, *, limit):
            return self._read(offset, limit=limit)

    raw_reader = Reader(25_001, "raw_offset")
    normalized_reader = Reader(30_001, "event_offset")

    raw = _read_bounded_journal(
        raw_reader,
        maximum_records=25_001,
        normalized=False,
    )
    normalized = _read_bounded_journal(
        normalized_reader,
        maximum_records=30_001,
        normalized=True,
    )

    assert len(raw) == 25_001
    assert raw_reader.limits == [10_000, 10_000, 5_001]
    assert len(normalized) == 30_001
    assert normalized_reader.limits == [10_000, 10_000, 10_000, 1]


def test_reconnect_backoff_is_bounded_before_next_attempt():
    waited = []

    async def sleep(seconds):
        waited.append(seconds)

    asyncio.run(
        _wait_for_reconnect_backoff(
            5,
            deadline=START + timedelta(seconds=6),
            now=lambda: START,
            sleep=sleep,
        )
    )

    assert waited == [5]

    with pytest.raises(
        RuntimeError,
        match="reconnect backoff leaves no capture time",
    ):
        asyncio.run(
            _wait_for_reconnect_backoff(
                5,
                deadline=START + timedelta(seconds=5),
                now=lambda: START,
                sleep=sleep,
            )
        )

    assert waited == [5]


def test_reconnect_backoff_policy_is_manifest_bounded(tmp_path):
    plan = _plan(tmp_path)
    delays = [
        _reconnect_backoff_seconds(
            plan,
            reconnect_number,
            random_source=Random(reconnect_number),
        )
        for reconnect_number in range(1, 6)
    ]

    assert delays[0] == 5
    assert all(delay >= 5 for delay in delays)
    assert all(
        delay <= ceiling
        for delay, ceiling in zip(delays, (5, 10, 20, 40, 60))
    )


def test_capture_waits_for_backoff_before_second_attempt(
    tmp_path,
    monkeypatch,
):
    events = []

    async def run_attempt(_gateway, _pump, _buffer, *, deadline, **_kwargs):
        events.append(("attempt", deadline))
        return {
            "termination": "DEADLINE",
            "received_frames": 0,
            "received_bytes": 0,
            "provider_statuses": [],
            "provider_messages": [],
        }

    async def wait_for_backoff(seconds, *, deadline):
        events.append(("wait", seconds, deadline))

    monkeypatch.setattr(p7_executor, "_run_attempt", run_attempt)
    monkeypatch.setattr(
        p7_executor,
        "_reconnect_backoff_seconds",
        lambda *_args, **_kwargs: 5.0,
    )
    monkeypatch.setattr(
        p7_executor,
        "_wait_for_reconnect_backoff",
        wait_for_backoff,
    )

    result = asyncio.run(
        p7_executor.capture_live_shadow(_plan(tmp_path), "test-secret")
    )

    reconnect_at = START + timedelta(minutes=5)
    stop_at = START + timedelta(minutes=10)
    assert events == [
        ("attempt", reconnect_at),
        ("wait", 5, stop_at),
        ("attempt", stop_at),
    ]
    assert [
        row["reconnect_backoff_seconds_before_attempt"]
        for row in result["sessions"]
    ] == [0.0, 5.0]
    assert len(result["sessions"]) == 2


def test_capture_exhausts_only_the_authorized_transient_attempts(
    tmp_path,
    monkeypatch,
):
    attempt_count = 0

    async def run_attempt(_gateway, _pump, _buffer, **_kwargs):
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            return {
                "termination": "DEADLINE",
                "received_frames": 0,
                "received_bytes": 0,
                "provider_statuses": [],
                "provider_messages": [],
            }
        return {
            "termination": "PROVIDER_DISCONNECT",
            "received_frames": 2,
            "received_bytes": 313,
            "failure_disposition": "TRANSIENT",
            "provider_statuses": ["max_connections"],
            "provider_messages": [
                "Maximum number of websocket connections exceeded."
            ],
        }

    async def wait_for_backoff(_seconds, *, deadline):
        assert deadline == START + timedelta(minutes=10)

    monkeypatch.setattr(p7_executor, "_run_attempt", run_attempt)
    monkeypatch.setattr(
        p7_executor,
        "_reconnect_backoff_seconds",
        lambda *_args, **_kwargs: 5.0,
    )
    monkeypatch.setattr(
        p7_executor,
        "_wait_for_reconnect_backoff",
        wait_for_backoff,
    )

    result = asyncio.run(
        p7_executor.capture_live_shadow(_plan(tmp_path), "test-secret")
    )

    assert len(result["sessions"]) == 6
    assert result["blockers"][:2] == [
        "FINAL_PROVIDER_DISCONNECT",
        "RECONNECT_ATTEMPT_BUDGET_EXHAUSTED",
    ]


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
                    APPROVAL_VERSION
                ),
                "manifest_id": plan["manifest_id"],
                "manifest_sha256": hashlib.sha256(
                    manifest.read_bytes()
                ).hexdigest(),
                "authorization_reference": plan[
                    "authorization_reference"
                ],
                "owner_decision": (
                    OWNER_DECISION
                ),
                "conditional_live_shadow_authorized": True,
                "provider_websocket_connection_attempts_authorized": 6,
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

    with pytest.raises(
        RuntimeError,
        match="provider socket lease ownership changed",
    ):
        script.main(
            [
                "--manifest",
                str(manifest),
                "--approval",
                str(approval),
                "--execute",
            ],
            provider_lease=SimpleNamespace(
                acquired=False,
                resource_key=plan["provider"][
                    "connection_lease_resource_key"
                ],
                owner_id=RUN_ID,
            ),
        )


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
        "O:SPY261020C00100000": InstrumentAdmission(
            1001,
            "O:SPY261020C00100000",
            "SPY",
            AssetClass.OPTION,
            0,
        ),
        "O:QQQ261020P00100000": InstrumentAdmission(
            1002,
            "O:QQQ261020P00100000",
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
