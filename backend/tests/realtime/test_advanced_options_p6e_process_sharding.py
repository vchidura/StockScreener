from __future__ import annotations

import multiprocessing
import json
from dataclasses import replace
from datetime import date, datetime, timezone
from uuid import UUID

from option_flow_support import advanced_contract_scope, advanced_policy, policy
from realtime.advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
)
from realtime.contracts import canonical_json
from realtime.option_flow.conditions import load_default_live_semantics
from realtime.option_flow.engine import EngineConfig, OptionFlowEngine
from realtime.option_flow.records import EngineOutput
from realtime.option_flow.volume import BaselineKind, VolumeConfig
from realtime.option_source_flow import build_option_source_flow_identity


UTC = timezone.utc
SESSION = date(2026, 10, 16)
T0 = int(datetime(2026, 10, 16, 14, 30, tzinfo=UTC).timestamp() * 1000)
COHORT = UUID("11111111-2222-3333-4444-555555555555")
GENERATION = UUID("66666666-7777-8888-9999-000000000000")
STREAMS = (
    UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
    UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
)
CONTRACTS = {
    "SPY": "O:SPY261120C00500000",
    "QQQ": "O:QQQ261120C00500000",
}


def _engine():
    semantics = load_default_live_semantics()
    detector_policy = policy(
        sweep_window_seconds=5,
        minimum_sweep_prints=3,
    )
    advanced = advanced_policy()
    scope = advanced_contract_scope(
        tuple(CONTRACTS.values()),
        session_date=SESSION,
        flow_policy=advanced,
    )
    exchange = load_default_options_exchange_reference()
    base = EngineConfig(
        cohort_id=COHORT,
        provider_tier="MASSIVE_ADVANCED",
        policy=detector_policy,
        semantics=semantics,
        journal_generation=GENERATION,
        advanced_flow_policy=advanced,
        contract_scope=scope,
        exchange_reference=exchange,
        volume=VolumeConfig(
            initial_baseline=BaselineKind.SESSION_COMPLETE,
        ),
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=GENERATION,
        stream_session_ids=STREAMS,
        condition_semantics_sha256=semantics.semantics_sha256,
        flow_policy_sha256=base.policy_sha256(),
        advanced_flow_policy_sha256=advanced.sha256,
        contract_scope_sha256=scope.sha256,
        provider_contract_sha256=MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        condition_reference_sha256=semantics.pin.payload_sha256,
        exchange_reference_sha256=exchange.sha256,
    )
    return OptionFlowEngine(replace(base, source_flow=source_flow))


def _messages(underlyer):
    contract = CONTRACTS[underlyer]
    base_offset = 0 if underlyer == "SPY" else 100
    rows = [(
        base_offset + 1,
        {
            "ev": "Q",
            "sym": contract,
            "bx": 301,
            "ax": 302,
            "bp": 4.9,
            "ap": 5.0,
            "bs": 10,
            "as": 12,
            "t": T0 - 1,
            "q": 1,
        },
    )]
    rows.extend(
        (
            base_offset + index + 1,
            {
                "ev": "T",
                "sym": contract,
                "x": 302 + index % 3,
                "p": 5.0,
                "s": 100,
                "c": [209],
                "t": T0 + index,
                "q": index,
            },
        )
        for index in range(1, 41)
    )
    return tuple(rows)


def _drive(rows):
    engine = _engine()
    output = EngineOutput()
    for offset, message in rows:
        output.extend(engine.feed(message, offset))
    output.extend(engine.flush(final=True))
    return {
        "underlyers": sorted({
            row.underlyer for row in output.events
        }),
        "events": canonical_json(tuple(sorted(
            output.events,
            key=lambda row: (row.event_time, str(row.event_id)),
        ))),
        "clusters": canonical_json(tuple(sorted(
            output.clusters,
            key=lambda row: (
                row.contract_ticker,
                row.window_seconds,
                row.window_start,
                row.revision,
            ),
        ))),
        "volume": canonical_json(tuple(sorted(
            output.checkpoints,
            key=lambda row: row.contract_ticker,
        ))),
        "source_flow_sha256": engine.config.source_flow.source_flow_sha256,
    }


def _worker(underlyer):
    result = _drive(_messages(underlyer))
    contract = CONTRACTS[underlyer]
    result["subscriptions"] = (
        f"T.{contract}",
        f"Q.{contract}",
    )
    return result


def test_disjoint_processes_merge_to_single_owner_reference():
    context = multiprocessing.get_context("spawn")
    with context.Pool(2) as pool:
        sharded = pool.map(_worker, ("SPY", "QQQ"))
    reference = _drive(tuple(sorted(
        (*_messages("SPY"), *_messages("QQQ")),
        key=lambda row: row[0],
    )))

    assert [row["underlyers"] for row in sharded] == [["SPY"], ["QQQ"]]
    assert all(
        "*" not in subscription
        for row in sharded
        for subscription in row["subscriptions"]
    )
    assert len({
        row["source_flow_sha256"] for row in sharded
    } | {reference["source_flow_sha256"]}) == 1
    merged = {
        "events": canonical_json(tuple(sorted(
            (
                event
                for row in sharded
                for event in json.loads(row["events"])
            ),
            key=lambda row: (row["event_time"], row["event_id"]),
        ))),
        "clusters": canonical_json(tuple(sorted(
            (
                cluster
                for row in sharded
                for cluster in json.loads(row["clusters"])
            ),
            key=lambda row: (
                row["contract_ticker"],
                row["window_seconds"],
                row["window_start"],
                row["revision"],
            ),
        ))),
        "volume": canonical_json(tuple(sorted(
            (
                checkpoint
                for row in sharded
                for checkpoint in json.loads(row["volume"])
            ),
            key=lambda row: row["contract_ticker"],
        ))),
    }
    assert merged["events"] == reference["events"]
    assert merged["clusters"] == reference["clusters"]
    assert merged["volume"] == reference["volume"]
