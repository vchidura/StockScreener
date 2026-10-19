import json
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest

from test_advanced_options_control_plane import _plan as _control_plane
from realtime.advanced_ingress_plan import (
    ADVANCED_OPTIONS_INGRESS_CHANNEL,
    ADVANCED_OPTIONS_INGRESS_FEED,
    AdvancedIngressBudgets,
    build_advanced_stream_authorization,
)
from realtime.advanced_ingress_runtime_profile import (
    load_advanced_ingress_runtime_catalog,
)
from realtime.advanced_options_ingress import (
    AdvancedOptionsJournalPump,
    AdvancedOptionsJournalSource,
    build_advanced_options_controller_plan,
    build_advanced_options_journal_registration,
)
from realtime.advanced_equity_ingress import AdvancedJournalDecodeError
from realtime.buffer import BufferLimits, RawMessageBuffer
from realtime.contracts import (
    AssetClass,
    MarketFeed,
    RawMessageEnvelope,
)
from realtime.journal import RawJournalReader
from realtime.normalizer import InstrumentAdmission, NORMALIZER_VERSION


UTC = timezone.utc
NOW = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
OPTION = "O:SPY261218C00500000"
OTHER_STREAM = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
RUNTIME_POLICY = (
    Path(__file__).resolve().parents[2]
    / "realtime"
    / "policies"
    / "advanced_equity_ingress_v1.json"
)


def _buffer():
    return RawMessageBuffer(
        BufferLimits(8, 64 * 1024, 60_000_000_000),
        monotonic_ns=time.monotonic_ns,
    )


def _admissions():
    return {
        OPTION: InstrumentAdmission(
            101,
            OPTION,
            "SPY",
            AssetClass.OPTION,
            0,
        )
    }


def _payload():
    timestamp = int(NOW.timestamp() * 1000)
    return json.dumps([
        {
            "ev": "T",
            "sym": OPTION,
            "p": 5.0,
            "s": 10,
            "x": 302,
            "t": timestamp,
            "q": 1,
        },
        {
            "ev": "Q",
            "sym": OPTION,
            "bp": 4.9,
            "ap": 5.0,
            "bs": 10,
            "as": 12,
            "t": timestamp,
            "q": 2,
        },
    ]).encode("ascii")


def _authorization():
    return build_advanced_stream_authorization(
        parent_authorization_id=None,
        allowed_universe_targets=(2,),
        valid_from=NOW.date(),
        valid_through=NOW.date(),
        maximum_budgets=AdvancedIngressBudgets(
            1,
            3,
            100_000_000,
            50,
            50_000_000,
        ),
        runtime_policy_sha256="a" * 64,
        approval_source="operator",
        approval_reference="advanced-options-p3a",
        feed=ADVANCED_OPTIONS_INGRESS_FEED,
        channel=ADVANCED_OPTIONS_INGRESS_CHANNEL,
    )


def _runtime_profile():
    equity = load_advanced_ingress_runtime_catalog(
        RUNTIME_POLICY
    ).profile_for_target(1)
    return replace(
        equity,
        version="advanced-options-ingress-2-v1",
        universe_target=2,
        feed="OPTIONS",
        channels=("T", "Q"),
    )


def _write_options_journal(tmp_path):
    control = _control_plane()
    stream_session_id = control.source_flow.stream_session_ids[0]
    buffer = _buffer()
    buffer.put_nowait(RawMessageEnvelope(
        stream_session_id,
        0,
        MarketFeed.OPTIONS,
        NOW,
        time.monotonic_ns(),
        _payload(),
    ))
    path = tmp_path / "mounted" / "options.sqlite"
    pump = AdvancedOptionsJournalPump(
        buffer,
        path,
        control.source_flow.journal_generation,
        _admissions(),
        maximum_batch_messages=4,
        maximum_batch_bytes=64 * 1024,
    )
    pump.start()
    pump.request_stop()
    progress = pump.join(5)
    return control, stream_session_id, path, progress


def test_options_journal_pump_retains_exact_tq_raw_and_normalized_rows(
    tmp_path,
):
    control, _stream, path, progress = _write_options_journal(tmp_path)

    reader = RawJournalReader(path)
    raw = reader.read_after(0)
    normalized = reader.read_normalized_after(0)
    assert progress.admitted_market_messages == 1
    assert progress.last_raw_offset == 1
    assert {record.event_kind.value for record in normalized} == {"T", "Q"}
    assert all(record.asset_class is AssetClass.OPTION for record in normalized)
    assert raw[0].feed is MarketFeed.OPTIONS
    assert raw[0].generation == control.source_flow.journal_generation


def test_options_journal_pump_rejects_wrong_asset_and_feed_scope(tmp_path):
    control = _control_plane()
    with pytest.raises(ValueError, match="bounds are invalid"):
        AdvancedOptionsJournalPump(
            _buffer(),
            tmp_path / "wrong-asset.sqlite",
            control.source_flow.journal_generation,
            {
                "SPY": InstrumentAdmission(
                    1,
                    "SPY",
                    "SPY",
                    AssetClass.EQUITY,
                    0,
                )
            },
            maximum_batch_messages=4,
            maximum_batch_bytes=64 * 1024,
        )

    buffer = _buffer()
    buffer.put_nowait(RawMessageEnvelope(
        control.source_flow.stream_session_ids[0],
        0,
        MarketFeed.STOCKS,
        NOW,
        time.monotonic_ns(),
        _payload(),
    ))
    pump = AdvancedOptionsJournalPump(
        buffer,
        tmp_path / "wrong-feed.sqlite",
        control.source_flow.journal_generation,
        _admissions(),
        maximum_batch_messages=4,
        maximum_batch_bytes=64 * 1024,
    )
    pump.start()
    pump.request_stop()
    with pytest.raises(
        AdvancedJournalDecodeError,
        match="FEED_SCOPE_MISMATCH",
    ):
        pump.join(5)


def test_controller_plan_is_deterministic_inert_and_exactly_registered(
    tmp_path,
    monkeypatch,
):
    control = _control_plane()
    root = tmp_path / "mounted"
    root.mkdir()
    path = root / "options.sqlite"
    registration = build_advanced_options_journal_registration(
        control_plane=control,
        root=root,
        path=path,
        stream_session_id=control.source_flow.stream_session_ids[0],
        owner_epoch=1,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256="f" * 64,
    )
    authorization = _authorization()
    runtime_profile = _runtime_profile()

    def reject_effect(*_args, **_kwargs):
        raise AssertionError("controller plan attempted a runtime effect")

    monkeypatch.setattr("socket.socket", reject_effect)
    monkeypatch.setattr("subprocess.Popen", reject_effect)
    first = build_advanced_options_controller_plan(
        control_plane=control,
        authorization=authorization,
        runtime_profile=runtime_profile,
        journal_registration=registration,
    )
    repeated = build_advanced_options_controller_plan(
        control_plane=control,
        authorization=authorization,
        runtime_profile=runtime_profile,
        journal_registration=registration,
    )

    assert first == repeated
    assert not first.can_start
    assert not first.can_publish
    assert not first.runtime_authorized
    assert not first.service_start_authorized
    assert not first.publication_authorized
    assert first.source_flow_id == control.source_flow.source_flow_id
    assert registration.plan_id == control.plan_id
    assert registration.status == "REGISTERED"


def test_controller_plan_rejects_non_options_authorization(tmp_path):
    control = _control_plane()
    root = tmp_path / "mounted"
    root.mkdir()
    registration = build_advanced_options_journal_registration(
        control_plane=control,
        root=root,
        path=root / "options.sqlite",
        stream_session_id=control.source_flow.stream_session_ids[0],
        owner_epoch=1,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256="f" * 64,
    )
    stocks = build_advanced_stream_authorization(
        parent_authorization_id=None,
        allowed_universe_targets=(2,),
        valid_from=NOW.date(),
        valid_through=NOW.date(),
        maximum_budgets=AdvancedIngressBudgets(
            1,
            3,
            100_000_000,
            50,
            50_000_000,
        ),
        runtime_policy_sha256="a" * 64,
        approval_source="operator",
        approval_reference="advanced-stocks-p3a",
    )

    with pytest.raises(ValueError, match="exact OPTIONS T/Q"):
        build_advanced_options_controller_plan(
            control_plane=control,
            authorization=stocks,
            runtime_profile=_runtime_profile(),
            journal_registration=registration,
        )


def test_registered_options_source_is_read_only_and_identity_checked(tmp_path):
    control, stream, path, progress = _write_options_journal(tmp_path)
    root = path.parent
    registration = build_advanced_options_journal_registration(
        control_plane=control,
        root=root,
        path=path,
        stream_session_id=stream,
        owner_epoch=1,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256="f" * 64,
    )
    registration = replace(
        registration,
        status="DRAINING",
        raw_committed_offset=progress.last_raw_offset,
        normalized_committed_offset=progress.last_raw_offset,
    )
    source = AdvancedOptionsJournalSource(
        root=root,
        registration=registration,
        source_flow=control.source_flow,
    )

    records = source.read_after(0)
    assert len(records) == 1
    assert source.generation == control.source_flow.journal_generation
    assert source.last_offset() == progress.last_raw_offset

    class WrongFeedReader:
        def __init__(self, _path):
            pass

        def read_after(self, _offset, *, limit):
            assert limit == 1000
            return (replace(records[0], feed=MarketFeed.STOCKS),)

    wrong = AdvancedOptionsJournalSource(
        root=root,
        registration=registration,
        source_flow=control.source_flow,
        reader_factory=WrongFeedReader,
    )
    with pytest.raises(ValueError, match="identity mismatch"):
        wrong.read_after(0)


def test_source_rejects_unregistered_or_wrong_session_identity(tmp_path):
    control = _control_plane()
    root = tmp_path / "mounted"
    root.mkdir()
    registration = build_advanced_options_journal_registration(
        control_plane=control,
        root=root,
        path=root / "options.sqlite",
        stream_session_id=control.source_flow.stream_session_ids[0],
        owner_epoch=1,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256="f" * 64,
    )
    with pytest.raises(ValueError, match="not admitted"):
        AdvancedOptionsJournalSource(
            root=root,
            registration=registration,
            source_flow=control.source_flow,
        )
    with pytest.raises(ValueError, match="source-flow identity"):
        AdvancedOptionsJournalSource(
            root=root,
            registration=replace(
                registration,
                status="WRITING",
                stream_session_id=OTHER_STREAM,
            ),
            source_flow=control.source_flow,
        )
