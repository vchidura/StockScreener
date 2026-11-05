from __future__ import annotations

import asyncio
import json
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime import advanced_options_acquisition_runtime as runtime
from realtime import advanced_options_p8_runtime as p8_runtime
from realtime.advanced_options_acquisition_runtime import (
    acquisition_binding_from_payload,
    acquisition_binding_payload,
    acquisition_health,
    build_acquisition_binding,
    run_acquisition,
)
from realtime.advanced_gateway import GatewayControlError, GatewayFeedState
from realtime.advanced_options_p8_runtime import (
    LegacyArtifactSessionStateAdapter,
    P8DownstreamBindingStore,
    P8ExecutionBundleStore,
    build_p8_downstream_binding,
    build_p8_preopen_session_binding,
    p8_downstream_binding_payload,
    p8_preopen_session_binding_from_payload,
    p8_preopen_session_binding_payload,
    validate_p8_execution_bundle,
)
from realtime.advanced_options_v3_authority import (
    SessionAcquisitionAuthorization,
    session_acquisition_authorization_payload,
)
from realtime.contracts import (
    AssetClass,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from realtime.normalizer import InstrumentAdmission
from realtime.advanced_options_selection_c6 import (
    C6SelectionPlan,
    C6SubscriptionShard,
)
from scripts.run_advanced_options_v3_acquisition import (
    _FixtureConnection,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 28, 13, 10, tzinfo=UTC)
CONTRACTS = (
    "O:QQQ261028C00600000",
    "O:QQQ261028P00600000",
    "O:SPY261028C00670000",
    "O:SPY261028P00670000",
)


def _authority():
    values = {
        "authorization_reference": "acquisition-runtime-test",
        "cohort_id": UUID("11111111-2222-3333-4444-555555555555"),
        "session_date": date(2026, 10, 28),
        "prepared_at": NOW - timedelta(minutes=20),
        "acquisition_start": NOW,
        "capture_boundary": NOW + timedelta(minutes=20),
        "acquisition_stop": NOW + timedelta(hours=7),
        "admission_id": UUID(
            "22222222-3333-4444-5555-666666666666"
        ),
        "admission_sha256": "a" * 64,
        "readiness_sha256": "b" * 64,
        "slot_set_member_sha256": "c" * 64,
        "provider_budget_sha256": "d" * 64,
        "socket_budget_sha256": "e" * 64,
        "journal_generation": UUID(
            "33333333-4444-5555-6666-777777777777"
        ),
        "journal_identity_sha256": "f" * 64,
    }
    unsigned = SessionAcquisitionAuthorization.__new__(
        SessionAcquisitionAuthorization
    )
    object.__setattr__(unsigned, "authorization_id", UUID(int=1))
    object.__setattr__(unsigned, "authorization_sha256", "0" * 64)
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        session_acquisition_authorization_payload(
            unsigned,
            include_identity=False,
        )
    )
    return SessionAcquisitionAuthorization(
        authorization_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:options-session-acquisition:{digest}",
        ),
        authorization_sha256=digest,
        **values,
    )


def _binding():
    return build_acquisition_binding(
        source_sha256="1" * 64,
        backend_image_id=f"sha256:{'2' * 64}",
        authority=_authority(),
        contract_tickers=CONTRACTS,
        connection_attempt_ceiling=78,
        decoded_message_ceiling=18_874_386,
        decoded_byte_ceiling=18_874_386,
    )


def _scope(binding):
    admissions = {
        ticker: InstrumentAdmission(
            index,
            ticker,
            ticker[2:5],
            AssetClass.OPTION,
            0,
        )
        for index, ticker in enumerate(
            binding.contract_tickers,
            start=1,
        )
    }
    members = tuple(
        SubscriptionMember(
            AssetClass.OPTION,
            ticker,
            ticker[2:5],
            channel,
            catalog_version_id=index,
        )
        for index, ticker in enumerate(
            binding.contract_tickers,
            start=1,
        )
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    manifest = SubscriptionManifest(
        version="acquisition-test-v1",
        session_date=binding.authority.session_date,
        universe=UniverseManifest(
            "acquisition-test-v1",
            ("QQQ", "SPY"),
            ("QQQ", "SPY"),
        ),
        capability_profile_sha256="d" * 64,
        allocation_policy_sha256="c" * 64,
        members=members,
        maximum_option_quote_contracts=4,
    )
    return admissions, manifest


class _Unit:
    cursor = object()


class _Transaction:
    def __enter__(self):
        return _Unit()

    def __exit__(self, *_):
        return False


class _Provider:
    def transaction(self, *_args, **_kwargs):
        return _Transaction()


def test_catalog_scope_uses_persisted_logical_shards():
    binding = _binding()
    physical_id = UUID(
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    )
    shards = tuple(
        C6SubscriptionShard(
            shard_id=UUID(
                f"00000000-0000-0000-0000-00000000000{index + 1}"
            ),
            shard_slot=index + 1,
            physical_subscription_id=physical_id,
            contract_tickers=tuple(
                binding.contract_tickers[index * 2:index * 2 + 2]
            ),
            member_sha256=canonical_sha256({
                "contract_tickers": tuple(
                    binding.contract_tickers[
                        index * 2:index * 2 + 2
                    ]
                ),
            }),
        )
        for index in range(2)
    )
    plan = C6SelectionPlan.__new__(C6SelectionPlan)
    object.__setattr__(plan, "shards", shards)
    rows = [
        {
            "contract_ticker": ticker,
            "underlying_security_id": str(UUID(int=index + 1)),
            "version_id": 100 + index,
            "instrument_id": 200 + index,
        }
        for index, ticker in enumerate(binding.contract_tickers)
    ]

    class _Cursor:
        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return rows

    class _CatalogTransaction:
        def __enter__(self):
            return SimpleNamespace(cursor=_Cursor())

        def __exit__(self, *_args):
            return False

    class _CatalogProvider:
        def transaction(self, *_args, **_kwargs):
            return _CatalogTransaction()

    admissions, _manifest = runtime._catalog_scope(
        _CatalogProvider(),
        binding,
        plan,
    )

    assert {
        ticker: admission.shard_id
        for ticker, admission in admissions.items()
    } == {
        binding.contract_tickers[0]: 0,
        binding.contract_tickers[1]: 0,
        binding.contract_tickers[2]: 1,
        binding.contract_tickers[3]: 1,
    }


class _DisconnectConnection:
    def __init__(self):
        self.incoming = [
            b'[{"ev":"status","status":"auth_success"}]',
            b'[{"ev":"status","status":"success"}]',
        ]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def send(self, _payload):
        return None

    async def recv(self, *, decode=None):
        del decode
        if self.incoming:
            return self.incoming.pop(0)
        raise ConnectionError("provider-free disconnect")

    async def close(self, *, code, reason):
        del code, reason


class _RejectedConnection(_DisconnectConnection):
    def __init__(self):
        self.incoming = [
            b'[{"ev":"status","status":"auth_failed"}]',
        ]


def _install_runtime_fakes(monkeypatch, binding):
    monkeypatch.setattr(
        runtime,
        "_catalog_scope",
        lambda *_: _scope(binding),
    )
    monkeypatch.setattr(
        runtime,
        "persist_session_acquisition_authorization_cursor",
        lambda *_: None,
    )
    monkeypatch.setattr(
        runtime,
        "consume_session_acquisition_authority_cursor",
        lambda *_args, **_kwargs: None,
    )

    class _Lease:
        def __init__(self, *_args):
            pass

        def acquire(self, **_kwargs):
            pass

        def release(self, **_kwargs):
            pass

    monkeypatch.setattr(runtime, "HostProviderSocketLease", _Lease)


def test_acquisition_binding_has_no_equity_or_serving_authority():
    binding = _binding()
    payload = acquisition_binding_payload(binding)
    serialized = str(payload).lower()

    assert acquisition_binding_from_payload(payload) == binding
    assert payload["authority"]["provider_socket"] is True
    assert payload["authority"]["journal"] is True
    assert payload["authority"]["publication"] is False
    assert payload["membership"]["equity_target"] == 9
    assert payload["membership"]["equity_tickers"] == list(
        runtime.EQUITY_VALIDATION_UNIVERSE.equity_tickers
    )
    assert payload["membership"]["option_contract_tickers"] == list(
        binding.contract_tickers
    )
    for forbidden in (
        "equity_receipt",
        "equity_cutoff",
        "release_id",
        "window_cycle",
    ):
        assert forbidden not in serialized


def _waiting_package(binding):
    session_date = binding.authority.session_date.isoformat()
    cohort_id = str(binding.authority.cohort_id)
    prefix = f"advanced-options:{cohort_id}:{session_date}"
    package = {
        "package_sha256": "f" * 64,
        "package_nonce": "9c05aeab-e50d-4504-b7dc-ae55e85f8813",
        "window": {
            "session_date": session_date,
            "start_utc": binding.authority.acquisition_start.isoformat(),
            "stop_utc": binding.authority.acquisition_stop.isoformat(),
        },
        "cohort": {"cohort_id": cohort_id},
        "orchestration": {
            "plan_id": "2fef884f-18a1-5567-a319-fc4d5c558ec6",
        },
        "owners": {
            "processes": [
                {
                    "kind": "INGRESS",
                    "owner_name": "advanced-options-ingress-v1",
                    "lease_resource_key": "massive:options:websocket",
                },
                {
                    "kind": "MARKET_STATE",
                    "owner_name": "market-state-materializer-v1",
                    "lease_resource_key": f"{prefix}:market-state",
                },
                {
                    "kind": "FLOW",
                    "owner_name": "option_flow_stream_v1",
                    "lease_resource_key": f"{prefix}:flow",
                },
                {
                    "kind": "WINDOW",
                    "owner_name": "advanced-options-window-v1",
                    "lease_resource_key": f"{prefix}:window",
                },
            ],
        },
        "v3_admitted_contracts": list(binding.contract_tickers),
        "v3_authorities": {
            "session_acquisition": (
                session_acquisition_authorization_payload(
                    binding.authority
                )
            ),
        },
    }
    return package


def _production_binding(
    *,
    window_id,
    receipt_id,
    input_sha256,
    equity_cutoff,
    options_cutoff,
    history_sha256,
    equity_sha256,
):
    value = {
        "schema": "advanced-options-p8-production-binding-v3",
        "window_id": str(window_id),
        "control": {
            "receipt_inputs": {"cutoff": options_cutoff.isoformat()},
        },
        "history_binding": {"binding_sha256": history_sha256},
        "equity_input_binding": {
            "equity_receipt_id": str(receipt_id),
            "input_sha256": input_sha256,
            "equity_cutoff": equity_cutoff.isoformat(),
            "binding_sha256": equity_sha256,
        },
    }
    value["binding_sha256"] = canonical_sha256(value)
    return value


def _downstream_binding(
    session,
    sequence,
    *,
    market_index=None,
    previous_binding_sha256=None,
):
    market_index = sequence if market_index is None else market_index
    equity_cutoff = datetime(
        2026,
        10,
        29,
        13,
        30 + (market_index * 5),
        tzinfo=timezone.utc,
    )
    receipt_id = uuid5(NAMESPACE_URL, f"receipt:{market_index}")
    window_id = uuid5(NAMESPACE_URL, f"window:{market_index}")
    input_sha256 = f"{market_index:064x}"
    history_sha256 = "a" * 64
    equity_sha256 = f"{market_index + 10:064x}"
    options_cutoff = equity_cutoff + timedelta(seconds=120)
    return build_p8_downstream_binding(
        session_binding=session,
        sequence=sequence,
        previous_binding_sha256=previous_binding_sha256,
        target_window_id=window_id,
        equity_receipt_id=receipt_id,
        equity_input_sha256=input_sha256,
        finalized_cutoff=equity_cutoff,
        receipt_created_at=equity_cutoff + timedelta(seconds=30),
        options_cutoff=options_cutoff,
        history_binding_sha256=history_sha256,
        equity_binding_sha256=equity_sha256,
        options_activity_evidence_sha256=f"{market_index + 20:064x}",
        window_cycle_authorization_sha256=f"{market_index + 30:064x}",
        publication_authorization_reference=(
            f"owned-publication-{market_index}"
        ),
        production_binding=_production_binding(
            window_id=window_id,
            receipt_id=receipt_id,
            input_sha256=input_sha256,
            equity_cutoff=equity_cutoff,
            options_cutoff=options_cutoff,
            history_sha256=history_sha256,
            equity_sha256=equity_sha256,
        ),
    )


def test_waiting_session_attaches_three_e_bindings_and_rehydrates(
    tmp_path,
):
    acquisition = _binding()
    session = build_p8_preopen_session_binding(
        package=_waiting_package(acquisition),
        acquisition_binding=acquisition,
    )
    serialized = str(
        p8_preopen_session_binding_payload(session)
    ).lower()
    assert "ready_waiting_for_e" in serialized
    for forbidden in (
        "equity_receipt",
        "equity_cutoff",
        "target_window",
        "publication_authorization",
    ):
        assert forbidden not in serialized

    root = tmp_path / "downstream"
    store = P8DownstreamBindingStore(
        root,
        session_binding=session,
        create=True,
    )
    previous = None
    attached = []
    for sequence in range(1, 4):
        equity_cutoff = datetime(
            2026,
            10,
            29,
            13,
            30 + (sequence * 5),
            tzinfo=timezone.utc,
        )
        receipt_id = uuid5(
            NAMESPACE_URL,
            f"receipt:{sequence}",
        )
        window_id = uuid5(
            NAMESPACE_URL,
            f"window:{sequence}",
        )
        input_sha256 = f"{sequence:064x}"
        history_sha256 = "a" * 64
        equity_sha256 = f"{sequence + 10:064x}"
        options_cutoff = equity_cutoff + timedelta(seconds=120)
        binding = build_p8_downstream_binding(
            session_binding=session,
            sequence=sequence,
            previous_binding_sha256=previous,
            target_window_id=window_id,
            equity_receipt_id=receipt_id,
            equity_input_sha256=input_sha256,
            finalized_cutoff=equity_cutoff,
            receipt_created_at=equity_cutoff + timedelta(seconds=30),
            options_cutoff=options_cutoff,
            history_binding_sha256=history_sha256,
            equity_binding_sha256=equity_sha256,
            options_activity_evidence_sha256=f"{sequence + 20:064x}",
            window_cycle_authorization_sha256=(
                f"{sequence + 30:064x}"
            ),
            publication_authorization_reference=(
                f"owned-publication-{sequence}"
            ),
            production_binding=_production_binding(
                window_id=window_id,
                receipt_id=receipt_id,
                input_sha256=input_sha256,
                equity_cutoff=equity_cutoff,
                options_cutoff=options_cutoff,
                history_sha256=history_sha256,
                equity_sha256=equity_sha256,
            ),
        )
        attached.append(store.attach(binding))
        previous = binding.binding_sha256

    rehydrated = P8DownstreamBindingStore(
        root,
        session_binding=session,
    )
    assert rehydrated.bindings() == tuple(attached)
    assert rehydrated.latest() == attached[-1]
    divergent = dict(
        json.loads((root / "003.json").read_text(encoding="utf-8"))
    )
    divergent["equity_input_sha256"] = "e" * 64
    (root / "003.json").write_text(
        json.dumps(divergent),
        encoding="utf-8",
    )
    with pytest.raises(
        (RuntimeError, ValueError),
        match="binding",
    ):
        rehydrated.latest()


def test_downstream_append_sequence_does_not_require_market_window_ordinal(
    tmp_path,
):
    acquisition = _binding()
    session = build_p8_preopen_session_binding(
        package=_waiting_package(acquisition),
        acquisition_binding=acquisition,
    )
    store = P8DownstreamBindingStore(
        tmp_path / "downstream",
        session_binding=session,
        create=True,
    )
    first = _downstream_binding(session, 1, market_index=1)
    store.attach(first)
    third_window = _downstream_binding(session, 2, market_index=3)
    third_window = build_p8_downstream_binding(
        session_binding=session,
        sequence=2,
        previous_binding_sha256=first.binding_sha256,
        target_window_id=third_window.target_window_id,
        equity_receipt_id=third_window.equity_receipt_id,
        equity_input_sha256=third_window.equity_input_sha256,
        finalized_cutoff=third_window.finalized_cutoff,
        receipt_created_at=third_window.receipt_created_at,
        options_cutoff=third_window.options_cutoff,
        history_binding_sha256=third_window.history_binding_sha256,
        equity_binding_sha256=third_window.equity_binding_sha256,
        options_activity_evidence_sha256=(
            third_window.options_activity_evidence_sha256
        ),
        window_cycle_authorization_sha256=(
            third_window.window_cycle_authorization_sha256
        ),
        publication_authorization_reference=(
            third_window.publication_authorization_reference
        ),
        production_binding=third_window.production_binding,
    )

    attached = store.attach(third_window)

    assert attached.sequence == 2
    assert attached.target_window_id == uuid5(NAMESPACE_URL, "window:3")
    assert [item.sequence for item in store.bindings()] == [1, 2]


def test_downstream_binding_recovers_unpointed_atomic_tail(
    tmp_path,
    monkeypatch,
):
    acquisition = _binding()
    session = build_p8_preopen_session_binding(
        package=_waiting_package(acquisition),
        acquisition_binding=acquisition,
    )
    root = tmp_path / "downstream"
    store = P8DownstreamBindingStore(
        root,
        session_binding=session,
        create=True,
    )
    binding = _downstream_binding(session, 1)
    real_replace = p8_runtime.os.replace

    def interrupted_replace(source, destination):
        if Path(destination).name == "LATEST.json":
            raise OSError("injected pointer interruption")
        return real_replace(source, destination)

    monkeypatch.setattr(p8_runtime.os, "replace", interrupted_replace)
    with pytest.raises(OSError, match="pointer interruption"):
        store.attach(binding)

    assert (root / "001.json").is_file()
    assert (root / "LATEST.partial").is_file()
    assert P8DownstreamBindingStore(
        root,
        session_binding=session,
    ).latest() == binding

    monkeypatch.setattr(p8_runtime.os, "replace", real_replace)
    assert store.attach(binding) == binding
    assert (root / "LATEST.json").is_file()
    assert not (root / "LATEST.partial").exists()


def test_downstream_binding_discards_only_uncommitted_corrupt_partial(
    tmp_path,
):
    acquisition = _binding()
    session = build_p8_preopen_session_binding(
        package=_waiting_package(acquisition),
        acquisition_binding=acquisition,
    )
    root = tmp_path / "downstream"
    store = P8DownstreamBindingStore(
        root,
        session_binding=session,
        create=True,
    )
    (root / "001.json.partial").write_text("{truncated", encoding="utf-8")
    binding = _downstream_binding(session, 1)

    assert store.attach(binding) == binding
    assert not (root / "001.json.partial").exists()
    assert store.latest() == binding


def test_waiting_execution_bundle_is_receipt_free(tmp_path):
    acquisition = _binding()
    package = _waiting_package(acquisition)
    package_path = tmp_path / "package.json"
    bundle_path = tmp_path / "bundle.json"
    package_path.write_text(
        json.dumps(package),
        encoding="utf-8",
    )
    store = P8ExecutionBundleStore(
        bundle_path,
        package=package,
        package_path=package_path,
        repository_root=tmp_path,
    )

    written = store.write_waiting(
        acquisition,
        written_at=acquisition.authority.prepared_at,
    )
    loaded = validate_p8_execution_bundle(
        package_path,
        bundle_path,
    )

    assert loaded == written
    assert loaded["production_binding"] is None
    assert loaded["window_id"] is None
    assert loaded["v3_authority_references"] is None
    serialized = bundle_path.read_text(encoding="utf-8").lower()
    assert "ready_waiting_for_e" in serialized
    assert "equity_receipt" not in serialized
    assert "publication_authorization" not in serialized


def _legacy_session_artifacts(tmp_path):
    acquisition = _binding()
    package = _waiting_package(acquisition)
    package_path = tmp_path / "package.json"
    bundle_path = tmp_path / "bundle.json"
    acquisition_path = tmp_path / "acquisition.json"
    downstream_path = tmp_path / "downstream"
    package_path.write_text(json.dumps(package), encoding="utf-8")
    acquisition_path.write_text(
        json.dumps(acquisition_binding_payload(acquisition)),
        encoding="utf-8",
    )
    bundle = P8ExecutionBundleStore(
        bundle_path,
        package=package,
        package_path=package_path,
        repository_root=tmp_path,
    ).write_waiting(
        acquisition,
        written_at=acquisition.authority.prepared_at,
    )
    session = p8_preopen_session_binding_from_payload(
        bundle["session_binding"]
    )
    downstream = _downstream_binding(session, 1)
    P8DownstreamBindingStore(
        downstream_path,
        session_binding=session,
        create=True,
    ).attach(downstream)
    adapter = LegacyArtifactSessionStateAdapter(
        package_path=package_path,
        bundle_path=bundle_path,
        acquisition_binding_path=acquisition_path,
        downstream_binding_directory=downstream_path,
    )
    return (
        adapter,
        package_path,
        bundle_path,
        acquisition_path,
        downstream_path,
        acquisition,
        session,
        downstream,
    )


def test_legacy_session_state_adapter_preserves_typed_payloads_and_files(
    tmp_path,
):
    (
        adapter,
        package_path,
        bundle_path,
        acquisition_path,
        downstream_path,
        acquisition,
        session,
        downstream,
    ) = _legacy_session_artifacts(tmp_path)
    files = (
        package_path,
        bundle_path,
        acquisition_path,
        downstream_path / "001.json",
        downstream_path / "LATEST.json",
    )
    before = {path: path.read_bytes() for path in files}

    package = adapter.load_package()
    bundle_document = adapter.load_bundle_document()
    validated_bundle = adapter.load_execution_bundle()
    loaded_acquisition = adapter.load_acquisition_binding()
    loaded_session = adapter.load_preopen_session_binding(validated_bundle)
    loaded_downstream = adapter.load_downstream_bindings(session)

    assert package == json.loads(package_path.read_text(encoding="utf-8"))
    assert bundle_document == validated_bundle
    assert loaded_acquisition == acquisition
    assert loaded_session == session
    assert loaded_downstream == (downstream,)
    assert adapter.load_latest_downstream_binding(session) == downstream
    assert acquisition_binding_payload(loaded_acquisition) == json.loads(
        acquisition_path.read_text(encoding="utf-8")
    )
    assert p8_preopen_session_binding_payload(loaded_session) == (
        validated_bundle["session_binding"]
    )
    assert p8_downstream_binding_payload(loaded_downstream[0]) == json.loads(
        (downstream_path / "001.json").read_text(encoding="utf-8")
    )
    assert {path: path.read_bytes() for path in files} == before
    next_binding = _downstream_binding(
        session,
        2,
        previous_binding_sha256=downstream.binding_sha256,
    )
    assert adapter.attach_downstream_binding(
        session,
        next_binding,
    ) == next_binding
    assert adapter.load_downstream_bindings(session) == (
        downstream,
        next_binding,
    )


def test_legacy_session_state_adapter_preserves_parser_error(tmp_path):
    (
        adapter,
        _package_path,
        _bundle_path,
        acquisition_path,
        _downstream_path,
        _acquisition,
        _session,
        _downstream,
    ) = _legacy_session_artifacts(tmp_path)
    payload = json.loads(acquisition_path.read_text(encoding="utf-8"))
    payload["binding_sha256"] = "0" * 64
    acquisition_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError) as direct:
        acquisition_binding_from_payload(payload)
    with pytest.raises(ValueError) as adapted:
        adapter.load_acquisition_binding()

    assert str(adapted.value) == str(direct.value)


def test_legacy_session_state_adapter_preserves_chain_divergence(
    tmp_path,
):
    (
        adapter,
        _package_path,
        _bundle_path,
        _acquisition_path,
        downstream_path,
        _acquisition,
        session,
        _downstream,
    ) = _legacy_session_artifacts(tmp_path)
    divergent = _downstream_binding(session, 2)
    (downstream_path / "002.json").write_text(
        json.dumps(p8_downstream_binding_payload(divergent)),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError) as direct:
        P8DownstreamBindingStore(
            downstream_path,
            session_binding=session,
        ).bindings()
    with pytest.raises(RuntimeError) as adapted:
        adapter.load_downstream_bindings(session)

    assert str(adapted.value) == str(direct.value)


def test_acquisition_runtime_rehydrates_same_owner_and_journal(
    tmp_path,
    monkeypatch,
):
    binding = _binding()
    events = []
    monkeypatch.setattr(
        runtime,
        "_catalog_scope",
        lambda *_: _scope(binding),
    )
    monkeypatch.setattr(
        runtime,
        "persist_session_acquisition_authorization_cursor",
        lambda *_: events.append("persist"),
    )
    monkeypatch.setattr(
        runtime,
        "consume_session_acquisition_authority_cursor",
        lambda *_args, **_kwargs: events.append("consume"),
    )
    class _Lease:
        def __init__(self, *_args):
            pass

        def acquire(self, **_kwargs):
            events.append("lease")

        def release(self, **_kwargs):
            events.append("release")

    monkeypatch.setattr(runtime, "HostProviderSocketLease", _Lease)
    status_path = tmp_path / "status.json"
    journal_root = tmp_path / "journal"
    lock_path = tmp_path / "socket.lock"

    def execute():
        stop = threading.Event()
        timer = threading.Timer(0.2, stop.set)
        timer.start()
        try:
            return run_acquisition(
                binding,
                provider=_Provider(),
                api_key="provider-free",
                journal_root=journal_root,
                status_path=status_path,
                lock_path=lock_path,
                stop_event=stop,
                connection_factory=lambda _: _FixtureConnection(
                    binding
                ),
                clock=lambda: NOW,
                monotonic_ns=lambda: 100,
                diagnostic=True,
            )
        finally:
            timer.cancel()

    first = execute()
    second = execute()
    health = acquisition_health(
        status_path,
        binding,
        checked_at=NOW,
    )

    assert first["owner_id"] == second["owner_id"] == str(
        binding.owner_id
    )
    assert first["journal_generation"] == second[
        "journal_generation"
    ] == str(binding.authority.journal_generation)
    assert second["connection_attempts"] == 2
    assert second["last_raw_offset"] > first["last_raw_offset"]
    assert second["last_event_offset"] > first["last_event_offset"]
    assert second["journal_integrity"] == "ok"
    assert second["quarantine_count"] == 0
    assert set(second["forbidden_effects"].values()) == {0}
    assert health["status"] == "READY"
    assert events.index("persist") < events.index("lease")
    assert events.index("lease") < events.index("consume")


def test_quiet_subscribed_health_uses_process_heartbeat_not_market_activity(
    tmp_path,
):
    binding = _binding()
    current = [NOW]
    status_path = tmp_path / "status.json"
    publisher = runtime._AcquisitionStatusPublisher(
        status_path,
        binding,
        {
            "schema": runtime.ACQUISITION_STATUS_SCHEMA,
            "state": "CAPTURING",
            "binding_sha256": binding.binding_sha256,
            "owner_id": str(binding.owner_id),
            "journal_generation": str(
                binding.authority.journal_generation
            ),
            "connection_attempts": 1,
            "decoded_messages": 8,
            "decoded_bytes": 100,
            "last_raw_offset": 0,
            "last_event_offset": 0,
            "socket_state": GatewayFeedState.WARMING.value,
            "subscription_ready": True,
            "last_market_event_at_utc": None,
            "transport_gaps": [],
        },
        clock=lambda: current[0],
        interval_seconds=0.02,
    )
    publisher.start()
    try:
        current[0] = NOW + timedelta(seconds=31)
        threading.Event().wait(0.05)
        health = acquisition_health(
            status_path,
            binding,
            checked_at=current[0],
        )
    finally:
        publisher.stop()

    assert health["status"] == "READY"
    assert health["subscription_ready"] is True
    assert health["last_market_event_at_utc"] is None


def test_status_publisher_paces_hot_path_updates(monkeypatch, tmp_path):
    binding = _binding()
    writes = []
    monkeypatch.setattr(
        runtime,
        "_write_json",
        lambda _path, value: writes.append(dict(value)),
    )
    publisher = runtime._AcquisitionStatusPublisher(
        tmp_path / "status.json",
        binding,
        {"decoded_messages": 0, "decoded_bytes": 0},
        clock=lambda: NOW,
        interval_seconds=60,
    )
    publisher.start()
    for index in range(1000):
        publisher.update(
            decoded_messages=index + 1,
            decoded_bytes=(index + 1) * 100,
        )
    publisher.stop()

    assert len(writes) == 2
    assert writes[-1]["decoded_messages"] == 1000
    assert writes[-1]["decoded_bytes"] == 100_000


def test_status_publisher_surfaces_background_storage_failure(
    monkeypatch,
    tmp_path,
):
    binding = _binding()
    calls = []

    def write(_path, _value):
        calls.append(True)
        if len(calls) > 1:
            raise OSError("injected status storage failure")

    monkeypatch.setattr(runtime, "_write_json", write)
    publisher = runtime._AcquisitionStatusPublisher(
        tmp_path / "status.json",
        binding,
        {"decoded_messages": 0, "decoded_bytes": 0},
        clock=lambda: NOW,
        interval_seconds=0.01,
    )
    publisher.start()
    threading.Event().wait(0.03)

    assert publisher.failed() is True
    with pytest.raises(RuntimeError, match="status persistence failed"):
        publisher.stop()


def test_status_publisher_latches_synchronous_storage_failure(
    monkeypatch,
    tmp_path,
):
    binding = _binding()
    monkeypatch.setattr(
        runtime,
        "_write_json",
        lambda _path, _value: (_ for _ in ()).throw(
            OSError("injected synchronous status storage failure")
        ),
    )
    publisher = runtime._AcquisitionStatusPublisher(
        tmp_path / "status.json",
        binding,
        {"decoded_messages": 0, "decoded_bytes": 0},
        clock=lambda: NOW,
        interval_seconds=60,
    )

    with pytest.raises(OSError, match="synchronous status"):
        publisher.start()

    assert publisher.failed() is True
    with pytest.raises(RuntimeError, match="status persistence failed"):
        publisher.persist()


def test_crash_resume_burns_only_durable_reserved_budget():
    usage = runtime._resume_budget_usage({
        "decoded_messages": 100,
        "decoded_bytes": 1_000,
        "reserved_decoded_messages": 1124,
        "reserved_decoded_bytes": 4_195_304,
    })

    assert usage == (1124, 4_195_304, 1024, 4_194_304)


def test_acquisition_reconnects_transient_transport_and_records_gap(
    tmp_path,
    monkeypatch,
):
    binding = _binding()
    _install_runtime_fakes(monkeypatch, binding)
    calls = []

    def connection_factory(_configuration):
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            return _DisconnectConnection()
        return _FixtureConnection(binding, now=NOW)

    stop = threading.Event()
    timer = threading.Timer(1.0, stop.set)
    timer.start()
    try:
        report = run_acquisition(
            binding,
            provider=_Provider(),
            api_key="provider-free",
            journal_root=tmp_path / "journal",
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "socket.lock",
            stop_event=stop,
            connection_factory=connection_factory,
            clock=lambda: NOW,
            monotonic_ns=lambda: 100,
            diagnostic=True,
        )
    finally:
        timer.cancel()

    assert report["connection_attempts"] == 2
    assert report["transport_gaps"] == [{
        "attempt": 1,
        "status": "RECONNECTED",
        "disconnected_at_utc": NOW.isoformat(),
        "error_type": "ConnectionError",
        "reconnected_at_utc": NOW.isoformat(),
    }]
    assert report["last_market_event_at_utc"] == NOW.isoformat()


class _QuietFixtureConnection(_FixtureConnection):
    """The feed goes quiet after its frames, and the socket stays open."""

    async def recv(self, *, decode=None):
        if self.incoming:
            return await super().recv(decode=decode)
        await asyncio.sleep(3600)


def _run_until_stopped(
    binding,
    tmp_path,
    *,
    seconds=0.4,
    clock=lambda: NOW,
    on_timer=None,
):
    stop = threading.Event()

    def fire():
        if on_timer is not None:
            on_timer()
        stop.set()

    timer = threading.Timer(seconds, fire)
    timer.start()
    try:
        return run_acquisition(
            binding,
            provider=_Provider(),
            api_key="provider-free",
            journal_root=tmp_path / "journal",
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "socket.lock",
            stop_event=stop,
            connection_factory=lambda _: _QuietFixtureConnection(
                binding,
                now=NOW,
            ),
            clock=clock,
            monotonic_ns=lambda: 100,
        )
    finally:
        timer.cancel()


def test_stop_before_the_session_end_pauses_and_restart_continues(
    tmp_path,
    monkeypatch,
):
    binding = _binding()
    _install_runtime_fakes(monkeypatch, binding)
    status_path = tmp_path / "status.json"

    first = _run_until_stopped(binding, tmp_path)
    stored = json.loads(status_path.read_text(encoding="utf-8"))

    assert first["state"] == stored["state"] == "PAUSED"
    assert "semantic_sha256" not in stored
    assert first["connection_attempts"] == 1
    assert stored["subscription_ready"] is False
    assert stored["last_raw_offset"] > 0
    assert stored["reserved_decoded_messages"] == stored["decoded_messages"]
    assert stored["transport_gaps"][-1]["error_type"] == "STOP_REQUESTED"
    assert stored["transport_gaps"][-1]["status"] == "OPEN"
    assert acquisition_health(
        status_path,
        binding,
        checked_at=NOW,
    )["state"] == "PAUSED"

    second = _run_until_stopped(binding, tmp_path)

    assert second["state"] == "PAUSED"
    assert second["journal_generation"] == first["journal_generation"]
    assert second["owner_id"] == first["owner_id"]
    assert second["connection_attempts"] == 2
    assert second["decoded_messages"] > first["decoded_messages"]
    assert second["decoded_bytes"] > first["decoded_bytes"]
    assert second["last_raw_offset"] > first["last_raw_offset"]
    assert second["last_event_offset"] > first["last_event_offset"]
    assert second["stream_session_id"] != first["stream_session_id"]
    assert second["transport_gaps"][0]["status"] == "RECONNECTED"
    assert len(second["transport_gaps"]) == 2


def test_stop_with_no_market_data_yet_is_a_pause_not_a_failure(
    tmp_path,
    monkeypatch,
):
    binding = _binding()
    _install_runtime_fakes(monkeypatch, binding)

    class _Silent(_QuietFixtureConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.incoming = type(self.incoming)(
                list(self.incoming)[:2]
            )

    stop = threading.Event()
    timer = threading.Timer(0.3, stop.set)
    timer.start()
    try:
        report = run_acquisition(
            binding,
            provider=_Provider(),
            api_key="provider-free",
            journal_root=tmp_path / "journal",
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "socket.lock",
            stop_event=stop,
            connection_factory=lambda _: _Silent(binding, now=NOW),
            clock=lambda: NOW,
            monotonic_ns=lambda: 100,
        )
    finally:
        timer.cancel()

    assert report["state"] == "PAUSED"
    assert report["connection_attempts"] == 1


def test_reaching_the_authority_stop_still_completes(tmp_path, monkeypatch):
    binding = _binding()
    _install_runtime_fakes(monkeypatch, binding)
    current = [NOW]

    def reach_stop():
        current[0] = binding.authority.acquisition_stop

    report = _run_until_stopped(
        binding,
        tmp_path,
        clock=lambda: current[0],
        on_timer=reach_stop,
    )
    stored = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )

    assert report["state"] == stored["state"] == "COMPLETE"
    assert report["journal_integrity"] == "ok"
    assert report["socket_state"] == GatewayFeedState.STOPPED.value
    assert "semantic_sha256" in stored


def test_stop_on_the_last_connection_attempt_completes(tmp_path, monkeypatch):
    binding = build_acquisition_binding(
        source_sha256="1" * 64,
        backend_image_id=f"sha256:{'2' * 64}",
        authority=_authority(),
        contract_tickers=CONTRACTS,
        connection_attempt_ceiling=1,
        decoded_message_ceiling=18_874_386,
        decoded_byte_ceiling=18_874_386,
    )
    _install_runtime_fakes(monkeypatch, binding)

    report = _run_until_stopped(binding, tmp_path)

    assert report["state"] == "COMPLETE"
    assert report["connection_attempts"] == 1


def test_acquisition_does_not_retry_permanent_provider_rejection(
    tmp_path,
    monkeypatch,
):
    binding = _binding()
    _install_runtime_fakes(monkeypatch, binding)
    status_path = tmp_path / "status.json"

    with pytest.raises(GatewayControlError, match="ADVANCED_AUTH_REJECTED"):
        run_acquisition(
            binding,
            provider=_Provider(),
            api_key="provider-free",
            journal_root=tmp_path / "journal",
            status_path=status_path,
            lock_path=tmp_path / "socket.lock",
            stop_event=threading.Event(),
            connection_factory=lambda _: _RejectedConnection(),
            clock=lambda: NOW,
            monotonic_ns=lambda: 100,
            diagnostic=True,
        )

    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["connection_attempts"] == 1
    assert status["state"] == "FAILED_PERMANENT"
    assert status["failure_class"] == "PERMANENT_PROVIDER_OR_SAFETY"


def test_status_file_from_another_session_does_not_block_this_one(tmp_path):
    binding = _binding()
    status_path = tmp_path / "status.json"
    last_session = {
        "schema": runtime.ACQUISITION_STATUS_SCHEMA,
        "binding_sha256": "0" * 64,
        "owner_id": str(binding.owner_id),
        "journal_generation": str(uuid5(NAMESPACE_URL, "last-session")),
        "state": "COMPLETE",
        "connection_attempts": 12,
        "decoded_messages": 108466,
        "decoded_bytes": 25961533,
    }
    status_path.write_text(json.dumps(last_session), encoding="utf-8")

    assert runtime._load_status(status_path, binding) == {
        "connection_attempts": 0,
        "decoded_messages": 0,
        "decoded_bytes": 0,
    }


def test_changed_status_for_this_session_still_fails_closed(tmp_path):
    binding = _binding()
    status_path = tmp_path / "status.json"
    status_path.write_text(
        json.dumps({
            "schema": runtime.ACQUISITION_STATUS_SCHEMA,
            "binding_sha256": "0" * 64,
            "owner_id": str(binding.owner_id),
            "journal_generation": str(binding.authority.journal_generation),
            "connection_attempts": 3,
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="durable status changed"):
        runtime._load_status(status_path, binding)


def test_compose_exposes_only_the_acquisition_role():
    compose = (
        Path(__file__).resolve().parents[3]
        / "docker-compose.realtime-v2.yml"
    ).read_text(encoding="utf-8")
    start = compose.index("  realtime-v3-options-acquisition:")
    end = compose.index(
        "\n  realtime-v3-options-p8-market-state:",
        start,
    )
    block = compose[start:end]

    assert "realtime-v3-options-acquisition" in block
    assert "advanced-realtime-v3" in block
    assert "restart: unless-stopped" in block
    assert "run_advanced_options_v3_acquisition.py" in block
    assert "--component\n      - options-v3-acquisition" in block
    assert "run_advanced_options_v3_production.py" not in block
    assert "--docker-service-kind" not in block
    assert "publication" not in block

    ingress_start = compose.index("  realtime-v3-options-p8-ingress:")
    ingress_end = compose.index(
        "\n  realtime-v3-options-acquisition:",
        ingress_start,
    )
    ingress = compose[ingress_start:ingress_end]
    assert "REALTIME_V3_ACQUISITION_SPLIT: ENABLED" in ingress
    assert "realtime_v2_provider_key" not in ingress
    assert "POLYGON_API_KEY" not in ingress


def test_a_dropped_socket_records_which_side_closed_it():
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close

    keepalive = ConnectionClosedError(None, Close(1011, "keepalive ping timeout"))
    assert runtime._close_detail(keepalive) == {
        "close_sent_code": 1011,
        "close_sent_reason": "keepalive ping timeout",
    }
    provider = ConnectionClosedError(Close(1008, "slow consumer"), None)
    assert runtime._close_detail(provider) == {
        "close_received_code": 1008,
        "close_received_reason": "slow consumer",
    }
    assert runtime._close_detail(ConnectionError("reset")) == {}
