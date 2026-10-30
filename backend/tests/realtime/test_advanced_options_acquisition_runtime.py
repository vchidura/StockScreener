from __future__ import annotations

import json
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime import advanced_options_acquisition_runtime as runtime
from realtime.advanced_options_acquisition_runtime import (
    acquisition_binding_from_payload,
    acquisition_binding_payload,
    acquisition_health,
    build_acquisition_binding,
    run_acquisition,
)
from realtime.advanced_options_p8_runtime import (
    P8DownstreamBindingStore,
    P8ExecutionBundleStore,
    build_p8_downstream_binding,
    build_p8_preopen_session_binding,
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


def test_compose_exposes_only_the_acquisition_role():
    compose = (
        Path(__file__).resolve().parents[3]
        / "docker-compose.realtime-v2.yml"
    ).read_text(encoding="utf-8")
    start = compose.index("  realtime-v3-options-acquisition:")
    end = compose.index(
        "\n  realtime-v2-options-p8-market-state:",
        start,
    )
    block = compose[start:end]

    assert 'profiles: ["realtime-v3-options-acquisition"]' in block
    assert 'restart: "on-failure:3"' in block
    assert "run_advanced_options_v3_acquisition.py" in block
    assert "--component\n      - options-v3-acquisition" in block
    assert "run_advanced_options_v3_production.py" not in block
    assert "--docker-service-kind" not in block
    assert "publication" not in block

    ingress_start = compose.index("  realtime-v2-options-p8-ingress:")
    ingress_end = compose.index(
        "\n  realtime-v3-options-acquisition:",
        ingress_start,
    )
    ingress = compose[ingress_start:ingress_end]
    assert "REALTIME_V3_ACQUISITION_SPLIT: ENABLED" in ingress
    assert "realtime_v2_provider_key" not in ingress
    assert "POLYGON_API_KEY" not in ingress
