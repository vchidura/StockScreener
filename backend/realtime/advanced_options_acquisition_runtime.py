"""Receipt-free Advanced Options session acquisition runtime."""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Event
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_gateway import AdvancedGateway, GatewayConfiguration
from .advanced_options_ingress import AdvancedOptionsJournalPump
from .advanced_options_v3_authority import (
    SessionAcquisitionAuthorization,
    consume_session_acquisition_authority_cursor,
    persist_session_acquisition_authorization_cursor,
    session_acquisition_authorization_from_payload,
    session_acquisition_authorization_payload,
)
from .buffer import BufferLimits, RawMessageBuffer
from .contracts import (
    AssetClass,
    EQUITY_VALIDATION_UNIVERSE,
    MarketChannel,
    MarketFeed,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from .database_access_v2 import APP_ROLE
from .journal import RawJournalReader
from .normalizer import InstrumentAdmission, NORMALIZER_VERSION
from .provider_socket_lease import (
    MASSIVE_OPTIONS_SOCKET_RESOURCE,
    HostProviderSocketLease,
)


UTC = timezone.utc
ACQUISITION_BINDING_SCHEMA = (
    "advanced-options-v3-session-acquisition-binding-v1"
)
ACQUISITION_STATUS_SCHEMA = (
    "advanced-options-v3-session-acquisition-status-v1"
)
ACQUISITION_OWNER_NAME = "advanced-options-v3-session-acquisition-v1"
CONTROL_MESSAGES_PER_ATTEMPT = 8
ZERO_FORBIDDEN_EFFECTS = {
    "detector": 0,
    "projection": 0,
    "publication": 0,
    "pointer": 0,
    "read_pin": 0,
    "dispatch": 0,
    "delivery": 0,
    "broker": 0,
    "trading": 0,
}


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _sha256(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


def _image_id(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise ValueError("backend_image_id must be immutable")
    _sha256(value.removeprefix("sha256:"), "backend_image_id")
    return value


def _contract_underlyer(ticker: str) -> str:
    value = ticker.strip().upper()
    if not value.startswith("O:") or len(value) < 5:
        raise ValueError("option contract ticker is invalid")
    return value[2:5]


@dataclass(frozen=True, slots=True)
class AdvancedOptionsAcquisitionBinding:
    binding_id: UUID
    binding_sha256: str
    source_sha256: str
    backend_image_id: str
    authority: SessionAcquisitionAuthorization
    contract_tickers: tuple[str, ...]
    owner_id: UUID
    stream_session_id: UUID
    connection_attempt_ceiling: int
    decoded_message_ceiling: int
    decoded_byte_ceiling: int
    equity_tickers: tuple[str, ...] = (
        EQUITY_VALIDATION_UNIVERSE.equity_tickers
    )
    maximum_message_bytes: int = 2_097_152
    buffer_event_limit: int = 128
    buffer_byte_limit: int = 8_388_608
    library_max_queue: int = 2

    def __post_init__(self) -> None:
        contracts = tuple(sorted(
            str(value).strip().upper()
            for value in self.contract_tickers
        ))
        equities = tuple(sorted(
            str(value).strip().upper()
            for value in self.equity_tickers
        ))
        membership = {
            (_contract_underlyer(ticker), ticker[11:12])
            for ticker in contracts
            if len(ticker) >= 12
        }
        if (
            not isinstance(self.binding_id, UUID)
            or not self.binding_id.int
            or not isinstance(
                self.authority,
                SessionAcquisitionAuthorization,
            )
            or not isinstance(self.owner_id, UUID)
            or not self.owner_id.int
            or not isinstance(self.stream_session_id, UUID)
            or not self.stream_session_id.int
            or len(contracts) != 4
            or len(set(contracts)) != 4
            or equities
            != tuple(sorted(EQUITY_VALIDATION_UNIVERSE.equity_tickers))
            or membership
            != {
                ("QQQ", "C"),
                ("QQQ", "P"),
                ("SPY", "C"),
                ("SPY", "P"),
            }
            or type(self.connection_attempt_ceiling) is not int
            or self.connection_attempt_ceiling <= 0
            or type(self.decoded_message_ceiling) is not int
            or self.decoded_message_ceiling <= 0
            or type(self.decoded_byte_ceiling) is not int
            or self.decoded_byte_ceiling <= 0
            or self.maximum_message_bytes != 2_097_152
            or self.buffer_event_limit != 128
            or self.buffer_byte_limit != 8_388_608
            or self.library_max_queue != 2
        ):
            raise ValueError("Options acquisition binding scope is invalid")
        _sha256(self.source_sha256, "source_sha256")
        _image_id(self.backend_image_id)
        object.__setattr__(self, "contract_tickers", contracts)
        object.__setattr__(self, "equity_tickers", equities)
        expected = canonical_sha256(
            acquisition_binding_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:options-acquisition-binding:{expected}",
        )
        if (
            self.binding_sha256 != expected
            or self.binding_id != expected_id
        ):
            raise ValueError("Options acquisition binding identity changed")


def acquisition_binding_payload(
    binding: AdvancedOptionsAcquisitionBinding,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": ACQUISITION_BINDING_SCHEMA,
        "source_sha256": binding.source_sha256,
        "backend_image_id": binding.backend_image_id,
        "session_authority": (
            session_acquisition_authorization_payload(
                binding.authority
            )
        ),
        "membership": {
            "equity_target": len(binding.equity_tickers),
            "equity_tickers": list(binding.equity_tickers),
            "option_contract_tickers": list(binding.contract_tickers),
        },
        "owner_name": ACQUISITION_OWNER_NAME,
        "owner_id": str(binding.owner_id),
        "stream_session_id": str(binding.stream_session_id),
        "budgets": {
            "connection_attempt_ceiling": (
                binding.connection_attempt_ceiling
            ),
            "decoded_message_ceiling": (
                binding.decoded_message_ceiling
            ),
            "decoded_byte_ceiling": binding.decoded_byte_ceiling,
        },
        "runtime": {
            "maximum_message_bytes": binding.maximum_message_bytes,
            "buffer_event_limit": binding.buffer_event_limit,
            "buffer_byte_limit": binding.buffer_byte_limit,
            "library_max_queue": binding.library_max_queue,
            "normalizer_version": NORMALIZER_VERSION,
        },
        "authority": {
            "provider_socket": True,
            "journal": True,
            "normalization": True,
            "detector": False,
            "projection": False,
            "publication": False,
            "pointer": False,
            "read_pin": False,
            "dispatch": False,
            "delivery": False,
            "broker": False,
            "trading": False,
        },
    }
    if include_identity:
        value["binding_id"] = str(binding.binding_id)
        value["binding_sha256"] = binding.binding_sha256
    return value


def build_acquisition_binding(
    *,
    source_sha256: str,
    backend_image_id: str,
    authority: SessionAcquisitionAuthorization,
    contract_tickers: tuple[str, ...],
    connection_attempt_ceiling: int,
    decoded_message_ceiling: int,
    decoded_byte_ceiling: int,
    equity_tickers: tuple[str, ...] = (
        EQUITY_VALIDATION_UNIVERSE.equity_tickers
    ),
) -> AdvancedOptionsAcquisitionBinding:
    values = {
        "source_sha256": source_sha256,
        "backend_image_id": backend_image_id,
        "authority": authority,
        "contract_tickers": contract_tickers,
        "owner_id": uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:options-acquisition-owner:"
                f"{authority.authorization_sha256}"
            ),
        ),
        "stream_session_id": uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:options-acquisition-stream:"
                f"{authority.authorization_sha256}"
            ),
        ),
        "connection_attempt_ceiling": connection_attempt_ceiling,
        "decoded_message_ceiling": decoded_message_ceiling,
        "decoded_byte_ceiling": decoded_byte_ceiling,
        "equity_tickers": equity_tickers,
    }
    unsigned = AdvancedOptionsAcquisitionBinding.__new__(
        AdvancedOptionsAcquisitionBinding
    )
    object.__setattr__(unsigned, "binding_id", UUID(int=1))
    object.__setattr__(unsigned, "binding_sha256", "0" * 64)
    for name, item in values.items():
        object.__setattr__(unsigned, name, item)
    for name, item in {
        "maximum_message_bytes": 2_097_152,
        "buffer_event_limit": 128,
        "buffer_byte_limit": 8_388_608,
        "library_max_queue": 2,
    }.items():
        object.__setattr__(unsigned, name, item)
    digest = canonical_sha256(
        acquisition_binding_payload(
            unsigned,
            include_identity=False,
        )
    )
    return AdvancedOptionsAcquisitionBinding(
        binding_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:options-acquisition-binding:{digest}",
        ),
        binding_sha256=digest,
        **values,
    )


def acquisition_binding_from_payload(
    value: object,
) -> AdvancedOptionsAcquisitionBinding:
    if not isinstance(value, Mapping):
        raise TypeError("Options acquisition binding must be an object")
    budgets = value.get("budgets")
    runtime = value.get("runtime")
    membership = value.get("membership")
    if (
        value.get("schema") != ACQUISITION_BINDING_SCHEMA
        or not isinstance(budgets, Mapping)
        or not isinstance(runtime, Mapping)
        or not isinstance(membership, Mapping)
        or membership.get("equity_target")
        != len(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        or value.get("owner_name") != ACQUISITION_OWNER_NAME
        or runtime.get("normalizer_version") != NORMALIZER_VERSION
    ):
        raise ValueError("Options acquisition binding contract changed")
    binding = AdvancedOptionsAcquisitionBinding(
        binding_id=UUID(str(value["binding_id"])),
        binding_sha256=str(value["binding_sha256"]),
        source_sha256=str(value["source_sha256"]),
        backend_image_id=str(value["backend_image_id"]),
        authority=session_acquisition_authorization_from_payload(
            value["session_authority"]
        ),
        contract_tickers=tuple(
            str(item)
            for item in membership["option_contract_tickers"]
        ),
        owner_id=UUID(str(value["owner_id"])),
        stream_session_id=UUID(str(value["stream_session_id"])),
        connection_attempt_ceiling=int(
            budgets["connection_attempt_ceiling"]
        ),
        decoded_message_ceiling=int(
            budgets["decoded_message_ceiling"]
        ),
        decoded_byte_ceiling=int(
            budgets["decoded_byte_ceiling"]
        ),
        equity_tickers=tuple(
            str(item)
            for item in membership["equity_tickers"]
        ),
        maximum_message_bytes=int(
            runtime["maximum_message_bytes"]
        ),
        buffer_event_limit=int(runtime["buffer_event_limit"]),
        buffer_byte_limit=int(runtime["buffer_byte_limit"]),
        library_max_queue=int(runtime["library_max_queue"]),
    )
    if acquisition_binding_payload(binding) != dict(value):
        raise ValueError("Options acquisition binding payload changed")
    return binding


def _write_json(path: Path, value: object) -> None:
    encoded = (
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open("wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _load_status(
    path: Path,
    binding: AdvancedOptionsAcquisitionBinding,
) -> dict[str, object]:
    if not path.is_file():
        return {
            "connection_attempts": 0,
            "decoded_messages": 0,
            "decoded_bytes": 0,
        }
    value = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(value, dict)
        or value.get("schema") != ACQUISITION_STATUS_SCHEMA
        or value.get("binding_sha256") != binding.binding_sha256
        or value.get("owner_id") != str(binding.owner_id)
        or value.get("journal_generation")
        != str(binding.authority.journal_generation)
    ):
        raise ValueError("Options acquisition durable status changed")
    return value


def _catalog_scope(provider, binding):
    with provider.transaction(
        APP_ROLE,
        read_only=True,
        dict_cursor=True,
    ) as unit:
        unit.cursor.execute(
            "SELECT contract.contract_ticker,"
            "contract.underlying_security_id::text,"
            "version.version_id,instrument.instrument_id "
            "FROM ref.option_contracts AS contract "
            "JOIN ref.option_contract_versions AS version "
            "ON version.contract_id=contract.contract_id "
            "AND version.effective_to IS NULL "
            "JOIN rt.instruments AS instrument "
            "ON instrument.contract_id=contract.contract_id "
            "WHERE contract.contract_ticker=ANY(%s) "
            "ORDER BY contract.contract_ticker",
            (list(binding.contract_tickers),),
        )
        rows = unit.cursor.fetchall()
    if (
        len(rows) != 4
        or tuple(str(row["contract_ticker"]) for row in rows)
        != binding.contract_tickers
        or any(
            row["version_id"] is None
            or row["instrument_id"] is None
            for row in rows
        )
    ):
        raise RuntimeError("exact-four acquisition catalog is unavailable")
    admissions = {
        str(row["contract_ticker"]): InstrumentAdmission(
            int(row["instrument_id"]),
            str(row["contract_ticker"]),
            _contract_underlyer(str(row["contract_ticker"])),
            AssetClass.OPTION,
            0,
        )
        for row in rows
    }
    members = tuple(
        SubscriptionMember(
            AssetClass.OPTION,
            str(row["contract_ticker"]),
            _contract_underlyer(str(row["contract_ticker"])),
            channel,
            catalog_version_id=int(row["version_id"]),
        )
        for row in rows
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    manifest = SubscriptionManifest(
        version=ACQUISITION_BINDING_SCHEMA,
        session_date=binding.authority.session_date,
        universe=UniverseManifest(
            ACQUISITION_BINDING_SCHEMA,
            ("QQQ", "SPY"),
            ("QQQ", "SPY"),
        ),
        capability_profile_sha256=(
            binding.authority.provider_budget_sha256
        ),
        allocation_policy_sha256=(
            binding.authority.slot_set_member_sha256
        ),
        members=members,
        maximum_option_quote_contracts=4,
    )
    return admissions, manifest


def acquisition_health(
    path: Path,
    binding: AdvancedOptionsAcquisitionBinding,
    *,
    checked_at: datetime,
    maximum_age_seconds: int = 30,
) -> dict[str, object]:
    status = _load_status(path, binding)
    observed = datetime.fromisoformat(str(status["observed_at_utc"]))
    checked = _utc(checked_at, "checked_at")
    if (
        status.get("state")
        not in {"READY", "CAPTURING", "DRAINING", "COMPLETE"}
        or observed > checked
        or (checked - observed).total_seconds() > maximum_age_seconds
    ):
        raise RuntimeError("Options acquisition health is stale")
    return {
        "status": "READY",
        "state": status["state"],
        "binding_sha256": binding.binding_sha256,
        "owner_id": str(binding.owner_id),
        "journal_generation": str(
            binding.authority.journal_generation
        ),
        "connection_attempts": status["connection_attempts"],
        "decoded_messages": status["decoded_messages"],
        "decoded_bytes": status["decoded_bytes"],
        "last_raw_offset": status["last_raw_offset"],
        "last_event_offset": status["last_event_offset"],
        "forbidden_effects": {
            **ZERO_FORBIDDEN_EFFECTS,
        },
    }


def run_acquisition(
    binding: AdvancedOptionsAcquisitionBinding,
    *,
    provider,
    api_key: str,
    journal_root: Path,
    status_path: Path,
    lock_path: Path,
    stop_event: Event,
    connection_factory=None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
    diagnostic: bool = False,
) -> dict[str, object]:
    if not api_key.strip():
        raise ValueError("Options acquisition provider key is required")
    now = _utc(clock(), "clock")
    if not binding.authority.prepared_at <= now < (
        binding.authority.acquisition_stop
    ):
        raise RuntimeError("Options acquisition is outside authority")
    admissions, manifest = _catalog_scope(provider, binding)
    status = _load_status(status_path, binding)
    attempts = int(status["connection_attempts"])
    decoded_messages = int(status["decoded_messages"])
    decoded_bytes = int(status["decoded_bytes"])
    journal_path = (
        journal_root.resolve()
        / "options-acquisition"
        / str(binding.authority.journal_generation)
        / "options.sqlite"
    )
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    effect_sha256 = canonical_sha256({
        "schema": ACQUISITION_BINDING_SCHEMA,
        "binding_sha256": binding.binding_sha256,
        "journal_generation": str(
            binding.authority.journal_generation
        ),
        "owner_id": str(binding.owner_id),
        "contracts": binding.contract_tickers,
    })
    with provider.transaction(APP_ROLE) as unit:
        persist_session_acquisition_authorization_cursor(
            unit.cursor,
            binding.authority,
        )
    buffer = RawMessageBuffer(
        BufferLimits(
            binding.buffer_event_limit,
            binding.buffer_byte_limit,
            30_000_000_000,
        ),
        monotonic_ns=monotonic_ns,
    )
    latest = [None]

    def progress(value):
        latest[0] = value

    pump = AdvancedOptionsJournalPump(
        buffer,
        journal_path,
        binding.authority.journal_generation,
        admissions,
        maximum_batch_messages=100,
        maximum_batch_bytes=binding.buffer_byte_limit,
        on_progress=progress,
    )
    configuration = GatewayConfiguration(
        MarketFeed.OPTIONS,
        GatewayConfiguration.options().endpoint,
        maximum_message_bytes=binding.maximum_message_bytes,
        library_max_queue=binding.library_max_queue,
    )
    lease = HostProviderSocketLease(
        lock_path,
        MASSIVE_OPTIONS_SOCKET_RESOURCE,
        binding.owner_id,
    )
    pump.start()
    lease.acquire(now=now)
    state = "CAPTURING"
    active_stream_session_id = binding.stream_session_id
    try:
        while (
            not stop_event.is_set()
            and _utc(clock(), "clock")
            < binding.authority.acquisition_stop
            and attempts < binding.connection_attempt_ceiling
            and decoded_messages < binding.decoded_message_ceiling
            and decoded_bytes < binding.decoded_byte_ceiling
        ):
            attempts += 1
            remaining_messages = (
                binding.decoded_message_ceiling
                - decoded_messages
                - CONTROL_MESSAGES_PER_ATTEMPT
            )
            remaining_bytes = (
                binding.decoded_byte_ceiling - decoded_bytes
            )
            if remaining_messages <= 0 or remaining_bytes <= 0:
                break
            with provider.transaction(APP_ROLE) as unit:
                consume_session_acquisition_authority_cursor(
                    unit.cursor,
                    binding.authority,
                    effect_kind="PROVIDER_SOCKET",
                    effect_identity_sha256=effect_sha256,
                    consumed_at=_utc(clock(), "clock"),
                )
            active_stream_session_id = uuid5(
                binding.stream_session_id,
                f"connection-attempt:{attempts}",
            )
            _write_json(status_path, {
                "schema": ACQUISITION_STATUS_SCHEMA,
                "state": "CONNECTING",
                "binding_sha256": binding.binding_sha256,
                "owner_id": str(binding.owner_id),
                "journal_generation": str(
                    binding.authority.journal_generation
                ),
                "stream_session_id": str(active_stream_session_id),
                "connection_attempts": attempts,
                "decoded_messages": decoded_messages,
                "decoded_bytes": decoded_bytes,
                "last_raw_offset": (
                    0 if latest[0] is None else latest[0].last_raw_offset
                ),
                "last_event_offset": (
                    0 if latest[0] is None else latest[0].last_event_offset
                ),
                "observed_at_utc": _utc(clock(), "clock").isoformat(),
                "diagnostic": diagnostic,
                "forbidden_effects": dict(ZERO_FORBIDDEN_EFFECTS),
            })

            def usage(session_messages, session_bytes):
                _write_json(status_path, {
                    **_load_status(status_path, binding),
                    "state": "CAPTURING",
                    "decoded_messages": (
                        decoded_messages
                        + CONTROL_MESSAGES_PER_ATTEMPT
                        + session_messages
                    ),
                    "decoded_bytes": decoded_bytes + session_bytes,
                    "observed_at_utc": (
                        _utc(clock(), "clock").isoformat()
                    ),
                })

            gateway = AdvancedGateway(
                configuration,
                manifest,
                buffer,
                api_key=api_key,
                connection_factory=connection_factory,
                utc_clock=clock,
                monotonic_ns=monotonic_ns,
                session_id_factory=lambda: active_stream_session_id,
                usage_observer=usage,
            )
            result = asyncio.run(gateway.run_session(
                maximum_market_messages=(
                    len(binding.contract_tickers)
                    if diagnostic
                    else remaining_messages
                ),
                maximum_received_bytes=remaining_bytes,
            ))
            decoded_messages += (
                result.received_messages
                + CONTROL_MESSAGES_PER_ATTEMPT
            )
            decoded_bytes += result.received_bytes
            _write_json(status_path, {
                "schema": ACQUISITION_STATUS_SCHEMA,
                "state": state,
                "binding_sha256": binding.binding_sha256,
                "owner_id": str(binding.owner_id),
                "journal_generation": str(
                    binding.authority.journal_generation
                ),
                "stream_session_id": str(active_stream_session_id),
                "connection_attempts": attempts,
                "decoded_messages": decoded_messages,
                "decoded_bytes": decoded_bytes,
                "last_raw_offset": (
                    0 if latest[0] is None else latest[0].last_raw_offset
                ),
                "last_event_offset": (
                    0 if latest[0] is None else latest[0].last_event_offset
                ),
                "observed_at_utc": _utc(clock(), "clock").isoformat(),
                "diagnostic": diagnostic,
                "forbidden_effects": dict(ZERO_FORBIDDEN_EFFECTS),
            })
            if diagnostic:
                while not stop_event.wait(0.1):
                    _write_json(status_path, {
                        **_load_status(status_path, binding),
                        "state": "READY",
                        "observed_at_utc": (
                            _utc(clock(), "clock").isoformat()
                        ),
                    })
                break
            stop_event.wait(min(30.0, 0.5 * (2 ** (attempts - 1))))
        state = "DRAINING"
    finally:
        pump.request_stop()
        final = pump.join(30)
        lease.release(now=_utc(clock(), "clock"))
    if final.last_raw_offset <= 0 or final.last_event_offset <= 0:
        raise RuntimeError("Options acquisition captured no T/Q evidence")
    recovery = RawJournalReader(journal_path).recovery_state(
        NORMALIZER_VERSION
    )
    if (
        recovery.generation != binding.authority.journal_generation
        or recovery.integrity_check != "ok"
        or recovery.quarantine_count != 0
    ):
        raise RuntimeError("Options acquisition journal recovery failed")
    report = {
        "schema": ACQUISITION_STATUS_SCHEMA,
        "state": "COMPLETE",
        "binding_sha256": binding.binding_sha256,
        "owner_id": str(binding.owner_id),
        "journal_generation": str(
            binding.authority.journal_generation
        ),
        "stream_session_id": str(active_stream_session_id),
        "connection_attempts": attempts,
        "decoded_messages": decoded_messages,
        "decoded_bytes": decoded_bytes,
        "last_raw_offset": final.last_raw_offset,
        "last_event_offset": final.last_event_offset,
        "observed_at_utc": _utc(clock(), "clock").isoformat(),
        "journal_path": str(journal_path),
        "journal_integrity": recovery.integrity_check,
        "quarantine_count": recovery.quarantine_count,
        "diagnostic": diagnostic,
        "forbidden_effects": dict(ZERO_FORBIDDEN_EFFECTS),
    }
    report["semantic_sha256"] = canonical_sha256(report)
    _write_json(status_path, report)
    return report
