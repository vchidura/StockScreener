"""Receipt-free Advanced Options session acquisition runtime."""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Event, RLock, Thread
from uuid import NAMESPACE_URL, UUID, uuid5

from websockets.exceptions import ConnectionClosed

from .advanced_gateway import (
    AdvancedGateway,
    GatewayConfiguration,
    GatewayControlError,
    GatewayFeedState,
    GatewayOverload,
)
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
from .advanced_options_selection_c6 import C6SelectionPlan


UTC = timezone.utc
ACQUISITION_BINDING_SCHEMA = (
    "advanced-options-v3-session-acquisition-binding-v1"
)
ACQUISITION_STATUS_SCHEMA = (
    "advanced-options-v3-session-acquisition-status-v1"
)
ACQUISITION_OWNER_NAME = "advanced-options-v3-session-acquisition-v1"
CONTROL_MESSAGES_PER_ATTEMPT = 8
ACQUISITION_GIL_SWITCH_SECONDS = 0.0005
ACQUISITION_PING_TIMEOUT_SECONDS = 60
# Under the buffer's 30 s age limit, so a journal that truly stopped still fails.
ACQUISITION_HANDOFF_WAIT_SECONDS = 20.0
STATUS_MESSAGE_RESERVATION_QUANTUM = 1024
STATUS_MESSAGE_RESERVATION_LOW_WATER = 256
STATUS_BYTE_RESERVATION_MESSAGES = 2
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
_OPTION_UNDERLYING = re.compile(r"^O:([A-Z]{1,6})")


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
    match = _OPTION_UNDERLYING.match(value)
    if match is None or value[-9:-8] not in {"C", "P"}:
        raise ValueError("option contract ticker is invalid")
    return match.group(1)


def _contract_side(ticker: str) -> str:
    _contract_underlyer(ticker)
    return ticker.strip().upper()[-9:-8]


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
            (_contract_underlyer(ticker), _contract_side(ticker))
            for ticker in contracts
        }
        underlyings = {
            underlyer for underlyer, _ in membership
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
            or not contracts
            or len(set(contracts)) != len(contracts)
            or equities
            != tuple(sorted(EQUITY_VALIDATION_UNIVERSE.equity_tickers))
            or not membership
            or not underlyings
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
        "contract_tickers": tuple(sorted(
            str(value).strip().upper() for value in contract_tickers
        )),
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


def _message_reservation_runs_low(
    current_messages: int,
    reserved_messages: int,
    ceiling: int,
) -> bool:
    """Whether to reserve more decoded messages, which flushes the status file.

    This runs on the socket thread for every message. Testing against the full
    quantum, the size each extension grants, was true again on the very next
    message, so every message waited on a disk flush: 62 per second on Nov 5,
    which held the real feed at about 0.7 times real time (G63).
    """
    return (
        current_messages + STATUS_MESSAGE_RESERVATION_LOW_WATER
        > reserved_messages
        and reserved_messages < ceiling
    )


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
    fresh = {
        "connection_attempts": 0,
        "decoded_messages": 0,
        "decoded_bytes": 0,
    }
    if not path.is_file():
        return fresh
    value = json.loads(path.read_text(encoding="utf-8"))
    # The path is fixed across sessions, so another journal's file is the last
    # session's status, not a change to this one.
    if (
        isinstance(value, dict)
        and value.get("journal_generation")
        != str(binding.authority.journal_generation)
    ):
        return fresh
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


def _resume_budget_usage(status: Mapping[str, object]):
    decoded_messages = int(status["decoded_messages"])
    decoded_bytes = int(status["decoded_bytes"])
    reserved_messages = int(
        status.get("reserved_decoded_messages") or decoded_messages
    )
    reserved_bytes = int(
        status.get("reserved_decoded_bytes") or decoded_bytes
    )
    return (
        max(decoded_messages, reserved_messages),
        max(decoded_bytes, reserved_bytes),
        max(0, reserved_messages - decoded_messages),
        max(0, reserved_bytes - decoded_bytes),
    )


class _AcquisitionStatusPublisher:
    """Serialize status updates and persist a process heartbeat at a fixed pace."""

    def __init__(
        self,
        path: Path,
        binding: AdvancedOptionsAcquisitionBinding,
        value: Mapping[str, object],
        *,
        clock: Callable[[], datetime],
        interval_seconds: float = 1.0,
    ) -> None:
        self.path = path
        self.binding = binding
        self.clock = clock
        self.interval_seconds = interval_seconds
        self._value = dict(value)
        self._lock = RLock()
        self._stop = Event()
        self._failure: BaseException | None = None
        self._thread = Thread(
            target=self._run,
            name="advanced-options-acquisition-status",
            daemon=True,
        )

    def update(self, *, persist: bool = False, **values: object) -> None:
        with self._lock:
            self._value.update(values)
        if persist:
            self.persist()

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return dict(self._value)

    def persist(self) -> None:
        self.raise_if_failed()
        with self._lock:
            heartbeat = _utc(self.clock(), "clock").isoformat()
            self._value["runtime_heartbeat_at_utc"] = heartbeat
            # Retained for readers of the v1 status schema. Health uses the
            # explicit runtime heartbeat, never market activity.
            self._value["observed_at_utc"] = heartbeat
            try:
                _write_json(self.path, self._value)
            except BaseException as error:
                self._failure = error
                self._stop.set()
                raise

    def start(self) -> None:
        self.persist()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.ident is not None:
            self._thread.join(timeout=max(2.0, self.interval_seconds * 2))
        if self._failure is not None:
            raise RuntimeError(
                "Options acquisition status persistence failed"
            ) from self._failure
        self.persist()

    def failed(self) -> bool:
        return self._failure is not None

    def raise_if_failed(self) -> None:
        if self._failure is not None:
            raise RuntimeError(
                "Options acquisition status persistence failed"
            ) from self._failure

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.persist()
            except BaseException as error:
                self._failure = error
                self._stop.set()
                return


def _close_detail(error: BaseException) -> dict[str, object]:
    """Which side closed a dropped socket and why, when the close frames say."""
    detail = {}
    for side, frame in (
        ("received", getattr(error, "rcvd", None)),
        ("sent", getattr(error, "sent", None)),
    ):
        if frame is not None:
            detail[f"close_{side}_code"] = frame.code
            detail[f"close_{side}_reason"] = str(frame.reason or "")[:200]
    return detail


def _transient_transport_failure(error: BaseException) -> bool:
    return isinstance(
        error,
        (ConnectionClosed, ConnectionError, OSError, TimeoutError),
    )


def _catalog_scope(provider, binding, selection_plan=None):
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
        len(rows) != len(binding.contract_tickers)
        or tuple(str(row["contract_ticker"]) for row in rows)
        != binding.contract_tickers
        or any(
            row["version_id"] is None
            or row["instrument_id"] is None
            for row in rows
        )
    ):
        raise RuntimeError(
            "configured acquisition catalog is unavailable"
        )
    if selection_plan is None:
        shard_by_ticker = {
            ticker: 0 for ticker in binding.contract_tickers
        }
    else:
        if not isinstance(selection_plan, C6SelectionPlan):
            raise TypeError("C6 acquisition selection plan is invalid")
        shards = tuple(sorted(
            selection_plan.shards,
            key=lambda row: row.shard_slot,
        ))
        shard_by_ticker = {
            ticker: shard_id
            for shard_id, shard in enumerate(shards)
            for ticker in shard.contract_tickers
        }
        if set(shard_by_ticker) != set(binding.contract_tickers):
            raise RuntimeError(
                "configured acquisition logical shards diverged"
            )
    admissions = {
        str(row["contract_ticker"]): InstrumentAdmission(
            int(row["instrument_id"]),
            str(row["contract_ticker"]),
            _contract_underlyer(str(row["contract_ticker"])),
            AssetClass.OPTION,
            shard_by_ticker[str(row["contract_ticker"])],
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
            tuple(sorted({
                _contract_underlyer(ticker)
                for ticker in binding.contract_tickers
            })),
            tuple(sorted({
                _contract_underlyer(ticker)
                for ticker in binding.contract_tickers
            })),
        ),
        capability_profile_sha256=(
            binding.authority.provider_budget_sha256
        ),
        allocation_policy_sha256=(
            binding.authority.slot_set_member_sha256
        ),
        members=members,
        maximum_option_quote_contracts=len(
            binding.contract_tickers
        ),
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
    heartbeat = datetime.fromisoformat(str(
        status.get("runtime_heartbeat_at_utc")
        or status["observed_at_utc"]
    ))
    checked = _utc(checked_at, "checked_at")
    if (
        status.get("state")
        not in {"READY", "CAPTURING", "DRAINING", "PAUSED", "COMPLETE"}
        or heartbeat > checked
        or (checked - heartbeat).total_seconds() > maximum_age_seconds
    ):
        raise RuntimeError("Options acquisition health is stale")
    if status.get("state") == "CAPTURING" and (
        status.get("socket_state")
        not in {GatewayFeedState.WARMING.value, GatewayFeedState.LIVE.value}
        or status.get("subscription_ready") is not True
    ):
        raise RuntimeError("Options acquisition socket is not ready")
    return {
        "status": "READY",
        "state": status["state"],
        "runtime_heartbeat_at_utc": heartbeat.isoformat(),
        "socket_state": status.get("socket_state"),
        "subscription_ready": status.get("subscription_ready", False),
        "last_market_event_at_utc": status.get(
            "last_market_event_at_utc"
        ),
        "last_consumer_progress_at_utc": status.get(
            "last_consumer_progress_at_utc"
        ),
        "transport_gaps": list(status.get("transport_gaps") or ()),
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
    selection_plan: C6SelectionPlan | None = None,
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
    diagnostic: bool = False,
) -> dict[str, object]:
    if not api_key.strip():
        raise ValueError("Options acquisition provider key is required")
    # The journal thread would otherwise hold the GIL for up to 5 ms after every
    # socket call, capping the feed below its live rate until pings time out.
    sys.setswitchinterval(ACQUISITION_GIL_SWITCH_SECONDS)
    now = _utc(clock(), "clock")
    if not binding.authority.prepared_at <= now < (
        binding.authority.acquisition_stop
    ):
        raise RuntimeError("Options acquisition is outside authority")
    admissions, manifest = (
        _catalog_scope(provider, binding)
        if selection_plan is None
        else _catalog_scope(
            provider,
            binding,
            selection_plan,
        )
    )
    status = _load_status(status_path, binding)
    attempts = int(status["connection_attempts"])
    (
        decoded_messages,
        decoded_bytes,
        recovered_reserved_messages,
        recovered_reserved_bytes,
    ) = _resume_budget_usage(status)
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
    publisher_holder = [None]

    def progress(value):
        latest[0] = value
        publisher = publisher_holder[0]
        if publisher is not None:
            publisher.update(
                last_raw_offset=value.last_raw_offset,
                last_event_offset=value.last_event_offset,
                last_consumer_progress_at_utc=(
                    _utc(clock(), "clock").isoformat()
                ),
            )

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
        ping_timeout_seconds=ACQUISITION_PING_TIMEOUT_SECONDS,
        maximum_message_bytes=binding.maximum_message_bytes,
        library_max_queue=binding.library_max_queue,
        handoff_wait_seconds=ACQUISITION_HANDOFF_WAIT_SECONDS,
    )
    lease = HostProviderSocketLease(
        lock_path,
        MASSIVE_OPTIONS_SOCKET_RESOURCE,
        binding.owner_id,
    )
    pump.start()
    lease.acquire(now=now)
    active_stream_session_id = binding.stream_session_id
    gaps = list(status.get("transport_gaps") or ())
    publisher = _AcquisitionStatusPublisher(
        status_path,
        binding,
        {
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
            "reserved_decoded_messages": decoded_messages,
            "reserved_decoded_bytes": decoded_bytes,
            "recovered_reserved_messages": recovered_reserved_messages,
            "recovered_reserved_bytes": recovered_reserved_bytes,
            "buffer_capacity_events": binding.buffer_event_limit,
            "buffer_capacity_bytes": binding.buffer_byte_limit,
            "last_raw_offset": int(status.get("last_raw_offset") or 0),
            "last_event_offset": int(
                status.get("last_event_offset") or 0
            ),
            "last_market_event_at_utc": status.get(
                "last_market_event_at_utc"
            ),
            "last_consumer_progress_at_utc": status.get(
                "last_consumer_progress_at_utc"
            ),
            "socket_state": GatewayFeedState.DISCONNECTED.value,
            "subscription_ready": False,
            "transport_gaps": gaps,
            "diagnostic": diagnostic,
            "forbidden_effects": dict(ZERO_FORBIDDEN_EFFECTS),
        },
        clock=clock,
    )
    publisher_holder[0] = publisher
    publisher.start()

    def stop_pauses_session() -> bool:
        """Only the authority's end or a spent ceiling ends a session. A stop
        before that leaves the journal open for a restart to continue."""
        return (
            stop_event.is_set()
            and not diagnostic
            and _utc(clock(), "clock")
            < binding.authority.acquisition_stop
            and attempts < binding.connection_attempt_ceiling
            and decoded_messages + CONTROL_MESSAGES_PER_ATTEMPT
            < binding.decoded_message_ceiling
            and decoded_bytes < binding.decoded_byte_ceiling
        )

    paused = False
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
            publisher.update(
                persist=True,
                state="CONNECTING",
                stream_session_id=str(active_stream_session_id),
                connection_attempts=attempts,
                decoded_messages=decoded_messages,
                decoded_bytes=decoded_bytes,
                socket_state=GatewayFeedState.CONNECTING.value,
                subscription_ready=False,
            )

            attempt_messages = decoded_messages
            attempt_bytes = decoded_bytes
            reserved_messages = min(
                binding.decoded_message_ceiling,
                attempt_messages
                + CONTROL_MESSAGES_PER_ATTEMPT
                + STATUS_MESSAGE_RESERVATION_QUANTUM,
            )
            reserved_bytes = min(
                binding.decoded_byte_ceiling,
                attempt_bytes
                + (
                    binding.maximum_message_bytes
                    * STATUS_BYTE_RESERVATION_MESSAGES
                ),
            )
            publisher.update(
                persist=True,
                reserved_decoded_messages=reserved_messages,
                reserved_decoded_bytes=reserved_bytes,
            )

            def usage(session_messages, session_bytes):
                nonlocal reserved_messages, reserved_bytes
                current_messages = (
                    attempt_messages
                    + CONTROL_MESSAGES_PER_ATTEMPT
                    + session_messages
                )
                current_bytes = attempt_bytes + session_bytes
                extend = False
                if _message_reservation_runs_low(
                    current_messages,
                    reserved_messages,
                    binding.decoded_message_ceiling,
                ):
                    reserved_messages = min(
                        binding.decoded_message_ceiling,
                        current_messages
                        + STATUS_MESSAGE_RESERVATION_QUANTUM,
                    )
                    extend = True
                if (
                    current_bytes + binding.maximum_message_bytes
                    > reserved_bytes
                    and reserved_bytes < binding.decoded_byte_ceiling
                ):
                    reserved_bytes = min(
                        binding.decoded_byte_ceiling,
                        current_bytes
                        + (
                            binding.maximum_message_bytes
                            * STATUS_BYTE_RESERVATION_MESSAGES
                        ),
                    )
                    extend = True
                publisher.update(
                    persist=extend,
                    decoded_messages=current_messages,
                    decoded_bytes=current_bytes,
                    reserved_decoded_messages=reserved_messages,
                    reserved_decoded_bytes=reserved_bytes,
                )

            def market_activity(_session_messages, _session_bytes):
                publisher.update(
                    last_market_event_at_utc=(
                        _utc(clock(), "clock").isoformat()
                    )
                )

            def gateway_state(value):
                ready = value in {
                    GatewayFeedState.WARMING,
                    GatewayFeedState.LIVE,
                }
                if ready and gaps and gaps[-1].get("status") == "OPEN":
                    gaps[-1] = {
                        **gaps[-1],
                        "status": "RECONNECTED",
                        "reconnected_at_utc": (
                            _utc(clock(), "clock").isoformat()
                        ),
                    }
                publisher.update(
                    persist=True,
                    state="CAPTURING" if ready else "CONNECTING",
                    socket_state=value.value,
                    subscription_ready=ready,
                    transport_gaps=list(gaps),
                )

            gateway = AdvancedGateway(
                configuration,
                manifest,
                buffer,
                api_key=api_key,
                connection_factory=connection_factory,
                utc_clock=clock,
                monotonic_ns=monotonic_ns,
                session_id_factory=lambda: active_stream_session_id,
                state_observer=gateway_state,
                usage_observer=usage,
                market_observer=market_activity,
            )
            try:
                result = asyncio.run(gateway.run_session(
                    maximum_market_messages=(
                        len(binding.contract_tickers)
                        if diagnostic
                        else remaining_messages
                    ),
                    maximum_received_bytes=remaining_bytes,
                    stop_requested=lambda: (
                        stop_event.is_set() or publisher.failed()
                    ),
                    stop_at=binding.authority.acquisition_stop,
                ))
            except (GatewayControlError, GatewayOverload) as error:
                publisher.update(
                    persist=True,
                    state="FAILED_PERMANENT",
                    subscription_ready=False,
                    failure_class="PERMANENT_PROVIDER_OR_SAFETY",
                    failure_code=getattr(
                        error,
                        "reason_code",
                        type(error).__name__,
                    ),
                )
                raise
            except Exception as error:
                # Local status publication is part of acquisition safety. An
                # I/O failure raised by a synchronous observer must not be
                # mistaken for a reconnectable provider transport failure.
                publisher.raise_if_failed()
                if not _transient_transport_failure(error):
                    publisher.update(
                        persist=True,
                        state="FAILED_INTERNAL",
                        subscription_ready=False,
                        failure_class="INTERNAL",
                        failure_code=type(error).__name__,
                    )
                    raise
                decoded_messages = (
                    attempt_messages
                    + CONTROL_MESSAGES_PER_ATTEMPT
                    + gateway.received_messages
                )
                decoded_bytes = attempt_bytes + gateway.received_bytes
                gaps.append({
                    "attempt": attempts,
                    "status": "OPEN",
                    "disconnected_at_utc": (
                        _utc(clock(), "clock").isoformat()
                    ),
                    "error_type": type(error).__name__,
                    **_close_detail(error),
                })
                publisher.update(
                    persist=True,
                    state="RECONNECTING",
                    socket_state=GatewayFeedState.DISCONNECTED.value,
                    subscription_ready=False,
                    decoded_messages=decoded_messages,
                    decoded_bytes=decoded_bytes,
                    reserved_decoded_messages=decoded_messages,
                    reserved_decoded_bytes=decoded_bytes,
                    transport_gaps=list(gaps),
                )
                stop_event.wait(
                    min(30.0, 0.5 * (2 ** (attempts - 1)))
                )
                continue
            publisher.raise_if_failed()
            decoded_messages += (
                result.received_messages
                + CONTROL_MESSAGES_PER_ATTEMPT
            )
            decoded_bytes += result.received_bytes
            publisher.update(
                persist=True,
                state="CAPTURING",
                decoded_messages=decoded_messages,
                decoded_bytes=decoded_bytes,
                reserved_decoded_messages=decoded_messages,
                reserved_decoded_bytes=decoded_bytes,
            )
            if result.stop_reason is not None:
                break
            if diagnostic:
                while not stop_event.wait(0.1):
                    publisher.update(state="READY")
                break
            stop_event.wait(min(30.0, 0.5 * (2 ** (attempts - 1))))
    finally:
        status_failure = None
        paused = stop_pauses_session()
        try:
            if not str(publisher.snapshot().get("state", "")).startswith(
                "FAILED_"
            ):
                publisher.update(
                    persist=True,
                    state="PAUSED" if paused else "DRAINING",
                    subscription_ready=False,
                )
        except BaseException as error:
            status_failure = error
        pump.request_stop()
        final = pump.join(30)
        try:
            publisher.update(
                persist=True,
                last_raw_offset=final.last_raw_offset,
                last_event_offset=final.last_event_offset,
                last_consumer_progress_at_utc=(
                    _utc(clock(), "clock").isoformat()
                ),
            )
            publisher.stop()
        except BaseException as error:
            status_failure = status_failure or error
        finally:
            lease.release(now=_utc(clock(), "clock"))
        if status_failure is not None:
            raise RuntimeError(
                "Options acquisition stopped after status persistence failure"
            ) from status_failure
    if paused:
        # The socket is down until a restart, so the journal has a hole here.
        gaps.append({
            "attempt": attempts,
            "status": "OPEN",
            "disconnected_at_utc": _utc(clock(), "clock").isoformat(),
            "error_type": "STOP_REQUESTED",
        })
        publisher.update(
            persist=True,
            state="PAUSED",
            socket_state=GatewayFeedState.STOPPED.value,
            subscription_ready=False,
            stream_session_id=str(active_stream_session_id),
            connection_attempts=attempts,
            decoded_messages=decoded_messages,
            decoded_bytes=decoded_bytes,
            reserved_decoded_messages=decoded_messages,
            reserved_decoded_bytes=decoded_bytes,
            transport_gaps=list(gaps),
        )
        return publisher.snapshot()
    if final.last_raw_offset <= 0 or final.last_event_offset <= 0:
        publisher.update(
            persist=True,
            state="FAILED_NO_EVIDENCE",
            failure_class="NO_MARKET_EVIDENCE",
        )
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
    current_status = publisher.snapshot()
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
        "reserved_decoded_messages": decoded_messages,
        "reserved_decoded_bytes": decoded_bytes,
        "recovered_reserved_messages": recovered_reserved_messages,
        "recovered_reserved_bytes": recovered_reserved_bytes,
        "buffer_capacity_events": binding.buffer_event_limit,
        "buffer_capacity_bytes": binding.buffer_byte_limit,
        "last_raw_offset": final.last_raw_offset,
        "last_event_offset": final.last_event_offset,
        "runtime_heartbeat_at_utc": _utc(
            clock(), "clock"
        ).isoformat(),
        "observed_at_utc": _utc(clock(), "clock").isoformat(),
        "socket_state": GatewayFeedState.STOPPED.value,
        "subscription_ready": False,
        "last_market_event_at_utc": current_status.get(
            "last_market_event_at_utc"
        ),
        "last_consumer_progress_at_utc": current_status.get(
            "last_consumer_progress_at_utc"
        ),
        "transport_gaps": list(gaps),
        "journal_path": str(journal_path),
        "journal_integrity": recovery.integrity_check,
        "quarantine_count": recovery.quarantine_count,
        "diagnostic": diagnostic,
        "forbidden_effects": dict(ZERO_FORBIDDEN_EFFECTS),
    }
    report["semantic_sha256"] = canonical_sha256(report)
    _write_json(status_path, report)
    return report
