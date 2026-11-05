"""Repository-owned P8 runtime composition and Docker execution bundle."""
from __future__ import annotations

import json
import os
import signal
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg2

from .advanced_options_control_plane import (
    advanced_options_equity_input_binding_payload,
    advanced_options_enrollment_plan_payload,
    advanced_options_history_binding_payload,
)
from .advanced_options_acquisition_runtime import (
    AdvancedOptionsAcquisitionBinding,
    acquisition_binding_from_payload,
    acquisition_binding_payload,
)
from .advanced_options_p8_canary_authorization import canonical_sha256
from .advanced_options_p8_canary_executor import (
    AdvancedOptionsP8LifecyclePort,
    P8BindingCapturePort,
    P8CanaryExecutionBinding,
    P8StoredReadPort,
)
from .advanced_options_p8_docker import (
    AdvancedOptionsDockerServicePort,
    P8_SERVICE_NAMES,
    isolated_p8_compose_environment,
    validate_p8_service_membership,
)
from .advanced_options_service import (
    AdvancedOptionsProcessKind,
    AdvancedOptionsResidentLauncher,
    build_owp3_advanced_options_resident_plan,
)
from .advanced_options_selection_c6 import C6SelectionPlan
from .advanced_realtime_fencing import (
    DurableRuntimeLease,
    claim_durable_runtime_lease_cursor,
    renew_durable_runtime_lease_cursor,
)
from .migrations import EXPECTED_V2_DATABASE_NAME
from .advanced_options_underlying_snapshot import (
    underlying_snapshot_binding_payload,
)
from .portal_projection_repository import PortalReleasePointer
from .portal_publisher import (
    PortalDeliverySink,
    PortalPublicationBatch,
    PortalPublicationConfig,
)
from .preflight import ProviderTier
from .stage_orchestrator import StageOrchestrator
from .stage_graph import STAGE_CATALOG_VERSION_V5
from .worker_database import RealtimeProcessDatabase


UTC = timezone.utc
P8_EXECUTION_BUNDLE_SCHEMA = "advanced-options-p8-execution-bundle-v3"
P8_EXECUTION_BUNDLE_MAX_BYTES = 1_048_576
P8_HEARTBEAT_INTERVAL_SECONDS = 10
P8_SERVICE_LEASE_SECONDS = 30
P8_EFFECT_DENIALS = {
    "dispatch": False,
    "delivery": False,
    "broker": False,
    "trading": False,
}
P8_STATUS_SCHEMA = "advanced-options-p8-docker-service-status-v1"
P8_STATUS_MAXIMUM_AGE_SECONDS = 20
P8_READY_WAITING_FOR_E = "READY_WAITING_FOR_E"
P8_BOUND_TO_E = "BOUND_TO_E"
P8_PREOPEN_SESSION_BINDING_SCHEMA = (
    "advanced-options-p8-preopen-session-binding-v1"
)
P8_DOWNSTREAM_BINDING_SCHEMA = "advanced-options-p8-downstream-binding-v2"
P8_DOWNSTREAM_LATEST_SCHEMA = (
    "advanced-options-p8-downstream-binding-latest-v1"
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path.name} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _resident_acquisition_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            "V3 acquisition binding is unavailable"
        ) from error


def _sha256(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class P8PreopenSessionBinding:
    binding_id: UUID
    binding_sha256: str
    package_sha256: str
    acquisition_binding_id: UUID
    acquisition_binding_sha256: str
    source_sha256: str
    backend_image_id: str
    session_authorization_id: UUID
    session_authorization_sha256: str
    cohort_id: UUID
    plan_id: UUID
    journal_generation: UUID
    contract_tickers: tuple[str, ...]
    service_owner_ids: tuple[tuple[str, UUID], ...]
    decision_stop: datetime

    def __post_init__(self) -> None:
        contracts = tuple(sorted(
            str(value).strip().upper()
            for value in self.contract_tickers
        ))
        owners = tuple(sorted(
            (str(kind), owner_id)
            for kind, owner_id in self.service_owner_ids
        ))
        decision_stop = _utc(self.decision_stop, "decision_stop")
        if (
            not all(
                isinstance(value, UUID) and value.int
                for value in (
                    self.binding_id,
                    self.acquisition_binding_id,
                    self.session_authorization_id,
                    self.cohort_id,
                    self.plan_id,
                    self.journal_generation,
                )
            )
            or not contracts
            or len(set(contracts)) != len(contracts)
            or [kind for kind, _owner_id in owners]
            != sorted(kind.value for kind in AdvancedOptionsProcessKind)
            or any(
                not isinstance(owner_id, UUID) or not owner_id.int
                for _kind, owner_id in owners
            )
            or not isinstance(self.backend_image_id, str)
            or not self.backend_image_id.startswith("sha256:")
        ):
            raise ValueError("P8 pre-open session binding scope is invalid")
        for value, name in (
            (self.binding_sha256, "binding_sha256"),
            (self.package_sha256, "package_sha256"),
            (
                self.acquisition_binding_sha256,
                "acquisition_binding_sha256",
            ),
            (self.source_sha256, "source_sha256"),
            (
                self.session_authorization_sha256,
                "session_authorization_sha256",
            ),
            (
                self.backend_image_id.removeprefix("sha256:"),
                "backend_image_id",
            ),
        ):
            _sha256(value, name)
        object.__setattr__(self, "contract_tickers", contracts)
        object.__setattr__(self, "service_owner_ids", owners)
        object.__setattr__(self, "decision_stop", decision_stop)
        expected = canonical_sha256(
            p8_preopen_session_binding_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:p8-preopen-session-binding:{expected}",
        )
        if (
            self.binding_sha256 != expected
            or self.binding_id != expected_id
        ):
            raise ValueError("P8 pre-open session binding identity changed")


def p8_preopen_session_binding_payload(
    binding: P8PreopenSessionBinding,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": P8_PREOPEN_SESSION_BINDING_SCHEMA,
        "package_sha256": binding.package_sha256,
        "acquisition_binding_id": str(binding.acquisition_binding_id),
        "acquisition_binding_sha256": (
            binding.acquisition_binding_sha256
        ),
        "source_sha256": binding.source_sha256,
        "backend_image_id": binding.backend_image_id,
        "session_authorization_id": str(
            binding.session_authorization_id
        ),
        "session_authorization_sha256": (
            binding.session_authorization_sha256
        ),
        "cohort_id": str(binding.cohort_id),
        "plan_id": str(binding.plan_id),
        "journal_generation": str(binding.journal_generation),
        "contract_tickers": list(binding.contract_tickers),
        "service_owner_ids": [
            {"kind": kind, "owner_id": str(owner_id)}
            for kind, owner_id in binding.service_owner_ids
        ],
        "decision_stop_utc": binding.decision_stop.isoformat(),
        "authority": {
            "provider_socket": False,
            "journal": False,
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
        "state": P8_READY_WAITING_FOR_E,
    }
    if include_identity:
        value.update({
            "binding_id": str(binding.binding_id),
            "binding_sha256": binding.binding_sha256,
        })
    return value


def build_p8_preopen_session_binding(
    *,
    package: Mapping[str, object],
    acquisition_binding: AdvancedOptionsAcquisitionBinding,
) -> P8PreopenSessionBinding:
    cohort = package.get("cohort")
    orchestration = package.get("orchestration")
    owners = package.get("owners")
    processes = (
        owners.get("processes")
        if isinstance(owners, Mapping)
        else None
    )
    if (
        not isinstance(cohort, Mapping)
        or not isinstance(orchestration, Mapping)
        or not isinstance(processes, list)
    ):
        raise ValueError("P8 pre-open package identity is unavailable")
    cohort_id = UUID(str(cohort["cohort_id"]))
    plan_id = UUID(str(orchestration["plan_id"]))
    service_owner_ids = tuple(sorted(
        (
            str(row["kind"]),
            _owner_ids(package)[
                AdvancedOptionsProcessKind(str(row["kind"]))
            ],
        )
        for row in processes
    ))
    if (
        cohort_id != acquisition_binding.authority.cohort_id
        or tuple(sorted(
            str(value)
            for value in package.get("v3_admitted_contracts", ())
        ))
        != acquisition_binding.contract_tickers
    ):
        raise ValueError("P8 pre-open acquisition identity changed")
    values = {
        "package_sha256": _sha256(
            package.get("package_sha256"),
            "package_sha256",
        ),
        "acquisition_binding_id": acquisition_binding.binding_id,
        "acquisition_binding_sha256": (
            acquisition_binding.binding_sha256
        ),
        "source_sha256": acquisition_binding.source_sha256,
        "backend_image_id": acquisition_binding.backend_image_id,
        "session_authorization_id": (
            acquisition_binding.authority.authorization_id
        ),
        "session_authorization_sha256": (
            acquisition_binding.authority.authorization_sha256
        ),
        "cohort_id": cohort_id,
        "plan_id": plan_id,
        "journal_generation": (
            acquisition_binding.authority.journal_generation
        ),
        "contract_tickers": acquisition_binding.contract_tickers,
        "service_owner_ids": service_owner_ids,
        "decision_stop": acquisition_binding.authority.acquisition_stop,
    }
    unsigned = P8PreopenSessionBinding.__new__(
        P8PreopenSessionBinding
    )
    object.__setattr__(unsigned, "binding_id", UUID(int=1))
    object.__setattr__(unsigned, "binding_sha256", "0" * 64)
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        p8_preopen_session_binding_payload(
            unsigned,
            include_identity=False,
        )
    )
    return P8PreopenSessionBinding(
        binding_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:p8-preopen-session-binding:{digest}",
        ),
        binding_sha256=digest,
        **values,
    )


def p8_preopen_session_binding_from_payload(
    value: object,
) -> P8PreopenSessionBinding:
    if not isinstance(value, Mapping):
        raise TypeError("P8 pre-open session binding must be an object")
    if value.get("schema") != P8_PREOPEN_SESSION_BINDING_SCHEMA:
        raise ValueError("P8 pre-open session binding schema changed")
    binding = P8PreopenSessionBinding(
        binding_id=UUID(str(value["binding_id"])),
        binding_sha256=str(value["binding_sha256"]),
        package_sha256=str(value["package_sha256"]),
        acquisition_binding_id=UUID(
            str(value["acquisition_binding_id"])
        ),
        acquisition_binding_sha256=str(
            value["acquisition_binding_sha256"]
        ),
        source_sha256=str(value["source_sha256"]),
        backend_image_id=str(value["backend_image_id"]),
        session_authorization_id=UUID(
            str(value["session_authorization_id"])
        ),
        session_authorization_sha256=str(
            value["session_authorization_sha256"]
        ),
        cohort_id=UUID(str(value["cohort_id"])),
        plan_id=UUID(str(value["plan_id"])),
        journal_generation=UUID(str(value["journal_generation"])),
        contract_tickers=tuple(
            str(item) for item in value["contract_tickers"]
        ),
        service_owner_ids=tuple(
            (
                str(row["kind"]),
                UUID(str(row["owner_id"])),
            )
            for row in value["service_owner_ids"]
        ),
        decision_stop=datetime.fromisoformat(
            str(value["decision_stop_utc"])
        ),
    )
    if p8_preopen_session_binding_payload(binding) != dict(value):
        raise ValueError("P8 pre-open session binding payload changed")
    return binding


@dataclass(frozen=True, slots=True)
class P8DownstreamBinding:
    binding_id: UUID
    binding_sha256: str
    session_binding_sha256: str
    sequence: int
    previous_binding_sha256: str | None
    target_window_id: UUID
    equity_receipt_id: UUID
    equity_input_sha256: str
    finalized_cutoff: datetime
    receipt_created_at: datetime
    options_cutoff: datetime
    history_binding_sha256: str
    equity_binding_sha256: str
    options_activity_evidence_sha256: str
    window_cycle_authorization_sha256: str
    publication_authorization_reference: str
    production_binding: Mapping[str, object]

    def __post_init__(self) -> None:
        finalized = _utc(self.finalized_cutoff, "finalized_cutoff")
        created = _utc(self.receipt_created_at, "receipt_created_at")
        options = _utc(self.options_cutoff, "options_cutoff")
        production = dict(self.production_binding)
        if (
            not all(
                isinstance(value, UUID) and value.int
                for value in (
                    self.binding_id,
                    self.target_window_id,
                    self.equity_receipt_id,
                )
            )
            or type(self.sequence) is not int
            or not 1 <= self.sequence <= 78
            or not finalized <= created <= options
            or not 0 <= (options - finalized).total_seconds() <= 300
            or not self.publication_authorization_reference.strip()
            or production.get("schema")
            != "advanced-options-p8-production-binding-v3"
            or production.get("window_id")
            != str(self.target_window_id)
        ):
            raise ValueError("P8 downstream binding scope is invalid")
        for value, name in (
            (self.binding_sha256, "binding_sha256"),
            (
                self.session_binding_sha256,
                "session_binding_sha256",
            ),
            (
                self.previous_binding_sha256,
                "previous_binding_sha256",
            ),
            (self.equity_input_sha256, "equity_input_sha256"),
            (
                self.history_binding_sha256,
                "history_binding_sha256",
            ),
            (self.equity_binding_sha256, "equity_binding_sha256"),
            (
                self.options_activity_evidence_sha256,
                "options_activity_evidence_sha256",
            ),
            (
                self.window_cycle_authorization_sha256,
                "window_cycle_authorization_sha256",
            ),
            (production.get("binding_sha256"), "production_binding"),
        ):
            if value is not None:
                _sha256(value, name)
        equity = production.get("equity_input_binding")
        control = production.get("control")
        history = production.get("history_binding")
        if (
            not isinstance(equity, Mapping)
            or not isinstance(control, Mapping)
            or not isinstance(history, Mapping)
            or equity.get("equity_receipt_id")
            != str(self.equity_receipt_id)
            or equity.get("input_sha256") != self.equity_input_sha256
            or equity.get("equity_cutoff") != finalized.isoformat()
            or equity.get("binding_sha256")
            != self.equity_binding_sha256
            or history.get("binding_sha256")
            != self.history_binding_sha256
            or control.get("receipt_inputs", {}).get("cutoff")
            != options.isoformat()
            or canonical_sha256({
                key: item
                for key, item in production.items()
                if key != "binding_sha256"
            })
            != production.get("binding_sha256")
        ):
            raise ValueError("P8 downstream production binding changed")
        object.__setattr__(self, "finalized_cutoff", finalized)
        object.__setattr__(self, "receipt_created_at", created)
        object.__setattr__(self, "options_cutoff", options)
        object.__setattr__(self, "production_binding", production)
        expected = canonical_sha256(
            p8_downstream_binding_payload(
                self,
                include_identity=False,
            )
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:p8-downstream-binding:{expected}",
        )
        if (
            self.binding_sha256 != expected
            or self.binding_id != expected_id
        ):
            raise ValueError("P8 downstream binding identity changed")


def p8_downstream_binding_payload(
    binding: P8DownstreamBinding,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": P8_DOWNSTREAM_BINDING_SCHEMA,
        "session_binding_sha256": binding.session_binding_sha256,
        "sequence": binding.sequence,
        "previous_binding_sha256": binding.previous_binding_sha256,
        "target_window_id": str(binding.target_window_id),
        "equity_receipt_id": str(binding.equity_receipt_id),
        "equity_input_sha256": binding.equity_input_sha256,
        "finalized_cutoff_utc": binding.finalized_cutoff.isoformat(),
        "receipt_created_at_utc": binding.receipt_created_at.isoformat(),
        "options_cutoff_utc": binding.options_cutoff.isoformat(),
        "e_o_seconds": (
            binding.options_cutoff - binding.finalized_cutoff
        ).total_seconds(),
        "history_binding_sha256": binding.history_binding_sha256,
        "equity_binding_sha256": binding.equity_binding_sha256,
        "options_activity_evidence_sha256": (
            binding.options_activity_evidence_sha256
        ),
        "window_cycle_authorization_sha256": (
            binding.window_cycle_authorization_sha256
        ),
        "publication_authorization_reference": (
            binding.publication_authorization_reference
        ),
        "production_binding": dict(binding.production_binding),
    }
    if include_identity:
        value.update({
            "binding_id": str(binding.binding_id),
            "binding_sha256": binding.binding_sha256,
        })
    return value


def build_p8_downstream_binding(
    *,
    session_binding: P8PreopenSessionBinding,
    sequence: int,
    previous_binding_sha256: str | None,
    target_window_id: UUID,
    equity_receipt_id: UUID,
    equity_input_sha256: str,
    finalized_cutoff: datetime,
    receipt_created_at: datetime,
    options_cutoff: datetime,
    history_binding_sha256: str,
    equity_binding_sha256: str,
    options_activity_evidence_sha256: str,
    window_cycle_authorization_sha256: str,
    publication_authorization_reference: str,
    production_binding: Mapping[str, object],
) -> P8DownstreamBinding:
    values = {
        "session_binding_sha256": session_binding.binding_sha256,
        "sequence": sequence,
        "previous_binding_sha256": previous_binding_sha256,
        "target_window_id": target_window_id,
        "equity_receipt_id": equity_receipt_id,
        "equity_input_sha256": equity_input_sha256,
        "finalized_cutoff": finalized_cutoff,
        "receipt_created_at": receipt_created_at,
        "options_cutoff": options_cutoff,
        "history_binding_sha256": history_binding_sha256,
        "equity_binding_sha256": equity_binding_sha256,
        "options_activity_evidence_sha256": (
            options_activity_evidence_sha256
        ),
        "window_cycle_authorization_sha256": (
            window_cycle_authorization_sha256
        ),
        "publication_authorization_reference": (
            publication_authorization_reference
        ),
        "production_binding": dict(production_binding),
    }
    unsigned = P8DownstreamBinding.__new__(P8DownstreamBinding)
    object.__setattr__(unsigned, "binding_id", UUID(int=1))
    object.__setattr__(unsigned, "binding_sha256", "0" * 64)
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        p8_downstream_binding_payload(
            unsigned,
            include_identity=False,
        )
    )
    return P8DownstreamBinding(
        binding_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:p8-downstream-binding:{digest}",
        ),
        binding_sha256=digest,
        **values,
    )


def p8_downstream_binding_from_payload(
    value: object,
) -> P8DownstreamBinding:
    if not isinstance(value, Mapping):
        raise TypeError("P8 downstream binding must be an object")
    if value.get("schema") != P8_DOWNSTREAM_BINDING_SCHEMA:
        raise ValueError("P8 downstream binding schema changed")
    binding = P8DownstreamBinding(
        binding_id=UUID(str(value["binding_id"])),
        binding_sha256=str(value["binding_sha256"]),
        session_binding_sha256=str(value["session_binding_sha256"]),
        sequence=int(value["sequence"]),
        previous_binding_sha256=(
            None
            if value.get("previous_binding_sha256") is None
            else str(value["previous_binding_sha256"])
        ),
        target_window_id=UUID(str(value["target_window_id"])),
        equity_receipt_id=UUID(str(value["equity_receipt_id"])),
        equity_input_sha256=str(value["equity_input_sha256"]),
        finalized_cutoff=datetime.fromisoformat(
            str(value["finalized_cutoff_utc"])
        ),
        receipt_created_at=datetime.fromisoformat(
            str(value["receipt_created_at_utc"])
        ),
        options_cutoff=datetime.fromisoformat(
            str(value["options_cutoff_utc"])
        ),
        history_binding_sha256=str(value["history_binding_sha256"]),
        equity_binding_sha256=str(value["equity_binding_sha256"]),
        options_activity_evidence_sha256=str(
            value["options_activity_evidence_sha256"]
        ),
        window_cycle_authorization_sha256=str(
            value["window_cycle_authorization_sha256"]
        ),
        publication_authorization_reference=str(
            value["publication_authorization_reference"]
        ),
        production_binding=dict(value["production_binding"]),
    )
    if p8_downstream_binding_payload(binding) != dict(value):
        raise ValueError("P8 downstream binding payload changed")
    return binding


class P8DownstreamBindingStore:
    def __init__(
        self,
        root: Path,
        *,
        session_binding: P8PreopenSessionBinding,
        create: bool = False,
    ) -> None:
        self.root = root.resolve()
        self.session_binding = session_binding
        if create:
            self.root.mkdir(parents=True, exist_ok=False)
        if not self.root.is_dir():
            raise ValueError("P8 downstream binding directory is unavailable")

    @property
    def latest_path(self) -> Path:
        return self.root / "LATEST.json"

    def _binding_path(self, sequence: int) -> Path:
        return self.root / f"{sequence:03d}.json"

    def _binding_partial_path(self, sequence: int) -> Path:
        return self.root / f"{sequence:03d}.json.partial"

    def _read_chain(self) -> tuple[P8DownstreamBinding, ...]:
        bindings = tuple(
            p8_downstream_binding_from_payload(_json(path))
            for path in sorted(self.root.glob("[0-9][0-9][0-9].json"))
        )
        previous = None
        for expected_sequence, binding in enumerate(bindings, start=1):
            if (
                binding.session_binding_sha256
                != self.session_binding.binding_sha256
                or binding.sequence != expected_sequence
                or binding.previous_binding_sha256 != previous
            ):
                raise RuntimeError("P8 downstream binding chain changed")
            previous = binding.binding_sha256
        return bindings

    def _validate_pointer(
        self,
        bindings: tuple[P8DownstreamBinding, ...],
    ) -> bool:
        """Return False only for a bounded concurrent publication race."""
        if not self.latest_path.exists():
            # A complete numbered tail is itself durable and recoverable. The
            # pointer is an acceleration/index record, not the commit record.
            return True
        latest = _json(self.latest_path)
        sequence = latest.get("sequence")
        if (
            latest.get("schema") != P8_DOWNSTREAM_LATEST_SCHEMA
            or type(sequence) is not int
            or sequence < 1
        ):
            raise RuntimeError("P8 downstream latest binding changed")
        if sequence > len(bindings):
            return False
        pointed = bindings[sequence - 1]
        if (
            latest.get("binding_sha256") != pointed.binding_sha256
            or latest.get("file") != self._binding_path(sequence).name
        ):
            raise RuntimeError("P8 downstream latest binding changed")
        return True

    def bindings(self) -> tuple[P8DownstreamBinding, ...]:
        for _ in range(3):
            bindings = self._read_chain()
            if self._validate_pointer(bindings):
                return bindings
        raise RuntimeError("P8 downstream latest binding raced publication")

    def latest(self) -> P8DownstreamBinding | None:
        bindings = self.bindings()
        return None if not bindings else bindings[-1]

    def attach(
        self,
        binding: P8DownstreamBinding,
    ) -> P8DownstreamBinding:
        if (
            binding.session_binding_sha256
            != self.session_binding.binding_sha256
        ):
            raise ValueError("P8 downstream binding targets another session")
        latest = self.latest()
        expected_sequence = 1 if latest is None else latest.sequence + 1
        expected_previous = (
            None if latest is None else latest.binding_sha256
        )
        path = self._binding_path(binding.sequence)
        if path.exists():
            stored = p8_downstream_binding_from_payload(_json(path))
            if stored != binding:
                raise RuntimeError("P8 downstream binding replay diverged")
            self._publish_latest(latest or stored)
            return stored
        if (
            binding.sequence != expected_sequence
            or binding.previous_binding_sha256 != expected_previous
            or (
                latest is not None
                and binding.finalized_cutoff <= latest.finalized_cutoff
            )
        ):
            raise ValueError("P8 downstream binding order changed")
        encoded = (
            json.dumps(
                p8_downstream_binding_payload(binding),
                indent=2,
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        temporary_binding = self._binding_partial_path(binding.sequence)
        if temporary_binding.exists():
            try:
                partial = p8_downstream_binding_from_payload(
                    _json(temporary_binding)
                )
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
                partial = None
            if partial == binding:
                os.replace(temporary_binding, path)
            else:
                temporary_binding.unlink()
        if not path.exists():
            with temporary_binding.open("xb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_binding, path)
        self._publish_latest(binding)
        return binding

    def _publish_latest(self, binding: P8DownstreamBinding) -> None:
        latest_value = {
            "schema": P8_DOWNSTREAM_LATEST_SCHEMA,
            "sequence": binding.sequence,
            "binding_sha256": binding.binding_sha256,
            "file": self._binding_path(binding.sequence).name,
        }
        temporary = self.latest_path.with_suffix(".partial")
        with temporary.open("wb") as stream:
            stream.write((
                json.dumps(latest_value, indent=2, sort_keys=True) + "\n"
            ).encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.latest_path)


def _inside(root: Path, path: Path, name: str) -> Path:
    resolved_root = root.resolve()
    resolved = path.resolve()
    try:
        resolved.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError(f"P8 {name} escaped the repository root") from error
    return resolved


class P8RuntimeComponentsFactory(Protocol):
    def __call__(
        self,
        *,
        package: Mapping[str, object],
        repository_root: Path,
        stored_reads_authorized: bool,
    ) -> "P8RuntimeComponents": ...


@dataclass(frozen=True, slots=True)
class P8RuntimeComponents:
    capture: P8BindingCapturePort
    enrollment_connection: object
    database: object
    orchestrator: StageOrchestrator
    publisher_database: RealtimeProcessDatabase
    publication_config: PortalPublicationConfig
    publication_batch: Callable[
        [P8CanaryExecutionBinding, Mapping[str, object]],
        PortalPublicationBatch,
    ]
    publication_observation: Callable[
        [P8CanaryExecutionBinding, PortalReleasePointer, datetime],
        Mapping[str, object],
    ]
    stored_reads: P8StoredReadPort | None = None
    delivery_sink: PortalDeliverySink | None = None
    cleanup: Callable[
        [P8CanaryExecutionBinding], Mapping[str, object]
    ] = lambda _binding: {
        "temporary_files": [],
        "processes": [],
        "locks": [],
    }
    close: Callable[[], None] = lambda: None
    clock: Callable[[], datetime] | None = None

    def __post_init__(self) -> None:
        if self.publication_config.dispatch_authorized:
            raise ValueError("P8 runtime components must deny dispatch")
        for value, name in (
            (self.publication_batch, "publication batch"),
            (self.publication_observation, "publication observation"),
            (self.cleanup, "cleanup"),
            (self.close, "close"),
        ):
            if not callable(value):
                raise TypeError(f"P8 {name} callback is invalid")
        if self.clock is not None and not callable(self.clock):
            raise TypeError("P8 runtime clock is invalid")


class P8ExecutionBundleStore:
    def __init__(
        self,
        path: Path,
        *,
        package: Mapping[str, object],
        package_path: Path,
        repository_root: Path,
    ) -> None:
        validate_p8_service_membership(package)
        self.path = _inside(
            repository_root,
            path,
            "execution bundle path",
        )
        self.package_path = _inside(
            repository_root,
            package_path,
            "package path",
        )
        if not self.package_path.is_file():
            raise ValueError("P8 package path is absent")
        if not self.path.parent.is_dir():
            raise ValueError("P8 execution bundle parent is absent")
        self.package = package

    def write_waiting(
        self,
        acquisition_binding: AdvancedOptionsAcquisitionBinding,
        *,
        written_at: datetime,
    ) -> Mapping[str, object]:
        if self.path.exists():
            raise FileExistsError("P8 execution bundle must be new")
        if not isinstance(
            acquisition_binding,
            AdvancedOptionsAcquisitionBinding,
        ):
            raise TypeError(
                "P8 waiting bundle requires an acquisition binding"
            )
        payload = build_p8_waiting_execution_bundle(
            package=self.package,
            acquisition_binding=acquisition_binding,
            written_at=written_at,
            package_path_reference=str(self.package_path),
        )
        encoded = (
            json.dumps(payload, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        if len(encoded) > P8_EXECUTION_BUNDLE_MAX_BYTES:
            raise ValueError("P8 execution bundle exceeded its byte ceiling")
        with self.path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        return payload

    def write(
        self,
        observation: Mapping[str, object],
        binding: P8CanaryExecutionBinding,
        *,
        written_at: datetime,
        production_binding: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        if self.path.exists():
            raise FileExistsError("P8 execution bundle must be new")
        if not isinstance(binding, P8CanaryExecutionBinding):
            raise TypeError("P8 execution bundle requires a typed binding")
        checked = _utc(written_at, "written_at")
        control = binding.enrollment.control_plane
        deployment = self.package.get("v3_deployment")
        authority_references = None
        if isinstance(deployment, Mapping):
            from .advanced_options_v3_deployment import (
                build_advanced_options_v3_authority_references,
            )

            authorities = self.package.get("v3_authorities")
            if not isinstance(authorities, Mapping):
                raise ValueError(
                    "V3 package session acquisition authority is unavailable"
                )
            authority_references = (
                build_advanced_options_v3_authority_references(
                    authorities.get("session_acquisition"),
                    target_window_id=binding.window_id,
                )
            )
        payload = {
            "schema": P8_EXECUTION_BUNDLE_SCHEMA,
            "package_sha256": self.package["package_sha256"],
            "package_nonce": self.package["package_nonce"],
            "package_path": str(self.package_path),
            "window": dict(self.package["window"]),
            "cohort_id": str(control.cohort_id),
            "plan_id": str(control.plan_id),
            "window_id": str(binding.window_id),
            "orchestration": dict(self.package["orchestration"]),
            "v3_authorities": (
                None
                if self.package.get("v3_authorities") is None
                else dict(self.package["v3_authorities"])
            ),
            "v3_authority_references": authority_references,
            "validation_mode": self.package.get("validation_mode"),
            "written_at_utc": checked.isoformat(),
            "services": [
                {
                    "kind": row["kind"],
                    "owner_name": row["owner_name"],
                    "lease_resource_key": row["lease_resource_key"],
                    "owner_id": str(_owner_ids(self.package)[
                        AdvancedOptionsProcessKind(str(row["kind"]))
                    ]),
                    "service_name": P8_SERVICE_NAMES[
                        AdvancedOptionsProcessKind(str(row["kind"]))
                    ],
                }
                for row in self.package["owners"]["processes"]
            ],
            "enrollment": advanced_options_enrollment_plan_payload(
                binding.enrollment
            ),
            "history_binding": advanced_options_history_binding_payload(
                binding.history
            ),
            "equity_input_binding": (
                advanced_options_equity_input_binding_payload(
                    binding.equity_input
                )
            ),
            "underlying_price_binding": (
                underlying_snapshot_binding_payload(
                    binding.underlying_price_binding
                )
            ),
            "session_binding": None,
            "acquisition_binding": None,
            "production_binding": (
                None
                if production_binding is None
                else dict(production_binding)
            ),
            "live_observation_sha256": canonical_sha256(observation),
            "effect_authorizations": dict(P8_EFFECT_DENIALS),
        }
        payload["bundle_sha256"] = canonical_sha256(payload)
        encoded = (
            json.dumps(payload, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        if len(encoded) > P8_EXECUTION_BUNDLE_MAX_BYTES:
            raise ValueError("P8 execution bundle exceeded its byte ceiling")
        with self.path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        return payload

    def load(
        self,
        *,
        expected_kind: AdvancedOptionsProcessKind | None = None,
        checked_at: datetime | None = None,
    ) -> dict[str, object]:
        return LegacyArtifactSessionStateAdapter(
            package_path=self.package_path,
            bundle_path=self.path,
        ).load_execution_bundle(
            expected_kind=expected_kind,
            checked_at=checked_at,
        )


def build_p8_waiting_execution_bundle(
    *,
    package: Mapping[str, object],
    acquisition_binding: AdvancedOptionsAcquisitionBinding,
    written_at: datetime,
    package_path_reference: str,
) -> dict[str, object]:
    validate_p8_service_membership(package)
    if not isinstance(
        acquisition_binding,
        AdvancedOptionsAcquisitionBinding,
    ):
        raise TypeError("P8 waiting bundle requires an acquisition binding")
    reference = package_path_reference.strip()
    if not reference:
        raise ValueError("P8 package path reference is unavailable")
    checked = _utc(written_at, "written_at")
    session_binding = build_p8_preopen_session_binding(
        package=package,
        acquisition_binding=acquisition_binding,
    )
    payload = {
        "schema": P8_EXECUTION_BUNDLE_SCHEMA,
        "package_sha256": package["package_sha256"],
        "package_nonce": package["package_nonce"],
        "package_path": reference,
        "window": dict(package["window"]),
        "cohort_id": str(session_binding.cohort_id),
        "plan_id": str(session_binding.plan_id),
        "window_id": None,
        "orchestration": dict(package["orchestration"]),
        "v3_authorities": dict(package["v3_authorities"]),
        "v3_authority_references": None,
        "validation_mode": package.get("validation_mode"),
        "written_at_utc": checked.isoformat(),
        "services": [
            {
                "kind": row["kind"],
                "owner_name": row["owner_name"],
                "lease_resource_key": row["lease_resource_key"],
                "owner_id": str(_owner_ids(package)[
                    AdvancedOptionsProcessKind(str(row["kind"]))
                ]),
                "service_name": P8_SERVICE_NAMES[
                    AdvancedOptionsProcessKind(str(row["kind"]))
                ],
            }
            for row in package["owners"]["processes"]
        ],
        "session_binding": p8_preopen_session_binding_payload(
            session_binding
        ),
        "acquisition_binding": acquisition_binding_payload(
            acquisition_binding
        ),
        "enrollment": None,
        "history_binding": None,
        "equity_input_binding": None,
        "underlying_price_binding": None,
        "production_binding": None,
        "live_observation_sha256": canonical_sha256({
            "schema": P8_PREOPEN_SESSION_BINDING_SCHEMA,
            "session_binding_sha256": session_binding.binding_sha256,
            "acquisition_binding_sha256": (
                acquisition_binding.binding_sha256
            ),
            "state": P8_READY_WAITING_FOR_E,
        }),
        "effect_authorizations": dict(P8_EFFECT_DENIALS),
    }
    payload["bundle_sha256"] = canonical_sha256(payload)
    encoded = json.dumps(
        payload,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) + 1 > P8_EXECUTION_BUNDLE_MAX_BYTES:
        raise ValueError("P8 execution bundle exceeded its byte ceiling")
    return payload


def validate_p8_execution_bundle(
    package_path: Path,
    bundle_path: Path,
    *,
    expected_kind: AdvancedOptionsProcessKind | None = None,
    checked_at: datetime | None = None,
) -> dict[str, object]:
    package = _json(package_path.resolve())
    bundle_file = bundle_path.resolve()
    if (
        not bundle_file.is_file()
        or bundle_file.stat().st_size > P8_EXECUTION_BUNDLE_MAX_BYTES
    ):
        raise ValueError("P8 execution bundle is absent or oversized")
    bundle = _json(bundle_file)
    return validate_p8_execution_bundle_document(
        package,
        bundle,
        expected_kind=expected_kind,
        checked_at=checked_at,
    )


def validate_p8_execution_bundle_document(
    package: Mapping[str, object],
    bundle: Mapping[str, object],
    *,
    expected_kind: AdvancedOptionsProcessKind | None = None,
    checked_at: datetime | None = None,
) -> dict[str, object]:
    package = dict(package)
    bundle = dict(bundle)
    digest = bundle.get("bundle_sha256")
    unsigned = dict(bundle)
    unsigned.pop("bundle_sha256", None)
    services = bundle.get("services")
    if (
        bundle.get("schema") != P8_EXECUTION_BUNDLE_SCHEMA
        or digest != canonical_sha256(unsigned)
        or bundle.get("package_sha256") != package.get("package_sha256")
        or bundle.get("package_nonce") != package.get("package_nonce")
        or bundle.get("window") != package.get("window")
        or bundle.get("cohort_id") != package.get("cohort", {}).get(
            "cohort_id"
        )
        or bundle.get("plan_id") != package.get("orchestration", {}).get(
            "plan_id"
        )
        or bundle.get("orchestration") != package.get("orchestration")
        or bundle.get("v3_authorities")
        != package.get("v3_authorities")
        or bundle.get("validation_mode")
        != package.get("validation_mode")
        or bundle.get("effect_authorizations") != P8_EFFECT_DENIALS
        or not isinstance(services, list)
        or [row.get("kind") for row in services] != [
            kind.value for kind in AdvancedOptionsProcessKind
        ]
        or [row.get("service_name") for row in services] != [
            P8_SERVICE_NAMES[kind] for kind in AdvancedOptionsProcessKind
        ]
        or [row.get("owner_id") for row in services] != [
            str(_owner_ids(package)[kind])
            for kind in AdvancedOptionsProcessKind
        ]
    ):
        raise ValueError("P8 execution bundle identity changed")
    session_payload = bundle.get("session_binding")
    if session_payload is not None:
        session_binding = p8_preopen_session_binding_from_payload(
            session_payload
        )
        acquisition = acquisition_binding_from_payload(
            bundle.get("acquisition_binding")
        )
        authorities = bundle.get("v3_authorities")
        session_authority = (
            authorities.get("session_acquisition")
            if isinstance(authorities, Mapping)
            else None
        )
        if (
            session_binding.package_sha256
            != package.get("package_sha256")
            or session_binding.acquisition_binding_id
            != acquisition.binding_id
            or session_binding.acquisition_binding_sha256
            != acquisition.binding_sha256
            or session_binding.session_authorization_id
            != acquisition.authority.authorization_id
            or session_binding.session_authorization_sha256
            != acquisition.authority.authorization_sha256
            or not isinstance(session_authority, Mapping)
            or session_authority.get("authorization_id")
            != str(session_binding.session_authorization_id)
            or session_authority.get("authorization_sha256")
            != session_binding.session_authorization_sha256
            or bundle.get("window_id") is not None
            or bundle.get("v3_authority_references") is not None
            or any(
                bundle.get(name) is not None
                for name in (
                    "enrollment",
                    "history_binding",
                    "equity_input_binding",
                    "underlying_price_binding",
                    "production_binding",
                )
            )
        ):
            raise ValueError("P8 waiting bundle identity changed")
    if (
        isinstance(package.get("v3_deployment"), Mapping)
        and package.get("v3_authorities") is not None
    ):
        from .advanced_options_v3_deployment import (
            validate_advanced_options_v3_authority_references,
        )

        if session_payload is None:
            validate_advanced_options_v3_authority_references(
                package,
                bundle,
            )
    validate_p8_service_membership(package)
    if expected_kind is not None and (
        not isinstance(expected_kind, AdvancedOptionsProcessKind)
        or expected_kind.value not in {
            str(row.get("kind")) for row in services
        }
    ):
        raise ValueError("P8 execution bundle worker kind changed")
    if checked_at is not None:
        checked = _utc(checked_at, "checked_at")
        start = datetime.fromisoformat(str(bundle["window"]["start_utc"]))
        stop = datetime.fromisoformat(str(bundle["window"]["stop_utc"]))
        if not start <= checked < stop:
            raise ValueError("P8 Docker service is outside its exact window")
    return bundle


class SessionRuntimeStatePort(Protocol):
    def load_package(self) -> Mapping[str, object]: ...

    def load_bundle_document(self) -> Mapping[str, object]: ...

    def load_execution_bundle(
        self,
        *,
        expected_kind: AdvancedOptionsProcessKind | None = None,
        checked_at: datetime | None = None,
    ) -> Mapping[str, object]: ...

    def load_acquisition_binding(
        self,
    ) -> AdvancedOptionsAcquisitionBinding: ...

    def load_preopen_session_binding(
        self,
        bundle: Mapping[str, object],
    ) -> P8PreopenSessionBinding | None: ...

    def load_downstream_bindings(
        self,
        session_binding: P8PreopenSessionBinding,
    ) -> tuple[P8DownstreamBinding, ...]: ...

    def load_latest_downstream_binding(
        self,
        session_binding: P8PreopenSessionBinding,
    ) -> P8DownstreamBinding | None: ...

    def load_c6_selection_plan(self) -> C6SelectionPlan | None: ...

    def load_c6_selection_health(
        self,
    ) -> Mapping[str, object] | None: ...

    def attach_downstream_binding(
        self,
        session_binding: P8PreopenSessionBinding,
        binding: P8DownstreamBinding,
    ) -> P8DownstreamBinding: ...


class LegacyArtifactSessionStateAdapter:
    def __init__(
        self,
        *,
        package_path: Path | None = None,
        bundle_path: Path | None = None,
        acquisition_binding_path: Path | None = None,
        downstream_binding_directory: Path | None = None,
        json_loader: Callable[[Path], dict[str, object]] = _json,
        acquisition_loader: Callable[[Path], object] | None = None,
        bundle_validator: Callable[..., dict[str, object]] = (
            validate_p8_execution_bundle
        ),
        acquisition_parser: Callable[
            [object], AdvancedOptionsAcquisitionBinding
        ] = acquisition_binding_from_payload,
        session_binding_parser: Callable[
            [object], P8PreopenSessionBinding
        ] = p8_preopen_session_binding_from_payload,
    ) -> None:
        if not all(
            callable(value)
            for value in (
                json_loader,
                acquisition_loader or json_loader,
                bundle_validator,
                acquisition_parser,
                session_binding_parser,
            )
        ):
            raise TypeError("legacy session-state parser is invalid")
        self._package_path = package_path
        self._bundle_path = bundle_path
        self._acquisition_binding_path = acquisition_binding_path
        self._downstream_binding_directory = downstream_binding_directory
        self._json_loader = json_loader
        self._acquisition_loader = acquisition_loader or json_loader
        self._bundle_validator = bundle_validator
        self._acquisition_parser = acquisition_parser
        self._session_binding_parser = session_binding_parser

    @staticmethod
    def _path(path: Path | None, name: str) -> Path:
        if not isinstance(path, Path):
            raise ValueError(f"legacy {name} path is unavailable")
        return path

    def load_package(self) -> Mapping[str, object]:
        return self._json_loader(
            self._path(self._package_path, "package")
        )

    def load_bundle_document(self) -> Mapping[str, object]:
        return self._json_loader(
            self._path(self._bundle_path, "execution bundle")
        )

    def load_execution_bundle(
        self,
        *,
        expected_kind: AdvancedOptionsProcessKind | None = None,
        checked_at: datetime | None = None,
    ) -> Mapping[str, object]:
        arguments: dict[str, object] = {}
        if expected_kind is not None:
            arguments["expected_kind"] = expected_kind
        if checked_at is not None:
            arguments["checked_at"] = checked_at
        return self._bundle_validator(
            self._path(self._package_path, "package"),
            self._path(self._bundle_path, "execution bundle"),
            **arguments,
        )

    def load_acquisition_binding(
        self,
    ) -> AdvancedOptionsAcquisitionBinding:
        return self._acquisition_parser(
            self._acquisition_loader(
                self._path(
                    self._acquisition_binding_path,
                    "acquisition binding",
                )
            )
        )

    def load_c6_selection_plan(self) -> C6SelectionPlan | None:
        return None

    def load_c6_selection_health(
        self,
    ) -> Mapping[str, object] | None:
        return None

    def load_preopen_session_binding(
        self,
        bundle: Mapping[str, object],
    ) -> P8PreopenSessionBinding | None:
        session_payload = bundle.get("session_binding")
        if session_payload is None:
            return None
        return self._session_binding_parser(session_payload)

    def load_downstream_bindings(
        self,
        session_binding: P8PreopenSessionBinding,
    ) -> tuple[P8DownstreamBinding, ...]:
        return self._downstream_store(session_binding).bindings()

    def _downstream_store(
        self,
        session_binding: P8PreopenSessionBinding,
    ) -> P8DownstreamBindingStore:
        return P8DownstreamBindingStore(
            self._path(
                self._downstream_binding_directory,
                "downstream binding directory",
            ),
            session_binding=session_binding,
        )

    def load_latest_downstream_binding(
        self,
        session_binding: P8PreopenSessionBinding,
    ) -> P8DownstreamBinding | None:
        bindings = self.load_downstream_bindings(session_binding)
        return None if not bindings else bindings[-1]

    def attach_downstream_binding(
        self,
        session_binding: P8PreopenSessionBinding,
        binding: P8DownstreamBinding,
    ) -> P8DownstreamBinding:
        return self._downstream_store(session_binding).attach(binding)


class _BundleCapture:
    def __init__(
        self,
        capture: P8BindingCapturePort,
        store: P8ExecutionBundleStore,
    ) -> None:
        self._capture = capture
        self._store = store

    def capture(
        self,
        package: Mapping[str, object],
        *,
        checked_at: datetime,
    ):
        observation, binding = self._capture.capture(
            package,
            checked_at=checked_at,
        )
        self._store.write(
            observation,
            binding,
            written_at=checked_at,
            production_binding=getattr(
                self._capture,
                "production_binding_payload",
                None,
            ),
        )
        return observation, binding


class P8HeartbeatResidentLauncher:
    """Keep all four database leases healthy until mandatory stop."""

    def __init__(
        self,
        launcher: AdvancedOptionsResidentLauncher,
        bundle: P8ExecutionBundleStore,
        *,
        interval_seconds: int = P8_HEARTBEAT_INTERVAL_SECONDS,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if (
            type(interval_seconds) is not int
            or interval_seconds <= 0
            or not callable(clock)
        ):
            raise ValueError("P8 heartbeat launcher configuration is invalid")
        self._launcher = launcher
        self._bundle = bundle
        self._interval_seconds = interval_seconds
        self._clock = clock
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure: BaseException | None = None

    def start(self, *, now: datetime):
        self._bundle.load(checked_at=now)
        health = self._launcher.start(now=now)

        def heartbeat() -> None:
            while not self._stop.wait(self._interval_seconds):
                try:
                    self._launcher.tick(now=self._clock())
                except BaseException as error:
                    self._failure = error
                    self._stop.set()

        self._thread = threading.Thread(
            target=heartbeat,
            name="advanced-options-p8-docker-heartbeat",
            daemon=True,
        )
        self._thread.start()
        return health

    def stop(self) -> None:
        self._stop.set()
        failure = self._failure
        if self._thread is not None:
            self._thread.join(timeout=self._interval_seconds + 1)
            if self._thread.is_alive():
                failure = RuntimeError(
                    "P8 resident heartbeat did not stop"
                )
        try:
            self._launcher.stop()
        except BaseException:
            raise
        if failure is not None:
            raise RuntimeError(
                "P8 resident heartbeat failed"
            ) from failure


class _ClosingLifecyclePort:
    def __init__(
        self,
        port: AdvancedOptionsP8LifecyclePort,
        close: Callable[[], None],
    ) -> None:
        self._port = port
        self._close = close
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._port, name)

    def cleanup(self, binding: P8CanaryExecutionBinding):
        try:
            return self._port.cleanup(binding)
        finally:
            self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._close()


def _owner_ids(package: Mapping[str, object]):
    nonce = UUID(str(package["package_nonce"]))
    return {
        kind: uuid5(
            NAMESPACE_URL,
            (
                "stockscreener:advanced-options-p8-owner:"
                f"{nonce}:{kind.value}"
            ),
        )
        for kind in AdvancedOptionsProcessKind
    }


def _default_components_factory(
    *,
    package: Mapping[str, object],
    repository_root: Path,
    stored_reads_authorized: bool,
) -> P8RuntimeComponents:
    class Deferred:
        def __init__(self):
            self._components = None

        def get(self):
            if self._components is None:
                from .advanced_options_p8_production import (
                    build_advanced_options_p8_production_components,
                )

                self._components = (
                    build_advanced_options_p8_production_components(
                        package=package,
                        repository_root=repository_root,
                        stored_reads_authorized=stored_reads_authorized,
                    )
                )
            return self._components

        def close(self):
            if self._components is not None:
                self._components.close()

    deferred = Deferred()

    class Capture:
        def capture(self, *args, **kwargs):
            return deferred.get().capture.capture(*args, **kwargs)

        @property
        def production_binding_payload(self):
            return getattr(
                deferred.get().capture,
                "production_binding_payload",
                None,
            )

    class EnrollmentConnection:
        def cursor(self, *args, **kwargs):
            return deferred.get().enrollment_connection.cursor(
                *args,
                **kwargs,
            )

        def commit(self):
            return deferred.get().enrollment_connection.commit()

        def rollback(self):
            return deferred.get().enrollment_connection.rollback()

    class Database:
        def transaction(self, *args, **kwargs):
            return deferred.get().database.transaction(*args, **kwargs)

    class Orchestrator:
        def start(self, *args, **kwargs):
            return deferred.get().orchestrator.start(*args, **kwargs)

        def run_window(self, *args, **kwargs):
            return deferred.get().orchestrator.run_window(*args, **kwargs)

    class PublisherDatabase(RealtimeProcessDatabase):
        @property
        def process_plan(self):
            return deferred.get().publisher_database.process_plan

        def transaction(self, *args, **kwargs):
            return deferred.get().publisher_database.transaction(
                *args,
                **kwargs,
            )

    class StoredReads:
        def _port(self):
            port = deferred.get().stored_reads
            if port is None:
                raise RuntimeError(
                    "P8 stored-read persistence is unavailable"
                )
            return port

        def snapshot(self, **kwargs):
            return self._port().snapshot(**kwargs)

        def advance(self, **kwargs):
            return self._port().advance(**kwargs)

        def restore(self, **kwargs):
            return self._port().restore(**kwargs)

    publication_config = PortalPublicationConfig(
        cohort_id=UUID(str(package["cohort"]["cohort_id"])),
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        owner_id=uuid5(
            NAMESPACE_URL,
            f"p8-publisher:{package['package_nonce']}",
        ),
        owner_epoch=1,
        dispatch_authorized=False,
    )
    return P8RuntimeComponents(
        capture=Capture(),
        enrollment_connection=EnrollmentConnection(),
        database=Database(),
        orchestrator=Orchestrator(),
        publisher_database=object.__new__(PublisherDatabase),
        publication_config=publication_config,
        publication_batch=lambda *args, **kwargs: (
            deferred.get().publication_batch(*args, **kwargs)
        ),
        publication_observation=lambda *args, **kwargs: (
            deferred.get().publication_observation(*args, **kwargs)
        ),
        stored_reads=StoredReads(),
        cleanup=lambda binding: deferred.get().cleanup(binding),
        close=deferred.close,
        clock=lambda: datetime.now(UTC),
    )


def build_advanced_options_p8_runtime(
    *,
    package: Mapping[str, object],
    package_path: Path,
    repository_root: Path,
    components_factory: P8RuntimeComponentsFactory | None = None,
    docker_executable: Path | None = None,
    environment: Mapping[str, str] | None = None,
    command_runner=None,
    stored_reads_authorized: bool = False,
):
    """Compose the exact repository runtime; no external module is loadable."""
    if type(stored_reads_authorized) is not bool:
        raise TypeError("P8 stored-read authority must be boolean")
    validate_p8_service_membership(package)
    root = repository_root.resolve()
    bundle_path = _inside(
        root,
        (
            root / str(package["cleanup"]["lock_path"])
        ).with_suffix(".execution-bundle.json"),
        "execution bundle path",
    )
    bundle = P8ExecutionBundleStore(
        bundle_path,
        package=package,
        package_path=package_path,
        repository_root=root,
    )
    if bundle.path.exists():
        raise FileExistsError(
            "P8 execution bundle already exists; execution is ambiguous"
        )
    source_environment = dict(os.environ if environment is None else environment)
    executable_value = docker_executable or Path(
        source_environment.get(
            "REALTIME_V2_DOCKER_EXECUTABLE",
            "docker.exe",
        )
    )
    if not executable_value.is_absolute():
        raise ValueError("REALTIME_V2_DOCKER_EXECUTABLE must be absolute")
    compose_environment = isolated_p8_compose_environment(
        source_environment,
        backend_image=source_environment.get(
            "REALTIME_V2_BACKEND_IMAGE",
            "",
        ),
        secret_directory=Path(source_environment.get(
            "REALTIME_V2_DOCKER_SECRET_DIR",
            root / "backend" / ".realtime-canary" / "v2-docker-secrets",
        )),
        package_path=package_path,
        bundle_path=bundle.path,
    )
    processes = AdvancedOptionsDockerServicePort(
        package=package,
        repository_root=root,
        docker_executable=executable_value,
        environment=compose_environment,
        runner=command_runner,
    )
    plan = build_owp3_advanced_options_resident_plan(
        plan_id=UUID(str(package["orchestration"]["plan_id"])),
        cohort_id=UUID(str(package["cohort"]["cohort_id"])),
        session_date=date.fromisoformat(
            str(package["window"]["session_date"])
        ),
    )
    if [
        {
            "kind": row.kind.value,
            "owner_name": row.owner_name,
            "lease_resource_key": row.lease_resource_key,
        }
        for row in plan.processes
    ] != [
        {
            key: row[key]
            for key in ("kind", "owner_name", "lease_resource_key")
        }
        for row in package["owners"]["processes"]
    ]:
        raise ValueError("P8 resident plan and package owners diverged")
    components = (components_factory or _default_components_factory)(
        package=package,
        repository_root=root,
        stored_reads_authorized=stored_reads_authorized,
    )
    try:
        launcher = P8HeartbeatResidentLauncher(
            AdvancedOptionsResidentLauncher(
                plan,
                components.database,
                processes,
                owner_ids=_owner_ids(package),
                cleanup_host_artifacts=True,
            ),
            bundle,
        )
        port = AdvancedOptionsP8LifecyclePort(
            capture=_BundleCapture(components.capture, bundle),
            enrollment_connection=components.enrollment_connection,
            resident_launcher=launcher,
            orchestrator=components.orchestrator,
            publisher_database=components.publisher_database,
            publication_config=components.publication_config,
            publication_batch=components.publication_batch,
            publication_observation=components.publication_observation,
            stored_reads=components.stored_reads,
            delivery_sink=components.delivery_sink,
            cleanup=components.cleanup,
            clock=components.clock,
        )
    except Exception:
        components.close()
        raise
    return _ClosingLifecyclePort(port, components.close)


def validate_p8_docker_service_environment(
    kind: AdvancedOptionsProcessKind,
    environment: Mapping[str, str],
) -> None:
    if not isinstance(kind, AdvancedOptionsProcessKind):
        raise TypeError("P8 Docker service kind is invalid")
    if not str(environment.get("REALTIME_V2_APP_DSN", "")).strip():
        raise RuntimeError("P8 Docker service app DSN is unavailable")
    provider = str(environment.get("POLYGON_API_KEY", "")).strip()
    split_acquisition = (
        environment.get("REALTIME_V3_ACQUISITION_SPLIT") == "ENABLED"
    )
    if kind is AdvancedOptionsProcessKind.INGRESS and not split_acquisition:
        if not provider:
            raise RuntimeError("P8 ingress provider credential is unavailable")
    elif provider:
        raise RuntimeError(
            "P8 downstream service received a provider credential"
        )
    forbidden = [
        name
        for name, value in environment.items()
        if value
        and any(
            token in name.upper()
            for token in ("BROKER", "DELIVERY", "TRADING", "EXECUTION")
        )
    ]
    if forbidden:
        raise RuntimeError(
            "P8 Docker service received forbidden authority: "
            + ",".join(sorted(forbidden))
        )


def _p8_status_path(
    kind: AdvancedOptionsProcessKind,
    environment: Mapping[str, str],
) -> Path:
    directory = Path(environment.get(
        "REALTIME_V2_P8_STATUS_DIR",
        "/var/lib/realtime-v2-reports/p8",
    ))
    return directory / f"{kind.value.casefold().replace('_', '-')}.json"


def _write_service_status(
    path: Path,
    *,
    kind: AdvancedOptionsProcessKind,
    bundle: Mapping[str, object],
    observed_at: datetime,
    state: str,
    owner_epoch: int | None = None,
    lease_until: datetime | None = None,
    role_evidence: Mapping[str, object] | None = None,
    downstream_binding: Mapping[str, object] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": P8_STATUS_SCHEMA,
        "kind": kind.value,
        "service_name": P8_SERVICE_NAMES[kind],
        "package_sha256": bundle["package_sha256"],
        "package_nonce": bundle["package_nonce"],
        "bundle_sha256": bundle["bundle_sha256"],
        "window_id": bundle["window_id"],
        "observed_at_utc": _utc(
            observed_at,
            "observed_at",
        ).isoformat(),
        "state": state,
        "owner_epoch": owner_epoch,
        "lease_until_utc": (
            None
            if lease_until is None
            else _utc(lease_until, "lease_until").isoformat()
        ),
        "role_evidence": (
            None if role_evidence is None else dict(role_evidence)
        ),
        "downstream_binding": (
            None
            if downstream_binding is None
            else dict(downstream_binding)
        ),
        "effects": dict(P8_EFFECT_DENIALS),
    }
    temporary = path.with_suffix(".partial")
    with temporary.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _validate_downstream_binding_evidence(value: object) -> None:
    if not isinstance(value, Mapping):
        raise RuntimeError("P8 downstream binding evidence is unavailable")
    if value.get("schema") == P8_DOWNSTREAM_BINDING_SCHEMA:
        try:
            p8_downstream_binding_from_payload(value)
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError(
                "P8 downstream binding evidence changed"
            ) from error
        return
    unsigned = dict(value)
    digest = unsigned.pop("binding_sha256", None)
    try:
        receipt_id = UUID(str(value["equity_receipt_id"]))
        equity_cutoff = _utc(
            datetime.fromisoformat(str(value["finalized_cutoff_utc"])),
            "finalized_cutoff_utc",
        )
        options_cutoff = _utc(
            datetime.fromisoformat(str(value["options_cutoff_utc"])),
            "options_cutoff_utc",
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError(
            "P8 downstream binding evidence is invalid"
        ) from error
    e_o_seconds = (options_cutoff - equity_cutoff).total_seconds()
    if (
        value.get("schema")
        != "advanced-options-p8-downstream-binding-v1"
        or not receipt_id.int
        or digest != canonical_sha256(unsigned)
        or value.get("e_o_seconds") != e_o_seconds
        or not 0 <= e_o_seconds <= 300
    ):
        raise RuntimeError("P8 downstream binding evidence changed")
    for name in (
        "equity_input_sha256",
        "equity_binding_sha256",
        "window_cycle_authorization_sha256",
    ):
        candidate = value.get(name)
        if (
            not isinstance(candidate, str)
            or len(candidate) != 64
            or any(
                character not in "0123456789abcdef"
                for character in candidate
            )
        ):
            raise RuntimeError(
                "P8 downstream binding digest changed"
            )


def _claim_service_lease_cursor(
    cursor,
    service: Mapping[str, object],
    *,
    checked_at: datetime,
) -> DurableRuntimeLease | None:
    """Hold this service's lease, adopting one this owner still holds after a restart."""
    resource_key = str(service["lease_resource_key"])
    owner_id = UUID(str(service["owner_id"]))
    cursor.execute(
        "SELECT owner_id::text,owner_epoch,lease_until "
        "FROM rt.worker_leases WHERE resource_key=%s",
        (resource_key,),
    )
    row = cursor.fetchone()
    if row is not None and str(row[0]) == str(owner_id) and row[2] > checked_at:
        return DurableRuntimeLease(resource_key, owner_id, int(row[1]), row[2])
    return claim_durable_runtime_lease_cursor(
        cursor,
        resource_key=resource_key,
        owner_id=owner_id,
        checked_at=checked_at,
        lease_seconds=P8_SERVICE_LEASE_SECONDS,
    )


def run_p8_docker_service(
    kind: AdvancedOptionsProcessKind,
    package_path: Path,
    bundle_path: Path,
    *,
    environment: Mapping[str, str] | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    wait: Callable[[float], None] = time.sleep,
    stop_event: threading.Event | None = None,
    role_runner: Callable[..., Mapping[str, object]] | None = None,
    session_state: SessionRuntimeStatePort | None = None,
) -> int:
    """Run one real bounded role while continuously proving lease ownership."""
    service_environment = dict(
        os.environ if environment is None else environment
    )
    validate_p8_docker_service_environment(kind, service_environment)
    state = session_state or LegacyArtifactSessionStateAdapter(
        package_path=package_path,
        bundle_path=bundle_path,
        acquisition_binding_path=Path(service_environment.get(
            "REALTIME_V3_OPTIONS_ACQUISITION_BINDING_PATH",
            "/run/advanced-options-acquisition/binding.json",
        )),
        downstream_binding_directory=Path(service_environment.get(
            "REALTIME_V3_OPTIONS_DOWNSTREAM_BINDING_DIR",
            "/var/lib/realtime-v2-reports/downstream-bindings",
        )),
        acquisition_loader=_resident_acquisition_json,
    )
    bundle = state.load_execution_bundle(
        expected_kind=kind,
        checked_at=clock(),
    )
    package = state.load_package()
    stop = stop_event or threading.Event()
    if stop_event is None:
        signal.signal(signal.SIGINT, lambda *_args: stop.set())
        signal.signal(signal.SIGTERM, lambda *_args: stop.set())
    status_path = _p8_status_path(kind, service_environment)
    window_stop = datetime.fromisoformat(str(bundle["window"]["stop_utc"]))
    session_binding = state.load_preopen_session_binding(bundle)
    latest_binding = (
        None
        if session_binding is None
        else state.load_latest_downstream_binding(session_binding)
    )
    service = next(
        row for row in bundle["services"] if row["kind"] == kind.value
    )
    if role_runner is None:
        from .advanced_options_p8_production import run_p8_production_role

        role_runner = run_p8_production_role
        observes_binding = True
    else:
        observes_binding = False
    role_result: dict[str, object] = {}
    role_failure: list[BaseException] = []
    role_state = {
        "state": (
            P8_BOUND_TO_E
            if latest_binding is not None
            else (
                P8_READY_WAITING_FOR_E
                if bundle.get("orchestration", {}).get("catalog_version")
                == STAGE_CATALOG_VERSION_V5
                else "ROLE_RUNNING"
            )
        ),
        "downstream_binding": (
            None
            if latest_binding is None
            else p8_downstream_binding_payload(latest_binding)
        ),
    }
    role_state_lock = threading.Lock()

    def observe_binding(
        state: str,
        evidence: Mapping[str, object] | None = None,
    ) -> None:
        if state not in {P8_READY_WAITING_FOR_E, P8_BOUND_TO_E}:
            raise ValueError("P8 downstream binding state is invalid")
        with role_state_lock:
            role_state["state"] = state
            role_state["downstream_binding"] = (
                None if evidence is None else dict(evidence)
            )

    def run_role() -> None:
        try:
            arguments = {
                "kind": kind,
                "package": package,
                "bundle": bundle,
                "environment": service_environment,
                "stop_event": stop,
            }
            if observes_binding:
                arguments["state_observer"] = observe_binding
                arguments["session_state"] = state
            result = role_runner(
                **arguments,
            )
            if not isinstance(result, Mapping):
                raise TypeError("P8 Docker role evidence must be an object")
            role_result.update(result)
        except BaseException as error:
            role_failure.append(error)
            stop.set()

    role_thread = threading.Thread(
        target=run_role,
        name=f"advanced-options-p8-{kind.value.casefold()}-role",
        daemon=True,
    )
    connection = psycopg2.connect(
        service_environment["REALTIME_V2_APP_DSN"],
        application_name=f"advanced_options_p8_{kind.value.casefold()}",
        # The login role has no schema access of its own; this connection reads as the app role.
        options=f"-c timezone=UTC -c role={EXPECTED_V2_DATABASE_NAME}_app",
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                lease = _claim_service_lease_cursor(
                    cursor,
                    service,
                    checked_at=_utc(clock(), "clock"),
                )
        if lease is None:
            raise RuntimeError(f"P8 {kind.value} lease ownership changed")
        role_thread.start()
        while not stop.is_set():
            observed = _utc(clock(), "clock")
            if observed >= window_stop:
                stop.set()
                break
            with connection:
                with connection.cursor() as cursor:
                    lease = renew_durable_runtime_lease_cursor(
                        cursor,
                        lease,
                        checked_at=observed,
                        lease_seconds=P8_SERVICE_LEASE_SECONDS,
                    )
            if lease is None:
                raise RuntimeError(
                    f"P8 {kind.value} lease ownership changed"
                )
            with role_state_lock:
                if session_binding is not None:
                    attached = state.load_latest_downstream_binding(
                        session_binding
                    )
                    if (
                        attached is not None
                        and (
                            role_state["downstream_binding"] is None
                            or role_state["downstream_binding"].get(
                                "binding_sha256"
                            )
                            != attached.binding_sha256
                        )
                    ):
                        role_state["state"] = P8_BOUND_TO_E
                        role_state["downstream_binding"] = (
                            p8_downstream_binding_payload(attached)
                        )
                current_state = str(role_state["state"])
                downstream_binding = role_state["downstream_binding"]
            _write_service_status(
                status_path,
                kind=kind,
                bundle=bundle,
                observed_at=observed,
                state=(
                    "ROLE_COMPLETE"
                    if not role_thread.is_alive()
                    else current_state
                ),
                owner_epoch=lease.owner_epoch,
                lease_until=lease.lease_until,
                role_evidence=role_result or None,
                downstream_binding=downstream_binding,
            )
            if role_failure:
                raise RuntimeError(
                    f"P8 {kind.value} role failed"
                ) from role_failure[0]
            stop.wait(min(
                5.0,
                max(0.0, (window_stop - observed).total_seconds()),
            ))
        role_thread.join(timeout=10)
        if role_thread.is_alive():
            raise RuntimeError(
                f"P8 {kind.value} role did not stop within its bound"
            )
        if role_failure:
            raise RuntimeError(
                f"P8 {kind.value} role failed"
            ) from role_failure[0]
        if (
            role_result.get("status") not in {"COMPLETE", "PAUSED"}
            or role_result.get("kind") != kind.value
            or role_result.get("effects") != P8_EFFECT_DENIALS
        ):
            raise RuntimeError(
                f"P8 {kind.value} role evidence is incomplete"
            )
        _write_service_status(
            status_path,
            kind=kind,
            bundle=bundle,
            observed_at=_utc(clock(), "clock"),
            state="STOPPED",
            role_evidence=role_result,
        )
    finally:
        connection.close()
    return 0


def check_p8_docker_service_health(
    kind: AdvancedOptionsProcessKind,
    package_path: Path,
    bundle_path: Path,
    *,
    environment: Mapping[str, str] | None = None,
    checked_at: datetime | None = None,
    session_state: SessionRuntimeStatePort | None = None,
) -> Mapping[str, object]:
    service_environment = dict(
        os.environ if environment is None else environment
    )
    checked = _utc(
        checked_at or datetime.now(UTC),
        "checked_at",
    )
    state = session_state or LegacyArtifactSessionStateAdapter(
        package_path=package_path,
        bundle_path=bundle_path,
    )
    bundle = state.load_execution_bundle(
        expected_kind=kind,
        checked_at=checked,
    )
    status = _json(_p8_status_path(kind, service_environment))
    observed = datetime.fromisoformat(str(status.get("observed_at_utc")))
    state = status.get("state")
    downstream_binding = status.get("downstream_binding")
    if state == P8_READY_WAITING_FOR_E and downstream_binding is not None:
        raise RuntimeError("P8 waiting service has an E binding")
    if state == P8_BOUND_TO_E:
        _validate_downstream_binding_evidence(downstream_binding)
    if (
        status.get("schema") != P8_STATUS_SCHEMA
        or status.get("kind") != kind.value
        or status.get("service_name") != P8_SERVICE_NAMES[kind]
        or status.get("package_sha256") != bundle["package_sha256"]
        or status.get("package_nonce") != bundle["package_nonce"]
        or status.get("bundle_sha256") != bundle["bundle_sha256"]
        or status.get("window_id") != bundle["window_id"]
        or state
        not in {
            P8_READY_WAITING_FOR_E,
            P8_BOUND_TO_E,
            "ROLE_RUNNING",
            "ROLE_COMPLETE",
        }
        or type(status.get("owner_epoch")) is not int
        or status["owner_epoch"] < 1
        or datetime.fromisoformat(
            str(status.get("lease_until_utc"))
        ) <= checked
        or status.get("effects") != P8_EFFECT_DENIALS
        or observed > checked
        or (
            checked - observed
        ).total_seconds() > P8_STATUS_MAXIMUM_AGE_SECONDS
    ):
        raise RuntimeError("P8 Docker service health identity changed")
    return {
        "status": (
            P8_READY_WAITING_FOR_E
            if state == P8_READY_WAITING_FOR_E
            else "READY"
        ),
        "kind": kind.value,
        "service_name": P8_SERVICE_NAMES[kind],
        "observed_at_utc": observed.isoformat(),
        "state": state,
        "role_evidence": status.get("role_evidence"),
        "downstream_binding": status.get("downstream_binding"),
        "effects": dict(P8_EFFECT_DENIALS),
    }
