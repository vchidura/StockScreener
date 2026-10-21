"""Host-local ownership for provider WebSocket connections."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO
from uuid import UUID


MASSIVE_OPTIONS_SOCKET_RESOURCE = "massive:options:websocket"
UTC = timezone.utc


class ProviderSocketLeaseUnavailable(RuntimeError):
    pass


def default_provider_lock_path() -> Path:
    return (
        Path(os.environ.get("LOCALAPPDATA", Path.home()))
        / "StockScreener"
        / "locks"
        / "massive-options-websocket.lock"
    ).resolve()


def _utc(value: datetime | None) -> datetime:
    result = datetime.now(UTC) if value is None else value
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("provider socket lease timestamp must be aware")
    return result.astimezone(UTC)


def _try_lock(handle: BinaryIO) -> bool:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock(handle: BinaryIO) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass(slots=True)
class HostProviderSocketLease:
    path: Path
    resource_key: str
    owner_id: UUID
    _handle: BinaryIO | None = field(init=False, default=None, repr=False)
    _acquired_at: datetime | None = field(
        init=False,
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.path = self.path.resolve()
        self.resource_key = self.resource_key.strip()
        if not self.resource_key or len(self.resource_key) > 160:
            raise ValueError("provider socket lease resource is invalid")
        if not isinstance(self.owner_id, UUID):
            raise TypeError("provider socket lease owner must be a UUID")

    @property
    def metadata_path(self) -> Path:
        return self.path.with_suffix(self.path.suffix + ".json")

    @property
    def acquired(self) -> bool:
        return self._handle is not None

    def _write_metadata(self, status: str, observed_at: datetime) -> None:
        value = {
            "owner_id": str(self.owner_id),
            "process_id": os.getpid(),
            "resource_key": self.resource_key,
            "status": status,
            "observed_at": observed_at.isoformat(),
        }
        temporary = self.metadata_path.with_suffix(
            self.metadata_path.suffix + ".tmp"
        )
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(temporary, self.metadata_path)

    def acquire(self, *, now: datetime | None = None) -> None:
        if self._handle is not None:
            raise RuntimeError("provider socket lease is already acquired")
        observed = _utc(now)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        if not _try_lock(handle):
            handle.close()
            raise ProviderSocketLeaseUnavailable(
                f"provider socket lease unavailable: {self.resource_key}"
            )
        self._handle = handle
        self._acquired_at = observed
        self._write_metadata("ACQUIRED", observed)

    def release(self, *, now: datetime | None = None) -> None:
        handle = self._handle
        if handle is None:
            return
        observed = _utc(now)
        try:
            self._write_metadata("RELEASED", observed)
        finally:
            _unlock(handle)
            handle.close()
            self._handle = None
            self._acquired_at = None

    def evidence(self) -> dict[str, object]:
        return {
            "scope": "HOST_LOCAL",
            "resource_key": self.resource_key,
            "owner_id": str(self.owner_id),
            "process_id": os.getpid(),
            "acquired_at": (
                None
                if self._acquired_at is None
                else self._acquired_at.isoformat()
            ),
            "lock_path": str(self.path),
        }

    def __enter__(self) -> HostProviderSocketLease:
        self.acquire()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.release()
