from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import threading
import time
from typing import Protocol

from http_client import get_session
from requests import ConnectionError, Session, Timeout
from requests.adapters import HTTPAdapter
from urllib3.exceptions import HTTPError as Urllib3HTTPError, ReadTimeoutError


@dataclass(frozen=True, slots=True)
class PolygonHttpResponse:
    status_code: int
    body: bytes
    headers: tuple[tuple[str, str], ...] = ()

    def header(self, name: str) -> str | None:
        expected = name.lower()
        return next((value for key, value in self.headers if key.lower() == expected), None)


class PolygonHttpTransport(Protocol):
    def get(self, url: str, params: Mapping[str, str]) -> PolygonHttpResponse:
        ...


class PolygonResponseTooLarge(ValueError):
    def __init__(self, observed_bytes: int):
        super().__init__("provider response exceeds its bounded read limit")
        self.observed_bytes = observed_bytes


class RequestsPolygonHttpTransport:
    def __init__(
        self,
        api_key: str,
        *,
        timeout_seconds: float = 30.0,
        session_factory: Callable = get_session,
        maximum_response_bytes: int | None = None,
    ) -> None:
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._session_factory = session_factory
        if maximum_response_bytes is not None and (
            type(maximum_response_bytes) is not int or maximum_response_bytes < 1
        ):
            raise ValueError("provider response limit must be positive")
        self._maximum_response_bytes = maximum_response_bytes

    def get(self, url: str, params: Mapping[str, str]) -> PolygonHttpResponse:
        response = self._session_factory().get(
            url,
            params=dict(params),
            headers={"Authorization": f"Bearer {self._api_key}"},
            timeout=self._timeout_seconds,
            allow_redirects=False,
            **({"stream": True} if self._maximum_response_bytes is not None else {}),
        )
        try:
            body = (
                response.content if self._maximum_response_bytes is None
                else response.raw.read(self._maximum_response_bytes + 1, decode_content=True)
            )
            if self._maximum_response_bytes is not None and len(body) > self._maximum_response_bytes:
                raise PolygonResponseTooLarge(len(body))
            return PolygonHttpResponse(
                status_code=response.status_code, body=body,
                headers=tuple((str(key), str(value)) for key, value in response.headers.items()),
            )
        finally:
            if self._maximum_response_bytes is not None:
                response.close()


class SingleAttemptPolygonHttpTransport(RequestsPolygonHttpTransport):
    """One bounded HTTP attempt with observable partial-body consumption."""

    def __init__(
        self, api_key: str, *, deadline: datetime,
        maximum_response_bytes: int, timeout_seconds: float = 30,
        clock=lambda: datetime.now(timezone.utc), session_factory=Session,
    ) -> None:
        super().__init__(
            api_key, timeout_seconds=timeout_seconds,
            session_factory=session_factory,
            maximum_response_bytes=maximum_response_bytes,
        )
        if deadline.tzinfo is None or deadline.utcoffset() is None or timeout_seconds <= 0:
            raise ValueError("single-attempt HTTP deadline or timeout is invalid")
        self._deadline = deadline
        self._clock = clock
        self._read_limit = maximum_response_bytes
        self.http_status: int | None = None
        self.response_body = b""
        self.response_bytes = 0

    def _remaining(self) -> float:
        remaining = (self._deadline - self._clock()).total_seconds()
        if remaining <= 0:
            raise Timeout("snapshot interval deadline expired")
        return min(self._timeout_seconds, remaining)

    def get(self, url: str, params: Mapping[str, str]) -> PolygonHttpResponse:
        self.http_status = None
        self.response_body = b""
        self.response_bytes = 0
        self._remaining()
        with self._session_factory() as session:
            adapter = HTTPAdapter(max_retries=0)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            response = session.get(
                url, params=dict(params),
                headers={"Authorization": f"Bearer {self._api_key}"},
                timeout=self._remaining(), allow_redirects=False, stream=True,
            )
            self.http_status = response.status_code
            body = bytearray()
            try:
                while True:
                    self._remaining()
                    try:
                        chunk = response.raw.read(
                            min(65_536, self._read_limit + 1 - len(body)),
                            decode_content=True,
                        )
                    except (Urllib3HTTPError, OSError) as error:
                        if isinstance(error, (ReadTimeoutError, Timeout)):
                            raise Timeout("snapshot body read timed out") from error
                        raise ConnectionError("snapshot body read failed") from error
                    body.extend(chunk)
                    self.response_bytes = len(body)
                    self._remaining()
                    if len(body) > self._read_limit:
                        raise PolygonResponseTooLarge(len(body))
                    if not chunk:
                        break
                return PolygonHttpResponse(
                    response.status_code, bytes(body),
                    tuple((str(key), str(value)) for key, value in response.headers.items()),
                )
            finally:
                self.response_body = bytes(body)
                response.close()


class PolygonRateLimitGate:
    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.Lock()
        self._blocked_until = 0.0

    def defer(self, seconds: float) -> None:
        with self._lock:
            self._blocked_until = max(self._blocked_until, self._monotonic() + seconds)

    def wait(self) -> None:
        while True:
            with self._lock:
                remaining = self._blocked_until - self._monotonic()
            if remaining <= 0:
                return
            self._sleep(remaining)