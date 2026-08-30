from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import threading
import time
from typing import Protocol

from http_client import get_session


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


class RequestsPolygonHttpTransport:
    def __init__(
        self,
        api_key: str,
        *,
        timeout_seconds: float = 30.0,
        session_factory: Callable = get_session,
    ) -> None:
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._session_factory = session_factory

    def get(self, url: str, params: Mapping[str, str]) -> PolygonHttpResponse:
        response = self._session_factory().get(
            url,
            params=dict(params),
            headers={"Authorization": f"Bearer {self._api_key}"},
            timeout=self._timeout_seconds,
            allow_redirects=False,
        )
        return PolygonHttpResponse(
            status_code=response.status_code,
            body=response.content,
            headers=tuple((str(key), str(value)) for key, value in response.headers.items()),
        )


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