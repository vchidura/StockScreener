"""Thin Polygon Advanced WebSocket adapter for stock minute aggregates."""
from __future__ import annotations

import os
from typing import Any, Callable, Iterable, Sequence


class PolygonAdvancedStreamClient:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        max_reconnects: int | None = 5,
        client_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("POLYGON_API_KEY", "")
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY is required")
        self.max_reconnects = max_reconnects
        self._client_factory = client_factory

    def run_minute_aggregates(
        self,
        tickers: Iterable[str],
        handler: Callable[[Sequence[Any]], None],
    ) -> None:
        client = self._build_client(tickers)
        client.run(handler)

    async def connect_minute_aggregates(
        self,
        tickers: Iterable[str],
        handler: Callable[[Sequence[Any]], Any],
    ) -> None:
        client = self._build_client(tickers)
        await client.connect(handler)

    def _build_client(self, tickers: Iterable[str]) -> Any:
        symbols = tuple(sorted({
            ticker.strip().upper() for ticker in tickers if ticker.strip()
        }))
        if not symbols:
            raise ValueError("at least one ticker is required")
        client_factory = self._client_factory
        if client_factory is None:
            from polygon import WebSocketClient

            client_factory = WebSocketClient
        client = client_factory(
            api_key=self.api_key,
            feed="socket.polygon.io",
            market="stocks",
            max_reconnects=self.max_reconnects,
            subscriptions=[f"AM.{ticker}" for ticker in symbols],
        )
        return client