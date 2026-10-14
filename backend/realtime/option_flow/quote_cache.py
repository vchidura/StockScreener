from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple

from .messages import QuoteMessage


class QuoteView(NamedTuple):
    """A quote copied by value. Nothing here is a reference into the cache."""

    bid: float
    ask: float
    bid_size: int
    ask_size: int
    time_ms: int
    seq: int
    bid_exchange: int | None
    ask_exchange: int | None


class QuoteUpdate(str, Enum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    TOO_OLD = "TOO_OLD"


class QuoteLookup(NamedTuple):
    quote: QuoteView | None
    reason: str | None
    age_ms: int | None
    same_ms: bool


_NOT_CACHED = QuoteLookup(None, "NO_QUOTE_CACHED", None, False)
_ONLY_LATER = QuoteLookup(None, "ONLY_QUOTES_AFTER_TRADE", None, False)


@dataclass(frozen=True, slots=True)
class QuoteCacheConfig:
    max_quotes_per_contract: int = 8
    max_contracts: int = 4096

    def __post_init__(self) -> None:
        if self.max_quotes_per_contract < 1 or self.max_contracts < 1:
            raise ValueError("quote cache bounds must be positive")


class QuoteCache:
    """Bounded per-contract quote history for at-or-before-trade-time lookups.

    Trade and quote streams have separate sequences, so ordering between them is by the
    millisecond timestamp only. A few quotes per contract are kept so a trade that arrives
    after a later quote still finds the quote that was current at its own time. Contracts
    are evicted least recently updated first and counted, never silently.
    """

    __slots__ = ("_config", "_by_contract", "evicted_contracts", "out_of_order", "duplicates",
                 "too_old", "invalidations")

    def __init__(self, config: QuoteCacheConfig | None = None) -> None:
        self._config = config or QuoteCacheConfig()
        self._by_contract: OrderedDict[str, list[QuoteView]] = OrderedDict()
        self.evicted_contracts = 0
        self.out_of_order = 0
        self.duplicates = 0
        self.too_old = 0
        self.invalidations = 0

    def __len__(self) -> int:
        return len(self._by_contract)

    def update(self, message: QuoteMessage) -> QuoteUpdate:
        view = QuoteView(message.bid, message.ask, message.bid_size, message.ask_size,
                         message.time_ms, message.seq, message.bid_exchange, message.ask_exchange)
        history = self._by_contract.get(message.symbol)
        if history is None:
            if len(self._by_contract) >= self._config.max_contracts:
                self._by_contract.popitem(last=False)
                self.evicted_contracts += 1
            self._by_contract[message.symbol] = [view]
            return QuoteUpdate.ACCEPTED
        self._by_contract.move_to_end(message.symbol)
        key = (view.time_ms, view.seq)
        last = history[-1]
        if key > (last.time_ms, last.seq):
            history.append(view)
            status = QuoteUpdate.ACCEPTED
        else:
            position = len(history)
            while position > 0 and (history[position - 1].time_ms, history[position - 1].seq) > key:
                position -= 1
            if position > 0 and (history[position - 1].time_ms, history[position - 1].seq) == key:
                self.duplicates += 1
                return QuoteUpdate.DUPLICATE
            if position == 0 and len(history) >= self._config.max_quotes_per_contract:
                self.too_old += 1
                return QuoteUpdate.TOO_OLD
            history.insert(position, view)
            self.out_of_order += 1
            status = QuoteUpdate.OUT_OF_ORDER
        if len(history) > self._config.max_quotes_per_contract:
            del history[0]
        return status

    def lookup(self, contract: str, trade_time_ms: int) -> QuoteLookup:
        history = self._by_contract.get(contract)
        if not history:
            return _NOT_CACHED
        for quote in reversed(history):
            if quote.time_ms <= trade_time_ms:
                age = trade_time_ms - quote.time_ms
                return QuoteLookup(quote, None, age, age == 0)
        return _ONLY_LATER

    def invalidate(self, contract: str | None = None) -> None:
        """Forget cached quotes after a quote-channel gap; stale data is worse than none."""
        if contract is None:
            if self._by_contract:
                self.invalidations += len(self._by_contract)
                self._by_contract.clear()
        elif self._by_contract.pop(contract, None) is not None:
            self.invalidations += 1

    def has_quotes(self, contract: str) -> bool:
        return contract in self._by_contract
