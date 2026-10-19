from __future__ import annotations

import math
import re
from typing import Any, Mapping, NamedTuple

from ..advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
)

_OCC = re.compile(r"^O:([A-Z][A-Z0-9.]{0,9}?)(\d{6})([CP])(\d{8})$")
TRADE_PROVIDER_FIELDS = MASSIVE_OPTIONS_PROVIDER_CONTRACT.trade_fields
QUOTE_PROVIDER_FIELDS = MASSIVE_OPTIONS_PROVIDER_CONTRACT.quote_fields


class MessageRejected(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class TradeMessage(NamedTuple):
    symbol: str
    exchange: int | None
    price: float
    size: int
    conditions: tuple[int, ...]
    time_ms: int
    seq: int
    offset: int | None


class QuoteMessage(NamedTuple):
    symbol: str
    bid_exchange: int | None
    ask_exchange: int | None
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    time_ms: int
    seq: int
    offset: int | None


def underlyer_of(symbol: str) -> str | None:
    match = _OCC.match(symbol)
    return match.group(1) if match else None


def _integer(raw: Mapping[str, Any], name: str, *, minimum: int) -> int:
    try:
        value = raw[name]
    except KeyError as error:
        raise MessageRejected(f"MISSING_FIELD_{name}") from error
    if type(value) is not int or value < minimum:
        raise MessageRejected(f"INVALID_FIELD_{name}")
    return value


def _optional_integer(raw: Mapping[str, Any], name: str) -> int | None:
    value = raw.get(name)
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise MessageRejected(f"INVALID_FIELD_{name}")
    return value


def _number(raw: Mapping[str, Any], name: str, *, allow_zero: bool) -> float:
    try:
        value = raw[name]
    except KeyError as error:
        raise MessageRejected(f"MISSING_FIELD_{name}") from error
    kind = type(value)
    if kind is not float and kind is not int:
        if kind.__name__ != "Decimal":
            raise MessageRejected(f"INVALID_FIELD_{name}")
    number = float(value)
    if not math.isfinite(number) or number < 0 or (number == 0 and not allow_zero):
        raise MessageRejected(f"INVALID_FIELD_{name}")
    return number


def _symbol(raw: Mapping[str, Any]) -> str:
    try:
        symbol = raw["sym"]
    except KeyError as error:
        raise MessageRejected("MISSING_FIELD_sym") from error
    if type(symbol) is not str or not symbol.isupper() or not symbol.startswith("O:"):
        raise MessageRejected("INVALID_FIELD_sym")
    return symbol


def parse_trade(raw: Mapping[str, Any], offset: int | None = None) -> TradeMessage:
    """Parse the documentation-pinned T event fields."""
    symbol = _symbol(raw)
    price = _number(raw, "p", allow_zero=False)
    size = _integer(raw, "s", minimum=1)
    time_ms = _integer(raw, "t", minimum=0)
    seq = _integer(raw, "q", minimum=0)
    exchange = _optional_integer(raw, "x")
    conditions = raw.get("c")
    if conditions is None:
        parsed: tuple[int, ...] = ()
    else:
        try:
            parsed = tuple(conditions)
        except TypeError as error:
            raise MessageRejected("INVALID_FIELD_c") from error
        for code in parsed:
            if type(code) is not int or code < 0:
                raise MessageRejected("INVALID_FIELD_c")
    return TradeMessage(symbol, exchange, price, size, parsed, time_ms, seq, offset)


def parse_quote(raw: Mapping[str, Any], offset: int | None = None) -> QuoteMessage:
    """Parse the documentation-pinned Q event fields."""
    symbol = _symbol(raw)
    bid = _number(raw, "bp", allow_zero=True)
    ask = _number(raw, "ap", allow_zero=True)
    bid_size = _integer(raw, "bs", minimum=0)
    ask_size = _integer(raw, "as", minimum=0)
    time_ms = _integer(raw, "t", minimum=0)
    seq = _integer(raw, "q", minimum=0)
    return QuoteMessage(symbol, _optional_integer(raw, "bx"), _optional_integer(raw, "ax"),
                        bid, ask, bid_size, ask_size, time_ms, seq, offset)
