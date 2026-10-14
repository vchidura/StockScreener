"""The canonical-hash fast path must be byte-identical to the original generic implementation."""
from collections import OrderedDict
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum
import json
from types import MappingProxyType
from typing import Mapping
from uuid import UUID, uuid5, NAMESPACE_URL

import pytest

from realtime.contracts import _canonical_value, _utc, canonical_json, canonical_sha256
import hashlib


class Color(Enum):
    RED = "red"


class Rank(IntEnum):
    FIRST = 1


class Label(str, Enum):
    A = "a"


@dataclass(frozen=True)
class Row:
    name: str
    when: datetime
    amount: Decimal
    tags: tuple


def _oracle(value):
    """The implementation before the exact-type fast path (generic isinstance chain)."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _utc(value, "canonical datetime").isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical Decimal must be finite")
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _oracle(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _oracle(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_oracle(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def _oracle_json(value):
    return json.dumps(_oracle(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


STAMP = datetime(2026, 10, 12, 14, 30, tzinfo=timezone(timedelta(hours=-4)))
IDENTITY = uuid5(NAMESPACE_URL, "canonical-fast-path")
CORPUS = (
    None, True, False, 0, -7, 3.5, float("inf"), "text", "ünï\u2028code", "",
    Color.RED, Rank.FIRST, Label.A, STAMP, date(2026, 10, 12), IDENTITY, Decimal("12.3400"),
    {}, [], (), {"b": 1, "a": [1, (2, 3)], "c": {"z": None}},
    {1: "int key", True: "bool key", 2.5: "float key", None: "none key"},
    OrderedDict(b=1, a=2), MappingProxyType({"k": (1, 2)}), set(),
    frozenset({"only"}), [IDENTITY, STAMP, Decimal("0.1"), Color.RED, {"n": (Label.A,)}],
    Row("r", STAMP, Decimal("1.50"), (IDENTITY, date(2026, 1, 2))),
    (Row("a", STAMP, Decimal("1"), ()), {"nested": [Row("b", STAMP, Decimal("2"), (1,))]}),
    {"rows": [{"id": IDENTITY, "start": STAMP, "ohlc": (Decimal("1.0"), 2, 3.5), "ok": True}] * 3},
)


@pytest.mark.parametrize("value", CORPUS, ids=lambda value: type(value).__name__)
def test_fast_path_is_byte_identical_to_the_generic_implementation(value):
    assert _canonical_value(value) == _oracle(value)
    assert canonical_json(value) == _oracle_json(value)
    assert canonical_sha256(value) == hashlib.sha256(_oracle_json(value).encode("ascii")).hexdigest()


def test_single_element_sets_hash_like_the_generic_implementation():
    # Sets of one element are the only deterministic set ordering.
    assert canonical_json({"s": {5}, "f": frozenset({"x"})}) == _oracle_json({"s": {5}, "f": frozenset({"x"})})


@pytest.mark.parametrize("value, error", [
    (datetime(2026, 1, 1), ValueError),
    (Decimal("NaN"), ValueError),
    ({"nested": [Decimal("Infinity")]}, ValueError),
    ({"bad": object()}, TypeError),
    ([b"bytes"], TypeError),
])
def test_unsupported_and_invalid_values_still_fail_the_same_way(value, error):
    with pytest.raises(error):
        _oracle(value)
    with pytest.raises(error):
        canonical_json(value)
