from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import date, datetime
from decimal import Decimal
from queue import Queue

from options.domain import DataCapability, RawOptionBatch, SpotPrice


class BaseDataEngine(ABC):
    @property
    @abstractmethod
    def capabilities(self) -> frozenset[DataCapability]:
        raise NotImplementedError

    @abstractmethod
    def get_spot_price(self, underlyer: str, as_of: datetime) -> SpotPrice:
        raise NotImplementedError

    @abstractmethod
    def get_option_chain(
        self,
        underlyer: str,
        as_of: datetime,
        expiration_through: date,
        strike_min: Decimal,
        strike_max: Decimal,
    ) -> RawOptionBatch:
        raise NotImplementedError

    @abstractmethod
    def stream_market_data(
        self,
        underlyers: Collection[str],
        output: Queue[RawOptionBatch],
        stop_event: threading.Event,
    ) -> None:
        raise NotImplementedError


def validate_developer_capabilities(capabilities: Collection[DataCapability]) -> None:
    observed = frozenset(capabilities)
    required = frozenset(
        {
            DataCapability.CHAIN_SNAPSHOT,
            DataCapability.OPTION_TRADES,
            DataCapability.UNDERLYING_PRICE,
        }
    )
    missing = required - observed
    if missing:
        names = ", ".join(sorted(capability.value for capability in missing))
        raise RuntimeError(f"Developer capability probe is missing: {names}")
    forbidden = observed & {
        DataCapability.OPTION_QUOTES,
        DataCapability.REAL_TIME,
    }
    if forbidden:
        names = ", ".join(sorted(capability.value for capability in forbidden))
        raise RuntimeError(f"Developer capability probe unexpectedly includes: {names}")