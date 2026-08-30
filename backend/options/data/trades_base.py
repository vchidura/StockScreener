from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import datetime
from queue import Queue

from options.domain import (
    OptionContractCatalogEntry,
    OptionTradeCursor,
    OptionTradeEvent,
    OptionTradeFetchResult,
)


class OptionsTradeSource(ABC):
    @abstractmethod
    def get_option_trades(
        self,
        contract: OptionContractCatalogEntry,
        start_time: datetime,
        end_time: datetime,
        cursor: OptionTradeCursor | None,
    ) -> OptionTradeFetchResult:
        raise NotImplementedError

    @abstractmethod
    def stream_option_trades(
        self,
        contracts: Collection[OptionContractCatalogEntry],
        output: Queue[OptionTradeEvent],
        stop_event: threading.Event,
    ) -> None:
        raise NotImplementedError