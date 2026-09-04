from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum

from .config import DataEngine, ExecutionEngine, OptionRuntimeConfiguration


class OptionServiceMode(str, Enum):
    READ_ONLY = "READ_ONLY"


@dataclass(frozen=True, slots=True)
class OptionStartupState:
    mode: OptionServiceMode
    configuration: OptionRuntimeConfiguration

    def metadata(self) -> dict[str, object]:
        return {"mode": self.mode.value, **self.configuration.metadata()}


def build_option_startup_state(
    configuration: OptionRuntimeConfiguration,
) -> OptionStartupState:
    if not configuration.settings.start_read_only:
        raise RuntimeError("Phase 1 startup requires OPTION_START_READ_ONLY=true")
    if configuration.settings.data_engine is not DataEngine.POLYGON_DEVELOPER:
        raise RuntimeError("Polygon Advanced is reserved but not implemented in Phase 1")
    if configuration.settings.execution_engine is not ExecutionEngine.PAPER_PROXY:
        raise RuntimeError("shadow and broker execution engines are not implemented in Phase 1")
    return OptionStartupState(
        mode=OptionServiceMode.READ_ONLY,
        configuration=configuration,
    )


def ensure_option_partitions(as_of: datetime) -> None:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    observed = as_of.astimezone(timezone.utc)
    current_month = date(observed.year, observed.month, 1)
    next_month = (
        date(observed.year + 1, 1, 1)
        if observed.month == 12
        else date(observed.year, observed.month + 1, 1)
    )
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT ensure_option_market_data_partitions(%s)",
            (current_month,),
        )
        cursor.execute(
            "SELECT ensure_option_market_data_partitions(%s)",
            (next_month,),
        )