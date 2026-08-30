from __future__ import annotations

from dataclasses import dataclass
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