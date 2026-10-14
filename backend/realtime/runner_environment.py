"""Per-process state shared by the V2 stage runners."""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from .contracts import UniverseManifest
from .database_access_v2 import V2ConnectionProvider
from .screening_delta_loader import ScreeningInputLoader
from .ticker_projection import TickerProjectionState

if TYPE_CHECKING:
    from options.config import FlowStrategyPolicy

    from .acquisition_producer import DeveloperAcquisitionSource
    from .option_flow.conditions import ConditionSemantics
    from .option_flow.consumer import OptionFlowSource
    from .option_flow.labels import PriorOpenInterest
    from .reference_service import ReferenceSeedService
    from .stage_orchestrator import StageContext
    from .stage_runners_options import OptionsFlowSettings, OptionsFlowState


def _new_options_flow_state() -> OptionsFlowState:
    from .stage_runners_options import OptionsFlowState

    return OptionsFlowState()


class CycleHandoff:
    """Bounded in-process hand-off of a computed ticker cycle from EQUITY_EVIDENCE to PORTAL_PUBLISH.

    Both stages run in the same process and principal lane, so the compute result need not be
    recomputed. It is an optimization, never a source of truth: when nothing is found (restart between
    the stages) the publishing stage fails closed and the next window recomputes. At most `capacity`
    cycles are retained, so memory does not grow when publication falls behind."""

    def __init__(self, capacity: int = 2) -> None:
        if capacity < 1:
            raise ValueError("handoff capacity must be positive")
        self._capacity = capacity
        self._items: OrderedDict[tuple[UUID, datetime], object] = OrderedDict()
        self._lock = threading.Lock()

    def put(self, cohort_id: UUID, cutoff: datetime, cycle: object) -> None:
        with self._lock:
            self._items[(cohort_id, cutoff)] = cycle
            while len(self._items) > self._capacity:
                self._items.popitem(last=False)

    def peek(self, cohort_id: UUID, cutoff: datetime) -> object | None:
        with self._lock:
            return self._items.get((cohort_id, cutoff))

    def discard(self, cohort_id: UUID, cutoff: datetime) -> None:
        with self._lock:
            self._items.pop((cohort_id, cutoff), None)

    def __len__(self) -> int:
        return len(self._items)


@dataclass(slots=True)
class RunnerEnvironment:
    """What a runner factory may use: the shared database provider and per-process state.

    The provider-facing sources are injected, never constructed by a runner: REFERENCE and
    EQUITY_INGEST fail closed (blocked, no provider call) while theirs is None. `universe` is the
    reviewed universe both stages work on; `reference_scope` selects the rt.seed_current pointer.

    OPTIONS_FLOW is injected the same way and fails closed without them: `options_flow_source` (the
    journal read protocol, a local mock in tests), `options_flow_semantics` (must be a pinned live capture;
    the synthetic fixture is refused), `options_flow_policy` (FlowStrategyPolicy), optionally
    `options_flow_settings`, `options_flow_oi_loader` (default: ref.oi_baselines) and the bounded-slice
    `options_flow_clock` (monotonic seconds). `options_flow_state` holds the live engine session and the
    divergence quarantine."""

    database: V2ConnectionProvider | None
    build_id: str = "unversioned"
    ticker_state: TickerProjectionState = field(default_factory=TickerProjectionState)
    handoff: CycleHandoff = field(default_factory=CycleHandoff)
    screening_loader: ScreeningInputLoader = field(default_factory=ScreeningInputLoader)
    reference_service: ReferenceSeedService | None = None
    equity_source: DeveloperAcquisitionSource | None = None
    universe: UniverseManifest | None = None
    reference_scope: str = "MARKET_REFERENCES"
    options_flow_source: OptionFlowSource | None = None
    options_flow_semantics: ConditionSemantics | None = None
    options_flow_policy: FlowStrategyPolicy | None = None
    options_flow_settings: OptionsFlowSettings | None = None
    options_flow_oi_loader: Callable[[StageContext, date], Mapping[str, PriorOpenInterest]] | None = None
    options_flow_clock: Callable[[], float] = time.monotonic
    options_flow_state: OptionsFlowState = field(default_factory=_new_options_flow_state)
