"""A RealtimeProcessDatabase over a V2 stage: the access adapter for the ported repositories.

Every repository, worker and producer ported from v1 takes a `RealtimeProcessDatabase` and uses only
`process_plan.role` and `transaction(read_only, dict_cursor, isolation_level)`. This adapter satisfies
that interface on top of `StageContext.transaction`, so those functions run unchanged on the V2
schema and inherit its guarantees: a cancelled stage cannot open a write transaction, and a window
stage's writes commit only while its stage run is RUNNING (the stage write fence). A stage that needs
two roles (for example compute and publication) builds one adapter per role over the same context. The
options roles (OPTIONS_INGRESS, OPTIONS_COMPUTE) lower-case to the `options_ingress` / `options_compute`
capability roles of the `runtime_options` principal profile.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass

from .database_access import RealtimeDatabaseRole
from .stage_orchestrator import StageContext
from .worker_database import RealtimeProcessDatabase


@dataclass(frozen=True, slots=True)
class StageProcessPlan:
    """The only attribute the ported code reads from a process plan is its role."""

    role: RealtimeDatabaseRole


class V2StageDatabase(RealtimeProcessDatabase):
    def __init__(self, context: StageContext, role: RealtimeDatabaseRole) -> None:
        if not isinstance(role, RealtimeDatabaseRole):
            raise TypeError("a stage database requires a RealtimeDatabaseRole")
        # The base class wires a v1 connection provider; this adapter replaces it with the stage context.
        self._context = context
        self._process_plan = StageProcessPlan(role)
        self._connection_provider = None

    @contextmanager
    def transaction(self, *, read_only: bool | None = None, dict_cursor: bool = False,
                    isolation_level: str | None = None):
        with self._context.transaction(
            read_only=bool(read_only), dict_cursor=dict_cursor, isolation_level=isolation_level,
            role=self._process_plan.role.value.lower(),
        ) as unit_of_work:
            yield unit_of_work.cursor


def stage_databases(context: StageContext, *roles: RealtimeDatabaseRole) -> tuple[V2StageDatabase, ...]:
    return tuple(V2StageDatabase(context, role) for role in roles)
