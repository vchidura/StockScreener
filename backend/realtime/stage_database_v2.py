"""A RealtimeProcessDatabase over a V2 stage: the access adapter for the ported repositories.

Every repository, worker and producer ported from v1 takes a `RealtimeProcessDatabase` and uses only
`process_plan.role` and `transaction(read_only, dict_cursor, isolation_level)`. This adapter satisfies
that interface on top of `StageContext.transaction`, so those functions run unchanged on the V2
schema and inherit its guarantees: a cancelled stage cannot open a write transaction, and a window
stage's writes commit only while its stage run is RUNNING (the stage write fence).

`process_plan.role` is only the logical wiring label the ported code asserts (a compute half must not
be handed the publication database). V2 has no per-label database role: every transaction runs as
the single `app` role.
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
        ) as unit_of_work:
            yield unit_of_work.cursor


class V2WorkerDatabase(RealtimeProcessDatabase):
    """An external leased-job worker over the V2 app pool.

    Stage fencing belongs to the coordinator. Worker writes are instead fenced by the materialization
    job lease in the same transaction that persists output and completes the job.
    """

    def __init__(self, provider, role: RealtimeDatabaseRole = RealtimeDatabaseRole.COMPUTE) -> None:
        if not isinstance(role, RealtimeDatabaseRole):
            raise TypeError("a worker database requires a RealtimeDatabaseRole")
        self._provider = provider
        self._process_plan = StageProcessPlan(role)
        self._connection_provider = None

    @contextmanager
    def transaction(self, *, read_only: bool | None = None, dict_cursor: bool = False,
                    isolation_level: str | None = None):
        with self._provider.transaction(
            read_only=bool(read_only),
            dict_cursor=dict_cursor,
            isolation_level=isolation_level,
        ) as unit_of_work:
            yield unit_of_work.cursor


def stage_databases(context: StageContext, *roles: RealtimeDatabaseRole) -> tuple[V2StageDatabase, ...]:
    return tuple(V2StageDatabase(context, role) for role in roles)
