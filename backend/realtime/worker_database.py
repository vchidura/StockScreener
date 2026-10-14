from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from .database_access import (
    RealtimeProcessDatabasePlan,
    RoleScopedConnectionProvider,
    wire_process_connection_provider,
)


class RealtimeProcessDatabase:
    def __init__(
        self,
        process_plan: RealtimeProcessDatabasePlan,
        connection_provider: RoleScopedConnectionProvider,
    ) -> None:
        if not isinstance(process_plan, RealtimeProcessDatabasePlan):
            raise TypeError("realtime process database plan is invalid")
        if not isinstance(connection_provider, RoleScopedConnectionProvider):
            raise TypeError("realtime process connection provider is invalid")
        self._process_plan = process_plan
        self._connection_provider = connection_provider

    @classmethod
    def wire(
        cls,
        process_plan: RealtimeProcessDatabasePlan,
        *,
        principal_pool: Any,
        role_name: str,
        statement_timeout_ms: int = 5_000,
    ) -> RealtimeProcessDatabase:
        provider = wire_process_connection_provider(
            process_plan,
            principal_pool=principal_pool,
            role_name=role_name,
            statement_timeout_ms=statement_timeout_ms,
        )
        return cls(process_plan, provider)

    @property
    def process_plan(self) -> RealtimeProcessDatabasePlan:
        return self._process_plan

    @contextmanager
    def transaction(
        self,
        *,
        read_only: bool | None = None,
        dict_cursor: bool = False,
        isolation_level: str | None = None,
    ):
        with self._connection_provider.transaction(
            self._process_plan.role,
            read_only=read_only,
            dict_cursor=dict_cursor,
            isolation_level=isolation_level,
        ) as unit_of_work:
            yield unit_of_work.cursor