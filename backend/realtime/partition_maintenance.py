"""Partition lifecycle for the maintenance loop: create ahead, then raise an alarm if any policy
relation has fewer than 7 pre-created days (a missed creation is never silent; there is no default
partition). Runs as the maintenance or partition_admin capability (EXECUTE is granted to both).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PartitionAlarm:
    relation_name: str
    horizon_date: date | None
    days_ahead: int


def partition_alarms(cursor, today: date) -> list[PartitionAlarm]:
    cursor.execute("SELECT relation_name, horizon_date, days_ahead FROM rt.partition_health(%s) "
                   "WHERE status <> 'OK' ORDER BY relation_name", (today,))
    return [PartitionAlarm(name, horizon, days) for name, horizon, days in cursor.fetchall()]


def ensure_partitions_and_check(cursor, today: date) -> tuple[int, list[PartitionAlarm]]:
    """Create missing partitions for every policy, then return (created, alarms). Alarms are logged at
    ERROR; the caller decides how to page. Never raises for a shortage."""
    cursor.execute("SELECT rt.ensure_partitions_all(%s)", (today,))
    created = int(cursor.fetchone()[0])
    alarms = partition_alarms(cursor, today)
    for alarm in alarms:
        logger.error("partition horizon alarm: %s has %s days ahead", alarm.relation_name, alarm.days_ahead)
    return created, alarms
