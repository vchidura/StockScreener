"""Partition lifecycle for the maintenance loop: create ahead, then raise an alarm if any policy
relation has fewer than 7 pre-created days (a missed creation is never silent; there is no default
partition). Runs as the app role (EXECUTE on the SECURITY DEFINER partition functions is granted to it).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PartitionAlarm:
    relation_name: str
    horizon_date: date | None
    days_ahead: int


@dataclass(frozen=True, slots=True)
class PartitionCreationCandidate:
    relation_name: str
    partition_name: str
    scheme: str
    lower: date
    upper: date

    def as_dict(self) -> dict[str, str]:
        return {
            "relation": self.relation_name,
            "partition": self.partition_name,
            "scheme": self.scheme,
            "lower": self.lower.isoformat(),
            "upper": self.upper.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class PartitionCreationPlan:
    as_of_date: date
    candidates: tuple[PartitionCreationCandidate, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "as_of_date": self.as_of_date.isoformat(),
            "would_create": len(self.candidates),
            "partitions": [candidate.as_dict() for candidate in self.candidates],
        }


def _partition_ranges(
    relation_name: str,
    scheme: str,
    pre_create_days: int,
    lookback_days: int,
    today: date,
) -> tuple[PartitionCreationCandidate, ...]:
    start = today - timedelta(days=lookback_days)
    if scheme == "WEEKLY":
        start -= timedelta(days=start.weekday())
    elif scheme == "MONTHLY":
        start = date(start.year, start.month, 1)
    elif scheme != "DAILY":
        raise ValueError(f"unsupported partition scheme {scheme}")
    range_end = today + timedelta(days=pre_create_days)
    relation = relation_name.partition(".")[2]
    candidates: list[PartitionCreationCandidate] = []
    while start <= range_end:
        if scheme == "DAILY":
            upper = start + timedelta(days=1)
        elif scheme == "WEEKLY":
            upper = start + timedelta(days=7)
        else:
            upper = date(
                start.year + start.month // 12,
                start.month % 12 + 1,
                1,
            )
        candidates.append(PartitionCreationCandidate(
            relation_name,
            f"rt.{relation}_p{start:%Y%m%d}",
            scheme,
            start,
            upper,
        ))
        start = upper
    return tuple(candidates)


def partition_creation_plan(
    cursor,
    today: date,
    relations: tuple[str, ...] | None = None,
) -> PartitionCreationPlan:
    cursor.execute(
        "SELECT relation_name,scheme,pre_create_days,lookback_days "
        "FROM rt.partition_policies ORDER BY relation_name"
    )
    selected = set(relations) if relations is not None else None
    expected = tuple(
        candidate
        for relation, scheme, pre_create_days, lookback_days in cursor.fetchall()
        if selected is None or relation in selected
        for candidate in _partition_ranges(
            relation,
            scheme,
            int(pre_create_days),
            int(lookback_days),
            today,
        )
    )
    names = [candidate.partition_name for candidate in expected]
    cursor.execute(
        "SELECT name,to_regclass(name) IS NOT NULL "
        "FROM unnest(%s::text[]) AS item(name)",
        (names,),
    )
    present = {name for name, exists in cursor.fetchall() if exists}
    return PartitionCreationPlan(
        today,
        tuple(candidate for candidate in expected if candidate.partition_name not in present),
    )


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


def ensure_selected_partitions_and_check(
    cursor,
    today: date,
    relations: tuple[str, ...],
) -> tuple[int, list[PartitionAlarm]]:
    if not relations:
        raise ValueError("partition maintenance relations cannot be empty")
    created = 0
    for relation in sorted(set(relations)):
        cursor.execute("SELECT rt.ensure_partitions(%s, %s)", (relation, today))
        created += int(cursor.fetchone()[0])
    alarms = partition_alarms(cursor, today)
    for alarm in alarms:
        logger.error("partition horizon alarm: %s has %s days ahead", alarm.relation_name, alarm.days_ahead)
    return created, alarms
