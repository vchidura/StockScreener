from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from options.alert_plans import AlertManagementPolicy, FrozenOptionAlertPlan, _canonical, freeze_indicative_alert_plan
from options.alert_qualification import retained_time

from .base import PostgresRepository


PUBLICATION_VERSION = "option_alert_publication_v1"


class AlertPublicationEvent(str, Enum):
    PUBLISHED = "PUBLISHED"
    OBSERVED = "OBSERVED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


def validate_publication_transition(
    payload: Mapping[str, object], previous: str | None,
    event: AlertPublicationEvent, recorded_at: datetime,
) -> None:
    recorded_at = retained_time(recorded_at)
    if payload.get("state") != "UNPUBLISHED_INDICATIVE_PLAN" or payload.get("execution_permission") is not False or payload.get("paper_position_created") is not False:
        raise ValueError("only indicative non-executable plans may be published")
    if recorded_at < retained_time(payload["decision_at"]):
        raise ValueError("publication event cannot precede plan decision")
    expired = recorded_at >= retained_time(payload["entry_deadline"])
    if previous in {"INVALIDATED", "EXPIRED"}:
        raise ValueError("terminal alert cannot be reopened")
    if event is AlertPublicationEvent.PUBLISHED:
        if previous is not None or expired:
            raise ValueError("publication requires a new unexpired plan")
    elif previous not in {"PUBLISHED", "OBSERVED"}:
        raise ValueError("lifecycle events require a published plan")
    elif event is AlertPublicationEvent.EXPIRED:
        if not expired:
            raise ValueError("cannot expire a plan before its entry deadline")
    elif expired:
        raise ValueError("entry deadline passed; only expiration may follow")


def publication_exposure_key(payload: Mapping[str, object]) -> str:
    exposure = {
        "version": PUBLICATION_VERSION,
        "underlying": payload["underlying"], "strategy": payload["strategy"],
        "strategy_version": payload["strategy_version"], "policy": payload["strategy_policy_sha256"],
        "structure": payload["structure"], "management": payload["management_policy"],
        "legs": [{key: leg[key] for key in ("index", "contract_id", "side", "ratio", "multiplier", "expiration_date", "strike", "contract_type")} for leg in payload["legs"]],
    }
    return hashlib.sha256(_canonical(exposure).encode("ascii")).hexdigest()


def validated_plan_payload(plan: FrozenOptionAlertPlan) -> dict[str, object]:
    payload = json.loads(plan.payload_json)
    management = None
    if payload.get("management_source") == "EXPLICIT_ALERT_POLICY":
        values = payload["management_policy"]
        management = AlertManagementPolicy(
            policy_version=values["policy_version"], strategy_name=values["strategy_name"],
            stop_loss_fraction=Decimal(values["stop_loss_fraction"]),
            take_profit_fraction=Decimal(values["take_profit_fraction"]),
            maximum_hold_seconds=values["maximum_hold_seconds"], minimum_exit_dte=values["exit_dte"],
        )
    rebuilt = freeze_indicative_alert_plan(
        payload["source_evidence"], decision_at=retained_time(payload["decision_at"]),
        entry_deadline=retained_time(payload["entry_deadline"]), exit_deadline=retained_time(payload["exit_deadline"]),
        entry_limit=Decimal(payload["entry_limit"]), management_policy=management,
    )
    if rebuilt.payload_json != plan.payload_json:
        raise ValueError("frozen plan integrity verification failed")
    return payload


class OptionAlertPublicationRepository(PostgresRepository):
    def publish(self, plan: FrozenOptionAlertPlan, *, request_key: str) -> dict[str, object]:
        payload = validated_plan_payload(plan)
        exposure_key = publication_exposure_key(payload)
        request = self._request(plan.plan_id, AlertPublicationEvent.PUBLISHED, request_key, (), None, None)
        with self._cursor() as cursor:
            self._lock(cursor, exposure_key)
            existing = self._existing(cursor, plan.plan_id, request_key, request)
            if existing:
                return existing
            now = self._clock(cursor)
            validate_publication_transition(payload, None, AlertPublicationEvent.PUBLISHED, now)
            cursor.execute(
                """
                SELECT plan.plan_id
                FROM option_alert_plans AS plan
                JOIN LATERAL (
                    SELECT event_type FROM option_alert_publication_events
                    WHERE plan_id = plan.plan_id ORDER BY sequence DESC LIMIT 1
                ) AS latest ON TRUE
                WHERE plan.exposure_key = %s AND latest.event_type IN ('PUBLISHED', 'OBSERVED')
                LIMIT 1
                """, (exposure_key,),
            )
            if cursor.fetchone():
                raise ValueError("an active publication already exists for this exact exposure; append an observation instead")
            cursor.execute(
                """
                INSERT INTO option_alert_plans (
                    plan_id, candidate_id, exposure_key, plan_sha256, payload_text,
                    decision_at, entry_deadline, exit_deadline
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (plan_id) DO NOTHING
                """,
                (plan.plan_id, payload["candidate_id"], exposure_key, plan.sha256, plan.payload_json,
                 payload["decision_at"], payload["entry_deadline"], payload["exit_deadline"]),
            )
            cursor.execute("SELECT plan_sha256 FROM option_alert_plans WHERE plan_id = %s FOR UPDATE", (plan.plan_id,))
            stored = cursor.fetchone()
            if not stored or stored["plan_sha256"] != plan.sha256:
                raise ValueError("stored plan identity conflict")
            return self._insert(cursor, plan.plan_id, AlertPublicationEvent.PUBLISHED, request_key, request)

    def append_event(
        self, plan_id: UUID, event: AlertPublicationEvent, *, request_key: str,
        source_ids: tuple[str, ...] = (), source_available_at: datetime | None = None,
        reason: str | None = None,
    ) -> dict[str, object]:
        if not isinstance(event, AlertPublicationEvent) or event is AlertPublicationEvent.PUBLISHED:
            raise ValueError("use publish for the initial event")
        request = self._request(plan_id, event, request_key, source_ids, source_available_at, reason)
        with self._cursor() as cursor:
            cursor.execute("SELECT exposure_key FROM option_alert_plans WHERE plan_id = %s", (plan_id,))
            row = cursor.fetchone()
            if not row:
                raise ValueError("alert plan not found")
            self._lock(cursor, row["exposure_key"])
            existing = self._existing(cursor, plan_id, request_key, request)
            if existing:
                return existing
            cursor.execute("SELECT payload_text FROM option_alert_plans WHERE plan_id = %s FOR UPDATE", (plan_id,))
            payload = json.loads(cursor.fetchone()["payload_text"])
            cursor.execute("SELECT event_type FROM option_alert_publication_events WHERE plan_id = %s ORDER BY sequence DESC LIMIT 1", (plan_id,))
            previous = cursor.fetchone()
            now = self._clock(cursor)
            validate_publication_transition(payload, previous["event_type"] if previous else None, event, now)
            if source_available_at is not None and not retained_time(payload["decision_at"]) <= retained_time(source_available_at) <= now:
                raise ValueError("event source must be available after the original decision and by recorded time")
            return self._insert(cursor, plan_id, event, request_key, request)

    def history(self, *, limit: int = 100, offset: int = 0) -> tuple[dict, ...]:
        if type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or offset < 0:
            raise ValueError("invalid alert history pagination")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                """
                WITH page AS MATERIALIZED (
                    SELECT event.event_id, event.sequence, event.plan_id, event.event_type,
                           event.recorded_at, event.request_text, plan.plan_sha256, plan.payload_text, plan.decision_at
                    FROM option_alert_publication_events AS event
                    JOIN option_alert_plans AS plan USING (plan_id)
                    ORDER BY event.sequence DESC LIMIT %s OFFSET %s
                ), hits AS (
                    SELECT event.plan_id, MIN(event.recorded_at) FILTER (WHERE event.event_type='PUBLISHED') AS published_at,
                        COUNT(DISTINCT CASE WHEN event.event_type='PUBLISHED' THEN selected.decision_at
                            WHEN event.event_type='OBSERVED'
                                AND jsonb_array_length(event.request_text::jsonb->'source_ids')>0
                            THEN (event.request_text::jsonb->>'source_available_at')::timestamptz END) AS hit_count
                    FROM (SELECT DISTINCT plan_id, decision_at FROM page) AS selected
                    JOIN option_alert_publication_events AS event USING(plan_id)
                    GROUP BY event.plan_id
                )
                SELECT page.*, hits.hit_count, hits.published_at FROM page JOIN hits USING(plan_id)
                ORDER BY page.sequence DESC
                """, (limit, offset),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def exposure_state(self, exposure_keys: tuple[str, ...]) -> dict[str, object]:
        if not isinstance(exposure_keys, tuple) or not 1 <= len(exposure_keys) <= 20 or any(
            not isinstance(key, str) or len(key) != 64 or any(character not in "0123456789abcdef" for character in key)
            for key in exposure_keys
        ):
            raise ValueError("exposure lookup requires 1-20 SHA256 keys")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                """
                SELECT clock_timestamp() AS checked_at,
                       to_regclass('public.option_alert_plans') IS NOT NULL
                       AND to_regclass('public.option_alert_publication_events') IS NOT NULL AS ready
                """
            )
            schema = cursor.fetchone()
            if not schema["ready"]:
                return {"available": False, "checked_at": schema["checked_at"],
                        "reason": "OPTION_ALERT_PUBLICATION_MIGRATION_REQUIRED", "rows": []}
            cursor.execute(
                """
                SELECT plan.exposure_key, count(*) AS published_plan_count,
                       count(*) FILTER (WHERE latest.event_type IN ('PUBLISHED', 'OBSERVED')) AS active_plan_count,
                       min(plan.plan_id::text) FILTER (WHERE latest.event_type IN ('PUBLISHED', 'OBSERVED')) AS active_plan_id,
                       min(plan.entry_deadline) FILTER (WHERE latest.event_type IN ('PUBLISHED', 'OBSERVED')) AS active_entry_deadline,
                       max(latest.sequence) AS latest_sequence, max(latest.recorded_at) AS last_recorded_at
                FROM option_alert_plans AS plan
                JOIN LATERAL (
                    SELECT event_type, sequence, recorded_at FROM option_alert_publication_events
                    WHERE plan_id = plan.plan_id ORDER BY sequence DESC LIMIT 1
                ) AS latest ON TRUE
                WHERE plan.exposure_key = ANY(%s)
                GROUP BY plan.exposure_key ORDER BY plan.exposure_key
                """, (sorted(set(exposure_keys)),),
            )
            return {"available": True, "checked_at": schema["checked_at"], "reason": None,
                    "rows": [dict(row) for row in cursor.fetchall()]}

    @staticmethod
    def _request(plan_id, event, key, source_ids, available_at, reason) -> str:
        if not isinstance(key, str) or not key.strip() or len(key) > 200:
            raise ValueError("publication request key must contain 1-200 characters")
        if not isinstance(source_ids, tuple) or any(not isinstance(value, str) or not value.strip() for value in source_ids):
            raise ValueError("event sources must be non-empty immutable IDs")
        if event in {AlertPublicationEvent.OBSERVED, AlertPublicationEvent.INVALIDATED} and (not source_ids or available_at is None):
            raise ValueError("observation and invalidation require causal source evidence")
        if event is AlertPublicationEvent.INVALIDATED and (not isinstance(reason, str) or not reason.strip()):
            raise ValueError("invalidation requires a reason")
        return _canonical({"version": PUBLICATION_VERSION, "plan_id": str(plan_id), "event": event.value,
                           "source_ids": sorted(set(source_ids)), "source_available_at": retained_time(available_at) if available_at else None,
                           "reason": reason, "execution_permission": False})

    @staticmethod
    def _lock(cursor, exposure_key):
        cursor.execute("SET LOCAL statement_timeout = '15s'")
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"option-alert:{exposure_key}",))

    @staticmethod
    def _clock(cursor):
        cursor.execute("SELECT clock_timestamp() AS recorded_at")
        return retained_time(cursor.fetchone()["recorded_at"])

    @staticmethod
    def _existing(cursor, plan_id, key, request):
        cursor.execute("SELECT event_id, event_type, recorded_at, request_text FROM option_alert_publication_events WHERE plan_id = %s AND request_key = %s", (plan_id, key))
        row = cursor.fetchone()
        if row and row["request_text"] != request:
            raise ValueError("idempotency key reused with different event evidence")
        return {**dict(row), "status": "ALREADY_RECORDED"} if row else None

    @staticmethod
    def _insert(cursor, plan_id, event, key, request):
        event_id = uuid5(NAMESPACE_URL, f"option-alert-event:{plan_id}:{key}")
        cursor.execute(
            """
            INSERT INTO option_alert_publication_events (event_id, plan_id, event_type, request_key, request_text)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING event_id, event_type, recorded_at
            """, (event_id, plan_id, event.value, key, request),
        )
        return {**dict(cursor.fetchone()), "status": "RECORDED"}