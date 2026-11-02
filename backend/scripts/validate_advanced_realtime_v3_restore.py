#!/usr/bin/env python3
"""Validate restored Advanced Realtime V3 relational state."""
from __future__ import annotations

import json
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations


REQUIRED_RELATIONS = (
    "rt.database_identity",
    "rt.schema_migrations",
    "rt.database_security_policies",
    "rt.security_phase_events",
    "rt.partition_policies",
    "rt.session_archives",
    "rt.archive_segments",
    "rt.realtime_desired_revisions",
    "rt.realtime_desired_current",
    "rt.realtime_desired_member_intents",
    "rt.realtime_desired_intent_readiness",
    "rt.realtime_desired_member_bindings",
    "rt.realtime_desired_session_snapshots",
    "rt.advanced_options_preparation_sessions",
    "rt.advanced_options_preparation_work",
    "rt.advanced_options_preparation_evidence",
    "rt.advanced_options_preparation_evidence_history",
    "rt.advanced_options_preparation_refresh_requests",
    "rt.advanced_options_continuous_sessions",
    "rt.advanced_options_downstream_bindings",
    "rt.advanced_options_downstream_heads",
    "rt.advanced_journal_registry",
    "rt.consumer_checkpoints",
    "rt.advanced_options_rolling_checkpoints",
    "rt.advanced_options_selection_generations",
    "rt.advanced_options_selection_members",
    "rt.advanced_options_physical_subscriptions",
    "rt.advanced_options_physical_subscription_states",
    "rt.advanced_options_subscription_shards",
    "rt.advanced_options_shard_states",
    "rt.advanced_options_consumer_states",
    "rt.advanced_options_detector_routing_states",
    "rt.portal_publication_work",
    "rt.portal_publication_receipts",
    "rt.portal_releases",
    "rt.portal_release_current",
    "rt.advanced_options_read_pins",
    "rt.advanced_realtime_service_states",
)

RAW_CHECKPOINT = "RAW"
NORMALIZED_CHECKPOINT = "NORMALIZED"
GENERIC_CHECKPOINT_COORDINATES = {
    "advanced_equity_am_checkpoint_v1": NORMALIZED_CHECKPOINT,
    "realtime-materializer-shard-json-v2": NORMALIZED_CHECKPOINT,
    "option_flow_state_v1+json+zlib": RAW_CHECKPOINT,
}
SELECTION_CONSUMER_COORDINATES = {
    "market-state-materializer-v1": NORMALIZED_CHECKPOINT,
    "advanced-options-flow-v1": RAW_CHECKPOINT,
}
EXPECTED_ROLES = (
    "stocks_realtime_v2_admin",
    "stocks_realtime_v2_app",
    "stocks_realtime_v2_api",
)


def checkpoint_exceeds_boundary(
    *,
    coordinate: str,
    checkpoint_offset: int,
    raw_committed_offset: int,
    normalized_committed_offset: int,
) -> bool:
    if coordinate == RAW_CHECKPOINT:
        boundary = raw_committed_offset
    elif coordinate == NORMALIZED_CHECKPOINT:
        boundary = normalized_committed_offset
    else:
        raise ValueError(
            f"unknown checkpoint coordinate: {coordinate}"
        )
    return checkpoint_offset > boundary


INTEGRITY_CHECKS = {
    "desired_pointer_metadata": """
        SELECT COUNT(*)
        FROM rt.realtime_desired_current AS current
        JOIN rt.realtime_desired_revisions AS revision USING(revision_id)
        WHERE current.effective_session_date
                  <> revision.effective_session_date
           OR current.revision_recorded_at <> revision.recorded_at
    """,
    "completed_preparation_without_evidence": """
        SELECT COUNT(*)
        FROM rt.advanced_options_preparation_work AS work
        LEFT JOIN rt.advanced_options_preparation_evidence AS evidence
          USING(revision_id, intent_id, stage)
        WHERE work.status = 'COMPLETE'
          AND evidence.revision_id IS NULL
    """,
    "completed_preparation_without_current_evidence_history": """
        SELECT COUNT(*)
        FROM rt.advanced_options_preparation_work AS work
        LEFT JOIN rt.advanced_options_preparation_evidence_history AS evidence
          ON evidence.revision_id = work.revision_id
         AND evidence.intent_id = work.intent_id
         AND evidence.stage = work.stage
         AND evidence.attempt = work.attempts
        WHERE work.status = 'COMPLETE'
          AND evidence.revision_id IS NULL
    """,
    "invalid_active_preparation_binding": """
        SELECT COUNT(*)
        FROM rt.advanced_options_preparation_sessions
        WHERE (status = 'ACTIVE')
              <> (activated_session_binding_id IS NOT NULL)
    """,
    "invalid_downstream_head": """
        SELECT COUNT(*)
        FROM rt.advanced_options_downstream_heads AS head
        LEFT JOIN rt.advanced_options_downstream_bindings AS binding
          ON binding.session_binding_id = head.session_binding_id
         AND binding.sequence = head.sequence
         AND binding.binding_sha256 = head.binding_sha256
        WHERE head.sequence > 0 AND binding.binding_id IS NULL
    """,
    "selection_physical_membership": """
        SELECT COUNT(*)
        FROM rt.advanced_options_selection_generations AS generation
        LEFT JOIN LATERAL (
            SELECT array_agg(member.contract_ticker
                             ORDER BY member.contract_ticker) AS members
            FROM rt.advanced_options_selection_members AS member
            WHERE member.selection_generation_id
                  = generation.selection_generation_id
        ) AS selected ON TRUE
        LEFT JOIN rt.advanced_options_physical_subscriptions AS physical
          USING(selection_generation_id)
        WHERE physical.subscription_id IS NULL
           OR physical.contract_tickers IS DISTINCT FROM selected.members
    """,
    "selection_shard_membership": """
        SELECT COUNT(*)
        FROM rt.advanced_options_selection_generations AS generation
        LEFT JOIN LATERAL (
            SELECT array_agg(member.contract_ticker
                             ORDER BY member.contract_ticker) AS members
            FROM rt.advanced_options_selection_members AS member
            WHERE member.selection_generation_id
                  = generation.selection_generation_id
        ) AS selected ON TRUE
        LEFT JOIN LATERAL (
            SELECT array_agg(DISTINCT ticker ORDER BY ticker) AS members
            FROM rt.advanced_options_subscription_shards AS shard
            CROSS JOIN LATERAL unnest(shard.contract_tickers) AS ticker
            WHERE shard.selection_generation_id
                  = generation.selection_generation_id
        ) AS sharded ON TRUE
        WHERE selected.members IS DISTINCT FROM sharded.members
    """,
    "unknown_selection_consumer_coordinate": """
        SELECT COUNT(*)
        FROM rt.advanced_options_consumer_states
        WHERE consumer_name NOT IN (
            'market-state-materializer-v1',
            'advanced-options-flow-v1'
        )
    """,
    "consumer_checkpoint_beyond_journal": """
        SELECT COUNT(*)
        FROM rt.advanced_options_consumer_states AS consumer
        JOIN rt.advanced_options_physical_subscriptions AS physical
          USING(selection_generation_id)
        LEFT JOIN rt.advanced_journal_registry AS journal
          ON journal.journal_generation = physical.journal_generation
        WHERE journal.journal_generation IS NULL
           OR (
                consumer.consumer_name = 'market-state-materializer-v1'
                AND consumer.committed_offset
                    > journal.normalized_committed_offset
           )
           OR (
                consumer.consumer_name = 'advanced-options-flow-v1'
                AND consumer.committed_offset
                    > journal.raw_committed_offset
           )
    """,
    "unknown_journal_checkpoint_coordinate": """
        SELECT COUNT(*)
        FROM rt.consumer_checkpoints AS checkpoint
        JOIN rt.advanced_journal_registry AS journal
          USING(journal_generation)
        WHERE checkpoint.state_codec NOT IN (
            'advanced_equity_am_checkpoint_v1',
            'realtime-materializer-shard-json-v2',
            'option_flow_state_v1+json+zlib'
        )
    """,
    "journal_checkpoint_beyond_boundary": """
        SELECT COUNT(*)
        FROM rt.consumer_checkpoints AS checkpoint
        LEFT JOIN rt.advanced_journal_registry AS journal
          USING(journal_generation)
        WHERE journal.journal_generation IS NULL
           OR (
                checkpoint.state_codec IN (
                    'advanced_equity_am_checkpoint_v1',
                    'realtime-materializer-shard-json-v2'
                )
                AND checkpoint.committed_offset
                    > journal.normalized_committed_offset
           )
           OR (
                checkpoint.state_codec = 'option_flow_state_v1+json+zlib'
                AND checkpoint.committed_offset
                    > journal.raw_committed_offset
           )
    """,
    "rolling_checkpoint_beyond_journal": """
        SELECT COUNT(*)
        FROM rt.advanced_options_rolling_checkpoints AS checkpoint
        LEFT JOIN rt.advanced_journal_registry AS journal
          USING(journal_generation)
        WHERE journal.journal_generation IS NULL
           OR checkpoint.committed_offset
              > journal.normalized_committed_offset
    """,
    "detector_checkpoint_beyond_journal": """
        SELECT COUNT(*)
        FROM rt.advanced_options_detector_routing_states AS checkpoint
        JOIN rt.advanced_options_physical_subscriptions AS physical
          USING(selection_generation_id)
        LEFT JOIN rt.advanced_journal_registry AS journal
          ON journal.journal_generation = physical.journal_generation
        WHERE journal.journal_generation IS NULL
           OR checkpoint.committed_event_offset
              > journal.normalized_committed_offset
    """,
    "current_pointer_without_release": """
        SELECT COUNT(*)
        FROM rt.portal_release_current AS current
        LEFT JOIN rt.portal_releases AS release
          ON release.release_id = current.release_id
         AND release.cutoff = current.cutoff
        WHERE release.release_id IS NULL
    """,
    "completed_publication_without_release": """
        SELECT COUNT(*)
        FROM rt.portal_publication_work AS work
        LEFT JOIN rt.portal_releases AS release
          ON release.release_id = work.release_id
         AND release.cutoff = work.release_cutoff
        WHERE work.state = 'COMPLETE' AND release.release_id IS NULL
    """,
    "enabled_read_pin_without_release": """
        SELECT COUNT(*)
        FROM rt.advanced_options_read_pins AS pin
        LEFT JOIN rt.portal_releases AS release
          ON release.release_id = pin.release_id
         AND release.cutoff = pin.release_cutoff
        WHERE pin.enabled AND release.release_id IS NULL
    """,
}


def validate() -> dict[str, object]:
    from database import get_db_cursor

    expected_migrations = {
        migration.version: migration.checksum_sha256
        for migration in load_realtime_migrations(V2_MIGRATIONS_DIR)
    }
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT relation_name
            FROM unnest(%s::text[]) AS relation_name
            WHERE to_regclass(relation_name) IS NULL
            ORDER BY relation_name
            """,
            (list(REQUIRED_RELATIONS),),
        )
        missing_relations = tuple(row["relation_name"] for row in cursor.fetchall())
        invalid_counts = {}
        migration_mismatches = []
        missing_roles = []
        security_phase = None
        if not missing_relations:
            for name, statement in INTEGRITY_CHECKS.items():
                cursor.execute(statement)
                invalid_counts[name] = int(cursor.fetchone()["count"])
            cursor.execute(
                "SELECT version,checksum_sha256 "
                "FROM rt.schema_migrations ORDER BY version"
            )
            stored_migrations = {
                str(row["version"]): str(row["checksum_sha256"])
                for row in cursor.fetchall()
            }
            migration_mismatches = sorted(
                version
                for version, checksum in expected_migrations.items()
                if stored_migrations.get(version) != checksum
            ) + sorted(
                version
                for version in set(stored_migrations)
                - set(expected_migrations)
            )
            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
                (list(EXPECTED_ROLES),),
            )
            stored_roles = {
                str(row["rolname"]) for row in cursor.fetchall()
            }
            missing_roles = sorted(set(EXPECTED_ROLES) - stored_roles)
            cursor.execute(
                "SELECT phase FROM rt.security_phase_events "
                "ORDER BY event_id DESC LIMIT 1"
            )
            phase_row = cursor.fetchone()
            security_phase = (
                None if phase_row is None else str(phase_row["phase"])
            )
            cursor.execute(
                "SELECT COUNT(*) AS count "
                "FROM rt.advanced_realtime_service_states"
            )
            service_state_rows = int(cursor.fetchone()["count"])
        else:
            service_state_rows = 0
    return {
        "status": (
            "VALID"
            if not missing_relations
            and all(count == 0 for count in invalid_counts.values())
            and not migration_mismatches
            and not missing_roles
            and security_phase == "AUDITED"
            else "INVALID"
        ),
        "required_relation_count": len(REQUIRED_RELATIONS),
        "missing_relations": list(missing_relations),
        "invalid_counts": invalid_counts,
        "migration_mismatches": migration_mismatches,
        "missing_roles": missing_roles,
        "security_phase": security_phase,
        "service_state_rows": service_state_rows,
        "service_state_authority": "STALE_UNTIL_NEW_LEASE",
    }


def main() -> int:
    result = validate()
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["status"] != "VALID":
        return 1
    print("ADVANCED_REALTIME_V3_RESTORE_VALIDATED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
