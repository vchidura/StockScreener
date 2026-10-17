SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.bar_revisions
    DROP CONSTRAINT bar_revisions_check5,
    ADD CONSTRAINT bar_revisions_source_lineage_check CHECK (
        (
            provider_tier = 'MASSIVE_ADVANCED'
            AND source_kind = 'REALTIME_STREAM'
            AND ingestion_segment_id IS NULL
            AND journal_generation IS NOT NULL
            AND stream_session_id IS NOT NULL
            AND source_offset > 0
            AND cardinality(source_revision_ids) = 0
        ) OR (
            provider_tier = 'MASSIVE_ADVANCED'
            AND source_kind = 'NATIVE_REST'
            AND ingestion_segment_id IS NOT NULL
            AND journal_generation IS NULL
            AND stream_session_id IS NULL
            AND source_offset IS NULL
            AND cardinality(source_revision_ids) = 0
            AND quality_codes @> ARRAY['EXPLICIT_ADVANCED_REST_REPAIR_V1']::text[]
        ) OR (
            provider_tier = 'MASSIVE_DEVELOPER'
            AND source_kind = 'NATIVE_REST'
            AND ingestion_segment_id IS NOT NULL
            AND journal_generation IS NULL
            AND stream_session_id IS NULL
            AND source_offset IS NULL
            AND cardinality(source_revision_ids) = 0
        ) OR (
            source_kind IN ('DERIVED', 'RECONCILED')
            AND ingestion_segment_id IS NOT NULL
            AND journal_generation IS NULL
            AND stream_session_id IS NULL
            AND source_offset IS NULL
            AND cardinality(source_revision_ids) > 0
        )
    );
