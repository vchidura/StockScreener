SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_options_flow_package_evidence (
    evidence_id uuid PRIMARY KEY,
    evidence_sha256 char(64) NOT NULL UNIQUE,
    evidence_version varchar(64) NOT NULL,
    dataset_id uuid NOT NULL REFERENCES rt.detector_datasets(dataset_id),
    evaluation_id uuid NOT NULL UNIQUE
        REFERENCES rt.detector_evaluations(evaluation_id),
    candidate_id uuid REFERENCES rt.option_packages(candidate_id),
    selection_status varchar(32) NOT NULL,
    reference_state_id uuid NOT NULL
        REFERENCES rt.advanced_options_reference_states(state_id),
    rolling_state_id uuid NOT NULL
        REFERENCES rt.advanced_options_rolling_checkpoints(state_id),
    evidence_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        evidence_version = 'advanced-options-flow-package-evidence-v1'
        AND evidence_sha256 ~ '^[0-9a-f]{64}$'
        AND selection_status IN (
            'SELECTED',
            'NOT_SELECTED',
            'REPEAT',
            'OBSERVATION'
        )
        AND (
            (selection_status = 'SELECTED' AND candidate_id IS NOT NULL)
            OR
            (selection_status <> 'SELECTED' AND candidate_id IS NULL)
        )
        AND jsonb_typeof(evidence_document) = 'object'
    )
);

CREATE INDEX idx_rt_advanced_options_flow_package_evidence_dataset
    ON rt.advanced_options_flow_package_evidence (
        dataset_id,
        selection_status,
        evidence_id
    );

CREATE TRIGGER trg_rt_advanced_options_flow_package_evidence_immutable
    BEFORE UPDATE OR DELETE ON rt.advanced_options_flow_package_evidence
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_flow_package_evidence_no_truncate
    BEFORE TRUNCATE ON rt.advanced_options_flow_package_evidence
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
