-- Immutable, all-universe Opportunity Board revisions and ordered membership.

CREATE TABLE IF NOT EXISTS option_board_publications (
    publication_id uuid PRIMARY KEY,
    scheduled_cycle timestamp with time zone NOT NULL,
    as_of_session date NOT NULL,
    status character varying(16) NOT NULL,
    selector_version character varying(64) NOT NULL,
    selector_sha256 character(64) NOT NULL,
    strategy_policy_sha256 character(64) NOT NULL,
    configuration_sha256 character(64) NOT NULL,
    expected_underlying_count integer NOT NULL,
    covered_underlying_count integer NOT NULL,
    source_matrix_ids uuid[] NOT NULL,
    market_data_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    selection_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    published_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),

    CONSTRAINT option_board_publications_cycle_configuration_key UNIQUE (
        scheduled_cycle, selector_sha256,
        strategy_policy_sha256, configuration_sha256
    ),
    CHECK (status = 'COMPLETE'),
    CHECK (selector_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (strategy_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (configuration_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (expected_underlying_count > 0),
    CHECK (covered_underlying_count = expected_underlying_count),
    CHECK (cardinality(source_matrix_ids) = covered_underlying_count),
    CHECK (market_data_time <= observed_time),
    CHECK (observed_time <= published_at),
    CHECK (jsonb_typeof(selection_evidence) = 'object')
);

CREATE TABLE IF NOT EXISTS option_board_members (
    publication_id uuid NOT NULL
        REFERENCES option_board_publications(publication_id),
    candidate_id uuid NOT NULL
        REFERENCES option_strategy_candidates(candidate_id),
    underlying character varying(16) NOT NULL,
    strategy_name character varying(64) NOT NULL,
    candidate_kind character varying(32) NOT NULL,
    board_position integer NOT NULL,
    raw_candidate_rank integer NOT NULL,
    source_matrix_id uuid NOT NULL
        REFERENCES option_analysis_runs(matrix_id),
    selection_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamp with time zone NOT NULL DEFAULT now(),

    PRIMARY KEY (publication_id, candidate_id),
    CONSTRAINT option_board_members_lane_position_key UNIQUE (
        publication_id, underlying, strategy_name,
        candidate_kind, board_position
    ),
    CHECK (candidate_kind IN ('RESEARCH_ONLY', 'SINGLE_CONTRACT', 'MULTI_LEG')),
    CHECK (board_position BETWEEN 1 AND 3),
    CHECK (raw_candidate_rank > 0),
    CHECK (jsonb_typeof(selection_evidence) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_option_board_publications_latest
    ON option_board_publications
        (strategy_policy_sha256, selector_sha256, scheduled_cycle DESC);

CREATE INDEX IF NOT EXISTS idx_option_board_members_candidate
    ON option_board_members (candidate_id, publication_id);

CREATE INDEX IF NOT EXISTS idx_option_board_members_lookup
    ON option_board_members
        (publication_id, underlying, strategy_name, candidate_kind, board_position);

COMMENT ON TABLE option_board_publications IS
    'Immutable complete-universe Opportunity Board revisions produced after a full option slot.';

COMMENT ON TABLE option_board_members IS
    'Ordered candidates selected by the recorded Board selector for one immutable publication.';