SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Product-scope the existing Advanced overlay. Stocks retain AM and Options uses
-- one combined T/Q authorization; no parallel ingress control plane is created.
DO $block$
DECLARE
    constraint_name text;
BEGIN
    SELECT catalog_constraint.conname INTO constraint_name
    FROM pg_constraint AS catalog_constraint
    WHERE catalog_constraint.conrelid =
            'rt.advanced_stream_authorizations'::regclass
      AND catalog_constraint.contype = 'c'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%provider%'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%feed%'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%channel%'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%STOCKS%'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%AM%';
    IF constraint_name IS NULL THEN
        RAISE EXCEPTION 'Advanced authorization product constraint is unavailable';
    END IF;
    EXECUTE format(
        'ALTER TABLE rt.advanced_stream_authorizations DROP CONSTRAINT %I',
        constraint_name
    );
END;
$block$;

ALTER TABLE rt.advanced_stream_authorizations
    ADD CONSTRAINT advanced_stream_authorizations_product_scope_check
    CHECK (
        provider = 'massive'
        AND (
            (feed = 'STOCKS' AND channel = 'AM')
            OR (feed = 'OPTIONS' AND channel = 'T,Q')
        )
    );

DO $block$
DECLARE
    constraint_name text;
BEGIN
    SELECT catalog_constraint.conname INTO constraint_name
    FROM pg_constraint AS catalog_constraint
    WHERE catalog_constraint.conrelid =
            'rt.advanced_ingress_runtime_profiles'::regclass
      AND catalog_constraint.contype = 'c'
      AND pg_get_constraintdef(catalog_constraint.oid)
            LIKE '%provider_tier%'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%feed%'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%channels%'
      AND pg_get_constraintdef(catalog_constraint.oid)
            LIKE '%MASSIVE_ADVANCED%'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%STOCKS%'
      AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%AM%';
    IF constraint_name IS NULL THEN
        RAISE EXCEPTION 'Advanced runtime profile product constraint is unavailable';
    END IF;
    EXECUTE format(
        'ALTER TABLE rt.advanced_ingress_runtime_profiles '
        'DROP CONSTRAINT %I',
        constraint_name
    );
END;
$block$;

ALTER TABLE rt.advanced_ingress_runtime_profiles
    ADD CONSTRAINT advanced_ingress_runtime_profiles_product_scope_check
    CHECK (
        universe_target BETWEEN 1 AND 400
        AND ladder_position >= 0
        AND (
            (ladder_position = 0 AND previous_universe_target IS NULL)
            OR (
                ladder_position > 0
                AND previous_universe_target BETWEEN 1 AND 400
                AND previous_universe_target < universe_target
            )
        )
        AND provider_tier = 'MASSIVE_ADVANCED'
        AND (
            (feed = 'STOCKS' AND channels = ARRAY['AM']::text[])
            OR (
                feed = 'OPTIONS'
                AND channels = ARRAY['T','Q']::text[]
            )
        )
        AND jsonb_typeof(profile_document) = 'object'
    );

DROP INDEX rt.uq_rt_advanced_ingress_active_profile_target;
CREATE UNIQUE INDEX uq_rt_advanced_ingress_active_profile_target
    ON rt.advanced_ingress_runtime_profiles (
        feed,
        channels,
        universe_target
    )
    WHERE status = 'ACTIVE';

CREATE OR REPLACE FUNCTION rt.enforce_advanced_authorization_promotion()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    profile_row rt.advanced_ingress_runtime_profiles%ROWTYPE;
    parent_row rt.advanced_stream_authorizations%ROWTYPE;
    expected_channels text[];
BEGIN
    expected_channels := CASE NEW.feed
        WHEN 'STOCKS' THEN ARRAY['AM']::text[]
        WHEN 'OPTIONS' THEN ARRAY['T','Q']::text[]
        ELSE ARRAY[]::text[]
    END;
    SELECT * INTO profile_row
    FROM rt.advanced_ingress_runtime_profiles
    WHERE universe_target = NEW.allowed_universe_targets[1]
      AND runtime_policy_sha256 = NEW.runtime_policy_sha256
      AND provider_tier = 'MASSIVE_ADVANCED'
      AND feed = NEW.feed
      AND channels = expected_channels
      AND status = 'ACTIVE';
    IF profile_row.runtime_profile_sha256 IS NULL THEN
        RAISE EXCEPTION
            'standing authorization requires an ACTIVE product runtime profile';
    END IF;
    IF profile_row.ladder_position = 0 THEN
        IF NEW.parent_authorization_id IS NOT NULL THEN
            RAISE EXCEPTION
                'initial ladder authorization cannot have a parent';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.parent_authorization_id IS NULL THEN
        RAISE EXCEPTION 'promotion requires a parent authorization';
    END IF;
    SELECT * INTO parent_row
    FROM rt.advanced_stream_authorizations
    WHERE authorization_id = NEW.parent_authorization_id;
    IF parent_row.authorization_id IS NULL
       OR parent_row.status <> 'READY'
       OR (parent_row.feed, parent_row.channel)
            IS DISTINCT FROM (NEW.feed, NEW.channel)
       OR parent_row.allowed_universe_targets[1]
            <> profile_row.previous_universe_target
       OR parent_row.runtime_policy_sha256 <> NEW.runtime_policy_sha256
       OR NEW.valid_from <= parent_row.valid_through THEN
        RAISE EXCEPTION
            'promotion must follow one product ladder rung at the next session';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE FUNCTION rt.enforce_advanced_ingress_product_scope()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    auth_row rt.advanced_stream_authorizations%ROWTYPE;
    profile_row rt.advanced_ingress_runtime_profiles%ROWTYPE;
    manifest_kind_value varchar(32);
    underlying_count integer;
    slot_contracts integer;
BEGIN
    SELECT * INTO auth_row
    FROM rt.advanced_stream_authorizations
    WHERE authorization_id = NEW.authorization_id;
    SELECT * INTO profile_row
    FROM rt.advanced_ingress_runtime_profiles
    WHERE runtime_profile_sha256 = NEW.runtime_profile_sha256;
    IF auth_row.authorization_id IS NULL
       OR profile_row.runtime_profile_sha256 IS NULL
       OR auth_row.feed <> profile_row.feed
       OR (
            auth_row.feed = 'STOCKS'
            AND (
                auth_row.channel <> 'AM'
                OR profile_row.channels <> ARRAY['AM']::text[]
            )
       )
       OR (
            auth_row.feed = 'OPTIONS'
            AND (
                auth_row.channel <> 'T,Q'
                OR profile_row.channels <> ARRAY['T','Q']::text[]
            )
       ) THEN
        RAISE EXCEPTION
            'advanced ingress plan product scope does not match';
    END IF;
    IF auth_row.feed = 'OPTIONS' THEN
        SELECT manifest_kind, option_underlying_count
        INTO manifest_kind_value, underlying_count
        FROM rt.subscription_manifests
        WHERE manifest_id = NEW.manifest_id;
        IF manifest_kind_value <> 'MARKET_OPTIONS'
           OR underlying_count < 1 THEN
            RAISE EXCEPTION
                'Advanced Options requires a MARKET_OPTIONS manifest';
        END IF;
        SELECT COUNT(DISTINCT member.instrument_id)
        INTO slot_contracts
        FROM rt.slot_set_current AS current
        JOIN rt.slot_set_members AS member
          ON member.cohort_id = current.cohort_id
         AND member.version = current.version
        WHERE current.cohort_id = NEW.cohort_id;
        IF slot_contracts IS NULL
           OR slot_contracts NOT BETWEEN 1 AND 1000 THEN
            RAISE EXCEPTION
                'Advanced Options requires an active bounded slot set';
        END IF;
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_ingress_options_product_scope
    BEFORE INSERT ON rt.advanced_ingress_plans
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_ingress_product_scope();

-- Every slot contract has exactly T and Q membership, one purpose/catalog
-- identity, and the provider-wide cap includes retained and position slots.
CREATE FUNCTION rt.enforce_option_slot_set_contract()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    contract_count integer;
    complete_count integer;
BEGIN
    SELECT
        COUNT(*),
        COUNT(*) FILTER (
            WHERE member_count = 2
              AND channel_count = 2
              AND purpose_count = 1
              AND catalog_count = 1
        )
    INTO contract_count, complete_count
    FROM (
        SELECT
            instrument_id,
            COUNT(*) AS member_count,
            COUNT(DISTINCT channel) AS channel_count,
            COUNT(DISTINCT purpose) AS purpose_count,
            COUNT(DISTINCT catalog_version_id) AS catalog_count
        FROM rt.slot_set_members
        WHERE cohort_id = NEW.cohort_id
          AND version = NEW.version
        GROUP BY instrument_id
    ) AS contracts;
    IF contract_count > 1000 OR complete_count <> contract_count THEN
        RAISE EXCEPTION
            'option slot set must contain at most 1000 exact T/Q contracts';
    END IF;
    RETURN NULL;
END;
$function$;

CREATE CONSTRAINT TRIGGER trg_rt_option_slot_set_contract
    AFTER INSERT ON rt.slot_set_members
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_option_slot_set_contract();

-- The immutable stage catalog advances without changing stages_v1-v3.
INSERT INTO rt.stage_catalog (
    stage_id,
    version,
    domain,
    prerequisites,
    execution_lane,
    schedule_kind,
    gates_strategy,
    deadline_offset_seconds
) VALUES
    ('REFERENCE','stages_v4','shared',ARRAY[]::text[],'data','INTERVAL',false,NULL),
    ('EQUITY_INPUT_GATE','stages_v4','equity',ARRAY['REFERENCE'],'data','WINDOW',false,120),
    ('EQUITY_MATERIALIZE','stages_v4','equity',ARRAY['EQUITY_INPUT_GATE'],'data','WINDOW',false,120),
    ('STOCK_ALERTS','stages_v4','equity',ARRAY['EQUITY_MATERIALIZE'],'alert','WINDOW',false,120),
    ('EQUITY_PROJECTION','stages_v4','equity',ARRAY['EQUITY_MATERIALIZE'],'portal','COALESCED',false,300),
    ('PORTAL_PUBLISH','stages_v4','equity',ARRAY['EQUITY_PROJECTION'],'portal','COALESCED',false,300),
    ('STRATEGY_PUBLISH','stages_v4','equity',ARRAY['EQUITY_MATERIALIZE'],'portal','COALESCED',false,300),
    ('OPTIONS_ACQUIRE','stages_v4','options',ARRAY['REFERENCE'],'options','SESSION',false,NULL),
    ('OPTIONS_ADMISSION','stages_v4','options',ARRAY['OPTIONS_ACQUIRE'],'options','SESSION',false,NULL),
    ('OPTIONS_FLOW','stages_v4','options',ARRAY['REFERENCE'],'options','CONTINUOUS',false,NULL),
    ('OPTIONS_INPUT_GATE','stages_v4','options',ARRAY['OPTIONS_ADMISSION','EQUITY_MATERIALIZE'],'options','WINDOW',false,NULL),
    ('OPTIONS_CYCLE','stages_v4','options',ARRAY['OPTIONS_INPUT_GATE'],'options','WINDOW',true,NULL),
    ('OPTIONS_PROJECTION','stages_v4','options',ARRAY['OPTIONS_CYCLE'],'options','COALESCED',false,NULL),
    ('OPTIONS_PUBLISH','stages_v4','options',ARRAY['OPTIONS_PROJECTION'],'options','COALESCED',false,NULL);

ALTER TABLE rt.cycle_receipts
    DROP CONSTRAINT cycle_receipts_receipt_kind_check,
    ALTER COLUMN receipt_kind TYPE varchar(32),
    ADD CONSTRAINT cycle_receipts_receipt_kind_check
    CHECK (
        receipt_kind IN (
            'EQUITY_CYCLE',
            'OPTIONS_CYCLE',
            'INPUT_READY',
            'PORTAL_READY',
            'OPTIONS_INPUT_READY',
            'OPTIONS_PORTAL_READY'
        )
    );

-- One canonical writer owns a journal generation for an exact condition/flow
-- policy identity. Detector cohorts reference this source rather than rewriting it.
CREATE TABLE rt.option_source_flows (
    source_flow_id uuid PRIMARY KEY,
    source_flow_sha256 char(64) NOT NULL UNIQUE,
    source_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    journal_generation uuid NOT NULL,
    stream_session_ids uuid[] NOT NULL,
    owner_name varchar(64) NOT NULL,
    condition_semantics_sha256 char(64) NOT NULL,
    flow_policy_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        journal_generation,
        condition_semantics_sha256,
        flow_policy_sha256
    ),
    UNIQUE (
        source_flow_id,
        condition_semantics_sha256,
        flow_policy_sha256
    ),
    UNIQUE (source_flow_id, source_cohort_id),
    CHECK (source_flow_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        condition_semantics_sha256 ~ '^[0-9a-f]{64}$'
        AND flow_policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (cardinality(stream_session_ids) > 0),
    CHECK (owner_name = 'option_flow_stream_v1')
);

CREATE FUNCTION rt.enforce_option_source_flow_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    unique_sessions integer;
BEGIN
    SELECT COUNT(DISTINCT session_id)
    INTO unique_sessions
    FROM unnest(NEW.stream_session_ids) AS session_id;
    IF unique_sessions <> cardinality(NEW.stream_session_ids) THEN
        RAISE EXCEPTION 'source flow stream sessions must be unique';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_option_source_flows_exact_sessions
    BEFORE INSERT ON rt.option_source_flows
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_option_source_flow_insert();
CREATE TRIGGER trg_rt_option_source_flows_immutable
    BEFORE UPDATE OR DELETE ON rt.option_source_flows
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_source_flows_no_truncate
    BEFORE TRUNCATE ON rt.option_source_flows
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE rt.option_flow_events
    ADD COLUMN source_flow_id uuid,
    ADD COLUMN condition_semantics_sha256 char(64),
    ADD COLUMN flow_policy_sha256 char(64),
    ADD COLUMN lineage_version smallint NOT NULL DEFAULT 1;

DO $block$
DECLARE
    constraint_name text;
BEGIN
    SELECT catalog_constraint.conname INTO constraint_name
    FROM pg_constraint AS catalog_constraint
    WHERE catalog_constraint.conrelid = 'rt.option_flow_events'::regclass
      AND catalog_constraint.contype = 'u'
      AND pg_get_constraintdef(catalog_constraint.oid)
            LIKE '%contract_ticker, source_seq, event_time, revision%';
    IF constraint_name IS NULL THEN
        RAISE EXCEPTION 'Legacy option flow natural key is unavailable';
    END IF;
    EXECUTE format(
        'ALTER TABLE rt.option_flow_events DROP CONSTRAINT %I',
        constraint_name
    );
END;
$block$;

ALTER TABLE rt.option_flow_events
    ALTER COLUMN lineage_version SET DEFAULT 2,
    ADD CONSTRAINT option_flow_events_source_flow_fkey
    FOREIGN KEY (
        source_flow_id,
        condition_semantics_sha256,
        flow_policy_sha256
    ) REFERENCES rt.option_source_flows (
        source_flow_id,
        condition_semantics_sha256,
        flow_policy_sha256
    ),
    ADD CONSTRAINT option_flow_events_lineage_check
    CHECK (
        (
            lineage_version = 1
            AND source_flow_id IS NULL
            AND condition_semantics_sha256 IS NULL
            AND flow_policy_sha256 IS NULL
        )
        OR (
            lineage_version = 2
            AND journal_generation IS NOT NULL
            AND source_flow_id IS NOT NULL
            AND condition_semantics_sha256 ~ '^[0-9a-f]{64}$'
            AND flow_policy_sha256 ~ '^[0-9a-f]{64}$'
        )
    );

CREATE UNIQUE INDEX uq_rt_option_flow_event_legacy_source
    ON rt.option_flow_events (
        contract_ticker,
        source_seq,
        event_time,
        revision
    )
    WHERE lineage_version = 1;
CREATE UNIQUE INDEX uq_rt_option_flow_event_source_policy
    ON rt.option_flow_events (
        source_flow_id,
        contract_ticker,
        source_seq,
        event_time,
        revision
    )
    WHERE lineage_version = 2;

ALTER TABLE rt.option_flow_clusters
    ADD COLUMN source_flow_id uuid,
    ADD COLUMN condition_semantics_sha256 char(64),
    ADD COLUMN lineage_version smallint NOT NULL DEFAULT 1;

ALTER TABLE rt.option_flow_clusters
    ALTER COLUMN lineage_version SET DEFAULT 2,
    ADD CONSTRAINT option_flow_clusters_source_flow_fkey
    FOREIGN KEY (
        source_flow_id,
        condition_semantics_sha256,
        policy_sha256
    ) REFERENCES rt.option_source_flows (
        source_flow_id,
        condition_semantics_sha256,
        flow_policy_sha256
    ),
    ADD CONSTRAINT option_flow_clusters_lineage_check
    CHECK (
        (
            lineage_version = 1
            AND source_flow_id IS NULL
            AND condition_semantics_sha256 IS NULL
        )
        OR (
            lineage_version = 2
            AND source_flow_id IS NOT NULL
            AND condition_semantics_sha256 ~ '^[0-9a-f]{64}$'
        )
    );

ALTER TABLE rt.detector_event_inputs
    ADD COLUMN source_flow_id uuid,
    ADD COLUMN source_cohort_id uuid,
    ADD CONSTRAINT detector_event_inputs_source_flow_fkey
    FOREIGN KEY (source_flow_id, source_cohort_id)
    REFERENCES rt.option_source_flows (
        source_flow_id,
        source_cohort_id
    ),
    ADD CONSTRAINT detector_event_inputs_source_flow_check
    CHECK (
        (
            source_flow_id IS NULL
            AND source_cohort_id IS NULL
        )
        OR (
            source_flow_id IS NOT NULL
            AND source_cohort_id IS NOT NULL
            AND journal_generation IS NOT NULL
            AND stream_session_id IS NOT NULL
            AND source_offset IS NOT NULL
        )
    );
