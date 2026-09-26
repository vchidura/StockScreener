import re
from pathlib import Path

from equity.portal_snapshots import SNAPSHOT_TYPES


BACKEND_DIR = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = BACKEND_DIR / "migrations"
BASELINE = MIGRATIONS_DIR / "000_canonical_schema.sql"


def baseline_sql() -> str:
    return BASELINE.read_text(encoding="utf-8")


def test_detector_evaluations_migration_is_additive_and_matches_baseline():
    sql = (MIGRATIONS_DIR / "053_option_detector_evaluations.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "UNIQUE (dataset_id,run_id,detector_id,recurrence_sha256)" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql
    assert "payload_sha256 = encode(sha256" in sql
    assert "detector_id IN ('O1','O2','S1','S2')" in sql
    assert "detector_id='O2' AND selection_status='OBSERVATION' AND candidate_id IS NULL" in sql
    assert "surface evaluation must preserve observation-only identity" in sql
    assert "ALTER TABLE" not in sql and "DROP TABLE" not in sql


def test_o1_indicator_observation_migration_is_additive_and_matches_baseline():
    sql = (MIGRATIONS_DIR / "054_option_o1_indicator_observations.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "option_o1_indicator_observations" in sql
    assert "option_participation_indicator_review_v1" in sql
    assert "INDICATOR_SHADOW_ONLY" in sql
    assert "changes_admission'='false'::jsonb" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql
    assert "ALTER TABLE" not in sql and "DROP TABLE" not in sql


def test_stock_setup_indicator_observation_migration_is_additive_and_matches_baseline():
    sql = (MIGRATIONS_DIR / "055_option_stock_setup_indicator_observations.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "option_stock_setup_indicator_observations" in sql
    assert "stock_first_mixed_indicator_review_v1" in sql
    assert "MIXED_INDICATOR_SHADOW_ONLY" in sql
    assert "detector_id IN ('S1','S2')" in sql
    assert "changes_admission'='false'::jsonb" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql
    assert "ALTER TABLE" not in sql and "DROP TABLE" not in sql


def test_o3_credit_observation_migration_is_additive_shadow_only_and_matches_baseline():
    sql = (MIGRATIONS_DIR / "056_option_o3_credit_observations.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "option_o3_credit_observations" in sql
    assert "CREDIT_SHADOW_ONLY" in sql
    assert "INDICATIVE_MODEL_MARK_ONLY" in sql
    assert "candidate.strategy_name='SPREAD_RANGE_LOCATOR'" in sql
    assert "candidate.structure_type IN ('PUT_CREDIT_VERTICAL','CALL_CREDIT_VERTICAL')" in sql
    assert "publication_permission'='false'::jsonb" in sql
    assert "execution_permission'='false'::jsonb" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql
    assert "ALTER TABLE" not in sql and "DROP TABLE" not in sql


def test_o3_indicative_admission_migration_promotes_empty_prospective_relation():
    sql = (MIGRATIONS_DIR / "057_option_o3_indicative_admission.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "requires an empty prospective relation" in sql
    assert "selection_status') = 'SELECTED'" in sql
    assert "O3_INDICATIVE_ADMISSION" in sql
    assert "QUALIFIED_INDICATIVE" in sql
    assert "publication_permission'='false'::jsonb" in sql
    assert "execution_permission'='false'::jsonb" in sql
    assert "DROP TABLE" not in sql


def normalized_sql() -> str:
    return " ".join(baseline_sql().split())


def test_baseline_plus_numbered_incremental_migrations() -> None:
    names = sorted(path.name for path in MIGRATIONS_DIR.glob("*.sql"))
    assert names[0] == "000_canonical_schema.sql"
    # Everything after the baseline must be an ordered, uniquely numbered increment.
    increments = names[1:]
    assert all(re.fullmatch(r"\d{3}_[a-z0-9_]+\.sql", name) for name in increments)
    prefixes = [name[:3] for name in increments]
    assert len(set(prefixes)) == len(prefixes)
    assert all(prefix > "000" for prefix in prefixes)
    runner = (
        BACKEND_DIR / "scripts" / "run_equity_materialization.py"
    ).read_text(encoding="utf-8")
    assert "from stock_screener.schema import BASELINE_VERSION" in runner
    assert "MIGRATION_PATHS" not in runner


def test_universe_revision_migration_enforces_linear_immutable_complete_history():
    sql = (MIGRATIONS_DIR / "039_equity_universe_revisions.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "supersedes_universe_run_id uuid" in sql
    assert "uq_equity_universe_revision_successor" in sql
    assert "FOR UPDATE" in sql
    assert "NEW.policy_sha256 <> parent.policy_sha256" in sql
    assert "NEW.replay_available_at IS DISTINCT FROM parent.replay_available_at" in sql
    assert "published universe revision lineage is immutable" in sql
    assert "published universe revision members are immutable" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "member_count <> run.admitted_members" in sql
    assert "CREATE OR REPLACE VIEW public.equity_original_universe_runs" in sql


def test_action_coverage_response_migration_matches_baseline():
    sql = (MIGRATIONS_DIR / "040_equity_action_coverage_responses.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "response_action_count integer" in sql
    assert "response_sha256 character(64)" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "action.security_id = coverage.security_id" in sql
    assert "linked_count <> coverage.response_action_count" in sql
    assert "response-bound action coverage is immutable" in sql


def test_option_alert_publication_migration_matches_baseline_and_guards_evidence():
    sql = (MIGRATIONS_DIR / "043_option_alert_publications.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    for name in ("option_alert_plans", "option_alert_publication_events", "option_alert_publication_sequence"):
        assert f"public.{name}" in sql
    assert "UNIQUE (plan_id, request_key)" in sql
    assert "NEW.recorded_at := clock_timestamp()" in sql
    assert "NEW.sequence := nextval" in sql
    assert sql.index("pg_advisory_xact_lock") < sql.index("NEW.sequence := nextval")
    assert "FOR UPDATE" in sql
    assert "sha256(convert_to(payload_text, 'UTF8'))" in sql
    assert "terminal option alert cannot reopen" in sql
    assert "expired entry window only accepts expiration" in sql
    assert "active option exposure already published" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql
    assert "execution_permission' = 'false'::jsonb" in sql


def test_stock_behavior_migration_is_additive_isolated_and_in_baseline():
    sql = (MIGRATIONS_DIR / "044_equity_stock_behavior_contract.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "context_kind text NOT NULL DEFAULT 'LEGACY'" in sql
    assert sql.count("ADD COLUMN IF NOT EXISTS") == 6
    assert "ADD COLUMN IF NOT EXISTS behavior_payload_text text" in sql
    for removed in ("behavior_profile", "behavior_data_status", "behavior_alignment_state",
                    "behavior_available_at", "behavior_valid_until", "behavior_payload jsonb"):
        assert removed not in sql
    assert "payload_text::jsonb->>'profile' = strategy_horizon" in sql
    assert "payload_text::jsonb->>'available_at')::timestamptz = observed_at" in sql
    assert "payload_text::jsonb->>'valid_until')::timestamptz = valid_until" in sql
    assert "GENERATED ALWAYS" not in sql
    assert "observed_at < valid_until" in sql
    assert "NOT VALID" in sql and "CREATE INDEX" not in sql
    assert "SET LOCAL lock_timeout = '2s'" in sql
    assert "VALIDATE CONSTRAINT ck_equity_context_behavior_contract" in baseline_sql()
    assert "CREATE INDEX idx_equity_behavior_asof" in baseline_sql()
    assert "IS TRUE" in sql
    assert "qualified_direction IS NULL" in sql
    assert "stock behavior evidence is immutable" in sql
    assert "stock behavior evidence links are immutable" in sql
    assert "stock behavior source evidence is immutable" in sql
    assert "stock behavior evidence prevents evidence truncation" in sql
    assert "xmin::text = pg_current_xact_id()::text" in sql
    assert "legacy context cannot be reinterpreted" in sql
    assert "stock behavior evidence prevents context truncation" in sql
    assert "BEFORE TRUNCATE" in sql
    assert "NEW.created_at := clock_timestamp()" in sql
    assert "CREATE TABLE" not in sql
    assert not re.search(r"\b(?:UPDATE|DELETE FROM|DROP COLUMN|RENAME COLUMN|TRUNCATE)\s+public\.", sql)


def test_stock_behavior_schema_identifier_correction_matches_baseline():
    sql = (MIGRATIONS_DIR / "045_stock_behavior_adjusted_schema_identifier.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "stock_behavior_adjusted_1d_v1" in sql
    assert "CREATE OR REPLACE FUNCTION public.guard_equity_behavior_evidence_link" in sql
    assert "ALTER TABLE" not in sql and "CREATE INDEX" not in sql


def test_option_stock_behavior_assessment_migration_matches_baseline():
    sql = (MIGRATIONS_DIR / "046_option_stock_behavior_assessments.sql").read_text(encoding="utf-8")
    assert sql.strip() in baseline_sql()
    assert "CREATE TABLE IF NOT EXISTS public.option_stock_behavior_assessments" in sql
    assert "candidate_id uuid NOT NULL REFERENCES public.option_strategy_candidates" in sql
    assert "stock_snapshot_id uuid REFERENCES public.equity_context_snapshots" in sql
    assert "option_configuration_sha256 character(64) NOT NULL" in sql
    assert "option_market_policy_sha256 character(64) NOT NULL" in sql
    assert "option_analysis_policy_sha256 character(64) NOT NULL" in sql
    assert "candidate.policy_sha256 <> NEW.option_strategy_policy_sha256" in sql
    assert "candidate.market_data_time <> NEW.option_market_time" in sql
    assert "JOIN public.option_ingestion_runs AS ingestion USING (batch_id)" in sql
    assert "source_configuration_sha256 <> NEW.option_configuration_sha256" in sql
    assert "source_scheduled_cycle <> NEW.stock_market_cutoff" in sql
    assert "assessment_only' = 'true'::jsonb" in sql
    assert "execution_permission' = 'false'::jsonb" in sql
    assert "NEW.recorded_at := clock_timestamp()" in sql
    assert "candidate.candidate_identity <> NEW.candidate_identity" in sql
    assert "stock_context.context_kind <> 'STOCK_BEHAVIOR'" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql


def test_option_stock_behavior_launch_identity_migration_matches_baseline():
    sql = (MIGRATIONS_DIR / "047_option_stock_behavior_launch_identity.sql").read_text(
        encoding="utf-8"
    )
    assert sql.strip() in baseline_sql()
    assert "launch identity migration requires an empty assessment ledger" in sql
    assert "ADD COLUMN IF NOT EXISTS launch_id varchar(64)" in sql
    assert "ADD COLUMN IF NOT EXISTS launch_manifest_sha256 character(64)" in sql
    assert "payload_text::jsonb->>'launch_id' = launch_id" in sql
    assert "payload_text::jsonb->>'launch_manifest_sha256' = launch_manifest_sha256" in sql
    assert "idx_option_stock_behavior_launch" in sql


def test_option_package_assessment_migration_matches_baseline():
    sql = (MIGRATIONS_DIR / "048_option_package_assessments.sql").read_text(
        encoding="utf-8"
    )
    assert sql.strip() in baseline_sql()
    assert "CREATE TABLE IF NOT EXISTS public.option_package_assessments" in sql
    assert "candidate_id uuid NOT NULL REFERENCES public.option_strategy_candidates" in sql
    assert "matrix_id uuid NOT NULL REFERENCES public.option_analysis_runs" in sql
    assert "assessment_status IN ('READY', 'UNAVAILABLE')" in sql
    assert "package_payload_text text" in sql
    assert "sha256(convert_to(package_payload_text, 'UTF8'))" in sql
    assert "payload_text::jsonb->'package' = package_payload_text::jsonb" in sql
    assert "payload_text::jsonb->'research_only' = 'true'::jsonb" in sql
    assert "payload_text::jsonb->'execution_permission' = 'false'::jsonb" in sql
    assert "candidate.candidate_identity <> NEW.candidate_identity" in sql
    assert "NEW.recorded_at := clock_timestamp()" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql


def test_option_outcome_unavailable_migration_matches_baseline():
    sql = (MIGRATIONS_DIR / "049_option_outcome_unavailable_evidence.sql").read_text(
        encoding="utf-8"
    )
    assert sql.strip() in baseline_sql()
    assert "CREATE TABLE IF NOT EXISTS public.option_outcome_unavailable_evidence" in sql
    assert "candidate_id uuid NOT NULL REFERENCES public.option_strategy_candidates" in sql
    assert "missing_contract_ids bigint[] NOT NULL" in sql
    assert "payload_text::jsonb->>'status' = 'UNAVAILABLE'" in sql
    assert "payload_text::jsonb->'research_only' = 'true'::jsonb" in sql
    assert "payload_text::jsonb->'execution_permission' = 'false'::jsonb" in sql
    assert "NEW.recorded_at < NEW.availability_deadline" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql


def test_option_package_assessment_policy_migration_matches_baseline():
    sql = (MIGRATIONS_DIR / "050_option_package_assessment_policy.sql").read_text(
        encoding="utf-8"
    )
    assert sql.strip() in baseline_sql()
    assert "package assessment policy migration requires an empty ledger" in sql
    assert "ADD COLUMN IF NOT EXISTS assessment_policy_version" in sql
    assert "ADD COLUMN IF NOT EXISTS assessment_policy_sha256" in sql
    assert "payload_text::jsonb->>'assessment_policy_version'" in sql
    assert "payload_text::jsonb->>'assessment_policy_sha256'" in sql
    assert "idx_option_package_assessment_policy" in sql


def test_option_outcome_unavailable_leg_contract_matches_baseline():
    sql = (MIGRATIONS_DIR / "051_option_outcome_unavailable_leg_contract.sql").read_text(
        encoding="utf-8"
    )
    assert sql.strip() in baseline_sql()
    assert "unavailable leg contract migration requires an empty ledger" in sql
    assert "payload_text::jsonb->'required_contract_ids'" in sql
    assert "array_agg(contract_id ORDER BY leg_index)" in sql
    assert "valuation_policy_sha256=NEW.valuation_policy_sha256" in sql
    assert "observed_contract_ids <@ NEW.required_contract_ids" in sql
    assert "expected_missing_ids <> NEW.missing_contract_ids" in sql


def test_option_package_candidate_contract_matches_baseline():
    sql = (MIGRATIONS_DIR / "052_option_package_candidate_contract.sql").read_text(
        encoding="utf-8"
    )
    assert sql.strip() in baseline_sql()
    assert "option_package_assessment_matches_candidate" in sql
    assert "LOCK TABLE public.option_package_assessments" in sql
    assert "existing option package assessment disagrees with candidate" in sql
    assert "package_breakevens IS DISTINCT FROM candidate.breakevens" in sql
    assert "LEFT JOIN public.option_candidate_legs" in sql
    assert "WHERE candidate_id=candidate.candidate_id" in sql
    assert "valuation_policy_sha256' <> assessment_valuation_policy_sha256" in sql


def test_baseline_creates_final_canonical_inventory() -> None:
    created = set(re.findall(
        r"CREATE TABLE public\.(\w+)", baseline_sql(), re.IGNORECASE
    ))
    assert {
        "schema_migrations",
        "selected_tickers",
        "cross_sectional_signals",
        "market_discovery_states",
        "equity_security_reference_revisions",
        "equity_corporate_action_coverage",
        "equity_corporate_action_coverage_members",
        "equity_universe_runs",
        "equity_universe_members",
        "equity_ingestion_segments",
        "equity_bar_revisions",
        "equity_bar_publications",
        "equity_bar_publication_members",
        "equity_current_bar_projection",
        "equity_analysis_runs",
        "equity_analysis_members",
        "equity_evidence",
        "equity_context_snapshots",
        "equity_current_projection",
        "equity_outcome_policies",
        "equity_research_outcomes",
        "equity_qualification_revisions",
        "equity_portal_source_state",
        "equity_portal_snapshots",
        "equity_portal_current_projections",
        "option_contract_catalog",
        "option_event_calendar_coverage",
        "option_risk_free_rate_observations",
        "option_context_market_event_evidence",
        "option_context_event_coverage_evidence",
        "option_chain_snapshots",
        "option_trade_events",
        "option_strategy_registry",
        "option_strategy_candidates",
        "option_signal_decay_outcomes",
    }.issubset(created)


def test_baseline_excludes_retired_relations() -> None:
    sql = baseline_sql()
    for retired in (
        "stock_prices_daily",
        "stock_prices_hourly",
        "stock_prices_intraday",
        "scanner_events",
        "scanner_event_occurrences",
        "scanner_event_outcomes",
        "scanner_portal_",
        "equity_portal_cutover_probes",
        "legacy_archive",
    ):
        assert retired not in sql


def test_baseline_is_atomic_versioned_and_bootstrapped() -> None:
    sql = baseline_sql()
    assert "BEGIN;" in sql
    assert sql.rstrip().endswith("COMMIT;")
    assert "CREATE SCHEMA public" not in sql
    assert "VALUES ('000_canonical_schema')" in sql
    assert "INSERT INTO public.equity_portal_source_state" in sql
    assert sql.count("SELECT public.ensure_option_market_data_partitions(") == 2
    assert not re.search(
        r"option_(?:chain_snapshots|trade_events)_y20\d{4}", sql
    )


def test_equity_bars_views_and_lineage_are_final() -> None:
    sql = baseline_sql()
    normalized = normalized_sql()
    assert "ck_equity_ingestion_segment_provider_mode" in sql
    assert "'HISTORICAL_RECONSTRUCTED'" in sql
    assert "replay_available_at" in sql
    assert "uq_equity_bar_revision_identity UNIQUE" in normalized
    assert "session_scope, adjusted, source_kind, availability_mode, payload_sha256" in normalized
    assert "trg_equity_bar_revisions_append_only" in sql
    assert "trg_validate_equity_current_bar_projection" in sql
    assert "CREATE VIEW public.equity_canonical_bars" in sql
    assert "CREATE VIEW public.equity_canonical_daily_bars" in sql
    assert "CREATE VIEW public.equity_canonical_hourly_bars" in sql
    assert "source_kind)::text = 'RECONCILED'" in normalized
    assert "revision.is_final = true" in normalized
    assert "revision.adjusted = false" in normalized


def test_outcomes_and_qualification_keep_causal_provenance() -> None:
    sql = baseline_sql()
    normalized = normalized_sql()
    assert "ck_equity_outcome_policy_horizons_object" in sql
    assert "jsonb_typeof(horizons) = 'object'" in normalized
    assert "entry_time > signal_time" in normalized
    assert "entry_time > confirmation_bar_end" in normalized
    assert "ck_equity_qualification_report_identity" in sql
    assert "market_benchmark_ticker" in sql
    assert "sector_benchmark_ticker" in sql
    assert "idx_equity_outcomes_benchmarks" in sql


def test_portal_snapshot_types_and_dependencies_are_complete() -> None:
    sql = baseline_sql()
    for snapshot_type in SNAPSHOT_TYPES:
        assert snapshot_type in sql
    for trigger in (
        "trg_selected_tickers_equity_portal_source",
        "trg_discovery_states_equity_portal_source",
        "trg_cross_sectional_equity_portal_source",
        "trg_equity_bar_publications_portal_source",
    ):
        assert trigger in sql
    assert "ARRAY['1d'::character varying, '1wk'::character varying]" in sql
    assert "ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying]" in sql


def test_option_schema_keeps_partition_and_proxy_contracts() -> None:
    sql = baseline_sql()
    normalized = normalized_sql()
    assert "ensure_option_market_data_partitions" in sql
    assert "LANGUAGE plpgsql SECURITY DEFINER" in sql
    assert "partition maintenance is limited to current and next month" in sql
    assert (
        "REVOKE ALL ON FUNCTION public.ensure_option_market_data_partitions(date) "
        "FROM PUBLIC"
    ) in normalized
    assert "option_market_data_partitions_ready" in sql
    assert "PARTITION BY RANGE (first_observed_at)" in normalized
    assert "PARTITION BY RANGE (sip_timestamp)" in normalized
    assert "uq_option_ingestion_slot_cohort" in sql
    assert (
        "provider, underlying, scheduled_cycle, request_filter_sha256, "
        "policy_sha256, configuration_sha256"
    ) in normalized
    assert "ck_option_decay_proxy_provenance" in sql
    assert "RESEARCH_DELAYED_PROXY" in sql
    assert "uq_option_decay_candidate_measurement_policy" in sql
    assert "valuation_policy_sha256" in sql


def test_runtime_sources_do_not_reference_retired_scanner_modules() -> None:
    for relative in (
        "main.py",
        "equity/stock_alert_views.py",
        "equity/sector_research.py",
        "scripts/refresh_equity_portal_snapshots.py",
    ):
        source = (BACKEND_DIR / relative).read_text(encoding="utf-8")
        assert "research.scanner_events" not in source
        assert "equity.scanner_research" not in source
        assert "equity.legacy" not in source


def test_runtime_and_research_sources_do_not_mutate_schema() -> None:
    ddl = re.compile(
        r"\b(?:CREATE|ALTER|DROP)\s+(?:TABLE|INDEX|VIEW|SCHEMA)\b",
        re.IGNORECASE,
    )
    for relative in (
        "main.py",
        "database.py",
        "research/evaluate.py",
        "scripts/discover_universe_polygon.py",
        "scripts/generate_cross_sectional_signal.py",
        "scripts/generate_market_discovery.py",
        "scripts/prepare_historical_signal_research.py",
        "scripts/run_historical_signal_outcomes.py",
    ):
        source = (BACKEND_DIR / relative).read_text(encoding="utf-8")
        assert ddl.search(source) is None, relative
