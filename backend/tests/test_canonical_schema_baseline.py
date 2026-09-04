import re
from pathlib import Path

from equity.portal_snapshots import SNAPSHOT_TYPES


BACKEND_DIR = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = BACKEND_DIR / "migrations"
BASELINE = MIGRATIONS_DIR / "000_canonical_schema.sql"


def baseline_sql() -> str:
    return BASELINE.read_text(encoding="utf-8")


def normalized_sql() -> str:
    return " ".join(baseline_sql().split())


def test_baseline_is_the_only_schema_migration() -> None:
    assert sorted(path.name for path in MIGRATIONS_DIR.glob("*.sql")) == [
        "000_canonical_schema.sql"
    ]
    runner = (
        BACKEND_DIR / "scripts" / "run_equity_materialization.py"
    ).read_text(encoding="utf-8")
    assert "from stock_screener.schema import BASELINE_VERSION" in runner
    assert "MIGRATION_PATHS" not in runner


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
        "equity/scanner_research.py",
        "scripts/refresh_equity_portal_snapshots.py",
    ):
        source = (BACKEND_DIR / relative).read_text(encoding="utf-8")
        assert "research.scanner_events" not in source
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
