#!/usr/bin/env python3
"""Apply the V2 baseline to a disposable database and verify it.

Creates test_realtime_v2_*, applies backend/realtime/sql_v2, compares the live catalog with
the relation registry and exercises the blocker-driven rules (stage independence,
zero-candidate diagnostics, AM-only manifests, slot caps, partition behavior). The database
is always dropped. It never touches stocks_realtime_v1 or stocks_realtime_v2 and makes no
provider request.

Two phases. STRUCTURE needs no roles (the runtime connects as the owner) and is
what --security-mode deferred runs. SECURITY is the final hardening phase (create the admin, app
and api roles, apply grants, exact-privilege audit, ledger AUDITED) and also runs with
--security-mode hardened.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2
from dotenv import load_dotenv
from psycopg2 import errors, sql

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    PHASE_SECURITY,
    PHASE_STRUCTURE,
    SECURITY_MODE_DEFERRED,
    SECURITY_MODE_HARDENED,
    SECURITY_MODES_V2,
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
    v2_provisioning_phases,
    validate_target_database_v2,
)
from realtime.relation_registry import (  # noqa: E402
    PARTITION_POLICY_SEEDS,
    PARTITIONED_RELATIONS,
    RELATIONS,
    SECURITY_POLICY_VERSION,
    VIEWS,
    security_policy,
)
from realtime.security_v2 import (  # noqa: E402
    apply_database_security_v2,
    audit_database_security_v2,
    build_role_names_v2,
    create_disposable_roles_v2,
    current_security_phase_v2,
    drop_disposable_roles_v2,
    record_security_audit_v2,
)

HASH = "0" * 63 + "1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--security-mode", choices=SECURITY_MODES_V2, default=SECURITY_MODE_HARDENED,
                        help="deferred runs the structure phase only (no roles); hardened adds the final "
                             "security phase")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--validate-existing",
        action="store_true",
        help="validate an already-provisioned disposable database without owning or dropping it",
    )
    args = parser.parse_args()
    validate_target_database_v2(args.database, args.confirm_database_name, allow_disposable=True,
                                security_mode=args.security_mode)
    if args.validate_existing and not args.execute:
        parser.error("--validate-existing requires --execute")
    return args


def _settings() -> dict[str, str]:
    load_dotenv(BACKEND_DIR / ".env", override=True)
    names = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("requires: " + ", ".join(missing))
    return settings


def _connect(settings: dict[str, str], database: str):
    return psycopg2.connect(
        dbname=database, user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"],
        port=settings["DB_PORT"], application_name="validate_realtime_v2_schema",
        options="-c timezone=UTC",
    )


def _expect(cursor, name: str, exception, statement: str, parameters=()) -> tuple[str, str]:
    cursor.execute("SAVEPOINT expectation")
    try:
        cursor.execute(statement, parameters)
    except exception:
        cursor.execute("ROLLBACK TO SAVEPOINT expectation")
        return name, "PASS"
    except Exception as error:  # noqa: BLE001
        cursor.execute("ROLLBACK TO SAVEPOINT expectation")
        return name, f"FAIL wrong error {type(error).__name__}: {str(error)[:120]}"
    cursor.execute("ROLLBACK TO SAVEPOINT expectation")
    return name, "FAIL statement was accepted"


def _accept(cursor, name: str, statement: str, parameters=()) -> tuple[str, str]:
    cursor.execute("SAVEPOINT acceptance")
    try:
        cursor.execute(statement, parameters)
    except Exception as error:  # noqa: BLE001
        cursor.execute("ROLLBACK TO SAVEPOINT acceptance")
        return name, f"FAIL {type(error).__name__}: {str(error)[:160]}"
    cursor.execute("RELEASE SAVEPOINT acceptance")
    return name, "PASS"


def catalog_checks(cursor) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    cursor.execute(
        "SELECT n.nspname || '.' || c.relname, c.relkind FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('ref', 'rt') AND c.relkind IN ('r', 'p') AND NOT c.relispartition"
    )
    live = {name: kind for name, kind in cursor.fetchall()}
    registry = set(RELATIONS) - set(VIEWS)
    missing = sorted(registry - set(live))
    extra = sorted(set(live) - registry)
    results.append(("registry_matches_catalog", "PASS" if not missing and not extra
                    else f"FAIL missing={missing} extra={extra}"))
    cursor.execute(
        "SELECT n.nspname || '.' || c.relname FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('ref', 'rt') AND c.relkind = 'v'"
    )
    live_views = {row[0] for row in cursor.fetchall()}
    results.append((
        "views_match_registry",
        "PASS" if live_views == set(VIEWS)
        else (
            f"FAIL missing={sorted(set(VIEWS) - live_views)} "
            f"extra={sorted(live_views - set(VIEWS))}"
        ),
    ))
    live_partitioned = sorted(name for name, kind in live.items() if kind == "p")
    results.append(("partitioned_relations_match", "PASS" if live_partitioned == list(PARTITIONED_RELATIONS)
                    else f"FAIL live={live_partitioned} registry={list(PARTITIONED_RELATIONS)}"))
    cursor.execute("SELECT relation_name, scheme, retention_class, window_kind, window_value "
                   "FROM rt.partition_policies")
    policies = {row[0]: tuple(row[1:]) for row in cursor.fetchall()}
    results.append(("partition_policies_match_registry", "PASS" if policies == PARTITION_POLICY_SEEDS
                    else f"FAIL {sorted(set(policies) ^ set(PARTITION_POLICY_SEEDS))}"))
    cursor.execute(
        "SELECT p.relname FROM pg_class p JOIN pg_namespace n ON n.oid = p.relnamespace "
        "WHERE n.nspname = 'rt' AND p.relkind = 'p'")
    empty = []
    for (name,) in cursor.fetchall():
        cursor.execute("SELECT count(*) FROM pg_inherits WHERE inhparent = %s::regclass", (f"rt.{name}",))
        if cursor.fetchone()[0] == 0:
            empty.append(name)
    results.append(("every_partitioned_relation_has_partitions", "PASS" if not empty else f"FAIL {empty}"))
    cursor.execute(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'rt' AND c.relispartition AND c.relname LIKE '%default%'")
    results.append(("no_default_partitions", "PASS" if cursor.fetchone()[0] == 0 else "FAIL"))
    cursor.execute("SELECT policy FROM rt.database_security_policies WHERE policy_version = %s",
                   (SECURITY_POLICY_VERSION,))
    row = cursor.fetchone()
    results.append(("security_policy_matches_source", "PASS" if row and row[0] == security_policy()
                    else "FAIL policy row differs from relation_registry.security_policy()"))
    cursor.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('ref','rt') AND c.relkind IN ('r','p') AND NOT c.relispartition "
        "AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) "
        "AND c.relname NOT IN ('database_identity')")
    keyless = [row[0] for row in cursor.fetchall()]
    results.append(("every_table_has_primary_key", "PASS" if not keyless else f"FAIL {keyless}"))
    cursor.execute(
        "SELECT confdeltype FROM pg_constraint WHERE conrelid = 'rt.job_barrier_members'::regclass "
        "AND contype = 'f' AND confrelid = 'rt.materialization_jobs'::regclass")
    action = cursor.fetchone()
    results.append(("job_prune_clears_barrier_member_job_reference", "PASS" if action and action[0] == "n"
                    else f"FAIL on-delete action {action}"))
    cursor.execute("SELECT relation_name, requires_archive, archive_kinds FROM rt.partition_policies "
                   "WHERE retention_class = 'ARCHIVE_FIRST' ORDER BY relation_name")
    archive_rows = cursor.fetchall()
    results.append(("archive_first_relations_name_their_required_archive_kinds", "PASS" if archive_rows == [
        ("rt.option_flow_clusters", True, ["OPTION_FLOW_CLUSTERS"]),
        ("rt.option_flow_events", True, ["OPTION_FLOW_EVENTS"])] else f"FAIL {archive_rows}"))
    cursor.execute("SELECT window_value FROM rt.partition_policies WHERE relation_name = 'rt.portal_snapshot_kept'")
    results.append(("kept_portal_snapshots_retain_the_26_session_strategy_window",
                    "PASS" if cursor.fetchone()[0] == 26 else "FAIL"))
    cursor.execute("SELECT count(*), count(*) FILTER (WHERE status = 'OK') FROM rt.partition_health(CURRENT_DATE)")
    total, healthy = cursor.fetchone()
    results.append(("partition_health_is_ok_after_provisioning",
                    "PASS" if total == len(PARTITION_POLICY_SEEDS) and healthy == total else f"FAIL {healthy}/{total}"))
    cursor.execute("SELECT count(*) FILTER (WHERE status = 'ALARM') FROM rt.partition_health(CURRENT_DATE + 30)")
    results.append(("partition_health_alarms_when_creation_is_missed",
                    "PASS" if cursor.fetchone()[0] == len(PARTITION_POLICY_SEEDS) else "FAIL"))
    return results


def deferred_phase_checks(cursor, role_prefix: str) -> list[tuple[str, str]]:
    """Structure-phase checks for the deferred security state. They need no database role.

    Insertions here are rolled back by the caller, so the ledger is DEFERRED again afterwards.
    """
    results: list[tuple[str, str]] = []
    cursor.execute("SELECT count(*) FROM pg_roles WHERE starts_with(rolname, %s)", (role_prefix + "_",))
    results.append(("structure_phase_runs_without_roles",
                    "PASS" if cursor.fetchone()[0] == 0 else "FAIL roles exist before the security phase"))
    cursor.execute(
        "SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, "
        "LATERAL aclexplode(c.relacl) a WHERE n.nspname IN ('ref','rt') AND a.grantee <> c.relowner) + "
        "(SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace, "
        "LATERAL aclexplode(p.proacl) a WHERE n.nspname = 'rt' AND a.grantee <> p.proowner)")
    extra = cursor.fetchone()[0]
    results.append(("deferred_baseline_grants_nothing_beyond_the_owner",
                    "PASS" if extra == 0 else f"FAIL {extra} non-owner privileges in the structural baseline"))
    cursor.execute("SELECT phase FROM rt.security_phase_events ORDER BY event_id")
    ledger = [row[0] for row in cursor.fetchall()]
    results.append(("security_phase_ledger_starts_deferred",
                    "PASS" if ledger == ["DEFERRED"] else f"FAIL ledger={ledger}"))
    insert = ("INSERT INTO rt.security_phase_events (policy_version, policy_sha256, phase) "
              "SELECT policy_version, policy_sha256, %s FROM rt.database_security_policies")
    results.append(_expect(cursor, "ledger_rejects_audited_before_applied", errors.RaiseException,
                           insert, ("AUDITED",)))
    results.append(_expect(cursor, "ledger_rejects_a_second_deferred", errors.RaiseException,
                           insert, ("DEFERRED",)))
    results.append(_expect(
        cursor, "ledger_rejects_a_policy_hash_that_does_not_match", errors.ForeignKeyViolation,
        "INSERT INTO rt.security_phase_events (policy_version, policy_sha256, phase) "
        "VALUES (%s, %s, 'APPLIED')", (SECURITY_POLICY_VERSION, HASH)))
    results.append(_accept(cursor, "ledger_accepts_applied_after_deferred", insert, ("APPLIED",)))
    results.append(_expect(cursor, "ledger_cannot_return_to_deferred", errors.RaiseException,
                           insert, ("DEFERRED",)))
    results.append(_accept(cursor, "ledger_accepts_audited_after_applied", insert, ("AUDITED",)))
    results.append(_accept(cursor, "ledger_accepts_reapply_after_audit", insert, ("APPLIED",)))
    results.append(_expect(cursor, "ledger_rows_are_immutable", errors.RaiseException,
                           "UPDATE rt.security_phase_events SET phase = 'DEFERRED'"))
    return results


def _base_rows(cursor) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    profile, manifest, cohort = uuid4(), uuid4(), uuid4()
    cursor.execute(
        "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
        "capabilities_sha256, universe_sha256, checked_at, expires_at, status, profile_sha256, "
        "provider_tier, security_policy_sha256, preflight_mode, provider_requests, action_authorized) "
        "VALUES (%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,'MASSIVE_DEVELOPER',%s,'PASSIVE',false,false)",
        (str(profile), HASH, HASH, HASH, HASH, now, now + timedelta(hours=1), HASH, HASH))
    cursor.execute(
        "INSERT INTO rt.subscription_manifests (manifest_id, manifest_sha256, universe_sha256, session_date, "
        "effective_at, profile_id, manifest_kind, stock_count, option_underlying_count, member_count, "
        "allocation_policy_sha256) VALUES (%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',400,50,400,%s)",
        (str(manifest), HASH, HASH, now.date(), now, str(profile), HASH))
    cursor.execute(
        "INSERT INTO rt.cohorts (cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
        "model_bundle_sha256, valuation_sha256, effective_at, mode) VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW')",
        (str(cohort), HASH, str(manifest), HASH, HASH, HASH, now))
    return {"now": now, "profile": profile, "manifest": manifest, "cohort": cohort}


def behavior_checks(cursor) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    base = _base_rows(cursor)
    cohort, now = str(base["cohort"]), base["now"]
    results.append(("manifest_accepts_am_only_400_stocks_and_50_underlyers", "PASS"))
    results.append(_expect(
        cursor, "manifest_rejects_fewer_members_than_stocks", errors.CheckViolation,
        "INSERT INTO rt.subscription_manifests (manifest_id, manifest_sha256, universe_sha256, session_date, "
        "effective_at, profile_id, manifest_kind, stock_count, option_underlying_count, member_count, "
        "allocation_policy_sha256) VALUES (%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',400,50,399,%s)",
        (str(uuid4()), "1" * 64, HASH, now.date(), now, str(base["profile"]), HASH)))
    # Slot sets: cap of 1,000 option quote contracts per version.
    results.append(_accept(
        cursor, "slot_set_accepts_600_primary_400_reserve",
        "INSERT INTO rt.slot_set_versions (cohort_id, version, kind, effective_at, policy_sha256, member_sha256, "
        "primary_contract_count, reserve_contract_count) VALUES (%s,1,'PRE_SESSION',%s,%s,%s,600,400)",
        (cohort, now, HASH, HASH)))
    results.append(_expect(
        cursor, "slot_set_rejects_more_than_1000_contracts", errors.CheckViolation,
        "INSERT INTO rt.slot_set_versions (cohort_id, version, kind, effective_at, policy_sha256, member_sha256, "
        "primary_contract_count, reserve_contract_count) VALUES (%s,2,'REFRESH',%s,%s,%s,600,401)",
        (cohort, now, HASH, HASH)))
    # Stage independence (RT-01).
    results.append(_expect(
        cursor, "options_stage_cannot_be_prerequisite_of_equity_stage", errors.RaiseException,
        "INSERT INTO rt.stage_catalog (stage_id, version, domain, prerequisites, schedule_kind) "
        "VALUES ('BAD_EQUITY', 'stages_v1', 'equity', ARRAY['OPTIONS_CYCLE'], 'WINDOW')"))
    results.append(_expect(
        cursor, "stage_catalog_is_immutable", errors.RaiseException,
        "UPDATE rt.stage_catalog SET schedule_kind = 'WINDOW' WHERE stage_id = 'REFERENCE'"))
    plan, window = str(uuid4()), str(uuid4())
    cursor.execute(
        "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, enabled_stages, "
        "config_sha256, deadline, scope_reference) VALUES (%s,%s,%s,'stages_v1',ARRAY['EQUITY_INGEST','PORTAL_PUBLISH'],"
        "%s,%s,'fixture scope')", (plan, cohort, now.date(), HASH, now + timedelta(hours=8)))
    results.append(_expect(
        cursor, "service_plan_rejects_unknown_stage", errors.RaiseException,
        "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, enabled_stages, "
        "config_sha256, deadline, scope_reference) VALUES (%s,%s,%s,'stages_v1',ARRAY['NO_SUCH_STAGE'],%s,%s,'x')",
        (str(uuid4()), cohort, now.date(), "2" * 64, now + timedelta(hours=8))))
    cursor.execute(
        "INSERT INTO rt.window_runs (window_id, plan_id, cohort_id, provider_tier, window_key, release_at, cutoff, "
        "deadline) VALUES (%s,%s,%s,'MASSIVE_DEVELOPER','w1',%s,%s,%s)",
        (window, plan, cohort, now, now - timedelta(seconds=90), now + timedelta(seconds=90)))

    def stage(stage_id, state, blocked_by=None, reasons=("FIXTURE",), attempt=1):
        return (str(uuid4()), window, stage_id, attempt, state, blocked_by, list(reasons) if state != "COMPLETE" else [],
                now if state == "COMPLETE" else None)

    insert_stage = (
        "INSERT INTO rt.stage_runs (stage_run_id, window_id, stage_id, stage_version, attempt, state, "
        "blocked_by_stage, reason_codes, finished_at) VALUES (%s,%s,%s,'stages_v1',%s,%s,%s,%s,%s)")
    results.append(_accept(cursor, "reference_and_equity_stages_complete", insert_stage, stage("REFERENCE", "COMPLETE")))
    results.append(_accept(cursor, "equity_ingest_complete_while_options_unadmitted", insert_stage,
                           stage("EQUITY_INGEST", "COMPLETE")))
    results.append(_expect(
        cursor, "options_cycle_cannot_be_blocked_before_admission_fails", errors.RaiseException,
        insert_stage, stage("OPTIONS_CYCLE", "BLOCKED_PREREQ", "OPTIONS_ADMISSION")))
    results.append(_accept(cursor, "options_admission_blocked_by_policy", insert_stage,
                           stage("OPTIONS_ADMISSION", "BLOCKED_POLICY", reasons=("REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE",))))
    results.append(_accept(cursor, "options_cycle_blocked_by_its_admission", insert_stage,
                           stage("OPTIONS_CYCLE", "BLOCKED_PREREQ", "OPTIONS_ADMISSION")))
    results.append(_expect(
        cursor, "portal_cannot_be_blocked_by_options_stage", errors.RaiseException,
        insert_stage, stage("PORTAL_PUBLISH", "BLOCKED_PREREQ", "OPTIONS_CYCLE")))
    results.append(_accept(cursor, "portal_publish_runs_while_options_blocked", insert_stage,
                           stage("PORTAL_PUBLISH", "COMPLETE")))
    # DATA-01 honest admissions.
    results.append(_expect(
        cursor, "blocked_admission_requires_reason", errors.CheckViolation,
        "INSERT INTO rt.option_strategy_admissions (admission_id, cohort_id, session_date, policy_sha256, underlyer, "
        "target_dte, side, required_depth, qualifying_count, status) VALUES (%s,%s,%s,%s,'AMZN',21,'PUT',2,1,'BLOCKED')",
        (str(uuid4()), cohort, now.date(), HASH)))
    results.append(_accept(
        cursor, "blocked_admission_keeps_counts",
        "INSERT INTO rt.option_strategy_admissions (admission_id, cohort_id, session_date, policy_sha256, underlyer, "
        "target_dte, side, required_depth, qualifying_count, status, reason_codes) "
        "VALUES (%s,%s,%s,%s,'AMZN',21,'PUT',2,1,'BLOCKED',ARRAY['SIDE_DEPTH_LIMIT'])",
        (str(uuid4()), cohort, now.date(), HASH)))
    results.append(_expect(
        cursor, "ready_admission_cannot_claim_missing_depth", errors.CheckViolation,
        "INSERT INTO rt.option_strategy_admissions (admission_id, cohort_id, session_date, policy_sha256, underlyer, "
        "target_dte, side, required_depth, qualifying_count, status) VALUES (%s,%s,%s,%s,'IWM',21,'PUT',2,1,'READY')",
        (str(uuid4()), cohort, now.date(), "3" * 64)))
    # Options flow: quote causality, honest opening-flow labelling, idempotency, independence.
    results.append(_accept(cursor, "options_flow_runs_while_options_admission_is_blocked", insert_stage,
                           stage("OPTIONS_FLOW", "COMPLETE")))
    results.append(_expect(
        cursor, "options_flow_is_not_blocked_by_options_admission", errors.RaiseException,
        insert_stage, stage("OPTIONS_FLOW", "BLOCKED_PREREQ", "OPTIONS_ADMISSION", attempt=2)))
    flow_columns = (
        "event_id, event_time, cohort_id, session_date, provider_tier, contract_ticker, underlyer, source_seq, "
        "exchange_id, price, size, premium, conditions, condition_semantics_version, trade_treatment, side_class, "
        "side_confidence, quote_bid, quote_ask, quote_bid_size, quote_ask_size, quote_time, quote_seq, "
        "cumulative_volume_before, cumulative_volume_after, volume_baseline_kind, prior_open_interest, "
        "open_interest_as_of, volume_exceeds_prior_oi, opening_inference, opening_reasons, spread_position, "
        "opening_score")
    flow_insert = f"INSERT INTO rt.option_flow_events ({flow_columns}) VALUES ({','.join(['%s'] * 33)})"
    trade_time = now - timedelta(seconds=5)

    def flow(seq=1, side="AT_OR_ABOVE_ASK", confidence="HIGH", quote_time=None, exceeds=False, oi=None,
             baseline="SESSION_COMPLETE", inference="NOT_ASSESSED", after=700, reasons=None, spread=None, score=None):
        quote_time = trade_time - timedelta(milliseconds=40) if quote_time is None else quote_time
        has_quote = side in ("AT_OR_ABOVE_ASK", "AT_OR_BELOW_BID", "INSIDE_SPREAD")
        reasons = ([] if inference == "NOT_ASSESSED" else ["VOLUME_EXCEEDS_PRIOR_OI"]) if reasons is None else reasons
        return (str(uuid4()), trade_time, cohort, trade_time.date(), "MASSIVE_ADVANCED", "O:SPY261218C00650000", "SPY",
                seq, 302, 1.25, 150, 18750, [227], "massive_options_conditions_v2", "INCLUDED", side, confidence,
                1.20 if has_quote else None, 1.25 if has_quote else None, 20 if has_quote else None,
                30 if has_quote else None, quote_time if has_quote else None, 7 if has_quote else None,
                after - 150, after, baseline, oi, trade_time.date() - timedelta(days=1) if oi else None,
                exceeds, inference, reasons, spread, score)

    results.append(_accept(cursor, "flow_event_accepts_aggressive_print_with_causal_quote", flow_insert, flow()))
    results.append(_expect(cursor, "flow_event_rejects_quote_after_the_trade", errors.CheckViolation, flow_insert,
                           flow(seq=2, quote_time=trade_time + timedelta(milliseconds=5))))
    results.append(_expect(cursor, "flow_event_rejects_aggression_without_a_quote", errors.CheckViolation, flow_insert,
                           flow(seq=3, side="NO_QUOTE", confidence="NONE")[:15] + ("AT_OR_ABOVE_ASK", "LOW")
                           + flow(seq=3, side="NO_QUOTE", confidence="NONE")[17:]))
    results.append(_accept(cursor, "flow_event_without_quote_is_labelled_no_quote", flow_insert,
                           flow(seq=4, side="NO_QUOTE", confidence="NONE")))
    results.append(_expect(cursor, "duplicate_print_is_idempotent_by_contract_sequence_and_time",
                           errors.UniqueViolation, flow_insert, flow(seq=1)))
    results.append(_expect(cursor, "volume_over_oi_flag_requires_prior_oi", errors.CheckViolation, flow_insert,
                           flow(seq=5, exceeds=True, oi=None)))
    results.append(_expect(cursor, "volume_over_oi_flag_requires_a_complete_baseline", errors.CheckViolation,
                           flow_insert, flow(seq=6, exceeds=True, oi=500, baseline="WARMING", after=800)))
    results.append(_expect(cursor, "volume_over_oi_flag_requires_volume_actually_above_oi", errors.CheckViolation,
                           flow_insert, flow(seq=7, exceeds=True, oi=900, after=800)))
    results.append(_accept(cursor, "volume_over_oi_with_complete_baseline_is_accepted", flow_insert,
                           flow(seq=8, exceeds=True, oi=500, after=800)))
    results.append(_expect(cursor, "likely_opening_requires_the_factual_volume_over_oi_flag", errors.CheckViolation,
                           flow_insert, flow(seq=9, inference="LIKELY_OPENING")))
    results.append(_accept(cursor, "likely_opening_is_accepted_with_the_factual_flag", flow_insert,
                           flow(seq=10, exceeds=True, oi=500, after=800, inference="LIKELY_OPENING", score=0.7)))
    results.append(_expect(cursor, "assessed_opening_inference_requires_reasons", errors.CheckViolation, flow_insert,
                           flow(seq=11, exceeds=True, oi=500, after=800, inference="LIKELY_OPENING", reasons=[])))
    results.append(_expect(cursor, "opening_score_requires_an_assessed_inference", errors.CheckViolation, flow_insert,
                           flow(seq=12, score=0.5)))
    results.append(_accept(cursor, "inside_spread_print_stores_its_spread_position", flow_insert,
                           flow(seq=13, side="INSIDE_SPREAD", confidence="MEDIUM", spread=0.4)))
    results.append(_expect(cursor, "spread_position_only_for_an_inside_spread_print", errors.CheckViolation,
                           flow_insert, flow(seq=14, spread=0.4)))
    results.append(_accept(cursor, "quote_list_miss_is_labelled_aggression_unavailable", flow_insert,
                           flow(seq=15, side="AGGRESSION_UNAVAILABLE_NO_QUOTE", confidence="NONE")))
    cluster = (
        "INSERT INTO rt.option_flow_clusters (cluster_id, revision, session_date, cohort_id, provider_tier, "
        "contract_ticker, underlyer, window_seconds, window_start, window_end, side_class, print_count, total_size, "
        "total_premium, distinct_exchanges, first_event_time, last_event_time, status, policy_sha256) VALUES "
        "(%s,%s,%s,%s,'MASSIVE_ADVANCED','O:SPY261218C00650000','SPY',300,%s,%s,'AT_OR_ABOVE_ASK',%s,500,62500,%s,%s,%s,"
        "'FINAL',%s)")
    start = trade_time - timedelta(minutes=2)
    cluster_id = str(uuid4())
    results.append(_accept(cursor, "flow_cluster_accepts_a_consistent_window", cluster,
                           (cluster_id, 1, trade_time.date(), cohort, start, start + timedelta(seconds=300), 4, 2,
                            start + timedelta(seconds=1), start + timedelta(seconds=200), HASH)))
    results.append(_expect(cursor, "flow_cluster_rejects_inconsistent_window_length", errors.CheckViolation, cluster,
                           (str(uuid4()), 1, trade_time.date(), cohort, start, start + timedelta(seconds=299), 4, 2,
                            start + timedelta(seconds=1), start + timedelta(seconds=200), HASH)))
    results.append(_expect(cursor, "flow_cluster_rejects_more_exchanges_than_prints", errors.CheckViolation, cluster,
                           (str(uuid4()), 1, trade_time.date(), cohort, start, start + timedelta(seconds=300), 2, 3,
                            start + timedelta(seconds=1), start + timedelta(seconds=200), HASH)))
    results.append(_expect(cursor, "flow_cluster_revisions_are_immutable", errors.RaiseException,
                           "UPDATE rt.option_flow_clusters SET status = 'PROVISIONAL' WHERE cluster_id = %s",
                           (cluster_id,)))
    retraction = (
        "INSERT INTO rt.option_flow_clusters (cluster_id, revision, session_date, cohort_id, provider_tier, "
        "contract_ticker, underlyer, window_seconds, window_start, window_end, side_class, print_count, total_size, "
        "total_premium, distinct_exchanges, first_event_time, last_event_time, status, policy_sha256) VALUES "
        "(%s,%s,%s,%s,'MASSIVE_ADVANCED','O:SPY261218C00650000','SPY',300,%s,%s,'AT_OR_ABOVE_ASK',%s,%s,0,%s,%s,%s,%s,%s)")

    def retract(revision=2, status="RETRACTED", prints=0, size=0, exchanges=0):
        return (cluster_id, revision, trade_time.date(), cohort, start, start + timedelta(seconds=300), prints, size,
                exchanges, start + timedelta(seconds=1), start + timedelta(seconds=200), status, HASH)

    results.append(_accept(cursor, "all_cancelled_cluster_is_stored_as_a_retracted_revision", retraction, retract()))
    results.append(_expect(cursor, "retraction_must_supersede_an_earlier_revision", errors.CheckViolation, retraction,
                           (str(uuid4()),) + retract(revision=1)[1:]))
    results.append(_expect(cursor, "retracted_cluster_cannot_carry_prints", errors.CheckViolation, retraction,
                           retract(revision=3, prints=1, size=1, exchanges=1)))
    results.append(_expect(cursor, "live_cluster_cannot_have_zero_prints", errors.CheckViolation, retraction,
                           retract(revision=4, status="FINAL")))
    # A cancel is a new revision of the same print; a provider that reuses the sequence and time cannot collide.
    original = flow(seq=20)
    results.append(_accept(cursor, "flow_event_original_print", flow_insert, original))
    revision_insert = (f"INSERT INTO rt.option_flow_events ({flow_columns}, revision, supersedes_event_id) "
                       f"VALUES ({','.join(['%s'] * 35)})")
    cancel = flow(seq=20)[1:] + (2, original[0])
    results.append(_accept(cursor, "cancel_revision_may_reuse_the_original_sequence_and_time", revision_insert,
                           (str(uuid4()),) + cancel))
    results.append(_expect(cursor, "cancel_revision_must_name_the_print_it_supersedes", errors.CheckViolation,
                           revision_insert, (str(uuid4()),) + flow(seq=20)[1:] + (2, None)))
    results.append(_expect(cursor, "replayed_cancel_revision_is_idempotent_by_key", errors.UniqueViolation,
                           revision_insert, (str(uuid4()),) + cancel))
    results.append(_accept(
        cursor, "option_flow_archive_kinds_are_recorded",
        "INSERT INTO rt.session_archives (archive_id, session_date, kind, uri, sha256, row_count, schema_version, "
        "build_id, verified_at) VALUES (%s,%s,'OPTION_FLOW_EVENTS','file:///archive/fixture',%s,0,'v1','fixture',now())",
        (str(uuid4()), trade_time.date(), HASH)))
    checkpoint = ("INSERT INTO rt.option_volume_checkpoints (cohort_id, session_date, contract_ticker, "
                  "cumulative_volume, last_source_seq, baseline_kind, snapshot_volume, snapshot_watermark_seq) "
                  "VALUES (%s,%s,'O:SPY261218C00650000',900,10,%s,%s,%s)")
    results.append(_expect(cursor, "reconciled_volume_requires_snapshot_volume_and_watermark", errors.CheckViolation,
                           checkpoint, (cohort, now.date(), "SNAPSHOT_RECONCILED", None, None)))
    results.append(_accept(cursor, "reconciled_volume_with_watermark_is_accepted", checkpoint,
                           (cohort, now.date(), "SNAPSHOT_RECONCILED", 850, 9)))
    results.append(_expect(
        cursor, "reviewed_condition_cannot_be_included_and_a_cancel", errors.CheckViolation,
        "INSERT INTO ref.option_condition_semantics (semantics_version, condition_id, treatment, volume_eligible, "
        "is_cancel, source_payload_sha256, reviewed_at) VALUES ('massive_options_conditions_v2', 201, 'INCLUDED', "
        "true, true, %s, now())", (HASH,)))
    # RT-05 diagnostics.
    diagnostic = (
        "INSERT INTO rt.normalization_diagnostics (diagnostic_id, cohort_id, underlyer, bar_count, pages_fetched, "
        "pagination_complete, bar_acquisition, price_basis, raw_rows, usable_mark_count, candidates, rejection_counts) "
        "VALUES (%s,%s,'SPY',%s,3,%s,'FETCHED','RAW_FINALIZED_MINUTE_CLOSE_V1',4132,4132,0,%s::jsonb)")
    results.append(_expect(cursor, "zero_candidates_require_rejection_reasons", errors.CheckViolation,
                           diagnostic, (str(uuid4()), cohort, 380, True, "{}")))
    results.append(_accept(cursor, "zero_candidates_with_reasons_accepted", diagnostic,
                           (str(uuid4()), cohort, 380, True, json.dumps({"OPTION_SPOT_SKEW": 4132}))))
    # RT-03 completeness.
    results.append(_expect(
        cursor, "incomplete_request_requires_failure_code", errors.CheckViolation,
        "INSERT INTO rt.provider_request_ledger (request_id, account_alias, lane, endpoint_class, request_sha256, "
        "reserved_at, completeness) VALUES (%s,'fixture','OPTIONS_ALIGNMENT','aggs_minute',%s,%s,'INCOMPLETE')",
        (str(uuid4()), HASH, now)))
    results.append(_expect(
        cursor, "price_basis_values_are_closed", errors.CheckViolation,
        "INSERT INTO rt.provider_request_ledger (request_id, account_alias, lane, endpoint_class, request_sha256, "
        "reserved_at, price_basis) VALUES (%s,'fixture','OPTIONS_ALIGNMENT','aggs_minute',%s,%s,'ADJUSTED_ANYTHING')",
        (str(uuid4()), HASH, now)))
    # Catalog and partition behavior.
    cursor.execute("INSERT INTO rt.raw_payloads (payload_id, arrived_at, content_sha256, byte_length, chunk_count) "
                   "VALUES (%s, now(), %s, 10, 1)", (str(uuid4()), HASH))
    results.append(("raw_payload_lands_in_precreated_partition", "PASS"))
    results.append(_expect(
        cursor, "write_outside_precreated_range_is_rejected", errors.CheckViolation,
        "INSERT INTO rt.raw_payloads (payload_id, arrived_at, content_sha256, byte_length, chunk_count) "
        "VALUES (%s, '2000-01-01', %s, 10, 1)", (str(uuid4()), HASH)))
    results.append(_expect(
        cursor, "partitioned_body_rows_are_immutable", errors.RaiseException,
        "UPDATE rt.raw_payloads SET byte_length = 11"))
    return results


def detach_check(settings: dict[str, str], database: str) -> list[tuple[str, str]]:
    """DETACH ... CONCURRENTLY must work (no default partition) outside a transaction."""
    with closing(_connect(settings, database)) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                "WHERE i.inhparent = 'rt.raw_payload_chunks'::regclass ORDER BY c.relname LIMIT 1")
            child = cursor.fetchone()[0]
            try:
                cursor.execute(sql.SQL("ALTER TABLE rt.raw_payload_chunks DETACH PARTITION rt.{} CONCURRENTLY")
                               .format(sql.Identifier(child)))
                cursor.execute(sql.SQL("DROP TABLE rt.{}").format(sql.Identifier(child)))
            except Exception as error:  # noqa: BLE001
                return [("detach_concurrently_then_drop", f"FAIL {type(error).__name__}: {str(error)[:160]}")]
    return [("detach_concurrently_then_drop", "PASS")]


def validation_summary(results: dict[str, list[tuple[str, str]]]) -> tuple[dict[str, object], bool]:
    summary = {
        phase: {
            "checks": len(items),
            "failures": [item for item in items if not item[1].startswith("PASS")],
        }
        for phase, items in results.items()
    }
    return summary, any(entry["failures"] for entry in summary.values())


def validate_existing_database(
    settings: dict[str, str],
    database: str,
    security_mode: str,
) -> int:
    phases = v2_provisioning_phases(security_mode)
    roles = build_role_names_v2(
        EXPECTED_V2_DATABASE_NAME
        if database == EXPECTED_V2_DATABASE_NAME
        else "test_rt_" + database[len("test_realtime_v2_"):]
    )
    with closing(_connect(settings, "postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=%s)", (database,))
            if not cursor.fetchone()[0]:
                raise RuntimeError("target database does not exist")
    results: dict[str, list[tuple[str, str]]] = {phase: [] for phase in phases}
    with closing(_connect(settings, database)) as connection:
        with connection.cursor() as cursor:
            results[PHASE_STRUCTURE].extend(catalog_checks(cursor))
            results[PHASE_STRUCTURE].extend(behavior_checks(cursor))
        connection.rollback()
    results[PHASE_STRUCTURE].extend(detach_check(settings, database))
    if PHASE_SECURITY in phases:
        with closing(_connect(settings, database)) as connection:
            audit = audit_database_security_v2(connection, roles)
            results[PHASE_SECURITY].extend(audit)
            phase = current_security_phase_v2(connection)
            results[PHASE_SECURITY].append((
                "security_phase_ledger_ends_audited",
                "PASS" if phase == "AUDITED" else f"FAIL phase={phase}",
            ))
            connection.rollback()
    summary, failed = validation_summary(results)
    print(json.dumps({
        "status": "FAILED" if failed else "VERIFIED",
        "mode": "VALIDATE_EXISTING",
        "security_mode": security_mode,
        "migrations": len(load_realtime_migrations(V2_MIGRATIONS_DIR)),
        "phases": summary,
    }, indent=2))
    return 1 if failed else 0


def main() -> int:
    args = parse_args()
    migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    phases = v2_provisioning_phases(args.security_mode)
    if not args.execute:
        print(json.dumps({"database": args.database, "mode": "PLAN", "security_mode": args.security_mode,
                          "phases": list(phases), "migrations": [m.version for m in migrations],
                          "relations": len(RELATIONS)}))
        print("No database changes made. Add --execute.")
        return 0
    settings = _settings()
    if args.validate_existing:
        return validate_existing_database(settings, args.database, args.security_mode)
    created = False
    roles = build_role_names_v2("test_rt_" + args.database[len("test_realtime_v2_"):])
    roles_created = False
    try:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=%s)", (args.database,))
                if cursor.fetchone()[0]:
                    raise RuntimeError("disposable database already exists; refusing ownership")
                cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'")
                               .format(sql.Identifier(args.database)))
        created = True
        results: dict[str, list[tuple[str, str]]] = {phase: [] for phase in phases}
        with closing(_connect(settings, args.database)) as connection:
            for migration in migrations:
                with connection.cursor() as cursor:
                    cursor.execute(migration.sql)
                    cursor.execute("INSERT INTO rt.schema_migrations (version, checksum_sha256) VALUES (%s, %s)",
                                   (migration.version, migration.checksum_sha256))
                connection.commit()
            with connection.cursor() as cursor:
                results[PHASE_STRUCTURE].extend(catalog_checks(cursor))
                results[PHASE_STRUCTURE].extend(behavior_checks(cursor))
                results[PHASE_STRUCTURE].extend(deferred_phase_checks(cursor, roles.prefix))
            connection.rollback()
        results[PHASE_STRUCTURE].extend(detach_check(settings, args.database))
        if PHASE_SECURITY in phases:
            with closing(_connect(settings, args.database)) as connection:
                create_disposable_roles_v2(connection, roles)
                roles_created = True
                connection.commit()
                apply_database_security_v2(connection, roles)
                connection.commit()
                audit = audit_database_security_v2(connection, roles)
                results[PHASE_SECURITY].extend(audit)
                if all(status.startswith("PASS") for _, status in audit):
                    record_security_audit_v2(connection, audit)
                    connection.commit()
                phase = current_security_phase_v2(connection)
                results[PHASE_SECURITY].append(("security_phase_ledger_ends_audited",
                                                "PASS" if phase == "AUDITED" else f"FAIL phase={phase}"))
                connection.rollback()
        summary, failed = validation_summary(results)
        print(json.dumps({"status": "FAILED" if failed else "VERIFIED", "security_mode": args.security_mode,
                          "migrations": len(migrations), "phases": summary}, indent=2))
        return 1 if failed else 0
    finally:
        if created:
            with closing(_connect(settings, "postgres")) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                   "WHERE datname=%s AND pid <> pg_backend_pid()", (args.database,))
                    cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(args.database)))
        if roles_created:
            with closing(_connect(settings, "postgres")) as connection:
                connection.autocommit = True
                drop_disposable_roles_v2(connection, roles)


if __name__ == "__main__":
    raise SystemExit(main())
