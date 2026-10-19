from dataclasses import replace
from datetime import date, datetime, timezone
import json
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from option_flow_support import (
    CALL,
    JOURNAL_GENERATION,
    advanced_contract_scope,
    advanced_policy,
    config,
    run,
    trade,
)
from realtime.advanced_ingress_plan import (
    ADVANCED_OPTIONS_INGRESS_CHANNEL,
    ADVANCED_OPTIONS_INGRESS_FEED,
    AdvancedIngressBudgets,
    build_advanced_stream_authorization,
)
from realtime.advanced_ingress_repository import (
    activate_advanced_runtime_profile_cursor,
)
from realtime.detector_evidence import RealtimeDetectorInput
from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations
from realtime.option_flow.engine import OptionFlowEngine
from realtime.option_source_flow import (
    CANONICAL_OPTION_FLOW_OWNER,
    build_option_source_flow_identity,
    persist_option_flow_policy_cursor,
    persist_option_source_flow_cursor,
)
from realtime.advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
)
from realtime.contracts import canonical_json
from realtime.orchestration_repository import (
    OPTIONS_CYCLE,
    OPTIONS_INPUT_READY,
    OPTIONS_PORTAL_READY,
    build_options_receipt_spec,
)
from realtime.relation_registry import RELATIONS
from realtime.stage_graph import STAGES_V4
from realtime.stage_runners_v2 import unwired_stages


UTC = timezone.utc
SESSION = date(2026, 10, 16)
NOW = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
STREAM = UUID("99999999-9999-9999-9999-999999999999")
SOURCE_COHORT = UUID("88888888-8888-8888-8888-888888888888")
OTHER_COHORT = UUID("77777777-7777-7777-7777-777777777777")
SQL_PATH = (
    Path(V2_MIGRATIONS_DIR)
    / "025_realtime_advanced_options_control_plane.sql"
)
FLOW_SQL_PATH = (
    Path(V2_MIGRATIONS_DIR)
    / "026_advanced_options_flow_evidence.sql"
)
PROVIDER_SQL_PATH = (
    Path(V2_MIGRATIONS_DIR)
    / "027_advanced_options_provider_references.sql"
)
REFERENCE_STATE_SQL_PATH = (
    Path(V2_MIGRATIONS_DIR)
    / "028_advanced_options_reference_state.sql"
)
ROLLING_STATE_SQL_PATH = (
    Path(V2_MIGRATIONS_DIR)
    / "029_advanced_options_rolling_checkpoints.sql"
)
PACKAGE_EVIDENCE_SQL_PATH = (
    Path(V2_MIGRATIONS_DIR)
    / "030_advanced_options_flow_package_evidence.sql"
)


def _source_flow(base_config=None, *, generation=JOURNAL_GENERATION):
    base = base_config or config(journal_generation=generation)
    return build_option_source_flow_identity(
        journal_generation=generation,
        stream_session_ids=(STREAM,),
        condition_semantics_sha256=base.semantics.semantics_sha256,
        flow_policy_sha256=base.policy_sha256(),
    )


def test_p2_is_one_forward_migration_with_no_parallel_lifecycle_tables():
    migrations = load_realtime_migrations(Path(V2_MIGRATIONS_DIR))
    assert migrations[-1].version == (
        "030_advanced_options_flow_package_evidence"
    )
    text = SQL_PATH.read_text(encoding="utf-8")
    assert "CREATE TABLE rt.option_source_flows" in text
    for prohibited in (
        "options_scheduler",
        "options_retry",
        "options_jobs",
        "options_releases",
        "options_detector_state",
        "options_archives",
        "options_retention",
    ):
        assert f"CREATE TABLE rt.{prohibited}" not in text


def test_p6d3_migration_has_one_immutable_reference_state_owner():
    text = " ".join(
        REFERENCE_STATE_SQL_PATH.read_text(encoding="utf-8").split()
    )

    for relation in (
        "advanced_options_reference_states",
        "advanced_options_reference_state_members",
        "advanced_options_reference_session_freezes",
        "advanced_options_reference_cycle_bindings",
    ):
        assert f"CREATE TABLE rt.{relation}" in text
        assert f"trg_rt_{relation}_immutable" in text
        assert f"trg_rt_{relation}_no_truncate" in text
    assert "contract_scope_sha256" in text
    assert "state_document jsonb NOT NULL" in text
    assert "member_document jsonb NOT NULL" in text
    assert "CREATE TABLE rt.options_reference" not in text


def test_p6d4_migration_has_one_immutable_checkpoint_owner():
    text = " ".join(
        ROLLING_STATE_SQL_PATH.read_text(encoding="utf-8").split()
    )

    assert (
        "CREATE TABLE rt.advanced_options_rolling_checkpoints"
        in text
    )
    assert (
        "trg_rt_advanced_options_rolling_checkpoints_immutable"
        in text
    )
    assert (
        "trg_rt_advanced_options_rolling_checkpoints_no_truncate"
        in text
    )
    assert "owner_epoch bigint NOT NULL" in text
    assert "committed_offset bigint NOT NULL" in text
    assert "state_document jsonb NOT NULL" in text
    assert "CREATE TABLE rt.underlying_tick" not in text
    assert "CREATE TABLE rt.iv_surface" not in text


def test_p6d5_migration_extends_existing_evidence_owners_only():
    text = " ".join(
        PACKAGE_EVIDENCE_SQL_PATH.read_text(encoding="utf-8").split()
    )

    assert (
        "CREATE TABLE rt.advanced_options_flow_package_evidence"
        in text
    )
    assert "REFERENCES rt.detector_evaluations(evaluation_id)" in text
    assert "REFERENCES rt.option_packages(candidate_id)" in text
    assert "REFERENCES rt.advanced_options_reference_states(state_id)" in text
    assert "REFERENCES rt.advanced_options_rolling_checkpoints(state_id)" in text
    assert (
        "trg_rt_advanced_options_flow_package_evidence_immutable"
        in text
    )
    for prohibited in (
        "advanced_options_events",
        "advanced_options_clusters",
        "advanced_options_packages",
        "advanced_options_releases",
        "advanced_options_current",
    ):
        assert f"CREATE TABLE rt.{prohibited}" not in text


def test_migration_product_scopes_overlay_receipts_slots_and_flow_identity():
    text = " ".join(SQL_PATH.read_text(encoding="utf-8").split())

    assert "(feed = 'STOCKS' AND channel = 'AM')" in text
    assert "(feed = 'OPTIONS' AND channel = 'T,Q')" in text
    assert "channels = ARRAY['T','Q']::text[]" in text
    assert "option slot set must contain at most 1000 exact T/Q contracts" in text
    assert "ALTER COLUMN receipt_kind TYPE varchar(32)" in text
    assert "'OPTIONS_INPUT_READY'" in text
    assert "'OPTIONS_PORTAL_READY'" in text
    assert "ADD COLUMN source_flow_id uuid" in text
    assert "ADD COLUMN lineage_version smallint NOT NULL DEFAULT 1" in text
    assert "uq_rt_option_flow_event_source_policy" in text
    assert "source_flow_id, source_cohort_id" in text
    assert "rt.option_source_flows" in RELATIONS
    assert RELATIONS["rt.option_source_flows"].writes == "APPEND"


def test_migration_026_normalizes_flow_profiles_members_and_scope_identity():
    text = " ".join(FLOW_SQL_PATH.read_text(encoding="utf-8").split())

    for relation in (
        "rt.option_flow_contract_scopes",
        "rt.option_flow_contract_scope_members",
        "rt.option_flow_event_profiles",
        "rt.option_flow_cluster_members",
    ):
        assert f"CREATE TABLE {relation}" in text
        assert relation in RELATIONS
        assert RELATIONS[relation].writes == "APPEND"
    assert "identity_version = 3" in text
    assert "lineage_version = 3" in text
    assert "flow_identity_version = 2" in text
    assert "FOREIGN KEY (event_id, event_time, profile)" in text
    assert "minimum_cumulative_contracts IS NULL" in text
    assert "minimum_cumulative_contracts > 0" in text
    assert "runtime_policy_sha256 = policy_sha256" in text
    assert RELATIONS["rt.option_flow_event_profiles"].partitioned
    assert RELATIONS["rt.option_flow_cluster_members"].partitioned


def test_migration_027_catalogs_policy_and_provider_reference_identity():
    text = " ".join(
        PROVIDER_SQL_PATH.read_text(encoding="utf-8").split()
    )

    assert "CREATE TABLE rt.option_flow_policies" in text
    assert "ADD COLUMN participant_id" in text
    assert "ADD COLUMN operating_mic" in text
    assert "identity_version = 4" in text
    assert "lineage_version = 4" in text
    assert "condition_reference_sha256" in text
    assert "exchange_reference_sha256" in text
    assert RELATIONS["rt.option_flow_policies"].writes == "APPEND"


def test_options_authorization_is_one_exact_tq_product_scope():
    authorization = build_advanced_stream_authorization(
        parent_authorization_id=None,
        allowed_universe_targets=(2,),
        valid_from=SESSION,
        valid_through=SESSION,
        maximum_budgets=AdvancedIngressBudgets(
            1,
            2,
            10_000,
            5,
            50_000,
        ),
        runtime_policy_sha256="a" * 64,
        approval_source="operator",
        approval_reference="advanced-options-p2",
        feed=ADVANCED_OPTIONS_INGRESS_FEED,
        channel=ADVANCED_OPTIONS_INGRESS_CHANNEL,
    )

    assert authorization.feed == "OPTIONS"
    assert authorization.channel == "T,Q"
    with pytest.raises(ValueError, match="provider/feed/channel"):
        replace(authorization, channel="T")


def test_runtime_profile_activation_adapter_is_product_scoped():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (
            "advanced-options-ingress-2-v1",
            2,
            "b" * 64,
            "a" * 64,
            "CANDIDATE",
            None,
            None,
            None,
            "OPTIONS",
            ["T", "Q"],
        ),
        (UUID("55555555-5555-5555-5555-555555555555"),),
    )

    registration = activate_advanced_runtime_profile_cursor(
        cursor,
        "c" * 64,
        stability_evidence_sha256="d" * 64,
        capacity_evidence_sha256="e" * 64,
        approval_reference="advanced-options-p2-profile",
        activated_at=NOW,
    )

    statements = tuple(call.args[0] for call in cursor.execute.call_args_list)
    assert registration.status == "ACTIVE"
    assert any(
        "WHERE universe_target=%s AND feed=%s AND channels=%s" in statement
        for statement in statements
    )
    assert any(
        call.args[1][-2:] == ("OPTIONS", ["T", "Q"])
        for call in cursor.execute.call_args_list
        if call.args[1] is not None and len(call.args[1]) >= 3
    )


def test_options_receipt_adapter_is_product_scoped_and_never_releases():
    receipts = tuple(
        build_options_receipt_spec(
            receipt_kind=kind,
            cutoff=NOW,
            reference_generation=3,
            universe_sha256="a" * 64,
            input_sha256="b" * 64,
            build_id="p2-build",
        )
        for kind in (
            OPTIONS_INPUT_READY,
            OPTIONS_CYCLE,
            OPTIONS_PORTAL_READY,
        )
    )

    assert tuple(receipt.receipt_kind for receipt in receipts) == (
        OPTIONS_INPUT_READY,
        OPTIONS_CYCLE,
        OPTIONS_PORTAL_READY,
    )
    assert all(not receipt.advance_release for receipt in receipts)
    with pytest.raises(ValueError, match="product-scoped"):
        build_options_receipt_spec(
            receipt_kind="INPUT_READY",
            cutoff=NOW,
            reference_generation=3,
            universe_sha256="a" * 64,
            input_sha256="b" * 64,
            build_id="p2-build",
        )


def test_source_flow_identity_and_persistence_are_canonical_and_idempotent():
    source_flow = _source_flow()
    repeated = _source_flow()
    assert source_flow == repeated
    assert source_flow.owner_name == CANONICAL_OPTION_FLOW_OWNER
    assert set(source_flow.lineage_identity) == {
        "contract",
        "journal_generation",
        "stream_session_ids",
        "owner_name",
        "condition_semantics_sha256",
        "flow_policy_sha256",
    }

    cursor = MagicMock()
    cursor.fetchone.return_value = (
        source_flow.source_flow_id,
        source_flow.source_flow_sha256,
        SOURCE_COHORT,
        source_flow.journal_generation,
        list(source_flow.stream_session_ids),
        source_flow.owner_name,
        source_flow.condition_semantics_sha256,
        source_flow.flow_policy_sha256,
        source_flow.identity_version,
        source_flow.advanced_flow_policy_sha256,
        source_flow.contract_scope_sha256,
    )
    persist_option_source_flow_cursor(
        cursor,
        source_flow,
        source_cohort_id=SOURCE_COHORT,
    )

    assert cursor.execute.call_count == 2
    assert all(
        effect not in " ".join(
            str(call.args[0]) for call in cursor.execute.call_args_list
        ).lower()
        for effect in ("release_current", "portal_cohort_current", "stage_runs")
    )


def test_source_flow_v3_persists_exact_contract_scope_before_identity():
    flow_policy = advanced_policy()
    scope = advanced_contract_scope((CALL,))
    base = config(
        advanced_flow_policy=flow_policy,
        contract_scope=scope,
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=JOURNAL_GENERATION,
        stream_session_ids=(STREAM,),
        condition_semantics_sha256=base.semantics.semantics_sha256,
        flow_policy_sha256=base.policy_sha256(),
        advanced_flow_policy_sha256=flow_policy.sha256,
        contract_scope_sha256=scope.sha256,
    )
    scope_values = (
        scope.sha256,
        scope.version,
        scope.session_date,
        scope.manifest_sha256,
        scope.slot_sha256,
        scope.flow_policy_sha256,
    )
    member_values = [
        (
            scope.sha256,
            row.contract_ticker,
            row.underlyer,
            row.expiration,
            row.universe_tier.value,
            row.dte_lane.value,
        )
        for row in scope.contracts
    ]
    source_values = (
        source_flow.source_flow_id,
        source_flow.source_flow_sha256,
        SOURCE_COHORT,
        source_flow.journal_generation,
        list(source_flow.stream_session_ids),
        source_flow.owner_name,
        source_flow.condition_semantics_sha256,
        source_flow.flow_policy_sha256,
        3,
        flow_policy.sha256,
        scope.sha256,
    )
    cursor = MagicMock()
    cursor.fetchone.side_effect = (scope_values, source_values)
    cursor.fetchall.return_value = member_values

    persist_option_source_flow_cursor(
        cursor,
        source_flow,
        source_cohort_id=SOURCE_COHORT,
        contract_scope=scope,
    )

    assert source_flow.identity_version == 3
    assert source_flow.lineage_identity[
        "advanced_flow_policy_sha256"
    ] == flow_policy.sha256
    assert cursor.executemany.call_args.args[1] == member_values
    statements = " ".join(
        call.args[0] for call in cursor.execute.call_args_list
    )
    assert statements.index(
        "INSERT INTO rt.option_flow_contract_scopes"
    ) < statements.index("INSERT INTO rt.option_source_flows")
    event = run(
        OptionFlowEngine(replace(base, source_flow=source_flow)),
        [trade(seq=1, price=5.0, size=500)],
    ).events[0]
    assert event.lineage_version == 3


def test_policy_catalog_adapter_is_immutable_and_source_addressed():
    policy = advanced_policy()
    document = json.loads(canonical_json(policy))
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        policy.sha256,
        policy.version,
        document,
        MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256,
        "backend/realtime/policies/advanced_options_flow_v1.json",
    )

    persist_option_flow_policy_cursor(cursor, policy)

    assert cursor.execute.call_count == 2
    assert "INSERT INTO rt.option_flow_policies" in (
        cursor.execute.call_args_list[0].args[0]
    )


def test_source_flow_v4_binds_all_provider_reference_hashes():
    source = build_option_source_flow_identity(
        journal_generation=JOURNAL_GENERATION,
        stream_session_ids=(STREAM,),
        condition_semantics_sha256="1" * 64,
        flow_policy_sha256="2" * 64,
        advanced_flow_policy_sha256="3" * 64,
        contract_scope_sha256="4" * 64,
        provider_contract_sha256="5" * 64,
        condition_reference_sha256="6" * 64,
        exchange_reference_sha256="7" * 64,
    )

    assert source.identity_version == 4
    assert source.lineage_identity[
        "provider_contract_sha256"
    ] == "5" * 64
    assert source.lineage_identity[
        "condition_reference_sha256"
    ] == "6" * 64
    assert source.lineage_identity[
        "exchange_reference_sha256"
    ] == "7" * 64


def test_v2_flow_ids_are_source_policy_based_not_detector_cohort_based():
    base = config()
    source_flow = _source_flow(base)
    first = OptionFlowEngine(replace(
        base,
        cohort_id=SOURCE_COHORT,
        source_flow=source_flow,
    ))
    shadow = OptionFlowEngine(replace(
        base,
        cohort_id=OTHER_COHORT,
        source_flow=source_flow,
    ))

    first_event = run(first, [trade()]).events[0]
    shadow_event = run(shadow, [trade()]).events[0]
    assert first_event.event_id == shadow_event.event_id
    assert first_event.source_flow_id == source_flow.source_flow_id
    assert first_event.lineage_version == 2
    assert first_event.condition_semantics_sha256 == (
        source_flow.condition_semantics_sha256
    )
    assert first_event.flow_policy_sha256 == source_flow.flow_policy_sha256


def test_source_flow_pins_and_generation_fail_closed():
    base = config()
    source_flow = _source_flow(base)

    with pytest.raises(ValueError, match="journal generation"):
        replace(
            base,
            journal_generation=UUID(
                "66666666-6666-6666-6666-666666666666"
            ),
            source_flow=source_flow,
        )
    with pytest.raises(ValueError, match="condition or policy"):
        replace(
            base,
            source_flow=build_option_source_flow_identity(
                journal_generation=JOURNAL_GENERATION,
                stream_session_ids=(STREAM,),
                condition_semantics_sha256=(
                    base.semantics.semantics_sha256
                ),
                flow_policy_sha256="f" * 64,
            ),
        )


def test_detector_journal_inputs_bind_source_flow_and_source_cohort_together():
    source_flow = _source_flow()
    input_row = RealtimeDetectorInput(
        input_role="FLOW_EVENT",
        input_key="event:1",
        source_kind="JOURNAL_EVENT",
        input_sha256="c" * 64,
        available_at=NOW,
        received_at=NOW,
        retained_input={"source_flow_id": str(source_flow.source_flow_id)},
        journal_generation=source_flow.journal_generation,
        stream_session_id=STREAM,
        source_offset=1,
        source_flow_id=source_flow.source_flow_id,
        source_cohort_id=SOURCE_COHORT,
    )

    assert input_row.source_flow_id == source_flow.source_flow_id
    assert input_row.source_cohort_id == SOURCE_COHORT
    with pytest.raises(ValueError, match="binding must be complete"):
        replace(input_row, source_cohort_id=None)


def test_p4_source_binds_every_catalog_stage_without_configuring_actions():
    assert unwired_stages(frozenset(STAGES_V4)) == frozenset()
