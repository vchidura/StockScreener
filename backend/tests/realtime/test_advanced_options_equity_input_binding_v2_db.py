from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

from realtime.advanced_equity_control_plane import (
    load_advanced_equity_control_plane_plan_cursor,
)
from realtime.advanced_options_control_plane import (
    build_advanced_options_equity_input_binding,
    load_advanced_options_equity_input_binding_cursor,
    persist_advanced_options_equity_input_binding_cursor,
    persist_advanced_options_enrollment_cursor,
)
from realtime.advanced_options_equity_input import (
    validate_advanced_options_equity_dependencies_cursor,
)
from tests.realtime.test_option_slot_sets_v2_db import (
    _owp4_enrollment,
    cursor,
)
from tests.realtime.v2_database import (
    disposable_v2_database,
    requires_v2_database,
)


pytestmark = requires_v2_database


def test_equity_input_binding_persists_exact_completed_source():
    with disposable_v2_database(
        "test_realtime_v2_options_equity_input"
    ) as pool:
        enrollment = _owp4_enrollment(pool)
        equity = enrollment.equity_plan
        options_cutoff = (
            enrollment.control_plane.receipts.options_input_ready.spec.cutoff
        )
        equity_cutoff = options_cutoff - timedelta(minutes=2)
        with cursor(pool) as database_cursor:
            loaded = load_advanced_equity_control_plane_plan_cursor(
                database_cursor,
                equity.cohort_id,
            )
            assert loaded == equity
            plan_id = uuid4()
            window_id = uuid4()
            receipt_id = uuid4()
            database_cursor.execute(
                "INSERT INTO rt.service_plans ("
                "plan_id,cohort_id,session_date,stage_catalog_version,"
                "enabled_stages,config_sha256,deadline,scope_reference"
                ") VALUES (%s,%s,%s,'stages_v3',%s,%s,%s,%s)",
                (
                    str(plan_id),
                    str(equity.cohort_id),
                    equity.session_date,
                    [
                        "EQUITY_INPUT_GATE",
                        "EQUITY_MATERIALIZE",
                        "EQUITY_PROJECTION",
                    ],
                    "d" * 64,
                    options_cutoff + timedelta(hours=1),
                    "P8 Equity input binding test",
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.window_runs ("
                "window_id,plan_id,cohort_id,provider_tier,window_key,"
                "release_at,cutoff,deadline,state) VALUES ("
                "%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'COMPLETE')",
                (
                    str(window_id),
                    str(plan_id),
                    str(equity.cohort_id),
                    f"equity-input-{window_id}",
                    equity_cutoff,
                    equity_cutoff,
                    equity_cutoff + timedelta(minutes=5),
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.cycle_receipts ("
                "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
                "reference_generation,universe_sha256,input_sha256,"
                "build_id) VALUES (%s,%s,%s,'INPUT_READY',%s,1,%s,%s,%s)",
                (
                    str(receipt_id),
                    str(window_id),
                    str(equity.cohort_id),
                    equity_cutoff,
                    equity.universe.sha256,
                    "e" * 64,
                    "advanced-equity-binding-test-v1",
                ),
            )
            binding = build_advanced_options_equity_input_binding(
                target_options_window_id=uuid4(),
                target_cohort_id=enrollment.control_plane.cohort_id,
                source_cohort_id=equity.cohort_id,
                source_cohort_sha256=equity.cohort_sha256,
                source_manifest_id=equity.manifest_id,
                source_manifest_sha256=equity.manifest.sha256,
                source_profile_id=equity.profile_id,
                source_profile_sha256=equity.profile_sha256,
                equity_window_id=window_id,
                equity_receipt_id=receipt_id,
                equity_cutoff=equity_cutoff,
                reference_generation=1,
                universe_sha256=equity.universe.sha256,
                input_sha256="e" * 64,
                build_id="advanced-equity-binding-test-v1",
                maximum_age_seconds=300,
                authorization_reference="p8-equity-input-test",
            )
            readiness = validate_advanced_options_equity_dependencies_cursor(
                database_cursor,
                equity_plan=equity,
                equity_input=binding,
                history_source_cohort_id=uuid4(),
                underlyings=("SPY",),
            )
            assert readiness["status"] == "READY"
            assert readiness["lane_context"]["O1"]["status"] == "AVAILABLE"
            assert readiness["lane_context"]["O3"]["status"] == "BLOCKED"
            assert persist_advanced_options_enrollment_cursor(
                database_cursor,
                enrollment,
                equity_input_binding=binding,
            )
            options_plan_id = uuid4()
            database_cursor.execute(
                "INSERT INTO rt.service_plans ("
                "plan_id,cohort_id,session_date,stage_catalog_version,"
                "enabled_stages,config_sha256,deadline,scope_reference"
                ") VALUES (%s,%s,%s,'stages_v4',%s,%s,%s,%s)",
                (
                    str(options_plan_id),
                    str(enrollment.control_plane.cohort_id),
                    enrollment.control_plane.session_date,
                    ["OPTIONS_INPUT_GATE", "OPTIONS_CYCLE"],
                    "f" * 64,
                    options_cutoff + timedelta(hours=1),
                    "P8 target Options window binding test",
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.window_runs ("
                "window_id,plan_id,cohort_id,provider_tier,window_key,"
                "release_at,cutoff,deadline,state) VALUES ("
                "%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'PENDING')",
                (
                    str(binding.target_options_window_id),
                    str(options_plan_id),
                    str(enrollment.control_plane.cohort_id),
                    f"options-target-{binding.target_options_window_id}",
                    options_cutoff,
                    options_cutoff,
                    options_cutoff + timedelta(minutes=5),
                ),
            )
            database_cursor.execute(
                "SELECT target_options_window_id::text,"
                "source_cohort_id::text,equity_receipt_id::text,"
                "equity_cutoff,binding_sha256 "
                "FROM rt.advanced_options_equity_input_bindings "
                "WHERE target_options_window_id=%s",
                (str(binding.target_options_window_id),),
            )
            assert database_cursor.fetchone() == (
                str(binding.target_options_window_id),
                str(equity.cohort_id),
                str(receipt_id),
                equity_cutoff,
                binding.binding_sha256,
            )
            assert load_advanced_options_equity_input_binding_cursor(
                database_cursor,
                binding.target_options_window_id,
            ) == binding
            second_window_id = uuid4()
            second_cutoff = options_cutoff + timedelta(minutes=1)
            database_cursor.execute(
                "INSERT INTO rt.window_runs ("
                "window_id,plan_id,cohort_id,provider_tier,window_key,"
                "release_at,cutoff,deadline,state) VALUES ("
                "%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'PENDING')",
                (
                    str(second_window_id),
                    str(options_plan_id),
                    str(enrollment.control_plane.cohort_id),
                    f"options-target-{second_window_id}",
                    second_cutoff,
                    second_cutoff,
                    second_cutoff + timedelta(minutes=5),
                ),
            )
            second = build_advanced_options_equity_input_binding(
                target_options_window_id=second_window_id,
                target_cohort_id=binding.target_cohort_id,
                source_cohort_id=binding.source_cohort_id,
                source_cohort_sha256=binding.source_cohort_sha256,
                source_manifest_id=binding.source_manifest_id,
                source_manifest_sha256=binding.source_manifest_sha256,
                source_profile_id=binding.source_profile_id,
                source_profile_sha256=binding.source_profile_sha256,
                equity_window_id=binding.equity_window_id,
                equity_receipt_id=binding.equity_receipt_id,
                equity_cutoff=binding.equity_cutoff,
                reference_generation=binding.reference_generation,
                universe_sha256=binding.universe_sha256,
                input_sha256=binding.input_sha256,
                build_id=binding.build_id,
                maximum_age_seconds=binding.maximum_age_seconds,
                authorization_reference="p8-equity-input-cycle-2",
            )
            persist_advanced_options_equity_input_binding_cursor(
                database_cursor,
                second,
            )
            persist_advanced_options_equity_input_binding_cursor(
                database_cursor,
                second,
            )
            database_cursor.execute(
                "SELECT count(*) "
                "FROM rt.advanced_options_equity_input_bindings "
                "WHERE target_cohort_id=%s",
                (str(binding.target_cohort_id),),
            )
            assert database_cursor.fetchone() == (2,)
            boundary_bindings = []
            for age_seconds in (300, 301):
                boundary_window_id = uuid4()
                boundary_cutoff = equity_cutoff + timedelta(
                    seconds=age_seconds
                )
                database_cursor.execute(
                    "INSERT INTO rt.window_runs ("
                    "window_id,plan_id,cohort_id,provider_tier,window_key,"
                    "release_at,cutoff,deadline,state) VALUES ("
                    "%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'PENDING')",
                    (
                        str(boundary_window_id),
                        str(options_plan_id),
                        str(enrollment.control_plane.cohort_id),
                        f"options-age-{age_seconds}",
                        boundary_cutoff,
                        boundary_cutoff,
                        boundary_cutoff + timedelta(minutes=5),
                    ),
                )
                boundary_bindings.append(
                    build_advanced_options_equity_input_binding(
                        target_options_window_id=boundary_window_id,
                        target_cohort_id=binding.target_cohort_id,
                        source_cohort_id=binding.source_cohort_id,
                        source_cohort_sha256=binding.source_cohort_sha256,
                        source_manifest_id=binding.source_manifest_id,
                        source_manifest_sha256=(
                            binding.source_manifest_sha256
                        ),
                        source_profile_id=binding.source_profile_id,
                        source_profile_sha256=(
                            binding.source_profile_sha256
                        ),
                        equity_window_id=binding.equity_window_id,
                        equity_receipt_id=binding.equity_receipt_id,
                        equity_cutoff=binding.equity_cutoff,
                        reference_generation=binding.reference_generation,
                        universe_sha256=binding.universe_sha256,
                        input_sha256=binding.input_sha256,
                        build_id=binding.build_id,
                        maximum_age_seconds=300,
                        authorization_reference=(
                            f"p8-equity-input-age-{age_seconds}"
                        ),
                    )
                )
            persist_advanced_options_equity_input_binding_cursor(
                database_cursor,
                boundary_bindings[0],
            )
            with pytest.raises(
                ValueError,
                match="target window binding changed",
            ):
                persist_advanced_options_equity_input_binding_cursor(
                    database_cursor,
                    boundary_bindings[1],
                )
