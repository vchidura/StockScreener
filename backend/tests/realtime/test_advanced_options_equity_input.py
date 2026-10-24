from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from realtime.advanced_options_control_plane import (
    build_advanced_options_equity_input_binding,
)
from realtime.advanced_options_equity_input import (
    validate_advanced_options_equity_dependencies_cursor,
)
from realtime.contracts import UniverseManifest
from tests.realtime.test_advanced_options_control_plane import (
    _enrollment_plan,
)


class _Cursor:
    def __init__(self, *, omit_qqq_context=False):
        self.query = ""
        self.parameters = ()
        self.omit_qqq_context = omit_qqq_context

    def execute(self, query, parameters):
        self.query = query
        self.parameters = parameters

    def fetchall(self):
        if "revision.bar_revision_id::text" in self.query:
            if (
                self.omit_qqq_context
                and self.parameters[1]
                == "44444444-4444-4444-8444-444444444444"
            ):
                return []
            cutoff = self.parameters[-2]
            return [
                {
                    "bar_revision_id": str(uuid4()),
                    "bar_end": cutoff,
                    "observed_at": cutoff,
                    "payload_sha256": "a" * 64,
                },
                {
                    "bar_revision_id": str(uuid4()),
                    "bar_end": cutoff - timedelta(minutes=5),
                    "observed_at": cutoff - timedelta(minutes=5),
                    "payload_sha256": "b" * 64,
                },
                {
                    "bar_revision_id": str(uuid4()),
                    "bar_end": cutoff - timedelta(minutes=10),
                    "observed_at": cutoff - timedelta(minutes=10),
                    "payload_sha256": "c" * 64,
                },
            ]
        raise AssertionError(self.query)


def _dependencies():
    enrollment = _enrollment_plan()
    equity = replace(
        enrollment.equity_plan,
        cohort_id=UUID("11111111-1111-4111-8111-111111111111"),
        cohort_sha256="8" * 64,
        manifest_id=UUID("22222222-2222-4222-8222-222222222222"),
        profile_id=UUID("33333333-3333-4333-8333-333333333333"),
        universe=UniverseManifest(
            "p8-equity-input-test",
            ("QQQ", "SPY"),
            (),
        ),
        security_ids=(
            ("QQQ", UUID("44444444-4444-4444-8444-444444444444")),
            ("SPY", UUID("55555555-5555-4555-8555-555555555555")),
        ),
    )
    options_cutoff = enrollment.control_plane.receipts.options_input_ready.spec.cutoff
    equity_cutoff = options_cutoff - timedelta(minutes=2)
    binding = build_advanced_options_equity_input_binding(
        target_options_window_id=uuid4(),
        target_cohort_id=enrollment.control_plane.cohort_id,
        source_cohort_id=equity.cohort_id,
        source_cohort_sha256=equity.cohort_sha256,
        source_manifest_id=equity.manifest_id,
        source_manifest_sha256=equity.manifest.sha256,
        source_profile_id=equity.profile_id,
        source_profile_sha256=equity.profile_sha256,
        equity_window_id=uuid4(),
        equity_receipt_id=uuid4(),
        equity_cutoff=equity_cutoff,
        reference_generation=1,
        universe_sha256=equity.universe.sha256,
        input_sha256="9" * 64,
        build_id="advanced-equity-input-test-v1",
        maximum_age_seconds=300,
        authorization_reference="p8-equity-input-test",
    )
    return equity, binding


def test_two_underlying_dependencies_are_exact_and_ready():
    equity, binding = _dependencies()
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(),
        equity_plan=equity,
        equity_input=binding,
        history_source_cohort_id=uuid4(),
        underlyings=("QQQ", "SPY"),
    )
    assert result["status"] == "READY"
    assert result["lane_context"]["O1"]["status"] == "AVAILABLE"
    assert result["lane_context"]["O3"]["status"] == "AVAILABLE"
    assert result["lane_context"]["O3"]["member_count"] == 6


def test_missing_qqq_context_blocks_only_o3_lane_before_provider_work():
    equity, binding = _dependencies()
    result = validate_advanced_options_equity_dependencies_cursor(
        _Cursor(omit_qqq_context=True),
        equity_plan=equity,
        equity_input=binding,
        history_source_cohort_id=uuid4(),
        underlyings=("QQQ", "SPY"),
    )

    assert result["status"] == "READY"
    assert result["lane_context"]["O1"]["status"] == "AVAILABLE"
    assert result["lane_context"]["O3"]["status"] == "BLOCKED"
    assert result["lane_context"]["O3"]["reason_codes"] == (
        "O3_QQQ_E_CONTEXT_INCOMPLETE",
    )
