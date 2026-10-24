from types import SimpleNamespace
from uuid import uuid4

from test_advanced_options_projection import _cycle
from realtime.advanced_options_projection import (
    build_advanced_options_projection_batch,
)
from realtime.runner_environment import RunnerEnvironment
from realtime.stage_graph import STAGES_V4, STAGE_CATALOG_VERSION_V4
from realtime.stage_runners_options import (
    OPTIONS_PUBLISH_AUTHORITY_INVALID,
    OptionsPublisherStageResult,
    OptionsReceiptStageResult,
    options_cycle_runner,
    options_input_gate_runner,
    options_projection_runner,
    options_publish_runner,
)
from realtime.stage_runners_v2 import fail_closed_stages, unwired_stages


def _context():
    return SimpleNamespace(
        is_cancelled=lambda: False,
        plan=SimpleNamespace(
            stage_catalog_version=STAGE_CATALOG_VERSION_V4,
        ),
    )


def test_all_stages_v4_are_bound_but_fail_closed_without_actions():
    environment = RunnerEnvironment(None)
    blocked = fail_closed_stages(frozenset(STAGES_V4), environment)

    assert unwired_stages(frozenset(STAGES_V4)) == frozenset()
    assert {
        "OPTIONS_INPUT_GATE",
        "OPTIONS_CYCLE",
        "OPTIONS_PROJECTION",
        "OPTIONS_PUBLISH",
    } <= set(blocked)


def test_injected_p4_receipt_actions_preserve_product_receipt_kinds():
    control, cycle = _cycle()
    projection = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=cycle.receipt.cutoff,
        build_id="p4-build",
    )
    environment = RunnerEnvironment(
        None,
        options_input_gate_action=lambda _context: OptionsReceiptStageResult(
            cycle.input_receipt,
            "input-ready",
        ),
        options_cycle_action=lambda _context: OptionsReceiptStageResult(
            cycle.receipt,
            "cycle",
        ),
        options_projection_action=lambda _context: OptionsReceiptStageResult(
            projection.portal_ready_receipt,
            "portal-ready",
        ),
    )

    assert options_input_gate_runner(environment)(_context()).receipt == (
        cycle.input_receipt
    )
    assert options_cycle_runner(environment)(_context()).receipt == cycle.receipt
    assert options_projection_runner(environment)(_context()).receipt == (
        projection.portal_ready_receipt
    )


def test_publisher_runner_accepts_only_inert_handoff():
    release_id = uuid4()
    safe = RunnerEnvironment(
        None,
        options_publish_action=lambda _context: OptionsPublisherStageResult(
            "a" * 64,
            "projection-only-handoff",
        ),
    )
    unsafe = RunnerEnvironment(
        None,
        options_publish_action=lambda _context: OptionsPublisherStageResult(
            "a" * 64,
            "unauthorized-release",
            release_authorized=True,
            pointer_authorized=True,
            release_id=release_id,
            pointer_release_id=release_id,
        ),
    )

    assert options_publish_runner(safe)(_context()).state == "COMPLETE"
    failed = options_publish_runner(unsafe)(_context())
    assert failed.state == "FAILED"
    assert failed.reason_codes == (OPTIONS_PUBLISH_AUTHORITY_INVALID,)
