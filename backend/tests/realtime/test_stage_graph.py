from __future__ import annotations

import itertools
import re
from pathlib import Path

from realtime.migrations import V2_MIGRATIONS_DIR
from realtime.relation_registry import CAPABILITY_ROLES, PRINCIPAL_PROFILES
from realtime.stage_graph import (
    NON_COMPLETING_STATES,
    STAGES_V1,
    Action,
    decide_window,
    principal_profiles_required,
    startable_stages,
)

ALL = frozenset(STAGES_V1)
OPTIONS = frozenset(name for name, stage in STAGES_V1.items() if stage.domain == "options")
INDEPENDENT = ALL - OPTIONS
STATES = ("PENDING", "RUNNING", "COMPLETE", "FAILED", "BLOCKED_PREREQ", "BLOCKED_POLICY", "MISSED")


def test_stage_graph_equals_sql_seed():
    text = (Path(V2_MIGRATIONS_DIR) / "004_realtime_orchestration.sql").read_text(encoding="utf-8")
    rows = re.findall(
        r"\('([A-Z_]+)', 'stages_v1', '(shared|equity|options)', ARRAY\[([^\]]*)\](?:::text\[\])?, '([a-z_]+)', "
        r"'(runtime_data|runtime_options)', '([A-Z]+)', (true|false)\)", text)
    parsed = {name: (domain, tuple(re.findall(r"'([A-Z_]+)'", prerequisites)), profile, capability, schedule,
                     gate == "true")
              for name, domain, prerequisites, capability, profile, schedule, gate in rows}
    assert parsed == {name: (stage.domain, stage.prerequisites, stage.principal_profile, stage.capability_role,
                             stage.schedule_kind, stage.gates_strategy)
                      for name, stage in STAGES_V1.items()}
    assert set(parsed) == set(STAGES_V1)


def test_each_stage_capability_role_belongs_to_its_principal_profile():
    for name, stage in STAGES_V1.items():
        assert stage.capability_role in PRINCIPAL_PROFILES[stage.principal_profile], name
        assert (stage.capability_role in CAPABILITY_ROLES)
        assert (stage.domain == "options") == stage.capability_role.startswith("options_")


def test_blocked_options_admission_does_not_stop_equity_reference_or_portal():
    # The 2026-10-12 blocker: Options admission BLOCKED / REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE.
    states = {"REFERENCE": "COMPLETE", "OPTIONS_ACQUIRE": "COMPLETE", "OPTIONS_ADMISSION": "BLOCKED_POLICY"}
    decisions = decide_window(ALL, states)
    assert decisions["EQUITY_INGEST"].action is Action.RUN
    assert decisions["OPTIONS_CYCLE"].action is Action.BLOCK_PREREQ
    assert decisions["OPTIONS_CYCLE"].blocked_by == "OPTIONS_ADMISSION"
    states.update({"EQUITY_INGEST": "COMPLETE", "EQUITY_EVIDENCE": "COMPLETE"})
    decisions = decide_window(ALL, states)
    assert {decisions[name].action for name in ("PORTAL_PUBLISH", "STRATEGY_PUBLISH", "STOCK_ALERTS")} == {Action.RUN}


def test_independent_stage_decisions_never_depend_on_any_options_stage_state():
    base = {"REFERENCE": "COMPLETE", "EQUITY_INGEST": "COMPLETE"}
    expected = decide_window(ALL, base)
    for combination in itertools.product(STATES, repeat=len(OPTIONS)):
        states = dict(base)
        states.update(dict(zip(sorted(OPTIONS), combination)))
        decisions = decide_window(ALL, states)
        for name in INDEPENDENT:
            assert decisions[name] == expected[name], (name, combination)


def test_failure_blocks_only_downstream_stages_with_the_named_prerequisite():
    for failed in sorted(ALL):
        for state in NON_COMPLETING_STATES:
            decisions = decide_window(ALL, {failed: state})
            for name, decision in decisions.items():
                if name == failed:
                    assert decision.action is Action.DONE
                elif decision.action is Action.BLOCK_PREREQ:
                    assert decision.blocked_by in STAGES_V1[name].prerequisites
                    assert decisions[decision.blocked_by].action in {Action.DONE, Action.BLOCK_PREREQ} or \
                        decision.blocked_by == failed


def test_reference_readiness_satisfies_reference_for_window_stages():
    decisions = decide_window(ALL, {}, {"REFERENCE": "READY"})
    assert decisions["EQUITY_INGEST"].action is Action.RUN
    assert decisions["OPTIONS_ACQUIRE"].action is Action.RUN
    assert decide_window(ALL, {}, {})["EQUITY_INGEST"].action is Action.WAIT


def test_disabled_stages_are_skipped_and_do_not_run():
    decisions = decide_window({"REFERENCE", "EQUITY_INGEST"}, {})
    assert decisions["REFERENCE"].action is Action.RUN
    assert decisions["OPTIONS_CYCLE"].action is Action.SKIPPED_DISABLED


def test_a_blocked_shared_reference_holds_only_its_actual_dependents_in_startup_reporting():
    readiness = {"REFERENCE": "BLOCKED"}
    started = startable_stages(ALL, readiness)
    dependents = {name for name, stage in STAGES_V1.items() if "REFERENCE" in stage.prerequisites}
    assert dependents == {"EQUITY_INGEST", "OPTIONS_ACQUIRE", "OPTIONS_FLOW"}
    assert not (dependents & started) and "REFERENCE" not in started
    # Stages whose own prerequisites are not blocked are unaffected; blocked readiness propagates as
    # each dependent records its own BLOCKED readiness.
    assert {"EQUITY_EVIDENCE", "PORTAL_PUBLISH", "OPTIONS_ADMISSION"} <= started
    # A shared prerequisite does not have to be enabled in this plan.
    assert "EQUITY_INGEST" in startable_stages(frozenset({"EQUITY_INGEST"}), {})
    assert "EQUITY_INGEST" not in startable_stages(frozenset({"EQUITY_INGEST"}), readiness)


def test_service_starts_with_blocked_options_and_needs_only_enabled_principals():
    readiness = {"OPTIONS_ADMISSION": "BLOCKED", "OPTIONS_CYCLE": "BLOCKED", "REFERENCE": "READY"}
    started = startable_stages(ALL, readiness)
    assert INDEPENDENT <= started
    assert "OPTIONS_CYCLE" not in started and "OPTIONS_ADMISSION" not in started
    assert principal_profiles_required(INDEPENDENT) == frozenset({"runtime_data"})
    assert principal_profiles_required(ALL) == frozenset({"runtime_data", "runtime_options"})
