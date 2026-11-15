"""G102 R62 (Nov 15): migration 053 matches the source catalog and changes only the size rules."""
from __future__ import annotations

from pathlib import Path

from realtime.advanced_ingress_runtime_profile import (
    SIZE_INDEPENDENT_PROFILE_APPROVAL_REFERENCE,
    load_advanced_ingress_runtime_catalog,
    size_independent_profile_registration,
)
from realtime.contracts import canonical_sha256
from realtime.universe_limits import SCHEMA_MAX_UNIVERSE_NAMES
from realtime.migrations import V2_MIGRATIONS_DIR, load_realtime_migrations

BACKEND_DIR = Path(__file__).resolve().parents[2]
POLICY = BACKEND_DIR / "realtime" / "policies" / "advanced_equity_ingress_v1.json"
VERSION = "053_g102_size_independent_runtime_profile"


def _migration(version: str) -> str:
    return next(item.sql for item in load_realtime_migrations(V2_MIGRATIONS_DIR) if item.version == version)


def _function(sql: str, name: str) -> str:
    start = sql.index(name)
    return sql[start:sql.index("$function$;", start)]


def test_053_is_in_the_contiguous_lineage():
    versions = [item.version for item in load_realtime_migrations(V2_MIGRATIONS_DIR)]
    assert VERSION in versions
    assert versions.index(VERSION) == 53


def test_053_registers_exactly_the_catalogs_size_independent_profile():
    catalog = load_advanced_ingress_runtime_catalog(POLICY)
    registration = size_independent_profile_registration(catalog)
    sql = _migration(VERSION)

    assert registration["runtime_profile_sha256"] == catalog.profile.sha256
    assert canonical_sha256(catalog.profile.identity) == catalog.profile.sha256
    for key in (
        "runtime_profile_sha256",
        "runtime_profile_version",
        "configuration_sha256",
        "runtime_policy_sha256",
        "stability_evidence_sha256",
        "capacity_evidence_sha256",
    ):
        assert f"'{registration[key]}'" in sql, key
    assert f"$profile${registration['profile_document']}$profile$::jsonb" in sql
    assert f"'{SIZE_INDEPENDENT_PROFILE_APPROVAL_REFERENCE}'" in sql
    assert registration["universe_target"] == SCHEMA_MAX_UNIVERSE_NAMES
    assert registration["ladder_position"] == 0
    assert registration["previous_universe_target"] is None
    # The live rows were all registered under this policy (checked read-only on Nov 15).
    assert registration["runtime_policy_sha256"].startswith("2504ed8ced91")


def test_053_changes_only_the_size_line_of_the_plan_rule():
    original = _function(_migration("023_realtime_advanced_stream_overlay"), "rt.enforce_advanced_ingress_plan_change()")
    changed = _function(_migration(VERSION), "rt.enforce_advanced_ingress_plan_change()")

    expected = original.replace(
        "           OR profile_row.universe_target <> NEW.universe_target\n",
        "           -- R62: the profile covers the plan's size (the size-independent profile covers every configured size).\n"
        "           OR profile_row.universe_target < NEW.universe_target\n",
    )
    assert expected != original
    assert changed == expected


def test_053_authorization_rule_needs_a_covering_profile_and_no_ladder():
    rule = _function(_migration(VERSION), "rt.enforce_advanced_authorization_promotion()")

    assert "universe_target >= NEW.allowed_universe_targets[1]" in rule
    assert "status = 'ACTIVE'" in rule
    assert "ladder_position" not in rule
    assert "previous_universe_target" not in rule
    assert "NEW.valid_from <= parent_row.valid_through" in rule


def test_no_later_migration_redefines_the_size_rules():
    later = [item for item in load_realtime_migrations(V2_MIGRATIONS_DIR) if item.sequence > 53]
    for item in later:
        assert "enforce_advanced_ingress_plan_change" not in item.sql
        assert "enforce_advanced_authorization_promotion" not in item.sql
