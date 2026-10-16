import importlib

import pytest

from realtime.reference_copy import order_self_referencing_rows


def test_self_referencing_rows_are_parent_first_even_when_primary_key_order_is_not():
    columns = ("action_id", "supersedes_action_id", "payload")
    rows = (
        ("a", "z", "child"),
        ("z", None, "parent"),
        ("m", "a", "grandchild"),
    )

    ordered = order_self_referencing_rows(
        rows,
        columns=columns,
        primary_key=("action_id",),
        foreign_keys=({
            "columns": ("supersedes_action_id",),
            "referenced_columns": ("action_id",),
        },),
    )

    assert [row[0] for row in ordered] == ["z", "a", "m"]


def test_self_referencing_rows_reject_missing_parent_and_cycles():
    options = {
        "columns": ("id", "parent_id"),
        "primary_key": ("id",),
        "foreign_keys": ({
            "columns": ("parent_id",),
            "referenced_columns": ("id",),
        },),
    }
    with pytest.raises(ValueError, match="parent is missing"):
        order_self_referencing_rows((("a", "missing"),), **options)
    with pytest.raises(ValueError, match="cycle"):
        order_self_referencing_rows((("a", "b"), ("b", "a")), **options)


def test_reference_copy_script_requires_canonical_target_and_execute_authorization():
    script = importlib.import_module("scripts.copy_realtime_v2_reference_data")
    base = [
        "--database",
        "stocks_realtime_v2",
        "--confirm-database-name",
        "stocks_realtime_v2",
    ]

    assert script.parse_args(base).execute is False
    with pytest.raises(SystemExit):
        script.parse_args([*base, "--execute"])
    assert script.parse_args([
        *base,
        "--execute",
        "--authorization-reference",
        "approved",
    ]).execute
    with pytest.raises(ValueError):
        script.parse_args([
            "--database",
            "stocks_realtime_v1",
            "--confirm-database-name",
            "stocks_realtime_v1",
        ])
