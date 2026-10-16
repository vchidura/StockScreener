from datetime import datetime, timezone

import pytest

from tests.realtime.session_close_acceptance_probe import parse_args


def _args():
    return [
        "--database", "test_realtime_v2_close_probe",
        "--confirm-database-name", "test_realtime_v2_close_probe",
        "--role-prefix", "test_rt_close_probe",
        "--cohort-id", "d1baee6f-4ab2-543c-8a86-ce10f8952e69",
        "--session-date", "2026-10-15",
        "--known-by", "2026-10-15T20:21:00+00:00",
    ]


@pytest.mark.parametrize("database", ("stocks_realtime_v1", "stocks_realtime_v2"))
def test_probe_refuses_live_database_names(database):
    args = _args()
    args[1] = args[3] = database
    with pytest.raises(SystemExit):
        parse_args(args)


def test_probe_requires_matching_confirmation_and_owned_role_prefix():
    for index, value in ((3, "test_realtime_v2_other"), (5, "stocks_realtime_v2")):
        args = _args()
        args[index] = value
        with pytest.raises(SystemExit):
            parse_args(args)


def test_probe_requires_explicit_persistence_pair():
    for flag in ("--persist", "--execute", "--expect-truncated-refusal"):
        with pytest.raises(SystemExit):
            parse_args([*_args(), flag])
    assert parse_args([*_args(), "--persist", "--execute"]).persist


def test_probe_requires_aware_inspection_clock():
    args = _args()
    args[-1] = "2026-10-15T20:21:00"
    with pytest.raises(SystemExit):
        parse_args(args)
    assert parse_args(_args()).known_by == datetime(
        2026, 10, 15, 20, 21, tzinfo=timezone.utc
    )
