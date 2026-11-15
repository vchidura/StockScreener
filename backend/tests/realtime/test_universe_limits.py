"""G102 universe sizing (Nov 15): every universe-size limit comes from one place (realtime/universe_limits.py)."""
import re
from pathlib import Path
from uuid import uuid4

import pytest

from realtime import universe_limits as limits
from realtime.advanced_realtime_desired_state import DesiredMemberIntent, DesiredMemberKind, DesiredStateChange
from realtime.universe_file import UniverseFileError, parse_universe

BACKEND = Path(__file__).resolve().parents[2]


def test_defaults_are_todays_limits():
    assert limits.universe_limits({}) == {
        "REALTIME_UNIVERSE_MAX_NAMES": 400,
        "REALTIME_OPTIONS_MAX_UNDERLYINGS": 50,
        "REALTIME_OPTIONS_MAX_CONTRACTS_PER_CONNECTION": 1000,
    }


@pytest.mark.parametrize("raw,expected", [("1", 1), ("450", 450), (" 5000 ", 5000), ("", 400)])
def test_universe_maximum_setting(raw, expected):
    assert limits.max_universe_names({"REALTIME_UNIVERSE_MAX_NAMES": raw}) == expected


@pytest.mark.parametrize("name,raw", [
    ("REALTIME_UNIVERSE_MAX_NAMES", "0"), ("REALTIME_UNIVERSE_MAX_NAMES", "5001"), ("REALTIME_UNIVERSE_MAX_NAMES", "x"),
    ("REALTIME_OPTIONS_MAX_UNDERLYINGS", "51"), ("REALTIME_OPTIONS_MAX_CONTRACTS_PER_CONNECTION", "1001"),
])
def test_invalid_settings_fail_with_their_name(name, raw):
    with pytest.raises(ValueError, match=name):
        limits.universe_limits({name: raw})


def test_lower_options_limits_are_allowed():
    assert limits.max_options_underlyings({"REALTIME_OPTIONS_MAX_UNDERLYINGS": "10"}) == 10
    assert limits.max_option_contracts_per_connection({"REALTIME_OPTIONS_MAX_CONTRACTS_PER_CONNECTION": "500"}) == 500


def test_the_universe_file_and_the_desired_state_follow_the_setting(monkeypatch):
    names = [f"T{index:03d}" for index in range(12)]
    document = {"version": 1, "equity": {"tickers": names}, "options": {"underlyings": names[:3]}}
    monkeypatch.setenv("REALTIME_UNIVERSE_MAX_NAMES", "10")
    with pytest.raises(UniverseFileError, match="at most 10 .REALTIME_UNIVERSE_MAX_NAMES"):
        parse_universe(document)
    members = tuple(DesiredMemberIntent(ticker, DesiredMemberKind.EQUITY) for ticker in names)
    with pytest.raises(ValueError, match="at most 10 exact names"):
        DesiredStateChange(source_cohort_id=uuid4(), members=members)
    monkeypatch.setenv("REALTIME_UNIVERSE_MAX_NAMES", "12")
    assert len(parse_universe(document).names) == 12
    monkeypatch.setenv("REALTIME_OPTIONS_MAX_UNDERLYINGS", "2")
    with pytest.raises(UniverseFileError, match="REALTIME_OPTIONS_MAX_UNDERLYINGS"):
        parse_universe(document)


def test_no_live_module_hardcodes_the_universe_maximum():
    """The universe-size limit appears only in universe_limits.py; a literal 400 used as a size bound elsewhere in the
    live realtime modules would bypass the setting."""
    pattern = re.compile(r"(<=|<|>|>=)\s*40[01]\b|\b40[01]\s*(<=|<)|maximum=400\b|range\(1, 401\)")
    offenders = []
    for path in sorted((BACKEND / "realtime").glob("*.py")):
        if path.name == "universe_limits.py":
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(line) and not line.lstrip().startswith("#"):
                offenders.append(f"{path.name}:{number}: {line.strip()}")
    assert offenders == []
