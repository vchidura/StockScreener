"""G102 universe sizing (Nov 15): the operator's universe file and its checks."""
from pathlib import Path
from uuid import uuid4

import pytest

from realtime.advanced_realtime_desired_state import DesiredMemberKind
from realtime.universe_file import (
    UniverseFile,
    UniverseFileError,
    diff_universe,
    load_universe_file,
    parse_universe,
    render_universe,
    universe_from_members,
)

BACKEND = Path(__file__).resolve().parents[2]


def _doc(equity=("AAPL", "SPY"), etfs=("XLK",), options=("AAPL",), **extra):
    document = {
        "version": 1,
        "equity": {"tickers": list(equity)},
        "sector_etfs": {"tickers": list(etfs)},
        "options": {"underlyings": list(options)},
    }
    document.update(extra)
    return document


def test_one_name_and_one_underlying_is_a_universe():
    universe = parse_universe(_doc(equity=("GOOGL",), etfs=(), options=("GOOGL",)))
    assert universe.names == ("GOOGL",)
    change = universe.to_change(source_cohort_id=uuid4())
    kinds = sorted((member.ticker, member.kind.value) for member in change.members)
    assert kinds == [("GOOGL", "EQUITY"), ("GOOGL", "OPTION_UNDERLYING")]


def test_tickers_are_normalized_and_sorted():
    universe = parse_universe(_doc(equity=(" msft", "aapl"), etfs=(), options=()))
    assert universe.equity == ("AAPL", "MSFT")


@pytest.mark.parametrize("document,message", [
    (_doc(options=("NVDA",)), "must also be listed"),
    (_doc(equity=("AAPL", "AAPL")), "listed twice"),
    (_doc(equity=("AAPL", "XLK")), "both [equity] and [sector_etfs]"),
    (_doc(equity=(), etfs=(), options=()), "at least one"),
    (_doc(version=2), "version must be 1"),
    (_doc(extra_key=1), "unknown top-level keys"),
    (_doc(equity=("AAPL", "not a ticker!")), "not a valid ticker"),
    (_doc(priority={"MSFT": 1}), "not in the universe"),
    (_doc(priority={"AAPL": 0}), "integer in [1, 32767]"),
])
def test_invalid_files_are_refused_with_the_reason(document, message):
    with pytest.raises(UniverseFileError, match=message.replace("[", r"\[").replace("]", r"\]")):
        parse_universe(document)


def test_every_problem_is_reported_at_once():
    with pytest.raises(UniverseFileError) as error:
        parse_universe(_doc(equity=("AAPL", "AAPL"), options=("NVDA",), version=3))
    text = str(error.value)
    assert "listed twice" in text and "must also be listed" in text and "version" in text


def test_size_limits():
    many = tuple(f"T{index:03d}" for index in range(401))
    with pytest.raises(UniverseFileError, match="at most 400"):
        parse_universe(_doc(equity=many, etfs=(), options=()))
    fifty_one = tuple(f"U{index:02d}" for index in range(51))
    with pytest.raises(UniverseFileError, match="at most 50"):
        parse_universe(_doc(equity=fifty_one, etfs=(), options=fifty_one))
    assert len(parse_universe(_doc(equity=fifty_one[:50], etfs=(), options=fifty_one[:50])).option_underlyings) == 50


def test_priorities_apply_to_every_role_of_the_ticker():
    universe = parse_universe(_doc(priority={"aapl": 3}))
    change = universe.to_change(source_cohort_id=uuid4())
    assert {(m.ticker, m.kind.value): m.priority for m in change.members} == {
        ("AAPL", "EQUITY"): 3, ("AAPL", "OPTION_UNDERLYING"): 3, ("SPY", "EQUITY"): 1, ("XLK", "SECTOR_ETF"): 1,
    }


def test_equity_only_universe_drops_the_slot_version():
    universe = parse_universe(_doc(options=()))
    assert universe.to_change(source_cohort_id=uuid4(), source_slot_version=4).source_slot_version is None


def test_render_and_load_round_trip(tmp_path):
    names = tuple(f"N{index}" for index in range(23))
    universe = parse_universe(_doc(equity=names, options=names[:2], priority={"N3": 2}))
    path = tmp_path / "u.toml"
    path.write_text(render_universe(universe, header="header line\n\nmore"), encoding="utf-8")
    assert load_universe_file(path) == universe


def test_stored_members_and_diff():
    stored = (("AAPL", "EQUITY", 1, True), ("SPY", "EQUITY", 1, True), ("ROKU", "EQUITY", 1, False),
              ("XLK", "SECTOR_ETF", 1, True), ("AAPL", "OPTION_UNDERLYING", 1, True))
    current = universe_from_members(stored)
    assert current.equity == ("AAPL", "SPY")  # a disabled member is not in the universe
    wanted = parse_universe(_doc(equity=("AAPL", "GOOGL"), options=("AAPL", "GOOGL"), priority={"GOOGL": 2}))
    diff = diff_universe(current, wanted)
    assert diff["equity"] == {"added": ["GOOGL"], "removed": ["SPY"], "count": 2}
    assert diff["options"]["added"] == ["GOOGL"]
    assert diff["priority_changes"] == {"GOOGL": [1, 2]}
    assert diff["changed"] is True
    assert diff_universe(current, current)["changed"] is False


def test_the_shipped_file_is_valid_and_matches_the_live_list_of_nov_16():
    universe = load_universe_file(BACKEND / "config" / "realtime_universe.toml")
    assert isinstance(universe, UniverseFile)
    assert (len(universe.equity), len(universe.sector_etfs), len(universe.option_underlyings)) == (38, 7, 9)
    assert set(universe.option_underlyings) <= set(universe.equity)
    assert all(member.kind in DesiredMemberKind for member in universe.members())


def test_priority_keys_with_a_dot_round_trip(tmp_path):
    """Review (Nov 15): tickers may contain '.', which TOML reads as a dotted key unless quoted."""
    universe = parse_universe(_doc(equity=("BRK.B", "AAPL"), options=("AAPL",), priority={"BRK.B": 2}))
    path = tmp_path / "u.toml"
    path.write_text(render_universe(universe), encoding="utf-8")
    assert load_universe_file(path) == universe


@pytest.mark.parametrize("moment,refused", [
    ("2026-11-16T14:14:59+00:00", False),
    ("2026-11-16T14:15:00+00:00", True),
    ("2026-11-16T14:29:59+00:00", True),
    ("2026-11-16T14:30:00+00:00", False),
    ("2026-11-15T14:20:00+00:00", False),  # Sunday: no session
])
def test_apply_is_refused_between_activation_and_open(moment, refused):
    import importlib.util
    from datetime import datetime

    spec = importlib.util.spec_from_file_location("manage_realtime_universe", BACKEND / "scripts" / "manage_realtime_universe.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert (module.activation_window_refusal(datetime.fromisoformat(moment)) is not None) is refused
