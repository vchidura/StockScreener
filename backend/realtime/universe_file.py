"""G102 universe sizing (Nov 15): the operator's universe file.

One TOML file lists the realtime universe: Equity names, sector ETFs and Options underlyings, with optional priorities.
`scripts/manage_realtime_universe.py` validates it and writes it as a new desired-state revision; the services keep
reading the desired-state tables (`rt.realtime_desired_*`). Design: `docs/g102/G102_UNIVERSE_SIZING_20261114.md`
section 3.

File format (version 1)::

    version = 1

    [equity]
    tickers = ["AAPL", "MSFT"]

    [sector_etfs]
    tickers = ["XLK"]

    [options]
    underlyings = ["AAPL"]      # each must also be listed under [equity] or [sector_etfs]

    [priority]                  # optional; 1 = highest; unlisted names get 1
    AAPL = 1

Names not in the file are not in the universe. A removed name stops live processing from the next session; its stored
data is kept.
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from .advanced_realtime_desired_state import DesiredMemberIntent, DesiredMemberKind, DesiredStateChange
from .universe_limits import max_options_underlyings, max_universe_names

UNIVERSE_FILE_VERSION = 1
DEFAULT_PRIORITY = 1
_TOP_KEYS = frozenset({"version", "equity", "sector_etfs", "options", "priority"})
_SECTIONS = {
    "equity": ("tickers", DesiredMemberKind.EQUITY),
    "sector_etfs": ("tickers", DesiredMemberKind.SECTOR_ETF),
    "options": ("underlyings", DesiredMemberKind.OPTION_UNDERLYING),
}


class UniverseFileError(ValueError):
    """The universe file is not valid; the message names every problem found."""


@dataclass(frozen=True, slots=True)
class UniverseFile:
    equity: tuple[str, ...]
    sector_etfs: tuple[str, ...]
    option_underlyings: tuple[str, ...]
    priorities: tuple[tuple[str, int], ...] = ()

    def priority(self, ticker: str) -> int:
        return dict(self.priorities).get(ticker, DEFAULT_PRIORITY)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(set(self.equity) | set(self.sector_etfs)))

    def members(self) -> tuple[DesiredMemberIntent, ...]:
        return tuple(
            DesiredMemberIntent(ticker=ticker, kind=kind, enabled=True, priority=self.priority(ticker))
            for tickers, kind in (
                (self.equity, DesiredMemberKind.EQUITY),
                (self.sector_etfs, DesiredMemberKind.SECTOR_ETF),
                (self.option_underlyings, DesiredMemberKind.OPTION_UNDERLYING),
            )
            for ticker in tickers
        )

    def to_change(self, *, source_cohort_id: UUID, source_slot_version: int | None = None) -> DesiredStateChange:
        return DesiredStateChange(
            source_cohort_id=source_cohort_id,
            source_slot_version=source_slot_version if self.option_underlyings else None,
            members=self.members(),
        )


def _normalize(value: object, where: str, problems: list[str]) -> str | None:
    if not isinstance(value, str):
        problems.append(f"{where}: {value!r} is not a ticker string")
        return None
    ticker = value.strip().upper()
    try:
        DesiredMemberIntent(ticker=ticker, kind=DesiredMemberKind.EQUITY)
    except (TypeError, ValueError):
        problems.append(f"{where}: {value!r} is not a valid ticker")
        return None
    return ticker


def parse_universe(document: dict) -> UniverseFile:
    """Validate a parsed TOML document; raise UniverseFileError listing every problem."""
    problems: list[str] = []
    unknown = sorted(set(document) - _TOP_KEYS)
    if unknown:
        problems.append("unknown top-level keys: " + ", ".join(unknown))
    if document.get("version") != UNIVERSE_FILE_VERSION:
        problems.append(f"version must be {UNIVERSE_FILE_VERSION}")
    lists: dict[str, tuple[str, ...]] = {}
    for section, (key, _kind) in _SECTIONS.items():
        table = document.get(section, {})
        if not isinstance(table, dict):
            problems.append(f"[{section}] must be a table")
            lists[section] = ()
            continue
        extra = sorted(set(table) - {key})
        if extra:
            problems.append(f"[{section}] unknown keys: " + ", ".join(extra))
        raw = table.get(key, [])
        if not isinstance(raw, list):
            problems.append(f"[{section}] {key} must be a list")
            raw = []
        seen: list[str] = []
        for index, value in enumerate(raw):
            ticker = _normalize(value, f"[{section}] {key}[{index}]", problems)
            if ticker is None:
                continue
            if ticker in seen:
                problems.append(f"[{section}] {ticker} is listed twice")
                continue
            seen.append(ticker)
        lists[section] = tuple(seen)
    both = sorted(set(lists["equity"]) & set(lists["sector_etfs"]))
    if both:
        problems.append("listed under both [equity] and [sector_etfs]: " + ", ".join(both))
    base = set(lists["equity"]) | set(lists["sector_etfs"])
    if not base:
        problems.append("the universe needs at least one [equity] or [sector_etfs] ticker")
    missing = sorted(set(lists["options"]) - base)
    if missing:
        problems.append(
            "Options underlyings must also be listed under [equity] or [sector_etfs]: " + ", ".join(missing)
        )
    maximum_names = max_universe_names()
    if len(base) > maximum_names:
        problems.append(f"{len(base)} names; the universe supports at most {maximum_names} (REALTIME_UNIVERSE_MAX_NAMES)")
    maximum_underlyings = max_options_underlyings()
    if len(lists["options"]) > maximum_underlyings:
        problems.append(
            f"{len(lists['options'])} Options underlyings; at most {maximum_underlyings} "
            "(REALTIME_OPTIONS_MAX_UNDERLYINGS)"
        )
    priorities: list[tuple[str, int]] = []
    raw_priority = document.get("priority", {})
    if not isinstance(raw_priority, dict):
        problems.append("[priority] must be a table")
        raw_priority = {}
    for key, value in raw_priority.items():
        ticker = _normalize(key, "[priority]", problems)
        if ticker is None:
            continue
        if ticker not in base:
            problems.append(f"[priority] {ticker} is not in the universe")
            continue
        if type(value) is not int or not 1 <= value <= 32767:
            problems.append(f"[priority] {ticker} must be an integer in [1, 32767]")
            continue
        priorities.append((ticker, value))
    if problems:
        raise UniverseFileError("; ".join(problems))
    return UniverseFile(
        equity=tuple(sorted(lists["equity"])),
        sector_etfs=tuple(sorted(lists["sector_etfs"])),
        option_underlyings=tuple(sorted(lists["options"])),
        priorities=tuple(sorted(priorities)),
    )


def load_universe_file(path: Path) -> UniverseFile:
    try:
        document = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as error:
        raise UniverseFileError(f"{path}: not valid TOML: {error}") from error
    return parse_universe(document)


def universe_from_members(members) -> UniverseFile:
    """The enabled members of a stored revision (rows with ticker, kind, priority, enabled) as a UniverseFile."""
    lists: dict[DesiredMemberKind, list[str]] = {kind: [] for kind in DesiredMemberKind}
    priorities: dict[str, int] = {}
    for ticker, kind, priority, enabled in members:
        if not enabled:
            continue
        member_kind = DesiredMemberKind(str(kind))
        lists[member_kind].append(str(ticker))
        if int(priority) != DEFAULT_PRIORITY:
            priorities[str(ticker)] = min(int(priority), priorities.get(str(ticker), int(priority)))
    return UniverseFile(
        equity=tuple(sorted(lists[DesiredMemberKind.EQUITY])),
        sector_etfs=tuple(sorted(lists[DesiredMemberKind.SECTOR_ETF])),
        option_underlyings=tuple(sorted(lists[DesiredMemberKind.OPTION_UNDERLYING])),
        priorities=tuple(sorted(priorities.items())),
    )


def diff_universe(current: UniverseFile, wanted: UniverseFile) -> dict[str, object]:
    """What applying `wanted` changes against `current` (names per kind, priorities)."""
    result: dict[str, object] = {}
    for label, attribute in (("equity", "equity"), ("sector_etfs", "sector_etfs"),
                             ("options", "option_underlyings")):
        before, after = set(getattr(current, attribute)), set(getattr(wanted, attribute))
        result[label] = {"added": sorted(after - before), "removed": sorted(before - after), "count": len(after)}
    before_priority, after_priority = dict(current.priorities), dict(wanted.priorities)
    result["priority_changes"] = {
        ticker: [before_priority.get(ticker, DEFAULT_PRIORITY), after_priority.get(ticker, DEFAULT_PRIORITY)]
        for ticker in sorted(set(before_priority) | set(after_priority))
        if before_priority.get(ticker, DEFAULT_PRIORITY) != after_priority.get(ticker, DEFAULT_PRIORITY)
    }
    result["changed"] = any(
        value["added"] or value["removed"] for key, value in result.items() if isinstance(value, dict) and "added" in value
    ) or bool(result["priority_changes"])
    return result


def _toml_list(values: tuple[str, ...], indent: str = "    ", per_line: int = 10) -> str:
    if not values:
        return "[]"
    rows = [values[index:index + per_line] for index in range(0, len(values), per_line)]
    body = ",\n".join(indent + ", ".join(f'"{value}"' for value in row) for row in rows)
    return "[\n" + body + ",\n]"


def render_universe(universe: UniverseFile, *, header: str = "") -> str:
    lines = []
    if header:
        lines.extend("# " + line if line else "#" for line in header.splitlines())
        lines.append("")
    lines.append(f"version = {UNIVERSE_FILE_VERSION}")
    lines.append("")
    lines.append("[equity]")
    lines.append("tickers = " + _toml_list(universe.equity))
    lines.append("")
    lines.append("[sector_etfs]")
    lines.append("tickers = " + _toml_list(universe.sector_etfs))
    lines.append("")
    lines.append("[options]")
    lines.append("underlyings = " + _toml_list(universe.option_underlyings))
    lines.append("")
    lines.append("[priority]")
    lines.extend(f"{ticker} = {value}" for ticker, value in universe.priorities)
    return "\n".join(lines) + "\n"
