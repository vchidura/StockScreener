"""Universe limits: the one place for every universe-size limit (G102 universe sizing, Nov 15).

Each limit is an environment setting with a default and a hard ceiling. Services receive the settings through the
compose `advanced-environment` anchor (`docker-compose.realtime-v2.yml`); local runs read them from the backend env
file (`backend/.env.realtime.example` lists them). Code never hardcodes these numbers: it calls the functions below.

| Setting | Default | Ceiling | Why the ceiling |
|---|---|---|---|
| REALTIME_UNIVERSE_MAX_NAMES | 400 | 5,000 | the database bound since migration 053 (stream plans, approvals, profiles) |
| REALTIME_OPTIONS_MAX_UNDERLYINGS | 50 | 50 | one Options window input holds at most 50 underlyings (128 KiB, no paging yet) |
| REALTIME_OPTIONS_MAX_CONTRACTS_PER_CONNECTION | 1,000 | 1,000 | the provider's option quote subscriptions per connection (slot-set CHECK) |

Raising a limit up to its ceiling is a configuration change. Raising a ceiling is a code or schema change, named in the
table. An invalid value (not an integer, below 1, above the ceiling) raises at the first use with the setting's name,
so a service fails at startup instead of running with a wrong limit.
"""
from __future__ import annotations

import os
from collections.abc import Mapping

UNIVERSE_MAX_NAMES_SETTING = "REALTIME_UNIVERSE_MAX_NAMES"
OPTIONS_MAX_UNDERLYINGS_SETTING = "REALTIME_OPTIONS_MAX_UNDERLYINGS"
OPTIONS_MAX_CONTRACTS_SETTING = "REALTIME_OPTIONS_MAX_CONTRACTS_PER_CONNECTION"

DEFAULT_UNIVERSE_MAX_NAMES = 400
DEFAULT_OPTIONS_MAX_UNDERLYINGS = 50
DEFAULT_OPTIONS_MAX_CONTRACTS = 1000

# Hard ceilings (not settings). Changing one needs the change named here.
SCHEMA_MAX_UNIVERSE_NAMES = 5000  # migration 053: rt.advanced_ingress_plans / _runtime_profiles / stream approvals
WINDOW_MAX_OPTIONS_UNDERLYINGS = 50  # realtime/option_cycle_input.py: one window input, 128 KiB, no paging
PROVIDER_MAX_OPTION_CONTRACTS_PER_CONNECTION = 1000  # provider limit; sql_v2/003 slot-set CHECK


def _setting(name: str, default: int, ceiling: int, environ: Mapping[str, str] | None) -> int:
    source = os.environ if environ is None else environ
    raw = str(source.get(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None
    if not 1 <= value <= ceiling:
        raise ValueError(f"{name} must be in [1, {ceiling}], got {value}")
    return value


def max_universe_names(environ: Mapping[str, str] | None = None) -> int:
    """The most Equity and sector ETF names a universe (and a session) may hold."""
    return _setting(UNIVERSE_MAX_NAMES_SETTING, DEFAULT_UNIVERSE_MAX_NAMES, SCHEMA_MAX_UNIVERSE_NAMES, environ)


def max_options_underlyings(environ: Mapping[str, str] | None = None) -> int:
    """The most Options underlyings a universe (and one Options window) may hold."""
    return _setting(
        OPTIONS_MAX_UNDERLYINGS_SETTING, DEFAULT_OPTIONS_MAX_UNDERLYINGS, WINDOW_MAX_OPTIONS_UNDERLYINGS, environ,
    )


def max_option_contracts_per_connection(environ: Mapping[str, str] | None = None) -> int:
    """The most option contracts one Options stream connection subscribes."""
    return _setting(
        OPTIONS_MAX_CONTRACTS_SETTING, DEFAULT_OPTIONS_MAX_CONTRACTS, PROVIDER_MAX_OPTION_CONTRACTS_PER_CONNECTION,
        environ,
    )


def universe_limits(environ: Mapping[str, str] | None = None) -> dict[str, int]:
    """Every effective limit, for reports and the universe command."""
    return {
        UNIVERSE_MAX_NAMES_SETTING: max_universe_names(environ),
        OPTIONS_MAX_UNDERLYINGS_SETTING: max_options_underlyings(environ),
        OPTIONS_MAX_CONTRACTS_SETTING: max_option_contracts_per_connection(environ),
    }
