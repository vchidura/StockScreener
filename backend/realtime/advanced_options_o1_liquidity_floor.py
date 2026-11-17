"""O1 liquidity floor on the live (advanced) Options window (G102 R87 / G106 Q2, Nov 17 2026).

O1's activity rule v1 (`options.dual_origin.ACTIVITY_POLICY`, `option_participation_origin_v1`) detects a contract when
its day volume is at least 3x its prior-session open interest, with no floor on either. On Nov 16 it selected AAPL
362.5P 11/27 with open interest 27 and day volume 109. The floor policy (`ACTIVITY_LIQUIDITY_FLOOR_POLICY`,
`option_participation_origin_v2_liquidity_floor`: prior OI >= 100 and day volume >= 500, reasons `O1LF_*`) is a new
activity policy version applied in the O1 executor (`advanced_options_native_adapters.build_advanced_o1_executor`), the
place where a detection becomes SELECTED / OBSERVATION / NOT_SELECTED. An event below the floor is NOT_DETECTED there,
so it never reaches the window's grouping (one alert per contract and package variant per window, earliest detection
first: `advanced_options_projection._one_alert_per_contract`) or the session alert (first trigger per session, detector
and contract: `persist_options_session_alerts_cursor`); both run on what is left, so a later event on the contract that
clears the floor becomes the alert instead (O1 has no top-N: every detected event with a package is selected).

Switch `REALTIME_V3_O1_LIQUIDITY_FLOOR` on the Options window (`realtime-v3-options-p8-window`):

| OFF (default, unset) | today's O1, byte for byte: v1 findings, no extra work, no log line |
| RECORD | v1 findings and selection unchanged; the floor is evaluated for every O1 event and logged: one
             `O1_LIQUIDITY_FLOOR_RECORD {json}` line per detected event below the floor and one
             `O1_LIQUIDITY_FLOOR_WINDOW {json}` line per window (counts, reasons, what selection would pick instead) |
| ON | every O1 evaluation carries the v2 finding (schema `option_participation_finding_v2`, the v2 policy hash, the
       floor's inputs and reasons); events below the floor are NOT_SELECTED; the same two log lines |
| anything else | the window service refuses to start and every window that reads it fails: never silently OFF |

Thresholds (Nov 17): `REALTIME_V3_O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST` (default 100) and
`REALTIME_V3_O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME` (default 500) on the same service. Unset or blank = the default (the
policy and its hash are exactly the built-in `ACTIVITY_LIQUIDITY_FLOOR_POLICY`); a set value must be a non-negative
integer (digits only) or the window service refuses to start. The values are stamped into the policy document, so the
policy hash, the v2 episode ids and every v2 evaluation change with them, and the window line names them. They are read
ONCE per process (`o1_liquidity_floor_policy`, cached; the window service reads them at start): one process = one policy,
and a compose env change needs a container recreate anyway, which is the restart that picks the new values up.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Iterable, Mapping, Sequence
from uuid import UUID

from functools import lru_cache

from options.dual_origin import ACTIVITY_LIQUIDITY_FLOOR_POLICY, ActivityLiquidityFloorPolicy

O1_LIQUIDITY_FLOOR_SETTING = "REALTIME_V3_O1_LIQUIDITY_FLOOR"
O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING = "REALTIME_V3_O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST"
O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING = "REALTIME_V3_O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME"
O1_LIQUIDITY_FLOOR_RECORD_EVENT = "O1_LIQUIDITY_FLOOR_RECORD"
O1_LIQUIDITY_FLOOR_WINDOW_EVENT = "O1_LIQUIDITY_FLOOR_WINDOW"
_RECORD_LIST_LIMIT = 60


class O1LiquidityFloorMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def o1_liquidity_floor_mode(environ: Mapping[str, str] | None = None) -> O1LiquidityFloorMode:
    """The switch value; unset or blank is OFF. Anything but OFF, RECORD or ON raises (fails loudly, never silently)."""
    raw = (os.environ if environ is None else environ).get(O1_LIQUIDITY_FLOOR_SETTING)
    value = (raw or "").strip().upper() or O1LiquidityFloorMode.OFF.value
    try:
        return O1LiquidityFloorMode(value)
    except ValueError:
        raise ValueError(f"{O1_LIQUIDITY_FLOOR_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


def non_negative_int_setting(environ: Mapping[str, str], key: str, default: int) -> int:
    """An integer threshold from the environment: unset or blank = `default`; otherwise ASCII digits only (surrounding
    blanks allowed), so '-1', '+5', '1.0', '1e3', '0x10' and words all raise with the key's name (fails loudly)."""
    raw = environ.get(key)
    value = (raw or "").strip()
    if not value:
        return default
    if not value.isascii() or not value.isdigit():
        raise ValueError(f"{key} must be a non-negative integer, not {raw!r}")
    return int(value)


def o1_liquidity_floor_policy_from(environ: Mapping[str, str]) -> ActivityLiquidityFloorPolicy:
    """The effective floor policy for these settings; unset = `ACTIVITY_LIQUIDITY_FLOOR_POLICY` itself."""
    default = ACTIVITY_LIQUIDITY_FLOOR_POLICY
    open_interest = non_negative_int_setting(
        environ, O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING, default.minimum_prior_open_interest)
    day_volume = non_negative_int_setting(environ, O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING, default.minimum_day_volume)
    if (open_interest, day_volume) == (default.minimum_prior_open_interest, default.minimum_day_volume):
        return default
    return ActivityLiquidityFloorPolicy(minimum_prior_open_interest=open_interest, minimum_day_volume=day_volume)


@lru_cache(maxsize=1)
def o1_liquidity_floor_policy() -> ActivityLiquidityFloorPolicy:
    """The process's floor policy, read once from the environment (the window service calls this at start)."""
    return o1_liquidity_floor_policy_from(os.environ)


@dataclass(frozen=True, slots=True)
class O1LiquidityFloorEntry:
    """One evaluated O1 event: its v1 disposition, its disposition under the floor and the floor's inputs."""

    event_id: UUID
    underlyer: str
    contract_ticker: str
    variant: str  # "" for a single contract; "O1_DEBIT_VERTICAL:<short contract>" for an AL2 vertical package
    observed_at: datetime
    v1_disposition: str
    floor_disposition: str
    liquidity_reasons: tuple[str, ...]
    open_interest: int | None
    day_volume: int | None
    volume_oi_ratio: Decimal | None
    has_package: bool

    @property
    def below_floor(self) -> bool:
        """Detected by v1 and taken out by the floor."""
        return self.v1_disposition == "DETECTED" and self.floor_disposition != "DETECTED"


def o1_liquidity_floor_entry(invocation, v1_finding, floored_finding) -> O1LiquidityFloorEntry:
    package = invocation.package
    variant = ""
    if package is not None and len(package.legs) == 2:
        variant = f"O1_DEBIT_VERTICAL:{package.legs[1].contract_ticker}"
    return O1LiquidityFloorEntry(
        event_id=invocation.event_id,
        underlyer=invocation.source.underlyer,
        contract_ticker=invocation.source.contract_ticker,
        variant=variant,
        observed_at=v1_finding.decision_at,
        v1_disposition=v1_finding.disposition,
        floor_disposition=floored_finding.disposition,
        liquidity_reasons=tuple(floored_finding.liquidity_reasons),
        open_interest=floored_finding.open_interest,
        day_volume=floored_finding.day_volume,
        volume_oi_ratio=v1_finding.volume_oi_ratio,
        has_package=package is not None,
    )


def _window_alerts(entries: Iterable[O1LiquidityFloorEntry]) -> dict[tuple[str, str], O1LiquidityFloorEntry]:
    """The window's alert per contract and package variant: its earliest selected event (the projection's grouping;
    the projection breaks a time tie by evaluation id, this record by event id)."""
    alerts: dict[tuple[str, str], O1LiquidityFloorEntry] = {}
    for entry in sorted(entries, key=lambda item: (item.observed_at, str(item.event_id))):
        alerts.setdefault((entry.contract_ticker, entry.variant), entry)
    return alerts


def o1_liquidity_floor_documents(
    *,
    mode: O1LiquidityFloorMode,
    cutoff: datetime,
    entries: Sequence[O1LiquidityFloorEntry],
    selection_enabled: bool = True,
    policy: ActivityLiquidityFloorPolicy | None = None,
) -> tuple[dict, list[dict]]:
    """(the window document, one document per detected event below the floor). Selection as the O1 executor makes it:
    SELECTED = detected, a package, selection enabled. `policy`: the effective floor policy (default: the process's)."""
    policy = policy or o1_liquidity_floor_policy()
    selected_v1 = [e for e in entries if selection_enabled and e.has_package and e.v1_disposition == "DETECTED"]
    selected_floor = [e for e in entries if selection_enabled and e.has_package and e.floor_disposition == "DETECTED"]
    alerts_v1, alerts_floor = _window_alerts(selected_v1), _window_alerts(selected_floor)
    below = [e for e in entries if e.below_floor]
    selected_v1_ids = {e.event_id for e in selected_v1}
    instead = []
    for key in sorted(alerts_v1):
        first, replacement = alerts_v1[key], alerts_floor.get(key)
        if replacement is not None and replacement.event_id == first.event_id:
            continue
        instead.append({
            "contract": key[0], "variant": key[1] or None, "alert_event_id": str(first.event_id),
            "reasons": list(first.liquidity_reasons),
            "instead_event_id": None if replacement is None else str(replacement.event_id),
            "instead_day_volume": None if replacement is None else replacement.day_volume,
        })
    window = {
        "contract": "o1-liquidity-floor-window-v1",
        "mode": mode.value,
        "policy": policy.version,
        "policy_sha256": policy.sha256,
        "minimum_prior_open_interest": policy.minimum_prior_open_interest,
        "minimum_day_volume": policy.minimum_day_volume,
        "cutoff": cutoff.isoformat(),
        "o1_events": len(entries),
        "detected": sum(e.v1_disposition == "DETECTED" for e in entries),
        "below_floor": len(below),
        "below_floor_selected": sum(e.event_id in selected_v1_ids for e in below),
        "selected_without_floor": len(selected_v1),
        "selected_with_floor": len(selected_floor),
        "alerts_without_floor": len(alerts_v1),
        "alerts_with_floor": len(alerts_floor),
        "alerts_refilled": sum(item["instead_event_id"] is not None for item in instead),
        "applied": mode is O1LiquidityFloorMode.ON,
        "reasons": dict(sorted(Counter(reason for e in below for reason in e.liquidity_reasons).items())),
        "by_underlying": dict(sorted(Counter(e.underlyer for e in below).items())),
        "instead": instead[:_RECORD_LIST_LIMIT],
    }
    events = [
        {
            "contract": "o1-liquidity-floor-record-v1",
            "mode": mode.value,
            "policy": policy.version,
            "policy_sha256": policy.sha256,
            "cutoff": cutoff.isoformat(),
            "event_id": str(e.event_id),
            "underlyer": e.underlyer,
            "contract_ticker": e.contract_ticker,
            "variant": e.variant or None,
            "observed_at": e.observed_at.isoformat(),
            "open_interest": e.open_interest,
            "day_volume": e.day_volume,
            "volume_oi_ratio": None if e.volume_oi_ratio is None else str(e.volume_oi_ratio),
            "reasons": list(e.liquidity_reasons),
            "without_floor": "SELECTED" if e.event_id in selected_v1_ids else "OBSERVATION",
            "with_floor": "NOT_SELECTED" if e.floor_disposition == "NOT_DETECTED" else e.floor_disposition,
            "applied": mode is O1LiquidityFloorMode.ON,
        }
        for e in sorted(below, key=lambda item: (item.observed_at, str(item.event_id)))
    ]
    return window, events


def log_o1_liquidity_floor_documents(window: Mapping[str, object], events: Sequence[Mapping[str, object]]) -> None:
    for document in events:
        print(f"{O1_LIQUIDITY_FLOOR_RECORD_EVENT} {json.dumps(document, sort_keys=True, default=str)}",
              file=sys.stderr, flush=True)
    print(f"{O1_LIQUIDITY_FLOOR_WINDOW_EVENT} {json.dumps(window, sort_keys=True, default=str)}",
          file=sys.stderr, flush=True)


__all__ = [
    "O1LiquidityFloorEntry", "O1LiquidityFloorMode", "O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME_SETTING",
    "O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST_SETTING", "O1_LIQUIDITY_FLOOR_RECORD_EVENT", "O1_LIQUIDITY_FLOOR_SETTING",
    "O1_LIQUIDITY_FLOOR_WINDOW_EVENT", "log_o1_liquidity_floor_documents", "non_negative_int_setting",
    "o1_liquidity_floor_documents", "o1_liquidity_floor_entry", "o1_liquidity_floor_mode", "o1_liquidity_floor_policy",
    "o1_liquidity_floor_policy_from",
]
