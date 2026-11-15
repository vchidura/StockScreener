"""Universe sizing 3.3 (Nov 15): a member minute with no trade inside a session.

The stream has no AM bar for a minute without a trade, and the REST repair's full-session answer has none either. Until
this rule, the gap repair raised "provider did not return every missing minute", waived the gap, and the Equity input
gate failed the whole five-minute window for every member (Nov 13 E:18:05 on a GS minute). With the switch on:

- the gap repair records such minutes as provider-confirmed empty on the gap row
  (`rt.stream_gaps.provider_confirmed_empty_starts`, migration 054) when the provider's answer is healthy (it holds
  bars after the hole, so the provider is not merely behind) and closes the gap as REPAIRED;
- the input gate accepts a member minute only when such a REPAIRED gap of the window's ingress plan records it.

Nothing is filled or synthesized (INV-5). The 5m finalizer already derives a sparse window on gap-free coverage (r88);
the ticker 1m chart labels the hole (`NATIVE_1M_HISTORY_INCOMPLETE`).

Switch off (default): no new column is read or written, so the code is safe before migration 054.
"""
from __future__ import annotations

import os
from collections.abc import Mapping

CONFIRMED_EMPTY_MINUTES_SWITCH = "REALTIME_V2_CONFIRMED_EMPTY_MINUTES"
# A no-trade stretch inside one repair hole; a longer hole stays a waived gap (an outage or a halt).
MAX_CONFIRMED_EMPTY_MINUTES_PER_HOLE = 5


def confirmed_empty_minutes_enabled(environ: Mapping[str, str] | None = None) -> bool:
    value = (os.environ if environ is None else environ).get(CONFIRMED_EMPTY_MINUTES_SWITCH, "")
    return value.strip().upper() in {"ON", "1", "TRUE"}
