"""Frozen offline research registrations; no publication/execution permission."""
from dataclasses import asdict, dataclass, replace
import hashlib
import json


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class ResearchPolicy:
    model_id: str
    style: str
    interval: str
    family: str
    origin: str
    horizon: int
    horizon_unit: str
    version: str = "roadmap_research_v1"
    feature_version: str = "wilder14_session_vwap_slot20_v1"
    variant: str = "combined"
    max_quote_age_seconds: float = 5.0
    macro_policy: str = "required"
    # Nov 8: live Stock Alerts runs Swing rules with earnings coverage optional (events are optional there since
    # Nov 5; the earnings calendar is N/A after Nov 9). The default stays "required"; it is left out of the hash so
    # every existing registration keeps its policy hash, and an "optional" policy hashes differently.
    earnings_policy: str = "required"

    @property
    def policy_hash(self):
        values = asdict(self)
        if values["earnings_policy"] == "required":
            values.pop("earnings_policy")
        return digest(values)


def _catalog():
    result = {}
    def add(name, style, interval, family, origin, hold, unit, variant="combined"):
        result[name] = ResearchPolicy(name, style, interval, family, origin, hold, unit,
                                      variant=variant)
    for name, family, hold in (("R2", "resumption", 240), ("B3", "acceptance", 120),
                               ("F2", "failure", 60)):
        for interval in ("30m", "1h"):
            for variant in ("structure", "context", "participation", "combined"):
                add(f"{name}_{interval}_{variant}", "INTRADAY", interval, family,
                    "CURRENT_SESSION", hold, "minutes", variant)
        if name == "B3":
            for interval in ("30m", "1h"):
                add(f"B3_{interval}_retest", "INTRADAY", interval, family,
                    "CURRENT_SESSION", hold, "minutes", "retest")
    for suffix, family, hold in (("R", "resumption", 21), ("B", "acceptance", 10),
                                  ("F", "failure", 5)):
        add(f"SW3_{suffix}", "SWING", "1h", family, "CURRENT_SESSION_HOURLY",
            hold, "sessions", "structure")
    for name, hold in (("P1", 252), ("P2", 378)):
        add(name, "POSITION", "1d", name, "MONTH_END", hold, "sessions")
    for name, hold in (("SW1", 15), ("SW2", 20)):
        add(name, "SWING", "1h", name, "PRIOR_SESSION_DAILY", hold, "sessions")
    for name, hold in (("D1", 120), ("D2", 90)):
        add(name, "INTRADAY", "5m", name, "CURRENT_SESSION", hold, "minutes")
    for name, hold in (("C1", 60), ("C2", 45)):
        add(name, "SCALPING", "event", name, "QUOTE_EVENT", hold, "seconds")
    return result


CATALOG = _catalog()


def research_policy(model_id, macro_policy="required", earnings_policy="required"):
    if macro_policy not in {"required", "none"}:
        raise ValueError("macro_policy must be required or none")
    if earnings_policy not in {"required", "optional"}:
        raise ValueError("earnings_policy must be required or optional")
    return replace(CATALOG[model_id], macro_policy=macro_policy, earnings_policy=earnings_policy)

DEFAULT_MODELS = tuple([f"{name}_{interval}_combined" for name in ("R2", "B3", "F2")
                        for interval in ("30m", "1h")] +
                       ["B3_30m_retest", "B3_1h_retest", "P1", "P2", "SW1", "SW2",
                        "SW3_R", "SW3_B", "SW3_F", "D1", "D2", "C1", "C2"])
