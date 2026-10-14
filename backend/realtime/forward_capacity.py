from __future__ import annotations

from dataclasses import dataclass

from .contracts import UniverseManifest, canonical_sha256


CAPACITY_VERSION = "developer_forward_capacity_v1"
NATIVE_DETECTOR_UNDERLYING_LIMIT = 13
CURRENT_OPTION_QUOTE_LIMIT = 1_000


@dataclass(frozen=True, slots=True)
class ForwardDetectorShard:
    index: int
    underlyings: tuple[str, ...]
    shard_sha256: str


@dataclass(frozen=True, slots=True)
class DeveloperForwardCapacityPlan:
    universe_sha256: str
    package_workers: int
    selected_slots_per_underlying: int
    selected_slot_count: int
    detector_underlying_limit: int
    detector_shards: tuple[ForwardDetectorShard, ...]
    capacity_sha256: str


def build_developer_forward_capacity_plan(universe: UniverseManifest, *, package_workers=4,
                                          selected_slots_per_underlying=12,
                                          detector_underlying_limit=NATIVE_DETECTOR_UNDERLYING_LIMIT,
                                          option_quote_limit=CURRENT_OPTION_QUOTE_LIMIT):
    if (not isinstance(universe, UniverseManifest) or not universe.option_underlyings
            or type(package_workers) is not int or not 1 <= package_workers <= 32
            or type(selected_slots_per_underlying) is not int or selected_slots_per_underlying <= 0
            or type(detector_underlying_limit) is not int or not 1 <= detector_underlying_limit <= 13
            or type(option_quote_limit) is not int or option_quote_limit != CURRENT_OPTION_QUOTE_LIMIT):
        raise ValueError("Developer forward capacity inputs are outside reviewed bounds")
    selected = len(universe.option_underlyings) * selected_slots_per_underlying
    if selected > option_quote_limit:
        raise ValueError("selected option slots exceed the current versioned 1,000-quote contract")
    shards = []
    ordered = tuple(sorted(universe.option_underlyings))
    for offset in range(0, len(ordered), detector_underlying_limit):
        underlyings = ordered[offset:offset + detector_underlying_limit]
        index = len(shards)
        digest = canonical_sha256({"version": CAPACITY_VERSION, "universe_sha256": universe.sha256,
            "index": index, "underlyings": underlyings, "native_limit": detector_underlying_limit})
        shards.append(ForwardDetectorShard(index, underlyings, digest))
    payload = {"version": CAPACITY_VERSION, "universe_sha256": universe.sha256,
        "package_workers": package_workers, "selected_slots_per_underlying": selected_slots_per_underlying,
        "selected_slot_count": selected, "detector_underlying_limit": detector_underlying_limit,
        "detector_shards": tuple((row.index, row.underlyings, row.shard_sha256) for row in shards)}
    return DeveloperForwardCapacityPlan(universe.sha256, package_workers,
        selected_slots_per_underlying, selected, detector_underlying_limit,
        tuple(shards), canonical_sha256(payload))