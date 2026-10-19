from __future__ import annotations

import heapq
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import NamedTuple
from uuid import UUID, uuid5

from ..option_source_flow import OptionSourceFlowIdentity
from .clock import SessionClock, to_datetime
from .records import ClusterRow, FlowRetentionEvidence


CANONICAL_ORDER_WINDOWS_SECONDS = (1, 5, 60, 180, 900)

CLUSTER_NAMESPACE = UUID("6a1f0e7e-3b52-4d0e-9c1a-5c0f1d2e7a10")
MIXED = "MIXED"
_ELIGIBLE_BUCKETS = {
    "AT_OR_ABOVE_ASK": "AT_OR_ABOVE_ASK",
    "AT_OR_BELOW_BID": "AT_OR_BELOW_BID",
    "INSIDE_SPREAD": "INSIDE_SPREAD",
    "NO_QUOTE": "NO_QUOTE",
    "AGGRESSION_UNAVAILABLE_NO_QUOTE": "AGGRESSION_UNAVAILABLE_NO_QUOTE",
    "QUOTE_CROSSED_OR_LOCKED": "NOT_CLASSIFIED",
    "QUOTE_STALE": "NOT_CLASSIFIED",
    "NOT_CLASSIFIED": "NOT_CLASSIFIED",
}
_FOREVER = 1 << 62


def cluster_bucket(side_class: str) -> str | None:
    """Cluster side for a print; None keeps multi-leg prints out of clusters entirely."""
    return _ELIGIBLE_BUCKETS.get(side_class)


@dataclass(frozen=True, slots=True)
class ClusterConfig:
    """Hopping event-time windows (hop = half the window) approximating sliding windows.

    A burst no longer than half a window always lands wholly inside at least one window.
    Thresholds mirror FlowStrategyPolicy so batch and streaming results stay comparable.
    """

    windows_seconds: tuple[int, ...]
    minimum_prints: int
    minimum_distinct_exchanges: int
    minimum_print_notional: Decimal
    allowed_lateness_ms: int = 2_000
    finalized_retention_ms: int = 60_000
    emit_mixed: bool = True

    def __post_init__(self) -> None:
        if not self.windows_seconds or any(type(value) is not int or value <= 0 for value in self.windows_seconds):
            raise ValueError("windows_seconds must be positive integers")
        if len(set(self.windows_seconds)) != len(self.windows_seconds):
            raise ValueError("windows_seconds must be unique")
        if self.minimum_prints < 1 or self.minimum_distinct_exchanges < 1:
            raise ValueError("cluster thresholds must be positive")
        if self.allowed_lateness_ms < 0 or self.finalized_retention_ms < 0:
            raise ValueError("lateness and retention must be nonnegative")

    @classmethod
    def from_policy(
        cls,
        policy,
        *,
        windows_seconds: tuple[int, ...] = CANONICAL_ORDER_WINDOWS_SECONDS,
        allowed_lateness_ms: int = 2_000,
        finalized_retention_ms: int = 60_000,
        emit_mixed: bool = True,
    ) -> "ClusterConfig":
        windows = tuple(sorted(set(windows_seconds) | {policy.sweep_window_seconds}))
        return cls(
            windows_seconds=windows,
            minimum_prints=policy.minimum_sweep_prints,
            minimum_distinct_exchanges=policy.minimum_distinct_exchanges,
            minimum_print_notional=policy.minimum_print_notional,
            allowed_lateness_ms=allowed_lateness_ms,
            finalized_retention_ms=finalized_retention_ms,
            emit_mixed=emit_mixed,
        )


class ClusterPrint(NamedTuple):
    event_id: UUID
    contract: str
    underlyer: str
    bucket: str
    time_ms: int
    seq: int
    size: int
    premium: Decimal
    exchange: int | None
    cumulative_contracts: int | None
    retention_thresholds: tuple[FlowRetentionEvidence, ...]


class _State:
    __slots__ = (
        "cluster_id",
        "contract",
        "underlyer",
        "side",
        "window_seconds",
        "start_ms",
        "end_ms",
        "session_date",
        "profile_evidence",
        "cumulative_contracts",
        "members",
        "revision",
        "emitted",
        "qualified",
        "final",
        "retracted",
        "span_ms",
    )

    def __init__(self, cluster_id: UUID, contract: str, underlyer: str, side: str, window_seconds: int,
                 start_ms: int, end_ms: int, session_date: date,
                 profile_evidence: FlowRetentionEvidence | None = None,
                 cumulative_contracts: int | None = None) -> None:
        self.cluster_id = cluster_id
        self.contract = contract
        self.underlyer = underlyer
        self.side = side
        self.window_seconds = window_seconds
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.session_date = session_date
        self.profile_evidence = profile_evidence
        self.cumulative_contracts = cumulative_contracts
        self.members: dict[
            tuple[int, int],
            tuple[int, Decimal, int | None, int, str, UUID],
        ] = {}
        self.revision = 0
        self.emitted = False
        self.qualified = False
        self.final = False
        self.retracted = False
        # First and last print time of the latest emitted revision; a retraction reuses them.
        self.span_ms: tuple[int, int] | None = None


class ClusterStatus(str, Enum):
    PROVISIONAL = "PROVISIONAL"
    FINAL = "FINAL"
    RETRACTED = "RETRACTED"


class ClusterEngine:
    """Per-shard clustering of persisted prints.

    A cluster is PROVISIONAL once its window holds enough prints and venues, FINAL when the
    watermark (latest event time minus the allowed lateness) passes the window end, and gets a
    superseding revision, never an edit, when a cancel or a late print changes it afterwards.
    When cancels remove its last print, a RETRACTED revision with zero counts supersedes it.
    Prints older than the retention are counted and left out; their volume is unaffected.
    """

    def __init__(
        self,
        config: ClusterConfig,
        *,
        cohort_id: UUID,
        provider_tier: str,
        policy_sha256: str,
        source_flow: OptionSourceFlowIdentity | None = None,
        advanced_flow_policy_sha256: str | None = None,
        contract_scope_sha256: str | None = None,
        manifest_sha256: str | None = None,
        slot_sha256: str | None = None,
        provider_contract_sha256: str | None = None,
        condition_reference_sha256: str | None = None,
        exchange_reference_sha256: str | None = None,
        clock: SessionClock | None = None,
    ) -> None:
        self._config = config
        self._cohort_id = cohort_id
        self._provider_tier = provider_tier
        self._policy_sha256 = policy_sha256
        self._source_flow = source_flow
        self._advanced_flow_policy_sha256 = (
            advanced_flow_policy_sha256
        )
        self._contract_scope_sha256 = contract_scope_sha256
        self._manifest_sha256 = manifest_sha256
        self._slot_sha256 = slot_sha256
        self._provider_contract_sha256 = provider_contract_sha256
        self._condition_reference_sha256 = condition_reference_sha256
        self._exchange_reference_sha256 = exchange_reference_sha256
        self._clock = clock or SessionClock()
        self._states: dict[tuple[str, str, str, int, int], _State] = {}
        self._open: list[tuple[int, str, str, str, int, int]] = []
        self._purge: list[
            tuple[int, tuple[str, str, str, int, int]]
        ] = []
        self._max_time_ms = -1
        self._watermark_ms = -1
        self.late_beyond_retention = 0
        self.late_revisions = 0
        self.finalized = 0

    @property
    def watermark_ms(self) -> int:
        return self._watermark_ms

    def open_states(self) -> int:
        return len(self._states)

    def observe_time(self, time_ms: int, clusters: list[ClusterRow]) -> None:
        if time_ms > self._max_time_ms:
            self._max_time_ms = time_ms
            if self._open and self._open[0][0] <= time_ms - self._config.allowed_lateness_ms:
                self._advance(time_ms - self._config.allowed_lateness_ms, clusters)

    def finalize_all(self, clusters: list[ClusterRow]) -> None:
        self._advance(_FOREVER, clusters)

    def _advance(self, watermark_ms: int, clusters: list[ClusterRow]) -> None:
        if watermark_ms <= self._watermark_ms:
            return
        self._watermark_ms = watermark_ms
        retention = self._config.finalized_retention_ms
        while self._open and self._open[0][0] <= watermark_ms:
            (
                end_ms,
                contract,
                profile,
                side,
                window,
                start,
            ) = heapq.heappop(self._open)
            key = (contract, profile, side, window, start)
            state = self._states.get(key)
            if state is None or state.final:
                continue
            state.final = True
            self.finalized += 1
            if state.members and (state.emitted or self._qualifies(state)):
                clusters.append(self._row(state, ClusterStatus.FINAL))
            heapq.heappush(self._purge, (end_ms + retention, key))
        while self._purge and self._purge[0][0] <= watermark_ms:
            self._states.pop(heapq.heappop(self._purge)[1], None)

    def _qualifies(self, state: _State) -> bool:
        members = state.members
        evidence = state.profile_evidence
        minimum_prints = (
            self._config.minimum_prints
            if evidence is None
            else evidence.minimum_cluster_prints
        )
        minimum_exchanges = (
            self._config.minimum_distinct_exchanges
            if evidence is None
            else evidence.minimum_distinct_exchanges
        )
        if (
            minimum_prints is None
            or minimum_exchanges is None
            or len(members) < minimum_prints
        ):
            return False
        exchanges = {member[2] for member in members.values() if member[2] is not None}
        if len(exchanges) < minimum_exchanges:
            return False
        if state.side == MIXED and len({member[4] for member in members.values()}) < 2:
            return False
        if evidence is not None:
            total_premium = sum(
                (member[1] for member in members.values()),
                Decimal(0),
            )
            if (
                evidence.minimum_cluster_premium is None
                or total_premium < evidence.minimum_cluster_premium
            ):
                return False
            if (
                evidence.minimum_cumulative_contracts is not None
                and (
                    state.cumulative_contracts is None
                    or state.cumulative_contracts
                    < evidence.minimum_cumulative_contracts
                )
            ):
                return False
        return True

    def _keys(self, print_: ClusterPrint):
        sides = (print_.bucket, MIXED) if self._config.emit_mixed else (print_.bucket,)
        time_ms = print_.time_ms
        if print_.retention_thresholds:
            profiles = tuple(
                (evidence.profile, evidence.window_seconds, evidence)
                for evidence in print_.retention_thresholds
                if evidence.window_seconds is not None
            )
        else:
            profiles = tuple(
                ("", window_seconds, None)
                for window_seconds in self._config.windows_seconds
            )
        for profile, window_seconds, evidence in profiles:
            window_ms = window_seconds * 1000
            hop = window_ms // 2
            index = time_ms // hop
            for start_index in (index - 1, index):
                if start_index < 0:
                    continue
                start = start_index * hop
                for side in sides:
                    yield (
                        profile,
                        side,
                        window_seconds,
                        window_ms,
                        start,
                        evidence,
                    )

    def on_print(self, print_: ClusterPrint, clusters: list[ClusterRow]) -> None:
        member = (
            print_.size,
            print_.premium,
            print_.exchange,
            print_.time_ms,
            print_.bucket,
            print_.event_id,
        )
        key_id = (print_.seq, print_.time_ms)
        for (
            profile,
            side,
            window_seconds,
            window_ms,
            start,
            evidence,
        ) in self._keys(print_):
            key = (
                print_.contract,
                profile,
                side,
                window_seconds,
                start,
            )
            state = self._states.get(key)
            if state is None:
                if start + window_ms + self._config.finalized_retention_ms <= self._watermark_ms:
                    self.late_beyond_retention += 1
                    continue
                state = _State(
                    uuid5(CLUSTER_NAMESPACE, f"{self._source_flow.source_flow_id if self._source_flow else self._cohort_id}|"
                                             f"{print_.contract}|{profile}|{side}|{window_seconds}|"
                                             f"{start}|{self._policy_sha256}"),
                    print_.contract, print_.underlyer, side, window_seconds, start, start + window_ms,
                    self._clock.session_date(start), evidence,
                    print_.cumulative_contracts,
                )
                self._states[key] = state
                if start + window_ms <= self._watermark_ms:
                    # A late print recreated a window the watermark has already passed.
                    state.final = True
                    heapq.heappush(self._purge, (start + window_ms + self._config.finalized_retention_ms, key))
                else:
                    heapq.heappush(
                        self._open,
                        (
                            state.end_ms,
                            print_.contract,
                            profile,
                            side,
                            window_seconds,
                            start,
                        ),
                    )
            if key_id in state.members:
                continue
            state.cumulative_contracts = print_.cumulative_contracts
            state.members[key_id] = member
            if state.final:
                if state.emitted or self._qualifies(state):
                    self.late_revisions += 1
                    clusters.append(self._row(state, ClusterStatus.FINAL))
            elif (not state.emitted or state.retracted) and self._qualifies(state):
                clusters.append(self._row(state, ClusterStatus.PROVISIONAL))

    def on_cancel(self, print_: ClusterPrint, clusters: list[ClusterRow]) -> None:
        key_id = (print_.seq, print_.time_ms)
        for (
            profile,
            side,
            window_seconds,
            _,
            start,
            _,
        ) in self._keys(print_):
            state = self._states.get((
                print_.contract,
                profile,
                side,
                window_seconds,
                start,
            ))
            if state is None or key_id not in state.members:
                continue
            state.cumulative_contracts = print_.cumulative_contracts
            del state.members[key_id]
            if not state.emitted:
                continue
            if state.members:
                clusters.append(self._row(state, ClusterStatus.FINAL if state.final else ClusterStatus.PROVISIONAL))
            else:
                clusters.append(self._retraction(state))

    def _retraction(self, state: _State) -> ClusterRow:
        """RETRACTED revision: every print was canceled, so all counts are zero."""
        state.revision += 1
        state.retracted = True
        state.qualified = False
        first_ms, last_ms = state.span_ms
        return ClusterRow(
            cluster_id=state.cluster_id,
            revision=state.revision,
            session_date=state.session_date,
            cohort_id=self._cohort_id,
            provider_tier=self._provider_tier,
            contract_ticker=state.contract,
            underlyer=state.underlyer,
            window_seconds=state.window_seconds,
            window_start=to_datetime(state.start_ms),
            window_end=to_datetime(state.end_ms),
            side_class=state.side,
            print_count=0,
            total_size=0,
            total_premium=Decimal(0),
            distinct_exchanges=0,
            first_event_time=to_datetime(first_ms),
            last_event_time=to_datetime(last_ms),
            status=ClusterStatus.RETRACTED.value,
            policy_sha256=self._policy_sha256,
            source_flow_id=(
                self._source_flow.source_flow_id
                if self._source_flow is not None
                else None
            ),
            condition_semantics_sha256=(
                self._source_flow.condition_semantics_sha256
                if self._source_flow is not None
                else None
            ),
            flow_policy_sha256=(
                self._source_flow.flow_policy_sha256
                if self._source_flow is not None
                else None
            ),
            lineage_version=(
                self._source_flow.identity_version
                if self._source_flow is not None
                else 1
            ),
            profile=(
                None
                if state.profile_evidence is None
                else state.profile_evidence.profile
            ),
            flow_universe_tier=(
                None
                if state.profile_evidence is None
                else state.profile_evidence.universe_tier
            ),
            flow_dte_lane=(
                None
                if state.profile_evidence is None
                else state.profile_evidence.dte_lane
            ),
            retention_threshold=state.profile_evidence,
            advanced_flow_policy_sha256=(
                self._advanced_flow_policy_sha256
            ),
            contract_scope_sha256=self._contract_scope_sha256,
            manifest_sha256=self._manifest_sha256,
            slot_sha256=self._slot_sha256,
            runtime_policy_sha256=self._policy_sha256,
            cumulative_contracts=state.cumulative_contracts,
            qualifies=False,
            member_event_ids=(),
            member_event_keys=(),
            provider_contract_sha256=self._provider_contract_sha256,
            condition_reference_sha256=self._condition_reference_sha256,
            exchange_reference_sha256=self._exchange_reference_sha256,
        )

    def _row(self, state: _State, status: ClusterStatus) -> ClusterRow:
        members = state.members.values()
        state.revision += 1
        state.emitted = True
        state.retracted = False
        state.qualified = self._qualifies(state)
        times = [member[3] for member in members]
        state.span_ms = (min(times), max(times))
        exchanges = {member[2] for member in members if member[2] is not None}
        return ClusterRow(
            cluster_id=state.cluster_id,
            revision=state.revision,
            session_date=state.session_date,
            cohort_id=self._cohort_id,
            provider_tier=self._provider_tier,
            contract_ticker=state.contract,
            underlyer=state.underlyer,
            window_seconds=state.window_seconds,
            window_start=to_datetime(state.start_ms),
            window_end=to_datetime(state.end_ms),
            side_class=state.side,
            print_count=len(state.members),
            total_size=sum(member[0] for member in members),
            total_premium=sum((member[1] for member in members), Decimal(0)),
            distinct_exchanges=max(1, len(exchanges)),
            first_event_time=to_datetime(min(times)),
            last_event_time=to_datetime(max(times)),
            status=status.value,
            policy_sha256=self._policy_sha256,
            source_flow_id=(
                self._source_flow.source_flow_id
                if self._source_flow is not None
                else None
            ),
            condition_semantics_sha256=(
                self._source_flow.condition_semantics_sha256
                if self._source_flow is not None
                else None
            ),
            flow_policy_sha256=(
                self._source_flow.flow_policy_sha256
                if self._source_flow is not None
                else None
            ),
            lineage_version=(
                self._source_flow.identity_version
                if self._source_flow is not None
                else 1
            ),
            profile=(
                None
                if state.profile_evidence is None
                else state.profile_evidence.profile
            ),
            flow_universe_tier=(
                None
                if state.profile_evidence is None
                else state.profile_evidence.universe_tier
            ),
            flow_dte_lane=(
                None
                if state.profile_evidence is None
                else state.profile_evidence.dte_lane
            ),
            retention_threshold=state.profile_evidence,
            advanced_flow_policy_sha256=(
                self._advanced_flow_policy_sha256
            ),
            contract_scope_sha256=self._contract_scope_sha256,
            manifest_sha256=self._manifest_sha256,
            slot_sha256=self._slot_sha256,
            runtime_policy_sha256=self._policy_sha256,
            cumulative_contracts=state.cumulative_contracts,
            qualifies=state.qualified,
            member_event_ids=tuple(
                sorted(
                    (member[5] for member in state.members.values()),
                    key=str,
                )
            ),
            member_event_keys=tuple(
                sorted(
                    (
                        (member[5], to_datetime(member[3]))
                        for member in state.members.values()
                    ),
                    key=lambda row: (row[1], str(row[0])),
                )
            ),
            provider_contract_sha256=self._provider_contract_sha256,
            condition_reference_sha256=self._condition_reference_sha256,
            exchange_reference_sha256=self._exchange_reference_sha256,
        )

    def update_cumulative(
        self,
        contract: str,
        cumulative_contracts: int,
        clusters: list[ClusterRow] | None = None,
    ) -> None:
        for state in self._states.values():
            if state.contract != contract:
                continue
            was_qualified = state.qualified
            state.cumulative_contracts = cumulative_contracts
            qualifies = self._qualifies(state)
            if clusters is None or not state.members:
                continue
            if (
                (not state.emitted and qualifies)
                or (state.emitted and qualifies != was_qualified)
            ):
                clusters.append(self._row(
                    state,
                    (
                        ClusterStatus.FINAL
                        if state.final
                        else ClusterStatus.PROVISIONAL
                    ),
                ))

    def digest_state(self) -> list:
        return [[key, state.profile_evidence, state.cumulative_contracts,
                 state.revision, state.emitted, state.qualified,
                 state.final, state.retracted, sorted(state.members)]
                for key, state in sorted(self._states.items())]
