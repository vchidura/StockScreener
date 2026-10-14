"""Offline screening projection over exact realtime-owned, split-qualified inputs."""
from __future__ import annotations

from dataclasses import InitVar, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping
from uuid import UUID

import exchange_calendars
import numpy as np
import pandas as pd

from equity.screening_hourly import hourly_slots, project_hourly
from equity.screening_projection import finalize, project_security
from research.screening import FIELDS, GAP_CATALOG, HOURLY_CATALOG, HOURLY_FIELDS, PATTERNS, digest

from .contracts import canonical_sha256
from .equity_screening_history import REALTIME_SCREENING_HISTORY_POLICY
from .lineage_head import (
    assemble_lineage_head, bar_group_head, id_set_identity, inputs_sha256, require_bounded_screening_manifest,
    revision_chain_sha256,
)
from .portal_projection import RealtimeScreeningGeneration, build_screening_generation
from .preflight import ProviderTier

SCREENING_LINEAGE_VERSION = "screening_lineage_head_v1"
DAILY_ADJUSTED_GROUP = "1d:ADJUSTED"
DAILY_RAW_GROUP = "1d:RAW_UNADJUSTED"
HOURLY_RAW_GROUP = "1h:RAW_UNADJUSTED"


SCREENING_BASIS = {
    "policy_sha256": REALTIME_SCREENING_HISTORY_POLICY.sha256,
    "historical_price_basis": "PROVIDER_SPLIT_ADJUSTED",
    "historical_daily_sessions": 253,
    "retained_daily_sessions": 283,
    "current_quote_basis": "RAW_ACTION_GATED",
    "hourly_price_basis": "RAW_ACTION_GATED",
    "discovery_liquidity_basis": "RAW_LAST_20_SESSIONS",
    "current_scale_max_relative_difference": 0.001,
}
REALTIME_FIELDS = {
    name: dict(spec, source="rt.bar_revisions" if spec["source"] == "canonical_daily_bars" else spec["source"],
               price_basis="RAW_ACTION_GATED" if name in {"price", "volume"} else
               "DATED_REFERENCE" if name == "instrument_type" else "PROVIDER_SPLIT_ADJUSTED")
    for name, spec in FIELDS.items()
}
REALTIME_GAP_CATALOG = dict(GAP_CATALOG, fields={
    name: dict(spec, price_basis="PROVIDER_SPLIT_ADJUSTED") for name, spec in GAP_CATALOG["fields"].items()
})


@dataclass(frozen=True)
class RealtimeScreeningInput:
    """`cutoff` is the market boundary (a bar must have ended by it) and the identity of the input.
    `known_by` (optional, timezone-aware, not before the cutoff; None means the cutoff) is when the system
    must have recorded a bar: observed_at, created_at, replay_available_at and provider_published_at are
    checked against `recorded_by`, bar_end and every market time against `cutoff`.

    `known_by` is an InitVar on purpose: it is not a dataclass field, so it stays out of equality, repr and
    `canonical_json`/`canonical_sha256` (every input hash and payload hash is byte-identical with and without
    it), yet `dataclasses.replace` carries it (it reads the instance attribute set below)."""

    cohort_id: UUID
    provider_tier: ProviderTier
    source_manifest_sha256: str
    source_generation: int
    session: date
    cutoff: datetime
    members: tuple[Mapping[str, object], ...]
    bars: tuple[Mapping[str, object], ...]
    actions: tuple[Mapping[str, object], ...]
    coverage: tuple[Mapping[str, object], ...]
    known_by: InitVar[datetime | None] = None

    def __post_init__(self, known_by):
        if self.cutoff.tzinfo is None or self.cutoff.utcoffset() is None:
            raise ValueError("Screening cutoff must be timezone-aware")
        object.__setattr__(self, "cutoff", self.cutoff.astimezone(timezone.utc))
        if known_by is not None:
            if known_by.tzinfo is None or known_by.utcoffset() is None:
                raise ValueError("Screening known_by must be timezone-aware")
            known_by = known_by.astimezone(timezone.utc)
            if known_by < self.cutoff:
                raise ValueError("Screening known_by must not precede the cutoff")
        object.__setattr__(self, "known_by", known_by)

    @property
    def recorded_by(self) -> datetime:
        """The instant by which every bar must have been recorded: `known_by`, else the cutoff."""
        return self.cutoff if self.known_by is None else self.known_by


def _chain(bars):
    return revision_chain_sha256((bar["bar_revision_id"], bar["payload_sha256"]) for bar in bars)


def _daily_lineage(member, row, adjusted, raw, actions, coverage):
    """What the generation keeps of one member's daily source: the lineage heads of the adjusted and raw daily
    bars the row was computed from (count, first/last revision and bar time, ordered chain hash over revision id
    and payload hash), the two continuity chains the new-membership comparison of consecutive generations needs
    (the adjusted history without its oldest and without its latest bar) and identities of the action and
    split-coverage evidence. Never a per-bar or per-revision list: the bars are reachable through rt.bar_current
    at exactly these heads."""
    return {
        "historical_price_basis": "PROVIDER_SPLIT_ADJUSTED",
        "bar_lineage": assemble_lineage_head({
            DAILY_ADJUSTED_GROUP: bar_group_head(adjusted), DAILY_RAW_GROUP: bar_group_head(raw),
        }),
        "adjusted_without_oldest_sha256": _chain(adjusted[1:]),
        "adjusted_without_latest_sha256": _chain(adjusted[:-1]),
        "current_quote_bar_id": row["source_bar_id"],
        "previous_quote_bar_id": str(raw[-2]["bar_revision_id"]),
        "actions": id_set_identity(action["action_id"] for action in actions),
        "split_coverage": id_set_identity(item["coverage_id"] for item in coverage),
        "action_evidence_sha256": canonical_sha256(actions),
        "split_coverage_sha256": canonical_sha256(coverage),
        "reference_id": str(member["security_revision_id"]),
        "reference_sha256": member["payload_sha256"],
        "instrument_id": member["instrument_id"],
    }


def _hourly_lineage(hourly, selected):
    """The lineage head of the hourly window a row's hourly context was computed from and the selected bar."""
    return {
        "bar_lineage": assemble_lineage_head({HOURLY_RAW_GROUP: bar_group_head(hourly)}) if hourly else None,
        "selected_bar_id": str(selected["bar_revision_id"]) if selected else None,
    }


def _validate_bars(rows, member, inputs):
    indexed = {str(row["bar_revision_id"]): row for row in rows}
    if len(indexed) != len(rows):
        raise ValueError("Screening bar revisions are duplicated")
    recorded_by = inputs.recorded_by
    for row in rows:
        if (
            str(row["cohort_id"]) != str(inputs.cohort_id)
            or row["provider_tier"] != inputs.provider_tier.value
            or str(row["security_id"]) != str(member["security_id"])
            or row["instrument_id"] != member["instrument_id"]
            or row["ticker"] != member["ticker"]
            or row["session_scope"] != "RTH" or not row["is_final"]
            or row["observed_at"] > recorded_by or row["bar_end"] > inputs.cutoff
            or row.get("replay_available_at") is not None and row["replay_available_at"] > recorded_by
        ):
            raise ValueError(f"Screening source identity/availability drift: {member['ticker']}")
        prices = np.array([row[key] for key in ("open", "high", "low", "close", "volume")], dtype=float)
        if (not np.isfinite(prices).all() or (prices[:4] <= 0).any() or prices[4] < 0
            or prices[1] < max(prices[0], prices[2], prices[3])
            or prices[2] > min(prices[0], prices[1], prices[3])):
            raise ValueError(f"Invalid screening bar: {member['ticker']}")
        if row["source_kind"] in {"DERIVED", "RECONCILED"} and not row["source_revision_ids"]:
            raise ValueError("Derived screening source lineage is missing")
        if row["source_kind"] == "DERIVED":
            sources = [indexed.get(str(source)) for source in row["source_revision_ids"]]
            if any(source is None for source in sources):
                raise ValueError("Derived screening causal source is missing")
            sources.sort(key=lambda source: source["bar_start"])
            if (sources[0]["bar_start"] != row["bar_start"] or sources[-1]["bar_end"] != row["bar_end"]
                or any(a["bar_end"] != b["bar_start"] for a, b in zip(sources, sources[1:]))
                or any(s["adjustment_basis"] != row["adjustment_basis"] for s in sources)):
                raise ValueError("Derived screening source window/basis drift")
            expected = [sources[0]["open"], max(s["high"] for s in sources),
                        min(s["low"] for s in sources), sources[-1]["close"], sum(s["volume"] for s in sources)]
            if not np.allclose(prices, expected, rtol=1e-8, atol=1e-8):
                raise ValueError("Derived screening source aggregate drift")


def produce_realtime_screening(inputs: RealtimeScreeningInput) -> RealtimeScreeningGeneration:
    if inputs.cutoff.tzinfo is None or inputs.cutoff.utcoffset() is None:
        raise ValueError("Screening cutoff must be timezone-aware")
    if len(inputs.source_manifest_sha256) != 64 or any(c not in "0123456789abcdef" for c in inputs.source_manifest_sha256):
        raise ValueError("Screening manifest hash is invalid")
    if type(inputs.source_generation) is not int or inputs.source_generation < 1:
        raise ValueError("Screening source generation must be positive")
    members = inputs.members
    bars_by_ticker = {}
    for bar in inputs.bars:
        bars_by_ticker.setdefault(bar["ticker"], []).append(bar)
    if not 0 < len(members) <= 1000 or len({str(m["security_id"]) for m in members}) != len(members) or len({m["ticker"] for m in members}) != len(members):
        raise ValueError("Full unique screening manifest membership is required")
    if set(bars_by_ticker) - {m["ticker"] for m in members}:
        raise ValueError("Screening bars extend beyond the exact manifest")
    calendar = exchange_calendars.get_calendar("XNYS")
    session = inputs.session
    sessions = [stamp.date() for stamp in calendar.sessions_in_range(calendar.session_offset(str(session), -282), str(session))]
    ordinals = {stamp: index for index, stamp in enumerate(sessions)}
    market_time = calendar.session_close(str(session)).to_pydatetime()
    if market_time > inputs.cutoff:
        raise ValueError("Screening session is not completed at cutoff")
    slots = hourly_slots(calendar, market_time)
    rows, lineage, hourly_lineage = [], {}, {}
    for member in sorted(members, key=lambda m: m["ticker"]):
        ticker, identity = member["ticker"], str(member["security_id"])
        if not member.get("security_revision_id") or member.get("security_type") not in {"CS", "ETF", "ETV"}:
            raise ValueError(f"Screening reference identity/type unavailable: {ticker}")
        if member["observed_at"] > inputs.cutoff:
            raise ValueError(f"Screening reference is not available: {ticker}")
        own = [dict(bar) for bar in bars_by_ticker.get(ticker, ())]
        _validate_bars(own, member, inputs)
        adjusted = sorted((b for b in own if b["interval"] == "1d" and b["adjustment_basis"] == "ADJUSTED"), key=lambda b: b["session_date"])
        raw = sorted((b for b in own if b["interval"] == "1d" and b["adjustment_basis"] == "RAW_UNADJUSTED"), key=lambda b: b["session_date"])
        if [b["session_date"] for b in adjusted] != sessions:
            raise ValueError(f"Screening adjusted history requires exact 283 retained sessions: {ticker}")
        if [b["session_date"] for b in raw[-20:]] != sessions[-20:]:
            raise ValueError(f"Screening nominal current/liquidity history unavailable: {ticker}")
        own_coverage = [c for c in inputs.coverage if str(c["security_id"]) == identity and c["action_type"] == "SPLIT"
                        and c["window_start"] <= sessions[0] and c["window_end"] >= session and c["received_at"] <= inputs.cutoff]
        if not own_coverage:
            raise ValueError(f"Screening split/action evidence unavailable: {ticker}")
        actions = [dict(a) for a in inputs.actions if str(a["security_id"]) == identity]
        if any(a["received_at"] > inputs.cutoff or a["ticker"] != ticker for a in actions):
            raise ValueError(f"Screening action evidence drift: {ticker}")
        if any(a["action_type"] == "SPLIT" and (
            not np.isfinite([float(a["split_from"]), float(a["split_to"])]).all()
            or float(a["split_from"]) <= 0 or float(a["split_to"]) <= 0
        ) for a in actions):
            raise ValueError(f"Invalid screening split evidence: {ticker}")
        own_coverage = [c for c in own_coverage if (
            c["response_action_count"] == sum(a["action_type"] == "SPLIT" and c["window_start"] <= a["effective_date"] <= c["window_end"] for a in actions)
            and all(a["received_at"] <= c["received_at"] for a in actions
                    if a["action_type"] == "SPLIT" and c["window_start"] <= a["effective_date"] <= c["window_end"])
        )]
        if not own_coverage:
            raise ValueError(f"Screening split/action coverage drift: {ticker}")
        split_dates = {a["effective_date"] for a in actions if a["action_type"] == "SPLIT"}
        for previous, current in zip(adjusted, adjusted[1:]):
            change = float(current["close"]) / float(previous["close"]) - 1
            if current["session_date"] in split_dates and (change <= -.38 or change >= .60):
                raise ValueError(f"Screening adjusted split/action drift: {ticker}")
        # The historical basis must meet the nominal current scale at its endpoint.
        endpoint_scale_difference = abs(
            float(raw[-1]["close"]) / float(adjusted[-1]["close"]) - 1
        )
        if endpoint_scale_difference > SCREENING_BASIS["current_scale_max_relative_difference"]:
            raise ValueError(f"Screening current adjusted/raw scale drift: {ticker}")
        historical = [dict(b) for b in adjusted[-253:]]
        nominal = {b["session_date"]: b for b in raw}
        for bar in historical:
            nominal_bar = nominal.get(bar["session_date"])
            bar["raw_close"] = float(nominal_bar["close"]) if nominal_bar else np.nan
            bar["raw_volume"] = float(nominal_bar["volume"]) if nominal_bar else np.nan
        selected = dict(member, selected_bar_revision_id=historical[-1]["bar_revision_id"], status="SELECTED")
        row = project_security(selected, historical, actions, session, ordinals, split_adjusted=True)
        row["source_bar_id"] = str(raw[-1]["bar_revision_id"])
        row["values"].update(price=float(raw[-1]["close"]), volume=float(raw[-1]["volume"]))
        row["quality"] = ["SPLIT_COVERAGE_VERIFIED_OTHER_KNOWN_ACTIONS_ONLY"]
        hourly = sorted((b for b in own if b["interval"] == "1h" and b["adjustment_basis"] == "RAW_UNADJUSTED"), key=lambda b: b["bar_end"])
        hourly = [b for b in hourly if slots[0][0] <= b["bar_start"] and b["bar_end"] <= market_time]
        if any(b["source_kind"] not in {"DERIVED", "RECONCILED"} for b in hourly):
            raise ValueError(f"Screening hourly context must use raw derived bars: {ticker}")
        if len({b["bar_end"] for b in hourly}) != len(hourly):
            raise ValueError(f"Screening hourly revisions are ambiguous: {ticker}")
        selected_hourly = next((b for b in hourly if b["bar_end"] == slots[-1][1]), None)
        row["hourly"] = project_hourly(identity, selected_hourly["bar_revision_id"] if selected_hourly else None, hourly, actions, slots)
        rows.append(row)
        lineage[identity] = _daily_lineage(member, row, adjusted, raw, actions, own_coverage)
        hourly_lineage[identity] = _hourly_lineage(hourly, selected_hourly)
    equity_dir = Path(__file__).resolve().parents[1] / "equity"
    manifest = dict(
        session=str(session), market_time=market_time.isoformat(), observed_at=inputs.cutoff.isoformat(),
        source_cutoff=inputs.cutoff.isoformat(), source_publication_id=inputs.source_manifest_sha256,
        source_manifest_sha256=inputs.source_manifest_sha256, cohort_id=str(inputs.cohort_id),
        provider_tier=inputs.provider_tier.value, source_generation=inputs.source_generation,
        previous_expected_session=str(calendar.previous_session(str(session)).date()),
        expected_members=len(members), source_selected_members=len(members),
        source_publication_status="COMPLETE", source_unavailable_members=[],
        daily_state_contract="daily_state_extracted_v1", screening_basis=SCREENING_BASIS,
        field_contract=REALTIME_FIELDS,
        code_hashes={name: digest((equity_dir / name).read_text(encoding="utf-8")) for name in ("screening_projection.py", "technicals.py", "daily_state.py")},
        gap_code_hashes={name: digest((equity_dir / name).read_text(encoding="utf-8")) for name in ("screening_gaps.py", "gap_detection.py")},
        contract_hash=digest(dict(fields=REALTIME_FIELDS, patterns=PATTERNS, basis=SCREENING_BASIS)),
        projection_code_sha256=digest(Path(__file__).read_text(encoding="utf-8")),
        input_sha256=inputs_sha256(inputs), lineage_contract=dict(version=SCREENING_LINEAGE_VERSION),
    )
    payload = finalize(rows, manifest, lineage)
    payload.update(
        gap_contract=REALTIME_GAP_CATALOG,
        action_coverage="SPLIT_COVERAGE_VERIFIED_OTHER_KNOWN_ACTIONS_ONLY",
        hourly_contract=HOURLY_CATALOG, hourly_lineage=hourly_lineage,
        hourly_source=dict(
            status="READY" if all(r["hourly"]["values"]["close"] is not None for r in rows) else "UNAVAILABLE",
            market_time=market_time.isoformat(), expected_market_time=market_time.isoformat(),
            source_cutoff=inputs.cutoff.isoformat(), source_publication_id=inputs.source_manifest_sha256,
            slot_start=slots[-1][0].isoformat(), slot_minutes=int((slots[-1][1] - slots[-1][0]).total_seconds() / 60),
            snapshot_cutoff=inputs.cutoff.isoformat(), price_basis="RAW_ACTION_GATED",
            code_hash=digest((equity_dir / "screening_hourly.py").read_text(encoding="utf-8")),
        ),
        hourly_coverage={name: sum(r["hourly"]["values"][name] is not None for r in rows) for name in HOURLY_FIELDS},
    )
    payload.pop("generation")
    payload["generation"] = digest(payload)
    manifest = require_bounded_screening_manifest(
        {name: value for name, value in payload.items() if name != "rows"}, members=len(members),
    )
    return build_screening_generation(
        cohort_id=inputs.cohort_id, provider_tier=inputs.provider_tier, session_date=session,
        expected_members=len(members), generated_at=inputs.cutoff,
        manifest=manifest,
        rows=tuple((UUID(r["security_id"]), r["ticker"], r["eligible"], r) for r in rows),
    )
