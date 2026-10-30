"""Exact immutable H binding for Advanced Options V3."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from uuid import UUID

from options.calendar import OptionExchangeCalendar

from .advanced_options_control_plane import (
    AdvancedOptionsHistoryBinding,
    advanced_options_history_binding_payload,
    build_advanced_options_history_binding,
)
from .contracts import canonical_sha256


UTC = timezone.utc
ADVANCED_OPTIONS_V3_HISTORY_EVIDENCE_SCHEMA = (
    "advanced-options-v3-history-binding-evidence-v1"
)
ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID = UUID(
    "d1baee6f-4ab2-543c-8a86-ce10f8952e69"
)
ADVANCED_OPTIONS_V3_HISTORY_SOURCE_MANIFEST_SHA256 = (
    "bed64f01df51057eab37239c312beedf8969f31a00e80b7882c7b59c50367749"
)
ADVANCED_OPTIONS_V3_HISTORY_SECURITY_IDS = {
    "QQQ": "5f2f5008-d84a-5b5c-b500-2d71f4389e4a",
    "SPY": "50138fad-5b52-5812-8648-d920f8868528",
}
ADVANCED_OPTIONS_V3_HISTORY_UNDERLYINGS = ("QQQ", "SPY")
ADVANCED_OPTIONS_V3_HISTORY_SESSION_COUNT = 31
ADVANCED_OPTIONS_V3_RAW_HISTORY_SESSION_COUNT = 15


class AdvancedOptionsHistoryBindingIdentityError(ValueError):
    """The immutable H source or binding identity is absent/divergent."""


class AdvancedOptionsHistoryCoverageError(ValueError):
    """The immutable H source identity exists but exact coverage is missing."""


@dataclass(frozen=True, slots=True)
class VerifiedAdvancedOptionsHistoryBinding:
    binding: AdvancedOptionsHistoryBinding
    evidence: dict[str, object]


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _source_identity(cursor):
    cursor.execute(
        "SELECT cohort.cohort_id::text,manifest.manifest_id::text,"
        "manifest.manifest_sha256,profile.provider_tier "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
        "JOIN rt.capability_profiles AS profile USING(profile_id) "
        "WHERE cohort.cohort_id=%s",
        (str(ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID),),
    )
    row = cursor.fetchone()
    if row is None:
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding source cohort is absent"
        )
    values = (
        str(row["cohort_id"]),
        str(row["manifest_id"]),
        str(row["manifest_sha256"]),
        str(row["provider_tier"]),
    ) if isinstance(row, Mapping) else tuple(str(value) for value in row)
    if (
        values[0] != str(ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID)
        or values[2]
        != ADVANCED_OPTIONS_V3_HISTORY_SOURCE_MANIFEST_SHA256
        or values[3] != "MASSIVE_DEVELOPER"
    ):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding source manifest/provider identity diverged"
        )
    return {
        "cohort_id": values[0],
        "manifest_id": values[1],
        "manifest_sha256": values[2],
        "provider_tier": values[3],
    }


def _manifest_member_identity(cursor, manifest_id: str):
    cursor.execute(
        "SELECT member.instrument_id,member.channel,member.priority,"
        "member.purpose,member.option_underlying "
        "FROM rt.subscription_members AS member "
        "WHERE member.manifest_id=%s "
        "ORDER BY member.channel,member.instrument_id",
        (manifest_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H source manifest members are absent"
        )
    members = tuple(
        (
            int(row["instrument_id"]),
            str(row["channel"]),
            int(row["priority"]),
            str(row["purpose"]),
            bool(row["option_underlying"]),
        )
        if isinstance(row, Mapping)
        else (
            int(row[0]),
            str(row[1]),
            int(row[2]),
            str(row[3]),
            bool(row[4]),
        )
        for row in rows
    )
    return {
        "member_count": len(members),
        "member_sha256": canonical_sha256(members),
    }


def _coverage(cursor, sessions: tuple[date, ...], known_at: datetime):
    cursor.execute(
        "SELECT revision.ticker,revision.security_id::text,"
        "revision.session_date,current.adjustment_basis,"
        "revision.bar_revision_id::text,"
        "revision.observed_at,revision.payload_sha256 "
        "FROM rt.bar_current AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "WHERE current.cohort_id=%s AND current.interval='1d' "
        "AND current.adjustment_basis=ANY(%s) "
        "AND revision.is_final AND revision.ticker=ANY(%s) "
        "AND revision.session_date=ANY(%s) "
        "AND revision.observed_at<=%s "
        "ORDER BY revision.ticker,current.adjustment_basis,"
        "revision.session_date",
        (
            str(ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID),
            ["ADJUSTED", "RAW_UNADJUSTED"],
            list(ADVANCED_OPTIONS_V3_HISTORY_UNDERLYINGS),
            list(sessions),
            known_at,
        ),
    )
    grouped = defaultdict(list)
    for raw in cursor.fetchall():
        row = dict(raw) if isinstance(raw, Mapping) else {
            "ticker": raw[0],
            "security_id": raw[1],
            "session_date": raw[2],
            "adjustment_basis": raw[3],
            "bar_revision_id": raw[4],
            "observed_at": raw[5],
            "payload_sha256": raw[6],
        }
        grouped[
            (str(row["ticker"]), str(row["adjustment_basis"]))
        ].append(row)
    symbols = {}
    missing = {}
    for ticker in ADVANCED_OPTIONS_V3_HISTORY_UNDERLYINGS:
        expected_by_basis = {
            "ADJUSTED": sessions,
            "RAW_UNADJUSTED": sessions[
                -ADVANCED_OPTIONS_V3_RAW_HISTORY_SESSION_COUNT:
            ],
        }
        members_by_basis = {}
        for basis, expected in expected_by_basis.items():
            members_by_basis[basis] = tuple(
                row
                for row in grouped.get((ticker, basis), ())
                if row["session_date"] in expected
            )
        for basis, expected in expected_by_basis.items():
            found = tuple(
                row["session_date"]
                for row in members_by_basis[basis]
            )
            missing_sessions = tuple(
                session for session in expected
                if session not in found
            )
            if missing_sessions:
                missing[f"{ticker}:{basis}"] = tuple(
                    session.isoformat()
                    for session in missing_sessions
                )
            elif (
                found != expected
                or len(members_by_basis[basis]) != len(expected)
            ):
                raise AdvancedOptionsHistoryBindingIdentityError(
                    f"V3 H {ticker} {basis} session membership diverged"
                )
        if any(key.startswith(f"{ticker}:") for key in missing):
            continue
        members = tuple(
            row
            for basis in ("ADJUSTED", "RAW_UNADJUSTED")
            for row in members_by_basis[basis]
        )
        security_ids = {str(row["security_id"]) for row in members}
        if security_ids != {
            ADVANCED_OPTIONS_V3_HISTORY_SECURITY_IDS[ticker]
        }:
            raise AdvancedOptionsHistoryBindingIdentityError(
                f"V3 H {ticker} security identity diverged"
            )
        symbols[ticker] = {
            "security_id": next(iter(security_ids)),
            "adjusted_session_count": len(
                members_by_basis["ADJUSTED"]
            ),
            "raw_session_count": len(
                members_by_basis["RAW_UNADJUSTED"]
            ),
            "member_sha256": canonical_sha256({
                basis: tuple(
                    (
                        str(row["bar_revision_id"]),
                        row["session_date"],
                        str(row["payload_sha256"]),
                    )
                    for row in members_by_basis[basis]
                )
                for basis in ("ADJUSTED", "RAW_UNADJUSTED")
            }),
            "latest_known_at_utc": max(
                row["observed_at"] for row in members
            ).isoformat(),
        }
    if missing:
        raise AdvancedOptionsHistoryCoverageError(
            "V3 H adjusted/raw daily coverage is missing: "
            + canonical_sha256(missing)
        )
    return symbols


def load_verified_advanced_options_v3_history_binding_cursor(
    cursor,
    *,
    target_cohort_id: UUID,
    session_date: date,
    known_at: datetime,
    authorization_reference: str,
) -> VerifiedAdvancedOptionsHistoryBinding:
    checked = _utc(known_at, "known_at")
    if (
        not isinstance(target_cohort_id, UUID)
        or target_cohort_id.int == 0
        or target_cohort_id
        == ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID
    ):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H target/source cohort identity diverged"
        )
    calendar = OptionExchangeCalendar()
    if calendar.session_on_or_after(session_date) != session_date:
        raise ValueError("V3 H session date must be an XNYS session")
    sessions = calendar.trailing_sessions_before(
        session_date,
        ADVANCED_OPTIONS_V3_HISTORY_SESSION_COUNT,
    )
    source = _source_identity(cursor)
    manifest_members = _manifest_member_identity(
        cursor,
        source["manifest_id"],
    )
    symbols = _coverage(cursor, sessions, checked)
    history_before = datetime.combine(
        session_date,
        time(13, 30),
        tzinfo=UTC,
    )
    evidence = {
        "schema": ADVANCED_OPTIONS_V3_HISTORY_EVIDENCE_SCHEMA,
        "target_cohort_id": str(target_cohort_id),
        "source_cohort_id": source["cohort_id"],
        "source_provider_tier": source["provider_tier"],
        "session_date": session_date.isoformat(),
        "history_before_utc": history_before.isoformat(),
        "known_at_utc": checked.isoformat(),
        "source_manifest": {
            "manifest_id": source["manifest_id"],
            "manifest_sha256": source["manifest_sha256"],
            **manifest_members,
        },
        "coverage": {
            "underlyings": list(
                ADVANCED_OPTIONS_V3_HISTORY_UNDERLYINGS
            ),
            "interval": "1d",
            "adjustment_basis": [
                "ADJUSTED",
                "RAW_UNADJUSTED",
            ],
            "session_count": ADVANCED_OPTIONS_V3_HISTORY_SESSION_COUNT,
            "adjusted_session_count": (
                ADVANCED_OPTIONS_V3_HISTORY_SESSION_COUNT
            ),
            "raw_session_count": (
                ADVANCED_OPTIONS_V3_RAW_HISTORY_SESSION_COUNT
            ),
            "sessions": [
                session.isoformat() for session in sessions
            ],
            "raw_daily_sessions_required": (
                ADVANCED_OPTIONS_V3_RAW_HISTORY_SESSION_COUNT
            ),
            "raw_sessions": [
                session.isoformat()
                for session in sessions[
                    -ADVANCED_OPTIONS_V3_RAW_HISTORY_SESSION_COUNT:
                ]
            ],
            "raw_members_bound": list(
                ADVANCED_OPTIONS_V3_HISTORY_UNDERLYINGS
            ),
            "symbols": symbols,
        },
        "mixed_history_reconstruction": False,
        "rows_copied_to_target": 0,
    }
    evidence["evidence_sha256"] = canonical_sha256(evidence)
    binding = build_advanced_options_history_binding(
        target_cohort_id=target_cohort_id,
        source_cohort_id=ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID,
        history_before=history_before,
        source_manifest_sha256=source["manifest_sha256"],
        authorization_reference=authorization_reference,
        evidence_sha256=evidence["evidence_sha256"],
    )
    return VerifiedAdvancedOptionsHistoryBinding(binding, evidence)


def verify_advanced_options_v3_history_binding_cursor(
    cursor,
    *,
    binding: AdvancedOptionsHistoryBinding,
    evidence: object,
) -> VerifiedAdvancedOptionsHistoryBinding:
    if not isinstance(evidence, dict):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding evidence is absent"
        )
    digest = evidence.get("evidence_sha256")
    unsigned = dict(evidence)
    unsigned.pop("evidence_sha256", None)
    if (
        evidence.get("schema")
        != ADVANCED_OPTIONS_V3_HISTORY_EVIDENCE_SCHEMA
        or digest != canonical_sha256(unsigned)
        or binding.evidence_sha256 != digest
        or advanced_options_history_binding_payload(binding).get(
            "evidence_sha256"
        )
        != digest
    ):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding evidence identity diverged"
        )
    rebuilt = load_verified_advanced_options_v3_history_binding_cursor(
        cursor,
        target_cohort_id=binding.target_cohort_id,
        session_date=date.fromisoformat(str(evidence["session_date"])),
        known_at=datetime.fromisoformat(str(evidence["known_at_utc"])),
        authorization_reference=binding.authorization_reference,
    )
    if (
        rebuilt.binding != binding
        or rebuilt.evidence != evidence
    ):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding or exact member clocks diverged"
        )
    return rebuilt


def advanced_options_v3_history_binding_from_payload(
    value: object,
) -> AdvancedOptionsHistoryBinding:
    if not isinstance(value, Mapping):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding payload is absent"
        )
    try:
        binding = build_advanced_options_history_binding(
            target_cohort_id=UUID(str(value["target_cohort_id"])),
            source_cohort_id=UUID(str(value["source_cohort_id"])),
            history_before=datetime.fromisoformat(
                str(value["history_before"])
            ),
            source_manifest_sha256=str(
                value["source_manifest_sha256"]
            ),
            authorization_reference=str(
                value["authorization_reference"]
            ),
            evidence_sha256=str(value["evidence_sha256"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding payload is invalid"
        ) from error
    if advanced_options_history_binding_payload(binding) != dict(value):
        raise AdvancedOptionsHistoryBindingIdentityError(
            "V3 H binding payload identity diverged"
        )
    return binding
