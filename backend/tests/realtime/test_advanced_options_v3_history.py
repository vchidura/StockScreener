from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import UUID, uuid5, NAMESPACE_URL

import pytest
from options.calendar import OptionExchangeCalendar

from realtime.advanced_options_v3_history import (
    ADVANCED_OPTIONS_V3_HISTORY_SECURITY_IDS,
    ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID,
    ADVANCED_OPTIONS_V3_HISTORY_SOURCE_MANIFEST_SHA256,
    AdvancedOptionsHistoryBindingIdentityError,
    AdvancedOptionsHistoryCoverageError,
    advanced_options_v3_history_source_configuration,
    advanced_options_v3_history_binding_from_payload,
    load_verified_advanced_options_v3_history_binding_cursor,
    verify_advanced_options_v3_history_binding_cursor,
)
from realtime.contracts import canonical_sha256
from realtime.advanced_options_control_plane import (
    advanced_options_history_binding_payload,
)


UTC = timezone.utc
SESSION_DATE = date(2026, 10, 23)
KNOWN_AT = datetime(2026, 10, 23, 10, 0, tzinfo=UTC)
TARGET_COHORT_ID = UUID("11111111-2222-3333-4444-555555555555")


class Cursor:
    def __init__(
        self,
        *,
        missing=None,
        manifest_sha256=None,
        include_older_raw=False,
    ):
        self.missing = missing
        self.include_older_raw = include_older_raw
        self.manifest_sha256 = (
            manifest_sha256
            or ADVANCED_OPTIONS_V3_HISTORY_SOURCE_MANIFEST_SHA256
        )
        self.rows = []

    def execute(self, statement, _parameters):
        if "FROM rt.cohorts AS cohort" in statement:
            self.rows = [{
                "cohort_id": str(
                    ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID
                ),
                "manifest_id": (
                    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
                ),
                "manifest_sha256": self.manifest_sha256,
                "provider_tier": "MASSIVE_DEVELOPER",
            }]
            return
        if "FROM rt.subscription_members AS member" in statement:
            self.rows = [
                {
                    "instrument_id": index,
                    "channel": "AM",
                    "priority": index,
                    "purpose": "PRIMARY",
                    "option_underlying": False,
                }
                for index in (1, 2)
            ]
            return
        if "FROM rt.bar_head AS current" in statement:  # G102 A4: the cohort-free head
            sessions = OptionExchangeCalendar().trailing_sessions_before(
                SESSION_DATE,
                31,
            )
            self.rows = [
                {
                    "ticker": ticker,
                    "security_id": (
                        ADVANCED_OPTIONS_V3_HISTORY_SECURITY_IDS[ticker]
                    ),
                    "session_date": session,
                    "adjustment_basis": basis,
                    "bar_revision_id": str(uuid5(
                        NAMESPACE_URL,
                        f"{ticker}:{basis}:{session}",
                    )),
                    "observed_at": KNOWN_AT,
                    "payload_sha256": canonical_sha256(
                        (ticker, basis, session)
                    ),
                }
                for ticker in ("QQQ", "SPY")
                for basis, selected_sessions in (
                    ("ADJUSTED", sessions),
                    (
                        "RAW_UNADJUSTED",
                        sessions
                        if self.include_older_raw
                        else sessions[-15:],
                    ),
                )
                for session in selected_sessions
                if (ticker, basis, session) != self.missing
            ]
            return
        raise AssertionError(statement)

    def fetchone(self):
        return None if not self.rows else self.rows[0]

    def fetchall(self):
        return list(self.rows)


def _load(cursor=None):
    return load_verified_advanced_options_v3_history_binding_cursor(
        cursor or Cursor(),
        target_cohort_id=TARGET_COHORT_ID,
        session_date=SESSION_DATE,
        known_at=KNOWN_AT,
        authorization_reference="V3-H-TEST",
    )


def test_exact_adjusted_h_binding_freezes_members_and_clocks():
    verified = _load()

    assert verified.binding.source_cohort_id == (
        ADVANCED_OPTIONS_V3_HISTORY_SOURCE_COHORT_ID
    )
    assert verified.binding.source_manifest_sha256 == (
        ADVANCED_OPTIONS_V3_HISTORY_SOURCE_MANIFEST_SHA256
    )
    assert verified.binding.evidence_sha256 == (
        verified.evidence["evidence_sha256"]
    )
    assert verified.evidence["coverage"]["session_count"] == 31
    assert verified.evidence["coverage"][
        "adjustment_basis"
    ] == ["ADJUSTED", "RAW_UNADJUSTED"]
    assert verified.evidence["coverage"][
        "raw_daily_sessions_required"
    ] == 15
    assert verified.evidence["coverage"]["raw_members_bound"] == [
        "QQQ",
        "SPY",
    ]
    assert set(verified.evidence["coverage"]["symbols"]) == {
        "QQQ",
        "SPY",
    }
    assert verified.evidence["rows_copied_to_target"] == 0
    assert verified.evidence["mixed_history_reconstruction"] is False
    assert verified.evidence["readiness"] == {
        "status": "READY",
        "required_through_session": "2026-10-22",
        "latest_covered_session": "2026-10-22",
        "known_at_utc": KNOWN_AT.isoformat(),
        "refresh_policy": "REQUIRED_THROUGH_PREVIOUS_XNYS_SESSION",
    }
    source = advanced_options_v3_history_source_configuration()
    assert verified.evidence["source_configuration"] == source
    assert source["source_provider_tier"] == "MASSIVE_DEVELOPER"
    assert source["copy_into_target"] is False
    assert advanced_options_v3_history_binding_from_payload(
        advanced_options_history_binding_payload(verified.binding)
    ) == verified.binding


def test_older_raw_history_does_not_expand_exact_h_membership():
    expected = _load()
    with_older_raw = _load(Cursor(include_older_raw=True))

    assert with_older_raw == expected


def test_missing_h_coverage_is_not_reported_as_identity_drift():
    missing = OptionExchangeCalendar().trailing_sessions_before(
        SESSION_DATE,
        31,
    )[0]

    with pytest.raises(
        AdvancedOptionsHistoryCoverageError,
        match="adjusted/raw daily coverage is missing",
    ):
        _load(Cursor(missing=("QQQ", "ADJUSTED", missing)))


def test_h_manifest_divergence_is_an_identity_defect():
    with pytest.raises(
        AdvancedOptionsHistoryBindingIdentityError,
        match="manifest/provider identity diverged",
    ):
        _load(Cursor(manifest_sha256="f" * 64))


def test_persisted_h_evidence_rejects_member_hash_drift():
    verified = _load()
    changed = {
        **verified.evidence,
        "coverage": {
            **verified.evidence["coverage"],
            "symbols": {
                **verified.evidence["coverage"]["symbols"],
                "QQQ": {
                    **verified.evidence["coverage"]["symbols"]["QQQ"],
                    "member_sha256": "f" * 64,
                },
            },
        },
    }
    unsigned = dict(changed)
    unsigned.pop("evidence_sha256")
    changed["evidence_sha256"] = canonical_sha256(unsigned)

    with pytest.raises(
        AdvancedOptionsHistoryBindingIdentityError,
        match="evidence identity diverged",
    ):
        verify_advanced_options_v3_history_binding_cursor(
            Cursor(),
            binding=verified.binding,
            evidence=changed,
        )
