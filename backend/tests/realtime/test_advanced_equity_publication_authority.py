from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from realtime.advanced_equity_publication_authority import (
    advanced_equity_publication_authority_from_payload,
    advanced_equity_publication_authority_payload,
    build_advanced_equity_publication_authority,
    validate_advanced_equity_publication_authority,
)
from realtime.orchestration_repository import CycleReceiptSpec


UTC = timezone.utc
COHORT = UUID("11111111-2222-3333-4444-555555555555")
SESSION = date(2026, 10, 23)
CUTOFF = datetime(2026, 10, 23, 19, 20, tzinfo=UTC)
SUFFIX = "-retry1"


def _receipt(kind):
    return CycleReceiptSpec(
        kind,
        CUTOFF,
        7,
        "a" * 64,
        "b" * 64,
        "advanced-equity-window-v1",
    )


def _authority():
    window_key = f"advanced-{CUTOFF:%Y%m%dT%H%M%SZ}{SUFFIX}"
    return build_advanced_equity_publication_authority(
        authorization_reference="P11-TEST",
        cohort_id=COHORT,
        session_date=SESSION,
        cutoff=CUTOFF,
        target_window_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:window:{COHORT}:{window_key}",
        ),
        window_key_suffix=SUFFIX,
        input_receipt=_receipt("INPUT_READY"),
        portal_receipt=_receipt("PORTAL_READY"),
        expected_prior_release_id=None,
        expected_prior_cutoff=None,
        prepared_at=CUTOFF + timedelta(minutes=1),
        expires_at=CUTOFF + timedelta(hours=1),
    )


def test_publication_authority_round_trips_and_matches_receipts():
    authority = _authority()
    payload = advanced_equity_publication_authority_payload(authority)

    assert (
        advanced_equity_publication_authority_from_payload(payload)
        == authority
    )
    validate_advanced_equity_publication_authority(
        authority,
        cohort_id=COHORT,
        session_date=SESSION,
        cutoff=CUTOFF,
        window_key_suffix=SUFFIX,
        input_receipt=_receipt("INPUT_READY"),
        portal_receipt=_receipt("PORTAL_READY"),
        prior_release_id=None,
        prior_cutoff=None,
        checked_at=CUTOFF + timedelta(minutes=2),
    )


def test_publication_authority_rejects_scope_or_receipt_drift():
    authority = _authority()
    with pytest.raises(ValueError, match="durable publication scope"):
        validate_advanced_equity_publication_authority(
            authority,
            cohort_id=COHORT,
            session_date=SESSION,
            cutoff=CUTOFF,
            window_key_suffix="-retry2",
            input_receipt=_receipt("INPUT_READY"),
            portal_receipt=_receipt("PORTAL_READY"),
            prior_release_id=None,
            prior_cutoff=None,
            checked_at=CUTOFF + timedelta(minutes=2),
        )

    changed = replace(
        _receipt("PORTAL_READY"),
        input_sha256="c" * 64,
    )
    with pytest.raises(ValueError, match="durable publication scope"):
        validate_advanced_equity_publication_authority(
            authority,
            cohort_id=COHORT,
            session_date=SESSION,
            cutoff=CUTOFF,
            window_key_suffix=SUFFIX,
            input_receipt=_receipt("INPUT_READY"),
            portal_receipt=changed,
            prior_release_id=None,
            prior_cutoff=None,
            checked_at=CUTOFF + timedelta(minutes=2),
        )


def test_publication_authority_has_no_alert_or_platform_authority():
    authority = _authority()

    assert authority.maximum_pointer_advances == 1
    assert authority.platform_pointer_authorized is False
    assert authority.alerts_authorized is False
    assert authority.strategies_authorized is False
