from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.stage_runners_advanced_equity import (
    advanced_input_gate_identity,
)


NOW = datetime(2026, 10, 16, 16, 30, tzinfo=timezone.utc)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
WINDOW = UUID("22222222-2222-2222-2222-222222222222")
MANIFEST = UUID("33333333-3333-3333-3333-333333333333")
SESSION = UUID("44444444-4444-4444-4444-444444444444")
GENERATION = UUID("55555555-5555-5555-5555-555555555555")


class Database:
    def __init__(self, gap_count=0, *, hybrid=False):
        self.cursor = MagicMock()
        subscribed_at = NOW - timedelta(
            minutes=4 if hybrid else 5
        )
        self.cursor.fetchone.side_effect = (
            (MANIFEST, 1, "MASSIVE_ADVANCED"),
            (
                SESSION,
                GENERATION,
                7,
                "LIVE",
                subscribed_at,
                None,
                NOW,
                1_000,
                10_000,
            ),
            (gap_count,),
        )
        bars = tuple(
            (
                11,
                NOW - timedelta(minutes=5 - index),
                UUID(int=100 + index),
                f"{index + 1:064x}",
                (
                    "NATIVE_REST"
                    if hybrid and index == 0
                    else "REALTIME_STREAM"
                ),
                (
                    ("EXPLICIT_ADVANCED_REST_REPAIR_V1",)
                    if hybrid and index == 0
                    else ()
                ),
                None if hybrid and index == 0 else GENERATION,
                None if hybrid and index == 0 else SESSION,
                None if hybrid and index == 0 else index + 1,
            )
            for index in range(5)
        )
        self.cursor.fetchall.side_effect = (
            ((11,),),
            ((GENERATION, 7, 100, str(SESSION), NOW),),
            bars,
        )

    def transaction(self, **_options):
        return nullcontext(self.cursor)


def _context():
    return SimpleNamespace(
        plan=SimpleNamespace(cohort_id=COHORT),
        window=SimpleNamespace(
            window_id=WINDOW,
            cutoff=NOW,
        ),
    )


def test_advanced_input_gate_binds_session_checkpoint_and_budget():
    digest = advanced_input_gate_identity(Database(), _context())

    assert len(digest) == 64


def test_advanced_input_gate_accepts_explicit_repair_before_subscription():
    digest = advanced_input_gate_identity(
        Database(hybrid=True),
        _context(),
    )

    assert len(digest) == 64


def test_advanced_input_gate_refuses_an_overlapping_gap():
    with pytest.raises(ValueError, match="unresolved"):
        advanced_input_gate_identity(Database(gap_count=1), _context())
