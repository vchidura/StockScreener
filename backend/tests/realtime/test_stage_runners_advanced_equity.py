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
SESSION_2 = UUID("66666666-6666-6666-6666-666666666666")
GENERATION_2 = UUID("77777777-7777-7777-7777-777777777777")
INGRESS_PLAN = UUID("88888888-8888-8888-8888-888888888888")
REPAIR_SEGMENT = UUID("99999999-9999-9999-9999-999999999999")
REPAIR_REQUEST = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SHARD_MAP = "b" * 64


class Database:
    def __init__(
        self,
        gap_count=0,
        *,
        hybrid=False,
        multi_generation=False,
        bar_payload_suffix=0,
        checkpoint_state_hash=SHARD_MAP,
    ):
        self.cursor = MagicMock()
        subscribed_at = NOW - timedelta(
            minutes=4 if hybrid else 5
        )
        self.cursor.fetchone.side_effect = (
            (MANIFEST, 1, "MASSIVE_ADVANCED"),
            (gap_count,),
        )
        bars = tuple(
            (
                11,
                NOW - timedelta(minutes=5 - index),
                NOW - timedelta(minutes=4 - index),
                UUID(int=100 + index),
                f"{index + 1 + bar_payload_suffix:064x}",
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
                (
                    None
                    if hybrid and index == 0
                    else GENERATION_2
                    if multi_generation and index >= 2
                    else GENERATION
                ),
                (
                    None
                    if hybrid and index == 0
                    else SESSION_2
                    if multi_generation and index >= 2
                    else SESSION
                ),
                (
                    None
                    if hybrid and index == 0
                    else index - 1
                    if multi_generation and index >= 2
                    else index if hybrid
                    else index + 1
                ),
                REPAIR_SEGMENT if hybrid and index == 0 else None,
            )
            for index in range(5)
        )
        if multi_generation:
            sessions = (
                (
                    SESSION,
                    GENERATION,
                    7,
                    "STOPPED",
                    subscribed_at,
                    NOW - timedelta(minutes=3),
                    NOW - timedelta(minutes=3),
                    500,
                    10_000,
                    INGRESS_PLAN,
                ),
                (
                    SESSION_2,
                    GENERATION_2,
                    8,
                    "LIVE",
                    NOW - timedelta(minutes=3),
                    None,
                    NOW,
                    500,
                    10_000,
                    INGRESS_PLAN,
                ),
            )
            journals = (
                (
                    GENERATION,
                    SESSION,
                    7,
                    INGRESS_PLAN,
                    "DRAINING",
                    (0,),
                    SHARD_MAP,
                ),
                (
                    GENERATION_2,
                    SESSION_2,
                    8,
                    INGRESS_PLAN,
                    "WRITING",
                    (0,),
                    SHARD_MAP,
                ),
            )
            checkpoints = (
                (
                    GENERATION,
                    7,
                    2,
                    str(SESSION),
                    NOW - timedelta(minutes=3),
                    0,
                    SHARD_MAP,
                    UUID(int=800),
                    checkpoint_state_hash,
                ),
                (
                    GENERATION_2,
                    8,
                    3,
                    str(SESSION_2),
                    NOW,
                    0,
                    SHARD_MAP,
                    UUID(int=801),
                    checkpoint_state_hash,
                ),
            )
        else:
            sessions = ((
                SESSION,
                GENERATION,
                7,
                "LIVE",
                subscribed_at,
                None,
                NOW,
                1_000,
                10_000,
                INGRESS_PLAN,
            ),)
            journals = ((
                GENERATION,
                SESSION,
                7,
                INGRESS_PLAN,
                "WRITING",
                (0,),
                SHARD_MAP,
            ),)
            checkpoints = ((
                GENERATION,
                7,
                4 if hybrid else 5,
                str(SESSION),
                NOW,
                0,
                SHARD_MAP,
                UUID(int=802),
                checkpoint_state_hash,
            ),)
        results = [
            ((11,),),
            bars,
            sessions,
            journals,
            checkpoints,
        ]
        if hybrid:
            results.append(((
                REPAIR_SEGMENT,
                REPAIR_REQUEST,
                "COMPLETE",
                COHORT,
                INGRESS_PLAN,
                "REPAIRED",
                "REPAIRED",
            ),))
        self.cursor.fetchall.side_effect = tuple(results)

    def transaction(self, **_options):
        return nullcontext(self.cursor)


def _context(*, window_id=WINDOW):
    return SimpleNamespace(
        plan=SimpleNamespace(cohort_id=COHORT),
        window=SimpleNamespace(
            window_id=window_id,
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


def test_advanced_input_gate_accepts_two_fenced_generations():
    digest = advanced_input_gate_identity(
        Database(multi_generation=True),
        _context(),
    )

    assert len(digest) == 64


def test_advanced_input_gate_digest_is_lineage_not_retry_window_identity():
    first = advanced_input_gate_identity(Database(), _context())
    retry = advanced_input_gate_identity(
        Database(),
        _context(window_id=UUID(int=999)),
    )

    assert retry == first


def test_advanced_input_gate_digest_changes_with_bar_or_checkpoint():
    baseline = advanced_input_gate_identity(Database(), _context())
    changed_bar = advanced_input_gate_identity(
        Database(bar_payload_suffix=10),
        _context(),
    )
    changed_checkpoint = advanced_input_gate_identity(
        Database(checkpoint_state_hash="c" * 64),
        _context(),
    )

    assert changed_bar != baseline
    assert changed_checkpoint != baseline


def test_advanced_input_gate_refuses_an_overlapping_gap():
    with pytest.raises(ValueError, match="unresolved"):
        advanced_input_gate_identity(Database(gap_count=1), _context())
