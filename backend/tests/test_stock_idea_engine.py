from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.stock_idea_engine import Candidate, select_candidates


NOW = datetime(2026, 8, 3, 14, 17, tzinfo=timezone.utc)


def candidate(security="A", model="resumption", **changes):
    base = Candidate(security, security, model, "30m", 1, "INTRADAY", "anchor-one",
                     NOW - timedelta(minutes=17), NOW + timedelta(minutes=13),
                     NOW - timedelta(minutes=2), 100., 98., 110., 2., 100., .9,
                     .1, 5., 30_000_000., .3, ("revision-one",))
    return replace(base, **changes)


def choose(rows, **kwargs):
    return select_candidates(rows, deadline=NOW, expected_members={row.security_id for row in rows},
                             window_key="2026-08-03T14:00:00Z", **kwargs)


def test_pooled_distinct_stock_quota_and_no_refill_after_display_dedup():
    rows = [candidate(security, model, liquidity=100 - index)
            for model in ("resumption", "acceptance", "failure")
            for index, security in enumerate(("A", "B", "C", "D"))]
    rows.append(candidate("A", interval="1h"))
    result = choose(rows)
    assert len(result["selected"]) == 9
    assert len(result["display"]) == 3
    assert {item["security_id"] for item in result["display"]} == {"A", "B", "C"}
    assert all(len(item["episode_ids"]) == 3 for item in result["display"])
    assert result == choose(list(reversed(rows)))


def test_conflicts_before_quota_countertrend_context_and_discovery_separation():
    long = candidate()
    short = candidate(model="failure", direction=-1, stop=102., target=90.)
    assert not choose([long, short])["selected"]
    daily = replace(short, interval="1d", horizon="DAILY_5")
    result = choose([long, daily, candidate(model="discovery")])
    assert len(result["selected"]) == 3
    assert any(row["context"] for row in result["dispositions"])
    assert {row["lane"] for row in result["display"]} == {"TRADE", "WATCH"}


def test_stable_identity_and_no_forced_or_stale_alerts():
    original = candidate()
    assert replace(original, price=100.1, revision_ids=("correction",), reference=100.001).episode_id == original.episode_id
    assert choose([])["display"] == []
    assert not choose([original], already_selected=[original.episode_id])["selected"]
    assert not choose([replace(original, expires_at=NOW - timedelta(seconds=1))])["selected"]
    assert not choose([replace(original, available_at=NOW + timedelta(seconds=1))])["selected"]
    assert len(choose([original, original])["selected"]) == 1


def test_episode_requires_termination_two_valid_resets_and_a_later_trigger():
    from research.stock_idea_engine import advance_episode
    original = candidate(model="acceptance")
    def observe(state, ordinal, **kwargs):
        observed = replace(original, trigger_at=NOW + timedelta(minutes=30 * ordinal))
        return advance_episode(state, observed, ordinal=ordinal, close=100., range_low=99., range_high=101., **kwargs)
    state, _ = observe(None, 0, triggered=True)
    episode_id = state["candidate"]["episode_id"]
    state, _ = observe(state, 1, invalidated=True)
    state, _ = observe(state, 2, triggered=True)
    assert state["reset_count"] == 1
    state, _ = observe(state, 3, triggered=True)
    assert state["reset_count"] == 2 and state["candidate"]["episode_id"] == episode_id
    state, _ = observe(state, 4, triggered=True)
    assert state["setup"] == "TRIGGERED" and state["candidate"]["episode_id"] != episode_id


def test_missing_bar_breaks_reset_and_preserves_frozen_trigger():
    from research.stock_idea_engine import advance_episode
    original = candidate()
    state, _ = advance_episode(None, original, ordinal=1, close=100., range_low=98., range_high=110., triggered=True)
    later = replace(original, trigger_at=NOW, expires_at=NOW + timedelta(hours=1), stop=99.)
    state, _ = advance_episode(state, later, ordinal=2, close=100., range_low=98., range_high=110., triggered=True)
    assert state["candidate"]["trigger_at"] == str(original.trigger_at)
    assert state["candidate"]["stop"] == 98.
    state, updates = advance_episode(state, later, ordinal=4, close=110., range_low=98., range_high=110.)
    assert state["setup"] == "INVALIDATED" and state["reset_count"] == 0
    assert updates[0]["kind"] == "DATA_RISK"


def test_atomic_retry_late_correction_and_outage_do_not_duplicate_positions(tmp_path):
    import sqlite3
    from concurrent.futures import ThreadPoolExecutor
    from research.stock_idea_engine import PublicationLedger, decide_publication
    ledger = PublicationLedger(tmp_path / "replay.sqlite")
    original = candidate()
    def commit():
        return ledger.commit(state_key="PRIORITY", window_key="window", policy_hash="policy", inputs=["revision"],
            decide=lambda state: decide_publication(state, [original], deadline=NOW, now=NOW,
                expected_members=["A", "MISSING"], available_members=["A"], window_key="window", policy_hash="policy"))
    with ThreadPoolExecutor(max_workers=4) as executor:
        outputs = list(executor.map(lambda _: commit(), range(8)))
    assert all(output == outputs[0] for output in outputs)
    assert outputs[0]["missing_members"] == ["MISSING"]
    late = ledger.commit(state_key="PRIORITY", window_key="window", policy_hash="policy", inputs=["correction"],
                         decide=lambda _: (_ for _ in ()).throw(AssertionError("must reuse committed publication")))
    assert late == outputs[0]
    with sqlite3.connect(ledger.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM notification_outbox").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM audit_updates").fetchone()[0] == 1
    _, missed, outbox = decide_publication({}, [original], deadline=NOW, now=NOW + timedelta(seconds=1),
        expected_members=["A"], available_members=["A"], window_key="missed", policy_hash="policy")
    assert missed["coverage"] == "MISSED_PUBLICATION" and not outbox


def test_short_strength_competes_in_the_same_pooled_model_quota():
    rows = [candidate(str(index), rs_rank=.8) for index in range(3)]
    short = candidate("SHORT", direction=-1, rs_rank=.01, stop=102., target=90., interval="1d", horizon="DAILY_21")
    result = choose(rows + [short])
    assert result["selected"][0] == short.episode_id and len(result["selected"]) == 3


def test_open_opposition_is_a_coalesced_risk_update_not_a_reversal():
    from research.stock_idea_engine import decide_publication, candidate_record
    original = candidate()
    opponent = candidate(model="failure", direction=-1, stop=102., target=90.)
    position = dict(state="OPEN", candidate=candidate_record(original), entry_price=100., publication_at=NOW.isoformat())
    state = dict(positions={original.episode_id: position}, selected=[original.episode_id])
    arguments = dict(deadline=NOW, now=NOW, expected_members=["A"], available_members=["A"], window_key="first", policy_hash="policy")
    state, result, outbox = decide_publication(state, [opponent], **arguments)
    assert not result["selected"] and state["positions"][original.episode_id] == position
    assert len(outbox) == 1 and outbox[0]["kind"] == "RISK_UPDATE"
    _, _, repeated = decide_publication(state, [opponent], **dict(arguments, window_key="next"))
    assert not repeated


def test_atomic_failure_rolls_back_and_publication_cannot_run_early(tmp_path):
    import pytest
    import sqlite3
    from research.stock_idea_engine import PublicationLedger, decide_publication
    ledger = PublicationLedger(tmp_path / "rollback.sqlite")
    with pytest.raises(ValueError):
        ledger.commit(state_key="arm", window_key="window", policy_hash="policy", inputs=[],
                      decide=lambda _: ({}, {"bad": float("nan")}, []))
    with sqlite3.connect(ledger.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM publications").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM engine_state").fetchone()[0] == 0
    with pytest.raises(ValueError, match="before"):
        decide_publication({}, [], deadline=NOW, now=NOW - timedelta(seconds=1), expected_members=[],
                           available_members=[], window_key="early", policy_hash="policy")


def test_lifecycle_snapshots_share_immutable_lineage_without_mutating_prefix():
    import json
    from research.stock_idea_engine import advance_episode, candidate_record
    original = candidate(revision_ids=tuple(f"revision-{index}" for index in range(2000)))
    first, _ = advance_episode(None, original, ordinal=1, close=100., range_low=98., range_high=110., triggered=True)
    prefix = json.dumps(first, sort_keys=True)
    later = replace(original, trigger_at=NOW)
    second, _ = advance_episode(first, later, ordinal=2, close=100., range_low=98., range_high=110.)
    assert json.dumps(first, sort_keys=True) == prefix
    assert first is not second and first["candidate"] is not second["candidate"]
    assert candidate_record(original)["revision_ids"] is original.revision_ids
    assert second["candidate"]["revision_ids"] is first["candidate"]["revision_ids"]


def test_catalog_deduplicates_lineage_and_restores_active_state_atomically(tmp_path):
    import json
    import sqlite3
    from research.stock_idea_engine import PublicationLedger, candidate_record
    ledger = PublicationLedger(tmp_path / "catalog.sqlite")
    original = candidate(revision_ids=tuple(f"revision-{index}" for index in range(2000)))
    position = dict(state="OPEN", candidate=candidate_record(original))
    state = dict(positions={original.episode_id: position}, lifecycles={"A:30m": {"resumption:1": dict(setup="TRIGGERED", candidate=candidate_record(original))}})
    for arm in ("ALL", "PRIORITY", "MOMENTUM"):
        ledger.commit(state_key=arm, window_key="first", policy_hash="policy", inputs=[], decide=lambda _: (state, {}, []))
    with sqlite3.connect(ledger.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM candidate_catalog").fetchone()[0] == 1
        records = connection.execute("SELECT payload FROM engine_state").fetchall()
        assert max(len(row[0]) for row in records) < 1000
        restored = ledger.restore_state(connection, json.loads(records[0][0]), include_lifecycles=True)
        assert json.dumps(restored, sort_keys=True) == json.dumps(state, sort_keys=True)
    def next_decision(restored):
        assert restored["positions"][original.episode_id]["candidate"]["revision_ids"] == list(original.revision_ids)
        return restored, {"restored": True}, []
    assert ledger.commit(state_key="PRIORITY", window_key="next", policy_hash="policy", inputs=[], decide=next_decision)["restored"]