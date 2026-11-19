"""G106 O1 episodes (Nov 19): REALTIME_V3_O1_EPISODES, OFF / RECORD / ON, in the O1 executor after every gate.

An episode is (underlying, direction) per session. Its first new O1 session alert opens it; a later new alert on the
key is allowed only after 60 minutes since the key's last alert, with >= 2x the opening premium, under 2 alerts that
day, and while the underlying has not moved more than 1.5% against it; otherwise it is folded into the episode. OFF is
today's O1 byte for byte; RECORD logs one O1_EPISODES_RECORD line per window and changes nothing; ON makes a folded
alert's events OBSERVATION without a package and writes an `o1_episode` block on the opening alert. The state is derived
from the session's stored rows every window, so it survives a window-service restart.
"""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

import pytest

import realtime.advanced_options_native_loader as loader
import realtime.advanced_options_o1_episodes as episodes
from realtime.advanced_options_native_adapters import build_advanced_o1_executor
from realtime.advanced_options_o1_debit_vertical import O1_DEBIT_VERTICAL_SETTING
from realtime.advanced_options_o1_episodes import (
    ALLOW,
    O1EP_INVALIDATED,
    O1EP_MAX_ALERTS,
    O1EP_MIN_GAP,
    O1EP_PREMIUM_BELOW,
    O1EP_PREMIUM_UNKNOWN,
    O1_EPISODE_POLICY,
    O1_EPISODES_INVALIDATION_BP_SETTING,
    O1_EPISODES_MAX_ALERTS_SETTING,
    O1_EPISODES_MIN_GAP_MINUTES_SETTING,
    O1_EPISODES_PREMIUM_MULTIPLE_PCT_SETTING,
    O1_EPISODES_SETTING,
    OPEN,
    SUPPRESS,
    O1EpisodeInputs,
    O1EpisodePolicy,
    O1EpisodesMode,
    Signal,
    StoredO1Alert,
    load_o1_episode_inputs_guarded,
    o1_episode_policy,
    o1_episode_policy_from,
    o1_episodes_mode,
    run_episodes,
    spot_at,
)
from realtime.advanced_options_o1_gates import O1_AGGRESSOR_SIDE_SETTING, O1_ZERO_DTE_SETTING
from realtime.advanced_options_o1_liquidity_floor import O1_LIQUIDITY_FLOOR_SETTING
from realtime.advanced_options_projection import (
    _alert_run_row,
    _one_alert_per_contract,
    options_session_alert_id,
    persist_options_session_alerts_cursor,
)
from realtime.preflight import DetectorFamily
from test_o1_debit_vertical import (
    COHORT, CUTOFF, LONG_CALL, LONG_PUT, NARROW_CALL, SESSION, SHORT_CALL, WIDE_CALL, _Cursor, _flow_row, _plan,
)
from test_o1_liquidity_floor import _context
from test_options_session_alerts import AlertTable

UTC = timezone.utc
SETTINGS = (O1_EPISODES_SETTING, O1_EPISODES_MIN_GAP_MINUTES_SETTING, O1_EPISODES_PREMIUM_MULTIPLE_PCT_SETTING,
            O1_EPISODES_MAX_ALERTS_SETTING, O1_EPISODES_INVALIDATION_BP_SETTING, O1_ZERO_DTE_SETTING,
            O1_AGGRESSOR_SIDE_SETTING, O1_LIQUIDITY_FLOOR_SETTING, O1_DEBIT_VERTICAL_SETTING)
# Pinned Nov 19: the built-in policy and so the hash the RECORD lines name.
POLICY_SHA256 = "5732e27141df1ee036250db866a0c8e31453878fb4590bc514cd951cbeab1105"


@pytest.fixture(autouse=True)
def _process_start(monkeypatch):
    """Every test starts as a fresh window process: switches unset, policy not read, nothing queued."""
    for key in SETTINGS:
        monkeypatch.delenv(key, raising=False)
    o1_episode_policy.cache_clear()
    episodes._PENDING_BLOCKS.clear()
    yield
    o1_episode_policy.cache_clear()
    episodes._PENDING_BLOCKS.clear()


def _id(name):
    return uuid5(NAMESPACE_URL, f"o1ep:{name}")


def _row(name, contract, *, minute, open_interest=1000, day_volume=5000):
    row = _flow_row(_id(name), contract)
    event_time = CUTOFF - timedelta(minutes=minute)
    row.update(event_time=event_time, created_at=event_time + timedelta(seconds=1),
               quote_time=event_time - timedelta(seconds=1), prior_open_interest=open_interest,
               cumulative_volume_after=day_volume, underlyer="AAPL")
    return row


# One window: two new bullish AAPL alerts (345C at minute 8 with two trades, 350C at minute 6) and one bearish (335P).
ROWS = (
    _row("c345-a", LONG_CALL, minute=8),
    _row("p335", LONG_PUT, minute=7),
    _row("c350", SHORT_CALL, minute=6),
    _row("c345-b", LONG_CALL, minute=3),
)
PREMIUM = {"c345-a": "40000", "p335": "30000", "c350": "100000", "c345-b": "20000"}
TRADES = {str(_id(name)): (CUTOFF - timedelta(minutes=minute), Decimal(PREMIUM[name]))
          for name, minute in (("c345-a", 8), ("p335", 7), ("c350", 6), ("c345-b", 3))}


def _alert_id(contract, variant=""):
    return str(options_session_alert_id(str(SESSION), "O1", contract.contract_id, variant))


def _stored(contract, *, minutes_before, direction=1, premium_event=None, block=None):
    """A stored O1 session alert first triggered `minutes_before` the cutoff (its first window = that time)."""
    at = CUTOFF - timedelta(minutes=minutes_before)
    return StoredO1Alert(alert_id=_alert_id(contract), underlyer="AAPL", direction=direction,
                         contract_id=contract.contract_id, variant="", contract_ticker=contract.contract_ticker,
                         triggered_at=at, first_cutoff=at, event_ids=(premium_event,) if premium_event else (),
                         episode_block=block)


def _path(*points):
    """1m closes: (minutes before the cutoff, close)."""
    return tuple((CUTOFF - timedelta(minutes=m), Decimal(str(close))) for m, close in points)


FLAT = {"AAPL": _path((300, 340), (120, 340), (60, 340), (30, 340), (1, 340))}


def _inputs(alerts=(), trades=None, spots=None, reason=None):
    merged = dict(TRADES)
    merged.update(trades or {})
    return O1EpisodeInputs(SESSION, CUTOFF, tuple(alerts), merged, FLAT if spots is None else spots,
                           inputs_reason=reason)


def _load(monkeypatch, mode=None, inputs=None, rows=ROWS):
    """The real `_load_o1` (AL2 / gates OFF) over ROWS; the episode read returns `inputs`."""
    calls = []

    def fake(cursor, **kwargs):
        calls.append(kwargs)
        return inputs if inputs is not None else _inputs()

    monkeypatch.setattr(loader, "load_o1_episode_inputs_guarded", fake)
    if mode is None:
        monkeypatch.delenv(O1_EPISODES_SETTING, raising=False)
    else:
        monkeypatch.setenv(O1_EPISODES_SETTING, mode)
    lane, identity = loader._load_o1(_Cursor(list(rows)), _plan(), [])
    return lane, identity, calls


def _run(monkeypatch, mode=None, inputs=None, rows=ROWS):
    lane, identity, calls = _load(monkeypatch, mode, inputs, rows)
    return build_advanced_o1_executor(lane)(_context()), identity, calls


def _status(result):
    return {row.event_id: row.selection_status for row in result.evaluations}


def _lines(capsys, event="O1_EPISODES_RECORD"):
    return [json.loads(line.split(" ", 1)[1]) for line in capsys.readouterr().err.strip().splitlines()
            if line.split(" ", 1)[0] == event]


# --- switch and policy ---------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert o1_episodes_mode({}) is O1EpisodesMode.OFF
    assert o1_episodes_mode({O1_EPISODES_SETTING: " "}) is O1EpisodesMode.OFF
    assert o1_episodes_mode({O1_EPISODES_SETTING: "record"}) is O1EpisodesMode.RECORD
    assert o1_episodes_mode({O1_EPISODES_SETTING: " On "}) is O1EpisodesMode.ON
    for bad in ("YES", "1", "TRUE", "RECORDED", "ENABLED"):
        with pytest.raises(ValueError, match=O1_EPISODES_SETTING):
            o1_episodes_mode({O1_EPISODES_SETTING: bad})


def test_built_in_policy_and_its_hash():
    policy = O1_EPISODE_POLICY
    assert (policy.version, policy.realert_minimum_gap_minutes, policy.realert_minimum_premium_multiple_pct,
            policy.maximum_alerts_per_key, policy.invalidation_adverse_move_bp) == ("o1_alert_episodes_v1", 60, 200, 2,
                                                                                   150)
    assert policy.execution_permission is False
    assert policy.sha256 == POLICY_SHA256
    assert o1_episode_policy_from({}) is policy
    explicit = {O1_EPISODES_MIN_GAP_MINUTES_SETTING: "60", O1_EPISODES_PREMIUM_MULTIPLE_PCT_SETTING: " 200 ",
                O1_EPISODES_MAX_ALERTS_SETTING: "2", O1_EPISODES_INVALIDATION_BP_SETTING: "150"}
    assert o1_episode_policy_from(explicit) is policy
    assert O1EpisodePolicy.model_validate_json(policy.canonical_json()) == policy


def test_set_thresholds_are_stamped_into_the_policy():
    changed = o1_episode_policy_from({O1_EPISODES_MIN_GAP_MINUTES_SETTING: "90",
                                      O1_EPISODES_PREMIUM_MULTIPLE_PCT_SETTING: "300",
                                      O1_EPISODES_MAX_ALERTS_SETTING: "3", O1_EPISODES_INVALIDATION_BP_SETTING: "100"})
    assert (changed.realert_minimum_gap_minutes, changed.realert_minimum_premium_multiple_pct,
            changed.maximum_alerts_per_key, changed.invalidation_adverse_move_bp) == (90, 300, 3, 100)
    assert changed.sha256 != POLICY_SHA256


@pytest.mark.parametrize("key,bad", [
    (O1_EPISODES_MIN_GAP_MINUTES_SETTING, "-1"), (O1_EPISODES_MIN_GAP_MINUTES_SETTING, "1.5"),
    (O1_EPISODES_PREMIUM_MULTIPLE_PCT_SETTING, "2x"), (O1_EPISODES_MAX_ALERTS_SETTING, "0"),
    (O1_EPISODES_MAX_ALERTS_SETTING, "two"), (O1_EPISODES_INVALIDATION_BP_SETTING, "0"),
    (O1_EPISODES_INVALIDATION_BP_SETTING, "1.5%"),
])
def test_invalid_thresholds_fail_loudly(key, bad):
    with pytest.raises(ValueError, match=key):
        o1_episode_policy_from({key: bad})


def test_policy_is_read_once_per_process(monkeypatch):
    monkeypatch.setenv(O1_EPISODES_MIN_GAP_MINUTES_SETTING, "30")
    first = o1_episode_policy()
    monkeypatch.setenv(O1_EPISODES_MIN_GAP_MINUTES_SETTING, "5")
    assert o1_episode_policy() is first and first.realert_minimum_gap_minutes == 30


def test_window_service_start_checks_the_switch_and_the_policy():
    source = (Path(loader.__file__).parents[1] / "scripts" / "run_advanced_options_v3_production.py").read_text(
        encoding="utf-8")
    start = source.index("kind = AdvancedOptionsProcessKind(")
    block = source[start:source.index("provider = None", start)]
    assert "o1_episodes_mode()" in block and "o1_episode_policy()" in block


# --- the rule (pure) -------------------------------------------------------------------------------------------------------

def _signal(name, minutes_before, premium, *, direction=1, underlyer="AAPL", stored=False):
    return Signal(alert_id=name, underlyer=underlyer, direction=direction, contract_id=1, variant="",
                  contract_ticker=f"O:{name}", triggered_at=CUTOFF - timedelta(minutes=minutes_before),
                  premium=None if premium is None else Decimal(premium), stored=stored)


def _decide(stored=(), window=(), spots=FLAT, all_stored_count=False, policy=O1_EPISODE_POLICY):
    found = run_episodes(list(stored), list(window), policy=policy, spots=spots, all_stored_count=all_stored_count)
    return found, {s.alert_id: (s.decision, s.reasons) for s in (*stored, *window)}


def test_first_alert_opens_later_ones_fold_until_gap_and_premium_hold():
    found, decided = _decide(window=[
        _signal("open", 200, "50000"),
        _signal("early-big", 170, "500000"),     # 30 min later: too soon
        _signal("late-small", 130, "60000"),     # 70 min later but 1.2x
        _signal("late-big", 120, "100000"),      # 80 min later and exactly 2x: allowed
        _signal("third", 10, "900000"),          # the cap (2 alerts) is reached
    ])
    assert decided["open"] == (OPEN, ())
    assert decided["early-big"] == (SUPPRESS, (O1EP_MIN_GAP,))
    assert decided["late-small"] == (SUPPRESS, (O1EP_PREMIUM_BELOW,))
    assert decided["late-big"] == (ALLOW, ())
    assert decided["third"] == (SUPPRESS, (O1EP_MAX_ALERTS,))
    episode = found["AAPL:BULLISH"]
    assert [a for a, _t, _c in episode.alerts] == ["open", "late-big"]
    assert [f[0] for f in episode.folded] == ["early-big", "late-small", "third"]
    assert episode.opening_premium == Decimal("50000") and episode.opening_spot == Decimal("340")


def test_gap_counts_from_the_last_alert_and_unknown_premium_never_allows():
    _found, decided = _decide(window=[
        _signal("open", 200, "10000"), _signal("again", 130, "30000"), _signal("unknown", 100, None),
    ], policy=O1EpisodePolicy(maximum_alerts_per_key=5))
    assert decided["again"] == (ALLOW, ())
    assert decided["unknown"] == (SUPPRESS, (O1EP_MIN_GAP, O1EP_PREMIUM_UNKNOWN))


def test_opposite_direction_and_other_names_are_their_own_episodes():
    found, decided = _decide(window=[
        _signal("bull", 30, "1000"), _signal("bear", 20, "1000", direction=-1), _signal("nvda", 10, "1000",
                                                                                         underlyer="NVDA"),
    ], spots={**FLAT, "NVDA": _path((60, 150), (1, 150))})
    assert {key: d for key, (d, _r) in decided.items()} == {"bull": OPEN, "bear": OPEN, "nvda": OPEN}
    assert sorted(found) == ["AAPL:BEARISH", "AAPL:BULLISH", "NVDA:BULLISH"]


def test_invalidation_is_the_first_bar_beyond_1_5_percent_against_the_episode():
    falling = {"AAPL": _path((200, 340), (180, 338), (150, 334.8), (100, 334.0), (1, 341))}
    found, decided = _decide(window=[
        _signal("open", 199, "10000"),        # opening spot 340 (the bar at 200 before)
        _signal("before", 160, "90000"),      # 39 min: gap fails; not yet invalidated (338 = -0.59%)
        _signal("after", 90, "90000"),        # gap and premium hold, but 334.8 (-1.53%) at 150 invalidated it
    ], spots=falling)
    episode = found["AAPL:BULLISH"]
    assert episode.invalidated_at == CUTOFF - timedelta(minutes=150) and episode.invalidation_spot == Decimal("334.8")
    assert decided["before"] == (SUPPRESS, (O1EP_MIN_GAP,))
    assert decided["after"] == (SUPPRESS, (O1EP_INVALIDATED,))
    assert episode.worst_move_bp == pytest.approx(-176.5, abs=0.1)
    assert episode.current_spot == Decimal("341")  # recovering does not reopen it
    # The bearish side of a falling name is not invalidated (the move is with it).
    found, _decided = _decide(window=[_signal("bear", 199, "1", direction=-1)], spots=falling)
    assert found["AAPL:BEARISH"].invalidated_at is None


def test_spot_unavailable_means_no_invalidation():
    found, decided = _decide(window=[_signal("open", 100, "1"), _signal("next", 30, "5")], spots={})
    assert found["AAPL:BULLISH"].opening_spot is None and found["AAPL:BULLISH"].invalidated_at is None
    assert decided["next"] == (ALLOW, ())
    assert spot_at(_path((10, 1), (5, 2)), CUTOFF - timedelta(minutes=7)) == Decimal("1")
    assert spot_at(_path((10, 1)), CUTOFF - timedelta(minutes=11)) is None


def test_record_resimulates_stored_alerts_and_on_counts_them_all():
    stored = [_signal("s1", 100, "10000", stored=True), _signal("s2", 80, "90000", stored=True)]
    window = [_signal("w", 5, "90000")]
    _found, record = _decide(stored=stored, window=window)
    assert record["s2"] == (SUPPRESS, (O1EP_MIN_GAP,)) and record["w"] == (ALLOW, ())  # s2 folded: w is the 2nd
    stored = [_signal("s1", 100, "10000", stored=True), _signal("s2", 80, "90000", stored=True)]
    window = [_signal("w", 5, "90000")]
    _found, on = _decide(stored=stored, window=window, all_stored_count=True)
    assert on["s2"] == ("ALERTED", ()) and on["w"] == (SUPPRESS, (O1EP_MAX_ALERTS,))


# --- the executor through the switch ------------------------------------------------------------------------------------

def test_off_is_todays_o1_byte_for_byte(monkeypatch, capsys):
    unset, unset_identity, calls = _run(monkeypatch)
    assert calls == [] and capsys.readouterr().err == ""
    explicit, explicit_identity, calls = _run(monkeypatch, "OFF")
    assert calls == [] and capsys.readouterr().err == ""
    assert unset == explicit and unset_identity == explicit_identity
    assert set(_status(unset).values()) == {"SELECTED"}
    lane, _identity, _calls = _load(monkeypatch)
    assert lane.o1_episodes is None


def test_record_changes_nothing_and_logs_one_line(monkeypatch, capsys):
    off, off_identity, _calls = _run(monkeypatch)
    capsys.readouterr()
    record, record_identity, calls = _run(monkeypatch, "RECORD")
    assert record == off and record_identity == off_identity
    assert len(calls) == 1 and sorted(calls[0]["window_event_ids"]) == sorted(str(_id(n)) for n in PREMIUM)
    lines = _lines(capsys)
    assert len(lines) == 1
    line = lines[0]
    assert (line["mode"], line["status"], line["applied"], line["policy_sha256"]) == ("RECORD", "OK", False,
                                                                                     POLICY_SHA256)
    # 345C (first at minute 8) opens the bullish episode; 350C (minute 6) folds into it; 335P opens the bearish one.
    assert (line["window_new_alerts"], line["would_open"], line["would_allow"], line["would_suppress"]) == (3, 2, 0, 1)
    assert line["would_suppress_alerts"] == [{"alert_id": _alert_id(SHORT_CALL), "contract": SHORT_CALL.contract_ticker,
                                              "episode": "AAPL:BULLISH"}]
    assert line["would_suppress_reasons"] == {O1EP_MIN_GAP: 1, O1EP_PREMIUM_BELOW: 1}  # $100k vs 2 x $60k
    window = {row["contract"]: row for row in line["window"]}
    assert window[LONG_CALL.contract_ticker]["premium"] == "60000"  # its two trades in the window
    assert window[SHORT_CALL.contract_ticker]["decision"] == SUPPRESS
    bull = next(e for e in line["episodes"] if e["key"] == "AAPL:BULLISH")
    assert (bull["state"], bull["alerts_today"], bull["folded_signals"], bull["opening_alert_id"]) == (
        "OPEN", 1, 1, _alert_id(LONG_CALL))
    assert len(json.dumps(line)) < 16_000


def test_on_folds_the_new_alert_and_queues_the_opening_alerts_block(monkeypatch, capsys):
    off, off_identity, _calls = _run(monkeypatch)
    capsys.readouterr()
    on, on_identity, _calls = _run(monkeypatch, "ON")
    status = _status(on)
    assert status[_id("c350")] == "OBSERVATION"
    assert next(row for row in on.evaluations if row.event_id == _id("c350")).package is None
    assert {status[_id(n)] for n in ("c345-a", "c345-b", "p335")} == {"SELECTED"}
    assert on_identity != off_identity and on_identity[-1][0] == "O1_EPISODES"
    # Everything else is today's evaluation, unchanged.
    for before, after in zip(off.evaluations, on.evaluations):
        if before.event_id != _id("c350"):
            assert before == after
    line = _lines(capsys)[0]
    assert (line["mode"], line["applied"], line["blocks_queued"]) == ("ON", True, 1)
    queued = episodes.pending_o1_episode_blocks()
    (blocks,) = queued.values()
    block = blocks[_alert_id(LONG_CALL)]
    assert (block["key"], block["folded_signals"], block["folded_contract_list"], block["folded_premium"]) == (
        "AAPL:BULLISH", 1, [SHORT_CALL.contract_ticker], "100000")


def test_on_repeat_hits_on_a_stored_alert_are_never_judged(monkeypatch, capsys):
    stored = [_stored(LONG_CALL, minutes_before=20, premium_event=str(_id("c345-a")))]
    on, _identity, _calls = _run(monkeypatch, "ON", _inputs(stored))
    status = _status(on)
    # 345C is a stored alert: its events stay hits on it. 350C is a new bullish alert 20 minutes later: folded.
    assert status[_id("c345-a")] == status[_id("c345-b")] == "SELECTED"
    assert status[_id("c350")] == "OBSERVATION"
    line = _lines(capsys)[0]
    assert (line["stored_alerts"], line["window_repeat_hits"], line["window_new_alerts"]) == (1, 2, 2)


def test_on_allows_a_bigger_signal_after_the_gap(monkeypatch, capsys):
    # The opening alert (345C, 90 minutes ago) had a $40k trade in its window; 350C has $100k now: 2.5x.
    stored = [_stored(LONG_CALL, minutes_before=90, premium_event="opening-trade")]
    opening_trade = {"opening-trade": (CUTOFF - timedelta(minutes=92), Decimal("40000"))}
    on, _identity, _calls = _run(monkeypatch, "ON", _inputs(stored, opening_trade))
    assert _status(on)[_id("c350")] == "SELECTED"
    window = {row["contract"]: row for row in _lines(capsys)[0]["window"]}
    assert (window[SHORT_CALL.contract_ticker]["decision"], window[SHORT_CALL.contract_ticker]["reasons"]) == (ALLOW, [])
    # The same with only 1.5x: folded.
    smaller = {"opening-trade": (CUTOFF - timedelta(minutes=92), Decimal("66667"))}
    on, _identity, _calls = _run(monkeypatch, "ON", _inputs(stored, smaller))
    assert _status(on)[_id("c350")] == "OBSERVATION"
    assert {row["contract"]: row for row in _lines(capsys)[0]["window"]}[SHORT_CALL.contract_ticker]["reasons"] == [
        O1EP_PREMIUM_BELOW]


def test_on_invalidated_episode_allows_no_further_alert(monkeypatch, capsys):
    stored = [_stored(LONG_CALL, minutes_before=90, premium_event="opening-trade")]
    opening_trade = {"opening-trade": (CUTOFF - timedelta(minutes=92), Decimal("10000"))}
    falling = {"AAPL": _path((95, 340), (60, 336), (30, 334), (1, 333))}  # 334 = -1.76%
    on, _identity, _calls = _run(monkeypatch, "ON", _inputs(stored, opening_trade, falling))
    assert _status(on)[_id("c350")] == "OBSERVATION"
    line = _lines(capsys)[0]
    window = {row["contract"]: row for row in line["window"]}
    assert window[SHORT_CALL.contract_ticker]["reasons"] == [O1EP_INVALIDATED]
    bull = next(e for e in line["episodes"] if e["key"] == "AAPL:BULLISH")
    assert (bull["state"], bull["invalidated"], bull["invalidation_spot"]) == ("INVALIDATED", True, "334")
    # The bearish 335P is its own episode: it opens.
    assert _status(on)[_id("p335")] == "SELECTED"


def test_inputs_unavailable_fails_open_and_says_so(monkeypatch, capsys):
    off, _identity, _calls = _run(monkeypatch)
    capsys.readouterr()
    on, _identity, _calls = _run(monkeypatch, "ON", _inputs(reason="OperationalError: timeout"))
    assert on == off
    line = _lines(capsys)[0]
    assert line["status"] == "INPUTS_UNAVAILABLE:OperationalError: timeout" and line["window_new_alerts"] is None
    assert episodes.pending_o1_episode_blocks() == {}


def test_an_error_in_the_rule_is_one_failed_line_and_todays_evaluations(monkeypatch, capsys):
    off, _identity, _calls = _run(monkeypatch)
    capsys.readouterr()

    def boom(*args, **kwargs):
        raise RuntimeError("bug")

    monkeypatch.setattr(episodes, "run_episodes", boom)
    on, _identity, _calls = _run(monkeypatch, "ON")
    assert on == off
    failed = _lines(capsys, "O1_EPISODES_RECORD_FAILED")
    assert len(failed) == 1 and failed[0]["reason"] == "RuntimeError: bug"


# --- the loader's read and the restart ----------------------------------------------------------------------------------

def _evaluations(result, dataset):
    return [
        SimpleNamespace(evaluation_id=uuid5(NAMESPACE_URL, f"evaluation:{dataset}:{row.event_id}"), event_id=row.event_id,
                        detector=DetectorFamily.O1, underlyer=row.underlyer, selection_status=row.selection_status,
                        observed_at=row.observed_at, payload=row.payload, package=row.package, dataset_id=dataset)
        for row in result.evaluations if row.selection_status == "SELECTED"
    ]


class StoreCursor(AlertTable):
    """`rt.alert_current` (the session writer's statements and the episode block's) plus the episode read."""

    def __init__(self, trades):
        super().__init__()
        self.trades = trades

    def execute(self, sql, params=()):
        if sql.startswith("SELECT alert_id::text AS alert_id,projection->>'underlyer'"):
            self.statements.append(sql)
            day, end, schema, session = params
            self.result = []
            for key, row in self.rows.items():
                p = json.loads(row["projection"])
                if row["schema"] == schema and p["session_date"] == session and p["detector_id"] == "O1":
                    self.result.append(dict(
                        alert_id=key, underlyer=p["underlyer"], direction=str(p["direction"]),
                        contract_id=str(p["contract_id"]), triggered_at=p["triggered_at"],
                        scheduled_cycle=p["scheduled_cycle"], legs=p["original_package"]["legs"],
                        event_ids=p["event_ids"], episode=p.get("o1_episode")))
            return
        if sql.startswith("SELECT event_id::text AS event_id,event_time,premium"):
            self.statements.append(sql)
            ids, start, end = params
            self.result = [dict(event_id=e, event_time=t, premium=p) for e, (t, p) in self.trades.items()
                           if e in set(ids) and start < t <= end]
            return
        if sql.startswith("SELECT alert_id::text AS alert_id,projection FROM rt.alert_current"):
            self.statements.append(sql)
            self.result = [dict(alert_id=key, projection=json.loads(row["projection"]))
                           for key, row in self.rows.items() if key in set(params[0])]
            return
        if sql.startswith("UPDATE rt.alert_current SET projection_sha256"):
            self.statements.append(sql)
            sha, projection, alert_id = params
            self._check(self.rows[alert_id]["as_of"], self.rows[alert_id]["valid_until"], sha, projection)
            self.rows[alert_id]["projection"] = projection
            return
        return super().execute(sql, params)


def _window(monkeypatch, store, mode, rows, cutoff, dataset):
    """One window over `rows` with the episode read on `store` (the real read), then the real session alert writer
    and the episode blocks: what the window service does, minus the database."""
    monkeypatch.setenv(O1_EPISODES_SETTING, mode)
    monkeypatch.setattr(episodes, "_load_spot_paths", lambda cursor, names, start, until: FLAT)

    class LoaderCursor(_Cursor):
        def execute(self, sql, params=None):
            if sql.split()[0] in {"SAVEPOINT", "RELEASE", "ROLLBACK"} or "rt.alert_current" in sql or \
                    sql.startswith("SELECT event_id::text AS event_id,event_time,premium"):
                store.execute(sql, params)
                self.result = store.result
                return
            return super().execute(sql, params)

        def fetchall(self):
            return self.result

    plan = _plan()
    plan.cutoff = cutoff
    lane, identity = loader._load_o1(LoaderCursor(list(rows)), plan, [])
    result = build_advanced_o1_executor(lane)(_context())
    evaluations = _evaluations(result, dataset)
    pairs = [(item, _alert_run_row(item, cutoff, {})) for item in evaluations]
    alert_rows = _one_alert_per_contract([(e, r) for e, r in pairs if r is not None])
    persist_options_session_alerts_cursor(store, cohort_id=COHORT, session_date=str(SESSION), alert_rows=alert_rows,
                                          evaluations=evaluations)
    episodes.apply_pending_o1_episode_blocks_cursor(store, cutoff=_context().cutoff)
    return SimpleNamespace(evaluations=result.evaluations, identity=identity)


def _shift(rows, minutes, tag):
    shifted = []
    for row in rows:
        row = dict(row)
        row["event_id"] = str(uuid5(NAMESPACE_URL, f"{tag}:{row['event_id']}"))
        for key in ("event_time", "created_at", "quote_time"):
            row[key] = row[key] + timedelta(minutes=minutes)
        shifted.append(row)
    return shifted


@pytest.mark.parametrize("mode", ["RECORD", "ON"])
def test_state_survives_a_window_service_restart(monkeypatch, capsys, mode):
    """Window 1 (16:35) opens the bullish episode on 345C. The process restarts (nothing in memory). Window 2 (16:50)
    has a new bullish alert on 346C: the rule still knows the episode from the stored alert and folds it."""
    first_rows = [_row("w1-c345", LONG_CALL, minute=4)]
    later_cutoff = CUTOFF + timedelta(minutes=15)
    second_rows = _shift([_row("w2-c346", NARROW_CALL, minute=4)], 15, "w2")
    trades = {first_rows[0]["event_id"]: (first_rows[0]["event_time"], Decimal("50000")),
              second_rows[0]["event_id"]: (second_rows[0]["event_time"], Decimal("400000"))}
    store = StoreCursor(trades)
    _window(monkeypatch, store, mode, first_rows, CUTOFF, "ds-1")
    assert list(store.rows) == [_alert_id(LONG_CALL)]
    capsys.readouterr()
    # Restart: a new process (policy re-read, nothing queued), the stored rows are all that is left.
    o1_episode_policy.cache_clear()
    episodes._PENDING_BLOCKS.clear()
    result = _window(monkeypatch, store, mode, second_rows, later_cutoff, "ds-2")
    line = _lines(capsys)[0]
    assert (line["stored_alerts"], line["window_new_alerts"], line["would_suppress"]) == (1, 1, 1)
    assert line["would_suppress_alerts"][0]["contract"] == NARROW_CALL.contract_ticker
    bull = next(e for e in line["episodes"] if e["key"] == "AAPL:BULLISH")
    assert (bull["opening_alert_id"], bull["opening_premium"]) == (_alert_id(LONG_CALL), "50000")
    second_status = {row.event_id: row.selection_status for row in result.evaluations}
    second_id = next(iter(second_status))
    if mode == "RECORD":
        assert second_status[second_id] == "SELECTED" and len(store.rows) == 2
        assert all("o1_episode" not in json.loads(row["projection"]) for row in store.rows.values())
    else:
        assert second_status[second_id] == "OBSERVATION" and list(store.rows) == [_alert_id(LONG_CALL)]
        block = json.loads(store.rows[_alert_id(LONG_CALL)]["projection"])["o1_episode"]
        assert (block["folded_signals"], block["folded_contract_list"], block["folded_premium"], block["state"]) == (
            1, [NARROW_CALL.contract_ticker], "400000", "OPEN")
        # A third window after another restart reads the block back: the fold count keeps growing from the store.
        episodes._PENDING_BLOCKS.clear()
        third_rows = _shift([_row("w3-c380", WIDE_CALL, minute=4)], 20, "w3")
        store.trades[third_rows[0]["event_id"]] = (third_rows[0]["event_time"], Decimal("1000"))
        capsys.readouterr()
        _window(monkeypatch, store, mode, third_rows, CUTOFF + timedelta(minutes=20), "ds-3")
        block = json.loads(store.rows[_alert_id(LONG_CALL)]["projection"])["o1_episode"]
        assert block["folded_signals"] == 2 and block["folded_contracts"] == 2


def test_a_retried_window_after_its_own_write_decides_and_identifies_the_same(monkeypatch, capsys):
    """ON: window 2 folds 346C into the 345C episode and writes the block; the window is retried after that write (the
    process restarted): same decisions, same O1 input identity, the block not counted twice."""
    first_rows = [_row("r1-c345", LONG_CALL, minute=4)]
    second_rows = _shift([_row("r2-c346", NARROW_CALL, minute=4), _row("r2-c345", LONG_CALL, minute=3)], 15, "r2")
    trades = {row["event_id"]: (row["event_time"], Decimal("50000")) for row in (*first_rows, *second_rows)}
    store = StoreCursor(trades)
    _window(monkeypatch, store, "ON", first_rows, CUTOFF, "ds-1")
    later = CUTOFF + timedelta(minutes=15)
    attempt = _window(monkeypatch, store, "ON", second_rows, later, "ds-2")
    written = json.loads(store.rows[_alert_id(LONG_CALL)]["projection"])
    episodes._PENDING_BLOCKS.clear()
    o1_episode_policy.cache_clear()
    retry = _window(monkeypatch, store, "ON", second_rows, later, "ds-2")
    assert retry.identity == attempt.identity
    assert [(row.event_id, row.selection_status) for row in retry.evaluations] == [
        (row.event_id, row.selection_status) for row in attempt.evaluations]
    block = json.loads(store.rows[_alert_id(LONG_CALL)]["projection"])["o1_episode"]
    assert block == written["o1_episode"] and block["folded_signals"] == 1


def test_the_read_parses_stored_alerts_and_survives_a_failure(monkeypatch):
    store = StoreCursor({})
    projection = {"session_date": str(SESSION), "detector_id": "O1", "underlyer": "AAPL", "direction": 1,
                  "contract_id": LONG_CALL.contract_id, "triggered_at": (CUTOFF - timedelta(minutes=30)).isoformat(),
                  "scheduled_cycle": (CUTOFF - timedelta(minutes=30)).isoformat(), "event_ids": ["e1"],
                  "original_package": {"legs": [
                      {"leg_index": 0, "contract_id": LONG_CALL.contract_id, "contract_ticker": LONG_CALL.contract_ticker},
                      {"leg_index": 1, "contract_id": SHORT_CALL.contract_id,
                       "contract_ticker": SHORT_CALL.contract_ticker}]}}
    late = dict(projection, triggered_at=(CUTOFF + timedelta(minutes=1)).isoformat(),
                scheduled_cycle=(CUTOFF + timedelta(minutes=5)).isoformat(), contract_id=LONG_PUT.contract_id)
    # Stored by this window's own projection (a retry after that write): judged again as this window's, not stored.
    own = dict(projection, triggered_at=(CUTOFF - timedelta(minutes=3)).isoformat(), scheduled_cycle=CUTOFF.isoformat(),
               contract_id=WIDE_CALL.contract_id)
    for key, value in (("a1", projection), ("a2", late), ("a3", own)):
        store.rows[key] = dict(schema="options-alert-session-v1", projection=json.dumps(value), event_id=key,
                               as_of=CUTOFF, valid_until=None, cohort_id=str(COHORT))
    store.trades = {"e1": (CUTOFF - timedelta(minutes=31), Decimal("12345"))}
    monkeypatch.setattr(episodes, "_load_spot_paths", lambda cursor, names, start, until: {"AAPL": FLAT["AAPL"]})
    inputs = load_o1_episode_inputs_guarded(store, session_date=SESSION, cutoff=CUTOFF, window_event_ids=(),
                                            window_underlyers=())
    assert inputs.inputs_reason is None and [a.alert_id for a in inputs.alerts] == ["a1"]  # a2 later, a3 this window
    alert = inputs.alerts[0]
    assert (alert.variant, alert.direction, alert.contract_ticker) == (
        f"O1_DEBIT_VERTICAL:{SHORT_CALL.contract_id}", 1, LONG_CALL.contract_ticker)
    assert episodes.stored_signals(inputs)[0].premium == Decimal("12345")
    assert store.statements[0] == "SAVEPOINT o1_episode_inputs" and store.statements[-1].startswith("RELEASE")

    class Broken(StoreCursor):
        def execute(self, sql, params=()):
            if "rt.alert_current" in sql:
                raise RuntimeError("relation locked")
            return super().execute(sql, params)

    broken = Broken({})
    failed = load_o1_episode_inputs_guarded(broken, session_date=SESSION, cutoff=CUTOFF, window_event_ids=(),
                                            window_underlyers=())
    assert failed.inputs_reason == "RuntimeError: relation locked" and failed.alerts == ()
    assert "ROLLBACK TO SAVEPOINT o1_episode_inputs" in broken.statements


def test_a_heavy_day_line_stays_under_16_kib_with_whole_counts(monkeypatch, capsys):
    """40 names with a stored alert each and 60 new alerts in the window: the lists shrink, the counts stay whole."""
    names = [f"N{i:02d}" for i in range(40)]
    alerts = tuple(StoredO1Alert(alert_id=f"stored-{n}", underlyer=n, direction=1, contract_id=i + 1, variant="",
                                 contract_ticker=f"O:{n}261120C00100000", triggered_at=CUTOFF - timedelta(minutes=30),
                                 first_cutoff=CUTOFF - timedelta(minutes=30), event_ids=())
                   for i, n in enumerate(names))
    inputs = O1EpisodeInputs(SESSION, CUTOFF, alerts, {}, {n: FLAT["AAPL"] for n in names})
    window = [Signal(alert_id=f"new-{i}", underlyer=names[i % 40], direction=1, contract_id=1000 + i, variant="",
                     contract_ticker=f"O:{names[i % 40]}261127C00{i:03d}000", triggered_at=CUTOFF - timedelta(minutes=2),
                     premium=Decimal(1000 + i), stored=False) for i in range(60)]
    run = episodes.O1EpisodeRun(O1EpisodesMode.RECORD, O1_EPISODE_POLICY, inputs)
    monkeypatch.setattr(episodes, "_alert_identity", lambda evaluation, session: None)
    original = episodes.run_episodes

    def with_window(stored, _window, **kwargs):
        return original(stored, window, **kwargs)

    monkeypatch.setattr(episodes, "run_episodes", with_window)
    assert run.apply([], CUTOFF) == []
    raw = [line for line in capsys.readouterr().err.splitlines() if line.startswith("O1_EPISODES_RECORD ")]
    assert len(raw) == 1 and len(raw[0]) < 16_000
    line = json.loads(raw[0].split(" ", 1)[1])
    assert (line["episodes_total"], line["stored_alerts"], len(line["episodes"])) == (40, 40, 16)
    # A tighter limit: the lists shrink and say so; the counts stay whole.
    monkeypatch.setattr(episodes, "LINE_BYTE_LIMIT", 4000)
    run.apply([], CUTOFF)
    raw = [line for line in capsys.readouterr().err.splitlines() if line.startswith("O1_EPISODES_RECORD ")]
    line = json.loads(raw[0].split(" ", 1)[1])
    assert line["truncated"] is True and len(line["episodes"]) < 16 and line["episodes_total"] == 40


def test_projection_writer_runs_no_episode_sql_when_nothing_is_queued():
    store = StoreCursor({})
    assert episodes.apply_pending_o1_episode_blocks_cursor(store, cutoff=CUTOFF) == 0
    assert store.statements == []
