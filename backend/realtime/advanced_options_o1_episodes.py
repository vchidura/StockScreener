"""G106 O1 episodes (Nov 19 2026): alert concentration control for O1 across windows, RECORD first.

Why (Nov 18, EOD_ANALYSIS section 2): 16 live O1 alerts, 14 of them bullish NVDA calls / call debit verticals on
different strikes, expiries and verticals from 16:02 to 20:35 (1-2 a window over 4.5 hours) while NVDA fell 5.1%; 424
of the ~443 distinct O1 events were NVDA. The session alert is one per contract and package variant
(`advanced_options_projection.options_session_alert_id`), so a new strike or a new vertical on the same name and side
is a new alert; a per-window cap would change nothing (the concentration is across windows). The losses were one
direction call repeated, so the unit of an alert becomes the episode: one underlying and one direction per session.

`REALTIME_V3_O1_EPISODES` on the Options window (`realtime-v3-options-p8-window`):
| OFF (default, unset, blank) | today's O1, byte for byte: no read, no line, no write |
| RECORD | alerts unchanged; one guarded read-only read per window (the session's stored O1 alerts, the premium of their
           trades and of this window's O1 trades, the 1m bars of their underlyings known by the cutoff); one
           `O1_EPISODES_RECORD {json}` line per window (per episode: state, alerts, folded signals, invalidation; this
           window's new alerts with what the rule would do) |
| ON | the same read and line; a new O1 session alert the rule does not allow becomes an OBSERVATION without a package
       (folded into its episode); the episode's opening alert gets an `o1_episode` block in its stored projection |
Anything else, or a threshold that is not a non-negative integer, and the window service refuses to start.

The rule (policy `o1_alert_episodes_v1`, hash in every line; thresholds read once per process):
- Episode key = (underlying, direction) per session; direction = the alert's (a long call or a call debit vertical +1,
  a long put or a put debit vertical -1). The opposite direction on the same underlying is its own episode.
- Only a NEW O1 session alert is judged (a window's SELECTED O1 events grouped by session alert key = contract and
  package variant, the key not yet stored today). Later events on a stored alert's contract stay repeat hits on it.
  F0DTE alerts (detector F0DTE) and every other detector are outside the rule.
- The first new alert of a key opens the episode (an alert as today). A later new alert on the key is allowed only when
  ALL hold: at least `REALTIME_V3_O1_EPISODES_MIN_GAP_MINUTES` (60) since the key's last alert, its signal premium >=
  `REALTIME_V3_O1_EPISODES_PREMIUM_MULTIPLE_PCT` (200 = 2x) of the episode's opening premium, fewer than
  `REALTIME_V3_O1_EPISODES_MAX_ALERTS` (2) alerts on the key today, and the episode is not invalidated. Otherwise it is
  folded into the episode (contracts, premium, count, last time).
- Signal premium = the premium of the alert's SELECTED trades in the window it is judged in (the window's 10-minute
  read); a stored alert's opening premium = the premium of its stored trades inside its first window (scheduled_cycle
  minus 10 minutes to scheduled_cycle).
- Invalidation: the episode is INVALIDATED from the first 1m RAW bar (known by the cutoff) after its opening alert whose
  close moved against the episode's direction by more than `REALTIME_V3_O1_EPISODES_INVALIDATION_BP` (150 = 1.5%) of
  the spot at the opening alert (the latest 1m bar closed at or before it: the same bars the window's spot and the
  breadth capture's BAR spot read); no further alert on the key that session. Recorded in RECORD; gates only under ON.

State survives a window-service restart: every decision is derived each window from the session's stored rows (read
only): `rt.alert_current` (the session's O1 alerts of earlier windows - an alert stored by this window's own projection,
i.e. a retry after that write, is judged again as this window's, so a retry decides and identifies as the first attempt -
and under ON the `o1_episode` block of each opening alert),
`rt.option_flow_events` (trade premium) and the 1m bars. In RECORD every alert is stored, so the episode history is
re-simulated from the session's first alert each window (a stored alert the rule would have folded is folded; its later
hits stay hits). Under ON the stored alerts are the delivered ones and all count. Recording never fails, delays or
changes the window: a read failure is `status: INPUTS_UNAVAILABLE` (ON then lets every alert through, as today: fail
open, a concentration rule never hides the day's first signal), any other error one `O1_EPISODES_RECORD_FAILED` line.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as clock_time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from typing import Iterable, Literal, Mapping, Sequence

from pydantic import Field

from equity.behavior import Contract

from .advanced_options_o1_liquidity_floor import non_negative_int_setting

UTC = timezone.utc

O1_EPISODES_SETTING = "REALTIME_V3_O1_EPISODES"
O1_EPISODES_MIN_GAP_MINUTES_SETTING = "REALTIME_V3_O1_EPISODES_MIN_GAP_MINUTES"
O1_EPISODES_PREMIUM_MULTIPLE_PCT_SETTING = "REALTIME_V3_O1_EPISODES_PREMIUM_MULTIPLE_PCT"
O1_EPISODES_MAX_ALERTS_SETTING = "REALTIME_V3_O1_EPISODES_MAX_ALERTS"
O1_EPISODES_INVALIDATION_BP_SETTING = "REALTIME_V3_O1_EPISODES_INVALIDATION_BP"
O1_EPISODES_RECORD_EVENT = "O1_EPISODES_RECORD"
O1_EPISODES_RECORD_FAILED_EVENT = "O1_EPISODES_RECORD_FAILED"
RECORD_CONTRACT = "o1-episodes-record-v1"
BLOCK_CONTRACT = "o1-episode-block-v1"
# The projection key of the episode block on an opening alert (ON only).
EPISODE_BLOCK_KEY = "o1_episode"
SESSION_ALERT_SCHEMA = "options-alert-session-v1"
WINDOW_LOOKBACK = timedelta(minutes=10)  # the O1 read of a window (advanced_options_native_loader._load_o1)
# Docker splits a log line over 16 KiB: the lists are capped, the counts are always complete.
_EPISODES_LISTED = 16
_WINDOW_LISTED = 24
_CONTRACTS_LISTED = 6
LINE_BYTE_LIMIT = 15000
_PENDING_WINDOWS = 8

OPEN, ALLOW, SUPPRESS, ALERTED = "OPEN", "ALLOW", "SUPPRESS", "ALERTED"
O1EP_INVALIDATED = "O1EP_INVALIDATED"
O1EP_MAX_ALERTS = "O1EP_MAX_ALERTS"
O1EP_MIN_GAP = "O1EP_MIN_GAP"
O1EP_PREMIUM_BELOW = "O1EP_PREMIUM_BELOW"
O1EP_PREMIUM_UNKNOWN = "O1EP_PREMIUM_UNKNOWN"


class O1EpisodesMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def o1_episodes_mode(environ: Mapping[str, str] | None = None) -> O1EpisodesMode:
    """`REALTIME_V3_O1_EPISODES`; unset or blank is OFF. Anything but OFF, RECORD or ON raises (never silently OFF)."""
    raw = (os.environ if environ is None else environ).get(O1_EPISODES_SETTING)
    value = (raw or "").strip().upper() or O1EpisodesMode.OFF.value
    try:
        return O1EpisodesMode(value)
    except ValueError:
        raise ValueError(f"{O1_EPISODES_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


class O1EpisodePolicy(Contract):
    """G106 O1 episodes: one alert per (underlying, direction) per session unless the episode grows (gap, premium, cap)
    and the underlying has not moved against it."""

    version: Literal["o1_alert_episodes_v1"] = "o1_alert_episodes_v1"
    episode_key: Literal["UNDERLYING_AND_DIRECTION_PER_SESSION"] = "UNDERLYING_AND_DIRECTION_PER_SESSION"
    scope: Literal["NEW_O1_SESSION_ALERTS_ONLY"] = "NEW_O1_SESSION_ALERTS_ONLY"
    realert_minimum_gap_minutes: int = Field(default=60, ge=0, strict=True)
    realert_minimum_premium_multiple_pct: int = Field(default=200, ge=0, strict=True)
    maximum_alerts_per_key: int = Field(default=2, ge=1, strict=True)
    invalidation_adverse_move_bp: int = Field(default=150, ge=1, strict=True)
    signal_premium_basis: Literal["PREMIUM_OF_THE_ALERT_SELECTED_TRADES_IN_ITS_WINDOW_READ"] = (
        "PREMIUM_OF_THE_ALERT_SELECTED_TRADES_IN_ITS_WINDOW_READ")
    spot_basis: Literal["LATEST_1M_RAW_BAR_CLOSE_KNOWN_BY_THE_CUTOFF"] = "LATEST_1M_RAW_BAR_CLOSE_KNOWN_BY_THE_CUTOFF"
    invalidation_basis: Literal["ANY_1M_BAR_CLOSE_AFTER_THE_OPENING_ALERT_BEYOND_THE_MOVE"] = (
        "ANY_1M_BAR_CLOSE_AFTER_THE_OPENING_ALERT_BEYOND_THE_MOVE")
    execution_permission: Literal[False] = False


O1_EPISODE_POLICY = O1EpisodePolicy()


def o1_episode_policy_from(environ: Mapping[str, str]) -> O1EpisodePolicy:
    """The effective policy for these settings; every value unset = `O1_EPISODE_POLICY` itself. A set value must be a
    non-negative integer (digits only); the alert cap and the invalidation move must be at least 1."""
    default = O1_EPISODE_POLICY
    values = {
        "realert_minimum_gap_minutes": non_negative_int_setting(
            environ, O1_EPISODES_MIN_GAP_MINUTES_SETTING, default.realert_minimum_gap_minutes),
        "realert_minimum_premium_multiple_pct": non_negative_int_setting(
            environ, O1_EPISODES_PREMIUM_MULTIPLE_PCT_SETTING, default.realert_minimum_premium_multiple_pct),
        "maximum_alerts_per_key": non_negative_int_setting(
            environ, O1_EPISODES_MAX_ALERTS_SETTING, default.maximum_alerts_per_key),
        "invalidation_adverse_move_bp": non_negative_int_setting(
            environ, O1_EPISODES_INVALIDATION_BP_SETTING, default.invalidation_adverse_move_bp),
    }
    for key, name in (("maximum_alerts_per_key", O1_EPISODES_MAX_ALERTS_SETTING),
                      ("invalidation_adverse_move_bp", O1_EPISODES_INVALIDATION_BP_SETTING)):
        if values[key] < 1:
            raise ValueError(f"{name} must be at least 1, not {values[key]}")
    if all(values[key] == getattr(default, key) for key in values):
        return default
    return O1EpisodePolicy(**values)


@lru_cache(maxsize=1)
def o1_episode_policy() -> O1EpisodePolicy:
    """The process's episode policy, read once from the environment (the window service calls this at start)."""
    return o1_episode_policy_from(os.environ)


# --- inputs (the loader's guarded read) -----------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class StoredO1Alert:
    """One stored O1 session alert of the session (`rt.alert_current`)."""

    alert_id: str
    underlyer: str
    direction: int
    contract_id: int
    variant: str
    contract_ticker: str | None
    triggered_at: datetime
    first_cutoff: datetime | None
    event_ids: tuple[str, ...]
    episode_block: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class O1EpisodeInputs:
    """What the window's episode decision reads: the session's stored O1 alerts (triggered by the cutoff), the premium
    and time of their trades and of this window's O1 trades, the 1m bar closes of their underlyings known by the cutoff.
    `inputs_reason` is set when the read failed (then everything else is empty)."""

    session_date: date
    cutoff: datetime
    alerts: tuple[StoredO1Alert, ...] = ()
    trades: Mapping[str, tuple[datetime, Decimal]] = field(default_factory=dict)
    spots: Mapping[str, tuple[tuple[datetime, Decimal], ...]] = field(default_factory=dict)
    inputs_reason: str | None = None
    window_event_ids: tuple[str, ...] = ()

    def identity(self) -> tuple:
        """Joins the O1 lane's input identity under ON only: exactly what the decision reads (the stored alerts as
        signals with their opening premium, this window's trades, the bars). The `o1_episode` blocks and the later hits
        on stored alerts are left out: they change when this window's own projection is written, so a retry of the
        window after that write still has the same identity."""
        return (
            self.inputs_reason,
            tuple((s.alert_id, s.triggered_at.isoformat(), None if s.first_cutoff is None else s.first_cutoff.isoformat(),
                   s.direction, s.underlyer, None if s.premium is None else str(s.premium))
                  for s in stored_signals(self)),
            tuple((key, self.trades[key][0].isoformat(), str(self.trades[key][1]))
                  for key in sorted(set(self.window_event_ids)) if key in self.trades),
            tuple((name, tuple((at.isoformat(), str(close)) for at, close in path))
                  for name, path in sorted(self.spots.items())),
        )


_ALERTS_SQL = (
    "SELECT alert_id::text AS alert_id,projection->>'underlyer' AS underlyer,projection->>'direction' AS direction,"
    "projection->>'contract_id' AS contract_id,projection->>'triggered_at' AS triggered_at,"
    "projection->>'scheduled_cycle' AS scheduled_cycle,projection->'original_package'->'legs' AS legs,"
    "projection->'event_ids' AS event_ids,projection->'" + EPISODE_BLOCK_KEY + "' AS episode "
    "FROM rt.alert_current WHERE alert_kind='OPTION' AND as_of>=%s AND as_of<%s AND projection_schema=%s "
    "AND projection->>'session_date'=%s AND projection->>'detector_id'='O1' "
    "ORDER BY projection->>'triggered_at',alert_id"
)
_TRADES_SQL = (
    "SELECT event_id::text AS event_id,event_time,premium FROM rt.option_flow_events "
    "WHERE event_id=ANY(%s::uuid[]) AND event_time>%s AND event_time<=%s"
)


def _utc(value) -> datetime:
    parsed = datetime.fromisoformat(str(value)) if not isinstance(value, datetime) else value
    return parsed.astimezone(UTC)


def _json(value):
    return json.loads(value) if isinstance(value, (str, bytes)) else value


def _stored_alert(row: Mapping[str, object]) -> StoredO1Alert | None:
    direction = row.get("direction")
    legs = sorted(_json(row.get("legs")) or (), key=lambda leg: int(leg.get("leg_index") or 0))
    if direction in (None, "") or row.get("contract_id") in (None, "") or not row.get("triggered_at"):
        return None
    variant = f"O1_DEBIT_VERTICAL:{legs[1]['contract_id']}" if len(legs) == 2 else ""
    block = _json(row.get("episode"))
    return StoredO1Alert(
        alert_id=str(row["alert_id"]), underlyer=str(row["underlyer"]).upper(), direction=int(direction),
        contract_id=int(row["contract_id"]), variant=variant,
        contract_ticker=str(legs[0]["contract_ticker"]) if legs and legs[0].get("contract_ticker") else None,
        triggered_at=_utc(row["triggered_at"]),
        first_cutoff=_utc(row["scheduled_cycle"]) if row.get("scheduled_cycle") else None,
        event_ids=tuple(str(value) for value in (_json(row.get("event_ids")) or ())),
        episode_block=block if isinstance(block, Mapping) else None,
    )


def _load_spot_paths(cursor, underlyings: Sequence[str], start: datetime, cutoff: datetime) -> dict:
    """The 1m RAW bar closes (bar_end, close) of each underlying from `start`, as known by `cutoff` (the window's spot
    read: `advanced_options_p8_production.load_selection_spot_rows_cursor`, over a range)."""
    from .equity_bar_repository import KNOWN_BY_OBSERVED, load_known_bar_revisions_cursor

    if not underlyings:
        return {}
    cursor.execute(
        "SELECT DISTINCT instrument.instrument_id FROM ref.security_revisions AS security "
        "JOIN rt.instruments AS instrument ON instrument.security_id=security.security_id "
        "AND instrument.contract_id IS NULL WHERE security.ticker=ANY(%s)",
        (list(underlyings),),
    )
    instrument_ids = tuple(int(row["instrument_id"]) for row in cursor.fetchall())
    loaded = load_known_bar_revisions_cursor(
        cursor, instrument_ids=instrument_ids, interval="1m", basis="RAW_UNADJUSTED", known_by=cutoff, start=start,
        scopes=("RTH", "EXTENDED", "FULL_DAY"), clocks=KNOWN_BY_OBSERVED,
    )
    best: dict[tuple[str, datetime], tuple] = {}
    for row in (row for rows in loaded.values() for row in rows):
        ticker = str(row["ticker"]).upper()
        if ticker not in underlyings or row["bar_end"] > cutoff:
            continue
        key = (ticker, row["bar_end"])
        if key not in best or row["observed_at"] > best[key][0]:
            best[key] = (row["observed_at"], Decimal(str(row["close_price"])))
    paths: dict[str, list] = {}
    for (ticker, bar_end), (_observed, close) in sorted(best.items()):
        paths.setdefault(ticker, []).append((bar_end, close))
    return {key: tuple(value) for key, value in paths.items()}


def load_o1_episode_inputs_guarded(cursor, *, session_date: date, cutoff: datetime, window_event_ids: Iterable[str],
                                   window_underlyers: Iterable[str]) -> O1EpisodeInputs:
    """RECORD / ON: one guarded read (a savepoint in the loader's read-only transaction). Never raises: a failure is
    `inputs_reason` (the line says INPUTS_UNAVAILABLE; ON lets the window's alerts through)."""
    session_date = date.fromisoformat(str(session_date))
    day = datetime.combine(session_date, clock_time(0), tzinfo=UTC)
    try:
        cursor.execute("SAVEPOINT o1_episode_inputs")
    except Exception as error:  # noqa: BLE001
        return O1EpisodeInputs(session_date, cutoff, inputs_reason=type(error).__name__)
    try:
        cursor.execute(_ALERTS_SQL, (day, day + timedelta(hours=36), SESSION_ALERT_SCHEMA, str(session_date)))
        # The alerts of earlier windows only: an alert this window's own projection stored (a retry after that write)
        # is judged again as this window's, so a retry decides, and identifies, exactly as the first attempt.
        alerts = tuple(
            alert for alert in (_stored_alert(row) for row in cursor.fetchall())
            if alert is not None and (alert.first_cutoff or alert.triggered_at) < cutoff
        )
        window_event_ids = tuple(sorted({str(value) for value in window_event_ids}))
        event_ids = sorted({*window_event_ids, *(e for a in alerts for e in a.event_ids)})
        trades = {}
        if event_ids:
            cursor.execute(_TRADES_SQL, (event_ids, day - timedelta(hours=12), cutoff))
            trades = {str(row["event_id"]): (_utc(row["event_time"]), Decimal(str(row["premium"])))
                      for row in cursor.fetchall() if row["premium"] is not None}
        underlyings = sorted({a.underlyer for a in alerts} | {str(u).upper() for u in window_underlyers})
        start = min([a.triggered_at for a in alerts] + [cutoff - WINDOW_LOOKBACK]) - timedelta(minutes=5)
        spots = _load_spot_paths(cursor, underlyings, start, cutoff)
    except Exception as error:  # noqa: BLE001
        try:
            cursor.execute("ROLLBACK TO SAVEPOINT o1_episode_inputs")
        except Exception:  # noqa: BLE001
            pass
        return O1EpisodeInputs(session_date, cutoff, inputs_reason=f"{type(error).__name__}: {error}"[:160])
    cursor.execute("RELEASE SAVEPOINT o1_episode_inputs")
    return O1EpisodeInputs(session_date, cutoff, alerts, trades, spots, window_event_ids=window_event_ids)


# --- the rule (pure) -----------------------------------------------------------------------------------------------------

@dataclass(slots=True)
class Signal:
    """A new O1 session alert to judge: a stored one (RECORD re-simulation / ON history) or one of this window's."""

    alert_id: str
    underlyer: str
    direction: int
    contract_id: int
    variant: str
    contract_ticker: str | None
    triggered_at: datetime
    premium: Decimal | None
    stored: bool
    event_ids: tuple[str, ...] = ()
    first_cutoff: datetime | None = None  # the window it was (or is) judged in: the order the windows ran in
    decision: str | None = None
    reasons: tuple[str, ...] = ()
    episode_key: str | None = None


@dataclass(slots=True)
class Episode:
    key: str
    underlyer: str
    direction: int
    opened_at: datetime
    opening_alert_id: str
    opening_contract: str | None
    opening_premium: Decimal | None
    opening_spot: Decimal | None
    alerts: list = field(default_factory=list)          # [(alert_id, at, contract)]
    folded: list = field(default_factory=list)          # [(alert_id, contract, premium, at, stored)]
    invalidated_at: datetime | None = None
    invalidation_spot: Decimal | None = None
    current_spot: Decimal | None = None
    current_move_bp: float | None = None
    worst_move_bp: float | None = None
    prior_block: Mapping[str, object] | None = None     # ON: the stored block of the opening alert

    @property
    def last_alert_at(self) -> datetime:
        return max(at for _id, at, _contract in self.alerts)


def episode_key(underlyer: str, direction: int) -> str:
    return f"{underlyer}:{'BULLISH' if direction > 0 else 'BEARISH'}"


def spot_at(path: Sequence[tuple[datetime, Decimal]], at: datetime) -> Decimal | None:
    """The latest 1m close whose bar ended at or before `at`."""
    value = None
    for bar_end, close in path:
        if bar_end > at:
            break
        value = close
    return value


def _move_bp(spot: Decimal, opening: Decimal, direction: int) -> float:
    """The move since the opening spot in the episode's direction, in basis points (negative = against it)."""
    return float((spot / opening - 1) * 10_000 * direction)


def _track_spot(episode: Episode, path: Sequence[tuple[datetime, Decimal]], policy: O1EpisodePolicy) -> None:
    if episode.opening_spot is None or not episode.opening_spot:
        return
    worst = None
    for bar_end, close in path:
        if bar_end <= episode.opened_at:
            continue
        move = _move_bp(close, episode.opening_spot, episode.direction)
        worst = move if worst is None else min(worst, move)
        if episode.invalidated_at is None and -move > policy.invalidation_adverse_move_bp:
            episode.invalidated_at, episode.invalidation_spot = bar_end, close
        episode.current_spot, episode.current_move_bp = close, round(move, 1)
    episode.worst_move_bp = None if worst is None else round(worst, 1)


def judge(signal: Signal, episode: Episode, policy: O1EpisodePolicy) -> tuple[str, tuple[str, ...]]:
    """ALLOW or SUPPRESS (with every reason that holds) for a new alert on an open episode."""
    reasons = []
    if episode.invalidated_at is not None and episode.invalidated_at <= signal.triggered_at:
        reasons.append(O1EP_INVALIDATED)
    if len(episode.alerts) >= policy.maximum_alerts_per_key:
        reasons.append(O1EP_MAX_ALERTS)
    if signal.triggered_at - episode.last_alert_at < timedelta(minutes=policy.realert_minimum_gap_minutes):
        reasons.append(O1EP_MIN_GAP)
    if signal.premium is None or episode.opening_premium is None:
        reasons.append(O1EP_PREMIUM_UNKNOWN)
    elif signal.premium * 100 < episode.opening_premium * policy.realert_minimum_premium_multiple_pct:
        reasons.append(O1EP_PREMIUM_BELOW)
    return (SUPPRESS if reasons else ALLOW), tuple(reasons)


def run_episodes(stored: Sequence[Signal], window: Sequence[Signal], *, policy: O1EpisodePolicy,
                 spots: Mapping[str, Sequence[tuple[datetime, Decimal]]], all_stored_count: bool,
                 blocks: Mapping[str, Mapping[str, object]] | None = None) -> dict[str, Episode]:
    """Judge the stored new alerts (in the order their windows ran, then by trigger time), then this window's (by
    trigger time); set each signal's decision.
    `all_stored_count` (ON): every stored alert was delivered and counts as an alert (ALERTED); False (RECORD): the
    stored alerts are re-judged as if the rule had run all session. Returns the episodes by key."""
    episodes: dict[str, Episode] = {}
    order = lambda item: (item.first_cutoff or item.triggered_at, item.triggered_at, item.alert_id)  # noqa: E731
    for signal in [*sorted(stored, key=order), *sorted(window, key=order)]:
        key = episode_key(signal.underlyer, signal.direction)
        signal.episode_key = key
        episode = episodes.get(key)
        if episode is None:
            path = spots.get(signal.underlyer, ())
            episode = Episode(key=key, underlyer=signal.underlyer, direction=signal.direction,
                              opened_at=signal.triggered_at, opening_alert_id=signal.alert_id,
                              opening_contract=signal.contract_ticker, opening_premium=signal.premium,
                              opening_spot=spot_at(path, signal.triggered_at),
                              prior_block=(blocks or {}).get(signal.alert_id))
            _track_spot(episode, path, policy)
            episode.alerts.append((signal.alert_id, signal.triggered_at, signal.contract_ticker))
            episodes[key] = episode
            signal.decision, signal.reasons = OPEN, ()
            continue
        if signal.stored and all_stored_count:
            signal.decision, signal.reasons = ALERTED, ()
        else:
            signal.decision, signal.reasons = judge(signal, episode, policy)
        if signal.decision == SUPPRESS:
            episode.folded.append((signal.alert_id, signal.contract_ticker, signal.premium, signal.triggered_at,
                                   signal.stored))
        else:
            episode.alerts.append((signal.alert_id, signal.triggered_at, signal.contract_ticker))
    return episodes


# --- the executor's use ---------------------------------------------------------------------------------------------------

def _alert_identity(evaluation, session_date) -> tuple[str, int, str] | None:
    """(session alert id, contract id, variant) of an O1 evaluation with a package, as the session alert writer keys
    it (`advanced_options_projection._session_alert_key` / `options_session_alert_id`); None otherwise."""
    from .advanced_options_projection import _session_alert_key, options_session_alert_id

    key = _session_alert_key(evaluation)
    if key is None:
        return None
    contract_id, variant = key
    return str(options_session_alert_id(str(session_date), "O1", contract_id, variant)), int(contract_id), variant


def _evaluation_direction(evaluation) -> int:
    """The alert row's direction (`advanced_options_projection._alert_run_row`): the payload's, else the debit
    vertical's, else the legs' (a long call +1, a long put -1)."""
    from .advanced_options_projection import _legs_direction, _o1_debit_vertical_fields

    payload = evaluation.payload if isinstance(evaluation.payload, Mapping) else {}
    direction = payload.get("direction")
    if direction in (-1, 1):
        return int(direction)
    vertical = _o1_debit_vertical_fields(evaluation.package)
    if vertical:
        return int(vertical["direction"])
    return _legs_direction(evaluation.package.legs) or 1


def _premium(event_ids: Iterable[str], trades: Mapping[str, tuple[datetime, Decimal]],
             since: datetime | None = None, until: datetime | None = None) -> Decimal | None:
    values = [trades[e][1] for e in set(event_ids) if e in trades
              and (since is None or trades[e][0] > since) and (until is None or trades[e][0] <= until)]
    return sum(values, Decimal(0)) if values else None


def stored_signals(inputs: O1EpisodeInputs) -> list[Signal]:
    rows = []
    for alert in inputs.alerts:
        premium = None
        if alert.first_cutoff is not None:
            premium = _premium(alert.event_ids, inputs.trades, alert.first_cutoff - WINDOW_LOOKBACK, alert.first_cutoff)
        rows.append(Signal(alert_id=alert.alert_id, underlyer=alert.underlyer, direction=alert.direction,
                           contract_id=alert.contract_id, variant=alert.variant, contract_ticker=alert.contract_ticker,
                           triggered_at=alert.triggered_at, premium=premium, stored=True, event_ids=alert.event_ids,
                           first_cutoff=alert.first_cutoff))
    return rows


def _iso(value) -> str | None:
    return None if value is None else value.isoformat()


def _money(value) -> str | None:
    return None if value is None else str(Decimal(value).quantize(Decimal("1")))


def episode_document(episode: Episode, *, cutoff: datetime) -> dict:
    folded_premium = sum((premium for _a, _c, premium, _t, _s in episode.folded if premium is not None), Decimal(0))
    contracts = sorted({contract for _a, contract, _p, _t, _s in episode.folded if contract})
    invalidated = episode.invalidated_at is not None
    return {
        "key": episode.key, "underlyer": episode.underlyer, "direction": episode.direction,
        "state": "INVALIDATED" if invalidated else "OPEN",
        "opened_at": _iso(episode.opened_at), "opening_alert_id": episode.opening_alert_id,
        "opening_contract": episode.opening_contract, "opening_premium": _money(episode.opening_premium),
        "opening_spot": None if episode.opening_spot is None else str(episode.opening_spot),
        "alerts_today": len(episode.alerts), "alert_ids": [a for a, _t, _c in episode.alerts][:_CONTRACTS_LISTED],
        "last_alert_at": _iso(episode.last_alert_at),
        "folded_signals": len(episode.folded), "folded_contracts": len(contracts),
        "folded_contract_list": contracts[:_CONTRACTS_LISTED], "folded_premium": _money(folded_premium),
        "last_folded_at": _iso(max((at for _a, _c, _p, at, _s in episode.folded), default=None)),
        "invalidated": invalidated, "invalidated_at": _iso(episode.invalidated_at),
        "invalidation_spot": None if episode.invalidation_spot is None else str(episode.invalidation_spot),
        "current_spot": None if episode.current_spot is None else str(episode.current_spot),
        "current_move_bp": episode.current_move_bp, "worst_move_bp": episode.worst_move_bp,
        "spot_available": episode.opening_spot is not None,
        "minutes_open": round((cutoff - episode.opened_at).total_seconds() / 60, 1),
    }


def _signal_document(signal: Signal) -> dict:
    return {
        "alert_id": signal.alert_id, "contract": signal.contract_ticker, "variant": signal.variant or None,
        "underlyer": signal.underlyer, "direction": signal.direction, "episode": signal.episode_key,
        "triggered_at": _iso(signal.triggered_at), "premium": _money(signal.premium), "decision": signal.decision,
        "reasons": list(signal.reasons),
    }


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(f"{event} {json.dumps(document, sort_keys=True, default=str)}", file=sys.stderr, flush=True)


def _fit(document: dict) -> dict:
    """Keep the line under LINE_BYTE_LIMIT (Docker splits a line over 16 KiB): the lists shrink, the counts stay whole;
    `truncated` says it happened."""
    size = lambda: len(json.dumps(document, sort_keys=True, default=str))  # noqa: E731
    for key, keep in (("window", 12), ("episodes", 8), ("would_allow_alerts", 8), ("would_suppress_alerts", 12),
                      ("window", 0), ("episodes", 4), ("would_suppress_alerts", 4)):
        if size() <= LINE_BYTE_LIMIT:
            break
        if isinstance(document.get(key), list) and len(document[key]) > keep:
            document[key] = document[key][:keep]
            document["truncated"] = True
    return document


_PENDING_BLOCKS: dict[str, dict[str, dict]] = {}


def pending_o1_episode_blocks() -> Mapping[str, Mapping[str, dict]]:
    return _PENDING_BLOCKS


def _queue_blocks(cutoff: datetime, blocks: Mapping[str, dict]) -> None:
    _PENDING_BLOCKS[cutoff.astimezone(UTC).isoformat()] = dict(blocks)
    while len(_PENDING_BLOCKS) > _PENDING_WINDOWS:
        _PENDING_BLOCKS.pop(next(iter(_PENDING_BLOCKS)))


def _block(episode: Episode, window_folds: Sequence[Signal], trades, *, cutoff: datetime, policy) -> dict:
    """ON: the opening alert's `o1_episode` block, cumulative (its stored block + this window's folds). A rerun of a
    window already in the stored block adds nothing (`last_window_cutoff`)."""
    prior = dict(episode.prior_block or {})
    last = prior.get("last_window_cutoff")
    already = last is not None and _utc(last) >= cutoff
    signals = int(prior.get("folded_signals") or 0)
    premium = Decimal(str(prior.get("folded_premium") or 0))
    contracts = list(prior.get("folded_contract_list") or ())
    count = int(prior.get("folded_contracts") or len(contracts))
    last_folded = prior.get("last_folded_at")
    since = _utc(last) if last is not None else None
    if not already:
        for signal in window_folds:
            signals += 1
            premium += _premium(signal.event_ids, trades, since=since) or Decimal(0)
            if signal.contract_ticker and signal.contract_ticker not in contracts:
                contracts.append(signal.contract_ticker)
                count += 1
            last_folded = max(filter(None, [last_folded, signal.triggered_at.isoformat()]))
    document = episode_document(episode, cutoff=cutoff)
    return {
        "contract": BLOCK_CONTRACT, "policy": policy.version, "policy_sha256": policy.sha256,
        "key": episode.key, "state": document["state"], "opened_at": document["opened_at"],
        "opening_premium": document["opening_premium"], "opening_spot": document["opening_spot"],
        "alerts_today": document["alerts_today"], "alert_ids": document["alert_ids"],
        "folded_signals": signals, "folded_contracts": count, "folded_contract_list": contracts[:_CONTRACTS_LISTED],
        "folded_premium": _money(premium), "last_folded_at": last_folded,
        "invalidated": document["invalidated"], "invalidated_at": document["invalidated_at"],
        "last_window_cutoff": (last if already else cutoff.isoformat()),
    }


@dataclass(slots=True)
class O1EpisodeRun:
    """One window's episode rule in the O1 executor (built only when the switch is not OFF)."""

    mode: O1EpisodesMode
    policy: O1EpisodePolicy
    inputs: O1EpisodeInputs | None

    @classmethod
    def for_window(cls, inputs) -> "O1EpisodeRun | None":
        mode = o1_episodes_mode()
        if mode is O1EpisodesMode.OFF:
            return None
        return cls(mode, o1_episode_policy(), inputs)

    def apply(self, evaluations: list, cutoff: datetime) -> list:
        """The window's evaluations: RECORD returns them as they are; ON makes every SELECTED event of a suppressed new
        alert an OBSERVATION without a package. Logs the window's line. Never raises (any error: today's evaluations
        and an O1_EPISODES_RECORD_FAILED line)."""
        try:
            return self._apply(evaluations, cutoff)
        except Exception as error:  # noqa: BLE001 - the rule never fails the window
            try:
                _print_line(O1_EPISODES_RECORD_FAILED_EVENT, {
                    "contract": RECORD_CONTRACT, "mode": self.mode.value, "status": "RECORD_FAILED",
                    "cutoff": cutoff.isoformat(), "reason": f"{type(error).__name__}: {error}"[:200]})
            except Exception:  # noqa: BLE001
                pass
            return evaluations

    def _apply(self, evaluations: list, cutoff: datetime) -> list:
        on = self.mode is O1EpisodesMode.ON
        inputs = self.inputs
        status = "OK"
        if inputs is None:
            status, inputs = "INPUTS_UNAVAILABLE:NOT_LOADED", None
        elif inputs.inputs_reason is not None:
            status = f"INPUTS_UNAVAILABLE:{inputs.inputs_reason}"
        document = {
            "contract": RECORD_CONTRACT, "mode": self.mode.value, "status": status, "applied": on,
            "policy": self.policy.version, "policy_sha256": self.policy.sha256,
            "realert_minimum_gap_minutes": self.policy.realert_minimum_gap_minutes,
            "realert_minimum_premium_multiple_pct": self.policy.realert_minimum_premium_multiple_pct,
            "maximum_alerts_per_key": self.policy.maximum_alerts_per_key,
            "invalidation_adverse_move_bp": self.policy.invalidation_adverse_move_bp,
            "cutoff": cutoff.isoformat(),
        }
        if status != "OK":
            # Fail open: today's alerts (ON included); the line says why.
            document.update(session_date=None if inputs is None else str(inputs.session_date), window_new_alerts=None)
            _print_line(O1_EPISODES_RECORD_EVENT, document)
            return evaluations
        session_date, window_cutoff = inputs.session_date, inputs.cutoff
        stored = stored_signals(inputs)
        stored_ids = {s.alert_id for s in stored}
        groups: dict[str, list] = {}
        identities: dict[str, tuple[int, str]] = {}
        repeat_hits = 0
        for index, evaluation in enumerate(evaluations):
            if evaluation.selection_status != "SELECTED" or evaluation.package is None:
                continue
            identity = _alert_identity(evaluation, session_date)
            if identity is None:
                continue
            alert_id, contract_id, variant = identity
            groups.setdefault(alert_id, []).append(index)
            identities[alert_id] = (contract_id, variant)
        window: list[Signal] = []
        for alert_id, indexes in groups.items():
            members = [evaluations[i] for i in indexes]
            if alert_id in stored_ids:
                repeat_hits += len(members)
                continue
            first = min(members, key=lambda item: (item.observed_at, str(item.event_id)))
            event_ids = tuple(sorted({str(item.event_id) for item in members}))
            contract_id, variant = identities[alert_id]
            window.append(Signal(
                alert_id=alert_id, underlyer=first.underlyer.upper(), direction=_evaluation_direction(first),
                contract_id=contract_id, variant=variant, contract_ticker=first.package.legs[0].contract_ticker,
                triggered_at=first.observed_at, premium=_premium(event_ids, inputs.trades), stored=False,
                event_ids=event_ids, first_cutoff=window_cutoff,
            ))
        # Under ON only the delivered alerts are stored (all count); RECORD re-simulates the session from its first alert.
        blocks = {a.alert_id: a.episode_block for a in inputs.alerts if a.episode_block is not None} if on else None
        episodes = run_episodes(stored, window, policy=self.policy, spots=inputs.spots, all_stored_count=on,
                                blocks=blocks)
        suppressed = {s.alert_id for s in window if s.decision == SUPPRESS}
        stored_folded = sum(1 for s in stored if s.decision == SUPPRESS)
        reasons: dict[str, int] = {}
        for signal in window:
            for reason in signal.reasons:
                reasons[reason] = reasons.get(reason, 0) + 1
        ordered = sorted(episodes.values(), key=lambda e: (-len(e.folded) - len(e.alerts), e.key))
        document.update({
            "session_date": str(session_date),
            "stored_alerts": len(stored), "stored_would_fold": None if on else stored_folded,
            "episodes_total": len(episodes),
            "episodes_invalidated": sum(e.invalidated_at is not None for e in episodes.values()),
            "window_new_alerts": len(window), "window_repeat_hits": repeat_hits,
            "would_open": sum(s.decision == OPEN for s in window),
            "would_allow": sum(s.decision == ALLOW for s in window),
            "would_suppress": len(suppressed),
            "would_suppress_reasons": dict(sorted(reasons.items())),
            "would_suppress_alerts": [
                {"alert_id": s.alert_id, "contract": s.contract_ticker, "episode": s.episode_key}
                for s in sorted(window, key=lambda s: (s.triggered_at, s.alert_id)) if s.decision == SUPPRESS
            ][:_WINDOW_LISTED],
            "would_allow_alerts": [
                {"alert_id": s.alert_id, "contract": s.contract_ticker, "episode": s.episode_key, "decision": s.decision}
                for s in sorted(window, key=lambda s: (s.triggered_at, s.alert_id)) if s.decision in (OPEN, ALLOW)
            ][:_WINDOW_LISTED],
            "window": [_signal_document(s) for s in sorted(window, key=lambda s: (s.triggered_at, s.alert_id))
                       ][:_WINDOW_LISTED],
            "episodes_listed": min(len(ordered), _EPISODES_LISTED),
            "episodes": [episode_document(e, cutoff=window_cutoff) for e in ordered][:_EPISODES_LISTED],
        })
        if not on:
            _print_line(O1_EPISODES_RECORD_EVENT, _fit(document))
            return evaluations
        # ON: the suppressed new alerts' events become observations (no package); each episode that changed this window
        # (a fold, a further alert, or newly invalidated) gets its opening alert's block, written after the session
        # alert writer of this window's projection.
        blocks_out = {}
        for episode in episodes.values():
            folds = [s for s in window if s.decision == SUPPRESS and s.episode_key == episode.key]
            allowed = any(s.decision == ALLOW and s.episode_key == episode.key for s in window)
            newly_invalidated = (episode.prior_block is not None and episode.invalidated_at is not None
                                 and not episode.prior_block.get("invalidated"))
            if folds or allowed or newly_invalidated:
                blocks_out[episode.opening_alert_id] = _block(episode, folds, inputs.trades, cutoff=window_cutoff,
                                                              policy=self.policy)
        if blocks_out:
            _queue_blocks(cutoff, blocks_out)
        document["blocks_queued"] = len(blocks_out)
        _print_line(O1_EPISODES_RECORD_EVENT, _fit(document))
        if not suppressed:
            return evaluations
        demote = {i for alert_id in suppressed for i in groups[alert_id]}
        return [
            replace(evaluation, selection_status="OBSERVATION", package=None) if index in demote else evaluation
            for index, evaluation in enumerate(evaluations)
        ]


# --- ON: the opening alert's block (the session alert writer's transaction) -----------------------------------------------

def apply_pending_o1_episode_blocks_cursor(cursor, *, cutoff: datetime) -> int:
    """ON only (nothing queued otherwise: no SQL): write this window's `o1_episode` blocks onto the stored opening
    alerts, in the projection's transaction, after the session alert writer (an opening alert inserted by this window
    is then stored). A missing row is skipped. Never raises: a failure rolls back to its savepoint and is one
    O1_EPISODES_RECORD_FAILED line. Returns the rows updated."""
    from .contracts import canonical_json, canonical_sha256

    key = cutoff.astimezone(UTC).isoformat()
    blocks = _PENDING_BLOCKS.pop(key, None)
    if not blocks:
        return 0
    try:
        cursor.execute("SAVEPOINT o1_episode_blocks")
    except Exception as error:  # noqa: BLE001
        _print_line(O1_EPISODES_RECORD_FAILED_EVENT, {"contract": RECORD_CONTRACT, "status": "BLOCK_WRITE_FAILED",
                                                      "cutoff": key, "reason": type(error).__name__})
        return 0
    written = 0
    try:
        cursor.execute(
            "SELECT alert_id::text AS alert_id,projection FROM rt.alert_current WHERE alert_id=ANY(%s::uuid[]) "
            "FOR UPDATE", (sorted(blocks),))
        rows = cursor.fetchall()
        for row in rows:
            record = dict(row) if isinstance(row, Mapping) else dict(zip(("alert_id", "projection"), row))
            projection = dict(_json(record["projection"]))
            projection[EPISODE_BLOCK_KEY] = blocks[str(record["alert_id"])]
            cursor.execute(
                "UPDATE rt.alert_current SET projection_sha256=%s,projection=%s,updated_at=clock_timestamp() "
                "WHERE alert_id=%s",
                (canonical_sha256(projection), canonical_json(projection), str(record["alert_id"])),
            )
            written += 1
    except Exception as error:  # noqa: BLE001
        try:
            cursor.execute("ROLLBACK TO SAVEPOINT o1_episode_blocks")
        except Exception:  # noqa: BLE001
            pass
        _print_line(O1_EPISODES_RECORD_FAILED_EVENT, {"contract": RECORD_CONTRACT, "status": "BLOCK_WRITE_FAILED",
                                                      "cutoff": key, "reason": f"{type(error).__name__}: {error}"[:200]})
        return 0
    cursor.execute("RELEASE SAVEPOINT o1_episode_blocks")
    return written


__all__ = [
    "ALERTED", "ALLOW", "BLOCK_CONTRACT", "EPISODE_BLOCK_KEY", "Episode", "O1EP_INVALIDATED", "O1EP_MAX_ALERTS",
    "O1EP_MIN_GAP", "O1EP_PREMIUM_BELOW", "O1EP_PREMIUM_UNKNOWN", "O1EpisodeInputs", "O1EpisodePolicy",
    "O1EpisodeRun", "O1EpisodesMode", "O1_EPISODES_INVALIDATION_BP_SETTING", "O1_EPISODES_MAX_ALERTS_SETTING",
    "O1_EPISODES_MIN_GAP_MINUTES_SETTING", "O1_EPISODES_PREMIUM_MULTIPLE_PCT_SETTING", "O1_EPISODES_RECORD_EVENT",
    "O1_EPISODES_RECORD_FAILED_EVENT", "O1_EPISODES_SETTING", "O1_EPISODE_POLICY", "OPEN", "SUPPRESS", "Signal",
    "StoredO1Alert", "apply_pending_o1_episode_blocks_cursor", "episode_document", "episode_key", "judge",
    "load_o1_episode_inputs_guarded", "o1_episode_policy", "o1_episode_policy_from", "o1_episodes_mode",
    "pending_o1_episode_blocks", "run_episodes", "spot_at", "stored_signals",
]
