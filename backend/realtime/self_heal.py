"""Self-heal checks: find stuck or failing work and report it, with the action that would fix it (G102 R21, Nov 11).

The user's rule (Nov 11): record-only for the first one or two sessions, then act. So by default every finding is
reported as a fault record (``realtime.fault_journal``) and nothing on live changes; a finding that has an automatic
fix is reported WOULD_ACT with the action named. Acting needs both ``REALTIME_V2_SELF_HEAL_MODE=ACT`` and the action in
``REALTIME_V2_SELF_HEAL_ACTIONS`` (comma separated), so each action is switched on on its own.

Checks (all read-only, one REPEATABLE READ snapshot):
- WINDOW_PAST_DEADLINE (R32): windows still PENDING or RUNNING after their deadline with no live lease. Options windows:
  action MARK_MISSED (the only action with an ACT path). Equity windows: reported only (the B20 sweep owns them).
- WINDOW_FAILED: each failed window today with its stage reasons.
- FEATURE_BREAKER (R21 layer 4): the last N Options windows failed for the same optional feature (O1 package selection,
  breadth, OS lanes). Action DISABLE_FEATURE_FOR_SESSION, record-only (no runtime switch exists yet, see FUTURE).
  Equity processing is never a breaker candidate.
- LANE_NOT_EVALUATED (R23): a detector lane BLOCKED or UNAVAILABLE in most of today's Options windows.
- PREPARATION_PARKED (R12, G21): Options preparation work during the session whose next retry is far away. Action
  REQUEUE_NOW, record-only (it moves a retry clock; needs the user's yes).
- STREAM_STALE (B14a) / STREAM_LAG (R28): an open stream session without a heartbeat, or an Options stream behind.
- LEASES_STALLED: no worker lease renewed for minutes while services are up (Nov 10 Docker Desktop wedge).
- WINDOW_RUNTIME (R35): Options windows using more than 60% of their release-to-deadline budget.

FUTURE:
- Breaker ACT: needs a per-session feature switch the window reads every cycle (a small table, so a migration and the
  user's yes); today O1 selection is only the window's env `REALTIME_V3_O1_PACKAGE_SELECTION`, read at start.
- MARK_MISSED: before switching it on, record a `rt.stage_runs` MISSED row like `orchestration_repository.record_missed`
  and confirm with a copy rehearsal that the Options window role and the session status treat MISSED as terminal.
- REQUEUE_NOW and END_STALE_SESSION: implement only with the user's yes (retry clocks and stream sessions on live).
- Stock Alerts batches (research-rule errors) and the Equity scheduler's own stage failures as breaker inputs.
- On servers: run `scripts/run_realtime_v2_self_heal.py` as its own service (compose or a cron job) instead of from the
  host collector.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .fault_journal import emit_fault

UTC = timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
SERVICE = "self-heal"
PAST_DEADLINE_GRACE = timedelta(minutes=2)
BREAKER_CONSECUTIVE_FAILURES = 3
LANE_MINIMUM_WINDOWS = 6
LANES_OFF_BY_DESIGN = frozenset({"O2"})
PARKED_RETRY_THRESHOLD = timedelta(minutes=30)
STREAM_HEARTBEAT_STALE = timedelta(minutes=3)
STREAM_PREVIOUS_SESSION_AGE = timedelta(hours=30)
OPTIONS_LAG_SECONDS = 120
LEASES_STALL = timedelta(minutes=3)
RUNTIME_SHARE_ALARM = 0.6
ACTIONS_WITH_ACT_PATH = frozenset({"MARK_MISSED"})
# Failure text -> optional feature it belongs to (case-insensitive substrings of the stage reasons).
FEATURE_SIGNATURES = (
    ("O1_PACKAGE_SELECTION", ("option_packages", "package evidence", "selected package", "package_entry",
                              "provider_tier")),
    ("OPTIONS_BREADTH", ("breadth",)),
    ("OS_LANES", ("os1", "os2", "stock_bar")),
)


@dataclass(frozen=True)
class Finding:
    kind: str
    severity: str
    message: str
    recoverable: bool
    key: str
    action: str = "NONE"
    context: dict = field(default_factory=dict)
    targets: tuple = ()


def session_date_at(now: datetime):
    return now.astimezone(NEW_YORK).date()


def in_session(now: datetime) -> bool:
    local = now.astimezone(NEW_YORK)
    return local.weekday() < 5 and time(9, 30) <= local.time() <= time(16, 0)


def attribute_feature(reasons: str) -> str | None:
    text = (reasons or "").lower()
    for feature, needles in FEATURE_SIGNATURES:
        if any(needle in text for needle in needles):
            return feature
    return None


_REASONS_SQL = (
    "coalesce((SELECT string_agg(s.stage_id || ':' || s.reason_codes::text, ' ' ORDER BY s.attempt) "
    "FROM rt.stage_runs s WHERE s.window_id=w.window_id AND s.state NOT IN ('COMPLETE','BLOCKED_PREREQ')),'')"
)


def check_windows_past_deadline(cursor, now: datetime) -> list[Finding]:
    cursor.execute(
        "SELECT w.window_id::text, w.window_key, w.state, w.cutoff, w.deadline FROM rt.window_runs w "
        "WHERE w.state IN ('PENDING','RUNNING') AND w.deadline < %s AND w.deadline > %s "
        "AND (w.lease_until IS NULL OR w.lease_until < %s) ORDER BY w.deadline LIMIT 500",
        (now - PAST_DEADLINE_GRACE, now - timedelta(days=7), now),
    )
    options, equity = [], []
    for window_id, key, state, cutoff, deadline in cursor.fetchall():
        (options if str(key).startswith("E:") else equity).append((window_id, key, state, cutoff, deadline))
    findings = []
    if options:
        per_day: dict[str, int] = {}
        for _id, _key, _state, cutoff, _deadline in options:
            day = cutoff.astimezone(NEW_YORK).date().isoformat()
            per_day[day] = per_day.get(day, 0) + 1
        findings.append(Finding(
            "WINDOW_PAST_DEADLINE", "ERROR", f"{len(options)} Options windows PENDING/RUNNING past their deadline",
            True, "OPTIONS", "MARK_MISSED",
            {"per_session": per_day, "windows": [f"{key} {state}" for _id, key, state, _c, _d in options[-30:]],
             "oldest_deadline": options[0][4].isoformat()},
            tuple(row[0] for row in options),
        ))
    if equity:
        findings.append(Finding(
            "WINDOW_PAST_DEADLINE", "ERROR", f"{len(equity)} Equity windows PENDING/RUNNING past their deadline",
            False, "EQUITY", "NONE",
            {"windows": [f"{key} {state}" for _id, key, state, _c, _d in equity[:30]],
             "note": "the Equity scheduler's missed-window sweep (B20) owns these"},
        ))
    return findings


def check_windows_today(cursor, now: datetime) -> list[Finding]:
    session = session_date_at(now)
    cursor.execute(
        f"SELECT w.window_key, w.cutoff, w.state, {_REASONS_SQL} FROM rt.window_runs w "
        "WHERE w.window_key LIKE %s OR w.window_key LIKE %s ORDER BY w.cutoff",
        (f"E:{session.isoformat()}%", f"advanced-{session:%Y%m%d}T%"),
    )
    rows = cursor.fetchall()
    findings = []
    for key, cutoff, state, reasons in rows:
        if state != "FAILED":
            continue
        family = "OPTIONS" if str(key).startswith("E:") else "EQUITY"
        feature = attribute_feature(reasons) if family == "OPTIONS" else None
        findings.append(Finding(
            "WINDOW_FAILED", "ERROR", f"{family} window {key} FAILED: {reasons[:300]}", True,
            f"{family}|{feature or (reasons or '')[:80]}", "NONE",
            {"window": key, "cutoff": cutoff.isoformat(), "reasons": reasons, "feature": feature},
        ))
    # Breaker: the trailing run of failed Options windows with one attributed feature.
    options = [(key, state, reasons) for key, _cutoff, state, reasons in rows if str(key).startswith("E:")]
    trailing = []
    for key, state, reasons in reversed(options):
        if state in ("PENDING", "RUNNING"):
            continue
        if state != "FAILED":
            break
        trailing.append((key, attribute_feature(reasons)))
    if len(trailing) >= BREAKER_CONSECUTIVE_FAILURES:
        features = {feature for _key, feature in trailing[:BREAKER_CONSECUTIVE_FAILURES]}
        if len(features) == 1 and None not in features:
            feature = features.pop()
            findings.append(Finding(
                "FEATURE_BREAKER", "WOULD_ACT",
                f"last {len(trailing)} Options windows failed in {feature}; would switch it off for the session",
                True, feature, "DISABLE_FEATURE_FOR_SESSION",
                {"feature": feature, "windows": [key for key, _f in trailing[:10]],
                 "how_today": "O1: REALTIME_V3_O1_PACKAGE_SELECTION=DISABLED on the window service (recreate)"},
            ))
    return findings


def check_lanes(cursor, now: datetime) -> list[Finding]:
    cursor.execute(
        "SELECT l->>'detector', l->>'state', count(*) FROM rt.detector_datasets d, "
        "jsonb_array_elements(d.summary->'lanes') l WHERE d.session_date=%s GROUP BY 1, 2",
        (session_date_at(now),),
    )
    totals: dict[str, int] = {}
    blocked: dict[str, int] = {}
    for detector, state, count in cursor.fetchall():
        totals[detector] = totals.get(detector, 0) + int(count)
        if state in ("BLOCKED", "UNAVAILABLE"):
            blocked[detector] = blocked.get(detector, 0) + int(count)
    findings = []
    for detector, total in sorted(totals.items()):
        bad = blocked.get(detector, 0)
        if detector in LANES_OFF_BY_DESIGN or total < LANE_MINIMUM_WINDOWS or bad * 2 <= total:
            continue
        findings.append(Finding(
            "LANE_NOT_EVALUATED", "DEGRADED", f"lane {detector} not evaluated in {bad} of {total} windows today",
            False, detector, "NONE", {"detector": detector, "not_evaluated": bad, "windows": total},
        ))
    return findings


def check_preparation_parked(cursor, now: datetime) -> list[Finding]:
    if not in_session(now):
        return []  # before the open the preparation waits for the open by design
    cursor.execute(
        "SELECT w.ticker, w.stage, w.status, w.next_retry_at, w.failure_streak, "
        "coalesce(w.blocked_reason, w.terminal_reason, '') FROM rt.advanced_options_preparation_work w "
        "JOIN rt.advanced_options_preparation_sessions s USING (revision_id) "
        "WHERE s.session_date=%s AND w.status <> 'COMPLETE' AND w.next_retry_at > %s ORDER BY w.next_retry_at",
        (session_date_at(now), now + PARKED_RETRY_THRESHOLD),
    )
    rows = cursor.fetchall()
    if not rows:
        return []
    return [Finding(
        "PREPARATION_PARKED", "WOULD_ACT", f"{len(rows)} Options preparation rows wait past {PARKED_RETRY_THRESHOLD}",
        True, "OPTIONS_PREPARATION", "REQUEUE_NOW",
        {"rows": [f"{ticker} {stage} {status} retry {retry.isoformat()} streak {streak} {reason[:80]}"
                  for ticker, stage, status, retry, streak, reason in rows[:20]]},
    )]


def check_streams(cursor, now: datetime) -> list[Finding]:
    cursor.execute(
        "SELECT feed, session_id::text, status, heartbeat_at, event_time_watermark FROM rt.stream_sessions "
        "WHERE ended_at IS NULL ORDER BY started_at"
    )
    findings = []
    for feed, session_id, status, heartbeat, watermark in cursor.fetchall():
        if status == "LIVE" and (heartbeat is None or heartbeat < now - STREAM_HEARTBEAT_STALE):
            context = {"feed": feed, "session": session_id[:8],
                       "heartbeat_at": None if heartbeat is None else heartbeat.isoformat()}
            if in_session(now):
                # FUTURE: action RESTART_ACQUISITION (R28's manual fix was a same-image restart of the acquisition).
                findings.append(Finding("STREAM_STALE", "ERROR", f"{feed} stream {session_id[:8]} has no heartbeat "
                                        f"since {heartbeat} during the session", True, str(feed), "NONE", context))
            elif heartbeat is not None and heartbeat > now - STREAM_PREVIOUS_SESSION_AGE:
                # B14a: the previous session's stream stays open until the next session's registration ends it.
                findings.append(Finding("STREAM_STALE", "DEGRADED", f"{feed} stream {session_id[:8]} from the previous "
                                        "session still open; the next session's registration ends it (B14a)", True,
                                        f"{feed}|previous", "NONE", context))
            else:
                findings.append(Finding("STREAM_STALE", "ERROR", f"{feed} stream {session_id[:8]} open with no heartbeat "
                                        f"for over a day (B14a did not end it)", True, f"{feed}|old",
                                        "END_STALE_SESSION", context))
        elif (in_session(now) and str(feed) == "OPTIONS" and watermark is not None
              and (now - watermark).total_seconds() > OPTIONS_LAG_SECONDS):
            findings.append(Finding(
                "STREAM_LAG", "DEGRADED", f"Options stream {(now - watermark).total_seconds():.0f} s behind",
                True, "OPTIONS", "NONE", {"lag_seconds": round((now - watermark).total_seconds())},
            ))
    return findings


def check_leases(cursor, now: datetime) -> list[Finding]:
    cursor.execute("SELECT count(*), max(updated_at) FROM rt.worker_leases")
    count, latest = cursor.fetchone()
    if count and latest is not None and latest < now - LEASES_STALL:
        return [Finding(
            "LEASES_STALLED", "FATAL", f"no worker lease renewed since {latest.isoformat()}", False, "LEASES",
            "NONE", {"leases": int(count), "last_renewal": latest.isoformat(),
                     "hint": "Nov 10 00:10: Docker Desktop wedge; containers show Up while services lost the DB"},
        )]
    return []


def check_window_runtime(cursor, now: datetime) -> list[Finding]:
    cursor.execute(
        "SELECT window_key, extract(epoch from updated_at - release_at) / "
        "nullif(extract(epoch from deadline - release_at), 0) FROM rt.window_runs "
        "WHERE window_key LIKE 'E:%%' AND state='COMPLETE' AND updated_at >= %s ORDER BY cutoff",
        (now - timedelta(minutes=30),),
    )
    over = [(key, float(share)) for key, share in cursor.fetchall() if share is not None
            and float(share) > RUNTIME_SHARE_ALARM]
    if not over:
        return []
    return [Finding(
        "WINDOW_RUNTIME", "DEGRADED", f"{len(over)} Options windows over {RUNTIME_SHARE_ALARM:.0%} of their budget "
        "in the last 30 minutes", True, "OPTIONS", "NONE",
        {"windows": [f"{key} {share:.0%}" for key, share in over[:10]]},
    )]


CHECKS = (
    check_windows_past_deadline, check_windows_today, check_lanes, check_preparation_parked, check_streams,
    check_leases, check_window_runtime,
)


def mark_missed_cursor(cursor, window_ids, now: datetime) -> list[str]:
    """ACT path of MARK_MISSED (off unless switched on). Guarded so a window a role claimed again is left alone."""
    cursor.execute(
        "UPDATE rt.window_runs SET state='MISSED', updated_at=%s WHERE window_id=ANY(%s::uuid[]) "
        "AND window_key LIKE 'E:%%' AND state IN ('PENDING','RUNNING') AND deadline < %s "
        "AND (lease_until IS NULL OR lease_until < %s) RETURNING window_key",
        (now, list(window_ids), now - PAST_DEADLINE_GRACE, now),
    )
    return [row[0] for row in cursor.fetchall()]


def configured_mode(environment=None) -> tuple[str, frozenset]:
    environment = os.environ if environment is None else environment
    mode = str(environment.get("REALTIME_V2_SELF_HEAL_MODE", "RECORD")).strip().upper()
    actions = frozenset(
        item.strip().upper() for item in str(environment.get("REALTIME_V2_SELF_HEAL_ACTIONS", "")).split(",")
        if item.strip()
    )
    return ("ACT" if mode == "ACT" else "RECORD"), actions & ACTIONS_WITH_ACT_PATH


def run_self_heal_pass(read_cursor, *, now: datetime, mode: str = "RECORD", actions=frozenset(),
                       write_transaction=None, emit=emit_fault) -> list[dict]:
    """Run every check on `read_cursor` (a read-only snapshot), report each finding, and act only where `mode` is ACT,
    the action is switched on and `write_transaction` (a context manager yielding a cursor) is given. A check that
    fails is itself reported and the others still run."""
    records = []
    for check in CHECKS:
        try:
            findings = check(read_cursor, now)
        except Exception as error:  # noqa: BLE001 - one check never stops the others
            records.append(emit(kind="SELF_HEAL_CHECK_FAILED", severity="ERROR", recoverable=True, action="NONE",
                                service=SERVICE, key=check.__name__, message=f"{check.__name__}: {error}"))
            try:
                read_cursor.connection.rollback()
            except Exception:  # noqa: BLE001
                pass
            continue
        for finding in findings:
            severity, action = finding.severity, finding.action
            context = dict(finding.context)
            if action != "NONE":
                if mode == "ACT" and action in actions and write_transaction is not None and finding.targets:
                    try:
                        with write_transaction() as cursor:
                            done = mark_missed_cursor(cursor, finding.targets, now)
                        severity, context["acted_on"] = "ACTED", done
                    except Exception as error:  # noqa: BLE001
                        severity, context["act_error"] = "ERROR", f"{type(error).__name__}: {error}"
                else:
                    severity = "WOULD_ACT"
            records.append(emit(kind=finding.kind, severity=severity, recoverable=finding.recoverable,
                                action=action, service=SERVICE, key=finding.key, message=finding.message,
                                context=context))
    return [record for record in records if record is not None]
