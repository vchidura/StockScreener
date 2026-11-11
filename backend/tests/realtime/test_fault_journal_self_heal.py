"""G102 R21 (Nov 11): fault records, bounded transient retry and record-only self-heal checks."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from realtime import fault_journal, self_heal, transient_retry

NOW = datetime(2026, 11, 11, 16, 0, tzinfo=timezone.utc)  # 11:00 New York, in session


def test_fault_lines_round_trip_redacted_and_bounded(capsys):
    record = fault_journal.emit_fault(
        kind="exception", severity="fatal", recoverable=False, service="options-window", key="k",
        message="connect postgresql://app:hunter2@db:5432/x failed apiKey=abc123 token: zzz",
        context={"dsn": "postgres://u:p@h/db", "items": list(range(100)), "nested": {"password": "pw=secret"}},
        traceback_text="x" * 10000,
    )
    line = capsys.readouterr().err.strip()
    parsed = fault_journal.parse_fault_line("2026-11-11T16:00:00.1Z " + line)
    assert parsed == record and parsed["severity"] == "FATAL" and parsed["kind"] == "EXCEPTION"
    assert "hunter2" not in line and "abc123" not in line and "zzz" not in line and "u:p@" not in line
    assert len(parsed["context"]["items"]) == 30 and len(parsed["traceback"]) == fault_journal.TRACEBACK_LIMIT
    # Hashes and ids stay readable; numbers in the key fold repeats into one fingerprint.
    assert fault_journal.redact("evidence " + "a" * 64) == "evidence " + "a" * 64
    assert fault_journal.fingerprint("s", "K", "window 18:30") == fault_journal.fingerprint("s", "K", "window 19:45")
    assert fault_journal.parse_fault_line("FAULT {not json") is None
    assert fault_journal.parse_fault_line('FAULT {"schema": "other"}') is None


def test_emit_never_raises():
    assert fault_journal.emit_fault(kind="X", severity="ERROR", recoverable=True, message=object(),
                                    context={"bad": object()}) is not None
    assert fault_journal.emit_fault(not_a_field=1) is None


class _Error(Exception):
    def __init__(self, pgcode=None, status=None):
        super().__init__(pgcode or status)
        self.pgcode = pgcode
        if status is not None:
            self.status = status


def test_transient_classification():
    assert transient_retry.is_transient(_Error("40001")) and transient_retry.is_transient(_Error("08006"))
    assert not transient_retry.is_transient(_Error("23505"))
    assert transient_retry.is_transient(_Error(status=503)) and not transient_retry.is_transient(_Error(status=404))
    assert transient_retry.is_transient(ConnectionResetError()) and not transient_retry.is_transient(ValueError())


def test_retry_transient_recovers_raises_and_reports(capsys):
    calls, waits = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise _Error("40P01")
        return "ok"

    assert transient_retry.retry_transient(flaky, name="op", wait=waits.append) == "ok"
    assert waits == [0.1, 0.2] and '"severity": "RECOVERED"' in capsys.readouterr().err
    with pytest.raises(ValueError):
        transient_retry.retry_transient(lambda: (_ for _ in ()).throw(ValueError("bad")), name="op", wait=waits.append)
    with pytest.raises(_Error):
        transient_retry.retry_transient(lambda: (_ for _ in ()).throw(_Error("40001")), name="op", attempts=2,
                                        wait=lambda _s: None)
    assert '"kind": "RETRY_EXHAUSTED"' in capsys.readouterr().err


class _Cursor:
    """Answers each check's query by a substring of its SQL."""

    def __init__(self, answers):
        self.answers, self.rows, self.executed = answers, [], []
        self.connection = SimpleNamespace(rollback=lambda: None)

    def execute(self, sql, params=None):
        self.executed.append(sql)
        for needle, rows in self.answers.items():
            if needle in sql:
                if isinstance(rows, Exception):
                    raise rows
                self.rows = list(rows)
                return
        self.rows = []

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else (0, None)


def _answers(**overrides):
    deadline = NOW - timedelta(hours=1)
    answers = {
        "WHERE w.state IN ('PENDING','RUNNING')": [
            ("w1", "E:2026-11-10T18:15:00+00:00", "PENDING", deadline, deadline),
            ("w2", "E:2026-11-10T18:20:00+00:00", "RUNNING", deadline, deadline),
            ("w3", "advanced-20261110T1830", "PENDING", deadline, deadline),
        ],
        "WHERE w.window_key LIKE %s OR": [
            ("E:2026-11-11T15:00", NOW - timedelta(minutes=60), "COMPLETE", ""),
            ("E:2026-11-11T15:35", NOW - timedelta(minutes=25), "FAILED",
             "OPTIONS_CYCLE:{ForeignKeyViolation option_packages_cohort_id_provider_tier_fkey}"),
            ("E:2026-11-11T15:40", NOW - timedelta(minutes=20), "FAILED", "OPTIONS_CYCLE:{selected package evidence}"),
            ("E:2026-11-11T15:45", NOW - timedelta(minutes=15), "FAILED", "OPTIONS_CYCLE:{option_packages fkey}"),
            ("E:2026-11-11T15:50", NOW - timedelta(minutes=10), "PENDING", ""),
        ],
        "jsonb_array_elements(d.summary->'lanes')": [
            ("O1", "COMPLETE", 10), ("O2", "UNAVAILABLE", 10), ("O3", "BLOCKED", 10), ("OS1", "BLOCKED", 7),
            ("OS1", "COMPLETE_ZERO", 3),
        ],
        "advanced_options_preparation_work": [("SPY", "CATALOG_QUOTE", "PENDING", NOW + timedelta(hours=5), 3, "")],
        "FROM rt.stream_sessions": [("OPTIONS", "b847f3cc-0000", "LIVE", NOW - timedelta(seconds=5),
                                     NOW - timedelta(seconds=400))],
        "FROM rt.worker_leases": [(20, NOW - timedelta(minutes=10))],
        "extract(epoch from updated_at - release_at)": [("E:2026-11-11T15:00", 0.72), ("E:2026-11-11T14:55", 0.3)],
    }
    answers.update(overrides)
    return answers


def test_record_only_pass_reports_every_check_and_changes_nothing():
    emitted = []
    records = self_heal.run_self_heal_pass(_Cursor(_answers()), now=NOW, emit=lambda **kw: emitted.append(kw) or kw)
    kinds = [row["kind"] for row in records]
    assert kinds.count("WINDOW_PAST_DEADLINE") == 2 and kinds.count("WINDOW_FAILED") == 3
    assert {"FEATURE_BREAKER", "LANE_NOT_EVALUATED", "PREPARATION_PARKED", "STREAM_LAG", "LEASES_STALLED",
            "WINDOW_RUNTIME"} <= set(kinds)
    by_kind = {(row["kind"], row["key"]): row for row in records}
    options = by_kind[("WINDOW_PAST_DEADLINE", "OPTIONS")]
    assert options["severity"] == "WOULD_ACT" and options["action"] == "MARK_MISSED"
    assert by_kind[("WINDOW_PAST_DEADLINE", "EQUITY")]["action"] == "NONE"
    breaker = by_kind[("FEATURE_BREAKER", "O1_PACKAGE_SELECTION")]
    assert breaker["severity"] == "WOULD_ACT" and breaker["action"] == "DISABLE_FEATURE_FOR_SESSION"
    lanes = {row["key"] for row in records if row["kind"] == "LANE_NOT_EVALUATED"}
    assert lanes == {"O3", "OS1"}  # O2 is off by design


def test_breaker_needs_one_feature_and_never_takes_the_equity_core():
    rows = [("E:1", NOW, "FAILED", "breadth capture"), ("E:2", NOW, "FAILED", "option_packages"),
            ("E:3", NOW, "FAILED", "breadth capture"), ("advanced-1", NOW, "FAILED", "EQUITY_PROJECTION")]
    findings = self_heal.check_windows_today(_Cursor({"WHERE w.window_key LIKE %s OR": rows}), NOW)
    assert not [row for row in findings if row.kind == "FEATURE_BREAKER"]
    assert self_heal.attribute_feature("OPTIONS_CYCLE timeout") is None


def test_act_needs_mode_and_the_action_switched_on():
    assert self_heal.configured_mode({}) == ("RECORD", frozenset())
    assert self_heal.configured_mode({"REALTIME_V2_SELF_HEAL_MODE": "act",
                                      "REALTIME_V2_SELF_HEAL_ACTIONS": "mark_missed, requeue_now"}) == \
        ("ACT", frozenset({"MARK_MISSED"}))
    writes = []

    @contextmanager
    def write_transaction():
        cursor = _Cursor({"UPDATE rt.window_runs": [("E:2026-11-10T18:15:00+00:00",)]})
        writes.append(cursor)
        yield cursor

    records = self_heal.run_self_heal_pass(_Cursor(_answers()), now=NOW, mode="ACT", actions=frozenset({"MARK_MISSED"}),
                                           write_transaction=write_transaction, emit=lambda **kw: kw)
    acted = [row for row in records if row["severity"] == "ACTED"]
    assert len(writes) == 1 and len(acted) == 1 and acted[0]["context"]["acted_on"] == ["E:2026-11-10T18:15:00+00:00"]
    assert "deadline < %s" in writes[0].executed[0] and "lease_until < %s" in writes[0].executed[0]
    # Record mode never opens a write transaction.
    writes.clear()
    self_heal.run_self_heal_pass(_Cursor(_answers()), now=NOW, write_transaction=write_transaction, emit=lambda **kw: kw)
    assert writes == []


def test_a_failing_check_is_reported_and_the_rest_still_run():
    records = self_heal.run_self_heal_pass(
        _Cursor(_answers(**{"FROM rt.worker_leases": RuntimeError("boom")})), now=NOW, emit=lambda **kw: kw)
    assert any(row["kind"] == "SELF_HEAL_CHECK_FAILED" and "check_leases" in row["key"] for row in records)
    assert any(row["kind"] == "WINDOW_RUNTIME" for row in records)


def test_before_the_open_the_parked_preparation_is_not_a_fault():
    early = datetime(2026, 11, 11, 13, 0, tzinfo=timezone.utc)  # 08:00 New York
    assert self_heal.check_preparation_parked(_Cursor(_answers()), early) == []


def test_a_previous_sessions_open_stream_waits_for_the_next_registration():
    """B14a: off-hours the previous session's stream stays open until the next session ends it; only a stream older
    than a day (or one silent during the session) is an ERROR."""
    night = datetime(2026, 11, 11, 5, 50, tzinfo=timezone.utc)
    previous = [("OPTIONS", "b847f3cc-0000", "LIVE", datetime(2026, 11, 10, 21, 5, tzinfo=timezone.utc), None)]
    (finding,) = self_heal.check_streams(_Cursor({"FROM rt.stream_sessions": previous}), night)
    assert finding.severity == "DEGRADED" and finding.action == "NONE"
    old = [("OPTIONS", "d42b2d84-0000", "LIVE", datetime(2026, 11, 6, 21, 5, tzinfo=timezone.utc), None)]
    (finding,) = self_heal.check_streams(_Cursor({"FROM rt.stream_sessions": old}), night)
    assert finding.severity == "ERROR" and finding.action == "END_STALE_SESSION"
    silent = [("OPTIONS", "x-0000", "LIVE", NOW - timedelta(minutes=10), NOW - timedelta(minutes=10))]
    (finding,) = self_heal.check_streams(_Cursor({"FROM rt.stream_sessions": silent}), NOW)
    assert finding.severity == "ERROR" and finding.action == "NONE"
