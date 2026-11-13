"""R53 (Nov 12): a FAILED or PARTIAL chain (breadth) capture writes one DEGRADED BREADTH_CAPTURE_FAILED fault per
underlying and window; a COMPLETE capture writes none. Before, only a plain log line recorded it."""
import importlib.util
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import realtime.advanced_options_breadth as breadth
from realtime.fault_journal import FAULT_PREFIX

_spec = importlib.util.spec_from_file_location("options_breadth_fixtures", Path(__file__).with_name("test_options_breadth.py"))
_fx = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fx)


def test_failed_and_partial_captures_are_reported_and_complete_ones_are_not(monkeypatch, capsys):
    provider = _fx._Provider()
    monkeypatch.setattr(breadth, "reserve_coordinated_provider_request_cursor", lambda *_a, **_k: None)
    monkeypatch.setattr(breadth, "finish_provider_request_cursor", lambda *_a, **_k: None)
    monkeypatch.setattr(breadth, "execute_values", lambda *_a, **_k: None)
    transport = _fx._Transport([
        _fx._Response({"error": "bad"}, status=500),                  # AAPL: HTTP error -> FAILED
        _fx._Response({"results": [_fx._contract("O:SPY1C")]}),      # SPY: COMPLETE
    ])
    summary = breadth.capture_options_breadth(
        provider, session_date=_fx.SESSION, underlyings=("AAPL", "NVDA", "SPY"),
        spots={"AAPL": Decimal("342.45"), "SPY": Decimal("735.44")}, equity_cutoff=_fx.CUTOFF, cohort_id=UUID(int=5),
        authorization_reference="AUTH", authorized_until=_fx.NOW + timedelta(hours=6), transport=transport,
        deadline=_fx.NOW + timedelta(minutes=2), clock=lambda: _fx.NOW,
    )
    assert summary["SPY"]["state"] == "COMPLETE"
    faults = [json.loads(line[len(FAULT_PREFIX):]) for line in capsys.readouterr().err.splitlines()
              if line.startswith(FAULT_PREFIX)]
    by_name = {fault["context"]["underlying"]: fault for fault in faults}
    assert sorted(by_name) == ["AAPL", "NVDA"]
    for name in ("AAPL", "NVDA"):
        fault = by_name[name]
        assert fault["kind"] == "BREADTH_CAPTURE_FAILED"
        assert fault["severity"] == "DEGRADED" and fault["action"] == "LAST_GOOD_CHAIN"
        assert fault["service"] == "options-scheduler" and fault["context"]["state"] == "FAILED"
    assert by_name["NVDA"]["context"]["reason"] == "SPOT_UNAVAILABLE"


def test_self_heal_reports_names_without_a_complete_capture_for_three_windows():
    from datetime import datetime, timezone
    from unittest.mock import MagicMock

    from realtime.self_heal import CHECKS, check_breadth_stale

    assert check_breadth_stale in CHECKS
    utc = timezone.utc
    now = datetime(2026, 11, 12, 19, 20, tzinfo=utc)  # 14:20 ET, a session day
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("AAPL", datetime(2026, 11, 12, 19, 0, tzinfo=utc), datetime(2026, 11, 12, 19, 18, tzinfo=utc)),  # stale
        ("SPY", datetime(2026, 11, 12, 19, 17, tzinfo=utc), datetime(2026, 11, 12, 19, 17, tzinfo=utc)),  # fresh
        ("QQQ", None, datetime(2026, 11, 12, 19, 16, tzinfo=utc)),                                          # never
        ("NFLX", datetime(2026, 11, 12, 17, 0, tzinfo=utc), datetime(2026, 11, 12, 17, 0, tzinfo=utc)),   # not running
    ]
    findings = check_breadth_stale(cursor, now)
    assert len(findings) == 1 and findings[0].kind == "BREADTH_STALE" and findings[0].action == "NONE"
    assert sorted(findings[0].context["names"]) == ["AAPL", "QQQ"]
    cursor.reset_mock()
    assert check_breadth_stale(cursor, datetime(2026, 11, 12, 22, 0, tzinfo=utc)) == []  # after the close
    cursor.execute.assert_not_called()
