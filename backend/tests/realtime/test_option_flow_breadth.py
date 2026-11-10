"""Nov 10: Options Flow and Activity show the full listed chain from breadth, in the page's existing shapes."""
from realtime.option_flow_breadth import BREADTH_FLOW_SOURCE, breadth_flow_data


def _row(underlying, kind, expiration, strike, volume, open_interest, mark="1.00", spot="100"):
    return {
        "contract_id": 0, "contract_ticker": f"O:{underlying}{expiration}{kind[0]}{strike}", "underlying": underlying,
        "contract_type": kind, "expiration_date": expiration, "strike": strike, "calendar_dte": 1, "spot": spot,
        "mark": mark, "day_volume": volume, "open_interest": open_interest,
        "volume_open_interest_ratio": None if not open_interest else str(round(volume / open_interest, 4)),
        "local_delta": 0.5, "local_iv": 0.3, "market_data_time": "2026-11-09T19:00:00+00:00", "asset_type": "ETF",
    }


def _data():
    streamed = {"underlying": "SPY", "asset_type": "ETF", "contract_count": 8, "batch_id": "b", "open_interest_change": {}}
    other = {"underlying": "QQQ", "asset_type": "ETF", "contract_count": 8}
    return {
        "selected": "SPY", "session_date": "2026-11-09", "underlyers": [streamed, other],
        "selected_summary": streamed, "expirations": [], "strikes": [], "top_contracts": [], "definitions": {"a": "b"},
    }


ROWS = [
    _row("SPY", "CALL", "2026-11-10", "100", 1000, 500, mark="2.00"),
    _row("SPY", "PUT", "2026-11-10", "100", 3000, 1500),
    _row("SPY", "CALL", "2026-11-11", "105", 10, None, mark=None),
    _row("QQQ", "CALL", "2026-11-10", "600", 7, 7),
]
COMPLETE = {"breadth_state": "COMPLETE", "breadth_observed_at": "2026-11-09T19:01:00+00:00", "breadth_contracts": 3}


def test_selected_underlying_uses_the_full_chain():
    data = breadth_flow_data(_data(), ROWS, {"SPY": COMPLETE})

    summary = data["selected_summary"]
    assert data["coverage_source"] == BREADTH_FLOW_SOURCE
    assert summary["contract_count"] == 3 and summary["streamed_contract_count"] == 8
    assert (summary["call_volume"], summary["put_volume"], summary["total_volume"]) == (1010, 3000, 4010)
    assert (summary["call_open_interest"], summary["put_open_interest"]) == (500, 1500)
    assert summary["total_premium_activity"] == "500000.00"  # 1000 x 2.00 x 100 + 3000 x 1.00 x 100
    assert summary["premium_activity_contract_count"] == 2
    assert summary["open_interest_change"] is None
    assert [row["expiration_date"] for row in data["expirations"]] == ["2026-11-10", "2026-11-11"]
    assert data["expirations"][0]["put_volume"] == 3000
    assert [(row["expiration_date"], row["strike"]) for row in data["strikes"]] == [
        ("2026-11-10", "100"), ("2026-11-11", "105"),
    ]
    assert data["top_contracts"][0]["premium_activity"] == "300000.00"
    assert data["definitions"]["a"] == "b" and "coverage_source" in data["definitions"]


def test_underlyings_without_complete_breadth_keep_their_streamed_rows():
    data = breadth_flow_data(_data(), ROWS, {"SPY": COMPLETE, "QQQ": {"breadth_state": "PARTIAL"}})
    qqq = next(row for row in data["underlyers"] if row["underlying"] == "QQQ")
    assert qqq == {"underlying": "QQQ", "asset_type": "ETF", "contract_count": 8}


def test_no_breadth_for_the_selected_underlying_leaves_the_release_unchanged():
    assert breadth_flow_data(_data(), ROWS, {"QQQ": COMPLETE}) == _data()
    assert breadth_flow_data(_data(), [], {}) == _data()


# ---- INV-9 (Nov 10 review R7): optional reads say what they served --------------------------------------------------

class _BrokenBreadthCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if not sql.startswith(("SAVEPOINT", "ROLLBACK TO", "RELEASE")):
            raise RuntimeError("statement timeout")


def test_a_failed_flow_breadth_read_serves_the_release_and_says_so(caplog):
    from datetime import datetime, timezone

    from realtime.portal_compatibility_repository import _flow_with_breadth

    envelope = {"available": True, "data": {"session_date": "2026-11-09", "underlyers": [{"underlying": "SPY"}],
                                            "rows": ["published"]}}
    cursor = _BrokenBreadthCursor()
    with caplog.at_level("WARNING"):
        result = _flow_with_breadth(cursor, envelope, cutoff=datetime(2026, 11, 9, 20, tzinfo=timezone.utc))
    assert result["data"]["rows"] == ["published"]
    assert result["data"]["optional_reads"] == [{
        "read": "flow_breadth", "status": "FALLBACK", "reason_code": "BREADTH_UNAVAILABLE",
        "serving_source": "RELEASE_ROWS",
    }]
    assert cursor.statements[-1] == "ROLLBACK TO SAVEPOINT options_flow_breadth"
    assert any("statement timeout" in (record.exc_text or "") or record.exc_info for record in caplog.records)


def test_an_envelope_without_data_is_left_alone():
    from realtime.portal_compatibility_repository import _optional_read, _with_optional_reads

    status = _optional_read("x", served=True, source="A", fallback_source="B", reason_code="R")
    assert status == {"read": "x", "status": "AVAILABLE", "reason_code": None, "serving_source": "A"}
    assert _with_optional_reads({"available": False}, status) == {"available": False}
    assert _with_optional_reads({"data": {"optional_reads": [status]}}, status)["data"]["optional_reads"] == [status,
                                                                                                           status]
