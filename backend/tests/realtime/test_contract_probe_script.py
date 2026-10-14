import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import pytest

from options.data.polygon_http import PolygonHttpResponse
from scripts import run_realtime_contract_probe


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 15, 0, tzinfo=UTC)


def _response(payload):
    return PolygonHttpResponse(200, json.dumps(payload).encode("utf-8"))


class FakeCalendar:
    def latest_completed_minute_start(self, _):
        return NOW - timedelta(minutes=1)


class RoutingHttp:
    def __init__(self, *, paginate=False):
        self.calls = []
        self.paginate = paginate

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        path = urlsplit(url).path
        ticker = path.split("/")[4]
        if "/v2/aggs/ticker/" in path:
            payload = {
                "status": "OK",
                "results": [{
                    "t": int((NOW - timedelta(minutes=1)).timestamp() * 1000),
                    "o": 100,
                    "h": 101,
                    "l": 99,
                    "c": 100,
                    "v": 1000,
                }],
            }
        else:
            payload = {
                "status": "DELAYED",
                "results": [
                    {
                        "details": {
                            "ticker": f"O:{ticker}261009C00050000",
                            "contract_type": "call",
                            "expiration_date": "2026-10-09",
                            "strike_price": 50,
                            "exercise_style": "american",
                            "shares_per_contract": 100,
                        },
                        "day": {"volume": 0},
                        "open_interest": 0,
                    },
                    {
                        "details": {
                            "ticker": f"O:{ticker}261009C00100000",
                            "contract_type": "call",
                            "expiration_date": "2026-10-09",
                            "strike_price": 100,
                            "exercise_style": "american",
                            "shares_per_contract": 100,
                        },
                        "day": {"volume": 25},
                        "open_interest": 150,
                    },
                ],
            }
        if self.paginate:
            payload["next_url"] = "https://api.polygon.io/next"
        return _response(payload)


def test_contract_probe_selects_exact_policy_eligible_calls():
    http = RoutingHttp()
    report = run_realtime_contract_probe.ContractQualificationProbe(
        http,
        calendar=FakeCalendar(),
    ).run(NOW)

    assert report["state"] == "COMPLETE"
    assert report["provider_requests"] == 16
    assert report["selection_count"] == 8
    assert all(row["strike"] == "100" for row in report["selections"])
    assert all(row["day_volume"] == 25 for row in report["selections"])
    assert all(row["expiration_date"] == "2026-10-09" for row in report["selections"])
    assert report["database_writes"] == 0


def test_contract_probe_rejects_pagination():
    with pytest.raises(ValueError, match="pagination"):
        run_realtime_contract_probe.ContractQualificationProbe(
            RoutingHttp(paginate=True),
            calendar=FakeCalendar(),
        ).run(NOW)


def test_contract_probe_plan_is_inert(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["run_realtime_contract_probe.py"])

    assert run_realtime_contract_probe.main() == 0

    output = capsys.readouterr().out
    assert '"maximum_provider_requests": 16' in output
    assert '"maximum_requests_per_minute": 5' in output
    assert '"automatic_retries": 0' in output
    assert '"pagination": false' in output
    assert '"database_writes": 0' in output