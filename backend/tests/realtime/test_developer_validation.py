import json
from datetime import date, datetime, timezone

from options.data.polygon_http import PolygonHttpResponse
from realtime.developer_validation import (
    DependencyDisposition,
    DeveloperCanaryPolicy,
    DeveloperCanaryValidator,
    assess_sanitized_developer_report,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 18, 0, tzinfo=UTC)


class FakeTransport:
    def __init__(
        self,
        *,
        quote_status=403,
        snapshot_marks=True,
        missing_call_marks=False,
        missing_aggregate=False,
    ):
        self.requests = []
        self.quote_status = quote_status
        self.snapshot_marks = snapshot_marks
        self.missing_call_marks = missing_call_marks
        self.missing_aggregate = missing_aggregate

    @staticmethod
    def _response(payload, status=200):
        return PolygonHttpResponse(
            status,
            json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )

    def get(self, url, params):
        self.requests.append((url, dict(params)))
        if "/v3/quotes/" in url:
            if self.quote_status == 403:
                return PolygonHttpResponse(403, b"{}")
            return self._response({"status": "OK", "results": []})
        if "/v2/aggs/ticker/O:" in url:
            return self._response(
                {
                    "status": "DELAYED",
                    "results": None if self.missing_aggregate else [{"c": 1.25}],
                }
            )
        if "/v2/aggs/" in url:
            count = 120 if "/1/day/" in url else 25 if "/30/minute/" in url else 2
            return self._response(
                {
                    "status": "DELAYED",
                    "results": [
                        {"c": 100 + index / 100, "t": 1791210600000 + index}
                        for index in range(count)
                    ],
                }
            )
        if "/v3/reference/options/contracts" in url:
            return self._response(
                {
                    "status": "OK",
                    "results": [{"expiration_date": "2026-10-23"}],
                }
            )
        if "/v3/snapshot/options/" in url:
            kind = str(params["contract_type"])
            suffix = "C" if kind == "call" else "P"
            return self._response(
                {
                    "status": "DELAYED",
                    "results": [
                        {
                            "details": {
                                "ticker": f"O:TEST261023{suffix}{index + 95:08d}",
                                "strike_price": index + 95,
                                "shares_per_contract": 100,
                            },
                            "open_interest": 100 + index,
                            **(
                                {"last_trade": {"price": 1 + index / 10}}
                                if self.snapshot_marks
                                and not (self.missing_call_marks and kind == "call")
                                else {}
                            ),
                        }
                        for index in range(8)
                    ],
                }
            )
        if "/v3/trades/" in url:
            return self._response(
                {"status": "DELAYED", "results": [{"id": "trade-1"}]}
            )
        raise AssertionError(f"unexpected request: {url}")


def test_developer_canary_validates_portable_dependencies_and_expected_quote_denial():
    transport = FakeTransport()
    policy = DeveloperCanaryPolicy(tickers=("SPY",), maximum_requests=11)
    report = DeveloperCanaryValidator(
        transport,
        configuration_sha256="a" * 64,
        policy=policy,
        monotonic=lambda: 1.0,
    ).validate(checked_at=NOW)

    assert report.status == "PORTABLE_READY"
    assert report.request_count == 11
    assert report.database_writes is False
    assert report.raw_payloads_retained is False
    result = report.ticker_results[0]
    assert result.call_snapshot_count == result.put_snapshot_count == 8
    assert result.delayed_trade_contract_count == 4
    assert result.delayed_trade_event_count == 4
    assert result.quote_access_denied is True
    assert result.reason_codes == ("O2_DEVELOPER_POLICY_DISABLED",)
    assert result.o2_group_dispositions == (("CALL", "READY"), ("PUT", "READY"))
    assert dict(result.dependency_dispositions)["O2"] == "UNAVAILABLE"
    assert all(
        disposition == DependencyDisposition.READY.value
        for detector, disposition in result.dependency_dispositions
        if detector != "O2"
    )
    assert report.developer_disabled == ("O2",)
    assert set(report.advanced_unavailable) == {
        "AGGRESSIVE_PREMIUM_BURST",
        "PERSISTENT_REALTIME_FLOW",
        "SWEEP_LIKE_CLUSTER",
    }


def test_developer_canary_blocks_when_quote_access_is_unexpected():
    report = DeveloperCanaryValidator(
        FakeTransport(quote_status=200),
        configuration_sha256="a" * 64,
        policy=DeveloperCanaryPolicy(tickers=("SPY",), maximum_requests=11),
        monotonic=lambda: 1.0,
    ).validate(checked_at=NOW)

    assert report.status == "PORTABLE_BLOCKED"
    assert report.ticker_results[0].reason_codes == (
        "O2_DEVELOPER_POLICY_DISABLED",
        "DEVELOPER_QUOTE_ACCESS_UNEXPECTED",
    )


def test_developer_canary_policy_is_exactly_bounded_for_reviewed_universe():
    policy = DeveloperCanaryPolicy()

    assert len(policy.tickers) == 8
    assert policy.maximum_requests == 88
    assert policy.o2_enabled is False
    assert policy.snapshot_contracts_per_type == 8
    assert policy.trade_contracts_per_type == 2
    assert policy.minimum_smile_contracts_per_type == 7


def test_developer_canary_uses_bounded_daily_aggregate_fallback_for_o2_marks():
    report = DeveloperCanaryValidator(
        FakeTransport(snapshot_marks=False),
        configuration_sha256="a" * 64,
        policy=DeveloperCanaryPolicy(
            tickers=("SPY",),
            o2_enabled=True,
            maximum_requests=25,
        ),
        monotonic=lambda: 1.0,
    ).validate(checked_at=NOW)

    result = report.ticker_results[0]
    assert report.status == "PORTABLE_READY"
    assert report.request_count == 25
    assert result.call_mark_count == result.put_mark_count == 7
    assert result.aggregate_fallback_request_count == 14
    assert dict(result.dependency_dispositions)["O2"] == "READY"


def test_developer_canary_treats_no_trade_aggregate_as_unavailable_not_schema_failure():
    report = DeveloperCanaryValidator(
        FakeTransport(snapshot_marks=False, missing_aggregate=True),
        configuration_sha256="a" * 64,
        policy=DeveloperCanaryPolicy(
            tickers=("SPY",),
            o2_enabled=True,
            maximum_requests=25,
        ),
        monotonic=lambda: 1.0,
    ).validate(checked_at=NOW)

    result = report.ticker_results[0]
    assert report.status == "PORTABLE_BLOCKED"
    assert result.aggregate_fallback_request_count == 14
    assert result.call_mark_count == result.put_mark_count == 0
    assert result.reason_codes == ("O2_SMILE_COVERAGE_UNAVAILABLE",)


def test_developer_canary_scopes_partial_o2_coverage_without_blocking_platform():
    report = DeveloperCanaryValidator(
        FakeTransport(missing_call_marks=True, missing_aggregate=True),
        configuration_sha256="a" * 64,
        policy=DeveloperCanaryPolicy(
            tickers=("SPY",),
            o2_enabled=True,
            maximum_requests=25,
        ),
        monotonic=lambda: 1.0,
    ).validate(checked_at=NOW)

    result = report.ticker_results[0]
    assert report.status == "PORTABLE_DEGRADED"
    assert result.call_mark_count == 0
    assert result.put_mark_count == 8
    assert result.o2_group_dispositions == (
        ("CALL", "UNAVAILABLE"),
        ("PUT", "READY"),
    )
    assert dict(result.dependency_dispositions)["O2"] == "DEGRADED"
    assert result.reason_codes == ("O2_PARTIAL_SMILE_COVERAGE",)


def test_o2_disabled_policy_assesses_retained_evidence_without_provider_replay():
    report = DeveloperCanaryValidator(
        FakeTransport(missing_call_marks=True, missing_aggregate=True),
        configuration_sha256="a" * 64,
        policy=DeveloperCanaryPolicy(tickers=("SPY",), maximum_requests=11),
        monotonic=lambda: 1.0,
    ).validate(checked_at=NOW)
    payload = {
        "database_writes": report.database_writes,
        "raw_payloads_retained": report.raw_payloads_retained,
        "advanced_unavailable": list(report.advanced_unavailable),
        "ticker_results": [
            {
                "ticker": row.ticker,
                "quote_access_denied": row.quote_access_denied,
                "dependency_dispositions": [list(item) for item in row.dependency_dispositions],
                "o2_group_dispositions": [list(item) for item in row.o2_group_dispositions],
            }
            for row in report.ticker_results
        ],
    }

    assessment = assess_sanitized_developer_report(
        payload,
        source_report_sha256="f" * 64,
        policy=DeveloperCanaryPolicy(tickers=("SPY",), maximum_requests=11),
    )

    assert assessment.status == "PORTABLE_READY"
    assert assessment.developer_disabled == ("O2",)
    assert assessment.blocking_reasons == ()
    assert assessment.o2_unavailable_groups == (("SPY", "CALL"),)