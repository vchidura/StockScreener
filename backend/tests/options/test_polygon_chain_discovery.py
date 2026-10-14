import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from options.data.polygon_chain_discovery import (
    PolygonOptionChainDiscoveryAdapter,
    PolygonOptionChainDiscoveryLimits,
    build_polygon_option_chain_discovery_plan,
)
from options.data.polygon_http import PolygonHttpResponse
from options.errors import OptionProviderError, ProviderErrorCategory


UTC = timezone.utc
AS_OF = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
OBSERVED_AT = AS_OF + timedelta(minutes=15)
BATCH_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
POLICY_SHA256 = "b" * 64


def _response(payload):
    return PolygonHttpResponse(200, json.dumps(payload).encode("utf-8"))


class QueueHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        return self.responses.pop(0)


def _plan():
    return build_polygon_option_chain_discovery_plan(
        "SPY",
        AS_OF,
        Decimal("100"),
        maximum_dte=60,
        acquisition_maximum_dte=60,
        strike_corridor_fraction=Decimal("0.15"),
    )


def _adapter(http, *, limits=None, pages=None, completions=None):
    pages = pages if pages is not None else []
    completions = completions if completions is not None else []
    return PolygonOptionChainDiscoveryAdapter(
        http.get,
        base_url="https://api.polygon.io",
        limits=limits or PolygonOptionChainDiscoveryLimits(4, 10, 10_000, 20_000),
        clock=lambda: OBSERVED_AT,
        page_sink=pages.append,
        completion_sink=lambda: completions.append("complete"),
    )


def test_discovery_preserves_exact_60_dte_corridor_lineage_and_exclusions():
    next_url = (
        "https://api.polygon.io/v3/snapshot/options/SPY?cursor=page-2"
        "&apiKey=must-not-persist"
    )
    first = _response({"request_id": "r1", "results": [{"ticker": "O:1"}], "next_url": next_url})
    second = _response({"request_id": "r2", "results": [{"ticker": "O:2"}]})
    http = QueueHttp([first, second])
    pages = []
    completions = []
    plan = _plan()

    result = _adapter(http, pages=pages, completions=completions).discover(
        plan,
        batch_id=BATCH_ID,
        policy_sha256=POLICY_SHA256,
        started_at=AS_OF,
    )

    assert plan.expiration_from == date(2026, 10, 5)
    assert plan.expiration_through == date(2026, 12, 4)
    assert plan.strike_min == Decimal("85.00")
    assert plan.strike_max == Decimal("115.00")
    assert plan.request_filters["expiration_date.lte"] == "2026-12-04"
    assert plan.request_filters["strike_price.gte"] == "85.00"
    assert result.batch.complete is True
    assert result.batch.started_at == AS_OF
    assert result.batch.completed_at == OBSERVED_AT
    assert result.rows == ({"ticker": "O:1"}, {"ticker": "O:2"})
    assert result.batch.pages == tuple(pages)
    assert all(page.received_at == OBSERVED_AT for page in result.batch.pages)
    assert result.batch.pages[0].payload_sha256 == hashlib.sha256(first.body).hexdigest()
    assert result.batch.pages[0].next_cursor_sha256 == hashlib.sha256(b"page-2").hexdigest()
    assert result.batch.pages[1].request_cursor_sha256 == hashlib.sha256(b"page-2").hexdigest()
    assert http.calls[1][1] == {"cursor": "page-2"}
    assert "must-not-persist" not in repr(
        [page.request_metadata for page in result.batch.pages]
    )
    assert completions == ["complete"]
    assert {(item.field, item.comparison, item.boundary) for item in result.included_ranges} == {
        ("expiration_date", "gte", "2026-10-05"),
        ("expiration_date", "lte", "2026-12-04"),
        ("strike_price", "gte", "85.00"),
        ("strike_price", "lte", "115.00"),
    }
    assert {(item.field, item.comparison, item.boundary) for item in result.excluded_ranges} == {
        ("expiration_date", "lt", "2026-10-05"),
        ("expiration_date", "gt", "2026-12-04"),
        ("strike_price", "lt", "85.00"),
        ("strike_price", "gt", "115.00"),
    }


def test_repeated_pagination_rejects_partial_batch_without_completion():
    next_url = "https://api.polygon.io/v3/snapshot/options/SPY?cursor=repeated"
    http = QueueHttp([
        _response({"results": [{"ticker": "O:1"}], "next_url": next_url}),
        _response({"results": [{"ticker": "O:2"}], "next_url": next_url}),
    ])
    pages = []
    completions = []

    with pytest.raises(OptionProviderError) as error:
        _adapter(http, pages=pages, completions=completions).discover(
            _plan(), batch_id=BATCH_ID, policy_sha256=POLICY_SHA256, started_at=AS_OF
        )

    assert error.value.category is ProviderErrorCategory.PAGINATION
    assert len(pages) == 2
    assert completions == []


def test_hostile_next_url_is_retained_invalid_and_never_completed():
    http = QueueHttp([_response({
        "results": [{"ticker": "O:1"}],
        "next_url": "https://example.com/v3/snapshot/options/SPY?cursor=stolen",
    })])
    pages = []
    completions = []

    with pytest.raises(OptionProviderError) as error:
        _adapter(http, pages=pages, completions=completions).discover(
            _plan(), batch_id=BATCH_ID, policy_sha256=POLICY_SHA256, started_at=AS_OF
        )

    assert error.value.category is ProviderErrorCategory.PAGINATION
    assert len(pages) == 1
    assert pages[0].validation_status.value == "INVALID"
    assert completions == []


@pytest.mark.parametrize(
    ("responses", "limits", "message"),
    [
        (
            [
                _response({
                    "results": [{"ticker": "O:1"}],
                    "next_url": "https://api.polygon.io/v3/snapshot/options/SPY?cursor=two",
                }),
                _response({"results": [{"ticker": "O:2"}]}),
            ],
            PolygonOptionChainDiscoveryLimits(1, 10, 10_000, 20_000),
            "page cap",
        ),
        (
            [_response({"results": [{"ticker": "O:1"}, {"ticker": "O:2"}]})],
            PolygonOptionChainDiscoveryLimits(2, 1, 10_000, 20_000),
            "contract cap",
        ),
        (
            [_response({"results": [{"ticker": "O:1"}]})],
            PolygonOptionChainDiscoveryLimits(2, 10, 10, 20),
            "per-page byte cap",
        ),
    ],
)
def test_discovery_caps_reject_partial_batches(responses, limits, message):
    completions = []
    http = QueueHttp(responses)

    with pytest.raises(OptionProviderError, match=message) as error:
        _adapter(http, limits=limits, completions=completions).discover(
            _plan(), batch_id=BATCH_ID, policy_sha256=POLICY_SHA256, started_at=AS_OF
        )

    assert error.value.category is ProviderErrorCategory.RESPONSE_LIMIT
    assert completions == []


def test_total_byte_cap_rejects_a_terminal_second_page():
    first = _response({
        "results": [{"ticker": "O:1"}],
        "next_url": "https://api.polygon.io/v3/snapshot/options/SPY?cursor=two",
    })
    second = _response({"results": [{"ticker": "O:2"}]})
    completions = []
    limit = len(first.body) + len(second.body) - 1

    with pytest.raises(OptionProviderError, match="batch byte cap") as error:
        _adapter(
            QueueHttp([first, second]),
            limits=PolygonOptionChainDiscoveryLimits(2, 10, limit, limit),
            completions=completions,
        ).discover(
            _plan(), batch_id=BATCH_ID, policy_sha256=POLICY_SHA256, started_at=AS_OF
        )

    assert error.value.category is ProviderErrorCategory.RESPONSE_LIMIT
    assert completions == []