from scripts import admit_historical_option_contracts


class Response:
    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self.payload = payload
        self.headers = headers or {}

    def json(self):
        return self.payload


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


def test_historical_admission_paginates_with_header_authentication():
    session = Session((
        Response(200, {
            "results": [{"ticker": "O:AAPL1"}],
            "next_url": "https://api.polygon.io/v3/reference/options/contracts?cursor=next",
        }),
        Response(200, {"results": [{"ticker": "O:AAPL2"}]}),
    ))

    rows, request_count = admit_historical_option_contracts._get_results(
        session,
        "secret",
        "/v3/reference/options/contracts",
        {"underlying_ticker": "AAPL"},
    )

    assert [row["ticker"] for row in rows] == ["O:AAPL1", "O:AAPL2"]
    assert request_count == 2
    assert session.calls[0][1]["headers"] == {"Authorization": "Bearer secret"}
    assert "apiKey" not in session.calls[0][1]["params"]
    assert session.calls[1][1]["params"] == {}


def test_historical_admission_honors_retry_after(monkeypatch):
    session = Session((
        Response(429, {}, {"Retry-After": "3"}),
        Response(200, {"results": []}),
    ))
    delays = []
    monkeypatch.setattr(admit_historical_option_contracts.time, "sleep", delays.append)

    rows, request_count = admit_historical_option_contracts._get_results(
        session,
        "secret",
        "/v3/reference/options/contracts",
        {},
    )

    assert rows == ()
    assert request_count == 2
    assert delays == [3.0]


def test_historical_admission_rejects_unapproved_pagination_host():
    session = Session((Response(200, {
        "results": [], "next_url": "https://example.com/contracts?cursor=bad",
    }),))

    try:
        admit_historical_option_contracts._get_results(
            session,
            "secret",
            "/v3/reference/options/contracts",
            {},
        )
    except ValueError as error:
        assert "approved HTTPS host" in str(error)
    else:
        raise AssertionError("unapproved pagination host was accepted")


def test_historical_admission_balances_horizon_anchors_and_contract_sides():
    rows = tuple(
        {
            "ticker": f"O:TEST-{contract_type}-{strike}",
            "contract_type": contract_type,
            "strike_price": strike,
        }
        for strike in (90.0, 100.0, 110.0)
        for contract_type in ("call", "put")
    )

    selected = admit_historical_option_contracts._select_anchor_contracts(
        rows,
        (90.0, 110.0),
        4,
    )

    assert {(row["contract_type"], row["strike_price"]) for row in selected} == {
        ("call", 90.0),
        ("put", 90.0),
        ("call", 110.0),
        ("put", 110.0),
    }