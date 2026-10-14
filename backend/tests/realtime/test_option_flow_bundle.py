"""v3 review findings 2-4: the Flow bundle is validated once, an unavailable requested underlying is explicit,
and the open-interest change pass is linear."""
from datetime import timedelta
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

from fastapi.testclient import TestClient

import realtime.option_flow_projection as flow
from options.domain import ContractType
from realtime.option_flow_projection import (
    _oi_changes, seal_offline_option_flow_publication, seal_offline_option_flow_publications,
)
from test_forward_ingestion import cycle  # noqa: F401 - pytest fixture
from test_option_flow_realtime import COHORT_ID, flow_matrices, oi_pairs, option_flow_app, read_flow


def _count_chain_validations(monkeypatch):
    calls = []
    original = flow._validate_chain

    def counted(*args, **kwargs):
        calls.append(args[2].underlyer if hasattr(args[2], "underlyer") else args[1].analysis.underlying.underlyer)
        return original(*args, **kwargs)

    monkeypatch.setattr(flow, "_validate_chain", counted)
    return calls


def test_one_validation_serves_every_underlying_and_matches_the_per_symbol_seals(cycle, monkeypatch):
    config, scope, matrices, evidence = flow_matrices(cycle)
    options = dict(selected_at=scope.available_after, chain_evidence=evidence,
                   open_interest_pairs=oi_pairs(scope, matrices))
    calls = _count_chain_validations(monkeypatch)
    publications = seal_offline_option_flow_publications(scope, matrices, config, **options)
    assert set(publications) == set(scope.underlyings) and len(calls) == len(scope.underlyings) == 8
    calls.clear()
    singles = {symbol: seal_offline_option_flow_publication(scope, matrices, config, underlyer=symbol, **options)
               for symbol in scope.underlyings}
    assert len(calls) == 8 * 8  # the old shape: every symbol re-validated every chain
    assert publications == singles
    assert {read_flow(item)["data"]["selected"] for item in publications.values()} == set(scope.underlyings)
    assert len({item.input_sha256 for item in publications.values()}) == 1


def test_a_requested_underlying_without_an_accepted_chain_is_explicitly_unavailable(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    missing = "SPY"
    partial_matrices = tuple(row for row in matrices if row.analysis.underlying.underlyer != missing)
    partial_evidence = tuple(row for row in evidence if row.discovery.result.batch.underlyer != missing)
    publications = seal_offline_option_flow_publications(
        scope, partial_matrices, config, selected_at=scope.available_after, chain_evidence=partial_evidence,
        unavailable_underlyers=((missing, ("MATRIX_UNAVAILABLE",)),))
    unavailable = read_flow(publications[missing])
    assert unavailable["available"] is False and unavailable["reason"] == "MATRIX_UNAVAILABLE"
    assert unavailable["data"]["selected"] == missing and unavailable["data"]["underlyers"] == []
    assert unavailable["data"]["reasons"] == ["MATRIX_UNAVAILABLE"]
    assert unavailable["as_of"] is None and unavailable["model_version"] is None
    for symbol, publication in publications.items():
        if symbol != missing:
            payload = read_flow(publication)
            assert payload["available"] is True and payload["data"]["selected"] == symbol
        assert publication.coverage()["status"] == "PARTIAL"
    outside = seal_offline_option_flow_publication(
        scope, partial_matrices, config, selected_at=scope.available_after, chain_evidence=partial_evidence,
        unavailable_underlyers=((missing, ("MATRIX_UNAVAILABLE",)),), underlyer="ZZZ")
    assert read_flow(outside)["reason"] == "UNDERLYER_NOT_IN_COHORT" and read_flow(outside)["data"]["selected"] == "ZZZ"
    # The original API default (no explicit underlying) still falls back to the first available summary.
    default = seal_offline_option_flow_publication(
        scope, partial_matrices, config, selected_at=scope.available_after, chain_evidence=partial_evidence,
        unavailable_underlyers=((missing, ("MATRIX_UNAVAILABLE",)),))
    assert read_flow(default)["available"] is True and read_flow(default)["data"]["selected"] != missing


def test_the_reader_serves_the_explicit_unavailable_result_instead_of_a_mismatch_error(cycle, monkeypatch):
    import realtime.forward_reader as forward_reader

    config, scope, matrices, evidence = flow_matrices(cycle)
    partial_matrices = tuple(row for row in matrices if row.analysis.underlying.underlyer != "SPY")
    partial_evidence = tuple(row for row in evidence if row.discovery.result.batch.underlyer != "SPY")
    publications = seal_offline_option_flow_publications(
        scope, partial_matrices, config, selected_at=scope.available_after, chain_evidence=partial_evidence,
        unavailable_underlyers=(("SPY", ("MATRIX_UNAVAILABLE",)),))
    monkeypatch.setattr(forward_reader, "load_forward_release_cursor",
                        lambda cursor, **kwargs: (publications, {"scope_sha256": scope.sha256}))
    app, _ = option_flow_app(option_underlying_count=8)
    with TestClient(app) as client:
        params = {"cohort_id": str(scope.cohort_id), "session_date": "2026-10-06"}
        missing = client.get("/api/options/flow", params={**params, "underlyer": "spy"})
        present = client.get("/api/options/flow", params={**params, "underlyer": "aapl"})
    assert missing.status_code == 200, missing.text
    assert missing.json()["available"] is False and missing.json()["data"]["selected"] == "SPY"
    assert missing.json()["reason"] == "MATRIX_UNAVAILABLE"
    assert present.status_code == 200 and present.json()["available"] is True
    assert present.json()["data"]["selected"] == "AAPL"


def test_the_open_interest_pass_is_linear_in_contracts(cycle):
    scope = cycle[1]
    accesses = []

    class Snapshot:
        def __init__(self, contract_id, symbol, side):
            self.contract_id, self._symbol, self.contract_type = contract_id, symbol, side
            self.contract_ticker = f"O:{symbol}-{contract_id}"
            self.open_interest, self.batch_id = 100, uuid5(NAMESPACE_URL, "oi-linear")
            self.first_observed_at = scope.available_after

        @property
        def underlyer(self):
            accesses.append(1)
            return self._symbol

    count = 3000
    rows = [Snapshot(index + 1, ("AAPL", "MSFT", "NVDA")[index % 3], ContractType.CALL if index % 2 else ContractType.PUT)
            for index in range(count)]
    matrices = (SimpleNamespace(snapshots=tuple(rows)),)
    pairs = oi_pairs(scope, matrices)
    accesses.clear()
    changes = _oi_changes(scope, matrices, pairs, scope.available_after + timedelta(seconds=1))
    assert [row["underlying"] for row in changes] == ["AAPL", "MSFT", "NVDA"]
    assert all(row["matched_contract_count"] == row["current_contract_count"] == count // 3 for row in changes)
    assert all(row["call_open_interest_change"] + row["put_open_interest_change"] == 10 * (count // 3)
               for row in changes)
    # Linear: a few reads per contract. The old shape read `underlyer` once per snapshot per pair (N^2 = 9,000,000).
    assert len(accesses) <= 5 * count
