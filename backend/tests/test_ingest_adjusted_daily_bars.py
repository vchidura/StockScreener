import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.polygon import normalize_security_reference, sha256_json
from scripts import ingest_adjusted_daily_bars as importer


OBSERVED_AT = datetime(2026, 9, 12, tzinfo=timezone.utc)


def write_references(cache_dir, session, payload, security_type="CS"):
    path = cache_dir / f"tickers-{security_type}_{session}.json"
    path.write_text(json.dumps({"payload": payload, "sha256": sha256_json(payload)}), encoding="utf-8")
    return path


def reference(ticker="TEST", **changes):
    return {"ticker": ticker, "type": "CS", "active": True,
            "cik": "0000123", "composite_figi": "OLD", "share_class_figi": "OLD_CLASS"} | changes


def resolve(cache_dir, session, tickers=("TEST",)):
    return importer.dated_security_ids(
        cache_dir, date.fromisoformat(session), tickers, observed_at=OBSERVED_AT, security_types=("CS",),
    ).security_ids


def test_ticker_reuse_resolves_each_exact_date(tmp_path):
    old = reference()
    new = reference(cik="0000999", composite_figi="NEW", share_class_figi="NEW_CLASS")
    write_references(tmp_path, "2024-03-01", [old])
    write_references(tmp_path, "2024-03-04", [new])

    first = resolve(tmp_path, "2024-03-01")
    second = resolve(tmp_path, "2024-03-04")

    assert first["TEST"] == normalize_security_reference(old, observed_at=OBSERVED_AT).security_id
    assert second["TEST"] == normalize_security_reference(new, observed_at=OBSERVED_AT).security_id
    assert first != second


@pytest.mark.parametrize("neighbor", ["2024-03-01", "2024-03-05"])
def test_no_forward_or_backward_fill_for_missing_reference_date(tmp_path, neighbor):
    write_references(tmp_path, neighbor, [reference()])
    with pytest.raises(ValueError, match="DATED_REFERENCE_CACHE_MISSING"):
        resolve(tmp_path, "2024-03-04")
    assert not (tmp_path / "tickers-CS_2024-03-04.json").exists()


def test_missing_ticker_is_not_filled_from_later_reference(tmp_path):
    write_references(tmp_path, "2024-03-04", [])
    write_references(tmp_path, "2024-03-05", [reference()])
    assert resolve(tmp_path, "2024-03-04") == {}


@pytest.mark.parametrize("changes", [
    {"composite_figi": "OTHER"}, {"cik": "0000999"}, {"share_class_figi": "OTHER_CLASS"},
])
def test_conflicting_same_date_ticker_evidence_is_rejected(tmp_path, changes):
    write_references(tmp_path, "2024-03-04", [reference(), reference(**changes)])
    with pytest.raises(ValueError, match="AMBIGUOUS_DATED_IDENTITY"):
        resolve(tmp_path, "2024-03-04")


def test_two_tickers_cannot_share_an_import_identity(tmp_path):
    write_references(tmp_path, "2024-03-04", [reference("DOC"), reference("PEAK")])
    with pytest.raises(ValueError, match="DATED_SECURITY_ID_COLLISION"):
        resolve(tmp_path, "2024-03-04", ("DOC", "PEAK"))


def test_corrupted_cache_cannot_supply_identity(tmp_path):
    path = write_references(tmp_path, "2024-03-04", [reference()])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["payload"][0]["composite_figi"] = "TAMPERED"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum mismatch"):
        resolve(tmp_path, "2024-03-04")


def test_cache_type_must_match_its_requested_contract(tmp_path):
    write_references(tmp_path, "2024-03-04", [reference(type="ETF")])
    with pytest.raises(ValueError, match="DATED_REFERENCE_TYPE_MISMATCH"):
        resolve(tmp_path, "2024-03-04")


@pytest.mark.parametrize("cik", [None, "INVALID", "0000"])
def test_ticker_only_reference_is_not_identity_evidence(tmp_path, cik):
    write_references(tmp_path, "2024-03-04", [reference(composite_figi=None, share_class_figi=None, cik=cik)])
    with pytest.raises(ValueError, match="DATED_IDENTITY_EVIDENCE_MISSING"):
        resolve(tmp_path, "2024-03-04")


def prepare_import(monkeypatch, tmp_path, *, apply=False, reconstructed=True):
    arguments = SimpleNamespace(
        start="2024-03-01", end="2024-03-04", calendar="XNYS", limit_sessions=None,
        from_reconstructed_universes=reconstructed, policy_version="test",
        include_non_common=False, include_non_common_ticker=[], apply=apply,
        ticker=[], skip_benchmarks=False,
        security_type=None, fetch_missing_reference_cache=False,
        output=tmp_path / "report.json", reference_cache_dir=tmp_path,
    )
    monkeypatch.setattr(importer, "parser", lambda: SimpleNamespace(parse_args=lambda: arguments))
    monkeypatch.setattr(importer, "reconstructed_union", lambda policy: ("TEST",))
    universe = MagicMock()
    universe.get_latest_as_of.return_value = {"universe_run_id": "live"}
    universe.member_tickers.return_value = ("TEST",)
    monkeypatch.setattr(importer, "EquityUniverseRepository", lambda: universe)
    monkeypatch.setattr(importer, "BENCHMARK_TICKERS", frozenset({"SPY"}))
    for session in ("2024-03-01", "2024-03-04"):
        write_references(tmp_path, session, [reference(composite_figi=session)])
        write_references(tmp_path, session, [reference("SPY", type="ETF", composite_figi="SPY_ID")], "ETF")
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    manager = MagicMock()
    manager.__enter__.return_value = cursor
    monkeypatch.setattr(importer, "get_db_cursor", lambda: manager)
    client_factory = MagicMock()
    client_factory.return_value.fetch_grouped_daily.return_value = [
        {"T": ticker, "o": 10, "h": 12, "l": 9, "c": 11, "v": 100} for ticker in ("TEST", "SPY")
    ]
    monkeypatch.setattr(importer, "PolygonEquityClient", client_factory)
    repository_factory = MagicMock()
    repository_factory.return_value.persist.side_effect = lambda bars: len(bars)
    monkeypatch.setattr(importer, "EquityBarRepository", repository_factory)
    return arguments, cursor, client_factory, repository_factory


@pytest.mark.parametrize("reconstructed", [True, False])
def test_apply_uses_each_dates_map_in_both_member_modes(monkeypatch, tmp_path, reconstructed):
    arguments, cursor, client, repository = prepare_import(monkeypatch, tmp_path, apply=True, reconstructed=reconstructed)

    assert importer.main() == 0

    batches = [call.args[0] for call in repository.return_value.persist.call_args_list]
    assert len(batches) == 2
    assert batches[0][0].security_id != batches[1][0].security_id
    assert batches[0][1].ticker == "SPY"
    assert batches[0][1].security_id == batches[1][1].security_id
    assert client.return_value.fetch_grouped_daily.call_count == 2
    assert cursor.execute.call_args_list[0].args[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert json.loads(arguments.output.read_text())["bars_inserted"] == 4


@pytest.mark.parametrize("apply", [True, False])
def test_missing_later_date_blocks_entire_import_before_fetch_or_write(monkeypatch, tmp_path, apply):
    arguments, _, client, repository = prepare_import(monkeypatch, tmp_path, apply=apply)
    (tmp_path / "tickers-CS_2024-03-04.json").unlink()

    assert importer.main() == 2

    client.assert_not_called()
    repository.assert_not_called()
    report = json.loads(arguments.output.read_text())
    assert report["identity_preflight"]["status"] == "BLOCKED"
    assert report["identity_preflight"]["existing_lineage_checked"] is False
    assert report["research_readiness"] == "NOT_CERTIFIED"


def test_existing_wrong_identity_requires_versioned_repair_not_reimport(monkeypatch, tmp_path):
    arguments, cursor, client, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    cursor.fetchall.return_value = [{"ticker": "TEST", "revisions": 1}]

    assert importer.main() == 2

    report = json.loads(arguments.output.read_text())
    assert report["identity_preflight"]["issue_counts"] == {"VERSIONED_IDENTITY_REPAIR_REQUIRED": 2}
    assert report["identity_preflight"]["existing_lineage_checked"] is True
    assert "bar.security_id <> expected.security_id" in cursor.execute.call_args.args[0]
    client.assert_not_called()
    repository.assert_not_called()


def test_partial_reference_coverage_still_reports_other_dates_conflicts(monkeypatch, tmp_path):
    arguments, cursor, client, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    (tmp_path / "tickers-CS_2024-03-04.json").unlink()
    cursor.fetchall.return_value = [{"ticker": "TEST", "revisions": 1}]
    assert importer.main() == 2
    preflight = json.loads(arguments.output.read_text())["identity_preflight"]
    assert preflight["issue_counts"] == {"DATED_REFERENCE_UNRESOLVED": 1, "VERSIONED_IDENTITY_REPAIR_REQUIRED": 1}
    assert preflight["existing_lineage_checked"] is False
    assert preflight["existing_lineage_checked_sessions"] == 1
    client.assert_not_called()
    repository.assert_not_called()


def test_ready_dry_run_writes_report_only(monkeypatch, tmp_path):
    arguments, _, client, repository = prepare_import(monkeypatch, tmp_path)
    assert importer.main() == 0
    report = json.loads(arguments.output.read_text())
    assert report["identity_preflight"]["status"] == "READY_FOR_PRICE_VALIDATION"
    client.assert_not_called()
    repository.assert_not_called()


def test_price_without_exact_date_identity_is_not_silently_dropped(monkeypatch, tmp_path):
    _, _, _, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    write_references(tmp_path, "2024-03-01", [])
    with pytest.raises(ValueError, match="PRICE_IDENTITY_UNRESOLVED"):
        importer.main()
    repository.return_value.persist.assert_not_called()


def test_malformed_selected_price_cannot_be_silently_dropped(monkeypatch, tmp_path):
    _, _, client, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    client.return_value.fetch_grouped_daily.return_value[0].pop("o")
    with pytest.raises(ValueError, match="PRICE_NORMALIZATION_INCOMPLETE"):
        importer.main()
    repository.return_value.persist.assert_not_called()


def test_benchmark_requires_its_own_dated_reference(monkeypatch, tmp_path):
    arguments, _, client, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    write_references(tmp_path, "2024-03-01", [], "ETF")
    assert importer.main() == 2
    assert json.loads(arguments.output.read_text())["identity_preflight"]["issue_counts"] == {"DATED_BENCHMARK_REFERENCE_MISSING": 1}
    client.assert_not_called()
    repository.assert_not_called()


def test_dated_type_exclusions_are_distinct_from_unresolved_tickers(tmp_path):
    write_references(tmp_path, "2024-03-04", [])
    write_references(tmp_path, "2024-03-04", [reference("FUND", type="ETF")], "ETF")
    identity = importer.dated_security_ids(tmp_path, date(2024, 3, 4), ("FUND",), observed_at=OBSERVED_AT)
    assert identity.excluded_tickers == frozenset({"FUND"})
    assert importer.validate_price_identities([{"T": "FUND"}], ("FUND",), identity, date(2024, 3, 4)) == set()


def test_one_explicit_selected_etf_can_be_included_without_admitting_all_etfs(tmp_path):
    write_references(tmp_path, "2024-03-04", [], "CS")
    write_references(tmp_path, "2024-03-04", [
        reference("IWM", type="ETF", composite_figi="IWM_ID"),
        reference("OTHER", type="ETF", composite_figi="OTHER_ID"),
    ], "ETF")
    identity = importer.dated_security_ids(
        tmp_path, date(2024, 3, 4), ("IWM", "OTHER"), observed_at=OBSERVED_AT,
        allowed_non_common_tickers=("IWM",),
    )
    assert set(identity.security_ids) == {"IWM"}
    assert identity.excluded_tickers == frozenset({"OTHER"})

    with pytest.raises(ValueError, match="selected universe"):
        importer.dated_security_ids(
            tmp_path, date(2024, 3, 4), ("IWM",), observed_at=OBSERVED_AT,
            allowed_non_common_tickers=("OTHER",),
        )


def test_targeted_iwm_dry_run_keeps_provider_and_repository_closed(monkeypatch, tmp_path):
    arguments, _, client, repository = prepare_import(monkeypatch, tmp_path)
    arguments.ticker = ["iwm"]
    arguments.skip_benchmarks = True
    arguments.include_non_common_ticker = ["iwm"]
    arguments.security_type = ["ETF"]
    monkeypatch.setattr(importer, "reconstructed_union", lambda policy: ("TEST", "IWM"))
    cache_type = "ETF-subset-" + sha256_json(("IWM",))[:16]
    for session in ("2024-03-01", "2024-03-04"):
        write_references(
            tmp_path, session,
            [reference("IWM", type="ETF", composite_figi="IWM_ID")], cache_type,
        )

    assert importer.main() == 0

    report = json.loads(arguments.output.read_text())
    assert report["explicit_tickers"] == ["IWM"]
    assert report["explicit_non_common_tickers"] == ["IWM"]
    assert report["benchmarks_included"] is False
    assert report["securities_requested"] == 1
    assert report["identity_preflight"]["status"] == "READY_FOR_PRICE_VALIDATION"
    client.assert_not_called()
    repository.assert_not_called()


def test_skip_benchmarks_or_out_of_universe_ticker_fails_before_provider(monkeypatch, tmp_path):
    arguments, _, client, repository = prepare_import(monkeypatch, tmp_path)
    arguments.skip_benchmarks = True
    with pytest.raises(SystemExit, match="requires"):
        importer.main()
    arguments.ticker = ["OTHER"]
    with pytest.raises(SystemExit, match="selected universe"):
        importer.main()
    client.assert_not_called()
    repository.assert_not_called()


def test_ticker_scoped_reference_cache_never_uses_full_type_cache_key(monkeypatch, tmp_path):
    fetch = MagicMock(return_value=[reference("IWM", type="ETF", composite_figi="IWM_ID")])
    identity = importer.dated_security_ids(
        tmp_path, date(2024, 3, 4), ("IWM",), observed_at=OBSERVED_AT,
        security_types=("ETF",), allowed_non_common_tickers=("IWM",),
        reference_fetcher=fetch, cache_scope_tickers=("IWM",),
    )
    assert set(identity.security_ids) == {"IWM"}
    fetch.assert_called_once_with("ETF", date(2024, 3, 4), ("IWM",))
    paths = list(tmp_path.glob("*.json"))
    assert len(paths) == 1 and "subset" in paths[0].name
    assert not (tmp_path / "tickers-ETF_2024-03-04.json").exists()


def test_reference_fetch_and_price_apply_cannot_be_combined(monkeypatch, tmp_path):
    arguments, _, client, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    arguments.fetch_missing_reference_cache = True
    with pytest.raises(SystemExit, match="separate stages"):
        importer.main()
    client.assert_not_called()
    repository.assert_not_called()


def test_unfinished_session_cannot_be_persisted_as_final(monkeypatch, tmp_path):
    _, _, client, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    monkeypatch.setattr(importer, "datetime", SimpleNamespace(now=lambda zone: datetime(2024, 3, 4, 15, tzinfo=zone)))
    with pytest.raises(SystemExit, match="unfinished exchange session"):
        importer.main()
    client.assert_not_called()
    repository.assert_not_called()


def test_duplicate_selected_price_ticker_is_rejected(monkeypatch, tmp_path):
    _, _, client, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    rows = client.return_value.fetch_grouped_daily.return_value
    rows.append(dict(rows[0]))
    with pytest.raises(ValueError, match="DUPLICATE_PRICE_TICKER"):
        importer.main()
    repository.return_value.persist.assert_not_called()


def test_explicit_non_common_mode_uses_dated_etv_identity(monkeypatch, tmp_path):
    arguments, _, _, repository = prepare_import(monkeypatch, tmp_path, apply=True)
    arguments.include_non_common = True
    for session in ("2024-03-01", "2024-03-04"):
        write_references(tmp_path, session, [])
        write_references(tmp_path, session, [reference(type="ETV")], "ETV")
    assert importer.main() == 0
    assert [bar.ticker for bar in repository.return_value.persist.call_args.args[0]] == ["TEST", "SPY"]