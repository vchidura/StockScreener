from datetime import timedelta
import json
from types import SimpleNamespace
from uuid import uuid4

from realtime.forward_storage import decode_forward_artifact, encode_forward_artifact
from realtime.option_pre_session_staged_runner import PreparedStagedOptionEnrollment
from realtime.option_pre_session_staging import option_pre_session_run_id
from scripts import run_realtime_option_staged_enrollment as script
from tests.realtime.test_option_pre_session_target import _request_for, _target_plan
from tests.realtime.test_option_session_manifest import MARKET_TIME


def test_prepared_staged_enrollment_codec_preserves_exact_target_plan():
    plan = _target_plan()
    request = _request_for(plan)
    value = PreparedStagedOptionEnrollment("a" * 64, "b" * 64,
        option_pre_session_run_id(request), plan, MARKET_TIME + timedelta(minutes=10),
        MARKET_TIME + timedelta(seconds=1))

    payload, digest = encode_forward_artifact(value)

    assert decode_forward_artifact(payload, digest) == value


def test_staged_cli_plan_supports_50_underlyings_without_io(monkeypatch, tmp_path, capsys):
    stocks = [f"X{index:03d}" for index in range(47)]
    etfs = ["SPY", "QQQ", "IWM"]
    universe = tmp_path / "universe.json"
    universe.write_text(json.dumps({"version": "reviewed-staged-50-v1",
        "equity_tickers": [*stocks, *etfs], "option_underlyings": [*stocks, *etfs]}), encoding="utf-8")
    monkeypatch.setattr(script, "load_dotenv", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("PLAN loaded env")))
    monkeypatch.setattr(script.pool, "SimpleConnectionPool",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("PLAN opened pool")))

    assert script.main(["--session-date", "2026-10-09", "--deadline", "2026-10-09T15:00:00+00:00",
        "--maximum-requests", "5000", "--maximum-response-bytes", "1073741824",
        "--universe-file", str(universe)]) == 0

    report = json.loads(capsys.readouterr().out)
    assert report["mode"] == "PLAN"
    assert report["option_underlying_count"] == report["equity_count"] == 50
    assert report["selected_slot_count"] == 600
    assert report["detector_shard_count"] == 4
    assert report["migration_required"] == "019_realtime_option_pre_session_staging"
    assert report["dividend_input_source"] is None
    assert report["default_dividend_yield"] == "0"
    assert report["provider_requests"] == 0 and report["database_writes"] is False


def test_staged_runtime_uses_current_builder_signature(monkeypatch):
    captured = {}
    source = SimpleNamespace(request_transport=SimpleNamespace())
    runtime = SimpleNamespace(runner=SimpleNamespace(references_database=object(),
        control_database=object(), source=source), target_plan=object())
    monkeypatch.setattr(script, "build_option_pre_session_runtime",
        lambda plan, **kwargs: captured.update(kwargs) or runtime)
    staged_runner = object()
    monkeypatch.setattr(script, "StagedTargetOptionPreSessionRunner",
        lambda *args, **kwargs: staged_runner)
    args = SimpleNamespace(package_maximum_requests=120,
        package_maximum_response_bytes=67_108_864,
        deadline=MARKET_TIME + timedelta(minutes=10))

    wired_runtime, wired_runner, scheduler = script._runtime(args, {}, object())

    assert (wired_runtime, wired_runner) == (runtime, staged_runner)
    assert captured["request_scheduler"] is scheduler
    assert "wait" not in captured


def test_staged_finalization_reports_blocked_selection_without_fake_cohort():
    selection_id = uuid4()
    result_value = SimpleNamespace(selection_id=selection_id,
        plan=SimpleNamespace(reason_codes=("REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE",),
            selected_contract_count=84))
    receipt = SimpleNamespace(cohort_id=None)

    result = script._finalization_result(receipt, result_value)

    assert result == {"status": "STAGED_SELECTION_BLOCKED", "cohort_id": None,
        "selection_id": str(selection_id),
        "reason_codes": ["REQUIRED_STRATEGY_COVERAGE_UNAVAILABLE"],
        "selected_contract_count": 84}