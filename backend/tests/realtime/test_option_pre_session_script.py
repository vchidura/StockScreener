from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, uuid5

import pytest

from options.domain import ContractType
from realtime.contracts import CANARY_UNIVERSE
from realtime.option_manifest_repository import option_manifest_selection_id
from realtime.option_pre_session_producer import OptionPreSessionProducerResult
from realtime.option_session_manifest import build_bounded_option_session_manifest
from realtime.preflight import ProviderTier
from scripts import run_realtime_option_pre_session
from tests.realtime.test_option_session_manifest import (
    MARKET_TIME,
    SESSION_DATE,
    _candidates,
    _scope,
)


def _argv(*extra):
    return [
        "run_realtime_option_pre_session.py",
        "--database",
        "stocks_realtime_v1",
        "--confirm-database-name",
        "stocks_realtime_v1",
        *extra,
    ]


def _execute_argv():
    return _argv(
        "--execute",
        "--scheduled-cycle",
        MARKET_TIME.isoformat(),
        "--effective-at",
        (MARKET_TIME - timedelta(minutes=30)).isoformat(),
    )


def _result(process_plan, *, blocked=False):
    policy = process_plan.manifest_policy
    scope = _scope(ProviderTier.MASSIVE_DEVELOPER, CANARY_UNIVERSE)
    candidates = _candidates(
        CANARY_UNIVERSE,
        policy.target_dtes,
        policy.strikes_per_side_per_expiry,
    )
    if blocked:
        candidates = tuple(
            row
            for row in candidates
            if not (
                row.catalog.underlyer == "AAPL"
                and row.snapshot.calendar_dte == 60
                and row.catalog.contract_type is ContractType.PUT
            )
        )
    plan = build_bounded_option_session_manifest(
        scope,
        policy,
        candidates,
        session_date=SESSION_DATE,
    )
    return OptionPreSessionProducerResult(
        discovery_batch_ids=tuple(
            uuid5(NAMESPACE_URL, f"script-discovery:{ticker}")
            for ticker in CANARY_UNIVERSE.option_underlyings
        ),
        selection_id=option_manifest_selection_id(plan),
        plan=plan,
        provider_requests=8,
        response_bytes=8_000,
    )


class FakeRunner:
    def __init__(self, result):
        self.result = result
        self.plans = []

    def run_once(self, plan):
        self.plans.append(plan)
        return self.result


def test_pre_session_plan_is_exact_inert_and_redacted(monkeypatch, capsys):
    monkeypatch.setattr(run_realtime_option_pre_session, "RUNTIME_FACTORY", None)
    monkeypatch.setattr("sys.argv", _argv())

    assert run_realtime_option_pre_session.main() == 0

    output = capsys.readouterr().out
    assert '"status": "PLAN"' in output
    assert '"option_underlying_count": 8' in output
    assert '"target_dtes": [' in output
    assert '"planned_primary_contracts": 128' in output
    assert '"reserved_option_quote_contracts": 200' in output
    assert '"maximum_option_quote_contracts": 1000' in output
    assert '"maximum_provider_requests": 320' in output
    assert '"provider_requests": 0' in output
    assert '"database_writes": false' in output
    assert '"canonical_fallback": false' in output
    assert '"runtime_factory_installed": false' in output
    assert "POLYGON_API_KEY" not in output
    assert "REALTIME_RUNTIME_DSN" not in output


def test_nine_name_cli_refuses_implicit_budget_policy_or_valuation_defaults(monkeypatch):
    monkeypatch.setattr("sys.argv", _argv("--forward-nine"))
    with pytest.raises(SystemExit):
        run_realtime_option_pre_session.parse_args()


def test_explicit_nine_name_cli_plan_pins_policies_budgets_without_loading_secrets(monkeypatch, tmp_path, capsys):
    import json

    environment_loader, launcher = MagicMock(), MagicMock()
    monkeypatch.setattr(run_realtime_option_pre_session, "load_dotenv", environment_loader)
    monkeypatch.setattr(run_realtime_option_pre_session, "DeveloperForwardEnrollmentLauncher", launcher)
    monkeypatch.setattr("sys.argv", _argv("--forward-nine", "--session-date", "2026-10-09",
        "--deadline", "2026-10-09T15:00:00+00:00", "--maximum-requests", "360", "--maximum-response-bytes", "33554432",
        "--developer-policy", "options/policies/developer_v1.json", "--valuation-policy", "options/policies/valuation_raw_spot_v2.json",
        "--strategy-policy", "options/policies/strategy_technical_forward_v2.json"))
    assert run_realtime_option_pre_session.main() == 0
    output = capsys.readouterr().out
    report = json.loads(output.split("\nNo pools", 1)[0])
    assert report["mode"] == "PLAN"
    assert report["option_underlying_count"] == 9
    assert "NFLX" in report["option_underlyings"]
    assert report["planned_primary_contracts"] == 144
    assert report["maximum_provider_requests"] == 360
    assert report["data_ready"] is False
    assert len(report["valuation_inputs_sha256"]) == 64
    assert len(report["configuration_sha256"]) == len(report["strategy_policy_sha256"]) == 64
    environment_loader.assert_not_called()
    launcher.assert_not_called()


@pytest.mark.parametrize("failure", [None, "launcher", "identity", "database"])
def test_nine_name_cli_persists_hashed_receipt_or_sanitized_failure(monkeypatch, tmp_path, capsys, failure):
    import json
    from dataclasses import replace
    from realtime.contracts import DEVELOPER_FORWARD_UNIVERSE
    from realtime.forward_storage import decode_forward_artifact
    from tests.realtime.test_option_pre_session_runtime import _ready_forward_receipt

    receipt_file = tmp_path / "enrollment.json"
    report_file = tmp_path / "lifecycle.json"
    _, base_receipt = _ready_forward_receipt()
    calls = []
    from psycopg2.errors import UniqueViolation

    class DatabaseConflict(UniqueViolation):
        @property
        def pgcode(self):
            return "23505"

        @property
        def diag(self):
            return SimpleNamespace(constraint_name="uq_ref_option_contract_current_version",
                table_name="option_contract_versions", message_detail="fixture-secret-never-print")

    def launcher_factory(process_plan, configuration, **bindings):
        calls.append(bindings)
        receipt = replace(base_receipt, process_plan_sha256="a" * 64 if failure == "identity" else process_plan.sha256,
            cohort_binding=process_plan.cohort_binding)
        if failure in {"launcher", "database"}:
            error = DatabaseConflict("fixture-secret-never-print") if failure == "database" else RuntimeError("fixture-secret-never-print")
            run_once = MagicMock(side_effect=error)
        else:
            run_once = MagicMock(return_value=receipt)
        return SimpleNamespace(run_once=run_once)
    monkeypatch.setattr(run_realtime_option_pre_session, "DeveloperForwardEnrollmentLauncher", launcher_factory)
    monkeypatch.setattr(run_realtime_option_pre_session, "load_dotenv", MagicMock())
    original_backend = run_realtime_option_pre_session.BACKEND_DIR
    monkeypatch.setattr(run_realtime_option_pre_session, "BACKEND_DIR", tmp_path / "backend")
    monkeypatch.setattr(run_realtime_option_pre_session, "_reviewed_policy_path", lambda environ, name, default: original_backend / environ[name])
    monkeypatch.setattr(run_realtime_option_pre_session, "load_option_runtime_configuration",
        lambda environ, backend: __import__("options.config", fromlist=["load_option_runtime_configuration"]).load_option_runtime_configuration(environ, original_backend))
    arguments = _argv("--forward-nine", "--execute", "--session-date", "2026-10-09",
        "--deadline", "2026-10-09T15:00:00+00:00", "--maximum-requests", "360", "--maximum-response-bytes", "33554432",
        "--developer-policy", "options/policies/developer_v1.json", "--valuation-policy", "options/policies/valuation_zero_yield_research_v1.json",
        "--research-variant", "zero-yield-research-v1",
        "--strategy-policy", "options/policies/strategy_technical_forward_v2.json",
        "--enrollment-output", str(receipt_file), "--report-path", str(report_file),
        "--server-max-connections", "20", "--reserved-connections", "5")
    monkeypatch.setattr("sys.argv", arguments)
    if failure:
        with pytest.raises(RuntimeError, match="inspect its report") as error:
            run_realtime_option_pre_session.main()
        assert "fixture-secret-never-print" not in str(error.value)
        assert not receipt_file.exists()
    else:
        assert run_realtime_option_pre_session.main() == 0
        envelope = json.loads(receipt_file.read_text(encoding="utf-8"))
        receipt = decode_forward_artifact(envelope["payload_text"], envelope["payload_sha256"])
        assert receipt.result.plan.scope.universe == DEVELOPER_FORWARD_UNIVERSE
        assert len(receipt.result.catalog) == 216
    report = json.loads(report_file.read_text(encoding="utf-8"))
    if not failure:
        assert receipt.cohort_binding.valuation_sha256 == report["valuation_policy_sha256"]
        assert receipt.cohort_binding.configuration_sha256 == report["configuration_sha256"]
    assert report["state"] == ("FAILED" if failure else "COMPLETE")
    assert report["data_ready"] is False
    assert report["starts_services"] is report["unlocks_principals"] is False
    if failure == "database":
        assert report["database_sqlstate"] == "23505"
        assert report["database_constraint"] == "uq_ref_option_contract_current_version"
        assert report["database_table"] == "option_contract_versions"
        assert "message_detail" not in report
    assert "fixture-secret-never-print" not in report_file.read_text(encoding="utf-8")
    assert len(calls) == 1
    with pytest.raises(ValueError, match="new workspace-local"):
        run_realtime_option_pre_session.main()
    capsys.readouterr()


@pytest.mark.parametrize(
    ("blocked", "expected_status", "selected_count"),
    ((False, "READY", 128), (True, "BLOCKED", 124)),
)
def test_injected_execution_reports_ready_or_blocked_without_secrets(
    monkeypatch,
    capsys,
    blocked,
    expected_status,
    selected_count,
):
    process_plan = run_realtime_option_pre_session.build_process_plan(
        "stocks_realtime_v1"
    )
    runner = FakeRunner(_result(process_plan, blocked=blocked))
    target_plan = SimpleNamespace(kind="fake-target-plan")
    close = MagicMock()
    monkeypatch.setattr(
        run_realtime_option_pre_session,
        "RUNTIME_FACTORY",
        lambda reviewed_plan, args: SimpleNamespace(
            runner=runner,
            target_plan=target_plan,
            close=close,
        ),
    )
    monkeypatch.setattr("sys.argv", _execute_argv())

    assert run_realtime_option_pre_session.main() == 0

    output = capsys.readouterr().out
    assert f'"status": "{expected_status}"' in output
    assert f'"selected_contract_count": {selected_count}' in output
    assert '"discovery_batch_count": 8' in output
    assert '"provider_requests": 8' in output
    assert '"database_writes": true' in output
    assert '"canonical_fallback": false' in output
    assert '"secrets_exposed": false' in output
    assert runner.plans == [target_plan]
    close.assert_called_once_with()
    assert "fake-only" not in output


def test_execute_without_runtime_factory_fails_before_side_effects(monkeypatch):
    monkeypatch.setattr(run_realtime_option_pre_session, "RUNTIME_FACTORY", None)
    monkeypatch.setattr("sys.argv", _execute_argv())

    with pytest.raises(RuntimeError, match="runtime is not installed"):
        run_realtime_option_pre_session.main()


def test_runtime_result_cannot_change_reviewed_scope_or_budget(monkeypatch):
    process_plan = run_realtime_option_pre_session.build_process_plan(
        "stocks_realtime_v1"
    )
    changed = _result(process_plan)
    changed = OptionPreSessionProducerResult(
        discovery_batch_ids=changed.discovery_batch_ids,
        selection_id=changed.selection_id,
        plan=changed.plan,
        provider_requests=process_plan.maximum_provider_requests + 1,
        response_bytes=changed.response_bytes,
    )
    monkeypatch.setattr(
        run_realtime_option_pre_session,
        "RUNTIME_FACTORY",
        lambda reviewed_plan, args: SimpleNamespace(
            runner=FakeRunner(changed),
            target_plan=SimpleNamespace(),
            close=MagicMock(),
        ),
    )
    monkeypatch.setattr("sys.argv", _execute_argv())

    with pytest.raises(ValueError, match="source budget"):
        run_realtime_option_pre_session.main()


def test_pre_session_script_has_no_live_or_canonical_construction():
    source = Path(run_realtime_option_pre_session.__file__).read_text(encoding="utf-8")

    for forbidden in (
        "RequestsPolygonHttpTransport",
        "backend.database",
        "from database",
        "main:app",
        "canonical_fallback = True",
    ):
        assert forbidden not in source
    assert "_execution_environment" in source
    assert "RUNTIME_FACTORY is None" in source
    assert "if not args.execute:" in source
    assert "finally:" in source and "runtime.close()" in source
    assert "No pools, HTTP clients, provider requests, or database writes constructed" in source
