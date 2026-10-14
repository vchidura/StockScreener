from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
import sys

from psycopg2 import pool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.config import load_option_runtime_configuration
from realtime.contracts import canonical_sha256
from realtime.forward_launcher import DeveloperForwardCohortBinding
from realtime.forward_runtime import DeveloperForwardBudget, DeveloperForwardScheduler
from realtime.option_pre_session_runtime import build_option_pre_session_runtime
from realtime.preflight import ProviderTier
from scripts.run_realtime_option_pre_session import (
    _forward_nine_environment, build_developer_forward_process_plan,
)


class _NoIoTransport:
    def __init__(self, *args, **kwargs):
        pass

    def get(self, *args, **kwargs):
        raise AssertionError("runtime wiring diagnostic attempted provider I/O")


def main():
    now = datetime.now(timezone.utc)
    arguments = SimpleNamespace(env_file=BACKEND_DIR / ".env",
        developer_policy="options/policies/developer_forward_liquidity_v2.json",
        valuation_policy="options/policies/valuation_raw_spot_v2.json",
        strategy_policy="options/policies/strategy_technical_forward_v2.json")
    environment = _forward_nine_environment(arguments, execute=True)
    configuration = load_option_runtime_configuration(environment, BACKEND_DIR)
    plan = build_developer_forward_process_plan("stocks_realtime_v1", environ=environment,
        configuration=configuration, maximum_provider_requests=360,
        maximum_response_bytes=33_554_432, maximum_target_dte_deviation=4,
        target_dtes=(7, 21, 45))
    scheduler = DeveloperForwardScheduler(DeveloperForwardBudget(maximum_requests=360,
        maximum_response_bytes=33_554_432), clock=lambda: now, wait=lambda seconds: None)
    profile = SimpleNamespace(provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        universe_sha256=plan.universe.sha256,
        readiness=SimpleNamespace(status=SimpleNamespace(value="BLOCKED")))
    runtime = None
    try:
        runtime = build_option_pre_session_runtime(plan, scheduled_cycle=now,
            effective_at=now, environ=environment, backend_dir=BACKEND_DIR,
            pool_factory=pool.SimpleConnectionPool, transport_factory=_NoIoTransport,
            profile_refresher=lambda *args, **kwargs: profile, clock=lambda: now,
            request_scheduler=scheduler, source_deadline=now + timedelta(minutes=10))
    except Exception as error:
        report = {"status": "BLOCKED", "error_type": type(error).__name__,
            "error_reason": str(error)[:240] if isinstance(error, ValueError) else None,
            "provider_requests": scheduler.requests, "response_bytes": scheduler.response_bytes,
            "database_writes": 0, "values_exposed": False}
    else:
        try:
            with runtime.runner.references_database.transaction(read_only=True,
                    isolation_level="REPEATABLE READ") as cursor:
                request = runtime.runner.loader(cursor, runtime.target_plan)
        except Exception as error:
            report = {"status": "REQUEST_LOAD_BLOCKED", "error_type": type(error).__name__,
                "error_reason": str(error)[:240] if isinstance(error, ValueError) else None,
                "provider_requests": scheduler.requests, "response_bytes": scheduler.response_bytes,
                "database_writes": 0, "values_exposed": False}
        else:
            report = {"status": "REQUEST_LOAD_READY", "profile_id": str(runtime.target_plan.scope.profile_id),
                "cohort_binding_sha256": canonical_sha256(
                    DeveloperForwardCohortBinding.from_configuration(configuration)),
                "underlyings": list(request.scope.universe.option_underlyings),
                "security_identity_count": len(request.underlying_security_ids),
                "provider_requests": scheduler.requests, "response_bytes": scheduler.response_bytes,
                "database_writes": 0, "values_exposed": False}
    finally:
        if runtime is not None:
            runtime.close()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())