"""R17 (Nov 10, live): numpy scalars in a research rule's result must not fail the Stock Alerts batch."""
import numpy as np
import pandas as pd

from realtime.canonical_stock_alert_research import _plain
from realtime.contracts import canonical_sha256


def test_rule_results_are_plain_values_the_canonical_hash_accepts():
    at = pd.Timestamp("2026-11-10T15:30:00Z")
    result = {"status": "NO_MATCH", "reasons": ["X"], "gates": [np.bool_(True)],
              "state": {"armed": np.bool_(False), "count": np.int64(3), "level": np.float64(1.5),
                        "seen": (np.bool_(True), 2), "since": at.isoformat()}}

    plain = _plain(result)

    assert plain["state"] == {"armed": False, "count": 3, "level": 1.5, "seen": (True, 2), "since": at.isoformat()}
    assert type(plain["state"]["armed"]) is bool and type(plain["state"]["count"]) is int
    assert type(plain["gates"][0]) is bool
    canonical_sha256(plain)
    assert _plain({"a": [1, "b", None]}) == {"a": [1, "b", None]}


def test_one_member_that_cannot_be_evaluated_is_that_members_error_not_the_batchs(monkeypatch, caplog):
    """R17 follow-up (Nov 11, INV-7): a failure building one member's evaluation records that member's rules as ERROR
    with its prior state, and every other member is evaluated as usual."""
    from datetime import date, datetime, timezone
    from types import SimpleNamespace
    from uuid import UUID

    import realtime.canonical_stock_alert_research as research_module

    calls = []

    def fake_member_evaluation(inputs, research, member, row, results, rules_state, **common):
        calls.append((member.ticker, results, rules_state))
        if member.ticker == "AAA" and len([c for c in calls if c[0] == "AAA"]) == 1:
            raise TypeError("unsupported canonical value: bool_")
        return (member.ticker, tuple(results))

    monkeypatch.setattr(research_module, "_member_evaluation", fake_member_evaluation)
    monkeypatch.setattr(research_module, "_product_members",
                        lambda inputs, membership: [{"security_id": "s1"}, {"security_id": "s2"}])
    monkeypatch.setattr(research_module, "_context", lambda *args: ("input", "id", "sha"))
    monkeypatch.setattr(research_module, "CanonicalStockAlertBatch", lambda **kwargs: SimpleNamespace(**kwargs))
    at = datetime(2026, 11, 10, 15, 30, tzinfo=timezone.utc)
    inputs = SimpleNamespace(recorded_by=at, session=date(2026, 11, 10), cohort_id=UUID(int=1), cutoff=at)
    research = SimpleNamespace(members=[
        SimpleNamespace(security_id="s1", ticker="AAA", is_stock=False),
        SimpleNamespace(security_id="s2", ticker="BBB", is_stock=False),
    ], data=None)
    prior = SimpleNamespace(revision=4, research_state={"rules": {"r1": {"processed_until": "x", "research_state": 1}}})
    with caplog.at_level("WARNING"):
        batch = research_module.produce_research_rule_batch(
            inputs, research, window_id=UUID(int=2), input_receipt_id=UUID(int=3), receipt_input_sha256="a" * 64,
            membership=None, prior_states={research_module.member_state_key("s1"): prior}, deadline=None,
            detector_input_sha256="b" * 64, version="v",
        )
    assert batch.evaluations == (("AAA", ()), ("BBB", ()))
    # AAA: the failed build, then the fallback with its prior rules state; BBB once.
    assert [c[0] for c in calls] == ["AAA", "AAA", "BBB"]
    assert calls[1][2] == {"r1": {"processed_until": "x", "research_state": 1}}
    assert "AAA evaluation failed" in caplog.text
