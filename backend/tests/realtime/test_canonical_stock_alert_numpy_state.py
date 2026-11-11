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
