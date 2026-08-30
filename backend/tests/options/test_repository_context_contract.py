import inspect
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.repositories import (
    OptionAnalysisRepository,
    OptionContractCatalogRepository,
    OptionTradeRepository,
    OptionTradeSemanticsRepository,
    OptionUniverseRepository,
)


OPERATIONAL_READS = {
    (OptionTradeRepository, "get_cursor"),
}


def test_every_decision_facing_repository_read_requires_decision_context():
    repositories = (
        OptionAnalysisRepository,
        OptionContractCatalogRepository,
        OptionTradeRepository,
        OptionTradeSemanticsRepository,
        OptionUniverseRepository,
    )
    read_prefixes = ("get", "list", "find", "load")
    violations = []

    for repository in repositories:
        for name, method in inspect.getmembers(repository, inspect.isfunction):
            if name.startswith("_") or not name.startswith(read_prefixes):
                continue
            if (repository, name) in OPERATIONAL_READS:
                continue
            if "context" not in inspect.signature(method).parameters:
                violations.append(f"{repository.__name__}.{name}")

    assert violations == [], (
        "Decision-facing repository reads require DecisionContext: "
        + ", ".join(violations)
    )