"""Mark intraday scanner outcomes stale so they are recomputed under a correction."""
import argparse
import json
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env")

from equity.intraday_research import (
    IntradayScannerEvent,
    intraday_event_evidence_id,
)
from equity.repositories import EquityOutcomeRepository

BATCH = 10000


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--reason", default="INTRADAY_ENTRY_BOUNDARY_CORRECTION")
    arguments = parser.parse_args()

    identities = []
    for line in arguments.events.read_text(encoding="utf-8").splitlines():
        if line.strip():
            identities.append(
                intraday_event_evidence_id(
                    IntradayScannerEvent.from_dict(json.loads(line))
                )
            )

    repository = EquityOutcomeRepository()
    marked = 0
    for offset in range(0, len(identities), BATCH):
        marked += repository.mark_outcomes_stale(
            identities[offset:offset + BATCH], arguments.reason
        )
    print(json.dumps({
        "subjects": len(identities),
        "outcomes_marked_stale": marked,
        "reason": arguments.reason,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
