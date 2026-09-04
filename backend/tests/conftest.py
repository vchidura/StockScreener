import os
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


# Local development read flags must not change default-path tests at import time.
for name in (
    "EQUITY_MATERIALIZED_30M_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1H_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1D_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1WK_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_1MO_SETUP_ENABLED",
    "EQUITY_MATERIALIZED_PATTERN_WATCH_ENABLED",
    "EQUITY_MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED",
    "EQUITY_MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED",
):
    os.environ[name] = "false"
