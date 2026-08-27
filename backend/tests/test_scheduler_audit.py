import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import run_scheduler


@contextmanager
def cursor_returning(row):
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    yield cursor


class SchedulerAuditTests(unittest.TestCase):
    def test_successful_self_heal_finishes_clean_and_scopes_active_universe(self):
        tickers = ["AAA", "BBB"]
        initial_cursor = MagicMock()
        initial_cursor.fetchone.return_value = {
            "daily": 2,
            "hourly": 2,
            "intraday": 2,
            "signals": 0,
            "discovery": 0,
            "events": 0,
            "unresolved": 0,
        }

        @contextmanager
        def initial_context():
            yield initial_cursor

        contexts = [
            initial_context(),
            cursor_returning({"n": 2}),
            cursor_returning({"n": 2}),
        ]
        with (
            patch.object(run_scheduler, "get_db_cursor", side_effect=contexts),
            patch.object(
                run_scheduler,
                "find_provisional_daily_rows",
                return_value={"provisional": [], "unverifiable": 0},
            ),
            patch.object(run_scheduler, "job_cross_sectional_signal") as signal_job,
            patch.object(run_scheduler, "job_market_discovery") as discovery_job,
        ):
            result = run_scheduler._post_run_audit(
                "2026-08-26", tickers, "polygon", True
            )

        self.assertEqual(result["issues"], [])
        self.assertEqual(result["counts"]["signals"], 2)
        self.assertEqual(result["counts"]["discovery"], 2)
        signal_job.assert_called_once_with()
        discovery_job.assert_called_once_with()

        query, params = initial_cursor.execute.call_args.args
        self.assertIn("WITH active AS", query)
        self.assertEqual(params[0], tickers)

    def test_unverifiable_daily_count_is_scoped_to_requested_tickers(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = {"n": 0}

        @contextmanager
        def context():
            yield cursor

        with patch.object(run_scheduler, "get_db_cursor", return_value=context()):
            result = run_scheduler.find_provisional_daily_rows(
                "2026-08-26", ["AAA", "BBB"]
            )

        self.assertEqual(result, {"provisional": [], "unverifiable": 0})
        second_query, second_params = cursor.execute.call_args_list[1].args
        self.assertIn("d.ticker = ANY(%s)", second_query)
        self.assertEqual(second_params, ["2026-08-26", ["AAA", "BBB"], "2026-08-26"])


if __name__ == "__main__":
    unittest.main()