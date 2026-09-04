import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options import build_option_startup_state, load_option_runtime_configuration
from options.startup import ensure_option_partitions


UTC = timezone.utc


def test_phase_one_startup_is_read_only_and_sanitized():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "never-print-this"}, BACKEND_DIR
    )

    startup = build_option_startup_state(configuration)
    metadata = startup.metadata()

    assert metadata["mode"] == "READ_ONLY"
    assert "polygon_api_key" not in metadata
    assert "never-print-this" not in repr(metadata)


def test_phase_one_rejects_non_read_only_startup():
    configuration = load_option_runtime_configuration(
        {
            "POLYGON_API_KEY": "test-secret",
            "OPTION_START_READ_ONLY": "false",
        },
        BACKEND_DIR,
    )

    with pytest.raises(RuntimeError, match="OPTION_START_READ_ONLY=true"):
        build_option_startup_state(configuration)


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"OPTION_DATA_ENGINE": "polygon_advanced"}, "Advanced"),
        ({"OPTION_EXECUTION_ENGINE": "advanced_shadow"}, "execution engines"),
        ({"OPTION_EXECUTION_ENGINE": "alpaca"}, "execution engines"),
    ],
)
def test_future_engines_are_typed_but_fail_closed(override, message):
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret", **override}, BACKEND_DIR
    )

    with pytest.raises(RuntimeError, match=message):
        build_option_startup_state(configuration)


def test_partition_maintenance_requests_current_and_next_month():
    cursor = MagicMock()

    @contextmanager
    def get_cursor():
        yield cursor

    with patch("database.get_db_cursor", get_cursor):
        ensure_option_partitions(datetime(2026, 12, 31, 23, 0, tzinfo=UTC))

    assert [call.args[1] for call in cursor.execute.call_args_list] == [
        (datetime(2026, 12, 1).date(),),
        (datetime(2027, 1, 1).date(),),
    ]