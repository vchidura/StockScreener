"""G102 stack stop (Nov 13 rehearsal): a whole-stack stop left Options lifecycle and market-state with status 241 (the
child ended by the wrapper's SIGTERM, returncode -15). A requested stop now reports 0; other exits pass through."""
import importlib
import signal

wrapper = importlib.import_module("scripts.run_realtime_v2_container")


def test_a_child_ended_by_the_stop_signal_after_a_requested_stop_is_a_clean_exit():
    for received in (signal.SIGTERM, signal.SIGINT):
        assert wrapper.requested_stop_exit_code(-int(signal.SIGTERM), received) == 0
        assert wrapper.requested_stop_exit_code(-int(signal.SIGINT), received) == 0


def test_other_exits_pass_through():
    # No stop signal: a lost lease or the child's own failure keeps its code (the restart policy still sees it).
    assert wrapper.requested_stop_exit_code(-int(signal.SIGTERM), None) == -int(signal.SIGTERM)
    assert wrapper.requested_stop_exit_code(1, None) == 1
    # A requested stop where the child exits by itself keeps the child's own code, failure or not.
    assert wrapper.requested_stop_exit_code(0, signal.SIGTERM) == 0
    assert wrapper.requested_stop_exit_code(3, signal.SIGTERM) == 3
    assert wrapper.requested_stop_exit_code(-int(signal.SIGKILL), signal.SIGTERM) == -int(signal.SIGKILL)
