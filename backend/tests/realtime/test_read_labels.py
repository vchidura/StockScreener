"""G102 A2 (design 6.1): the labels a page read carries when an input changed after publication."""
from realtime.read_labels import input_change_labels


def test_no_change_has_no_labels():
    assert input_change_labels([], False) is None


def test_a_reference_change_names_the_tickers_once_in_order():
    labels = input_change_labels(["MSFT", "AAPL", "MSFT"], False)
    assert labels["reference_status"] == "CHANGED" and labels["action_status"] == "CURRENT"
    assert labels["changed_reference_tickers"] == ["AAPL", "MSFT"]
    assert list(labels["section_warnings"]) == ["reference"]
    assert "(AAPL, MSFT)" in labels["section_warnings"]["reference"]


def test_an_action_change_and_a_reference_change_are_both_labelled():
    labels = input_change_labels(["AAPL"], True)
    assert (labels["reference_status"], labels["action_status"]) == ("CHANGED", "CHANGED")
    assert list(labels["section_warnings"]) == ["reference", "actions"]
    assert input_change_labels([], True)["section_warnings"].keys() == {"actions"}
