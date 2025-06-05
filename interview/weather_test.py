import pytest
from .weather import process_events

def test_sample_only_no_output():
    # If we only send a single sample, we shouldn’t see any output.
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 100, "temperature": 5.0}
    ]
    assert list(process_events(events)) == []


def test_snapshot_after_sample():
    # After one sample, asking for a snapshot should give exactly one JSON
    # with the high and low both equal to that sample’s temperature.
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 100, "temperature": 5.0},
        {"type": "control", "command": "snapshot"}
    ]
    output = list(process_events(events))
    assert len(output) == 1

    expected = {
        "type": "snapshot",
        "asOf": 100,
        "stations": {
            "A": {"high": 5.0, "low": 5.0}
        }
    }
    assert output[0] == expected


def test_reset_after_sample():
    # If we send one sample and then reset, we should get exactly one JSON
    # that says “type”: “reset” and “asOf” equal to that sample’s timestamp.
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 100, "temperature": 5.0},
        {"type": "control", "command": "reset"}
    ]
    output = list(process_events(events))
    assert len(output) == 1

    expected = {"type": "reset", "asOf": 100}
    assert output[0] == expected


def test_snapshot_ignored_before_any_sample():
    # If we ask for a snapshot before any sample has arrived, it should do nothing.
    events = [
        {"type": "control", "command": "snapshot"}
    ]
    assert list(process_events(events)) == []


def test_reset_ignored_before_any_sample():
    # If we ask for a reset before any sample has arrived, it should do nothing.
    events = [
        {"type": "control", "command": "reset"}
    ]
    assert list(process_events(events)) == []


def test_multiple_samples_snapshot():
    # Send several samples from two stations, then take a snapshot:
    #  - Station “A” at t=100, temp=5.0
    #  - Station “A” again at t=110, temp=7.0  (now A’s high=7.0, low=5.0)
    #  - Station “B” at t=120, temp=3.0       (B’s high=3.0, low=3.0)
    # Finally, control “snapshot” should produce:
    #  - asOf = 120
    #  - A: high=7.0, low=5.0
    #  - B: high=3.0, low=3.0
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 100, "temperature": 5.0},
        {"type": "sample", "stationName": "A", "timestamp": 110, "temperature": 7.0},
        {"type": "sample", "stationName": "B", "timestamp": 120, "temperature": 3.0},
        {"type": "control", "command": "snapshot"}
    ]
    output = list(process_events(events))
    assert len(output) == 1

    snapshot = output[0]
    assert snapshot["type"] == "snapshot"
    assert snapshot["asOf"] == 120

    expected_stations = {
        "A": {"high": 7.0, "low": 5.0},
        "B": {"high": 3.0, "low": 3.0}
    }
    assert snapshot["stations"] == expected_stations


def test_reset_clears_state_then_snapshot_after_new_sample():
    # 1) Send a sample for “A” at t=100.
    # 2) Reset → we should see a reset JSON with asOf=100, and then
    #    the internal data is wiped.
    # 3) Asking for a snapshot right after reset should do nothing.
    # 4) Send a new sample for “B” at t=200, temp=10.0.
    # 5) Asking for a snapshot now should give exactly one JSON that
    #    shows station B with high=10.0, low=10.0, asOf=200.
    events = [
        {"type": "sample", "stationName": "A", "timestamp": 100, "temperature": 5.0},
        {"type": "control", "command": "reset"},
        {"type": "control", "command": "snapshot"},    # should be ignored
        {"type": "sample", "stationName": "B", "timestamp": 200, "temperature": 10.0},
        {"type": "control", "command": "snapshot"}
    ]
    output = list(process_events(events))

    # We expect exactly two outputs:
    #   1) reset at t=100
    #   2) snapshot after the new sample
    assert len(output) == 2

    # First output: reset with asOf=100
    assert output[0] == {"type": "reset", "asOf": 100}

    # Second output: snapshot with asOf=200, showing only station B
    expected_snapshot = {
        "type": "snapshot",
        "asOf": 200,
        "stations": {
            "B": {"high": 10.0, "low": 10.0}
        }
    }
    assert output[1] == expected_snapshot


def test_malformed_message_raises_exception():
    # If a message doesn’t even have a “type” field, we should raise an Exception
    # and the exception message should mention “Please verify input.”
    with pytest.raises(Exception) as excinfo:
        list(process_events([{}]))
    assert "Please verify input." in str(excinfo.value)
