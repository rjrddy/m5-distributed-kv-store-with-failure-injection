from main import linearizable


def test_real_time_precedence_rejects_stale_completed_read():
    history = [
        {"kind": "put", "key": "x", "value": 1, "start": 0, "end": 1},
        {"kind": "get", "key": "x", "value": 0, "start": 2, "end": 3},
    ]
    assert not linearizable(history)
    history[1]["value"] = 1
    assert linearizable(history)


def test_overlapping_operations_allow_valid_serialization():
    history = [
        {"kind": "get", "key": "x", "value": None, "start": 0, "end": 3},
        {"kind": "put", "key": "x", "value": 1, "start": 1, "end": 2},
    ]
    assert linearizable(history)
