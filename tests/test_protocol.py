from typing import Any

import pytest

from kinopio_hub import KinopioError
from kinopio_hub import _protocol as p


def test_javascript_canonical_values():
    value = p.value_of({"\ue000": 1, "😀": 2, "10": 3, "2": 4, "a": -0.0})
    assert p.js_stringify(value) == '{"2":4,"10":3,"a":0,"😀":2,"\ue000":1}'
    assert p.js_stringify({"z": 1e-7, "a": .000001}) == '{"z":1e-7,"a":0.000001}'
    assert p.value_of({"1" * 5000: 1}) == {"1" * 5000: 1}


@pytest.mark.parametrize("value", [float("nan"), float("inf"), 2**53, float(2**53), "\ud800", {"\udfff": 1}, {1: "x"}, (1, 2)])
def test_invalid_values(value):
    with pytest.raises(KinopioError) as error:
        p.value_of(value)
    assert error.value.code == "INVALID_VALUE"


def test_bounds_cycles_and_shared_values():
    with pytest.raises(KinopioError, match="exceeds"):
        p.value_of("x" * 65536)
    cycle: list[Any] = []
    cycle.append(cycle)
    with pytest.raises(KinopioError):
        p.value_of(cycle)
    shared = [1]
    assert p.value_of([shared, shared]) == [[1], [1]]
    value: Any = None
    for _ in range(34):
        value = [value]
    with pytest.raises(KinopioError):
        p.value_of(value)


@pytest.mark.parametrize("counter", ["0", "01", "-1", "1" * 41, 1, "1\n"])
def test_bad_clock(counter):
    with pytest.raises(KinopioError):
        p.version_of({"counter": counter, "writer": "w"})


def test_record_and_decode():
    assert p.compare({"counter": "10", "writer": "a"}, {"counter": "9", "writer": "z"}) == 1
    assert p.record_of({"scope": "s", "name": "v", "version": {"counter": "1", "writer": "w"}, "deleted": True})["deleted"]
    for value in [b'NaN', b'Infinity', b'\xff', b'{', b'x' * (1024 * 1024 + 1)]:
        with pytest.raises(KinopioError):
            p.decode(value)
    assert p.prefix("中文") == "kh.v3.e4b8ade69687"


@pytest.mark.parametrize("field", "uptimeMs pendingVariables pendingBytes variables subscriptions reconnects sentMessages receivedMessages sentBytes receivedBytes".split())
def test_status_counters_reject_null(field):
    with pytest.raises(KinopioError) as error:
        p.status_of({"instanceId": "sdk", field: None}, "sdk")
    assert error.value.code == "INVALID_REPORT"


def test_status_optional_counters_and_nullable_rtt():
    assert p.status_of({"instanceId": "sdk"}, "sdk") == {"instanceId": "sdk"}
    status = {"instanceId": "sdk", "pendingBytes": 0, "rttMs": None}
    assert p.status_of(status, "sdk") == status
