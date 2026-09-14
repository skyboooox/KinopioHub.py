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
    assert p.record_of({"name": "v", "version": {"counter": "1", "writer": "w"}, "deleted": True})["deleted"]
    for value in [b'NaN', b'Infinity', b'\xff', b'{', b'x' * (1024 * 1024 + 1)]:
        with pytest.raises(KinopioError):
            p.decode(value)
    assert p.prefix("中文") == "_sys.v4.e4b8ade69687"


@pytest.mark.parametrize("field", "uptimeMs pendingVariables pendingBytes variables subscriptions reconnects sentMessages receivedMessages sentBytes receivedBytes".split())
def test_status_counters_reject_null(field):
    with pytest.raises(KinopioError) as error:
        p.status_of({"instanceId": "sdk", "namespace": "test", field: None}, "sdk", "test")
    assert error.value.code == "INVALID_REPORT"


def test_status_optional_counters_and_nullable_rtt():
    assert p.status_of({"instanceId": "sdk", "namespace": "test"}, "sdk", "test") == {"instanceId": "sdk", "namespace": "test"}
    status = {"instanceId": "sdk", "namespace": "test", "pendingBytes": 0, "rttMs": None}
    assert p.status_of(status, "sdk", "test") == status


@pytest.mark.parametrize("value", ["", "x" * 129, "\x00", "\x1f", "\x7f", "\ud800", None, 1])
def test_invalid_names(value):
    with pytest.raises(KinopioError) as error:
        p.token(value)
    assert error.value.code == "INVALID_NAME"


def test_names_are_literal_utf8_without_normalization():
    for value in [" ", ".", "*", ">", "中文", "😀" * 32, "é", "e\u0301", "\u0080", "\u0800"]:
        assert p.token(value) == value.encode("utf-8").hex()
        assert p.key(" namespace ", value) == "206e616d65737061636520." + value.encode().hex()
    assert p.token("é") != p.token("e\u0301")
    assert p.token("A") != p.token("a")
    assert p.PROTOCOL == 4
    with pytest.raises(KinopioError, match="record"):
        p.record_of({"scope": "old", "name": "v", "version": {"counter": "1", "writer": "w"}, "deleted": True})


@pytest.mark.parametrize("fields", [{}, {"namespace": "other"}, {"namespace": "test", "name": "legacy"}])
def test_status_requires_matching_namespace_and_no_legacy_name(fields):
    with pytest.raises(KinopioError) as error:
        p.status_of({"instanceId": "sdk", **fields}, "sdk", "test")
    assert error.value.code == "INVALID_REPORT"
