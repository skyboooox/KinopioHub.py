"""Version 4 JSON wire protocol shared with the JavaScript SDK."""
from __future__ import annotations

import copy as _copy
import json
import math
import re
import time
import uuid
from builtins import id as builtins_id
from typing import Any

import rfc8785

VERSION = "3.0.0"
PROTOCOL = 4
MAX_VALUE_BYTES = 64 * 1024


class KinopioError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class _Unset:
    def __repr__(self) -> str:
        return "UNSET"

    def __deepcopy__(self, memo: dict[int, Any]) -> _Unset:
        return self


UNSET = _Unset()


def fail(code: str, message: str) -> Any:
    raise KinopioError(code, message)


def id() -> str:
    return str(uuid.uuid4())


def name(value: Any, label: str = "name") -> str:
    try:
        if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 128 or re.search(r"[\x00-\x1f\x7f]", value):
            raise ValueError
    except (UnicodeError, ValueError):
        fail("INVALID_NAME", f"{label} must be 1–128 UTF-8 bytes without control characters")
    return value


def token(value: str) -> str:
    return name(value).encode().hex()


def key(namespace: str, variable: str) -> str:
    return f"{token(namespace)}.{token(variable)}"


def prefix(namespace: str) -> str:
    return f"_sys.v4.{token(namespace)}"


def copy(value: Any) -> Any:
    return _copy.deepcopy(value)


def _keys(value: dict[str, Any]) -> list[str]:
    integer = [k for k in value if len(k) <= 10 and k.isascii() and k.isdigit() and str(int(k)) == k and int(k) < 4294967295]
    integer_set = set(integer)
    return sorted(integer, key=int) + [k for k in value if k not in integer_set]


def js_stringify(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, (int, float)):
        return rfc8785.dumps(float(value) if isinstance(value, int) and abs(value) > 9007199254740991 else value).decode()
    if isinstance(value, list):
        return "[" + ",".join(js_stringify(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(js_stringify(k) + ":" + js_stringify(value[k]) for k in _keys(value)) + "}"
    raise TypeError("Expected JSON data")


def value_of(value: Any, max_bytes: int = MAX_VALUE_BYTES) -> Any:
    seen: set[int] = set()
    nodes = 0

    def visit(v: Any, depth: int) -> Any:
        nonlocal nodes
        nodes += 1
        if nodes > 20000 or depth > 32:
            fail("INVALID_VALUE", "Value is too deeply nested or complex")
        if v is None or type(v) is bool:
            return v
        if type(v) is str:
            try:
                v.encode("utf-8")
            except UnicodeError:
                fail("INVALID_VALUE", "Strings must contain valid Unicode")
            return v
        if type(v) in (int, float):
            if (type(v) is int and abs(v) > 9007199254740991) or not math.isfinite(v) or (v == int(v) and abs(v) > 9007199254740991):
                fail("INVALID_VALUE", "Use finite JSON values and safe integers")
            return 0 if v == 0 else v
        if type(v) not in (list, dict) or builtins_id(v) in seen:
            fail("INVALID_VALUE", "Use plain JSON values without cycles")
        seen.add(builtins_id(v))
        if isinstance(v, list):
            result: Any = [visit(item, depth + 1) for item in v]
        else:
            if any(type(k) is not str for k in v):
                fail("INVALID_VALUE", "Object keys must be strings")
            try:
                keys = sorted(v, key=lambda k: k.encode("utf-16-be"))
            except UnicodeError:
                fail("INVALID_VALUE", "Object keys must contain valid Unicode")
            result = {k: visit(v[k], depth + 1) for k in keys}
        seen.remove(builtins_id(v))
        return result

    result = visit(value, 0)
    if len(encode(result)) > max_bytes:
        fail("VALUE_TOO_LARGE", f"Value exceeds {max_bytes} bytes")
    return result


def version_of(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or not isinstance(value.get("counter"), str) or not re.fullmatch(r"[1-9][0-9]{0,39}", value["counter"]) or not isinstance(value.get("writer"), str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value["writer"]):
        fail("INVALID_VERSION", "Invalid logical version")
    return {"counter": value["counter"], "writer": value["writer"]}


def compare(a: Any, b: Any) -> int:
    if not a:
        return -1 if b else 0
    if not b:
        return 1
    av, bv = (int(a["counter"]), a["writer"]), (int(b["counter"]), b["writer"])
    return (av > bv) - (av < bv)


def record_of(value: Any, max_bytes: int = MAX_VALUE_BYTES) -> dict[str, Any]:
    if not isinstance(value, dict) or "scope" in value or ("deleted" in value and type(value["deleted"]) is not bool):
        fail("INVALID_RECORD", "Expected a variable record")
    record = {"name": name(value.get("name"), "variable"), "version": version_of(value.get("version")), "deleted": value.get("deleted") is True}
    if not record["deleted"]:
        record["value"] = value_of(value.get("value", UNSET), max_bytes)
    return record


def encode(value: Any) -> bytes:
    return js_stringify(value).encode("utf-8")


def decode(data: bytes) -> Any:
    if len(data) > 1024 * 1024:
        fail("MESSAGE_TOO_LARGE", "Protocol message exceeds 1 MiB")
    try:
        return json.loads(data.decode("utf-8"), parse_constant=lambda _: fail("INVALID_MESSAGE", "Invalid JSON message"))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise KinopioError("INVALID_MESSAGE", "Invalid JSON message") from error


def safe_error(error: Any) -> dict[str, Any]:
    return {"code": getattr(error, "code", "SDK_ERROR"), "message": re.sub(r"(?:nats|tls|wss?)://[^\s]+", "[endpoint]", str(error))[:240], "at": int(time.time() * 1000)}


def status_of(value: Any, instance_id: str, namespace: str) -> dict[str, Any]:
    try:
        value = value_of(value, 2048)
        if not isinstance(value, dict) or value.get("instanceId") != instance_id or value.get("namespace") != namespace or "name" in value or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", instance_id):
            raise ValueError
        allowed = "instanceId namespace sdk version runtime uptimeMs connection server rttMs lastSwitchReason pendingVariables pendingBytes variables subscriptions reconnects sentMessages receivedMessages sentBytes receivedBytes health currentError lastError mesh messaging".split()
        result = {k: value[k] for k in allowed if k in value}
        for field in "uptimeMs pendingVariables pendingBytes variables subscriptions reconnects sentMessages receivedMessages sentBytes receivedBytes".split():
            if field in result and (type(result[field]) not in (int, float) or result[field] < 0):
                raise ValueError
        if result.get("rttMs") is not None and (type(result["rttMs"]) not in (int, float) or result["rttMs"] < 0):
            raise ValueError
        for field in "namespace sdk version runtime connection health".split():
            if field in result and not isinstance(result[field], str):
                raise ValueError
        for field in ("server", "lastSwitchReason"):
            if result.get(field) is not None and not isinstance(result[field], str):
                raise ValueError
        if "connection" in result and result["connection"] not in ("starting", "connecting", "connected", "offline", "error", "closed"):
            raise ValueError
        if "health" in result and result["health"] not in ("ok", "warning", "error"):
            raise ValueError
        for field in ("currentError", "lastError"):
            detail = result.get(field)
            if detail is not None and (not isinstance(detail, dict) or not isinstance(detail.get("code"), str) or not isinstance(detail.get("message"), str) or type(detail.get("at")) not in (int, float)):
                raise ValueError
        if "messaging" in result:
            messaging = result['messaging']
            if not isinstance(messaging, dict):
                raise ValueError
            fields = ('pendingRequests', 'pendingMessages', 'pendingBytes', 'inFlightHandlers', 'droppedMessages', 'nativeDroppedMessages')
            if messaging.get('phase') not in ('offline', 'active', 'handoff', 'draining', 'closed'):
                raise ValueError
            for field in fields:
                if field in messaging and messaging[field] is not None and (type(messaging[field]) not in (int, float) or messaging[field] < 0):
                    raise ValueError
            result['messaging'] = {key: messaging[key] for key in ('phase', *fields) if key in messaging}
        if "mesh" in result:
            mesh = result["mesh"]
            if not isinstance(mesh, dict) or mesh.get("role") not in ("discovering", "candidate", "leader", "follower", "error", "disabled") or (mesh.get("leaderId") is not None and (not isinstance(mesh["leaderId"], str) or len(mesh["leaderId"]) > 128)) or type(mesh.get("members")) is not int or not 0 <= mesh["members"] <= 9007199254740991 or not isinstance(mesh.get("reason"), str) or len(mesh["reason"]) > 240 or (mesh.get("upstreamConnected") is not None and type(mesh["upstreamConnected"]) is not bool):
                raise ValueError
            result["mesh"] = {k: mesh.get(k) for k in ("role", "leaderId", "members", "reason", "upstreamConnected")}
        return result
    except (ValueError, TypeError, KinopioError) as error:
        raise KinopioError("INVALID_REPORT", "Invalid SDK instance status") from error
