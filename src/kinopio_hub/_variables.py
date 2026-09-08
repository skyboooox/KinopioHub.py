"""Process-local records, stable variable references and observation."""

from __future__ import annotations

from typing import Any, Callable, TYPE_CHECKING
from collections.abc import Iterable

from . import _protocol as p

if TYPE_CHECKING:
    from ._hub import KinopioHub

Callback = Callable[..., Any]


class VariableStore:
    def __init__(self, hub: KinopioHub):
        self.hub = hub
        self.clock = 0
        self.records: dict[str, Any] = {}
        self.pending: dict[str, Any] = {}
        self.record_bytes: dict[str, int] = {}
        self.memory_bytes = 0
        self.references: dict[str, Variable] = {}
        self.scopes: dict[str, Scope] = {}

    def scope(self, scope_name: str) -> Scope:
        self.hub._assert_open()
        p.name(scope_name, "scope")
        if scope_name not in self.scopes:
            if len(self.scopes) >= self.hub.options["max_variables"]:
                raise p.KinopioError("MEMORY_FULL", "Scope reference capacity reached")
            self.scopes[scope_name] = Scope(self.hub, scope_name)
        return self.scopes[scope_name]

    def _reference(self, scope: str, variable: str) -> Variable:
        self.hub._assert_open()
        k = p.key(scope, variable)
        if k not in self.references:
            if len(self.references) >= self.hub.options["max_variables"]:
                raise p.KinopioError("MEMORY_FULL", "Variable reference capacity reached")
            self.references[k] = Variable(self.hub, scope, variable)
        return self.references[k]

    def _commit(self, record: dict[str, Any], pending: bool = False) -> None:
        self.hub._assert_open()
        k = p.key(record["scope"], record["name"])
        current = self.records.get(k)
        order = p.compare(record["version"], current["version"] if current else None)
        if order < 0:
            return
        if order == 0:
            if p.encode(record) != p.encode(current):
                raise p.KinopioError("VERSION_COLLISION", "The same version has conflicting content")
            return
        size = len(p.encode(record))
        if (
            current is None and len(self.records) >= self.hub.options["max_variables"]
        ) or self.memory_bytes - self.record_bytes.get(k, 0) + size > self.hub.options["max_memory_bytes"]:
            raise p.KinopioError("MEMORY_FULL", "Memory capacity reached; previous values remain unchanged")
        self.memory_bytes += size - self.record_bytes.get(k, 0)
        self.record_bytes[k] = size
        self.records[k] = record
        self.clock = max(self.clock, int(record["version"]["counter"]))
        if pending:
            self.pending[k] = record["version"]
        elif p.compare(record["version"], self.pending.get(k)) > 0:
            self.pending.pop(k, None)
        self.emit((k,))
        self.hub._notify()

    async def _write(self, ref: Variable, value: Any, deleted: bool) -> None:
        self.hub._ensure_started()
        try:
            record = p.record_of(
                {
                    "scope": ref.scope_name,
                    "name": ref.name,
                    "version": {"counter": str(self.clock + 1), "writer": self.hub.writer},
                    "deleted": deleted,
                    "value": value,
                }
            )
            self._commit(record, True)
            self.hub._clear_error("write")
            self.hub.connection.schedule_flush()
        except Exception as error:
            self.hub._error(error, "write")
            raise

    def emit(self, keys: Iterable[str] | None = None) -> None:
        references = (
            list(self.references.values())
            if keys is None
            else [self.references[k] for k in keys if k in self.references]
        )
        for ref in references:
            ref._emit()

    def close(self) -> None:
        for ref in self.references.values():
            ref._closed_initialized = ref.meta["initialized"]
        self.records.clear()
        self.pending.clear()
        self.record_bytes.clear()
        self.memory_bytes = 0
        for ref in self.references.values():
            ref._emit(force=True)
            ref.listeners.clear()
        self.references.clear()
        self.scopes.clear()

    def _acknowledge(self, records: list[Any]) -> None:
        changed = []
        for record in records:
            k = p.key(record["scope"], record["name"])
            if p.compare(self.pending.get(k), record["version"]) == 0:
                self.pending.pop(k, None)
                changed.append(k)
        self.emit(changed)
        self.hub._clear_error("transport")
        self.hub._notify()


class Scope:
    def __init__(self, hub: KinopioHub, name: str):
        self.hub = hub
        self.name = name

    def var(self, variable_name: str) -> Variable:
        return self.hub.store._reference(self.name, variable_name)


class Variable:
    def __init__(self, hub: KinopioHub, scope_name: str, name: str):
        self.hub = hub
        self.scope_name = scope_name
        self.name = name
        self.key = p.key(scope_name, name)
        self.listeners: dict[object, Callback] = {}
        self._signature: Any = None
        self._closed_initialized = False

    @property
    def value(self) -> Any:
        record = self.hub.store.records.get(self.key)
        return p.copy(record["value"]) if record and not record["deleted"] else p.UNSET

    @property
    def meta(self) -> dict[str, Any]:
        record = self.hub.store.records.get(self.key)
        initialized = record is not None or self.hub.discovery_done or self._closed_initialized
        return {
            "initialized": initialized,
            "exists": not record["deleted"] if record else False if initialized else None,
            "version": p.copy(record["version"]) if record else None,
            "pending": self.key in self.hub.store.pending,
            "connected": self.hub.state == "connected",
        }

    def watch(self, callback: Callback) -> Callable[[], None]:
        stop = self.hub._listen(self.listeners, callback)
        meta = self.meta
        if meta["initialized"]:
            self._signature = p.encode(meta)
            self.hub._invoke(callback, self.value, meta)
        return stop

    def _emit(self, force: bool = False) -> None:
        if not self.listeners:
            return
        meta = self.meta
        signature = p.encode(meta)
        if not force and (not meta["initialized"] or signature == self._signature):
            return
        self._signature = signature
        for callback in list(self.listeners.values()):
            self.hub._invoke(callback, self.value, p.copy(meta))

    async def set(self, value: Any) -> None:
        await self.hub.store._write(self, value, False)

    async def delete(self) -> None:
        await self.hub.store._write(self, p.UNSET, True)

    async def ready(self, timeout: float | None = None) -> Variable:
        await self.hub._wait(
            lambda: self.meta["initialized"], self.hub.options["timeout"] if timeout is None else timeout
        )
        return self
