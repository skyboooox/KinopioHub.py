"""Process-local replicated variables and broker selection."""

from __future__ import annotations

from kinopio_hub._compat import timeout as async_timeout

import asyncio
import inspect
import math
import platform
import time
from typing import Any, Callable

from . import _protocol as p
from . import _transport
from ._connection import Connection
from ._variables import Variable, VariableStore
from ._instances import Instances
from ._live import LiveChannel
from ._messaging import Messaging

Callback = Callable[..., Any]


class KinopioHub:
    def __init__(
        self,
        namespace: str | None = None,
        *,
        servers: str | list[str] | None = None,
        mesh: bool | dict[str, Any] = True,
        **options: Any,
    ):
        if not isinstance(mesh, (bool, dict)) or (
            "selection" in options and not isinstance(options["selection"], dict)
        ):
            raise p.KinopioError("INVALID_OPTIONS", "mesh and selection must use supported option types")
        self._namespace = p.name(p.id() if namespace is None else namespace, "namespace")
        self.options: dict[str, Any] = {
            "mesh": mesh,
            "health_interval": 5,
            "probe_interval": 15,
            "timeout": 3,
            "max_variables": 10000,
            "max_memory_bytes": 16 * 1024 * 1024,
            "max_instances": 1024,
            **options,
        }
        self.options.setdefault("peer_timeout", self.options["timeout"] * 0.8)
        for field in (
            "health_interval",
            "probe_interval",
            "timeout",
            "peer_timeout",
            "max_variables",
            "max_memory_bytes",
            "max_instances",
        ):
            v = self.options[field]
            if (
                type(v) not in (int, float)
                or not math.isfinite(v)
                or v <= 0
                or (field.startswith("max_") and (type(v) is not int or v > 9007199254740991))
                or (not field.startswith("max_") and v * 1000 > 2147483647)
            ):
                raise p.KinopioError(
                    "INVALID_OPTIONS", f"{field} must be positive and within supported limits"
                )
        if "name" in options:
            raise p.KinopioError("INVALID_OPTIONS", "name is no longer supported")
        for field, v in options.get("selection", {}).items():
            if (
                field not in ("improvement_ms", "improvement_ratio", "cooldown")
                or type(v) not in (int, float)
                or not math.isfinite(v)
                or v < 0
            ):
                raise p.KinopioError("INVALID_OPTIONS", "Invalid selection settings")
        self.explicit_servers = servers is not None
        self.servers = [
            _transport.endpoint(u)
            for u in (
                [servers]
                if isinstance(servers, str)
                else servers
                if servers is not None
                else ["nats://127.0.0.1:4222"]
                if mesh is False
                else []
            )
        ]
        if servers is not None:
            self.options["servers"] = self.servers
        self.connection_timeout = options.get("timeout", 60 if mesh is not False else 3)
        self._live_channels: dict[str, LiveChannel] = {}
        self.store = VariableStore(self)
        self.connection = Connection(self)
        self.messaging = Messaging(self)
        self.instances = Instances(self)
        self.writer = p.id()
        self.instance_id = p.id()
        self.listeners: dict[object, Callback] = {}
        self.discovered: dict[str, float] = {}
        self._discovery_transport: asyncio.DatagramTransport | None = None
        self.errors: dict[str, Any] = {}
        self.last_error: Any = None
        self.state = "starting"
        self.closed = False
        self.discovery_done = False
        self.started = time.monotonic()
        self.base = p.prefix(self.namespace)
        self.mesh_manager: Any = None
        self.mesh_leader: Any = None
        self.mesh_trusted_url: Any = None
        self.mesh_status = {
            "role": "disabled" if mesh is False else "discovering",
            "leaderId": None,
            "members": 0,
            "reason": "initializing",
            "upstreamConnected": None,
        }
        self.counters = dict.fromkeys(
            ("reconnects", "sentMessages", "receivedMessages", "sentBytes", "receivedBytes"), 0
        )
        self._tasks: set[asyncio.Task[Any]] = set()
        self._start_task: asyncio.Task[Any] | None = None
        self._close_task: asyncio.Task[Any] | None = None
        self._wake = asyncio.Event()
        self._changed = asyncio.Event()
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            self._ensure_started()

    def _spawn(self, coro: Any, source: str = "operation") -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._tasks.add(task)

        def done(t: asyncio.Task[Any]) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                self._error(t.exception(), source)

        task.add_done_callback(done)
        return task

    def _ensure_started(self) -> None:
        self._assert_open()
        if self._start_task is None:
            self._start_task = self._spawn(self._initialize())

    async def _initialize(self) -> None:
        if self.options["mesh"] is not False:
            self._spawn(self._start_mesh(), "mesh")
        if self.options.get("discovery") is not False:
            try:

                def discovered(candidate: Any) -> None:
                    if self.closed:
                        return
                    url = _transport.endpoint(candidate["url"])
                    if len(self.discovered) < 32 or url in self.discovered:
                        self.discovered[url] = time.monotonic() + 30
                    self._clear_error("discovery")
                    if not self.connection.active:
                        self._wake.set()

                self._discovery_transport = await _transport.discover(
                    discovered, lambda error: self._error(error, "discovery")
                )
            except Exception as error:
                self._error(error, "discovery")
        self._set_state("offline")
        self._spawn(self.connection._cycle_loop(), "connection")
        self._spawn(self.instances._health_loop(), "report")
        self._spawn(self._discover_window())

    async def _start_mesh(self) -> None:
        from ._mesh import acquire_mesh

        def leader(value: Any) -> None:
            if self.closed:
                return
            if (value or {}).get("url") != (self.mesh_leader or {}).get("url"):
                self.mesh_trusted_url = None
            self.mesh_leader = value
            if value and value.get("upstreamConnected"):
                self.mesh_trusted_url = value["url"]
            self._wake.set()

        def status(value: Any) -> None:
            if not self.closed:
                self.mesh_status = value
                self._notify()

        def error(value: Any) -> None:
            if value is None:
                self._clear_error("mesh")
            else:
                self._error(value, "mesh")

        self.mesh_manager = await acquire_mesh(
            self.options, {"on_leader": leader, "on_status": status, "on_error": error}
        )

    async def _discover_window(self) -> None:
        await asyncio.sleep(self.options["peer_timeout"])
        self.discovery_done = True
        self.store.emit()
        self._notify()

    def _assert_open(self) -> None:
        if self.closed:
            raise p.KinopioError("CLOSED", "SDK is closed")

    def _invoke(self, callback: Callback, *args: Any) -> None:
        try:
            result = callback(*args)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError("Watch callbacks must be synchronous")
        except Exception as error:
            handler = self.options.get("on_callback_error")
            if handler:
                try:
                    result = handler(error)
                    if inspect.iscoroutine(result):
                        result.close()
                except Exception:
                    pass

    def _listen(self, listeners: dict[object, Callback], callback: Callback) -> Callable[[], None]:
        self._assert_open()
        if not callable(callback) or inspect.iscoroutinefunction(callback):
            raise TypeError("Expected a synchronous callback")
        registration = object()
        listeners[registration] = callback
        return lambda: listeners.pop(registration, None) and None

    def watch(self, callback: Callback) -> Callable[[], None]:
        stop = self._listen(self.listeners, callback)
        self._invoke(callback, self.status())
        return stop

    def _notify(self) -> None:
        self._changed.set()
        for callback in list(self.listeners.values()):
            self._invoke(callback, self.status())

    def _set_state(self, state: str) -> None:
        changed = self.state != state
        self.state = state
        if changed:
            self.store.emit()
        self._notify()

    def _error(self, error: Any, source: str = "operation") -> None:
        if self.closed:
            return
        detail = p.safe_error(error)
        for secret in (self.options.get("token"), self.options.get("password")):
            if isinstance(secret, str) and secret:
                detail["message"] = detail["message"].replace(secret, "[redacted]")
        self.errors.pop(source, None)
        self.errors[source] = detail
        self.last_error = detail
        self._notify()

    def _clear_error(self, source: str) -> None:
        if self.errors.pop(source, None) is not None:
            self._notify()

    def status(self) -> dict[str, Any]:
        current_error = next(reversed(self.errors.values()), None)
        return {
            "instanceId": self.instance_id,
            "namespace": self.namespace,
            "sdk": "python",
            "version": p.VERSION,
            "runtime": f"python/{platform.python_version()}",
            "uptimeMs": round((time.monotonic() - self.started) * 1000),
            "connection": self.state,
            "mesh": p.copy(self.mesh_status),
            "messaging": self.messaging.status(),
            "server": _transport.display_endpoint(
                self.connection.active["url"] if self.connection.active else None
            ),
            "rttMs": self.connection.active["rtt"] if self.connection.active else None,
            "lastSwitchReason": self.connection.last_switch_reason,
            "pendingVariables": len(self.store.pending),
            "pendingBytes": sum(self.store.record_bytes[k] for k in self.store.pending),
            "variables": len(self.store.references),
            "subscriptions": sum(len(ref.listeners) for ref in self.store.references.values()),
            **self.counters,
            "health": ("warning" if set(self.errors) == {"discovery"} else "error")
            if current_error
            else "ok"
            if self.state == "connected"
            else "warning",
            "currentError": p.copy(current_error),
            "lastError": p.copy(self.last_error),
        }

    async def ready(self) -> KinopioHub:
        self._ensure_started()
        assert self._start_task is not None
        await asyncio.shield(self._start_task)
        self._assert_open()
        return self

    async def _wait(self, predicate: Callable[[], bool], timeout: float) -> None:
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise p.KinopioError("INVALID_OPTIONS", "timeout must be positive")
        try:
            async with async_timeout(timeout):
                await self.ready()
                while not predicate():
                    self._assert_open()
                    self._changed.clear()
                    await self._changed.wait()
                self._assert_open()
        except asyncio.TimeoutError as error:
            raise p.KinopioError(
                "TIMEOUT", "Operation timed out; its outcome may still need checking"
            ) from error

    async def connected(self, timeout: float | None = None) -> KinopioHub:
        await self._wait(
            lambda: self.state == "connected", self.connection_timeout if timeout is None else timeout
        )
        return self

    def _begin_close(self) -> asyncio.Task[Any]:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._shutdown())
        return self._close_task

    async def close(self) -> None:
        await asyncio.shield(self._begin_close())

    async def _shutdown(self) -> None:
        self.closed = True
        if self._discovery_transport:
            self._discovery_transport.close()
        self._set_state("closed")
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.messaging.close()
        for channel in self._live_channels.values():
            await channel.close()
        await self.connection.close()
        if self.mesh_manager:
            await self.mesh_manager.close()
        self.store.close()
        self.instances.close()
        self._notify()
        self.listeners.clear()

    async def __aenter__(self) -> KinopioHub:
        return await self.ready()

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def flush(self, timeout: float | None = None) -> None:
        await self.connection.flush(timeout)

    @property
    def namespace(self) -> str:
        return self._namespace

    def var(self, variable_name: str) -> Variable:
        return self.store._reference(variable_name)

    def live(self, name: str) -> LiveChannel:
        self._assert_open()
        p.name(name, 'live channel')
        if name not in self._live_channels:
            if len(self._live_channels) >= 128:
                raise p.KinopioError('LIMIT_EXCEEDED', 'At most 128 live channels are supported')
            self._live_channels[name] = LiveChannel(self, name)
        return self._live_channels[name]

    async def drain(self, *, timeout: float = 5) -> None:
        await self.messaging.drain(timeout)
