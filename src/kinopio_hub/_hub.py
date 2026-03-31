from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass
from enum import Enum
from time import time
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, Protocol, Sequence

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription as NATSSubscription

KINOPIO_STATE_EVENT = "kinopio.state"

StateListener = Callable[["ConnectionState"], None]
VariableCallback = Callable[[Any, Msg], Awaitable[None] | None]
ServiceHandler = Callable[[Any, Msg], Awaitable[Any] | Any]
JSONDefault = Callable[[Any], Any]
JSONObjectHook = Callable[[Dict[str, Any]], Any]


class ConnectionState(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


class KinopioCodec(Protocol):
    def encode(self, data: Any) -> bytes:
        ...

    def decode(self, payload: bytes) -> Any:
        ...


@dataclass(frozen=True)
class _SubscriptionSpec:
    callback: VariableCallback
    queue: str | None
    max_messages: int | None


@dataclass(frozen=True)
class _ServiceSpec:
    handler: ServiceHandler
    queue: str


class SubscriptionHandle:
    def __init__(
        self,
        *,
        subject: str,
        queue: str | None = None,
        max_messages: int | None = None,
        on_unsubscribe: Optional[Callable[["SubscriptionHandle"], None]] = None,
    ) -> None:
        self.subject = subject
        self.queue = queue
        self.max_messages = max_messages
        self._subscription: NATSSubscription | None = None
        self._on_unsubscribe = on_unsubscribe
        self._closed = False

    @property
    def active(self) -> bool:
        return not self._closed and self._subscription is not None

    def bind(self, subscription: NATSSubscription) -> None:
        self._subscription = subscription
        self._closed = False

    async def unsubscribe(self) -> None:
        if self._closed:
            return

        subscription = self._subscription
        self._subscription = None
        self._closed = True

        if subscription is not None:
            try:
                await subscription.unsubscribe()
            except Exception as exc:
                logging.warning(
                    f"[SubscriptionHandle] unsubscribe failed for subject {self.subject}",
                    exc_info=exc,
                )

        if self._on_unsubscribe is not None:
            self._on_unsubscribe(self)


class KinopioHub:
    def __init__(
        self,
        *,
        servers: Optional[Sequence[str]] = None,
        debug: bool = False,
        no_echo: bool = False,
        no_randomize: bool = True,
        max_reconnect_attempts: int = -1,
        wait_on_first_connect: bool = True,
        reconnect_timeout: float = 5.0,
        reconnect_time_wait: float = 0.5,
        ping_interval: int = 3,
        max_ping_out: int = 3,
        timeout: float = 3.0,
        health_report: float = 5.0,
        auto_retry: bool = True,
        retry_delay: float = 1.0,
        retry_backoff_factor: float = 1.5,
        max_retry_delay: float = 30.0,
        codec: KinopioCodec | None = None,
        json_default: JSONDefault | None = None,
        json_object_hook: JSONObjectHook | None = None,
        tls: Any | None = None,
        tls_hostname: str | None = None,
        ws_connection_headers: Mapping[str, Sequence[str]] | None = None,
        name: str | None = None,
    ) -> None:
        self._servers = list(servers or ["wss://demo.nats.io:8443", "wss://demo.nats.io:4443"])
        self._debug = debug
        self._no_echo = no_echo
        self._no_randomize = no_randomize
        self._max_reconnect_attempts = max_reconnect_attempts
        self._wait_on_first_connect = wait_on_first_connect
        self._reconnect_timeout = reconnect_timeout
        self._reconnect_time_wait = reconnect_time_wait
        self._ping_interval = ping_interval
        self._max_ping_out = max_ping_out
        self._timeout = timeout
        self._health_report = health_report
        self._auto_retry = auto_retry
        self._retry_delay = retry_delay
        self._retry_backoff_factor = retry_backoff_factor
        self._max_retry_delay = max_retry_delay
        self._codec = codec
        self._json_default = json_default
        self._json_object_hook = json_object_hook
        self._tls = tls
        self._tls_hostname = tls_hostname
        self._ws_connection_headers = (
            {key: list(values) for key, values in ws_connection_headers.items()}
            if ws_connection_headers
            else None
        )
        self._name = name

        self._logger = logging.getLogger("kinopio_hub")
        if self._debug:
            self._logger.setLevel(logging.DEBUG)

        self._state = ConnectionState.DISCONNECTED
        self._scopes: dict[str, Scope] = {}
        self._listeners: set[StateListener] = set()
        self._state_condition = asyncio.Condition()
        self._connect_task: asyncio.Task[None] | None = None
        self._health_task: asyncio.Task[None] | None = None
        self._closing = False
        self._reconnecting = asyncio.Lock()
        self._nc: NATSClient | None = None
        self._nc_lock = asyncio.Lock()

        self._schedule_auto_connect()

    def __getattr__(self, name: str) -> Scope:
        if name.startswith("_"):
            raise AttributeError(name)
        return self.get_scope(name)

    async def __aenter__(self) -> "KinopioHub":
        await self.wait_connected()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def is_connected(self) -> bool:
        return (
            self._state == ConnectionState.CONNECTED
            and self._nc is not None
            and self._nc.is_connected
        )

    @property
    def servers(self) -> tuple[str, ...]:
        return tuple(self._servers)

    def on_state_change(self, listener: StateListener) -> Callable[[], None]:
        self._listeners.add(listener)
        return lambda: self.off_state_change(listener)

    def off_state_change(self, listener: StateListener) -> None:
        self._listeners.discard(listener)

    def get_scope(self, name: str) -> "Scope":
        scope = self._scopes.get(name)
        if scope is None:
            scope = Scope(self, name)
            self._scopes[name] = scope
            self._log(logging.DEBUG, "new scope created", scope=name)
        return scope

    async def wait_connected(self, timeout: float | None = 10.0) -> None:
        await self._ensure_connection_started()
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout

        while True:
            if self.is_connected:
                return
            if self._state == ConnectionState.ERROR:
                raise RuntimeError("Connection failed")

            async with self._state_condition:
                if self.is_connected:
                    return
                if self._state == ConnectionState.ERROR:
                    raise RuntimeError("Connection failed")

                if deadline is None:
                    await self._state_condition.wait()
                else:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise TimeoutError(f"Connection timeout after {timeout} seconds")
                    await asyncio.wait_for(self._state_condition.wait(), timeout=remaining)

    async def reconnect(self) -> None:
        async with self._reconnecting:
            self._log(logging.INFO, "manual reconnect requested")
            await self._close_connection_only()
            await self._set_state(ConnectionState.DISCONNECTED)
            await self._connect_once()
            await self._rebind_all_scopes()

    async def request(self, subject: str, data: Any, *, timeout: float | None = None) -> Any:
        await self.wait_connected()
        async with self._nc_lock:
            nc = self._nc
            if nc is None:
                raise RuntimeError("NATS connection is not available")

            response = await nc.request(
                subject,
                self.serialize_data(data),
                timeout=timeout if timeout is not None else self._timeout,
            )
            return self.deserialize_data(response.data)

    def serialize_data(self, data: Any) -> bytes:
        if data is None:
            return b""
        if isinstance(data, bytes):
            return data
        if isinstance(data, bytearray):
            return bytes(data)
        if isinstance(data, memoryview):
            return data.tobytes()
        if isinstance(data, str):
            return data.encode("utf-8")

        if self._codec is not None:
            try:
                return self._codec.encode(data)
            except Exception as exc:
                self._log(
                    logging.WARNING,
                    "custom codec encode failed; falling back to json",
                    error=exc,
                )

        try:
            return json.dumps(data, default=self._json_default).encode("utf-8")
        except Exception as exc:
            self._log(
                logging.WARNING,
                "json serialization failed; falling back to string",
                error=exc,
            )
            return str(data).encode("utf-8")

    def deserialize_data(self, payload: bytes) -> Any:
        if not payload:
            return None

        if self._codec is not None:
            try:
                return self._codec.decode(payload)
            except Exception as exc:
                self._log(
                    logging.WARNING,
                    "custom codec decode failed; falling back to text/json",
                    error=exc,
                )

        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            return payload

        try:
            return json.loads(text, object_hook=self._json_object_hook)
        except json.JSONDecodeError:
            return text

    async def aclose(self) -> None:
        if self._closing:
            return

        self._closing = True
        connect_task = self._connect_task
        if connect_task is not None and not connect_task.done():
            connect_task.cancel()
            try:
                await connect_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self._log(
                    logging.WARNING,
                    "connect task cleanup error after cancel",
                    error=exc,
                )
            finally:
                # Ensure connection is always cleaned up, even if cancel failed
                await self._close_connection_only()

        for scope in list(self._scopes.values()):
            await scope.aclose()
        self._scopes.clear()

        if self._health_task is not None:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self._log(
                    logging.WARNING,
                    "health task cleanup error after cancel",
                    error=exc,
                )
            finally:
                self._health_task = None

        await self._close_connection_only()
        await self._set_state(ConnectionState.DISCONNECTED)

    async def _ensure_connection_started(self) -> None:
        if self.is_connected:
            return

        if self._connect_task is None or self._connect_task.done():
            self._connect_task = asyncio.create_task(self._connect_once(), name="kinopio.connect")
        await asyncio.shield(self._connect_task)

    def _schedule_auto_connect(self) -> None:
        if not self._wait_on_first_connect:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._connect_task = loop.create_task(self._connect_once(), name="kinopio.connect")

    async def _connect_once(self) -> None:
        if self._closing:
            return

        attempt = 0
        delay = self._retry_delay

        while not self._closing:
            attempt += 1
            await self._set_state(ConnectionState.CONNECTING)
            self._log(logging.INFO, "connecting to nats", attempt=attempt, servers=self._servers)

            try:
                nc = await nats.connect(
                    self._servers,
                    error_cb=self._error_cb,
                    disconnected_cb=self._disconnected_cb,
                    closed_cb=self._closed_cb,
                    reconnected_cb=self._reconnected_cb,
                    allow_reconnect=self._auto_retry,
                    connect_timeout=self._reconnect_timeout,
                    reconnect_time_wait=self._reconnect_time_wait,
                    max_reconnect_attempts=self._max_reconnect_attempts,
                    ping_interval=self._ping_interval,
                    max_outstanding_pings=self._max_ping_out,
                    dont_randomize=self._no_randomize,
                    no_echo=self._no_echo,
                    tls=self._tls,
                    tls_hostname=self._tls_hostname,
                    ws_connection_headers=self._ws_connection_headers,
                    name=self._name,
                )
                await nc.flush(timeout=max(1, int(self._timeout)))
                self._nc = nc
                await self._set_state(ConnectionState.CONNECTED)
                self._start_health_task()
                return
            except Exception as exc:
                self._nc = None
                self._log(logging.ERROR, "connection attempt failed", attempt=attempt, error=exc)
                if not self._auto_retry:
                    await self._set_state(ConnectionState.ERROR)
                    raise

                delay = self._retry_delay if attempt == 1 else min(
                    delay * self._retry_backoff_factor, self._max_retry_delay
                )
                jitter = random.uniform(0, delay)
                await asyncio.sleep(jitter)

        await self._set_state(ConnectionState.DISCONNECTED)

    async def _rebind_all_scopes(self) -> None:
        for scope in self._scopes.values():
            await scope._rebind()

    async def _close_connection_only(self) -> None:
        async with self._nc_lock:
            nc = self._nc
            self._nc = None
            if nc is not None:
                try:
                    if nc.is_connected:
                        await nc.drain()
                    else:
                        await nc.close()
                except Exception as exc:
                    self._log(
                        logging.WARNING,
                        "connection close error",
                        error=exc,
                    )
                    try:
                        await nc.close()
                    except Exception as exc2:
                        self._log(
                            logging.WARNING,
                            "fallback connection close error",
                            error=exc2,
                        )

    def _start_health_task(self) -> None:
        if self._health_report <= 0:
            return
        if self._health_task is not None and not self._health_task.done():
            return
        self._health_task = asyncio.create_task(self._health_reporter(), name="kinopio.health")

    async def _health_reporter(self) -> None:
        try:
            while not self._closing:
                await asyncio.sleep(self._health_report)
                self._log(
                    logging.DEBUG,
                    "health report",
                    state=self._state.value,
                    scopes=len(self._scopes),
                    connected=self.is_connected,
                )
        except asyncio.CancelledError:
            raise

    async def _set_state(self, state: ConnectionState) -> None:
        if self._state == state:
            return
        self._state = state
        self._log(logging.INFO, "state changed", state=state.value)
        async with self._state_condition:
            self._state_condition.notify_all()
        for listener in list(self._listeners):
            try:
                listener(state)
            except Exception as exc:
                self._log(logging.WARNING, "state listener failed", error=exc)

    async def _error_cb(self, exc: Exception) -> None:
        self._log(logging.ERROR, "nats client error", error=exc)

    async def _disconnected_cb(self) -> None:
        await self._set_state(ConnectionState.DISCONNECTED)

    async def _reconnected_cb(self) -> None:
        await self._set_state(ConnectionState.CONNECTED)

    async def _closed_cb(self) -> None:
        if not self._closing:
            await self._set_state(ConnectionState.DISCONNECTED)

    def _log(self, level: int, message: str, **fields: Any) -> None:
        if level == logging.DEBUG and not self._debug:
            return
        details = " ".join(f"{key}={value!r}" for key, value in fields.items())
        line = f"[{time():.3f}] [KinopioHub] {message}"
        if details:
            line = f"{line} {details}"
        self._logger.log(level, line)


class Scope:
    def __init__(self, hub: KinopioHub, name: str) -> None:
        self._hub = hub
        self.name = name
        self._variables: dict[str, Variable] = {}

    def __getattr__(self, name: str) -> "Variable":
        if name.startswith("_"):
            raise AttributeError(name)
        return self.get_variable(name)

    def get_variable(self, name: str) -> "Variable":
        variable = self._variables.get(name)
        if variable is None:
            variable = Variable(self._hub, self.name, name)
            self._variables[name] = variable
        return variable

    async def aclose(self) -> None:
        for variable in list(self._variables.values()):
            await variable.aclose()
        self._variables.clear()

    async def _rebind(self) -> None:
        for variable in self._variables.values():
            await variable._rebind()


class Variable:
    def __init__(self, hub: KinopioHub, scope_name: str, name: str) -> None:
        self._hub = hub
        self.scope_name = scope_name
        self.name = name
        self.subject = f"{scope_name}.{name}"
        self._latest_value: Any = None
        self._last_published_message: bytes | None = None
        self._subscriptions: dict[SubscriptionHandle, _SubscriptionSpec] = {}
        self._service_spec: _ServiceSpec | None = None
        self._service_handle: SubscriptionHandle | None = None
        self._tracker_handle: SubscriptionHandle | None = None
        self._tracker_task: asyncio.Task[None] | None = None
        self._closed = False
        self._start_tracker_task()

    @property
    def value(self) -> Any:
        return self._latest_value

    def _start_tracker_task(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._tracker_task = loop.create_task(
            self._ensure_tracker(), name=f"kinopio.track.{self.subject}"
        )

    async def publish(
        self,
        data: Any,
        *,
        headers: Mapping[str, str] | None = None,
        reply: str | None = None,
    ) -> None:
        await self._hub.wait_connected()
        async with self._hub._nc_lock:
            nc = self._hub._nc
            if nc is None:
                raise RuntimeError("NATS connection is not available")

            payload = self._hub.serialize_data(data)
            if payload == self._last_published_message:
                self._hub._log(logging.DEBUG, "duplicate publish skipped", subject=self.subject)
                return

            await nc.publish(
                self.subject,
                payload,
                reply=reply or "",
                headers=dict(headers) if headers else None,
            )
            self._last_published_message = payload
            self._latest_value = data

    async def subscribe(
        self,
        callback: VariableCallback,
        *,
        queue: str | None = None,
        max_messages: int | None = None,
    ) -> SubscriptionHandle:
        handle = SubscriptionHandle(
            subject=self.subject,
            queue=queue,
            max_messages=max_messages,
            on_unsubscribe=self._drop_subscription,
        )
        spec = _SubscriptionSpec(callback=callback, queue=queue, max_messages=max_messages)
        self._subscriptions[handle] = spec
        await self._bind_subscription(handle, spec)
        return handle

    async def request(self, data: Any, *, timeout: float | None = None) -> Any:
        return await self._hub.request(self.subject, data, timeout=timeout)

    async def serve(
        self,
        handler: ServiceHandler,
        *,
        queue: str | None = None,
    ) -> SubscriptionHandle:
        if self._service_handle is not None:
            await self._service_handle.unsubscribe()

        actual_queue = queue or f"{self.subject}.service"
        handle = SubscriptionHandle(subject=self.subject, queue=actual_queue)
        self._service_spec = _ServiceSpec(handler=handler, queue=actual_queue)
        self._service_handle = handle
        await self._bind_service()
        return handle

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._tracker_task is not None and not self._tracker_task.done():
            self._tracker_task.cancel()
            try:
                await self._tracker_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self._hub._log(
                    logging.WARNING,
                    "tracker task cleanup error after cancel",
                    subject=self.subject,
                    error=exc,
                )
            finally:
                self._tracker_task = None

        if self._tracker_handle is not None:
            await self._tracker_handle.unsubscribe()
            self._tracker_handle = None

        for handle in list(self._subscriptions):
            await handle.unsubscribe()
        self._subscriptions.clear()

        if self._service_handle is not None:
            await self._service_handle.unsubscribe()
            self._service_handle = None
            self._service_spec = None

        self._last_published_message = None
        self._latest_value = None

    async def _ensure_tracker(self) -> None:
        if self._closed:
            return
        if self._tracker_handle is not None and self._tracker_handle.active:
            return

        handle = SubscriptionHandle(subject=self.subject)
        self._tracker_handle = handle
        await self._bind_tracker(handle)

    async def _bind_tracker(self, handle: SubscriptionHandle) -> None:
        await self._hub.wait_connected()
        async with self._hub._nc_lock:
            nc = self._hub._nc
            if nc is None:
                raise RuntimeError("NATS connection is not available")

            async def tracker(msg: Msg) -> None:
                try:
                    self._latest_value = self._hub.deserialize_data(msg.data)
                except Exception as exc:
                    self._hub._log(
                        logging.WARNING,
                        "tracker deserialization error",
                        subject=self.subject,
                        error=exc,
                    )

            subscription = await nc.subscribe(self.subject, cb=tracker)
            await nc.flush(timeout=max(1, int(self._hub._timeout)))
            handle.bind(subscription)

    async def _bind_subscription(self, handle: SubscriptionHandle, spec: _SubscriptionSpec) -> None:
        await self._hub.wait_connected()
        async with self._hub._nc_lock:
            nc = self._hub._nc
            if nc is None:
                raise RuntimeError("NATS connection is not available")

            async def wrapped(msg: Msg) -> None:
                try:
                    data = self._hub.deserialize_data(msg.data)
                    self._latest_value = data
                    result = spec.callback(data, msg)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    self._hub._log(
                        logging.WARNING,
                        "subscription callback error",
                        subject=self.subject,
                        error=exc,
                    )

            subscription = await nc.subscribe(
                self.subject,
                queue=spec.queue or "",
                cb=wrapped,
                max_msgs=spec.max_messages or 0,
            )
            await nc.flush(timeout=max(1, int(self._hub._timeout)))
            handle.bind(subscription)

    async def _bind_service(self) -> None:
        await self._hub.wait_connected()
        async with self._hub._nc_lock:
            nc = self._hub._nc
            if nc is None or self._service_spec is None or self._service_handle is None:
                raise RuntimeError("NATS connection is not available")
            spec = self._service_spec

            async def wrapped(msg: Msg) -> None:
                request_data = self._hub.deserialize_data(msg.data)
                try:
                    response_data = spec.handler(request_data, msg)
                    if asyncio.iscoroutine(response_data):
                        response_data = await response_data
                except Exception as exc:
                    response_data = {"error": True, "message": str(exc)}

                if msg.reply:
                    await nc.publish(msg.reply, self._hub.serialize_data(response_data))

            subscription = await nc.subscribe(self.subject, queue=spec.queue, cb=wrapped)
            await nc.flush(timeout=max(1, int(self._hub._timeout)))
            self._service_handle.bind(subscription)

    async def _rebind(self) -> None:
        if self._closed:
            return

        await self._ensure_tracker()

        for handle, spec in list(self._subscriptions.items()):
            await self._bind_subscription(handle, spec)

        if self._service_spec is not None and self._service_handle is not None:
            await self._bind_service()

    def _drop_subscription(self, handle: SubscriptionHandle) -> None:
        self._subscriptions.pop(handle, None)
        if handle is self._service_handle:
            self._service_handle = None
            self._service_spec = None
