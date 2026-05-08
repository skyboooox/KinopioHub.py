from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass, replace
from enum import Enum
from time import perf_counter, time
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    cast,
)
from urllib.parse import urlparse

from nats import errors as nats_errors
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription as NATSSubscription

KINOPIO_STATE_EVENT = "kinopio.state"

StateListener = Callable[["ConnectionState"], None]
VariableCallback = Callable[[Any, Msg], Awaitable[None] | None]
ServiceHandler = Callable[[Any, Msg], Awaitable[Any] | Any]
JSONDefault = Callable[[Any], Any]
JSONObjectHook = Callable[[Dict[str, Any]], Any]
ServerSelectionMode = Literal["ordered", "random", "latency"]
_VALID_SERVER_SELECTION_MODES = frozenset({"ordered", "random", "latency"})


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


@dataclass(frozen=True)
class _ConnectionPlan:
    raw_servers: tuple[str, ...]
    server_selection_mode: ServerSelectionMode
    candidate_servers: tuple[str, ...]
    active_server: str | None
    background_probe_enabled: bool
    probe_results: tuple["_ServerProbeResult", ...] = ()


@dataclass(frozen=True)
class _ServerProbeResult:
    server: str
    original_index: int
    available: bool
    round_trip_ms: float | None
    error: str | None = None


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
            except nats_errors.ConnectionClosedError:
                pass
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
        server_selection_mode: ServerSelectionMode | None = None,
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
        tls_handshake_first: bool = False,
        ws_connection_headers: Mapping[str, Sequence[str]] | None = None,
        name: str | None = None,
    ) -> None:
        self._servers = tuple(servers or ["wss://demo.nats.io:8443", "wss://demo.nats.io:4443"])
        self._debug = debug
        self._no_echo = no_echo
        self._no_randomize = no_randomize
        self._server_selection_mode = self._resolve_server_selection_mode(
            server_selection_mode, no_randomize
        )
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
        self._tls_handshake_first = tls_handshake_first
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
        self._latency_monitor_task: asyncio.Task[None] | None = None
        self._closing = False
        self._reconnecting = asyncio.Lock()
        self._hot_switch_lock = asyncio.Lock()
        self._nc: NATSClient | None = None
        self._nc_lock = asyncio.Lock()
        self._latency_probe_interval_seconds = 600.0
        self._latency_switch_threshold_ms = 30.0
        self._connection_plan = self._build_connection_plan()

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
            async with self._hot_switch_lock:
                self._log(logging.INFO, "manual reconnect requested")
                await self._stop_latency_monitor_task()
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
        await self._stop_latency_monitor_task()
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

        plan = await self._start_connection_cycle()
        attempt = 0
        delay = self._retry_delay

        while not self._closing:
            attempt += 1
            await self._set_state(ConnectionState.CONNECTING)
            self._log(
                logging.INFO,
                "connecting to nats",
                attempt=attempt,
                servers=plan.candidate_servers,
                server_selection_mode=plan.server_selection_mode,
            )

            try:
                nc, connected_candidates = await self._open_connection_for_candidates(
                    plan.candidate_servers
                )
                if connected_candidates != plan.candidate_servers:
                    plan = replace(plan, candidate_servers=connected_candidates)
                self._nc = nc
                self._connection_plan = replace(
                    plan,
                    active_server=self._resolve_active_server(nc, plan.candidate_servers),
                )
                await self._set_state(ConnectionState.CONNECTED)
                self._start_health_task()
                self._start_latency_monitor_task()
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

    async def _open_connection_for_candidates(
        self,
        candidate_servers: Sequence[str],
    ) -> tuple[NATSClient, tuple[str, ...]]:
        resolved_candidates = tuple(candidate_servers)
        try:
            if self._should_recover_websocket_candidates(resolved_candidates):
                timeout = self._websocket_multi_server_attempt_timeout(resolved_candidates)
                return (
                    await asyncio.wait_for(self._open_connection(resolved_candidates), timeout),
                    resolved_candidates,
                )
            return await self._open_connection(resolved_candidates), resolved_candidates
        except Exception:
            recovered_candidates = await self._recover_websocket_candidate_order(
                resolved_candidates
            )
            if recovered_candidates is None:
                raise

        self._log(
            logging.INFO,
            "recovered websocket candidate order after failed initial connect",
            candidate_servers=recovered_candidates,
        )
        return await self._open_connection(recovered_candidates), recovered_candidates

    async def _rebind_all_scopes(self, connection: NATSClient | None = None) -> None:
        for scope in self._scopes.values():
            await scope._rebind(connection=connection)

    async def _close_connection_only(self) -> None:
        async with self._nc_lock:
            nc = self._nc
            self._nc = None
            self._set_active_server(None)
        if nc is not None:
            await self._close_specific_connection(nc)

    async def _recover_websocket_candidate_order(
        self,
        candidate_servers: Sequence[str],
    ) -> tuple[str, ...] | None:
        if len(candidate_servers) <= 1:
            return None
        if not self._should_recover_websocket_candidates(candidate_servers):
            return None

        for index, server in enumerate(candidate_servers):
            try:
                probe_nc = await asyncio.wait_for(
                    self._open_connection((server,)),
                    timeout=self._websocket_single_server_attempt_timeout(),
                )
            except Exception as exc:
                self._log(
                    logging.DEBUG,
                    "websocket recovery probe failed",
                    server=server,
                    error=exc,
                )
                continue

            await self._close_specific_connection(probe_nc)
            return tuple(candidate_servers[index:]) + tuple(candidate_servers[:index])

        return None

    def _start_health_task(self) -> None:
        if self._health_report <= 0:
            return
        if self._health_task is not None and not self._health_task.done():
            return
        self._health_task = asyncio.create_task(self._health_reporter(), name="kinopio.health")

    def _start_latency_monitor_task(self) -> None:
        if not self._connection_plan.background_probe_enabled:
            return
        if self._latency_monitor_task is not None and not self._latency_monitor_task.done():
            return
        self._latency_monitor_task = asyncio.create_task(
            self._latency_monitor(),
            name="kinopio.latency",
        )

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

    async def _latency_monitor(self) -> None:
        try:
            while not self._closing:
                await asyncio.sleep(self._latency_probe_interval_seconds)
                try:
                    await self._maybe_hot_switch()
                except Exception as exc:
                    self._log(logging.WARNING, "latency monitor iteration failed", error=exc)
        except asyncio.CancelledError:
            raise

    async def _stop_latency_monitor_task(self) -> None:
        task = self._latency_monitor_task
        self._latency_monitor_task = None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log(
                logging.WARNING,
                "latency monitor cleanup error after cancel",
                error=exc,
            )

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

    def _log(self, level: int, message: str, **fields: Any) -> None:
        if level == logging.DEBUG and not self._debug:
            return
        details = " ".join(f"{key}={value!r}" for key, value in fields.items())
        line = f"[{time():.3f}] [KinopioHub] {message}"
        if details:
            line = f"{line} {details}"
        self._logger.log(level, line)

    def _resolve_server_selection_mode(
        self,
        server_selection_mode: str | None,
        no_randomize: bool,
    ) -> ServerSelectionMode:
        if server_selection_mode is None:
            return "ordered" if no_randomize else "random"

        if server_selection_mode not in _VALID_SERVER_SELECTION_MODES:
            raise ValueError(
                "server_selection_mode must be one of 'ordered', 'random', or 'latency'"
            )
        return cast(ServerSelectionMode, server_selection_mode)

    def _build_candidate_servers(self, raw_servers: Sequence[str]) -> tuple[str, ...]:
        if len(raw_servers) <= 1:
            return tuple(raw_servers)

        if self._server_selection_mode == "random":
            shuffled_servers = list(raw_servers)
            random.shuffle(shuffled_servers)
            return tuple(shuffled_servers)

        # Phase 1 only establishes the explicit strategy layer. Latency probing
        # keeps caller-provided order until phase-2 probe results are applied.
        return tuple(raw_servers)

    def _build_connection_plan(
        self,
        *,
        candidate_servers: Sequence[str] | None = None,
        active_server: str | None = None,
        probe_results: Sequence[_ServerProbeResult] = (),
    ) -> _ConnectionPlan:
        raw_servers = tuple(self._servers)
        resolved_candidates = tuple(
            candidate_servers or self._build_candidate_servers(raw_servers)
        )
        return _ConnectionPlan(
            raw_servers=raw_servers,
            server_selection_mode=self._server_selection_mode,
            candidate_servers=resolved_candidates,
            active_server=active_server,
            background_probe_enabled=self._should_monitor_latency(raw_servers),
            probe_results=tuple(probe_results),
        )

    async def _start_connection_cycle(self) -> _ConnectionPlan:
        raw_servers = tuple(self._servers)
        probe_results: tuple[_ServerProbeResult, ...] = ()
        candidate_servers = self._build_candidate_servers(raw_servers)

        if self._should_probe_candidate_servers(raw_servers):
            probe_results = await self._probe_candidate_servers(raw_servers)
            candidate_servers = self._order_servers_by_probe_results(raw_servers, probe_results)

        self._connection_plan = self._build_connection_plan(
            candidate_servers=candidate_servers,
            probe_results=probe_results,
        )
        return self._connection_plan

    def _set_active_server(self, active_server: str | None) -> None:
        self._connection_plan = replace(self._connection_plan, active_server=active_server)

    def _should_probe_candidate_servers(self, raw_servers: Sequence[str]) -> bool:
        return self._server_selection_mode == "latency" and len(raw_servers) > 1

    def _should_monitor_latency(self, raw_servers: Sequence[str]) -> bool:
        return self._server_selection_mode == "latency" and len(raw_servers) > 1

    async def _maybe_hot_switch(self) -> None:
        if (
            self._closing
            or not self.is_connected
            or not self._connection_plan.background_probe_enabled
        ):
            return

        async with self._hot_switch_lock:
            if self._closing or not self.is_connected:
                return

            current_nc = self._nc
            current_server = self._connection_plan.active_server
            raw_servers = self._connection_plan.raw_servers
            if current_nc is None or current_server is None:
                return

            probe_results = await self._probe_candidate_servers(raw_servers)
            candidate_servers = self._order_servers_by_probe_results(raw_servers, probe_results)
            current_result = self._find_probe_result(current_server, probe_results)
            best_result = self._find_best_probe_result(probe_results)

            if best_result is None:
                self._connection_plan = self._build_connection_plan(
                    candidate_servers=candidate_servers,
                    active_server=current_server,
                    probe_results=probe_results,
                )
                return

            if (
                current_result is not None
                and current_result.available
                and current_result.round_trip_ms is not None
            ):
                current_round_trip = current_result.round_trip_ms
            else:
                current_round_trip = float("inf")
            best_round_trip = (
                best_result.round_trip_ms
                if best_result.round_trip_ms is not None
                else float("inf")
            )
            improvement_ms = current_round_trip - best_round_trip

            self._connection_plan = self._build_connection_plan(
                candidate_servers=candidate_servers,
                active_server=current_server,
                probe_results=probe_results,
            )

            if best_result.server == current_server:
                return
            if improvement_ms < self._latency_switch_threshold_ms:
                return

            await self._hot_switch_to_connection(
                current_nc=current_nc,
                candidate_servers=candidate_servers,
                probe_results=probe_results,
            )

    async def _probe_candidate_servers(
        self,
        raw_servers: Sequence[str],
    ) -> tuple[_ServerProbeResult, ...]:
        probe_tasks = [
            self._probe_server(server, index)
            for index, server in enumerate(raw_servers)
        ]
        return tuple(await asyncio.gather(*probe_tasks))

    async def _probe_server(self, server: str, original_index: int) -> _ServerProbeResult:
        probe_client = NATSClient()

        try:
            await asyncio.wait_for(
                probe_client.connect(
                    [server],
                    error_cb=self._probe_error_cb,
                    allow_reconnect=False,
                    connect_timeout=self._connect_timeout_seconds(),
                    reconnect_time_wait=0,
                    max_reconnect_attempts=0,
                    ping_interval=self._ping_interval,
                    max_outstanding_pings=self._max_ping_out,
                    dont_randomize=True,
                    no_echo=self._no_echo,
                    tls=self._tls,
                    tls_hostname=self._tls_hostname,
                    tls_handshake_first=self._tls_handshake_first,
                    ws_connection_headers=self._ws_connection_headers,
                    name=self._name,
                ),
                timeout=self._connect_timeout_seconds(),
            )
            started = perf_counter()
            await probe_client.flush(timeout=self._flush_timeout_seconds())
            round_trip_ms = (perf_counter() - started) * 1000.0
            return _ServerProbeResult(
                server=server,
                original_index=original_index,
                available=True,
                round_trip_ms=round_trip_ms,
            )
        except Exception as exc:
            self._log(logging.DEBUG, "server probe failed", server=server, error=exc)
            return _ServerProbeResult(
                server=server,
                original_index=original_index,
                available=False,
                round_trip_ms=None,
                error=self._format_probe_error(exc),
            )
        finally:
            await asyncio.shield(
                self._force_close_client(
                    probe_client,
                    level=logging.DEBUG,
                    message="server probe close failed",
                    server=server,
                )
            )

    def _find_probe_result(
        self,
        server: str,
        probe_results: Sequence[_ServerProbeResult],
    ) -> _ServerProbeResult | None:
        for result in probe_results:
            if result.server == server:
                return result
        return None

    def _find_best_probe_result(
        self,
        probe_results: Sequence[_ServerProbeResult],
    ) -> _ServerProbeResult | None:
        for result in sorted(
            probe_results,
            key=lambda item: (
                0 if item.available else 1,
                item.round_trip_ms if item.round_trip_ms is not None else float("inf"),
                item.original_index,
            ),
        ):
            if result.available and result.round_trip_ms is not None:
                return result
        return None

    def _order_servers_by_probe_results(
        self,
        raw_servers: Sequence[str],
        probe_results: Sequence[_ServerProbeResult],
    ) -> tuple[str, ...]:
        healthy_results = [result for result in probe_results if result.available]
        if not healthy_results:
            return tuple(raw_servers)

        ordered_results = sorted(
            probe_results,
            key=lambda result: (
                0 if result.available else 1,
                result.round_trip_ms if result.round_trip_ms is not None else float("inf"),
                result.original_index,
            ),
        )
        return tuple(result.server for result in ordered_results)

    def _connect_timeout_seconds(self) -> int:
        return max(1, int(self._reconnect_timeout))

    def _reconnect_wait_seconds(self) -> int:
        return max(0, int(self._reconnect_time_wait))

    def _flush_timeout_seconds(self) -> int:
        return max(1, int(self._timeout))

    def _format_probe_error(self, exc: Exception) -> str:
        return f"{type(exc).__name__}: {exc}"

    async def _probe_error_cb(self, exc: Exception) -> None:
        self._log(logging.DEBUG, "server probe client error", error=exc)

    async def _open_connection(self, servers: Sequence[str]) -> NATSClient:
        client = NATSClient()
        callbacks = self._make_connection_callbacks(client)
        try:
            await client.connect(
                list(servers),
                error_cb=callbacks["error_cb"],
                disconnected_cb=callbacks["disconnected_cb"],
                closed_cb=callbacks["closed_cb"],
                reconnected_cb=callbacks["reconnected_cb"],
                allow_reconnect=self._auto_retry,
                connect_timeout=self._connect_timeout_seconds(),
                reconnect_time_wait=self._reconnect_wait_seconds(),
                max_reconnect_attempts=self._max_reconnect_attempts,
                ping_interval=self._ping_interval,
                max_outstanding_pings=self._max_ping_out,
                dont_randomize=True,
                no_echo=self._no_echo,
                tls=self._tls,
                tls_hostname=self._tls_hostname,
                tls_handshake_first=self._tls_handshake_first,
                ws_connection_headers=self._ws_connection_headers,
                name=self._name,
            )
            await client.flush(timeout=self._flush_timeout_seconds())
            return client
        except BaseException:
            await asyncio.shield(
                self._force_close_client(
                    client,
                    level=logging.DEBUG,
                    message="connection cleanup after failed open failed",
                    servers=tuple(servers),
                )
            )
            raise

    def _make_connection_callbacks(
        self,
        client: NATSClient,
    ) -> dict[str, Callable[..., Any]]:
        async def error_cb(exc: Exception) -> None:
            await self._handle_client_error(client, exc)

        async def disconnected_cb() -> None:
            await self._handle_client_disconnected(client)

        async def closed_cb() -> None:
            await self._handle_client_closed(client)

        async def reconnected_cb() -> None:
            await self._handle_client_reconnected(client)

        return {
            "error_cb": error_cb,
            "disconnected_cb": disconnected_cb,
            "closed_cb": closed_cb,
            "reconnected_cb": reconnected_cb,
        }

    async def _handle_client_error(self, client: NATSClient, exc: Exception) -> None:
        if self._is_active_client(client):
            self._log(logging.ERROR, "nats client error", error=exc)
            return
        self._log(logging.DEBUG, "inactive nats client error", error=exc)

    async def _handle_client_disconnected(self, client: NATSClient) -> None:
        if not self._is_active_client(client):
            return
        self._set_active_server(None)
        await self._set_state(ConnectionState.DISCONNECTED)

    async def _handle_client_reconnected(self, client: NATSClient) -> None:
        if not self._is_active_client(client):
            return
        self._set_active_server(
            self._resolve_active_server(client, self._connection_plan.candidate_servers)
        )
        await self._set_state(ConnectionState.CONNECTED)

    async def _handle_client_closed(self, client: NATSClient) -> None:
        if not self._is_active_client(client):
            return
        self._set_active_server(None)
        if not self._closing:
            await self._set_state(ConnectionState.DISCONNECTED)

    def _is_active_client(self, client: NATSClient) -> bool:
        return self._nc is client

    async def _hot_switch_to_connection(
        self,
        *,
        current_nc: NATSClient,
        candidate_servers: Sequence[str],
        probe_results: Sequence[_ServerProbeResult],
    ) -> None:
        self._log(
            logging.INFO,
            "starting hot switch",
            current_server=self._connection_plan.active_server,
            candidate_servers=tuple(candidate_servers),
        )
        candidate_nc = await self._open_connection(candidate_servers)
        try:
            await self._rebind_all_scopes(connection=candidate_nc)
        except Exception:
            await self._close_specific_connection(candidate_nc)
            raise

        async with self._nc_lock:
            if self._closing or self._nc is not current_nc:
                await self._close_specific_connection(candidate_nc)
                return
            self._nc = candidate_nc

        new_active_server = self._resolve_active_server(candidate_nc, candidate_servers)
        self._connection_plan = self._build_connection_plan(
            candidate_servers=candidate_servers,
            active_server=new_active_server,
            probe_results=probe_results,
        )
        await self._close_specific_connection(current_nc)
        self._log(
            logging.INFO,
            "hot switch completed",
            active_server=new_active_server,
        )

    async def _close_specific_connection(self, nc: NATSClient) -> None:
        try:
            if nc.is_connected and not self._is_websocket_connection(nc):
                await nc.drain()
            else:
                await nc.close()
        except Exception as exc:
            self._log(
                logging.WARNING,
                "connection close error",
                error=exc,
            )
            await self._force_close_client(nc, message="fallback connection close error")

    async def _force_close_client(
        self,
        nc: NATSClient,
        *,
        level: int = logging.WARNING,
        message: str,
        **fields: Any,
    ) -> None:
        try:
            if await self._close_half_open_websocket_transport(nc):
                return
            if not nc.is_closed:
                await nc.close()
        except Exception as exc:
            self._log(
                level,
                message,
                error=exc,
                **fields,
            )

    async def _close_half_open_websocket_transport(self, nc: NATSClient) -> bool:
        transport = getattr(nc, "_transport", None)
        if transport is None or type(transport).__name__ != "WebSocketTransport":
            return False

        websocket = getattr(transport, "_ws", None)
        client_session = getattr(transport, "_client", None)
        if websocket is not None or client_session is None:
            return False

        close_task = getattr(transport, "_close_task", None)
        if close_task is not None and hasattr(close_task, "done") and not close_task.done():
            close_task.set_result(None)

        await client_session.close()
        transport._client = None
        return True

    def _is_websocket_connection(self, nc: NATSClient) -> bool:
        transport = getattr(nc, "_transport", None)
        if transport is not None and type(transport).__name__ == "WebSocketTransport":
            return True

        connected_url = getattr(nc, "connected_url", None)
        resolved_url: str | None = None
        if isinstance(connected_url, str):
            resolved_url = connected_url
        else:
            geturl = getattr(connected_url, "geturl", None)
            if callable(geturl):
                value = geturl()
                if isinstance(value, str):
                    resolved_url = value

        if resolved_url is None:
            return False

        return self._is_websocket_server_url(resolved_url)

    def _is_websocket_server_url(self, server: str) -> bool:
        return urlparse(server).scheme in {"ws", "wss"}

    def _should_recover_websocket_candidates(self, candidate_servers: Sequence[str]) -> bool:
        return len(candidate_servers) > 1 and self._is_websocket_server_url(candidate_servers[0])

    def _websocket_multi_server_attempt_timeout(self, candidate_servers: Sequence[str]) -> float:
        return float(self._connect_timeout_seconds() + 1)

    def _websocket_single_server_attempt_timeout(self) -> float:
        return float(self._connect_timeout_seconds() + 1)

    def _resolve_active_server(
        self,
        nc: NATSClient,
        candidate_servers: Sequence[str],
    ) -> str | None:
        connected_url = getattr(nc, "connected_url", None)
        if connected_url is None:
            if len(candidate_servers) == 1:
                return candidate_servers[0]
            return None

        if isinstance(connected_url, str):
            return connected_url

        geturl = getattr(connected_url, "geturl", None)
        if callable(geturl):
            resolved = geturl()
            if isinstance(resolved, str):
                return resolved

        return str(connected_url)


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

    async def _rebind(self, connection: NATSClient | None = None) -> None:
        for variable in self._variables.values():
            await variable._rebind(connection=connection)


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

    async def _bind_tracker(
        self,
        handle: SubscriptionHandle,
        connection: NATSClient | None = None,
    ) -> None:
        nc = await self._resolve_binding_connection(connection)

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
        await nc.flush(timeout=self._hub._flush_timeout_seconds())
        handle.bind(subscription)

    async def _bind_subscription(
        self,
        handle: SubscriptionHandle,
        spec: _SubscriptionSpec,
        connection: NATSClient | None = None,
    ) -> None:
        nc = await self._resolve_binding_connection(connection)

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
        await nc.flush(timeout=self._hub._flush_timeout_seconds())
        handle.bind(subscription)

    async def _bind_service(self, connection: NATSClient | None = None) -> None:
        nc = await self._resolve_binding_connection(connection)
        if self._service_spec is None or self._service_handle is None:
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
        await nc.flush(timeout=self._hub._flush_timeout_seconds())
        self._service_handle.bind(subscription)

    async def _rebind(self, connection: NATSClient | None = None) -> None:
        if self._closed:
            return

        if connection is None:
            await self._ensure_tracker()
        else:
            if self._tracker_handle is None:
                self._tracker_handle = SubscriptionHandle(subject=self.subject)
            await self._bind_tracker(self._tracker_handle, connection=connection)

        for handle, spec in list(self._subscriptions.items()):
            await self._bind_subscription(handle, spec, connection=connection)

        if self._service_spec is not None and self._service_handle is not None:
            await self._bind_service(connection=connection)

    async def _resolve_binding_connection(
        self,
        connection: NATSClient | None = None,
    ) -> NATSClient:
        if connection is not None:
            return connection

        await self._hub.wait_connected()
        async with self._hub._nc_lock:
            nc = self._hub._nc
            if nc is None:
                raise RuntimeError("NATS connection is not available")
            return nc

    def _drop_subscription(self, handle: SubscriptionHandle) -> None:
        self._subscriptions.pop(handle, None)
        if handle is self._service_handle:
            self._service_handle = None
            self._service_spec = None
