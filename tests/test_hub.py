from __future__ import annotations

import asyncio
import json
import socket
import ssl
from dataclasses import dataclass
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest

from kinopio_hub import ConnectionState, KinopioHub
from kinopio_hub._hub import _ServerProbeResult


async def wait_for(predicate: Any, *, timeout: float = 5.0, interval: float = 0.05) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition was not met in time")


async def wait_for_active_server(
    hub: KinopioHub,
    server: str,
    *,
    timeout: float = 5.0,
) -> None:
    await wait_for(
        lambda: hub._connection_plan.active_server == server and hub.is_connected,
        timeout=timeout,
    )


def probe_result(
    server: str,
    original_index: int,
    round_trip_ms: float,
    *,
    available: bool = True,
    error: str | None = None,
) -> _ServerProbeResult:
    return _ServerProbeResult(
        server=server,
        original_index=original_index,
        available=available,
        round_trip_ms=round_trip_ms if available else None,
        error=error,
    )


class FakeNATSConnection:
    def __init__(
        self,
        connected_url: str | None = None,
        *,
        transport_name: str | None = None,
        connect_error: BaseException | None = None,
    ) -> None:
        self.connected_url = connected_url
        self.is_connected = True
        self.is_closed = False
        self.connect_error = connect_error
        self.drain_calls = 0
        self.close_calls = 0
        self.connect_calls = 0
        self._transport = type(transport_name, (), {})() if transport_name is not None else None

    async def connect(self, *_args: Any, **_kwargs: Any) -> None:
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error
        self.is_connected = True

    async def flush(self, timeout: int) -> None:
        return None

    async def drain(self) -> None:
        self.drain_calls += 1
        self.is_connected = False
        self.is_closed = True

    async def close(self) -> None:
        self.close_calls += 1
        self.is_connected = False
        self.is_closed = True


class FakeClientSession:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


def unused_nats_url() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = int(sock.getsockname()[1])
    return f"nats://127.0.0.1:{port}"


def test_default_server_selection_mode_preserves_legacy_order() -> None:
    hub = KinopioHub(wait_on_first_connect=False)

    assert hub._server_selection_mode == "ordered"
    assert hub._connection_plan.raw_servers == hub.servers
    assert hub._connection_plan.candidate_servers == hub.servers


def test_no_randomize_false_maps_to_random_mode() -> None:
    hub = KinopioHub(
        servers=["nats://server-a:4222", "nats://server-b:4222"],
        no_randomize=False,
        wait_on_first_connect=False,
    )

    assert hub._server_selection_mode == "random"


def test_explicit_server_selection_mode_overrides_legacy_flag() -> None:
    hub = KinopioHub(
        servers=["nats://server-a:4222", "nats://server-b:4222"],
        server_selection_mode="ordered",
        no_randomize=False,
        wait_on_first_connect=False,
    )

    assert hub._server_selection_mode == "ordered"
    assert hub._connection_plan.candidate_servers == (
        "nats://server-a:4222",
        "nats://server-b:4222",
    )


def test_latency_mode_enables_background_probe_for_multi_server() -> None:
    hub = KinopioHub(
        servers=["nats://server-a:4222", "nats://server-b:4222"],
        server_selection_mode="latency",
        wait_on_first_connect=False,
    )

    assert hub._connection_plan.server_selection_mode == "latency"
    assert hub._connection_plan.candidate_servers == (
        "nats://server-a:4222",
        "nats://server-b:4222",
    )
    assert hub._connection_plan.background_probe_enabled is True


def test_latency_probe_order_keeps_equal_rtt_servers_in_input_order() -> None:
    hub = KinopioHub(
        servers=[
            "nats://server-a:4222",
            "nats://server-b:4222",
            "nats://server-c:4222",
        ],
        server_selection_mode="latency",
        wait_on_first_connect=False,
    )

    ordered = hub._order_servers_by_probe_results(
        hub.servers,
        (
            _ServerProbeResult(
                server="nats://server-a:4222",
                original_index=0,
                available=True,
                round_trip_ms=10.0,
            ),
            _ServerProbeResult(
                server="nats://server-b:4222",
                original_index=1,
                available=True,
                round_trip_ms=10.0,
            ),
            _ServerProbeResult(
                server="nats://server-c:4222",
                original_index=2,
                available=False,
                round_trip_ms=None,
                error="OSError: unreachable",
            ),
        ),
    )

    assert ordered == (
        "nats://server-a:4222",
        "nats://server-b:4222",
        "nats://server-c:4222",
    )


def test_invalid_server_selection_mode_raises_value_error() -> None:
    with pytest.raises(
        ValueError,
        match="server_selection_mode must be one of 'ordered', 'random', or 'latency'",
    ):
        KinopioHub(
            server_selection_mode=cast(Any, "fastest"),
            wait_on_first_connect=False,
        )


@pytest.mark.asyncio
async def test_random_server_selection_mode_reorders_candidates_before_connect() -> None:
    hub = KinopioHub(
        servers=["nats://server-a:4222", "nats://server-b:4222"],
        server_selection_mode="random",
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )

    fake_nc = FakeNATSConnection("nats://server-b:4222")

    try:
        with (
            patch(
                "kinopio_hub._hub.random.shuffle",
                side_effect=lambda values: values.reverse(),
            ) as shuffle_mock,
            patch.object(
                hub,
                "_open_connection",
                new=AsyncMock(return_value=fake_nc),
            ) as connect_mock,
        ):
            await hub._connect_once()

        assert shuffle_mock.called
        assert connect_mock.await_count == 1
        connect_call = connect_mock.await_args
        assert connect_call is not None
        assert connect_call.args[0] == (
            "nats://server-b:4222",
            "nats://server-a:4222",
        )
        assert hub._connection_plan.candidate_servers == (
            "nats://server-b:4222",
            "nats://server-a:4222",
        )
        assert hub._connection_plan.active_server == "nats://server-b:4222"
    finally:
        await hub.aclose()


@pytest.mark.asyncio
async def test_latency_server_selection_mode_reorders_candidates_before_connect() -> None:
    hub = KinopioHub(
        servers=[
            "nats://server-a:4222",
            "nats://server-b:4222",
            "nats://server-c:4222",
        ],
        server_selection_mode="latency",
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )

    fake_nc = FakeNATSConnection("nats://server-b:4222")
    probe_results = (
        _ServerProbeResult(
            server="nats://server-a:4222",
            original_index=0,
            available=True,
            round_trip_ms=35.0,
        ),
        _ServerProbeResult(
            server="nats://server-b:4222",
            original_index=1,
            available=True,
            round_trip_ms=12.5,
        ),
        _ServerProbeResult(
            server="nats://server-c:4222",
            original_index=2,
            available=False,
            round_trip_ms=None,
            error="OSError: unreachable",
        ),
    )

    try:
        with (
            patch.object(
                hub,
                "_probe_candidate_servers",
                new=AsyncMock(return_value=probe_results),
            ) as probe_mock,
            patch.object(
                hub,
                "_open_connection",
                new=AsyncMock(return_value=fake_nc),
            ) as connect_mock,
        ):
            await hub._connect_once()

        assert probe_mock.await_count == 1
        connect_call = connect_mock.await_args
        assert connect_call is not None
        assert connect_call.args[0] == (
            "nats://server-b:4222",
            "nats://server-a:4222",
            "nats://server-c:4222",
        )
        assert hub._connection_plan.candidate_servers == (
            "nats://server-b:4222",
            "nats://server-a:4222",
            "nats://server-c:4222",
        )
        assert hub._connection_plan.probe_results == probe_results
    finally:
        await hub.aclose()


@pytest.mark.asyncio
async def test_latency_server_selection_mode_falls_back_to_input_order_when_all_probes_fail(
) -> None:
    hub = KinopioHub(
        servers=["nats://server-a:4222", "nats://server-b:4222"],
        server_selection_mode="latency",
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )

    fake_nc = FakeNATSConnection("nats://server-a:4222")
    probe_results = (
        _ServerProbeResult(
            server="nats://server-a:4222",
            original_index=0,
            available=False,
            round_trip_ms=None,
            error="OSError: timeout",
        ),
        _ServerProbeResult(
            server="nats://server-b:4222",
            original_index=1,
            available=False,
            round_trip_ms=None,
            error="OSError: timeout",
        ),
    )

    try:
        with (
            patch.object(
                hub,
                "_probe_candidate_servers",
                new=AsyncMock(return_value=probe_results),
            ) as probe_mock,
            patch.object(
                hub,
                "_open_connection",
                new=AsyncMock(return_value=fake_nc),
            ) as connect_mock,
        ):
            await hub._connect_once()

        assert probe_mock.await_count == 1
        connect_call = connect_mock.await_args
        assert connect_call is not None
        assert connect_call.args[0] == (
            "nats://server-a:4222",
            "nats://server-b:4222",
        )
        assert hub._connection_plan.candidate_servers == (
            "nats://server-a:4222",
            "nats://server-b:4222",
        )
        assert hub._connection_plan.probe_results == probe_results
    finally:
        await hub.aclose()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_tls_handshake_first_connects_to_tls_first_server(
    nats_tls_first_server: Any,
) -> None:
    tls_context = ssl.create_default_context(cafile=str(nats_tls_first_server.ca_cert_file))

    async with KinopioHub(
        servers=[nats_tls_first_server.tcp_url],
        tls=tls_context,
        tls_hostname="127.0.0.1",
        tls_handshake_first=True,
    ) as hub:
        assert hub.is_connected
        assert hub._connection_plan.active_server == nats_tls_first_server.tcp_url


@pytest.mark.asyncio
@pytest.mark.integration
async def test_latency_mode_tls_handshake_first_probe_connects_to_tls_first_server(
    nats_tls_first_server: Any,
) -> None:
    dead_url = unused_nats_url()
    tls_context = ssl.create_default_context(cafile=str(nats_tls_first_server.ca_cert_file))
    hub = KinopioHub(
        servers=[dead_url, nats_tls_first_server.tcp_url],
        server_selection_mode="latency",
        reconnect_timeout=0.2,
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
        tls=tls_context,
        tls_hostname="127.0.0.1",
        tls_handshake_first=True,
    )

    try:
        await asyncio.wait_for(hub.wait_connected(), timeout=5)

        assert hub.is_connected
        assert hub._connection_plan.candidate_servers[0] == nats_tls_first_server.tcp_url
        assert hub._connection_plan.active_server == nats_tls_first_server.tcp_url

        probe_results = {result.server: result for result in hub._connection_plan.probe_results}
        assert probe_results[nats_tls_first_server.tcp_url].available is True
        assert probe_results[nats_tls_first_server.tcp_url].round_trip_ms is not None
        assert probe_results[dead_url].available is False
    finally:
        await asyncio.wait_for(hub.aclose(), timeout=5)


@pytest.mark.asyncio
async def test_close_connection_only_prefers_direct_close_for_websocket_connections() -> None:
    hub = KinopioHub(
        servers=["wss://server-a:443"],
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )
    fake_nc = FakeNATSConnection(
        "wss://server-a:443",
        transport_name="WebSocketTransport",
    )
    hub._nc = cast(Any, fake_nc)

    try:
        await hub._close_connection_only()

        assert fake_nc.drain_calls == 0
        assert fake_nc.close_calls == 1
    finally:
        await hub.aclose()


@pytest.mark.asyncio
async def test_open_connection_closes_client_when_connect_fails() -> None:
    hub = KinopioHub(
        servers=["wss://server-a:443"],
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )
    fake_nc = FakeNATSConnection(
        "wss://server-a:443",
        transport_name="WebSocketTransport",
        connect_error=TimeoutError(),
    )

    try:
        with patch("kinopio_hub._hub.NATSClient", return_value=fake_nc):
            with pytest.raises(TimeoutError):
                await hub._open_connection(("wss://server-a:443",))

        assert fake_nc.connect_calls == 1
        assert fake_nc.close_calls == 1
    finally:
        await hub.aclose()


@pytest.mark.asyncio
async def test_open_connection_closes_client_when_connect_is_cancelled() -> None:
    hub = KinopioHub(
        servers=["wss://server-a:443"],
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )
    fake_nc = FakeNATSConnection(
        "wss://server-a:443",
        transport_name="WebSocketTransport",
        connect_error=asyncio.CancelledError(),
    )

    try:
        with patch("kinopio_hub._hub.NATSClient", return_value=fake_nc):
            with pytest.raises(asyncio.CancelledError):
                await hub._open_connection(("wss://server-a:443",))

        assert fake_nc.connect_calls == 1
        assert fake_nc.close_calls == 1
    finally:
        await hub.aclose()


@pytest.mark.asyncio
async def test_probe_server_closes_client_when_connect_fails_before_connected() -> None:
    hub = KinopioHub(
        servers=["wss://server-a:443"],
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )
    fake_nc = FakeNATSConnection(
        "wss://server-a:443",
        transport_name="WebSocketTransport",
        connect_error=TimeoutError(),
    )
    fake_nc.is_connected = False

    try:
        with patch("kinopio_hub._hub.NATSClient", return_value=fake_nc):
            result = await hub._probe_server("wss://server-a:443", 0)

        assert result.available is False
        assert fake_nc.connect_calls == 1
        assert fake_nc.close_calls == 1
    finally:
        await hub.aclose()


@pytest.mark.asyncio
async def test_force_close_client_closes_half_open_websocket_transport() -> None:
    hub = KinopioHub(wait_on_first_connect=False)
    fake_nc = FakeNATSConnection("wss://server-a:443", transport_name="WebSocketTransport")
    client_session = FakeClientSession()
    close_task = asyncio.get_running_loop().create_future()
    transport = fake_nc._transport
    assert transport is not None
    transport._ws = None
    transport._client = client_session
    transport._close_task = close_task

    try:
        await hub._force_close_client(cast(Any, fake_nc), message="ignored")

        assert client_session.close_calls == 1
        assert close_task.done()
        assert fake_nc.close_calls == 0
    finally:
        await hub.aclose()


@pytest.mark.asyncio
async def test_open_connection_for_candidates_recovers_websocket_order_after_failure() -> None:
    hub = KinopioHub(
        servers=["wss://server-a:443", "wss://server-b:443", "wss://server-c:443"],
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )
    first_success = FakeNATSConnection("wss://server-b:443", transport_name="WebSocketTransport")
    final_success = FakeNATSConnection("wss://server-b:443", transport_name="WebSocketTransport")

    async def fake_open_connection(servers: Any) -> FakeNATSConnection:
        key = tuple(servers)
        if key == (
            "wss://server-a:443",
            "wss://server-b:443",
            "wss://server-c:443",
        ):
            raise TimeoutError()
        if key == ("wss://server-a:443",):
            raise TimeoutError()
        if key == ("wss://server-b:443",):
            return first_success
        if key == (
            "wss://server-b:443",
            "wss://server-c:443",
            "wss://server-a:443",
        ):
            return final_success
        raise AssertionError(f"unexpected server tuple: {key}")

    try:
        with patch.object(hub, "_open_connection", side_effect=fake_open_connection):
            nc, recovered_candidates = await hub._open_connection_for_candidates(
                ("wss://server-a:443", "wss://server-b:443", "wss://server-c:443")
            )

        assert nc is final_success
        assert recovered_candidates == (
            "wss://server-b:443",
            "wss://server-c:443",
            "wss://server-a:443",
        )
        assert first_success.close_calls == 1
    finally:
        await hub.aclose()


@pytest.mark.asyncio
async def test_open_connection_for_candidates_recovers_websocket_order_after_timeout() -> None:
    hub = KinopioHub(
        servers=["wss://server-a:443", "wss://server-b:443", "wss://server-c:443"],
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )
    first_success = FakeNATSConnection("wss://server-b:443", transport_name="WebSocketTransport")
    final_success = FakeNATSConnection("wss://server-b:443", transport_name="WebSocketTransport")

    async def fake_open_connection(servers: Any) -> FakeNATSConnection:
        key = tuple(servers)
        if key == (
            "wss://server-a:443",
            "wss://server-b:443",
            "wss://server-c:443",
        ):
            await asyncio.sleep(0.05)
            raise AssertionError("initial websocket open should have timed out first")
        if key == ("wss://server-a:443",):
            raise TimeoutError()
        if key == ("wss://server-b:443",):
            return first_success
        if key == (
            "wss://server-b:443",
            "wss://server-c:443",
            "wss://server-a:443",
        ):
            return final_success
        raise AssertionError(f"unexpected server tuple: {key}")

    try:
        with (
            patch.object(hub, "_open_connection", side_effect=fake_open_connection),
            patch.object(
                hub,
                "_websocket_multi_server_attempt_timeout",
                return_value=0.01,
            ),
        ):
            nc, recovered_candidates = await hub._open_connection_for_candidates(
                ("wss://server-a:443", "wss://server-b:443", "wss://server-c:443")
            )

        assert nc is final_success
        assert recovered_candidates == (
            "wss://server-b:443",
            "wss://server-c:443",
            "wss://server-a:443",
        )
        assert first_success.close_calls == 1
    finally:
        await hub.aclose()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_latency_mode_real_probe_handles_partial_failure_and_connects(
    nats_server: Any,
) -> None:
    dead_url = unused_nats_url()
    hub = KinopioHub(
        servers=[dead_url, nats_server.tcp_url],
        server_selection_mode="latency",
        reconnect_timeout=0.2,
        wait_on_first_connect=False,
        auto_retry=False,
        health_report=0,
    )

    try:
        await asyncio.wait_for(hub.wait_connected(), timeout=5)

        assert hub.is_connected
        assert hub._connection_plan.candidate_servers[0] == nats_server.tcp_url
        assert hub._connection_plan.active_server == nats_server.tcp_url

        probe_results = {result.server: result for result in hub._connection_plan.probe_results}
        assert probe_results[nats_server.tcp_url].available is True
        assert probe_results[nats_server.tcp_url].round_trip_ms is not None
        assert probe_results[dead_url].available is False
        assert probe_results[dead_url].round_trip_ms is None
        assert probe_results[dead_url].error is not None
    finally:
        await asyncio.wait_for(hub.aclose(), timeout=5)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_ordered_mode_connects_against_independent_server_pool(
    nats_server_pool: Any,
) -> None:
    server_a, server_b = nats_server_pool.urls
    hub = KinopioHub(
        servers=[server_a, server_b],
        server_selection_mode="ordered",
        wait_on_first_connect=False,
        health_report=0,
    )

    try:
        await asyncio.wait_for(hub.wait_connected(), timeout=5)
        await wait_for_active_server(hub, server_a)
        assert hub._connection_plan.candidate_servers == (server_a, server_b)
        assert hub._connection_plan.active_server == server_a
    finally:
        await asyncio.wait_for(hub.aclose(), timeout=5)


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.slow
async def test_latency_mode_background_hot_switch_preserves_pubsub_and_request(
    nats_cluster: Any,
) -> None:
    server_a, server_b = nats_cluster.urls
    allow_switch = asyncio.Event()

    initial_results = (
        probe_result(server_a, 0, 10.0),
        probe_result(server_b, 1, 75.0),
    )
    switched_results = (
        probe_result(server_a, 0, 80.0),
        probe_result(server_b, 1, 10.0),
    )

    async def fake_probe(_: Any) -> tuple[_ServerProbeResult, ...]:
        if allow_switch.is_set():
            return switched_results
        return initial_results

    hub = KinopioHub(
        servers=[server_a, server_b],
        server_selection_mode="latency",
        wait_on_first_connect=False,
        health_report=0,
    )
    hub._latency_probe_interval_seconds = 0.05

    inbound_messages: list[Any] = []
    outbound_messages: list[Any] = []

    try:
        with patch.object(hub, "_probe_candidate_servers", side_effect=fake_probe):
            async with (
                KinopioHub(servers=[server_b]) as publisher,
                KinopioHub(servers=[server_b]) as subscriber,
                KinopioHub(servers=[server_b]) as responder,
            ):
                await hub.wait_connected()
                await wait_for_active_server(hub, server_a)

                async def inbound_callback(data: Any, _: Any) -> None:
                    inbound_messages.append(data)

                async def outbound_callback(data: Any, _: Any) -> None:
                    outbound_messages.append(data)

                async def responder_handler(data: Any, _: Any) -> Any:
                    return {"echo": data["message"]}

                await hub.chat.messages.subscribe(inbound_callback)
                await subscriber.audit.events.subscribe(outbound_callback)
                await responder.echo.service.serve(responder_handler)

                allow_switch.set()
                await wait_for_active_server(hub, server_b)

                await publisher.chat.messages.publish({"message": "after-switch"})
                await wait_for(lambda: inbound_messages == [{"message": "after-switch"}])

                await hub.audit.events.publish({"source": "hot-switch"})
                await wait_for(lambda: outbound_messages == [{"source": "hot-switch"}])

                response = await hub.echo.service.request({"message": "ok"})
                assert response == {"echo": "ok"}
    finally:
        await hub.aclose()


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.slow
async def test_latency_mode_background_hot_switch_preserves_service_and_value_tracking(
    nats_cluster: Any,
) -> None:
    server_a, server_b = nats_cluster.urls
    allow_switch = asyncio.Event()

    initial_results = (
        probe_result(server_a, 0, 12.0),
        probe_result(server_b, 1, 70.0),
    )
    switched_results = (
        probe_result(server_a, 0, 90.0),
        probe_result(server_b, 1, 15.0),
    )

    async def fake_probe(_: Any) -> tuple[_ServerProbeResult, ...]:
        if allow_switch.is_set():
            return switched_results
        return initial_results

    hub = KinopioHub(
        servers=[server_a, server_b],
        server_selection_mode="latency",
        wait_on_first_connect=False,
        health_report=0,
    )
    hub._latency_probe_interval_seconds = 0.05

    try:
        with patch.object(hub, "_probe_candidate_servers", side_effect=fake_probe):
            async with (
                KinopioHub(servers=[server_b]) as publisher,
                KinopioHub(servers=[server_b]) as requester,
            ):
                tracked_variable = hub.sensors.temperature

                async def adder(data: Any, _: Any) -> Any:
                    return {"sum": data["a"] + data["b"]}

                await hub.math.service.serve(adder)
                await hub.wait_connected()
                await wait_for_active_server(hub, server_a)
                await wait_for(
                    lambda: tracked_variable._tracker_handle is not None
                    and tracked_variable._tracker_handle.active,
                )

                await publisher.sensors.temperature.publish({"reading": 1})
                await wait_for(lambda: tracked_variable.value == {"reading": 1})

                allow_switch.set()
                await wait_for_active_server(hub, server_b)

                nats_cluster.server_a.stop()
                await asyncio.sleep(0.2)

                await publisher.sensors.temperature.publish({"reading": 2})
                await wait_for(lambda: tracked_variable.value == {"reading": 2})

                response = await requester.math.service.request({"a": 2, "b": 3})
                assert response == {"sum": 5}
    finally:
        await hub.aclose()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_publish_updates_cached_value(nats_server: Any) -> None:
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        variable = hub.chat.messages
        payload = {"message": "hello", "count": 1}

        await variable.publish(payload)

        assert variable.value == payload


@pytest.mark.asyncio
@pytest.mark.integration
async def test_subscribe_receives_messages(nats_server: Any) -> None:
    received: list[dict[str, Any]] = []
    event = asyncio.Event()

    async with KinopioHub(servers=[nats_server.tcp_url]) as subscriber, KinopioHub(
        servers=[nats_server.tcp_url]
    ) as publisher:
        async def callback(data: Any, _: Any) -> None:
            received.append(data)
            event.set()

        await subscriber.chat.messages.subscribe(callback)
        await publisher.chat.messages.publish({"user": "alice", "message": "hi"})

        await asyncio.wait_for(event.wait(), timeout=5)

    assert received == [{"user": "alice", "message": "hi"}]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_queue_subscriptions_distribute_work(nats_server: Any) -> None:
    results_a: list[int] = []
    results_b: list[int] = []

    async with KinopioHub(servers=[nats_server.tcp_url]) as hub_a, KinopioHub(
        servers=[nats_server.tcp_url]
    ) as hub_b, KinopioHub(servers=[nats_server.tcp_url]) as publisher:
        async def callback_a(data: Any, _: Any) -> None:
            results_a.append(int(data["job"]))

        async def callback_b(data: Any, _: Any) -> None:
            results_b.append(int(data["job"]))

        await hub_a.jobs.worker.subscribe(callback_a, queue="workers")
        await hub_b.jobs.worker.subscribe(callback_b, queue="workers")

        for index in range(8):
            await publisher.jobs.worker.publish({"job": index})

        await wait_for(lambda: len(results_a) + len(results_b) == 8)

    assert len(results_a) > 0
    assert len(results_b) > 0
    assert sorted(results_a + results_b) == list(range(8))


@pytest.mark.asyncio
@pytest.mark.integration
async def test_request_reply(nats_server: Any) -> None:
    async with KinopioHub(servers=[nats_server.tcp_url]) as server, KinopioHub(
        servers=[nats_server.tcp_url]
    ) as client:
        async def handler(data: Any, _: Any) -> Any:
            return {"result": data["a"] + data["b"]}

        await server.math.calculator.serve(handler)
        response = await client.math.calculator.request({"a": 3, "b": 9})

    assert response == {"result": 12}


@pytest.mark.asyncio
@pytest.mark.integration
async def test_service_exception_returns_error_payload(nats_server: Any) -> None:
    async with KinopioHub(servers=[nats_server.tcp_url]) as server, KinopioHub(
        servers=[nats_server.tcp_url]
    ) as client:
        async def handler(_: Any, __: Any) -> Any:
            raise ValueError("boom")

        await server.math.calculator.serve(handler)
        response = await client.math.calculator.request({"a": 3, "b": 9})

    assert response == {"error": True, "message": "boom"}


@pytest.mark.asyncio
@pytest.mark.integration
async def test_duplicate_publish_is_suppressed(nats_server: Any) -> None:
    received: list[Any] = []

    async with KinopioHub(servers=[nats_server.tcp_url]) as subscriber, KinopioHub(
        servers=[nats_server.tcp_url]
    ) as publisher:
        async def callback(data: Any, _: Any) -> None:
            received.append(data)

        await subscriber.chat.messages.subscribe(callback)
        await publisher.chat.messages.publish({"message": "same"})
        await publisher.chat.messages.publish({"message": "same"})

        await wait_for(lambda: len(received) == 1)
        await asyncio.sleep(0.3)

    assert received == [{"message": "same"}]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_dynamic_attribute_access_reuses_instances(nats_server: Any) -> None:
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        from_scope = hub.get_scope("chat").get_variable("messages")
        from_attr = hub.chat.messages

        assert from_scope is from_attr
        assert from_attr.subject == "chat.messages"


@dataclass
class WrappedJSONCodec:
    def encode(self, data: Any) -> bytes:
        return b"wrapped:" + json.dumps(data).encode("utf-8")

    def decode(self, payload: bytes) -> Any:
        return json.loads(payload.removeprefix(b"wrapped:").decode("utf-8"))


@pytest.mark.asyncio
@pytest.mark.integration
async def test_custom_codec_is_used(nats_server: Any) -> None:
    received: list[Any] = []
    event = asyncio.Event()
    payload = {"message": "hello", "count": 1}

    async with (
        KinopioHub(servers=[nats_server.tcp_url], codec=WrappedJSONCodec()) as subscriber,
        KinopioHub(servers=[nats_server.tcp_url], codec=WrappedJSONCodec()) as publisher,
    ):
        async def callback(data: Any, _: Any) -> None:
            received.append(data)
            event.set()

        await subscriber.chat.messages.subscribe(callback)
        await publisher.chat.messages.publish(payload)

        await asyncio.wait_for(event.wait(), timeout=5)

    assert received == [payload]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_manual_reconnect_restores_service(nats_server: Any) -> None:
    async with KinopioHub(servers=[nats_server.tcp_url]) as server, KinopioHub(
        servers=[nats_server.tcp_url]
    ) as client:
        async def handler(data: Any, _: Any) -> Any:
            return {"result": data["a"] + data["b"]}

        await server.math.calculator.serve(handler)
        await server.reconnect()
        response = await client.math.calculator.request({"a": 1, "b": 2})

    assert response == {"result": 3}


@pytest.mark.asyncio
@pytest.mark.integration
async def test_auto_reconnect_updates_state_and_recovers(nats_server: Any) -> None:
    transitions: list[ConnectionState] = []
    hub = KinopioHub(
        servers=[nats_server.tcp_url],
        reconnect_time_wait=0.2,
        retry_delay=0.1,
        max_retry_delay=0.5,
    )
    hub.on_state_change(transitions.append)

    try:
        await hub.wait_connected()
        nats_server.stop()
        await wait_for(lambda: ConnectionState.DISCONNECTED in transitions, timeout=10)

        nats_server.start()
        await wait_for(lambda: hub.is_connected, timeout=15)
    finally:
        await hub.aclose()

    assert ConnectionState.CONNECTED in transitions
    assert ConnectionState.DISCONNECTED in transitions


@pytest.mark.asyncio
@pytest.mark.integration
async def test_aclose_is_idempotent(nats_server: Any) -> None:
    hub = KinopioHub(servers=[nats_server.tcp_url])
    await hub.wait_connected()

    await hub.aclose()
    await hub.aclose()

    assert hub.state == ConnectionState.DISCONNECTED


@pytest.mark.asyncio
@pytest.mark.integration
async def test_websocket_connection_and_pubsub(nats_server: Any) -> None:
    received: list[Any] = []
    event = asyncio.Event()

    async with KinopioHub(servers=[nats_server.ws_url]) as subscriber, KinopioHub(
        servers=[nats_server.tcp_url]
    ) as publisher:
        async def callback(data: Any, _: Any) -> None:
            received.append(data)
            event.set()

        await subscriber.chat.messages.subscribe(callback)
        await publisher.chat.messages.publish({"message": "ws works"})

        await asyncio.wait_for(event.wait(), timeout=5)

    assert received == [{"message": "ws works"}]
