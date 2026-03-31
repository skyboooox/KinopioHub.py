from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import pytest

from kinopio_hub import ConnectionState, KinopioHub


async def wait_for(predicate: Any, *, timeout: float = 5.0, interval: float = 0.05) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition was not met in time")


@pytest.mark.asyncio
async def test_publish_updates_cached_value(nats_server: Any) -> None:
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        variable = hub.chat.messages
        payload = {"message": "hello", "count": 1}

        await variable.publish(payload)

        assert variable.value == payload


@pytest.mark.asyncio
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
async def test_aclose_is_idempotent(nats_server: Any) -> None:
    hub = KinopioHub(servers=[nats_server.tcp_url])
    await hub.wait_connected()

    await hub.aclose()
    await hub.aclose()

    assert hub.state == ConnectionState.DISCONNECTED


@pytest.mark.asyncio
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
