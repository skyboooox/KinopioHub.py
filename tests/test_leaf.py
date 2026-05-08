from __future__ import annotations

import asyncio
import json
import socket
import ssl
from pathlib import Path
from typing import Any, Callable
from urllib.request import urlopen

import pytest

from kinopio_hub import KinopioHub
from kinopio_hub.leaf import LeafNodeOptions

pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def wait_for(
    predicate: Callable[[], bool],
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition was not met in time")


def unused_nats_url() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = int(sock.getsockname()[1])
    return f"nats://127.0.0.1:{port}"


@pytest.mark.asyncio
async def test_leaf_runtime_exposes_wss_discovery_and_backbone_bridge(
    nats_leaf_backbone: Any,
    tmp_path: Path,
    leaf_node_factory: Any,
) -> None:
    handle = leaf_node_factory(
        LeafNodeOptions(
            backbone_servers=(nats_leaf_backbone.leaf_url,),
            runtime_dir=tmp_path / "leaf-runtime",
        )
    )

    try:
        await wait_for(lambda: handle.status().bridge_state == "connected", timeout=10.0)

        with urlopen(handle.discovery_url, timeout=2.0) as response:
            manifest = json.load(response)

        assert manifest["wssUrl"] == handle.wss_url
        assert manifest["clientUrl"] == handle.client_url
        assert manifest["monitorUrl"] == handle.monitor_url
        assert manifest["discoveryUrl"] == handle.discovery_url
        assert manifest["bridgeState"] == "connected"
        assert manifest["backboneRttMs"] is None
        assert manifest["leaderEpoch"] == 0
        assert manifest["expiresAt"] == manifest["leaseExpiresAt"]
        assert manifest["isLeader"] is True
        assert manifest["candidateRole"] == "leader"

        tls_context = ssl.create_default_context(cafile=str(handle.ca_cert_file))
        inbound_messages: list[dict[str, Any]] = []
        inbound_event = asyncio.Event()

        async with (
            KinopioHub(servers=[handle.wss_url], tls=tls_context) as wss_client,
            KinopioHub(servers=[handle.client_url]) as local_client,
            KinopioHub(servers=[nats_leaf_backbone.client_url]) as upstream_client,
        ):
            async def on_message(data: Any, _: Any) -> None:
                inbound_messages.append(data)
                inbound_event.set()

            async def add_handler(data: Any, _: Any) -> Any:
                return {"sum": data["a"] + data["b"]}

            await wss_client.chat.messages.subscribe(on_message)
            await local_client.chat.messages.publish({"message": "hello-leaf"})
            await asyncio.wait_for(inbound_event.wait(), timeout=5)

            await upstream_client.math.add.serve(add_handler)
            response = await local_client.math.add.request({"a": 2, "b": 5})

        assert inbound_messages == [{"message": "hello-leaf"}]
        assert response == {"sum": 7}
        assert handle.status().client.ready is True
        assert handle.status().websocket.ready is True
        assert handle.status().discovery.ready is True
        assert handle.status().monitor.ready is True
    finally:
        handle.stop()


@pytest.mark.asyncio
async def test_leaf_runtime_starts_without_backbone_and_cleans_up(
    tmp_path: Path,
    leaf_node_factory: Any,
) -> None:
    handle = leaf_node_factory(
        LeafNodeOptions(
            backbone_servers=(unused_nats_url(),),
            runtime_dir=tmp_path / "leaf-runtime-offline",
        )
    )
    runtime_dir = handle._runtime_dir

    try:
        await wait_for(lambda: handle.status().server_ready, timeout=10.0)
        assert handle.status().bridge_state == "connecting"

        received: list[dict[str, Any]] = []
        event = asyncio.Event()

        async with (
            KinopioHub(servers=[handle.client_url]) as subscriber,
            KinopioHub(servers=[handle.client_url]) as publisher,
        ):
            async def callback(data: Any, _: Any) -> None:
                received.append(data)
                event.set()

            await subscriber.local.offline.subscribe(callback)
            await publisher.local.offline.publish({"status": "ok"})
            await asyncio.wait_for(event.wait(), timeout=5)

        assert received == [{"status": "ok"}]
    finally:
        process = handle._process
        handle.stop()

    assert handle.status().bridge_state == "stopped"
    assert process.poll() is not None
    assert not runtime_dir.exists()
