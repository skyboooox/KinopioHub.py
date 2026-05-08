from __future__ import annotations

import asyncio
import json
import ssl
import uuid
from typing import Literal

from kinopio_hub import KinopioHub

DEFAULT_SERVERS = (
    "wss://home.skyboooox.com:55588",
    "wss://hf.skyboooox.com:55588",
    "wss://hub.skyboooox.com:55588",
)
MODES: tuple["ServerSelectionMode", ...] = ("ordered", "random", "latency")


ServerSelectionMode = Literal["ordered", "random", "latency"]


async def run_mode(mode: ServerSelectionMode, tls_context: ssl.SSLContext) -> None:
    hub = KinopioHub(
        servers=DEFAULT_SERVERS,
        server_selection_mode=mode,
        tls=tls_context,
        auto_retry=False,
        reconnect_timeout=3.0,
        timeout=3.0,
        health_report=0,
    )

    try:
        await asyncio.wait_for(hub.wait_connected(), timeout=20.0)
        print(
            json.dumps(
                {
                    "kind": "mode",
                    "mode": mode,
                    "active_server": hub._connection_plan.active_server,
                    "candidate_servers": hub._connection_plan.candidate_servers,
                    "probe_results": [
                        {
                            "server": item.server,
                            "available": item.available,
                            "round_trip_ms": item.round_trip_ms,
                            "error": item.error,
                        }
                        for item in hub._connection_plan.probe_results
                    ],
                }
            ),
            flush=True,
        )
    finally:
        await hub.aclose()


async def run_interop_smoke(tls_context: ssl.SSLContext) -> None:
    prefix = f"phase7.remote.wss.{uuid.uuid4().hex[:8]}"
    received: list[dict[str, str]] = []
    event = asyncio.Event()
    recovered_event = asyncio.Event()

    async with (
        KinopioHub(
            servers=DEFAULT_SERVERS,
            server_selection_mode="latency",
            tls=tls_context,
            auto_retry=False,
            reconnect_timeout=3.0,
            timeout=3.0,
            health_report=0,
        ) as subscriber,
        KinopioHub(
            servers=DEFAULT_SERVERS,
            server_selection_mode="latency",
            tls=tls_context,
            auto_retry=False,
            reconnect_timeout=3.0,
            timeout=3.0,
            health_report=0,
        ) as publisher,
        KinopioHub(
            servers=DEFAULT_SERVERS,
            server_selection_mode="latency",
            tls=tls_context,
            auto_retry=False,
            reconnect_timeout=3.0,
            timeout=3.0,
            health_report=0,
        ) as server,
        KinopioHub(
            servers=DEFAULT_SERVERS,
            server_selection_mode="latency",
            tls=tls_context,
            auto_retry=False,
            reconnect_timeout=3.0,
            timeout=3.0,
            health_report=0,
        ) as client,
    ):
        async def callback(data: dict[str, str], _message: object) -> None:
            received.append(data)
            event.set()
            if data.get("message") == "after-reconnect":
                recovered_event.set()

        async def handler(data: dict[str, str], _message: object) -> dict[str, str]:
            return {"echo": data["message"]}

        await subscriber.get_scope(prefix).get_variable("events").subscribe(callback)
        await server.get_scope(prefix).get_variable("rpc").serve(handler)
        await publisher.get_scope(prefix).get_variable("events").publish(
            {"message": "hello-phase7"}
        )
        await asyncio.wait_for(event.wait(), timeout=8.0)
        response = await client.get_scope(prefix).get_variable("rpc").request(
            {"message": "ping"},
            timeout=5.0,
        )
        await subscriber.reconnect()
        await server.reconnect()
        await publisher.get_scope(prefix).get_variable("events").publish(
            {"message": "after-reconnect"}
        )
        await asyncio.wait_for(recovered_event.wait(), timeout=8.0)
        recovered_response = await client.get_scope(prefix).get_variable("rpc").request(
            {"message": "after-reconnect"},
            timeout=5.0,
        )

        print(
            json.dumps(
                {
                    "kind": "interop",
                    "active_servers": {
                        "subscriber": subscriber._connection_plan.active_server,
                        "publisher": publisher._connection_plan.active_server,
                        "server": server._connection_plan.active_server,
                        "client": client._connection_plan.active_server,
                    },
                    "received": received,
                    "response": response,
                    "recovered_response": recovered_response,
                }
            ),
            flush=True,
        )


async def main() -> None:
    tls_context = ssl.create_default_context()
    for mode in MODES:
        try:
            await run_mode(mode, tls_context)
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "kind": "mode-error",
                        "mode": mode,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                ),
                flush=True,
            )
    await run_interop_smoke(tls_context)


if __name__ == "__main__":
    asyncio.run(main())
