from __future__ import annotations

import asyncio
import ssl

from kinopio_hub import KinopioHub
from kinopio_hub.leaf import LeafNodeOptions, start_leaf_node


async def main() -> None:
    # Equivalent CLI: kinopio-hub leaf start
    with start_leaf_node(
        LeafNodeOptions(
            backbone_servers=(),
        )
    ) as leaf:
        print("client:", leaf.client_url)
        print("wss:", leaf.wss_url)
        print("discovery:", leaf.discovery_url)
        print("monitor:", leaf.monitor_url)
        print("bridge:", leaf.status().bridge_state)

        tls_context = ssl.create_default_context(cafile=str(leaf.ca_cert_file))
        async with KinopioHub(servers=[leaf.wss_url], tls=tls_context) as hub:
            await hub.demo.message.publish({"text": "hello from local leaf"})


asyncio.run(main())
