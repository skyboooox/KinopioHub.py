from __future__ import annotations

import asyncio

from kinopio_hub import KinopioHub


async def main() -> None:
    hub = KinopioHub(servers=["nats://demo.nats.io:4222"], debug=True)
    await hub.wait_connected()
    print("Connected:", hub.is_connected, hub.state.value)
    await asyncio.sleep(3)
    await hub.aclose()


if __name__ == "__main__":
    asyncio.run(main())
