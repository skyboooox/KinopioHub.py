import asyncio
from typing import Any
from kinopio_hub import KinopioHub


async def main() -> None:
    options: dict[str, Any] = {"mesh": False, "servers": [], "discovery": False}
    async with KinopioHub(**options) as hub:
        battery = hub.scope("devices").var("battery")
        for value in (80, 60, 40):
            await battery.set(value)
        print("Offline RAM value:", battery.value)
    async with KinopioHub(**options) as hub:
        print("New instance:", hub.scope("devices").var("battery").value)


asyncio.run(main())
