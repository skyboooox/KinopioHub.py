import asyncio
from kinopio_hub import KinopioHub
from _options import options


async def main() -> None:
    async with KinopioHub(**options("basic")) as hub:
        battery = hub.scope("devices").var("battery")
        await battery.set(80)
        await hub.flush()
        print("Current value:", battery.value)


asyncio.run(main())
